"""Safe conversion planning for virt-v2v workflows."""

from __future__ import annotations

import os
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .models import DiscoveredVM


class ConversionPlanningError(Exception):
    """Raised when conversion planning cannot be generated safely."""


@dataclass
class ConversionPlan:
    command: str
    command_args: list[str]
    input_disks: list[str]
    output_path: str
    notes: list[str] = field(default_factory=list)


def _sanitize_name(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]", "-", value).strip("-._")
    return clean or "vm"


def _extract_disk_paths(disks: Any) -> list[str]:
    if not isinstance(disks, list):
        return []

    paths: list[str] = []
    for disk in disks:
        if isinstance(disk, dict):
            path = disk.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path.strip())
        elif isinstance(disk, str) and disk.strip():
            paths.append(disk.strip())

    seen = set()
    deduped: list[str] = []
    for path in paths:
        if path not in seen:
            deduped.append(path)
            seen.add(path)
    return deduped


def _build_command(args: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in args)


def plan_vmware_conversion(
    discovered_vm: DiscoveredVM,
    output_dir: str | None = None,
    *,
    esxi_uri: str | None = None,
    password_file: str | None = None,
    esxi_transport: str | None = None,
    vddk_libdir: str | None = None,
    vddk_thumbprint: str | None = None,
) -> ConversionPlan:
    """Build a virt-v2v command plan from discovered VM data."""

    configured_output_dir = output_dir or os.getenv("MIGRATION_OUTPUT_DIR", "/var/lib/vm-migrator/images")
    output_dir = Path(configured_output_dir).expanduser()
    output_path = str(output_dir / f"{_sanitize_name(discovered_vm.name)}.qcow2")

    if discovered_vm.source == DiscoveredVM.Source.WORKSTATION:
        input_disks = _extract_disk_paths(discovered_vm.disks)
        if not input_disks:
            raise ConversionPlanningError(
                f"No local VMDK paths available for workstation VM '{discovered_vm.name}'."
            )

        command_args: list[str]
        notes: list[str] = []
        vmx_path = None
        if isinstance(discovered_vm.metadata, dict):
            vmx_path = discovered_vm.metadata.get("vmx_path")

        # Prefer VMX import for full VM conversion (handles multi-disk VMs).
        if isinstance(vmx_path, str) and vmx_path.strip():
            vmx = Path(vmx_path).expanduser()
            if vmx.exists() and vmx.is_file():
                command_args = [
                    "virt-v2v",
                    "-i",
                    "vmx",
                    str(vmx),
                    "-o",
                    "local",
                    "-os",
                    str(output_dir),
                    "-of",
                    "qcow2",
                    "-on",
                    discovered_vm.name,
                ]
                if len(input_disks) > 1:
                    notes.append("multi-disk VM detected; conversion uses VMX import to preserve all disks")
            else:
                notes.append(f"vmx_path not found ({vmx}); falling back to first disk conversion")
                command_args = [
                    "virt-v2v",
                    "-i",
                    "disk",
                    input_disks[0],
                    "-o",
                    "local",
                    "-os",
                    str(output_dir),
                    "-of",
                    "qcow2",
                    "-on",
                    discovered_vm.name,
                ]
                if len(input_disks) > 1:
                    notes.append("fallback mode uses first disk only")
        else:
            command_args = [
                "virt-v2v",
                "-i",
                "disk",
                input_disks[0],
                "-o",
                "local",
                "-os",
                str(output_dir),
                "-of",
                "qcow2",
                "-on",
                discovered_vm.name,
            ]
            if len(input_disks) > 1:
                notes.append("vmx_path unavailable; fallback mode uses first disk only")

        return ConversionPlan(
            command=_build_command(command_args),
            command_args=command_args,
            input_disks=input_disks,
            output_path=output_path,
            notes=notes,
        )

    if discovered_vm.source == DiscoveredVM.Source.ESXI:
        # ESXi conversion (safe default): use libvirt ESX driver over HTTPS.
        # This does not require enabling ESXi SSH or installing proprietary VDDK.
        if not esxi_uri:
            raise ConversionPlanningError("Missing esxi_uri for ESXi conversion planning.")

        command_args = ["virt-v2v", "-i", "libvirt", "-ic", esxi_uri]
        if password_file:
            command_args += ["-ip", password_file]

        notes = ["esxi conversion via libvirt esx:// (requires VM powered off for safety)"]
        if esxi_transport == "vddk":
            if not vddk_libdir or not vddk_thumbprint:
                raise ConversionPlanningError("VDDK transport requires vddk_libdir and vddk_thumbprint.")
            command_args += [
                "-it",
                "vddk",
                "-io",
                f"vddk-libdir={vddk_libdir}",
                "-io",
                f"vddk-thumbprint={vddk_thumbprint}",
            ]
            notes = ["esxi conversion via VDDK (requires nbdkit-vddk-plugin; VM powered off)"]

        command_args += [
            discovered_vm.name,
            "-o",
            "local",
            "-os",
            str(output_dir),
            "-of",
            "qcow2",
            "-on",
            discovered_vm.name,
        ]
        return ConversionPlan(
            command=_build_command(command_args),
            command_args=command_args,
            input_disks=[],
            output_path=output_path,
            notes=notes,
        )

    raise ConversionPlanningError(
        f"Unsupported VMware source '{discovered_vm.source}' for VM '{discovered_vm.name}'."
    )
