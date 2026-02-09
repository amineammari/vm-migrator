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


def plan_vmware_conversion(discovered_vm: DiscoveredVM, output_dir: str | None = None) -> ConversionPlan:
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

        # For current safe execution phase we convert the primary disk.
        primary_disk = input_disks[0]
        command_args = [
            "virt-v2v",
            "-i",
            "disk",
            primary_disk,
            "-o",
            "local",
            "-os",
            str(output_dir),
            "-of",
            "qcow2",
            "-on",
            discovered_vm.name,
        ]
        notes: list[str] = []
        if len(input_disks) > 1:
            notes.append("multiple disks detected; current execution uses first disk")

        return ConversionPlan(
            command=_build_command(command_args),
            command_args=command_args,
            input_disks=input_disks,
            output_path=output_path,
            notes=notes,
        )

    if discovered_vm.source == DiscoveredVM.Source.ESXI:
        input_disks = _extract_disk_paths(discovered_vm.disks)
        command_args = [
            "virt-v2v",
            "-i",
            "vmx",
            "<esxi-vm-path>",
            "-it",
            "vddk",
            "-io",
            "vddk-libdir=/path/to/vmware-vix-disklib-distrib",
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
            input_disks=input_disks,
            output_path=output_path,
            notes=["esxi conversion execution is not implemented yet"],
        )

    raise ConversionPlanningError(
        f"Unsupported VMware source '{discovered_vm.source}' for VM '{discovered_vm.name}'."
    )
