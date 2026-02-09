"""Read-only VMware discovery clients for Workstation and ESXi/vCenter."""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim


class VMwareClientError(Exception):
    """Raised when VMware discovery cannot complete."""


class VMwareClient:
    """Base interface for VMware discovery clients."""

    source: str

    def discover_vms(self) -> list[dict[str, Any]]:
        raise NotImplementedError


@dataclass
class WorkstationVMwareClient(VMwareClient):
    """Discover local VMware Workstation/Fusion VMs from .vmx files."""

    scan_paths: list[str] | None = None
    source: str = "workstation"

    def _effective_scan_paths(self) -> list[Path]:
        if self.scan_paths:
            return [Path(p).expanduser() for p in self.scan_paths]

        raw = os.getenv("VMWARE_WORKSTATION_PATHS", "")
        if raw.strip():
            return [Path(p.strip()).expanduser() for p in raw.split(",") if p.strip()]

        # Safe defaults for common home directories.
        return [
            Path.home() / "vmware",
            Path.home() / "VMs",
            Path.home() / "Virtual Machines",
        ]

    @staticmethod
    def _extract_name_from_vmx(vmx_path: Path) -> str:
        try:
            for line in vmx_path.read_text(errors="ignore").splitlines():
                if line.strip().startswith("displayName") and "=" in line:
                    _, value = line.split("=", 1)
                    return value.strip().strip('"')
        except OSError:
            pass
        return vmx_path.stem

    @staticmethod
    def _collect_disks(vm_dir: Path) -> list[dict[str, Any]]:
        disks: list[dict[str, Any]] = []
        for disk in sorted(vm_dir.glob("*.vmdk")):
            try:
                size_bytes = disk.stat().st_size
            except OSError:
                size_bytes = None
            disks.append(
                {
                    "path": str(disk),
                    "size_bytes": size_bytes,
                }
            )
        return disks

    def discover_vms(self) -> list[dict[str, Any]]:
        discovered: list[dict[str, Any]] = []
        for root in self._effective_scan_paths():
            if not root.exists() or not root.is_dir():
                continue

            for vmx in root.rglob("*.vmx"):
                vm_name = self._extract_name_from_vmx(vmx)
                disks = self._collect_disks(vmx.parent)
                discovered.append(
                    {
                        "name": vm_name,
                        "cpu": None,
                        "ram": None,
                        "disks": disks,
                        "power_state": "unknown",
                        "vmx_path": str(vmx),
                    }
                )
        return discovered


@dataclass
class ESXiVMwareClient(VMwareClient):
    """Discover VMs from ESXi/vCenter using pyVmomi in read-only mode."""

    host: str
    username: str
    password: str
    port: int = 443
    insecure: bool = True
    source: str = "esxi"

    @classmethod
    def from_env(cls) -> "ESXiVMwareClient":
        host = os.getenv("VMWARE_ESXI_HOST", "").strip()
        username = os.getenv("VMWARE_ESXI_USERNAME", "").strip()
        password = os.getenv("VMWARE_ESXI_PASSWORD", "").strip()
        if not host or not username or not password:
            raise VMwareClientError(
                "VMWARE_ESXI_HOST, VMWARE_ESXI_USERNAME, and VMWARE_ESXI_PASSWORD are required."
            )

        port = int(os.getenv("VMWARE_ESXI_PORT", "443"))
        insecure = os.getenv("VMWARE_ESXI_INSECURE", "true").lower() in {"1", "true", "yes", "on"}
        return cls(host=host, username=username, password=password, port=port, insecure=insecure)

    def _connect(self):
        try:
            if self.insecure:
                ctx = ssl._create_unverified_context()
            else:
                ctx = ssl.create_default_context()

            return SmartConnect(
                host=self.host,
                user=self.username,
                pwd=self.password,
                port=self.port,
                sslContext=ctx,
            )
        except Exception as exc:
            raise VMwareClientError(f"Failed to connect to ESXi/vCenter '{self.host}': {exc}") from exc

    @staticmethod
    def _serialize_vm(vm: vim.VirtualMachine) -> dict[str, Any]:
        cpu = None
        ram = None
        power_state = None
        disks: list[dict[str, Any]] = []

        config = getattr(vm, "config", None)
        if config and getattr(config, "hardware", None):
            cpu = getattr(config.hardware, "numCPU", None)
            ram = getattr(config.hardware, "memoryMB", None)

            for device in getattr(config.hardware, "device", []):
                if isinstance(device, vim.vm.device.VirtualDisk):
                    size_bytes = None
                    if getattr(device, "capacityInKB", None) is not None:
                        size_bytes = int(device.capacityInKB) * 1024
                    disks.append(
                        {
                            "label": getattr(getattr(device, "deviceInfo", None), "label", "disk"),
                            "size_bytes": size_bytes,
                        }
                    )

        runtime = getattr(vm, "runtime", None)
        if runtime and getattr(runtime, "powerState", None) is not None:
            power_state = str(runtime.powerState)

        return {
            "name": vm.name,
            "cpu": cpu,
            "ram": ram,
            "disks": disks,
            "power_state": power_state,
        }

    def discover_vms(self) -> list[dict[str, Any]]:
        si = self._connect()
        try:
            content = si.RetrieveContent()
            container = content.viewManager.CreateContainerView(
                content.rootFolder,
                [vim.VirtualMachine],
                True,
            )
            try:
                return [self._serialize_vm(vm) for vm in container.view]
            finally:
                container.Destroy()
        except VMwareClientError:
            raise
        except Exception as exc:
            raise VMwareClientError(f"Failed to discover VMs from ESXi/vCenter '{self.host}': {exc}") from exc
        finally:
            try:
                Disconnect(si)
            except Exception:
                pass
