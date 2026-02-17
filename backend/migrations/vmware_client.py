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
        patterns = ["*.vmdk", "*.raw", "*.img", "*.vhd", "*.vhdx", "*.vdi", "*.qcow2"]
        seen: set[str] = set()
        candidates: list[Path] = []
        for pattern in patterns:
            for disk in sorted(vm_dir.glob(pattern)):
                key = str(disk.resolve()) if disk.exists() else str(disk)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(disk)

        for disk in sorted(candidates, key=lambda p: p.name):
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
                        "metadata": {"vmx_path": str(vmx)},
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
    def _snapshot_count(tree_nodes: list[Any] | None) -> int:
        if not tree_nodes:
            return 0
        total = 0
        for node in tree_nodes:
            total += 1
            total += ESXiVMwareClient._snapshot_count(getattr(node, "childSnapshotList", None))
        return total

    @staticmethod
    def _disk_metadata(device: Any) -> dict[str, Any]:
        size_bytes = None
        if getattr(device, "capacityInKB", None) is not None:
            size_bytes = int(device.capacityInKB) * 1024

        backing = getattr(device, "backing", None)
        datastore = None
        filename = None
        thin_provisioned = None
        eagerly_scrub = None
        if backing is not None:
            if getattr(backing, "datastore", None) is not None:
                datastore = getattr(backing.datastore, "name", None)
            filename = getattr(backing, "fileName", None)
            thin_provisioned = getattr(backing, "thinProvisioned", None)
            eagerly_scrub = getattr(backing, "eagerlyScrub", None)

        return {
            "label": getattr(getattr(device, "deviceInfo", None), "label", "disk"),
            "size_bytes": size_bytes,
            "key": getattr(device, "key", None),
            "unit_number": getattr(device, "unitNumber", None),
            "controller_key": getattr(device, "controllerKey", None),
            "datastore": datastore,
            "filename": filename,
            "thin_provisioned": thin_provisioned,
            "eagerly_scrub": eagerly_scrub,
        }

    @staticmethod
    def _nic_metadata(device: Any) -> dict[str, Any]:
        backing = getattr(device, "backing", None)
        network_name = None
        portgroup = None
        if backing is not None:
            network_name = getattr(backing, "deviceName", None)
            if network_name is None and getattr(backing, "network", None) is not None:
                network_name = getattr(backing.network, "name", None)
            if getattr(backing, "port", None) is not None:
                portgroup = getattr(backing.port, "portgroupKey", None)

        return {
            "label": getattr(getattr(device, "deviceInfo", None), "label", "nic"),
            "mac_address": getattr(device, "macAddress", None),
            "key": getattr(device, "key", None),
            "unit_number": getattr(device, "unitNumber", None),
            "controller_key": getattr(device, "controllerKey", None),
            "network": network_name,
            "portgroup_key": portgroup,
            "connected": bool(
                getattr(getattr(device, "connectable", None), "connected", False)
            ),
            "start_connected": bool(
                getattr(getattr(device, "connectable", None), "startConnected", False)
            ),
        }

    @staticmethod
    def _serialize_vm(vm: vim.VirtualMachine) -> dict[str, Any]:
        cpu = None
        ram = None
        power_state = None
        disks: list[dict[str, Any]] = []
        nics: list[dict[str, Any]] = []

        config = getattr(vm, "config", None)
        if config and getattr(config, "hardware", None):
            cpu = getattr(config.hardware, "numCPU", None)
            ram = getattr(config.hardware, "memoryMB", None)

            for device in getattr(config.hardware, "device", []):
                if isinstance(device, vim.vm.device.VirtualDisk):
                    disks.append(ESXiVMwareClient._disk_metadata(device))
                elif isinstance(device, vim.vm.device.VirtualEthernetCard):
                    nics.append(ESXiVMwareClient._nic_metadata(device))

        runtime = getattr(vm, "runtime", None)
        if runtime and getattr(runtime, "powerState", None) is not None:
            power_state = str(runtime.powerState)

        vmx_datastore_path = None
        instance_uuid = None
        bios_uuid = None
        guest_id = None
        guest_full_name = None
        firmware = None
        vm_hw_version = None
        num_cores_per_socket = None
        cpu_hot_add_enabled = None
        memory_hot_add_enabled = None
        memory_reservation_locked_to_max = None
        annotation = None
        has_snapshots = False
        if config:
            files = getattr(config, "files", None)
            if files and getattr(files, "vmPathName", None):
                vmx_datastore_path = str(files.vmPathName)
            if getattr(config, "instanceUuid", None):
                instance_uuid = str(config.instanceUuid)
            if getattr(config, "uuid", None):
                bios_uuid = str(config.uuid)
            guest_id = getattr(config, "guestId", None)
            guest_full_name = getattr(config, "guestFullName", None)
            firmware = getattr(config, "firmware", None)
            vm_hw_version = getattr(config, "version", None)
            num_cores_per_socket = getattr(config, "numCoresPerSocket", None)
            cpu_hot_add_enabled = getattr(config, "cpuHotAddEnabled", None)
            memory_hot_add_enabled = getattr(config, "memoryHotAddEnabled", None)
            memory_reservation_locked_to_max = getattr(config, "memoryReservationLockedToMax", None)
            annotation = getattr(config, "annotation", None)

        snapshot_obj = getattr(vm, "snapshot", None)
        snapshot_count = 0
        current_snapshot_name = None
        if snapshot_obj is not None:
            has_snapshots = True
            snapshot_count = ESXiVMwareClient._snapshot_count(getattr(snapshot_obj, "rootSnapshotList", None))
            current = getattr(snapshot_obj, "currentSnapshot", None)
            if current is not None and getattr(current, "name", None):
                current_snapshot_name = str(current.name)

        summary = getattr(vm, "summary", None)
        summary_cfg = getattr(summary, "config", None) if summary is not None else None
        summary_guest = getattr(summary, "guest", None) if summary is not None else None
        summary_storage = getattr(summary, "storage", None) if summary is not None else None

        provisioned_storage_bytes = None
        committed_storage_bytes = None
        uncommitted_storage_bytes = None
        unshared_storage_bytes = None
        if summary_storage is not None:
            committed_storage_bytes = getattr(summary_storage, "committed", None)
            uncommitted_storage_bytes = getattr(summary_storage, "uncommitted", None)
            unshared_storage_bytes = getattr(summary_storage, "unshared", None)
            if committed_storage_bytes is not None and uncommitted_storage_bytes is not None:
                provisioned_storage_bytes = int(committed_storage_bytes) + int(uncommitted_storage_bytes)

        host_name = None
        host_moid = None
        cluster_name = None
        if runtime is not None and getattr(runtime, "host", None) is not None:
            host_ref = runtime.host
            host_name = getattr(host_ref, "name", None)
            host_moid = getattr(host_ref, "_moId", None)
            parent_ref = getattr(host_ref, "parent", None)
            if parent_ref is not None:
                cluster_name = getattr(parent_ref, "name", None)

        datastores = []
        for ds in getattr(vm, "datastore", []) or []:
            ds_name = getattr(ds, "name", None)
            if ds_name:
                datastores.append(ds_name)

        network_names = []
        for net in getattr(vm, "network", []) or []:
            net_name = getattr(net, "name", None)
            if net_name:
                network_names.append(net_name)

        guest = getattr(vm, "guest", None)
        guest_hostname = None
        guest_ip = None
        guest_state = None
        guest_tools_running_status = None
        guest_tools_version_status = None
        guest_tools_version = None
        guest_nics = []
        if guest is not None:
            guest_hostname = getattr(guest, "hostName", None)
            guest_ip = getattr(guest, "ipAddress", None)
            guest_state = getattr(guest, "guestState", None)
            guest_tools_running_status = getattr(guest, "toolsRunningStatus", None)
            guest_tools_version_status = getattr(guest, "toolsVersionStatus2", None)
            guest_tools_version = getattr(guest, "toolsVersion", None)
            for gnet in getattr(guest, "net", []) or []:
                guest_nics.append(
                    {
                        "network": getattr(gnet, "network", None),
                        "mac_address": getattr(gnet, "macAddress", None),
                        "connected": bool(getattr(gnet, "connected", False)),
                        "ips": list(getattr(gnet, "ipAddress", []) or []),
                    }
                )

        boot_time = None
        connection_state = None
        if runtime is not None:
            if getattr(runtime, "bootTime", None) is not None:
                boot_time = runtime.bootTime.isoformat()
            if getattr(runtime, "connectionState", None) is not None:
                connection_state = str(runtime.connectionState)

        return {
            "name": vm.name,
            "cpu": cpu,
            "ram": ram,
            "disks": disks,
            "nics": nics,
            "guest_ip": guest_ip,
            "power_state": power_state,
            "metadata": {
                "moid": getattr(vm, "_moId", None),
                "vmx_datastore_path": vmx_datastore_path,
                "instance_uuid": instance_uuid,
                "bios_uuid": bios_uuid,
                "guest_id": guest_id,
                "guest_full_name": guest_full_name,
                "firmware": firmware,
                "vm_hw_version": vm_hw_version,
                "num_cores_per_socket": num_cores_per_socket,
                "cpu_hot_add_enabled": cpu_hot_add_enabled,
                "memory_hot_add_enabled": memory_hot_add_enabled,
                "memory_reservation_locked_to_max": memory_reservation_locked_to_max,
                "annotation": annotation,
                "has_snapshots": has_snapshots,
                "snapshot_count": snapshot_count,
                "current_snapshot_name": current_snapshot_name,
                "host_name": host_name,
                "host_moid": host_moid,
                "cluster_name": cluster_name,
                "connection_state": connection_state,
                "boot_time": boot_time,
                "datastores": datastores,
                "networks": network_names,
                "nics": nics,
                "guest": {
                    "hostname": guest_hostname,
                    "ip_address": guest_ip,
                    "state": guest_state,
                    "tools_running_status": guest_tools_running_status,
                    "tools_version_status": guest_tools_version_status,
                    "tools_version": guest_tools_version,
                    "nics": guest_nics,
                },
                "storage": {
                    "disk_count": len(disks),
                    "provisioned_bytes": provisioned_storage_bytes,
                    "committed_bytes": committed_storage_bytes,
                    "uncommitted_bytes": uncommitted_storage_bytes,
                    "unshared_bytes": unshared_storage_bytes,
                },
                "summary": {
                    "template": getattr(summary_cfg, "template", None),
                    "guest_full_name": getattr(summary_cfg, "guestFullName", None),
                    "guest_id": getattr(summary_cfg, "guestId", None),
                    "guest_host_name": getattr(summary_guest, "hostName", None),
                    "guest_ip_address": getattr(summary_guest, "ipAddress", None),
                },
            },
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
