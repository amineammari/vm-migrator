from __future__ import annotations

import ssl
import time
from typing import Any

from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim


class SnapshotError(Exception):
    """Raised when VMware snapshot operations fail."""


def _connect(host: str, username: str, password: str, port: int, insecure: bool):
    try:
        if insecure:
            context = ssl._create_unverified_context()
        else:
            context = ssl.create_default_context()
        return SmartConnect(host=host, user=username, pwd=password, port=port, sslContext=context)
    except Exception as exc:  # noqa: BLE001
        raise SnapshotError(f"Failed VMware connection to {host}:{port}: {exc}") from exc


def _find_vm_by_moid(content: Any, moid: str) -> vim.VirtualMachine | None:
    container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    try:
        for vm in container.view:
            if str(getattr(vm, "_moId", "")) == str(moid):
                return vm
    finally:
        container.Destroy()
    return None


def _wait_for_task(task: Any, timeout_seconds: int = 900) -> None:
    deadline = time.monotonic() + max(1, int(timeout_seconds))
    while time.monotonic() < deadline:
        state = str(getattr(getattr(task, "info", None), "state", ""))
        if state == "success":
            return
        if state == "error":
            err = getattr(getattr(task, "info", None), "error", None)
            raise SnapshotError(f"Snapshot task failed: {err}")
        time.sleep(2)
    raise SnapshotError(f"Timed out waiting for snapshot task after {timeout_seconds}s.")


def create_vm_snapshot(
    *,
    vmware_host: str,
    vmware_username: str,
    vmware_password: str,
    vmware_port: int,
    vmware_insecure: bool,
    vm_moid: str,
    snapshot_name: str,
    description: str = "",
    timeout_seconds: int = 900,
) -> dict[str, Any]:
    si = _connect(
        vmware_host,
        vmware_username,
        vmware_password,
        vmware_port,
        vmware_insecure,
    )
    try:
        content = si.RetrieveContent()
        vm = _find_vm_by_moid(content, vm_moid)
        if vm is None:
            raise SnapshotError(f"Could not find VM with moid={vm_moid}.")

        existing = getattr(vm, "snapshot", None)
        if existing is not None and getattr(existing, "currentSnapshot", None) is not None:
            current = existing.currentSnapshot
            current_name = getattr(current, "name", None)
            if current_name == snapshot_name:
                return {
                    "status": "exists",
                    "snapshot_name": snapshot_name,
                }

        task = vm.CreateSnapshot_Task(
            name=snapshot_name,
            description=description,
            memory=False,
            quiesce=False,
        )
        _wait_for_task(task, timeout_seconds=timeout_seconds)
        return {
            "status": "created",
            "snapshot_name": snapshot_name,
        }
    except SnapshotError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SnapshotError(f"Failed to create snapshot '{snapshot_name}': {exc}") from exc
    finally:
        try:
            Disconnect(si)
        except Exception:
            pass

