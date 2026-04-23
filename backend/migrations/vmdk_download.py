"""
VMDK Download Utility for VM Migration Platform

Downloads VMDK disks from VMware ESXi as a parallel, non-blocking side-effect.
Does NOT interfere with the main migration pipeline.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from celery import shared_task
from django.conf import settings

logger = logging.getLogger(__name__)


@dataclass
class VmdkDownloadResult:
    """Result of VMDK download operation."""

    success: bool
    downloaded_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    total_bytes: int = 0
    duration_seconds: float = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "downloaded_paths": self.downloaded_paths,
            "errors": self.errors,
            "total_bytes": self.total_bytes,
            "duration_seconds": round(self.duration_seconds, 2),
        }


def get_vmdk_base_dir() -> Path:
    """Get the base directory for VMDK storage."""
    base_dir = getattr(settings, "MIGRATION_OUTPUT_DIR", None)
    if base_dir:
        return Path(base_dir) / "vmdk"
    return Path("/var/lib/vm-migrator/vmdk")


def ensure_vmdk_directory(vm_name: str, job_id: Optional[int] = None) -> Path:
    """Create and return the target directory for a VM's VMDK files."""
    if job_id:
        dir_name = f"{vm_name}-job{job_id}"
    else:
        dir_name = vm_name

    dir_path = get_vmdk_base_dir() / dir_name
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def _sanitize_filename(name: str) -> str:
    """Sanitize filename to be safe for filesystem."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, "_")
    return name[:255]


def _check_disk_space(required_bytes: int, path: Path) -> bool:
    """Check if there's enough disk space available."""
    try:
        stat = os.statvfs(path)
        available_bytes = stat.f_bavail * stat.f_frsize
        return available_bytes >= required_bytes
    except Exception:
        return True


def download_vmdk_from_esxi(
    vm_name: str,
    esxi_host: str,
    esxi_port: int,
    esxi_username: str,
    esxi_password: str,
    datastore_name: str,
    vmdk_paths: list[str],
    job_id: Optional[int] = None,
    insecure: bool = True,
) -> VmdkDownloadResult:
    """
    Download VMDK disks from ESXi datastore.

    This runs as a PARALLEL side-effect and does NOT block the migration pipeline.
    If download fails, the main migration continues normally.

    Args:
        vm_name: Name of the VM
        esxi_host: ESXi host IP/hostname
        esxi_port: ESXi port (default 443)
        esxi_username: ESXi username
        esxi_password: ESXi password
        datastore_name: Datastore name on ESXi
        vmdk_paths: List of VMDK paths relative to datastore
        job_id: Optional job ID for directory naming
        insecure: Allow insecure SSL connections

    Returns:
        VmdkDownloadResult with paths and status
    """
    start_time = time.time()
    result = VmdkDownloadResult(success=True)

    if not vmdk_paths:
        result.errors.append("No VMDK paths provided")
        result.success = False
        return result

    target_dir = ensure_vmdk_directory(vm_name, job_id)

    logger.info(
        "vmdk_download.start",
        extra={
            "vm_name": vm_name,
            "esxi_host": esxi_host,
            "datastore": datastore_name,
            "vmdk_count": len(vmdk_paths),
            "target_dir": str(target_dir),
        },
    )

    govc_bin = shutil.which("govc")
    if not govc_bin:
        result.errors.append("govc binary not found")
        result.success = False
        return result

    insecure_flag = "-insecure" if insecure else ""
    env = os.environ.copy()
    env["GOVC_URL"] = f"https://{esxi_username}:{esxi_password}@{esxi_host}:{esxi_port}"
    env["GOVC_DATACENTER"] = "ha-datacenter"
    env["GOVC_INSECURE"] = "1" if insecure else "0"

    for vmdk_path in vmdk_paths:
        try:
            full_datastore_path = f"[{datastore_name}] {vmdk_path}"
            filename = _sanitize_filename(Path(vmdk_path).name)
            target_path = target_dir / filename

            cmd = [
                govc_bin,
                "datastore.download",
                "-ds", datastore_name,
                full_datastore_path,
                str(target_path),
            ]
            if insecure_flag:
                cmd.append(insecure_flag)

            logger.info("vmdk_download.file_start", extra={"path": vmdk_path, "target": str(target_path)})

            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=getattr(settings, "VMDK_DOWNLOAD_TIMEOUT", 7200),
            )

            if proc.returncode != 0:
                error_msg = proc.stderr.strip() or proc.stdout.strip() or "Unknown error"
                logger.warning("vmdk_download.file_failed", extra={"path": vmdk_path, "error": error_msg})
                result.errors.append(f"{vmdk_path}: {error_msg}")
                continue

            if not target_path.exists():
                result.errors.append(f"{vmdk_path}: file not created")
                continue

            file_size = target_path.stat().st_size
            if file_size == 0:
                result.errors.append(f"{vmdk_path}: empty file")
                continue

            if not _check_disk_space(file_size, target_dir):
                result.errors.append(f"{vmdk_path}: insufficient disk space")
                target_path.unlink(missing_ok=True)
                continue

            result.downloaded_paths.append(str(target_path))
            result.total_bytes += file_size

            logger.info(
                "vmdk_download.file_complete",
                extra={"path": vmdk_path, "size_bytes": file_size},
            )

        except subprocess.TimeoutExpired:
            result.errors.append(f"{vmdk_path}: download timeout")
            logger.warning("vmdk_download.file_timeout", extra={"path": vmdk_path})
        except Exception as e:
            result.errors.append(f"{vmdk_path}: {str(e)}")
            logger.warning("vmdk_download.file_error", extra={"path": vmdk_path, "error": str(e)})

    result.duration_seconds = time.time() - start_time
    result.success = len(result.downloaded_paths) > 0

    logger.info(
        "vmdk_download.complete",
        extra={
            "vm_name": vm_name,
            "success": result.success,
            "downloaded_count": len(result.downloaded_paths),
            "total_bytes": result.total_bytes,
            "duration_seconds": round(result.duration_seconds, 2),
        },
    )

    return result


def download_vmdk_from_vm(
    discovered_vm: dict[str, Any],
    vmware_session: Any,
    job_id: Optional[int] = None,
) -> VmdkDownloadResult:
    """
    Download VMDK disks from a discovered VM.

    Args:
        discovered_vm: Discovered VM dict with disk info
        vmware_session: VMwareEndpointSession instance
        job_id: Optional job ID

    Returns:
        VmdkDownloadResult
    """
    vm_name = discovered_vm.get("name", "unknown")
    disks = discovered_vm.get("disks", [])

    vmdk_paths = []
    for disk in disks:
        if isinstance(disk, dict):
            path = disk.get("path") or disk.get("filename")
            if path and str(path).lower().endswith(".vmdk"):
                vmdk_paths.append(path)

    if not vmdk_paths:
        logger.info("vmdk_download.no_disks", extra={"vm_name": vm_name})
        return VmdkDownloadResult(success=True)

    return download_vmdk_from_esxi(
        vm_name=vm_name,
        esxi_host=vmware_session.host,
        esxi_port=vmware_session.port or 443,
        esxi_username=vmware_session.username,
        esxi_password=vmware_session.password,
        datastore_name=discovered_vm.get("datastore_name", "datastore1"),
        vmdk_paths=vmdk_paths,
        job_id=job_id,
        insecure=vmware_session.insecure,
    )


@shared_task(name="migrations.download_vmdk", max_retries=2, default_retry_delay=60, acks_late=True)
def download_vmdk(
    job_id: int,
    vm_name: str,
    esxi_host: str,
    esxi_port: int,
    esxi_username: str,
    esxi_password: str,
    datastore_name: str,
    vmdk_paths: list[str],
    insecure: bool = True,
) -> dict[str, Any]:
    """
    Celery task to download VMDK disks as a parallel side-effect.

    This task runs independently and does NOT block the main migration.
    If download fails, the migration continues normally.
    """
    result = download_vmdk_from_esxi(
        vm_name=vm_name,
        esxi_host=esxi_host,
        esxi_port=esxi_port,
        esxi_username=esxi_username,
        esxi_password=esxi_password,
        datastore_name=datastore_name,
        vmdk_paths=vmdk_paths,
        job_id=job_id,
        insecure=insecure,
    )

    logger.info(
        "vmdk_download.celery_complete",
        extra={
            "job_id": job_id,
            "vm_name": vm_name,
            "success": result.success,
            "downloaded_count": len(result.downloaded_paths),
        },
    )

    return result.to_dict()