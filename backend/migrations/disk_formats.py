"""Disk format detection and qemu-img conversion helpers."""

from __future__ import annotations

import shutil
import struct
import subprocess
from pathlib import Path
from typing import Any


class DiskFormatError(Exception):
    """Raised when disk format inspection fails."""


class DiskConversionError(Exception):
    """Raised when qemu-img conversion fails."""

    def __init__(
        self,
        message: str,
        *,
        returncode: int | None = None,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


SUPPORTED_INPUT_FORMATS: dict[str, str] = {
    "vmdk": "vmdk",
    "raw": "raw",
    "qcow2": "qcow2",
    "vhd": "vpc",
    "vhdx": "vhdx",
    "vdi": "vdi",
}

SUPPORTED_OUTPUT_FORMATS = {"vmdk", "qcow2", "raw"}


def detect_disk_format(path: str | Path) -> str:
    """Best-effort disk format detection from file headers."""
    p = Path(path).expanduser()
    if not p.exists() or not p.is_file():
        raise DiskFormatError(f"Disk file not found: {p}")

    try:
        with p.open("rb") as f:
            head = f.read(1024 * 1024)
            size = p.stat().st_size
            tail = b""
            if size >= 512:
                f.seek(size - 512)
                tail = f.read(512)
    except OSError as exc:
        raise DiskFormatError(f"Cannot read disk header for '{p}': {exc}") from exc

    if head.startswith(b"QFI\xfb"):
        return "qcow2"
    if head[:4] == b"KDMV":
        return "vmdk"
    if b"# Disk DescriptorFile" in head[:4096] or b"createType" in head[:4096]:
        return "vmdk"
    if head.startswith(b"vhdxfile"):
        return "vhdx"
    if tail[:8].lower() == b"conectix":
        return "vhd"
    if len(head) >= 68:
        # VirtualBox VDI signature at offset 0x40 (little-endian 0xbeda107f).
        sig = struct.unpack("<I", head[64:68])[0]
        if sig == 0xBEDA107F:
            return "vdi"
    if b"<<< Oracle VM VirtualBox Disk Image >>>" in head[:512]:
        return "vdi"

    # RAW has no fixed magic; if unrecognized, treat as raw and let qemu-img validate.
    return "raw"


def convert_with_qemu_img(
    *,
    source_path: str | Path,
    target_path: str | Path,
    source_format: str,
    target_format: str,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """Convert one disk with qemu-img."""
    if shutil.which("qemu-img") is None:
        raise DiskConversionError("qemu-img not found in PATH.")

    src = Path(source_path).expanduser()
    dst = Path(target_path).expanduser()
    if not src.exists() or not src.is_file():
        raise DiskConversionError(f"Source disk not found: {src}")

    if source_format not in SUPPORTED_INPUT_FORMATS:
        raise DiskConversionError(f"Unsupported source disk format '{source_format}' for {src}.")
    if target_format not in SUPPORTED_OUTPUT_FORMATS:
        raise DiskConversionError(f"Unsupported target disk format '{target_format}'.")

    dst.parent.mkdir(parents=True, exist_ok=True)
    qemu_source_format = SUPPORTED_INPUT_FORMATS[source_format]

    cmd = [
        "qemu-img",
        "convert",
        "-f",
        qemu_source_format,
        "-O",
        target_format,
        str(src),
        str(dst),
    ]

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(1, int(timeout_seconds)),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DiskConversionError(f"qemu-img conversion failed for {src}: {exc}") from exc

    if completed.returncode != 0:
        raise DiskConversionError(
            f"qemu-img conversion failed for {src} -> {dst} (exit={completed.returncode}).",
            returncode=completed.returncode,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )

    size_bytes = 0
    try:
        size_bytes = int(dst.stat().st_size)
    except OSError:
        size_bytes = 0

    return {
        "source_path": str(src),
        "target_path": str(dst),
        "source_format": source_format,
        "target_format": target_format,
        "command": " ".join(cmd),
        "size_bytes": size_bytes,
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
    }


def convert_to_vmware_compatible(
    *,
    source_path: str | Path,
    source_format: str,
    target_path: str | Path,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """Convert a non-VMDK disk to VMDK for VMware-compatible workflows."""
    return convert_with_qemu_img(
        source_path=source_path,
        target_path=target_path,
        source_format=source_format,
        target_format="vmdk",
        timeout_seconds=timeout_seconds,
    )


def convert_to_openstack_compatible(
    *,
    source_path: str | Path,
    source_format: str,
    target_path: str | Path,
    target_format: str = "qcow2",
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """Convert source disk to OpenStack ingest format (qcow2/raw)."""
    if target_format not in {"qcow2", "raw"}:
        raise DiskConversionError(f"Unsupported OpenStack target format '{target_format}'.")
    return convert_with_qemu_img(
        source_path=source_path,
        target_path=target_path,
        source_format=source_format,
        target_format=target_format,
        timeout_seconds=timeout_seconds,
    )
