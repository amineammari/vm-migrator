from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any


class DiskInspectionError(Exception):
    """Raised when disk inspection/analysis fails."""


def _run(
    cmd: list[str],
    *,
    timeout_seconds: int = 180,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=check,
        timeout=max(1, int(timeout_seconds)),
    )


def _extract_vmdk_paths(disks: list[Any]) -> list[str]:
    paths: list[str] = []
    for disk in disks:
        if isinstance(disk, dict):
            path = disk.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path.strip())
        elif isinstance(disk, str) and disk.strip():
            paths.append(disk.strip())
    return paths


def collect_source_disk_inventory(discovered_vm) -> dict[str, Any]:
    source_paths = _extract_vmdk_paths(discovered_vm.disks if isinstance(discovered_vm.disks, list) else [])
    disk_items: list[dict[str, Any]] = []
    for idx, disk in enumerate(discovered_vm.disks if isinstance(discovered_vm.disks, list) else []):
        if isinstance(disk, dict):
            disk_items.append(
                {
                    "index": idx,
                    "path": disk.get("path"),
                    "size_bytes": disk.get("size_bytes"),
                    "datastore": disk.get("datastore"),
                    "filename": disk.get("filename"),
                    "unit_number": disk.get("unit_number"),
                    "label": disk.get("label"),
                }
            )
        elif isinstance(disk, str):
            disk_items.append({"index": idx, "path": disk, "size_bytes": None})

    return {
        "vm_name": discovered_vm.name,
        "source": discovered_vm.source,
        "disk_count": len(disk_items),
        "disk_paths": source_paths,
        "disks": disk_items,
        "multi_disk": len(disk_items) > 1,
    }


def collect_local_disk_metadata(disk_paths: list[str], timeout_seconds: int = 180) -> dict[str, Any]:
    qemu_img_available = shutil.which("qemu-img") is not None
    virt_filesystems_available = shutil.which("virt-filesystems") is not None
    virt_df_available = shutil.which("virt-df") is not None
    guestfish_available = shutil.which("guestfish") is not None

    per_disk: list[dict[str, Any]] = []
    for path in disk_paths:
        p = Path(path).expanduser()
        exists = p.exists() and p.is_file()
        item: dict[str, Any] = {
            "path": str(p),
            "exists": exists,
            "size_bytes": int(p.stat().st_size) if exists else None,
            "qemu_img_info": None,
            "qemu_img_check": None,
            "partition_layout": None,
            "filesystem_summary": None,
            "guestfish_list_filesystems": None,
        }
        if not exists:
            per_disk.append(item)
            continue

        if qemu_img_available:
            info = _run(["qemu-img", "info", "--output=json", str(p)], timeout_seconds=timeout_seconds)
            item["qemu_img_info"] = {
                "returncode": info.returncode,
                "stdout": (info.stdout or "").strip(),
                "stderr": (info.stderr or "").strip(),
            }

            check = _run(["qemu-img", "check", str(p)], timeout_seconds=timeout_seconds)
            item["qemu_img_check"] = {
                "returncode": check.returncode,
                "stdout": (check.stdout or "").strip(),
                "stderr": (check.stderr or "").strip(),
                "ok": check.returncode == 0,
            }

        if virt_filesystems_available:
            layout = _run(
                ["virt-filesystems", "--long", "--parts", "--blkdevs", "-a", str(p)],
                timeout_seconds=timeout_seconds,
            )
            item["partition_layout"] = {
                "returncode": layout.returncode,
                "stdout": (layout.stdout or "").strip(),
                "stderr": (layout.stderr or "").strip(),
            }

        if virt_df_available:
            usage = _run(["virt-df", "-a", str(p)], timeout_seconds=timeout_seconds)
            item["filesystem_summary"] = {
                "returncode": usage.returncode,
                "stdout": (usage.stdout or "").strip(),
                "stderr": (usage.stderr or "").strip(),
            }

        if guestfish_available:
            fslist = _run(
                ["guestfish", "--ro", "-a", str(p), "run", ":", "list-filesystems"],
                timeout_seconds=timeout_seconds,
            )
            item["guestfish_list_filesystems"] = {
                "returncode": fslist.returncode,
                "stdout": (fslist.stdout or "").strip(),
                "stderr": (fslist.stderr or "").strip(),
            }

        per_disk.append(item)

    return {
        "tools": {
            "qemu_img": qemu_img_available,
            "virt_filesystems": virt_filesystems_available,
            "virt_df": virt_df_available,
            "guestfish": guestfish_available,
        },
        "per_disk": per_disk,
    }


def infer_sparse_candidate(disk_metadata: dict[str, Any]) -> bool:
    per_disk = disk_metadata.get("per_disk", []) if isinstance(disk_metadata, dict) else []
    if not isinstance(per_disk, list):
        return True

    for disk in per_disk:
        summary = disk.get("filesystem_summary") if isinstance(disk, dict) else None
        if not isinstance(summary, dict):
            continue
        text = str(summary.get("stdout", "") or "")
        # If tool output is available, default to sparse to avoid allocating zeroed blocks.
        if text.strip():
            return True
    return True


def concatenate_disk_images(
    *,
    input_paths: list[str],
    output_path: str,
    timeout_seconds: int = 3600,
    sparse: bool = True,
) -> dict[str, Any]:
    if len(input_paths) < 2:
        raise DiskInspectionError("Disk concatenation requires at least two input images.")
    if shutil.which("qemu-img") is None:
        raise DiskInspectionError("qemu-img is required for concatenation workflow.")

    output = Path(output_path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    work_dir = output.parent / f".concat-{output.stem}"
    work_dir.mkdir(parents=True, exist_ok=True)

    raw_parts: list[Path] = []
    mapping: list[dict[str, Any]] = []
    byte_cursor = 0

    try:
        for idx, src_raw in enumerate(input_paths):
            src = Path(src_raw).expanduser()
            if not src.exists() or not src.is_file():
                raise DiskInspectionError(f"Concatenation source not found: {src}")

            raw_part = work_dir / f"part-{idx}.raw"
            convert_cmd = ["qemu-img", "convert", "-O", "raw", str(src), str(raw_part)]
            completed = _run(convert_cmd, timeout_seconds=timeout_seconds)
            if completed.returncode != 0:
                raise DiskInspectionError(
                    f"Failed to convert '{src}' to raw for concatenation: {completed.stderr or completed.stdout}"
                )

            size_bytes = int(raw_part.stat().st_size)
            mapping.append(
                {
                    "source_index": idx,
                    "source_path": str(src),
                    "raw_part_path": str(raw_part),
                    "offset_start": byte_cursor,
                    "offset_end": byte_cursor + size_bytes,
                    "size_bytes": size_bytes,
                }
            )
            byte_cursor += size_bytes
            raw_parts.append(raw_part)

        merged_raw = work_dir / "merged.raw"
        with merged_raw.open("wb") as out:
            for part in raw_parts:
                with part.open("rb") as src_f:
                    shutil.copyfileobj(src_f, out, length=8 * 1024 * 1024)

        convert_final_cmd = ["qemu-img", "convert", "-f", "raw", "-O", "qcow2"]
        if sparse:
            convert_final_cmd += ["-S", "4096"]
        convert_final_cmd += [str(merged_raw), str(output)]
        final = _run(convert_final_cmd, timeout_seconds=timeout_seconds)
        if final.returncode != 0:
            raise DiskInspectionError(
                f"Failed to build final concatenated qcow2 image: {final.stderr or final.stdout}"
            )

        return {
            "output_path": str(output),
            "input_paths": input_paths,
            "mapping": mapping,
            "virtual_size_bytes": byte_cursor,
            "sparse": sparse,
            "work_dir": str(work_dir),
            "status": "concatenated",
        }
    except Exception:
        if output.exists():
            output.unlink(missing_ok=True)
        raise
    finally:
        # Keep only final artifact. Intermediate raws are always disposable.
        shutil.rmtree(work_dir, ignore_errors=True)


def validate_disk_sources_for_precheck(discovered_vm) -> list[str]:
    errors: list[str] = []
    metadata = discovered_vm.metadata if isinstance(discovered_vm.metadata, dict) else {}
    disks = discovered_vm.disks if isinstance(discovered_vm.disks, list) else []
    if not disks:
        errors.append("No source disks attached to VM.")
        return errors

    for idx, disk in enumerate(disks):
        if isinstance(disk, dict):
            size = disk.get("size_bytes")
            if isinstance(size, int) and size <= 0:
                errors.append(f"Disk #{idx} has non-positive size.")
            if discovered_vm.source == "esxi":
                datastore = disk.get("datastore")
                filename = disk.get("filename")
                if not datastore:
                    errors.append(f"Disk #{idx} is missing datastore metadata.")
                if not filename:
                    errors.append(f"Disk #{idx} is missing datastore file metadata.")
        elif isinstance(disk, str):
            p = Path(disk).expanduser()
            if discovered_vm.source == "workstation" and (not p.exists() or not p.is_file()):
                errors.append(f"Disk path not accessible: {p}")

    if discovered_vm.source == "esxi":
        datastores = metadata.get("datastores")
        if not isinstance(datastores, list) or len(datastores) == 0:
            errors.append("ESXi datastore metadata is missing or empty.")

    return errors

