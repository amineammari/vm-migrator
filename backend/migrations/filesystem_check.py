from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any


class FilesystemCheckError(Exception):
    """Raised when filesystem consistency checks fail."""


def _run(cmd: list[str], timeout_seconds: int = 240) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=max(1, int(timeout_seconds)),
    )


def collect_partition_layout(image_path: str, timeout_seconds: int = 240) -> dict[str, Any]:
    if shutil.which("virt-filesystems") is None:
        return {"available": False, "stdout": "", "stderr": "virt-filesystems not installed", "returncode": None}
    result = _run(
        ["virt-filesystems", "--long", "--parts", "--blkdevs", "--filesystems", "-a", image_path],
        timeout_seconds=timeout_seconds,
    )
    return {
        "available": True,
        "returncode": result.returncode,
        "stdout": (result.stdout or "").strip(),
        "stderr": (result.stderr or "").strip(),
    }


def _extract_filesystems(image_path: str, timeout_seconds: int = 180) -> dict[str, str]:
    if shutil.which("guestfish") is None:
        return {}
    result = _run(
        ["guestfish", "--ro", "-a", image_path, "run", ":", "list-filesystems"],
        timeout_seconds=timeout_seconds,
    )
    if result.returncode != 0:
        return {}

    filesystems: dict[str, str] = {}
    for line in (result.stdout or "").splitlines():
        line = line.strip()
        if ":" not in line:
            continue
        dev, fstype = line.split(":", 1)
        dev = dev.strip()
        fstype = fstype.strip()
        if dev and fstype:
            filesystems[dev] = fstype
    return filesystems


def run_filesystem_consistency_check(image_paths: list[str], timeout_seconds: int = 300) -> dict[str, Any]:
    fsck_available = shutil.which("fsck") is not None
    guestfish_available = shutil.which("guestfish") is not None

    checks: list[dict[str, Any]] = []
    for raw in image_paths:
        path = Path(raw).expanduser()
        if not path.exists() or not path.is_file():
            raise FilesystemCheckError(f"Image not found for filesystem check: {path}")

        partition_layout = collect_partition_layout(str(path), timeout_seconds=timeout_seconds)
        fs_map = _extract_filesystems(str(path), timeout_seconds=timeout_seconds)

        fsck_runs: list[dict[str, Any]] = []
        if guestfish_available and fs_map:
            for dev, fstype in fs_map.items():
                cmd = ["guestfish", "-a", str(path), "run", ":", "fsck", fstype, dev]
                run = _run(cmd, timeout_seconds=timeout_seconds)
                fsck_runs.append(
                    {
                        "device": dev,
                        "filesystem": fstype,
                        "command": " ".join(cmd),
                        "returncode": run.returncode,
                        "stdout": (run.stdout or "").strip(),
                        "stderr": (run.stderr or "").strip(),
                        "ok": run.returncode == 0,
                    }
                )
        elif fsck_available:
            cmd = ["fsck", "-N", str(path)]
            run = _run(cmd, timeout_seconds=timeout_seconds)
            fsck_runs.append(
                {
                    "device": str(path),
                    "filesystem": "unknown",
                    "command": " ".join(cmd),
                    "returncode": run.returncode,
                    "stdout": (run.stdout or "").strip(),
                    "stderr": (run.stderr or "").strip(),
                    "ok": run.returncode == 0,
                    "note": "fallback dry-run because guestfish filesystem mapping was unavailable",
                }
            )

        checks.append(
            {
                "path": str(path),
                "partition_layout": partition_layout,
                "filesystems": fs_map,
                "fsck_runs": fsck_runs,
                "ok": all(item.get("ok", False) for item in fsck_runs) if fsck_runs else True,
            }
        )

    ok = all(item.get("ok", False) for item in checks)
    return {
        "checks": checks,
        "ok": ok,
        "tools": {
            "guestfish": guestfish_available,
            "fsck": fsck_available,
        },
    }


def compare_partition_layout(source_layout: str, target_layout: str) -> dict[str, Any]:
    source_lines = [line.strip() for line in (source_layout or "").splitlines() if line.strip()]
    target_lines = [line.strip() for line in (target_layout or "").splitlines() if line.strip()]
    return {
        "source_entries": len(source_lines),
        "target_entries": len(target_lines),
        "matches": source_lines == target_lines,
    }

