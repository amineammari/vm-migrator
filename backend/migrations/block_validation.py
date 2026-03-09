from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any


class BlockValidationError(Exception):
    """Raised when block-level image validation cannot be completed."""


def _run_qemu_img_check(path: Path, timeout_seconds: int) -> dict[str, Any]:
    cmd = ["qemu-img", "check", str(path)]
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        timeout=max(1, int(timeout_seconds)),
    )
    return {
        "path": str(path),
        "command": " ".join(cmd),
        "returncode": completed.returncode,
        "stdout": (completed.stdout or "").strip(),
        "stderr": (completed.stderr or "").strip(),
        "ok": completed.returncode == 0,
    }


def validate_qcow2_images(paths: list[str], timeout_seconds: int = 600) -> dict[str, Any]:
    if shutil.which("qemu-img") is None:
        raise BlockValidationError("qemu-img is not available in PATH.")

    checks: list[dict[str, Any]] = []
    for raw in paths:
        image = Path(raw).expanduser()
        if not image.exists() or not image.is_file():
            raise BlockValidationError(f"Converted image does not exist: {image}")
        checks.append(_run_qemu_img_check(image, timeout_seconds))

    failed = [item for item in checks if not item.get("ok")]
    return {
        "tool": "qemu-img",
        "checks": checks,
        "failed": failed,
        "ok": len(failed) == 0,
    }

