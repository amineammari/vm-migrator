from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class AnsibleRunnerError(Exception):
    """Raised when ansible-playbook execution fails."""


class AnsibleRunner:
    def __init__(self, *, binary: str = "ansible-playbook") -> None:
        self.binary = binary

    def run_playbook(
        self,
        *,
        playbook_path: str,
        inventory_path: str,
        extra_vars: dict[str, Any] | None = None,
        limit: str | None = None,
        timeout_seconds: int = 7200,
    ) -> dict[str, Any]:
        playbook = Path(playbook_path).expanduser().resolve()
        inventory = Path(inventory_path).expanduser().resolve()

        if not playbook.exists():
            raise AnsibleRunnerError(f"Playbook not found: {playbook}")
        if not inventory.exists():
            raise AnsibleRunnerError(f"Inventory not found: {inventory}")

        cmd = [self.binary, "-i", str(inventory), str(playbook)]
        if limit:
            cmd.extend(["--limit", limit])
        if extra_vars:
            cmd.extend(["--extra-vars", json.dumps(extra_vars)])

        logger.info(
            "ansible.run.start",
            extra={
                "playbook": str(playbook),
                "inventory": str(inventory),
                "limit": limit,
            },
        )

        started = time.monotonic()
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds,
            )
        except FileNotFoundError as exc:
            raise AnsibleRunnerError("ansible-playbook binary not found") from exc
        except subprocess.TimeoutExpired as exc:
            raise AnsibleRunnerError(
                f"ansible-playbook timed out after {timeout_seconds}s"
            ) from exc
        except OSError as exc:
            raise AnsibleRunnerError(f"ansible-playbook failed to start: {exc}") from exc

        duration = round(time.monotonic() - started, 3)
        status = "success" if completed.returncode == 0 else "failed"

        result = {
            "status": status,
            "returncode": completed.returncode,
            "duration_seconds": duration,
            "stdout": completed.stdout or "",
            "stderr": completed.stderr or "",
            "command": cmd,
        }

        logger.info(
            "ansible.run.finished",
            extra={
                "status": status,
                "returncode": completed.returncode,
                "duration_seconds": duration,
                "playbook": str(playbook),
            },
        )

        return result
