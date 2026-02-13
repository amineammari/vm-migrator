from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class TerraformRunnerError(Exception):
    """Raised when terraform execution fails."""


class TerraformRunner:
    def __init__(self, *, binary: str = "terraform") -> None:
        self.binary = binary

    def apply(
        self,
        *,
        working_dir: str,
        var_overrides: dict[str, Any] | None = None,
        timeout_seconds: int = 1800,
        auto_approve: bool = True,
    ) -> dict[str, Any]:
        workdir = Path(working_dir).expanduser().resolve()
        if not workdir.exists() or not workdir.is_dir():
            raise TerraformRunnerError(f"Terraform working directory not found: {workdir}")

        init_result = self._run(
            [self.binary, "init", "-input=false"],
            cwd=workdir,
            timeout_seconds=timeout_seconds,
            step="init",
        )
        if init_result["returncode"] != 0:
            raise TerraformRunnerError(f"terraform init failed: {init_result['stderr']}")

        apply_cmd = [self.binary, "apply", "-input=false"]
        if auto_approve:
            apply_cmd.append("-auto-approve")

        if var_overrides:
            for key, value in var_overrides.items():
                apply_cmd.extend(["-var", f"{key}={value}"])

        apply_result = self._run(
            apply_cmd,
            cwd=workdir,
            timeout_seconds=timeout_seconds,
            step="apply",
        )
        if apply_result["returncode"] != 0:
            raise TerraformRunnerError(f"terraform apply failed: {apply_result['stderr']}")

        output_json: dict[str, Any] = {}
        output_result = self._run(
            [self.binary, "output", "-json"],
            cwd=workdir,
            timeout_seconds=300,
            step="output",
        )
        if output_result["returncode"] == 0 and output_result["stdout"].strip():
            try:
                output_json = json.loads(output_result["stdout"])
            except json.JSONDecodeError:
                output_json = {}

        return {
            "status": "success",
            "init": init_result,
            "apply": apply_result,
            "outputs": output_json,
        }

    def _run(
        self,
        cmd: list[str],
        *,
        cwd: Path,
        timeout_seconds: int,
        step: str,
    ) -> dict[str, Any]:
        logger.info("terraform.run.start", extra={"step": step, "cwd": str(cwd), "command": cmd})
        started = time.monotonic()

        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_seconds,
                cwd=str(cwd),
            )
        except FileNotFoundError as exc:
            raise TerraformRunnerError("terraform binary not found") from exc
        except subprocess.TimeoutExpired as exc:
            raise TerraformRunnerError(f"terraform {step} timed out after {timeout_seconds}s") from exc
        except OSError as exc:
            raise TerraformRunnerError(f"terraform {step} failed to start: {exc}") from exc

        duration = round(time.monotonic() - started, 3)
        result = {
            "step": step,
            "returncode": completed.returncode,
            "duration_seconds": duration,
            "stdout": completed.stdout or "",
            "stderr": completed.stderr or "",
            "command": cmd,
        }

        logger.info(
            "terraform.run.finished",
            extra={
                "step": step,
                "returncode": completed.returncode,
                "duration_seconds": duration,
            },
        )
        return result
