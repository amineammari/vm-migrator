from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import quote

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .ansible_runner import AnsibleRunner, AnsibleRunnerError
from .conversion import ConversionPlanningError, ConversionPlan, plan_vmware_conversion
from .disk_formats import DiskConversionError, convert_to_openstack_compatible, detect_disk_format
from .models import (
    DiscoveredVM,
    InvalidTransitionError,
    MigrationJob,
    OpenstackEndpointSession,
    VmwareEndpointSession,
)
from .openstack_deployment import (
    OpenStackDeploymentError,
    attach_volume_to_server,
    build_openstack_names,
    connect_openstack,
    delete_image_if_exists,
    delete_server_if_exists,
    delete_volume_if_exists,
    ensure_server_booted_from_volume,
    ensure_empty_volume,
    ensure_uploaded_image,
    ensure_volume_from_image,
    get_flavor_choice_by_id,
    map_vmware_to_flavor,
    select_default_network,
    verify_server_active,
)
from .terraform_runner import TerraformRunner, TerraformRunnerError
from .vmware_client import ESXiVMwareClient, VMwareClientError, WorkstationVMwareClient

logger = logging.getLogger(__name__)


class ConversionExecutionError(Exception):
    """Raised when real virt-v2v execution fails."""

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


@shared_task(name="migrations.celery_ping")
def celery_ping():
    return {"status": "ok", "message": "celery task executed"}


def _truncate_log(text: str, limit: int = 12000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]"


def _sanitize_name(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]", "-", value).strip("-._")
    return clean or "vm"


def _find_discovered_vm_for_job(job: MigrationJob) -> DiscoveredVM:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    selected_source = metadata.get("selected_source")
    vmware_endpoint_session_id = metadata.get("selected_vmware_endpoint_session_id")

    qs = DiscoveredVM.objects.filter(name=job.vm_name)
    if selected_source:
        qs = qs.filter(source=selected_source)
    if isinstance(vmware_endpoint_session_id, int):
        qs = qs.filter(vmware_endpoint_session_id=vmware_endpoint_session_id)

    count = qs.count()
    if count == 0:
        raise ConversionPlanningError(
            f"No DiscoveredVM found for vm_name='{job.vm_name}' source='{selected_source}'."
        )
    if count > 1:
        sources = list(qs.values_list("source", flat=True))
        raise ConversionPlanningError(
            f"Ambiguous DiscoveredVM reference for vm_name='{job.vm_name}'. Matches sources={sources}."
        )
    return qs.first()


def _validate_workstation_paths(input_disks: list[str], output_path: str) -> dict[str, Any]:
    errors: list[str] = []
    checked: list[dict[str, Any]] = []
    total_input_size = 0

    for disk in input_disks:
        disk_path = Path(disk).expanduser()
        exists = disk_path.exists()
        readable = os.access(disk_path, os.R_OK) if exists else False
        size_bytes = None

        if exists:
            try:
                size_bytes = disk_path.stat().st_size
                total_input_size += size_bytes
            except OSError:
                size_bytes = None

        checked.append(
            {
                "path": str(disk_path),
                "exists": exists,
                "readable": readable,
                "size_bytes": size_bytes,
            }
        )

        if not exists:
            errors.append(f"Missing disk path: {disk_path}")
        elif not readable:
            errors.append(f"Disk path is not readable: {disk_path}")

    output_dir = Path(output_path).expanduser().parent
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        errors.append(f"Output directory permission error: {output_dir} ({exc})")

    if output_dir.exists():
        if not os.access(output_dir, os.W_OK):
            errors.append(f"Output directory is not writable: {output_dir}")
        else:
            usage = shutil.disk_usage(output_dir)
            required = int(total_input_size * 1.15) if total_input_size else 0
            if required and usage.free < required:
                errors.append(
                    "Insufficient disk space in output directory: "
                    f"free={usage.free} required~={required}"
                )

    return {
        "checked_paths": checked,
        "output_dir": str(output_dir),
        "total_input_size_bytes": total_input_size,
        "errors": errors,
    }


def _ensure_libguestfs_kernel_readable() -> None:
    """Fail fast if libguestfs/supermin cannot read the host kernel image.

    On some hardened installs, `/boot/vmlinuz-*` is mode 0600 (root-only) which
    causes supermin to fail and virt-v2v to exit early.
    """
    release = os.uname().release
    kernel = Path("/boot") / f"vmlinuz-{release}"
    if kernel.exists() and not os.access(kernel, os.R_OK):
        raise ConversionPlanningError(
            f"libguestfs cannot read host kernel image: {kernel}. "
            "Fix permissions (example): "
            f"sudo chmod 0644 {kernel}"
        )


def _build_esxi_libvirt_uri() -> str:
    host = os.getenv("VMWARE_ESXI_HOST", "").strip()
    username = os.getenv("VMWARE_ESXI_USERNAME", "").strip()
    insecure = os.getenv("VMWARE_ESXI_INSECURE", "true").lower() in {"1", "true", "yes", "on"}
    return _build_esxi_libvirt_uri_with_values(host=host, username=username, insecure=insecure)


def _build_esxi_libvirt_uri_with_values(*, host: str, username: str, insecure: bool) -> str:
    if not host or not username:
        raise ConversionPlanningError("VMWARE_ESXI_HOST and VMWARE_ESXI_USERNAME are required for ESXi conversion.")

    # Avoid leaking any special characters in the username; URI component should be encoded.
    user_enc = quote(username, safe="")
    uri = f"esx://{user_enc}@{host}"
    if insecure:
        uri += "?no_verify=1"
    return uri


def _write_password_file(tmp_dir: Path, password: str) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    passfile = tmp_dir / "esxi.password"
    passfile.write_text(password, encoding="utf-8")
    os.chmod(passfile, 0o600)
    return passfile


def _normalize_disk_artifact_path(p: Path) -> Path:
    if p.suffix != "":
        return p

    renamed = p.with_name(p.name + ".qcow2")
    if not renamed.exists():
        try:
            p.rename(renamed)
            return renamed
        except OSError:
            return p
    return renamed


def _find_output_qcow2_paths(output_path: str, vm_name: str) -> list[Path]:
    expected = Path(output_path)
    output_dir = expected.parent
    if not output_dir.exists():
        raise ConversionExecutionError(f"Output directory not found after conversion: {output_dir}")

    candidates: list[Path] = []

    if expected.exists() and expected.is_file():
        candidates.append(expected)

    for pattern in [f"{vm_name}*.qcow2", f"{vm_name}-sd*", f"{vm_name}*"]:
        for p in output_dir.glob(pattern):
            if not p.is_file():
                continue
            if p.suffix.lower() == ".xml":
                continue
            candidates.append(p)

    normalized = [_normalize_disk_artifact_path(p) for p in candidates]
    unique = sorted({str(p): p for p in normalized}.values(), key=lambda x: x.name)
    if unique:
        return unique

    raise ConversionExecutionError(
        f"No QCOW2 output found in {output_dir} for VM '{vm_name}' after conversion."
    )


def _select_primary_disk(paths: list[Path], vm_name: str) -> Path:
    if not paths:
        raise ConversionExecutionError(f"No conversion artifacts found for VM '{vm_name}'.")

    for p in paths:
        if p.name.endswith("-sda") or p.name.endswith("-sda.qcow2"):
            return p
    for p in paths:
        if p.name == f"{vm_name}.qcow2":
            return p
    return paths[0]


def _inspect_disk_for_system_filesystem(path: Path) -> dict[str, Any]:
    """Best-effort OS inspection for a converted disk image.

    Uses virt-inspector when available to detect a root filesystem and score
    likely system disks. Returns score=0 when undetermined.
    """
    result: dict[str, Any] = {
        "path": str(path),
        "tool": "virt-inspector",
        "available": bool(shutil.which("virt-inspector")),
        "score": 0,
        "has_operating_system": False,
        "has_root_mount": False,
        "has_boot_mount": False,
        "mountpoints": [],
        "os_names": [],
        "error": "",
    }
    if not result["available"]:
        return result

    try:
        completed = subprocess.run(
            ["virt-inspector", "-a", str(path)],
            capture_output=True,
            text=True,
            check=False,
            timeout=int(getattr(settings, "DISK_INSPECT_TIMEOUT_SECONDS", 90)),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        result["error"] = str(exc)
        return result

    if completed.returncode != 0:
        result["error"] = (completed.stderr or completed.stdout or "").strip()[:500]
        return result

    try:
        root = ET.fromstring(completed.stdout or "")
    except ET.ParseError as exc:
        result["error"] = f"invalid XML: {exc}"
        return result

    os_nodes = root.findall(".//operatingsystem")
    if not os_nodes:
        return result

    best_score = 0
    best_mountpoints: list[str] = []
    os_names: list[str] = []
    has_root_mount = False
    has_boot_mount = False

    for os_node in os_nodes:
        os_name = os_node.findtext("name")
        if isinstance(os_name, str) and os_name.strip():
            os_names.append(os_name.strip())

        mountpoints = [
            (mp.text or "").strip()
            for mp in os_node.findall(".//mountpoint")
            if isinstance(mp.text, str) and mp.text.strip()
        ]
        local_has_root = "/" in mountpoints
        local_has_boot = "/boot" in mountpoints or "/boot/efi" in mountpoints
        local_score = 40
        if local_has_root:
            local_score += 80
        if local_has_boot:
            local_score += 20
        if os_name:
            local_score += 5

        if local_score > best_score:
            best_score = local_score
            best_mountpoints = mountpoints

        has_root_mount = has_root_mount or local_has_root
        has_boot_mount = has_boot_mount or local_has_boot

    result["score"] = best_score
    result["has_operating_system"] = True
    result["has_root_mount"] = has_root_mount
    result["has_boot_mount"] = has_boot_mount
    result["mountpoints"] = sorted(set(best_mountpoints))
    result["os_names"] = sorted(set(os_names))
    return result


def _order_qcow2_paths_for_boot(paths: list[Path], vm_name: str) -> tuple[list[Path], Path, int, list[dict[str, Any]]]:
    """Detect likely boot/system disk while preserving original disk order."""
    if not paths:
        raise ConversionExecutionError(f"No conversion artifacts found for VM '{vm_name}'.")
    if len(paths) == 1:
        primary = paths[0]
        analysis = [{
            "path": str(primary),
            "score": 1,
            "index": 0,
            "selected_as_primary": True,
            "reason": "single_disk",
        }]
        return paths, primary, 0, analysis

    heuristic_primary = _select_primary_disk(paths, vm_name)
    inspected: list[dict[str, Any]] = []
    for idx, p in enumerate(paths):
        inspect = _inspect_disk_for_system_filesystem(p)
        filename_score = 0
        if p.name.endswith("-sda") or p.name.endswith("-sda.qcow2"):
            filename_score = 10
        elif p == heuristic_primary:
            filename_score = 5

        try:
            size_bytes = int(p.stat().st_size)
        except OSError:
            size_bytes = 0

        total_score = int(inspect.get("score", 0)) + filename_score
        inspected.append(
            {
                **inspect,
                "filename_score": filename_score,
                "score": total_score,
                "index": idx,
                "size_bytes": size_bytes,
                "selected_as_primary": False,
            }
        )

    selected = max(
        inspected,
        key=lambda x: (
            int(x.get("score", 0)),
            int(x.get("size_bytes", 0)),
            str(x.get("path", "")),
        ),
    )

    # If nothing clearly indicates an OS/filesystem disk, keep legacy heuristic.
    primary_index = int(selected.get("index", 0))
    if int(selected.get("score", 0)) <= 0:
        primary = heuristic_primary
        for idx, candidate in enumerate(paths):
            if str(candidate) == str(primary):
                primary_index = idx
                break
    else:
        primary = Path(str(selected["path"]))

    for item in inspected:
        if str(item.get("path")) == str(primary):
            item["selected_as_primary"] = True
            break

    return paths, primary, primary_index, inspected


def _execute_workstation_qemu_pipeline(plan: ConversionPlan, vm_name: str) -> dict[str, Any]:
    """Convert workstation-exported disks with qemu-img in strict 1-to-1 mode."""
    start = time.monotonic()
    target_format = str(getattr(settings, "OPENSTACK_OUTPUT_DISK_FORMAT", "qcow2")).strip().lower() or "qcow2"
    if target_format not in {"qcow2", "raw"}:
        raise ConversionExecutionError(
            f"Unsupported OPENSTACK_OUTPUT_DISK_FORMAT='{target_format}'. Allowed: qcow2, raw."
        )

    input_disks = [str(p).strip() for p in plan.input_disks if isinstance(p, str) and str(p).strip()]
    if not input_disks:
        raise ConversionExecutionError(f"No source disks found for workstation VM '{vm_name}'.")

    output_dir = Path(plan.output_path).expanduser().parent
    output_dir.mkdir(parents=True, exist_ok=True)

    conversion_steps: list[dict[str, Any]] = []
    output_paths: list[Path] = []

    for idx, source in enumerate(input_disks):
        src_path = Path(source).expanduser()
        detected = detect_disk_format(src_path)
        if detected not in {"vmdk", "raw", "vhd", "vhdx", "vdi", "qcow2"}:
            raise ConversionExecutionError(
                f"Unsupported source disk format '{detected}' for disk '{src_path}'. "
                "Disk architecture must remain unchanged (1-to-1, no merge)."
            )

        out_name = f"{_sanitize_name(vm_name)}-disk{idx}.{target_format}"
        out_path = output_dir / out_name
        try:
            step = convert_to_openstack_compatible(
                source_path=src_path,
                target_path=out_path,
                source_format=detected,
                target_format=target_format,
                timeout_seconds=int(getattr(settings, "QEMU_IMG_TIMEOUT_SECONDS", 3600)),
            )
            step["disk_index"] = idx
            step["status"] = "converted"
            conversion_steps.append(step)
            output_paths.append(out_path)
            logger.info(
                "migration.disk.converted",
                extra={
                    "vm_name": vm_name,
                    "disk_index": idx,
                    "source": str(src_path),
                    "source_format": detected,
                    "target": str(out_path),
                    "target_format": target_format,
                },
            )
        except DiskConversionError as exc:
            logger.error(
                "migration.disk.conversion_failed",
                extra={
                    "vm_name": vm_name,
                    "disk_index": idx,
                    "source": str(src_path),
                    "source_format": detected,
                    "error": str(exc),
                },
            )
            conversion_steps.append(
                {
                    "disk_index": idx,
                    "source_path": str(src_path),
                    "source_format": detected,
                    "target_format": target_format,
                    "status": "failed",
                    "error": str(exc),
                    "stdout": getattr(exc, "stdout", ""),
                    "stderr": getattr(exc, "stderr", ""),
                }
            )
            raise ConversionExecutionError(
                f"Unsupported or failed disk conversion for '{src_path}' ({detected}): {exc}"
            ) from exc

    if len(output_paths) != len(input_disks):
        raise ConversionExecutionError(
            "Disk conversion count mismatch. Disk architecture must remain unchanged "
            f"(source={len(input_disks)}, output={len(output_paths)})."
        )

    disk_sizes: dict[str, int] = {}
    for p in output_paths:
        try:
            disk_sizes[str(p)] = int(p.stat().st_size)
        except OSError:
            disk_sizes[str(p)] = 0

    duration = round(time.monotonic() - start, 3)
    output_strings = [str(p) for p in output_paths]
    return {
        "returncode": 0,
        "runner": "qemu-img",
        "duration_seconds": duration,
        "stdout": "",
        "stderr": "",
        "output_qcow2_path": output_strings[0],
        "output_qcow2_paths": output_strings,
        "primary_disk_index": 0,
        "disk_analysis": conversion_steps,
        "disk_size": disk_sizes.get(output_strings[0], 0),
        "disk_sizes": disk_sizes,
        "disk_count": len(output_paths),
        "output_disk_format": target_format,
    }


def _execute_virt_v2v(plan: ConversionPlan, vm_name: str) -> dict[str, Any]:
    start = time.monotonic()

    run_env = os.environ.copy()
    # If using VDDK transport, ensure nbdkit can locate the vddk plugin and VDDK libs.
    transport = os.getenv("VMWARE_ESXI_CONVERSION_TRANSPORT", "").strip().lower()
    if transport == "vddk":
        # Ensure virt-v2v finds the intended nbdkit binary (it executes `nbdkit` via PATH).
        # Prefer explicit binary, otherwise default to ~/.local/bin when present.
        nbdkit_bin = os.getenv("VMWARE_NBDKIT_BIN", "").strip()
        nbdkit_dir = None
        if nbdkit_bin:
            try:
                nbdkit_dir = str(Path(nbdkit_bin).expanduser().resolve().parent)
            except OSError:
                nbdkit_dir = None
        else:
            candidate = Path.home() / ".local" / "bin" / "nbdkit"
            if candidate.exists():
                nbdkit_dir = str(candidate.parent)

        if nbdkit_dir:
            existing_path = run_env.get("PATH", "")
            run_env["PATH"] = f"{nbdkit_dir}:{existing_path}" if existing_path else nbdkit_dir

        plugin_path = os.getenv("VMWARE_VDDK_NBDKIT_PLUGIN_PATH", "").strip()
        if plugin_path:
            run_env["NBDKIT_PLUGIN_PATH"] = plugin_path

        # virt-v2v uses nbdkit filters like "cow". Ensure nbdkit can find them.
        filter_path = os.getenv("VMWARE_NBDKIT_FILTER_PATH", "").strip()
        if filter_path:
            run_env["NBDKIT_FILTER_PATH"] = filter_path

        vddk_libdir = os.getenv("VMWARE_VDDK_LIBDIR", "").strip()
        if vddk_libdir:
            lib64 = str(Path(vddk_libdir).expanduser() / "lib64")
            existing = run_env.get("LD_LIBRARY_PATH", "")
            run_env["LD_LIBRARY_PATH"] = f"{lib64}:{existing}" if existing else lib64

    try:
        completed = subprocess.run(
            plan.command_args,
            capture_output=True,
            text=True,
            check=False,
            timeout=int(getattr(settings, "VIRT_V2V_TIMEOUT_SECONDS", 7200)),
            env=run_env,
        )
    except PermissionError as exc:
        raise ConversionExecutionError(f"Permission error executing virt-v2v: {exc}") from exc
    except FileNotFoundError as exc:
        raise ConversionExecutionError("virt-v2v command not found. Is virt-v2v installed?") from exc
    except subprocess.TimeoutExpired as exc:
        raise ConversionExecutionError(
            f"virt-v2v timed out after {getattr(settings, 'VIRT_V2V_TIMEOUT_SECONDS', 7200)}s",
            stdout=exc.stdout or "",
            stderr=exc.stderr or "",
        ) from exc
    except OSError as exc:
        raise ConversionExecutionError(f"OS error executing virt-v2v: {exc}") from exc

    duration = round(time.monotonic() - start, 3)
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""

    if completed.returncode != 0:
        raise ConversionExecutionError(
            f"virt-v2v failed with exit code {completed.returncode}",
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    try:
        qcow2_paths = _find_output_qcow2_paths(plan.output_path, vm_name)
        qcow2_paths, primary_qcow2_path, primary_disk_index, disk_analysis = _order_qcow2_paths_for_boot(qcow2_paths, vm_name)
    except ConversionExecutionError as exc:
        # Preserve virt-v2v logs even when artifact detection fails.
        raise ConversionExecutionError(str(exc), stdout=stdout, stderr=stderr) from exc

    disk_sizes: dict[str, int] = {}
    for p in qcow2_paths:
        try:
            disk_sizes[str(p)] = int(p.stat().st_size)
        except OSError:
            disk_sizes[str(p)] = 0

    return {
        "returncode": completed.returncode,
        "duration_seconds": duration,
        "stdout": _truncate_log(stdout),
        "stderr": _truncate_log(stderr),
        "output_qcow2_path": str(primary_qcow2_path),
        "output_qcow2_paths": [str(p) for p in qcow2_paths],
        "primary_disk_index": primary_disk_index,
        "disk_analysis": disk_analysis,
        "disk_size": disk_sizes.get(str(primary_qcow2_path), 0),
        "disk_sizes": disk_sizes,
        "disk_count": len(qcow2_paths),
        "output_disk_format": "qcow2",
    }


def _execute_ansible_conversion(plan: ConversionPlan, vm_name: str) -> dict[str, Any]:
    runner = AnsibleRunner(binary=getattr(settings, "ANSIBLE_BIN", "ansible-playbook"))
    metadata_vars: dict[str, Any] = {
        "vm_name": vm_name,
        "output_dir": str(Path(plan.output_path).expanduser().parent),
        "virt_v2v_command": plan.command,
    }

    result = runner.run_playbook(
        playbook_path=getattr(settings, "ANSIBLE_PLAYBOOK_PATH"),
        inventory_path=getattr(settings, "ANSIBLE_INVENTORY_PATH"),
        extra_vars=metadata_vars,
        limit=(getattr(settings, "ANSIBLE_LIMIT", "") or None),
        timeout_seconds=int(getattr(settings, "ANSIBLE_TIMEOUT_SECONDS", 7200)),
    )
    if result["status"] != "success":
        raise ConversionExecutionError(
            f"Ansible conversion failed with exit code {result.get('returncode')}",
            returncode=result.get("returncode"),
            stdout=result.get("stdout", ""),
            stderr=result.get("stderr", ""),
        )

    try:
        qcow2_paths = _find_output_qcow2_paths(plan.output_path, vm_name)
        qcow2_paths, primary_qcow2_path, primary_disk_index, disk_analysis = _order_qcow2_paths_for_boot(qcow2_paths, vm_name)
    except ConversionExecutionError as exc:
        raise ConversionExecutionError(
            f"Ansible conversion completed but artifacts are unavailable: {exc}",
            stdout=result.get("stdout", ""),
            stderr=result.get("stderr", ""),
        ) from exc

    disk_sizes: dict[str, int] = {}
    for p in qcow2_paths:
        try:
            disk_sizes[str(p)] = int(p.stat().st_size)
        except OSError:
            disk_sizes[str(p)] = 0

    return {
        "returncode": result.get("returncode", 0),
        "duration_seconds": result.get("duration_seconds", 0),
        "stdout": _truncate_log(result.get("stdout", "")),
        "stderr": _truncate_log(result.get("stderr", "")),
        "output_qcow2_path": str(primary_qcow2_path),
        "output_qcow2_paths": [str(p) for p in qcow2_paths],
        "primary_disk_index": primary_disk_index,
        "disk_analysis": disk_analysis,
        "disk_size": disk_sizes.get(str(primary_qcow2_path), 0),
        "disk_sizes": disk_sizes,
        "disk_count": len(qcow2_paths),
        "runner": "ansible",
        "output_disk_format": "qcow2",
    }


def _mark_job_failed(job: MigrationJob, error_message: str) -> None:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    metadata["last_error"] = error_message
    job.conversion_metadata = metadata

    if job.status != MigrationJob.Status.FAILED and job.can_transition_to(MigrationJob.Status.FAILED):
        job.transition(MigrationJob.Status.FAILED)
    else:
        job.status = MigrationJob.Status.FAILED

    job.save(update_fields=["status", "conversion_metadata", "updated_at"])


def _schedule_rollback(job: MigrationJob, reason: str, extra_context: dict[str, Any] | None = None) -> None:
    if not getattr(settings, "ENABLE_ROLLBACK", True):
        logger.info(
            "migration.rollback disabled",
            extra={"job_id": job.id, "vm_name": job.vm_name, "reason": reason},
        )
        return

    context: dict[str, Any] = {"rollback_reason": reason}
    if extra_context:
        context.update(extra_context)

    rollback_migration.delay(job.id, context=context)


def _collect_cleanup_targets(job: MigrationJob, context: dict[str, Any] | None) -> tuple[list[Path], list[Path]]:
    context = context or {}
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    conversion = metadata.get("conversion", {}) if isinstance(metadata.get("conversion"), dict) else {}
    execution = conversion.get("execution", {}) if isinstance(conversion.get("execution"), dict) else {}

    file_candidates: list[str] = []
    dir_candidates: list[str] = []

    # Never delete backup artifacts during rollback.
    exclude_files = set()
    if isinstance(conversion.get("backup"), dict):
        backup_path = conversion["backup"].get("path")
        if isinstance(backup_path, str) and backup_path.strip():
            exclude_files.add(str(Path(backup_path).expanduser()))
        backup_paths = conversion["backup"].get("paths")
        if isinstance(backup_paths, list):
            for backup_item in backup_paths:
                if isinstance(backup_item, str) and backup_item.strip():
                    exclude_files.add(str(Path(backup_item).expanduser()))

    for candidate in [
        execution.get("output_qcow2_path"),
        conversion.get("output_path"),
        context.get("output_qcow2_path"),
    ]:
        if isinstance(candidate, str) and candidate.strip():
            p = str(Path(candidate.strip()).expanduser())
            if p not in exclude_files:
                file_candidates.append(candidate.strip())

    if isinstance(execution.get("output_qcow2_paths"), list):
        for candidate in execution.get("output_qcow2_paths", []):
            if isinstance(candidate, str) and candidate.strip():
                p = str(Path(candidate.strip()).expanduser())
                if p not in exclude_files:
                    file_candidates.append(candidate.strip())

    for candidate in context.get("temp_dirs", []):
        if isinstance(candidate, str) and candidate.strip():
            dir_candidates.append(candidate.strip())

    if isinstance(conversion.get("temp_dirs"), list):
        for candidate in conversion.get("temp_dirs", []):
            if isinstance(candidate, str) and candidate.strip():
                dir_candidates.append(candidate.strip())

    files: list[Path] = []
    seen_files = set()
    for candidate in file_candidates:
        p = Path(candidate).expanduser()
        if str(p) not in seen_files:
            files.append(p)
            seen_files.add(str(p))

    dirs: list[Path] = []
    seen_dirs = set()
    for candidate in dir_candidates:
        p = Path(candidate).expanduser()
        if str(p) not in seen_dirs:
            dirs.append(p)
            seen_dirs.add(str(p))

    return files, dirs


def _rollback_openstack_resources(job: MigrationJob, actions: list[dict[str, Any]]) -> None:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    os_meta = metadata.get("openstack", {}) if isinstance(metadata.get("openstack"), dict) else {}

    server_id = os_meta.get("server_id")
    image_ids: list[str] = []
    if isinstance(os_meta.get("image_ids"), list):
        image_ids.extend([str(v) for v in os_meta.get("image_ids") if isinstance(v, str) and v.strip()])
    legacy_image_id = os_meta.get("image_id")
    if isinstance(legacy_image_id, str) and legacy_image_id.strip():
        image_ids.append(legacy_image_id.strip())
    image_ids = list(dict.fromkeys(image_ids))

    volume_ids: list[str] = []
    if isinstance(os_meta.get("volume_ids"), list):
        volume_ids.extend([str(v) for v in os_meta.get("volume_ids") if isinstance(v, str) and v.strip()])
    if isinstance(os_meta.get("extra_volume_ids"), list):
        volume_ids.extend([str(v) for v in os_meta.get("extra_volume_ids") if isinstance(v, str) and v.strip()])
    volume_ids = list(dict.fromkeys(volume_ids))

    if not image_ids and not server_id and not volume_ids:
        return

    selected_openstack_endpoint_session_id = metadata.get("selected_openstack_endpoint_session_id")
    cloud = getattr(settings, "OPENSTACK_CLOUD_NAME", "openstack")
    try:
        auth_overrides = None
        if isinstance(selected_openstack_endpoint_session_id, int):
            openstack_session = OpenstackEndpointSession.objects.filter(
                id=selected_openstack_endpoint_session_id
            ).first()
            auth_overrides = openstack_session.to_connect_kwargs() if openstack_session else None
        conn = connect_openstack(cloud=cloud, auth_overrides=auth_overrides)
    except OpenStackDeploymentError as exc:
        actions.append({"action": "openstack_cleanup", "status": "error", "error": str(exc)})
        return

    if server_id:
        try:
            status = delete_server_if_exists(conn, server_id)
            actions.append({"action": "delete_server", "server_id": server_id, "status": status})
        except Exception as exc:
            actions.append({
                "action": "delete_server",
                "server_id": server_id,
                "status": "error",
                "error": str(exc),
            })

    for volume_id in volume_ids:
        try:
            status = delete_volume_if_exists(conn, volume_id)
            actions.append({"action": "delete_volume", "volume_id": volume_id, "status": status})
        except Exception as exc:
            actions.append({
                "action": "delete_volume",
                "volume_id": volume_id,
                "status": "error",
                "error": str(exc),
            })

    for image_id in image_ids:
        try:
            status = delete_image_if_exists(conn, image_id)
            actions.append({"action": "delete_image", "image_id": image_id, "status": status})
        except Exception as exc:
            actions.append({
                "action": "delete_image",
                "image_id": image_id,
                "status": "error",
                "error": str(exc),
            })


@shared_task(name="migrations.rollback_migration", max_retries=1, default_retry_delay=30, acks_late=True)
def rollback_migration(job_id: int, context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Rollback conversion artifacts for failed jobs and mark them ROLLED_BACK."""

    try:
        job = MigrationJob.objects.get(id=job_id)
    except MigrationJob.DoesNotExist:
        logger.warning("migration.rollback missing job", extra={"job_id": job_id})
        return {"job_id": job_id, "result": "missing"}

    actions: list[dict[str, Any]] = []
    rollback_reason = (context or {}).get("rollback_reason", "unspecified failure")

    try:
        files, dirs = _collect_cleanup_targets(job, context)

        for path in files:
            if path.exists() and path.is_file():
                path.unlink()
                actions.append({"action": "delete_file", "path": str(path), "status": "deleted"})
            else:
                actions.append({"action": "delete_file", "path": str(path), "status": "not_found"})

        for path in dirs:
            if path.exists() and path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
                actions.append({"action": "delete_dir", "path": str(path), "status": "deleted"})
            else:
                actions.append({"action": "delete_dir", "path": str(path), "status": "not_found"})

        _rollback_openstack_resources(job, actions)

        metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
        metadata["rollback_at"] = timezone.now().isoformat()
        metadata["rollback_reason"] = rollback_reason
        metadata["rollback_actions"] = actions
        job.conversion_metadata = metadata

        if job.status == MigrationJob.Status.FAILED and job.can_transition_to(MigrationJob.Status.ROLLED_BACK):
            job.transition(MigrationJob.Status.ROLLED_BACK)
        elif job.status == MigrationJob.Status.ROLLED_BACK:
            pass
        else:
            metadata["rollback_note"] = f"rollback executed while job in state {job.status}"

        job.save(update_fields=["status", "conversion_metadata", "updated_at"])

        logger.info(
            "migration.rollback completed",
            extra={"job_id": job.id, "vm_name": job.vm_name, "actions": actions},
        )
        return {
            "job_id": job.id,
            "result": "rolled_back",
            "status": job.status,
            "actions": actions,
        }

    except Exception as exc:
        logger.exception(
            "migration.rollback failed",
            extra={"job_id": job.id, "vm_name": job.vm_name, "error": str(exc)},
        )
        return {
            "job_id": job.id,
            "result": "rollback_error",
            "status": job.status,
            "error": str(exc),
            "actions": actions,
        }


def _build_base_conversion_metadata(
    *,
    discovered_vm: DiscoveredVM,
    plan: ConversionPlan,
    validation: dict[str, Any],
    mode: str,
) -> dict[str, Any]:
    return {
        "selected_source": discovered_vm.source,
        "selected_vmware_endpoint_session_id": discovered_vm.vmware_endpoint_session_id,
        "conversion": {
            "mode": mode,
            "command": plan.command,
            "command_args": plan.command_args,
            "input_disks": plan.input_disks,
            "output_path": plan.output_path,
            "notes": plan.notes,
            "validation": validation,
        },
    }


def _effective_target_spec(job: MigrationJob, discovered_vm: DiscoveredVM) -> dict[str, Any]:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    requested = metadata.get("requested_spec", {}) if isinstance(metadata.get("requested_spec"), dict) else {}
    disk_layout_mode = str(requested.get("disk_layout_mode", "") or "").strip().lower()
    disk_merge = bool(requested.get("disk_merge", False))
    if disk_merge or disk_layout_mode in {"merge", "concat", "concatenate"}:
        raise ConversionPlanningError(
            "Disk concatenation/merge is forbidden in production mode. "
            "Disk architecture must remain unchanged (1-to-1, same order, no merge)."
        )

    flavor_id = requested.get("flavor_id") if isinstance(requested.get("flavor_id"), str) else None
    if isinstance(flavor_id, str) and not flavor_id.strip():
        flavor_id = None
    if isinstance(flavor_id, str):
        flavor_id = flavor_id.strip()

    target_cpu = requested.get("cpu") if isinstance(requested.get("cpu"), int) and requested.get("cpu") > 0 else discovered_vm.cpu
    target_ram = requested.get("ram") if isinstance(requested.get("ram"), int) and requested.get("ram") > 0 else discovered_vm.ram

    network_overrides = requested.get("network", {}) if isinstance(requested.get("network"), dict) else {}
    network_id = network_overrides.get("network_id")
    network_name = network_overrides.get("network_name")
    fixed_ip = network_overrides.get("fixed_ip")

    if not isinstance(network_id, str) or not network_id.strip():
        network_id = None
    else:
        network_id = network_id.strip()

    if not isinstance(network_name, str) or not network_name.strip():
        network_name = None
    else:
        network_name = network_name.strip()

    if not isinstance(fixed_ip, str) or not fixed_ip.strip():
        fixed_ip = None
    else:
        fixed_ip = fixed_ip.strip()

    raw_extra_disks = requested.get("extra_disks_gb")
    extra_disks_gb: list[int] = []
    if isinstance(raw_extra_disks, list):
        extra_disks_gb = [int(v) for v in raw_extra_disks if isinstance(v, int) and v > 0]

    return {
        "flavor_id": flavor_id,
        "cpu": target_cpu,
        "ram": target_ram,
        "network_id": network_id,
        "network_name": network_name,
        "fixed_ip": fixed_ip,
        "extra_disks_gb": extra_disks_gb,
    }


def _validate_openstack_disk_attachments(conn, server_id: str, expected_volume_ids: list[str]) -> dict[str, Any]:
    server = conn.compute.get_server(server_id)
    attached = getattr(server, "attached_volumes", None) or []
    attached_ids = {
        str(item.get("id"))
        for item in attached
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }

    missing = [vid for vid in expected_volume_ids if vid not in attached_ids]
    per_volume: list[dict[str, Any]] = []
    for volume_id in expected_volume_ids:
        volume = conn.block_storage.get_volume(volume_id)
        status = str(getattr(volume, "status", "")).lower()
        per_volume.append({"volume_id": volume_id, "status": status})
        if status not in {"in-use", "in_use"}:
            missing.append(volume_id)

    return {
        "ok": len(missing) == 0,
        "missing_or_not_in_use": sorted(set(missing)),
        "attached_volume_ids": sorted(attached_ids),
        "volumes": per_volume,
        "note": (
            "Validated boot + volume attachments. Guest-level filesystem mount validation "
            "requires in-guest agent checks."
        ),
    }


def _run_openstack_deployment(job: MigrationJob, discovered_vm: DiscoveredVM) -> dict[str, Any]:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    conversion = metadata.get("conversion", {}) if isinstance(metadata.get("conversion"), dict) else {}
    execution = conversion.get("execution", {}) if isinstance(conversion.get("execution"), dict) else {}

    qcow2_paths_raw = execution.get("output_qcow2_paths")
    qcow2_paths: list[str] = []
    if isinstance(qcow2_paths_raw, list):
        qcow2_paths = [str(p).strip() for p in qcow2_paths_raw if isinstance(p, str) and str(p).strip()]

    if not qcow2_paths:
        legacy_path = execution.get("output_qcow2_path") or conversion.get("output_path")
        if isinstance(legacy_path, str) and legacy_path.strip():
            qcow2_paths = [legacy_path.strip()]

    if not qcow2_paths:
        raise OpenStackDeploymentError("Missing QCOW2 path in conversion metadata for OpenStack upload.")
    output_disk_format = str(execution.get("output_disk_format", "qcow2") or "qcow2").strip().lower()
    if output_disk_format not in {"qcow2", "raw"}:
        raise OpenStackDeploymentError(
            f"Unsupported converted disk format '{output_disk_format}'. Allowed: qcow2, raw."
        )

    selected_openstack_endpoint_session_id = metadata.get("selected_openstack_endpoint_session_id")
    cloud = getattr(settings, "OPENSTACK_CLOUD_NAME", "openstack")
    auth_overrides = None
    if isinstance(selected_openstack_endpoint_session_id, int):
        openstack_session = OpenstackEndpointSession.objects.filter(id=selected_openstack_endpoint_session_id).first()
        auth_overrides = openstack_session.to_connect_kwargs() if openstack_session else None
    conn = connect_openstack(cloud=cloud, auth_overrides=auth_overrides)

    os_meta = metadata.get("openstack", {}) if isinstance(metadata.get("openstack"), dict) else {}
    if isinstance(selected_openstack_endpoint_session_id, int):
        os_meta["selected_openstack_endpoint_session_id"] = selected_openstack_endpoint_session_id
    names = build_openstack_names(job.vm_name, job.id)
    target_spec = _effective_target_spec(job, discovered_vm)

    if target_spec.get("flavor_id"):
        flavor = get_flavor_choice_by_id(conn, target_spec["flavor_id"])
    else:
        flavor = map_vmware_to_flavor(conn, target_spec["cpu"], target_spec["ram"])

    preferred_network = target_spec["network_name"] or getattr(settings, "OPENSTACK_DEFAULT_NETWORK", "") or None
    network = select_default_network(
        conn,
        preferred_name=preferred_network,
        preferred_id=target_spec.get("network_id"),
    )

    existing_image_ids = os_meta.get("image_ids") if isinstance(os_meta.get("image_ids"), list) else []
    image_ids: list[str] = []
    for idx, qcow2_path in enumerate(qcow2_paths):
        image_name = names["image_name"] if idx == 0 else f"{names['image_name']}-disk{idx}"
        existing_image_id = None
        if idx < len(existing_image_ids) and isinstance(existing_image_ids[idx], str):
            existing_image_id = existing_image_ids[idx]
        elif idx == 0 and isinstance(os_meta.get("image_id"), str):
            existing_image_id = os_meta.get("image_id")

        image_id = ensure_uploaded_image(
            conn,
            qcow2_path=qcow2_path,
            image_name=image_name,
            disk_format=output_disk_format,
            existing_image_id=existing_image_id,
            timeout_seconds=int(getattr(settings, "OPENSTACK_IMAGE_UPLOAD_TIMEOUT", 900)),
            poll_interval_seconds=int(getattr(settings, "OPENSTACK_IMAGE_UPLOAD_POLL_INTERVAL", 5)),
            retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
            retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
        )
        image_ids.append(image_id)

    existing_volume_ids = os_meta.get("volume_ids") if isinstance(os_meta.get("volume_ids"), list) else []
    attached_volumes: list[dict[str, Any]] = []
    converted_volume_ids: list[str] = []
    for idx, image_id in enumerate(image_ids):
        vol_name = f"{names['server_name']}-disk{idx}"
        existing_volume_id = None
        if idx < len(existing_volume_ids) and isinstance(existing_volume_ids[idx], str):
            existing_volume_id = existing_volume_ids[idx]
        volume_id = ensure_volume_from_image(
            conn,
            volume_name=vol_name,
            image_id=image_id,
            existing_volume_id=existing_volume_id,
            timeout_seconds=int(getattr(settings, "OPENSTACK_VERIFY_TIMEOUT", 900)),
            poll_interval_seconds=int(getattr(settings, "OPENSTACK_IMAGE_UPLOAD_POLL_INTERVAL", 5)),
            retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
            retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
        )
        converted_volume_ids.append(volume_id)

    if len(converted_volume_ids) != len(qcow2_paths):
        raise OpenStackDeploymentError(
            "Converted volume count mismatch: "
            f"source_disks={len(qcow2_paths)} converted_volumes={len(converted_volume_ids)}. "
            "Disk architecture must remain unchanged (1-to-1, same order, no merge)."
        )

    primary_disk_index = execution.get("primary_disk_index", 0)
    if not isinstance(primary_disk_index, int) or primary_disk_index < 0 or primary_disk_index >= len(converted_volume_ids):
        primary_disk_index = 0
    primary_image_id = image_ids[primary_disk_index]
    primary_volume_id = converted_volume_ids[primary_disk_index]

    server_id = ensure_server_booted_from_volume(
        conn,
        server_name=names["server_name"],
        boot_volume_id=primary_volume_id,
        flavor_id=flavor.id,
        network_id=network.id,
        fixed_ip=target_spec["fixed_ip"],
        existing_server_id=os_meta.get("server_id"),
        retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
        retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
    )

    # Wait for Nova to finish server build before attaching non-boot volumes.
    server_ready_status = verify_server_active(
        conn,
        server_id=server_id,
        timeout_seconds=int(getattr(settings, "OPENSTACK_VERIFY_TIMEOUT", 900)),
        poll_interval_seconds=int(getattr(settings, "OPENSTACK_VERIFY_POLL_INTERVAL", 10)),
    )

    for idx, volume_id in enumerate(converted_volume_ids):
        if idx == primary_disk_index:
            attached_volumes.append(
                {
                    "index": idx,
                    "kind": "converted",
                    "image_id": image_ids[idx],
                    "volume_id": volume_id,
                    "status": "boot_volume",
                    "boot": True,
                }
            )
            continue
        attach_status = attach_volume_to_server(
            conn,
            server_id=server_id,
            volume_id=volume_id,
            retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
            retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
        )
        attached_volumes.append(
            {
                "index": idx,
                "kind": "converted",
                "image_id": image_ids[idx],
                "volume_id": volume_id,
                "status": attach_status,
                "boot": False,
            }
        )

    extra_volume_ids = os_meta.get("extra_volume_ids") if isinstance(os_meta.get("extra_volume_ids"), list) else []
    requested_extra_disks = target_spec["extra_disks_gb"]
    for extra_idx, size_gb in enumerate(requested_extra_disks, start=1):
        vol_name = f"{names['server_name']}-extra{extra_idx}"
        existing_extra_volume_id = None
        if (extra_idx - 1) < len(extra_volume_ids) and isinstance(extra_volume_ids[extra_idx - 1], str):
            existing_extra_volume_id = extra_volume_ids[extra_idx - 1]

        volume_id = ensure_empty_volume(
            conn,
            volume_name=vol_name,
            size_gb=size_gb,
            existing_volume_id=existing_extra_volume_id,
            timeout_seconds=int(getattr(settings, "OPENSTACK_VERIFY_TIMEOUT", 900)),
            poll_interval_seconds=int(getattr(settings, "OPENSTACK_IMAGE_UPLOAD_POLL_INTERVAL", 5)),
            retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
            retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
        )
        extra_volume_ids.append(volume_id)

        attach_status = attach_volume_to_server(
            conn,
            server_id=server_id,
            volume_id=volume_id,
            retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
            retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
        )
        attached_volumes.append(
            {
                "index": extra_idx,
                "kind": "extra",
                "size_gb": size_gb,
                "volume_id": volume_id,
                "status": attach_status,
            }
        )

    os_meta.update(
        {
            "cloud": cloud,
            "image_id": primary_image_id,
            "image_ids": image_ids,
            "image_name": names["image_name"],
            "image_names": [names["image_name"]] + [f"{names['image_name']}-disk{i}" for i in range(1, len(image_ids))],
            "source_qcow2_paths": qcow2_paths,
            "source_disk_count": len(qcow2_paths),
            "output_disk_format": output_disk_format,
            "flavor_id": flavor.id,
            "flavor_name": flavor.name,
            "target_cpu": target_spec["cpu"],
            "target_ram": target_spec["ram"],
            "network_id": network.id,
            "network_name": network.name,
            "fixed_ip": target_spec["fixed_ip"],
            "server_id": server_id,
            "server_name": names["server_name"],
            "server_status_before_attach": server_ready_status,
            "boot_volume_id": primary_volume_id,
            "boot_disk_index": primary_disk_index,
            "volume_ids": converted_volume_ids,
            "extra_volume_ids": extra_volume_ids,
            "requested_extra_disks_gb": requested_extra_disks,
            "attached_volumes": attached_volumes,
        }
    )

    metadata["openstack"] = os_meta
    job.conversion_metadata = metadata

    if job.status == MigrationJob.Status.UPLOADING and job.can_transition_to(MigrationJob.Status.DEPLOYED):
        job.transition(MigrationJob.Status.DEPLOYED)

    verified_status = verify_server_active(
        conn,
        server_id=server_id,
        timeout_seconds=int(getattr(settings, "OPENSTACK_VERIFY_TIMEOUT", 900)),
        poll_interval_seconds=int(getattr(settings, "OPENSTACK_VERIFY_POLL_INTERVAL", 10)),
    )

    os_meta["server_status"] = verified_status
    os_meta["verified_at"] = timezone.now().isoformat()
    attachment_validation = _validate_openstack_disk_attachments(conn, server_id, converted_volume_ids)
    os_meta["disk_attachment_validation"] = attachment_validation
    if not attachment_validation.get("ok"):
        raise OpenStackDeploymentError(
            "Post-migration disk attachment validation failed: "
            f"{attachment_validation.get('missing_or_not_in_use')}"
        )

    if job.status == MigrationJob.Status.DEPLOYED and job.can_transition_to(MigrationJob.Status.VERIFIED):
        job.transition(MigrationJob.Status.VERIFIED)

    job.conversion_metadata = metadata
    job.save(update_fields=["status", "conversion_metadata", "updated_at"])

    return {
        "job_id": job.id,
        "result": "deployed",
        "status": job.status,
        "image_id": primary_image_id,
        "image_ids": image_ids,
        "server_id": server_id,
        "volume_ids": converted_volume_ids,
        "flavor": {"id": flavor.id, "name": flavor.name},
        "network": {"id": network.id, "name": network.name},
    }


@shared_task(name="migrations.start_migration", max_retries=0, acks_late=True)
def start_migration(job_id: int) -> dict[str, Any]:
    """Migration starter with conversion and optional OpenStack deployment."""

    try:
        try:
            job = MigrationJob.objects.get(id=job_id)
        except MigrationJob.DoesNotExist:
            logger.error("migration.start missing job", extra={"job_id": job_id})
            return {"job_id": job_id, "result": "missing"}

        logger.info(
            "migration.start begin",
            extra={"job_id": job.id, "vm_name": job.vm_name, "status": job.status},
        )

        discovered_vm: DiscoveredVM | None = None

        # Keep DB transactions short: only lock+transition the job state here.
        with transaction.atomic():
            job = MigrationJob.objects.select_for_update().get(id=job_id)
            if job.status == MigrationJob.Status.PENDING:
                job.transition(MigrationJob.Status.DISCOVERED)
            if job.status == MigrationJob.Status.DISCOVERED:
                job.transition(MigrationJob.Status.CONVERTING)

        job.refresh_from_db()

        # Conversion stage (may take minutes): no DB transaction should be held open here.
        if job.status == MigrationJob.Status.CONVERTING:
            discovered_vm = _find_discovered_vm_for_job(job)
            esxi_uri = None
            passfile: Path | None = None

            if discovered_vm.source == DiscoveredVM.Source.ESXI:
                # For safety: require powered off in ESXi conversions.
                if (discovered_vm.power_state or "").lower() not in {"poweredoff", "powered_off", "poweroff", "off"}:
                    raise ConversionPlanningError(
                        f"ESXi VM '{discovered_vm.name}' must be powered off for safe conversion "
                        f"(current power_state='{discovered_vm.power_state}')."
                    )

                # Minimal snapshot guardrail: refuse to proceed if VM has snapshots (default).
                require_no_snaps = bool(getattr(settings, "VMWARE_REQUIRE_NO_SNAPSHOTS", True))
                has_snaps = bool((discovered_vm.metadata or {}).get("has_snapshots"))
                if require_no_snaps and has_snaps:
                    raise ConversionPlanningError(
                        f"ESXi VM '{discovered_vm.name}' has snapshots; consolidate/remove snapshots before conversion."
                    )

                metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
                vmware_endpoint_session_id = metadata.get("selected_vmware_endpoint_session_id")
                vmware_session = None
                if isinstance(vmware_endpoint_session_id, int):
                    vmware_session = VmwareEndpointSession.objects.filter(id=vmware_endpoint_session_id).first()

                esxi_password = (
                    vmware_session.password.strip()
                    if vmware_session and isinstance(vmware_session.password, str)
                    else os.getenv("VMWARE_ESXI_PASSWORD", "").strip()
                )
                if not esxi_password:
                    raise ConversionPlanningError("VMWARE_ESXI_PASSWORD is required for ESXi conversion.")

                # Create a per-job temp dir for secret files; rollback cleans it.
                temp_dir = Path(settings.MIGRATION_OUTPUT_DIR) / "tmp" / f"job-{job.id}"
                passfile = _write_password_file(temp_dir, esxi_password)
                if vmware_session:
                    esxi_uri = _build_esxi_libvirt_uri_with_values(
                        host=vmware_session.host,
                        username=vmware_session.username,
                        insecure=bool(vmware_session.insecure),
                    )
                else:
                    esxi_uri = _build_esxi_libvirt_uri()

            plan = plan_vmware_conversion(
                discovered_vm,
                output_dir=settings.MIGRATION_OUTPUT_DIR,
                esxi_uri=esxi_uri,
                password_file=str(passfile) if passfile else None,
                esxi_transport=os.getenv("VMWARE_ESXI_CONVERSION_TRANSPORT", "").strip().lower() or None,
                vddk_libdir=os.getenv("VMWARE_VDDK_LIBDIR", "").strip() or None,
                vddk_thumbprint=os.getenv("VMWARE_VDDK_THUMBPRINT", "").strip() or None,
            )

            validation: dict[str, Any] = {"checked_paths": [], "errors": [], "skipped": False}
            if discovered_vm.source == DiscoveredVM.Source.WORKSTATION:
                validation = _validate_workstation_paths(plan.input_disks, plan.output_path)
                if validation["errors"]:
                    raise ConversionPlanningError("; ".join(validation["errors"]))
            elif discovered_vm.source == DiscoveredVM.Source.ESXI:
                validation["checked_paths"] = [
                    {"password_file": str(passfile) if passfile else None, "esxi_uri": esxi_uri}
                ]
            else:
                raise ConversionPlanningError(f"Unsupported VMware source '{discovered_vm.source}'.")

            real_conversion_enabled = bool(getattr(settings, "ENABLE_REAL_CONVERSION", False))
            mode = "real" if real_conversion_enabled else "dry-run"
            if real_conversion_enabled:
                _ensure_libguestfs_kernel_readable()

            metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
            previous_execution: dict[str, Any] = {}
            if isinstance(metadata.get("conversion"), dict) and isinstance(metadata["conversion"].get("execution"), dict):
                previous_execution = metadata["conversion"]["execution"]

            metadata.update(
                _build_base_conversion_metadata(
                    discovered_vm=discovered_vm,
                    plan=plan,
                    validation=validation,
                    mode=mode,
                )
            )

            # Track temp dirs so rollback can clean them.
            if discovered_vm.source == DiscoveredVM.Source.ESXI:
                temp_dirs = metadata.get("conversion", {}).get("temp_dirs")
                if not isinstance(temp_dirs, list):
                    temp_dirs = []
                temp_dir_str = str((Path(settings.MIGRATION_OUTPUT_DIR) / "tmp" / f"job-{job.id}"))
                if temp_dir_str not in temp_dirs:
                    temp_dirs.append(temp_dir_str)
                metadata["conversion"]["temp_dirs"] = temp_dirs

            # Preserve earlier execution metadata if present.
            if previous_execution:
                metadata["conversion"]["execution"] = previous_execution

            prior = metadata.get("conversion", {}).get("execution", {})
            if prior.get("state") == "succeeded" and prior.get("output_qcow2_path"):
                out = Path(prior["output_qcow2_path"])
                if out.exists() and out.is_file():
                    with transaction.atomic():
                        job = MigrationJob.objects.select_for_update().get(id=job_id)
                        if job.status == MigrationJob.Status.CONVERTING and job.can_transition_to(MigrationJob.Status.UPLOADING):
                            job.transition(MigrationJob.Status.UPLOADING)
                        job.conversion_metadata = metadata
                        job.save(update_fields=["status", "conversion_metadata", "updated_at"])
                else:
                    previous_execution = {}

            if not real_conversion_enabled:
                job.conversion_metadata = metadata
                job.save(update_fields=["conversion_metadata", "updated_at"])
                logger.info(
                    "migration.start planned_dry_run",
                    extra={
                        "job_id": job.id,
                        "vm_name": job.vm_name,
                        "source": discovered_vm.source,
                        "command": plan.command,
                    },
                )
                return {
                    "job_id": job.id,
                    "result": "planned",
                    "status": job.status,
                    "vm_name": job.vm_name,
                    "source": discovered_vm.source,
                    "command": plan.command,
                    "input_disks": plan.input_disks,
                    "output_path": plan.output_path,
                    "dry_run": True,
                }

            # Concurrency guard: only one worker should run conversion for a given job at a time.
            # We do a short "compare-and-set" under a row lock, then release it before running virt-v2v.
            with transaction.atomic():
                job = MigrationJob.objects.select_for_update().get(id=job_id)
                db_meta = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
                db_conv = db_meta.get("conversion", {}) if isinstance(db_meta.get("conversion"), dict) else {}
                db_exec = db_conv.get("execution", {}) if isinstance(db_conv.get("execution"), dict) else {}
                if db_exec.get("state") == "running":
                    logger.info(
                        "migration.start conversion_already_running",
                        extra={"job_id": job.id, "vm_name": job.vm_name},
                    )
                    return {
                        "job_id": job.id,
                        "result": "already_running",
                        "status": job.status,
                    }

                metadata["conversion"]["execution"] = {
                    "state": "running",
                    "started_at": timezone.now().isoformat(),
                }
                job.conversion_metadata = metadata
                job.save(update_fields=["conversion_metadata", "updated_at"])

            if discovered_vm.source == DiscoveredVM.Source.WORKSTATION:
                exec_result = _execute_workstation_qemu_pipeline(plan, discovered_vm.name)
            elif bool(getattr(settings, "ENABLE_ANSIBLE_CONVERSION", False)):
                exec_result = _execute_ansible_conversion(plan, discovered_vm.name)
            else:
                exec_result = _execute_virt_v2v(plan, discovered_vm.name)
            metadata["conversion"]["execution"] = {
                "state": "succeeded",
                **exec_result,
            }

            # Optional minimal artifact backup: keep a copy of the QCOW2 before OpenStack upload.
            if bool(getattr(settings, "ENABLE_ARTIFACT_BACKUP", False)):
                try:
                    src_paths = exec_result.get("output_qcow2_paths")
                    if not isinstance(src_paths, list) or not src_paths:
                        src_paths = [exec_result["output_qcow2_path"]]
                    backup_root = Path(
                        getattr(
                            settings,
                            "ARTIFACT_BACKUP_DIR",
                            str(Path(settings.MIGRATION_OUTPUT_DIR) / "backups"),
                        )
                    ).expanduser()
                    backup_dir = backup_root / f"job-{job.id}"
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    backup_paths: list[str] = []
                    for src_raw in src_paths:
                        src = Path(str(src_raw)).expanduser().resolve()
                        dst = backup_dir / src.name
                        if not dst.exists():
                            shutil.copy2(src, dst)
                        backup_paths.append(str(dst))
                    metadata["conversion"]["backup"] = {
                        "enabled": True,
                        "path": backup_paths[0] if backup_paths else "",
                        "paths": backup_paths,
                        "method": "copy2",
                        "created_at": timezone.now().isoformat(),
                    }
                except Exception as exc:
                    if bool(getattr(settings, "ARTIFACT_BACKUP_REQUIRED", False)):
                        raise
                    warnings = metadata["conversion"].get("warnings")
                    if not isinstance(warnings, list):
                        warnings = []
                    warnings.append(f"artifact backup failed: {exc}")
                    metadata["conversion"]["warnings"] = warnings

            with transaction.atomic():
                job = MigrationJob.objects.select_for_update().get(id=job_id)
                job.conversion_metadata = metadata
                if job.status == MigrationJob.Status.CONVERTING and job.can_transition_to(MigrationJob.Status.UPLOADING):
                    job.transition(MigrationJob.Status.UPLOADING)
                job.save(update_fields=["status", "conversion_metadata", "updated_at"])

            logger.info(
                "migration.start conversion_success",
                extra={
                    "job_id": job.id,
                    "vm_name": job.vm_name,
                    "command": plan.command,
                    "output_qcow2_path": exec_result["output_qcow2_path"],
                },
            )

            job.refresh_from_db()

        if job.status == MigrationJob.Status.UPLOADING and not discovered_vm:
            discovered_vm = _find_discovered_vm_for_job(job)

        if not getattr(settings, "ENABLE_OPENSTACK_DEPLOYMENT", False):
            return {
                "job_id": job.id,
                "result": "converted" if job.status in {MigrationJob.Status.UPLOADING, MigrationJob.Status.DEPLOYED, MigrationJob.Status.VERIFIED} else "skipped",
                "status": job.status,
                "dry_run": False,
                "deployment_enabled": False,
            }

        if job.status in {MigrationJob.Status.UPLOADING, MigrationJob.Status.DEPLOYED}:
            if not discovered_vm:
                discovered_vm = _find_discovered_vm_for_job(job)
            deploy_result = _run_openstack_deployment(job, discovered_vm)
            logger.info(
                "migration.start openstack_deploy_success",
                extra={
                    "job_id": job.id,
                    "vm_name": job.vm_name,
                    "image_id": deploy_result["image_id"],
                    "server_id": deploy_result["server_id"],
                },
            )
            return deploy_result

        return {
            "job_id": job.id,
            "result": "skipped",
            "status": job.status,
            "reason": "job is not in deployable state",
        }

    except ConversionExecutionError as exc:
        metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
        conv = metadata.get("conversion", {}) if isinstance(metadata.get("conversion"), dict) else {}
        conv["execution"] = {
            "state": "failed",
            "returncode": exc.returncode,
            "stdout": _truncate_log(exc.stdout),
            "stderr": _truncate_log(exc.stderr),
        }
        metadata["conversion"] = conv
        job.conversion_metadata = metadata
        job.save(update_fields=["conversion_metadata", "updated_at"])

        _mark_job_failed(job, str(exc))
        _schedule_rollback(job, str(exc), extra_context={"output_qcow2_path": conv.get("output_qcow2_path")})

        logger.error(
            "migration.start conversion_failed",
            extra={"job_id": job.id, "vm_name": job.vm_name, "error": str(exc)},
        )
        return {
            "job_id": job.id,
            "result": "failed",
            "status": MigrationJob.Status.FAILED,
            "error": str(exc),
        }

    except (
        OpenStackDeploymentError,
        ConversionPlanningError,
        AnsibleRunnerError,
        InvalidTransitionError,
        PermissionError,
        OSError,
        subprocess.SubprocessError,
    ) as exc:
        error_message = str(exc)
        _mark_job_failed(job, error_message)
        _schedule_rollback(job, error_message)
        logger.error(
            "migration.start failed",
            extra={"job_id": job.id, "vm_name": job.vm_name, "error": error_message},
        )
        return {
            "job_id": job.id,
            "result": "failed",
            "status": MigrationJob.Status.FAILED,
            "error": error_message,
        }
    except Exception as exc:
        error_message = f"unexpected error: {exc}"
        _mark_job_failed(job, error_message)
        _schedule_rollback(job, error_message)
        logger.exception(
            "migration.start unexpected_error",
            extra={"job_id": job.id, "vm_name": job.vm_name, "error": error_message},
        )
        return {
            "job_id": job.id,
            "result": "failed",
            "status": MigrationJob.Status.FAILED,
            "error": error_message,
        }


@shared_task(name="migrations.discover_vmware_vms", max_retries=2, default_retry_delay=15, acks_late=True)
def discover_vmware_vms(
    include_workstation: bool = True,
    include_esxi: bool = True,
    vmware_endpoint_session_id: int | None = None,
) -> dict[str, Any]:
    """Discover VMs from configured VMware sources and upsert DiscoveredVM rows."""

    now = timezone.now()
    result: dict[str, Any] = {
        "workstation": {"discovered": 0, "upserted": 0, "errors": []},
        "esxi": {"discovered": 0, "upserted": 0, "errors": []},
    }

    vmware_session = None
    if isinstance(vmware_endpoint_session_id, int):
        vmware_session = VmwareEndpointSession.objects.filter(id=vmware_endpoint_session_id).first()
        if vmware_session is None:
            raise VMwareClientError(f"VMware endpoint session '{vmware_endpoint_session_id}' not found.")
        include_workstation = False
        include_esxi = True

    def upsert_many(source: str, items: list[dict[str, Any]], endpoint_session: VmwareEndpointSession | None = None) -> int:
        upserted = 0
        for item in items:
            defaults = {
                "cpu": item.get("cpu"),
                "ram": item.get("ram"),
                "disks": item.get("disks", []),
                "metadata": item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {},
                "power_state": item.get("power_state") or "",
                "last_seen": now,
            }
            DiscoveredVM.objects.update_or_create(
                name=item["name"],
                source=source,
                vmware_endpoint_session=endpoint_session,
                defaults=defaults,
            )
            upserted += 1
        return upserted

    if include_workstation:
        try:
            ws_items = WorkstationVMwareClient().discover_vms()
            result["workstation"]["discovered"] = len(ws_items)
            result["workstation"]["upserted"] = upsert_many(DiscoveredVM.Source.WORKSTATION, ws_items)
        except VMwareClientError as exc:
            result["workstation"]["errors"].append(str(exc))

    if include_esxi:
        try:
            if vmware_session:
                esxi_client = ESXiVMwareClient(
                    host=vmware_session.host,
                    username=vmware_session.username,
                    password=vmware_session.password,
                    port=vmware_session.port,
                    insecure=vmware_session.insecure,
                )
            else:
                esxi_client = ESXiVMwareClient.from_env()
            esxi_items = esxi_client.discover_vms()
            result["esxi"]["discovered"] = len(esxi_items)
            result["esxi"]["upserted"] = upsert_many(DiscoveredVM.Source.ESXI, esxi_items, vmware_session)
        except VMwareClientError as exc:
            result["esxi"]["errors"].append(str(exc))

    if vmware_session:
        result["vmware_endpoint_session_id"] = vmware_session.id

    return result


@shared_task(name="migrations.provision_openstack_infra", max_retries=0, acks_late=True)
def provision_openstack_infra(var_overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Optionally run terraform apply from Celery when explicitly enabled."""
    if not getattr(settings, "ENABLE_TERRAFORM_INFRA", False):
        return {"status": "skipped", "reason": "ENABLE_TERRAFORM_INFRA is false"}
    if not getattr(settings, "ENABLE_TERRAFORM_FROM_CELERY", False):
        return {"status": "skipped", "reason": "ENABLE_TERRAFORM_FROM_CELERY is false"}

    vars_payload = dict(getattr(settings, "TERRAFORM_DEFAULT_VARS", {}))
    if isinstance(var_overrides, dict):
        vars_payload.update(var_overrides)

    runner = TerraformRunner(binary=getattr(settings, "TERRAFORM_BIN", "terraform"))
    try:
        result = runner.apply(
            working_dir=getattr(settings, "TERRAFORM_WORKING_DIR"),
            var_overrides=vars_payload,
            timeout_seconds=int(getattr(settings, "TERRAFORM_TIMEOUT_SECONDS", 1800)),
            auto_approve=True,
        )
    except TerraformRunnerError as exc:
        logger.error("terraform.apply.failed", extra={"error": str(exc)})
        return {"status": "failed", "error": str(exc)}

    return {"status": "success", "result": result}
