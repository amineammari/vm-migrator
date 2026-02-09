from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .conversion import ConversionPlanningError, ConversionPlan, plan_vmware_conversion
from .models import DiscoveredVM, InvalidTransitionError, MigrationJob
from .openstack_deployment import (
    OpenStackDeploymentError,
    build_openstack_names,
    connect_openstack,
    delete_image_if_exists,
    delete_server_if_exists,
    ensure_server_booted,
    ensure_uploaded_image,
    map_vmware_to_flavor,
    select_default_network,
    verify_server_active,
)
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


def _find_discovered_vm_for_job(job: MigrationJob) -> DiscoveredVM:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    selected_source = metadata.get("selected_source")

    qs = DiscoveredVM.objects.filter(name=job.vm_name)
    if selected_source:
        qs = qs.filter(source=selected_source)

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


def _find_output_qcow2(output_path: str, vm_name: str) -> Path:
    expected = Path(output_path)
    if expected.exists() and expected.is_file():
        return expected

    output_dir = expected.parent
    if not output_dir.exists():
        raise ConversionExecutionError(f"Output directory not found after conversion: {output_dir}")

    candidates = sorted(output_dir.glob(f"{vm_name}*.qcow2"), key=lambda p: p.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    raise ConversionExecutionError(
        f"No QCOW2 output found in {output_dir} for VM '{vm_name}' after conversion."
    )


def _execute_virt_v2v(plan: ConversionPlan, vm_name: str) -> dict[str, Any]:
    start = time.monotonic()

    try:
        completed = subprocess.run(
            plan.command_args,
            capture_output=True,
            text=True,
            check=False,
            timeout=int(getattr(settings, "VIRT_V2V_TIMEOUT_SECONDS", 7200)),
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

    qcow2_path = _find_output_qcow2(plan.output_path, vm_name)
    disk_size = qcow2_path.stat().st_size

    return {
        "returncode": completed.returncode,
        "duration_seconds": duration,
        "stdout": _truncate_log(stdout),
        "stderr": _truncate_log(stderr),
        "output_qcow2_path": str(qcow2_path),
        "disk_size": disk_size,
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

    for candidate in [
        execution.get("output_qcow2_path"),
        conversion.get("output_path"),
        context.get("output_qcow2_path"),
    ]:
        if isinstance(candidate, str) and candidate.strip():
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

    image_id = os_meta.get("image_id")
    server_id = os_meta.get("server_id")
    if not image_id and not server_id:
        return

    cloud = getattr(settings, "OPENSTACK_CLOUD_NAME", "openstack")
    try:
        conn = connect_openstack(cloud=cloud)
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

    if image_id:
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


def _run_openstack_deployment(job: MigrationJob, discovered_vm: DiscoveredVM) -> dict[str, Any]:
    metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
    conversion = metadata.get("conversion", {}) if isinstance(metadata.get("conversion"), dict) else {}
    execution = conversion.get("execution", {}) if isinstance(conversion.get("execution"), dict) else {}

    qcow2_path = execution.get("output_qcow2_path") or conversion.get("output_path")
    if not isinstance(qcow2_path, str) or not qcow2_path.strip():
        raise OpenStackDeploymentError("Missing QCOW2 path in conversion metadata for OpenStack upload.")

    cloud = getattr(settings, "OPENSTACK_CLOUD_NAME", "openstack")
    conn = connect_openstack(cloud=cloud)

    os_meta = metadata.get("openstack", {}) if isinstance(metadata.get("openstack"), dict) else {}
    names = build_openstack_names(job.vm_name, job.id)

    flavor = map_vmware_to_flavor(conn, discovered_vm.cpu, discovered_vm.ram)
    preferred_network = getattr(settings, "OPENSTACK_DEFAULT_NETWORK", "") or None
    network = select_default_network(conn, preferred_name=preferred_network)

    image_id = ensure_uploaded_image(
        conn,
        qcow2_path=qcow2_path,
        image_name=names["image_name"],
        existing_image_id=os_meta.get("image_id"),
        timeout_seconds=int(getattr(settings, "OPENSTACK_IMAGE_UPLOAD_TIMEOUT", 900)),
        poll_interval_seconds=int(getattr(settings, "OPENSTACK_IMAGE_UPLOAD_POLL_INTERVAL", 5)),
        retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
        retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
    )

    server_id = ensure_server_booted(
        conn,
        server_name=names["server_name"],
        image_id=image_id,
        flavor_id=flavor.id,
        network_id=network.id,
        existing_server_id=os_meta.get("server_id"),
        retries=int(getattr(settings, "OPENSTACK_API_RETRIES", 2)),
        retry_delay_seconds=int(getattr(settings, "OPENSTACK_API_RETRY_DELAY", 3)),
    )

    os_meta.update(
        {
            "cloud": cloud,
            "image_id": image_id,
            "image_name": names["image_name"],
            "flavor_id": flavor.id,
            "flavor_name": flavor.name,
            "network_id": network.id,
            "network_name": network.name,
            "server_id": server_id,
            "server_name": names["server_name"],
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

    if job.status == MigrationJob.Status.DEPLOYED and job.can_transition_to(MigrationJob.Status.VERIFIED):
        job.transition(MigrationJob.Status.VERIFIED)

    job.conversion_metadata = metadata
    job.save(update_fields=["status", "conversion_metadata", "updated_at"])

    return {
        "job_id": job.id,
        "result": "deployed",
        "status": job.status,
        "image_id": image_id,
        "server_id": server_id,
        "flavor": {"id": flavor.id, "name": flavor.name},
        "network": {"id": network.id, "name": network.name},
    }


@shared_task(name="migrations.start_migration", max_retries=0, acks_late=True)
def start_migration(job_id: int) -> dict[str, Any]:
    """Migration starter with conversion and optional OpenStack deployment."""

    try:
        job = MigrationJob.objects.get(id=job_id)
    except MigrationJob.DoesNotExist:
        logger.error("migration.start missing job", extra={"job_id": job_id})
        return {"job_id": job_id, "result": "missing"}

    try:
        logger.info(
            "migration.start begin",
            extra={"job_id": job.id, "vm_name": job.vm_name, "status": job.status},
        )

        discovered_vm: DiscoveredVM | None = None

        with transaction.atomic():
            if job.status == MigrationJob.Status.PENDING:
                job.transition(MigrationJob.Status.DISCOVERED)

            if job.status == MigrationJob.Status.DISCOVERED:
                job.transition(MigrationJob.Status.CONVERTING)

            if job.status == MigrationJob.Status.CONVERTING:
                discovered_vm = _find_discovered_vm_for_job(job)
                plan = plan_vmware_conversion(discovered_vm, output_dir=settings.MIGRATION_OUTPUT_DIR)

                validation = {
                    "checked_paths": [],
                    "errors": [],
                    "skipped": False,
                }
                if discovered_vm.source == DiscoveredVM.Source.WORKSTATION:
                    validation = _validate_workstation_paths(plan.input_disks, plan.output_path)
                    if validation["errors"]:
                        raise ConversionPlanningError("; ".join(validation["errors"]))
                else:
                    validation["skipped"] = True
                    validation["reason"] = "esxi execution not implemented"

                if discovered_vm.source == DiscoveredVM.Source.ESXI and settings.ENABLE_REAL_CONVERSION:
                    raise ConversionPlanningError("Real ESXi conversion execution is not implemented yet.")

                real_conversion_enabled = bool(getattr(settings, "ENABLE_REAL_CONVERSION", False))
                mode = "real" if real_conversion_enabled else "dry-run"

                metadata = job.conversion_metadata if isinstance(job.conversion_metadata, dict) else {}
                previous_execution = {}
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
                if previous_execution:
                    metadata["conversion"]["execution"] = previous_execution

                prior = metadata.get("conversion", {}).get("execution", {})
                if prior.get("state") == "succeeded" and prior.get("output_qcow2_path"):
                    out = Path(prior["output_qcow2_path"])
                    if out.exists() and out.is_file():
                        if job.can_transition_to(MigrationJob.Status.UPLOADING):
                            job.transition(MigrationJob.Status.UPLOADING)
                        job.conversion_metadata = metadata
                        job.save(update_fields=["status", "conversion_metadata", "updated_at"])
                    else:
                        previous_execution = {}

                if job.status == MigrationJob.Status.CONVERTING:
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

                    exec_result = _execute_virt_v2v(plan, discovered_vm.name)
                    metadata["conversion"]["execution"] = {
                        "state": "succeeded",
                        **exec_result,
                    }

                    job.conversion_metadata = metadata
                    if job.can_transition_to(MigrationJob.Status.UPLOADING):
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

    except (OpenStackDeploymentError, ConversionPlanningError, InvalidTransitionError, PermissionError, OSError, subprocess.SubprocessError) as exc:
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
def discover_vmware_vms(include_workstation: bool = True, include_esxi: bool = True) -> dict[str, Any]:
    """Discover VMs from configured VMware sources and upsert DiscoveredVM rows."""

    now = timezone.now()
    result: dict[str, Any] = {
        "workstation": {"discovered": 0, "upserted": 0, "errors": []},
        "esxi": {"discovered": 0, "upserted": 0, "errors": []},
    }

    def upsert_many(source: str, items: list[dict[str, Any]]) -> int:
        upserted = 0
        for item in items:
            defaults = {
                "cpu": item.get("cpu"),
                "ram": item.get("ram"),
                "disks": item.get("disks", []),
                "power_state": item.get("power_state") or "",
                "last_seen": now,
            }
            DiscoveredVM.objects.update_or_create(
                name=item["name"],
                source=source,
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
            esxi_items = ESXiVMwareClient.from_env().discover_vms()
            result["esxi"]["discovered"] = len(esxi_items)
            result["esxi"]["upserted"] = upsert_many(DiscoveredVM.Source.ESXI, esxi_items)
        except VMwareClientError as exc:
            result["esxi"]["errors"].append(str(exc))

    return result
