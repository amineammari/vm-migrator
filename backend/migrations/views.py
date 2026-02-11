from django.db import transaction
from celery.result import AsyncResult
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import APIException
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from .models import DiscoveredVM, MigrationJob
from .openstack_client import OpenStackClient, OpenStackClientError
from .serializers import CreateMigrationFromVMwareSerializer, MigrationJobSummarySerializer
from .tasks import discover_vmware_vms, rollback_migration, start_migration


@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    return Response({"status": "ok"})


@api_view(["GET"])
@permission_classes([AllowAny])
def openstack_health(request):
    """Read-only OpenStack health summary for cloud='openstack'."""
    try:
        client = OpenStackClient(cloud="openstack")
        project_id = client.validate_connection()
        images = client.list_images()
        flavors = client.list_flavors()
        networks = client.list_networks()
        return Response(
            {
                "project_id": project_id,
                "image_count": len(images),
                "flavor_count": len(flavors),
                "network_count": len(networks),
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([AllowAny])
def openstack_images(request):
    """Read-only list of OpenStack images."""
    try:
        client = OpenStackClient(cloud="openstack")
        return Response({"items": client.list_images()}, status=status.HTTP_200_OK)
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([AllowAny])
def openstack_flavors(request):
    """Read-only list of OpenStack flavors."""
    try:
        client = OpenStackClient(cloud="openstack")
        return Response({"items": client.list_flavors()}, status=status.HTTP_200_OK)
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([AllowAny])
def openstack_networks(request):
    """Read-only list of OpenStack networks."""
    try:
        client = OpenStackClient(cloud="openstack")
        return Response({"items": client.list_networks()}, status=status.HTTP_200_OK)
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([AllowAny])
def vmware_vms(request):
    """Return discovered VMware VMs from local persistence (read-only API)."""
    qs = DiscoveredVM.objects.order_by("-last_seen", "name")
    items = [
        {
            "id": vm.id,
            "name": vm.name,
            "source": vm.source,
            "cpu": vm.cpu,
            "ram": vm.ram,
            "disks": vm.disks,
            "metadata": vm.metadata,
            "power_state": vm.power_state,
            "last_seen": vm.last_seen.isoformat(),
        }
        for vm in qs
    ]
    return Response({"items": items}, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([AllowAny])
def list_migrations(request):
    """List migration jobs for dashboard polling."""
    jobs = MigrationJob.objects.order_by("-created_at")
    return Response(MigrationJobSummarySerializer(jobs, many=True).data, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([AllowAny])
def migration_detail(request, job_id: int):
    """Return one migration job including conversion metadata."""
    try:
        job = MigrationJob.objects.get(id=job_id)
    except MigrationJob.DoesNotExist:
        return Response({"error": f"Migration job {job_id} not found."}, status=status.HTTP_404_NOT_FOUND)

    payload = MigrationJobSummarySerializer(job).data
    payload["conversion_metadata"] = job.conversion_metadata
    return Response(payload, status=status.HTTP_200_OK)


@api_view(["POST"])
@permission_classes([AllowAny])
def create_migrations_from_vmware(request):
    """Create migration jobs from selected discovered VMware VMs."""
    serializer = CreateMigrationFromVMwareSerializer(data=request.data, context={})
    serializer.is_valid(raise_exception=True)

    selected_vms = serializer.validated_data["vms"]

    active_statuses = [
        MigrationJob.Status.PENDING,
        MigrationJob.Status.DISCOVERED,
        MigrationJob.Status.CONVERTING,
        MigrationJob.Status.UPLOADING,
        MigrationJob.Status.DEPLOYED,
    ]

    created_jobs = []
    skipped_jobs = []

    try:
        with transaction.atomic():
            for selected_vm in selected_vms:
                vm_name = selected_vm["name"]
                source = selected_vm["source"]

                candidates = MigrationJob.objects.filter(
                    vm_name=vm_name, status__in=active_statuses
                ).order_by("-created_at")
                existing_job = None
                for candidate in candidates:
                    meta = candidate.conversion_metadata if isinstance(candidate.conversion_metadata, dict) else {}
                    existing_source = meta.get("selected_source")
                    if existing_source in (None, source):
                        existing_job = candidate
                        break

                if existing_job:
                    skipped_jobs.append(
                        {
                            "vm_name": vm_name,
                            "source": source,
                            "job_id": existing_job.id,
                            "status": existing_job.status,
                            "reason": "already in progress",
                        }
                    )
                    continue

                job = MigrationJob.objects.create(
                    vm_name=vm_name,
                    status=MigrationJob.Status.PENDING,
                    conversion_metadata={"selected_source": source},
                )
                created_jobs.append(
                    {
                        **MigrationJobSummarySerializer(job).data,
                        "source": source,
                    }
                )

                # Trigger async pipeline stub (PENDING -> DISCOVERED).
                start_migration.delay(job.id)
    except Exception as exc:
        raise APIException(f"Failed to create migration jobs: {exc}") from exc

    return Response(
        {
            "created_jobs": created_jobs,
            "skipped_jobs": skipped_jobs,
        },
        status=status.HTTP_201_CREATED,
    )


@api_view(["POST"])
@permission_classes([AllowAny])
def discover_now(request):
    """
    Enqueue a discovery run immediately (async) and return the Celery task id.

    Optional JSON body:
      - include_workstation: bool (default true)
      - include_esxi: bool (default true)
    """
    body = request.data if isinstance(request.data, dict) else {}
    include_workstation = bool(body.get("include_workstation", True))
    include_esxi = bool(body.get("include_esxi", True))

    async_result = discover_vmware_vms.delay(
        include_workstation=include_workstation,
        include_esxi=include_esxi,
    )
    return Response(
        {
            "task_id": async_result.id,
            "queued": True,
            "include_workstation": include_workstation,
            "include_esxi": include_esxi,
        },
        status=status.HTTP_202_ACCEPTED,
    )


@api_view(["POST"])
@permission_classes([AllowAny])
def start_migration_now(request, job_id: int):
    """Enqueue start_migration(job_id) (async) and return the Celery task id."""
    async_result = start_migration.delay(job_id)
    return Response({"task_id": async_result.id, "queued": True, "job_id": job_id}, status=status.HTTP_202_ACCEPTED)


@api_view(["POST"])
@permission_classes([AllowAny])
def rollback_migration_now(request, job_id: int):
    """Enqueue rollback_migration(job_id) (async) and return the Celery task id."""
    context = request.data if isinstance(request.data, dict) else {}
    async_result = rollback_migration.delay(job_id, context=context)
    return Response({"task_id": async_result.id, "queued": True, "job_id": job_id}, status=status.HTTP_202_ACCEPTED)


@api_view(["GET"])
@permission_classes([AllowAny])
def task_status(request, task_id: str):
    """Return Celery task state and (when available) its result."""
    res = AsyncResult(task_id)
    payload = {
        "task_id": task_id,
        "state": res.state,
        "ready": res.ready(),
        "successful": res.successful() if res.ready() else None,
    }
    if res.ready():
        # Result is expected to be JSON-serializable (dict/str/etc.)
        payload["result"] = res.result
    return Response(payload, status=status.HTTP_200_OK)
