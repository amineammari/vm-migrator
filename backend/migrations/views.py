from django.db import transaction
from django.utils import timezone
from celery.result import AsyncResult
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.exceptions import APIException
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response

from .models import (
    DiscoveredVM,
    MigrationJob,
    OpenstackEndpointSession,
    OpenStackProvisioningRun,
    VmwareEndpointSession,
)
from .openstack_client import OpenStackClient, OpenStackClientError
from .serializers import (
    CreateMigrationFromVMwareSerializer,
    MigrationJobCreateSerializer,
    MigrationJobDetailSerializer,
    MigrationJobSummarySerializer,
    OpenstackEndpointConnectSerializer,
    VmwareEndpointConnectSerializer,
)
from .permissions import IsOwnerOrSuperAdmin, IsSuperAdmin
from .tasks import (
    discover_vmware_vms,
    provision_openstack_infra,
    rollback_migration,
    start_migration,
)
from .vmware_client import ESXiVMwareClient, VMwareClientError


def _resolve_openstack_endpoint_session(*, requested_id: int | None = None) -> OpenstackEndpointSession | None:
    """Return explicitly requested OpenStack session or latest passing one."""
    if isinstance(requested_id, int):
        return OpenstackEndpointSession.objects.filter(id=requested_id).first()
    return (
        OpenstackEndpointSession.objects.filter(last_test_status=OpenstackEndpointSession.TestStatus.PASSED)
        .order_by("-updated_at")
        .first()
    )


def _build_openstack_client(*, endpoint_session: OpenstackEndpointSession | None = None) -> OpenStackClient:
    if endpoint_session is not None:
        return OpenStackClient(auth_config=endpoint_session.to_connect_kwargs())
    return OpenStackClient(cloud="openstack")


def _parse_optional_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_external_network_id(client: OpenStackClient) -> str | None:
    networks = client.list_networks_detail()
    externals = [n for n in networks if n.get("is_router_external") is True and n.get("id")]
    if not externals:
        return None

    externals.sort(key=lambda n: (0 if str(n.get("name", "")).strip().lower() == "public" else 1, str(n.get("name", ""))))
    return str(externals[0]["id"])


def _terraform_overrides_from_openstack_session(session: OpenstackEndpointSession) -> dict[str, object]:
    """Map OpenStack endpoint session fields to Terraform variable names."""
    overrides: dict[str, object] = {
        "auth_url": session.auth_url,
        "username": session.username,
        "password": session.password,
        "project_name": session.project_name,
        "domain_name": session.user_domain_name or session.project_domain_name or "Default",
    }
    if session.region_name:
        overrides["region"] = session.region_name
    return overrides


def _user_is_super_admin(user) -> bool:
    return bool(user and user.is_authenticated and getattr(user, "role", None) == "SUPER_ADMIN")


def _can_access_migration(user, job: MigrationJob) -> bool:
    return _user_is_super_admin(user) or (job.user_id is not None and job.user_id == user.id)


def _status_bucket(status_value: str) -> str:
    if status_value in {MigrationJob.Status.FAILED, MigrationJob.Status.ROLLED_BACK}:
        return "failed"
    if status_value in {MigrationJob.Status.VERIFIED, MigrationJob.Status.DEPLOYED}:
        return "completed"
    return "running"


@api_view(["GET"])
@permission_classes([AllowAny])
def health(request):
    return Response({"status": "ok"})


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_health(request):
    """Read-only OpenStack health summary for selected/latest OpenStack endpoint session."""
    requested_session_id = _parse_optional_int(request.query_params.get("openstack_endpoint_session_id"))
    endpoint_session = _resolve_openstack_endpoint_session(requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session)
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
                "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_images(request):
    """Read-only list of OpenStack images for selected/latest OpenStack endpoint session."""
    requested_session_id = _parse_optional_int(request.query_params.get("openstack_endpoint_session_id"))
    endpoint_session = _resolve_openstack_endpoint_session(requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session)
        return Response(
            {
                "items": client.list_images(),
                "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_flavors(request):
    """Read-only list of OpenStack flavors for selected/latest OpenStack endpoint session."""
    requested_session_id = _parse_optional_int(request.query_params.get("openstack_endpoint_session_id"))
    endpoint_session = _resolve_openstack_endpoint_session(requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session)
        return Response(
            {
                "items": client.list_flavors(),
                "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_networks(request):
    """Read-only list of OpenStack networks for selected/latest OpenStack endpoint session."""
    requested_session_id = _parse_optional_int(request.query_params.get("openstack_endpoint_session_id"))
    endpoint_session = _resolve_openstack_endpoint_session(requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session)
        return Response(
            {
                "items": client.list_networks_detail(),
                "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_test(request):
    serializer = VmwareEndpointConnectSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    payload = serializer.validated_data
    try:
        client = ESXiVMwareClient(
            host=payload["host"],
            username=payload["username"],
            password=payload["password"],
            port=payload["port"],
            insecure=payload["insecure"],
        )
        items = client.discover_vms()
        return Response(
            {
                "ok": True,
                "message": "Connection successful.",
                "vm_count": len(items),
            },
            status=status.HTTP_200_OK,
        )
    except VMwareClientError as exc:
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_connect(request):
    serializer = VmwareEndpointConnectSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    payload = serializer.validated_data

    try:
        session = VmwareEndpointSession.objects.create(
            label=payload.get("label", ""),
            host=payload["host"],
            port=payload["port"],
            username=payload["username"],
            password=payload["password"],
            insecure=payload["insecure"],
            last_test_status=VmwareEndpointSession.TestStatus.PASSED,
            last_test_message="Connection successful.",
            last_test_at=timezone.now(),
        )
        result = discover_vmware_vms(
            include_workstation=False,
            include_esxi=True,
            vmware_endpoint_session_id=session.id,
        )
        esxi_result = result.get("esxi", {}) if isinstance(result, dict) else {}
        if esxi_result.get("errors"):
            session.last_test_status = VmwareEndpointSession.TestStatus.FAILED
            session.last_test_message = str(esxi_result.get("errors", ["Unknown error"])[0])
            session.last_test_at = timezone.now()
            session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])
            return Response(
                {"ok": False, "message": session.last_test_message},
                status=status.HTTP_400_BAD_REQUEST,
            )
    except VMwareClientError as exc:
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    items_qs = DiscoveredVM.objects.filter(
        source=DiscoveredVM.Source.ESXI,
        vmware_endpoint_session_id=session.id,
    ).order_by("-last_seen", "name")
    items = [
        {
            "id": vm.id,
            "name": vm.name,
            "source": vm.source,
            "cpu": vm.cpu,
            "ram": vm.ram,
            "disks": vm.disks,
            "nics": vm.metadata.get("nics", []) if isinstance(vm.metadata, dict) else [],
            "guest_ip": vm.metadata.get("guest", {}).get("ip_address")
            if isinstance(vm.metadata, dict) and isinstance(vm.metadata.get("guest"), dict)
            else None,
            "metadata": vm.metadata,
            "power_state": vm.power_state,
            "last_seen": vm.last_seen.isoformat(),
            "vmware_endpoint_session_id": session.id,
        }
        for vm in items_qs
    ]
    return Response(
        {
            "ok": True,
            "vmware_endpoint_session": {
                "id": session.id,
                "label": session.label,
                "host": session.host,
                "port": session.port,
                "username": session.username,
                "insecure": session.insecure,
                "last_test_status": session.last_test_status,
                "last_test_at": session.last_test_at.isoformat() if session.last_test_at else None,
            },
            "items": items,
        },
        status=status.HTTP_200_OK,
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_detail(request, session_id: int):
    session = VmwareEndpointSession.objects.filter(id=session_id).first()
    if session is None:
        return Response(
            {"error": f"VMware endpoint session '{session_id}' not found."},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(
        {
            "vmware_endpoint_session": {
                "id": session.id,
                "label": session.label,
                "host": session.host,
                "port": session.port,
                "username": session.username,
                "insecure": session.insecure,
                "last_test_status": session.last_test_status,
                "last_test_at": session.last_test_at.isoformat() if session.last_test_at else None,
            }
        },
        status=status.HTTP_200_OK,
    )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_close(request):
    requested_session_id = _parse_optional_int(request.data.get("vmware_endpoint_session_id"))
    if requested_session_id is None:
        return Response(
            {"error": "vmware_endpoint_session_id is required."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    session = VmwareEndpointSession.objects.filter(id=requested_session_id).first()
    if session is None:
        return Response(
            {"error": f"VMware endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    session.delete()
    return Response({"ok": True}, status=status.HTTP_200_OK)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_endpoint_test(request):
    serializer = OpenstackEndpointConnectSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    payload = serializer.validated_data
    connect_kwargs = {
        "auth_url": payload["auth_url"],
        "username": payload["username"],
        "password": payload["password"],
        "project_name": payload["project_name"],
        "user_domain_name": payload.get("user_domain_name", "Default"),
        "project_domain_name": payload.get("project_domain_name", "Default"),
        "verify": payload.get("verify", False),
    }
    if payload.get("region_name"):
        connect_kwargs["region_name"] = payload["region_name"]
    if payload.get("interface"):
        connect_kwargs["interface"] = payload["interface"]
    if payload.get("identity_api_version"):
        connect_kwargs["identity_api_version"] = payload["identity_api_version"]
    if payload.get("image_endpoint_override"):
        connect_kwargs["image_endpoint_override"] = payload["image_endpoint_override"]

    try:
        client = OpenStackClient(auth_config=connect_kwargs)
        project_id = client.validate_connection()
        images = client.list_images()
        flavors = client.list_flavors()
        networks = client.list_networks()
        return Response(
            {
                "ok": True,
                "message": "Connection successful.",
                "project_id": project_id,
                "image_count": len(images),
                "flavor_count": len(flavors),
                "network_count": len(networks),
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_endpoint_connect(request):
    serializer = OpenstackEndpointConnectSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    payload = serializer.validated_data

    session = OpenstackEndpointSession.objects.create(
        label=payload.get("label", ""),
        auth_url=payload["auth_url"],
        username=payload["username"],
        password=payload["password"],
        project_name=payload["project_name"],
        user_domain_name=payload.get("user_domain_name", "Default"),
        project_domain_name=payload.get("project_domain_name", "Default"),
        region_name=payload.get("region_name", ""),
        interface=payload.get("interface", ""),
        identity_api_version=payload.get("identity_api_version", ""),
        verify=payload.get("verify", False),
        image_endpoint_override=payload.get("image_endpoint_override", ""),
        last_test_status=OpenstackEndpointSession.TestStatus.PASSED,
        last_test_message="Connection successful.",
        last_test_at=timezone.now(),
    )
    try:
        client = OpenStackClient(auth_config=session.to_connect_kwargs())
        project_id = client.validate_connection()
        images = client.list_images()
        flavors = client.list_flavors()
        networks = client.list_networks_detail()
    except OpenStackClientError as exc:
        session.last_test_status = OpenstackEndpointSession.TestStatus.FAILED
        session.last_test_message = str(exc)
        session.last_test_at = timezone.now()
        session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    return Response(
        {
            "ok": True,
            "openstack_endpoint_session": {
                "id": session.id,
                "label": session.label,
                "auth_url": session.auth_url,
                "username": session.username,
                "project_name": session.project_name,
                "region_name": session.region_name,
                "verify": session.verify,
                "last_test_status": session.last_test_status,
                "last_test_at": session.last_test_at.isoformat() if session.last_test_at else None,
            },
            "project_id": project_id,
            "images": images,
            "flavors": flavors,
            "networks": networks,
        },
        status=status.HTTP_200_OK,
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_endpoint_detail(request, session_id: int):
    session = OpenstackEndpointSession.objects.filter(id=session_id).first()
    if session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{session_id}' not found."},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(
        {
            "openstack_endpoint_session": {
                "id": session.id,
                "label": session.label,
                "auth_url": session.auth_url,
                "username": session.username,
                "project_name": session.project_name,
                "region_name": session.region_name,
                "verify": session.verify,
                "last_test_status": session.last_test_status,
                "last_test_at": session.last_test_at.isoformat() if session.last_test_at else None,
            }
        },
        status=status.HTTP_200_OK,
    )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_endpoint_close(request):
    requested_session_id = _parse_optional_int(request.data.get("openstack_endpoint_session_id"))
    if requested_session_id is None:
        return Response(
            {"error": "openstack_endpoint_session_id is required."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    session = OpenstackEndpointSession.objects.filter(id=requested_session_id).first()
    if session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    session.delete()
    return Response({"ok": True}, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def vmware_vms(request):
    """Return discovered VMware VMs from local persistence (read-only API)."""
    endpoint_session_id = request.query_params.get("endpoint_session_id")
    qs = DiscoveredVM.objects.order_by("-last_seen", "name")
    if endpoint_session_id:
        try:
            qs = qs.filter(vmware_endpoint_session_id=int(endpoint_session_id))
        except (TypeError, ValueError):
            return Response({"error": "Invalid endpoint_session_id query parameter."}, status=status.HTTP_400_BAD_REQUEST)
    items = [
        {
            "id": vm.id,
            "name": vm.name,
            "source": vm.source,
            "cpu": vm.cpu,
            "ram": vm.ram,
            "disks": vm.disks,
            "nics": vm.metadata.get("nics", []) if isinstance(vm.metadata, dict) else [],
            "guest_ip": vm.metadata.get("guest", {}).get("ip_address")
            if isinstance(vm.metadata, dict) and isinstance(vm.metadata.get("guest"), dict)
            else None,
            "metadata": vm.metadata,
            "power_state": vm.power_state,
            "last_seen": vm.last_seen.isoformat(),
            "vmware_endpoint_session_id": vm.vmware_endpoint_session_id,
        }
        for vm in qs
    ]
    return Response({"items": items}, status=status.HTTP_200_OK)


@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def list_migrations(request):
    if request.method == "POST":
        serializer = MigrationJobCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        job = serializer.save(user=request.user, status=MigrationJob.Status.PENDING)
        return Response(MigrationJobDetailSerializer(job).data, status=status.HTTP_201_CREATED)

    jobs = MigrationJob.objects.select_related("user").order_by("-created_at")
    if not _user_is_super_admin(request.user):
        jobs = jobs.filter(user=request.user)
    else:
        requested_user_id = _parse_optional_int(request.query_params.get("user_id"))
        requested_username = str(request.query_params.get("username", "") or "").strip()
        if requested_user_id is not None:
            jobs = jobs.filter(user_id=requested_user_id)
        if requested_username:
            jobs = jobs.filter(user__username__icontains=requested_username)
        allowed_ordering = {
            "created_at": "created_at",
            "-created_at": "-created_at",
            "updated_at": "updated_at",
            "-updated_at": "-updated_at",
            "vm_name": "vm_name",
            "-vm_name": "-vm_name",
            "status": "status",
            "-status": "-status",
            "username": "user__username",
            "-username": "-user__username",
        }
        order_by = str(request.query_params.get("ordering", "-created_at") or "-created_at")
        jobs = jobs.order_by(allowed_ordering.get(order_by, "-created_at"))
    return Response(MigrationJobSummarySerializer(jobs, many=True).data, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def migration_detail(request, job_id: int):
    try:
        job = MigrationJob.objects.select_related("user").get(id=job_id)
    except MigrationJob.DoesNotExist:
        return Response({"error": f"Migration job {job_id} not found."}, status=status.HTTP_404_NOT_FOUND)
    if not _can_access_migration(request.user, job):
        return Response({"detail": IsOwnerOrSuperAdmin.message}, status=status.HTTP_403_FORBIDDEN)
    return Response(MigrationJobDetailSerializer(job).data, status=status.HTTP_200_OK)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def create_migrations_from_vmware(request):
    """Create migration jobs from selected discovered VMware VMs."""
    serializer = CreateMigrationFromVMwareSerializer(data=request.data, context={})
    serializer.is_valid(raise_exception=True)

    vmware_endpoint_session = serializer.context["vmware_endpoint_session"]
    openstack_endpoint_session = serializer.context["openstack_endpoint_session"]
    selected_vms = serializer.validated_data["vms"]

    active_statuses = [
        MigrationJob.Status.PENDING,
        MigrationJob.Status.DISCOVERED,
        MigrationJob.Status.PRECHECK,
        MigrationJob.Status.SNAPSHOT_CREATED,
        MigrationJob.Status.DISK_ANALYZING,
        MigrationJob.Status.CONVERTING,
        MigrationJob.Status.BLOCK_VALIDATING,
        MigrationJob.Status.UPLOADING,
        MigrationJob.Status.DEPLOYED,
    ]

    created_jobs = []
    skipped_jobs = []
    queued_job_ids: list[int] = []

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
                    existing_vmware_endpoint_session_id = meta.get("selected_vmware_endpoint_session_id")
                    if existing_source in (None, source) and existing_vmware_endpoint_session_id in (
                        None,
                        vmware_endpoint_session.id,
                    ):
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
                    user=request.user,
                    vm_name=vm_name,
                    source=source,
                    destination=openstack_endpoint_session.project_name,
                    status=MigrationJob.Status.PENDING,
                    conversion_metadata={
                        "selected_source": source,
                        "selected_vmware_endpoint_session_id": vmware_endpoint_session.id,
                        "selected_openstack_endpoint_session_id": openstack_endpoint_session.id,
                        "requested_spec": selected_vm.get("overrides", {}),
                    },
                )
                created_jobs.append(
                    {
                        **MigrationJobSummarySerializer(job).data,
                        "source": source,
                        "vmware_endpoint_session_id": vmware_endpoint_session.id,
                        "openstack_endpoint_session_id": openstack_endpoint_session.id,
                        "requested_spec": selected_vm.get("overrides", {}),
                    }
                )

                queued_job_ids.append(job.id)
            transaction.on_commit(lambda: [start_migration.delay(queued_job_id) for queued_job_id in queued_job_ids])
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
@permission_classes([IsAuthenticated])
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
    vmware_endpoint_session_id = body.get("vmware_endpoint_session_id")
    if vmware_endpoint_session_id is not None:
        try:
            vmware_endpoint_session_id = int(vmware_endpoint_session_id)
        except (TypeError, ValueError):
            return Response({"error": "vmware_endpoint_session_id must be an integer."}, status=status.HTTP_400_BAD_REQUEST)

    async_result = discover_vmware_vms.delay(
        include_workstation=include_workstation,
        include_esxi=include_esxi,
        vmware_endpoint_session_id=vmware_endpoint_session_id,
    )
    return Response(
        {
            "task_id": async_result.id,
            "queued": True,
            "include_workstation": include_workstation,
            "include_esxi": include_esxi,
            "vmware_endpoint_session_id": vmware_endpoint_session_id,
        },
        status=status.HTTP_202_ACCEPTED,
    )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def start_migration_now(request, job_id: int):
    """Enqueue start_migration(job_id) (async) and return the Celery task id."""
    async_result = start_migration.delay(job_id)
    return Response({"task_id": async_result.id, "queued": True, "job_id": job_id}, status=status.HTTP_202_ACCEPTED)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def rollback_migration_now(request, job_id: int):
    """Enqueue rollback_migration(job_id) (async) and return the Celery task id."""
    context = request.data if isinstance(request.data, dict) else {}
    async_result = rollback_migration.delay(job_id, context=context)
    return Response({"task_id": async_result.id, "queued": True, "job_id": job_id}, status=status.HTTP_202_ACCEPTED)


@api_view(["POST"])
@permission_classes([AllowAny])
def terraform_apply_now(request):
    """Enqueue terraform infrastructure provisioning task."""
    body = request.data if isinstance(request.data, dict) else {}
    var_overrides = body.get("var_overrides")
    if not isinstance(var_overrides, dict):
        var_overrides = {}
    async_result = provision_openstack_infra.delay(var_overrides=var_overrides)
    return Response({"task_id": async_result.id, "queued": True}, status=status.HTTP_202_ACCEPTED)


def _summarize_provision_result(res: AsyncResult) -> tuple[str, str]:
    raw_state = res.state
    if raw_state in {"PENDING", "RECEIVED"}:
        display_state = "QUEUED"
        message = "Queued"
    elif raw_state in {"STARTED", "RETRY"}:
        display_state = "RUNNING"
        message = "Running"
    elif raw_state in {"FAILURE", "REVOKED"}:
        display_state = "FAILED"
        message = "Provisioning failed"
    else:
        display_state = "SUCCESS"
        message = "Provisioning complete"

    if res.ready():
        result = res.result
        if isinstance(result, dict):
            result_status = str(result.get("status", "")).lower()
            if result_status == "failed":
                display_state = "FAILED"
                message = result.get("error") or "Provisioning failed"
            elif result_status == "skipped":
                display_state = "SKIPPED"
                message = result.get("reason") or "Provisioning skipped"
            elif result_status == "success":
                display_state = "SUCCESS"
                message = "Provisioning complete"
            else:
                message = result.get("reason") or result.get("error") or message
        elif result:
            message = str(result)
    return display_state, message


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_provision_now(request):
    """Enqueue OpenStack infra provisioning (async) and track the task id."""
    body = request.data if isinstance(request.data, dict) else {}
    var_overrides = body.get("var_overrides") if isinstance(body.get("var_overrides"), dict) else {}
    requested_session_id = _parse_optional_int(body.get("openstack_endpoint_session_id"))
    endpoint_session = _resolve_openstack_endpoint_session(requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": f"OpenStack endpoint session '{requested_session_id}' not found."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    effective_overrides: dict[str, object] = {}
    if endpoint_session is not None:
        effective_overrides.update(_terraform_overrides_from_openstack_session(endpoint_session))
        if "external_network_id" not in var_overrides:
            try:
                client = _build_openstack_client(endpoint_session=endpoint_session)
                external_network_id = _resolve_external_network_id(client)
            except OpenStackClientError as exc:
                return Response(
                    {"error": f"OpenStack external network lookup failed: {exc}"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if external_network_id:
                effective_overrides["external_network_id"] = external_network_id

    effective_overrides.update(var_overrides)
    if "external_network_id" not in effective_overrides:
        return Response(
            {
                "error": (
                    "Missing external_network_id for Terraform provisioning. "
                    "Provide var_overrides.external_network_id or connect/select an OpenStack endpoint "
                    "with an external network."
                )
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    async_result = provision_openstack_infra.delay(var_overrides=effective_overrides)
    run = OpenStackProvisioningRun.objects.create(
        task_id=async_result.id,
        state="QUEUED",
        message=(
            f"Queued (OpenStack session #{endpoint_session.id})"
            if endpoint_session is not None
            else "Queued"
        ),
    )
    return Response(
        {
            "run_id": run.id,
            "task_id": async_result.id,
            "state": run.state,
            "message": run.message,
            "queued": True,
            "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
        },
        status=status.HTTP_202_ACCEPTED,
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_provision_status(request):
    """Return the latest OpenStack provisioning task status."""
    run = OpenStackProvisioningRun.objects.order_by("-created_at").first()
    if run is None:
        return Response(
            {
                "state": "IDLE",
                "message": "No provisioning runs yet.",
                "task_id": None,
                "run_id": None,
                "ready": True,
                "successful": None,
            },
            status=status.HTTP_200_OK,
        )

    res = AsyncResult(run.task_id)
    display_state, message = _summarize_provision_result(res)

    if run.state != display_state or run.message != message:
        run.state = display_state
        run.message = message
        run.save(update_fields=["state", "message", "updated_at"])

    return Response(
        {
            "run_id": run.id,
            "task_id": run.task_id,
            "state": display_state,
            "message": message,
            "ready": res.ready(),
            "successful": res.successful() if res.ready() else None,
            "created_at": run.created_at.isoformat(),
            "updated_at": run.updated_at.isoformat(),
        },
        status=status.HTTP_200_OK,
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
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


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def dashboard(request):
    jobs = MigrationJob.objects.select_related("user").order_by("-created_at")
    if not _user_is_super_admin(request.user):
        jobs = jobs.filter(user=request.user)
    else:
        requested_user_id = _parse_optional_int(request.query_params.get("user_id"))
        if requested_user_id is not None:
            jobs = jobs.filter(user_id=requested_user_id)

    status_buckets = {"completed": 0, "running": 0, "failed": 0}
    for item in jobs:
        status_buckets[_status_bucket(item.status)] += 1

    return Response(
        {
            "total_migrations": jobs.count(),
            "stats_by_status": status_buckets,
            "migrations": MigrationJobSummarySerializer(jobs[:25], many=True).data,
        },
        status=status.HTTP_200_OK,
    )
