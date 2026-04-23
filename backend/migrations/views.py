from rest_framework.views import APIView
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework import status

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_routers(request):
    """List all routers for the current OpenStack session/project."""
    requested_session_id = _parse_optional_int(request.query_params.get("openstack_endpoint_session_id"))
    project_name = _parse_optional_project_name(request)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response({"error": "OpenStack endpoint session is required and must belong to you."}, status=status.HTTP_400_BAD_REQUEST)
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
        routers = client.list_routers()
        return Response({"items": routers}, status=status.HTTP_200_OK)
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_router_create(request):
    """Create a router and set its external gateway."""
    requested_session_id = _parse_optional_int(request.data.get("openstack_endpoint_session_id"))
    project_name = str(request.data.get("project_name", "")).strip() or None
    name = str(request.data.get("name", "")).strip()
    external_network_id = str(request.data.get("external_network_id", "")).strip()
    if not name or not external_network_id:
        return Response({"error": "'name' and 'external_network_id' are required."}, status=status.HTTP_400_BAD_REQUEST)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response({"error": "OpenStack endpoint session is required and must belong to you."}, status=status.HTTP_400_BAD_REQUEST)
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
        router = client.create_router(name, external_network_id)
        return Response(router, status=status.HTTP_201_CREATED)
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_router_attach_subnet(request):
    """Attach a subnet to a router as an interface."""
    requested_session_id = _parse_optional_int(request.data.get("openstack_endpoint_session_id"))
    project_name = str(request.data.get("project_name", "")).strip() or None
    router_id = str(request.data.get("router_id", "")).strip()
    subnet_id = str(request.data.get("subnet_id", "")).strip()
    if not router_id or not subnet_id:
        return Response({"error": "'router_id' and 'subnet_id' are required."}, status=status.HTTP_400_BAD_REQUEST)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response({"error": "OpenStack endpoint session is required and must belong to you."}, status=status.HTTP_400_BAD_REQUEST)
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
        result = client.add_interface_to_router(router_id, subnet_id)
        return Response(result, status=status.HTTP_200_OK)
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
from django.db import transaction
from django.utils import timezone
from celery.result import AsyncResult
from rest_framework.exceptions import APIException

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
    rollback_migration,
    start_migration,
)
from .vmware_client import ESXiProvider, VMwareClientError


def _session_queryset(user, model_cls):
    qs = model_cls.objects.all()
    if user is not None and not _user_is_super_admin(user):
        qs = qs.filter(user=user)
    return qs


def _resolve_openstack_endpoint_session(*, user, requested_id: int | None = None) -> OpenstackEndpointSession | None:
    """Return OpenStack session without user filtering."""
    qs = OpenstackEndpointSession.objects.all()
    if isinstance(requested_id, int):
        return qs.filter(id=requested_id).first()
    return qs.order_by("-created_at").first()


def _build_openstack_client(
    *,
    endpoint_session: OpenstackEndpointSession | None = None,
    project_name: str | None = None,
) -> OpenStackClient:
    if endpoint_session is None:
        return OpenStackClient()
    return OpenStackClient(auth_config=endpoint_session.to_connect_kwargs(project_name=project_name))


def _parse_optional_project_name(request) -> str | None:
    project_name = str(request.query_params.get("project_name", "") or "").strip()
    return project_name or None


def _vmware_session_payload(session: VmwareEndpointSession) -> dict[str, object]:
    return {
        "id": session.id,
        "label": session.label,
        "host": session.host,
        "port": session.port,
        "username": session.username,
        "insecure": session.insecure,
        "last_test_status": session.last_test_status,
        "last_test_message": session.last_test_message,
        "last_test_at": session.last_test_at.isoformat() if session.last_test_at else None,
        "created_at": session.created_at.isoformat() if session.created_at else None,
    }


def _openstack_session_payload(session: OpenstackEndpointSession) -> dict[str, object]:
    return {
        "id": session.id,
        "label": session.label,
        "auth_url": session.auth_url,
        "username": session.username,
        "project_name": session.project_name,
        "user_domain_name": session.user_domain_name,
        "project_domain_name": session.project_domain_name,
        "region_name": session.region_name,
        "interface": session.interface,
        "identity_api_version": session.identity_api_version,
        "verify": session.verify,
        "image_endpoint_override": session.image_endpoint_override,
        "last_test_status": session.last_test_status,
        "last_test_message": session.last_test_message,
        "last_test_at": session.last_test_at.isoformat() if session.last_test_at else None,
        "created_at": session.created_at.isoformat() if session.created_at else None,
    }


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


def _optional_openstack_images(client: OpenStackClient) -> tuple[list[dict[str, object]], str | None]:
    try:
        return client.list_images(), None
    except OpenStackClientError as exc:
        return [], str(exc)


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
    project_name = _parse_optional_project_name(request)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": "OpenStack endpoint session is required and must belong to you."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
        project_id = client.validate_connection()
        images, image_error = _optional_openstack_images(client)
        flavors = client.list_flavors()
        networks = client.list_networks()
        return Response(
            {
                "project_id": project_id,
                "image_count": len(images) if image_error is None else None,
                "flavor_count": len(flavors),
                "network_count": len(networks),
                "image_error": image_error,
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
    project_name = _parse_optional_project_name(request)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": "OpenStack endpoint session is required and must belong to you."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
        images, image_error = _optional_openstack_images(client)
        return Response(
            {
                "items": images,
                "image_error": image_error,
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
    project_name = _parse_optional_project_name(request)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": "OpenStack endpoint session is required and must belong to you."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
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
    project_name = _parse_optional_project_name(request)
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": "OpenStack endpoint session is required and must belong to you."},
            status=status.HTTP_400_BAD_REQUEST,
        )
    try:
        client = _build_openstack_client(endpoint_session=endpoint_session, project_name=project_name)
        return Response(
            {
                "items": client.list_networks_detail(),
                "external_networks": client.list_external_networks(),
                "available_floating_ips": client.list_floating_ips(available_only=True),
                "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"error": str(exc)}, status=status.HTTP_503_SERVICE_UNAVAILABLE)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def openstack_network_create(request):
    return Response({"error": "OpenstackNetworkCreateSerializer has been removed."}, status=status.HTTP_400_BAD_REQUEST)

    requested_session_id = payload.get("openstack_endpoint_session_id")
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if requested_session_id is not None and endpoint_session is None:
        return Response(
            {"error": "OpenStack endpoint session is required and must belong to you."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        client = _build_openstack_client(endpoint_session=endpoint_session)
        created = client.create_network(
            name=payload["name"],
            subnet_name=payload.get("subnet_name", ""),
            cidr=payload["cidr"],
            gateway_ip=payload.get("gateway_ip") or None,
            enable_dhcp=payload.get("enable_dhcp", True),
            allocation_pool_start=payload.get("allocation_pool_start") or None,
            allocation_pool_end=payload.get("allocation_pool_end") or None,
            dns_nameservers=payload.get("dns_nameservers", []),
        )
        networks = client.list_networks_detail()
        return Response(
            {
                "ok": True,
                "message": f"Network '{payload['name']}' created successfully.",
                "network": created,
                "items": networks,
                "openstack_endpoint_session_id": endpoint_session.id if endpoint_session else None,
            },
            status=status.HTTP_201_CREATED,
        )
    except OpenStackClientError as exc:
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_test(request):
    serializer = VmwareEndpointConnectSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    payload = serializer.validated_data
    try:
        provider_type = payload.get("type", "esxi")
        if provider_type == "vcenter":
            from .vmware_client import VCenterProvider
            client = VCenterProvider(
                host=payload["host"],
                username=payload["username"],
                password=payload["password"],
                port=payload["port"],
                insecure=payload["insecure"],
                datacenter=payload.get("datacenter") or None,
            )
        else:
            client = ESXiProvider(
                host=payload["host"],
                username=payload["username"],
                password=payload["password"],
                port=payload["port"],
                insecure=payload["insecure"],
            )
        items = client.list_vms()
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


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_list(request):
    sessions = VmwareEndpointSession.objects.all().order_by("-created_at")
    return Response({"items": [_vmware_session_payload(session) for session in sessions]}, status=status.HTTP_200_OK)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def vmware_endpoint_connect(request):
    import logging
    import traceback
    logger = logging.getLogger(__name__)

    logger.info("vmware_endpoint_connect: starting", extra={"user": request.user.id if request.user else None})

    try:
        serializer = VmwareEndpointConnectSerializer(data=request.data)
        if not serializer.is_valid():
            logger.warning("vmware_endpoint_connect: validation failed", extra={"errors": serializer.errors})
            return Response({"ok": False, "message": serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        payload = serializer.validated_data
        provider_type = payload.get("type", "esxi")

        logger.info("vmware_endpoint_connect: creating session", extra={"type": provider_type, "host": payload.get("host")})

        try:
            session = VmwareEndpointSession.objects.create(
                label=payload.get("label", ""),
                host=payload["host"],
                port=payload["port"],
                username=payload["username"],
                password=payload["password"],
                insecure=payload["insecure"],
                last_test_status=VmwareEndpointSession.TestStatus.UNKNOWN,
                user=request.user,
            )
            logger.info("vmware_endpoint_connect: session created", extra={"session_id": session.id})
        except Exception as exc:
            logger.error("vmware_endpoint_connect: session creation failed", extra={"error": str(exc), "error_type": type(exc).__name__})
            return Response({"ok": False, "message": f"Failed to create session: {exc}"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            if provider_type == "vcenter":
                logger.info("vmware_endpoint_connect: testing vCenter connection", extra={"host": payload["host"]})
                from .vmware_client import VCenterProvider
                client = VCenterProvider(
                    host=payload["host"],
                    username=payload["username"],
                    password=payload["password"],
                    port=payload["port"],
                    insecure=payload["insecure"],
                    datacenter=payload.get("datacenter", ""),
                )
                items = client.list_vms()
                logger.info("vmware_endpoint_connect: vCenter discovery complete", extra={"vm_count": len(items)})

                session.last_test_status = VmwareEndpointSession.TestStatus.PASSED
                session.last_test_message = f"Connection successful. Discovered {len(items)} VMs."
                session.last_test_at = timezone.now()
                session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])

                now = timezone.now()
                for vm in items:
                    DiscoveredVM.objects.update_or_create(
                        name=vm["name"],
                        source=DiscoveredVM.Source.VCENTER,
                        vmware_endpoint_session=session,
                        defaults={
                            "cpu": vm.get("cpu"),
                            "ram": vm.get("ram"),
                            "disks": vm.get("disks", []),
                            "metadata": vm.get("metadata", {}),
                            "power_state": vm.get("power_state") or "",
                            "last_seen": now,
                        },
                    )
            else:
                logger.info("vmware_endpoint_connect: scheduling ESXi discovery", extra={"session_id": session.id})
                discover_vmware_vms.delay(
                    include_workstation=False,
                    include_esxi=True,
                    vmware_endpoint_session_id=session.id,
                )
                logger.info("vmware_endpoint_connect: discovery task scheduled", extra={"session_id": session.id})

                session.last_test_status = VmwareEndpointSession.TestStatus.PASSED
                session.last_test_message = "Connection successful. Discovery running in background."
                session.last_test_at = timezone.now()
                session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])

        except VMwareClientError as exc:
            logger.warning("vmware_endpoint_connect: connection test failed", extra={"error": str(exc), "session_id": session.id})
            session.last_test_status = VmwareEndpointSession.TestStatus.FAILED
            session.last_test_message = str(exc)
            session.last_test_at = timezone.now()
            session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])
            return Response(
                {"ok": False, "message": str(exc)},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as exc:
            logger.error("vmware_endpoint_connect: unexpected error", extra={"error": str(exc), "error_type": type(exc).__name__, "session_id": session.id})
            session.last_test_status = VmwareEndpointSession.TestStatus.FAILED
            session.last_test_message = f"Unexpected error: {exc}"
            session.last_test_at = timezone.now()
            session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])

        logger.info("vmware_endpoint_connect: building response", extra={"session_id": session.id})

        if provider_type == "vcenter":
            items_qs = DiscoveredVM.objects.filter(
                source=DiscoveredVM.Source.VCENTER,
                vmware_endpoint_session_id=session.id,
            ).order_by("-last_seen", "name")
        else:
            items_qs = DiscoveredVM.objects.filter(
                source=DiscoveredVM.Source.ESXI,
                vmware_endpoint_session_id=session.id,
            ).order_by("-last_seen", "name")

        items = []
        for vm in items_qs:
            try:
                items.append({
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
                    "vmware_endpoint_label": session.label or f"{session.host}:{session.port}",
                })
            except Exception as exc:
                logger.warning("vmware_endpoint_connect: error serializing vm", extra={"vm_id": vm.id, "error": str(exc)})

        logger.info("vmware_endpoint_connect: returning success", extra={"session_id": session.id, "item_count": len(items)})

        return Response(
            {
                "ok": True,
                "vmware_endpoint_session": _vmware_session_payload(session),
                "items": items,
            },
            status=status.HTTP_201_CREATED,
        )

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("vmware_endpoint_connect: CRITICAL uncaught exception", extra={
            "error": str(exc),
            "error_type": type(exc).__name__,
            "traceback": tb,
        })
        return Response(
            {"ok": False, "message": f"Internal error: {exc}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            "vmware_endpoint_session": _vmware_session_payload(session)
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
            {"error": "VMware endpoint session not found or not accessible."},
            status=status.HTTP_403_FORBIDDEN,
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
        images, image_error = _optional_openstack_images(client)
        flavors = client.list_flavors()
        networks = client.list_networks()
        message = "Connection successful."
        if image_error:
            message = f"Connection successful, but the OpenStack image service is unhealthy: {image_error}"
        return Response(
            {
                "ok": True,
                "message": message,
                "project_id": project_id,
                "image_count": len(images) if image_error is None else None,
                "flavor_count": len(flavors),
                "network_count": len(networks),
                "image_error": image_error,
            },
            status=status.HTTP_200_OK,
        )
    except OpenStackClientError as exc:
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_endpoint_list(request):
    sessions = OpenstackEndpointSession.objects.all().order_by("-created_at")
    return Response({"items": [_openstack_session_payload(session) for session in sessions]}, status=status.HTTP_200_OK)


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
        user=request.user,
    )
    try:
        client = OpenStackClient(auth_config=session.to_connect_kwargs())
        project_id = client.validate_connection()
        images, image_error = _optional_openstack_images(client)
        flavors = client.list_flavors()
        networks = client.list_networks_detail()
    except OpenStackClientError as exc:
        session.last_test_status = OpenstackEndpointSession.TestStatus.FAILED
        session.last_test_message = str(exc)
        session.last_test_at = timezone.now()
        session.save(update_fields=["last_test_status", "last_test_message", "last_test_at", "updated_at"])
        return Response({"ok": False, "message": str(exc)}, status=status.HTTP_400_BAD_REQUEST)

    message = "Connection successful."
    if image_error:
        message = f"Connection successful, but the OpenStack image service is unhealthy: {image_error}"
    session.last_test_message = message
    session.last_test_at = timezone.now()
    session.save(update_fields=["last_test_message", "last_test_at", "updated_at"])

    return Response(
        {
            "ok": True,
            "message": message,
            "openstack_endpoint_session": {
                **_openstack_session_payload(session),
            },
            "project_id": project_id,
            "images": images,
            "image_error": image_error,
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
            {"error": "OpenStack endpoint session not found or not accessible."},
            status=status.HTTP_404_NOT_FOUND,
        )
    return Response(
        {
            "openstack_endpoint_session": _openstack_session_payload(session)
        },
        status=status.HTTP_200_OK,
    )


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def openstack_endpoint_projects(request, session_id: int):
    session = OpenstackEndpointSession.objects.filter(id=session_id).first()
    if session is None:
        return Response(
            {"error": "OpenStack endpoint session not found or not accessible."},
            status=status.HTTP_404_NOT_FOUND,
        )
    try:
        client = OpenStackClient(auth_config=session.to_connect_kwargs())
        projects = client.list_projects()
    except OpenStackClientError as exc:
        projects = []
        message = str(exc)
    else:
        message = ""

    if not projects:
        projects = [
            {
                "id": None,
                "name": session.project_name,
                "domain_id": None,
                "description": "",
                "is_enabled": True,
                "is_current": True,
            }
        ]

    return Response(
        {
            "items": projects,
            "message": message,
            "openstack_endpoint_session_id": session.id,
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
            {"error": "OpenStack endpoint session not found or not accessible."},
            status=status.HTTP_403_FORBIDDEN,
        )

    session.delete()
    return Response({"ok": True}, status=status.HTTP_200_OK)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def vmware_vms(request):
    """Return discovered VMware VMs from local persistence (read-only API)."""
    endpoint_session_id = request.query_params.get("endpoint_session_id")
    qs = DiscoveredVM.objects.select_related("vmware_endpoint_session").order_by("-last_seen", "name")
    if not _user_is_super_admin(request.user):
        qs = qs.filter(vmware_endpoint_session__user=request.user)
    if endpoint_session_id:
        try:
            endpoint_session_id = int(endpoint_session_id)
        except (TypeError, ValueError):
            return Response({"error": "Invalid endpoint_session_id query parameter."}, status=status.HTTP_400_BAD_REQUEST)
        qs = qs.filter(vmware_endpoint_session_id=endpoint_session_id)
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
            "vmware_endpoint_label": str(vm.vmware_endpoint_session) if vm.vmware_endpoint_session else "",
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
    serializer = CreateMigrationFromVMwareSerializer(data=request.data, context={"request": request})
    serializer.is_valid(raise_exception=True)

    vmware_endpoint_sessions = serializer.context["vmware_endpoint_sessions"]
    openstack_endpoint_session = serializer.context["openstack_endpoint_session"]
    openstack_project_name = serializer.context.get("openstack_project_name") or (
        openstack_endpoint_session.project_name if openstack_endpoint_session is not None else ""
    )
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
                selected_vmware_endpoint_session_id = selected_vm["vmware_endpoint_session_id"]
                vmware_endpoint_session = vmware_endpoint_sessions[selected_vmware_endpoint_session_id]
                source_label = vmware_endpoint_session.label or f"{vmware_endpoint_session.host}:{vmware_endpoint_session.port}"
                store_disks_locally = bool((selected_vm.get("overrides") or {}).get("store_disks_locally", False))
                if openstack_endpoint_session is None or store_disks_locally:
                    destination_label = "Local storage"
                else:
                    destination_label = openstack_endpoint_session.label or openstack_endpoint_session.auth_url
                    if openstack_project_name:
                        destination_label = f"{destination_label} / {openstack_project_name}"

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
                    source=source_label,
                    destination=destination_label,
                    status=MigrationJob.Status.PENDING,
                    conversion_metadata={
                        "selected_source": source,
                        "selected_vmware_endpoint_session_id": vmware_endpoint_session.id,
                        "selected_openstack_endpoint_session_id": openstack_endpoint_session.id if openstack_endpoint_session else None,
                        "selected_openstack_project_name": openstack_project_name,
                        "selected_vmware_label": source_label,
                        "selected_openstack_label": destination_label,
                        "requested_spec": selected_vm.get("overrides", {}),
                    },
                )
                created_jobs.append(
                    {
                        **MigrationJobSummarySerializer(job).data,
                        "source": source_label,
                        "vmware_endpoint_session_id": vmware_endpoint_session.id,
                        "openstack_endpoint_session_id": openstack_endpoint_session.id if openstack_endpoint_session else None,
                        "openstack_project_name": openstack_project_name,
                        "requested_spec": selected_vm.get("overrides", {}),
                    }
                )

                queued_job_ids.append(job.id)
    except Exception as exc:
        raise APIException(f"Failed to create migration jobs: {exc}") from exc

    for queued_job_id in queued_job_ids:
        start_migration.delay(queued_job_id)

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
    return Response({"error": "provision_openstack_infra has been removed."}, status=status.HTTP_400_BAD_REQUEST)
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
        session = VmwareEndpointSession.objects.filter(id=vmware_endpoint_session_id).first()
        if session is None:
            return Response({"error": "VMware endpoint session not found or not accessible."}, status=status.HTTP_403_FORBIDDEN)
    else:
        return Response(
            {"error": "vmware_endpoint_session_id is required for discovery."},
            status=status.HTTP_400_BAD_REQUEST,
        )

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
    try:
        job = MigrationJob.objects.select_related("user").get(id=job_id)
    except MigrationJob.DoesNotExist:
        return Response({"error": f"Migration job {job_id} not found."}, status=status.HTTP_404_NOT_FOUND)
    if not _can_access_migration(request.user, job):
        return Response({"detail": IsOwnerOrSuperAdmin.message}, status=status.HTTP_403_FORBIDDEN)

    async_result = start_migration.delay(job_id)
    return Response({"task_id": async_result.id, "queued": True, "job_id": job_id}, status=status.HTTP_202_ACCEPTED)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def rollback_migration_now(request, job_id: int):
    """Enqueue rollback_migration(job_id) (async) and return the Celery task id."""
    try:
        job = MigrationJob.objects.select_related("user").get(id=job_id)
    except MigrationJob.DoesNotExist:
        return Response({"error": f"Migration job {job_id} not found."}, status=status.HTTP_404_NOT_FOUND)
    if not _can_access_migration(request.user, job):
        return Response({"detail": IsOwnerOrSuperAdmin.message}, status=status.HTTP_403_FORBIDDEN)

    context = request.data if isinstance(request.data, dict) else {}
    async_result = rollback_migration.delay(job_id, context=context)
    return Response({"task_id": async_result.id, "queued": True, "job_id": job_id}, status=status.HTTP_202_ACCEPTED)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
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
    endpoint_session = _resolve_openstack_endpoint_session(user=request.user, requested_id=requested_session_id)
    if endpoint_session is None:
        return Response(
            {"error": "OpenStack endpoint session is required and must belong to you."},
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
        user=request.user,
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
    runs = OpenStackProvisioningRun.objects
    if not _user_is_super_admin(request.user):
        runs = runs.filter(user=request.user)
    run = runs.order_by("-created_at").first()
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
