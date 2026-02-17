"""OpenStack deployment helpers for migration jobs."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Any, Callable

import openstack
from keystoneauth1 import exceptions as ks_exceptions
from openstack import exceptions as os_exceptions
from openstack.config import OpenStackConfig
from openstack.connection import Connection


class OpenStackDeploymentError(Exception):
    """Raised when OpenStack deployment steps fail."""


@dataclass
class FlavorChoice:
    id: str
    name: str
    vcpus: int
    ram: int


def _sanitize_name(value: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]", "-", value).strip("-._")
    return clean or "vm"


def _retry_call(operation_name: str, attempts: int, delay_seconds: int, fn: Callable[[], Any]):
    last_exc: Exception | None = None
    for idx in range(max(1, attempts)):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if idx >= attempts - 1:
                break
            time.sleep(max(0, delay_seconds))
    raise OpenStackDeploymentError(f"{operation_name} failed after {attempts} attempts: {last_exc}") from last_exc


def _bool_from_env(value: str | None) -> bool | None:
    if value is None:
        return None
    v = value.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _connect_kwargs_from_env() -> dict[str, Any] | None:
    """Build auth kwargs from OS_* env vars (preferred for DevStack setups)."""
    auth_url = os.environ.get("OS_AUTH_URL", "").strip() or None
    if not auth_url:
        return None

    kwargs: dict[str, Any] = {
        "auth_url": auth_url,
        "username": os.environ.get("OS_USERNAME", "").strip() or None,
        "password": os.environ.get("OS_PASSWORD", "").strip() or None,
        "project_name": os.environ.get("OS_PROJECT_NAME", "").strip() or None,
        "user_domain_name": os.environ.get("OS_USER_DOMAIN_NAME", "").strip() or "Default",
        "project_domain_name": os.environ.get("OS_PROJECT_DOMAIN_NAME", "").strip() or "Default",
        "region_name": os.environ.get("OS_REGION_NAME", "").strip() or None,
        "interface": os.environ.get("OS_INTERFACE", "").strip() or None,
        "identity_api_version": os.environ.get("OS_IDENTITY_API_VERSION", "").strip() or None,
    }

    verify = _bool_from_env(os.environ.get("OS_VERIFY"))
    if verify is not None:
        kwargs["verify"] = verify

    image_endpoint_override = os.environ.get("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE", "").strip() or None
    if image_endpoint_override:
        kwargs["image_endpoint_override"] = image_endpoint_override

    return kwargs


def connect_openstack(cloud: str = "openstack", auth_overrides: dict[str, Any] | None = None):
    try:
        if isinstance(auth_overrides, dict) and auth_overrides:
            conn = openstack.connect(
                cloud=None,
                load_yaml_config=False,
                load_envvars=False,
                app_name="vm-migrator",
                app_version="1",
                **auth_overrides,
            )
            conn.authorize()
            return conn

        env_kwargs = _connect_kwargs_from_env()
        if env_kwargs:
            conn = openstack.connect(
                cloud=None,
                load_yaml_config=False,
                load_envvars=False,
                app_name="vm-migrator",
                app_version="1",
                **env_kwargs,
            )
            conn.authorize()
            return conn

        image_endpoint_override = os.environ.get("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE", "").strip() or None
        if image_endpoint_override:
            # DevStack often publishes a public Glance endpoint as http://HOST/image (apache proxy),
            # which can reject PUT /v2/images/<id>/file with HTTP 415. Override to talk to Glance directly.
            cfg = OpenStackConfig(load_yaml_config=True, load_envvars=True)
            region = cfg.get_one_cloud(cloud=cloud)
            region.config["image_endpoint_override"] = image_endpoint_override
            conn = Connection(config=region)
        else:
            conn = openstack.connect(cloud=cloud)
        conn.authorize()
        return conn
    except (os_exceptions.ConfigException, os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
        raise OpenStackDeploymentError(f"OpenStack connection failed for cloud '{cloud}': {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        raise OpenStackDeploymentError(f"Unexpected OpenStack connection error: {exc}") from exc


def map_vmware_to_flavor(conn, cpu: int | None, ram_mb: int | None) -> FlavorChoice:
    if not cpu or not ram_mb:
        raise OpenStackDeploymentError(
            f"VM CPU/RAM values are required for flavor mapping. Received cpu={cpu}, ram={ram_mb}."
        )

    flavors = list(conn.compute.flavors())
    if not flavors:
        raise OpenStackDeploymentError("No flavors available in OpenStack project.")

    exact = [f for f in flavors if int(getattr(f, "vcpus", 0)) == cpu and int(getattr(f, "ram", 0)) == ram_mb]
    if exact:
        picked = sorted(exact, key=lambda f: str(getattr(f, "name", "")))[0]
        return FlavorChoice(id=picked.id, name=picked.name, vcpus=int(picked.vcpus), ram=int(picked.ram))

    sufficient = [
        f
        for f in flavors
        if int(getattr(f, "vcpus", 0)) >= cpu and int(getattr(f, "ram", 0)) >= ram_mb
    ]
    if not sufficient:
        raise OpenStackDeploymentError(
            f"No suitable flavor found for cpu={cpu}, ram_mb={ram_mb}."
        )

    picked = sorted(
        sufficient,
        key=lambda f: (
            int(getattr(f, "vcpus", 0)),
            int(getattr(f, "ram", 0)),
            int(getattr(f, "disk", 0) or 0),
            str(getattr(f, "name", "")),
        ),
    )[0]
    return FlavorChoice(id=picked.id, name=picked.name, vcpus=int(picked.vcpus), ram=int(picked.ram))


def get_flavor_choice_by_id(conn, flavor_id: str) -> FlavorChoice:
    flavor = conn.compute.find_flavor(flavor_id, ignore_missing=True)
    if flavor is None:
        raise OpenStackDeploymentError(f"Flavor '{flavor_id}' not found.")
    return FlavorChoice(id=flavor.id, name=flavor.name, vcpus=int(flavor.vcpus), ram=int(flavor.ram))


def select_default_network(conn, preferred_name: str | None = None, preferred_id: str | None = None):
    networks = list(conn.network.networks())
    if not networks:
        raise OpenStackDeploymentError("No networks available for server boot.")

    if preferred_id:
        preferred = conn.network.find_network(preferred_id, ignore_missing=True)
        if preferred is None:
            raise OpenStackDeploymentError(f"Preferred network '{preferred_id}' not found.")
        return preferred

    if preferred_name:
        preferred = next((n for n in networks if getattr(n, "name", None) == preferred_name), None)
        if preferred is None:
            raise OpenStackDeploymentError(f"Preferred network '{preferred_name}' not found.")
        return preferred

    non_external = [n for n in networks if not bool(getattr(n, "is_router_external", False))]
    if non_external:
        return sorted(non_external, key=lambda n: str(getattr(n, "name", "")))[0]

    return sorted(networks, key=lambda n: str(getattr(n, "name", "")))[0]


def ensure_uploaded_image(
    conn,
    *,
    qcow2_path: str,
    image_name: str,
    disk_format: str = "qcow2",
    existing_image_id: str | None = None,
    timeout_seconds: int = 900,
    poll_interval_seconds: int = 5,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    path = Path(qcow2_path).expanduser()
    if not path.exists() or not path.is_file():
        raise OpenStackDeploymentError(f"Disk artifact not found for upload: {path}")
    if disk_format not in {"qcow2", "raw"}:
        raise OpenStackDeploymentError(f"Unsupported Glance disk format '{disk_format}'. Use qcow2 or raw.")

    if existing_image_id:
        existing = conn.image.find_image(existing_image_id, ignore_missing=True)
        if existing is not None:
            return existing.id

    existing_by_name = conn.image.find_image(image_name, ignore_missing=True)
    if existing_by_name is not None:
        return existing_by_name.id

    # NOTE: `conn.image.upload_image(...)` is deprecated in openstacksdk and does not
    # accept a `filename=` argument (it expects `data=`). Using it will create a queued
    # image with a 0-byte backing file. Use `create_image(filename=...)` instead.
    image = _retry_call(
        "image upload",
        retries,
        retry_delay_seconds,
        lambda: conn.image.create_image(
            image_name,
            filename=str(path),
            disk_format=disk_format,
            container_format="bare",
            visibility="private",
            wait=False,
            timeout=max(1, timeout_seconds),
            validate_checksum=False,
        ),
    )

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = conn.image.get_image(image.id)
        status = str(getattr(current, "status", "")).lower()
        if status == "active":
            return current.id
        if status in {"killed", "deleted", "error"}:
            raise OpenStackDeploymentError(
                f"Uploaded image '{image_name}' entered terminal status '{status}'."
            )
        time.sleep(max(1, poll_interval_seconds))

    raise OpenStackDeploymentError(f"Timed out waiting for image '{image_name}' to become active.")


def ensure_server_booted(
    conn,
    *,
    server_name: str,
    image_id: str,
    flavor_id: str,
    network_id: str,
    fixed_ip: str | None = None,
    existing_server_id: str | None = None,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    if existing_server_id:
        existing = conn.compute.find_server(existing_server_id, ignore_missing=True)
        if existing is not None:
            return existing.id

    existing_by_name = conn.compute.find_server(server_name, ignore_missing=True)
    if existing_by_name is not None:
        return existing_by_name.id

    network_payload = {"uuid": network_id}
    if fixed_ip:
        network_payload["fixed_ip"] = fixed_ip

    server = _retry_call(
        "server boot",
        retries,
        retry_delay_seconds,
        lambda: conn.compute.create_server(
            name=server_name,
            image_id=image_id,
            flavor_id=flavor_id,
            networks=[network_payload],
        ),
    )

    return server.id


def ensure_server_booted_from_volume(
    conn,
    *,
    server_name: str,
    boot_volume_id: str,
    flavor_id: str,
    network_id: str,
    fixed_ip: str | None = None,
    existing_server_id: str | None = None,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    if existing_server_id:
        existing = conn.compute.find_server(existing_server_id, ignore_missing=True)
        if existing is not None:
            return existing.id

    existing_by_name = conn.compute.find_server(server_name, ignore_missing=True)
    if existing_by_name is not None:
        return existing_by_name.id

    network_payload = {"uuid": network_id}
    if fixed_ip:
        network_payload["fixed_ip"] = fixed_ip

    block_device_mapping_v2 = [
        {
            "uuid": boot_volume_id,
            "source_type": "volume",
            "destination_type": "volume",
            "boot_index": 0,
            "delete_on_termination": False,
        }
    ]

    server = _retry_call(
        "server boot from volume",
        retries,
        retry_delay_seconds,
        lambda: conn.compute.create_server(
            name=server_name,
            image_id=None,
            flavor_id=flavor_id,
            networks=[network_payload],
            block_device_mapping_v2=block_device_mapping_v2,
        ),
    )

    return server.id


def ensure_volume_from_image(
    conn,
    *,
    volume_name: str,
    image_id: str,
    existing_volume_id: str | None = None,
    size_gb: int | None = None,
    timeout_seconds: int = 900,
    poll_interval_seconds: int = 5,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    if existing_volume_id:
        existing = conn.block_storage.find_volume(existing_volume_id, ignore_missing=True)
        if existing is not None:
            return existing.id

    existing_by_name = conn.block_storage.find_volume(volume_name, ignore_missing=True)
    if existing_by_name is not None:
        return existing_by_name.id

    if size_gb is None:
        image = conn.image.get_image(image_id)
        image_size = int(getattr(image, "size", 0) or 0)
        # For sparse qcow2, Glance `size` can be very small while `virtual_size`
        # reflects the provisioned disk capacity needed by Cinder.
        virtual_size = int(getattr(image, "virtual_size", 0) or 0)
        min_disk_gb = int(getattr(image, "min_disk", 0) or 0)
        bytes_gb = max(
            int(ceil(image_size / (1024 ** 3))) if image_size > 0 else 0,
            int(ceil(virtual_size / (1024 ** 3))) if virtual_size > 0 else 0,
        )
        size_gb = max(1, min_disk_gb, bytes_gb)

    volume = _retry_call(
        "volume create",
        retries,
        retry_delay_seconds,
        lambda: conn.block_storage.create_volume(
            name=volume_name,
            image_id=image_id,
            size=size_gb,
        ),
    )

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = conn.block_storage.get_volume(volume.id)
        status = str(getattr(current, "status", "")).lower()
        if status == "available":
            return current.id
        if status in {"error", "error_extending"}:
            raise OpenStackDeploymentError(
                f"Volume '{volume_name}' entered terminal status '{status}'."
            )
        time.sleep(max(1, poll_interval_seconds))

    raise OpenStackDeploymentError(f"Timed out waiting for volume '{volume_name}' to become available.")


def ensure_empty_volume(
    conn,
    *,
    volume_name: str,
    size_gb: int,
    existing_volume_id: str | None = None,
    timeout_seconds: int = 900,
    poll_interval_seconds: int = 5,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    if size_gb < 1:
        raise OpenStackDeploymentError(f"Volume '{volume_name}' size must be >= 1GB.")

    if existing_volume_id:
        existing = conn.block_storage.find_volume(existing_volume_id, ignore_missing=True)
        if existing is not None:
            return existing.id

    existing_by_name = conn.block_storage.find_volume(volume_name, ignore_missing=True)
    if existing_by_name is not None:
        return existing_by_name.id

    volume = _retry_call(
        "empty volume create",
        retries,
        retry_delay_seconds,
        lambda: conn.block_storage.create_volume(
            name=volume_name,
            size=size_gb,
        ),
    )

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = conn.block_storage.get_volume(volume.id)
        status = str(getattr(current, "status", "")).lower()
        if status == "available":
            return current.id
        if status in {"error", "error_extending"}:
            raise OpenStackDeploymentError(
                f"Volume '{volume_name}' entered terminal status '{status}'."
            )
        time.sleep(max(1, poll_interval_seconds))

    raise OpenStackDeploymentError(f"Timed out waiting for volume '{volume_name}' to become available.")


def attach_volume_to_server(
    conn,
    *,
    server_id: str,
    volume_id: str,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    server = conn.compute.get_server(server_id)
    existing_attachments = getattr(server, "attached_volumes", None) or []
    if any(str(att.get("id")) == str(volume_id) for att in existing_attachments if isinstance(att, dict)):
        return "already_attached"

    _retry_call(
        "volume attachment",
        retries,
        retry_delay_seconds,
        lambda: conn.compute.create_volume_attachment(
            server,
            volumeId=volume_id,
        ),
    )
    return "attached"


def verify_server_active(
    conn,
    *,
    server_id: str,
    timeout_seconds: int = 900,
    poll_interval_seconds: int = 10,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        server = conn.compute.get_server(server_id)
        status = str(getattr(server, "status", "")).upper()
        if status == "ACTIVE":
            return status
        if status == "ERROR":
            raise OpenStackDeploymentError(f"Server '{server_id}' entered ERROR state.")
        time.sleep(max(1, poll_interval_seconds))

    raise OpenStackDeploymentError(
        f"Timed out waiting for server '{server_id}' to reach ACTIVE state."
    )


def delete_server_if_exists(conn, server_id: str) -> str:
    server = conn.compute.find_server(server_id, ignore_missing=True)
    if server is None:
        return "not_found"

    conn.compute.delete_server(server.id, ignore_missing=True)
    return "deleted"


def delete_image_if_exists(conn, image_id: str) -> str:
    image = conn.image.find_image(image_id, ignore_missing=True)
    if image is None:
        return "not_found"

    conn.image.delete_image(image.id, ignore_missing=True)
    return "deleted"


def delete_volume_if_exists(conn, volume_id: str) -> str:
    volume = conn.block_storage.find_volume(volume_id, ignore_missing=True)
    if volume is None:
        return "not_found"

    conn.block_storage.delete_volume(volume.id, ignore_missing=True, force=True)
    return "deleted"


def build_openstack_names(vm_name: str, job_id: int) -> dict[str, str]:
    safe = _sanitize_name(vm_name)
    return {
        "image_name": f"vm-migrator-{job_id}-{safe}",
        "server_name": f"vm-migrator-{job_id}-{safe}",
    }
