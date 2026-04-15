"""OpenStack deployment helpers for migration jobs."""

from __future__ import annotations

import os
import re
import socket
import time
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import openstack
from django.conf import settings
from keystoneauth1 import exceptions as ks_exceptions
from openstack import exceptions as os_exceptions
from openstack.config import OpenStackConfig
from openstack.connection import Connection


class OpenStackDeploymentError(Exception):
    """Raised when OpenStack deployment steps fail."""


_VERSIONED_IMAGE_PATH_RE = re.compile(r"/v\d+(?:\.\d+)?/?$")


@dataclass
class FlavorChoice:
    id: str
    name: str
    vcpus: int
    ram: int


@dataclass
class FloatingIPAssignment:
    address: str
    id: str | None
    port_id: str | None
    status: str
    mode: str
    external_network_id: str | None
    external_network_name: str | None
    reused_existing: bool
    ssh_command_example: str


@dataclass(frozen=True)
class SecurityGroupRuleSpec:
    direction: str
    ether_type: str
    protocol: str | None = None
    port_range_min: int | None = None
    port_range_max: int | None = None
    remote_ip_prefix: str | None = None


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
        kwargs["image_endpoint_override"] = _normalize_image_endpoint_override(image_endpoint_override)

    return kwargs


def _normalize_image_endpoint_override(endpoint: str | None) -> str | None:
    """Ensure Glance endpoint overrides point at a concrete API version."""
    if not isinstance(endpoint, str):
        return None

    value = endpoint.strip()
    if not value:
        return None

    parsed = urlsplit(value)
    path = (parsed.path or "").rstrip("/")
    if _VERSIONED_IMAGE_PATH_RE.search(path):
        normalized_path = path
    else:
        normalized_path = f"{path}/v2" if path else "/v2"

    return urlunsplit(parsed._replace(path=normalized_path))


def _apply_image_endpoint_override(conn: Connection, endpoint: str | None) -> None:
    normalized = _normalize_image_endpoint_override(endpoint)
    if not normalized:
        return
    conn.config.config["image_endpoint_override"] = normalized
    conn.config.config["image_api_version"] = "2"


def _is_endpoint_reachable(endpoint: str | None, timeout_seconds: float = 1.0) -> bool:
    normalized = _normalize_image_endpoint_override(endpoint)
    if not normalized:
        return False

    parsed = urlsplit(normalized)
    host = parsed.hostname
    if not host:
        return False

    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _merge_image_endpoint_override(connect_kwargs: dict[str, Any]) -> dict[str, Any]:
    merged = dict(connect_kwargs)
    configured_override = _normalize_image_endpoint_override(merged.get("image_endpoint_override"))
    env_override = _normalize_image_endpoint_override(os.environ.get("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE"))
    effective_override = configured_override
    if not effective_override and _is_endpoint_reachable(env_override):
        effective_override = env_override
    if effective_override:
        merged["image_endpoint_override"] = effective_override
        merged.setdefault("image_api_version", "2")
    return merged


def _auto_fix_image_endpoint(conn: Connection) -> None:
    """Promote unversioned Glance catalog endpoints to /v2 for older setups."""
    configured_override = conn.config.get_endpoint("image")
    if configured_override:
        _apply_image_endpoint_override(conn, configured_override)
        return

    try:
        catalog_endpoint = conn.endpoint_for("image")
    except Exception:  # noqa: BLE001
        return

    normalized = _normalize_image_endpoint_override(catalog_endpoint)
    if normalized and normalized != catalog_endpoint:
        _apply_image_endpoint_override(conn, normalized)


def connect_openstack(cloud: str = "openstack", auth_overrides: dict[str, Any] | None = None):
    try:
        if isinstance(auth_overrides, dict) and auth_overrides:
            connect_kwargs = _merge_image_endpoint_override(auth_overrides)
            conn = openstack.connect(
                cloud=None,
                load_yaml_config=False,
                load_envvars=False,
                app_name="vm-migrator",
                app_version="1",
                **connect_kwargs,
            )
            conn.authorize()
            _auto_fix_image_endpoint(conn)
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
            _auto_fix_image_endpoint(conn)
            return conn

        image_endpoint_override = os.environ.get("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE", "").strip() or None
        if image_endpoint_override:
            # DevStack often publishes a public Glance endpoint as http://HOST/image (apache proxy),
            # which can reject PUT /v2/images/<id>/file with HTTP 415. Override to talk to Glance directly.
            cfg = OpenStackConfig(load_yaml_config=True, load_envvars=True)
            region = cfg.get_one_cloud(cloud=cloud)
            region.config["image_endpoint_override"] = _normalize_image_endpoint_override(image_endpoint_override)
            region.config["image_api_version"] = "2"
            conn = Connection(config=region)
        else:
            conn = openstack.connect(cloud=cloud)
        conn.authorize()
        _auto_fix_image_endpoint(conn)
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


def _to_flavor_choice(flavor: Any) -> FlavorChoice:
    return FlavorChoice(
        id=str(getattr(flavor, "id", "")),
        name=str(getattr(flavor, "name", "")),
        vcpus=int(getattr(flavor, "vcpus", 0) or 0),
        ram=int(getattr(flavor, "ram", 0) or 0),
    )


def find_flavor_choice(conn, flavor_ref: str | None) -> FlavorChoice | None:
    if not isinstance(flavor_ref, str) or not flavor_ref.strip():
        return None

    ref = flavor_ref.strip()
    flavor = conn.compute.find_flavor(ref, ignore_missing=True)
    if flavor is None:
        for candidate in conn.compute.flavors():
            candidate_id = str(getattr(candidate, "id", "") or "")
            candidate_name = str(getattr(candidate, "name", "") or "")
            if ref in {candidate_id, candidate_name}:
                flavor = candidate
                break

    if flavor is None:
        return None
    return _to_flavor_choice(flavor)


def get_flavor_choice_by_id(conn, flavor_id: str) -> FlavorChoice:
    flavor = find_flavor_choice(conn, flavor_id)
    if flavor is None:
        raise OpenStackDeploymentError(f"Flavor '{flavor_id}' not found.")
    return flavor


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
    image_properties: dict[str, Any] | None = None,
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
    create_kwargs: dict[str, Any] = {
        "filename": str(path),
        "disk_format": disk_format,
        "container_format": "bare",
        "visibility": "private",
        "wait": False,
        "timeout": max(1, timeout_seconds),
        "validate_checksum": False,
    }
    if isinstance(image_properties, dict):
        for key, value in image_properties.items():
            if isinstance(key, str) and key.strip() and value is not None:
                create_kwargs[key.strip()] = value

    image = _retry_call(
        "image upload",
        retries,
        retry_delay_seconds,
        lambda: conn.image.create_image(image_name, **create_kwargs),
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

    ensure_volume_bootable(
        conn,
        volume_id=boot_volume_id,
        timeout_seconds=60,
        poll_interval_seconds=3,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
    )

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
            flavor_id=flavor_id,
            networks=[network_payload],
            block_device_mapping_v2=block_device_mapping_v2,
        ),
    )

    return server.id


def _volume_is_bootable(volume: Any) -> bool:
    value = getattr(volume, "is_bootable", None)
    if value is None:
        value = getattr(volume, "bootable", None)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if value is None:
        return False
    return bool(value)


def ensure_volume_bootable(
    conn,
    *,
    volume_id: str,
    timeout_seconds: int = 60,
    poll_interval_seconds: int = 3,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> None:
    """Ensure a Cinder volume is marked bootable before server creation."""
    volume = conn.block_storage.get_volume(volume_id)
    if _volume_is_bootable(volume):
        return

    _retry_call(
        "set volume bootable",
        retries,
        retry_delay_seconds,
        lambda: conn.block_storage.set_volume_bootable(volume_id, True),
    )

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        volume = conn.block_storage.get_volume(volume_id)
        if _volume_is_bootable(volume):
            return
        time.sleep(max(1, poll_interval_seconds))

    raise OpenStackDeploymentError(
        f"Volume '{volume_id}' did not become bootable within {timeout_seconds}s."
    )


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        parsed = _bool_from_env(value)
        if parsed is not None:
            return parsed
    return default


def _baseline_security_group_name() -> str:
    return str(getattr(settings, "OPENSTACK_BASELINE_ACCESS_SECURITY_GROUP_NAME", "") or "").strip()


def _baseline_security_group_description() -> str:
    return str(getattr(settings, "OPENSTACK_BASELINE_ACCESS_SECURITY_GROUP_DESCRIPTION", "") or "").strip()


def _baseline_security_group_rule_specs() -> list[SecurityGroupRuleSpec]:
    raw_rules = getattr(settings, "OPENSTACK_BASELINE_ACCESS_SECURITY_GROUP_RULES", []) or []
    specs: list[SecurityGroupRuleSpec] = []
    for item in raw_rules:
        if not isinstance(item, dict):
            continue
        direction = str(item.get("direction", "") or "").strip()
        ether_type = str(item.get("ether_type", "") or "").strip()
        if not direction or not ether_type:
            continue
        protocol = str(item.get("protocol", "") or "").strip() or None
        specs.append(
            SecurityGroupRuleSpec(
                direction=direction,
                ether_type=ether_type,
                protocol=protocol,
                port_range_min=item.get("port_range_min"),
                port_range_max=item.get("port_range_max"),
                remote_ip_prefix=str(item.get("remote_ip_prefix", "") or "").strip() or None,
            )
        )
    return specs


def _find_security_group_by_name(conn, *, name: str) -> Any | None:
    try:
        found = conn.network.find_security_group(name, ignore_missing=True)
    except Exception:  # noqa: BLE001
        found = None
    if found is not None:
        return found

    for candidate in conn.network.security_groups():
        if str(getattr(candidate, "name", "") or "") == name:
            return candidate
    return None


def _list_security_group_rules(conn, *, security_group_id: str) -> list[Any]:
    try:
        return list(conn.network.security_group_rules(security_group_id=security_group_id))
    except TypeError:
        return [
            rule
            for rule in conn.network.security_group_rules()
            if str(getattr(rule, "security_group_id", "") or "") == str(security_group_id)
        ]


def _security_group_rule_matches(rule: Any, spec: SecurityGroupRuleSpec) -> bool:
    return (
        str(getattr(rule, "direction", "") or "") == spec.direction
        and str(getattr(rule, "ether_type", "") or "") == spec.ether_type
        and str(getattr(rule, "protocol", "") or "") == str(spec.protocol or "")
        and (getattr(rule, "port_range_min", None) == spec.port_range_min)
        and (getattr(rule, "port_range_max", None) == spec.port_range_max)
        and str(getattr(rule, "remote_ip_prefix", "") or "") == str(spec.remote_ip_prefix or "")
    )


def _ensure_security_group_rule(
    conn,
    *,
    security_group_id: str,
    spec: SecurityGroupRuleSpec,
    retries: int,
    retry_delay_seconds: int,
) -> None:
    rules = _list_security_group_rules(conn, security_group_id=security_group_id)
    if any(_security_group_rule_matches(rule, spec) for rule in rules):
        return

    payload: dict[str, Any] = {
        "security_group_id": security_group_id,
        "direction": spec.direction,
        "ether_type": spec.ether_type,
    }
    if spec.protocol:
        payload["protocol"] = spec.protocol
    if spec.port_range_min is not None:
        payload["port_range_min"] = spec.port_range_min
    if spec.port_range_max is not None:
        payload["port_range_max"] = spec.port_range_max
    if spec.remote_ip_prefix:
        payload["remote_ip_prefix"] = spec.remote_ip_prefix

    last_exc: Exception | None = None
    for idx in range(max(1, retries)):
        try:
            created = conn.network.create_security_group_rule(**payload)
            if created is not None:
                rules.append(created)
            return
        except os_exceptions.ConflictException as exc:
            # Treat "already exists" conflicts as success to avoid rollback on racing workers.
            message = str(exc).lower()
            if "already exists" in message:
                return
            # If we got a different conflict, re-check before retrying.
            rules = _list_security_group_rules(conn, security_group_id=security_group_id)
            if any(_security_group_rule_matches(rule, spec) for rule in rules):
                return
            last_exc = exc
        except Exception as exc:  # noqa: BLE001
            last_exc = exc

        if idx < retries - 1:
            time.sleep(max(0, retry_delay_seconds))

    raise OpenStackDeploymentError(
        f"security group rule create failed after {retries} attempts: {last_exc}"
    ) from last_exc


def ensure_server_access_baseline(
    conn,
    *,
    server_id: str,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> str:
    if not bool(getattr(settings, "OPENSTACK_ENSURE_BASELINE_ACCESS_SECURITY_GROUP", True)):
        return ""

    security_group_name = _baseline_security_group_name()
    if not security_group_name:
        return ""

    security_group = _find_security_group_by_name(conn, name=security_group_name)
    if security_group is None:
        security_group = _retry_call(
            "security group create",
            retries,
            retry_delay_seconds,
            lambda: conn.network.create_security_group(
                name=security_group_name,
                description=_baseline_security_group_description(),
            ),
        )

    security_group_id = str(getattr(security_group, "id", "") or "")
    for spec in _baseline_security_group_rule_specs():
        _ensure_security_group_rule(
            conn,
            security_group_id=security_group_id,
            spec=spec,
            retries=retries,
            retry_delay_seconds=retry_delay_seconds,
        )

    server = conn.compute.get_server(server_id)
    attached_names = {
        str((item or {}).get("name", "") or "")
        for item in getattr(server, "security_groups", None) or []
        if isinstance(item, dict)
    }
    security_group_name = str(getattr(security_group, "name", "") or security_group_name)
    if security_group_name not in attached_names:
        _retry_call(
            "security group attach",
            retries,
            retry_delay_seconds,
            lambda: conn.compute.add_security_group_to_server(server_id, security_group_name),
        )

    return str(getattr(security_group, "id", "") or "")


def _list_server_ports(conn, server_id: str) -> list[Any]:
    return list(conn.network.ports(device_id=server_id))


def _extract_fixed_ip_addresses(port: Any) -> list[str]:
    values: list[str] = []
    for item in getattr(port, "fixed_ips", None) or []:
        if not isinstance(item, dict):
            continue
        ip_value = item.get("ip_address")
        if isinstance(ip_value, str) and ip_value.strip():
            values.append(ip_value.strip())
    return values


def _select_server_port(
    conn,
    *,
    server_id: str,
    attached_network_id: str | None = None,
    fixed_ip: str | None = None,
) -> Any:
    ports = _list_server_ports(conn, server_id)
    if not ports:
        raise OpenStackDeploymentError(f"Server '{server_id}' has no Neutron ports to bind a floating IP.")

    if attached_network_id:
        for port in ports:
            if str(getattr(port, "network_id", "") or "") == str(attached_network_id):
                return port

    if fixed_ip:
        for port in ports:
            if fixed_ip in _extract_fixed_ip_addresses(port):
                return port

    return ports[0]


def _find_external_network(
    conn,
    *,
    preferred_id: str | None = None,
    preferred_name: str | None = None,
) -> Any:
    networks = [network for network in conn.network.networks() if getattr(network, "is_router_external", False) is True]
    if not networks:
        raise OpenStackDeploymentError(
            "No external OpenStack network was found. Configure an external/provider network before requesting a floating IP."
        )

    if preferred_id:
        for network in networks:
            if str(getattr(network, "id", "") or "") == str(preferred_id):
                return network
        raise OpenStackDeploymentError(f"External network '{preferred_id}' was not found or is not marked external.")

    if preferred_name:
        matches = [network for network in networks if str(getattr(network, "name", "") or "") == str(preferred_name)]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise OpenStackDeploymentError(
                f"External network name '{preferred_name}' is ambiguous. Select the external network by ID instead."
            )
        raise OpenStackDeploymentError(f"External network '{preferred_name}' was not found or is not marked external.")

    networks.sort(
        key=lambda network: (
            0 if str(getattr(network, "name", "") or "").strip().lower() == "public" else 1,
            0 if str(getattr(network, "name", "") or "").strip().lower() == "ext-net" else 1,
            str(getattr(network, "name", "") or ""),
            str(getattr(network, "id", "") or ""),
        )
    )
    return networks[0]


def _find_floating_ip_resource(conn, *, address: str) -> Any | None:
    try:
        found = conn.network.find_ip(address, ignore_missing=True)
    except Exception:  # noqa: BLE001
        found = None
    if found is not None:
        return found

    for floating_ip in conn.network.ips():
        current_address = str(getattr(floating_ip, "floating_ip_address", "") or "")
        if current_address == address:
            return floating_ip
    return None


def _list_server_floating_ips(conn, *, server_id: str) -> list[Any]:
    port_ids = {
        str(getattr(port, "id", "") or "")
        for port in _list_server_ports(conn, server_id)
        if str(getattr(port, "id", "") or "")
    }
    attached: list[Any] = []
    for floating_ip in conn.network.ips():
        port_id = str(getattr(floating_ip, "port_id", "") or "")
        if port_id and port_id in port_ids:
            attached.append(floating_ip)
    attached.sort(key=lambda item: str(getattr(item, "floating_ip_address", "") or ""))
    return attached


def _pick_unassigned_floating_ip(conn, *, external_network_id: str) -> Any | None:
    available: list[Any] = []
    for floating_ip in conn.network.ips():
        if str(getattr(floating_ip, "floating_network_id", "") or "") != str(external_network_id):
            continue
        if getattr(floating_ip, "port_id", None):
            continue
        available.append(floating_ip)

    available.sort(key=lambda item: str(getattr(item, "floating_ip_address", "") or ""))
    return available[0] if available else None


def ensure_server_floating_ip(
    conn,
    *,
    server_id: str,
    attached_network_id: str | None = None,
    fixed_ip: str | None = None,
    floating_ip: dict[str, Any] | None = None,
    server_name: str | None = None,
    retries: int = 2,
    retry_delay_seconds: int = 3,
) -> FloatingIPAssignment | None:
    config = floating_ip if isinstance(floating_ip, dict) else {}
    mode = str(config.get("mode", "") or "").strip().lower()
    if not mode or mode == "disabled":
        return None
    if mode not in {"auto", "manual"}:
        raise OpenStackDeploymentError(
            f"Unsupported floating IP mode '{mode}'. Use 'auto', 'manual', or omit the option."
        )

    port = _select_server_port(
        conn,
        server_id=server_id,
        attached_network_id=attached_network_id,
        fixed_ip=fixed_ip,
    )

    existing_assignments = _list_server_floating_ips(conn, server_id=server_id)
    if existing_assignments:
        current = existing_assignments[0]
        current_address = str(getattr(current, "floating_ip_address", "") or "")
        if current_address:
            return FloatingIPAssignment(
                address=current_address,
                id=str(getattr(current, "id", "") or "") or None,
                port_id=str(getattr(current, "port_id", "") or "") or None,
                status="already_attached",
                mode=mode,
                external_network_id=str(getattr(current, "floating_network_id", "") or "") or None,
                external_network_name=None,
                reused_existing=True,
                ssh_command_example=f"ssh user@{current_address}",
            )

    requested_address = str(config.get("address", "") or "").strip() or None
    preferred_external_network_id = str(config.get("external_network_id", "") or "").strip() or None
    preferred_external_network_name = str(config.get("external_network_name", "") or "").strip() or None
    reuse_existing = _coerce_bool(config.get("reuse_existing"), default=True)

    floating_ip_resource = None
    external_network = None
    reused_existing_ip = False

    if requested_address:
        floating_ip_resource = _find_floating_ip_resource(conn, address=requested_address)
        if floating_ip_resource is None:
            raise OpenStackDeploymentError(f"Floating IP '{requested_address}' was not found.")

        current_port_id = str(getattr(floating_ip_resource, "port_id", "") or "") or None
        if current_port_id and current_port_id != str(getattr(port, "id", "") or ""):
            raise OpenStackDeploymentError(
                f"Floating IP '{requested_address}' is already associated with another port and cannot be reused."
            )
        external_network_id = str(getattr(floating_ip_resource, "floating_network_id", "") or "") or None
        external_network = _find_external_network(
            conn,
            preferred_id=preferred_external_network_id or external_network_id,
            preferred_name=preferred_external_network_name if not (preferred_external_network_id or external_network_id) else None,
        )
    else:
        external_network = _find_external_network(
            conn,
            preferred_id=preferred_external_network_id,
            preferred_name=preferred_external_network_name,
        )
        if reuse_existing:
            floating_ip_resource = _pick_unassigned_floating_ip(conn, external_network_id=external_network.id)
            reused_existing_ip = floating_ip_resource is not None
        if floating_ip_resource is None and mode == "manual":
            raise OpenStackDeploymentError(
                "Manual floating IP mode requested but no unassigned floating IP is available on the selected external network."
            )
        if floating_ip_resource is None:
            floating_ip_resource = _retry_call(
                "floating IP allocate",
                retries,
                retry_delay_seconds,
                lambda: conn.network.create_ip(floating_network_id=external_network.id),
            )

    floating_ip_address = str(getattr(floating_ip_resource, "floating_ip_address", "") or "") or requested_address
    if not floating_ip_address:
        raise OpenStackDeploymentError("Floating IP allocation succeeded but no floating IP address was returned.")

    current_port_id = str(getattr(floating_ip_resource, "port_id", "") or "") or None
    if current_port_id != str(getattr(port, "id", "") or ""):
        floating_ip_resource = _retry_call(
            "floating IP associate",
            retries,
            retry_delay_seconds,
            lambda: conn.network.update_ip(floating_ip_resource, port_id=port.id),
        )

    external_network_name = None
    if external_network is not None:
        external_network_name = str(getattr(external_network, "name", "") or "") or None

    return FloatingIPAssignment(
        address=floating_ip_address,
        id=str(getattr(floating_ip_resource, "id", "") or "") or None,
        port_id=str(getattr(floating_ip_resource, "port_id", "") or "") or None,
        status="associated",
        mode=mode,
        external_network_id=str(getattr(floating_ip_resource, "floating_network_id", "") or "") or (
            str(getattr(external_network, "id", "") or "") or None
        ),
        external_network_name=external_network_name,
        reused_existing=reused_existing_ip or bool(requested_address),
        ssh_command_example=f"ssh user@{floating_ip_address}",
    )


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
            existing_status = str(getattr(existing, "status", "")).lower()
            if existing_status in {"available", "in-use", "in_use"}:
                return existing.id
            if existing_status in {"error", "error_extending"}:
                raise OpenStackDeploymentError(_format_volume_error(existing, volume_name))
            return existing.id

    existing_by_name = conn.block_storage.find_volume(volume_name, ignore_missing=True)
    if existing_by_name is not None:
        existing_status = str(getattr(existing_by_name, "status", "")).lower()
        if existing_status in {"error", "error_extending"}:
            raise OpenStackDeploymentError(_format_volume_error(existing_by_name, volume_name))
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
            raise OpenStackDeploymentError(_format_volume_error(current, volume_name))
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
            existing_status = str(getattr(existing, "status", "")).lower()
            if existing_status in {"available", "in-use", "in_use"}:
                return existing.id
            if existing_status in {"error", "error_extending"}:
                raise OpenStackDeploymentError(_format_volume_error(existing, volume_name))
            return existing.id

    existing_by_name = conn.block_storage.find_volume(volume_name, ignore_missing=True)
    if existing_by_name is not None:
        existing_status = str(getattr(existing_by_name, "status", "")).lower()
        if existing_status in {"error", "error_extending"}:
            raise OpenStackDeploymentError(_format_volume_error(existing_by_name, volume_name))
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
            raise OpenStackDeploymentError(_format_volume_error(current, volume_name))
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


def wait_for_volume_attachment(
    conn,
    *,
    server_id: str,
    volume_id: str,
    timeout_seconds: int = 180,
    poll_interval_seconds: int = 5,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        server = conn.compute.get_server(server_id)
        attached = getattr(server, "attached_volumes", None) or []
        attached_ids = {
            str(item.get("id"))
            for item in attached
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        }
        volume = conn.block_storage.get_volume(volume_id)
        status = str(getattr(volume, "status", "")).lower()

        if status in {"error", "error_extending", "error_deleting", "error_restoring", "error_managing"}:
            raise OpenStackDeploymentError(
                f"Volume '{volume_id}' entered error state during attach: {status}."
            )

        if volume_id in attached_ids and status in {"in-use", "in_use"}:
            return status

        time.sleep(max(1, poll_interval_seconds))

    raise OpenStackDeploymentError(
        f"Timed out waiting for volume '{volume_id}' to attach to server '{server_id}'."
    )


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

    _prepare_volume_for_deletion(conn, volume)
    conn.block_storage.delete_volume(volume.id, ignore_missing=True, force=True)
    return "deleted"


def delete_volume_by_name_if_exists(conn, volume_name: str) -> tuple[str, str | None]:
    volume = conn.block_storage.find_volume(volume_name, ignore_missing=True)
    if volume is None:
        return "not_found", None

    _prepare_volume_for_deletion(conn, volume)
    conn.block_storage.delete_volume(volume.id, ignore_missing=True, force=True)
    return "deleted", str(volume.id)


def _prepare_volume_for_deletion(conn, volume: Any, *, timeout_seconds: int = 60, poll_interval_seconds: int = 3) -> None:
    volume_id = str(getattr(volume, "id", "") or "")
    if not volume_id:
        return

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        current = conn.block_storage.get_volume(volume_id)
        attachments = getattr(current, "attachments", None) or []
        if not attachments:
            return

        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            server_id = attachment.get("server_id")
            if not server_id:
                continue
            try:
                conn.compute.delete_volume_attachment(server_id, volume_id, ignore_missing=True)
            except Exception:  # noqa: BLE001
                continue

        time.sleep(max(1, poll_interval_seconds))

    current = conn.block_storage.get_volume(volume_id)
    attachments = getattr(current, "attachments", None) or []
    if attachments:
        raise OpenStackDeploymentError(
            f"Volume '{volume_id}' is still attached and could not be prepared for deletion."
        )


def _format_volume_error(volume: Any, volume_name: str) -> str:
    details: list[str] = []
    volume_id = getattr(volume, "id", None)
    status = str(getattr(volume, "status", "")).lower() or "unknown"

    if volume_id:
        details.append(f"id={volume_id}")
    details.append(f"status={status}")

    size = getattr(volume, "size", None)
    if size is not None:
        details.append(f"size_gb={size}")

    host = getattr(volume, "host", None)
    if host:
        details.append(f"host={host}")

    bootable = getattr(volume, "is_bootable", None)
    if bootable is not None:
        details.append(f"bootable={bootable}")

    image_id = getattr(volume, "image_id", None)
    if image_id:
        details.append(f"image_id={image_id}")

    return f"Volume '{volume_name}' entered terminal status '{status}' ({', '.join(details)})."


def build_openstack_names(vm_name: str, job_id: int) -> dict[str, str]:
    safe = _sanitize_name(vm_name)
    return {
        "image_name": f"vm-migrator-{job_id}-{safe}",
        "server_name": f"vm-migrator-{job_id}-{safe}",
    }
