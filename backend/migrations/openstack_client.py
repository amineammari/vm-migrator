"""OpenStack read-only client wrapper for API views."""

from __future__ import annotations

import os
from collections import defaultdict
from ipaddress import ip_address, ip_network
from typing import Any

import openstack
from keystoneauth1 import exceptions as ks_exceptions
from openstack import exceptions as os_exceptions
from openstack.config import OpenStackConfig
from openstack.connection import Connection

from .openstack_deployment import OpenStackDeploymentError, connect_openstack

class OpenStackClientError(Exception):
    """Raised when OpenStack connectivity or API reads fail."""

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

    # Keep required fields explicit so we don't accidentally pick up stale root-owned clouds.yaml.
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

    # Optional: bypass proxy endpoints for Glance.
    image_endpoint_override = os.environ.get("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE", "").strip() or None
    if image_endpoint_override:
        kwargs["image_endpoint_override"] = image_endpoint_override

    return kwargs


class OpenStackClient:
    """Small abstraction around openstacksdk using cloud='openstack'."""

    def __init__(self, cloud: str = "openstack", auth_config: dict[str, Any] | None = None) -> None:
        self.cloud = cloud
        self.auth_config = auth_config
        self._conn = self._connect()

    def _connect(self):
        try:
            # Reuse the exact same connection path as deployment tasks.
            # This prevents drift between read-only API and migration runtime behavior.
            return connect_openstack(cloud=self.cloud, auth_overrides=self.auth_config)
        except OpenStackDeploymentError as exc:
            raise OpenStackClientError(f"OpenStack authentication/configuration failed: {exc}") from exc
        except (os_exceptions.ConfigException, os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"OpenStack authentication/configuration failed: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected OpenStack client initialization error: {exc}") from exc

    def validate_connection(self) -> str:
        """Validate auth/session and return current project_id."""
        try:
            # Force auth/session resolution.
            self._conn.authorize()
            project_id = self._conn.current_project_id
            if not project_id:
                raise OpenStackClientError("OpenStack project_id is unavailable for the active cloud.")
            return project_id
        except OpenStackClientError:
            raise
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"OpenStack connection validation failed: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected OpenStack validation error: {exc}") from exc

    def list_images(self) -> list[dict[str, Any]]:
        """List available images from the image service."""
        try:
            return [
                {
                    "id": image.id,
                    "name": image.name,
                    "status": getattr(image, "status", None),
                    "visibility": getattr(image, "visibility", None),
                    "disk_format": getattr(image, "disk_format", None),
                    "container_format": getattr(image, "container_format", None),
                    "size": getattr(image, "size", None),
                }
                for image in self._conn.image.images()
            ]
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to list OpenStack images: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing OpenStack images: {exc}") from exc

    def list_flavors(self) -> list[dict[str, Any]]:
        """List available compute flavors."""
        try:
            return [
                {
                    "id": flavor.id,
                    "name": flavor.name,
                    "vcpus": getattr(flavor, "vcpus", None),
                    "ram": getattr(flavor, "ram", None),
                    "disk": getattr(flavor, "disk", None),
                    "is_public": getattr(flavor, "is_public", None),
                }
                for flavor in self._conn.compute.flavors()
            ]
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to list OpenStack flavors: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing OpenStack flavors: {exc}") from exc

    def list_networks(self) -> list[dict[str, Any]]:
        """List available tenant/provider networks (basic fields)."""
        try:
            return [
                {
                    "id": network.id,
                    "name": network.name,
                    "status": getattr(network, "status", None),
                    "is_admin_state_up": getattr(network, "is_admin_state_up", None),
                    "is_router_external": getattr(network, "is_router_external", None),
                }
                for network in self._conn.network.networks()
            ]
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to list OpenStack networks: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing OpenStack networks: {exc}") from exc

    def list_networks_detail(self) -> list[dict[str, Any]]:
        """List available tenant/provider networks with subnet pools and available IPs."""
        try:
            networks = list(self._conn.network.networks())
            subnets = list(self._conn.network.subnets())
            ports = list(self._conn.network.ports())

            used_by_subnet: dict[str, set[int]] = defaultdict(set)
            for port in ports:
                fixed_ips = getattr(port, "fixed_ips", None) or []
                for fixed in fixed_ips:
                    if not isinstance(fixed, dict):
                        continue
                    subnet_id = fixed.get("subnet_id")
                    ip_value = fixed.get("ip_address")
                    if not subnet_id or not ip_value:
                        continue
                    try:
                        used_by_subnet[subnet_id].add(int(ip_address(str(ip_value))))
                    except ValueError:
                        continue

            subnets_by_network: dict[str, list[dict[str, Any]]] = defaultdict(list)
            max_ips = int(os.environ.get("OPENSTACK_AVAILABLE_IPS_LIMIT", "512"))
            for subnet in subnets:
                subnet_id = str(getattr(subnet, "id", ""))
                network_id = getattr(subnet, "network_id", None)
                if not subnet_id or not network_id:
                    continue
                used_set = used_by_subnet.get(subnet_id, set())
                subnets_by_network[network_id].append(
                    _format_subnet_details(subnet, used_set, max_ips)
                )

            return [
                {
                    "id": network.id,
                    "name": network.name,
                    "status": getattr(network, "status", None),
                    "is_admin_state_up": getattr(network, "is_admin_state_up", None),
                    "is_router_external": getattr(network, "is_router_external", None),
                    "subnets": subnets_by_network.get(network.id, []),
                }
                for network in networks
            ]
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to list OpenStack networks: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing OpenStack networks: {exc}") from exc

    def validate_fixed_ip(self, *, network_id: str, fixed_ip: str) -> tuple[bool, str | None]:
        """Validate fixed IP against allocation pools and existing ports."""
        try:
            network = self._conn.network.find_network(network_id, ignore_missing=True)
            if network is None:
                return False, f"Network '{network_id}' not found."

            try:
                fixed_ip_value = ip_address(str(fixed_ip))
            except ValueError:
                return False, "Invalid IP address format."

            subnets = [
                subnet
                for subnet in self._conn.network.subnets()
                if getattr(subnet, "network_id", None) == network_id
            ]
            if not subnets:
                return False, "Network has no subnets."

            in_pool = False
            for subnet in subnets:
                gateway_ip = getattr(subnet, "gateway_ip", None)
                if gateway_ip and str(fixed_ip_value) == str(gateway_ip):
                    return False, "IP matches subnet gateway."

                allocation_pools = getattr(subnet, "allocation_pools", None) or []
                if allocation_pools:
                    for pool in allocation_pools:
                        if not isinstance(pool, dict):
                            continue
                        start = pool.get("start")
                        end = pool.get("end")
                        if not start or not end:
                            continue
                        try:
                            start_ip = ip_address(str(start))
                            end_ip = ip_address(str(end))
                        except ValueError:
                            continue
                        if start_ip <= fixed_ip_value <= end_ip:
                            in_pool = True
                            break
                else:
                    cidr = getattr(subnet, "cidr", None)
                    if cidr:
                        try:
                            if fixed_ip_value in ip_network(str(cidr), strict=False):
                                in_pool = True
                        except ValueError:
                            pass

                if in_pool:
                    break

            if not in_pool:
                return False, "IP is not inside any allocation pool."

            for port in self._conn.network.ports(network_id=network_id):
                fixed_ips = getattr(port, "fixed_ips", None) or []
                for fixed in fixed_ips:
                    if not isinstance(fixed, dict):
                        continue
                    if str(fixed.get("ip_address")) == str(fixed_ip_value):
                        return False, "IP is already in use."

            return True, None
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to validate fixed IP: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while validating fixed IP: {exc}") from exc


def _format_subnet_details(subnet: Any, used_ips: set[int], limit: int) -> dict[str, Any]:
    cidr = getattr(subnet, "cidr", None)
    gateway_ip = getattr(subnet, "gateway_ip", None)
    allocation_pools = getattr(subnet, "allocation_pools", None) or []
    if not allocation_pools and cidr:
        try:
            network = ip_network(str(cidr), strict=False)
            if network.num_addresses >= 4:
                allocation_pools = [
                    {"start": str(network[1]), "end": str(network[-2])}
                ]
            elif network.num_addresses == 2:
                allocation_pools = [
                    {"start": str(network[0]), "end": str(network[1])}
                ]
            elif network.num_addresses == 1:
                allocation_pools = [
                    {"start": str(network[0]), "end": str(network[0])}
                ]
        except ValueError:
            allocation_pools = []

    available_ips, available_count, truncated = _calculate_available_ips(
        allocation_pools,
        used_ips,
        gateway_ip=gateway_ip,
        limit=limit,
    )

    return {
        "id": getattr(subnet, "id", None),
        "name": getattr(subnet, "name", None),
        "cidr": cidr,
        "gateway_ip": gateway_ip,
        "allocation_pools": allocation_pools,
        "available_ips": available_ips,
        "available_ip_count": available_count,
        "available_ips_truncated": truncated,
        "used_ip_count": len(used_ips),
    }


def _calculate_available_ips(
    allocation_pools: list[dict[str, Any]],
    used_ips: set[int],
    *,
    gateway_ip: str | None,
    limit: int,
) -> tuple[list[str], int, bool]:
    gateway_int = None
    if gateway_ip:
        try:
            gateway_int = int(ip_address(str(gateway_ip)))
        except ValueError:
            gateway_int = None

    available: list[str] = []
    total_available = 0
    truncated = False

    for pool in allocation_pools:
        if not isinstance(pool, dict):
            continue
        start = pool.get("start")
        end = pool.get("end")
        if not start or not end:
            continue
        try:
            start_int = int(ip_address(str(start)))
            end_int = int(ip_address(str(end)))
        except ValueError:
            continue
        if start_int > end_int:
            start_int, end_int = end_int, start_int

        used_in_range = sum(1 for ip_int in used_ips if start_int <= ip_int <= end_int)
        gateway_in_range = 1 if gateway_int is not None and start_int <= gateway_int <= end_int else 0
        total_available += max(0, (end_int - start_int + 1) - used_in_range - gateway_in_range)

        if len(available) >= limit:
            truncated = True
            continue

        for ip_int in range(start_int, end_int + 1):
            if ip_int in used_ips:
                continue
            if gateway_int is not None and ip_int == gateway_int:
                continue
            available.append(str(ip_address(ip_int)))
            if len(available) >= limit:
                truncated = True
                break

    return available, total_available, truncated
