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
    # --- ROUTER MANAGEMENT (OpenStack Neutron) ---

    def create_router(self, name: str, external_network_id: str) -> dict[str, Any]:
            """
            Create a Neutron router and set its external gateway.
            Args:
                name: Name of the router.
                external_network_id: Network ID to use as external gateway.
            Returns:
                Router details as dict.
            Raises:
                OpenStackClientError on fatal error.
            """
            import logging
            logger = logging.getLogger("migrations.openstack_client")
            try:
                existing = self._conn.network.find_router(name, ignore_missing=True)
                if existing:
                    logger.info(f"Router '{name}' already exists (id={existing.id})")
                    return existing.to_dict()
                router = self._conn.network.create_router(name=name)
                logger.info(f"Created router '{name}' (id={router.id})")
                # Set external gateway
                router = self._conn.network.update_router(router, external_gateway_info={"network_id": external_network_id})
                logger.info(f"Set external gateway for router '{name}' (id={router.id}) to network {external_network_id}")
                return router.to_dict()
            except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
                logger.error(f"Failed to create router '{name}': {exc}")
                raise OpenStackClientError(f"Failed to create router: {exc}") from exc
            except Exception as exc:
                logger.error(f"Unexpected error during router creation: {exc}")
                raise OpenStackClientError(f"Unexpected error during router creation: {exc}") from exc

    def add_interface_to_router(self, router_id: str, subnet_id: str) -> dict[str, Any]:
            """
            Attach a subnet as an interface to a router.
            Args:
                router_id: Router ID.
                subnet_id: Subnet ID to attach.
            Returns:
                Interface attachment result as dict.
            """
            import logging
            logger = logging.getLogger("migrations.openstack_client")
            try:
                result = self._conn.network.add_interface_to_router(router_id, subnet_id=subnet_id)
                logger.info(f"Attached subnet {subnet_id} to router {router_id}")
                return result
            except os_exceptions.ConflictException:
                logger.warning(f"Subnet {subnet_id} is already attached to router {router_id}")
                return {"status": "already-attached"}
            except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
                logger.error(f"Failed to attach subnet {subnet_id} to router {router_id}: {exc}")
                raise OpenStackClientError(f"Failed to attach subnet to router: {exc}") from exc
            except Exception as exc:
                logger.error(f"Unexpected error during interface attachment: {exc}")
                raise OpenStackClientError(f"Unexpected error during interface attachment: {exc}") from exc

    def remove_interface_from_router(self, router_id: str, subnet_id: str) -> dict[str, Any]:
            """
            Detach a subnet from a router.
            Args:
                router_id: Router ID.
                subnet_id: Subnet ID to detach.
            Returns:
                Interface detachment result as dict.
            """
            import logging
            logger = logging.getLogger("migrations.openstack_client")
            try:
                result = self._conn.network.remove_interface_from_router(router_id, subnet_id=subnet_id)
                logger.info(f"Detached subnet {subnet_id} from router {router_id}")
                return result
            except os_exceptions.ConflictException:
                logger.warning(f"Subnet {subnet_id} is not attached to router {router_id}")
                return {"status": "not-attached"}
            except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
                logger.error(f"Failed to detach subnet {subnet_id} from router {router_id}: {exc}")
                raise OpenStackClientError(f"Failed to detach subnet from router: {exc}") from exc
            except Exception as exc:
                logger.error(f"Unexpected error during interface detachment: {exc}")
                raise OpenStackClientError(f"Unexpected error during interface detachment: {exc}") from exc

    def list_routers(self) -> list[dict[str, Any]]:
            """
            List all routers visible to the current project.
            Returns:
                List of routers as dicts.
            """
            import logging
            logger = logging.getLogger("migrations.openstack_client")
            try:
                routers = [router.to_dict() for router in self._conn.network.routers()]
                logger.info(f"Listed {len(routers)} routers")
                return routers
            except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
                logger.error(f"Failed to list routers: {exc}")
                raise OpenStackClientError(f"Failed to list routers: {exc}") from exc
            except Exception as exc:
                logger.error(f"Unexpected error while listing routers: {exc}")
                raise OpenStackClientError(f"Unexpected error while listing routers: {exc}") from exc

    def delete_router(self, router_id: str) -> None:
            """
            Delete a router by ID.
            Args:
                router_id: Router ID to delete.
            """
            import logging
            logger = logging.getLogger("migrations.openstack_client")
            try:
                self._conn.network.delete_router(router_id, ignore_missing=True)
                logger.info(f"Deleted router {router_id}")
            except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
                logger.error(f"Failed to delete router {router_id}: {exc}")
                raise OpenStackClientError(f"Failed to delete router: {exc}") from exc
            except Exception as exc:
                logger.error(f"Unexpected error during router deletion: {exc}")
                raise OpenStackClientError(f"Unexpected error during router deletion: {exc}") from exc
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

    def list_projects(self) -> list[dict[str, Any]]:
        """List projects visible to the authenticated user."""
        try:
            current_project_id = getattr(self._conn, "current_project_id", None)
            items: list[dict[str, Any]] = []
            for project in self._conn.identity.projects():
                project_id = getattr(project, "id", None)
                project_name = getattr(project, "name", None)
                if not project_id and not project_name:
                    continue
                items.append(
                    {
                        "id": project_id,
                        "name": project_name,
                        "domain_id": getattr(project, "domain_id", None),
                        "description": getattr(project, "description", None),
                        "is_enabled": getattr(project, "is_enabled", None),
                        "is_current": bool(project_id and current_project_id and str(project_id) == str(current_project_id)),
                    }
                )
            items.sort(key=lambda item: (str(item.get("name") or ""), str(item.get("id") or "")))
            return items
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to list OpenStack projects: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing OpenStack projects: {exc}") from exc

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

    def list_external_networks(self) -> list[dict[str, Any]]:
        """List networks that can allocate floating IPs."""
        try:
            return [
                item
                for item in self.list_networks()
                if item.get("id") and item.get("is_router_external") is True
            ]
        except OpenStackClientError:
            raise
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing external networks: {exc}") from exc

    def list_floating_ips(self, *, available_only: bool = False) -> list[dict[str, Any]]:
        """List floating IPs visible to the active project."""
        try:
            items: list[dict[str, Any]] = []
            for floating_ip in self._conn.network.ips():
                port_id = getattr(floating_ip, "port_id", None)
                if available_only and port_id:
                    continue
                items.append(
                    {
                        "id": getattr(floating_ip, "id", None),
                        "address": getattr(floating_ip, "floating_ip_address", None),
                        "status": getattr(floating_ip, "status", None),
                        "port_id": port_id,
                        "fixed_ip_address": getattr(floating_ip, "fixed_ip_address", None),
                        "floating_network_id": getattr(floating_ip, "floating_network_id", None),
                        "router_id": getattr(floating_ip, "router_id", None),
                    }
                )
            items.sort(key=lambda item: (str(item.get("address") or ""), str(item.get("id") or "")))
            return items
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to list floating IPs: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while listing floating IPs: {exc}") from exc

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

    def create_network(
        self,
        *,
        name: str,
        subnet_name: str,
        cidr: str,
        gateway_ip: str | None = None,
        enable_dhcp: bool = True,
        allocation_pool_start: str | None = None,
        allocation_pool_end: str | None = None,
        dns_nameservers: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a network and an IPv4 subnet, then return the formatted network details."""
        try:
            existing = self._conn.network.find_network(name, ignore_missing=True)
            if existing is not None:
                raise OpenStackClientError(f"Network '{name}' already exists.")

            network = self._conn.network.create_network(name=name)
            subnet_payload: dict[str, Any] = {
                "name": subnet_name or f"{name}-subnet",
                "network_id": network.id,
                "ip_version": 4,
                "cidr": cidr,
                "enable_dhcp": bool(enable_dhcp),
            }
            if gateway_ip:
                subnet_payload["gateway_ip"] = gateway_ip
            if allocation_pool_start and allocation_pool_end:
                subnet_payload["allocation_pools"] = [
                    {"start": allocation_pool_start, "end": allocation_pool_end}
                ]
            if dns_nameservers:
                subnet_payload["dns_nameservers"] = list(dns_nameservers)

            try:
                self._conn.network.create_subnet(**subnet_payload)
            except Exception:
                self._conn.network.delete_network(network.id, ignore_missing=True)
                raise

            created = next(
                (item for item in self.list_networks_detail() if str(item.get("id")) == str(network.id)),
                None,
            )
            return created or {
                "id": network.id,
                "name": network.name,
                "status": getattr(network, "status", None),
                "is_admin_state_up": getattr(network, "is_admin_state_up", None),
                "is_router_external": getattr(network, "is_router_external", None),
                "subnets": [],
            }
        except OpenStackClientError:
            raise
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to create OpenStack network: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while creating OpenStack network: {exc}") from exc

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

    def validate_floating_ip(
        self,
        *,
        address: str,
        external_network_id: str | None = None,
    ) -> tuple[bool, str | None]:
        """Validate that a floating IP exists and is currently unassigned."""
        try:
            for floating_ip in self._conn.network.ips():
                current_address = str(getattr(floating_ip, "floating_ip_address", "") or "")
                if current_address != str(address):
                    continue
                current_network_id = str(getattr(floating_ip, "floating_network_id", "") or "") or None
                if external_network_id and current_network_id != str(external_network_id):
                    return False, "Floating IP belongs to a different external network."
                if getattr(floating_ip, "port_id", None):
                    return False, "Floating IP is already assigned."
                return True, None
            return False, "Floating IP not found."
        except (os_exceptions.SDKException, ks_exceptions.ClientException) as exc:
            raise OpenStackClientError(f"Failed to validate floating IP: {exc}") from exc
        except Exception as exc:
            raise OpenStackClientError(f"Unexpected error while validating floating IP: {exc}") from exc


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
