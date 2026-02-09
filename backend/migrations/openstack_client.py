"""OpenStack read-only client wrapper for API views."""

from __future__ import annotations

from typing import Any

import openstack
from keystoneauth1 import exceptions as ks_exceptions
from openstack import exceptions as os_exceptions


class OpenStackClientError(Exception):
    """Raised when OpenStack connectivity or API reads fail."""


class OpenStackClient:
    """Small abstraction around openstacksdk using cloud='openstack'."""

    def __init__(self, cloud: str = "openstack") -> None:
        self.cloud = cloud
        self._conn = self._connect()

    def _connect(self):
        try:
            return openstack.connect(cloud=self.cloud)
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
        """List available tenant/provider networks."""
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
