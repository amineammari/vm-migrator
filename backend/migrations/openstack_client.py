"""OpenStack read-only client wrapper for API views."""

from __future__ import annotations

import os
from typing import Any

import openstack
from keystoneauth1 import exceptions as ks_exceptions
from openstack import exceptions as os_exceptions
from openstack.config import OpenStackConfig
from openstack.connection import Connection


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

    def __init__(self, cloud: str = "openstack") -> None:
        self.cloud = cloud
        self._conn = self._connect()

    def _connect(self):
        try:
            env_kwargs = _connect_kwargs_from_env()
            if env_kwargs:
                return openstack.connect(
                    cloud=None,
                    load_yaml_config=False,
                    load_envvars=False,
                    app_name="vm-migrator",
                    app_version="1",
                    **env_kwargs,
                )

            # When DevStack publishes the /image proxy endpoint (eg http://HOST/image) it can
            # reject binary uploads (HTTP 415). Allow an env-based override for Glance.
            image_endpoint_override = os.environ.get("OPENSTACK_IMAGE_ENDPOINT_OVERRIDE", "").strip() or None
            if not image_endpoint_override:
                return openstack.connect(cloud=self.cloud)

            cfg = OpenStackConfig(load_yaml_config=True, load_envvars=True)
            region = cfg.get_one_cloud(cloud=self.cloud)
            region.config["image_endpoint_override"] = image_endpoint_override
            return Connection(config=region)
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
