from django.urls import path

from .views import (
    create_migrations_from_vmware,
    health,
    openstack_flavors,
    openstack_health,
    openstack_images,
    openstack_networks,
    vmware_vms,
)

urlpatterns = [
    path("health", health, name="health"),
    path("openstack/health", openstack_health, name="openstack-health"),
    path("openstack/images", openstack_images, name="openstack-images"),
    path("openstack/flavors", openstack_flavors, name="openstack-flavors"),
    path("openstack/networks", openstack_networks, name="openstack-networks"),
    path("vmware/vms", vmware_vms, name="vmware-vms"),
    path("migrations/from-vmware", create_migrations_from_vmware, name="migrations-from-vmware"),
]
