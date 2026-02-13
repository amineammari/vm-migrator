from django.urls import path

from .views import (
    create_migrations_from_vmware,
    discover_now,
    health,
    list_migrations,
    migration_detail,
    openstack_flavors,
    openstack_health,
    openstack_images,
    openstack_networks,
    openstack_provision_now,
    openstack_provision_status,
    rollback_migration_now,
    start_migration_now,
    task_status,
    vmware_vms,
)

urlpatterns = [
    path("health", health, name="health"),
    path("openstack/health", openstack_health, name="openstack-health"),
    path("openstack/images", openstack_images, name="openstack-images"),
    path("openstack/flavors", openstack_flavors, name="openstack-flavors"),
    path("openstack/networks", openstack_networks, name="openstack-networks"),
    path("openstack/provision", openstack_provision_now, name="openstack-provision-now"),
    path("openstack/provision/status", openstack_provision_status, name="openstack-provision-status"),
    path("vmware/vms", vmware_vms, name="vmware-vms"),
    path("vmware/discover-now", discover_now, name="vmware-discover-now"),
    path("tasks/<str:task_id>", task_status, name="task-status"),
    path("migrations", list_migrations, name="migrations-list"),
    path("migrations/<int:job_id>", migration_detail, name="migrations-detail"),
    path("migrations/from-vmware", create_migrations_from_vmware, name="migrations-from-vmware"),
    path("migrations/<int:job_id>/start", start_migration_now, name="migrations-start-now"),
    path("migrations/<int:job_id>/rollback", rollback_migration_now, name="migrations-rollback-now"),
]
