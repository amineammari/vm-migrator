from django.contrib import admin

from .models import DiscoveredVM, MigrationJob


@admin.register(MigrationJob)
class MigrationJobAdmin(admin.ModelAdmin):
    list_display = ("id", "vm_name", "status", "created_at", "updated_at")
    list_filter = ("status", "created_at")
    search_fields = ("vm_name",)
    ordering = ("-created_at",)


@admin.register(DiscoveredVM)
class DiscoveredVMAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "source", "cpu", "ram", "power_state", "last_seen")
    list_filter = ("source", "power_state", "last_seen")
    search_fields = ("name",)
    ordering = ("-last_seen",)
