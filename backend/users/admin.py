from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin

from .models import User


@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    list_display = ("id", "username", "email", "role", "is_active", "created_at")
    list_filter = ("role", "is_active", "is_staff", "is_superuser")
    search_fields = ("username", "email")
    ordering = ("-created_at",)
    fieldsets = DjangoUserAdmin.fieldsets + (
        ("VM Migrator", {"fields": ("role", "created_at")}),
    )
    readonly_fields = ("created_at",)
