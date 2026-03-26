from rest_framework.permissions import BasePermission


class IsSuperAdmin(BasePermission):
    message = "Super admin privileges are required."

    def has_permission(self, request, view):
        user = getattr(request, "user", None)
        return bool(user and user.is_authenticated and getattr(user, "role", None) == "SUPER_ADMIN")


class IsOwnerOrSuperAdmin(BasePermission):
    message = "You do not have permission to access this migration."

    def has_object_permission(self, request, view, obj):
        user = getattr(request, "user", None)
        if not user or not user.is_authenticated:
            return False
        if getattr(user, "role", None) == "SUPER_ADMIN":
            return True
        return getattr(obj, "user_id", None) == user.id
