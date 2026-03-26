from django.urls import path

from .views import LoginView, MeView, RefreshView, RegisterView, UserDetailView, UserListCreateView


urlpatterns = [
    path("auth/register", RegisterView.as_view(), name="auth-register"),
    path("auth/login", LoginView.as_view(), name="auth-login"),
    path("auth/refresh", RefreshView.as_view(), name="auth-refresh"),
    path("auth/me", MeView.as_view(), name="auth-me"),
    path("users/", UserListCreateView.as_view(), name="users-list"),
    path("users/<int:pk>/", UserDetailView.as_view(), name="users-detail"),
]
