from django.contrib.auth import get_user_model
from rest_framework import generics, permissions, status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from migrations.permissions import IsSuperAdmin

from .serializers import (
    RegisterSerializer,
    UserManagementSerializer,
    UserSummarySerializer,
    VMMigratorTokenObtainPairSerializer,
)


User = get_user_model()


class RegisterView(generics.CreateAPIView):
    queryset = User.objects.all()
    serializer_class = RegisterSerializer
    permission_classes = [permissions.AllowAny]


class LoginView(TokenObtainPairView):
    permission_classes = [permissions.AllowAny]
    serializer_class = VMMigratorTokenObtainPairSerializer


class RefreshView(TokenRefreshView):
    permission_classes = [permissions.AllowAny]


class UserListCreateView(generics.ListCreateAPIView):
    queryset = User.objects.all().order_by("-created_at")
    serializer_class = UserManagementSerializer
    permission_classes = [IsSuperAdmin]


class UserDetailView(generics.RetrieveUpdateDestroyAPIView):
    queryset = User.objects.all()
    serializer_class = UserManagementSerializer
    permission_classes = [IsSuperAdmin]


class MeView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        return Response(UserSummarySerializer(request.user).data, status=status.HTTP_200_OK)
