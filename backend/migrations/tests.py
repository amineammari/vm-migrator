from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from rest_framework.test import APIClient

from .disk_formats import DiskConversionError, convert_with_qemu_img, detect_disk_format
from .models import MigrationJob
from .serializers import VMOverridesSerializer


User = get_user_model()


class DiskFormatDetectionTests(SimpleTestCase):
    def test_detect_qcow2(self):
        with TemporaryDirectory() as td:
            p = Path(td) / "disk.qcow2"
            p.write_bytes(b"QFI\xfb" + b"\x00" * 4096)
            self.assertEqual(detect_disk_format(p), "qcow2")

    def test_detect_vmdk_sparse_magic(self):
        with TemporaryDirectory() as td:
            p = Path(td) / "disk.vmdk"
            p.write_bytes(b"KDMV" + b"\x00" * 4096)
            self.assertEqual(detect_disk_format(p), "vmdk")

    def test_detect_vhdx(self):
        with TemporaryDirectory() as td:
            p = Path(td) / "disk.vhdx"
            p.write_bytes(b"vhdxfile" + b"\x00" * 4096)
            self.assertEqual(detect_disk_format(p), "vhdx")

    def test_detect_vhd_footer(self):
        with TemporaryDirectory() as td:
            p = Path(td) / "disk.vhd"
            payload = bytearray(b"\x00" * 1024)
            payload[-512:-504] = b"conectix"
            p.write_bytes(bytes(payload))
            self.assertEqual(detect_disk_format(p), "vhd")

    def test_detect_unknown_as_raw(self):
        with TemporaryDirectory() as td:
            p = Path(td) / "disk.bin"
            p.write_bytes(b"\x00" * 4096)
            self.assertEqual(detect_disk_format(p), "raw")


class QemuImgWrapperTests(SimpleTestCase):
    @patch("migrations.disk_formats.shutil.which")
    @patch("migrations.disk_formats.subprocess.run")
    def test_convert_with_qemu_img_success(self, run_mock, which_mock):
        which_mock.return_value = "/usr/bin/qemu-img"
        run_mock.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")
        with TemporaryDirectory() as td:
            src = Path(td) / "src.vmdk"
            dst = Path(td) / "dst.qcow2"
            src.write_bytes(b"KDMV" + b"\x00" * 1024)
            result = convert_with_qemu_img(
                source_path=src,
                target_path=dst,
                source_format="vmdk",
                target_format="qcow2",
            )
            self.assertEqual(result["source_format"], "vmdk")
            self.assertEqual(result["target_format"], "qcow2")
            self.assertIn("qemu-img convert", result["command"])

    @patch("migrations.disk_formats.shutil.which")
    @patch("migrations.disk_formats.subprocess.run")
    def test_convert_with_qemu_img_failure(self, run_mock, which_mock):
        which_mock.return_value = "/usr/bin/qemu-img"
        run_mock.return_value = SimpleNamespace(returncode=1, stdout="x", stderr="boom")
        with TemporaryDirectory() as td:
            src = Path(td) / "src.vmdk"
            dst = Path(td) / "dst.qcow2"
            src.write_bytes(b"KDMV" + b"\x00" * 1024)
            with self.assertRaises(DiskConversionError):
                convert_with_qemu_img(
                    source_path=src,
                    target_path=dst,
                    source_format="vmdk",
                    target_format="qcow2",
                )


class DiskPolicySerializerTests(SimpleTestCase):
    def test_map_disk_merge_flag_to_concat_mode(self):
        serializer = VMOverridesSerializer(data={"disk_merge": True})
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data.get("disk_layout_mode"), "concat")

    def test_accept_disk_concat_mode(self):
        serializer = VMOverridesSerializer(data={"disk_layout_mode": "concat"})
        self.assertTrue(serializer.is_valid(), serializer.errors)
        self.assertEqual(serializer.validated_data.get("disk_layout_mode"), "concat")


class EndpointAccessTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(
            username="eve",
            email="eve@example.com",
            password="secret123",
            role=User.Role.USER,
        )

    @patch("migrations.views.OpenStackClient")
    def test_regular_user_can_read_openstack_health(self, client_mock):
        instance = client_mock.return_value
        instance.validate_connection.return_value = "proj-id"
        instance.list_images.return_value = [{"id": "img1"}]
        instance.list_flavors.return_value = [{"id": "flv1"}, {"id": "flv2"}]
        instance.list_networks.return_value = []

        self.client.force_authenticate(self.user)
        response = self.client.get("/api/openstack/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["project_id"], "proj-id")
        self.assertEqual(response.data["image_count"], 1)
        self.assertEqual(response.data["flavor_count"], 2)

    @patch("migrations.views.ESXiVMwareClient")
    def test_regular_user_can_test_vmware_endpoint(self, vmware_mock):
        vmware_mock.return_value.test_connection.return_value = {"ok": True, "message": "ok"}

        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/vmware/endpoints/test",
            {
                "host": "1.2.3.4",
                "username": "root",
                "password": "pw",
                "port": 443,
                "insecure": True,
            },
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["ok"])


class AuthAndRBACTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.super_admin = User.objects.create_user(
            username="admin",
            email="admin@example.com",
            password="secret123",
            role=User.Role.SUPER_ADMIN,
        )
        self.user = User.objects.create_user(
            username="alice",
            email="alice@example.com",
            password="secret123",
            role=User.Role.USER,
        )
        self.other_user = User.objects.create_user(
            username="bob",
            email="bob@example.com",
            password="secret123",
            role=User.Role.USER,
        )

    def test_register_login_and_refresh_flow(self):
        register_response = self.client.post(
            "/api/auth/register",
            {
                "username": "charlie",
                "email": "charlie@example.com",
                "password": "secret123",
            },
            format="json",
        )
        self.assertEqual(register_response.status_code, 201)
        self.assertEqual(register_response.data["email"], "charlie@example.com")

        login_response = self.client.post(
            "/api/auth/login",
            {"username": "charlie", "password": "secret123"},
            format="json",
        )
        self.assertEqual(login_response.status_code, 200)
        self.assertIn("access", login_response.data)
        self.assertIn("refresh", login_response.data)
        self.assertEqual(login_response.data["user"]["role"], "USER")

        refresh_response = self.client.post(
            "/api/auth/refresh",
            {"refresh": login_response.data["refresh"]},
            format="json",
        )
        self.assertEqual(refresh_response.status_code, 200)
        self.assertIn("access", refresh_response.data)

    def test_super_admin_can_manage_users(self):
        self.client.force_authenticate(self.super_admin)
        list_response = self.client.get("/api/users/")
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(len(list_response.data), 3)

        create_response = self.client.post(
            "/api/users/",
            {
                "username": "new-admin",
                "email": "new-admin@example.com",
                "password": "secret123",
                "role": "SUPER_ADMIN",
            },
            format="json",
        )
        self.assertEqual(create_response.status_code, 201)
        created_id = create_response.data["id"]

        update_response = self.client.put(
            f"/api/users/{created_id}/",
            {
                "username": "renamed-admin",
                "email": "renamed-admin@example.com",
                "role": "SUPER_ADMIN",
            },
            format="json",
        )
        self.assertEqual(update_response.status_code, 200)

        delete_response = self.client.delete(f"/api/users/{created_id}/")
        self.assertEqual(delete_response.status_code, 204)

    def test_regular_user_cannot_manage_users(self):
        self.client.force_authenticate(self.user)
        response = self.client.get("/api/users/")
        self.assertEqual(response.status_code, 403)

    def test_user_only_sees_own_migrations(self):
        own_job = MigrationJob.objects.create(
            user=self.user,
            vm_name="vm-user",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.PENDING,
        )
        MigrationJob.objects.create(
            user=self.other_user,
            vm_name="vm-other",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.FAILED,
        )

        self.client.force_authenticate(self.user)
        response = self.client.get("/api/migrations")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["id"], own_job.id)

    def test_super_admin_sees_all_migrations_and_can_filter(self):
        job_one = MigrationJob.objects.create(
            user=self.user,
            vm_name="vm-user",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.PENDING,
        )
        MigrationJob.objects.create(
            user=self.other_user,
            vm_name="vm-other",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.FAILED,
        )

        self.client.force_authenticate(self.super_admin)
        response = self.client.get("/api/migrations")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data), 2)

        filtered = self.client.get(f"/api/migrations?user_id={self.user.id}")
        self.assertEqual(filtered.status_code, 200)
        self.assertEqual(len(filtered.data), 1)
        self.assertEqual(filtered.data[0]["id"], job_one.id)

    def test_post_migration_assigns_request_user(self):
        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/migrations",
            {
                "vm_name": "new-vm",
                "source": "vmware",
                "destination": "openstack",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 201)
        job = MigrationJob.objects.get(id=response.data["id"])
        self.assertEqual(job.user_id, self.user.id)
        self.assertEqual(job.status, MigrationJob.Status.PENDING)

    def test_owner_or_super_admin_can_view_migration_detail(self):
        job = MigrationJob.objects.create(
            user=self.user,
            vm_name="vm-secure",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.PENDING,
        )

        self.client.force_authenticate(self.user)
        own_response = self.client.get(f"/api/migrations/{job.id}")
        self.assertEqual(own_response.status_code, 200)

        self.client.force_authenticate(self.other_user)
        forbidden = self.client.get(f"/api/migrations/{job.id}")
        self.assertEqual(forbidden.status_code, 403)

        self.client.force_authenticate(self.super_admin)
        admin_response = self.client.get(f"/api/migrations/{job.id}")
        self.assertEqual(admin_response.status_code, 200)

    def test_dashboard_returns_scoped_stats(self):
        MigrationJob.objects.create(
            user=self.user,
            vm_name="vm-run",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.PENDING,
        )
        MigrationJob.objects.create(
            user=self.user,
            vm_name="vm-ok",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.VERIFIED,
        )
        MigrationJob.objects.create(
            user=self.user,
            vm_name="vm-fail",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.FAILED,
        )
        MigrationJob.objects.create(
            user=self.other_user,
            vm_name="vm-other",
            source="vmware",
            destination="openstack",
            status=MigrationJob.Status.FAILED,
        )

        self.client.force_authenticate(self.user)
        response = self.client.get("/api/dashboard")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["total_migrations"], 3)
        self.assertEqual(response.data["stats_by_status"]["running"], 1)
        self.assertEqual(response.data["stats_by_status"]["completed"], 1)
        self.assertEqual(response.data["stats_by_status"]["failed"], 1)

        self.client.force_authenticate(self.super_admin)
        admin_response = self.client.get(f"/api/dashboard?user_id={self.user.id}")
        self.assertEqual(admin_response.status_code, 200)
        self.assertEqual(admin_response.data["total_migrations"], 3)
