from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone
from rest_framework.test import APIClient

from .conversion import ConversionPlan
from .disk_formats import DiskConversionError, convert_with_qemu_img, detect_disk_format
from .models import (
    DiscoveredVM,
    MigrationJob,
    OpenstackEndpointSession,
    VmwareEndpointSession,
)
from .network_remediation import (
    apply_guest_network_remediation,
    render_cloud_init_network_disable_config,
    render_network_heal_script,
    render_network_heal_service,
)
from .openstack_client import OpenStackClientError
from .openstack_deployment import (
    _auto_fix_image_endpoint,
    _normalize_image_endpoint_override,
    delete_volume_if_exists,
    find_flavor_choice,
)
from .serializers import CreateMigrationFromVMwareSerializer, VMOverridesSerializer
from .tasks import start_migration


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


class OpenStackEndpointNormalizationTests(SimpleTestCase):
    def test_normalize_unversioned_image_endpoint_override(self):
        self.assertEqual(
            _normalize_image_endpoint_override("http://192.168.72.169/image"),
            "http://192.168.72.169/image/v2",
        )

    def test_keep_versioned_image_endpoint_override(self):
        self.assertEqual(
            _normalize_image_endpoint_override("http://192.168.72.169/image/v2"),
            "http://192.168.72.169/image/v2",
        )

    def test_auto_fix_image_endpoint_promotes_catalog_endpoint(self):
        conn = SimpleNamespace(
            config=SimpleNamespace(
                config={},
                get_endpoint=lambda service_type: None,
            ),
            endpoint_for=lambda service_type: "http://192.168.72.169/image",
        )

        _auto_fix_image_endpoint(conn)

        self.assertEqual(
            conn.config.config["image_endpoint_override"],
            "http://192.168.72.169/image/v2",
        )
        self.assertEqual(conn.config.config["image_api_version"], "2")


class OpenStackDeploymentHelperTests(SimpleTestCase):
    def test_find_flavor_choice_falls_back_to_flavor_name(self):
        flavor = SimpleNamespace(id="flavor-123", name="ds2G", vcpus=2, ram=2048)
        conn = SimpleNamespace(
            compute=SimpleNamespace(
                find_flavor=lambda ref, ignore_missing=True: None,
                flavors=lambda: [flavor],
            )
        )

        choice = find_flavor_choice(conn, "ds2G")

        self.assertIsNotNone(choice)
        self.assertEqual(choice.id, "flavor-123")
        self.assertEqual(choice.name, "ds2G")
        self.assertEqual(choice.vcpus, 2)
        self.assertEqual(choice.ram, 2048)

    @patch("migrations.openstack_deployment.time.sleep")
    def test_delete_volume_if_exists_detaches_volume_before_delete(self, sleep_mock):
        volume = SimpleNamespace(id="vol-1")
        attached_volume = SimpleNamespace(id="vol-1", attachments=[{"server_id": "srv-1"}])
        detached_volume = SimpleNamespace(id="vol-1", attachments=[])
        get_volume_mock = Mock(side_effect=[attached_volume, detached_volume])
        delete_attachment_mock = Mock()
        delete_volume_mock = Mock()

        conn = SimpleNamespace(
            block_storage=SimpleNamespace(
                find_volume=lambda volume_id, ignore_missing=True: volume if volume_id == "vol-1" else None,
                get_volume=get_volume_mock,
                delete_volume=delete_volume_mock,
            ),
            compute=SimpleNamespace(
                delete_volume_attachment=delete_attachment_mock,
            ),
        )

        status = delete_volume_if_exists(conn, "vol-1")

        self.assertEqual(status, "deleted")
        delete_attachment_mock.assert_called_once_with("srv-1", "vol-1", ignore_missing=True)
        delete_volume_mock.assert_called_once_with("vol-1", ignore_missing=True, force=True)
        sleep_mock.assert_called_once()


class GuestNetworkRemediationTests(SimpleTestCase):
    def test_render_network_heal_script_is_generic(self):
        script = render_network_heal_script()
        self.assertIn("candidate_ifaces()", script)
        self.assertIn("dhclient", script)
        self.assertIn("nmcli", script)
        self.assertNotIn("ens3", script)
        self.assertNotIn("eth0", script)

    def test_render_network_heal_service_enables_boot_execution(self):
        service = render_network_heal_service()
        self.assertIn("ExecStart=/usr/local/sbin/vm-migrator-network-heal", service)
        self.assertIn("WantedBy=multi-user.target", service)
        self.assertIn("Before=multi-user.target", service)

    def test_render_cloud_init_network_disable_config(self):
        config = render_cloud_init_network_disable_config()
        self.assertEqual(config.strip(), "network: {config: disabled}")

    @patch("migrations.network_remediation.subprocess.run")
    @patch("migrations.network_remediation.shutil.which")
    def test_apply_guest_network_remediation_adds_safe_cleanup(self, which_mock, run_mock):
        which_mock.return_value = "/usr/bin/virt-customize"
        run_mock.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        with TemporaryDirectory() as td:
            image_path = Path(td) / "disk.qcow2"
            image_path.write_bytes(b"QFI\xfb" + b"\x00" * 4096)

            result = apply_guest_network_remediation([str(image_path)], timeout_seconds=30)

        self.assertTrue(result["ok"])
        self.assertFalse(result["cloud_init_network_config_disabled"])
        self.assertEqual(run_mock.call_count, 1)
        cmd = run_mock.call_args.args[0]
        joined = " ".join(cmd)
        self.assertIn("virt-customize", cmd[0])
        self.assertIn("70-persistent-net.rules", joined)
        self.assertIn("ifcfg-*", joined)
        self.assertNotIn("99-vm-migrator-disable-network-config.cfg", joined)

    @patch("migrations.network_remediation.subprocess.run")
    @patch("migrations.network_remediation.shutil.which")
    def test_apply_guest_network_remediation_can_disable_cloud_init_network_config(self, which_mock, run_mock):
        which_mock.return_value = "/usr/bin/virt-customize"
        run_mock.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        with TemporaryDirectory() as td:
            image_path = Path(td) / "disk.qcow2"
            image_path.write_bytes(b"QFI\xfb" + b"\x00" * 4096)

            result = apply_guest_network_remediation(
                [str(image_path)],
                timeout_seconds=30,
                disable_cloud_init_network_config=True,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["cloud_init_network_config_disabled"])
        cmd = run_mock.call_args.args[0]
        joined = " ".join(cmd)
        self.assertIn("99-vm-migrator-disable-network-config.cfg", joined)
        self.assertIn("network-config", joined)


class StartMigrationGuestNetworkRemediationTests(TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.source_disk = Path(self.tempdir.name) / "source.vmdk"
        self.source_disk.write_bytes(b"KDMV" + b"\x00" * 4096)
        self.output_disk = Path(self.tempdir.name) / "converted.qcow2"
        self.output_disk.write_bytes(b"QFI\xfb" + b"\x00" * 4096)
        self.plan = ConversionPlan(
            command="qemu-img convert",
            command_args=["qemu-img", "convert"],
            input_disks=[str(self.source_disk)],
            output_path=str(self.output_disk),
            notes=[],
        )

    def _create_job_and_vm(self, *, status: str, execution: dict | None = None) -> MigrationJob:
        DiscoveredVM.objects.create(
            name="vm-remediate",
            source=DiscoveredVM.Source.WORKSTATION,
            cpu=2,
            ram=2048,
            disks=[{"path": str(self.source_disk)}],
            metadata={},
            power_state="poweredOff",
            last_seen=timezone.now(),
        )
        conversion_metadata = {
            "selected_source": DiscoveredVM.Source.WORKSTATION,
            "requested_spec": {},
        }
        if execution is not None:
            conversion_metadata["conversion"] = {"execution": execution}
        return MigrationJob.objects.create(
            vm_name="vm-remediate",
            source="vmware",
            destination="openstack",
            status=status,
            conversion_metadata=conversion_metadata,
        )

    @override_settings(
        ENABLE_REAL_CONVERSION=True,
        ENABLE_OPENSTACK_DEPLOYMENT=False,
        ENABLE_GUEST_NETWORK_REMEDIATION=True,
        GUEST_NETWORK_DISABLE_CLOUD_INIT_NETWORK_CONFIG=True,
        MIGRATION_OUTPUT_DIR="/tmp",
    )
    @patch("migrations.tasks.run_filesystem_consistency_check")
    @patch("migrations.tasks.validate_qcow2_images")
    @patch("migrations.tasks.apply_guest_network_remediation")
    @patch("migrations.tasks._execute_workstation_qemu_pipeline")
    @patch("migrations.tasks.plan_vmware_conversion")
    def test_start_migration_applies_guest_network_remediation_with_cloud_init_flag(
        self,
        plan_mock,
        execute_mock,
        remediation_mock,
        block_validation_mock,
        filesystem_mock,
    ):
        job = self._create_job_and_vm(status=MigrationJob.Status.CONVERTING)
        plan_mock.return_value = self.plan
        execute_mock.return_value = {
            "returncode": 0,
            "runner": "qemu-img",
            "duration_seconds": 1,
            "stdout": "",
            "stderr": "",
            "output_qcow2_path": str(self.output_disk),
            "output_qcow2_paths": [str(self.output_disk)],
            "primary_disk_index": 0,
            "disk_analysis": [],
            "disk_size": 4096,
            "disk_sizes": {str(self.output_disk): 4096},
            "disk_count": 1,
            "output_disk_format": "qcow2",
            "disk_layout_mode": "individual",
            "concatenation": None,
        }
        remediation_mock.return_value = {"ok": True, "checks": [], "cloud_init_network_config_disabled": True}
        block_validation_mock.return_value = {"ok": True, "failed": []}
        filesystem_mock.return_value = {"ok": True, "checks": []}

        result = start_migration(job.id)

        self.assertEqual(result["result"], "converted")
        remediation_mock.assert_called_once_with(
            [str(self.output_disk)],
            timeout_seconds=300,
            disable_cloud_init_network_config=True,
        )
        job.refresh_from_db()
        self.assertEqual(job.status, MigrationJob.Status.UPLOADING)
        execution = job.conversion_metadata["conversion"]["execution"]
        self.assertTrue(execution["guest_network_remediation_applied"])
        self.assertEqual(
            job.conversion_metadata["conversion"]["guest_network_remediation"]["cloud_init_network_config_disabled"],
            True,
        )

    @override_settings(
        ENABLE_REAL_CONVERSION=True,
        ENABLE_OPENSTACK_DEPLOYMENT=False,
        ENABLE_GUEST_NETWORK_REMEDIATION=True,
    )
    @patch("migrations.tasks.run_filesystem_consistency_check")
    @patch("migrations.tasks.validate_qcow2_images")
    @patch("migrations.tasks.apply_guest_network_remediation")
    def test_start_migration_skips_guest_network_remediation_when_already_applied(
        self,
        remediation_mock,
        block_validation_mock,
        filesystem_mock,
    ):
        job = self._create_job_and_vm(
            status=MigrationJob.Status.BLOCK_VALIDATING,
            execution={
                "state": "succeeded",
                "output_qcow2_path": str(self.output_disk),
                "output_qcow2_paths": [str(self.output_disk)],
                "disk_sizes": {str(self.output_disk): 4096},
                "guest_network_remediation_applied": True,
            },
        )
        block_validation_mock.return_value = {"ok": True, "failed": []}
        filesystem_mock.return_value = {"ok": True, "checks": []}

        result = start_migration(job.id)

        self.assertEqual(result["result"], "converted")
        remediation_mock.assert_not_called()
        job.refresh_from_db()
        self.assertEqual(job.status, MigrationJob.Status.UPLOADING)

    @override_settings(
        ENABLE_REAL_CONVERSION=True,
        ENABLE_OPENSTACK_DEPLOYMENT=False,
        ENABLE_GUEST_NETWORK_REMEDIATION=False,
        MIGRATION_OUTPUT_DIR="/tmp",
    )
    @patch("migrations.tasks.run_filesystem_consistency_check")
    @patch("migrations.tasks.validate_qcow2_images")
    @patch("migrations.tasks.apply_guest_network_remediation")
    @patch("migrations.tasks._execute_workstation_qemu_pipeline")
    @patch("migrations.tasks.plan_vmware_conversion")
    def test_start_migration_respects_disabled_guest_network_remediation_setting(
        self,
        plan_mock,
        execute_mock,
        remediation_mock,
        block_validation_mock,
        filesystem_mock,
    ):
        job = self._create_job_and_vm(status=MigrationJob.Status.CONVERTING)
        plan_mock.return_value = self.plan
        execute_mock.return_value = {
            "returncode": 0,
            "runner": "qemu-img",
            "duration_seconds": 1,
            "stdout": "",
            "stderr": "",
            "output_qcow2_path": str(self.output_disk),
            "output_qcow2_paths": [str(self.output_disk)],
            "primary_disk_index": 0,
            "disk_analysis": [],
            "disk_size": 4096,
            "disk_sizes": {str(self.output_disk): 4096},
            "disk_count": 1,
            "output_disk_format": "qcow2",
            "disk_layout_mode": "individual",
            "concatenation": None,
        }
        block_validation_mock.return_value = {"ok": True, "failed": []}
        filesystem_mock.return_value = {"ok": True, "checks": []}

        result = start_migration(job.id)

        self.assertEqual(result["result"], "converted")
        remediation_mock.assert_not_called()
        job.refresh_from_db()
        self.assertEqual(job.status, MigrationJob.Status.UPLOADING)
        self.assertNotIn("guest_network_remediation", job.conversion_metadata.get("conversion", {}))


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

    @patch("migrations.views.OpenStackClient")
    def test_openstack_health_tolerates_image_service_failure(self, client_mock):
        instance = client_mock.return_value
        instance.validate_connection.return_value = "proj-id"
        instance.list_images.side_effect = OpenStackClientError("glance 500")
        instance.list_flavors.return_value = [{"id": "flv1"}]
        instance.list_networks.return_value = [{"id": "net1"}]

        self.client.force_authenticate(self.user)
        response = self.client.get("/api/openstack/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["project_id"], "proj-id")
        self.assertIsNone(response.data["image_count"])
        self.assertEqual(response.data["image_error"], "glance 500")

    @patch("migrations.views.OpenStackClient")
    def test_openstack_endpoint_test_tolerates_image_service_failure(self, client_mock):
        instance = client_mock.return_value
        instance.validate_connection.return_value = "proj-id"
        instance.list_images.side_effect = OpenStackClientError("glance 500")
        instance.list_flavors.return_value = [{"id": "flv1"}]
        instance.list_networks.return_value = [{"id": "net1"}]

        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/openstack/endpoints/test",
            {
                "auth_url": "http://192.168.72.169/identity",
                "username": "admin",
                "password": "secret",
                "project_name": "admin",
                "user_domain_name": "Default",
                "project_domain_name": "Default",
                "region_name": "RegionOne",
                "interface": "public",
                "identity_api_version": "3",
                "verify": False,
            },
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["ok"])
        self.assertIn("image service is unhealthy", response.data["message"])
        self.assertEqual(response.data["image_error"], "glance 500")

    @patch("migrations.views.OpenStackClient")
    def test_openstack_network_create_returns_created_network(self, client_mock):
        instance = client_mock.return_value
        instance.create_network.return_value = {
            "id": "net-1",
            "name": "private-migration",
            "subnets": [{"id": "subnet-1", "cidr": "192.168.100.0/24"}],
        }
        instance.list_networks_detail.return_value = [
            {
                "id": "net-1",
                "name": "private-migration",
                "subnets": [{"id": "subnet-1", "cidr": "192.168.100.0/24"}],
            }
        ]

        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/openstack/networks/create",
            {
                "name": "private-migration",
                "cidr": "192.168.100.0/24",
                "enable_dhcp": True,
                "dns_nameservers": ["8.8.8.8"],
            },
            format="json",
        )
        self.assertEqual(response.status_code, 201)
        self.assertTrue(response.data["ok"])
        self.assertEqual(response.data["network"]["id"], "net-1")
        self.assertEqual(len(response.data["items"]), 1)

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


class DiskSelectionSerializerTests(TestCase):
    def test_selected_disk_indexes_always_include_system_disk(self):
        vmware_session = VmwareEndpointSession.objects.create(
            label="vmware",
            host="1.1.1.1",
            port=443,
            username="root",
            password="pw",
            insecure=True,
        )
        openstack_session = OpenstackEndpointSession.objects.create(
            label="os",
            auth_url="http://os.example.com",
            username="alice",
            password="pw",
            project_name="proj",
            user_domain_name="Default",
            project_domain_name="Default",
            region_name="",
            interface="",
            identity_api_version="",
            verify=False,
            image_endpoint_override="",
        )
        DiscoveredVM.objects.create(
            name="vm-selected-disks",
            source=DiscoveredVM.Source.ESXI,
            vmware_endpoint_session=vmware_session,
            cpu=2,
            ram=2048,
            disks=[
                {"label": "Hard disk 1", "unit_number": 0, "size_bytes": 10},
                {"label": "Hard disk 2", "unit_number": 1, "size_bytes": 20},
                {"label": "Hard disk 3", "unit_number": 2, "size_bytes": 30},
            ],
            metadata={},
            power_state="poweredOff",
            last_seen=timezone.now(),
        )

        serializer = CreateMigrationFromVMwareSerializer(
            data={
                "vmware_endpoint_session_id": vmware_session.id,
                "openstack_endpoint_session_id": openstack_session.id,
                "vms": [
                    {
                        "name": "vm-selected-disks",
                        "source": "esxi",
                        "overrides": {"selected_disk_indexes": [2]},
                    }
                ],
            },
            context={},
        )
        self.assertTrue(serializer.is_valid(), serializer.errors)
        overrides = serializer.validated_data["vms"][0]["overrides"]
        self.assertEqual(overrides["selected_disk_indexes"], [0, 2])


class SessionOwnershipTests(TestCase):
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
        self.vmware_user_session = VmwareEndpointSession.objects.create(
            label="own",
            host="1.1.1.1",
            port=443,
            username="root",
            password="pw",
            insecure=True,
            user=self.user,
        )
        self.vmware_other_session = VmwareEndpointSession.objects.create(
            label="other",
            host="2.2.2.2",
            port=443,
            username="root",
            password="pw",
            insecure=True,
            user=self.other_user,
        )
        self.openstack_user_session = OpenstackEndpointSession.objects.create(
            label="own-os",
            auth_url="http://os.example.com",
            username="alice",
            password="pw",
            project_name="proj",
            user_domain_name="Default",
            project_domain_name="Default",
            region_name="",
            interface="",
            identity_api_version="",
            verify=False,
            image_endpoint_override="",
            user=self.user,
        )
        self.openstack_other_session = OpenstackEndpointSession.objects.create(
            label="other-os",
            auth_url="http://os.example.com",
            username="bob",
            password="pw",
            project_name="proj2",
            user_domain_name="Default",
            project_domain_name="Default",
            region_name="",
            interface="",
            identity_api_version="",
            verify=False,
            image_endpoint_override="",
            user=self.other_user,
        )

    def test_user_cannot_view_other_vmware_detail(self):
        self.client.force_authenticate(self.user)
        response = self.client.get(f"/api/vmware/endpoints/{self.vmware_other_session.id}")
        self.assertEqual(response.status_code, 404)

    def test_super_admin_can_view_any_vmware_detail(self):
        self.client.force_authenticate(self.super_admin)
        response = self.client.get(f"/api/vmware/endpoints/{self.vmware_other_session.id}")
        self.assertEqual(response.status_code, 200)

    def test_user_cannot_close_other_vmware_session(self):
        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/vmware/endpoints/close",
            {"vmware_endpoint_session_id": self.vmware_other_session.id},
            format="json",
        )
        self.assertEqual(response.status_code, 403)
        self.assertTrue(VmwareEndpointSession.objects.filter(id=self.vmware_other_session.id).exists())

    def test_user_cannot_view_other_openstack_detail(self):
        self.client.force_authenticate(self.user)
        response = self.client.get(f"/api/openstack/endpoints/{self.openstack_other_session.id}")
        self.assertEqual(response.status_code, 404)

    @patch("migrations.views.start_migration.delay")
    def test_user_cannot_create_migration_with_unowned_sessions(self, mock_delay):
        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/migrations/from-vmware",
            {
                "vmware_endpoint_session_id": self.vmware_other_session.id,
                "openstack_endpoint_session_id": self.openstack_other_session.id,
                "vms": [{"name": "vm1", "source": "esxi"}],
            },
            format="json",
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(MigrationJob.objects.count(), 0)
        mock_delay.assert_not_called()

    @patch("migrations.views.start_migration.delay")
    def test_user_creates_migration_with_owned_sessions(self, mock_delay):
        DiscoveredVM.objects.create(
            name="vm-owned",
            source=DiscoveredVM.Source.ESXI,
            vmware_endpoint_session=self.vmware_user_session,
            cpu=1,
            ram=1024,
            disks=[{"label": "Hard disk 1", "unit_number": 0, "size_bytes": 10}],
            metadata={},
            power_state="off",
            last_seen=timezone.now(),
        )

        self.client.force_authenticate(self.user)
        response = self.client.post(
            "/api/migrations/from-vmware",
            {
                "vmware_endpoint_session_id": self.vmware_user_session.id,
                "openstack_endpoint_session_id": self.openstack_user_session.id,
                "vms": [
                    {
                        "name": "vm-owned",
                        "source": "esxi",
                        "overrides": {"selected_disk_indexes": [0]},
                    }
                ],
            },
            format="json",
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(MigrationJob.objects.count(), 1)
        job = MigrationJob.objects.first()
        self.assertEqual(job.conversion_metadata["requested_spec"]["selected_disk_indexes"], [0])
        mock_delay.assert_called_once()
