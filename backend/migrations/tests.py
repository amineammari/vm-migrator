from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from .disk_formats import DiskConversionError, convert_with_qemu_img, detect_disk_format
from .serializers import VMOverridesSerializer


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
    @patch("backend.migrations.disk_formats.shutil.which")
    @patch("backend.migrations.disk_formats.subprocess.run")
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

    @patch("backend.migrations.disk_formats.shutil.which")
    @patch("backend.migrations.disk_formats.subprocess.run")
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
    def test_block_disk_merge_flag(self):
        serializer = VMOverridesSerializer(data={"disk_merge": True})
        self.assertFalse(serializer.is_valid())
        self.assertIn("Disk concatenation/merge is not allowed", str(serializer.errors))

    def test_block_disk_concat_mode(self):
        serializer = VMOverridesSerializer(data={"disk_layout_mode": "concat"})
        self.assertFalse(serializer.is_valid())
        self.assertIn("Disk concatenation/merge is not allowed", str(serializer.errors))

