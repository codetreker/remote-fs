#!/usr/bin/env python3
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("fixture_image", Path(__file__).with_name("build-image.py"))
image = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(image)


def entries(data):
    result = {}
    offset = 0
    while True:
        if data[offset:offset + 6] != b"070701":
            raise ValueError("newc magic")
        values = [int(data[offset + 6 + i * 8:offset + 14 + i * 8], 16) for i in range(13)]
        offset += 110
        name = data[offset:offset + values[11] - 1].decode()
        offset += values[11]
        offset += -offset % 4
        body = data[offset:offset + values[6]]
        offset += values[6]
        offset += -offset % 4
        if name == "TRAILER!!!":
            return result
        if name in result:
            raise ValueError("duplicate cpio member")
        result[name] = (values, body)


class ImageSafetyTests(unittest.TestCase):
    def setUp(self):
        parent = image.ROOT / ".tmp/native-current-authority-fixture/build-unit"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_outputs_cannot_escape_task_temporary_directory(self):
        for path in ("/", "/tmp/fixture", image.ROOT, image.ROOT / ".tmp"):
            with self.subTest(path=str(path)), self.assertRaises(ValueError):
                image.owned_path(path)
        (self.root / "escape").symlink_to("/tmp", target_is_directory=True)
        with self.assertRaises(ValueError):
            image.owned_path(self.root / "escape/child")
        self.assertEqual(image.owned_path(self.root / "inside"), self.root / "inside")

    def test_initramfs_has_kernel_console_before_init_executes(self):
        path = self.root / "initramfs.gz"
        files = {"init": (b"ELF init", 0o555), "fixture-config.json": (b"{}\n", 0o400)}
        image.initramfs(path, files)
        parsed = entries(gzip.decompress(path.read_bytes()))
        for name, device, permissions in (("dev/console", (5, 1), 0o600), ("dev/null", (1, 3), 0o666)):
            fields, body = parsed[name]
            self.assertEqual(fields[1], stat.S_IFCHR | permissions)
            self.assertEqual(tuple(fields[9:11]), device)
            self.assertEqual(fields[2:4], [0, 0])
            self.assertEqual(body, b"")
        self.assertEqual(parsed["fixture-config.json"][0][1], stat.S_IFREG | 0o400)
        self.assertEqual(parsed["tmp"][0][1], stat.S_IFDIR | 0o1777)
        self.assertEqual(parsed["init"][1], b"ELF init")

    def test_initramfs_order_and_timestamps_are_reproducible(self):
        first, second = self.root / "a.gz", self.root / "b.gz"
        image.initramfs(first, {"z": (b"Z", 0o400), "a": (b"A", 0o400)})
        image.initramfs(second, {"a": (b"A", 0o400), "z": (b"Z", 0o400)})
        self.assertEqual(first.read_bytes(), second.read_bytes())
        self.assertTrue(all(fields[5] == 0 for fields, _ in entries(gzip.decompress(first.read_bytes())).values()))

    def test_bad_cached_input_fails_without_network_or_replacement(self):
        path = self.root / "package.deb"
        path.write_bytes(b"wrong")
        with self.assertRaisesRegex(ValueError, "cached input"):
            image.download("https://invalid.invalid/no-request", path, "0" * 64, 5)
        self.assertEqual(path.read_bytes(), b"wrong")
        self.assertFalse(path.with_suffix(".deb.partial").exists())

    def test_artifact_binds_actual_bytes(self):
        path = self.root / "payload"
        path.write_bytes(b"fixture")
        self.assertEqual(image.artifact(path, self.root), {
            "path": "payload", "size": 7, "sha256": hashlib.sha256(b"fixture").hexdigest()})

    def test_dependency_json_is_separate_from_download_diagnostics(self):
        script = 'import json,sys; print("go: downloading fixture-dependency v1",file=sys.stderr); print(json.dumps({"Dir":"/repo/packages/storage"}))'
        data = image.command([sys.executable, "-c", script], capture=True)
        self.assertEqual(json.loads(data), {"Dir": "/repo/packages/storage"})

    def test_version_capture_can_explicitly_include_stderr(self):
        script = 'import sys; print("fixture formatter 1.0",file=sys.stderr)'
        self.assertEqual(image.command([sys.executable, "-c", script], capture=True, diagnostics=True), "fixture formatter 1.0")


if __name__ == "__main__":
    unittest.main()
