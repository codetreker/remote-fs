#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("fixture_linux", Path(__file__).with_name("run-linux.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class LinuxRunnerTests(unittest.TestCase):
    def setUp(self):
        parent = runner.ROOT / ".tmp/native-current-authority-fixture/linux-runner-unit"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.identity = {"run_id": "unit", "source_sha": "a" * 40, "fixture_nonce": "b" * 64}

    def test_guest_identity_order_and_actual_exit_are_required(self):
        value = {**self.identity, "sequence": 4, "op": "shutdown", "phase": "shutdown", "status": "ok", "exit_code": 0}
        self.assertEqual(runner.response(json.dumps(value).encode(), self.identity, 4, "shutdown", "shutdown"), value)
        for key, invalid in (("fixture_nonce", "c" * 64), ("sequence", 3), ("op", "start"),
                             ("phase", "stop-authority"), ("status", "error"), ("exit_code", 1)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                runner.response(json.dumps({**value, key: invalid}).encode(), self.identity, 4, "shutdown", "shutdown")
        del value["exit_code"]
        with self.assertRaises(ValueError):
            runner.response(json.dumps(value).encode(), self.identity, 4, "shutdown", "shutdown")

    def test_artifact_traversal_symlink_and_nonfile_are_rejected(self):
        (self.directory / "escape").symlink_to("/tmp")
        for relative in ("../no", "/etc/passwd", "escape/no", "."):
            with self.subTest(relative=relative), self.assertRaises(ValueError):
                runner.file_under(self.directory, relative)

    def test_run_parent_symlink_cannot_escape_task_temporary_directory(self):
        (self.directory / "escape").symlink_to("/tmp", target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "below this worktree"):
            runner.new_run_directory(self.directory / "escape", "never-created")

    def test_qemu_has_only_loopback_forward_and_flushing_private_disk(self):
        manifest = {"boot_token": "d" * 64, "volume": "fixture-current", "qemu": {"machine": "pc-q35-11.1"}}
        args = runner.qemu_arguments(manifest, self.identity, {"kernel": Path("/kernel"), "initramfs": Path("/initrd")},
                                     Path("/private/a,b.ext4"), Path("/bios"), 34567)
        self.assertIn("tcg,thread=multi", args)
        self.assertIn("user,id=net,ipv6=off,restrict=on,hostfwd=tcp:127.0.0.1:34567-10.0.2.15:8080", args)
        self.assertIn("file=/private/a,,b.ext4,format=raw,if=none,id=data,cache=writeback,rerror=report,werror=report", args)
        self.assertNotIn("-enable-kvm", args)
        self.assertIn("RFS_FIXTURE_BOOT_TOKEN=" + "d" * 64, args[args.index("-append") + 1])

    def test_source_inventory_rejects_added_deleted_or_changed_inputs(self):
        expected = {"packages/storage/kept.go": "a" * 64}
        candidates = [{}, {**expected, "packages/storage/added.go": "b" * 64},
                      {"packages/storage/kept.go": "c" * 64}]
        for actual in candidates:
            with self.subTest(actual=actual), patch.object(runner.image, "source_snapshot", return_value=actual):
                with self.assertRaisesRegex(ValueError, "inventory changed"):
                    runner.verify_source_inventory(expected)
        with patch.object(runner.image, "source_snapshot", return_value=expected):
            runner.verify_source_inventory(expected)

    def test_owned_child_drains_and_exits_without_forcing(self):
        child = runner.OwnedChild([sys.executable, "-c", "print('ok')"], self.directory, self.directory, "normal")
        try:
            child.wait(3)
            self.assertEqual(child.stdout, b"ok\n")
            self.assertIsNone(child.process.returncode)
            self.assertTrue(Path(f"/proc/{child.process.pid}").exists())
        finally:
            child.finish()
        self.assertFalse(child.forced)

    def test_output_limit_fails_and_cleanup_reaps_the_process(self):
        child = runner.OwnedChild([sys.executable, "-c", "print('x'*2048)"], self.directory, self.directory, "overflow")
        try:
            with self.assertRaisesRegex(ValueError, "1024 bytes"):
                child.wait(3)
        finally:
            child.finish()

    def test_forced_cleanup_cannot_be_counted_as_graceful(self):
        child = runner.OwnedChild([sys.executable, "-c", "import time; time.sleep(30)"], self.directory, self.directory, "held")
        child.finish()
        self.assertTrue(child.forced)
        self.assertIsNotNone(child.process.returncode)
        with self.assertRaises(ProcessLookupError):
            os.killpg(child.process.pid, 0)


if __name__ == "__main__":
    unittest.main()
