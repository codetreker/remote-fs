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
from unittest import mock

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

    def test_probe_catalogs_link_current_host_without_a_second_main(self):
        expected = {
            "linux": {"probe.go", "private_other.go", "smb_observer.go", "smb_probe_other.go"},
            "windows": {"probe.go", "private_windows.go", "smb_observer.go", "smb_probe_windows.go",
                        "smb_mapping_windows.go", "controller_windows.go"},
        }
        for os_name, required in expected.items():
            with self.subTest(os=os_name):
                names = image.probe_templates(os_name)
                self.assertEqual(set(names), required)
                self.assertEqual(len(names), len(required))
                self.assertNotIn("controller.go", names)
                self.assertFalse(any(name.endswith("_test.go") for name in names))
        with self.assertRaises(ValueError):
            image.probe_templates("darwin")

    def test_template_compile_receives_exact_files_and_architecture(self):
        for os_name, arch in (("linux", "amd64"), ("windows", "arm64"), ("windows", "amd64")):
            with self.subTest(os=os_name, arch=arch):
                work = self.root / f"{os_name}-{arch}"
                work.mkdir()
                output = self.root / (f"probe-{os_name}-{arch}" + (".exe" if os_name == "windows" else ""))
                names = image.probe_templates(os_name)
                environment = {"CGO_ENABLED": "0", "GOWORK": "off"}
                with mock.patch.object(image, "command") as run:
                    image.compile_templates(work, output, names, environment, os_name, arch)
                command, settings = run.call_args.args
                self.assertEqual(command[:6], ["go", "build", "-trimpath", "-buildvcs=false", "-o", str(output)])
                self.assertEqual([Path(path).name for path in command[6:]], list(names))
                self.assertEqual(settings, {**environment, "GOOS": os_name, "GOARCH": arch})
                self.assertEqual(environment, {"CGO_ENABLED": "0", "GOWORK": "off"})
                for path in command[6:]:
                    self.assertEqual(Path(path).read_bytes(), (image.TOOLS / (Path(path).name + ".txt")).read_bytes())

    def test_checker_groups_share_every_compiled_probe_source(self):
        spec = importlib.util.spec_from_file_location("fixture_catalog_checks", image.TOOLS / "check-tooling.py")
        checks = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checks)
        for os_name in ("linux", "windows"):
            with self.subTest(os=os_name):
                groups = checks.template_groups(os_name)
                self.assertEqual(groups, image.unit_template_groups(os_name))
                self.assertEqual({name for name in groups["probe"] if not name.endswith("_test.go")},
                                 set(image.probe_templates(os_name)))
                self.assertIn("probe_test.go", groups["probe"])
                self.assertIn("smb_observer_test.go", groups["probe"])
                self.assertFalse(any(name.startswith("smb_") for name in groups["controller"]))
                for files in groups.values():
                    self.assertEqual(len(files), len(set(files)))
                if os_name == "windows":
                    self.assertIn("smb_probe_windows_test.go", groups["probe"])
                    self.assertIn("smb_mapping_windows_test.go", groups["probe"])
                    self.assertNotIn("controller_windows_test.go", groups["probe"])
                    self.assertNotIn("init", groups)
                else:
                    self.assertFalse(any("_windows" in name for name in groups["probe"]))
                    self.assertEqual(groups["init"], ("init_linux.go", "init_linux_test.go"))

    def test_dependency_graph_binds_smb_and_authority_embedded_sources(self):
        root = self.root / "repository"
        files = {
            "go.mod": b"module fixture\n",
            "go.sum": b"module hashes\n",
            "cmd/remote-fs-server/main.go": b"package main\n",
            "packages/transport/httprest/client.go": b"package httprest\n",
            "packages/smb/server.go": b"package smb\n",
            "packages/smb/windows/auth.go": b"package windows\n",
            "packages/smb/internal/wire/packet.go": b"package wire\n",
            "packages/metastore/sqlite/internal/schema/migrations/0006.sql": b"SELECT 1;\n",
            "packages/smb/server_test.go": b"package smb\n",
            "packages/unrelated/unused.go": b"package unused\n",
            ".tmp/cache/external.go": b"package external\n",
            ".github/scripts/native-current-authority/smb_probe_windows.go.txt": b"package main\n",
        }
        for name, data in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        calls = []

        def go_list(args, env, capture=False, diagnostics=False):
            self.assertEqual(args[:4], ["go", "list", "-deps", "-json"])
            self.assertTrue(capture)
            self.assertFalse(diagnostics)
            calls.append((env["GOOS"], env["GOARCH"], args[4:]))
            if env["GOOS"] == "linux":
                packages = [{"Dir": str(root / "cmd/remote-fs-server")},
                            {"Dir": str(root / "packages/metastore/sqlite/internal/schema"),
                             "EmbedFiles": ["migrations/0006.sql"]}]
            else:
                packages = [{"Dir": str(root / name)} for name in (
                    "packages/transport/httprest", "packages/smb", "packages/smb/windows", "packages/smb/internal/wire")]
            packages += [{"Dir": str(self.root / "external-module")}, {"Dir": str(root / ".tmp/cache")}]
            return "\n\n".join(json.dumps(package) for package in packages)

        listed = "\0".join(files).encode() + b"\0"
        with mock.patch.object(image, "ROOT", root), mock.patch.object(image, "command", side_effect=go_list), \
                mock.patch.object(image.subprocess, "check_output", return_value=listed):
            directories = image.dependency_directories({"GOWORK": "off"})
            before = image.source_snapshot(directories)
            selected = set(files) - {"packages/smb/server_test.go", "packages/unrelated/unused.go", ".tmp/cache/external.go"}
            self.assertEqual(set(before), selected)
            (root / "packages/smb/server.go").write_bytes(b"package smb\nvar boundSource = 1\n")
            after = image.source_snapshot(directories)
            self.assertEqual({name for name in before if before[name] != after[name]}, {"packages/smb/server.go"})
        self.assertEqual(calls, [
            ("linux", "amd64", ["./cmd/remote-fs-server"]),
            ("windows", "arm64", ["./packages/transport/httprest", "./packages/smb", "./packages/smb/windows"]),
        ])

    def test_dependency_graph_cannot_omit_a_requested_windows_package(self):
        def go_list(args, env, **kwargs):
            targets = args[4:]
            if env["GOOS"] == "windows":
                targets = [target for target in targets if target != "./packages/smb/windows"]
            return "\n".join(json.dumps({"Dir": str(image.ROOT / target.removeprefix("./"))}) for target in targets)

        with mock.patch.object(image, "command", side_effect=go_list), \
                self.assertRaisesRegex(ValueError, "windows dependency inventory lacks requested packages: packages/smb/windows"):
            image.dependency_directories({})


if __name__ == "__main__":
    unittest.main()
