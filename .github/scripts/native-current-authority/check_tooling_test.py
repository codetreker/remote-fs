#!/usr/bin/env python3
from contextlib import redirect_stderr
import importlib.util
import io
import json
import os
from pathlib import Path, PureWindowsPath
import sys
import tempfile
import unittest
from unittest import mock

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("fixture_checks", Path(__file__).with_name("check-tooling.py"))
checks = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checks)


class CheckAccountingTests(unittest.TestCase):
    def setUp(self):
        parent = checks.ROOT / ".tmp/native-current-authority-fixture/checker-unit"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def test_root_inventory_requires_every_started_verdict_without_skips(self):
        good = [{"Action": "run", "Test": "TestRoot"}, {"Action": "run", "Test": "TestRoot/cell"},
                {"Action": "pass", "Test": "TestRoot/cell"}, {"Action": "pass", "Test": "TestRoot"}, {"Action": "pass"}]
        path = self.directory / "test.jsonl"
        path.write_text("\n".join(json.dumps(row) for row in good))
        self.assertEqual(checks.audit(path, {"TestRoot"}), {"roots": 1, "verdicts": 2, "fail": 0, "skip": 0})
        bad = [good[:2], good + [good[3]], [{"Action": "skip", "Test": "TestRoot"}],
               good + [{"Action": "fail"}], good[1:]]
        for rows in bad:
            with self.subTest(rows=rows):
                path.write_text("\n".join(json.dumps(row) for row in rows))
                with self.assertRaises(ValueError):
                    checks.audit(path, {"TestRoot"})

    def test_timeout_preserves_output_and_reaps_owned_command(self):
        path = self.directory / "deadline.log"
        command = [sys.executable, "-u", "-c", "import os,time; print(os.getpid(),flush=True); time.sleep(30)"]
        with self.assertRaises(TimeoutError):
            checks.run(command, self.directory, path, os.environ.copy(), timeout=0.15)
        pid = int(path.read_text())
        self.assertFalse(Path(f"/proc/{pid}").exists())

    def test_default_catalog_preserves_probe_controller_and_native_unit_sources(self):
        image = checks.image_catalog()
        for platform in ("linux", "windows"):
            with self.subTest(platform=platform):
                groups = checks.template_groups(platform)
                self.assertEqual(groups, image.unit_template_groups(platform))
                self.assertEqual(set(groups), {"probe", "controller"} if platform == "windows" else {"probe", "controller", "init"})
                self.assertEqual({name for name in groups["probe"] if not name.endswith("_test.go")}, set(image.probe_templates(platform)))
                for files in groups.values():
                    self.assertEqual(len(files), len(set(files)))
                    self.assertNotIn("mapping_startup_diagnostic_windows_test.go", files)
                self.assertIn("probe_test.go", groups["probe"])
                self.assertIn("smb_observer_test.go", groups["probe"])
                self.assertIn("controller_test.go", groups["controller"])
                self.assertIn("private_test.go", groups["controller"])
                if platform == "windows":
                    self.assertIn("smb_mapping_windows_test.go", groups["probe"])
                    self.assertIn("smb_probe_windows_test.go", groups["probe"])
                    self.assertIn("controller_windows_test.go", groups["controller"])
                    self.assertIn("private_windows_test.go", groups["controller"])
                else:
                    self.assertIn("smb_probe_other.go", groups["probe"])
                    self.assertEqual(groups["init"], ("init_linux.go", "init_linux_test.go"))

    def test_windows_ordinary_source_hashes_use_canonical_posix_keys(self):
        groups = checks.template_groups("windows")
        files = sorted({name for names in groups.values() for name in names})
        expected = checks.source_hashes(checks.TOOLS / (name + ".txt") for name in files)
        self.assertTrue(expected)
        root = PureWindowsPath("C:/a/remote-fs/remote-fs")
        paths = [root / name for name in expected]
        self.assertTrue(all(str(path.relative_to(root)) not in expected for path in paths))
        with (mock.patch.object(checks, "ROOT", root),
              mock.patch.object(checks, "checksum", side_effect=lambda path: expected[path.relative_to(root).as_posix()])):
            self.assertEqual(checks.source_hashes(paths), expected)

    def test_retired_diagnostic_arguments_fail_before_output_or_process_creation(self):
        output = self.directory / "rejected"
        for arguments in (("--mapping-startup-diagnostic",), ("--run-id", "123-1"), ("--source-sha", "a" * 40)):
            with self.subTest(arguments=arguments):
                with (mock.patch.object(checks.sys, "argv", ["check-tooling.py", "--output", str(output), *arguments]),
                      mock.patch.object(checks, "run") as launch, redirect_stderr(io.StringIO())):
                    with self.assertRaises(SystemExit) as result:
                        checks.main()
                self.assertEqual(result.exception.code, 2)
                launch.assert_not_called()
                self.assertFalse(output.exists())

    def ordinary_driver(self, *, race=False, failure=None):
        root = self.directory / ("repository-" + (failure or "success") + ("-race" if race else ""))
        tools = root / ".github/scripts/native-current-authority"
        tools.mkdir(parents=True)
        groups = {name: (name + ".go", name + "_test.go") for name in ("probe", "controller", "init")}
        for files in groups.values():
            for name in files:
                (tools / (name + ".txt")).write_text("package main\n")
        output = root / ".tmp/checks"
        calls = []

        def run(arguments, directory, log, env, timeout=180):
            calls.append((arguments, timeout))
            self.assertEqual(arguments[0], "go")
            self.assertEqual(directory, root)
            self.assertEqual(env["GOFLAGS"], "-mod=readonly")
            self.assertEqual(env["GOMAXPROCS"], "2")
            self.assertEqual(env["GOENV"], "off")
            self.assertNotIn("GOTMPDIR", env)
            for name in ("GOCACHE", "GOMODCACHE", "GOPATH", "TMPDIR", "TMP", "TEMP"):
                self.assertTrue(Path(env[name]).is_relative_to(root / ".tmp"))
            if arguments == ["go", "version"]:
                return "go version go1.26.8 linux/amd64\n"
            group = Path(arguments[-1]).name
            test = "Test" + group.title()
            if "-list" in arguments:
                self.assertEqual(timeout, 120)
                self.assertIn("-count=1", arguments)
                return "ok fixture\n" if failure == "empty-inventory" else test + "\nok fixture\n"
            if "-json" in arguments:
                self.assertIn("-count=1", arguments)
                self.assertIn("-timeout=120s", arguments)
                self.assertEqual("-race" in arguments, race)
                self.assertIn("-coverprofile=" + str(output / group / "coverage.out"), arguments)
                if failure == "wrong-root":
                    test = "TestUnexpected"
                verdict = failure if failure in ("skip", "fail") else "pass"
                rows = [{"Action": "run", "Test": test}, {"Action": "run", "Test": test + "/case"},
                        {"Action": verdict, "Test": test + "/case"}, {"Action": verdict, "Test": test}, {"Action": "pass"}]
                if failure == "missing-verdict":
                    del rows[2]
                log.write_text("\n".join(json.dumps(row) for row in rows))
                return log.read_text()
            self.assertEqual(arguments[:2], ["go", "vet"])
            if failure == "source-change":
                (tools / "probe.go.txt").write_text("package main\nvar changed = true\n")
            return ""

        arguments = ["check-tooling.py", "--output", str(output)] + (["--race"] if race else [])
        with (mock.patch.object(checks, "ROOT", root), mock.patch.object(checks, "TOOLS", tools),
              mock.patch.object(checks.sys, "platform", "linux"), mock.patch.object(checks.sys, "argv", arguments),
              mock.patch.object(checks, "WINDOWS_JOB", None), mock.patch.object(checks, "template_groups", return_value=groups),
              mock.patch.object(checks, "run", side_effect=run)):
            if failure:
                with self.assertRaises(ValueError):
                    checks.main()
                self.assertFalse((output / "receipt.json").exists())
                return calls, None
            checks.main()
        return calls, json.loads((output / "receipt.json").read_text())

    def test_ordinary_driver_preserves_all_groups_race_profiles_and_vet(self):
        for race in (False, True):
            with self.subTest(race=race):
                calls, receipt = self.ordinary_driver(race=race)
                self.assertEqual(receipt["race"], race)
                self.assertEqual(set(receipt["groups"]), {"probe", "controller", "init"})
                self.assertEqual(len(calls), 10)
                self.assertEqual(sum(arguments[:2] == ["go", "vet"] for arguments, _ in calls), 3)
                for name, group in receipt["groups"].items():
                    self.assertEqual({key: group[key] for key in ("roots", "verdicts", "fail", "skip")},
                                     {"roots": 1, "verdicts": 2, "fail": 0, "skip": 0})
                    self.assertEqual(set(group["sources"]), {
                        ".github/scripts/native-current-authority/" + name + suffix for suffix in (".go.txt", "_test.go.txt")})
                    self.assertEqual("-race" in group["command"], race)

    def test_ordinary_driver_refuses_empty_wrong_skipped_missing_failed_or_changed_sources(self):
        for failure in ("empty-inventory", "wrong-root", "skip", "missing-verdict", "fail", "source-change"):
            with self.subTest(failure=failure):
                calls, receipt = self.ordinary_driver(failure=failure)
                self.assertIsNone(receipt)
                self.assertEqual(len(calls), 2 if failure == "empty-inventory" else 4 if failure == "source-change" else 3)


if __name__ == "__main__":
    unittest.main()
