#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
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

    def prerequisite(self):
        self.run_id, self.source_sha, self.nonce = "12345-1", "a" * 40, "b" * 64
        self.identity = {"run_id": self.run_id, "source_sha": self.source_sha, "fixture_nonce": self.nonce}
        self.prior = self.directory / ".tmp/native-current-authority-fixture/windows-runs" / self.run_id / "evidence"
        self.prior.mkdir(parents=True)
        self.output = self.directory / ".tmp/native-current-authority-fixture/unit-windows/mapping-startup"
        self.output.mkdir(parents=True)
        stream = {"observed_bytes": 0, "prefix_base64": "", "prefix_truncated": False}
        self.diagnostic = {"action": "inventory", "action_truncated": False,
                           "stdout_observed": stream.copy(), "stderr_observed": stream.copy()}
        self.cold = [
            {"kind": "smb-host", "status": "ok", "sid": "S-1-5-21-1000"},
            {"kind": "smb-cleanup", "status": "ok", "serve_joined": True},
            {"kind": "cold-result", "status": "error", "error": "context deadline exceeded\nmapping command diagnostic: " + json.dumps(self.diagnostic)},
        ]
        self.events = [
            {"phase": "current-smb-cold", "category": "probe-failed", "details": {"sequence": 2}},
            {"phase": "current-smb-cleanup", "category": "mapping-recovery", "details": {"sequence": 2, "status": "ok"}},
        ]
        self.receipt = {**self.identity, "fixture_readiness": "failed", "current_smb_cold_requested": True,
                        "cause": "current-smb-cold: original cold failure"}
        source = self.directory / ".github/scripts/native-current-authority/check-tooling.py"
        source.parent.mkdir(parents=True)
        source.write_text("source-bound checker")
        self.sources = {str(source.relative_to(self.directory)): checks.checksum(source)}
        self.manifest = {"source_sha": self.source_sha, "source_dirty": False, "source_files": self.sources.copy()}
        self.manifest_path = self.directory / ".tmp/native-current-authority-fixture/artifact/manifest.json"
        self.manifest_path.parent.mkdir(parents=True)
        self.write_prerequisite()

    def write_prerequisite(self):
        (self.prior / "receipt.json").write_text(json.dumps(self.receipt))
        (self.prior / "cold-observations.jsonl").write_text("\n".join(
            json.dumps({**self.identity, "event": event}) for event in self.cold))
        (self.prior / "events.jsonl").write_text("\n".join(json.dumps({**self.identity, **event}) for event in self.events))
        self.manifest_path.write_text(json.dumps(self.manifest))

    def admit(self):
        with mock.patch.object(checks, "ROOT", self.directory):
            return checks.diagnostic_prerequisite(self.run_id, self.source_sha, self.output, self.sources)

    def test_diagnostic_catalog_uses_only_production_probe_and_tagged_test(self):
        catalog = checks.diagnostic_templates()
        self.assertEqual(catalog[:-1], checks.image_catalog().probe_templates("windows"))
        self.assertEqual(catalog[-1], checks.DIAGNOSTIC_TEMPLATE)
        self.assertEqual([name for name in catalog if name.endswith("_test.go")], [checks.DIAGNOSTIC_TEMPLATE])
        self.assertNotIn("controller.go", catalog)
        for platform in ("linux", "windows"):
            for files in checks.template_groups(platform).values():
                self.assertNotIn(checks.DIAGNOSTIC_TEMPLATE, files)

    def test_prerequisite_binds_exact_cold_failure_identity_and_source_bytes(self):
        self.prerequisite()
        config, provenance = self.admit()
        self.assertEqual(set(config), {"format", *self.identity, "prior_evidence_directory", "evidence_directory",
                                       "owner_sid", "initial_inventory_diagnostic"})
        self.assertEqual(config["initial_inventory_diagnostic"], self.diagnostic)
        self.assertEqual(config["owner_sid"], "S-1-5-21-1000")
        self.assertEqual(config["prior_evidence_directory"], str(self.prior))
        self.assertEqual(provenance["fixture_failure"], self.receipt["cause"])
        self.assertEqual(len(provenance["files"]), 4)

    def test_prerequisite_rejects_missing_foreign_successful_or_unbound_evidence(self):
        self.prerequisite()
        cases = (
            ("missing", lambda: (self.prior / "receipt.json").unlink()),
            ("foreign-run", lambda: self.receipt.update(run_id="other")),
            ("foreign-source", lambda: self.receipt.update(source_sha="d" * 40)),
            ("successful-cold", lambda: self.receipt.update(fixture_readiness="ok")),
            ("not-cold", lambda: self.receipt.update(current_smb_cold_requested=False)),
            ("dirty-source", lambda: self.manifest.update(source_dirty=True)),
            ("changed-bytes", lambda: self.manifest["source_files"].update({next(iter(self.sources)): "e" * 64})),
        )
        original_receipt, original_manifest = json.dumps(self.receipt), json.dumps(self.manifest)
        for name, change in cases:
            with self.subTest(name=name):
                self.receipt, self.manifest = json.loads(original_receipt), json.loads(original_manifest)
                if name == "missing":
                    self.write_prerequisite()
                    change()
                else:
                    change()
                    self.write_prerequisite()
                with self.assertRaises((ValueError, FileNotFoundError)):
                    self.admit()

    def test_prerequisite_rejects_later_inventory_streams_and_unknown_cleanup(self):
        self.prerequisite()
        original_cold, original_events = json.dumps(self.cold), json.dumps(self.events)
        mutations = (
            lambda: self.cold.insert(1, {"kind": "native", "status": "ok"}),
            lambda: self.cold[1].update(serve_joined=False),
            lambda: self.events[1]["details"].update(status="blocked"),
            lambda: self.events.append(self.events[0].copy()),
            lambda: self.cold[2].update(error='mapping command diagnostic: {"action":"create","action_truncated":false}'),
            lambda: self.cold[2].update(error="mapping command diagnostic: " + json.dumps({
                **self.diagnostic, "stderr_observed": {"observed_bytes": 1, "prefix_base64": "eA==", "prefix_truncated": False}})),
            lambda: self.cold[2].update(error=self.cold[2]["error"] + self.cold[2]["error"]),
        )
        for number, change in enumerate(mutations):
            with self.subTest(number=number):
                self.cold, self.events = json.loads(original_cold), json.loads(original_events)
                change()
                self.write_prerequisite()
                with self.assertRaises(ValueError):
                    self.admit()
        self.cold, self.events = json.loads(original_cold), json.loads(original_events)
        self.write_prerequisite()
        (self.prior / "smb-mapping.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "ledger"):
            self.admit()

    def test_prerequisite_json_rejects_duplicate_fields_and_oversize(self):
        self.prerequisite()
        for data in ('{"run_id":"one","run_id":"two"}', '{"value":NaN}', " " * ((1 << 20) + 1)):
            with self.subTest(data=data[:50]):
                (self.prior / "receipt.json").write_text(data)
                with self.assertRaises(ValueError):
                    self.admit()

    def diagnostic_events(self, fail=None):
        root = checks.DIAGNOSTIC_ROOT
        rows = [{"Action": "run", "Test": root}]
        for cell in checks.DIAGNOSTIC_CELLS:
            rows.extend(({"Action": "run", "Test": root + "/" + cell},
                         {"Action": "fail" if cell == fail else "pass", "Test": root + "/" + cell}))
        verdict = "fail" if fail else "pass"
        return rows + [{"Action": verdict, "Test": root}, {"Action": verdict}]

    def test_diagnostic_audit_retains_failed_cells_and_all_verdicts(self):
        path = self.directory / "diagnostic.jsonl"
        for failing in (None, checks.DIAGNOSTIC_CELLS[0]):
            with self.subTest(failing=failing):
                path.write_text("\n".join(json.dumps(row) for row in self.diagnostic_events(failing)))
                result = checks.diagnostic_audit(path)
                self.assertEqual(result["status"], "failed" if failing else "passed")
                self.assertEqual(len(result["started"]), 5)
                self.assertEqual(len(result["verdicts"]), 5)
                self.assertEqual(result["missing_verdicts"], [])
                self.assertEqual(result["not_started"], [])
                self.assertEqual(len(result["failed"]), 2 if failing else 0)

    def test_diagnostic_audit_rejects_skips_duplicates_missing_and_wrong_order(self):
        path = self.directory / "diagnostic.jsonl"
        good = self.diagnostic_events()
        cases = [good[:2], good + [good[2]], good + [good[1]],
                 [*good[:2], {**good[2], "Action": "skip"}, *good[3:]],
                 [good[0], *good[3:5], *good[1:3], *good[5:]], good[:-1]]
        for rows in cases:
            with self.subTest(rows=rows):
                path.write_text("\n".join(json.dumps(row) for row in rows))
                self.assertEqual(checks.diagnostic_audit(path)["status"], "failed")
        path.write_text("compiler error\n")
        result = checks.diagnostic_audit(path)
        self.assertEqual(len(result["not_started"]), 5)
        self.assertIn("invalid Go JSON event", result["errors"][0])
        for malformed in (None, [], {"Action": "run", "Test": []}, {"Action": None}, {"Action": "pass", "Test": 1}):
            with self.subTest(malformed=malformed):
                path.write_text(json.dumps(malformed))
                result = checks.diagnostic_audit(path)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["started"], [])

    def test_prerequisite_rejects_changed_dependency_bytes(self):
        self.prerequisite()
        dependency = self.directory / "packages/smb/server.go"
        dependency.parent.mkdir(parents=True)
        dependency.write_text("package smb\n")
        self.manifest["source_files"]["packages/smb/server.go"] = checks.checksum(dependency)
        self.write_prerequisite()
        self.admit()
        dependency.write_text("package smb\nvar changed = true\n")
        with self.assertRaisesRegex(ValueError, "dependency bytes"):
            self.admit()

    def diagnostic_driver(self, fail=None, early_error=None, invalid_prior=False, native_missing=False, unknown_job=False):
        self.prerequisite()
        tools = self.directory / ".github/scripts/native-current-authority"
        tools.mkdir(parents=True, exist_ok=True)
        files = ("probe.go", checks.DIAGNOSTIC_TEMPLATE)
        sources = [tools / (name + ".txt") for name in files]
        sources += [tools / name for name in ("check-tooling.py", "build-image.py", "check_windows.py")]
        for path in sources:
            path.write_text("fixture source: " + path.name)
        self.sources = {str(path.relative_to(self.directory)): checks.checksum(path) for path in sources}
        self.manifest["source_files"] = self.sources.copy()
        if invalid_prior:
            self.receipt["source_sha"] = "f" * 40
        self.write_prerequisite()
        calls = []

        def execute(command, directory, log, env, timeout, on_abort):
            calls.append(command)
            self.assertEqual(timeout, 120)
            self.assertEqual(directory, self.directory)
            config = json.loads(Path(env["RFS_MAPPING_STARTUP_CONFIG"]).read_text())
            self.assertEqual(config["initial_inventory_diagnostic"], self.diagnostic)
            if command == ["go", "version"]:
                if early_error:
                    raise RuntimeError(early_error)
                return "go version go1.26.8 windows/arm64\n"
            self.assertIn("-tags=" + checks.DIAGNOSTIC_TAG, command)
            self.assertIn("-count=1", command)
            if "-list" in command:
                return checks.DIAGNOSTIC_ROOT + "\nok fixture\n"
            self.assertIn("-run=^" + checks.DIAGNOSTIC_ROOT + "$", command)
            self.assertIn("-timeout=120s", command)
            self.assertFalse(any(value.startswith("-coverprofile") for value in command))
            log.write_text("\n".join(json.dumps(row) for row in self.diagnostic_events(fail)))
            if not native_missing:
                native = {"format": 1, **self.identity, "status": "failed" if fail else "passed",
                          "aborted": False, "cleanup_confirmed": True, "cells": list(checks.DIAGNOSTIC_CELLS)}
                (self.output / "mapping-startup.json").write_text(json.dumps(native))
            if fail:
                raise RuntimeError("command exited 1: native cell failure")
            return log.read_text()

        class Terminated(BaseException):
            pass

        job = mock.Mock(spec=["assert_idle", "close_success", "terminate"])
        if unknown_job:
            job.assert_idle.side_effect = RuntimeError("unconfirmed live descendants")
            job.terminate.side_effect = Terminated
        args = SimpleNamespace(run_id=self.run_id, source_sha=self.source_sha, race=False)
        with (mock.patch.object(checks, "ROOT", self.directory), mock.patch.object(checks, "TOOLS", tools),
              mock.patch.object(checks.sys, "platform", "win32"), mock.patch.object(checks, "WINDOWS_JOB", job),
              mock.patch.object(checks, "diagnostic_templates", return_value=files), mock.patch.object(checks, "run", side_effect=execute)):
            if unknown_job:
                with self.assertRaises(Terminated):
                    checks.run_startup_diagnostic(args, self.output, {})
            elif fail or early_error or invalid_prior or native_missing:
                with self.assertRaisesRegex(RuntimeError, "startup diagnostic failed"):
                    checks.run_startup_diagnostic(args, self.output, {})
            else:
                checks.run_startup_diagnostic(args, self.output, {})
        return json.loads((self.output / "diagnostic-receipt.json").read_text()), calls, job

    def test_diagnostic_driver_writes_failed_receipt_after_all_native_cell_verdicts(self):
        receipt, calls, job = self.diagnostic_driver(fail="01_entry_open")
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(receipt["audit"]["verdicts"]), 5)
        self.assertEqual(receipt["audit"]["failed"], [checks.DIAGNOSTIC_ROOT + "/01_entry_open", checks.DIAGNOSTIC_ROOT])
        self.assertIn("command exited 1", receipt["errors"][0])
        self.assertEqual(receipt["prerequisite"]["fixture_failure"], self.receipt["cause"])
        self.assertTrue(receipt["cleanup_confirmed"])
        job.close_success.assert_called_once()

    def test_diagnostic_driver_success_is_separate_from_original_cold_failure(self):
        receipt, calls, _ = self.diagnostic_driver()
        self.assertEqual(receipt["status"], "passed")
        self.assertEqual(receipt["prerequisite"]["fixture_failure"], "current-smb-cold: original cold failure")
        self.assertEqual(len(calls), 3)
        self.assertEqual(self.sources, receipt["sources"])
        self.assertEqual(json.loads((self.prior / "receipt.json").read_text())["fixture_readiness"], "failed")

    def test_diagnostic_driver_never_launches_for_foreign_prerequisite(self):
        receipt, calls, _ = self.diagnostic_driver(invalid_prior=True)
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(calls, [])
        self.assertFalse((self.output / "mapping-startup-config.json").exists())

    def test_diagnostic_driver_preserves_early_go_failure_receipt(self):
        receipt, calls, _ = self.diagnostic_driver(early_error="go version failed")
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(calls, [["go", "version"]])
        self.assertIn("go version failed", receipt["errors"])
        self.assertFalse(receipt["cleanup_confirmed"])

    def test_diagnostic_driver_missing_native_receipt_keeps_cleanup_unknown(self):
        receipt, _, _ = self.diagnostic_driver(native_missing=True)
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["audit"]["status"], "passed")
        self.assertFalse(receipt["cleanup_confirmed"])

    def test_diagnostic_driver_records_unknown_checker_cleanup_before_termination(self):
        receipt, _, job = self.diagnostic_driver(unknown_job=True)
        self.assertEqual(receipt["status"], "aborted")
        self.assertFalse(receipt["cleanup_confirmed"])
        self.assertIn("unconfirmed live descendants", receipt["abort_cause"])
        job.terminate.assert_called_once()
        job.close_success.assert_not_called()

    def test_workflow_diagnostic_follows_only_the_failed_cold_step(self):
        workflow = (checks.ROOT / ".github/workflows/native-current-authority.yml").read_text()
        cold = workflow.index("id: current_smb_cold")
        diagnostic = workflow.index("if: ${{ failure() && steps.current_smb_cold.outcome == 'failure' }}")
        upload = workflow.index("name: Preserve first observations")
        self.assertLess(cold, diagnostic)
        self.assertLess(diagnostic, upload)
        self.assertIn("--output .tmp/native-current-authority-fixture/unit-windows/mapping-startup", workflow)
        self.assertIn("timeout-minutes: 15", workflow)
        self.assertNotIn("continue-on-error", workflow)
        self.assertNotIn("timeout-minutes:", workflow[diagnostic:upload])


if __name__ == "__main__":
    unittest.main()
