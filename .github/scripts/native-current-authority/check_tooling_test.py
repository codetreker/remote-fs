#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
