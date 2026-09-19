"""Portable ABI and ownership-state tests; no Windows API is executed."""

import ctypes
import threading
import unittest
from unittest import mock

import check_windows as checker


class FakeAPI:
    def __init__(self):
        self.calls = []
        self.counts = [1]
        self.fail = set()
        self.handle = 0xFEDCBA9876543210

    def record(self, name, *args):
        self.calls.append((name, *args))
        if name in self.fail:
            raise OSError(name + " failed")

    def create_job(self):
        self.record("create")
        return self.handle

    def disallow_handle_inheritance(self, handle):
        self.record("noninherit", handle)

    def set_limits(self, handle, flags, active_limit=0):
        self.record("limits", handle, flags, active_limit)

    def assign_current(self, handle):
        self.record("assign", handle)

    def check_membership(self, handle):
        self.record("membership", handle)

    def active_processes(self, handle):
        self.record("count", handle)
        return self.counts.pop(0) if len(self.counts) > 1 else self.counts[0]

    def terminate_job(self, handle):
        self.record("terminate", handle)

    def close_handle(self, handle):
        self.record("close", handle)


class WindowsJobTests(unittest.TestCase):
    def make_job(self):
        api = FakeAPI()
        with mock.patch.object(checker, "_WindowsAPI", return_value=api):
            job = checker.WindowsJob()
        self.assertEqual(api.calls, [
            ("create",), ("noninherit", api.handle),
            ("limits", api.handle, checker._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, 0),
            ("assign", api.handle), ("membership", api.handle),
        ])
        return job, api

    def test_64_bit_structure_layout(self):
        checker._check_abi()
        self.assertEqual(ctypes.sizeof(checker._DWORD), 4)
        self.assertEqual(ctypes.sizeof(checker._BOOL), 4)
        self.assertEqual(checker._BasicLimits.ActiveProcessLimit.offset, 40)
        self.assertEqual(checker._ExtendedLimits.PeakJobMemoryUsed.offset, 136)

    def test_success_freezes_child_admission_before_releasing_kill_flag(self):
        job, api = self.make_job()
        api.calls.clear()
        job.close_success()
        self.assertEqual(api.calls, [
            ("count", api.handle),
            ("limits", api.handle, checker._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | checker._JOB_OBJECT_LIMIT_ACTIVE_PROCESS, 1),
            ("count", api.handle),
            ("limits", api.handle, checker._JOB_OBJECT_LIMIT_ACTIVE_PROCESS, 1),
            ("close", api.handle),
        ])
        with self.assertRaisesRegex(RuntimeError, "already been released"):
            job.assert_idle()

    def test_live_descendants_or_missing_checker_cannot_pass_idle(self):
        for count in (0, 2, 17):
            with self.subTest(count=count):
                job, api = self.make_job()
                api.counts = [count]
                with self.assertRaisesRegex(RuntimeError, "active processes"):
                    job.close_success()
                self.assertFalse(any(call[0] == "close" for call in api.calls))

    def test_child_racing_admission_freeze_keeps_kill_flag(self):
        job, api = self.make_job()
        api.calls.clear()
        api.counts = [1, 2]
        with self.assertRaisesRegex(RuntimeError, "2 active processes"):
            job.close_success()
        limit_calls = [call for call in api.calls if call[0] == "limits"]
        self.assertEqual(len(limit_calls), 1)
        self.assertTrue(limit_calls[0][2] & checker._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
        self.assertFalse(any(call[0] == "close" for call in api.calls))

    def test_constructor_failure_before_assignment_closes_only_owned_handle(self):
        api = FakeAPI()
        api.fail.add("assign")
        with mock.patch.object(checker, "_WindowsAPI", return_value=api):
            with self.assertRaisesRegex(OSError, "assign failed"):
                checker.WindowsJob()
        self.assertEqual(api.calls[-1], ("close", api.handle))

    def test_post_assignment_failure_exits_checker(self):
        api = FakeAPI()
        api.fail.add("membership")
        with mock.patch.object(checker, "_WindowsAPI", return_value=api), \
                mock.patch.object(checker, "_report_fatal"), \
                mock.patch.object(checker, "_exit_process", side_effect=SystemExit(1)):
            with self.assertRaises(SystemExit) as raised:
                checker.WindowsJob()
        self.assertEqual(raised.exception.code, 1)
        self.assertIn(("terminate", api.handle), api.calls)
        self.assertEqual(api.calls[-1], ("close", api.handle))

    def test_termination_failures_still_attempt_close_and_exit_one(self):
        for failures in (set(), {"terminate"}, {"limits", "terminate", "close"}):
            with self.subTest(failures=failures):
                job, api = self.make_job()
                api.calls.clear()
                api.fail = failures
                with mock.patch.object(checker, "_report_fatal"), \
                        mock.patch.object(checker, "_exit_process", side_effect=SystemExit(1)):
                    with self.assertRaises(SystemExit) as raised:
                        job.terminate()
                self.assertEqual(raised.exception.code, 1)
                self.assertIn(("terminate", api.handle), api.calls)
                self.assertEqual(api.calls[-1], ("close", api.handle))

    def test_late_timeout_after_success_is_still_failure(self):
        job, api = self.make_job()
        job.close_success()
        api.calls.clear()
        with mock.patch.object(checker, "_report_fatal"), \
                mock.patch.object(checker, "_exit_process", side_effect=SystemExit(1)):
            with self.assertRaises(SystemExit) as raised:
                job.terminate()
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(api.calls, [])

    def test_broken_diagnostic_thread_cannot_prevent_exit(self):
        job, api = self.make_job()
        with mock.patch.object(threading.Thread, "start", side_effect=RuntimeError("thread unavailable")), \
                mock.patch.object(checker, "_exit_process", side_effect=SystemExit(1)):
            with self.assertRaises(SystemExit):
                job.terminate()
        self.assertIn(("terminate", api.handle), api.calls)


if __name__ == "__main__":
    unittest.main()
