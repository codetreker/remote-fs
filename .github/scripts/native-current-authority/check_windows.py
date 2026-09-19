"""Own the Windows checker and its descendants in one non-breakaway Job Object."""

import ctypes
import json
import os
import sys
import threading

_DWORD = ctypes.c_uint32
_BOOL = ctypes.c_int32
_HANDLE = ctypes.c_void_p
_SIZE_T = ctypes.c_size_t
_LARGE_INTEGER = ctypes.c_int64
_ULONGLONG = ctypes.c_uint64
_JOB_OBJECT_LIMIT_ACTIVE_PROCESS = 0x00000008
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION = 1
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_IMAGE_FILE_MACHINE_ARM64 = 0xAA64
_HANDLE_FLAG_INHERIT = 1
_exit_process = os._exit


class _BasicLimits(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", _LARGE_INTEGER),
        ("PerJobUserTimeLimit", _LARGE_INTEGER),
        ("LimitFlags", _DWORD),
        ("MinimumWorkingSetSize", _SIZE_T),
        ("MaximumWorkingSetSize", _SIZE_T),
        ("ActiveProcessLimit", _DWORD),
        ("Affinity", _SIZE_T),
        ("PriorityClass", _DWORD),
        ("SchedulingClass", _DWORD),
    ]


class _IOCounters(ctypes.Structure):
    _fields_ = [(name, _ULONGLONG) for name in (
        "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
        "ReadTransferCount", "WriteTransferCount", "OtherTransferCount",
    )]


class _ExtendedLimits(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimits),
        ("IoInfo", _IOCounters),
        ("ProcessMemoryLimit", _SIZE_T),
        ("JobMemoryLimit", _SIZE_T),
        ("PeakProcessMemoryUsed", _SIZE_T),
        ("PeakJobMemoryUsed", _SIZE_T),
    ]


class _BasicAccounting(ctypes.Structure):
    _fields_ = [
        ("TotalUserTime", _LARGE_INTEGER),
        ("TotalKernelTime", _LARGE_INTEGER),
        ("ThisPeriodTotalUserTime", _LARGE_INTEGER),
        ("ThisPeriodTotalKernelTime", _LARGE_INTEGER),
        ("TotalPageFaultCount", _DWORD),
        ("TotalProcesses", _DWORD),
        ("ActiveProcesses", _DWORD),
        ("TotalTerminatedProcesses", _DWORD),
    ]


def _check_abi():
    if ctypes.sizeof(_HANDLE) != 8 or ctypes.sizeof(_SIZE_T) != 8:
        raise RuntimeError("Windows checker requires a 64-bit Python process")
    if (ctypes.sizeof(_BasicLimits), ctypes.sizeof(_IOCounters),
            ctypes.sizeof(_ExtendedLimits), ctypes.sizeof(_BasicAccounting)) != (64, 48, 144, 48):
        raise RuntimeError("Windows Job Object structure layout differs from the 64-bit ABI")
    if (_BasicLimits.MinimumWorkingSetSize.offset, _BasicLimits.Affinity.offset,
            _ExtendedLimits.IoInfo.offset, _ExtendedLimits.ProcessMemoryLimit.offset,
            _BasicAccounting.ActiveProcesses.offset) != (24, 48, 64, 112, 40):
        raise RuntimeError("Windows Job Object field alignment differs from the 64-bit ABI")


class _WindowsAPI:
    def __init__(self):
        if sys.platform != "win32":
            raise RuntimeError("WindowsJob requires a native Windows ARM64 process")
        _check_abi()
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        signatures = {
            "GetCurrentProcess": ([], _HANDLE),
            "IsWow64Process2": ([_HANDLE, ctypes.POINTER(ctypes.c_uint16), ctypes.POINTER(ctypes.c_uint16)], _BOOL),
            "CreateJobObjectW": ([ctypes.c_void_p, ctypes.c_wchar_p], _HANDLE),
            "SetHandleInformation": ([_HANDLE, _DWORD, _DWORD], _BOOL),
            "GetHandleInformation": ([_HANDLE, ctypes.POINTER(_DWORD)], _BOOL),
            "SetInformationJobObject": ([_HANDLE, ctypes.c_int32, ctypes.c_void_p, _DWORD], _BOOL),
            "AssignProcessToJobObject": ([_HANDLE, _HANDLE], _BOOL),
            "IsProcessInJob": ([_HANDLE, _HANDLE, ctypes.POINTER(_BOOL)], _BOOL),
            "QueryInformationJobObject": ([_HANDLE, ctypes.c_int32, ctypes.c_void_p, _DWORD, ctypes.POINTER(_DWORD)], _BOOL),
            "TerminateJobObject": ([_HANDLE, _DWORD], _BOOL),
            "CloseHandle": ([_HANDLE], _BOOL),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.kernel, name)
            function.argtypes, function.restype = arguments, result
        process_machine, native_machine = ctypes.c_uint16(), ctypes.c_uint16()
        self.current = self.kernel.GetCurrentProcess()
        self._checked("IsWow64Process2", self.current, ctypes.byref(process_machine), ctypes.byref(native_machine))
        if process_machine.value != 0 or native_machine.value != _IMAGE_FILE_MACHINE_ARM64:
            raise RuntimeError("Windows checker must run natively on ARM64")

    def _checked(self, name, *arguments):
        value = getattr(self.kernel, name)(*arguments)
        if not value:
            cause = ctypes.WinError(ctypes.get_last_error())
            raise OSError(f"{name}: {cause}") from cause
        return value

    def create_job(self):
        return self._checked("CreateJobObjectW", None, None)

    def disallow_handle_inheritance(self, handle):
        self._checked("SetHandleInformation", handle, _HANDLE_FLAG_INHERIT, 0)
        flags = _DWORD()
        self._checked("GetHandleInformation", handle, ctypes.byref(flags))
        if flags.value & _HANDLE_FLAG_INHERIT:
            raise RuntimeError("Windows checker job handle remains inheritable")

    def set_limits(self, handle, flags, active_limit=0):
        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = flags
        limits.BasicLimitInformation.ActiveProcessLimit = active_limit
        self._checked("SetInformationJobObject", handle, _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                      ctypes.byref(limits), ctypes.sizeof(limits))

    def assign_current(self, handle):
        self._checked("AssignProcessToJobObject", handle, self.current)

    def check_membership(self, handle):
        member = _BOOL()
        self._checked("IsProcessInJob", self.current, handle, ctypes.byref(member))
        if member.value != 1:
            raise RuntimeError("Current checker is not a member of its owned job")

    def active_processes(self, handle):
        accounting, returned = _BasicAccounting(), _DWORD()
        self._checked("QueryInformationJobObject", handle, _JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
                      ctypes.byref(accounting), ctypes.sizeof(accounting), ctypes.byref(returned))
        if returned.value != ctypes.sizeof(accounting):
            raise RuntimeError("Windows job accounting returned an unexpected byte count")
        return accounting.ActiveProcesses

    def terminate_job(self, handle):
        self._checked("TerminateJobObject", handle, 1)

    def close_handle(self, handle):
        self._checked("CloseHandle", handle)


def _report_fatal(message):
    # A full or broken stderr pipe must not prevent timeout termination.
    finished = threading.Event()

    def write():
        try:
            print(json.dumps({"subsystem": "windows-checker-job", "category": "failure",
                              "cause": str(message)[:2048], "exit_code": 1}), file=sys.stderr, flush=True)
        except (OSError, ValueError):
            pass
        finally:
            finished.set()

    try:
        threading.Thread(target=write, daemon=True).start()
        finished.wait(0.1)
    except BaseException:
        # Fatal cleanup cannot depend on starting a diagnostic thread.
        return


class WindowsJob:
    """Construct before any subprocess; keep alive until the checker has finished.

    Cancel and join timeout threads before close_success. terminate never returns:
    it terminates this Python process together with every process in the job.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._api = _WindowsAPI()
        self._handle = None
        self._flags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        self._active_limit = 0
        assigned = False
        try:
            self._handle = self._api.create_job()
            self._api.disallow_handle_inheritance(self._handle)
            self._api.set_limits(self._handle, self._flags)
            self._api.assign_current(self._handle)
            assigned = True
            self._api.check_membership(self._handle)
        except BaseException:
            if assigned:
                self.terminate()
            if self._handle is not None:
                self._api.close_handle(self._handle)
                self._handle = None
            raise

    def _active_handle(self):
        if self._handle is None:
            raise RuntimeError("Windows checker job has already been released")
        return self._handle

    def assert_idle(self):
        with self._lock:
            count = self._api.active_processes(self._active_handle())
            if count != 1:
                raise RuntimeError(f"Windows checker job still has {count} active processes; expected only the checker")

    def close_success(self):
        with self._lock:
            handle = self._active_handle()
            self.assert_idle()
            # Freeze child admission before the final count and kill-flag release.
            # https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_limit_information
            self._flags |= _JOB_OBJECT_LIMIT_ACTIVE_PROCESS
            self._active_limit = 1
            self._api.set_limits(handle, self._flags, self._active_limit)
            self.assert_idle()
            self._flags &= ~_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            self._api.set_limits(handle, self._flags, self._active_limit)
            self._api.close_handle(handle)
            self._handle = None

    def terminate(self):
        with self._lock:
            try:
                _report_fatal("Terminating the checker and its owned Windows job")
                if self._handle is not None:
                    try:
                        self._api.set_limits(self._handle, self._flags | _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                                             self._active_limit)
                    except BaseException as error:
                        _report_fatal(error)
                    try:
                        self._api.terminate_job(self._handle)
                    except BaseException as error:
                        _report_fatal(error)
                    finally:
                        try:
                            self._api.close_handle(self._handle)
                        except BaseException as error:
                            _report_fatal(error)
                        self._handle = None
            finally:
                # Exit also closes a handle whose explicit CloseHandle failed.
                _exit_process(1)
                raise RuntimeError("Forced process exit unexpectedly returned")
