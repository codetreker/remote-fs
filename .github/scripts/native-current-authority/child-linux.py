#!/usr/bin/env python3
"""Install the parent-death guard in a fresh interpreter, then replace it with the owned child."""

import ctypes
import os
import signal
import sys

if sys.platform != "linux" or len(sys.argv) < 3 or not os.path.isabs(sys.argv[2]):
    raise SystemExit("Linux child launcher requires a parent PID and an absolute executable")

parent = int(sys.argv[1])
if ctypes.CDLL(None).prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
    raise SystemExit("cannot establish child parent-death signal")
if os.getppid() != parent:
    raise SystemExit("owning controller exited before child launch")
os.execv(sys.argv[2], sys.argv[2:])
