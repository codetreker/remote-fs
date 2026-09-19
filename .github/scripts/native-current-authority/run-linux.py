#!/usr/bin/env python3
"""Validate the guest artifact in an owned Linux TCG process; this is not Windows acceptance."""

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("fixture_image", Path(__file__).with_name("build-image.py"))
image = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(image)
ROOT = image.ROOT
LIMIT = 16 << 20
PREFIX = b"RFS-FIXTURE/1 "


def private_directory(path):
    path.mkdir(mode=0o700)
    if path.is_symlink() or path.stat().st_mode & 0o777 != 0o700:
        raise ValueError("run directory must be private")


def new_run_directory(parent, run_id):
    parent = image.owned_path(parent)
    parent.mkdir(parents=True, exist_ok=True)
    directory = parent / run_id
    private_directory(directory)
    return directory


def file_under(root, relative):
    value = Path(relative)
    if value.is_absolute() or not value.parts or any(p in (".", "..") for p in value.parts):
        raise ValueError("invalid artifact path")
    current = root
    for part in value.parts:
        current /= part
        if current.is_symlink():
            raise ValueError("artifact path contains a symlink")
    if not current.is_file():
        raise ValueError("artifact must be a regular file")
    return current


def verify_manifest(directory, allow_dirty):
    m = json.loads((directory / "manifest.json").read_text())
    if m["format"] != 1 or m["source_dirty"] and not allow_dirty:
        raise ValueError("dirty local artifacts require --allow-dirty")
    if m["source_sha"] != image.command(["git", "rev-parse", "HEAD"], capture=True):
        raise ValueError("artifact source differs from current checkout")
    if m["source_tree_sha"] != image.command(["git", "rev-parse", "HEAD^{tree}"], capture=True):
        raise ValueError("artifact source tree differs from current checkout")
    if m["volume"] != image.PINS["volume"] or m["qemu"]["machine"] != image.PINS["qemu"]["machine"]:
        raise ValueError("fixture volume or machine differs from pins")
    if not re.fullmatch("[0-9a-f]{64}", m["boot_token"]):
        raise ValueError("invalid boot token")
    verify_source_inventory(m["source_files"])
    for entry in m["artifacts"].values():
        path = file_under(directory, entry["path"])
        if path.stat().st_size != entry["size"] or image.digest(path) != entry["sha256"]:
            raise ValueError("artifact size or SHA256 mismatch: " + entry["path"])
    return m


def verify_source_inventory(expected):
    directories = {str(Path(path).parent) for path in expected
                   if path.endswith((".go", ".sql")) and not path.startswith(".github/")}
    actual = image.source_snapshot(directories)
    if expected != actual:
        changed = sorted(path for path in expected.keys() | actual.keys() if expected.get(path) != actual.get(path))
        raise ValueError("source inventory changed after artifact build: " + ", ".join(changed))


def qemu_arguments(m, identity, artifacts, disk, bios, port):
    append = "console=ttyS0,115200 rdinit=/init panic=1 net.ifnames=0"
    append += " RFS_FIXTURE_BOOT_TOKEN=" + m["boot_token"]
    append += " RFS_FIXTURE_NONCE=" + identity["fixture_nonce"]
    append += " RFS_FIXTURE_RUN_ID=" + identity["run_id"]
    append += " RFS_FIXTURE_VOLUME=" + m["volume"]
    return ["-L", str(bios), "-machine", m["qemu"]["machine"], "-accel", "tcg,thread=multi",
            "-cpu", "qemu64", "-smp", "2", "-m", "1024", "-nodefaults", "-no-user-config",
            "-display", "none", "-monitor", "none", "-no-reboot", "-kernel", str(artifacts["kernel"]),
            "-initrd", str(artifacts["initramfs"]), "-append", append,
            "-chardev", "stdio,id=console,signal=off", "-serial", "chardev:console",
            "-drive", "file=" + str(disk).replace(",", ",,") + ",format=raw,if=none,id=data,cache=writeback,rerror=report,werror=report",
            "-device", "virtio-blk-pci,drive=data", "-netdev",
            f"user,id=net,ipv6=off,restrict=on,hostfwd=tcp:127.0.0.1:{port}-10.0.2.15:8080",
            "-device", "virtio-net-pci,netdev=net", "-object", "rng-builtin,id=rng",
            "-device", "virtio-rng-pci,rng=rng", "-rtc", "base=utc,clock=host"]


def response(data, identity, sequence, op, phase):
    if len(data) > 1024:
        raise ValueError("guest control response exceeds 1 KiB")
    value = json.loads(data)
    if any(value.get(key) != expected for key, expected in identity.items()):
        raise ValueError("guest response identity mismatch")
    if value.get("status") != "ok":
        raise ValueError("guest reported failure: " + json.dumps(value))
    if value.get("sequence") != sequence or value.get("op") != op or value.get("phase") != phase:
        raise ValueError("guest response sequence or operation mismatch")
    if op in ("stop-authority", "shutdown") and value.get("exit_code") != 0:
        raise ValueError("guest did not confirm successful authority exit")
    return value


class OwnedChild:
    def __init__(self, command, directory, evidence, name, serial=False, output_limit=1024, stream_output=False, env=None):
        launcher = Path(__file__).with_name("child-linux.py")
        argv = [sys.executable, "-I", str(launcher), str(os.getpid()), *command]
        self.process = subprocess.Popen(argv, cwd=directory, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, start_new_session=True, env=env)
        self.responses = queue.Queue(maxsize=8)
        self.error = None
        self.stdout = bytearray()
        self.threads = []
        self.forced = False
        self.finished = False
        self.output_limit = output_limit
        self.stream_output = stream_output
        for stream, suffix in ((self.process.stdout, "stdout"), (self.process.stderr, "stderr")):
            thread = threading.Thread(target=self.read, args=(stream, evidence, name, suffix, serial), daemon=True)
            thread.start()
            self.threads.append(thread)

    def read(self, stream, evidence, name, suffix, serial):
        try:
            with (evidence / f"{name}-{suffix}.log").open("xb") as log:
                total = 0
                authority = {}
                try:
                    while line := stream.readline(65537):
                        total += len(line)
                        if total > LIMIT or len(line) > 65536:
                            raise ValueError("owned output exceeded its stream/line limit")
                        log.write(line)
                        log.flush()
                        if self.stream_output:
                            target = sys.stdout.buffer if suffix == "stdout" else sys.stderr.buffer
                            target.write(line)
                            target.flush()
                        if suffix != "stdout":
                            continue
                        if not serial:
                            self.stdout.extend(line)
                            if len(self.stdout) > self.output_limit:
                                raise ValueError(f"owned stdout exceeds {self.output_limit} bytes")
                            continue
                        line = line.rstrip(b"\r\n")
                        if line.startswith(PREFIX):
                            self.responses.put_nowait(line[len(PREFIX):])
                        elif line.startswith(b"RFS-AUTHORITY/"):
                            header, encoded = line.split(b" ", 1)
                            channel = header.removeprefix(b"RFS-AUTHORITY/").decode()
                            if channel not in ("stdout", "stderr"):
                                raise ValueError("invalid authority log channel")
                            if channel not in authority:
                                authority[channel] = (evidence / f"authority-{channel}.log").open("xb")
                            data = base64.b64decode(encoded, validate=True)
                            authority[channel].write(data)
                            authority[channel].flush()
                finally:
                    for output in authority.values():
                        output.close()
        except BaseException as error:
            self.error = error

    def receive(self, identity, sequence, op, phase, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.error:
                raise self.error
            try:
                data = self.responses.get(timeout=min(0.1, max(0.001, deadline - time.monotonic())))
                return response(data, identity, sequence, op, phase)
            except queue.Empty:
                if self.exited() is not None and not any(t.is_alive() for t in self.threads):
                    raise RuntimeError("QEMU exited before guest acknowledgement")
        raise TimeoutError("guest acknowledgement deadline: " + op)

    def command(self, identity, sequence, op):
        if not self.responses.empty():
            raise ValueError("unsolicited guest acknowledgement")
        request = {"nonce": identity["fixture_nonce"], "sequence": sequence, "command": op}
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()
        return self.receive(identity, sequence, op, op, 30)

    def wait(self, timeout):
        deadline = time.monotonic() + timeout
        while (code := self.exited()) is None:
            if time.monotonic() >= deadline:
                raise TimeoutError("owned process exit deadline")
            time.sleep(0.01)
        for thread in self.threads:
            thread.join(timeout=5)
            if thread.is_alive():
                raise TimeoutError("owned output drain did not finish")
        if self.error:
            raise self.error
        if code != 0:
            raise RuntimeError(f"owned process exited {code}")

    def exited(self):
        # The unreaped leader pins its process-group ID until finish() has inspected and stopped descendants.
        status = os.waitid(os.P_PID, self.process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
        if status is None:
            return None
        return status.si_status if status.si_code == os.CLD_EXITED else -status.si_status

    def live_group(self):
        members = []
        for path in Path("/proc").iterdir():
            if not path.name.isdecimal():
                continue
            try:
                fields = (path / "stat").read_text().rsplit(") ", 1)[1].split()
            except FileNotFoundError:
                continue
            if int(fields[2]) == self.process.pid and fields[0] not in ("Z", "X"):
                members.append(int(path.name))
        return members

    def finish(self):
        if self.finished:
            return
        if self.live_group():
            self.forced = True
            os.killpg(self.process.pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while self.live_group():
            if time.monotonic() >= deadline:
                raise RuntimeError("owned process group survived cleanup")
            time.sleep(0.01)
        self.process.wait(timeout=5)
        self.finished = True
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            stream.close()
        for thread in self.threads:
            thread.join(timeout=5)
            if thread.is_alive():
                raise TimeoutError("owned output reader survived cleanup")


def run(args):
    if sys.platform != "linux" or os.geteuid() == 0:
        raise ValueError("local validation requires an unprivileged Linux host")
    os.umask(0o077)
    directory = image.owned_path(args.artifact_dir)
    m = verify_manifest(directory, args.allow_dirty)
    executable, bios = Path(args.qemu).resolve(strict=True), Path(args.bios).resolve(strict=True)
    if not executable.is_file() or not bios.is_dir():
        raise ValueError("QEMU executable and firmware directory are required")
    version = subprocess.check_output([str(executable), "--version"], timeout=15, text=True)
    if "QEMU emulator version 11.1.0" not in version.splitlines()[0]:
        raise ValueError("Linux diagnostic QEMU must be 11.1.0")
    for name in ("bios-256k.bin", "kvmvapic.bin", "linuxboot_dma.bin"):
        file_under(bios, name)
    if not re.fullmatch("[A-Za-z0-9_-]{1,96}", args.run_id):
        raise ValueError("invalid run ID")
    parent = ROOT / ".tmp/native-current-authority-fixture/linux-runs"
    run_dir = new_run_directory(parent, args.run_id)
    evidence, work = run_dir / "evidence", run_dir / "work"
    private_directory(evidence)
    private_directory(work)
    identity = {"run_id": args.run_id, "source_sha": m["source_sha"], "fixture_nonce": secrets.token_hex(32)}
    artifacts = {name: directory / entry["path"] for name, entry in m["artifacts"].items()}
    receipt = {**identity, "host": "linux", "native_windows_acceptance": "not-run", "fixture_readiness": "failed",
               "source_dirty": m["source_dirty"], "manifest_sha256": image.digest(directory / "manifest.json"),
               "qemu": {"version": version.strip(), "executable_sha256": image.digest(executable)},
               "started_at": time.time(), "events": []}
    children = []
    failure = None
    cleanup_confirmed = True

    def event(phase, value):
        item = {"phase": phase, "at": time.time(), "value": value}
        receipt["events"].append(item)
        print(json.dumps(item), flush=True)

    def probe(phase, sequence):
        argv = [str(artifacts["probe_linux_amd64"]), "--endpoint", endpoint, "--volume", m["volume"],
                "--run-id", identity["run_id"], "--source-sha", identity["source_sha"],
                "--fixture-nonce", identity["fixture_nonce"], "--phase", phase,
                "--state-file", str(evidence / "probe-state.json"), "--sequence", str(sequence)]
        child = OwnedChild(argv, work, evidence, phase)
        children.append(child)
        child.wait(30)
        value = json.loads(child.stdout)
        if any(value.get(key) != expected for key, expected in identity.items()):
            raise ValueError("probe returned another run identity")
        if value.get("phase") != phase or value.get("sequence") != sequence or value.get("status") != "ok":
            raise ValueError("probe did not confirm its exact phase")
        if not value.get("authority_epoch") or not value.get("object_id"):
            raise ValueError("probe omitted authority/object identity")
        event(phase, value)
        return value

    try:
        disk = work / "volume.ext4"
        shutil.copyfile(artifacts["data"], disk)
        disk.chmod(0o600)
        if image.digest(disk) != m["artifacts"]["data"]["sha256"]:
            raise ValueError("private disk copy differs from artifact")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        argv = [str(executable), *qemu_arguments(m, identity, artifacts, disk, bios, port)]
        receipt["qemu"]["command"] = argv
        vm = OwnedChild(argv, work, evidence, "qemu", serial=True)
        children.append(vm)
        event("qemu-started", {"pid": vm.process.pid, "endpoint": endpoint})
        event("boot", vm.receive(identity, 0, "boot-ready", "boot", 120))
        event("start", vm.command(identity, 1, "start"))
        created = probe("readiness-create", 1)
        event("stop-authority", vm.command(identity, 2, "stop-authority"))
        event("restart", vm.command(identity, 3, "start"))
        reopened = probe("readiness-reopen", 2)
        if created["object_id"] != reopened["object_id"] or created["authority_epoch"] == reopened["authority_epoch"]:
            raise ValueError("restart did not preserve identity under a new authority incarnation")
        event("shutdown", vm.command(identity, 4, "shutdown"))
        vm.wait(30)
        if not vm.responses.empty():
            raise ValueError("guest emitted an unsolicited final response")
        receipt["fixture_readiness"] = "passed"
    except BaseException as error:
        failure = error
        receipt["cause"] = str(error)
    finally:
        for child in reversed(children):
            try:
                child.finish()
            except BaseException as error:
                cleanup_confirmed = False
                receipt.setdefault("cleanup_errors", []).append(str(error))
                failure = failure or error
            if child.forced:
                error = RuntimeError("owned process required forced termination")
                receipt.setdefault("cleanup_errors", []).append(str(error))
                failure = failure or error
        if cleanup_confirmed:
            shutil.rmtree(work)
        pristine, stable = False, False
        try:
            pristine = image.digest(artifacts["data"]) == m["artifacts"]["data"]["sha256"]
            verify_source_inventory(m["source_files"])
            stable = True
        except BaseException as error:
            failure = failure or error
            receipt["provenance_error"] = str(error)
        receipt.update({"private_disk_removed": not work.exists(), "owned_processes_exited": cleanup_confirmed,
                        "pristine_unchanged": pristine, "source_inputs_unchanged": stable, "finished_at": time.time()})
        if failure or not pristine or not stable:
            receipt["fixture_readiness"] = "failed"
        (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps({"receipt": str(evidence / "receipt.json"), "fixture_readiness": receipt["fixture_readiness"]}), flush=True)
    if failure:
        raise failure
    if receipt["fixture_readiness"] != "passed":
        raise RuntimeError("fixture source or pristine disk changed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--qemu", required=True)
    parser.add_argument("--bios", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
