#!/usr/bin/env python3
"""Compile and test the hidden fixture templates with exact root/verdict accounting."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading

ROOT = Path(__file__).resolve().parents[3]
TOOLS = Path(__file__).resolve().parent
WINDOWS_JOB = None


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes(paths):
    return {path.relative_to(ROOT).as_posix(): checksum(path) for path in paths}


def run(args, directory, log, env, timeout=180):
    print(json.dumps({"command": args}), flush=True)
    if sys.platform == "linux":
        spec = importlib.util.spec_from_file_location("fixture_linux_owner", TOOLS / "run-linux.py")
        owner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(owner)
        executable = shutil.which(args[0], path=env.get("PATH"))
        if executable is None:
            raise ValueError("command executable was not found: " + args[0])
        child = owner.OwnedChild([executable, *args[1:]], directory, log.parent, log.stem,
                                 output_limit=16 << 20, stream_output=True, env=env)
        try:
            child.wait(timeout)
        finally:
            child.finish()
            shutil.copyfile(log.parent / (log.stem + "-stdout.log"), log)
        if child.forced:
            raise RuntimeError("command left live descendants requiring termination")
        return bytes(child.stdout).decode()
    process = subprocess.Popen(args, cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    timer = threading.Timer(timeout, WINDOWS_JOB.terminate)
    timer.start()
    try:
        with log.open("xb") as output:
            for line in process.stdout:
                output.write(line)
                output.flush()
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
        code = process.wait()
        if code:
            raise RuntimeError(f"command exited {code}: {args}")
        WINDOWS_JOB.assert_idle()
    finally:
        timer.cancel()
        timer.join()
        if process.poll() is None:
            WINDOWS_JOB.terminate()
        process.wait()
    return log.read_text()


def audit(path, expected):
    started, verdicts = set(), {}
    for line in path.read_text().splitlines():
        value = json.loads(line)
        name, action = value.get("Test"), value["Action"]
        if name and action == "run":
            started.add(name)
        if name and action in ("pass", "fail", "skip"):
            if name in verdicts:
                raise ValueError("duplicate test verdict: " + name)
            verdicts[name] = action
        if not name and action in ("fail", "skip"):
            raise ValueError("package failed or skipped")
    roots = {name for name in started if "/" not in name}
    if roots != expected or set(verdicts) != started or any(v != "pass" for v in verdicts.values()):
        raise ValueError("root inventory or started/verdict accounting differs")
    return {"roots": len(roots), "verdicts": len(verdicts), "fail": 0, "skip": 0}


def image_catalog():
    spec = importlib.util.spec_from_file_location("fixture_image_catalog", TOOLS / "build-image.py")
    image = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(image)
    return image


def template_groups(os_name):
    return image_catalog().unit_template_groups(os_name)


def main():
    global WINDOWS_JOB
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--race", action="store_true")
    args = parser.parse_args()
    output = Path(args.output).resolve()
    if not output.is_relative_to(ROOT / ".tmp") or output == ROOT / ".tmp" or output.exists():
        raise ValueError("checks require a new output directory below the worktree .tmp")
    output.mkdir(parents=True)
    if sys.platform == "win32":
        spec = importlib.util.spec_from_file_location("fixture_windows_checks", TOOLS / "check_windows.py")
        owner = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(owner)
        WINDOWS_JOB = owner.WindowsJob()
    env = os.environ.copy()
    env.update({"GOENV": "off", "GOTOOLCHAIN": "local", "GOWORK": "off", "GOMAXPROCS": "2", "GOFLAGS": "-mod=readonly"})
    env.pop("GOTMPDIR", None)
    for key, suffix in (("GOCACHE", "go-cache/build"), ("GOMODCACHE", "go-cache/mod"), ("GOPATH", "go-cache/gopath"),
                        ("TMPDIR", "tmp"), ("TMP", "tmp"), ("TEMP", "tmp")):
        path = ROOT / ".tmp" / suffix
        path.mkdir(parents=True, exist_ok=True)
        env[key] = str(path)
    version = run(["go", "version"], ROOT, output / "go-version.log", env).strip()
    if "go1.26.8" not in version.split():
        raise ValueError("fixture checks require Go1.26.8")
    if sys.platform not in ("linux", "win32"):
        raise ValueError("fixture tooling supports only Linux and Windows hosts")
    groups = template_groups("windows" if sys.platform == "win32" else "linux")
    receipt = {"go_version": version, "platform": sys.platform, "race": args.race, "groups": {}}
    for name, files in groups.items():
        directory = output / name
        directory.mkdir()
        hashes = source_hashes(TOOLS / (filename + ".txt") for filename in files)
        for filename in files:
            source = TOOLS / (filename + ".txt")
            shutil.copyfile(source, directory / filename)
        inventory = run(["go", "test", "-count=1", "-list", "^Test", str(directory)], ROOT, directory / "inventory.log", env, timeout=120)
        (directory / "inventory.txt").write_text(inventory)
        expected = {line for line in inventory.splitlines() if line.startswith("Test")}
        if not expected:
            raise ValueError("empty test inventory")
        command = ["go", "test", "-json", "-count=1", "-timeout=120s", "-coverprofile=" + str(directory / "coverage.out")]
        if args.race:
            command += ["-race"]
        command.append(str(directory))
        run(command, ROOT, directory / "test.jsonl", env)
        result = audit(directory / "test.jsonl", expected)
        run(["go", "vet", str(directory)], ROOT, directory / "vet.log", env)
        if any(checksum(ROOT / path) != value for path, value in hashes.items()):
            raise ValueError("fixture source changed during checks")
        receipt["groups"][name] = {**result, "sources": hashes, "command": command}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if WINDOWS_JOB is not None:
        WINDOWS_JOB.close_success()


if __name__ == "__main__":
    main()
