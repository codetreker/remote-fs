#!/usr/bin/env python3
"""Compile and test the hidden fixture templates with exact root/verdict accounting."""

import argparse
import base64
import binascii
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import threading

ROOT = Path(__file__).resolve().parents[3]
TOOLS = Path(__file__).resolve().parent
WINDOWS_JOB = None
DIAGNOSTIC_ROOT = "TestMappingPowerShellStartupDiagnostic"
DIAGNOSTIC_TAG = "rfs_mapping_startup_diagnostic"
DIAGNOSTIC_CELLS = ("01_entry_open", "02_entry_eof", "03_inventory_open", "04_inventory_eof",
                    "05_resolve_json_command", "06_import_utility_parse")
DIAGNOSTIC_TEMPLATE = "mapping_startup_diagnostic_windows_test.go"


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes(paths):
    return {path.relative_to(ROOT).as_posix(): checksum(path) for path in paths}


def canonical_source_key(name):
    if (not isinstance(name, str) or not name or "\\" in name or ":" in name
            or PurePosixPath(name).is_absolute() or ".." in PurePosixPath(name).parts
            or PurePosixPath(name).as_posix() != name or name == "."):
        raise ValueError("source manifest key is not a canonical POSIX relative path: " + repr(name))
    return name


def run(args, directory, log, env, timeout=180, on_abort=None):
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
    def terminate_owned():
        try:
            if on_abort is not None:
                on_abort("owned command exceeded its watchdog deadline; cleanup is unconfirmed")
        finally:
            WINDOWS_JOB.terminate()

    timer = threading.Timer(timeout, terminate_owned)
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
            terminate_owned()
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


def diagnostic_templates():
    return image_catalog().probe_templates("windows") + (DIAGNOSTIC_TEMPLATE,)


def strict_json(text):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate JSON field: " + key)
            result[key] = value
        return result

    def invalid(value):
        raise ValueError("invalid JSON constant: " + value)

    return json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)


def bounded_text(path, limit):
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("diagnostic evidence exceeds its size limit: " + path.name)
    return data.decode("utf-8")


def validate_capture(stream):
    if not isinstance(stream, dict) or set(stream) != {"observed_bytes", "prefix_base64", "prefix_truncated"}:
        raise ValueError("inventory capture lacks its exact count/prefix/truncation fields")
    count, encoded, truncated = stream["observed_bytes"], stream["prefix_base64"], stream["prefix_truncated"]
    if type(count) is not int or not 0 <= count <= (1 << 63) - 1:
        raise ValueError("inventory capture byte count is not a nonnegative signed 64-bit integer")
    if not isinstance(encoded, str) or len(encoded) > 2732 or type(truncated) is not bool:
        raise ValueError("inventory capture has an invalid prefix or truncation flag")
    try:
        prefix = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError("inventory capture prefix is not canonical base64") from error
    if (base64.b64encode(prefix).decode("ascii") != encoded or len(prefix) != min(count, 2048)
            or truncated != (count > len(prefix))):
        raise ValueError("inventory capture count, bounded prefix and truncation disagree")


def validate_inventory_timeout(error, diagnostic):
    if not isinstance(error, str) or error.split("\n", 1)[0] != "context deadline exceeded":
        raise ValueError("initial inventory failure is not an original command timeout")
    if (not isinstance(diagnostic, dict) or diagnostic.get("action") != "inventory"
            or diagnostic.get("action_truncated") is not False or diagnostic.get("eof_requested") is not False):
        raise ValueError("cold mapping failure is not the initial open-input inventory")
    for name in ("stdout_observed", "stderr_observed"):
        validate_capture(diagnostic.get(name))
    started, joined = diagnostic.get("workers_started"), diagnostic.get("workers_joined")
    if (diagnostic.get("cleanup_confirmed") is not True or type(started) is not int or type(joined) is not int
            or not 0 <= started <= 3 or joined != started):
        raise ValueError("initial inventory cleanup or worker joins are unconfirmed")
    for count_key, hash_key, maximum in (("input_bytes", "input_sha256", 4096),
                                         ("script_utf16_units", "script_utf16le_sha256", (1 << 63) - 1)):
        count, digest = diagnostic.get(count_key), diagnostic.get(hash_key)
        if type(count) is not int or not 0 <= count <= maximum or not isinstance(digest, str):
            raise ValueError("initial inventory lacks valid input/script observation fields")
        if (count == 0 and digest != "") or (count > 0 and not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise ValueError("initial inventory input/script observation length and hash disagree")


def diagnostic_prerequisite(run_id, source_sha, output, sources):
    if not re.fullmatch(r"[0-9]+-[0-9]+", run_id) or not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("diagnostic requires the workflow run/attempt and source SHA")
    prior = ROOT / ".tmp/native-current-authority-fixture/windows-runs" / run_id / "evidence"
    manifest_path = ROOT / ".tmp/native-current-authority-fixture/artifact/manifest.json"
    paths = [prior / name for name in ("receipt.json", "cold-observations.jsonl", "events.jsonl")]
    texts = [bounded_text(path, 1 << 20) for path in paths]
    receipt = strict_json(texts[0])
    identity = {"run_id": run_id, "source_sha": source_sha, "fixture_nonce": receipt.get("fixture_nonce")}
    if not isinstance(identity["fixture_nonce"], str) or not re.fullmatch(r"[0-9a-f]{64}", identity["fixture_nonce"]):
        raise ValueError("cold receipt has no valid fixture nonce")

    def bound(value):
        if any(value.get(key) != expected for key, expected in identity.items()):
            raise ValueError("diagnostic prerequisite has foreign run/source/nonce evidence")

    bound(receipt)
    if (receipt.get("fixture_readiness") != "failed" or receipt.get("current_smb_cold_requested") is not True
            or not isinstance(receipt.get("cause"), str) or "current-smb-cold:" not in receipt["cause"]):
        raise ValueError("diagnostic requires a failed current SMB cold attempt")
    observations = [strict_json(line) for line in texts[1].splitlines()]
    events = [strict_json(line) for line in texts[2].splitlines()]
    for row in observations + events:
        bound(row)
    cold = [row["event"] for row in observations]
    if [event.get("kind") for event in cold] != ["smb-host", "smb-cleanup", "cold-result"]:
        raise ValueError("cold trace does not identify the initial mapping inventory failure")
    host, cleanup, result = cold
    if (host.get("status") != "ok" or not re.fullmatch(r"S-1-[0-9-]+", host.get("sid", ""))
            or cleanup.get("status") != "ok" or cleanup.get("serve_joined") is not True
            or result.get("status") != "error"):
        raise ValueError("cold trace lacks host identity or confirmed SMB cleanup")
    failure = [row for row in events if row.get("phase") == "current-smb-cold"]
    recovery = [row for row in events if row.get("phase") == "current-smb-cleanup"]
    if (len(failure) != 1 or failure[0].get("category") != "probe-failed"
            or failure[0]["details"].get("sequence") != 2 or len(recovery) != 1
            or recovery[0].get("category") != "mapping-recovery"
            or recovery[0]["details"].get("status") != "ok"):
        raise ValueError("cold child or mapping recovery cleanup is unconfirmed")
    marker = "mapping command diagnostic: "
    error = result.get("error", "")
    if not isinstance(error, str) or error.count(marker) != 1:
        raise ValueError("cold failure must contain exactly one mapping diagnostic")
    diagnostic = strict_json(error.split(marker, 1)[1])
    validate_inventory_timeout(error, diagnostic)
    if (prior / "smb-mapping.json").exists():
        raise ValueError("cold inventory left a mapping ledger")
    manifest = strict_json(bounded_text(manifest_path, 4 << 20))
    if manifest.get("source_sha") != source_sha or manifest.get("source_dirty") is not False:
        raise ValueError("diagnostic source manifest differs from the failed cold source")
    for name, digest in manifest["source_files"].items():
        canonical_source_key(name)
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("source manifest digest is not a lowercase SHA256: " + name)
    if any(manifest["source_files"].get(canonical_source_key(path)) != value for path, value in sources.items()):
        raise ValueError("diagnostic template/checker bytes differ from the cold source manifest")
    for name, expected in manifest["source_files"].items():
        path = ROOT / name
        if (Path(name).is_absolute() or ".." in Path(name).parts or not path.resolve().is_relative_to(ROOT.resolve())
                or checksum(path) != expected):
            raise ValueError("diagnostic dependency bytes differ from the cold source manifest: " + name)
    config = {"format": 1, **identity, "prior_evidence_directory": str(prior),
              "evidence_directory": str(output), "native_evidence_directory": str(output / "native-evidence"),
              "owner_sid": host["sid"],
              "initial_inventory_error": error, "initial_inventory_diagnostic": diagnostic}
    if len(json.dumps(config).encode()) > 64 << 10:
        raise ValueError("diagnostic config exceeds 64 KiB")
    provenance = {"files": source_hashes([*paths, manifest_path]),
                  "source_files": manifest["source_files"], "cold_failure": error,
                  "fixture_failure": receipt["cause"], "identity": identity}
    return config, provenance


def diagnostic_audit(path):
    expected = [DIAGNOSTIC_ROOT, *(DIAGNOSTIC_ROOT + "/" + name for name in DIAGNOSTIC_CELLS)]
    started, verdicts, packages, errors = [], {}, [], []
    try:
        lines = bounded_text(path, 16 << 20).splitlines()
    except (OSError, ValueError) as error:
        lines = []
        errors.append(str(error))
    for ordinal, line in enumerate(lines, 1):
        try:
            value = strict_json(line)
            if not isinstance(value, dict) or not isinstance(value.get("Action"), str):
                raise ValueError("Go event must have an action")
            action, name = value["Action"], value.get("Test")
            if name is not None and (not isinstance(name, str) or not name):
                raise ValueError("Go event has an invalid test name")
            if name and action == "run":
                if name in started:
                    errors.append("duplicate test start: " + name)
                started.append(name)
            if action in ("pass", "fail", "skip"):
                if name:
                    if name in verdicts:
                        errors.append("duplicate test verdict: " + name)
                    verdicts[name] = action
                else:
                    packages.append(action)
        except (ValueError, KeyError, TypeError) as error:
            errors.append(f"invalid Go JSON event at line {ordinal}: {error}")
    if started != expected:
        errors.append("diagnostic root/cell starts differ from the exact ordered inventory")
    if set(verdicts) != set(started):
        errors.append("diagnostic started/verdict accounting differs")
    if packages != ["pass"]:
        errors.append("diagnostic package did not report one passing verdict")
    failed = [name for name, verdict in verdicts.items() if verdict == "fail"]
    skipped = [name for name, verdict in verdicts.items() if verdict == "skip"]
    if failed or skipped:
        errors.append("diagnostic contains failed or skipped tests")
    return {"status": "failed" if errors else "passed", "expected": expected, "started": started,
            "verdicts": verdicts, "missing_verdicts": sorted(set(started) - set(verdicts)),
            "not_started": [name for name in expected if name not in started],
            "failed": failed, "skipped": skipped, "package_verdicts": packages, "errors": errors}


def run_startup_diagnostic(args, output, env):
    receipt_path = output / "diagnostic-receipt.json"
    receipt = {"format": 1, "mode": "mapping-startup-diagnostic", "status": "running",
               "run_id": args.run_id, "source_sha": args.source_sha, "phase": "prerequisite",
               "cleanup_confirmed": False, "errors": [], "sources": {}, "commands": []}
    receipt_lock = threading.RLock()

    def save():
        with receipt_lock:
            temporary = output / "diagnostic-receipt.partial"
            temporary.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
            temporary.replace(receipt_path)

    def abort(cause):
        with receipt_lock:
            receipt.update(status="aborted", cleanup_confirmed=False, abort_cause=cause)
            receipt["audit"] = diagnostic_audit(output / "test.jsonl")
            save()

    def command(arguments, log):
        with receipt_lock:
            receipt["commands"].append(arguments)
            save()
        return run(arguments, ROOT, output / log, env, timeout=120, on_abort=abort)

    save()
    try:
        if sys.platform != "win32" or args.race:
            raise ValueError("startup diagnostic requires native Windows without race mode")
        files = diagnostic_templates()
        source_paths = [TOOLS / (name + ".txt") for name in files]
        source_paths += [TOOLS / name for name in ("check-tooling.py", "build-image.py", "check_windows.py")]
        receipt["sources"] = source_hashes(source_paths)
        config, receipt["prerequisite"] = diagnostic_prerequisite(
            args.run_id, args.source_sha, output, receipt["sources"])
        config_path = output / "mapping-startup-config.json"
        config_path.write_text(json.dumps(config, separators=(",", ":")) + "\n", encoding="utf-8")
        env = {**env, "RFS_MAPPING_STARTUP_CONFIG": str(config_path)}
        directory = output / "probe"
        directory.mkdir()
        for name in files:
            shutil.copyfile(TOOLS / (name + ".txt"), directory / name)
        receipt["phase"] = "go-version"
        version = command(["go", "version"], "go-version.log").strip()
        receipt["go_version"] = version
        if version != "go version go1.26.8 windows/arm64":
            raise ValueError("startup diagnostic requires Go1.26.8 windows/arm64")
        receipt["phase"] = "inventory"
        inventory = command(["go", "test", "-count=1", "-tags=" + DIAGNOSTIC_TAG,
                             "-list", "^Test", str(directory)], "inventory.log")
        if [line for line in inventory.splitlines() if line.startswith("Test")] != [DIAGNOSTIC_ROOT]:
            raise ValueError("startup diagnostic test root inventory differs")
        receipt["phase"] = "cells"
        try:
            command(["go", "test", "-json", "-count=1", "-timeout=120s", "-tags=" + DIAGNOSTIC_TAG,
                     "-run=^" + DIAGNOSTIC_ROOT + "$", str(directory)], "test.jsonl")
        except Exception as error:
            receipt["errors"].append(str(error))
        receipt["audit"] = diagnostic_audit(output / "test.jsonl")
        if receipt["audit"]["status"] != "passed":
            receipt["errors"].append("diagnostic Go start/verdict audit failed")
        native_path = output / "native-evidence" / "mapping-startup.json"
        native = strict_json(bounded_text(native_path, 1 << 20))
        receipt["native_receipt"] = {"path": native_path.relative_to(output).as_posix(),
                                     "sha256": checksum(native_path), "details": native}
        if any(native.get(key) != config[key] for key in ("format", "run_id", "source_sha", "fixture_nonce",
                                                        "initial_inventory_error", "initial_inventory_diagnostic")):
            raise ValueError("native startup receipt differs from the admitted cold evidence")
        if native.get("status") != "passed" or native.get("aborted") is not False or native.get("cleanup_confirmed") is not True:
            receipt["errors"].append("native startup cells failed or cleanup is unconfirmed")
    except Exception as error:
        receipt["errors"].append(str(error))
    finally:
        if "audit" not in receipt:
            receipt["audit"] = diagnostic_audit(output / "test.jsonl")
        try:
            before = {**receipt.get("prerequisite", {}).get("source_files", {}), **receipt["sources"]}
            if any(checksum(ROOT / path) != value for path, value in before.items()):
                receipt["errors"].append("fixture source changed during startup diagnostic")
        except OSError as error:
            receipt["errors"].append("cannot verify diagnostic source stability: " + str(error))
        try:
            if WINDOWS_JOB is not None:
                WINDOWS_JOB.assert_idle()
                WINDOWS_JOB.close_success()
                receipt["checker_cleanup_confirmed"] = True
            native_cleanup = receipt.get("native_receipt", {}).get("details", {}).get("cleanup_confirmed")
            receipt["cleanup_confirmed"] = receipt.get("checker_cleanup_confirmed") is True and native_cleanup is True
        except Exception as error:
            abort("checker Job cleanup is unconfirmed: " + str(error))
            WINDOWS_JOB.terminate()
        receipt["status"] = "failed" if receipt["errors"] else "passed"
        receipt["phase"] = "complete"
        save()
    if receipt["errors"]:
        raise RuntimeError("startup diagnostic failed; see " + str(receipt_path))
    return receipt


def main():
    global WINDOWS_JOB
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--mapping-startup-diagnostic", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--source-sha")
    args = parser.parse_args()
    if args.mapping_startup_diagnostic != bool(args.run_id and args.source_sha) or (
            not args.mapping_startup_diagnostic and (args.run_id or args.source_sha)):
        parser.error("startup diagnostic requires --run-id and --source-sha together")
    output = Path(args.output).resolve()
    if not output.is_relative_to(ROOT / ".tmp") or output == ROOT / ".tmp" or output.exists():
        raise ValueError("checks require a new output directory below the worktree .tmp")
    if args.mapping_startup_diagnostic and output.parent != ROOT / ".tmp/native-current-authority-fixture/unit-windows":
        raise ValueError("startup diagnostic output must be a new child of unit-windows")
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
    if args.mapping_startup_diagnostic:
        run_startup_diagnostic(args, output, env)
        return
    version = run(["go", "version"], ROOT, output / "go-version.log", env).strip()
    if "go1.26.8" not in version.split():
        raise ValueError("fixture checks require Go1.26.8")
    if sys.platform not in ("linux", "win32"):
        raise ValueError("fixture tooling supports only Linux and Windows hosts")
    platform = "windows" if sys.platform == "win32" else "other"
    groups = template_groups("windows" if sys.platform == "win32" else "linux")
    receipt = {"go_version": version, "platform": sys.platform, "race": args.race, "groups": {}}
    for name, files in groups.items():
        directory = output / name
        directory.mkdir()
        hashes = {}
        for filename in files:
            source = TOOLS / (filename + ".txt")
            hashes[source.relative_to(ROOT).as_posix()] = checksum(source)
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
