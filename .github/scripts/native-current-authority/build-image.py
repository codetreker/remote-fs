#!/usr/bin/env python3
"""Assemble the current authority fixture; never mount or execute its guest init."""

import argparse
import datetime
import gzip
import hashlib
import io
import json
import lzma
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
TOOLS = Path(__file__).resolve().parent
PINS = json.loads((TOOLS / "inputs.json").read_text())


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1 << 20), b""):
            h.update(data)
    return h.hexdigest()


def owned_path(value):
    path = Path(value).resolve()
    if not path.is_relative_to(ROOT / ".tmp") or path == ROOT / ".tmp":
        raise ValueError("fixture outputs must be below this worktree's .tmp")
    return path


def command(args, env=None, capture=False):
    print(json.dumps({"command": [str(x) for x in args]}), flush=True)
    if capture:
        return subprocess.check_output(args, cwd=ROOT, env=env, text=True, stderr=subprocess.STDOUT).strip()
    subprocess.run(args, cwd=ROOT, env=env, check=True)


def download(url, path, expected, size):
    if path.exists():
        if path.stat().st_size != size or digest(path) != expected:
            raise ValueError(f"cached input does not match its pin: {path.name}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    started = time.monotonic()
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "remote-fs-fixture-builder/1"})
        with urllib.request.urlopen(request, timeout=60) as response, partial.open("xb") as output:
            count = 0
            while data := response.read(1 << 20):
                count += len(data)
                if count > size or time.monotonic() - started > 300:
                    raise ValueError("pinned download exceeded its byte/time bound")
                output.write(data)
        if partial.stat().st_size != size or digest(partial) != expected:
            raise ValueError("download does not match the pinned size/hash")
        partial.replace(path)
    finally:
        partial.unlink(missing_ok=True)


def extract_kernel(package, target):
    expected = {"vmlinuz-" + PINS["kernel"]["release"]: ("vmlinuz", PINS["kernel"]["vmlinuz_sha256"]),
                "config-" + PINS["kernel"]["release"]: ("config", PINS["kernel"]["config_sha256"])}
    for name, checksum in PINS["kernel"]["modules"].items():
        expected[name + ".ko.xz"] = ("modules/" + name + ".ko", checksum)
    found = set()
    process = subprocess.Popen(["dpkg-deb", "--fsys-tarfile", str(package)], stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            for entry in archive:
                key = Path(entry.name).name
                if key not in expected:
                    continue
                if not entry.isfile() or key in found or entry.size > 32 << 20:
                    raise ValueError("invalid or duplicate pinned kernel member")
                data = archive.extractfile(entry).read()
                if key.endswith(".xz"):
                    data = lzma.decompress(data)
                relative, checksum = expected[key]
                if hashlib.sha256(data).hexdigest() != checksum:
                    raise ValueError("kernel member hash mismatch: " + key)
                output = target / relative
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(data)
                found.add(key)
        if process.wait() != 0 or found != set(expected):
            raise ValueError("kernel package lacks the exact required members")
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()


def cpio_entry(stream, inode, name, mode, data=b"", rdev=(0, 0)):
    raw_name = name.encode() + b"\0"
    fields = [inode, mode, 0, 0, 1, 0, len(data), 0, 0, rdev[0], rdev[1], len(raw_name), 0]
    header = b"070701" + b"".join(f"{value:08x}".encode() for value in fields)
    stream.write(header + raw_name)
    stream.write(b"\0" * (-len(header + raw_name) % 4))
    stream.write(data)
    stream.write(b"\0" * (-len(data) % 4))


def initramfs(path, files):
    with path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as stream:
        inode = 1
        for name in (".", "data", "dev", "modules", "proc", "sys", "tmp"):
            cpio_entry(stream, inode, name, stat.S_IFDIR | (0o1777 if name == "tmp" else 0o755))
            inode += 1
        for name, device, permissions in (("dev/console", (5, 1), 0o600), ("dev/null", (1, 3), 0o666)):
            cpio_entry(stream, inode, name, stat.S_IFCHR | permissions, rdev=device)
            inode += 1
        for name, (data, permissions) in sorted(files.items()):
            cpio_entry(stream, inode, name, stat.S_IFREG | permissions, data)
            inode += 1
        cpio_entry(stream, inode, "TRAILER!!!", 0)


def source_snapshot(package_directories=None):
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
    ).decode().split("\0")
    result = {}
    for name in paths:
        path = ROOT / name
        if not name or not path.is_file() or path.is_symlink():
            continue
        selected = (name.startswith(("packages/", "cmd/remote-fs-server/")) if package_directories is None
                    else str(Path(name).parent) in package_directories)
        production = selected and (
            name.endswith(".sql") or name.endswith(".go") and not name.endswith("_test.go"))
        tooling = name.startswith(".github/scripts/native-current-authority/")
        if production or tooling or name in ("go.mod", "go.sum", ".github/workflows/native-current-authority.yml"):
            result[name] = digest(path)
    return result


def dependency_directories(env):
    directories = set()
    for os_name, targets in (("linux", ["./cmd/remote-fs-server"]), ("windows", ["./packages/transport/httprest"])):
        arch = "arm64" if os_name == "windows" else "amd64"
        raw = command(["go", "list", "-deps", "-json", *targets], {**env, "GOOS": os_name, "GOARCH": arch}, capture=True)
        decoder, offset = json.JSONDecoder(), 0
        while offset < len(raw):
            package, used = decoder.raw_decode(raw[offset:])
            offset += used
            while offset < len(raw) and raw[offset].isspace():
                offset += 1
            directory = Path(package["Dir"]).resolve()
            if not directory.is_relative_to(ROOT):
                continue
            relative = directory.relative_to(ROOT)
            if relative.parts and relative.parts[0] == ".tmp":
                continue
            directories.add(str(relative))
            for embedded in package.get("EmbedFiles", []):
                directories.add(str((relative / embedded).parent))
    if not directories:
        raise ValueError("current authority/client module dependency inventory is empty")
    return directories


def build_environment(work):
    env = os.environ.copy()
    env.update({"GOENV": "off", "GOTOOLCHAIN": "local", "GOWORK": "off", "GOMAXPROCS": "2", "GOFLAGS": "-mod=readonly"})
    for key, suffix in (("GOCACHE", "go-cache/build"), ("GOMODCACHE", "go-cache/mod"), ("GOPATH", "go-cache/gopath")):
        candidate = Path(env[key]).resolve() if env.get(key) else ROOT / ".tmp" / suffix
        env[key] = str(owned_path(candidate))
        candidate.mkdir(parents=True, exist_ok=True)
    temporary = work / "tmp"
    temporary.mkdir()
    env.update({"TMPDIR": str(temporary), "TMP": str(temporary), "TEMP": str(temporary)})
    env.pop("GOTMPDIR", None)
    env["CGO_ENABLED"] = "0"
    return env


def compile_templates(work, output, names, env, os_name, arch):
    directory = work / output.stem
    directory.mkdir()
    inputs = []
    for name in names:
        source = TOOLS / (name + ".txt")
        target = directory / name
        shutil.copyfile(source, target)
        inputs.append(str(target))
    specific = {**env, "GOOS": os_name, "GOARCH": arch}
    command(["go", "build", "-trimpath", "-buildvcs=false", "-o", str(output), *inputs], specific)


def artifact(path, directory):
    return {"path": str(path.relative_to(directory)), "sha256": digest(path), "size": path.stat().st_size}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--cache", default=str(ROOT / ".tmp/native-current-authority-fixture/downloads"))
    parser.add_argument("--allow-dirty", action="store_true", help="local preparation only; Windows CI refuses dirty provenance")
    args = parser.parse_args()
    output, cache = owned_path(args.output), owned_path(args.cache)
    if output.exists():
        raise ValueError("artifact output must not already exist")
    source_sha = command(["git", "rev-parse", "HEAD"], capture=True)
    tree_sha = command(["git", "rev-parse", "HEAD^{tree}"], capture=True)
    dirty = bool(command(["git", "status", "--porcelain", "--untracked-files=normal"], capture=True))
    if dirty and not args.allow_dirty:
        raise ValueError("artifact build requires a clean checkout; local preparation must explicitly allow dirty inputs")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir()
    for directory in ("kernel", "guest", "host", "evidence"):
        (output / directory).mkdir()
    work = Path(tempfile.mkdtemp(prefix="image-build-", dir=output.parent))
    env = build_environment(work)
    go_version = command(["go", "version"], env, capture=True)
    if PINS["go_version"] not in go_version.split():
        raise ValueError("expected Go1.26.8; source the task Go environment")
    package_directories = dependency_directories(env)
    before = source_snapshot(package_directories)
    package = cache / "linux-image.deb"
    download(PINS["kernel"]["url"], package, PINS["kernel"]["sha256"], PINS["kernel"]["size"])
    extract_kernel(package, output / "kernel")
    init = output / "guest/init-linux-amd64"
    authority = output / "guest/remote-fs-server-linux-amd64"
    compile_templates(work, init, ["init_linux.go"], env, "linux", "amd64")
    command(["go", "build", "-trimpath", "-buildvcs=false", "-o", str(authority), "./cmd/remote-fs-server"],
            {**env, "GOOS": "linux", "GOARCH": "amd64"})
    binaries = {"init_linux_amd64": init, "authority_linux_amd64": authority}
    for arch in ("arm64", "amd64"):
        probe = output / f"host/probe-windows-{arch}.exe"
        controller = output / f"host/controller-windows-{arch}.exe"
        compile_templates(work, probe, ["probe.go", "private_windows.go"], env, "windows", arch)
        compile_templates(work, controller, ["controller.go", "controller_windows.go", "private_windows.go"], env, "windows", arch)
        binaries[f"probe_windows_{arch}"] = probe
        binaries[f"controller_windows_{arch}"] = controller
    probe = output / "host/probe-linux-amd64"
    compile_templates(work, probe, ["probe.go", "private_other.go"], env, "linux", "amd64")
    binaries["probe_linux_amd64"] = probe
    boot_token = hashlib.sha256((source_sha + digest(init) + digest(authority) + PINS["kernel"]["vmlinuz_sha256"]).encode()).hexdigest()
    config = json.dumps({"source_sha": source_sha, "boot_token": boot_token}, separators=(",", ":")).encode() + b"\n"
    files = {"init": (init.read_bytes(), 0o555), "remote-fs-server": (authority.read_bytes(), 0o555),
             "fixture-config.json": (config, 0o400)}
    for name in PINS["kernel"]["modules"]:
        files[f"modules/{name}.ko"] = ((output / f"kernel/modules/{name}.ko").read_bytes(), 0o444)
    image = output / "guest/initramfs.cpio.gz"
    initramfs(image, files)
    data = output / "guest/pristine.ext4"
    with data.open("xb") as stream:
        stream.truncate(PINS["disk_bytes"])
    command(["mke2fs", "-t", "ext4", "-F", "-q", "-m", "0", "-L", "RFS_FIXTURE",
             "-E", "lazy_itable_init=0,lazy_journal_init=0", str(data)])
    final_directories = dependency_directories(env)
    if final_directories != package_directories:
        raise ValueError("current authority/client dependency graph changed during artifact build")
    after = source_snapshot(final_directories)
    if before != after:
        changed = sorted(name for name in before.keys() | after.keys() if before.get(name) != after.get(name))
        raise ValueError("source changed during artifact build: " + ", ".join(changed))
    artifacts = {"kernel": artifact(output / "kernel/vmlinuz", output),
                 "kernel_config": artifact(output / "kernel/config", output),
                 "initramfs": artifact(image, output), "data": artifact(data, output)}
    artifacts.update({name: artifact(path, output) for name, path in binaries.items()})
    artifacts.update({"module_" + name.replace("-", "_"): artifact(output / f"kernel/modules/{name}.ko", output)
                      for name in PINS["kernel"]["modules"]})
    manifest = {"format": 1, "source_sha": source_sha, "source_tree_sha": tree_sha,
                "source_dirty": dirty, "source_files": after, "volume": PINS["volume"],
                "boot_token": boot_token, "quota_bytes": PINS["quota_bytes"], "disk_bytes": PINS["disk_bytes"],
                "build": {"go_version": PINS["go_version"], "goos": "linux", "goarch": "amd64", "cgo_enabled": "0",
                          "flags": ["-trimpath", "-buildvcs=false"],
                          "tool_versions": {"go": go_version, "python": sys.version.splitlines()[0],
                                            "mke2fs": command(["mke2fs", "-V"], capture=True),
                                            "dpkg_deb": command(["dpkg-deb", "--version"], capture=True)}},
                "qemu": {key: PINS["qemu"][key] for key in ("version", "url", "sha512", "machine")},
                "artifacts": artifacts}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "manifest.sha256").write_text(digest(output / "manifest.json") + "  manifest.json\n")
    (output / "evidence/source-before.json").write_text(json.dumps(before, indent=2) + "\n")
    (output / "evidence/dependency-directories.json").write_text(json.dumps(sorted(package_directories), indent=2) + "\n")
    shutil.rmtree(work)
    print(json.dumps({"artifact": str(output), "manifest_sha256": digest(output / "manifest.json"),
                      "source_sha": source_sha, "source_dirty": dirty}), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"fixture image build failed: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1)
