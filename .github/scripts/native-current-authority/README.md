# Current authority fixture

This fixture connects the current Windows ARM64 HTTP client to the current Linux
authority. Native Windows ARM64 QEMU runs the x86-64 Linux guest under TCG. Its
SQLite database, native lease witness and local objects live on a private ext4
image. The Windows host exposes one loopback HTTP forwarding port and a separate,
controller-owned serial-control listener. No guest network download, external
service or public endpoint is required.

The result is an authority readiness and restart check. It does not certify the
Windows SMB adapter, native filesystem caching or the one-second visibility
requirement. Those need the actual adapter and unchanged native acceptance cases.
The controller reports `native_acceptance: not-run`; requesting native acceptance
from this fixture fails.

## Inputs and build

`inputs.json` pins the kernel package, extracted kernel/module hashes, QEMU bundle
and Go version. `build-image.py` verifies the package and assembles a minimal
initramfs without executing package scripts, mounting a filesystem or executing
the guest init on the host. It builds the current `cmd/remote-fs-server` and the
fixture helpers, then formats a new 1 GiB regular file as ext4. The manifest binds
each artifact and source input to the checkout commit and tree.

The Windows QEMU bundle is the project-linked Weil native ARM64 11.1.0 build.
`inputs.json` pins its exact size and SHA-512; the installer is extracted with the
runner's 7-Zip and is never executed. Every extracted file, extractor version and
complete DLL/firmware tree are recorded. The controller requires ARM64 PE files
for QEMU, controller and probe, then checks QEMU version/capabilities. The
`qemu-system-x86_64.exe` name identifies the Linux guest target, not its host PE
architecture. The guest kernel, backend, VM manager and deadlines are unchanged.

Static inspection verified the archive and its bundled ARM64 import closure.
The publisher labels the Windows-on-ARM build untested; the checksum is byte
provenance, not a signing or reproducible-build claim. Actual native guest boot
and HTTP readiness remain separate gates. The previous x64 package exited with
`0xC00000FF` before guest output or authority startup; its offending unwind table
and module are unknown.

The [workflow](../../workflows/native-current-authority.yml) assembles the image on
Ubuntu 24.04 and downloads that exact artifact into the Windows 11 ARM64 job. Both
jobs check out the same source SHA with LF file bytes. Caches, downloads, temporary
files and receipts stay under the checkout's `.tmp` directory. Source drift,
dirty CI provenance, missing inputs, skips and incomplete test verdicts fail the
run. The workflow triggers only for its own scripts/workflow paths on pull
requests, with manual dispatch available after default-branch registration.

The hidden Go templates are tested explicitly by `check-tooling.py`; ordinary
module test discovery does not include them. Linux runs the init, controller and
probe unit binaries normally and with the race detector. Windows runs the native
DACL and Job Object tests before the VM check. Each invocation inventories all
test roots separately from its coverage profile.

## Guest and process ownership

PID 1 validates its PID, real/effective UID, initial RAMFS/TMPFS root, immutable
configuration and boot token before any mount or filesystem mutation. After
mounting proc, it validates the unique command-line identity before preparing the
remaining guest devices. The store belongs to UID 1000 under root-owned,
non-writable ancestors. The authority runs as UID/GID 1000.

The host creates a private writable copy of the pristine disk. QEMU uses
`cache=writeback` with guest flushes enabled, one restricted user-net interface and
one `127.0.0.1` HTTP forward. It has no shared host filesystem. These ordinary
restart checks do not establish power-loss durability of the host or hypervisor.

On Windows, directories are created with a protected owner/SYSTEM DACL. Writable
files are sealed before their content is written; reparse paths, inherited DACLs,
unexpected owners and extra ACEs are rejected. QEMU, probes and archive extraction
start suspended, enter an unnamed kill-on-close Job Object, and only then resume.
The Job handle is not inherited. Actual exit and empty-job observations are
required before deleting the writable disk. Forced termination is a failure.

## Serial control and deadlines

On Windows the controller binds one exclusive `tcp4` listener at `127.0.0.1:0`
before launching QEMU. The socket chardev connects once as a client with reconnect
disabled. The first accept closes the listener; an unexpected peer fails the run.
No bytes are parsed and no command is sent until the reversed TCP four-tuple is
matched to exactly one established row owned by the retained, live QEMU process.
The existing process handle is checked before and after the owner-table query,
with closure serialized against its sole waiter. No process is reopened by PID.
Loopback location and the subsequent nonce are not substitutes for this proof.

The TCP-owner query has a fixed 1 MiB buffer and one call; invalid bounds, unknown
ownership and any API error fail. The synchronous Windows query cannot be
canceled: the existing deadline is checked before and after it, and an expired
proof never permits command I/O. A single 120-second boot deadline covers bind,
launch, accept, peer proof and the identity-bound boot-ready frame. Commands remain
one JSON object plus LF, at most 1 KiB, with the same 30-second context and socket
write deadline. Nonce, sequence, source and phase checks remain mandatory; commands
are not paced or resent.

QEMU stdout and stderr are drained immediately into separate bounded diagnostic
logs. The authenticated serial socket carries guest control and separately framed
authority output; each stream retains its 16 MiB cap. Success requires the exact
shutdown/4 acknowledgement, natural QEMU exit 0 and all readers joined within
one 30-second shutdown deadline. Serial EOF is accepted normally. A terminal
WSAECONNRESET is eligible only at an independently tracked complete LF record
boundary and through a single error chain containing error 10054; joined errors
remain failures. The original serial result must be the one being qualified.

While waiting for the exact final ACK, an eligible reset selected ahead of an
already parsed ACK may consume and validate that queued response. It cannot infer
an ACK from reset, exit, kernel text or earlier HTTP success. After the ACK, the
same remaining shutdown deadline governs natural exit 0 and all readers. Only
then can that serial reset be recorded as `qualified-reset`, with its raw
`serial_error` retained; it is never relabeled EOF. Missing/invalid ACK, partial
trailing records, nonzero exit, timeout and other errors still fail. Non-forced
empty-Job proof, file closure and pristine-disk checks remain required.

Error cleanup closes socket I/O, finishes
the owned Job and joins readers using a single ten-second deadline fixed at cleanup
entry. Natural and forced Job stages retain their five-second caps, clamped to that
same deadline. Close errors, incomplete readers and forced termination remain
failures. Disk removal still requires process/Job quiescence.

The separate Linux validation driver retains its POSIX stdio transport. The
Windows socket does not change guest PID 1, the authority, the image, the HTTP
forward or the public storage API. Native ownership and HTTP/restart have been observed on Windows; the complete
lifecycle remains unconfirmed after a terminal reset triggered forced cleanup.
The conditional terminal-reset correction still needs its own native execution;
portable controls and cross-builds do not prove it.

## Readiness sequence

1. Prepare the guest and start the authority with explicit lock-state
   initialization on the new private volume.
2. Enroll a real authority lease and file session through HTTP. Exercise captured
   node references, atomic open, byte writes/reads, metadata conditions, retained
   rename/unlink and logical quota. Leave one exact sentinel and close all handles.
3. Stop and wait for the actual authority child. Restart the same volume without
   lock-state initialization.
4. Require the sentinel's ID, bytes and metadata to survive under a new authority
   incarnation. Remove it and close every reference/session.
5. Stop the authority, synchronize and unmount ext4, acknowledge shutdown and
   power off. Require QEMU exit and complete stream drain, then remove the writable
   copy and verify the pristine image is unchanged.

Receipts and first failure logs remain in
`.tmp/native-current-authority-fixture/windows-runs/<run-id>/evidence`. The
packaging receipt is separate. No success is inferred from the TCP listener or
the server's startup log.

## Local Linux validation

Use Go 1.26.8 with caches under this checkout. An explicitly dirty local artifact
records that fact; Windows CI accepts only clean provenance.

```sh
python3 -u .github/scripts/native-current-authority/build_image_test.py -v
python3 -u .github/scripts/native-current-authority/run_linux_test.py -v
python3 -u .github/scripts/native-current-authority/check-tooling.py --output .tmp/fixture-units
python3 -u .github/scripts/native-current-authority/build-image.py --allow-dirty --output .tmp/fixture-artifact
python3 -u .github/scripts/native-current-authority/run-linux.py \
  --allow-dirty --artifact-dir .tmp/fixture-artifact \
  --qemu /absolute/path/to/qemu-system-x86_64 --bios /absolute/path/to/pc-bios \
  --run-id local-check
```

The local controller requires an unprivileged Linux host and QEMU 11.1.0 with
TCG/slirp. It never executes the guest init directly. Its child process group and
parent-death signal belong to the run. The resulting receipt proves the Linux
guest and current HTTP composition on that host, not Windows execution.
