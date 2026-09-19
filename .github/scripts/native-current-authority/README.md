# Current authority fixture

This fixture connects the current Windows ARM64 HTTP client to the current Linux
authority. Native Windows ARM64 QEMU runs the x86-64 Linux guest under TCG. Its
SQLite database, native lease witness and local objects live on a private ext4
image. The Windows host exposes one loopback HTTP forwarding port and a separate,
controller-owned serial-control listener. No guest network download, external
service or public endpoint is required.

The default invocation checks authority readiness and ordinary restart. The
workflow explicitly adds `-CurrentSMBCold`, forwarded as `-current-smb-cold`, to
check one cold file through the current SMB endpoint and system redirector. A
standalone invocation without that switch keeps the HTTP-only sequence.

Cold-open results are recorded separately from `native_acceptance: not-run`.
`require-native` remains an unavailable broader gate. One object and one HANDLE
do not establish identity uniqueness, rename/replacement retention, native
writes, directory or Explorer behavior, disconnect handling, warm-cache
invalidation or the unchanged one-second visibility requirement.

## Inputs and build

`inputs.json` pins the kernel package, extracted kernel/module hashes, QEMU bundle
and Go version. `build-image.py` verifies the package and assembles a minimal
initramfs without executing package scripts, mounting a filesystem or executing
the guest init on the host. It builds the current `cmd/remote-fs-server` and the
fixture helpers, including the current SMB/Windows dependency graph, then formats
a new 1 GiB regular file as ext4. The manifest binds
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
forward or the public storage API. [Run 35430629381](https://github.com/codetreker/remote-fs/actions/runs/35430629381)
on source `31817a904fa42896b47ce5eb64e40cbe8be73e90` passed the native Windows ARM64
HTTP fixture lifecycle: owned socket, real authority restart/reopen, exact final
ACK, recorded qualified reset, natural QEMU exit 0, non-forced empty Job and disk
cleanup. That run did not execute the current SMB cold phase or cache acceptance.
The cold phase needs its own source-bound native result.

## Readiness sequence

1. Prepare the guest and start the authority with explicit lock-state
   initialization on the new private volume.
2. Enroll a real authority lease and file session through HTTP. Exercise captured
   node references, atomic open, byte writes/reads, metadata conditions, retained
   rename/unlink and logical quota. Leave one exact sentinel and close all handles.
3. When selected, run `current-smb-cold` sequence 2 against that immutable seed.
   Its object ID and authority epoch must match `readiness-create` sequence 1.
4. Stop the authority with command 2, then restart it with command 3 without
   lock-state initialization. `readiness-reopen` sequence 2 must preserve the
   sentinel's ID, bytes and metadata under a new authority epoch; then remove it.
5. Send shutdown command 4, synchronize and unmount ext4, receive its exact ACK
   and require natural QEMU exit plus qualified stream/Job completion. Remove the
   writable copy and verify the pristine image is unchanged.

Receipts and first failure logs remain in
`.tmp/native-current-authority-fixture/windows-runs/<run-id>/evidence`. The
packaging receipt is separate. No success is inferred from the TCP listener or
the server's startup log.

## Current SMB cold phase

The Windows probe publishes one nonce-named share through the current SMB server,
SSPI and exact current-SID authorizer. Its current HTTP FileStorage reaches the
same Linux authority and immutable seed; no pinned prototype or memory authority
is substituted. The canonical volume key remains `fixture-current`.

CurrentSMBCold is supported only in a fresh isolated GitHub-hosted Windows ARM64
job, with this fixture as the sole mapping-mutating actor in its logon. The driver
requires GitHub Actions, github-hosted runner context, exact run/attempt identity
and the canonical artifact directory. The workflow passes runner context at the
cold step. Manual/shared-logon and self-hosted execution are refused. These checks
state the supported host conditions; they cannot attest to arbitrary external
actors or coordinate unrelated checkout roots.

The existing source-bound probe executable runs its own one-shot native mapping
helper through the same Windows Job owner. One bounded JSON+LF request (at most
4 KiB) and one final frame replace the mapping interpreter. The helper does not
wait for EOF or start another executable. Non-Windows builds refuse this mode.
The envelope binds action, run/source/nonce/volume/sequence, exact state identity,
SID/AuthenticationId and selected drive/UNC/port; the reply also binds the request
digest and actual owner. Missing, foreign, malformed or oversized final mutation
frames remain unknown. Exit zero alone cannot supply a missing result.

The helper validates its immediate kill-on-close/non-breakaway Job and primary
token, canonical executable/root/state paths, private ACLs and matching claim plus
pending ledger before a mutation. One four-second context covers launch, all API
pages/calls and result processing for each Snapshot/Create/Remove. The probe's
24-second context, controller's 30-second watchdog, recovery's separate 30-second
total and existing Job cleanup budgets remain. Killing a blocked helper confirms
local process exit, not provider cancellation or rollback.

Creation resolves the system MPR WNetAddConnection4W export only. It requests a
DISK resource at the exact selected drive and nonce loopback UNC, NULL auth with
length zero, and CONNECT_REQUIRE_INTEGRITY only. The one 24-byte SDK transport
option specifies Wsk and the owned TCP port; QUIC/RDMA ports, certificate-skip,
reserved bytes and padding stay zero. Profile/global/interactive/credential-save
flags remain clear. The encoding is host ABI, not network byte order.

Acceptance of this SDK option record by the selected WNet4 provider is unproved.
The first native run must bind the exact API/flags/options/status and actual
signed traffic on the owned listener. Requested integrity or a copied port is not
that observation. Missing export, unsupported options, authentication failure,
wrong traffic or any API failure fails the run without another creation API,
PowerShell/CIM/CLI fallback, implicit port 445 retry or setting change.

Inventory combines complete WNet connected-DISK enumeration, all 26 DOS/logical
observations and WNetGetConnectionW for each drive. NOT_CONNECTED is distinct
from CONNECTION_UNAVAIL; unavailable drives are occupied/unusable. A free drive
has no connected row, DOS target or logical bit and an exact not-connected lookup.
An enumerated local row must agree with its successful lookup. Other errors and
inconsistent or incomplete snapshots fail; they never become an empty inventory.

Enumeration uses a fixed aligned 1 MiB buffer, at most 128 total entries and 129
calls including a nonzero terminal probe. MORE_DATA, invalid pointers/counts or
no-progress success fail; only NO_MORE_ITEMS completes enumeration. Used strings
are copied while bounded by the buffer and existing 1024-byte row-field limit.
Lookup uses 1025 UTF-16 units. The enumeration handle is always closed, with close
errors retained. This native setup evidence deliberately does not reproduce CIM
Status==OK or prove data-path health.

A protected owner directory directly under the canonical fixture root consumes
one attempt for the SID/AuthenticationId. Its exclusive immutable synced claim
binds the run and supported host context. An existing directory blocks another
cold admission even after a clean run or partial initialization; it is never
adopted, reset or deleted. The owner lock protects the existing mutable per-run
ledger. Only exact same-attempt cleanup may re-enter. The claim and ledger remain
uploaded evidence.

Create and remove each persist pending before their sole dispatch. Only a
complete bound final frame can establish not-invoked or an API return; terminal
knowledge is saved under the owner lock. Replacement may publish terminal bytes
and still return an error, so readback is not the caller's persistence acknowledgement.
Any mutable-ledger write, sync, close or replacement error, including an observational
Save, irrevocably revokes live removal authority. A complete success frame does not
cancel independent deadline, exit, stream or cleanup errors and does not prove all
provider work has stopped.

Unknown create/remove is sticky quarantine: no further mapping mutation, replay
or force retry is allowed. A visible row or later absence cannot clear it. Local
server/process/disk cleanup and read-only evidence may finish, then the isolated
runner is discarded. Missing/corrupt ledger is not no-dispatch evidence. Initial
read-only failure still consumes the claim. A new run directory under the same
root/logon cannot bypass this state.

Only the original live attempt can hold nonserializable authority for its sole
normal removal. It requires validated create API NO_ERROR, caller-acknowledged
successful persistence of that result, confirmed helper Job quiescence and joined
streams. A terminal-looking ledger or complete success frame alone is insufficient.
Read-only snapshot failure may leave this live authority intact, but removal still
requires a freshly loaded complete matching ledger and fresh resource/baseline checks.

Consume the authority before attempting removal-pending publication or dispatch.
It cannot be retried, serialized or reconstructed after exit or any persistence
error. Then the selected drive may be passed to WNetCancelConnection2W with flags 0
and force FALSE. Success still requires API success, complete subsequent
row/lookup/DOS/logical absence and unchanged baseline. Cleanup-only re-entry is
always read-only for mappings, even when terminal success bytes or absence are
visible. It is failure-path recovery, not a step after verified normal cold success.
Observed absence cannot prove the prior caller's Save acknowledgement or settled
removal, so unproved recovered state remains an error/quarantine. It can report
current observations and local process/disk cleanup, not restore mutation authority
or clear original uncertainty. The public removal API has no generation
compare, so the fresh exclusive-logon condition matters.

The unchanged file oracle makes its first ordinary synchronous CreateFileW on the
sentinel, then uses the same HANDLE for Basic, Standard, FileIdInfo, bytes/EOF and
repeated opaque identity. It requires stability on that HANDLE, not a nonzero or
NodeID-equal value. Actual current CREATE/READ, HTTP retained-reference origin,
owned listener, authentication/signing and observed QUERY_INFO/QFid lineage remain
mandatory. No missing query is manufactured from a native API result. One object
does not prove replacement identity, native writes, directories or warm-cache
visibility.

SMB I/O remains 64 KiB, frames 128 KiB and HTTP bodies 1 MiB. The shared native
result pool remains 8 MiB with MaxOpens/MaxRequests 16. Success also requires owned
HANDLE/mapping/server/Serve/export/HTTP and remote-reference cleanup; first errors
and unsupported classes are retained. A mapping row or requested option cannot
replace this complete cold oracle or the separate one-second cache requirement.

The retired interpreter path remains historical evidence.
[Its last first run 35459298859](https://github.com/codetreker/remote-fs/actions/runs/35459298859)
on `58d1663c19d675817d25154be8e35e6a35726c03` failed initial inventory at
4010 ms with zero stdout/stderr and no observed entry, despite the recorded system
ARM64 executable, resume count 1 and 219 input bytes written. Later object-bound
inventory controls passed in 3793/947 ms, discovery failed in 4020 ms and the
import/parse control passed in 480 ms. The original import/object/native-file stage
was not observed; this is not a diagnosis of binding or parser failure. Later
post-VM/warmed controls do not erase it.

The PowerShell mapping suite/tag and automatic post-failure controls are retired.
PowerShell still drives the existing Windows harness; it is not a mapping fallback.
The WNet4 provider/options pairing and changed-source cold path have not run
natively. Historical HTTP lifecycle proof and native helper units do not supply
that missing acceptance.

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
