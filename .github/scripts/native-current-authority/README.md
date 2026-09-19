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

The Windows probe publishes one nonce-named share through the current `smb.Server`,
SSPI authenticator and exact current-SID authorizer. Its backend is the current
HTTP FileStorage pointing to the same Linux authority; no pinned SMB prototype,
legacy dispatcher or memory authority is used. The canonical volume key stays
`fixture-current`, independent of the share alias. The seed's original HTTP
references are already closed; SMB creates its own FileSession and references.

A free drive is selected from current SMB, DOS-device and logical-drive inventories.
The private intent ledger is written before `New-SmbMapping`. The mapping uses a
loopback alternate TCP port, required integrity, no saved credentials and no
persistent/global mapping. Windows 11 24H2+ and an elevated host are checked.
The observed drive, remote share, port, SID/logon identity and DOS-device binding
must match the ledger; no existing mapping is adopted or removed. Mapping commands
remain descendants of the existing owned Job.

The first call is ordinary synchronous `CreateFileW(OPEN_EXISTING, GENERIC_READ)`
with read/write/delete sharing and `FILE_ATTRIBUTE_NORMAL`. The same HANDLE supplies
Basic, Standard and FileIdInfo, exact file bytes and EOF, then FileIdInfo again.
Basic times and attributes match the captured seed; Standard matches logical EOF
and the current 512-byte dense virtual allocation. The complete native volume/128-bit
file-ID tuple is opaque: only repeat stability on this HANDLE is required, with no
nonzero or numeric-equality rule tying it to NodeID. A cold result becomes successful
only after the HANDLE, mapping, SMB server/export and HTTP connections close cleanly.
The first error is retained without an alternate API, flag, reseed or retry.

A bounded transparent SMB/HTTP observer requires a fresh current CREATE and READ
bound to the seeded authority object. It validates SessionID/TreeID and related
compound allocation lineage; unsupported async traffic is refused explicitly.
Actual QUERY_INFO and QFid encodings are checked against their correlated backend
identity only when observed. Native APIs may use captured CREATE metadata, so no
missing wire query is manufactured. Backend NodeID, SMB open FileID, wire fields
and opaque native identity remain separate observations. Tokens, keys and file
contents are excluded from the trace; content verification records digests.

SMB I/O is bounded at 64 KiB and frames at 128 KiB; HTTP bodies are 1 MiB.
The shared native-result pool is 8 MiB with MaxOpens/MaxRequests still 16. It covers
the 4,867,072-byte maximum of sixteen fixed Standard-state reservations; metadata
admission must not consume the wire/data frame allowance. Exhaustion still refuses
before effects. Unsupported commands/classes remain recorded first failures,
not a reason to switch to the prototype or expand backend facts.

The cold probe retains the existing 30-second execution watchdog and owned-child
cleanup. Once child quiescence is confirmed, abnormal completion may invoke
`current-smb-cleanup` sequence 2 against the same immutable seed and ledger.
This has a separate total 30-second recovery budget, including the existing Job
cleanup allowance. Unknown child quiescence blocks mapping recovery. The helper
removes only an exact owned mapping, confirms absent rows/devices, and refuses a
mismatch. Recovery preserves the original failed result. Ordinary success closes
the native HANDLE, removes the mapping, calls Shutdown, joins Serve, Unpublishes
the export, closes idle HTTP connections and verifies remote/local cleanup.

[The source-bound run 35446688647](https://github.com/codetreker/remote-fs/actions/runs/35446688647)
on `26ba565b43b9c9569a7aad5e303a9290b7a9e61d` passed the native private-writer
and Go-child environment tests. Its nested 27,917-byte diagnostic report was
persisted and cell/checker cleanup completed. The earlier protected-parent DACL
defect is closed, but initial inventory still failed at 4010 ms before-json.
Post-VM entry/open and entry/EOF passed in 248/186 ms; both inventory cells failed
at 4019 ms despite the configured System32 Modules/NUL input, including completed
EOF in the last cell. Parent-inherited and configured-child facts remain separate.

Those facts do not identify the child's ReadLine value, effective runtime module
path, command resolution or catch branch. The catch also serializes JSON, so the
last before-json marker does not establish a parser hang. Original SMB resources
and recovery settled; forced QEMU cleanup keeps the overall run failed despite
disk removal and an unchanged pristine image. Prior failures retain their result.
Current-SMB mapping, cold object access and one-second cache acceptance remain
unproved.

Mapping failures retain a bounded diagnostic after the owned child is finished
and all three I/O workers join. It records action, actual PID when available,
pre-cleanup exit observation, elapsed time and input-write progress/error. Each
stdout/stderr prefix is at most 2 KiB, base64 encoded with observed byte count and
truncation state; the diagnostic stays below 16 KiB and supplements the original
error chain. Fixed stderr stage markers cover entry, request read/parse, owner,
module and inventory; stdout retains its single result JSON. The 1 MiB stream
limit applies through `io.Copy` as well as direct Write calls. Missing prefix
bytes do not prove a stage never began, and a prefix is not complete output.

Between after-readline and before-json, bounded observations record the existing
ReadLine string's null/empty/value state, UTF-16 and re-encoded UTF-8 lengths and
SHA256 without changing the string. Parent `input_line_bytes`/`input_line_sha256`
exclude LF; the original framed input fields remain separate. Child observation
refuses more than 4096 UTF-16 units or 12288 UTF-8 bytes before allocating the
encoded array and does not invent a digest on refusal. Effective process values
for the same three environment names are read once through .NET, with state,
UTF-16 length and at most 96 bytes of base64 prefix. They are distinct from both
parent inheritance and configured child input, and do not prove module selection.

Catch first saves the original ErrorRecord. Before error JSON serialization it
emits catch-entry and bounded origin/type/error-ID facts with at most 64-byte
prefixes. Input/environment observation failures keep their own stage and stop
operation; observation of an already-caught error is secondary and the original
serialization/exit-one path still uses the saved ErrorRecord. Missing completion
cannot become success. Fixed begin/end/failure markers distinguish observation
work from the existing operation markers; request text, full environment values
and additional exception messages are not emitted.

The maximum-branch formatter projection, including existing markers and CRLF,
is bounded at 1981 bytes within the existing 2048-byte captured prefix. PowerShell's own extra error
output may still be truncated. Observer work consumes the same four seconds;
framework initialization and console writes have no separate latency guarantee.
No operation, import, environment policy, stdin/owner behavior or diagnostic cell
is changed, and no command-discovery or prewarming call is added. These new
observations have not yet run under native Windows PowerShell.

The Windows owner records CreateProcess, Job assignment and ResumeThread timings
and errors. ResumeThread must return the previous suspend count 1. The retained
original process handle supplies the actual image path and process/native machine
values before the sole waiter can close it. These synchronous queries are not
context-cancellable. Image hashing and PE inspection occur after the experiment
and describe the on-disk file then, not every byte loaded by an earlier child.

[Startup diagnostics](mapping_startup_diagnostic_windows_test.go.txt) run only
after the actual cold attempt has failed and saved source/run/nonce-bound evidence
qualifies an initial inventory command timeout, no mapping/SMB/native effects,
and settled cleanup with all started workers joined. The original first error
line must be exactly `context deadline exceeded`. Both streams may contain valid
bounded captures: canonical base64 for exactly min(observed bytes, 2048) prefix
bytes and a matching truncation flag. Contents cannot grant successful mapping;
partial or unknown stage text remains unclassified. Original input/script facts,
when present, must match; unavailable facts remain unavailable. Manifest keys are
strict POSIX-relative paths produced on both platforms, with exact hashes and no
separator fallback. The tagged test is absent from normal unit and production
catalogs. Its four sequential cells run once each: minimal immediate entry/result with stdin open,
the same script with EOF, exact read-only inventory with stdin open, then the
same inventory with EOF. Each gets the unchanged four-second command deadline
and existing owned cleanup. EOF follows one complete successful write and is
closed once by the same owner used by Finish; short writes and close errors fail.
The ordinary mapping path keeps its original open-stdin policy.

Mapping PowerShell children use an explicit Unicode environment block through
the existing process owner. A private snapshot removes case-insensitive copies
of PSModulePath and PSModuleAnalysisCachePath, then supplies the verified
System32 WindowsPowerShell/v1.0/Modules root and NUL. Other entries, including
drive-current-directory entries, are preserved. The parent environment is never
mutated; ordinary Start retains inheritance and other children are unchanged.
The module root shares the existing verified system PowerShell home and must be
an existing local directory without a reparse ancestor. Invalid input fails
before launch. Script, flags, stdin policy and the four-second deadline stay the
same. Windows PowerShell may reconstruct its runtime search path; configured
input does not prove a system-only path, actual command resolution or the sole
cause of the earlier timeout.

At the explicit delegated Start, the diagnostic observer records only
PSModulePath, WinPSModulePath and PSModuleAnalysisCachePath, separately labeled
parent-inherited values and configured child input. Presence, UTF-8 length,
SHA256, at most 4096 prefix bytes encoded as base64 and truncation remain separate
facts; absent values are not invented. It delegates once to the same owner and
does not expose other environment entries. These later facts cannot reconstruct
the earlier cold process's unrecorded environment.

The tagged report is capped at 256 KiB including LF and written only to
`OUTPUT/native-evidence/mapping-startup.json`. Go exclusively creates that fixed
protected child directory, creates the file exclusively and seals it before
payload write, sync and close. The checker reads the same fixed nested path.
An existing child/file is refused without adoption or truncation; the outer
checker's ACL stays unchanged. Every write/sync/close error remains visible and
created evidence remains on failure. Command capture and executor bounds are
unchanged. The source-bound native run above verified the private writer and
Go-child delivery; it did not establish cold or cache acceptance.

Each cell joins its process and three workers before the next; unknown Job
quiescence stops further launches. The cells record their order, actual creation
count, request/script hashes, launch facts, capture and cleanup. No create/remove,
intent ledger, mapping change or native file I/O is allowed. The original cold
failure remains failed even if later cells succeed. The experiment runs after VM
shutdown and SMB cleanup attempts, under different load; later cells may be warmed
by earlier launches. Its results cannot alone identify the original cold cause.
The 120-second test and 15-minute job limits remain unchanged. The measured
four-cell results belong to their exact source and environment.
A local PowerShell 7 held-open/EOF success establishes only that local behavior;
neither observation proves a timeout repair.

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
