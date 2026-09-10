# Go FUSE Library Selection — Research

Status: COMPLETE
Date: 2026-08-18

## Task context

We are building a Go **library** (importable by third parties) + reference daemon that mounts a
remote volume as a FUSE filesystem on Linux. Server is ours, persistent connection, server
pushes change events -> client MUST actively invalidate kernel caches. Semantics: cloud-drive,
commit-on-close, weak concurrency, no atomic rename / hardlink / coherent shared mmap.

Hard requirements driving the choice:
1. Kernel cache invalidation notifications (`notify_inval_inode`, `notify_inval_entry`,
   `notify_delete`, `notify_store`). Non-negotiable.
2. Per-response `attr_timeout` / `entry_timeout` (we vary TTL by connection health).
3. `FOPEN_KEEP_CACHE` per open, `FOPEN_DIRECT_IO`, `FUSE_WRITEBACK_CACHE`, `FUSE_AUTO_INVAL_DATA`.
4. readdirplus, INTERRUPT handling, FORGET accounting, statfs/xattr/symlink/fallocate/O_APPEND.
5. Module hygiene: no forced cgo on importers, stable module path.

---

## 1. Candidate landscape & maintenance (measured 2026-08-18)

Clones taken at these SHAs; all permalinks below are pinned.

| Library | Module path | HEAD SHA (2026-08-18) | Last commit | Commits last 18mo | Latest tag |
|---|---|---|---|---|---|
| hanwen/go-fuse | `github.com/hanwen/go-fuse/v2` | `5e1b6c816c2db624f5012b3f8bceffcd702b634e` | 2026-08-03 | **167** | v2.11.0 (2026-07-20), SHA `423b377e1452ab7b3522229185a3047f72e3f966` |
| bazil/fuse | `bazil.org/fuse` | `62a210ff1fd54902d27be7ac05d1b13b6f323ccd` | **2023-01-19** | **0** | (no semver tags) |
| jacobsa/fuse | `github.com/jacobsa/fuse` | `a124548f6da78ddcc3681b4e61868e1e4dadd728` | 2026-06-30 | 21 | (no semver tags) |
| winfsp/cgofuse | `github.com/winfsp/cgofuse` | `2fa812d1bdc77bb86c4bbf17bf4745d11674b85b` | 2026-05-31 | 1 | v1.6.0 era |

go-fuse release cadence is healthy and accelerating: v2.5.1 (2024-03) -> v2.6.0..v2.6.4 (2024-09..11)
-> v2.7.0..v2.7.2 (2024-11..12) -> v2.8.0 (2025-06) -> v2.9.0 (2025-10) -> v2.10/v2.10.1 (2026-04)
-> v2.11.0 (2026-07). Han-Wen Nienhuys is the dominant author (288 of the last-3y commits), with
regular outside contributors (Jakob Unterwurzacher of gocryptfs, and others).

**bazil/fuse is effectively dead**: last commit 2023-01-19, zero commits in the last 18 months,
`go 1.19` in go.mod, and a test-only dependency on the abandoned `github.com/dvyukov/go-fuzz`.

**cgofuse** has had exactly one commit in the last 18 months (a Windows large-buffer fix).
It is in maintenance mode, not development.

---

## 2. HARD REQUIREMENT: kernel cache invalidation notifications

This is the axis that decides the choice. Verified by reading source at the pinned SHAs.

### hanwen/go-fuse — ALL FOUR + retrieve. PASS.

Raw layer, on `*fuse.Server` (methods are promoted from the embedded `protocolServer`):

- `InodeNotify(node uint64, off, length int64) Status` — FUSE_NOTIFY_INVAL_INODE
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L590
- `EntryNotify(parent uint64, name string) Status` — FUSE_NOTIFY_INVAL_ENTRY
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L790
- `DeleteNotify(parent, child uint64, name string) Status` — FUSE_NOTIFY_DELETE
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L769
- `InodeNotifyStoreCache(node uint64, offset int64, data []byte) Status` — FUSE_NOTIFY_STORE
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L625
- `InodeRetrieveCache(node uint64, offset int64, dest []byte) (int, Status)` — FUSE_NOTIFY_RETRIEVE
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L669

High-level `fs` layer wraps these on `*fs.Inode` so you never touch raw node IDs:
`NotifyEntry(name)`, `NotifyDelete(name, child)`, `NotifyContent(off, sz)`, `NotifyPrune(nodes)`,
`WriteCache(offset, data)`:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/inode.go#L715-L770

`NotifyPrune` has no libfuse equivalent — it instructs the kernel to *forget* a batch of inodes,
which then issues FORGETs for as many as it can. That is a memory-reclaim primitive, not a
coherence primitive; see gotcha G1. It requires FUSE protocol v45 (mainline kernel 6.18, 2025-11)
and go-fuse gates it on `kernelSettings.SupportsNotify(NOTIFY_PRUNE)`.

### jacobsa/fuse — 2 of 4, added only 2025-05. WEAK.

`Notifier` with exactly `InvalidateInode` and `InvalidateEntry`; no NOTIFY_DELETE, no NOTIFY_STORE,
no NOTIFY_RETRIEVE:
https://github.com/jacobsa/fuse/blob/a124548f6da78ddcc3681b4e61868e1e4dadd728/notifier.go

Added in a single commit `fb5c71e` dated 2025-05-13 ("Add support for inode and dentry
invalidation") — i.e. gcsfuse ran for a decade with no invalidation at all.

Two structural problems with the design:
1. Notifications are funnelled through **one unbuffered channel pair serviced by a single
   goroutine** (`func (n *Notifier) notify`), and `InvalidateInode` blocks until the kernel write
   returns. A burst of server-pushed invalidations serialises, and any one slow/blocking kernel
   write stalls all others. For our push-heavy design this is head-of-line blocking by
   construction.
2. It requires wrapping the server: `fuse.NewServerWithNotifier(n, s)`. The notifier goroutine only
   exists for the lifetime of `ServeOps`.

### bazil/fuse — 3 of 4, but unmaintained. DISQUALIFIED on maintenance.

`Server.InvalidateNodeAttr/InvalidateNodeData/InvalidateNodeDataRange`, `Server.InvalidateEntry`,
`Server.NotifyDelete` (gated on `Protocol.HasNotifyDelete()`), no NOTIFY_STORE/RETRIEVE:
https://github.com/bazil/fuse/blob/62a210ff1fd54902d27be7ac05d1b13b6f323ccd/fs/serve.go#L1766-L1890

### winfsp/cgofuse — ZERO on Linux. DISQUALIFIED.

`FileSystemHost.Notify(path string, action uint32)` exists, but it calls `fsp_fuse_notify`, a
**WinFsp-only** DLL export, and the action constants are `NOTIFY_MKDIR/RMDIR/CREATE/UNLINK/CHMOD/
CHOWN/UTIME/CHFLAGS/TRUNCATE` — a WinFsp concept, not FUSE lowlevel notify:
https://github.com/winfsp/cgofuse/blob/2fa812d1bdc77bb86c4bbf17bf4745d11674b85b/fuse/host.go#L833-L846
https://github.com/winfsp/cgofuse/blob/2fa812d1bdc77bb86c4bbf17bf4745d11674b85b/fuse/fsop_cgo.go#L319-L330

On Linux/macOS cgofuse binds libfuse's **high-level, path-based** API (`fuse_main`), which does not
expose `fuse_lowlevel_notify_inval_inode` / `_inval_entry` / `_delete` / `_store` **at all**.
There is no way to punch the page cache. **cgofuse cannot satisfy our hard requirement on Linux.**

---

## 3. jacobsa/fuse in detail (and why gcsfuse uses it)

**Why gcsfuse uses it:** pure historical coupling. Aaron Jacobs wrote both `jacobsa/fuse` and
gcsfuse at Google in 2015; the library is the extraction of gcsfuse's FUSE layer. Its README states
it "owes its inspiration and most of its kernel-related code to bazil.org/fuse", i.e. it is a
re-cut of bazil around an ops-struct API. There is no public Google statement about
switching to go-fuse; instead Google engineers (Abhishek Gupta, Prince Kumar, Tulsi Shah, Kislay
Kishore) actively push patches *into* jacobsa/fuse for gcsfuse's needs — e.g. `MaxWrite`/`MaxPages`
config for large reads (HEAD commit `a124548f6da78ddcc3681b4e61868e1e4dadd728`, 2026-06-30) and the
"large page sizes" fixes landed via gcsfuse PRs #4484/#4546/#4485 (2026-03). Michael Stapelberg is
the de-facto maintainer/reviewer. So: alive, but it is *gcsfuse's* library, evolving to gcsfuse's
requirements, not a general-purpose one.

**API shape.** A single fat interface `fuseutil.FileSystem` with one method per op, taking
`(ctx context.Context, op *fuseops.XxxOp)`. You embed `fuseutil.NotImplementedFileSystem` for
defaults. **You manage InodeIDs, generation numbers, and lookup counts entirely yourself** — there
is no inode tree, no path resolution, no automatic FORGET accounting.

**Capabilities (verified at `a124548f6da78ddcc3681b4e61868e1e4dadd728`):**
- Per-response TTLs: `ChildInodeEntry.AttributesExpiration` / `.EntryExpiration` as absolute
  `time.Time`. PASS (fuseops/simple_types.go:145).
- `OpenFileOp.KeepPageCache bool` (FOPEN_KEEP_CACHE) and `.UseDirectIO bool` (FOPEN_DIRECT_IO). PASS.
- `MountConfig`: `DisableWritebackCaching` (writeback is ON by default), `EnableReaddirplus`,
  `EnableAutoReaddirplus`, `EnableAsyncReads`, `EnableAsyncDIO`, `EnableParallelDirOps`,
  `EnableAtomicTrunc`, `EnableNoOpenSupport`, `EnableNoOpendirSupport`, `EnableSymlinkCaching`,
  `MaxPages`, `MaxWrite`, `UseVectoredRead`.
- **No `MaxReadAhead` knob.** No `FUSE_AUTO_INVAL_DATA` / `EXPLICIT_INVAL_DATA` control.
- INTERRUPT: yes, `context.WithCancel` per request, cancelled on OpInterrupt
  (connection.go:288-383, dispatch at connection.go:491).
- Concurrency: single reader goroutine, `go s.handleOp(...)` per op, except FORGET which is handled
  inline on the reader goroutine to avoid a goroutine flood
  https://github.com/jacobsa/fuse/blob/a124548f6da78ddcc3681b4e61868e1e4dadd728/fuseutil/file_system.go

**Known problems:**
- jacobsa/fuse#78 (open since 2020-03): measured **1.8x slower than hanwen/go-fuse** by Kirill
  Smelkov (wendelin.core/wcfs), who benchmarked both. Root causes discussed: goroutine-per-request
  dispatch and buffer handling. Still open.
- jacobsa/fuse#197 (open, 2026-06): every request checks out a **1MB + pagesize** buffer for its
  whole lifetime; 100 concurrent metadata ops = ~100MB wasted. **This directly hits our design**
  (hundreds of requests blocked on network I/O).
- jacobsa/fuse#143 (open since 2023): no zero-copy path; forced memory copies on read/write.
- Module hygiene: go.mod requires `github.com/jacobsa/ogletest`, `oglematchers`, `oglemock`,
  `syncutil`, `timeutil`, `reqtrace`, `detailyang/go-fallocate`, `golang.org/x/net` — all abandoned
  personal libraries (ogletest last touched 2017). Verified they are used only from `samples/` and
  `fusetesting/`, so Go 1.17+ module-graph pruning keeps them out of an importer's build; they
  still show up in the module graph and in `go mod graph` output.
  hanwen/go-fuse by comparison: its **only non-test import is `golang.org/x/sys`**
  (`godebug`, `moby/sys/mountinfo` and `x/sync` appear exclusively in `_test.go` files).

---

## 4. hanwen/go-fuse in detail

### 4.1 Which layer to build on: `fs` or raw `fuse`?

go-fuse has two layers:

- `github.com/hanwen/go-fuse/v2/fuse` — the raw protocol layer. You implement `fuse.RawFileSystem`
  (one method per opcode, dealing in raw `uint64` node IDs, `*fuse.EntryOut`, `*fuse.AttrOut`) and
  hand it to `fuse.NewServer`. You do all inode identity, lookup-count/FORGET accounting, and
  concurrency-safe tree bookkeeping yourself.
- `github.com/hanwen/go-fuse/v2/fs` — the tree layer. You embed `fs.Inode` in your node types
  (`fs.InodeEmbedder`) and implement whichever `Node*er` interfaces you care about
  (`NodeLookuper`, `NodeOpener`, `NodeReader`, `NodeReaddirer`, ...). It provides the inode tree,
  the identity table keyed on `fs.StableAttr{Mode, Ino, Gen}`, hard-link/multi-parent handling,
  automatic lookup-count and FORGET accounting, and lifts the notify calls onto `*fs.Inode`.

**Recommendation: build on `fs`, not raw `fuse`.** The task brief says "we manage our own node
identity and cache", which sounds like an argument for the raw layer, but it is not:

1. `fs.StableAttr.Ino` is *our* number. The `fs` layer's identity map is keyed on the
   `(Mode, Ino, Gen)` we supply — `fs.Options.FirstAutomaticIno` only kicks in when we leave `Ino`
   at zero. So we keep full control of node identity and simply get a correct, race-tested
   inode-number -> `*fs.Inode` index for free. That index is exactly what the push-invalidation
   path needs: server says "node 12345 changed" -> look up the `*fs.Inode` -> `NotifyContent`.
2. Lookup-count/FORGET accounting is the single most bug-prone part of a raw-layer FUSE server
   (see gotchas below). Getting it wrong means either kernel EBADF/ESTALE storms or an
   unbounded-memory leak. `fs` implements it and has dedicated tests (`fs/forget_test.go`).
3. The notify entry points live on `*fs.Inode`, so we never have to keep a parallel
   `ino -> nodeId` map alive with correct lifetime — the hardest part of doing this by hand.
4. `fs` does not take away any low-level control: `Lookup` receives `*fuse.EntryOut` and `Open`
   returns raw `fuse.FOPEN_*` flags, so per-response TTL and per-open cache flags are ours.
5. Escape hatch exists: `fs.NewNodeFS(root, opts)` returns a `fuse.RawFileSystem` you can wrap,
   and `fs.Options` embeds `fuse.MountOptions` in full.

The raw layer is the right choice only for filesystems that are not tree-shaped at all
(e.g. `virtiofs`, or a proxy that forwards opcodes verbatim). Ours is tree-shaped.

### 4.2 Feature verification (permalinks at tag v2.11.0, SHA `423b377e1452ab7b3522229185a3047f72e3f966`)

- **Per-response TTLs.** `EntryOut.SetEntryTimeout(d)` / `SetAttrTimeout(d)` and
  `AttrOut.SetTimeout(d)`:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/types.go#L620-L648
  The bridge only fills in the global default **when the node left the field at zero**:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L205-L235
  GOTCHA: because "0" means "apply the default", you cannot ask for a genuine 0s TTL while a
  non-nil `fs.Options.AttrTimeout`/`EntryTimeout` is set. For TTL-varies-with-connection-health we
  should leave both `fs.Options` fields **nil** and always set the timeout explicitly on every
  `EntryOut`/`AttrOut`.
- **FOPEN flags, per open.** `Open`/`Create` return `(fh, fuseFlags uint32, errno)`; the returned
  `fuseFlags` go straight into `OpenOut.OpenFlags`. Constants
  `FOPEN_DIRECT_IO`, `FOPEN_KEEP_CACHE`, `FOPEN_NONSEEKABLE`, `FOPEN_CACHE_DIR`, `FOPEN_STREAM`,
  `FOPEN_NOFLUSH`, `FOPEN_PARALLEL_DIRECT_WRITES`, `FOPEN_PASSTHROUGH`:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/types.go#L252-L259
  So "keep the page cache across opens and rely on push invalidation" is directly expressible:
  return `fuse.FOPEN_KEEP_CACHE` from `Open`. There is a worked example in
  `fs/directio_example_test.go` / `fs/cache_test.go`.
- **INIT-time capability control.** `MountOptions.ExtraCapabilities` and `.DisabledCapabilities`
  are raw `CAP_*` bitmasks OR'd/AND-NOT'd into the negotiated flags:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/opcode.go#L112-L135
  - `FUSE_WRITEBACK_CACHE`: `CAP_WRITEBACK_CACHE = 1<<16` is *not* in the default mask, so enable it
    with `ExtraCapabilities: fuse.CAP_WRITEBACK_CACHE`.
  - `FUSE_AUTO_INVAL_DATA`: **on by default** (`kernelFlags |= input.Flags64() & CAP_AUTO_INVAL_DATA`).
    Setting `MountOptions.ExplicitDataCacheControl = true` swaps it for `CAP_EXPLICIT_INVAL_DATA`
    (kernel >= 4.19), i.e. "the filesystem is fully responsible for invalidating data cache".
    **This is the flag our design wants**: with AUTO_INVAL_DATA the kernel drops the page cache
    whenever it notices mtime/size changed, which fights our push-invalidation scheme and produces
    spurious re-reads. With EXPLICIT_INVAL_DATA the cache survives until *we* say otherwise.
  - readdirplus is **on by default** (`CAP_READDIRPLUS` in the default mask);
    `MountOptions.DisableReadDirPlus` turns it off.
- **max_write / max_pages / max_readahead.** `MountOptions.MaxWrite` (drives both `InitOut.MaxWrite`
  and `max_read=` mount option and `MaxPages`, rounded up to pages), `MountOptions.MaxReadAhead`,
  plus `MaxBackground` and `CongestionThreshold`:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/api.go
  MaxWrite defaults to 128 KiB and is capped at `MAX_KERNEL_WRITE` (1 MiB on Linux >= 4.20).
- **INTERRUPT -> `context.Context` cancellation.** `fuse.Context` implements `context.Context`
  (`Done()` closes when the kernel sends FUSE_INTERRUPT for that request), and the `fs` layer passes
  it as the `ctx` argument of every `Node*` method:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/context.go
  Dispatch: `doInterrupt` -> `protocolServer.interruptRequest(unique)` scans in-flight requests and
  closes the request's `cancel` channel:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/protocol-server.go#L116-L130
  On unmount (ENODEV from the device) `cancelAll()` cancels every in-flight request.
  **This is exactly what we need to abort an in-flight network read on Ctrl-C**: plumb the incoming
  `ctx` into the RPC.
- **Restart without unmount / fd passing.** `NewServer` accepts a **magic `/dev/fd/N` mountpoint**:
  if the mountpoint string parses as `/dev/fd/N`, go-fuse uses fd N directly as the already-mounted
  `/dev/fuse` fd instead of calling `fusermount`. Documented in the "Mount styles" section:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/api.go#L85-L124
  Implementation: `parseFuseFd(mountPoint)` in
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/mount_linux.go#L170-L204
  This is the seam for "daemon restarts, mount survives": the supervisor holds the `/dev/fuse` fd
  and re-execs the daemon with it inherited.
- **Privileges.** Three mount styles: (1) default, exec the setuid `fusermount3` (falls back to
  `fusermount`) — no root needed; (2) `DirectMount`/`DirectMountStrict`, call `mount(2)` directly —
  needs root/CAP_SYS_ADMIN, no fusermount binary needed (important for minimal containers);
  (3) inherited fd as above — no privileges at all.
- **`-o auto_unmount`.** Not a first-class option; go-fuse has no `AutoUnmount` field. You can pass
  it through `MountOptions.Options` (it is forwarded verbatim to `fusermount`), but note libfuse's
  `auto_unmount` works by keeping the `fusermount` helper alive as a child process, and go-fuse
  `os.StartProcess`es fusermount and **waits for it to exit** before taking the fd
  (`callFusermount`, mount_linux.go#L115-L167). So `auto_unmount` will not behave as it does under
  libfuse. Plan to clean up ourselves instead (see 4.3).
- **Crash with open fds.** If the daemon dies, the mount stays and callers hang on the dead
  connection. The documented recovery is the fusectl route:
  `echo 1 > /sys/fs/fuse/connections/$ID/abort`, after which pending syscalls get ENOTCONN; the
  connection id is the `Dev` field of a `stat` inside the mount. This is documented in
  the "Aborting a file system" section of the package doc (fuse/api.go). `Server.Unmount()` calls
  `syscall.Unmount` (when DirectMount) then `fusermount -u`.

### 4.3 Production users

GitHub code search: **615 `go.mod` files** reference `github.com/hanwen/go-fuse/v2`. Verified direct
(non-indirect) dependencies and their pinned versions, read from each repo's root `go.mod`
on 2026-08-18:

| Project | go-fuse version | What it is |
|---|---|---|
| kopia/kopia | v2.11.0 | backup tool, `kopia mount` |
| awslabs/soci-snapshotter | v2.11.0 | AWS lazy-pull container snapshotter |
| ipfs/kubo | v2.10.1 | IPFS reference implementation, `ipfs mount` |
| containerd/stargz-snapshotter | v2.10.1 | CNCF lazy-pull container snapshotter (pulled into k3s, rke2, k8e) |
| rclone/rclone | v2.10.1 | `rclone mount2` |
| rfjakob/gocryptfs | v2.9.0 | encrypted overlay FS; maintainer also contributes upstream |
| yandex/perforator | v2.9.0 | Yandex continuous profiler |
| moby/buildkit | v2.9.0 (indirect) | via stargz-snapshotter |
| buildbuddy-io/buildbuddy | v2.7.2 | remote build execution (their FUSE-based action input FS) |
| Velocidex/velociraptor | v2.5.1 | DFIR platform |
| jstaf/onedriver | v2.4.2 | **closest analogue to us**: OneDrive network FS with local cache |
| folbricht/desync | v2.2.0 | casync-compatible content store |
| pachyderm/pachyderm | v2.1.0 | data versioning platform |
| juicedata/juicefs | fork, see below | distributed POSIX FS |

Also present in the search: berty/berty, pydio/cells, vitessio/vitess, hanwen/go-mtpfs,
GoogleCloudPlatform/{cloud-sql-proxy,alloydb-auth-proxy}, grailbio/base, buildbarn/*, beam-cloud/beta9.

**rclone is the informative case: it ships three FUSE backends.** Its root go.mod pins all three —
`bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5` (the `mount` command, Linux),
`github.com/hanwen/go-fuse/v2 v2.10.1` (the `mount2` command, Linux), and
`github.com/winfsp/cgofuse v1.6.1-...` (the `cmount` command, used for Windows/macOS via
WinFsp/macFUSE). That is a direct confirmation of the cross-platform seam shape: the low-level
Linux binding and the cgo/WinFsp binding are *different* backends behind an internal VFS interface,
not one binding stretched across platforms.

**juicefs runs a fork.** Its go.mod contains
`replace github.com/hanwen/go-fuse/v2 v2.1.1-... => github.com/juicedata/go-fuse/v2 v2.1.1-0.20260811090623-38a391aab45e`
— a fork branched off go-fuse ~v2.1 (2021) and still being updated (2026-08-11). Treat this as
evidence that a very demanding consumer needed patches upstream would not take, not as evidence
against go-fuse; but worth knowing what they changed (see gotchas).

**seaweedfs** dropped to `winfsp/cgofuse` in its root go.mod, but `weed/mount/weedfs.go` still
imports `hanwen/go-fuse/v2` — i.e. go-fuse for the Linux mount, cgofuse for Windows.

---

## 5. Notification concurrency — a version boundary that matters to us

Up to and including **v2.10.1**, every go-fuse notify call took a process-wide `writeMu sync.Mutex`
before writing to `/dev/fuse` ("Protect notify writes with a separate lock", commit `a95445a`,
2014). All server-pushed invalidations therefore serialised on one mutex. juicedata hit this and
patched their fork to `sync.RWMutex` in
https://github.com/juicedata/go-fuse/commit/c9623809d765f694a3757b281dcfda687fdb1706
("allow notify in parallel (#19)", 2024-04-23).

**Upstream fixed this properly in the v2.11.0 cycle.** `writeMu` is gone; the fd is now owned by a
`fuseFD` that runs every write through `syscall.RawConn.Control`, which holds a reference on the fd
so concurrent writers and `close()` are safe without any serializing mutex:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/fusefd.go#L74-L97
(landed in `6c5d127` "fuse: encapsulate per-fd state in fuseFD struct" and `8043edb`, 2026-06-14;
`git tag --contains` shows both only in v2.11.0.)

**Action: require `github.com/hanwen/go-fuse/v2 >= v2.11.0`.** Our design pushes invalidations from
a server event stream; on v2.10.x that stream would fight a global mutex with every reply path.

---

## 6. Concurrency model under 500 blocked requests

**hanwen/go-fuse.** `Server.loop()` reads a request from `/dev/fuse` and, when it can accept
another, spawns `go ms.handleRequest(req)` and continues reading; otherwise it processes inline
(the "singleReader" optimisation, worth ~2x — the benchmark numbers are in the source comment)
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L436-L497
Reader concurrency is bounded by `maxReaders = clamp(GOMAXPROCS)`; **request concurrency is not
bounded by goroutine count**, it is bounded by memory: `MountOptions.MaxInflightRequestBytes`
(default `math.MaxInt`) accounts request structs + input buffers, and `readRequest` refuses to
start a new read when the budget is exhausted. 500 requests blocked on network I/O is 500 cheap
goroutines plus 500 * (MaxWrite + header) bytes of buffer.

Head-of-line blocking analysis for our workload:
- Replies are `writev` on the fd with **no mutex** (v2.11.0+), so a slow reply does not block others.
- Notifications share that same lock-free path.
- The one real backpressure knob is `MaxInflightRequestBytes`. If we set it, a burst of blocked
  network reads will stop the reader loop and thereby stall *all* traffic including FORGET and
  INTERRUPT. **Recommendation: leave `MaxInflightRequestBytes` unset and do our own admission
  control in the RPC layer**, so that INTERRUPT can always get through to cancel.
- `MaxBackground` (default 12) / `CongestionThreshold` limit only *async* (readahead / writeback)
  requests inside the kernel; synchronous requests are not limited. For a network FS,
  `MaxBackground` at 12 is far too low — raise it (juicefs uses hundreds).
- Known deadlock class, documented by the library and applying to *any* binding: serving a FUSE fs
  from the same process that reads it (fork/exec ACCESS, `dup3()`-triggered FLUSH, epoll->POLL,
  mmap page faults). go-fuse disables the POLL opcode on mount and reserves fd 3 to mitigate.
  See the "Deadlocks" section of
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/api.go

**jacobsa/fuse.** Single reader, `go handleOp` per request except FORGET (inline). Same shape, but
each in-flight request pins a **1 MiB + pagesize** buffer for its whole lifetime (jacobsa/fuse#197,
open) — 500 blocked requests is ~500 MB of idle buffers. And notifications go through one
goroutine with unbuffered channels, so they serialise.

---

## 7. Testing story

**`/dev/fuse` is available on GitHub Actions `ubuntu-*` runners.** All of go-fuse, gcsfuse and
juicefs run real mounts in CI on stock hosted runners; you need `sudo apt-get install fuse3`, and
for `allow_other` you need `echo user_allow_other | sudo tee -a /etc/fuse.conf`. No privileged
container, no special runner. Root (via `sudo`) is only needed for the direct-`mount(2)` path and
for tests that exercise real uid/gid.

**hanwen/go-fuse CI** — https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/.github/workflows/ci.yml
- matrix: Go 1.21 .. 1.26, and crucially `GOMAXPROCS` in `{"", "1"}` — "Some failures are only
  visible like this". Copy this; GOMAXPROCS=1 is where the fork/exec and epoll deadlocks show up.
- daily `schedule: cron '0 12 * * *'`, `fail-fast: false`.
- runs `./all.bash`: https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/all.bash
  - `go build ./...` on Linux, plus `GOOS=darwin`/`GOOS=freebsd` cross-builds of `./fs/...`
    (i.e. upstream keeps the darwin seam compiling — good news for our future macOS port).
  - `go test -timeout 5m -p 1 -count 1 ./...` — `-p 1` for serial + live output, `-count 1` to
    defeat caching so flakes are visible, `-timeout 5m` to get a backtrace before the CI kills it.
  - a second root pass: `sudo env PATH=$PATH go test -run 'Test(DirectMount|Forget|Passthrough|IDMappedMount)' ./fs ./fuse`
  - virtiofs tests inside QEMU/KVM when available (auto-skipped otherwise).
  - benchmarks with `-test.cpu 1,2`.

**go-fuse ships an importable POSIX conformance suite: `github.com/hanwen/go-fuse/v2/posixtest`.**
`posixtest.All` is an exported `map[string]func(t *testing.T, mnt string)` we can run against our
own mount with a couple of lines. Coverage includes `AppendWrite` (O_APPEND), `Fallocate`, `XAttr`,
`SymlinkReadlink`, `Link`, `LinkUnlinkRename`, `RenameOpenDir`, `RenameOverwriteDest*`,
`FstatDeleted`, `NlinkZero`, `FdLeak`, `ParallelFileOpen`, `OpenSymlinkRace`, `ReadDirConsistency`,
`DirSeek` (a Go port of **xfstests generic/257**), `LseekHoleSeeksToEOF`, `DirectIO`,
`Fcntl(Flock|SetLk)`, `TruncateFile/NoFile`.
This alone is a strong reason to pick go-fuse: **we get a conformance suite for free and can wire
it into our own CI on day one**, and it doubles as the checklist for which semantics we must
implement.

**What the big consumers run** (for the "how much more should we do" question):
- juicefs has the most complete matrix — dedicated workflows for `pjdfstest`, `ltpfs`,
  `ltpsyscalls`, `fsrand`, `random-test`, `chaos`, `vdbench`, `mutate-test`, all on `ubuntu-24.04`
  hosted runners: https://github.com/juicedata/juicefs/tree/main/.github/workflows
- gcsfuse: `ubuntu-22.04`, `sudo apt-get install -y fuse3 libfuse-dev`, plus a separate
  `flake-detector.yml` workflow — an explicit acknowledgement that FUSE tests are flaky:
  https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/.github/workflows/ci.yml
- rclone runs its mount tests inside `build.yml` (single mega-workflow).

**Unmount flakiness in CI, and the workarounds people use:**
- `fusermount -u` fails with EBUSY if anything still has the mount as cwd or an open fd. The
  reliable sequence is: close all fds -> `Server.Unmount()` -> on EBUSY retry with backoff ->
  last resort `umount -l` (lazy).
- If the daemon has already died, unmount can hang forever. The escape hatch is fusectl:
  `echo 1 > /sys/fs/fuse/connections/<devid>/abort`, which makes every pending syscall return
  ENOTCONN. `<devid>` is the `Dev` field of `stat()` on anything inside the mount. Wire this into
  the test harness's cleanup so a hung test cannot wedge the runner.
- Serial test execution (`-p 1`) matters: parallel packages each mounting under `/tmp` interleave
  their unmounts and produce spurious EBUSY.

---

## 8. Cross-platform seam (macOS / Windows) and what it costs

**cgofuse requires cgo on Linux and macOS.** Only the Windows target has a pure-Go path
(`host_nocgo_windows.go`, `//go:build !cgo && windows`, which `LoadLibrary`s the WinFsp DLL). On
Linux/macOS/BSD it is `//go:build cgo` with `#cgo CFLAGS: -DFUSE_USE_VERSION=28 ...` and needs
gcc + `libfuse-dev`/`libfuse3-dev`/macFUSE headers at build time; FUSE3 on Linux is behind a
`-tags=fuse3` build tag, default is FUSE **2.9** high-level API:
https://github.com/winfsp/cgofuse/blob/2fa812d1bdc77bb86c4bbf17bf4745d11674b85b/fuse/host_cgo.go#L1-L20
https://github.com/winfsp/cgofuse/blob/2fa812d1bdc77bb86c4bbf17bf4745d11674b85b/README.md

**Consequence for us as a library:** if `cgofuse` sits in the import graph of our mount package,
*every importer* loses `CGO_ENABLED=0`, loses trivial cross-compilation, and gains a system
package prerequisite. That is unacceptable for a package meant to be imported by other projects.

**Therefore the seam must be a Go interface with per-platform backends in separate packages
(ideally separate modules or at minimum build-tag-isolated packages), not one binding stretched
across platforms.** rclone is the existence proof: it ships `cmd/mount` (bazil), `cmd/mount2`
(go-fuse) and `cmd/cmount` (cgofuse) as three backends behind its internal `vfs` package, and only
`cmount` requires cgo. seaweedfs does the same split (go-fuse for Linux, cgofuse for Windows), as
does beam-cloud/airstore (go-fuse v2.9.0 + cgofuse).

**POSIX semantics lost on the Windows/WinFsp path** (and therefore things our core must not
*depend* on, only *use where available*):
- **No lowlevel cache invalidation.** WinFsp's `fsp_fuse_notify` takes a *path* and a coarse action
  (`NOTIFY_MKDIR/RMDIR/CREATE/UNLINK/CHMOD/CHOWN/UTIME/CHFLAGS/TRUNCATE`). There is no
  "invalidate byte range [off,len) of inode X" and no dentry-level invalidation. Our push-based
  cache coherence degrades to path-granular invalidation on Windows.
- **Path-based, not inode-based.** cgofuse binds the libfuse *high-level* API: every op receives a
  `path string`. Open-unlinked-file semantics, rename-while-open, and hardlinks are lossy.
  Our internal design must stay inode-based and treat "path" as a Windows-side adaptation.
- No `O_DIRECT`/`FOPEN_DIRECT_IO` equivalence, no readdirplus, no INTERRUPT, no FORGET,
  no per-lookup TTL control (libfuse high-level fixes `entry_timeout`/`attr_timeout` per mount via
  `-o` options).
- Windows: no POSIX permission bits/uid-gid without WinFsp's mapping, case-insensitivity by
  default, reserved filenames (`CON`, `NUL`, `AUX`, ...), no `:` in names, path length limits,
  different delete-on-close semantics.
- macOS via macFUSE: `FOPEN_KEEP_CACHE` is effectively always on for same-mode reopens
  (osxfuse#223), `xattr` has the `com.apple.*` finder-info special cases, and macFUSE is now a
  paid/kext-signed dependency; the modern alternative is FSKit (go-fuse issue #604 tracks it).
  Note go-fuse itself keeps `GOOS=darwin` compiling in CI (`all.bash` cross-builds `./fs/...` for
  darwin and freebsd), and has darwin-specific files throughout `fuse/` and `fs/`, so a future
  macOS port on go-fuse is not starting from zero.

**Recommendation for the seam:** define our own `Mounter`/`FileSystem` interface in the core
package with inode-based, low-level semantics (that is the superset), implement
`mount/fuselinux` on hanwen/go-fuse now, and leave `mount/winfsp` and `mount/macos` as later
packages behind build tags. Do **not** design the core interface down to cgofuse's path-based
high-level shape — that would throw away everything we need on Linux.

---

## 9. Newer entrants, 2024-2026

Searched GitHub for Go repos with a FUSE angle, >50 stars, pushed since 2025-06, plus the
`go.mod` of every FUSE-shaped Go project I could find. **There is no new pure-Go FUSE binding that
has gained traction.** The field is still exactly the four libraries above. What *has* happened is
forking:

- `github.com/vitalif/fusego` — fork of jacobsa/fuse used by **yandex-cloud/geesefs** and
  **tigrisdata/tigrisfs**, paired with `github.com/vitalif/cgofuse` for Windows.
- `github.com/juicedata/go-fuse/v2` — juicefs's fork of go-fuse (see §5).
- `github.com/git-lfs-fuse/go-fuse/v2` — git-lfs-fuse's fork of go-fuse (v2.7.4-dev).
- **cubefs** vendors jacobsa/fuse into `./depends/jacobsa/fuse`.

Recent project choices, for calibration (read from each root `go.mod`, 2026-08-18):
`tailscale/gomodfs` -> go-fuse v2.8.0; `beam-cloud/airstore` -> go-fuse v2.9.0 + cgofuse;
`cloudflare/artifact-fs` -> jacobsa/fuse; `superfly/litefs` -> still bazil.org/fuse;
`buildbarn/bb-clientd` -> go-fuse v2.10.1.

**FUSE-over-io_uring** (kernel 6.14+, libfuse 3.18): **no Go binding implements it.** go-fuse knows
the capability bit (`CAP_OVER_IO_URING = 1<<41`, and prints it in debug output since commit
`c7d04c8`, 2025-09-26) but does not negotiate or use the ring. Treat this as a future performance
avenue, not a selection criterion. Since go-fuse also grew `fuse.ProtocolServer` in v2.10
("enables running the FUSE protocol over transports that are not the standard kernel connection",
used for the new `virtiofs` package), it is the library best positioned to grow an io_uring
transport without an API break.

---

## 10. Feature support matrix

Legend: **Y** = supported and verified in source; **P** = partial; **N** = absent; **n/a** = not
applicable to that binding's model.

| Capability | hanwen/go-fuse v2.11.0 | jacobsa/fuse @a124548f | bazil/fuse @62a210ff | cgofuse @2fa812d1 (Linux) |
|---|---|---|---|---|
| `notify_inval_inode` | **Y** `Server.InodeNotify` / `Inode.NotifyContent` | Y `Notifier.InvalidateInode` | Y `InvalidateNodeData(Range)` | **N** |
| `notify_inval_entry` | **Y** `EntryNotify` / `Inode.NotifyEntry` | Y `Notifier.InvalidateEntry` | Y `InvalidateEntry` | **N** |
| `notify_delete` | **Y** `DeleteNotify` / `Inode.NotifyDelete` | **N** | Y `NotifyDelete` | **N** |
| `notify_store` | **Y** `InodeNotifyStoreCache` / `Inode.WriteCache` | **N** | **N** | **N** |
| `notify_retrieve` | **Y** `InodeRetrieveCache` / `Inode.ReadCache` | **N** | **N** | **N** |
| `notify_prune` (kernel 6.18+) | **Y** `PruneNotify` / `Inode.NotifyPrune`, gated on proto v45 | N | N | N |
| Parallel notifications (no global mutex) | **Y** (v2.11.0+ only; serialized on <= v2.10.1) | N (single goroutine, unbuffered chan) | N (`Conn` write lock) | n/a |
| Per-response `entry_timeout` | **Y** `EntryOut.SetEntryTimeout` | Y `ChildInodeEntry.EntryExpiration` | Y `LookupResponse.EntryValid` | **N** (mount-global `-o`) |
| Per-response `attr_timeout` | **Y** `EntryOut.SetAttrTimeout` / `AttrOut.SetTimeout` | Y `AttributesExpiration` | Y `Attr.Valid` | **N** |
| Negative-entry caching | **Y** `Options.NegativeTimeout` | P (return entry with `Child=0`) | Y | N |
| `FOPEN_KEEP_CACHE` per open | **Y** (returned `fuseFlags` from `Open`/`Create`) | Y `OpenFileOp.KeepPageCache` | Y `OpenResponse.Flags` | N |
| `FOPEN_DIRECT_IO` per open | **Y** | Y `OpenFileOp.UseDirectIO` | Y | P (mount-global `direct_io`) |
| `FOPEN_CACHE_DIR` / `FOPEN_NOFLUSH` / `FOPEN_STREAM` / `FOPEN_PARALLEL_DIRECT_WRITES` / `FOPEN_PASSTHROUGH` | **Y** all | N | P | N |
| `FUSE_WRITEBACK_CACHE` | **Y** `ExtraCapabilities: CAP_WRITEBACK_CACHE` | Y (on by default, `DisableWritebackCaching` to opt out) | Y `WritebackCache` | N |
| `FUSE_AUTO_INVAL_DATA` control | **Y** on by default; `ExplicitDataCacheControl` swaps to `CAP_EXPLICIT_INVAL_DATA` | **N** | Y `ExplicitInvalidateData()` | N |
| readdirplus | **Y** on by default; `DisableReadDirPlus`; `FileLookuper` for batch-served lookups | Y opt-in `EnableReaddirplus` / `EnableAutoReaddirplus` | P | N |
| `max_write` | **Y** `MountOptions.MaxWrite` | Y `MountConfig.MaxWrite` | Y `MaxReadahead` only + `WriteRequest` | N |
| `max_pages` | **Y** derived from MaxWrite | Y `MountConfig.MaxPages` | N | N |
| `max_readahead` | **Y** `MountOptions.MaxReadAhead` | **N** | Y `MaxReadahead` | N |
| `max_background` / congestion threshold | **Y** both | N | Y `MaxBackground()` / `CongestionThreshold()` | N |
| INTERRUPT surfaced | **Y** `ctx` cancelled (`fuse.Context` implements `context.Context`); `cancelAll()` on unmount | Y `context.WithCancel` per op | Y `ctx` cancelled in `fs/serve.go` | **N** |
| FORGET / lookup-count accounting | **Y** done by `fs` layer; `NodeOnForgetter` hook; `RememberInodes` to disable | **N** — caller's problem | **Y** done by `fs` layer | n/a (path-based) |
| statfs | **Y** `NodeStatfser` | Y `StatFSOp` | Y | Y |
| xattr | **Y** get/set/list/remove + `DisableXAttrs`, `IgnoreSecurityLabels` | Y | Y | Y |
| symlink / readlink | **Y** + `EnableSymlinkCaching` (kernel caches readlink) | Y + `EnableSymlinkCaching` | Y | Y |
| fallocate | **Y** `NodeAllocater` | Y `FallocateOp` | N | Y |
| O_APPEND | **Y** (covered by `posixtest.AppendWrite`) | Y | Y | Y |
| copy_file_range | **Y** `NodeCopyFileRanger` | N (issue #156 open since 2023) | N | N |
| statx | **Y** (v2.7+) | N | N | N |
| ioctl | **Y** (v2.8+) | N | N | Y |
| POSIX/BSD locks | **Y** `EnableLocks` + Getlk/Setlk/Setlkw | P | Y | Y |
| passthrough (FUSE_PASSTHROUGH) | **Y** (v2.6+), `MaxStackDepth` | N (issue #163 open) | N | N |
| ID-mapped mounts | **Y** (v2.8+) | N | N | N |
| Pass in existing `/dev/fuse` fd | **Y** magic `/dev/fd/N` mountpoint | **N** upstream (juicefs forked to add it) | N | N |
| Mount without `fusermount` (direct `mount(2)`) | **Y** `DirectMount`/`DirectMountStrict` | N | N | N |
| `-o auto_unmount` | P (pass-through string only; helper is reaped, so libfuse semantics do not hold) | P | P | Y (libfuse handles it) |
| Panic containment | **Y** `MountOptions.PanicHandler` (v2.11) | N (`panic(err)` in ServeOps) | P | N |
| Runs the protocol over a non-kernel transport | **Y** `fuse.ProtocolServer` (v2.10+), used by `virtiofs` | N | N | N |
| **cgo required** | **No** — only non-test import is `golang.org/x/sys` | No | No | **Yes** on Linux/macOS |
| Semver-tagged module | **Y** `/v2`, 11 minor releases | **N** — pseudo-versions only | **N** | Y |

---

## 11. Prioritized gotcha list

Mined from hanwen/go-fuse and jacobsa/fuse issue trackers and from the consumers' forks.

### P0 — will bite us, design around them now

**G1. Unbounded inode-cache growth on a long-running mount.**
go-fuse issue #603 (open, 2026-04-26): a production loopback FS on go-fuse `fs` held
**30,344,144 cached inodes / 25 GB heap after 172 days**, p99 RSS 43.9 GB across 22 pods.
pprof attribution: `rawBridge.addNewChild` 14.4 GB (the `kernelNodeIds` + `stableAttrs` maps),
user `*Inode` structs 5.9 GB, `inodeChildren.set` 3.2 GB — **~825 bytes amortized per cached
inode**. Workload was just periodic `find` / `grep -r` / `ls -R`.
https://github.com/hanwen/go-fuse/issues/603
This is *not* a go-fuse bug: the inode tree's lifetime is owned by the kernel, and go-fuse must
keep a node alive until FORGET arrives. `EntryTimeout`/`AttrTimeout` do **not** cause reclaim —
they only control how long the kernel *trusts* the entry, not whether it holds a reference.
`echo 3 > /proc/sys/vm/drop_caches` produces a flurry of FORGETs and does free it, and the kernel
reclaims under memory pressure — but on a memory-rich node "memory pressure" never comes.
Our exposure is identical (cloud drive, users run `find`/backup scanners). Mitigations, in order:
  - `Inode.NotifyPrune([]*Inode)` is the right primitive (multiple inodes per call, kernel handles
    hardlink aliases), **but FUSE_NOTIFY_PRUNE only landed in mainline 6.18 (2025-11)** and go-fuse
    gates it on `kernelSettings.SupportsNotify(NOTIFY_PRUNE)` (protocol v45). Fleets on 5.x / 6.6 /
    6.12 LTS will not have it for years. Design the eviction *policy* ourselves (LRU / 2Q, skip
    busy/persistent nodes) and use NotifyPrune when available, `NotifyEntry` per-name otherwise.
  - Keep the per-node payload small. #603's 825 B/inode included the node's own `path` string. **Do
    not store a path string in the node.** Store an opaque server-side ID.
  - Export a `cached_inodes` metric from day one; this is invisible until it is 25 GB.

**G2. `Inode.Forgotten()` is racy for inode-number recycling.**
go-fuse issue #504 (closed) / containerd/stargz-snapshotter#1594: `Forgotten()` returns `true`
briefly *between* Inode construction and Inode initialization, so a client that recycles `Attr.Ino`
values when `Forgotten()` reports true will hand out an Ino that is about to become live. hanwen's
position: `Forgotten()` is for cleanup when you are sure the inode cannot be revived, and any
lock-free read of it can be stale by the time you act.
https://github.com/hanwen/go-fuse/issues/504
**Rule for us: never recycle inode numbers.** Allocate from a 64-bit monotonic counter (or hash a
stable server-side object ID). Inode-number collision is the classic way to get ESTALE / wrong-file
reads in a multi-client volume; gcsfuse has the same class of bug open right now
(GoogleCloudPlatform/gcsfuse#4813, "promoteToGenerationBacked collision bug causing ESTALE errors").

**G3. SIGURG makes every Go program on our mount look like it is pressing Ctrl-C.**
jacobsa/fuse#122 (open since 2022): the Go runtime uses SIGURG for non-cooperative goroutine
preemption; *any* unmasked signal delivered to a process blocked in a FUSE request makes the kernel
queue a FUSE_INTERRUPT. go-fuse documents this explicitly ("All unmasked signals generate an
interrupt. In particular, the SIGURG signal ... also generates an interrupt")
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/api.go#L124-L135
**Do not blindly return EINTR when `ctx` is cancelled.** Reproducer per the issue: `git clone` into
the mount, or any Go binary reading it. Our policy should be: propagate cancellation to the network
RPC (so we stop wasting bandwidth), but for a *read* that is already in flight, prefer completing
it; return EINTR only for operations that are genuinely long and unbounded, and consider requiring
the cancellation to persist for a short grace period before honouring it.

**G4. Set `ExplicitDataCacheControl`, or AUTO_INVAL_DATA will fight our push invalidation.**
go-fuse enables `CAP_AUTO_INVAL_DATA` by default. With it, the kernel drops the page cache whenever
it observes a changed mtime/size in a GETATTR reply — which is constantly true for a multi-client
cloud drive, so `FOPEN_KEEP_CACHE` buys us nothing and we re-read data the server never said had
changed. `MountOptions.ExplicitDataCacheControl = true` negotiates `CAP_EXPLICIT_INVAL_DATA`
(kernel >= 4.19) instead, which means "the filesystem is fully responsible" — exactly our model.
Consequence: **any bug in our invalidation path becomes stale data with no self-healing.** Budget
test coverage for it (two clients, write on A, assert B sees it after the push).

**G5. Do not set a global `AttrTimeout`/`EntryTimeout` if you want to vary TTL per response.**
`rawBridge.setEntryOutTimeout` applies the global default only when the node's value is still 0,
so with a global set you can never express a genuine 0-second TTL. Leave `fs.Options.AttrTimeout`,
`.EntryTimeout` and `.NegativeTimeout` **nil** and set the timeout explicitly on every `EntryOut` /
`AttrOut`. (Note: leaving them nil means the raw zero value goes to the kernel, i.e. no caching, so
"always set explicitly" becomes mandatory, not optional — enforce it with a helper.)

### P1 — cost us a debugging week if unknown

**G6. READDIRPLUS in the `fs` layer calls `Lookup` once per entry.**
`rawBridge.ReadDirPlus` iterates the `DirStream` and calls `b.lookup(...)` for each name
(fs/bridge.go around L1060-L1095). For a network FS that is N round trips per `ls -l`. The escape
hatch is `FileLookuper` (added v2.9.0): if the *directory handle* implements
`Lookup(ctx, name, out)`, READDIRPLUS calls that instead of `NodeLookuper.Lookup`, so the handle
can serve every name out of the single batched listing it already fetched.
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/api.go#L733-L742
**Implement `FileLookuper` on our directory handle. This is the single highest-leverage
performance decision in the readdir path.**

**G7. `NotifyEntry` on a node the kernel has already forgotten used to panic; more generally,
notify racing FORGET is a real hazard.** go-fuse issue #354: "Panic on unknown node after
NotifyEntry", triggered by an application-side LRU that sent `NotifyEntry` on eviction. The debug
log in that issue shows the exact interleaving: reply for LOOKUP -> FORGET -> LOOKUP again, with a
NOTIFY_INVAL_ENTRY in between. https://github.com/hanwen/go-fuse/issues/354
Our push path does exactly this shape (server event arrives for a node that is concurrently being
forgotten). Take the notification path through the `fs` layer (`Inode.Notify*`), never cache raw
`nodeId`s in our own map, and treat `ENOENT` from a notify call as normal, not an error.
Related, in our favour: v2.11 added `MountOptions.PanicHandler`, so an FS panic returns an errno
instead of taking the process down. Set it.

**G8. `Server.Unmount()` returns EBUSY while anything holds the mount; and unmount does not
guarantee `Serve()` returns.** go-fuse issue #532. The FUSE loops only exit when the kernel agrees
the fs can be unmounted, so a process holding an open fd inside the mount blocks it forever.
Escape hatch is the fusectl abort (`/sys/fs/fuse/connections/<devid>/abort`). Also
go-fuse#506: concurrent mount/unmount of many mounts deadlocked at go-fuse v2.5 because of the
`fusermount` subprocess; `DirectMountStrict` avoids the subprocess entirely.
**Design the daemon's shutdown around this**: SIGTERM -> stop accepting new RPCs -> `Unmount()` ->
retry with backoff -> fusectl abort -> exit. And expose fusectl abort as a supported operation in
our test harness cleanup.

**G9. `MaxBackground` defaults to 12.** That is the kernel's async-request window (readahead and
writeback). For a network FS with per-request latency in the tens of milliseconds, 12 in flight
caps throughput hard. Raise it (hundreds) and set `CongestionThreshold` accordingly (v2.11 exposes
it separately; before that it was hardcoded to 3/4 * MaxBackground).

**G10. Getattr-vs-Lookup confusion.** `Lookup` returns an `EntryOut` (entry TTL + attr TTL +
nodeId + generation) and **takes a reference** — the kernel's lookup count for that node goes up by
one, and it will only come down via FORGET. `Getattr` returns an `AttrOut` (attr TTL only) and
takes no reference. Two consequences: (a) returning attributes from `Lookup` and forgetting to set
`EntryOut.SetAttrTimeout` gives the attributes a 0 TTL, causing a GETATTR storm right after every
LOOKUP; (b) if a node can be looked up by more than one name (or the same name twice), the *same*
`*fs.Inode` must be returned, or the kernel ends up with two nodeIds for one file and the lookup
counts never balance.

**G11. Splice/zero-copy path is on by default and silently skipped on failure.**
`MountOptions.DisableSplice` exists; go-fuse tries `trySplice` for reads with a `readResult`. The
current in-flight review (go-fuse#616, open) notes the splice write length is the one write in the
package whose length is not checked. If we see truncated reads under load, `DisableSplice: true`
is the first thing to try.

### P2 — worth knowing

- **G12.** `MaxInflightRequestBytes` (v2.11) throttles by *stopping the reader loop*, which stalls
  FORGET and INTERRUPT too. Prefer our own admission control in the RPC layer.
- **G13.** Serving a FUSE fs from the process that reads it deadlocks in specific ways
  (fork/exec ACCESS; `dup3()` -> FLUSH; epoll -> POLL; mmap page faults). go-fuse disables POLL on
  mount and reserves fd 3. Test with `GOMAXPROCS=1`, which is where these surface.
- **G14.** `fs.Options.FirstAutomaticIno` defaults to 2^63 — inodes we leave at `Ino == 0` get
  numbered sequentially from there. If we assign our own `Ino` values, keep them below 2^63 or we
  collide with go-fuse's automatic allocations. Separately, `Ino == ^uint64(0)` is **reserved** for
  go-fuse's POLL hack and `newInodeUnlocked` does `log.Panicf("using reserved ID ...")` if we ever
  hand it over (`StableAttr.Reserved()`, fs/inode.go:43).
- **G15.** jacobsa/fuse pins a 1 MiB + pagesize buffer per in-flight request for its whole lifetime
  (#197). If we had gone that way, 500 blocked requests = ~500 MB. Another point against it.
- **G16.** `DirStream` results must be deterministic — a map-iteration-order listing makes entries
  disappear when two processes read the same directory concurrently (documented on
  `NodeReaddirer`).
- **G17.** go-fuse's `Attr` uses the *filesystem's* `Ino`, but hard-link detection by userspace
  compares `(st_dev, st_ino)`. Since we do not need hardlinks, just make sure distinct files never
  share an `Ino`.

---

## 12. Recommendation

### Pick `github.com/hanwen/go-fuse/v2`, build on its `fs` package, require **>= v2.11.0**.

It is the only candidate that clears the hard requirement outright, and it clears it by a wide
margin: all four notification types plus `notify_store`, `notify_retrieve` and `notify_prune`,
exposed both raw and lifted onto `*fs.Inode`, and (from v2.11.0) written to `/dev/fuse` with no
serializing mutex. Every other requirement on the list is a `Y` in the matrix, several of them
uniquely so (`CAP_EXPLICIT_INVAL_DATA` control, `MaxReadAhead`, `MaxBackground`/congestion
threshold, `FileLookuper` for batched readdirplus, `/dev/fd/N` fd hand-off for
restart-without-unmount, `PanicHandler`).

For the library angle specifically:
- **Zero cgo, one dependency.** The only non-test import in the entire module is
  `golang.org/x/sys`. Our importers keep `CGO_ENABLED=0` and trivial cross-compilation.
- **Proper semver on a `/v2` module path**, 11 minor releases, no `replace` needed. jacobsa/fuse
  and bazil/fuse have never cut a tag — every consumer carries a pseudo-version, which is a bad
  thing to inflict on people who import us.
- **Logging is injectable, but there IS package-level `init()` work — know what it does.** Debug
  output is opt-in (`MountOptions.Debug`) with an injectable sink (`MountOptions.Logger`,
  `fs.Options.Logger`). However, importing the packages has three side effects worth stating in our
  own docs, because they land in *our importers'* processes:
  - `fuse/mount.go` `init()` **permanently grabs file descriptor 3** and never releases it, so that
    the `fusermount` subprocess (which inherits exactly one fd) can never be handed an fd pointing
    into a FUSE mount. Deliberate deadlock protection, documented in the source comment.
  - `splice/splice.go` `init()` reads `/proc/sys/fs/pipe-max-size`, creates a pipe and probes it
    with `fcntl(F_GETPIPE_SZ/F_SETPIPE_SZ)`, and **`log.Panicf`s to the global logger if
    `os.Pipe()` fails**.
  - A handful of BUG/impossible-state paths call the global `log` package directly rather than the
    injected logger (`fuse/server.go` "Serve() must only be called once", `fs/bridge.go`
    `log.Panicf` on reserved-Ino misuse, `fuse/opcode.go` on protocol mismatch).
  There is no global mutable configuration and no network/file state beyond the above.
- **Mount needs no privileges** in the default path (setuid `fusermount3`), and there are two
  privilege-free alternatives (`DirectMount` with CAP_SYS_ADMIN; inherited `/dev/fuse` fd).
- **We inherit a POSIX conformance suite** (`posixtest`) we can run against our own mount.
- **Broadest production validation of the four**: 615 go.mod references; kopia, ipfs/kubo,
  containerd/stargz-snapshotter, awslabs/soci-snapshotter, gocryptfs, rclone, yandex/perforator,
  buildbuddy, tailscale/gomodfs all on v2.7+.

### Build on `fs`, not raw `fuse`

Reasoning in §4.1. Short version: `fs.StableAttr.Ino` is *our* number, so we keep node identity;
what we get in exchange is a correct inode index (which is precisely the lookup table the
push-invalidation path needs) and correct FORGET accounting, which is the hardest thing to get
right at the raw layer. `fs.NewNodeFS` is the escape hatch if we ever need to intercept raw
opcodes, and `fuse.MountOptions` is embedded in `fs.Options` so nothing is hidden from us.

### Disqualifiers, stated plainly

1. **winfsp/cgofuse is disqualified for the Linux core.** It binds libfuse's *high-level path-based*
   API, which does not expose `fuse_lowlevel_notify_*` at all. `FileSystemHost.Notify` is a WinFsp
   DLL call with Windows-shaped actions and does nothing on Linux. **It cannot punch the kernel page
   cache.** It also forces cgo + libfuse-dev + gcc on every importer. It remains the right choice
   for a *future Windows backend*, behind a build tag, in a separate package.
2. **bazil/fuse is disqualified on maintenance.** Zero commits since 2023-01-19, no tags, `go 1.19`.
   It actually has 3 of the 4 notifications and a decent option set — it is not a bad library, it is
   an abandoned one. Adopting it makes us its maintainer.
3. **jacobsa/fuse is not disqualified but should not be chosen.** It has only 2 of 4 notifications
   (no `notify_delete`, no `notify_store`), added only in May 2025; the notifier serialises through
   one goroutine on unbuffered channels; it has no `AUTO_INVAL_DATA`/`EXPLICIT_INVAL_DATA` control,
   no `max_readahead`, no `max_background`; it is measured ~1.8x slower than go-fuse (#78, open
   since 2020); it pins ~1 MiB per in-flight request (#197, open); it makes us implement lookup-count
   and FORGET accounting from scratch; and it has never been tagged. Its trajectory is set by
   gcsfuse's needs, not ours.

### Concrete configuration to start from

```
fs.Options{
    // Leave EntryTimeout/AttrTimeout/NegativeTimeout nil -> always set per response (G5).
    FirstAutomaticIno: <above our own Ino space>,               // G14
    MountOptions: fuse.MountOptions{
        Name:                     "<fsname>",
        ExplicitDataCacheControl: true,     // CAP_EXPLICIT_INVAL_DATA, not AUTO_INVAL_DATA (G4)
        ExtraCapabilities:        fuse.CAP_WRITEBACK_CACHE,     // for commit-on-close write batching
        MaxWrite:                 1 << 20,  // 1 MiB; also drives max_read and MaxPages
        MaxReadAhead:             1 << 20,
        MaxBackground:            256,      // default 12 is far too low for a network FS (G9)
        PanicHandler:             ...,      // v2.11; do not let an FS bug kill the daemon (G7)
        Logger:                   ...,      // never write to the global logger
        // MaxInflightRequestBytes: leave unset; do admission control in the RPC layer (G12)
        // EnableSymlinkCaching: only together with content notification on symlinks
    },
}
```
Per-open: return `fuse.FOPEN_KEEP_CACHE` from `Open`. Implement `FileLookuper` on the directory
handle (G6). Never recycle inode numbers (G2). Do not store paths in nodes (G1).

### Open questions to settle in design, not blockers for the library choice

- Eviction policy for our own inode cache (G1) — LRU vs watermark; and the fallback path for
  kernels < 6.18 that lack `NOTIFY_PRUNE`.
- Interrupt policy (G3): which operations may return EINTR, and whether to require the cancellation
  to persist before honouring it.
- Whether `FUSE_WRITEBACK_CACHE` is actually right for commit-on-close, or whether we want the
  simpler unbuffered-write path. Writeback caching changes O_APPEND and mtime ownership semantics
  (the kernel takes over both), which interacts with a multi-client volume.
- Whether to adopt the `/dev/fd/N` supervisor pattern for restart-without-unmount from day one.
  It shapes the daemon's process model, so decide early even if implemented later.

---

## 13. Sources

Primary (source read at pinned SHAs):
- hanwen/go-fuse @ `423b377e1452ab7b3522229185a3047f72e3f966` (v2.11.0) and HEAD
  `5e1b6c816c2db624f5012b3f8bceffcd702b634e`
- jacobsa/fuse @ `a124548f6da78ddcc3681b4e61868e1e4dadd728`
- bazil/fuse @ `62a210ff1fd54902d27be7ac05d1b13b6f323ccd`
- winfsp/cgofuse @ `2fa812d1bdc77bb86c4bbf17bf4745d11674b85b`
- juicedata/go-fuse @ `0752e53` (fork), notably `c9623809d765f694a3757b281dcfda687fdb1706`

Issues cited: hanwen/go-fuse #354, #352, #382, #504, #506, #532, #603, #606, #615, #616;
jacobsa/fuse #78, #122, #143, #156, #163, #197; GoogleCloudPlatform/gcsfuse #4813, #4484/#4546.

CI: https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/.github/workflows/ci.yml
and `all.bash` at the same SHA; https://github.com/GoogleCloudPlatform/gcsfuse/blob/master/.github/workflows/ci.yml;
https://github.com/juicedata/juicefs/tree/main/.github/workflows

Status: COMPLETE.

---

## 14. Adversarial review of the architecture document

Reviewed: `docs/design/architecture.md` (Status: draft, 2026-08-18), against FUSE kernel behaviour
and hanwen/go-fuse implementation. Sources pinned:

- hanwen/go-fuse **v2.11.0** = `423b377e1452ab7b3522229185a3047f72e3f966`
- libfuse `1f28a174efd4c5045d40fb93485b15763f8af457`
- Linux **v6.12** = `adc218676eef25575469234709c2d87185ca223a`

Findings are numbered A1..A9 and ordered by severity. Everything not listed here was checked and
holds.

### A1 (CRITICAL) — §6.4's "long lifetimes" and §6.3's "flushed explicitly at the same moment" cannot both be true

§6.4 says that while **Observing**, "lifetimes are long, because push rather than time is what keeps
answers honest", and that on **Severed** "nothing is served from cache. Every operation reaches the
provider, or fails." §6.3 covers the kernel side with one sentence: "The kernel's caches have no
notion of generations, so they are flushed explicitly at the same moment."

That sentence is doing far more work than it looks, and the arithmetic does not close.

**There is no bulk-flush primitive in the FUSE protocol.** The complete set of reverse notifications
is per-inode or per-(parent, name): `NOTIFY_INVAL_INODE`, `NOTIFY_INVAL_ENTRY`, `NOTIFY_DELETE`,
`NOTIFY_STORE`, `NOTIFY_RETRIEVE`, and (kernel 6.18+ only) `NOTIFY_PRUNE`. Flushing the kernel's
attribute and dentry caches therefore costs **one write(2) per cached inode**, and each
`NOTIFY_INVAL_ENTRY` takes the parent directory's `i_rwsem` **exclusively** for its duration:

```c
	inode_lock_nested(parent, I_MUTEX_PARENT);
```
https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/dir.c#L1360-L1428

Combine that with the measured inode counts from a real go-fuse deployment — 30,344,144 cached
inodes after 172 days (go-fuse#603, cited in §11/G1) — and "flushed at the same moment" is tens of
millions of syscalls, each contending on directory locks, executed at exactly the moment the daemon
is already in trouble.

**Until that flush completes, the kernel answers `stat(2)` and path resolution from its own caches
without contacting us at all.** The window is bounded only by the `attr_valid` / `entry_valid` we
handed out. So the longer the lifetime, the longer §1.4 is violated after we lose the right to
serve — and it is violated silently, since we never see the requests.

**This inverts the role of cache lifetimes.** §6.2 calls them "a backstop against a provider failing
to report something it should have reported". They are also, and more importantly, **the hard upper
bound on how long the kernel keeps answering after we go Severed**. That is not a backstop; it is a
primary safety parameter.

Required change to §6.4: lifetimes must be **bounded by the transport's liveness-detection window**,
not "long". The correct framing is that the TTL is the residual exposure after detection fails, so
`TTL <= detection window` makes the exposure at most one detection interval. §10.2's liveness row
already says "Loss detected faster than the cache grace window"; §6.4 must be made to agree with it
rather than pulling the other way. The bulk flush then becomes a best-effort accelerator rather than
the mechanism the guarantee rests on.

Secondary note: `NOTIFY_PRUNE` (go-fuse `Inode.NotifyPrune`, batched, one write for many inodes) is
the right tool for this, and is the reason to care about a kernel 6.18 baseline sooner than one
otherwise would.

### A2 (CRITICAL) — §7.4's "mounts are configured to be released automatically when the daemon goes away" is not achievable with go-fuse; attempting it hangs the mount

§7.4 states that "mounts are configured to be released automatically when the daemon goes away".
The only mechanism that does this is libfuse's `-o auto_unmount`. It is incompatible with go-fuse.

**How `auto_unmount` actually works.** `fusermount3 --auto-unmount` does *not* exit after handing
the `/dev/fuse` fd back over the `_FUSE_COMMFD` socket. It falls through to `wait_for_auto_unmount:`,
`setsid()`s, blocks all signals, and sits in `recv()` on the control socket forever; when that socket
closes (i.e. the FUSE daemon died) it unmounts:
https://github.com/libfuse/libfuse/blob/1f28a174efd4c5045d40fb93485b15763f8af457/util/fusermount.c#L1856-L1890
libfuse's own mount path documents the consequence explicitly:
```c
	if (!mo->auto_unmount) {
		/* with auto_unmount option fusermount3 will not exit until
		   this socket is closed */
		close(fds[1]);
		waitpid(pid, NULL, 0); /* bury zombie */
	}
```
https://github.com/libfuse/libfuse/blob/1f28a174efd4c5045d40fb93485b15763f8af457/lib/mount.c#L437-L442

**What go-fuse does.** `callFusermount` starts `fusermount`, then `proc.Wait()`s for it to exit
*before* reading the fd off the socket, and `defer`s closing **both** ends of the socketpair:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/mount_linux.go#L121-L167

So passing `-o auto_unmount` through `MountOptions.Options` produces two failures at once:
1. `fusermount3` never exits (it does not fork — the comment in `fusermount.c` says so
   deliberately), so `proc.Wait()` **blocks forever and `fuse.NewServer` never returns.**
2. Even if it did return, go-fuse closes the control socket on the way out, which is precisely the
   auto-unmount trigger.

go-fuse has no `AutoUnmount` field and no code path that keeps the helper alive. This is not a
missing convenience; the option actively deadlocks mount.

**Required change to §7.4.** Either:
- **(preferred) move the fusermount call into the daemon.** Run `fusermount3 --auto-unmount`
  ourselves, keep the control socket open for the daemon's lifetime, receive the `/dev/fuse` fd, and
  hand go-fuse the **magic `/dev/fd/N` mountpoint** so it skips `callFusermount` entirely
  (`parseFuseFd`, mount_linux.go#L170-L204; documented under "Mount styles" in
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/api.go#L85-L124).
  This keeps the library free of the process-model decision, which §1.2 wants anyway. It is also
  exactly the mechanism A8 needs, so the two should be designed together.
- or **drop the claim** and rely solely on the other half of §7.4 (a starting daemon clears a stale
  mount at a path it is asked to reuse) plus an external supervisor. That is honest but leaves a
  wedged mount behind between the crash and the next start.

Note that this is also the seam that makes §9's "restart without unmounting" row tractable, so
choosing the first option buys both.

### A3 (MAJOR) — §5.1's negative caching cannot vary by connection health, and negative entries are invisible to the adapter

Two separate defects, one cause.

**(a) The kernel-level claim in §5.4 is correct.** `FUSE_NOTIFY_INVAL_ENTRY` *does* clear a
negatively cached name. `fuse_reverse_inval_entry` does `d_lookup(dir, name)` — which finds negative
dentries, they are hashed in the dcache — then `d_invalidate(entry)` and
`fuse_invalidate_entry_cache(entry)`:
https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/dir.c#L1360-L1428
and `d_invalidate` handles them explicitly:
```c
	__d_drop(dentry);
	spin_unlock(&dentry->d_lock);

	/* Negative dentries can be dropped without further checks */
	if (!dentry->d_inode)
		return;
```
https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/dcache.c#L1589-L1616
So "a create must actively forget a remembered miss" is implementable. Good.

**(b) But the TTL on that miss is global-only in go-fuse, and setting it per response silently
disables negative caching altogether.** `rawBridge.Lookup`:

```go
	if errno != 0 {
		if errno == syscall.ENOENT && b.options.NegativeTimeout != nil && out.EntryTimeout() == 0 {
			out.SetEntryTimeout(*b.options.NegativeTimeout)
			errno = 0
		}
		return errnoToStatus(errno)
	}
```
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L358-L375

A negative dentry exists only when the reply is **OK with `NodeId == 0` and a nonzero
`entry_valid`** — the kernel comments this directly ("Zero nodeid is same as -ENOENT, but with valid
timeout", `fuse_lookup_name`) and only applies the timeout in that case; a genuine ENOENT *error*
reply makes `fuse_lookup` call `fuse_invalidate_entry_cache` and cache nothing:
https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/dir.c#L403-L455

The only go-fuse path to that reply is the branch above, and the guard `out.EntryTimeout() == 0`
means: **if our node sets an entry timeout on `out` and then returns ENOENT, the branch is skipped,
go-fuse replies with a real ENOENT error, and the kernel actively clears the negative entry.**
Returning `(nil, 0)` instead is not an option — `b.Lookup` would then dereference the nil child in
`child.setEntryOut(out)`. So:

> **Per-response negative-entry TTL is not expressible in go-fuse's `fs` layer.** The one knob is
> `fs.Options.NegativeTimeout`, a single fixed duration for the whole mount
> (https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/api.go#L779).

**Consequences for §6.4.** The state table has no row that survives this. Negative entries cached
while **Observing** persist into **Severed** for the full global timeout, and the kernel answers
`ENOENT` from them **without calling us**. That is §1.4's forbidden outcome ("never 'no such file'
when it means 'I could not reach the server'") reached by default, in the one case §7.1 spends a
paragraph insisting must never happen. Note it is not saved by §7.1's rule that a severed provider
returns EIO rather than ENOENT: no request reaches us at all.

**Required changes:**
1. §6.4 needs an explicit exception: the negative-entry lifetime is a fixed mount-wide constant, not
   a function of connection state. It should be set short — shorter than the positive lifetimes — for
   the same reason as A1.
2. **The core must track, per directory, the names it answered ENOENT for**, purely so they can be
   invalidated. go-fuse's `fs` layer never materialises an `*fs.Inode` for a miss, so the adapter
   has no way to enumerate them; nothing above the core knows they exist. This is a concrete data
   structure §3.2's "second index from parent-and-name to node" does not currently cover, because a
   miss has no node. Say so in §3.2 or §6.1.
3. §6.1's table row "Kernel name and metadata cache ... controlled by lifetimes we attach to every
   answer" should be qualified: for *negative* answers the lifetime is not per-answer.

### A4 (MAJOR) — §5.2's "keep the page cache across opens" is false unless `explicit_inval_data` is negotiated

§5.2 relies on retaining the kernel page cache across opens, invalidating only on push. `go-fuse`
does let us do the first part per open: `NodeOpener.Open` returns `fuseFlags`, and the bridge assigns
them verbatim (`out.OpenFlags = flags`), so `fuse.FOPEN_KEEP_CACHE` is ours to set per open:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L745-L768

**But something else drops those pages, and it fires constantly in a multi-client volume.**
`fuse_change_attributes` runs on *every* attribute we return — from GETATTR replies and from LOOKUP
replies:

```c
	if (!cache_mask && S_ISREG(inode->i_mode)) {
		bool inval = false;

		if (oldsize != attr->size) {
			truncate_pagecache(inode, attr->size);
			if (!fc->explicit_inval_data)
				inval = true;
		} else if (fc->auto_inval_data) {
			...
			if (!timespec64_equal(&old_mtime, &new_mtime))
				inval = true;
		}

		if (inval)
			invalidate_inode_pages2(inode->i_mapping);
	}
```
https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/inode.c#L262-L331

Three facts follow:
1. **A reported size change always calls `truncate_pagecache()`**, unconditionally. `FOPEN_KEEP_CACHE`
   does not protect pages past the new size. (Fine for us — a size change is a real content change.)
2. Unless `explicit_inval_data` is set, a size change **also** invalidates the *entire* mapping.
3. With `auto_inval_data`, **any mtime change in any attribute reply invalidates the entire mapping**.

**go-fuse negotiates `auto_inval_data` by default:**
```go
	if server.opts.ExplicitDataCacheControl {
		kernelFlags |= input.Flags64() & CAP_EXPLICIT_INVAL_DATA
	} else {
		kernelFlags |= input.Flags64() & CAP_AUTO_INVAL_DATA
	}
```
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/opcode.go#L127-L133

So with default settings, the first `stat` after any other client touches a file discards our whole
page cache for it, and §5.2's stated benefit ("re-reading an unchanged file is free") evaporates for
exactly the workloads the design targets. `cache_mask` is nonzero only under writeback caching, which
is inappropriate here because §5.3 stages writes in a local file rather than letting the kernel own
dirty pages.

**Required change:** §5.2 (and §10.1, which currently records only the invalidation requirement)
must state that the design depends on negotiating `FUSE_EXPLICIT_INVAL_DATA`, i.e.
`MountOptions.ExplicitDataCacheControl = true`, kernel >= 4.19. This is a second binding-level
constraint of the same kind as invalidation, and it has a sharp edge worth naming in §6: once the
kernel stops auto-invalidating, **there is no self-healing** — every stale page is a bug in our push
path with no timeout that eventually rescues it.

### A5 (MAJOR) — §5.4's stated reason for cache-first ordering is wrong, and ordering alone does not close the race

§5.4: "The core's cache is updated before the kernel is told to forget, because the kernel will
immediately ask again; telling it to forget first means it re-asks a cache that has not yet learned
anything and receives the stale answer it was just told to discard."

**The kernel does not immediately ask again.** Both reverse-invalidation handlers only *drop*:
`fuse_reverse_inval_inode` does `fuse_invalidate_attr` + `invalidate_inode_pages2_range` and returns
(https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/inode.c#L619-L648);
`fuse_reverse_inval_entry` does `d_invalidate` + `fuse_invalidate_entry_cache` and returns. Neither
issues a request. The re-ask happens on the next access by a program, which may be immediately, in a
microsecond, or never.

The conclusion (update first) is still right, but for the general reason — the invalidation is a
release fence, and an access concurrent with the notify write must find the new value — not for the
stated one. As written, the reasoning invites an implementer to conclude that ordering is
*sufficient*. **It is not.**

**The race that ordering does not close:**

```
 T0  LOOKUP/GETATTR for node X dispatched to the provider   (in flight, will return v1)
 T1  change event for X arrives; core updates its cache to v2
 T2  core tells the kernel to invalidate X
 T3  the T0 request completes and writes v1 back into the cache
 T4  kernel re-asks, gets v1, and caches it with a full-length TTL
```

After T4 the kernel holds the stale value with **no pending invalidation to correct it**, for the
whole TTL. §6.5 is what fixes this — every answer carries the position of the state it reflects and
is applied only if at least as recent as what is held — and §6.5 already notes that the ordering is
"the normal case, not a rare interleaving".

**Required change:** §5.4 should replace the "kernel asks again immediately" justification with the
correct one and **explicitly forward-reference §6.5** as the rule that makes the ordering safe rather
than merely well-intentioned. The two rules are one mechanism described in two places; a reader of
§5.4 alone would build something wrong.

### A6 (MAJOR) — §5.4's "queue it and drain it off the request path" is necessary but not sufficient; the queue must never block a producer

§5.4 correctly identifies the deadlock geometry and correctly prescribes asynchrony. The geometry is
real and worth pinning precisely, because the exact shape determines what "asynchronous" has to mean.

`fuse_reverse_inval_entry` takes the **parent directory's `i_rwsem` exclusively** and holds it for
the whole operation:
```c
	inode_lock_nested(parent, I_MUTEX_PARENT);
	...
 unlock:
	inode_unlock(parent);
```
https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/dir.c#L1360-L1428

The VFS holds that same lock across `->lookup`, `->create`, `->unlink`, `->mkdir` and `->rename` —
that is, **while it is waiting for our reply**. So:

- Thread A (a user process) calls `unlink("/mnt/d/f")`. VFS takes `d`'s `i_rwsem`, sends FUSE_UNLINK,
  and waits for us.
- Our handler goroutine for that UNLINK calls `NotifyEntry(d, "anything")` before replying. The
  `write(2)` enters `fuse_reverse_inval_entry`, blocks on `inode_lock_nested(d)` — held by thread A,
  which is waiting on us. Deadlock.

**go-fuse itself does not reintroduce the problem at v2.11.0.** Notify writes go through
`fuseFD.writevFD`, which uses `syscall.RawConn.Control` — a refcount, not a mutex — precisely so
"concurrent writers and close() [can] run without a serializing mutex":
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/fusefd.go#L74-L97
Replies (`fuseFD.write`) take no lock either. So one stuck notify blocks only its own goroutine.
(This is the §5 version boundary again: on <= v2.10.1 a global `writeMu` covered notify writes, and
a stuck notify would have blocked every other notify.)

**What the architecture must add.** "Off the request path" closes the *self*-deadlock only if
enqueueing can never block a request handler. Concretely:

- If the invalidation queue is **bounded** and a request handler produces into it, then when the
  single drainer is blocked on a directory lock held by a request that is itself waiting for that
  producer, the producer blocks on a full queue and the deadlock returns through the back door.
  **Enqueue must be non-blocking**: unbounded, or coalescing/dropping with a fallback to a
  whole-generation flush.
- The drainer must not share the concurrency budget of §8.1. §8.1 says "reaching a bound causes
  waiting, not failure"; a waiting drainer is a stalled invalidation path, and a drainer that waits
  on a semaphore held by request handlers that are waiting on the drainer is the same cycle again.
  State that the invalidation path has its own, unshared capacity.
- Because the parent lock is **exclusive**, a burst of invalidations in one hot directory serialises
  against every lookup in that directory. This is not a deadlock but it is a latency coupling that a
  push-heavy design will meet often. **Coalesce per directory** before draining.

Also worth recording in §5.4's table: `NOTIFY_DELETE` (the "Node removed" row) is not
best-effort-succeeds. When a child node id is supplied the kernel takes a *second* lock on the child
inode and can return `-ENOTEMPTY` (cached non-empty directory), `-EBUSY` (mountpoint), or `-ENOENT`
(node id mismatch), leaving the entry cached. It needs a `NOTIFY_INVAL_ENTRY` fallback.

And a useful primitive the table currently has no way to express: the "Metadata changed — leave the
contents alone" row is available by passing a **negative offset** to `InodeNotify` /
`Inode.NotifyContent`, since the kernel guards the page-range invalidation with `if (offset >= 0)`
while always doing `fuse_invalidate_attr`
(https://github.com/torvalds/linux/blob/adc218676eef25575469234709c2d87185ca223a/fs/fuse/inode.c#L619-L648).
`NotifyInvalInodeOut.Off` is `int64`, so go-fuse passes it through unchanged.

### A7 (MODERATE) — §3.2's rule is right, but it names the wrong identifier, and the real failure is worse than described

§3.2: "Kernel-side identifiers are a third, separate space, owned by the adapter and never derived
from provider identifiers by hashing. A hash collision would merge two unrelated files into one."

The *conclusion* is exactly right. Two details need correcting.

**The kernel-side identifier is not ours to get wrong.** go-fuse allocates the FUSE NodeID itself,
as a monotonic counter starting at 2 (root is 1), and we never see or choose it:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L95-L102
So §3.2's requirement for that space is satisfied for free by the binding.

**The identifier that is actually at risk is `fs.StableAttr.Ino`, and it does double duty.** It is
both (a) the `st_ino` reported to userspace and (b) **the deduplication key of go-fuse's identity
table**. `addNewChild` looks up `b.stableAttrs[id]` where `id = child.stableAttr` (`{Mode, Ino, Gen}`)
and, on a hit, **discards the node we just constructed and returns the existing one**:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L178-L240

So an `Ino` collision between two files of the same type does not merely report a duplicate `st_ino`
to userspace — go-fuse **silently makes them the same node**. Every subsequent operation on either
path reaches the same `*fs.Inode`, with our own per-node state (the provider identifier, the staging
file, the open handles) belonging to whichever won. Reads of one file return the other's bytes; a
commit to one overwrites the other. It is precisely §1.4's forbidden failure, one layer below where
§3.2 places it, with no error anywhere.

Note this is deliberate on go-fuse's part — it is how the library supports hard links (the source
comment shows `dir1/file` and `dir2/file` resolving to one node). We have no hard links (§1.3), so
for us any collision is pure corruption.

**Suggested edit to §3.2:** name the at-risk identifier as "the inode number reported to the kernel"
and state that in the chosen binding it is also the identity key of the adapter's node table, so a
collision merges nodes rather than merely confusing `stat`. Add the positive rule: allocate from a
monotonic 64-bit counter, never recycle (§11/G2), and keep values below `2^63` because that is where
go-fuse's own automatic allocation starts (`fs.Options.FirstAutomaticIno`), and never equal to
`^uint64(0)`, which go-fuse reserves and panics on.

### A8 (MODERATE) — §7.4 / §9's "no cheap seam" is the right verdict, reached by the wrong route

§7.4 says restart-without-unmounting "requires handing the kernel connection to a successor process
along with the node table, open handles, and change position", and §9 calls it "the one genuinely
expensive item".

**Handing over the kernel connection is the cheap part, and it already exists upstream.** go-fuse's
`NewServer` accepts a magic `/dev/fd/N` mountpoint and uses the inherited fd directly instead of
calling `fusermount`:
https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/api.go#L85-L124
So the sentence as written attributes the cost to the one component that is free.

**What actually blocks it, and neither is mentioned:**

1. **`NewServer` unconditionally performs the INIT handshake**, and FUSE_INIT happens exactly once
   per connection. `handleInit()` reads one request and treats it as INIT:
   https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/server.go#L412-L434
   On a *reused* connection there is no INIT to read, so the successor consumes a live request as if
   it were INIT and proceeds with a zero-valued `kernelSettings` — wrong negotiated flags, wrong
   `MaxWrite`, wrong protocol minor. juicedata hit exactly this: their graceful-restart fork
   **serialises the negotiated `InitIn` over the same unix socket that carries the fd** and
   reconstructs `ms.kernelSettings` on the far side instead of running the handshake
   (https://github.com/juicedata/go-fuse/commit/002ef792942ef19087df4f2ae5a53436b7f5a05f,
   330 lines changed in `fuse/server.go`). There is no upstream seam for this.
2. **An empty node table is fatal on the first request.** The kernel still holds every NodeID it had,
   and go-fuse's lookup does:
   ```go
   	n, f := b.kernelNodeIds[id], b.files[fh]
   	if n == nil {
   		log.Panicf("unknown node %d", id)
   	}
   ```
   https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L348-L356
   With `MountOptions.PanicHandler` set (v2.11) this is recovered into an errno rather than killing
   the process — `protocolServer.handleRequest` wraps the handler in `recover()`
   (https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/protocol-server.go#L48-L60) —
   but `log.Panicf` still writes to the *global* logger first, so a successor with an empty table
   floods stderr and returns EIO for everything. The node table must be transferred or rebuilt; it
   cannot be lazily repopulated.

**Verdict: the claim stands, and is if anything understated** — it needs either a go-fuse fork or an
upstream contribution, not just a supervisor. The wording should be corrected so a later reader does
not go looking for the saving in the wrong place. And note the connection to A2: the supervisor that
holds the fd for `auto_unmount` is the same supervisor that would hold it across a restart, so if A2
is resolved the first way, the *fd-custody* half of this row is already built and what remains is
INIT-state plus node-table transfer.

### A9 (MINOR) — smaller corrections

- **§6.1's table, row 1.** "lifetimes we attach to every answer" is true for positive entries and for
  attributes (`EntryOut.SetEntryTimeout` / `SetAttrTimeout`, `AttrOut.SetTimeout`, honoured
  per-response because `setEntryOutTimeout` fills the mount-wide default only when the node left the
  field at zero:
  https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fs/bridge.go#L255-L262).
  It is **false for negative entries** (A3). One consequence of that same code: because zero means
  "apply the default", a mount-wide `fs.Options.AttrTimeout`/`EntryTimeout` makes a genuine
  zero-second lifetime inexpressible. §6.4's **Severed** row wants exactly zero, so the mount-wide
  values must be left unset and every response must set its lifetime explicitly. Worth one sentence
  in §6.1 so it is not discovered by experiment.

- **§4.1 / §1.2's "no process-wide mutations".** Importing `hanwen/go-fuse/v2/fuse` **permanently
  reserves file descriptor 3** in the host process (deliberate: the `fusermount` helper inherits
  exactly one fd, and it must not be one pointing into a FUSE mount), and importing the `splice`
  package reads `/proc/sys/fs/pipe-max-size`, creates a probe pipe, and calls `log.Panicf` on the
  global logger if `os.Pipe()` fails. Neither is a defect, but both are process-wide effects we
  inherit and cannot suppress, so §4.1's promise needs a stated exception rather than being quietly
  untrue. (Details in §12 of this document.)

- **§8.1's "reaching a bound causes waiting, not failure"** is right for the request path, but see
  A6: the invalidation drainer must be outside that budget.

- **§5.5 / §6.4's "Interrupted" state.** Holding FUSE requests open while waiting for reconnection is
  fine for go-fuse (a blocked request is one goroutine), but it makes `Server.Unmount()` fail
  indefinitely — the FUSE loops exit only when the kernel agrees the filesystem can be unmounted
  (go-fuse#532). §7.4's shutdown story therefore needs the fusectl escape hatch
  (`/sys/fs/fuse/connections/<devid>/abort`, where `<devid>` is the `Dev` field of a `stat` inside
  the mount) as a supported step, not just as folklore. Also note `MountOptions.MaxBackground`
  defaults to **12**, which throttles kernel readahead/writeback hard for a network filesystem; §8.1
  should say the kernel-side window is a tunable we own, not only the client-side budgets.

- **§5.2's interrupt paragraph checks out.** `fuse.Context` implements `context.Context` and is
  cancelled on FUSE_INTERRUPT, and the `fs` layer passes it as the `ctx` of every node method, so
  "interruption propagates the whole way down" is implementable end to end. One caveat for the
  detailed design, not for this document: the Go runtime's SIGURG preemption signal makes *every Go
  program* reading our mount generate interrupts, so honouring cancellation by returning EINTR
  unconditionally will break them (§11/G3).

### What was checked and holds

§1.4's I/O-error-over-plausible-data rule (nothing in FUSE forces a different mapping); §3.1's
attributes-with-listings requirement (go-fuse enables READDIRPLUS by default and `FileLookuper`
exists so the per-entry lookups can be served from one batch); §3.3's tiering; §5.3's staging and
commit-on-close (commit at FLUSH is the only point where an error reaches `close(2)`, and go-fuse
exposes `NodeFlusher` separately from `NodeReleaser`); §6.3's generation counter as a whole-cache
invalidation for *our own* cache; §6.5's position rule; §7.1; §7.6; §8.2; §8.3's default that a mount
is reachable only by its creator (`allow_other` additionally requires `user_allow_other` in
`/etc/fuse.conf`); §10.1's binding conclusion; §10.2's transport table.

