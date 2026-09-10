# Backing a volume with an object store

Evidence gathered while designing `packages/storage/objectstore`. Three questions were
investigated, each because a design decision turned on it and the answer was not obvious
from the documentation alone.

Frozen on completion. Where a claim was checked by running something rather than by reading
something, it says so.

## 1. Reclaiming objects nothing references

An object lands in the store at T, and the metadata transaction that references it commits
at T+δ. A sweeper running in between, applying "delete every object no row references",
destroys live data. Nobody who has shipped this solves it by scanning alone.

The systems surveyed split into two answers.

### Those that guess, with a grace period

| System | Default | Setting |
|---|---|---|
| Apache Iceberg | 3 days | `older_than` on `remove_orphan_files` |
| Delta Lake | 7 days | `delta.deletedFileRetentionDuration` |
| SeaweedFS | 5 hours | `volume.fsck -cutoffTimeAgo` |
| JuiceFS | 1 hour | mtime cutoff in `gc --delete` |

Every one of them documents that too short an interval loses data. Iceberg's is the
plainest — <https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files>:

> It is dangerous to remove orphan files with a retention interval shorter than the time
> expected for any write to complete because it might corrupt the table if in-progress files
> are considered orphaned and are deleted. The default interval is 3 days.

The procedure's own parameter table gives the default:
<https://iceberg.apache.org/docs/latest/spark-procedures/#remove_orphan_files>. Delta's
equivalent is `VACUUM`, whose safety check must be explicitly disabled to go below the
retention period: <https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table>.

The interval is not a tuning parameter in any real sense. It is an assertion about the
slowest write the system will ever perform, made in advance, by someone who cannot know.

### Those that do not guess

**s3ql** writes an intent row into its metadata database *before* uploading — an `objects`
row with `phys_size = -1`. Every blob in the backend is therefore named by a committed row
from the instant it exists, so its sweeper needs no grace period and has none: it deletes
unknown blobs outright. That is only sound because s3ql enforces single-mount exclusivity;
its sweeper cannot see another mount's rows. (Read from the s3ql tree at
`d6860094fe5464cd1fe35f744aae706ea1d52dc6`.)

**Ceph RGW** never scans. Garbage entries are enqueued in the same atomic operation that
unlinks the old version, tagged with that version's `tail_tag`, so a concurrent writer's
data is structurally un-nameable by the collector:
<https://docs.ceph.com/en/latest/radosgw/orphans/>. It does keep a floor —
`rgw_gc_obj_min_wait`, default 2 hours
(<https://docs.ceph.com/en/latest/radosgw/config-ref/#confval-rgw_gc_obj_min_wait>) — but
that protects **readers already in flight**, not writers. The two hazards are separate and
the mechanisms for them are separate.

**RocksDB** fences by generation: a file is deleted only if it is unreferenced *and* its
file number is below the oldest in-flight output. The pending set makes the question
decidable rather than probable.

### What this repository took

Both of the mechanisms that do not guess, because they answer different halves of the
problem. The intent row covers an object uploaded before a crash; enqueue-on-unlink covers
an object displaced by an overwrite or a removal. Together the sweeper never lists the
container and never waits out an interval.

The reader hazard Ceph's floor addresses is real here too and is **not** solved by either:
a reader holding a key can have it swept out from under it. That is handled by making the
read distinguish "replaced" from "lost" rather than by a floor, and the cost of that choice
is recorded as a proposal rather than a fact — see
[`.agents/notes/proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md`](../../.agents/notes/proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md).

## 2. What Azure Blob reports as a content digest

The dedup proposal turns on whether the digest that comes back from an upload is the
service's own report or an echo of what the client sent. An echo proves nothing about what
was stored.

**Computed, not echoed.** Probed against Azurite 3.36.0 with a hand-rolled shared-key
client, so the request headers were under direct control:

- A `Put Blob` carrying **no checksum header at all** answered
  `content-md5: 9T/uc4AGQ/9nyEFFt9Xmhg==`, equal to the true MD5 of the payload. There was
  nothing to echo.
- A `Put Blob` carrying a **deliberately wrong** `Content-MD5` was refused with
  `400 InvalidOperation "Provided contentMD5 doesn't match"`. The object was not stored.

Microsoft's own description agrees for the real service: Content-MD5 is computed by Blob
Storage and is returned even when the request does not include it
(<https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties>).

**Two limits that matter more than the above.**

*It is not a corruption detector, whatever the header names suggest.* The client sends a
checksum with the bytes and the service refuses a mismatch, so a corrupted transfer arrives
as a failed `Put` and never as a digest that differs from the expected one. Any code that
compares the returned digest against a locally computed hash to look for damage can never
fire.

*It is MD5.* Chosen-prefix collisions against MD5 are practical, so anything that treats
equal digests as equal content is forgeable by anyone who can write to the volume. The
alternative Azure offers, `x-ms-content-crc64`, is a checksum rather than a
collision-resistant hash and is no better for this purpose. A dedup pass that must survive a
hostile writer has to compute its own digest.

**Azurite and Azure differ here.** Azurite returns Content-MD5 only; the real service also
returns `x-ms-content-crc64`, documented as always present since version 2019-02-02. That
half is doc-only — nothing in this repository has been run against a real account.

## 3. Pure-Go SQLite drivers

The module is cgo-free and [`fuse-libs.md`](fuse-libs.md) records rejecting a whole FUSE
library to keep it that way, so the driver had to be pure Go. Two candidates were measured
rather than compared on reputation. Both were put through the same probe: WAL mode, a
`BLOB` name column, byte-order `ORDER BY`, an invalid-UTF-8 name round-trip, and a
detectable uniqueness violation.

| Driver | Version | Vendor size | `.go` files | Modules | Probe |
|---|---|---|---|---|---|
| `modernc.org/sqlite` | v1.57.0 | 140 MB | 1886 | 24 | all four pass |
| `github.com/ncruces/go-sqlite3` | v0.35.3 | 14 MB | 389 | 15 | all four pass |

Measured by building a throwaway module importing only the driver and running
`go mod vendor`. The module counts include build-time dependencies in the graph.

Both order `"B" < "README" < "Z" < "a" < "readme" < "\xff\xfe"` — byte order,
case-sensitive, invalid UTF-8 preserved — which is what the storage contract's listing
order requires and what a `TEXT` column under a locale collation would not give.

`github.com/mattn/go-sqlite3` was not measured: it requires cgo, which is the property
`fuse-libs.md` refused to give up.

The choice went to `modernc.org/sqlite` on deployment breadth rather than on any measured
advantage — it is an order of magnitude heavier. The driver sits behind `database/sql`, so
the decision is cheap to revisit; the reasoning is in
[`2026-08-21-volume-in-an-object-store.md`](../../.agents/notes/implemented/architecture/2026-08-21-volume-in-an-object-store.md).

## 4. Azurite as a test dependency

Azurite's blob service rejects an `x-ms-version` above the highest it knows, answering
`400 InvalidHeaderValue` rather than serving the request. The emulator's version and the
SDK's are therefore one compatibility pair, not two independent choices: the pinned Azurite
determines the newest azblob SDK that can be used against it, and upgrading the SDK requires
a newer emulator rather than a header rewrite.

Two flags matter for a test dependency. `--inMemoryPersistence` keeps state in memory, so no
run inherits blobs from the run before it and there is no file on disk to grow. Binding the
published port to loopback matters because the emulator authenticates with a published,
well-known account key: reachable from the network, it is an open door.

The image declares no health check, and the port accepts connections before the service
answers on them, so a TCP probe reports ready too early. Asking the blob service to identify
itself works — an unauthenticated request is refused, but the refusal still carries the
`Server: Azurite-Blob/` header, and that header rather than the status code is the fact
worth testing.
