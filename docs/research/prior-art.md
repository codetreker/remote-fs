# Prior Art: Remote Filesystems Exposed via FUSE / OS File-Provider APIs

Research checkpoint file. Sections are appended as each system is finished.
Status: IN PROGRESS (started 2026-08-18).

Scope: a Go library + daemon mounting our own remote service as a FUSE filesystem on
Linux, with cloud-drive semantics (commit-on-close, weak concurrency, no atomic rename,
no hardlink), server-pushed change events driving kernel cache invalidation.

Decisions under test (verdicts at the end of this document):

- D1. Nodes identified by server-assigned stable ids, not paths.
- D2. Cache validity = "we have continuously observed the change stream since fetching",
  implemented with an epoch counter; TTL is only a backstop.
- D3. Directory listings carry attributes inline (readdirplus-style).
- D4. Page cache kept across opens (FOPEN_KEEP_CACHE), relying on push invalidation.
- D5. Writes staged in a local file, hydrated on open-for-write, committed in `flush`.
- D6. Conflict detection and offline write replay explicitly out of scope.

---
