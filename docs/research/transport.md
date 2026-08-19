# Transport & Push-Invalidation Research

Status: IN PROGRESS (checkpointed incrementally; sections appended as completed)
Scope: wire transport + change-stream/invalidation channel for a Go FUSE client/daemon
mounting a remote FS over a WAN, where we control both ends.

Cache-validity invariant driving everything below:

> A cached entry is valid iff we have **continuously observed** the change stream since
> fetching it. After any gap in observation the client must either *prove* it missed
> nothing, or drop the whole cache. Silently resuming "from now" is a correctness bug.

## Table of contents
1. Transport shootout
2. Head-of-line blocking
3. Control plane vs data plane
4. Push / invalidation channel (architectural core)
5. Backpressure and flow control
6. Connection lifecycle
7. Go library landscape
8. Auth on long-lived connections
9. Recommendation
10. Pitfall list

---
