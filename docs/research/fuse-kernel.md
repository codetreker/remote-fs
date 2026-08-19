# FUSE kernel semantics for a high-latency remote-backed Go daemon

Research record. Investigation only; no code was written into the project.

## Sources of record

All kernel line references below are against a local sparse checkout of Linus'
tree pinned at:

- commit `0f23d56f17fdfc7db69d51f64c8b91bbab947aa9` (2026-08-17), i.e. **Linux
  7.2 / 7.3-rc1 merge window**, `VERSION = 7 / PATCHLEVEL = 2` in the top-level
  `Makefile`.
- `include/uapi/linux/fuse.h` declares `FUSE_KERNEL_VERSION 7`,
  `FUSE_KERNEL_MINOR_VERSION 45`.

libfuse references are against `libfuse/libfuse` master at commit
`1f28a174efd4c5045d40fb93485b15763f8af457` (2026-08-13).

Permalink form used throughout:
`https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/<path>?id=<sha>`
and `https://github.com/libfuse/libfuse/blob/<sha>/<path>`.

Because the kernel HEAD moves, where a behaviour is old and stable I also cite
the original commit that introduced it, which is stable forever.

---

## 1. The kernel's FUSE caching model

### 1.1 `entry_timeout` / `attr_timeout` are strictly per-response

**Verdict: your health-based variable-TTL scheme is architecturally sound. The
kernel stores the timeout as an absolute deadline recomputed from every single
reply; there is no per-mount or per-connection TTL anywhere in the code path.**

The conversion is `fuse_time_to_jiffies()` in `fs/fuse/dir.c`:

```c
u64 fuse_time_to_jiffies(u64 sec, u32 nsec)
{
	if (sec || nsec) {
		struct timespec64 ts = { sec, min_t(u32, nsec, NSEC_PER_SEC - 1) };
		return get_jiffies_64() + timespec64_to_jiffies(&ts);
	} else
		return 0;
}
```

Note the `else return 0` — a zero timeout is stored as the *absolute jiffies
value 0*, which is always `time_before64(0, get_jiffies_64())`, i.e. permanently
expired. It is not a sentinel with special handling; it just falls out of the
same comparison.

- Entry timeout lands in `dentry->d_fsdata->time` via
  `fuse_change_entry_timeout()` (`fs/fuse/dir.c:297`). Every reply carrying a
  `struct fuse_entry_out` calls it: `fuse_lookup` (`dir.c:644`),
  `fuse_dentry_revalidate` (`dir.c:451`), `fuse_create_open` (`dir.c:902`),
  `fuse_mknod`/`mkdir`/`symlink`/`link` via `create_new_entry` (`dir.c:1032`,
  `dir.c:1035`), and readdirplus (`readdir.c`).
- Attr timeout lands in `fuse_inode->i_time` via
  `fuse_change_attributes_common()` (`fs/fuse/inode.c:235`, `fi->i_time =
  attr_valid`), where `attr_valid` is `ATTR_TIMEOUT(o)` =
  `fuse_time_to_jiffies(o->attr_valid, o->attr_valid_nsec)` (`fuse_i.h:1052`).

Each is an unconditional overwrite with the value derived from *that* reply.
Consequences that matter for a health-driven scheme:

1. **Raising the TTL takes effect immediately** on the next reply for that
   object; you do not need to wait out the old TTL.
2. **Lowering the TTL to 0 does not retroactively expire already-cached
   objects.** If you served `entry_timeout=60` and the change stream then dies
   at t+1s, those dentries stay valid until t+60s. Serving `timeout=0` on
   *subsequent* replies does nothing for entries nobody re-looks-up. If you
   want the drop to be effective you must **actively invalidate** on the
   transition to unhealthy — see §1.4, and in particular
   `FUSE_NOTIFY_INC_EPOCH`, which is exactly the "invalidate everything now"
   primitive and is O(1) on the wire.
3. Very large TTLs are safe, but there is no "infinite". libfuse clamps in
   `calc_timeout_sec()` (`lib/fuse_lowlevel.c:471`, `t > ULONG_MAX → ULONG_MAX`)
   and the kernel clamps again in `__timespec64_to_jiffies()`
   (`sec >= MAX_SEC_IN_JIFFIES → sec = MAX_SEC_IN_JIFFIES`,
   `include/linux/jiffies.h:422-426`). On 64-bit `MAX_SEC_IN_JIFFIES` is ~10^11
   seconds, so nothing overflows and "huge" really does mean "never expires in
   practice". Production filesystems conventionally use values around `86400.0`
   (1 day) rather than the maximum, so that a bug in the invalidation path
   self-heals within a day instead of never.

### 1.2 What `timeout = 0` costs, per syscall

With `entry_timeout = attr_timeout = 0`:

- `->d_revalidate` is called on every path component of every path resolution.
  `fuse_dentry_revalidate` (`dir.c:383`) sees the expired time and issues a
  fresh `FUSE_LOOKUP` for that component — **one round trip per component per
  syscall**. `/mnt/a/b/c/file` is 4 lookups (`a`, `b`, `c`, `file`); the mount
  root itself is not revalidated.
- Additionally, `LOOKUP_RCU` mode is abandoned: `fuse_dentry_revalidate`
  returns `-ECHILD` when it must talk to the daemon while in RCU-walk
  (`dir.c:410`), forcing the VFS to restart the whole resolution in ref-walk.
  So the cost is not just the round trips, it is also a full path-walk restart.
- `stat()` then adds a `FUSE_GETATTR` because `fuse_update_get_attr` sees
  `fi->i_time` expired.

So an uncached `stat("/mnt/a/b/c/file")` = 4 LOOKUP + 1 GETATTR = **5 serialized
round trips**. Path resolution is inherently serial — component *n+1* cannot be
looked up until *n* returns a nodeid — so no amount of client concurrency
compresses it.

### 1.3 Negative dentry caching — and the trap that silently disables it

**This is the single highest-leverage knob for `PATH` walks, Python `import`,
linker `-L` search, and `./configure`, and it is easy to get wrong.**

There are two ways for a FUSE server to say "no such file", and they behave
completely differently:

| Server reply | Kernel behaviour |
|---|---|
| `fuse_reply_err(req, ENOENT)` | `fuse_lookup_name()` returns `-ENOENT` → `fuse_lookup()` sets `outarg_valid = false` → `fuse_invalidate_entry_cache(entry)` → **`fuse_dentry_settime(entry, 0)` — the negative dentry is created but immediately expired. Every future access re-round-trips.** |
| `fuse_reply_entry()` with `e.ino == 0` and `e.entry_timeout = T` | `fuse_lookup_name()` hits `/* Zero nodeid is same as -ENOENT, but with valid timeout */` and returns 0 with `*inode == NULL` → `outarg_valid` stays true → `fuse_change_entry_timeout(entry, &outarg)` → **negative dentry cached for T seconds.** |

Source: `fs/fuse/dir.c:576-578` (`if (err || !outarg->nodeid) goto
out_put_forget;`) and `fs/fuse/dir.c:634-650`. libfuse documents this on
`struct fuse_entry_param::ino`:

> In lookup, zero means negative entry (from version 2.5). Returning ENOENT
> also means negative entry, but by setting zero ino the kernel may cache
> negative entries for entry_timeout seconds.

(`include/fuse_lowlevel.h:63-72` at libfuse `1f28a174`.)

Once cached, `fuse_dentry_revalidate` returns 1 without entering the round-trip
block at all, because the block is gated on `time_before64(fuse_dentry_time(entry),
get_jiffies_64())`. Only when the timeout *has* expired does the
`/* For negative dentries, always do a fresh lookup */` short-circuit
(`dir.c:404-406`) kick in, dropping the dentry so the VFS redoes `->lookup`.

Magnitude of the saving:

- `python -c 'import requests'` on a stock CPython does on the order of
  10^2-10^3 failed `stat`/`open` probes across `sys.path` entries (each path
  entry × each of `.cpython-3x.so`, `.abi3.so`, `.so`, `.py`, `.pyc`,
  `/__init__.py`, ...). Python's `importlib` mitigates this with a per-directory
  listing cache keyed on the directory's mtime, so the probes become `listdir`
  rather than `stat` — but only for directories it has already listed, and the
  cache is invalidated whenever the directory mtime changes.
- A shell `PATH` with N entries costs N-1 negative lookups for every command
  not in the first entry.
- `ld` with M `-L` paths × K libraries costs up to M×K negative probes
  (`lib<k>.so`, `lib<k>.a` each).
- `./configure` is dominated by "does header X exist" probes.

At 20 ms RTT, 500 negative probes = **10 s** uncached, ~0 s cached. This is not
a marginal optimisation.

**Caveat that constrains the design:** a cached negative dentry masks a file
another client creates. That is exactly why the `Created` event must invalidate
the entry — see §1.4 and the verdict in §7.
