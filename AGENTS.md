# AGENTS.md

Instructions for coding agents working in this repository.

## What this is

`github.com/codetreker/remote-fs` — a Go library that exposes a remote namespace
as a local filesystem, plus a reference server, daemon, and CLI built on it.

**Read `docs/spec/` before doing anything.** It defines what the system is for
and what it must guarantee. `docs/spec/requirements.md` is the authoritative
list; everything else is downstream of it.

## Documentation layout

| Path | Contents |
|---|---|
| `docs/spec/` | What we are building and why. Never how. |
| `docs/design/` | Architecture and design — how. |
| `docs/research/` | Investigation reports with citations. Historical; not updated once written. |

Three rules that are not derivable from looking around:

- **`spec` never contains mechanism.** The line is obligation versus mechanism —
  what the system guarantees, and what a third party must guarantee for it to
  work, are spec; the machinery either side uses is design. `docs/spec/README.md`
  gives the tests and worked examples.
- **`docs/spec/problem.md` and `requirements.md` are living**; rewrite them in
  place. **`docs/spec/iterations/YYYY-MM-DD-*.md` are frozen** once agreed —
  change of scope means a new iteration, not an edit to an old one. Iteration
  documents cite requirement IDs and never introduce a requirement that
  `requirements.md` lacks; if one is missing, add it there first.
- **Rewrite documents; do not append corrections.** A paragraph that contradicts
  what is above it is worse than the error it was trying to fix.

## Working language

`docs/spec/` and `docs/design/` are written in Chinese, README files included.
They are read and argued over repeatedly, and read more naturally that way.

`docs/research/`, code, comments, and commit messages are in English.
