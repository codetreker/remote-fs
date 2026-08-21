# AGENTS.md

Instructions for coding agents working in this repository.

## What this is

`github.com/codetreker/remote-fs` — a Go library that exposes a remote namespace
as a local filesystem, plus a reference server, daemon, and CLI built on it.

**Read `docs/spec/` before doing anything.** It defines what the system is for
and what it must guarantee. `docs/spec/requirements.md` is the authoritative
list; everything else is downstream of it.

## Where things live

| Path | Contents | Rules |
|---|---|---|
| `docs/spec/` | What we are building and why. | [`docs/AGENTS.md`](docs/AGENTS.md) |
| `docs/design/` | Architecture — how. | [`docs/AGENTS.md`](docs/AGENTS.md) |
| `docs/research/` | Investigation reports with citations. | [`docs/AGENTS.md`](docs/AGENTS.md) |
| `.agents/notes/` | **Why** a decision was made, what it beat, what it gave up. | [`.agents/notes/AGENTS.md`](.agents/notes/AGENTS.md) |

Those two files own their directories' rules; do not restate them here.

One rule that belongs nowhere else because it applies everywhere: **rewrite
documents, do not append corrections.** A paragraph that contradicts what is
above it is worse than the error it was trying to fix.

## Workflow

Work enters through `docs/spec/requirements.md`. If the requirement is not there,
add it there first — nothing downstream may introduce a requirement the spec
lacks.

1. **Is the change non-trivial?** It is, when it alters behavior, architecture, a
   contract shared across packages, process or tooling, testing strategy, an
   on-disk or wire format, or any decision a maintainer may reasonably revisit.
   Purely mechanical or local edits are exempt from everything below.
2. **Now, or later?** Substantial work not being done now becomes an Agent Note
   in `proposed/`. Work being done now **skips `proposed/` entirely**.
3. **Cut the scope** with [`rfs-scope-cut`](.agents/skills/rfs-scope-cut/SKILL.md):
   what is deferred, what each deferral costs, and which deferrals must stay
   reversible.
4. **In the same change, always together:** the code, an `implemented/` Agent
   Note (or the `proposed/` one moved and rewritten per
   `.agents/notes/README.md`), the affected `docs/design/` sections, and the
   tests. [`docs/testing.md`](docs/testing.md) says what "the tests" means for a
   given change — including that error paths are not optional coverage.
5. **Supersession check.** Does the new note supersede an older one? Search the
   tree and resolve it in the same change. [`rfs-agent-notes`](.agents/skills/rfs-agent-notes/SKILL.md)
   owns the procedure.
6. **Before pushing**, run the checks that cover this diff — not the full suite.
   Report only commands actually run, with their output.

**`docs/design/` moves with the code**, in the same change. It does not lead by a
separate step and it does not lag behind.

## Two rules learned the hard way

**An architecture nobody has attacked is not ready to build against.** An
adversarial review of this design set — three independent reviewers, one pass —
found ten blocking defects, two of which were silent-staleness bugs that no
amount of careful writing had caught. Review the design before implementing
against it, not after.

**Never report success, or plausible data, when the truth is unknown.** This is a
filesystem: returning stale bytes, an empty directory, or "no such file" when the
real answer is "I could not reach the server" causes silent, irreversible data
loss in whatever runs on top. `requirements.md` R-ERR-1 and R-ERR-2 own this.
In review, a swallowed error or an invented fallback is a blocking defect, not a
style note.

The same applies to your own reporting: never claim a check passed without
having run it and read its output.

## Skills

`.agents/skills/` holds this repository's skills; `.claude/skills` is a symlink
to it so both toolchains find them.

| Skill | Use it when |
|---|---|
| `rfs-scope-cut` | Deciding what a piece of work contains and what is deferred. |
| `rfs-agent-notes` | Adding, moving, or pruning an Agent Note. |
| `rfs-prose-standard` | Writing, reviewing, or trimming any prose here. |
| `rfs-trim-cot-leakage` | Prose reads like a leaked reasoning transcript. |

## Working language

`docs/spec/`, `docs/design/`, and `.agents/notes/` are written in Chinese,
README files included. They are read and argued over repeatedly, and read more
naturally that way.

`docs/testing.md` is Chinese for the same reason.

`docs/research/`, `.agents/skills/`, code, comments, and commit messages are in
English.

---

## Rules

### All changes go through a worktree (hard rule)

- **Always** make any code, config, or doc change in a dedicated worktree under `.worktrees/` — even a tiny single-file bug fix or a throwaway experiment.
- **Never** edit the primary working tree (the repo's current checkout) directly — it must stay clean so you can switch branches / pull main anytime.
- **One task = one worktree = one branch = one PR.** Implementation, tests, doc sync, and acceptance state all land in that single PR.
  - Work that genuinely **depends on an unmerged PR** stacks on top of that PR's branch instead of waiting for it or bundling into it.
- Already edited the primary tree? **Move** those changes into a worktree before continuing.

### The main context coordinates; it delegates context-heavy work

The main context does what **needs a global view but doesn't burn context** — driving the workflow, deciding gates, draft architecture, breaking down tasks, feeding each subagent the context it needs, reviewing, merging, and synthesizing results. It preserves its own context by handing off everything that would consume a lot of it.

- **Delegate context-heavy work to worker subagents** — deep research, coding, detailed verification, and broad git / GitHub operations (commit, push, opening PRs, checking CI gates, merging, cleaning up worktrees / branches). Only simple orientation queries (e.g. `git status`, `git log --oneline -5`) stay inline, when they keep the main context oriented without derailing it.

### Writing large files (hard rule)

- **Write large files in chunks.** When creating or heavily editing a large file, write an initial slice, then **append** the rest with follow-up edits — **never** emit the whole file in one tool call. One oversized write can time out and waste the turn.

### Use Subagent Driven Development

- Figure out the task clearly.
- Breakdown into tasks and resolve the dependencies.
- Spawn worker subagents in parallel where the dependencies allow it.
- Review independently.
