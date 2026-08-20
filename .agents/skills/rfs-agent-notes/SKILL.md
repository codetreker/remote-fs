---
name: rfs-agent-notes
description: Use when adding, reviewing, moving, or pruning Agent Notes under .agents/notes — writing a new proposal, moving one to implemented or rejected, checking whether a new note supersedes an older one, or deciding whether a rejected note still earns its place.
---

# Working with Agent Notes

An Agent Note records a decision or proposal — **why it was made and what was given up**. [`.agents/notes/README.md`](../../notes/README.md) owns the layout, the format, and the lifecycle rules; read it first. This skill is the workflow around them. It is guidance, not a script.

## Before writing a new note

**Every new note triggers a supersession check.** Search the tree for an existing note covering the same decision or mechanism:

```
rg --hidden -l '<the mechanism>' .agents/notes/
```

Then classify what you find:

- **The existing note owns this decision** → update it. Do not create a second note about the same thing; two notes about one decision means one of them is silently wrong.
- **This decision replaces the existing one** → write a new note and cross-link both. **Never edit an existing note into its opposite** — that erases why the earlier position was once reasonable, which is the one thing notes exist to preserve.
- **This decision partially supersedes** → keep both, cross-linked, and update every fact in the old note that remains current.

## Writing the note

Follow the skeleton in the README. Two sections carry the weight:

**`## 问题`** must stand without the solution. If it reads as "we need X because X is good", it is not a problem statement. A reader should be able to disagree with the proposal while agreeing with the problem.

**`## 备选方案`** is mandatory and is the section most often faked. Rules:

- **Record, never invent.** If no alternative was genuinely considered, write that. A fabricated straw alternative is worse than an absent section — it makes a decision look tested when it was not.
- Each alternative gets its own bold-led paragraph: what it is, and **why it lost**.
- "Simpler" and "cleaner" are not reasons. Name the concrete cost or the concrete failure.
- An alternative that was not chosen *for now* but remains open should say so, and say what would decide it.

## Moving between lifecycles

Moving the file is not enough. The `Status:` line and the section skeleton must both change in the same edit.

**`proposed/` → `implemented/`:**
- `## 提案` becomes a present-tense `## 决定` describing what exists.
- `## 验收标准` and `## 风险` fold into `## 后果` — what the trade-off cost *and* bought.
- Plans, migration steps, and open questions are deleted, not archived in place.
- Anything that turned out differently from the proposal is stated as it shipped, not as a correction of the proposal.

**`proposed/` → `rejected/`:**
- Add the reason to the `Status:` line, in one line.
- Change nothing else. A rejected note is the proposal frozen — its value is the reasoning that was current when it was declined.

## Keeping implemented notes current

An implemented note describes reality. When the code later moves a file, renames a package, or changes a default, **update the note in the same change** — facts only, never the decision.

If the decision itself changes, that is a new note (see the supersession rules above).

## Pruning

**A rejected note earns its place only while its rationale prevents a tempting, meaningful mistake.** When the idea has stopped being tempting — the surrounding design moved, or nobody would propose it now — delete the file. A rejected-notes graveyard nobody reads costs attention on every search.

**An implemented note may be deleted only when it is fully superseded** and the owning note has absorbed every unique rationale, alternative, consequence, and named gap, and every inbound link is repaired. Partial supersession does not qualify — keep both, cross-linked.

Never delete a note on the grounds that git history has it. Git history is not a place anyone looks for rationale.

**Never archive a proposed note.** An obsolete proposal is rejected, with the reason on the status line.

## Checks worth running

```
# Status line agrees with the directory
rg -n '^Status: ' .agents/notes/*/*.md

# Relative links resolve
rg -o '\]\(\.\./[^)]*\.md\)' .agents/notes/*/*.md

# Every note has the mandatory sections
rg -L -n '^## 备选方案' .agents/notes/*/*.md
```
