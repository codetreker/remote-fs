---
name: rfs-prose-standard
description: Use when writing, reviewing, trimming, or auditing prose in this repository — documents under docs/, Agent Notes, code comments, commit messages, diagnostics, and CLI strings — including deciding whether a passage is required at all.
---

# Prose Standard

Write enough to preserve the contract, then remove reasoning transcripts, repetition, and decoration.

A **contract** is an obligation, invariant, precondition, postcondition, or compatibility promise that a caller, callee, implementer, producer, or consumer relies on.

This skill owns editorial judgment and required coverage. Use [rfs-trim-cot-leakage](../rfs-trim-cot-leakage/SKILL.md) for hunting reasoning-transcript leakage specifically. It is guidance, not a script.

## Preserve the complete proposition

Before editing, identify every proposition in the passage. Preserve each relevant:

- actor and action;
- condition, timing, and ordering;
- modality — must, may, never;
- negative guarantee and exception;
- ownership, side effect, failure mode, consequence.

Remove adjectives, repetition, and narration **only when every factual clause survives and the result is clearer**. A smaller word count alone is not an improvement.

Keep a complete local contract at the point of use: the behavior, failure, ownership, and consequence a reader needs *there*. Link aggressively to the owning document for architecture, rationale, algorithms, history, or extended examples. **One explanation has one home**; essential contract facts may repeat locally.

Keep non-obvious rationale when omitting it could plausibly cause misuse or an incorrect simplification. Otherwise state the consequence and link the rationale home.

## This is not a one-way shortening pass

Add or restore prose when the code, the types, and the structure do not communicate a required contract. Do not add a comment when those facts are already obvious locally.

| Surface | What must be there |
|---|---|
| `docs/spec/` | What the system guarantees, to whom, and what it refuses. Never a mechanism. |
| `docs/design/` | Structure, boundary contracts, data flows, and what the structure costs. Present tense. No deliberation. |
| `.agents/notes/` | Unique rationale, mechanisms, alternatives, consequences, named gaps. Implemented notes state shipped reality in present tense. |
| `docs/research/` | The evidence chain with resolvable citations. Frozen once written. |
| Exported identifiers | Caller-visible return distinctions, errors, side effects, ownership, timing, cancellation, durability. |
| Internal comments | Non-local structure, invariants, ordering hazards, ownership, security boundaries, surprising failure behavior. **Not** control-flow narration or code restatement. |
| Package comments | The package's role, its dependencies, its responsibilities, and non-obvious structural choices — linked to the document that owns each choice. |
| Tests | Only non-obvious test design: why this fixture, why this indirect observation, why this platform accommodation. Not walkthroughs. |
| Diagnostics and error strings | The failing subject, the violated rule, and the correction when it is non-obvious. Not internal execution narration. |

Preserve searchable mechanism names, and modal, temporal, or negative emphasis that carries meaning. Normalize decorative emphasis only.

## Terms to check before using

`contract`, `boundary`, `shape`, `surface`, `seam`, `vocabulary`, `invariant` — these are terms to check, not banned words. First ask whether the exact rule, operation, field set, failure state, timing point, or component split states the fact better.

Keep the term when it names the exact technical subject. "The storage contract obliges the implementer to…" is precise. "This improves the architectural boundary" is decoration.

## Comments

Comments describe non-obvious contracts or rationale that code cannot express. They do not restate what the code already implies.

Write as the author of the system — first person plural, or the passive voice. Never mention users, agents, reviews, or sessions.

Never cite an uncommitted document. If the rationale has no committed home, either commit one or state the rationale inline.

## Workflow

1. Confirm the scope. Require it explicitly; do not infer a repository-wide scope.
2. Read the governing document before judging a passage — the relevant `README.md` under `docs/`, and `AGENTS.md`.
3. Inspect the whole requested scope, not only the largest files. Use searches to find candidates, then judge semantically.
4. Classify each candidate: keep, add, trim, restore, restructure, defer. Apply changes only when the task authorizes edits. **Do not manufacture edits to satisfy a deletion target.**
5. Fix the owning document before any document that derives from it. After learning a rule, re-check analogous passages.
6. Report the inspected scope, the changes, the deliberate keeps, and what was deferred.

## Borderline decisions

A case is borderline only when **at least two versions satisfy the complete-proposition rule** and they trade accepted principles against each other. A rewrite with one proposition-preserving answer is not borderline — it has an answer.

When a case is genuinely borderline, present two or three viable versions, recommend one, and state the factual or structural difference between them. Do not offer inferior distractors, and do not weaken a proposition to make progress.
