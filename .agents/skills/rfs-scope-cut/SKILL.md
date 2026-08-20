---
name: rfs-scope-cut
description: Use when deciding what a piece of work contains and what is deferred — scoping a first version, trimming an over-large change, answering "can we ship without X", or reviewing a scope decision someone else made. Produces a defensible cut plus the constraints that keep each deferral reversible.
---

# Cutting Scope

Every scope decision is a bet that the deferred thing stays cheap. This skill is about telling apart the bets that are cheap from the ones that only look cheap.

It is guidance, not a script. The output is a decision recorded as an Agent Note, so read [`rfs-agent-notes`](../rfs-agent-notes/SKILL.md) for the format.

## The one question

For every candidate, ask:

> **If we defer this, is the cost "add it later" or "redo it later"?**

That question sorts everything into three classes, and the third is the one that gets lost.

| Class | Deferring costs | Decision |
|---|---|---|
| **Feature** | Add it later | Defer freely |
| **Guarantee** | Redo it later | Do it now, or accept a rewrite |
| **Shape** | Nothing now — *if* you do not choose a form that forecloses it | Do not build it; **do** constrain the form |

## Why "shape" needs its own class

A feature is visibly absent. A guarantee is visibly absent. **A foreclosed shape is invisible** — the system looks complete, and the cost only appears much later, as "we have to redo the write path to support that".

That invisibility is exactly why it gets sacrificed under the banner of simplification: cutting it appears to cost nothing today.

So a scope cut has three outputs, not two:

1. what is **in**;
2. what is **out**, each with the cost of deferring it;
3. what is **out but constrained** — the form the implementation may not choose.

A scope document with only the first two is incomplete.

## Telling a guarantee from a feature

The test is not importance; it is **where the thing lives**.

- A **feature** is code you add at a call site. Symbolic links, a second transport, a batch operation.
- A **guarantee** is a property of the structure. It holds because of how components are arranged and what they promise each other, so adding it means rearranging them.

Practical probes for "this is a guarantee, not a feature":

- Does it constrain what **every** component may do, rather than adding a new one? (Error honesty, ordering, atomicity.)
- Would adding it later require touching call sites that are unrelated to it?
- Is its absence the **default behavior**? Defaults are structural — "returns empty on error" is what code does when nobody decided otherwise, and reversing it means auditing every path.
- Does anything downstream already assume it?

If two or more are yes, deferring it costs a rewrite.

## Telling a shape constraint from a guarantee

Both feel like "we must handle this now". The difference:

- A **guarantee** must be *true* now.
- A **shape constraint** need not be true now, but the form chosen now must **leave room** for it.

Write a shape constraint as a prohibition on the implementation, not as a behavior:

> *"Renaming must remain atomically implementable"* — not *"rename is atomic"*.
> *"Credentials are a refreshable element of the protocol vocabulary, not a handshake property"* — not *"credentials can be rotated"*.

If you cannot phrase it as a prohibition on form, it is probably a guarantee wearing a disguise.

## Procedure

1. **List the candidates from the requirements**, by ID. Do not invent scope; if something is missing, it goes in the requirements first.
2. **Classify each** with the one question. Record the classification, not just the verdict — the classification is what a later reader will challenge.
3. **For every deferral, write its cost.** "Low" is a claim; state what makes it low. A deferral whose cost you cannot state is one you have not thought about.
4. **For every deferral that is cheap only under a condition**, promote the condition to a shape constraint and phrase it as a prohibition.
5. **Write the note.** `## 备选方案` must contain the other cuts you actually considered — a smaller one and a larger one at minimum, with what each would have bought.
6. **State what the cut does not buy.** A scope decision that only lists wins is a sales pitch.

## Anti-patterns

**Deferring a guarantee because it is small.** Size is irrelevant; position is everything. A one-line version check placed inside the storage write is cheap; the same check retrofitted after the write path exists is a redesign.

**"Simplify" quietly deleting a shape constraint.** Constraints look like unnecessary weight because they constrain something you are not building. Whoever removes one must state which deferral they are making irreversible.

**Deferring by pointing at a document that does not say so.** A deferral cited to a scope decision nobody made reads as resolved and is not. If the deferral is real, record it where scope decisions live.

**"Not well supported" as a behavior.** Every deferral must have an observable outcome. If a case is out of scope, say what happens when someone hits it — an error, a limit, a slow path. "Not well supported" is not implementable and not testable.

**Sequencing dressed as scope.** "We'll do it in phase 2" is not a scope decision unless phase 2 exists. Either it is deferred with a cost, or it is in.
