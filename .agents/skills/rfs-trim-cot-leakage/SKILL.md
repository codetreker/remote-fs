---
name: rfs-trim-cot-leakage
description: Use when auditing or fixing prose that reads like a leaked reasoning transcript — citations to uncommitted drafts or review artifacts (§N of a design doc, "audit C2", "decision 7"), change narration ("used to", "no longer", "this round"), review vantage ("rejected in review", "the reviewer said"), reviewer-addressed justification, control-flow narration, or hedged planning residue, in docs, Agent Notes, comments, or commit messages.
---

# Trimming Chain-of-Thought Leakage

Chain-of-thought leakage is prose whose vantage is **the authoring session** rather than **the repository**: it cites artifacts only that session could see, narrates the change instead of the state, or argues with a reviewer who has left.

The fix is rarely deletion alone. When a passage carries factual clauses, restate each so it stands at HEAD, then delete the transcript around it. A passage carrying no factual clause — an audit code, control-flow narration — is deleted outright.

Required background: [rfs-prose-standard](../rfs-prose-standard/SKILL.md) owns the complete-proposition rule this skill applies. This is guidance, not a script.

## The one test

For every suspect passage ask:

> **Could a reader at HEAD, with no access to any session transcript, review thread, or uncommitted draft, resolve every reference and verify every claim?**

If no — restate the surviving facts from the repository's vantage and delete the rest.

If yes, it is not leakage, however historical it sounds. But resolvability only clears *this* skill's bar. On current-state surfaces (`docs/design/`, READMEs, code comments) a resolvable change story is still change narration, and class 3 below routes it to its sanctioned home.

## Taxonomy

1. **Dead design-session citations** — `(decision 7)`, `(audit C2)`, `design §4.7`, `plan §1.4`, phase labels (`T4`, `P-I`), "the review found". If the decision has a committed owner, cite it by name and relative path; otherwise delete the citation and restate its factual clause so it stands alone.
2. **Review and change vantage** — "this change adds", "the previous version", "a later change will". State the shipped mechanism or the extension point. Deferred work goes to an Agent Note in `proposed/`, a `TODO` marker, or an issue — not to a sentence about the future.
3. **Change narration and version stamps** — "used to", "no longer", "the old X", and indexical stamps ("本期", "this round", "now" contrasting with a past state). State the present behavior. A fixed defect becomes a present-tense counterfactual ("without X, Y happens"), never repo history ("used to Y").
4. **Review choreography** — "rejected in review", "the reviewer confirmed", draft ordinals ("v2 of this note"). Keep the surviving decision and its rationale as plain fact; delete who said it when.
5. **Reviewer-addressed justification** — "this is safe because it simply…", "note that this is correct". A comment arguing its own correctness addresses a reviewer, not a maintainer. State the invariant that makes it safe, or delete the comment if the code shows it.
6. **Restatement and derivation transcripts** — control-flow narration ("first we X, then we Y"), walkthroughs, proofs of obvious branches. Delete; keep only a non-obvious contract or invariant.
7. **Hedges and planning residue** — "probably fine for now", "should be enough", deferrals with no marker. Promote to a `TODO`, restate as the actual bound, or delete the hedge.
8. **Working-language slips** — an English fragment stranded in a Chinese document, or the reverse. Per the repository language rule, `docs/spec/`, `docs/design/`, and `.agents/notes/` are Chinese; `docs/research/`, code, comments, commit messages, and skills are English. Technical terms may stay in English inside Chinese prose; whole clauses may not.

## What is not leakage

Unaided pattern-matching fails in both directions — deleting durable references while keeping dead ones. Apply these keeps as written:

- **Issue and merged-PR references** — they resolve at HEAD. Keep them on any surface.
- **Citations between committed documents** — a relative link into `docs/design/` or another Agent Note is durable. The §-ban covers *uncommitted drafts*, not committed documents that own their numbering.
- **External standards references** — `RFC 9110 §10.1.5`, a kernel source permalink pinned to a commit. These resolve outside the repository by design.
- **Counterfactual-present pins** — "without the version check, two concurrent commits both succeed". Present tense, states a real property.
- **Measured bounds** — "(measured: 1000 entries ≈ 31 ms at 20 ms RTT)". The provenance word "measured" is load-bearing; keep it.
- **Runtime old/new states** — "the previous connection drains before the new one accepts" is runtime lifecycle, not change history.
- **An Agent Note's `## 备选方案` section** — recording what a decision beat is the genre's purpose, not leakage. Likewise a note's own historical framing of *why* a decision was made.
- **Project voice** — "we" as the authoring collective.

## Where change stories are allowed to live

Class 2 and 3 do not mean the change story is forbidden; they mean it has one home:

| Surface | Change story? |
|---|---|
| `docs/design/`, READMEs, code comments | **No.** Present tense, current state only. |
| `.agents/notes/` | **Yes** — that is what a note is for. Rationale, alternatives, what was given up. |
| Commit messages | **Yes.** |

## Workflow

1. Require an explicit scope. Do not infer a repository-wide scope.
2. Audit read-only first. Grep for the patterns below with `--hidden` so `.agents/` is searched, then judge every hit semantically — the patterns are probes, not the definition. Also read the densest prose in scope without a pattern in hand; every real purge finds cases no pattern caught.
3. Before deleting anything, enumerate the passage's propositions and check the overcorrection traps below.
4. Report the inspected scope, the changes made, the deliberate keeps, and anything deferred.

### Recall probes

```
rg --hidden -n '§[0-9]|\(decision [0-9]|audit [A-Z][0-9]|see the (design|plan|review)'
rg --hidden -n '以前|原来是|不再|曾经|本期|这一轮|used to|no longer|previously|this round'
rg --hidden -n 'reviewer|review 指出|评审指出|rejected in review|v[0-9]+ of this'
rg --hidden -n '暂时|大概|应该够|probably|should be enough|for now' 
rg --hidden -n 'first we|then we|接下来我们|然后我们'
```

### Overcorrection traps

A trim is wrong, not merely aggressive, when it:

- flips an obligation into an endorsement ("must not" → "does not");
- promotes a hypothetical or proposed thing to a shipped fact;
- deletes a true fact along with the transcript around it;
- drops provenance that made a number trustworthy ("measured", "per the kernel source").
