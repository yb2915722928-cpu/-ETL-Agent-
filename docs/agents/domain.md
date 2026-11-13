# Domain Docs

## Layout

This repo uses **single-context**: one shared `GLOSSARY.md` at the repo
root, with architecture decisions in `docs/adr/`.

## Before exploring

- Read the root `GLOSSARY.md` when it exists.
- Read ADRs in `docs/adr/` that concern the area being explored.
- If these documents do not exist, proceed silently. Create them lazily
  through `/domain-modeling` when terms or decisions are actually resolved.

## Use the glossary's vocabulary

Use the terms defined in `GLOSSARY.md` when naming domain concepts in
issues, proposals, hypotheses and tests. Follow its preferred terms.
If a needed concept is missing, reconsider the wording or note a real gap
for `/domain-modeling`.

## Flag ADR conflicts

Explicitly surface any contradiction with an existing ADR, including the
ADR reference and the reason to reconsider it.
