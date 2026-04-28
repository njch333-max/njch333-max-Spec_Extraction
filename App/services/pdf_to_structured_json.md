# `pdf_to_structured_json.py` — vendored copy provenance and divergence

This document records why `App/services/pdf_to_structured_json.py` is a **fork** of the sibling repo's v6 PDF extractor, what diverges, and why the divergence cannot be moved out into a separate rules module.

Last updated: 2026-04-26.

---

## Two copies of this file exist

| Location | Role | Modifiable? |
|---|---|---|
| `C:\Users\Jason Niu - XM\code\claude-spec-extraction\pdf_to_structured_json.py` | **Authoritative v6 raw parser** in the sibling repo `njch333-max/claude-spec-extraction`. Git-tagged `v6`. | ❌ No (per memory `project_claude_spec_extraction.md` red line) |
| `App/services/pdf_to_structured_json.py` (this file) | **Vendored fork** with downstream PDF-geometry rule patches applied directly. Subprocess-invoked by `App/services/imperial_v6_adapter.py`. | ✅ Yes (this is where downstream rule patches live) |

The two copies are **not byte-identical**. Use `wc -l` and `git log -- App/services/pdf_to_structured_json.py` to see current divergence; do not rely on any line-count number cached in this doc, as it drifts every time a new patch lands.

---

## What's different and why

The vendored copy carries **PDF-geometry rule patches** that apply to v6 raw parser output but cannot live in a separate downstream module. As of 2026-04-26, two such patches exist:

- **Bug B fix** (commit `f8c4759`, `Fix Bug B: split v6 missing-separator rows`)
  - Splits a single record into N records when the source PDF was missing a horizontal row separator and pdfplumber merged adjacent rows into one cell. Triggered by AREA having ≥3 non-empty lines and SPECS having matching line count.

- **Bug C v2 fix** (commit `31b0d5c`, `Fix Bug C v2: supplier-count as grouping signal, wrap vs multi-row disambiguation`)
  - Coalesces SPECS lines back to supplier-count when the row is a single-area, multi-supplier wrap (so Bug B's missing-separator detector does not falsely fire on a wrapped row).
  - Also relaxes the cell newline cleanup from `re.sub(r"\n{2,}", "\n", cell)` to `re.sub(r"\n{3,}", "\n\n", cell)` so that `\n\n` paragraph signals survive into the disambiguation layer.

These two patches are **inter-locked**: Bug C v2 must run before Bug B's detector, otherwise wrapped rows get falsely split into N rows that don't exist in the source PDF.

---

## Why these patches cannot live in a separate rules module

The repo-level convention (memory `project_claude_spec_extraction.md`) is:

> "PDF-geometry limits (column truncation, missing dividers, row gluing <25px) are explicitly **not** parser bugs — they belong in the downstream rules layer."

By that convention, both Bug B and Bug C v2 fixes should live in a separate module under `App/services/` that consumes the parser's output. But they don't. They are **inside** the vendored parser file. The reason is data-signal timing:

### The `\n\n` paragraph signal is the disambiguation key

Bug C v2 distinguishes two superficially similar cases by looking for `\n\n` in cell text:

| `\n\n` present in SPECS? | Case | Correct downstream action |
|---|---|---|
| Yes | Independent rows merged because PDF separator was too thin | Bug B splits into N records |
| No | Single row whose multi-supplier SPECS wrapped onto multiple visual lines | Bug C v2 coalesces SPECS back to supplier-count, Bug B does **not** fire |

### The signal is destroyed by cell cleanup inside the parser

The sibling-repo v6 parser's internal cell cleanup contains:

```python
cell = re.sub(r"\n{2,}", "\n", cell)
```

This collapses every run of two-or-more newlines into a single newline. After cleanup, `\n\n` is gone — and with it, the disambiguation signal Bug C v2 needs.

### A separate downstream rules module would receive cleaned-up records

If the patches lived in a separate module that consumed parser output, that module would only see post-cleanup records, where:

- `\n\n` is already collapsed to `\n`
- multi-row vs wrapped-row cases are already indistinguishable
- both cases look identical at the record-string level

Bug C v2's discrimination is therefore impossible from a separate module without changing the parser's output schema (e.g., adding `paragraph_breaks: [...]` metadata), which would itself require modifying parser code — i.e., touching the sibling repo.

### So the only viable insertion point is inside the parser

The vendored-fork solution inserts the patches directly between cell cleanup and record emission:

```
v6 parser cell extraction
   → cell cleanup  (modified: r"\n{3,}" → "\n\n", preserving \n\n)
   → _coalesce_single_area_multisupplier_specs(record)   ← Bug C v2 reads \n\n signal
   → _should_add_missing_row_separator_review_hint(...)   ← Bug B detector
   → _split_review_hint_record(...)                       ← Bug B split
   → record emission to caller
```

All four steps share the same parser-internal data flow. Moving any one step out of the parser breaks the chain.

That is what is meant by "tightly coupled to v6 parser cell extraction" — not literal variable name coupling, but **data-flow position coupling**: the patches must run inside the cell-extraction pipeline because the disambiguation signal exists only there.

---

## Maintenance implications

### When the sibling repo bumps v6 → v7

If Jason re-vendors the new sibling-repo extractor, the maintainer must **re-apply** the local patches on top, not blindly replace the file. Steps:

1. `diff` the old sibling-repo v6 against the new sibling-repo v7 to understand upstream changes.
2. Replace this vendored copy with v7.
3. Re-apply Bug B fix and Bug C v2 fix on top of v7. Verify `_coalesce_*` runs before `_split_*` and that the cell cleanup regex still preserves `\n\n`.
4. Run full pytest suite. Pay attention to the Bug B regression suite (`test_pdf_extractor_split.py` etc.) and Bug C v2 fixtures.
5. Update `Project_state.md` and `Arch.md` to reflect the new vendored version.

### When a new PDF-geometry bug is found

Decision tree:

- Is the bug fixable using **only** parser-output records (post-cleanup, post-emission)? → Write a separate rules module under `App/services/`. Do not touch this file.
- Does the bug require parser-internal signal (e.g., `\n\n`, raw cell coordinates, pre-cleanup state)? → Add another patch directly to this vendored fork, inserted at the appropriate point in the cell-extraction pipeline. Document it in this file.

### When the sibling repo gets the same fix upstream

If `claude-spec-extraction` v7 (or later) absorbs the equivalent of Bug B and/or Bug C v2 into its own parser, the local patches become redundant. Remove them from this vendored copy and re-vendor cleanly. Verify the upstream version's behavior against the existing fixtures before declaring parity.

---

## References

- Sibling repo memory: `project_claude_spec_extraction.md`
- v6 hold list memory: `project_v6_bugs_hold_list.md` (Bug H/I/L/M are different — they are upstream limits the vendored fork **cannot** patch)
- Bug B task spec: `docs/BUG_B_TASK.md`
- Bug C v2 task spec: `docs/BUG_C_V2_TASK.md`
- Sibling repo location: `C:\Users\Jason Niu - XM\code\claude-spec-extraction\`
- Adapter that subprocess-invokes this file: `App/services/imperial_v6_adapter.py:15-43`

---

## Anti-patterns (do not do)

- ❌ **Do not edit this file as if it were the authoritative parser.** Authoritative source is the sibling repo. This is a fork.
- ❌ **Do not bypass `\n\n` preservation.** If a future patch wants to re-collapse `\n{2,}` → `\n` here, it will silently regress Bug C v2's disambiguation.
- ❌ **Do not move Bug B / Bug C v2 patches into `App/services/parsing.py` or a new "rules" module without first solving the parser-output schema problem** (i.e., adding paragraph-break metadata to the parser output). Otherwise the patches lose their input signal and stop working.
- ❌ **Do not re-vendor the sibling repo file blindly.** Re-application of local patches must be deliberate, with diff review and full pytest verification.
