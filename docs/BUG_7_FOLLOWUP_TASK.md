# ⚠ STATUS: WITHDRAWN 2026-04-30 — DO NOT EXECUTE

**Withdrawn by**: Claude (reviewer), after Codex (executor) pushed back with PDF-truth evidence.

**Reason**: This task spec was based on the false premise that the EVOC447 PDF puts enum values (`Wall-mounted` / `Overmount` / etc) in the `Type` slot under mixer groups. The handoff at [docs/EVOCA_STRUCTURED_HANDOFF_2026-04-30.md:180](EVOCA_STRUCTURED_HANDOFF_2026-04-30.md#L180) (written 2026-04-30, predates this task) explicitly states the opposite: **"PDF truth for `Basin Mixer Type` is `Alder 54082 Brushed Nickel`."** Same convention applies to Kitchen Sink Mixer `Type` and Laundry Tub Mixer `Type`. The PDF designer used the literal label `Type` as the slot that carries the product/model string for mixer groups; the row labelled with the parent group name (`Basin Mixer`, `Sink Mixer`, `Tub Mixer`) is empty in the source. Bug 7's raw-text fallback restored those product strings into `Type` rows from the actual PDF — that is **source-faithful**, not corruption.

**What I missed during spot-check**: I did not grep the project handoff / `EVOCA_STRUCTURED_SCHEMA_v0.md` / PRD before raising the alarm. I formed an English-label heuristic ("Type field = enum") without checking the PDF-truth claim that was already codified in the docs/. This is exactly the failure mode warned against in memory `feedback_grep_docs_before_heuristic.md`. New memory `feedback_evoca_label_semantic_grep.md` captures the specific lesson for the Evoca parser.

**Real risk that remains** (not a parser bug, do NOT fix here): when Evoca eventually wires into `_finalize_evoca_rooms` / SnapshotPayload, the adapter layer must NOT treat mixer `Type` as enum/mounting and must NOT route those product strings into a canonical "mounting type" canonical field. Captured in [docs/EVOCA_TYPE_LABEL_SEMANTIC_NOTE.md](EVOCA_TYPE_LABEL_SEMANTIC_NOTE.md) as a future-adapter constraint.

**Bug 7 status**: closes clean (no Bug 7.1). Regression file `tmp/evoca_structured_bug7/` IS the correct output, including the three "Type = product" rows.

**Do not act on anything below this line.** The spec is preserved verbatim only as historical record of the misjudgement and the criteria that would have been wrong to apply.

---

# ~~Task — EVOCA-STRUCTURED-V0 Bug 7.1: raw-text fallback leaks parent anchor product into child slot labels~~ (WITHDRAWN)

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: master at HEAD post Bug 7 landing. `App/services/evoca_structured_extractor.py` includes `_apply_raw_text_fallback`, `_parse_raw_text_pairs_for_group`, `_next_raw_text_continuation_value`, `_label_token_specs`, `raw_text_fallback_groups` / `raw_text_fallback_pairs_filled` diagnostic counters. `tmp/evoca_structured_bug7/` contains the post-Bug 7 reference outputs (38148 EVOC447, EVOC467, EVOC473).

**Layer scope**:
- `App/services/evoca_structured_extractor.py` only. Surgical change to whichever of `_apply_raw_text_fallback`, `_parse_raw_text_pairs_for_group`, or `_rescue_group` resolves the defect at root.
- `tests/test_evoca_structured_extractor.py` — new regression cases.
- `tools/evoca_structured_export.py` — re-run only; no code change expected.

**Out of scope**:
- `extraction_service.py`, `_finalize_evoca_rooms`, `runtime.py`, `SnapshotPayload` — still standalone-spike phase.
- `tools/evoca_structured_extractor_claude.py` and `tools/evoca_structured_export_claude.py` — those are the Claude reference spike, do not touch.
- Sibling repo `code/claude-spec-extraction/`.
- Bug 8 ("Continuation" pseudo-label), Bug 9 (Powder cross-page anchor) — both still on hold per memory `project_evoca_structured_parser_bugs.md`.
- 4-doc sync (`AGENTS.md`/`Arch.md`/`PRD.md`/`Project_state.md`) — still standalone tool, not yet wired. **Skip 4-doc sync for this fix** unless Codex finds an existing standalone-parser entry that genuinely needs amendment; if skipped, note `4-doc sync skipped — standalone parser, not yet wired` in delivery item 6.
- Re-running signed-off jobs 61/62/64/67 (per memory `project_signoff_jobs_not_v6.md`).
- `tools/checkpoint.ps1` (per memory `feedback_checkpoint_ps1_boundary.md` — working tree has intentional untracked task docs and Claude reference tools).

---

## Context — read before editing

Bug 7 added a raw-text-fallback pass (pdfplumber `extract_words` based) that fills child rows whose line-strategy and text-grid-rescue both miss. The fix correctly recovered EVOC447 Kitchen Benchtops `Colour = "Statuario Zero"` and several Bath / Shower product fields, but it has a **false-positive class** that violates the project rule **"rescue 留空 > 填错"** (memory `feedback_rescue_empty_over_wrong.md`).

### Reproduction — three confirmed sites in `tmp/evoca_structured_bug7/38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.json`

All three groups share the same shape: parent label whose value-cell is empty in the table-extraction layer (so no `is_group_anchor` row exists in `rows`), child labels include the slot/enum-typed label `Type` first, then `Location`. The fallback pops a product string into `Type` instead of routing it to a parent-anchor row.

| JSON line | Room | Group | Wrongly-filled child | Wrong value (`source_method = pdfplumber_raw_text_fallback`) |
|---|---|---|---|---|
| 7167 | Kitchen | Sink Mixer | `Type` | `Vito Bertoni Alfie Pull-Out Sink Mixer in Brushed Nickel (85972LF)` |
| 7626 | Powder | Tub Mixer | `Type` | `Alder Soho Gooseneck Sink Mixer in Brushed Nickel (54480LF)` |
| 8285 | Bathroom | Basin Mixer | `Type` | `Alder 54082 Brushed Nickel` |

Each block has `_decompose_meta = {labels_count: 2, values_count: 0}`, `value_lines: []`, and the parent `Sink Mixer` / `Tub Mixer` / `Basin Mixer` itself has **no anchor row in `rows`** (neither `is_group_anchor: true` nor any plain row carrying the parent label). The product string that should sit on that missing parent-anchor row has migrated into the first child slot.

Sibling row at line 7191 (Kitchen Sink Mixer `Location = "Centre of Sink"`) and line 8310 (Bathroom Basin Mixer `Location = "Centre of Basin"`) come from the older `pdfplumber_text_rescue` pass and look correct; do not regress those.

### Why this matters more than it looks

`Type` is an enum-shaped slot label (typical legitimate values: `Wall-mounted`, `Overmount`, `Undermount`, `Centre of Basin`-style positional). A product description in that slot:

- Is not visually distinguishable from a legitimate Type value in Excel — Jason cannot eyeball it during QA (memory `feedback_ui_visual_verification.md` does not help here, the cell is just a string).
- Survives downstream because it is non-empty.
- Replaces the prior Bug 6 negative-case improvement: Bathroom Basin Mixer `Type` went `"Overmount"` (Bug 6 stale cross-group) → `""` (Bug 6 fix, correct) → `"Alder 54082 Brushed Nickel"` (Bug 7 fallback, also wrong).
- Drops the actual Basin/Sink/Tub Mixer **product** entirely from output (no parent-anchor row, no Excel cell).

Net data effect per occurrence: **one product lost, one Type silently corrupted**.

### What the diagnostic counters already prove

```
EVOC447 38148:  raw_text_fallback_groups: 19   raw_text_fallback_pairs_filled: 28
EVOC467     :  raw_text_fallback_groups:  0   raw_text_fallback_pairs_filled:  0
EVOC473     :  raw_text_fallback_groups:  0   raw_text_fallback_pairs_filled:  0
```

EVOC467 / EVOC473 do not invoke the fallback at all, so they are clean by construction and cannot regress on the fallback path. Any regression there would mean the fix accidentally changed line-strategy or text-grid-rescue behavior, which is a stop-and-report signal.

EVOC447 has 28 fills across 19 groups — at least 3 are wrong (the table above). Codex must enumerate the remaining 25 fills in delivery item 5 and label each `OK` / `WRONG` / `SUSPICIOUS` so we have full visibility, not just the 3 already triaged.

---

## Goal

For groups where the parent label has no `is_group_anchor` row (i.e. the table layer extracted no value for the parent), `_apply_raw_text_fallback` must NOT inject a product-shape candidate into the first child slot when that child label is in a known slot/enum-shape whitelist (`Type`, `Location`, `Position`, `Mount`, `Mounting`, `Style`, `Finish Position`).

Two acceptable resolutions, in order of preference:

**(A) Preferred — synthesize a parent-anchor row.** When the fallback finds a candidate keyed by the parent group label (i.e. `_norm_label_key(group_label)` is present in `raw_text_lookup`), emit a synthetic row at position 0 with `label = group_label`, `value = candidate`, `is_group_anchor = True`, `source_method = "pdfplumber_raw_text_fallback"`. This is the truthful fix: parent product survives, child `Type` stays empty.

**(B) Fallback — refuse the fill.** If routing to a parent-anchor row is structurally too invasive for this pass, gate `_apply_raw_text_fallback` so it skips child rows whose `_norm_label_key` is in a slot-label whitelist when the candidate value is product-shape. Leaving `Type` empty preserves Bug 6's correctness gain.

**Either resolution must be deterministic.** No fuzzy matching, no language heuristics like "if value contains a number then it's a product" alone — combine signals (token count + slot-label whitelist + presence of brand-vocab token) and document the signal set inline.

### Slot-label whitelist (case/whitespace insensitive after `_norm_label_key`)

```
type
location
position
mount
mounting
style
finish position
```

These are the labels we have evidence for in EVOC447/467/473. **Do not extend this list** without re-running all three PDFs and showing the new label is also enum-shaped — extension is a stop-and-report event.

### Product-shape signal (required for option B; optional confirmatory check for option A)

A candidate value is product-shape if **at least two** of the following hold:

1. ≥ 3 whitespace-separated tokens after `parsing.normalize_space`.
2. Contains a digit run of ≥ 2 consecutive digits (e.g. `54082`, `85972LF`, `HS375`).
3. Contains at least one token that case-insensitively matches a known Evoca brand vocabulary entry. Seed list (extend only with PDF evidence, document any extension in delivery item 6):
   ```
   alder, adler, vito bertoni, eden, decina, caroma, fisher & paykel,
   smeg, miele, blanco, abey, methven, paini, phoenix, brodware,
   franke, oliveri, clark, raymor
   ```
4. Contains a parenthesized model code, e.g. `(85972LF)`, `(NO1480W)`.

Single-signal matches (e.g. just "has a number") are too aggressive — `Centre of Sink 2` would falsely match. Two-signal threshold is conservative.

### Out of scope (do not touch)

- The `Bath Mixer / Spout` block at JSON line 8388 (`Model = "Alder Soho 54380 Brushed Nickel"`, `Bath Spout Model = "Alder Brushed Nickel Round Swivel Bath Spout 240mm"`). Both child labels are product-shape labels (Model + Bath Spout Model), the fills look correct given the PDF likely has a single mixer/spout combo product. Verify it stays unchanged in regression but do not "fix" it.
- The `Adler` vs `Alder` spelling divergence between Bath Mixer (`Alder`) and Shower Mixer (`Adler`) blocks. Memory `feedback_pdf_typo_string_dedupe.md` covers this — likely a real PDF typo, not a parser defect. Note it in delivery item 6 if you observe it but do not normalize the spelling.
- `_promote_group_anchor_value` (line 1531) — that path handles a different anchor-promotion case (anchor exists in lookup AND value_lines has the same value AND first child currently equals it). The defect here is the opposite: anchor is missing entirely from `rows`. Don't try to merge or generalize the two paths in this pass; if the cleanest fix lives next to `_promote_group_anchor_value`, add a sibling helper instead.

---

## Required first step — re-run extractor and enumerate all 28 fallback fills

Before editing fix code, run the standalone exporter against EVOC447 and capture the post-Bug-7 baseline. This is non-negotiable: we need a verbatim list of every fallback fill so the fix can be evaluated row-by-row.

```powershell
.\.venv\Scripts\python.exe tools\evoca_structured_export.py `
  --out-dir tmp\evoca_structured_bug7_baseline `
  "C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38148\38148 - EVOC447 (Lot 1042 Rufous - COLOUR SELECTION DOCUMENT).pdf"
```

(EVOC467 / EVOC473 invocations: see `docs/EVOCA_STRUCTURED_HANDOFF_2026-04-30.md` for paths. Re-run them too — EVOC467/473 fallback counters must remain 0 at baseline.)

Then walk the EVOC447 JSON and collect every row where `source_method == "pdfplumber_raw_text_fallback"`. There should be 28. For each, paste in the delivery report:

- Room label
- Group label
- Child label
- Filled value (verbatim)
- Verdict: `OK` (value semantically matches the child label) / `WRONG` (product-shape value in slot-shape label) / `SUSPICIOUS` (cross-group bleed candidate, needs PDF check)

This enumeration is delivery item 5 and gates whether the slot-label whitelist is complete. If any `WRONG` row uses a child label NOT in the whitelist above, **stop and report** before writing fix code — the whitelist needs evidence-based extension first.

### Stop-and-report triggers

- Any `WRONG` fill in EVOC447 whose child label is not in the slot whitelist (e.g. a product string filling a label like `Manufacturer` or `Colour`). Means the defect class is broader than slot-label slots.
- `raw_text_fallback_groups > 0` on EVOC467 or EVOC473 in baseline. Means Bug 7 fired on PDFs Codex previously believed it skipped — the entire blast radius needs re-triage.
- Any `WRONG` fill in EVOC447 where the candidate value's normalized text does NOT also match a token sequence appearing on the same page's parent group anchor row (i.e. the product did not come from where we think). Means the lookup-construction bug is somewhere else.
- `_decompose_meta.values_count > 0` on any of the 3 known-broken groups (Kitchen Sink Mixer / Powder Tub Mixer / Bathroom Basin Mixer) in your re-run. Means the table-extraction layer changed behavior between Bug 7 commit and now — file integrity issue, halt.

---

## Implementation guidance

Codex's call on which of `_apply_raw_text_fallback` / `_parse_raw_text_pairs_for_group` / `_rescue_group` is the right surgery point. Likely shape:

1. Decide A vs B (parent-anchor synthesis vs slot-label refusal). If choosing A, you also need to update `_decompose_meta` for the affected group so `values_count` reflects the synthetic row, and ensure the synthetic row gets `row_order` consistent with neighboring rows.
2. Add a private helper `_is_slot_shape_label(label_key: str) -> bool` returning whether the normalized key is in the slot whitelist.
3. Add a private helper `_is_product_shape_value(value: str) -> bool` implementing the two-of-four signal threshold above.
4. Modify `_apply_raw_text_fallback` to consult these helpers before mutating `row["value"]`. Existing skip conditions (`is_group_anchor`, `_is_note_row`, empty key, `key == "continuation"`, non-empty current value) are preserved.
5. Bump diagnostics: in addition to existing counters, add `raw_text_fallback_pairs_skipped_slot_label` (option B) or `raw_text_fallback_anchor_synthesized` (option A). One new int counter, initialized in the same dict, included in the same `statistics` block at JSON line ~123.
6. No new top-level imports unless genuinely needed. `re` is already imported.

Keep the change tight. No reformatting. No renames. No new module.

---

## Test requirements

### New unit tests (add to `tests/test_evoca_structured_extractor.py`)

1. **EVOC447 Kitchen Sink Mixer regression**: assert that after extraction, the row with `group_label == "Sink Mixer"` and child `label == "Type"` (in Kitchen room) has `value == ""` (option B) OR there exists a sibling anchor row with `label == "Sink Mixer"` and `value == "Vito Bertoni Alfie Pull-Out Sink Mixer in Brushed Nickel (85972LF)"` AND `Type` value is empty (option A). Both interpretations must leave `Type` empty.

2. **EVOC447 Powder Tub Mixer regression**: same shape, value to check is `Alder Soho Gooseneck Sink Mixer in Brushed Nickel (54480LF)`.

3. **EVOC447 Bathroom Basin Mixer regression**: same shape, value to check is `Alder 54082 Brushed Nickel`. Also assert `Location == "Centre of Basin"` is preserved (must not regress the `pdfplumber_text_rescue` fill).

4. **Bug 6 negative-case still empty (regression of regression)**: assert Bathroom Basin Mixer `Type` does not equal `Overmount` (the original Bug 6 stale value) — i.e. Bug 6's fix is still effective.

5. **EVOC447 Kitchen Benchtops Colour preserved (Bug 7 positive case)**: assert Kitchen Benchtops `Colour == "Statuario Zero"`.

6. **EVOC447 Bath Mixer / Spout preserved**: Model and Bath Spout Model both remain populated with the Bug 7 fallback values (do not regress good fills).

7. **Synthetic fixture — slot-label guard**: construct a minimal group fixture with parent label `"Foo Mixer"`, no anchor row, child labels `["Type", "Location"]`, raw-text lookup `{"type": ["Acme 9999 Brushed Steel"]}`. Assert post-fallback: `Type` value is empty (option B) or anchor row `label="Foo Mixer"` value is `"Acme 9999 Brushed Steel"` and `Type` empty (option A).

8. **Synthetic fixture — product-shape value detection**: construct lookup `{"type": ["Wall-mounted"]}` (legitimate enum value, not product-shape). Assert `Type` value becomes `"Wall-mounted"` — the slot-label guard alone is not enough to refuse; product-shape signal must also gate. (Skip this test if you choose option A and don't implement product-shape detection.)

9. **Synthetic fixture — non-slot label still fills**: construct lookup `{"colour": ["Statuario Zero"]}`, child label `Colour`. Assert `Colour` value becomes `"Statuario Zero"` — non-slot-shape labels are unaffected by the new guard.

10. **Diagnostic counter**: assert the new counter (`raw_text_fallback_pairs_skipped_slot_label` or `raw_text_fallback_anchor_synthesized`) is present in `statistics` and increments correctly on the EVOC447 run.

### Existing regression — must still pass verbatim

Full pytest suite must still pass:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\ -x
```

Codex reported 1022 passed at Bug 7 landing. Post-fix expected: 1022 + 10 new = 1032 passed (give or take if any of the 10 above are merged into one parametrized test). If any pre-existing test fails, **stop and report**. Do not edit existing assertions or fixtures.

### Per-PDF regression (manual diff against `tmp/evoca_structured_bug7/`)

Re-run extractor against EVOC447, EVOC467, EVOC473. For each, diff the new JSON against `tmp/evoca_structured_bug7/`:

- **EVOC447**: only the 3 broken groups (Kitchen Sink Mixer / Powder Tub Mixer / Bathroom Basin Mixer) and any other `WRONG`/`SUSPICIOUS` rows from the step-1 enumeration should change. Bug 3 (Section 17 Appliances), Bug 4 (skip — EVOC473 only), Bug 5 (No-shelf room note), and Kitchen Benchtops Colour must not change. `raw_text_fallback_pairs_filled` should drop by ≥ 3 (option B) or stay similar with `raw_text_fallback_anchor_synthesized` ≥ 3 (option A).
- **EVOC467**: zero diff. `raw_text_fallback_groups` and `raw_text_fallback_pairs_filled` both remain 0.
- **EVOC473**: zero diff. Same counter expectation. Bug 4 fixture (Kitchen Overhead Cupboards anchor) unchanged.

---

## Manual verification (Jason will do; codex must call out exactly what to look for)

Open `tmp/evoca_structured_bug7_fix/38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.xlsx` in Excel after Codex delivers. Inspect:

- **Kitchen → Sink Mixer rows**: Type cell empty, Location = `Centre of Sink`. (Option A: a parent `Sink Mixer` row above showing the Vito Bertoni product.)
- **Powder → Tub Mixer rows**: Type cell empty, Location = `Corner of Tub`. (Option A: parent `Tub Mixer` row above showing the Alder Soho Gooseneck product.)
- **Bathroom → Basin Mixer rows**: Type cell empty, Location = `Centre of Basin`. (Option A: parent `Basin Mixer` row above showing Alder 54082.)
- **Kitchen → Benchtops → Colour cell**: still `Statuario Zero`. (Bug 7 positive case preserved.)
- **Bathroom → Bath Mixer / Spout**: Model and Bath Spout Model both still populated.

Codex must paste in delivery item 5: the Excel-equivalent (or JSON snippet) of each of the 5 spot-check sites above so Jason can eyeball without re-running anything.

---

## Delivery gate — 6 items, hard requirement

Per memory rule `feedback_codex_delivery_discipline.md`. Replying "done" without all 6 items will be rejected.

1. **Exact `git diff`** of all modified files (verbatim, not summary). Includes `evoca_structured_extractor.py`, `test_evoca_structured_extractor.py`, and any other touched file. Confirm no `tools/evoca_structured_export.py` change.
2. **Exact `pytest` output** for the 10 new unit tests (verbatim stdout, including pass counts and timings).
3. **Exact `pytest` output** for the full `tests/` run (verbatim — confirms 1032-ish pass, no regression).
4. **Line counts before/after** for `evoca_structured_extractor.py` and `test_evoca_structured_extractor.py` (`wc -l` each).
5. **Step-1 enumeration** of all 28 `pdfplumber_raw_text_fallback` fills in baseline `tmp/evoca_structured_bug7/...EVOC447...json` with `OK`/`WRONG`/`SUSPICIOUS` verdict per row, AND post-fix verdict per row showing each `WRONG` is now resolved (either `EMPTY` for option B or `MOVED_TO_ANCHOR` for option A). Plus the 5 manual-verification spot-check snippets pasted from the post-fix run.
6. **Surprises / deviations**: any `WRONG` row whose child label was not in the seed slot whitelist; any product-shape signal that needed extension to vocabulary list; any test you needed to mark `xfail` or skip and why; whether you chose option A or option B and the one-line reason; the `Adler` vs `Alder` divergence observation if you encountered it. If nothing surprising, write `none observed`. Also explicitly state `4-doc sync skipped — standalone parser, not yet wired` if no doc files were touched.

---

## Constraints recap

- **No edits** to `extraction_service.py`, `_finalize_evoca_rooms`, `runtime.py`, `SnapshotPayload`.
- **No edits** to `tools/evoca_structured_extractor_claude.py` or `tools/evoca_structured_export_claude.py` (Claude reference spike).
- **No edits** to sibling repo `code/claude-spec-extraction/`.
- **No expansion** of `SECTION_TITLE_PATTERNS` beyond Evoca sections 15-25.
- **No fuzzy matching** in fix code — slot whitelist + product-shape signal threshold only (per memory `feedback_rescue_empty_over_wrong.md`).
- **No `tools/checkpoint.ps1`** — working tree has intentional untracked task docs and Claude reference tools (per memory `feedback_checkpoint_ps1_boundary.md`); use precise `git add` listing exact filenames.
- **No `git add .` / `-A` / `-u`** — list precise filenames.
- **No 4-doc sync** unless an existing standalone-parser entry needs amendment; explicit skipped-note required if not (item 6).
- **PowerShell commit message**: single-quoted outer wrapper, no embedded double quotes (per memory `feedback_powershell_commit_quoting.md`). Example: `git commit -m 'EVOCA-STRUCTURED-V0 Bug 7.1: stop raw-text fallback from leaking parent product into Type slot'`.
- **No `--no-verify`, no `--amend`, no force-push.**
- **Touch only Bug 7.1**. Do NOT opportunistically address Bug 8 (Continuation pseudo-label) or Bug 9 (Powder cross-page anchor) in this pass — both still on hold per memory `project_evoca_structured_parser_bugs.md`.
