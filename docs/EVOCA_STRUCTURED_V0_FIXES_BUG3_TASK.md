# Task — EVOCA-STRUCTURED-V0 Bug 3: label-value shift on `labels > values > 1` non-terminal anchors

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: master at HEAD post-`EVOCA_STRUCTURED_V0_FIXES_TASK.md` (Bug 1 + Bug 2 already landed). `App/services/evoca_structured_extractor.py` includes `TERMINAL_GROUP_VALUES`, terminal-group special case in `_build_group_rows`, `_consume_unanchored_group` (or boundary fix), `_build_value_lookup`, `_rescue_missing_values`, `_norm_label_key`. `tmp/evoca_structured_fix/EVOC{467,473}*.json` are the post-Bug 1+2 reference outputs.

**Layer scope**:
- `App/services/evoca_structured_extractor.py` only — extend the rescue pass with "shift-suspected override". No new files. No changes to `_build_value_lookup`. `TERMINAL_GROUP_VALUES` constant reused.
- `tools/evoca_structured_export.py` — re-run only; no code changes expected.

**Out of scope**:
- `extraction_service.py` / `_finalize_evoca_rooms` / `runtime.py` — still standalone-spike phase.
- `SECTION_TITLE_PATTERNS` scope — keep 15-25.
- The two pre-existing edge cases Claude flagged but **not** part of Bug 3:
  - `WC**` mis-eat in Powder room Accessories & Toilet Suite (Toilet Roll Holder = "WC")
  - Wrapped value lines in Ensuite 1 / Ensuite 2 Shower (`R166-` / `GM & EDEN-GM)` split-line and `Omega Integrated...250mm Round` / `Monsoon Shower (...)` split-line)
- Sibling repo / Docling / SnapshotPayload contract.
- 4-doc sync (still standalone tool).

---

## Context — read before editing

### Bug 3 reproduction

Open `tmp/evoca_structured_fix/EVOC467_Lot_1038_Oyster_-_COLOUR_SELECTION_DOCUMENT_20251111125911918v06.json`. Find Section 17 APPLIANCES → group "Appliances". Current `rows`:

```
Freestanding Cooker  | Fisher & Paykel 900mm Dual Fuel OR90SCG1LX1 (Electric & Gas)   ✓
Hot Plate            | Not Applicable                                                  ❌  PDF: empty
Second Hot Plate     | Not Applicable                                                  ✓
Oven                 | Not Applicable                                                  ❌  PDF: empty
Second Oven          | Fisher & Paykel 900mm Undermount Rangehood (HP90ICSX4)          ❌  PDF: Not Applicable
Microwave            | Fisher & Paykel 600mm Freestanding (DW60FC1X2)                  ❌  PDF: Not Applicable
Rangehood            | Fisher & Paykel 900mm Undermount Rangehood (HP90ICSX4)          ✓  (filled by rescue)
Dishwasher           | Fisher & Paykel 600mm Freestanding (DW60FC1X2)                  ✓  (filled by rescue)
```

PDF page 12 actually shows:

```
- Appliances
    Freestanding Cooker      Fisher & Paykel 900mm Dual Fuel OR90SCG1LX1 (Electric & Gas)
    Hot Plate                (empty cell)
    Second Hot Plate         Not Applicable
    Oven                     (empty cell)
    Second Oven              Not Applicable
    Microwave                Not Applicable
    Rangehood                Fisher & Paykel 900mm Undermount Rangehood (HP90ICSX4)
    Dishwasher               Fisher & Paykel 600mm Freestanding (DW60FC1X2)
```

8 labels, 6 actual non-empty values. **4 of 8 rows are wrong**. Downstream impact: every appliance feature (Material Summary, appliance grid, room-card display) reading these fields will mis-render.

### Root cause

In `_consume_group` / `_build_group_rows`, when the anchor's combined value cells contain N values and the label list has M > N labels (and it is **not** a terminal-group case where a single value applies to the whole group), the pairing:

```python
for index, label in enumerate(cleaned_labels):
    value = cleaned_values[index] if index < len(cleaned_values) else ""
```

silently shifts every value into the wrong label whenever pdfplumber's `lines` strategy collapses out empty cells. pdfplumber's default `lines` strategy compresses rows that have no border-detected cell content — so PDF rows with label-only (no value) get dropped from the value column, but their label still appears in the merged label cell. Result: the Nth label gets the Nth value even though some labels in between should have been empty.

`_rescue_missing_values` (Bug 2 fix) only fills rows whose `value == ""`. The shifted-but-non-empty rows are never visited.

### Why the text-strategy lookup is the right authority

The companion `vertical_strategy="lines", horizontal_strategy="text"` extraction Codex already uses for Bug 2 rescue does **not** collapse empty-value rows — it splits rows by text y-position regardless of border presence. So for Section 17 Appliances on EVOC467 page 12, the text-strategy table yields per-label (label, value) pairs in source order, with empty-value rows simply missing from the lookup keys.

Empirically (run pdfplumber with text-h on the page; you can verify with a 5-line REPL):

```
"freestanding cooker"  → ["Fisher & Paykel 900mm Dual Fuel..."]
"second hot plate"     → ["Not Applicable"]
"second oven"          → ["Not Applicable"]
"microwave"            → ["Not Applicable"]
"rangehood"            → ["Fisher & Paykel 900mm Undermount Rangehood (HP90ICSX4)"]
"dishwasher"           → ["Fisher & Paykel 600mm Freestanding (DW60FC1X2)"]
```

`hot plate` and `oven` keys are absent — correct, because the PDF cells are empty. So the lookup is **fully authoritative** for this group. We just need to use it.

---

## Target behaviour

Add a **shift-suspected override** step in `_rescue_missing_values` (or co-located in a new helper called from the same loop). The override fires per-group, bounded by these guards:

```
override_eligible = (
    labels_count > values_count       # under-supplied by lines strategy
    AND values_count > 0              # group is not entirely empty
    AND group is NOT terminal         # anchor value not in TERMINAL_GROUP_VALUES
)
```

When eligible, for each `Row` in the group:

1. Compute `key = _norm_label_key(row.label)`.
2. Look it up in the per-page consumable dict (the same `consumable` already built by `_rescue_missing_values`).
3. **If the key has 1+ remaining candidates**:
   - `row.value = candidates.pop(0)` (override whatever positional zip put there)
4. **If the key has 0 remaining candidates** (or the key isn't in the lookup at all):
   - `row.value = ""` (clear the wrong shifted value — PDF cell is empty)
5. **Skip rows where `is_group_anchor=True`** — the anchor row's value is the structural anchor (group label / terminal value); leave it alone.

Counters to add to diagnostics (extend the dict already on the document):
- `shift_override_groups`: number of groups that triggered the override path
- `shift_overrides_applied`: rows whose value was overridden with a lookup candidate
- `shift_clears_applied`: rows cleared to empty because no lookup match

### How to know `labels_count` and `values_count` at rescue time

`_consume_group` already records `value_lines` per group (visible in the Bug 1+2 JSON output). Either:

- (a) Re-derive `labels_count` and `values_count` at rescue time from `group["raw_rows"]` (count `_split_lines(_cell(raw_anchor, 1))` vs flatten `value_lines`), or
- (b) During flush, attach a small `_decompose_meta` dict (or two int fields `_labels_count` / `_values_count`) to the group; rescue reads them.

Option (b) is cleaner — single source of truth, no re-parsing. Choose whichever you prefer; (b) is more robust against future refactors.

### Integration order

The override must run **inside the same loop** as the existing fill-empties rescue, NOT as a separate pass — because the override consumes from the same `consumable` dict. Sequence per group:

1. Skip if anchor value is in `TERMINAL_GROUP_VALUES` (existing).
2. **NEW:** if `labels_count > values_count > 0` and not terminal → run the shift-override on every non-anchor row (overwriting wrong values, clearing missing labels).
3. **OTHERWISE (the original Bug 2 path):** for each row with empty `value`, fill from `consumable` if a candidate exists.

Step 2 and step 3 are mutually exclusive — a group runs one or the other, never both. This avoids double-popping the same lookup candidates.

---

## Acceptance criteria

All four checks must pass on the refreshed `tmp/evoca_structured_fix/EVOC{467,473}*.{json,xlsx}`. Re-run via the existing `tools/evoca_structured_export.py` invocation.

### Check 1 — EVOC467 Section 17 Appliances exact match (the headline bug)

Open the EVOC467 sheet `17 APPLIANCES`, group "Appliances". Every row must match:

| Label | Expected value |
|---|---|
| Freestanding Cooker | `Fisher & Paykel 900mm Dual Fuel OR90SCG1LX1 (Electric & Gas)` |
| Hot Plate | (empty) |
| Second Hot Plate | `Not Applicable` |
| Oven | (empty) |
| Second Oven | `Not Applicable` |
| Microwave | `Not Applicable` |
| Rangehood | `Fisher & Paykel 900mm Undermount Rangehood (HP90ICSX4)` |
| Dishwasher | `Fisher & Paykel 600mm Freestanding (DW60FC1X2)` |

`source_method` for each will be `pdfplumber_text_rescue` after override (or `pdfplumber_table` if you choose to mark only the changed ones — your call, document in code).

### Check 2 — EVOC473 Section 17 regression (mustn't break)

Open EVOC473 sheet `17 APPLIANCES`, group "Appliances". The current Bug 1+2 output is **already correct** here (8 labels, 8 values, no shift). After the override, every row must remain unchanged:

| Label | Expected value (unchanged) |
|---|---|
| Freestanding Cooker | `Not Applicable` |
| Hot Plate | `Fisher & Paykel 900mm 5 Zone Induction Cooktop CI905DTB4 (Electric)` |
| Second Hot Plate | `Not Applicable` |
| Oven | `2x Fisher & Paykel 600mm Oven OB60SC5LB1 (Electric)` |
| Second Oven | `Not Applicable` |
| Microwave | `Not Applicable` |
| Rangehood | `Haier 900mm Undermount Rangehood (HPH90ILX2)` |
| Dishwasher | `Fisher & Paykel Black Dishwasher (DW60FC2B2)` |

Reason: this group has `labels_count == values_count`, so the override path **doesn't fire**. Confirms guards work.

### Check 3 — terminal-group regression (Contrasting Facings still empty)

Open EVOC467 sheet `15 CABINETS`, Kitchen → Contrasting Facings. After override, all 5 rows must remain:

```
Contrasting Facings  | Not Applicable      (anchor)
Manufacturer         | (empty)
Type                 | (empty)
Colour               | (empty)
Location             | (empty)
```

Reason: terminal value short-circuits the override. Confirms no bleed-through.

### Check 4 — diagnostics + pytest

- Re-run `pytest tests/ -x` — all green.
- New diagnostic keys present in JSON statistics or as a sibling block: `shift_override_groups`, `shift_overrides_applied`, `shift_clears_applied`. Numbers should be small (single-digit groups across both fixtures, ~8-12 row overrides total).
- Total `row_count` shouldn't change materially from current (~280-310 across the two fixtures).

---

## Known limitation (document, do not fix in this task)

When the same label key appears in multiple groups on the same page (e.g. `Manufacturer` recurs across Kitchen / Butlers / Laundry / Powder / Ensuite groups in section 15), the `consumable[key].pop(0)` consumption order assumes the text-strategy table lists labels in the same source order as `_consume_group` processes anchors. Empirically this holds for Evoca PDFs, but document this assumption in the docstring of the override function so a future bug report can find it.

If a future PDF violates this (different layout order, multi-column page, etc.), the override could mis-consume candidates. Mitigation for that future case (if it appears): fall back to position-bounded lookup using anchor's `bbox.y` from pdfplumber. Out of scope for v1.

---

## Delivery checklist

- [ ] `App/services/evoca_structured_extractor.py` updated with shift-override step in `_rescue_missing_values` (or peer helper), guarded by `labels_count > values_count > 0` AND not terminal AND non-anchor row.
- [ ] Anchor row (`is_group_anchor=True`) is **never** touched by the override.
- [ ] Existing Bug 1 (terminal group) and Bug 2 (rescue empty values) paths unchanged in their respective trigger conditions.
- [ ] Three new diagnostic counters (`shift_override_groups`, `shift_overrides_applied`, `shift_clears_applied`) wired in.
- [ ] Refreshed `tmp/evoca_structured_fix/EVOC{467,473}*.{json,xlsx}` checked in (or written to the same path Codex used).
- [ ] All 4 acceptance checks above hand-verified by Jason.
- [ ] `pytest tests/ -x` green (1009+ passed, 0 regressions).
- [ ] Commit message: `EVOCA-STRUCTURED-V0: bug 3 — label-value shift override on under-supplied non-terminal groups`. Use single-quoted PowerShell heredoc per repo convention.
