# Task — EVOCA-STRUCTURED-V0 Excel refresh: room banners, summary sheet, row-type colour coding

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: master at `bca83c4` (EVOCA-STRUCTURED-V0 standalone spike with Bug 1/2/3 already shipped). `App/services/evoca_structured_extractor.py` `write_structured_workbook` is the current 50 LOC starting point. `tools/evoca_structured_export.py` is unchanged. `tmp/evoca_structured_bug3/EVOC{467,473}*.xlsx` is the current reference output that this task replaces.

**Layer scope**:
- `App/services/evoca_structured_extractor.py` — only `write_structured_workbook` and its helpers (`_sheet_name`, `_unique_sheet_name`, `flatten_rows_for_export`, `_export_group`, `_export_row`). Reach into `_summary` and per-row-type styling.
- No changes to JSON output, schema, extraction logic, Bug 1/2/3 fixes, or test fixtures.
- Re-run `tools/evoca_structured_export.py` on EVOC467/EVOC473 fixtures into the same output dir; overwrite is fine.

**Out of scope**:
- JSON schema (`schema_version: "evoca_structured_v0"` stays as-is — `raw_cells` and `source_method` remain in JSON for round-trip).
- Extractor / decomposition logic. No changes to `_consume_group`, `_build_group_rows`, `_rescue_missing_values`.
- `extraction_service.py`, `_finalize_evoca_rooms`, SnapshotPayload — still standalone-spike phase.
- Section coverage (`SECTION_TITLE_PATTERNS` stays at 15-25).
- Pre-existing edge cases (`WC**` mis-eat, multi-line wrap split) — separate future work.

---

## Why this change

The current Excel layout is structurally correct but visually flat — Page / Source Row / Room / Group / Label / Value / **Raw Cells JSON** / **Source Method** all show as plain text rows. When Jason opens `15_CABINETS` to spot-check 11 rooms × 7 groups × ~120 rows against the source PDF, there's no visual anchor for "where does Kitchen end and Butlers start". Result: every QA pass takes longer than necessary.

Goal is to make the workbook **scannable at a glance**: room dividers obvious, anchor rows obvious, terminal-N/A rows visually de-emphasised, and an at-a-glance summary sheet so Jason knows whether section coverage matches expectations before drilling in.

---

## Concrete changes

### 1. Add `_summary` sheet at index 0

Builder runs after all section sheets. Columns:

```
Section #  |  Title  |  Sheet  |  Page  |  Rows  |  Anchors  |  Notes
```

Plus a small header block above the table:
- `Source PDF` | `<filename>`
- `Pages` | `<page_count>`
- `Sections` | `<section_count>`
- `Schema version` | `evoca_structured_v0`
- (optional) Diagnostics block: `shift_override_groups`, `shift_overrides_applied`, `shift_clears_applied`

### 2. Room banner rows

Whenever a section has rooms, before the **first** group row of each room, emit a merged banner row spanning all data columns with:
- Text: `room: <room_label>`
- Fill: light blue (`#DCE6F1`)
- Font: bold

Example for `15_CABINETS`:
```
[merged blue]                 room: Kitchen
[gray]    Page  Order  Kitchen  Benchtops          Manufacturer  Quantum Quartz   ...
[gray]    Page  Order  Kitchen  Benchtops          Colour        Champagne        ...
[merged blue]                 room: Butlers
[gray]    Page  Order  Butlers  Benchtops          Manufacturer  Polytec          ...
...
```

For sections with no rooms (e.g. 17 APPLIANCES, 22 WINDOW FURNISHINGS), no banner — groups attach directly to section.

### 3. Per-row-type colour coding

| Row type | Fill | Font |
|---|---|---|
| Header row | dark `#222222` | white bold |
| Room banner | light blue `#DCE6F1` | bold |
| Anchor row (`is_group_anchor=True`) | cream `#FFF2CC` | normal weight; "ANCHOR" text in dedicated column with bold colour `#996600` |
| Group sub-rows | light gray `#F2F2F2` | normal |
| Note / disclaimer | peach `#FCE4D6` | italic |
| Value cell where value lower-cases to `not applicable` / `not included` / `n/a` / `#n/a` / `tbc` | (no fill change) | italic gray `#888888` |

All cells get a thin gray border (`#BBBBBB`).

### 4. Column changes

Drop two columns from the current 8-column layout:
- **Drop**: `Raw Cells` (JSON dump — bloats Excel, info still in JSON file)
- **Drop**: `Source Method` (debug-only; available in JSON if needed)

New 8-column header (last column replaces both):

```
Page  |  Order  |  Room  |  Group  |  Label  |  Value  |  Anchor  |  Source Text
```

- `Order` = the existing `row_order` field
- `Anchor` = the literal string `ANCHOR` if `is_group_anchor=True`, else empty
- `Source Text` = a compact reconstruction of the row, e.g. `"Manufacturer: Quantum Quartz"` or `"- Benchtops"` for anchor rows. Build inline from `label` + `value`; no need to add a new field to the JSON.

Column widths (tuned for readability): `[6, 7, 22, 28, 32, 44, 8, 60]`.

### 5. Wrap text + freeze panes

- Freeze panes at `A2` (already done).
- Set `wrap_text=True` and `vertical=top` on every data cell — long values like `"Lana Rimless Back to Wall Toilet Suite Gloss White (6002-R-W)"` should wrap inside the cell, not get clipped.

---

## Acceptance gate

Run `python tools/evoca_structured_export.py <both PDFs> --out-dir tmp/evoca_structured_excel_v2/` on both fixtures. All 5 checks below must pass on hand-inspection:

1. **`_summary` sheet** — opens at first tab, lists 6 sections for EVOC473 / 11 sections for EVOC467, with row counts matching JSON statistics. Diagnostics block visible.

2. **Room banner — `15_CABINETS` (EVOC467)**:
   - Banner `room: Kitchen` precedes the first row of Kitchen → Benchtops → Manufacturer.
   - Banner `room: Butlers` precedes the first row of Butlers → Benchtops.
   - Banner appears for all 10 rooms (Kitchen / Butlers / Laundry / Bathroom / Ensuite / Ensuite 2 / Powder / Make Up Desk / Study Desk / Alfresco).

3. **Anchor visual — `15_CABINETS` Kitchen Contrasting Facings**:
   - Anchor row `Contrasting Facings | Not Applicable` — cream fill, "ANCHOR" in the Anchor column with bold dark-yellow text.
   - Sub-rows `Manufacturer / Type / Colour / Location` — light gray fill, empty Anchor column, value cells italic gray (because empty equivalent to terminal-N/A treatment is OK; or just leave value styling neutral when empty — pick one and document).

4. **Terminal-N/A italic — `17_APPLIANCES` (EVOC467)**:
   - Rows `Hot Plate` and `Oven` have empty Value cells (no italic needed since empty).
   - Rows `Second Hot Plate / Second Oven / Microwave` have `Not Applicable` in italic gray.
   - Rows `Freestanding Cooker / Rangehood / Dishwasher` have full product names in normal black.

5. **No regression on JSON** — `tmp/evoca_structured_excel_v2/EVOC{467,473}*.json` byte-identical to `tmp/evoca_structured_bug3/EVOC{467,473}*.json` (or differ only in absolute paths inside `source_pdf`). Sanity-check via `diff` on a key section like `15 CABINETS > Kitchen > Benchtops`.

Plus pytest: `pytest tests/ -x` stays at 1015 passing. The 5 evoca tests in `tests/test_evoca_structured_extractor.py` may need a small adjustment if any assertion checked for `Raw Cells` or `Source Method` columns by name — update the test to match new headers, NOT the other way around.

---

## Delivery checklist

- [ ] `write_structured_workbook` (and helpers) refactored. ~150-200 LOC net change.
- [ ] `_build_summary_sheet` helper added.
- [ ] Room banner emission integrated into the section-sheet loop.
- [ ] 8-column header replaces current 8-column header (1:1 column count, no schema change).
- [ ] Constants block at top for `HEADER_FILL`, `ROOM_BANNER_FILL`, `ANCHOR_FILL`, `GROUP_FILL`, `NOTE_FILL`, `BORDER`, `WRAP_TOP` etc — easier to tweak hex codes later.
- [ ] Re-run on both fixtures, hand-verify all 5 acceptance items.
- [ ] `pytest tests/ -x` 1015 green.
- [ ] Commit message: `EVOCA-STRUCTURED-V0: excel refresh — room banners, summary sheet, row-type colour coding`. Single-quoted PowerShell heredoc per repo convention.
