# Bug Task - Imperial V6 Base Cabinetry Wrapped Label Split

## Status

Fixed and production-verified 2026-05-18 in
`App/services/pdf_to_structured_json.py` and `App/services/parsing.py`.

The job 38 source PDF page 2 has one logical base cabinetry colour row:

```text
AREA / ITEM:
BASE CABINETRY COLOUR
INCLUDES SINGLE CABINET ON BAR
BACK AREA

SPECS / DESCRIPTION:
Polytec
Classic White
Matt

SUPPLIER:
Polytec
```

Before this fix, the v6 continuation splitter treated the three AREA lines and
three SPECS lines as three separate missing-row-separator records:

```text
BASE CABINETRY COLOUR       -> Polytec / Polytec
INCLUDES SINGLE CABINET ON  -> Classic White / Polytec
BACK AREA                   -> Matt / Polytec
```

That was wrong: the later AREA lines are a wrapped note/qualifier for the same
`BASE CABINETRY COLOUR` row, not independent source rows.

## Fix

- `assign_word_to_column()` now keeps words that overflow from AREA into the
  blank gap before SPECS on AREA. This preserves the source word `BAR` in
  `INCLUDES SINGLE CABINET ON BAR` instead of dropping it because it sits just
  outside the visible AREA column range.
- `_repair_wrapped_cabinetry_colour_record()` detects the source-backed wrapped
  base-cabinetry label, keeps the canonical AREA as `BASE CABINETRY COLOUR`,
  moves the wrapped qualifier into `notes`, and removes the duplicate leading
  supplier from `specs` when it matches the SUPPLIER cell.
- `_should_add_missing_row_separator_review_hint()` and
  `_split_review_hint_record()` now treat this wrapped cabinetry-colour pattern
  as one logical row, not as a missing-row-separator split candidate.
- `parsing._imperial_extract_material_row_notes()` now removes a duplicated
  note prefix from finalized door-colour descriptions, so the final material row
  remains `Classic White Matt` with the note kept only in `notes`.

## Local Verification

Source PDF gate:

```powershell
.\.venv\Scripts\python.exe App\services\pdf_to_structured_json.py "C:\Users\Jason Niu - XM\Desktop\Builder\Imperial\38251\SIGNED FINAL COLOURS_FOXOVER 21 Shadowood st KENMORE 23 3 26.pdf" tmp\job38_38251_v6_after_base_fix.json
```

Result:

```text
KITCHEN JOINERY SELECTION SHEET: 11 items
BASE CABINETRY COLOUR
specs = Classic White / Matt
supplier = Polytec
notes = INCLUDES SINGLE CABINET ON BAR BACK AREA
```

V6 finalizer gate:

```text
door_colours_base = Polytec - Classic White Matt
material row description = Classic White Matt
material row supplier = Polytec
material row notes = INCLUDES SINGLE CABINET ON BAR BACK AREA
material row display = Polytec - Classic White Matt
```

## Production Verification

Production deploy completed successfully, then job 38 was rerun as spec parse:

```text
job 38 / run 2371 / Imperial v6 / build local-89aa455a / Completed
```

Live historical Spec List and latest `spec-list.xlsx` verified:

```text
By Section row:
BASE CABINETRY COLOUR | Classic White / Matt | Polytec | INCLUDES SINGLE CABINET ON BAR BACK AREA | page 2

Material Summary row:
Door Colours | KITCHEN JOINERY SELECTION SHEET | BASE CABINETRY COLOUR | Polytec | Classic White / Matt | INCLUDES SINGLE CABINET ON BAR BACK AREA
```

No fake `INCLUDES SINGLE CABINET ON` or `BACK AREA` label rows remain in the
Excel export. The earlier job 38 recoveries for `DISHWASHER` and
`TAPWARE (KITCHEN)` were also still present on run 2371.

Targeted tests:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_pdf_extractor_split.py tests\test_imperial_v6_adapter.py tests\test_v6_titled_template_recovery.py -q
```

Result: `36 passed`.

## Out Of Scope

- handle grouped-summary / display dedupe
- label or note clipping such as `SINKWARE (KITCHE` / `Tap Landi`
- changing Imperial primary `Tap` display policy
