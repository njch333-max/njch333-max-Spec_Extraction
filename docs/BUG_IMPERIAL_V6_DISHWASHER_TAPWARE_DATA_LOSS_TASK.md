# Bug Task - Imperial V6 Dishwasher and Kitchen Tapware Data Loss

## Status

Fixed 2026-05-18 in `App/services/pdf_to_structured_json.py` and verified on
production job 38 run 2370 (`Imperial v6`, build `local-6b508c03`). Layer (a)
extractor fix; no adapter changes required. Verified against the job 38
sign-off PDF locally and against the fresh live run:

- APPLIANCES section grew from 5 to 6 items (DISHWASHER recovered on page 5
  with `_source.method = "titled_template_recovery"`).
- SINKWARE & TAPWARE section grew from 1 to 2 items, spanning pages 7-8;
  the spurious standalone section `TAPWARE (KITCHEN) BY CLIENT` no longer
  appears (TAPWARE row recovered on page 8 via the existing
  `template_anchor` continuation path).
- Imperial `Tap` primary-display exclusion is unchanged.
- Whole test suite green (1120 passed, 12 skipped).
- Live run 2370 historical Spec List and latest `spec-list.xlsx` both contain
  `DISHWASHER` / `DBI364ID.S.AU` and `TAPWARE (KITCHEN)` / `TA9601CP`; the
  spurious `TAPWARE (KITCHEN) BY CLIENT` section is absent.

Fix scope (intentionally narrow per CC review):

- `extract_page_title()` now skips lines matching
  `^(SINKWARE|TAPWARE)\s*\(...\)` so page 8's AREA-column row label is no
  longer mistaken for a section title. The canonical
  `SINKWARE & TAPWARE` / `APPLIANCES` / `... SELECTION SHEET` matches are
  preserved.
- Titled APPLIANCES pages now run a scoped template-recovery pass after the
  grid loop. Only AREA labels in `_APPLIANCE_AREA_WHITELIST`
  (`OVEN / COOKTOP / DISHWASHER / RANGEHOOD / RANGE HOOD / MICROWAVE /
  FRIDGE / BAR FRIDGE / FREEZER / WINE FRIDGE / COFFEE MACHINE /
  STEAM OVEN / WARMING DRAWER`) are appended, and only when not already
  present from grid extraction. The recovery is intentionally not enabled
  on joinery / material / sinkware pages.
- CLI JSON write now uses explicit `encoding="utf-8"` to keep local
  Windows gates from hitting cp1252 default-encoding noise.
- Synthetic unit tests in `tests/test_v6_titled_template_recovery.py`
  cover the title rule (row-label vs section-title), the appliance
  whitelist, and the dedup branch - no customer PDF is committed.

## Problem

Live job 38 run 2369 used the Imperial v6 fast path:

```text
parser_strategy = imperial_v6
layout_provider = pdf_to_structured_json_v6
layout_attempted = No
docling_attempted = No
vision_attempted = No
build = local-b6eaa172
```

The source PDF contains a page 5 dishwasher row and a page 8 kitchen tapware row,
but the live v6 output omits both. This is source-backed data loss, not a
factory-display preference.

Because the fast path skips legacy layout, Docling, Heavy Vision, builder polish,
and raw crosscheck, the recovery belongs in the v6 section-heading /
continuation extraction path first, then in the adapter only if the raw v6 JSON
already contains the missing row.

## Source-Backed Examples

### Page 5 - Dishwasher

The source PDF text contains:

```text
DISHWASHER
ASKO 60cm classic built under dishwasher stainless steel
DBI364ID.S.AU
BY CLIENT
```

Expected source-row meaning:

```text
area = DISHWASHER
specs/model include ASKO, 60cm classic built under dishwasher, stainless steel, DBI364ID.S.AU
supplier = BY CLIENT
page = 5
```

### Page 8 - Tapware (Kitchen)

The source PDF text contains the visual kitchen tapware row. The extracted text
can glue the model and row heading together:

```text
Tap Franke Eos Neo
pull out tap copper TA9601CPTAPWARE (KITCHEN) BY CLIENT
```

Expected source-row meaning:

```text
area = TAPWARE (KITCHEN)
specs/model include Tap Franke Eos Neo, pull out tap copper, TA9601CP
supplier = BY CLIENT
page = 8
```

This task is about retaining the source row in v6 raw JSON and the raw snapshot
review/export surfaces. It must not change the current Imperial room-card policy
that excludes `Tap` from primary room cards, primary material summary, and
primary Imperial room-field display unless that policy is explicitly reopened in
a separate task.

## Current Code Surface

Start by proving the layer:

- `App/services/extraction_service.py`
  - `_build_imperial_v6_fast_snapshot(...)`
  - owns the fast-path bypass and fallback behavior
- `App/services/parsing.py`
  - `_parse_spec_documents_structure_first(...)`
  - `_process_v6_imperial_document(...)`
  - calls the v6 adapter for Imperial room-master documents
- `App/services/imperial_v6_adapter.py`
  - `run_v6_extraction(...)`
  - `build_room_from_v6_section(...)`
  - maps v6 section JSON into room rows and `material_rows`
- `App/services/pdf_to_structured_json.py`
  - `extract_page_title(...)`
  - `extract_pdf(...)`
  - `extract_continuation_with_template(...)`
  - likely owner when the raw v6 JSON is already missing page 5 or page 8 rows

## Likely Failure Modes To Verify

- Page 5 `DISHWASHER` may appear before or around the repeated appliance heading
  in text/table order. If the page is visually an `APPLIANCES` page, the row
  should stay in the appliances section instead of being dropped as pre-heading
  noise.
- Page 8 has no clean repeated table heading and the text layer can glue the
  model code to the next label as `TA9601CPTAPWARE`. The extractor should split
  known area labels such as `TAPWARE (KITCHEN)` from adjacent product/model text
  only when the split is source-backed.
- A continuation page under `SINKWARE & TAPWARE` must remain attached to that
  section until a stronger new section title or footer boundary appears.

## Fix Boundary

Do:

- recover the page 5 `DISHWASHER` source row
- recover the page 8 `TAPWARE (KITCHEN)` source row
- keep page, row, and source-provider provenance inspectable
- add targeted regression coverage that can run without committing private PDFs
- run a private real-PDF gate locally against job 38 before production deploy

Do not:

- fix the page 2 kitchen base-cabinetry continuation split in this task
- fix handle display dedupe or grouped handle summary wording in this task
- fix label/note clipping such as `SINKWARE (KITCHE` or `Tap Landi` in this task
- reintroduce `Tap` into Imperial primary room cards or material summary
- invent missing appliance or tap rows from generic page text

## Acceptance Criteria

Layer proof:

- If `pdf_to_structured_json.py` raw JSON is missing `DISHWASHER` or
  `TAPWARE (KITCHEN)`, fix layer (a) extractor behavior.
- If raw JSON contains the rows but the website/export does not, fix layer (b)
  adapter/post-processing behavior.

Expected output after the fix:

- Page 5 dishwasher is present with ASKO, `DBI364ID.S.AU`, and `BY CLIENT`.
- Page 8 tapware is present with Franke Eos Neo, `TA9601CP`, and `BY CLIENT`.
- Page provenance is retained as page 5 and page 8 respectively.
- Existing page 7 sinkware rows remain source-equivalent.
- Imperial `Tap` primary display exclusion is unchanged unless a separate task
  explicitly changes it.

## Verification Gate

Local source-PDF gate:

```powershell
.\.venv\Scripts\python.exe App\services\pdf_to_structured_json.py "C:\Users\Jason Niu - XM\Desktop\Builder\Imperial\38251\SIGNED FINAL COLOURS_FOXOVER 21 Shadowood st KENMORE 23 3 26.pdf" tmp\job38_38251_v6_after.json
```

Then inspect the JSON for:

```text
page 5 DISHWASHER
page 8 TAPWARE (KITCHEN)
```

Targeted regression gate:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_pdf_extractor_split.py tests\test_imperial_v6_adapter.py tests\test_imperial_v6_path_dispatch.py tests\test_imperial_v6_room_fields.py -q
```

Production gate after implementation:

- deployed the scoped extractor file to `spec.lxtransport.online`
- reran job 38 with the Imperial v6 path as run 2370
- verified run 2370, not run 2369
- compared the affected rows against the source PDF
- confirmed `DISHWASHER` and `TAPWARE (KITCHEN)` source rows are retained on
  the live historical Spec List and latest `spec-list.xlsx`

## Follow-Up Tasks

Separate task specs should be written for:

- kitchen base-cabinetry continuation row assembly
- handle display dedupe and grouped handle summary supplier splitting
- label/note clipping such as truncated `SINKWARE (KITCHEN)` and `Tap Landing`
