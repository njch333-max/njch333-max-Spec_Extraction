# Task - Evoca Native Structured Fast Path

**Owner**: Jason  
**Executor**: Codex  
**Reviewer**: Claude  
**Status**: Task spec only. No production code has been changed.

## Scope

Build an Evoca-native, `pdfplumber`/grid-first parser path that preserves the source PDF structure before any app-level flattening.

Primary deliverables:

- `App/services/evoca_structured_extractor.py`
- `tools/evoca_structured_export.py`
- `docs/EVOCA_STRUCTURED_SCHEMA_v0.md`
- Adapter/caller audit notes for the existing Evoca finalizer path
- Later, a guarded Evoca fast path in `build_spec_snapshot()`

Out of scope for the first spike:

- Production deployment
- Removing Evoca from `SPEC_EXTRACTION_DOCLING_BUILDERS`
- Replacing `_finalize_evoca_rooms()` blindly
- Rewriting Imperial v6 extractor logic
- Relying on cell background color as a required v0 signal

## Context

The current Evoca path already uses `pdfplumber` in the base PDF loader:

- `App/services/parsing.py:604` `extract_pdf_pages(...)`
- `App/services/parsing.py:650-667` reads `pdfplumber.extract_tables()` into `page["table_rows"]`

The existing Evoca finalizer also already consumes table rows:

- `App/services/parsing.py:22891` `_finalize_evoca_rooms(...)`
- `App/services/parsing.py:23678` `_evoca_collect_room_recovery_data_from_tables(...)`
- `App/services/parsing.py:23939` `_finalize_evoca_appliances(...)`

The current bottleneck is the legacy layout pipeline still allowing Evoca through Docling:

- `App/services/runtime.py:75` default `SPEC_EXTRACTION_DOCLING_BUILDERS = imperial,simonds,evoca,yellowwood`
- `App/services/extraction_service.py:213` `_spec_docling_enabled(...)`
- `App/services/extraction_service.py:619` `_apply_layout_pipeline(...)`

Measured local evidence from the two supplied PDFs:

- `pdfplumber` base read: about `1.3s` to `1.6s`
- Evoca layout pipeline with Docling enabled: `EVOC467` layout-only took about `153.5s`
- Same layout pipeline with Evoca Docling disabled: about `4.3s`
- Full snapshot with Evoca Docling disabled: about `10.7s` and `14.5s`, including appliance official lookup

Conclusion: Evoca should get an Evoca-native structured path. The shape should borrow Imperial v6's engineering pattern, not Imperial's five-column schema.

## Design Principle

Borrow from Imperial v6:

- `pdfplumber`/grid-first source extraction
- source-order rows
- structured JSON before app flattening
- adapter into the existing snapshot contract
- source-backed regression artifacts

Do not borrow Imperial v6's schema:

- Imperial source shape: `section -> rows`, with columns like `area/specs/image/supplier/notes`
- Evoca source shape: `section -> room -> group -> label/value rows`

Evoca must preserve its own PDF semantics first, then derive the app's normalized room fields second.

## Target Evoca JSON Shape

The first parser output should be Evoca-native and close to the PDF:

```json
{
  "source_pdf": "...",
  "builder": "Evoca",
  "pages": [
    {
      "page_no": 9,
      "sections_detected": ["15 CABINETS"],
      "table_count": 1
    }
  ],
  "sections": [
    {
      "section_code": "15",
      "section_title": "15 CABINETS",
      "page_start": 9,
      "page_end": 11,
      "rooms": [
        {
          "room_label": "Kitchen",
          "room_key": "kitchen",
          "groups": [
            {
              "group_label": "Benchtops",
              "rows": [
                {
                  "label": "Manufacturer",
                  "value": "Quantum Quartz",
                  "page_no": 9,
                  "row_order": 12,
                  "raw_cells": ["-", "Benchtops\nManufacturer\nColour", "Quantum Quartz\nChampagne"],
                  "source_method": "pdfplumber_table"
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

Required row-level preservation:

- `label`
- `value`
- `page_no`
- `row_order`
- `raw_cells`
- `source_method`
- optional diagnostics such as `table_index`, `row_index`, `confidence`, `notes`, `style_hint`

Important: `Not Applicable`, `#N/A`, blank values, continuation rows, and apparent redundant sub-fields must remain in the structured JSON. Suppression belongs in the adapter or app finalizer layer, not in the source parser.

## Boundary Detection Rules

The v0 extractor should use text/table geometry first.

Section headers:

- Detect known Evoca section titles by text, for example:
  - `15 CABINETS`
  - `17 APPLIANCES, ACCESSORIES & HOT WATER UNIT`
  - `20 PLUMBING FIXTURES & TAPWARE`
  - `23 TILING / HARD FLOORING`
  - `24 GLASS SPLASHBACK`
- Carry the current section forward across later pages until a new section header appears.
- Pages with pure paragraph content, such as painting specification pages, may be recorded as `unstructured_pages` and skipped by the structured table parser in v0.

Room headers:

- Treat known room labels as room boundaries when they appear as a table row with no meaningful value cell.
- Known examples include `Kitchen`, `Butlers`, `Laundry`, `Bathroom`, `Ensuite`, `Ensuite 1`, `Ensuite 2`, `Ensuite 3`, `Ensuite 4`, `Ensuite 5`, `Powder`, `Alfresco`, `Study Desk`, and `Make Up Desk`.
- Do not treat a room header as a material row.

Group rows:

- Treat rows with a leading `-` cell and a grouped label cell as group anchors.
- Examples:
  - `Benchtops`
  - `Underbench including Island`
  - `Overhead Cupboards`
  - `Pantry Doors`
  - `Sink`
  - `Sink Mixer`
  - `Basin`
  - `Basin Mixer`
  - `Appliances`
- Child label/value rows after a group anchor belong to that group until the next group anchor, room header, or section header.

Cell background color:

- Do not make color required for v0 correctness.
- It may be collected later as diagnostics from PDF rects/fills if it proves stable.
- Parser correctness must not depend on green/peach/blue color extraction in the first implementation.

## Excel QA Export

`tools/evoca_structured_export.py` must export the Evoca-native JSON to an Excel workbook that is easy to compare with the source PDF.

Required behavior:

- Input: one Evoca PDF path
- Output:
  - `tmp/evoca_structured/<safe_file_stem>.json`
  - `tmp/evoca_structured/<safe_file_stem>.xlsx`

Workbook shape:

- One sheet per detected section where possible:
  - `15 CABINETS`
  - `17 APPLIANCES`
  - `20 PLUMBING`
  - `23 FLOORING`
  - `24 SPLASHBACK`
- Each sheet keeps PDF source order.
- Minimum columns:
  - `Page`
  - `Source Row`
  - `Room`
  - `Group`
  - `Label`
  - `Value`
  - `Raw Cells`
  - `Source Method`
- Rows that are skipped by the app adapter should still be visible in this raw QA workbook.

This Excel export is an acceptance tool, not a nice-to-have. The webpage is already finalizer-cleaned and cannot be the parser acceptance source.

## Required Spike PDFs

Run the first spike on these two PDFs:

- `C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38117\EVOC473 (Lot 403 Sehmish - Color Selection Document) 20251107090209080v08.pdf`
- `C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38225\EVOC467 (Lot 1038 Oyster - COLOUR SELECTION DOCUMENT) 20251111125911918v06.pdf`

Acceptance for the spike:

- JSON contains the source-order section/room/group/row hierarchy.
- Excel can be manually checked against the PDF without needing to inspect the app webpage.
- `15 CABINETS`, `17 APPLIANCES`, `20 PLUMBING FIXTURES & TAPWARE`, and `23 TILING / HARD FLOORING` are represented where present.
- `Not Applicable` rows remain visible in JSON and Excel.
- Parser runtime for raw JSON/XLSX generation is seconds-level, not minutes-level.

## Adapter Audit Before Fast Path

Before wiring the new extractor into `build_spec_snapshot()`, audit current Evoca finalizer dependencies.

Current code anchors:

- `App/services/parsing.py:22891` `_finalize_evoca_rooms(...)`
- `App/services/parsing.py:23420` `_evoca_extract_flooring_values(...)`
- `App/services/parsing.py:23678` `_evoca_collect_room_recovery_data_from_tables(...)`
- `App/services/parsing.py:23939` `_finalize_evoca_appliances(...)`
- `App/services/extraction_service.py:11201` `_extract_generic_layout_overlay(...)`

Audit output should be a mapping table in `docs/EVOCA_STRUCTURED_SCHEMA_v0.md` or a sibling audit section:

| Snapshot field | Current source | New Evoca JSON source | Notes |
|---|---|---|---|
| `bench_tops_wall_run` | table/text recovery | `15 CABINETS -> room -> Benchtops` | Preserve wall/island split |
| `bench_tops_island` | table/text recovery | `15 CABINETS -> room -> Benchtops` | `As Above` should resolve in adapter, not parser |
| `door_colours_base` | Underbench mapping | `15 CABINETS -> room -> Underbench` | Suppress only in adapter/finalizer |
| `door_colours_overheads` | Overheads mapping | `15 CABINETS -> room -> Overhead Cupboards` | Preserve source group wording |
| `door_colours_tall` | Pantry mapping | `15 CABINETS -> room -> Pantry Doors` | Preserve kickboard separately |
| `handles` | section handle rows + merge logic | group rows under Underbench/Overheads/Pantry | Preserve door/drawer orientation lines |
| `sink_info` | plumbing table/text recovery | `20 PLUMBING -> room -> Sink/Tub` | Keep source model/type rows |
| `basin_info` | plumbing table/text recovery | `20 PLUMBING -> room -> Basin` | Room-family mapping belongs in adapter |
| `tap_info` | plumbing table/text recovery | `20 PLUMBING -> room -> Sink Mixer/Basin Mixer` | Keep location row |
| `flooring` | text-block extraction | `23 TILING/HARD FLOORING` groups | May remain text-block derived in v0 |
| `splashback` | text-block extraction | `23/24` section rows | Preserve raw row evidence |
| `appliances` | appliance table/text recovery | `17 APPLIANCES` rows | Official lookup remains a separate enrichment step |

The audit must call out which existing Evoca fixes rely on:

- handle merging
- soft-close default detection
- material alignment
- room retention/clear-only behavior
- table rows and raw text fallback
- appliance model extraction

## Fast Path Wiring

Only after JSON/XLSX spike and adapter audit:

1. Add an Evoca adapter that converts Evoca-native JSON to the existing snapshot contract.
2. Add a guarded `_build_evoca_fast_snapshot(...)` next to `_build_imperial_v6_fast_snapshot(...)` in `App/services/extraction_service.py`.
3. In `build_spec_snapshot(...)`, try the Evoca fast path before `_apply_layout_pipeline(...)`, mirroring the Imperial v6 early-return pattern.
4. If Evoca fast path fails or produces no usable source rows, fall back to the existing legacy pipeline.
5. Do not remove Evoca from `SPEC_EXTRACTION_DOCLING_BUILDERS` until the fast path has passed source-PDF verification.

The runtime builder-level Docling switch is `SPEC_EXTRACTION_DOCLING_BUILDERS`, not `PAGE_FAMILY_PROVIDER_MATRIX["evoca"][...]["docling_default"]`.

## Tests

Spike tests:

- Unit-test section detection for `15 CABINETS`, `17 APPLIANCES`, `20 PLUMBING FIXTURES & TAPWARE`, and `23 TILING / HARD FLOORING`.
- Unit-test room header detection for Kitchen/Laundry/Bathroom/Ensuite variants.
- Unit-test group anchor detection from `-` rows.
- Unit-test that `Not Applicable` rows are preserved in JSON.
- Unit-test multi-page section carry-forward.
- Unit-test Excel export creates expected sheets and columns.

Regression tests:

- Existing Evoca table recovery tests must still pass:
  - `tests/smoke_test.py:15594`
  - `tests/smoke_test.py:22737`
  - `tests/smoke_test.py:22859`
  - `tests/smoke_test.py:23314`
- The full suite should be run before production wiring.

Suggested initial commands:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/smoke_test.py -k "evoca" -q
.\.venv\Scripts\python.exe -m pytest tests/ -q
```

## Verification Gates

Gate 1 - Raw extractor spike:

- Run the export tool on both supplied PDFs.
- Open the generated Excel files and compare against the source PDFs.
- Record parser runtime for each file.

Gate 2 - Schema review:

- Document `sections -> rooms -> groups -> rows` in `docs/EVOCA_STRUCTURED_SCHEMA_v0.md`.
- Record known edge cases and which layer owns each cleanup.

Gate 3 - Adapter audit:

- Complete the current-finalizer mapping table.
- Confirm no existing Evoca field depends on Docling-only markdown.

Gate 4 - Fast path implementation:

- Add guarded early path.
- Keep legacy pipeline fallback.
- Do not change production env yet.

Gate 5 - Source-PDF acceptance:

- Verify fields against the PDFs, not only old webpages or old snapshots.
- Minimum fields to check:
  - benchtops
  - door colours
  - handles
  - sinks/basins/taps
  - flooring
  - appliances

Gate 6 - Deployment:

- Deploy only after local tests and source-PDF review pass.
- Restart web and worker services.
- Verify `/api/health`.
- Rerun affected live Evoca jobs only after the implementation is confirmed.

## Documentation Sync

This task spec alone is not a major behavior change.

When the fast path is implemented, update the mandatory docs together:

- `PRD.md`
- `Arch.md`
- `Project_state.md`
- `AGENTS.md`

Likely doc additions:

- Evoca uses an Evoca-native structured JSON truth layer.
- Raw Evoca structured Excel export is the parser QA artifact.
- Evoca fast path skips Docling when the native structured parser succeeds.
- Docling remains fallback only until source-PDF verification is complete.

## Stop Conditions

Stop and report before production wiring if any of these occur:

- The structured JSON cannot preserve section/room/group boundaries for either supplied PDF.
- The Excel export is not close enough to the source PDF for manual QA.
- `Not Applicable` or blank rows disappear from the source JSON.
- Multi-page section carry-forward assigns rows to the wrong section or room.
- Existing Evoca tests fail and the failure is not clearly explained by the new source-backed behavior.
- Adapter audit finds a current Evoca output that cannot be derived from the Evoca-native JSON.

