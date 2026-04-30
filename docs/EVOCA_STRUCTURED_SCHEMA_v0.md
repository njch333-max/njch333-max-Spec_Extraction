# Evoca Structured Schema v0

Status: Draft schema and spike audit notes. This document describes the raw parser artifact used by `tools/evoca_structured_export.py`; it is not yet wired into production snapshot generation.

## Purpose

Evoca PDFs should be parsed into an Evoca-native structure before any app-level flattening. The source shape is:

```text
section -> room -> group -> label/value rows
```

This is intentionally different from Imperial v6's five-column material row model. The parser should preserve source rows first; adapter/finalizer code can later derive `bench_tops`, `door_colours`, `handles`, fixtures, flooring, and appliances.

## Top-Level Shape

```json
{
  "source_pdf": "...",
  "document_name": "...",
  "builder": "Evoca",
  "schema_version": "evoca_structured_v0",
  "pages": [],
  "sections": [],
  "unstructured_pages": [],
  "statistics": {}
}
```

## Section

Each section corresponds to a visible numbered Evoca heading such as `15 CABINETS` or `20 PLUMBING FIXTURES & TAPWARE`.

```json
{
  "section_code": "15",
  "section_title": "15 CABINETS",
  "section_order": 1,
  "page_start": 8,
  "page_end": 10,
  "rooms": [],
  "groups": [],
  "notes": []
}
```

Sections with room cards, such as `15 CABINETS` and `20 PLUMBING FIXTURES & TAPWARE`, place groups under `rooms[]`. Section-level schedules, such as `17 APPLIANCES`, may place groups directly under `section.groups[]` with a blank room in the Excel QA export.

## Room

Room rows are visible table boundary rows such as `Kitchen`, `Laundry`, `Bathroom`, and `Ensuite`.

```json
{
  "room_label": "Kitchen",
  "room_key": "kitchen",
  "page_start": 8,
  "page_end": 8,
  "groups": [],
  "notes": []
}
```

Room notes preserve unanchored rows that appear inside a room before the next group, for example builder notes about shelves or trough positions.

## Group

A group is normally a row with a leading `-` cell and a grouped label cell.

```json
{
  "group_label": "Benchtops",
  "page_start": 8,
  "page_end": 8,
  "raw_rows": [],
  "value_lines": [],
  "rows": []
}
```

Examples:

- `Benchtops`
- `Underbench including Island`
- `Overhead Cupboards`
- `Pantry Doors`
- `Appliances`
- `Sink`
- `Sink Mixer`
- `Basin`
- `Basin Mixer`

## Row

Rows preserve the parsed label/value pair and the raw table evidence used to derive it.

```json
{
  "label": "Manufacturer",
  "value": "Quantum Quartz",
  "page_no": 8,
  "row_order": 6,
  "table_index": 0,
  "row_index": 5,
  "raw_cells": ["-", "Benchtops\nManufacturer\nColour", "Quantum Quartz\nChampagne"],
  "source_rows": [],
  "source_method": "pdfplumber_table"
}
```

`Not Applicable`, `#N/A`, blank values, and continuation values are intentionally preserved. The raw parser layer must not suppress them.
The parser must not emit a literal business label named `Continuation`. Source-backed overflow text is either appended to the owning prior row when the PDF evidence supports that ownership, or preserved as a diagnostic row:

```json
{
  "label": "Unassigned Source Text",
  "value": "WC",
  "is_diagnostic": true,
  "source_method": "pdfplumber_table"
}
```

Rows whose value is filled from the secondary text-aligned table pass use:

```json
"source_method": "pdfplumber_text_rescue"
```

Rows whose value is filled from the bounded raw-text word layer use:

```json
"source_method": "pdfplumber_raw_text_fallback"
```

Rows that belong to a group synthesized from exact raw-text evidence across a page boundary use:

```json
"source_method": "pdfplumber_raw_text_cross_page"
```

The rescue pass only fills empty parser values and leaves existing table-derived values intact.

When the visible group heading itself has a value, the parser promotes that value to a group anchor row:

```json
{
  "label": "Overhead Cupboards",
  "value": "* Overhead Cupboard above Oven to be Push to Open",
  "is_group_anchor": true,
  "source_method": "pdfplumber_text_rescue"
}
```

Child rows remain row-local beneath that anchor, for example `Manufacturer`, `Colour & Finish`, and `Handles`.

## Boundary Rules

Section detection:

- Known section titles are matched from table text.
- Current section carries forward across later pages until another section title is encountered.
- Mixed pages can contain more than one section. The parser switches section at the visible heading row.

Room detection:

- A room boundary is a known Evoca room label in the second cell with no meaningful value cell.
- The parser does not treat room boundaries as material rows.
- Single-line room notes before a known group boundary remain room notes. For example, `No shelf to cupboard underneath sink` must not become a parent group that swallows the following `Benchtops` group.

Group detection:

- A group boundary is a row whose first cell is `-` and whose second cell has a label.
- Child labels come from the remaining lines in the group label cell.
- Values come from the group value cell plus following continuation rows until the next section, room, or group boundary.
- Extra value lines must not become a literal `Continuation` business label. Known source-backed wraps such as `Extent` second lines are appended to the prior row; unsafe extras remain `Unassigned Source Text` diagnostics and are ignored by text rescue / shift override passes.
- Some Evoca rows visually start a new group without a leading `-`, for example `Overhead Cupboards` under cabinets. v0 detects these as unanchored groups when the label cell has more lines than the value cell.
- Terminal group values such as `Not Applicable`, `Not Included`, `Not Required`, `N/A`, `#N/A`, and `TBC` apply to the group anchor row. Child property rows remain present with blank values so the Excel QA workbook stays close to the source PDF.
- Non-terminal group-level values are detected from the secondary text-grid lookup when `group_label -> value` exists on the same source row. The group value becomes an `is_group_anchor` row and child property rows are realigned from the text-grid lookup.
- The secondary rescue lookup uses `pdfplumber` with line-based vertical boundaries and text-based horizontal boundaries. It is a value backfill, not a new section/room detector.
- When the text-grid pass exposes group headings, rescue candidates are bounded to the matching group block before falling back to page-wide lookup. Generic labels such as `Model`, `Type`, `Location`, `Handles`, and `Colour` must not be reused across later groups on the same page.
- If both the table pass and text-grid pass miss a value, the raw-text fallback may fill the blank row only inside that group bbox. It matches exact current-group labels, prefers same-line values, may use the immediate next line only when that line is not another current-group label, and rejects footer noise such as `Page ... Client Initials`.
- If the table layer drops a group anchor at a page edge, the cross-page raw-text pass may synthesize a missing group only from exact known Evoca group labels and exact known child labels. Current allowed synthesis is narrow: `Benchtops` and `Basin Mixer`.
- Diagnostics include `raw_text_fallback_groups`, `raw_text_fallback_pairs_filled`, `raw_text_cross_page_groups`, and `raw_text_cross_page_pairs_filled` so QA can see when the raw-text layers changed the artifact.

Color and visual styling:

- v0 does not require cell background color.
- Color may be added later as diagnostics if stable, but correctness cannot depend on it.

## Excel QA Workbook

`tools/evoca_structured_export.py` writes one workbook per source PDF. Each detected section gets a sheet when possible.

Columns:

- `Page`
- `Source Row`
- `Room`
- `Group`
- `Label`
- `Value`
- `Raw Cells`
- `Source Method`

The workbook is intended for manual source-PDF comparison. It is not the formal app export.

## Adapter Audit

Before this schema is wired into `build_spec_snapshot()`, the current Evoca finalizer inputs must be mapped to schema paths.

| Snapshot field | Current source | New Evoca JSON source | Adapter owner |
|---|---|---|---|
| `bench_tops_wall_run` | `_evoca_collect_room_recovery_data_from_tables()` / text fallback | `15 CABINETS -> room -> Benchtops` | Resolve manufacturer, colour, edge profile |
| `bench_tops_island` | table/text recovery | `15 CABINETS -> room -> Benchtops` | Resolve `Island Colour` and `Island Edge Profile`, including `As Above` |
| `bench_tops_other` | table/text recovery | `15 CABINETS -> room -> Benchtops` | Non-kitchen room benchtop formatting |
| `door_colours_base` | `Underbench` mapping | `15 CABINETS -> room -> Underbench` | Suppress placeholders in adapter/finalizer only |
| `door_colours_overheads` | `Overhead Cupboards` mapping | `15 CABINETS -> room -> Overhead Cupboards` | Preserve handle split separately |
| `door_colours_tall` | `Pantry Doors` mapping | `15 CABINETS -> room -> Pantry Doors` | Preserve kickboard separately |
| `toe_kick` | Underbench/Pantry `Kickboard` | `15 CABINETS -> room -> Underbench/Pantry Doors` | Apply material cleanup later |
| `handles` | Underbench/Overheads/Pantry handle rows | same group rows | Existing handle merge rules must be re-derived |
| `drawers_soft_close` / `hinges_soft_close` | global Evoca soft-close note | section notes / raw page text | May remain raw-text derived in adapter |
| `sink_info` | plumbing table/text recovery | `20 PLUMBING -> room -> Sink/Tub` | Room-family mapping belongs in adapter |
| `basin_info` | plumbing table/text recovery | `20 PLUMBING -> room -> Basin` | Do not emit wet-area fixture noise as room-retention evidence |
| `tap_info` | plumbing table/text recovery | `20 PLUMBING -> room -> Sink Mixer/Tub Mixer/Basin Mixer` | Keep location row local |
| `flooring` | text-block extraction | `23 TILING / HARD FLOORING` groups | v0 can keep current text-block fallback |
| `splashback` | text-block extraction | `23 TILING / HARD FLOORING` / `24 GLASS SPLASHBACK` | Keep raw evidence visible |
| `appliances` | appliance table/text recovery | `17 APPLIANCES -> Appliances` | Official lookup remains separate enrichment |

Existing behavior that must not be lost:

- table rows and raw-text fallback can both contribute to Evoca recovery
- handle entries are cleaned and merged
- global soft-close notes populate cabinetry rooms
- `_clear_only` behavior removes placeholder-only rooms such as non-selected Butlers/Powder variants
- appliance extraction ignores hot water, water filter, air-conditioning, and similar non-appliance rows
- flooring and splashback recovery currently use text blocks where table rows are insufficient

## Spike Artifacts

The first spike should run:

```powershell
.\.venv\Scripts\python.exe tools\evoca_structured_export.py "C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38117\EVOC473 (Lot 403 Sehmish - Color Selection Document) 20251107090209080v08.pdf" "C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38225\EVOC467 (Lot 1038 Oyster - COLOUR SELECTION DOCUMENT) 20251111125911918v06.pdf"
```

Expected artifact directory:

```text
tmp/evoca_structured/
```
