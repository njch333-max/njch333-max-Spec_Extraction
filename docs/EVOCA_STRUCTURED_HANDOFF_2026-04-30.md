# Evoca Structured Parser Handoff - 2026-04-30

## Context

Repository:

```text
C:\Users\Jason Niu - XM\Python Project\Spec_Extraction
```

Current work is a standalone Evoca structured parser spike. It is **not wired into production snapshot generation**, `build_spec_snapshot()`, `_finalize_evoca_rooms()`, or `runtime.py`.

Current key files:

- `App/services/evoca_structured_extractor.py`
- `tools/evoca_structured_export.py`
- `tests/test_evoca_structured_extractor.py`
- `docs/EVOCA_STRUCTURED_SCHEMA_v0.md`

Current worktree still has uncommitted parser changes:

- `App/services/evoca_structured_extractor.py`
- `docs/EVOCA_STRUCTURED_SCHEMA_v0.md`
- `tests/test_evoca_structured_extractor.py`

There are also unrelated untracked task docs and Claude reference tools in the worktree. Do not delete or revert them unless Jason explicitly asks.

## Hard Constraints

- Do not connect this parser to production fast path yet.
- Do not edit `extraction_service.py`, `_finalize_evoca_rooms()`, `runtime.py`, or SnapshotPayload code during the next bug-fix pass.
- Do not expand `SECTION_TITLE_PATTERNS` beyond Evoca sections 15-25 unless Jason explicitly approves.
- Source PDF remains the parser acceptance source of truth. Do not sign off based only on old JSON or Excel.
- Keep this as standalone parser work until EVOC447 issues are resolved.
- Prefer targeted parser fixes over adapter cleanup. Wrong values in structured JSON must be fixed in parser, not hidden downstream.
- Use `.venv` for tests and exports:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\ -x
.\.venv\Scripts\python.exe tools\evoca_structured_export.py --out-dir <dir> "<pdf>"
```

Note: `rg.exe` may fail with Access denied on this Windows environment. Use PowerShell `Select-String` / `Get-ChildItem -Recurse` fallback.

## Source PDFs Used

EVOC473:

```text
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38117\EVOC473 (Lot 403 Sehmish - Color Selection Document) 20251107090209080v08.pdf
```

EVOC467:

```text
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38225\EVOC467 (Lot 1038 Oyster - COLOUR SELECTION DOCUMENT) 20251111125911918v06.pdf
```

EVOC447 / job 38148:

```text
C:\Users\Jason Niu - XM\Downloads\38148 - EVOC447 (Lot 1042 Rufous - COLOUR SELECTION DOCUMENT).pdf
```

## What Has Been Built

Standalone extractor:

- Parses Evoca PDFs into source-native JSON:

```text
section -> room -> group -> label/value rows
```

- Exports QA Excel workbooks with `_summary` sheet, section tabs, room banners, anchor rows, row-type coloring, and terminal value styling.
- Current schema version remains:

```text
evoca_structured_v0
```

Current export command pattern:

```powershell
.\.venv\Scripts\python.exe tools\evoca_structured_export.py --out-dir tmp\evoca_structured_<name> "<PDF path>"
```

Latest EVOC447 output:

```text
tmp\evoca_structured_38148\38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.json
tmp\evoca_structured_38148\38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.xlsx
```

EVOC447 run result:

```text
3.15s
sections=11
rooms=18
groups=77
rows=285
diagnostics:
  shift_override_groups=5
  shift_overrides_applied=20
  shift_clears_applied=9
  anchor_value_groups=1
  anchor_values_promoted=1
  anchor_value_child_realignments=8
```

## Fixes Already Implemented

| Fix | Status | Summary | Main Validation |
|---|---:|---|---|
| Initial v0 extractor | Done | `pdfplumber` table-first structured JSON and Excel export | EVOC467/EVOC473 export in ~3s |
| Bug 1 | Done | Terminal group handling and unanchored group split, e.g. EVOC467 Kitchen `Contrasting Facings` vs `Overhead Cupboards` | Contrasting Facings stays `Not Applicable`; Overhead Cupboards separate |
| Bug 2 | Done | Text-strategy value rescue for rows where line-grid loses values | EVOC473 temporary Section 1 House values mostly rescued |
| Bug 3 | Done | Under-supplied non-terminal group shift override using text-grid lookup | EVOC467 Section 17 Appliances exact match |
| Excel refresh | Done | `_summary`, room banners, `Anchor`, `Source Text`, row colors | JSON byte-identical to Bug 3 reference |
| Bug 4 | Done | Promote group heading value to `is_group_anchor` row when text-grid has `group_label -> value` | EVOC473 Kitchen Overhead Cupboards fixed |

Last full test after Bug 4:

```text
.\.venv\Scripts\python.exe -m pytest tests\ -x
1016 passed
```

`git diff --check` passed with only LF/CRLF warnings.

## Current Bug Inventory

| Bug | Status | Root Problem | Example | Risk | Suggested Order |
|---|---:|---|---|---|---:|
| Bug 4 | Fixed, keep regression | Group heading has its own value but old parser had no anchor slot | EVOC473 Kitchen `Overhead Cupboards = * Overhead Cupboard above Oven...` | Medium | 0 |
| Bug 5 | Fixed 2026-04-30 | Unanchored parent/group heuristic was too broad and treated room notes as group headers | EVOC447 Kitchen `No shelf to cupboard underneath sink` swallowed `Benchtops` | High | 1 |
| Bug 6 | Fixed 2026-04-30 | Page-level rescue pool reused candidates across groups; table-filled rows did not consume text candidates | EVOC447 Bathroom `Basin Mixer Type = Overmount`, `Bath Mixer Model = Eden Bench Mount...`, handles cross-contamination | Highest | 2 |
| Bug 8 | Fixed 2026-04-30 | Extra values became fake business row label `Continuation` | `Continuation = WC`, `Gunmetal`, `Splashback window**` | Medium-high | 3 |
| Bug 7 | Fixed 2026-04-30 | Text-strategy missed valid label/value pairs; bounded raw-text fallback now fills source-backed blank rows | EVOC447 multiple Benchtops `Colour` rows blank; Shower Mixer/Rail blank | Medium-high | 4 |
| Bug 9 | Fixed 2026-04-30 | `pdfplumber` table rows drop group anchors at page edges; first values on the next page become notes or diagnostics | EVOC473 `Powder / Benchtops`, `Ensuite 2 / Basin Mixer`, `Ensuite 5 / Basin Mixer` | High | 5 |

## EVOC447 Evidence Already Confirmed

Bug 5 confirmed:

- Page 8 raw table has:

```text
['', 'No shelf to cupboard underneath sink', None, None]
['-', 'Benchtops\nManufacturer\nColour\nIsland Colour\nEdge Profile\nIsland Edge Profile\nWaterfall End to Island', None, None]
```

- Current parser incorrectly creates group:

```text
group_label = "No shelf to cupboard underneath sink"
```

Expected:

- `No shelf to cupboard underneath sink` should be a room note.
- Next group should be `Benchtops`.

Bug 6 confirmed:

- EVOC447 Bathroom `Basin` has table-filled:

```text
Model = Eden Bench Mount Gloss White (FL135-W)
Type = Overmount
```

- Later `Basin Mixer` incorrectly rescues:

```text
Type = Overmount
Location = Centre of Basin
```

PDF truth for `Basin Mixer Type` is `Alder 54082 Brushed Nickel`.

Cause: page-level text lookup pool still contains candidates already consumed by table-derived rows. Later groups pop stale candidates for generic labels such as `type`, `model`, `handles`.

Bug 4 still working:

- EVOC473 Kitchen `Overhead Cupboards` now outputs:

```text
Overhead Cupboards = * Overhead Cupboard above Oven to be Push to Open  [ANCHOR]
Manufacturer = Polytec
Colour & Finish = Rojo Walnut Woodmatt
Handles = Finger Grip
```

Bug 9 confirmed and fixed:

- EVOC473 page 11/12 source PDF has `Powder -> Benchtops -> Manufacturer Quantum Quartz / Colour Polar / Edge Profile 20mm Arissed`, but the table layer dropped the `Benchtops` anchor at the page edge and previously emitted `Polar` / `20mm Arissed` as room notes.
- EVOC473 page 13/14 source PDF has `Ensuite 2 -> Basin Mixer -> Type Spin Gun Metal Tall Basin Mixer (SP110-GM) / Location Centre of Basin`, but the table layer dropped the `Basin Mixer` label and previously left the product as an unsafe diagnostic under `Basin`.
- EVOC473 page 15/16 has the same `Ensuite 5 -> Basin Mixer` split and is fixed by the same narrow rule.
- The fix synthesizes only exact source-backed `Benchtops` / `Basin Mixer` groups from raw-text group and child-label evidence, with `source_method = pdfplumber_raw_text_cross_page`.
- Same-room stale notes or diagnostics that held the recovered values are removed after synthesis to avoid duplicates.

## Recommended Next Work

Do **not** proceed to adapter or fast path.

Completed parser pass on 2026-04-30:

1. Bug 5 fixed: room notes no longer become broad unanchored group headers.
2. Bug 6 fixed: rescue is group-bounded so generic labels do not reuse stale values from earlier groups.
3. Bug 7 fixed: bounded raw-text fallback fills source-backed values missed by table and text-grid extraction.
4. Bug 8 fixed: literal `Continuation` rows are removed; safe wraps are merged and unsafe extras become `Unassigned Source Text` diagnostics.
5. Bug 9 fixed: narrow cross-page raw-text synthesis restores table-dropped `Benchtops` and `Basin Mixer` groups.

Next focused task should remain evidence-first and standalone-parser only:

- Do not proceed to adapter or fast path without explicit approval.
- If another issue is raised, confirm the source PDF and current `tmp/evoca_structured_bug9/` JSON before changing code.

## Suggested Next Chat Prompt

Copy this into the next chat:

```text
We are in C:\Users\Jason Niu - XM\Python Project\Spec_Extraction.

Please read docs\EVOCA_STRUCTURED_HANDOFF_2026-04-30.md first.

Current task: continue the standalone Evoca structured parser work. Do not wire it into production, do not edit extraction_service.py, _finalize_evoca_rooms(), runtime.py, or SnapshotPayload code.

Use the current modified files as the latest baseline:
- App/services/evoca_structured_extractor.py
- docs/EVOCA_STRUCTURED_SCHEMA_v0.md
- tests/test_evoca_structured_extractor.py

Focus on Bug 5 and Bug 6 only:

Bug 5:
- EVOC447 page 8 row "No shelf to cupboard underneath sink" is a room note.
- Current parser incorrectly treats it as a group header and swallows Benchtops.
- Fix unanchored parent/group detection so the next group is Benchtops and the note stays a note.

Bug 6:
- Current rescue uses page-level label_key pools and reuses stale candidates across groups.
- Bathroom Basin table-filled Model/Type values remain in the text rescue pool and then contaminate Basin Mixer / Bath Mixer / Shower rows.
- Redesign rescue consumption to be group-bounded or otherwise synchronized so table-filled rows consume their matching text candidates before later rescue runs.

Acceptance sources:
- C:\Users\Jason Niu - XM\Downloads\38148 - EVOC447 (Lot 1042 Rufous - COLOUR SELECTION DOCUMENT).pdf
- Existing EVOC467 and EVOC473 PDFs must not regress.

Generate new outputs to:
tmp\evoca_structured_bug6\

Run:
.\.venv\Scripts\python.exe -m pytest tests\ -x
git diff --check

Do not proceed to adapter or fast path. Source PDF is the acceptance source of truth.
```
