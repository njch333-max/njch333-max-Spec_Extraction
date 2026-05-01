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

Additional pressure-test PDFs:

```text
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38324\38324 - EVOC471 (Lot 214 Sora - COLOUR SELECTION DOCUMENT).pdf
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38335\38335 EVOC482 (Lot 1097 Harbour - COLOUR SELECTION DOCUMENT).pdf
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38208\38208 - EVOC436 (Lot 1850 Streambed - Colour Selection Document).pdf
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38213\38213 - EVOC449 (Lot 1900 Streambed - Colour Selection Document).pdf
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38337\38337 - EVOC479 (Lot 1870 Dewdrop - COLOUR SELECTION DOCUMENT).pdf
C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38338\38338 - EVOC480 (Lot 1842 Streambed - COLOUR SELECTION DOCUMENT).pdf
```

## What Has Been Built

Standalone extractor:

- Parses Evoca PDFs into source-native JSON:

```text
section -> room -> group -> label/value rows
```

- Exports QA Excel workbooks with `_summary` sheet, section tabs, room banners, anchor rows, row-type coloring, and terminal value styling.
- Recognizes Evoca sections 16/18/19/21/22 as boundaries but intentionally skips them from standalone JSON and QA workbook output because those Electrical, Air-conditioning, Plumbing & Gas, Mirrors, and Window Furnishings sections are out of the current downstream review scope.
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
tmp\evoca_structured_section_filter\38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.json
tmp\evoca_structured_section_filter\38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.xlsx
```

EVOC447 run result:

```text
3.87s
sections=6
rooms=18
groups=62
rows=241
diagnostics:
  shift_override_groups=4
  shift_overrides_applied=14
  shift_clears_applied=4
  anchor_value_groups=1
  anchor_values_promoted=1
  anchor_value_child_realignments=8
  raw_text_fallback_groups=20
  raw_text_fallback_pairs_filled=32
  raw_text_anchor_synthesized_same_page_groups=0
  raw_text_anchor_synthesized_same_page_pairs_filled=0
  raw_text_anchor_synthesized_cross_page_groups=0
  raw_text_anchor_synthesized_cross_page_pairs_filled=0
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
| Bug 10 | Fixed 2026-04-30 | Terminal group values with source-native suffixes were not promoted to group anchors and were later cleared by rescue | EVOC482 Kitchen/Butlers `Benchtops = Not Applicable - by owner after handover`; EVOC471 `Carpets = Client to supply & install after handover` | Medium-high | 6 |
| Bug 11 | Fixed 2026-04-30 | Raw-text group cursor could fall behind when terminal/skipped groups returned before advancing the cursor; same-line label words inside value-column product names could also truncate values | EVOC471 page 10 `Powder / Benchtops` and `Powder / Underbench` blank; EVOC471 `Toilet Suite` / appliances product names truncated | High | 7 |
| Bug 12 | Fixed 2026-04-30 | Wrapped value cells needed pairing with following label-only continuation rows; `Drawers` was also missing from raw-text group boundaries | EVOC482 Kitchen/Butlers `Drawers` `Standard` / `Pot` / `Bin` | Medium-high | 8 |
| Bug 13 | Fixed 2026-05-01 | A no-dash group subheading with an empty value cell was swallowed by the previous non-terminal group | EVOC436 `Bathroom / Shower` became `Bath Mixer / Spout` diagnostics | High | 9 |
| Bug 14 | Fixed 2026-05-01 | `pdfplumber` emitted dash rows with blank label cells, so same-page group anchors were lost and values became diagnostics or notes under the previous group | EVOC449 `Underbench` and `Accessories & Toilet Suite`; EVOC482 `Bathroom / Underbench` | High | 10 |
| Bug 15 | Fixed 2026-05-01 | Group-level values with no child labels could wrap across multiple lines and become `Unassigned Source Text` diagnostics instead of one anchor value | EVOC479 Section 16 `Alarm System` | Medium | 11 |

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
- The fix synthesizes only exact source-backed groups from raw-text group and child-label evidence, now marked with `source_method = pdfplumber_raw_text_anchor_synthesis`.
- Same-room stale notes or diagnostics that held the recovered values are removed after synthesis to avoid duplicates.

Bug 10 confirmed and fixed:

- EVOC482 page 8/9 has `Kitchen` and `Butlers` `Benchtops` rows whose only value is `Not Applicable - by owner after handover`.
- EVOC471 page 15 has group-level flooring/carpet terminal values such as `Client to supply & install after handover`.
- These narrow extended terminal values now become `is_group_anchor` rows. Child rows stay present with blank values.
- EVOC447/EVOC467/EVOC473 business row diff against the Bug 9 reference is zero.

Bug 11 evidence:

- Evidence dump: `tmp/evoca_bug11_evidence/evoc471_page10_powder_evidence.json` and `.txt`.
- Page 10 raw-text blocks contain the missing Powder values, including `Manufacturer Quantum Quartz`, `Colour Verona Gold WK Stone`, `Manufacturer Polytec`, and `Handles Client to supply & install after handover`.
- The failure is cursor alignment: terminal/skipped `Ensuite 2` groups return before advancing the raw-text cursor, so `Powder` consumes an empty `Ensuite 2` block instead of its own block.
- The same cursor fix also restores EVOC471 page 13 `Ensuite / Accessories` values. Raw-text fallback now keeps label-like words inside the value column, so product text such as `Lana Rimless Back to Wall Toilet Suite Gloss White (6002-R-W)` is not truncated at the inner `Toilet Suite` words.

Bug 12 confirmed and fixed:

- EVOC482 page 8 `Kitchen / Drawers` table row has three wrapped value lines in the value cell, followed by label-only `Standard`, `Pot`, and `Bin` rows. These are now paired as child rows rather than emitted as an anchor plus `Unassigned Source Text` diagnostics.
- EVOC482 page 9 `Butlers / Drawers` has `Standard`, `Pot`, and `Bin` values in raw text but missing from the table extraction. Adding `Drawers` to group-boundary detection lets raw-text fallback fill the correct Butlers block instead of borrowing the later Laundry drawer value.
- The same source-backed raw-text boundary fix restores blank `Butlers / Drawers` values in EVOC447 and EVOC471.

Bug 13 confirmed and fixed:

- EVOC436 page 13 `Bathroom / Bath Mixer / Spout` has real `Model` and `Bath Spout Model` values, followed by a no-dash `Shower` subheading whose value cell is blank and whose values appear on following value-only rows.
- The table-layer group boundary detector now treats known no-dash group labels with child labels as unanchored groups even when the value cell is empty.
- `Bathroom / Shower` now owns `Mixer`, `Shower Rail / Rose`, `Shower Screen`, and `Shower Screen Colour`; `Bath Mixer / Spout` no longer carries these as `Unassigned Source Text`.

Bug 14 confirmed and fixed:

- EVOC449 pages 8-10 have dash rows where the dash cell survives but the group label cell is blank, for example `['-', None, 'Polytec']` where source PDF text shows `- Underbench`.
- EVOC449 page 13 has the same failure for `Accessories & Toilet Suite`; product values previously spilled into `Shower` diagnostics and note rows.
- Raw-text anchor synthesis now supports same-page and cross-page missing anchors for `Accessories`, `Accessories & Toilet Suite`, `Basin Mixer`, `Benchtops`, `Underbench`, and `Underbench including Island`, using exact known child labels only.
- Diagnostics are split into `raw_text_anchor_synthesized_same_page_*` and `raw_text_anchor_synthesized_cross_page_*`; legacy `raw_text_cross_page_*` counters now represent true cross-page synthesis only.

Bug 15 confirmed and fixed:

- EVOC479 page 12 has `Alarm System` as a group-level value with no child labels. The value cell wraps over five physical lines:
  `1 x Paradox MG5050 alarm system...`, `siren...`, `Keypad...`, `commission...`, `house`.
- The previous output kept only the first line on the `Alarm System` anchor and emitted the remaining four lines as `Unassigned Source Text`.
- Groups with no child labels now merge all wrapped value lines into one `is_group_anchor` row. Existing child-label groups are not affected.
- EVOC479 `Unassigned Source Text` count is now 0; EVOC480 is unchanged by row-level diff.

## Recommended Next Work

Do **not** proceed to adapter or fast path.

Completed parser pass on 2026-04-30:

1. Bug 5 fixed: room notes no longer become broad unanchored group headers.
2. Bug 6 fixed: rescue is group-bounded so generic labels do not reuse stale values from earlier groups.
3. Bug 7 fixed: bounded raw-text fallback fills source-backed values missed by table and text-grid extraction.
4. Bug 8 fixed: literal `Continuation` rows are removed; safe wraps are merged and unsafe extras become `Unassigned Source Text` diagnostics.
5. Bug 9 fixed: narrow cross-page raw-text synthesis restores table-dropped `Benchtops` and `Basin Mixer` groups.
6. Bug 10 fixed: narrow extended terminal group values are promoted to anchor rows instead of being cleared.
7. Bug 11 fixed: raw-text cursor advances through terminal/skipped repeated groups, and label-like product wording in the value column is preserved.
8. Bug 12 fixed: Drawers `Standard` / `Pot` / `Bin` wrapped values are paired correctly and bounded raw-text fallback owns repeated Drawers groups.
9. Bug 13 fixed: no-dash empty-value group subheadings such as EVOC436 `Bathroom / Shower` split correctly after a non-terminal group.
10. Bug 14 fixed: same-page blank-label dash rows synthesize exact source-backed `Underbench` / `Accessories` groups instead of leaving values as diagnostics or notes.
11. Bug 15 fixed: no-child-label group anchors merge wrapped multiline values instead of leaking the tail into `Unassigned Source Text`.

Next focused task should remain evidence-first and standalone-parser only:

- Do not proceed to adapter or fast path without explicit approval.
- Next candidate work should come from new PDF evidence or a dedicated draft PR review; do not start adapter or fast path wiring from the standalone JSON alone.

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

Current state:
- Bugs 1-15 are fixed in the standalone Evoca parser.
- Latest output directory is tmp\evoca_structured_bug15\.
- Nine pressure PDFs have been used: EVOC447, EVOC467, EVOC471, EVOC473, EVOC482, EVOC436, EVOC449, EVOC479, and EVOC480.
- Do not start adapter / fast path work unless Jason explicitly approves.

Acceptance sources:
- C:\Users\Jason Niu - XM\Downloads\38148 - EVOC447 (Lot 1042 Rufous - COLOUR SELECTION DOCUMENT).pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38225\EVOC467 (Lot 1038 Oyster - COLOUR SELECTION DOCUMENT) 20251111125911918v06.pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38117\EVOC473 (Lot 403 Sehmish - Color Selection Document) 20251107090209080v08.pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38324\38324 - EVOC471 (Lot 214 Sora - COLOUR SELECTION DOCUMENT).pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38335\38335 EVOC482 (Lot 1097 Harbour - COLOUR SELECTION DOCUMENT).pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38208\38208 - EVOC436 (Lot 1850 Streambed - Colour Selection Document).pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38213\38213 - EVOC449 (Lot 1900 Streambed - Colour Selection Document).pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38337\38337 - EVOC479 (Lot 1870 Dewdrop - COLOUR SELECTION DOCUMENT).pdf
- C:\Users\Jason Niu - XM\Desktop\Builder\Evoca\38338\38338 - EVOC480 (Lot 1842 Streambed - COLOUR SELECTION DOCUMENT).pdf

Next recommended action:
- Open a draft PR / review checkpoint for the standalone parser branch, or run 2-3 more new EVOC PDFs as pressure tests before adapter design.

Run:
.\.venv\Scripts\python.exe -m pytest tests\ -x
git diff --check

Do not proceed to adapter or fast path. Source PDF is the acceptance source of truth.
```
