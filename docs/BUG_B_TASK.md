# Task — Bug B: split v6 extractor items flagged with `_review_hint` (missing row separator)

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude (after codex delivers)
**Target layer**: (a) — `App/services/pdf_to_structured_json.py`
**Prerequisite**: commit `ef1f01b` (Bug 6 fix) already merged. Do not revert or conflict with it.

## Context — read before editing

When an Imperial joinery selection PDF has multiple `AREA / ITEM` rows whose horizontal row separator is missing, pdfplumber's grid detection returns them as **a single table cell containing multi-line text**. The extractor already detects this pattern at [pdf_to_structured_json.py:691-699](../App/services/pdf_to_structured_json.py#L691-L699) and attaches a `_review_hint`:

```
"AREA contains multiple line items and SPECS has matching line count.
 Source PDF may be missing a row separator."
```

Currently the extractor **does not split** — it emits one item whose `area`, `specs`, `supplier` all contain `\n`-separated multi-line text. Downstream consumers then display a single merged row. Example — Job 76 KITCHEN page 1 has a real 4-row block `LED LIGHTING / PULL-OUT BIN / PULL-OUT SHELVES / PULL-OUT CORNER SHELVES`, each with its own specs and supplier, which currently emits as:

```json
{
  "area": "LED LIGHTING\nPULL-OUT BIN\nPULL-OUT SHELVES\nPULL-OUT CORNER SHELVES",
  "specs": "LED Provision ONLY to underside of OHC\n9291592 Waste Bin PO - 400mm 2x32Ltrs\nVSDSA.200.SSL.FG - VS Sub 200mm Wire\nST22MCU.450L.CPWH - Elka Magic Corner",
  "supplier": "Imperial\nHettich\nFurnware\nFurnware",
  "notes": "Incl Internal of Open Upper Cabinet",
  "image": "Location: Rear back",
  "_review_hint": "AREA contains multiple line items and SPECS has matching line count. Source PDF may be missing a row separator.",
  "_source": {"page": 1, "row_index": 8, "method": "grid"}
}
```

The correct output is 4 separate items, one per logical PDF row.

## Goal

Post-process cells that carry the `_review_hint` so they emit **N items** instead of **1 item**, when `area` and `specs` have matching newline counts. The split must preserve `_source` page number and assign a deterministic sub-index so ordering is stable.

## Approach (required — no alternative)

**Do the split inside `pdf_to_structured_json.py`, at the point where the record is about to be appended** (around [line 706](../App/services/pdf_to_structured_json.py#L706)). Do the same for the continuation-path in `extract_continuation_with_template` (around [line 535-538](../App/services/pdf_to_structured_json.py#L535-L538)) — the continuation path may emit the same merged-row pattern.

### Split rules (implement exactly)

Input: record with `area` (multi-line), `specs` (multi-line), `supplier`, `notes`, `image`, `_review_hint`, `_source`.

1. Only split when **all** of the following are true:
   - `_review_hint` is present and contains the substring `"missing a row separator"`
   - `area` splits into N ≥ 2 non-empty lines after stripping each line
   - `specs` splits into exactly N non-empty lines
2. If the conditions are not all met, **keep the original record unchanged** (still with its `_review_hint`). Do not touch it. Do not remove the hint.
3. When splitting:
   - For i in 0..N-1, emit a child record:
     - `area` = area_lines[i]
     - `specs` = specs_lines[i]
     - `supplier` = `supplier_lines[i]` if `supplier` has exactly N non-empty lines; otherwise **the full original `supplier` string** (do not silently drop it, do not partial-assign)
     - `notes` = the full original `notes` string on i=0 only; empty string on i>0. **Do not split notes by newline.** (Notes in this PDF pattern are per-cell, not per-row.)
     - `image` = the full original `image` string on i=0 only; empty string on i>0.
     - `_source` = `{page: original.page, row_index: f"{original.row_index}.{i}", method: "grid_split"}` (for grid path) or `method: "text_split"` (for continuation path, matching the original path's method label)
     - **Do not copy `_review_hint` to child records.** The hint was on the merged parent; children are the resolved form.
   - After emitting the N child records, **do not emit the original merged record**.
4. The child records must preserve the original page-level and section-level ordering: i.e. they replace the single original record in `page_record["items"]` and `current_section["items"]`, in order.

### Telemetry

Each child record MUST include `"_split_from_review_hint": True` at the top level (not inside `_source`). This lets downstream verification scripts count how many times split fired per PDF.

## Do NOT

- ❌ Do not change the `_review_hint` detection at line 691-699. Its current conservative condition (`area_lines >= 3 and specs_lines >= area_lines`) is the right gate.
- ❌ Do not lower the detection threshold from 3 to 2 or anything else.
- ❌ Do not change `clean_cell`, `smart_filter_y_edges`, `extract_page_grid`, or any geometry helper.
- ❌ Do not touch `App/services/imperial_v6_adapter.py`, `imperial_v6_room_fields.py`, `parsing.py`, or `main.py`.
- ❌ Do not reintroduce the `_review_hint` string on child records after splitting.
- ❌ Do not split on supplier newline count alone (supplier is optional signal, not the trigger).
- ❌ Do not run `tools/deploy_online.py`. Jason deploys.

## Tests (all must pass before delivery)

1. **New unit test** in `tests/test_pdf_extractor_split.py` (new file): use an inline fixture representing the Job 76 `LED LIGHTING/...` merged record (4 area lines, 4 specs lines, 4 supplier lines, 1 notes, 1 image). Feed it through your new split function. Assert:
   - Returns 4 records
   - `area` values are `["LED LIGHTING", "PULL-OUT BIN", "PULL-OUT SHELVES", "PULL-OUT CORNER SHELVES"]` in that order
   - `specs` values match the 4 lines in order
   - `supplier` values are `["Imperial", "Hettich", "Furnware", "Furnware"]`
   - `notes` is `"Incl Internal of Open Upper Cabinet"` on record 0, empty on 1/2/3
   - `image` is `"Location: Rear back"` on record 0, empty on 1/2/3
   - All 4 have `_split_from_review_hint == True`
   - None of the 4 have `_review_hint`
   - `_source.row_index` is `"8.0"`, `"8.1"`, `"8.2"`, `"8.3"` (or numeric equivalent — document your choice)
   - `_source.method` is `"grid_split"`
2. **New unit test** — "no-split" case: feed a record where `_review_hint` is present but `area` has 3 non-empty lines and `specs` has 2 non-empty lines (mismatch). Assert the function returns the original record unchanged, `_review_hint` still present.
3. **New unit test** — "supplier row-count mismatch": 4 area lines, 4 specs lines, 1 supplier line. Assert 4 child records emitted with supplier = the full original string on all 4, and `_split_from_review_hint == True`.
4. **End-to-end**: run `python App/services/pdf_to_structured_json.py <path_to_Job76_PDF> /tmp/job76_split.json` using the PDF Jason will provide via path in his delivery message. If Jason does not provide a path in advance, skip this and mark "Skipped — no PDF path provided. Will re-verify during review."
   - Expected: resulting JSON has the KITCHEN section with **10 items** (was 7) — `LED LIGHTING`, `PULL-OUT BIN`, `PULL-OUT SHELVES`, `PULL-OUT CORNER SHELVES` now each a separate item.
5. **Existing tests**: `pytest tests/ -q` must pass in full.

## Regression safety

The signed-off jobs are 61 / 62 / 64 / 67 and their snapshots are frozen (not re-run automatically). The change only affects **new extractions**. Nonetheless:

- After implementation, run `pytest tests/test_imperial_v6_adapter.py tests/test_parsing_v6_finalize.py tests/smoke_test.py -q` and include output.
- If any smoke test fails, **STOP** and report. Do not "fix" smoke tests to make them green.

## Acceptance criteria — your delivery report MUST include

**Do NOT reply "done" without the 6 items below. Claude will not accept a report missing any of them.** If you cannot complete an item, STOP and explain.

1. **Exact git diff** (`git diff` for modified files, full content for new test file). Not a summary.
2. **Exact pytest output** for the 3+ new unit tests (show PASSED lines).
3. **Exact pytest output** for `tests/test_imperial_v6_adapter.py tests/test_parsing_v6_finalize.py tests/smoke_test.py -q` (show PASSED/FAILED count).
4. **Line counts** before and after: `wc -l App/services/pdf_to_structured_json.py`.
5. **End-to-end Job 76 re-extraction result** (or explicit "skipped, no PDF path provided"): paste the area values of KITCHEN section items; confirm 10 items with PULL-OUT rows separated.
6. **Surprises / deviations** — list anything you changed beyond the spec, and why.

## Out of scope

- Bug C (Material Summary information loss on Door Colours / Handles). Separate task.
- Retroactive snapshot migration for existing jobs (they will naturally pick up the fix on next re-run).
- Changing `_review_hint` detection criteria.
- PDF geometry / grid detection tuning.

## Deployment

Codex delivers → Claude reviews diff + tests → Jason decides whether to commit/push/deploy. **Do not deploy yourself.**
