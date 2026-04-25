# Task — Bug G: De-duplicate v6 same-source HANDLES rows in adapter layer

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: Bug E + Bug F deployed (commit `f5aa815` + `438f359`).
**Layer scope**: `App/main.py` v6 fast-path `material_rows` handling. **No** changes to extractor / sibling repo / parser / templates / CSS.

---

## Context — read before editing

Jason rerun **job 73 (37558-2 Lot 532 Sandpiper Terrace, Imperial v6)** post-Bug-F deploy. Two adjacent rooms (KITCHEN, PANTRY) plus several others render duplicated HANDLES rows in the Spec List Rooms card:

### Observed bug (KITCHEN room card excerpt — same shape in PANTRY)

```
HANDLES
  BASE- BEVEL EDGE FINGERPULL              ← grouped block (correct, expected)
  UPPER - FINGERPULL
  TALL - PTO
…(other unrelated rows)…
HANDLES: BASE- BEVEL EDGE FINGERPULL       ← duplicate single row
HANDLES: UPPER - FINGERPULL                ← duplicate single row
HANDLES: BASE- BEVEL EDGE FINGERPULL       ← duplicate single row (again)
HANDLES: UPPER - FINGERPULL                ← duplicate single row (again)
```

The PDF source row in KITCHEN/PANTRY is a single HANDLES cell with 3 wrapped lines (`BASE- BEVEL EDGE FINGERPULL` / `UPPER - FINGERPULL` / `TALL - PTO`), no supplier. **Note**: `TALL - PTO` does **not** appear among the duplicate single rows — only `BASE-` and `UPPER -` repeat (each ×2).

### Material Summary > Handles is also polluted

`7 distinct items` includes overlapping entries from the same physical row:
1. grouped 3-line entry across `KITCHEN | PANTRY | LAUNDRY & MUD ROOM | LIVING & OFFICE`
2. flat `BASE- BEVEL EDGE FINGERPULL` across the same 4 rooms + 3 more
3. flat `UPPER - FINGERPULL` across the same 4 rooms + 1 more

These three Material Summary entries are different views of the **same** v6 PDF row.

### Hypothesis (codex must verify, not assume)

The v6 extractor (or adapter normalization) is emitting, for the same PDF HANDLES row, both:
- a "complete" row with multi-line `specs_or_description` and `display_groups` populated, **and**
- one or more wrap-split derivative rows where each line becomes a standalone `material_row` entry.

The display layer currently renders all of them. Bug G's job is to detect the same-source overlap **at the adapter / display layer only** and keep the canonical row, dropping the derivatives. Bug H (the underlying extractor multi-emit) is tracked separately and is **not in scope here** — do not touch `pdf_to_structured_json_v6.py` or sibling repo `claude-spec-extraction`.

### Target state (KITCHEN / PANTRY post-fix)

```
HANDLES
  BASE- BEVEL EDGE FINGERPULL
  UPPER - FINGERPULL
  TALL - PTO
```

That's it. The 4 duplicate single rows must be gone from the room card. Material Summary > Handles must drop the redundant flat `BASE-` and `UPPER -` entries (their content is already represented inside the grouped entry).

---

## Goal

1. In the Imperial v6 adapter / display path (NOT the extractor), de-duplicate **handle** material rows when one row's lines are a strict subset of another row's lines under the same `(room_key, area_or_item)` group.
2. The "winning" row is the one with the longest line set (or `display_groups` populated). Subset-only rows are dropped before they reach room card rendering AND before they reach `_build_imperial_material_summary`.
3. The fix runs **only** for v6-origin rows in the **HANDLES** bucket. Door Colours, Bench Tops, Drawers, Hinges, Flooring, Sink — all unchanged.
4. Rooms whose HANDLES row is a single line (BAR, POWDER, UPPER BATHROOM, MASTER ENSUITE in job 73) must be unaffected — there is nothing to deduplicate when there is only one row.
5. Rooms with grouped supplier handles (WALK IN ROBE in job 73 = Kethy 2 lines; job 76 KITCHEN = Kethy 4 lines; job 74 UPPER-BED 3 = Furnware + Titus Tekform) must be unaffected — supplier-bearing groups are not subset-derived.

### Out of scope (do not touch)

- `pdf_to_structured_json_v6.py` and **anything** under `code/claude-spec-extraction` (sibling repo)
- `_imperial_handle_summary_has_handle_identity` or any helper load-bearing for signed-off jobs 61 / 62 / 64 / 67
- `App/templates/spec_list.html` — no template changes needed; the grouped path already exists from Bug E/F
- Door Colours / Bench Tops bucket logic
- Bug H (`area_or_item` row-name merging across PDF lines — `BENCHTOP ISLAND CABINETRY COLOUR`, `KICKBOARDS GPO'S`, `BIN ACCESSORIES LED'S`)
- Bug I (`As per drawings` from LED's leaking into BED1&2 HANDLES)
- Bug J (`TALL - (PTO)` parenthesization in summary path)
- Bug K/L (Door Colours summary missing KITCHEN, Bench Tops summary mis-routing KITCHEN)
- Re-running signed-off jobs

These are tracked separately. Bug G must not depend on or alter their behavior.

---

## Required first step — verify the hypothesis before coding

Before writing any production code, **dump the v6 raw snapshot for job 73** and confirm the actual shape of `material_rows` for KITCHEN. Use the existing `tools/` helper or read directly from the SQLite store. Specifically check:

- How many `material_rows` does KITCHEN have where `area_or_item` (case-insensitive) equals `HANDLES`?
- For each such row, what are: `specs_or_description`, `display_lines`, `display_groups`, `supplier`, `provenance.source_provider`, `page_no`, `row_order`?
- Is the duplication present in the snapshot itself, or is it introduced by `_build_imperial_room_view` / `_build_imperial_material_summary`?

**Paste the raw JSON of KITCHEN's HANDLES rows verbatim into the delivery report.** This is part of the 6-item gate (item 5 — verification). If the actual shape contradicts the hypothesis, stop and report — do not invent a fix for a hypothesis you cannot reproduce.

---

## Implementation guidance (codex's judgment, not a prescription)

A natural shape:

1. Inside the v6 fast-path that materializes Imperial room `material_rows` (search around `_imperial_material_row_is_v6_origin` and the v6 dispatch site), apply a per-room handle de-duplication pass before the rows are returned for rendering.
2. Group v6-origin handle rows by `room_key`. Within each group, build the canonical line set per row from whichever of `display_groups[*].lines`, `display_lines`, or split `specs_or_description` actually exists. Treat lines as case-and-whitespace-normalized strings for comparison only — keep the original strings for rendering.
3. A row R is "subset-derivative" if there exists a different row R' in the same group such that `lines(R) ⊆ lines(R')` and `len(lines(R)) < len(lines(R'))`. Drop R.
4. Do not collapse rows that are equal in line count but differ in content — that's a different case (Bug J territory).
5. Preserve original `row_order` for the surviving rows.

This must run before both:
- room card rendering (so the duplicates disappear from the Rooms section), and
- `_build_imperial_material_summary` Handles bucket aggregation (so `distinct items` and `rooms_display` only see canonical rows).

Reasonable place: a dedicated helper like `_dedupe_v6_handle_subset_rows(material_rows: list[dict]) -> list[dict]` invoked from the v6 fast path before the rows are committed onto the room dict. Naming and exact placement is codex's call — keep the helper narrow and document the contract in a one-line docstring.

---

## Test requirements

### New unit tests (add to `tests/test_material_summary_v6_option_b.py` or a sibling test file under `tests/`)

1. **Subset case (job 73 KITCHEN shape)**: 3 v6 handle rows in one room
   - row A: `display_groups=[{supplier:"", lines:["BASE- BEVEL EDGE FINGERPULL","UPPER - FINGERPULL","TALL - PTO"]}]`, `display_lines` = same 3 lines, source_provider = "v6"
   - row B: `specs_or_description="BASE- BEVEL EDGE FINGERPULL"`, `display_lines=["BASE- BEVEL EDGE FINGERPULL"]`, no `display_groups`, source_provider = "v6"
   - row C: `specs_or_description="UPPER - FINGERPULL"`, `display_lines=["UPPER - FINGERPULL"]`, no `display_groups`, source_provider = "v6"
   - Assert: post-dedupe `material_rows` for handles in this room contains **only row A**. Material Summary `Handles` `distinct items` == 1.

2. **Repeat-of-derivative case (job 73 actual shape)**: same as above but with rows B and C **each duplicated** (B, C, B, C → 4 derivative rows). Post-dedupe must collapse to row A.

3. **No-subset preservation case**: 2 v6 handle rows where neither is a subset of the other (e.g. WALK IN ROBE shape with Kethy 2 lines + a separate hypothetical no-supplier line). Both rows survive.

4. **Single-row room (BAR / POWDER shape)**: 1 v6 handle row with 1 line. Survives untouched.

5. **Grouped-supplier preservation (Bug F regression)**: WALK IN ROBE shape — 1 row with `display_groups=[{supplier:"Kethy", lines:[...2 lines...]}]`. Material Summary `Handles` still shows 1 grouped entry with `<strong>Kethy</strong>` and 2 indented lines (current Bug F test must still pass).

6. **Non-v6 row pass-through**: a handle row with `provenance.source_provider != "v6"` plus a v6 row that would have deduped it — the non-v6 row must survive (we don't dedupe across origins).

### Existing regression subsets that MUST still pass verbatim

- All tests in `tests/test_material_summary_v6_option_b.py`
- `tests/smoke_test.py::test_spec_list_renders_v6_handle_supplier_groups` (Bug E)
- `tests/smoke_test.py::test_spec_list_material_summary_renders_grouped_handles_entries` (Bug F)
- `tests/fixtures/imperial_37867_gold.json`-driven regression (highest-priority Imperial fixture per AGENTS.md 28g)

If any existing test starts failing, **stop and report** — do not edit the test to make it pass unless the failure is a legitimate snapshot update (justify in deliverable item 6).

---

## Manual verification (Jason will do; codex must call out what to look for)

After deploy + rerun job 73:
- KITCHEN room card: HANDLES shows exactly 3 lines (no extra single-row duplicates)
- PANTRY room card: same
- LAUNDRY & MUD ROOM, LIVING & OFFICE: same
- BAR / POWDER / UPPER BATHROOM / MASTER ENSUITE: HANDLES still shows exactly the original line(s), no regression
- WALK IN ROBE: still shows `Kethy` group header + 2 indented lines (Bug E behavior preserved)
- Material Summary > Handles: `distinct items` count drops from 7. Expected new count: 4 or 5 depending on whether Bug I/J residual entries persist (those are out of scope, so do not engineer the count to a specific number — just confirm the 3 redundant flat entries from rooms 1–4 are gone).

---

## Delivery gate — 6 items, hard requirement

Per memory rule `feedback_codex_delivery_discipline.md`. Replying "done" without the following 6 items will be rejected by the Claude reviewer and the work will be re-queued.

1. **Exact `git diff`** of all modified files (not a summary, not a description — the actual diff text)
2. **Exact `pytest` output** for the new unit tests (verbatim stdout, including pass/fail counts and timings)
3. **Exact `pytest` output** for the regression subsets listed above (verbatim)
4. **Line counts before/after** for each modified source file (`wc -l` before and after, per file)
5. **End-to-end verification**: paste the raw JSON of job 73 KITCHEN's HANDLES `material_rows` from the v6 snapshot **before** the fix (proving the duplication exists at the snapshot layer) AND **after** the fix (proving the dedupe pass removed the derivatives). If end-to-end against a real snapshot is not feasible, mark this item explicitly `skipped, reason: ...` — do not silently omit.
6. **Surprises / deviations**: anything that didn't match the hypothesis, any test that needed updating, any helper rename, any incidental Bug I/J/K side-effects observed (positive or negative). If there are no surprises, write `none observed`.

---

## Constraints recap

- No edits to `pdf_to_structured_json_v6.py`, `code/claude-spec-extraction/`, signed-off job fixtures, or the Material Summary template (`spec_list.html` was already done in Bug F).
- No edits to `_imperial_handle_summary_has_handle_identity`.
- No re-running signed-off jobs 61 / 62 / 64 / 67.
- No commit using `tools/checkpoint.ps1` if other untracked task docs are present (per memory `feedback_checkpoint_ps1_boundary.md`) — use precise `git add`.
- No `--no-verify`, no `--amend`, no force-push.
