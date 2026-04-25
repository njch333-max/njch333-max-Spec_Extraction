# Task — Bug C v2: supplier-row-count as the grouping signal; wrap vs. multi-row disambiguation

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Supersedes** (partially): `BUG_C_FULL_TASK.md` Option B rules for multi-line specs. Adapter display_lines construction is replaced. Material Summary consumption logic (main.py) is unchanged.
**Prerequisites**: `ef1f01b` (Bug 6) + `f8c4759` (Bug B) + `60fc307` (Bug C Option B) all in prod. This task modifies adapter rules atop those.

---

## Context — read before editing

### Problem exposed by Job 74 (17 Park Street Kelvin Grove)

The current `_display_lines_for_v6_item` (adapter) unconditionally splits specs on `\n`. Job 74's `UPPER-BED 2 (Evyn)` section shows two distinct failures:

**Failure 1: wrap artifacts become independent entries (over-split)**
- BENCHTOP specs `"33MM Laminated Benchtop with bullnose\nprofile (Internal shelf in Robe)"` + supplier `"Polytec"` (1 line) → currently shown as 2 entries. Should be 1 entry: `Polytec - 33MM Laminated Benchtop with bullnose profile (Internal shelf in Robe)`.
- IRONING BOARD / HANGING RAIL same pattern (3-4 specs lines, 1 supplier line, all wrap).

**Failure 2: row-merge from missing table separator stays invisible (under-reported)**
- PDF has two physical rows `TROUSER RACK` (supplier `Furnware`) and `EVYN'S ROOM DRAWERS & SHELF` (supplier `Polytec`) separated by a missing horizontal line. Extractor merges them into one item with area=`TROUSER RACK EVYN'S ROOM DRAWERS & SHELF`, supplier=`Furnware\nPolytec`, specs=8 lines. Current display emits 7 display_lines each prefixed `"Furnware Polytec - ..."`. Should visibly signal "this needs human review".

### Jason's grouping rule (authoritative)

**Supplier row count is the grouping signal.**

- **Rule A — supplier has 1 line**: specs newlines are PDF physical wrap artifacts, merge them. However, if the PDF had a **blank line** between spec lines (intentional paragraph break), preserve that as a `|` separator.
- **Rule B1 — supplier has N lines AND specs has exactly N non-empty lines**: one-to-one pair each supplier line with its spec line (existing Option B behavior). No hint.
- **Rule B2 — supplier has N lines AND specs line count ≠ N**: extractor lost information. Emit a single merged entry, wrap supplier prefix in `*...*` asterisks to signal "needs review".

Accepted regressions:
- Job 76 Material Summary Door Colours goes **from 3 entries to 2** — `Polytec - Surround - Prime Oak Matt` + `Polytec - Backs only - Forage Smooth` collapse into `Polytec - Surround - Prime Oak Matt Backs only - Forage Smooth`. Jason explicitly accepted this.
- Job 76 Bench Tops stays at **2 entries** (Rule B1, 2 supplier lines + 2 specs lines match).
- Job 76 Handles stays at **4 entries** (Rule B1, supplier is 1 line but specs 4 lines — wait, that's Rule A → merged into 1 entry). **This is a regression from current prod.** See below.

### Handles (Job 76) specific outcome

Job 76 HANDLES row: supplier `"Kethy"` (1 line), specs `"Finger Pull on Uppers- PTO where required\nL7817 - Oak Matt Black (OAKBK)\n160mm - Lowers and Drawers\n320mm - Pantry Door"` (4 lines, no blank lines in PDF — check Job 76 raw JSON to confirm).

Under new Rule A: supplier 1 line → merge → 1 entry `Kethy - Finger Pull on Uppers- PTO where required L7817 - Oak Matt Black (OAKBK) 160mm - Lowers and Drawers 320mm - Pantry Door`.

**But Jason explicitly wanted 4 Handles entries** (confirmed in the earlier conversation: "这个job其实有4个handle"). This contradicts pure Rule A.

**Resolution (codex must implement)**: When the row's primary tag is `"handles"`, keep the per-line enumeration regardless of supplier line count. Rule A applies to `door_colours` and `bench_tops` buckets only; Handles always enumerates per line (as in Option B current prod). Document this carve-out in the diff.

---

## Implementation — required changes

### Change 1: Preserve paragraph breaks in extractor `clean_cell`

File: `App/services/pdf_to_structured_json.py`

Current [line 300](../App/services/pdf_to_structured_json.py#L300):
```python
cell = re.sub(r"\n{2,}", "\n", cell)
```

Replace with:
```python
cell = re.sub(r"\n{3,}", "\n\n", cell)
```

Rationale: `\n\n` signals an intentional paragraph break in the source PDF. Collapsing to single `\n` destroys that signal. The new regex caps runs of 3+ newlines at exactly 2 (preserves blank-line separator, avoids pathological long runs).

### Change 2: Update Bug B gate to ignore blank lines

File: `App/services/pdf_to_structured_json.py`, around [line 691-699](../App/services/pdf_to_structured_json.py#L691-L699) (grid path) and the matching continuation path at ~line 592-601 after Bug B's edit.

Current gate uses `area_text.count("\n") + 1` which inflates when `\n\n` is preserved. Replace with a non-empty line count:

```python
area_lines = len([line for line in area_text.splitlines() if line.strip()]) if area_text else 0
specs_lines = len([line for line in specs_text.splitlines() if line.strip()]) if specs_text else 0
```

Apply this change to **both** the grid path and the continuation path.

Also verify `_split_review_hint_record` (from Bug B) still correctly splits when `\n\n` is present in area/specs: its internal `_non_empty_cell_lines` already filters blanks, so splitting should still work. Add a unit test covering the `\n\n` input case.

### Change 3: Rewrite `_display_lines_for_v6_item` in adapter

File: `App/services/imperial_v6_adapter.py`

Replace the current function + the `_coalesce_soft_wrapped_v6_spec_lines` helper with the logic below. Delete `_coalesce_soft_wrapped_v6_spec_lines` (no longer needed — the BENCHTOP soft-wrap special case naturally disappears under Rule A).

Pseudocode:

```
def _display_lines_for_v6_item(item):
    specs = item.get("specs", "") or ""
    supplier = item.get("supplier", "") or ""
    tags = item.get("tags", []) or []  # note: tags are set later in the pipeline
    area = item.get("area", "") or ""

    supplier_lines = [line.strip() for line in supplier.splitlines() if line.strip()]
    n_supplier = len(supplier_lines)

    # Detect handles carve-out by area keyword (tags not yet attached at this layer)
    is_handles_row = "HANDLES" in area.upper()

    # Parse specs into blocks: blank line separator -> paragraph break
    spec_blocks = _split_specs_into_blocks(specs)
    # Each block is a list of non-empty lines. Blocks are joined with " | " ;
    # lines within a block are joined with " ".
    merged_spec = " | ".join(" ".join(block) for block in spec_blocks if block)
    total_spec_nonempty = sum(len(block) for block in spec_blocks)

    # HANDLES CARVE-OUT (always per-line, keeps current prod behavior for Job 76)
    if is_handles_row:
        lines = [line.strip() for line in specs.splitlines() if line.strip()]
        if n_supplier == len(lines) and n_supplier >= 1:
            return [_join_supplier_spec(supplier_lines[i], lines[i]) for i in range(n_supplier)]
        supplier_prefix = supplier.strip() if supplier else ""
        return [_join_supplier_spec(supplier_prefix, line) for line in lines]

    # RULE A: supplier 1 line -> merge specs (respecting blank-line -> "|")
    if n_supplier <= 1:
        prefix = supplier_lines[0] if n_supplier == 1 else ""
        if not merged_spec:
            return [prefix] if prefix else []
        return [_join_supplier_spec(prefix, merged_spec)]

    # RULE B1: supplier N lines AND specs exactly N non-empty lines -> 1:1 pair
    flat_spec_lines = [line for block in spec_blocks for line in block]
    if n_supplier >= 2 and len(flat_spec_lines) == n_supplier:
        return [_join_supplier_spec(supplier_lines[i], flat_spec_lines[i]) for i in range(n_supplier)]

    # RULE B2: supplier N lines AND specs count != N -> single merged entry with asterisk hint
    hinted_supplier = "*" + " / ".join(supplier_lines) + "*"
    if merged_spec:
        return [f"{hinted_supplier} - {merged_spec}"]
    return [hinted_supplier]


def _split_specs_into_blocks(specs):
    """Split specs text into a list of blocks, each block being a list of
    non-empty stripped lines. Blocks are separated by one or more blank lines."""
    blocks = []
    current = []
    for raw_line in specs.splitlines():
        line = raw_line.strip()
        if line:
            current.append(line)
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    return blocks


def _join_supplier_spec(supplier, spec):
    supplier = (supplier or "").strip()
    spec = (spec or "").strip()
    if not supplier:
        return spec
    if not spec:
        return supplier
    # Defensive double-prefix guard
    if spec.lower().startswith(f"{supplier.lower()} - "):
        return spec
    return f"{supplier} - {spec}"
```

Key semantics to preserve:
- Empty specs and empty supplier both produce `[]` (no display_lines added, keep existing adapter behavior: omit the field).
- Handles carve-out uses area text `"HANDLES"` substring match (case-insensitive). Handles with weird area names (`"HANDLES - BASE"` etc.) still qualify.
- Rule B1 only activates when flat non-empty specs count **exactly** equals supplier count, not when block count equals supplier count.

### Change 4: main.py unchanged

`_flatten_imperial_material_rows`, `_build_imperial_material_summary`, `_imperial_material_row_handle_summary_candidates`, `_imperial_material_row_is_v6_origin` — **no changes**. They consume `display_lines` whatever the adapter produces. The new adapter output naturally flows through.

---

## Do NOT

- ❌ Do not modify `parsing.py` (Bug 6 lives there)
- ❌ Do not modify `App/main.py` — Material Summary logic is already correct for the new display_lines shape
- ❌ Do not modify `imperial_v6_room_fields.py` or `App/services/store.py`
- ❌ Do not modify any normalizer function (`_normalize_door_colour_summary_value`, `_normalize_imperial_handle_summary_value`, etc.)
- ❌ Do not touch `App/templates/`
- ❌ Do not retroactively migrate signed-off job snapshots
- ❌ Do not deploy

## Tests

### Update existing tests (required because accepted regressions change assertions)

1. `tests/test_material_summary_v6_option_b.py::test_v6_door_colour_summary_enumerates_display_lines_and_dedupes_rooms`:
   - Currently asserts 3 Door Colours entries. Change to 2: `{"Polytec - Amaro Matt", "Polytec - Surround - Prime Oak Matt Backs only - Forage Smooth"}`. Rename test to `test_v6_door_colour_single_supplier_merges_specs_within_row`.

2. `tests/test_material_summary_v6_option_b.py::test_v6_benchtop_display_lines_coalesce_soft_wrapped_spec_line`:
   - The soft-wrap coalescer is deleted. Rework this test: with specs `"2Omm Stone - 4030 Oyster - PR\n20mm Shadowline under Benchtop -Forage\nSmooth"` + supplier `"Caesarstone\nBy Imperial"` (supplier 2, specs 3 non-empty) → under Rule B2 → emits `*Caesarstone / By Imperial* - 2Omm Stone - 4030 Oyster - PR 20mm Shadowline under Benchtop -Forage Smooth`. Rename to `test_v6_benchtop_mismatched_spec_count_emits_hinted_entry`.

3. `tests/test_material_summary_v6_option_b.py::test_v6_benchtop_summary_pairs_supplier_and_spec_lines`:
   - Keep as-is. Rule B1 produces the same 1:1 pairing.

4. `tests/test_material_summary_v6_option_b.py::test_v6_handle_summary_candidates_emit_all_display_lines_with_supplier_prefix`:
   - Keep 4-line expectation. Handles carve-out preserves this.

### New tests (required)

Add to `tests/test_material_summary_v6_option_b.py` or new file `tests/test_display_lines_grouping.py`:

5. Rule A with blank line `|` separator: specs `"line one\nline two\n\nline three"`, supplier `"Polytec"`, non-handles area → single entry `"Polytec - line one line two | line three"`.

6. Rule B2 hint format: specs 5 lines, supplier 2 lines (mismatch), non-handles → single entry starting with `"*Supplier1 / Supplier2* - ..."`.

7. Legacy (non-v6 or empty) rows: adapter without supplier+specs returns no display_lines field.

8. Handles row under Rule B1 match: supplier 4 lines + specs 4 lines → 4 paired entries (not merged).

### Bug B interaction tests

9. Update `tests/test_pdf_extractor_split.py` to add a case where area has blank-line separator `"A\n\nB\n\nC"` with matching specs. Verify `_split_review_hint_record` still splits into 3 (not 5) items because `_non_empty_cell_lines` filters blanks.

10. Update extractor gate test: construct a record with `area_text = "A\n\nB"` (2 real items + 1 blank). Verify the gate does NOT fire (`area_lines` counts non-empty, so = 2, below the 3 threshold).

### Regression suites

11. `pytest tests/test_imperial_v6_adapter.py tests/test_parsing_v6_finalize.py tests/test_pdf_extractor_split.py tests/test_material_summary_v6_option_b.py tests/smoke_test.py -q` — all pass.

12. Full suite `pytest tests/ -q` — all pass.

### End-to-end

13. Job 74 re-extraction (provide PDF path in dispatch message, or use any stored Job 74 PDF in `tmp/`): assert that BENCHTOP, IRONING BOARD, HANGING RAIL rows each produce exactly 1 display_line entry. Assert that TROUSER RACK row produces 1 display_line entry starting with `*Furnware / Polytec*`.

14. Job 76 re-extraction: assert Material Summary Door Colours = 2 entries, Handles = 4 entries (unchanged), Bench Tops = 2 entries (Rule B1 match unchanged). Paste the full entries.

---

## Acceptance criteria — 6-item delivery report (same rule)

1. Exact git diff for every modified and new file.
2. Exact pytest output for the new/updated unit tests.
3. Exact pytest output for regression suite #11 and full suite #12.
4. Line counts before/after for each modified source file.
5. Job 74 and Job 76 end-to-end results (or explicit "skipped, reason: ..."). Paste Material Summary entries + one example of a merged wrap case (e.g. BENCHTOP) and one Rule B2 hinted entry.
6. Surprises / deviations — expected topics:
   - Did the Handles carve-out introduce any awkward cases (e.g. Job 74 HANDLES if it has different supplier count)?
   - Did removing `_coalesce_soft_wrapped_v6_spec_lines` expose any test that was relying on its behavior?
   - Any existing test beyond the 4 listed above whose assertion needed to change?

**"Done" without the 6 items → rejected.**

---

## Files touched (expected)

- `App/services/pdf_to_structured_json.py` (clean_cell + gate updates)
- `App/services/imperial_v6_adapter.py` (rewrite `_display_lines_for_v6_item`, delete `_coalesce_soft_wrapped_v6_spec_lines`)
- `tests/test_material_summary_v6_option_b.py` (update 2 tests + add 3-4)
- `tests/test_pdf_extractor_split.py` (update + add 1-2 tests)
- Optional: `tests/test_display_lines_grouping.py` (new, if grouping adapter tests don't fit elsewhere)

Files NOT touched: `App/main.py`, `App/services/parsing.py`, `App/services/imperial_v6_room_fields.py`, `App/services/store.py`, `App/models.py`, `App/templates/`.

## Deployment

Codex delivers → Claude audits → Jason commits/pushes/deploys.
