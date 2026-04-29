# Task — EVOCA-STRUCTURED-V0 fixes: terminal-group pairing + value rescue for unbordered rows

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: master at HEAD (post-EVOCA-STRUCTURED-V0 spike). `App/services/evoca_structured_extractor.py` (569 LOC) and `tools/evoca_structured_export.py` (54 LOC) already in place from Codex's first pass; `tmp/evoca_structured/EVOC{467,473}.{json,xlsx}` already produced. Jason has reviewed both these and Claude's parallel spike at `tools/evoca_structured_extractor_claude.py` / `tools/evoca_structured_export_claude.py` with outputs in `tmp/evoca_structured_claude/`. This task fixes two specific bugs in Codex's version surfaced by Jason's eyeball comparison.

**Layer scope**:
- `App/services/evoca_structured_extractor.py` — modify `_consume_group` (line 295), `_build_group_rows` (line 348), and add a value-rescue pass invoked from `extract_evoca_pdf` / `extract_evoca_pages`. Add 1 module-level constant `TERMINAL_GROUP_VALUES`.
- (optionally) `App/services/parsing.py` — if you choose to expose a `text_grid_rows` view for the rescue pass at the loader level, do it here in a single new helper. Otherwise call `pdfplumber` directly inside `evoca_structured_extractor.py`.
- `tools/evoca_structured_export.py` — no functional changes expected. Re-run after the fix to refresh `tmp/evoca_structured/*.{json,xlsx}`.

**Out of scope** (do **not** touch):
- `App/services/extraction_service.py` — not wiring the structured extractor into `build_spec_snapshot()` yet. This is still standalone-spike phase.
- `_finalize_evoca_rooms()` and any Evoca finalizer / SnapshotPayload code — schema contract preserved.
- `SECTION_TITLE_PATTERNS` scope (currently 15/16/17/18/19/20/21/22/23/24/25). **Do not** expand to sections 1-14 or 26-28 in this task; that is a separate scope decision Jason has not yet greenlit. If you find yourself needing section 1 (PRELIMINARIES) or section 13 (PAINTING) coverage to validate, surface the question rather than expanding silently.
- Sibling repo `code/claude-spec-extraction/`.
- Claude's parallel spike `tools/evoca_structured_extractor_claude.py` / `tools/evoca_structured_export_claude.py` — leave them in place as a reference; Jason will delete them after this task lands.
- Production Docling pipeline / `runtime.py` `SPEC_DOCLING_BUILDERS`.
- Existing `tmp/evoca_structured/*.json` / `*.xlsx` are reference-only; overwrite when validating.
- 4-doc sync (PRD / Arch / Project_state / AGENTS) — **not required** for this task because the structured extractor is not yet wired into any production code path. Update only if your fix changes a public contract that downstream code already imports (e.g. `parsing.load_document_pages` shape).

---

## Context — read before editing

### Bug 1 — terminal-group pairing wrong; greedy `_consume_group` swallows next group

**Reproduction**: Open `tmp/evoca_structured/EVOC467_Lot_1038_Oyster_-_COLOUR_SELECTION_DOCUMENT_20251111125911918v06.xlsx`, sheet `15 CABINETS`. Find Kitchen → Contrasting Facings group. Current output:

```
Group: Contrasting Facings (rows=8)
  Manufacturer        | Not Applicable
  Type                | Polytec               ← WRONG (belongs to Overhead Cupboards)
  Colour              | Belgian Oak Matt      ← WRONG
  Location            | Finger Grip           ← WRONG
  Overhead Cupboards  | (empty)
  Manufacturer        | (empty)
  Colour & Finish     | (empty)
  Handles             | (empty)
```

Source PDF (page 8 of EVOC467) actually says:

```
- Contrasting Facings   Not Applicable
    Manufacturer
    Type
    Colour
    Location
  Overhead Cupboards     (group header, no '-' marker)
    Manufacturer        Polytec
    Colour & Finish     Belgian Oak Matt
    Handles             Finger Grip
```

i.e. **Contrasting Facings is a terminal-N/A group** (whole feature excluded; sub-fields intentionally blank), and **Overhead Cupboards is a separate group** that lacks a leading `-` so `_consume_group` keeps eating its rows as continuations of Contrasting Facings.

### Root cause

Two compounding faults in `App/services/evoca_structured_extractor.py`:

1. **`_consume_group` boundary too loose** (line 311–326): the loop only stops on `detect_section_title || detect_room_label || detect_group_label`. None of those fire for an unanchored sub-section header like "Overhead Cupboards" (no `-` in column 0). So the loop swallows it and its three child rows into Contrasting Facings.
2. **`_build_group_rows` zip is naive** (line 358–397): it pairs `cleaned_labels[i]` with `cleaned_values[i]` regardless of how many of each there are. When 5 labels but 1 terminal value, the value gets bound to the wrong label (`Manufacturer` instead of staying on the group as a whole).

### Bug 2 — values lost on rows that lack inner cell borders

**Reproduction**: This bug is currently **invisible in Codex's output** because `SECTION_TITLE_PATTERNS` does not include section 1. To see it, temporarily add `("1", "1 PRELIMINARIES")` to `SECTION_TITLE_PATTERNS`, re-run, and inspect Sheet `1 PRELIMINARIES` for EVOC473. The "House" sub-block under PRELIMINARIES will show 11 labels (Specification, Ceiling Height - Lower Level, ..., Sliding Stacking Door Height) but **all values empty**.

Source PDF (EVOC473 page 1) has:

```
Specification                           Luxury Living
Ceiling Height - Lower Level            2740mm
Ceiling Height - Upper Level            2590mm
Front Entry Door Height                 2340mm
Garage Panel Lift Door Height           2400mm
Internal Hinged Door Height             2340mm
Cavity Sliding Door Height**            2340mm
Barn Door Height**                      2040mm
Special Hinged Door Height              Not Applicable
Internal Sliding (Robe) Door Height     2100mm
Sliding Stacking Door Height (to        2400mm
Living & Dining)
```

### Root cause

pdfplumber's default table strategy (`vertical_strategy="lines"`, `horizontal_strategy="lines"`) detects cells by ruled borders. Evoca's "House" sub-block draws an **outer cell border** but **no inner row borders** between the 11 properties. pdfplumber returns:

- One anchor row with all 11 labels merged into c1 (multi-line) and `c2 = None`
- Some label-continuation rows with `c0 = None`, `c1 = label_lines`, `c2 = None` (no values column at all)

The values column **is** in the rendered PDF text, but pdfplumber's grid detector ignores it because no border defines a column there.

The fix: re-extract each page with `vertical_strategy="lines", horizontal_strategy="text"` — this uses the outer cell column boundaries from the line grid but uses **text alignment** to split rows. Empirical result on EVOC473 page 1:

```
['Specification', 'Luxury Living']
['Ceiling Height - Lower Level', '2740mm']
['Ceiling Height - Upper Level', '2590mm']
... (11 clean label/value pairs)
```

Use this as a **rescue lookup**: build a per-page `{normalized_label → [value, ...]}` map; for any row in the structured JSON whose `value` is empty, fill it from the lookup if the label matches and the candidate is unique.

### Why these two fixes are coupled

The terminal-group fix (Bug 1) **must land before** the value-rescue fix (Bug 2). Otherwise rescue will populate the empty sub-fields of a terminal-N/A group with values from a same-named label elsewhere on the page (e.g. Contrasting Facings → Colour would get filled with "Champagne" from Benchtops → Colour). The rescue logic must explicitly skip groups whose anchor carries a `TERMINAL_GROUP_VALUES` value.

---

## Target behaviour

### Bug 1 fix — two changes

**Change 1a — Add terminal-group special case in `_build_group_rows`**:

Add module-level constant near top of `evoca_structured_extractor.py`:

```python
TERMINAL_GROUP_VALUES: frozenset[str] = frozenset({
    "not applicable", "not included", "not required",
    "n/a", "#n/a", "tbc",
})
```

In `_build_group_rows` (line 348), before the existing zip pairing, check:

```python
if (
    len(cleaned_labels) >= 2
    and len(cleaned_values) == 1
    and cleaned_values[0].strip().lower() in TERMINAL_GROUP_VALUES
):
    # Terminal value applies to the whole group — anchor carries it,
    # sub-properties remain empty.
    rows.append(_structured_row(
        label=cleaned_labels[0],   # group label as anchor
        value=cleaned_values[0],
        ...,
    ))
    for sub_label in cleaned_labels[1:]:
        rows.append(_structured_row(label=sub_label, value="", ...))
    return rows
```

**Change 1b — Tighten `_consume_group` boundary to stop on unanchored group headers**:

In `_consume_group` (line 311), the inner `while next_index < len(table)` loop currently stops on:

```python
if detect_section_title(next_row) or detect_room_label(next_row) or detect_group_label(next_row):
    break
```

Add a fourth stop condition: an "unanchored group header" — a row where `c0` is empty, `c1` is a single short Title-Case line (no `**`, no colon), and the row carries values that, when paired with the labels accumulated so far, would not match. The simplest practical heuristic that resolves the EVOC467 case:

```python
def detect_unanchored_group_header(row: list[str]) -> bool:
    """A row that visually begins a new property group without a leading '-'.
    
    Pattern: c0 empty, c1 has multi-line content where line 0 looks like
    a group label, and c2/c3 carry values aligned to c1 lines 1..N.
    """
    if _cell(row, 0):
        return False
    label_lines = _split_lines(_cell(row, 1))
    value_lines = _split_lines(_value_text(row))
    # Heuristic: 2+ labels AND 1+ values AND first label has no value column
    # ("Overhead Cupboards" pattern: 4 labels, 3 values)
    return len(label_lines) >= 2 and len(value_lines) >= 1 and len(label_lines) > len(value_lines)
```

Stop `_consume_group` on this signal too. The next iteration of the outer `while row_index < len(table)` loop will see the same row, and you can either (a) treat it as an implicit group via a small new code path, or (b) for v0 simplicity, route it into a new "unanchored group" code path that mirrors `_consume_group` minus the `-` requirement.

Recommendation: do (b). Add a single helper `_consume_unanchored_group` that's a near-copy of `_consume_group` except (i) it doesn't require `-` in c0 of the head row, (ii) it labels the group from `_split_lines(_cell(row, 1))[0]`, and (iii) it consumes only follow-up `[None, None, value, None]` value-continuation rows (no further label-continuation eating, since this group's labels are already in c1 of the head row).

**Acceptance for Bug 1**: After both changes, EVOC467 Kitchen Contrasting Facings shows exactly 5 rows (Contrasting Facings=Not Applicable, then Manufacturer/Type/Colour/Location all empty), and Overhead Cupboards is a **separate** group with 3 rows (Manufacturer=Polytec, Colour & Finish=Belgian Oak Matt, Handles=Finger Grip).

### Bug 2 fix — value rescue pass

Add a new function `_build_value_lookup(pdf_path: Path) -> dict[int, dict[str, list[str]]]` that opens the PDF with `pdfplumber.open(...)` and for each page calls:

```python
text_tables = page.extract_tables(table_settings={
    "vertical_strategy": "lines",
    "horizontal_strategy": "text",
}) or []
```

Walk these tables collecting 2-cell rows (`label`, `value`) where both are non-empty, both pass `_clean_label` / `parsing.normalize_space`, and the label does not match `_cell(row, 0) in {"-", "—", "–"}` or `detect_section_title`. Key the lookup by `_norm_label_key(label)` (normalize: strip `**`, collapse whitespace, lowercase). Value is a `list[str]` because the same label can recur on a page.

Add `_norm_label_key`:

```python
import re as _re
def _norm_label_key(label: str) -> str:
    s = parsing.normalize_space(label)
    s = _re.sub(r"\*+", "", s)
    s = _re.sub(r"\s+", " ", s)
    return s.strip().lower()
```

Then add a post-processing pass invoked at the end of `extract_evoca_pdf`:

```python
def _rescue_missing_values(structured: dict, lookup: dict[int, dict[str, list[str]]]) -> None:
    consumable = {p: {k: list(v) for k, v in d.items()} for p, d in lookup.items()}
    for section in structured.get("sections", []):
        for room in section.get("rooms", []):
            for group in room.get("groups", []):
                _rescue_group(group, consumable)
            # Section-level groups (no room)
        for group in section.get("groups", []):
            _rescue_group(group, consumable)

def _rescue_group(group, consumable):
    rows = group.get("rows", [])
    if not rows:
        return
    # Skip rescue when anchor row carries a terminal value
    anchor_value = (rows[0].get("value") or "").strip().lower()
    if anchor_value in TERMINAL_GROUP_VALUES:
        return
    for row in rows:
        if row.get("value"):
            continue
        page_lookup = consumable.get(int(row.get("page_no", 0) or 0), {})
        key = _norm_label_key(row.get("label", ""))
        candidates = page_lookup.get(key)
        if candidates:
            row["value"] = candidates.pop(0)
```

Important contract notes:

- **Skip terminal-value groups** — guard prevents Contrasting Facings from being polluted by Benchtops' "Colour: Champagne".
- **Consume candidates** — `pop(0)` ensures one lookup match per emitted row, so duplicate labels don't all point to the first value.
- **Do not overwrite** — only fill if `row["value"]` is empty.

Where to call it from: at the bottom of `extract_evoca_pdf` after `_update_statistics`. Pass the PDF path so you can open pdfplumber separately for the lookup. If you don't want to open the PDF twice, alternatively extend `parsing.load_document_pages` to also return text-grid rows; if you go that route, keep the new field optional and default-empty so any other caller is unaffected.

**Acceptance for Bug 2**: Add a temporary `("1", "1 PRELIMINARIES")` entry to `SECTION_TITLE_PATTERNS` (revert before commit). Re-run on EVOC473. Sheet `1 PRELIMINARIES` should show House group with 11 properties, including:

```
Specification                          | Luxury Living
Ceiling Height - Lower Level           | 2740mm
Ceiling Height - Upper Level           | 2590mm
Front Entry Door Height                | 2340mm
Garage Panel Lift Door Height          | 2400mm
Internal Hinged Door Height            | 2340mm
Cavity Sliding Door Height**           | 2340mm
Barn Door Height**                     | 2040mm
Special Hinged Door Height             | Not Applicable
Internal Sliding (Robe) Door Height    | 2100mm
Sliding Stacking Door Height (to ...)  | 2400mm    (or empty — see note)
```

Note: the last row's label is wrapped onto two PDF lines ("Sliding Stacking Door Height (to Living &" / "Dining)"). pdfplumber's text-strategy lookup may or may not match a wrapped label cleanly. If it doesn't, leave the value empty for v0 — Jason will see it in Excel and can decide whether to invest in label-unwrapping logic later. Document this in `_norm_label_key`'s docstring.

After validation, **revert** the temporary `("1", "1 PRELIMINARIES")` entry. Bug 2's correctness is verified by the temporary expansion; the fix itself stays scoped to sections 15-25 unless Jason explicitly approves expansion.

---

## Validation gate

Run after both fixes are in place. All four checks must pass before commit:

1. **Re-run extractor on both fixtures**:
   ```bash
   python tools/evoca_structured_export.py \
     "/path/to/EVOC473 (Lot 403 Sehmish - Color Selection Document) 20251107090209080v08.pdf" \
     "/path/to/EVOC467 (Lot 1038 Oyster - COLOUR SELECTION DOCUMENT) 20251111125911918v06.pdf"
   ```
   Expect both to complete in **< 10s each** (current: ~2.5s baseline; rescue pass adds ~1-2s).

2. **EVOC467 Kitchen Contrasting Facings** — open `tmp/evoca_structured/EVOC467_*.xlsx`, sheet `15 CABINETS`. Verify:
   - Contrasting Facings group has exactly 5 rows: anchor "Contrasting Facings = Not Applicable", then Manufacturer/Type/Colour/Location all empty.
   - Overhead Cupboards is a **distinct group** following Contrasting Facings, with 3 rows: Manufacturer=Polytec, Colour & Finish=Belgian Oak Matt, Handles=Finger Grip.

3. **EVOC467 Kitchen Benchtops** (regression check) — same sheet. Verify the existing correct rows are still correct: Manufacturer=Quantum Quartz, Colour=Champagne, Island Colour=As Above, Edge Profile=20mm Arissed, Island Edge Profile=40mm Arissed, Waterfall End to Island=Not Applicable.

4. **House values** — temporarily add `("1", "1 PRELIMINARIES")` to `SECTION_TITLE_PATTERNS`, re-run, verify the 11 House properties as listed above, then revert the temporary entry.

5. **Diagnostics in JSON** — verify `structured["statistics"]["row_count"]` increased over the previous baselines (EVOC473: 278, EVOC467: 267). Expect roughly +5 to +30 rows added by terminal-group correctness + rescued values within the existing 15-25 scope. (The temporary `1 PRELIMINARIES` toggle is not part of the committed numbers.)

6. **No regression in pytest** — run `pytest tests/ -x` to confirm the existing 1009 tests still pass. The structured extractor is not yet imported by any production code path, so impact should be zero, but verify `App/services/parsing.py` changes (if any) didn't break callers.

---

## Delivery checklist

- [ ] `App/services/evoca_structured_extractor.py` updated with TERMINAL_GROUP_VALUES, `_build_group_rows` terminal-case, `_consume_group` boundary, optional `_consume_unanchored_group` helper, `_build_value_lookup`, `_rescue_missing_values`, `_norm_label_key`.
- [ ] `tools/evoca_structured_export.py` re-runs cleanly; `tmp/evoca_structured/EVOC{467,473}.{json,xlsx}` regenerated.
- [ ] All 4 numbered validation checks above hand-verified by Jason on the refreshed XLSX.
- [ ] No code changes in `extraction_service.py`, `_finalize_evoca_rooms`, `runtime.py`, `tests/`, or sibling repo.
- [ ] Temporary `("1", "1 PRELIMINARIES")` entry **removed** from `SECTION_TITLE_PATTERNS` before commit.
- [ ] Commit message references this task: `EVOCA-STRUCTURED-V0: terminal-group + value rescue fixes`. Use single-quoted PowerShell heredoc for the message body (per repo convention).
