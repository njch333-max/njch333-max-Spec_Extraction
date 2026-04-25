# Task — Bug E: HANDLES supplier-grouped rendering (Option B: group header UX)

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: `31b0d5c` (Bug C v2) already in prod. All prior v6 fixes merged.
**Layer scope**: adapter + main.py flatten + template + CSS. Extractor untouched.

---

## Context — read before editing

### The symptom (Job 74 `UPPER-BED 3 (Astrid)`)

PDF `Colour Selection - 4.9.25 - 17 Park Street Kelvin Grove.pdf`, room `UPPER-BED 3 (Astrid)`, HANDLES area has **2 distinct PDF rows** (separator line present but too thin for pdfplumber to detect as a row break):

| Supplier | Specs |
|---|---|
| Furnware | Tall Door Handles - Momo Hinoki Wood Big D<br>832mm Handle Oak-HIN0682.832.OAK<br>High Split Handle -Momo hinoki wood big d<br>416mm handle oak-HIN0682.416.OAK |
| Titus Tekform | Drawers - Bevel Edge finger pull<br>DESK - 2163 Voda Profile Handle Brushed<br>Nickel 300mm - SO-2163-300-BN<br>BENCHSEAT DRAWERS - PTO |

Extractor collapses to a single item: `supplier = "Furnware\nTitus Tekform"` (2 lines) + `specs = 8 non-empty lines`.

Current Bug C v2 HANDLES carve-out renders each spec line with the full supplier string prefix:
```
Furnware Titus Tekform - Tall Door Handles - Momo Hinoki Wood Big D
Furnware Titus Tekform - 832mm Handle Oak-HIN0682.832.OAK
... (x8)
```

Jason's business reading of the PDF: 2 supplier groups, each with 4 spec lines. He wants:

```
Furnware
  Tall Door Handles - Momo Hinoki Wood Big D
  832mm Handle Oak-HIN0682.832.OAK
  High Split Handle -Momo hinoki wood big d
  416mm handle oak-HIN0682.416.OAK
Titus Tekform
  Drawers - Bevel Edge finger pull
  DESK - 2163 Voda Profile Handle Brushed
  Nickel 300mm - SO-2163-300-BN
  BENCHSEAT DRAWERS - PTO
```

Supplier header appears **once** per group, spec lines listed vertically underneath.

### The data problem (accepted limitation)

The extractor does not preserve supplier-to-spec y-coordinate alignment. We **cannot know** which spec line belongs to which supplier without a heuristic. This task uses one heuristic:

> **Equal-share assumption**: when specs count `M` is a positive multiple of supplier count `N`, each supplier is assigned `M/N` consecutive spec lines in order. Otherwise the equal-share path is not safe and we fall back to the existing Rule B2 hint.

For Astrid: `N=2, M=8, M/N=4` → Furnware gets spec lines 0-3, Titus Tekform gets lines 4-7. This matches the PDF's true structure by luck-of-layout (PDF renders suppliers top-down, specs top-down).

Jason accepted this assumption for HANDLES only. Other row types continue using the Rule B2 hint path.

---

## Goal

1. Adapter produces a new **structured field** `display_groups` on v6 HANDLES rows whenever the equal-share assumption holds (or the row has a single supplier, which is a trivial single-group case).
2. Template `spec_list.html` renders `display_groups` as group-header + indented lines when present, falling back to `display_lines` otherwise. Unchanged behavior for non-HANDLES rows.
3. Adapter also updates `display_lines` (flat form) for HANDLES multi-supplier cases so Material Summary receives per-spec single-supplier prefixes (not the current duplicated "Furnware Titus Tekform - ..." prefix).
4. Material Summary logic in main.py is unchanged — it continues consuming `display_lines` and dedupes per its existing rules.

Out of scope:
- Non-HANDLES rows (Door Colours / Bench Tops / other tags). They retain Bug C v2 behavior.
- Rule B2 hint `*Supplier1 / Supplier2*` behavior for non-HANDLES.
- Extractor changes.
- Signed-off job snapshot refresh.

---

## Implementation — required changes

### 1. Adapter: new `display_groups` field + updated `display_lines` logic

File: `App/services/imperial_v6_adapter.py`

In `_display_lines_for_v6_item`, when the row is a HANDLES row, compute **both** `display_lines` (flat, as before — but adjusted as below) **and** `display_groups` (new structured field). The function's return type should be extended. Simplest approach: change the caller `_map_v6_item_to_material_row` to compute both via a helper and assign two keys.

Rename/refactor suggestion (codex may choose cleaner structure):

```python
def _map_v6_item_to_material_row(item, source_pdf, row_order, section_title):
    row = { ...existing fields... }
    display_payload = _display_payload_for_v6_item(item)
    if display_payload.get("display_lines"):
        row["display_lines"] = display_payload["display_lines"]
    if display_payload.get("display_groups"):
        row["display_groups"] = display_payload["display_groups"]
    return row


def _display_payload_for_v6_item(item):
    """Return {"display_lines": [...], "display_groups": [...]} for v6 items.

    - display_lines: flat list with per-line supplier prefix (for Material Summary / fallback)
    - display_groups: list of {"supplier": str, "lines": [str, ...]} for structured UI rendering.
      Only populated for HANDLES rows where we can assign specs to suppliers confidently
      (single-supplier, or multi-supplier with specs count a positive multiple of supplier count).
    """
```

#### Grouping rules (HANDLES only)

Let `n_supplier = len(supplier_lines)`, `m_specs = len(flat_spec_lines)`.

- **Case H0 — HANDLES + empty specs**: no groups. display_lines follows existing behavior.
- **Case H1 — HANDLES + n_supplier <= 1 + specs non-empty**:
  - display_groups = `[{"supplier": supplier_text_or_empty, "lines": flat_spec_lines}]`
  - display_lines = `["<supplier> - <line>" for line in flat_spec_lines]` (supplier prefix per line; same as current Bug C v2 output for Job 76 Kethy case)
- **Case H2 — HANDLES + n_supplier >= 2 + m_specs == n_supplier** (1:1 match, existing Rule B1 for HANDLES):
  - display_groups = `[{"supplier": s_i, "lines": [spec_i]} for i in 0..n-1]`
  - display_lines = `["<s_i> - <spec_i>" for i in 0..n-1]` (same as current Bug C v2)
- **Case H3 — HANDLES + n_supplier >= 2 + m_specs > n_supplier + m_specs % n_supplier == 0** (equal-share, new behavior):
  - Let `chunk = m_specs // n_supplier`. supplier_i gets `flat_spec_lines[i*chunk : (i+1)*chunk]`.
  - display_groups = one entry per supplier with its chunk of lines.
  - display_lines = flat concatenation `["<s_i> - <line>" for i in 0..n-1 for line in chunk_i]` — per-line with that line's owning supplier as prefix (NOT the old "Furnware Titus Tekform - ..." concatenated prefix).
- **Case H4 — HANDLES + n_supplier >= 2 + (m_specs < n_supplier OR m_specs % n_supplier != 0)** (mismatch, can't safely split):
  - display_groups = **not populated** (falls back to flat rendering)
  - display_lines = keep current Bug C v2 behavior (per-line full-supplier-string prefix)

Non-HANDLES rows: no `display_groups`; `display_lines` unchanged from Bug C v2.

#### Blank-line handling within a chunk

Bug C v2 already collapses `\n\n` separators into `|` at display line level via `_split_specs_into_blocks`. For groups: within each supplier chunk, preserve lines as-is (each chunk line is already a non-empty stripped string from `flat_spec_lines`, which came from `_split_specs_into_blocks`). Groups do NOT introduce `|` separators between lines — each line stands alone.

### 2. Template: group rendering

File: `App/templates/spec_list.html`

Around [line 111-118](../App/templates/spec_list.html#L111-L118), update the render block to prefer `display_groups`:

```jinja
{% if item.display_groups %}
  {% for group in item.display_groups %}
    <div class="supplier-group-header">{{ group.supplier }}</div>
    {% for line in group.lines %}
    <div class="supplier-group-line">{{ line }}</div>
    {% endfor %}
  {% endfor %}
{% elif item.display_lines %}
  {% for line in item.display_lines %}
  <div>{{ line }}</div>
  {% endfor %}
{% else %}
  {{ item.display_value or item.value }}
{% endif %}
```

Empty `group.supplier` should still render a header div (it'll be visually empty but CSS can skip styling or you can emit nothing for the header when supplier is empty — codex's call; keep it simple). Document the choice.

### 3. CSS: group styling

File: `App/static/style.css` (find the existing `.room-field-stack` or `.room-field-row` rules)

Add minimal CSS:

```css
.supplier-group-header {
  font-weight: 600;
  margin-top: 0.3em;
}
.supplier-group-header:first-child {
  margin-top: 0;
}
.supplier-group-line {
  padding-left: 1em;
}
```

Keep styling minimal — factory QA needs a clear visual hierarchy, not a designer showcase. If the codebase uses a CSS framework or has existing conventions, follow them; don't invent new patterns.

### 4. Main.py: `_flatten_imperial_material_rows` pass-through

File: `App/main.py`, around [line 886-899](../App/main.py#L886-L899) where `display_lines` is computed.

Currently:
```python
source_display_lines = [...]  # from item.display_lines
rendered_display_lines = [...]  # from legacy view
display_lines = source_display_lines if v6_origin and source_display_lines else rendered_display_lines
```

Add `display_groups` pass-through alongside. Something like:
```python
source_display_groups = item.get("display_groups") or []
display_groups = source_display_groups if _imperial_material_row_is_v6_origin(item) else []
```

Include `display_groups` in the dict returned to the template (add to the existing returned dict assembly — the flattened dict that templates iterate).

**No changes to Material Summary** (`_build_imperial_material_summary`, `_imperial_material_row_handle_summary_candidates`). Material Summary continues reading `display_lines` only.

---

## Do NOT

- ❌ Do not touch `App/services/pdf_to_structured_json.py`, `parsing.py`, `imperial_v6_room_fields.py`, `App/services/store.py`, `App/models.py`
- ❌ Do not add `display_groups` to non-HANDLES rows
- ❌ Do not modify Material Summary aggregation logic in main.py (only add pass-through for `display_groups`)
- ❌ Do not change existing `display_lines` behavior for non-HANDLES rows
- ❌ Do not introduce new dependencies
- ❌ Do not deploy

---

## Tests

### Adapter unit tests (update + add)

File: `tests/test_material_summary_v6_option_b.py` (or new `tests/test_handles_display_groups.py`)

Update existing:

1. `test_v6_handle_summary_candidates_emit_all_display_lines_with_supplier_prefix` (Job 76 Kethy): display_lines assertion unchanged (4 entries, each `"Kethy - <line>"`). Also assert row has `display_groups == [{"supplier": "Kethy", "lines": [4 lines]}]`.

2. `test_v6_handles_with_matching_supplier_count_pair_one_to_one`: display_lines unchanged; also assert `display_groups == [{"supplier": "Supplier1", "lines": [line one]}, {"supplier": "Supplier2", "lines": [line two]}, ...]`.

Add new:

3. **Astrid equal-share grouping (Case H3)**: supplier=`"Furnware\nTitus Tekform"`, specs = 8 lines as in the Astrid fixture. Assert:
   - `display_groups` has 2 entries: Furnware with first 4 lines, Titus Tekform with last 4 lines
   - `display_lines` has 8 entries, first 4 prefixed `"Furnware - "`, last 4 prefixed `"Titus Tekform - "` (NOT the old `"Furnware Titus Tekform - "` concatenated prefix)

4. **Mismatch fallback (Case H4)**: supplier 2 lines, specs 5 lines (not a multiple). Assert:
   - `display_groups` is absent (key missing or empty list — codex's choice, document it)
   - `display_lines` behaves as current Bug C v2 HANDLES fallback (full supplier string prefix on each line)

5. **Single-supplier HANDLES produces single group** (Case H1): supplier=`"Kethy"` alone, specs 4 lines. Assert `display_groups == [{"supplier": "Kethy", "lines": [4 lines]}]`.

6. **Non-HANDLES row does not produce display_groups**: Bench Tops or Door Colours row, multi-supplier even with matching specs count. Assert `display_groups` key is absent.

### Main.py pass-through test

7. **Flatten preserves display_groups**: construct a v6 HANDLES row with `display_groups` set, pass through `_flatten_imperial_material_rows`, assert the flattened dict has `display_groups` field equal to the input.

### Template rendering smoke test

8. **Template renders display_groups when present**: use Jinja2 directly or FastAPI TestClient to render `spec_list.html` with a fixture snapshot containing a HANDLES row with display_groups. Assert rendered HTML contains:
   - the supplier header text (e.g. "Furnware") exactly once
   - each of the chunk lines as separate `div` elements with class `supplier-group-line`

   If the template test is awkward due to existing test harness structure, at minimum assert the template file's updated jinja block parses (no syntax error) by loading it via the app's template engine.

### Regression suite

9. `pytest tests/test_imperial_v6_adapter.py tests/test_parsing_v6_finalize.py tests/test_pdf_extractor_split.py tests/test_material_summary_v6_option_b.py tests/smoke_test.py -q` — all pass.

10. Full suite `pytest tests/ -q` — all pass.

### End-to-end

11. **Job 74 UPPER-BED 3 (Astrid) HANDLES verification**: re-extract the PDF locally via v6 pipeline. Assert the KITCHEN- Sorry, the UPPER-BED 3 room's HANDLES material_row has:
    - `supplier == "Furnware\nTitus Tekform"`
    - `display_groups == [{"supplier": "Furnware", "lines": [4 Furnware lines]}, {"supplier": "Titus Tekform", "lines": [4 Titus lines]}]`
    - `display_lines` has 8 entries, first 4 `"Furnware - ..."`, last 4 `"Titus Tekform - ..."`

12. **Job 76 KITCHEN HANDLES verification**: re-extract. Assert `display_groups == [{"supplier": "Kethy", "lines": [4 lines]}]` and `display_lines` unchanged (4 entries with `"Kethy - "` prefix).

13. **Material Summary invariance**: for both Job 74 and Job 76, compute `_build_imperial_material_summary` and confirm the Handles bucket entries are:
    - Job 74 UPPER-BED 3: 8 entries, first 4 with `"Furnware - ..."` prefix, last 4 with `"Titus Tekform - ..."` prefix (NOT duplicated concatenated prefix — this is an intentional improvement)
    - Job 76 KITCHEN: 4 entries with `"Kethy - ..."` prefix (unchanged)

---

## Acceptance criteria — 6-item delivery report (same rule)

1. **Exact git diff** for every modified and new file
2. **Exact pytest output** for the new/updated unit tests (tests 1-8 above)
3. **Exact pytest output** for regression suite #9 and full suite #10
4. **Line counts before/after** for every modified source file (not tests, not docs)
5. **Job 74 + Job 76 end-to-end results** (or explicit "skipped, reason: ..."):
   - Paste Astrid HANDLES `display_groups` and first 2 entries of `display_lines`
   - Paste Job 76 Kethy HANDLES `display_groups` and `display_lines`
   - Paste Material Summary Handles bucket for both jobs (full list)
   - Paste a 5-line HTML snippet from the rendered `/jobs/74/spec-list` page showing the Astrid HANDLES section
6. **Surprises / deviations**:
   - Did the CSS change touch any other page's layout? (Run local dev server if possible, check home / jobs list / other pages at a glance.)
   - Did any existing test require modification beyond tests 1-2?
   - Did the template change affect any non-HANDLES row rendering?

**Reply "done" without these 6 items → rejected.**

---

## Files expected to be touched

- `App/services/imperial_v6_adapter.py` (rewrite display logic, extend map_v6_item output)
- `App/main.py` (add `display_groups` pass-through in flatten, ~3 lines)
- `App/templates/spec_list.html` (add `display_groups` branch in existing render block)
- `App/static/style.css` (add 2 small rules for group header + group line)
- `tests/test_material_summary_v6_option_b.py` (update 2 tests + add 4-5 tests) OR new `tests/test_handles_display_groups.py`

Files NOT touched: `parsing.py`, `pdf_to_structured_json.py`, `imperial_v6_room_fields.py`, `store.py`, `models.py`, other templates.

---

## Deployment

Codex delivers → Claude audits (diff + unit tests + e2e pasted values + HTML snippet) → Jason commits/pushes/deploys/reruns job 74 & 76 for live verification.

**UI-specific caveat**: unit tests cannot fully verify CSS rendering. Jason must eyeball the live page after deploy and confirm visual hierarchy looks right. Be prepared for a post-deploy CSS tweak.
