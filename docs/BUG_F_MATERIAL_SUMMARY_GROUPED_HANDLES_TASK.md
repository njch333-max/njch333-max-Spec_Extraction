# Task — Bug F: Material Summary Handles bucket adopts grouped rendering (supplier header + indented lines)

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Prerequisites**: Bug E (`display_groups` adapter + Rooms card template rendering) already deployed to prod.
**Layer scope**: `App/main.py` `_build_imperial_material_summary` logic + `App/templates/spec_list.html` Material Summary Handles card. No extractor/adapter/parsing changes.

---

## Context — read before editing

### Current state (post-Bug-E prod)

Job 76 `/jobs/76/spec-list` Rooms card KITCHEN HANDLES renders the new grouped form:
```
Kethy
  Finger Pull on Uppers- PTO where required
  L7817 - Oak Matt Black (OAKBK)
  160mm - Lowers and Drawers
  320mm - Pantry Door
```

Job 76 Material Summary Handles card still uses old flat bullet list:
```
Handles          4 distinct items
• Kethy - Finger Pull on Uppers- PTO where required   Room: KITCHEN
• Kethy - L7817 - Oak Matt Black (OAKBK)              Room: KITCHEN
• Kethy - 160mm - Lowers and Drawers                  Room: KITCHEN
• Kethy - 320mm - Pantry Door                         Room: KITCHEN
```

Jason wants Material Summary Handles to match the Rooms card grouped layout: supplier header + indented lines, one group per supplier, with `Room: XXX` on the supplier line.

### Target state (Job 76 example)

```
Handles          1 distinct item
• Kethy                                               Room: KITCHEN
    Finger Pull on Uppers- PTO where required
    L7817 - Oak Matt Black (OAKBK)
    160mm - Lowers and Drawers
    320mm - Pantry Door
```

### Target state (Job 74 UPPER-BED 3 Astrid example)

```
Handles          2 distinct items
• Furnware                                            Room: UPPER-BED 3 (Astrid)
    Tall Door Handles - Momo Hinoki Wood Big D
    832mm Handle Oak-HIN0682.832.OAK
    High Split Handle -Momo hinoki wood big d
    416mm handle oak-HIN0682.416.OAK
• Titus Tekform                                       Room: UPPER-BED 3 (Astrid)
    Drawers - Bevel Edge finger pull
    DESK - 2163 Voda Profile Handle Brushed
    Nickel 300mm - SO-2163-300-BN
    BENCHSEAT DRAWERS - PTO
```

---

## Goal

1. Material Summary **Handles bucket only** displays one entry per `display_groups` group sourced from v6 HANDLES material_rows. Each entry shows:
   - supplier as the header text (bold / emphasis)
   - the group's lines as indented sub-items (reuse existing `.supplier-group-line` CSS class)
   - `Room: {rooms_display}` on the same visual row as the supplier header
2. `distinct item` count is **the number of entries (groups)**, not the number of individual lines.
3. Cross-room dedupe: two material_rows with identical `(supplier, tuple(lines))` across different rooms collapse into one entry whose `rooms_display` joins the room names with ` | ` (reuse existing dedupe format).
4. Door Colours and Bench Tops buckets are **unchanged** (no change in their data shape, count, or rendering).
5. Material rows without `display_groups` (legacy rows, or future v6 handle rows with `display_groups` absent) fall through to current flat candidate behavior.

Out of scope:
- Any change to adapter / extractor / parsing.
- Door Colours / Bench Tops grouping.
- New CSS rules (reuse `.supplier-group-line` from Bug E; add at most one utility class if strictly necessary and document why).
- Signed-off job snapshot refresh.

---

## Implementation

### 1. `_build_imperial_material_summary` — new `handles` entry path

File: `App/main.py`, function `_build_imperial_material_summary` at [line 2353](../App/main.py#L2353). The handles branch is intertwined with the general bucket code around line 2381-2470.

Add a **parallel path** for handles entries when the flattened material_row has `display_groups`:

```python
# Inside the existing loop over flattened material_rows, when bucket_key == "handles"
# and the item has non-empty display_groups, emit grouped entries instead of flat
# candidates.

if bucket_key == "handles" and item.get("display_groups"):
    for group in item["display_groups"]:
        supplier_text = _display_value(group.get("supplier", ""))
        lines = [
            _display_value(line)
            for line in (group.get("lines", []) or [])
            if _display_value(line)
        ]
        if not supplier_text and not lines:
            continue
        dedupe_key = (supplier_text, tuple(lines))
        existing = _find_imperial_summary_grouped_entry(bucket_entries, dedupe_key)
        if existing is None:
            entry = {
                "text": supplier_text,
                "display_text": supplier_text,
                "lines": lines,
                "rooms": [room_label] if room_label else [],
                "rooms_display": "",
                "area_or_items": [area_or_item] if area_or_item else [],
                "_dedupe_key": dedupe_key,  # internal marker; strip before rendering if needed
            }
            bucket_entries.append(entry)
        else:
            if room_label and room_label not in existing["rooms"]:
                existing["rooms"].append(room_label)
            if area_or_item and area_or_item not in existing["area_or_items"]:
                existing["area_or_items"].append(area_or_item)
    # Skip the existing flat candidates path for this item
    continue
```

Add a helper:
```python
def _find_imperial_summary_grouped_entry(entries, dedupe_key):
    for entry in entries:
        if entry.get("_dedupe_key") == dedupe_key:
            return entry
    return None
```

After the main loop completes and rooms_display is computed (existing code), ensure grouped entries' `rooms_display` is also computed the same way (existing `_finalize_...` logic already iterates over `entry.rooms` — verify it covers grouped entries too; if not, extend it).

**Important — fall-through rule**: `item.get("display_groups")` must be a truthy non-empty list. An empty list or missing key → fall through to the existing `_imperial_material_row_handle_summary_candidates(item)` flat path.

### 2. `_build_imperial_material_summary` — absorbed handle texts path

The existing `absorbed_inline_handle_texts` processing (around [main.py:2449](../App/main.py#L2449)) adds entries for inline handles absorbed from non-handle rows. That path is unrelated to this change — leave it untouched. Its entries will remain flat (no `lines` field), and the template branch below handles that mix.

### 3. `_build_imperial_material_summary` — count semantics

The `bucket["count"] = len(bucket["entries"])` line (around [main.py:2489](../App/main.py#L2489)) is already correct for the new semantics: each grouped entry is one item. Verify no adjustment needed.

### 4. Template `spec_list.html` — Handles card rendering

Update the Handles card block around [spec_list.html:58-73](../App/templates/spec_list.html#L58-L73):

```jinja
<article class="summary-card">
  <h4>{{ material_summary.handles.label }}</h4>
  <p class="muted">{{ material_summary.handles.count }} distinct item{{ "" if material_summary.handles.count == 1 else "s" }}</p>
  <ul class="summary-list">
  {% for item in material_summary.handles.entries %}
  <li class="room-field-stack">
    {% if item.lines %}
      <strong>{{ item.display_text }}</strong>
      {% if item.rooms_display is defined %}
      <span class="muted">Room: {{ item.rooms_display or "-" }}</span>
      {% endif %}
      {% for line in item.lines %}
      <div class="supplier-group-line">{{ line }}</div>
      {% endfor %}
    {% else %}
      <span>{{ item.display_text }}</span>
      {% if item.rooms_display is defined %}
      <span class="muted">Room: {{ item.rooms_display or "-" }}</span>
      {% endif %}
    {% endif %}
  </li>
  {% else %}
  <li class="muted">No handles found.</li>
  {% endfor %}
  </ul>
</article>
```

Do **not** change the Door Colours or Bench Tops cards. They continue rendering flat.

### 5. CSS

No new CSS classes needed. Reuse existing `.supplier-group-line` and `.supplier-group-header` from Bug E. If the existing `.supplier-group-line` `padding-left: 1em` looks too tight inside the Material Summary card's bullet list context (double-indent with the `<li>` padding), Codex may add a tiny targeted rule like:

```css
.summary-list .supplier-group-line {
  padding-left: 1em;  /* or adjusted value */
}
```

Only if visual alignment requires it — if the existing class works fine in both contexts, don't add new rules. Test by rendering the page locally and eyeballing. Document the decision in the delivery report.

---

## Do NOT

- ❌ Do not modify Door Colours or Bench Tops bucket logic or template
- ❌ Do not modify `_imperial_material_row_handle_summary_candidates` (used by the flat fallback path)
- ❌ Do not modify any adapter, extractor, or parsing code
- ❌ Do not change the `material_summary.X.count` formula (already correct as `len(entries)`)
- ❌ Do not introduce new data shapes beyond the `lines` key on handle entries
- ❌ Do not touch the `absorbed_inline_handle_texts` path
- ❌ Do not deploy

---

## Tests (all must pass before delivery)

### New unit tests

File: `tests/test_material_summary_v6_option_b.py` or new `tests/test_material_summary_grouped_handles.py`.

1. **Job 76 Kethy single-supplier grouped entry**:
   - Build a v6 HANDLES material_row with supplier "Kethy", specs 4 lines, and populated `display_groups = [{"supplier": "Kethy", "lines": [4 lines]}]`, `tags = ["handles"]`.
   - Run `_build_imperial_material_summary({rooms: [...]})`.
   - Assert `summary["handles"]["count"] == 1`
   - Assert the single entry has:
     - `display_text == "Kethy"`
     - `lines == [4 spec lines]`
     - `rooms == ["KITCHEN"]` (or whatever room_label is)
     - `rooms_display == "KITCHEN"` (after finalization)

2. **Job 74 Astrid multi-supplier grouped entries**:
   - Build a v6 HANDLES row with supplier "Furnware\nTitus Tekform", 8 spec lines, `display_groups = [2 groups of 4 lines]`.
   - Assert `summary["handles"]["count"] == 2`
   - Assert both entries present with correct supplier / lines / room labels.

3. **Cross-room dedupe**:
   - Build two separate rooms (`KITCHEN` and `LOWER LINEN`), each with identical HANDLES display_groups (same supplier, same lines).
   - Assert `summary["handles"]["count"] == 1` (deduped)
   - Assert the single entry's `rooms_display == "KITCHEN | LOWER LINEN"` (or whatever format existing dedupe produces).

4. **Fall-through when display_groups absent**:
   - Build a HANDLES row with `display_lines` but no `display_groups` (legacy / odd shape).
   - Assert `summary["handles"]["count"] > 0` (flat entries as before)
   - Assert the entries have no `lines` field (so template falls through to flat render).

5. **Door Colours unchanged**:
   - Build a door_colours row with display_lines populated.
   - Assert `summary["door_colours"]["count"]` and entries are identical to current prod behavior (no `lines` field on door_colours entries).

6. **Bench Tops unchanged**:
   - Similar to #5 for bench_tops bucket.

### Template rendering smoke test

7. **Material Summary Handles renders grouped layout**:
   - Use `TestClient` to GET `/jobs/{id}/spec-list` against a fixture snapshot with a v6 HANDLES row having display_groups.
   - Assert response HTML contains:
     - `<strong>Kethy</strong>` (supplier header in the Material Summary card)
     - `class="supplier-group-line"` 4 times (one per line)
     - Text "1 distinct item" (count correct)
   - Assert response HTML does NOT contain old flat bullet `"Kethy - Finger Pull on Uppers..."` in the Material Summary Handles card (but it may still appear in the Rooms card — that's fine).

### Regression suites

8. `pytest tests/test_material_summary_v6_option_b.py tests/test_imperial_v6_adapter.py tests/test_parsing_v6_finalize.py tests/test_pdf_extractor_split.py tests/smoke_test.py -q` — all pass.

9. Full suite `pytest tests/ -q` — all pass. If any existing test in smoke_test.py asserts the old flat Material Summary Handles behavior, **update its assertion**. Document each such test in the delivery report's "Surprises" section.

### End-to-end

10. **Job 76 live re-render**:
    - Use local dev server (or TestClient) to render `/jobs/76/spec-list`.
    - Paste the Material Summary Handles card's rendered HTML (5-15 lines around the `<strong>Kethy</strong>` block).
    - Confirm: 1 supplier header "Kethy", 4 indented lines, "Room: KITCHEN" on header row.

11. **Job 74 live re-render**:
    - Render `/jobs/74/spec-list`.
    - Paste the Material Summary Handles card's full HTML.
    - Confirm: multiple supplier headers (at least Furnware and Titus Tekform for UPPER-BED 3 Astrid), each with correct lines and room labels.
    - Note: Job 74 will show **all** handles entries across all rooms (not just Astrid). This includes LOWER LINEN's handles, TROUSER RACK area's absorbed text (if applicable), etc. Paste the full list, don't prune.

---

## Acceptance criteria — 6-item delivery report

1. **Exact git diff** for every modified and new file
2. **Exact pytest output** for the new unit tests + template smoke test (tests 1-7)
3. **Exact pytest output** for regression suite #8 and full suite #9
4. **Line counts before/after** for modified source files (main.py, spec_list.html, optionally style.css)
5. **Job 76 + Job 74 end-to-end Material Summary Handles HTML** (tests 10 + 11 pasted as-is)
6. **Surprises / deviations**:
   - Did any existing smoke test assertion conflict with the new grouped shape? (Must be explicit — list each affected test.)
   - Did the existing `_finalize_...rooms_display` logic correctly handle grouped entries? (If it needed extension, explain.)
   - Did the CSS look right without new rules, or did you add a rule? (Document.)
   - Is there any case where `display_groups` contains both an empty-supplier group AND lines (from Bug E's Case H1 with empty supplier)? How is that entry rendered in Material Summary? (Document; acceptable to have a bare supplier-less block.)

**Reply "done" without these 6 items → rejected.**

---

## Files expected to be touched

- `App/main.py` (add grouped path in `_build_imperial_material_summary` + helper `_find_imperial_summary_grouped_entry`; ~30 lines)
- `App/templates/spec_list.html` (Handles card branch; ~15 line delta)
- `tests/test_material_summary_v6_option_b.py` or new `tests/test_material_summary_grouped_handles.py` (6-7 tests)
- `App/static/style.css` (maybe 0-3 lines; only if visual alignment requires)

Files NOT to touch: `imperial_v6_adapter.py`, `pdf_to_structured_json.py`, `parsing.py`, `imperial_v6_room_fields.py`, `store.py`, `models.py`, other templates.

---

## Deployment

Codex delivers → Claude audits (diff + tests + both e2e HTML snippets) → Jason commits/pushes/deploys/reruns jobs for live visual confirmation.

**Visual eyeball required**: same caveat as Bug E — CSS alignment in the Material Summary card context differs from Rooms card. Jason must confirm on live page after deploy and flag any layout issues for post-deploy CSS tweak.
