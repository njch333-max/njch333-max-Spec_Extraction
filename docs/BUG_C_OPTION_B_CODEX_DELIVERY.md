# Bug C Option B - Codex Delivery Summary

Reviewer: Claude  
Executor: Codex  
Status: Local implementation complete. Not committed, not pushed, not deployed.

## Scope

Implemented Bug C full Option B for Imperial v6 Material Summary enumeration.

Touched files:

- `App/services/imperial_v6_adapter.py`
- `App/main.py`
- `tests/test_material_summary_v6_option_b.py`

Files intentionally not touched:

- `App/services/parsing.py`
- `App/services/pdf_to_structured_json.py`
- `App/services/imperial_v6_room_fields.py`
- `App/templates/`
- Existing test files

## Implementation Summary

### Adapter

`_map_v6_item_to_material_row()` now adds `display_lines` for v6-origin rows only.

Display lines are built from non-empty `specs` lines and supplier prefixes:

- If specs and supplier line counts match, pair them 1-to-1.
- If specs exist but supplier line count does not match, use the full original supplier string as the prefix for each spec line.
- If no specs lines exist, no `display_lines` key is added.
- Defensive guard prevents double-prefixing when a line already starts with the supplier.

Added a narrow v6 BENCHTOP soft-wrap coalescer because Job 76 extractor output had:

- `20mm Shadowline under Benchtop -Forage`
- `Smooth`

as two specs lines while supplier had two lines total. The adapter coalesces trailing BENCHTOP spec lines until spec and supplier counts match. This avoids touching `pdf_to_structured_json.py`.

### Material Summary

`App/main.py` now preserves adapter-provided `display_lines` for v6-origin rows in `_flatten_imperial_material_rows()`.

V6 origin detection uses:

- `provenance.source_provider == "v6"`
- `provenance.source_extractor == "pdf_to_structured_json_v6"`
- `provenance.raw == "v6_cell"`

For Door Colours and Bench Tops:

- When v6 `display_lines` are present, Material Summary enumerates one entry per display line.
- It skips the legacy collapsed fallback for those rows.
- Legacy rows keep the existing fallback behavior.

For Handles:

- V6 rows with at least two `display_lines` use a v6-specific early branch in `_imperial_material_row_handle_summary_candidates()`.
- The branch bypasses the existing handle identity filter so lines such as `160mm - Lowers and Drawers` survive.
- The existing normalizer is still used as a non-empty / not-`None` gate.
- Returned candidates preserve the v6 display line with supplier prefix intact.
- V6 handle rows with only one display line fall through to existing logic.

## Job 76 End-To-End Result

Input PDF:

`tmp/bug6_job76_verify/job76_38020_colour_selections.pdf`

Pipeline result:

```text
analysis.parser_strategy= imperial_v6
analysis.layout_provider= pdf_to_structured_json_v6
```

Door Colours:

```text
Polytec - Amaro Matt
Polytec - Surround - Prime Oak Matt
Polytec - Backs only - Forage Smooth
```

Handles:

```text
Kethy - Finger Pull on Uppers- PTO where required
Kethy - L7817 - Oak Matt Black (OAKBK)
Kethy - 160mm - Lowers and Drawers
Kethy - 320mm - Pantry Door
```

Bench Tops:

```text
Caesarstone - 2Omm Stone - 4030 Oyster - PR
By Imperial - 20mm Shadowline under Benchtop -Forage Smooth
```

No deviation from the target lists after the BENCHTOP soft-wrap fix.

## Tests Run

New Option B unit tests:

```text
.\.venv\Scripts\python.exe -m pytest tests/test_material_summary_v6_option_b.py -v
7 passed in 0.91s
```

Regression subset:

```text
.\.venv\Scripts\python.exe -m pytest tests/test_imperial_v6_adapter.py tests/test_parsing_v6_finalize.py tests/test_pdf_extractor_split.py tests/smoke_test.py -q
898 passed in 29.73s
```

Full suite:

```text
.\.venv\Scripts\python.exe -m pytest tests/ -q
922 passed in 30.78s
```

`git diff --check` passed with only Git line-ending warnings for `App/main.py` and `App/services/imperial_v6_adapter.py`.

## Line Counts

```text
App/services/imperial_v6_adapter.py before=141 after=173 delta=32
App/main.py before=3544 after=3585 delta=41
```

## Notes For Review

- The v6 handle branch intentionally preserves supplier prefixes and `PTO where required` because the Option B target list requires complete per-PDF-line enumeration.
- Existing handle normalizer strips `Kethy` and `PTO where required`; this implementation does not modify the normalizer.
- Existing door-colour candidate logic normalizes `Polytec - Surround - Prime Oak Matt` to `Polytec - Prime Oak Matt`; v6 display-line enumeration preserves the source-backed display line to satisfy Option B.
- No signed-off job snapshots were rerun or migrated.
- No deploy was run.

