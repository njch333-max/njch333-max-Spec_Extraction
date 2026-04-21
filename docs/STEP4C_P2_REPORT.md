# Step 4c P2 Report

## Scope

Implemented Strategy 2: a high-level Imperial v6 fast path in `build_spec_snapshot`.

P3/deploy was not started.

## Fast Path Function Decisions

### Preserved in v6 fast path

- `_load_documents`
- `parsing.parse_documents`
- `_process_v6_imperial_document`
- `imperial_v6_adapter.run_v6_extraction`
- `imperial_v6_adapter.build_room_from_v6_section`
- `parsing._imperial_finalize_material_rows`
- `parsing._imperial_attach_handle_subitems`
- `parsing.enrich_snapshot_rooms`
- `_enrich_snapshot_appliances`
- `parsing.apply_snapshot_cleaning_rules` indirectly through `parse_documents`

### Bypassed in v6 fast path

- `_apply_layout_pipeline`
- `_try_openai`
- `_merge_ai_result`
- `_stabilize_snapshot_layout`
- `_apply_builder_specific_polish`
- `_apply_imperial_row_polish`
- `_build_raw_spec_crosscheck_snapshot`
- `_crosscheck_imperial_snapshot_with_raw`
- Docling layout branch inside `_apply_layout_pipeline`
- Heavy vision branch inside `_apply_layout_pipeline`

## Fallback Behavior

The v6 fast path now falls back to the legacy Imperial pipeline in two cases:

- `parsing.parse_documents` raises during the v6 fast path.
- v6 returns no material rows with provenance `source_provider == "v6"` or `source_extractor == "pdf_to_structured_json_v6"`.

Fallback behavior is:

- Emit progress warning stage `imperial_v6_fallback`.
- Return `None` from `_build_imperial_v6_fast_snapshot`.
- Continue into the existing legacy pipeline: `_apply_layout_pipeline`, Docling/heavy vision as configured, old parser enrichment, builder-specific polish, Imperial row polish, and raw crosscheck.
- Do not fail the run just because v6 failed or returned zero usable rows.

## Parser Strategy Dependency Check

Command run locally:

```powershell
rg -n "parser_strategy\s*(==|in|not in)|global_conservative" App -g "*.py"
```

Remote prod grep with `ssh -o BatchMode=yes` was attempted and failed because this shell has no SSH key and I did not embed the production password in a command. Since P2 changes are local-only and P3/deploy is blocked pending review, the local source grep is the actionable pre-commit check.

Relevant local call sites:

- `App\models.py:8`: Pydantic default `parser_strategy = "global_conservative"`. Not a branch. No correctness impact.
- `App\services\cleaning_rules.py:7`: `GLOBAL_PARSER_STRATEGY = "global_conservative"`. Expected.
- `App\services\extraction_service.py:7954`: `_try_openai` checks `parser_strategy == "heuristic_only"`. v6 fast path bypasses `_try_openai`.
- `App\services\extraction_service.py:8297`: `_merge_rooms` checks `parser_strategy in {"stable_hybrid", global_parser_strategy()}`. v6 fast path bypasses `_merge_ai_result`.
- `App\services\extraction_service.py:9422`: builder polish guard checks stable/global. v6 fast path bypasses `_apply_builder_specific_polish`.
- `App\services\extraction_service.py:11727`: Clarendon row polish guard. Not relevant to Imperial v6.
- `App\services\extraction_service.py:11751`: Imperial row polish guard. v6 fast path bypasses `_apply_imperial_row_polish`; if reached accidentally with `imperial_v6`, this guard would also skip it.

Assessment: no app code depends on `parser_strategy == "global_conservative"` as a required invariant for rendering/export. UI/runtime display uses `cleaning_rules.parser_strategy_label`, so `imperial_v6` was added to `PARSER_STRATEGIES`.

## Code Changes

- `App/services/extraction_service.py`
  - Adds `IMPERIAL_V6_PARSER_STRATEGY = "imperial_v6"`.
  - Adds top-level Imperial v6 fast path before layout/docling.
  - Adds `_build_imperial_v6_fast_snapshot`.
  - Adds `_snapshot_has_v6_material_rows`.
- `App/services/worker.py`
  - Updates run metadata after snapshot build if snapshot analysis reports a different parser strategy.
- `App/services/cleaning_rules.py`
  - Adds `imperial_v6` parser strategy label/option.
- `tests/test_imperial_v6_path_dispatch.py`
  - Adds fast-path and fallback coverage.

## Tests

```text
.\.venv\Scripts\python.exe -m pytest tests/test_imperial_v6_path_dispatch.py -v
4 passed in 0.59s

.\.venv\Scripts\python.exe -m pytest tests/test_imperial_v6_adapter.py -v
10 passed in 0.65s

.\.venv\Scripts\python.exe -m pytest tests/test_imperial_v6_room_fields.py -v
13 passed in 0.21s

.\.venv\Scripts\python.exe -m pytest -v
909 passed in 32.40s
```

Logs:

- `tmp/step4c/p2/pytest_dispatch_step4c_p2.log`
- `tmp/step4c/p2/pytest_adapter_step4c_p2.log`
- `tmp/step4c/p2/pytest_room_fields_step4c_p2.log`
- `tmp/step4c/p2/pytest_full_step4c_p2.log`

Diff artifacts:

- `tmp/step4c/p2/code_diff_step4c_p2.diff`
- `tmp/step4c/p2/test_diff_step4c_p2.diff`

## Current Git State

Expected modified files:

- `App/services/cleaning_rules.py`
- `App/services/extraction_service.py`
- `App/services/worker.py`
- `tests/test_imperial_v6_path_dispatch.py`
- `docs/STEP4C_DIAGNOSIS_REPORT.md`
- `docs/STEP4C_P2_REPORT.md`

P3 is intentionally blocked pending Jason/Claude review.
