from __future__ import annotations

import json
import os
import re
import urllib.request
from pathlib import Path
from typing import Any, Callable

from App.services import appliance_official
from App.services import cleaning_rules, parsing, runtime


ProgressCallback = Callable[[str, str], None] | None


def build_spec_snapshot(
    job: dict[str, Any],
    builder: dict[str, Any],
    files: list[dict[str, Any]],
    template_files: list[dict[str, Any]],
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    rule_flags = cleaning_rules.global_rule_flags()
    parser_strategy = cleaning_rules.global_parser_strategy()
    documents = _load_documents(files, role="spec")
    _report_progress(progress_callback, "heuristic", f"Running heuristic extraction on {len(documents)} spec file(s)")
    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="spec", documents=documents, rule_flags=rule_flags)
    ai_result, analysis = _try_openai(
        job,
        builder,
        documents,
        template_files,
        source_kind="spec",
        parser_strategy=parser_strategy,
        progress_callback=progress_callback,
    )
    analysis.update(
        {
            "parser_strategy": parser_strategy,
            "rule_config_updated_at": "",
            "rule_flags": rule_flags,
            "worker_pid": os.getpid(),
            "app_build_id": runtime.APP_BUILD_ID,
        }
    )
    if ai_result:
        _report_progress(progress_callback, "openai_merge", "Merging OpenAI result with heuristic extraction")
        merged = _merge_ai_result(heuristic, ai_result, parser_strategy=parser_strategy, rule_flags=rule_flags)
        merged = parsing.enrich_snapshot_rooms(merged, documents, rule_flags=rule_flags)
        merged = _stabilize_snapshot_layout(merged, builder_name=str(builder.get("name", "")), parser_strategy=parser_strategy)
        merged = _apply_builder_specific_polish(
            merged,
            documents,
            builder_name=str(builder.get("name", "")),
            parser_strategy=parser_strategy,
            rule_flags=rule_flags,
            progress_callback=progress_callback,
        )
        merged = _enrich_snapshot_appliances(merged, progress_callback, rule_flags=rule_flags)
        merged["analysis"] = analysis
        return merged
    _report_progress(progress_callback, "room_enrichment", "Applying room fixture and door-colour overlays")
    heuristic = parsing.enrich_snapshot_rooms(heuristic, documents, rule_flags=rule_flags)
    heuristic = _stabilize_snapshot_layout(heuristic, builder_name=str(builder.get("name", "")), parser_strategy=parser_strategy)
    heuristic = _apply_builder_specific_polish(
        heuristic,
        documents,
        builder_name=str(builder.get("name", "")),
        parser_strategy=parser_strategy,
        rule_flags=rule_flags,
        progress_callback=progress_callback,
    )
    heuristic = _enrich_snapshot_appliances(heuristic, progress_callback, rule_flags=rule_flags)
    heuristic["analysis"] = analysis
    return heuristic


def build_drawing_snapshot(
    job: dict[str, Any],
    builder: dict[str, Any],
    files: list[dict[str, Any]],
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    rule_flags = cleaning_rules.global_rule_flags()
    parser_strategy = cleaning_rules.global_parser_strategy()
    documents = _load_documents(files, role="drawing")
    _report_progress(progress_callback, "heuristic", f"Running heuristic extraction on {len(documents)} drawing file(s)")
    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="drawing", documents=documents, rule_flags=rule_flags)
    ai_result, analysis = _try_openai(
        job,
        builder,
        documents,
        [],
        source_kind="drawing",
        parser_strategy=parser_strategy,
        progress_callback=progress_callback,
    )
    analysis.update(
        {
            "parser_strategy": parser_strategy,
            "rule_config_updated_at": "",
            "rule_flags": rule_flags,
            "worker_pid": os.getpid(),
            "app_build_id": runtime.APP_BUILD_ID,
        }
    )
    if ai_result:
        _report_progress(progress_callback, "openai_merge", "Merging OpenAI result with heuristic extraction")
        merged = _merge_ai_result(heuristic, ai_result, parser_strategy=parser_strategy, rule_flags=rule_flags)
        merged = parsing.enrich_snapshot_rooms(merged, documents, rule_flags=rule_flags)
        merged = _stabilize_snapshot_layout(merged, builder_name=str(builder.get("name", "")), parser_strategy=parser_strategy)
        merged = _enrich_snapshot_appliances(merged, progress_callback, rule_flags=rule_flags)
        merged["analysis"] = analysis
        return merged
    _report_progress(progress_callback, "room_enrichment", "Applying room fixture and door-colour overlays")
    heuristic = parsing.enrich_snapshot_rooms(heuristic, documents, rule_flags=rule_flags)
    heuristic = _stabilize_snapshot_layout(heuristic, builder_name=str(builder.get("name", "")), parser_strategy=parser_strategy)
    heuristic = _enrich_snapshot_appliances(heuristic, progress_callback, rule_flags=rule_flags)
    heuristic["analysis"] = analysis
    return heuristic


def _load_documents(files: list[dict[str, Any]], role: str) -> list[dict[str, object]]:
    documents: list[dict[str, object]] = []
    for file_row in files:
        path = Path(file_row["path"])
        documents.append(
            {
                "file_name": file_row["original_name"],
                "path": str(path),
                "role": role,
                "pages": parsing.load_document_pages(path),
            }
        )
    return documents


def _try_openai(
    job: dict[str, Any],
    builder: dict[str, Any],
    documents: list[dict[str, object]],
    template_files: list[dict[str, Any]],
    source_kind: str,
    parser_strategy: str,
    progress_callback: ProgressCallback = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    analysis = {
        "mode": "heuristic_only",
        "parser_strategy": parser_strategy,
        "openai_attempted": False,
        "openai_succeeded": False,
        "openai_model": runtime.OPENAI_MODEL,
        "note": "",
    }
    if parser_strategy == "heuristic_only":
        analysis["note"] = "Parser strategy is set to Heuristic Only."
        _report_progress(progress_callback, "openai_skipped", analysis["note"])
        return None, analysis
    if not runtime.OPENAI_ENABLED:
        analysis["note"] = "OpenAI is disabled in runtime settings."
        _report_progress(progress_callback, "openai_skipped", analysis["note"])
        return None, analysis
    if not runtime.OPENAI_API_KEY:
        analysis["note"] = "OPENAI_API_KEY is not configured."
        _report_progress(progress_callback, "openai_skipped", analysis["note"])
        return None, analysis

    analysis["openai_attempted"] = True

    combined_text: list[str] = []
    for doc in documents:
        combined_text.append(f"FILE: {doc['file_name']}")
        for page in doc["pages"]:
            combined_text.append(f"PAGE {page['page_no']}: {page['text']}")

    template_text: list[str] = []
    for row in template_files[:3]:
        try:
            pages = parsing.load_document_pages(Path(row["path"]))
        except Exception:
            continue
        template_text.append(f"TEMPLATE FILE: {row['original_name']}")
        template_text.append("\n".join(f"PAGE {page['page_no']}: {page['text']}" for page in pages[:5]))

    prompt = {
        "job_no": job["job_no"],
        "builder_name": builder["name"],
        "source_kind": source_kind,
        "instructions": (
            "Extract cabinet and appliance information into JSON. "
            "Return JSON only with keys: rooms, appliances, others, warnings. "
            "Room rows must include room_key, original_room_label, bench_tops, door_panel_colours, toe_kick, bulkheads, handles, "
            "drawers_soft_close, hinges_soft_close, splashback, flooring, door_colours_overheads, door_colours_base, door_colours_island, door_colours_bar_back, "
            "sink_info, basin_info, tap_info, source_file, page_refs, evidence_snippet, confidence. "
            "Appliance rows must include appliance_type, make, model_no, product_url, spec_url, manual_url, website_url, overall_size, source_file, page_refs, evidence_snippet, confidence, "
            "but do not return sinks, basins, taps, or tubs as appliances because those belong on the related room row. "
            "When an appliance table has separate make and model columns, capture the model column into model_no. "
            "Preserve quantity prefixes for model numbers when they are explicit, for example '2 x WVE6515SDA'. "
            "Do not use brand-only words, generic notes, or size units as model_no."
        ),
        "templates": "\n\n".join(template_text)[:18000],
        "documents": "\n\n".join(combined_text)[:60000],
    }

    _report_progress(progress_callback, "openai_prepare", f"Preparing OpenAI prompt for {runtime.OPENAI_MODEL}")
    try:
        _report_progress(progress_callback, "openai_request", f"Calling OpenAI model {runtime.OPENAI_MODEL}")
        response_json = _post_responses_api(prompt)
    except Exception as exc:
        analysis["mode"] = "openai_fallback"
        analysis["note"] = f"OpenAI request failed: {_truncate_note(exc)}"
        _report_progress(progress_callback, "openai_fallback", analysis["note"])
        return None, analysis

    output_text = _extract_output_text(response_json)
    if not output_text:
        analysis["mode"] = "openai_fallback"
        analysis["note"] = "OpenAI returned no output text."
        _report_progress(progress_callback, "openai_fallback", analysis["note"])
        return None, analysis
    try:
        parsed = _parse_openai_json_output(output_text)
    except json.JSONDecodeError as exc:
        analysis["mode"] = "openai_fallback"
        analysis["note"] = f"OpenAI returned invalid JSON: {_truncate_note(exc)} | preview: {_preview_output(output_text)}"
        _report_progress(progress_callback, "openai_fallback", analysis["note"])
        return None, analysis
    if not isinstance(parsed, dict):
        analysis["mode"] = "openai_fallback"
        analysis["note"] = "OpenAI returned a non-object payload."
        _report_progress(progress_callback, "openai_fallback", analysis["note"])
        return None, analysis
    parsed = _normalize_ai_result(parsed)

    parsed.setdefault("job_no", job["job_no"])
    parsed.setdefault("builder_name", builder["name"])
    parsed.setdefault("source_kind", source_kind)
    parsed.setdefault("generated_at", runtime.utc_now_iso())
    parsed.setdefault(
        "source_documents",
        [{"file_name": str(doc["file_name"]), "role": str(doc["role"])} for doc in documents],
    )
    analysis["mode"] = "openai_merged"
    analysis["openai_succeeded"] = True
    analysis["note"] = f"OpenAI result merged with the global 37016-style conservative profile ({cleaning_rules.parser_strategy_label(parser_strategy)})."
    _report_progress(progress_callback, "openai_merge", analysis["note"])
    return parsed, analysis


def _post_responses_api(prompt: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(
        {
            "model": runtime.OPENAI_MODEL,
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": json.dumps(prompt, ensure_ascii=False)}],
                }
            ],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=body,
        headers={"Authorization": f"Bearer {runtime.OPENAI_API_KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_output_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str):
        return str(payload["output_text"]).strip()
    texts: list[str] = []
    for item in payload.get("output", []):
        for content in item.get("content", []):
            text = content.get("text")
            if isinstance(text, str):
                texts.append(text)
    return "\n".join(texts).strip()


def _parse_openai_json_output(output_text: str) -> dict[str, Any]:
    candidates = _json_candidates(output_text)
    last_error: json.JSONDecodeError | None = None
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            last_error = exc
            continue
        if isinstance(parsed, dict):
            return parsed
        raise json.JSONDecodeError("OpenAI returned a non-object payload.", candidate, 0)
    if last_error:
        raise last_error
    raise json.JSONDecodeError("OpenAI returned invalid JSON.", output_text, 0)


def _json_candidates(output_text: str) -> list[str]:
    text = output_text.strip().lstrip("\ufeff")
    candidates: list[str] = []
    if text:
        candidates.append(text)

    fenced_match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.IGNORECASE | re.DOTALL)
    if fenced_match:
        candidates.append(fenced_match.group(1).strip())

    object_candidate = _extract_balanced_json_object(text)
    if object_candidate:
        candidates.append(object_candidate)

    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate and candidate not in seen:
            unique_candidates.append(candidate)
            seen.add(candidate)
    return unique_candidates


def _extract_balanced_json_object(text: str) -> str:
    start = text.find("{")
    while start != -1:
        depth = 0
        in_string = False
        escape = False
        for index in range(start, len(text)):
            char = text[index]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : index + 1].strip()
        start = text.find("{", start + 1)
    return ""


def _merge_ai_result(base: dict[str, Any], ai_result: dict[str, Any], parser_strategy: str = "ai_hybrid", rule_flags: Any = None) -> dict[str, Any]:
    merged = dict(base)
    ai_rooms = _as_list_of_dicts(ai_result.get("rooms"))
    ai_appliances = _as_list_of_dicts(ai_result.get("appliances"))
    ai_others = _as_dict(ai_result.get("others"))
    ai_warnings = _as_string_list(ai_result.get("warnings"))
    merged["rooms"] = _merge_rooms(list(base.get("rooms", [])), ai_rooms, parser_strategy=parser_strategy, rule_flags=rule_flags)
    merged["appliances"] = _merge_appliances(list(base.get("appliances", [])), ai_appliances)
    merged["others"] = _merge_other_fields(dict(base.get("others") or {}), ai_others)
    merged["warnings"] = _merge_warning_lists(list(base.get("warnings", [])), ai_warnings)
    merged["source_documents"] = base.get("source_documents", [])
    return parsing.apply_snapshot_cleaning_rules(merged, rule_flags=rule_flags)


def _normalize_ai_result(parsed: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(parsed)
    normalized["rooms"] = _as_list_of_dicts(parsed.get("rooms"))
    normalized["appliances"] = _as_list_of_dicts(parsed.get("appliances"))
    normalized["others"] = _as_dict(parsed.get("others"))
    normalized["warnings"] = _as_string_list(parsed.get("warnings"))
    return normalized


def _as_list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _merge_rooms(
    base_rows: list[dict[str, Any]],
    ai_rows: list[dict[str, Any]],
    parser_strategy: str = "ai_hybrid",
    rule_flags: Any = None,
) -> list[dict[str, Any]]:
    if not base_rows:
        return ai_rows
    if not ai_rows:
        return base_rows
    if parser_strategy in {"stable_hybrid", cleaning_rules.global_parser_strategy()}:
        merged_rows: list[dict[str, Any]] = []
        used_secondary: set[int] = set()
        for primary_row in base_rows:
            match_index = _find_best_room_match(primary_row, ai_rows, used_secondary)
            if match_index is None:
                merged_rows.append(primary_row)
                continue
            used_secondary.add(match_index)
            merged_rows.append(_merge_single_room(primary_row, ai_rows[match_index], stable_hybrid=True))
        return merged_rows
    heuristic_first = cleaning_rules.rule_enabled(rule_flags, "heuristic_first_room_layout")
    primary_rows = base_rows if heuristic_first else ai_rows
    secondary_rows = ai_rows if heuristic_first else base_rows
    merged_rows: list[dict[str, Any]] = []
    used_secondary: set[int] = set()
    for primary_row in primary_rows:
        match_index = _find_best_room_match(primary_row, secondary_rows, used_secondary)
        if match_index is None:
            merged_rows.append(primary_row)
            continue
        used_secondary.add(match_index)
        merged_rows.append(_merge_single_room(primary_row, secondary_rows[match_index]))
    if not heuristic_first:
        for index, row in enumerate(secondary_rows):
            if index not in used_secondary:
                merged_rows.append(row)
    return merged_rows


def _find_best_room_match(base_row: dict[str, Any], ai_rows: list[dict[str, Any]], used_ai: set[int]) -> int | None:
    base_key = _norm(base_row.get("room_key", ""))
    base_label = _norm(base_row.get("original_room_label", ""))
    best_index: int | None = None
    best_score = 0
    for index, ai_row in enumerate(ai_rows):
        if index in used_ai:
            continue
        score = 0
        ai_key = _norm(ai_row.get("room_key", ""))
        ai_label = _norm(ai_row.get("original_room_label", ""))
        if base_key and ai_key and base_key == ai_key:
            score += 6
        if base_label and ai_label and base_label == ai_label:
            score += 4
        if _norm(base_row.get("source_file", "")) and _norm(base_row.get("source_file", "")) == _norm(ai_row.get("source_file", "")):
            score += 2
        if _norm(base_row.get("page_refs", "")) and _norm(base_row.get("page_refs", "")) == _norm(ai_row.get("page_refs", "")):
            score += 1
        if score > best_score:
            best_index = index
            best_score = score
    return best_index


def _merge_single_room(base_row: dict[str, Any], ai_row: dict[str, Any], stable_hybrid: bool = False) -> dict[str, Any]:
    merged = dict(base_row)
    for field_name in ("room_key", "original_room_label", "splashback", "flooring", "sink_info", "basin_info", "tap_info", "source_file", "page_refs", "evidence_snippet"):
        if not merged.get(field_name) and ai_row.get(field_name):
            merged[field_name] = ai_row[field_name]
    for field_name in ("bench_tops", "door_panel_colours", "toe_kick", "bulkheads", "handles"):
        if stable_hybrid:
            merged[field_name] = parsing._coerce_string_list(base_row.get(field_name)) or parsing._coerce_string_list(ai_row.get(field_name))
        else:
            merged[field_name] = _merge_list_field(base_row.get(field_name), ai_row.get(field_name))
    for field_name in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
    ):
        if not merged.get(field_name) and ai_row.get(field_name):
            merged[field_name] = ai_row[field_name]
    merged["drawers_soft_close"] = _merge_soft_close_field(base_row.get("drawers_soft_close", ""), ai_row.get("drawers_soft_close", ""), keyword="drawer")
    merged["hinges_soft_close"] = _merge_soft_close_field(base_row.get("hinges_soft_close", ""), ai_row.get("hinges_soft_close", ""), keyword="hinge")
    merged["confidence"] = max(_safe_float(base_row.get("confidence", 0)), _safe_float(ai_row.get("confidence", 0)))
    return merged


def _merge_list_field(left: Any, right: Any) -> list[str]:
    values = parsing._coerce_string_list(left) + parsing._coerce_string_list(right)
    deduped: list[str] = []
    for value in values:
        if value and value not in deduped:
            deduped.append(value)
    return deduped


def _merge_soft_close_field(left: Any, right: Any, keyword: str) -> str:
    left_value = parsing.normalize_soft_close_value(left, keyword=keyword) or parsing.normalize_soft_close_value(left)
    right_value = parsing.normalize_soft_close_value(right, keyword=keyword) or parsing.normalize_soft_close_value(right)
    if left_value == "Not Soft Close" or right_value == "Not Soft Close":
        return "Not Soft Close"
    if left_value == "Soft Close" or right_value == "Soft Close":
        return "Soft Close"
    return ""


def _merge_appliances(base_rows: list[dict[str, Any]], ai_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not ai_rows:
        return base_rows
    merged_rows: list[dict[str, Any]] = []
    used_ai: set[int] = set()
    for base_row in base_rows:
        match_index = _find_best_appliance_match(base_row, ai_rows, used_ai)
        if match_index is None:
            merged_rows.append(base_row)
            continue
        used_ai.add(match_index)
        ai_row = ai_rows[match_index]
        merged_rows.append(_merge_single_appliance(base_row, ai_row))
    for index, ai_row in enumerate(ai_rows):
        if index not in used_ai:
            merged_rows.append(ai_row)
    return _dedupe_merged_appliances(merged_rows)


def _find_best_appliance_match(base_row: dict[str, Any], ai_rows: list[dict[str, Any]], used_ai: set[int]) -> int | None:
    base_type = _norm(base_row.get("appliance_type", ""))
    if not base_type:
        return None
    best_index: int | None = None
    best_score = 0
    for index, ai_row in enumerate(ai_rows):
        if index in used_ai:
            continue
        if _norm(ai_row.get("appliance_type", "")) != base_type:
            continue
        score = 1
        if _norm(base_row.get("source_file", "")) and _norm(base_row.get("source_file", "")) == _norm(ai_row.get("source_file", "")):
            score += 3
        if _norm(base_row.get("page_refs", "")) and _norm(base_row.get("page_refs", "")) == _norm(ai_row.get("page_refs", "")):
            score += 2
        if _norm(base_row.get("make", "")) and _norm(base_row.get("make", "")) == _norm(ai_row.get("make", "")):
            score += 2
        if _norm(base_row.get("model_no", "")) and _norm(base_row.get("model_no", "")) == _norm(ai_row.get("model_no", "")):
            score += 4
        if _overlapping_evidence(base_row.get("evidence_snippet", ""), ai_row.get("evidence_snippet", "")):
            score += 1
        if score > best_score:
            best_index = index
            best_score = score
    return best_index


def _merge_single_appliance(base_row: dict[str, Any], ai_row: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base_row)
    for field_name in (
        "make",
        "model_no",
        "product_url",
        "spec_url",
        "manual_url",
        "website_url",
        "overall_size",
        "source_file",
        "page_refs",
        "evidence_snippet",
    ):
        if not merged.get(field_name) and ai_row.get(field_name):
            merged[field_name] = ai_row[field_name]
    merged["confidence"] = max(_safe_float(base_row.get("confidence", 0)), _safe_float(ai_row.get("confidence", 0)))
    return merged


def _merge_other_fields(base_other: dict[str, Any], ai_other: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base_other)
    for key, value in ai_other.items():
        if value and not merged.get(key):
            merged[key] = value
    return merged


def _merge_warning_lists(base_warnings: list[str], ai_warnings: list[str]) -> list[str]:
    return list(dict.fromkeys(base_warnings + ai_warnings))


def _dedupe_merged_appliances(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in rows:
        key = (
            _norm(row.get("appliance_type", "")),
            _norm(row.get("make", "")),
            _norm(row.get("model_no", "")),
            _norm(row.get("source_file", "")),
            _norm(row.get("page_refs", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def _overlapping_evidence(left: Any, right: Any) -> bool:
    left_text = _norm(str(left or ""))[:48]
    right_text = _norm(str(right or ""))[:48]
    return bool(left_text and right_text and (left_text in right_text or right_text in left_text))


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _truncate_note(value: Exception) -> str:
    return str(value).strip().replace("\n", " ")[:180]


def _preview_output(value: str) -> str:
    return value.strip().replace("\n", " ")[:120]


def _report_progress(progress_callback: ProgressCallback, stage: str, message: str) -> None:
    if progress_callback:
        progress_callback(stage, message)


CLARENDON_STABLE_GROUPS = {
    "pantry": "butlers_pantry",
    "wip": "butlers_pantry",
    "bathroom": "vanities",
    "ensuite": "vanities",
    "powder": "vanities",
    "vanity": "vanities",
    "wir": "",
    "robe": "",
}

CLARENDON_STABLE_ORDER = ["kitchen", "butlers_pantry", "vanities", "laundry", "theatre", "rumpus", "study", "office", "kitchenette"]

CLARENDON_STABLE_LABELS = {
    "kitchen": "Kitchen",
    "butlers_pantry": "Butler's Pantry",
    "vanities": "Vanities",
    "laundry": "Laundry",
    "theatre": "Theatre",
    "rumpus": "Rumpus",
    "study": "Study",
    "office": "Office",
    "kitchenette": "Kitchenette",
}

CLARENDON_ROOM_PRIORITY = {
    "kitchen": 100,
    "butlers_pantry": 100,
    "pantry": 80,
    "wip": 60,
    "vanity": 100,
    "bathroom": 80,
    "ensuite": 70,
    "powder": 60,
    "laundry": 100,
    "theatre": 100,
    "rumpus": 100,
    "study": 100,
    "office": 100,
    "kitchenette": 100,
}

CLARENDON_SCHEDULE_PAGE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bKITCHEN COLOUR SCHEDULE\b", "kitchen"),
    (r"\bBUTLERS?\s+PANTRY COLOUR SCHEDULE\b", "butlers_pantry"),
    (r"\bVANITIES COLOUR SCHEDULE\b", "vanities"),
    (r"\bLAUNDRY COLOUR SCHEDULE\b", "laundry"),
    (r"\bTHEATRE(?: ROOM)? COLOUR SCHEDULE\b", "theatre"),
    (r"\bRUMPUS(?: ROOM)? COLOUR SCHEDULE\b", "rumpus"),
    (r"\bRUMPUS\s*-\s*DESK JOINERY COLOUR SCHEDULE\b", "rumpus"),
    (r"\bSTUDY COLOUR SCHEDULE\b", "study"),
    (r"\bOFFICE COLOUR SCHEDULE\b", "office"),
    (r"\bKITCHENETTE COLOUR SCHEDULE\b", "kitchenette"),
)

CLARENDON_FIELD_STOP_MARKERS = (
    r"BENCHTOP(?: COLOUR \d+)?\s*-",
    r"DOOR COLOUR(?: \d+)?\s*-",
    r"THERMOLAMINATE NOTES",
    r"CARCASS",
    r"STANDARD WHITE",
    r"HANDLES?\s*-",
    r"HANDLE \d+\s*-",
    r"DOOR HINGES",
    r"DRAWER RUNNERS",
    r"ACCESSORIES",
    r"APPLIANCES",
    r"Sink Type/Model:",
    r"Vanity Inset Basin",
    r"Drop in Tub:",
    r"Docusign Envelope ID",
    r"Client Signature",
)

CLARENDON_METADATA_MARKERS = (
    "client signature",
    "date of signed dwgs",
    "dwg. by",
    "scale:",
    "sheet ",
    "product:",
    "job no",
    "site address",
    "forstan pty ltd",
    "docusign envelope id",
    "aest",
    "page ",
    "category supplier description design comments",
    "all dimensions in millimetres",
    "drawings are indicative",
    "po. box 8248",
    "phone :",
    "fax :",
    "abn :",
)

CLARENDON_NOISE_PATTERNS = (
    r"(?i)SINKCUT OUTCENTRE.*$",
    r"(?i)\b\d+\s*MM\s*UP/?\s*DOWN TO DOORS?.*$",
    r"(?i)\bCUT-?OUT\b.*$",
    r"(?i)\bBENCHCUT-?OUT\b.*$",
    r"(?i)\bTO CABINET UNDER CUT OUT DETAIL FOR\b",
    r"(?i)\bCABINETRY - REFER TO [\"']?YOUR HOME KITCHENS[\"']?\b.*$",
    r"(?i)\bNOTE\s*:\s*35mm DIA\.? TAP CUT OUT\b.*$",
    r"(?i)\bFRAME WALL TO CTR OF BASIN\b.*$",
    r"(?i)\bCTR TO BASIN\b.*$",
    r"(?i)\bPROFILED END PANEL\b.*$",
    r"(?i)\bTHERMO FILLER\b.*$",
    r"(?i)\b865SLAB\b.*$",
)

CLARENDON_EXTERNAL_HANDLE_NOISE = (
    "external laundry door",
    "external garage door",
    "acrylic render",
    "entry frame",
    "entry door",
    "meter box",
    "eaves:",
    "lightweight cladding",
    "gainsborough",
    "garage door",
    "deadbolt",
    "painted",
    "corinthian",
    "hume",
)

CLARENDON_MINOR_WORDS = {
    "and",
    "or",
    "of",
    "to",
    "the",
    "by",
    "as",
    "with",
    "on",
    "in",
    "at",
    "for",
    "up",
    "down",
}


def _apply_builder_specific_polish(
    snapshot: dict[str, Any],
    documents: list[dict[str, object]],
    builder_name: str,
    parser_strategy: str,
    rule_flags: Any = None,
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    if "clarendon" not in builder_name.strip().lower():
        return snapshot
    return _apply_clarendon_reference_polish(
        snapshot,
        documents,
        builder_name=builder_name,
        parser_strategy=parser_strategy,
        rule_flags=rule_flags,
        progress_callback=progress_callback,
    )


def _apply_clarendon_reference_polish(
    snapshot: dict[str, Any],
    documents: list[dict[str, object]],
    builder_name: str,
    parser_strategy: str,
    rule_flags: Any = None,
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    if parser_strategy not in {"stable_hybrid", cleaning_rules.global_parser_strategy()} or "clarendon" not in builder_name.strip().lower():
        return snapshot
    _report_progress(progress_callback, "clarendon_polish", "Applying Clarendon 37016-style field polish")
    overlays = _collect_clarendon_polish_overlays(documents)
    polished_rooms = [
        _polish_clarendon_room(dict(room), overlays.get(str(room.get("room_key", "")), {}))
        for room in snapshot.get("rooms", [])
        if isinstance(room, dict)
    ]
    polished = dict(snapshot)
    polished["rooms"] = polished_rooms
    return parsing.apply_snapshot_cleaning_rules(polished, rule_flags=rule_flags)


def _collect_clarendon_polish_overlays(documents: list[dict[str, object]]) -> dict[str, dict[str, Any]]:
    overlays: dict[str, dict[str, Any]] = {}
    for document in documents:
        for page in document.get("pages", []):
            text = parsing.normalize_space(str(page.get("text") or ""))
            if not text:
                continue
            schedule_room_key = _clarendon_schedule_room_key(text)
            if schedule_room_key:
                _merge_clarendon_overlay(
                    overlays.setdefault(schedule_room_key, _blank_clarendon_overlay()),
                    _extract_clarendon_schedule_overlay(schedule_room_key, text),
                )
            for room_key, overlay in _extract_clarendon_fixture_overlays(text).items():
                _merge_clarendon_overlay(
                    overlays.setdefault(room_key, _blank_clarendon_overlay()),
                    overlay,
                )
    return overlays


def _blank_clarendon_overlay() -> dict[str, Any]:
    return {
        "bench_tops_wall_run": "",
        "bench_tops_island": "",
        "bench_tops_other": "",
        "bench_tops": [],
        "door_panel_colours": [],
        "door_colours_overheads": "",
        "door_colours_base": "",
        "door_colours_island": "",
        "door_colours_bar_back": "",
        "toe_kick": "",
        "bulkheads": "",
        "handles": [],
        "sink_info": "",
        "basin_info": "",
        "tap_info": "",
        "splashback": "",
        "drawers_soft_close": "",
        "hinges_soft_close": "",
    }


def _merge_clarendon_overlay(target: dict[str, Any], candidate: dict[str, Any]) -> None:
    for key in ("bench_tops", "door_panel_colours", "handles"):
        values = list(target.get(key, []))
        for value in candidate.get(key, []):
            text = parsing.normalize_space(str(value or ""))
            if text and text not in values:
                values.append(text)
        target[key] = values
    for key in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
        "toe_kick",
        "bulkheads",
        "sink_info",
        "basin_info",
        "tap_info",
        "splashback",
    ):
        target[key] = parsing._merge_text(target.get(key, ""), candidate.get(key, ""))
    target["drawers_soft_close"] = _merge_soft_close_field(
        target.get("drawers_soft_close", ""),
        candidate.get("drawers_soft_close", ""),
        keyword="drawer",
    )
    target["hinges_soft_close"] = _merge_soft_close_field(
        target.get("hinges_soft_close", ""),
        candidate.get("hinges_soft_close", ""),
        keyword="hinge",
    )


def _clarendon_schedule_room_key(text: str) -> str:
    for pattern, room_key in CLARENDON_SCHEDULE_PAGE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return room_key
    return ""


def _extract_clarendon_schedule_overlay(room_key: str, text: str) -> dict[str, Any]:
    overlay = _blank_clarendon_overlay()
    benchtop_segments = _extract_clarendon_labeled_segments(
        text,
        r"BENCHTOP(?: COLOUR \d+)?",
        CLARENDON_FIELD_STOP_MARKERS,
    )
    for segment in benchtop_segments:
        cleaned = _clarendon_clean_benchtop_text(segment)
        if not cleaned:
            continue
        lowered = segment.lower()
        if room_key == "kitchen" and any(token in lowered for token in ("cooktop run", "wall run", "wall bench", "wall side")):
            overlay["bench_tops_wall_run"] = parsing._merge_text(overlay["bench_tops_wall_run"], cleaned)
        elif room_key == "kitchen" and "island" in lowered:
            overlay["bench_tops_island"] = parsing._merge_text(overlay["bench_tops_island"], cleaned)
        else:
            overlay["bench_tops_other"] = parsing._merge_text(overlay["bench_tops_other"], cleaned)
        overlay["bench_tops"].append(cleaned)

    door_segments = _extract_clarendon_labeled_segments(
        text,
        r"DOOR COLOUR(?: \d+)?",
        CLARENDON_FIELD_STOP_MARKERS,
    )
    if door_segments:
        groups = parsing._split_door_colour_groups(door_segments)
        overlay["door_panel_colours"] = list(parsing._rebuild_door_panel_colours(groups))
        for key in ("door_colours_overheads", "door_colours_base", "door_colours_island", "door_colours_bar_back"):
            overlay[key] = parsing._merge_text(overlay[key], groups.get(key, ""))

    notes_block = _extract_clarendon_notes_block(text)
    if notes_block:
        toe_kick = _clarendon_extract_note_value(notes_block, "KICKBOARDS")
        bulkhead = _clarendon_extract_note_value(notes_block, "BULKHEAD SHADOWLINE")
        if toe_kick:
            overlay["toe_kick"] = parsing._merge_text(overlay["toe_kick"], _clarendon_clean_toe_kick_text(toe_kick))
        if bulkhead:
            overlay["bulkheads"] = parsing._merge_text(overlay["bulkheads"], _clarendon_clean_bulkhead_text(bulkhead))

    handle_segments = _extract_clarendon_handle_segments(text)
    overlay["handles"] = _clarendon_merge_unique_list(overlay["handles"], handle_segments)

    hinges_segment = _extract_clarendon_single_segment(text, r"DOOR HINGES", CLARENDON_FIELD_STOP_MARKERS)
    drawers_segment = _extract_clarendon_single_segment(text, r"DRAWER RUNNERS", CLARENDON_FIELD_STOP_MARKERS)
    if hinges_segment:
        overlay["hinges_soft_close"] = parsing.normalize_soft_close_value(hinges_segment, keyword="hinge") or parsing.normalize_soft_close_value(hinges_segment)
    if drawers_segment:
        overlay["drawers_soft_close"] = parsing.normalize_soft_close_value(drawers_segment, keyword="drawer") or parsing.normalize_soft_close_value(drawers_segment)
    return overlay


def _extract_clarendon_fixture_overlays(text: str) -> dict[str, dict[str, Any]]:
    overlays: dict[str, dict[str, Any]] = {}
    kitchen_segment = _extract_between_markers(
        text,
        start_marker=r"KITCHEN SUPPLIER DESCRIPTION DESIGN COMMENTS",
        end_markers=(r"Sink Type/Model\s*:", r"BUTLERS PANTRY", r"WALK IN PANTRY"),
    )
    if kitchen_segment:
        overlay = overlays.setdefault("kitchen", _blank_clarendon_overlay())
        sink = _extract_clarendon_value_after_label(kitchen_segment, r"Sink Type")
        tap = _extract_clarendon_value_after_label(kitchen_segment, r"Tap Type")
        if sink:
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], _clarendon_clean_sink_text(sink))
        if tap:
            overlay["tap_info"] = parsing._merge_text(overlay["tap_info"], _clarendon_clean_tap_text(tap))
        if "splashback:" in kitchen_segment.lower():
            overlay["splashback"] = parsing._merge_text(overlay["splashback"], "Tiled splashback by others")

    butlers_segment = _extract_between_markers(
        text,
        start_marker=r"Sink Type/Model\s*:",
        end_markers=(r"BUTLERS PANTRY", r"Client Signature", r"$"),
        include_start=True,
    )
    if butlers_segment:
        overlay = overlays.setdefault("butlers_pantry", _blank_clarendon_overlay())
        sink = _extract_clarendon_value_after_label(butlers_segment, r"Sink Type/Model")
        tap = _extract_clarendon_value_after_label(butlers_segment, r"Tap Type")
        if sink:
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], _clarendon_clean_sink_text(sink))
        if tap:
            overlay["tap_info"] = parsing._merge_text(overlay["tap_info"], _clarendon_clean_tap_text(tap))

    if re.search(r"Vanity Inset Basin", text, re.IGNORECASE):
        overlay = overlays.setdefault("vanities", _blank_clarendon_overlay())
        basin = _extract_clarendon_value_after_label(text, r"Vanity Inset Basin")
        tap = _extract_clarendon_value_after_label(text, r"Vanity Tap Style")
        if basin:
            overlay["basin_info"] = parsing._merge_text(overlay["basin_info"], _clarendon_clean_basin_text(basin))
        if tap:
            overlay["tap_info"] = parsing._merge_text(overlay["tap_info"], _clarendon_clean_tap_text(tap))

    laundry_segment = _extract_between_markers(
        text,
        start_marker=r"LAUNDRY SUPPLIER DESCRIPTION DESIGN COMMENTS",
        end_markers=(r"ENSUITE\s+\d+", r"Client Signature", r"$"),
    )
    if laundry_segment:
        overlay = overlays.setdefault("laundry", _blank_clarendon_overlay())
        sink = _extract_clarendon_value_after_label(laundry_segment, r"Drop in Tub")
        sink_detail = _extract_first_pattern(laundry_segment, r"EVERHARD INDUSTRIES.+?\([A-Z0-9.-]+\)")
        tap_detail = _extract_first_pattern(laundry_segment, r"PINA SINK MIXER.+?\([A-Z0-9.-]+\)")
        washing_tap = _extract_first_pattern(laundry_segment, r"\d+MM CP QUARTER TURN WASHING MACHINE COCK\s*\([A-Z0-9.-]+\)")
        if sink_detail:
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], _clarendon_clean_sink_text(f"{sink_detail} drop-in tub"))
        elif sink:
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], _clarendon_clean_sink_text(sink))
        if tap_detail:
            cleaned_tap = _clarendon_clean_tap_text(tap_detail)
            if washing_tap:
                cleaned_tap = f"{cleaned_tap}; {_clarendon_clean_tap_text(washing_tap)}"
            overlay["tap_info"] = parsing._merge_text(overlay["tap_info"], cleaned_tap)
        if "splashback:" in laundry_segment.lower():
            overlay["splashback"] = parsing._merge_text(overlay["splashback"], "Tiled splashback by others")

    return overlays


def _polish_clarendon_room(row: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    room_key = parsing.normalize_space(str(row.get("room_key", "")))
    polished = dict(row)
    overlay_present = _clarendon_overlay_has_content(overlay)

    current_benchtops = " | ".join(_clarendon_clean_benchtop_text(value) for value in parsing._coerce_string_list(row.get("bench_tops", [])) if _clarendon_clean_benchtop_text(value))
    wall_run = _clarendon_clean_benchtop_text(overlay.get("bench_tops_wall_run", ""))
    island = _clarendon_clean_benchtop_text(overlay.get("bench_tops_island", ""))
    other = _clarendon_clean_benchtop_text(overlay.get("bench_tops_other", ""))
    if room_key == "kitchen":
        polished["bench_tops_wall_run"] = wall_run or _clarendon_clean_benchtop_text(row.get("bench_tops_wall_run", ""))
        polished["bench_tops_island"] = island or _clarendon_clean_benchtop_text(row.get("bench_tops_island", ""))
        polished["bench_tops_other"] = other if (wall_run or island or other) else _clarendon_clean_benchtop_text(row.get("bench_tops_other", ""))
        if not polished["bench_tops_other"] and current_benchtops and not (polished["bench_tops_wall_run"] or polished["bench_tops_island"]):
            polished["bench_tops_other"] = current_benchtops
    else:
        folded = parsing._merge_text(other, parsing._merge_text(wall_run, island)) or " | ".join(_clarendon_clean_benchtop_text(value) for value in overlay.get("bench_tops", []) if _clarendon_clean_benchtop_text(value))
        polished["bench_tops_other"] = folded or current_benchtops
        polished["bench_tops_wall_run"] = ""
        polished["bench_tops_island"] = ""
    polished["bench_tops"] = parsing._rebuild_benchtop_entries(polished)

    overlay_has_door_groups = any(parsing.normalize_space(overlay.get(key, "")) for key in ("door_colours_overheads", "door_colours_base", "door_colours_island", "door_colours_bar_back")) or bool(overlay.get("door_panel_colours"))
    grouped_doors = {
        "door_colours_overheads": _clarendon_clean_door_group_text(overlay.get("door_colours_overheads", "")) if overlay_has_door_groups else _clarendon_clean_door_group_text(row.get("door_colours_overheads", "")),
        "door_colours_base": _clarendon_clean_door_group_text(overlay.get("door_colours_base", "")) if overlay_has_door_groups else _clarendon_clean_door_group_text(row.get("door_colours_base", "")),
        "door_colours_island": _clarendon_clean_door_group_text(overlay.get("door_colours_island", "")) if overlay_has_door_groups else _clarendon_clean_door_group_text(row.get("door_colours_island", "")),
        "door_colours_bar_back": _clarendon_clean_door_group_text(overlay.get("door_colours_bar_back", "")) if overlay_has_door_groups else _clarendon_clean_door_group_text(row.get("door_colours_bar_back", "")),
    }
    grouped_doors = parsing._prune_door_group_overlap(grouped_doors)
    for key, value in grouped_doors.items():
        polished[key] = value
    overlay_door_entries = overlay.get("door_panel_colours", [])
    if overlay_door_entries:
        polished["door_panel_colours"] = [_clarendon_clean_door_group_text(entry) for entry in overlay_door_entries if _clarendon_clean_door_group_text(entry)]
    else:
        polished["door_panel_colours"] = parsing._rebuild_door_panel_colours(polished)

    toe_kick = _clarendon_clean_toe_kick_text(overlay.get("toe_kick", "")) if overlay_present else _clarendon_clean_toe_kick_text(row.get("toe_kick", ""))
    bulkheads = _clarendon_clean_bulkhead_text(overlay.get("bulkheads", "")) if overlay_present else _clarendon_clean_bulkhead_text(row.get("bulkheads", ""))
    handles = _clarendon_clean_handles(overlay.get("handles", [])) if overlay_present else _clarendon_clean_handles(row.get("handles", []))
    polished["toe_kick"] = [toe_kick] if toe_kick else []
    polished["bulkheads"] = [bulkheads] if bulkheads else []
    polished["handles"] = handles

    polished["sink_info"] = _clarendon_clean_fixture_text(overlay.get("sink_info", ""), fixture_kind="sink") if overlay_present else _clarendon_clean_fixture_text(row.get("sink_info", ""), fixture_kind="sink")
    polished["basin_info"] = (
        _clarendon_clean_fixture_text(overlay.get("basin_info", ""), fixture_kind="basin")
        if room_key == "vanities" and overlay_present
        else (_clarendon_clean_fixture_text(row.get("basin_info", ""), fixture_kind="basin") if room_key == "vanities" else "")
    )
    polished["tap_info"] = _clarendon_clean_fixture_text(overlay.get("tap_info", ""), fixture_kind="tap") if overlay_present else _clarendon_clean_fixture_text(row.get("tap_info", ""), fixture_kind="tap")
    overlay_splashback = _clarendon_clean_splashback_text(overlay.get("splashback", ""), room_key=room_key)
    current_splashback = _clarendon_clean_splashback_text(row.get("splashback", ""), room_key=room_key)
    polished["splashback"] = overlay_splashback or (current_splashback if room_key in {"kitchen", "laundry"} else "")
    polished["drawers_soft_close"] = _clarendon_select_soft_close(overlay.get("drawers_soft_close", "") if overlay_present else "", row.get("drawers_soft_close", ""), keyword="drawer")
    polished["hinges_soft_close"] = _clarendon_select_soft_close(overlay.get("hinges_soft_close", "") if overlay_present else "", row.get("hinges_soft_close", ""), keyword="hinge")
    return polished


def _extract_clarendon_labeled_segments(text: str, label_pattern: str, stop_markers: tuple[str, ...]) -> list[str]:
    pattern = re.compile(
        rf"(?is)(?:{label_pattern})\s*-\s*(.+?)(?=(?:{'|'.join(stop_markers)})|$)"
    )
    return [parsing.normalize_space(match.group(1)) for match in pattern.finditer(text) if parsing.normalize_space(match.group(1))]


def _extract_clarendon_single_segment(text: str, label_pattern: str, stop_markers: tuple[str, ...]) -> str:
    segments = _extract_clarendon_labeled_segments(text, label_pattern, stop_markers)
    return segments[0] if segments else ""


def _extract_clarendon_notes_block(text: str) -> str:
    match = re.search(
        rf"(?is)THERMOLAMINATE NOTES\s*:\s*(.+?)(?=(?:CARCASS|STANDARD WHITE|HANDLES?\s*-|HETTICH|DOOR HINGES|DRAWER RUNNERS|Docusign Envelope ID|Client Signature|$))",
        text,
    )
    return parsing.normalize_space(match.group(1)) if match else ""


def _clarendon_extract_note_value(notes_block: str, label: str) -> str:
    match = re.search(rf"(?is){re.escape(label)}\s*:\s*([^*]+)", notes_block)
    return parsing.normalize_space(match.group(1)) if match else ""


def _extract_clarendon_handle_segments(text: str) -> list[str]:
    segments = _extract_clarendon_labeled_segments(text, r"HANDLES?|HANDLE \d+", CLARENDON_FIELD_STOP_MARKERS)
    return _clarendon_clean_handles(segments)


def _extract_between_markers(text: str, start_marker: str, end_markers: tuple[str, ...], include_start: bool = False) -> str:
    start_match = re.search(start_marker, text, re.IGNORECASE)
    if not start_match:
        return ""
    start_index = start_match.start() if include_start else start_match.end()
    remainder = text[start_index:]
    end_index = len(remainder)
    for marker in end_markers:
        match = re.search(marker, remainder, re.IGNORECASE)
        if match and match.start() < end_index:
            end_index = match.start()
    return parsing.normalize_space(remainder[:end_index])


def _extract_clarendon_value_after_label(text: str, label_pattern: str) -> str:
    match = re.search(
        rf"(?is){label_pattern}\s*:?\s*(.+?)(?=(?:Sink Type/Model|Sink Type|Tap Type|Tap Style|Vanity Tap Style|Tap for Fridge|Splashback|APPLIANCES|Vanity Waste Colour|Shower Tap Style|Drop in Tub|Washing Machine Taps|BUTLERS PANTRY|WALK IN PANTRY|LAUNDRY SUPPLIER DESCRIPTION DESIGN COMMENTS|Client Signature|$))",
        text,
    )
    return parsing.normalize_space(match.group(1)) if match else ""


def _extract_first_pattern(text: str, pattern: str) -> str:
    match = re.search(pattern, text, re.IGNORECASE)
    return parsing.normalize_space(match.group(0)) if match else ""


def _clarendon_clean_benchtop_text(value: Any) -> str:
    text = parsing.normalize_space(str(value or ""))
    if not text:
        return ""
    text = text.replace("_", " ")
    text = _clarendon_strip_metadata(text)
    for pattern in CLARENDON_NOISE_PATTERNS:
        text = re.sub(pattern, "", text)
    text = re.sub(r"(?i)^BENCHTOP COLOUR \d+\s*-\s*", "", text)
    text = re.sub(r"(?i)^BENCHTOP\s*-\s*", "", text)
    text = re.sub(r"(?i)\s*-\s*TO\s+(?:THE\s+)?(?:COOKTOP RUN|WALL RUN|WALL BENCH|WALL SIDE|ISLAND BENCH|ISLAND)\b.*$", "", text)
    text = parsing.normalize_brand_casing_text(text)
    text = _clarendon_to_readable_text(text)
    text = _clarendon_inline_text(text)
    if _looks_like_clarendon_noise(text):
        return ""
    return text.strip(" -;,")


def _clarendon_clean_door_group_text(value: Any) -> str:
    entries = parsing._split_group_entries(value)
    cleaned = []
    for entry in entries:
        text = parsing._clean_door_colour_value(entry)
        text = re.sub(r"(?i)\b(?:plain glass|'?glazing bar'?) display cabinet with.*$", "", text)
        text = re.sub(r"(?i)\bto tall open shelves\b.*$", "", text)
        text = _clarendon_to_readable_text(text)
        text = _clarendon_inline_text(text)
        if text:
            cleaned.append(text.strip(" -;,'\""))
    return " | ".join(entry for entry in cleaned if entry)


def _clarendon_clean_toe_kick_text(value: Any) -> str:
    text = _clarendon_clean_note_text(value)
    if not text:
        return ""
    lowered = text.lower()
    if "n/a" in lowered and "floating" in lowered:
        return "N/A floating - no kickboard"
    return text


def _clarendon_clean_bulkhead_text(value: Any) -> str:
    text = _clarendon_clean_note_text(value)
    lowered = text.lower()
    if "bulkhead shadowline" in lowered and "matching" in lowered:
        cleaned = re.sub(r"(?i)^bulkhead shadowline\s*:?\s*", "", text)
        cleaned = cleaned.strip(" -;,.")
        if cleaned:
            cleaned = re.sub(r"(?i)^as\s+", "", cleaned)
            return f"Bulkhead shadowline as {cleaned[:1].lower() + cleaned[1:]}"
    if "matching melamine finish" in lowered:
        return "Bulkhead shadowline as matching Melamine finish"
    return text


def _clarendon_clean_note_text(value: Any) -> str:
    text = parsing._string_value(value)
    text = _clarendon_strip_metadata(text)
    text = re.sub(r"(?i)^BY BUILDER", "", text)
    text = re.sub(r"(?i)BENCHTOP SHADOWLINE.*$", "", text)
    text = re.sub(r"(?i)TILED.*$", "", text)
    text = re.sub(r"(?i)\b\d{4,}\b.*$", "", text)
    text = parsing.normalize_brand_casing_text(text)
    text = _clarendon_to_readable_text(text)
    text = _clarendon_inline_text(text)
    if _looks_like_clarendon_noise(text):
        return ""
    return text.strip(" -;,")


def _clarendon_clean_splashback_text(value: Any, room_key: str = "") -> str:
    text = parsing._string_value(value)
    text = _clarendon_strip_metadata(text)
    if re.search(r"(?i)\bBY OTHERS\b", text) or re.search(r"(?i)\bBY OTHERS\d", text) or re.search(r"(?i)\bBEAUMONT TILES\b", text):
        if room_key in {"kitchen", "laundry"}:
            return "Tiled splashback by others"
        return ""
    if room_key in {"kitchen", "laundry"} and re.search(r"(?i)\bSPLASHBACK\b", text):
        return "Tiled splashback by others"
    text = re.sub(r"(?i)\b20MM STONE\b.*$", "", text)
    text = re.sub(r"(?i)BENCHTOP SHADOWLINE.*$", "", text)
    text = re.sub(r"(?i)KICKBOARD.*$", "", text)
    text = parsing.normalize_brand_casing_text(text)
    text = _clarendon_to_readable_text(text)
    text = _clarendon_inline_text(text)
    if _looks_like_clarendon_noise(text):
        return ""
    return text.strip(" -;,")


def _clarendon_clean_handles(value: Any) -> list[str]:
    cleaned = parsing._clean_handle_entries(parsing._coerce_string_list(value))
    result: list[str] = []
    for entry in cleaned:
        text = _clarendon_strip_metadata(entry)
        if any(noise in text.lower() for noise in CLARENDON_EXTERNAL_HANDLE_NOISE):
            continue
        text = re.sub(r"(?i)\s*-\s*\d+MM\s+IN\s+AND\s+\d+MM\s+UP\s*/?\s*DOWN\s+TO\s+DOORS\b.*$", "", text)
        text = re.sub(r"(?i)\s*-\s*DOOR LOCATION\s*:.*$", "", text)
        text = re.sub(r"(?i)\s*DRAWER LOCATION\s*:.*$", "", text)
        text = parsing.normalize_brand_casing_text(text)
        text = _clarendon_to_readable_text(text)
        text = _clarendon_inline_text(text)
        if text and text not in result and not _looks_like_clarendon_noise(text):
            result.append(text)
    return result


def _clarendon_clean_fixture_text(value: Any, fixture_kind: str) -> str:
    entries = parsing._split_group_entries(value)
    if not entries:
        text = parsing._string_value(value)
        entries = [text] if text else []
    cleaned_entries: list[str] = []
    for entry in entries:
        text = entry.replace("_", " ")
        text = _clarendon_strip_metadata(text)
        text = re.sub(r"(?i)^&\s*TAP TOCABINET UNDER CUT OUT DETAIL FOR\s*", "", text)
        text = re.sub(r"(?i)^TOCABINET UNDER CUT OUT DETAIL FOR\s*", "", text)
        text = re.sub(r"(?i)^Washing Machine Taps\s*:\s*", "", text)
        text = parsing.normalize_brand_casing_text(text)
        text = _clarendon_to_readable_text(text)
        text = _clarendon_inline_text(text)
        if fixture_kind == "sink":
            text = _clarendon_clean_sink_text(text)
        elif fixture_kind == "basin":
            text = _clarendon_clean_basin_text(text)
        elif fixture_kind == "tap":
            text = _clarendon_clean_tap_text(text)
        text = text.strip(" -;,")
        if not text or _looks_like_clarendon_noise(text):
            continue
        if any(text.lower() in existing.lower() for existing in cleaned_entries):
            continue
        cleaned_entries = [existing for existing in cleaned_entries if existing.lower() not in text.lower()]
        cleaned_entries.append(text)
    return " | ".join(cleaned_entries)


def _clarendon_clean_sink_text(value: Any) -> str:
    text = _clarendon_to_readable_text(parsing.normalize_brand_casing_text(str(value or "").replace("_", " ")))
    text = re.sub(r"(?i)\bUndermount\s*-\s*", "", text)
    text = re.sub(r"(?i)\bUNDERMOUNT\b$", "undermount sink", text)
    text = re.sub(r"(?i)\bDROP IN TUB\b$", "drop-in tub", text)
    return _clarendon_inline_text(text).strip(" -;,")


def _clarendon_clean_basin_text(value: Any) -> str:
    text = _clarendon_to_readable_text(parsing.normalize_brand_casing_text(str(value or "").replace("_", " ")))
    text = re.sub(r"(?i)\bVanity\b$", "", text)
    return _clarendon_inline_text(text).strip(" -;,")


def _clarendon_clean_tap_text(value: Any) -> str:
    text = _clarendon_to_readable_text(parsing.normalize_brand_casing_text(str(value or "").replace("_", " ")))
    text = re.sub(r"(?is)\bBasin Mixer to Be Installed.*$", "", text)
    return _clarendon_inline_text(text).strip(" -;,")


def _clarendon_strip_metadata(value: Any) -> str:
    text = parsing.normalize_space(str(value or ""))
    if not text:
        return ""
    lowered = text.lower()
    cut_index = len(text)
    for marker in CLARENDON_METADATA_MARKERS:
        index = lowered.find(marker)
        if index != -1:
            cut_index = min(cut_index, index)
    text = text[:cut_index]
    text = re.sub(r"(?i)\bDocusign Envelope ID\b.*$", "", text)
    text = re.sub(r"(?i)\bClient:\b.*$", "", text)
    text = re.sub(r"(?i)\bClient Signature\b.*$", "", text)
    text = re.sub(r"(?i)\bNOTE\s*:\s*ALL PLUMBING SETOUT DIMENSIONS.*$", "", text)
    text = re.sub(r"(?i)\bNOTE\s*:\s*DRAWINGS ARE INDICATIVE.*$", "", text)
    return parsing.normalize_space(text)


def _clarendon_inline_text(value: Any) -> str:
    return re.sub(r"\s+", " ", parsing.normalize_space(str(value or ""))).strip()


def _clarendon_to_readable_text(value: Any) -> str:
    text = parsing.normalize_space(str(value or ""))
    if not text:
        return ""
    parts = re.split(r"(\s+)", text)
    normalized: list[str] = []
    for part in parts:
        if not part or part.isspace():
            normalized.append(part)
            continue
        prefix_match = re.match(r"^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9]*)$", part)
        if not prefix_match:
            normalized.append(part)
            continue
        prefix, core, suffix = prefix_match.groups()
        normalized.append(f"{prefix}{_clarendon_case_token(core)}{suffix}")
    return parsing.normalize_space("".join(normalized))


def _clarendon_case_token(token: str) -> str:
    if not token:
        return token
    lowered = token.lower()
    if lowered in CLARENDON_MINOR_WORDS:
        return lowered
    if re.fullmatch(r"\d+(?:MM|CM|L)", token, re.IGNORECASE):
        return token.upper()
    if re.search(r"\d", token):
        return token.upper()
    if token.upper() in {"N/A", "PVC", "ABS", "ABSE", "GPO", "CTR", "EQ"}:
        return token.upper()
    if token.isupper() or token.islower():
        return token[:1].upper() + token[1:].lower()
    return token


def _looks_like_clarendon_noise(value: Any) -> bool:
    text = parsing.normalize_space(str(value or ""))
    if not text:
        return True
    lowered = text.lower()
    if any(marker in lowered for marker in CLARENDON_METADATA_MARKERS):
        return True
    if len(re.findall(r"\d{4,}", text)) >= 2:
        return True
    if re.search(r"(?i)\b(?:frame wall|ctr to basin|profiled end panel|docusign)\b", text):
        return True
    return False


def _clarendon_select_soft_close(overlay_value: Any, row_value: Any, keyword: str) -> str:
    overlay_clean = parsing.normalize_soft_close_value(overlay_value, keyword=keyword) or parsing.normalize_soft_close_value(overlay_value)
    if overlay_clean:
        return overlay_clean
    return parsing.normalize_soft_close_value(row_value, keyword=keyword) or parsing.normalize_soft_close_value(row_value)


def _clarendon_merge_unique_list(left: list[str], right: list[str]) -> list[str]:
    result = list(left)
    for value in right:
        text = parsing.normalize_space(str(value or ""))
        if text and text not in result:
            result.append(text)
    return result


def _clarendon_overlay_has_content(overlay: dict[str, Any]) -> bool:
    for key, value in overlay.items():
        if isinstance(value, list) and any(parsing.normalize_space(str(item or "")) for item in value):
            return True
        if not isinstance(value, list) and parsing.normalize_space(str(value or "")):
            return True
    return False


def _stabilize_snapshot_layout(snapshot: dict[str, Any], builder_name: str, parser_strategy: str) -> dict[str, Any]:
    if parser_strategy not in {"stable_hybrid", cleaning_rules.global_parser_strategy()} or "clarendon" not in builder_name.strip().lower():
        return snapshot
    rooms = [dict(row) for row in snapshot.get("rooms", []) if isinstance(row, dict)]
    snapshot["rooms"] = _compact_clarendon_rooms(rooms)
    return snapshot


def _compact_clarendon_rooms(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        source_key = parsing.normalize_room_key(str(row.get("room_key", "")))
        target_key = CLARENDON_STABLE_GROUPS.get(source_key, source_key)
        if not target_key:
            continue
        grouped.setdefault(target_key, []).append(dict(row))

    compacted: list[dict[str, Any]] = []
    for room_key in CLARENDON_STABLE_ORDER:
        if room_key not in grouped:
            continue
        candidates = sorted(
            grouped[room_key],
            key=lambda row: (
                CLARENDON_ROOM_PRIORITY.get(parsing.normalize_room_key(str(row.get("room_key", ""))), 0),
                _room_content_score(row),
            ),
            reverse=True,
        )
        merged = dict(candidates[0])
        for candidate in candidates[1:]:
            merged = _merge_single_room(merged, candidate, stable_hybrid=True)
        merged["room_key"] = room_key
        merged["original_room_label"] = CLARENDON_STABLE_LABELS.get(room_key, merged.get("original_room_label", room_key.title()))
        if room_key == "kitchen" or _room_has_meaningful_content(merged):
            compacted.append(merged)
    return compacted


def _room_has_meaningful_content(row: dict[str, Any]) -> bool:
    for field_name in (
        "bench_tops",
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_panel_colours",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
        "toe_kick",
        "bulkheads",
        "handles",
        "sink_info",
        "basin_info",
        "tap_info",
        "splashback",
        "flooring",
    ):
        if _field_has_value(row.get(field_name)):
            return True
    return False


def _field_has_value(value: Any) -> bool:
    if isinstance(value, (list, tuple, set)):
        return any(bool(parsing.normalize_space(str(item))) for item in value)
    return bool(parsing.normalize_space(str(value or "")))


def _room_content_score(row: dict[str, Any]) -> int:
    score = 0
    for field_name in (
        "bench_tops",
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_panel_colours",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
        "toe_kick",
        "bulkheads",
        "handles",
        "sink_info",
        "basin_info",
        "tap_info",
        "splashback",
        "flooring",
    ):
        if _field_has_value(row.get(field_name)):
            score += 1
    return score


def _enrich_snapshot_appliances(snapshot: dict[str, Any], progress_callback: ProgressCallback = None, rule_flags: Any = None) -> dict[str, Any]:
    appliances = [row for row in snapshot.get("appliances", []) if isinstance(row, dict)]
    if not appliances:
        snapshot["appliances"] = []
        return snapshot
    snapshot["appliances"] = appliance_official.enrich_appliance_rows(appliances, progress_callback=progress_callback, rule_flags=rule_flags)
    return snapshot
