from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

from App.services import parsing
from App.services.runtime import OPENAI_API_KEY, OPENAI_ENABLED, OPENAI_MODEL, utc_now_iso


def build_spec_snapshot(job: dict[str, Any], builder: dict[str, Any], files: list[dict[str, Any]], template_files: list[dict[str, Any]]) -> dict[str, Any]:
    documents = _load_documents(files, role="spec")
    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="spec", documents=documents)
    ai_result = _try_openai(job, builder, documents, template_files, source_kind="spec")
    return _merge_ai_result(heuristic, ai_result) if ai_result else heuristic


def build_drawing_snapshot(job: dict[str, Any], builder: dict[str, Any], files: list[dict[str, Any]]) -> dict[str, Any]:
    documents = _load_documents(files, role="drawing")
    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="drawing", documents=documents)
    ai_result = _try_openai(job, builder, documents, [], source_kind="drawing")
    return _merge_ai_result(heuristic, ai_result) if ai_result else heuristic


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
) -> dict[str, Any] | None:
    if not OPENAI_ENABLED or not OPENAI_API_KEY:
        return None

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
            "drawers_soft_close, hinges_soft_close, splashback, flooring, source_file, page_refs, evidence_snippet, confidence. "
            "Appliance rows must include appliance_type, make, model_no, website_url, overall_size, source_file, page_refs, evidence_snippet, confidence."
        ),
        "templates": "\n\n".join(template_text)[:18000],
        "documents": "\n\n".join(combined_text)[:60000],
    }

    try:
        response_json = _post_responses_api(prompt)
    except Exception:
        return None

    output_text = _extract_output_text(response_json)
    if not output_text:
        return None
    try:
        parsed = json.loads(output_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    parsed.setdefault("job_no", job["job_no"])
    parsed.setdefault("builder_name", builder["name"])
    parsed.setdefault("source_kind", source_kind)
    parsed.setdefault("generated_at", utc_now_iso())
    parsed.setdefault(
        "source_documents",
        [{"file_name": str(doc["file_name"]), "role": str(doc["role"])} for doc in documents],
    )
    return parsed


def _post_responses_api(prompt: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(
        {
            "model": OPENAI_MODEL,
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
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
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


def _merge_ai_result(base: dict[str, Any], ai_result: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key in ("rooms", "appliances", "others", "warnings"):
        if ai_result.get(key):
            merged[key] = ai_result[key]
    if base.get("warnings") and ai_result.get("warnings"):
        merged["warnings"] = list(dict.fromkeys(list(base["warnings"]) + list(ai_result["warnings"])))
    merged["source_documents"] = base.get("source_documents", [])
    return merged
