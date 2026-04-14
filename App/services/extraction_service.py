from __future__ import annotations

import base64
from dataclasses import dataclass, field
import functools
import html
import json
import os
import re
import tempfile
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Callable

from pypdf import PdfReader, PdfWriter

from App.services import appliance_official
from App.services import cleaning_rules, parsing, runtime


ProgressCallback = Callable[[str, str], None] | None

_DOCLING_CONVERTER: Any = None
_DOCLING_CONVERTER_CLASS: Any = None
_DOCLING_IMPORT_ERROR: Exception | None = None


EXTRA_LAYOUT_ROW_LABELS: tuple[str, ...] = (
    "Manufacturer",
    "Range",
    "Profile",
    "Finish",
    "Colour",
    "Colour & Finish",
    "Category",
    "Model",
    "Type",
    "Location",
    "Fixing",
    "Style",
    "Mechanism",
    "Underlay",
    "Edge Profile",
    "Island Edge Profile",
    "Wall Run Benchtop",
    "Waterfall End to Island",
    "Underbench",
    "Contrasting Facings",
    "Overhead Cupboards",
    "Shaving Cabinets",
    "Door Handle",
    "Drawer Handle",
    "Base Cabinetry Handles",
    "Pantry Door Handle",
    "Bin & Pot Drawers Handle",
    "Standard",
    "Pot",
    "Bin",
    "Integrated Appliances",
    "Range hood",
    "Rangehood",
    "Cooktop",
    "Oven",
    "Wall Run Base Cabinet Panels",
    "Wall Run Kickboard",
    "Island/Penisula Benchtop",
    "Island/Penisula Base Cabinet Panels",
    "Island/Penisula Kickboard",
    "Island/Penisula Feature Panels",
    "Cabinetry Handles",
    "Overhead Cabinetry Handles",
    "Shelving",
    "Shadowline",
    "Cabinet Panels",
    "Mirror",
    "Kitchen Sink",
    "Kitchen Tapware",
    "Pantry Sink",
    "Pantry Tapware",
    "Laundry Trough",
    "Laundry Tapware",
    "Washing Machine Taps",
    "Vanity Basin",
    "Vanity Basin Tapware",
    "Feature Waste",
    "Tub",
    "Tub Mixer",
    "Basin Mixer",
    "Bath",
    "Bath Mixer / Spout",
    "Bath Spout Model",
    "Shower Base",
    "Shower Frame",
    "Shower Mixer",
    "Shower Rose",
    "Robe Hook",
    "Hand Towel Rail",
    "Towel Rail",
    "Floor Waste",
    "Selection Required",
    "Drawers",
    "Contrasting Facings",
    "Robe Fitout",
    "Robe Hanging Rail",
    "Hanging Rail",
    "Additional Wet Area",
    "Additional Bath/Ensuite/Powder",
    "Accessories & Toilet Suite",
    "Toilet Suite",
    "Toilet Roll Holder",
    "Wet Area Location",
    "Hinges & Drawer Runners",
    "Floor Type & Kick refacing required",
    "GPO'S",
    "Hamper",
)

LAYOUT_ROW_LABELS: tuple[str, ...] = tuple(
    sorted(
        set(parsing.FIELD_LABELS) | set(EXTRA_LAYOUT_ROW_LABELS) | {"SINKWARE", "TAPWARE", "APPLIANCES", "AREA / ITEM"},
        key=len,
        reverse=True,
    )
)

_LAST_OPENAI_REQUEST_AT = 0.0


def _normalized_builder_key(builder_name: str) -> str:
    return parsing.normalize_space(builder_name).lower()


PAGE_FAMILY_PROVIDER_MATRIX: dict[str, dict[str, dict[str, bool | str]]] = {
    "imperial": {
        "joinery_material_sheet": {"table_grid_first": True, "vision_grid_default": True, "docling_default": True},
        "special_section_page": {"table_grid_first": True, "vision_grid_default": True, "docling_default": True},
        "sinkware_tapware_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "appliance_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
    },
    "yellowwood": {
        "joinery_material_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "colour_schedule": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "sinkware_tapware_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "appliance_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
    },
    "simonds": {
        "grouped_property_schedule": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "sinkware_tapware_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "appliance_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
    },
    "evoca": {
        "grouped_property_schedule": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "sinkware_tapware_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
        "appliance_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": True},
    },
    "clarendon": {
        "colour_schedule": {"table_grid_first": True, "vision_grid_default": False, "docling_default": False},
        "sinkware_tapware_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": False},
        "appliance_sheet": {"table_grid_first": True, "vision_grid_default": False, "docling_default": False},
        "drawing_page": {"table_grid_first": False, "vision_grid_default": False, "docling_default": False},
    },
}


def _classify_spec_page_family(builder_name: str, text: str) -> str:
    upper = str(text or "").upper()
    builder_key = _normalized_builder_key(builder_name)
    if not upper:
        return "unknown"
    if "DRAWINGS AND COLOURS" in upper or ("AMENDED DRAWINGS" in upper and builder_key == "clarendon"):
        return "drawing_page"
    if "SINKWARE & TAPWARE" in upper or "SINKWARE (" in upper or "TAPWARE (" in upper or "PLUMBING FIXTURES & TAPWARE" in upper:
        return "sinkware_tapware_sheet"
    if "APPLIANCES" in upper or "17 APPLIANCES" in upper:
        return "appliance_sheet"
    if builder_key == "imperial" and (
        "FEATURE TALL DOORS" in upper
        or "FEATURE TALL CABINETS" in upper
        or "FEATURE JOINERY SELECTION SHEET" in upper
        or "CUSTOM HANDLES" in upper
    ):
        return "special_section_page"
    if builder_key == "imperial" and _looks_like_imperial_joinery_material_page(upper):
        return "joinery_material_sheet"
    if builder_key == "yellowwood" and any(marker in upper for marker in ("JOINERY", "CABINETRY", "COLOUR SCHEDULE", "BASE CUPBOARDS", "OVERHEAD CUPBOARDS")):
        return "joinery_material_sheet"
    if builder_key == "clarendon" and "COLOUR SCHEDULE" in upper:
        return "colour_schedule"
    if builder_key in {"simonds", "evoca"} and any(marker in upper for marker in ("15 CABINETS", "MANUFACTURER", "COLOUR & FINISH", "PROFILE", "CATEGORY")):
        return "grouped_property_schedule"
    if "COLOUR SCHEDULE" in upper or "JOINERY SELECTION SHEET" in upper:
        return "joinery_material_sheet"
    return "unknown"


def _page_family_strategy(builder_name: str, text: str) -> dict[str, bool | str]:
    builder_key = _normalized_builder_key(builder_name)
    family = _classify_spec_page_family(builder_name, text)
    defaults: dict[str, bool | str] = {"page_family": family, "table_grid_first": False, "vision_grid_default": False, "docling_default": False}
    strategy = PAGE_FAMILY_PROVIDER_MATRIX.get(builder_key, {}).get(family)
    if strategy:
        return {**defaults, **strategy}
    return defaults


def _job_matches_runtime_override(job_no: str, overrides: set[str]) -> bool:
    return parsing.normalize_space(job_no).lower() in overrides


def _spec_docling_enabled(builder_name: str, source_kind: str) -> bool:
    if source_kind != "spec":
        return False
    normalized_builder = _normalized_builder_key(builder_name)
    if normalized_builder in runtime.SPEC_DOCLING_BUILDERS:
        return True
    for policy_builder in runtime.SPEC_DOCLING_BUILDERS:
        if re.search(rf"(?<![a-z0-9]){re.escape(policy_builder)}(?![a-z0-9])", normalized_builder):
            return True
    return False


def _spec_heavy_vision_enabled(job_no: str, source_kind: str, builder_name: str = "") -> bool:
    if source_kind != "spec":
        return runtime.OPENAI_VISION_ENABLED
    if _is_imperial_builder_name(builder_name):
        return True
    if runtime.SPEC_HEAVY_VISION_ENABLED:
        return True
    return _job_matches_runtime_override(job_no, runtime.FORCE_SPEC_HEAVY_VISION_JOBS)


def _spec_openai_merge_enabled(job_no: str, source_kind: str) -> bool:
    if source_kind != "spec":
        return runtime.OPENAI_ENABLED
    if runtime.SPEC_OPENAI_MERGE_ENABLED:
        return True
    return _job_matches_runtime_override(job_no, runtime.FORCE_SPEC_OPENAI_MERGE_JOBS)


def _is_imperial_builder_name(builder_name: str) -> bool:
    return _normalized_builder_key(builder_name) == "imperial"

INVALID_ROOM_HEADING_TOKENS: tuple[str, ...] = (
    "manufacturer",
    "range",
    "profile",
    "finish",
    "colour",
    "colour & finish",
    "category",
    "model",
    "type",
    "location",
    "fixing",
    "style",
    "mechanism",
    "underlay",
    "edge profile",
    "waterfall",
    "sink",
    "tapware",
    "tap",
    "mixer",
    "basin",
    "bath",
    "shower",
    "toilet",
    "floor waste",
    "robe hook",
    "towel rail",
    "mirror",
    "desk",
    "floating vanity",
    "floating shelf",
    "bath towel",
    "hand towel hook",
    "towel hook",
    "cabinet panels",
    "cabinetry",
    "kickboard",
    "handles",
    "selection required",
    "frame colour",
    "frameless",
    "ref. number",
    "document ref",
    "staircase",
    "wet area",
)


def build_spec_snapshot(
    job: dict[str, Any],
    builder: dict[str, Any],
    files: list[dict[str, Any]],
    template_files: list[dict[str, Any]],
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    rule_flags = cleaning_rules.global_rule_flags()
    parser_strategy = cleaning_rules.global_parser_strategy()
    raw_documents = _load_documents(files, role="spec")
    documents = [
        {
            **document,
            "pages": [dict(page) for page in list(document.get("pages", []))],
        }
        for document in raw_documents
    ]
    documents, vision_meta = _apply_layout_pipeline(
        job,
        builder,
        documents,
        source_kind="spec",
        progress_callback=progress_callback,
    )
    _report_progress(progress_callback, "heuristic", f"Running heuristic extraction on {len(documents)} spec file(s)")
    heuristic = parsing.parse_documents(job_no=job["job_no"], builder_name=builder["name"], source_kind="spec", documents=documents, rule_flags=rule_flags)
    heuristic_analysis = dict(heuristic.get("analysis") or {})
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
            "layout_attempted": vision_meta["layout_attempted"],
            "layout_succeeded": vision_meta["layout_succeeded"],
            "layout_mode": vision_meta["layout_mode"],
            "layout_provider": vision_meta["layout_provider"],
            "layout_pages": vision_meta["layout_pages"],
            "heavy_vision_pages": vision_meta["heavy_vision_pages"],
            "layout_note": vision_meta["layout_note"],
            "docling_attempted": vision_meta["docling_attempted"],
            "docling_succeeded": vision_meta["docling_succeeded"],
            "docling_pages": vision_meta["docling_pages"],
            "docling_note": vision_meta["docling_note"],
            "room_master_file": heuristic_analysis.get("room_master_file", ""),
            "room_master_reason": heuristic_analysis.get("room_master_reason", ""),
            "supplement_files": heuristic_analysis.get("supplement_files", []),
            "ignored_room_like_lines_count": heuristic_analysis.get("ignored_room_like_lines_count", 0),
            "vision_attempted": vision_meta["vision_attempted"],
            "vision_succeeded": vision_meta["vision_succeeded"],
            "vision_pages": vision_meta["vision_pages"],
            "vision_page_count": vision_meta["vision_page_count"],
            "vision_note": vision_meta["vision_note"],
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
        if str(builder.get("name", "") or "").strip().lower() == "imperial":
            raw_crosscheck = _build_raw_spec_crosscheck_snapshot(
                job_no=str(job.get("job_no", "") or ""),
                builder_name=str(builder.get("name", "") or ""),
                documents=raw_documents,
                parser_strategy=parser_strategy,
                rule_flags=rule_flags,
            )
            merged = _crosscheck_imperial_snapshot_with_raw(merged, raw_crosscheck)
        elif str(builder.get("name", "") or "").strip().lower() == "clarendon":
            raw_crosscheck = _build_raw_spec_crosscheck_snapshot(
                job_no=str(job.get("job_no", "") or ""),
                builder_name=str(builder.get("name", "") or ""),
                documents=raw_documents,
                parser_strategy=parser_strategy,
                rule_flags=rule_flags,
            )
            merged = _crosscheck_clarendon_snapshot_with_raw(merged, raw_crosscheck)
        merged = parsing.enrich_snapshot_rooms(merged, documents, rule_flags=rule_flags)
        merged = parsing.apply_snapshot_cleaning_rules(merged, rule_flags=rule_flags)
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
    if str(builder.get("name", "") or "").strip().lower() == "imperial":
        raw_crosscheck = _build_raw_spec_crosscheck_snapshot(
            job_no=str(job.get("job_no", "") or ""),
            builder_name=str(builder.get("name", "") or ""),
            documents=raw_documents,
            parser_strategy=parser_strategy,
            rule_flags=rule_flags,
        )
        heuristic = _crosscheck_imperial_snapshot_with_raw(heuristic, raw_crosscheck)
    elif str(builder.get("name", "") or "").strip().lower() == "clarendon":
        raw_crosscheck = _build_raw_spec_crosscheck_snapshot(
            job_no=str(job.get("job_no", "") or ""),
            builder_name=str(builder.get("name", "") or ""),
            documents=raw_documents,
            parser_strategy=parser_strategy,
            rule_flags=rule_flags,
        )
        heuristic = _crosscheck_clarendon_snapshot_with_raw(heuristic, raw_crosscheck)
    heuristic = parsing.enrich_snapshot_rooms(heuristic, documents, rule_flags=rule_flags)
    heuristic = parsing.apply_snapshot_cleaning_rules(heuristic, rule_flags=rule_flags)
    if str(builder.get("name", "") or "").strip().lower() == "imperial":
        heuristic = _apply_imperial_row_polish(
            heuristic,
            documents,
            builder_name=str(builder.get("name", "")),
            parser_strategy=parser_strategy,
            rule_flags=rule_flags,
            progress_callback=progress_callback,
        )
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
    documents, heuristic, vision_meta = _apply_vision_fallback(
        job,
        builder,
        documents,
        heuristic,
        source_kind="drawing",
        rule_flags=rule_flags,
        progress_callback=progress_callback,
    )
    heuristic_analysis = dict(heuristic.get("analysis") or {})
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
            "layout_attempted": vision_meta["layout_attempted"],
            "layout_succeeded": vision_meta["layout_succeeded"],
            "layout_mode": vision_meta["layout_mode"],
            "layout_provider": vision_meta["layout_provider"],
            "layout_pages": vision_meta["layout_pages"],
            "heavy_vision_pages": vision_meta["heavy_vision_pages"],
            "layout_note": vision_meta["layout_note"],
            "docling_attempted": vision_meta["docling_attempted"],
            "docling_succeeded": vision_meta["docling_succeeded"],
            "docling_pages": vision_meta["docling_pages"],
            "docling_note": vision_meta["docling_note"],
            "room_master_file": heuristic_analysis.get("room_master_file", ""),
            "room_master_reason": heuristic_analysis.get("room_master_reason", ""),
            "supplement_files": heuristic_analysis.get("supplement_files", []),
            "ignored_room_like_lines_count": heuristic_analysis.get("ignored_room_like_lines_count", 0),
            "vision_attempted": vision_meta["vision_attempted"],
            "vision_succeeded": vision_meta["vision_succeeded"],
            "vision_pages": vision_meta["vision_pages"],
            "vision_page_count": vision_meta["vision_page_count"],
            "vision_note": vision_meta["vision_note"],
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
        pages = []
        for page in parsing.load_document_pages(path):
            page_payload = dict(page)
            page_payload["raw_text"] = str(page_payload.get("raw_text", page_payload.get("text", "")) or "")
            page_payload["text"] = str(page_payload.get("text", page_payload["raw_text"]) or "")
            pages.append(page_payload)
        documents.append(
            {
                "file_name": file_row["original_name"],
                "path": str(path),
                "role": role,
                "pages": pages,
            }
        )
    return documents


def _blank_vision_meta() -> dict[str, Any]:
    return {
        "layout_attempted": False,
        "layout_succeeded": False,
        "layout_mode": "",
        "layout_provider": "heuristic",
        "layout_pages": [],
        "heavy_vision_pages": [],
        "layout_note": "",
        "docling_attempted": False,
        "docling_succeeded": False,
        "docling_pages": [],
        "docling_note": "",
        "vision_attempted": False,
        "vision_succeeded": False,
        "vision_pages": [],
        "vision_page_count": 0,
        "vision_note": "",
    }


def _unique_page_numbers(values: list[int]) -> list[int]:
    ordered: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            page_no = int(value)
        except (TypeError, ValueError):
            continue
        if page_no <= 0 or page_no in seen:
            continue
        seen.add(page_no)
        ordered.append(page_no)
    return ordered


def _apply_layout_pipeline(
    job: dict[str, Any],
    builder: dict[str, Any],
    documents: list[dict[str, object]],
    source_kind: str,
    progress_callback: ProgressCallback = None,
) -> tuple[list[dict[str, object]], dict[str, Any]]:
    layout_meta = _blank_vision_meta()
    if not documents:
        layout_meta["layout_note"] = "No source documents available for layout analysis."
        return documents, layout_meta

    builder_name = str(builder.get("name", "") or "")
    job_no = str(job.get("job_no", "") or "")
    updated_documents: list[dict[str, object]] = []
    layout_pages: list[int] = []
    layout_candidates: list[tuple[int, int]] = []
    heavy_candidates: list[tuple[int, int]] = []

    _report_progress(progress_callback, "layout_prepare", f"Building page layouts for {len(documents)} {source_kind} file(s)")
    for doc_index, document in enumerate(documents):
        updated_document = {
            **document,
            "builder_name": builder_name,
            "pages": [dict(page) for page in list(document.get("pages", []))],
        }
        pages = list(updated_document.get("pages", []))
        for page_index, page in enumerate(pages):
            page_no = int(page.get("page_no", 0) or 0)
            raw_text = str(page.get("raw_text", page.get("text", "")) or "")
            heuristic_layout = _build_heuristic_page_layout(
                builder_name=builder_name,
                source_kind=source_kind,
                file_name=str(updated_document.get("file_name", "") or ""),
                page=page,
            )
            page["page_layout"] = heuristic_layout
            page["layout_mode"] = "lightweight"
            page["text"] = _vision_layout_to_text(heuristic_layout, fallback_text=raw_text)
            layout_pages.append(page_no)
            if source_kind == "spec" and _page_requires_vision(
                builder_name=builder_name,
                source_kind=source_kind,
                file_name=str(updated_document.get("file_name", "") or ""),
                page=page,
                heuristic={},
            ):
                layout_candidates.append((doc_index, page_index))
                if _page_requires_heavy_vision(
                    builder_name=builder_name,
                    source_kind=source_kind,
                    file_name=str(updated_document.get("file_name", "") or ""),
                    page=page,
                ):
                    heavy_candidates.append((doc_index, page_index))
        updated_documents.append(updated_document)

    layout_meta["layout_attempted"] = True
    layout_meta["layout_pages"] = _unique_page_numbers(layout_pages)
    layout_meta["layout_succeeded"] = bool(layout_pages)
    layout_meta["layout_mode"] = "lightweight"
    layout_meta["layout_provider"] = "heuristic"
    layout_meta["layout_note"] = f"Lightweight structure analysis applied to {len(layout_meta['layout_pages'])} page(s)."

    docling_enabled = _spec_docling_enabled(builder_name, source_kind)
    heavy_vision_enabled = _spec_heavy_vision_enabled(job_no, source_kind, builder_name=builder_name)

    if not layout_candidates:
        if source_kind == "spec" and not docling_enabled:
            layout_meta["docling_note"] = f"Docling is disabled by builder policy for {builder_name or 'this builder'}."
        else:
            layout_meta["docling_note"] = "No pages matched the Docling layout rules."
        if source_kind == "spec" and not heavy_vision_enabled:
            layout_meta["vision_note"] = (
                "Heavy vision is disabled by default for spec runs. "
                "Use SPEC_EXTRACTION_FORCE_SPEC_HEAVY_VISION_JOBS to override a specific job."
            )
        else:
            layout_meta["vision_note"] = "No pages matched the heavy vision layout rules."
        return updated_documents, layout_meta

    max_pages = max(1, runtime.OPENAI_VISION_MAX_PAGES)
    docling_candidate_pages = layout_candidates[:max_pages]
    heavy_candidate_pages = heavy_candidates[:max_pages]
    docling_attempted_pages: list[int] = []
    docling_applied_pages: list[int] = []
    docling_notes: list[str] = []
    docling_layouts: dict[tuple[int, int], dict[str, Any]] = {}
    if source_kind == "spec" and docling_enabled and _docling_available():
        layout_meta["docling_attempted"] = True
        for doc_index, page_index in docling_candidate_pages:
            document = updated_documents[doc_index]
            pages = list(document.get("pages", []))
            if page_index >= len(pages):
                continue
            page = pages[page_index]
            page_no = int(page.get("page_no", 0) or 0)
            docling_attempted_pages.append(page_no)
            try:
                _report_progress(progress_callback, "layout_prepare", f"Calling Docling for {document['file_name']} page {page_no}")
                layout = _request_docling_page_layout(
                    builder_name=builder_name,
                    source_kind=source_kind,
                    file_name=str(document.get("file_name", "") or ""),
                    page=page,
                    document_path=Path(str(document.get("path", "") or "")),
                )
                if not _layout_is_usable(layout, raw_page_text=str(page.get("raw_text", page.get("text", "")) or "")):
                    docling_notes.append(f"page {page_no}: Docling returned no usable room/row structure")
                    continue
                docling_layouts[(doc_index, page_index)] = layout
                docling_applied_pages.append(page_no)
            except Exception as exc:
                docling_notes.append(f"page {page_no}: {_truncate_note(exc)}")
    elif source_kind == "spec" and not docling_enabled:
        layout_meta["docling_note"] = f"Docling is disabled by builder policy for {builder_name or 'this builder'}."
    elif source_kind == "spec":
        layout_meta["docling_note"] = "Docling is not installed or unavailable in the runtime environment."
    else:
        layout_meta["docling_note"] = "Docling is only enabled for spec parsing."

    layout_meta["docling_pages"] = _unique_page_numbers(docling_applied_pages if docling_applied_pages else docling_attempted_pages)
    layout_meta["docling_succeeded"] = bool(docling_applied_pages)
    if layout_meta["docling_attempted"] and not layout_meta["docling_note"]:
        if docling_applied_pages:
            note = f"Docling layout applied to {len(docling_applied_pages)} page(s): {', '.join(str(page_no) for page_no in _unique_page_numbers(docling_applied_pages))}."
            if len(layout_candidates) > max_pages:
                note = f"{note} Skipped {len(layout_candidates) - max_pages} candidate page(s) because of the configured page cap."
            if docling_notes:
                note = f"{note} Partial issues: {'; '.join(docling_notes)[:220]}"
            layout_meta["docling_note"] = note[:400]
        else:
            layout_meta["docling_note"] = (
                "; ".join(docling_notes)[:400] if docling_notes else "Docling layout attempted but no usable page layout was applied."
            )

    if not heavy_vision_enabled:
        if docling_applied_pages:
            _apply_docling_layout_meta(layout_meta, docling_applied_pages)
        if source_kind == "spec":
            layout_meta["vision_note"] = (
                "Heavy vision is disabled by default for spec runs. "
                "Use SPEC_EXTRACTION_FORCE_SPEC_HEAVY_VISION_JOBS to override a specific job."
            )
        else:
            layout_meta["vision_note"] = "OpenAI vision structure cross-check is disabled in runtime settings."
        _apply_final_layout_pages(updated_documents, docling_candidate_pages, docling_layouts=docling_layouts, vision_layouts={}, builder_name=builder_name)
        return updated_documents, layout_meta
    if not heavy_candidate_pages:
        if docling_applied_pages:
            _apply_docling_layout_meta(layout_meta, docling_applied_pages)
        layout_meta["vision_note"] = "No pages matched the heavy vision layout rules."
        _apply_final_layout_pages(updated_documents, docling_candidate_pages, docling_layouts=docling_layouts, vision_layouts={}, builder_name=builder_name)
        return updated_documents, layout_meta
    if not runtime.OPENAI_ENABLED:
        if docling_applied_pages:
            _apply_docling_layout_meta(layout_meta, docling_applied_pages)
        layout_meta["vision_note"] = "OpenAI is disabled, so heavy vision structure cross-check was skipped."
        _apply_final_layout_pages(updated_documents, docling_candidate_pages, docling_layouts=docling_layouts, vision_layouts={}, builder_name=builder_name)
        return updated_documents, layout_meta
    if not runtime.OPENAI_API_KEY:
        if docling_applied_pages:
            _apply_docling_layout_meta(layout_meta, docling_applied_pages)
        layout_meta["vision_note"] = "OPENAI_API_KEY is not configured, so heavy vision structure cross-check was skipped."
        _apply_final_layout_pages(updated_documents, docling_candidate_pages, docling_layouts=docling_layouts, vision_layouts={}, builder_name=builder_name)
        return updated_documents, layout_meta

    attempted_pages: list[int] = []
    applied_pages: list[int] = []
    page_notes: list[str] = []
    vision_layouts: dict[tuple[int, int], dict[str, Any]] = {}
    layout_meta["vision_attempted"] = True

    for doc_index, page_index in heavy_candidate_pages:
        document = updated_documents[doc_index]
        pages = list(document.get("pages", []))
        if page_index >= len(pages):
            continue
        page = pages[page_index]
        page_no = int(page.get("page_no", 0) or 0)
        attempted_pages.append(page_no)
        try:
            _report_progress(progress_callback, "vision_prepare", f"Preparing page {page_no} from {document['file_name']} for heavy vision layout")
            image_bytes = _render_pdf_page_png(
                Path(str(document.get("path", ""))),
                page_no=page_no,
                dpi=runtime.OPENAI_VISION_DPI,
            )
            _report_progress(progress_callback, "vision_request", f"Calling OpenAI vision for {document['file_name']} page {page_no}")
            layout = _request_page_layout(
                job_no=str(job.get("job_no", "")),
                builder_name=builder_name,
                source_kind=source_kind,
                file_name=str(document.get("file_name", "") or ""),
                page_no=page_no,
                page_text=str(page.get("raw_text", page.get("text", "")) or ""),
                image_bytes=image_bytes,
                document_path=Path(str(document.get("path", ""))),
                table_rows=page.get("table_rows", []),
            )
            if not _layout_is_usable(layout, raw_page_text=str(page.get("raw_text", page.get("text", "")) or "")):
                page_notes.append(f"page {page_no}: OpenAI vision returned no usable room/row structure")
                continue
            vision_layouts[(doc_index, page_index)] = layout
            applied_pages.append(page_no)
        except Exception as exc:
            page_notes.append(f"page {page_no}: {_truncate_note(exc)}")
            if _is_openai_insufficient_quota_error(exc):
                page_notes.append("OpenAI vision quota is exhausted; skipped remaining candidate pages.")
                break

    mixed_pages, final_docling_pages, final_vision_pages = _apply_final_layout_pages(
        updated_documents,
        _ordered_page_candidates(docling_candidate_pages, heavy_candidate_pages),
        docling_layouts=docling_layouts,
        vision_layouts=vision_layouts,
        builder_name=builder_name,
    )

    layout_meta["vision_pages"] = _unique_page_numbers(applied_pages if applied_pages else attempted_pages)
    layout_meta["vision_page_count"] = len(layout_meta["vision_pages"])
    layout_meta["heavy_vision_pages"] = _unique_page_numbers(final_vision_pages if final_vision_pages else applied_pages)
    layout_meta["vision_succeeded"] = bool(applied_pages)
    if mixed_pages:
        layout_meta["layout_mode"] = "mixed"
        layout_meta["layout_provider"] = "mixed"
        layout_meta["layout_note"] = (
            f"Structure-first parsing applied to {len(layout_meta['layout_pages'])} page(s); "
            f"Docling corrected {len(_unique_page_numbers(docling_applied_pages))} page(s); "
            f"heavy vision checked {len(_unique_page_numbers(applied_pages))} page(s); "
            f"merged structure applied to {len(_unique_page_numbers(mixed_pages))} page(s)."
        )
    elif final_vision_pages:
        layout_meta["layout_mode"] = "heavy_vision"
        layout_meta["layout_provider"] = "heavy_vision"
        layout_meta["layout_note"] = (
            f"Structure-first parsing applied to {len(layout_meta['layout_pages'])} page(s); "
            f"heavy vision corrected {len(_unique_page_numbers(final_vision_pages))} page(s)."
        )
    elif final_docling_pages:
        _apply_docling_layout_meta(layout_meta, final_docling_pages)
    if applied_pages:
        success_note = f"Heavy vision structure checked {len(layout_meta['vision_pages'])} page(s): {', '.join(str(page_no) for page_no in layout_meta['vision_pages'])}."
        if mixed_pages:
            success_note = f"{success_note} Mixed Docling/OpenAI structure applied to {len(_unique_page_numbers(mixed_pages))} page(s): {', '.join(str(page_no) for page_no in _unique_page_numbers(mixed_pages))}."
        if len(heavy_candidates) > max_pages:
            success_note = f"{success_note} Skipped {len(heavy_candidates) - max_pages} candidate page(s) because of the configured page cap."
        if page_notes:
            success_note = f"{success_note} Partial issues: {'; '.join(page_notes)[:220]}"
        layout_meta["vision_note"] = success_note[:400]
    else:
        note = "Heavy vision structure cross-check attempted but no usable page layout was applied."
        if page_notes:
            note = "; ".join(page_notes)[:400]
        layout_meta["vision_note"] = note
        if final_docling_pages:
            _apply_docling_layout_meta(layout_meta, final_docling_pages)
    return updated_documents, layout_meta


def _docling_available() -> bool:
    return _get_docling_converter_class() is not None


def _get_docling_converter() -> Any:
    global _DOCLING_CONVERTER
    converter_class = _get_docling_converter_class()
    if converter_class is None:
        raise RuntimeError("Docling is not installed in the runtime environment.")
    if _DOCLING_CONVERTER is None:
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import PdfFormatOption

        pdf_options = PdfPipelineOptions()
        # We only need structural recovery from text-based builder PDFs here.
        # Keeping OCR off avoids making the runtime depend on Tesseract/EasyOCR.
        pdf_options.do_ocr = False
        _DOCLING_CONVERTER = converter_class(
            allowed_formats=[InputFormat.PDF],
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options),
            },
        )
    return _DOCLING_CONVERTER


def _get_docling_converter_class() -> Any:
    global _DOCLING_CONVERTER_CLASS, _DOCLING_IMPORT_ERROR
    if _DOCLING_CONVERTER_CLASS is not None:
        return _DOCLING_CONVERTER_CLASS
    if _DOCLING_IMPORT_ERROR is not None:
        return None
    try:  # pragma: no cover - optional runtime dependency
        from docling.document_converter import DocumentConverter as converter_class
    except ImportError as exc:  # pragma: no cover - graceful fallback when docling is unavailable
        _DOCLING_IMPORT_ERROR = exc
        return None
    _DOCLING_CONVERTER_CLASS = converter_class
    return _DOCLING_CONVERTER_CLASS


def _write_single_page_pdf(source_path: Path, page_no: int, destination_path: Path) -> None:
    reader = PdfReader(str(source_path))
    if page_no < 1 or page_no > len(reader.pages):
        raise RuntimeError(f"Requested page {page_no} is outside the PDF page range.")
    writer = PdfWriter()
    writer.add_page(reader.pages[page_no - 1])
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("wb") as handle:
        writer.write(handle)


def _request_docling_page_layout(
    builder_name: str,
    source_kind: str,
    file_name: str,
    page: dict[str, Any],
    document_path: Path,
) -> dict[str, Any]:
    if not _docling_available():
        raise RuntimeError("Docling is not installed in the runtime environment.")
    page_no = int(page.get("page_no", 0) or 0)
    if not document_path.exists():
        raise RuntimeError(f"Source PDF was not found for Docling layout: {document_path}")
    with tempfile.TemporaryDirectory(prefix="spec-docling-") as temp_dir:
        subset_path = Path(temp_dir) / f"page-{page_no}.pdf"
        _write_single_page_pdf(document_path, page_no=page_no, destination_path=subset_path)
        result = _get_docling_converter().convert(str(subset_path))
        markdown = result.document.export_to_markdown()
    raw_page_text = str(page.get("raw_text", page.get("text", "")) or "")
    return _postprocess_builder_layout(
        builder_name=builder_name,
        source_kind=source_kind,
        layout=_docling_markdown_to_layout(
            markdown,
            builder_name=builder_name,
            source_kind=source_kind,
            file_name=file_name,
            raw_page_text=raw_page_text,
        ),
        raw_page_text=raw_page_text,
        provider="docling",
        table_rows=page.get("table_rows", []),
        document_path=document_path,
        page_no=page_no,
    )


def _docling_markdown_to_layout(
    markdown: str,
    builder_name: str,
    source_kind: str,
    file_name: str,
    raw_page_text: str,
) -> dict[str, Any]:
    source = html.unescape(str(markdown or "")).replace("\r", "\n")
    heading_lines = _docling_heading_lines(source)
    heuristic_text = "\n".join(heading_lines) if heading_lines else source
    page_type = _infer_page_type_from_text(builder_name, source_kind, f"{raw_page_text}\n{heuristic_text}")
    section_label, room_label = _infer_layout_labels(builder_name, heading_lines or _docling_plain_lines(source), page_type)
    if not section_label:
        section_label = _docling_section_label_from_headings(heading_lines)
    if not room_label and section_label:
        _, room_label = _infer_layout_labels(builder_name, [section_label], page_type)

    if page_type == "sinkware_tapware":
        room_blocks = _docling_sink_tap_room_blocks(source, builder_name=builder_name)
        rows = [row for block in room_blocks for row in block.get("rows", [])]
    else:
        rows = []
        for table in _docling_markdown_tables(source):
            rows.extend(_docling_table_to_rows(table, page_type=page_type))
        if not rows:
            lines = _docling_plain_lines(source)
            rows = _split_lines_to_layout_rows(lines, page_type=page_type)
        room_blocks = [{"room_label": room_label, "rows": rows}] if (room_label or rows) else []

    return _normalize_page_layout(
        {
            "page_type": page_type,
            "section_label": section_label,
            "room_label": room_label,
            "room_blocks": room_blocks,
            "rows": rows,
            "file_name": file_name,
        }
    )


_IMPERIAL_JOINERY_FIELD_ROOM_MAP: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)\bBENCHTOP(?:S)?\b"), "BENCHTOP"),
    (re.compile(r"(?i)\bUPPER CABINETRY\b|\bOVERHEADS?\b"), "UPPER CABINETRY COLOUR"),
    (re.compile(r"(?i)\bBASE CABINETRY COLOUR\b.*\bTALL\b|\bTALL PANTRY\b"), "BASE CABINETRY COLOUR + TALL PANTRY"),
    (re.compile(r"(?i)\bBASE CABINETRY COLOUR\b"), "BASE CABINETRY COLOUR"),
    (re.compile(r"(?i)\bFEATURE\b.*\bBAR BACK\b|\bBAR BACK\b"), "FEATURE COLOUR BAR BACK + BAR BACK DOOR"),
    (re.compile(r"(?i)\bFLOATING SHELV"), "FLOATING SHELVES"),
    (re.compile(r"(?i)\bOPEN SHELV"), "OPEN SHELVES"),
    (re.compile(r"(?i)\bKICKBOARDS?\b"), "KICKBOARDS"),
    (re.compile(r"(?i)\bBULKHEAD\b"), "BULKHEAD"),
    (re.compile(r"(?i)\bSPLASHBACK\b"), "SPLASHBACK"),
)

_IMPERIAL_LAYOUT_NOISE_PATTERNS: tuple[str, ...] = (
    r"(?i)\bCLIENT NAME\b",
    r"(?i)\bSIGNATURE\b",
    r"(?i)\bSIGNED DATE\b",
    r"(?i)\bNOTES SUPPLIER\b",
    r"(?i)\bDOCUMENT REF\b",
    r"(?i)\bPAGE \d+\s+OF\s+\d+\b",
    r"(?i)\bDESIGNER\b",
)


@dataclass
class ImperialGridRow:
    room_scope: str
    row_label: str
    description: str
    image: str = ""
    supplier: str = ""
    notes: str = ""
    row_kind: str = "material"
    page_no: int = 0
    row_order: int = 0
    repair_source: str = ""
    position_scope: str = ""
    confidence: float = 0.0
    provenance: dict[str, Any] | None = None
    needs_review: bool = False

    def to_layout_row(self) -> dict[str, Any]:
        return {
            "room_scope": parsing.normalize_space(self.room_scope),
            "row_label": parsing.normalize_space(self.row_label),
            "value_region_text": parsing.normalize_space(self.description),
            "image_region_text": parsing.normalize_space(self.image),
            "supplier_region_text": parsing.normalize_space(self.supplier),
            "notes_region_text": parsing.normalize_space(self.notes),
            "row_kind": str(self.row_kind or "other"),
            "page_no": int(self.page_no or 0),
            "row_order": int(self.row_order or 0),
            "repair_source": parsing.normalize_space(self.repair_source),
            "position_scope": parsing.normalize_space(self.position_scope),
            "confidence": _normalize_confidence_value(self.confidence),
            "provenance": _coerce_provenance_dict(self.provenance),
            "needs_review": bool(self.needs_review),
        }


@dataclass
class ImperialColumnModel:
    label_right: float = 240.0
    description_right: float = 780.0
    image_right: float = 780.0
    supplier_right: float = 940.0
    has_image_column: bool = False

    def role_for_x(self, x0: float) -> str:
        if x0 < self.label_right:
            return "label"
        if x0 < self.description_right:
            return "description"
        if self.has_image_column and x0 < self.image_right:
            return "image"
        if x0 < self.supplier_right:
            return "supplier"
        return "notes"


@dataclass
class ImperialSeparatorModel:
    visible_vertical_edges: list[float] = field(default_factory=list)
    visible_horizontal_edges: list[float] = field(default_factory=list)
    inferred_vertical_edges: list[float] = field(default_factory=list)
    inferred_horizontal_edges: list[float] = field(default_factory=list)
    visible_vertical_segments: list[dict[str, float]] = field(default_factory=list)
    visible_horizontal_segments: list[dict[str, float]] = field(default_factory=list)
    inferred_vertical_segments: list[dict[str, float]] = field(default_factory=list)
    inferred_horizontal_segments: list[dict[str, float]] = field(default_factory=list)
    image_bboxes: list[dict[str, float]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "visible_vertical_edges": [float(value) for value in self.visible_vertical_edges],
            "visible_horizontal_edges": [float(value) for value in self.visible_horizontal_edges],
            "inferred_vertical_edges": [float(value) for value in self.inferred_vertical_edges],
            "inferred_horizontal_edges": [float(value) for value in self.inferred_horizontal_edges],
            "visible_vertical_segments": [
                {
                    "orientation": str(item.get("orientation", "vertical") or "vertical"),
                    "edge": float(item.get("edge", 0.0) or 0.0),
                    "start": float(item.get("start", 0.0) or 0.0),
                    "end": float(item.get("end", 0.0) or 0.0),
                    "source": str(item.get("source", "visible_line") or "visible_line"),
                    "confidence": str(item.get("confidence", "visible") or "visible"),
                }
                for item in self.visible_vertical_segments
                if isinstance(item, dict)
            ],
            "visible_horizontal_segments": [
                {
                    "orientation": str(item.get("orientation", "horizontal") or "horizontal"),
                    "edge": float(item.get("edge", 0.0) or 0.0),
                    "start": float(item.get("start", 0.0) or 0.0),
                    "end": float(item.get("end", 0.0) or 0.0),
                    "source": str(item.get("source", "visible_line") or "visible_line"),
                    "confidence": str(item.get("confidence", "visible") or "visible"),
                }
                for item in self.visible_horizontal_segments
                if isinstance(item, dict)
            ],
            "inferred_vertical_segments": [
                {
                    "orientation": str(item.get("orientation", "vertical") or "vertical"),
                    "edge": float(item.get("edge", 0.0) or 0.0),
                    "start": float(item.get("start", 0.0) or 0.0),
                    "end": float(item.get("end", 0.0) or 0.0),
                    "source": str(item.get("source", "inferred") or "inferred"),
                    "confidence": str(item.get("confidence", "inferred_high") or "inferred_high"),
                }
                for item in self.inferred_vertical_segments
                if isinstance(item, dict)
            ],
            "inferred_horizontal_segments": [
                {
                    "orientation": str(item.get("orientation", "horizontal") or "horizontal"),
                    "edge": float(item.get("edge", 0.0) or 0.0),
                    "start": float(item.get("start", 0.0) or 0.0),
                    "end": float(item.get("end", 0.0) or 0.0),
                    "source": str(item.get("source", "inferred") or "inferred"),
                    "confidence": str(item.get("confidence", "inferred_high") or "inferred_high"),
                }
                for item in self.inferred_horizontal_segments
                if isinstance(item, dict)
            ],
            "image_bboxes": [
                {
                    "x0": float(item.get("x0", 0.0) or 0.0),
                    "x1": float(item.get("x1", 0.0) or 0.0),
                    "top": float(item.get("top", 0.0) or 0.0),
                    "bottom": float(item.get("bottom", 0.0) or 0.0),
                }
                for item in self.image_bboxes
                if isinstance(item, dict)
            ],
        }


@dataclass
class ImperialCell:
    text: str
    col_role: str
    row_band: int
    x0: float = 0.0
    x1: float = 0.0
    top: float = 0.0
    bottom: float = 0.0
    source: str = ""


@dataclass
class ImperialLogicalRow:
    room_scope: str
    row_label: str
    description_parts: list[str]
    raw_area_or_item: str = ""
    image_parts: list[str] = field(default_factory=list)
    supplier_parts: list[str] = field(default_factory=list)
    note_parts: list[str] = field(default_factory=list)
    row_kind: str = "other"
    page_no: int = 0
    row_order: int = 0
    repair_source: str = ""
    position_scope: str = ""
    confidence: float = 0.0
    provenance: dict[str, Any] | None = None
    needs_review: bool = False

    def to_grid_row(self) -> ImperialGridRow:
        description = parsing.normalize_brand_casing_text(
            parsing.normalize_space(" ".join(_dedupe_preserve_order(self.description_parts)))
        ).strip(" -;,")
        image = parsing.normalize_brand_casing_text(
            parsing.normalize_space(" ".join(_dedupe_preserve_order(self.image_parts)))
        ).strip(" -;,")
        notes = parsing.normalize_brand_casing_text(
            parsing.normalize_space(" ".join(_dedupe_preserve_order(self.note_parts)))
        ).strip(" -;,")
        supplier = next(
            (
                parsing.normalize_brand_casing_text(parsing.normalize_space(value))
                for value in self.supplier_parts
                if parsing.normalize_space(value)
            ),
            "",
        ).strip(" -;,")
        if supplier and description:
            supplier_pattern = re.escape(supplier)
            description = re.sub(rf"(?i)\b{supplier_pattern}\b", " ", description)
            description = parsing.normalize_space(description).strip(" -;,")
            description = re.sub(r"(?i)\b([A-Z0-9./-]+)(?:\s+\1\b)+", r"\1", description).strip(" -;,")
        return ImperialGridRow(
            room_scope=self.room_scope,
            row_label=self.row_label,
            description=description,
            image=image,
            supplier=supplier,
            notes=notes,
            row_kind=self.row_kind,
            page_no=self.page_no,
            row_order=self.row_order,
            repair_source=self.repair_source,
            position_scope=self.position_scope or _imperial_grid_row_position_scope(self.row_label, self.row_kind),
            confidence=_normalize_confidence_value(self.confidence),
            provenance=_coerce_provenance_dict(
                {
                    **(_coerce_provenance_dict(self.provenance)),
                    "raw_area_or_item": parsing.normalize_space(self.raw_area_or_item or self.row_label),
                }
            ),
            needs_review=bool(self.needs_review),
        )


@dataclass
class ImperialFiveColumnRow:
    room_scope: str
    row_order: int
    area_or_item: str
    specs_or_description: str
    image: str
    supplier: str
    notes: str
    canonical_label: str = ""
    remainder: str = ""
    row_kind: str = "other"
    match_source: str = ""
    confidence: float = 0.0
    provenance: dict[str, Any] | None = None
    needs_review: bool = False


@dataclass
class ImperialGridPage:
    room_scope: str
    rows: list[ImperialGridRow]
    provider: str = ""
    unresolved_rows: list[dict[str, Any]] = field(default_factory=list)

    def to_layout(self) -> dict[str, Any]:
        room_scope = parsing.normalize_space(self.room_scope)
        return _normalize_page_layout(
            {
                "page_type": "joinery",
                "section_label": room_scope,
                "room_label": room_scope,
                "room_blocks": [{"room_label": room_scope, "rows": [row.to_layout_row() for row in self.rows]}],
                "rows": [],
                "provider": self.provider,
                "unresolved_rows": list(self.unresolved_rows),
            }
        )


_IMPERIAL_GRID_SUPPLIER_HINTS: tuple[str, ...] = (
    "Caesarstone",
    "Polytec",
    "Laminex",
    "WK Stone",
    "Smartstone",
    "Dekton",
    "Porcelain",
    "Kethy",
    "Allegra",
    "Momo",
    "Titus Tekform",
    "Furnware",
    "Hettich",
)

_IMPERIAL_TABLE_REPAIR_ROW_SPECS: tuple[tuple[str, str, str], ...] = (
    (r"(?i)^ISLAND\s+BENCHTOP\b", "ISLAND BENCHTOP", "material"),
    (r"(?i)^COOKTOP\s+RUN\s+BENCHTOP\b", "COOKTOP RUN BENCHTOP", "material"),
    (r"(?i)^WALL\s+RUN\s+BENCHTOP\b", "WALL RUN BENCHTOP", "material"),
    (r"(?i)^DESK\s+BENCHTOP\s+AND\s+DESK\s+BASE\s+CABINETRY\s+COLOUR\b", "DESK BENCHTOP AND DESK BASE CABINETRY COLOUR", "material"),
    (r"(?i)^FEATURE\s+COLOUR\s+BAR\s+BACK\s*\+\s*BAR\s+BACK\s+DOOR\b", "FEATURE COLOUR BAR BACK + BAR BACK DOOR", "material"),
    (r"(?i)^FEATURE\s+BAR\s+BACK\b", "FEATURE BAR BACK", "material"),
    (r"(?i)^BAR\s+BACK\b", "BAR BACK", "material"),
    (r"(?i)^UPPER\s+CABINETRY\s+COLOUR\s*\+\s*FRIDGE\s+PANELS?\s+AND\s+FRIDGE\s+OVERHEAD\b", "UPPER CABINETRY COLOUR + FRIDGE PANELS AND FRIDGE OVERHEAD", "material"),
    (r"(?i)^UPPER\s+CABINETRY\s+COLOUR\s*\+\s*TALL\s+CABINETS?\b", "UPPER CABINETRY COLOUR + TALL CABINETS", "material"),
    (r"(?i)^BASE\s*\+\s*OVERHEAD(?:S)?\s*\+\s*OPEN\s+OVERHEADS?\s*\+\s*TALLS?\b", "BASE + OVERHEAD + OPEN OVERHEADS + TALLS", "material"),
    (r"(?i)^BASE\s*,\s*UPPER\s*&\s*TALL\s+CABINETRY\s+COLOUR(?:ED)?(?:\s+BOTTOMS\s+TO\s+OVERHEADS)?\b", "BASE + OVERHEAD + OPEN OVERHEADS + TALLS", "material"),
    (r"(?i)^BASE\s*&\s*TALL\s+CABINETRY\s+COLOUR\b", "BASE CABINETRY COLOUR + TALL CABINETS", "material"),
    (r"(?i)^BASE\s+CABINETRY\s+COLOUR\s*\+\s*TALL\s+PANTRY\b", "BASE CABINETRY COLOUR + TALL PANTRY", "material"),
    (r"(?i)^EXTERNAL\s+BASE\s+CABINETRY\s+COLOUR(?:\s+AND\s+SLIDING\s+DOORS?)?\b", "EXTERNAL BASE CABINETRY COLOUR AND SLIDING DOORS", "material"),
    (r"(?i)^INTERNAL\s+CABINETRY\s+COLOUR(?:\s*\(.*\))?\b", "INTERNAL CABINETRY COLOUR", "material"),
    (r"(?i)^BENCH\s+CABINETRY\s+COLOUR\b", "BENCH CABINETRY COLOUR", "material"),
    (r"(?i)^TALL\s+CABINETRY\s+COLOUR\b", "TALL CABINETRY COLOUR", "material"),
    (r"(?i)^FRAME\s+DETAIL(?:\s*\(.*\))?\b", "FRAME DETAIL", "material"),
    (r"(?i)^VERTICAL\s+UPPER\s+PARTITIONS\s+CABINETRY\s+COLOUR(?:\s*\(.*\))?\b", "VERTICAL UPPER PARTITIONS CABINETRY COLOUR (REFER TO DRAWINGS)", "material"),
    (r"(?i)^BOOKSHELF\b", "BOOKSHELF", "material"),
    (r"(?i)^BENCHSEAT\s+AND\s+ROBE\s+INTERNALS\s+CABINETRY\s+COLOUR\b", "BENCHSEAT AND ROBE INTERNALS CABINETRY COLOUR", "material"),
    (r"(?i)^EVYN'?S\s+ROOM\s+DRAWERS?\s*&\s*SHELF\b", "EVYN'S ROOM DRAWERS & SHELF", "material"),
    (r"(?i)^UPPER\s*/\s*FEATURE\s+CABINETRY\s+COLOUR\b", "FEATURE COLOUR OVERHEADS", "material"),
    (r"(?i)^FEATURE\s+CABINETRY\s+COLOUR\b", "FEATURE CABINETRY COLOUR", "material"),
    (r"(?i)^FEATURE\s+TALL\s+CABINETRY\s+COLOUR\b", "FEATURE TALL CABINETRY COLOUR", "material"),
    (r"(?i)^FEATURE\s+COLOUR\s+OVERHEADS?\b", "FEATURE COLOUR OVERHEADS", "material"),
    (r"(?i)^FEATURE\s+ISLAND\s+COLOUR\b", "FEATURE ISLAND COLOUR", "material"),
    (r"(?i)^ISLAND\s+CABINETRY\s+COLOUR\b", "ISLAND CABINETRY COLOUR", "material"),
    (r"(?i)^BASE\s+CABINETRY\s+COLOUR\b", "BASE CABINETRY COLOUR", "material"),
    (r"(?i)^UPPER\s+CABINETRY\s+COLOUR\b", "UPPER CABINETRY COLOUR", "material"),
    (r"(?i)^OVERHEAD\s+CABINETS?\b", "OVERHEAD CABINETS", "material"),
    (r"(?i)^OVERHEAD\s+CUPBOARDS?\b", "OVERHEAD CUPBOARDS", "material"),
    (r"(?i)^SHELVING\s+CABINETRY\s+COLOUR\b", "SHELVING CABINETRY COLOUR", "material"),
    (r"(?i)^FLOATING\s+SHELVES?\b", "FLOATING SHELVES", "material"),
    (r"(?i)^OPEN\s+SHELVES?\b", "OPEN SHELVES", "material"),
    (r"(?i)^TO\s+BE\s+OPEN\s+SHELVES?\b", "TO BE OPEN SHELVES", "other"),
    (r"(?i)^NO\s+HANDLES?\s*-\s*FEATURE\s+OVERHEADS?\b", "NO HANDLES - FEATURE OVERHEADS", "handle"),
    (r"(?i)^FEATURE\s+OVERHEADS?\s*-\s*NO\s+HANDLES?\b", "FEATURE OVERHEADS - NO HANDLES", "handle"),
    (r"(?i)^NO\s+HANDLES?\s+OVERHEADS?\b", "NO HANDLES OVERHEADS", "handle"),
    (r"(?i)^HANDLES?\s+TO\s+OVERHEADS?\b", "HANDLES to OVERHEADS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*BASE\s*CABS?\s*\+\s*DRAWERS\b", "HANDLES - BASE CABS + DRAWERS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*BASE\s*DRAWERS\s*\+\s*TALL\s+PANTRY\s+DOORS\b", "HANDLES - BASE DRAWERS + TALL PANTRY DOORS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*BASE\s*DRAWERS\b", "HANDLES - BASE DRAWERS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*BASE\s*DOORS\b", "HANDLES - BASE DOORS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s+BASE\s*DOORS\b", "HANDLES BASE DOORS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*BASE\s*CABS?\b", "HANDLES - BASE CABS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*DRAWERS\b", "HANDLES - DRAWERS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*TALL\s+CABS?\s*/\s*PANTRY\s+CABS?\s*ONLY\b", "HANDLES - TALL CABS / PANTRY CABS ONLY", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s*-\s*TALL\s+CABS?\b", "HANDLES - TALL CABS", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\s+TALL\s*\+\s*PANTRY\s+DRAWERS\b", "HANDLES TALL + PANTRY DRAWERS", "handle"),
    (r"(?i)^CUSTOM\s+HANDLES?\b", "CUSTOM HANDLES", "handle"),
    (r"(?i)^(?:HANLDES|HANDLES)\b", "HANDLES", "handle"),
    (r"(?i)^KNOB\b", "KNOB", "handle"),
    (r"(?i)^KICKBOARDS?\b", "KICKBOARDS", "material"),
    (r"(?i)^SPLASHBACK\b", "SPLASHBACK", "material"),
    (r"(?i)^BENCHTOP\b", "BENCHTOP", "material"),
    (r"(?i)^BIN\b", "BIN", "accessory"),
    (r"(?i)^DESK\s+GROMMETS?\b", "DESK GROMMETS", "accessory"),
    (r"(?i)^IRONING\s+BOARD\b", "IRONING BOARD", "accessory"),
    (r"(?i)^TROUSER\s+RACK\b", "TROUSER RACK", "accessory"),
    (r"(?i)^HANGING\s+RAIL\b", "HANGING RAIL", "accessory"),
)


def _postprocess_builder_layout(
    *,
    builder_name: str,
    source_kind: str,
    layout: dict[str, Any],
    raw_page_text: str,
    provider: str = "",
    table_rows: list[Any] | None = None,
    document_path: Path | None = None,
    page_no: int = 0,
) -> dict[str, Any]:
    normalized = _normalize_page_layout(layout)
    if source_kind == "spec" and _is_imperial_builder_name(builder_name):
        normalized = _canonicalize_imperial_joinery_material_layout(
            normalized,
            raw_page_text=raw_page_text,
            provider=provider,
            table_rows=table_rows or [],
            document_path=document_path,
            page_no=page_no,
        )
    return _normalize_page_layout(normalized)


def _canonicalize_imperial_joinery_material_layout(
    layout: dict[str, Any],
    *,
    raw_page_text: str,
    provider: str = "",
    table_rows: list[Any] | None = None,
    document_path: Path | None = None,
    page_no: int = 0,
) -> dict[str, Any]:
    normalized = _normalize_page_layout(layout)
    if not _imperial_layout_looks_like_joinery_material(normalized, raw_page_text):
        return normalized
    title = parsing._extract_imperial_section_title(raw_page_text) if hasattr(parsing, "_extract_imperial_section_title") else ""
    section_room = parsing.normalize_space(re.sub(r"(?i)\s+JOINERY SELECTION SHEET\b", "", title)).strip(" -")
    if not section_room and document_path and page_no > 1:
        section_room = _infer_imperial_continuation_room_scope(str(document_path), page_no, raw_page_text)
    raw_blocks = list(normalized.get("room_blocks", []) or [])
    if not raw_blocks:
        raw_blocks = [{"room_label": str(normalized.get("room_label", "") or ""), "rows": list(normalized.get("rows", []) or [])}]
    canonical_rows: list[dict[str, str]] = []
    for raw_block in raw_blocks:
        if not isinstance(raw_block, dict):
            continue
        block_label = parsing.normalize_space(str(raw_block.get("room_label", "") or ""))
        for raw_row in raw_block.get("rows", []) or []:
            if not isinstance(raw_row, dict):
                continue
            canonical_row = _canonicalize_imperial_joinery_row(block_label, raw_row)
            if canonical_row:
                canonical_rows.append(canonical_row)
    separator_model = (
        _load_pdfplumber_page_separator_model(str(document_path or ""), page_no)
        if document_path and page_no > 0
        else ImperialSeparatorModel()
    )
    repaired_page = _repair_imperial_joinery_grid_page(
        canonical_rows=canonical_rows,
        raw_page_text=raw_page_text,
        room_scope=section_room,
        provider=provider,
        page_no=page_no,
        table_rows=table_rows or [],
        page_words=_load_pdfplumber_page_words(str(document_path or ""), page_no) if document_path and page_no > 0 else (),
        separator_model=separator_model,
    )
    if repaired_page and repaired_page.rows:
        return repaired_page.to_layout()
    canonical_rows = _normalize_layout_rows(canonical_rows)
    if not canonical_rows:
        return normalized
    if section_room:
        return _normalize_page_layout(
            {
                "page_type": "joinery",
                "section_label": section_room,
                "room_label": section_room,
                "room_blocks": [{"room_label": section_room, "rows": canonical_rows}],
                "rows": [],
                "provider": provider,
            }
        )
    return _normalize_page_layout(
        {
            "page_type": "joinery",
            "section_label": str(normalized.get("section_label", "") or ""),
            "room_label": "",
            "room_blocks": [],
            "rows": canonical_rows,
            "provider": provider,
        }
    )


def _repair_imperial_joinery_grid_page(
    *,
    canonical_rows: list[dict[str, str]],
    raw_page_text: str,
    room_scope: str,
    provider: str,
    page_no: int = 0,
    table_rows: list[Any] | None = None,
    page_words: tuple[tuple[float, float, str], ...] = (),
    separator_model: ImperialSeparatorModel | None = None,
) -> ImperialGridPage | None:
    normalized_room = parsing.normalize_space(room_scope)
    word_grid_rows, unresolved_rows = _extract_imperial_joinery_word_grid_rows(
        page_words,
        room_scope=normalized_room,
        page_no=page_no,
        separator_model=separator_model or ImperialSeparatorModel(),
    )
    text_rows = word_grid_rows or _extract_imperial_joinery_text_grid_rows(raw_page_text, room_scope=normalized_room)
    candidate_rows = [
        _imperial_grid_row_from_layout_row(row, room_scope=normalized_room, row_order=index)
        for index, row in enumerate(canonical_rows)
        if isinstance(row, dict)
    ]
    if not text_rows:
        if not candidate_rows:
            return None
        return ImperialGridPage(room_scope=normalized_room, rows=candidate_rows, provider=provider, unresolved_rows=unresolved_rows)
    merged_rows = _merge_imperial_grid_rows(text_rows, candidate_rows)
    if not merged_rows:
        return None
    return ImperialGridPage(room_scope=normalized_room, rows=merged_rows, provider=provider, unresolved_rows=unresolved_rows)


@functools.lru_cache(maxsize=512)
def _load_pdfplumber_page_words(document_path: str, page_no: int) -> tuple[tuple[float, float, str], ...]:
    path = str(document_path or "").strip()
    if not path or page_no <= 0:
        return ()
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ()
    try:
        with pdfplumber.open(path) as pdf:
            if page_no - 1 < 0 or page_no - 1 >= len(pdf.pages):
                return ()
            page = pdf.pages[page_no - 1]
            words = page.extract_words(
                x_tolerance=2,
                y_tolerance=2,
                keep_blank_chars=False,
                use_text_flow=False,
            ) or []
    except Exception:
        return ()
    normalized_words: list[tuple[float, float, str]] = []
    for word in words:
        text = parsing.normalize_space(str(word.get("text", "") or ""))
        if not text:
            continue
        normalized_words.append((float(word.get("top", 0.0) or 0.0), float(word.get("x0", 0.0) or 0.0), text))
    return tuple(normalized_words)


def _imperial_separator_edge_values(items: list[dict[str, Any]], *, keys: tuple[str, ...]) -> list[float]:
    values: list[float] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in keys:
            try:
                value = float(item.get(key, 0.0) or 0.0)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0:
                values.append(round(value, 1))
    return sorted(set(values))


def _imperial_separator_segments(
    items: list[dict[str, Any]],
    *,
    orientation: str,
) -> list[dict[str, float]]:
    segments: list[dict[str, float]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            x0 = float(item.get("x0", 0.0) or 0.0)
            x1 = float(item.get("x1", 0.0) or 0.0)
            top = float(item.get("top", 0.0) or 0.0)
            bottom = float(item.get("bottom", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if orientation == "horizontal":
            if x1 <= x0:
                continue
            if top <= 0.0 and bottom <= 0.0:
                continue
            edges = [value for value in (top, bottom) if value > 0.0]
            if not edges:
                continue
            for edge in sorted(set(round(value, 1) for value in edges)):
                segments.append({
                    "orientation": "horizontal",
                    "edge": edge,
                    "start": round(x0, 1),
                    "end": round(x1, 1),
                    "source": "visible_rect" if any(key in item for key in ("height", "width")) else "visible_line",
                    "confidence": "visible",
                })
        else:
            if bottom <= top:
                continue
            if x0 <= 0.0 and x1 <= 0.0:
                continue
            edges = [value for value in (x0, x1) if value > 0.0]
            if not edges:
                continue
            for edge in sorted(set(round(value, 1) for value in edges)):
                segments.append({
                    "orientation": "vertical",
                    "edge": edge,
                    "start": round(top, 1),
                    "end": round(bottom, 1),
                    "source": "visible_rect" if any(key in item for key in ("height", "width")) else "visible_line",
                    "confidence": "visible",
                })
    normalized: list[dict[str, float]] = []
    seen: set[tuple[float, float, float]] = set()
    for segment in segments:
        key = (
            round(float(segment.get("edge", 0.0) or 0.0), 1),
            round(float(segment.get("start", 0.0) or 0.0), 1),
            round(float(segment.get("end", 0.0) or 0.0), 1),
        )
        if key in seen:
            continue
        seen.add(key)
        normalized.append(segment)
    return sorted(normalized, key=lambda item: (item["edge"], item["start"], item["end"]))


def _imperial_bridge_separator_segments_across_images(
    *,
    orientation: str,
    visible_segments: list[dict[str, float]],
    image_bboxes: list[dict[str, float]],
) -> list[dict[str, float]]:
    if not visible_segments or not image_bboxes:
        return []
    inferred: list[dict[str, float]] = []
    if orientation == "horizontal":
        for image_box in image_bboxes:
            left_segments = [
                segment
                for segment in visible_segments
                if image_box["top"] - 6.0 <= float(segment.get("edge", 0.0) or 0.0) <= image_box["bottom"] + 6.0
                and float(segment.get("end", 0.0) or 0.0) <= image_box["x0"] + 8.0
            ]
            right_segments = [
                segment
                for segment in visible_segments
                if image_box["top"] - 6.0 <= float(segment.get("edge", 0.0) or 0.0) <= image_box["bottom"] + 6.0
                and float(segment.get("start", 0.0) or 0.0) >= image_box["x1"] - 8.0
            ]
            for left in left_segments:
                for right in right_segments:
                    if abs(float(left.get("edge", 0.0) or 0.0) - float(right.get("edge", 0.0) or 0.0)) > 4.0:
                        continue
                    inferred.append(
                        {
                            "orientation": "horizontal",
                            "edge": round((float(left.get("edge", 0.0) or 0.0) + float(right.get("edge", 0.0) or 0.0)) / 2.0, 1),
                            "start": round(float(left.get("start", 0.0) or 0.0), 1),
                            "end": round(float(right.get("end", 0.0) or 0.0), 1),
                            "source": "bridge_over_image",
                            "confidence": "inferred_high",
                        }
                    )
    else:
        for image_box in image_bboxes:
            above_segments = [
                segment
                for segment in visible_segments
                if image_box["x0"] - 6.0 <= float(segment.get("edge", 0.0) or 0.0) <= image_box["x1"] + 6.0
                and float(segment.get("end", 0.0) or 0.0) <= image_box["top"] + 8.0
            ]
            below_segments = [
                segment
                for segment in visible_segments
                if image_box["x0"] - 6.0 <= float(segment.get("edge", 0.0) or 0.0) <= image_box["x1"] + 6.0
                and float(segment.get("start", 0.0) or 0.0) >= image_box["bottom"] - 8.0
            ]
            for above in above_segments:
                for below in below_segments:
                    if abs(float(above.get("edge", 0.0) or 0.0) - float(below.get("edge", 0.0) or 0.0)) > 4.0:
                        continue
                    inferred.append(
                        {
                            "orientation": "vertical",
                            "edge": round((float(above.get("edge", 0.0) or 0.0) + float(below.get("edge", 0.0) or 0.0)) / 2.0, 1),
                            "start": round(float(above.get("start", 0.0) or 0.0), 1),
                            "end": round(float(below.get("end", 0.0) or 0.0), 1),
                            "source": "bridge_over_image",
                            "confidence": "inferred_high",
                        }
                    )
    normalized: list[dict[str, float]] = []
    seen: set[tuple[float, float, float]] = set()
    for segment in inferred:
        key = (
            round(float(segment.get("edge", 0.0) or 0.0), 1),
            round(float(segment.get("start", 0.0) or 0.0), 1),
            round(float(segment.get("end", 0.0) or 0.0), 1),
        )
        if key in seen:
            continue
        seen.add(key)
        normalized.append(segment)
    return sorted(normalized, key=lambda item: (item["edge"], item["start"], item["end"]))


def _infer_imperial_separator_model_for_page(
    *,
    page: Any,
    visible_horizontal_edges: list[float],
    visible_vertical_edges: list[float],
) -> ImperialSeparatorModel:
    image_bboxes: list[dict[str, float]] = []
    for raw_image in list(getattr(page, "images", []) or []):
        if not isinstance(raw_image, dict):
            continue
        try:
            x0 = float(raw_image.get("x0", 0.0) or 0.0)
            x1 = float(raw_image.get("x1", 0.0) or 0.0)
            top = float(raw_image.get("top", 0.0) or 0.0)
            bottom = float(raw_image.get("bottom", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if x1 <= x0 or bottom <= top:
            continue
        image_bboxes.append({"x0": x0, "x1": x1, "top": top, "bottom": bottom})
    visible_horizontal_segments = _imperial_separator_segments(
        list(getattr(page, "lines", []) or []) + list(getattr(page, "rects", []) or []),
        orientation="horizontal",
    )
    visible_vertical_segments = _imperial_separator_segments(
        list(getattr(page, "lines", []) or []) + list(getattr(page, "rects", []) or []),
        orientation="vertical",
    )
    inferred_horizontal_edges: list[float] = []
    inferred_vertical_edges: list[float] = []
    inferred_horizontal_segments = _imperial_bridge_separator_segments_across_images(
        orientation="horizontal",
        visible_segments=visible_horizontal_segments,
        image_bboxes=image_bboxes,
    )
    inferred_vertical_segments = _imperial_bridge_separator_segments_across_images(
        orientation="vertical",
        visible_segments=visible_vertical_segments,
        image_bboxes=image_bboxes,
    )
    if len(visible_vertical_edges) >= 2:
        inferred_vertical_edges = sorted(set(round(value, 1) for value in visible_vertical_edges))
    if image_bboxes and visible_horizontal_edges:
        for image_box in image_bboxes:
            above = [
                edge
                for edge in visible_horizontal_edges
                if image_box["top"] - 18.0 <= edge <= image_box["top"] + 18.0
            ]
            below = [
                edge
                for edge in visible_horizontal_edges
                if image_box["bottom"] - 18.0 <= edge <= image_box["bottom"] + 18.0
            ]
            if above:
                inferred_horizontal_edges.extend(above)
            if below:
                inferred_horizontal_edges.extend(below)
    inferred_horizontal_edges.extend(
        round(float(segment.get("edge", 0.0) or 0.0), 1)
        for segment in inferred_horizontal_segments
        if float(segment.get("edge", 0.0) or 0.0) > 0.0
    )
    inferred_vertical_edges.extend(
        round(float(segment.get("edge", 0.0) or 0.0), 1)
        for segment in inferred_vertical_segments
        if float(segment.get("edge", 0.0) or 0.0) > 0.0
    )
    return ImperialSeparatorModel(
        visible_vertical_edges=visible_vertical_edges,
        visible_horizontal_edges=visible_horizontal_edges,
        inferred_vertical_edges=sorted(set(round(value, 1) for value in inferred_vertical_edges)),
        inferred_horizontal_edges=sorted(set(round(value, 1) for value in inferred_horizontal_edges)),
        visible_vertical_segments=visible_vertical_segments,
        visible_horizontal_segments=visible_horizontal_segments,
        inferred_vertical_segments=inferred_vertical_segments,
        inferred_horizontal_segments=inferred_horizontal_segments,
        image_bboxes=image_bboxes,
    )


@functools.lru_cache(maxsize=256)
def _load_pdfplumber_page_separator_model(document_path: str, page_no: int) -> ImperialSeparatorModel:
    path = str(document_path or "").strip()
    if not path or page_no <= 0:
        return ImperialSeparatorModel()
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ImperialSeparatorModel()
    try:
        with pdfplumber.open(path) as pdf:
            if page_no - 1 < 0 or page_no - 1 >= len(pdf.pages):
                return ImperialSeparatorModel()
            page = pdf.pages[page_no - 1]
            visible_horizontal_edges = _imperial_separator_edge_values(
                list(getattr(page, "lines", []) or []),
                keys=("top", "bottom"),
            )
            visible_vertical_edges = _imperial_separator_edge_values(
                list(getattr(page, "lines", []) or []),
                keys=("x0", "x1"),
            )
            rects = list(getattr(page, "rects", []) or [])
            visible_horizontal_edges.extend(
                value
                for value in _imperial_separator_edge_values(rects, keys=("top", "bottom"))
                if value not in visible_horizontal_edges
            )
            visible_vertical_edges.extend(
                value
                for value in _imperial_separator_edge_values(rects, keys=("x0", "x1"))
                if value not in visible_vertical_edges
            )
            return _infer_imperial_separator_model_for_page(
                page=page,
                visible_horizontal_edges=sorted(set(visible_horizontal_edges)),
                visible_vertical_edges=sorted(set(visible_vertical_edges)),
            )
    except Exception:
        return ImperialSeparatorModel()


def _enrich_imperial_separator_model_for_row_bands(
    separator_model: ImperialSeparatorModel,
    row_bands: list[dict[str, Any]],
) -> ImperialSeparatorModel:
    if not row_bands:
        return separator_model
    inferred_horizontal_edges = list(separator_model.inferred_horizontal_edges)
    visible_horizontal_edges = list(separator_model.visible_horizontal_edges)
    inferred_horizontal_segments = list(separator_model.inferred_horizontal_segments)
    for previous, current in zip(row_bands, row_bands[1:]):
        previous_top = float(previous.get("top", 0.0) or 0.0)
        current_top = float(current.get("top", 0.0) or 0.0)
        if current_top <= previous_top:
            continue
        midpoint = round((previous_top + current_top) / 2.0, 1)
        if any(abs(edge - midpoint) <= 4.0 for edge in visible_horizontal_edges):
            continue
        gap = current_top - previous_top
        current_text = parsing.normalize_space(str(current.get("text", "") or ""))
        start = min((float(word[1]) for word in list(previous.get("words", []) or []) + list(current.get("words", []) or [])), default=0.0)
        end = max((float(word[1]) for word in list(previous.get("words", []) or []) + list(current.get("words", []) or [])), default=0.0)
        if gap >= 18.0:
            inferred_horizontal_edges.append(midpoint)
            inferred_horizontal_segments.append(
                {
                    "orientation": "horizontal",
                    "edge": midpoint,
                    "start": round(start, 1),
                    "end": round(end, 1),
                    "source": "row_gap",
                    "confidence": "inferred_high",
                }
            )
            continue
        if current_text and _imperial_five_column_item_starts_row(current_text):
            inferred_horizontal_edges.append(midpoint)
            inferred_horizontal_segments.append(
                {
                    "orientation": "horizontal",
                    "edge": midpoint,
                    "start": round(start, 1),
                    "end": round(end, 1),
                    "source": "row_gap",
                    "confidence": "inferred_low",
                }
            )
    return ImperialSeparatorModel(
        visible_vertical_edges=list(separator_model.visible_vertical_edges),
        visible_horizontal_edges=list(separator_model.visible_horizontal_edges),
        inferred_vertical_edges=list(separator_model.inferred_vertical_edges),
        inferred_horizontal_edges=sorted(set(round(value, 1) for value in inferred_horizontal_edges)),
        visible_vertical_segments=list(separator_model.visible_vertical_segments),
        visible_horizontal_segments=list(separator_model.visible_horizontal_segments),
        inferred_vertical_segments=list(separator_model.inferred_vertical_segments),
        inferred_horizontal_segments=list(inferred_horizontal_segments),
        image_bboxes=list(separator_model.image_bboxes),
    )


def _imperial_separator_segment_for_boundary(
    separator_model: ImperialSeparatorModel,
    *,
    previous_bottom: float,
    current_top: float,
) -> dict[str, Any]:
    if previous_bottom <= 0 or current_top <= 0 or current_top <= previous_bottom:
        return {}
    midpoint = (previous_bottom + current_top) / 2.0
    boundary_min = previous_bottom + 1.5
    boundary_max = current_top + 3.5

    def _best_match(segments: list[dict[str, Any]]) -> dict[str, Any]:
        best: dict[str, Any] = {}
        best_score = float("inf")
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            edge = float(segment.get("edge", 0.0) or 0.0)
            if not edge:
                continue
            if not (abs(edge - midpoint) <= 6.0 or boundary_min <= edge <= boundary_max):
                continue
            score = abs(edge - midpoint)
            if score < best_score:
                best_score = score
                best = dict(segment)
        return best

    return (
        _best_match(list(separator_model.visible_horizontal_segments))
        or _best_match(list(separator_model.inferred_horizontal_segments))
    )


def _imperial_separator_confidence_for_boundary(
    separator_model: ImperialSeparatorModel,
    *,
    previous_bottom: float,
    current_top: float,
) -> str:
    if previous_bottom <= 0 or current_top <= 0 or current_top <= previous_bottom:
        return "none"
    midpoint = (previous_bottom + current_top) / 2.0
    boundary_min = previous_bottom + 1.5
    boundary_max = current_top + 3.5
    if any(
        abs(edge - midpoint) <= 6.0 or boundary_min <= edge <= boundary_max
        for edge in separator_model.visible_horizontal_edges
    ):
        return "visible"
    if any(
        abs(edge - midpoint) <= 6.0 or boundary_min <= edge <= boundary_max
        for edge in separator_model.inferred_horizontal_edges
    ):
        return "inferred_high"
    if current_top - previous_bottom >= 14.0:
        return "inferred_low"
    return "none"


def _imperial_visual_fragment_from_row(
    row: ImperialFiveColumnRow,
    *,
    separator_model: ImperialSeparatorModel,
    previous_row: ImperialFiveColumnRow | None = None,
) -> dict[str, Any]:
    provenance = _coerce_provenance_dict(row.provenance)
    area_box = _coerce_provenance_dict(provenance.get("area_or_item"))
    description_box = _coerce_provenance_dict(provenance.get("specs_or_description"))
    supplier_box = _coerce_provenance_dict(provenance.get("supplier"))
    notes_box = _coerce_provenance_dict(provenance.get("notes"))
    top_candidates = [
        float(box.get("top", 0.0) or 0.0)
        for box in (area_box, description_box, supplier_box, notes_box)
        if isinstance(box, dict) and float(box.get("top", 0.0) or 0.0) > 0.0
    ]
    bottom_candidates = [
        float(box.get("bottom", 0.0) or 0.0)
        for box in (area_box, description_box, supplier_box, notes_box)
        if isinstance(box, dict) and float(box.get("bottom", 0.0) or 0.0) > 0.0
    ]
    previous_bottom = 0.0
    if previous_row is not None:
        previous_provenance = _coerce_provenance_dict(previous_row.provenance)
        prev_boxes = [
            _coerce_provenance_dict(previous_provenance.get(key))
            for key in ("area_or_item", "specs_or_description", "supplier", "notes")
        ]
        previous_bottom = max(
            (
                float(box.get("bottom", 0.0) or 0.0)
                for box in prev_boxes
                if isinstance(box, dict) and float(box.get("bottom", 0.0) or 0.0) > 0.0
            ),
            default=0.0,
        )
    current_top = min(top_candidates) if top_candidates else 0.0
    separator_confidence = _imperial_separator_confidence_for_boundary(
        separator_model,
        previous_bottom=previous_bottom,
        current_top=current_top,
    )
    separator_segment = _imperial_separator_segment_for_boundary(
        separator_model,
        previous_bottom=previous_bottom,
        current_top=current_top,
    )
    return {
        "area_or_item": parsing.normalize_space(row.area_or_item),
        "specs_or_description": parsing.normalize_space(row.specs_or_description),
        "supplier": parsing.normalize_space(row.supplier),
        "notes": parsing.normalize_space(row.notes),
        "row_band": int(provenance.get("row_band", 0) or 0),
        "top": current_top,
        "bottom": max(bottom_candidates) if bottom_candidates else current_top,
        "separator_confidence_from_previous": separator_confidence,
        "separator_kind_from_previous": separator_confidence,
        "separator_segment_from_previous": separator_segment,
    }


@functools.lru_cache(maxsize=256)
def _load_pdfplumber_page_text(document_path: str, page_no: int) -> str:
    path = str(document_path or "").strip()
    if not path or page_no <= 0:
        return ""
    try:
        import pdfplumber  # type: ignore
    except Exception:
        return ""
    try:
        with pdfplumber.open(path) as pdf:
            if page_no - 1 < 0 or page_no - 1 >= len(pdf.pages):
                return ""
            return parsing.normalize_space(pdf.pages[page_no - 1].extract_text() or "")
    except Exception:
        return ""


def _infer_imperial_continuation_room_scope(document_path: str, page_no: int, raw_page_text: str) -> str:
    upper = str(raw_page_text or "").upper()
    if not any(
        marker in upper
        for marker in (
            "CUSTOM HANDLES",
            "HANDLES",
            "KICKBOARDS",
            "SPLASHBACK",
            "BENCHTOP",
            "BASE CABINETRY",
            "UPPER CABINETRY",
            "FLOATING SHELV",
        )
    ):
        return ""
    for previous_page in range(page_no - 1, max(0, page_no - 4), -1):
        previous_text = _load_pdfplumber_page_text(document_path, previous_page)
        if not previous_text:
            continue
        title = parsing._extract_imperial_section_title(previous_text) if hasattr(parsing, "_extract_imperial_section_title") else ""
        if title:
            return parsing.normalize_space(re.sub(r"(?i)\s+JOINERY SELECTION SHEET\b", "", title)).strip(" -")
    return ""


def _group_imperial_word_row_bands(
    page_words: tuple[tuple[float, float, str], ...],
    *,
    tolerance: float = 8.0,
) -> list[dict[str, Any]]:
    if not page_words:
        return []
    sorted_words = sorted(page_words, key=lambda item: (item[0], item[1]))
    grouped_rows: list[list[tuple[float, float, str]]] = []
    current_row: list[tuple[float, float, str]] = []
    current_top: float | None = None
    for top, x0, text in sorted_words:
        if current_top is None or abs(top - current_top) <= tolerance:
            current_row.append((top, x0, text))
            current_top = top if current_top is None else min(current_top, top)
            continue
        grouped_rows.append(sorted(current_row, key=lambda item: item[1]))
        current_row = [(top, x0, text)]
        current_top = top
    if current_row:
        grouped_rows.append(sorted(current_row, key=lambda item: item[1]))
    row_bands: list[dict[str, Any]] = []
    for row_band, words in enumerate(grouped_rows):
        row_bands.append(
            {
                "row_band": row_band,
                "words": words,
                "text": parsing.normalize_space(" ".join(word for _, _, word in words)),
                "top": min(float(top) for top, _, _ in words) if words else 0.0,
            }
        )
    return row_bands


def _looks_like_imperial_table_header_row(text: str) -> bool:
    upper = parsing.normalize_space(text).upper()
    if not upper:
        return False
    has_label = "AREA / ITEM" in upper or re.search(r"(?i)\bITEM\b", upper)
    has_description = "SPECS / DESCRIPTION" in upper or re.search(r"(?i)\bDESCRIPTION\b", upper)
    has_supplier = "SUPPLIER" in upper or "MANUFACTURER" in upper
    return bool(has_label and has_description and has_supplier)


def _split_imperial_word_page_regions(
    row_bands: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int | None, list[dict[str, Any]], list[dict[str, Any]]]:
    if not row_bands:
        return [], None, [], []
    table_header_index = next(
        (
            index
            for index, row in enumerate(row_bands)
            if _looks_like_imperial_table_header_row(str(row.get("text", "") or ""))
        ),
        None,
    )
    content_start = table_header_index + 1 if table_header_index is not None else 0
    header_rows = row_bands[:content_start]
    content_rows: list[dict[str, Any]] = []
    footer_rows: list[dict[str, Any]] = []
    for index, row in enumerate(row_bands[content_start:], start=content_start):
        row_text = parsing.normalize_space(str(row.get("text", "") or ""))
        upper = row_text.upper()
        if any(
            marker in upper
            for marker in (
                "ALL COLOURS SHOWN",
                "PRODUCT AVAILABILITY",
                "DESIGNER:",
                "CLIENT NAME:",
                "SIGNATURE:",
                "SIGNED DATE:",
                "DOCUMENT REF:",
                "PAGE ",
            )
        ):
            footer_rows = row_bands[index:]
            break
        if _looks_like_layout_metadata_line(row_text):
            footer_rows.append(row)
            continue
        content_rows.append(row)
    return header_rows, table_header_index, content_rows, footer_rows


def _infer_imperial_column_model(
    row_bands: list[dict[str, Any]],
    *,
    table_header_index: int | None,
) -> ImperialColumnModel:
    model = ImperialColumnModel()
    if table_header_index is None or table_header_index >= len(row_bands):
        return model
    header_words = list(row_bands[table_header_index].get("words", []) or [])
    if not header_words:
        return model
    description_start: float | None = None
    image_start: float | None = None
    supplier_start: float | None = None
    notes_start: float | None = None
    for _, x0, text in header_words:
        upper = parsing.normalize_space(text).upper()
        if not upper:
            continue
        if (
            "SPECS" in upper
            or "DESCRIPTION" in upper
            or "SELECTION LEVEL" in upper
        ):
            description_start = x0 if description_start is None else min(description_start, x0)
        elif upper == "IMAGE" or upper.startswith("IMAGE "):
            image_start = x0 if image_start is None else min(image_start, x0)
        elif "SUPPLIER" in upper or "MANUFACTURER" in upper:
            supplier_start = x0 if supplier_start is None else min(supplier_start, x0)
        elif "NOTES" in upper or "COMMENT" in upper:
            notes_start = x0 if notes_start is None else min(notes_start, x0)
    if description_start is not None and description_start > 120:
        model.label_right = max(160.0, description_start - 18.0)
    if image_start is not None and image_start > model.label_right + 120:
        model.description_right = image_start - 18.0
        model.has_image_column = True
    elif supplier_start is not None and supplier_start > model.label_right + 120:
        model.description_right = supplier_start - 18.0
    if model.has_image_column and supplier_start is not None and supplier_start > model.description_right + 40:
        model.image_right = supplier_start - 18.0
    else:
        model.image_right = model.description_right
    if notes_start is not None and notes_start > model.description_right + 80:
        model.supplier_right = notes_start - 18.0
    if model.description_right <= model.label_right:
        model.description_right = max(model.label_right + 260.0, 780.0)
    if model.has_image_column and model.image_right <= model.description_right:
        model.image_right = max(model.description_right + 60.0, model.description_right)
    if model.supplier_right <= model.description_right:
        model.supplier_right = max(model.description_right + 120.0, 940.0)
    return model


def _extract_imperial_cells_from_word_rows(
    row_bands: list[dict[str, Any]],
    *,
    column_model: ImperialColumnModel,
) -> list[ImperialCell]:
    cells: list[ImperialCell] = []
    for row in row_bands:
        row_band = int(row.get("row_band", 0) or 0)
        top = float(row.get("top", 0.0) or 0.0)
        for _, x0, text in list(row.get("words", []) or []):
            cleaned = parsing.normalize_space(text)
            if not cleaned:
                continue
            cells.append(
                ImperialCell(
                    text=cleaned,
                    col_role=column_model.role_for_x(float(x0)),
                    row_band=row_band,
                    x0=float(x0),
                    top=top,
                    source="word_grid",
                )
            )
    return cells


def _match_imperial_row_from_cell_text(
    label_text: str,
    description_text: str,
    supplier_text: str = "",
    notes_text: str = "",
) -> tuple[str, str, str, str]:
    label, remainder, row_kind = _match_imperial_table_repair_row(label_text)
    if label:
        return label, remainder, row_kind, "label"
    exact_area = parsing.normalize_space(label_text)
    if exact_area and _imperial_five_column_item_starts_row(exact_area):
        return exact_area, "", _imperial_row_kind_from_area_or_item(exact_area), "area"
    combined = parsing.normalize_space(
        " ".join(
            part
            for part in (label_text, description_text, supplier_text, notes_text)
            if parsing.normalize_space(part)
        )
    )
    if not combined:
        return "", "", "", ""
    label, remainder, row_kind = _match_imperial_table_repair_row(combined)
    if label:
        return label, remainder, row_kind, "combined"
    return "", "", "", ""


def _imperial_row_role_text(row_cells: list[ImperialCell], role: str) -> str:
    return parsing.normalize_space(" ".join(cell.text for cell in row_cells if cell.col_role == role))


def _imperial_row_role_provenance(row_cells: list[ImperialCell], role: str) -> dict[str, Any]:
    role_cells = [cell for cell in row_cells if cell.col_role == role]
    if not role_cells:
        return {}
    return {
        "row_band": int(role_cells[0].row_band or 0),
        "x0": min(float(cell.x0 or 0.0) for cell in role_cells),
        "x1": max(float(cell.x1 or cell.x0 or 0.0) for cell in role_cells),
        "top": min(float(cell.top or 0.0) for cell in role_cells),
        "bottom": max(float(cell.bottom or cell.top or 0.0) for cell in role_cells),
        "text": parsing.normalize_space(" ".join(cell.text for cell in role_cells)),
        "source": _dedupe_preserve_order([parsing.normalize_space(cell.source) for cell in role_cells if parsing.normalize_space(cell.source)]),
    }


def _imperial_five_column_row_confidence(
    *,
    area_or_item: str,
    specs_or_description: str,
    supplier: str,
    notes: str,
    canonical_label: str,
    match_source: str,
) -> float:
    score = 0.0
    if area_or_item:
        score += 0.35
    if specs_or_description:
        score += 0.25
    if canonical_label:
        score += 0.2
    if match_source == "label":
        score += 0.15
    elif match_source == "combined":
        score += 0.05
    if supplier:
        score += 0.05
    if notes:
        score += 0.05
    if canonical_label and _imperial_grid_fragment_has_foreign_row_label(specs_or_description or notes, row_label=canonical_label):
        score -= 0.35
    if not canonical_label:
        score -= 0.25
    return max(0.0, min(1.0, score))


def _imperial_force_raw_area_row_start(
    area_text: str,
    description_text: str,
    supplier_text: str,
    note_text: str,
) -> bool:
    cleaned_area = parsing.normalize_space(area_text).upper()
    combined_text = parsing.normalize_space(
        " ".join(
            part
            for part in (description_text, supplier_text, note_text)
            if parsing.normalize_space(part)
        )
    )
    if cleaned_area == "DESK" and re.search(
        r"(?i)\b(?:handle|profile handle|voda|knob|finger pull|bevel edge|momo|tekform|touch catch|no handles?)\b",
        combined_text,
    ):
        return True
    if re.search(
        r"(?i)\b(?:moulding|decorative flutes?|panelling|profile|drawer handle position|door(?:\s+and\s+drawer)?\s+profile|gpo|glass doors only|back and sides(?: of island)?|bin)\b",
        cleaned_area,
    ):
        return True
    return False


def _imperial_area_or_item_overflow_looks_value(text: str) -> bool:
    cleaned = parsing.normalize_space(text).strip(" -;,")
    if not cleaned:
        return False
    if _imperial_five_column_item_is_label_continuation(cleaned):
        return False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        return False
    if re.match(r"(?i)^\d+\s*MM\b", cleaned):
        return True
    if cleaned and cleaned[0].islower():
        return True
    return bool(
        re.search(
            r"(?i)\b(?:thermolaminated|laminate|woodmatt|matt|natural|colour code|oak|walnut|clay|cinder|alabaster|gumnut|coastal|outback|florentine|boston|porcelain|blush|whiteboard|anthracite|brushed|nickel|storg|green)\b",
            cleaned,
        )
    )


def _imperial_split_area_or_item_overflow(area_or_item: str, canonical_label: str = "") -> tuple[str, str]:
    cleaned = parsing.normalize_space(area_or_item)
    canonical = parsing.normalize_space(canonical_label)
    if not cleaned:
        return "", ""
    if canonical:
        canonical_upper = canonical.upper()
        cleaned_upper = cleaned.upper()
        canonical_parenthetical_match = re.match(r"^(?P<prefix>.+?)\s*\((?P<inside>[^)]+)\)$", canonical)
        cleaned_parenthetical_match = re.match(r"^(?P<prefix>.+?)\s*\((?P<inside>[^)]+)\)$", cleaned)
        if canonical_parenthetical_match and cleaned_parenthetical_match:
            canonical_prefix = parsing.normalize_space(canonical_parenthetical_match.group("prefix"))
            cleaned_prefix = parsing.normalize_space(cleaned_parenthetical_match.group("prefix"))
            canonical_inside = parsing.normalize_space(canonical_parenthetical_match.group("inside"))
            cleaned_inside = parsing.normalize_space(cleaned_parenthetical_match.group("inside"))
            if canonical_prefix and cleaned_prefix and cleaned_prefix.upper().startswith(canonical_prefix.upper()):
                canonical_tokens = canonical_inside.split()
                cleaned_tokens = cleaned_inside.split()
                extras: list[str] = []
                cursor = 0
                for token in cleaned_tokens:
                    if cursor < len(canonical_tokens) and token.upper() == canonical_tokens[cursor].upper():
                        cursor += 1
                        continue
                    extras.append(token)
                if canonical_tokens and cursor == len(canonical_tokens):
                    cleaned = canonical
                    overflow = parsing.normalize_space(" ".join(extras))
                    if overflow:
                        return cleaned, overflow
        if cleaned_upper.startswith(canonical_upper):
            suffix = parsing.normalize_space(cleaned[len(canonical) :]).strip(" -;,")
            if suffix and _imperial_area_or_item_overflow_looks_value(suffix):
                return canonical, suffix
        if canonical_upper == "VERTICAL UPPER PARTITIONS CABINETRY COLOUR (REFER TO DRAWINGS)":
            match = re.match(
                r"(?i)^VERTICAL\s+UPPER\s+PARTITIONS\s+CABINETRY\s+COLOUR\s*\(REFER\s+TO\s+(.+?)\s+DRAWINGS\)$",
                cleaned,
            )
            if match:
                embedded = parsing.normalize_space(match.group(1))
                if embedded and _imperial_area_or_item_overflow_looks_value(embedded):
                    return canonical, embedded
        if canonical_upper == "BENCHTOP":
            bench_with_suffix_match = re.match(
                r"(?i)^BENCHTOP\s*\((?P<inside>[^)]+)\)\s*(?P<suffix>\d+\s*mm)\b$",
                cleaned,
            )
            if bench_with_suffix_match:
                inside = parsing.normalize_space(bench_with_suffix_match.group("inside"))
                suffix = parsing.normalize_space(bench_with_suffix_match.group("suffix"))
                repaired_inside = parsing.normalize_space(
                    re.sub(rf"(?i)\b{re.escape(suffix)}(?=\s+WM\s*/\s*Dryer\b)", "", inside)
                )
                if repaired_inside and repaired_inside.upper() != inside.upper():
                    return f"BENCHTOP ({repaired_inside})", suffix
            bench_parenthetical_match = re.match(
                r"(?i)^BENCHTOP\s*\((?P<inside>[^)]+)\)$",
                cleaned,
            )
            if bench_parenthetical_match:
                inside = parsing.normalize_space(bench_parenthetical_match.group("inside"))
                repaired_inside = re.sub(
                    r"(?i)\b(\d+\s*mm\b.*?)(?=\bWM\s*/\s*Dryer\b)",
                    "",
                    inside,
                )
                repaired_inside = parsing.normalize_space(repaired_inside)
                if repaired_inside and repaired_inside.upper() != inside.upper():
                    return f"BENCHTOP ({repaired_inside})", ""
    if re.match(r"(?i)^CABINETRY\s+COLOUR\b", cleaned):
        suffix = parsing.normalize_space(re.sub(r"(?i)^CABINETRY\s+COLOUR\b", "", cleaned)).strip(" -;,")
        if suffix and _imperial_area_or_item_overflow_looks_value(suffix):
            return "CABINETRY COLOUR", suffix
    thermolaminated_match = re.match(
        r"(?i)^(?P<label>.+?\bCABINETRY\s+COLOUR)\s+(?P<suffix>THERMOLAMINATED)\b$",
        cleaned,
    )
    if thermolaminated_match:
        repaired_label = parsing.normalize_space(thermolaminated_match.group("label"))
        suffix = parsing.normalize_space(thermolaminated_match.group("suffix"))
        if repaired_label and suffix:
            return repaired_label, suffix
    lighting_match = re.match(r"(?i)^(LIGHTING)\s+(\d+\s*X?)\b$", cleaned)
    if lighting_match:
        return parsing.normalize_space(lighting_match.group(1)), parsing.normalize_space(lighting_match.group(2))
    return cleaned, ""


def _imperial_merge_area_overflow_into_description(
    overflow: str,
    description: str,
    *,
    canonical_label: str = "",
) -> str:
    cleaned_overflow = parsing.normalize_space(overflow)
    cleaned_description = parsing.normalize_space(description)
    if not cleaned_overflow:
        return cleaned_description
    if (
        canonical_label.upper().startswith("BENCHTOP")
        and re.fullmatch(r"(?i)[A-Za-z]+", cleaned_overflow)
        and re.search(r"(?i)\b(?:natural|matt|woodmatt|gloss|ultramatt)\b", cleaned_description)
    ):
        repaired_description = re.sub(
            r"(?i)(-\s*)([A-Za-z][A-Za-z]+)(\s*-\s*(?:natural|matt|woodmatt|gloss|ultramatt)\b)",
            lambda match: (
                match.group(0)
                if cleaned_overflow.upper() in match.group(2).upper()
                else f"{match.group(1)}{cleaned_overflow} {match.group(2)}{match.group(3)}"
            ),
            cleaned_description,
            count=1,
        )
        if repaired_description != cleaned_description:
            return parsing.normalize_space(repaired_description)
    return parsing.normalize_space(
        " ".join(part for part in (cleaned_overflow, cleaned_description) if parsing.normalize_space(part))
    )


def _build_imperial_five_column_rows_from_cells(
    cells: list[ImperialCell],
    *,
    room_scope: str,
    page_no: int = 0,
    separator_model: ImperialSeparatorModel | None = None,
) -> list[ImperialFiveColumnRow]:
    if not cells:
        return []
    grouped: dict[int, list[ImperialCell]] = {}
    for cell in cells:
        grouped.setdefault(cell.row_band, []).append(cell)
    rows: list[ImperialFiveColumnRow] = []
    for row_band in sorted(grouped):
        row_cells = sorted(grouped[row_band], key=lambda item: (item.x0, item.text))
        original_area_or_item = _imperial_row_role_text(row_cells, "label")
        area_or_item = original_area_or_item
        specs_or_description = _imperial_row_role_text(row_cells, "description")
        image = _imperial_row_role_text(row_cells, "image")
        supplier = _imperial_row_role_text(row_cells, "supplier")
        notes = _imperial_row_role_text(row_cells, "notes")
        combined_text = parsing.normalize_space(" ".join(part for part in (area_or_item, specs_or_description, image, supplier, notes) if part))
        if not combined_text:
            continue
        if _looks_like_imperial_table_header_row(combined_text) or _looks_like_layout_metadata_line(combined_text):
            continue
        label, remainder, row_kind, match_source = _match_imperial_row_from_cell_text(
            area_or_item,
            specs_or_description,
            supplier,
            notes,
        )
        area_or_item, area_or_item_overflow = _imperial_split_area_or_item_overflow(area_or_item, label)
        if area_or_item_overflow:
            specs_or_description = _imperial_merge_area_overflow_into_description(
                area_or_item_overflow,
                specs_or_description,
                canonical_label=label,
            )
        recovered_area_or_item = ""
        if not area_or_item and label:
            recovered_area_or_item = label
            area_or_item = label
            if remainder and not specs_or_description:
                specs_or_description = remainder
            elif remainder and specs_or_description:
                specs_or_description = parsing.normalize_space(
                    re.sub(rf"(?i)^\s*{re.escape(label)}\b", "", specs_or_description)
                ) or specs_or_description
            # Once a missing label has been recovered into the label column, the
            # remaining text should flow through the explicit description/supplier
            # columns only. Keeping the raw combined-text remainder here causes the
            # same words to be appended a second time during logical-row assembly.
            remainder = ""
        confidence = _imperial_five_column_row_confidence(
            area_or_item=area_or_item,
            specs_or_description=specs_or_description,
            supplier=supplier,
            notes=notes,
            canonical_label=label,
            match_source=match_source,
        )
        provenance = {
            "page_no": int(page_no or 0),
            "row_band": int(row_band),
            "canonical_row_order": int(row_band),
            "row_band_top": min((float(cell.top or 0.0) for cell in row_cells if float(cell.top or 0.0) > 0.0), default=0.0),
            "row_band_bottom": max((float(cell.bottom or 0.0) for cell in row_cells if float(cell.bottom or 0.0) > 0.0), default=0.0),
            "area_or_item": _imperial_row_role_provenance(row_cells, "label"),
            "specs_or_description": _imperial_row_role_provenance(row_cells, "description"),
            "image": _imperial_row_role_provenance(row_cells, "image"),
            "supplier": _imperial_row_role_provenance(row_cells, "supplier"),
            "notes": _imperial_row_role_provenance(row_cells, "notes"),
            "canonical_label": label,
            "match_source": match_source,
            "raw_area_or_item": original_area_or_item,
            "separator_model": (separator_model or ImperialSeparatorModel()).to_dict(),
            "visual_subrows": [
                {
                    "separator_kind_from_previous": "visible",
                    "top": min((float(cell.top or 0.0) for cell in row_cells if float(cell.top or 0.0) > 0.0), default=0.0),
                    "bottom": max((float(cell.bottom or 0.0) for cell in row_cells if float(cell.bottom or 0.0) > 0.0), default=0.0),
                    "area_or_item": parsing.normalize_space(area_or_item),
                    "specs_or_description": parsing.normalize_space(specs_or_description),
                    "supplier": parsing.normalize_space(supplier),
                    "notes": parsing.normalize_space(notes),
                }
            ],
        }
        if recovered_area_or_item:
            provenance["recovered_area_or_item"] = recovered_area_or_item
        if area_or_item_overflow:
            provenance["area_or_item_overflow"] = area_or_item_overflow
        rows.append(
            ImperialFiveColumnRow(
                room_scope=room_scope,
                row_order=int(row_band),
                area_or_item=area_or_item,
                specs_or_description=specs_or_description,
                image=image,
                supplier=supplier,
                notes=notes,
                canonical_label=label,
                remainder=remainder,
                row_kind=row_kind,
                match_source=match_source,
                confidence=confidence,
                provenance=provenance,
                needs_review=not label or confidence < 0.55,
            )
        )
    return rows


def _repair_imperial_five_column_rows(rows: list[ImperialFiveColumnRow]) -> list[ImperialFiveColumnRow]:
    if not rows:
        return []

    def _sequence_row_start_label(item: ImperialFiveColumnRow) -> str:
        candidate = parsing.normalize_space(item.canonical_label or item.area_or_item)
        if candidate and _imperial_five_column_item_starts_row(candidate):
            return candidate
        raw_area = parsing.normalize_space(item.area_or_item)
        if raw_area and _imperial_five_column_item_starts_row(raw_area):
            return raw_area
        return ""

    def _is_soft_continuation_row(item: ImperialFiveColumnRow) -> bool:
        raw_area = parsing.normalize_space(item.area_or_item)
        if not raw_area:
            return True
        if (
            raw_area
            and _imperial_five_column_item_starts_row(raw_area)
            and not _imperial_five_column_item_is_label_continuation(raw_area)
            and not _imperial_area_or_item_overflow_looks_value(raw_area)
        ):
            return False
        if _imperial_five_column_item_is_label_continuation(raw_area):
            return True
        if not item.canonical_label:
            if raw_area.endswith(")") or _imperial_area_or_item_overflow_looks_value(raw_area):
                return True
            if len(raw_area.split()) <= 3:
                return True
        return False

    def _merge_chunk(chunk: list[ImperialFiveColumnRow], *, current: ImperialFiveColumnRow) -> ImperialFiveColumnRow | None:
        combined_area = parsing.normalize_space(
            " ".join(candidate.area_or_item for candidate in chunk if parsing.normalize_space(candidate.area_or_item))
        )
        combined_description = parsing.normalize_space(
            " ".join(candidate.specs_or_description for candidate in chunk if parsing.normalize_space(candidate.specs_or_description))
        )
        combined_supplier = parsing.normalize_space(
            " ".join(candidate.supplier for candidate in chunk if parsing.normalize_space(candidate.supplier))
        )
        combined_notes = parsing.normalize_space(
            " ".join(candidate.notes for candidate in chunk if parsing.normalize_space(candidate.notes))
        )
        label, remainder, row_kind, match_source = _match_imperial_row_from_cell_text(
            combined_area,
            combined_description,
            combined_supplier,
            combined_notes,
        )
        sequence_area = parsing.normalize_space(current.area_or_item or combined_area)
        sequence_overflow = ""
        if not label and current.canonical_label:
            label = current.canonical_label
            row_kind = current.row_kind
            match_source = current.match_source or "sequence"
        elif not label and sequence_area:
            repaired_area, repaired_overflow = _imperial_split_area_or_item_overflow(sequence_area)
            if repaired_area and _imperial_five_column_item_starts_row(repaired_area):
                label = repaired_area
                row_kind = _imperial_row_kind_from_area_or_item(repaired_area) or current.row_kind or "other"
                match_source = current.match_source or "sequence"
                sequence_area = repaired_area
                sequence_overflow = repaired_overflow
        if not label:
            return None
        remainder = ""
        cleaned_area, overflow = _imperial_split_area_or_item_overflow(combined_area, label)
        if not overflow and sequence_overflow:
            overflow = sequence_overflow
        if not cleaned_area and sequence_area:
            cleaned_area = sequence_area
        repaired_description = _imperial_merge_area_overflow_into_description(
            overflow,
            combined_description,
            canonical_label=label,
        )
        merged_provenance = _coerce_provenance_dict(current.provenance)
        separator_model = ImperialSeparatorModel(**{
            key: value
            for key, value in _coerce_provenance_dict(merged_provenance.get("separator_model")).items()
            if key in {
                "visible_vertical_edges",
                "visible_horizontal_edges",
                "inferred_vertical_edges",
                "inferred_horizontal_edges",
                "visible_vertical_segments",
                "visible_horizontal_segments",
                "inferred_vertical_segments",
                "inferred_horizontal_segments",
                "image_bboxes",
            }
        }) if _coerce_provenance_dict(merged_provenance.get("separator_model")) else ImperialSeparatorModel()
        merged_provenance["sequence_repair_span"] = len(chunk)
        merged_provenance["sequence_repair_area_or_item"] = combined_area
        merged_provenance["visual_fragments"] = [
            _imperial_visual_fragment_from_row(
                candidate,
                separator_model=separator_model,
                previous_row=chunk[idx - 1] if idx > 0 else None,
            )
            for idx, candidate in enumerate(chunk)
        ]
        merged_provenance["visual_subrows"] = _imperial_visual_subrows_from_fragments(
            list(merged_provenance["visual_fragments"])
        )
        merged_provenance["canonical_row_order"] = int(current.row_order or 0)
        merged_provenance["row_band_top"] = min(
            (
                float(fragment.get("top", 0.0) or 0.0)
                for fragment in merged_provenance["visual_fragments"]
                if float(fragment.get("top", 0.0) or 0.0) > 0.0
            ),
            default=float(merged_provenance.get("row_band_top", 0.0) or 0.0),
        )
        merged_provenance["row_band_bottom"] = max(
            (
                float(fragment.get("bottom", 0.0) or 0.0)
                for fragment in merged_provenance["visual_fragments"]
                if float(fragment.get("bottom", 0.0) or 0.0) > 0.0
            ),
            default=float(merged_provenance.get("row_band_bottom", 0.0) or 0.0),
        )
        if overflow:
            merged_provenance["area_or_item_overflow"] = overflow
        return ImperialFiveColumnRow(
            room_scope=current.room_scope,
            row_order=int(current.row_order or 0),
            area_or_item=cleaned_area or label,
            specs_or_description=repaired_description,
            image=parsing.normalize_space(" ".join(candidate.image for candidate in chunk if parsing.normalize_space(candidate.image))),
            supplier=combined_supplier,
            notes=combined_notes,
            canonical_label=label,
            remainder=remainder,
            row_kind=row_kind,
            match_source=match_source or "sequence",
            confidence=max(_normalize_confidence_value(candidate.confidence) for candidate in chunk),
            provenance=merged_provenance,
            needs_review=False,
        )

    def _continuation_compatible_with_current(current: ImperialFiveColumnRow, candidate: ImperialFiveColumnRow) -> bool:
        candidate_text = parsing.normalize_space(
            " ".join(
                part
                for part in (
                    candidate.area_or_item,
                    candidate.specs_or_description,
                    candidate.supplier,
                    candidate.notes,
                )
                if parsing.normalize_space(part)
            )
        )
        if not candidate_text:
            return True
        current_label = parsing.normalize_space(current.area_or_item or current.canonical_label)
        current_kind = str(current.row_kind or "").lower()
        current_row_start = _sequence_row_start_label(current)
        candidate_row_start = _sequence_row_start_label(candidate)
        if current_row_start and candidate_row_start:
            current_upper = current_row_start.upper()
            candidate_upper = candidate_row_start.upper()
            same_start_family = (
                current_upper == candidate_upper
                or candidate_upper.startswith(f"{current_upper} ")
                or current_upper.startswith(f"{candidate_upper} ")
            )
            if not same_start_family:
                return False
        if current_kind != "handle" and re.search(
            r"(?i)\b(?:handles?|knob|voda profile handle|momo|tekform|bevel edge|finger pull)\b",
            candidate_text,
        ):
            return False
        if re.search(r"(?i)\b(?:cabinetry colour|colour)\b", current_label) and re.search(
            r"(?i)\b(?:moulding|decorative flutes?|panelling|drawer handle position|door(?:\s+and\s+drawer)?\s+profile|gpo|cut out required)\b",
            candidate_text,
        ):
            return False
        if current_kind == "handle" and re.search(
            r"(?i)\b(?:lighting|led|strip lighting|cabinetry and kicks|recessed kick)\b",
            candidate_text,
        ):
            return False
        if current_kind == "handle" and re.search(
            r"(?i)\b(?:moulding|accessory|bin|kickboards?|decorative flutes?|panelling|drawer(?:\s+and\s+door)?\s+profile)\b",
            candidate_text,
        ):
            return False
        if "BENCHTOP" in current_label.upper() and re.search(r"(?i)\b(?:cabinetry colour|kickboards?|frame detail)\b", candidate_text):
            return False
        if re.search(r"(?i)\b(?:SPLASHBACK|UPSTAND)\b", current_label) and re.search(
            r"(?i)\b(?:cabinetry colour|handles?|knob|kickboards?|moulding|decorative flutes?|panelling)\b",
            candidate_text,
        ):
            return False
        if "CABINETRY COLOUR" in current_label.upper() and re.search(r"(?i)\b(?:benchtop|bin|lighting)\b", candidate_text):
            return False
        if current_kind == "handle" and re.search(r"(?i)\b(?:cabinetry colour|benchtop|frame detail)\b", candidate_text):
            return False
        return True

    repaired: list[ImperialFiveColumnRow] = []
    index = 0
    total = len(rows)
    while index < total:
        current = rows[index]
        merged_row: ImperialFiveColumnRow | None = None
        consumed = 1
        max_span = min(5, total - index)
        if current.canonical_label:
            current_area = parsing.normalize_space(current.area_or_item)
            if (
                current_area.count("(") > current_area.count(")")
                or current.row_kind == "handle"
                or current_area.upper() in {"BENCHTOP", "FRAME DETAIL", "BIN"}
            ):
                for span in range(max_span, 1, -1):
                    chunk = rows[index : index + span]
                    if any(candidate.canonical_label for candidate in chunk[1:] if not _is_soft_continuation_row(candidate)):
                        continue
                    if not all(_is_soft_continuation_row(candidate) for candidate in chunk[1:]):
                        continue
                    if not all(_continuation_compatible_with_current(current, candidate) for candidate in chunk[1:]):
                        continue
                    merged_candidate = _merge_chunk(chunk, current=current)
                    if merged_candidate is None:
                        continue
                    merged_row = merged_candidate
                    consumed = span
                    break
        elif parsing.normalize_space(current.area_or_item):
            for span in range(max_span, 1, -1):
                chunk = rows[index : index + span]
                if any(candidate.canonical_label for candidate in chunk[1:]):
                    continue
                if not all(_continuation_compatible_with_current(current, candidate) for candidate in chunk[1:]):
                    continue
                merged_candidate = _merge_chunk(chunk, current=current)
                if merged_candidate is None:
                    continue
                merged_row = merged_candidate
                consumed = span
                break
        if merged_row is not None:
            repaired.append(merged_row)
            index += consumed
            continue
        repaired.append(current)
        index += 1
    return repaired


def _append_imperial_fragment_to_logical_row(
    row: ImperialLogicalRow,
    fragment: str,
    *,
    prefer_supplier: bool = False,
    prefer_note: bool = False,
) -> None:
    cleaned = parsing.normalize_space(fragment)
    if not cleaned:
        return
    preserve_handle_block = bool(
        str(row.row_kind or "").lower() == "handle"
        or re.search(r"(?i)\b(?:handles?|knob)\b", parsing.normalize_space(row.row_label))
    )
    if prefer_supplier:
        if preserve_handle_block:
            existing_text = parsing.normalize_space(" ".join(row.description_parts[-2:]))
            if existing_text and (
                cleaned.upper() in existing_text.upper()
                or existing_text.upper() in cleaned.upper()
            ):
                return
            row.description_parts.append(cleaned)
        else:
            row.supplier_parts.append(cleaned)
        return
    supplier, remainder = _extract_imperial_grid_supplier_fragment(cleaned)
    if supplier and not prefer_note:
        if preserve_handle_block:
            supplier_fragment = supplier if not remainder else f"{supplier} - {parsing.normalize_space(remainder)}"
            row.description_parts.append(parsing.normalize_space(supplier_fragment))
        else:
            row.supplier_parts.append(supplier)
        cleaned = parsing.normalize_space(remainder)
        if not cleaned:
            return
    if prefer_note or _imperial_grid_fragment_is_note(cleaned, row_label=row.row_label):
        if preserve_handle_block:
            existing_text = parsing.normalize_space(" ".join(row.description_parts[-2:]))
            if existing_text and (
                cleaned.upper() in existing_text.upper()
                or existing_text.upper() in cleaned.upper()
            ):
                return
            row.description_parts.append(cleaned)
        else:
            row.note_parts.append(cleaned)
        return
    row.description_parts.append(cleaned)


def _append_imperial_supplier_fragment_to_logical_row(
    row: ImperialLogicalRow,
    fragment: str,
) -> None:
    cleaned = parsing.normalize_space(fragment)
    if not cleaned:
        return
    preserve_handle_block = bool(
        str(row.row_kind or "").lower() == "handle"
        or re.search(r"(?i)\b(?:handles?|knob)\b", parsing.normalize_space(row.row_label))
    )
    supplier, remainder = _extract_imperial_grid_supplier_fragment(cleaned)
    if supplier:
        if preserve_handle_block:
            existing_text = parsing.normalize_space(" ".join(row.description_parts[-2:]))
            normalized_remainder = parsing.normalize_space(remainder)
            if normalized_remainder and existing_text and (
                normalized_remainder.upper() in existing_text.upper()
                or existing_text.upper().endswith(normalized_remainder.upper())
            ):
                row.description_parts.append(supplier)
            else:
                merged_fragment = supplier if not normalized_remainder else f"{supplier} - {normalized_remainder}"
                row.description_parts.append(parsing.normalize_space(merged_fragment))
        else:
            if remainder:
                _append_imperial_fragment_to_logical_row(row, remainder)
            row.supplier_parts.append(supplier)
        return
    if _looks_like_supplier_only_line(cleaned):
        if preserve_handle_block:
            row.description_parts.append(cleaned)
        else:
            row.supplier_parts.append(cleaned)
        return
    _append_imperial_fragment_to_logical_row(row, cleaned)


def _should_defer_imperial_cell_row_to_next_logical_row(
    *,
    current: ImperialLogicalRow,
    description_text: str,
    supplier_text: str,
    note_text: str,
    next_label: str,
) -> bool:
    return _should_defer_imperial_word_grid_fragment_to_next_row(
        current={"row_label": current.row_label, "description_parts": list(current.description_parts)},
        description_text=description_text,
        supplier_text=supplier_text,
        note_text=note_text,
        next_label=next_label,
    )


def _build_imperial_logical_rows_from_cells(
    cells: list[ImperialCell],
    *,
    room_scope: str,
    page_no: int = 0,
    separator_model: ImperialSeparatorModel | None = None,
) -> list[ImperialLogicalRow]:
    row_items = _repair_imperial_five_column_rows(
        _build_imperial_five_column_rows_from_cells(
            cells,
            room_scope=room_scope,
            page_no=page_no,
            separator_model=separator_model,
        )
    )
    logical_rows: list[ImperialLogicalRow] = []
    pending_items: list[ImperialFiveColumnRow] = []
    current: ImperialLogicalRow | None = None

    def _next_row_start_label(start_index: int) -> str:
        for candidate in row_items[start_index + 1 :]:
            candidate_label = parsing.normalize_space(candidate.canonical_label)
            if candidate_label:
                return candidate_label
            candidate_area = parsing.normalize_space(candidate.area_or_item)
            if candidate_area and (
                _imperial_five_column_item_starts_row(candidate_area)
                or _imperial_force_raw_area_row_start(
                    candidate_area,
                    candidate.specs_or_description,
                    candidate.supplier,
                    candidate.notes,
                )
            ):
                return candidate_area
        return ""

    def pending_to_current(target: ImperialLogicalRow) -> None:
        nonlocal pending_items
        carry_items: list[ImperialFiveColumnRow] = []
        deferred_items: list[ImperialFiveColumnRow] = []
        for pending in pending_items:
            pending_area = parsing.normalize_space(pending.area_or_item)
            if pending_area and _imperial_five_column_item_starts_row(pending_area):
                deferred_items.append(pending)
                continue
            carry_items.append(pending)
        for pending in carry_items[-3:]:
            pending_prefix = parsing.normalize_space(pending.area_or_item)
            pending_fragment = parsing.normalize_space(
                " ".join(
                    part
                    for part in (
                        "" if pending.canonical_label else pending_prefix,
                        pending.specs_or_description,
                    )
                    if parsing.normalize_space(part)
                )
            )
            _append_imperial_fragment_to_logical_row(target, pending_fragment)
            _append_imperial_supplier_fragment_to_logical_row(target, pending.supplier)
            _append_imperial_fragment_to_logical_row(target, pending.notes, prefer_note=True)
        pending_items = deferred_items

    for index, item in enumerate(row_items):
        next_label = _next_row_start_label(index)
        raw_area_or_item = parsing.normalize_space(item.area_or_item)
        label = item.canonical_label
        description_text = item.specs_or_description
        supplier_text = item.supplier
        note_text = item.notes
        if raw_area_or_item and _imperial_five_column_item_is_label_continuation(raw_area_or_item):
            if current is not None:
                continuation_label, continuation_overflow = _imperial_split_area_or_item_overflow(raw_area_or_item)
                merged_area_or_item = _imperial_merge_area_or_item_parts(
                    current.raw_area_or_item or current.row_label,
                    continuation_label or raw_area_or_item,
                )
                current.raw_area_or_item = merged_area_or_item
                current_label = parsing.normalize_space(current.row_label)
                if current_label and (
                    current_label.upper() in merged_area_or_item.upper()
                    or current_label.upper().startswith(("CABINETRY COLOUR", "VERTICAL UPPER PARTITIONS"))
                ):
                    current.row_label = merged_area_or_item
                if continuation_overflow:
                    _append_imperial_fragment_to_logical_row(current, continuation_overflow)
                _append_imperial_fragment_to_logical_row(current, description_text)
                _append_imperial_supplier_fragment_to_logical_row(current, supplier_text)
                _append_imperial_fragment_to_logical_row(current, note_text, prefer_note=True)
                continue
            pending_items.append(item)
            continue
        if raw_area_or_item and (
            _imperial_five_column_item_starts_row(raw_area_or_item)
            or _imperial_force_raw_area_row_start(raw_area_or_item, description_text, supplier_text, note_text)
        ):
            if current is not None:
                logical_rows.append(current)
            current_label = label or raw_area_or_item
            current_kind = str(item.row_kind or _imperial_row_kind_from_area_or_item(raw_area_or_item) or "other")
            current = ImperialLogicalRow(
                room_scope=room_scope,
                row_label=current_label,
                raw_area_or_item=raw_area_or_item,
                description_parts=[],
                image_parts=[],
                supplier_parts=[],
                note_parts=[],
                row_kind=current_kind,
                page_no=int(page_no or 0),
                row_order=int(_coerce_provenance_dict(item.provenance).get("canonical_row_order", item.row_order or index) or index),
                repair_source="cell_grid_repair",
                position_scope=_imperial_grid_row_position_scope(current_label, current_kind),
                confidence=_normalize_confidence_value(item.confidence),
                provenance=_coerce_provenance_dict(item.provenance),
                needs_review=bool(item.needs_review and not label),
            )
            pending_to_current(current)
            if item.image:
                current.image_parts.append(item.image)
            _append_imperial_fragment_to_logical_row(current, item.remainder)
            _append_imperial_fragment_to_logical_row(current, description_text)
            _append_imperial_supplier_fragment_to_logical_row(current, supplier_text)
            _append_imperial_fragment_to_logical_row(current, note_text, prefer_note=True)
            continue
        if label:
            if current is not None:
                logical_rows.append(current)
            current = ImperialLogicalRow(
                room_scope=room_scope,
                row_label=label,
                raw_area_or_item=raw_area_or_item or label,
                description_parts=[],
                image_parts=[],
                supplier_parts=[],
                note_parts=[],
                row_kind=str(item.row_kind or "other"),
                page_no=int(page_no or 0),
                row_order=int(_coerce_provenance_dict(item.provenance).get("canonical_row_order", item.row_order or index) or index),
                repair_source="cell_grid_repair",
                position_scope=_imperial_grid_row_position_scope(label, str(item.row_kind or "other")),
                confidence=_normalize_confidence_value(item.confidence),
                provenance=_coerce_provenance_dict(item.provenance),
                needs_review=bool(item.needs_review),
            )
            pending_to_current(current)
            if item.image:
                current.image_parts.append(item.image)
            if item.match_source == "combined":
                _append_imperial_fragment_to_logical_row(current, item.remainder)
            else:
                _append_imperial_fragment_to_logical_row(current, item.remainder)
                _append_imperial_fragment_to_logical_row(current, description_text)
            _append_imperial_supplier_fragment_to_logical_row(current, supplier_text)
            _append_imperial_fragment_to_logical_row(current, note_text, prefer_note=True)
            continue
        if current is None:
            pending_items.append(item)
            continue
        if _should_defer_imperial_cell_row_to_next_logical_row(
            current=current,
            description_text=description_text,
            supplier_text=supplier_text,
            note_text=note_text,
            next_label=next_label,
        ):
            pending_items.append(item)
            continue
        if item.image:
            current.image_parts.append(item.image)
        continuation_fragment = parsing.normalize_space(
            " ".join(
                part
                for part in (
                    "" if label else raw_area_or_item,
                    description_text,
                )
                if parsing.normalize_space(part)
            )
        )
        _append_imperial_fragment_to_logical_row(current, continuation_fragment)
        _append_imperial_supplier_fragment_to_logical_row(current, supplier_text)
        _append_imperial_fragment_to_logical_row(current, note_text, prefer_note=True)
    if current is not None:
        logical_rows.append(current)
    return logical_rows


def _imperial_five_column_item_is_label_continuation(area_or_item: str) -> bool:
    cleaned = parsing.normalize_space(area_or_item)
    if not cleaned:
        return False
    if cleaned.startswith("+"):
        return True
    if cleaned.startswith("(") and cleaned.endswith(")"):
        return True
    upper = cleaned.upper()
    if upper in {
        "SHELVING",
        "SHELVING & TALL CABINETRY",
        "AND SLIDING DOORS",
        "INCLUDING",
        "OPEN SHELVING",
        "TALL OPEN SHELVING",
        "BACK AND SIDES",
        "OF ISLAND PANELLING",
        "GLASS DOORS ONLY",
        "DOORS ONLY",
        "DOORS ONLY)",
        "DOOR FRAMES AND",
        "FRAMES AND KICKBOARDS",
        "DRAWERS AND OPEN SHELVING)",
        "GPO'S ISLAND",
        "DRAWER",
        "DOORS",
        "DRAWERS",
        "INTERNAL STEEL SUPPORTS)",
        "BACK DOOR",
        "DRAWINGS)",
        "LIGHTING ALLOW",
        "LIGHTING 3X",
        "LED LIGHTING LED",
        "RAIL PROVISION",
        "HANGING RAIL OVAL",
    }:
        return True
    if re.fullmatch(r"(?i)CABINET X \d+ DOORS?", cleaned):
        return True
    if re.fullmatch(r"(?i)(?:DOORS|DRAWERS)-\d+MM", cleaned):
        return True
    if re.match(r"(?i)^CABINETRY\s+COLOUR\b", cleaned):
        return True
    if re.match(
        r"(?i)^(?:INCLUDING|OPEN\s+SHELVING|TALL\s+OPEN\s+SHELVING|OF\s+ISLAND\s+PANELLING|GLASS\s+DOORS\s+ONLY)\)?$",
        cleaned,
    ):
        return True
    return False


def _imperial_five_column_item_starts_row(area_or_item: str) -> bool:
    cleaned = parsing.normalize_space(area_or_item)
    if not cleaned or _imperial_five_column_item_is_label_continuation(cleaned):
        return False
    upper = cleaned.upper()
    if upper in {"AREA / ITEM", "COLOUR", "HIGH", "TALL", "LAUNDRY", "DESK", "PROFILE"}:
        return False
    if any(
        marker in upper
        for marker in (
            "CLIENT NAME",
            "SIGNATURE",
            "SIGNED DATE",
            "DOCUMENT REF",
            "DESIGNER",
            "REF. NUMBER",
            "SIGNER",
            "EMAIL",
            "LINK SHARED VIA",
        )
    ):
        return False
    return bool(
        re.search(
            r"(?i)\b(?:benchtop|colour|cabinetry|handles?|knob|kickboards?|splashback|upstand|bin|lighting|rail|accessories|floating shelves?|open shelves?|frame|drawers?|doors?|gpo|internals?|shelving|bookshelf|partitions|ironing\s+board|trouser\s+rack|hanging\s+rail|robe\s+internals?|moulding|decorative flutes?|panelling|profile|drawer handle position|door(?:\s+and\s+drawer)?\s+profile|glass doors only|bar back|back and sides(?: of island)?)\b",
            cleaned,
        )
    )


def _imperial_visual_subrows_from_fragments(fragments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not fragments:
        return []
    grouped: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for index, fragment in enumerate(fragments):
        if not isinstance(fragment, dict):
            continue
        separator_kind = parsing.normalize_space(
            str(
                fragment.get("separator_kind_from_previous", "")
                or fragment.get("separator_confidence_from_previous", "")
                or ""
            )
        ).lower()
        start_new = (
            current is None
            or index == 0
            or separator_kind in {"visible", "inferred_high"}
        )
        if start_new:
            current = {
                "separator_kind_from_previous": separator_kind or ("visible" if index == 0 else "none"),
                "top": float(fragment.get("top", 0.0) or 0.0),
                "bottom": float(fragment.get("bottom", 0.0) or 0.0),
                "area_or_item": parsing.normalize_space(str(fragment.get("area_or_item", "") or "")),
                "specs_or_description": parsing.normalize_space(str(fragment.get("specs_or_description", "") or "")),
                "supplier": parsing.normalize_space(str(fragment.get("supplier", "") or "")),
                "notes": parsing.normalize_space(str(fragment.get("notes", "") or "")),
            }
            grouped.append(current)
            continue
        for key in ("area_or_item", "specs_or_description", "supplier", "notes"):
            existing = parsing.normalize_space(str(current.get(key, "") or ""))
            incoming = parsing.normalize_space(str(fragment.get(key, "") or ""))
            if not incoming:
                continue
            if not existing:
                current[key] = incoming
            elif incoming.upper() not in existing.upper():
                current[key] = parsing.normalize_space(f"{existing} | {incoming}")
        current["bottom"] = max(
            float(current.get("bottom", 0.0) or 0.0),
            float(fragment.get("bottom", 0.0) or 0.0),
        )
    return grouped


def _imperial_row_kind_from_area_or_item(area_or_item: str) -> str:
    cleaned = parsing.normalize_space(area_or_item)
    upper = cleaned.upper()
    if not cleaned:
        return "other"
    if re.search(r"(?i)\b(?:handles?|knob)\b", cleaned):
        return "handle"
    if re.search(r"(?i)\b(?:bin|lighting|gpo|accessories|rail)\b", cleaned):
        return "accessory"
    if any(token in upper for token in ("SINKWARE", "TAPWARE", "BASIN")):
        return "sink"
    return "material"


def _imperial_merge_area_or_item_parts(current_label: str, continuation: str) -> str:
    base = parsing.normalize_space(current_label)
    raw_suffix = parsing.normalize_space(continuation)
    explicit_plus = raw_suffix.startswith("+")
    suffix = raw_suffix.lstrip("+").strip()
    if not base:
        return suffix
    if not suffix:
        return base
    base_upper = base.upper()
    suffix_upper = suffix.upper()
    if suffix_upper in base_upper:
        return base
    if base_upper in suffix_upper:
        return suffix
    if not explicit_plus and suffix_upper.startswith("AND ") and "COLOUR" in base_upper:
        colour_index = base_upper.rfind("COLOUR")
        if colour_index >= 0:
            base = parsing.normalize_space(base[: colour_index + len("COLOUR")])
    base_words = base.split()
    suffix_words = suffix.split()
    base_keys = [re.sub(r"[^A-Za-z0-9']+", "", word).upper() for word in base_words]
    suffix_keys = [re.sub(r"[^A-Za-z0-9']+", "", word).upper() for word in suffix_words]
    limit = min(len(base_words), len(suffix_words))
    for span in range(limit, 0, -1):
        if base_keys[-span:] == suffix_keys[:span]:
            remainder = " ".join(suffix_words[span:])
            return parsing.normalize_space(f"{base} {remainder}" if remainder else base)
        if suffix_keys[-span:] == base_keys[:span]:
            remainder = " ".join(base_words[span:])
            return parsing.normalize_space(f"{suffix} {remainder}" if remainder else suffix)
    connector = " + " if explicit_plus else " "
    return parsing.normalize_space(f"{base}{connector}{suffix}")


def _extract_imperial_joinery_word_grid_rows(
    page_words: tuple[tuple[float, float, str], ...],
    *,
    room_scope: str,
    page_no: int = 0,
    separator_model: ImperialSeparatorModel | None = None,
) -> tuple[list[ImperialGridRow], list[dict[str, Any]]]:
    if not page_words:
        return [], []
    row_bands = _group_imperial_word_row_bands(page_words)
    _, table_header_index, content_rows, _ = _split_imperial_word_page_regions(row_bands)
    if not content_rows:
        return [], []
    separator_model = _enrich_imperial_separator_model_for_row_bands(
        separator_model or ImperialSeparatorModel(),
        content_rows,
    )
    column_model = _infer_imperial_column_model(row_bands, table_header_index=table_header_index)
    cells = _extract_imperial_cells_from_word_rows(content_rows, column_model=column_model)
    five_column_rows = _build_imperial_five_column_rows_from_cells(
        cells,
        room_scope=room_scope,
        page_no=page_no,
        separator_model=separator_model,
    )
    logical_rows = _build_imperial_logical_rows_from_cells(
        cells,
        room_scope=room_scope,
        page_no=page_no,
        separator_model=separator_model,
    )
    rows: list[ImperialGridRow] = []
    for logical_row in logical_rows:
        built = logical_row.to_grid_row()
        raw_area_or_item = ""
        if isinstance(built.provenance, dict):
            raw_area_or_item = parsing.normalize_space(str(built.provenance.get("raw_area_or_item", "") or ""))
        if built.needs_review and built.confidence < 0.55 and not _imperial_low_confidence_grid_row_should_keep(built, raw_area_or_item=raw_area_or_item):
            continue
        if built.description or built.notes:
            rows.append(built)
    unresolved_rows = [
        {
            "room_scope": row.room_scope,
            "row_order": row.row_order,
            "area_or_item": row.area_or_item,
            "specs_or_description": row.specs_or_description,
            "image": row.image,
            "supplier": row.supplier,
            "notes": row.notes,
            "confidence": row.confidence,
            "provenance": _coerce_provenance_dict(row.provenance),
        }
        for row in five_column_rows
        if row.needs_review
    ]
    return rows, unresolved_rows


def _imperial_low_confidence_grid_row_should_keep(row: ImperialGridRow, *, raw_area_or_item: str) -> bool:
    raw_label = parsing.normalize_space(raw_area_or_item or row.row_label)
    if not raw_label:
        return False
    if not (
        _imperial_five_column_item_starts_row(raw_label)
        or _imperial_force_raw_area_row_start(raw_label, row.description, row.supplier, row.notes)
    ):
        return False
    combined = parsing.normalize_space(" ".join(part for part in (row.supplier, row.description, row.notes) if parsing.normalize_space(part)))
    if not combined:
        return False
    if any(
        marker in combined.upper()
        for marker in (
            "CLIENT NAME",
            "SIGNATURE",
            "SIGNED DATE",
            "DOCUMENT REF",
            "DESIGNER",
            "LINK SHARED VIA",
        )
    ):
        return False
    return True


def _should_defer_imperial_word_grid_fragment_to_next_row(
    *,
    current: dict[str, Any],
    description_text: str,
    supplier_text: str,
    note_text: str,
    next_label: str,
) -> bool:
    if not next_label:
        return False
    fragment_text = parsing.normalize_space(" ".join(part for part in (description_text, supplier_text, note_text) if part))
    if not fragment_text:
        return False
    current_label = parsing.normalize_space(str(current.get("row_label", "") or "")).upper()
    next_label_upper = parsing.normalize_space(next_label).upper()
    if current_label.startswith("HANDLES") and next_label_upper.startswith("NO HANDLES"):
        return bool(
            re.search(
                r"(?i)\b(?:recessed\s+f(?:i|in)d?ger\s+space|touch\s+catch|no\s+handles?|fridge|microwave|cooktop|beside|above|below)\b",
                fragment_text,
            )
        )
    if next_label_upper.startswith("HANDLES") and current_label == "KICKBOARDS":
        return bool(
            re.search(r"(?i)\b(?:fingerpull|fing[ee]r\s*pull|bevel edge|recessed finger|touch catch|required\)?|vertical|horizontal|no handles?)\b", fragment_text)
        )
    if current_label == "IRONING BOARD" and next_label_upper == "HANGING RAIL":
        return bool(re.search(r"(?i)\b(?:oval|wardrobe tube|aluminium|matt black)\b", fragment_text))
    if current_label == "HANGING RAIL" and next_label_upper == "TROUSER RACK":
        return bool(re.search(r"(?i)\b(?:storg|trouser rack|anthracite|pull-out)\b", fragment_text))
    if next_label_upper == "BIN" and current_label.startswith("HANDLES"):
        return bool(
            re.search(r"(?i)\b(?:bin|vauth-sagel|envi space|furnware|litre|part no)\b", fragment_text)
        )
    if current_label == "INTERNAL CABINETRY COLOUR" and next_label_upper.endswith("CABINETRY COLOUR"):
        return bool(
            not supplier_text
            and not note_text
            and current.get("description_parts")
            and re.search(
                r"(?i)\b(?:gumnut|natural|colour code|oak|walnut|whiteboard|matt|woodmatt|outback|clay|alabaster|boston|coastal|florentine|blush|porcelain)\b",
                fragment_text,
            )
        )
    if current_label.startswith("HANDLES") and next_label_upper.startswith("LIGHTING"):
        return bool(
            re.search(r"(?i)\b(?:lighting|led|strip lighting|cabinetry and kicks|recessed kick|underside of cabinetry)\b", fragment_text)
        )
    if next_label_upper in {"IRONING BOARD", "TROUSER RACK", "HANGING RAIL", "BOOKSHELF", "BIN"} and (
        "CABINETRY COLOUR" in current_label or current_label.startswith("HANDLES")
    ):
        return bool(
            re.search(
                r"(?i)\b(?:vauth-sagel|iron(?:ing)?|tube|trouser|rack|part number|pull out|mounted inside cabinet|woodmatt|matt black|anthracite)\b",
                fragment_text,
            )
        )
    if next_label_upper.startswith("HANDLES") and (
        "CABINETRY COLOUR" in current_label
        or current_label.startswith("BENCHTOP")
    ):
        return bool(
            re.search(
                r"(?i)\b(?:handles?|tall door handles?|high split handle|voda profile handle|momo|tekform|knob|bevel edge|finger pull|no handles?)\b",
                fragment_text,
            )
        )
    if "BAR BACK" in next_label_upper and "CABINETRY COLOUR" in current_label:
        return bool(
            re.search(
                r"(?i)\b(?:profile\s*panel|calcutta|bar\s+back|panel\b|to\s+bar\s+back\s+only)\b",
                fragment_text,
            )
        )
    if next_label_upper == "FLOATING SHELVES" and (
        "BAR BACK" in current_label or "CABINETRY COLOUR" in current_label
    ):
        return bool(
            re.search(
                r"(?i)\b(?:tasmanian|oak|woodmatt|floating\s+shelves?|51mm\s+thick|steel\s+support|bullnose|square\s+edge)\b",
                fragment_text,
            )
        )
    if "CABINETRY COLOUR" in current_label and "BENCHTOP" in next_label_upper:
        return bool(
            re.search(
                r"(?i)\b(?:upstand|splashback|laminated apron|bullnose edge|colour code|alabaster|outback|clay|natural)\b",
                fragment_text,
            )
        )
    if "BENCHTOP" in next_label_upper and re.search(r"(?i)\b(?:UPSTAND|SPLASHBACK)\b", current_label):
        return bool(
            re.search(
                r"(?i)\b(?:laminated apron|bullnose edge|bullnose|profile)\b",
                fragment_text,
            )
        )
    if supplier_text or note_text:
        if current_label.startswith("BENCHTOP") and next_label_upper.startswith("SPLASHBACK"):
            return bool(
                re.search(
                    r"(?i)\b(?:up to overheads?|same height|cooktop run|all other walls?|as per plans?)\b",
                    fragment_text,
                )
            )
        return False
    if current_label != "SPLASHBACK":
        return False
    if not current.get("description_parts"):
        return False
    current_description = parsing.normalize_space(" ".join(str(part or "") for part in list(current.get("description_parts", []) or [])))
    if not re.search(r"(?i)\btiles?\s+by\b", current_description):
        return False
    if next_label_upper not in {
        "BASE CABINETRY COLOUR",
        "UPPER CABINETRY COLOUR",
        "OVERHEAD CABINETS",
        "OVERHEAD CUPBOARDS",
        "FEATURE COLOUR OVERHEADS",
        "FEATURE ISLAND COLOUR",
    }:
        return False
    if re.search(r"(?i)\btiles?\b|\bgrout\b", fragment_text):
        return False
    return _imperial_grid_fragment_looks_material(fragment_text) or _looks_like_supplier_only_line(fragment_text)


def _build_imperial_grid_row_from_word_entry(entry: dict[str, Any], *, room_scope: str) -> ImperialGridRow | None:
    label = parsing.normalize_space(str(entry.get("row_label", "") or ""))
    if not label:
        return None
    row_kind = str(entry.get("row_kind", "other") or "other")
    supplier = ""
    description_parts: list[str] = []
    note_parts: list[str] = []
    for fragment in list(entry.get("description_parts", []) or []):
        cleaned = parsing.normalize_space(str(fragment or ""))
        if not cleaned:
            continue
        fragment_supplier, remainder = _extract_imperial_grid_supplier_fragment(cleaned)
        if fragment_supplier:
            supplier = supplier or fragment_supplier
        cleaned = parsing.normalize_space(remainder)
        if not cleaned:
            continue
        if _imperial_grid_fragment_is_note(cleaned, row_label=label):
            note_parts.append(cleaned)
            continue
        description_parts.append(cleaned)
    for fragment in list(entry.get("supplier_parts", []) or []):
        cleaned = parsing.normalize_space(str(fragment or ""))
        if not cleaned:
            continue
        supplier = supplier or parsing.normalize_brand_casing_text(cleaned)
    for fragment in list(entry.get("note_parts", []) or []):
        cleaned = parsing.normalize_space(str(fragment or ""))
        if not cleaned:
            continue
        note_parts.append(cleaned)
    description = parsing.normalize_brand_casing_text(
        parsing.normalize_space(" ".join(_dedupe_preserve_order(description_parts)))
    ).strip(" -;,")
    notes = parsing.normalize_brand_casing_text(
        parsing.normalize_space(" ".join(_dedupe_preserve_order(note_parts)))
    ).strip(" -;,")
    if not description and not notes:
        return None
    return ImperialGridRow(
        room_scope=room_scope,
        row_label=label,
        description=description,
        supplier=supplier,
        notes=notes,
        row_kind=row_kind,
        row_order=int(entry.get("row_order", 0) or 0),
        repair_source="word_grid_repair",
        position_scope=_imperial_grid_row_position_scope(label, row_kind),
    )


def _imperial_grid_row_from_layout_row(raw_row: dict[str, Any], *, room_scope: str, row_order: int = 0) -> ImperialGridRow:
    return ImperialGridRow(
        room_scope=parsing.normalize_space(str(raw_row.get("room_scope", "") or room_scope)),
        row_label=parsing.normalize_space(str(raw_row.get("row_label", "") or "")),
        description=parsing.normalize_space(str(raw_row.get("value_region_text", "") or "")),
        image=parsing.normalize_space(str(raw_row.get("image_region_text", "") or "")),
        supplier=parsing.normalize_space(str(raw_row.get("supplier_region_text", "") or "")),
        notes=parsing.normalize_space(str(raw_row.get("notes_region_text", "") or "")),
        row_kind=str(raw_row.get("row_kind", "other") or "other"),
        page_no=int(raw_row.get("page_no", 0) or 0),
        row_order=int(raw_row.get("row_order", row_order) or row_order),
        repair_source=parsing.normalize_space(str(raw_row.get("repair_source", "") or "")),
        position_scope=parsing.normalize_space(str(raw_row.get("position_scope", "") or "")),
        confidence=_normalize_confidence_value(raw_row.get("confidence", 0.0)),
        provenance=_coerce_provenance_dict(raw_row.get("provenance", {})),
        needs_review=bool(raw_row.get("needs_review", False)),
    )


def _extract_imperial_joinery_text_grid_rows(raw_page_text: str, *, room_scope: str) -> list[ImperialGridRow]:
    lines = _imperial_table_repair_content_lines(raw_page_text)
    if not lines:
        return []
    entries: list[dict[str, Any]] = []
    pending_prelude: list[str] = []
    current: dict[str, Any] | None = None
    for line in lines:
        label, remainder, row_kind = _match_imperial_table_repair_row(line)
        if label:
            if current is not None:
                entries.append(current)
            current = {
                "row_label": label,
                "row_kind": row_kind,
                "prelude": list(pending_prelude),
                "tail": remainder,
                "post": [],
            }
            pending_prelude = []
            continue
        if current is None:
            pending_prelude.append(line)
            continue
        current["post"].append(line)
    if current is not None:
        entries.append(current)
    _repair_imperial_table_entry_boundaries(entries)
    rows: list[ImperialGridRow] = []
    for row_order, entry in enumerate(entries):
        built = _build_imperial_grid_row_from_text_entry(entry, room_scope=room_scope, row_order=row_order)
        if built:
            rows.append(built)
    return rows


def _imperial_table_repair_content_lines(raw_page_text: str) -> list[str]:
    lines = [
        parsing.normalize_space(line)
        for line in str(raw_page_text or "").replace("\r", "\n").split("\n")
        if parsing.normalize_space(line)
    ]
    if not lines:
        return []
    start_index = 0
    header_index = next((index for index, line in enumerate(lines) if "AREA / ITEM" in line.upper()), None)
    if header_index is not None:
        start_index = header_index + 1
    filtered: list[str] = []
    for line in lines[start_index:]:
        upper = line.upper()
        if any(marker in upper for marker in ("ALL COLOURS SHOWN", "PRODUCT AVAILABILITY", "DESIGNER:", "CLIENT NAME:", "SIGNATURE:", "SIGNED DATE:", "DOCUMENT REF:", "PAGE ")):
            break
        if _looks_like_layout_metadata_line(line):
            continue
        filtered.append(line)
    return filtered


def _match_imperial_table_repair_row(line: str) -> tuple[str, str, str]:
    text = parsing.normalize_space(line)
    if not text:
        return "", "", ""
    for pattern, canonical_label, row_kind in _IMPERIAL_TABLE_REPAIR_ROW_SPECS:
        match = re.match(pattern, text)
        if not match:
            continue
        remainder = parsing.normalize_space(text[match.end() :]).strip(" -:;,")
        return canonical_label, remainder, row_kind
    return "", "", ""


def _repair_imperial_table_entry_boundaries(entries: list[dict[str, Any]]) -> None:
    for index in range(len(entries) - 1):
        current = entries[index]
        upcoming = entries[index + 1]
        post = list(current.get("post", []))
        current_label = parsing.normalize_space(str(current.get("row_label", "") or ""))
        upcoming_label = parsing.normalize_space(str(upcoming.get("row_label", "") or ""))
        if post and parsing.normalize_space(post[-1]).upper() == "MATCH ABOVE" and str(upcoming.get("row_label", "") or "") == "KICKBOARDS":
            upcoming.setdefault("prelude", []).insert(0, post.pop())
        if current_label in {"NO HANDLES - FEATURE OVERHEADS", "FEATURE OVERHEADS - NO HANDLES"} and upcoming_label == "DESK GROMMETS":
            while post:
                upcoming.setdefault("prelude", []).insert(0, post.pop())
        while post and _looks_like_supplier_only_line(post[-1]):
            upcoming.setdefault("prelude", []).insert(0, post.pop())
        if len(post) >= 2 and _looks_like_supplier_only_line(post[-2]) and _imperial_grid_fragment_looks_material(post[-1]):
            preview = [
                fragment
                for fragment in [*current.get("prelude", []), str(current.get("tail", "") or ""), *post[:-2]]
                if _imperial_grid_fragment_looks_primary(fragment)
            ]
            if (
                preview
                or current_label in {"SPLASHBACK", "TO BE OPEN SHELVES", "NO HANDLES - FEATURE OVERHEADS", "FEATURE OVERHEADS - NO HANDLES"}
            ):
                supplier_line = post[-2]
                material_line = post[-1]
                post = post[:-2]
                upcoming.setdefault("prelude", []).insert(0, material_line)
                upcoming.setdefault("prelude", []).insert(0, supplier_line)
        next_supplier = _imperial_text_entry_supplier_hint(upcoming)
        if post and next_supplier and re.match(rf"(?i)^{re.escape(next_supplier)}(?:\b|[^A-Z])", post[-1]):
            upcoming.setdefault("prelude", []).insert(0, post.pop())
        current["post"] = post


def _imperial_text_entry_supplier_hint(entry: dict[str, Any]) -> str:
    for fragment in [*list(entry.get("prelude", []) or []), str(entry.get("tail", "") or "")]:
        supplier, _ = _extract_imperial_grid_supplier_fragment(fragment)
        if supplier:
            return supplier
    return ""


def _build_imperial_grid_row_from_text_entry(
    entry: dict[str, Any],
    *,
    room_scope: str,
    row_order: int,
) -> ImperialGridRow | None:
    label = parsing.normalize_space(str(entry.get("row_label", "") or ""))
    if not label:
        return None
    row_kind = str(entry.get("row_kind", "other") or "other")
    supplier = ""
    description_parts: list[str] = []
    note_parts: list[str] = []
    fragments = [*list(entry.get("prelude", []) or []), str(entry.get("tail", "") or ""), *list(entry.get("post", []) or [])]
    for raw_fragment in fragments:
        fragment = parsing.normalize_space(str(raw_fragment or ""))
        if not fragment:
            continue
        fragment_supplier, remainder = _extract_imperial_grid_supplier_fragment(fragment)
        if fragment_supplier:
            supplier = supplier or fragment_supplier
        cleaned = parsing.normalize_space(remainder)
        if not cleaned:
            continue
        if _imperial_grid_fragment_is_note(cleaned, row_label=label):
            note_parts.append(cleaned)
            continue
        description_parts.append(cleaned)
    description = parsing.normalize_brand_casing_text(
        parsing.normalize_space(" ".join(_dedupe_preserve_order(description_parts)))
    ).strip(" -;,")
    notes = parsing.normalize_brand_casing_text(
        parsing.normalize_space(" ".join(_dedupe_preserve_order(note_parts)))
    ).strip(" -;,")
    if not description and not notes:
        return None
    return ImperialGridRow(
        room_scope=room_scope,
        row_label=label,
        description=description,
        supplier=supplier,
        notes=notes,
        row_kind=row_kind,
        row_order=row_order,
        repair_source="text_grid_repair",
        position_scope=_imperial_grid_row_position_scope(label, row_kind),
    )


def _merge_imperial_grid_rows(text_rows: list[ImperialGridRow], candidate_rows: list[ImperialGridRow]) -> list[ImperialGridRow]:
    if not candidate_rows:
        return text_rows
    merged: list[ImperialGridRow] = []
    used: set[int] = set()
    for text_row in text_rows:
        text_key = _imperial_grid_row_merge_key(text_row)
        candidate_index = next(
            (
                index
                for index, candidate in enumerate(candidate_rows)
                if index not in used and _imperial_grid_row_merge_key(candidate) == text_key
            ),
            None,
        )
        candidate = candidate_rows[candidate_index] if candidate_index is not None and candidate_index < len(candidate_rows) else None
        if candidate_index is not None:
            used.add(candidate_index)
        merged.append(_prefer_imperial_grid_row(text_row, candidate))
    for candidate_index, candidate in enumerate(candidate_rows):
        if candidate_index in used:
            continue
        normalized_label = _normalized_imperial_grid_label(candidate.row_label)
        candidate_key = _imperial_grid_row_merge_key(candidate)
        if not normalized_label:
            continue
        if any(_imperial_grid_row_merge_key(existing) == candidate_key for existing in merged):
            continue
        if _imperial_grid_row_is_redundant_candidate(candidate, merged):
            continue
        merged.append(candidate)
    merged.sort(key=lambda row: (int(row.row_order or 0), int(row.page_no or 0), row.row_label))
    return merged


def _prefer_imperial_grid_row(text_row: ImperialGridRow, candidate: ImperialGridRow | None) -> ImperialGridRow:
    if candidate is None:
        return text_row
    if _imperial_grid_row_merge_key(candidate) != _imperial_grid_row_merge_key(text_row):
        return text_row
    text_score = _imperial_grid_row_quality(text_row)
    candidate_score = _imperial_grid_row_quality(candidate)
    preferred = text_row
    secondary = candidate
    if candidate_score > text_score + 12:
        preferred = candidate
        secondary = text_row
    elif candidate_score >= text_score and parsing.normalize_space(candidate.description):
        preferred = candidate
        secondary = text_row
    row_label = preferred.row_label or secondary.row_label
    description = parsing.normalize_space(preferred.description or secondary.description)
    supplier = preferred.supplier or secondary.supplier
    notes = _merge_imperial_grid_notes(preferred.notes, secondary.notes)
    return ImperialGridRow(
        room_scope=preferred.room_scope or secondary.room_scope,
        row_label=row_label,
        description=description,
        image=preferred.image or secondary.image,
        supplier=supplier,
        notes=notes,
        row_kind=preferred.row_kind or secondary.row_kind,
        page_no=preferred.page_no or secondary.page_no,
        row_order=preferred.row_order or secondary.row_order,
        repair_source=preferred.repair_source or secondary.repair_source or "vision_table_repair",
        position_scope=preferred.position_scope or secondary.position_scope or _imperial_grid_row_position_scope(row_label, preferred.row_kind or secondary.row_kind),
        confidence=max(_normalize_confidence_value(preferred.confidence), _normalize_confidence_value(secondary.confidence)),
        provenance=_coerce_provenance_dict(preferred.provenance or secondary.provenance or {}),
        needs_review=bool(preferred.needs_review and secondary.needs_review),
    )


def _imperial_grid_row_merge_key(row: ImperialGridRow) -> tuple[str, str]:
    label = _normalized_imperial_grid_label(row.row_label)
    position_scope = parsing.normalize_space(row.position_scope).upper()
    row_kind = parsing.normalize_space(row.row_kind).lower()
    if row_kind == "handle" or "HANDLE" in label or label == "KNOB":
        return label, position_scope
    return label, ""


def _imperial_grid_row_position_scope(row_label: str, row_kind: str) -> str:
    label = _normalized_imperial_grid_label(row_label)
    kind = parsing.normalize_space(row_kind).lower()
    if kind != "handle" and "HANDLE" not in label and label != "KNOB":
        return ""
    if "BASE DRAWERS + TALL PANTRY DOORS" in label:
        return "Base Drawers + Tall Pantry Doors"
    if "TALL + PANTRY DRAWERS" in label:
        return "Tall + Pantry Drawers"
    if "TALL CABS / PANTRY CABS ONLY" in label:
        return "Tall + Pantry Cabs"
    if "TALL CABS" in label:
        return "Tall Cabinets"
    if "BASE DOORS" in label:
        return "Base Doors"
    if "BASE DRAWERS" in label:
        return "Base Drawers"
    if "BASE CABS + DRAWERS" in label:
        return "Base Cabinets + Drawers"
    if "BASE CABS" in label:
        return "Base Cabinets"
    if "DRAWERS" in label:
        return "Drawers"
    if "OVERHEAD" in label:
        return "Overheads"
    if label == "CUSTOM HANDLES":
        return "Custom Handles"
    if label == "KNOB":
        return "Knob"
    return ""


def _normalized_imperial_grid_label(label: str) -> str:
    return parsing.normalize_space(label).upper()


def _imperial_grid_row_is_redundant_candidate(candidate: ImperialGridRow, merged_rows: list[ImperialGridRow]) -> bool:
    candidate_label = _normalized_imperial_grid_label(candidate.row_label)
    candidate_kind = parsing.normalize_space(candidate.row_kind).lower()
    if candidate_kind == "handle" and "CABINETRY COLOUR" in candidate_label:
        return True
    if candidate_kind == "material" and candidate_label in {"HANDLES", "KNOB"}:
        return True
    if candidate_label.startswith("KNOB "):
        return any(
            _normalized_imperial_grid_label(existing.row_label) == "KNOB"
            or _normalized_imperial_grid_label(existing.row_label).startswith("HANDLES")
            for existing in merged_rows
        )
    if "TO BE OPEN SHELVES" in candidate_label and any(
        _normalized_imperial_grid_label(existing.row_label) in {"OVERHEAD CABINETS", "OPEN SHELVES"}
        for existing in merged_rows
    ):
        return True
    if candidate_label == "HANDLES":
        return any(
            _normalized_imperial_grid_label(existing.row_label).startswith("HANDLES") or _normalized_imperial_grid_label(existing.row_label) == "KNOB"
            for existing in merged_rows
        )
    return False


def _imperial_grid_row_quality(row: ImperialGridRow) -> int:
    label = _normalized_imperial_grid_label(row.row_label)
    field_name = _imperial_grid_row_target_field(label, row.row_kind)
    combined = parsing.normalize_space(" ".join(part for part in (row.description, row.notes) if parsing.normalize_space(part)))
    score = _imperial_field_quality(combined or row.description or row.notes, field_name)
    if row.supplier:
        score += 5
    if row.repair_source == "cell_grid_repair":
        score += 44
    elif row.repair_source == "word_grid_repair":
        score += 32
    elif row.repair_source == "text_grid_repair":
        score += 20
    elif row.repair_source == "vision_table_repair":
        score += 8
    if _imperial_grid_fragment_has_foreign_row_label(combined or row.description, row_label=label):
        score -= 220
    if field_name == "handles" and "NO HANDLES" in label:
        score -= 80
    if field_name == "handles" and re.search(r"(?i)\b(?:waste\s*bin|avantech|bin\b|spice\s*tray|corner\s*unit|drawer\s*gpo|grommet|lighting|led\s*strip|hinges?|runners?)\b", combined):
        score -= 320
    if field_name == "handles" and parsing.normalize_space(row.supplier).lower() in {"polytec", "laminex", "caesarstone", "wk stone", "smartstone"}:
        score -= 180
    if field_name == "toe_kick" and re.search(r"(?i)\b(?:base doors?|overhead doors?|allegra|kethy)\b", combined):
        score -= 220
    if field_name == "toe_kick" and re.search(r"(?i)\b(?:touch catch|horizontal|vertical|waste\s*bin|avantech|drawer|doors?)\b", combined):
        score -= 260
    if field_name.startswith("bench_tops_") and re.search(r"(?i)\b(?:feature colour overheads?|feature tall cabinetry colour|open shelves?)\b", combined):
        score -= 220
    if field_name.startswith("bench_tops_") and re.search(r"(?i)\b(?:profiledoors?|hampton|classic white smooth|quartiera maple|allegra|kethy)\b", combined):
        score -= 220
    if field_name.startswith("door_colours_") and re.search(r"(?i)\b(?:desk grommets?|cable entry covers?|furnware|knob|allegra|kethy)\b", combined):
        score -= 200
    if field_name.startswith("door_colours_") and re.search(r"(?i)\b\d{2,4}\s*mm\b", combined):
        score -= 120
    if field_name == "floating_shelf" and re.search(r"(?i)\b(?:allegra|install|brushed nickel|6368-)\b", combined):
        score -= 220
    if field_name == "floating_shelf" and re.search(r"(?i)\b(?:kethy|handles?|touch catch|no handles?)\b", combined):
        score -= 260
    score += int(float(row.confidence or 0.0) * 40)
    if row.needs_review:
        score -= 120
    return score


def _imperial_grid_row_target_field(row_label: str, row_kind: str) -> str:
    label = _normalized_imperial_grid_label(row_label)
    kind = parsing.normalize_space(row_kind).lower()
    if "ISLAND BENCHTOP" in label:
        return "bench_tops_island"
    if "COOKTOP RUN BENCHTOP" in label or "WALL RUN BENCHTOP" in label:
        return "bench_tops_wall_run"
    if "FEATURE CABINETRY COLOUR" in label or "FEATURE COLOUR" in label or "FEATURE TALL CABINETRY COLOUR" in label:
        return "feature_colour"
    if "BENCHTOP" in label:
        return "bench_tops_wall_run"
    if "SPLASHBACK" in label:
        return "splashback"
    if "FLOATING SHELV" in label or "OPEN SHELV" in label:
        return "floating_shelf"
    if "SHELVING CABINETRY COLOUR" in label:
        return "shelf"
    if "FEATURE COLOUR OVERHEADS" in label or "UPPER CABINETRY COLOUR" in label or "OVERHEAD CABINETS" in label or "OVERHEAD CUPBOARDS" in label or "GLASS INLAY" in label:
        return "door_colours_overheads"
    if "UPPER / FEATURE CABINETRY COLOUR" in label:
        return "door_colours_overheads"
    if "FEATURE ISLAND COLOUR" in label or "ISLAND CABINETRY COLOUR" in label:
        return "door_colours_island"
    if "FEATURE TALL CABINETRY COLOUR" in label or ("TALL" in label and "HANDLE" not in label):
        return "door_colours_tall"
    if "BASE, UPPER & TALL CABINETRY COLOUR" in label or "BASE & TALL CABINETRY COLOUR" in label:
        return "door_colours_base"
    if "BASE CABINETRY COLOUR" in label:
        return "door_colours_base"
    if "BAR BACK" in label:
        return "door_colours_bar_back"
    if "KICKBOARD" in label:
        return "toe_kick"
    if "HANDLE" in label or label == "KNOB" or kind == "handle":
        return "handles"
    return "bench_tops_other"


def _prefer_imperial_grid_fragment(primary: str, secondary: str, *, row_label: str) -> str:
    primary_clean = parsing.normalize_space(primary)
    secondary_clean = parsing.normalize_space(secondary)
    if not primary_clean:
        return secondary_clean
    if not secondary_clean:
        return primary_clean
    if _imperial_grid_fragment_has_foreign_row_label(primary_clean, row_label=row_label) and not _imperial_grid_fragment_has_foreign_row_label(
        secondary_clean,
        row_label=row_label,
    ):
        return secondary_clean
    if len(secondary_clean) > len(primary_clean) and not _imperial_grid_fragment_has_foreign_row_label(secondary_clean, row_label=row_label):
        return secondary_clean
    return primary_clean


def _imperial_grid_fragment_has_foreign_row_label(text: str, *, row_label: str) -> bool:
    upper = parsing.normalize_space(text).upper()
    current = _normalized_imperial_grid_label(row_label)
    if not upper:
        return False
    forbidden_markers = (
        "FEATURE COLOUR OVERHEADS",
        "FEATURE COLOUR BAR BACK + BAR BACK DOOR",
        "FEATURE ISLAND COLOUR",
        "FEATURE TALL CABINETRY COLOUR",
        "BASE CABINETRY COLOUR",
        "UPPER CABINETRY COLOUR",
        "OVERHEAD CABINETS",
        "TO BE OPEN SHELVES",
        "OPEN SHELVES",
        "KICKBOARDS",
        "HANDLES",
        "DESK GROMMETS",
        "NO HANDLES - FEATURE OVERHEADS",
        "FEATURE OVERHEADS - NO HANDLES",
        "SPLASHBACK",
        "BENCHTOP",
    )
    return any(marker in upper and marker != current for marker in forbidden_markers)


def _merge_imperial_grid_notes(primary: str, secondary: str) -> str:
    parts = _dedupe_preserve_order([parsing.normalize_space(primary), parsing.normalize_space(secondary)])
    return parsing.normalize_space(" - ".join(part for part in parts if part))


def _extract_imperial_grid_supplier_fragment(fragment: str) -> tuple[str, str]:
    text = parsing.normalize_space(fragment)
    upper = text.upper()
    for supplier in sorted(_IMPERIAL_GRID_SUPPLIER_HINTS, key=len, reverse=True):
        supplier_upper = supplier.upper()
        if upper == supplier_upper:
            return parsing.normalize_brand_casing_text(supplier), ""
        if upper == f"{supplier_upper} NOTE:":
            return parsing.normalize_brand_casing_text(supplier), "NOTE:"
        if upper.startswith(f"{supplier_upper} "):
            remainder = parsing.normalize_space(text[len(supplier) :]).strip(" -;,")
            return parsing.normalize_brand_casing_text(supplier), remainder
        if upper.endswith(f" {supplier_upper}"):
            remainder = parsing.normalize_space(text[: -len(supplier)]).strip(" -;,")
            return parsing.normalize_brand_casing_text(supplier), remainder
    return "", text


def _imperial_grid_fragment_is_note(text: str, *, row_label: str) -> bool:
    cleaned = parsing.normalize_space(text)
    if not cleaned:
        return False
    upper = cleaned.upper()
    label_upper = parsing.normalize_space(row_label).upper()
    if label_upper == "KICKBOARDS" and "MATCH ABOVE" in upper:
        return False
    if label_upper == "SPLASHBACK" and re.search(r"(?i)\btiles?\s+by\b", cleaned):
        return False
    if label_upper.startswith("HANDLES") or label_upper == "KNOB":
        return bool(
            re.search(r"(?i)\b(?:install(?:ed)?|horizontal|vertical|no handles?|touch catch|recessed finger space|base doors?|overhead doors?|tall pantry doors?)\b", cleaned)
            and not re.search(r"(?i)\b[A-Z0-9./-]*\d[A-Z0-9./-]*\b", cleaned)
        )
    return bool(
        re.search(
            r"(?i)\b(?:um sink|undermount sink|open shelf|installed by|by client|by imperial|supplied by client|std tile|white grout|touch catch|recessed finger space|open above|shadowline|refer to drawings|for allocations|allocations)\b",
            cleaned,
        )
    )


def _imperial_grid_fragment_looks_material(text: str) -> bool:
    cleaned = parsing.normalize_space(text)
    if not cleaned or _looks_like_supplier_only_line(cleaned):
        return False
    return bool(
        re.search(
            r"(?i)\b(?:matt|smooth|ravine|woodmatt|oak|white|grey|black|bluffs|glacier|quartiera|caesarstone|polytec|laminate|stone|door|flat|tile)\b",
            cleaned,
        )
        or re.search(r"(?i)\b[A-Z0-9./-]*\d[A-Z0-9./-]*\b", cleaned)
    )


def _imperial_grid_fragment_looks_primary(text: str) -> bool:
    cleaned = parsing.normalize_space(text)
    if not cleaned or _looks_like_supplier_only_line(cleaned):
        return False
    return not _imperial_grid_fragment_is_note(cleaned, row_label="")


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = parsing.normalize_space(value)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def _canonicalize_imperial_joinery_row(block_label: str, raw_row: dict[str, Any]) -> dict[str, str] | None:
    row = dict(raw_row)
    label = parsing.normalize_space(str(row.get("row_label", "") or ""))
    value = parsing.normalize_space(str(row.get("value_region_text", "") or ""))
    supplier = parsing.normalize_space(str(row.get("supplier_region_text", "") or ""))
    notes = parsing.normalize_space(str(row.get("notes_region_text", "") or ""))
    merged_text = " ".join(part for part in (block_label, label, value, supplier, notes) if part)
    if not merged_text:
        return None
    if any(re.search(pattern, merged_text) for pattern in _IMPERIAL_LAYOUT_NOISE_PATTERNS):
        return None
    canonical_label = _imperial_joinery_canonical_row_label(block_label, label)
    if not canonical_label:
        canonical_label = label
    canonical_label = parsing.normalize_space(canonical_label)
    if not canonical_label or any(re.search(pattern, canonical_label) for pattern in _IMPERIAL_LAYOUT_NOISE_PATTERNS):
        return None
    if canonical_label == "BENCHTOP" and supplier and supplier.upper() == "POLYTEC" and re.search(
        r"(?i)\b(?:stone|caesarstone|georgian bluffs|dreamwave|calacatta)\b",
        f"{value} {notes}",
    ):
        supplier = ""
    if canonical_label == "SPLASHBACK":
        value = re.sub(r"(?i)\b(?:handles?.*|recessed finger.*|touch catch.*)\b", "", value).strip(" -;,")
        notes = re.sub(r"(?i)\b(?:handles?.*|recessed finger.*|touch catch.*)\b", "", notes).strip(" -;,")
    return {
        "row_label": canonical_label,
        "value_region_text": value,
        "image_region_text": parsing.normalize_space(str(row.get("image_region_text", "") or "")),
        "supplier_region_text": supplier,
        "notes_region_text": notes,
        "row_kind": str(row.get("row_kind", "other") or "other"),
        "page_no": int(row.get("page_no", 0) or 0),
        "row_order": int(row.get("row_order", 0) or 0),
        "repair_source": parsing.normalize_space(str(row.get("repair_source", "") or "")),
        "position_scope": parsing.normalize_space(str(row.get("position_scope", "") or "")),
        "confidence": _normalize_confidence_value(row.get("confidence", 0.0)),
        "provenance": _coerce_provenance_dict(row.get("provenance", {})),
        "needs_review": bool(row.get("needs_review", False)),
    }


def _imperial_joinery_canonical_row_label(block_label: str, row_label: str) -> str:
    block = parsing.normalize_space(block_label)
    label = parsing.normalize_space(row_label)
    block_upper = block.upper()
    label_upper = label.upper()
    if label_upper in {"MATERIAL DESCRIPTION", "DESCRIPTION", "SPECS / DESCRIPTION", "ITEM", "AREA / ITEM"}:
        label = ""
        label_upper = ""
    if "HANDLES" in label_upper:
        if block_upper == "PANTRY":
            return "PANTRY DOOR HANDLES"
        if block_upper in {"OVERHEAD", "OVERHEADS"}:
            return "HANDLES TO OVERHEADS"
        if block_upper == "UNDER BENCH DOORS":
            return "HANDLES TO UNDER BENCH DOORS"
        return "HANDLES"
    if label_upper == "SPLASHBACK" or block_upper == "SPLASHBACK":
        return "SPLASHBACK"
    for pattern, replacement in _IMPERIAL_JOINERY_FIELD_ROOM_MAP:
        if pattern.search(block):
            return replacement
    if label and label == block and any(pattern.search(label) for pattern, _ in _IMPERIAL_JOINERY_FIELD_ROOM_MAP):
        for pattern, replacement in _IMPERIAL_JOINERY_FIELD_ROOM_MAP:
            if pattern.search(label):
                return replacement
    return label


def _docling_heading_lines(markdown: str) -> list[str]:
    results: list[str] = []
    for line in str(markdown or "").splitlines():
        match = re.match(r"^\s{0,3}#{1,6}\s+(.*)$", line)
        if not match:
            continue
        heading = parsing.normalize_space(html.unescape(match.group(1)))
        if heading and heading not in results:
            results.append(heading)
    return results


def _docling_section_label_from_headings(headings: list[str]) -> str:
    for heading in headings:
        upper = heading.upper()
        if any(marker in upper for marker in ("JOINERY SELECTION SHEET", "COLOUR SCHEDULE", "SINKWARE & TAPWARE", "APPLIANCES")):
            return heading
    return headings[0] if headings else ""


def _docling_plain_lines(markdown: str) -> list[str]:
    lines: list[str] = []
    in_table = False
    for raw_line in str(markdown or "").splitlines():
        line = parsing.normalize_space(html.unescape(raw_line))
        if not line or line == "<!-- image -->":
            if not raw_line.lstrip().startswith("|"):
                in_table = False
            continue
        if raw_line.lstrip().startswith("|"):
            in_table = True
            continue
        if in_table:
            continue
        if line not in lines:
            lines.append(line)
    return lines


def _docling_markdown_tables(markdown: str) -> list[list[list[str]]]:
    tables: list[list[str]] = []
    current: list[str] = []
    for raw_line in str(markdown or "").splitlines():
        if raw_line.lstrip().startswith("|"):
            current.append(raw_line.rstrip())
            continue
        if current:
            tables.append(current)
            current = []
    if current:
        tables.append(current)

    parsed_tables: list[list[list[str]]] = []
    for raw_table in tables:
        rows: list[list[str]] = []
        for raw_row in raw_table:
            stripped = raw_row.strip()
            if not stripped.startswith("|"):
                continue
            inner = stripped.strip("|")
            if re.fullmatch(r"[\s:\-|]+", inner):
                continue
            cells = [parsing.normalize_space(html.unescape(cell)) for cell in inner.split("|")]
            if any(cells):
                rows.append(cells)
        if rows:
            parsed_tables.append(rows)
    return parsed_tables


def _docling_table_to_rows(table: list[list[str]], page_type: str) -> list[dict[str, str]]:
    if not table:
        return []
    header_map: dict[str, int] = {}
    start_index = 0
    first_row = table[0]
    if any(_docling_is_table_header_cell(cell) for cell in first_row):
        start_index = 1
        for index, cell in enumerate(first_row):
            upper = cell.upper()
            if "AREA / ITEM" in upper or upper == "ITEM":
                header_map["label"] = index
            elif "SPECS / DESCRIPTION" in upper or "DESCRIPTION" in upper or "SELECTION LEVEL" in upper:
                header_map["value"] = index
            elif "SUPPLIER" in upper or "MANUFACTURER" in upper:
                header_map["supplier"] = index
            elif "NOTES" in upper or "COMMENT" in upper:
                header_map["notes"] = index
    rows: list[dict[str, str]] = []
    for cells in table[start_index:]:
        record = _docling_cells_to_row(cells, page_type=page_type, header_map=header_map)
        if record:
            rows.append(record)
    return rows


def _docling_is_table_header_cell(cell: str) -> bool:
    upper = parsing.normalize_space(cell).upper()
    return any(
        token in upper
        for token in ("AREA / ITEM", "SPECS / DESCRIPTION", "SUPPLIER", "NOTES", "SELECTION LEVEL", "MANUFACTURER")
    )


def _docling_cells_to_row(cells: list[str], page_type: str, header_map: dict[str, int]) -> dict[str, str] | None:
    normalized_cells = [parsing.normalize_space(cell) for cell in cells]
    if not any(normalized_cells):
        return None
    label_cell = ""
    if "label" in header_map and header_map["label"] < len(normalized_cells):
        label_cell = normalized_cells[header_map["label"]]
    elif normalized_cells:
        label_cell = normalized_cells[0]
    label, remainder = _match_layout_row_label(label_cell)
    if not label:
        label, remainder = _match_layout_row_label(" ".join(cell for cell in normalized_cells[:2] if cell))
    if not label:
        candidate = parsing.normalize_space(label_cell)
        if candidate and not _looks_like_layout_metadata_line(candidate):
            label = candidate
    if not label:
        return None
    if label.upper() in {"AREA / ITEM", "ITEM", "SUPPLIER", "NOTES"}:
        return None

    value = ""
    supplier = ""
    notes = ""
    if header_map:
        if "value" in header_map and header_map["value"] < len(normalized_cells):
            value = normalized_cells[header_map["value"]]
        if "supplier" in header_map and header_map["supplier"] < len(normalized_cells):
            supplier = normalized_cells[header_map["supplier"]]
        if "notes" in header_map and header_map["notes"] < len(normalized_cells):
            notes = normalized_cells[header_map["notes"]]
    else:
        remaining = [cell for cell in normalized_cells[1:] if cell]
        if remaining:
            value = remaining[0]
        if len(remaining) == 2:
            if _docling_looks_like_note_cell(remaining[1]):
                notes = remaining[1]
            else:
                supplier = remaining[1]
        elif len(remaining) >= 3:
            value = " ".join(part for part in remaining[:-2] if part).strip() or remaining[0]
            supplier = remaining[-2]
            notes = remaining[-1]
    if remainder:
        value = parsing.normalize_space(f"{remainder} {value}")
    if not value and len(normalized_cells) > 1:
        value = parsing.normalize_space(" ".join(cell for cell in normalized_cells[1:] if cell))
    if not value and _looks_like_layout_metadata_line(label):
        return None
    return {
        "row_label": label,
        "value_region_text": value,
        "supplier_region_text": supplier,
        "notes_region_text": notes,
        "row_kind": _infer_layout_row_kind(label, page_type, value),
    }


def _docling_looks_like_note_cell(text: str) -> bool:
    upper = parsing.normalize_space(text).upper()
    return any(
        token in upper
        for token in (
            "INSTALLED",
            "SUPPLIED BY CLIENT",
            "HORIZONTAL",
            "VERTICAL",
            "OVERHANG",
            "TAPHOLE",
            "LOCATION",
            "MATCH ABOVE",
            "STD",
            "ONLY",
        )
    )


def _docling_sink_tap_room_blocks(markdown: str, builder_name: str) -> list[dict[str, Any]]:
    del builder_name
    lines = _docling_plain_lines(markdown)
    blocks: list[dict[str, Any]] = []
    current_room = ""
    current_label = ""
    current_values: list[str] = []
    for line in lines:
        label, remainder = _match_layout_row_label(line)
        heading_match = re.match(r"(?i)^(SINKWARE|TAPWARE)\s*\(([^)]+)\)\s*(.*)$", line)
        if heading_match:
            if current_room and current_label:
                _append_docling_sink_tap_row(blocks, current_room, current_label, current_values)
            current_room = parsing.source_room_label(heading_match.group(2), fallback_key=parsing.source_room_key(heading_match.group(2)))
            current_label = heading_match.group(1).upper()
            current_values = [remainder] if remainder else []
            continue
        if label and current_room and label.upper() in {"SINKWARE", "TAPWARE"}:
            _append_docling_sink_tap_row(blocks, current_room, current_label, current_values)
            current_label = label.upper()
            current_values = [remainder] if remainder else []
            continue
        if current_room and current_label and not _looks_like_layout_metadata_line(line):
            current_values.append(line)
    if current_room and current_label:
        _append_docling_sink_tap_row(blocks, current_room, current_label, current_values)
    return blocks


def _append_docling_sink_tap_row(
    blocks: list[dict[str, Any]],
    room_label: str,
    row_label: str,
    values: list[str],
) -> None:
    block = next((item for item in blocks if item.get("room_label") == room_label), None)
    if block is None:
        block = {"room_label": room_label, "rows": []}
        blocks.append(block)
    value = parsing.normalize_space(" ".join(part for part in values if parsing.normalize_space(part)))
    if not value:
        return
    block["rows"].append(
        {
            "row_label": row_label,
            "value_region_text": value,
            "supplier_region_text": "",
            "notes_region_text": "",
            "row_kind": _infer_layout_row_kind(row_label, "sinkware_tapware", value),
        }
    )


def _layout_is_usable(layout: dict[str, Any], raw_page_text: str = "") -> bool:
    if not isinstance(layout, dict):
        return False
    rows = parsing._page_layout_rows(layout)
    if not rows:
        return False
    page_type = parsing._effective_layout_page_type(
        "",
        parsing.normalize_space(str(layout.get("page_type", "") or "")).lower(),
        raw_page_text,
        layout,
    )
    if page_type == "joinery" and not any(parsing.normalize_space(str(row.get("row_label", "") or "")) for row in rows):
        return False
    if page_type == "sinkware_tapware":
        return any(
            parsing.normalize_space(str(row.get("row_kind", "") or "")).lower() in {"sink", "tap", "basin"}
            for row in rows
        )
    return True


def _apply_docling_layout_meta(layout_meta: dict[str, Any], applied_pages: list[int]) -> None:
    layout_meta["layout_mode"] = "docling"
    layout_meta["layout_provider"] = "docling"
    layout_meta["layout_note"] = (
        f"Structure-first parsing applied to {len(layout_meta['layout_pages'])} page(s); "
        f"Docling corrected {len(_unique_page_numbers(applied_pages))} page(s)."
    )


_MERGE_ROOM_TITLE_NOISE_PATTERNS: tuple[str, ...] = (
    r"(?i)^(?:na|n/?a)\b[\s:./-]*",
    r"(?i)^ref\.?\s*number\b[\s:./-]*",
    r"(?i)^(?:image|notes|supplier|client|date|address|document ref|document reference|designer)\b[\s:./-]*",
)


def _apply_final_layout_pages(
    documents: list[dict[str, object]],
    candidate_pages: list[tuple[int, int]],
    docling_layouts: dict[tuple[int, int], dict[str, Any]],
    vision_layouts: dict[tuple[int, int], dict[str, Any]],
    builder_name: str = "",
) -> tuple[list[int], list[int], list[int]]:
    mixed_pages: list[int] = []
    final_docling_pages: list[int] = []
    final_vision_pages: list[int] = []
    for doc_index, page_index in candidate_pages:
        pages = list(documents[doc_index].get("pages", []))
        if page_index >= len(pages):
            continue
        page = pages[page_index]
        page_no = int(page.get("page_no", 0) or 0)
        raw_page_text = str(page.get("raw_text", page.get("text", "")) or "")
        heuristic_layout = _normalize_page_layout(dict(page.get("page_layout") or {}))
        docling_layout = docling_layouts.get((doc_index, page_index))
        vision_layout = vision_layouts.get((doc_index, page_index))
        final_layout: dict[str, Any] | None = None
        layout_mode = "lightweight"
        if docling_layout and vision_layout:
            if _should_prefer_unmerged_imperial_joinery_layout(
                builder_name,
                raw_page_text,
                heuristic_layout,
                docling_layout,
                vision_layout,
            ):
                if _force_imperial_joinery_vision_layout(builder_name, raw_page_text, vision_layout):
                    final_layout = _normalize_page_layout(vision_layout)
                    layout_mode = "heavy_vision"
                else:
                    final_layout, layout_mode = _select_best_layout_candidate(
                        [
                            ("heavy_vision", vision_layout),
                            ("docling", docling_layout),
                            ("lightweight", heuristic_layout),
                        ],
                        raw_page_text=raw_page_text,
                    )
            else:
                merged_layout = _merge_page_layouts(
                    docling_layout,
                    vision_layout,
                    builder_name=builder_name,
                    raw_page_text=raw_page_text,
                )
                final_layout, layout_mode = _select_best_layout_candidate(
                    [
                        ("mixed", merged_layout),
                        ("docling", docling_layout),
                        ("heavy_vision", vision_layout),
                        ("lightweight", heuristic_layout),
                    ],
                    raw_page_text=raw_page_text,
                )
        elif docling_layout:
            final_layout, layout_mode = _select_best_layout_candidate(
                [
                    ("docling", docling_layout),
                    ("lightweight", heuristic_layout),
                ],
                raw_page_text=raw_page_text,
            )
        elif vision_layout:
            final_layout, layout_mode = _select_best_layout_candidate(
                [
                    ("heavy_vision", vision_layout),
                    ("lightweight", heuristic_layout),
                ],
                raw_page_text=raw_page_text,
            )
        elif _layout_is_usable(heuristic_layout, raw_page_text=raw_page_text):
            final_layout = heuristic_layout

        if not final_layout:
            continue
        if layout_mode == "mixed":
            mixed_pages.append(page_no)
        page["page_layout"] = final_layout
        page["layout_mode"] = layout_mode
        page["docling_applied"] = bool(docling_layout)
        page["vision_applied"] = bool(vision_layout)
        page["text"] = _vision_layout_to_text(final_layout, fallback_text=raw_page_text)
        if docling_layout and layout_mode in {"docling", "mixed"}:
            final_docling_pages.append(page_no)
        if vision_layout and layout_mode in {"heavy_vision", "mixed"}:
            final_vision_pages.append(page_no)
    return _unique_page_numbers(mixed_pages), _unique_page_numbers(final_docling_pages), _unique_page_numbers(final_vision_pages)


def _should_prefer_unmerged_imperial_joinery_layout(
    builder_name: str,
    raw_page_text: str,
    *candidate_layouts: dict[str, Any],
) -> bool:
    if not _is_imperial_builder_name(builder_name):
        return False
    if _looks_like_imperial_joinery_material_page(raw_page_text):
        return True
    return any(_imperial_layout_looks_like_joinery_material(layout, raw_page_text) for layout in candidate_layouts)


def _force_imperial_joinery_vision_layout(
    builder_name: str,
    raw_page_text: str,
    vision_layout: dict[str, Any],
) -> bool:
    if not _is_imperial_builder_name(builder_name):
        return False
    if not _looks_like_imperial_joinery_material_page(raw_page_text):
        return False
    return _imperial_layout_looks_like_joinery_material(vision_layout, raw_page_text)


def _imperial_layout_looks_like_joinery_material(layout: dict[str, Any], raw_page_text: str = "") -> bool:
    normalized = _normalize_page_layout(layout)
    page_type = parsing._effective_layout_page_type(
        "Imperial",
        str(normalized.get("page_type", "") or ""),
        raw_page_text,
        normalized,
    )
    if page_type != "joinery":
        return False
    if _looks_like_imperial_joinery_material_page(raw_page_text):
        return True
    text_bits = [
        parsing.normalize_space(str(normalized.get("section_label", "") or "")),
        parsing.normalize_space(str(normalized.get("room_label", "") or "")),
    ]
    for block in list(normalized.get("room_blocks", []) or []):
        if isinstance(block, dict):
            text_bits.append(parsing.normalize_space(str(block.get("room_label", "") or "")))
    for row in parsing._page_layout_rows(normalized):
        text_bits.append(parsing.normalize_space(str(row.get("row_label", "") or "")))
    joined = " ".join(part for part in text_bits if part).upper()
    return any(
        marker in joined
        for marker in (
            "BENCHTOP",
            "BASE CABINETRY",
            "UPPER CABINETRY",
            "OVERHEAD",
            "KICKBOARD",
            "HANDLES",
            "SPLASHBACK",
            "FLOATING SHELV",
            "BAR BACK",
            "BULKHEAD",
        )
    )


def _prefer_more_complete_layout(
    primary: dict[str, Any],
    secondary: dict[str, Any],
    raw_page_text: str = "",
) -> dict[str, Any]:
    primary_score = _layout_completeness_score(primary, raw_page_text=raw_page_text)
    secondary_score = _layout_completeness_score(secondary, raw_page_text=raw_page_text)
    return primary if primary_score >= secondary_score else secondary


def _select_best_layout_candidate(
    candidates: list[tuple[str, dict[str, Any]]],
    *,
    raw_page_text: str = "",
) -> tuple[dict[str, Any] | None, str]:
    best_layout: dict[str, Any] | None = None
    best_mode = "lightweight"
    best_score: int | None = None
    for mode, layout in candidates:
        normalized = _normalize_page_layout(layout)
        if not _layout_is_usable(normalized, raw_page_text=raw_page_text):
            continue
        score = _layout_completeness_score(normalized, raw_page_text=raw_page_text)
        if best_score is None or score > best_score:
            best_layout = normalized
            best_mode = mode
            best_score = score
    return best_layout, best_mode


def _layout_completeness_score(layout: dict[str, Any], raw_page_text: str = "") -> int:
    normalized_layout = _normalize_page_layout(layout)
    rows = [
        row
        for row in parsing._page_layout_rows(normalized_layout)
        if _layout_row_is_mergeable(row)
    ]
    row_labels = [
        parsing.normalize_space(str(row.get("row_label", "") or "")).upper()
        for row in rows
        if parsing.normalize_space(str(row.get("row_label", "") or ""))
    ]
    room_blocks = normalized_layout.get("room_blocks", []) or []
    plausible_room_blocks = [
        block
        for block in room_blocks
        if _is_plausible_merged_room_label(
            _clean_merged_room_label(str(block.get("room_label", "") or ""), str(normalized_layout.get("section_label", "") or ""))
        )
    ]
    room_bonus = 120 * len(plausible_room_blocks)
    room_label = _clean_merged_room_label(str(normalized_layout.get("room_label", "") or ""), str(normalized_layout.get("section_label", "") or ""))
    title_bonus = 80 if _is_plausible_merged_room_label(room_label) else 0
    page_type = parsing._effective_layout_page_type(
        "",
        parsing.normalize_space(str(normalized_layout.get("page_type", "") or "")).lower(),
        raw_page_text,
        normalized_layout,
    )
    imperial_joinery_probe = _looks_like_imperial_joinery_material_page(raw_page_text)
    if page_type == "unknown" and imperial_joinery_probe:
        page_type = "joinery"
    core_row_count = sum(1 for label in row_labels if _is_joinery_core_row_label(label)) if page_type == "joinery" else 0
    joinery_penalty = 0
    joinery_core_bonus = 0
    field_like_room_penalty = 0
    if page_type == "joinery" and not plausible_room_blocks and not title_bonus:
        if imperial_joinery_probe and core_row_count >= 2:
            joinery_penalty = 0
        else:
            joinery_penalty = 100 if core_row_count >= 2 else 500
    if page_type == "joinery" and room_blocks and not plausible_room_blocks:
        joinery_penalty += 1200 if core_row_count >= 2 else 20000
    elif page_type == "joinery" and room_blocks and len(plausible_room_blocks) < len(room_blocks):
        joinery_penalty += 250 * (len(room_blocks) - len(plausible_room_blocks))
    if page_type == "joinery":
        joinery_core_bonus = 180 * core_row_count
        if imperial_joinery_probe and core_row_count >= 2 and not plausible_room_blocks and not title_bonus:
            joinery_core_bonus += 400
        if core_row_count == 0:
            joinery_penalty += 900
        if room_label and _looks_like_field_like_room_label(room_label):
            field_like_room_penalty += 600
        field_like_room_penalty += 400 * sum(
            1
            for block in room_blocks
            if _looks_like_field_like_room_label(str(block.get("room_label", "") or ""))
        )
    elif imperial_joinery_probe:
        joinery_penalty += 1800
    header_noise_penalty = 0
    noisy_rows = sum(1 for row in rows if _layout_row_has_header_noise(row))
    if noisy_rows:
        header_noise_penalty = noisy_rows * (1500 if page_type == "joinery" else 500)
    if not rows and not raw_page_text.strip():
        return 0
    return room_bonus + title_bonus + joinery_core_bonus + sum(_layout_row_score(row) for row in rows) - joinery_penalty - header_noise_penalty - field_like_room_penalty


def _is_joinery_core_row_label(label: str) -> bool:
    upper = parsing.normalize_space(label).upper()
    if not upper:
        return False
    return any(
        marker in upper
        for marker in (
            "BENCHTOP",
            "BASE CABINETRY",
            "UPPER CABINETRY",
            "OVERHEAD",
            "TALL",
            "PANTRY",
            "KICKBOARD",
            "SPLASHBACK",
            "FLOATING SHELV",
            "BAR BACK",
            "BULKHEAD",
        )
    )


def _layout_row_has_header_noise(row: dict[str, Any]) -> bool:
    text = _row_fragment_text(row).lower()
    if not text:
        return False
    return any(
        token in text
        for token in (
            "all cabinets include soft close",
            "benchtops over maximum length",
            "colour selections framework",
            "item selection level",
            "supplier description design comments",
            "client initials",
            "page ",
            "job number",
            "area / item",
            "specs / description",
            "ceiling height",
            "cabinetry height",
            "floor type & kick refacing required",
            "hinges & drawer runners",
        )
    )


def _merge_page_layouts(
    primary: dict[str, Any],
    secondary: dict[str, Any],
    *,
    builder_name: str = "",
    raw_page_text: str = "",
) -> dict[str, Any]:
    primary_layout = _normalize_page_layout(primary)
    secondary_layout = _normalize_page_layout(secondary)
    page_type = _merge_layout_page_type(primary_layout, secondary_layout, builder_name=builder_name, raw_page_text=raw_page_text)
    section_label = _merge_section_label(
        str(primary_layout.get("section_label", "") or ""),
        str(secondary_layout.get("section_label", "") or ""),
        page_type=page_type,
    )
    room_label = _merge_room_label(
        str(primary_layout.get("room_label", "") or ""),
        str(secondary_layout.get("room_label", "") or ""),
        section_label=section_label,
    )
    primary_blocks = _coerce_layout_blocks_for_merge(primary_layout, section_label=section_label, room_label=room_label)
    secondary_blocks = _coerce_layout_blocks_for_merge(secondary_layout, section_label=section_label, room_label=room_label)
    merged_blocks = _merge_layout_blocks(
        primary_blocks,
        secondary_blocks,
        section_label=section_label,
        room_label=room_label,
        page_type=page_type,
    )
    merged_rows = [row for block in merged_blocks for row in block.get("rows", [])]
    if not merged_blocks:
        merged_rows = _merge_layout_rows(primary_layout.get("rows", []), secondary_layout.get("rows", []), page_type=page_type)
    merged_layout = {
        "page_type": page_type,
        "section_label": section_label,
        "room_label": room_label,
        "room_blocks": merged_blocks,
        "rows": merged_rows,
    }
    return _normalize_page_layout(merged_layout)


def _merge_layout_page_type(
    primary_layout: dict[str, Any],
    secondary_layout: dict[str, Any],
    *,
    builder_name: str = "",
    raw_page_text: str = "",
) -> str:
    primary_type = str(primary_layout.get("page_type", "unknown") or "unknown")
    secondary_type = str(secondary_layout.get("page_type", "unknown") or "unknown")
    if primary_type == secondary_type:
        return primary_type
    if primary_type == "unknown":
        return secondary_type
    if secondary_type == "unknown":
        return primary_type
    heuristic_type = _infer_page_type_from_text(builder_name, "spec", raw_page_text)
    if heuristic_type in {primary_type, secondary_type}:
        return heuristic_type
    priority = {
        "sinkware_tapware": 4,
        "appliance": 3,
        "joinery": 2,
        "special": 1,
        "unknown": 0,
    }
    return primary_type if priority.get(primary_type, 0) >= priority.get(secondary_type, 0) else secondary_type


def _merge_section_label(primary_label: str, secondary_label: str, *, page_type: str = "") -> str:
    candidates = [_clean_merged_section_label(primary_label), _clean_merged_section_label(secondary_label)]
    candidates = [candidate for candidate in candidates if candidate]
    if not candidates:
        return ""
    if len(candidates) == 1 or candidates[0] == candidates[1]:
        return candidates[0]
    ranked = sorted(candidates, key=lambda value: _layout_section_score(value, page_type=page_type), reverse=True)
    return ranked[0]


def _clean_merged_section_label(label: str) -> str:
    return parsing.normalize_space(str(label or ""))


def _layout_section_score(label: str, *, page_type: str = "") -> int:
    cleaned = _clean_merged_section_label(label)
    if not cleaned:
        return 0
    upper = cleaned.upper()
    score = len(cleaned)
    if "JOINERY SELECTION SHEET" in upper:
        score += 120
    if "COLOUR SCHEDULE" in upper:
        score += 100
    if "SINKWARE" in upper or "TAPWARE" in upper:
        score += 90
    if "APPLIANCE" in upper:
        score += 80
    if page_type == "joinery" and "JOINERY" in upper:
        score += 30
    return score


def _merge_room_label(primary_label: str, secondary_label: str, *, section_label: str = "") -> str:
    candidates = [
        _clean_merged_room_label(primary_label, section_label),
        _clean_merged_room_label(secondary_label, section_label),
    ]
    candidates = [candidate for candidate in candidates if candidate]
    if not candidates:
        return ""
    if len(candidates) == 1 or candidates[0] == candidates[1]:
        return candidates[0]
    ranked = sorted(candidates, key=_layout_room_title_score, reverse=True)
    return ranked[0]


def _clean_merged_room_label(label: str, section_label: str = "") -> str:
    cleaned = parsing.normalize_space(str(label or ""))
    if not cleaned:
        return ""
    cleaner = getattr(parsing, "_clean_layout_room_label", None)
    if callable(cleaner):
        cleaned = cleaner(cleaned, section_label)
    else:
        cleaned = parsing.source_room_label(cleaned, fallback_key=parsing.normalize_room_key(cleaned))
    for pattern in _MERGE_ROOM_TITLE_NOISE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned).strip(" -:/")
    return parsing.normalize_space(cleaned)


def _layout_room_title_score(label: str) -> int:
    cleaned = _clean_merged_room_label(label)
    if not cleaned:
        return 0
    score = len(cleaned)
    if _is_plausible_merged_room_label(cleaned):
        score += 120
    noise_checker = getattr(parsing, "_looks_like_structured_room_noise", None)
    if callable(noise_checker) and noise_checker(cleaned):
        score -= 120
    if _looks_like_field_like_room_label(cleaned):
        score -= 80
    return score


def _is_plausible_merged_room_label(label: str) -> bool:
    cleaned = _clean_merged_room_label(label)
    if not cleaned:
        return False
    if _looks_like_field_like_room_label(cleaned):
        return False
    checker = getattr(parsing, "_looks_like_plausible_room_label", None)
    if callable(checker):
        return bool(checker(cleaned))
    return bool(cleaned) and not _looks_like_field_like_room_label(cleaned)


def _looks_like_field_like_room_label(text: str) -> bool:
    cleaned = _clean_merged_room_label(text)
    if not cleaned:
        return False
    return bool(
        re.search(
            r"(?i)\b(?:manufacturer|colour|type|model|sink|tapware|notes|supplier|document ref|selection required|base cabinetry|upper cabinetry|overhead|benchtop|kickboard|bulkhead|floating shelves?|bar back|handles?|splashback)\b",
            cleaned,
        )
    )


def _coerce_layout_blocks_for_merge(layout: dict[str, Any], *, section_label: str = "", room_label: str = "") -> list[dict[str, Any]]:
    blocks = []
    raw_blocks = layout.get("room_blocks", []) or []
    default_room_label = _merge_room_label(str(layout.get("room_label", "") or ""), room_label, section_label=section_label) or room_label or section_label
    for raw_block in raw_blocks:
        if not isinstance(raw_block, dict):
            continue
        rows = _merge_layout_rows(raw_block.get("rows", []), [], page_type=str(layout.get("page_type", "") or ""))
        if not rows:
            continue
        block_label = _merge_room_label(str(raw_block.get("room_label", "") or ""), default_room_label, section_label=section_label) or default_room_label
        if block_label:
            blocks.append({"room_label": block_label, "rows": rows})
    if blocks:
        return blocks
    top_rows = _merge_layout_rows(layout.get("rows", []), [], page_type=str(layout.get("page_type", "") or ""))
    if top_rows and default_room_label:
        return [{"room_label": default_room_label, "rows": top_rows}]
    return []


def _layout_room_identity(label: str) -> str:
    cleaned = _clean_merged_room_label(label)
    if not cleaned:
        return ""
    return parsing.source_room_key(cleaned, fallback_key=parsing.normalize_room_key(cleaned))


def _merge_layout_blocks(
    primary_blocks: list[dict[str, Any]],
    secondary_blocks: list[dict[str, Any]],
    *,
    section_label: str = "",
    room_label: str = "",
    page_type: str = "",
) -> list[dict[str, Any]]:
    merged_blocks: list[dict[str, Any]] = []
    used_secondary: set[int] = set()
    for primary_block in primary_blocks:
        match_index = _find_best_layout_block_match(primary_block, secondary_blocks, used_secondary)
        if match_index is None:
            if primary_block.get("rows"):
                merged_blocks.append(primary_block)
            continue
        used_secondary.add(match_index)
        secondary_block = secondary_blocks[match_index]
        merged_label = _merge_room_label(
            str(primary_block.get("room_label", "") or ""),
            str(secondary_block.get("room_label", "") or ""),
            section_label=section_label,
        ) or room_label
        merged_rows = _merge_layout_rows(primary_block.get("rows", []), secondary_block.get("rows", []), page_type=page_type)
        if merged_label and merged_rows:
            merged_blocks.append({"room_label": merged_label, "rows": merged_rows})
    for index, secondary_block in enumerate(secondary_blocks):
        if index in used_secondary:
            continue
        if secondary_block.get("room_label") and secondary_block.get("rows"):
            merged_blocks.append(secondary_block)
    return merged_blocks


def _find_best_layout_block_match(
    primary_block: dict[str, Any],
    secondary_blocks: list[dict[str, Any]],
    used_secondary: set[int],
) -> int | None:
    primary_identity = _layout_room_identity(str(primary_block.get("room_label", "") or ""))
    best_index: int | None = None
    best_score = -1
    for index, secondary_block in enumerate(secondary_blocks):
        if index in used_secondary:
            continue
        secondary_identity = _layout_room_identity(str(secondary_block.get("room_label", "") or ""))
        score = 0
        if primary_identity and secondary_identity and primary_identity == secondary_identity:
            score += 100
        primary_label = _clean_merged_room_label(str(primary_block.get("room_label", "") or ""))
        secondary_label = _clean_merged_room_label(str(secondary_block.get("room_label", "") or ""))
        if primary_label and secondary_label and primary_label == secondary_label:
            score += 80
        if score > best_score:
            best_score = score
            best_index = index
    return best_index if best_score > 0 else None


def _merge_layout_rows(primary_rows: Any, secondary_rows: Any, *, page_type: str = "") -> list[dict[str, str]]:
    left_rows = [row for row in _normalize_layout_rows(primary_rows) if _layout_row_is_mergeable(row)]
    right_rows = [row for row in _normalize_layout_rows(secondary_rows) if _layout_row_is_mergeable(row)]
    if not left_rows:
        return right_rows
    if not right_rows:
        return left_rows
    merged_rows: list[dict[str, str]] = []
    used_secondary: set[int] = set()
    for primary_row in left_rows:
        match_index = _find_best_layout_row_match(primary_row, right_rows, used_secondary)
        if match_index is None:
            merged_rows.append(dict(primary_row))
            continue
        used_secondary.add(match_index)
        merged_rows.append(_merge_single_layout_row(primary_row, right_rows[match_index], page_type=page_type))
    for index, secondary_row in enumerate(right_rows):
        if index not in used_secondary:
            merged_rows.append(dict(secondary_row))
    return [row for row in _normalize_layout_rows(merged_rows) if _layout_row_is_mergeable(row)]


def _layout_row_signature(row: dict[str, str]) -> str:
    return parsing.normalize_space(str(row.get("row_label", "") or "")).casefold()


def _find_best_layout_row_match(
    primary_row: dict[str, str],
    secondary_rows: list[dict[str, str]],
    used_secondary: set[int],
) -> int | None:
    primary_signature = _layout_row_signature(primary_row)
    primary_kind = str(primary_row.get("row_kind", "") or "")
    best_index: int | None = None
    best_score = -1
    for index, secondary_row in enumerate(secondary_rows):
        if index in used_secondary:
            continue
        secondary_signature = _layout_row_signature(secondary_row)
        secondary_kind = str(secondary_row.get("row_kind", "") or "")
        score = 0
        if primary_signature and secondary_signature and primary_signature == secondary_signature:
            score += 100
        elif primary_signature and secondary_signature and (
            primary_signature in secondary_signature or secondary_signature in primary_signature
        ):
            score += 60
        if primary_kind and secondary_kind and primary_kind == secondary_kind:
            score += 20
        if score > best_score:
            best_score = score
            best_index = index
    return best_index if best_score > 0 else None


def _merge_single_layout_row(primary_row: dict[str, str], secondary_row: dict[str, str], *, page_type: str = "") -> dict[str, str]:
    primary_score = _layout_row_score(primary_row)
    secondary_score = _layout_row_score(secondary_row)
    preferred = primary_row if primary_score >= secondary_score else secondary_row
    other = secondary_row if preferred is primary_row else primary_row
    merged = dict(preferred)
    merged["row_label"] = parsing.normalize_space(
        str(preferred.get("row_label", "") or "") or str(other.get("row_label", "") or "")
    )
    if str(merged.get("row_kind", "other") or "other") == "other" and str(other.get("row_kind", "other") or "other") != "other":
        merged["row_kind"] = str(other.get("row_kind", "other") or "other")
    for field in ("value_region_text", "supplier_region_text"):
        preferred_value = parsing.normalize_space(str(preferred.get(field, "") or ""))
        other_value = parsing.normalize_space(str(other.get(field, "") or ""))
        merged[field] = _merge_layout_field(preferred_value, other_value)
    merged["notes_region_text"] = _merge_layout_field(
        parsing.normalize_space(str(preferred.get("notes_region_text", "") or "")),
        parsing.normalize_space(str(other.get("notes_region_text", "") or "")),
        allow_union=True,
    )
    if page_type == "sinkware_tapware" and str(merged.get("row_kind", "other") or "other") == "other":
        merged["row_kind"] = _infer_layout_row_kind(str(merged.get("row_label", "") or ""), page_type, str(merged.get("value_region_text", "") or ""))
    return merged


def _merge_layout_field(primary_value: str, secondary_value: str, *, allow_union: bool = False) -> str:
    left = parsing.normalize_space(primary_value)
    right = parsing.normalize_space(secondary_value)
    if not left:
        return right
    if not right:
        return left
    if left.casefold() == right.casefold():
        return left
    if left in right:
        return right
    if right in left:
        return left
    if allow_union:
        return parsing.normalize_space(f"{left} | {right}")
    return left if len(left) >= len(right) else right


def _layout_row_score(row: dict[str, str]) -> int:
    if not _layout_row_is_mergeable(row):
        return -1000
    label = parsing.normalize_space(str(row.get("row_label", "") or ""))
    value = parsing.normalize_space(str(row.get("value_region_text", "") or ""))
    supplier = parsing.normalize_space(str(row.get("supplier_region_text", "") or ""))
    notes = parsing.normalize_space(str(row.get("notes_region_text", "") or ""))
    if label and not any((value, supplier, notes)):
        return 10
    return (80 if label else 0) + (90 if value else 0) + (45 if supplier else 0) + (30 if notes else 0) + len(value) + len(supplier) + len(notes)


def _layout_row_is_mergeable(row: dict[str, str]) -> bool:
    if not isinstance(row, dict):
        return False
    row_kind = str(row.get("row_kind", "other") or "other").strip().lower()
    if row_kind in {"metadata", "footer"}:
        return False
    label = parsing.normalize_space(str(row.get("row_label", "") or ""))
    value = parsing.normalize_space(str(row.get("value_region_text", "") or ""))
    supplier = parsing.normalize_space(str(row.get("supplier_region_text", "") or ""))
    notes = parsing.normalize_space(str(row.get("notes_region_text", "") or ""))
    if not any((label, value, supplier, notes)):
        return False
    if label and not any((value, supplier, notes)):
        return False
    if label and re.match(r"(?i)^(?:client|date|signature|designer|document ref|image|notes|supplier|ref\.?\s*number)$", label):
        return False
    if label and re.match(
        r"(?i)^(?:area\s*/\s*item|specs\s*/\s*description|ceiling height|cabinetry height|floor type(?:\s*&\s*kick refacing required)?|hinges(?:\s*&\s*drawer runners)?|shadowline)$",
        label,
    ):
        return False
    merged_text = " ".join(part for part in (label, value, supplier, notes) if part)
    return not re.search(r"(?i)\b(?:client name|signed date|signature|all colours shown|product availability)\b", merged_text)


def _build_heuristic_page_layout(
    builder_name: str,
    source_kind: str,
    file_name: str,
    page: dict[str, Any],
) -> dict[str, Any]:
    raw_text = str(page.get("raw_text", page.get("text", "")) or "")
    layout_text = _prepare_layout_source_text(builder_name, raw_text, source_kind=source_kind)
    lines = [parsing.normalize_space(line) for line in layout_text.replace("\r", "\n").split("\n") if parsing.normalize_space(line)]
    page_type = _infer_page_type_from_text(builder_name, source_kind, layout_text)
    section_label, room_label = _infer_layout_labels(builder_name, lines, page_type)
    if page_type in {"joinery", "sinkware_tapware"}:
        lowered_builder = builder_name.strip().lower()
        room_blocks: list[dict[str, Any]] = []
        if page_type == "sinkware_tapware" and "simonds" in lowered_builder:
            room_blocks = _heuristic_room_heading_blocks(lines, page_type=page_type, builder_name=builder_name)
            if not room_blocks:
                room_blocks = _build_layout_from_pdf_tables(builder_name, page_type, lines, page)
        else:
            room_blocks = _build_layout_from_pdf_tables(builder_name, page_type, lines, page) if page_type == "sinkware_tapware" else []
        if page_type == "sinkware_tapware" and not room_blocks:
            room_blocks = _build_sink_tap_layout_from_text_blocks(page)
        if not room_blocks:
            room_blocks = _heuristic_room_heading_blocks(lines, page_type=page_type, builder_name=builder_name)
        if page_type == "sinkware_tapware" and not room_blocks:
            room_blocks = _heuristic_sink_tap_room_blocks(lines)
        rows = [row for block in room_blocks for row in block.get("rows", [])]
    else:
        rows = _split_lines_to_layout_rows(lines, page_type=page_type)
        room_blocks = [{"room_label": room_label, "rows": rows}] if (room_label or rows) else []
    return _normalize_page_layout(
        {
            "page_type": page_type,
            "section_label": section_label,
            "room_label": room_label,
            "room_blocks": room_blocks,
            "rows": rows,
            "file_name": file_name,
        }
    )


def _prepare_layout_source_text(builder_name: str, raw_text: str, source_kind: str) -> str:
    text = str(raw_text or "").replace("\r", "\n")
    if source_kind != "spec":
        return text
    lowered_builder = builder_name.strip().lower()
    if "simonds" in lowered_builder:
        return _prepare_simonds_layout_text(text)
    if "evoca" in lowered_builder:
        return _prepare_evoca_layout_text(text)
    return text


def _prepare_evoca_layout_text(text: str) -> str:
    normalized = re.sub(r"(?i)Client Initials .*?$", "", text, flags=re.DOTALL)
    normalized = re.sub(r"(?i)Printed:.*$", "", normalized)
    normalized = re.sub(r"(?i)Report:.*$", "", normalized)
    return re.sub(r"\n{3,}", "\n\n", normalized)


def _page_text_blocks(page: dict[str, Any]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for raw_block in page.get("text_blocks", []) or []:
        if not isinstance(raw_block, dict):
            continue
        try:
            x0 = float(raw_block.get("x0", 0.0) or 0.0)
            y0 = float(raw_block.get("y0", 0.0) or 0.0)
            x1 = float(raw_block.get("x1", 0.0) or 0.0)
            y1 = float(raw_block.get("y1", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        raw_text = str(raw_block.get("text", "") or "").replace("\r", "\n").replace("\x00", " ")
        line_texts = [parsing.normalize_space(line) for line in raw_text.split("\n") if parsing.normalize_space(line)]
        if not line_texts:
            continue
        line_height = (y1 - y0) / max(len(line_texts), 1)
        for index, line_text in enumerate(line_texts):
            if _looks_like_layout_metadata_line(line_text):
                continue
            line_y0 = y0 + (line_height * index)
            line_y1 = line_y0 + max(line_height, 1.0)
            blocks.append({"x0": x0, "y0": line_y0, "x1": x1, "y1": line_y1, "text": line_text})
    return sorted(blocks, key=lambda block: (round(float(block["y0"]), 1), round(float(block["x0"]), 1)))


def _normalize_table_cell(value: Any) -> str:
    return parsing.normalize_space(str(value or "").replace("\x00", " "))


def _page_table_rows(page: dict[str, Any]) -> list[list[list[str]]]:
    normalized_tables: list[list[list[str]]] = []
    for table in page.get("table_rows", []) or []:
        if not isinstance(table, list):
            continue
        normalized_rows: list[list[str]] = []
        for row in table:
            if not isinstance(row, list):
                continue
            cleaned = [_normalize_table_cell(cell) for cell in row]
            if any(cleaned):
                normalized_rows.append(cleaned)
        if normalized_rows:
            normalized_tables.append(normalized_rows)
    return normalized_tables


def _looks_like_invalid_room_heading_candidate(text: str) -> bool:
    normalized = parsing.normalize_space(text).strip(" -")
    lowered = normalized.lower()
    if not normalized:
        return True
    if normalized.upper().strip("()[]{} ") in {"N/A", "NA", "#N/A", "NOT APPLICABLE"}:
        return True
    if re.search(r"\b\d+\s*no\b", lowered) and any(token in lowered for token in ("hook", "rail", "holder")):
        return True
    exact_room_aliases = {
        alias.lower()
        for aliases in parsing.ROOM_ALIASES.values()
        for alias in aliases
    }
    canonical_room = parsing.source_room_label(normalized)
    prefix_label, prefix_rest = parsing._extract_room_prefix_parts(normalized)
    if canonical_room and parsing._looks_like_plausible_room_label(canonical_room):
        if normalized.lower() == canonical_room.lower() and normalized.lower() in exact_room_aliases:
            return False
        if prefix_label and not prefix_rest:
            return False
        if prefix_label and prefix_rest and parsing._looks_like_room_field_tail(prefix_rest):
            return True
    if any(token in lowered for token in INVALID_ROOM_HEADING_TOKENS):
        return True
    if re.match(r"^(?:\d+\s+)?[A-Z][A-Z ]+$", normalized) and any(token in lowered for token in ("cabinet", "doors", "sliding", "mirror")):
        return True
    return False


def _text_block_value_threshold(blocks: list[dict[str, Any]], page_width: float) -> float:
    x_positions = sorted({round(float(block["x0"]), 1) for block in blocks if parsing.normalize_space(str(block.get("text", "")))})
    if len(x_positions) >= 2:
        best_gap = 0.0
        threshold = 0.0
        for left, right in zip(x_positions, x_positions[1:]):
            gap = right - left
            if gap > best_gap:
                best_gap = gap
                threshold = left + gap / 2.0
        if best_gap >= 40.0:
            return threshold
    if page_width > 0:
        return page_width * 0.3
    return 160.0


def _room_heading_from_block_text(text: str, *, x0: float = 0.0, heading_max_x: float = 65.0) -> str:
    if x0 > heading_max_x:
        return ""
    if _match_layout_row_label(text)[0]:
        return ""
    cleaned = parsing._clean_layout_room_label(text)
    if _looks_like_invalid_room_heading_candidate(cleaned):
        return ""
    if len(cleaned.split()) > 4 or "," in cleaned:
        return ""
    if cleaned and parsing._looks_like_plausible_room_label(cleaned):
        return parsing.source_room_label(cleaned)
    return ""


def _table_row_explicit_room_label(row: list[str]) -> str:
    def valid_room_label(value: str) -> str:
        candidate = parsing.source_room_label(value)
        normalized = parsing.normalize_space(candidate).upper()
        if (
            normalized in {"N/A", "NA", "#N/A", "NOT APPLICABLE"}
            or "NOT APPLICABLE" in normalized
            or "#N/A" in normalized
        ):
            return ""
        if (
            candidate
            and parsing._looks_like_plausible_room_label(candidate)
            and not _looks_like_invalid_room_heading_candidate(candidate)
        ):
            return candidate
        return ""

    if not row:
        return ""
    nonempty = [(index, cell) for index, cell in enumerate(row) if cell]
    if not nonempty:
        return ""
    if nonempty[0][1].lower() in {"location", "wet area location"} and len(nonempty) >= 2:
        candidate = valid_room_label(nonempty[1][1])
        if candidate:
            return candidate
    if len(nonempty) == 1:
        candidate = valid_room_label(nonempty[0][1])
        if candidate:
            return candidate
    if len(nonempty) == 2 and not nonempty[0][1] and nonempty[1][1]:
        candidate = valid_room_label(nonempty[1][1])
        if candidate:
            return candidate
    if len(nonempty) == 1 and nonempty[0][0] == 1:
        candidate = valid_room_label(nonempty[0][1])
        if candidate:
            return candidate
    return ""


def _table_row_pairwise_rows(row: list[str], page_type: str) -> list[dict[str, Any]]:
    cells = [cell for cell in row if cell and cell != "-"]
    if len(cells) < 2:
        return []
    if len(cells) == 2 and (_match_layout_row_label(cells[0])[0] or cells[0].istitle()):
        return [
            {
                "row_label": cells[0],
                "value_region_text": cells[1],
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": _infer_layout_row_kind(cells[0], page_type, cells[1]),
            }
        ]
    rows: list[dict[str, Any]] = []
    index = 0
    while index < len(cells) - 1:
        label = cells[index]
        value = cells[index + 1]
        if not label:
            break
        rows.append(
            {
                "row_label": label,
                "value_region_text": value,
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": _infer_layout_row_kind(label, page_type, value),
            }
        )
        index += 2
    return rows


def _table_group_label_rows(
    labels_cell: str,
    value_cells: list[str],
    page_type: str,
) -> list[dict[str, Any]]:
    labels = [parsing.normalize_space(line).strip("- ") for line in labels_cell.split("\n") if parsing.normalize_space(line).strip("- ")]
    normalized_labels = [_normalize_generic_row_label(label) for label in labels]
    values: list[str] = []
    for cell in value_cells:
        if not cell:
            continue
        parts = [parsing.normalize_space(line) for line in cell.split("\n") if parsing.normalize_space(line)]
        if parts:
            values.extend(parts)
    # When the first label is a grouped anchor like "Underbench" or "Benchtops",
    # that anchor usually has no direct value cell. Discount it from the skip
    # budget so later property rows (for example Drawers -> Not Included) don't
    # get shifted into the next field.
    leading_generic_anchor = bool(
        page_type != "sinkware_tapware"
        and normalized_labels
        and _looks_like_grouped_generic_anchor_label(normalized_labels[0])
        and any(_looks_like_grouped_generic_follower_label(label) for label in normalized_labels[1:])
    )
    anchor_skip_budget = max(0, len(labels) - len(values) - (1 if leading_generic_anchor else 0))

    def should_skip_value(label: str, index: int) -> bool:
        nonlocal anchor_skip_budget
        normalized = _normalize_generic_row_label(label)
        if index == 0 and leading_generic_anchor:
            return True
        if anchor_skip_budget <= 0:
            return False
        if page_type == "sinkware_tapware":
            return False
        if index != 0 and normalized not in {"benchtops", "bench tops", "drawers"}:
            return False
        if normalized in GENERIC_LAYOUT_ANCHOR_LABELS:
            anchor_skip_budget -= 1
            return True
        return False

    rows: list[dict[str, Any]] = []
    value_index = 0
    shift_accessory_placeholder = bool(
        labels
        and labels[0] in {"Accessories", "Accessories & Toilet Suite"}
        and len(labels) > 1
        and values
        and values[0].lower() in {"not applicable", "#n/a"}
    )
    for index, label in enumerate(labels):
        normalized_label = label
        if should_skip_value(normalized_label, index):
            value_text = ""
        elif shift_accessory_placeholder and normalized_label in {"Accessories", "Accessories & Toilet Suite"}:
            value_text = ""
        elif shift_accessory_placeholder and normalized_label not in {"Accessories", "Accessories & Toilet Suite"} and value_index == 0:
            value_text = values[value_index]
            value_index += 1
        elif normalized_label in {"Sink", "Basin", "Bath", "Shower", "Accessories", "Accessories & Toilet Suite"} and value_index < len(values) and values[value_index].lower() in {"not applicable", "#n/a"}:
            value_text = values[value_index]
            value_index += 1
        elif normalized_label in {"Sink", "Basin", "Bath", "Shower", "Sink Mixer", "Basin Mixer", "Bath Mixer / Spout"}:
            value_text = ""
        else:
            value_text = values[value_index] if value_index < len(values) else ""
            if value_index < len(values):
                value_index += 1
        rows.append(
            {
                "row_label": normalized_label,
                "value_region_text": value_text,
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": _infer_layout_row_kind(normalized_label, page_type, value_text),
            }
        )
    return rows


def _table_looks_like_sink_tap_table(table: list[list[str]]) -> bool:
    explicit_room_found = False
    wet_area_label_found = False
    for row in table[:40]:
        room_label = _table_row_explicit_room_label(row)
        if room_label:
            explicit_room_found = True
        for cell in row:
            label, _ = _match_layout_row_label(cell)
            normalized = parsing.normalize_space(cell)
            if label in {
                "Sink",
                "Sink Mixer",
                "Tub",
                "Tub Mixer",
                "Basin",
                "Basin Mixer",
                "Bath",
                "Bath Mixer / Spout",
                "Shower",
                "Shower Mixer",
                "Shower Rose",
                "Shower Rail / Rose",
                "Washing Machine Taps",
                "Accessories & Toilet Suite",
                "Accessories",
                "Toilet Suite",
                "Floor Waste",
            }:
                wet_area_label_found = True
            if normalized.upper().startswith("SINKWARE") or normalized.upper().startswith("TAPWARE"):
                wet_area_label_found = True
        if explicit_room_found and wet_area_label_found:
            return True
    return explicit_room_found and wet_area_label_found


def _build_layout_from_pdf_tables(
    builder_name: str,
    page_type: str,
    lines: list[str],
    page: dict[str, Any],
) -> list[dict[str, Any]]:
    tables = _page_table_rows(page)
    if not tables:
        return []
    fallback_room_blocks = _heuristic_room_heading_blocks(lines, page_type=page_type, builder_name=builder_name)
    fallback_labels = [str(block.get("room_label", "") or "") for block in fallback_room_blocks if str(block.get("room_label", "") or "")]
    room_blocks: list[dict[str, Any]] = []
    current_block: dict[str, Any] | None = None
    leading_rows: list[dict[str, Any]] = []
    first_explicit_room = ""

    def ensure_room(room_label: str) -> dict[str, Any]:
        nonlocal current_block
        canonical = parsing.source_room_label(room_label)
        if current_block is not None and current_block.get("room_label") == canonical:
            return current_block
        current_block = {"room_label": canonical, "rows": []}
        room_blocks.append(current_block)
        return current_block

    def flush_leading_rows(target_room_label: str) -> None:
        nonlocal leading_rows
        if not leading_rows:
            return
        block = ensure_room(target_room_label)
        block["rows"].extend(leading_rows)
        leading_rows = []

    for table in tables:
        if page_type == "sinkware_tapware" and not _table_looks_like_sink_tap_table(table):
            continue
        table_room_hint = ""
        explicit_rooms = [room for room in (_table_row_explicit_room_label(row) for row in table[:6]) if room]
        if len(explicit_rooms) == 1:
            table_room_hint = explicit_rooms[0]
        elif len(fallback_labels) == 1:
            table_room_hint = fallback_labels[0]
        if table_room_hint:
            ensure_room(table_room_hint)
        row_index = 0
        while row_index < len(table):
            row = table[row_index]
            if not any(row):
                row_index += 1
                continue
            room_label = _table_row_explicit_room_label(row)
            if room_label:
                if not first_explicit_room:
                    first_explicit_room = room_label
                if leading_rows:
                    if table_room_hint:
                        flush_leading_rows(table_room_hint)
                    elif len(fallback_labels) >= 2:
                        preferred = fallback_labels[0]
                        if parsing.source_room_key(preferred) == parsing.source_room_key(room_label) and len(fallback_labels) > 1:
                            preferred = fallback_labels[1]
                        flush_leading_rows(preferred)
                    elif len(fallback_labels) == 1:
                        flush_leading_rows(fallback_labels[0])
                ensure_room(room_label)
                row_index += 1
                continue
            joined = " ".join(cell for cell in row if cell)
            if _looks_like_major_section_heading(joined):
                row_index += 1
                continue
            if row and len(row) >= 2 and row[1] and "\n" in row[1]:
                label_lines = [parsing.normalize_space(part).strip("- ") for part in row[1].split("\n") if parsing.normalize_space(part).strip("- ")]
                if label_lines and any(_match_layout_row_label(label)[0] or label in EXTRA_LAYOUT_ROW_LABELS for label in label_lines):
                    value_cells = [cell for idx, cell in enumerate(row) if idx != 1 and cell and cell != "-"]
                    look_ahead = row_index + 1
                    while look_ahead < len(table):
                        next_row = table[look_ahead]
                        nonempty = [cell for cell in next_row if cell]
                        if not nonempty:
                            look_ahead += 1
                            continue
                        if _table_row_explicit_room_label(next_row):
                            break
                        if len(nonempty) == 1 and nonempty[0] != "-" and not _looks_like_major_section_heading(nonempty[0]):
                            value_cells.extend(nonempty)
                            look_ahead += 1
                            continue
                        break
                    rows = _table_group_label_rows(row[1], value_cells, page_type)
                    if current_block is None:
                        inferred = table_room_hint or (fallback_labels[0] if len(fallback_labels) == 1 else "")
                        if inferred:
                            ensure_room(inferred)
                    if current_block is not None:
                        current_block["rows"].extend(rows)
                    else:
                        leading_rows.extend(rows)
                    row_index = look_ahead
                    continue
            pair_rows = _table_row_pairwise_rows(row, page_type)
            if pair_rows:
                if current_block is None:
                    inferred = table_room_hint or (fallback_labels[0] if len(fallback_labels) == 1 else "")
                    if inferred:
                        ensure_room(inferred)
                if current_block is not None:
                    current_block["rows"].extend(pair_rows)
                else:
                    leading_rows.extend(pair_rows)
            row_index += 1
        if leading_rows:
            if table_room_hint:
                flush_leading_rows(table_room_hint)
            elif len(fallback_labels) == 1:
                flush_leading_rows(fallback_labels[0])
    return [block for block in room_blocks if block.get("room_label") and block.get("rows")]


def _split_sink_tap_trailing_room(text: str) -> tuple[str, str, str]:
    label, remainder = _match_layout_row_label(text)
    if not label or not remainder:
        return "", "", ""
    normalized_remainder = parsing.normalize_space(remainder)
    room_label = _room_heading_from_block_text(normalized_remainder)
    if room_label and normalized_remainder.lower() == room_label.lower():
        return label, "", room_label
    for alias in (
        "Master Ensuite",
        "Guest Ensuite 2",
        "Ensuite 3",
        "Ensuite 2",
        "Ensuite",
        "Bathroom",
        "Powder",
        "Laundry",
        "Kitchen",
        "Pantry",
        "Butlers/WIP",
        "Butlers",
        "Alfresco",
    ):
        room_label = parsing.source_room_label(alias)
        match = re.match(rf"(?i)^(?P<body>.*?)(?:\s+|/|-)+{re.escape(alias)}$", normalized_remainder)
        if match:
            return label, parsing.normalize_space(match.group("body")), room_label
    return "", "", ""


def _build_sink_tap_layout_from_text_blocks(page: dict[str, Any]) -> list[dict[str, Any]]:
    blocks = _page_text_blocks(page)
    if not blocks:
        return []
    threshold = _text_block_value_threshold(blocks, float(page.get("page_width", 0.0) or 0.0))
    left_blocks = [block for block in blocks if float(block["x0"]) <= threshold]
    right_blocks = [block for block in blocks if float(block["x0"]) > threshold]
    if not left_blocks or not right_blocks:
        return []

    room_blocks: list[dict[str, Any]] = []
    row_refs: list[dict[str, Any]] = []
    current_block: dict[str, Any] | None = None

    def ensure_room(room_label: str) -> dict[str, Any]:
        nonlocal current_block
        if current_block is not None and current_block.get("room_label") == room_label:
            return current_block
        current_block = {"room_label": room_label, "rows": []}
        room_blocks.append(current_block)
        return current_block

    for block in left_blocks:
        text = parsing.normalize_space(str(block.get("text", "") or ""))
        if not text:
            continue
        room_label = _room_heading_from_block_text(text, x0=float(block.get("x0", 0.0) or 0.0), heading_max_x=max(min(threshold * 0.45, 80.0), 55.0))
        if room_label and not _match_layout_row_label(text)[0]:
            ensure_room(room_label)
            continue
        row_label, remainder, trailing_room = _split_sink_tap_trailing_room(text)
        if row_label:
            if current_block is None:
                continue
            row = {
                "row_label": row_label,
                "value_region_text": remainder,
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": _infer_layout_row_kind(row_label, "sinkware_tapware"),
                "_center_y": (float(block["y0"]) + float(block["y1"])) / 2.0,
            }
            current_block["rows"].append(row)
            row_refs.append(row)
            if trailing_room:
                ensure_room(trailing_room)
            continue
        row_label, remainder = _match_layout_row_label(text)
        if not row_label:
            if current_block is not None and current_block.get("rows"):
                last_row = current_block["rows"][-1]
                existing = parsing.normalize_space(str(last_row.get("value_region_text", "") or ""))
                last_row["value_region_text"] = parsing.normalize_space(f"{existing} {text}".strip()) if existing else text
            continue
        if current_block is None:
            continue
        row = {
            "row_label": row_label,
            "value_region_text": remainder,
            "supplier_region_text": "",
            "notes_region_text": "",
            "row_kind": _infer_layout_row_kind(row_label, "sinkware_tapware"),
            "_center_y": (float(block["y0"]) + float(block["y1"])) / 2.0,
        }
        current_block["rows"].append(row)
        row_refs.append(row)

    if not row_refs:
        return []

    for block in right_blocks:
        text = parsing.normalize_space(str(block.get("text", "") or ""))
        if not text:
            continue
        center_y = (float(block["y0"]) + float(block["y1"])) / 2.0
        nearest = min(row_refs, key=lambda row: abs(center_y - float(row.get("_center_y", 0.0))))
        if abs(center_y - float(nearest.get("_center_y", 0.0))) > 18.0:
            continue
        existing = parsing.normalize_space(str(nearest.get("value_region_text", "") or ""))
        nearest["value_region_text"] = parsing.normalize_space(f"{existing} {text}".strip()) if existing else text

    normalized_blocks: list[dict[str, Any]] = []
    for block in room_blocks:
        rows = []
        for row in block.get("rows", []):
            cleaned = dict(row)
            cleaned.pop("_center_y", None)
            rows.append(cleaned)
        if block.get("room_label") and rows:
            normalized_blocks.append({"room_label": block["room_label"], "rows": rows})
    return normalized_blocks


def _flexible_layout_marker_pattern(marker: str) -> str:
    tokens = [re.escape(token) for token in re.split(r"\s+", marker.strip()) if token]
    if not tokens:
        return ""
    return r"\s*".join(tokens)


def _prepare_simonds_layout_text(text: str) -> str:
    normalized = re.sub(r"(?i)Client Initials .*?$", "", text, flags=re.DOTALL)
    normalized = re.sub(r"(?i)Printed:.*$", "", normalized)
    normalized = re.sub(r"(?i)Report:.*$", "", normalized)
    if "Selection Level 1" in normalized:
        normalized = normalized.split("Selection Level 1", 1)[1]
    room_markers = (
        "Master Ensuite",
        "Guest Ensuite 2",
        "Ensuite 3",
        "Bulters/WIP",
        "Butlers/WIP",
        "Kitchen",
        "Pantry",
        "Laundry",
        "Bathroom",
        "Powder",
        "Study",
        "Staircase",
    )
    attached_room_markers = tuple(sorted(room_markers, key=len, reverse=True))
    attached_field_markers = tuple(
        sorted(
            (
                "Wall Run Benchtop",
                "Benchtop",
                "Wall Run Base Cabinet Panels",
                "Base Cabinet Panels",
                "Wall Run Kickboard",
                "Kickboard",
                "Island/Penisula Benchtop",
                "Island/Penisula Base Cabinet Panels",
                "Island/Penisula Kickboard",
                "Island/Penisula Feature Panels",
                "Waterfall End Panels",
                "Tall Panel",
                "Kitchen Sink",
                "Kitchen Tapware",
                "Pantry Sink",
                "Pantry Tapware",
                "Laundry Trough",
                "Laundry Tapware",
                "Vanity Basin Tapware",
                "Vanity Basin",
                "Feature Waste",
                "Cabinetry Handles",
                "Overhead Cabinetry Handles",
                "Cabinet Panels",
                "Shadowline",
                "Range hood",
                "Cooktop",
                "Oven",
                "Toilet Roll Holder",
                "Toilet Suite",
                "Wet Area Location",
                "Manufacturer",
                "Range",
                "Profile",
                "Finish",
                "Colour",
                "Category",
                "Model",
                "Location",
                "Fixing",
                "Style",
                "Mechanism",
                "Underlay",
                "Overheads",
                "Mirror",
                "Shower Base",
                "Shower Frame",
                "Shower Mixer",
                "Shower Rose",
                "Accessories",
                "Robe Hook",
                "Towel Rail",
            ),
            key=len,
            reverse=True,
        )
    )
    for marker in attached_room_markers:
        marker_pattern = _flexible_layout_marker_pattern(marker)
        normalized = re.sub(rf"(?<!\n)(?={marker_pattern}(?=\s*Manufacturer))", "\n", normalized, flags=re.IGNORECASE)
    for marker in attached_field_markers:
        marker_pattern = _flexible_layout_marker_pattern(marker)
        normalized = re.sub(rf"(?<!\n)(?={marker_pattern})", "\n", normalized, flags=re.IGNORECASE)
        normalized = re.sub(rf"(?i)\b({marker_pattern})(?=[A-Z0-9#(])", r"\1 ", normalized)
    split_label_repairs: tuple[tuple[str, str], ...] = (
        (r"(?im)^Wall Run\s*\n+\s*Benchtop\b", "Wall Run Benchtop"),
        (r"(?im)^Wall Run\s*\n+\s*Base Cabinet Panels\b", "Wall Run Base Cabinet Panels"),
        (r"(?im)^Wall Run\s*\n+\s*Base\s*\n+\s*Cabinet Panels\b", "Wall Run Base Cabinet Panels"),
        (r"(?im)^Wall Run Base\s*\n+\s*Cabinet Panels\b", "Wall Run Base Cabinet Panels"),
        (r"(?im)^Base\s*\n+\s*Cabinet Panels\b", "Base Cabinet Panels"),
        (r"(?im)^Island/Penisula\s*\n+\s*Base Cabinet Panels\b", "Island/Penisula Base Cabinet Panels"),
        (r"(?im)^Island/Peninsula\s*\n+\s*Base Cabinet Panels\b", "Island/Penisula Base Cabinet Panels"),
        (r"(?im)^Island/Penisula\s*\n+\s*Feature Panels\b", "Island/Penisula Feature Panels"),
        (r"(?im)^Island/Peninsula\s*\n+\s*Feature Panels\b", "Island/Penisula Feature Panels"),
        (r"(?im)^Island/Penisula\s*\n+\s*Base\s*\n+\s*Cabinet Panels\b", "Island/Penisula Base Cabinet Panels"),
        (r"(?im)^Island/Peninsula\s*\n+\s*Base\s*\n+\s*Cabinet Panels\b", "Island/Penisula Base Cabinet Panels"),
        (r"(?im)^Island/Penisula\s*\n+\s*Kickboard\b", "Island/Penisula Kickboard"),
        (r"(?im)^Island/Peninsula\s*\n+\s*Kickboard\b", "Island/Penisula Kickboard"),
        (r"(?im)^Island/Penisula\s*\n+\s*Benchtop\b", "Island/Penisula Benchtop"),
        (r"(?im)^Island/Peninsula\s*\n+\s*Benchtop\b", "Island/Penisula Benchtop"),
        (r"(?im)^Overhead\s*\n+\s*Cabinetry Handles\b", "Overhead Cabinetry Handles"),
        (r"(?im)^Base\s*\n+\s*Cabinetry Handles\b", "Base Cabinetry Handles"),
        (r"(?im)^Wall Run\s*\n+\s*Kickboard\b", "Wall Run Kickboard"),
    )
    for pattern, replacement in split_label_repairs:
        normalized = re.sub(pattern, replacement, normalized)
    continuation_headings = (
        ("Kitchen", ("Additional Kitchen/Butlers/Kitchenette", "Wall Run Benchtop", "Kitchen Sink", "Kitchen Tapware", "Island/Penisula Benchtop")),
        ("Pantry", ("Pantry Sink", "Pantry Tapware")),
        ("Laundry", ("Laundry Trough", "Laundry Tapware")),
        ("Master Ensuite", ("Master Ensuite",)),
        ("Bathroom", ("Bathroom",)),
        ("Powder", ("Powder",)),
        ("Guest Ensuite 2", ("Guest Ensuite 2", "Additional Bath/Ensuite/Powder", "Wet Area Location")),
    )
    for heading, markers in continuation_headings:
        exact_heading_re = re.compile(rf"(?im)^{re.escape(heading)}\s*$")
        if exact_heading_re.search(normalized):
            continue
        earliest = None
        for marker in markers:
            match = re.search(_flexible_layout_marker_pattern(marker), normalized, flags=re.IGNORECASE)
            if match and (earliest is None or match.start() < earliest):
                earliest = match.start()
        if earliest is not None:
            normalized = f"{normalized[:earliest]}{heading}\n{normalized[earliest:]}"
    continuation_room_targets = (
        "Kitchen",
        "Bulters/WIP",
        "Butlers/WIP",
        "Pantry",
        "Laundry",
        "Bathroom",
        "Powder",
        "Master Ensuite",
        "Guest Ensuite 2",
        "Ensuite 3",
    )
    continuation_stop = "|".join(re.escape(target) for target in continuation_room_targets)
    internal_noise_patterns = (
        r"Internal Paint Selctions",
        r"Internal Paint Selections",
        r"Internal Fittings Selections",
    )
    for pattern in internal_noise_patterns:
        normalized = re.sub(
            rf"(?is){pattern}.*?(?=\n(?:{continuation_stop})\b)",
            "",
            normalized,
        )
    normalized = re.sub(r"(?i)\bClient Initials\b.*$", "", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized


def _trim_lines_to_primary_section(lines: list[str], page_type: str) -> list[str]:
    if not lines:
        return []
    joinery_markers = ("15 CABINETS", "CABINETS", "JOINERY SELECTION SHEET", "COLOUR SCHEDULE")
    sink_markers = ("20 PLUMBING FIXTURES & TAPWARE", "SINKWARE & TAPWARE", "PLUMBING FIXTURES & TAPWARE")
    markers = sink_markers if page_type == "sinkware_tapware" else joinery_markers
    for index, line in enumerate(lines):
        if any(marker in line.upper() for marker in markers):
            return lines[index:]
    return lines


def _looks_like_major_section_heading(line: str) -> bool:
    text = parsing.normalize_space(line)
    upper = text.upper()
    if not text:
        return False
    if re.match(r"^\d+\s+[A-Z]", text):
        return True
    return any(
        marker in upper
        for marker in (
            " ELECTRICAL ",
            " APPLIANCES",
            " PLUMBING FIXTURES",
            " SINKWARE & TAPWARE",
            " MIRRORS",
            " TILING",
            " HARD FLOORING",
        )
    )


def _looks_like_layout_room_heading(line: str, following_lines: list[str] | None = None) -> bool:
    raw_text = parsing.normalize_space(line)
    if re.match(r"^[\-\u2022]+\s*", raw_text):
        return False
    text = parsing.normalize_space(re.sub(r"^[\-\u2022]+\s*", "", raw_text))
    if not text or len(text) > 60:
        return False
    if _looks_like_layout_metadata_line(text) or _looks_like_major_section_heading(text):
        return False
    matched_label, _ = _match_layout_row_label(text)
    if matched_label:
        return False
    cleaned = parsing._clean_layout_room_label(text)
    if not cleaned or cleaned == "Room":
        return False
    if _looks_like_invalid_room_heading_candidate(cleaned):
        return False
    if not (parsing._looks_like_plausible_room_label(cleaned) or parsing._is_room_heading_line(cleaned)):
        return False
    following = [parsing.normalize_space(candidate) for candidate in (following_lines or []) if parsing.normalize_space(candidate)]
    if cleaned.upper().startswith("WC") and any("SELECTION REQUIRED" in candidate.upper() for candidate in following[:3]):
        return False
    if not following:
        return True
    for next_text in following[:4]:
        if next_text.startswith("-"):
            return True
        next_label, _ = _match_layout_row_label(next_text)
        if next_label or parsing._looks_like_field_label(next_text):
            return True
    return False


def _looks_like_row_fragment_prelude(lines: list[str], page_type: str) -> bool:
    meaningful = [parsing.normalize_space(line) for line in lines if parsing.normalize_space(line)]
    if len(meaningful) < 2:
        return False
    prefix_like = 0
    for line in meaningful:
        if _looks_like_layout_metadata_line(line) or _looks_like_major_section_heading(line):
            return False
        label, _ = _match_layout_row_label(line)
        if label or parsing._looks_like_field_label(line):
            prefix_like += 1
            continue
        return False
    return prefix_like >= 2


def _heuristic_room_heading_blocks(lines: list[str], page_type: str, builder_name: str) -> list[dict[str, Any]]:
    trimmed_lines = _trim_lines_to_primary_section(lines, page_type=page_type)
    blocks: list[dict[str, Any]] = []
    current_label = ""
    current_lines: list[str] = []
    heading_indices: list[int] = []
    leading_lines: list[str] = []

    def flush() -> None:
        nonlocal current_label, current_lines
        if not current_label:
            current_lines = []
            return
        rows = _split_lines_to_layout_rows(current_lines, page_type=page_type)
        if rows:
            blocks.append({"room_label": parsing.source_room_label(current_label), "rows": rows})
        current_label = ""
        current_lines = []

    for index, line in enumerate(trimmed_lines):
        next_lines = trimmed_lines[index + 1 : index + 5]
        if _looks_like_major_section_heading(line):
            if current_label or blocks:
                flush()
                break
            continue
        if _looks_like_layout_metadata_line(line):
            continue
        if _looks_like_layout_room_heading(line, following_lines=next_lines):
            flush()
            current_label = parsing.normalize_space(re.sub(r"^[\-\u2022]+\s*", "", line))
            heading_indices.append(index)
            if not blocks and not current_lines and _looks_like_row_fragment_prelude(leading_lines, page_type):
                current_lines = list(leading_lines)
            leading_lines = []
            continue
        if current_label:
            current_lines.append(line)
        elif not blocks:
            leading_lines.append(line)
    flush()
    if page_type == "sinkware_tapware" and len(blocks) == 1 and len(heading_indices) == 1 and heading_indices[0] > 0:
        leading_lines = [
            line
            for line in trimmed_lines[: heading_indices[0]]
            if not _looks_like_major_section_heading(line) and not _looks_like_layout_metadata_line(line)
        ]
        leading_rows = _split_lines_to_layout_rows(leading_lines, page_type=page_type)
        if leading_rows:
            blocks[0]["rows"] = [*leading_rows, *blocks[0].get("rows", [])]
    return [block for block in blocks if block.get("room_label") and block.get("rows")]


def _infer_page_type_from_text(builder_name: str, source_kind: str, text: str) -> str:
    upper = str(text or "").upper()
    builder_key = builder_name.strip().lower()
    if source_kind != "spec":
        return "unknown"
    has_explicit_sinkware_heading = "SINKWARE & TAPWARE" in upper or "SINKWARE (" in upper or "TAPWARE (" in upper
    sink_table_score = sum(
        1
        for marker in (
            "KITCHEN SINK",
            "KITCHEN TAPWARE",
            "PANTRY SINK",
            "PANTRY TAPWARE",
            "LAUNDRY TROUGH",
            "LAUNDRY TAPWARE",
            "VANITY BASIN",
            "VANITY BASIN TAPWARE",
            "TOILET SUITE",
            "TOILET ROLL HOLDER",
            "SHOWER BASE",
            "SHOWER FRAME",
            "SHOWER MIXER",
            "SHOWER ROSE",
            "FLOOR WASTE",
            "WET AREA LOCATION",
        )
        if marker in upper
    )
    joinery_score = sum(
        1
        for token in (
            "BENCHTOP",
            "UNDERBENCH",
            "BASE CUPBOARDS",
            "DRAWERS",
            "BASE CABINET PANELS",
            "CABINET PANELS",
            "KICKBOARD",
            "OVERHEAD",
            "OVERHEAD CUPBOARDS",
            "PANTRY DOOR HANDLES",
            "ISLAND BAR BACK",
            "FLOOR MOUNTED VANITY",
            "LINEN CUPBOARD FITOUT",
            "ROBE FIT OUT",
            "CABINETRY HANDLES",
            "SHAVING CABINETS",
            "WALL RUN",
            "ISLAND/PENISULA",
        )
        if token in upper
    )
    yellowwood_schedule_markers = (
        "BASE CUPBOARDS",
        "OVERHEAD CUPBOARDS",
        "PANTRY DOOR HANDLES",
        "ISLAND BAR BACK",
        "FLOOR MOUNTED VANITY",
        "LINEN CUPBOARD FITOUT",
        "ROBE FIT OUT",
        "FLOOR TILE",
        "WALL TILE",
        "SKIRTING",
        "AREA",
        "ITEM",
    )
    if builder_key == "yellowwood" and sum(1 for token in yellowwood_schedule_markers if token in upper) >= 2:
        return "joinery"
    if (
        not has_explicit_sinkware_heading
        and any(token in upper for token in ("KITCHEN", "PANTRY", "LAUNDRY", "BATHROOM", "ENSUITE", "POWDER", "ALFRESCO", "BUTLERS", "WIP", "STUDY"))
        and joinery_score >= 2
    ):
        return "joinery"
    if sink_table_score >= 3:
        return "sinkware_tapware"
    if (
        any(token in upper for token in ("KITCHEN", "PANTRY", "LAUNDRY", "BATHROOM", "ENSUITE", "POWDER", "ALFRESCO", "BUTLERS", "WIP", "STUDY"))
        and joinery_score >= 2
    ):
        return "joinery"
    if has_explicit_sinkware_heading:
        return "sinkware_tapware"
    if "PLUMBING FIXTURES & TAPWARE" in upper or "VANITY BASIN TAPWARE" in upper or "KITCHEN TAPWARE" in upper:
        if any(
            marker in upper
            for marker in (
                "WALL RUN BENCHTOP",
                "BASE CABINET PANELS",
                "CABINET PANELS",
                "KICKBOARD",
                "OVERHEADS",
                "SHADOWLINE",
                "CABINETRY HANDLES",
            )
        ):
            return "joinery"
        return "sinkware_tapware"
    if (
        "JOINERY SELECTION SHEET" in upper
        or "COLOUR SCHEDULE" in upper
        or "SUPPLIER DESCRIPTION" in upper
        or "15 CABINETS" in upper
        or "WALL RUN BENCHTOP" in upper
        or "BASE CABINET PANELS" in upper
        or "CABINETRY HANDLES" in upper
    ):
        return "joinery"
    if "APPLIANCES" in upper or ("MODEL" in upper and "SUPPLIER" in upper and "AREA / ITEM" in upper):
        return "appliance"
    if "FEATURE TALL DOORS" in upper:
        return "special"
    if any(marker in upper for marker in (" CABINETS", " PLUMBING FIXTURES & TAPWARE", " TILING / HARD FLOORING")):
        return "unknown"
    return "unknown"


def _infer_layout_labels(builder_name: str, lines: list[str], page_type: str) -> tuple[str, str]:
    joined = "\n".join(lines)
    if "imperial" in builder_name.strip().lower():
        section_label = parsing.normalize_space(getattr(parsing, "_extract_imperial_section_title")(joined) or "")
        if section_label:
            room_label = parsing.normalize_space(re.sub(r"(?i)\bJOINERY SELECTION SHEET\b", "", section_label)).strip(" -")
            if room_label.upper() in {"FEATURE TALL DOORS", "APPLIANCES", "SINKWARE & TAPWARE"}:
                room_label = ""
            return section_label, room_label
    if page_type == "sinkware_tapware":
        return "SINKWARE & TAPWARE", ""
    for line in lines[:12]:
        if "COLOUR SCHEDULE" in line.upper():
            cleaned_label = parsing._clean_layout_room_label(line, line)
            return line, cleaned_label or parsing.source_room_label(line)
    for line in lines[:8]:
        label = parsing._clean_layout_room_label(line, line) or parsing.source_room_label(line)
        if label and label not in {"Room", "room"}:
            return line, label
    return "", ""


def _heuristic_sink_tap_room_blocks(lines: list[str]) -> list[dict[str, Any]]:
    room_blocks: list[dict[str, Any]] = []
    current_block: dict[str, Any] | None = None
    current_row: dict[str, str] | None = None
    heading_re = re.compile(r"(?i)^(?P<label>SINKWARE|TAPWARE)\s*\((?P<room>[^)]+)\)\s*(?P<tail>.*)$")
    wet_area_heading_re = re.compile(
        r"(?i)^Location\s+(?P<room>(?:Guest\s+)?Ensuite\s*\d*|Master Ensuite|Ensuite|Bathroom|Powder|Laundry|Kitchen|Pantry|Butlers/?WIP)\b"
    )
    recent_lines: list[str] = []
    for line in lines:
        if _looks_like_layout_metadata_line(line):
            continue
        heading_match = heading_re.match(line)
        if heading_match:
            room_label = parsing.normalize_space(heading_match.group("room"))
            if not current_block or current_block.get("room_label") != room_label:
                current_block = {"room_label": room_label, "rows": []}
                room_blocks.append(current_block)
            current_row = {
                "row_label": f"{heading_match.group('label').upper()} ({room_label})",
                "value_region_text": parsing.normalize_space(heading_match.group("tail")),
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": heading_match.group("label").strip().lower(),
            }
            current_block["rows"].append(current_row)
            recent_lines.append(line)
            recent_lines = recent_lines[-3:]
            continue
        wet_area_match = wet_area_heading_re.match(line)
        if wet_area_match and any(
            token in candidate.upper()
            for candidate in recent_lines[-3:]
            for token in ("WET AREA", "BATH/ENSUITE/POWDER", "ADDITIONAL WET AREA")
        ):
            room_label = parsing.normalize_space(wet_area_match.group("room"))
            current_block = {"room_label": room_label, "rows": []}
            room_blocks.append(current_block)
            current_row = None
            recent_lines.append(line)
            recent_lines = recent_lines[-3:]
            continue
        if current_row is not None:
            if current_row["value_region_text"]:
                current_row["value_region_text"] = f"{current_row['value_region_text']} {line}".strip()
            else:
                current_row["value_region_text"] = line
        recent_lines.append(line)
        recent_lines = recent_lines[-3:]
    return room_blocks


def _split_lines_to_layout_rows(lines: list[str], page_type: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in lines:
        if _looks_like_layout_metadata_line(line):
            if current:
                rows.append(current)
                current = None
            continue
        label, remainder = _match_layout_row_label(line)
        if label:
            if current:
                rows.append(current)
            current = {
                "row_label": label,
                "value_region_text": remainder,
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": _infer_layout_row_kind(label, page_type),
            }
            continue
        if current is None:
            current = {
                "row_label": "",
                "value_region_text": line,
                "supplier_region_text": "",
                "notes_region_text": "",
                "row_kind": _infer_layout_row_kind("", page_type, line),
            }
            continue
        if _looks_like_supplier_only_line(line):
            current["supplier_region_text"] = f"{current['supplier_region_text']} {line}".strip()
        elif current["notes_region_text"]:
            current["notes_region_text"] = f"{current['notes_region_text']} {line}".strip()
        elif current["value_region_text"]:
            current["value_region_text"] = f"{current['value_region_text']} {line}".strip()
        else:
            current["value_region_text"] = line
    if current:
        rows.append(current)
    return rows


def _match_layout_row_label(line: str) -> tuple[str, str]:
    text = parsing.normalize_space(re.sub(r"^[\-\u2022]+\s*", "", line))
    sink_tap_match = re.match(r"(?i)^(SINKWARE|TAPWARE)\s*\(([^)]+)\)\s*(.*)$", text)
    if sink_tap_match:
        label = f"{sink_tap_match.group(1).upper()} ({parsing.normalize_space(sink_tap_match.group(2))})"
        return label, parsing.normalize_space(sink_tap_match.group(3))
    for candidate in LAYOUT_ROW_LABELS:
        match = re.match(rf"(?i)^{re.escape(candidate)}(?:\b|(?=[^A-Za-z0-9]))", text)
        if match:
            remainder = parsing.normalize_space(text[match.end() :])
            return candidate, remainder
    return "", ""


def _infer_layout_row_kind(label: str, page_type: str, line: str = "") -> str:
    label_upper = parsing.normalize_space(str(label or "")).upper()
    upper = f"{label} {line}".upper()
    def has_token(*tokens: str) -> bool:
        return any(re.search(rf"(?<![A-Z0-9]){re.escape(token.upper())}(?![A-Z0-9])", upper) for token in tokens)

    def label_has_token(*tokens: str) -> bool:
        return any(re.search(rf"(?<![A-Z0-9]){re.escape(token.upper())}(?![A-Z0-9])", label_upper) for token in tokens)

    if parsing._is_blacklisted_wet_area_label(label_upper):
        return "metadata"

    if page_type == "sinkware_tapware":
        if label_upper.startswith("SINKWARE"):
            return "sink"
        if label_upper.startswith("TAPWARE"):
            return "tap"
        if label_has_token("TAP", "TAPWARE", "MIXER", "SPOUT"):
            return "tap"
        if label_upper.startswith("BASIN"):
            return "basin"
    if label_has_token("TAP", "TAPWARE", "MIXER", "SPOUT"):
        return "tap"
    if label_has_token("BASIN"):
        return "basin"
    if has_token("BENCHTOP", "UNDERBENCH", "CABINET PANELS", "KICKBOARD", "OVERHEAD", "FLOATING SHELF", "SHELVING"):
        return "material"
    if has_token("HANDLE"):
        return "handle"
    if has_token("ACCESSORIES", "GPO", "BIN", "HAMPER"):
        return "accessory"
    if label_has_token("SINK", "DROP IN TUB", "TROUGH", "TUB", "BATH"):
        return "sink"
    if _looks_like_layout_metadata_line(upper):
        return "metadata"
    if page_type == "appliance":
        return "other"
    return "material"


def _looks_like_layout_metadata_line(line: str) -> bool:
    text = parsing.normalize_space(line)
    upper = text.upper()
    if not upper:
        return True
    metadata_tokens = (
        "ADDRESS:",
        "SITE ADDRESS:",
        "CLIENT:",
        "DATE:",
        "DOCUMENT REF:",
        "DESIGNER:",
        "CLIENT NAME:",
        "SIGNATURE:",
        "SIGNED DATE:",
        "ALL COLOURS SHOWN",
        "PRODUCT AVAILABILITY",
        "PAGE ",
        "AREA / ITEM SPECS / DESCRIPTION IMAGE SUPPLIER NOTES",
    )
    return any(token in upper for token in metadata_tokens)


def _looks_like_supplier_only_line(line: str) -> bool:
    text = parsing.normalize_space(line)
    upper = text.upper()
    if not text:
        return False
    return upper in {
        "POLYTEC",
        "LAMINEX",
        "CAESARSTONE",
        "SMARTSTONE",
        "WK STONE",
        "ABI INTERIORS",
        "HETTICH",
        "MOMO",
        "HAFELE",
        "ABEY",
        "FRANKE",
        "TITUS TEKFORM",
    }


def _apply_vision_fallback(
    job: dict[str, Any],
    builder: dict[str, Any],
    documents: list[dict[str, object]],
    heuristic: dict[str, Any],
    source_kind: str,
    rule_flags: Any = None,
    progress_callback: ProgressCallback = None,
) -> tuple[list[dict[str, object]], dict[str, Any], dict[str, Any]]:
    vision_meta = _blank_vision_meta()
    if not documents:
        vision_meta["vision_note"] = "No source documents available for vision fallback."
        _report_progress(progress_callback, "vision_skipped", vision_meta["vision_note"])
        return documents, heuristic, vision_meta
    if not runtime.OPENAI_VISION_ENABLED:
        vision_meta["vision_note"] = "OpenAI vision fallback is disabled in runtime settings."
        _report_progress(progress_callback, "vision_skipped", vision_meta["vision_note"])
        return documents, heuristic, vision_meta
    if not runtime.OPENAI_ENABLED:
        vision_meta["vision_note"] = "OpenAI is disabled, so vision fallback was skipped."
        _report_progress(progress_callback, "vision_skipped", vision_meta["vision_note"])
        return documents, heuristic, vision_meta
    if not runtime.OPENAI_API_KEY:
        vision_meta["vision_note"] = "OPENAI_API_KEY is not configured, so vision fallback was skipped."
        _report_progress(progress_callback, "vision_skipped", vision_meta["vision_note"])
        return documents, heuristic, vision_meta

    candidates = _select_vision_pages(
        builder_name=str(builder.get("name", "") or ""),
        documents=documents,
        heuristic=heuristic,
        source_kind=source_kind,
    )
    if not candidates:
        vision_meta["vision_note"] = "No high-risk pages matched the vision fallback rules."
        _report_progress(progress_callback, "vision_skipped", vision_meta["vision_note"])
        return documents, heuristic, vision_meta

    updated_documents: list[dict[str, object]] = []
    for document in documents:
        updated_documents.append(
            {
                **document,
                "pages": [dict(page) for page in list(document.get("pages", []))],
            }
        )

    attempted_pages: list[int] = []
    applied_pages: list[int] = []
    page_notes: list[str] = []
    vision_meta["vision_attempted"] = True
    max_pages = max(1, runtime.OPENAI_VISION_MAX_PAGES)

    for doc_index, page_index in candidates[:max_pages]:
        document = updated_documents[doc_index]
        pages = list(document.get("pages", []))
        if page_index >= len(pages):
            continue
        page = pages[page_index]
        page_no = int(page.get("page_no", 0) or 0)
        attempted_pages.append(page_no)
        try:
            _report_progress(
                progress_callback,
                "vision_prepare",
                f"Preparing page {page_no} from {document['file_name']} for vision layout",
            )
            image_bytes = _render_pdf_page_png(
                Path(str(document.get("path", ""))),
                page_no=page_no,
                dpi=runtime.OPENAI_VISION_DPI,
            )
            _report_progress(
                progress_callback,
                "vision_request",
                f"Calling OpenAI vision for {document['file_name']} page {page_no}",
            )
            layout = _request_page_layout(
                job_no=str(job.get("job_no", "")),
                builder_name=str(builder.get("name", "") or ""),
                source_kind=source_kind,
                file_name=str(document.get("file_name", "") or ""),
                page_no=page_no,
                page_text=str(page.get("raw_text", page.get("text", "")) or ""),
                image_bytes=image_bytes,
            )
            normalized_text = _vision_layout_to_text(layout, fallback_text=str(page.get("raw_text", page.get("text", "")) or ""))
            if not normalized_text:
                page_notes.append(f"page {page_no}: OpenAI vision returned no usable layout text")
                continue
            page["page_layout"] = layout
            page["vision_applied"] = True
            page["text"] = normalized_text
            applied_pages.append(page_no)
        except Exception as exc:
            page_notes.append(f"page {page_no}: {_truncate_note(exc)}")
            if _is_openai_insufficient_quota_error(exc):
                page_notes.append("OpenAI vision quota is exhausted; skipped remaining candidate pages.")
                break

    if not applied_pages:
        vision_meta["vision_pages"] = attempted_pages
        vision_meta["vision_page_count"] = len(attempted_pages)
        vision_meta["vision_note"] = "; ".join(page_notes)[:400] if page_notes else "Vision fallback attempted but no page layout was applied."
        _report_progress(progress_callback, "vision_fallback", vision_meta["vision_note"])
        return documents, heuristic, vision_meta

    try:
        _report_progress(
            progress_callback,
            "vision_apply",
            f"Re-running heuristic extraction with vision-normalized layout on {len(applied_pages)} page(s)",
        )
        heuristic = parsing.parse_documents(
            job_no=str(job.get("job_no", "")),
            builder_name=str(builder.get("name", "") or ""),
            source_kind=source_kind,
            documents=updated_documents,
            rule_flags=rule_flags,
        )
    except Exception as exc:
        vision_meta["vision_pages"] = attempted_pages
        vision_meta["vision_page_count"] = len(attempted_pages)
        vision_meta["vision_note"] = f"Vision layout applied but heuristic re-parse failed: {_truncate_note(exc)}"
        _report_progress(progress_callback, "vision_fallback", vision_meta["vision_note"])
        return documents, heuristic, vision_meta

    vision_meta["vision_succeeded"] = True
    vision_meta["vision_pages"] = applied_pages
    vision_meta["vision_page_count"] = len(applied_pages)
    success_note = f"Vision fallback applied to {len(applied_pages)} page(s): {', '.join(str(page_no) for page_no in applied_pages)}."
    if page_notes:
        success_note = f"{success_note} Partial issues: {'; '.join(page_notes)[:220]}"
    vision_meta["vision_note"] = success_note[:400]
    return updated_documents, heuristic, vision_meta


def _select_vision_pages(
    builder_name: str,
    documents: list[dict[str, object]],
    heuristic: dict[str, Any],
    source_kind: str,
) -> list[tuple[int, int]]:
    candidates: list[tuple[int, int]] = []
    for doc_index, document in enumerate(documents):
        document_path = Path(str(document.get("path", "") or ""))
        if document_path.suffix.lower() != ".pdf":
            continue
        pages = list(document.get("pages", []))
        for page_index, page in enumerate(pages):
            if _page_requires_vision(
                builder_name=builder_name,
                source_kind=source_kind,
                file_name=str(document.get("file_name", "") or ""),
                page=page,
                heuristic=heuristic,
            ):
                candidates.append((doc_index, page_index))
    return candidates


def _page_requires_vision(
    builder_name: str,
    source_kind: str,
    file_name: str,
    page: dict[str, Any],
    heuristic: dict[str, Any],
) -> bool:
    text = str(page.get("raw_text", page.get("text", "")) or "")
    if not text:
        return False
    strategy = _page_family_strategy(builder_name, text) if source_kind == "spec" else {}
    upper = text.upper()
    if bool(strategy.get("vision_grid_default")):
        return True
    if source_kind == "spec" and any(
        marker in upper
        for marker in (
            "COLOUR SCHEDULE",
            "JOINERY SELECTION SHEET",
            "SINKWARE & TAPWARE",
            "SINKWARE (",
            "TAPWARE (",
            "APPLIANCES",
            "15 CABINETS",
            "PLUMBING FIXTURES & TAPWARE",
            "WALL RUN BENCHTOP",
            "BASE CABINET PANELS",
            "CABINETRY HANDLES",
        )
    ):
        return True
    if bool(page.get("needs_ocr")):
        return True
    if _looks_like_glued_field_page(text):
        return True
    if _looks_like_mixed_field_value_page(text):
        return True
    if _looks_like_reversed_sinkware_page(text):
        return True
    if _looks_like_high_risk_table_page(text, builder_name=builder_name, source_kind=source_kind):
        return True
    if "JOINERY SELECTION SHEET" in upper and "imperial" in builder_name.strip().lower():
        return True
    if "SINKWARE & TAPWARE" in upper:
        return True
    if "APPLIANCES" in upper and ("MODEL" in upper or "SUPPLIER" in upper or "NOTES" in upper):
        return True
    _ = heuristic
    _ = file_name
    return False


def _page_requires_heavy_vision(
    builder_name: str,
    source_kind: str,
    file_name: str,
    page: dict[str, Any],
) -> bool:
    _ = file_name
    text = str(page.get("raw_text", page.get("text", "")) or "")
    if not text:
        return False
    if source_kind != "spec":
        return _page_requires_vision(builder_name, source_kind, file_name, page, heuristic={})
    strategy = _page_family_strategy(builder_name, text)
    if bool(strategy.get("vision_grid_default")):
        return True
    if str(strategy.get("page_family", "") or "") != "unknown":
        return False
    return _page_requires_vision(builder_name, source_kind, file_name, page, heuristic={})


def _looks_like_imperial_joinery_material_page(text: str) -> bool:
    upper = str(text or "").upper()
    if "APPLIANCES" in upper or "SINKWARE & TAPWARE" in upper or "SINKWARE (" in upper or "TAPWARE (" in upper:
        return False
    if "JOINERY SELECTION SHEET" in upper:
        return True
    if "CUSTOM HANDLES" in upper:
        return True
    if "COLOUR SCHEDULE" in upper and any(
        marker in upper
        for marker in (
            "BENCHTOP",
            "BASE CABINETRY",
            "UPPER CABINETRY",
            "KICKBOARDS",
            "HANDLES",
            "FLOATING SHELV",
            "SPLASHBACK",
        )
    ):
        return True
    joinery_markers = (
        "BENCHTOP",
        "BASE CABINETRY",
        "UPPER CABINETRY",
        "OVERHEAD",
        "KICKBOARD",
        "HANDLES",
        "SPLASHBACK",
        "FLOATING SHELV",
        "FEATURE BAR BACK",
        "BULKHEAD",
    )
    if sum(1 for marker in joinery_markers if marker in upper) >= 2:
        return True
    return False


def _looks_like_glued_field_page(text: str) -> bool:
    glue_patterns = (
        r"(?i)(?:HANDLES|BENCHTOPS?|SPLASHBACK|KICKBOARDS?|BASE CABINETRY COLOUR|UPPER CABINETRY COLOUR|TALL CABINETRY COLOUR|FLOATING SHELV(?:ES|ING)|GPO'?S|BIN|HAMPER)(?=[A-Z])",
        r"(?i)(?:AREA / ITEM|NOTES|SUPPLIER)(?=[A-Z])",
    )
    return any(re.search(pattern, text) for pattern in glue_patterns)


def _looks_like_mixed_field_value_page(text: str) -> bool:
    lines = [parsing.normalize_space(line) for line in str(text).splitlines() if parsing.normalize_space(line)]
    for line in lines:
        upper = line.upper()
        if "BENCHTOP" in upper and any(token in upper for token in ("KICKBOARD", "HANDLES", "GPO", "BIN", "HAMPER")):
            return True
        if "SPLASHBACK" in upper and any(token in upper for token in ("BENCHTOP", "HANDLES", "KICKBOARD")):
            return True
        if "HANDLES" in upper and any(token in upper for token in ("BASE CABINETRY COLOUR", "UPPER CABINETRY COLOUR", "BENCHTOP")):
            return True
    return False


def _looks_like_reversed_sinkware_page(text: str) -> bool:
    upper = text.upper()
    sinkware_heading = upper.find("SINKWARE & TAPWARE")
    room_block = min(
        [index for index in (upper.find("SINKWARE ("), upper.find("TAPWARE (")) if index >= 0] or [-1],
    )
    return sinkware_heading >= 0 and room_block >= 0 and room_block < sinkware_heading


def _looks_like_high_risk_table_page(text: str, builder_name: str, source_kind: str) -> bool:
    upper = text.upper()
    builder_key = builder_name.strip().lower()
    has_table_headers = "AREA / ITEM" in upper and ("SUPPLIER" in upper or "NOTES" in upper)
    if has_table_headers:
        return True
    if source_kind == "spec" and "COLOUR SCHEDULE" in upper and "SUPPLIER DESCRIPTION" in upper:
        return True
    if builder_key == "yellowwood" and sum(
        1
        for marker in (
            "BASE CUPBOARDS",
            "OVERHEAD CUPBOARDS",
            "PANTRY DOOR HANDLES",
            "ISLAND BAR BACK",
            "FLOOR MOUNTED VANITY",
            "LINEN CUPBOARD FITOUT",
            "ROBE FIT OUT",
            "FLOOR TILE",
            "WALL TILE",
            "SKIRTING",
        )
        if marker in upper
    ) >= 2:
        return True
    if builder_key == "imperial" and any(
        marker in upper for marker in ("JOINERY SELECTION SHEET", "SINKWARE & TAPWARE", "APPLIANCES")
    ):
        return True
    return False


def _render_pdf_page_png(path: Path, page_no: int, dpi: int) -> bytes:
    try:
        import fitz
    except ImportError as exc:  # pragma: no cover - runtime guard
        raise RuntimeError("PyMuPDF is not installed for vision page rendering.") from exc

    if not path.exists():
        raise RuntimeError(f"Source PDF was not found for vision fallback: {path}")
    with fitz.open(str(path)) as document:
        if page_no < 1 or page_no > document.page_count:
            raise RuntimeError(f"Requested page {page_no} is outside the PDF page range.")
        page = document.load_page(page_no - 1)
        scale = max(float(dpi) / 72.0, 1.0)
        matrix = fitz.Matrix(scale, scale)
        pixmap = page.get_pixmap(matrix=matrix, alpha=False)
        return pixmap.tobytes("png")


def _request_page_layout(
    job_no: str,
    builder_name: str,
    source_kind: str,
    file_name: str,
    page_no: int,
    page_text: str,
    image_bytes: bytes,
    document_path: Path | None = None,
    table_rows: list[Any] | None = None,
) -> dict[str, Any]:
    imperial_joinery_hint = ""
    if _is_imperial_builder_name(builder_name) and _looks_like_imperial_joinery_material_page(page_text):
        imperial_joinery_hint = (
            " For Imperial joinery/material selection sheets, prioritize the visible table grid over OCR order. "
            "Do not invent room blocks named Benchtop, Splashback, Pantry, Overheads, Under Bench Doors, "
            "Base Cabinetry Colour, Upper Cabinetry, Floating Shelves, or Feature Bar Back when the page already belongs "
            "to one room. Treat those as row labels or field groups instead. "
            "If the page is a continuation sheet with cabinetry/handles/splashback only, you may leave room_label blank "
            "rather than inventing a field-like room block. "
            "Treat CLIENT NAME, SIGNATURE, SIGNED DATE, NOTES SUPPLIER, and DOCUMENT REF as footer noise. "
            "Return each visible table row as a structured row repair with room_scope, row_label, value_region_text, image_region_text, supplier_region_text, notes_region_text, confidence, and provenance. "
            "Keep AREA / ITEM labels such as BENCHTOP, SPLASHBACK, BASE CABINETRY COLOUR, FEATURE COLOUR OVERHEADS, FEATURE ISLAND COLOUR, KICKBOARDS, and HANDLES on their own rows and never merge them into the previous value row."
        )
    prompt = {
        "job_no": job_no,
        "builder_name": builder_name,
        "source_kind": source_kind,
        "file_name": file_name,
        "page_no": page_no,
        "instructions": (
            "You are correcting PDF table structure. Return JSON only. "
            "Identify the page_type, section_label, room_label, room_blocks, and rows. "
            "Rows must preserve the visible table or block order and must not mix adjacent rows. "
            "Each row must include row_label, value_region_text, image_region_text, supplier_region_text, notes_region_text, row_kind, confidence, and provenance. "
            "When available, also include room_scope, row_order, and page_no for each row. "
            "When a page contains multiple room blocks, room_blocks must preserve each room_label and its rows in visible order. "
            "Room labels must be actual room or section titles, not field labels. "
            "Do not use labels like Manufacturer, Colour, Type, Model, Sink, Tapware, Pantry Doors, Bathroom Type Frameless, Kitchen Sink Model, Vanity Basin Tapware, or Document Ref as room_label. "
            "On dense builder tables, headings like Kitchen, Butlers/WIP, Pantry, Laundry, Bathroom, Ensuite, Powder, Alfresco, Study Desk, and Dining Banquette are room labels; labels like Robe Hook, Hand Towel Rail, Toilet Roll Holder, Mirrors, Ref. Number, or Selection Required are never room labels. "
            "If a row label includes a room prefix such as 'Kitchen - Sink Model' or 'Pantry Sink', keep the room in room_label/room_blocks and leave only the field part in row_label. "
            "If a joinery title has leading noise like 'NA DINING BANQUETTE JOINERY SELECTION SHEET', drop the noise and keep 'DINING BANQUETTE' as the room/section title. "
            "On plumbing, sinkware, and tapware tables, keep each wet-area room in its own room block. "
            "Rows such as Basin, Basin Mixer, Bath, Bath Mixer / Spout, Shower Mixer, Shower Rose, Accessories, Toilet Suite, Toilet Roll Holder, and Floor Waste must stay separate and must not be collapsed into Selection Required or Floor Waste. "
            "If OCR text order is scrambled, use the visual row alignment from the image to keep values with the correct row, not the nearest OCR line. "
            "Use row_kind only from: material, handle, accessory, sink, tap, basin, metadata, footer, other. "
            "Treat disclaimers, signatures, dates, and client blocks as metadata or footer, not material rows."
            f"{imperial_joinery_hint}"
        ),
        "ocr_text": page_text,
    }
    encoded_image = base64.b64encode(image_bytes).decode("ascii")
    response_json = _post_responses_api_content(
        [
            {"type": "input_text", "text": json.dumps(prompt, ensure_ascii=False)},
            {"type": "input_image", "image_url": f"data:image/png;base64,{encoded_image}"},
        ]
    )
    output_text = _extract_output_text(response_json)
    if not output_text:
        raise RuntimeError("OpenAI vision returned no output text.")
    parsed = _parse_openai_json_output(output_text)
    if isinstance(parsed.get("page_layout"), dict):
        parsed = parsed["page_layout"]
    if not isinstance(parsed, dict):
        raise RuntimeError("OpenAI vision returned a non-object page layout.")
    return _postprocess_builder_layout(
        builder_name=builder_name,
        source_kind=source_kind,
        layout=parsed,
        raw_page_text=page_text,
        provider="heavy_vision",
        table_rows=table_rows or [],
        document_path=document_path,
        page_no=page_no,
    )


def _normalize_page_layout(layout: dict[str, Any]) -> dict[str, Any]:
    page_type = str(layout.get("page_type", "unknown") or "unknown").strip().lower().replace(" ", "_")
    if page_type not in {"joinery", "sinkware_tapware", "appliance", "special", "unknown"}:
        page_type = "unknown"
    normalized_rows = _normalize_layout_rows(layout.get("rows", []))
    normalized_blocks: list[dict[str, Any]] = []
    for raw_block in layout.get("room_blocks", []):
        if not isinstance(raw_block, dict):
            continue
        block_rows = _normalize_layout_rows(raw_block.get("rows", []))
        normalized_blocks.append(
            {
                "room_label": parsing.normalize_space(str(raw_block.get("room_label", "") or "")),
                "rows": block_rows,
            }
        )
    if not normalized_blocks and (normalized_rows or layout.get("room_label")):
        normalized_blocks = [
            {
                "room_label": parsing.normalize_space(str(layout.get("room_label", "") or "")),
                "rows": normalized_rows,
            }
        ]
    return {
        "page_type": page_type,
        "section_label": parsing.normalize_space(str(layout.get("section_label", "") or "")),
        "room_label": parsing.normalize_space(str(layout.get("room_label", "") or "")),
        "room_blocks": normalized_blocks,
        "rows": normalized_rows,
        "provider": parsing.normalize_space(str(layout.get("provider", "") or "")),
        "unresolved_rows": [dict(row) for row in layout.get("unresolved_rows", []) if isinstance(row, dict)],
    }


def _vision_layout_to_text(layout: dict[str, Any], fallback_text: str = "") -> str:
    if not isinstance(layout, dict):
        return fallback_text
    lines: list[str] = []
    section_label = parsing.normalize_space(str(layout.get("section_label", "") or ""))
    room_label = parsing.normalize_space(str(layout.get("room_label", "") or ""))
    if section_label:
        lines.append(section_label)
    elif room_label:
        lines.append(room_label)

    room_blocks = layout.get("room_blocks", [])
    if isinstance(room_blocks, list) and room_blocks:
        for block in room_blocks:
            if not isinstance(block, dict):
                continue
            block_room = parsing.normalize_space(str(block.get("room_label", "") or ""))
            if block_room and block_room != room_label and block_room != section_label:
                lines.append(block_room)
            for raw_row in block.get("rows", []):
                rendered = _render_layout_row_text(raw_row)
                if rendered:
                    lines.append(rendered)
    else:
        for raw_row in layout.get("rows", []):
            rendered = _render_layout_row_text(raw_row)
            if rendered:
                lines.append(rendered)

    normalized = parsing.normalize_space("\n".join(line for line in lines if line))
    return normalized or fallback_text


def _normalize_layout_rows(raw_rows: Any) -> list[dict[str, str]]:
    normalized_rows: list[dict[str, str]] = []
    for raw_row in raw_rows or []:
        if not isinstance(raw_row, dict):
            continue
        row_kind = str(raw_row.get("row_kind", "other") or "other").strip().lower().replace(" ", "_")
        if row_kind not in {"material", "handle", "accessory", "sink", "tap", "basin", "metadata", "footer", "other"}:
            row_kind = "other"
        normalized_rows.append(
            {
                "room_scope": parsing.normalize_space(str(raw_row.get("room_scope", "") or "")),
                "row_label": parsing.normalize_space(str(raw_row.get("row_label", "") or "")),
                "value_region_text": parsing.normalize_space(str(raw_row.get("value_region_text", "") or "")),
                "image_region_text": parsing.normalize_space(str(raw_row.get("image_region_text", "") or "")),
                "supplier_region_text": parsing.normalize_space(str(raw_row.get("supplier_region_text", "") or "")),
                "notes_region_text": parsing.normalize_space(str(raw_row.get("notes_region_text", "") or "")),
                "row_kind": row_kind,
                "page_no": int(raw_row.get("page_no", 0) or 0),
                "row_order": int(raw_row.get("row_order", 0) or 0),
                "repair_source": parsing.normalize_space(str(raw_row.get("repair_source", "") or "")),
                "position_scope": parsing.normalize_space(str(raw_row.get("position_scope", "") or "")),
                "confidence": _normalize_confidence_value(raw_row.get("confidence", 0.0)),
                "provenance": _coerce_provenance_dict(raw_row.get("provenance", {})),
                "needs_review": bool(raw_row.get("needs_review", False)),
            }
        )
    return _normalize_layout_row_fragments(normalized_rows)


IMPLICIT_LAYOUT_ANCHOR_LABELS: tuple[str, ...] = (
    "Wall Run Base Cabinet Panels",
    "Base Cabinet Panels",
    "Island/Penisula Base Cabinet Panels",
    "Island/Penisula Feature Panels",
    "Wall Run Kickboard",
    "Island/Penisula Kickboard",
    "Overhead Cupboards",
    "Pantry Doors",
    "Shaving Cabinets",
    "Floating Shelves",
    "Shelving",
    "Benchtops",
    "Benchtop",
    "Underbench",
    "Kickboard",
    "Handles",
    "Accessories",
    "Laundry Trough",
    "Laundry Tapware",
    "Kitchen Sink",
    "Kitchen Tapware",
    "Pantry Sink",
    "Pantry Tapware",
    "Vanity Basin",
    "Vanity Basin Tapware",
    "Mirror",
)

EMBEDDED_LAYOUT_ANCHOR_COMBINATIONS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("Wall Run Base", ("Cabinet Panels",), "Wall Run Base Cabinet Panels"),
    ("Base", ("Cabinet Panels",), "Base Cabinet Panels"),
    ("Wall Run", ("Benchtop", "Benchtops"), "Wall Run Benchtop"),
    ("Wall Run", ("Kickboard",), "Wall Run Kickboard"),
    ("Island/Penisula Base", ("Cabinet Panels",), "Island/Penisula Base Cabinet Panels"),
    ("Island/Peninsula Base", ("Cabinet Panels",), "Island/Penisula Base Cabinet Panels"),
    ("Island/Penisula", ("Benchtop", "Benchtops"), "Island/Penisula Benchtop"),
    ("Island/Peninsula", ("Benchtop", "Benchtops"), "Island/Penisula Benchtop"),
    ("Island/Penisula", ("Kickboard",), "Island/Penisula Kickboard"),
    ("Island/Peninsula", ("Kickboard",), "Island/Penisula Kickboard"),
    ("Overhead", ("Cabinetry Handles",), "Overhead Cabinetry Handles"),
)

EMBEDDED_LAYOUT_DIRECT_TAILS: tuple[tuple[str, str], ...] = (
    ("Waterfall End Panels", "Waterfall End Panels"),
    ("Base Cabinet Panels", "Base Cabinet Panels"),
    ("Cabinet Panels", "Cabinet Panels"),
    ("Kickboard", "Kickboard"),
    ("Pantry Doors", "Pantry Doors"),
    ("Overhead Cupboards", "Overhead Cupboards"),
    ("Overheads", "Overheads"),
    ("Tall Panel", "Tall Panel"),
    ("Floating Shelves", "Floating Shelves"),
    ("Floating Shelf", "Floating Shelf"),
    ("Shelving", "Shelving"),
    ("Shaving Cabinets", "Shaving Cabinets"),
)


def _normalize_layout_row_fragments(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    working_rows = [dict(row) for row in rows]
    normalized: list[dict[str, str]] = []
    total = len(working_rows)
    for index, raw_row in enumerate(working_rows):
        row = dict(raw_row)
        label = parsing.normalize_space(str(row.get("row_label", "") or ""))
        value = parsing.normalize_space(str(row.get("value_region_text", "") or ""))
        notes = parsing.normalize_space(str(row.get("notes_region_text", "") or ""))
        next_label = ""
        if index + 1 < total:
            next_label = parsing.normalize_space(str(working_rows[index + 1].get("row_label", "") or ""))

        if not label:
            implicit_label, implicit_value, implicit_note = _extract_implicit_layout_anchor(value)
            if implicit_label:
                row["row_label"] = implicit_label
                row["value_region_text"] = implicit_value
                row["notes_region_text"] = implicit_note
                row["row_kind"] = _infer_layout_row_kind(implicit_label, "joinery", implicit_value)
                label = implicit_label
                value = implicit_value
                notes = implicit_note

        current_label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
        trailing_label = ""
        cleaned_value = value
        if not current_label or current_label in GENERIC_LAYOUT_ANCHOR_LABELS or current_label in {
            "manufacturer",
            "range",
            "model",
            "colour",
            "colour & finish",
            "finish",
            "profile",
            "edge profile",
            "island edge profile",
            "style",
            "type",
        }:
            trailing_label, cleaned_value = _extract_embedded_layout_anchor_tail(value, next_label)
        if trailing_label:
            row["value_region_text"] = cleaned_value
            normalized.append(row)
            if index + 1 < total and _can_promote_next_layout_row_label(next_label):
                working_rows[index + 1]["row_label"] = trailing_label
                working_rows[index + 1]["row_kind"] = _infer_layout_row_kind(trailing_label, "joinery", "")
            else:
                normalized.append(
                    {
                        "row_label": trailing_label,
                        "value_region_text": "",
                        "supplier_region_text": "",
                        "notes_region_text": "",
                        "row_kind": _infer_layout_row_kind(trailing_label, "joinery", ""),
                    }
                )
            continue

        normalized.append(row)
    return normalized


def _extract_implicit_layout_anchor(text: str) -> tuple[str, str, str]:
    source = parsing.normalize_space(text)
    if not source:
        return "", "", ""
    for label in IMPLICIT_LAYOUT_ANCHOR_LABELS:
        pattern = _flexible_layout_marker_pattern(label)
        match = re.search(rf"(?i)(?P<prefix>.*?)(?:[-:]\s*)?(?P<label>{pattern})(?P<suffix>\b.*)?$", source)
        if not match:
            continue
        prefix = parsing.normalize_space(match.group("prefix") or "")
        suffix = parsing.normalize_space(match.group("suffix") or "")
        if prefix and prefix.upper() in {"N/A", "NA"}:
            prefix = ""
        return label, suffix, prefix
    return "", source, ""


def _extract_embedded_layout_anchor_tail(value_text: str, next_label: str) -> tuple[str, str]:
    value = parsing.normalize_space(value_text)
    if not value:
        return "", value
    next_normalized = _normalize_generic_row_label(next_label)
    for tail, next_candidates, anchor in EMBEDDED_LAYOUT_ANCHOR_COMBINATIONS:
        if next_normalized not in {_normalize_generic_row_label(candidate) for candidate in next_candidates}:
            continue
        tail_pattern = _flexible_layout_marker_pattern(tail)
        match = re.search(rf"(?i)^(?P<body>.*?)(?:[-:]\s*)?(?P<tail>{tail_pattern})$", value)
        if not match:
            continue
        body = parsing.normalize_space(match.group("body") or "")
        if body and body.upper() not in {"N/A", "NA", "NOT APPLICABLE"}:
            return anchor, body
    for tail, anchor in EMBEDDED_LAYOUT_DIRECT_TAILS:
        tail_pattern = _flexible_layout_marker_pattern(tail)
        match = re.search(rf"(?i)^(?P<body>.*?)(?:[-:]\s*)?(?P<tail>{tail_pattern})$", value)
        if not match:
            continue
        if next_normalized == _normalize_generic_row_label(anchor):
            continue
        body = parsing.normalize_space(match.group("body") or "")
        if body and body.upper() not in {"N/A", "NA", "NOT APPLICABLE"}:
            return anchor, body
    return "", value


def _can_promote_next_layout_row_label(next_label: str) -> bool:
    normalized = _normalize_generic_row_label(next_label)
    return normalized in {
        "benchtop",
        "benchtops",
        "cabinet panels",
        "kickboard",
        "handles",
        "cabinetry handles",
        "overhead cabinetry handles",
        "pantry sink",
        "pantry tapware",
        "kitchen sink",
        "kitchen tapware",
        "laundry trough",
        "laundry tapware",
        "tapware",
        "sink",
        "shelving",
        "floating shelves",
        "floating shelf",
    }


def _render_layout_row_text(raw_row: Any) -> str:
    if not isinstance(raw_row, dict):
        return ""
    row_kind = str(raw_row.get("row_kind", "other") or "other").strip().lower()
    if row_kind in {"metadata", "footer"}:
        return ""
    label = parsing.normalize_space(str(raw_row.get("row_label", "") or ""))
    value_bits = [
        parsing.normalize_space(str(raw_row.get("value_region_text", "") or "")),
        parsing.normalize_space(str(raw_row.get("supplier_region_text", "") or "")),
        parsing.normalize_space(str(raw_row.get("notes_region_text", "") or "")),
    ]
    value_text = parsing.normalize_space(" ".join(bit for bit in value_bits if bit))
    if label and value_text:
        return f"{label} {value_text}"
    if label:
        return label
    return value_text


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
        "layout_attempted": False,
        "layout_succeeded": False,
        "layout_mode": "",
        "layout_pages": [],
        "heavy_vision_pages": [],
        "layout_note": "",
        "openai_attempted": False,
        "openai_succeeded": False,
        "openai_model": runtime.OPENAI_MODEL,
        "vision_attempted": False,
        "vision_succeeded": False,
        "vision_pages": [],
        "vision_page_count": 0,
        "vision_note": "",
        "note": "",
    }
    if source_kind == "spec" and not _spec_openai_merge_enabled(str(job.get("job_no", "") or ""), source_kind):
        analysis["note"] = (
            "OpenAI merge is disabled by default for spec runs. "
            "Use SPEC_EXTRACTION_FORCE_SPEC_OPENAI_MERGE_JOBS to override a specific job."
        )
        _report_progress(progress_callback, "openai_skipped", analysis["note"])
        return None, analysis
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
            "Return JSON only with keys: rooms, special_sections, appliances, others, warnings. "
            "Room rows must include room_key, original_room_label, bench_tops, door_panel_colours, toe_kick, bulkheads, handles, "
            "drawers_soft_close, hinges_soft_close, splashback, flooring, floating_shelf, shelf, led, led_note, accessories, other_items, door_colours_overheads, door_colours_base, door_colours_tall, door_colours_island, door_colours_bar_back, feature_colour, "
            "sink_info, basin_info, tap_info, source_file, page_refs, evidence_snippet, confidence. "
            "Special sections, when present, must include section_key, original_section_label, fields, source_file, page_refs, evidence_snippet, confidence. "
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
    return _post_responses_api_content(
        [{"type": "input_text", "text": json.dumps(prompt, ensure_ascii=False)}],
        model=runtime.OPENAI_MODEL,
    )


def _post_responses_api_content(content: list[dict[str, Any]], model: str = "") -> dict[str, Any]:
    global _LAST_OPENAI_REQUEST_AT

    body = json.dumps(
        {
            "model": model or runtime.OPENAI_MODEL,
            "input": [
                {
                    "role": "user",
                    "content": content,
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
    last_error: Exception | None = None
    for attempt in range(max(runtime.OPENAI_REQUEST_MAX_RETRIES, 1)):
        min_interval = max(float(runtime.OPENAI_REQUEST_MIN_INTERVAL_SECONDS), 0.0)
        wait_for_gap = min_interval - (time.monotonic() - _LAST_OPENAI_REQUEST_AT)
        if wait_for_gap > 0:
            time.sleep(wait_for_gap)
        try:
            with urllib.request.urlopen(request, timeout=float(runtime.OPENAI_REQUEST_TIMEOUT_SECONDS)) as response:
                _LAST_OPENAI_REQUEST_AT = time.monotonic()
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_error = exc
            _LAST_OPENAI_REQUEST_AT = time.monotonic()
            error_body = _read_http_error_body(exc)
            error_code, error_message = _parse_openai_error_details(error_body)
            if error_code == "insufficient_quota":
                raise RuntimeError(_format_openai_http_error(exc, error_code, error_message))
            if exc.code not in {408, 409, 429, 500, 502, 503, 504} or attempt >= runtime.OPENAI_REQUEST_MAX_RETRIES - 1:
                if error_code or error_message:
                    raise RuntimeError(_format_openai_http_error(exc, error_code, error_message))
                raise
            retry_after = exc.headers.get("Retry-After", "") if exc.headers else ""
            reset_after = exc.headers.get("x-ratelimit-reset-requests", "") if exc.headers else ""
            try:
                retry_after_seconds = float(retry_after)
            except (TypeError, ValueError):
                retry_after_seconds = 0.0
            if not retry_after_seconds:
                try:
                    reset_after_seconds = float(reset_after)
                except (TypeError, ValueError):
                    reset_after_seconds = 0.0
                retry_after_seconds = max(retry_after_seconds, reset_after_seconds)
            backoff = max(
                retry_after_seconds,
                float(runtime.OPENAI_REQUEST_RETRY_BASE_SECONDS) * (2 ** attempt),
            )
            backoff = min(backoff, float(runtime.OPENAI_REQUEST_RETRY_MAX_SECONDS))
            time.sleep(backoff)
        except (TimeoutError, urllib.error.URLError) as exc:
            last_error = exc
            _LAST_OPENAI_REQUEST_AT = time.monotonic()
            if attempt >= runtime.OPENAI_REQUEST_MAX_RETRIES - 1:
                raise
            backoff = max(float(runtime.OPENAI_REQUEST_RETRY_BASE_SECONDS), 1.0) * (2 ** attempt)
            backoff = min(backoff, float(runtime.OPENAI_REQUEST_RETRY_MAX_SECONDS))
            time.sleep(backoff)
    if last_error:
        raise last_error
    raise RuntimeError("OpenAI request failed without a captured error.")


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


def _read_http_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        body = exc.read()
    except Exception:
        return ""
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="replace")
    return str(body or "")


def _parse_openai_error_details(body: str) -> tuple[str, str]:
    if not body:
        return "", ""
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return "", body.strip()[:240]
    error = payload.get("error")
    if not isinstance(error, dict):
        return "", ""
    code = str(error.get("code", "") or "").strip()
    message = str(error.get("message", "") or "").strip()
    return code, message


def _format_openai_http_error(exc: urllib.error.HTTPError, error_code: str, error_message: str) -> str:
    detail = error_code or "http_error"
    if error_message:
        return f"OpenAI {detail}: {error_message}"
    return f"OpenAI {detail}: HTTP {exc.code} {getattr(exc, 'reason', '')}".strip()


def _is_openai_insufficient_quota_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    return "insufficient_quota" in text or "exceeded your current quota" in text


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
    merged["special_sections"] = list(base.get("special_sections", []))
    merged["appliances"] = _merge_appliances(list(base.get("appliances", [])), ai_appliances)
    merged["others"] = _merge_other_fields(dict(base.get("others") or {}), ai_others)
    merged["warnings"] = _merge_warning_lists(list(base.get("warnings", [])), ai_warnings)
    merged["source_documents"] = base.get("source_documents", [])
    return parsing.apply_snapshot_cleaning_rules(merged, rule_flags=rule_flags)


def _normalize_ai_result(parsed: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(parsed)
    normalized["rooms"] = _as_list_of_dicts(parsed.get("rooms"))
    normalized["special_sections"] = _as_list_of_dicts(parsed.get("special_sections"))
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
    base_identity = parsing.same_room_identity(str(base_row.get("original_room_label", "")), str(base_row.get("room_key", "")))
    best_index: int | None = None
    best_score = 0
    for index, ai_row in enumerate(ai_rows):
        if index in used_ai:
            continue
        score = 0
        ai_key = _norm(ai_row.get("room_key", ""))
        ai_label = _norm(ai_row.get("original_room_label", ""))
        ai_identity = parsing.same_room_identity(str(ai_row.get("original_room_label", "")), str(ai_row.get("room_key", "")))
        if base_identity and ai_identity and base_identity == ai_identity:
            score += 8
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
    for field_name in ("room_key", "original_room_label", "splashback", "flooring", "floating_shelf", "shelf", "feature_colour", "led", "led_note", "sink_info", "basin_info", "tap_info", "source_file", "page_refs", "evidence_snippet"):
        if not merged.get(field_name) and ai_row.get(field_name):
            merged[field_name] = ai_row[field_name]
    for field_name in ("bench_tops", "door_panel_colours", "toe_kick", "bulkheads", "handles", "accessories"):
        if stable_hybrid:
            if field_name == "accessories":
                merged[field_name] = parsing._coerce_string_list(base_row.get(field_name))
            else:
                merged[field_name] = parsing._coerce_string_list(base_row.get(field_name)) or parsing._coerce_string_list(ai_row.get(field_name))
        else:
            merged[field_name] = _merge_list_field(base_row.get(field_name), ai_row.get(field_name))
    merged["other_items"] = parsing._merge_other_items(base_row.get("other_items", []), ai_row.get("other_items", []))
    for field_name in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "feature_colour",
    ):
        if not merged.get(field_name) and ai_row.get(field_name):
            ai_value = ai_row[field_name]
            if stable_hybrid and field_name.startswith("door_colours_"):
                ai_value = parsing._clean_door_colour_value(ai_value)
            if ai_value:
                merged[field_name] = ai_value
    for field_name in ("has_explicit_overheads", "has_explicit_base", "has_explicit_tall", "has_explicit_island", "has_explicit_bar_back"):
        merged[field_name] = bool(base_row.get(field_name, False) or ai_row.get(field_name, False))
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


def _build_raw_spec_crosscheck_snapshot(
    *,
    job_no: str,
    builder_name: str,
    documents: list[dict[str, Any]],
    parser_strategy: str,
    rule_flags: Any = None,
) -> dict[str, Any]:
    snapshot = parsing.parse_documents(
        job_no=job_no,
        builder_name=builder_name,
        source_kind="spec",
        documents=documents,
        rule_flags=rule_flags,
    )
    snapshot = parsing.enrich_snapshot_rooms(snapshot, documents, rule_flags=rule_flags)
    snapshot = _stabilize_snapshot_layout(snapshot, builder_name=builder_name, parser_strategy=parser_strategy)
    snapshot = _apply_builder_specific_polish(
        snapshot,
        documents,
        builder_name=builder_name,
        parser_strategy=parser_strategy,
        rule_flags=rule_flags,
        progress_callback=None,
    )
    return snapshot


CLARENDON_RAW_SCALAR_FIELDS: tuple[str, ...] = (
    "bench_tops_wall_run",
    "bench_tops_island",
    "bench_tops_other",
    "door_colours_overheads",
    "door_colours_base",
    "door_colours_tall",
    "door_colours_island",
    "door_colours_bar_back",
    "feature_colour",
    "floating_shelf",
    "shelf",
    "sink_info",
    "basin_info",
    "tap_info",
    "drawers_soft_close",
    "hinges_soft_close",
)

CLARENDON_RAW_LIST_FIELDS: tuple[str, ...] = (
    "toe_kick",
    "bulkheads",
    "handles",
    "accessories",
)


def _crosscheck_clarendon_snapshot_with_raw(layout_snapshot: dict[str, Any], raw_snapshot: dict[str, Any]) -> dict[str, Any]:
    if not raw_snapshot:
        return layout_snapshot
    merged = dict(layout_snapshot)
    merged_rooms: list[dict[str, Any]] = []
    raw_rooms = list(raw_snapshot.get("rooms", []))
    used_raw: set[int] = set()
    for layout_row in list(layout_snapshot.get("rooms", [])):
        match_index = _find_best_room_match(layout_row, raw_rooms, used_raw)
        if match_index is None:
            merged_rooms.append(layout_row)
            continue
        used_raw.add(match_index)
        raw_row = raw_rooms[match_index]
        merged_row = dict(layout_row)
        for field_name in CLARENDON_RAW_SCALAR_FIELDS:
            merged_row[field_name] = _prefer_clarendon_raw_scalar(field_name, layout_row.get(field_name), raw_row.get(field_name))
        for field_name in CLARENDON_RAW_LIST_FIELDS:
            merged_row[field_name] = _prefer_clarendon_raw_list(field_name, layout_row.get(field_name), raw_row.get(field_name))
        merged_rooms.append(merged_row)
    merged["rooms"] = merged_rooms
    if raw_snapshot.get("site_address") and not merged.get("site_address"):
        merged["site_address"] = raw_snapshot["site_address"]
    return merged


def _prefer_clarendon_raw_scalar(field_name: str, layout_value: Any, raw_value: Any) -> Any:
    layout_text = parsing.normalize_space(str(layout_value or ""))
    raw_text = parsing.normalize_space(str(raw_value or ""))
    if not raw_text:
        return layout_value
    if not layout_text:
        return raw_value
    if _clarendon_field_quality(raw_text, field_name) > _clarendon_field_quality(layout_text, field_name):
        return raw_value
    return layout_value


def _prefer_clarendon_raw_list(field_name: str, layout_value: Any, raw_value: Any) -> list[str]:
    layout_items = parsing._coerce_string_list(layout_value)
    raw_items = parsing._coerce_string_list(raw_value)
    if not raw_items:
        return layout_items
    if not layout_items:
        return raw_items
    layout_score = max((_clarendon_field_quality(value, field_name) for value in layout_items), default=-999)
    raw_score = max((_clarendon_field_quality(value, field_name) for value in raw_items), default=-999)
    if raw_score > layout_score:
        return raw_items
    return layout_items


def _clarendon_field_quality(text: str, field_name: str) -> int:
    cleaned = parsing.normalize_space(str(text or ""))
    if not cleaned:
        return -1000
    lowered = cleaned.lower()
    score = min(len(cleaned), 180)
    for token in (
        "client",
        "date",
        "signature",
        "designer",
        "document ref",
        "colour schedule",
        "thermolaminate notes",
    ):
        if token in lowered:
            score -= 60
    if field_name.startswith("door_colours_") and "display cabinet" in lowered:
        score -= 70
    if field_name.startswith("door_colours_") and any(token in lowered for token in ("open shelves", "shelf edges", "carcass edges", "10mm door overhang")):
        score -= 90
    if field_name.startswith("bench_tops_") and any(token in lowered for token in ("sink run", "shadowline", "wfa water filter")):
        score -= 80
    if field_name == "handles" and not any(token in lowered for token in ("belluno", "salemi", "hettich")):
        score -= 40
    if field_name == "handles" and any(token in lowered for token in ("10mm door overhang", "docusign envelope id", "aedt")):
        score -= 120
    if field_name in {"sink_info", "basin_info", "tap_info"}:
        if re.fullmatch(r"(?i)(?:franke|phoenix|caroma|everhard|fienza|parisi)", cleaned):
            score -= 140
        if re.fullmatch(r"(?i)\(?\d{4,}\)?", cleaned):
            score -= 160
        if "washing machine taps" in lowered and field_name == "tap_info":
            score -= 120
        if any(token in lowered for token in ("wfa water filter", "powder floating", "no kick")):
            score -= 120
    if "profile" in lowered or re.search(r"\b\d+\s*mm\b", cleaned, re.I):
        score += 10
    return score


IMPERIAL_RAW_SCALAR_FIELDS: tuple[str, ...] = (
    "bench_tops_wall_run",
    "bench_tops_island",
    "bench_tops_other",
    "splashback",
    "door_colours_overheads",
    "door_colours_base",
    "door_colours_tall",
    "door_colours_island",
    "door_colours_bar_back",
    "feature_colour",
    "floating_shelf",
    "shelf",
    "sink_info",
    "basin_info",
    "tap_info",
    "drawers_soft_close",
    "hinges_soft_close",
    "flooring",
)

IMPERIAL_RAW_LIST_FIELDS: tuple[str, ...] = (
    "toe_kick",
    "bulkheads",
    "handles",
    "accessories",
)

IMPERIAL_LAYOUT_FIRST_SCALAR_FIELDS: tuple[str, ...] = (
    "bench_tops_wall_run",
    "bench_tops_island",
    "bench_tops_other",
    "door_colours_overheads",
    "door_colours_base",
    "door_colours_tall",
    "door_colours_island",
    "door_colours_bar_back",
    "feature_colour",
    "floating_shelf",
    "splashback",
)


def _crosscheck_imperial_snapshot_with_raw(layout_snapshot: dict[str, Any], raw_snapshot: dict[str, Any]) -> dict[str, Any]:
    if not raw_snapshot:
        return layout_snapshot
    merged = dict(layout_snapshot)
    merged_rooms: list[dict[str, Any]] = []
    raw_rooms = list(raw_snapshot.get("rooms", []))
    used_raw: set[int] = set()
    for layout_row in list(layout_snapshot.get("rooms", [])):
        match_index = _find_best_room_match(layout_row, raw_rooms, used_raw)
        if match_index is None:
            merged_rooms.append(layout_row)
            continue
        used_raw.add(match_index)
        raw_row = raw_rooms[match_index]
        merged_row = dict(layout_row)
        for field_name in IMPERIAL_RAW_SCALAR_FIELDS:
            merged_row[field_name] = _prefer_imperial_raw_scalar(field_name, layout_row.get(field_name), raw_row.get(field_name))
        for field_name in IMPERIAL_RAW_LIST_FIELDS:
            merged_row[field_name] = _prefer_imperial_raw_list(field_name, layout_row.get(field_name), raw_row.get(field_name))
        merged_row["bench_tops"] = [
            value
            for value in (
                merged_row.get("bench_tops_wall_run", ""),
                merged_row.get("bench_tops_island", ""),
                merged_row.get("bench_tops_other", ""),
            )
            if parsing.normalize_space(str(value or ""))
        ]
        merged_row["door_panel_colours"] = parsing._rebuild_door_panel_colours(merged_row)
        merged_rooms.append(merged_row)
    merged["rooms"] = merged_rooms
    return merged


def _prefer_imperial_raw_scalar(field_name: str, layout_value: Any, raw_value: Any) -> Any:
    layout_text = parsing.normalize_space(str(layout_value or ""))
    raw_text = parsing.normalize_space(str(raw_value or ""))
    if not raw_text:
        return layout_value
    if not layout_text:
        return raw_value
    layout_score = _imperial_field_quality(layout_text, field_name)
    raw_score = _imperial_field_quality(raw_text, field_name)
    if _imperial_field_looks_crosscheck_contaminated(raw_text, field_name) and not _imperial_field_looks_crosscheck_contaminated(layout_text, field_name):
        return layout_value
    if field_name.startswith("door_colours_") and _imperial_layout_scalar_looks_too_generic(field_name, layout_text):
        if raw_score > layout_score and not _imperial_field_looks_crosscheck_contaminated(raw_text, field_name):
            return raw_value
    if field_name == "splashback" and re.search(r"(?i)\btiles?\s+by\b", raw_text):
        if (
            "tile" not in layout_text.lower()
            or "grout" in layout_text.lower()
            or len(raw_text) + 18 < len(layout_text)
        ) and not _imperial_field_looks_crosscheck_contaminated(raw_text, field_name):
            return raw_value
    if field_name.startswith("bench_tops_") and raw_score > layout_score and not _imperial_field_looks_crosscheck_contaminated(raw_text, field_name):
        return raw_value
    if field_name in IMPERIAL_LAYOUT_FIRST_SCALAR_FIELDS and not _imperial_field_looks_crosscheck_contaminated(layout_text, field_name):
        if raw_score <= layout_score + 25:
            return layout_value
    if field_name.startswith("bench_tops_"):
        layout_dedup = parsing._dedupe_delimited_fragments(layout_text)
        raw_dedup = parsing._dedupe_delimited_fragments(raw_text)
        if layout_dedup and raw_dedup and layout_dedup == raw_dedup and layout_text != raw_text:
            return layout_value
    if raw_score > layout_score:
        return raw_value
    return layout_value


def _prefer_imperial_raw_list(field_name: str, layout_value: Any, raw_value: Any) -> list[str]:
    layout_items = parsing._coerce_string_list(layout_value)
    raw_items = parsing._coerce_string_list(raw_value)
    if not raw_items:
        return layout_items
    if not layout_items:
        return raw_items
    if field_name == "bulkheads":
        cleaned_raw = [parsing.normalize_space(str(item or "")) for item in raw_items if parsing.normalize_space(str(item or ""))]
        if cleaned_raw and all(re.fullmatch(r"(?i)(?:n/?a\s+)?open above|n/?a|na|tbc", item) for item in cleaned_raw):
            return layout_items
    if field_name == "handles":
        layout_scoped = sum(1 for item in layout_items if re.match(r"(?i)^(?:base drawers|base doors|overheads|tall \+ pantry drawers|doors / drawers|drawers|tall cabinets|base cabinets|knob):", parsing.normalize_space(str(item or ""))))
        raw_scoped = sum(1 for item in raw_items if re.match(r"(?i)^(?:base drawers|base doors|overheads|tall \+ pantry drawers|doors / drawers|drawers|tall cabinets|base cabinets|knob):", parsing.normalize_space(str(item or ""))))
        if layout_scoped and layout_scoped >= raw_scoped:
            return layout_items
        if any(_imperial_handle_entry_looks_compound(item) for item in layout_items) and raw_items:
            return raw_items
    if field_name == "toe_kick":
        if any(re.search(r"(?i)\bmatch above\b", str(item or "")) for item in layout_items):
            return layout_items
        if any(
            any(token in str(item or "").lower() for token in ("cabinetry colour", "mirrored shaving cabinet", "external panels only"))
            for item in layout_items
        ):
            return raw_items
    layout_score = max((_imperial_field_quality(value, field_name) for value in layout_items), default=-999)
    raw_score = max((_imperial_field_quality(value, field_name) for value in raw_items), default=-999)
    layout_total = sum(_imperial_field_quality(value, field_name) for value in layout_items)
    raw_total = sum(_imperial_field_quality(value, field_name) for value in raw_items)
    if field_name == "handles":
        raw_noise = sum(1 for value in raw_items if _imperial_field_quality(value, field_name) < 0)
        layout_noise = sum(1 for value in layout_items if _imperial_field_quality(value, field_name) < 0)
        if layout_total >= raw_total and layout_noise <= raw_noise:
            return layout_items
    if raw_score > layout_score or raw_total > layout_total + 25:
        return raw_items
    return layout_items


def _imperial_handle_entry_looks_compound(value: Any) -> bool:
    lowered = parsing.normalize_space(str(value or "")).lower()
    if not lowered:
        return False
    role_markers = (
        "base-",
        "base -",
        "upper -",
        "upper-",
        "tall -",
        "tall-",
        "pto",
    )
    return sum(marker in lowered for marker in role_markers) >= 2


def _imperial_field_quality(text: str, field_name: str) -> int:
    cleaned = parsing.normalize_space(str(text or ""))
    if not cleaned:
        return -1000
    lowered = cleaned.lower()
    score = min(len(cleaned), 180)
    deduped_fragments = parsing._dedupe_delimited_fragments(cleaned)
    if "|" in cleaned and deduped_fragments and deduped_fragments != cleaned:
        score -= 90
    for token in (
        "client",
        "date",
        "signature",
        "designer",
        "document ref",
        "private",
        "ceiling height",
        "cabinetry height",
        "joinery selection sheet",
        "all colours shown",
        "product availability",
        "signed date",
    ):
        if token in lowered:
            score -= 70
    if field_name.startswith("door_colours_") and any(token in lowered for token in ("handle", "knob", "pull", "part number", "so-")):
        score -= 140
    if field_name.startswith("door_colours_") and any(token in lowered for token in ("bulkhead:", "cornicing", "90mm")):
        score -= 120
    if field_name.startswith("door_colours_") and any(
        token in lowered for token in ("desk grommet", "desk grommets", "cable entry covers", "furnware", "open shelves", "open shelf")
    ):
        score -= 180
    if field_name.startswith("door_colours_") and any(
        token in lowered
        for token in (
            "floating shelving colour",
            "mirrored shaving cabinet",
            "external panels only",
            "kickboard",
            "as doors",
        )
    ):
        score -= 120
    if field_name.startswith("door_colours_") and any(
        token in lowered
        for token in (
            "caesarstone",
            "smartstone",
            "wk stone",
            "stone",
            "benchtop",
            "pencil round",
            "waterfall",
            "undermount sink",
            "to bar back only",
        )
    ):
        score -= 180
    if field_name.startswith("bench_tops_") and any(token in lowered for token in ("base cabinetry", "upper cabinetry", "kickboard", "handle")):
        score -= 140
    if field_name.startswith("bench_tops_") and any(
        token in lowered for token in ("feature colour overheads", "feature tall cabinetry", "open shelves", "open shelf")
    ):
        score -= 220
    if field_name.startswith("bench_tops_") and any(token in lowered for token in ("lighting", "shadowline", "splashback", "ceiling height", "cabinetry height")):
        score -= 160
    if field_name == "bench_tops_wall_run" and any(token in lowered for token in ("wfall", "waterfall")):
        score -= 180
    if field_name == "bench_tops_wall_run" and re.search(r"\b\d+\s*x\b", cleaned, re.I):
        score -= 40
    if field_name == "bench_tops_wall_run" and re.fullmatch(r"(?i)\d{3,4}\s*mm", cleaned):
        score -= 220
    if field_name in {"sink_info", "tap_info", "basin_info"} and any(token in lowered for token in ("client", "date", "signature", "designer", "document ref", "private")):
        score -= 160
    if field_name in {"toe_kick", "bulkheads"} and any(token in lowered for token in ("soft close", "floor type", "handle", "benchtop")):
        score -= 140
    if field_name == "door_colours_bar_back" and "note:" in lowered:
        score -= 140
    if field_name == "floating_shelf" and re.fullmatch(r"(?i)(?:\d+\s*mm\s+)?polytec\s*-\s*woodmatt", cleaned.strip(" -;,")):
        score -= 120
    if field_name == "floating_shelf" and "floating shelves" in lowered and "tasmanian oak" not in lowered:
        score -= 80
    if field_name == "splashback" and lowered in {"stone", "polytec"}:
        score -= 180
    if field_name == "splashback" and any(token in lowered for token in ("shadowline", "base cabinetry colour", "document ref", "notes supplier")):
        score -= 180
    if field_name == "toe_kick" and any(token in lowered for token in ("cabinetry colour", "mirrored shaving cabinet", "external panels only")):
        score -= 160
    if field_name == "handles" and any(token in lowered for token in ("base cabinetry colour", "upper cabinetry colour", "benchtop", "kickboard")):
        score -= 140
    if field_name == "handles" and any(token in lowered for token in ("no handles", "touch catch", "pto where req")):
        score -= 180
    if field_name == "handles" and any(token in lowered for token in ("desk grommet", "cable entry", "furnware")):
        score -= 180
    if field_name == "handles" and _imperial_handle_entry_looks_compound(cleaned):
        score -= 180
    if field_name == "flooring" and any(token in lowered for token in ("polytec", "laminex", "caesarstone", "ceiling height", "cabinetry height")):
        score -= 120
    if "part number" in lowered or "so-" in lowered:
        score += 15
    if re.search(r"\b\d{2,4}mm\b", cleaned, re.I):
        score += 10
    return score


def _imperial_field_looks_crosscheck_contaminated(text: str, field_name: str) -> bool:
    lowered = parsing.normalize_space(str(text or "")).lower()
    if not lowered:
        return False
    if field_name == "sink_info":
        if ("cr340db" in lowered and "lt120" in lowered) or ("kitchen" in lowered and "laundry" in lowered):
            return True
    if field_name == "tap_info":
        if "kitchen" in lowered and "laundry" in lowered:
            return True
    if field_name.startswith("door_colours_"):
        return lowered in {"polytec", "laminex", "caesarstone", "smartstone", "wk stone"} or bool(
            re.fullmatch(r"\d{2,4}\s*mm", lowered)
        ) or any(
            token in lowered
            for token in (
                "caesarstone",
                "smartstone",
                "wk stone",
                "pencil round",
                "waterfall",
                "undermount sink",
                "to bar back only",
                "feature bar back",
                "desk grommet",
                "desk grommets",
                "cable entry covers",
                "cable entry",
                "furnware",
                "open shelves",
                "open shelf",
            )
        )
    if field_name.startswith("bench_tops_"):
        return bool(
            re.fullmatch(r"\d{3,4}\s*mm", lowered)
            or any(
                token in lowered
                for token in (
                    "base cabinetry",
                    "upper cabinetry",
                    "overhead cabinetry",
                    "lighting",
                    "shadowline",
                    "ceiling height",
                    "cabinetry height",
                )
            )
        )
    if field_name == "floating_shelf":
        return lowered in {"polytec - woodmatt", "woodmatt", "polytec"} or bool(
            re.search(r"(?i)\b(?:kethy|allegra|furnware|darwen|handle|horizontal|vertical)\b", lowered)
        )
    if field_name == "splashback":
        return lowered in {"stone", "polytec"} or any(token in lowered for token in ("shadowline", "document ref", "notes supplier"))
    return False


def _imperial_layout_scalar_looks_too_generic(field_name: str, text: str) -> bool:
    cleaned = parsing.normalize_space(str(text or ""))
    lowered = cleaned.lower()
    if not cleaned:
        return True
    if field_name.startswith("door_colours_"):
        return bool(
            re.fullmatch(r"(?i)(?:polytec|laminex)(?:\s*-\s*)?(?:flat door|door|cabinetry colour)?", cleaned)
            or re.fullmatch(r"(?i)(?:flat door|door|matt|smooth)", cleaned)
        )
    return False


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


def _normalize_confidence_value(value: Any) -> float:
    if isinstance(value, str):
        normalized = parsing.normalize_space(value).lower()
        if normalized in {"high", "strong"}:
            return 0.9
        if normalized in {"medium", "moderate"}:
            return 0.65
        if normalized in {"low", "weak"}:
            return 0.35
    return _safe_float(value)


def _coerce_provenance_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (list, tuple)):
        pairs = [item for item in value if isinstance(item, (list, tuple)) and len(item) == 2]
        if pairs and len(pairs) == len(value):
            return {str(key): item_value for key, item_value in pairs}
        items: list[Any] = []
        for item in value:
            if isinstance(item, dict):
                items.append(dict(item))
            elif isinstance(item, (str, int, float, bool)) or item is None:
                items.append(item)
            else:
                items.append(str(item))
        return {"items": items}
    if isinstance(value, str):
        normalized = parsing.normalize_space(value)
        return {"raw": normalized} if normalized else {}
    if isinstance(value, (int, float, bool)):
        return {"raw": value}
    return {}


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _truncate_note(value: Exception) -> str:
    return str(value).strip().replace("\n", " ")[:180]


def _preview_output(value: str) -> str:
    return value.strip().replace("\n", " ")[:120]


def _report_progress(progress_callback: ProgressCallback, stage: str, message: str) -> None:
    if progress_callback:
        progress_callback(stage, message)


CLARENDON_SCHEDULE_PAGE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bKITCHEN COLOUR SCHEDULE\b", "kitchen"),
    (r"\bBUTLERS?\s+PANTRY COLOUR SCHEDULE\b", "butlers_pantry"),
    (r"\bVANITIES COLOUR SCHEDULE\b", "vanities"),
    (r"\bLAUNDRY COLOUR SCHEDULE\b", "laundry"),
    (r"\bTHEATRE(?: ROOM)? COLOUR SCHEDULE\b", "theatre"),
    (r"\bRUMPUS(?: ROOM)? COLOUR SCHEDULE\b", "rumpus_room"),
    (r"\bRUMPUS\s*-\s*DESK JOINERY COLOUR SCHEDULE\b", "rumpus_desk"),
    (r"\bSTUDY COLOUR SCHEDULE\b", "study"),
    (r"\bOFFICE COLOUR SCHEDULE\b", "office"),
    (r"\bKITCHENETTE COLOUR SCHEDULE\b", "kitchenette"),
)

CLARENDON_FIELD_STOP_MARKERS = (
    r"BENCHTOP(?: COLOUR \d+)?\s*-",
    r"BENCHTOPS?\s*-",
    r"DOOR COLOUR(?: \d+)?\s*-",
    r"DOOR/PANEL COLOUR(?: \d+)?\s*-",
    r"DOORS?/PANELS?\s*-",
    r"(?:MIRROR\s+)?SPLASHBACK\s*-",
    r"KICKBOARDS?\s*-",
    r"SQUARE EDGE RAILS\s*-",
    r"BULKHEAD SHADOWLINE\s*-",
    r"THERMOLAMINATE NOTES",
    r"CARCASS",
    r"STANDARD WHITE",
    r"PLAIN GLASS DISPLAY CABINET",
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
    if "imperial" in builder_name.strip().lower():
        return _apply_imperial_row_polish(
            snapshot,
            documents,
            builder_name=builder_name,
            parser_strategy=parser_strategy,
            rule_flags=rule_flags,
            progress_callback=progress_callback,
        )
    if "clarendon" in builder_name.strip().lower():
        return _apply_clarendon_reference_polish(
            snapshot,
            documents,
            builder_name=builder_name,
            parser_strategy=parser_strategy,
            rule_flags=rule_flags,
            progress_callback=progress_callback,
        )
    return _apply_shared_layout_row_polish(
        snapshot,
        documents,
        builder_name=builder_name,
        parser_strategy=parser_strategy,
        rule_flags=rule_flags,
        progress_callback=progress_callback,
    )


GENERIC_LAYOUT_PROPERTY_MAP: dict[str, str] = {
    "manufacturer": "manufacturer",
    "range": "range",
    "model": "model",
    "colour": "colour",
    "island colour": "island_colour",
    "colour & finish": "colour",
    "finish": "finish",
    "profile": "profile",
    "edge profile": "profile",
    "island edge profile": "island_profile",
    "style": "style",
    "type": "type",
    "location": "location",
    "fixing": "fixing",
    "category": "category",
    "mechanism": "mechanism",
    "underlay": "note",
    "waterfall end to island": "note",
    "handles": "model",
    "cabinetry handles": "model",
    "overhead cabinetry handles": "model",
    "door handle": "door handle",
    "drawer handle": "drawer handle",
    "pantry door handle": "pantry door handle",
    "bin & pot drawers handle": "bin & pot drawers handle",
    "standard": "note",
    "pot": "note",
    "bin": "note",
    "integrated appliances": "note",
    "range hood": "note",
    "rangehood": "note",
    "cooktop": "note",
    "oven": "note",
}

GENERIC_INLINE_PROPERTY_LABELS: tuple[str, ...] = tuple(
    sorted(GENERIC_LAYOUT_PROPERTY_MAP.keys(), key=len, reverse=True)
)

GENERIC_LAYOUT_ANCHOR_LABELS: set[str] = {
    "benchtop",
    "benchtops",
    "bench tops",
    "wall run benchtop",
    "island/penisula benchtop",
    "underbench",
    "underbench including island",
    "base cabinet panels",
    "wall run base cabinet panels",
    "island/penisula base cabinet panels",
    "island/penisula feature panels",
    "waterfall end panels",
    "contrasting facings",
    "overhead cupboards",
    "overheads",
    "pantry doors",
    "integrated appliances",
    "tall panel",
    "cabinet panels",
    "shadowline",
    "kickboard",
    "wall run kickboard",
    "island/penisula kickboard",
    "handles",
    "base cabinetry handles",
    "cabinetry handles",
    "overhead cabinetry handles",
    "shelving",
    "floating shelves",
    "floating shelf",
    "drawers",
    "contrasting facings",
    "sink",
    "sink mixer",
    "pantry sink",
    "pantry tapware",
    "kitchen sink",
    "kitchen tapware",
    "laundry trough",
    "laundry tapware",
    "vanity basin",
    "vanity basin tapware",
    "feature waste",
    "tub",
    "tub mixer",
    "basin",
    "basin mixer",
    "bath",
    "bath mixer / spout",
    "shower",
    "shower base",
    "shower frame",
    "shower mixer",
    "shower rose",
    "accessories",
    "accessories & toilet suite",
    "toilet suite",
    "toilet roll holder",
    "robe hook",
    "hand towel rail",
    "towel rail",
    "floor waste",
    "mirror",
    "range hood",
    "rangehood",
    "cooktop",
    "oven",
    "selection required",
    "robe fitout",
    "robe hanging rail",
    "hanging rail",
    "hinges & drawer runners",
    "floor type & kick refacing required",
    "gpo's",
    "hamper",
}


GENERIC_LAYOUT_FUTURE_PREFIX_ANCHOR_KINDS: set[str] = {
    "handles",
    "accessories",
    "other",
    "soft_close",
    "flooring",
    "sink",
    "tap",
    "basin",
}

GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS: set[str] = {
    "manufacturer",
    "range",
    "model",
    "colour",
    "colour & finish",
    "finish",
    "profile",
    "edge profile",
    "island edge profile",
    "style",
    "type",
    "location",
    "fixing",
    "category",
    "mechanism",
    "underlay",
    "waterfall end to island",
}

GENERIC_COMPOUND_PROPERTY_LABELS: set[str] = set(GENERIC_LAYOUT_PROPERTY_MAP) | {
    "drawers",
    "shaving cabinets",
}

GENERIC_LAYOUT_CURRENT_ATTACHMENT_LABELS: dict[str, set[str]] = {
    "bench": {
        "manufacturer",
        "range",
        "colour",
        "island colour",
        "colour & finish",
        "finish",
        "profile",
        "edge profile",
        "island edge profile",
        "style",
        "type",
        "waterfall end to island",
    },
    "base": {"manufacturer", "range", "colour", "colour & finish", "finish", "style", "type"},
    "overheads": {"manufacturer", "range", "colour", "colour & finish", "finish", "style", "type"},
    "tall": {"manufacturer", "range", "colour", "colour & finish", "finish", "style", "type"},
    "toe_kick": {"manufacturer", "range", "colour", "colour & finish", "finish", "style", "type"},
    "floating_shelf": {"manufacturer", "range", "colour", "colour & finish", "finish", "profile", "style", "type"},
    "handles": {"manufacturer", "range", "model", "style", "profile", "fixing", "category", "mechanism", "type", "finish"},
    "accessories": {"manufacturer", "range", "model", "style", "profile", "fixing", "category", "mechanism", "type", "finish", "location"},
    "sink": {"manufacturer", "range", "model", "style", "profile", "type", "finish", "location", "accessories"},
    "tap": {"manufacturer", "range", "model", "style", "profile", "type", "finish", "location"},
    "basin": {"manufacturer", "range", "model", "style", "profile", "type", "finish", "location"},
    "soft_close": {"manufacturer", "range", "model", "style", "profile", "fixing", "category", "mechanism", "type", "finish", "location"},
    "flooring": {"manufacturer", "range", "model", "style", "profile", "fixing", "category", "mechanism", "type", "finish", "location"},
    "other": {"manufacturer", "range", "model", "style", "profile", "type", "finish", "location"},
}


def _apply_shared_layout_row_polish(
    snapshot: dict[str, Any],
    documents: list[dict[str, object]],
    builder_name: str,
    parser_strategy: str,
    rule_flags: Any = None,
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    if parser_strategy not in {"stable_hybrid", cleaning_rules.global_parser_strategy()}:
        return snapshot
    _report_progress(progress_callback, "layout_row_polish", f"Rebuilding {builder_name} room rows from shared layout blocks")
    overlays: dict[str, dict[str, Any]] = {}
    for document in documents:
        for section in parsing._collect_room_sections_for_document(document):
            if not section.get("layout_rows"):
                continue
            overlay = _extract_generic_layout_overlay(section, documents=documents)
            if not overlay:
                continue
            room_keys = {
                parsing.same_room_identity(str(section.get("original_section_label", "")), str(section.get("section_key", ""))),
                parsing.same_room_identity(str(overlay.get("original_room_label", "")), str(section.get("section_key", ""))),
            }
            for room_key in room_keys:
                if not room_key:
                    continue
                candidate_label = str(overlay.get("original_room_label", section.get("original_section_label", "")) or room_key.replace("_", " "))
                if parsing._looks_like_spec_room_label_noise(candidate_label):
                    continue
                if room_key not in overlays:
                    overlays[room_key] = overlay
                else:
                    overlays[room_key] = _merge_generic_layout_overlay(overlays[room_key], overlay)
    if not overlays:
        return snapshot
    polished_rooms: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for room in snapshot.get("rooms", []):
        if not isinstance(room, dict):
            continue
        room_key = parsing.same_room_identity(str(room.get("original_room_label", "")), str(room.get("room_key", "")))
        overlay = overlays.get(room_key, {})
        seen_keys.add(room_key)
        if overlay.get("original_room_label"):
            seen_keys.add(parsing.same_room_identity(str(overlay.get("original_room_label", "")), room_key))
        polished_rooms.append(_polish_generic_layout_room(dict(room), overlay))
    for room_key, overlay in overlays.items():
        if room_key in seen_keys:
            continue
        overlay_label = str(overlay.get("original_room_label", room_key.replace("_", " ").title()) or room_key)
        if parsing._looks_like_spec_room_label_noise(overlay_label):
            continue
        missing_room = {
            "room_key": room_key,
            "original_room_label": overlay_label,
        }
        polished_rooms.append(_polish_generic_layout_room(missing_room, overlay))
    resolved_rooms: list[dict[str, Any]] = []
    for room in polished_rooms:
        current = dict(room)
        resolved_label = _resolve_generic_room_label_from_documents(current, documents)
        if resolved_label:
            current["original_room_label"] = resolved_label
            current["room_key"] = parsing.same_room_identity(resolved_label, str(current.get("room_key", "")))
        if parsing._looks_like_spec_room_label_noise(str(current.get("original_room_label", "") or current.get("room_key", ""))):
            continue
        resolved_rooms.append(current)
    resolved_keys = {
        parsing.same_room_identity(str(room.get("original_room_label", "") or ""), str(room.get("room_key", "")))
        for room in resolved_rooms
        if isinstance(room, dict)
    }
    for room in snapshot.get("rooms", []):
        if not isinstance(room, dict):
            continue
        room_key = parsing.same_room_identity(str(room.get("original_room_label", "") or ""), str(room.get("room_key", "")))
        if not room_key or room_key in resolved_keys:
            continue
        if parsing._looks_like_spec_room_label_noise(str(room.get("original_room_label", "") or room_key)):
            continue
        resolved_rooms.append(dict(room))
        resolved_keys.add(room_key)
    polished = dict(snapshot)
    polished["rooms"] = _merge_rooms_by_source_identity(resolved_rooms)
    polished = parsing.apply_snapshot_cleaning_rules(polished, rule_flags=rule_flags)
    return parsing.enrich_snapshot_rooms(polished, documents, rule_flags=rule_flags)


def _blank_generic_layout_overlay() -> dict[str, Any]:
    return {
        "original_room_label": "",
        "bench_tops_wall_run": "",
        "bench_tops_island": "",
        "bench_tops_other": "",
        "has_bench_block": False,
        "door_colours_base": "",
        "door_colours_overheads": "",
        "door_colours_tall": "",
        "door_colours_island": "",
        "door_colours_bar_back": "",
        "feature_colour": "",
        "has_explicit_base": False,
        "has_explicit_overheads": False,
        "has_explicit_tall": False,
        "has_explicit_island": False,
        "has_explicit_bar_back": False,
        "toe_kick": [],
        "handles": [],
        "floating_shelf": "",
        "shelf": "",
        "has_floating_shelf_block": False,
        "sink_info": "",
        "tap_info": "",
        "basin_info": "",
        "has_sink_block": False,
        "has_tap_block": False,
        "has_basin_block": False,
        "has_handles_block": False,
        "has_accessories_block": False,
        "has_flooring_block": False,
        "accessories": [],
        "other_items": [],
        "drawers_soft_close": "",
        "hinges_soft_close": "",
        "flooring": "",
        "source_file": "",
        "page_refs": "",
        "evidence_snippet": "",
    }


def _merge_generic_layout_overlay(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left)
    field_kinds = {
        "original_room_label": "other",
        "bench_tops_wall_run": "material",
        "bench_tops_island": "material",
        "bench_tops_other": "material",
        "door_colours_base": "material",
        "door_colours_overheads": "material",
        "door_colours_tall": "material",
        "door_colours_island": "material",
        "door_colours_bar_back": "material",
        "feature_colour": "material",
        "floating_shelf": "material",
        "shelf": "material",
        "sink_info": "fixture",
        "tap_info": "fixture",
        "basin_info": "fixture",
        "drawers_soft_close": "other",
        "hinges_soft_close": "other",
        "flooring": "other",
        "source_file": "other",
        "page_refs": "other",
        "evidence_snippet": "other",
    }
    for field_name, field_kind in field_kinds.items():
        merged[field_name] = _prefer_generic_overlay_value(
            str(merged.get(field_name, "") or ""),
            str(right.get(field_name, "") or ""),
            field=field_kind,
        )
    merged["toe_kick"] = _merge_list_field(merged.get("toe_kick", []), right.get("toe_kick", []))
    merged["handles"] = _merge_list_field(merged.get("handles", []), right.get("handles", []))
    merged["accessories"] = _merge_list_field(merged.get("accessories", []), right.get("accessories", []))
    merged["other_items"] = parsing._merge_other_items(merged.get("other_items", []), right.get("other_items", []))
    for field_name in (
        "has_bench_block",
        "has_sink_block",
        "has_tap_block",
        "has_basin_block",
        "has_handles_block",
        "has_accessories_block",
        "has_floating_shelf_block",
        "has_flooring_block",
        "has_explicit_base",
        "has_explicit_overheads",
        "has_explicit_tall",
        "has_explicit_island",
        "has_explicit_bar_back",
    ):
        merged[field_name] = bool(merged.get(field_name) or right.get(field_name))
    return merged


def _normalize_generic_row_label(value: str) -> str:
    return parsing.normalize_space(str(value or "")).strip(" -").lower()


def _split_compound_generic_row_label(label: str) -> tuple[str, str]:
    normalized = _normalize_generic_row_label(label)
    if not normalized or normalized in GENERIC_LAYOUT_PROPERTY_MAP or normalized in GENERIC_LAYOUT_ANCHOR_LABELS:
        return "", ""
    for anchor_label in sorted(GENERIC_LAYOUT_ANCHOR_LABELS, key=len, reverse=True):
        anchor_normalized = _normalize_generic_row_label(anchor_label)
        if not anchor_normalized or normalized == anchor_normalized:
            continue
        remainder = ""
        for separator in (" - ", " "):
            prefix = f"{anchor_normalized}{separator}"
            if normalized.startswith(prefix):
                remainder = normalized[len(prefix) :].strip(" -")
                break
        if remainder and remainder in GENERIC_COMPOUND_PROPERTY_LABELS:
            return parsing.normalize_space(anchor_label).title(), remainder
    return "", ""


def _looks_like_grouped_generic_anchor_label(label: str) -> bool:
    normalized = _normalize_generic_row_label(label)
    if not normalized:
        return False
    if normalized in GENERIC_LAYOUT_ANCHOR_LABELS:
        return True
    return any(
        token in normalized
        for token in (
            "benchtop",
            "underbench",
            "cabinet panels",
            "overhead cupboards",
            "overheads",
            "pantry doors",
            "drawers",
            "contrasting facings",
            "integrated appliances",
            "kickboard",
            "handles",
            "floating shelf",
            "shelving",
        )
    )


def _looks_like_grouped_generic_follower_label(label: str) -> bool:
    normalized = _normalize_generic_row_label(label)
    if not normalized:
        return False
    if normalized in GENERIC_LAYOUT_PROPERTY_MAP:
        return True
    return normalized in {
        "door handle",
        "drawer handle",
        "pantry door handle",
        "bin & pot drawers handle",
        "dishwasher",
        "fridge",
        "kickboard",
    } or bool(_match_layout_row_label(label)[0])


def _row_region_text(row: dict[str, Any], *names: str) -> str:
    for name in names:
        value = parsing.normalize_space(str(row.get(name, "") or ""))
        if value:
            return value
    return ""


def _generic_anchor_signal(row: dict[str, Any]) -> str:
    label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
    value = _normalize_generic_row_label(_row_region_text(row, "value_text", "value_region_text"))
    return " ".join(part for part in (label, value) if part).strip()


def _is_generic_anchor_row(row: dict[str, Any]) -> bool:
    raw_label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
    label = _generic_anchor_signal(row)
    row_kind = parsing.normalize_space(str(row.get("row_kind", "") or "")).lower().replace(" ", "_")
    if parsing._is_blacklisted_wet_area_label(raw_label) or parsing._is_blacklisted_wet_area_label(label):
        return False
    if row_kind in {"sink", "tap", "basin"} and raw_label not in GENERIC_LAYOUT_PROPERTY_MAP:
        return True
    if not label:
        return False
    if _layout_row_has_header_noise(row):
        return False
    if raw_label in GENERIC_LAYOUT_ANCHOR_LABELS:
        return True
    if raw_label in GENERIC_LAYOUT_PROPERTY_MAP:
        return False
    if label in GENERIC_LAYOUT_PROPERTY_MAP:
        return False
    if len(label.split()) > 10:
        return False
    return any(
        token in label
        for token in (
            "benchtop",
            "underbench",
            "cabinet panels",
            "overhead cupboards",
            "overheads",
            "pantry doors",
            "integrated appliances",
            "drawers",
            "tall",
            "kickboard",
            "handles",
            "shelving",
            "floating shelf",
            "sink",
            "tapware",
            "mixer",
            "spout",
            "shower rail",
            "shower rose",
            "shower screen",
            "basin",
            "tub",
            "bath",
            "mirror",
            "accessories",
            "robe hook",
            "towel rail",
            "toilet suite",
            "toilet roll holder",
            "hanging rail",
        )
    )


def _is_generic_empty_property_row(row: dict[str, Any]) -> bool:
    label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
    if label not in GENERIC_LAYOUT_PROPERTY_MAP:
        return False
    value_text = _clean_generic_fragment(_row_region_text(row, "value_text", "value_region_text"))
    supplier_text = _clean_generic_fragment(_row_region_text(row, "supplier_text", "supplier_region_text"))
    notes_text = _clean_generic_fragment(_row_region_text(row, "notes_text", "notes_region_text"))
    if supplier_text or notes_text:
        return False
    stripped = _strip_generic_property_prefix(value_text, label)
    stripped = re.sub(r"(?i)^&\s*finish\b", "", stripped)
    stripped = parsing.normalize_space(stripped).strip(" -;,")
    return not stripped or stripped in {"&", "finish", "& finish"}


def _is_generic_layout_noise_row(row: dict[str, Any]) -> bool:
    label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
    text = _row_fragment_text(row).lower()
    if parsing._is_blacklisted_wet_area_label(label):
        return True
    if _is_generic_anchor_row(row):
        return False
    if not text:
        return True
    if _is_generic_empty_property_row(row):
        return True
    if _layout_row_has_header_noise(row):
        return True
    if any(
        token in text
        for token in (
            "all cabinets include soft close",
            "benchtops over maximum length",
            "additional $350 charge",
            "one stone colour included",
            "client initials",
            "supplier description design comments",
            "colour selections framework",
            "item selection level",
        )
    ):
        return True
    if label in {"", "-"} and any(
        token in text
        for token in (
            "all cabinets include",
            "benchtops over maximum length",
            "additional $350 charge",
        )
    ):
        return True
    return False


def _classify_generic_anchor(row: dict[str, Any], page_type: str = "") -> str:
    raw_label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
    label = _generic_anchor_signal(row)
    row_kind = parsing.normalize_space(str(row.get("row_kind", "") or "")).lower().replace(" ", "_")
    if parsing._is_blacklisted_wet_area_label(raw_label) or parsing._is_blacklisted_wet_area_label(label):
        return "metadata"
    if "hinges & drawer runners" in raw_label:
        return "soft_close"
    if "floor type & kick refacing required" in raw_label or raw_label == "flooring":
        return "flooring"
    if "benchtop" in raw_label:
        return "bench"
    if ("island" in raw_label or "penisula" in raw_label or "peninsula" in raw_label) and any(
        token in raw_label for token in ("base cabinet panels", "cabinet panels")
    ):
        return "island"
    if "waterfall end" in raw_label:
        return "other"
    if "feature panels" in raw_label:
        return "other"
    if any(token in raw_label for token in ("underbench", "base cabinet panels", "cabinet panels")):
        return "base"
    if "overhead cupboards" in raw_label or raw_label == "overheads" or "shaving cabinets" in raw_label:
        return "overheads"
    if "tall" in raw_label or "pantry doors" in raw_label:
        return "tall"
    if "kickboard" in raw_label:
        return "toe_kick"
    if "gpo" in raw_label or "hamper" in raw_label:
        return "accessories"
    if raw_label in {"range hood", "rangehood", "cooktop", "oven"}:
        return "other"
    if any(token in raw_label for token in ("drawers", "contrasting facings", "selection required", "robe fitout", "robe hanging rail")):
        return "other"
    if "handles" in raw_label or raw_label == "handle":
        return "handles"
    if any(token in raw_label for token in ("shelving", "floating shelf")):
        return "floating_shelf"
    if page_type == "sinkware_tapware":
        if any(
            token in raw_label
            for token in (
                "vanity basin tapware",
                "sink mixer",
                "tub mixer",
                "basin mixer",
                "tapware",
                "tap",
            )
        ):
            return "tap"
        if any(
            token in raw_label
            for token in (
                "bath mixer / spout",
                "bath spout model",
                "shower mixer",
                "shower rose",
                "shower rail",
                "shower screen",
                "feature waste",
                "floor waste",
                "bath",
            )
        ):
            return "other"
        if "basin" in raw_label:
            return "basin"
        if any(token in raw_label for token in ("sink", "tub", "trough")):
            return "sink"
        if any(
            token in raw_label
            for token in (
                "accessories",
                "accessories & toilet suite",
                "toilet suite",
                "toilet roll holder",
                "robe hook",
                "hand towel rail",
                "towel rail",
            )
        ):
            return "accessories"
        if "mirror" in raw_label:
            return "other"
        return "other"
    if "benchtop" in label:
        return "bench"
    if ("island" in label or "penisula" in label or "peninsula" in label) and any(
        token in label for token in ("base cabinet panels", "cabinet panels")
    ):
        return "island"
    if any(token in label for token in ("underbench", "base cabinet panels", "cabinet panels")):
        return "base"
    if "overhead cupboards" in label or "overheads" in label or "shaving cabinets" in label:
        return "overheads"
    if "tall" in label or "pantry doors" in label:
        return "tall"
    if "kickboard" in label:
        return "toe_kick"
    if "handles" in label or label == "handle":
        return "handles"
    if any(token in label for token in ("shelving", "floating shelf")):
        return "floating_shelf"
    if row_kind in {"sink", "tap", "basin"} and raw_label not in GENERIC_LAYOUT_PROPERTY_MAP:
        return row_kind
    if "tapware" in label or "mixer" in label or "spout" in label:
        return "tap"
    if "basin" in label:
        return "basin"
    if any(token in label for token in ("sink", "tub", "bath")):
        return "sink"
    if any(token in label for token in ("accessories", "robe hook", "towel rail", "toilet suite", "toilet roll holder")):
        return "accessories"
    if "mirror" in label or "hanging rail" in label:
        return "other"
    return "other"


def _row_fragment_text(row: dict[str, Any]) -> str:
    return parsing.normalize_space(
        " ".join(
            part
            for part in (
                _row_region_text(row, "value_text", "value_region_text"),
                _row_region_text(row, "supplier_text", "supplier_region_text"),
                _row_region_text(row, "notes_text", "notes_region_text"),
            )
            if parsing.normalize_space(part)
        )
    )


def _generic_section_prefers_prefix_anchors(layout_rows: list[dict[str, Any]]) -> bool:
    leading_property_count = 0
    for row in layout_rows[:8]:
        if not isinstance(row, dict):
            continue
        if _is_generic_anchor_row(row):
            return leading_property_count >= 2
        label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
        if label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS:
            leading_property_count += 1
    return False


def _row_should_attach_to_current_anchor(current_block: dict[str, Any], row: dict[str, Any], page_type: str) -> bool:
    anchor_kind = str(current_block.get("anchor_kind", "") or "")
    label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
    if not label:
        return False
    if page_type == "sinkware_tapware":
        return True
    allowed = GENERIC_LAYOUT_CURRENT_ATTACHMENT_LABELS.get(anchor_kind, set())
    if label in allowed:
        if label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS:
            existing_labels = {
                _normalize_generic_row_label(str(existing.get("row_label", "") or ""))
                for existing in current_block.get("rows", [])
                if isinstance(existing, dict)
            }
            if label in existing_labels:
                return False
        return True
    return False


def _has_prefix_properties(block: dict[str, Any]) -> bool:
    for existing in block.get("rows", []):
        if not isinstance(existing, dict):
            continue
        label = _normalize_generic_row_label(str(existing.get("row_label", "") or ""))
        if label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS:
            return True
    return False


def _upcoming_anchor_within(layout_rows: list[dict[str, Any]], start_index: int, limit: int = 4) -> bool:
    for candidate in layout_rows[start_index + 1 : start_index + 1 + limit]:
        if not isinstance(candidate, dict):
            continue
        if _is_generic_anchor_row(candidate):
            return True
    return False


def _generic_anchor_prefers_future_prefix(anchor_kind: str) -> bool:
    return anchor_kind in GENERIC_LAYOUT_FUTURE_PREFIX_ANCHOR_KINDS


def _current_block_has_property_label(block: dict[str, Any], label: str) -> bool:
    normalized = _normalize_generic_row_label(label)
    if not normalized:
        return False
    for existing in block.get("rows", []):
        if _normalize_generic_row_label(str(existing.get("row_label", "") or "")) == normalized:
            return True
    return False


def _build_generic_layout_blocks(layout_rows: list[dict[str, Any]], page_type: str = "") -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    prefix_mode = _generic_section_prefers_prefix_anchors(layout_rows)
    redirect_prefix_to_pending = False
    for index, row in enumerate(layout_rows):
        if not isinstance(row, dict):
            continue
        if _is_generic_layout_noise_row(row):
            continue
        compound_anchor, compound_property = _split_compound_generic_row_label(str(row.get("row_label", "") or ""))
        if compound_anchor and compound_property:
            normalized_anchor = _normalize_generic_row_label(compound_anchor)
            compound_row = dict(row)
            compound_row["row_label"] = compound_property
            if current is None or _normalize_generic_row_label(str(current.get("anchor_label", "") or "")) != normalized_anchor:
                if current:
                    blocks.append(current)
                current = {
                    "anchor_kind": _classify_generic_anchor({"row_label": compound_anchor, "row_kind": row.get("row_kind", "")}, page_type=page_type),
                    "anchor_label": compound_anchor,
                    "rows": [compound_row],
                }
            else:
                current["rows"].append(compound_row)
            continue
        if _is_generic_anchor_row(row):
            if current:
                blocks.append(current)
            current = {
                "anchor_kind": _classify_generic_anchor(row, page_type=page_type),
                "anchor_label": str(row.get("row_label", "") or ""),
                "rows": [*pending, row] if (prefix_mode or pending) else [row],
            }
            pending = []
            redirect_prefix_to_pending = False
            continue
        if prefix_mode:
            label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
            if (
                current is not None
                and label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS
                and _upcoming_anchor_within(layout_rows, index)
                and (
                    (
                        page_type == "sinkware_tapware"
                        and (_has_prefix_properties(current) or pending)
                    )
                    or (
                        _generic_anchor_prefers_future_prefix(str(current.get("anchor_kind", "") or ""))
                        and (_has_prefix_properties(current) or pending)
                    )
                    or _current_block_has_property_label(current, label)
                )
            ):
                pending.append(row)
            elif current is not None and _row_should_attach_to_current_anchor(current, row, page_type):
                current["rows"].append(row)
                if label not in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS:
                    redirect_prefix_to_pending = False
            else:
                pending.append(row)
        else:
            label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
            if (
                current is not None
                and redirect_prefix_to_pending
                and label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS
                and _upcoming_anchor_within(layout_rows, index)
            ):
                pending.append(row)
            elif (
                current is not None
                and page_type != "sinkware_tapware"
                and _generic_anchor_prefers_future_prefix(str(current.get("anchor_kind", "") or ""))
                and label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS
                and _has_prefix_properties(current)
                and _upcoming_anchor_within(layout_rows, index)
            ):
                pending.append(row)
                redirect_prefix_to_pending = True
            elif (
                current is not None
                and label in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS
                and _has_prefix_properties(current)
                and _current_block_has_property_label(current, label)
                and _upcoming_anchor_within(layout_rows, index)
            ):
                pending.append(row)
                redirect_prefix_to_pending = True
            elif current is None:
                pending.append(row)
            else:
                current["rows"].append(row)
                if label not in GENERIC_LAYOUT_PREFIX_PROPERTY_LABELS:
                    redirect_prefix_to_pending = False
    if current:
        blocks.append(current)
    return blocks


def _collect_generic_block_parts(block: dict[str, Any]) -> dict[str, list[str]]:
    parts: dict[str, list[str]] = {"note": [], "_ordered_fragments": []}
    anchor_label = parsing.normalize_space(str(block.get("anchor_label", "") or ""))
    unrelated_label_tokens = (
        "internal paint",
        "internal ceiling",
        "cornice",
        "skirtings",
        "architraves",
        "internal doors",
        "internal walls",
        "feature room/walls",
        "internal fittings selections",
        "flooring",
        "carpet",
        "timber",
        "client initials",
    )
    if anchor_label:
        parts.setdefault("anchor", []).append(anchor_label)
    for row in block.get("rows", []):
        label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
        if label and any(token in label for token in unrelated_label_tokens):
            continue
        value_text = _row_region_text(row, "value_text", "value_region_text")
        supplier_text = _row_region_text(row, "supplier_text", "supplier_region_text")
        notes_text = _row_region_text(row, "notes_text", "notes_region_text")
        text = _generic_property_row_text(label, value_text, supplier_text, notes_text)
        if not text:
            continue
        inline_pairs = _extract_generic_inline_property_pairs(text)
        if inline_pairs and (not label or label in GENERIC_LAYOUT_PROPERTY_MAP or len(inline_pairs) > 1):
            for inline_label, inline_value in inline_pairs:
                _append_generic_part_value(parts, inline_label, inline_value)
                _append_generic_ordered_fragment(parts, inline_value, label=inline_label)
            continue
        if label in {"colour", "colour & finish"}:
            text = re.sub(r"(?i)\bIsland Colour As Above\b", "", text)
            text = parsing.normalize_space(text).strip(" -;,")
            if not text:
                continue
        if label in {"door handle", "drawer handle", "pantry door handle", "bin & pot drawers handle"}:
            text = parsing.normalize_space(text.replace("**", "")).strip(" -;,")
            if not text or text.upper() in {"N/A", "NOT APPLICABLE", "#N/A"}:
                continue
        if label in {"contrasting facings", "selection required"}:
            cleaned_note = parsing.normalize_space(text.replace("**", "")).strip(" -;,")
            if cleaned_note and cleaned_note.upper() not in {"N/A", "NOT APPLICABLE", "#N/A"}:
                parts.setdefault("note", []).append(cleaned_note)
                _append_generic_ordered_fragment(parts, cleaned_note, label=label)
            continue
        bucket = GENERIC_LAYOUT_PROPERTY_MAP.get(label)
        if bucket:
            _append_generic_part_value(parts, label, text)
            _append_generic_ordered_fragment(parts, text, label=label)
        elif not label:
            lower_text = text.lower()
            if "benchtop" in lower_text:
                parts.setdefault("anchor", []).append("Benchtop")
                note_text = re.sub(r"(?i)^.*?\bbenchtops?\b", "", text).strip(" -")
                if note_text:
                    parts.setdefault("note", []).append(note_text)
                    _append_generic_ordered_fragment(parts, note_text)
            elif any(token in lower_text for token in ("underbench", "base cabinet panels", "cabinet panels")):
                parts.setdefault("anchor", []).append("Base Cabinetry")
                note_text = re.sub(r"(?i)^.*?\b(?:underbench|base cabinet panels|cabinet panels)\b", "", text).strip(" -")
                if note_text:
                    parts.setdefault("note", []).append(note_text)
                    _append_generic_ordered_fragment(parts, note_text)
            elif "overhead cupboards" in lower_text:
                parts.setdefault("anchor", []).append("Overhead Cupboards")
                note_text = re.sub(r"(?i)^.*?\boverhead cupboards\b", "", text).strip(" -")
                if note_text:
                    parts.setdefault("note", []).append(note_text)
                    _append_generic_ordered_fragment(parts, note_text)
            elif any(token in lower_text for token in ("handles", "handle")):
                parts.setdefault("anchor", []).append("Handles")
                note_text = re.sub(r"(?i)^.*?\bhandles?\b", "", text).strip(" -")
                if note_text:
                    parts.setdefault("note", []).append(note_text)
                    _append_generic_ordered_fragment(parts, note_text)
            elif "kickboard" in lower_text:
                parts.setdefault("anchor", []).append("Kickboard")
                note_text = re.sub(r"(?i)^.*?\bkickboard\b", "", text).strip(" -")
                if note_text:
                    parts.setdefault("note", []).append(note_text)
                    _append_generic_ordered_fragment(parts, note_text)
        elif label and label != _normalize_generic_row_label(anchor_label):
            note_text = f"{parsing.normalize_space(str(row.get('row_label', '') or ''))} {text}".strip()
            parts.setdefault("note", []).append(note_text)
            _append_generic_ordered_fragment(parts, note_text, label=label)
        elif label:
            text = _strip_generic_anchor_echo(text, anchor_label)
            if not text:
                continue
            if _normalize_generic_row_label(text) == label:
                continue
            parts.setdefault("note", []).append(text)
            _append_generic_ordered_fragment(parts, text, label=label)
    return parts


def _generic_property_row_text(label: str, value_text: str, supplier_text: str, notes_text: str) -> str:
    normalized_label = _normalize_generic_row_label(label)
    value_clean = _clean_generic_fragment(value_text)
    supplier_clean = _clean_generic_fragment(supplier_text)
    notes_clean = _clean_generic_fragment(notes_text)
    if normalized_label in GENERIC_LAYOUT_PROPERTY_MAP and supplier_clean:
        stripped = _strip_generic_property_prefix(value_clean, normalized_label)
        stripped = re.sub(r"(?i)^&\s*finish\b", "", stripped)
        stripped = parsing.normalize_space(stripped).strip(" -;,")
        if (
            not stripped
            or stripped in {"&", "finish", "& finish"}
            or _normalize_generic_row_label(stripped)
            in {
                normalized_label,
                normalized_label.replace(" & ", " "),
                "colour",
                "colour & finish",
                "finish",
                "manufacturer",
                "range",
                "model",
                "profile",
                "type",
                "location",
            }
        ):
            return parsing.normalize_space(" ".join(part for part in (supplier_clean, notes_clean) if part))
    return parsing.normalize_space(" ".join(part for part in (value_clean, supplier_clean, notes_clean) if part))


def _strip_generic_property_prefix(text: str, label: str) -> str:
    cleaned = _clean_generic_fragment(text)
    normalized_label = _normalize_generic_row_label(label)
    if not cleaned or not normalized_label:
        return cleaned
    pattern = rf"(?i)^\s*{re.escape(normalized_label)}(?:\s*&\s*finish)?\s*:?\s*"
    cleaned = re.sub(pattern, "", cleaned)
    return parsing.normalize_space(cleaned).strip(" -;,")


def _strip_generic_anchor_echo(text: str, anchor_label: str) -> str:
    cleaned = _clean_generic_fragment(text)
    anchor = parsing.normalize_space(anchor_label)
    if not cleaned or not anchor:
        return cleaned
    anchor_pattern = re.sub(r"\\ ", r"\\s+", re.escape(anchor))
    prefix_pattern = rf"(?i)^(?:{anchor_pattern}(?:\s*[-:]\s*|\s+))*{anchor_pattern}(?:\s*[-:]\s*|\s+)?"
    prefix_match = re.match(prefix_pattern, cleaned)
    stripped = re.sub(prefix_pattern, "", cleaned).strip(" -;,")
    if stripped:
        return parsing.normalize_space(stripped)
    if prefix_match:
        return ""
    if re.fullmatch(rf"(?i)(?:{anchor_pattern})(?:\s*[-:]\s*|\s+|$)+", cleaned):
        return ""
    return cleaned


def _extract_generic_inline_property_pairs(text: str) -> list[tuple[str, str]]:
    cleaned = _clean_generic_fragment(text)
    if not cleaned:
        return []
    label_pattern = "|".join(re.escape(label) for label in GENERIC_INLINE_PROPERTY_LABELS)
    matches = list(
        re.finditer(
            rf"(?i)(?<![A-Za-z0-9])(?:{label_pattern})(?=(?:\s*:|\s+|$))",
            cleaned,
        )
    )
    if not matches:
        return []
    if len(matches) == 1 and matches[0].start() > 0:
        return []
    pairs: list[tuple[str, str]] = []
    for index, match in enumerate(matches):
        label = _normalize_generic_row_label(match.group(0))
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(cleaned)
        value = _strip_generic_property_prefix(cleaned[start:end], label)
        if value:
            pairs.append((label, value))
    return pairs


def _append_generic_part_value(parts: dict[str, list[str]], label: str, value: str) -> None:
    normalized_label = _normalize_generic_row_label(label)
    bucket = GENERIC_LAYOUT_PROPERTY_MAP.get(normalized_label)
    if not bucket:
        return
    text = _strip_generic_property_prefix(value, normalized_label)
    if normalized_label not in {"location"}:
        text = _strip_generic_anchor_tail(text)
    if normalized_label in {"colour", "colour & finish"}:
        text = re.sub(r"(?i)\bIsland Colour As Above\b", "", text)
        text = parsing.normalize_space(text).strip(" -;,")
    if normalized_label in {"door handle", "drawer handle", "pantry door handle", "bin & pot drawers handle"}:
        text = parsing.normalize_space(text.replace("**", "")).strip(" -;,")
    if not text or text.upper() in {"N/A", "NOT APPLICABLE", "#N/A"}:
        return
    parts.setdefault(bucket, []).append(text)


def _append_generic_ordered_fragment(parts: dict[str, list[str]], value: str, *, label: str = "") -> None:
    text = _strip_generic_property_prefix(value, label) if label else _clean_generic_fragment(value)
    if label not in {"location"}:
        text = _strip_generic_anchor_tail(text)
    text = parsing.normalize_space(text).strip(" -;,")
    if not text:
        return
    ordered = parts.setdefault("_ordered_fragments", [])
    if text.lower() not in {existing.lower() for existing in ordered}:
        ordered.append(text)


def _strip_generic_anchor_tail(text: str) -> str:
    cleaned = _clean_generic_fragment(text)
    if not cleaned:
        return ""
    for anchor_label in sorted(GENERIC_LAYOUT_ANCHOR_LABELS, key=len, reverse=True):
        pattern = re.sub(r"\\ ", r"\\s*", re.escape(anchor_label))
        cleaned = re.sub(rf"(?i)\s*{pattern}\s*$", "", cleaned).strip(" -;,")
    return parsing.normalize_space(cleaned).strip(" -;,")


def _clean_generic_fragment(value: str) -> str:
    text = parsing.normalize_space(str(value or ""))
    text = re.sub(r"(?i)^\d+\s+cabinets?\b", "", text)
    text = re.sub(r"(?i)\badditional(?:\s+wet\s+area|\s+bath/ensuite/powder|\s+kitchen/butlers/kitchenette)\b", "", text)
    text = re.sub(r"(?i)\bdwarf wall capping finish\b", "", text)
    text = re.sub(r"(?i)\bto\s+rumpus\s+feature\s+battens\b", "", text)
    text = re.sub(r"(?i)\blocation\s+[A-Za-z0-9 /&'()_-]+?\s+location\b", "", text)
    text = re.sub(r"(?i)simonds queensland construction pty ltd.*$", "", text)
    text = re.sub(r"(?i)page:\s*\d+\s+of\s+\d+.*$", "", text)
    text = re.sub(r"(?i)printed:\s+.*$", "", text)
    text = re.sub(r"(?i)report:\s+.*$", "", text)
    text = re.sub(r"(?i)client initials.*$", "", text)
    text = re.sub(r"(?i)\b(?:client name|designer|signature|signed date|document ref|job number|job address|colour consultant)\b.*$", "", text)
    text = re.sub(r"(?i)\b(?:site address|address|client|date)\s*:.*$", "", text)
    text = re.sub(r"(?i)\b(?:supplier description design comments|supplier description|area / item|image supplier notes)\b.*$", "", text)
    text = re.sub(r"(?i)\b(?:forstan pty ltd|phone\s*:|fax\s*:|abn\s*:|job no\s*:|sheet\s+\d+|scale\s*:)\b.*$", "", text)
    text = re.sub(r"(?i)page\s+\d+\s+of\s+\d+.*$", "", text)
    text = text.replace("**", " ")
    text = re.sub(r"\(\s*(?:#?\s*n\s*/?\s*a|not applicable)\s*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\+\s*", "", text)
    text = parsing.normalize_space(text).strip(" -;,+")
    return text


GENERIC_NOISE_LABEL_TOKENS: tuple[str, ...] = (
    "manufacturer",
    "range",
    "profile",
    "colour",
    "colour & finish",
    "finish",
    "model",
    "type",
    "location",
    "category",
    "fixing",
    "mechanism",
    "style",
)


def _looks_like_generic_field_noise(value: str, *, field: str = "") -> bool:
    text = _clean_generic_fragment(value)
    if _is_generic_placeholder_text(text):
        return True
    lowered = text.lower()
    if re.match(
        rf"(?i)^\s*(?:{'|'.join(re.escape(label) for label in GENERIC_INLINE_PROPERTY_LABELS)})\b",
        lowered,
    ):
        return True
    if any(
        token in lowered
        for token in (
            "joinery selection sheet",
            "colour schedule",
            "supplier description design comments",
            "client name",
            "signed date",
            "document ref",
            "address:",
            "client:",
            "date:",
        )
    ):
        return True
    if field == "fixture":
        if any(token in lowered for token in ("client name", "signed date", "document ref", "designer:", "signature:")):
            return True
        if sum(token in lowered for token in ("manufacturer", "range", "model", "type", "location")) >= 2:
            return True
    if field == "material":
        if sum(token in lowered for token in GENERIC_NOISE_LABEL_TOKENS) >= 2:
            return True
        if any(token in lowered for token in ("all cabinets include soft close", "benchtops over maximum length")):
            return True
        if any(
            token in lowered
            for token in (
                "electrical / alarm system / cctv / solar pv system",
                "switch plates / gpo",
                "tv antenna",
                "home automation",
                "alarm system",
                "solar pv system",
                "air-conditioning",
                "air conditioning",
                "hot water unit",
                "outlets & zones",
                "controller type",
                "vent type",
            )
        ):
            return True
    if field == "handle":
        if sum(token in lowered for token in ("manufacturer", "range", "model", "style", "finish", "fixing")) >= 2:
            return True
        if any(
            token in lowered
            for token in (
                "switch plates",
                "gpo",
                "tv antenna",
                "home automation",
                "alarm system",
                "cctv",
                "solar pv system",
                "freestanding cooker",
            )
        ):
            return True
    return False


def _prefer_generic_overlay_value(left: str, right: str, *, field: str = "") -> str:
    left_clean = parsing.normalize_space(str(left or ""))
    right_clean = parsing.normalize_space(str(right or ""))
    if not left_clean:
        return right_clean
    if not right_clean:
        return left_clean
    left_noisy = _looks_like_generic_field_noise(left_clean, field=field)
    right_noisy = _looks_like_generic_field_noise(right_clean, field=field)
    if left_noisy and not right_noisy:
        return right_clean
    if right_noisy and not left_noisy:
        return left_clean
    return right_clean if len(right_clean) > len(left_clean) else left_clean


def _is_generic_placeholder_text(value: str) -> bool:
    text = _clean_generic_fragment(value)
    if not text:
        return True
    lowered = text.lower()
    compact = re.sub(r"[\s./()-]+", "", lowered)
    if lowered in {"n/a", "na", "not applicable", "not included", "#n/a", "-", "wc"}:
        return True
    if compact in {"na", "nna", "nanan", "#na"}:
        return True
    if re.fullmatch(r"(?:#?\s*n\s*/?\s*a[\s./()-]*){1,4}", lowered):
        return True
    return False


def _meaningful_generic_values(values: list[str], *, exclude_tokens: tuple[str, ...] = ()) -> list[str]:
    meaningful: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        text = _clean_generic_fragment(raw_value)
        inline_pairs = _extract_generic_inline_property_pairs(text)
        if len(inline_pairs) == 1:
            text = inline_pairs[0][1]
        if _is_generic_placeholder_text(text):
            continue
        lowered = text.lower()
        if lowered in {"&", "finish", "& finish"}:
            continue
        if exclude_tokens and any(token in lowered for token in exclude_tokens):
            continue
        if text not in seen:
            seen.add(text)
            meaningful.append(text)
    return meaningful


def _ordered_generic_fragments_from_parts(
    parts: dict[str, list[str]],
    *,
    exclude_tokens: tuple[str, ...] = (),
    preserve_placeholders: bool = False,
) -> list[str]:
    ordered_values = parts.get("_ordered_fragments", [])
    if isinstance(ordered_values, list) and ordered_values:
        source_values = ordered_values
    else:
        source_values = [
            *_meaningful_generic_values(parts.get("manufacturer", [])),
            *_meaningful_generic_values(parts.get("colour", [])),
            *_meaningful_generic_values(parts.get("finish", [])),
            *_meaningful_generic_values(parts.get("range", [])),
            *_meaningful_generic_values(parts.get("model", [])),
            *_meaningful_generic_values(parts.get("style", [])),
            *_meaningful_generic_values(parts.get("type", [])),
            *_meaningful_generic_values(parts.get("profile", [])),
            *_meaningful_generic_values(parts.get("location", [])),
            *_meaningful_generic_values(parts.get("note", [])),
        ]
    ordered: list[str] = []
    seen: set[str] = set()
    for raw_value in source_values:
        text = _clean_generic_fragment(raw_value)
        if not text:
            continue
        if not preserve_placeholders and _is_generic_placeholder_text(text):
            continue
        lowered = text.lower()
        if lowered in {"&", "finish", "& finish"}:
            continue
        if exclude_tokens and any(token in lowered for token in exclude_tokens):
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(text)
    return ordered


def _ordered_page_candidates(*candidate_lists: list[tuple[int, int]]) -> list[tuple[int, int]]:
    ordered: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for candidate_list in candidate_lists:
        for candidate in candidate_list:
            if not isinstance(candidate, tuple) or len(candidate) != 2:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _first_meaningful(values: list[str]) -> str:
    meaningful = _meaningful_generic_values(values)
    return meaningful[0] if meaningful else ""


def _format_generic_flooring_from_parts(parts: dict[str, list[str]]) -> str:
    values = _ordered_material_fragments_from_parts(parts, exclude_tokens=("soft close",))
    if not values:
        values = _meaningful_generic_values(parts.get("note", []), exclude_tokens=("soft close",))
    if not values:
        return ""
    return parsing.normalize_brand_casing_text(" - ".join(values))


def _format_generic_material_from_parts(parts: dict[str, list[str]]) -> str:
    ordered = _ordered_material_fragments_from_parts(parts)
    if ordered and all(_is_material_business_placeholder_text(value) for value in ordered):
        return parsing.normalize_brand_casing_text(" - ".join(ordered))
    manufacturer = _first_meaningful(parts.get("manufacturer", []))
    finish_values = _meaningful_generic_values(parts.get("finish", []))
    material = _first_meaningful(parts.get("colour", [])) or (finish_values[-1] if finish_values else "")
    profile = _first_meaningful(parts.get("profile", [])) or _first_meaningful(parts.get("style", [])) or _first_meaningful(parts.get("type", []))
    if not manufacturer and material and material.lower() in {"not applicable", "n/a"}:
        return ""
    note_excludes = (
        "not applicable",
        "n/a",
        "#n/a",
        "not included",
        "contrasting facings",
        "selection required",
        "island colour as above",
        "no shelf to cupboard",
        "washing machine taps located inside cupboards",
        "one stone colour included",
        "additional $350 charge",
        "location",
    )
    notes = [
        value
        for value in _meaningful_generic_values(parts.get("note", []))
        if not any(token in value.lower() for token in note_excludes)
        and not value.lower().startswith("including")
        and not re.match(r"(?i)^nook\b", value)
    ]
    composed: list[str] = []
    if profile:
        thickness_match = re.match(r"(?i)^(?P<thickness>\d+\s*mm)\s+(?P<edge>.+)$", profile)
        if thickness_match:
            composed.append(
                parsing.normalize_space(
                    f"{thickness_match.group('thickness')} {manufacturer}".strip() if manufacturer else thickness_match.group("thickness")
                )
            )
            if material:
                composed.append(material)
            edge = parsing.normalize_space(thickness_match.group("edge"))
            if edge:
                composed.append(edge)
        else:
            if manufacturer:
                composed.append(manufacturer)
            if material:
                composed.append(material)
            composed.append(profile)
    else:
        if manufacturer:
            composed.append(manufacturer)
        if material:
            composed.append(material)
    for note in notes:
        if note not in composed:
            composed.append(note)
    formatted = parsing.normalize_brand_casing_text(" - ".join(part for part in composed if part))
    return re.sub(r"\s+-\s+-\s+", " - ", formatted).strip(" -")


def _format_generic_island_bench_from_parts(parts: dict[str, list[str]], wall_run_text: str = "") -> str:
    island_colour_values = _meaningful_generic_values(parts.get("island_colour", []))
    island_profile = _first_meaningful(parts.get("island_profile", []))
    if not island_colour_values and not island_profile:
        return ""
    island_colour = island_colour_values[0] if island_colour_values else ""
    if island_colour.lower() in {"as above", "same as above"}:
        island_colour = _first_meaningful(parts.get("colour", [])) or ""
    island_parts = {key: list(values) for key, values in parts.items()}
    if island_colour:
        island_parts["colour"] = [island_colour]
    if island_profile:
        island_parts["profile"] = [island_profile]
    island_text = _format_generic_material_from_parts(island_parts)
    if island_text:
        return island_text
    if wall_run_text and island_profile:
        material_only = re.sub(r"(?i)\s*-\s*\d+\s*mm\b.*$", "", wall_run_text).strip(" -;,")
        if material_only:
            return parsing.normalize_brand_casing_text(f"{material_only} - {island_profile}")
    return ""


def _trim_bench_noise_from_cabinetry_fragment(fragment: str) -> str:
    cleaned = _clean_generic_fragment(fragment)
    lowered = cleaned.lower()
    if not any(token in lowered for token in ("caesarstone", "quantum quartz", "mitred", "arris", "benchtop", "waterfall")):
        return cleaned
    cabinetry_match = re.search(r"(?i)\b(polytec|laminex|colourboard|formica)\b", cleaned)
    if cabinetry_match:
        trimmed = parsing.normalize_space(cleaned[cabinetry_match.start() :]).strip(" -;,")
        return trimmed or cleaned
    return cleaned


def _generic_material_fragment_is_noise(fragment: str, *, field_name: str = "") -> bool:
    text = _clean_generic_fragment(fragment)
    if not text:
        return True
    if _material_field_should_preserve_text(text, field_name=field_name):
        return False
    if _looks_like_generic_field_noise(text, field="material"):
        return True
    lowered = text.lower()
    if any(
        token in lowered
        for token in (
            "supplier description",
            "joinery selection sheet",
            "colour schedule",
            "document ref",
            "client initials",
            "dwarf wall capping",
            "feature battens",
        )
    ):
        return True
    if field_name in {
        "door_colours_base",
        "door_colours_overheads",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
    }:
        if any(token in lowered for token in ("caesarstone", "quantum quartz", "mitred", "arris", "waterfall", "benchtop")):
            return True
        if any(
            token in lowered
            for token in (
                "handle",
                "finger grip",
                "soft close",
                "cutlery tray",
                "robe hook",
                "towel rail",
                "toilet suite",
                "toilet roll holder",
                "sink",
                "tap",
                "basin",
                "shower",
                "bath",
                "feature waste",
                "towel hook",
                "hand towel",
                "mirror",
                "installed above inclusion",
            )
        ):
            return True
        if field_name != "door_colours_tall" and any(token in lowered for token in ("drawers", "pot drawers", "bin & pot")):
            return True
        if lowered == "laminate":
            return True
    if field_name in {"bench_tops_wall_run", "bench_tops_island", "bench_tops_other"}:
        if any(
            token in lowered
            for token in (
                "underbench",
                "cabinet panels",
                "overhead cupboards",
                "pantry doors",
                "kickboard",
                "handle",
                "drawer handle",
                "door handle",
                "pantry door handle",
                "bin & pot drawers handle",
            )
        ):
            return True
    if field_name == "toe_kick":
        if any(token in lowered for token in ("caesarstone", "quantum quartz", "mitred", "arris", "waterfall")):
            return True
    if field_name == "floating_shelf":
        if any(token in lowered for token in ("handle", "soft close", "client", "date")):
            return True
    if re.fullmatch(r"[A-Z0-9-]{4,}", text) and field_name.startswith("door_colours_"):
        return True
    return False


def _generic_fragment_token_key(value: str) -> set[str]:
    text = _clean_generic_fragment(value).lower()
    return {
        token
        for token in re.split(r"[^a-z0-9]+", text)
        if token
        and token not in {"and", "the", "with", "mm", "n", "a", "na"}
        and not token.isdigit()
    }


def _is_material_like_field(field_name: str) -> bool:
    return field_name in {
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_base",
        "door_colours_overheads",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "feature_colour",
        "floating_shelf",
        "toe_kick",
        "bulkheads",
        "splashback",
        "flooring",
    }


def _material_field_should_preserve_text(text: str, *, field_name: str = "") -> bool:
    if not _is_material_like_field(field_name):
        return False
    return _is_material_business_placeholder_text(text)


def _is_material_business_placeholder_text(text: str) -> bool:
    lowered = _clean_generic_fragment(text).lower()
    if lowered in {"as above", "same as above", "by client", "by builder", "not applicable", "n/a", "na", "#n/a", "not included"}:
        return True
    return lowered.startswith("tiles by client")


def _is_generic_na_placeholder(text: str) -> bool:
    lowered = _clean_generic_fragment(text).lower()
    return lowered in {"not applicable", "n/a", "na", "#n/a"}


def _ordered_material_fragments_from_parts(
    parts: dict[str, list[str]],
    *,
    exclude_tokens: tuple[str, ...] = (),
) -> list[str]:
    ordered = _ordered_generic_fragments_from_parts(
        parts,
        exclude_tokens=exclude_tokens,
        preserve_placeholders=True,
    )
    if not ordered:
        return []
    if any(not _is_generic_na_placeholder(value) for value in ordered):
        ordered = [value for value in ordered if not _is_generic_na_placeholder(value)]
    return ordered


def _sanitize_generic_material_field(value: Any, *, field_name: str = "", room_key: str = "") -> str:
    fragments = [
        _clean_generic_fragment(fragment)
        for fragment in re.split(r"\s*\|\s*", parsing.normalize_space(str(value or "")))
    ]
    cleaned: list[str] = []
    cleaned_keys: list[set[str]] = []
    for fragment in fragments:
        if not fragment:
            continue
        fragment = re.sub(
            r"(?i)^(?:underbench|overhead cupboards|pantry doors|drawers|handles?|kickboard|shaving cabinets|benchtops?)\s*-\s*",
            "",
            fragment,
        ).strip(" -;,")
        if field_name.startswith("door_colours_") or field_name == "toe_kick":
            fragment = _trim_bench_noise_from_cabinetry_fragment(fragment)
        if _generic_material_fragment_is_noise(fragment, field_name=field_name):
            continue
        normalized = parsing.normalize_brand_casing_text(fragment).strip(" -;,")
        normalized = re.sub(
            r"(?i)^(?:not applicable|n/?a|#n/?a)\s+(?=(?:polytec|laminex|colourboard|formica|caesarstone|quantum quartz|quantum zero|stone ambassador|smartstone|essastone|dekton)\b)",
            "",
            normalized,
        ).strip(" -;,")
        if not normalized:
            continue
        token_key = _generic_fragment_token_key(normalized)
        if token_key and any(token_key <= existing_key for existing_key in cleaned_keys if existing_key):
            continue
        if token_key:
            replacement_indexes = [
                index for index, existing_key in enumerate(cleaned_keys) if existing_key and existing_key < token_key
            ]
            for index in reversed(replacement_indexes):
                del cleaned[index]
                del cleaned_keys[index]
        if any(
            normalized.lower() == existing.lower()
            or normalized.lower() in existing.lower()
            or existing.lower() in normalized.lower()
            for existing in cleaned
        ):
            continue
        cleaned.append(normalized)
        cleaned_keys.append(token_key)
    if room_key == "kitchen" and field_name == "bench_tops_other" and cleaned:
        return " | ".join(cleaned)
    return " | ".join(cleaned)


def _sanitize_generic_material_entries(values: Any, *, field_name: str = "", room_key: str = "") -> list[str]:
    entries = parsing._coerce_string_list(values)
    if not entries:
        return []
    cleaned_text = _sanitize_generic_material_field(" | ".join(entries), field_name=field_name, room_key=room_key)
    return [
        fragment
        for fragment in re.split(r"\s*\|\s*", cleaned_text)
        if parsing.normalize_space(fragment)
    ]


def _bench_field_is_combined_duplicate(other: str, wall_run: str, island: str) -> bool:
    other_fragments = {
        _clean_generic_fragment(fragment).lower()
        for fragment in re.split(r"\s*\|\s*", parsing.normalize_space(other))
        if _clean_generic_fragment(fragment)
    }
    if not other_fragments:
        return False
    reference_fragments = {
        _clean_generic_fragment(fragment).lower()
        for source in (wall_run, island)
        for fragment in re.split(r"\s*\|\s*", parsing.normalize_space(source))
        if _clean_generic_fragment(fragment)
    }
    return bool(reference_fragments) and other_fragments.issubset(reference_fragments)


def _format_generic_fixture_from_parts(parts: dict[str, list[str]], *, kind: str = "", anchor_label: str = "") -> str:
    supplier = _first_meaningful(parts.get("manufacturer", []))
    tap_tokens = ("mixer", "tap", "spout", "stop", "shower rail", "shower rose", "shower system")
    sink_excludes = tap_tokens if kind in {"sink", "basin"} else ()
    fixture_note_excludes = {
        "robe hook",
        "hand towel rail",
        "towel rail",
        "toilet suite",
        "toilet roll holder",
        "accessories",
        "selection required",
    }
    description_parts = [
        _first_meaningful(parts.get("range", [])),
        _first_meaningful(parts.get("model", [])),
        *(_meaningful_generic_values(parts.get("style", []), exclude_tokens=sink_excludes)[:1]),
        *(_meaningful_generic_values(parts.get("type", []), exclude_tokens=sink_excludes)[:1]),
        *(_meaningful_generic_values(parts.get("profile", []), exclude_tokens=sink_excludes)[:1]),
    ]
    description = " - ".join(part for part in description_parts if part)
    if supplier and description.lower().startswith(supplier.lower()):
        description = description[len(supplier) :].lstrip(" -")
    note_parts = [
        _first_meaningful(parts.get("finish", [])),
        _first_meaningful(parts.get("location", [])),
        *[
            value
            for value in _meaningful_generic_values(parts.get("note", []))
            if "client name" not in value.lower()
            and "signed date" not in value.lower()
            and not any(token in value.lower() for token in fixture_note_excludes)
        ],
    ]
    note = " - ".join(part for part in note_parts if part)
    result = " - ".join(part for part in (supplier, description, note) if part)
    if not supplier and not description:
        return ""
    normalized = parsing.normalize_brand_casing_text(result)
    if _looks_like_placeholder_fixture_text(normalized):
        return ""
    if kind == "tap":
        normalized = re.sub(r"(?i)\bby client\b.*$", "", normalized).strip(" -;,")
        normalized = re.sub(r"(?i)\s*-\s*(?:alder\s+sachi|sachi|wish)\b.*$", "", normalized).strip(" -;,")
    if kind in {"sink", "basin"} and any(token in normalized.lower() for token in tap_tokens):
        normalized = " - ".join(
            part for part in [supplier, _first_meaningful(parts.get("range", [])), _first_meaningful(parts.get("model", [])), _first_meaningful(parts.get("type", []))]
            if part
        ).strip(" -;,")
    return normalized


def _sanitize_generic_fixture_field(value: Any, *, kind: str = "") -> str:
    fragments = [
        _clean_generic_fragment(fragment)
        for fragment in re.split(r"\s*\|\s*", parsing.normalize_space(str(value or "")))
    ]
    non_empty_fragments = [fragment for fragment in fragments if fragment]
    multiple_fragments = len(non_empty_fragments) > 1
    cleaned: list[str] = []
    cleaned_keys: list[set[str]] = []
    for fragment in fragments:
        text = fragment
        if not text:
            continue
        text = re.sub(
            r"(?i)\s*\+\s*[^+]*(?:robe hook|towel rail|hand towel rail|toilet suite|toilet roll holder).*$",
            "",
            text,
        )
        text = re.sub(
            r"(?i)\b(?:client name|designer|signature|signed date|document ref|category)\b.*$",
            "",
            text,
        )
        text = re.sub(r"(?i)\b(?:type|model|location)\b\s*:?(\s+|$)", " ", text)
        text = re.sub(r"(?i)\b(?:image|supplier|notes)\b\s*:?(\s+|$)", " ", text)
        location_first = re.match(
            r"(?i)^(?P<location>(?:centre|center|corner|left|right)(?:\s+of(?:\s+(?:sink|basin|tub))?)?)\s*-\s*(?P<rest>.+)$",
            text,
        )
        if location_first and any(
            token in location_first.group("rest").lower()
            for token in ("mixer", "tap", "spout", "sink", "basin", "gooseneck", "pull-out")
        ):
            text = f"{location_first.group('rest')} - {location_first.group('location')}"
        if kind == "tap":
            text = re.sub(r"(?i)\b(?:by client|supplied by client)\b.*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\s*-\s*(?:alder\s+sachi|sachi|wish)\s*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\s*-\s*washing machine taps\b.*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\bwashing machine taps\b.*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\b(?:robe hook|towel rail|hand towel rail|toilet suite|toilet roll holder|switch plates?|gpo'?s?|tv antenna|home automation|alarm system|cctv|solar pv system|freestanding cooker)\b.*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\b(?:shower rose|rail hs\d+)\b.*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\s*-\s*(?:corner|centre|center)\s+of\s*$", "", text).strip(" -;,")
            text = re.sub(r"(?i)\b(?:not included|not applicable|#n/?a)\b", "", text).strip(" -;,")
        text = parsing.normalize_brand_casing_text(parsing.normalize_space(text)).strip(" -;,")
        if kind == "tap":
            text = re.sub(r"(?i)^tap\s+", "", text).strip(" -;,")
            if multiple_fragments:
                tap_signal = any(
                    token in text.lower()
                    for token in (
                        "mixer",
                        "tap",
                        "spout",
                        "gooseneck",
                        "pull-out",
                        "pull down",
                        "vegie",
                        "basin",
                        "sink",
                        "wall basin",
                        "bath mixer",
                    )
                )
                location_signal = bool(re.search(r"(?i)\b(?:centre|center|corner)\s+of\b", text))
                if not tap_signal and not location_signal:
                    continue
        if kind == "tap" and text.lower() in {"type", "location", "centre of", "center of", "corner of", "type location"}:
            continue
        if kind in {"sink", "basin"} and text.lower() in {"type", "model", "type overmount", "model type", "model type overmount"}:
            continue
        if not text:
            continue
        token_key = _generic_fragment_token_key(text)
        if token_key and any(token_key <= existing_key for existing_key in cleaned_keys if existing_key):
            continue
        if token_key:
            replacement_indexes = [
                index for index, existing_key in enumerate(cleaned_keys) if existing_key and existing_key < token_key
            ]
            for index in reversed(replacement_indexes):
                del cleaned[index]
                del cleaned_keys[index]
        if any(
            text.lower() == existing.lower()
            or text.lower() in existing.lower()
            or existing.lower() in text.lower()
            for existing in cleaned
        ):
            continue
        cleaned.append(text)
        cleaned_keys.append(token_key)
    return " | ".join(cleaned)


def _format_generic_handles_from_parts(parts: dict[str, list[str]]) -> str:
    supplier = _first_meaningful(parts.get("manufacturer", []))
    handle_excludes = ("soft close", "hanging rail", "robe hook", "towel rail", "mirror")
    description_parts = [
        _first_meaningful(parts.get("model", [])),
        _first_meaningful(parts.get("range", [])),
        *(_meaningful_generic_values(parts.get("style", []), exclude_tokens=handle_excludes)[:1]),
        *(_meaningful_generic_values(parts.get("profile", []), exclude_tokens=handle_excludes)[:1]),
        *(_meaningful_generic_values(parts.get("handles", []), exclude_tokens=handle_excludes)[:1]),
    ]
    description = " - ".join(part for part in description_parts if part)
    filtered_notes: list[str] = []
    for value in _meaningful_generic_values(parts.get("note", []), exclude_tokens=("soft close", "hanging rail", "not applicable", "#n/a")):
        cleaned = re.sub(r"(?i)^(?:door|drawers?|pantry door|bin\s*&\s*pot drawers?)\s*handle\s*", "", value).strip(" -;,")
        cleaned = re.sub(r"(?i)\b#n/?a\b", "", cleaned).strip(" -;,")
        cleaned = re.sub(r"(?i)\b(?:shaving cabinets?|drawers?)\s+not included\b", "", cleaned).strip(" -;,")
        cleaned = re.sub(r"(?i)\bpink text\b", "", cleaned).strip(" -;,")
        cleaned = parsing.normalize_space(cleaned).strip(" -;,")
        if not cleaned or cleaned.lower() in {"bin&", "bin", "pot", "drawers handle", "handle", "has", "not included"} or cleaned.lower().startswith("bin"):
            continue
        filtered_notes.append(cleaned)
    note_parts = [
        _first_meaningful(parts.get("finish", [])),
        _first_meaningful(parts.get("fixing", [])),
        *(_meaningful_generic_values(parts.get("mechanism", []), exclude_tokens=("soft close",))[:1]),
        _first_meaningful(parts.get("door handle", [])),
        _first_meaningful(parts.get("drawer handle", [])),
        _first_meaningful(parts.get("pantry door handle", [])),
        _first_meaningful(parts.get("bin & pot drawers handle", [])),
        *filtered_notes,
    ]
    note = " - ".join(part for part in note_parts if part)
    normalized_description = _clean_generic_fragment(description)
    normalized_note = _clean_generic_fragment(note)
    result = " - ".join(part for part in (supplier, normalized_description, normalized_note) if part)
    result = parsing.normalize_brand_casing_text(result)
    result = re.sub(r"(?i)\bContrasting Facings\b.*$", "", result).strip(" -;,")
    result = re.sub(r"\s+-\s+-\s+", " - ", result)
    return result.strip(" -;,")


def _sanitize_generic_handle_entries(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for value in values:
        text = parsing.normalize_brand_casing_text(_clean_generic_fragment(str(value or ""))).strip(" -;,")
        if not text:
            continue
        preserve_na = bool(re.search(r"(?i)\b(?:handless|lip pull|no handle|recessed|finger space)\b", text))
        if preserve_na:
            text = re.sub(r"(?i)\bN\s*/?\s*A\b", "N/A", text)
        else:
            text = re.sub(r"(?i)\bN/?A\b", "", text)
        text = re.sub(r"(?i)\bCategory\s*\d+\b", "", text)
        text = re.sub(r"(?i)\bSoft Close\b", "", text)
        text = re.sub(r"(?i)\b(?:shaving cabinets?|drawers?)\s+not included\b", "", text)
        text = re.sub(r"(?i)\bnot included\b", "", text)
        text = re.sub(r"(?i)\bpink text\b", "", text)
        text = re.sub(r"(?i)\b(?:switch plates?|gpo'?s?|tv antenna|home automation|alarm system|cctv|solar pv system|freestanding cooker)\b.*$", "", text)
        text = text.replace("**", " ")
        text = parsing.normalize_space(text)
        text = re.sub(r"\s+,", ",", text)
        text = parsing.normalize_space(text).strip(" -;,")
        if not text or text.lower() in {"has", "n/a", "na"} or _looks_like_placeholder_entry(text):
            continue
        if any(
            text.lower() == existing.lower()
            or text.lower() in existing.lower()
            or existing.lower() in text.lower()
            for existing in cleaned
        ):
            continue
        cleaned.append(text)
    return cleaned


def _merge_generic_handle_entries(existing: list[str], overlay: list[str]) -> list[str]:
    merged: list[str] = []
    for value in [*existing, *overlay]:
        text = parsing.normalize_space(str(value or ""))
        if not text:
            continue
        replaced = False
        for index, existing_text in enumerate(merged):
            lowered = text.lower()
            existing_lowered = existing_text.lower()
            if lowered == existing_lowered or lowered in existing_lowered or existing_lowered in lowered:
                if len(text) > len(existing_text):
                    merged[index] = text
                replaced = True
                break
        if not replaced:
            merged.append(text)
    return _sanitize_generic_handle_entries(merged)


def _format_generic_accessory_from_parts(parts: dict[str, list[str]], *, anchor_label: str = "") -> str:
    supplier = _first_meaningful(parts.get("manufacturer", []))
    description_parts = [
        _first_meaningful(parts.get("range", [])),
        _first_meaningful(parts.get("model", [])),
        _first_meaningful(parts.get("style", [])),
        _first_meaningful(parts.get("type", [])),
        _first_meaningful(parts.get("profile", [])),
    ]
    description = " - ".join(part for part in description_parts if part)
    note_parts = [
        _first_meaningful(parts.get("finish", [])),
        _first_meaningful(parts.get("location", [])),
        *[value for value in _meaningful_generic_values(parts.get("note", [])) if "selection required" not in value.lower()],
    ]
    note = " - ".join(part for part in note_parts if part)
    anchor_title = parsing.normalize_space(anchor_label).title()
    if not supplier and not description:
        if note and anchor_title and anchor_title.lower() not in {"accessories", "accessories & toilet suite"}:
            return parsing.normalize_brand_casing_text(f"{anchor_title} - {note}")
        return ""
    result = " - ".join(part for part in (supplier, description, note) if part)
    return parsing.normalize_brand_casing_text(result).strip(" -;,")


def _looks_like_placeholder_fixture_text(value: str) -> bool:
    text = _clean_generic_fragment(value)
    if _is_generic_placeholder_text(text):
        return True
    lowered = text.lower()
    if lowered in {
        "centre of sink",
        "centre of basin",
        "location outdoor shower model not applicable",
        "shower rail / rose shower screen",
        "shower screen colour",
    }:
        return True
    placeholder_tokens = (
        "model type",
        "type location",
        "type not applicable location",
        "not applicable model type",
        "not applicable type location",
    )
    label_words = {"shower", "rail", "rose", "screen", "colour", "location", "model", "type", "mixer"}
    normalized_words = set(re.sub(r"[^a-z]+", " ", lowered).split())
    if normalized_words and normalized_words <= label_words:
        return True
    return any(token in lowered for token in placeholder_tokens)


def _looks_like_placeholder_entry(value: str) -> bool:
    text = _clean_generic_fragment(value)
    if _is_generic_placeholder_text(text):
        return True
    lowered = text.lower()
    return any(
        token in lowered
        for token in (
            "not applicable",
            "#n/a",
            "wc**",
            "door handle drawer handle",
            "type location",
            "model type",
            "shower screen colour",
            "**",
        )
    )


def _room_label_is_wet_area(room_label: str) -> bool:
    lowered = parsing.normalize_space(room_label).lower()
    return any(token in lowered for token in ("bathroom", "ensuite", "powder", "wc", "toilet"))


def _extract_generic_layout_overlay(section: dict[str, Any], *, documents: list[dict[str, object]] | None = None) -> dict[str, Any]:
    layout_rows = [row for row in section.get("layout_rows", []) if isinstance(row, dict)]
    if not layout_rows:
        return {}
    page_type = parsing.normalize_space(str(section.get("page_type", "") or "")).lower().replace(" ", "_")
    overlay = _blank_generic_layout_overlay()
    overlay["original_room_label"] = _generic_overlay_room_label(section, layout_rows, documents=documents)
    overlay["source_file"] = str(section.get("file_name", "") or "")
    overlay["page_refs"] = ",".join(str(page_no) for page_no in section.get("page_nos", []) if page_no)
    overlay["evidence_snippet"] = parsing.normalize_space(str(section.get("text", "") or ""))[:240]
    last_cabinetry_material = ""
    for block in _build_generic_layout_blocks(layout_rows, page_type=page_type):
        parts = _collect_generic_block_parts(block)
        kind = str(block.get("anchor_kind", "") or "")
        anchor_label = _normalize_generic_row_label(str(block.get("anchor_label", "") or ""))
        if parsing._is_blacklisted_wet_area_label(anchor_label):
            continue
        embedded_handle_text = ""
        if any(
            parts.get(key)
            for key in (
                "model",
                "handles",
                "door handle",
                "drawer handle",
                "pantry door handle",
                "bin & pot drawers handle",
            )
        ):
            embedded_handle_parts = {key: list(values) for key, values in parts.items()}
            if kind in {"base", "island", "overheads", "tall"} and "handle" not in anchor_label:
                embedded_handle_parts.pop("manufacturer", None)
            embedded_handle_text = _format_generic_handles_from_parts(embedded_handle_parts)
        if kind == "bench":
            overlay["has_bench_block"] = True
            bench_text = _format_generic_material_from_parts(parts)
            if not bench_text:
                continue
            island_bench_text = _format_generic_island_bench_from_parts(parts, wall_run_text=bench_text)
            if "island" in anchor_label or "penisula" in anchor_label or "peninsula" in anchor_label:
                overlay["bench_tops_island"] = parsing._merge_text(overlay["bench_tops_island"], bench_text)
            elif "study" in overlay["original_room_label"].lower():
                overlay["bench_tops_other"] = parsing._merge_text(overlay["bench_tops_other"], bench_text)
            else:
                overlay["bench_tops_wall_run"] = parsing._merge_text(overlay["bench_tops_wall_run"], bench_text)
                if island_bench_text:
                    overlay["bench_tops_island"] = parsing._merge_text(overlay["bench_tops_island"], island_bench_text)
        elif kind == "base":
            text = _format_generic_material_from_parts(parts)
            overlay["has_explicit_base"] = True
            overlay["door_colours_base"] = parsing._merge_text(overlay["door_colours_base"], text)
            if text:
                last_cabinetry_material = text
            if embedded_handle_text and embedded_handle_text not in overlay["handles"]:
                overlay["has_handles_block"] = True
                overlay["handles"].append(embedded_handle_text)
        elif kind == "island":
            text = _format_generic_material_from_parts(parts)
            overlay["has_explicit_island"] = True
            overlay["door_colours_island"] = parsing._merge_text(overlay["door_colours_island"], text)
            if text:
                last_cabinetry_material = text
            if embedded_handle_text and embedded_handle_text not in overlay["handles"]:
                overlay["has_handles_block"] = True
                overlay["handles"].append(embedded_handle_text)
        elif kind == "overheads":
            text = _format_generic_material_from_parts(parts)
            overlay["has_explicit_overheads"] = True
            overlay["door_colours_overheads"] = parsing._merge_text(overlay["door_colours_overheads"], text)
            if text:
                last_cabinetry_material = text
            if embedded_handle_text and embedded_handle_text not in overlay["handles"]:
                overlay["has_handles_block"] = True
                overlay["handles"].append(embedded_handle_text)
        elif kind == "tall":
            text = _format_generic_material_from_parts(parts)
            overlay["has_explicit_tall"] = True
            overlay["door_colours_tall"] = parsing._merge_text(overlay["door_colours_tall"], text)
            if text:
                last_cabinetry_material = text
            if embedded_handle_text and embedded_handle_text not in overlay["handles"]:
                overlay["has_handles_block"] = True
                overlay["handles"].append(embedded_handle_text)
        elif kind == "toe_kick":
            text = _format_generic_material_from_parts(parts)
            text = re.sub(r"(?i)^kickboard\s+", "", text).strip(" -;,")
            if last_cabinetry_material and text and text.lower() in {"laminate", "as doors", "floating vanity"}:
                text = parsing.normalize_brand_casing_text(f"{last_cabinetry_material} - {text}")
            elif last_cabinetry_material and not text:
                text = last_cabinetry_material
            if text and text not in overlay["toe_kick"]:
                overlay["toe_kick"].append(text)
        elif kind == "floating_shelf":
            overlay["has_floating_shelf_block"] = True
            text = _format_generic_material_from_parts(parts)
            overlay["floating_shelf"] = parsing._merge_text(overlay["floating_shelf"], text)
        elif kind == "handles":
            overlay["has_handles_block"] = True
            handle_text = _format_generic_handles_from_parts(parts)
            if handle_text and not _looks_like_placeholder_entry(handle_text) and handle_text not in overlay["handles"]:
                overlay["handles"].append(handle_text)
            soft_close_text = " ".join(
                _meaningful_generic_values(
                    [
                        *parts.get("mechanism", []),
                        *parts.get("style", []),
                        *parts.get("note", []),
                    ]
                )
            )
            if "soft close" in soft_close_text.lower():
                overlay["drawers_soft_close"] = "Soft Close"
                overlay["hinges_soft_close"] = "Soft Close"
        elif kind == "soft_close":
            soft_close_text = " ".join(_meaningful_generic_values(parts.get("note", [])))
            if "soft close" in soft_close_text.lower():
                overlay["drawers_soft_close"] = "Soft Close"
                overlay["hinges_soft_close"] = "Soft Close"
        elif kind == "flooring":
            overlay["has_flooring_block"] = True
            text = _format_generic_flooring_from_parts(parts)
            overlay["flooring"] = parsing._merge_text(overlay["flooring"], text)
        elif kind == "sink":
            overlay["has_sink_block"] = True
            text = _format_generic_fixture_from_parts(parts, kind=kind, anchor_label=anchor_label)
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], text)
        elif kind == "tap":
            overlay["has_tap_block"] = True
            text = _format_generic_fixture_from_parts(parts, kind=kind, anchor_label=anchor_label)
            overlay["tap_info"] = parsing._merge_text(overlay["tap_info"], text)
        elif kind == "basin":
            overlay["has_basin_block"] = True
            text = _format_generic_fixture_from_parts(parts, kind=kind, anchor_label=anchor_label)
            overlay["basin_info"] = parsing._merge_text(overlay["basin_info"], text)
        elif kind == "accessories":
            overlay["has_accessories_block"] = True
            anchor_text = str(block.get("anchor_label", "") or "")
            anchor_lower = _normalize_generic_row_label(anchor_text)
            if (
                anchor_lower in {"robe hook", "hand towel rail", "towel rail", "toilet roll holder", "toilet suite"}
                and not _room_label_is_wet_area(overlay["original_room_label"])
            ):
                continue
            text = _format_generic_accessory_from_parts(parts, anchor_label=anchor_text)
            if text and text not in overlay["accessories"]:
                overlay["accessories"].append(text)
        elif kind == "other":
            text = _format_generic_fixture_from_parts(parts, kind=kind, anchor_label=anchor_label) or _format_generic_material_from_parts(parts)
            if text:
                overlay["other_items"].append({"label": parsing.normalize_space(str(block.get("anchor_label", "") or "")), "value": text})
    return overlay


def _generic_overlay_room_label(
    section: dict[str, Any],
    layout_rows: list[dict[str, Any]],
    *,
    documents: list[dict[str, object]] | None = None,
) -> str:
    original_label = parsing.normalize_space(str(section.get("original_section_label", "") or ""))
    if not re.match(r"(?i)^additional\b", original_label):
        return original_label
    for row in layout_rows:
        label = _normalize_generic_row_label(str(row.get("row_label", "") or ""))
        value = _clean_generic_fragment(_row_region_text(row, "value_text", "value_region_text", "notes_text", "notes_region_text"))
        if not value:
            continue
        if label == "location":
            value = re.sub(r"(?i)\bwet area location\b", "", value)
            value = parsing.normalize_space(value).strip(" -;,")
            if value and not _is_generic_placeholder_text(value) and "additional" not in value.lower():
                return parsing.normalize_space(value).title()
    document_location = _extract_additional_room_location_from_documents(section, documents or [])
    if document_location:
        return document_location
    return original_label


def _extract_additional_room_location_from_documents(section: dict[str, Any], documents: list[dict[str, object]]) -> str:
    original_label = parsing.normalize_space(str(section.get("original_section_label", "") or ""))
    if not re.match(r"(?i)^additional\b", original_label):
        return ""
    file_name = parsing.normalize_space(str(section.get("file_name", "") or ""))
    page_nos = [int(page_no) for page_no in section.get("page_nos", []) if str(page_no).isdigit() or isinstance(page_no, int)]
    if not file_name or not page_nos:
        return ""
    label_pattern = re.escape(original_label)
    location_patterns = (
        rf"(?is){label_pattern}.*?Location\s*([A-Za-z0-9 /&'()-]+?)\s*(?:Wet Area Location|Location|Manufacturer|Benchtop|Kitchen Sink|Kitchen Tapware|$)",
        rf"(?is){label_pattern}.*?Wet Area Location\s*([A-Za-z0-9 /&'()-]+?)\s*(?:Location|Manufacturer|Benchtop|$)",
        r"(?is)\bAdditional(?:\s+Wet\s+Area|\s+Bath/Ensuite/Powder|\s+Kitchen/Butlers/Kitchenette)?\b.{0,320}?\b(?:Wet Area )?Location\s*([A-Za-z0-9 /&'()-]+?)\s*(?:Location|Manufacturer|Benchtop|Kitchen Sink|Kitchen Tapware|Basin|Shower|Sink|$)",
    )
    for document in documents:
        if parsing.normalize_space(str(document.get("file_name", "") or "")) != file_name:
            continue
        pages = [page for page in document.get("pages", []) if isinstance(page, dict)]
        for page_no in page_nos:
            page = next((item for item in pages if int(item.get("page_no", 0) or 0) == page_no), None)
            if not page:
                continue
            raw_text = str(page.get("raw_text", "") or page.get("text", "") or "")
            searchable_text = _normalize_generic_document_search_text(raw_text)
            text = parsing.normalize_space(searchable_text)
            if not text:
                continue
            for pattern in location_patterns:
                match = re.search(pattern, text)
                if not match:
                    continue
                candidate = parsing.normalize_space(match.group(1)).strip(" -;,")
                candidate = re.sub(r"(?i)\b(?:wet area )?location\b", "", candidate).strip(" -;,")
                if candidate and not _is_generic_placeholder_text(candidate) and "additional" not in candidate.lower():
                    return parsing.source_room_label(candidate)
            additional_index = raw_text.lower().find("additional")
            if additional_index >= 0:
                focused = parsing.normalize_space(
                    _normalize_generic_document_search_text(raw_text[additional_index : additional_index + 420])
                )
                for match in re.finditer(
                    r"(?is)\b(?:wet area )?location\s*([A-Za-z0-9 /&'()-]+?)\s*(?:location|manufacturer|benchtop|kitchen sink|kitchen tapware|basin|shower|sink|$)",
                    focused,
                ):
                    candidate = parsing.normalize_space(match.group(1)).strip(" -;,")
                    candidate = re.sub(r"(?i)\b(?:wet area )?location\b", "", candidate).strip(" -;,")
                    if not candidate:
                        continue
                    lowered = candidate.lower()
                    if _is_generic_placeholder_text(candidate):
                        continue
                    if "additional" in lowered:
                        continue
                    if lowered in {"centre of sink", "center of sink", "corner of sink", "centre of basin", "center of basin", "corner of tub"}:
                        continue
                    return parsing.source_room_label(candidate)
    return ""


def _resolve_generic_room_label_from_documents(row: dict[str, Any], documents: list[dict[str, object]]) -> str:
    original_label = parsing.normalize_space(str(row.get("original_room_label", "") or ""))
    source_file = parsing.normalize_space(str(row.get("source_file", "") or ""))
    page_refs = parsing.normalize_space(str(row.get("page_refs", "") or ""))
    if not original_label or not source_file or not page_refs:
        return ""
    if not (re.match(r"(?i)^additional\b", original_label) or original_label.lower() in {"vanity", "wet area"}):
        return ""
    page_nos = [int(part) for part in re.split(r"\s*,\s*", page_refs) if part.isdigit()]
    if not page_nos:
        return ""
    if re.match(r"(?i)^additional\b", original_label):
        resolved = _extract_additional_room_location_from_documents(
            {
                "original_section_label": original_label,
                "file_name": source_file,
                "page_nos": page_nos,
            },
            documents,
        )
        if resolved:
            return resolved
    location_pattern = re.compile(
        r"(?is)\b(?:wet area )?location\s*([A-Za-z0-9 /&'()-]+?)\s*(?:location|manufacturer|benchtop|kitchen sink|kitchen tapware|basin|shower|sink|$)"
    )
    room_candidate_pattern = re.compile(
        r"(?i)\b(?:Guest\s+Ensuite\s*\d*|Master Ensuite|Ensuite\s*\d*|Bathroom|Powder|Laundry|Kitchen|Pantry|Butlers/?WIP|Rumpus|Study|Study Desk|Make Up Desk)\b"
    )
    for document in documents:
        if parsing.normalize_space(str(document.get("file_name", "") or "")) != source_file:
            continue
        pages = [page for page in document.get("pages", []) if isinstance(page, dict)]
        for page_no in page_nos:
            page = next((item for item in pages if int(item.get("page_no", 0) or 0) == page_no), None)
            if not page:
                continue
            raw_text = str(page.get("raw_text", "") or page.get("text", "") or "")
            if not raw_text:
                continue
            focused = parsing.normalize_space(_normalize_generic_document_search_text(raw_text[:1200]))
            for match in location_pattern.finditer(focused):
                candidate = parsing.normalize_space(match.group(1)).strip(" -;,")
                candidate = re.sub(r"(?i)\b(?:wet area )?location\b", "", candidate).strip(" -;,")
                if not candidate or _is_generic_placeholder_text(candidate):
                    continue
                if "additional" in candidate.lower():
                    continue
                room_match = room_candidate_pattern.search(candidate)
                if room_match:
                    return parsing.source_room_label(room_match.group(0))
            room_match = room_candidate_pattern.search(focused)
            if room_match:
                return parsing.source_room_label(room_match.group(0))
    return ""


def _normalize_generic_document_search_text(raw_text: str) -> str:
    text = str(raw_text or "")
    if not text:
        return ""
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Z])", " ", text)
    for marker in (
        "Wet Area Location",
        "Location",
        "Manufacturer",
        "Range",
        "Model",
        "Colour & Finish",
        "Colour",
        "Finish",
        "Profile",
        "Kitchen Sink",
        "Kitchen Tapware",
        "Pantry Sink",
        "Pantry Tapware",
        "Laundry Trough",
        "Laundry Tapware",
        "Vanity Basin",
        "Vanity Basin Tapware",
        "Benchtop",
        "Sink",
        "Tapware",
        "Basin",
        "Shower",
    ):
        marker_pattern = re.escape(marker)
        text = re.sub(rf"(?i)\b({marker_pattern})(?=[A-Z0-9])", r"\1 ", text)
    return text


def _polish_generic_layout_room(row: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    overlay = overlay or {}
    polished = dict(row)
    room_key = parsing.same_room_identity(str(polished.get("original_room_label", "")), str(polished.get("room_key", "")))
    if overlay.get("original_room_label"):
        polished["original_room_label"] = parsing._prefer_more_specific_room_label(
            str(polished.get("original_room_label", "")),
            str(overlay.get("original_room_label", "")),
        )
        room_key = parsing.same_room_identity(str(polished.get("original_room_label", "")), str(polished.get("room_key", "")))
    existing_bench_fields = (
        str(polished.get("bench_tops_wall_run", "") or ""),
        str(polished.get("bench_tops_island", "") or ""),
        str(polished.get("bench_tops_other", "") or ""),
        " | ".join(str(value or "") for value in polished.get("bench_tops", []) if parsing.normalize_space(str(value or ""))),
    )
    if overlay.get("has_bench_block") or any(
        overlay.get(field_name) for field_name in ("bench_tops_wall_run", "bench_tops_island", "bench_tops_other")
    ):
        polished["bench_tops"] = []
        polished["bench_tops_wall_run"] = ""
        polished["bench_tops_island"] = ""
        polished["bench_tops_other"] = ""
    material_field_rules = (
        ("door_colours_base", overlay.get("has_explicit_base"), "material"),
        ("door_colours_overheads", overlay.get("has_explicit_overheads"), "material"),
        ("door_colours_tall", overlay.get("has_explicit_tall"), "material"),
        ("door_colours_island", overlay.get("has_explicit_island"), "material"),
        ("door_colours_bar_back", overlay.get("has_explicit_bar_back"), "material"),
        ("floating_shelf", overlay.get("has_floating_shelf_block"), "material"),
    )
    for field_name, explicit_flag, field_kind in material_field_rules:
        existing_value = str(polished.get(field_name, "") or "")
        overlay_value = str(overlay.get(field_name, "") or "")
        if explicit_flag or overlay_value or (explicit_flag and _looks_like_generic_field_noise(existing_value, field=field_kind)):
            polished[field_name] = ""
    for field_name in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_base",
        "door_colours_overheads",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "floating_shelf",
        "sink_info",
        "tap_info",
        "basin_info",
        "drawers_soft_close",
        "hinges_soft_close",
        "source_file",
        "page_refs",
        "evidence_snippet",
    ):
        if overlay.get(field_name):
            polished[field_name] = overlay[field_name]
    for field_name in ("has_explicit_base", "has_explicit_overheads", "has_explicit_tall", "has_explicit_island", "has_explicit_bar_back"):
        if overlay.get(field_name):
            polished[field_name] = True
    if overlay.get("toe_kick"):
        polished["toe_kick"] = _sanitize_generic_material_entries(
            [entry for entry in overlay["toe_kick"] if not _looks_like_placeholder_entry(entry)],
            field_name="toe_kick",
            room_key=room_key,
        )
    if any(overlay.get(field_name) for field_name in ("bench_tops_wall_run", "bench_tops_island", "bench_tops_other")):
        polished["bench_tops"] = [
            value
            for value in (
                polished.get("bench_tops_wall_run", ""),
                polished.get("bench_tops_island", ""),
                polished.get("bench_tops_other", ""),
            )
            if parsing.normalize_space(str(value or ""))
        ]
    if overlay.get("has_handles_block"):
        overlay_handles = parsing._coerce_string_list(overlay.get("handles", []))
        if overlay_handles:
            polished["handles"] = _merge_generic_handle_entries(
                parsing._coerce_string_list(polished.get("handles", [])),
                overlay_handles,
            )
        else:
            polished["handles"] = []
    elif overlay.get("handles"):
        polished["handles"] = _merge_generic_handle_entries([], parsing._coerce_string_list(overlay.get("handles", [])))
    if overlay.get("has_accessories_block") and overlay.get("accessories"):
        polished["accessories"] = _merge_list_field(polished.get("accessories", []), overlay.get("accessories", []))
    elif overlay.get("has_accessories_block"):
        polished["accessories"] = []
    elif overlay.get("accessories"):
        polished["accessories"] = _merge_list_field(polished.get("accessories", []), overlay.get("accessories", []))
    if overlay.get("other_items"):
        polished["other_items"] = parsing._merge_other_items(polished.get("other_items", []), overlay.get("other_items", []))
    if overlay.get("has_sink_block") and overlay.get("sink_info"):
        polished["sink_info"] = overlay.get("sink_info", "")
    elif overlay.get("has_sink_block") and (
        _looks_like_placeholder_fixture_text(polished.get("sink_info", ""))
        or _looks_like_generic_field_noise(str(polished.get("sink_info", "") or ""), field="fixture")
    ):
        polished["sink_info"] = ""
    elif overlay.get("sink_info"):
        polished["sink_info"] = overlay["sink_info"]
    if overlay.get("has_tap_block") and overlay.get("tap_info"):
        polished["tap_info"] = overlay.get("tap_info", "")
    elif overlay.get("has_tap_block") and (
        _looks_like_placeholder_fixture_text(polished.get("tap_info", ""))
        or _looks_like_generic_field_noise(str(polished.get("tap_info", "") or ""), field="fixture")
    ):
        polished["tap_info"] = ""
    elif overlay.get("tap_info"):
        polished["tap_info"] = overlay["tap_info"]
    if overlay.get("has_basin_block") and overlay.get("basin_info"):
        polished["basin_info"] = overlay.get("basin_info", "")
    elif overlay.get("has_basin_block") and (
        _looks_like_placeholder_fixture_text(polished.get("basin_info", ""))
        or _looks_like_generic_field_noise(str(polished.get("basin_info", "") or ""), field="fixture")
    ):
        polished["basin_info"] = ""
    elif overlay.get("basin_info"):
        polished["basin_info"] = overlay["basin_info"]
    if overlay.get("has_flooring_block"):
        polished["flooring"] = overlay.get("flooring", "")
    else:
        flooring_value = parsing.normalize_space(str(polished.get("flooring", "") or ""))
        if flooring_value and (
            _looks_like_generic_field_noise(flooring_value, field="material")
            or any(
                token in flooring_value.lower()
                for token in (
                    "internal paint",
                    "internal ceiling",
                    "cornice",
                    "skirtings",
                    "architraves",
                    "internal walls",
                    "client initials",
                )
            )
        ):
            polished["flooring"] = ""
    if overlay.get("has_handles_block"):
        polished["handles"] = [entry for entry in polished.get("handles", []) if not _looks_like_placeholder_entry(entry)]
    if overlay.get("has_accessories_block"):
        polished["accessories"] = [entry for entry in polished.get("accessories", []) if not _looks_like_placeholder_entry(entry)]
    polished["accessories"] = parsing._filter_blacklisted_room_accessories(polished.get("accessories", []))
    polished["other_items"] = parsing._filter_blacklisted_room_other_items(polished.get("other_items", []))
    for field_name in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_base",
        "door_colours_overheads",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "floating_shelf",
    ):
        polished[field_name] = _sanitize_generic_material_field(
            polished.get(field_name, ""),
            field_name=field_name,
            room_key=room_key,
        )
    island_bench = parsing.normalize_space(str(polished.get("bench_tops_island", "") or ""))
    if (
        polished.get("bench_tops_wall_run") or polished.get("bench_tops_island")
    ) and _bench_field_is_combined_duplicate(
        str(polished.get("bench_tops_other", "") or ""),
        str(polished.get("bench_tops_wall_run", "") or ""),
        str(polished.get("bench_tops_island", "") or ""),
    ):
        polished["bench_tops_other"] = ""
    polished["bench_tops"] = [
        value
        for value in (
            polished.get("bench_tops_wall_run", ""),
            polished.get("bench_tops_island", ""),
            polished.get("bench_tops_other", ""),
        )
        if parsing.normalize_space(str(value or ""))
    ]
    polished["handles"] = _sanitize_generic_handle_entries(polished.get("handles", []))
    for field_name, fixture_kind in (("sink_info", "sink"), ("tap_info", "tap"), ("basin_info", "basin")):
        polished[field_name] = _sanitize_generic_fixture_field(polished.get(field_name, ""), kind=fixture_kind)
    return polished


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
    analysis = snapshot.get("analysis") or {}
    room_master_file = str(analysis.get("room_master_file", "") or "")
    overlays = _collect_clarendon_polish_overlays(documents, room_master_file=room_master_file)
    polished_rooms = [
        _polish_clarendon_room(dict(room), _select_clarendon_room_overlay(dict(room), overlays))
        for room in snapshot.get("rooms", [])
        if isinstance(room, dict)
    ]
    polished = dict(snapshot)
    polished["rooms"] = polished_rooms
    return parsing.apply_snapshot_cleaning_rules(polished, rule_flags=rule_flags)


def _apply_imperial_row_polish(
    snapshot: dict[str, Any],
    documents: list[dict[str, object]],
    builder_name: str,
    parser_strategy: str,
    rule_flags: Any = None,
    progress_callback: ProgressCallback = None,
) -> dict[str, Any]:
    if parser_strategy not in {"stable_hybrid", cleaning_rules.global_parser_strategy()} or "imperial" not in builder_name.strip().lower():
        return snapshot
    _report_progress(progress_callback, "imperial_polish", "Rebuilding Imperial room rows from source PDF text boundaries")
    rebuilt_rooms: dict[str, dict[str, Any]] = {}
    rebuilt_special_sections: list[dict[str, Any]] = []
    room_master_document, _ = parsing._select_imperial_room_master_document(documents)
    for document in documents:
        if room_master_document and document is not room_master_document:
            continue
        for section in parsing._collect_imperial_sections_for_document(document):
            if str(section.get("section_kind", "room") or "room") == "special":
                rebuilt_special_sections.append(parsing._imperial_special_section_from_section(section).model_dump())
                continue
            row = parsing._imperial_room_from_section(section).model_dump()
            rebuilt_rooms[str(row.get("room_key", ""))] = row
    polished = dict(snapshot)
    if rebuilt_rooms:
        polished["rooms"] = list(rebuilt_rooms.values())
    if rebuilt_special_sections:
        polished["special_sections"] = rebuilt_special_sections
    extracted_site_address = parsing._extract_site_address_from_documents(
        [room_master_document] if room_master_document else documents
    ) or parsing._extract_site_address_from_documents(documents)
    if extracted_site_address:
        polished["site_address"] = extracted_site_address
    polished = parsing.apply_snapshot_cleaning_rules(polished, rule_flags=rule_flags)
    polished = parsing.enrich_snapshot_rooms(polished, documents, rule_flags=rule_flags)
    polished = parsing.apply_snapshot_cleaning_rules(polished, rule_flags=rule_flags)
    if rebuilt_rooms and isinstance(polished.get("rooms"), list):
        authoritative_fields = (
            "bench_tops",
            "bench_tops_wall_run",
            "bench_tops_island",
            "bench_tops_other",
            "floating_shelf",
            "shelf",
            "splashback",
            "door_colours_overheads",
            "door_colours_base",
            "door_colours_tall",
            "door_colours_island",
            "door_colours_bar_back",
            "feature_colour",
            "toe_kick",
            "handles",
            "bulkheads",
            "has_explicit_overheads",
            "has_explicit_base",
            "has_explicit_tall",
            "has_explicit_island",
            "has_explicit_bar_back",
        )
        refreshed_rooms: list[dict[str, Any]] = []
        for room in polished.get("rooms", []):
            if not isinstance(room, dict):
                refreshed_rooms.append(room)
                continue
            authoritative = rebuilt_rooms.get(str(room.get("room_key", "")))
            if not authoritative:
                refreshed_rooms.append(room)
                continue
            refreshed = dict(room)
            for field in authoritative_fields:
                value = authoritative.get(field)
                if value in ("", [], None, False):
                    continue
                refreshed[field] = value
            refreshed_rooms.append(refreshed)
        polished["rooms"] = refreshed_rooms
    return polished


def _select_clarendon_room_overlay(room: dict[str, Any], overlays: dict[str, dict[str, Any]]) -> dict[str, Any]:
    room_key = str(room.get("room_key", ""))
    original_room_label = str(room.get("original_room_label", ""))
    overlay = _blank_clarendon_overlay()
    room_identity = parsing.same_room_identity(original_room_label, room_key)
    if room_key in overlays:
        _merge_clarendon_overlay(overlay, overlays[room_key])
    for overlay_key, overlay_value in overlays.items():
        if overlay_key == room_key:
            continue
        if parsing.same_room_identity(overlay_key) != room_identity:
            continue
        _merge_clarendon_overlay(overlay, overlay_value)
    fallback_keys: list[tuple[str, str]] = []
    if room_key == "walk_in_pantry":
        fallback_keys.append(("butlers_pantry", "full"))
    elif room_key == "butlers_pantry":
        fallback_keys.append(("walk_in_pantry", "full"))
    if _clarendon_is_vanity_room(room_key, original_room_label):
        if room_key in {"vanities", "vanity"}:
            counterpart = "vanity" if room_key == "vanities" else "vanities"
            fallback_keys.append((counterpart, "fixtures"))
        else:
            if not _clarendon_overlay_has_material_content(overlay):
                fallback_keys.append(("vanities", "full"))
            fallback_keys.append(("vanity", "fixtures"))
    for fallback_key, merge_mode in fallback_keys:
        if fallback_key == room_key or fallback_key not in overlays:
            continue
        if merge_mode == "fixtures":
            _merge_clarendon_fixture_overlay(overlay, overlays[fallback_key])
        else:
            _merge_clarendon_overlay(overlay, overlays[fallback_key])
    return overlay if _clarendon_overlay_has_content(overlay) else {}


def _collect_clarendon_polish_overlays(documents: list[dict[str, object]], room_master_file: str = "") -> dict[str, dict[str, Any]]:
    overlays: dict[str, dict[str, Any]] = {}
    for document in documents:
        file_name = str(document.get("file_name", ""))
        material_allowed = not room_master_file or file_name == room_master_file
        for page in document.get("pages", []):
            text = str(page.get("raw_text", page.get("text", "")) or page.get("text", "") or "")
            if not text:
                continue
            schedule_room_key = _clarendon_schedule_room_key(_clarendon_spacing_normalize(text))
            if schedule_room_key and material_allowed:
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
        "floating_shelf": "",
        "shelf": "",
        "door_panel_colours": [],
        "door_colours_overheads": "",
        "door_colours_base": "",
        "door_colours_island": "",
        "door_colours_bar_back": "",
        "feature_colour": "",
        "has_explicit_overheads": False,
        "has_explicit_base": False,
        "has_explicit_island": False,
        "has_explicit_bar_back": False,
        "toe_kick": "",
        "bulkheads": "",
        "handles": [],
        "led": "",
        "led_note": "",
        "accessories": [],
        "other_items": [],
        "sink_info": "",
        "basin_info": "",
        "tap_info": "",
        "splashback": "",
        "drawers_soft_close": "",
        "hinges_soft_close": "",
    }


def _merge_clarendon_overlay(target: dict[str, Any], candidate: dict[str, Any]) -> None:
    for key in ("bench_tops", "door_panel_colours", "handles", "accessories"):
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
        "floating_shelf",
        "shelf",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
        "feature_colour",
        "toe_kick",
        "bulkheads",
        "led",
        "led_note",
        "sink_info",
        "basin_info",
        "tap_info",
    ):
        target[key] = parsing._merge_text(target.get(key, ""), candidate.get(key, ""))
    target["other_items"] = parsing._merge_other_items(target.get("other_items", []), candidate.get("other_items", []))
    for key in ("has_explicit_overheads", "has_explicit_base", "has_explicit_island", "has_explicit_bar_back"):
        target[key] = bool(target.get(key, False) or candidate.get(key, False))
    target["splashback"] = _merge_clarendon_splashback(target.get("splashback", ""), candidate.get("splashback", ""))
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


def _merge_clarendon_fixture_overlay(target: dict[str, Any], candidate: dict[str, Any]) -> None:
    for key in ("sink_info", "basin_info", "tap_info"):
        target[key] = parsing._merge_text(target.get(key, ""), candidate.get(key, ""))


def _merge_clarendon_fixture_overlay(target: dict[str, Any], candidate: dict[str, Any]) -> None:
    for key in ("sink_info", "basin_info", "tap_info"):
        target[key] = parsing._merge_text(target.get(key, ""), candidate.get(key, ""))


def _clarendon_schedule_room_key(text: str) -> str:
    for pattern, room_key in CLARENDON_SCHEDULE_PAGE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return room_key
    generic_match = re.search(r"(?is)\b([A-Z][A-Z0-9/&' \-]{2,80}?)\s+COLOUR SCHEDULE\b", text)
    if generic_match:
        label = parsing.normalize_space(generic_match.group(1))
        label = re.sub(r"(?i)\s+JOINERY\s*$", "", label)
        key = parsing.source_room_key(label)
        if key:
            return key
    return ""


def _extract_clarendon_schedule_overlay(room_key: str, text: str) -> dict[str, Any]:
    overlay = _blank_clarendon_overlay()
    template_family = _clarendon_detect_template_family(text)
    lines = parsing._preprocess_chunk(_clarendon_spacing_normalize(text))
    benchtop_segments = _extract_clarendon_labeled_segments(
        text,
        r"BENCHTOP(?: COLOUR \d+)?(?:S)?",
        CLARENDON_FIELD_STOP_MARKERS,
    )
    for segment in benchtop_segments:
        _merge_clarendon_benchtop_segment(overlay, room_key, segment, template_family)

    overhead_value = parsing._first_value(parsing._collect_field(lines, ["Overhead Cupboards"]))
    base_value = parsing._first_value(parsing._collect_field(lines, ["Base Cupboards & Drawers", "Floor Mounted Vanity"]))
    island_value = parsing._first_value(parsing._collect_field(lines, ["Island Bench Base Cupboards & Drawers"]))
    bar_back_value = parsing._first_value(parsing._collect_field(lines, ["Island Bar Back"]))
    if overhead_value:
        overlay["has_explicit_overheads"] = True
    if base_value:
        overlay["has_explicit_base"] = True
    if island_value:
        overlay["has_explicit_island"] = True
    if bar_back_value:
        overlay["has_explicit_bar_back"] = True
    overlay["door_colours_overheads"] = parsing._merge_clean_group_text(overlay["door_colours_overheads"], overhead_value, cleaner=parsing._clean_door_colour_value)
    overlay["door_colours_base"] = parsing._merge_clean_group_text(overlay["door_colours_base"], base_value, cleaner=parsing._clean_door_colour_value)
    overlay["door_colours_island"] = parsing._merge_clean_group_text(overlay["door_colours_island"], island_value, cleaner=parsing._clean_door_colour_value)
    overlay["door_colours_bar_back"] = parsing._merge_clean_group_text(overlay["door_colours_bar_back"], bar_back_value, cleaner=parsing._clean_door_colour_value)
    if not parsing._has_explicit_door_group_markers(overlay):
        door_segments = parsing._collect_field(lines, parsing.DOOR_COLOUR_FIELD_PREFIXES)
        if door_segments:
            groups = parsing._split_door_colour_groups(door_segments)
            overlay["door_panel_colours"] = list(parsing._rebuild_door_panel_colours(groups))
            for key in ("door_colours_overheads", "door_colours_base", "door_colours_island", "door_colours_bar_back"):
                overlay[key] = parsing._merge_text(overlay[key], groups.get(key, ""))
            if parsing.normalize_space(groups.get("door_colours_overheads", "")):
                overlay["has_explicit_overheads"] = True
            if parsing.normalize_space(groups.get("door_colours_base", "")):
                overlay["has_explicit_base"] = True
            if parsing.normalize_space(groups.get("door_colours_island", "")):
                overlay["has_explicit_island"] = True
            if parsing.normalize_space(groups.get("door_colours_bar_back", "")):
                overlay["has_explicit_bar_back"] = True
            if (
                room_key in {"kitchen", "butlers_pantry", "walk_in_pantry", "laundry"}
                and not parsing.normalize_space(overlay.get("door_colours_overheads", ""))
                and parsing.normalize_space(overlay.get("door_colours_base", ""))
                and re.search(r"(?i)\b(?:upper cabinetry|upper cabinets|upper cabinet|upper cupboards|upper cupboard)\b", text)
            ):
                overlay["door_colours_overheads"] = overlay["door_colours_base"]
                overlay["has_explicit_overheads"] = True
    else:
        overlay["door_panel_colours"] = list(parsing._rebuild_door_panel_colours(overlay))

    notes_block = _extract_clarendon_notes_block(text)
    if notes_block:
        toe_kick = _clarendon_extract_note_value(notes_block, "KICKBOARDS")
        bulkhead = _clarendon_extract_note_value(notes_block, "BULKHEAD SHADOWLINE")
        if toe_kick:
            overlay["toe_kick"] = parsing._merge_text(overlay["toe_kick"], _clarendon_clean_toe_kick_text(toe_kick))
        if bulkhead:
            overlay["bulkheads"] = parsing._merge_text(overlay["bulkheads"], _clarendon_clean_bulkhead_text(bulkhead))
    else:
        toe_kick = _extract_clarendon_single_segment(text, r"KICKBOARDS?", CLARENDON_FIELD_STOP_MARKERS)
        bulkhead = _extract_clarendon_single_segment(text, r"BULKHEAD SHADOWLINE", CLARENDON_FIELD_STOP_MARKERS)
        if toe_kick:
            overlay["toe_kick"] = parsing._merge_text(overlay["toe_kick"], _clarendon_clean_toe_kick_text(toe_kick))
        if bulkhead:
            overlay["bulkheads"] = parsing._merge_text(overlay["bulkheads"], _clarendon_clean_bulkhead_text(bulkhead))

    splashback = _extract_clarendon_single_segment(text, r"(?:MIRROR\s+)?SPLASHBACK", CLARENDON_FIELD_STOP_MARKERS)
    if splashback:
        raw_splashback = f"Mirror Splashback - {splashback}" if re.search(r"(?i)\bMIRROR\s+SPLASHBACK\s*-", text) else splashback
        cleaned_splashback = _clarendon_clean_splashback_text(raw_splashback, room_key=room_key)
        if cleaned_splashback:
            overlay["splashback"] = parsing._merge_text(overlay["splashback"], cleaned_splashback)

    handle_segments = _extract_clarendon_handle_segments(text)
    overlay["handles"] = _clarendon_merge_unique_list(overlay["handles"], handle_segments)

    hinges_segment = _extract_clarendon_single_segment(text, r"DOOR HINGES", CLARENDON_FIELD_STOP_MARKERS)
    drawers_segment = _extract_clarendon_single_segment(text, r"DRAWER RUNNERS", CLARENDON_FIELD_STOP_MARKERS)
    if hinges_segment:
        overlay["hinges_soft_close"] = parsing.normalize_soft_close_value(hinges_segment, keyword="hinge") or parsing.normalize_soft_close_value(hinges_segment)
    if drawers_segment:
        overlay["drawers_soft_close"] = parsing.normalize_soft_close_value(drawers_segment, keyword="drawer") or parsing.normalize_soft_close_value(drawers_segment)
    return overlay


def _clarendon_detect_template_family(text: str) -> str:
    lowered = parsing.normalize_space(text).lower()
    if any(marker in lowered for marker in ("square edge handleless", "mirror splashback", "door/panel colour", "tightform edge laminate")):
        return "luxe_single_line"
    return "reference_37016"


def _clarendon_spacing_normalize(text: str) -> str:
    normalized = parsing.normalize_space(text)
    normalized = re.sub(
        r"(?i)(?<=\w)(KITCHEN COLOUR SCHEDULE|BUTLERS?\s+PANTRY COLOUR SCHEDULE|VANITIES COLOUR SCHEDULE|LAUNDRY COLOUR SCHEDULE|THEATRE(?: ROOM)? COLOUR SCHEDULE|RUMPUS(?: ROOM)? COLOUR SCHEDULE|RUMPUS\s*-\s*DESK JOINERY COLOUR SCHEDULE|STUDY COLOUR SCHEDULE|OFFICE COLOUR SCHEDULE|KITCHENETTE COLOUR SCHEDULE)",
        r" \1",
        normalized,
    )
    normalized = re.sub(r"(?i)(COLOUR SCHEDULE)(?=[A-Z])", r"\1 ", normalized)
    normalized = re.sub(r"(?i)(SUPPLIER DESCRIPTION DESIGN COMMENTS)(?=[A-Z])", r"\1 ", normalized)
    for label_pattern in (
        r"BENCHTOP(?: COLOUR \d+)?(?:S)?\s*-",
        r"DOOR COLOUR(?: \d+)?\s*-",
        r"DOOR/PANEL COLOUR(?: \d+)?\s*-",
        r"PLAIN GLASS DISPLAY CABINET",
        r"HANDLE \d+\s*-",
        r"HANDLES?\s*-",
        r"DOOR HINGES",
        r"DRAWER RUNNERS",
        r"KICKBOARDS?\s*:",
        r"BULKHEAD SHADOWLINE\s*:",
    ):
        normalized = re.sub(rf"(?i)(?<=\w)({label_pattern})", r" \1", normalized)
    normalized = re.sub(r"(?i)(PROFILE)(PLAIN GLASS DISPLAY CABINET)", r"\1 \2", normalized)
    normalized = re.sub(r"(?i)(PROFILE)(HANDLE \d+\s*-)", r"\1 \2", normalized)
    normalized = re.sub(r"(?i)(DOWN)(DRAWER LOCATION)", r"\1 \2", normalized)
    normalized = re.sub(r"(?i)(UP)(DOOR HINGES)", r"\1 \2", normalized)
    return normalized


def _merge_clarendon_splashback(left: Any, right: Any) -> str:
    left_text = parsing.normalize_space(str(left or ""))
    right_text = parsing.normalize_space(str(right or ""))
    merged = parsing._merge_text(left_text, right_text)
    candidates = [part.strip() for part in merged.split("|") if part.strip()]
    for candidate in candidates:
        if "mirror splashback" in candidate.lower():
            return candidate
    for candidate in candidates:
        if candidate:
            return candidate
    return ""


def _merge_clarendon_benchtop_segment(overlay: dict[str, Any], room_key: str, segment: str, template_family: str) -> None:
    cleaned = _clarendon_clean_benchtop_text(segment)
    if not cleaned:
        return
    lowered = parsing.normalize_space(segment).lower()
    if room_key == "kitchen":
        extracted = _extract_clarendon_kitchen_benchtops(segment, template_family)
        if extracted["wall_run"]:
            overlay["bench_tops_wall_run"] = parsing._merge_text(overlay["bench_tops_wall_run"], extracted["wall_run"])
        if extracted["island"]:
            overlay["bench_tops_island"] = parsing._merge_text(overlay["bench_tops_island"], extracted["island"])
        if extracted["other"]:
            overlay["bench_tops_other"] = parsing._merge_text(overlay["bench_tops_other"], extracted["other"])
        if any(extracted.values()):
            for value in extracted.values():
                if value:
                    overlay["bench_tops"].append(value)
            return
    if room_key == "kitchen" and any(token in lowered for token in ("cooktop run", "wall run", "wall bench", "wall side")):
        overlay["bench_tops_wall_run"] = parsing._merge_text(overlay["bench_tops_wall_run"], cleaned)
    elif room_key == "kitchen" and "island" in lowered:
        overlay["bench_tops_island"] = parsing._merge_text(overlay["bench_tops_island"], cleaned)
    else:
        overlay["bench_tops_other"] = parsing._merge_text(overlay["bench_tops_other"], cleaned)
    overlay["bench_tops"].append(cleaned)


def _extract_clarendon_kitchen_benchtops(segment: str, template_family: str) -> dict[str, str]:
    result = {"wall_run": "", "island": "", "other": ""}
    normalized = parsing.normalize_space(segment)
    if not normalized:
        return result
    match = re.search(
        r"(?is)^(?P<material>.+?)\s*-\s*(?P<wall>.+?)\s*-\s*TO\s+(?:THE\s+)?(?:COOKTOP\s*\+\s*SIDE\s+BENCHTOP|COOKTOP(?:\s+RUN)?|WALL RUN|WALL BENCH|WALL SIDE|SIDE BENCHTOP|SIDE BENCH)\s*(?:/|$)\s*(?P<island>.+?)\s*-\s*TO\s+(?:THE\s+)?ISLAND(?:\s+BENCH(?:TOP)?)?(?P<tail>.*)$",
        normalized,
    )
    if not match:
        return result
    material = _clarendon_clean_benchtop_text(match.group("material"))
    wall_detail = _clarendon_inline_text(match.group("wall"))
    island_detail = _clarendon_inline_text(f"{match.group('island')} {match.group('tail')}")
    if material and wall_detail:
        result["wall_run"] = _clarendon_clean_benchtop_text(f"{material} - {wall_detail}")
    if material and island_detail:
        result["island"] = _clarendon_clean_benchtop_text(f"{material} - {island_detail}")
    return result


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
        overlay = overlays.setdefault(_clarendon_detect_fixture_room_key(text), _blank_clarendon_overlay())
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
        sink_detail = (
            _extract_first_pattern(laundry_segment, r"EVERHARD INDUSTRIES.+?\([A-Z0-9.-]+\)")
            or _extract_first_pattern(laundry_segment, r"PRISM LARGE SINGLE BOWL UNDERMOUNT\s+[A-Z0-9.-]+")
            or _extract_first_pattern(laundry_segment, r"[A-Z0-9 /-]+UTILITY SINK\s*\([A-Z0-9.-]+\)")
        )
        tap_detail = _extract_first_pattern(laundry_segment, r"PINA SINK MIXER.+?\([A-Z0-9.-]+\)")
        if not tap_detail:
            tap_detail = _extract_first_pattern(laundry_segment, r"GASTON PULL DOWN MIXER[ A-Z0-9/-]*\([A-Z0-9.-]+\)")
        if not tap_detail:
            tap_detail = _extract_first_pattern(
                laundry_segment,
                r"[A-Z][A-Z0-9 /_-]*SINK MIXER[ A-Z0-9_./()-]*?(?=\s+(?:\d+MM CP QUARTER TURN WASHING MACHINE COCK\b|CABINETRY\b|Client Signature\b|$))",
            )
        washing_tap = _extract_first_pattern(laundry_segment, r"\d+MM CP QUARTER TURN WASHING MACHINE COCK\s*\([A-Z0-9.-]+\)")
        if sink_detail:
            sink_text = sink_detail
            if sink and sink.lower() not in sink_detail.lower():
                sink_text = f"{sink} - {sink_detail}"
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], _clarendon_clean_sink_text(sink_text))
        elif sink:
            overlay["sink_info"] = parsing._merge_text(overlay["sink_info"], _clarendon_clean_sink_text(sink))
        if tap_detail:
            tap_label = _extract_clarendon_value_after_label(laundry_segment, r"Tap Style")
            tap_label_clean = parsing.normalize_space(str(tap_label or ""))
            if (
                tap_label_clean
                and len(tap_label_clean) <= 40
                and "description details" not in tap_label_clean.lower()
                and tap_label_clean.lower() not in tap_detail.lower()
            ):
                tap_detail = f"{tap_label} - {tap_detail}"
            cleaned_tap = _clarendon_clean_tap_text(tap_detail)
            if washing_tap:
                cleaned_tap = f"{cleaned_tap}; {_clarendon_clean_tap_text(washing_tap)}"
            overlay["tap_info"] = parsing._merge_text(overlay["tap_info"], cleaned_tap)
        if "splashback:" in laundry_segment.lower():
            overlay["splashback"] = parsing._merge_text(overlay["splashback"], "Tiled splashback by others")

    return overlays


def _clarendon_detect_fixture_room_key(text: str) -> str:
    normalized = _clarendon_spacing_normalize(text)
    patterns = (
        r"\bBED\s*\d+\s+ENSUITE\b",
        r"\bENSUITE\s*\d+\b",
        r"\bPOWDER(?:\s+ROOM)?\s*\d+\b",
        r"\bMAIN BATHROOM\b",
        r"\bBATHROOM\b",
        r"\bENSUITE\b",
        r"\bPOWDER(?:\s+ROOM)?\b",
        r"\bVANITIES\b",
        r"\bVANITY\b",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            return parsing.source_room_key(match.group(0))
    return "vanity"


def _polish_clarendon_room(row: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    room_key = parsing.normalize_space(str(row.get("room_key", "")))
    polished = dict(row)
    overlay_present = _clarendon_overlay_has_content(overlay)
    is_vanity_room = _clarendon_is_vanity_room(room_key, str(row.get("original_room_label", "")))
    for key in ("has_explicit_overheads", "has_explicit_base", "has_explicit_island", "has_explicit_bar_back"):
        polished[key] = bool(row.get(key, False) or overlay.get(key, False))

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
    if room_key != "kitchen":
        if grouped_doors["door_colours_overheads"] and not polished.get("has_explicit_overheads", False):
            grouped_doors["door_colours_base"] = parsing._merge_clean_group_text(
                grouped_doors.get("door_colours_base", ""),
                grouped_doors["door_colours_overheads"],
                cleaner=parsing._clean_door_colour_value,
            )
            grouped_doors["door_colours_overheads"] = ""
        grouped_doors["door_colours_island"] = ""
        grouped_doors["door_colours_bar_back"] = ""
        polished["has_explicit_island"] = False
        polished["has_explicit_bar_back"] = False
    for key, value in grouped_doors.items():
        polished[key] = value
    polished["door_panel_colours"] = parsing._rebuild_door_panel_colours(polished)

    toe_kick = _clarendon_clean_toe_kick_text(overlay.get("toe_kick", "")) if overlay_present else _clarendon_clean_toe_kick_text(row.get("toe_kick", ""))
    bulkheads = _clarendon_clean_bulkhead_text(overlay.get("bulkheads", "")) if overlay_present else _clarendon_clean_bulkhead_text(row.get("bulkheads", ""))
    handles = _clarendon_clean_handles(overlay.get("handles", [])) if overlay_present else _clarendon_clean_handles(row.get("handles", []))
    polished["toe_kick"] = [toe_kick] if toe_kick else []
    polished["bulkheads"] = [bulkheads] if bulkheads else []
    polished["handles"] = handles

    polished["sink_info"] = _clarendon_select_fixture_text(
        row.get("sink_info", ""),
        overlay.get("sink_info", "") if overlay_present else "",
        fixture_kind="sink",
    )
    polished["basin_info"] = (
        _clarendon_select_fixture_text(
            row.get("basin_info", ""),
            overlay.get("basin_info", "") if overlay_present else "",
            fixture_kind="basin",
        )
        if is_vanity_room
        else ""
    )
    polished["tap_info"] = _clarendon_select_fixture_text(
        row.get("tap_info", ""),
        overlay.get("tap_info", "") if overlay_present else "",
        fixture_kind="tap",
    )
    overlay_splashback = _clarendon_clean_splashback_text(overlay.get("splashback", ""), room_key=room_key)
    current_splashback = _clarendon_clean_splashback_text(row.get("splashback", ""), room_key=room_key)
    polished["splashback"] = overlay_splashback or (current_splashback if room_key in {"kitchen", "laundry"} else "")
    polished["drawers_soft_close"] = _clarendon_select_soft_close(overlay.get("drawers_soft_close", "") if overlay_present else "", row.get("drawers_soft_close", ""), keyword="drawer")
    polished["hinges_soft_close"] = _clarendon_select_soft_close(overlay.get("hinges_soft_close", "") if overlay_present else "", row.get("hinges_soft_close", ""), keyword="hinge")
    return polished


def _clarendon_is_vanity_room(room_key: str, original_room_label: str) -> bool:
    identity = parsing.same_room_identity(room_key, original_room_label)
    return identity == "vanities" or any(token in identity for token in ("vanity", "bathroom", "ensuite", "powder"))


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
    match = re.search(
        rf"(?is){re.escape(label)}\s*:\s*(.+?)(?=(?:\*\s*[A-Z][A-Z /&']+\s*:|HANDLE \d+\s*-|HANDLES?\s*-|DOOR HINGES|DRAWER RUNNERS|$))",
        notes_block,
    )
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
    text = re.sub(r"(?i)^shadowline\s*:?\s*matching\b.*$", "", text)
    if _looks_like_clarendon_noise(text):
        return ""
    return text.strip(" -;,")


def _clarendon_clean_door_group_text(value: Any) -> str:
    entries = parsing._split_group_entries(value)
    cleaned = []
    for entry in entries:
        text = parsing._clean_door_colour_value(entry)
        text = re.sub(r"(?i)\b(?:plain glass|'?glazing bar'?) display cabinet with.*$", "", text)
        text = re.sub(r"(?i)\b(?:plain glass\s+)?display cabinet\b.*$", "", text)
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
    if "matching melamine finish" in lowered:
        return "Matching Melamine finish"
    return text


def _clarendon_clean_bulkhead_text(value: Any) -> str:
    text = _clarendon_clean_note_text(value)
    lowered = text.lower()
    if "bulkhead shadowline" in lowered and "matching" in lowered:
        cleaned = re.sub(r"(?i)^bulkhead shadowline\s*:?\s*", "", text)
        cleaned = cleaned.strip(" -;,.")
        if cleaned:
            cleaned = re.sub(r"(?i)^as\s+", "", cleaned)
            if cleaned.lower() == "matching melamine finish":
                cleaned = "matching Melamine finish"
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
    if re.search(r"(?i)\bmirror splashback\b", text):
        text = re.sub(r"(?i)^mirror splashback\s*:?\s*-?\s*", "", text)
        text = parsing.normalize_brand_casing_text(text)
        text = _clarendon_to_readable_text(text)
        text = _clarendon_inline_text(text)
        text = re.sub(r"(?i)\bkickboards?\b.*$", "", text).strip(" -;,")
        return f"Mirror Splashback - {text}" if text else "Mirror Splashback"
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
        text = re.sub(r"(?i)\*\s*NOTE\s*:.*$", "", text)
        text = re.sub(r"(?i)\b10MM DOOR OVERHANG TO UPPER CABINETS\b.*$", "", text)
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


def _clarendon_fixture_detail_score(value: str, fixture_kind: str) -> int:
    text = parsing.normalize_space(str(value or ""))
    if not text:
        return 0
    score = len(text)
    if re.search(r"\b[A-Z]{1,4}\d[A-Z0-9-]{2,}\b", text):
        score += 20
    if re.search(r"(?i)\b(?:chrome|white|stainless|brushed|black)\b", text):
        score += 8
    if fixture_kind == "tap" and re.search(r"(?i)\b(?:mixer|shepherds? crook|gooseneck|tap)\b", text):
        score += 12
    if fixture_kind == "sink" and re.search(r"(?i)\b(?:sink|bowl|undermount|drop-?in|tub)\b", text):
        score += 12
    if fixture_kind == "basin" and re.search(r"(?i)\b(?:basin|semi-?inset|undercounter|wall basin)\b", text):
        score += 12
    return score


def _clarendon_select_fixture_text(current_value: Any, overlay_value: Any, fixture_kind: str) -> str:
    current = _clarendon_clean_fixture_text(current_value, fixture_kind=fixture_kind)
    overlay = _clarendon_clean_fixture_text(overlay_value, fixture_kind=fixture_kind)
    if not current:
        return overlay
    if not overlay:
        return current
    current_lower = current.lower()
    overlay_lower = overlay.lower()
    if overlay_lower in current_lower and len(current) >= len(overlay) + 8:
        return current
    if current_lower in overlay_lower and len(overlay) >= len(current) + 8:
        return overlay
    current_score = _clarendon_fixture_detail_score(current, fixture_kind)
    overlay_score = _clarendon_fixture_detail_score(overlay, fixture_kind)
    return current if current_score >= overlay_score else overlay


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


def _clarendon_overlay_has_material_content(overlay: dict[str, Any]) -> bool:
    material_keys = (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "bench_tops",
        "floating_shelf",
        "shelf",
        "door_panel_colours",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
        "feature_colour",
        "toe_kick",
        "bulkheads",
        "splashback",
    )
    for key in material_keys:
        value = overlay.get(key, "")
        if isinstance(value, list):
            if any(parsing.normalize_space(str(item or "")) for item in value):
                return True
        elif parsing.normalize_space(str(value or "")):
            return True
    return False


def _stabilize_snapshot_layout(snapshot: dict[str, Any], builder_name: str, parser_strategy: str) -> dict[str, Any]:
    rooms = [dict(row) for row in snapshot.get("rooms", []) if isinstance(row, dict)]
    snapshot["rooms"] = _merge_rooms_by_source_identity(rooms)
    return snapshot


def _merge_rooms_by_source_identity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for row in rows:
        target_key = parsing.same_room_identity(str(row.get("original_room_label", "")), str(row.get("room_key", "")))
        if not target_key:
            continue
        current = dict(row)
        current["room_key"] = target_key
        current["original_room_label"] = parsing.source_room_label(str(row.get("original_room_label", "")), fallback_key=target_key)
        current["room_name"] = parsing.normalize_space(
            str(current.get("original_room_label", "") or target_key.replace("_", " "))
        )
        if target_key not in grouped:
            grouped[target_key] = current
            order.append(target_key)
            continue
        grouped[target_key] = _merge_single_room(grouped[target_key], current, stable_hybrid=True)
    return [grouped[room_key] for room_key in order]


def _room_has_meaningful_content(row: dict[str, Any]) -> bool:
    for field_name in (
        "bench_tops",
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "floating_shelf",
        "door_panel_colours",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_island",
        "door_colours_bar_back",
        "feature_colour",
        "toe_kick",
        "bulkheads",
        "handles",
        "led",
        "accessories",
        "other_items",
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


def _enrich_snapshot_appliances(snapshot: dict[str, Any], progress_callback: ProgressCallback = None, rule_flags: Any = None) -> dict[str, Any]:
    appliances = [row for row in snapshot.get("appliances", []) if isinstance(row, dict)]
    if not appliances:
        snapshot["appliances"] = []
        return snapshot
    if _is_imperial_builder_name(str(snapshot.get("builder_name", "") or "")):
        for appliance in appliances:
            if parsing.normalize_space(str(appliance.get("make", "") or "")):
                continue
            model_no = str(appliance.get("model_no", "") or "")
            if parsing._looks_like_appliance_placeholder_model(model_no):
                continue
            evidence_probe = parsing.normalize_space(
                " ".join(
                    [
                        str(appliance.get("evidence_snippet", "") or ""),
                        model_no,
                    ]
                )
            )
            if not evidence_probe:
                continue
            inferred_make = parsing._extract_contextual_appliance_make(
                evidence_probe,
                str(appliance.get("appliance_type", "") or ""),
                model_no,
            )
            if not inferred_make:
                inferred_make = parsing._guess_make(evidence_probe)
            if inferred_make:
                appliance["make"] = inferred_make
        snapshot["appliances"] = appliances
        return snapshot
    snapshot["appliances"] = appliance_official.enrich_appliance_rows(appliances, progress_callback=progress_callback, rule_flags=rule_flags)
    return snapshot
