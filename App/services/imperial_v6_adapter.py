from __future__ import annotations

import json
from copy import deepcopy
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from App.models import RoomRow


PDF_EXTRACTOR_PATH = str(Path(__file__).parent / "pdf_to_structured_json.py")

REMAP = {
    "KITCHEN": "kitchen",
    "PANTRY": "pantry",
    "LAUNDRY": "laundry",
    "POWDER": "powder",
    "BATHROOM": "bathroom",
    "MASTER ENSUITE": "master_ensuite",
    "WALK IN ROBE": "wir",
    "DRY BAR": "dry_bar",
    "KITCHEN & PANTRY": "kitchen",
}


def run_v6_extraction(pdf_path: str) -> dict:
    """Run pdf_to_structured_json.py as subprocess, return parsed JSON."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as handle:
        out_path = Path(handle.name)
    try:
        result = subprocess.run(
            [sys.executable, PDF_EXTRACTOR_PATH, pdf_path, str(out_path)],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"v6 extractor failed: {result.stderr}")
        return json.loads(out_path.read_text(encoding="utf-8"))
    finally:
        out_path.unlink(missing_ok=True)


def build_material_rows_from_v6_section(v6_section: dict, source_pdf: str) -> list[dict]:
    """v6 section -> raw material_rows (before finalize)."""
    section_title = str(v6_section.get("section_title") or "")
    items = _merge_adjacent_subrow_items(list(v6_section.get("items") or []))
    page_orders: dict[int, int] = {}
    rows: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        page_no = _item_page_no(item)
        page_orders[page_no] = page_orders.get(page_no, 0) + 1
        rows.append(_map_v6_item_to_material_row(item, source_pdf, page_orders[page_no], section_title))
    return rows


def build_review_rows_from_v6_section(v6_section: dict, source_pdf: str) -> list[dict]:
    """v6 section -> Claude review rows, preserving original item boundaries."""
    section_title = str(v6_section.get("section_title") or "")
    items = list(v6_section.get("items") or [])
    page_orders: dict[int, int] = {}
    rows: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        page_no = _item_page_no(item)
        page_orders[page_no] = page_orders.get(page_no, 0) + 1
        rows.append(_map_v6_item_to_material_row(item, source_pdf, page_orders[page_no], section_title))
    return rows


def build_room_from_v6_section(v6_section: dict, source_pdf: str, all_v6_sections=None) -> RoomRow:
    """v6 section -> complete RoomRow (material_rows + basic RoomRow fields)."""
    section_title = str(v6_section.get("section_title") or "")
    label = section_title.replace(" JOINERY SELECTION SHEET", "").strip()
    pages = v6_section.get("pages") or []
    material_rows = build_material_rows_from_v6_section(v6_section, source_pdf)
    review_rows = build_review_rows_from_v6_section(v6_section, source_pdf)
    room = RoomRow(
        room_key=_derive_room_key(section_title),
        original_room_label=label,
        room_order=0,
        v6_metadata=dict(v6_section.get("metadata") or {}),
        v6_review_rows=deepcopy(review_rows),
        material_rows=material_rows,
        source_file=source_pdf,
        page_refs=",".join(str(page) for page in pages),
        evidence_snippet="",
        confidence=0.85,
    )
    from App.services.imperial_v6_room_fields import populate_room_fields_from_v6

    populate_room_fields_from_v6(room, v6_section, all_v6_sections or [])
    return room


def _merge_adjacent_subrow_items(items: list[dict]) -> list[dict]:
    """Merge consecutive v6 items where the follow-up has empty area."""
    merged: list[dict] = []
    for item in items:
        cur_area = (item.get("area") or "").strip()
        if merged and not cur_area:
            parent = merged[-1]
            child_specs = (item.get("specs") or "").strip()
            if child_specs:
                parent_specs = (parent.get("specs") or "").strip()
                parent["specs"] = f"{parent_specs}\n{child_specs}" if parent_specs else child_specs
            if not (parent.get("supplier") or "").strip():
                parent["supplier"] = item.get("supplier", "")
            if not (parent.get("notes") or "").strip():
                parent["notes"] = item.get("notes", "")
        else:
            merged.append(dict(item))
    return merged


def _map_v6_item_to_material_row(item: dict, source_pdf: str, row_order: int, section_title: str) -> dict:
    source = item.get("_source") if isinstance(item.get("_source"), dict) else {}
    page_no = int(source.get("page") or 0)
    return {
        "area_or_item": item.get("area", ""),
        "supplier": item.get("supplier", ""),
        "specs_or_description": item.get("specs", ""),
        "notes": item.get("notes", ""),
        "tags": [],
        "page_no": page_no,
        "row_order": row_order,
        "confidence": 0.9,
        "provenance": _provenance(page_no, row_order, section_title),
        "needs_review": False,
        "issues": [],
        "repair_candidates": [],
        "repair_verdicts": [],
        "repair_log": [],
        "revalidation_issues": [],
        "revalidation_status": "passed",
    }


def _derive_room_key(section_title: str) -> str:
    title = section_title.replace(" JOINERY SELECTION SHEET", "").strip().upper()
    if title in REMAP:
        return REMAP[title]
    return re.sub(r"\s+", "_", title.lower())


def _item_page_no(item: dict) -> int:
    source = item.get("_source") if isinstance(item.get("_source"), dict) else {}
    return int(source.get("page") or 0)


def _provenance(page_no: int, row_order: int, section_title: str) -> dict[str, Any]:
    return {
        "raw": "v6_cell",
        "source_provider": "v6",
        "source_extractor": "pdf_to_structured_json_v6",
        "section_title": section_title,
        "visual_sort_key": [page_no, float(row_order), 0, 0],
    }
