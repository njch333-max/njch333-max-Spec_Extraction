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
    row = {
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
    display_payload = _display_payload_for_v6_item(item)
    if display_payload.get("display_lines"):
        row["display_lines"] = display_payload["display_lines"]
    if display_payload.get("display_groups"):
        row["display_groups"] = display_payload["display_groups"]
    return row


def _display_payload_for_v6_item(item: dict) -> dict[str, list]:
    specs = item.get("specs", "") or ""
    supplier = item.get("supplier", "") or ""
    supplier_lines = [line.strip() for line in str(supplier).splitlines() if line.strip()]
    n_supplier = len(supplier_lines)
    area = str(item.get("area", "") or "")
    is_handles_row = "HANDLES" in area.upper()

    spec_blocks = _split_specs_into_blocks(str(specs))
    flat_spec_lines = [line for block in spec_blocks for line in block]
    merged_spec = " | ".join(" ".join(block) for block in spec_blocks if block)

    if is_handles_row:
        return _handle_display_payload(str(supplier), supplier_lines, flat_spec_lines)

    if n_supplier <= 1:
        prefix = supplier_lines[0] if n_supplier == 1 else ""
        if not merged_spec:
            return {"display_lines": [prefix] if prefix else []}
        return {"display_lines": [_join_supplier_spec(prefix, merged_spec)]}

    if len(flat_spec_lines) == n_supplier:
        return {
            "display_lines": [_join_supplier_spec(supplier_lines[index], flat_spec_lines[index]) for index in range(n_supplier)]
        }

    hinted_supplier = f"*{' / '.join(supplier_lines)}*"
    if merged_spec:
        return {"display_lines": [f"{hinted_supplier} - {merged_spec}"]}
    return {"display_lines": [hinted_supplier]}


def _handle_display_payload(supplier: str, supplier_lines: list[str], flat_spec_lines: list[str]) -> dict[str, list]:
    n_supplier = len(supplier_lines)
    if not flat_spec_lines:
        return {"display_lines": []}

    supplier_prefix = supplier.strip() if supplier else ""
    if n_supplier <= 1:
        return {
            "display_lines": [_join_supplier_spec(supplier_prefix, line) for line in flat_spec_lines],
            "display_groups": [{"supplier": supplier_prefix, "lines": flat_spec_lines}],
        }

    if len(flat_spec_lines) == n_supplier:
        groups = [{"supplier": supplier_lines[index], "lines": [flat_spec_lines[index]]} for index in range(n_supplier)]
        return {
            "display_lines": [_join_supplier_spec(supplier_lines[index], flat_spec_lines[index]) for index in range(n_supplier)],
            "display_groups": groups,
        }

    if len(flat_spec_lines) > n_supplier and len(flat_spec_lines) % n_supplier == 0:
        chunk_size = len(flat_spec_lines) // n_supplier
        display_lines: list[str] = []
        display_groups: list[dict[str, list[str] | str]] = []
        for index, supplier_line in enumerate(supplier_lines):
            chunk = flat_spec_lines[index * chunk_size : (index + 1) * chunk_size]
            display_groups.append({"supplier": supplier_line, "lines": chunk})
            display_lines.extend(_join_supplier_spec(supplier_line, line) for line in chunk)
        return {"display_lines": display_lines, "display_groups": display_groups}

    return {"display_lines": [_join_supplier_spec(supplier_prefix, line) for line in flat_spec_lines]}


def _split_specs_into_blocks(specs: str) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for raw_line in str(specs or "").splitlines():
        line = raw_line.strip()
        if line:
            current.append(line)
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    return blocks


def _join_supplier_spec(supplier: str, spec: str) -> str:
    supplier = (supplier or "").strip()
    spec = (spec or "").strip()
    if not supplier:
        return spec
    if not spec:
        return supplier
    if spec.lower().startswith(f"{supplier.lower()} - "):
        return spec
    return f"{supplier} - {spec}"


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
