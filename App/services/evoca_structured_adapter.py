from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any

from App.models import AnalysisMeta, ApplianceRow, RoomRow, SnapshotPayload, SpecialSectionRow


SCHEMA_VERSION = "evoca_structured_v0"
SOURCE_PROVIDER = "evoca_structured_v0"
SOURCE_EXTRACTOR = "evoca_structured_extractor"

INCLUDED_SECTION_CODES = {"15", "17", "20", "23", "24", "25"}
SKIPPED_SECTION_CODES = {"16", "18", "19", "21", "22"}

MIXER_GROUPS = {"sink mixer", "tub mixer", "basin mixer"}
IGNORED_APPLIANCE_GROUPS = {"hot water unit", "accessories"}
NON_APPLIANCE_LABELS = {"hot water unit", "water filter", "insinkerator", "air-conditioning", "air conditioning"}
SOURCE_DOCUMENT_METADATA_KEYS = {
    "file_name",
    "original_name",
    "stored_name",
    "path",
    "source_pdf",
    "role",
    "file_role",
    "mime_type",
    "size_bytes",
}

MATERIAL_RETAIN_FIELDS = (
    "bench_tops_wall_run",
    "bench_tops_island",
    "bench_tops_other",
    "floating_shelf",
    "shelf",
    "door_colours_overheads",
    "door_colours_base",
    "door_colours_tall",
    "door_colours_island",
    "door_colours_bar_back",
    "splashback",
)


def build_evoca_snapshot_from_structured(
    structured: dict[str, Any],
    job_no: str,
    source_document: str | dict[str, Any] | None = None,
) -> SnapshotPayload:
    """Build a SnapshotPayload from standalone Evoca structured JSON.

    This is intentionally a pure adapter. It does not run PDF parsing, does not
    read raw PDF text, and does not connect itself to production dispatch.
    """

    _validate_structured_payload(structured)
    source_file = _source_file(structured, source_document)
    source_documents = [_source_document_payload(structured, source_document, source_file)]
    room_candidates: dict[str, RoomRow] = {}
    section_group_evidence: list[SpecialSectionRow] = []
    appliances: list[ApplianceRow] = []

    for section in structured.get("sections") or []:
        if not isinstance(section, dict):
            continue
        section_code = _section_code(section)
        if section_code in SKIPPED_SECTION_CODES or section_code not in INCLUDED_SECTION_CODES:
            continue
        for room in section.get("rooms") or []:
            if not isinstance(room, dict):
                continue
            mapped = map_evoca_room(section, room, source_file=source_file)
            _merge_room_candidate(room_candidates, mapped)
        for group in section.get("groups") or []:
            if isinstance(group, dict):
                section_group_evidence.append(_special_section_from_group(section, group, source_file))
        if section_code == "17":
            appliances.extend(map_evoca_appliances(section, source_file=source_file))

    for section in structured.get("sections") or []:
        if isinstance(section, dict) and _section_code(section) in {"23", "24", "25"}:
            _apply_finish_section_enrichment(room_candidates, section)

    retained_rooms: list[RoomRow] = []
    unretained_evidence: list[SpecialSectionRow] = []
    for room in sorted(room_candidates.values(), key=lambda item: item.room_order):
        if _room_has_true_material_evidence(room):
            retained_rooms.append(room)
        else:
            evidence = _special_section_from_unretained_room(room)
            if evidence is not None:
                unretained_evidence.append(evidence)

    return SnapshotPayload(
        job_no=str(job_no),
        builder_name="Evoca",
        source_kind="spec",
        analysis=AnalysisMeta(
            mode="layout_parser",
            parser_strategy=SCHEMA_VERSION,
            layout_attempted=True,
            layout_succeeded=True,
            layout_provider=SOURCE_EXTRACTOR,
            layout_mode=SCHEMA_VERSION,
            layout_pages=_structured_layout_pages(structured),
            docling_attempted=False,
            docling_succeeded=False,
            openai_attempted=False,
            openai_succeeded=False,
            vision_attempted=False,
            vision_succeeded=False,
            note="Built from standalone Evoca structured JSON; runtime dispatch not wired by adapter module.",
        ),
        rooms=retained_rooms,
        special_sections=[*section_group_evidence, *unretained_evidence],
        appliances=appliances,
        source_documents=source_documents,
    )


def map_evoca_room(section: dict[str, Any], room: dict[str, Any], source_file: str = "") -> RoomRow:
    room_label = _text(room.get("room_label")) or _text(room.get("room_key"))
    room_key = _room_key(room.get("room_key") or room_label)
    mapped = RoomRow(
        room_key=room_key,
        original_room_label=room_label,
        room_name=room_label,
        room_order=_room_order(section, room),
        source_file=source_file,
        page_refs=_page_range(room) or _page_range(section),
        confidence=0.85,
    )

    for group in room.get("groups") or []:
        if not isinstance(group, dict):
            continue
        material_rows = _material_rows_from_group(section, room, group, source_file)
        mapped.material_rows.extend(material_rows)
        _apply_room_group_mapping(mapped, _section_code(section), group)

    mapped.evidence_snippet = _first_evidence_snippet(mapped.material_rows)
    return mapped


def map_evoca_appliances(section: dict[str, Any], source_file: str = "") -> list[ApplianceRow]:
    if _section_code(section) != "17":
        return []
    appliances: list[ApplianceRow] = []
    for group in section.get("groups") or []:
        if not isinstance(group, dict):
            continue
        group_label = _text(group.get("group_label"))
        if _key(group_label) in IGNORED_APPLIANCE_GROUPS:
            continue
        for row in group.get("rows") or []:
            if not isinstance(row, dict) or row.get("is_diagnostic"):
                continue
            label = _text(row.get("label"))
            value = _text(row.get("value"))
            if not label or not value or _key(label) in NON_APPLIANCE_LABELS:
                continue
            appliances.append(
                ApplianceRow(
                    appliance_type=label,
                    make="",
                    model_no=value,
                    source_file=source_file,
                    page_refs=_row_page_ref(row, group),
                    evidence_snippet=_appliance_evidence(group_label, label, value),
                    confidence=0.85,
                )
            )
    return appliances


def _validate_structured_payload(structured: dict[str, Any]) -> None:
    if not isinstance(structured, dict):
        raise ValueError("Evoca structured payload must be a dict.")
    if structured.get("builder") != "Evoca":
        raise ValueError("Evoca structured adapter only accepts builder = Evoca.")
    if structured.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"Evoca structured adapter requires schema_version = {SCHEMA_VERSION}.")
    if not isinstance(structured.get("sections"), list):
        raise ValueError("Evoca structured adapter requires sections[].")


def _source_file(structured: dict[str, Any], source_document: str | dict[str, Any] | None) -> str:
    if isinstance(source_document, dict):
        return _text(source_document.get("file_name") or source_document.get("path") or source_document.get("source_pdf"))
    if source_document:
        return _text(source_document)
    return _text(structured.get("source_pdf") or structured.get("document_name"))


def _source_document_payload(
    structured: dict[str, Any],
    source_document: str | dict[str, Any] | None,
    source_file: str,
) -> dict[str, str]:
    if isinstance(source_document, dict):
        payload = {
            str(key): _text(value)
            for key, value in source_document.items()
            if key in SOURCE_DOCUMENT_METADATA_KEYS and _text(value)
        }
    else:
        payload = {}
    payload.setdefault("file_name", _text(structured.get("document_name")) or source_file)
    payload.setdefault("path", _text(structured.get("source_pdf")) or source_file)
    payload.setdefault("source_provider", SOURCE_PROVIDER)
    return payload


def _structured_layout_pages(structured: dict[str, Any]) -> list[int]:
    pages: list[int] = []
    for page in structured.get("pages") or []:
        if not isinstance(page, dict):
            continue
        try:
            page_no = int(page.get("page_no") or 0)
        except (TypeError, ValueError):
            page_no = 0
        if page_no and page_no not in pages:
            pages.append(page_no)
    return pages


def _section_code(section: dict[str, Any]) -> str:
    code = _text(section.get("section_code"))
    if code:
        return code
    match = re.match(r"\s*(\d{1,2})\b", _text(section.get("section_title")))
    return match.group(1) if match else ""


def _room_order(section: dict[str, Any], room: dict[str, Any]) -> int:
    try:
        section_order = int(section.get("section_order") or 0)
    except (TypeError, ValueError):
        section_order = 0
    try:
        page_start = int(room.get("page_start") or section.get("page_start") or 0)
    except (TypeError, ValueError):
        page_start = 0
    return section_order * 1000 + page_start


def _material_rows_from_group(
    section: dict[str, Any],
    room: dict[str, Any] | None,
    group: dict[str, Any],
    source_file: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    section_code = _section_code(section)
    section_title = _text(section.get("section_title"))
    room_label = _text((room or {}).get("room_label"))
    group_label = _text(group.get("group_label"))
    for index, row in enumerate(group.get("rows") or [], start=1):
        if not isinstance(row, dict):
            continue
        label = _text(row.get("label"))
        value = _text(row.get("value"))
        page_ref = _row_page_ref(row, group)
        rows.append(
            {
                "source_provider": SOURCE_PROVIDER,
                "source_extractor": SOURCE_EXTRACTOR,
                "source_file": source_file,
                "section_code": section_code,
                "section_title": section_title,
                "room_label": room_label,
                "group_label": group_label,
                "label": label,
                "value": value,
                "area_or_item": _source_area_label(group_label, label),
                "specs_or_description": value,
                "supplier": "",
                "notes": "",
                "page_no": _int(row.get("page_no")) or _int(group.get("page_start")),
                "page_refs": page_ref,
                "row_order": _int(row.get("row_order")) or index,
                "table_index": _int(row.get("table_index")),
                "row_index": _int(row.get("row_index")),
                "source_method": _text(row.get("source_method")),
                "raw_cells": deepcopy(row.get("raw_cells") or []),
                "source_rows": deepcopy(row.get("source_rows") or []),
                "is_diagnostic": bool(row.get("is_diagnostic")),
                "is_group_anchor": bool(row.get("is_group_anchor")),
                "needs_review": False,
                "confidence": 0.85,
                "provenance": _provenance(section, room, group, row, source_file, page_ref),
            }
        )
    return rows


def _provenance(
    section: dict[str, Any],
    room: dict[str, Any] | None,
    group: dict[str, Any],
    row: dict[str, Any],
    source_file: str,
    page_ref: str,
) -> dict[str, Any]:
    return {
        "source_provider": SOURCE_PROVIDER,
        "source_extractor": SOURCE_EXTRACTOR,
        "source_file": source_file,
        "page_refs": page_ref,
        "section_code": _section_code(section),
        "section_title": _text(section.get("section_title")),
        "room_label": _text((room or {}).get("room_label")),
        "group_label": _text(group.get("group_label")),
        "child_label": _text(row.get("label")),
        "source_method": _text(row.get("source_method")),
        "raw_cells": deepcopy(row.get("raw_cells") or []),
        "row_order": _int(row.get("row_order")),
        "table_index": _int(row.get("table_index")),
        "row_index": _int(row.get("row_index")),
    }


def _source_area_label(group_label: str, label: str) -> str:
    if group_label and label and _key(group_label) != _key(label):
        return f"{group_label} / {label}"
    return group_label or label


def _apply_room_group_mapping(room: RoomRow, section_code: str, group: dict[str, Any]) -> None:
    group_label = _text(group.get("group_label"))
    group_key = _key(group_label)
    if section_code == "15":
        _apply_cabinetry_group(room, group, group_key)
    elif section_code == "20":
        _apply_plumbing_group(room, group, group_key)


def _apply_cabinetry_group(room: RoomRow, group: dict[str, Any], group_key: str) -> None:
    if group_key == "benchtops":
        wall_run = _format_labeled_values(group, ("Manufacturer", "Colour", "Edge Profile"))
        island = _format_labeled_values(group, ("Island Colour", "Island Edge Profile", "Waterfall End to Island"))
        if room.room_key == "kitchen":
            room.bench_tops_wall_run = _append_text(room.bench_tops_wall_run, wall_run)
            room.bench_tops_island = _append_text(room.bench_tops_island, island)
        else:
            room.bench_tops_other = _append_text(room.bench_tops_other, wall_run)
            room.bench_tops_other = _append_text(room.bench_tops_other, island)
        return
    if group_key in {"underbench", "underbench including island"}:
        room.door_colours_base = _append_text(
            room.door_colours_base,
            _format_labeled_values(group, ("Manufacturer", "Colour", "Colour & Finish", "Profile", "Finish")),
        )
        room.has_explicit_base = True
        _append_group_values_to_list(room.handles, group, ("Handles", "Door Handle", "Drawer Handle", "Pantry Door Handle", "Bin & Pot Drawers Handle"))
        _append_group_values_to_list(room.toe_kick, group, ("Kickboard", "Kicker"))
        return
    if group_key in {"overhead cupboards", "overhead cupboard"}:
        room.door_colours_overheads = _append_text(
            room.door_colours_overheads,
            _format_labeled_values(group, ("Manufacturer", "Colour", "Colour & Finish", "Profile", "Finish")),
        )
        room.has_explicit_overheads = True
        _append_group_values_to_list(room.handles, group, ("Handles", "Door Handle", "Drawer Handle"))
        return
    if group_key in {"pantry doors", "tall cupboards", "tall cupboard"}:
        room.door_colours_tall = _append_text(
            room.door_colours_tall,
            _format_labeled_values(group, ("Manufacturer", "Colour", "Colour & Finish", "Profile", "Finish")),
        )
        room.has_explicit_tall = True
        _append_group_values_to_list(room.handles, group, ("Handles", "Door Handle", "Drawer Handle", "Pantry Door Handle"))
        _append_group_values_to_list(room.toe_kick, group, ("Kickboard", "Kicker"))
        return
    if group_key == "drawers":
        for row in _business_rows(group):
            room.other_items.append({"label": _source_area_label(_text(group.get("group_label")), _text(row.get("label"))), "value": _text(row.get("value"))})


def _apply_plumbing_group(room: RoomRow, group: dict[str, Any], group_key: str) -> None:
    if group_key in {"sink", "tub"}:
        room.sink_info = _append_text(room.sink_info, _format_labeled_values(group))
        return
    if group_key == "basin":
        room.basin_info = _append_text(room.basin_info, _format_labeled_values(group))
        return
    if group_key in MIXER_GROUPS:
        room.tap_info = _append_text(room.tap_info, _format_labeled_values(group))
        return
    if "accessories" in group_key or group_key in {"shower", "bath", "bath mixer spout", "bath mixer / spout"}:
        _append_group_values_to_list(room.accessories, group)


def _apply_finish_section_enrichment(room_candidates: dict[str, RoomRow], section: dict[str, Any]) -> None:
    section_code = _section_code(section)
    for group in section.get("groups") or []:
        if not isinstance(group, dict):
            continue
        room_key = _room_key(group.get("group_label"))
        room = room_candidates.get(room_key)
        if room is None:
            continue
        value = _format_labeled_values(group)
        if not value:
            continue
        if section_code == "24":
            room.splashback = _append_text(room.splashback, value)
        elif section_code in {"23", "25"}:
            room.flooring = _append_text(room.flooring, value)


def _format_labeled_values(group: dict[str, Any], labels: tuple[str, ...] | None = None) -> str:
    wanted = {_key(label) for label in labels or ()}
    lines: list[str] = []
    for row in _business_rows(group):
        label = _text(row.get("label"))
        value = _text(row.get("value"))
        if labels is not None and _key(label) not in wanted:
            continue
        if label and _key(label) != _key(_text(group.get("group_label"))):
            lines.append(f"{label}: {value}")
        else:
            lines.append(value)
    return "\n".join(_unique_text(lines))


def _append_group_values_to_list(target: list[str], group: dict[str, Any], labels: tuple[str, ...] | None = None) -> None:
    wanted = {_key(label) for label in labels or ()}
    for row in _business_rows(group):
        label = _text(row.get("label"))
        if labels is not None and _key(label) not in wanted:
            continue
        value = _text(row.get("value"))
        if not value:
            continue
        if value not in target:
            target.append(value)


def _business_rows(group: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in group.get("rows") or []
        if isinstance(row, dict) and not row.get("is_diagnostic") and _text(row.get("value"))
    ]


def _merge_room_candidate(candidates: dict[str, RoomRow], incoming: RoomRow) -> None:
    existing = candidates.get(incoming.room_key)
    if existing is None:
        candidates[incoming.room_key] = incoming
        return
    existing.material_rows.extend(deepcopy(incoming.material_rows))
    existing.page_refs = _merge_page_refs(existing.page_refs, incoming.page_refs)
    existing.evidence_snippet = existing.evidence_snippet or incoming.evidence_snippet
    for field_name in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "floating_shelf",
        "shelf",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "sink_info",
        "basin_info",
        "tap_info",
        "splashback",
        "flooring",
    ):
        setattr(existing, field_name, _append_text(getattr(existing, field_name), getattr(incoming, field_name)))
    for list_field in ("toe_kick", "bulkheads", "handles", "accessories", "other_items"):
        current = getattr(existing, list_field)
        for item in getattr(incoming, list_field):
            if item not in current:
                current.append(item)
    for bool_field in ("has_explicit_overheads", "has_explicit_base", "has_explicit_tall", "has_explicit_island", "has_explicit_bar_back"):
        setattr(existing, bool_field, bool(getattr(existing, bool_field) or getattr(incoming, bool_field)))


def _room_has_true_material_evidence(room: RoomRow) -> bool:
    for field_name in MATERIAL_RETAIN_FIELDS:
        if _has_nonterminal_text(getattr(room, field_name)):
            return True
    for values in (room.toe_kick, room.bulkheads):
        if any(_has_nonterminal_text(value) for value in values):
            return True
    return False


def _has_nonterminal_text(value: Any) -> bool:
    text = _text(value)
    return bool(text) and not _is_terminal_value(text)


def _is_terminal_value(value: Any) -> bool:
    text = _text(value).strip(" -;,")
    if not text:
        return True
    if re.fullmatch(r"(?i)(?:not applicable|not included|not required|n/?a|#n/?a|tbc)", text):
        return True
    if re.fullmatch(r"(?i)(?:not applicable|client to supply & install after handover|not included)\s*[-,].*", text):
        return True
    if re.fullmatch(r"(?i)client to supply & install after handover", text):
        return True
    return False


def _special_section_from_group(section: dict[str, Any], group: dict[str, Any], source_file: str) -> SpecialSectionRow:
    group_label = _text(group.get("group_label"))
    section_title = _text(section.get("section_title"))
    rows = _material_rows_from_group(section, None, group, source_file)
    return SpecialSectionRow(
        section_key=f"evoca_{_section_code(section)}_{_key(group_label) or 'section_group'}",
        original_section_label=f"{section_title} / {group_label}".strip(" /"),
        fields=_fields_from_material_rows(rows),
        source_file=source_file,
        page_refs=_page_range(group) or _page_range(section),
        evidence_snippet=_first_evidence_snippet(rows),
        confidence=0.85,
    )


def _special_section_from_unretained_room(room: RoomRow) -> SpecialSectionRow | None:
    if not room.material_rows:
        return None
    return SpecialSectionRow(
        section_key=f"evoca_unretained_{room.room_key}",
        original_section_label=f"Unretained Evoca source rows / {room.original_room_label or room.room_key}",
        fields=_fields_from_material_rows(room.material_rows),
        source_file=room.source_file,
        page_refs=room.page_refs,
        evidence_snippet=_first_evidence_snippet(room.material_rows),
        confidence=0.85,
    )


def _fields_from_material_rows(rows: list[dict[str, Any]]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for row in rows:
        label = _source_area_label(_text(row.get("group_label")), _text(row.get("label")))
        if row.get("is_diagnostic"):
            label = f"Diagnostic / {label}"
        value = _text(row.get("value"))
        key = label or "Value"
        if key in fields:
            suffix = 2
            while f"{key} #{suffix}" in fields:
                suffix += 1
            key = f"{key} #{suffix}"
        fields[key] = value
    return fields


def _first_evidence_snippet(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        label = _text(row.get("label"))
        value = _text(row.get("value"))
        if label or value:
            return f"{label}: {value}" if label else value
    return ""


def _appliance_evidence(group_label: str, label: str, value: str) -> str:
    return json.dumps(
        {
            "source_provider": SOURCE_PROVIDER,
            "group_label": group_label,
            "label": label,
            "value": value,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _row_page_ref(row: dict[str, Any], group: dict[str, Any]) -> str:
    page_no = _int(row.get("page_no")) or _int(group.get("page_start"))
    return str(page_no) if page_no else _page_range(group)


def _page_range(payload: dict[str, Any]) -> str:
    start = _int(payload.get("page_start"))
    end = _int(payload.get("page_end"))
    if start and end and start != end:
        return f"{start}-{end}"
    if start:
        return str(start)
    return ""


def _merge_page_refs(left: str, right: str) -> str:
    refs = [part for part in [left, right] if part]
    merged: list[str] = []
    for ref in refs:
        for part in str(ref).split(","):
            piece = part.strip()
            if piece and piece not in merged:
                merged.append(piece)
    return ",".join(merged)


def _append_text(existing: str, incoming: str) -> str:
    if not incoming:
        return existing
    if not existing:
        return incoming
    parts = [part for part in existing.splitlines() if part]
    for part in incoming.splitlines():
        if part and part not in parts:
            parts.append(part)
    return "\n".join(parts)


def _unique_text(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def _room_key(value: Any) -> str:
    text = _text(value).lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _key(value: Any) -> str:
    text = _text(value).lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
