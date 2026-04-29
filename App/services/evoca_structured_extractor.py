from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from App.services import parsing


SECTION_TITLE_PATTERNS: tuple[tuple[str, str], ...] = (
    ("15", "15 CABINETS"),
    ("16", "16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM"),
    ("17", "17 APPLIANCES, ACCESSORIES & HOT WATER UNIT"),
    ("18", "18 AIR-CONDITIONING"),
    ("19", "19 PLUMBING & GAS"),
    ("20", "20 PLUMBING FIXTURES & TAPWARE"),
    ("21", "21 MIRRORS"),
    ("22", "22 WINDOW FURNISHINGS"),
    ("23", "23 TILING / HARD FLOORING"),
    ("24", "24 GLASS SPLASHBACK"),
    ("25", "25 CARPET"),
)

ROOM_HEADINGS: dict[str, str] = dict(getattr(parsing, "EVOCA_ROOM_HEADINGS", {}))
TERMINAL_GROUP_VALUES: frozenset[str] = frozenset(
    {
        "not applicable",
        "not included",
        "not required",
        "n/a",
        "#n/a",
        "tbc",
    }
)
WORKBOOK_HEADERS: tuple[str, ...] = ("Page", "Order", "Room", "Group", "Label", "Value", "Anchor", "Source Text")
WORKBOOK_COLUMN_WIDTHS: tuple[int, ...] = (6, 7, 22, 28, 32, 44, 8, 60)
HEADER_FILL = "222222"
ROOM_BANNER_FILL = "DCE6F1"
ANCHOR_FILL = "FFF2CC"
GROUP_FILL = "F2F2F2"
NOTE_FILL = "FCE4D6"
BORDER_COLOR = "BBBBBB"
TERMINAL_FONT_COLOR = "888888"
ANCHOR_FONT_COLOR = "996600"


def extract_evoca_pdf(pdf_path: str | Path) -> dict[str, Any]:
    path = Path(pdf_path)
    pages = parsing.load_document_pages(path)
    structured = extract_evoca_pages(pages, source_pdf=str(path), document_name=path.name)
    value_lookup = _build_value_lookup(path)
    _rescue_missing_values(structured, value_lookup)
    _update_statistics(structured)
    return structured


def extract_evoca_pages(
    pages: list[dict[str, Any]],
    *,
    source_pdf: str = "",
    document_name: str = "",
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "source_pdf": source_pdf,
        "document_name": document_name or Path(source_pdf).name,
        "builder": "Evoca",
        "schema_version": "evoca_structured_v0",
        "pages": [],
        "sections": [],
        "unstructured_pages": [],
        "diagnostics": {
            "shift_override_groups": 0,
            "shift_overrides_applied": 0,
            "shift_clears_applied": 0,
        },
        "statistics": {
            "page_count": len(pages),
            "section_count": 0,
            "room_count": 0,
            "group_count": 0,
            "row_count": 0,
        },
    }
    current_section: dict[str, Any] | None = None
    current_room: dict[str, Any] | None = None
    current_group: dict[str, Any] | None = None

    for page in pages:
        page_no = int(page.get("page_no", 0) or 0)
        tables = _normalize_tables(page.get("table_rows", []))
        page_summary = {
            "page_no": page_no,
            "table_count": len(tables),
            "sections_detected": [],
            "row_count": sum(len(table) for table in tables),
        }
        document["pages"].append(page_summary)
        if not tables:
            raw_text = parsing.normalize_space(str(page.get("raw_text") or page.get("text") or ""))
            if raw_text:
                document["unstructured_pages"].append(
                    {
                        "page_no": page_no,
                        "text_preview": raw_text[:500],
                    }
                )
            continue

        for table_index, table in enumerate(tables):
            row_index = 0
            while row_index < len(table):
                row = table[row_index]
                section = detect_section_title(row)
                if section:
                    if current_section is not None and current_section.get("section_title") == section["section_title"]:
                        _extend_section_page(current_section, page_no)
                    else:
                        current_section = _new_section(section, page_no, len(document["sections"]) + 1)
                        document["sections"].append(current_section)
                    current_room = None
                    current_group = None
                    page_summary["sections_detected"].append(section["section_title"])
                    row_index += 1
                    continue

                if detect_untracked_section_heading(row):
                    current_room = None
                    current_group = None
                    row_index += 1
                    continue

                if current_section is None:
                    row_index += 1
                    continue

                _extend_section_page(current_section, page_no)

                room_label = detect_room_label(row)
                if room_label:
                    current_room = _ensure_room(current_section, room_label, page_no)
                    current_group = None
                    row_index += 1
                    continue

                if detect_unanchored_parent_header(table, row_index):
                    owner = current_room if current_room is not None else current_section
                    group, next_index = _consume_unanchored_parent_group(
                        table,
                        row_index,
                        page_no=page_no,
                        table_index=table_index,
                        owner=owner,
                    )
                    current_group = group
                    row_index = next_index
                    continue

                group_label = detect_group_label(row)
                if group_label:
                    owner = current_room if current_room is not None else current_section
                    group, next_index = _consume_group(
                        table,
                        row_index,
                        page_no=page_no,
                        table_index=table_index,
                        owner=owner,
                    )
                    current_group = group
                    row_index = next_index
                    continue

                if detect_unanchored_group_header(row):
                    owner = current_room if current_room is not None else current_section
                    group, next_index = _consume_unanchored_group(
                        table,
                        row_index,
                        page_no=page_no,
                        table_index=table_index,
                        owner=owner,
                    )
                    current_group = group
                    row_index = next_index
                    continue

                if _row_has_text(row):
                    target = current_group or current_room or current_section
                    _append_unanchored_row(
                        target,
                        row,
                        page_no=page_no,
                        table_index=table_index,
                        row_index=row_index,
                    )
                row_index += 1

    _dedupe_page_sections(document)
    _update_statistics(document)
    return document


def detect_section_title(row: list[str]) -> dict[str, str] | None:
    joined = _row_joined(row).upper()
    for code, title in SECTION_TITLE_PATTERNS:
        title_upper = title.upper()
        if joined == title_upper or joined.startswith(title_upper):
            return {"section_code": code, "section_title": title}
    return None


def detect_untracked_section_heading(row: list[str]) -> bool:
    if detect_section_title(row):
        return False
    joined = _row_joined(row)
    return bool(re.fullmatch(r"(?:[1-9]|[12][0-9]|30)\s+[A-Z][A-Z0-9 /&(),'\\-]+", joined))


def detect_room_label(row: list[str]) -> str:
    if len(row) < 2:
        return ""
    left = _cell(row, 0)
    center = _cell(row, 1)
    value = _value_text(row)
    if left or value:
        return ""
    first_line = _split_lines(center)[0] if _split_lines(center) else ""
    return first_line if first_line in ROOM_HEADINGS else ""


def detect_group_label(row: list[str]) -> str:
    if _cell(row, 0) != "-":
        return ""
    label_lines = _clean_label_lines(_cell(row, 1))
    if not label_lines:
        return ""
    return label_lines[0]


def detect_unanchored_group_header(row: list[str]) -> bool:
    """Detect a visual group header that lacks Evoca's leading '-' marker."""
    if _cell(row, 0):
        return False
    label_lines = _clean_label_lines(_cell(row, 1))
    value_lines = _split_lines(_value_text(row))
    return len(label_lines) >= 2 and len(value_lines) >= 1 and len(label_lines) > len(value_lines)


def detect_unanchored_parent_header(table: list[list[str]], row_index: int) -> bool:
    """Detect a single-line parent header whose properties start on the next '-' row."""
    if row_index + 1 >= len(table):
        return False
    row = table[row_index]
    next_row = table[row_index + 1]
    if _cell(row, 0) or _value_text(row):
        return False
    label_lines = _clean_label_lines(_cell(row, 1))
    if len(label_lines) != 1:
        return False
    if not detect_group_label(next_row) or _value_text(next_row):
        return False
    next_labels = _clean_label_lines(_cell(next_row, 1))
    return len(next_labels) >= 2


def flatten_rows_for_export(structured: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for section in structured.get("sections", []):
        section_title = str(section.get("section_title", "") or "")
        for note in section.get("notes", []) or []:
            rows.append(_export_row(section_title, "", "", note))
        for group in section.get("groups", []) or []:
            rows.extend(_export_group(section_title, "", group))
        for room in section.get("rooms", []) or []:
            room_label = str(room.get("room_label", "") or "")
            for note in room.get("notes", []) or []:
                rows.append(_export_row(section_title, room_label, "", note))
            for group in room.get("groups", []) or []:
                rows.extend(_export_group(section_title, room_label, group))
    return rows


def write_structured_workbook(structured: dict[str, Any], xlsx_path: str | Path) -> None:
    from openpyxl import Workbook

    path = Path(xlsx_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)
    used_sheet_names: set[str] = set()
    styles = _workbook_styles()
    summary_rows: list[dict[str, Any]] = []

    for section in structured.get("sections", []) or []:
        title = str(section.get("section_title", "Section") or "Section")
        sheet_name = _unique_sheet_name(_sheet_name(title), used_sheet_names)
        sheet = workbook.create_sheet(sheet_name)
        summary_rows.append(_write_section_sheet(sheet, section, sheet_name, styles))

    if not workbook.worksheets:
        workbook.create_sheet("Evoca")
    _build_summary_sheet(workbook, structured, summary_rows, styles)
    workbook.save(path)


def write_structured_json(structured: dict[str, Any], json_path: str | Path) -> None:
    path = Path(json_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(structured, indent=2, ensure_ascii=False), encoding="utf-8")


def _normalize_tables(raw_tables: Any) -> list[list[list[str]]]:
    tables: list[list[list[str]]] = []
    for table in raw_tables or []:
        if not isinstance(table, list):
            continue
        normalized_rows: list[list[str]] = []
        for raw_row in table:
            if not isinstance(raw_row, (list, tuple)):
                continue
            row = [parsing.normalize_space(str(cell).replace("\x00", " ")) if cell is not None else "" for cell in raw_row]
            while row and not row[-1]:
                row.pop()
            if any(row):
                normalized_rows.append(row)
        if normalized_rows:
            tables.append(normalized_rows)
    return tables


def _new_section(section: dict[str, str], page_no: int, order: int) -> dict[str, Any]:
    return {
        "section_code": section["section_code"],
        "section_title": section["section_title"],
        "section_order": order,
        "page_start": page_no,
        "page_end": page_no,
        "rooms": [],
        "groups": [],
        "notes": [],
    }


def _extend_section_page(section: dict[str, Any], page_no: int) -> None:
    if not section.get("page_start"):
        section["page_start"] = page_no
    section["page_end"] = max(int(section.get("page_end", page_no) or page_no), page_no)


def _ensure_room(section: dict[str, Any], room_label: str, page_no: int) -> dict[str, Any]:
    for room in section.get("rooms", []):
        if room.get("room_label") == room_label:
            room["page_end"] = max(int(room.get("page_end", page_no) or page_no), page_no)
            return room
    room = {
        "room_label": room_label,
        "room_key": ROOM_HEADINGS.get(room_label, parsing.source_room_key(room_label)),
        "page_start": page_no,
        "page_end": page_no,
        "groups": [],
        "notes": [],
    }
    section.setdefault("rooms", []).append(room)
    return room


def _consume_group(
    table: list[list[str]],
    row_index: int,
    *,
    page_no: int,
    table_index: int,
    owner: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    row = table[row_index]
    label_lines = _clean_label_lines(_cell(row, 1))
    group_label = label_lines[0] if label_lines else "Group"
    child_labels = label_lines[1:]
    values = _split_lines(_value_text(row))
    raw_rows = [_raw_row_record(row, page_no=page_no, table_index=table_index, row_index=row_index)]
    next_index = row_index + 1

    while next_index < len(table):
        next_row = table[next_index]
        if (
            detect_section_title(next_row)
            or detect_untracked_section_heading(next_row)
            or detect_room_label(next_row)
            or detect_group_label(next_row)
            or detect_unanchored_group_header(next_row)
            or detect_unanchored_parent_header(table, next_index)
        ):
            break
        if _row_has_text(next_row):
            raw_rows.append(_raw_row_record(next_row, page_no=page_no, table_index=table_index, row_index=next_index))
            center_lines = _clean_label_lines(_cell(next_row, 1))
            value_lines = _split_lines(_value_text(next_row))
            if center_lines and value_lines:
                child_labels.extend(center_lines)
                values.extend(value_lines)
            elif center_lines:
                if values:
                    values.extend(center_lines)
                else:
                    child_labels.extend(center_lines)
            else:
                values.extend(value_lines)
        next_index += 1

    group = {
        "group_label": group_label,
        "page_start": page_no,
        "page_end": page_no,
        "raw_rows": raw_rows,
        "value_lines": list(values),
        "_decompose_meta": _decompose_meta(child_labels, values),
        "rows": _build_group_rows(
            group_label,
            child_labels,
            values,
            page_no=page_no,
            table_index=table_index,
            source_row=row_index,
            raw_rows=raw_rows,
        ),
    }
    owner.setdefault("groups", []).append(group)
    return group, next_index


def _consume_unanchored_parent_group(
    table: list[list[str]],
    row_index: int,
    *,
    page_no: int,
    table_index: int,
    owner: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    row = table[row_index]
    property_row = table[row_index + 1]
    group_label_lines = _clean_label_lines(_cell(row, 1))
    group_label = group_label_lines[0] if group_label_lines else "Group"
    labels = _clean_label_lines(_cell(property_row, 1))
    values = _split_lines(_value_text(property_row))
    raw_rows = [
        _raw_row_record(row, page_no=page_no, table_index=table_index, row_index=row_index),
        _raw_row_record(property_row, page_no=page_no, table_index=table_index, row_index=row_index + 1),
    ]
    next_index = row_index + 2

    while next_index < len(table):
        next_row = table[next_index]
        if (
            detect_section_title(next_row)
            or detect_untracked_section_heading(next_row)
            or detect_room_label(next_row)
            or detect_group_label(next_row)
            or detect_unanchored_group_header(next_row)
            or detect_unanchored_parent_header(table, next_index)
        ):
            break
        if _row_has_text(next_row):
            raw_rows.append(_raw_row_record(next_row, page_no=page_no, table_index=table_index, row_index=next_index))
            center_lines = _clean_label_lines(_cell(next_row, 1))
            value_lines = _split_lines(_value_text(next_row))
            if center_lines and value_lines:
                labels.extend(center_lines)
                values.extend(value_lines)
            elif center_lines:
                labels.extend(center_lines)
            else:
                values.extend(value_lines)
        next_index += 1

    group = {
        "group_label": group_label,
        "page_start": page_no,
        "page_end": page_no,
        "raw_rows": raw_rows,
        "value_lines": list(values),
        "_decompose_meta": _decompose_meta(labels, values),
        "rows": _build_group_rows(
            group_label,
            labels,
            values,
            page_no=page_no,
            table_index=table_index,
            source_row=row_index,
            raw_rows=raw_rows,
        ),
    }
    owner.setdefault("groups", []).append(group)
    return group, next_index


def _consume_unanchored_group(
    table: list[list[str]],
    row_index: int,
    *,
    page_no: int,
    table_index: int,
    owner: dict[str, Any],
) -> tuple[dict[str, Any], int]:
    row = table[row_index]
    label_lines = _clean_label_lines(_cell(row, 1))
    group_label = label_lines[0] if label_lines else "Group"
    child_labels = label_lines[1:]
    values = _split_lines(_value_text(row))
    raw_rows = [_raw_row_record(row, page_no=page_no, table_index=table_index, row_index=row_index)]
    next_index = row_index + 1

    while next_index < len(table):
        next_row = table[next_index]
        if (
            detect_section_title(next_row)
            or detect_untracked_section_heading(next_row)
            or detect_room_label(next_row)
            or detect_group_label(next_row)
            or detect_unanchored_group_header(next_row)
            or detect_unanchored_parent_header(table, next_index)
        ):
            break
        if not _row_has_text(next_row):
            next_index += 1
            continue
        if _cell(next_row, 0) or _cell(next_row, 1):
            break
        value_lines = _split_lines(_value_text(next_row))
        if not value_lines:
            break
        raw_rows.append(_raw_row_record(next_row, page_no=page_no, table_index=table_index, row_index=next_index))
        values.extend(value_lines)
        next_index += 1

    group = {
        "group_label": group_label,
        "page_start": page_no,
        "page_end": page_no,
        "raw_rows": raw_rows,
        "value_lines": list(values),
        "_decompose_meta": _decompose_meta(child_labels, values),
        "rows": _build_group_rows(
            group_label,
            child_labels,
            values,
            page_no=page_no,
            table_index=table_index,
            source_row=row_index,
            raw_rows=raw_rows,
        ),
    }
    owner.setdefault("groups", []).append(group)
    return group, next_index


def _build_group_rows(
    group_label: str,
    labels: list[str],
    values: list[str],
    *,
    page_no: int,
    table_index: int,
    source_row: int,
    raw_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cleaned_labels = [_clean_label(label) for label in labels if _clean_label(label)]
    cleaned_values = [parsing.normalize_space(value) for value in values if parsing.normalize_space(value)]
    all_labels = [group_label, *cleaned_labels]
    if len(all_labels) >= 2 and len(cleaned_values) == 1 and _is_terminal_group_value(cleaned_values[0]):
        for index, label in enumerate(all_labels):
            rows.append(
                _structured_row(
                    label=label,
                    value=cleaned_values[0] if index == 0 else "",
                    page_no=page_no,
                    table_index=table_index,
                    row_index=source_row,
                    raw_rows=raw_rows,
                    is_group_anchor=index == 0,
                )
            )
        return rows
    if not cleaned_labels:
        cleaned_labels = [group_label] if cleaned_values else []
    for index, label in enumerate(cleaned_labels):
        value = cleaned_values[index] if index < len(cleaned_values) else ""
        rows.append(
            _structured_row(
                label=label,
                value=value,
                page_no=page_no,
                table_index=table_index,
                row_index=source_row,
                raw_rows=raw_rows,
                is_group_anchor=label == group_label,
            )
        )
    for extra_index, value in enumerate(cleaned_values[len(cleaned_labels) :], start=len(cleaned_labels)):
        rows.append(
            _structured_row(
                label="Continuation",
                value=value,
                page_no=page_no,
                table_index=table_index,
                row_index=source_row + extra_index,
                raw_rows=raw_rows,
            )
        )
    if not rows and raw_rows:
        rows.append(
            _structured_row(
                label=group_label,
                value="",
                page_no=page_no,
                table_index=table_index,
                row_index=source_row,
                raw_rows=raw_rows,
            )
        )
    return rows


def _decompose_meta(labels: list[str], values: list[str]) -> dict[str, int]:
    return {
        "labels_count": len([_clean_label(label) for label in labels if _clean_label(label)]),
        "values_count": len([parsing.normalize_space(value) for value in values if parsing.normalize_space(value)]),
    }


def _append_unanchored_row(
    target: dict[str, Any],
    row: list[str],
    *,
    page_no: int,
    table_index: int,
    row_index: int,
) -> None:
    text = _row_joined(row)
    if not text:
        return
    record = _structured_row(
        label="Note" if target.get("rooms") is None else "Section Note",
        value=text,
        page_no=page_no,
        table_index=table_index,
        row_index=row_index,
        raw_rows=[_raw_row_record(row, page_no=page_no, table_index=table_index, row_index=row_index)],
    )
    if "rows" in target and "group_label" in target:
        target["rows"].append(record)
    else:
        target.setdefault("notes", []).append(record)


def _structured_row(
    *,
    label: str,
    value: str,
    page_no: int,
    table_index: int,
    row_index: int,
    raw_rows: list[dict[str, Any]],
    is_group_anchor: bool = False,
) -> dict[str, Any]:
    row = {
        "label": parsing.normalize_space(label),
        "value": parsing.normalize_space(value),
        "page_no": page_no,
        "row_order": row_index + 1,
        "table_index": table_index,
        "row_index": row_index,
        "raw_cells": raw_rows[0]["raw_cells"] if raw_rows else [],
        "source_rows": raw_rows,
        "source_method": "pdfplumber_table",
    }
    if is_group_anchor:
        row["is_group_anchor"] = True
    return row


def _raw_row_record(row: list[str], *, page_no: int, table_index: int, row_index: int) -> dict[str, Any]:
    return {
        "page_no": page_no,
        "table_index": table_index,
        "row_index": row_index,
        "raw_cells": list(row),
    }


def _export_group(section_title: str, room_label: str, group: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    group_label = str(group.get("group_label", "") or "")
    for row in group.get("rows", []) or []:
        rows.append(_export_row(section_title, room_label, group_label, row))
    return rows


def _export_row(section_title: str, room_label: str, group_label: str, row: dict[str, Any]) -> dict[str, str]:
    _ = section_title
    is_anchor = bool(row.get("is_group_anchor"))
    row_type = "anchor" if is_anchor else "note" if _is_note_row(row, group_label) else "group"
    return {
        "page": str(row.get("page_no", "") or ""),
        "order": str(row.get("row_order", "") or ""),
        "room": room_label,
        "group": group_label,
        "label": str(row.get("label", "") or ""),
        "value": str(row.get("value", "") or ""),
        "anchor": "ANCHOR" if is_anchor else "",
        "source_text": _compact_source_text(row, is_anchor=is_anchor),
        "row_type": row_type,
    }


def _workbook_styles() -> dict[str, Any]:
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    thin_side = Side(style="thin", color=BORDER_COLOR)
    return {
        "header_fill": PatternFill("solid", fgColor=HEADER_FILL),
        "room_fill": PatternFill("solid", fgColor=ROOM_BANNER_FILL),
        "anchor_fill": PatternFill("solid", fgColor=ANCHOR_FILL),
        "group_fill": PatternFill("solid", fgColor=GROUP_FILL),
        "note_fill": PatternFill("solid", fgColor=NOTE_FILL),
        "header_font": Font(color="FFFFFF", bold=True),
        "room_font": Font(bold=True),
        "anchor_font": Font(color=ANCHOR_FONT_COLOR, bold=True),
        "note_font": Font(italic=True),
        "terminal_font": Font(color=TERMINAL_FONT_COLOR, italic=True),
        "default_font": Font(color="000000"),
        "border": Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side),
        "wrap_top": Alignment(wrap_text=True, vertical="top"),
    }


def _write_section_sheet(sheet: Any, section: dict[str, Any], sheet_name: str, styles: dict[str, Any]) -> dict[str, Any]:
    sheet.append(list(WORKBOOK_HEADERS))
    _style_header_row(sheet, 1, styles)
    _apply_sheet_layout(sheet)

    row_count = 0
    anchor_count = 0
    note_count = 0

    for note in section.get("notes", []) or []:
        stats = _append_export_row(sheet, _export_row(str(section.get("section_title", "") or ""), "", "", note), styles)
        row_count += stats["rows"]
        anchor_count += stats["anchors"]
        note_count += stats["notes"]
    for group in section.get("groups", []) or []:
        for row in _export_group(str(section.get("section_title", "") or ""), "", group):
            stats = _append_export_row(sheet, row, styles)
            row_count += stats["rows"]
            anchor_count += stats["anchors"]
            note_count += stats["notes"]

    for room in section.get("rooms", []) or []:
        room_label = str(room.get("room_label", "") or "")
        _append_room_banner(sheet, room_label, styles)
        for note in room.get("notes", []) or []:
            stats = _append_export_row(sheet, _export_row(str(section.get("section_title", "") or ""), room_label, "", note), styles)
            row_count += stats["rows"]
            anchor_count += stats["anchors"]
            note_count += stats["notes"]
        for group in room.get("groups", []) or []:
            for row in _export_group(str(section.get("section_title", "") or ""), room_label, group):
                stats = _append_export_row(sheet, row, styles)
                row_count += stats["rows"]
                anchor_count += stats["anchors"]
                note_count += stats["notes"]

    return {
        "section_code": str(section.get("section_code", "") or ""),
        "title": str(section.get("section_title", "") or ""),
        "sheet": sheet_name,
        "page": _page_range(section),
        "rows": row_count,
        "anchors": anchor_count,
        "notes": note_count,
    }


def _build_summary_sheet(
    workbook: Any,
    structured: dict[str, Any],
    summary_rows: list[dict[str, Any]],
    styles: dict[str, Any],
) -> None:
    sheet = workbook.create_sheet("_summary", 0)
    stats = structured.get("statistics", {}) or {}
    diagnostics = structured.get("diagnostics", {}) or {}
    source_pdf = str(structured.get("source_pdf", "") or "")
    source_name = Path(source_pdf).name if source_pdf else str(structured.get("document_name", "") or "")

    header_block = [
        ("Source PDF", source_name),
        ("Pages", stats.get("page_count", "")),
        ("Sections", stats.get("section_count", "")),
        ("Schema version", structured.get("schema_version", "")),
    ]
    for label, value in header_block:
        sheet.append([label, value])
        _style_summary_pair(sheet, sheet.max_row, styles)

    if diagnostics:
        sheet.append(["Diagnostics", ""])
        _style_summary_pair(sheet, sheet.max_row, styles)
        for key in ("shift_override_groups", "shift_overrides_applied", "shift_clears_applied"):
            sheet.append([key, diagnostics.get(key, 0)])
            _style_summary_pair(sheet, sheet.max_row, styles)

    sheet.append([])
    table_header_row = sheet.max_row + 1
    sheet.append(["Section #", "Title", "Sheet", "Page", "Rows", "Anchors", "Notes"])
    _style_header_row(sheet, table_header_row, styles)
    for row in summary_rows:
        sheet.append(
            [
                row["section_code"],
                row["title"],
                row["sheet"],
                row["page"],
                row["rows"],
                row["anchors"],
                row["notes"],
            ]
        )
        _style_summary_table_row(sheet, sheet.max_row, styles)
    if summary_rows:
        sheet.append(["", "Total", "", "", sum(row["rows"] for row in summary_rows), sum(row["anchors"] for row in summary_rows), sum(row["notes"] for row in summary_rows)])
        _style_summary_table_row(sheet, sheet.max_row, styles, bold=True)

    for index, width in enumerate((12, 48, 24, 14, 10, 10, 10), start=1):
        sheet.column_dimensions[_column_letter(index)].width = width
    sheet.freeze_panes = f"A{table_header_row + 1}"


def _append_export_row(sheet: Any, row: dict[str, str], styles: dict[str, Any]) -> dict[str, int]:
    sheet.append(
        [
            row["page"],
            row["order"],
            row["room"],
            row["group"],
            row["label"],
            row["value"],
            row["anchor"],
            row["source_text"],
        ]
    )
    row_index = sheet.max_row
    row_type = row.get("row_type", "group")
    fill = styles["anchor_fill"] if row_type == "anchor" else styles["note_fill"] if row_type == "note" else styles["group_fill"]
    for cell in sheet[row_index]:
        cell.fill = fill
        cell.font = styles["note_font"] if row_type == "note" else styles["default_font"]
        cell.border = styles["border"]
        cell.alignment = styles["wrap_top"]
    if row.get("anchor"):
        sheet.cell(row=row_index, column=7).font = styles["anchor_font"]
    if _is_terminal_group_value(row.get("value", "")):
        sheet.cell(row=row_index, column=6).font = styles["terminal_font"]
    return {
        "rows": 1,
        "anchors": 1 if row.get("anchor") else 0,
        "notes": 1 if row_type == "note" else 0,
    }


def _append_room_banner(sheet: Any, room_label: str, styles: dict[str, Any]) -> None:
    sheet.append([f"room: {room_label}", "", "", "", "", "", "", ""])
    row_index = sheet.max_row
    sheet.merge_cells(start_row=row_index, start_column=1, end_row=row_index, end_column=len(WORKBOOK_HEADERS))
    for cell in sheet[row_index]:
        cell.fill = styles["room_fill"]
        cell.font = styles["room_font"]
        cell.border = styles["border"]
        cell.alignment = styles["wrap_top"]


def _style_header_row(sheet: Any, row_index: int, styles: dict[str, Any]) -> None:
    for cell in sheet[row_index]:
        cell.fill = styles["header_fill"]
        cell.font = styles["header_font"]
        cell.border = styles["border"]
        cell.alignment = styles["wrap_top"]


def _style_summary_pair(sheet: Any, row_index: int, styles: dict[str, Any]) -> None:
    for cell in sheet[row_index]:
        cell.border = styles["border"]
        cell.alignment = styles["wrap_top"]
    sheet.cell(row=row_index, column=1).fill = styles["header_fill"]
    sheet.cell(row=row_index, column=1).font = styles["header_font"]


def _style_summary_table_row(sheet: Any, row_index: int, styles: dict[str, Any], *, bold: bool = False) -> None:
    for cell in sheet[row_index]:
        cell.fill = styles["group_fill"]
        cell.border = styles["border"]
        cell.alignment = styles["wrap_top"]
        if bold:
            cell.font = styles["room_font"]


def _apply_sheet_layout(sheet: Any) -> None:
    for index, width in enumerate(WORKBOOK_COLUMN_WIDTHS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = width
    sheet.freeze_panes = "A2"


def _column_letter(index: int) -> str:
    from openpyxl.utils import get_column_letter

    return get_column_letter(index)


def _page_range(section: dict[str, Any]) -> str:
    start = str(section.get("page_start", "") or "")
    end = str(section.get("page_end", "") or "")
    if not start:
        return ""
    return start if not end or end == start else f"{start}-{end}"


def _compact_source_text(row: dict[str, Any], *, is_anchor: bool) -> str:
    label = parsing.normalize_space(str(row.get("label", "") or ""))
    value = parsing.normalize_space(str(row.get("value", "") or ""))
    if is_anchor:
        return f"- {label}" if label else ""
    if _is_note_row(row, ""):
        return value or label
    if label and value:
        return f"{label}: {value}"
    return label or value


def _is_note_row(row: dict[str, Any], group_label: str) -> bool:
    _ = group_label
    return parsing.normalize_space(str(row.get("label", "") or "")).lower() in {"note", "section note"}


def _build_value_lookup(pdf_path: Path) -> dict[int, dict[str, list[str]]]:
    import pdfplumber

    lookup: dict[int, dict[str, list[str]]] = {}
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            page_lookup: dict[str, list[str]] = {}
            text_tables = page.extract_tables(
                table_settings={
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "text",
                }
            ) or []
            for table in text_tables:
                for raw_row in table or []:
                    pair = _lookup_pair_from_row(raw_row)
                    if pair is None:
                        continue
                    label, value = pair
                    key = _norm_label_key(label)
                    if not key:
                        continue
                    page_lookup.setdefault(key, []).append(value)
            if page_lookup:
                lookup[page_index] = page_lookup
    return lookup


def _lookup_pair_from_row(raw_row: Any) -> tuple[str, str] | None:
    if not isinstance(raw_row, (list, tuple)):
        return None
    cells = [parsing.normalize_space(str(cell).replace("\x00", " ")) if cell is not None else "" for cell in raw_row]
    non_empty = [cell for cell in cells if cell]
    if len(non_empty) != 2:
        return None
    label = _clean_label(non_empty[0])
    value = parsing.normalize_space(non_empty[1])
    if not label or not value:
        return None
    if label in {"-", "–", "—"}:
        return None
    if detect_section_title([label, value]) or detect_section_title([label]):
        return None
    return label, value


def _rescue_missing_values(structured: dict[str, Any], lookup: dict[int, dict[str, list[str]]]) -> None:
    consumable = {page: {key: list(values) for key, values in page_values.items()} for page, page_values in lookup.items()}
    diagnostics = structured.setdefault("diagnostics", {})
    for key in ("shift_override_groups", "shift_overrides_applied", "shift_clears_applied"):
        diagnostics.setdefault(key, 0)
    for section in structured.get("sections", []) or []:
        for group in section.get("groups", []) or []:
            _rescue_group(group, consumable, diagnostics)
        for room in section.get("rooms", []) or []:
            for group in room.get("groups", []) or []:
                _rescue_group(group, consumable, diagnostics)


def _rescue_group(
    group: dict[str, Any],
    consumable: dict[int, dict[str, list[str]]],
    diagnostics: dict[str, int],
) -> None:
    rows = list(group.get("rows", []) or [])
    if not rows:
        return
    anchor_value = rows[0].get("value", "")
    if _is_terminal_group_value(anchor_value):
        return
    if _shift_override_eligible(group, rows, consumable):
        _apply_shift_override(rows, consumable, diagnostics)
        return
    for row in rows:
        key = _norm_label_key(row.get("label", ""))
        if not key:
            continue
        page_lookup = consumable.get(int(row.get("page_no", 0) or 0), {})
        candidates = page_lookup.get(key)
        if not candidates:
            continue
        current_value = parsing.normalize_space(str(row.get("value") or ""))
        if current_value:
            if _norm_label_key(candidates[0]) == _norm_label_key(current_value):
                candidates.pop(0)
            continue
        row["value"] = candidates.pop(0)
        row["source_method"] = "pdfplumber_text_rescue"


def _shift_override_eligible(
    group: dict[str, Any],
    rows: list[dict[str, Any]],
    consumable: dict[int, dict[str, list[str]]],
) -> bool:
    meta = group.get("_decompose_meta", {}) or {}
    labels_count = int(meta.get("labels_count", 0) or 0)
    values_count = int(meta.get("values_count", 0) or 0)
    if not labels_count > values_count > 0:
        return False
    for row in rows:
        if row.get("is_group_anchor"):
            continue
        old_value = parsing.normalize_space(str(row.get("value") or ""))
        if not old_value:
            continue
        key = _norm_label_key(row.get("label", ""))
        page_lookup = consumable.get(int(row.get("page_no", 0) or 0), {})
        if key and not page_lookup.get(key):
            return True
    return False


def _apply_shift_override(
    rows: list[dict[str, Any]],
    consumable: dict[int, dict[str, list[str]]],
    diagnostics: dict[str, int],
) -> None:
    """Correct under-supplied groups using text-grid lookup in source order.

    This assumes repeated labels on a page appear in the text-grid lookup in the
    same order as structured groups are processed. If a future multi-column PDF
    violates that, this should become bbox-bounded instead of page-wide.
    """
    diagnostics["shift_override_groups"] += 1
    for row in rows:
        if row.get("is_group_anchor"):
            continue
        key = _norm_label_key(row.get("label", ""))
        old_value = parsing.normalize_space(str(row.get("value") or ""))
        page_lookup = consumable.get(int(row.get("page_no", 0) or 0), {})
        candidates = page_lookup.get(key) if key else None
        if candidates:
            value = candidates.pop(0)
            if old_value != parsing.normalize_space(value):
                diagnostics["shift_overrides_applied"] += 1
            row["value"] = value
            row["source_method"] = "pdfplumber_text_rescue"
            continue
        if old_value:
            diagnostics["shift_clears_applied"] += 1
        row["value"] = ""
        row["source_method"] = "pdfplumber_text_rescue"


def _dedupe_page_sections(document: dict[str, Any]) -> None:
    for page in document.get("pages", []) or []:
        seen: set[str] = set()
        unique: list[str] = []
        for title in page.get("sections_detected", []) or []:
            if title in seen:
                continue
            seen.add(title)
            unique.append(title)
        page["sections_detected"] = unique


def _update_statistics(document: dict[str, Any]) -> None:
    sections = list(document.get("sections", []) or [])
    room_count = 0
    group_count = 0
    row_count = 0
    for section in sections:
        group_count += len(section.get("groups", []) or [])
        row_count += sum(len(group.get("rows", []) or []) for group in section.get("groups", []) or [])
        for room in section.get("rooms", []) or []:
            room_count += 1
            group_count += len(room.get("groups", []) or [])
            row_count += sum(len(group.get("rows", []) or []) for group in room.get("groups", []) or [])
            row_count += len(room.get("notes", []) or [])
        row_count += len(section.get("notes", []) or [])
    document["statistics"] = {
        "page_count": len(document.get("pages", []) or []),
        "section_count": len(sections),
        "room_count": room_count,
        "group_count": group_count,
        "row_count": row_count,
    }


def _is_terminal_group_value(value: Any) -> bool:
    return parsing.normalize_space(str(value or "")).strip().lower() in TERMINAL_GROUP_VALUES


def _norm_label_key(label: Any) -> str:
    """Normalize labels for text-grid rescue; wrapped PDF labels may still miss in v0."""
    normalized = parsing.normalize_space(str(label or ""))
    normalized = re.sub(r"\*+", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().lower()


def _cell(row: list[str], index: int) -> str:
    if 0 <= index < len(row):
        return parsing.normalize_space(row[index])
    return ""


def _value_text(row: list[str]) -> str:
    values = [_cell(row, index) for index in range(2, len(row))]
    return "\n".join(value for value in values if value)


def _split_lines(value: Any) -> list[str]:
    return [parsing.normalize_space(line) for line in str(value or "").splitlines() if parsing.normalize_space(line)]


def _clean_label_lines(value: Any) -> list[str]:
    lines = [_clean_label(line) for line in _split_lines(value) if _clean_label(line)]
    merged: list[str] = []
    for line in lines:
        if merged and _label_wraps_to_next_line(merged[-1]):
            merged[-1] = parsing.normalize_space(f"{merged[-1]} {line}")
        else:
            merged.append(line)
    return merged


def _label_wraps_to_next_line(label: str) -> bool:
    stripped = parsing.normalize_space(label)
    return stripped.endswith("&") or stripped.count("(") > stripped.count(")")


def _clean_label(value: Any) -> str:
    return parsing.normalize_space(str(value or "")).strip(" -*:")


def _row_joined(row: list[str]) -> str:
    return parsing.normalize_space(" ".join(cell for cell in row if cell))


def _row_has_text(row: list[str]) -> bool:
    return any(parsing.normalize_space(cell) for cell in row)


def _sheet_name(value: str) -> str:
    aliases = {
        "15 CABINETS": "15_CABINETS",
        "16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM": "16_ELECTRICAL",
        "17 APPLIANCES, ACCESSORIES & HOT WATER UNIT": "17_APPLIANCES",
        "18 AIR-CONDITIONING": "18_AIR-CONDITIONING",
        "19 PLUMBING & GAS": "19_PLUMBING_GAS",
        "20 PLUMBING FIXTURES & TAPWARE": "20_PLUMBING",
        "21 MIRRORS": "21_MIRRORS",
        "22 WINDOW FURNISHINGS": "22_WINDOW_FURNISHINGS",
        "23 TILING / HARD FLOORING": "23_FLOORING",
        "24 GLASS SPLASHBACK": "24_SPLASHBACK",
        "25 CARPET": "25_CARPET",
    }
    raw = parsing.normalize_space(value)
    if raw in aliases:
        return aliases[raw]
    cleaned = re.sub(r"[\[\]:*?/\\]", " ", raw)
    cleaned = re.sub(r"\s+", "_", cleaned).strip("_") or "Evoca"
    return cleaned[:31]


def _unique_sheet_name(base: str, used: set[str]) -> str:
    name = base[:31] or "Sheet"
    if name not in used:
        used.add(name)
        return name
    counter = 2
    while True:
        suffix = f" {counter}"
        candidate = f"{base[:31 - len(suffix)]}{suffix}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        counter += 1
