from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment

from App.services import cleaning_rules, parsing
from App.services.runtime import ensure_job_dirs, safe_filename, utc_now_iso


def build_exports(job_no: str, snapshot: dict[str, Any]) -> dict[str, str]:
    dirs = ensure_job_dirs(job_no)
    stamp = utc_now_iso().replace(":", "").replace("-", "")
    excel_path = dirs["export_dir"] / f"{safe_filename(job_no)}_{stamp}.xlsx"
    csv_path = dirs["export_dir"] / f"{safe_filename(job_no)}_{stamp}.csv"
    _write_excel(excel_path, snapshot)
    _write_csv(csv_path, snapshot)
    return {"excel": str(excel_path), "csv": str(csv_path)}


def build_spec_list_excel(job_no: str, snapshot: dict[str, Any]) -> str:
    dirs = ensure_job_dirs(job_no)
    stamp = utc_now_iso().replace(":", "").replace("-", "")
    excel_path = dirs["export_dir"] / f"{safe_filename(job_no)}_spec_list_{stamp}.xlsx"
    _write_excel(excel_path, snapshot)
    return str(excel_path)


def _write_excel(path: Path, snapshot: dict[str, Any]) -> None:
    wb = Workbook()
    ws_rooms = wb.active
    ws_rooms.title = "Rooms"
    ws_rooms.append(
        [
            "room_key",
            "original_room_label",
            "bench_tops",
            "bench_tops_wall_run",
            "bench_tops_island",
            "bench_tops_other",
            "floating_shelf",
            "door_panel_colours",
            "door_colours_overheads",
            "door_colours_base",
            "door_colours_tall",
            "door_colours_island",
            "door_colours_bar_back",
            "toe_kick",
            "bulkheads",
            "handles",
            "led",
            "accessories",
            "other_items",
            "sink_info",
            "basin_info",
            "tap_info",
            "drawers_soft_close",
            "hinges_soft_close",
            "splashback",
            "flooring",
            "source_file",
            "page_refs",
            "evidence_snippet",
            "confidence",
        ]
    )
    for row in _mapping_rows(snapshot.get("rooms", [])):
        benchtop_groups = _split_export_benchtops(row)
        ws_rooms.append(
            [
                _display_value(row.get("room_key", "")),
                _display_value(row.get("original_room_label", "")),
                _display_value(row.get("bench_tops", [])),
                benchtop_groups["bench_tops_wall_run"],
                benchtop_groups["bench_tops_island"],
                benchtop_groups["bench_tops_other"],
                _display_value(row.get("floating_shelf", "")),
                _display_value(row.get("door_panel_colours", [])),
                _display_value(row.get("door_colours_overheads", "")),
                _display_value(row.get("door_colours_base", "")),
                _display_value(row.get("door_colours_tall", "")),
                _display_value(row.get("door_colours_island", "")),
                _display_value(row.get("door_colours_bar_back", "")),
                _display_value(row.get("toe_kick", [])),
                _display_value(row.get("bulkheads", [])),
                _display_value(row.get("handles", [])),
                _display_value(row.get("led", "")),
                _display_value(row.get("accessories", [])),
                _display_other_items(row.get("other_items", [])),
                _display_value(row.get("sink_info", "")),
                _display_value(row.get("basin_info", "")),
                _display_value(row.get("tap_info", "")),
                _normalize_soft_close(row.get("drawers_soft_close", ""), "drawer"),
                _normalize_soft_close(row.get("hinges_soft_close", ""), "hinge"),
                _display_value(row.get("splashback", "")),
                _display_value(row.get("flooring", "")),
                _display_value(row.get("source_file", "")),
                _display_value(row.get("page_refs", "")),
                _display_value(row.get("evidence_snippet", "")),
                _display_value(row.get("confidence", "")),
            ]
        )

    ws_appliances = wb.create_sheet("Appliances")
    ws_appliances.append(
        [
            "appliance_type",
            "make",
            "model_no",
            "product_url",
            "website_url",
            "overall_size",
            "source_file",
            "page_refs",
            "evidence_snippet",
            "confidence",
        ]
    )
    for row in _mapping_rows(snapshot.get("appliances", [])):
        if not _include_appliance_row(row):
            continue
        product_url = _display_value(row.get("product_url", "") or row.get("website_url", ""))
        ws_appliances.append(
            [
                _display_value(row.get("appliance_type", "")),
                _display_value(row.get("make", "")),
                _display_value(row.get("model_no", "")),
                product_url,
                product_url,
                _display_value(row.get("overall_size", "")),
                _display_value(row.get("source_file", "")),
                _display_value(row.get("page_refs", "")),
                _display_value(row.get("evidence_snippet", "")),
                _display_value(row.get("confidence", "")),
            ]
        )
        current_row = ws_appliances.max_row
        for column in ("D", "E", "F", "I"):
            ws_appliances[f"{column}{current_row}"].alignment = Alignment(wrap_text=True, vertical="top")
        for column in ("D", "E"):
            cell = ws_appliances[f"{column}{current_row}"]
            if cell.value:
                cell.hyperlink = str(cell.value)
                cell.style = "Hyperlink"

    ws_others = wb.create_sheet("Others")
    ws_others.append(["key", "value"])
    others = snapshot.get("others") or {}
    if isinstance(others, dict):
        for key, value in others.items():
            ws_others.append([_display_value(key), _display_value(value)])
    elif others:
        ws_others.append(["notes", _display_value(others)])

    ws_special = wb.create_sheet("Special Sections")
    ws_special.append(
        [
            "section_key",
            "original_section_label",
            "field_key",
            "field_value",
            "source_file",
            "page_refs",
            "evidence_snippet",
            "confidence",
        ]
    )
    for row in _mapping_rows(snapshot.get("special_sections", [])):
        fields = row.get("fields") or {}
        if not isinstance(fields, dict):
            fields = {}
        if fields:
            for field_key, field_value in fields.items():
                ws_special.append(
                    [
                        _display_value(row.get("section_key", "")),
                        _display_value(row.get("original_section_label", "")),
                        _display_value(field_key),
                        _display_value(field_value),
                        _display_value(row.get("source_file", "")),
                        _display_value(row.get("page_refs", "")),
                        _display_value(row.get("evidence_snippet", "")),
                        _display_value(row.get("confidence", "")),
                    ]
                )
        else:
            ws_special.append(
                [
                    _display_value(row.get("section_key", "")),
                    _display_value(row.get("original_section_label", "")),
                    "",
                    "",
                    _display_value(row.get("source_file", "")),
                    _display_value(row.get("page_refs", "")),
                    _display_value(row.get("evidence_snippet", "")),
                    _display_value(row.get("confidence", "")),
                ]
            )

    ws_warnings = wb.create_sheet("Warnings")
    ws_warnings.append(["warning"])
    for warning in _string_list(snapshot.get("warnings", [])):
        ws_warnings.append([warning])

    ws_meta = wb.create_sheet("Meta")
    ws_meta.append(["key", "value"])
    for key in ("job_no", "builder_name", "source_kind", "generated_at"):
        ws_meta.append([key, snapshot.get(key, "")])
    analysis = dict(snapshot.get("analysis") or {})
    for key in ("mode", "parser_strategy", "openai_attempted", "openai_succeeded", "openai_model", "note", "rule_config_updated_at", "worker_pid", "app_build_id"):
        if key in analysis:
            ws_meta.append([f"analysis_{key}", analysis.get(key, "")])
    ws_meta.append(["analysis_rule_flags", json.dumps(cleaning_rules.normalize_rule_flags(analysis.get("rule_flags", {})), ensure_ascii=False)])
    for document in _mapping_rows(snapshot.get("source_documents", [])):
        ws_meta.append(["source_document", f"{_display_value(document.get('role', ''))}: {_display_value(document.get('file_name', ''))}"])

    wb.save(path)


def _write_csv(path: Path, snapshot: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["section", "primary_key", "field", "value"])
        for row in _mapping_rows(snapshot.get("rooms", [])):
            room_key = _display_value(row.get("room_key", ""))
            benchtop_groups = _split_export_benchtops(row)
            for field_name in (
                "original_room_label",
                "bench_tops_wall_run",
                "bench_tops_island",
                "bench_tops_other",
                "floating_shelf",
                "door_colours_overheads",
                "door_colours_base",
                "door_colours_tall",
                "door_colours_island",
                "door_colours_bar_back",
                "led",
                "sink_info",
                "basin_info",
                "tap_info",
                "splashback",
                "flooring",
                "source_file",
                "page_refs",
                "evidence_snippet",
                "confidence",
            ):
                field_value = benchtop_groups.get(field_name, _display_value(row.get(field_name, "")))
                writer.writerow(["rooms", room_key, field_name, field_value])
            writer.writerow(["rooms", room_key, "drawers_soft_close", _normalize_soft_close(row.get("drawers_soft_close", ""), "drawer")])
            writer.writerow(["rooms", room_key, "hinges_soft_close", _normalize_soft_close(row.get("hinges_soft_close", ""), "hinge")])
            for field_name in ("bench_tops", "door_panel_colours", "toe_kick", "bulkheads", "handles", "accessories"):
                writer.writerow(["rooms", room_key, field_name, _display_value(row.get(field_name, []))])
            writer.writerow(["rooms", room_key, "other_items", _display_other_items(row.get("other_items", []))])
        for row in _mapping_rows(snapshot.get("appliances", [])):
            if not _include_appliance_row(row):
                continue
            row_key = f"{_display_value(row.get('appliance_type', ''))}:{_display_value(row.get('model_no', ''))}"
            product_url = _display_value(row.get("product_url", "") or row.get("website_url", ""))
            for field_name, field_value in (
                ("make", row.get("make", "")),
                ("model_no", row.get("model_no", "")),
                ("product_url", product_url),
                ("website_url", product_url),
                ("overall_size", row.get("overall_size", "")),
                ("source_file", row.get("source_file", "")),
                ("page_refs", row.get("page_refs", "")),
                ("evidence_snippet", row.get("evidence_snippet", "")),
                ("confidence", row.get("confidence", "")),
            ):
                writer.writerow(["appliances", row_key, field_name, _display_value(field_value)])
        others = snapshot.get("others") or {}
        if isinstance(others, dict):
            for key, value in others.items():
                writer.writerow(["others", "others", _display_value(key), _display_value(value)])
        elif others:
            writer.writerow(["others", "others", "notes", _display_value(others)])
        for row in _mapping_rows(snapshot.get("special_sections", [])):
            fields = row.get("fields") or {}
            row_key = _display_value(row.get("section_key", "")) or _display_value(row.get("original_section_label", ""))
            writer.writerow(["special_sections", row_key, "original_section_label", _display_value(row.get("original_section_label", ""))])
            writer.writerow(["special_sections", row_key, "source_file", _display_value(row.get("source_file", ""))])
            writer.writerow(["special_sections", row_key, "page_refs", _display_value(row.get("page_refs", ""))])
            writer.writerow(["special_sections", row_key, "evidence_snippet", _display_value(row.get("evidence_snippet", ""))])
            writer.writerow(["special_sections", row_key, "confidence", _display_value(row.get("confidence", ""))])
            if isinstance(fields, dict):
                for field_name, field_value in fields.items():
                    writer.writerow(["special_sections", row_key, _display_value(field_name), _display_value(field_value)])


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [row for row in value if isinstance(row, dict)]


def _include_appliance_row(row: dict[str, Any]) -> bool:
    appliance_type = _display_value(row.get("appliance_type", "")).lower()
    return not any(token in appliance_type for token in ("sink", "basin", "tap", "tub"))


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [text for item in value if (text := _display_value(item))]
    text = _display_value(value)
    return [text] if text else []


def _display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        parts = [_display_value(item) for item in value]
        return " | ".join(part for part in parts if part)
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return str(value)
    return str(value)


def _display_other_items(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    parts: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = _display_value(item.get("label", ""))
        entry_value = _display_value(item.get("value", ""))
        if label and entry_value:
            parts.append(f"{label}: {entry_value}")
    return " | ".join(parts)


def _normalize_soft_close(value: Any, keyword: str) -> str:
    return parsing.normalize_soft_close_value(value, keyword=keyword) or parsing.normalize_soft_close_value(value)


def _split_export_benchtops(row: dict[str, Any]) -> dict[str, str]:
    entries = parsing._coerce_string_list(row.get("bench_tops", []))
    grouped = parsing._split_benchtop_groups(entries)
    return {
        "bench_tops_wall_run": _display_value(row.get("bench_tops_wall_run", "")) or grouped["bench_tops_wall_run"],
        "bench_tops_island": _display_value(row.get("bench_tops_island", "")) or grouped["bench_tops_island"],
        "bench_tops_other": _display_value(row.get("bench_tops_other", "")) or grouped["bench_tops_other"],
    }
