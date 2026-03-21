from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from openpyxl import Workbook

from App.services.runtime import ensure_job_dirs, safe_filename, utc_now_iso


def build_exports(job_no: str, snapshot: dict[str, Any]) -> dict[str, str]:
    dirs = ensure_job_dirs(job_no)
    stamp = utc_now_iso().replace(":", "").replace("-", "")
    excel_path = dirs["export_dir"] / f"{safe_filename(job_no)}_{stamp}.xlsx"
    csv_path = dirs["export_dir"] / f"{safe_filename(job_no)}_{stamp}.csv"
    _write_excel(excel_path, snapshot)
    _write_csv(csv_path, snapshot)
    return {"excel": str(excel_path), "csv": str(csv_path)}


def _write_excel(path: Path, snapshot: dict[str, Any]) -> None:
    wb = Workbook()
    ws_rooms = wb.active
    ws_rooms.title = "Rooms"
    ws_rooms.append(
        [
            "room_key",
            "original_room_label",
            "bench_tops",
            "door_panel_colours",
            "toe_kick",
            "bulkheads",
            "handles",
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
    for row in snapshot.get("rooms", []):
        ws_rooms.append(
            [
                row.get("room_key", ""),
                row.get("original_room_label", ""),
                " | ".join(row.get("bench_tops", [])),
                " | ".join(row.get("door_panel_colours", [])),
                " | ".join(row.get("toe_kick", [])),
                " | ".join(row.get("bulkheads", [])),
                " | ".join(row.get("handles", [])),
                row.get("drawers_soft_close", ""),
                row.get("hinges_soft_close", ""),
                row.get("splashback", ""),
                row.get("flooring", ""),
                row.get("source_file", ""),
                row.get("page_refs", ""),
                row.get("evidence_snippet", ""),
                row.get("confidence", ""),
            ]
        )

    ws_appliances = wb.create_sheet("Appliances")
    ws_appliances.append(
        ["appliance_type", "make", "model_no", "website_url", "overall_size", "source_file", "page_refs", "evidence_snippet", "confidence"]
    )
    for row in snapshot.get("appliances", []):
        ws_appliances.append(
            [
                row.get("appliance_type", ""),
                row.get("make", ""),
                row.get("model_no", ""),
                row.get("website_url", ""),
                row.get("overall_size", ""),
                row.get("source_file", ""),
                row.get("page_refs", ""),
                row.get("evidence_snippet", ""),
                row.get("confidence", ""),
            ]
        )

    ws_others = wb.create_sheet("Others")
    ws_others.append(["key", "value"])
    for key, value in (snapshot.get("others") or {}).items():
        ws_others.append([key, value])

    ws_meta = wb.create_sheet("Meta")
    ws_meta.append(["key", "value"])
    for key in ("job_no", "builder_name", "source_kind", "generated_at"):
        ws_meta.append([key, snapshot.get(key, "")])
    for warning in snapshot.get("warnings", []):
        ws_meta.append(["warning", warning])

    wb.save(path)


def _write_csv(path: Path, snapshot: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["section", "primary_key", "field", "value"])
        for row in snapshot.get("rooms", []):
            room_key = row.get("room_key", "")
            for field_name in ("original_room_label", "drawers_soft_close", "hinges_soft_close", "splashback", "flooring", "source_file", "page_refs", "evidence_snippet", "confidence"):
                writer.writerow(["rooms", room_key, field_name, row.get(field_name, "")])
            for field_name in ("bench_tops", "door_panel_colours", "toe_kick", "bulkheads", "handles"):
                writer.writerow(["rooms", room_key, field_name, " | ".join(row.get(field_name, []))])
        for row in snapshot.get("appliances", []):
            row_key = f"{row.get('appliance_type', '')}:{row.get('model_no', '')}"
            for field_name in ("make", "model_no", "website_url", "overall_size", "source_file", "page_refs", "evidence_snippet", "confidence"):
                writer.writerow(["appliances", row_key, field_name, row.get(field_name, "")])
        for key, value in (snapshot.get("others") or {}).items():
            writer.writerow(["others", "others", key, value])
