from __future__ import annotations

import json
from pathlib import Path

import pytest

from App.services.evoca_structured_adapter import build_evoca_snapshot_from_structured


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "evoca_structured_section_filter"
VALIDATED_PDF_IDS = (
    "EVOC447",
    "EVOC467",
    "EVOC473",
    "EVOC471",
    "EVOC482",
    "EVOC436",
    "EVOC449",
    "EVOC479",
    "EVOC480",
)
INCLUDED_SECTION_CODES = {"15", "17", "20", "23", "24", "25"}
SKIPPED_SECTION_CODES = {"16", "18", "19", "21", "22"}


def _row(
    label: str,
    value: str,
    *,
    page_no: int = 1,
    row_order: int = 1,
    source_method: str = "pdfplumber_table",
    is_diagnostic: bool = False,
    is_group_anchor: bool = False,
) -> dict[str, object]:
    return {
        "label": label,
        "value": value,
        "page_no": page_no,
        "row_order": row_order,
        "table_index": 0,
        "row_index": row_order,
        "raw_cells": ["-", label, value],
        "source_method": source_method,
        "is_diagnostic": is_diagnostic,
        "is_group_anchor": is_group_anchor,
    }


def _group(label: str, rows: list[dict[str, object]], *, page_start: int = 1, page_end: int | None = None) -> dict[str, object]:
    return {
        "group_label": label,
        "page_start": page_start,
        "page_end": page_end or page_start,
        "rows": rows,
    }


def _room(label: str, groups: list[dict[str, object]], *, page_start: int = 1) -> dict[str, object]:
    return {
        "room_label": label,
        "room_key": label.lower().replace(" ", "_"),
        "page_start": page_start,
        "page_end": page_start,
        "groups": groups,
    }


def _section(
    code: str,
    title: str,
    *,
    rooms: list[dict[str, object]] | None = None,
    groups: list[dict[str, object]] | None = None,
    order: int = 1,
) -> dict[str, object]:
    return {
        "section_code": code,
        "section_title": title,
        "section_order": order,
        "page_start": order,
        "page_end": order,
        "rooms": rooms or [],
        "groups": groups or [],
    }


def _structured(sections: list[dict[str, object]]) -> dict[str, object]:
    return {
        "source_pdf": "evoca.pdf",
        "document_name": "evoca.pdf",
        "builder": "Evoca",
        "schema_version": "evoca_structured_v0",
        "pages": [{"page_no": 1}, {"page_no": 2}, {"page_no": 3}],
        "sections": sections,
        "statistics": {},
    }


def test_source_document_payload_does_not_retain_loaded_page_text() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Kitchen",
                        [_group("Benchtops", [_row("Colour", "Statuario Zero")])],
                    )
                ],
            )
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(
        structured,
        job_no="38148",
        source_document={
            "file_name": "evoca.pdf",
            "path": "evoca.pdf",
            "role": "spec",
            "pages": [
                {
                    "page_no": 1,
                    "text": "16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM\nDaiken",
                    "raw_text": "16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM\nDaiken",
                }
            ],
        },
    )

    assert "pages" not in snapshot.source_documents[0]
    assert "Daiken" not in snapshot.model_dump_json()
    assert "16 ELECTRICAL" not in snapshot.model_dump_json()


def _fixture_path(pdf_id: str) -> Path:
    matches = sorted(FIXTURE_DIR.glob(f"*{pdf_id}*.json"))
    assert len(matches) == 1, f"{pdf_id}: expected one structured fixture, found {matches}"
    return matches[0]


def _section_code(section: dict[str, object]) -> str:
    code = str(section.get("section_code") or "").strip()
    if code:
        return code
    title = str(section.get("section_title") or "").strip()
    first = title.split(maxsplit=1)[0] if title else ""
    return first if first.isdigit() else ""


def _snapshot_values(snapshot) -> set[str]:
    values: set[str] = set()
    for room in snapshot.rooms:
        for material_row in room.material_rows:
            value = str(material_row.get("value") or "")
            if value:
                values.add(value)
        for item in room.other_items:
            value = item.get("value", "")
            if value:
                values.add(value)
        for field_name in (
            "bench_tops_wall_run",
            "bench_tops_island",
            "bench_tops_other",
            "door_colours_overheads",
            "door_colours_base",
            "door_colours_tall",
            "sink_info",
            "basin_info",
            "tap_info",
            "splashback",
            "flooring",
        ):
            values.update(line for line in str(getattr(room, field_name) or "").splitlines() if line)
        values.update(room.handles)
        values.update(room.toe_kick)
        values.update(room.bulkheads)
        values.update(room.accessories)
    for appliance in snapshot.appliances:
        if appliance.model_no:
            values.add(appliance.model_no)
    for special in snapshot.special_sections:
        values.update(value for value in special.fields.values() if value)
    return values


def _business_values(structured: dict[str, object]) -> set[str]:
    values: set[str] = set()
    for section in structured.get("sections", []):
        section_code = _section_code(section)
        if section_code in SKIPPED_SECTION_CODES or section_code not in INCLUDED_SECTION_CODES:
            continue
        for room in section.get("rooms", []):
            for group in room.get("groups", []):
                for row in group.get("rows", []):
                    if not row.get("is_diagnostic") and row.get("value"):
                        values.add(str(row["value"]))
        for group in section.get("groups", []):
            for row in group.get("rows", []):
                if not row.get("is_diagnostic") and row.get("value"):
                    values.add(str(row["value"]))
    return values


def _continuation_labels(snapshot) -> list[str]:
    labels: list[str] = []
    for room in snapshot.rooms:
        for material_row in room.material_rows:
            labels.extend(
                str(material_row.get(field_name) or "")
                for field_name in ("label", "area_or_item", "group_label")
            )
        labels.extend(str(item.get("label") or "") for item in room.other_items)
    for special in snapshot.special_sections:
        labels.extend(str(field_name) for field_name in special.fields)
        labels.append(str(special.original_section_label or ""))
    for appliance in snapshot.appliances:
        labels.append(str(appliance.appliance_type or ""))
    return [label for label in labels if "Continuation" in label]


def test_evoca_adapter_requires_evoca_structured_schema() -> None:
    payload = _structured([])
    payload["schema_version"] = "legacy"

    with pytest.raises(ValueError, match="schema_version"):
        build_evoca_snapshot_from_structured(payload, "38148")


def test_evoca_adapter_routes_type_by_parent_group() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Bathroom",
                        [
                            _group(
                                "Underbench",
                                [
                                    _row("Manufacturer", "Polytec", page_no=8),
                                    _row("Colour & Finish", "Alora Gloss White", page_no=8),
                                ],
                                page_start=8,
                            )
                        ],
                        page_start=8,
                    )
                ],
            ),
            _section(
                "20",
                "20 PLUMBING FIXTURES & TAPWARE",
                rooms=[
                    _room(
                        "Bathroom",
                        [
                            _group(
                                "Basin",
                                [
                                    _row("Model", "Eden Bench Mount Gloss White (FL135-W)", page_no=12),
                                    _row("Type", "Overmount", page_no=12),
                                ],
                                page_start=12,
                            ),
                            _group(
                                "Basin Mixer",
                                [
                                    _row("Type", "Alder 54082 Brushed Nickel", page_no=12),
                                    _row("Location", "Centre of Basin", page_no=12),
                                ],
                                page_start=12,
                            ),
                        ],
                        page_start=12,
                    )
                ],
                order=2,
            ),
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    bathroom = snapshot.rooms[0]
    assert bathroom.room_key == "bathroom"
    assert "Type: Overmount" in bathroom.basin_info
    assert "Type: Alder 54082 Brushed Nickel" in bathroom.tap_info
    assert "Overmount" not in bathroom.tap_info
    assert all(row.get("source_provider") == "evoca_structured_v0" for row in bathroom.material_rows)
    assert bathroom.v6_review_rows == []


def test_evoca_adapter_uses_cabinet_section_soft_close_note() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Kitchen",
                        [
                            _group(
                                "Benchtops",
                                [
                                    _row("Manufacturer", "Quantum Quartz", page_no=9),
                                    _row("Colour", "Ambra", page_no=9),
                                ],
                                page_start=9,
                            )
                        ],
                        page_start=9,
                    )
                ],
            )
        ]
    )
    structured["sections"][0]["notes"] = [
        _row(
            "Section Note",
            "All Cabinets include Soft Close Hinges & Runners, lined internally with White Melamine.",
            page_no=9,
        )
    ]

    snapshot = build_evoca_snapshot_from_structured(structured, "38337")

    kitchen = snapshot.rooms[0]
    assert kitchen.drawers_soft_close == "Soft Close"
    assert kitchen.hinges_soft_close == "Soft Close"


def test_evoca_adapter_does_not_map_section20_accessories_to_room_accessories() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[_room("Bathroom", [_group("Underbench", [_row("Manufacturer", "Polytec"), _row("Colour & Finish", "Tasmanian Oak Matt")])])],
            ),
            _section(
                "20",
                "20 PLUMBING FIXTURES & TAPWARE",
                rooms=[
                    _room(
                        "Bathroom",
                        [
                            _group("Basin", [_row("Model", "Byron Bench Mount Gloss White"), _row("Type", "Overmount")]),
                            _group("Basin Mixer", [_row("Type", "Spin Brushed Brass Tall Basin Mixer"), _row("Location", "Centre of Basin")]),
                            _group("Shower", [_row("Shower Screen", "Semi-frameless with Clear Toughened Glass")]),
                            _group("Accessories", [_row("Toilet Suite", "Kirra Rimless Close Coupled Toilet Suite Gloss White")]),
                        ],
                    )
                ],
                order=2,
            ),
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38337")

    bathroom = snapshot.rooms[0]
    assert "Byron Bench Mount Gloss White" in bathroom.basin_info
    assert "Spin Brushed Brass Tall Basin Mixer" in bathroom.tap_info
    assert bathroom.accessories == []
    assert any(row.get("value") == "Semi-frameless with Clear Toughened Glass" for row in bathroom.material_rows)
    assert any(row.get("value") == "Kirra Rimless Close Coupled Toilet Suite Gloss White" for row in bathroom.material_rows)


def test_evoca_adapter_round_trips_business_values_verbatim() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Kitchen",
                        [
                            _group(
                                "Benchtops",
                                [
                                    _row("Manufacturer", "Quantum Quartz", page_no=8),
                                    _row("Colour", "Verona Gold WK Stone", page_no=8),
                                    _row("Waterfall End to Island", "40mm Waterfall End", page_no=8),
                                ],
                                page_start=8,
                            ),
                            _group(
                                "Drawers",
                                [
                                    _row("Standard", "Soft Close Drawers", page_no=8),
                                    _row("Pot", "Pot Drawer Set", page_no=8),
                                ],
                                page_start=8,
                            ),
                        ],
                        page_start=8,
                    )
                ],
            ),
            _section(
                "17",
                "17 APPLIANCES, ACCESSORIES & HOT WATER UNIT",
                groups=[
                    _group(
                        "Appliances",
                        [
                            _row("Hot Plate", "Fisher & Paykel 900mm 5 Zone Ceramic Cooktop CE905CBX2 (Electric)", page_no=11),
                            _row("Second Hot Plate", "N/A CLIENT TO CHECK", page_no=11),
                        ],
                        page_start=11,
                    ),
                    _group("Hot Water Unit", [_row("Hot Water Unit", "Ariston Primos 205 Litre Heat Pump (Electric)", page_no=11)], page_start=11),
                ],
                order=2,
            ),
            _section(
                "20",
                "20 PLUMBING FIXTURES & TAPWARE",
                rooms=[
                    _room(
                        "Powder",
                        [
                            _group("Accessories & Toilet Suite", [_row("Toilet Suite", "WC", is_diagnostic=True, page_no=12)]),
                        ],
                        page_start=12,
                    )
                ],
                order=3,
            ),
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    assert _business_values(structured) <= _snapshot_values(snapshot)
    assert all("WC" not in str(getattr(room, field_name)) for room in snapshot.rooms for field_name in ("sink_info", "basin_info", "tap_info", "flooring", "splashback"))
    assert any(key.startswith("Diagnostic /") and value == "WC" for special in snapshot.special_sections for key, value in special.fields.items())
    assert "N/A CLIENT TO CHECK" in _snapshot_values(snapshot)


@pytest.mark.parametrize("pdf_id", VALIDATED_PDF_IDS)
def test_evoca_adapter_round_trips_validated_pdf_json(pdf_id: str) -> None:
    path = _fixture_path(pdf_id)
    structured = json.loads(path.read_text(encoding="utf-8"))

    snapshot = build_evoca_snapshot_from_structured(structured, job_no=pdf_id, source_document=str(path))

    missing = _business_values(structured) - _snapshot_values(snapshot)
    assert not missing, f"{pdf_id}: business values missing from snapshot: {sorted(missing)[:10]}"
    assert _continuation_labels(snapshot) == []
    skipped_keys = tuple(f"evoca_{section_code}_" for section_code in SKIPPED_SECTION_CODES)
    assert not any(special.section_key.startswith(skipped_keys) for special in snapshot.special_sections)


def test_evoca_adapter_does_not_reconstruct_skipped_sections() -> None:
    structured = _structured(
        [
            _section("16", "16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM", groups=[_group("Alarm System", [_row("Alarm System", "Included")])]),
            _section(
                "15",
                "15 CABINETS",
                rooms=[_room("Kitchen", [_group("Benchtops", [_row("Manufacturer", "Quantum Quartz")])])],
                order=2,
            ),
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    assert "Included" not in _snapshot_values(snapshot)
    assert all("16" not in special.section_key for special in snapshot.special_sections)


def test_evoca_adapter_preserves_bug11_toilet_suite_full_name() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room("Bathroom", [_group("Underbench", [_row("Manufacturer", "Polytec"), _row("Colour & Finish", "Oyster Grey")])]),
                    _room("Ensuite", [_group("Underbench", [_row("Manufacturer", "Polytec"), _row("Colour & Finish", "Oyster Grey")])]),
                ],
            ),
            _section(
                "20",
                "20 PLUMBING FIXTURES & TAPWARE",
                rooms=[
                    _room("Bathroom", [_group("Accessories", [_row("Toilet Suite", "Alora Gloss White Wall Faced Toilet Suite", page_no=12)])]),
                    _room("Ensuite", [_group("Accessories & Toilet Suite", [_row("Toilet Suite", "Alora Gloss White Wall Faced Toilet Suite", page_no=13)])]),
                ],
                order=2,
            ),
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    values = _snapshot_values(snapshot)
    assert "Alora Gloss White Wall Faced Toilet Suite" in values
    assert "Alora Gloss White Wall Faced" not in values


def test_evoca_adapter_preserves_cross_page_synthesis_rows_with_page_refs() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Powder",
                        [
                            _group(
                                "Benchtops",
                                [
                                    _row("Manufacturer", "Quantum Quartz", page_no=12, source_method="pdfplumber_raw_text_anchor_synthesis"),
                                    _row("Colour", "Polar", page_no=12, source_method="pdfplumber_raw_text_anchor_synthesis"),
                                ],
                                page_start=11,
                                page_end=12,
                            )
                        ],
                        page_start=11,
                    ),
                    _room(
                        "Ensuite 2",
                        [
                            _group(
                                "Underbench",
                                [_row("Manufacturer", "Polytec", page_no=13)],
                                page_start=13,
                            )
                        ],
                        page_start=13,
                    ),
                ],
            ),
            _section(
                "20",
                "20 PLUMBING FIXTURES & TAPWARE",
                rooms=[
                    _room(
                        "Ensuite 2",
                        [
                            _group(
                                "Basin Mixer",
                                [_row("Type", "Spin Gun Metal Tall Basin Mixer (SP110-GM)", page_no=14, source_method="pdfplumber_raw_text_anchor_synthesis")],
                                page_start=13,
                                page_end=14,
                            )
                        ],
                        page_start=13,
                    )
                ],
                order=2,
            ),
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38117")

    powder = next(room for room in snapshot.rooms if room.room_key == "powder")
    ensuite_2 = next(room for room in snapshot.rooms if room.room_key == "ensuite_2")
    assert any(row["value"] == "Polar" and row["page_refs"] == "12" for row in powder.material_rows)
    assert any(row["value"] == "Spin Gun Metal Tall Basin Mixer (SP110-GM)" and row["page_refs"] == "14" for row in ensuite_2.material_rows)


def test_evoca_adapter_routes_terminal_values_without_retaining_by_them() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Kitchen",
                        [
                            _group("Benchtops", [_row("Manufacturer", "Quantum Quartz")]),
                            _group(
                                "Overhead Cupboards",
                                [_row("Colour & Finish", "Not Applicable - by owner after handover")],
                            ),
                        ],
                    )
                ],
            )
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    kitchen = snapshot.rooms[0]
    assert kitchen.has_explicit_overheads is True
    assert "Colour & Finish: Not Applicable - by owner after handover" in kitchen.door_colours_overheads


def test_evoca_adapter_keeps_benchtop_colour_finish_in_canonical_field() -> None:
    structured = _structured(
        [
            _section(
                "15",
                "15 CABINETS",
                rooms=[
                    _room(
                        "Study Desk",
                        [
                            _group(
                                "Benchtops",
                                [
                                    _row("Manufacturer", "Polytec", page_no=10),
                                    _row("Colour & Finish", "Liguarian Wallnut Woodmatt", page_no=10),
                                    _row("Edge Profile", "10/10 Radius", page_no=10),
                                ],
                                page_start=10,
                            )
                        ],
                        page_start=10,
                    )
                ],
            )
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    study = snapshot.rooms[0]
    assert study.room_key == "study_desk"
    assert "Manufacturer: Polytec" in study.bench_tops_other
    assert "Colour & Finish: Liguarian Wallnut Woodmatt" in study.bench_tops_other
    assert "Edge Profile: 10/10 Radius" in study.bench_tops_other


def test_evoca_adapter_evoc479_fixture_extracts_soft_close_from_section_note() -> None:
    path = _fixture_path("EVOC479")
    structured = json.loads(path.read_text(encoding="utf-8"))

    snapshot = build_evoca_snapshot_from_structured(structured, job_no="EVOC479", source_document=str(path))

    assert snapshot.rooms
    assert {room.drawers_soft_close for room in snapshot.rooms} == {"Soft Close"}
    assert {room.hinges_soft_close for room in snapshot.rooms} == {"Soft Close"}


def test_evoca_adapter_drops_fixture_only_rooms_but_keeps_evidence() -> None:
    structured = _structured(
        [
            _section(
                "20",
                "20 PLUMBING FIXTURES & TAPWARE",
                rooms=[
                    _room(
                        "Alfresco",
                        [
                            _group("Sink Mixer", [_row("Type", "Not Applicable"), _row("Location", "Outdoor Shower")]),
                        ],
                    )
                ],
            )
        ]
    )

    snapshot = build_evoca_snapshot_from_structured(structured, "38148")

    assert snapshot.rooms == []
    assert any(special.section_key == "evoca_unretained_alfresco" for special in snapshot.special_sections)
    assert "Outdoor Shower" in _snapshot_values(snapshot)
