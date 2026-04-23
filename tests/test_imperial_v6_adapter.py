from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from App.services.imperial_v6_adapter import (
    _merge_adjacent_subrow_items,
    build_material_rows_from_v6_section,
    build_review_rows_from_v6_section,
    build_room_from_v6_section,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_adapter_produces_rows():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    rows = build_material_rows_from_v6_section(v6_section, "dummy.pdf")
    assert len(rows) > 0
    for row in rows:
        assert "area_or_item" in row
        assert "supplier" in row
        assert "specs_or_description" in row
        assert "notes" in row
        assert row["provenance"]["source_provider"] == "v6"


def test_row_order_is_sequential():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    rows = build_material_rows_from_v6_section(v6_section, "dummy.pdf")
    by_page = defaultdict(list)
    for row in rows:
        by_page[row["page_no"]].append(row["row_order"])
    for page, orders in by_page.items():
        assert orders == list(range(1, len(orders) + 1)), f"page {page}: {orders}"


def test_merge_basic():
    items = [
        {
            "area": "HANDLES",
            "specs": "DRAWERS -Momo Trianon D Handle 128mm",
            "supplier": "Momo",
            "notes": "Knobs on Doors",
            "_source": {"page": 3, "row_index": 2, "method": "grid"},
        },
        {
            "area": "",
            "specs": "DOORS - Momo Lugo Knob 38mm",
            "supplier": "",
            "notes": "",
            "_source": {"page": 3, "row_index": 3, "method": "grid"},
        },
    ]
    merged = _merge_adjacent_subrow_items(items)
    assert len(merged) == 1
    assert "DRAWERS" in merged[0]["specs"]
    assert "DOORS" in merged[0]["specs"]
    assert merged[0]["supplier"] == "Momo"
    assert merged[0]["_source"]["row_index"] == 2


def test_merge_multi_level():
    items = [
        {"area": "HANDLES", "specs": "UPPER cabinetry - No Handles", "supplier": "Titus", "notes": "", "_source": {"page": 1, "row_index": 1}},
        {"area": "", "specs": "Doors - Knurled D Handle", "supplier": "", "notes": "", "_source": {"page": 1, "row_index": 2}},
        {"area": "", "specs": "Drawers - Knurled D Handle 256m", "supplier": "", "notes": "", "_source": {"page": 1, "row_index": 3}},
    ]
    merged = _merge_adjacent_subrow_items(items)
    assert len(merged) == 1
    assert "UPPER" in merged[0]["specs"]
    assert "Doors" in merged[0]["specs"]
    assert "Drawers" in merged[0]["specs"]


def test_merge_leading_empty_area():
    items = [
        {"area": "", "specs": "orphan content", "supplier": "", "notes": "", "_source": {"page": 1, "row_index": 1}},
        {"area": "HANDLES", "specs": "DRAWERS", "supplier": "Momo", "notes": "", "_source": {"page": 1, "row_index": 2}},
    ]
    merged = _merge_adjacent_subrow_items(items)
    assert len(merged) == 2
    assert merged[0]["area"] == ""


def test_merge_no_changes():
    items = [
        {"area": "BENCHTOP", "specs": "20mm Stone", "supplier": "Caesarstone", "notes": "", "_source": {"page": 1, "row_index": 1}},
        {"area": "BASE CABINETRY COLOUR", "specs": "Thermolaminated - Matt", "supplier": "Polytec", "notes": "", "_source": {"page": 1, "row_index": 2}},
    ]
    merged = _merge_adjacent_subrow_items(items)
    assert len(merged) == 2
    assert merged[0]["area"] == "BENCHTOP"
    assert merged[1]["area"] == "BASE CABINETRY COLOUR"


def test_review_rows_preserve_empty_area_continuations():
    v6_section = {
        "section_title": "KITCHEN JOINERY SELECTION SHEET",
        "items": [
            {"area": "HANDLES", "specs": "DRAWERS - Momo", "supplier": "Momo", "notes": "", "_source": {"page": 1}},
            {"area": "", "specs": "DOORS - Momo Lugo", "supplier": "", "notes": "", "_source": {"page": 1}},
        ],
    }
    review_rows = build_review_rows_from_v6_section(v6_section, "dummy.pdf")
    material_rows = build_material_rows_from_v6_section(v6_section, "dummy.pdf")
    assert len(review_rows) == 2
    assert review_rows[1]["area_or_item"] == ""
    assert review_rows[1]["specs_or_description"] == "DOORS - Momo Lugo"
    assert len(material_rows) == 1
    assert "DOORS - Momo Lugo" in material_rows[0]["specs_or_description"]


def test_rows_survive_finalize():
    import App.services.parsing as parsing

    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    rows = build_material_rows_from_v6_section(v6_section, "dummy.pdf")
    finalized = parsing._imperial_finalize_material_rows(rows)
    assert len(finalized) > 0


def test_tags_correct():
    """Verify tagger classifies v6 rows into expected categories.

    Note: Job 61 KITCHEN doesn't have a BENCHTOP row (not all Imperial kitchens do).
    The test verifies tagger behavior, not fixture content completeness.
    """
    import App.services.parsing as parsing

    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    rows = build_material_rows_from_v6_section(v6_section, "dummy.pdf")
    finalized = parsing._imperial_finalize_material_rows(rows)
    tags_seen = {row["tags"][0] for row in finalized if row.get("tags")}

    tagged_count = sum(1 for row in finalized if row.get("tags"))
    assert tagged_count == len(finalized), f"{len(finalized) - tagged_count} rows have no tag - tagger failed"

    assert "handles" in tags_seen, "handles tag must appear in Job 61 KITCHEN"
    assert "door_colours" in tags_seen, "door_colours tag must appear in Job 61 KITCHEN"
    # bench_tops intentionally NOT asserted - Job 61 KITCHEN has no benchtop row


def test_handle_subitems_generated():
    import App.services.parsing as parsing

    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    rows = build_material_rows_from_v6_section(v6_section, "dummy.pdf")
    finalized = parsing._imperial_finalize_material_rows(rows)
    with_subitems = parsing._imperial_attach_handle_subitems(finalized)
    handles_rows = [row for row in with_subitems if row.get("tags") == ["handles"]]
    assert len(handles_rows) > 0
    assert any(row.get("handle_subitems") for row in handles_rows)


def test_build_room():
    from App.models import RoomRow

    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room_from_v6_section(v6_section, "dummy.pdf")
    assert isinstance(room, RoomRow)
    assert room.room_key == "kitchen"
    assert room.original_room_label
    assert len(room.v6_review_rows) >= len(room.material_rows)
    assert any(row.get("notes") for row in room.v6_review_rows)
    assert len(room.material_rows) > 0
