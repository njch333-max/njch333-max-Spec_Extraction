from __future__ import annotations

import json
from pathlib import Path

from App.models import RoomRow
from App.services.imperial_v6_room_fields import populate_room_fields_from_v6


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def build_room(v6_section: dict, all_sections=None) -> RoomRow:
    label = str(v6_section.get("section_title") or "").replace(" JOINERY SELECTION SHEET", "").strip()
    room = RoomRow(room_key="kitchen", original_room_label=label)
    populate_room_fields_from_v6(room, v6_section, all_sections or [v6_section])
    return room


def test_soft_close_populated_from_metadata():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert room.hinges_soft_close == "Soft Close"
    assert room.drawers_soft_close == "Soft Close"


def test_flooring_from_metadata():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert room.flooring == "Vinyl"


def test_toe_kick_populated():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert "As Doors" in room.toe_kick


def test_door_colours_base_populated():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert room.door_colours_base
    assert "Polytec" in room.door_colours_base
    assert "Classic White" in room.door_colours_base


def test_door_colours_overheads_populated():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert room.door_colours_overheads


def test_door_panel_colours_list_not_empty():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert len(room.door_panel_colours) >= 1


def test_handles_parsed_from_multiline_specs():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert len(room.handles) >= 2


def test_accessories_contains_bin():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert "Hettich" in str(room.accessories)


def test_has_explicit_base_true():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert room.has_explicit_base is True


def test_led_defaults_to_no_when_absent():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section)
    assert room.led == "No"


def test_sink_info_empty_when_no_other_sections_provided():
    v6_section = load_fixture("job_61_kitchen_v6_section.json")
    room = build_room(v6_section, all_sections=[])
    assert room.sink_info == ""


def test_sink_info_matches_sink_prefix_variant():
    """v6 may label items as 'SINK (LAUNDRY)' (not 'SINKWARE (LAUNDRY)'). Both must match."""
    laundry_section = {
        "section_title": "LAUNDRY JOINERY SELECTION SHEET",
        "metadata": {"hinges": "Soft Close", "floor_type": "Tiled"},
        "items": [
            {"area": "BASE CABINETRY COLOUR", "specs": "White", "supplier": "Polytec",
             "notes": "", "_source": {"page": 1, "row_index": 1}},
        ],
    }
    sinkware_section = {
        "section_title": "SINKWARE & TAPWARE",
        "metadata": {},
        "items": [
            {"area": "SINK (LAUNDRY)", "specs": "45L tub YH236C",
             "supplier": "By Others", "notes": "Taphole location: In sink corner",
             "_source": {"page": 3, "row_index": 1}},
        ],
    }
    room = RoomRow(room_key="laundry", original_room_label="LAUNDRY")
    populate_room_fields_from_v6(room, laundry_section, [laundry_section, sinkware_section])
    assert "45L tub YH236C" in room.sink_info, f"Expected sink_info to contain SINK (LAUNDRY) item, got: {room.sink_info!r}"


def test_basin_info_matches_master_ensuite_via_ensuite_marker():
    """v6 may use '(ENSUITE)' marker when room label is 'MASTER ENSUITE'. Must cross-match."""
    ensuite_section = {
        "section_title": "MASTER ENSUITE JOINERY SELECTION SHEET",
        "metadata": {"hinges": "Soft Close"},
        "items": [
            {"area": "BASE CABINETRY COLOUR", "specs": "Oak", "supplier": "Polytec",
             "notes": "", "_source": {"page": 7, "row_index": 1}},
        ],
    }
    sinkware_section = {
        "section_title": "SINKWARE & TAPWARE",
        "metadata": {},
        "items": [
            {"area": "BASIN (ENSUITE)", "specs": "Above counter Specs TBC",
             "supplier": "By Others", "notes": "Taphole location: In stone",
             "_source": {"page": 12, "row_index": 5}},
        ],
    }
    room = RoomRow(room_key="master_ensuite", original_room_label="MASTER ENSUITE")
    populate_room_fields_from_v6(room, ensuite_section, [ensuite_section, sinkware_section])
    assert "Above counter" in room.basin_info, f"Expected basin_info via (ENSUITE) marker, got: {room.basin_info!r}"
