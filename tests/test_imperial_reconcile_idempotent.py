from __future__ import annotations

from copy import deepcopy

from App.services import parsing as parsing_module


def _synth_rows(room: dict[str, object]) -> list[dict[str, object]]:
    return [
        row
        for row in room.get("material_rows", [])
        if isinstance(row, dict)
        and isinstance(row.get("provenance", {}), dict)
        and row.get("provenance", {}).get("synthesized_from_room_handles") is True
    ]


def _room_with_missing_handle(*, handles: list[str], material_rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "room_key": "kitchen",
        "source_file": "source.pdf",
        "page_refs": "1, 2, 3, 4",
        "confidence": 0.72,
        "handles": list(handles),
        "material_rows": deepcopy(material_rows or []),
    }


def test_imperial_reconcile_material_rows_with_room_fields_is_idempotent_for_existing_synth_rows():
    room = _room_with_missing_handle(
        handles=["Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"],
        material_rows=[
            {
                "area_or_item": "BENCHTOP",
                "supplier": "Caesarstone",
                "specs_or_description": "Mirabel",
                "notes": "",
                "tags": ["bench_tops"],
                "page_no": 1,
                "row_order": 1,
            }
        ],
    )

    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)
    assert len(_synth_rows(room)) == 1

    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)
    assert len(_synth_rows(room)) == 1


def test_imperial_reconcile_material_rows_with_room_fields_still_emits_new_handle_text_on_second_pass():
    room = _room_with_missing_handle(
        handles=["Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"],
        material_rows=[
            {
                "area_or_item": "BENCHTOP",
                "supplier": "Caesarstone",
                "specs_or_description": "Mirabel",
                "notes": "",
                "tags": ["bench_tops"],
                "page_no": 1,
                "row_order": 1,
            }
        ],
    )

    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)
    assert [row["provenance"]["layout_value_text"] for row in _synth_rows(room)] == [
        "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"
    ]

    room["handles"] = [
        "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above",
        "Talls: Push to open",
    ]
    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)

    synth_rows = _synth_rows(room)
    assert len(synth_rows) == 2
    assert [row["provenance"]["layout_value_text"] for row in synth_rows] == [
        "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above",
        "Talls: Push to open",
    ]


def test_imperial_reconcile_material_rows_with_room_fields_job55_kitchen_replay_emits_one_synth_row():
    room = {
        "room_key": "kitchen",
        "source_file": "source.pdf",
        "page_refs": "1, 2",
        "confidence": 0.72,
        "handles": [
            "NO HANDLES OVERHEADS",
            "Base Drawers: Kethy - PM2817 / 288 / MSIL Matt Silver 288 Hole centres - 312 OA SIZE - Polytec Horizontal Install",
            "Base Doors: Kethy - PM2817 / 192 / MSIL Matt Silver 192 Hole centres - 216 OA SIZE - KETHY Vertical Install",
            "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above",
        ],
        "material_rows": [
            {
                "area_or_item": "HANDLES - BASE DRAWERS",
                "supplier": "",
                "specs_or_description": "Polytec - KETHY PM2817 / 288 / MSIL Matt Silver 288 Hole centres - 312 OA SIZE - Horizontal Install",
                "notes": "",
                "tags": ["handles"],
                "page_no": 2,
                "row_order": 2,
            },
            {
                "area_or_item": "HANDLES - BASE DOORS\nNO HANDLES OVERHEADS",
                "supplier": "",
                "specs_or_description": "Kethy - PM2817 / 192 / MSIL Matt Silver 192 Hole centres - 216 OA SIZE Touch catch - Overheads above - Vertical Install",
                "notes": "",
                "tags": ["handles"],
                "page_no": 2,
                "row_order": 3,
            },
        ],
    }

    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)
    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)

    synth_rows = _synth_rows(room)
    assert len(synth_rows) == 1
    assert synth_rows[0]["provenance"]["layout_value_text"] == (
        "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"
    )


def test_imperial_reconcile_material_rows_with_room_fields_ignores_empty_existing_layout_value_text():
    room = _room_with_missing_handle(
        handles=["Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"],
        material_rows=[
            {
                "area_or_item": "HANDLES",
                "supplier": "",
                "specs_or_description": "placeholder",
                "notes": "",
                "tags": ["handles"],
                "page_no": 1,
                "row_order": 1,
                "provenance": {
                    "synthesized_from_room_handles": True,
                    "layout_value_text": "",
                },
            }
        ],
    )

    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)

    synth_rows = _synth_rows(room)
    assert len(synth_rows) == 2
    assert [row["provenance"].get("layout_value_text", "") for row in synth_rows] == [
        "",
        "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above",
    ]


def test_imperial_reconcile_material_rows_with_room_fields_real_row_does_not_block_synth_guard():
    room = _room_with_missing_handle(
        handles=["Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"],
        material_rows=[
            {
                "area_or_item": "HANDLES",
                "supplier": "",
                "specs_or_description": "Overheads:",
                "notes": "Recessed finger space cooktop overheads. Touch catch - Overheads above",
                "tags": ["handles"],
                "page_no": 1,
                "row_order": 1,
                "provenance": {
                    "layout_value_text": "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above",
                },
            }
        ],
    )

    parsing_module._imperial_reconcile_material_rows_with_room_fields(room)

    synth_rows = _synth_rows(room)
    assert len(synth_rows) == 1
    assert synth_rows[0]["provenance"]["layout_value_text"] == (
        "Overheads: Recessed finger space cooktop overheads. Touch catch - Overheads above"
    )
