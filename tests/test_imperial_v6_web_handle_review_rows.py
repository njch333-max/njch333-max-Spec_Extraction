from App import main as app_main


def _one_line(value: str) -> str:
    return " ".join(str(value or "").split())


def _row(area: str, supplier: str, specs: str, notes: str, row_order: int) -> dict:
    return {
        "area_or_item": area,
        "supplier": supplier,
        "specs_or_description": specs,
        "notes": notes,
        "page_no": 3,
        "row_order": row_order,
        "provenance": {"visual_sort_key": [3, float(row_order), 0, 0]},
    }


def _job38_kitchen_room() -> dict:
    base_review_row = _row(
        "HANDLES - BASE CABS\n+ OVERHEAD CABS",
        "SUPPLIED BY CLIENT\nINSTALLED BY IMPERIAL",
        "ABI INTERIORS\nElsa Cabinetry Knob- brushed copper\n(14494) for any of the other doors and the\ngas strut oh doors.",
        "",
        1,
    )
    drawer_review_row = _row(
        "LIP PULL HANDLES - DRAWERS",
        "ABI INTERIORS\nSUPPLIED BY CLIENT\nINSTALLED BY IMPERIAL",
        "ABI INTERIORS\nRappana\nCabinetry pull extended 100mm\nbrushed copper (10469)",
        "Installed Horizontally",
        2,
    )
    pantry_review_row = _row(
        "FEATURE LIP PULL\nPANTRY HANDLES",
        "ABI INTERIORS\nSUPPLIED BY CLIENT\nINSTALLED BY IMPERIAL",
        "2 X Rappana Cabinetry Pull Extended\n800mm - Brushed Copper",
        "Installed Vertically\nto pantry doors only\n(recessed into edge tape to allow\nsmallest gap possible between doors.)",
        3,
    )
    collapsed_material_row = {
        **base_review_row,
        "tags": ["handles"],
        "supplier": "",
        "specs_or_description": (
            "SUPPLIED BY CLIENT INSTALLED BY IMPERIAL - ABI INTERIORS Elsa Cabinetry Knob- brushed copper "
            "(14494) for any of the other doors and the gas strut oh doors. "
            "LIP PULL HANDLES - DRAWERS - ABI INTERIORS SUPPLIED BY CLIENT INSTALLED BY IMPERIAL - "
            "ABI INTERIORS Rappana Cabinetry pull extended 100mm brushed copper (10469) - Installed Horizontally "
            "FEATURE LIP PULL PANTRY HANDLES - ABI INTERIORS SUPPLIED BY CLIENT INSTALLED BY IMPERIAL - "
            "2 X Rappana Cabinetry Pull Extended 800mm - Brushed Copper - Installed Vertically to pantry doors only"
        ),
        "display_groups": [
            {
                "supplier": "SUPPLIED BY CLIENT",
                "lines": ["ABI INTERIORS", "Elsa Cabinetry Knob- brushed copper"],
            },
            {
                "supplier": "INSTALLED BY IMPERIAL",
                "lines": ["(14494) for any of the other doors and the", "gas strut oh doors."],
            },
        ],
        "provenance": {"visual_sort_key": [3, 1.0, 0, 0], "source_provider": "v6"},
    }
    return {
        "room_key": "kitchen",
        "original_room_label": "KITCHEN",
        "room_order": 1,
        "material_rows": [collapsed_material_row],
        "v6_review_rows": [base_review_row, drawer_review_row, pantry_review_row],
    }


def _titles(rows: list[dict]) -> list[str]:
    return [_one_line(row["title"]) for row in rows]


def test_flatten_imperial_material_rows_supplements_missing_v6_handle_review_rows() -> None:
    rows = app_main._flatten_imperial_material_rows(_job38_kitchen_room())

    assert _titles(rows) == [
        "HANDLES - BASE CABS + OVERHEAD CABS",
        "LIP PULL HANDLES - DRAWERS",
        "FEATURE LIP PULL PANTRY HANDLES",
    ]
    assert rows[1]["notes"] == "Installed Horizontally"
    assert "Rappana" in rows[1]["display_value"]
    assert rows[2]["provenance"]["supplemented_from_v6_review_rows"] is True


def test_flatten_imperial_material_rows_does_not_duplicate_existing_v6_handle_review_rows() -> None:
    room = _job38_kitchen_room()
    room["material_rows"].append({**room["v6_review_rows"][1], "tags": ["handles"]})

    rows = app_main._flatten_imperial_material_rows(room)

    assert _titles(rows).count("LIP PULL HANDLES - DRAWERS") == 1


def test_imperial_material_summary_uses_supplemented_v6_handle_review_rows() -> None:
    summary = app_main._build_material_summary(
        {
            "builder_name": "Imperial",
            "rooms": [_job38_kitchen_room()],
        }
    )

    handle_entries = summary["handles"]["entries"]
    area_labels = {
        _one_line(area)
        for entry in handle_entries
        for area in entry.get("area_or_items", [])
    }

    assert "LIP PULL HANDLES - DRAWERS" in area_labels
    assert "FEATURE LIP PULL PANTRY HANDLES" in area_labels
    assert any("Rappana" in entry.get("display_text", "") for entry in handle_entries)
