from App.services.export_service import _review_sections_from_snapshot


def _row(area, page, row_order, visual_top):
    return {
        "area_or_item": area,
        "specs_or_description": area.title(),
        "supplier": "Supplier",
        "notes": "",
        "page_no": page,
        "row_order": row_order,
        "tags": [],
        "provenance": {
            "section_title": "KITCHEN JOINERY SELECTION SHEET",
            "visual_sort_key": [0, float(visual_top), row_order, 0],
        },
    }


def test_imperial_by_section_uses_visual_order_instead_of_row_order_only():
    snapshot = {
        "builder_name": "Imperial",
        "rooms": [
            {
                "room_key": "kitchen",
                "original_room_label": "KITCHEN",
                "room_order": 1,
                "material_rows": [_row("MATERIAL FALLBACK", 1, 1, 10)],
                "v6_review_rows": [
                    _row("PAGE 2 FIRST", 2, 1, 10),
                    _row("PAGE 1 SECOND", 1, 2, 200),
                    _row("PAGE 3 FIRST", 3, 1, 10),
                    _row("PAGE 1 FIRST", 1, 1, 100),
                ],
            }
        ],
    }

    sections, _flagged, _summary, _counts = _review_sections_from_snapshot(snapshot)

    assert [item["area"] for item in sections[0]["items"]] == [
        "PAGE 1 FIRST",
        "PAGE 1 SECOND",
        "PAGE 2 FIRST",
        "PAGE 3 FIRST",
    ]
