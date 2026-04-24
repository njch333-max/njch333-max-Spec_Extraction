from __future__ import annotations

from copy import deepcopy

import App.main as app_main
from App.services.imperial_v6_adapter import _map_v6_item_to_material_row


SECTION_TITLE = "KITCHEN JOINERY SELECTION SHEET"


def _v6_item(area: str, specs: str, supplier: str, row_index: int = 1) -> dict:
    return {
        "area": area,
        "specs": specs,
        "supplier": supplier,
        "notes": "",
        "_source": {"page": 1, "row_index": row_index, "method": "grid"},
    }


def _v6_material_row(area: str, specs: str, supplier: str, tags: list[str], row_order: int = 1) -> dict:
    row = _map_v6_item_to_material_row(_v6_item(area, specs, supplier, row_order), "dummy.pdf", row_order, SECTION_TITLE)
    row["tags"] = tags
    return row


def _summary_for_rows(rows: list[dict]) -> dict:
    return app_main._build_imperial_material_summary(
        {
            "builder_name": "Imperial",
            "rooms": [
                {
                    "room_key": "kitchen",
                    "original_room_label": "KITCHEN",
                    "room_order": 1,
                    "material_rows": deepcopy(rows),
                }
            ],
        }
    )


def _bucket_texts(summary: dict, bucket_key: str) -> list[str]:
    return [entry["text"] for entry in summary[bucket_key]["entries"]]


def test_v6_handle_summary_candidates_emit_all_display_lines_with_supplier_prefix():
    row = _v6_material_row(
        "HANDLES",
        (
            "Finger Pull on Uppers- PTO where required\n"
            "L7817 - Oak Matt Black (OAKBK)\n"
            "160mm - Lowers and Drawers\n"
            "320mm - Pantry Door"
        ),
        "Kethy",
        ["handles"],
    )

    assert app_main._imperial_material_row_handle_summary_candidates(row) == [
        "Kethy - Finger Pull on Uppers- PTO where required",
        "Kethy - L7817 - Oak Matt Black (OAKBK)",
        "Kethy - 160mm - Lowers and Drawers",
        "Kethy - 320mm - Pantry Door",
    ]


def test_v6_door_colour_single_supplier_merges_specs_within_row():
    rows = [
        _v6_material_row("BASE AND UPPER (INCL BOTTOMS) CABINETRY COLOUR", "Amaro Matt", "Polytec", ["door_colours"], 1),
        _v6_material_row(
            "OPEN UPPER CABINETRY COLOUR (INCL BOTTOMS)",
            "Surround - Prime Oak Matt\nBacks only - Forage Smooth",
            "Polytec",
            ["door_colours"],
            2,
        ),
        _v6_material_row(
            "OPEN LOWER CABINETRY COLOUR",
            "Surround - Prime Oak Matt\nBacks only - Forage Smooth",
            "Polytec",
            ["door_colours"],
            3,
        ),
    ]

    texts = _bucket_texts(_summary_for_rows(rows), "door_colours")

    assert set(texts) == {
        "Polytec - Amaro Matt",
        "Polytec - Surround - Prime Oak Matt Backs only - Forage Smooth",
    }
    assert len(texts) == 2


def test_v6_benchtop_summary_pairs_supplier_and_spec_lines():
    row = _v6_material_row(
        "BENCHTOP",
        "2Omm Stone - 4030 Oyster - PR\n20mm Shadowline under Benchtop -Forage Smooth",
        "Caesarstone\nBy Imperial",
        ["bench_tops"],
    )

    assert _bucket_texts(_summary_for_rows([row]), "bench_tops") == [
        "Caesarstone - 2Omm Stone - 4030 Oyster - PR",
        "By Imperial - 20mm Shadowline under Benchtop -Forage Smooth",
    ]


def test_v6_benchtop_mismatched_spec_count_emits_hinted_entry():
    row = _v6_material_row(
        "BENCHTOP",
        "2Omm Stone - 4030 Oyster - PR\n20mm Shadowline under Benchtop -Forage\nSmooth",
        "Caesarstone\nBy Imperial",
        ["bench_tops"],
    )

    assert row["display_lines"] == [
        "*Caesarstone / By Imperial* - 2Omm Stone - 4030 Oyster - PR 20mm Shadowline under Benchtop -Forage Smooth",
    ]


def test_legacy_multiline_row_does_not_gain_display_lines_or_new_summary_entries():
    legacy_row = {
        "area_or_item": "CABINETRY COLOUR",
        "supplier": "Polytec",
        "specs_or_description": "Amaro Matt\nBacks only - Forage Smooth",
        "notes": "",
        "tags": ["door_colours"],
        "page_no": 1,
        "row_order": 1,
        "provenance": {"source_provider": "legacy"},
    }

    assert "display_lines" not in legacy_row
    assert _bucket_texts(_summary_for_rows([legacy_row]), "door_colours") == ["Polytec - Amaro Matt"]
    assert "display_lines" not in legacy_row


def test_v6_single_line_display_lines_emit_one_summary_entry():
    row = _v6_material_row("BASE AND UPPER (INCL BOTTOMS) CABINETRY COLOUR", "Amaro Matt", "Polytec", ["door_colours"])

    assert row["display_lines"] == ["Polytec - Amaro Matt"]
    assert _bucket_texts(_summary_for_rows([row]), "door_colours") == ["Polytec - Amaro Matt"]


def test_v6_single_display_line_handle_row_falls_through_to_existing_logic():
    row = _v6_material_row("HANDLES", "Finger Pull on Uppers- PTO where required", "Kethy", ["handles"])

    assert row["display_lines"] == ["Kethy - Finger Pull on Uppers- PTO where required"]
    assert app_main._imperial_material_row_handle_summary_candidates(row) == ["Finger Pull on Uppers"]


def test_v6_single_supplier_merges_blank_line_into_pipe_separator():
    row = _v6_material_row("OPEN SHELVES", "line one\nline two\n\nline three", "Polytec", ["door_colours"])

    assert row["display_lines"] == ["Polytec - line one line two | line three"]


def test_v6_supplier_count_mismatch_emits_single_hinted_display_line():
    row = _v6_material_row(
        "DRAWERS",
        "line one\nline two\nline three\nline four\nline five",
        "Supplier1\nSupplier2",
        ["door_colours"],
    )

    assert row["display_lines"] == ["*Supplier1 / Supplier2* - line one line two line three line four line five"]


def test_v6_empty_supplier_and_specs_do_not_add_display_lines():
    row = _v6_material_row("DRAWERS", "", "", ["door_colours"])

    assert "display_lines" not in row


def test_v6_handles_with_matching_supplier_count_pair_one_to_one():
    row = _v6_material_row(
        "HANDLES - BASE",
        "line one\nline two\nline three\nline four",
        "Supplier1\nSupplier2\nSupplier3\nSupplier4",
        ["handles"],
    )

    assert row["display_lines"] == [
        "Supplier1 - line one",
        "Supplier2 - line two",
        "Supplier3 - line three",
        "Supplier4 - line four",
    ]
