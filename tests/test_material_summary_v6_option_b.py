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
    return _summary_for_rooms(
        [
            {
                "room_key": "kitchen",
                "original_room_label": "KITCHEN",
                "room_order": 1,
                "material_rows": deepcopy(rows),
            }
        ]
    )


def _summary_for_rooms(rooms: list[dict]) -> dict:
    return app_main._build_imperial_material_summary(
        {
            "builder_name": "Imperial",
            "rooms": deepcopy(rooms),
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

    assert row["display_groups"] == [
        {
            "supplier": "Kethy",
            "lines": [
                "Finger Pull on Uppers- PTO where required",
                "L7817 - Oak Matt Black (OAKBK)",
                "160mm - Lowers and Drawers",
                "320mm - Pantry Door",
            ],
        }
    ]
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

    assert row["display_groups"] == [
        {"supplier": "Supplier1", "lines": ["line one"]},
        {"supplier": "Supplier2", "lines": ["line two"]},
        {"supplier": "Supplier3", "lines": ["line three"]},
        {"supplier": "Supplier4", "lines": ["line four"]},
    ]
    assert row["display_lines"] == [
        "Supplier1 - line one",
        "Supplier2 - line two",
        "Supplier3 - line three",
        "Supplier4 - line four",
    ]


def test_v6_handles_equal_share_groups_multi_supplier_block():
    row = _v6_material_row(
        "HANDLES",
        (
            "Tall Door Handles - Momo Hinoki Wood Big D\n"
            "832mm Handle Oak-HIN0682.832.OAK\n"
            "High Split Handle -Momo hinoki wood big d\n"
            "416mm handle oak-HIN0682.416.OAK\n"
            "Drawers - Bevel Edge finger pull\n"
            "DESK - 2163 Voda Profile Handle Brushed\n"
            "Nickel 300mm - SO-2163-300-BN\n"
            "BENCHSEAT DRAWERS - PTO"
        ),
        "Furnware\nTitus Tekform",
        ["handles"],
    )

    assert row["display_groups"] == [
        {
            "supplier": "Furnware",
            "lines": [
                "Tall Door Handles - Momo Hinoki Wood Big D",
                "832mm Handle Oak-HIN0682.832.OAK",
                "High Split Handle -Momo hinoki wood big d",
                "416mm handle oak-HIN0682.416.OAK",
            ],
        },
        {
            "supplier": "Titus Tekform",
            "lines": [
                "Drawers - Bevel Edge finger pull",
                "DESK - 2163 Voda Profile Handle Brushed",
                "Nickel 300mm - SO-2163-300-BN",
                "BENCHSEAT DRAWERS - PTO",
            ],
        },
    ]
    assert row["display_lines"] == [
        "Furnware - Tall Door Handles - Momo Hinoki Wood Big D",
        "Furnware - 832mm Handle Oak-HIN0682.832.OAK",
        "Furnware - High Split Handle -Momo hinoki wood big d",
        "Furnware - 416mm handle oak-HIN0682.416.OAK",
        "Titus Tekform - Drawers - Bevel Edge finger pull",
        "Titus Tekform - DESK - 2163 Voda Profile Handle Brushed",
        "Titus Tekform - Nickel 300mm - SO-2163-300-BN",
        "Titus Tekform - BENCHSEAT DRAWERS - PTO",
    ]


def test_v6_handles_mismatch_keeps_flat_fallback_without_display_groups():
    row = _v6_material_row(
        "HANDLES",
        "line one\nline two\nline three\nline four\nline five",
        "Supplier1\nSupplier2",
        ["handles"],
    )

    assert "display_groups" not in row
    assert row["display_lines"] == [
        "Supplier1\nSupplier2 - line one",
        "Supplier1\nSupplier2 - line two",
        "Supplier1\nSupplier2 - line three",
        "Supplier1\nSupplier2 - line four",
        "Supplier1\nSupplier2 - line five",
    ]


def test_v6_handles_empty_specs_do_not_emit_display_groups_or_lines():
    row = _v6_material_row("HANDLES", "", "Kethy", ["handles"])

    assert "display_groups" not in row
    assert "display_lines" not in row


def test_v6_non_handles_do_not_emit_display_groups():
    row = _v6_material_row(
        "BENCHTOP",
        "line one\nline two",
        "Supplier1\nSupplier2",
        ["bench_tops"],
    )

    assert "display_groups" not in row


def test_v6_grouped_handles_summary_uses_single_kethy_group_entry():
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

    summary = _summary_for_rows([row])

    assert summary["handles"]["count"] == 1
    assert summary["handles"]["entries"] == [
        {
            "text": "Kethy",
            "display_text": "Kethy",
            "lines": [
                "Finger Pull on Uppers- PTO where required",
                "L7817 - Oak Matt Black (OAKBK)",
                "160mm - Lowers and Drawers",
                "320mm - Pantry Door",
            ],
            "rooms": ["KITCHEN"],
            "rooms_display": "KITCHEN",
            "area_or_items": ["HANDLES"],
        }
    ]


def test_v6_grouped_handles_summary_emits_two_astrid_supplier_groups():
    row = _v6_material_row(
        "HANDLES",
        (
            "Tall Door Handles - Momo Hinoki Wood Big D\n"
            "832mm Handle Oak-HIN0682.832.OAK\n"
            "High Split Handle -Momo hinoki wood big d\n"
            "416mm handle oak-HIN0682.416.OAK\n"
            "Drawers - Bevel Edge finger pull\n"
            "DESK - 2163 Voda Profile Handle Brushed\n"
            "Nickel 300mm - SO-2163-300-BN\n"
            "BENCHSEAT DRAWERS - PTO"
        ),
        "Furnware\nTitus Tekform",
        ["handles"],
    )
    summary = _summary_for_rooms(
        [
            {
                "room_key": "upper_bed_3_astrid",
                "original_room_label": "UPPER-BED 3 (Astrid)",
                "room_order": 1,
                "material_rows": [row],
            }
        ]
    )

    assert summary["handles"]["count"] == 2
    assert summary["handles"]["entries"] == [
        {
            "text": "Furnware",
            "display_text": "Furnware",
            "lines": [
                "Tall Door Handles - Momo Hinoki Wood Big D",
                "832mm Handle Oak-HIN0682.832.OAK",
                "High Split Handle -Momo hinoki wood big d",
                "416mm handle oak-HIN0682.416.OAK",
            ],
            "rooms": ["UPPER-BED 3 (Astrid)"],
            "rooms_display": "UPPER-BED 3 (Astrid)",
            "area_or_items": ["HANDLES"],
        },
        {
            "text": "Titus Tekform",
            "display_text": "Titus Tekform",
            "lines": [
                "Drawers - Bevel Edge finger pull",
                "DESK - 2163 Voda Profile Handle Brushed",
                "Nickel 300mm - SO-2163-300-BN",
                "BENCHSEAT DRAWERS - PTO",
            ],
            "rooms": ["UPPER-BED 3 (Astrid)"],
            "rooms_display": "UPPER-BED 3 (Astrid)",
            "area_or_items": ["HANDLES"],
        },
    ]


def test_v6_grouped_handles_summary_dedupes_identical_groups_across_rooms():
    row = _v6_material_row(
        "HANDLES",
        "Finger Pull on Uppers- PTO where required\nL7817 - Oak Matt Black (OAKBK)",
        "Kethy",
        ["handles"],
    )
    summary = _summary_for_rooms(
        [
            {
                "room_key": "kitchen",
                "original_room_label": "KITCHEN",
                "room_order": 1,
                "material_rows": [deepcopy(row)],
            },
            {
                "room_key": "lower_linen",
                "original_room_label": "LOWER LINEN",
                "room_order": 2,
                "material_rows": [deepcopy(row)],
            },
        ]
    )

    assert summary["handles"]["count"] == 1
    assert summary["handles"]["entries"][0]["display_text"] == "Kethy"
    assert summary["handles"]["entries"][0]["lines"] == [
        "Finger Pull on Uppers- PTO where required",
        "L7817 - Oak Matt Black (OAKBK)",
    ]
    assert summary["handles"]["entries"][0]["rooms_display"] == "KITCHEN | LOWER LINEN"


def test_v6_grouped_handles_summary_falls_back_to_flat_entries_without_display_groups():
    row = _v6_material_row(
        "HANDLES",
        "line one\nline two\nline three\nline four\nline five",
        "Supplier1\nSupplier2",
        ["handles"],
    )

    summary = _summary_for_rows([row])

    assert summary["handles"]["count"] > 0
    assert all("lines" not in entry for entry in summary["handles"]["entries"])


def test_v6_grouped_handles_summary_keeps_door_colours_flat():
    row = _v6_material_row("BASE AND UPPER (INCL BOTTOMS) CABINETRY COLOUR", "Amaro Matt", "Polytec", ["door_colours"])

    summary = _summary_for_rows([row])

    assert summary["door_colours"]["count"] == 1
    assert summary["door_colours"]["entries"] == [
        {
            "text": "Polytec - Amaro Matt",
            "display_text": "Polytec - Amaro Matt",
            "rooms": ["KITCHEN"],
            "rooms_display": "KITCHEN",
            "area_or_items": ["BASE AND UPPER (INCL BOTTOMS) CABINETRY COLOUR"],
        }
    ]


def test_v6_grouped_handles_summary_keeps_bench_tops_flat():
    row = _v6_material_row(
        "BENCHTOP",
        "2Omm Stone - 4030 Oyster - PR\n20mm Shadowline under Benchtop -Forage Smooth",
        "Caesarstone\nBy Imperial",
        ["bench_tops"],
    )

    summary = _summary_for_rows([row])

    assert summary["bench_tops"]["count"] == 2
    assert summary["bench_tops"]["entries"] == [
        {
            "text": "Caesarstone - 2Omm Stone - 4030 Oyster - PR",
            "display_text": "Caesarstone - 2Omm Stone - 4030 Oyster - PR",
            "rooms": ["KITCHEN"],
            "rooms_display": "KITCHEN",
            "area_or_items": ["BENCHTOP"],
        },
        {
            "text": "By Imperial - 20mm Shadowline under Benchtop -Forage Smooth",
            "display_text": "By Imperial - 20mm Shadowline under Benchtop -Forage Smooth",
            "rooms": ["KITCHEN"],
            "rooms_display": "KITCHEN",
            "area_or_items": ["BENCHTOP"],
        },
    ]
