from __future__ import annotations

from copy import deepcopy

from App.services.parsing import _imperial_finalize_material_rows, _imperial_finalize_material_rows_v6


def _job76_shared_finish_rows() -> list[dict]:
    return [
        {
            "area_or_item": "OPEN UPPER CABINETRY COLOUR (INCL\nBOTTOMS)",
            "supplier": "Polytec",
            "specs_or_description": "Surround - Prime Oak Matt Backs only - Forage Smooth",
            "notes": "",
            "tags": ["door_colours"],
            "page_no": 1,
            "row_order": 3,
            "provenance": {"source_provider": "v6", "source_extractor": "pdf_to_structured_json_v6"},
        },
        {
            "area_or_item": "OPEN LOWER CABINETRY COLOUR",
            "supplier": "Polytec",
            "specs_or_description": "Surround - Prime Oak Matt Backs only - Forage Smooth",
            "notes": "",
            "tags": ["door_colours"],
            "page_no": 1,
            "row_order": 4,
            "provenance": {"source_provider": "v6", "source_extractor": "pdf_to_structured_json_v6"},
        },
    ]


def test_v6_material_finalize_keeps_distinct_same_spec_rows():
    result = _imperial_finalize_material_rows_v6(deepcopy(_job76_shared_finish_rows()))

    assert len(result) == 2
    assert [row["area_or_item"] for row in result] == [
        "OPEN UPPER CABINETRY COLOUR (INCL\nBOTTOMS)",
        "OPEN LOWER CABINETRY COLOUR",
    ]
    assert all(row.get("provenance", {}).get("merged_duplicate") is not True for row in result)


def test_legacy_material_finalize_still_merges_same_spec_rows():
    result = _imperial_finalize_material_rows(deepcopy(_job76_shared_finish_rows()))

    assert len(result) == 1
    assert result[0].get("provenance", {}).get("merged_duplicate") is True
