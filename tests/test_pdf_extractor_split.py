from __future__ import annotations

from copy import deepcopy

from App.services.pdf_to_structured_json import (
    _coalesce_single_area_multisupplier_specs,
    _repair_wrapped_cabinetry_colour_record,
    _should_add_missing_row_separator_review_hint,
    _split_review_hint_record,
    assign_word_to_column,
)


def _job76_pullout_merged_record(supplier: str = "Imperial\nHettich\nFurnware\nFurnware") -> dict:
    return {
        "area": "LED LIGHTING\nPULL-OUT BIN\nPULL-OUT SHELVES\nPULL-OUT CORNER SHELVES",
        "specs": (
            "LED Provision ONLY to underside of OHC\n"
            "9291592 Waste Bin PO - 400mm 2x32Ltrs\n"
            "VSDSA.200.SSL.FG - VS Sub 200mm Wire\n"
            "ST22MCU.450L.CPWH - Elka Magic Corner"
        ),
        "supplier": supplier,
        "notes": "Incl Internal of Open Upper Cabinet",
        "image": "Location: Rear back",
        "_review_hint": (
            "AREA contains multiple line items and SPECS has matching line count. "
            "Source PDF may be missing a row separator."
        ),
        "_source": {"page": 1, "row_index": 8, "method": "grid"},
    }


def test_split_review_hint_record_splits_job76_pullout_block():
    result = _split_review_hint_record(deepcopy(_job76_pullout_merged_record()), "grid_split")

    assert len(result) == 4
    assert [row["area"] for row in result] == [
        "LED LIGHTING",
        "PULL-OUT BIN",
        "PULL-OUT SHELVES",
        "PULL-OUT CORNER SHELVES",
    ]
    assert [row["specs"] for row in result] == [
        "LED Provision ONLY to underside of OHC",
        "9291592 Waste Bin PO - 400mm 2x32Ltrs",
        "VSDSA.200.SSL.FG - VS Sub 200mm Wire",
        "ST22MCU.450L.CPWH - Elka Magic Corner",
    ]
    assert [row["supplier"] for row in result] == ["Imperial", "Hettich", "Furnware", "Furnware"]
    assert [row["notes"] for row in result] == ["Incl Internal of Open Upper Cabinet", "", "", ""]
    assert [row["image"] for row in result] == ["Location: Rear back", "", "", ""]
    assert all(row["_split_from_review_hint"] is True for row in result)
    assert all("_review_hint" not in row for row in result)
    assert [row["_source"]["row_index"] for row in result] == ["8.0", "8.1", "8.2", "8.3"]
    assert all(row["_source"]["method"] == "grid_split" for row in result)


def test_split_review_hint_record_keeps_mismatched_lines_unchanged():
    record = _job76_pullout_merged_record()
    record["area"] = "LED LIGHTING\nPULL-OUT BIN\nPULL-OUT SHELVES"
    record["specs"] = "LED Provision ONLY to underside of OHC\n9291592 Waste Bin PO - 400mm 2x32Ltrs"

    result = _split_review_hint_record(record, "grid_split")

    assert result == [record]
    assert result[0]["_review_hint"]


def test_split_review_hint_record_reuses_full_supplier_when_supplier_count_mismatches():
    record = _job76_pullout_merged_record(supplier="Imperial")

    result = _split_review_hint_record(deepcopy(record), "grid_split")

    assert len(result) == 4
    assert [row["supplier"] for row in result] == ["Imperial", "Imperial", "Imperial", "Imperial"]
    assert all(row["_split_from_review_hint"] is True for row in result)


def test_split_review_hint_record_ignores_blank_lines_when_splitting():
    record = _job76_pullout_merged_record()
    record["area"] = "A\n\nB\n\nC"
    record["specs"] = "spec a\n\nspec b\n\nspec c"
    record["supplier"] = "Supplier A\n\nSupplier B\n\nSupplier C"

    result = _split_review_hint_record(deepcopy(record), "grid_split")

    assert len(result) == 3
    assert [row["area"] for row in result] == ["A", "B", "C"]
    assert [row["specs"] for row in result] == ["spec a", "spec b", "spec c"]
    assert [row["supplier"] for row in result] == ["Supplier A", "Supplier B", "Supplier C"]


def test_split_review_hint_record_keeps_wrapped_base_cabinetry_colour_label():
    record = {
        "area": "BASE CABINETRY COLOUR\nINCLUDES SINGLE CABINET ON BAR\nBACK AREA",
        "specs": "Polytec\nClassic White\nMatt",
        "supplier": "Polytec",
        "_review_hint": (
            "AREA contains multiple line items and SPECS has matching line count. "
            "Source PDF may be missing a row separator."
        ),
        "_source": {"page": 2, "row_index": "row_1", "method": "template_anchor"},
    }

    result = _split_review_hint_record(deepcopy(record), "text_split")

    assert len(result) == 1
    assert result[0]["area"] == "BASE CABINETRY COLOUR\nINCLUDES SINGLE CABINET ON BAR\nBACK AREA"
    assert result[0]["specs"] == "Polytec\nClassic White\nMatt"
    assert result[0]["supplier"] == "Polytec"


def test_repair_wrapped_base_cabinetry_colour_record_moves_tail_to_notes():
    record = {
        "area": "BASE CABINETRY COLOUR\nINCLUDES SINGLE CABINET ON BAR\nBACK AREA",
        "specs": "Polytec\nClassic White\nMatt",
        "supplier": "Polytec",
        "notes": "",
    }

    result = _repair_wrapped_cabinetry_colour_record(record)

    assert result["area"] == "BASE CABINETRY COLOUR"
    assert result["specs"] == "Classic White\nMatt"
    assert result["supplier"] == "Polytec"
    assert result["notes"] == "INCLUDES SINGLE CABINET ON BAR BACK AREA"


def test_missing_row_separator_gate_counts_only_non_empty_lines():
    assert _should_add_missing_row_separator_review_hint("A\n\nB", "spec a\n\nspec b") is False


def test_missing_row_separator_gate_ignores_wrapped_base_cabinetry_colour_label():
    assert _should_add_missing_row_separator_review_hint(
        "BASE CABINETRY COLOUR\nINCLUDES SINGLE CABINET ON BAR\nBACK AREA",
        "Polytec\nClassic White\nMatt",
    ) is False


def test_coalesce_single_area_multisupplier_specs_merges_soft_wrap_overflow():
    record = {
        "area": "BENCHTOP",
        "specs": "2Omm Stone - 4030 Oyster - PR\n20mm Shadowline under Benchtop -Forage\nSmooth",
        "supplier": "Caesarstone\nBy Imperial",
    }

    result = _coalesce_single_area_multisupplier_specs(record)

    assert result["specs"] == "2Omm Stone - 4030 Oyster - PR\n20mm Shadowline under Benchtop -Forage Smooth"


def test_assign_word_to_column_keeps_area_overflow_gap_word():
    template = [
        ("area", 52, 200),
        ("specs", 258, 464),
        ("supplier", 722, 929),
    ]
    word = {"x0": 218.5, "x1": 238.8}

    assert assign_word_to_column(word, template) == "area"
