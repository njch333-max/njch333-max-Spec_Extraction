from __future__ import annotations

from copy import deepcopy

from App.services.pdf_to_structured_json import _split_review_hint_record


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
