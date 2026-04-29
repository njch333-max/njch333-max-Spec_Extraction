from __future__ import annotations

from pathlib import Path

from openpyxl import load_workbook

from App.services import evoca_structured_extractor


def _page(page_no: int, rows: list[list[object]]) -> dict[str, object]:
    return {
        "page_no": page_no,
        "raw_text": " ".join(str(cell) for row in rows for cell in row if cell),
        "text": "",
        "table_rows": [rows],
    }


def test_evoca_structured_extracts_section_room_group_rows() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Kitchen", None, None],
                    ["-", "Benchtops\nManufacturer\nColour", "Quantum Quartz\nChampagne", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )

    section = structured["sections"][0]
    assert section["section_title"] == "15 CABINETS"
    room = section["rooms"][0]
    assert room["room_label"] == "Kitchen"
    group = room["groups"][0]
    assert group["group_label"] == "Benchtops"
    assert [(row["label"], row["value"]) for row in group["rows"]] == [
        ("Manufacturer", "Quantum Quartz"),
        ("Colour", "Champagne"),
    ]


def test_evoca_structured_preserves_not_applicable_rows() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Butlers", None, None],
                    ["-", "Underbench\nManufacturer\nColour & Finish\nHandles", "Not Applicable", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )

    rows = structured["sections"][0]["rooms"][0]["groups"][0]["rows"]
    assert any(row["value"] == "Not Applicable" for row in rows)


def test_evoca_structured_carries_section_across_pages() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Kitchen", None, None],
                    ["-", "Benchtops\nManufacturer", "Quantum Quartz", None],
                ],
            ),
            _page(
                2,
                [
                    ["", "Laundry", None, None],
                    ["-", "Benchtops\nManufacturer", "Quantum Quartz", None],
                ],
            ),
        ],
        source_pdf="evoca.pdf",
    )

    assert len(structured["sections"]) == 1
    section = structured["sections"][0]
    assert section["page_start"] == 1
    assert section["page_end"] == 2
    assert [room["room_label"] for room in section["rooms"]] == ["Kitchen", "Laundry"]


def test_evoca_structured_detects_section_level_appliances() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["17 APPLIANCES, ACCESSORIES & HOT WATER UNIT", None, None, ""],
                    ["-", "Appliances\nFreestanding Cooker\nHot Plate", "Not Applicable", None],
                    [None, None, "Fisher & Paykel 900mm Induction Cooktop", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )

    section = structured["sections"][0]
    assert section["section_title"] == "17 APPLIANCES, ACCESSORIES & HOT WATER UNIT"
    assert section["rooms"] == []
    group = section["groups"][0]
    assert group["group_label"] == "Appliances"
    assert [row["label"] for row in group["rows"]] == ["Freestanding Cooker", "Hot Plate"]


def test_evoca_structured_workbook_exports_expected_columns(tmp_path: Path) -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["20 PLUMBING FIXTURES & TAPWARE", None, None, ""],
                    ["", "Kitchen", None, None],
                    ["-", "Sink\nModel\nType", "", None],
                    [None, None, "Burazzo Sink", None],
                    [None, None, "Top Mount", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )
    output = tmp_path / "evoca.xlsx"
    evoca_structured_extractor.write_structured_workbook(structured, output)

    workbook = load_workbook(output)
    assert workbook.sheetnames[0] == "_summary"
    assert "20_PLUMBING" in workbook.sheetnames
    sheet = workbook["20_PLUMBING"]
    headers = [cell.value for cell in sheet[1]]
    assert headers == ["Page", "Order", "Room", "Group", "Label", "Value", "Anchor", "Source Text"]
    values = [row[5].value for row in sheet.iter_rows(min_row=2)]
    assert "Burazzo Sink" in values
