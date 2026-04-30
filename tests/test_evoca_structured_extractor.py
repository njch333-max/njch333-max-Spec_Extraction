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


def _raw_line(top: float, words: list[tuple[str, float]]) -> dict[str, object]:
    return {
        "top": top,
        "bottom": top + 9,
        "center": top + 4.5,
        "text": " ".join(word for word, _ in words),
        "words": [
            {
                "text": word,
                "x0": x0,
                "x1": x0 + max(len(word), 1) * 5,
                "top": top,
                "bottom": top + 9,
                "center": top + 4.5,
            }
            for word, x0 in words
        ],
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


def test_evoca_structured_promotes_group_anchor_value_before_child_rows() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Kitchen", None, None],
                    [
                        "",
                        "Overhead Cupboards\nManufacturer\nColour & Finish\nHandles",
                        "Push to Open Above Oven\nPolytec",
                        None,
                    ],
                    [None, None, "Rojo Walnut Woodmatt", None],
                    [None, None, "Finger Grip", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )

    evoca_structured_extractor._rescue_missing_values(
        structured,
        {
            1: {
                "overhead cupboards": ["Push to Open Above Oven"],
                "manufacturer": ["Polytec"],
                "colour & finish": ["Rojo Walnut Woodmatt"],
                "handles": ["Finger Grip"],
            }
        },
    )

    group = structured["sections"][0]["rooms"][0]["groups"][0]
    assert [(row["label"], row["value"], bool(row.get("is_group_anchor"))) for row in group["rows"]] == [
        ("Overhead Cupboards", "Push to Open Above Oven", True),
        ("Manufacturer", "Polytec", False),
        ("Colour & Finish", "Rojo Walnut Woodmatt", False),
        ("Handles", "Finger Grip", False),
    ]
    assert structured["diagnostics"]["anchor_value_groups"] == 1
    assert structured["diagnostics"]["anchor_value_child_realignments"] == 3


def test_evoca_structured_keeps_room_note_before_next_group() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Kitchen", None, None],
                    ["", "No shelf to cupboard underneath sink", None, None],
                    [
                        "-",
                        "Benchtops\nManufacturer\nColour\nIsland Colour\nEdge Profile",
                        "Quantum Quartz\nStatuario Zero\nAs Above\n20mm Arissed",
                        None,
                    ],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )

    room = structured["sections"][0]["rooms"][0]
    assert [(note["label"], note["value"]) for note in room["notes"]] == [
        ("Note", "No shelf to cupboard underneath sink")
    ]
    assert [group["group_label"] for group in room["groups"]] == ["Benchtops"]
    rows = room["groups"][0]["rows"]
    assert [(row["label"], row["value"]) for row in rows[:2]] == [
        ("Manufacturer", "Quantum Quartz"),
        ("Colour", "Statuario Zero"),
    ]


def test_evoca_structured_rescue_is_bounded_to_matching_group_block() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["20 PLUMBING FIXTURES & TAPWARE", None, None, ""],
                    ["", "Bathroom", None, None],
                    ["-", "Basin\nModel\nType", "", None],
                    [None, None, "Eden Bench Mount Gloss White (FL135-W)", None],
                    [None, None, "Overmount", None],
                    ["-", "Basin Mixer\nType\nLocation", "", None],
                    ["", "Ensuite", None, None],
                    ["-", "Basin\nModel\nType", "", None],
                    [None, None, "Eden Bench Mount Gloss White (FL135-W)", None],
                    [None, None, "Overmount", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )
    entry_key = evoca_structured_extractor.LOOKUP_ENTRIES_KEY
    evoca_structured_extractor._rescue_missing_values(
        structured,
        {
            1: {
                entry_key: [
                    {"key": "bathroom", "label": "Bathroom", "value": ""},
                    {"key": "basin", "label": "Basin", "value": ""},
                    {"key": "model", "label": "Model", "value": "Eden Bench Mount Gloss White (FL135-W)"},
                    {"key": "type", "label": "Type", "value": "Overmount"},
                    {"key": "basin mixer", "label": "Basin Mixer", "value": ""},
                    {"key": "type", "label": "Type", "value": ""},
                    {"key": "location", "label": "Location", "value": "Centre of Basin"},
                    {"key": "ensuite", "label": "Ensuite", "value": ""},
                    {"key": "basin", "label": "Basin", "value": ""},
                    {"key": "model", "label": "Model", "value": "Eden Bench Mount Gloss White (FL135-W)"},
                    {"key": "type", "label": "Type", "value": "Overmount"},
                ],
                "model": ["Eden Bench Mount Gloss White (FL135-W)", "Eden Bench Mount Gloss White (FL135-W)"],
                "type": ["Overmount", "Overmount"],
                "location": ["Centre of Basin"],
            }
        },
    )

    bathroom = structured["sections"][0]["rooms"][0]
    basin_mixer = bathroom["groups"][1]
    assert basin_mixer["group_label"] == "Basin Mixer"
    assert [(row["label"], row["value"]) for row in basin_mixer["rows"]] == [
        ("Type", ""),
        ("Location", "Centre of Basin"),
    ]


def test_evoca_structured_raw_text_fallback_fills_group_bounded_missing_pair() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Kitchen", None, None],
                    ["", "No shelf to cupboard underneath sink", None, None],
                    [
                        "-",
                        "Benchtops\nManufacturer\nColour\nIsland Colour\nEdge Profile",
                        "",
                        None,
                    ],
                    [None, None, "As Above", None],
                    [None, None, "20mm Arissed", None],
                    ["-", "Underbench\nManufacturer\nColour & Finish", "Polytec\nAston White Matte", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )
    entry_key = evoca_structured_extractor.LOOKUP_ENTRIES_KEY
    line_key = evoca_structured_extractor.LOOKUP_LINES_KEY
    evoca_structured_extractor._rescue_missing_values(
        structured,
        {
            1: {
                entry_key: [
                    {"key": "kitchen", "label": "Kitchen", "value": ""},
                    {"key": "benchtops", "label": "Benchtops", "value": ""},
                    {"key": "manufacturer", "label": "Manufacturer", "value": "Quantum Quartz"},
                    {"key": "colour", "label": "Colour", "value": ""},
                    {"key": "island colour", "label": "Island Colour", "value": "As Above"},
                    {"key": "edge profile", "label": "Edge Profile", "value": "20mm Arissed"},
                    {"key": "underbench", "label": "Underbench", "value": ""},
                ],
                line_key: [
                    _raw_line(100, [("Kitchen", 40)]),
                    _raw_line(120, [("-", 30), ("Benchtops", 45)]),
                    _raw_line(135, [("Manufacturer", 70), ("Quantum", 200), ("Quartz", 238)]),
                    _raw_line(150, [("Colour", 70), ("Statuario", 200), ("Zero", 238)]),
                    _raw_line(165, [("Island", 70), ("Colour", 100), ("As", 200), ("Above", 215)]),
                    _raw_line(180, [("Edge", 70), ("Profile", 95), ("20mm", 200), ("Arissed", 228)]),
                    _raw_line(210, [("-", 30), ("Underbench", 45)]),
                ],
                "manufacturer": ["Quantum Quartz"],
                "island colour": ["As Above"],
                "edge profile": ["20mm Arissed"],
            }
        },
    )

    group = structured["sections"][0]["rooms"][0]["groups"][0]
    assert [(row["label"], row["value"], row["source_method"]) for row in group["rows"]] == [
        ("Manufacturer", "Quantum Quartz", "pdfplumber_text_rescue"),
        ("Colour", "Statuario Zero", "pdfplumber_raw_text_fallback"),
        ("Island Colour", "As Above", "pdfplumber_text_rescue"),
        ("Edge Profile", "20mm Arissed", "pdfplumber_text_rescue"),
    ]
    assert structured["diagnostics"]["raw_text_fallback_groups"] == 1
    assert structured["diagnostics"]["raw_text_fallback_pairs_filled"] == 1


def test_evoca_structured_raw_text_fallback_does_not_cross_group_boundary() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["20 PLUMBING FIXTURES & TAPWARE", None, None, ""],
                    ["", "Bathroom", None, None],
                    ["-", "Basin\nModel\nType", "", None],
                    [None, None, "Eden Bench Mount Gloss White (FL135-W)", None],
                    [None, None, "Overmount", None],
                    ["-", "Basin Mixer\nType\nLocation", "", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )
    entry_key = evoca_structured_extractor.LOOKUP_ENTRIES_KEY
    line_key = evoca_structured_extractor.LOOKUP_LINES_KEY
    evoca_structured_extractor._rescue_missing_values(
        structured,
        {
            1: {
                entry_key: [
                    {"key": "bathroom", "label": "Bathroom", "value": ""},
                    {"key": "basin", "label": "Basin", "value": ""},
                    {"key": "model", "label": "Model", "value": "Eden Bench Mount Gloss White (FL135-W)"},
                    {"key": "type", "label": "Type", "value": "Overmount"},
                    {"key": "basin mixer", "label": "Basin Mixer", "value": ""},
                    {"key": "type", "label": "Type", "value": ""},
                    {"key": "location", "label": "Location", "value": "Centre of Basin"},
                ],
                line_key: [
                    _raw_line(100, [("Bathroom", 40)]),
                    _raw_line(120, [("-", 30), ("Basin", 45)]),
                    _raw_line(135, [("Model", 70), ("Eden", 200), ("Bench", 230), ("Mount", 265)]),
                    _raw_line(150, [("Type", 70), ("Overmount", 200)]),
                    _raw_line(180, [("-", 30), ("Basin", 45), ("Mixer", 75)]),
                    _raw_line(195, [("Type", 70)]),
                    _raw_line(210, [("Location", 70), ("Centre", 200), ("of", 235), ("Basin", 250)]),
                ],
                "model": ["Eden Bench Mount Gloss White (FL135-W)"],
                "type": ["Overmount"],
                "location": ["Centre of Basin"],
            }
        },
    )

    bathroom = structured["sections"][0]["rooms"][0]
    basin = bathroom["groups"][0]
    basin_mixer = bathroom["groups"][1]
    assert [(row["label"], row["value"]) for row in basin["rows"]] == [
        ("Model", "Eden Bench Mount Gloss White (FL135-W)"),
        ("Type", "Overmount"),
    ]
    assert [(row["label"], row["value"]) for row in basin_mixer["rows"]] == [
        ("Type", ""),
        ("Location", "Centre of Basin"),
    ]


def test_evoca_structured_raw_text_fallback_uses_label_owned_next_line_value() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["20 PLUMBING FIXTURES & TAPWARE", None, None, ""],
                    ["", "Bathroom", None, None],
                    ["-", "Shower\nMixer\nShower Rail / Rose\nShower Screen", "", None],
                    [None, None, "Semi-frameless with Clear Toughened Glass", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )
    entry_key = evoca_structured_extractor.LOOKUP_ENTRIES_KEY
    line_key = evoca_structured_extractor.LOOKUP_LINES_KEY
    evoca_structured_extractor._rescue_missing_values(
        structured,
        {
            1: {
                entry_key: [
                    {"key": "bathroom", "label": "Bathroom", "value": ""},
                    {"key": "shower", "label": "Shower", "value": ""},
                    {"key": "mixer", "label": "Mixer", "value": ""},
                    {"key": "shower rail / rose", "label": "Shower Rail / Rose", "value": ""},
                    {"key": "shower screen", "label": "Shower Screen", "value": "Semi-frameless with Clear Toughened Glass"},
                ],
                line_key: [
                    _raw_line(100, [("Bathroom", 40)]),
                    _raw_line(120, [("-", 30), ("Shower", 45)]),
                    _raw_line(135, [("Mixer", 70), ("Adler", 200), ("Soho", 230), ("54380", 260)]),
                    _raw_line(150, [("Shower", 70), ("Rail", 105), ("/", 128), ("Rose", 138)]),
                    _raw_line(165, [("Alder", 200), ("Dual", 230), ("Shower", 255), ("Round", 290), ("Rail", 320)]),
                    _raw_line(180, [("Shower", 70), ("Screen", 105), ("Semi-frameless", 200)]),
                ],
                "shower screen": ["Semi-frameless with Clear Toughened Glass"],
            }
        },
    )

    group = structured["sections"][0]["rooms"][0]["groups"][0]
    assert [(row["label"], row["value"], row["source_method"]) for row in group["rows"]] == [
        ("Mixer", "Adler Soho 54380", "pdfplumber_raw_text_fallback"),
        ("Shower Rail / Rose", "Alder Dual Shower Round Rail", "pdfplumber_raw_text_fallback"),
        ("Shower Screen", "Semi-frameless with Clear Toughened Glass", "pdfplumber_text_rescue"),
    ]


def test_evoca_structured_raw_text_fallback_rejects_footer_continuation() -> None:
    structured = evoca_structured_extractor.extract_evoca_pages(
        [
            _page(
                1,
                [
                    ["15 CABINETS", None, None, ""],
                    ["", "Bathroom", None, None],
                    ["-", "Underbench\nHandles\nDrawer Handle", "", None],
                ],
            )
        ],
        source_pdf="evoca.pdf",
    )
    entry_key = evoca_structured_extractor.LOOKUP_ENTRIES_KEY
    line_key = evoca_structured_extractor.LOOKUP_LINES_KEY
    evoca_structured_extractor._rescue_missing_values(
        structured,
        {
            1: {
                entry_key: [
                    {"key": "bathroom", "label": "Bathroom", "value": ""},
                    {"key": "underbench", "label": "Underbench", "value": ""},
                    {"key": "handles", "label": "Handles", "value": "2163 Voda Profile Handle"},
                    {"key": "drawer handle", "label": "Drawer Handle", "value": ""},
                ],
                line_key: [
                    _raw_line(100, [("Bathroom", 40)]),
                    _raw_line(120, [("-", 30), ("Underbench", 45)]),
                    _raw_line(135, [("Handles", 70), ("2163", 200), ("Voda", 230), ("Profile", 260), ("Handle", 300)]),
                    _raw_line(150, [("Drawer", 70), ("Handle", 105)]),
                    _raw_line(700, [("Page", 200), ("9", 230), ("of", 242), ("83", 255), ("Client", 300), ("Initials:______________________", 335)]),
                ],
                "handles": ["2163 Voda Profile Handle"],
            }
        },
    )

    rows = structured["sections"][0]["rooms"][0]["groups"][0]["rows"]
    assert [(row["label"], row["value"]) for row in rows] == [
        ("Handles", "2163 Voda Profile Handle"),
        ("Drawer Handle", ""),
    ]
