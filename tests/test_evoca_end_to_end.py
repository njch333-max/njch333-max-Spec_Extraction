from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest

from App import main
from App.services import evoca_structured_extractor, parsing
from App.services.evoca_structured_adapter import build_evoca_snapshot_from_structured


VALIDATED_PDF_IDS = (
    "EVOC447",
    "EVOC467",
    "EVOC473",
    "EVOC471",
    "EVOC482",
    "EVOC436",
    "EVOC449",
    "EVOC479",
    "EVOC480",
)

INCLUDED_SECTION_CODES = {"15", "17", "20", "23", "24", "25"}
SKIPPED_SECTION_CODES = {"16", "18", "19", "21", "22"}
SOURCE_DOCUMENT_TEXT_KEYS = {"pages", "text", "raw_text", "unstructured_pages"}

HARD_EXCLUSION_PHRASES = (
    "16 ELECTRICAL",
    "ALARM SYSTEM",
    "CCTV",
    "SOLAR PV",
    "18 AIR-CONDITIONING",
    "19 PLUMBING & GAS",
    "21 MIRRORS",
    "22 WINDOW FURNISHINGS",
    "Daiken",
    "Paltec",
    "LPG Gas",
    "Natural Gas",
    "To Width of Vanity",
)

EXPECTED_SNAPSHOT_CONTAINS: dict[str, tuple[str, ...]] = {
    "EVOC447": (
        "Alder 54082 Brushed Nickel",
        "Alora Gloss White Wall Faced Toilet Suite",
        "Vito Bertoni Alfie Pull-Out Sink Mixer",
    ),
    "EVOC449": (
        "Burazzo 750mm Stainless Steel Double Bowl Sink (BU754522D) ($185)",
        "Undermount",
    ),
    "EVOC473": (
        "Spin Gun Metal Tall Basin Mixer",
        "SP110-GM",
        "Polar",
    ),
    "EVOC482": (
        "Omega Square Bench Mount Gloss White (FL238-W) with Overflow",
        "Not Applicable",
    ),
}

EXPECTED_DISPLAY_CONTAINS: dict[str, tuple[str, ...]] = {
    "EVOC449": (
        "Burazzo 750mm Stainless Steel Double Bowl Sink (BU754522D) ($185) - Undermount",
        "Zara Chrome Pull-Out (ZA120-CH)",
    ),
    "EVOC471": (
        "Finger Pulls to Kitchen",
        "Finger Pulls to WIP",
    ),
    "EVOC482": (
        "Omega Square Bench Mount Gloss White (FL238-W) with Overflow - Overmount",
    ),
}

EXPECTED_ROOM_FIELD_EQUALS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "EVOC482": (("Bathroom", "bench_tops_other", ""),),
}


def _pdf_fixture_dir() -> Path:
    configured = os.environ.get("EVOCA_E2E_PDF_DIR", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).parent / "fixtures" / "evoca_real_pdfs"


def _pdf_path(pdf_id: str) -> Path:
    fixture_dir = _pdf_fixture_dir()
    matches = sorted(fixture_dir.rglob(f"*{pdf_id}*.pdf")) if fixture_dir.exists() else []
    if not matches:
        pytest.skip(
            f"{pdf_id}: private real-PDF fixture not found under {fixture_dir}. "
            "Set EVOCA_E2E_PDF_DIR to run the real Evoca PDF e2e suite."
        )
    if len(matches) > 1:
        pytest.fail(f"{pdf_id}: expected one private PDF fixture, found {matches}")
    return matches[0]


@lru_cache(maxsize=None)
def _build_snapshot_dict(pdf_id: str, pdf_path_text: str) -> dict[str, Any]:
    pdf_path = Path(pdf_path_text)
    structured = evoca_structured_extractor.extract_evoca_pdf(pdf_path)
    source_pages = parsing.load_document_pages(pdf_path)
    snapshot = build_evoca_snapshot_from_structured(
        structured,
        job_no=pdf_id,
        source_document={
            "file_name": pdf_path.name,
            "path": str(pdf_path),
            "role": "spec",
            "pages": source_pages,
        },
    )
    return {
        "structured": structured,
        "snapshot": snapshot.model_dump(mode="json"),
    }


def _section_code(section: dict[str, Any]) -> str:
    code = str(section.get("section_code") or "").strip()
    if code:
        return code
    title = str(section.get("section_title") or "").strip()
    first = title.split(maxsplit=1)[0] if title else ""
    return first if first.isdigit() else ""


def _business_values(structured: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for section in structured.get("sections", []):
        if not isinstance(section, dict):
            continue
        section_code = _section_code(section)
        if section_code in SKIPPED_SECTION_CODES or section_code not in INCLUDED_SECTION_CODES:
            continue
        for container_name in ("rooms", "groups"):
            for container in section.get(container_name, []) or []:
                groups = container.get("groups", []) if isinstance(container, dict) and container_name == "rooms" else [container]
                for group in groups or []:
                    if not isinstance(group, dict):
                        continue
                    for row in group.get("rows", []) or []:
                        if isinstance(row, dict) and not row.get("is_diagnostic") and row.get("value"):
                            values.add(str(row["value"]))
    return values


def _collect_strings(value: Any) -> set[str]:
    strings: set[str] = set()
    if isinstance(value, str):
        if value:
            strings.add(value)
        return strings
    if isinstance(value, dict):
        for key, item in value.items():
            strings.update(_collect_strings(key))
            strings.update(_collect_strings(item))
        return strings
    if isinstance(value, list | tuple | set):
        for item in value:
            strings.update(_collect_strings(item))
    return strings


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _source_document_text_key_paths(value: Any, prefix: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if str(key) in SOURCE_DOCUMENT_TEXT_KEYS:
                paths.append(path)
            paths.extend(_source_document_text_key_paths(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            paths.extend(_source_document_text_key_paths(item, f"{prefix}[{index}]"))
    return paths


def _hard_exclusion_leaks(snapshot_text: str) -> list[str]:
    return [phrase for phrase in HARD_EXCLUSION_PHRASES if phrase in snapshot_text]


def _room_key(value: Any) -> str:
    return parsing.normalize_room_key(str(value or ""))


def _flattened_room(flattened_rooms: list[dict[str, Any]], room_label: str) -> dict[str, Any]:
    wanted = _room_key(room_label)
    for room in flattened_rooms:
        if _room_key(room.get("room_key")) == wanted or _room_key(room.get("original_room_label")) == wanted:
            return room
    pytest.fail(f"Room {room_label!r} not found in flattened rooms: {[room.get('original_room_label') for room in flattened_rooms]}")


@pytest.mark.parametrize("pdf_id", VALIDATED_PDF_IDS)
def test_evoca_real_pdf_extracts_to_snapshot_without_leaks(pdf_id: str) -> None:
    pdf_path = _pdf_path(pdf_id)
    result = _build_snapshot_dict(pdf_id, str(pdf_path))
    structured = result["structured"]
    snapshot = result["snapshot"]
    snapshot_text = _json_text(snapshot)

    assert structured["schema_version"] == "evoca_structured_v0"
    assert snapshot["analysis"]["parser_strategy"] == "evoca_structured_v0"
    assert snapshot["rooms"], f"{pdf_id}: structured PDF produced no retained rooms"
    assert not _hard_exclusion_leaks(snapshot_text)

    for source_document in snapshot.get("source_documents", []):
        assert not _source_document_text_key_paths(source_document)

    missing = _business_values(structured) - _collect_strings(snapshot)
    assert not missing, f"{pdf_id}: source business values missing from SnapshotPayload: {sorted(missing)[:10]}"

    for expected in EXPECTED_SNAPSHOT_CONTAINS.get(pdf_id, ()):
        assert expected in snapshot_text


@pytest.mark.parametrize("pdf_id", sorted(set(EXPECTED_DISPLAY_CONTAINS) | set(EXPECTED_ROOM_FIELD_EQUALS)))
def test_evoca_real_pdf_display_contract(pdf_id: str) -> None:
    pdf_path = _pdf_path(pdf_id)
    snapshot = _build_snapshot_dict(pdf_id, str(pdf_path))["snapshot"]
    flattened_rooms = main._flatten_rooms(snapshot)
    display_text = _json_text(flattened_rooms)

    assert not _hard_exclusion_leaks(display_text)
    for expected in EXPECTED_DISPLAY_CONTAINS.get(pdf_id, ()):
        assert expected in display_text

    for room_label, field_name, expected_value in EXPECTED_ROOM_FIELD_EQUALS.get(pdf_id, ()):
        room = _flattened_room(flattened_rooms, room_label)
        assert room.get(field_name, "") == expected_value
