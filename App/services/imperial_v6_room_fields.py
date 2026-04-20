"""Populate Imperial v6 RoomRow room-level fields from structured v6 JSON.

This replaces page-text room field scraping for the v6 path by operating on
v6's structured cells: section metadata plus item area/specs/supplier/notes.
"""
from __future__ import annotations

from typing import Any, Callable

from App.models import RoomRow


ACCESSORY_AREA_KEYWORDS = (
    "ACCESSORIES",
    "ACCESSORY",
    "BIN",
    "HAMPER",
    "HANGING RAIL",
    "RAIL",
    "JEWELLERY",
    "LIGHTING",
    "TROUSER RACK",
    "IRONING BOARD",
)


def populate_room_fields_from_v6(
    room: RoomRow,
    v6_section: dict[str, Any],
    all_v6_sections: list[dict[str, Any]],
) -> None:
    """Fill RoomRow room-level fields in-place from v6 section data."""
    items = [item for item in (v6_section.get("items") or []) if isinstance(item, dict)]
    metadata = dict(v6_section.get("metadata") or {})

    _populate_soft_close(room, metadata)
    _populate_flooring(room, metadata)
    _populate_toe_kick(room, items)
    _populate_led(room, items)
    _populate_door_colours_base(room, items)
    _populate_door_colours_overheads(room, items)
    _populate_door_panel_colours(room, items)
    _populate_handles(room, items)
    _populate_has_explicit_flags(room, items)
    _populate_evidence_snippet(room, items)
    _populate_accessories(room, items)
    _populate_shelf(room, items)
    _populate_bench_tops_other(room, items)
    _populate_sink_basin_info(room, all_v6_sections)
    _populate_door_colours_tall(room, items)
    _populate_led_note(room, items)


def _populate_soft_close(room: RoomRow, metadata: dict[str, Any]) -> None:
    hinges = _clean(metadata.get("hinges"))
    if "soft close" in hinges.lower() or "softclose" in hinges.lower():
        room.drawers_soft_close = "Soft Close"
        room.hinges_soft_close = "Soft Close"


def _populate_flooring(room: RoomRow, metadata: dict[str, Any]) -> None:
    flooring = _clean(metadata.get("floor_type"))
    if flooring and "N/A" not in flooring.upper():
        room.flooring = flooring


def _populate_toe_kick(room: RoomRow, items: list[dict[str, Any]]) -> None:
    values = []
    for item in items:
        area = _area(item)
        if "KICKBOARD" in area or "KICK" in area:
            values.append(_clean(item.get("specs")))
    room.toe_kick = _unique(values)


def _populate_led(room: RoomRow, items: list[dict[str, Any]]) -> None:
    item = _first_item(items, lambda area: "LED" in area or "LIGHTING" in area)
    room.led = _clean(item.get("specs")) if item else "No"


def _populate_door_colours_base(room: RoomRow, items: list[dict[str, Any]]) -> None:
    item = _first_item(items, lambda area: _area_has(area, "BASE", "CABINETRY", "COLOUR"))
    if item:
        room.door_colours_base = _format_supplier_specs(item)


def _populate_door_colours_overheads(room: RoomRow, items: list[dict[str, Any]]) -> None:
    matches = [
        _format_supplier_specs(item)
        for item in items
        if _area_has(_area(item), "UPPER", "CABINETRY", "COLOUR")
    ]
    room.door_colours_overheads = " | ".join(_unique(matches))


def _populate_door_panel_colours(room: RoomRow, items: list[dict[str, Any]]) -> None:
    values = []
    for item in items:
        area = _area(item)
        if "CABINETRY COLOUR" in area or "PANELLING" in area:
            values.append(_format_supplier_specs(item))
    room.door_panel_colours = _unique(values)


def _populate_handles(room: RoomRow, items: list[dict[str, Any]]) -> None:
    values = []
    for item in items:
        area = _area(item)
        if area == "HANDLES" or "HANDLES" in area:
            supplier = _clean(item.get("supplier"))
            for line in _lines(item.get("specs")):
                value = f"{supplier} - {line}" if supplier else line
                values.append(value)
    room.handles = _unique(values)


def _populate_has_explicit_flags(room: RoomRow, items: list[dict[str, Any]]) -> None:
    room.has_explicit_base = any(_area_has(_area(item), "BASE", "CABINETRY") for item in items)
    room.has_explicit_overheads = any(_area_has(_area(item), "UPPER", "CABINETRY") for item in items)


def _populate_evidence_snippet(room: RoomRow, items: list[dict[str, Any]]) -> None:
    snippets = []
    for item in items:
        text = _clean(" ".join([str(item.get("area") or ""), str(item.get("specs") or ""), str(item.get("notes") or "")]))
        if text:
            snippets.append(text)
    room.evidence_snippet = "\n".join(snippets)[:300]


def _populate_accessories(room: RoomRow, items: list[dict[str, Any]]) -> None:
    values = []
    for item in items:
        area = _area(item)
        if any(keyword in area for keyword in ACCESSORY_AREA_KEYWORDS):
            values.append(_format_supplier_specs(item))
    room.accessories = _unique(values)


def _populate_shelf(room: RoomRow, items: list[dict[str, Any]]) -> None:
    item = _first_item(items, lambda area: "SHELF" in area or "SHELVES" in area or "FLOATING" in area)
    if item:
        room.shelf = _format_supplier_specs(item)


def _populate_bench_tops_other(room: RoomRow, items: list[dict[str, Any]]) -> None:
    values = [
        _format_supplier_specs(item)
        for item in items
        if "BENCHTOP" in _area(item) or "BENCH TOP" in _area(item)
    ]
    room.bench_tops_other = " | ".join(_unique(values))


def _populate_sink_basin_info(room: RoomRow, all_v6_sections: list[dict[str, Any]]) -> None:
    markers = _accepted_room_markers(room.original_room_label)
    sinks: list[str] = []
    basins: list[str] = []
    for section in all_v6_sections:
        title = _clean(section.get("section_title")).upper()
        if "SINKWARE" not in title and "TAPWARE" not in title:
            continue
        for item in section.get("items") or []:
            if isinstance(item, dict):
                _collect_sink_basin_item(item, markers, sinks, basins)
    room.sink_info = " | ".join(_unique(sinks))
    room.basin_info = " | ".join(_unique(basins))


def _accepted_room_markers(room_label: str) -> tuple[str, ...]:
    """Build (MARKER, ...) tuple of acceptable SINKWARE-section area substrings for this room.

    Handles label variants: v6 items may use '(ENSUITE)' when room label is 'MASTER ENSUITE',
    or '(KITCHEN)' when room label is 'KITCHEN & PANTRY'.
    """
    label = _clean(room_label).upper()
    markers = [f"({label})"]
    if "MASTER ENSUITE" in label:
        markers.append("(ENSUITE)")
    if "KITCHEN & PANTRY" in label or "KITCHEN &PANTRY" in label:
        markers.append("(KITCHEN)")
    return tuple(markers)


def _populate_door_colours_tall(room: RoomRow, items: list[dict[str, Any]]) -> None:
    item = _first_item(items, lambda area: _area_has(area, "TALL", "CABINETRY", "COLOUR"))
    if item:
        room.door_colours_tall = _format_supplier_specs(item)


def _populate_led_note(room: RoomRow, items: list[dict[str, Any]]) -> None:
    item = _first_item(items, lambda area: "LED" in area or "LIGHTING" in area)
    if item and _clean(item.get("notes")):
        room.led_note = _clean(item.get("notes"))


def _collect_sink_basin_item(
    item: dict[str, Any],
    markers: tuple[str, ...],
    sinks: list[str],
    basins: list[str],
) -> None:
    area = _area(item)
    if not any(m in area for m in markers):
        return
    value = _sink_basin_value(item)
    if not value:
        return
    # 'SINK' prefix covers both 'SINK (...)' and 'SINKWARE (...)' variants
    if area.startswith("SINK") or area.startswith("TUB"):
        sinks.append(value)
    elif area.startswith("BASIN"):
        basins.append(value)


def _sink_basin_value(item: dict[str, Any]) -> str:
    specs = _clean(item.get("specs"))
    notes = _clean(item.get("notes"))
    if notes and "TAPHOLE" in notes.upper():
        return f"{specs} - Taphole location: {notes}" if specs else f"Taphole location: {notes}"
    return specs


def _first_item(items: list[dict[str, Any]], predicate: Callable[[str], bool]) -> dict[str, Any] | None:
    for item in items:
        if predicate(_area(item)):
            return item
    return None


def _area_has(area: str, *keywords: str) -> bool:
    return all(keyword in area for keyword in keywords)


def _area(item: dict[str, Any]) -> str:
    return _clean(item.get("area")).upper()


def _format_supplier_specs(item: dict[str, Any]) -> str:
    supplier = _clean(item.get("supplier"))
    specs = _clean(item.get("specs"))
    if supplier and specs:
        return f"{supplier} - {specs}"
    return supplier or specs


def _lines(value: Any) -> list[str]:
    lines = []
    for raw_line in str(value or "").replace("\r", "\n").split("\n"):
        line = _clean(raw_line).lstrip("-").strip()
        if line:
            lines.append(line)
    return lines


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _clean(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result
