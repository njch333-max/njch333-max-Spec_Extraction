from __future__ import annotations

import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from pypdf import PdfReader

from App.models import ApplianceRow, RoomRow, SnapshotPayload
from App.services.runtime import utc_now_iso


ROOM_ALIASES: dict[str, list[str]] = {
    "kitchen": ["kitchen"],
    "pantry": ["pantry"],
    "butlers_pantry": ["butler's pantry", "butlers pantry", "butler pantry"],
    "laundry": ["laundry"],
    "robe": ["robe", "robes"],
    "wir": ["walk in robe", "wir"],
    "wip": ["walk in pantry", "wip"],
    "vanity": ["vanity", "ensuite vanity", "bathroom vanity", "powder vanity"],
    "study": ["study"],
    "rumpus": ["rumpus"],
    "office": ["office"],
    "kitchenette": ["kitchenette"],
    "powder": ["powder", "wc", "powder room"],
    "ensuite": ["ensuite", "ensuite 1", "ensuite 2", "ensuite 3", "ensuite 4"],
    "bathroom": ["bathroom", "main bathroom"],
}

APPLIANCE_TYPES = ["sink", "cooktop", "oven", "rangehood", "dishwasher", "microwave", "fridge", "refrigerator"]

KNOWN_BRANDS = {
    "aeg": "https://www.aeg.com.au/",
    "westinghouse": "https://www.westinghouse.com.au/",
    "fisher & paykel": "https://www.fisherpaykel.com/au/",
    "fisher and paykel": "https://www.fisherpaykel.com/au/",
    "parisi": "https://www.parisi.com.au/",
    "everhard": "https://www.everhard.com.au/",
    "phoenix": "https://www.phoenixtapware.com.au/",
    "caroma": "https://www.caroma.com.au/",
    "johnson suisse": "https://www.johnsonsuisse.com.au/",
    "polytec": "https://www.polytec.com.au/",
    "laminex": "https://www.laminex.com.au/",
}


def extract_pdf_pages(path: Path) -> list[dict[str, str | bool | int]]:
    pages: list[dict[str, str | bool | int]] = []
    reader = PdfReader(str(path))
    for index, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").replace("\x00", " ")
        text = normalize_space(text)
        pages.append({"page_no": index, "text": text, "needs_ocr": len(text) < 80})
    return pages


def extract_docx_text(path: Path) -> list[dict[str, str | bool | int]]:
    with zipfile.ZipFile(path) as archive:
        xml = archive.read("word/document.xml")
    root = ET.fromstring(xml)
    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paras: list[str] = []
    for para in root.findall(".//w:p", ns):
        texts = [node.text for node in para.findall(".//w:t", ns) if node.text]
        if texts:
            paras.append("".join(texts))
    full_text = normalize_space("\n".join(paras))
    return [{"page_no": 1, "text": full_text, "needs_ocr": False}]


def load_document_pages(path: Path) -> list[dict[str, str | bool | int]]:
    if path.suffix.lower() == ".pdf":
        return extract_pdf_pages(path)
    if path.suffix.lower() == ".docx":
        return extract_docx_text(path)
    return [{"page_no": 1, "text": "", "needs_ocr": False}]


def normalize_space(text: str) -> str:
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_room_key(label: str) -> str:
    candidate = normalize_space(label).lower()
    for room_key, aliases in ROOM_ALIASES.items():
        for alias in aliases:
            if candidate == alias or alias in candidate:
                return room_key
    return re.sub(r"[^a-z0-9]+", "_", candidate).strip("_") or "room"


def parse_documents(job_no: str, builder_name: str, source_kind: str, documents: list[dict[str, object]]) -> dict:
    rooms: dict[str, RoomRow] = {}
    appliances: list[ApplianceRow] = []
    warnings: list[str] = []
    source_documents: list[dict[str, str]] = []
    flooring_notes: list[str] = []
    splashback_notes: list[str] = []

    for document in documents:
        file_name = str(document["file_name"])
        pages = list(document["pages"])
        source_documents.append({"file_name": file_name, "role": str(document["role"]), "page_count": str(len(pages))})
        full_text = "\n\n".join(str(page["text"]) for page in pages if page["text"])
        if not full_text.strip():
            warnings.append(f"No extractable text found in {file_name}.")
            continue
        for page in pages:
            if page.get("needs_ocr"):
                warnings.append(f"Low-text page detected in {file_name} page {page['page_no']}.")
        for room_key, chunk in _find_room_sections(full_text):
            lines = _preprocess_chunk(chunk)
            row = rooms.get(room_key) or RoomRow(room_key=room_key, original_room_label=chunk.split("\n", 1)[0][:80], source_file=file_name)
            row.bench_tops = _merge_lists(row.bench_tops, _collect_field(lines, ["Bench Tops", "Benchtop"]))
            row.door_panel_colours = _merge_lists(row.door_panel_colours, _collect_field(lines, ["Door/Panel Colour", "Door/Panel Colours", "Door Colour"]))
            row.toe_kick = _merge_lists(row.toe_kick, _collect_field(lines, ["Toe Kick", "Kickboard"]))
            row.bulkheads = _merge_lists(row.bulkheads, _collect_field(lines, ["Bulkheads", "Bulkhead"]))
            row.handles = _merge_lists(row.handles, _collect_field(lines, ["Handles", "Handle", "Base Cabinet Handles", "Overhead Handles"]))
            row.drawers_soft_close = row.drawers_soft_close or _extract_soft_close(lines, "drawer")
            row.hinges_soft_close = row.hinges_soft_close or _extract_soft_close(lines, "hinge")
            row.splashback = row.splashback or _first_value(_collect_field(lines, ["Splashback"]))
            row.flooring = row.flooring or _first_value(_collect_field(lines, ["Flooring"]))
            row.page_refs = row.page_refs or _guess_page_refs(chunk, pages)
            row.evidence_snippet = row.evidence_snippet or chunk[:300]
            row.confidence = max(row.confidence, 0.55)
            rooms[room_key] = row
        appliances.extend(_extract_appliances(full_text, file_name, pages))
        flooring_text = _extract_global_value(full_text, "flooring")
        splashback_text = _extract_global_value(full_text, "splashback")
        if flooring_text:
            flooring_notes.append(flooring_text)
        if splashback_text:
            splashback_notes.append(splashback_text)

    payload = SnapshotPayload(
        job_no=job_no,
        builder_name=builder_name,
        source_kind=source_kind,
        generated_at=utc_now_iso(),
        rooms=list(rooms.values()),
        appliances=_dedupe_appliances(appliances),
        others={
            "flooring_notes": " | ".join(_unique(flooring_notes)),
            "splashback_notes": " | ".join(_unique(splashback_notes)),
        },
        warnings=_unique(warnings),
        source_documents=source_documents,
    )
    return payload.model_dump()


def _find_room_sections(text: str) -> list[tuple[str, str]]:
    matches: list[tuple[int, str, str]] = []
    for room_key, aliases in ROOM_ALIASES.items():
        for alias in aliases:
            pattern = re.compile(rf"\b{re.escape(alias)}\b", re.IGNORECASE)
            for match in pattern.finditer(text):
                matches.append((match.start(), match.group(0), room_key))
    matches.sort(key=lambda item: item[0])
    deduped: list[tuple[int, str, str]] = []
    last_start = -100
    for start, label, room_key in matches:
        if start - last_start < 20:
            continue
        deduped.append((start, label, room_key))
        last_start = start
    sections: list[tuple[str, str]] = []
    for index, (start, _label, room_key) in enumerate(deduped):
        end = deduped[index + 1][0] if index + 1 < len(deduped) else min(len(text), start + 2500)
        chunk = normalize_space(text[start:end])
        if chunk:
            sections.append((room_key, chunk))
    return sections


def _preprocess_chunk(chunk: str) -> list[str]:
    labels = [
        "Bench Tops",
        "Benchtop",
        "Door/Panel Colour",
        "Door Colour",
        "Door/Panel Colours",
        "Toe Kick",
        "Kickboard",
        "Bulkheads",
        "Bulkhead",
        "Handles",
        "Handle",
        "Drawers",
        "Hinges",
        "Splashback",
        "Flooring",
    ]
    for label in labels:
        chunk = re.sub(rf"(?i)\b{re.escape(label)}\b", f"\n{label}", chunk)
    return [normalize_space(line) for line in chunk.split("\n") if normalize_space(line)]


def _collect_field(lines: list[str], prefixes: list[str]) -> list[str]:
    values: list[str] = []
    for line in lines:
        lowered = line.lower()
        for prefix in prefixes:
            if lowered.startswith(prefix.lower()):
                value = line[len(prefix):].strip(" :-")
                if value and value not in values:
                    values.append(value)
    return values


def _extract_soft_close(lines: list[str], keyword: str) -> str:
    for line in lines:
        lowered = line.lower()
        if keyword not in lowered:
            continue
        if "not soft close" in lowered or "not soft closed" in lowered:
            return "Not Soft Close"
        if "soft close" in lowered or "soft closed" in lowered:
            return "Soft Close"
    return ""


def _extract_appliances(text: str, file_name: str, pages: list[dict[str, object]]) -> list[ApplianceRow]:
    prepared = text
    for appliance_type in APPLIANCE_TYPES:
        prepared = re.sub(rf"(?i)\b{re.escape(appliance_type)}\b", lambda m: f"\n{m.group(0).title()}", prepared)
    lines = [normalize_space(line) for line in prepared.split("\n") if normalize_space(line)]
    rows: list[ApplianceRow] = []
    for line in lines:
        lowered = line.lower()
        matched_type = next((item for item in APPLIANCE_TYPES if lowered.startswith(item)), "")
        if not matched_type:
            continue
        rows.append(
            ApplianceRow(
                appliance_type="Fridge" if matched_type in {"fridge", "refrigerator"} else matched_type.title(),
                make=_guess_make(line),
                model_no=_guess_model(line),
                website_url=_brand_url(line),
                overall_size=_guess_size(line),
                source_file=file_name,
                page_refs=_guess_page_refs(line, pages),
                evidence_snippet=line[:300],
                confidence=0.5,
            )
        )
    return rows


def _guess_make(text: str) -> str:
    lowered = text.lower()
    for brand in KNOWN_BRANDS:
        if brand in lowered:
            return brand.title()
    return ""


def _guess_model(text: str) -> str:
    match = re.search(r"\b[A-Z0-9][A-Z0-9/-]{4,}\b", text)
    return match.group(0) if match else ""


def _guess_size(text: str) -> str:
    match = re.search(r"\b\d{2,4}\s?(?:mm|cm)\b", text, re.IGNORECASE)
    return match.group(0) if match else ""


def _brand_url(text: str) -> str:
    lowered = text.lower()
    for brand, url in KNOWN_BRANDS.items():
        if brand in lowered:
            return url
    return ""


def _guess_page_refs(snippet: str, pages: list[dict[str, object]]) -> str:
    for page in pages:
        page_text = str(page["text"])
        if snippet[:80] and snippet[:80] in page_text:
            return str(page["page_no"])
    return ""


def _extract_global_value(text: str, label: str) -> str:
    match = re.search(rf"(?i)\b{re.escape(label)}\b[:\s-]*(.{0,160})", text)
    return normalize_space(match.group(1))[:160] if match else ""


def _merge_lists(left: list[str], right: list[str]) -> list[str]:
    return _unique(left + right)


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def _first_value(values: list[str]) -> str:
    return values[0] if values else ""


def _dedupe_appliances(rows: list[ApplianceRow]) -> list[ApplianceRow]:
    seen: set[tuple[str, str, str]] = set()
    result: list[ApplianceRow] = []
    for row in rows:
        key = (row.appliance_type.lower(), row.make.lower(), row.model_no.lower())
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result
