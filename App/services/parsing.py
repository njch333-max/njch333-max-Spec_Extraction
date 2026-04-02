from __future__ import annotations

import re
import zipfile
from ast import literal_eval
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from pypdf import PdfReader

from App.models import AnalysisMeta, ApplianceRow, RoomRow, SnapshotPayload, SpecialSectionRow
from App.services import cleaning_rules
from App.services.runtime import utc_now_iso


ROOM_ALIASES: dict[str, list[str]] = {
    "kitchen": ["kitchen"],
    "pantry": ["pantry"],
    "butlers_pantry": ["butler's pantry", "butlers pantry", "butler pantry"],
    "walk_in_pantry": ["walk in pantry", "walk-in-pantry", "wip"],
    "laundry": ["laundry"],
    "robe": ["robe", "robes"],
    "wir": ["walk in robe", "wir"],
    "vanity": ["vanity", "vanities", "ensuite vanity", "bathroom vanity", "powder vanity"],
    "study": ["study"],
    "study_desk": ["study desk"],
    "meals_room": ["meals room"],
    "rumpus": ["rumpus"],
    "office": ["office"],
    "make_up_desk": ["make up desk", "makeup desk"],
    "dining_banquette": ["dining banquette"],
    "alfresco": ["alfresco"],
    "robe_sliding": ["robe sliding"],
    "master_ensuite": ["master ensuite"],
    "butlers_wip": ["butlers wip", "butlers/wip"],
    "theatre": ["theatre", "theatre room", "media room"],
    "kitchenette": ["kitchenette"],
    "powder": ["powder", "wc", "powder room"],
    "ensuite": ["ensuite", "ensuite 1", "ensuite 2", "ensuite 3", "ensuite 4"],
    "bathroom": ["bathroom", "bathrooms", "main bathroom"],
}

ROOM_HEADING_CLEANUP_PATTERNS = (
    r"(?i)^\s*room\b",
    r"(?i)\bcolour schedule\b",
    r"(?i)\bsupplier description design comments\b",
    r"(?i)\bjoinery selection sheet\b",
    r"(?i)\bjoinery\b",
    r"(?i)\bthermolaminate notes\b.*$",
)

ROOM_HEADING_PREFIX_NOISE_PATTERNS = (
    r"(?i)^(?:na|n/?a)\b[\s:./-]*",
    r"(?i)^ref\.?\s*number\b[\s:./-]*",
    r"(?i)^(?:image|notes|supplier|client|date|address|document ref)\b[\s:./-]*",
)

ROOM_HEADING_TRIM_MARKERS = (
    r"(?i)\bbench tops?\b.*$",
    r"(?i)\bbenchtop\b.*$",
    r"(?i)\bdoor(?:/panel)? colours?\b.*$",
    r"(?i)\bglazing\b.*$",
    r"(?i)\bdoor\b.*$",
    r"(?i)\bfinish\b.*$",
    r"(?i)\bwaste colour\b.*$",
    r"(?i)\bflooring\b.*$",
)

ROOM_LIKE_NOISE_PATTERNS = (
    r"(?i)\bglazing\b",
    r"(?i)\bdoor\b",
    r"(?i)\bfinish\b",
    r"(?i)\bwaste colour\b",
    r"(?i)\bflooring\b",
    r"(?i)\bwindows?\b",
    r"(?i)\bframe\b",
    r"(?i)\bpaint(?:ed)?\b",
    r"(?i)\bcolorbond\b",
)

ROOM_PREFIX_PATTERN_SPECS: tuple[tuple[str, str], ...] = (
    (r"main kitchen", "Main Kitchen"),
    (r"master ensuite", "Master Ensuite"),
    (r"study desk", "Study Desk"),
    (r"make(?:\s+up|up)\s+desk", "Make Up Desk"),
    (r"dining banquette", "DINING BANQUETTE"),
    (r"laundry\s*&\s*mud room", "Laundry & Mud Room"),
    (r"bulters?\s*/\s*wip", "Butlers/WIP"),
    (r"butlers?\s*/\s*wip", "Butlers/WIP"),
    (r"butlers?\s+pantry", "Butlers Pantry"),
    (r"walk[- ]in[- ]pantry", "WALK-IN-PANTRY"),
    (r"walk[- ]in[- ]robe", "Walk In Robe"),
    (r"robe sliding", "Robe Sliding"),
    (r"meals room", "Meals Room"),
    (r"family room", "Family Room"),
    (r"kitchenette", "Kitchenette"),
    (r"alfresco", "Alfresco"),
    (r"kitchen", "Kitchen"),
    (r"butlers?", "Butlers"),
    (r"pantry", "Pantry"),
    (r"laundry", "Laundry"),
    (r"bathroom", "Bathroom"),
    (r"ensuite\s*\d+", "Ensuite"),
    (r"ensuite", "Ensuite"),
    (r"powder", "Powder"),
    (r"wc", "WC"),
    (r"study", "Study"),
    (r"office", "Office"),
    (r"robe", "Robe"),
)

ROOM_PREFIX_FIELD_HINTS: tuple[str, ...] = (
    "accessories",
    "architrave",
    "appliances",
    "basin",
    "bench",
    "benchtop",
    "cabinet",
    "cabinetry",
    "colour",
    "door",
    "drawer",
    "finish",
    "floating",
    "frame",
    "glazing",
    "handle",
    "hinge",
    "island",
    "kick",
    "location",
    "manufacturer",
    "mirror",
    "model",
    "mixer",
    "overhead",
    "panel",
    "panels",
    "profile",
    "range",
    "shelf",
    "shaving",
    "sink",
    "sinkware",
    "spout",
    "style",
    "tap",
    "tapware",
    "toilet",
    "towel",
    "trough",
    "tub",
    "type",
    "underbench",
    "wall run",
    "waste",
)

ROOM_PREFIX_SPLIT_JOINERY_TAILS: tuple[str, ...] = (
    "benchtop",
    "bench tops",
    "wall run benchtop",
    "island/penisula benchtop",
    "underbench",
    "underbench including island",
    "base cabinet panels",
    "wall run base cabinet panels",
    "island/penisula base cabinet panels",
    "overhead cupboards",
    "overheads",
    "kickboard",
    "wall run kickboard",
    "island/penisula kickboard",
    "cabinet panels",
    "floating shelf",
    "floating shelves",
    "shelving",
    "shadowline",
)

ROOM_PREFIX_SPLIT_FIXTURE_TAILS: tuple[str, ...] = (
    "sink",
    "sinkware",
    "tap",
    "tapware",
    "trough",
    "basin",
    "basin mixer",
    "vanity basin",
    "vanity basin tapware",
    "bath",
    "bath/spa bath",
    "bath tapware",
    "feature waste",
    "toilet suite",
    "toilet roll holder",
    "shower mixer",
    "shower rose",
    "shower frame",
    "shower screen",
    "floor waste",
    "mirror",
    "accessories",
)

ROOM_PREFIX_SPLIT_EXCLUDED_TAILS: tuple[str, ...] = (
    "door handle",
    "drawer handle",
    "pantry door handle",
    "bin & pot drawers handle",
    "base cabinetry handles",
    "cabinetry handles",
    "overhead cabinetry handles",
    "handles",
    "pantry doors",
    "door colour",
    "door colours",
)

APPLIANCE_TYPES = ["sink", "cooktop", "oven", "rangehood", "dishwasher", "microwave", "fridge", "refrigerator"]

APPLIANCE_LABEL_SPECS: list[tuple[str, list[str]]] = [
    ("Sink", [r"Sink Type/Model\s*:", r"Sink Type\s*:", r"Drop in Tub\s*:"]),
    ("Oven", [r"Under Bench Oven\s*:", r"Freestanding Cooker\s*:", r"Oven\s*:"]),
    ("Cooktop", [r"Cooktop\s*:"]),
    ("Microwave", [r"Microwave Make\s*:", r"Microwave\s*:"]),
    ("Dishwasher", [r"Dishwasher Make\s*:", r"Dishwasher\s*:"]),
    ("Rangehood", [r"Rangehood\s*:"]),
    ("Fridge", [r"Integrated Fridge/Freezer\s*:", r"Integrated Fridge Freezer\s*:", r"Fridge/Freezer\s*:", r"Refrigerator\s*:", r"Fridge\s*:"]),
]

LOOSE_APPLIANCE_TYPE_MAP: list[tuple[str, str]] = [
    ("dishwasher", "Dishwasher"),
    ("rangehood", "Rangehood"),
    ("microwave", "Microwave"),
    ("cooktop", "Cooktop"),
    ("oven", "Oven"),
    ("fridge/freezer", "Fridge"),
    ("refrigerator", "Fridge"),
    ("fridge", "Fridge"),
    ("sink", "Sink"),
]

LOOSE_SKIP_PHRASES = {
    "tap for fridge",
    "sink mixer",
    "rangehood ducting",
    "client to check",
    "opening on plans",
    "cut out",
    "cutout",
    "to cabinet",
    "by builder",
    "distance between",
    "taphole",
    "if applicable",
    "heatdeflectors",
}

MODEL_STOPWORDS = {
    "CLIENT",
    "OPENING",
    "ELECTRIC",
    "FREESTANDING",
    "UNDERMOUNT",
    "STAINLESS",
    "STEEL",
    "CHROME",
    "WHITE",
    "BUILDER",
    "APPLICABLE",
    "MIXER",
    "DUCTING",
    "OTHERS",
    "N",
    "NA",
}

KNOWN_BRANDS = {
    "fisher & paykel": "https://www.fisherpaykel.com/au/",
    "fisher and paykel": "https://www.fisherpaykel.com/au/",
    "johnson suisse": "https://www.johnsonsuisse.com.au/",
    "westinghouse": "https://www.westinghouse.com.au/",
    "everhard": "https://www.everhard.com.au/",
    "phoenix": "https://www.phoenixtapware.com.au/",
    "parisi": "https://www.parisi.com.au/",
    "caroma": "https://www.caroma.com.au/",
    "laminex": "https://www.laminex.com.au/",
    "polytec": "https://www.polytec.com.au/",
    "ceasarstone": "https://www.caesarstone.com.au/",
    "aeg": "https://www.aegaustralia.com.au/",
}

CANONICAL_BRAND_LABELS = {
    "fisher & paykel": "Fisher & Paykel",
    "fisher and paykel": "Fisher & Paykel",
    "johnson suisse": "Johnson Suisse",
    "westinghouse": "Westinghouse",
    "everhard": "Everhard",
    "phoenix": "Phoenix",
    "parisi": "Parisi",
    "caroma": "Caroma",
    "laminex": "Laminex",
    "polytec": "Polytec",
    "ceasarstone": "Caesarstone",
    "aeg": "AEG",
}

FIELD_LABELS = [
    "Back Benchtops",
    "Island Benchtop",
    "Wall Run Bench Top",
    "Island Bench Top",
    "Bench Tops",
    "Benchtop",
    "Floating Shelves",
    "Floating Shelf",
    "Overhead Cupboards",
    "Open Shelving",
    "Base Cupboards & Drawers",
    "Island Bar Back",
    "Island Bench Base Cupboards & Drawers",
    "Floor Mounted Vanity",
    "Upper Cabinetry Colour + Tall Cabinets",
    "Upper Cabinetry Colour",
    "Tall Cabinets",
    "Tall Cabinet",
    "Tall Doors",
    "Base Cabinetry Colour",
    "Door/Panel Colour",
    "Door/Panel Colour 1",
    "Door/Panel Colour 2",
    "Door Colour",
    "Door Colour 1",
    "Door Colour 2",
    "Door/Panel Colours",
    "Doors/Panels",
    "Doors/Panel",
    "Island Bench Kickboard",
    "Kickboards",
    "Toe Kick",
    "Kickboard",
    "Benchtop Shadowline",
    "Bulkheads",
    "Bulkhead",
    "Bulkhead Shadowline",
    "Carcass & Shelf Edges",
    "Thermolaminate Notes",
    "Base Cabinet Handles",
    "Overhead Handles",
    "Handles to Overheads",
    "Handles Base Cabs",
    "Custom Handles",
    "Handles",
    "Handle",
    "LED Strip Lighting",
    "LED Lighting",
    "LED",
    "Accessories",
    "Accessory",
    "Rail",
    "Jewellery Insert",
    "Sink Type/Model",
    "Sink Type",
    "Drop in Tub",
    "Vanity Inset Basin",
    "Vanity Tap Style",
    "Sink Mixer",
    "Pull-Out Mixer",
    "Tap Type",
    "Tap Style",
    "Mixer",
    "Sink",
    "Basin",
    "Door Hinges",
    "Drawer Runners",
    "Drawers",
    "Hinges",
    "Splashback",
    "Flooring",
]

DOOR_COLOUR_FIELD_PREFIXES = [
    "Door/Panel Colour",
    "Door/Panel Colours",
    "Door Colour",
    "Doors/Panels",
    "Doors/Panel",
]

CONTINUATION_SKIP_PATTERNS = (
    r"^yellowwood supplier$",
    r"^as supplied by\b.*$",
    r"^supplied by client$",
    r"^handle house$",
    r"^national tiles$",
    r"^dowell$",
    r"^gliderol$",
    r"^corinthian doors$",
    r"^hume doors$",
    r"^pgh bricks$",
    r"^wattyl\b.*$",
    r"^lot \d+\b.*$",
    r"^page \d+/\d+$",
    r"^tone interior design consulting\b.*$",
    r"^n/?a$",
)

CABINET_ONLY_EXCLUDE_PATTERNS = (
    r"\bpainted\b",
    r"\bpaint(?:\s+finish)?\b",
    r"\bcolorbond\b",
    r"\bgarage door\b",
    r"\bentry door\b",
    r"\bdoor frame\b",
    r"\bwindow(?: frame)?s?\b",
    r"\bexternal finishes?\b",
    r"\binternal doors?\b",
    r"\bposts?\b",
    r"\bfascia\b",
    r"\bgutter\b",
    r"\bsoffits?\b",
    r"\bcladding\b",
)

JOINERY_PAGE_HINTS = (
    "joinery - refer to cabinetry plans",
    "colour schedule",
    "overhead cupboards",
    "base cupboards & drawers",
    "base cabinet handles",
    "floor mounted vanity",
    "back benchtops",
    "island benchtop",
    "as supplied by cabinetmaker",
)

ROOM_SCHEDULE_PATTERNS = (
    r"butler'?s pantry",
    r"walk[- ]in[- ]pantry",
    r"vanities",
    r"vanity",
    r"main bathroom",
    r"bathrooms?",
    r"ensuite(?:\s+\d+)?",
    r"powder(?:\s+room)?(?:\s+\d+)?",
    r"laundry",
    r"meals room",
    r"family room",
    r"kitchen",
    r"pantry",
    r"wip",
    r"study desk",
    r"make(?:\s+up|up)\s+desk",
    r"dining banquette",
    r"alfresco",
    r"robe(?:s)?",
    r"wir",
    r"theatre(?:\s+room)?",
    r"rumpus(?:\s+room)?",
    r"study",
    r"office",
    r"kitchenette",
)

ROOM_SCHEDULE_PATTERN = "|".join(sorted(ROOM_SCHEDULE_PATTERNS, key=len, reverse=True))
ROOM_HEADING_MATCH_PATTERNS = tuple(sorted(ROOM_SCHEDULE_PATTERNS, key=len, reverse=True))


def extract_pdf_pages(path: Path) -> list[dict[str, str | bool | int]]:
    pages: list[dict[str, str | bool | int]] = []
    reader = PdfReader(str(path))
    fitz_document = None
    pdfplumber_document = None
    try:
        import fitz  # type: ignore

        fitz_document = fitz.open(str(path))
    except Exception:
        fitz_document = None
    try:
        import pdfplumber  # type: ignore

        pdfplumber_document = pdfplumber.open(str(path))
    except Exception:
        pdfplumber_document = None
    for index, page in enumerate(reader.pages, start=1):
        raw_text = (page.extract_text() or "").replace("\x00", " ")
        text = normalize_space(raw_text)
        text_blocks: list[dict[str, Any]] = []
        page_width = 0.0
        table_rows: list[list[list[str | None]]] = []
        if fitz_document is not None and index - 1 < fitz_document.page_count:
            try:
                fitz_page = fitz_document.load_page(index - 1)
                page_width = float(getattr(fitz_page.rect, "width", 0.0) or 0.0)
                for block in fitz_page.get_text("blocks"):
                    if len(block) < 5:
                        continue
                    x0, y0, x1, y1, block_text = block[:5]
                    cleaned = normalize_space(str(block_text or "").replace("\x00", " "))
                    if not cleaned:
                        continue
                    text_blocks.append(
                        {
                            "x0": float(x0),
                            "y0": float(y0),
                            "x1": float(x1),
                            "y1": float(y1),
                            "text": cleaned,
                        }
                    )
            except Exception:
                text_blocks = []
                page_width = 0.0
        if pdfplumber_document is not None and index - 1 < len(pdfplumber_document.pages):
            try:
                plumber_page = pdfplumber_document.pages[index - 1]
                extracted_tables = plumber_page.extract_tables() or []
                normalized_tables: list[list[list[str | None]]] = []
                for table in extracted_tables:
                    normalized_rows: list[list[str | None]] = []
                    for row in table or []:
                        cleaned_row = [
                            normalize_space(str(cell).replace("\x00", " ")) if cell is not None else None
                            for cell in (row or [])
                        ]
                        if any(cell for cell in cleaned_row if cell):
                            normalized_rows.append(cleaned_row)
                    if normalized_rows:
                        normalized_tables.append(normalized_rows)
                table_rows = normalized_tables
            except Exception:
                table_rows = []
        pages.append(
            {
                "page_no": index,
                "raw_text": raw_text,
                "text": text,
                "needs_ocr": len(text) < 80,
                "text_blocks": text_blocks,
                "page_width": page_width,
                "table_rows": table_rows,
            }
        )
    if fitz_document is not None:
        fitz_document.close()
    if pdfplumber_document is not None:
        pdfplumber_document.close()
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
    raw_text = "\n".join(paras)
    full_text = normalize_space(raw_text)
    return [{"page_no": 1, "raw_text": raw_text, "text": full_text, "needs_ocr": False}]


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


def normalize_brand_label(value: str) -> str:
    lowered = normalize_space(str(value or "")).lower()
    for brand, label in sorted(CANONICAL_BRAND_LABELS.items(), key=lambda item: len(item[0]), reverse=True):
        if brand == lowered or brand in lowered or lowered in brand:
            return label
    return normalize_space(str(value or ""))


def normalize_brand_casing_text(value: Any, rule_flags: Any = None) -> str:
    text = normalize_space(str(value or ""))
    if not text or not cleaning_rules.rule_enabled(rule_flags, "normalize_brand_casing"):
        return text
    normalized = text
    for brand, label in sorted(CANONICAL_BRAND_LABELS.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(rf"(?i)\b{re.escape(brand)}\b", label, normalized)
    normalized = re.sub(r"(?i)\bpol\s+ytec\b", "Polytec", normalized)
    normalized = re.sub(
        r"(?i)\b(Polytec|Laminex|Caesarstone|Smartstone|Westinghouse|Caroma|Phoenix|Parisi|Everhard|AEG)(?=[A-Z])",
        r"\1 ",
        normalized,
    )
    return normalize_space(normalized)


def normalize_room_key(label: str) -> str:
    candidate = normalize_space(label).lower()
    for room_key, aliases in ROOM_ALIASES.items():
        for alias in aliases:
            if candidate == alias or alias in candidate:
                return room_key
    return re.sub(r"[^a-z0-9]+", "_", candidate).strip("_") or "room"


def _looks_like_room_field_tail(text: str) -> bool:
    tail = normalize_space(text).strip(" -:/,;()")
    if not tail:
        return False
    lowered = tail.lower()
    return any(hint in lowered for hint in ROOM_PREFIX_FIELD_HINTS)


def _extract_room_prefix_parts(text: str) -> tuple[str, str]:
    normalized = normalize_space(text)
    if not normalized:
        return "", ""
    bed_robes_match = re.match(r"(?i)^(bed\s*\d+\s+robes?)\b(?P<rest>.*)$", normalized)
    if bed_robes_match:
        label = normalize_space(bed_robes_match.group(1)).title().replace("Robes", "Robes")
        rest = normalize_space(bed_robes_match.group("rest") or "").strip(" -:/,;")
        return label, rest
    for pattern, label in ROOM_PREFIX_PATTERN_SPECS:
        match = re.match(rf"(?i)^(?:room\s+)?(?P<label>{pattern})\b(?P<rest>.*)$", normalized)
        if not match:
            continue
        rest = normalize_space(match.group("rest") or "").strip(" -:/,;")
        return label, rest
    return "", ""


def _extract_room_prefix_label(text: str) -> str:
    label, rest = _extract_room_prefix_parts(text)
    if not label:
        return ""
    if not rest or _looks_like_room_field_tail(rest):
        return label
    return ""


def source_room_label(label: str, fallback_key: str = "") -> str:
    text = normalize_space(label)
    if not text and fallback_key:
        text = fallback_key.replace("_", " ")
    if not text:
        return "Room"
    for pattern in ROOM_HEADING_PREFIX_NOISE_PATTERNS:
        previous = None
        while text and previous != text:
            previous = text
            text = normalize_space(re.sub(pattern, "", text))
    text = re.sub(r"(?i)(colour schedule)(?=[A-Z])", r"\1 ", text)
    text = re.sub(r"(?i)\b(WALK[- ]IN[- ]PANTRY)\b\s+PANTRY$", r"\1", text)
    for pattern in ROOM_HEADING_CLEANUP_PATTERNS:
        text = re.sub(pattern, "", text)
    exact_specific_match = _extract_specific_room_heading(text)
    if exact_specific_match:
        return exact_specific_match
    for pattern in ROOM_HEADING_TRIM_MARKERS:
        text = re.sub(pattern, "", text)
    if ":" in text:
        text = text.split(":", 1)[0]
    text = re.sub(r"(?i)\broom specifications?\b", "", text)
    text = re.sub(r"\s*[:\-]+\s*$", "", text)
    text = normalize_space(text)
    text = re.sub(r"(?i)\b(WALK[- ]IN[- ]PANTRY)\b(?:\s+PANTRY)?$", r"\1", text)
    specific_match = _extract_specific_room_heading(text)
    if specific_match:
        return specific_match
    prefix_match = _extract_room_prefix_label(text)
    if prefix_match:
        return prefix_match
    if not text and fallback_key:
        return fallback_key.replace("_", " ").title()
    return text or "Room"


def _extract_specific_room_heading(text: str) -> str:
    for pattern in ROOM_HEADING_MATCH_PATTERNS:
        match = re.search(rf"(?i)\b{pattern}\b", text)
        if not match:
            continue
        value = normalize_space(match.group(0))
        normalized_text = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
        normalized_value = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
        if normalized_text != normalized_value:
            continue
        if value.lower() == "wip":
            return "WIP"
        return value
    return ""


def source_room_key(label: str, fallback_key: str = "") -> str:
    text = source_room_label(label, fallback_key=fallback_key)
    lowered = normalize_space(text).lower()
    lowered = lowered.replace("-", " ")
    lowered = lowered.replace("&", " and ")
    lowered = re.sub(r"[’']", "", lowered)
    lowered = re.sub(r"\btheatre room\b", "theatre", lowered)
    lowered = re.sub(r"\bmedia room\b", "media room", lowered)
    lowered = re.sub(r"\bwalk in robe\b", "wir", lowered)
    lowered = re.sub(r"\bbutlers pantry\b", "butlers pantry", lowered)
    lowered = re.sub(r"\bpowder room\b", "powder room", lowered)
    bed_ensuite_match = re.search(r"\bbed\s*(\d+)\s+ensuite\b", lowered)
    if bed_ensuite_match:
        return f"ensuite_{bed_ensuite_match.group(1)}"
    ensuite_match = re.search(r"\bensuite\s*(\d+)\b", lowered)
    if ensuite_match:
        return f"ensuite_{ensuite_match.group(1)}"
    powder_match = re.search(r"\bpowder(?:\s+room)?\s*(\d+)\b", lowered)
    if powder_match:
        return f"powder_room_{powder_match.group(1)}"
    if "butlers pantry" in lowered or "butler pantry" in lowered:
        return "butlers_pantry"
    if "main bathroom vanity" in lowered:
        return "main_bathroom"
    if "bathroom vanity" in lowered:
        return "bathroom"
    powder_vanity_match = re.search(r"\bpowder(?:\s+room)?\s*(\d+)?\s+vanity\b", lowered)
    if powder_vanity_match:
        room_no = powder_vanity_match.group(1)
        return f"powder_room_{room_no}" if room_no else "powder"
    ensuite_vanity_match = re.search(r"\bensuite\s*(\d+)?\s+vanity\b", lowered)
    if ensuite_vanity_match:
        room_no = ensuite_vanity_match.group(1)
        return f"ensuite_{room_no}" if room_no else "ensuite"
    if lowered in {"wip", "walk in pantry"}:
        return "walk_in_pantry"
    if lowered in {"main kitchen", "kitchen"}:
        return "kitchen"
    if lowered in {"master ensuite"}:
        return "master_ensuite"
    if lowered in {"study desk"}:
        return "study_desk"
    if lowered in {"make up desk", "makeup desk"}:
        return "make_up_desk"
    if lowered in {"dining banquette"}:
        return "dining_banquette"
    if lowered in {"alfresco"}:
        return "alfresco"
    if lowered in {"robe sliding"}:
        return "robe_sliding"
    if lowered in {"butlers wip"}:
        return "butlers_wip"
    if lowered in {"butlers"}:
        return "butlers"
    if lowered in {"wir", "walk in robe"}:
        return "wir"
    if lowered in {"meals room"}:
        return "meals_room"
    if lowered in {"family room"}:
        return "family_room"
    if lowered in {"robe", "robes"}:
        return "robe"
    if "main bathroom" in lowered:
        return "main_bathroom"
    if lowered in {"bathroom", "bathrooms"}:
        return "bathroom"
    if lowered in {"ensuite"}:
        return "ensuite"
    if lowered.startswith("powder"):
        return "powder"
    return re.sub(r"[^a-z0-9]+", "_", lowered).strip("_") or (fallback_key or "room")


def same_room_identity(*values: str) -> str:
    for value in values:
        key = source_room_key(value)
        if key and key != "room":
            return key
    return "room"


def _looks_like_field_label(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return False
    for label in FIELD_LABELS:
        if re.match(rf"^{re.escape(label)}(?:\s*\d+)?\b", text, re.IGNORECASE):
            return True
    return False


def _is_room_heading_line(line: str) -> bool:
    text = normalize_space(line)
    if not text or len(text) > 80 or any(char.isdigit() for char in text):
        return False
    lowered = text.lower()
    if lowered in {"wc", "powder", "bathroom", "pantry", "kitchen", "laundry"}:
        return True
    return any(
        lowered == alias or lowered.startswith(f"{alias} ")
        for aliases in ROOM_ALIASES.values()
        for alias in aliases
    )


def _skip_continuation_line(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return True
    return any(re.match(pattern, text, re.IGNORECASE) for pattern in CONTINUATION_SKIP_PATTERNS)


def _should_stop_field_continuation(prefix: str, next_line: str) -> bool:
    text = normalize_space(next_line)
    if _is_room_heading_line(text):
        return True
    if _is_schedule_room_heading(text):
        return True
    if _looks_like_strict_appliance_label(text):
        return True
    if not _looks_like_field_label(text):
        return False
    if prefix in {"Tap Type", "Tap Style", "Vanity Tap Style"} and re.match(r"^(?:Sink Mixer|Pull-Out Mixer|Basin Mixer)\b", text, re.IGNORECASE):
        return False
    if prefix in {"Vanity Inset Basin", "Basin"} and re.match(r"^Basin\b(?!\s*Mixer\b)", text, re.IGNORECASE):
        return False
    return True


def _looks_like_joinery_schedule_page(text: str) -> bool:
    lowered = normalize_space(text).lower()
    return any(hint in lowered for hint in JOINERY_PAGE_HINTS)


def _inject_schedule_heading_breaks(text: str) -> str:
    normalized = re.sub(
        rf"(?i)({ROOM_SCHEDULE_PATTERN})\s+colour\s+schedule(?=\s|[A-Z]|$)",
        lambda match: f"\n{normalize_space(match.group(0))}\n",
        text,
    )
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized


def _looks_like_non_joinery_room_label(label: str) -> bool:
    text = normalize_space(label)
    if not text:
        return False
    return any(re.search(pattern, text) for pattern in ROOM_LIKE_NOISE_PATTERNS)


def _is_schedule_room_heading(line: str) -> bool:
    text = normalize_space(line)
    if not text or _looks_like_field_label(text) or len(text) > 80:
        return False
    lowered = text.lower()
    if not any(token in lowered for token in ("kitchen", "pantry", "laundry", "ensuite", "bathroom", "vanity", "vanities", "powder", "butler", "wip", "walk", "theatre", "rumpus", "study", "office", "kitchenette", "meals", "family")):
        return False
    stripped = re.sub(r"[^A-Za-z0-9 ]+", "", text)
    return bool(stripped and stripped == stripped.upper())


def _schedule_room_key(line: str) -> str:
    return source_room_key(line)


STRUCTURE_ROOM_NOISE_PATTERNS: tuple[str, ...] = (
    r"(?i)\baddress\b",
    r"(?i)\bclient initials?\b",
    r"(?i)\binitials?\b",
    r"(?i)\bclient\b",
    r"(?i)\bdate\b",
    r"(?i)\bsignature\b",
    r"(?i)\bdesigner\b",
    r"(?i)\bjob number\b",
    r"(?i)\bmanufacturer\b",
    r"(?i)\bsupplier\b",
    r"(?i)\bmodel\b",
    r"(?i)\bframe\b",
    r"(?i)\bsliding\b",
    r"(?i)\bpanel(?:s)?\b",
    r"(?i)\bdoor(?:s)?\b",
    r"(?i)\bcolour\b",
    r"(?i)\bhandles?\b",
    r"(?i)\bkickboards?\b",
    r"(?i)\bbenchtop(?:s)?\b",
    r"(?i)\bcabinet(?:ry)?\b",
    r"(?i)\bspecification\b",
    r"(?i)\bselections?\b",
    r"(?i)\bdisclaimer\b",
    r"(?i)\bfooter\b",
    r"(?i)\btowel\b",
    r"(?i)\bhooks?\b",
    r"(?i)\bref\.?\s*number\b",
    r"(?i)\bdocument ref\b",
    r"(?i)\bselection required\b",
)

STRUCTURE_ROOM_SUFFIX_TRIM_PATTERNS: tuple[str, ...] = (
    r"(?i)\b(?:sink(?:ware)?|tap(?:ware)?|basin(?:\s+tapware)?|feature waste|waste|shower base|shower mixer|shower rose|bath/spa bath|bath tapware|toilet suite|toilet roll holder|mirrors?)\b.*$",
    r"(?i)\bshower\b.*$",
    r"(?i)\bbath/spa\b.*$",
    r"(?i)\bsliding\s+type\b.*$",
    r"(?i)\btype\s+frameless\b.*$",
    r"(?i)\bselection\s+required\b.*$",
    r"(?i)\bnot\s+applicable\b.*$",
    r"(?i)\bdesk\b.*$",
    r"(?i)\bdoors?\b.*$",
)


def _page_has_layout_schema(page: dict[str, object]) -> bool:
    layout = page.get("page_layout")
    return isinstance(layout, dict) and bool(
        layout.get("room_blocks") or layout.get("rows") or layout.get("room_label") or layout.get("section_label")
    )


def _document_has_layout_schema(document: dict[str, object]) -> bool:
    return any(_page_has_layout_schema(page) for page in document.get("pages", []))


def _layout_row_to_text(raw_row: Any) -> str:
    if not isinstance(raw_row, dict):
        return ""
    row_kind = normalize_space(str(raw_row.get("row_kind", "") or "")).lower().replace(" ", "_")
    if row_kind in {"metadata", "footer"}:
        return ""
    label = normalize_space(str(raw_row.get("row_label", "") or ""))
    value_bits = [
        normalize_space(str(raw_row.get("value_region_text", "") or "")),
        normalize_space(str(raw_row.get("supplier_region_text", "") or "")),
        normalize_space(str(raw_row.get("notes_region_text", "") or "")),
    ]
    value_text = normalize_space(" ".join(bit for bit in value_bits if bit))
    if label and value_text:
        return f"{label} {value_text}"
    return label or value_text


def _layout_row_record(raw_row: Any, page_no: int, room_identity: str = "") -> dict[str, Any] | None:
    if not isinstance(raw_row, dict):
        return None
    row_kind = normalize_space(str(raw_row.get("row_kind", "") or "")).lower().replace(" ", "_")
    if row_kind in {"metadata", "footer"}:
        return None
    return {
        "page_no": page_no,
        "room_identity": same_room_identity(room_identity),
        "row_label": normalize_space(str(raw_row.get("row_label", "") or "")),
        "row_kind": row_kind,
        "value_text": normalize_space(str(raw_row.get("value_region_text", "") or "")),
        "supplier_text": normalize_space(str(raw_row.get("supplier_region_text", "") or "")),
        "notes_text": normalize_space(str(raw_row.get("notes_region_text", "") or "")),
    }


def _layout_row_to_line(raw_row: Any) -> str:
    if not isinstance(raw_row, dict):
        return ""
    label = normalize_space(str(raw_row.get("row_label", "") or ""))
    value = normalize_space(str(raw_row.get("value_text", raw_row.get("value_region_text", "")) or ""))
    supplier = normalize_space(str(raw_row.get("supplier_text", raw_row.get("supplier_region_text", "")) or ""))
    notes = normalize_space(str(raw_row.get("notes_text", raw_row.get("notes_region_text", "")) or ""))
    row_kind = normalize_space(str(raw_row.get("row_kind", "") or "")).lower().replace(" ", "_")
    if row_kind in {"metadata", "footer"}:
        return ""
    tail = normalize_space(" ".join(part for part in (value, supplier, notes) if part))
    if label and tail:
        return f"{label} {tail}"
    return label or tail


def _section_layout_rows(section: dict[str, Any]) -> list[dict[str, Any]]:
    rows = section.get("layout_rows", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _section_lines(section: dict[str, Any]) -> list[str]:
    layout_rows = _section_layout_rows(section)
    if layout_rows:
        return [line for row in layout_rows for line in [_layout_row_to_line(row)] if line]
    return _preprocess_chunk(str(section.get("text", "") or ""))


def _looks_like_structured_room_noise(label: str) -> bool:
    text = normalize_space(label)
    if not text:
        return True
    if re.match(r"^\d+\b", text):
        return True
    if _looks_like_field_label(text):
        return True
    if re.search(r"(?i)\b(?:joinery selection sheet|colour schedule)\b", text):
        return False
    return any(re.search(pattern, text) for pattern in STRUCTURE_ROOM_NOISE_PATTERNS)


def _clean_layout_room_label(
    room_label: str,
    section_label: str = "",
    fallback_key: str = "",
) -> str:
    candidates = [room_label, section_label]
    for candidate in candidates:
        text = normalize_space(str(candidate or ""))
        if not text:
            continue
        exact_specific = _extract_specific_room_heading(text)
        if exact_specific:
            return exact_specific
        for pattern in STRUCTURE_ROOM_SUFFIX_TRIM_PATTERNS:
            text = normalize_space(re.sub(pattern, "", text))
        if not text:
            continue
        cleaned = source_room_label(text, fallback_key=fallback_key)
        if not cleaned or cleaned == "Room":
            alias_cleaned = _extract_layout_room_alias(text)
            if alias_cleaned:
                cleaned = alias_cleaned
            else:
                continue
        elif _looks_like_structured_room_noise(cleaned):
            alias_cleaned = _extract_layout_room_alias(text)
            if alias_cleaned:
                cleaned = alias_cleaned
            else:
                continue
        return cleaned
    return ""


def _looks_like_plausible_room_label(text: str) -> bool:
    cleaned = _clean_layout_room_label(text)
    if not cleaned or cleaned == "Room":
        return False
    if _looks_like_structured_room_noise(cleaned):
        return False
    if _extract_layout_room_alias(cleaned):
        return True
    if _extract_room_prefix_label(text):
        return True
    lowered = cleaned.lower()
    return any(
        token in lowered
        for token in (
            "kitchen",
            "pantry",
            "laundry",
            "bathroom",
            "ensuite",
            "powder",
            "wc",
            "alfresco",
            "study",
            "office",
            "desk",
            "robe",
            "banquette",
            "butlers",
            "wip",
        )
    )


def _clone_layout_row(raw_row: dict[str, Any], row_label: str = "") -> dict[str, Any]:
    return {
        "row_label": normalize_space(row_label or str(raw_row.get("row_label", "") or "")),
        "value_region_text": normalize_space(str(raw_row.get("value_region_text", "") or "")),
        "supplier_region_text": normalize_space(str(raw_row.get("supplier_region_text", "") or "")),
        "notes_region_text": normalize_space(str(raw_row.get("notes_region_text", "") or "")),
        "row_kind": normalize_space(str(raw_row.get("row_kind", "") or "")).lower().replace(" ", "_") or "other",
    }


def _layout_row_has_meaningful_content(raw_row: dict[str, Any]) -> bool:
    return any(
        normalize_space(str(raw_row.get(field_name, "") or ""))
        for field_name in ("row_label", "value_region_text", "supplier_region_text", "notes_region_text")
    )


def _search_room_reference(text: str) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    cleaned = _clean_layout_room_label(normalized)
    if cleaned and _looks_like_plausible_room_label(cleaned):
        return cleaned
    for pattern, label in ROOM_PREFIX_PATTERN_SPECS:
        if re.search(rf"(?i)\b(?:room\s+)?{pattern}\b", normalized):
            candidate = _clean_layout_room_label(label)
            if candidate and _looks_like_plausible_room_label(candidate):
                return candidate
    return ""


def _room_prefix_tail_allows_room_split(tail: str, *, page_type: str, row_kind: str = "") -> bool:
    normalized_tail = normalize_space(tail).strip(" -:/,;()").lower()
    if not normalized_tail:
        return True
    if normalized_tail in ROOM_PREFIX_SPLIT_EXCLUDED_TAILS:
        return False
    if page_type == "sinkware_tapware":
        return (
            normalized_tail in ROOM_PREFIX_SPLIT_FIXTURE_TAILS
            or row_kind in {"sink", "tap", "basin", "accessory"}
            or _looks_like_room_field_tail(normalized_tail)
        )
    if page_type == "joinery":
        return normalized_tail in ROOM_PREFIX_SPLIT_JOINERY_TAILS or normalized_tail in ROOM_PREFIX_SPLIT_FIXTURE_TAILS
    return _looks_like_room_field_tail(normalized_tail)


def _infer_default_layout_room_label(
    *,
    room_label: str,
    section_label: str,
    raw_page_text: str,
    rows: list[dict[str, Any]],
    page_type: str,
) -> str:
    explicit = _clean_layout_room_label(room_label, section_label)
    if explicit and _looks_like_plausible_room_label(explicit):
        return explicit
    lines = [normalize_space(line) for line in str(raw_page_text or "").replace("\r", "\n").split("\n") if normalize_space(line)]
    for line in lines[:24]:
        if len(line.split()) > 8:
            continue
        candidate = _search_room_reference(line)
        if candidate:
            return candidate
    for raw_row in rows[:18]:
        if not isinstance(raw_row, dict):
            continue
        row_label = normalize_space(str(raw_row.get("row_label", "") or ""))
        row_kind = normalize_space(str(raw_row.get("row_kind", "") or "")).lower().replace(" ", "_")
        if row_label:
            prefix_label, remainder = _extract_room_prefix_parts(row_label)
            if prefix_label and _room_prefix_tail_allows_room_split(remainder, page_type=page_type, row_kind=row_kind):
                candidate = _clean_layout_room_label(prefix_label)
                if candidate and _looks_like_plausible_room_label(candidate):
                    return candidate
            # Property rows like "Pantry Door Handle" should never steal the page's
            # default room identity just because they contain a known room token.
            if not (prefix_label and remainder):
                candidate = _search_room_reference(row_label)
                if candidate:
                    return candidate
    return ""


def _looks_like_inline_room_heading_row(raw_row: dict[str, Any]) -> bool:
    label = normalize_space(str(raw_row.get("row_label", "") or ""))
    if not label:
        return False
    cleaned = _clean_layout_room_label(label)
    if not cleaned or not _looks_like_plausible_room_label(cleaned):
        return False
    prefix_label, remainder = _extract_room_prefix_parts(label)
    if prefix_label and remainder:
        return False
    value_text = normalize_space(str(raw_row.get("value_region_text", "") or ""))
    supplier_text = normalize_space(str(raw_row.get("supplier_region_text", "") or ""))
    notes_text = normalize_space(str(raw_row.get("notes_region_text", "") or ""))
    if supplier_text or notes_text:
        return False
    return not value_text or value_text.lower() in {label.lower(), cleaned.lower()}


def _split_layout_rows_by_inline_room_headings(
    rows: list[dict[str, Any]],
    *,
    default_room_label: str = "",
) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    current_room = _clean_layout_room_label(default_room_label)
    if current_room:
        grouped.append({"room_label": current_room, "rows": []})
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        if _looks_like_inline_room_heading_row(raw_row):
            current_room = _clean_layout_room_label(str(raw_row.get("row_label", "") or ""))
            if not current_room:
                continue
            if not grouped or grouped[-1].get("room_label") != current_room:
                grouped.append({"room_label": current_room, "rows": []})
            continue
        if current_room and grouped:
            grouped[-1]["rows"].append(_clone_layout_row(raw_row))
    return [block for block in grouped if block.get("room_label") and block.get("rows")]


def _split_layout_rows_by_room_prefix(
    rows: list[dict[str, Any]],
    *,
    page_type: str = "",
    default_room_label: str = "",
) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    current_room = _clean_layout_room_label(default_room_label)
    if current_room:
        grouped.append({"room_label": current_room, "rows": []})
    for raw_row in rows:
        label = normalize_space(str(raw_row.get("row_label", "") or ""))
        prefix_label, remainder = _extract_room_prefix_parts(label)
        row_kind = normalize_space(str(raw_row.get("row_kind", "") or "")).lower().replace(" ", "_")
        if prefix_label and _room_prefix_tail_allows_room_split(remainder, page_type=page_type, row_kind=row_kind):
            current_room = source_room_label(prefix_label, fallback_key=source_room_key(prefix_label))
            if not grouped or grouped[-1].get("room_label") != current_room:
                grouped.append({"room_label": current_room, "rows": []})
            adjusted_row = _clone_layout_row(raw_row, row_label=remainder)
            if _layout_row_has_meaningful_content(adjusted_row):
                grouped[-1]["rows"].append(adjusted_row)
            continue
        if current_room and grouped:
            grouped[-1]["rows"].append(_clone_layout_row(raw_row))
    return [block for block in grouped if block.get("room_label") and block.get("rows")]


def _page_layout_rows(layout: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for block in layout.get("room_blocks", []) or []:
        if isinstance(block, dict):
            rows.extend([row for row in block.get("rows", []) if isinstance(row, dict)])
    rows.extend([row for row in layout.get("rows", []) or [] if isinstance(row, dict)])
    return rows


def _effective_layout_page_type(builder_name: str, page_type: str, raw_page_text: str, layout: dict[str, Any]) -> str:
    if page_type in {"joinery", "sinkware_tapware", "appliance", "special"}:
        return page_type
    upper = raw_page_text.upper()
    if _is_imperial_builder(builder_name) and _extract_imperial_section_title(raw_page_text):
        return "joinery"
    if "SINKWARE & TAPWARE" in upper or "PLUMBING FIXTURES & TAPWARE" in upper:
        return "sinkware_tapware"
    if "APPLIANCES" in upper:
        return "appliance"
    if any(marker in upper for marker in ("JOINERY SELECTION SHEET", "COLOUR SCHEDULE", " CABINETS")):
        return "joinery"
    row_kinds = {normalize_space(str(row.get("row_kind", "") or "")).lower().replace(" ", "_") for row in _page_layout_rows(layout)}
    if row_kinds & {"sink", "tap", "basin"}:
        return "sinkware_tapware"
    if any(_looks_like_plausible_room_label(str(block.get("room_label", "") or "")) for block in layout.get("room_blocks", []) if isinstance(block, dict)):
        return "joinery"
    return page_type


def _coerce_layout_room_blocks(
    layout: dict[str, Any],
    section_label: str,
    room_label: str,
    *,
    raw_page_text: str = "",
    page_type: str = "",
) -> list[dict[str, Any]]:
    room_blocks = layout.get("room_blocks", [])
    if not isinstance(room_blocks, list) or not room_blocks:
        room_blocks = [{"room_label": room_label, "rows": layout.get("rows", [])}]
    explicit_plausible_blocks_exist = any(
        isinstance(block, dict)
        and _looks_like_plausible_room_label(
            _clean_layout_room_label(
                normalize_space(str(block.get("room_label", "") or "")),
                section_label,
            )
        )
        for block in room_blocks
    )
    derived_blocks: list[dict[str, Any]] = []
    for block in room_blocks:
        if not isinstance(block, dict):
            continue
        block_rows = [row for row in block.get("rows", []) if isinstance(row, dict)]
        block_label = normalize_space(str(block.get("room_label", "") or ""))
        cleaned_label = _clean_layout_room_label(block_label, section_label)
        if cleaned_label and _looks_like_plausible_room_label(cleaned_label):
            derived_blocks.append({"room_label": cleaned_label, "rows": [_clone_layout_row(row) for row in block_rows]})
            continue
        if (
            explicit_plausible_blocks_exist
            and block_label
            and (not cleaned_label or _looks_like_structured_room_noise(cleaned_label or block_label))
        ):
            # When the merged layout already carries explicit room blocks, ignore
            # extra metadata/header blocks such as "Client Initials" or "Ref. Number"
            # instead of trying to re-split them into duplicate noisy room sections.
            continue
        default_room_label = _infer_default_layout_room_label(
            room_label=room_label,
            section_label=section_label,
            raw_page_text=raw_page_text,
            rows=block_rows,
            page_type=page_type,
        )
        prefixed_blocks = _split_layout_rows_by_room_prefix(
            block_rows,
            page_type=page_type,
            default_room_label=default_room_label,
        )
        inline_blocks = _split_layout_rows_by_inline_room_headings(block_rows, default_room_label=default_room_label)
        prefixed_row_count = sum(len(block.get("rows", [])) for block in prefixed_blocks)
        inline_row_count = sum(len(block.get("rows", [])) for block in inline_blocks)
        if inline_blocks and (
            not prefixed_blocks
            or len(inline_blocks) > len(prefixed_blocks)
            or (len(inline_blocks) == len(prefixed_blocks) and inline_row_count < prefixed_row_count)
        ):
            derived_blocks.extend(inline_blocks)
            continue
        if prefixed_blocks:
            derived_blocks.extend(prefixed_blocks)
            continue
        if inline_blocks:
            derived_blocks.extend(inline_blocks)
            continue
        if not block_label and room_label and _looks_like_plausible_room_label(room_label):
            derived_blocks.append({"room_label": _clean_layout_room_label(room_label, section_label), "rows": [_clone_layout_row(row) for row in block_rows]})
            continue
        if default_room_label and block_rows:
            derived_blocks.append({"room_label": default_room_label, "rows": [_clone_layout_row(row) for row in block_rows]})
    merged_blocks: list[dict[str, Any]] = []
    for block in derived_blocks:
        room_name = normalize_space(str(block.get("room_label", "") or ""))
        block_rows = [row for row in block.get("rows", []) if isinstance(row, dict)]
        if not room_name or not block_rows:
            continue
        existing = next((item for item in merged_blocks if item.get("room_label") == room_name), None)
        if existing is None:
            merged_blocks.append({"room_label": room_name, "rows": [_clone_layout_row(row) for row in block_rows]})
            continue
        existing["rows"].extend(_clone_layout_row(row) for row in block_rows)
    return [block for block in merged_blocks if block.get("room_label") and block.get("rows")]


def _extract_layout_room_alias(text: str) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    lowered = normalized.lower()
    for room_key, aliases in sorted(ROOM_ALIASES.items(), key=lambda item: max(len(alias) for alias in item[1]), reverse=True):
        for alias in sorted(aliases, key=len, reverse=True):
            if not re.search(rf"(?i)\b{re.escape(alias)}\b", lowered):
                continue
            label = source_room_label(alias, fallback_key=room_key)
            if label and label != "Room":
                return label
    return ""


def _layout_section_seed(
    file_name: str,
    page_no: int,
    section_label: str,
    room_label: str,
    rows: list[dict[str, Any]],
    page_type: str,
    raw_page_text: str = "",
    section_kind: str = "room",
) -> dict[str, Any] | None:
    rendered_rows = [line for raw_row in rows for line in [_layout_row_to_text(raw_row)] if line]
    layout_rows = [
        record
        for raw_row in rows
        for record in [_layout_row_record(raw_row, page_no=page_no, room_identity=room_label or section_label)]
        if record
    ]
    normalized_room_label = _clean_layout_room_label(room_label, section_label)
    if section_kind == "room" and (not normalized_room_label or not _looks_like_plausible_room_label(normalized_room_label)):
        return None
    title_line = normalized_room_label or normalize_space(section_label)
    if not title_line:
        return None
    text_parts = [title_line, *rendered_rows]
    section_text = normalize_space("\n".join(part for part in text_parts if part))
    if not section_text:
        return None
    if section_kind == "special":
        section_key = re.sub(r"[^a-z0-9]+", "_", title_line.lower()).strip("_") or "special_section"
    else:
        section_key = source_room_key(normalized_room_label, fallback_key=normalize_room_key(normalized_room_label))
    return {
        "section_key": section_key,
        "original_section_label": normalized_room_label or title_line,
        "section_kind": section_kind,
        "file_name": file_name,
        "page_nos": [page_no],
        "page_texts": [{"page_no": page_no, "text": section_text}],
        "raw_page_texts": [{"page_no": page_no, "text": raw_page_text}] if normalize_space(raw_page_text) else [],
        "layout_rows": layout_rows,
        "text_parts": [section_text],
        "text": section_text,
        "page_type": page_type,
    }


def _append_section(sections: list[dict[str, Any]], section: dict[str, Any]) -> None:
    for existing in sections:
        if (
            existing.get("section_key") == section.get("section_key")
            and existing.get("section_kind") == section.get("section_kind")
            and existing.get("file_name") == section.get("file_name")
            and existing.get("page_type") == section.get("page_type")
        ):
            existing["page_nos"] = _unique([*existing.get("page_nos", []), *section.get("page_nos", [])])
            existing.setdefault("page_texts", []).extend(section.get("page_texts", []))
            existing.setdefault("raw_page_texts", []).extend(section.get("raw_page_texts", []))
            existing.setdefault("layout_rows", []).extend(section.get("layout_rows", []))
            existing.setdefault("text_parts", []).extend(section.get("text_parts", []))
            existing["text"] = normalize_space("\n".join(existing.get("text_parts", [])))
            if section.get("original_section_label"):
                existing["original_section_label"] = str(section.get("original_section_label", ""))
            return
    sections.append(section)


def _collect_layout_sections_for_document(document: dict[str, object]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    file_name = str(document.get("file_name", "") or "")
    builder_name = str(document.get("builder_name", "") or "")
    for page in document.get("pages", []):
        if not _page_has_layout_schema(page):
            continue
        layout = dict(page.get("page_layout") or {})
        raw_page_text = str(page.get("raw_text") or page.get("text") or "")
        page_type = _effective_layout_page_type(
            builder_name=builder_name,
            page_type=normalize_space(str(layout.get("page_type", "") or "")).lower().replace(" ", "_"),
            raw_page_text=raw_page_text,
            layout=layout,
        )
        section_label = normalize_space(str(layout.get("section_label", "") or ""))
        room_label = normalize_space(str(layout.get("room_label", "") or ""))
        page_no = int(page.get("page_no", 0) or 0)
        extracted_title = _extract_imperial_section_title(raw_page_text)
        extracted_room_label = _imperial_section_label(extracted_title) if extracted_title else ""
        if extracted_room_label:
            page_type = "joinery"
            section_label = extracted_room_label
            room_label = extracted_room_label
        if page_type not in {"joinery", "sinkware_tapware", "special"}:
            continue
        room_blocks = _coerce_layout_room_blocks(
            layout,
            section_label=section_label,
            room_label=room_label,
            raw_page_text=raw_page_text,
            page_type=page_type,
        )
        if page_type == "special":
            special_rows = [row for row in layout.get("rows", []) if isinstance(row, dict)]
            section = _layout_section_seed(
                file_name=file_name,
                page_no=page_no,
                section_label=section_label or room_label,
                room_label=section_label or room_label,
                rows=special_rows,
                page_type=page_type,
                raw_page_text=raw_page_text,
                section_kind="special",
            )
            if section:
                _append_section(sections, section)
            continue
        if page_type == "sinkware_tapware" and not room_blocks:
            room_blocks = [{"room_label": room_label, "rows": [row for row in layout.get("rows", []) if isinstance(row, dict)]}]
        for block in room_blocks:
            if not isinstance(block, dict):
                continue
            block_label = normalize_space(str(block.get("room_label", "") or ""))
            if extracted_room_label:
                block_label = extracted_room_label
            block_rows = [row for row in block.get("rows", []) if isinstance(row, dict)]
            section = _layout_section_seed(
                file_name=file_name,
                page_no=page_no,
                section_label=section_label,
                room_label=block_label or room_label,
                rows=block_rows,
                page_type=page_type,
                raw_page_text=raw_page_text,
            )
            if section:
                _append_section(sections, section)
    return sections


def _collect_room_sections_for_document(document: dict[str, object]) -> list[dict[str, Any]]:
    layout_sections = _collect_layout_sections_for_document(document)
    if layout_sections:
        return [section for section in layout_sections if section.get("section_kind") == "room"]
    full_text = _document_full_text(document)
    text_sections = _collect_schedule_room_sections([document]) or _find_room_sections(full_text)
    sections: list[dict[str, Any]] = []
    file_name = str(document.get("file_name", "") or "")
    pages = list(document.get("pages", []))
    for detected_room_key, chunk in text_sections:
        room_label = source_room_label(chunk.split("\n", 1)[0], fallback_key=detected_room_key)
        section = {
            "section_key": source_room_key(room_label, fallback_key=detected_room_key),
            "original_section_label": room_label,
            "section_kind": "room",
            "file_name": file_name,
            "page_nos": [int(page.get("page_no", 0) or 0) for page in pages if page.get("page_no")],
            "page_texts": [{"page_no": int(page.get("page_no", 0) or 0), "text": chunk} for page in pages if page.get("page_no")],
            "text_parts": [chunk],
            "text": normalize_space(chunk),
            "page_type": "joinery",
        }
        sections.append(section)
    return sections


def _collect_schedule_room_sections(documents: list[dict[str, object]]) -> list[tuple[str, str]]:
    sections: list[tuple[str, str]] = []
    current_key = ""
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_key, current_lines
        if current_key and current_lines:
            sections.append((current_key, normalize_space("\n".join(current_lines))))
        current_key = ""
        current_lines = []

    for document in documents:
        for page in document.get("pages", []):
            page_text = _inject_schedule_heading_breaks(str(page.get("text") or ""))
            if not _looks_like_joinery_schedule_page(page_text):
                continue
            for line in _preprocess_chunk(page_text):
                if not line or _skip_continuation_line(line):
                    continue
                if _is_schedule_room_heading(line):
                    candidate_key = _schedule_room_key(line)
                    lowered_line = line.lower()
                    if (
                        current_key
                        and "colour schedule" not in lowered_line
                        and candidate_key
                        and (
                            candidate_key == current_key
                            or current_key.endswith(candidate_key)
                            or candidate_key.endswith(current_key)
                        )
                    ):
                        current_lines.append(line)
                        continue
                    flush()
                    current_key = candidate_key
                    current_lines = [line]
                    continue
                if current_key:
                    current_lines.append(line)
    flush()
    return sections


def _document_full_text(document: dict[str, object]) -> str:
    return "\n\n".join(str(page["text"]) for page in document.get("pages", []) if page.get("text"))


def _document_room_master_score(document: dict[str, object]) -> dict[str, Any]:
    pages = list(document.get("pages", []))
    full_text = _document_full_text(document)
    file_name = normalize_space(str(document.get("file_name", ""))).lower()
    if _document_has_layout_schema(document):
        schedule_sections = [
            (str(section.get("section_key", "")), str(section.get("text", "")))
            for section in _collect_layout_sections_for_document(document)
            if section.get("section_kind") == "room"
        ]
    else:
        schedule_sections = _collect_schedule_room_sections([document])
    schedule_pages = sum(1 for page in pages if _looks_like_joinery_schedule_page(str(page.get("text") or "")))
    colour_schedule_hits = len(re.findall(r"(?i)\bcolour schedule\b", full_text))
    cabinetry_field_hits = len(
        re.findall(
            r"(?i)\b(?:overhead cupboards|base cupboards\s*&\s*drawers|floor mounted vanity|back benchtops|island benchtop|island bench base cupboards\s*&\s*drawers|island bar back)\b",
            full_text,
        )
    )
    room_heading_hits = sum(
        1
        for page in pages
        for line in _preprocess_chunk(str(page.get("text") or ""))
        if _is_schedule_room_heading(line)
    )
    generic_sections = len(_find_room_sections(full_text))
    looks_like_cabinetry_schedule = bool(schedule_sections) or cabinetry_field_hits >= 2 or (colour_schedule_hits > 0 and cabinetry_field_hits > 0)
    filename_boost = 0
    if looks_like_cabinetry_schedule:
        if re.search(r"(?i)\bcolou?rs?\s*afc\b", file_name):
            filename_boost += 220
        elif re.search(r"(?i)\bcolou?rs?\b", file_name):
            filename_boost += 120
        elif re.search(r"(?i)\bcolour\s+schedule\b", file_name):
            filename_boost += 120
    if re.search(r"(?i)\bdrawings?\b", file_name):
        filename_boost -= 35
    score = (
        filename_boost
        + len(schedule_sections) * 40
        + schedule_pages * 25
        + colour_schedule_hits * 10
        + cabinetry_field_hits * 18
        + room_heading_hits * 6
        + generic_sections
    )
    reason = (
        f"filename boost {filename_boost}, "
        f"{len(schedule_sections)} schedule sections, "
        f"{schedule_pages} schedule page(s), "
        f"{colour_schedule_hits} colour-schedule hit(s), "
        f"{cabinetry_field_hits} cabinetry-field hit(s)"
    )
    return {
        "score": score,
        "reason": reason,
        "schedule_sections": schedule_sections,
        "generic_sections": generic_sections,
    }


def select_room_master_document(documents: list[dict[str, object]], source_kind: str) -> tuple[dict[str, object] | None, str]:
    if source_kind != "spec" or len(documents) <= 1:
        return None, ""
    best_document: dict[str, object] | None = None
    best_reason = ""
    best_score = -1
    for document in documents:
        metrics = _document_room_master_score(document)
        if metrics["score"] > best_score:
            best_document = document
            best_score = int(metrics["score"])
            best_reason = f"{document['file_name']} selected as room master by schedule density ({metrics['reason']})."
    return best_document, best_reason


def _map_to_existing_master_room(detected_room_key: str, master_room_keys: set[str]) -> str:
    if detected_room_key in master_room_keys:
        return detected_room_key
    if "vanities" in master_room_keys and any(token in detected_room_key for token in ("vanity", "bathroom", "ensuite", "powder")):
        return "vanities"
    return ""


def _resolve_room_target(
    detected_room_key: str,
    original_room_label: str,
    room_master_keys: set[str],
    is_room_master: bool,
) -> tuple[str, str]:
    if is_room_master or not room_master_keys:
        return detected_room_key, ""
    mapped = _map_to_existing_master_room(detected_room_key, room_master_keys)
    if mapped:
        return mapped, ""
    if _looks_like_non_joinery_room_label(original_room_label):
        return "", "non-joinery/non-fixture noise"
    return "", "not in room master"


def _is_imperial_builder(builder_name: str) -> bool:
    return "imperial" in normalize_space(builder_name).lower()


def _select_imperial_room_master_document(documents: list[dict[str, object]]) -> tuple[dict[str, object] | None, str]:
    if not documents:
        return None, ""
    if len(documents) == 1:
        document = documents[0]
        return document, f"{document['file_name']} selected as room master for Imperial single-file parse."
    best_document: dict[str, object] | None = None
    best_score = -1
    best_reason = ""
    for document in documents:
        section_count = len(_collect_imperial_sections_for_document(document))
        score = section_count * 100
        if score > best_score:
            best_document = document
            best_score = score
            best_reason = f"{document['file_name']} selected as room master by Imperial title count ({section_count} section title(s))."
    return best_document, best_reason


def _collect_imperial_sections_for_document(document: dict[str, object]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def flush() -> None:
        nonlocal current
        if not current:
            return
        text_parts = [part for part in current.get("text_parts", []) if normalize_space(part)]
        section_text = normalize_space("\n".join(text_parts))
        if section_text:
            current["text"] = section_text
            sections.append(current)
        current = None

    def page_layout_rows(page: dict[str, object], room_identity: str) -> list[dict[str, Any]]:
        if str(page.get("layout_mode", "") or "") == "lightweight" and not bool(page.get("vision_applied")):
            return []
        layout = dict(page.get("page_layout") or {})
        records: list[dict[str, Any]] = []
        for raw_row in _page_layout_rows(layout):
            record = _layout_row_record(raw_row, page_no=int(page.get("page_no", 0) or 0), room_identity=room_identity)
            if record:
                records.append(record)
        return records

    def looks_like_layout_continuation(page: dict[str, object]) -> bool:
        for row in page_layout_rows(page, room_identity=str(current.get("original_section_label", "")) if current else ""):
            label = normalize_space(str(row.get("row_label", "") or ""))
            if not label:
                continue
            upper = label.upper()
            if _imperial_match_field_label(label)[0]:
                return True
            if any(
                token in upper
                for token in (
                    "CABINETRY COLOUR",
                    "GLASS INLAY",
                    "FLOATING SHELV",
                    "KICKBOARD",
                    "HANDLES",
                    "BENCHTOP",
                    "SPLASHBACK",
                    "SINKWARE",
                    "TAPWARE",
                )
            ):
                return True
        return False

    for page in document.get("pages", []):
        raw_text = str(page.get("raw_text") or page.get("text") or "")
        if not raw_text.strip():
            continue
        title = _extract_imperial_section_title(raw_text)
        trimmed_text = _trim_imperial_page_text(raw_text, title)
        if title:
            flush()
            section_label = _imperial_section_label(title)
            section_kind = "special" if section_label in IMPERIAL_SPECIAL_SECTION_TITLES else "room"
            section_key = _imperial_section_key(section_label, section_kind)
            current = {
                "section_key": section_key,
                "original_section_label": section_label,
                "section_kind": section_kind,
                "file_name": str(document.get("file_name", "")),
                "page_nos": [int(page.get("page_no", 0) or 0)],
                "page_texts": [{"page_no": int(page.get("page_no", 0) or 0), "text": trimmed_text}],
                "raw_page_texts": [{"page_no": int(page.get("page_no", 0) or 0), "text": raw_text}],
                "layout_rows": page_layout_rows(page, room_identity=section_label),
                "text_parts": [trimmed_text] if trimmed_text else [],
            }
            continue
        if current and _is_imperial_non_joinery_page(raw_text):
            flush()
            continue
        if current and not (_looks_like_imperial_continuation_page(trimmed_text) or looks_like_layout_continuation(page)):
            flush()
            continue
        if current:
            current["page_nos"].append(int(page.get("page_no", 0) or 0))
            if trimmed_text:
                current.setdefault("page_texts", []).append({"page_no": int(page.get("page_no", 0) or 0), "text": trimmed_text})
                current.setdefault("text_parts", []).append(trimmed_text)
            current.setdefault("raw_page_texts", []).append({"page_no": int(page.get("page_no", 0) or 0), "text": raw_text})
            current.setdefault("layout_rows", []).extend(page_layout_rows(page, room_identity=str(current.get("original_section_label", ""))))
    flush()
    return sections


def _extract_imperial_section_title(text: str) -> str:
    match = IMPERIAL_SECTION_TITLE_RE.search(text)
    if not match:
        return ""
    title = normalize_space(match.group("title"))
    prefix_window = text[max(0, match.start() - 120) : match.start()]
    prefix_lines = [normalize_space(line) for line in prefix_window.replace("\r", "\n").split("\n") if normalize_space(line)]
    if prefix_lines:
        prefix_candidate = prefix_lines[-1]
        if _looks_like_imperial_title_prefix(prefix_candidate):
            title = normalize_space(f"{prefix_candidate} {title}")
    return title


def _imperial_section_label(title: str) -> str:
    return normalize_space(re.sub(r"(?i)\s+joinery selection sheet\b", "", title)).strip(" -")


def _imperial_section_key(label: str, section_kind: str) -> str:
    if section_kind == "special":
        return re.sub(r"[^a-z0-9]+", "_", normalize_space(label).lower()).strip("_") or "special_section"
    lowered = normalize_space(label).lower()
    if lowered == "bath + ensuite":
        return "bath_ensuite"
    if any(token in lowered for token in (" & ", "/", " and ")):
        return re.sub(r"[^a-z0-9]+", "_", lowered).strip("_") or "room"
    return source_room_key(label)


def _trim_imperial_page_text(text: str, title: str = "") -> str:
    working = text.replace("\r", "\n")
    if title:
        working = re.sub(r"(?im)^.*JOINERY SELECTION SHEET.*$", "", working, count=1)
    lines = [normalize_space(line) for line in working.split("\n") if normalize_space(line)]
    cleaned_lines: list[str] = []
    for line in lines:
        if _is_imperial_page_noise_line(line):
            continue
        if cleaned_lines and _is_imperial_full_page_break(line):
            break
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def _looks_like_imperial_title_prefix(line: str) -> bool:
    text = normalize_space(line)
    if not text or ":" in text or len(text) > 40:
        return False
    if text.upper() in {"NA", "N/A", "REF", "REF.", "REF NUMBER", "IMAGE", "NOTES", "SUPPLIER", "CLIENT", "DATE", "ADDRESS", "DOCUMENT REF"}:
        return False
    if _imperial_match_field_label(text)[0]:
        return False
    if any(text.upper().startswith(prefix) for prefix in ("ADDRESS", "CLIENT", "DATE", "DOCUMENT REF", "PAGE ")):
        return False
    letters = re.sub(r"[^A-Za-z&/+ '\-]", "", text)
    return bool(letters and letters == letters.upper())


def _is_imperial_non_joinery_page(text: str) -> bool:
    if IMPERIAL_SECTION_TITLE_RE.search(text):
        return False
    lines = [normalize_space(line) for line in text.replace("\r", "\n").split("\n") if normalize_space(line)]
    for line in lines[:30]:
        if line.upper() in IMPERIAL_NON_JOINERY_HEADINGS:
            return True
    return False


def _is_imperial_page_noise_line(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return True
    if text.upper() == "IMAGE":
        return True
    if _clean_site_address_candidate(text):
        return True
    if any(text.startswith(marker) for marker in IMPERIAL_FOOTER_MARKERS):
        return True
    if re.match(r"(?i)^(address|client|date|document ref)\s*:", text):
        return True
    if re.match(r"(?i)^document ref\b.*$", text):
        return True
    if re.match(r"(?i)^page\s+\d+\s+of\s+\d+$", text):
        return True
    if re.match(r"(?i)^\d{1,2}\.\d{1,2}\.\d{2,4}$", text):
        return True
    if re.match(r"(?i)^\d{1,2}-\d{1,2}-\d{2,4}$", text):
        return True
    return False


def _looks_like_person_name_line(line: str) -> bool:
    text = normalize_space(line).strip(" -;,")
    if not text or re.search(r"\d", text):
        return False
    if re.search(r"(?i)\b(?:sink|tap|mixer|basin|drawer|handle|cabinet|laminex|polytec|caesarstone|franke|abey|alder|caroma|technika|abi interiors)\b", text):
        return False
    return bool(re.fullmatch(r"[A-Z][a-z]+(?:[ '-][A-Z][a-z]+){1,4}", text))


def _is_imperial_full_page_break(line: str) -> bool:
    text = normalize_space(line).upper()
    return text in IMPERIAL_NON_JOINERY_HEADINGS


def _is_useful_imperial_line(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return False
    if any(text.startswith(marker) for marker in IMPERIAL_HEADER_START_MARKERS):
        return True
    return _imperial_match_field_label(text)[0] != ""


def _looks_like_imperial_continuation_page(text: str) -> bool:
    raw_lines = [normalize_space(line) for line in text.split("\n") if normalize_space(line)]
    lines = _preprocess_imperial_lines(raw_lines)
    if not lines:
        return False
    return any(_is_useful_imperial_line(line) for line in lines[:40])


def _imperial_match_field_label(line: str) -> tuple[str, str]:
    text = normalize_space(line)
    for field_key, pattern in IMPERIAL_SECTION_FIELD_PATTERNS:
        match = re.match(rf"(?i)^{pattern}(?:\s*[:\-]?\s*(?P<tail>.*))?$", text)
        if match:
            return field_key, normalize_space(match.group("tail") or "")
    return "", ""


def _imperial_is_supplier_only_line(line: str) -> bool:
    normalized = normalize_space(normalize_brand_casing_text(line)).upper()
    return normalized in {normalize_space(item).upper() for item in IMPERIAL_SUPPLIER_ONLY_LINES}


ADDRESS_STREET_TYPE_PATTERN = (
    r"(?:street|st|road|rd|crescent|cres|avenue|ave|drive|dr|court|ct|place|pl|boulevard|blvd|lane|ln|terrace|tce|close|cl|way|parade|pde)"
)

SITE_ADDRESS_STOP_PATTERNS: tuple[str, ...] = (
    r"(?i)\bREV(?:ISION)?\b",
    r"(?i)\bJOB\s+NUMBER\b",
    r"(?i)\bJOB\s+NO\.?\b",
    r"(?i)\bKITCHEN COLOUR SCHEDULE\b",
    r"(?i)\bBENCHTOP(?:S)?\b",
    r"(?i)\bDOOR(?:/PANEL)? COLOUR(?:S)?\b",
    r"(?i)\bHANDLES?\b",
    r"(?i)\bTHERMOLAMINATE NOTES\b",
    r"(?i)\bDOOR HINGES\b",
    r"(?i)\bDRAWER RUNNERS\b",
    r"(?i)\bSUPPLIER DESCRIPTION DESIGN COMMENTS\b",
    r"(?i)\*\s*ALL DIMENSIONS\b",
    r"(?i)\bALL DIMENSIONS IN MILLIMETRES\b",
    r"(?i)\bDRAWINGS ARE INDICATIVE\b",
)

SITE_ADDRESS_REJECT_PATTERNS: tuple[str, ...] = (
    r"(?i)\b(?:client|date|document ref|designer|signature|signed date|private\b|all colours shown|subject to supplier)\b",
    r"(?i)\bTHERMOLAMINATE NOTES\b",
    r"(?i)\bHANDLE\s*\d+\b",
    r"(?i)\bDOOR HINGES\b",
    r"(?i)\bDRAWER RUNNERS\b",
    r"(?i)\bBENCHTOP(?:S)?\b",
    r"(?i)\bDOOR(?:/PANEL)? COLOUR(?:S)?\b",
)


def _extract_site_address_from_documents(documents: list[dict[str, object]]) -> str:
    ordered_documents = list(documents)
    for document in ordered_documents:
        pages = list(document.get("pages", []))
        for page in pages:
            for candidate_text in (page.get("raw_text", ""), page.get("text", "")):
                address = _extract_site_address_from_text(str(candidate_text or ""))
                if address:
                    return address
    return ""


def _extract_site_address_from_text(text: str) -> str:
    raw_text = str(text or "").replace("\r", "\n")
    lines = [normalize_space(line) for line in raw_text.splitlines() if normalize_space(line)]
    header_lines = lines[:24]
    header_window = "\n".join(header_lines)
    client_job_match = re.search(
        r"(?is)client\s+name\s*:\s*(?P<value>.+?)\s*job\s+address\s*:",
        header_window,
    )
    if client_job_match:
        cleaned = _clean_site_address_candidate(client_job_match.group("value"))
        if cleaned:
            return cleaned
    direct_inline_match = re.search(
        r"(?is)\b(?:site\s+)?address\s*:\s*(?P<value>.+?)(?=(?:\n|$))",
        header_window,
    )
    if direct_inline_match:
        cleaned = _clean_site_address_candidate(direct_inline_match.group("value"))
        if cleaned:
            return cleaned
    for index, line in enumerate(header_lines):
        candidate = line
        if re.match(r"(?i)^(?:site\s+)?address\s*:", line):
            tail = normalize_space(re.sub(r"(?i)^(?:site\s+)?address\s*:\s*", "", line))
            if tail:
                candidate = tail
            elif index + 1 < len(header_lines):
                candidate = header_lines[index + 1]
        cleaned = _clean_site_address_candidate(candidate)
        if cleaned:
            return cleaned
    for line in header_lines[:6]:
        cleaned = _clean_site_address_candidate(line)
        if cleaned:
            return cleaned
    return ""


def _clean_site_address_candidate(value: str) -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    text = re.sub(r"(?i)^(?:site\s+)?address\s*:\s*", "", text)
    text = _truncate_site_address_candidate(text)
    text = _extract_site_address_core(text)
    if not text:
        return ""
    if len(text) > 140:
        return ""
    for pattern in SITE_ADDRESS_REJECT_PATTERNS:
        if re.search(pattern, text):
            return ""
    if not re.search(r"\b\d+[A-Za-z]?(?:/\d+[A-Za-z]?)?\b", text):
        return ""
    if not re.search(rf"(?i)\b{ADDRESS_STREET_TYPE_PATTERN}\b", text):
        return ""
    return text.strip(" -;,")


def _truncate_site_address_candidate(value: str) -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    end_index = len(text)
    for pattern in SITE_ADDRESS_STOP_PATTERNS:
        match = re.search(pattern, text)
        if match and match.start() < end_index:
            end_index = match.start()
    return normalize_space(text[:end_index]).strip(" -;,")


def _extract_site_address_core(value: str) -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    text = re.sub(r"(?i)^lot\s+address\s*", "", text).strip(" -:,;")
    dash_parts = [normalize_space(part) for part in re.split(r"\s+-\s+", text) if normalize_space(part)]
    if len(dash_parts) > 1:
        for index in range(len(dash_parts) - 1, -1, -1):
            suffix = normalize_space(" - ".join(dash_parts[index:]))
            if _looks_like_address_core(suffix):
                text = suffix
                break
    text = re.sub(r"(?i)^private\s*-\s*", "", text).strip(" -:,;")
    return normalize_space(text)


def _looks_like_address_core(value: str) -> bool:
    text = normalize_space(str(value or ""))
    if not text:
        return False
    has_street_type = bool(re.search(rf"(?i)\b{ADDRESS_STREET_TYPE_PATTERN}\b", text))
    has_number = bool(re.search(r"\b\d+[A-Za-z]?(?:/\d+[A-Za-z]?)?\b", text))
    has_lot = bool(re.search(r"(?i)\blot\s+\d+\b", text))
    return has_street_type and (has_number or has_lot)


def _imperial_collect_fields(lines: list[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    index = 0
    while index < len(lines):
        field_key, tail = _imperial_match_field_label(lines[index])
        if not field_key:
            index += 1
            continue
        parts: list[str] = [tail] if tail else []
        index += 1
        while index < len(lines):
            next_field_key, _ = _imperial_match_field_label(lines[index])
            next_line = normalize_space(lines[index])
            allow_as_continuation = (
                next_field_key
                and field_key in {"rail", "hanging_rail"}
                and next_field_key == "led"
                and "provision" in next_line.lower()
            )
            allow_as_continuation = allow_as_continuation or (
                field_key == "island_cabinetry" and next_line.upper().startswith("ISLAND CURVE")
            )
            allow_as_continuation = allow_as_continuation or (
                field_key == "custom_handles"
                and next_field_key == "handles"
                and next_line.upper().startswith("HANDLES -")
            )
            if allow_as_continuation:
                next_field_key = ""
            next_next_line = normalize_space(lines[index + 1]) if index + 1 < len(lines) else ""
            if (
                field_key in {"base", "upper", "upper_tall", "cabinetry_colour", "island_cabinetry", "base_back_wall", "tall_cabinetry"}
                and not next_field_key
                and _looks_like_imperial_materialish_line(next_line)
                and next_next_line
                and (_imperial_match_field_label(next_next_line)[0] or _looks_like_imperial_auxiliary_row(next_next_line))
            ):
                if not (field_key == "island_cabinetry" and next_line.upper().startswith("ISLAND CURVE")):
                    break
            if next_field_key or _is_imperial_field_stop_line(next_line) or (not allow_as_continuation and _looks_like_imperial_auxiliary_row(next_line)):
                break
            if next_line:
                parts.append(next_line)
            index += 1
        cleaned = _imperial_clean_field_value(field_key, parts)
        if cleaned:
            fields[field_key] = _merge_text(fields.get(field_key, ""), cleaned)
    return fields


def _is_imperial_field_stop_line(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return True
    if _is_imperial_page_noise_line(text):
        return True
    if _is_imperial_full_page_break(text):
        return True
    upper = text.upper()
    if any(marker in upper for marker in ("AREA / ITEM", "SHADOWLINE:", "BULKHEAD:", "CEILING HEIGHT:", "CABINETRY HEIGHT:")):
        return True
    if upper.startswith("SQUARE SET CEILING") or re.match(r"(?i)^PIC\s+\d+\b", text):
        return True
    if any(text.startswith(marker) for marker in IMPERIAL_HEADER_START_MARKERS):
        return True
    if re.match(r"(?i)^(ceiling height|cabinetry height|bulkhead|shadowline|hinges\s*&\s*drawer runners|area\s*/\s*item|notes|supplier)\b", text):
        return True
    return False


def _preprocess_imperial_lines(lines: list[str]) -> list[str]:
    merged: list[str] = []
    for raw_line in lines:
        for segment in _imperial_split_combined_line(raw_line):
            line = normalize_space(segment)
            if not line:
                continue
            next_key, next_tail = _imperial_match_field_label(line)
            if merged:
                combined = normalize_space(f"{merged[-1]} {line}")
                previous_key, previous_tail = _imperial_match_field_label(merged[-1])
                previous_previous_key = _imperial_match_field_label(merged[-2])[0] if len(merged) > 1 else ""
                if _looks_like_imperial_section_title_line(line):
                    merged.append(line)
                    continue
                if (
                    next_key in {"bench_tops", "splashback", "floating_shelf"}
                    and (not next_tail or next_tail.startswith("("))
                    and not previous_key
                    and (
                        not previous_previous_key
                        or (
                            next_key == "bench_tops"
                            and bool(re.search(r"(?i)\b(?:\d+\s*mm|laminate|stone|oak|walnut|edge|waterfall|mitred)\b", merged[-1]))
                        )
                    )
                    and _looks_like_imperial_materialish_line(merged[-1])
                    and not _looks_like_imperial_auxiliary_row(merged[-1])
                ):
                    merged[-1] = normalize_space(f"{line} {merged[-1]}")
                    continue
                if line.startswith(("+ ", "- ")) or (
                    previous_key and not previous_tail and not next_key and not _is_imperial_field_stop_line(line)
                ):
                    merged[-1] = combined
                    continue
            merged.append(line)
    rebuilt: list[str] = []
    index = 0
    while index < len(merged):
        line = merged[index]
        upper = line.upper()
        if index + 1 < len(merged):
            next_line = merged[index + 1]
            next_upper = next_line.upper()
            if line.upper() == "CUSTOM" and next_line.upper().startswith("HANDLES"):
                rebuilt.append(normalize_space(f"{line} {next_line}"))
                index += 2
                continue
            if upper == "FEATURE TALL" and next_upper.startswith("CABINETRY COLOUR"):
                combined_line = normalize_space(f"{line} {next_line}")
                if index + 2 < len(merged) and merged[index + 2].strip().startswith("+"):
                    combined_line = normalize_space(f"{combined_line} {merged[index + 2]}")
                    rebuilt.append(combined_line)
                    index += 3
                    continue
                rebuilt.append(combined_line)
                index += 2
                continue
            if upper == "GLASS INLAY DOORS" and next_upper == "TO OVERHEAD" and index + 2 < len(merged) and merged[index + 2].upper() == "FEATURE CABINETRY":
                rebuilt.append(normalize_space(f"{line} {next_line} {merged[index + 2]}"))
                index += 3
                continue
            if upper == "FEATURE TIMBER LOOK FLOATING" and next_upper == "SHELVES":
                rebuilt.append(normalize_space(f"{line} {next_line}"))
                index += 2
                continue
            if upper == "FEATURE TIMBER LOOK CABINETRY" and next_upper.startswith("COFFEE STATION AREA"):
                combined_line = normalize_space(f"{line} {next_line}")
                if index + 2 < len(merged) and "SHELVES + SURROUNDS" in merged[index + 2].upper():
                    combined_line = normalize_space(f"{combined_line} {merged[index + 2]}")
                    rebuilt.append(combined_line)
                    index += 3
                    continue
                rebuilt.append(combined_line)
                index += 2
                continue
            if upper == "LIP PULL" and next_upper.startswith("HANDLES - DRAWERS"):
                rebuilt.append(normalize_space(f"{line} {next_line}"))
                index += 2
                continue
            if upper == "FEATURE LIP PULL" and next_upper == "PANTRY" and index + 2 < len(merged) and merged[index + 2].upper().startswith("HANDLES"):
                rebuilt.append(normalize_space(f"{line} {next_line} {merged[index + 2]}"))
                index += 3
                continue
            if upper.endswith("SINGLE CABINET ON BAR") and next_upper == "BACK AREA":
                rebuilt.append(normalize_space(f"{line} {next_line}"))
                index += 2
                continue
            combined = normalize_space(f"{line} {next_line}")
            combined_key, _ = _imperial_match_field_label(combined)
            current_key, _ = _imperial_match_field_label(line)
            next_key, _ = _imperial_match_field_label(next_line)
            if combined_key and not current_key and not next_key and _looks_like_imperial_auxiliary_row(line):
                rebuilt.append(combined)
                index += 2
                continue
        upper = line.upper()
        if upper in {"BASE", "UPPER", "TALL", "ISLAND"} and index + 1 < len(merged):
            next_line = merged[index + 1]
            if next_line.upper().startswith("CABINETRY COLOUR"):
                rebuilt.append(normalize_space(f"{line} {next_line}"))
                index += 2
                continue
        if upper.startswith("KICKBOARDS") and "BENCHTOP" in upper and "POLYTEC" in upper:
            polytec_index = upper.find("POLYTEC")
            benchtop_index = upper.find("BENCHTOP")
            if polytec_index > 0 and benchtop_index > polytec_index:
                kick_line = normalize_space(f"KICKBOARDS {line[polytec_index:benchtop_index]}")
                rebuilt.append(kick_line)
                rebuilt.append("BENCHTOP")
                index += 1
                continue
        rebuilt.append(line)
        index += 1
    return rebuilt


def _imperial_split_combined_line(raw_line: str) -> list[str]:
    text = normalize_space(raw_line)
    if not text:
        return []
    split_points: set[int] = {0}
    for marker in IMPERIAL_INLINE_SPLIT_MARKERS:
        pattern = re.compile(re.escape(marker), re.IGNORECASE)
        for match in pattern.finditer(text):
            index = match.start()
            prefix_text = text[max(0, index - 25) : index]
            prefix = prefix_text.upper()
            if marker in {"BENCHTOP", "BENCHTOPS"} and "TO TOP OF " in prefix:
                continue
            if marker in {"BENCHTOP", "BENCHTOPS"} and re.search(r"(?i)\b(?:laminate|stone)\s*$", prefix_text):
                suffix_text = text[match.end() : match.end() + 24]
                if not re.match(r"^\s*\(", suffix_text):
                    continue
            if marker == "SPLASHBACK" and prefix.endswith("BENCHTOP+ "):
                continue
            if marker == "BIN" and re.search(r"(?i)\b(?:gpo|drawer|pull\s*out|upper)\b", prefix_text):
                continue
            if marker.startswith("HANDLES") and (prefix.endswith("NO ") or prefix.endswith("TOUCH CATCH ")):
                continue
            if not _imperial_marker_start_allowed(text, index, marker):
                continue
            if not _imperial_marker_end_allowed(text, match.end(), marker):
                continue
            if index > 0:
                split_points.add(index)
    ordered = sorted(split_points)
    if len(ordered) == 1:
        return [text]
    parts: list[str] = []
    for idx, start in enumerate(ordered):
        end = ordered[idx + 1] if idx + 1 < len(ordered) else len(text)
        segment = normalize_space(text[start:end])
        if segment:
            parts.append(segment)
    return parts


def _imperial_marker_start_allowed(text: str, index: int, marker: str) -> bool:
    if index <= 0:
        return True
    previous = text[index - 1]
    if not previous.isalpha():
        return True
    if previous.islower():
        if marker.upper() in {"LED", "LED'S", "GPO'S", "BIN"}:
            return False
        if marker.upper() == marker and len(marker) <= 4:
            return False
        return True
    if any(char.islower() for char in marker):
        prefix = text[max(0, index - 3) : index]
        if prefix.isupper():
            return True
    return False


def _imperial_marker_end_allowed(text: str, end_index: int, marker: str) -> bool:
    if end_index >= len(text):
        return True
    next_char = text[end_index]
    if not next_char.isalpha():
        return True
    if next_char.isupper() and any(char.islower() for char in marker):
        return True
    return False


def _looks_like_imperial_section_title_line(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return False
    if re.search(r"(?i)\bJOINERY SELECTION SHEET\b", text):
        return True
    return text.upper() in {"FEATURE TALL DOORS"}


def _looks_like_imperial_materialish_line(line: str) -> bool:
    text = normalize_space(line)
    if not text or _is_imperial_page_noise_line(text):
        return False
    if _imperial_is_supplier_only_line(text):
        return False
    if _imperial_match_field_label(text)[0]:
        return False
    if _looks_like_imperial_section_title_line(text) or _is_imperial_full_page_break(text):
        return False
    return bool(
        re.search(
            r"(?i)\b(?:\d+\s*mm|stone|laminate|woodmatt|smooth|matt|thermolaminated|thermolaminate|melamine|vinyl|caesarstone|polytec|smartstone|wk stone|oak|walnut|white|black|grey|carrina|nuvo|arissed|profile|style|by builder)\b",
            text,
        )
        or re.search(r"\(\d{3,}\)", text)
    )


def _looks_like_imperial_auxiliary_row(line: str) -> bool:
    text = normalize_space(line)
    if not text or _is_imperial_page_noise_line(text):
        return False
    if _imperial_match_field_label(text)[0]:
        return True
    if _looks_like_imperial_section_title_line(text) or _is_imperial_full_page_break(text):
        return True
    upper = text.upper()
    if any(upper.startswith(marker) for marker in IMPERIAL_AUXILIARY_ROW_START_MARKERS):
        return True
    if re.match(r"(?i)^PIC\s+\d+\b", text):
        return True
    if text == upper and len(text) <= 90 and any(token in upper for token in IMPERIAL_AUXILIARY_ROW_TOKENS):
        return True
    return False


def _dedupe_overlapping_text_parts(parts: list[str]) -> list[str]:
    deduped: list[str] = []
    for raw_part in parts:
        part = normalize_brand_casing_text(normalize_space(raw_part)).strip(" -;,")
        if not part:
            continue
        lowered = part.lower()
        if any(lowered == existing.lower() or lowered in existing.lower() for existing in deduped):
            continue
        deduped = [existing for existing in deduped if existing.lower() not in lowered]
        deduped.append(part)
    return deduped


def _collapse_repeated_token_sequence(text: str) -> str:
    normalized_text = normalize_space(text)
    dash_parts = [part.strip() for part in normalized_text.split(" - ")]
    if len(dash_parts) == 3:
        third_tokens = dash_parts[2].split()
        second_tokens = dash_parts[1].split()
        if (
            len(third_tokens) >= len(second_tokens) + 2
            and [token.lower() for token in third_tokens[1 : 1 + len(second_tokens)]] == [token.lower() for token in second_tokens]
            and third_tokens[0].lower() == third_tokens[-1].lower()
        ):
            return f"{dash_parts[0]} - {dash_parts[1]} - {third_tokens[0]}".strip()
    tokens = normalized_text.split()
    if len(tokens) < 4:
        return normalized_text
    changed = True
    while changed:
        changed = False
        max_span = min(4, len(tokens) // 2)
        for span in range(max_span, 1, -1):
            for index in range(len(tokens) - (2 * span) + 1):
                left = [token.lower() for token in tokens[index : index + span]]
                right = [token.lower() for token in tokens[index + span : index + (2 * span)]]
                if left != right:
                    continue
                tokens = tokens[: index + span] + tokens[index + (2 * span) :]
                changed = True
                break
            if changed:
                break
    return " ".join(tokens).strip()


def _dedupe_delimited_fragments(text: str, delimiter: str = "|") -> str:
    normalized = normalize_space(str(text or ""))
    if not normalized:
        return ""
    fragments = [normalize_space(part).strip(" -;,") for part in normalized.split(delimiter)]
    deduped: list[str] = []
    signatures: list[tuple[str, ...]] = []
    for fragment in fragments:
        if not fragment:
            continue
        lowered = fragment.lower()
        signature_text = re.sub(r"(?i)\b(\d+\s*mm)\s+thick\b", r"\1thick", lowered)
        signature_tokens = [
            token
            for token in re.findall(r"[a-z0-9]+(?:mmthick|mm)?", signature_text)
            if token
        ]
        if "thick" in signature_tokens:
            thick_index = signature_tokens.index("thick")
            for idx, token in enumerate(signature_tokens):
                if idx == thick_index:
                    continue
                if re.fullmatch(r"\d+mm", token):
                    signature_tokens[idx] = f"{token}thick"
                    del signature_tokens[thick_index]
                    break
        signature = tuple(sorted(signature_tokens))
        if signature and signature in signatures:
            continue
        if any(
            lowered == existing.lower()
            or lowered in existing.lower()
            or existing.lower() in lowered
            for existing in deduped
        ):
            if not any(existing.lower() in lowered and existing.lower() != lowered for existing in deduped):
                continue
            filtered_pairs = [
                (existing, existing_signature)
                for existing, existing_signature in zip(deduped, signatures)
                if existing.lower() not in lowered or existing.lower() == lowered
            ]
            deduped = [existing for existing, _ in filtered_pairs]
            signatures = [existing_signature for _, existing_signature in filtered_pairs]
        deduped.append(fragment)
        signatures.append(signature)
    return f" {delimiter} ".join(deduped)


def _imperial_clean_field_value(field_key: str, parts: list[str]) -> str:
    if field_key in {"bench_tops", "splashback"}:
        return _imperial_clean_material_value(parts, drop_note_lines=True)
    if field_key == "island_cabinetry":
        island_parts = []
        for part in parts:
            normalized = normalize_space(part)
            if re.match(r"(?i)^\(incl\.", normalized):
                continue
            if normalized.upper().startswith("ISLAND CURVE"):
                normalized = re.sub(r"(?i)^ISLAND\s+CURVE\s+AND\s+COLUMN\)\s*", "", normalized)
            island_parts.append(normalized)
        return _imperial_clean_material_value(island_parts, drop_note_lines=False)
    if field_key in {
        "upper_tall",
        "upper",
        "base",
        "feature_cabinetry",
        "overhead_feature_cabinetry",
        "feature_tall_bar_back",
        "tall_doors",
        "tall_cabinetry",
        "island_cabinetry",
        "cabinetry_colour",
        "mirrored_shaving_cabinet",
        "floating_shelf",
        "rail",
        "hanging_rail",
        "jewellery_insert",
        "extra_top",
    }:
        return _imperial_clean_material_value(parts, drop_note_lines=False)
    if field_key == "led":
        return "Yes" if any(normalize_space(part) for part in parts) else ""
    if field_key == "flooring":
        return _imperial_clean_flooring_value(parts)
    if field_key == "accessories":
        return _imperial_clean_accessories_value(parts)
    if field_key in {"gpo", "hamper", "bin"}:
        return _imperial_clean_accessories_value(parts)
    if field_key == "toe_kick":
        return _imperial_clean_toe_kick_value(parts)
    if field_key == "custom_handles":
        return _imperial_clean_custom_handles_value(parts)
    if field_key in {"handles_overheads", "handles_base", "handles"}:
        return _imperial_clean_handles_value(parts)
    return _imperial_clean_material_value(parts, drop_note_lines=False)


def _imperial_clean_material_value(parts: list[str], drop_note_lines: bool) -> str:
    supplier = ""
    builder_note = ""
    special_value = ""
    cleaned_parts: list[str] = []
    for raw_part in parts:
        part = normalize_space(raw_part)
        if not part:
            continue
        part = re.sub(r"([A-Za-z])(\d+\s*mm\b)", r"\1 \2", part)
        if _looks_like_imperial_auxiliary_row(part) and not _imperial_match_field_label(part)[0]:
            continue
        if _imperial_is_supplier_only_line(part):
            supplier = normalize_brand_casing_text(part)
            continue
        part = re.sub(r"(?i)\bby builder(?=\d)", "By Builder ", part)
        if re.search(r"(?i)\bby builder\b", part):
            builder_note = "By Builder"
            part = re.sub(r"(?i)\bby builder\b", "", part)
            part = normalize_space(part)
        lowered = part.lower()
        if drop_note_lines and (
            lowered.startswith("note:")
            or "undermount sink" in lowered
            or "same height on all other walls" in lowered
            or "as per plans" in lowered
            or lowered.startswith("up to overheads")
        ):
            continue
        if lowered in {"n/a", "image"}:
            continue
        if re.match(r"(?i)^pic\s+\d+\b", part):
            continue
        if re.match(r"(?i)^includes\b.*$", part):
            continue
        if re.match(r"(?i)^back area\b", part):
            continue
        if re.match(r"(?i)^(?:gpo'?s|bin|hamper|hanging rail|rail|jewellery insert|mirror(?:ed)? shaving cabinet)\b", part):
            continue
        if "handle" in lowered and "profile door" not in lowered:
            continue
        split_parts = [normalize_space(item) for item in re.split(r"\s+-\s+", part) if normalize_space(item)]
        if len(split_parts) > 1:
            cleaned_parts.extend(normalize_brand_casing_text(item) for item in split_parts)
            continue
        laminate_benchtop_match = re.match(
            r"(?i)^(?P<material>.+?)\s+(?P<label>Laminate\s+Benchtop|Benchtop\s+Laminate)\s+(?P<tail>\d+\s*mm.*)$",
            part,
        )
        if laminate_benchtop_match:
            material = normalize_brand_casing_text(laminate_benchtop_match.group("material"))
            tail = normalize_brand_casing_text(laminate_benchtop_match.group("tail"))
            special_value = " - ".join(part for part in (material, "Laminate Benchtop", tail) if part)
            continue
        thickness_match = re.match(r"(?i)^(?P<thickness>\d+\s*mm)\b(?P<rest>.*)$", part)
        if thickness_match and re.search(r"(?i)\b(edge|mitred|profile|style)\b", part):
            thickness = normalize_brand_casing_text(thickness_match.group("thickness"))
            rest = normalize_brand_casing_text(thickness_match.group("rest")).strip(" -;,")
            if thickness:
                cleaned_parts.append(thickness)
            if rest:
                cleaned_parts.append(rest)
            continue
        cleaned_parts.append(normalize_brand_casing_text(part))
    value = special_value or _imperial_compose_material_text(supplier, cleaned_parts)
    if supplier and special_value and supplier.lower() not in special_value.lower():
        value = f"{supplier} - {special_value}"
    if builder_note and value and builder_note.lower() not in value.lower():
        value = f"{value} - {builder_note}"
    return value


def _imperial_clean_flooring_value(parts: list[str]) -> str:
    cleaned_candidates: list[str] = []
    for raw_part in parts:
        part = normalize_space(raw_part)
        if not part:
            continue
        part = re.sub(r"(?i)\bsoft close\b.*$", "", part)
        part = re.sub(r"(?i)\bnotes?\b.*$", "", part)
        part = re.sub(r"(?i)\bsupplier\b.*$", "", part)
        part = re.sub(r"(?i)\bwaterfall ends?\b.*$", "", part)
        part = normalize_space(part).strip(" -;,:")
        if not part:
            continue
        if re.search(r"(?i)\bN\s*/?\s*A\b", part):
            cleaned_candidates.append("NA")
            continue
        match = re.search(r"(?i)\b(tiled|tiles|tile|hybrid|carpet|laminate|timber|vinyl|stone)\b", part)
        if match:
            cleaned = normalize_brand_casing_text(part).strip(" -;,")
            if re.fullmatch(r"(?i)(tiled|tiles|tile|hybrid|carpet|laminate|timber|vinyl|stone)", cleaned):
                value = match.group(1).lower()
                if value in {"tile", "tiles"}:
                    cleaned_candidates.append("Tiled")
                else:
                    cleaned_candidates.append(normalize_brand_casing_text(value))
            else:
                cleaned_candidates.append(cleaned)
    for preferred in cleaned_candidates:
        if preferred.lower() != "stone":
            return preferred
    return cleaned_candidates[0] if cleaned_candidates else ""


def _imperial_clean_toe_kick_value(parts: list[str]) -> str:
    raw_text = normalize_space(" ".join(normalize_space(part) for part in parts if normalize_space(part)))
    if raw_text and re.search(r"(?i)\boverhang to be\b", raw_text):
        raw_text = re.sub(r"(?i)^.*?\b(polytec|laminex)\b", r"\1", raw_text)
        raw_text = normalize_space(raw_text)
    if raw_text and re.search(r"(?i)\bmatch above\b", raw_text) and re.search(r"(?i)\bor\b", raw_text):
        cleaned = normalize_brand_casing_text(raw_text)
        cleaned = re.sub(r"(?i)\b(?:polytec\s*\+\s*laminex|laminex\s*\+\s*polytec)\b", "", cleaned)
        cleaned = re.sub(r"(?i)\bmatch above\b", "Match Above", cleaned)
        cleaned = re.sub(r"(?i)\s+or\s+", " / ", cleaned)
        cleaned = normalize_space(cleaned).strip(" -;/,")
        return cleaned
    supplier = ""
    candidate_parts: list[str] = []
    entries: list[str] = []
    for raw_part in parts:
        part = normalize_space(raw_part)
        if not part:
            continue
        if re.search(r"(?i)\b(?:soft close|shadowline|builders bulkhead|cabinetry height|ceiling height)\b", part):
            continue
        if re.match(r"(?i)^pic\s+\d+\b", part):
            continue
        if part.upper() in {"SQUARE SET CEILING", "EVOCA"}:
            continue
        if re.search(r"(?i)\bprovision\b", part):
            continue
        if part in {"ONLY)", "(Provision"}:
            continue
        if re.search(r"(?i)\b(?:polytec\s*\+\s*laminex|laminex\s*\+\s*polytec)\b", part):
            continue
        if re.search(r"(?i)\b(?:all colours shown|subject to supplier at time of install|product availability|client name|signature|signed date|designer)\b", part):
            break
        if _imperial_is_supplier_only_line(part):
            supplier = normalize_brand_casing_text(part)
            continue
        candidate_parts.append(part)
    for part in candidate_parts:
        if part.upper().startswith("MATCH ABOVE"):
            continue
        if re.search(r"(?i)\b(?:momo|kethy|bronte|barrington|part number|so-[a-z0-9-]+|bepl\d+|matt brass|matt black|brushed nickel)\b", part):
            continue
        normalized = normalize_brand_casing_text(part).strip(" -;,.")
        if supplier:
            normalized = re.sub(rf"(?i)\b{re.escape(supplier)}\b\s*$", "", normalized).strip(" -;,.")
        if supplier and normalized and not normalized.lower().startswith(supplier.lower()):
            normalized = _compose_supplier_description_note(supplier, normalized)
        if normalized and normalized not in entries:
            entries.append(normalized)
    return "; ".join(entries)


def _imperial_clean_handles_value(parts: list[str]) -> str:
    supplier = ""
    note_entries: list[str] = []
    description_entries: list[str] = []
    for raw_part in parts:
        part = normalize_space(raw_part)
        if not part or _imperial_is_supplier_only_line(part):
            if _imperial_is_supplier_only_line(part):
                supplier = normalize_brand_casing_text(part)
            continue
        if _looks_like_imperial_handle_stop_line(part) or _looks_like_imperial_section_title_line(part):
            continue
        if re.match(r"(?i)^(?:base cabinetry colour|upper cabinetry colour|tall cabinetry colour|cabinetry colour|benchtop|splashback|kickboards?|gpo'?s|bin|hamper|floating shelv(?:es|ing)|rail|jewellery insert)\b", part):
            continue
        part = re.sub(r"(?i)\bpolytec\b", "", part)
        part = normalize_space(part).strip(" -;,")
        if not part:
            continue
        supplier_hint, remainder = _normalize_entry_supplier_text(re.sub(r"(?<=[a-z])(?=[A-Z])", " ", part))
        if supplier_hint and supplier_hint.upper() in {item.upper() for item in IMPERIAL_HANDLE_SUPPLIER_HINTS}:
            supplier = supplier_hint
        cleaned = normalize_brand_casing_text(remainder or part)
        if note_entries and re.fullmatch(r"(?i)drawers?", cleaned):
            note_entries[-1] = normalize_space(f"{note_entries[-1]} {cleaned}")
            continue
        if note_entries and re.match(r"(?i)^anodised\b", cleaned):
            note_entries[-1] = normalize_space(f"{note_entries[-1]} {cleaned}")
            continue
        if note_entries and re.fullmatch(r"(?i)doors?", cleaned):
            note_entries[-1] = normalize_space(f"{note_entries[-1]} {cleaned}")
            continue
        if description_entries and re.match(r"(?i)^(?:matt\b|brushed\b|satin\b|part number:|so-[a-z0-9-]+|bepl\d+)", cleaned):
            description_entries[-1] = normalize_space(f"{description_entries[-1]} {cleaned}")
            continue
        if description_entries and re.search(r"(?i)\bin$", description_entries[-1]):
            description_entries[-1] = normalize_space(f"{description_entries[-1]} {cleaned}")
            continue
        if _is_handle_note_like(cleaned):
            note_entries.append(cleaned)
            continue
        if _is_handle_description_like(cleaned):
            description_entries.append(cleaned)
            continue
        note_entries.append(cleaned)
    entries: list[str] = []
    used_notes = 0
    for description in description_entries:
        note = ""
        if used_notes < len(note_entries) and re.search(r"(?i)\b(?:horizontal on|vertical on|no handles?|no handle for|touch catch|recessed finger space)\b", note_entries[used_notes]):
            note = note_entries[used_notes]
            used_notes += 1
        formatted = _compose_supplier_description_note(
            supplier if supplier.upper() in {item.upper() for item in IMPERIAL_HANDLE_SUPPLIER_HINTS} else "",
            description,
            note,
        )
        entries.append(formatted or description)
    entries.extend(note_entries[used_notes:])
    return "; ".join(_unique([entry for entry in entries if normalize_space(entry)]))


def _imperial_clean_custom_handles_value(parts: list[str]) -> str:
    supplier = ""
    material_parts: list[str] = []
    size_text = ""
    orientation = ""
    for raw_part in parts:
        part = normalize_space(raw_part)
        if not part or _looks_like_imperial_handle_stop_line(part):
            continue
        if _imperial_is_supplier_only_line(part):
            supplier = normalize_brand_casing_text(part)
            continue
        part = re.sub(r"(?i)^CUSTOM\s+HANDLES?\b", "", part).strip(" -;,")
        part = re.sub(r"(?i)^HANDLES?\s*-\s*", "", part).strip(" -;,")
        part = re.sub(r"(?i)\bpolytec\b", "", part).strip(" -;,")
        if not part:
            continue
        if re.fullmatch(r"(?i)vertical", part):
            orientation = "VERTICAL"
            continue
        size_match = re.search(r"(?i)\b\d+\s*mm high x \d+\s*mm wide outset \d+\s*mm\b", part)
        if size_match:
            size_text = normalize_space(size_match.group(0))
            continue
        part = re.sub(r"(?i)\bcustom made handles?\b", "", part).strip(" -;,")
        part = re.sub(r"(?i)\bcustom made\b", "", part).strip(" -;,")
        if not part:
            continue
        material_parts.append(normalize_brand_casing_text(part))
    material = normalize_space(" ".join(_unique(material_parts))).strip(" -;,")
    if material and supplier and not material.lower().startswith(supplier.lower()):
        material = f"{supplier} {material}"
    parts_out = ["Custom Made Handles"]
    if material:
        parts_out.append(material)
    if size_text:
        parts_out.append(size_text)
    if orientation:
        parts_out.append(orientation)
    return " - ".join(part for part in parts_out if part).strip(" -;,")


def _normalize_entry_supplier_text(text: str) -> tuple[str, str]:
    normalized = normalize_brand_casing_text(normalize_space(text)).strip(" -;,")
    if not normalized:
        return "", ""
    for supplier in sorted(ENTRY_SUPPLIER_HINTS, key=len, reverse=True):
        prefix_match = re.match(rf"(?i)^{re.escape(supplier)}(?:\s*[,/-]\s*|\s+)(?P<rest>.+)$", normalized)
        if prefix_match:
            return normalize_brand_casing_text(supplier), normalize_brand_casing_text(prefix_match.group("rest")).strip(" -;,")
        suffix_match = re.match(rf"(?i)^(?P<rest>.+?)(?:\s*[,/-]\s*|\s+){re.escape(supplier)}$", normalized)
        if suffix_match:
            return normalize_brand_casing_text(supplier), normalize_brand_casing_text(suffix_match.group("rest")).strip(" -;,")
    return "", normalized


def _compose_supplier_description_note(supplier: str, description: str, note: str = "") -> str:
    supplier_text = normalize_brand_casing_text(normalize_space(supplier)).strip(" -;,")
    description_text = normalize_brand_casing_text(normalize_space(description)).strip(" -;,")
    note_text = normalize_brand_casing_text(normalize_space(note)).strip(" -;,")
    if supplier_text and description_text:
        description_text = re.sub(rf"(?i)^{re.escape(supplier_text)}(?:\s*-\s*|\s+)", "", description_text).strip(" -;,")
    if note_text and description_text and note_text.lower() in description_text.lower():
        note_text = ""
    return " - ".join(
        part.strip(" -;,")
        for part in (
            supplier_text,
            description_text,
            note_text,
        )
        if normalize_space(part)
    ).strip(" -;,")


def _is_handle_note_like(text: str) -> bool:
    normalized = normalize_brand_casing_text(normalize_space(text))
    if not normalized:
        return False
    return bool(
        re.search(
            r"(?i)\b(?:horizontal on|vertical on|finger pull|recessed finger|touch catch|pto\b|no handles?|no handle for|bronte handle - base cabs only|recessed finger space)\b",
            normalized,
        )
    )


def _is_handle_description_like(text: str) -> bool:
    normalized = normalize_brand_casing_text(normalize_space(text))
    if not normalized:
        return False
    if re.search(r"(?i)\bpull-?out\b", normalized) and not re.search(r"(?i)\bcabinetry\s+pull|pull\s+extended\b", normalized):
        return False
    return bool(
        re.search(
            r"(?i)\b(?:handle|profile handle|square handle|knob|cabinetry pull|pull extended|voda|danes|barrington|rappana|elsa cabinetry knob|custom made handles|part number|so-[a-z0-9-]+|bepl\d+)\b",
            normalized,
        )
        and not _is_handle_note_like(normalized)
    )


def _imperial_clean_accessories_value(parts: list[str]) -> str:
    cleaned_parts: list[str] = []
    for raw_part in parts:
        part = normalize_space(raw_part)
        if not part or _imperial_is_supplier_only_line(part):
            continue
        if _is_imperial_page_noise_line(part):
            continue
        if re.match(r"(?i)^product code\s*:", part):
            continue
        if re.search(r"(?i)\bsubject to supplier at time of install\b", part):
            break
        if re.match(r"(?i)^installed\b", part):
            break
        cleaned_parts.append(normalize_brand_casing_text(part).strip(" -;,"))
    text = normalize_space(" ".join(cleaned_parts)).strip(" -;,")
    text = re.sub(r"(?i)\binstal(?:l(?:ed)?)?$", "", text).strip(" -;,")
    supplier, remainder = _normalize_entry_supplier_text(text)
    if supplier and remainder:
        text = f"{supplier} - {remainder}"
    elif remainder:
        text = remainder
    return text.strip(" -;,")


def _imperial_compose_material_text(supplier: str, parts: list[str]) -> str:
    cleaned_parts = [normalize_brand_casing_text(normalize_space(part)).strip(" -;,") for part in parts if normalize_space(part)]
    if not supplier:
        extracted_parts: list[str] = []
        for part in cleaned_parts:
            embedded_supplier = ""
            for known_supplier in sorted(IMPERIAL_SUPPLIER_ONLY_LINES, key=len, reverse=True):
                match = re.search(rf"(?i)\b{re.escape(known_supplier)}\b", part)
                if match:
                    embedded_supplier = normalize_brand_casing_text(match.group(0))
                    part = normalize_space(re.sub(rf"(?i)\b{re.escape(known_supplier)}\b", "", part)).strip(" -;,")
                    break
            if embedded_supplier and not supplier:
                supplier = embedded_supplier
            if part:
                extracted_parts.append(part)
        cleaned_parts = extracted_parts
    expanded_parts: list[str] = []
    for part in cleaned_parts:
        split_parts = [normalize_space(item) for item in re.split(r"\s+-\s+", part) if normalize_space(item)]
        expanded_parts.extend(split_parts or [part])
    cleaned_parts = expanded_parts
    if supplier:
        supplier_pattern = re.compile(rf"(?i)\b{re.escape(supplier)}\b")
        normalized_parts: list[str] = []
        for part in cleaned_parts:
            if part.lower() == supplier.lower():
                continue
            trimmed = normalize_space(supplier_pattern.sub("", part)).strip(" -;,")
            normalized_parts.append(trimmed or part)
        cleaned_parts = [part for part in normalized_parts if part]
    if not cleaned_parts and not supplier:
        return ""
    thickness = ""
    thickness_stripped_parts: list[str] = []
    for part in cleaned_parts:
        match = re.search(r"\b\d+\s*mm\b", part, re.IGNORECASE)
        if match and not thickness:
            thickness = match.group(0)
            stripped = normalize_space(part.replace(match.group(0), "")).strip(" -;,")
            if stripped:
                thickness_stripped_parts.append(stripped)
            continue
        thickness_stripped_parts.append(part)
    cleaned_parts = thickness_stripped_parts
    profile_parts = [part for part in cleaned_parts if re.search(r"(?i)\b(profile|style|edge|woodmatt|smooth|matt finish|thermolaminate|melamine|vinyl)\b", part)]
    profile_parts.extend([part for part in cleaned_parts if re.search(r"(?i)\b(laminate|vertical grain|horizontal grain|grain|arissed|waterfall|mitred|pencil round)\b", part)])
    profile_parts = _unique(profile_parts)
    material_parts = [
        part
        for part in cleaned_parts
        if part not in profile_parts and part != thickness and not _imperial_is_supplier_only_line(part) and part.lower() not in {"n/a", "image"}
    ]
    ordered: list[str] = []
    lead = ""
    if thickness and supplier:
        lead = f"{thickness} {supplier}"
    elif supplier:
        lead = supplier
    elif thickness and material_parts:
        first_material = material_parts[0]
        ordered.append(normalize_space(f"{thickness} {first_material}"))
        material_parts = material_parts[1:]
    elif thickness:
        lead = thickness
    if lead:
        ordered.append(lead)
    if material_parts:
        ordered.append(" ".join(_unique(material_parts[:1])))
        material_parts = material_parts[1:]
    if material_parts:
        ordered.append(" ".join(_unique(material_parts)))
    remaining_profiles = [part for part in _unique(profile_parts) if part not in ordered]
    ordered.extend(remaining_profiles)
    return " - ".join(part for part in ordered if part).strip(" -;,")


def _imperial_handle_value_looks_noisy(value: str) -> bool:
    text = normalize_space(value)
    if not text:
        return False
    return bool(
        re.search(r"(?i)\b(?:thermolaminated|polytec|vinyl style|vienna|classic white|woodmatt|smooth|matt finish)\b", text)
        and not re.search(r"(?i)\bhandle|touch catch|finger pull|recessed rail\b", text)
    )


def _looks_like_imperial_handle_stop_line(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return True
    if _is_imperial_page_noise_line(text) or _is_imperial_field_stop_line(text):
        return True
    if re.match(r"(?i)^(?:shadowline|bulkhead|ceiling height|cabinetry height)\s*:", text):
        return True
    if re.search(r"(?i)\b(?:all colours shown|product availability|availability is subject to supplier|consultation\.)\b", text):
        return True
    if re.search(r"(?i)\b(?:client name|signature|signed date|designer)\b", text):
        return True
    if re.match(r"(?i)^kickboards?\b", text):
        return True
    if re.search(r"(?i)\bbenchtop\b.*\bhandles?\b", text):
        return True
    return False


def _imperial_handle_entry_is_valid(value: str) -> bool:
    text = normalize_space(value)
    if not text or _looks_like_imperial_handle_stop_line(text):
        return False
    if re.search(r"(?i)\bpull-?out\b", text) and not re.search(r"(?i)\bcabinetry\s+pull|pull\s+extended\b", text):
        return False
    return bool(
        re.search(r"(?i)\b(?:handles?|knob|cabinetry pull|pull extended|touch catch|finger pull|fingerpull|recessed|pto|kethy|anodised|momo|barrington|danes|voda|rappana|elsa|part number)\b", text)
        or re.search(r"(?i)\b(?:horizontal|vertical)\b.*\b(?:drawers?|doors?|uppers?)\b", text)
        or re.match(r"(?i)^(?:doors?|drawers?)\s*-\s*[A-Z0-9-]+", text)
        or re.search(r"(?i)\b(?:so-[a-z0-9-]+|bepl\d+)\b", text)
    )


def _imperial_extract_structured_handle_rows(lines: list[str]) -> list[str]:
    entries: list[str] = []
    index = 0
    while index < len(lines):
        line = normalize_space(lines[index])
        upper = line.upper()
        if not upper:
            index += 1
            continue
        if not (
            "HANDLES" in upper
            or upper.startswith("LIP PULL")
            or upper.startswith("FEATURE LIP PULL")
        ):
            index += 1
            continue
        if _looks_like_imperial_handle_stop_line(line) and "HANDLES" not in upper:
            index += 1
            continue
        supplier = ""
        description_parts: list[str] = []
        note_parts: list[str] = []
        leading_tail = re.sub(
            r"(?i)^(?:feature lip pull pantry handles|lip pull handles\s*-\s*drawers|handles\s*-\s*base cabs\s*\+\s*overhead cabs|handles\s*-\s*base cabs|handles\s*-\s*drawers|handles)\b",
            "",
            line,
        ).strip(" -;,")
        if leading_tail and _is_handle_description_like(leading_tail):
            description_parts.append(normalize_brand_casing_text(leading_tail))
        cursor = index + 1
        while cursor < len(lines):
            next_line = normalize_space(lines[cursor])
            next_upper = next_line.upper()
            if not next_line:
                cursor += 1
                continue
            if cursor > index + 1 and "HANDLES" in next_upper and not _imperial_is_supplier_only_line(next_line):
                break
            if cursor > index + 1 and (
                _imperial_match_field_label(next_line)[0]
                or _looks_like_imperial_section_title_line(next_line)
                or _looks_like_imperial_handle_stop_line(next_line)
            ):
                break
            if _imperial_is_supplier_only_line(next_line):
                supplier = normalize_brand_casing_text(next_line)
                cursor += 1
                continue
            if re.search(r"(?i)\b(?:supplied by client|installed by imperial)\b", next_line):
                note_parts.append(normalize_brand_casing_text(next_line))
                cursor += 1
                continue
            if re.search(r"(?i)\b(?:installed horizontally|installed vertically|to pantry doors only|finger pull|touch catch)\b", next_line):
                note_parts.append(normalize_brand_casing_text(next_line))
                cursor += 1
                continue
            if re.search(r"(?i)\b(?:rappana|elsa cabinetry knob|cabinetry pull|pull extended|knob|momo|titus|lincoln sentry|danes|voda|brushed|matt|chrome|nickel|part number|so-[a-z0-9-]+|bepl\d+|\d+\s*mm)\b", next_line):
                description_parts.append(normalize_brand_casing_text(next_line))
                cursor += 1
                continue
            if description_parts and re.fullmatch(r"(?i)[+ ]?(?:overhead cabs?|base cabs?|drawers?)", next_line):
                cursor += 1
                continue
            if description_parts and re.search(r"(?i)\b(?:doors?|drawers?|uppers?)\b", next_line):
                note_parts.append(normalize_brand_casing_text(next_line))
                cursor += 1
                continue
            break
        if not description_parts:
            index += 1
            continue
        entry = _compose_supplier_description_note(
            supplier,
            normalize_space(" ".join(description_parts)).strip(" -;,"),
            normalize_space(" ".join(note_parts)).strip(" -;,"),
        )
        if entry and _imperial_handle_entry_is_valid(entry):
            entries.append(entry)
        index = max(cursor, index + 1)
    return _clean_handle_entries(entries)


def _imperial_extract_delayed_handles(lines: list[str]) -> list[str]:
    entries: list[str] = []
    index = 0
    while index < len(lines):
        line = normalize_space(lines[index])
        field_key, _ = _imperial_match_field_label(line)
        if not line or (_looks_like_imperial_handle_stop_line(line) and field_key not in {"handles", "handles_base", "handles_overheads", "custom_handles"}):
            index += 1
            continue
        candidate = ""
        tail_match = re.match(r"(?i)^HANDLES?\s*[:\-]?\s*(.*)$", line)
        if tail_match:
            tail = normalize_space(tail_match.group(1))
            if tail and not _imperial_handle_value_looks_noisy(tail):
                candidate = tail
        elif re.search(r"(?i)\bno handles?\b", line):
            candidate = line
        elif re.search(r"(?i)\b(?:touch catch|recessed rail|finger pull)\b", line):
            candidate = line
        elif re.search(r"(?i)\b(?:fingerpull|finger pull|pto)\b", line):
            candidate = line
        elif re.search(r"(?i)\bhandle\b", line) and not _imperial_handle_value_looks_noisy(line):
            candidate = line

        if not candidate:
            index += 1
            continue
        parts = [candidate]
        cursor = index + 1
        skipped_kick = False
        while cursor < len(lines):
            next_line = normalize_space(lines[cursor])
            if not next_line:
                cursor += 1
                continue
            if parts and re.match(r"(?i)^(?:base|upper|tall)\s*-\s*", next_line):
                break
            next_key, _ = _imperial_match_field_label(next_line)
            if re.match(r"(?i)^kickboards?\b", next_line):
                skipped_kick = True
                cursor += 1
                continue
            if next_key and next_key not in {"handles", "handles_base", "handles_overheads", "custom_handles"}:
                break
            if _looks_like_imperial_handle_stop_line(next_line):
                break
            if _imperial_is_supplier_only_line(next_line):
                cursor += 1
                continue
            if re.search(r"(?i)\b(?:handle|touch catch|recessed|finger pull|fingerpull|pto|kethy|momo|barrington|part number|matt brass|matt black|brushed nickel|chrome|nickel|brass|anodised|so-[a-z0-9-]+|bepl\d+)\b", next_line):
                parts.append(next_line)
                cursor += 1
                continue
            if skipped_kick and re.search(r"(?i)\b(?:momo|part number|matt brass|matt black|brushed nickel|anodised|so-[a-z0-9-]+|bepl\d+)\b", next_line):
                parts.append(next_line)
                cursor += 1
                continue
            if re.fullmatch(r"(?i)doors?", next_line):
                parts.append(next_line)
                cursor += 1
                continue
            break

        candidate = normalize_space(" ".join(parts))
        candidate = re.sub(r"(?i)^Tall Pantry Doors\s*-\s*", "", candidate)
        candidate = re.sub(r"(?i)^HANDLES?\s*[:\-]?\s*", "", candidate)
        candidate = re.sub(r"(?i)^to\s+overheads?\s+", "", candidate)
        candidate = re.sub(r"(?i)^base\s+cabs?\s+", "", candidate)
        candidate = re.sub(r"(?i)\bpolytec\b$", "", candidate).strip(" -;,")
        candidate = normalize_space(candidate).strip(" -;,")
        if candidate:
            entries.append(candidate)
        index = max(cursor, index + 1)
    return _clean_handle_entries(entries)


def _imperial_extract_fragment_handle_entries(lines: list[str]) -> list[str]:
    has_base_handle_label = any("HANDLES - BASE CABS" in normalize_space(line).upper() for line in lines)
    title_index = next((index for index, line in enumerate(lines) if _looks_like_imperial_section_title_line(line)), -1)
    boundary_index = title_index
    if boundary_index < 0:
        boundary_index = next(
            (
                index
                for index, line in enumerate(lines)
                if re.match(r"(?i)^(?:ceiling height|shadowline|bulkhead|area / item|base cabinetry colour|upper cabinetry colour|benchtop|kickboards?)\b", normalize_space(line))
            ),
            len(lines),
        )
    pretitle_lines = [normalize_space(line) for line in lines[:boundary_index] if normalize_space(line)]
    posttitle_lines = [normalize_space(line) for line in lines[boundary_index + 1 :] if normalize_space(line)]
    if not pretitle_lines or not posttitle_lines:
        return []

    description_clusters: list[str] = []
    index = 0
    while index < len(pretitle_lines):
        line = pretitle_lines[index]
        if not _is_handle_description_like(line):
            index += 1
            continue
        parts = [line]
        cursor = index + 1
        while cursor < len(pretitle_lines):
            next_line = pretitle_lines[cursor]
            if _imperial_match_field_label(next_line)[0] or _looks_like_imperial_section_title_line(next_line):
                break
            if re.search(r"(?i)\b(?:so-[a-z0-9-]+|bepl\d+|part number:|matt\b|brushed\b|satin\b|\d+\s*mm\b)\b", next_line):
                parts.append(next_line)
                cursor += 1
                continue
            break
        description_clusters.append(normalize_space(" ".join(parts)).strip(" -;,"))
        index = max(cursor, index + 1)

    posttitle_descriptions = [
        line
        for line in posttitle_lines
        if _is_handle_description_like(line)
        and not re.search(r"(?i)\b(?:450mm\s+pull-out|bin\s+hettich)\b", line)
    ]
    if (len(description_clusters) != 1 or posttitle_descriptions) and not has_base_handle_label:
        return []

    supplier = next(
        (
            normalize_brand_casing_text(line)
            for line in posttitle_lines
            if normalize_brand_casing_text(line).upper() in {item.upper() for item in IMPERIAL_HANDLE_SUPPLIER_HINTS}
        ),
        "",
    )
    if not supplier:
        return []

    note_parts: list[str] = []
    index = 0
    while index < len(posttitle_lines):
        line = posttitle_lines[index]
        if not _is_handle_note_like(line):
            index += 1
            continue
        parts = [line]
        cursor = index + 1
        while cursor < len(posttitle_lines):
            next_line = posttitle_lines[cursor]
            if _imperial_match_field_label(next_line)[0] or _looks_like_imperial_handle_stop_line(next_line):
                break
            if re.fullmatch(r"(?i)(?:doors?|drawers?|uppers?|only)", next_line):
                parts.append(next_line)
                cursor += 1
                continue
            break
        note_parts.append(normalize_space(" ".join(parts)).strip(" -;,"))
        index = max(cursor, index + 1)
    note = note_parts[0] if note_parts else ""
    entries: list[str] = []
    if len(description_clusters) == 1:
        entry = _compose_supplier_description_note(supplier, description_clusters[0], note)
        entries = [entry] if entry else []
        if entries:
            return entries

    if has_base_handle_label:
        supplier_index = next(
            (
                index
                for index, line in enumerate(lines)
                if normalize_brand_casing_text(line).upper() in {item.upper() for item in IMPERIAL_HANDLE_SUPPLIER_HINTS}
            ),
            -1,
        )
        supplier = normalize_brand_casing_text(lines[supplier_index]) if supplier_index >= 0 else ""
        description_parts: list[str] = []
        note_parts: list[str] = []
        for index, line in enumerate(lines):
            normalized = normalize_space(line)
            if not normalized or _looks_like_imperial_handle_stop_line(normalized):
                if note_parts:
                    break
                continue
            if supplier_index >= 0 and index < supplier_index:
                continue
            if re.search(r"(?i)\b(?:rappana|elsa cabinetry knob|cabinetry pull|pull extended|knob|momo|titus|lincoln sentry|danes|voda|brushed|matt|chrome|nickel|part number|so-[a-z0-9-]+|bepl\d+|\d+\s*mm)\b", normalized):
                cleaned = normalize_brand_casing_text(normalized).replace("每", "").strip(" -;,")
                if cleaned:
                    description_parts.append(cleaned)
                continue
            if re.search(r"(?i)\b(?:supplied by client|installed by imperial|installed horizontally|installed vertically)\b", normalized):
                note_parts.append(normalize_brand_casing_text(normalized))
        description = normalize_space(" ".join(description_parts)).strip(" -;,")
        note = normalize_space(" ".join(note_parts)).strip(" -;,")
        fallback_entry = _compose_supplier_description_note(supplier, description, note)
        if fallback_entry and _imperial_handle_entry_is_valid(fallback_entry):
            return [fallback_entry]
    return []


def _imperial_extract_fragment_cabinetry_overrides(lines: list[str]) -> dict[str, str]:
    overrides = {"base": "", "upper": "", "upper_tall": ""}
    label_indexes: dict[str, int] = {}
    bench_index = -1
    for index, line in enumerate(lines):
        field_key, _ = _imperial_match_field_label(line)
        if field_key in {"base", "upper", "upper_tall"} and field_key not in label_indexes:
            label_indexes[field_key] = index
        if field_key == "bench_tops" and bench_index < 0:
            bench_index = index
    if not label_indexes or bench_index < 0:
        return overrides
    label_start = min(label_indexes.values())
    supplier_block = lines[label_start : min(len(lines), bench_index + 20)]
    material_end = next(
        (
            index
            for index in range(bench_index + 1, len(lines))
            if re.match(r"(?i)^(?:hinges\s*&\s*drawer runners|floor type\s*&\s*kick refacing required|handles|accessories|gpo'?s|bin|hamper|area / item)\b", normalize_space(lines[index]))
        ),
        min(len(lines), bench_index + 6),
    )
    material_block = lines[label_start:material_end]
    suppliers = [
        normalize_brand_casing_text(line)
        for line in supplier_block
        if _imperial_is_supplier_only_line(line) and normalize_brand_casing_text(line).upper() in {item.upper() for item in CABINETRY_SUPPLIER_HINTS}
    ]
    supplier = suppliers[0] if suppliers else ""
    material_parts = [
        normalize_brand_casing_text(line)
        for line in material_block
        if not _imperial_match_field_label(line)[0]
        and not _imperial_is_supplier_only_line(line)
        and _looks_like_imperial_materialish_line(line)
        and not re.search(r"(?i)\b(?:soft close|tiles?|carpet|stone|waterfall|mitred|caesarstone|smartstone|wk stone)\b", line)
    ]
    material_parts = _unique(material_parts)
    if not material_parts:
        return overrides
    composed = _imperial_compose_material_text(supplier, material_parts)
    if not composed:
        return overrides
    for key in overrides:
        if key in label_indexes:
            overrides[key] = composed
    return overrides


def _imperial_extract_fragment_accessory_entries(lines: list[str]) -> list[str]:
    entries: list[str] = []
    index = 0
    while index < len(lines):
        line = normalize_space(lines[index])
        field_key, tail = _imperial_match_field_label(line)
        if field_key not in {"bin", "gpo", "hamper"}:
            index += 1
            continue
        supplier = ""
        parts: list[str] = [tail] if tail else []
        cursor = index + 1
        while cursor < len(lines):
            next_line = normalize_space(lines[cursor])
            if not next_line:
                cursor += 1
                continue
            next_key, next_tail = _imperial_match_field_label(next_line)
            if next_key and next_key not in {"handles", field_key}:
                break
            if next_key == "handles":
                if re.search(r"(?i)\b(?:pull-out|bin|basket|insert|drawer gpo|gpo|hamper)\b", next_tail):
                    parts.append(next_tail)
                    cursor += 1
                    continue
                break
            if _is_imperial_field_stop_line(next_line) or _looks_like_imperial_section_title_line(next_line):
                break
            if _imperial_is_supplier_only_line(next_line):
                supplier = normalize_brand_casing_text(next_line)
                cursor += 1
                continue
            parts.append(next_line)
            cursor += 1
        description = _imperial_clean_accessories_value(parts)
        if description:
            if field_key == "bin" and "bin" not in description.lower():
                description = f"{description} Bin"
            elif field_key == "gpo" and "gpo" not in description.lower():
                description = f"GPO - {description}"
            elif field_key == "hamper" and "hamper" not in description.lower():
                description = f"Hamper - {description}"
            entries.append(_compose_supplier_description_note(supplier, description))
        index = max(cursor, index + 1)
    return _clean_accessory_entries(entries)


def _imperial_collect_page_fields(page_text: str) -> dict[str, Any]:
    lines = _preprocess_imperial_lines([normalize_space(line) for line in page_text.split("\n") if normalize_space(line)])
    fields = _imperial_collect_fields(lines)
    bench_value = fields.get("bench_tops", "")
    if bench_value and not re.search(r"(?i)\b(?:edge|waterfall|mitred|arissed|pencil round|square edge)\b", bench_value):
        bench_index = next((index for index, line in enumerate(lines) if _imperial_match_field_label(line)[0] == "bench_tops"), -1)
        if bench_index >= 0:
            edge_lines: list[str] = []
            cursor = bench_index + 1
            while cursor < len(lines):
                next_line = normalize_space(lines[cursor])
                if _imperial_match_field_label(next_line)[0] or _is_imperial_field_stop_line(next_line) or _looks_like_imperial_auxiliary_row(next_line):
                    break
                if re.search(r"(?i)\b(?:edge|waterfall|mitred|arissed|pencil round|square edge)\b", next_line):
                    edge_line = normalize_brand_casing_text(next_line)
                    edge_line = re.sub(r"(?i)^\d+\s*mm\s+", "", edge_line).strip(" -;,")
                    if edge_line:
                        edge_lines.append(edge_line)
                cursor += 1
            if edge_lines:
                suffix = " - ".join(_unique(edge_lines))
                if suffix and suffix.lower() not in bench_value.lower():
                    fields["bench_tops"] = f"{bench_value} - {suffix}".strip(" -")
    overrides: dict[str, Any] = {
        "bench_tops_other": "",
        "bench_tops_wall_run": "",
        "bench_tops_island": "",
        "splashback": "",
        "feature_cabinetry": "",
        "accessories_list": [],
        "delayed_handles": [],
        "bulkhead": "",
        "base": "",
        "upper": "",
        "upper_tall": "",
        "flooring": "",
        "soft_close_text": "",
    }
    if any(line.upper().startswith("BENCHTOP+ SPLASHBACK") for line in lines):
        overrides.update(_imperial_extract_combined_bench_splash_fields(lines))
        fields.pop("bench_tops", None)
        fields.pop("splashback", None)
    delayed_benchtop = _imperial_extract_delayed_benchtop(lines)
    if delayed_benchtop and (
        not (fields.get("bench_tops") or overrides["bench_tops_other"] or overrides["bench_tops_wall_run"])
        or _imperial_benchtop_value_looks_noisy(fields.get("bench_tops", ""))
    ):
        overrides["bench_tops_other"] = delayed_benchtop
        fields.pop("bench_tops", None)
    benchtop_fallback = _imperial_extract_freeform_benchtop(lines)
    if benchtop_fallback and not (fields.get("bench_tops") or overrides["bench_tops_other"] or overrides["bench_tops_wall_run"]):
        overrides["bench_tops_other"] = benchtop_fallback
    feature_cabinetry = _imperial_extract_feature_cabinetry_material(lines)
    if feature_cabinetry:
        overrides["feature_cabinetry"] = feature_cabinetry
    accessory_entries = _imperial_extract_accessory_entries(lines)
    if accessory_entries:
        overrides["accessories_list"] = accessory_entries
    fragment_accessory_entries = _imperial_extract_fragment_accessory_entries(lines)
    if fragment_accessory_entries:
        overrides["accessories_list"] = _merge_lists(overrides["accessories_list"], fragment_accessory_entries)
    delayed_handles = _imperial_extract_delayed_handles(lines)
    if delayed_handles:
        overrides["delayed_handles"] = delayed_handles
    structured_handles = _imperial_extract_structured_handle_rows(lines)
    if structured_handles:
        overrides["delayed_handles"] = _merge_lists(overrides["delayed_handles"], structured_handles)
    fragment_handles = _imperial_extract_fragment_handle_entries(lines)
    if fragment_handles:
        overrides["delayed_handles"] = _merge_lists(overrides["delayed_handles"], fragment_handles)
    prelabel_overrides = _imperial_extract_prelabel_field_overrides(lines)
    for key, value in prelabel_overrides.items():
        if value:
            overrides[key] = value
    fragment_cabinetry_overrides = _imperial_extract_fragment_cabinetry_overrides(lines)
    for key, value in fragment_cabinetry_overrides.items():
        if value:
            overrides[key] = value
    soft_close_text, flooring_text = _imperial_extract_soft_close_and_flooring(page_text, lines)
    if soft_close_text:
        overrides["soft_close_text"] = soft_close_text
    if flooring_text:
        overrides["flooring"] = flooring_text
    bulkhead = _imperial_clean_bulkhead_value(
        _imperial_extract_inline_value(
            page_text,
            "Bulkhead:",
            ("Shadowline:", "Hinges & Drawer Runners:", "AREA / ITEM", "SPLASHBACK", "BENCHTOP", "Ceiling height:", "Cabinetry Height:"),
        )
    )
    if bulkhead:
        overrides["bulkhead"] = bulkhead
    return {"lines": lines, "fields": fields, "overrides": overrides}


def _imperial_extract_combined_bench_splash_fields(lines: list[str]) -> dict[str, str]:
    block: list[str] = []
    capturing = False
    for line in lines:
        if line.upper().startswith("BENCHTOP+ SPLASHBACK"):
            capturing = True
            continue
        if capturing:
            field_key, _ = _imperial_match_field_label(line)
            if field_key and field_key not in {"bench_tops", "splashback"} and not re.match(r"(?i)^BENCHTOP\s+(?:ON\s+ISLAND|AREA\s+WITH\s+COOKTOP)\b", line):
                break
            if _is_imperial_field_stop_line(line) and not field_key:
                break
        if capturing:
            block.append(line)
    block_text = normalize_space(" ".join(block))
    supplier = next((normalize_brand_casing_text(line) for line in block if _imperial_is_supplier_only_line(line)), "")
    thickness_match = re.search(r"\b(\d+\s*mm)\b", block_text, re.IGNORECASE)
    thickness = thickness_match.group(1) if thickness_match else ""
    material = next(
        (
            normalize_brand_casing_text(line)
            for line in block
            if "(" in line and not _imperial_is_supplier_only_line(line) and "SPLASHBACK" not in line.upper()
        ),
        "",
    )
    edge_parts = [
        normalize_brand_casing_text(line)
        for line in block
        if re.search(r"(?i)\b(edge|pencil round|mitred apron)\b", line)
        and "SPLASHBACK" not in line.upper()
        and "BENCHTOP ON ISLAND" not in line.upper()
        and "BENCHTOP AREA WITH COOKTOP" not in line.upper()
    ]
    base_value = _imperial_compose_material_text(supplier, [thickness, material, *edge_parts])
    island_match = re.search(r"(?i)(BENCHTOP ON ISLAND.*?)(?=$)", block_text)
    island_note = normalize_space(island_match.group(1)) if island_match else ""
    wall_match = re.search(r"(?i)(BENCHTOP AREA WITH COOKTOP.*?)(?=$)", block_text)
    wall_note = normalize_space(wall_match.group(1)) if wall_match else ""
    splash_note = ""
    splash_match = re.search(r"(?i)SPLASHBACK\s*-\s*(.*?)(?=Caesarstone\b|BENCHTOP AREA WITH COOKTOP|BASE CABINETRY COLOUR|$)", block_text)
    if splash_match:
        splash_note = normalize_space(splash_match.group(1))
    elif re.search(r"(?i)plus Splashback", block_text):
        plus_match = re.search(r"(?i)plus Splashback\s+(.*?)(?=Caesarstone\b|BENCHTOP ON ISLAND|$)", block_text)
        splash_note = normalize_space(plus_match.group(1)) if plus_match else ""
    if "BENCHTOP AREA WITH COOKTOP" in wall_note.upper() and "SPLASHBACK TO REMAIN" in wall_note.upper():
        wall_note = wall_note.replace(" BUT ", " ").strip()
    return {
        "bench_tops_wall_run": f"{base_value} - {wall_note}".strip(" -") if wall_note else base_value,
        "bench_tops_island": f"{base_value} - {island_note}".strip(" -") if island_note else "",
        "bench_tops_other": "",
        "splashback": f"{base_value} - {splash_note}".strip(" -") if splash_note else "",
    }


def _imperial_extract_delayed_benchtop(lines: list[str]) -> str:
    if any(line.upper().startswith("BENCHTOP+ SPLASHBACK") for line in lines):
        return ""
    bench_indexes = [index for index, line in enumerate(lines) if line.upper().startswith("BENCHTOP")]
    if not bench_indexes:
        return ""
    if any(re.search(r"(?i)\b(?:laminate\s+benchtop|benchtop\s+laminate)\b", line) for line in lines):
        for index, line in enumerate(lines):
            if not re.search(r"(?i)\b(?:laminate\s+benchtop|benchtop\s+laminate)\b", line):
                continue
            material = normalize_brand_casing_text(lines[index - 1]) if index > 0 and not _imperial_match_field_label(lines[index - 1])[0] else ""
            profile = normalize_brand_casing_text(lines[index + 1]) if index + 1 < len(lines) and not _imperial_match_field_label(lines[index + 1])[0] else ""
            label = "Laminate Benchtop"
            parts = [part for part in (material, label, profile) if part]
            return " - ".join(parts)
    search_start = bench_indexes[0] + 1
    ceiling_indexes = [index for index, line in enumerate(lines) if line.startswith("Ceiling height:")]
    if ceiling_indexes:
        search_start = max(search_start, ceiling_indexes[-1] + 1)
    search_lines = lines[search_start:]
    supplier = next(
        (
            normalize_brand_casing_text(line)
            for line in search_lines
            if _imperial_is_supplier_only_line(line) and normalize_brand_casing_text(line) in {"Caesarstone", "Ceasarstone", "Smartstone", "WK Stone", "Laminex", "Polytec"}
        ),
        "",
    )
    thickness = ""
    thickness_index = -1
    for index, line in enumerate(search_lines):
        match = re.search(r"\b\d+\s*mm\b", line, re.IGNORECASE)
        if not match:
            continue
        thickness = match.group(0)
        thickness_index = index
        break
    material = ""
    candidate_lines = search_lines[thickness_index + 1 :] if thickness_index >= 0 else search_lines
    for line in candidate_lines:
        normalized = normalize_brand_casing_text(line)
        upper = normalized.upper()
        if _imperial_is_supplier_only_line(normalized):
            continue
        if "HANDLES" in upper or "VERTICAL ON" in upper or "HORIZONTAL ON" in upper or "SPRING FREE" in upper:
            continue
        if re.fullmatch(r"(?i)\d+\s*mm(?:\s+stone)?", normalized):
            continue
        if re.search(r"(?i)\b(?:calacattra|calacatta|organic white|frosty carrina|carrina|stone)\b", normalized) or re.search(r"\(\d{3,}\)", normalized) or re.search(r"^\d{3,4}\b", normalized):
            material = normalized
            break
    if material and re.search(r"(?i)\b(?:calacattra|calacatta|carrina|stone|caesarstone|smartstone|wk stone)\b", material):
        stone_supplier = next(
            (
                normalize_brand_casing_text(line)
                for line in search_lines
                if _imperial_is_supplier_only_line(line) and normalize_brand_casing_text(line) in {"Caesarstone", "Ceasarstone", "Smartstone", "WK Stone"}
            ),
            "",
        )
        if stone_supplier:
            supplier = stone_supplier
    extra = next(
        (
            normalize_brand_casing_text(line)
            for line in search_lines
            if re.search(r"(?i)\b(waterfall end|waterfall ends|pencil round edge|mitred apron edge|square edge)\b", line)
            and "HANDLES" not in line.upper()
        ),
        "",
    )
    if not any((supplier, thickness, material, extra)):
        return ""
    return _imperial_compose_material_text(supplier, [thickness, material, extra])


def _imperial_benchtop_value_looks_noisy(value: str) -> bool:
    text = normalize_space(value)
    if not text:
        return False
    noisy_tokens = (
        "JOINERY SELECTION SHEET",
        "Address:",
        "Client:",
        "Date:",
        "Document Ref:",
        "ASHGROVE",
        "REGENTS PARK",
    )
    return any(token.lower() in text.lower() for token in noisy_tokens)


def _imperial_extract_feature_cabinetry_material(lines: list[str]) -> str:
    if "FEATURE CABINETRY COLOUR" not in " ".join(lines):
        return ""
    block: list[str] = []
    capture = False
    for line in lines:
        if line.upper().startswith("FEATURE CABINETRY COLOUR"):
            tail = normalize_space(re.sub(r"(?i)^FEATURE CABINETRY COLOUR\b", "", line)).strip(" :-")
            if tail:
                block.append(tail)
            capture = True
            continue
        if capture and (_imperial_match_field_label(line)[0] or _is_imperial_field_stop_line(line)):
            break
        if capture:
            block.append(line)
    supplier = ""
    material_parts: list[str] = []
    profile_parts: list[str] = []
    for line in block:
        normalized = normalize_brand_casing_text(line)
        if _imperial_is_supplier_only_line(normalized):
            supplier = normalized
            continue
        if re.search(r"(?i)\boverheads?\b|\bbar back\b", normalized):
            continue
        if re.search(r"(?i)\bprofile\b", normalized):
            profile_parts.append(normalized)
            continue
        material_parts.append(normalized)
    return _imperial_compose_material_text(supplier, [*material_parts, *profile_parts])


def _imperial_extract_accessory_entries(lines: list[str]) -> list[str]:
    entries: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.upper().startswith("ACCESSORIES"):
            index += 1
            continue
        entry_parts: list[str] = []
        tail = normalize_space(re.sub(r"(?i)^ACCESSORIES?\b", "", line)).strip(" :-")
        if tail:
            entry_parts.append(tail)
        index += 1
        while index < len(lines):
            next_line = normalize_space(lines[index])
            if not next_line or _imperial_match_field_label(next_line)[0] or next_line.upper().startswith("ACCESSORIES"):
                break
            if re.match(r"(?i)^product code\s*:", next_line):
                index += 1
                continue
            if re.match(r"(?i)^installed\b", next_line):
                break
            if _imperial_is_supplier_only_line(next_line):
                index += 1
                continue
            if entry_parts and re.match(r"(?i)^(?:\d+\s*x\b|\d+\s*mm\b|island drawer gpo\b|gpo\b|bin\b|hamper\b|wardrobe rail\b|tanova\b|oe\b|veronar\b)", next_line):
                entry = normalize_space(" ".join(entry_parts)).strip(" -;,")
                entry = re.sub(r"(?i)\binstal(?:l(?:ed)?)?$", "", entry).strip(" -;,")
                if entry:
                    entries.append(normalize_brand_casing_text(entry))
                entry_parts = [next_line]
                index += 1
                continue
            entry_parts.append(next_line)
            index += 1
        entry = normalize_space(" ".join(entry_parts)).strip(" -;,")
        entry = re.sub(r"(?i)\binstal(?:l(?:ed)?)?$", "", entry).strip(" -;,")
        if entry:
            entries.append(normalize_brand_casing_text(entry))
        continue
    return _unique(entries)


def _imperial_extract_freeform_benchtop(lines: list[str]) -> str:
    for index, line in enumerate(lines):
        if "Benchtop" not in line and "BENCHTOP" not in line:
            continue
        if line.upper().startswith("BENCHTOP") and _imperial_match_field_label(line)[0]:
            continue
        candidate_parts = [line]
        cursor = index + 1
        while cursor < len(lines):
            next_line = lines[cursor]
            if _imperial_match_field_label(next_line)[0] or _is_imperial_field_stop_line(next_line):
                break
            if _imperial_is_supplier_only_line(next_line):
                break
            candidate_parts.append(next_line)
            cursor += 1
        candidate = normalize_space(" ".join(candidate_parts)).strip(" -;,")
        candidate = re.sub(r"(?i)^BENCHTOPS?\b", "", candidate).strip(" -;,")
        if candidate:
            return normalize_brand_casing_text(candidate)
    return ""


def _imperial_extract_prelabel_cabinetry_material(lines: list[str]) -> str:
    return ""


def _imperial_extract_prelabel_field_overrides(lines: list[str]) -> dict[str, str]:
    overrides = {"base": "", "upper": "", "upper_tall": ""}
    for index, line in enumerate(lines):
        field_key, tail = _imperial_match_field_label(line)
        if field_key not in {"base", "upper", "upper_tall"}:
            continue
        normalized_tail = normalize_space(tail)
        if normalized_tail and not _imperial_is_supplier_only_line(normalized_tail):
            continue
        previous = normalize_space(lines[index - 1]) if index > 0 else ""
        if not previous:
            continue
        if _imperial_match_field_label(previous)[0] or _looks_like_imperial_auxiliary_row(previous) or _is_imperial_page_noise_line(previous):
            continue
        if not _looks_like_imperial_materialish_line(previous):
            continue
        if re.search(r"(?i)\b(?:handle|momo|kethy|barrington|part number|matt brass|matt black|chrome|nickel|brass)\b", previous):
            continue
        supplier = normalize_brand_casing_text(normalized_tail) if normalized_tail and _imperial_is_supplier_only_line(normalized_tail) else ""
        composed = _imperial_compose_material_text(supplier, [previous])
        if composed:
            overrides[field_key] = composed
    return overrides


def _imperial_override_looks_like_material(value: str) -> bool:
    text = normalize_space(value)
    if not text:
        return False
    if _imperial_is_supplier_only_line(text):
        return False
    if re.search(r"(?i)\b(?:handle|momo|kethy|barrington|part number|matt brass|matt black|chrome|nickel|brass|touch catch|finger pull)\b", text):
        return False
    return _looks_like_imperial_materialish_line(text)


def _imperial_material_field_needs_override(existing: str, override: str) -> bool:
    current = normalize_space(existing)
    candidate = normalize_space(override)
    if not candidate or not _imperial_override_looks_like_material(candidate):
        return False
    if not current:
        return True
    if _imperial_is_supplier_only_line(current):
        return True
    if re.search(r"(?i)\bkickboards?\b", current):
        return True
    if re.search(r"(?i)\b(?:cabinetry pull|knob|installed horizontally|installed vertically|supplied by client|abi interiors|laminate benchtop)\b", current):
        return True
    if len(current) < len(candidate) and not re.search(r"(?i)\b(?:woodmatt|smooth|matt|stone|laminate|melamine|thermolaminate|vinyl|oak|walnut|white|black|grey|snow)\b", current):
        return True
    return False


def _imperial_clean_bulkhead_value(value: str) -> str:
    lines = [normalize_brand_casing_text(normalize_space(line)) for line in value.split("\n") if normalize_space(line)]
    cleaned = [line for line in lines if line.upper() not in {"IMAGE", "N/A"}]
    return normalize_space(" ".join(cleaned)).strip(" -;,")


def _imperial_extract_inline_value(text: str, start_label: str, stop_labels: tuple[str, ...]) -> str:
    pattern = rf"(?is){re.escape(start_label)}\s*(?P<value>.*?)(?={'|'.join(re.escape(label) for label in stop_labels)}|$)"
    match = re.search(pattern, text)
    if not match:
        return ""
    return normalize_space(match.group("value"))


def _imperial_extract_soft_close_text(page_text: str, lines: list[str]) -> str:
    soft_close, _ = _imperial_extract_soft_close_and_flooring(page_text, lines)
    return soft_close


def _looks_like_soft_close_candidate(text: str) -> bool:
    normalized = normalize_space(text)
    return bool(normalized and re.search(r"(?i)\bsoft\s*close\b|\bnot\s*soft\s*close\b", normalized))


def _looks_like_flooring_candidate(text: str) -> bool:
    normalized = normalize_space(text)
    return bool(normalized and re.search(r"(?i)\b(?:tiled|tiles|tile|hybrid|carpet|timber|vinyl|laminate|engineered|stone)\b", normalized))


def _imperial_extract_flooring_text(page_text: str, lines: list[str]) -> str:
    _, flooring = _imperial_extract_soft_close_and_flooring(page_text, lines)
    return flooring


def _imperial_extract_soft_close_and_flooring(page_text: str, lines: list[str]) -> tuple[str, str]:
    soft_close_candidates: list[str] = []
    flooring_candidates: list[str] = []
    direct_soft_close_candidates: list[str] = []
    direct_flooring_candidates: list[str] = []
    raw_lines = [normalize_space(line) for line in page_text.replace("\r", "\n").split("\n") if normalize_space(line)]
    line_patterns = (
        r"(?is)Hinges\s*&\s*Drawer\s*Runners:\s*(?P<soft>.{0,80}?)Floor\s*Type\s*&\s*Kick\s*refacing\s*required:\s*(?P<floor>.{0,80})",
        r"(?is)Floor\s*Type\s*&\s*Kick\s*refacing\s*required:\s*(?P<floor>.{0,80}?)Hinges\s*&\s*Drawer\s*Runners:\s*(?P<soft>.{0,80})",
    )
    for raw_line in raw_lines:
        for pattern in line_patterns:
            match = re.search(pattern, raw_line)
            if not match:
                continue
            soft = normalize_space(match.group("soft"))
            floor = normalize_space(match.group("floor"))
            if _looks_like_soft_close_candidate(soft):
                soft_close_candidates.append(soft)
                direct_soft_close_candidates.append(soft)
            if _looks_like_flooring_candidate(floor):
                flooring_candidates.append(floor)
                direct_flooring_candidates.append(floor)
    patterns = (
        r"(?is)Hinges\s*&\s*Drawer\s*Runners:\s*(?P<left>.{0,160}?)Floor\s*Type\s*&\s*Kick\s*refacing\s*required:\s*(?P<right>.{0,160})",
        r"(?is)Floor\s*Type\s*&\s*Kick\s*refacing\s*required:\s*(?P<left>.{0,160}?)Hinges\s*&\s*Drawer\s*Runners:\s*(?P<right>.{0,160})",
    )
    for pattern in patterns:
        combined_match = re.search(pattern, page_text)
        if not combined_match:
            continue
        left = normalize_space(combined_match.group("left"))
        right = normalize_space(combined_match.group("right"))
        for candidate in (left, right):
            if _looks_like_soft_close_candidate(candidate):
                soft_close_candidates.append(candidate)
            if _looks_like_flooring_candidate(candidate) and not _looks_like_soft_close_candidate(candidate):
                flooring_candidates.append(candidate)
    for line in lines:
        normalized = normalize_space(line)
        if "HINGES & DRAWER RUNNERS" in normalized.upper():
            tail = normalize_space(re.sub(r"(?is)^.*?Hinges\s*&\s*Drawer\s*Runners:\s*", "", normalized))
            if tail:
                if _looks_like_soft_close_candidate(tail):
                    soft_close_candidates.append(tail)
                if _looks_like_flooring_candidate(tail) and not _looks_like_soft_close_candidate(tail):
                    flooring_candidates.append(tail)
                    direct_flooring_candidates.append(tail)
        if "FLOOR TYPE" in normalized.upper():
            tail = normalize_space(re.sub(r"(?is)^.*?Floor\s*Type\s*&\s*Kick\s*refacing\s*required:\s*", "", normalized))
            if tail:
                if _looks_like_flooring_candidate(tail) and not _looks_like_soft_close_candidate(tail):
                    flooring_candidates.append(tail)
                    direct_flooring_candidates.append(tail)
                if _looks_like_soft_close_candidate(tail):
                    soft_close_candidates.append(tail)
                    direct_soft_close_candidates.append(tail)
    soft_close = next((candidate for candidate in direct_soft_close_candidates if re.search(r"(?i)\bsoft\s*close\b", candidate)), "")
    if not soft_close:
        soft_close = next((candidate for candidate in soft_close_candidates if re.search(r"(?i)\bsoft\s*close\b", candidate)), "")
    cleaned_direct_flooring_candidates = [
        _imperial_clean_flooring_value([candidate])
        for candidate in direct_flooring_candidates
        if _imperial_clean_flooring_value([candidate])
    ]
    flooring = next((candidate for candidate in cleaned_direct_flooring_candidates if candidate != "stone"), "")
    cleaned_flooring_candidates = [
        _imperial_clean_flooring_value([candidate])
        for candidate in flooring_candidates
        if _imperial_clean_flooring_value([candidate])
    ]
    if not flooring:
        flooring = next((candidate for candidate in cleaned_flooring_candidates if candidate != "stone"), "")
    if not flooring and cleaned_direct_flooring_candidates:
        flooring = cleaned_direct_flooring_candidates[0]
    if not flooring and cleaned_flooring_candidates:
        flooring = cleaned_flooring_candidates[0]
    return soft_close, flooring


def _imperial_value_looks_material_note(value: str) -> bool:
    text = normalize_space(value)
    return bool(
        text
        and re.search(r"(?i)\b(?:polytec|caesarstone|smartstone|laminex|wk stone|woodmatt|smooth|matt|vertical grain|horizontal grain|grain)\b", text)
        and not re.search(r"(?i)\b(?:drawer|basket|insert|gpo\s*\d|hamper|rail|tap|sink|basin)\b", text)
    )


def _imperial_merge_material_note(base_value: str, note_value: str) -> str:
    base = normalize_brand_casing_text(base_value)
    note = normalize_brand_casing_text(note_value)
    if not base:
        return note
    supplier = ""
    for known_supplier in sorted(IMPERIAL_SUPPLIER_ONLY_LINES, key=len, reverse=True):
        supplier_pattern = re.compile(rf"(?i)\b{re.escape(known_supplier)}\b")
        if not supplier and supplier_pattern.search(base):
            supplier = normalize_brand_casing_text(known_supplier)
            base = normalize_space(supplier_pattern.sub("", base)).strip(" -;,")
        if supplier_pattern.search(note):
            supplier = normalize_brand_casing_text(known_supplier)
            note = normalize_space(supplier_pattern.sub("", note)).strip(" -;,")
    if re.search(r"(?i)\bvertical grain\b", note):
        note = "VERTICAL GRAIN"
    elif re.search(r"(?i)\bhorizontal grain\b", note):
        note = "HORIZONTAL GRAIN"
    parts = [part for part in (base, note) if part]
    return _imperial_compose_material_text(supplier, parts)


def _imperial_accessory_entries_from_fields(fields: dict[str, str]) -> list[str]:
    entries: list[str] = []
    gpo_value = normalize_space(fields.get("gpo", ""))
    if gpo_value and not _imperial_value_looks_material_note(gpo_value):
        entries.append(f"GPO - {gpo_value}" if gpo_value.upper() != "GPO" else "GPO")
    elif gpo_value:
        entries.append("GPO")
    bin_value = normalize_space(fields.get("bin", "")).replace(" | ", " - ")
    if bin_value:
        entries.append(bin_value if re.search(r"(?i)\bbin\b", bin_value) else f"{bin_value} Bin")
    hamper_value = normalize_space(fields.get("hamper", "")).replace(" | ", " - ")
    if hamper_value:
        entries.append(hamper_value if re.search(r"(?i)\bhamper\b", hamper_value) else f"Hamper - {hamper_value}")
    return _clean_accessory_entries([normalize_brand_casing_text(entry) for entry in entries if entry])


def _imperial_finalize_accessory_entries(values: list[str]) -> list[str]:
    normalized_entries: list[str] = []
    for value in values:
        text = normalize_brand_casing_text(normalize_space(value)).replace(" | ", " - ").strip(" -;,")
        if text:
            normalized_entries.append(text)
    unique_entries = _unique(normalized_entries)
    if any("gpo" in entry.lower() and entry.lower() != "gpo" for entry in unique_entries):
        unique_entries = [entry for entry in unique_entries if entry.lower() != "gpo"]
    filtered_entries: list[str] = []
    for entry in unique_entries:
        lowered = entry.lower()
        if lowered.startswith("hamper - "):
            model = _guess_model(entry)
            compare_text = lowered.replace("hamper - ", "", 1)
            if any(
                other.lower() != lowered
                and (
                    (model and model.lower() in other.lower())
                    or compare_text in other.lower()
                )
                for other in unique_entries
            ):
                continue
        filtered_entries.append(entry)
    return filtered_entries


def _imperial_page_refs(page_nos: list[int]) -> str:
    ordered = [str(page_no) for page_no in page_nos if page_no]
    return ", ".join(ordered)


def _imperial_layout_row_text(row: dict[str, Any]) -> str:
    return normalize_space(
        " ".join(
            part
            for part in (
                str(row.get("value_text", "") or ""),
                str(row.get("supplier_text", "") or ""),
                str(row.get("notes_text", "") or ""),
            )
            if normalize_space(str(part or ""))
        )
    )


def _clean_imperial_layout_fragment(text: str) -> str:
    cleaned = normalize_brand_casing_text(normalize_space(text)).strip(" -;,")
    if not cleaned:
        return ""
    cleaned = re.sub(r"(?i)^#+\s*", "", cleaned)
    cleaned = re.sub(r"(?i)^.*?\bjoinery selection sheet\b", "", cleaned)
    cleaned = re.sub(r"(?i)^.*?\bcolour schedule\b", "", cleaned)
    cleaned = re.sub(
        r"(?i)^\d+\s+[A-Za-z0-9' .,-]+?(?:street|st|court|ct|road|rd|crescent|cres|terrace|tce|boulevard|blvd|drive|dr|avenue|ave|lane|ln)\b(?:,\s*[A-Za-z][A-Za-z' -]+)?\s*",
        "",
        cleaned,
    )
    cleaned = re.sub(
        r"(?i)\bprivate\b\s*-\s*[A-Z][A-Za-z]+(?:[-'][A-Za-z]+)?(?:\s+[A-Z][A-Za-z]+(?:[-'][A-Za-z]+)?){0,3}\s+\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b",
        "",
        cleaned,
    )
    cleaned = re.sub(
        r"(?i)^.*?\b(?:street|st|court|ct|road|rd|crescent|cres|terrace|tce|boulevard|blvd|drive|dr|avenue|ave|lane|ln)\b.*?\bprivate\b\s*-\s*[A-Z][A-Za-z]+(?:[-'][A-Za-z]+)?(?:\s+[A-Z][A-Za-z]+(?:[-'][A-Za-z]+)?){0,3}\s+\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b",
        "",
        cleaned,
    )
    cleaned = re.sub(r"(?i)\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", "", cleaned)
    cleaned = re.sub(r"(?i)\b(?:ceiling height|cabinetry height|ref\.?\s*number|selection required)\b.*$", "", cleaned)
    cleaned = re.sub(r"(?i)\b(?:client|designer|signature|signed date|document ref|address|date)\b\s*:.*$", "", cleaned)
    cleaned = normalize_brand_casing_text(normalize_space(cleaned)).strip(" -;,")
    return cleaned


def _imperial_material_fragment_is_noise(text: str) -> bool:
    cleaned = _clean_imperial_layout_fragment(text)
    if not cleaned:
        return True
    return bool(
        re.search(
            r"(?i)\b(?:client|designer|signature|signed date|document ref|private|supplied by client|installed by imperial|notes?\s+supplier|taphole location)\b",
            cleaned,
        )
        or re.search(r"(?i)\bsoft close\b", cleaned)
        or re.search(r"(?i)\bshadowline\b", cleaned)
        or re.search(r"(?i)\bbulkhead\b", cleaned)
        or re.search(r"(?i)\b(?:sinkware|tapware)\b", cleaned)
    )


def _imperial_layout_row_material_text(
    row: dict[str, Any],
    *,
    drop_value_patterns: tuple[str, ...] = (),
    extra_parts: list[str] | None = None,
) -> str:
    supplier = _clean_imperial_layout_fragment(str(row.get("supplier_text", "") or ""))
    value = _clean_imperial_layout_fragment(str(row.get("value_text", "") or ""))
    notes = _clean_imperial_layout_fragment(str(row.get("notes_text", "") or ""))
    parts: list[str] = []
    for fragment in (value, notes, *(extra_parts or [])):
        cleaned = _clean_imperial_layout_fragment(fragment)
        if not cleaned:
            continue
        if _imperial_material_fragment_is_noise(cleaned):
            continue
        if supplier:
            cleaned = re.sub(rf"(?i)\b{re.escape(supplier)}\b", "", cleaned)
            cleaned = _clean_imperial_layout_fragment(cleaned)
            if not cleaned:
                continue
        if any(re.search(pattern, cleaned) for pattern in drop_value_patterns):
            continue
        parts.extend(_split_material_profile_fragment(cleaned))
    return _imperial_compose_material_text(supplier, parts)


def _split_material_profile_fragment(text: str) -> list[str]:
    cleaned = normalize_brand_casing_text(normalize_space(text)).strip(" -;,")
    if not cleaned:
        return []
    match = re.search(
        r"(?i)\b(laminate benchtop|pencil round edge|mitred apron edge|square edge|arissed|waterfall ends?|10x10 edge|vertical grain|horizontal grain)\b",
        cleaned,
    )
    if not match or match.start() <= 0:
        return [cleaned]
    head = normalize_space(cleaned[: match.start()]).strip(" -;,")
    tail = normalize_space(cleaned[match.start() :]).strip(" -;,")
    return [part for part in (head, tail) if part]


def _imperial_layout_row_handle_entry(row: dict[str, Any]) -> str:
    raw_supplier = normalize_brand_casing_text(str(row.get("supplier_text", "") or "")).strip(" -;,")
    supplier = _clean_imperial_layout_fragment(raw_supplier)
    description = _clean_imperial_layout_fragment(str(row.get("value_text", "") or ""))
    note = _clean_imperial_layout_fragment(str(row.get("notes_text", "") or ""))
    supplier_note_parts: list[str] = []
    if re.search(r"(?i)\bsupplied by client\b", raw_supplier):
        supplier_note_parts.append("Supplied By Client")
    if re.search(r"(?i)\binstalled by imperial\b", raw_supplier):
        supplier_note_parts.append("Installed By Imperial")
    supplier = re.sub(r"(?i)\b(?:supplied by client|installed by imperial|supplied by imperial|by client|by imperial)\b", "", supplier)
    supplier = normalize_brand_casing_text(normalize_space(supplier)).strip(" -;,")
    supplier_hint, description_remainder = _normalize_entry_supplier_text(description)
    if supplier_hint:
        supplier = supplier or supplier_hint
        description = description_remainder
    if supplier_note_parts:
        note = normalize_space(" - ".join([note, *supplier_note_parts]) if note else " - ".join(supplier_note_parts)).strip(" -;,")
    return _compose_supplier_description_note(supplier, description, note)


def _imperial_layout_row_accessory_entry(row: dict[str, Any]) -> str:
    label = normalize_space(str(row.get("row_label", "") or ""))
    label_upper = label.upper()
    raw_supplier = normalize_brand_casing_text(str(row.get("supplier_text", "") or "")).strip(" -;,")
    supplier = _clean_imperial_layout_fragment(raw_supplier)
    description = _clean_imperial_layout_fragment(str(row.get("value_text", "") or ""))
    note = _clean_imperial_layout_fragment(str(row.get("notes_text", "") or ""))
    supplier_note_parts: list[str] = []
    if re.search(r"(?i)\bsupplied by client\b", raw_supplier):
        supplier_note_parts.append("Supplied By Client")
    if re.search(r"(?i)\binstalled by imperial\b", raw_supplier):
        supplier_note_parts.append("Installed By Imperial")
    supplier = re.sub(r"(?i)\b(?:supplied by client|installed by imperial|supplied by imperial|by client|by imperial)\b", "", supplier)
    supplier = normalize_brand_casing_text(normalize_space(supplier)).strip(" -;,")
    supplier_hint, description_remainder = _normalize_entry_supplier_text(description)
    if supplier_hint:
        supplier = supplier or supplier_hint
        description = description_remainder
    if "GPO" in label_upper and description and "GPO" not in description.upper():
        description = f"GPO - {description}"
    if "BIN" in label_upper and description and "BIN" not in description.upper():
        description = f"{description} Bin"
    if "HAMPER" in label_upper and description and "HAMPER" not in description.upper():
        description = f"Hamper - {description}"
    if supplier_note_parts:
        note = normalize_space(" - ".join([note, *supplier_note_parts]) if note else " - ".join(supplier_note_parts)).strip(" -;,")
    return _compose_supplier_description_note(supplier, description, note)


def _imperial_layout_row_fixture_entry(row: dict[str, Any], kind: str) -> str:
    parts = [
        _clean_imperial_layout_fragment(str(row.get("value_text", "") or "")),
        _clean_imperial_layout_fragment(str(row.get("supplier_text", "") or "")),
        _clean_imperial_layout_fragment(str(row.get("notes_text", "") or "")),
    ]
    text = normalize_space(" ".join(part for part in parts if part)).strip(" -;,")
    text = re.sub(r"(?i)\b(?:by client|by builder|by imperial|supplied by client|installed by imperial)\b", "", text)
    text = re.sub(r"(?i)\b(?:client name|signature|signed date|designer|document ref|address|date)\b.*$", "", text)
    text = normalize_space(text).strip(" -;,")
    if kind == "tap":
        text = re.sub(r"(?i)^tap\s+", "", text).strip(" -;,")
        text = re.sub(r"(?i)\b(?:client|private)\b.*$", "", text).strip(" -;,")
        text = re.sub(
            r"(?i)\b[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?){1,3}\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b$",
            "",
            text,
        ).strip(" -;,")
        text = re.sub(
            r"(?i)\b[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?){1,3}\b$",
            "",
            text,
        ).strip(" -;,")
    if kind in {"sink", "basin"}:
        text = re.sub(r"(?i)\b(?:tap ?hole|taphole)\s+location\b.*$", "", text).strip(" -;,")
        text = re.sub(r"(?i)\b(?:tapware|mixer|tap)\s+location\b.*$", "", text).strip(" -;,")
        text = re.sub(r"(?i)\bsink\s+pre-?punched\s+hole\b.*$", "", text).strip(" -;,")
        text = re.sub(r"(?i)\b(?:centre|center|corner)\s+of\s+(?:sink|basin|tub)\b.*$", "", text).strip(" -;,")
    return text


def _imperial_extract_layout_soft_close_and_flooring(section: dict[str, Any]) -> tuple[str, str]:
    soft_close = ""
    flooring = ""
    for row in _section_layout_rows(section):
        label_upper = normalize_space(str(row.get("row_label", "") or "")).upper()
        text = _imperial_layout_row_text(row)
        if "HINGES" in label_upper and "DRAWER" in label_upper:
            soft_close = normalize_soft_close_value(text, keyword="drawer") or normalize_soft_close_value(text) or soft_close
        if "FLOOR TYPE" in label_upper:
            flooring = _imperial_clean_flooring_value([text]) or flooring
    return soft_close, flooring


def _imperial_layout_overlay_from_section(section: dict[str, Any]) -> dict[str, Any]:
    overlay = {
        "bench_tops_wall_run": "",
        "bench_tops_island": "",
        "bench_tops_other": "",
        "splashback": "",
        "door_colours_overheads": "",
        "door_colours_base": "",
        "door_colours_tall": "",
        "door_colours_island": "",
        "door_colours_bar_back": "",
        "toe_kick": [],
        "handles": [],
        "floating_shelf": "",
        "led": "",
        "accessories": [],
        "other_items": [],
        "sink_info": "",
        "basin_info": "",
        "tap_info": "",
        "drawers_soft_close": "",
        "hinges_soft_close": "",
        "flooring": "",
        "bulkhead": "",
        "has_explicit_overheads": False,
        "has_explicit_base": False,
        "has_explicit_tall": False,
        "has_explicit_island": False,
        "has_explicit_bar_back": False,
    }
    room_key = source_room_key(str(section.get("section_key", "")), fallback_key=str(section.get("section_key", "")))
    for row in _section_layout_rows(section):
        label = normalize_space(str(row.get("row_label", "") or ""))
        if not label:
            continue
        label_upper = label.upper()
        row_text_upper = _imperial_layout_row_text(row).upper()
        combined_upper = f"{label_upper} {row_text_upper}".strip()
        material_drop_patterns = (
            r"(?i)^includes\b.*$",
            r"(?i)^coffee station area\b.*$",
        )
        if "BENCHTOP" in label_upper and "SPLASHBACK" not in label_upper:
            material = _imperial_layout_row_material_text(row)
            if not material:
                continue
            if room_key == "kitchen":
                if "ISLAND" in label_upper:
                    overlay["bench_tops_island"] = _merge_text(overlay["bench_tops_island"], material)
                else:
                    overlay["bench_tops_wall_run"] = _merge_text(overlay["bench_tops_wall_run"], material)
            else:
                overlay["bench_tops_other"] = _merge_text(overlay["bench_tops_other"], material)
            continue
        if "SPLASHBACK" in label_upper:
            overlay["splashback"] = _merge_text(overlay["splashback"], _imperial_layout_row_material_text(row))
            continue
        if ("GLASS INLAY" in combined_upper and ("OVERHEAD" in combined_upper or "FEATURE DOORS" in combined_upper)):
            material = _imperial_layout_row_material_text(row)
            overlay["door_colours_overheads"] = _merge_clean_group_text(overlay["door_colours_overheads"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_overheads"] = overlay["has_explicit_overheads"] or bool(material)
            continue
        if "UPPER CABINETRY COLOUR + TALL CABINETS" in label_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_overheads"] = _merge_clean_group_text(overlay["door_colours_overheads"], material, cleaner=_clean_door_colour_value)
            overlay["door_colours_tall"] = _merge_clean_group_text(overlay["door_colours_tall"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_overheads"] = overlay["has_explicit_overheads"] or bool(material)
            overlay["has_explicit_tall"] = overlay["has_explicit_tall"] or bool(material)
            continue
        if "FEATURE TALL CABINETRY COLOUR" in combined_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_tall"] = _merge_clean_group_text(overlay["door_colours_tall"], material, cleaner=_clean_door_colour_value)
            if "BAR BACK" in combined_upper:
                overlay["door_colours_bar_back"] = _merge_clean_group_text(overlay["door_colours_bar_back"], material, cleaner=_clean_door_colour_value)
                overlay["has_explicit_bar_back"] = overlay["has_explicit_bar_back"] or bool(material)
            overlay["has_explicit_tall"] = overlay["has_explicit_tall"] or bool(material)
            continue
        if "UPPER CABINETRY COLOUR" in combined_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_overheads"] = _merge_clean_group_text(overlay["door_colours_overheads"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_overheads"] = overlay["has_explicit_overheads"] or bool(material)
            continue
        if "TALL CABINETRY COLOUR" in combined_upper or "TALL DOORS" in combined_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_tall"] = _merge_clean_group_text(overlay["door_colours_tall"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_tall"] = overlay["has_explicit_tall"] or bool(material)
            continue
        if "BACK WALL & COFFEE NOOK INTERNAL CABINETRY COLOUR" in combined_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_base"] = _merge_clean_group_text(overlay["door_colours_base"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_base"] = overlay["has_explicit_base"] or bool(material)
            continue
        if "BASE CABINETRY COLOUR" in combined_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_base"] = _merge_clean_group_text(overlay["door_colours_base"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_base"] = overlay["has_explicit_base"] or bool(material)
            continue
        if "ISLAND CABINETRY COLOUR" in combined_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_island"] = _merge_clean_group_text(overlay["door_colours_island"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_island"] = overlay["has_explicit_island"] or bool(material)
            continue
        if re.search(r"(?i)\bCABINETRY COLOUR\b", label) and "FEATURE" not in label_upper:
            material = _imperial_layout_row_material_text(row, drop_value_patterns=material_drop_patterns)
            overlay["door_colours_base"] = _merge_clean_group_text(overlay["door_colours_base"], material, cleaner=_clean_door_colour_value)
            overlay["has_explicit_base"] = overlay["has_explicit_base"] or bool(material)
            continue
        if "FLOATING SHELF" in label_upper or "FLOATING SHELV" in label_upper:
            material = _imperial_layout_row_material_text(row)
            overlay["floating_shelf"] = _merge_text(overlay["floating_shelf"], material)
            continue
        if "KICKBOARD" in label_upper:
            kick_value = _imperial_clean_toe_kick_value(
                [
                    str(row.get("value_text", "") or ""),
                    str(row.get("supplier_text", "") or ""),
                    str(row.get("notes_text", "") or ""),
                ]
            )
            if not kick_value:
                kick_value = normalize_brand_casing_text(_imperial_layout_row_text(row))
            if kick_value:
                overlay["toe_kick"] = _merge_lists(_coerce_string_list(overlay["toe_kick"]), [kick_value])
            continue
        if "HANDLES" in label_upper:
            handle_value = _imperial_layout_row_handle_entry(row)
            if handle_value:
                overlay["handles"] = _merge_lists(_coerce_string_list(overlay["handles"]), [handle_value])
            continue
        if "LED" in label_upper:
            overlay["led"] = "Yes"
            continue
        if any(token in label_upper for token in ("GPO", "BIN", "HAMPER", "ACCESSORIES")):
            accessory_value = _imperial_layout_row_accessory_entry(row)
            if accessory_value:
                overlay["accessories"] = _merge_lists(_coerce_string_list(overlay["accessories"]), [accessory_value])
            continue
        if "JEWELLERY INSERT" in label_upper:
            value = _imperial_layout_row_accessory_entry(row)
            if value:
                overlay["other_items"] = _merge_other_items(overlay["other_items"], [{"label": "JEWELLERY INSERT", "value": value}])
            continue
        if "HANGING RAIL" in label_upper or re.fullmatch(r"(?i)RAIL", label):
            value = _imperial_layout_row_accessory_entry(row)
            if value:
                overlay["other_items"] = _merge_other_items(overlay["other_items"], [{"label": "RAIL", "value": value}])
            continue
        if label_upper.startswith("SINKWARE (") or str(row.get("row_kind", "")) == "sink":
            overlay["sink_info"] = _merge_text(overlay["sink_info"], _imperial_layout_row_fixture_entry(row, "sink"))
            continue
        if label_upper.startswith("TAPWARE (") or str(row.get("row_kind", "")) == "tap":
            overlay["tap_info"] = _merge_text(overlay["tap_info"], _imperial_layout_row_fixture_entry(row, "tap"))
            continue
        if label_upper.startswith("BASIN (") or str(row.get("row_kind", "")) == "basin":
            overlay["basin_info"] = _merge_text(overlay["basin_info"], _imperial_layout_row_fixture_entry(row, "basin"))
            continue
    soft_close, flooring = _imperial_extract_layout_soft_close_and_flooring(section)
    if soft_close:
        overlay["drawers_soft_close"] = soft_close
        overlay["hinges_soft_close"] = soft_close
    if flooring:
        overlay["flooring"] = flooring
    return overlay


def _imperial_room_from_section(section: dict[str, Any]) -> RoomRow:
    section_text = str(section.get("text", ""))
    page_entries = list(section.get("raw_page_texts", []) or section.get("page_texts", []))
    fields: dict[str, str] = {}
    accessories: list[str] = []
    delayed_handles: list[str] = []
    other_items: list[dict[str, str]] = []
    bulkhead_text = ""
    soft_close_text = ""
    bench_wall = ""
    bench_island = ""
    bench_other = ""
    splashback_text = ""
    feature_cabinetry = ""
    flooring_text = ""
    section_upper = section_text.upper()
    layout_overlay = _imperial_layout_overlay_from_section(section)
    for page_entry in page_entries:
        page_text = str(page_entry.get("text", ""))
        page_result = _imperial_collect_page_fields(page_text)
        page_fields = page_result["fields"]
        overrides = page_result["overrides"]
        for key, value in page_fields.items():
            if value:
                fields[key] = _merge_text(fields.get(key, ""), value)
        for key in ("base", "upper", "upper_tall"):
            if _imperial_material_field_needs_override(fields.get(key, ""), overrides.get(key, "")):
                fields[key] = overrides[key]
        if overrides.get("bench_tops_wall_run"):
            bench_wall = _merge_text(bench_wall, overrides["bench_tops_wall_run"])
        if overrides.get("bench_tops_island"):
            bench_island = _merge_text(bench_island, overrides["bench_tops_island"])
        if overrides.get("bench_tops_other"):
            bench_other = _merge_text(bench_other, overrides["bench_tops_other"])
        if overrides.get("splashback"):
            splashback_text = _merge_text(splashback_text, overrides["splashback"])
        if overrides.get("feature_cabinetry"):
            feature_cabinetry = _merge_text(feature_cabinetry, overrides["feature_cabinetry"])
        accessories = _merge_lists(accessories, overrides.get("accessories_list", []))
        accessories = _merge_lists(accessories, _imperial_accessory_entries_from_fields(page_fields))
        delayed_handles = _merge_lists(delayed_handles, overrides.get("delayed_handles", []))
        other_items = _merge_other_items(
            other_items,
            [
                {"label": label, "value": page_fields.get(key, "")}
                for key, label in IMPERIAL_CURATED_OTHER_FIELD_KEYS.items()
                if page_fields.get(key, "")
            ],
        )
        if overrides.get("bulkhead") and not bulkhead_text:
            bulkhead_text = overrides["bulkhead"]
        soft_close_candidate = overrides.get("soft_close_text", "")
        if soft_close_candidate and not soft_close_text:
            soft_close_text = soft_close_candidate
        flooring_candidate = overrides.get("flooring", "")
        if flooring_candidate and not flooring_text:
            flooring_text = flooring_candidate

    row = RoomRow(
        room_key=str(section.get("section_key", "")),
        original_room_label=str(section.get("original_section_label", "")),
        source_file=str(section.get("file_name", "")),
        page_refs=_imperial_page_refs(list(section.get("page_nos", []))),
        evidence_snippet=section_text[:300],
        confidence=0.72,
    )
    bench_text = bench_other or fields.get("bench_tops", "")
    row.bench_tops_wall_run = bench_wall
    row.bench_tops_island = bench_island
    if bench_text and not row.bench_tops_wall_run and row.room_key == "kitchen":
        row.bench_tops_wall_run = bench_text
        bench_text = ""
    if bench_text and bench_text != row.bench_tops_wall_run:
        row.bench_tops_other = bench_text
    row.bench_tops = _unique([value for value in (bench_wall, bench_island, bench_text) if value])
    floating_shelf_note = fields.get("gpo", "") if _imperial_value_looks_material_note(fields.get("gpo", "")) else ""
    row.floating_shelf = _imperial_merge_material_note(fields.get("floating_shelf", ""), floating_shelf_note) or fields.get("floating_shelf", "")
    row.splashback = splashback_text or fields.get("splashback", "")
    cabinetry_colour = fields.get("cabinetry_colour", "")
    base_value = _merge_text(fields.get("base", ""), fields.get("base_back_wall", ""))
    row.door_colours_overheads = _merge_text(fields.get("upper", ""), fields.get("upper_tall", ""))
    row.door_colours_overheads = _merge_text(row.door_colours_overheads, fields.get("overhead_feature_cabinetry", ""))
    row.door_colours_tall = _merge_text(fields.get("tall_cabinetry", ""), fields.get("upper_tall", ""))
    row.door_colours_tall = _merge_text(row.door_colours_tall, fields.get("feature_tall_bar_back", ""))
    row.door_colours_base = base_value or (cabinetry_colour if not any(fields.get(key) for key in ("base", "base_back_wall", "upper", "upper_tall", "tall_cabinetry", "island_cabinetry")) else "")
    if fields.get("feature_tall_bar_back"):
        row.door_colours_bar_back = _merge_clean_group_text(row.door_colours_bar_back, fields.get("feature_tall_bar_back", ""), cleaner=_clean_door_colour_value)
    if not row.door_colours_base and "BACK WALL & COFFEE NOOK INTERNAL" in section_upper:
        row.door_colours_base = _imperial_merge_material_note(row.floating_shelf, floating_shelf_note) or row.floating_shelf
    row.door_colours_island = fields.get("island_cabinetry", "")
    if feature_cabinetry:
        row.door_colours_overheads = _merge_clean_group_text(row.door_colours_overheads, feature_cabinetry, cleaner=_clean_door_colour_value)
        row.door_colours_bar_back = _merge_clean_group_text(row.door_colours_bar_back, feature_cabinetry, cleaner=_clean_door_colour_value)
    row.has_explicit_overheads = bool(fields.get("upper") or fields.get("upper_tall") or fields.get("overhead_feature_cabinetry"))
    row.has_explicit_tall = bool(fields.get("upper_tall") or fields.get("tall_cabinetry") or fields.get("feature_tall_bar_back"))
    row.has_explicit_base = bool(fields.get("base") or fields.get("base_back_wall") or cabinetry_colour or row.door_colours_base)
    row.has_explicit_island = bool(fields.get("island_cabinetry"))
    row.has_explicit_bar_back = bool(row.door_colours_bar_back or fields.get("feature_tall_bar_back"))
    row.door_panel_colours = _rebuild_door_panel_colours(row.model_dump())
    toe_kick_text = fields.get("toe_kick", "")
    if toe_kick_text:
        row.toe_kick = [
            item
            for item in [part.strip() for part in toe_kick_text.split(";") if part.strip()]
            if "benchtop" not in item.lower() and "square edge" not in item.lower() and "waterfall" not in item.lower()
        ]
    if row.toe_kick and any("match above" in item.lower() for item in row.toe_kick):
        if row.door_colours_base:
            row.toe_kick = [row.door_colours_base]
    if row.toe_kick and any("overhang to be" in item.lower() for item in row.toe_kick):
        if row.door_colours_base:
            row.toe_kick = [row.door_colours_base]
    for key in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "floating_shelf",
        "splashback",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "sink_info",
        "basin_info",
        "tap_info",
    ):
        value = getattr(row, key, "")
        if isinstance(value, str) and value:
            setattr(row, key, _collapse_repeated_token_sequence(value))
    if row.toe_kick:
        row.toe_kick = _unique(
            [
                _collapse_repeated_token_sequence(item)
                for item in row.toe_kick
                if _collapse_repeated_token_sequence(item)
            ]
        )
    if bulkhead_text:
        row.bulkheads = [_imperial_clean_bulkhead_value(bulkhead_text)]
    row.led = fields.get("led", "")
    row.accessories = _imperial_finalize_accessory_entries(accessories or _coerce_string_list(fields.get("accessories", "")))
    row.other_items = other_items
    handles: list[str] = []
    for key in ("handles_overheads", "handles_base", "handles", "custom_handles"):
        if fields.get(key):
            handles.extend([part for part in fields[key].split("; ") if part])
    cleaned_handles = [
        item
        for item in _clean_handle_entries(handles)
        if not _imperial_handle_value_looks_noisy(item) and _imperial_handle_entry_is_valid(item)
    ]
    if delayed_handles:
        cleaned_handles = _merge_lists(cleaned_handles, [item for item in delayed_handles if _imperial_handle_entry_is_valid(item)])
    row.handles = [
        item
        for item in _clean_handle_entries(cleaned_handles)
        if not _imperial_handle_value_looks_noisy(item) and _imperial_handle_entry_is_valid(item)
    ]
    soft_close = normalize_soft_close_value(soft_close_text, keyword="drawer") or normalize_soft_close_value(soft_close_text)
    if soft_close:
        row.drawers_soft_close = soft_close
        row.hinges_soft_close = soft_close
    row.flooring = flooring_text

    if layout_overlay.get("bench_tops_wall_run"):
        row.bench_tops_wall_run = layout_overlay["bench_tops_wall_run"]
    if layout_overlay.get("bench_tops_island"):
        row.bench_tops_island = layout_overlay["bench_tops_island"]
    if layout_overlay.get("bench_tops_other"):
        row.bench_tops_other = layout_overlay["bench_tops_other"]
    if row.bench_tops_other and row.bench_tops_other in {row.bench_tops_wall_run, row.bench_tops_island}:
        row.bench_tops_other = ""
    if any(
        (
            layout_overlay.get("bench_tops_wall_run"),
            layout_overlay.get("bench_tops_island"),
            layout_overlay.get("bench_tops_other"),
        )
    ):
        row.bench_tops = _unique(
            [
                value
                for value in (
                    row.bench_tops_wall_run,
                    row.bench_tops_island,
                    row.bench_tops_other,
                )
                if value
            ]
        )
    if layout_overlay.get("floating_shelf"):
        row.floating_shelf = layout_overlay["floating_shelf"]
    if layout_overlay.get("splashback"):
        row.splashback = layout_overlay["splashback"]
    if layout_overlay.get("door_colours_overheads"):
        row.door_colours_overheads = layout_overlay["door_colours_overheads"]
    if layout_overlay.get("door_colours_base"):
        row.door_colours_base = layout_overlay["door_colours_base"]
    if layout_overlay.get("door_colours_tall"):
        row.door_colours_tall = layout_overlay["door_colours_tall"]
    if layout_overlay.get("door_colours_island"):
        row.door_colours_island = layout_overlay["door_colours_island"]
    if layout_overlay.get("door_colours_bar_back"):
        row.door_colours_bar_back = layout_overlay["door_colours_bar_back"]
    if layout_overlay.get("toe_kick"):
        row.toe_kick = _coerce_string_list(layout_overlay["toe_kick"])
    if layout_overlay.get("handles"):
        row.handles = _clean_handle_entries(_coerce_string_list(layout_overlay["handles"]))
    if layout_overlay.get("led"):
        row.led = layout_overlay["led"]
    if layout_overlay.get("accessories"):
        row.accessories = _imperial_finalize_accessory_entries(_coerce_string_list(layout_overlay["accessories"]))
    if layout_overlay.get("other_items"):
        row.other_items = layout_overlay["other_items"]
    if layout_overlay.get("sink_info"):
        row.sink_info = layout_overlay["sink_info"]
    if layout_overlay.get("basin_info"):
        row.basin_info = layout_overlay["basin_info"]
    if layout_overlay.get("tap_info"):
        row.tap_info = layout_overlay["tap_info"]
    if layout_overlay.get("drawers_soft_close"):
        row.drawers_soft_close = layout_overlay["drawers_soft_close"]
    if layout_overlay.get("hinges_soft_close"):
        row.hinges_soft_close = layout_overlay["hinges_soft_close"]
    if layout_overlay.get("flooring"):
        row.flooring = layout_overlay["flooring"]
    if layout_overlay.get("bulkhead"):
        row.bulkheads = [layout_overlay["bulkhead"]]
    row.has_explicit_overheads = row.has_explicit_overheads or bool(layout_overlay.get("has_explicit_overheads"))
    row.has_explicit_base = row.has_explicit_base or bool(layout_overlay.get("has_explicit_base"))
    row.has_explicit_tall = row.has_explicit_tall or bool(layout_overlay.get("has_explicit_tall"))
    row.has_explicit_island = row.has_explicit_island or bool(layout_overlay.get("has_explicit_island"))
    row.has_explicit_bar_back = row.has_explicit_bar_back or bool(layout_overlay.get("has_explicit_bar_back"))
    if row.floating_shelf:
        row.floating_shelf = _dedupe_delimited_fragments(_collapse_repeated_token_sequence(row.floating_shelf))
    if row.tap_info:
        row.tap_info = _imperial_layout_row_fixture_entry(
            {"value_text": row.tap_info, "supplier_text": "", "notes_text": ""},
            "tap",
        )
    if row.sink_info:
        row.sink_info = _collapse_repeated_token_sequence(row.sink_info)
    if row.basin_info:
        row.basin_info = _collapse_repeated_token_sequence(row.basin_info)
    if row.bench_tops_wall_run:
        row.bench_tops_wall_run = _collapse_repeated_token_sequence(row.bench_tops_wall_run)
    if row.bench_tops_other:
        row.bench_tops_other = _collapse_repeated_token_sequence(row.bench_tops_other)
    row.door_panel_colours = _rebuild_door_panel_colours(row.model_dump())
    return row


def _imperial_special_section_from_section(section: dict[str, Any]) -> SpecialSectionRow:
    section_text = str(section.get("text", ""))
    lines = _preprocess_imperial_lines([normalize_space(line) for line in section_text.split("\n") if normalize_space(line)])
    fields = _imperial_collect_fields(lines)
    normalized_fields: dict[str, str] = {}
    if fields.get("tall_doors"):
        normalized_fields["Tall"] = fields["tall_doors"]
    if fields.get("toe_kick"):
        normalized_fields["Toe Kick"] = fields["toe_kick"]
    if fields.get("custom_handles"):
        normalized_fields["Handles"] = fields["custom_handles"]
    soft_close_text = _imperial_extract_soft_close_text(section_text, lines)
    soft_close = normalize_soft_close_value(soft_close_text, keyword="drawer") or normalize_soft_close_value(soft_close_text)
    if soft_close:
        normalized_fields["Drawers"] = soft_close
        normalized_fields["Hinges"] = soft_close
    return SpecialSectionRow(
        section_key=str(section.get("section_key", "")),
        original_section_label=str(section.get("original_section_label", "")),
        fields=normalized_fields,
        source_file=str(section.get("file_name", "")),
        page_refs=_imperial_page_refs(list(section.get("page_nos", []))),
        evidence_snippet=section_text[:300],
        confidence=0.72,
    )


def _parse_imperial_documents(
    job_no: str,
    builder_name: str,
    source_kind: str,
    documents: list[dict[str, object]],
    rule_flags: Any = None,
) -> dict[str, Any]:
    rooms: dict[str, RoomRow] = {}
    appliances: list[ApplianceRow] = []
    special_sections: list[SpecialSectionRow] = []
    warnings: list[str] = []
    source_documents: list[dict[str, str]] = []
    room_master_document, room_master_reason = _select_imperial_room_master_document(documents)
    room_master_file = str(room_master_document["file_name"]) if room_master_document else ""
    supplement_files: list[str] = []
    site_address = _extract_site_address_from_documents([room_master_document] if room_master_document else documents) or _extract_site_address_from_documents(documents)

    for document in documents:
        file_name = str(document["file_name"])
        pages = list(document["pages"])
        is_room_master = not room_master_document or document is room_master_document
        if not is_room_master:
            supplement_files.append(file_name)
        source_documents.append(
            {
                "file_name": file_name,
                "role": str(document["role"]),
                "page_count": str(len(pages)),
                "room_role": "room_master" if is_room_master else "supplement",
            }
        )
        full_text = "\n\n".join(str(page.get("text") or page.get("raw_text") or "") for page in pages if page.get("text") or page.get("raw_text"))
        if not full_text.strip():
            warnings.append(f"No extractable text found in {file_name}.")
            continue
        for page in pages:
            if page.get("needs_ocr"):
                warnings.append(f"Low-text page detected in {file_name} page {page['page_no']}.")
        if is_room_master:
            for section in _collect_imperial_sections_for_document(document):
                if section["section_kind"] == "room":
                    row = _imperial_room_from_section(section)
                    rooms[row.room_key] = row
                else:
                    special_sections.append(_imperial_special_section_from_section(section))
        appliances.extend(_extract_appliances(full_text, file_name, pages))

    payload = SnapshotPayload(
        job_no=job_no,
        builder_name=builder_name,
        source_kind=source_kind,
        generated_at=utc_now_iso(),
        site_address=site_address,
        analysis=AnalysisMeta(
            parser_strategy=cleaning_rules.global_parser_strategy(),
            room_master_file=room_master_file,
            room_master_reason=room_master_reason,
            supplement_files=supplement_files,
            ignored_room_like_lines_count=0,
        ),
        rooms=list(rooms.values()),
        special_sections=special_sections,
        appliances=_dedupe_appliances(appliances),
        others={},
        warnings=_unique(warnings),
        source_documents=source_documents,
    )
    return apply_snapshot_cleaning_rules(payload.model_dump(), rule_flags=rule_flags)


def _documents_have_layout_schema(documents: list[dict[str, object]]) -> bool:
    return any(_document_has_layout_schema(document) for document in documents)


def _select_spec_room_master_document(builder_name: str, documents: list[dict[str, object]]) -> tuple[dict[str, object] | None, str]:
    if _is_imperial_builder(builder_name):
        return _select_imperial_room_master_document(documents)
    return select_room_master_document(documents, "spec")


def _collect_spec_sections_for_document(builder_name: str, document: dict[str, object]) -> list[dict[str, Any]]:
    if _is_imperial_builder(builder_name):
        imperial_sections = _collect_imperial_sections_for_document(document)
        if imperial_sections:
            return imperial_sections
    if _document_has_layout_schema(document):
        layout_sections = _collect_layout_sections_for_document({**document, "builder_name": builder_name})
        if layout_sections:
            return layout_sections
    return _collect_room_sections_for_document(document)


def _parse_spec_documents_structure_first(
    job_no: str,
    builder_name: str,
    documents: list[dict[str, object]],
    rule_flags: Any = None,
) -> dict[str, Any]:
    imperial_builder = _is_imperial_builder(builder_name)
    rooms: dict[str, RoomRow] = {}
    appliances: list[ApplianceRow] = []
    special_sections: list[SpecialSectionRow] = []
    warnings: list[str] = []
    source_documents: list[dict[str, str]] = []
    flooring_notes: list[str] = []
    splashback_notes: list[str] = []
    room_master_document, room_master_reason = _select_spec_room_master_document(builder_name, documents)
    room_master_file = str(room_master_document["file_name"]) if room_master_document else ""
    site_address = _extract_site_address_from_documents([room_master_document] if room_master_document else documents) or _extract_site_address_from_documents(documents)
    room_master_keys: set[str] = set()
    supplement_files: list[str] = []
    ignored_room_like_lines_count = 0

    if room_master_document:
        for section in _collect_spec_sections_for_document(builder_name, room_master_document):
            if section.get("section_kind") != "room":
                continue
            if not imperial_builder and str(section.get("page_type", "") or "") != "joinery":
                continue
            original_room_label = source_room_label(
                str(section.get("original_section_label", "")),
                fallback_key=str(section.get("section_key", "")),
            )[:80]
            room_key = source_room_key(original_room_label, fallback_key=str(section.get("section_key", "")))
            if room_key:
                room_master_keys.add(room_key)

    for document in documents:
        file_name = str(document["file_name"])
        pages = list(document["pages"])
        is_room_master = not room_master_document or document is room_master_document
        if not is_room_master:
            supplement_files.append(file_name)
        source_documents.append(
            {
                "file_name": file_name,
                "role": str(document["role"]),
                "page_count": str(len(pages)),
                "room_role": "room_master" if is_room_master else "supplement",
            }
        )
        full_text = "\n\n".join(str(page.get("text") or page.get("raw_text") or "") for page in pages if page.get("text") or page.get("raw_text"))
        if not full_text.strip():
            warnings.append(f"No extractable text found in {file_name}.")
            continue
        for page in pages:
            if page.get("needs_ocr"):
                warnings.append(f"Low-text page detected in {file_name} page {page['page_no']}.")

        for section in _collect_spec_sections_for_document(builder_name, document):
            section_kind = str(section.get("section_kind", "room") or "room")
            if section_kind == "special":
                if imperial_builder and is_room_master:
                    special_sections.append(_imperial_special_section_from_section(section))
                continue

            if imperial_builder and not is_room_master:
                continue
            if not imperial_builder and str(section.get("page_type", "") or "") != "joinery":
                continue

            original_room_label = source_room_label(
                str(section.get("original_section_label", "")),
                fallback_key=str(section.get("section_key", "")),
            )[:80]
            room_key = source_room_key(original_room_label, fallback_key=str(section.get("section_key", "")))
            target_room_key, ignore_reason = _resolve_room_target(room_key, original_room_label, room_master_keys, is_room_master)
            if ignore_reason:
                ignored_room_like_lines_count += 1
                warnings.append(f"Ignored room-like section '{original_room_label}' from {file_name}: {ignore_reason}.")
                continue
            if is_room_master:
                room_master_keys.add(target_room_key)

            if imperial_builder:
                row = _imperial_room_from_section(section)
                row.room_key = target_room_key
                row.original_room_label = original_room_label or row.original_room_label
                if is_room_master:
                    row.source_file = file_name
                rooms[target_room_key] = row
                continue

            chunk = str(section.get("text", "") or "")
            lines = _section_lines(section)
            row = rooms.get(target_room_key) or RoomRow(
                room_key=target_room_key,
                original_room_label=original_room_label,
                source_file=file_name,
            )
            if is_room_master:
                row.original_room_label = original_room_label
                row.source_file = file_name
            section_pages = [
                {"page_no": page_no, "text": text}
                for page_no, text in [
                    (int(page_entry.get("page_no", 0) or 0), str(page_entry.get("text", "") or ""))
                    for page_entry in section.get("page_texts", [])
                    if isinstance(page_entry, dict)
                ]
                if page_no
            ]
            _merge_room_section_into_row(
                row,
                lines,
                chunk,
                file_name,
                section_pages or pages,
                allow_material_fields=is_room_master,
                authoritative_room_section=is_room_master,
            )
            rooms[target_room_key] = row

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
        source_kind="spec",
        generated_at=utc_now_iso(),
        site_address=site_address,
        analysis=AnalysisMeta(
            parser_strategy=cleaning_rules.global_parser_strategy(),
            room_master_file=room_master_file,
            room_master_reason=room_master_reason,
            supplement_files=supplement_files,
            ignored_room_like_lines_count=ignored_room_like_lines_count,
        ),
        rooms=list(rooms.values()),
        special_sections=special_sections,
        appliances=_dedupe_appliances(appliances),
        others={
            "flooring_notes": " | ".join(_unique(flooring_notes)),
            "splashback_notes": " | ".join(_unique(splashback_notes)),
        },
        warnings=_unique(warnings),
        source_documents=source_documents,
    )
    return apply_snapshot_cleaning_rules(payload.model_dump(), rule_flags=rule_flags)


def parse_documents(
    job_no: str,
    builder_name: str,
    source_kind: str,
    documents: list[dict[str, object]],
    rule_flags: Any = None,
) -> dict:
    if source_kind == "spec":
        return _parse_spec_documents_structure_first(job_no, builder_name, documents, rule_flags=rule_flags)

    rooms: dict[str, RoomRow] = {}
    appliances: list[ApplianceRow] = []
    warnings: list[str] = []
    source_documents: list[dict[str, str]] = []
    flooring_notes: list[str] = []
    splashback_notes: list[str] = []
    room_master_document, room_master_reason = select_room_master_document(documents, source_kind)
    room_master_file = str(room_master_document["file_name"]) if room_master_document else ""
    site_address = _extract_site_address_from_documents([room_master_document] if room_master_document else documents) or _extract_site_address_from_documents(documents)
    room_master_keys: set[str] = set()
    supplement_files: list[str] = []
    ignored_room_like_lines_count = 0

    if room_master_document:
        master_full_text = _document_full_text(room_master_document)
        master_sections = _collect_schedule_room_sections([room_master_document]) or _find_room_sections(master_full_text)
        for detected_room_key, chunk in master_sections:
            original_room_label = source_room_label(chunk.split("\n", 1)[0], fallback_key=detected_room_key)[:80]
            room_key = source_room_key(original_room_label, fallback_key=detected_room_key)
            if room_key:
                room_master_keys.add(room_key)

    for document in documents:
        file_name = str(document["file_name"])
        pages = list(document["pages"])
        is_room_master = not room_master_document or document is room_master_document
        if not is_room_master:
            supplement_files.append(file_name)
        source_documents.append(
            {
                "file_name": file_name,
                "role": str(document["role"]),
                "page_count": str(len(pages)),
                "room_role": "room_master" if is_room_master else "supplement",
            }
        )
        full_text = "\n\n".join(str(page.get("text") or page.get("raw_text") or "") for page in pages if page.get("text") or page.get("raw_text"))
        if not full_text.strip():
            warnings.append(f"No extractable text found in {file_name}.")
            continue
        for page in pages:
            if page.get("needs_ocr"):
                warnings.append(f"Low-text page detected in {file_name} page {page['page_no']}.")
        room_sections = _collect_schedule_room_sections([document]) or _find_room_sections(full_text)
        for detected_room_key, chunk in room_sections:
            lines = _preprocess_chunk(chunk)
            original_room_label = source_room_label(chunk.split("\n", 1)[0], fallback_key=detected_room_key)[:80]
            room_key = source_room_key(original_room_label, fallback_key=detected_room_key)
            target_room_key, ignore_reason = _resolve_room_target(room_key, original_room_label, room_master_keys, is_room_master)
            if ignore_reason:
                ignored_room_like_lines_count += 1
                warnings.append(f"Ignored room-like section '{original_room_label}' from {file_name}: {ignore_reason}.")
                continue
            if is_room_master:
                room_master_keys.add(target_room_key)
            row = rooms.get(target_room_key) or RoomRow(
                room_key=target_room_key,
                original_room_label=original_room_label,
                source_file=file_name,
            )
            if is_room_master:
                row.original_room_label = original_room_label
                row.source_file = file_name
            _merge_room_section_into_row(
                row,
                lines,
                chunk,
                file_name,
                pages,
                allow_material_fields=is_room_master,
                authoritative_room_section=is_room_master,
            )
            rooms[target_room_key] = row
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
        site_address=site_address,
        analysis=AnalysisMeta(
            parser_strategy=cleaning_rules.global_parser_strategy(),
            room_master_file=room_master_file,
            room_master_reason=room_master_reason,
            supplement_files=supplement_files,
            ignored_room_like_lines_count=ignored_room_like_lines_count,
        ),
        rooms=list(rooms.values()),
        appliances=_dedupe_appliances(appliances),
        others={
            "flooring_notes": " | ".join(_unique(flooring_notes)),
            "splashback_notes": " | ".join(_unique(splashback_notes)),
        },
        warnings=_unique(warnings),
        source_documents=source_documents,
    )
    return apply_snapshot_cleaning_rules(payload.model_dump(), rule_flags=rule_flags)


def _merge_room_section_into_row(
    row: RoomRow,
    lines: list[str],
    chunk: str,
    file_name: str,
    pages: list[dict[str, object]],
    allow_material_fields: bool = True,
    authoritative_room_section: bool = False,
) -> None:
    if allow_material_fields:
        generic_bench_tops = _collect_field(lines, ["Bench Tops", "Benchtop"])
        wall_run_bench_top = _first_value(_collect_field(lines, ["Back Benchtops", "Wall Run Bench Top"]))
        island_bench_top = _first_value(_collect_field(lines, ["Island Benchtop", "Island Bench Top"]))
        row.bench_tops_wall_run = _merge_text(row.bench_tops_wall_run, wall_run_bench_top)
        row.bench_tops_island = _merge_text(row.bench_tops_island, island_bench_top)
        row.bench_tops_other = _merge_text(row.bench_tops_other, " | ".join(generic_bench_tops))
        if wall_run_bench_top:
            generic_bench_tops.append(f"Back Benchtops {wall_run_bench_top}")
        if island_bench_top:
            generic_bench_tops.append(f"Island Benchtop {island_bench_top}")
        row.bench_tops = _merge_lists(row.bench_tops, _unique(generic_bench_tops))
        row.door_panel_colours = _merge_lists(row.door_panel_colours, _collect_field(lines, DOOR_COLOUR_FIELD_PREFIXES))
        overhead_value = _first_value(_collect_field(lines, ["Overhead Cupboards", "Upper Cabinetry Colour + Tall Cabinets", "Upper Cabinetry Colour"]))
        base_value = _first_value(_collect_field(lines, ["Base Cupboards & Drawers", "Floor Mounted Vanity", "Base Cabinetry Colour"]))
        tall_value = _first_value(_collect_field(lines, ["Tall Cabinets", "Tall Cabinet", "Tall Doors", "Upper Cabinetry Colour + Tall Cabinets"]))
        island_value = _first_value(_collect_field(lines, ["Island Bench Base Cupboards & Drawers"]))
        bar_back_value = _first_value(_collect_field(lines, ["Island Bar Back"]))
        if overhead_value:
            row.has_explicit_overheads = True
        if base_value:
            row.has_explicit_base = True
        if tall_value:
            row.has_explicit_tall = True
        if island_value:
            row.has_explicit_island = True
        if bar_back_value:
            row.has_explicit_bar_back = True
        row.door_colours_overheads = _merge_clean_group_text(row.door_colours_overheads, overhead_value, cleaner=_clean_door_colour_value)
        row.door_colours_base = _merge_clean_group_text(row.door_colours_base, base_value, cleaner=_clean_door_colour_value)
        row.door_colours_tall = _merge_clean_group_text(row.door_colours_tall, tall_value, cleaner=_clean_door_colour_value)
        row.door_colours_island = _merge_clean_group_text(row.door_colours_island, island_value, cleaner=_clean_door_colour_value)
        row.door_colours_bar_back = _merge_clean_group_text(row.door_colours_bar_back, bar_back_value, cleaner=_clean_door_colour_value)
        if not _has_explicit_door_group_markers(row):
            _apply_door_colour_groups(row, row.door_panel_colours)
        row.toe_kick = _merge_lists(row.toe_kick, _collect_field(lines, ["Toe Kick", "Kickboard", "Island Bench Kickboard"]))
        row.bulkheads = _merge_lists(row.bulkheads, _collect_field(lines, ["Bulkheads", "Bulkhead"]))
        row.handles = _merge_lists(row.handles, _clean_handle_entries(_collect_field(lines, ["Handles", "Handle", "Base Cabinet Handles", "Overhead Handles"])))
        row.floating_shelf = _merge_text(row.floating_shelf, _first_value(_collect_field(lines, ["Floating Shelves", "Floating Shelf"])))
        if _collect_field(lines, ["LED Strip Lighting", "LED Lighting", "LED"]):
            row.led = "Yes"
        row.accessories = _merge_lists(row.accessories, _collect_field(lines, ["Accessories", "Accessory"]))
        row.other_items = _merge_other_items(
            row.other_items,
            [
                {"label": "RAIL", "value": _first_value(_collect_field(lines, ["Rail"]))},
                {"label": "JEWELLERY INSERT", "value": _first_value(_collect_field(lines, ["Jewellery Insert"]))},
            ],
        )
        row.drawers_soft_close = merge_soft_close_values(row.drawers_soft_close, _extract_soft_close(lines, "drawer"))
        row.hinges_soft_close = merge_soft_close_values(row.hinges_soft_close, _extract_soft_close(lines, "hinge"))
        row.splashback = row.splashback or _first_value(_collect_field(lines, ["Splashback"]))
        row.flooring = row.flooring or _first_value(_collect_field(lines, ["Flooring"]))
    row.sink_info = _merge_text(row.sink_info, _first_value(_collect_field(lines, ["Sink Type/Model", "Sink Type", "Drop in Tub", "Sink"])))
    basin_value = _first_value(_collect_field(lines, ["Vanity Inset Basin"])) or _first_value(_collect_field(lines, ["Basin"]))
    row.basin_info = _merge_text(row.basin_info, basin_value)
    row.tap_info = _merge_text(row.tap_info, _first_value(_collect_field(lines, ["Vanity Tap Style", "Tap Type", "Tap Style", "Sink Mixer", "Pull-Out Mixer", "Mixer"])))
    if authoritative_room_section or not row.source_file:
        row.source_file = file_name
    if authoritative_room_section or not row.page_refs:
        row.page_refs = _guess_page_refs(chunk, pages)
    if authoritative_room_section or not row.evidence_snippet:
        row.evidence_snippet = chunk[:300]
    row.confidence = max(row.confidence, 0.55)


def _find_room_sections(text: str) -> list[tuple[str, str]]:
    matches: list[tuple[int, str, str]] = []
    for room_key, aliases in ROOM_ALIASES.items():
        for alias in aliases:
            pattern = re.compile(rf"(?:(?<=\n)|^)\s*{re.escape(alias)}\b", re.IGNORECASE)
            for match in pattern.finditer(text):
                line_end = text.find("\n", match.start())
                if line_end == -1:
                    line_end = len(text)
                line_text = normalize_space(text[match.start() : line_end])
                if _looks_like_field_label(line_text):
                    continue
                matches.append((match.start(), normalize_space(match.group(0)), room_key))
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
    chunk = _inject_schedule_heading_breaks(chunk)
    for label in sorted(FIELD_LABELS, key=len, reverse=True):
        chunk = re.sub(rf"(?i)(?<=\w){re.escape(label)}\b", f" {label}", chunk)
        chunk = re.sub(rf"(?i)\b{re.escape(label)}\b", f"\n{label}", chunk)
    lines = [normalize_space(line) for line in chunk.split("\n") if normalize_space(line)]
    return _merge_broken_schedule_lines(lines)


def _merge_broken_schedule_lines(lines: list[str]) -> list[str]:
    merged: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if index + 1 < len(lines):
            pair = f"{line} {lines[index + 1]}"
            if re.fullmatch(r"(?i)island benchtop.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)vanity inset basin.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)vanity tap style.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)sink mixer.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)basin mixer.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)pull-out mixer.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)overhead handles.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)base cabinet handles.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)base cupboards & drawers.*", pair):
                merged.append(pair)
                index += 2
                continue
            if re.fullmatch(r"(?i)island bench kickboard.*", pair):
                merged.append(pair)
                index += 2
                continue
            if index + 2 < len(lines):
                triple = f"{line} {lines[index + 1]} {lines[index + 2]}"
                if re.fullmatch(r"(?i)island bench base cupboards & drawers.*", triple):
                    merged.append(triple)
                    index += 3
                    continue
                if re.fullmatch(r"(?i)\(to all lower doors? & drawers?\).*", triple):
                    merged.append(triple)
                    index += 3
                    continue
        merged.append(line)
        index += 1
    return merged


def _collect_field(lines: list[str], prefixes: list[str]) -> list[str]:
    values: list[str] = []
    for index, line in enumerate(lines):
        for prefix in prefixes:
            match = re.match(rf"^{re.escape(prefix)}(?:\s*\d+)?\b", line, re.IGNORECASE)
            if match:
                parts: list[str] = []
                initial_value = line[match.end() :].strip(" :-")
                if initial_value:
                    parts.append(initial_value)
                cursor = index + 1
                while cursor < len(lines):
                    next_line = normalize_space(lines[cursor])
                    if _should_stop_field_continuation(prefix, next_line):
                        break
                    if _skip_continuation_line(next_line):
                        cursor += 1
                        continue
                    parts.append(next_line)
                    cursor += 1
                value = normalize_space(" ".join(parts)).strip(" :-")
                if value and value not in values:
                    values.append(value)
                break
    return values


def _extract_soft_close(lines: list[str], keyword: str) -> str:
    field_hints = SOFT_CLOSE_FIELD_HINTS.get(keyword, (keyword,))
    for index, line in enumerate(lines):
        candidates = [line]
        if index + 1 < len(lines) and not _looks_like_field_label(lines[index + 1]):
            candidates.append(f"{line} {lines[index + 1]}")
        for candidate in candidates:
            lowered = candidate.lower()
            if keyword == "hinge" and "drawer" in lowered:
                continue
            if not any(token in lowered for token in field_hints):
                continue
            line_value = normalize_soft_close_value(candidate, keyword)
            if line_value:
                return line_value
    return ""


def _extract_appliances(text: str, file_name: str, pages: list[dict[str, object]]) -> list[ApplianceRow]:
    rows = _extract_labeled_appliances(text, file_name, pages)
    rows.extend(_extract_loose_appliances(text, file_name, pages))
    return _dedupe_appliances(rows)


def _extract_labeled_appliances(text: str, file_name: str, pages: list[dict[str, object]]) -> list[ApplianceRow]:
    matches = _collect_appliance_label_matches(text)
    rows: list[ApplianceRow] = []
    for index, match in enumerate(matches):
        next_start = matches[index + 1]["start"] if index + 1 < len(matches) else len(text)
        segment = normalize_space(text[match["start"]:next_start])
        details = _limit_appliance_details_to_local_context(text[match["end"]:next_start])
        row = _build_appliance_row(
            appliance_type=str(match["appliance_type"]),
            details=details,
            evidence=segment or details,
            file_name=file_name,
            pages=pages,
            confidence=0.72,
        )
        if row:
            rows.append(row)
    return rows


def _collect_appliance_label_matches(text: str) -> list[dict[str, object]]:
    raw_matches: list[dict[str, object]] = []
    for appliance_type, patterns in APPLIANCE_LABEL_SPECS:
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                raw_matches.append(
                    {
                        "start": match.start(),
                        "end": match.end(),
                        "length": match.end() - match.start(),
                        "appliance_type": appliance_type,
                    }
                )
    raw_matches.sort(key=lambda item: (int(item["start"]), -int(item["length"])))
    deduped: list[dict[str, object]] = []
    current_end = -1
    for match in raw_matches:
        start = int(match["start"])
        end = int(match["end"])
        if start < current_end:
            continue
        deduped.append(match)
        current_end = end
    return deduped


def _extract_loose_appliances(text: str, file_name: str, pages: list[dict[str, object]]) -> list[ApplianceRow]:
    rows: list[ApplianceRow] = []
    for raw_line in text.split("\n"):
        line = normalize_space(raw_line)
        if not line:
            continue
        if _looks_like_strict_appliance_label(line):
            continue
        lowered = line.lower()
        if any(skip in lowered for skip in LOOSE_SKIP_PHRASES):
            continue
        appliance_type = _match_loose_appliance_type(lowered)
        if not appliance_type:
            continue
        make = _guess_make(line)
        model_no = _guess_model(line)
        if not (make or model_no):
            continue
        if not (make or _has_parenthesized_model(line)):
            continue
        row = _build_appliance_row(
            appliance_type=appliance_type,
            details=line,
            evidence=line,
            file_name=file_name,
            pages=pages,
            confidence=0.5,
        )
        if row:
            rows.append(row)
    return rows


def _looks_like_strict_appliance_label(line: str) -> bool:
    for _appliance_type, patterns in APPLIANCE_LABEL_SPECS:
        for pattern in patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return True
    return False


def _match_loose_appliance_type(lowered_line: str) -> str:
    for token, appliance_type in LOOSE_APPLIANCE_TYPE_MAP:
        if token in lowered_line:
            return appliance_type
    return ""


def _has_parenthesized_model(text: str) -> bool:
    return any(_valid_model_candidate(candidate.upper(), allow_numeric=True) for candidate in re.findall(r"\(([A-Za-z0-9/-]{3,})\)", text))


def _build_appliance_row(
    appliance_type: str,
    details: str,
    evidence: str,
    file_name: str,
    pages: list[dict[str, object]],
    confidence: float,
) -> ApplianceRow | None:
    clean_details = normalize_space(details)
    if not clean_details:
        return None
    if clean_details.upper().startswith("N/A") and not _guess_model(clean_details):
        return None
    make = _guess_make(clean_details)
    model_no = _guess_model(clean_details)
    if not any((make, model_no)):
        return None
    return ApplianceRow(
        appliance_type=appliance_type,
        make=make,
        model_no=model_no,
        product_url="",
        spec_url="",
        manual_url="",
        website_url="",
        overall_size="",
        source_file=file_name,
        page_refs=_guess_page_refs(evidence or clean_details, pages),
        evidence_snippet=(evidence or clean_details)[:300],
        confidence=confidence,
    )


def _limit_appliance_details_to_local_context(details: str) -> str:
    lines = [normalize_space(line) for line in str(details or "").splitlines() if normalize_space(line)]
    if not lines:
        return normalize_space(details)
    kept = [lines[0]]
    for line in lines[1:]:
        if _looks_like_strict_appliance_label(line) or _is_room_heading_line(line) or _is_schedule_room_heading(line) or _looks_like_field_label(line):
            break
        combined = normalize_space(" ".join(kept))
        if _guess_model(combined) or _guess_make(combined):
            break
        if len(kept) >= 2:
            break
        kept.append(line)
    return normalize_space(" ".join(kept))


def _guess_make(text: str) -> str:
    lowered = text.lower()
    for brand in sorted(KNOWN_BRANDS, key=len, reverse=True):
        if brand in lowered:
            return normalize_brand_label(brand)
    return ""


def _guess_model(text: str) -> str:
    quantity_match = re.search(r"\b(\d+)\s*[xX]\s*([A-Z0-9/-]*[A-Z][A-Z0-9/-]*\d[A-Z0-9/-]*)\b", text)
    if quantity_match:
        candidate = quantity_match.group(2).upper()
        if _valid_model_candidate(candidate):
            return f"{quantity_match.group(1)} x {candidate}"

    for candidate in re.findall(r"\(([A-Za-z0-9/-]{3,})\)", text):
        normalized = candidate.upper()
        if _valid_model_candidate(normalized, allow_numeric=True):
            return normalized

    for match in re.finditer(r"\b([A-Z0-9/-]*[A-Z][A-Z0-9/-]*\d[A-Z0-9/-]*)\b", text):
        candidate = match.group(1).upper()
        if _valid_model_candidate(candidate):
            return candidate
    return ""


def _valid_model_candidate(candidate: str, allow_numeric: bool = False) -> bool:
    token = candidate.strip().strip("()").upper()
    if len(token) < 4:
        return False
    if token in MODEL_STOPWORDS:
        return False
    if re.fullmatch(r"\d{2,4}(MM|CM|L)", token):
        return False
    if re.fullmatch(r"\d{2,4}", token):
        return allow_numeric
    if not any(char.isdigit() for char in token):
        return False
    if allow_numeric and token.isdigit():
        return True
    return any(char.isalpha() for char in token)


def _guess_size(text: str) -> str:
    match = re.search(r"\b\d{2,4}\s?(?:mm|cm)\b", text, re.IGNORECASE)
    return match.group(0) if match else ""


def _brand_url(text: str) -> str:
    lowered = text.lower()
    for brand, url in sorted(KNOWN_BRANDS.items(), key=lambda item: len(item[0]), reverse=True):
        if brand in lowered:
            return url
    return ""


SOFT_CLOSE_NEGATIVE_HINTS = (
    "not soft close",
    "not soft closed",
    "not soft-close",
    "standard runner",
    "standard runners",
    "standard hinge",
    "standard hinges",
    "standard construction",
    "builder standard",
    "no soft close",
)

SOFT_CLOSE_POSITIVE_HINTS = (
    "soft close",
    "soft closed",
    "soft-close",
    "softclose",
    "blumotion",
)

SOFT_CLOSE_FIELD_HINTS = {
    "drawer": ("drawer", "drawers", "runner", "runners"),
    "hinge": ("hinge", "hinges", "door", "doors"),
}


def normalize_soft_close_value(value: Any, keyword: str = "") -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    lowered = text.lower()
    if keyword:
        field_hints = SOFT_CLOSE_FIELD_HINTS.get(keyword, (keyword,))
        if keyword == "hinge" and "drawer" in lowered:
            return ""
        if not any(token in lowered for token in field_hints):
            return ""
    if any(token in lowered for token in SOFT_CLOSE_NEGATIVE_HINTS):
        return "Not Soft Close"
    if any(token in lowered for token in SOFT_CLOSE_POSITIVE_HINTS):
        return "Soft Close"
    if lowered in {"yes", "y", "true"}:
        return "Soft Close"
    if lowered in {"no", "n", "false"}:
        return "Not Soft Close"
    return ""


def merge_soft_close_values(current: Any, candidate: Any) -> str:
    current_value = normalize_soft_close_value(current)
    candidate_value = normalize_soft_close_value(candidate)
    if current_value == "Not Soft Close" or candidate_value == "Not Soft Close":
        return "Not Soft Close"
    if current_value == "Soft Close" or candidate_value == "Soft Close":
        return "Soft Close"
    return ""


def _guess_page_refs(snippet: str, pages: list[dict[str, object]]) -> str:
    probe = normalize_space(snippet)[:120]
    if not probe:
        return ""
    for page in pages:
        page_text = normalize_space(str(page["text"]))
        if probe and probe in page_text:
            return str(page["page_no"])
    probe_head = probe[:80]
    for page in pages:
        page_text = normalize_space(str(page["text"]))
        if probe_head and probe_head in page_text:
            return str(page["page_no"])
    return ""


def _extract_global_value(text: str, label: str) -> str:
    match = re.search(rf"(?i)\b{re.escape(label)}\b[:\s-]*(.{{0,160}})", text)
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


def enrich_snapshot_rooms(snapshot: dict[str, Any], documents: list[dict[str, object]], rule_flags: Any = None) -> dict[str, Any]:
    extracted_site_address = _extract_site_address_from_documents(documents)
    if extracted_site_address:
        snapshot["site_address"] = extracted_site_address
    rooms = [row for row in snapshot.get("rooms", []) if isinstance(row, dict)]
    analysis = snapshot.get("analysis") or {}
    room_master_file = str(analysis.get("room_master_file", "") or "")
    imperial_builder = _is_imperial_builder(str(snapshot.get("builder_name", "")))
    overlays = (
        _collect_imperial_room_overlays(documents)
        if imperial_builder
        else _collect_room_overlays(documents, room_master_file=room_master_file)
    )
    for row in rooms:
        overlay = _match_room_overlay(row, overlays)
        if imperial_builder:
            row["bench_tops"] = _rebuild_benchtop_entries(row)
            row["door_panel_colours"] = _rebuild_door_panel_colours(row)
            row["handles"] = _clean_handle_entries(_coerce_string_list(row.get("handles", [])))
            row["floating_shelf"] = _string_value(row.get("floating_shelf", ""))
            row["led"] = "Yes" if row.get("led") else ""
            row["accessories"] = _clean_accessory_entries(_coerce_string_list(row.get("accessories", [])))
            row["other_items"] = _merge_other_items([], row.get("other_items", []))
            row["sink_info"] = _clean_fixture_text(overlay.get("sink_info", "") or _string_value(row.get("sink_info", "")))
            row["basin_info"] = _clean_fixture_text(overlay.get("basin_info", "") or _string_value(row.get("basin_info", "")))
            row["tap_info"] = _clean_fixture_text(overlay.get("tap_info", "") or _string_value(row.get("tap_info", "")))
            row["drawers_soft_close"] = merge_soft_close_values(row.get("drawers_soft_close", ""), "")
            row["hinges_soft_close"] = merge_soft_close_values(row.get("hinges_soft_close", ""), "")
            continue
        for key in ("has_explicit_overheads", "has_explicit_base", "has_explicit_tall", "has_explicit_island", "has_explicit_bar_back"):
            row[key] = bool(row.get(key, False) or overlay.get(key, False))
        benchtop_groups = _split_benchtop_groups(_coerce_string_list(row.get("bench_tops", [])))
        row["bench_tops_wall_run"] = overlay.get("bench_tops_wall_run", "") or _merge_text(_string_value(row.get("bench_tops_wall_run", "")), benchtop_groups["bench_tops_wall_run"])
        row["bench_tops_island"] = overlay.get("bench_tops_island", "") or _merge_text(_string_value(row.get("bench_tops_island", "")), benchtop_groups["bench_tops_island"])
        row["bench_tops_other"] = overlay.get("bench_tops_other", "") or _merge_text(_string_value(row.get("bench_tops_other", "")), benchtop_groups["bench_tops_other"])
        row["bench_tops"] = _rebuild_benchtop_entries(row)
        door_groups = (
            _blank_door_group_values()
            if _has_explicit_door_group_markers(row)
            else _split_door_colour_groups(_coerce_string_list(row.get("door_panel_colours", [])))
        )
        row["door_colours_overheads"] = overlay.get("door_colours_overheads", "") or _merge_clean_group_text(row.get("door_colours_overheads", ""), door_groups["door_colours_overheads"], cleaner=_clean_door_colour_value)
        row["door_colours_base"] = overlay.get("door_colours_base", "") or _merge_clean_group_text(row.get("door_colours_base", ""), door_groups["door_colours_base"], cleaner=_clean_door_colour_value)
        row["door_colours_tall"] = overlay.get("door_colours_tall", "") or _merge_clean_group_text(row.get("door_colours_tall", ""), door_groups["door_colours_tall"], cleaner=_clean_door_colour_value)
        row["door_colours_island"] = overlay.get("door_colours_island", "") or _merge_clean_group_text(row.get("door_colours_island", ""), door_groups["door_colours_island"], cleaner=_clean_door_colour_value)
        row["door_colours_bar_back"] = overlay.get("door_colours_bar_back", "") or _merge_clean_group_text(row.get("door_colours_bar_back", ""), door_groups["door_colours_bar_back"], cleaner=_clean_door_colour_value)
        row.update(
            _prune_door_group_overlap(
                {
                    "door_colours_overheads": row["door_colours_overheads"],
                    "door_colours_base": row["door_colours_base"],
                    "door_colours_tall": row["door_colours_tall"],
                    "door_colours_island": row["door_colours_island"],
                    "door_colours_bar_back": row["door_colours_bar_back"],
                }
            )
        )
        row["door_panel_colours"] = _rebuild_door_panel_colours(row)
        row["handles"] = _clean_handle_entries(_coerce_string_list(row.get("handles", [])))
        row["floating_shelf"] = _merge_text(_string_value(row.get("floating_shelf", "")), overlay.get("floating_shelf", ""))
        row["led"] = "Yes" if (overlay.get("led") or row.get("led")) else ""
        row["accessories"] = _merge_lists(_coerce_string_list(row.get("accessories", [])), _coerce_string_list(overlay.get("accessories", [])))
        row["other_items"] = _merge_other_items(row.get("other_items", []), overlay.get("other_items", []))
        row["sink_info"] = _merge_text(_string_value(row.get("sink_info", "")), overlay.get("sink_info", ""))
        row["basin_info"] = _merge_text(_string_value(row.get("basin_info", "")), overlay.get("basin_info", ""))
        row["tap_info"] = _merge_text(_string_value(row.get("tap_info", "")), overlay.get("tap_info", ""))
        row["drawers_soft_close"] = merge_soft_close_values(row.get("drawers_soft_close", ""), "")
        row["hinges_soft_close"] = merge_soft_close_values(row.get("hinges_soft_close", ""), "")
    snapshot["rooms"] = rooms
    snapshot["appliances"] = [row for row in snapshot.get("appliances", []) if isinstance(row, dict) and not _is_room_fixture_appliance(row)]
    return apply_snapshot_cleaning_rules(snapshot, rule_flags=rule_flags)


def apply_snapshot_cleaning_rules(snapshot: dict[str, Any], rule_flags: Any = None) -> dict[str, Any]:
    flags = cleaning_rules.normalize_rule_flags(rule_flags)
    cleaned = dict(snapshot)
    cleaned["rooms"] = [_apply_room_cleaning_rules(dict(row), flags) for row in snapshot.get("rooms", []) if isinstance(row, dict)]
    cleaned["special_sections"] = [
        _apply_special_section_cleaning_rules(dict(row), flags)
        for row in snapshot.get("special_sections", [])
        if isinstance(row, dict)
    ]
    cleaned["appliances"] = [
        _apply_appliance_cleaning_rules(dict(row), flags)
        for row in snapshot.get("appliances", [])
        if isinstance(row, dict) and not _is_room_fixture_appliance(row)
    ]
    others = snapshot.get("others") or {}
    if isinstance(others, dict):
        cleaned["others"] = {
            _display_rule_text(key, flags): _display_rule_text(value, flags)
            for key, value in others.items()
        }
    return cleaned


def _apply_room_cleaning_rules(row: dict[str, Any], rule_flags: dict[str, bool]) -> dict[str, Any]:
    row["room_key"] = normalize_space(str(row.get("room_key", "")))
    row["has_explicit_overheads"] = bool(row.get("has_explicit_overheads", False))
    row["has_explicit_base"] = bool(row.get("has_explicit_base", False))
    row["has_explicit_tall"] = bool(row.get("has_explicit_tall", False))
    row["has_explicit_island"] = bool(row.get("has_explicit_island", False))
    row["has_explicit_bar_back"] = bool(row.get("has_explicit_bar_back", False))
    row["original_room_label"] = _display_rule_text(row.get("original_room_label", ""), rule_flags)
    row["bench_tops"] = _normalize_text_list(row.get("bench_tops", []), rule_flags)
    row["toe_kick"] = _normalize_text_list(row.get("toe_kick", []), rule_flags)
    row["bulkheads"] = _normalize_text_list(row.get("bulkheads", []), rule_flags)
    row["handles"] = _normalize_text_list(_clean_handle_entries(_coerce_string_list(row.get("handles", []))), rule_flags)
    row["floating_shelf"] = _display_rule_text(row.get("floating_shelf", ""), rule_flags)
    row["led"] = "Yes" if normalize_space(str(row.get("led", ""))) else ""
    row["accessories"] = _normalize_text_list(row.get("accessories", []), rule_flags)
    row["other_items"] = [
        {
            "label": _display_rule_text(item.get("label", ""), rule_flags),
            "value": _display_rule_text(item.get("value", ""), rule_flags),
        }
        for item in _merge_other_items([], row.get("other_items", []))
        if _display_rule_text(item.get("label", ""), rule_flags) and _display_rule_text(item.get("value", ""), rule_flags)
    ]
    row["sink_info"] = _clean_fixture_text(_display_rule_text(row.get("sink_info", ""), rule_flags))
    row["basin_info"] = _clean_fixture_text(_display_rule_text(row.get("basin_info", ""), rule_flags))
    row["tap_info"] = _clean_fixture_text(_display_rule_text(row.get("tap_info", ""), rule_flags))
    row["splashback"] = _display_rule_text(row.get("splashback", ""), rule_flags)
    row["flooring"] = _display_rule_text(row.get("flooring", ""), rule_flags)
    row["drawers_soft_close"] = normalize_soft_close_value(row.get("drawers_soft_close", ""), keyword="drawer") or normalize_soft_close_value(row.get("drawers_soft_close", ""))
    row["hinges_soft_close"] = normalize_soft_close_value(row.get("hinges_soft_close", ""), keyword="hinge") or normalize_soft_close_value(row.get("hinges_soft_close", ""))

    row["door_panel_colours"] = _normalize_door_colour_entries(row.get("door_panel_colours", []), rule_flags)
    grouped_doors = (
        _blank_door_group_values()
        if _has_explicit_door_group_markers(row)
        else _split_door_colour_groups(row["door_panel_colours"])
    )
    for key in ("door_colours_overheads", "door_colours_base", "door_colours_tall", "door_colours_island", "door_colours_bar_back"):
        existing = _display_rule_text(row.get(key, ""), rule_flags)
        merged = _merge_clean_group_text(existing, grouped_doors.get(key, ""), cleaner=_clean_door_colour_value)
        row[key] = _display_rule_text(merged, rule_flags)
    if cleaning_rules.rule_enabled(rule_flags, "door_colour_dedupe_cleanup"):
        row.update(_prune_door_group_overlap({key: row.get(key, "") for key in ("door_colours_overheads", "door_colours_base", "door_colours_tall", "door_colours_island", "door_colours_bar_back")}))
    normalized_room_key = normalize_room_key(str(row.get("room_key", "")))
    if normalized_room_key != "kitchen":
        if row["door_colours_overheads"] and not row["has_explicit_overheads"]:
            row["door_colours_base"] = _merge_clean_group_text(row.get("door_colours_base", ""), row["door_colours_overheads"], cleaner=_clean_door_colour_value)
            row["door_colours_overheads"] = ""
        row["door_colours_island"] = ""
        row["door_colours_bar_back"] = ""
        row["has_explicit_island"] = False
        row["has_explicit_bar_back"] = False
    row["door_panel_colours"] = _rebuild_door_panel_colours(row)

    benchtop_groups = _split_benchtop_groups(row["bench_tops"])
    row["bench_tops_wall_run"] = _display_rule_text(_merge_text(row.get("bench_tops_wall_run", ""), benchtop_groups["bench_tops_wall_run"]), rule_flags)
    row["bench_tops_island"] = _display_rule_text(_merge_text(row.get("bench_tops_island", ""), benchtop_groups["bench_tops_island"]), rule_flags)
    row["bench_tops_other"] = _display_rule_text(_merge_text(row.get("bench_tops_other", ""), benchtop_groups["bench_tops_other"]), rule_flags)
    if row["bench_tops_other"] and row["bench_tops_other"] in {row["bench_tops_wall_run"], row["bench_tops_island"]}:
        row["bench_tops_other"] = ""
    if cleaning_rules.rule_enabled(rule_flags, "kitchen_only_split_benchtops") and normalized_room_key != "kitchen":
        folded = " | ".join(part for part in [row.get("bench_tops_other", ""), row.get("bench_tops_wall_run", ""), row.get("bench_tops_island", "")] if part)
        row["bench_tops_other"] = _merge_text(row.get("bench_tops_other", ""), folded)
        row["bench_tops_wall_run"] = ""
        row["bench_tops_island"] = ""
    row["bench_tops"] = _normalize_text_list(_rebuild_benchtop_entries(row), rule_flags)

    return row


def _apply_special_section_cleaning_rules(row: dict[str, Any], rule_flags: dict[str, bool]) -> dict[str, Any]:
    cleaned = dict(row)
    cleaned["section_key"] = normalize_space(str(cleaned.get("section_key", "")))
    cleaned["original_section_label"] = _display_rule_text(cleaned.get("original_section_label", ""), rule_flags)
    fields = cleaned.get("fields") or {}
    if isinstance(fields, dict):
        cleaned["fields"] = {
            _display_rule_text(key, rule_flags): _display_rule_text(value, rule_flags)
            for key, value in fields.items()
            if _display_rule_text(value, rule_flags)
        }
    else:
        cleaned["fields"] = {}
    for key in ("source_file", "page_refs", "evidence_snippet"):
        cleaned[key] = _display_rule_text(cleaned.get(key, ""), rule_flags)
    try:
        cleaned["confidence"] = float(cleaned.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        cleaned["confidence"] = 0.0
    return cleaned


def _apply_appliance_cleaning_rules(row: dict[str, Any], rule_flags: dict[str, bool]) -> dict[str, Any]:
    row["appliance_type"] = _display_rule_text(row.get("appliance_type", ""), rule_flags)
    row["make"] = normalize_brand_label(str(row.get("make", ""))) if cleaning_rules.rule_enabled(rule_flags, "normalize_brand_casing") else normalize_space(str(row.get("make", "")))
    row["model_no"] = normalize_space(str(row.get("model_no", "")))
    for key in ("product_url", "spec_url", "manual_url", "website_url", "overall_size", "source_file", "page_refs", "evidence_snippet"):
        row[key] = _display_rule_text(row.get(key, ""), rule_flags)
    return row


def _normalize_text_list(value: Any, rule_flags: dict[str, bool]) -> list[str]:
    normalized: list[str] = []
    for item in _coerce_string_list(value):
        text = _display_rule_text(item, rule_flags)
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _normalize_door_colour_entries(value: Any, rule_flags: dict[str, bool]) -> list[str]:
    entries: list[str] = []
    for item in _coerce_string_list(value):
        normalized = _display_rule_text(item, rule_flags)
        cleaned = _clean_door_colour_value(normalized) if cleaning_rules.rule_enabled(rule_flags, "cabinet_only_colour_filter") else normalized
        if cleaned:
            entries.append(cleaned)
    if cleaning_rules.rule_enabled(rule_flags, "door_colour_dedupe_cleanup"):
        return _dedupe_prefer_specific(entries, cleaner=_clean_door_colour_value)
    return _unique(entries)


def _display_rule_text(value: Any, rule_flags: dict[str, bool]) -> str:
    return normalize_brand_casing_text(value, rule_flags)


def _imperial_preprocess_non_joinery_lines(text: str) -> list[str]:
    raw_lines = [normalize_space(line) for line in text.replace("\r", "\n").split("\n") if normalize_space(line)]
    lines: list[str] = []
    heading_patterns = (
        r"(?i)(?=(?:SINKWARE|TAPWARE)\s*\()",
        r"(?i)(?=(?:SINKWARE|TAPWARE)\s+[A-Z][A-Z0-9 &/'\-]+\))",
        r"(?i)(?=(?:BASIN|TUB|DROP\s+IN\s+TUB)\s*\()",
        r"(?i)(?=Taphole location:)",
    )
    for raw_line in raw_lines:
        split_points = {0}
        for pattern in heading_patterns:
            for match in re.finditer(pattern, raw_line):
                if match.start() > 0:
                    split_points.add(match.start())
        ordered = sorted(split_points)
        for index, start in enumerate(ordered):
            end = ordered[index + 1] if index + 1 < len(ordered) else len(raw_line)
            segment = normalize_space(raw_line[start:end])
            if not segment:
                continue
            segment = re.sub(r"(?i)^(SINKWARE|TAPWARE)\s+([^(][^)]+)\)$", r"\1 (\2)", segment)
            lines.append(segment)
    return lines


def _imperial_extract_non_joinery_blocks(text: str, kind: str) -> list[tuple[str, str]]:
    lines = _imperial_preprocess_non_joinery_lines(text)
    blocks: list[tuple[str, str]] = []
    pending: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if _is_imperial_page_noise_line(line) or line.upper() in {"SINKWARE & TAPWARE", "AREA / ITEM SPECS / DESCRIPTION IMAGE SUPPLIER NOTES", "AREA / ITEM SPECS / DESCRIPTION IMAGE SUPPLIER"}:
            index += 1
            continue
        any_heading_match = re.match(r"(?i)^(?P<label>SINKWARE|TAPWARE)\s*\((?P<room>[^)]+)\)\s*(?P<tail>.*)$", line)
        if any_heading_match and any_heading_match.group("label").upper() != ("SINKWARE" if kind == "sinkware" else "TAPWARE"):
            if kind == "tapware":
                pending = []
            index += 1
            while index < len(lines):
                next_line = lines[index]
                if re.match(r"(?i)^(?:SINKWARE|TAPWARE)\s*\(", next_line):
                    break
                if next_line.upper().startswith("SINK ACCESSORIES") or next_line.upper().startswith("NOTES"):
                    break
                index += 1
            continue
        heading_match = re.match(rf"(?i)^{'SINKWARE' if kind == 'sinkware' else 'TAPWARE'}\s*\((?P<room>[^)]+)\)\s*(?P<tail>.*)$", line)
        if heading_match:
            heading_cluster: list[tuple[str, str]] = [(normalize_space(heading_match.group("room")), normalize_space(heading_match.group("tail")))]
            body_prefix = pending[:]
            pending = []
            index += 1
            while index < len(lines):
                same_kind_heading = re.match(rf"(?i)^{'SINKWARE' if kind == 'sinkware' else 'TAPWARE'}\s*\((?P<room>[^)]+)\)\s*(?P<tail>.*)$", lines[index])
                if not same_kind_heading:
                    break
                heading_cluster.append((normalize_space(same_kind_heading.group("room")), normalize_space(same_kind_heading.group("tail"))))
                index += 1
            body_parts: list[str] = []
            while index < len(lines):
                next_line = lines[index]
                if _is_imperial_page_noise_line(next_line):
                    index += 1
                    continue
                if re.match(r"(?i)^(?:SINKWARE|TAPWARE)\s*\(", next_line):
                    break
                if re.match(r"(?i)^(?:BASIN|TUB|DROP\s+IN\s+TUB)\s*\(", next_line):
                    break
                if next_line.upper().startswith("SINK ACCESSORIES") or next_line.upper().startswith("NOTES"):
                    break
                body_parts.append(next_line)
                index += 1
            if len(heading_cluster) == 1:
                room_label, tail = heading_cluster[0]
                single_body = body_prefix[:]
                if tail:
                    single_body.append(tail)
                single_body.extend(body_parts)
                cleaned = _imperial_clean_non_joinery_body("\n".join(single_body), kind)
                if room_label and cleaned:
                    blocks.append((room_label, cleaned))
                continue

            primary_lines = [line for line in body_parts if not _imperial_is_non_joinery_note_line(line, kind)]
            note_lines = [line for line in body_parts if line not in primary_lines]
            assigned: list[list[str]] = [[] for _ in heading_cluster]
            filtered_prefix = [line for line in body_prefix if _imperial_is_non_joinery_note_line(line, kind)]
            if filtered_prefix:
                assigned[0].extend(filtered_prefix)
            for cluster_index, (_, tail) in enumerate(heading_cluster):
                if tail:
                    assigned[cluster_index].append(tail)
            for cluster_index in range(len(heading_cluster)):
                if cluster_index < len(primary_lines):
                    assigned[cluster_index].append(primary_lines[cluster_index])
            if len(primary_lines) > len(heading_cluster):
                assigned[-1].extend(primary_lines[len(heading_cluster):])
            if note_lines:
                if kind == "sinkware" and len(heading_cluster) > 1 and all(_imperial_is_non_joinery_note_line(line, kind) for line in note_lines):
                    for cluster_parts in assigned:
                        cluster_parts.extend(note_lines)
                else:
                    assigned[-1].extend(note_lines)
            for (room_label, _), parts in zip(heading_cluster, assigned):
                cleaned = _imperial_clean_non_joinery_body("\n".join(parts), kind)
                if room_label and cleaned:
                    blocks.append((room_label, cleaned))
            continue
        if re.match(r"(?i)^(?:BASIN|TUB|DROP\s+IN\s+TUB)\s*\(", line):
            pending = []
            index += 1
            continue
        if _imperial_is_relevant_preheading_non_joinery_line(line, kind):
            pending.append(line)
            pending = pending[-5:]
        elif pending and not re.match(r"(?i)^(?:SINKWARE|TAPWARE|SINK ACCESSORIES|NOTES)\b", line):
            pending.append(line)
            pending = pending[-5:]
        else:
            pending = []
        index += 1
    return blocks


def _imperial_is_relevant_preheading_non_joinery_line(line: str, kind: str) -> bool:
    text = normalize_space(line)
    if not text or _is_imperial_page_noise_line(text):
        return False
    upper = text.upper()
    if kind == "sinkware":
        return bool(
            re.match(r"(?i)^taphole location\s*:", text)
            or "UNDERMOUT" in upper
            or re.search(r"(?i)\b(?:corner of tub|centre of sink|sink pre-?punched hole|sink mounting|topmount|matrix sink|double bowl|double drain|undermount)\b", text)
        )
    return bool(
        re.search(r"(?i)\b(?:tap|mixer|gooseneck|spray|pull out|pull-out|filter)\b", text)
        or (
            bool(_guess_model(text))
            and not re.search(r"(?i)\b(?:sink|bowl|drain|undermount|stainless|basin)\b", text)
        )
    )


def _imperial_is_non_joinery_note_line(line: str, kind: str) -> bool:
    text = normalize_space(line)
    if not text:
        return False
    if re.match(r"(?i)^taphole location\s*:", text):
        if re.search(r"(?i)\([^)]*\)|\btub\b", text):
            return False
        return True
    if kind == "sinkware" and re.search(r"(?i)\b(?:centre|center|ctr)\s+of\s+sink|sink pre-?punched hole\b", text):
        return True
    return False


def _imperial_clean_non_joinery_body(body: str, kind: str) -> str:
    raw_lines = [normalize_space(line) for line in body.replace("\r", "\n").split("\n") if normalize_space(line)]
    lines: list[str] = []
    for line in raw_lines:
        if lines and not _is_imperial_page_noise_line(line) and not re.match(r"(?i)^(?:SINKWARE|TAPWARE|SINK ACCESSORIES|NOTES)\b", line):
            previous = lines[-1]
            if previous.endswith(("TO", "OF", "IN", "WITH", "UNDERMOUTNED", "UNDERMOUT", "INSTAL", "INSTALLED")) or line[:1].islower():
                lines[-1] = normalize_space(f"{previous} {line}")
                continue
        lines.append(line)
    notes: list[str] = []
    values: list[str] = []
    pending_taphole = False
    for line in lines:
        upper = line.upper()
        if _is_imperial_page_noise_line(line):
            continue
        if _looks_like_person_name_line(line):
            continue
        if upper.startswith("SINKWARE") or upper.startswith("TAPWARE") or upper.startswith("SINK ACCESSORIES") or upper.startswith("NOTES"):
            continue
        if _imperial_is_supplier_only_line(line):
            continue
        if re.match(r"(?i)^taphole location\s*:", line):
            location = normalize_space(re.sub(r"(?i)^taphole location\s*:\s*", "", line))
            if location:
                normalized_location = normalize_brand_casing_text(location)
                if kind == "sinkware" and re.search(r"(?i)\b(?:undermount|topmount|above counter|matrix sink|double bowl|double drain|specs tbc)\b", normalized_location):
                    values.append(normalized_location)
                else:
                    notes.append(f"Taphole location: {normalized_location}")
                pending_taphole = False
            else:
                pending_taphole = True
            continue
        if pending_taphole:
            if _imperial_is_non_joinery_note_line(line, kind):
                notes.append(f"Taphole location: {normalize_brand_casing_text(line)}")
                pending_taphole = False
                continue
            if kind == "sinkware" and re.search(r"(?i)\b(?:undermount|topmount|above counter|matrix sink|double bowl|double drain|specs tbc)\b", line):
                values.append(normalize_brand_casing_text(line))
                pending_taphole = False
                continue
            pending_taphole = False
        if "UNDERMOUT" in upper:
            notes.append("Undermounted")
            continue
        if re.match(r"(?i)^n\s*/?\s*a\b.*\bby others\b", line):
            continue
        if re.match(r"(?i)^.*\bBY IMPERIAL\b$", line):
            continue
        if re.match(r"(?i)^\d{1,2}/\d{1,2}/\d{2,4}", line):
            continue
        if re.match(r"(?i)^(available to back order|by imperial|by client)\b", line):
            continue
        cleaned_line = re.sub(r"(?i)\bN\s*/?\s*A\s*-\s*By others(?:\s+By others)?\b", "", line)
        cleaned_line = re.sub(r"(?i)\bN\s*/\s*A\b(?:\s*-\s*By others(?:\s+By others)?)?", "", cleaned_line)
        cleaned_line = normalize_brand_casing_text(normalize_space(cleaned_line)).strip(" -;,")
        cleaned_line = re.sub(r"(?i)\bSINKWARE\s*&\s*TAPWARE\b", "", cleaned_line).strip(" -;,")
        cleaned_line = re.sub(r"(?i)\b(?:client name|designer|signature|signed date|document ref)\b.*$", "", cleaned_line).strip(" -;,")
        cleaned_line = re.sub(r"(?i)\bSPECS\s*/\s*DESCRIPTION\b.*$", "", cleaned_line).strip(" -;,")
        cleaned_line = re.sub(r"(?i)\bAREA\s*/\s*ITEM\b.*$", "", cleaned_line).strip(" -;,")
        if kind == "sinkware" and re.search(r"(?i)\btapware location\b", cleaned_line):
            cleaned_line = re.sub(r"(?i)\btapware location\b", "Taphole location", cleaned_line)
        if kind == "tapware" and re.search(r"(?i)\b(?:undermount|sink\b|double bowl|drain|sink mounting|solid surface wall basin)\b", cleaned_line) and not re.search(r"(?i)\b(?:tap|mixer|gooseneck|pull[ -]?out|filter)\b", cleaned_line):
            continue
        if kind == "tapware":
            cleaned_line = re.sub(r"(?i)\bBY CLIENT\b.*$", "", cleaned_line).strip(" -;,")
            cleaned_line = re.sub(r"(?i)\b(?:client name|designer|signature|signed date|document ref)\b.*$", "", cleaned_line).strip(" -;,")
        if kind == "sinkware" and re.search(r"(?i)\bwall mounted taps\b", cleaned_line):
            continue
        if cleaned_line:
            values.append(cleaned_line)
    unique_values = _unique(values)
    value_text = normalize_space(" ".join(unique_values)).strip(" -;,")
    for repeated_value in unique_values:
        doubled = normalize_space(f"{repeated_value} {repeated_value}")
        if doubled and doubled in value_text:
            value_text = normalize_space(value_text.replace(doubled, repeated_value)).strip(" -;,")
    if kind == "sinkware":
        value_text = re.sub(r"(?i)\bN\s*/\s*A\b(?:\s*-\s*By others(?:\s+By others)?)?", "", value_text).strip(" -;,")
        value_text = re.sub(
            r"(?i)\b(undermount(?:ed)?\s*-\s*specs\s*tbc)\s+\1\b",
            r"\1",
            value_text,
        ).strip(" -;,")
    notes = _unique(notes)
    if kind == "tapware":
        tap_start = re.search(r"(?i)\b(?:veronar|phoenix|furnware|abey|caroma|parisi|franke|alder|abi interiors|tap|mixer|gooseneck|pull[ -]?out|filter)\b", value_text)
        sink_prefix = re.search(r"(?i)\b[A-Z0-9.]*SINK[A-Z0-9.]*\b", value_text)
        if tap_start and sink_prefix and sink_prefix.start() < tap_start.start():
            value_text = value_text[tap_start.start() :].strip(" -;,")
        value_text = re.sub(r"(?i)\s*Taphole location\b.*$", "", value_text).strip(" -;,")
        return value_text
    if not value_text and not notes:
        return ""
    return " - ".join(part for part in [value_text, *notes] if part)


def _collect_room_overlays(documents: list[dict[str, object]], room_master_file: str = "") -> dict[str, dict[str, str]]:
    overlays: dict[str, dict[str, str]] = {}
    for document in documents:
        file_name = str(document.get("file_name", ""))
        full_text = "\n\n".join(
            str(page.get("text") or page.get("raw_text") or "")
            for page in document.get("pages", [])
            if page.get("text") or page.get("raw_text")
        )
        if not full_text.strip():
            continue
        sections = _collect_room_sections_for_document(document)
        material_allowed = not room_master_file or file_name == room_master_file
        for section in sections:
            chunk = str(section.get("text", "") or "")
            detected_room_key = str(section.get("section_key", "") or "")
            room_label = source_room_label(str(section.get("original_section_label", "")), fallback_key=detected_room_key)
            room_key = source_room_key(room_label, fallback_key=detected_room_key)
            lines = _section_lines(section)
            section_page_type = normalize_space(str(section.get("page_type", "") or "")).lower().replace(" ", "_")
            section_material_allowed = material_allowed and section_page_type in {"", "joinery"}
            overlay = overlays.setdefault(
                room_key,
                {
                    "bench_tops_wall_run": "",
                    "bench_tops_island": "",
                    "bench_tops_other": "",
                    "floating_shelf": "",
                    "door_colours_overheads": "",
                    "door_colours_base": "",
                    "door_colours_tall": "",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "has_explicit_overheads": False,
                    "has_explicit_base": False,
                    "has_explicit_tall": False,
                    "has_explicit_island": False,
                    "has_explicit_bar_back": False,
                    "led": "",
                    "accessories": [],
                    "other_items": [],
                    "sink_info": "",
                    "basin_info": "",
                    "tap_info": "",
                },
            )
            if section_material_allowed:
                generic_bench_tops = _collect_field(lines, ["Bench Tops", "Benchtop"])
                explicit_bench_values = _unique(
                    [
                        *(f"Back Benchtops {value}" for value in _collect_field(lines, ["Back Benchtops", "Wall Run Bench Top"])),
                        *(f"Island Benchtop {value}" for value in _collect_field(lines, ["Island Benchtop", "Island Bench Top"])),
                    ]
                )
                benchtop_groups = _split_benchtop_groups(generic_bench_tops + explicit_bench_values)
                for key, value in benchtop_groups.items():
                    overlay[key] = value or overlay[key]
                overhead_value = _first_value(_collect_field(lines, ["Overhead Cupboards", "Upper Cabinetry Colour + Tall Cabinets", "Upper Cabinetry Colour"]))
                base_value = _first_value(_collect_field(lines, ["Base Cupboards & Drawers", "Floor Mounted Vanity", "Base Cabinetry Colour"]))
                tall_value = _first_value(_collect_field(lines, ["Tall Cabinets", "Tall Cabinet", "Tall Doors", "Upper Cabinetry Colour + Tall Cabinets"]))
                island_value = _first_value(_collect_field(lines, ["Island Bench Base Cupboards & Drawers"]))
                bar_back_value = _first_value(_collect_field(lines, ["Island Bar Back"]))
                if overhead_value:
                    overlay["has_explicit_overheads"] = True
                if base_value:
                    overlay["has_explicit_base"] = True
                if tall_value:
                    overlay["has_explicit_tall"] = True
                if island_value:
                    overlay["has_explicit_island"] = True
                if bar_back_value:
                    overlay["has_explicit_bar_back"] = True
                overlay["door_colours_overheads"] = _merge_clean_group_text(overlay["door_colours_overheads"], overhead_value, cleaner=_clean_door_colour_value)
                overlay["door_colours_base"] = _merge_clean_group_text(overlay["door_colours_base"], base_value, cleaner=_clean_door_colour_value)
                overlay["door_colours_tall"] = _merge_clean_group_text(overlay["door_colours_tall"], tall_value, cleaner=_clean_door_colour_value)
                overlay["door_colours_island"] = _merge_clean_group_text(overlay["door_colours_island"], island_value, cleaner=_clean_door_colour_value)
                overlay["door_colours_bar_back"] = _merge_clean_group_text(overlay["door_colours_bar_back"], bar_back_value, cleaner=_clean_door_colour_value)
                if not _has_explicit_door_group_markers(overlay):
                    door_groups = _split_door_colour_groups(_collect_field(lines, DOOR_COLOUR_FIELD_PREFIXES))
                    for key, value in door_groups.items():
                        overlay[key] = _merge_clean_group_text(overlay[key], value, cleaner=_clean_door_colour_value)
                overlay["floating_shelf"] = _merge_text(overlay["floating_shelf"], _first_value(_collect_field(lines, ["Floating Shelves", "Floating Shelf"])))
                if _collect_field(lines, ["LED Strip Lighting", "LED Lighting", "LED"]):
                    overlay["led"] = "Yes"
                overlay["accessories"] = _merge_lists(_coerce_string_list(overlay.get("accessories", [])), _collect_field(lines, ["Accessories", "Accessory"]))
                overlay["other_items"] = _merge_other_items(
                    overlay.get("other_items", []),
                    [
                        {"label": "RAIL", "value": _first_value(_collect_field(lines, ["Rail"]))},
                        {"label": "JEWELLERY INSERT", "value": _first_value(_collect_field(lines, ["Jewellery Insert"]))},
                    ],
                )
            overlay["sink_info"] = _merge_text(overlay["sink_info"], _first_value(_collect_field(lines, ["Sink Type/Model", "Sink Type", "Drop in Tub", "Sink"])))
            basin_value = _first_value(_collect_field(lines, ["Vanity Inset Basin"])) or _first_value(_collect_field(lines, ["Basin"]))
            overlay["basin_info"] = _merge_text(overlay["basin_info"], basin_value)
            overlay["tap_info"] = _merge_text(
                overlay["tap_info"],
                _first_value(_collect_field(lines, ["Vanity Tap Style", "Tap Type", "Tap Style", "Sink Mixer", "Pull-Out Mixer", "Mixer"])),
            )
    return overlays


def _collect_imperial_room_overlays(documents: list[dict[str, object]]) -> dict[str, dict[str, str]]:
    overlays: dict[str, dict[str, str]] = {}
    for document in documents:
        for page in document.get("pages", []):
            layout = page.get("page_layout") if isinstance(page.get("page_layout"), dict) else {}
            used_layout = False
            raw_text = str(page.get("raw_text") or page.get("text") or "")
            layout_mode = normalize_space(str(page.get("layout_mode", "") or "")).lower()
            layout_is_precise = bool(layout and (layout_mode != "lightweight" or page.get("vision_applied")))
            effective_page_type = _effective_layout_page_type("Imperial", normalize_space(str(layout.get("page_type", "") or "")).lower(), raw_text, layout) if layout else ""
            if layout_is_precise and effective_page_type == "sinkware_tapware":
                room_blocks = list(layout.get("room_blocks", []) or [])
                for block in room_blocks:
                    if not isinstance(block, dict):
                        continue
                    room_label = normalize_space(str(block.get("room_label", "") or layout.get("room_label", "") or ""))
                    if not room_label:
                        continue
                    room_key = source_room_key(room_label, fallback_key=room_label)
                    overlay = overlays.setdefault(room_key, _blank_overlay())
                    for raw_row in block.get("rows", []) or []:
                        if not isinstance(raw_row, dict):
                            continue
                        row = _layout_row_record(raw_row, page_no=int(page.get("page_no", 0) or 0), room_identity=room_label) or dict(raw_row)
                        row_kind = normalize_space(str(row.get("row_kind", "") or "")).lower().replace(" ", "_")
                        row_label_upper = normalize_space(str(row.get("row_label", "") or "")).upper()
                        if row_kind == "sink" or row_label_upper.startswith("SINKWARE"):
                            sink_text = _imperial_layout_row_fixture_entry(row, "sink")
                            if sink_text:
                                overlay["sink_info"] = _merge_text(overlay["sink_info"], sink_text)
                                used_layout = True
                        elif row_kind == "tap" or row_label_upper.startswith("TAPWARE"):
                            tap_text = _imperial_layout_row_fixture_entry(row, "tap")
                            if tap_text:
                                overlay["tap_info"] = _merge_text(overlay["tap_info"], tap_text)
                                used_layout = True
                        elif row_kind == "basin" or row_label_upper.startswith("BASIN"):
                            basin_text = _imperial_layout_row_fixture_entry(row, "basin")
                            if basin_text:
                                overlay["basin_info"] = _merge_text(overlay["basin_info"], basin_text)
                                used_layout = True
            if used_layout:
                continue
            text = raw_text
            if not text.strip():
                continue
            if not _is_imperial_non_joinery_page(text) and "SINKWARE" not in text.upper() and "TAPWARE" not in text.upper():
                continue
            for room_label, sink_text in _imperial_extract_non_joinery_blocks(text, "sinkware"):
                room_key = source_room_key(room_label, fallback_key=room_label)
                overlay = overlays.setdefault(room_key, _blank_overlay())
                overlay["sink_info"] = _merge_text(overlay["sink_info"], sink_text)
            for room_label, tap_text in _imperial_extract_non_joinery_blocks(text, "tapware"):
                room_key = source_room_key(room_label, fallback_key=room_label)
                overlay = overlays.setdefault(room_key, _blank_overlay())
                overlay["tap_info"] = _merge_text(overlay["tap_info"], tap_text)
    return overlays


def _match_room_overlay(row: dict[str, Any], overlays: dict[str, dict[str, str]]) -> dict[str, str]:
    for key in _room_lookup_candidates(row):
        if key in overlays:
            return overlays[key]
    return _blank_overlay()


def _blank_overlay() -> dict[str, Any]:
    return {
        "bench_tops_wall_run": "",
        "bench_tops_island": "",
        "bench_tops_other": "",
        "floating_shelf": "",
        "door_colours_overheads": "",
        "door_colours_base": "",
        "door_colours_tall": "",
        "door_colours_island": "",
        "door_colours_bar_back": "",
        "has_explicit_overheads": False,
        "has_explicit_base": False,
        "has_explicit_tall": False,
        "has_explicit_island": False,
        "has_explicit_bar_back": False,
        "led": "",
        "accessories": [],
        "other_items": [],
        "sink_info": "",
        "basin_info": "",
        "tap_info": "",
    }


def _room_lookup_candidates(row: dict[str, Any]) -> list[str]:
    texts = [_string_value(row.get("room_key", "")), _string_value(row.get("original_room_label", ""))]
    candidates: list[str] = []
    for text in texts:
        normalized = source_room_key(text)
        if not normalized:
            continue
        candidates.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate and candidate not in seen:
            deduped.append(candidate)
            seen.add(candidate)
    return deduped


DOOR_CONTEXT_TOKENS = (
    "upper",
    "overhead",
    "tall cabinetry",
    "tall cabinet",
    "tall cabinets",
    "tall door",
    "tall doors",
    "tall panel",
    "tall panels",
    "base cabinetry",
    "base cabinet",
    "cooktop run",
    "island",
    "bar back",
    "back panel",
    "back panels",
)


def _clean_handle_value(value: Any) -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    text = re.sub(r"^\((?:to all lower doors? & drawers?)\)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bHandle House\b", "", text, flags=re.IGNORECASE)
    text = normalize_brand_casing_text(text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" -;,")


def _split_handle_text_fragments(text: str) -> list[str]:
    fragments = [normalize_space(text)]
    split_patterns = (
        r"(?i)(?=No handles? on \w)",
        r"(?i)(?=No handle for \w)",
        r"(?i)(?=No handles?\s*-\s*[A-Z])",
        r"(?i)(?=Recessed finger space\b)",
        r"(?i)(?=Finger Pull on Uppers\b)",
        r"(?i)(?=Touch catch\b)",
        r"(?i)(?<!- )(?=Momo\b)",
        r"(?i)(?<!- )(?=Kethy\b)",
        r"(?i)(?<!- )(?=Custom Made Handles\b)",
        r"(?i)(?=Doors\s*-\s*[A-Z0-9-])",
        r"(?i)(?=Drawers\s*-\s*[A-Z0-9-])",
    )
    for pattern in split_patterns:
        updated: list[str] = []
        for fragment in fragments:
            split_points = [match.start() for match in re.finditer(pattern, fragment) if match.start() > 0]
            if not split_points:
                updated.append(fragment)
                continue
            indexes = [0, *split_points, len(fragment)]
            for index in range(len(indexes) - 1):
                part = normalize_space(fragment[indexes[index] : indexes[index + 1]])
                if part:
                    updated.append(part)
        fragments = updated
    merged: list[str] = []
    index = 0
    while index < len(fragments):
        fragment = fragments[index]
        if (
            index + 1 < len(fragments)
            and re.search(r"(?i)^No handles?\s+(?:for\s+|to\s+)?overheads?\b", fragment)
            and re.search(r"(?i)^Recessed finger space\b", fragments[index + 1])
        ):
            merged.append(normalize_space(f"{fragment.rstrip(' -')} - {fragments[index + 1]}"))
            index += 2
            continue
        merged.append(fragment)
        index += 1
    return [fragment for fragment in merged if fragment]


def _extract_handle_parts(value: Any) -> list[str]:
    if isinstance(value, dict):
        parts: list[str] = []
        for item in value.values():
            parts.extend(_extract_handle_parts(item))
        return _unique(parts)
    if isinstance(value, str):
        text = normalize_space(value)
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = literal_eval(text)
            except (ValueError, SyntaxError):
                parsed = None
            if isinstance(parsed, dict):
                return _extract_handle_parts(parsed)
        cleaned = _clean_handle_value(text)
        if not cleaned:
            return []
        return [_clean_handle_value(part) for part in _split_handle_text_fragments(cleaned) if _clean_handle_value(part)]
    cleaned = _clean_handle_value(value)
    return [cleaned] if cleaned else []


def _clean_handle_entries(values: list[str]) -> list[str]:
    cleaned_entries: list[str] = []
    for value in values:
        for entry in _extract_handle_parts(value):
            supplier, remainder = _normalize_entry_supplier_text(entry)
            note = ""
            if remainder and not _is_handle_note_like(remainder):
                note_match = re.search(
                    r"(?i)\b(?:horizontal on|vertical on|finger pull|recessed finger|touch catch|pto\b|no handles?|no handle for)\b.*$",
                    remainder,
                )
                if note_match and note_match.start() > 0:
                    note = remainder[note_match.start() :]
                    remainder = remainder[: note_match.start()]
            formatted = _compose_supplier_description_note(supplier, remainder, note)
            cleaned_entries.append(formatted or entry)
    merged_supplier_entries: list[str] = []
    pending_suppliers: list[str] = []
    for entry in cleaned_entries:
        normalized_entry = normalize_brand_casing_text(normalize_space(entry)).replace("每", "").strip(" -;,")
        normalized_entry = re.sub(r"[^\x00-\x7F]+", " ", normalized_entry)
        normalized_entry = normalize_space(normalized_entry).strip(" -;,")
        if not normalized_entry:
            continue
        if normalized_entry in ENTRY_SUPPLIER_HINTS:
            pending_suppliers.append(normalized_entry)
            continue
        if pending_suppliers and _is_handle_description_like(normalized_entry):
            supplier_text = " / ".join(_unique(pending_suppliers))
            normalized_entry = _compose_supplier_description_note(supplier_text, normalized_entry)
            pending_suppliers = []
        merged_supplier_entries.append(normalized_entry)
    merged_supplier_entries.extend(pending_suppliers)
    unique_entries = _unique(merged_supplier_entries)
    supplier_only_entries = [entry for entry in unique_entries if entry in ENTRY_SUPPLIER_HINTS]
    description_entries = [entry for entry in unique_entries if entry not in supplier_only_entries]
    if supplier_only_entries:
        merged_description_entries: list[str] = []
        pending_supplier = " / ".join(_unique(supplier_only_entries))
        merged_any = False
        for entry in description_entries:
            entry_supplier, _ = _normalize_entry_supplier_text(entry)
            merge_probe = entry
            note_match = re.search(
                r"(?i)\b(?:horizontal on|vertical on|finger pull|recessed finger|touch catch|pto\b|no handles?|no handle for)\b.*$",
                merge_probe,
            )
            if note_match and note_match.start() > 0:
                merge_probe = merge_probe[: note_match.start()]
            merge_probe = normalize_space(merge_probe).strip(" -;,")
            if (
                pending_supplier
                and not entry_supplier
                and merge_probe
                and not _is_handle_note_like(merge_probe)
                and _imperial_handle_entry_is_valid(entry)
            ):
                merged_description_entries.append(_compose_supplier_description_note(pending_supplier, entry))
                pending_supplier = ""
                merged_any = True
            else:
                merged_description_entries.append(entry)
        unique_entries = merged_description_entries if merged_any else [*description_entries, *supplier_only_entries]
    filtered: list[str] = []
    for entry in unique_entries:
        lowered = entry.lower()
        note_only_match = re.match(r"^(?P<prefix>[^-]+?)\s*-\s*(?P<note>.+)$", entry)
        if note_only_match:
            note_text = normalize_brand_casing_text(note_only_match.group("note"))
            if _is_handle_note_like(note_text) and not _is_handle_description_like(note_text):
                if any(
                    other.lower() != lowered and note_text.lower() in other.lower()
                    for other in unique_entries
                ):
                    continue
        if any(lowered != other.lower() and lowered in other.lower() for other in unique_entries):
            continue
        filtered.append(entry)
    return filtered


def _clean_accessory_entries(values: list[str]) -> list[str]:
    cleaned_entries: list[str] = []
    for value in values:
        text = normalize_brand_casing_text(normalize_space(value))
        if not text:
            continue
        if re.search(r"(?i)\b(?:spring free|upgrade promotion|document ref|address:|client:|date:)\b", text):
            continue
        if _imperial_is_supplier_only_line(text):
            continue
        material_probe = re.sub(r"(?i)^gpo\s*-\s*", "", text).strip(" -;,")
        if _imperial_value_looks_material_note(material_probe):
            continue
        supplier, remainder = _normalize_entry_supplier_text(text)
        cleaned_entries.append(_compose_supplier_description_note(supplier, remainder) if supplier else remainder)
    return _unique(cleaned_entries)


def _split_group_entries(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            result.extend(_split_group_entries(item))
        return _unique(result)
    text = normalize_space(str(value or ""))
    if not text:
        return []
    return _unique([normalize_space(part) for part in re.split(r"\s*\|\s*|\s*;\s*|\n+", text) if normalize_space(part)])


def _clean_door_colour_value(value: Any) -> str:
    text = normalize_space(re.sub(r"(?i)^door(?:/panel)? colour(?:s)?(?:\s*\d+)?\s*-\s*", "", str(value or "")))
    if not text:
        return ""
    if re.fullmatch(r"(?i)(?:not applicable|n/?a)(?:\s+manufacturer)?(?:\s+colour\s*&\s*finish)?", text.strip(" -;,") or ""):
        return ""
    text = normalize_brand_casing_text(text)
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in CABINET_ONLY_EXCLUDE_PATTERNS):
        return ""
    token_pattern = "|".join(re.escape(token) for token in DOOR_CONTEXT_TOKENS)
    text = re.sub(rf"(?i)\s+\b(?:to|for)\b\s+.*(?:{token_pattern}).*$", "", text)
    text = re.sub(rf"(?i)\s+-\s*(?:{token_pattern}).*$", "", text)
    text = re.sub(rf"(?i)(?<=\w)\s+(?:{token_pattern}).*$", "", text)
    text = re.sub(rf"(?i)\s*\((?:{token_pattern}).*$", "", text)
    text = re.sub(r"\s*\([^)]*$", "", text)
    text = re.sub(r"\s{2,}", " ", text)
    text = text.strip(" -;,")
    orientation_text = normalize_space(re.sub(r"(?i)\b(?:polytec|laminex|caesarstone|smartstone|wk stone)\b", "", text)).strip(" -;,")
    if re.fullmatch(r"(?i)(?:vertical|horizontal)(?:\s+on.*)?", orientation_text):
        return ""
    if re.fullmatch(r"(?i)incl\.?\s+spring\s+free.*", text):
        return ""
    if re.search(r"(?i)(kickboards?|bench\s*top|benchtop|thermolaminate notes?|carcass|shelf edges?)", text):
        return ""
    if re.search(r"(?i)\b([a-z])\1{4,}\b", text):
        return ""
    return text


def _material_signature(value: str) -> str:
    lowered = value.lower()
    lowered = re.sub(r"\b(finish|profile|vertical|horizontal|grain|direction|notes?)\b", " ", lowered)
    return re.sub(r"[^a-z0-9]+", "", lowered)


def _dedupe_prefer_specific(entries: list[str], cleaner: Any = None) -> list[str]:
    result: list[str] = []
    signatures: list[str] = []
    for entry in entries:
        cleaned = cleaner(entry) if cleaner else normalize_space(str(entry or ""))
        if not cleaned:
            continue
        signature = _material_signature(cleaned)
        if not signature:
            continue
        replaced = False
        for index, existing_signature in enumerate(signatures):
            if signature == existing_signature:
                if len(cleaned) > len(result[index]):
                    result[index] = cleaned
                replaced = True
                break
            if signature in existing_signature:
                replaced = True
                break
            if existing_signature in signature:
                result[index] = cleaned
                signatures[index] = signature
                replaced = True
                break
        if not replaced:
            result.append(cleaned)
            signatures.append(signature)
    return result


def _merge_clean_group_text(*values: Any, cleaner: Any = None) -> str:
    entries: list[str] = []
    for value in values:
        entries.extend(_split_group_entries(value))
    return " | ".join(_dedupe_prefer_specific(entries, cleaner=cleaner))


def _prune_door_group_overlap(groups: dict[str, str]) -> dict[str, str]:
    cleaned = {
        key: _dedupe_prefer_specific(_split_group_entries(value), cleaner=_clean_door_colour_value)
        for key, value in groups.items()
    }
    island_signatures = [_material_signature(entry) for entry in cleaned.get("door_colours_island", []) + cleaned.get("door_colours_bar_back", []) if entry]
    if island_signatures and len(cleaned["door_colours_base"]) > 1:
        filtered: list[str] = []
        for entry in cleaned["door_colours_base"]:
            signature = _material_signature(entry)
            if any(signature == other or signature in other or other in signature for other in island_signatures):
                continue
            filtered.append(entry)
        if filtered:
            cleaned["door_colours_base"] = filtered
    return {key: " | ".join(value) for key, value in cleaned.items()}


def _split_door_colour_groups(values: list[str]) -> dict[str, str]:
    grouped = {
        "door_colours_overheads": [],
        "door_colours_base": [],
        "door_colours_tall": [],
        "door_colours_island": [],
        "door_colours_bar_back": [],
    }
    for raw_value in values:
        split_values = [
            normalize_space(part)
            for part in re.split(r"(?i)\s*,\s*(?=(?:pol\s*ytec|polytec|laminex|polytec|melamine|thermolaminate))", raw_value)
            if normalize_space(part)
        ]
        for value in split_values or [raw_value]:
            text = normalize_space(re.sub(r"(?i)^door(?:/panel)? colour(?:s)?(?:\s*\d+)?\s*-\s*", "", value))
            if not text:
                continue
            cleaned = _clean_door_colour_value(text)
            if not cleaned:
                continue
            lowered = text.lower()
            matched = False
            if any(token in lowered for token in ["upper", "overhead"]):
                grouped["door_colours_overheads"].append(cleaned)
                matched = True
            if any(token in lowered for token in ["tall cabinetry", "tall cabinet", "tall cabinets", "tall door", "tall doors", "tall panel", "tall panels"]):
                grouped["door_colours_tall"].append(cleaned)
                matched = True
            if "base" in lowered and "island" not in lowered:
                grouped["door_colours_base"].append(cleaned)
                matched = True
            if "island" in lowered:
                grouped["door_colours_island"].append(cleaned)
                matched = True
            if any(token in lowered for token in ["bar back", "back panel", "back panels"]):
                grouped["door_colours_bar_back"].append(cleaned)
                matched = True
            if not matched:
                grouped["door_colours_base"].append(cleaned)
    return {key: " | ".join(_dedupe_prefer_specific(entries, cleaner=_clean_door_colour_value)) for key, entries in grouped.items()}


def _blank_door_group_values() -> dict[str, str]:
    return {
        "door_colours_overheads": "",
        "door_colours_base": "",
        "door_colours_tall": "",
        "door_colours_island": "",
        "door_colours_bar_back": "",
    }


def _has_explicit_door_group_markers(row: Any) -> bool:
    if isinstance(row, dict):
        getter = lambda key: row.get(key, False)
    else:
        getter = lambda key: getattr(row, key, False)
    return any(
        bool(getter(key))
        for key in ("has_explicit_overheads", "has_explicit_base", "has_explicit_tall", "has_explicit_island", "has_explicit_bar_back")
    )


BENCHTOP_WALL_HINTS = ("wall run", "cooktop run", "wall bench", "wall side", "back benchtops", "back benchtop")
BENCHTOP_ISLAND_HINTS = ("island bench", "island")

IMPERIAL_SECTION_TITLE_RE = re.compile(r"(?im)\b(?P<title>[A-Z][A-Z +/&'\-]{2,}?)\s+JOINERY SELECTION SHEET\b")
IMPERIAL_SPECIAL_SECTION_TITLES = {"FEATURE TALL DOORS"}
IMPERIAL_NON_JOINERY_HEADINGS = {
    "APPLIANCES",
    "SINKWARE & TAPWARE",
    "SINKWARE",
    "TAPWARE",
}
IMPERIAL_HEADER_START_MARKERS = (
    "Ceiling height:",
    "Bulkhead:",
    "Shadowline:",
    "Hinges & Drawer Runners:",
    "Floor Type & Kick refacing required:",
    "AREA / ITEM",
    "NOTES",
    "FEATURE CABINETRY COLOUR",
    "SPLASHBACK",
    "BENCHTOP",
    "UPPER CABINETRY",
    "BASE CABINETRY",
    "KICKBOARD",
    "KICKBOARDS",
    "HANDLES",
    "TALL DOORS",
    "ISLAND CABINETRY COLOUR",
    "TALL CABINETRY COLOUR",
    "CABINETRY COLOUR",
    "FLOATING SHELVING COLOUR",
    "HANGING RAIL",
    "HAMPER",
    "MIRRORED SHAVING CABINET",
    "GPO'S",
    "BIN",
    "EXTRA TOP IN",
)
IMPERIAL_FOOTER_MARKERS = (
    "CLIENT NAME:",
    "SIGNATURE:",
    "SIGNED DATE:",
    "DESIGNER:",
    "ALL COLOURS SHOWN ARE APPROXIMATE REPRESENTATIONS ONLY",
    "SUBJECT TO SUPPLIER AT TIME OF INSTALL.",
    "Document Ref:",
)
IMPERIAL_SUPPLIER_ONLY_LINES = {
    "Polytec",
    "Caesarstone",
    "Ceasarstone",
    "Smartstone",
    "Laminex",
    "WK Stone",
    "Furnware",
    "Lincoln Sentry",
    "Titus Tekform",
    "ABEY",
    "Hettich",
    "Veronar",
    "Tanova",
    "Safe Desk",
    "OE Elsafe",
    "ABI Interiors",
    "Franke",
}
IMPERIAL_HANDLE_SUPPLIER_HINTS = {
    "Furnware",
    "Titus Tekform",
    "Lincoln Sentry",
    "ABI Interiors",
}
CABINETRY_SUPPLIER_HINTS = {"Polytec", "Laminex"}
ENTRY_SUPPLIER_HINTS = {
    *set(CANONICAL_BRAND_LABELS.values()),
    *IMPERIAL_SUPPLIER_ONLY_LINES,
    "Hettich",
    "Veronar",
    "Tanova",
    "Safe Desk",
    "OE Elsafe",
    "Furnware",
    "ABI Interiors",
    "Franke",
}
IMPERIAL_SECTION_FIELD_PATTERNS: list[tuple[str, str]] = [
    ("overhead_feature_cabinetry", r"GLASS INLAY DOORS\s+TO OVERHEAD\s+FEATURE CABINETRY\b"),
    ("feature_tall_bar_back", r"FEATURE TALL\s+CABINETRY COLOUR(?:\s*\+\s*bar back)?\b"),
    ("upper_tall", r"UPPER CABINETRY COLOUR\s*\+\s*TALL CABINETS\b"),
    ("island_cabinetry", r"ISLAND CABINETRY COLOUR\b"),
    ("base_back_wall", r"BACK WALL\s*&\s*COFFEE NOOK INTERNAL\s+CABINETRY COLOUR\b"),
    ("upper", r"UPPER CABINETRY COLOUR(?:\s+DOORS)?\b"),
    ("tall_cabinetry", r"TALL CABINETRY COLOUR\b"),
    ("cabinetry_colour", r"CABINETRY COLOUR(?:\s*&\s*TOP(?:\s*\([^)]*\))?)?\b"),
    ("feature_cabinetry", r"FEATURE CABINETRY COLOUR\b"),
    ("mirrored_shaving_cabinet", r"MIRRORED SHAVING CABINET\b"),
    ("handles_overheads", r"HANDLES\s+to\s+OVERHEADS\b"),
    ("handles_base", r"HANDLES\s+BASE\s+CABS\b"),
    ("custom_handles", r"CUSTOM HANDLES\b"),
    ("handles", r"HANDLES\b"),
    ("base", r"BASE CABINETRY COLOUR\b"),
    ("splashback", r"SPLASHBACK(?:\s+COLOUR)?\b"),
    ("bench_tops", r"BENCHTOPS?(?:\s+COLOUR)?\b"),
    ("floating_shelf", r"(?:FEATURE TIMBER LOOK\s+)?FLOATING SHELV(?:ES|ING)(?:\s+COLOUR)?\b"),
    ("led", r"LED'?S?(?:\s+STRIP\s+LIGHTING|\s+LIGHTING)?\b"),
    ("accessories", r"ACCESSORIES?\b"),
    ("flooring", r"FLOOR TYPE\s*&\s*KICK REFACING REQUIRED\b"),
    ("hanging_rail", r"HANGING RAIL\b"),
    ("hamper", r"HAMPER\b"),
    ("gpo", r"GPO'?S\b"),
    ("bin", r"BIN\b"),
    ("rail", r"RAIL\b"),
    ("jewellery_insert", r"JEWELLERY\s+INSERT\b"),
    ("toe_kick", r"KICKBOARDS?\b"),
    ("tall_doors", r"TALL DOORS\b"),
    ("extra_top", r"EXTRA TOP IN\b"),
]

IMPERIAL_CURATED_OTHER_FIELD_KEYS = {
    "rail": "RAIL",
    "hanging_rail": "RAIL",
    "jewellery_insert": "JEWELLERY INSERT",
}

IMPERIAL_INLINE_SPLIT_MARKERS = (
    "GLASS INLAY DOORS TO OVERHEAD FEATURE CABINETRY",
    "FEATURE TALL CABINETRY COLOUR + bar back",
    "FEATURE TALL CABINETRY COLOUR",
    "FEATURE TIMBER LOOK FLOATING SHELVES",
    "FEATURE CABINETRY COLOUR",
    "UPPER CABINETRY COLOUR + TALL CABINETS",
    "ISLAND CABINETRY COLOUR",
    "UPPER CABINETRY COLOUR",
    "BASE CABINETRY COLOUR",
    "TALL CABINETRY COLOUR",
    "MIRRORED SHAVING CABINET",
    "BENCHTOP+ SPLASHBACK",
    "SPLASHBACK",
    "BENCHTOPS",
    "BENCHTOP",
    "FLOATING SHELF",
    "FLOATING SHELVES",
    "FLOATING SHELVING COLOUR",
    "LED STRIP LIGHTING",
    "LED LIGHTING",
    "LED'S",
    "LED",
    "ACCESSORIES",
    "Floor Type & Kick refacing required:",
    "HANGING RAIL",
    "HAMPER",
    "GPO'S",
    "BIN",
    "CUSTOM HANDLES",
    "HANDLES to OVERHEADS",
    "HANDLES BASE CABS",
    "HANDLES",
    "KICKBOARD",
    "KICKBOARDS",
    "TALL DOORS",
    "EXTRA TOP IN",
    "Bulkhead:",
    "Shadowline:",
    "Hinges & Drawer Runners:",
)

IMPERIAL_AUXILIARY_ROW_START_MARKERS = (
    "BACK WALL & COFFEE NOOK INTERNAL",
    "ISLAND CABINETRY COLOUR",
    "TALL CABINETRY COLOUR",
    "CABINETRY COLOUR",
    "FLOATING SHELVING COLOUR",
    "FLOOR TYPE & KICK REFACING REQUIRED",
    "MIRRORED SHAVING CABINET",
    "HANGING RAIL",
    "KICKBOARD",
    "HAMPER",
    "GPO'S",
    "BIN",
    "EXTRA TOP IN",
    "PIC ",
)

IMPERIAL_AUXILIARY_ROW_TOKENS = (
    "CABINET",
    "RAIL",
    "INSERT",
    "SHELF",
    "GPO",
    "BIN",
    "HAMPER",
    "MIRROR",
    "BENCHTOP",
    "SPLASHBACK",
    "KICK",
    "HANDLE",
    "LED",
    "SEAT",
    "TOP",
)


def _split_benchtop_groups(values: list[str]) -> dict[str, str]:
    grouped = {
        "bench_tops_wall_run": [],
        "bench_tops_island": [],
        "bench_tops_other": [],
    }
    for value in values:
        inline_split = _extract_inline_benchtop_variants(value)
        if inline_split:
            for key, entries in inline_split.items():
                grouped[key].extend(entries)
            continue
        structured = _extract_structured_benchtop_groups(value)
        if structured:
            for key, entries in structured.items():
                grouped[key].extend(entries)
            continue
        for segment in _split_benchtop_segments(value):
            cleaned = _clean_benchtop_segment(segment)
            if not cleaned:
                continue
            lowered = segment.lower()
            if any(token in lowered for token in BENCHTOP_ISLAND_HINTS):
                grouped["bench_tops_island"].append(cleaned)
            elif any(token in lowered for token in BENCHTOP_WALL_HINTS):
                grouped["bench_tops_wall_run"].append(cleaned)
            else:
                grouped["bench_tops_other"].append(cleaned)
    return {key: " | ".join(_unique(entries)) for key, entries in grouped.items()}


def _extract_inline_benchtop_variants(value: Any) -> dict[str, list[str]]:
    text = normalize_space(str(value or ""))
    if not text:
        return {}
    text = re.sub(r"(?i)^bench tops?\s*-\s*", "", text)
    text = re.sub(r"(?i)^benchtop\s*-\s*", "", text)
    match = re.search(
        r"(?is)^(?P<material>.+?)\s*-\s*(?P<wall>.+?)\s*-\s*TO\s+(?:THE\s+)?(?:COOKTOP RUN|WALL RUN|WALL BENCH|WALL SIDE)\s*(?:/|$)\s*(?P<island>.+?)\s*-\s*TO\s+(?:THE\s+)?ISLAND(?:\s+BENCH(?:TOP)?)?(?P<tail>.*)$",
        text,
    )
    if not match:
        return {}
    material = _clean_benchtop_segment(match.group("material"))
    wall_detail = normalize_space(match.group("wall"))
    island_detail = normalize_space(f"{match.group('island')} {match.group('tail')}")
    wall = _clean_benchtop_segment(f"{material} - {wall_detail}") if material and wall_detail else ""
    island = _clean_benchtop_segment(f"{material} - {island_detail}") if material and island_detail else ""
    if not (wall or island):
        return {}
    return {
        "bench_tops_wall_run": [wall] if wall else [],
        "bench_tops_island": [island] if island else [],
        "bench_tops_other": [],
    }


def _extract_structured_benchtop_groups(value: Any) -> dict[str, list[str]]:
    result = {
        "bench_tops_wall_run": [],
        "bench_tops_island": [],
        "bench_tops_other": [],
    }
    parsed: Any = value
    if isinstance(value, str):
        text = normalize_space(value)
        if not (text.startswith("{") and text.endswith("}")):
            return {}
        try:
            parsed = literal_eval(text)
        except (ValueError, SyntaxError):
            return {}
    if not isinstance(parsed, dict):
        return {}
    for key, group_key in (("back", "bench_tops_wall_run"), ("wall_run", "bench_tops_wall_run"), ("wall", "bench_tops_wall_run"), ("island", "bench_tops_island"), ("other", "bench_tops_other")):
        cleaned = _clean_benchtop_segment(parsed.get(key, ""))
        if cleaned:
            result[group_key].append(cleaned)
    return result


def _split_benchtop_segments(value: str) -> list[str]:
    text = normalize_space(value)
    if not text:
        return []
    parts = re.split(r"(?i)\s+\band\b\s+(?=[A-Z0-9])|\s*,\s*(?=(?:quantum|caesarstone|smartstone|silestone|wkstone|polytec|laminex))", text)
    segments = [normalize_space(part) for part in parts if normalize_space(part)]
    return segments or [text]


def _clean_benchtop_segment(value: str) -> str:
    text = normalize_space(value)
    text = re.sub(r"(?i)^back benchtops?\s*", "", text)
    text = re.sub(r"(?i)^wall run bench top\s*", "", text)
    text = re.sub(r"(?i)^island bench top\s*", "", text)
    text = re.sub(r"(?i)^island benchtop\s*", "", text)
    text = re.sub(
        r"(?i)\s+\b(?:to|for)\b\s+(?:the\s+)?(?:cooktop run|wall run|wall bench|wall side|island bench|island)\b.*$",
        "",
        text,
    )
    if re.search(r"(?i)(kickboards?|shadowline|join to be determined|glazing|door legend|client signature|job no|undermount|oven|cooktop|rangehood|handles?)", text):
        return ""
    text = normalize_brand_casing_text(text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip(" -;,")


def _rebuild_benchtop_entries(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    wall_run = _string_value(row.get("bench_tops_wall_run", ""))
    island = _string_value(row.get("bench_tops_island", ""))
    other = _string_value(row.get("bench_tops_other", ""))
    if wall_run:
        values.append(f"Back Benchtops {wall_run}")
    if island:
        values.append(f"Island Benchtop {island}")
    values.extend(_coerce_string_list(other))
    return _unique(values)


def _rebuild_door_panel_colours(row: dict[str, Any]) -> list[str]:
    entries: list[str] = []
    for value in (
        row.get("door_colours_overheads", ""),
        row.get("door_colours_base", ""),
        row.get("door_colours_tall", ""),
        row.get("door_colours_island", ""),
        row.get("door_colours_bar_back", ""),
    ):
        entries.extend(_split_group_entries(value))
    return _dedupe_prefer_specific(entries, cleaner=_clean_door_colour_value)


def _apply_door_colour_groups(row: RoomRow, values: list[str]) -> None:
    groups = _split_door_colour_groups(values)
    row.door_colours_overheads = _merge_clean_group_text(row.door_colours_overheads, groups["door_colours_overheads"], cleaner=_clean_door_colour_value)
    row.door_colours_base = _merge_clean_group_text(row.door_colours_base, groups["door_colours_base"], cleaner=_clean_door_colour_value)
    row.door_colours_tall = _merge_clean_group_text(row.door_colours_tall, groups["door_colours_tall"], cleaner=_clean_door_colour_value)
    row.door_colours_island = _merge_clean_group_text(row.door_colours_island, groups["door_colours_island"], cleaner=_clean_door_colour_value)
    row.door_colours_bar_back = _merge_clean_group_text(row.door_colours_bar_back, groups["door_colours_bar_back"], cleaner=_clean_door_colour_value)


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_coerce_string_list(item))
        return _unique(result)
    if isinstance(value, str):
        parts = re.split(r"\s*\|\s*|\s*;\s*", value)
        return _unique([normalize_space(part) for part in parts if normalize_space(part)])
    text = _string_value(value)
    return [text] if text else []


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return _clean_fixture_text(value)
    if isinstance(value, str):
        return _clean_fixture_text(value)
    if isinstance(value, (list, tuple, set)):
        return " | ".join(part for item in value if (part := _string_value(item)))
    return str(value)


def _merge_text(left: str, right: str) -> str:
    values = [value for value in [_string_value(left), _string_value(right)] if value]
    return " | ".join(_unique(values))


def _coerce_other_items(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        label = normalize_space(str(item.get("label", "")))
        entry_value = _string_value(item.get("value", ""))
        if label and entry_value:
            items.append({"label": label, "value": entry_value})
    return items


def _merge_other_items(left: Any, right: Any) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in [*_coerce_other_items(left), *_coerce_other_items(right)]:
        key = (item["label"].lower(), item["value"].lower())
        if key in seen:
            continue
        merged.append(item)
        seen.add(key)
    return merged


def _is_room_fixture_appliance(row: dict[str, Any]) -> bool:
    appliance_type = _string_value(row.get("appliance_type", "")).lower()
    return any(token in appliance_type for token in ("sink", "basin", "tap", "tub"))


def _clean_fixture_text(value: Any) -> str:
    if isinstance(value, list):
        flattened = [_clean_fixture_text(item) for item in value]
        return " | ".join(part for part in _unique(flattened) if part)
    if isinstance(value, dict):
        return _format_fixture_mapping(value)
    text = normalize_space(str(value))
    if "|" in text:
        parts = [_clean_fixture_text(part) for part in re.split(r"\s*\|\s*", text) if normalize_space(part)]
        return " | ".join(part for part in _unique(parts) if part)
    if text.startswith("[") and text.endswith("]"):
        parsed_list: Any = None
        try:
            parsed_list = literal_eval(text)
        except (ValueError, SyntaxError):
            parsed_list = None
        if isinstance(parsed_list, list):
            return _clean_fixture_text(parsed_list)
    if not text.startswith("{") or not text.endswith("}"):
        text = normalize_brand_casing_text(text)
        text = re.sub(r"(?i)\bSINKWARE\s*&\s*TAPWARE\b", "", text).strip(" -;,")
        text = re.sub(r"(?i)\b(?:client name|designer|signature|signed date|document ref)\b.*$", "", text).strip(" -;,")
        text = re.sub(r"(?i)^tap\s+", "", text).strip(" -;,")
        return text
    parsed: Any
    try:
        parsed = literal_eval(text)
    except (ValueError, SyntaxError):
        return text
    if isinstance(parsed, dict):
        return _clean_fixture_text(parsed)
    return normalize_brand_casing_text(text)


def _format_fixture_mapping(entry: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("sink_type", "basin_type", "tap_type", "type", "sink", "basin", "tap", "description", "value"):
        text = normalize_space(str(entry.get(key, "")))
        if text:
            parts.append(normalize_brand_casing_text(text))
            break
    ownership = normalize_space(str(entry.get("ownership", "")))
    if ownership:
        parts.append(normalize_brand_casing_text(ownership))
    taphole = normalize_space(str(entry.get("taphole_location", "")))
    if taphole:
        parts.append(f"Taphole location: {normalize_brand_casing_text(taphole)}")
    notes = normalize_space(str(entry.get("notes", "")))
    if notes:
        parts.append(normalize_brand_casing_text(notes))
    return " - ".join(part for part in parts if part)


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
