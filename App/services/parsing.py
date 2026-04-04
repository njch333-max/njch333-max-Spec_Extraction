from __future__ import annotations

import re
import zipfile
from ast import literal_eval
from pathlib import Path
from typing import Any, Callable
from xml.etree import ElementTree as ET

import pdfplumber
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
    "rumpus_desk": ["rumpus desk", "rumpus - desk"],
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
    r"(?i)\bsplashback\b.*$",
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
    "fit out",
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
    "wall hung",
    "waste",
    "open shelving",
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

WET_AREA_PLUMBING_BLACKLIST_PATTERNS: tuple[str, ...] = (
    r"\bshower mixer\b",
    r"\bshower screen\b",
    r"\bshower floor waste\b",
    r"\bshower base\b",
    r"\bshower frame\b",
    r"\bshower on rail\b",
    r"\bshower rail(?:\s*/\s*rose)?\b",
    r"\bshower rose\b",
    r"\btowel rail\b",
    r"\bhand towel rail\b",
    r"\bbath towel hooks?\b",
    r"\bhand towel hooks?\b",
    r"\btowel hooks?\b",
    r"\brobe hooks?\b",
    r"\btoilet roll holder\b",
    r"\btoilet suite\b",
    r"\btoilet\b",
    r"\bfloor waste\b",
    r"\bfeature waste\b",
    r"\bin wall mixer\b",
    r"\bbath(?:\s+mixer|\s+spout|\s+waste)?\b",
    r"\bbasin waste\b",
    r"\bbottle trap\b",
)

APPLIANCE_TYPES = ["sink", "cooktop", "oven", "rangehood", "dishwasher", "microwave", "fridge", "refrigerator"]

STRICT_APPLIANCE_FIELD_PREFIXES = {
    "sink type/model",
    "sink type",
    "drop in tub",
    "under bench oven",
    "freestanding cooker",
    "oven",
    "cooktop",
    "microwave make",
    "microwave",
    "dishwasher make",
    "dishwasher",
    "rangehood",
    "integrated fridge/freezer",
    "integrated fridge freezer",
    "fridge/freezer",
    "refrigerator",
    "fridge",
}

APPLIANCE_LABEL_SPECS: list[tuple[str, list[str]]] = [
    ("Sink", [r"Sink Type/Model\s*:", r"Sink Type\s*:", r"Drop in Tub\s*:"]),
    ("Oven", [r"Under Bench Oven\s*:", r"Freestanding Cooker\s*:", r"Oven\s*:", r"(?im)^\s*OVEN(?:\s*/\s*STOVE)?(?:\s*\([^)]*\))?(?:\s+BY CLIENT)?\b"]),
    ("Cooktop", [r"Cooktop\s*:", r"(?im)^\s*COOKTOP(?:\s*\([^)]*\))?(?:\s+AS ABOVE)?(?:\s+BY CLIENT)?\b"]),
    ("Microwave", [r"Microwave Make\s*:", r"Microwave\s*:", r"(?im)^\s*MICROWAVE(?:\s*\([^)]*\))?(?:\s+LEAVE STANDARD SPACE BY CLIENT)?\b"]),
    ("Dishwasher", [r"Dishwasher Make\s*:", r"Dishwasher\s*:", r"(?im)^\s*DISHWASHER(?:\s*\([^)]*\))?(?:\s+BY CLIENT)?\b"]),
    ("Rangehood", [r"Rangehood\s*:", r"(?im)^\s*RANGEHOOD(?:\s*\([^)]*\))?(?:\s+BY CLIENT)?\b"]),
    ("Fridge", [r"Integrated Fridge/Freezer\s*:", r"Integrated Fridge Freezer\s*:", r"Fridge/Freezer\s*:", r"Refrigerator\s*:", r"Fridge\s*:", r"(?im)^\s*FRIDGE(?:\s*/\s*FREEZER)?(?:\s*\([^)]*\))?(?:\s+BY CLIENT(?:\s+PLUMBED IN FRIDGE)?)?\b"]),
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
    "fridge space",
    "specs tbc",
    "docusign envelope id",
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
    "asko": "https://au.asko.com/",
    "bertazzoni": "https://au.bertazzoni.com/",
    "schweigen": "https://schweigen.com.au/",
    "whispar": "https://www.whispar.com.au/",
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
    "asko": "ASKO",
    "bertazzoni": "Bertazzoni",
    "schweigen": "Schweigen",
    "whispar": "Whispar",
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
    "Pantry Door Handles",
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
    r"rumpus\s*-\s*desk",
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
    if not lowered:
        return ""
    for brand, label in sorted(CANONICAL_BRAND_LABELS.items(), key=lambda item: len(item[0]), reverse=True):
        if brand == lowered or brand in lowered:
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
    for pattern in (
        r"bed\s*\d+\s+master\s+walk[- ]in[- ]robe\s+fit\s+out",
        r"bed\s*\d+\s+walk[- ]in[- ]robe\s+fit\s+out",
        r"bed\s*\d+\s+robe\s+fit\s+out",
        r"bed\s*\d+\s+master\s+ensuite\s+vanity",
        r"ground\s+floor\s+powder\s+room",
        r"upper[- ](?:level|floor)\s+powder\s+room",
    ):
        match = re.match(rf"(?i)^(?P<label>{pattern})\b(?P<rest>.*)$", normalized)
        if match:
            label = normalize_space(match.group("label") or "").replace("  ", " ").upper()
            label = label.replace(" WALK IN ROBE ", " WALK IN ROBE ").replace(" FIT OUT", " FIT OUT")
            if "UPPER FLOOR" in label:
                label = label.replace("UPPER FLOOR", "UPPER-LEVEL")
            rest = normalize_space(match.group("rest") or "").strip(" -:/,;")
            return label, rest
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
    if "FIT OUT" in label.upper() and re.match(r"(?i)^(?:walk in )?robe fit out as per plan\b", rest):
        return label
    if not rest or _looks_like_room_field_tail(rest):
        return label
    return ""


def source_room_label(label: str, fallback_key: str = "") -> str:
    text = normalize_space(label)
    if not text and fallback_key:
        text = fallback_key.replace("_", " ")
    if not text:
        return "Room"
    text = re.sub(r"(?i)\bbathoom\b", "bathroom", text)
    detailed_match = _extract_detailed_room_heading(text)
    if detailed_match:
        return detailed_match
    for pattern in ROOM_HEADING_PREFIX_NOISE_PATTERNS:
        previous = None
        while text and previous != text:
            previous = text
            text = normalize_space(re.sub(pattern, "", text))
    text = re.sub(r"(?i)(colour schedule)(?=[A-Z])", r"\1 ", text)
    text = re.sub(r"(?i)\b(WALK[- ]IN[- ]PANTRY)\b\s+PANTRY$", r"\1", text)
    for pattern in ROOM_HEADING_CLEANUP_PATTERNS:
        text = re.sub(pattern, "", text)
    embedded_schedule_heading = _extract_embedded_schedule_room_heading(text)
    if embedded_schedule_heading:
        return embedded_schedule_heading
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


def _extract_detailed_room_heading(text: str) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    reverse_robe_match = re.search(r"(?i)\bROBE\s+FIT\s+OUT\s+TO\s+BED\s+(\d+)\b", normalized)
    if reverse_robe_match:
        return f"BED {reverse_robe_match.group(1)} ROBE"
    patterns: tuple[tuple[str, str | None], ...] = (
        (r"(?i)\b(BED\s*\d+\s+MASTER\s+ENSUITE\s+VANITY)\b", None),
        (r"(?i)\b(BED\s*\d+\s+ENSUITE\s+VANITY)\b", None),
        (r"(?i)\b(GROUND\s+FLOOR\s+BATH(?:ROOM|OOM)\s+VANITY)\b", "BATHROOM VANITY"),
        (r"(?i)\b(BATH(?:ROOM|OOM)\s+VANITY)\b", "BATHROOM VANITY"),
        (r"(?i)\b(GROUND\s+FLOOR\s+POWDER\s+ROOM)\b", "GROUND FLOOR POWDER ROOM"),
        (r"(?i)\b(UPPER[- ](?:LEVEL|FLOOR)\s+POWDER\s+ROOM)\b", "UPPER-LEVEL POWDER ROOM"),
        (r"(?i)\b(BED\s*\d+\s+MASTER\s+WALK[- ]IN[- ]ROBE\s+FIT\s+OUT)\b", None),
        (r"(?i)\b(BED\s*\d+\s+WALK[- ]IN[- ]ROBE\s+FIT\s+OUT)\b", None),
        (r"(?i)\b(BED\s*\d+\s+ROBE\s+FIT\s+OUT)\b", None),
        (r"(?i)\b(BED\s*\d+\s+WALK[- ]IN[- ]ROBE)\b", None),
        (r"(?i)\b(BED\s*\d+\s+ROBE)\b", None),
        (r"(?i)\b(THEATRE\s+ROOM)\b", "THEATRE ROOM"),
    )
    for pattern, replacement in patterns:
        match = re.search(pattern, normalized)
        if not match:
            continue
        if replacement:
            return replacement
        return normalize_space(match.group(1)).replace("-", " ").upper()
    return ""


def _extract_embedded_schedule_room_heading(text: str) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    has_schedule_context = bool(
        re.search(r"(?i)\b(?:colour schedule|joinery selection sheet|supplier description design comments)\b", normalized)
    )
    has_glued_header_context = bool(re.search(r"(?i)[A-Za-z](?:Date|DWG)\b", normalized))
    if not has_schedule_context and not has_glued_header_context:
        return ""
    candidates = [
        normalized,
        re.sub(r"(?<=[a-z])(?=[A-Z])", " ", normalized),
        re.sub(r"(?i)([A-Za-z])(?=(?:Date|DWG)\b)", r"\1 ", normalized),
    ]
    marker_match = re.search(r"(?i)\b(?:colour schedule|joinery selection sheet|supplier description design comments)\b", normalized)
    if marker_match:
        prefix = normalize_space(normalized[: marker_match.start()])
        if prefix:
            candidates.insert(0, prefix)
    best_match = ""
    for candidate in candidates:
        candidate = normalize_space(candidate)
        if not candidate:
            continue
        for pattern in ROOM_HEADING_MATCH_PATTERNS:
            for match in re.finditer(rf"(?i)\b{pattern}\b", candidate):
                value = normalize_space(match.group(0))
                if len(value) >= len(best_match):
                    best_match = value
    if not best_match:
        return ""
    if best_match.lower() == "wip":
        return "WIP"
    return best_match


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
    lowered = re.sub(r"\bupper\s+floor\b", "upper level", lowered)
    lowered = re.sub(r"\bpowder room\b", "powder room", lowered)
    if "ground floor powder room" in lowered:
        return "ground_floor_powder_room"
    if "upper level powder room" in lowered:
        return "upper_level_powder_room"
    if re.search(r"\bground\s+floor\s+bath(?:room|oom)\s+vanity\b", lowered):
        return "bathroom"
    bed_master_ensuite_match = re.search(r"\bbed\s*(\d+)\s+master\s+ensuite\b", lowered)
    if bed_master_ensuite_match:
        return f"ensuite_{bed_master_ensuite_match.group(1)}"
    bed_master_wir_match = re.search(r"\bbed\s*(\d+)\s+master\s+.*\b(?:walk in robe|wir)\b", lowered)
    if bed_master_wir_match:
        return f"bed_{bed_master_wir_match.group(1)}_wir"
    bed_ensuite_match = re.search(r"\bbed\s*(\d+)\s+ensuite\b", lowered)
    if bed_ensuite_match:
        return f"ensuite_{bed_ensuite_match.group(1)}"
    bed_wir_match = re.search(r"\bbed\s*(\d+)\b.*\b(?:walk in robe|wir)\b", lowered)
    if bed_wir_match:
        return f"bed_{bed_wir_match.group(1)}_wir"
    bed_robe_match = re.search(r"\bbed\s*(\d+)\b.*\brobe\b", lowered)
    if bed_robe_match:
        return f"bed_{bed_robe_match.group(1)}_robe"
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
    if prefix in {"Sink Type/Model", "Sink Type", "Drop in Tub", "Sink"} and re.match(
        r"(?i)^(?:Sink Mixer|Pull-?Out Mixer|Basin Mixer|Vanity Tap Style|Tap Type|Tap Style|Mixer|Basin(?:\s+Waste)?|Bottle Trap|Toilet(?:\s+Roll Holder)?|Shower\b|Bath\b|Floor Waste\b|Mirror\b)",
        text,
    ):
        return True
    if prefix in {"Vanity Inset Basin", "Basin"} and re.match(
        r"(?i)^(?:Basin Waste|Bottle Trap|Toilet(?:\s+Roll Holder)?|Sink(?:\s+Mixer|\s+Waste)?|Pull-?Out Mixer|Vanity Tap Style|Tap Type|Tap Style|Mixer|Shower\b|Bath\b|Floor Waste\b|Mirror\b)",
        text,
    ):
        return True
    if prefix in {"Vanity Tap Style", "Tap Type", "Tap Style", "Sink Mixer", "Pull-Out Mixer", "Mixer"} and re.match(
        r"(?i)^(?:Basin Waste|Bottle Trap|Toilet(?:\s+Roll Holder)?|Shower\b|Bath\b|Floor Waste\b|Mirror\b|Sink Waste\b)",
        text,
    ):
        return True
    if prefix in {"Overhead Cupboards", "Upper Cabinetry Colour + Tall Cabinets", "Upper Cabinetry Colour"} and re.match(
        r"(?i)^(?:\*?\s*to builders\b|bulkhead above\b)",
        text,
    ):
        return False
    if prefix in {"Toe Kick", "Kickboard", "Island Bench Kickboard"} and re.match(r"(?i)^Pantry\b.*\bshelves\b", text):
        return True
    if prefix in {"Bulkheads", "Bulkhead", "Overhead Cupboards"} and re.match(r"(?i)^Island Bench(?:\b| )", text):
        return True
    if prefix in {"Handles", "Handle", "Base Cabinet Handles", "Overhead Handles"} and re.match(r"(?i)^Pantry Door Handles?\b", text):
        return True
    if not _looks_like_field_label(text):
        return False
    if prefix in {"Tap Type", "Tap Style", "Vanity Tap Style"} and re.match(r"^(?:Sink Mixer|Pull-Out Mixer|Basin Mixer)\b", text, re.IGNORECASE):
        return False
    if prefix in {"Vanity Inset Basin", "Basin"} and re.match(r"^Basin\b(?!\s*Mixer\b)", text, re.IGNORECASE):
        return False
    return True


def _field_prefix_match(line: str, prefix: str) -> re.Match[str] | None:
    if prefix == "Sink":
        pattern = rf"^{re.escape(prefix)}(?!\s*(?:Mixer|Waste)\b)(?:\s*\d+)?\b"
    elif prefix == "Basin":
        pattern = rf"^{re.escape(prefix)}(?!\s*(?:Waste|Mixer)\b)(?:\s*\d+)?\b"
    else:
        pattern = rf"^{re.escape(prefix)}(?:\s*\d+)?\b"
    return re.match(pattern, line, re.IGNORECASE)


def _looks_like_joinery_schedule_page(text: str) -> bool:
    lowered = normalize_space(text).lower()
    return any(hint in lowered for hint in JOINERY_PAGE_HINTS)


def _inject_schedule_heading_breaks(text: str) -> str:
    normalized = re.sub(
        rf"(?i)({ROOM_SCHEDULE_PATTERN})\s+(?:joinery\s+)?colour\s+schedule(?=\s|[A-Z]|$)",
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
    merge_across_pages: bool = True,
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
        "merge_across_pages": merge_across_pages,
    }


def _room_label_specificity_score(label: str) -> tuple[int, int]:
    text = normalize_space(label)
    lowered = text.lower()
    score = 0
    if re.search(r"\bbed\s*\d+\b", lowered):
        score += 100
    if "vanity" in lowered:
        score += 40
    if "walk in robe" in lowered:
        score += 35
    elif re.search(r"\brobe\b", lowered):
        score += 25
    if "theatre room" in lowered:
        score += 20
    if "bathroom" in lowered or "ensuite" in lowered:
        score += 15
    if "fit out" in lowered:
        score -= 5
    return score, len(text)


def _prefer_more_specific_room_label(existing_label: str, new_label: str) -> str:
    existing = normalize_space(existing_label)
    new = normalize_space(new_label)
    if not existing:
        return new
    if not new:
        return existing
    if _room_label_specificity_score(new) >= _room_label_specificity_score(existing):
        return new
    return existing


def _append_section(sections: list[dict[str, Any]], section: dict[str, Any]) -> None:
    for existing in sections:
        if (
            existing.get("section_key") == section.get("section_key")
            and existing.get("section_kind") == section.get("section_kind")
            and existing.get("file_name") == section.get("file_name")
            and existing.get("page_type") == section.get("page_type")
            and bool(existing.get("merge_across_pages", True))
            and bool(section.get("merge_across_pages", True))
        ):
            existing["page_nos"] = _unique([*existing.get("page_nos", []), *section.get("page_nos", [])])
            existing.setdefault("page_texts", []).extend(section.get("page_texts", []))
            existing.setdefault("raw_page_texts", []).extend(section.get("raw_page_texts", []))
            existing.setdefault("layout_rows", []).extend(section.get("layout_rows", []))
            existing.setdefault("text_parts", []).extend(section.get("text_parts", []))
            existing["text"] = normalize_space("\n".join(existing.get("text_parts", [])))
            if section.get("original_section_label"):
                existing["original_section_label"] = _prefer_more_specific_room_label(
                    str(existing.get("original_section_label", "") or ""),
                    str(section.get("original_section_label", "") or ""),
                )
            return
    sections.append(section)


def _yellowwood_page_room_hint(raw_page_text: str) -> str:
    for raw_line in str(raw_page_text or "").splitlines():
        line = normalize_space(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith(("lot ", "page ", "tone interior")):
            continue
        if not re.fullmatch(r"[A-Z0-9 &'()/.-]{2,}", line):
            continue
        explicit_heading = _extract_embedded_schedule_room_heading(line) or _extract_specific_room_heading(line)
        if explicit_heading and not _looks_like_structured_room_noise(explicit_heading):
            return explicit_heading
        prefix_heading = _extract_room_prefix_label(line)
        if prefix_heading:
            candidate = _clean_layout_room_label(prefix_heading, prefix_heading)
            if candidate and not _looks_like_structured_room_noise(candidate):
                return candidate
        candidate = source_room_label(line)
        if not candidate or candidate == "Room":
            continue
        if _looks_like_structured_room_noise(candidate):
            continue
        if re.search(r"(?i)\b(?:excluding|x\d+\b|shelves?|melamine|cupboards?|drawers?|doors?|handles?)\b", line):
            continue
        if len(candidate.split()) > 4:
            continue
        return candidate
    return ""


def _yellowwood_should_inherit_previous_room(label: str) -> bool:
    lowered = normalize_space(label).lower()
    if not lowered:
        return True
    if lowered == "wip":
        return True
    return any(
        token in lowered
        for token in (
            "x4 shelves",
            "excluding pantry",
            "white melamine",
            "melamine",
            "shelves",
            "cupboards",
            "drawers",
            "pantry door",
            "handless lip pull",
            "matt black",
        )
    )


def _normalize_yellowwood_layout_room_label(candidate: str, raw_page_text: str, previous_room_label: str) -> str:
    cleaned = _clean_layout_room_label(candidate, candidate)
    page_hint = _yellowwood_page_room_hint(raw_page_text)
    if _yellowwood_should_inherit_previous_room(candidate or cleaned):
        if page_hint:
            return page_hint
        if previous_room_label:
            return previous_room_label
        return ""
    if cleaned:
        return cleaned
    if page_hint:
        return page_hint
    return normalize_space(previous_room_label)


def _yellowwood_room_heading_label(line: str) -> str:
    text = normalize_space(line)
    if not text or _skip_continuation_line(text):
        return ""
    lowered = text.lower()
    if lowered.startswith(("lot ", "page ", "tone interior")):
        return ""
    if re.search(r"(?i)\b(?:yellowwood supplier|as supplied by cabinetmaker|handle house|national tiles)\b", text):
        return ""
    prefix_heading = _extract_room_prefix_label(text)
    if prefix_heading:
        candidate = _clean_layout_room_label(prefix_heading, prefix_heading)
        if (
            candidate
            and _looks_like_plausible_room_label(candidate)
            and not _looks_like_structured_room_noise(candidate)
            and not re.search(r"(?i)\b(?:excluding|x\d+\b|shelves?|melamine|cupboards?|drawers?|doors?|handles?)\b", text)
        ):
            return candidate
    if not re.fullmatch(r"[A-Z0-9 &'()/.-]{2,}", text):
        return ""
    candidate = _extract_embedded_schedule_room_heading(text) or _extract_specific_room_heading(text) or source_room_label(text)
    if not candidate or candidate == "Room":
        return ""
    candidate = _clean_layout_room_label(candidate, candidate) or candidate
    if not _yellowwood_is_supported_room_label(candidate):
        return ""
    if not _looks_like_plausible_room_label(candidate) or _looks_like_structured_room_noise(candidate):
        return ""
    if re.search(r"(?i)\b(?:excluding|x\d+\b|shelves?|melamine|cupboards?|drawers?|doors?|handles?)\b", text):
        return ""
    return candidate


def _yellowwood_is_supported_room_label(label: str) -> bool:
    cleaned = _clean_layout_room_label(label, label) or normalize_space(label)
    room_key = source_room_key(cleaned)
    if room_key in {
        "kitchen",
        "pantry",
        "butlers_pantry",
        "walk_in_pantry",
        "laundry",
        "wc",
        "powder",
        "ground_floor_powder_room",
        "upper_level_powder_room",
        "bathroom",
        "main_bathroom",
        "theatre",
        "media_room",
        "study",
        "office",
        "robe",
        "wir",
        "rumpus",
        "master_ensuite",
        "ensuite",
    }:
        return True
    if room_key.startswith(("ensuite_", "powder_room_", "bed_")):
        return True
    return bool(re.fullmatch(r"(?:bed_\d+_(?:wir|robe)|robe_fit_out(?:_to_bed_\d+)?)", room_key))


def _yellowwood_has_material_evidence(text: str) -> bool:
    normalized = normalize_space(text)
    if not normalized:
        return False
    return bool(
        re.search(
            r"(?i)\b(?:polytec|laminex|ydl|caesarstone|smartstone|wk stone|woodmatt|thermolaminate|thermolaminated|"
            r"melamine|laminate|board colour|standard white|classic white|natural finish|yellowwood supplier|"
            r"as supplied by cabinetmaker|floor mounted vanity|back benchtops?|benchtop|"
            r"base cupboards?\s*&\s*drawers?|overhead cupboards?|kickboard|floating shelves?)\b",
            normalized,
        )
    )


def _yellowwood_has_room_content_evidence(text: str) -> bool:
    normalized = normalize_space(text)
    if not normalized:
        return False
    if _yellowwood_has_material_evidence(normalized):
        return True
    return bool(
        re.search(
            r"(?i)\b(?:benchtop|cupboards?|drawers?|handles?|vanity|mirror|screen|tile|tiles|skirting|sink|basin|tap|"
            r"shower|splashback|cabinet(?:ry)?|robe fit out|linen cupboard|bar back|island)\b",
            normalized,
        )
    )


def _yellowwood_looks_like_contents_noise(text: str) -> bool:
    normalized = normalize_space(text)
    if not normalized:
        return False
    lowered = normalized.lower()
    page_range_count = len(re.findall(r"\b\d+\s*-\s*\d+\b", normalized))
    contents_markers = sum(
        1
        for marker in (
            "joinery - refer to cabinetry plans",
            "tiling schedule",
            "external painting refe",
            "bathware & fixtures",
            "appliances",
            "other than tiling to wet areas",
        )
        if marker in lowered
    )
    if page_range_count >= 3 and contents_markers >= 2:
        return True
    if "page 2/" in lowered and contents_markers >= 2:
        return True
    return False


def _yellowwood_is_material_driven_room_label(label: str) -> bool:
    lowered = normalize_space(label).lower()
    if not lowered:
        return False
    return any(
        token in lowered
        for token in (
            "robe",
            "wir",
            "walk in robe",
            "media room",
            "theatre",
            "study",
            "office",
            "rumpus",
        )
    )


def _yellowwood_should_keep_section(section: dict[str, Any]) -> bool:
    original_label = source_room_label(
        str(section.get("original_section_label", "")),
        fallback_key=str(section.get("section_key", "")),
    )
    original_label = _clean_layout_room_label(original_label, original_label) or original_label
    if not original_label or original_label == "Room":
        return False
    if _looks_like_spec_room_label_noise(original_label):
        return False
    if not _yellowwood_is_supported_room_label(original_label):
        return False
    if not _looks_like_plausible_room_label(original_label):
        return False
    text = normalize_space(str(section.get("text", "") or ""))
    if not text:
        return False
    if _yellowwood_looks_like_contents_noise(text):
        return False
    if _yellowwood_is_material_driven_room_label(original_label):
        return _yellowwood_has_material_evidence(text)
    return _yellowwood_has_room_content_evidence(text)


def _yellowwood_is_ignored_fit_out_heading(line: str) -> bool:
    text = normalize_space(line)
    if not text:
        return False
    return bool(
        re.match(
            r"(?i)^(?:laundry|passage)\s+linen\s+fit\s+out\b",
            text,
        )
    )


def _yellowwood_looks_like_fixture_schedule_page(page_lines: list[str], raw_page_text: str) -> bool:
    if not page_lines:
        return False
    heading_count = sum(1 for line in page_lines if _yellowwood_room_heading_label(line))
    if heading_count < 1:
        return False
    combined = normalize_space(raw_page_text)
    return bool(
        re.search(
            r"(?i)\b(?:sink|basin|mixer|sink waste|basin waste|plumbing|shower|bath|toilet|tap|feature waste|floor waste)\b",
            combined,
        )
    )


def _collect_yellowwood_text_room_sections_for_document(document: dict[str, object]) -> list[dict[str, Any]]:
    raw_document = _clone_document_for_raw_room_detection(document)
    file_name = str(raw_document.get("file_name", "") or "")
    sections: list[dict[str, Any]] = []
    previous_page_room_label = ""
    previous_page_room_key = ""
    for page in raw_document.get("pages", []):
        if not isinstance(page, dict):
            continue
        raw_page_text = str(page.get("text") or "")
        if not raw_page_text.strip():
            continue
        upper = raw_page_text.upper()
        page_lines = _preprocess_chunk(raw_page_text)
        if not (
            _looks_like_joinery_schedule_page(raw_page_text)
            or "TILING SCHEDULE" in upper
            or "INTERNAL FINISHES" in upper
            or _yellowwood_looks_like_fixture_schedule_page(page_lines, raw_page_text)
            or sum(1 for line in page_lines if _yellowwood_room_heading_label(line)) >= 2
        ):
            continue
        page_no = int(page.get("page_no", 0) or 0)
        current_label = ""
        current_key = ""
        current_lines: list[str] = []
        def _yellowwood_heading_at(index: int) -> tuple[str, int, str]:
            line = normalize_space(page_lines[index]) if index < len(page_lines) else ""
            if not line:
                return "", 1, ""
            candidates: list[tuple[str, int]] = [(line, 1)]
            combined = line
            for offset in range(1, 3):
                if index + offset >= len(page_lines):
                    break
                next_line = normalize_space(page_lines[index + offset])
                if not next_line:
                    break
                combined = normalize_space(f"{combined} {next_line}")
                candidates.append((combined, offset + 1))
            best_label = ""
            best_text = line
            best_consumed = 1
            for candidate, consumed in candidates:
                heading_label = _yellowwood_room_heading_label(candidate)
                if not heading_label:
                    continue
                if len(heading_label) >= len(best_label):
                    best_label = heading_label
                    best_text = candidate
                    best_consumed = consumed
            return best_label, best_consumed, best_text

        headings_in_page = [label for index in range(len(page_lines)) if (label := _yellowwood_heading_at(index)[0])]
        first_heading_index = next(
            (index for index in range(len(page_lines)) if _yellowwood_heading_at(index)[0]),
            len(page_lines),
        )
        inherited_page_room = bool(
            previous_page_room_key
            and not headings_in_page
            and _looks_like_joinery_schedule_page(raw_page_text)
        )
        if inherited_page_room:
            current_label = previous_page_room_label
            current_key = previous_page_room_key
            current_lines = [previous_page_room_label]
        elif previous_page_room_key and first_heading_index > 0 and _looks_like_joinery_schedule_page(raw_page_text):
            current_label = previous_page_room_label
            current_key = previous_page_room_key
            current_lines = [previous_page_room_label]
        page_last_room_label = previous_page_room_label
        page_last_room_key = previous_page_room_key

        def flush() -> None:
            nonlocal current_label, current_key, current_lines
            nonlocal page_last_room_label, page_last_room_key
            if current_key and current_lines:
                chunk = normalize_space("\n".join(current_lines))
                sections.append(
                    {
                        "section_key": current_key,
                        "original_section_label": current_label,
                        "section_kind": "room",
                        "file_name": file_name,
                        "page_nos": [page_no] if page_no else [],
                        "page_texts": [{"page_no": page_no, "text": chunk}] if page_no else [],
                        "text_parts": [chunk],
                        "text": chunk,
                        "page_type": "joinery",
                        "merge_across_pages": False,
                    }
                )
                page_last_room_label = current_label
                page_last_room_key = current_key
            current_label = ""
            current_key = ""
            current_lines = []

        index = 0
        while index < len(page_lines):
            line = page_lines[index]
            if _yellowwood_is_ignored_fit_out_heading(line):
                flush()
                index += 1
                continue
            heading_label, consumed, heading_text = _yellowwood_heading_at(index)
            if heading_label:
                flush()
                current_label = _clean_layout_room_label(heading_label, heading_label) or heading_label
                current_key = source_room_key(current_label)
                current_lines = [heading_text or line]
                index += consumed
                continue
            if current_key:
                current_lines.append(line)
            index += 1
        flush()
        previous_page_room_label = page_last_room_label
        previous_page_room_key = page_last_room_key
    return sections


def _yellowwood_trim_section_override_pages(section: dict[str, Any], override_pages: set[int]) -> dict[str, Any] | None:
    page_nos = [int(page_no or 0) for page_no in section.get("page_nos", [])]
    if not page_nos or not set(page_nos).intersection(override_pages):
        return dict(section)
    kept_page_nos = [page_no for page_no in page_nos if page_no not in override_pages]
    if not kept_page_nos:
        return None
    trimmed = dict(section)
    trimmed["page_nos"] = kept_page_nos
    kept_page_texts = [
        dict(page_entry)
        for page_entry in section.get("page_texts", [])
        if isinstance(page_entry, dict) and int(page_entry.get("page_no", 0) or 0) not in override_pages
    ]
    kept_raw_page_texts = [
        dict(page_entry)
        for page_entry in section.get("raw_page_texts", [])
        if isinstance(page_entry, dict) and int(page_entry.get("page_no", 0) or 0) not in override_pages
    ]
    kept_layout_rows = [
        dict(row)
        for row in section.get("layout_rows", [])
        if isinstance(row, dict) and int(row.get("page_no", 0) or 0) not in override_pages
    ]
    trimmed["page_texts"] = kept_page_texts
    trimmed["raw_page_texts"] = kept_raw_page_texts
    trimmed["layout_rows"] = kept_layout_rows
    kept_text_parts = [str(page_entry.get("text", "") or "") for page_entry in kept_page_texts if str(page_entry.get("text", "") or "").strip()]
    trimmed["text_parts"] = kept_text_parts
    trimmed["text"] = normalize_space("\n".join(kept_text_parts))
    if not trimmed["text"] and not kept_layout_rows:
        return None
    return trimmed


def _merge_yellowwood_layout_and_text_sections(
    layout_sections: list[dict[str, Any]],
    text_sections: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    layout_sections = [section for section in layout_sections if _yellowwood_should_keep_section(section)]
    text_sections = [section for section in text_sections if _yellowwood_should_keep_section(section)]
    override_pages = {
        int(section.get("page_nos", [0])[0] or 0)
        for section in text_sections
        if section.get("section_kind") == "room" and len(section.get("page_nos", [])) == 1
    }
    text_page_counts: dict[int, int] = {}
    for section in text_sections:
        if section.get("section_kind") != "room" or len(section.get("page_nos", [])) != 1:
            continue
        page_no = int(section.get("page_nos", [0])[0] or 0)
        text_page_counts[page_no] = text_page_counts.get(page_no, 0) + 1
    override_pages = {page_no for page_no, count in text_page_counts.items() if page_no and count >= 2}
    merged: list[dict[str, Any]] = []
    for section in layout_sections:
        current = dict(section)
        if section.get("section_kind") == "room" and override_pages:
            current = _yellowwood_trim_section_override_pages(section, override_pages) or {}
        if current:
            _append_section(merged, current)
    for section in text_sections:
        _append_section(merged, section)
    return merged


def _collect_layout_sections_for_document(document: dict[str, object]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    file_name = str(document.get("file_name", "") or "")
    builder_name = str(document.get("builder_name", "") or "")
    previous_room_label = ""
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
            elif _is_yellowwood_builder(builder_name):
                block_label = _normalize_yellowwood_layout_room_label(block_label or room_label or section_label, raw_page_text, previous_room_label)
            block_rows = [row for row in block.get("rows", []) if isinstance(row, dict)]
            section = _layout_section_seed(
                file_name=file_name,
                page_no=page_no,
                section_label=section_label,
                room_label=block_label if _is_yellowwood_builder(builder_name) else (block_label or room_label),
                rows=block_rows,
                page_type=page_type,
                raw_page_text=raw_page_text,
                merge_across_pages=not _is_yellowwood_builder(builder_name),
            )
            if section:
                _append_section(sections, section)
                if _is_yellowwood_builder(builder_name) and section.get("section_kind") == "room":
                    previous_room_label = normalize_space(str(section.get("original_section_label", "") or previous_room_label))
    return sections


def _clone_document_for_raw_room_detection(document: dict[str, object]) -> dict[str, object]:
    cloned_pages: list[dict[str, object]] = []
    for page in document.get("pages", []):
        if not isinstance(page, dict):
            continue
        cloned = dict(page)
        preferred_text = str(page.get("raw_text") or page.get("text") or "")
        cloned["text"] = preferred_text
        cloned_pages.append(cloned)
    return {**document, "pages": cloned_pages}


def _collect_text_room_sections_for_document(document: dict[str, object]) -> list[dict[str, Any]]:
    raw_document = _clone_document_for_raw_room_detection(document)
    full_text = _document_full_text(raw_document)
    text_sections = _collect_schedule_room_sections([raw_document]) or _find_room_sections(full_text)
    sections: list[dict[str, Any]] = []
    file_name = str(raw_document.get("file_name", "") or "")
    pages = list(raw_document.get("pages", []))
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


def _collect_room_sections_for_document(document: dict[str, object], builder_name_override: str = "") -> list[dict[str, Any]]:
    builder_name = normalize_space(builder_name_override or str(document.get("builder_name", "") or "")).lower()
    if _is_yellowwood_builder(builder_name):
        layout_sections = _collect_layout_sections_for_document({**document, "builder_name": "Yellowwood"})
        text_sections = _collect_yellowwood_text_room_sections_for_document(document)
        if text_sections:
            layout_sections = _merge_yellowwood_layout_and_text_sections(layout_sections, text_sections)
        else:
            layout_sections = [section for section in layout_sections if _yellowwood_should_keep_section(section)]
        if layout_sections:
            return [section for section in layout_sections if section.get("section_kind") == "room"]
        return [section for section in text_sections if section.get("section_kind") == "room"]
    layout_sections = _collect_layout_sections_for_document(document)
    if layout_sections:
        return [section for section in layout_sections if section.get("section_kind") == "room"]
    return _collect_text_room_sections_for_document(document)


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


def _document_full_raw_text(document: dict[str, object]) -> str:
    return "\n\n".join(
        str(page.get("raw_text") or page.get("text") or "")
        for page in document.get("pages", [])
        if page.get("raw_text") or page.get("text")
    )


def _document_room_master_score(document: dict[str, object]) -> dict[str, Any]:
    pages = list(document.get("pages", []))
    full_text = _document_full_text(document)
    file_name = normalize_space(str(document.get("file_name", ""))).lower()
    builder_name = str(document.get("builder_name", "") or "").strip().lower()
    if builder_name == "clarendon":
        schedule_sections = [
            (str(section.get("section_key", "")), str(section.get("text", "")))
            for section in _collect_text_room_sections_for_document(document)
            if section.get("section_kind") == "room"
        ]
    elif _document_has_layout_schema(document):
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
        "schedule_pages": schedule_pages,
        "colour_schedule_hits": colour_schedule_hits,
        "cabinetry_field_hits": cabinetry_field_hits,
        "room_heading_hits": room_heading_hits,
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


def _looks_like_spec_room_label_noise(label: str) -> bool:
    text = normalize_space(label)
    lowered = text.lower()
    if not text:
        return True
    if lowered in {"location", "manufacturer", "range", "model", "profile", "colour", "color", "type"}:
        return True
    if lowered.startswith("additional "):
        return True
    if any(
        token in lowered
        for token in (
            "phone",
            "fax",
            "abn",
            "job no",
            "sheet ",
            "scale:",
            "forstan pty ltd",
            "customer service coordinator",
            "product:",
            "client signature",
            "date of signed dwgs",
            "frame wall to ctr",
        )
    ):
        return True
    if re.search(r"\d{7,}", text):
        return True
    digit_count = sum(char.isdigit() for char in text)
    if digit_count >= 8 and len(text) >= 24:
        return True
    return False


def _clarendon_supplement_fixture_section_is_standalone_candidate(section_text: str) -> bool:
    lowered = normalize_space(section_text).lower()
    if not lowered:
        return False
    return any(
        token in lowered
        for token in (
            "vanity inset basin",
            "vanity basin",
            "vanity tap style",
            "basin mixer",
            "basin tap",
            "bath combination tap",
            "bath mixer",
            "bath type",
            "shower tap style",
            "shower outlet style",
            "shower type",
            "toilet suite",
            "toilet roll holder",
            "mirror:",
            "vanity waste colour",
        )
    )


def _is_imperial_builder(builder_name: str) -> bool:
    return "imperial" in normalize_space(builder_name).lower()


def _is_yellowwood_builder(builder_name: str) -> bool:
    return "yellowwood" in normalize_space(builder_name).lower()


def _is_blacklisted_wet_area_label(text: Any) -> bool:
    normalized = normalize_space(str(text or "")).lower()
    if not normalized:
        return False
    normalized = re.sub(r"[_|]+", " ", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized).strip()
    if not normalized:
        return False
    return any(re.search(pattern, normalized, re.IGNORECASE) for pattern in WET_AREA_PLUMBING_BLACKLIST_PATTERNS)


def _filter_blacklisted_room_accessories(values: Any) -> list[str]:
    filtered: list[str] = []
    for value in _coerce_string_list(values):
        cleaned = normalize_space(value)
        if not cleaned or _is_blacklisted_wet_area_label(cleaned):
            continue
        filtered.append(cleaned)
    return filtered


def _filter_blacklisted_room_other_items(items: Any) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    for item in _merge_other_items([], items):
        label = normalize_space(str(item.get("label", "") or ""))
        value = normalize_space(str(item.get("value", "") or ""))
        if not label or not value:
            continue
        if re.fullmatch(r"(?i)(?:not applicable|not included|n/?a|na)", value):
            continue
        if _is_blacklisted_wet_area_label(label) or _is_blacklisted_wet_area_label(value) or _is_blacklisted_wet_area_label(f"{label} {value}"):
            continue
        filtered.append({"label": label, "value": value})
    return filtered


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
    for document in ordered_documents:
        document_path = Path(str(document.get("path", "") or ""))
        if not document_path.exists() or document_path.suffix.lower() != ".pdf":
            continue
        try:
            with pdfplumber.open(str(document_path)) as pdf:
                for page in pdf.pages[:3]:
                    address = _extract_site_address_from_text(page.extract_text() or "")
                    if address:
                        return address
        except Exception:
            pass
        try:
            reader = PdfReader(str(document_path))
        except Exception:
            continue
        for page in reader.pages[:3]:
            try:
                address = _extract_site_address_from_text(page.extract_text() or "")
            except Exception:
                address = ""
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
    return bool(
        normalized
        and (
            re.search(r"(?i)\b(?:tiled|tiles|tile|hybrid|carpet|timber|vinyl|laminate|engineered|stone)\b", normalized)
            or re.search(r"(?i)\bN\s*/?\s*A\b", normalized)
        )
    )


def _normalize_imperial_soft_close_floor_pair(soft: str, floor: str) -> tuple[str, str]:
    soft_value = normalize_space(soft)
    floor_value = normalize_space(floor)
    if not soft_value and not floor_value:
        return "", ""
    soft_is_soft_close = _looks_like_soft_close_candidate(soft_value)
    floor_is_soft_close = _looks_like_soft_close_candidate(floor_value)
    soft_is_flooring = _looks_like_flooring_candidate(soft_value)
    floor_is_flooring = _looks_like_flooring_candidate(floor_value)
    if (not soft_is_soft_close and soft_is_flooring) and floor_is_soft_close:
        return floor_value, soft_value
    if soft_is_soft_close and (not floor_is_flooring and floor_value) and soft_is_flooring:
        return floor_value, soft_value
    return soft_value, floor_value


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
            soft, floor = _normalize_imperial_soft_close_floor_pair(match.group("soft"), match.group("floor"))
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
        left, right = _normalize_imperial_soft_close_floor_pair(combined_match.group("left"), combined_match.group("right"))
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
        "shelf": "",
        "led": "",
        "led_note": "",
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
        if "SHELV" in label_upper and "FLOATING" not in label_upper:
            material = _extract_explicit_shelf_material_from_text(f"{label} {_imperial_layout_row_material_text(row)}")
            overlay["shelf"] = _merge_text(overlay["shelf"], material)
            if material:
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
            overlay["led_note"] = _merge_led_note(overlay.get("led_note", ""), _extract_led_note_from_layout_row(row))
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
    row.shelf = _extract_explicit_shelf_material_from_text(section_text)
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
        "shelf",
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
    row.led_note = _extract_led_note_from_lines(section_text.splitlines())
    row.led = _normalize_led_value(fields.get("led", ""), row.led_note)
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
    if layout_overlay.get("shelf"):
        row.shelf = _merge_text(row.shelf, layout_overlay["shelf"])
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
    row.led_note = _merge_led_note(row.led_note, layout_overlay.get("led_note", ""))
    row.led = _normalize_led_value(layout_overlay.get("led") or row.led, row.led_note)
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
        raw_full_text = _document_full_raw_text(document)
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
        appliances.extend(_extract_appliances_from_pages(file_name, pages))

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


def _clarendon_is_drawings_and_colours_file(document: dict[str, object]) -> bool:
    file_name = normalize_space(str(document.get("file_name", "") or ""))
    simplified = re.sub(r"[_-]+", " ", file_name)
    return bool(re.search(r"(?i)\bdrawings?\s+and\s+colours?\b", simplified))


def _select_spec_room_master_document(builder_name: str, documents: list[dict[str, object]]) -> tuple[dict[str, object] | None, str]:
    if _is_imperial_builder(builder_name):
        return _select_imperial_room_master_document(documents)
    if builder_name.strip().lower() == "clarendon" and len(documents) > 1:
        explicit_master = next((document for document in documents if _clarendon_is_drawings_and_colours_file(document)), None)
        if explicit_master is not None:
            return explicit_master, f"{explicit_master['file_name']} selected as room master by Clarendon Drawings and Colours filename match."
        best_document: dict[str, object] | None = None
        best_reason = ""
        best_score = -1
        for document in documents:
            metrics = _document_room_master_score(document)
            if metrics["schedule_pages"] <= 0 and metrics["cabinetry_field_hits"] <= 0:
                continue
            if metrics["score"] > best_score:
                best_document = document
                best_score = int(metrics["score"])
                best_reason = (
                    f"{document['file_name']} selected as room master by Clarendon schedule density "
                    f"({metrics['reason']})."
                )
        if best_document is not None:
            return best_document, best_reason
    return select_room_master_document(documents, "spec")


def _collect_spec_sections_for_document(builder_name: str, document: dict[str, object]) -> list[dict[str, Any]]:
    normalized_builder = builder_name.strip().lower()
    if _is_imperial_builder(builder_name):
        imperial_sections = _collect_imperial_sections_for_document(document)
        if imperial_sections:
            return imperial_sections
    if normalized_builder == "clarendon":
        return _collect_text_room_sections_for_document({**document, "pages": [dict(page) for page in list(document.get("pages", []))]})
    if _document_has_layout_schema(document):
        layout_sections = _collect_layout_sections_for_document({**document, "builder_name": builder_name})
        if _is_yellowwood_builder(builder_name):
            text_sections = _collect_yellowwood_text_room_sections_for_document(document)
            if text_sections:
                layout_sections = _merge_yellowwood_layout_and_text_sections(layout_sections, text_sections)
            else:
                layout_sections = [section for section in layout_sections if _yellowwood_should_keep_section(section)]
            if layout_sections:
                return layout_sections
            return text_sections
        if layout_sections:
            return layout_sections
    return _collect_room_sections_for_document(document, builder_name_override=builder_name)


def _parse_spec_documents_structure_first(
    job_no: str,
    builder_name: str,
    documents: list[dict[str, object]],
    rule_flags: Any = None,
) -> dict[str, Any]:
    imperial_builder = _is_imperial_builder(builder_name)
    normalized_builder = normalize_space(builder_name).lower()
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
            if not imperial_builder and _looks_like_spec_room_label_noise(original_room_label):
                continue
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
        raw_full_text = _document_full_raw_text(document)
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
            if not imperial_builder and _looks_like_spec_room_label_noise(original_room_label):
                ignored_room_like_lines_count += 1
                warnings.append(f"Ignored room-like section '{original_room_label}' from {file_name}: room-label metadata noise.")
                continue
            chunk = str(section.get("text", "") or "")
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

            lines = _preprocess_chunk(chunk) if _is_yellowwood_builder(builder_name) else _section_lines(section)
            row = rooms.get(target_room_key) or RoomRow(
                room_key=target_room_key,
                original_room_label=original_room_label,
                source_file=file_name,
            )
            if is_room_master:
                row.original_room_label = (
                    _prefer_more_specific_room_label(row.original_room_label, original_room_label)
                    if _is_yellowwood_builder(builder_name)
                    else original_room_label
                )
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

        appliances.extend(_extract_appliances_from_pages(file_name, pages))
        flooring_text = _extract_global_value(full_text, "flooring")
        splashback_text = _extract_global_value(full_text, "splashback")
        if flooring_text:
            flooring_notes.append(flooring_text)
        if splashback_text:
            splashback_notes.append(splashback_text)

    if not imperial_builder:
        filtered_rooms: dict[str, RoomRow] = {}
        for room_key, row in rooms.items():
            room_label = normalize_space(row.original_room_label or room_key)
            if _looks_like_spec_room_label_noise(room_label):
                ignored_room_like_lines_count += 1
                warnings.append(f"Ignored room-like section '{room_label[:80]}' from final snapshot: room-label metadata noise.")
                continue
            row.room_name = normalize_space(row.original_room_label or room_key.replace("_", " "))
            filtered_rooms[room_key] = row
        rooms = filtered_rooms
    else:
        for room_key, row in rooms.items():
            row.room_name = normalize_space(row.original_room_label or room_key.replace("_", " "))

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
        raw_full_text = _document_full_raw_text(document)
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
                row.original_room_label = (
                    _prefer_more_specific_room_label(row.original_room_label, original_room_label)
                    if _is_yellowwood_builder(builder_name)
                    else original_room_label
                )
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
        appliances.extend(_extract_appliances_from_pages(file_name, pages))
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
        island_bench_top = _first_value(_collect_island_benchtop_values(lines))
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
        row.bulkheads = _merge_lists(row.bulkheads, _collect_explicit_bulkhead_values(lines))
        row.handles = _merge_lists(
            row.handles,
            _clean_handle_entries(_collect_field(lines, ["Handles", "Handle", "Base Cabinet Handles", "Overhead Handles", "Pantry Door Handles"])),
        )
        row.floating_shelf = _merge_text(row.floating_shelf, _first_value(_collect_field(lines, ["Floating Shelves", "Floating Shelf"])))
        row.shelf = _merge_text(row.shelf, _extract_explicit_shelf_material_from_text("\n".join(lines)))
        led_note = _extract_led_note_from_lines(lines)
        row.led_note = _merge_led_note(row.led_note, led_note)
        row.led = _normalize_led_value(row.led, row.led_note)
        row.accessories = _merge_lists(row.accessories, _collect_field(lines, ["Accessories", "Accessory"]))
        row.other_items = _merge_other_items(
            row.other_items,
            [
                {"label": "RAIL", "value": _first_value(_collect_field(lines, ["Rail"]))},
                {"label": "JEWELLERY INSERT", "value": _first_value(_collect_field(lines, ["Jewellery Insert"]))},
                {"label": "BATH", "value": _first_value(_collect_field(lines, ["Bath"]))},
                {"label": "BATH MIXER", "value": _first_value(_collect_field(lines, ["Bath Mixer"]))},
                {"label": "BATH SPOUT", "value": _first_value(_collect_field(lines, ["Bath Spout"]))},
                {"label": "BATH WASTE", "value": _first_value(_collect_field(lines, ["Bath Waste"]))},
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
                if re.fullmatch(r"(?i)\(to all lower doors? & drawers?.*", triple):
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
            match = _field_prefix_match(line, prefix)
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


def _collect_island_benchtop_values(lines: list[str]) -> list[str]:
    values = _collect_field(lines, ["Island Benchtop", "Island Bench Top", "Island Bench"])
    filtered: list[str] = []
    for value in values:
        text = normalize_space(value)
        if not text:
            continue
        if re.match(r"(?i)^(?:\+?\s*end panels?|base cupboards?\s*&\s*drawers?|kickboards?|bar back)\b", text):
            continue
        filtered.append(text)
    return _unique(filtered)


def _collect_explicit_bulkhead_values(lines: list[str]) -> list[str]:
    values: list[str] = []
    for value in _collect_field(lines, ["Bulkheads", "Bulkhead"]):
        text = normalize_space(value)
        if not text:
            continue
        if re.match(r"(?i)^(?:above\b|to builders\b)", text):
            continue
        values.append(text)
    return _unique(values)


def _is_led_noise_text(text: str) -> bool:
    return bool(re.search(r"(?i)\b(?:topmount|undermount(?:ed)?|undermoutned|installed?|instal)\b", normalize_space(text)))


def _clean_led_note_text(value: Any) -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    text = re.sub(r"(?i)\bLED\s*-\s*'S\b", "LED's", text)
    text = re.sub(r"(?i)\bLED'S\b", "LED's", text)
    text = re.sub(r"(?i)\bLED\s+LIGHTING\s+LED\s+STRIP\s+LIGHTING\b", "LED LIGHTING - LED Strip Lighting", text)
    text = re.sub(r"(?i)\bLED\s+STRIP\s+LIGHTING\s*-\s+to\b", "LED Strip Lighting to", text)
    text = re.sub(
        r"(?i)\b(LED(?:'?S)?(?:\s+\([^)]+\))?)\s+HANDLES\b.*$",
        lambda match: f"{match.group(1)} As per drawings" if "AS PER DRAWINGS" in text.upper() else match.group(1),
        text,
    )
    text = re.sub(r"(?i)\b(BASE-|UPPER\b|TALL\b|ACCESSORIES\b|BIN\b).*$", "", text).strip(" -|")
    if "LED LIGHTING" in text.upper() and "LED STRIP LIGHTING" in text.upper():
        text = re.sub(r"(?i)\bLED\s+STRIP\s+LIGHTING\s*-\s*", "LED Strip Lighting ", text)
    return normalize_space(text.strip(" -|"))


def _merge_led_note(*values: Any) -> str:
    notes: list[str] = []
    for value in values:
        raw_text = normalize_space(str(value or ""))
        if not raw_text:
            continue
        for fragment in [part.strip() for part in raw_text.split("|")]:
            text = _clean_led_note_text(fragment)
            if not text:
                continue
            if any(text.lower() == existing.lower() or text.lower() in existing.lower() for existing in notes):
                continue
            notes = [existing for existing in notes if existing.lower() not in text.lower()]
            notes.append(text)
    return " | ".join(notes)


def _extract_led_note_from_lines(lines: list[str]) -> str:
    notes: list[str] = []
    for index, line in enumerate(lines):
        text = normalize_space(line)
        match = re.match(r"(?i)^LED(?:'?S)?(?:\s+STRIP\s+LIGHTING|\s+LIGHTING)?\b", text)
        if not match:
            continue
        tail = normalize_space(text[match.end() :].strip(" :-"))
        if tail:
            if _is_led_noise_text(tail):
                continue
            note = text
            if "(" in text and ")" not in text and index + 1 < len(lines):
                continuation = normalize_space(lines[index + 1])
                if continuation and not _looks_like_field_label(continuation) and not _is_led_noise_text(continuation):
                    note = normalize_space(f"{note} {continuation}")
        else:
            note = normalize_space(text[: match.end()])
            if index + 1 < len(lines) and not _looks_like_field_label(lines[index + 1]):
                continuation = normalize_space(lines[index + 1])
                if continuation and not _is_led_noise_text(continuation):
                    note = normalize_space(f"{note} - {continuation}")
        note = _clean_led_note_text(note)
        if not note or _is_led_noise_text(note) or note in notes:
            continue
        notes.append(note)
    return " | ".join(notes)


def _extract_led_note_from_layout_row(row: dict[str, Any]) -> str:
    label = normalize_space(str(row.get("row_label", "") or ""))
    if not label or "LED" not in label.upper():
        return ""
    note = label
    extras: list[str] = []
    for key in ("value_region_text", "value_text", "notes_region_text", "notes_text"):
        text = normalize_space(str(row.get(key, "") or ""))
        if not text or text.lower() == label.lower() or _is_led_noise_text(text):
            continue
        extras.append(text)
    if extras:
        note = normalize_space(f"{label} - {' - '.join(_unique(extras))}")
    note = _clean_led_note_text(note)
    return "" if _is_led_noise_text(note) else note


def _normalize_led_value(value: Any, note: Any = "") -> str:
    note_text = normalize_space(str(note or ""))
    text = normalize_space(str(value or ""))
    if note_text:
        return "Yes"
    lowered = text.lower()
    if lowered in {"", "no", "n", "false", "0"}:
        return "No"
    if lowered in {"yes", "y", "true", "1"}:
        return "Yes"
    return "Yes"


def _has_explicit_led_field(lines: list[str]) -> bool:
    return bool(_extract_led_note_from_lines(lines))


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
    prepared_text = _preprocess_appliance_text(text)
    rows = _extract_labeled_appliances(prepared_text, file_name, pages)
    labeled_types = {row.appliance_type.lower() for row in rows}
    loose_rows = _extract_loose_appliances(prepared_text, file_name, pages)
    for row in loose_rows:
        if row.appliance_type.lower() in labeled_types:
            continue
        rows.append(row)
    return _dedupe_appliances(rows)


def _extract_appliances_from_pages(file_name: str, pages: list[dict[str, object]]) -> list[ApplianceRow]:
    rows: list[ApplianceRow] = []
    for page in pages:
        page_text = str(page.get("raw_text") or page.get("text") or "")
        if not page_text.strip():
            continue
        rows.extend(_extract_appliances(page_text, file_name, [page]))
    return _dedupe_appliances(rows)


def _preprocess_appliance_text(text: str) -> str:
    prepared = str(text or "")
    prepared = re.sub(
        r"(?<!^)(?<!\n)\s+(?=(?:RANGEHOOD|DISHWASHER|COOKTOP|OVEN|MICROWAVE|FRIDGE)(?:\s|\(|$))",
        "\n",
        prepared,
    )
    return prepared


def _extract_labeled_appliances(text: str, file_name: str, pages: list[dict[str, object]]) -> list[ApplianceRow]:
    matches = _collect_appliance_label_matches(text)
    rows: list[ApplianceRow] = []
    for index, match in enumerate(matches):
        next_start = matches[index + 1]["start"] if index + 1 < len(matches) else len(text)
        segment = normalize_space(text[match["start"]:next_start])
        label_text = normalize_space(text[match["start"]:match["end"]])
        details = _limit_appliance_details_to_local_context(
            text[match["end"]:next_start],
            appliance_type=str(match["appliance_type"]),
        )
        placeholder = _extract_appliance_placeholder_model(label_text)
        if placeholder:
            if (
                not details
                or "as above" in placeholder.lower()
                or "leave standard space" in placeholder.lower()
                or _details_look_like_other_appliance_context(details, appliance_type=str(match["appliance_type"]))
            ):
                details = label_text
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


def _details_look_like_other_appliance_context(details: str, *, appliance_type: str) -> bool:
    lowered = normalize_space(details).lower()
    if not lowered:
        return False
    other_tokens = {
        "dishwasher": "Dishwasher",
        "rangehood": "Rangehood",
        "microwave": "Microwave",
        "cooktop": "Cooktop",
        "fridge": "Fridge",
        "refrigerator": "Fridge",
        "oven": "Oven",
        "oven/stove": "Oven",
        "stove": "Oven",
    }
    for token, token_type in other_tokens.items():
        if token_type.lower() == appliance_type.lower():
            continue
        if token in lowered:
            return True
    return False


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
    lines = [normalize_space(raw_line) for raw_line in text.split("\n")]
    for index, line in enumerate(lines):
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
        details = _build_loose_appliance_details(lines, index, appliance_type)
        if not details:
            continue
        make = _guess_make(details)
        model_no = _guess_model(details)
        placeholder_model = _extract_appliance_placeholder_model(details)
        if not (make or model_no or placeholder_model):
            continue
        if not (make or _has_parenthesized_model(details) or model_no or placeholder_model):
            continue
        row = _build_appliance_row(
            appliance_type=appliance_type,
            details=details,
            evidence=details,
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
        if re.match(rf"^\s*(?:{re.escape(token)})\b", lowered_line):
            return appliance_type
        inline_match = re.search(rf"\b{re.escape(token)}\b", lowered_line)
        if not inline_match:
            continue
        prefix = normalize_space(lowered_line[: inline_match.start()])
        if ":" in prefix:
            continue
        guess_text = _strip_urls_for_appliance_guessing(lowered_line)
        if _guess_make(guess_text) or _guess_model(guess_text) or _extract_appliance_placeholder_model(guess_text):
            return appliance_type
    return ""


def _has_parenthesized_model(text: str) -> bool:
    return any(_valid_model_candidate(candidate.upper(), allow_numeric=True) for candidate in re.findall(r"\(([A-Za-z0-9/-]{3,})\)", text))


def _is_appliance_context_boundary_line(line: str, appliance_type: str = "") -> bool:
    text = normalize_space(line)
    if not text:
        return True
    lowered = text.lower()
    if ":" in text and not re.search(r"(?i)\bmodel\s*[:#-]?\s*", text):
        prefix = normalize_space(text.split(":", 1)[0]).lower()
        if prefix and prefix not in STRICT_APPLIANCE_FIELD_PREFIXES:
            return True
    if (
        _looks_like_strict_appliance_label(text)
        or _is_room_heading_line(text)
        or _is_schedule_room_heading(text)
        or _looks_like_field_label(text)
    ):
        return True
    if any(
        token in lowered
        for token in (
            "address:",
            "client:",
            "area / item",
            "specs / description",
            "appliances",
        )
    ):
        return True
    if lowered == "by client" or lowered.startswith("by client "):
        return True
    if any(token in lowered for token in ("designer:", "client name", "signature:", "signed date", "document ref:")):
        return True
    other_type = _match_loose_appliance_type(lowered)
    if other_type and other_type.lower() != appliance_type.lower():
        return True
    return False


def _build_loose_appliance_details(lines: list[str], index: int, appliance_type: str) -> str:
    if index < 0 or index >= len(lines):
        return ""
    current = normalize_space(lines[index])
    if not current:
        return ""
    lowered_current = current.lower()
    if any(token in lowered_current for token in ("leave standard space", "space by client", "provide space only")):
        return current
    starts_with_label = lowered_current.startswith(appliance_type.lower())
    collected: list[str] = []
    tail = ""
    if starts_with_label:
        tail = normalize_space(re.sub(rf"(?i)^\s*{re.escape(appliance_type)}\b", "", current)).strip(" -:")
        if tail and not re.fullmatch(r"\(\s*[^)]+\s*\)", tail):
            collected.append(tail)
    else:
        collected = [current]

    should_back_collect = not starts_with_label
    if starts_with_label and tail:
        placeholder = _extract_appliance_placeholder_model(tail)
        if placeholder and "as above" not in placeholder.lower() and "leave standard space" not in placeholder.lower():
            should_back_collect = not bool(_guess_make(_strip_urls_for_appliance_guessing(tail)))
        elif not _guess_make(_strip_urls_for_appliance_guessing(tail)):
            should_back_collect = True

    if should_back_collect:
        back = index - 1
        while back >= 0 and len(collected) < 4:
            candidate = normalize_space(lines[back])
            if not candidate:
                break
            if _is_appliance_context_boundary_line(candidate, appliance_type):
                break
            collected.insert(0, candidate)
            if _guess_make(" ".join(collected)):
                break
            back -= 1

    forward = index + 1
    skipped_preface = 0
    while forward < len(lines) and len(collected) < 6:
        candidate = normalize_space(lines[forward])
        if not candidate or _is_appliance_context_boundary_line(candidate, appliance_type):
            break
        if not collected and (_looks_like_url(candidate) or candidate.lower() in {"by others", "recirculating"}):
            skipped_preface += 1
            if skipped_preface >= 3:
                break
            forward += 1
            continue
        if not collected:
            collected.append(candidate)
            forward += 1
            continue
        if "as above" in lowered_current and _guess_make(candidate) and not _guess_model(candidate):
            break
        joined = _strip_urls_for_appliance_guessing(" ".join(collected))
        has_make = bool(_guess_make(joined))
        has_model = bool(_guess_model(joined))
        if has_make and has_model:
            break
        if re.search(r"(?i)\bmodel\s*[:#-]?\s*", candidate):
            collected.append(candidate)
            break
        if starts_with_label and has_make and not has_model:
            collected.append(candidate)
            forward += 1
            continue
        if has_make or has_model:
            break
        collected.append(candidate)
        forward += 1
    return normalize_space(" ".join(collected))


def _strip_urls_for_appliance_guessing(text: str) -> str:
    stripped = re.sub(r"(?i)\bhttps?://\S+", " ", str(text or ""))
    stripped = re.sub(r"(?i)\bwww\.\S+", " ", stripped)
    return normalize_space(stripped)


def _looks_like_url(text: str) -> bool:
    normalized = normalize_space(text)
    return bool(normalized and re.match(r"(?i)^(?:https?://|www\.)", normalized))


def _extract_explicit_appliance_model(text: str) -> str:
    cleaned = normalize_space(str(text or ""))
    if not cleaned:
        return ""
    for match in re.finditer(r"(?i)\bmodel\s*[:#-]?\s*([A-Za-z0-9./-]{3,})", cleaned):
        candidate = match.group(1).upper().strip(".")
        if _valid_model_candidate(candidate, allow_numeric=True):
            return candidate
    return ""


def _extract_appliance_placeholder_model(text: str) -> str:
    clean_text = normalize_space(text)
    if not clean_text:
        return ""
    normalized = re.sub(r"(?i)^\s*(?:oven|cooktop|rangehood|microwave|fridge|dishwasher|washer|dryer|washing machine|appliance)\b", "", clean_text)
    normalized = normalize_space(normalized).strip(" -;,")
    if not normalized:
        return ""
    placeholder_patterns = (
        r"(?i)\bn\s*/\s*a(?:\s*-\s*by others)?(?:\s+client\s+to\s+check)?\b",
        r"(?i)\bleave standard space by client\b",
        r"(?i)\bprovide space only\b",
        r"(?i)\bas above(?:\s+by client)?\b",
        r"(?i)\bby client\b(?:\s+plumbed in fridge)?",
        r"(?i)\bby builder\b",
    )
    matches: list[str] = []
    spans: list[tuple[int, int]] = []
    for pattern in placeholder_patterns:
        for match in re.finditer(pattern, normalized):
            span = match.span()
            if any(not (span[1] <= existing[0] or span[0] >= existing[1]) for existing in spans):
                continue
            spans.append(span)
            matches.append(normalize_space(match.group(0)))
    if not matches:
        return ""
    ordered: list[str] = []
    seen: set[str] = set()
    for match in matches:
        lowered = match.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(match)
    return " ".join(ordered).strip()


def _looks_like_appliance_placeholder_model(text: str) -> bool:
    lowered = normalize_space(text).lower()
    if not lowered:
        return False
    return any(
        token in lowered
        for token in (
            "n/a",
            "as above",
            "by client",
            "client to check",
            "provide space only",
            "leave standard space",
            "by builder",
        )
    )


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
    guess_text = _strip_urls_for_appliance_guessing(clean_details)
    make = _guess_make(guess_text)
    guessed_model = _guess_model(guess_text)
    model_no = guessed_model
    placeholder_model = ""
    placeholder_model = _extract_appliance_placeholder_model(clean_details)
    explicit_model = _extract_explicit_appliance_model(evidence or clean_details)
    if appliance_type == "Cooktop" and re.search(r"(?i)\boven/stove\b", clean_details) and (_guess_make(guess_text) or explicit_model):
        appliance_type = "Oven"
    if clean_details.upper().startswith("N/A") and not (guessed_model or placeholder_model or explicit_model):
        return None
    if explicit_model and (not model_no or _looks_like_appliance_placeholder_model(model_no) or model_no.startswith("WWW.")):
        model_no = explicit_model
    if placeholder_model and ("as above" in placeholder_model.lower() or not guessed_model):
        make = ""
        model_no = placeholder_model
    elif not model_no and placeholder_model:
        model_no = placeholder_model
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


def _limit_appliance_details_to_local_context(details: str, appliance_type: str = "") -> str:
    lines = [normalize_space(line) for line in str(details or "").splitlines() if normalize_space(line)]
    if not lines:
        return normalize_space(details)
    while len(lines) > 1 and (_looks_like_url(lines[0]) or lines[0].lower() in {"by others", "recirculating"}):
        lines.pop(0)
    kept = [lines[0]]
    for line in lines[1:]:
        if _is_appliance_context_boundary_line(line, appliance_type):
            break
        combined = _strip_urls_for_appliance_guessing(" ".join(kept))
        if (_guess_model(combined) or _guess_make(combined)) and not re.search(r"(?i)\bmodel\s*[:#-]?\s*", line):
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
    upper_text = str(text or "").upper()
    quantity_match = re.search(r"\b(\d+)\s*[xX]\s*([A-Z0-9./-]*[A-Z][A-Z0-9./-]*\d[A-Z0-9./-]*)\b", upper_text)
    if quantity_match:
        candidate = quantity_match.group(2).upper().strip(".")
        if _valid_model_candidate(candidate):
            return f"{quantity_match.group(1)} x {candidate}"

    for candidate in re.findall(r"\(([A-Za-z0-9./-]{3,})\)", upper_text):
        normalized = candidate.upper().strip(".")
        if _valid_model_candidate(normalized, allow_numeric=True):
            return normalized

    for match in re.finditer(r"\b([A-Z0-9./-]*[A-Z][A-Z0-9./-]*\d[A-Z0-9./-]*)\b", upper_text):
        candidate = match.group(1).upper().strip(".")
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


def _looks_like_flooring_overlay_value(value: str) -> bool:
    lowered = normalize_space(value).lower()
    if not lowered:
        return False
    return bool(
        re.search(
            r"\b(?:tiled?|tiles?|carpet|hybrid flooring|hybrid floorboards?|floorboards?|timber|vinyl|laminate|floor tile|concrete)\b",
            lowered,
        )
    )


def _clean_flooring_overlay_text(value: str) -> str:
    cleaned = normalize_space(str(value or "")).strip(" -|;,")
    cleaned = re.sub(r"(?i)\btiles refer to [\"“”'`]?tiling[\"“”'`]? section below\b", "", cleaned)
    cleaned = normalize_space(cleaned).strip(" -|;,")
    return cleaned


def _merge_room_flooring_overlay(overlays: dict[str, dict[str, Any]], room_key: str, value: str) -> None:
    normalized_value = _clean_flooring_overlay_text(value)
    if not room_key or not normalized_value:
        return
    overlay = overlays.setdefault(room_key, _blank_overlay())
    overlay["flooring"] = _merge_text(str(overlay.get("flooring", "") or ""), normalized_value)


def _clarendon_flooring_targets(area_label: str, overlays: dict[str, dict[str, Any]]) -> list[str]:
    normalized = normalize_space(area_label).lower()
    collapsed = re.sub(r"[^a-z0-9]+", "", normalized)
    if "kitchenpantryfamilymeals" in collapsed:
        targets = [source_room_key("KITCHEN"), "butlers_pantry"]
        if "walk_in_pantry" in overlays:
            targets.append("walk_in_pantry")
        return _unique(targets)
    if re.search(r"\btheatre\b", normalized):
        return [source_room_key("THEATRE ROOM")]
    if re.search(r"\brumpus\b", normalized):
        return [source_room_key("RUMPUS ROOM")]
    if re.search(r"(?i)\b(?:wir/?s?|robes?)\b", area_label):
        return [
            key
            for key in overlays
            if re.search(r"(?:^|_)(?:wir|robe)(?:_|$)", key)
        ]
    return []


def _collect_clarendon_flooring_overlays(
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    for document in documents:
        for page in document.get("pages", []):
            text = str(page.get("raw_text") or page.get("text") or "")
            upper = text.upper()
            if "CARPET & MAIN FLOOR" not in upper or "TILE" not in upper:
                continue
            for raw_line in text.splitlines():
                line = normalize_space(raw_line)
                if not line or ":" not in line:
                    continue
                area_label, flooring_value = [normalize_space(part) for part in line.split(":", 1)]
                if not _looks_like_flooring_overlay_value(flooring_value):
                    continue
                for room_key in _clarendon_flooring_targets(area_label, overlays):
                    _merge_room_flooring_overlay(overlays, room_key, flooring_value)


def _extract_flooring_block_from_text(text: str, area_pattern: str, stop_patterns: tuple[str, ...]) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    stop_union = "|".join(stop_patterns)
    match = re.search(
        rf"(?is)\b{area_pattern}\b\s+(?P<value>.+?)(?=\b(?:{stop_union})\b|$)",
        normalized,
    )
    if not match:
        return ""
    return _clean_flooring_overlay_text(match.group("value"))


def _extract_floor_tile_block_from_text(text: str, area_pattern: str, stop_patterns: tuple[str, ...]) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    area_match = re.search(rf"(?is)\b{area_pattern}\b", normalized)
    if not area_match:
        return ""
    trailing = normalized[area_match.end() :]
    stop_union = "|".join(stop_patterns)
    floor_match = re.search(
        rf"(?is)\bFloor Tile\b\s+(?P<value>.+?)(?=\b(?:{stop_union})\b|$)",
        trailing,
    )
    if not floor_match:
        return ""
    return _clean_flooring_overlay_text(f"Floor Tile {floor_match.group('value')}")


def _extract_area_block_from_text(text: str, area_pattern: str, stop_patterns: tuple[str, ...]) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    stop_union = "|".join(stop_patterns)
    match = re.search(
        rf"(?is)\b{area_pattern}\b\s+(?P<value>.+?)(?=\b(?:{stop_union})\b|$)",
        normalized,
    )
    if not match:
        return ""
    return normalize_space(match.group("value"))


def _extract_named_value_from_block(block: str, label_pattern: str, stop_patterns: tuple[str, ...]) -> str:
    normalized = normalize_space(block)
    if not normalized:
        return ""
    stop_union = "|".join(stop_patterns)
    match = re.search(
        rf"(?is)\b{label_pattern}\b\s+(?P<value>.+?)(?=\b(?:{stop_union})\b|$)",
        normalized,
    )
    if not match:
        return ""
    return normalize_space(match.group("value")).strip(" -|;,")


def _clean_yellowwood_overlay_source_text(text: str) -> str:
    cleaned_lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = normalize_space(raw_line)
        if not line:
            continue
        if re.match(r"(?i)^page\s+\d+\s*/\s*\d+$", line):
            continue
        if "Tone Interior Design Consulting" in line:
            continue
        if re.match(r"(?i)^LOT\s+\d+\b", line):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def _build_yellowwood_overlay_text(
    document: dict[str, object],
    page_filter: Callable[[str], bool] | None = None,
) -> str:
    page_texts: list[str] = []
    for page in sorted(document.get("pages", []), key=lambda item: int(item.get("page_no", 0) or 0)):
        text = str(page.get("raw_text") or page.get("text") or "")
        if not text or _yellowwood_looks_like_contents_noise(text):
            continue
        upper = text.upper()
        if page_filter and not page_filter(upper):
            continue
        cleaned = _clean_yellowwood_overlay_source_text(text)
        if cleaned:
            page_texts.append(cleaned)
    return "\n".join(page_texts)


def _build_yellowwood_overlay_lines(
    document: dict[str, object],
    page_filter: Callable[[str], bool] | None = None,
) -> list[str]:
    lines: list[str] = []
    for page in sorted(document.get("pages", []), key=lambda item: int(item.get("page_no", 0) or 0)):
        text = str(page.get("raw_text") or page.get("text") or "")
        if not text or _yellowwood_looks_like_contents_noise(text):
            continue
        upper = text.upper()
        if page_filter and not page_filter(upper):
            continue
        cleaned = _clean_yellowwood_overlay_source_text(text)
        if not cleaned:
            continue
        lines.extend(normalize_space(line) for line in cleaned.splitlines() if normalize_space(line))
    return lines


def _extract_area_block_from_lines(lines: list[str], area_pattern: str, header_patterns: tuple[str, ...]) -> str:
    if not lines:
        return ""
    start_index = -1
    for index, line in enumerate(lines):
        if re.fullmatch(rf"(?i){area_pattern}", line):
            start_index = index
            break
    if start_index < 0:
        return ""
    collected: list[str] = []
    for line in lines[start_index + 1 :]:
        if any(re.fullmatch(rf"(?i){pattern}", line) for pattern in header_patterns):
            break
        collected.append(line)
    return normalize_space(" ".join(collected))


def _collect_yellowwood_flooring_overlays(
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    non_wet_areas: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            r"ENTRY,\s*PASSAGE,\s*HALLWAYS,\s*DINING,\s*LIVING,\s*KITCHEN,\s*PANTRY",
            (source_room_key("KITCHEN"), source_room_key("PANTRY")),
        ),
        (r"KITCHEN\s*&\s*DINING", (source_room_key("KITCHEN"),)),
        (r"MASTER\s+BED\s*1\s*\+\s*WALK\s+IN\s+ROBE", (source_room_key("BED 1 MASTER WALK IN ROBE FIT OUT"),)),
        (r"BED\s*1\s*\+\s*WIR", (source_room_key("BED 1 WALK IN ROBE"),)),
        (r"BED\s*2\s*\+\s*ROBE", (source_room_key("BED 2 ROBE FIT OUT"),)),
        (r"BED\s*3\s*\+\s*ROBE", (source_room_key("BED 3 ROBE FIT OUT"),)),
        (r"BED\s*4\s*\+\s*ROBE", (source_room_key("BED 4 ROBE FIT OUT"),)),
        (r"BED\s*5\s*\+\s*ROBE", (source_room_key("BED 5 ROBE FIT OUT"),)),
    )
    non_wet_stops = (
        r"MEDIA ROOM",
        r"MASTER\s+BED\s*1\s*\+\s*WALK\s+IN\s+ROBE",
        r"BED\s*1\s*\+\s*WIR",
        r"BED\s*2\s*\+\s*ROBE",
        r"BED\s*3\s*\+\s*ROBE",
        r"BED\s*4\s*\+\s*ROBE",
        r"BED\s*5\s*\+\s*ROBE",
        r"LAUNDRY(?:\s+LINEN)?",
        r"GROUND\s+FLOOR\s+BATHROOM",
        r"GROUND\s+FLOOR\s+POWDER\s+ROOM",
        r"BED\s*1\s+ENSUITE",
        r"BED\s*1\s+MASTER\s+ENSUITE",
        r"BATHROOM",
        r"UPPER[- ](?:LEVEL|FLOOR)\s+BED\s*5\s+ENSUITE",
        r"UPPER[- ](?:LEVEL|FLOOR)\s+POWDER\s+ROOM",
        r"WC",
        r"PORCH",
        r"ALFRESCO",
        r"INTERNAL FINISHES",
        r"JOINERY",
    )
    wet_area_targets: tuple[tuple[str, str], ...] = (
        (r"BED\s*1\s+MASTER\s+ENSUITE", source_room_key("BED 1 MASTER ENSUITE VANITY")),
        (r"BED\s*1\s+ENSUITE", source_room_key("BED 1 ENSUITE VANITY")),
        (r"GROUND\s+FLOOR\s+BATHROOM", source_room_key("BATHROOM VANITY")),
        (r"BATHROOM", source_room_key("BATHROOM VANITY")),
        (r"GROUND\s+FLOOR\s+POWDER\s+ROOM", source_room_key("GROUND FLOOR POWDER ROOM")),
        (r"UPPER[- ](?:LEVEL|FLOOR)\s+BED\s*5\s+ENSUITE", source_room_key("BED 5 ENSUITE VANITY")),
        (r"UPPER[- ](?:LEVEL|FLOOR)\s+POWDER\s+ROOM", source_room_key("UPPER-LEVEL POWDER ROOM")),
        (r"LAUNDRY(?:\s*\(INC LINEN FLOOR\))?", source_room_key("LAUNDRY")),
    )
    wet_area_stops = (
        r"Wall Tile",
        r"Splashback Tile",
        r"Vanity Splashback",
        r"Niche Tile",
        r"Skirting",
        r"LAUNDRY(?:\s*\(INC LINEN FLOOR\))?",
        r"GROUND\s+FLOOR\s+BATHROOM",
        r"GROUND\s+FLOOR\s+POWDER\s+ROOM",
        r"UPPER[- ](?:LEVEL|FLOOR)\s+BED\s*5\s+ENSUITE",
        r"UPPER[- ](?:LEVEL|FLOOR)\s+POWDER\s+ROOM",
        r"BED\s*1\s+MASTER\s+ENSUITE",
        r"BATHROOM",
        r"BED\s*1\s+ENSUITE",
        r"WC",
        r"PORCH",
        r"ALFRESCO",
    )
    for document in documents:
        combined_text = _build_yellowwood_overlay_text(document)
        if not combined_text:
            continue
        combined_upper = combined_text.upper()
        if "OTHER THAN TILING TO WET AREAS" in combined_upper:
            for area_pattern, room_keys in non_wet_areas:
                flooring_value = _extract_flooring_block_from_text(combined_text, area_pattern, non_wet_stops)
                if flooring_value and _looks_like_flooring_overlay_value(flooring_value):
                    for room_key in room_keys:
                        _merge_room_flooring_overlay(overlays, room_key, flooring_value)
        if "TILING SCHEDULE" in combined_upper or "FLOOR TILE" in combined_upper:
            for area_pattern, room_key in wet_area_targets:
                flooring_value = _extract_floor_tile_block_from_text(combined_text, area_pattern, wet_area_stops)
                if flooring_value and _looks_like_flooring_overlay_value(flooring_value):
                    _merge_room_flooring_overlay(overlays, room_key, flooring_value)


def _collect_yellowwood_fixture_overlays(
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    fixture_targets = _yellowwood_fixture_area_targets()
    header_patterns = tuple(pattern for pattern, _room_key in fixture_targets) + (
        r"INTERNAL\s+FINISHES",
        r"JOINERY",
        r"TILING\s+SCHEDULE",
        r"FLOORING",
        r"PORCH",
        r"ALFRESCO",
    )
    for document in documents:
        lines = _build_yellowwood_overlay_lines(document, page_filter=_yellowwood_fixture_page_filter)
        if not lines:
            continue
        for area_pattern, room_key in fixture_targets:
            block = _extract_yellowwood_area_block_from_lines(lines, area_pattern, header_patterns)
            if not block:
                continue
            overlay = overlays.setdefault(room_key, _blank_overlay())
            sink_text = _extract_yellowwood_fixture_from_block(block, "sink")
            basin_text = _extract_yellowwood_fixture_from_block(block, "basin")
            tap_text = _extract_yellowwood_fixture_from_block(block, "tap")
            if sink_text:
                overlay["sink_info"] = _merge_text(overlay.get("sink_info", ""), sink_text)
            if basin_text:
                overlay["basin_info"] = _merge_text(overlay.get("basin_info", ""), basin_text)
            if tap_text:
                overlay["tap_info"] = _merge_text(overlay.get("tap_info", ""), tap_text)


def _clear_room_specific_flooring_notes(snapshot: dict[str, Any]) -> None:
    builder_name = normalize_space(str(snapshot.get("builder_name", "") or "")).lower()
    others = snapshot.get("others")
    if not isinstance(others, dict):
        return
    flooring_notes = normalize_space(str(others.get("flooring_notes", "") or ""))
    if not flooring_notes:
        return
    if _is_yellowwood_builder(builder_name) and (
        _yellowwood_looks_like_contents_noise(flooring_notes)
        or re.search(r"(?i)\bother than tiling to wet areas\b", flooring_notes)
    ):
        others["flooring_notes"] = ""
        return
    if re.search(r"(?i)\brequires expansion joints\b", flooring_notes):
        others["flooring_notes"] = ""
        return
    if re.search(r"(?i)\bsupplier\s+beaumont\s+tiles\s+tile\s+range\s+floor\s+tile\s+type\b", flooring_notes):
        others["flooring_notes"] = ""
        return
    if builder_name == "clarendon" and re.search(r"(?i)\bcarpet\s*&\s*main\s*floor\s*tile\b", flooring_notes):
        others["flooring_notes"] = ""


def _looks_like_appliance_noise(text: str) -> bool:
    normalized = normalize_space(text).lower()
    if not normalized:
        return False
    if "appliances" in normalized:
        return True
    markers = (
        "freestanding cooker",
        "under bench oven",
        "cooktop",
        "dishwasher",
        "rangehood",
        "fridge",
        "microwave",
    )
    return sum(1 for marker in markers if marker in normalized) >= 2


def _clear_room_specific_splashback_notes(snapshot: dict[str, Any]) -> None:
    builder_name = normalize_space(str(snapshot.get("builder_name", "") or "")).lower()
    others = snapshot.get("others")
    if not isinstance(others, dict):
        return
    splashback_notes = normalize_space(str(others.get("splashback_notes", "") or ""))
    if not splashback_notes:
        return
    if builder_name == "clarendon" and _looks_like_appliance_noise(splashback_notes):
        others["splashback_notes"] = ""


def _apply_clarendon_room_overlap_corrections(rooms: list[dict[str, Any]]) -> None:
    room_lookup: dict[str, dict[str, Any]] = {}
    for row in rooms:
        if not isinstance(row, dict):
            continue
        candidates = (
            normalize_space(str(row.get("room_key", ""))),
            source_room_key(str(row.get("original_room_label", "")), fallback_key=str(row.get("room_key", ""))),
            same_room_identity(str(row.get("original_room_label", "")), str(row.get("room_key", ""))),
        )
        for room_key in candidates:
            if room_key and room_key not in room_lookup:
                room_lookup[room_key] = row
    rumpus_room = room_lookup.get("rumpus_room") or room_lookup.get("rumpus")
    rumpus_desk = room_lookup.get("rumpus_desk")
    if not rumpus_room or not rumpus_desk:
        return
    parent_tall = normalize_space(str(rumpus_room.get("door_colours_tall", "") or ""))
    if not re.search(r"(?i)\btall open shelves\b", parent_tall):
        return
    rumpus_room["door_colours_tall"] = ""
    rumpus_room["has_explicit_tall"] = False
    if not normalize_space(str(rumpus_desk.get("door_colours_tall", "") or "")):
        rumpus_desk["door_colours_tall"] = parent_tall
    rumpus_desk["has_explicit_tall"] = bool(normalize_space(str(rumpus_desk.get("door_colours_tall", "") or "")))
    rumpus_room["door_panel_colours"] = _rebuild_door_panel_colours(rumpus_room)
    rumpus_desk["door_panel_colours"] = _rebuild_door_panel_colours(rumpus_desk)


def _apply_clarendon_accessory_room_corrections(rooms: list[dict[str, Any]]) -> None:
    room_lookup: dict[str, dict[str, Any]] = {}
    for row in rooms:
        if not isinstance(row, dict):
            continue
        room_key = source_room_key(
            str(row.get("original_room_label", "") or row.get("room_name", "") or ""),
            fallback_key=str(row.get("room_key", "")),
        )
        if room_key and room_key not in room_lookup:
            room_lookup[room_key] = row
    vanities = room_lookup.get("vanities")
    laundry = room_lookup.get("laundry")
    if not vanities or not laundry:
        return
    moved: list[str] = []
    kept: list[str] = []
    for value in _coerce_string_list(vanities.get("accessories", [])):
        if re.search(r"(?i)\b(?:lincoln sentry|finista|centre pillar|end support)\b", value):
            moved.append(value)
        else:
            kept.append(value)
    if not moved:
        return
    vanities["accessories"] = _clean_accessory_entries(kept)
    laundry["accessories"] = _clean_accessory_entries(
        _merge_lists(_coerce_string_list(laundry.get("accessories", [])), moved)
    )


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


def _snapshot_builder_finalizer(builder_name: str) -> Callable[[list[dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, object]]], None] | None:
    normalized = normalize_space(builder_name).lower()
    if normalized == "clarendon":
        return _finalize_clarendon_rooms
    if _is_yellowwood_builder(normalized):
        return _finalize_yellowwood_rooms
    if _is_imperial_builder(normalized):
        return _finalize_imperial_rooms
    if normalized in {"simonds", "evoca"}:
        return _finalize_grouped_row_builder_rooms
    return None


def _apply_builder_room_finalizer(
    builder_name: str,
    rooms: list[dict[str, Any]],
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    finalizer = _snapshot_builder_finalizer(builder_name)
    if finalizer is None:
        return
    finalizer(rooms, overlays, documents)


def _finalize_clarendon_rooms(
    rooms: list[dict[str, Any]],
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    _apply_clarendon_room_overlap_corrections(rooms)
    _apply_clarendon_accessory_room_corrections(rooms)


def _finalize_imperial_rooms(
    rooms: list[dict[str, Any]],
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    return


def _finalize_grouped_row_builder_rooms(
    rooms: list[dict[str, Any]],
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    return


def _yellowwood_row_probe_text(row: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in (
        "original_room_label",
        "room_name",
        "evidence_snippet",
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "floating_shelf",
        "shelf",
        "sink_info",
        "basin_info",
        "tap_info",
        "splashback",
        "flooring",
    ):
        value = normalize_space(str(row.get(key, "") or ""))
        if value:
            parts.append(value)
    parts.extend(_coerce_string_list(row.get("bench_tops", [])))
    parts.extend(_coerce_string_list(row.get("door_panel_colours", [])))
    parts.extend(_coerce_string_list(row.get("toe_kick", [])))
    parts.extend(_coerce_string_list(row.get("bulkheads", [])))
    parts.extend(_coerce_string_list(row.get("handles", [])))
    parts.extend(_coerce_string_list(row.get("accessories", [])))
    for item in _merge_other_items([], row.get("other_items", [])):
        label = normalize_space(str(item.get("label", "") or ""))
        value = normalize_space(str(item.get("value", "") or ""))
        if label or value:
            parts.append(f"{label} {value}".strip())
    return normalize_space(" | ".join(part for part in parts if part))


ROOM_MATERIAL_EVIDENCE_SCALAR_FIELDS: tuple[str, ...] = (
    "bench_tops_wall_run",
    "bench_tops_island",
    "bench_tops_other",
    "door_colours_overheads",
    "door_colours_base",
    "door_colours_tall",
    "door_colours_island",
    "door_colours_bar_back",
    "splashback",
    "floating_shelf",
    "shelf",
)

ROOM_MATERIAL_EVIDENCE_LIST_FIELDS: tuple[str, ...] = (
    "toe_kick",
    "bulkheads",
)


def _is_placeholder_material_value(text: Any) -> bool:
    cleaned = normalize_space(str(text or "")).strip(" -;,")
    if not cleaned:
        return True
    return bool(re.fullmatch(r"(?i)(?:not applicable|not included|n/?a|na)(?:\b.*)?", cleaned))


def _room_material_evidence_values(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ROOM_MATERIAL_EVIDENCE_SCALAR_FIELDS:
        text = normalize_space(str(row.get(key, "") or ""))
        if text and not _is_placeholder_material_value(text):
            values.append(text)
    for key in ROOM_MATERIAL_EVIDENCE_LIST_FIELDS:
        values.extend(
            value
            for value in _coerce_string_list(row.get(key, []))
            if not _is_placeholder_material_value(value)
        )
    return [value for value in values if normalize_space(value) and not _is_placeholder_material_value(value)]


def _room_has_material_evidence(row: dict[str, Any]) -> bool:
    return bool(_room_material_evidence_values(row))


def _yellowwood_specific_room_label(row: dict[str, Any]) -> str:
    room_key = normalize_space(str(row.get("room_key", "") or ""))
    if room_key == "ground_floor_powder_room":
        return "GROUND FLOOR POWDER ROOM"
    if room_key == "upper_level_powder_room":
        return "UPPER-LEVEL POWDER ROOM"
    if room_key == "bathroom":
        return "BATHROOM VANITY"
    if room_key == "pantry":
        return "PANTRY"
    if room_key == "walk_in_pantry":
        probe = _yellowwood_row_probe_text(row)
        if re.search(r"(?i)\bopen shelving\b", probe):
            return "PANTRY"
    probe = _yellowwood_row_probe_text(row)
    for pattern, replacement in (
        (r"(?i)\b(BED\s*\d+\s+MASTER\s+ENSUITE\s+VANITY)\b", None),
        (r"(?i)\b(BED\s*\d+\s+ENSUITE\s+VANITY)\b", None),
        (r"(?i)\b(BED\s*\d+\s+MASTER\s+WALK[- ]IN[- ]ROBE\s+FIT\s+OUT)\b", None),
        (r"(?i)\b(BED\s*\d+\s+WALK[- ]IN[- ]ROBE\s+FIT\s+OUT)\b", None),
        (r"(?i)\b(BED\s*\d+\s+ROBE\s+FIT\s+OUT)\b", None),
        (r"(?i)\b(GROUND\s+FLOOR\s+POWDER\s+ROOM)\b", "GROUND FLOOR POWDER ROOM"),
        (r"(?i)\b(UPPER[- ](?:LEVEL|FLOOR)\s+POWDER\s+ROOM)\b", "UPPER-LEVEL POWDER ROOM"),
        (r"(?i)\b(BATH(?:ROOM|OOM)\s+VANITY)\b", "BATHROOM VANITY"),
    ):
        match = re.search(pattern, probe)
        if not match:
            continue
        return replacement or normalize_space(match.group(1)).replace("UPPER FLOOR", "UPPER-LEVEL").upper()
    original_label = normalize_space(str(row.get("original_room_label", "") or ""))
    return original_label


def _yellowwood_is_false_led_note(note: str) -> bool:
    normalized = normalize_space(note)
    if not normalized:
        return False
    return bool(
        re.search(
            r"(?i)\b(?:led edge|led top mounted|top mounted\*?|penciled edge)\b",
            normalized,
        )
    )


def _yellowwood_cleanup_bulkheads(values: list[str]) -> list[str]:
    cleaned: list[str] = []
    for value in _coerce_string_list(values):
        text = normalize_space(value)
        if not text:
            continue
        if text.startswith("*") and not re.search(r"(?i)\bbulkhead\b", text):
            continue
        if re.search(r"(?i)\b(?:to bulkhead|to builders|bulkhead above)\b", text):
            continue
        cleaned.append(text)
    return _unique(cleaned)


def _yellowwood_cleanup_handles(row: dict[str, Any]) -> list[str]:
    room_label = normalize_space(str(row.get("original_room_label", "") or ""))
    cleaned: list[str] = []
    for value in _clean_handle_entries(_coerce_string_list(row.get("handles", []))):
        text = normalize_space(value)
        if not text:
            continue
        if "VANITY" in room_label.upper() or "POWDER ROOM" in room_label.upper():
            if re.search(r"(?i)\b(?:mirrored shaving cabinet|highgrove bathrooms|led edge)\b", text):
                continue
        if text.startswith("House "):
            text = f"Handle {text}"
        cleaned.append(text)
    if len(cleaned) >= 2 and cleaned[0].lower().startswith("handle house") and not cleaned[1].lower().startswith("handle house"):
        merged = normalize_space(f"{cleaned[0]} {cleaned[1]}")
        cleaned = [merged, *cleaned[2:]]
    return _unique(cleaned)


def _yellowwood_remove_island_duplication(row: dict[str, Any]) -> None:
    wall_run = normalize_space(str(row.get("bench_tops_wall_run", "") or ""))
    island = normalize_space(str(row.get("bench_tops_island", "") or ""))
    other = normalize_space(str(row.get("bench_tops_other", "") or ""))
    if not other:
        return
    protected_signatures = {
        _material_signature(normalize_space(re.sub(r"(?i)^only\s+", "", value)))
        for value in (wall_run, island)
        if normalize_space(re.sub(r"(?i)^only\s+", "", value))
    }
    protected_values = [value for value in (wall_run, island) if value]
    parts = [normalize_space(part) for part in other.split("|") if normalize_space(part)]
    kept: list[str] = []
    for part in parts:
        part_probe = normalize_space(re.sub(r"(?i)^only\s+", "", part))
        if any(value.lower() in part.lower() for value in protected_values):
            continue
        if protected_signatures and _material_signature(part_probe) in protected_signatures:
            continue
        kept.append(part)
    row["bench_tops_other"] = " | ".join(_unique(kept))


def _yellowwood_normalize_kitchen_material_fields(row: dict[str, Any]) -> None:
    if normalize_room_key(str(row.get("room_key", "") or "")) != "kitchen":
        return
    for key in ("door_colours_overheads", "door_colours_base", "door_colours_tall", "door_colours_island", "door_colours_bar_back"):
        row[key] = _clean_door_colour_value(row.get(key, ""))
    _yellowwood_remove_island_duplication(row)
    if row.get("has_explicit_overheads") and not normalize_space(str(row.get("door_colours_overheads", "") or "")):
        base_value = normalize_space(str(row.get("door_colours_base", "") or ""))
        if base_value:
            row["door_colours_overheads"] = base_value
    wall_run = normalize_space(str(row.get("bench_tops_wall_run", "") or ""))
    island = normalize_space(str(row.get("bench_tops_island", "") or ""))
    other_parts = [normalize_space(part) for part in str(row.get("bench_tops_other", "") or "").split("|") if normalize_space(part)]
    if not wall_run and island and other_parts:
        row["bench_tops_wall_run"] = other_parts[0]
        row["bench_tops_other"] = " | ".join(other_parts[1:])


def _yellowwood_normalize_vanity_material_fields(row: dict[str, Any]) -> None:
    room_label = normalize_space(str(row.get("original_room_label", "") or row.get("room_name", "") or ""))
    if not room_label:
        return
    if "VANITY" not in room_label.upper() and "POWDER ROOM" not in room_label.upper():
        return
    bench_other = normalize_space(str(row.get("bench_tops_other", "") or ""))
    if bench_other:
        match = re.search(r"(?i)\b(?:wall\s+hung\s+vanity|floor\s+mount(?:ed)?\s+vanity)\b", bench_other)
        if match:
            benchtop_value = normalize_space(bench_other[: match.start()])
            vanity_value = _clean_door_colour_value(bench_other[match.end() :])
            if benchtop_value:
                row["bench_tops_other"] = benchtop_value
            if vanity_value:
                row["door_colours_base"] = _merge_clean_group_text(row.get("door_colours_base", ""), vanity_value, cleaner=_clean_door_colour_value)
    toe_kick_values = []
    for value in _coerce_string_list(row.get("toe_kick", [])):
        cleaned = normalize_space(value)
        if re.fullmatch(r"(?i)n/?a(?:\s+n/?a)?", cleaned):
            continue
        toe_kick_values.append(cleaned)
    row["toe_kick"] = _unique(toe_kick_values)


def _yellowwood_material_fallback(row: dict[str, Any]) -> str:
    candidates = [
        *(_coerce_string_list(row.get("toe_kick", []))),
        *(_coerce_string_list(row.get("door_panel_colours", []))),
        normalize_space(str(row.get("door_colours_bar_back", "") or "")),
    ]
    for candidate in candidates:
        text = _clean_door_colour_value(candidate)
        if text:
            return text
    return ""


def _yellowwood_merge_overlay_into_row(row: dict[str, Any], overlay: dict[str, Any]) -> None:
    if not overlay:
        return
    for key in (
        "bench_tops_wall_run",
        "bench_tops_island",
        "bench_tops_other",
        "door_colours_overheads",
        "door_colours_base",
        "door_colours_tall",
        "door_colours_island",
        "door_colours_bar_back",
        "floating_shelf",
        "sink_info",
        "basin_info",
        "tap_info",
        "flooring",
    ):
        overlay_value = normalize_space(str(overlay.get(key, "") or ""))
        current_value = normalize_space(str(row.get(key, "") or ""))
        if overlay_value and (not current_value or current_value.startswith("*")):
            row[key] = overlay_value
    for list_key in ("toe_kick", "bulkheads", "handles", "accessories"):
        row[list_key] = _merge_lists(_coerce_string_list(row.get(list_key, [])), _coerce_string_list(overlay.get(list_key, [])))
    row["other_items"] = _merge_other_items(row.get("other_items", []), overlay.get("other_items", []))
    row["led_note"] = _merge_led_note(row.get("led_note", ""), overlay.get("led_note", ""))
    row["led"] = _normalize_led_value(row.get("led", ""), row.get("led_note", ""))


def _yellowwood_is_placeholder_fixture(text: str, kind: str) -> bool:
    normalized = normalize_space(text)
    if not normalized:
        return True
    lowered = normalized.lower()
    if "refer to" in lowered and "plumbing" in lowered:
        return True
    if re.fullmatch(r"(?i)(?:n/?a|na|only)", normalized):
        return True
    if kind == "tap":
        return normalized in {"Spin", "Spin Tall", "Zara"}
    return False


def _yellowwood_prefer_overlay_text(current: Any, overlay: Any, kind: str = "") -> str:
    current_text = normalize_space(str(current or ""))
    overlay_text = normalize_space(str(overlay or ""))
    if not overlay_text:
        return current_text
    if not current_text:
        return overlay_text
    current_parts = [normalize_space(part) for part in current_text.split("|") if normalize_space(part)]
    if current_parts and all(part.lower() in overlay_text.lower() for part in current_parts):
        return overlay_text
    if kind in {"sink", "basin", "tap"}:
        if _yellowwood_is_placeholder_fixture(current_text, kind) and not _yellowwood_is_placeholder_fixture(overlay_text, kind):
            return overlay_text
        if len(overlay_text) > len(current_text) + 12:
            return overlay_text
    elif kind == "flooring" and len(overlay_text) > len(current_text) + 16:
        return overlay_text
    return current_text


def _yellowwood_cleanup_flooring_text(text: Any, room_key: str) -> str:
    cleaned = normalize_space(str(text or ""))
    if not cleaned:
        return ""
    if _yellowwood_looks_like_contents_noise(cleaned):
        return ""
    if room_key in {"bathroom", "ensuite_1", "ensuite_5", "ground_floor_powder_room", "upper_level_powder_room", "laundry"}:
        if re.search(r"(?i)\bother than tiling to wet areas\b", cleaned):
            return ""
    if room_key in {"bed_1_wir", "bed_2_robe", "bed_3_robe", "bed_4_robe", "bed_5_robe"}:
        cleaned = _trim_fixture_text_at_markers(
            cleaned,
            (
                r"\bUPPER[- ]LEVEL\b",
                r"\bUPPER[- ]FLOOR\b",
                r"\bRETREAT\b",
                r"\bPORCH\b",
            ),
        )
    return cleaned


def _yellowwood_cleanup_splashback_text(text: Any, room_key: str) -> str:
    cleaned = normalize_space(str(text or ""))
    if not cleaned:
        return ""
    if _yellowwood_looks_like_contents_noise(cleaned):
        return ""
    lowered = cleaned.lower()
    if "refer to" in lowered and "tiling" in lowered:
        return ""
    if "tile refer to" in lowered and "section" in lowered:
        return ""
    if room_key in {"bathroom", "ensuite_1", "ensuite_5", "ground_floor_powder_room", "upper_level_powder_room", "laundry"}:
        if re.search(r"(?i)\b(?:wall tile|floor tile|lay pattern|grout|tiling to)\b", cleaned):
            return ""
    return cleaned


def _collapse_pipe_text_variants(text: Any) -> str:
    parts = [normalize_space(part) for part in re.split(r"\s*\|\s*", str(text or "")) if normalize_space(part)]
    if not parts:
        return ""
    canonical = {
        part: re.sub(r"[^a-z0-9]+", " ", part.lower()).strip()
        for part in parts
    }
    kept: list[str] = []
    seen_canonical: set[str] = set()
    for part in parts:
        if canonical[part] in seen_canonical:
            continue
        if any(
            canonical[part] != canonical[other] and canonical[part] and canonical[part] in canonical[other]
            for other in parts
        ):
            continue
        seen_canonical.add(canonical[part])
        if part not in kept:
            kept.append(part)
    return " | ".join(kept)


def _yellowwood_fixture_page_filter(upper_text: str) -> bool:
    upper = normalize_space(upper_text).upper()
    if not upper:
        return False
    if any(token in upper for token in ("JOINERY", "TILING SCHEDULE", "FLOORING - OTHER THAN TILING")):
        return False
    return any(
        token in upper
        for token in (
            "SINK MIXER",
            "BASIN MIXER",
            "PULL-OUT KITCHEN MIXER",
            "BASIN WASTE",
            "SINK WASTE",
            "UNDERMOUNT 750MM DOUBLE",
            "GOOSENECK",
        )
    )


def _yellowwood_fixture_area_targets() -> tuple[tuple[str, str], ...]:
    return (
        (r"KITCHEN", source_room_key("KITCHEN")),
        (r"PANTRY", source_room_key("PANTRY")),
        (r"LAUNDRY", source_room_key("LAUNDRY")),
        (r"GROUND\s+FLOOR\s+BATHROOM", source_room_key("BATHROOM VANITY")),
        (r"BATHROOM", source_room_key("BATHROOM VANITY")),
        (r"GROUND\s+FLOOR\s+POWDER\s+ROOM", source_room_key("GROUND FLOOR POWDER ROOM")),
        (r"UPPER[- ](?:LEVEL|FLOOR)\s+POWDER\s+ROOM", source_room_key("UPPER-LEVEL POWDER ROOM")),
        (r"BED\s*1\s+MASTER\s+ENSUITE", source_room_key("BED 1 MASTER ENSUITE VANITY")),
        (r"BED\s*1\s+ENSUITE", source_room_key("BED 1 ENSUITE VANITY")),
        (r"UPPER[- ](?:LEVEL|FLOOR)\s+BED\s*5\s+ENSUITE", source_room_key("BED 5 ENSUITE VANITY")),
    )


def _extract_yellowwood_fixture_from_block(block: str, kind: str) -> str:
    normalized = normalize_space(block)
    if not normalized:
        return ""
    if kind == "tap":
        match = re.search(
            r"(?is)\b(?:Sink\s+)?Mixer\b\s+(?P<value>.+?)(?=\b(?:Wall\s+Hung\s+Basin|Basin\s+(?!Mixer)|Basin\s+Waste|Sink\s+(?!Waste|Mixer)|Sink\s+Waste|Toilet|Shower|Bath|Floor\s+Waste|Towel\s+Rail|Toilet\s+Roll\s+Holder)\b|$)",
            normalized,
        )
        if not match:
            return ""
        return normalize_space(re.sub(r"(?i)\bLED\s+(?:Top\s+Mounted|Undermount(?:ed)?)\b", "", match.group("value"))).strip(" -|;,")
    if kind == "sink":
        match = re.search(
            r"(?is)\bSink\b(?!\s+Mixer)\s+(?P<value>.+?)(?=\b(?:Sink\s+Waste|(?:Sink\s+)?Mixer|Wall\s+Hung\s+Basin|Basin\b|Toilet|Shower|Bath|Floor\s+Waste)\b|$)",
            normalized,
        )
        if not match:
            return ""
        return normalize_space(re.sub(r"(?i)\bLED\s+(?:Top\s+Mounted|Undermount(?:ed)?)\b", "", match.group("value"))).strip(" -|;,")
    if kind == "basin":
        match = re.search(
            r"(?is)\b(?:Wall\s+Hung\s+)?Basin\b(?!\s+Mixer)\s+(?P<value>.+?)(?=\b(?:Basin\s+Bottle\s+Trap|Bottle\s+Trap|Basin\s+Waste|Sink\s+Waste|Toilet|Shower|Bath|Floor\s+Waste|Toilet\s+Roll\s+Holder|Towel\s+Rail)\b|$)",
            normalized,
        )
        if not match:
            return ""
        return normalize_space(match.group("value")).strip(" -|;,")
    return ""


def _extract_yellowwood_area_block_from_lines(lines: list[str], area_pattern: str, header_patterns: tuple[str, ...]) -> str:
    if not lines:
        return ""
    start_index = -1
    consumed = 0
    collected: list[str] = []
    for index, raw_line in enumerate(lines):
        line = normalize_space(raw_line)
        if not line:
            continue
        candidates = [(line, 1)]
        if index + 1 < len(lines):
            candidates.append((normalize_space(f"{line} {lines[index + 1]}"), 2))
        if index + 2 < len(lines):
            candidates.append((normalize_space(f"{line} {lines[index + 1]} {lines[index + 2]}"), 3))
        for candidate, width in candidates:
            match = re.match(rf"(?i)^{area_pattern}(?:\b|$)", candidate)
            if not match:
                continue
            start_index = index
            consumed = width
            remainder = normalize_space(candidate[match.end() :])
            if remainder:
                collected.append(remainder)
            break
        if start_index >= 0:
            break
    if start_index < 0:
        return ""
    for offset, line in enumerate(lines[start_index + max(consumed, 1) :], start=start_index + max(consumed, 1)):
        stop = False
        candidates = [(normalize_space(line), 1)]
        if offset + 1 < len(lines):
            candidates.append((normalize_space(f"{line} {lines[offset + 1]}"), 2))
        if offset + 2 < len(lines):
            candidates.append((normalize_space(f"{line} {lines[offset + 1]} {lines[offset + 2]}"), 3))
        if any(any(re.match(rf"(?i)^{pattern}(?:\b|$)", candidate) for pattern in header_patterns) for candidate, _width in candidates):
            break
        collected.append(normalize_space(line))
    return normalize_space("\n".join(part for part in collected if normalize_space(part)))


def _finalize_yellowwood_rooms(
    rooms: list[dict[str, Any]],
    overlays: dict[str, dict[str, Any]],
    documents: list[dict[str, object]],
) -> None:
    merged_by_key: dict[str, dict[str, Any]] = {}
    for row in rooms:
        room_key = source_room_key(
            str(row.get("original_room_label", "") or row.get("room_name", "") or ""),
            fallback_key=str(row.get("room_key", "")),
        )
        if room_key:
            row["room_key"] = room_key
        specific_label = _yellowwood_specific_room_label(row)
        if specific_label:
            row["original_room_label"] = specific_label
            row["room_name"] = specific_label
        row["room_key"] = source_room_key(
            str(row.get("original_room_label", "") or row.get("room_name", "") or ""),
            fallback_key=str(row.get("room_key", "")),
        )
        _yellowwood_merge_overlay_into_row(row, overlays.get(str(row.get("room_key", "")), {}))
        if _yellowwood_is_false_led_note(str(row.get("led_note", "") or "")):
            row["led_note"] = ""
            row["led"] = "No"
        fallback_material = _yellowwood_material_fallback(row)
        if fallback_material:
            if normalize_space(str(row.get("door_colours_base", "") or "")).startswith("*"):
                row["door_colours_base"] = fallback_material
            if normalize_space(str(row.get("door_colours_overheads", "") or "")).startswith("*"):
                row["door_colours_overheads"] = fallback_material
        row["bulkheads"] = _yellowwood_cleanup_bulkheads(row.get("bulkheads", []))
        row["handles"] = _yellowwood_cleanup_handles(row)
        _yellowwood_normalize_kitchen_material_fields(row)
        _yellowwood_normalize_vanity_material_fields(row)
        overlay = overlays.get(str(row.get("room_key", "")), {})
        row["sink_info"] = _clean_room_fixture_text(
            _yellowwood_prefer_overlay_text(row.get("sink_info", ""), overlay.get("sink_info", ""), "sink"),
            "sink",
        )
        row["basin_info"] = _clean_room_fixture_text(
            _yellowwood_prefer_overlay_text(row.get("basin_info", ""), overlay.get("basin_info", ""), "basin"),
            "basin",
        )
        row["tap_info"] = _clean_room_fixture_text(
            _yellowwood_prefer_overlay_text(row.get("tap_info", ""), overlay.get("tap_info", ""), "tap"),
            "tap",
        )
        row["sink_info"] = _collapse_pipe_text_variants(row.get("sink_info", ""))
        row["basin_info"] = _collapse_pipe_text_variants(row.get("basin_info", ""))
        row["tap_info"] = _collapse_pipe_text_variants(row.get("tap_info", ""))
        row["flooring"] = _yellowwood_cleanup_flooring_text(
            _yellowwood_prefer_overlay_text(row.get("flooring", ""), overlay.get("flooring", ""), "flooring"),
            str(row.get("room_key", "") or ""),
        )
        row["splashback"] = _yellowwood_cleanup_splashback_text(row.get("splashback", ""), str(row.get("room_key", "") or ""))
        row["shelf"] = _merge_text(_string_value(row.get("shelf", "")), overlay.get("shelf", ""))
        if normalize_space(str(row.get("original_room_label", "") or "")).upper() == "PANTRY":
            pantry_materials = any(
                normalize_space(str(row.get(key, "") or ""))
                for key in ("bench_tops_wall_run", "bench_tops_island", "bench_tops_other", "door_colours_base", "door_colours_overheads")
            ) or bool(_coerce_string_list(row.get("door_panel_colours", [])))
            pantry_probe = _yellowwood_row_probe_text(row)
            if re.search(r"(?i)\b(?:shelving only|open shelving|x\d+\s+shelves?)\b", pantry_probe) and not pantry_materials:
                row["sink_info"] = ""
                row["basin_info"] = ""
                row["tap_info"] = ""
        room_key = normalize_space(str(row.get("room_key", "") or ""))
        label = normalize_space(str(row.get("original_room_label", "") or ""))
        if room_key == "walk_in_pantry" and label.upper() == "WIP":
            continue
        if room_key.startswith("bed_") and "FIT OUT" in label.upper():
            row["other_items"] = [
                item
                for item in _merge_other_items([], row.get("other_items", []))
                if "LINEN CUPBOARD" not in normalize_space(str(item.get("value", "") or "")).upper()
            ]
        existing = merged_by_key.get(room_key)
        if existing is None:
            merged_by_key[room_key] = row
            continue
        existing["original_room_label"] = _prefer_more_specific_room_label(
            str(existing.get("original_room_label", "") or ""),
            str(row.get("original_room_label", "") or ""),
        )
        existing["room_name"] = existing["original_room_label"]
        for key in (
            "bench_tops_wall_run",
            "bench_tops_island",
            "bench_tops_other",
            "door_colours_overheads",
            "door_colours_base",
            "door_colours_tall",
            "door_colours_island",
            "door_colours_bar_back",
            "floating_shelf",
            "shelf",
        ):
            existing[key] = _merge_text(existing.get(key, ""), row.get(key, ""))
        existing["sink_info"] = _yellowwood_prefer_overlay_text(existing.get("sink_info", ""), row.get("sink_info", ""), "sink")
        existing["basin_info"] = _yellowwood_prefer_overlay_text(existing.get("basin_info", ""), row.get("basin_info", ""), "basin")
        existing["tap_info"] = _yellowwood_prefer_overlay_text(existing.get("tap_info", ""), row.get("tap_info", ""), "tap")
        existing["flooring"] = _yellowwood_prefer_overlay_text(existing.get("flooring", ""), row.get("flooring", ""), "flooring")
        for list_key in ("bench_tops", "door_panel_colours", "toe_kick", "bulkheads", "handles", "accessories"):
            existing[list_key] = _merge_lists(_coerce_string_list(existing.get(list_key, [])), _coerce_string_list(row.get(list_key, [])))
        existing["other_items"] = _merge_other_items(existing.get("other_items", []), row.get("other_items", []))
        existing["led_note"] = _merge_led_note(existing.get("led_note", ""), row.get("led_note", ""))
        existing["led"] = _normalize_led_value(existing.get("led", ""), existing.get("led_note", ""))
    rooms[:] = list(merged_by_key.values())
    for row in rooms:
        _yellowwood_normalize_kitchen_material_fields(row)
        _yellowwood_normalize_vanity_material_fields(row)
        row["splashback"] = _yellowwood_cleanup_splashback_text(row.get("splashback", ""), str(row.get("room_key", "") or ""))


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
        else _collect_room_overlays(documents, room_master_file=room_master_file, builder_name=str(snapshot.get("builder_name", "")))
    )
    for row in rooms:
        overlay = _match_room_overlay(row, overlays)
        if imperial_builder:
            row["bench_tops"] = _rebuild_benchtop_entries(row)
            row["door_panel_colours"] = _rebuild_door_panel_colours(row)
            row["handles"] = _clean_handle_entries(_coerce_string_list(row.get("handles", [])))
            row["floating_shelf"] = _string_value(row.get("floating_shelf", ""))
            row["led_note"] = _merge_led_note(row.get("led_note", ""))
            row["led"] = _normalize_led_value(row.get("led", ""), row["led_note"])
            row["accessories"] = _clean_accessory_entries(_coerce_string_list(row.get("accessories", [])))
            row["other_items"] = _merge_other_items([], row.get("other_items", []))
            row["sink_info"] = _clean_room_fixture_text(overlay.get("sink_info", "") or _string_value(row.get("sink_info", "")), "sink")
            row["basin_info"] = _clean_room_fixture_text(overlay.get("basin_info", "") or _string_value(row.get("basin_info", "")), "basin")
            row["tap_info"] = _clean_room_fixture_text(overlay.get("tap_info", "") or _string_value(row.get("tap_info", "")), "tap")
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
        row["toe_kick"] = _coerce_string_list(overlay.get("toe_kick", [])) or _coerce_string_list(row.get("toe_kick", []))
        row["bulkheads"] = _coerce_string_list(overlay.get("bulkheads", [])) or _coerce_string_list(row.get("bulkheads", []))
        row["handles"] = _clean_handle_entries(
            _coerce_string_list(overlay.get("handles", [])) or _coerce_string_list(row.get("handles", []))
        )
        row["floating_shelf"] = _merge_text(_string_value(row.get("floating_shelf", "")), overlay.get("floating_shelf", ""))
        row["shelf"] = _merge_text(_string_value(row.get("shelf", "")), overlay.get("shelf", ""))
        row["led_note"] = _merge_led_note(row.get("led_note", ""), overlay.get("led_note", ""))
        row["led"] = _normalize_led_value(overlay.get("led") or row.get("led", ""), row["led_note"])
        row["accessories"] = _merge_lists(_coerce_string_list(row.get("accessories", [])), _coerce_string_list(overlay.get("accessories", [])))
        row["other_items"] = _merge_other_items(row.get("other_items", []), overlay.get("other_items", []))
        row["sink_info"] = _merge_text(_string_value(row.get("sink_info", "")), overlay.get("sink_info", ""))
        row["basin_info"] = _merge_text(_string_value(row.get("basin_info", "")), overlay.get("basin_info", ""))
        row["tap_info"] = _merge_text(_string_value(row.get("tap_info", "")), overlay.get("tap_info", ""))
        if overlay.get("flooring"):
            row["flooring"] = _string_value(overlay.get("flooring", ""))
        row["drawers_soft_close"] = merge_soft_close_values(row.get("drawers_soft_close", ""), "")
        row["hinges_soft_close"] = merge_soft_close_values(row.get("hinges_soft_close", ""), "")
    _apply_builder_room_finalizer(str(snapshot.get("builder_name", "") or ""), rooms, overlays, documents)
    snapshot["rooms"] = rooms
    snapshot["appliances"] = [row for row in snapshot.get("appliances", []) if isinstance(row, dict) and not _is_room_fixture_appliance(row)]
    _clear_room_specific_flooring_notes(snapshot)
    _clear_room_specific_splashback_notes(snapshot)
    cleaned = apply_snapshot_cleaning_rules(snapshot, rule_flags=rule_flags)
    cleaned_rooms = [row for row in cleaned.get("rooms", []) if isinstance(row, dict)]
    _apply_builder_room_finalizer(str(cleaned.get("builder_name", "") or ""), cleaned_rooms, overlays, documents)
    cleaned["rooms"] = cleaned_rooms
    for row in cleaned["rooms"]:
        row["accessories"] = _filter_blacklisted_room_accessories(row.get("accessories", []))
        row["other_items"] = _filter_blacklisted_room_other_items(row.get("other_items", []))
        _promote_conditional_shelf_field(row)
    if _is_yellowwood_builder(str(cleaned.get("builder_name", "") or "")):
        for row in cleaned["rooms"]:
            row["accessories"] = _yellowwood_filter_accessories(row)
            row["other_items"] = _yellowwood_filter_other_items(row)
            _promote_conditional_shelf_field(row)
    _clear_room_specific_flooring_notes(cleaned)
    _clear_room_specific_splashback_notes(cleaned)
    return _apply_builder_material_room_gate(cleaned)


def _yellowwood_row_material_probe(row: dict[str, Any]) -> str:
    return normalize_space(" | ".join(_room_material_evidence_values(row)))


def _yellowwood_should_keep_final_room(row: dict[str, Any]) -> bool:
    label = source_room_label(
        str(row.get("original_room_label", "") or row.get("room_name", "") or row.get("room_key", "")),
        fallback_key=str(row.get("room_key", "")),
    )
    if not label or label == "Room":
        return False
    if _looks_like_spec_room_label_noise(label):
        return False
    if not _yellowwood_is_supported_room_label(label):
        return False
    return _room_has_material_evidence(row)


def _apply_builder_material_room_gate(snapshot: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(snapshot)
    builder_name = str(cleaned.get("builder_name", "") or "")
    if _is_yellowwood_builder(builder_name):
        cleaned["rooms"] = [row for row in cleaned.get("rooms", []) if isinstance(row, dict) and _yellowwood_should_keep_final_room(row)]
    else:
        cleaned["rooms"] = [row for row in cleaned.get("rooms", []) if isinstance(row, dict) and _room_has_material_evidence(row)]
    return cleaned


def _has_joinery_material_keyword(text: str) -> bool:
    normalized = normalize_space(text)
    if not normalized:
        return False
    return bool(
        re.search(
            r"(?i)\b(?:polytec|laminex|ydl|caesarstone|smartstone|wk stone|quantum quartz|silestone|"
            r"melamine|laminate|thermolaminate|thermolaminated|vinyl wrap|woodmatt|truescale|"
            r"natural finish|classic white|blackbutt|walnut|oak|polished|white melamine)\b",
            normalized,
        )
    )


def _extract_explicit_shelf_material_from_text(text: Any) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    lowered = normalized.lower()
    if "floating shelf" in lowered or "floating shelving" in lowered:
        return ""
    if re.search(r"(?i)\binternals?\b.{0,30}\bshelves?\b|\bshelves?\b.{0,30}\binternals?\b", normalized):
        return ""
    if not re.search(r"(?i)\b(?:open shelving|shelving|single shelf|double shelf|shelf material|shelves|shelf)\b", normalized):
        return ""
    shelf_match = re.search(r"(?i)\b(?:open shelving|shelving|single shelf|double shelf|shelf material|shelves|shelf)\b", normalized)
    if shelf_match:
        prefix = normalize_space(normalized[: shelf_match.start()])
        prefix = re.sub(r"(?i)\bwith\s+hanging\s+rail\b", "", prefix)
        prefix = re.sub(r"(?i)\bhanging\s+rail\b", "", prefix)
        prefix = re.sub(r"(?i)\byellowwood supplier\b", "", prefix)
        prefix = re.sub(r"(?i)\bas supplied by cabinetmaker\b", "", prefix)
        prefix = re.sub(r"(?i)\bas supplied by builder\b", "", prefix)
        prefix = re.sub(r"(?i)\b(?:robe fit out|fit out|as per plan|pantry|wip|linen cupboard)\b", "", prefix)
        prefix = re.sub(r"(?i)\bbed\s*\d+\b", "", prefix)
        prefix = re.sub(r"(?i)\bmaster\b", "", prefix)
        prefix = re.sub(r"(?i)\b(?:ground floor|upper[- ]level|upper[- ]floor)\b", "", prefix)
        prefix = re.sub(r"(?i)\b(?:walk[- ]in[- ]robe|walk in robe|wir|robe|ensuite|bathroom|powder room|laundry|kitchen)\b", "", prefix)
        prefix = re.sub(r"(?i)^(?:in\s+)+", "", prefix)
        prefix = normalize_space(prefix).strip(" -:|,")
        if prefix:
            prefix_tokens = prefix.split()
            for start in range(max(0, len(prefix_tokens) - 8), len(prefix_tokens)):
                candidate = normalize_space(" ".join(prefix_tokens[start:]))
                if _is_clean_material_phrase(candidate):
                    return normalize_brand_casing_text(candidate)
    candidate_parts = re.split(r"(?i)\b(?:open shelving|shelving|single shelf|double shelf|shelf material|shelves|shelf)\b", normalized)
    candidate = normalize_space(candidate_parts[-1] if candidate_parts else normalized)
    candidate = re.sub(r"(?i)\bwith\s+hanging\s+rail\b", "", candidate)
    candidate = re.sub(r"(?i)\bhanging\s+rail\b", "", candidate)
    candidate = re.sub(r"(?i)\byellowwood supplier\b", "", candidate)
    candidate = re.sub(r"(?i)\bas supplied by cabinetmaker\b", "", candidate)
    candidate = re.sub(r"(?i)\bas supplied by builder\b", "", candidate)
    candidate = re.sub(r"(?i)\b(?:robe fit out|fit out|pantry|wip|linen cupboard)\b", "", candidate)
    candidate = _trim_fixture_text_at_markers(
        candidate,
        (
            r"\bOVERHEAD HANDLES\b",
            r"\bBASE CABINET HANDLES\b",
            r"\bPANTRY DOOR HANDLES\b",
            r"\bHANDLES?\b",
            r"\bSINK(?:\s+INFO)?\b",
            r"\bBASIN(?:\s+INFO)?\b",
            r"\bTAP(?:\s+INFO)?\b",
            r"\bTOE KICK\b",
            r"\bKICKBOARDS?\b",
            r"\bBULKHEADS?\b",
            r"\bSPLASHBACK\b",
            r"\bBENCH\s*TOPS?\b",
            r"\bOVERHEAD CUPBOARDS?\b",
            r"\bBASE CUPBOARDS?\b",
            r"\bINTERNAL FINISHES\b",
            r"\bTILING SCHEDULE\b",
            r"\bREFER TO NATIONAL TILES\b",
            r"\bLAUNDRY\b",
            r"\bGROUND FLOOR\b",
            r"\bUPPER[- ]LEVEL\b",
            r"\bUPPER[- ]FLOOR\b",
            r"\bBATHROOM\b",
            r"\bENSUITE\b",
            r"\bPOWDER ROOM\b",
        ),
    )
    candidate = normalize_space(candidate).strip(" -:|,")
    candidate = _collapse_pipe_text_variants(candidate)
    if not candidate or not _has_joinery_material_keyword(candidate):
        return ""
    return normalize_brand_casing_text(candidate)


def _is_clean_material_phrase(text: str) -> bool:
    normalized = normalize_space(text)
    if not normalized or not _has_joinery_material_keyword(normalized):
        return False
    if len(normalized.split()) > 10:
        return False
    if re.search(r"(?i)\bx\s*\d+\b|\b\d+\s*x\b", normalized):
        return False
    if re.search(
        r"(?i)\b(?:handles?|lip pull|sink|basin|tap|toe\s*kick|bulkheads?|splashback|"
        r"surrounds?|internals?|drawer|cabinet|overhead|base|tall|bench(?:top)?|rail|kickboards?|"
        r"bed\s*\d+|walk(?:\s+in)?|robe|pantry|powder|ensuite|bathroom|kitchen|laundry|vanity)\b",
        normalized,
    ):
        return False
    return True


def _other_item_is_actual_rail(item: dict[str, str]) -> bool:
    label = normalize_space(str(item.get("label", "") or ""))
    value = normalize_space(str(item.get("value", "") or ""))
    if label.upper() != "RAIL" or not value:
        return False
    if re.search(r"(?i)\b(?:open shelving|shelving|shelves|shelf)\b", value):
        return False
    return bool(
        re.search(
            r"(?i)\b(?:rail|recessed rail|hanging rail|wardrobe rail|wardrobe tube|oval wardrobe tube|tube)\b",
            value,
        )
    )


def _promote_conditional_shelf_field(row: dict[str, Any]) -> None:
    shelf_value = normalize_space(str(row.get("shelf", "") or ""))
    if shelf_value:
        cleaned_parts: list[str] = []
        for part in re.split(r"\s*\|\s*", shelf_value):
            part = normalize_space(re.sub(r"(?i)^(?:in\s+)+", "", part))
            if not part:
                continue
            cleaned_existing = _extract_explicit_shelf_material_from_text(part)
            if cleaned_existing:
                cleaned_parts.append(cleaned_existing)
                continue
            if _is_clean_material_phrase(part):
                cleaned_parts.append(normalize_brand_casing_text(part))
        shelf_value = " | ".join(_unique(cleaned_parts))
    candidate_texts: list[str] = [normalize_space(str(row.get("evidence_snippet", "") or ""))]
    filtered_other_items: list[dict[str, str]] = []
    for item in _merge_other_items([], row.get("other_items", [])):
        label = normalize_space(str(item.get("label", "") or ""))
        value = normalize_space(str(item.get("value", "") or ""))
        if not label or not value:
            continue
        if label.upper() == "RAIL" and not _other_item_is_actual_rail(item):
            candidate_texts.append(value)
            continue
        filtered_other_items.append({"label": label, "value": value})
    if not shelf_value:
        for candidate in candidate_texts:
            extracted = _extract_explicit_shelf_material_from_text(candidate)
            if extracted:
                shelf_value = extracted
                break
    row["shelf"] = shelf_value
    row["other_items"] = filtered_other_items


def _yellowwood_wet_area_tail_markers() -> tuple[str, ...]:
    return (
        r"\bShower Floor Waste\b",
        r"\bShower on Rail\b",
        r"\bShower Screen\b",
        r"\bShower Mixer\b",
        r"\bShower Rose\b",
        r"\bBath(?: Mixer| Spout| Waste)?\b",
        r"\bTowel Rail\b",
        r"\bHand Towel Rail\b",
        r"\bToilet Roll Holder\b",
        r"\bToilet Suite\b",
        r"\bToilet\b",
        r"\bSink Mixer\b",
        r"\bBasin(?: Waste)?\b",
        r"\bBottle Trap\b",
        r"\bBED\s*\d+\s+ENSUITE\b",
        r"\bBATHROOM\b",
    )


def _yellowwood_filter_accessories(row: dict[str, Any]) -> list[str]:
    filtered: list[str] = []
    room_label = source_room_label(
        str(row.get("original_room_label", "") or row.get("room_name", "") or row.get("room_key", "")),
        fallback_key=str(row.get("room_key", "")),
    )
    is_vanity_room = "vanity" in room_label.lower()
    drop_prefixes = (
        "shower floor waste",
        "shower on rail",
        "shower mixer",
        "bath mixer",
        "bath spout",
        "bath waste",
        "sink mixer",
        "basin waste",
    )
    for value in _coerce_string_list(row.get("accessories", [])):
        cleaned = normalize_space(value)
        if is_vanity_room and cleaned:
            cleaned = _trim_fixture_text_at_markers(cleaned, _yellowwood_wet_area_tail_markers())
        lowered = cleaned.lower()
        if not cleaned or any(lowered.startswith(prefix) for prefix in drop_prefixes) or _is_blacklisted_wet_area_label(cleaned):
            continue
        filtered.append(cleaned)
    return _filter_blacklisted_room_accessories(filtered)


def _yellowwood_filter_other_items(row: dict[str, Any]) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    tap_present = bool(normalize_space(str(row.get("tap_info", "") or "")))
    room_label = source_room_label(
        str(row.get("original_room_label", "") or row.get("room_name", "") or row.get("room_key", "")),
        fallback_key=str(row.get("room_key", "")),
    )
    is_vanity_room = "vanity" in room_label.lower()
    is_fitout_or_pantry_room = bool(re.search(r"(?i)\b(?:pantry|robe fit out|walk in robe fit out)\b", room_label))
    for item in _merge_other_items([], row.get("other_items", [])):
        label = normalize_space(str(item.get("label", "") or ""))
        value = normalize_space(str(item.get("value", "") or ""))
        if is_vanity_room and value:
            value = _trim_fixture_text_at_markers(value, _yellowwood_wet_area_tail_markers())
        if not label or not value:
            continue
        if tap_present and label.lower() in {"mixer", "pull-out mixer", "sink mixer", "basin mixer"}:
            continue
        if is_fitout_or_pantry_room and re.search(r"(?i)\b(?:mixer|sink|basin|tap|waste|floor waste|shower|bath|toilet)\b", f"{label} {value}"):
            continue
        if _is_blacklisted_wet_area_label(label) or _is_blacklisted_wet_area_label(value) or _is_blacklisted_wet_area_label(f"{label} {value}"):
            continue
        filtered.append({"label": label, "value": value})
    return _filter_blacklisted_room_other_items(filtered)


def apply_snapshot_cleaning_rules(snapshot: dict[str, Any], rule_flags: Any = None) -> dict[str, Any]:
    flags = cleaning_rules.normalize_rule_flags(rule_flags)
    cleaned = dict(snapshot)
    cleaned["rooms"] = [_apply_room_cleaning_rules(dict(row), flags) for row in snapshot.get("rooms", []) if isinstance(row, dict)]
    for row in cleaned["rooms"]:
        row["accessories"] = _filter_blacklisted_room_accessories(row.get("accessories", []))
        row["other_items"] = _filter_blacklisted_room_other_items(row.get("other_items", []))
        _promote_conditional_shelf_field(row)
    if _is_yellowwood_builder(str(cleaned.get("builder_name", "") or "")):
        for row in cleaned["rooms"]:
            row["accessories"] = _yellowwood_filter_accessories(row)
            row["other_items"] = _yellowwood_filter_other_items(row)
            _promote_conditional_shelf_field(row)
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
    row["room_name"] = row["original_room_label"] or _display_rule_text(row.get("room_name", ""), rule_flags)
    row["bench_tops"] = _normalize_text_list(row.get("bench_tops", []), rule_flags)
    row["toe_kick"] = _normalize_text_list(row.get("toe_kick", []), rule_flags)
    row["bulkheads"] = _normalize_text_list(row.get("bulkheads", []), rule_flags)
    row["handles"] = _normalize_text_list(_clean_handle_entries(_coerce_string_list(row.get("handles", []))), rule_flags)
    row["floating_shelf"] = _display_rule_text(row.get("floating_shelf", ""), rule_flags)
    row["shelf"] = _display_rule_text(row.get("shelf", ""), rule_flags)
    row["led_note"] = _display_rule_text(_merge_led_note(row.get("led_note", "")), rule_flags)
    row["led"] = _normalize_led_value(row.get("led", ""), row["led_note"])
    row["accessories"] = _normalize_text_list(row.get("accessories", []), rule_flags)
    row["other_items"] = [
        {
            "label": _display_rule_text(item.get("label", ""), rule_flags),
            "value": _display_rule_text(item.get("value", ""), rule_flags),
        }
        for item in _merge_other_items([], row.get("other_items", []))
        if _display_rule_text(item.get("label", ""), rule_flags) and _display_rule_text(item.get("value", ""), rule_flags)
    ]
    _promote_conditional_shelf_field(row)
    row["sink_info"] = _clean_room_fixture_text(_display_rule_text(row.get("sink_info", ""), rule_flags), "sink")
    row["basin_info"] = _clean_room_fixture_text(_display_rule_text(row.get("basin_info", ""), rule_flags), "basin")
    row["tap_info"] = _clean_room_fixture_text(_display_rule_text(row.get("tap_info", ""), rule_flags), "tap")
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


def _collect_room_overlays(
    documents: list[dict[str, object]],
    room_master_file: str = "",
    builder_name: str = "",
) -> dict[str, dict[str, str]]:
    overlays: dict[str, dict[str, str]] = {}
    normalized_builder = normalize_space(builder_name).lower()
    for document in documents:
        file_name = str(document.get("file_name", ""))
        full_text = "\n\n".join(
            str(page.get("text") or page.get("raw_text") or "")
            for page in document.get("pages", [])
            if page.get("text") or page.get("raw_text")
        )
        if not full_text.strip():
            continue
        sections = _collect_room_sections_for_document(document, builder_name_override=builder_name)
        material_allowed = not room_master_file or file_name == room_master_file
        for section in sections:
            chunk = str(section.get("text", "") or "")
            detected_room_key = str(section.get("section_key", "") or "")
            room_label = source_room_label(str(section.get("original_section_label", "")), fallback_key=detected_room_key)
            room_key = source_room_key(room_label, fallback_key=detected_room_key)
            lines = _section_lines(section)
            section_page_type = normalize_space(str(section.get("page_type", "") or "")).lower().replace(" ", "_")
            section_material_allowed = material_allowed and section_page_type in {"", "joinery"}
            overlay = overlays.setdefault(room_key, _blank_overlay())
            if section_material_allowed:
                generic_bench_tops = _collect_field(lines, ["Bench Tops", "Benchtop"])
                explicit_bench_values = _unique(
                    [
                        *(f"Back Benchtops {value}" for value in _collect_field(lines, ["Back Benchtops", "Wall Run Bench Top"])),
                        *(f"Island Benchtop {value}" for value in _collect_island_benchtop_values(lines)),
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
                overlay["toe_kick"] = _merge_lists(
                    _coerce_string_list(overlay.get("toe_kick", [])),
                    _collect_field(lines, ["Toe Kick", "Kickboard", "Island Bench Kickboard"]),
                )
                overlay["bulkheads"] = _merge_lists(
                    _coerce_string_list(overlay.get("bulkheads", [])),
                    _collect_explicit_bulkhead_values(lines),
                )
                overlay["handles"] = _merge_lists(
                    _coerce_string_list(overlay.get("handles", [])),
                    _clean_handle_entries(_collect_field(lines, ["Handles", "Handle", "Base Cabinet Handles", "Overhead Handles", "Pantry Door Handles"])),
                )
                overlay["floating_shelf"] = _merge_text(overlay["floating_shelf"], _first_value(_collect_field(lines, ["Floating Shelves", "Floating Shelf"])))
                overlay["shelf"] = _merge_text(overlay["shelf"], _extract_explicit_shelf_material_from_text(chunk))
                led_note = _extract_led_note_from_lines(lines)
                overlay["led_note"] = _merge_led_note(overlay.get("led_note", ""), led_note)
                overlay["led"] = _normalize_led_value(overlay.get("led", ""), overlay["led_note"])
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
    if normalized_builder == "clarendon":
        _collect_clarendon_flooring_overlays(overlays, documents)
    elif _is_yellowwood_builder(builder_name):
        _collect_yellowwood_flooring_overlays(overlays, documents)
        _collect_yellowwood_fixture_overlays(overlays, documents)
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
        "shelf": "",
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
        "led_note": "",
        "accessories": [],
        "other_items": [],
        "sink_info": "",
        "basin_info": "",
        "tap_info": "",
        "flooring": "",
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
    text = re.sub(r"[每]+", " ", text)
    text = re.sub(r"(?<=\w)\?(?=\w)", " ", text)
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
        if normalized_entry.lower() == "house":
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
    merged_fragments: list[str] = []
    index = 0
    while index < len(filtered):
        entry = filtered[index]
        if (
            index + 1 < len(filtered)
            and entry.rstrip().endswith("/")
            and re.match(r"(?i)^(?:doors|drawers)\s*-", filtered[index + 1])
        ):
            merged_fragments.append(normalize_space(f"{entry.rstrip()}{filtered[index + 1]}"))
            index += 2
            continue
        merged_fragments.append(entry)
        index += 1
    return _unique(merged_fragments)


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
    text = re.sub(r"[每]+", " ", text)
    text = re.sub(r"(?<=\w)\?(?=\w)", " ", text)
    text = re.sub(r"(?<=\w)\s*[^\w\s/&()'.,:-]+\s*(?=\w)", " ", text)
    text = text.replace("每", " ")
    text = re.sub(r"(?i)\bas supplied by (?:cabinetmaker|builder)\b", "", text)
    if any(re.search(pattern, text, re.IGNORECASE) for pattern in CABINET_ONLY_EXCLUDE_PATTERNS):
        return ""
    token_pattern = "|".join(re.escape(token) for token in DOOR_CONTEXT_TOKENS)
    text = re.sub(rf"(?i)\s+\b(?:to|for)\b\s+.*(?:{token_pattern}).*$", "", text)
    text = re.sub(rf"(?i)\s+-\s*(?:{token_pattern}).*$", "", text)
    text = re.sub(rf"(?i)(?<=\w)\s+(?:{token_pattern}).*$", "", text)
    text = re.sub(rf"(?i)\s*\((?:{token_pattern}).*$", "", text)
    text = re.sub(r"\s*\([^)]*$", "", text)
    if re.match(r"(?i)^\*?\s*to builders\b", text):
        material_match = re.search(
            r"(?i)\b(?:polytec|laminex|caesarstone|smartstone|wk stone|ydl|melamine|laminate|thermolaminate|classic white|natural finish|white melamine)\b",
            text,
        )
        if material_match:
            text = text[material_match.start() :]
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
            if any(token in lowered for token in ["tall cabinetry", "tall cabinet", "tall cabinets", "tall door", "tall doors", "tall panel", "tall panels", "tall open shelf", "tall open shelves"]):
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


def _trim_fixture_text_at_markers(text: str, markers: tuple[str, ...]) -> str:
    trimmed = normalize_space(text)
    for marker in markers:
        match = re.search(marker, trimmed, re.IGNORECASE)
        if match and match.start() > 0:
            trimmed = normalize_space(trimmed[: match.start()])
            break
    return trimmed.strip(" -;,")


def _clean_room_fixture_text(value: Any, kind: str) -> str:
    text = _clean_fixture_text(value)
    if not text:
        return ""
    text = re.sub(r"(?i)\bonly\s+refer\s+to\s+[\"“”'`]?plumbing[\"“”'`]? section below\b", "", text)
    text = normalize_space(re.sub(r"(?i)\bn/?a\b", "", text)).strip(" -;,")
    if not text:
        return ""
    basin_bath_combo = bool(re.search(r"(?i)\b(?:wall\s+)?basin\s*/\s*bath mixer\b", text))
    wet_area_tail_markers = (
        r"\bBasin Waste\b",
        r"\bSink Waste\b",
        r"\bWaste\b",
        r"\bBottle Trap\b",
        r"\bToilet Roll Holder\b",
        r"\bToilet Suite\b",
        r"\bToilet\b",
        r"\bFloor Waste\b",
        r"\bFeature Waste\b",
        r"\bHand Towel Rail\b",
        r"\bTowel Rail\b",
        r"\bHand Towel Hooks?\b",
        r"\bBath Towel Hooks?\b",
        r"\bTowel Hooks?\b",
        r"\bRobe Hooks?\b",
        r"\bShower Screen\b",
        r"\bShower Base\b",
        r"\bShower Frame\b",
        r"\bShower on Rail\b",
        r"\bShower Rose\b",
        r"\bShower\b",
        r"\bSemi[- ]Frameless\b",
        r"\bFrameless\b",
        r"\bGlazing\b",
        r"\bHandle\b",
        r"\bPop-?up\b",
        r"\bMirror\b",
    )
    if kind == "sink":
        if re.match(r"(?i)^(?:sink mixer|pull-?out mixer|basin mixer|mixer)\b", text):
            return ""
        text = _trim_fixture_text_at_markers(text, wet_area_tail_markers + (r"\bBath\b",))
    elif kind == "basin":
        if re.match(r"(?i)^(?:waste\b|pop up waste\b|bottle trap\b|mixer\b|sink mixer\b|pull-?out mixer\b|tap\b)", text):
            return ""
        text = _trim_fixture_text_at_markers(text, wet_area_tail_markers + (r"\bBath\b",))
        if text.lower().endswith(" basin") and " basin " in text[:-6].lower():
            text = text[:-6].rstrip(" -;,")
    elif kind == "tap":
        if re.match(r"(?i)^(?:basin waste\b|bottle trap\b|toilet(?: roll holder)?\b|floor waste\b|mirror\b|shower(?: mixer| screen| floor waste| on rail| rose)?\b|bath(?: mixer| spout| waste)?\b|bath\b|towel rail\b|hand towel rail\b)", text):
            return ""
        basin_mixer_match = re.search(r"(?i)\b(?:tall\s+)?basin(?:\s*/\s*bath)?\s+mixer\b.*", text)
        if basin_mixer_match:
            prefix = normalize_space(text[: basin_mixer_match.start()])
            if prefix and re.search(r"(?i)\b(?:in-wall mixer|shower mixer|bath mixer|bath spout|bath waste)\b", prefix):
                cut_start = text.rfind(" - ", 0, basin_mixer_match.start())
                if cut_start != -1:
                    text = normalize_space(text[cut_start + 3 :])
                else:
                    text = normalize_space(text[basin_mixer_match.start() :])
                basin_bath_combo = bool(re.search(r"(?i)\b(?:wall\s+)?basin\s*/\s*bath mixer\b", text))
        tap_markers: list[str] = list(wet_area_tail_markers)
        if not basin_bath_combo:
            tap_markers.append(r"\bBath\b")
        text = _trim_fixture_text_at_markers(
            text,
            tuple(tap_markers),
        )
    return text.strip(" -;,")


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
    model_to_make = {
        row.model_no.lower(): row.make
        for row in result
        if row.model_no and row.make
    }
    if model_to_make:
        for row in result:
            if not row.make and row.model_no:
                inferred_make = model_to_make.get(row.model_no.lower(), "")
                if inferred_make:
                    row.make = inferred_make
    typed_make_with_model = {
        (row.appliance_type.lower(), row.make.lower())
        for row in result
        if row.make and row.model_no
    }
    filtered: list[ApplianceRow] = []
    typed_source_with_concrete_model = {
        (row.source_file.lower(), row.appliance_type.lower())
        for row in result
        if row.source_file and row.model_no and not _looks_like_appliance_placeholder_model(row.model_no)
    }
    typed_with_concrete_model = {
        row.appliance_type.lower()
        for row in result
        if row.appliance_type and row.model_no and not _looks_like_appliance_placeholder_model(row.model_no)
    }
    for row in result:
        typed_make = (row.appliance_type.lower(), row.make.lower())
        if row.make and not row.model_no and typed_make in typed_make_with_model:
            continue
        if row.model_no and _looks_like_drawing_dimension_noise_model(row.model_no):
            if typed_make in typed_make_with_model or (
                row.source_file and (row.source_file.lower(), row.appliance_type.lower()) in typed_source_with_concrete_model
            ) or row.appliance_type.lower() in typed_with_concrete_model:
                continue
        if (
            row.source_file
            and row.model_no
            and _looks_like_appliance_placeholder_model(row.model_no)
            and (row.source_file.lower(), row.appliance_type.lower()) in typed_source_with_concrete_model
        ):
            continue
        if row.model_no and (
            row.model_no.upper().startswith("WWW.")
            or "HTTP" in row.model_no.upper()
            or row.model_no.upper().startswith("FRAME")
            or row.model_no.upper().startswith("KIT")
            or re.fullmatch(r"[A-F0-9-]{16,}", row.model_no.upper())
        ):
            if (row.source_file.lower(), row.appliance_type.lower()) in typed_source_with_concrete_model:
                continue
        filtered.append(row)
    return filtered


def _looks_like_drawing_dimension_noise_model(text: str) -> bool:
    model = normalize_space(str(text or "")).upper()
    if len(model) < 16:
        return False
    if "/" in model or "-" in model:
        return False
    digit_count = sum(char.isdigit() for char in model)
    alpha_count = sum(char.isalpha() for char in model)
    if digit_count < 10 or alpha_count > 8:
        return False
    if re.search(r"(?:\d{3,4}[A-Z]{0,3}){3,}", model):
        return True
    if re.search(r"\d{5,}[A-Z]{1,3}\d{5,}", model):
        return True
    return False
