# BUG H Recon Report

Date: 2026-04-25  
Scope: reconnaissance only. No production code changes, no tests, no deploy, no commits.

## Executive summary

- **Job 73 is confirmed as** `37558-2 - Lot 532 Sandpiper Terrace, Worongary` on live prod.
- **Comparison job used:** **job 64** (`38146.2 - Lot 1041 Rufous Street, Worongary`), because the task explicitly prefers 64 when available.
- **Recommendation:** **Option A - sibling repo fix recommended.**
- Reason: the merged KITCHEN rows in job 73 are **shape (b)**, not shape (a). The label side is merged, but the value side is not already cleanly segmented into one segment per original PDF row. A main-repo adapter split would have to do semantic guessing on rows like `KICKBOARDS\nGPO'S` and `BIN\nACCESSORIES\nLED's`, which is not safe.

## 1. Retrieval method

### 1.1 Live identity and visible-row verification

This path is reproducible from the current machine and does not mutate prod data.

```powershell
@'
import requests, re
from bs4 import BeautifulSoup

base = "https://spec.lxtransport.online"
s = requests.Session()
login = s.get(base + "/login", timeout=30)
csrf = re.search(r'name="csrf_token" value="([^"]+)"', login.text).group(1)
s.post(
    base + "/login",
    data={"username": "admin", "password": "admin", "csrf_token": csrf},
    timeout=30,
)

for job_id in (73, 64, 67):
    r = s.get(f"{base}/jobs/{job_id}", timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    print(job_id, soup.find("h2").get_text(" ", strip=True))

r = s.get(base + "/jobs/73/spec-list", timeout=30)
print(r.status_code)
print("KITCHEN" in r.text, "PANTRY" in r.text, "LAUNDRY & MUD ROOM" in r.text, "BED1&2 & RUMPUS" in r.text)
'@ | .\.venv\Scripts\python.exe -
```

### 1.2 Preferred prod snapshot retrieval path

This was the direct store path used earlier in this same Bug H session before context compaction. It is the correct reproducible method if Jason wants to re-run the raw dump with prod access available.

```powershell
$env:SPEC_EXTRACTION_DEPLOY_PASSWORD = "<ubuntu password>"
@'
import os, json, paramiko

host = "43.160.209.86"
user = "ubuntu"
password = os.environ["SPEC_EXTRACTION_DEPLOY_PASSWORD"]

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=host, username=user, password=password, timeout=30)

command = r"""cd /opt/spec-extraction && .venv/bin/python - <<'PY'
import os, json
os.environ['SPEC_EXTRACTION_DATA_DIR'] = '/var/lib/spec-extraction'
from App.services import store
snap = store.get_snapshot(73, 'raw_spec')
print(json.dumps(snap['data'], ensure_ascii=False, indent=2))
PY"""

stdin, stdout, stderr = client.exec_command(command, timeout=120)
print(stdout.read().decode("utf-8", errors="replace"))
print(stderr.read().decode("utf-8", errors="replace"))
client.close()
'@ | .\.venv\Scripts\python.exe -
```

Equivalent prod metadata check via SQLite:

```powershell
$env:SPEC_EXTRACTION_DEPLOY_PASSWORD = "<ubuntu password>"
@'
import os, json, paramiko

host = "43.160.209.86"
user = "ubuntu"
password = os.environ["SPEC_EXTRACTION_DEPLOY_PASSWORD"]

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=host, username=user, password=password, timeout=30)

command = r"""python3 - <<'PY'
import sqlite3, json
conn = sqlite3.connect('/var/lib/spec-extraction/spec_extraction.sqlite3')
conn.row_factory = sqlite3.Row
job = conn.execute('select * from jobs where id=?', (73,)).fetchone()
builder = conn.execute('select * from builders where id=?', (job['builder_id'],)).fetchone()
print(json.dumps({'job': dict(job), 'builder': dict(builder)}, ensure_ascii=False, indent=2))
PY"""

stdin, stdout, stderr = client.exec_command(command, timeout=120)
print(stdout.read().decode("utf-8", errors="replace"))
print(stderr.read().decode("utf-8", errors="replace"))
client.close()
'@ | .\.venv\Scripts\python.exe -
```

### 1.3 Comparison-job cached baseline extraction

Local cached baseline file used for the comparison KITCHEN raw snapshot extract:

```powershell
@'
import json
from pathlib import Path

data = json.loads(Path("tmp/baseline_snapshots/job_64_raw_spec.json").read_text(encoding="utf-8"))
kitchen = next(room for room in data["rooms"] if room.get("original_room_label") == "KITCHEN")
print(json.dumps(kitchen["material_rows"], ensure_ascii=False, indent=2))
'@ | .\.venv\Scripts\python.exe -
```

## 2. Job IDs verified

- **Prod job 73** live header: `37558-2 - Lot 532 Sandpiper Terrace, Worongary`
- Builder: `Imperial`
- Live parser mode on job 73 detail page: `Imperial V6`
- Live parser strategy on job 73 detail page: `Imperial v6`
- Room-master file shown on job 73 detail page: `Colour Selections - 15.12.25 - Lot 532 Sandpiper Terrace.pdf`

Comparison-job identity:

- **Used:** **job 64**
- Live header: `38146.2 - Lot 1041 Rufous Street, Worongary`
- Builder: `Imperial`
- Live parser strategy: `Global Conservative`

Why job 64 instead of 67:

- the task says to use **job 64 if available**
- job 64 is available and has a plain `KITCHEN` room
- live job 67 exists, but its room label is `KITCHEN & PANTRY`, which is a worse side-by-side KITCHEN comparison for this Bug H question

## 3. Raw evidence dumps

### 3.1 Prod job 73: direct snapshot extracts retained from the earlier in-session store dump

These are the exact key rows preserved from the earlier direct prod snapshot read in this Bug H session. They are the decisive rows for the Option A vs B decision.

```json
{
  "job_id": 73,
  "room": "KITCHEN",
  "critical_merged_rows": [
    {
      "area_or_item": "BENCHTOP\nISLAND CABINETRY COLOUR (incl. BACK OF\nISLAND CURVE AND COLUMN)",
      "supplier": "Polytec",
      "specs_or_description": "By builder 40mm stone - Arissed Notaio Walnut Woodmatt",
      "notes": "",
      "display_lines": [
        "By builder - 40mm stone - Arissed",
        "Polytec - Notaio Walnut Woodmatt"
      ],
      "tags": [
        "bench_tops"
      ],
      "page_no": 1,
      "row_order": 1,
      "provenance": {
        "source_provider": "v6",
        "source_extractor": "pdf_to_structured_json_v6",
        "raw": "v6_cell"
      }
    },
    {
      "area_or_item": "KICKBOARDS\nGPO'S",
      "supplier": "Polytec",
      "specs_or_description": "AS DOORS Island Drawer GPO 1 - Side of Upper Bin Draw 'By Builder'(see pic 1 shown) Island Drawer GPO 2 - Rear panel of Utensil Drawer - Hafele Trio 822.53.151 'By Imperial' (See pic 2 shown)",
      "notes": "",
      "display_lines": [
        "Polytec - AS DOORS Island Drawer GPO 1 - Side of Upper Bin Draw 'By Builder'(see pic 1 shown) Island Drawer GPO 2 - Rear panel of Utensil Drawer - Hafele Trio 822.53.151 'By Imperial' (See pic 2 shown)"
      ],
      "tags": [
        "other_material"
      ],
      "page_no": 1,
      "row_order": 4
    },
    {
      "area_or_item": "BIN\nACCESSORIES\nLED's",
      "supplier": "Hettich\nFurnware",
      "specs_or_description": "450MM (SHORT) - 2 X 29LTR Veronar, Spice Tray Insert, To Suit 450mm & 600MM Drawer, White - VCT.450E.WH.FGx1 & VCT.600E.WH.FGx1 As per drawings",
      "notes": "",
      "display_lines": [
        "*Hettich / Furnware* - 450MM (SHORT) - 2 X 29LTR Veronar, Spice Tray Insert, To Suit 450mm & 600MM Drawer, White - VCT.450E.WH.FGx1 & VCT.600E.WH.FGx1 As per drawings"
      ],
      "tags": [
        "other_material"
      ],
      "page_no": 1,
      "row_order": 6
    }
  ],
  "room_count_note": "The earlier direct prod snapshot inspection showed 10 material_rows in KITCHEN: 6 canonical rows plus 4 synthesized handle backfill rows."
}
```

```json
{
  "job_id": 73,
  "room": "BED1&2 & RUMPUS",
  "critical_bug_i_row": {
    "area_or_item": "LED'S (BED 2)\nHANDLES",
    "supplier": "",
    "specs_or_description": "As per drawings | BASE- BEVEL EDGE FINGERPULL | UPPER - FINGERPULL, TALL - PTO",
    "notes": "",
    "display_lines": [
      "As per drawings",
      "BASE- BEVEL EDGE FINGERPULL",
      "UPPER - FINGERPULL, TALL - PTO"
    ],
    "tags": [
      "handles"
    ],
    "page_no": 1,
    "row_order": 7
  },
  "synthetic_followups_note": "The earlier direct prod snapshot inspection also showed rows 8-11 as synthesized handle backfill rows derived from room-level handles."
}
```

```json
{
  "job_id": 73,
  "room": "BED1&2 & RUMPUS",
  "selected_room_level_fields_from_direct_prod_snapshot": {
    "handles": [
      "BASE- BEVEL EDGE FINGERPULL",
      "UPPER - FINGERPULL, TALL - PTO"
    ],
    "flooring": "Hybrid",
    "led": "Yes",
    "led_note": ""
  }
}
```

### 3.2 Prod job 73: current authenticated live spec-list extracts for the 4 requested rooms

These are the full visible room-row extracts from the current live `spec-list` page. They are not a substitute for the store dump above, but they do confirm the exact merged titles currently rendered on prod.

```json
{
  "KITCHEN": {
    "visible_room_rows": [
      {
        "area_or_item": "BENCHTOP\nISLAND CABINETRY COLOUR (incl. BACK OF\nISLAND CURVE AND COLUMN)",
        "display_lines": [
          "By builder - 40mm stone - Arissed",
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "BACK WALL & COFFEE NOOK INTERNAL\nCABINETRY COLOUR",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "FLOATING SHELVES",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "KICKBOARDS\nGPO'S",
        "display_lines": [
          "Polytec - AS DOORS Island Drawer GPO 1 - Side of Upper Bin Draw 'By Builder'(see pic 1 shown) Island Drawer GPO 2 - Rear panel of Utensil Drawer - Hafele Trio 822.53.151 'By Imperial' (See pic 2 shown)"
        ]
      },
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "BASE- BEVEL EDGE FINGERPULL",
          "UPPER - FINGERPULL",
          "TALL - PTO"
        ]
      },
      {
        "area_or_item": "BIN\nACCESSORIES\nLED's",
        "display_lines": [
          "*Hettich / Furnware* - 450MM (SHORT) - 2 X 29LTR Veronar, Spice Tray Insert, To Suit 450mm & 600MM Drawer, White - VCT.450E.WH.FGx1 & VCT.600E.WH.FGx1 As per drawings"
        ]
      },
      {
        "area_or_item": "Drawers",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Hinges",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Flooring",
        "display_lines": [
          "tiled"
        ]
      },
      {
        "area_or_item": "Sink",
        "display_lines": [
          "undermount - specs tbc By Others - Taphole location: Ctr of sink"
        ]
      }
    ],
    "room_meta": {
      "Source": "Colour Selections - 15.12.25 - Lot 532 Sandpiper Terrace.pdf",
      "Pages": "1",
      "Confidence": "0.85",
      "Evidence": "BENCHTOP ISLAND CABINETRY COLOUR (incl. BACK OF ISLAND CURVE AND COLUMN) 40mm stone - Arissed Notaio Walnut Woodmatt VERTICAL GRAIN\nBACK WALL & COFFEE NOOK INTERNAL CABINETRY COLOUR Notaio Walnut Woodmatt VERTICAL GRAIN\nFLOATING SHELVES Notaio Walnut Woodmatt VERTICAL GRAIN\nKICKBOARDS GPO'S AS DOORS"
    }
  }
}
```

```json
{
  "PANTRY": {
    "visible_room_rows": [
      {
        "area_or_item": "BENCHTOP",
        "display_lines": [
          "BY BUILDER - 40MM STONE - Arissed"
        ]
      },
      {
        "area_or_item": "BASE CABINETRY COLOUR",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "UPPER CABINETRY COLOUR",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "TALL CABINETRY COLOUR",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "KICKBOARDS",
        "display_lines": [
          "Polytec - AS DOORS"
        ]
      },
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "BASE- BEVEL EDGE FINGERPULL",
          "UPPER - FINGERPULL",
          "TALL - PTO"
        ]
      },
      {
        "area_or_item": "LED'S",
        "display_lines": [
          "As per drawings"
        ]
      },
      {
        "area_or_item": "Drawers",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Hinges",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Flooring",
        "display_lines": [
          "Tiled"
        ]
      },
      {
        "area_or_item": "Sink",
        "display_lines": [
          "undermount - specs tbc By Others - Taphole location: Centre of Sink / Sink Pre-punched Hole"
        ]
      }
    ],
    "room_meta": {
      "Source": "Colour Selections - 15.12.25 - Lot 532 Sandpiper Terrace.pdf",
      "Pages": "3",
      "Confidence": "0.85",
      "Evidence": "BENCHTOP 40MM STONE - Arissed\nBASE CABINETRY COLOUR Notaio Walnut Woodmatt VERTICAL GRAIN\nUPPER CABINETRY COLOUR Notaio Walnut Woodmatt VERTICAL GRAIN\nTALL CABINETRY COLOUR Notaio Walnut Woodmatt VERTICAL GRAIN\nKICKBOARDS AS DOORS\nHANDLES BASE- BEVEL EDGE FINGERPULL UPPER - FINGERPULL TALL - PTO\nLED"
    }
  }
}
```

```json
{
  "LAUNDRY & MUD ROOM": {
    "visible_room_rows": [
      {
        "area_or_item": "BENCHTOP\nBENCHTOP (SEAT)",
        "display_lines": [
          "BY BUILDER - laundry - 20MM STONE",
          "Polytec - Mud room - 33mm Notaio Walnut laminate"
        ]
      },
      {
        "area_or_item": "BASE CABINETRY COLOUR",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "UPPER CABINETRY COLOUR",
        "display_lines": [
          "Polytec - Notaio Walnut Woodmatt"
        ]
      },
      {
        "area_or_item": "KICKBOARDS",
        "display_lines": [
          "Polytec - AS DOORS"
        ]
      },
      {
        "area_or_item": "HANGING RAIL",
        "display_lines": [
          "Furnware - Oval wardrobe tube, aluminium, 15mm x 30mm x 1.2m, gunmetal -"
        ]
      },
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "BASE- BEVEL EDGE FINGERPULL",
          "UPPER - FINGERPULL",
          "TALL - PTO"
        ]
      },
      {
        "area_or_item": "HAMPER",
        "display_lines": [
          "Furnware - Tanova, Designer Laundry System, 1 X 65L Metal Hamper, White PART NO: LTDS45.165L.WH"
        ]
      },
      {
        "area_or_item": "Drawers",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Hinges",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Flooring",
        "display_lines": [
          "Tiled"
        ]
      },
      {
        "area_or_item": "Sink",
        "display_lines": [
          "undermount - specs tbc By Others Corner of Tub"
        ]
      }
    ],
    "room_meta": {
      "Source": "Colour Selections - 15.12.25 - Lot 532 Sandpiper Terrace.pdf",
      "Pages": "4",
      "Confidence": "0.85",
      "Evidence": "BENCHTOP BENCHTOP (SEAT) laundry - 20MM STONE Mud room - 33mm Notaio Walnut laminate VERTICAL GRAIN\nBASE CABINETRY COLOUR Notaio Walnut Woodmatt VERTICAL GRAIN\nUPPER CABINETRY COLOUR Notaio Walnut Woodmatt VERTICAL GRAIN\nKICKBOARDS AS DOORS\nHANGING RAIL Oval wardrobe tube, aluminium, 15mm x 30mm x 1"
    }
  }
}
```

```json
{
  "BED1&2 & RUMPUS": {
    "visible_room_rows": [
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "BASE- BEVEL EDGE FINGERPULL"
        ]
      },
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "UPPER - FINGERPULL, TALL - (PTO)"
        ]
      },
      {
        "area_or_item": "CABINETRY COLOUR & TOP (BED 1-\nMASTER)",
        "display_lines": [
          "Polytec - Stone Grey - Matt"
        ]
      },
      {
        "area_or_item": "EXTRA TOP IN MASTER BEDROOM (BED\n1 -MASTER)",
        "display_lines": [
          "Polytec - Laminated 500mm deep x 50mm floating shelf with internal steel support - Laminate Fontaine Matt - 10x10 edge"
        ]
      },
      {
        "area_or_item": "BENCHTOP AND SHELVES COLOUR -\n(BED 2)",
        "display_lines": [
          "Polytec - 33mm Laminated benchtop and 50mm floating shelf with internal steel support - Stone Grey - Matt"
        ]
      },
      {
        "area_or_item": "CABINETRY COLOUR (BED 2) AND\nKICKBOARDS",
        "display_lines": [
          "Polytec - Stone Grey - Matt"
        ]
      },
      {
        "area_or_item": "BENCHTOP COLOUR (RUMPUS)",
        "display_lines": [
          "Estella Oak - Woodmatt - 33mm Laminate benchtop"
        ]
      },
      {
        "area_or_item": "CABINETRY COLOUR (RUMPUS)",
        "display_lines": [
          "Estella Oak - Woodmatt"
        ]
      },
      {
        "area_or_item": "LED'S (BED 2)\nHANDLES",
        "display_lines": [
          "As per drawings",
          "BASE- BEVEL EDGE FINGERPULL",
          "UPPER - FINGERPULL, TALL - PTO"
        ]
      },
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "BASE- BEVEL EDGE FINGERPULL"
        ]
      },
      {
        "area_or_item": "HANDLES",
        "display_lines": [
          "UPPER - FINGERPULL, TALL - (PTO)"
        ]
      },
      {
        "area_or_item": "Drawers",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Hinges",
        "display_lines": [
          "Soft Close"
        ]
      },
      {
        "area_or_item": "Flooring",
        "display_lines": [
          "Hybrid"
        ]
      },
      {
        "area_or_item": "Sink",
        "display_lines": [
          "-"
        ]
      }
    ],
    "room_meta": {
      "Source": "Colour Selections - 15.12.25 - Lot 532 Sandpiper Terrace.pdf",
      "Pages": "9",
      "Confidence": "0.85",
      "Evidence": "CABINETRY COLOUR & TOP (BED 1- MASTER) Stone Grey - Matt\nEXTRA TOP IN MASTER BEDROOM (BED 1 -MASTER) Laminated 500mm deep x 50mm floating shelf with internal steel support - Laminate Fontaine Matt - 10x10 edge\nBENCHTOP AND SHELVES COLOUR - (BED 2) 33mm Laminated benchtop and 50mm floating shelf with"
    }
  }
}
```

### 3.3 Comparison job 64: KITCHEN raw snapshot extract

This comes from the local cached baseline file `tmp/baseline_snapshots/job_64_raw_spec.json`.

```json
[
  {
    "area_or_item": "BENCHTOP",
    "supplier": "By Others",
    "specs_or_description": "20mm Stone | WFE's x 2",
    "notes": "",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "bench_tops"
    ],
    "page_no": 1,
    "row_order": 1,
    "provenance": {
      "source_provider": "cell_grid_repair",
      "canonical_label": "BENCHTOP",
      "layout_row_label": "BENCHTOP",
      "layout_value_text": "20mm Stone WFE's x 2",
      "layout_supplier_text": "By Others",
      "layout_notes_text": ""
    }
  },
  {
    "area_or_item": "BASE CABINETRY COLOUR",
    "supplier": "Polytec",
    "specs_or_description": "BLACK - MATT",
    "notes": "",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "door_colours"
    ],
    "page_no": 1,
    "row_order": 2,
    "provenance": {
      "source_provider": "cell_grid_repair",
      "canonical_label": "BASE CABINETRY COLOUR",
      "layout_row_label": "BASE CABINETRY COLOUR",
      "layout_value_text": "BLACK - MATT",
      "layout_supplier_text": "Polytec",
      "layout_notes_text": ""
    }
  },
  {
    "area_or_item": "UPPER CABINETRY COLOUR",
    "supplier": "Polytec",
    "specs_or_description": "BLACK - MATT",
    "notes": "",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "door_colours"
    ],
    "page_no": 1,
    "row_order": 3,
    "provenance": {
      "source_provider": "cell_grid_repair",
      "canonical_label": "UPPER CABINETRY COLOUR",
      "layout_row_label": "UPPER CABINETRY COLOUR",
      "layout_value_text": "BLACK - MATT",
      "layout_supplier_text": "Polytec",
      "layout_notes_text": ""
    }
  },
  {
    "area_or_item": "KICKBOARDS",
    "supplier": "",
    "specs_or_description": "As Doors",
    "notes": "",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "other_material"
    ],
    "page_no": 1,
    "row_order": 4,
    "provenance": {
      "source_provider": "cell_grid_repair",
      "canonical_label": "KICKBOARDS",
      "layout_row_label": "KICKBOARDS",
      "layout_value_text": "As Doors",
      "layout_supplier_text": "",
      "layout_notes_text": ""
    }
  },
  {
    "area_or_item": "HANDLES",
    "supplier": "Furnware",
    "specs_or_description": "Momo flapp pull handle 256mm in brushed black - FPH256.BBL",
    "notes": "Horizontal on ALL",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "handles"
    ],
    "page_no": 1,
    "row_order": 5,
    "provenance": {
      "raw": "visible table",
      "layout_row_label": "HANDLES",
      "layout_value_text": "Momo flapp pull handle 256mm in brushed black - FPH256.BBL",
      "layout_supplier_text": "Furnware",
      "layout_notes_text": "Horizontal on ALL"
    }
  },
  {
    "area_or_item": "BIN",
    "supplier": "Furnware",
    "specs_or_description": "450mm Short Pull-Out - (Short drawer on bottom) - 2 x 29Ltr Buckets",
    "notes": "",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "other_material"
    ],
    "page_no": 1,
    "row_order": 6,
    "provenance": {
      "raw": "visible table",
      "layout_row_label": "BIN",
      "layout_value_text": "450mm Short Pull-Out - (Short drawer on bottom) - 2 x 29Ltr Buckets",
      "layout_supplier_text": "Furnware",
      "layout_notes_text": ""
    }
  },
  {
    "area_or_item": "ACCESSORIES",
    "supplier": "",
    "specs_or_description": "GPO - Double Powerpoint with 2xUSB sockets - Black",
    "notes": "Island bench, front of MW cupboard",
    "display_lines": [],
    "display_groups": [],
    "tags": [
      "other_material"
    ],
    "page_no": 1,
    "row_order": 7,
    "provenance": {
      "source_provider": "cell_grid_repair",
      "canonical_label": "ACCESSORIES",
      "layout_row_label": "ACCESSORIES",
      "layout_value_text": "GPO - Double Powerpoint with 2xUSB sockets - Black- (Island bench, front of MW cupboard)",
      "layout_supplier_text": "",
      "layout_notes_text": ""
    }
  }
]
```

## 4. Analysis

### Table 1 - merged-label row anatomy (job 73 KITCHEN)

| `area_or_item` (verbatim) | `specs_or_description` (verbatim, `\n` escaped) | `supplier` | `notes` | `display_lines` | concatenated PDF row names | clean N-way split already present? |
|---|---|---|---|---|---:|---|
| `BENCHTOP\nISLAND CABINETRY COLOUR (incl. BACK OF\nISLAND CURVE AND COLUMN)` | `By builder 40mm stone - Arissed Notaio Walnut Woodmatt` | `Polytec` | `` | `["By builder - 40mm stone - Arissed", "Polytec - Notaio Walnut Woodmatt"]` | 2 | **No.** There are two visible output lines, but they are not a safe one-to-one segmentation of the original two row labels. One line is a bench-top phrase without supplier; the other line is a cabinetry colour phrase with supplier. Splitting this in the adapter would still require semantic inference. |
| `KICKBOARDS\nGPO'S` | `AS DOORS Island Drawer GPO 1 - Side of Upper Bin Draw 'By Builder'(see pic 1 shown) Island Drawer GPO 2 - Rear panel of Utensil Drawer - Hafele Trio 822.53.151 'By Imperial' (See pic 2 shown)` | `Polytec` | `` | `["Polytec - AS DOORS Island Drawer GPO 1 - Side of Upper Bin Draw 'By Builder'(see pic 1 shown) Island Drawer GPO 2 - Rear panel of Utensil Drawer - Hafele Trio 822.53.151 'By Imperial' (See pic 2 shown)"]` | 2 | **No.** The value side is one contaminated blob. There is no clean delimiter that yields exactly `KICKBOARDS` content plus `GPO'S` content. |
| `BIN\nACCESSORIES\nLED's` | `450MM (SHORT) - 2 X 29LTR Veronar, Spice Tray Insert, To Suit 450mm & 600MM Drawer, White - VCT.450E.WH.FGx1 & VCT.600E.WH.FGx1 As per drawings` | `Hettich\nFurnware` | `` | `["*Hettich / Furnware* - 450MM (SHORT) - 2 X 29LTR Veronar, Spice Tray Insert, To Suit 450mm & 600MM Drawer, White - VCT.450E.WH.FGx1 & VCT.600E.WH.FGx1 As per drawings"]` | 3 | **No.** The supplier side has 2 lines, the label side has 3 labels, and the value side is a single ambiguous sentence ending in LED-like text (`As per drawings`). This is not heuristically safe to split in the adapter. |

**Decision from Table 1:** the KITCHEN evidence is decisively **shape (b)**, not shape (a). The main repo does not receive already segmented per-row values that merely need pairing. The cell-grid layer is already merging row ownership.

### Table 2 - cross-room pattern check

| room | rows whose `area_or_item` is not a single canonical row name | interpretation |
|---|---|---|
| `PANTRY` | none observed | clean comparison room |
| `LAUNDRY & MUD ROOM` | `BENCHTOP\nBENCHTOP (SEAT)` | same family as KITCHEN: adjacent label merge on an IMAGE-column page |
| `BED1&2 & RUMPUS` | `LED'S (BED 2)\nHANDLES` | direct Bug I evidence; LED and HANDLES row ownership merged |
| `BED1&2 & RUMPUS` | `CABINETRY COLOUR & TOP (BED 1-\nMASTER)`, `BENCHTOP AND SHELVES COLOUR -\n(BED 2)`, `CABINETRY COLOUR (BED 2) AND\nKICKBOARDS` | these look like wrapped long labels, not necessarily cross-row merges |

**Takeaway:** this is not PANTRY-wide or whole-job-wide. It clusters on rooms/pages where the joinery sheet layout is more crowded, especially pages with IMAGE-column content or denser multi-row packing.

### Table 3 - v6 vs signed-off comparison (KITCHEN)

| field | job 73 v6 | signed-off job used |
|---|---|---|
| signed-off comparison job | n/a | **job 64** |
| room label compared | `KITCHEN` | `KITCHEN` |
| number of `material_rows` for KITCHEN | **10** (6 canonical rows + 4 synthesized handle backfill rows from the direct prod snapshot inspection) | **7** |
| any `area_or_item` containing 2+ canonical row name tokens? | **Yes**: `BENCHTOP\nISLAND CABINETRY COLOUR...`, `KICKBOARDS\nGPO'S`, `BIN\nACCESSORIES\nLED's` | **No** |
| `provenance.source_provider` distribution | direct prod snapshot inspection: **6** rows with v6 provenance, plus **4** synthesized handle backfill rows carrying `synthesized_from_room_handles=true` | cached baseline raw snapshot: `{"cell_grid_repair": 5, "visible table": 2}` |
| parser strategy | `imperial_v6` | `global_conservative` |
| layout provider | `pdf_to_structured_json_v6` | `heavy_vision` |

**Interpretation:** the merge is a **v6 regression / v6-specific limitation**, not something seen in the older signed-off comparison KITCHEN.

### Table 4 - Bug I (LED contamination) anatomy

| row family in `BED1&2 & RUMPUS` | `area_or_item` | `specs_or_description` / rendered payload | `display_lines` | interpretation |
|---|---|---|---|---|
| clean handles row | `HANDLES` | rendered as a single handle line | `["BASE- BEVEL EDGE FINGERPULL"]` | normal |
| clean handles row | `HANDLES` | rendered as a single handle line | `["UPPER - FINGERPULL, TALL - (PTO)"]` | normal |
| **merged bug row** | `LED'S (BED 2)\nHANDLES` | direct prod snapshot: `As per drawings | BASE- BEVEL EDGE FINGERPULL | UPPER - FINGERPULL, TALL - PTO` | `["As per drawings", "BASE- BEVEL EDGE FINGERPULL", "UPPER - FINGERPULL, TALL - PTO"]` | **same root-cause family as Bug H** |
| synthesized followups | `HANDLES` | room-level backfill | duplicate handle lines after the merged bug row | downstream symptom, not root cause |

**Bug I classification:** **(i) a separate LED row got merged into HANDLES**. This is not just a flat summary-gating issue. The raw row title itself is already `LED'S (BED 2)\nHANDLES`.

## 5. Recommendation

## Option A - sibling repo fix recommended

### Why Option A is the correct call

1. **The decisive KITCHEN rows are shape (b), not shape (a).**
   - `KICKBOARDS\nGPO'S` is one mixed blob.
   - `BIN\nACCESSORIES\nLED's` is one mixed blob with a 3-label / 2-supplier mismatch.
   - `BENCHTOP\nISLAND CABINETRY COLOUR...` is only superficially split; it still requires semantic reassignment, not simple row pairing.

2. **Bug I is already the same family of defect.**
   - The bad row is not merely a summary candidate problem.
   - The raw row itself is `LED'S (BED 2)\nHANDLES`, with `As per drawings` already absorbed into the handle block.

3. **The older signed-off KITCHEN comparison does not show the merge.**
   - Job 64 KITCHEN remains row-separated as `BENCHTOP`, `BASE CABINETRY COLOUR`, `UPPER CABINETRY COLOUR`, `KICKBOARDS`, `HANDLES`, `BIN`, `ACCESSORIES`.

4. **An adapter split heuristic would be too destructive.**
   - It would have to guess:
     - which fragment belongs to `KICKBOARDS` vs `GPO'S`
     - whether `As per drawings` belongs to `LED's`, `ACCESSORIES`, or the tail of a hardware line
     - how to allocate `supplier` lines when label count and supplier count diverge
   - That is no longer a deterministic display repair. It is semantic re-parsing downstream of a broken cell-grid truth layer.

### Recommended fix boundary

Fix the row-boundary / cell-ownership recovery in the sibling extractor (`pdf_to_structured_json_v6.py` side), specifically where IMAGE-column-heavy rows are allowed to merge adjacent label bands into one `area_or_item` cell.

## 6. Surprises / deviations

1. **Job 64 is available, but it is not v6.**
   - Live job 64 is `Global Conservative / Heuristic only`, not `imperial_v6`.
   - That contradicts the task brief wording that called 64 the most recent signed-off Imperial v6 fixture.

2. **Live job 67 exists, but it is a poor direct KITCHEN comparison.**
   - Its room label is `KITCHEN & PANTRY`, not plain `KITCHEN`.
   - Also, the local cached `tmp/baseline_snapshots/job_67_raw_spec.json` is stale and shows the old heuristic path, so it is not trustworthy as a current v6 comparison artifact.

3. **The post-compaction shell could not re-establish direct prod SSH access from this machine.**
   - The current machine had no working SSH key path for prod, and the password env var was not present.
   - Because of that, the report combines:
     - direct prod snapshot extracts already captured earlier in this same Bug H session
     - fresh authenticated live HTML verification
     - local cached baseline snapshot evidence for job 64

4. **Live spec-list does not show the whole raw `material_rows` count anymore for job 73 KITCHEN.**
   - Bug G/Bug F display-layer dedupe/grouping means the current room card hides some synthesized handle duplicates.
   - The earlier direct prod snapshot inspection still showed **10 KITCHEN material_rows**.

5. **No contradiction to the core recommendation was observed.**
   - All decisive evidence still points to extractor-side row-boundary recovery, not adapter-side splitting.
