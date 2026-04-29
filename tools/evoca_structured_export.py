from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from App.services import evoca_structured_extractor


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Evoca source-native structured JSON and QA Excel workbook.")
    parser.add_argument("pdf", nargs="+", help="Evoca PDF path(s) to parse.")
    parser.add_argument("--out-dir", default="tmp/evoca_structured", help="Output directory for JSON/XLSX artifacts.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for raw_pdf in args.pdf:
        pdf_path = Path(raw_pdf)
        started = time.perf_counter()
        structured = evoca_structured_extractor.extract_evoca_pdf(pdf_path)
        elapsed = time.perf_counter() - started
        stem = _safe_stem(pdf_path.stem)
        json_path = out_dir / f"{stem}.json"
        xlsx_path = out_dir / f"{stem}.xlsx"
        evoca_structured_extractor.write_structured_json(structured, json_path)
        evoca_structured_extractor.write_structured_workbook(structured, xlsx_path)
        stats = structured.get("statistics", {})
        print(
            f"{pdf_path.name}: {elapsed:.2f}s, "
            f"sections={stats.get('section_count', 0)}, "
            f"rooms={stats.get('room_count', 0)}, "
            f"groups={stats.get('group_count', 0)}, "
            f"rows={stats.get('row_count', 0)}"
        )
        print(f"  JSON: {json_path}")
        print(f"  XLSX: {xlsx_path}")
    return 0


def _safe_stem(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
    return cleaned[:120] or "evoca"


if __name__ == "__main__":
    raise SystemExit(main())

