from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from App.services import store
from App.services.runtime import DB_PATH


JOB_IDS = (61, 62, 64, 67)
SNAPSHOT_KIND = "raw_spec"
OUTPUT_DIR = Path(__file__).resolve().parent / "baseline_snapshots"


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _room_material_rows(room: dict[str, Any]) -> list[dict[str, Any]]:
    rows = room.get("material_rows")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _summarize_snapshot(data: dict[str, Any]) -> tuple[int, int, int]:
    rooms = [room for room in _as_list(data.get("rooms")) if isinstance(room, dict)]
    material_rows = [row for room in rooms for row in _room_material_rows(room)]
    rows_with_handle_subitems = sum(1 for row in material_rows if _as_list(row.get("handle_subitems")))
    return len(rooms), len(material_rows), rows_with_handle_subitems


def dump_job_snapshot(job_id: int) -> bool:
    snapshot = store.get_snapshot(job_id, SNAPSHOT_KIND)
    if snapshot is None:
        print(f"job {job_id}: missing {SNAPSHOT_KIND} snapshot", file=sys.stderr)
        return False

    data = snapshot.get("data")
    if not isinstance(data, dict):
        print(f"job {job_id}: snapshot data is not a JSON object", file=sys.stderr)
        return False

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"job_{job_id}_{SNAPSHOT_KIND}.json"
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    room_count, material_row_count, handle_subitem_row_count = _summarize_snapshot(data)
    print(
        f"job {job_id}: rooms={room_count}, "
        f"material_rows={material_row_count}, "
        f"rows_with_handle_subitems={handle_subitem_row_count}, "
        f"wrote={output_path}"
    )
    return True


def main() -> int:
    ok = True
    for job_id in JOB_IDS:
        ok = dump_job_snapshot(job_id) and ok
    if not ok:
        print(f"checked database: {DB_PATH}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
