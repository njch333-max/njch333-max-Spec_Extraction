from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from App.services import extraction_service


DEFAULT_OUTPUT_DIR = REPO_ROOT / "tmp" / "imperial_grid_debug"


def _bbox_rect_svg(bbox: dict[str, Any], *, stroke: str, fill: str = "none", opacity: float = 1.0, width: float = 1.0) -> str:
    try:
        x0 = float(bbox.get("x0", 0.0) or 0.0)
        x1 = float(bbox.get("x1", 0.0) or 0.0)
        top = float(bbox.get("top", 0.0) or 0.0)
        bottom = float(bbox.get("bottom", 0.0) or 0.0)
    except (TypeError, ValueError):
        return ""
    if x1 <= x0 or bottom <= top:
        return ""
    return (
        f'<rect x="{x0:.1f}" y="{top:.1f}" width="{x1 - x0:.1f}" height="{bottom - top:.1f}" '
        f'fill="{fill}" fill-opacity="{opacity:.2f}" stroke="{stroke}" stroke-width="{width:.1f}" />'
    )


def _segment_svg(segment: dict[str, Any], *, stroke: str, dash: str = "") -> str:
    try:
        orientation = str(segment.get("orientation", "") or "")
        edge = float(segment.get("edge", 0.0) or 0.0)
        start = float(segment.get("start", 0.0) or 0.0)
        end = float(segment.get("end", 0.0) or 0.0)
    except (TypeError, ValueError):
        return ""
    if not edge or end <= start:
        return ""
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    if orientation == "vertical":
        return f'<line x1="{edge:.1f}" y1="{start:.1f}" x2="{edge:.1f}" y2="{end:.1f}" stroke="{stroke}" stroke-width="1.0"{dash_attr} />'
    return f'<line x1="{start:.1f}" y1="{edge:.1f}" x2="{end:.1f}" y2="{edge:.1f}" stroke="{stroke}" stroke-width="1.0"{dash_attr} />'


def render_debug_svg(payload: dict[str, Any]) -> str:
    page_size = payload.get("page_size", {}) if isinstance(payload.get("page_size"), dict) else {}
    width = float(page_size.get("width", 1200.0) or 1200.0)
    height = float(page_size.get("height", 900.0) or 900.0)
    page_structure = payload.get("page_structure", {}) if isinstance(payload.get("page_structure"), dict) else {}
    separator_model = payload.get("separator_model", {}) if isinstance(payload.get("separator_model"), dict) else {}
    elements: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.1f}" height="{height:.1f}" viewBox="0 0 {width:.1f} {height:.1f}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white" />',
    ]
    elements.append(_bbox_rect_svg(page_structure.get("header_bbox", {}) if isinstance(page_structure.get("header_bbox"), dict) else {}, stroke="#7c7c7c", fill="#7c7c7c", opacity=0.06, width=1.0))
    elements.append(_bbox_rect_svg(page_structure.get("table_header_bbox", {}) if isinstance(page_structure.get("table_header_bbox"), dict) else {}, stroke="#2563eb", fill="#2563eb", opacity=0.08, width=2.0))
    elements.append(_bbox_rect_svg(page_structure.get("content_grid_bbox", {}) if isinstance(page_structure.get("content_grid_bbox"), dict) else {}, stroke="#dc2626", fill="#dc2626", opacity=0.04, width=2.0))
    elements.append(_bbox_rect_svg(page_structure.get("footer_bbox", {}) if isinstance(page_structure.get("footer_bbox"), dict) else {}, stroke="#6b7280", fill="#6b7280", opacity=0.07, width=1.0))
    for image_box in separator_model.get("image_bboxes", []) or []:
        if isinstance(image_box, dict):
            elements.append(_bbox_rect_svg(image_box, stroke="#16a34a", fill="#16a34a", opacity=0.10, width=1.0))
    for key, color, dash in (
        ("visible_horizontal_segments", "#111827", ""),
        ("visible_vertical_segments", "#111827", ""),
        ("inferred_horizontal_segments", "#f97316", "5 3"),
        ("inferred_vertical_segments", "#f97316", "5 3"),
    ):
        for segment in separator_model.get(key, []) or []:
            if isinstance(segment, dict):
                elements.append(_segment_svg(segment, stroke=color, dash=dash))
    role_colors = {
        "label": "#0ea5e9",
        "description": "#a855f7",
        "image": "#22c55e",
        "supplier": "#eab308",
        "notes": "#ef4444",
    }
    for cell in payload.get("cell_ownership", []) or []:
        if not isinstance(cell, dict):
            continue
        bbox = cell.get("bbox", {}) if isinstance(cell.get("bbox"), dict) else {}
        role = str(cell.get("role", "") or "")
        elements.append(_bbox_rect_svg(bbox, stroke=role_colors.get(role, "#64748b"), fill=role_colors.get(role, "#64748b"), opacity=0.03, width=0.8))
    title = f"Imperial grid debug page {payload.get('page_no', '')}"
    elements.append(f'<text x="12" y="22" font-family="monospace" font-size="14" fill="#111827">{html.escape(title)}</text>')
    elements.append("</svg>")
    return "\n".join(part for part in elements if part)


def write_debug_artifacts(payload: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    page_no = int(payload.get("page_no", 0) or 0)
    json_path = output_dir / f"page_{page_no:03d}_grid.json"
    svg_path = output_dir / f"page_{page_no:03d}_grid.svg"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    svg_path.write_text(render_debug_svg(payload), encoding="utf-8")
    return json_path, svg_path


def _parse_pages(value: str) -> list[int]:
    pages: list[int] = []
    for raw_part in str(value or "").split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            pages.extend(range(start, end + 1))
        else:
            pages.append(int(part))
    return sorted(set(page for page in pages if page > 0))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Imperial grid debug JSON/SVG overlays.")
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--pages", default="1", help="Page list/ranges, for example 1,3-5.")
    parser.add_argument("--room-scope", default="")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    written: list[str] = []
    for page_no in _parse_pages(args.pages):
        payload = extraction_service.build_imperial_grid_debug_page(args.pdf, page_no, room_scope=args.room_scope)
        if not payload:
            continue
        json_path, svg_path = write_debug_artifacts(payload, args.out)
        written.extend([str(json_path), str(svg_path)])
    for path in written:
        print(path)
    return 0 if written else 1


if __name__ == "__main__":
    raise SystemExit(main())
