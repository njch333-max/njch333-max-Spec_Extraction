"""
Cell-aware PDF extractor for Imperial Kitchens joinery selection sheets.

v2 changes (per user feedback 2026-04-19):
  - Section boundaries anchored to the footer "DESIGNER: ... CLIENT NAME:
    SIGNATURE: SIGNED DATE:" instead of per-page title. Pages without a
    title are continuation of the previous section, not standalone.
  - Accept 3-column continuation tables (AREA column merged upward via
    rowspan from previous page still counts).
  - Fall back to text-line extraction when a continuation page has no
    usable grid (e.g. only a footer + 1-2 data rows).

Usage:
    python pdf_to_structured_json.py <input.pdf> <output.json>
"""
import json
import re
import sys
from pathlib import Path

import pdfplumber


FOOTER_RE = re.compile(
    r"(DESIGNER\s*:.*?CLIENT\s*NAME\s*:.*?SIGNATURE\s*:.*?SIGNED\s*DATE)",
    re.IGNORECASE | re.DOTALL,
)

def page_has_footer(page):
    text = page.extract_text() or ""
    return bool(FOOTER_RE.search(text))


# ---------- Geometry helpers ----------

def snap(edges, tol=3):
    edges = sorted(set(edges))
    if not edges:
        return []
    snapped = [edges[0]]
    for e in edges[1:]:
        if e - snapped[-1] > tol:
            snapped.append(e)
    return snapped


def extract_page_grid(page):
    x_set, y_set = set(), set()
    for r in page.rects:
        x0, x1 = r["x0"], r["x1"]
        y0, y1 = r["top"], r["bottom"]
        w, h = r["width"], r["height"]
        if w <= 3:
            x_set.add(round((x0 + x1) / 2))
        elif h <= 3:
            y_set.add(round((y0 + y1) / 2))
        else:
            x_set.add(round(x0))
            x_set.add(round(x1))
            y_set.add(round(y0))
            y_set.add(round(y1))
    return snap(x_set), snap(y_set)


def get_horizontal_segments(page):
    segs = []
    for r in page.rects:
        x0, x1 = r["x0"], r["x1"]
        y0, y1 = r["top"], r["bottom"]
        if r["height"] <= 3:
            segs.append((x0, x1, (y0 + y1) / 2))
        else:
            segs.append((x0, x1, y0))
            segs.append((x0, x1, y1))
    return segs


def line_exists(x0, x1, y_target, h_segs, tol=3):
    for sx0, sx1, sy in h_segs:
        if abs(sy - y_target) <= tol and sx0 <= x0 + 2 and sx1 >= x1 - 2:
            return True
    return False


def filter_data_y_edges(y_edges, min_height=40):
    """Legacy filter: keep edges separated by at least min_height pixels."""
    if len(y_edges) < 2:
        return y_edges
    out = [y_edges[0]]
    for y in y_edges[1:]:
        if y - out[-1] >= min_height:
            out.append(y)
    return out


def smart_filter_y_edges(page, min_width_frac=0.85, cluster_gap=25):
    """
    Better y-edge filter that uses TWO signals:
      1. Width: only consider horizontal rects covering >= 85% of page width
         (this rules out short decorative lines and per-cell underlines).
      2. Density: lines that cluster within `cluster_gap` pixels of each other
         form a "decoration band" (e.g. metadata header block with multiple
         sub-divider lines). Within a band, keep only the top and bottom
         edges -- the middle ones are decoration, not row separators.
    
    Compared to filter_data_y_edges(min_height=40), this preserves real row
    boundaries down to ~25 pixels apart (e.g. HANGING RAIL / KICKBOARDS).
    """
    page_w = page.width
    raw_ys = []
    for r in page.rects:
        if r["height"] <= 3 and r["width"] / page_w >= min_width_frac:
            y = round((r["top"] + r["bottom"]) / 2)
            raw_ys.append(y)
    raw_ys = sorted(set(raw_ys))
    if not raw_ys:
        return []

    # Group consecutive y's where each gap <= cluster_gap into a band.
    clusters = [[raw_ys[0]]]
    for y in raw_ys[1:]:
        if y - clusters[-1][-1] <= cluster_gap:
            clusters[-1].append(y)
        else:
            clusters.append([y])

    # For each band, keep only the boundaries (top and bottom).
    result = []
    for cluster in clusters:
        result.append(cluster[0])
        if len(cluster) > 1:
            result.append(cluster[-1])
    return sorted(set(result))


def detect_merges(table, x_edges, y_edges, h_segs):
    if not table:
        return table
    n_rows = len(table)
    n_cols = len(table[0]) if table else 0
    new_table = [list(r) for r in table]
    for col in range(n_cols):
        if col >= len(x_edges) - 1:
            continue
        x0 = x_edges[col]
        x1 = x_edges[col + 1]
        for row in range(1, n_rows):
            if row >= len(y_edges) - 1:
                continue
            y_top = y_edges[row]
            if not line_exists(x0, x1, y_top, h_segs):
                prev = new_table[row - 1][col]
                cur = new_table[row][col]
                if prev is not None:
                    if cur is not None and cur.strip() and cur.strip() != prev.strip():
                        merged = prev + "\n" + cur
                        new_table[row - 1][col] = merged
                    new_table[row][col] = None
    return new_table


# ---------- Page identity ----------

def extract_page_title(page):
    words = page.extract_words(keep_blank_chars=False)
    lines_by_y = {}
    for w in words:
        y_key = round(w["top"] / 3) * 3
        lines_by_y.setdefault(y_key, []).append(w)

    for y_key in sorted(lines_by_y):
        words_at_y = sorted(lines_by_y[y_key], key=lambda w: w["x0"])
        line_text = " ".join(w["text"] for w in words_at_y)
        up = line_text.upper()
        if any(kw in up for kw in (
            "SELECTION SHEET", "APPLIANCES", "SINKWARE & TAPWARE",
            "SINKWARE", "TAPWARE"
        )):
            return re.sub(r"\s+", " ", line_text).strip()
    return None


def extract_page_metadata(page):
    text = page.extract_text() or ""
    meta = {}
    patterns = {
        "address":           r"Address:\s*([^\n]+)",
        "client":            r"Client:\s*([^\n]+)",
        "date":              r"Date:\s*([^\n]+)",
        "ceiling_height":    r"Ceiling height:\s*([^\n]+?)(?=\s*Cabinetry Height|\n|$)",
        "cabinetry_height":  r"Cabinetry Height:\s*([^\n]+?)(?=\s*Corniced|\s*CORNICE|\n|$)",
        "bulkhead":          r"Bulkhead:\s*([^\n]+?)(?=\s*Shadowline|\n|$)",
        "shadowline":        r"Shadowline:\s*([^\n]+)",
        "hinges":            r"Hinges & Drawer Runners:\s*([^\n]+?)(?=\s*Floor Type|\n|$)",
        "floor_type":        r"Floor Type & Kick refacing required:\s*([^\n]+)",
        "designer":          r"DESIGNER:\s*([^\n]+?)(?=\s*CLIENT NAME|\n|$)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            val = m.group(1).strip()
            val = re.sub(r"\s{2,}.*$", "", val).strip()
            if val and val.lower() not in ("na", "n/a"):
                meta[key] = val
    return meta


# ---------- Table recognition ----------

def classify_table(table, x_edges, expect_header=True):
    if not table or len(table[0]) < 3:
        return "other"
    for row in table[:6]:
        joined = " ".join((c or "") for c in row).upper()
        if "AREA / ITEM" in joined or "AREA/ITEM" in joined or "SPECS / DESCRIPTION" in joined:
            return "data"
    if expect_header:
        return "other"
    for row in table:
        if sum(1 for c in row if c and c.strip()) >= 2:
            return "continuation"
    return "other"


def detect_header_row(table):
    for i, row in enumerate(table[:6]):
        joined = " ".join((c or "") for c in row).upper()
        if "AREA / ITEM" in joined or "SPECS / DESCRIPTION" in joined or "SUPPLIER" in joined:
            return i
    return 0


def header_to_keys(header_row):
    """
    Map raw header cells to canonical column keys.

    Step 1: keyword match (each cell text checked for AREA/SPECS/IMAGE/SUPPLIER/NOTES).
    Step 2: positional inference. The Imperial standard column order is
        [area, specs, image, supplier, notes]
    If we identified some keys via keyword match, infer the missing ones by
    looking at their position relative to the identified ones. This recovers
    from cases where a header cell contains stray content (e.g. detect_merges
    pulled "Soft Close" up into the header row of column 1).
    """
    CANONICAL = ["area", "specs", "image", "supplier", "notes"]

    # Step 1: keyword match
    keys = []
    for cell in header_row:
        up = (cell or "").upper()
        if "AREA" in up:
            keys.append("area")
        elif "SPECS" in up or "DESCRIPTION" in up:
            keys.append("specs")
        elif "IMAGE" in up:
            keys.append("image")
        elif "SUPPLIER" in up:
            keys.append("supplier")
        elif "NOTES" in up:
            keys.append("notes")
        else:
            keys.append(None)

    # Step 2: positional inference
    # Only attempt if header has exactly 5 cols (Imperial canonical)
    # AND at least 3 of the 5 keys were identified via keyword match.
    if len(keys) == 5 and sum(1 for k in keys if k is not None) >= 3:
        # For each None, fill it with the canonical key at that position
        # if that canonical key is not already used at another index.
        used = set(k for k in keys if k is not None)
        for i, k in enumerate(keys):
            if k is None and CANONICAL[i] not in used:
                keys[i] = CANONICAL[i]
                used.add(CANONICAL[i])

    return keys


def is_disclaimer_row(row):
    joined = " ".join((c or "") for c in row).upper()
    if (
        "SIGNING THIS" in joined or
        "CANNOT BE RELIED" in joined or
        "COLOURS SHOWN ARE APPROXIMATE" in joined or
        "CLIENT NAME" in joined or
        "DESIGNER:" in joined or
        "DOCUMENT REF:" in joined
    ):
        return True
    if PANDADOC_REF_RE.search(joined):
        return True
    return False


def clean_cell(cell):
    if cell is None:
        return None
    cell = cell.strip()
    cell = re.sub(r"[ \t]+", " ", cell)
    cell = re.sub(r"\n{2,}", "\n", cell)
    return cell


# ---------- Section template ----------
#
# When a section's first page (the one with the title and full grid)
# is processed, we capture its (column_x_ranges, column_keys). All
# subsequent continuation pages in the same section will use this
# template to assign text to columns by x-position, regardless of
# whether the continuation page has its own grid drawn.
#
# This is critical because Imperial draws GRID only on the data area
# of continuation pages (typically just SPECS + IMAGE + SUPPLIER cells),
# while AREA labels and NOTES sit OUTSIDE any grid box but are still
# rendered on the page as plain text aligned to their parent column.

def build_section_template(x_edges, keys):
    """
    Build a list of (key, x_lo, x_hi) tuples representing column ranges.
    keys is the parent header_to_keys output (length = len(x_edges)-1).
    """
    template = []
    for i, key in enumerate(keys):
        if key is None:
            continue
        if i + 1 >= len(x_edges):
            continue
        x_lo = x_edges[i]
        x_hi = x_edges[i + 1]
        template.append((key, x_lo, x_hi))
    return template


def assign_word_to_column(word, template, tol=10):
    """
    Given a word's (x0, x1) and a section template, return the column key
    whose range contains the word's x-center, or None if it falls outside.
    """
    cx = (word["x0"] + word["x1"]) / 2
    for key, x_lo, x_hi in template:
        if x_lo - tol <= cx < x_hi + tol:
            return key
    return None


# ---------- Text-line fallback for continuation pages (template-based) ----------

EXCLUDE_PHRASES = (
    "COLOURS SHOWN ARE APPROXIMATE", "SIGNING THIS", "DESIGNER:",
    "CLIENT NAME:", "CANNOT BE RELIED", "ADDRESS:",
    "PH:", "IMPERIAL KITCHENS",
    "AVAILABILITY IS SUBJECT", "BY SIGNING",
    "MATCH ABOVE:",        # leading sticky-header phrase
    "DOCUMENT REF:",       # PandaDoc per-page footer/watermark
)

# PandaDoc page footer pattern: "Document Ref: PCM9L-CZUST-FQKSY-ZSXXR Page 3 of 11"
# The reference code itself appears alone on the last page of some PDFs.
# Match: 5 alphanumeric chars + dash repeated 3-4 times.
PANDADOC_REF_RE = re.compile(r"\b[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}-[A-Z0-9]{5}\b")

def is_excluded_line(text):
    up = text.upper()
    if any(p in up for p in EXCLUDE_PHRASES):
        return True
    if PANDADOC_REF_RE.search(up):
        return True
    return False


def extract_continuation_with_template(page, template, last_area, y_edges):
    """
    Extract rows from a continuation page using the section's column
    template.

    Algorithm (anchor-based row clustering):
      1. Pick the rightmost text-bearing column (usually SUPPLIER) as
         the row anchor. Each occurrence of a word in the anchor column
         marks the y-center of one logical row.
      2. For every other word on the page, assign it to the row whose
         anchor y-center is closest, within a tolerance of half the
         distance to neighboring anchors.
      3. This handles multi-line cells correctly — all the wrapped
         text in a SPECS or AREA cell falls within the same anchor's
         vertical band.

    If no anchor words exist (e.g. SUPPLIER column is blank for the whole
    page), fall back to using grid y_edges or simple y clustering.
    """
    words = page.extract_words(keep_blank_chars=False)
    if not words:
        return [], last_area

    # Filter out disclaimer/footer/header words by line-level content first
    # We'll do this by computing y-rough lines and dropping excluded ones.
    rough_lines = {}
    for w in words:
        y_key = round(w["top"] / 3) * 3
        rough_lines.setdefault(y_key, []).append(w)
    excluded_y = set()
    for y_key, wlist in rough_lines.items():
        wlist_sorted = sorted(wlist, key=lambda w: w["x0"])
        text = " ".join(w["text"] for w in wlist_sorted)
        if is_excluded_line(text):
            excluded_y.add(y_key)
    words = [w for w in words if round(w["top"] / 3) * 3 not in excluded_y]
    if not words:
        return [], last_area

    # Find SUPPLIER and NOTES column ranges. Both can serve as row anchors.
    # Using both is critical because some rows have empty SUPPLIER but populated
    # NOTES (e.g. KICKBOARDS rows often have neither, but HANDLES OVERHEADS
    # rows often have NOTES but no SUPPLIER).
    anchor_ranges = []  # list of (x_lo, x_hi)
    for key, x_lo, x_hi in template:
        if key in ("supplier", "notes"):
            anchor_ranges.append((x_lo, x_hi))
    # Fallback: rightmost non-area column with x_lo > 600
    if not anchor_ranges:
        for key, x_lo, x_hi in template:
            if x_lo > 600:
                anchor_ranges.append((x_lo, x_hi))
                break

    # Collect anchor words (words inside any anchor column)
    anchor_words = []
    if anchor_ranges:
        for w in words:
            cx = (w["x0"] + w["x1"]) / 2
            for x_lo, x_hi in anchor_ranges:
                if x_lo - 5 <= cx < x_hi + 5:
                    anchor_words.append(w)
                    break

    # Cluster anchor words by y proximity (15-pt gap = different row).
    # Because SUPPLIER and NOTES anchors for the same row often appear
    # at slightly different y (e.g. SUPPLIER y=90.9, NOTES y=90.9 — but
    # multi-line cells can spread by a few px), the cluster_gap stays at 15
    # which conflates same-row anchors regardless of which column they're in.
    anchor_ys = []
    if anchor_words:
        anchor_words.sort(key=lambda w: (w["top"] + w["bottom"]) / 2)
        cluster = []
        last_y = None
        for w in anchor_words:
            wy = (w["top"] + w["bottom"]) / 2
            if last_y is not None and wy - last_y > 15:
                cy = sum((wd["top"] + wd["bottom"]) / 2 for wd in cluster) / len(cluster)
                anchor_ys.append(cy)
                cluster = []
            cluster.append(w)
            last_y = wy
        if cluster:
            cy = sum((wd["top"] + wd["bottom"]) / 2 for wd in cluster) / len(cluster)
            anchor_ys.append(cy)

    # If we have no anchor row centers (e.g. SUPPLIER column is empty),
    # fall back to y_edges or simple grouping.
    if not anchor_ys:
        if y_edges and len(y_edges) >= 2:
            anchor_ys = [(y_edges[i] + y_edges[i + 1]) / 2 for i in range(len(y_edges) - 1)]
        else:
            # Last resort: cluster all words by 25-pt y gap (looser)
            ws = sorted(words, key=lambda w: (w["top"] + w["bottom"]) / 2)
            cluster = []; last_y = None
            for w in ws:
                wy = (w["top"] + w["bottom"]) / 2
                if last_y is not None and wy - last_y > 25:
                    cy = sum((wd["top"] + wd["bottom"]) / 2 for wd in cluster) / len(cluster)
                    anchor_ys.append(cy)
                    cluster = []
                cluster.append(w); last_y = wy
            if cluster:
                cy = sum((wd["top"] + wd["bottom"]) / 2 for wd in cluster) / len(cluster)
                anchor_ys.append(cy)

    if not anchor_ys:
        return [], last_area

    # Assign each word to the nearest anchor row
    rows_by_anchor = [[] for _ in anchor_ys]
    for w in words:
        wy = (w["top"] + w["bottom"]) / 2
        # Find nearest anchor
        best_i = 0
        best_d = abs(wy - anchor_ys[0])
        for i, ay in enumerate(anchor_ys[1:], start=1):
            d = abs(wy - ay)
            if d < best_d:
                best_d = d
                best_i = i
        rows_by_anchor[best_i].append(w)

    items = []
    for ri, row_words in enumerate(rows_by_anchor):
        if not row_words:
            continue

        # Bucket words into columns by x vs template
        col_words = {}
        for w in row_words:
            key = assign_word_to_column(w, template)
            if key is None:
                continue
            col_words.setdefault(key, []).append(w)

        # For each column, sort words by (y_top, x0) and join with spaces
        # within a y-line, newlines between y-lines
        rec = {}
        for key, wlist in col_words.items():
            # Group by y-line within column
            wlist.sort(key=lambda w: (w["top"], w["x0"]))
            sublines = []
            current = []
            current_y = None
            for w in wlist:
                wy = w["top"]
                if current_y is not None and wy - current_y > 8:
                    sublines.append(current)
                    current = []
                current.append(w)
                current_y = wy
            if current:
                sublines.append(current)
            joined = "\n".join(
                " ".join(ww["text"] for ww in sl) for sl in sublines
            ).strip()
            if joined:
                rec[key] = joined

        if not rec:
            continue

        # Carry area down only if missing entirely
        if "area" not in rec and last_area is not None:
            rec["area"] = f"{last_area} (cont.)"
        elif "area" in rec:
            last_area = rec["area"]

        rec["_source"] = {
            "page": page.page_number,
            "row_index": f"row_{ri}",
            "method": "template_anchor",
        }
        items.append(rec)

    return items, last_area


# ---------- PandaDoc signature page detection ----------

def is_pandadoc_signature_page(page):
    """
    Detect PandaDoc-generated signature pages (used by Imperial for
    customer signing). Marker: 'Signed with PandaDoc' or 'PandaDoc'
    watermark. These pages contain no spec data and should be skipped.
    """
    text = (page.extract_text() or "").lower()
    return "signed with pandadoc" in text or "pandadoc" in text


# ---------- Main pipeline ----------

def extract_pdf(pdf_path):
    out = {
        "source_pdf": str(pdf_path),
        "pages": [],
        "sections": [],
    }

    current_section = None
    current_template = None       # list of (key, x_lo, x_hi) for current section
    current_x_edges = None        # full x_edges from section's first page
    last_area = None

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            page_meta = extract_page_metadata(page)

            # Skip PandaDoc signature pages early
            if is_pandadoc_signature_page(page):
                page_record = {
                    "page_number": page_num,
                    "title": None,
                    "skipped": "pandadoc_signature_page",
                    "items": [],
                }
                out["pages"].append(page_record)
                # PandaDoc page also closes any open section (it's at end of doc)
                if current_section is not None:
                    current_section = None
                    current_template = None
                    current_x_edges = None
                    last_area = None
                continue

            page_title = extract_page_title(page)
            has_footer = page_has_footer(page)

            x_edges, y_edges_all = extract_page_grid(page)
            y_edges = smart_filter_y_edges(page)
            # Fallback to legacy filter if smart filter returned too few edges
            # (e.g. PDF has no full-width horizontal lines at all)
            if len(y_edges) < 2:
                y_edges = filter_data_y_edges(y_edges_all, min_height=40)

            page_record = {
                "page_number": page_num,
                "title": page_title,
                "has_footer": has_footer,
                "metadata": page_meta,
                "items": [],
            }

            # --- Section gating ---
            if page_title:
                current_section = {
                    "section_title": page_title,
                    "metadata": page_meta,
                    "items": [],
                    "pages": [],
                }
                out["sections"].append(current_section)
                current_template = None    # rebuild template from this page
                current_x_edges = None
                last_area = None
            elif current_section is None:
                current_section = {
                    "section_title": f"Unknown (p{page_num})",
                    "metadata": page_meta,
                    "items": [],
                    "pages": [],
                }
                out["sections"].append(current_section)

            if page_num not in current_section["pages"]:
                current_section["pages"].append(page_num)

            # =================================================================
            # CASE 1: This page is the start of a section (has title)
            # → use grid-based extraction; build template from detected header
            # =================================================================
            if page_title:
                if len(x_edges) >= 3 and len(y_edges) >= 2:
                    tables = page.extract_tables(table_settings={
                        "vertical_strategy": "explicit",
                        "horizontal_strategy": "explicit",
                        "explicit_vertical_lines": x_edges,
                        "explicit_horizontal_lines": y_edges,
                    })
                    h_segs = get_horizontal_segments(page)

                    for table in tables:
                        kind = classify_table(table, x_edges, expect_header=True)
                        if kind != "data":
                            continue

                        table = detect_merges(table, x_edges, y_edges, h_segs)
                        hdr_idx = detect_header_row(table)
                        keys = header_to_keys(table[hdr_idx])

                        # Build template for continuation pages
                        current_template = build_section_template(x_edges, keys)
                        current_x_edges = list(x_edges)

                        for ri in range(hdr_idx + 1, len(table)):
                            row = table[ri]
                            if all(c is None or (c is not None and not c.strip())
                                   for c in row if c is not None):
                                continue
                            if is_disclaimer_row(row):
                                continue

                            record = {}
                            for ci, cell in enumerate(row):
                                key = keys[ci] if ci < len(keys) else None
                                if key is None:
                                    continue
                                cleaned = clean_cell(cell) if cell is not None else None
                                if cleaned is None:
                                    continue
                                record[key] = cleaned

                            if not any(record.values()):
                                continue
                            if "area" not in record and last_area is not None:
                                record["area"] = f"{last_area} (cont.)"
                            elif "area" in record:
                                last_area = record["area"]

                            area_text = record.get("area", "")
                            area_lines = area_text.count("\n") + 1 if area_text else 0
                            specs_text = record.get("specs", "")
                            specs_lines = specs_text.count("\n") + 1 if specs_text else 0
                            if area_lines >= 3 and specs_lines >= area_lines:
                                record["_review_hint"] = (
                                    "AREA contains multiple line items and SPECS has "
                                    "matching line count. Source PDF may be missing a row separator."
                                )

                            record["_source"] = {
                                "page": page_num,
                                "row_index": ri,
                                "method": "grid",
                            }
                            page_record["items"].append(record)
                            current_section["items"].append(record)

            # =================================================================
            # CASE 2: This page is a continuation (no title)
            # → use template-based extraction; ignore the page's own grid columns
            #   (they often only cover the SPECS+IMAGE+SUPPLIER subset).
            #   But still use this page's y_edges for row boundaries if present.
            # =================================================================
            else:
                if current_template is None:
                    # No template available yet — first page of doc with no
                    # title? Skip.
                    pass
                else:
                    # Use this page's y_edges if available, else cluster by y.
                    items, last_area = extract_continuation_with_template(
                        page, current_template, last_area, y_edges
                    )
                    for it in items:
                        page_record["items"].append(it)
                        current_section["items"].append(it)

            # Footer closes the section
            if has_footer:
                current_section = None
                current_template = None
                current_x_edges = None
                last_area = None

            out["pages"].append(page_record)

    return out


def main():
    if len(sys.argv) != 3:
        print("Usage: python pdf_to_structured_json.py <input.pdf> <output.json>", file=sys.stderr)
        sys.exit(1)
    pdf_path = sys.argv[1]
    out_path = sys.argv[2]
    result = extract_pdf(pdf_path)
    Path(out_path).write_text(json.dumps(result, indent=2, ensure_ascii=False))
    total_items = sum(len(s["items"]) for s in result["sections"])
    print(f"Extracted {len(result['pages'])} pages, {len(result['sections'])} sections, {total_items} items.")
    for s in result["sections"]:
        pg = s["pages"]
        pg_str = f"{pg[0]}-{pg[-1]}" if len(pg) > 1 else str(pg[0]) if pg else ""
        print(f"  - {s['section_title']}: {len(s['items'])} items (pages {pg_str})")


if __name__ == "__main__":
    main()
