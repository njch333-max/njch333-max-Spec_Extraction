# Task — Bug H Reconnaissance: dump v6 area_or_item merge data, no code changes

**Owner**: Jason · **Executor**: Codex · **Reviewer**: Claude
**Type**: Reconnaissance / data analysis only. **No code changes**, **no doc sync**, **no deploy**, **no commits**.
**Goal**: produce evidence to decide between Option A (sibling repo cell-grid fix) vs Option B (main repo adapter split heuristic) for Bug H.

---

## Context

Job 73 (37558-2 Lot 532 Sandpiper Terrace, Imperial v6) Spec List shows merged `area_or_item` row names on KITCHEN page:

- `BENCHTOP ISLAND CABINETRY COLOUR (INCL. BACK OF ISLAND CURVE AND COLUMN)` — should be 2 rows (`BENCHTOP` + `ISLAND CABINETRY COLOUR (incl...)`)
- `KICKBOARDS GPO'S` — should be 2 rows
- `BIN ACCESSORIES LED'S` — should be 3 rows

This is Bug H. The hypothesis is that v6 extractor's cell-grid recovery merges adjacent row labels into one `area_or_item` cell when the PDF layout has IMAGE column content nearby.

We need data, not assumptions, to choose the fix path. **Do not write any production code in this task.** This task only produces a report.

Bug H related but separate manifestation to also examine: **Bug I** — BED1&2 & RUMPUS HANDLES Material Summary entry contains `As per drawings` which is LED's content leaking into HANDLES. Likely same root cause family (cell-grid recovery). Include BED1&2 dump so we can confirm.

---

## What to dump (raw JSON, verbatim)

For **prod job 73** (`/jobs/<id>/raw-snapshot` JSON or directly from SQLite store, whichever is faster):

1. Full `material_rows` list for KITCHEN (this is the one with 3 merged labels — primary evidence)
2. Full `material_rows` list for PANTRY (clean comparison — should NOT have merging since PDF layout differs)
3. Full `material_rows` list for LAUNDRY & MUD ROOM (also has IMAGE column, see if it merged similarly)
4. Full `material_rows` list for BED1&2 & RUMPUS (Bug I evidence — `As per drawings` contamination)
5. The room-level `handles` field, `flooring` field, and any other room fields that might have informed `synthesized_from_room_handles` backfill — for the same 4 rooms

For **one signed-off job** (use **job 64** — 38146_2, the most recent signed-off Imperial v6 fixture if available, otherwise 67):

6. Full `material_rows` list for that job's KITCHEN, looking for whether label merging exists in older signed-off output too

Paste each dump as a fenced JSON block in the report. Truncate `repair_log` / `repair_candidates` / `_repair_events` arrays to first 1 element each + `... (N more)` marker if any single row exceeds ~60 lines, to keep the report readable. **Do not truncate** `area_or_item`, `specs_or_description`, `supplier`, `notes`, `display_lines`, `display_groups`, `provenance`, `tags`, `page_no`, `row_order` — those are the primary fields we're analyzing.

If the snapshot is not directly accessible from local, document the exact command / SQL / endpoint used to retrieve it.

---

## Analysis required (markdown table + prose)

### Table 1 — Merged-label row anatomy (KITCHEN)

For each merged-label row in job 73 KITCHEN, fill in:

| `area_or_item` (verbatim) | `specs_or_description` (verbatim, escape newlines as `\n`) | `supplier` | `notes` | `display_lines` | how many distinct PDF row names are concatenated? | does the specs text already split cleanly by `\n` or `\|` into N segments matching the row name count? |
|---|---|---|---|---|---|---|

This table answers the **decisive question for Option A vs B**:
- If `specs_or_description` already splits cleanly into N segments matching N row names (data shape (a)) → **Option B is feasible**: adapter can heuristic-split and re-pair specs/supplier to row names
- If `specs_or_description` is a single un-segmented blob (data shape (b)) → **Option B is impossible without semantic parsing**, Option A (sibling repo) is the only safe fix

### Table 2 — Cross-room pattern check

For PANTRY / LAUNDRY / BED1&2 KITCHEN, list any rows where `area_or_item` is NOT a single canonical row name (i.e. contains a space-separated 2nd token that matches a known Imperial row label). Use this to confirm whether the merging is KITCHEN-page-specific (PDF layout effect) or systematic across v6.

Known Imperial row label tokens for the merge detection (case-insensitive):
`BENCHTOP`, `ISLAND CABINETRY COLOUR`, `BACK WALL & COFFEE NOOK INTERNAL CABINETRY COLOUR`, `FLOATING SHELVES`, `KICKBOARDS`, `GPO'S`, `HANDLES`, `BIN`, `ACCESSORIES`, `LED'S`, `LED LIGHTING`, `BASE CABINETRY COLOUR`, `UPPER CABINETRY COLOUR`, `TALL CABINETRY COLOUR`, `MIRRORED SHAVING CABINET (EXTERNAL PANELS ONLY)`, `BENCHTOP (SEAT)`, `HANGING RAIL`, `HAMPER`, `JEWELLERY INSERT`, `GLASS TOP`, `RAIL`, `EXTRA TOP IN MASTER BEDROOM (BED 1 -MASTER)`, `CABINETRY COLOUR & TOP (BED 1- MASTER)`, `CABINETRY COLOUR (BED 2) AND KICKBOARDS`, `BENCHTOP AND SHELVES COLOUR - (BED 2)`, `CABINETRY COLOUR (RUMPUS)`, `BENCHTOP COLOUR (RUMPUS)`, `LED'S (BED 2)`

(This list is a starting hint, not a frozen contract — codex can expand if other tokens appear in the dump.)

### Table 3 — v6 vs signed-off comparison (KITCHEN)

For job 73 (v6) KITCHEN vs job 64 (or 67) KITCHEN, side by side:

| field | job 73 v6 | signed-off job (specify which) |
|---|---|---|
| number of `material_rows` for KITCHEN | | |
| any `area_or_item` containing 2+ canonical row name tokens? | | |
| `provenance.source_provider` distribution | | |

This answers: **is the merge a v6 regression (signed-off path didn't have it) or a long-standing limit?**

### Table 4 — Bug I (LED's contamination) anatomy

For job 73 BED1&2 & RUMPUS HANDLES rows, list each row's `area_or_item` / `specs_or_description` / `display_lines`. We expect to find `As per drawings` either:
- (i) as a separate LED's row that got merged into HANDLES (same root cause as Bug H — area_or_item merge)
- (ii) somehow appended to a HANDLES row's `specs_or_description`
- (iii) something else entirely

Identify which.

---

## Recommendation section (final part of the report)

Based on the dumps, codex picks one of:

- **Option A — sibling repo fix recommended** (root cause in `pdf_to_structured_json_v6.py` cell-grid recovery; main repo can't safely split). Justify with specific evidence rows.
- **Option B — adapter split heuristic recommended** (data shape supports clean per-PDF-row split in main repo without touching extractor). Sketch the split algorithm in 5-10 lines of pseudo-code, identifying:
  - how to detect a merged `area_or_item`
  - how to assign each split's `specs_or_description` / `supplier` / `notes` / `display_lines`
  - what to do if alignment is ambiguous (refuse to split? warn?)
- **Option C — neither cleanly works, more PDFs needed** (hold for more job samples, list which job IDs would help). Acceptable answer if the data genuinely doesn't support a clear pick.

If Option B is recommended, also state:
- whether Bug I (LED's contamination) is reachable by the same heuristic
- whether Bug L/M (Door Colours / Bench Tops bucket misrouting) auto-resolves once Bug H is fixed

---

## Hard constraints

- **No code changes anywhere** — no edits to `App/main.py`, no edits to sibling repo, no edits to tests, no edits to project docs (AGENTS / Arch / PRD / Project_state)
- **No commits** — any local file you create for the report should be the report markdown only, in `docs/BUG_H_RECON_REPORT.md`
- **No deploy**
- **No `git add` / `git commit` / `git push`** — Jason will commit the report after reviewing it
- **No memory updates** — Claude reviewer handles memory based on findings
- **Read-only on production data** — do not mutate snapshots while dumping

---

## Delivery (5 items, hard requirement)

This is a recon task, so the standard 6-item code-change gate is replaced by:

1. **Report file** — `docs/BUG_H_RECON_REPORT.md` written, containing all 4 tables + raw JSON dumps + recommendation
2. **Exact retrieval method** — paste the command(s) / SQL / endpoint used to extract each snapshot, so Jason can reproduce
3. **Job IDs verified** — confirm that prod job 73 is `37558-2 Lot 532 Sandpiper Terrace` (matches the Bug G fix-cycle PDF) and confirm which signed-off job ID was used for comparison
4. **Pick one option** with explicit justification — Option A / B / C, no fence-sitting unless C is genuinely the right answer
5. **Surprises / deviations** — anything in the snapshots that contradicts assumptions made in this task brief, or any field/structure not anticipated. If none, write `none observed`.

Reply must be the report content (or a clear pointer to `docs/BUG_H_RECON_REPORT.md`) plus items 2-5. Do not reply "done" without the report file existing.
