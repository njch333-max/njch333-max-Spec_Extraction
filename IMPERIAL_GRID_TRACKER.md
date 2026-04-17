# Imperial Grid Tracker

## Goal
- Stabilize Imperial table/grid structure so `material_rows` are usually correct before semantic cleanup and summary aggregation.
- Prioritize grid boundaries, merged cells, canonical row order, label continuity, and same-cell continuation over downstream string cleanup.
- Treat source-PDF QA as the only acceptance source of truth for Imperial structural parser work.

## Locked Decisions
- Geometry first; LLM only acts as a constrained tie-breaker or repair helper.
- `grid boundary recovery` is the first truth layer for Imperial structure work. Do not treat downstream string cleanup, summary cleanup, or QA fixes as substitutes for missing row/cell boundaries.
- Scope stays `Imperial` first; do not expand structure work to other builders until Imperial blockers are materially reduced.
- Builder routing and QA scope are determined by the website job's assigned Builder, not by PDF header text such as `Client`, `Builder`, logos, or sheet styling. If a job is classified as `Imperial` in the app, treat it as Imperial even when the uploaded sheet originates from another builder's colour-consult format.
- `joinery/material` must stabilize first; `sinkware / appliances overlay` follows after the grid/row layer is stronger.
- `material_rows` remain the Imperial truth layer.
- `AREA / ITEM` must default to the table's original label text for room-card display. Parser normalization may still support tags and constrained repair internally, but the UI must not invent a cleaner replacement title unless the original cell text is clearly missing.
- Room cards preserve source wording first; summaries aggregate only from clean rows or clean subitems.
- `Tap` stays excluded from Imperial primary room cards, primary summary output, and primary PDF QA signoff.
- Imperial PDF QA signoff is strict source-PDF, field-by-field signoff. A populated extracted value is not enough for `pass`.
- Bulk `pass/na` write-backs based only on non-empty checklist values are invalid as final signoff and must not close a cycle.
- Work happens inside the current codebase, not via a parallel parser rewrite:
  - `App/services/extraction_service.py`
  - `App/services/parsing.py`
  - `App/main.py`
- One work cycle may have only one primary blocker. Do not spread a cycle across multiple unrelated Imperial failure modes.

## Current Architecture Baseline
- `App/services/extraction_service.py` now carries `ImperialSeparatorModel` segment provenance, `visual_subrows`, `canonical_row_order`, page-structure bboxes, and cell-ownership provenance. Phase 1A debug overlay artifacts exist, Phase 1B now coalesces adjacent row bands when the boundary is `none` or `inferred_low` and the evidence fits same-cell continuation, and Phase 2A overlays report repaired `grid_rows` while preserving pre-repair evidence as `unrepaired_grid_rows`.
- `App/services/parsing.py` already contains `material_rows`, constrained self-repair, `FieldIssue`, `RepairCandidate`, `RepairVerdict`, `repair_log`, and `revalidation_status`, but it still depends too much on post-assembly cleanup when upstream structure is weak.
- `App/main.py` already renders `material_rows` and gates Imperial summary output via `revalidation_status`.
- Current bottleneck: upstream structure truth is still too soft on merged cells, missing lines, long label continuation, same-cell continuation, and row-cluster ownership. When that layer is weak, `AREA / ITEM` gets polluted by `SPECS / DESCRIPTION`, then room cards and summaries both inherit the error.

## Phase Plan
### Phase 1: Grid Truth
- Upgrade `ImperialSeparatorModel` into a segment-level separator model.
- Make separator `source` and `confidence` explicit and testable.
- Stabilize `visual_subrows` and `canonical_row_order`.
- Add page-level debug overlay outputs for grid/edge inspection.
- Exit criteria:
  - `row_order_drift` only fires on real canonical-order conflicts.
  - merged-cell evidence is explicit instead of inferred later from broken rows.

### Phase 2: Row Assembly
- Make `AREA / ITEM` anchored row assembly the hard path.
- Solve label continuity before continuation/value assembly.
- Solve same-cell continuation before supplier/notes cleanup.
- Apply strong termination rules for short-value rows.
- Exit criteria:
  - `material_rows` stop losing legal continuation on desk / shelf / robe / study style pages.
  - long labels such as `INCLUDING ...` or `GLASS DOORS ONLY` no longer split into fake rows.

### Phase 3: Semantic / Summary
- Keep raw rows source-like; do not over-canonicalize at raw-row level.
- Split `HANDLES` into semantic subitems before summary aggregation.
- Build `Door Colours / Handles / Bench Tops` summaries only from clean rows or clean subitems.
- Keep `FieldIssue / RepairCandidate / RepairVerdict / revalidation_status`, but only for constrained repair.
- Tighten `sinkware / appliances overlay` to cluster-local / row-first behavior.
- Exit criteria:
  - summaries stop masking broken raw rows.
  - sinkware/appliance overlays stop cross-room contamination on Imperial overlays.

## Regression Matrix
| Job | Known failures | Expected fixes | Latest verified run | PDF QA status |
| --- | --- | --- | --- | --- |
| 52 | Splashback/base boundary, knob spillover, placeholder appliance handling | Stable row boundary, clean handle rows, appliance placeholders retained correctly | `1972` | `passed` |
| 55 | Handle row separation, `NO HANDLES OVERHEADS` vs `ACCESSORY`, handle summary holes | Strong row separation, no accessory bleed, clean handle summary | `1995` | `passed` |
| 56 | Handle row completeness, `HANDLES to OVERHEADS`, soft-close stability | Complete handle rows and stable `Hinges & Drawer Runners` | `1996` | `passed` |
| 59 | Handle summary canonicalization / grouping | Canonical handle grouping without over-merging | `2065` | `passed` |
| 49 | Appliance checklist evidence refresh, clean bench-top summary text, strict field-by-field QA on alfresco material/appliance/sink pages | Clean `Bench Tops` summary from room-card truth, richer appliance QA evidence from current snapshot, strict source-PDF signoff without non-empty bulk pass | `2182` | `passed` |
| 50 | Door-colour summary fallback, handle family fallback, final room-field backfill | Clean `Door Colours / Handles / Bench Tops` summary from raw rows, clean `door_colours_island` derivation, strict field-by-field QA on kitchen/appliance/sink pages | `2170` | `passed` |
| 60 | Continuation-heavy desk / shelf / robe rows | Legal continuation retained, raw rows at least as strong as the source-heavy gold sample | `2037` | `passed` |
| 61 | Base/upper contamination, handle grouping, appliance loss, sinkware ownership | Raw-row boundary cleanup, handle family separation, appliance recovery, sinkware ownership fix | `2064` | `passed` |
| 62 | Short-value termination, pantry/kitchen handle subitems, sinkware/appliance overlays | Long-label stability completed; next focus is short-value hard stops, handle subitems, overlay isolation | `2209` | `targeted regression checked` |
| 67 | First-row header/meta bleed into `BENCHTOP`, supplier/notes column split, raw spelling/case preservation, appliances/sinkware row-first ownership | Hard `content_grid` boundary, cell-owned supplier/notes split, `IMAGE` ignored as content, summary pollution gate, row-first appliance capture | `2207` | `passed` |

`tests/fixtures/imperial_37867_gold.json` remains the highest-priority Imperial structural regression fixture.

## Open Problems
- Segment-level separator provenance has a first local implementation and debug overlay output; it still needs repeated live-PDF use to expose weak edge cases.
- Phase 1B local implementation now handles the first class of adjacent-band failures: no-boundary / `inferred_low` same-cell continuation can merge before cell extraction, while `visible` and `inferred_high` remain hard row boundaries.
- Remaining risk in this area: coalescing must stay conservative. Supplier-only prelude bands such as `Polytec` before a later cabinetry-colour row must not merge into the previous complete row.
- Short-value row termination is still too weak on `KICKBOARDS`, `LIGHTING`, and similar rows.
- Handle cells still need first-class subitem modeling instead of raw-string-first splitting.
- `sinkware / appliances` remain structurally weaker than `joinery/material`.
- Active live blocker cleared: `job 67 / run 2207` passed strict source-PDF QA after hard-boundary cleanup. Phase 1A grid-debug work is now the current structural focus unless a new live failure is reported.

## Last Verified Live Jobs
- `job 52 / run 1972`: `passed`
- `job 55 / run 1995`: `passed`
- `job 56 / run 1996`: `passed`
- `job 59 / run 2065 / build local-7fd87645`: `passed` with `39 pass / 1 na / 0 fail / 0 pending`. The live verification in this cycle confirmed the canonical four-family handle summary (`UPPER - FINGERPULL`, `BASE - BEVEL EDGE FINGERPULL`, `TALL - S225.280.MBK.`, `CHUTE DOOR - S225.160.MBK.`) and current raw-row contamination checks around handles.
- `job 49 / run 2182 / build local-cd4b984e`: `passed` with strict field-by-field PDF QA and `19 pass / 1 na / 0 fail / 0 pending`. This cycle verified the clean `Bench Tops` summary value, rebuilt the stale appliance QA checklist from the current snapshot so `Inset BBQ / Side Burner / Bar Fridge` carry the richer room-material evidence, and explicitly cleaned up stale follow-up runs `2181` and `2183` after their worker PIDs were confirmed dead.
- `job 50 / run 2170 / build local-0697aa6e`: `passed` with strict field-by-field PDF QA. This cycle verified the repaired `Door Colours` summary fallback, the three expected handle families only, current `BENCHTOP (INCL PANTRY)` summary input, kitchen sink carry-through, and the current four-row appliance capture for `Cooktop / Oven / Rangehood / Dishwasher`.
- `job 60 / run 2037 / build local-c061c5e6`: `passed`
- `job 62 / run 2067 / build local-7fd87645`: `passed` with `33 pass / 22 na / 0 fail / 0 pending`. Pantry long-label continuation remains stable; kitchen/dry bar/laundry short-value termination, hanging-rail preservation, current sinkware separation, and the current five-row appliance capture were re-verified on the latest build.
- `job 61 / run 2064 / build local-7fd87645`: `passed` with `42 pass / 4 na / 0 fail / 0 pending`. The live fixes verified in this cycle were walk-in-robe hanging-rail continuation restoration, wet-area basin-to-sink fallback for room cards and PDF QA, stable five-row appliance capture, and preserving the recovered handle / cabinetry cleanup on the current snapshot.
- `job 64 / run 2139 / build local-3c9b6cd2`: `passed` with `62 pass / 1 na / 0 fail / 0 pending`. The strict field-by-field QA on this run re-verified the repaired `GPO / ACCESSORIES` separation, original flooring case preservation (`Timber` / `Tiled`), sink/basin taphole carry-through, and current appliance capture including `Bar Fridge`. The only `N/A` item is `KITCHEN / sink`, because no kitchen sink row exists in the source sinkware page.
- `job 64 / run 2212 / build local-7cc74371`: targeted regression check passed after the hard-boundary cycle. `GPO` no longer appears as a separate row and `ACCESSORIES` renders `GPO - Double Powerpoint with 2xUSB sockets - Black` with `Island bench, front of MW cupboard`; flooring remains source-case `Timber`.
- `job 67 / run 2207 / build local-d251ab53`: `passed` with strict source-PDF QA. Verified `KITCHEN & PANTRY / BENCHTOP` as `[By Others] - 40mm Stone | WFE x 1`, clean `Polytec / BLACK - MATT / Variation for Black - Venette` supplier-notes ownership, source spelling `Mirrorred`, seven appliance rows including `Specs - TBC`, and sinkware/basin taphole ownership including `behind sink` / `behind basin`.
- `job 41 / run 2155 / build local-9df1bf9a`: `passed` with strict field-by-field PDF QA. The live fixes verified in this cycle were `LAUNDRY + STORAGE NOOK` handle-family room ownership, `KICKBOARDS / NO HANDLES OVERHEADS / FEATURE COLOUR ...` short-value cleanup, and Imperial appliance dedupe so the Bosch `Cooktop` row no longer absorbs `Oven` evidence.
- `job 51 / run 2165 / build local-478c8854`: `passed` with `39 pass / 3 na / 0 fail / 0 pending`. The live fixes verified in this cycle were preserving `OVERHEAD CABINETS TO BE OPEN SHELVES` through room-card rendering, retaining split `Blossom White RAVINE / Blossom White Matt` door-colour summary entries, and completing strict field-by-field PDF QA against the source kitchen/study, appliance, and sinkware sheets.
- `2026-04-13 non-QA rerun coverage`: all remaining unique-address Imperial jobs outside the tracked blocker matrix were fresh rerun once and completed successfully on the current production worker queue. Deduped coverage set: `job 51/run 2085`, `50/2086`, `49/2087`, `48/2088`, `47/2089`, `44/2090`, `43/2091`, `42/2092`, `41/2095`, `40/2096`, `38/2098`, `36/2107`, `35/2108`, `34/2113`, `32/2114`, `31/2115`, `27/2116`. No duplicate addresses were found in the visible Imperial inventory at the time of the sweep.
- `tests/fixtures/imperial_37867_gold.json` remains the highest-priority structural regression fixture.

## Next Actions
- Primary live blocker: none after `job 67 / run 2207` signoff.
- Current structural target: rerun `job 64` after fixing the downstream postprocess/display rollbacks found on fresh runs `2213`, `2214`, `2215`, `2216`, `2217`, `2218`, and `2219`. The deployed grid/row assembler correctly reassigned the weak-boundary leading `GPO` fragment into `ACCESSORIES`, but parser postprocess then trimmed the accepted `GPO` prefix back out of the final snapshot. Later reruns exposed handle label contamination (`Momo HANDLES oval`), a provenance-only case where the final label was already `HANDLES` but the valid `Momo` brand prefix still lived in the label-cell provenance, visual-subrow rebuild trimming that accepted prefix back out, display/checklist rendering using suffix-only visual fragments, a boundary-straddling size prefix where `450mm BIN` put the `450mm` value fragment into the `AREA / ITEM` label, and the same `450mm` prefix being lost from display/checklist visual fragments after raw rows were correct. Local fixes now preserve accepted `GPO -> ACCESSORIES` evidence, repair handle label/value spillover before display/summary, and move size prefixes such as `450mm` back to the value side through raw rows, visual-fragment display, and PDF-QA checklist values when the source label resolves to `BIN`.
- Target order:
  1. Deploy the postprocess rollback and provenance-backed handle-prefix fix
  2. Run a targeted live parser rerun for `job 64`
  3. Source-PDF check the rerun for `ACCESSORIES / GPO`, flooring source case, and existing sink/appliance behavior
  4. Phase 1C only if future overlays cannot explain a boundary; current live overlays are explainable
  5. Phase 3 semantic follow-up remains later: handle subitems, sinkware cluster-local assignment, and appliance row-first tightening
- Standing rule during live analysis:
  - if `AREA / ITEM` and `SPECS / DESCRIPTION` are bleeding together, treat that as a grid/row-assembly blocker first, not a summary or UI bug
  - do not close a cycle by only cleaning the displayed text if the raw row boundary is still wrong
- Mandatory acceptance loop for every Imperial structural cycle:
  1. read this tracker before starting
  2. choose exactly one primary blocker
  3. run local tests / compile
  4. deploy
  5. rerun the target live job
  6. compare against the source PDF
  7. complete strict field-by-field PDF QA
  8. update this tracker before closing the cycle

## Change Log
- `2026-04-12`: Tracker created. Current Imperial architecture baseline, phase plan, regression matrix, and live acceptance history were imported from the existing codebase and live job history.
- `2026-04-12`: First tracked `job 62` cycle completed. Long label continuation for `PANTRY` was verified live on `run 2047 / build local-6b0bfd37`; `UPPER CABINETRY COLOUR INCLUDING TALL OPEN SHELVING` now holds as a single clean raw row with `[Polytec] - Black Wenge - Venette`. The next primary blocker remains short-value row termination (`KITCHEN / DRY BAR / LAUNDRY / WIR`), followed by handle-cell subitems, sinkware cluster-local assignment, and appliances row-first capture.
- `2026-04-13`: `job 62` reached live signoff on `run 2060 / build local-2999aec8`. PDF QA is now `passed` with `33 pass / 22 na / 0 fail / 0 pending`. The extraction-side fixes verified in this cycle were pantry long-label continuation, pantry handle subitems, short-value termination improvements across kitchen/dry bar/laundry, sinkware kitchen/laundry separation, and restoring the current five-row appliance capture. The primary live blocker now moves to `job 61`.
- `2026-04-14`: `job 50` reached strict field-by-field signoff on `run 2170 / build local-0697aa6e`. This cycle fixed the handle-summary fallback duplication, restored `Door Colours` summary fallback for safe cabinetry-colour rows, and forced a final Imperial room-field backfill pass after snapshot cleaning so kitchen colour groups and summary inputs align with cleaned `material_rows`. The next live blocker moves to `job 49`.
- `2026-04-14`: `job 49` reached strict live signoff on `run 2182 / build local-cd4b984e`. This cycle fixed the bench-top summary fallback path so the clean room-card `BENCHTOP` string wins over dirtier fallback text, improved Imperial appliance checklist generation to prefer richer matching room-material evidence, rebuilt the stale production verification row from the current snapshot, and then completed strict field-by-field PDF QA with `19 pass / 1 na / 0 fail / 0 pending`. Stale follow-up runs `2181` and `2183` were marked failed after their worker PIDs were confirmed dead. The next unverified Imperial rerun in the queue becomes `job 48`.

- `2026-04-13`: `job 61` reached live signoff on `run 2064 / build local-7fd87645`. PDF QA is now `passed` with `42 pass / 4 na / 0 fail / 0 pending`. The verified live changes in this cycle were hanging-rail continuation restoration, basin fallback into Imperial `sink` display/checklist fields, and retaining the current five-row appliance capture. The primary live blocker now moves to `job 59`.
- `2026-04-13`: `job 59` reached live signoff on `run 2065 / build local-7fd87645`. PDF QA is now `passed` with `39 pass / 1 na / 0 fail / 0 pending`. The verified live changes in this cycle were the canonical four-family handle summary and maintaining clean room ownership for the current handle rows. The primary live blocker now returns to `job 62`.
- `2026-04-13`: `job 62` reached refreshed live signoff on `run 2067 / build local-7fd87645`. PDF QA is now `passed` with `33 pass / 22 na / 0 fail / 0 pending`. The re-verified live behavior in this cycle was stable short-value row termination across `KITCHEN / PANTRY / DRY BAR / LAUNDRY`, preserved hanging-rail rows, stable sinkware separation for `KITCHEN / LAUNDRY`, and retaining the current five-row appliance capture on the latest build.
- `2026-04-13`: Remaining Imperial jobs outside the tracked regression matrix were deduped by visible address and fresh rerun once to refresh production snapshots without reprocessing duplicate addresses. Completed non-QA rerun coverage: `job 51/run 2085`, `50/2086`, `49/2087`, `48/2088`, `47/2089`, `44/2090`, `43/2091`, `42/2092`, `41/2095`, `40/2096`, `38/2098`, `36/2107`, `35/2108`, `34/2113`, `32/2114`, `31/2115`, `27/2116`. No duplicate addresses were present in the visible Imperial inventory at the time of the sweep; `job 51` remained address-poor (`37993`) but did not collide with another visible Imperial job title.
- `2026-04-13`: The earlier `job 64 / run 2123` bulk `pass/na` write-back was invalidated. It was based on checklist value presence rather than strict source-PDF field-by-field review. `job 64` remains open until `ACCESSORIES` row-boundary contamination, flooring raw-case preservation, and the remaining source-PDF mismatches are re-reviewed and signed off correctly.
- `2026-04-14`: `job 64` reached corrected live signoff on `run 2139 / build local-3c9b6cd2`. PDF QA is now `passed` with `62 pass / 1 na / 0 fail / 0 pending`. This closed the previously invalidated `run 2123` signoff after strict source-PDF review confirmed `GPO / ACCESSORIES` separation, source-case flooring preservation, and the current sink/appliance overlay behavior. The primary live blocker now moves to `job 41`.
- `2026-04-14`: `job 41` reached strict live signoff on `run 2155 / build local-9df1bf9a`. The cycle closed after fixing Imperial appliance dedupe so the Bosch `Cooktop` row no longer absorbed `Oven` evidence. The next unverified Imperial rerun in the queue becomes `job 51`.
- `2026-04-14`: `job 51` reached strict live signoff on `run 2165 / build local-478c8854`. PDF QA is now `passed` with `39 pass / 3 na / 0 fail / 0 pending`. This cycle closed after fixing a display-layer rollback that was showing `OVERHEAD CABINETS` instead of the already-correct stored label `OVERHEAD CABINETS TO BE OPEN SHELVES`; the continuation repair now updates provenance `raw_area_or_item` as well. The next unverified Imperial rerun in the queue becomes `job 50`.
- `2026-04-17`: `job 67` was promoted to the active Imperial hard-boundary blocker. Local implementation now rejects header/meta/table-heading polluted layout candidates before they can override clean cell-grid rows, treats `IMAGE` as non-content, splits supplier/note tails by recovered cell ownership, adds a summary pollution gate, and adds row-first Imperial appliance capture for layout rows. Local smoke and compile checks passed; live deploy/rerun/PDF-QA is pending.
- `2026-04-17`: `job 67` reached strict source-PDF signoff on `run 2207 / build local-d251ab53`. The cycle fixed `content_grid` hard-boundary leakage, supplier/notes ownership for `Polytec Variation for Black - Venette`, raw spelling preservation for `Mirrorred`, row-first appliance capture for seven rows, `WFE x 1` visual-break preservation, and sinkware taphole tail preservation for `behind sink/basin`. PDF QA was written as `passed` for the latest raw snapshot.
- `2026-04-17`: Post-`job 67` regression found `job 64 / run 2208` still splitting `GPO` out of `ACCESSORIES`. A generic Imperial repair now merges non-adjacent `GPO` spillover fragments back into `ACCESSORIES` and protects that merged value from later self-repair rollback. `job 64 / run 2212 / build local-7cc74371` passed targeted regression (`ACCESSORIES` restored; flooring source case preserved). `job 62 / run 2209 / build local-d251ab53` completed as the companion regression check for long-label and short-value stability.
- `2026-04-17`: Phase 1A grid-debug implementation added page-structure bboxes, cell-ownership provenance, word-level bbox propagation, `content_grid`/footer/table-header debug payloads, and `tools/imperial_grid_debug.py` for JSON/SVG overlay generation under `tmp/imperial_grid_debug/`. Local structural tests now cover bbox provenance, content-grid/footer exclusion, `inferred_low` non-hard-split behavior, and debug artifact generation.
- `2026-04-17`: Phase 1A was deployed and exercised on real source PDFs for `job 67`, `job 64`, and `job 62`. JSON/SVG overlays were generated under `/opt/spec-extraction/tmp/imperial_grid_debug_live/`. The overlays confirmed correct high-level `content_grid`/footer separation and cell bboxes, but also proved the next blocker: no-boundary or `inferred_low` adjacent bands are still being handed to row assembly as separate bands. Evidence: `job 67 page 1` has `WFE x 1` as a second description band under `BENCHTOP` with no hard separator; `job 64 page 1` has `GPO` and `ACCESSORIES` split across adjacent bands with only weak separator evidence; `job 62 page 2` has `UPPER CABINETRY COLOUR INCLUDING` and `TALL OPEN SHELVING` split by `inferred_low` before a later `inferred_high` separator.
- `2026-04-17`: Phase 1B local implementation added separator-aware row-band coalescing before cell extraction. `visible` and `inferred_high` separators remain hard boundaries; `none` and `inferred_low` can merge only when the current band is a same-cell continuation or label continuation. Local tests cover `BENCHTOP + WFE x 1`, `UPPER CABINETRY COLOUR INCLUDING + TALL OPEN SHELVING`, visible separator non-merge, `inferred_low` non-hard visual subrows, and a regression that prevents supplier-only `Polytec` preludes from merging into `SPLASHBACK`. Verification: `python -m compileall App tests tools` passed and `829` smoke tests passed.
- `2026-04-17`: Phase 1B was deployed as build `aad69e3` and live overlays were regenerated under `/opt/spec-extraction/tmp/imperial_grid_debug_live_phase1b/`. Verified live: `job 67 page 1` now emits `BENCHTOP / 40mm Stone WFE x 1` from two coalesced bands (`visible`, `none`); `job 62 page 2` now emits `UPPER CABINETRY COLOUR INCLUDING TALL OPEN SHELVING / Black Wenge - Venette / Polytec` from two coalesced bands; `job 64 page 1` now coalesces `BENCHTOP / WFE's x 2` and keeps the trailing `cupboard)` with `ACCESSORIES`, but still leaves `GPO` as a separate leading row fragment. That remaining case is classified as Phase 2A row-assembler ownership, not Phase 1B separator recovery.
- `2026-04-17`: Phase 2A local implementation added `ACCESSORIES` / `GPO` canonical row specs and a constrained five-column repair for leading `GPO` fragments before `ACCESSORIES`. The repair only fires across soft boundaries and only when `GPO` carries accessory-value evidence such as `Powerpoint`, `USB`, or `socket`; a visible / `inferred_high` boundary blocks the merge. Local tests now cover both the positive `GPO -> ACCESSORIES` ownership repair and the visible-boundary non-merge case. Verification: `python -m compileall App tests tools` passed and `831` smoke tests passed.
- `2026-04-17`: Phase 2A debug overlay semantics were corrected so `grid_rows` reflects the same repaired row view used by the parser, while `unrepaired_grid_rows` keeps the pre-repair five-column rows for boundary diagnosis. This prevents false live-analysis failures where the parser output is fixed but the overlay still displays an unrepaired intermediate row split.
- `2026-04-17`: Phase 2A was deployed as build `7c4728e` and `job 64 page 1` overlay was regenerated under `/opt/spec-extraction/tmp/imperial_grid_debug_live_phase2a/job64/`. Verified live: repaired `grid_rows` now shows `BENCHTOP / 20mm Stone WFE's x 2 / By Others` and one `ACCESSORIES / GPO - Double Powerpoint with 2xUSB sockets - Black- (Island bench, front of MW cupboard)` row with `leading_fragment_repair = gpo_to_accessories`; `GPO` remains only in `unrepaired_grid_rows` as the original weak-boundary fragment. The production parser function returned the same rows with `unresolved 0`. A fresh full `job 64` rerun and source-PDF QA are still required before closing the Phase 2A cycle.
- `2026-04-17`: Fresh `job 64 / run 2213` exposed a downstream rollback: the overlay/parser function preserved the full `GPO - Double Powerpoint with 2xUSB sockets - Black- (Island bench, front of MW cupboard)` value, but final snapshot postprocess reduced it to `sockets - Black` plus notes. Local fix now bypasses legacy accessory `GPO` prefix trimming when provenance has `leading_fragment_repair = gpo_to_accessories` or `merged_gpo_spillover`. Verification: targeted accessory tests passed, `python -m compileall App tests tools` passed, and `833` smoke tests passed.
- `2026-04-17`: Fresh `job 64 / run 2214` then exposed handle label/value spillover: `Momo HANDLES oval` was treated as a handle label and pulled `oval wardrobe tube` into the handle summary. Local fix now normalizes contaminated labels back to `HANDLES`, carries valid supplier/brand prefix text such as `Momo` into the handle description, moves supplier-tail install notes such as `Furnware - Horizontal on ALL` into notes, and dedupes `Oval wardrobe tube` hanging-rail prefixes. Verification: targeted handle/hanging-rail tests passed, `python -m compileall App tests tools` passed, and `835` smoke tests passed.
- `2026-04-17`: Fresh `job 64 / run 2215` exposed the remaining handle-prefix edge case: LAUNDRY had already normalized the row label to `HANDLES`, but the valid `Momo` prefix still existed only in `raw_area_or_item` / label-cell provenance. Local fix now recovers handle brand spillover from provenance as well as the current label. Verification: targeted handle tests passed, `python -m compileall App tests tools` passed, and `836` smoke tests passed.
- `2026-04-17`: Fresh `job 64 / run 2216` showed the provenance-backed `Momo` prefix was recovered but then removed by the later visual-subrow rebuild because the visual subrow only contained `flapp pull...`. Local fix now preserves valid brand-prefix + visual-text descriptions instead of replacing them with the suffix-only visual text. Verification: targeted handle tests passed, `python -m compileall App tests tools` passed, and `836` smoke tests passed.
- `2026-04-17`: Fresh `job 64 / run 2217` showed raw `material_rows` were correct but display/checklist generation still preferred suffix-only visual fragments for KITCHEN/PANTRY handles. Local fix now applies the same handle-label spillover and supplier-tail split when converting visual fragments into display lines, so `[Furnware] - Momo ... - (Horizontal on ALL)` survives into UI and PDF QA checklist values. Verification: targeted display tests passed, `python -m compileall App tests tools` passed, and `837` smoke tests passed.
- `2026-04-17`: Fresh `job 64 / run 2218` showed `450mm` was still being treated as part of the `AREA / ITEM` label for `BIN`, even though word bbox evidence places it on the description side of the visible grid boundary. Local fix now normalizes `450mm BIN` back to label `BIN` and prefixes `450mm` to the row value. Verification: targeted BIN/display tests passed, `python -m compileall App tests tools` passed, and `838` smoke tests passed.
- `2026-04-17`: Fresh `job 64 / run 2219` showed the raw `BIN` material row was corrected, but PDF-QA checklist/display generation still rebuilt from visual fragments and dropped the recovered `450mm` prefix. Local fix now applies the same label-spillover repair when non-handle visual fragments are rendered, so `[Furnware] - 450mm Short Pull-Out...` survives into room-card display and checklist values. Verification: targeted BIN/display tests passed, `python -m compileall App tests tools` passed, and `838` smoke tests passed.
