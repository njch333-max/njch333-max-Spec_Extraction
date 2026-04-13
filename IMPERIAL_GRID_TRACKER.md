# Imperial Grid Tracker

## Goal
- Stabilize Imperial table/grid structure so `material_rows` are usually correct before semantic cleanup and summary aggregation.
- Prioritize grid boundaries, merged cells, canonical row order, label continuity, and same-cell continuation over downstream string cleanup.
- Treat source-PDF QA as the only acceptance source of truth for Imperial structural parser work.

## Locked Decisions
- Geometry first; LLM only acts as a constrained tie-breaker or repair helper.
- Scope stays `Imperial` first; do not expand structure work to other builders until Imperial blockers are materially reduced.
- `joinery/material` must stabilize first; `sinkware / appliances overlay` follows after the grid/row layer is stronger.
- `material_rows` remain the Imperial truth layer.
- Room cards preserve source wording first; summaries aggregate only from clean rows or clean subitems.
- `Tap` stays excluded from Imperial primary room cards, primary summary output, and primary PDF QA signoff.
- Work happens inside the current codebase, not via a parallel parser rewrite:
  - `App/services/extraction_service.py`
  - `App/services/parsing.py`
  - `App/main.py`
- One work cycle may have only one primary blocker. Do not spread a cycle across multiple unrelated Imperial failure modes.

## Current Architecture Baseline
- `App/services/extraction_service.py` already contains `ImperialSeparatorModel`, `visual_subrows`, and `canonical_row_order`, but they are not yet a full segment-level grid truth layer.
- `App/services/parsing.py` already contains `material_rows`, constrained self-repair, `FieldIssue`, `RepairCandidate`, `RepairVerdict`, `repair_log`, and `revalidation_status`, but it still depends too much on post-assembly cleanup when upstream structure is weak.
- `App/main.py` already renders `material_rows` and gates Imperial summary output via `revalidation_status`.
- Current bottleneck: upstream structure truth is still too soft on merged cells, missing lines, long label continuation, and same-cell continuation.

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
| 60 | Continuation-heavy desk / shelf / robe rows | Legal continuation retained, raw rows at least as strong as the source-heavy gold sample | `2037` | `passed` |
| 61 | Base/upper contamination, handle grouping, appliance loss, sinkware ownership | Raw-row boundary cleanup, handle family separation, appliance recovery, sinkware ownership fix | `2064` | `passed` |
| 62 | Short-value termination, pantry/kitchen handle subitems, sinkware/appliance overlays | Long-label stability completed; next focus is short-value hard stops, handle subitems, overlay isolation | `2067` | `passed` |

`tests/fixtures/imperial_37867_gold.json` remains the highest-priority Imperial structural regression fixture.

## Open Problems
- Segment-level separator provenance is still incomplete.
- Short-value row termination is still too weak on `KICKBOARDS`, `LIGHTING`, and similar rows.
- Handle cells still need first-class subitem modeling instead of raw-string-first splitting.
- `sinkware / appliances` remain structurally weaker than `joinery/material`.
- No active live blocker is currently open in the tracked Imperial regression matrix.

## Last Verified Live Jobs
- `job 52 / run 1972`: `passed`
- `job 55 / run 1995`: `passed`
- `job 56 / run 1996`: `passed`
- `job 59 / run 2065 / build local-7fd87645`: `passed` with `39 pass / 1 na / 0 fail / 0 pending`. The live verification in this cycle confirmed the canonical four-family handle summary (`UPPER - FINGERPULL`, `BASE - BEVEL EDGE FINGERPULL`, `TALL - S225.280.MBK.`, `CHUTE DOOR - S225.160.MBK.`) and current raw-row contamination checks around handles.
- `job 60 / run 2037 / build local-c061c5e6`: `passed`
- `job 62 / run 2067 / build local-7fd87645`: `passed` with `33 pass / 22 na / 0 fail / 0 pending`. Pantry long-label continuation remains stable; kitchen/dry bar/laundry short-value termination, hanging-rail preservation, current sinkware separation, and the current five-row appliance capture were re-verified on the latest build.
- `job 61 / run 2064 / build local-7fd87645`: `passed` with `42 pass / 4 na / 0 fail / 0 pending`. The live fixes verified in this cycle were walk-in-robe hanging-rail continuation restoration, wet-area basin-to-sink fallback for room cards and PDF QA, stable five-row appliance capture, and preserving the recovered handle / cabinetry cleanup on the current snapshot.
- `2026-04-13 non-QA rerun coverage`: all remaining unique-address Imperial jobs outside the tracked blocker matrix were fresh rerun once and completed successfully on the current production worker queue. Deduped coverage set: `job 51/run 2085`, `50/2086`, `49/2087`, `48/2088`, `47/2089`, `44/2090`, `43/2091`, `42/2092`, `41/2095`, `40/2096`, `38/2098`, `36/2107`, `35/2108`, `34/2113`, `32/2114`, `31/2115`, `27/2116`. No duplicate addresses were found in the visible Imperial inventory at the time of the sweep.
- `tests/fixtures/imperial_37867_gold.json` remains the highest-priority structural regression fixture.

## Next Actions
- Primary live blocker: none currently open
- Target order:
  1. Phase 1 grid-truth follow-up: segment-level separator provenance and debug overlay output
  2. Phase 2 row-assembly follow-up: handle subitem modeling for pantry/kitchen style cells
  3. Phase 3 semantic follow-up: stronger sinkware cluster-local assignment and appliance row-first capture
- Mandatory acceptance loop for every Imperial structural cycle:
  1. read this tracker before starting
  2. choose exactly one primary blocker
  3. run local tests / compile
  4. deploy
  5. rerun the target live job
  6. compare against the source PDF
  7. complete PDF QA
  8. update this tracker before closing the cycle

## Change Log
- `2026-04-12`: Tracker created. Current Imperial architecture baseline, phase plan, regression matrix, and live acceptance history were imported from the existing codebase and live job history.
- `2026-04-12`: First tracked `job 62` cycle completed. Long label continuation for `PANTRY` was verified live on `run 2047 / build local-6b0bfd37`; `UPPER CABINETRY COLOUR INCLUDING TALL OPEN SHELVING` now holds as a single clean raw row with `[Polytec] - Black Wenge - Venette`. The next primary blocker remains short-value row termination (`KITCHEN / DRY BAR / LAUNDRY / WIR`), followed by handle-cell subitems, sinkware cluster-local assignment, and appliances row-first capture.
- `2026-04-13`: `job 62` reached live signoff on `run 2060 / build local-2999aec8`. PDF QA is now `passed` with `33 pass / 22 na / 0 fail / 0 pending`. The extraction-side fixes verified in this cycle were pantry long-label continuation, pantry handle subitems, short-value termination improvements across kitchen/dry bar/laundry, sinkware kitchen/laundry separation, and restoring the current five-row appliance capture. The primary live blocker now moves to `job 61`.

- `2026-04-13`: `job 61` reached live signoff on `run 2064 / build local-7fd87645`. PDF QA is now `passed` with `42 pass / 4 na / 0 fail / 0 pending`. The verified live changes in this cycle were hanging-rail continuation restoration, basin fallback into Imperial `sink` display/checklist fields, and retaining the current five-row appliance capture. The primary live blocker now moves to `job 59`.
- `2026-04-13`: `job 59` reached live signoff on `run 2065 / build local-7fd87645`. PDF QA is now `passed` with `39 pass / 1 na / 0 fail / 0 pending`. The verified live changes in this cycle were the canonical four-family handle summary and maintaining clean room ownership for the current handle rows. The primary live blocker now returns to `job 62`.
- `2026-04-13`: `job 62` reached refreshed live signoff on `run 2067 / build local-7fd87645`. PDF QA is now `passed` with `33 pass / 22 na / 0 fail / 0 pending`. The re-verified live behavior in this cycle was stable short-value row termination across `KITCHEN / PANTRY / DRY BAR / LAUNDRY`, preserved hanging-rail rows, stable sinkware separation for `KITCHEN / LAUNDRY`, and retaining the current five-row appliance capture on the latest build.
- `2026-04-13`: Remaining Imperial jobs outside the tracked regression matrix were deduped by visible address and fresh rerun once to refresh production snapshots without reprocessing duplicate addresses. Completed non-QA rerun coverage: `job 51/run 2085`, `50/2086`, `49/2087`, `48/2088`, `47/2089`, `44/2090`, `43/2091`, `42/2092`, `41/2095`, `40/2096`, `38/2098`, `36/2107`, `35/2108`, `34/2113`, `32/2114`, `31/2115`, `27/2116`. No duplicate addresses were present in the visible Imperial inventory at the time of the sweep; `job 51` remained address-poor (`37993`) but did not collide with another visible Imperial job title.
