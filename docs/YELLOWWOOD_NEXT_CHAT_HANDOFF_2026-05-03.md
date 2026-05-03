# Yellowwood Dedicated Parser Next-Chat Handoff - 2026-05-03

## Purpose

The next chat should start the Yellowwood dedicated parser line of work. The
work should reuse the staged discipline proven by Evoca, but it must not copy
Evoca-specific section rules, field names, or hard exclusions without first
checking Yellowwood source PDFs.

This handoff is a starting point for the next agent. It is not an approval to
wire a Yellowwood fast path into production.

## Current Repository State

- Repository: `C:\Users\Jason Niu - XM\Python Project\Spec_Extraction`
- Current branch at handoff time: `master`
- Latest pushed commit at handoff time: `e68fbbf Add Evoca parser mode run buttons`
- Default production environment remains conservative.
- Evoca structured production rollout is controlled per run through Job
  Workspace buttons rather than a global default-on flip.

## Evoca Progress To Carry Forward

Evoca is now the proven architectural pattern:

1. Standalone source-native extractor first.
2. Explicit structured JSON schema.
3. Real-PDF regression fixtures and e2e runner before broader rollout.
4. Adapter spec before adapter code.
5. Pure adapter with offline tests before production wiring.
6. Production dispatch controlled by the website Builder record.
7. Per-run parser controls in Job Workspace for controlled rollout and
   rollback comparison.

The latest Evoca production evidence:

- Job 77 run `2352` was started through the live `Evoca Structured` button.
- Production global `SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED` was still false.
- The run still produced `parser_strategy = evoca_structured_v0`, proving the
  per-run override works.
- Hard-exclusion scan was clean.
- `source_documents` did not retain loaded page text.
- Historical Spec List opened successfully and did not render raw Special
  Sections.

## Yellowwood Baseline Constraints Already In The Project

Before proposing a Yellowwood parser, read these project rules:

- `AGENTS.md` rules 17-25.
- `PRD.md` Yellowwood requirements in the extraction and engineering workflow
  sections.
- `Arch.md` section `Yellowwood-specific behavior`.
- `Project_state.md` current Yellowwood notes and regression samples.

Key constraints already agreed:

- Yellowwood cabinetry, vanity, flooring, and tiling schedules are
  table/grid-first.
- Existing live regression references in the docs include Yellowwood jobs
  `12`, `24`, and `37`; verify the current live jobs and source PDFs before
  treating any old snapshot as source truth.
- Room names must preserve concrete source titles such as `PANTRY`,
  `BED 1 MASTER ENSUITE VANITY`, `GROUND FLOOR POWDER ROOM`,
  `UPPER-LEVEL POWDER ROOM`, `BED 1 MASTER WALK IN ROBE FIT OUT`, and
  `BED 2/3/4/5 ROBE FIT OUT`.
- Rooms survive only with true joinery/material evidence.
- `robe` and `media` rooms survive only with real material evidence such as
  `Polytec` or `Laminex`.
- Fake room fragments such as `WIP`, row-note cells, shelving-only cell text,
  and collapsed generic labels such as a single `ROBE FIT OUT` room must not
  survive as final room cards.
- Flooring and tiling schedules are overlays for retained rooms; contents-page
  flooring text must not become `others.flooring_notes`.
- Vanity plumbing must stay room-relevant. Only `Basin`, `Basin Mixer`,
  room-local flooring, and joinery/material fields may survive on Yellowwood
  vanity cards.
- General wet-area noise such as shower, bath, toilet, towel rail, towel hook,
  floor waste, feature waste, shower base/frame, basin waste, bottle trap, and
  in-wall-mixer-only rows must be removed from final room output.
- `Shelf` is conditional and belongs only to simple fit-out/storage room
  families when same-room source evidence assigns a shelf material or finish.

## Do Not Start With Production Wiring

The first Yellowwood structured-parser chat should not edit production dispatch
unless Jason explicitly expands scope.

Do not start by editing:

- `App/services/extraction_service.py`
- `App/services/runtime.py`
- `App/services/worker.py`
- `App/templates/job_detail.html`
- `SnapshotPayload` model shape

The safer first target is a standalone parser and proof harness.

## Recommended Next-Chat Plan

1. Read this handoff and the four root project docs.
2. Inventory current Yellowwood samples from the live site and local files:
   - job IDs cited in docs: `12`, `24`, `37`
   - any newer Yellowwood job Jason names in the next chat
   - source PDFs, not just rendered Spec List pages
3. Create `docs/YELLOWWOOD_STRUCTURED_PARSER_SPEC.md` before coding.
4. Define a source-native schema such as `yellowwood_structured_v0`.
5. Implement a standalone extractor first, for example:
   - `App/services/yellowwood_structured_extractor.py`
   - optional tool runner under `tools/`
   - private PDF fixtures under an ignored fixture path
6. Add real-PDF e2e tests similar to Evoca before production rollout:
   - real PDF to structured JSON
   - structured JSON to expected source-native rows
   - no cross-room row bleed
   - no retention of plumbing-only or flooring-only fake rooms
   - title preservation checks
7. Only after standalone parser output is source-PDF reviewed, write an adapter
   spec and pure adapter.
8. Only after adapter tests pass, discuss production wiring and whether
   Yellowwood should get per-run `Yellowwood Structured` / `Heuristic Only`
   controls like Evoca.

## Yellowwood Parser Design Warnings

Do not blindly clone Evoca.

Evoca was built around numbered sections, room groups, group anchors, and
section hard exclusions. Yellowwood appears to be more about schedule families,
concrete joinery titles, vanity/robe retention, and overlay discipline. The
common idea is staged source-native parsing; the business rules are different.

The Yellowwood parser should be strict about:

- row-local ownership
- same-room overlays only
- concrete source titles
- table/grid columns before string cleanup
- preserving material wording
- blacklisting final-output wet-area noise without deleting source provenance

## Suggested Opening Prompt For Next Chat

```text
We are in C:\Users\Jason Niu - XM\Python Project\Spec_Extraction.

Use Chinese in chat with Jason, but keep code, UI, exports, and project docs in
English.

Read first:
- docs/YELLOWWOOD_NEXT_CHAT_HANDOFF_2026-05-03.md
- AGENTS.md Yellowwood rules 17-25
- Arch.md Yellowwood-specific behavior
- PRD.md Yellowwood requirements
- Project_state.md current Yellowwood status

Goal:
Start Yellowwood dedicated structured-parser work, using the Evoca staged
approach as the process model: standalone extractor -> schema -> private real
PDF e2e -> adapter spec -> pure adapter -> controlled production wiring later.

Important boundaries:
- Do not start with production wiring.
- Do not edit extraction_service.py, runtime.py, worker.py, Job Workspace
  buttons, or SnapshotPayload unless Jason explicitly approves.
- Source PDFs are the acceptance truth.
- Website Builder record owns routing; PDF header text does not.
- Preserve source-native row values and provenance. Do not hide parser mistakes
  in downstream cleanup.

First task:
Inventory Yellowwood source PDFs/jobs and draft docs/YELLOWWOOD_STRUCTURED_PARSER_SPEC.md.
```

## Working Tree Reminder

There are unrelated untracked docs and helper files in the workspace. Do not
use `git add .`; stage only the exact files needed for the Yellowwood task.
