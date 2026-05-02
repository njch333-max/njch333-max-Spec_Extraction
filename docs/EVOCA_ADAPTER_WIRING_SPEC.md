# Evoca Structured Adapter Wiring Spec

Status: Adapter implementation merged; production dispatch wiring is implemented
behind `SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED`, which defaults off until
controlled rollout activation.
Created: 2026-05-01

## Purpose

The standalone Evoca parser now emits source-native JSON with this shape:

```text
section -> room -> group -> label/value rows
```

This spec defines how the adapter maps that JSON into the app's
`SnapshotPayload` model without hiding parser mistakes or widening the parser
scope behind the adapter.

## Required Reading

Before writing or reviewing adapter implementation, read these files directly:

- `docs/EVOCA_STRUCTURED_SCHEMA_v0.md`
- `docs/EVOCA_TYPE_LABEL_SEMANTIC_NOTE.md`
- `docs/EVOCA_STRUCTURED_HANDOFF_2026-04-30.md`

`docs/EVOCA_TYPE_LABEL_SEMANTIC_NOTE.md` is the canonical reference for the
overloaded `Type` label, including per-PDF examples and the allowed mapping
options. Do not rely on a paraphrase of that rule.

## Non-Goals

- Do not widen production wiring beyond the explicit dispatch gate described
  here.
- Do not change `extraction_service.py`, `_finalize_evoca_rooms()`, `runtime.py`,
  or `SnapshotPayload` until implementation is explicitly approved.
- Do not re-parse raw PDF text in the adapter to recover skipped sections or
  patch source-native JSON values.
- Do not normalize, clean up, deduplicate, or interpret source-native business
  values in the adapter layer. If the JSON is wrong, fix
  `App/services/evoca_structured_extractor.py` first.
- Do not use PDF header text, logos, or visual styling to decide builder routing.
  The website job's Builder record remains authoritative.

## Input Contract

The adapter may accept only structured artifacts whose top-level shape includes:

```text
builder = Evoca
schema_version = evoca_structured_v0
sections[]
statistics{}
```

The adapter should reject or fall back when `schema_version` is missing or
different. It should not silently treat legacy Evoca extraction output as the
new structured JSON.

Recognized output sections are:

- `15 CABINETS`
- `17 APPLIANCES, ACCESSORIES & HOT WATER UNIT`
- `20 PLUMBING FIXTURES & TAPWARE`
- `23 TILING / HARD FLOORING`
- `24 GLASS SPLASHBACK`
- `25 CARPET`

Sections `16`, `18`, `19`, `21`, and `22` are parser boundaries only. The
adapter must not reconstruct those sections from raw PDF text, `unstructured_pages`,
page summaries, or diagnostics. For these sections, do not pull from raw PDF.

## Module Boundary

The implementation uses a separate adapter module:

```text
App/services/evoca_structured_adapter.py
```

The adapter exposes pure mapping functions that can be tested with saved
structured JSON artifacts before any runtime wiring:

```text
build_evoca_snapshot_from_structured(structured, job_no, source_document) -> SnapshotPayload
map_evoca_room(section, room) -> RoomRow
map_evoca_appliances(section) -> list[ApplianceRow]
```

Runtime dispatch is a small wrapper around those pure functions in
`App/services/extraction_service.py`. It is attempted only when the website
Builder record is `Evoca` and `SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED` is
explicitly enabled.
PDF header text, logos, or visual styling must not trigger this path.

## Snapshot Metadata

The structured path records that it produced the snapshot:

```text
analysis.parser_strategy = evoca_structured_v0
analysis.layout_provider = evoca_structured_extractor
analysis.layout_attempted = true
analysis.layout_succeeded = true
analysis.docling_attempted = false
analysis.vision_attempted = false
analysis.openai_attempted = false
```

The feature flag is `SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED`, defaulting off.
Flag-off behavior stays on the current production Evoca legacy path with no
behavior change. Controlled rollout requires setting the flag to `1` in the
target environment before rerunning Evoca jobs.

## Room Identity And Order

- Room labels come from `sections[].rooms[].room_label`.
- Room keys come from the parser's `room_key` where available, otherwise from
  the app's existing conservative room-key normalizer.
- When the same room appears in multiple included sections, merge by normalized
  room key only.
- Room order follows the first source occurrence in the structured JSON.
- Section-level schedules such as `17 APPLIANCES` should not create room cards
  unless the source JSON has a real room boundary.

## Row Provenance

Every field mapped from structured JSON should preserve:

- `source_file`
- `page_refs`
- section code and title
- room label
- group label
- child label
- value
- `source_method`
- `raw_cells` when present
- `row_order`, `table_index`, and `row_index` when present

Do not store Evoca provenance in Imperial-only names such as `v6_review_rows`.
If row-level raw display is needed later, add a builder-neutral row surface or
store provider-tagged rows with explicit `source_provider = evoca_structured_v0`.
The production fast path must not copy loaded PDF `pages`, `text`, or `raw_text`
into `source_documents`; keep only file-level metadata so hard-excluded sections
cannot leak through snapshot metadata.

## Section 15 Cabinets Mapping

The adapter should map cabinetry rows by parent group first, then child label.
It must not map generic child labels such as `Colour`, `Type`, `Model`, or
`Handles` without the parent group.

| Source group | Source labels | Snapshot target | Notes |
|---|---|---|---|
| `Benchtops` | `Manufacturer`, `Colour`, `Colour & Finish`, `Finish`, `Edge Profile` | `bench_tops_wall_run` or `bench_tops_other` | Kitchen wall-run values stay separate from island values. Non-kitchen benchtops can use `bench_tops_other`; Study Desk `Colour & Finish` values must appear in the canonical bench-top field as well as row provenance. |
| `Benchtops` | `Island Colour`, `Island Edge Profile`, `Waterfall End to Island` | `bench_tops_island` | Resolve `As Above` only inside the same room/group. |
| `Underbench`, `Underbench including Island` | manufacturer/colour/finish/profile rows | `door_colours_base`, optional `door_colours_island` | Keep source wording verbatim. The adapter may at most trim leading/trailing whitespace for empty checks; it must not strip product codes, normalize spelling, collapse newlines, or transform Unicode characters. |
| `Overhead Cupboards` | manufacturer/colour/finish/profile rows | `door_colours_overheads` | Preserve explicit overhead evidence with `has_explicit_overheads = true`. |
| `Pantry Doors`, `Tall Cupboards` | manufacturer/colour/finish/profile rows | `door_colours_tall` | Do not collapse tall/pantry evidence into base doors. |
| cabinetry groups | `Handles` | `handles` | Merge only same-room handle values. Do not use helper-rendered text as a source. |
| cabinetry groups | `Kickboard`, `Kicker`, toe-kick-like labels | `toe_kick` | Keep room-local only. |
| `Drawers` | `Standard`, `Pot`, `Bin` | `other_items` unless a later field is approved | These are source rows, but current `RoomRow` has no detailed drawer-material field. |

Terminal anchor values such as `Not Applicable - by owner after handover` should
still route verbatim into their canonical group field when that group is mapped.
They must not create material-retained rooms by themselves; room retention must
continue to check for non-terminal material evidence. Display suppression, if
needed later, belongs outside this adapter.

## Spec List Presentation

The user-facing Spec List may format Evoca structured canonical values for
legacy-style readability, for example displaying `Manufacturer`, `Colour &
Finish`, and `Edge Profile` lines as a compact room value. It may also derive
visible flooring and splashback rows from included finish sections. This
formatting is a presentation-only layer and must not mutate stored
`SnapshotPayload` values, `material_rows`, or provenance. Evoca structured raw
`special_sections` evidence cards should stay hidden on Spec List pages unless a
debug view is added later.

## Section 17 Appliances Mapping

Section-level appliance groups should map to `ApplianceRow` objects when they
carry a real appliance type plus make/model evidence.

Adapter rules:

- Keep appliance source wording before official-resource enrichment.
- Ignore hot water, water filter, air-conditioning, alarm/CCTV, and other
  non-appliance rows as appliance rows.
- Placeholder values such as `As Above`, `By Client`, `N/A - By others`, and
  `N/A CLIENT TO CHECK` must not be dropped or deduplicated in the adapter. If
  the same source group emits both a placeholder and a concrete model for one
  appliance type, the concrete model may win the canonical `ApplianceRow` make
  or model fields, but the placeholder must still be retained verbatim as source
  provenance or associated source evidence on that row.
- Official product/spec/manual URL enrichment remains a separate existing stage.

## Section 20 Plumbing Fixtures And Tapware Mapping

Map plumbing rows by parent group before child label:

| Parent group | Label semantics | Snapshot target |
|---|---|---|
| `Sink`, `Tub` | anchor/model/product rows plus fixture `Type` as mounting/style enum | `sink_info` |
| `Basin` | anchor/model/product rows plus fixture `Type` as mounting/style enum | `basin_info` |
| `Sink Mixer`, `Tub Mixer`, `Basin Mixer` | child `Type` is product/model text, not an enum; see `docs/EVOCA_TYPE_LABEL_SEMANTIC_NOTE.md` | `tap_info` |

The `Type` label is overloaded in Evoca source PDFs. Mixer-class parent groups
route `Type` to product/model text. Fixture-class parent groups route `Type`
only to mounting/style when the value matches a known enum-like vocabulary such
as `Overmount` or `Undermount`. The detailed rule lives in
`docs/EVOCA_TYPE_LABEL_SEMANTIC_NOTE.md` and must be followed by path, not by
memory or paraphrase.

Wet-area plumbing rows that are not `Sink`, `Basin`, `Sink Mixer`, or
`Basin Mixer` do not retain a room card by themselves. Fixture rows may enrich a
room that already survives on material/joinery evidence.

## Sections 23, 24, And 25 Finishes Mapping

Finishes should enrich existing rooms when the room key matches:

- `23 TILING / HARD FLOORING` -> `flooring`, and source-backed splashback where
  the group label clearly indicates splashback.
- `24 GLASS SPLASHBACK` -> `splashback`.
- `25 CARPET` -> `flooring`.

Flooring, carpet, and splashback do not retain a room by themselves under the
global room-retention rule. They can enrich a retained room.

## Room Retention

Evoca adapter output must follow the global retention rule:

```text
A room survives only when it has true joinery/material evidence.
```

True retaining evidence includes bench tops, door colours, splashback, toe kick,
bulkheads, floating shelf, or explicitly source-backed shelf material. Handles,
plumbing fixtures, flooring, LED, accessories, diagnostics, and generic notes
do not keep a room alive by themselves.

## Diagnostics And Unsafe Text

Structured rows with `is_diagnostic = true` remain diagnostics. They should not
be converted into business fields unless a later parser fix proves row ownership
from source PDF evidence.

The adapter must never emit a business field named `Continuation`. Source-backed
wraps belong in the parser output; unsafe extras remain diagnostics.

## Acceptance Tests For Adapter Implementation

Offline adapter tests build `SnapshotPayload` objects
from tracked structured JSON fixtures for the nine validated PDFs:

- EVOC447
- EVOC467
- EVOC473
- EVOC471
- EVOC482
- EVOC436
- EVOC449
- EVOC479
- EVOC480

Minimum assertions:

- `schema_version = evoca_structured_v0` is required.
- For each of the nine validated PDFs, every non-diagnostic business row value
  in the structured JSON must appear verbatim in the resulting
  `SnapshotPayload`, either as a complete canonical field value, an `other_items`
  value, or provenance evidence. No string transforms. Test with byte-for-byte
  comparison of value atoms before any display-format composition.
- Sections `16`, `18`, `19`, `21`, and `22` are hard exclusions: acceptance
  round-trip tests must ignore their values, and the adapter must produce no
  snapshot rows, provenance evidence, appliances, or special sections from them.
- No snapshot field, row label, or `other_items` label is `Continuation`.
- EVOC447 mixer-class `Basin Mixer / Type` maps to `tap_info` product text, not
  basin mounting type.
- Fixture-class `Basin / Type = Overmount` stays with `basin_info`.
- EVOC447 Bathroom and Ensuite `Toilet Suite` rows must contain the full PDF
  string `Alora Gloss White Wall Faced Toilet Suite`, not the Bug-11-era
  truncated `Alora Gloss White Wall Faced`. The same rule applies to any product
  name in retained sections that contains its own group label as a substring.
- EVOC473 cross-page raw-text synthesis preserves `Powder / Benchtops` and
  `Ensuite 2` / `Ensuite 5` `Basin Mixer` source-backed rows with page refs.
- EVOC449 `Underbench` and `Accessories & Toilet Suite` synthesized groups map
  from source-backed rows and preserve page refs.
- EVOC482 `Bathroom / Underbench` is present as source-backed cabinetry evidence.
- Existing diagnostic `Unassigned Source Text = WC` rows remain diagnostics and
  do not become room fields.
- Rooms with only fixtures, flooring, handles, LED, or accessories are not
  retained.

Production wiring acceptance is separate: after implementation is approved and
deployed, the affected live Evoca job must be rerun and checked against the
source PDF, not just against an older webpage or snapshot.

## Implementation Sequence

1. Build the pure adapter module and tests from saved structured JSON outputs.
2. Validate adapter snapshots against the nine source PDFs and JSON artifacts.
3. Wire the adapter into runtime dispatch behind the explicit Evoca structured
   path and rollback flag, defaulting off.
4. Deploy with the flag off, then enable `SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED=1`
   only for controlled Evoca rollout verification before rerunning the affected
   live Evoca job.
