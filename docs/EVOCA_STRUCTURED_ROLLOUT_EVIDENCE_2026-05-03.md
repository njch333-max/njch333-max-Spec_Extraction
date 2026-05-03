# Evoca Structured Rollout Evidence - 2026-05-03

## Purpose

This note consolidates the controlled production rollout evidence for the Evoca
structured extraction path before deciding whether to change the default rollout
flag.

The current production default remains:

```text
SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED=0
```

Controlled reruns temporarily enabled the flag per job, then restored it to
`0` and restarted services.

## Current Gate

Local private-PDF e2e gate on the current code line passed:

```text
tools/run_evoca_real_pdf_e2e.ps1
14 passed
```

The private PDF gate includes the original validated Evoca corpus plus EVOC434,
which covers the observed excluded-heading numbering variant:

```text
18 PLUMBING & GAS
19 AIR-CONDITIONING
```

Customer PDFs remain outside git under the ignored private fixture folder.

## Exporter Reference

Current production Raw Snapshot Excel export for Evoca structured snapshots is
implemented in `App/services/export_service.py` and runs from stored
`raw_spec` snapshot provenance. It does not re-run the PDF parser.

The older Claude-spike standalone QA workbook exporter is still present as an
untracked local reference file:

```text
tools/evoca_structured_export_claude.py
```

That helper reads JSON produced by
`tools/evoca_structured_extractor_claude.py` or runs that extractor directly
from a PDF, then writes a section-tab workbook for manual parser QA. Treat it
as historical/reference code only unless Jason explicitly asks to preserve or
promote it.

## Production Audit Summary

Live DB audited: `/var/lib/spec-extraction/spec_extraction.sqlite3`

Common audit checks:

- latest run status is `succeeded`
- snapshot parser strategy is `evoca_structured_v0`
- hard-excluded section leak scan is empty
- display leak scan is empty
- `source_documents` contains no loaded page text keys
- raw `special_sections` are not displayed on Spec List

Hard-excluded leak phrases scanned:

```text
16 ELECTRICAL
ALARM SYSTEM
CCTV
SOLAR PV
18 AIR-CONDITIONING
18 PLUMBING & GAS
19 PLUMBING & GAS
19 AIR-CONDITIONING
21 MIRRORS
22 WINDOW FURNISHINGS
Daiken
Paltec
LPG Gas
Natural Gas
Iconic - White
To Width of Vanity
```

| Job | Job No | PDF | Latest Run | Build | Rooms | Appliances | Hard Leaks | Display Leaks | Source Text Keys | Special Sections Display | Verdict |
| --- | --- | --- | ---: | --- | ---: | ---: | --- | --- | --- | ---: | --- |
| 83 | 38208 | EVOC436 | 2350 | local-53134398 | 6 | 8 | none | none | none | 0 | PASS |
| 82 | 38335 | EVOC482 | 2346 | local-23fc33ca | 7 | 8 | none | none | none | 0 | PASS |
| 81 | 38213 | EVOC449 | 2343 | local-ef383d62 | 4 | 8 | none | none | none | 0 | PASS |
| 80 | 38324 | EVOC471 | 2340 | local-c233e696 | 7 | 8 | none | none | none | 0 | PASS |
| 79 | 38338 | EVOC480 | 2338 | local-c233e696 | 4 | 8 | none | none | none | 0 | PASS |
| 78 | 38337 | EVOC479 | 2335 | local-c80a7d7f | 5 | 8 | none | none | none | 0 | PASS |
| 77 | 38148 | EVOC447 | 2351 | local-53134398 | 6 | 7 | none | none | none | 0 | PASS |
| 63 | 38146 | EVOC434 | 2349 | local-53134398 | 5 | 8 | none | none | none | 0 | PASS |
| 53 | 38117 | EVOC473 | 2344 | local-ef383d62 | 9 | 8 | none | none | none | 0 | PASS |
| 39 | 38225 | EVOC467 | 2347 | local-23fc33ca | 4 | 6 | none | none | none | 0 | PASS |

## Source-PDF Spot Checks

### EVOC436 / Job 83 / Run 2350

Source PDF: `38208 - EVOC436 (Lot 1850 Streambed - Colour Selection Document).pdf`

Confirmed against PDF text/tables:

- Page 8/9/10: `WHITE SWIRL`, `CLASSIC WHITE MATT`, `TASMANIAN OAK MATT`,
  `815 Square Pearl Anthracite`, and `Soft Close Hinges`.
- Page 12: Kitchen, Butlers, Laundry, and Powder sink/tap/basin values.
- Page 12 table cell confirms Butlers sink type `Undermount`.
- Page 13: Bathroom/Ensuite basin and basin mixer values.
- Bathroom/Ensuite shower, bath, towel, toilet, and floor-waste rows remain
  suppressed from room-card display by product decision, while source rows stay
  available as provenance.

Representative output:

```text
Kitchen bench: 20mm Quantum Quartz - WHITE SWIRL - Arissed
Kitchen base: Polytec - CLASSIC WHITE MATT
Kitchen sink: Burazzo 450mm Stainless Steel Single Bowl Sink (BU454520S) ($185)
Kitchen tap: Zara Brushed Nickel Pull-Out (ZA120-BN) - Centre of Sink
Butlers sink: Burazzo 650mm Stainless Steel Single Bowl Sink (BU654520S) ($385) - Undermount
Bathroom basin: Eden Bench Mount Gloss White (FL135-W) - Overmount
Bathroom tap: Spin Brushed Nickel Tall Basin Mixer (SP110-BN) - Centre of Basin
Drawers/Hinges: Soft Close
```

### EVOC434 / Job 63 / Run 2349

Source PDF: `38146 - EVOC434 (Lot 1041 Rufous - Colour Selection Document).pdf`

Confirmed against PDF text/tables:

- Page 8: `Caesarstone`, `Pure White`, `Edge Profile 40mm Arissed`,
  `Black Matt- Venette finish`, handles, and soft-close cabinet note.
- Page 12: Kitchen/Laundry sink and tap values.
- Page 13: Bathroom/Ensuite basin and basin mixer values.
- Page 11 contains excluded `18 PLUMBING & GAS`, `19 AIR-CONDITIONING`,
  `Daiken`, and `Paltec`; none leaked to snapshot or display.

Representative output:

```text
Kitchen bench: 40mm Caesarstone - Pure White - Arissed
Kitchen base: Polytec - Black Matt- Venette finish
Kitchen sink: Rocher 540mm Granite Single Bowl Sink (ROCH540-B) ($185) - Undermount
Kitchen tap: Zara Matte Black Pull-Out (ZA120-MB) - Centre of Sink
Bathroom basin: Eden Bench Mount Matte White (FL135-M) ($100 Per Basin) - Overmount
Bathroom tap: Spin Gun Metal Tall Basin Mixer (SP110-GM) - Centre of Basin
Drawers/Hinges: Soft Close
```

### Earlier Controlled Samples

The following samples were previously reviewed against source PDF evidence
during controlled rollout:

- EVOC467 / job 39 / run 2347: Champagne benchtops, Belgian Oak cabinetry,
  Gun Metal sink/tap, Bathroom/Ensuite basin mixer, Soft Close.
- EVOC473 / job 53 / run 2344: Bug 9 cross-page Powder/Benchtops and
  Ensuite 2/5 Basin Mixer rows preserved; hard exclusions clean.
- EVOC449 / job 81 / run 2343: Bug 14 source-backed Underbench / Accessories
  rows retained as provenance; Kitchen Sink inline label/value repair verified.
- EVOC482 / job 82 / run 2346: terminal group-anchor repair and
  `Manufacturer & Model` display repair verified.
- EVOC471 / job 80 / run 2340: handle display formatter verified for
  `Not Applicable - Finger Pulls...` and `Client to supply & install after handover`.
- EVOC479 / job 78 / run 2335: high-risk alarm/AC text leak audit passed;
  wet-area display suppression and Soft Close derivation verified.
- EVOC480 / job 79 / run 2338: structured path succeeded with clean hard
  exclusion and source-document text audits.
- EVOC447 / job 77 / run 2351: rerun under the current deployed build closed
  the previous drawer/hinge caveat; hard exclusions, key PDF-truth fields, and
  `Drawers/Hinges = Soft Close` are clean.

## Caveats

1. Several production samples were generated by earlier local builds. The
   current code line is protected by the private-PDF e2e gate, but live evidence
   is not uniformly same-build evidence.
2. The self-hosted GitHub private-PDF runner setup is documented but not yet
   confirmed as active. Until then, the private-PDF e2e gate is a local/manual
   gate rather than an always-on GitHub CI gate.

## Rollout Recommendation

The structured path is strong enough for broader use, but the safer operator
workflow is now per-run selection in Job Workspace instead of immediately
flipping the global default.

The global rollback switch remains:

```text
SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED=0
```

and `Heuristic Only` is also available as a per-run fallback for Evoca jobs.

Recommended next step:

1. Use `Evoca Structured` as the normal Evoca run button in Job Workspace.
2. Use `Heuristic Only` only for rollback comparison or urgent fallback.
3. Keep `SPEC_EXTRACTION_ENABLE_EVOCA_STRUCTURED=0` until the per-run workflow
   has been used cleanly on live jobs.
4. Do not remove either fallback path until at least one week of clean Evoca
   production runs has accumulated.
