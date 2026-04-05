# Job 19 Review Demo

This branch is the first GitHub Codex review demo branch for `Spec_Extraction`.

## Review Target

- Builder: `Simonds`
- Job: `19`
- Theme: grouped-row / property-row cleanup

## Problem Summary

`job 19` previously showed grouped-row pollution in several rooms, including:

- `Study`
- `Butlers/WIP`
- `Laundry`
- `Bathroom`
- `Powder`
- `Rumpus`

The main failure mode was property labels like `Manufacturer / Finish / Profile / Colour / Model` leaking into final room fields.

## Review Focus

When reviewing this branch, prioritize:

- `same-room-only`
- `same-section-only`
- `same-row-or-row-fragment-only`
- grouped property-label leakage into final material fields
- `shelf` staying source-driven
- sink/tap backfill not crossing room boundaries
- regression risk to other grouped-row builders such as `Evoca` and `Yellowwood`

## Expected Outcome

The grouped-row cleanup should keep room fields readable, room-local, and PDF-grounded without regressing other builders.
