# Evoca PDF — `Type` Label Semantic Note (heads-up for adapter wiring)

**Audience**: whoever wires the standalone Evoca structured parser into `_finalize_evoca_rooms` / `SnapshotPayload` / fast path.
**Status**: informational, not a bug. Standalone parser is source-faithful; the risk is downstream semantic mapping.
**Created**: 2026-04-30 (after Bug 7 spot-check and BUG_7_FOLLOWUP_TASK.md withdrawal).

---

## Why this note exists

The label `Type` in EVOC-series PDFs is **not consistently enum-typed across groups**. The PDF designer overloads the same literal string `Type` to mean different things depending on the parent group. When the standalone parser is wired into the production adapter, naive canonical-field mapping (e.g. `if row.label == "Type": canonical.mounting_type = row.value`) will silently corrupt the canonical record because product strings will be routed into mounting-type fields.

Bug 7 already proved this: see `tmp/evoca_structured_bug7/38148_-_EVOC447_Lot_1042_Rufous_-_COLOUR_SELECTION_DOCUMENT.json` and the handoff line 180 confirming PDF truth.

## Confirmed `Type`-label dual-semantic groups in EVOC447

| Parent group | `Type` slot semantics in PDF | Example value (verbatim from EVOC447) |
|---|---|---|
| `Basin` | Enum (mounting type) | `Overmount` |
| `Basin Mixer` | **Product / model string** | `Alder 54082 Brushed Nickel` |
| `Sink Mixer` (Kitchen) | **Product / model string** | `Vito Bertoni Alfie Pull-Out Sink Mixer in Brushed Nickel (85972LF)` |
| `Tub Mixer` (Laundry) | **Product / model string** | `Alder Soho Gooseneck Sink Mixer in Brushed Nickel (54480LF)` |
| `Sink` (some) | Enum (mounting type) — not yet observed populated, but label structure suggests enum | — |

**Pattern**: under mixer-class groups (`Basin Mixer`, `Sink Mixer`, `Tub Mixer`) the parent-group anchor row has **no value** in PDF; the product/model string is carried on the child row labelled `Type`. Under non-mixer fixture groups (`Basin`, `Sink`) the parent anchor row holds the product (e.g. `Eden Bench Mount Gloss White (FL135-W)`) and the child `Type` row holds an enum (e.g. `Overmount`).

This is a **PDF-source authoring convention**, not a parser interpretation. The standalone parser correctly emits both shapes verbatim.

## Constraint for the adapter layer

When mapping standalone-parser rows to canonical SnapshotPayload fields, do **at least one** of the following:

1. **Group-aware Type routing**: maintain a per-parent-group classifier table that maps `(parent_group_label, child_label)` → canonical field. Mixer-class parents route `Type` → `product` / `model`; fixture-class parents route `Type` → `mounting_type` / `style`.
2. **Value-shape gating**: before assigning `Type` to any enum-typed canonical field, reject values that match the product-shape signal (≥3 tokens AND brand-vocabulary token AND/or parenthesized model code). Route those to a free-text product field instead.
3. **Whitelist enum values**: maintain an enum vocabulary (`Overmount`, `Undermount`, `Wall-mounted`, `Bench Mount`, `Vessel`, `Top Mount`, etc) and only accept `Type` values that match. Anything else → free-text product.

Option 1 is most truthful but requires upfront classification. Option 3 is conservative but loses unknown legitimate enum values until vocab grows.

**Do not** flatten `Type` to a single canonical field across groups. Do not write generic `_populate_type` helpers in the adapter without group-awareness. Do not treat `_decompose_meta.values_count == 0` parent groups as data-loss — that shape is correct for mixer groups in EVOC447.

## What is **out of scope** of this note

- The standalone parser at `App/services/evoca_structured_extractor.py` is source-faithful for these rows. **Do not modify it** to "normalize" `Type` semantics — that would destroy PDF fidelity and break the audit trail back to the source PDF.
- This note does not catalogue every group's label semantics. Add new entries here when new EVOC PDFs surface new `Type`-overload patterns. Do **not** generalise from EVOC447 alone to other Evoca PDFs without verification.
- This is **not** a Bug 8 / Bug 9 / Continuation issue. Those are separate, still on hold per memory `project_evoca_structured_parser_bugs.md`.

## Related references

- [docs/EVOCA_STRUCTURED_HANDOFF_2026-04-30.md:180](EVOCA_STRUCTURED_HANDOFF_2026-04-30.md#L180) — the original PDF-truth claim for Basin Mixer Type.
- [docs/EVOCA_STRUCTURED_SCHEMA_v0.md](EVOCA_STRUCTURED_SCHEMA_v0.md) — standalone schema; describes raw row shape, does not yet codify Type-semantic dual-meaning.
- [docs/EVOCA_FAST_PATH_TASK.md](EVOCA_FAST_PATH_TASK.md) — future fast-path wiring task; this note is the canonical Type-semantic constraint that wiring must respect.
- [docs/BUG_7_FOLLOWUP_TASK.md](BUG_7_FOLLOWUP_TASK.md) — WITHDRAWN; preserves the misjudgement that motivated this note, as a record of what NOT to do.
- Memory `feedback_evoca_label_semantic_grep.md` — process lesson: grep handoff/schema before raising alarm on rescue fills.
