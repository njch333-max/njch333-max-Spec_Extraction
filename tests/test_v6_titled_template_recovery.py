"""
Unit tests for the v6 fast-path fixes that recover Imperial spec rows
which the grid-only extractor missed on job 38 / run 2369:

  Fix #1 - extract_page_title() must not treat AREA-column row labels
           like "TAPWARE (KITCHEN) BY CLIENT" or "SINKWARE (KITCHEN) ..."
           as section titles. Only canonical Imperial section titles
           ("KITCHEN/DINING/... SELECTION SHEET", "APPLIANCES",
           "SINKWARE & TAPWARE") should match.

  Fix #2 - On a titled APPLIANCES page, after the grid extraction runs,
           an anchor-based template recovery pass is allowed to append
           rows the grid missed (e.g. DISHWASHER when the row separator
           is not full-page-width). Acceptance is gated by an appliance
           AREA whitelist + dedup against rows already produced by the
           grid pass.

These tests use a tiny FakePage object instead of a real PDF so the
suite stays portable and does not require committing customer PDFs.
"""
from __future__ import annotations

from App.services.pdf_to_structured_json import (
    _APPLIANCE_AREA_WHITELIST,
    _appliance_recovery_area_accepted,
    extract_page_title,
)


class _FakePage:
    """Minimal stand-in for a pdfplumber.Page object.

    Only the bits extract_page_title() touches are implemented.
    """

    def __init__(self, words):
        self._words = words

    def extract_words(self, keep_blank_chars=False):  # noqa: ARG002
        return list(self._words)


def _word(text, x0, top, *, x1=None, bottom=None):
    return {
        "text": text,
        "x0": float(x0),
        "x1": float(x1 if x1 is not None else x0 + 6 * len(text)),
        "top": float(top),
        "bottom": float(bottom if bottom is not None else top + 12),
    }


# ----- Fix #1 ----------------------------------------------------------

def test_extract_page_title_skips_tapware_row_label():
    """job38 p8: TAPWARE (KITCHEN) in AREA column must NOT be the title."""
    page = _FakePage([
        # AREA column row label (this is what currently triggered the bug)
        _word("TAPWARE", x0=102.7, top=105.3),
        _word("(KITCHEN)", x0=156.0, top=105.3),
        # SPECS column words on a nearby y row
        _word("Tap", x0=317.6, top=98.5),
        _word("Franke", x0=336.9, top=98.5),
        # SUPPLIER column
        _word("BY", x0=802.6, top=105.8),
        _word("CLIENT", x0=817.1, top=105.8),
    ])
    assert extract_page_title(page) is None


def test_extract_page_title_skips_sinkware_row_label():
    """SINKWARE (KITCHEN) ... must also be treated as a row label."""
    page = _FakePage([
        _word("SINKWARE", x0=100.9, top=414.0),
        _word("(KITCHEN)", x0=160.0, top=414.0),
        _word("horizontal", x0=300.0, top=414.0),
        _word("double", x0=370.0, top=414.0),
        _word("bowl", x0=410.0, top=414.0),
        _word("BY", x0=802.0, top=414.0),
        _word("CLIENT", x0=817.0, top=414.0),
    ])
    assert extract_page_title(page) is None


def test_extract_page_title_detects_sinkware_and_tapware_section_title():
    """job38 p7: full 'SINKWARE & TAPWARE' phrase remains a valid title."""
    page = _FakePage([
        _word("SINKWARE", x0=415.7, top=168.0),
        _word("&", x0=485.0, top=168.0),
        _word("TAPWARE", x0=503.0, top=168.0),
    ])
    title = extract_page_title(page)
    assert title == "SINKWARE & TAPWARE"


def test_extract_page_title_detects_appliances():
    page = _FakePage([
        _word("APPLIANCES", x0=494.9, top=166.5),
    ])
    assert extract_page_title(page) == "APPLIANCES"


def test_extract_page_title_detects_selection_sheet_title():
    page = _FakePage([
        _word("KITCHEN", x0=300.0, top=160.0),
        _word("JOINERY", x0=360.0, top=160.0),
        _word("SELECTION", x0=420.0, top=160.0),
        _word("SHEET", x0=500.0, top=160.0),
    ])
    title = extract_page_title(page)
    assert title is not None
    assert "SELECTION SHEET" in title.upper()


def test_extract_page_title_row_label_does_not_block_real_title_later():
    """If a row label appears before the real title in y order, we still
    return the real title - the row label is skipped, not aborting the
    scan."""
    page = _FakePage([
        # Real section title at top
        _word("APPLIANCES", x0=494.9, top=166.5),
        # Row label further down - must be ignored when checking lines but
        # must not cause us to return early before the title is seen.
        # (The current implementation iterates sorted(y) so APPLIANCES is
        # actually first; this test is a guardrail for future refactors.)
        _word("TAPWARE", x0=100.0, top=414.0),
        _word("(KITCHEN)", x0=160.0, top=414.0),
    ])
    assert extract_page_title(page) == "APPLIANCES"


# ----- Fix #2: appliance recovery whitelist + dedup --------------------

def test_appliance_whitelist_contains_known_appliances():
    for name in ("OVEN", "COOKTOP", "DISHWASHER", "RANGEHOOD",
                 "MICROWAVE", "FRIDGE", "BAR FRIDGE"):
        assert name in _APPLIANCE_AREA_WHITELIST


def test_recovery_accepts_dishwasher_when_not_already_present():
    existing = {"OVEN", "COOKTOP"}
    assert _appliance_recovery_area_accepted("DISHWASHER", existing) is True


def test_recovery_accepts_lowercase_and_whitespace():
    existing = {"OVEN", "COOKTOP"}
    assert _appliance_recovery_area_accepted("  dishwasher  ", existing) is True


def test_recovery_accepts_multiline_whitespace_variant():
    existing = {"OVEN", "COOKTOP"}
    assert _appliance_recovery_area_accepted("BAR\nFRIDGE", existing) is True


def test_recovery_dedup_normalizes_existing_area_whitespace():
    existing = {"BAR\nFRIDGE"}
    assert _appliance_recovery_area_accepted("BAR FRIDGE", existing) is False


def test_recovery_rejects_dup_already_in_existing():
    existing = {"OVEN", "COOKTOP", "DISHWASHER"}
    assert _appliance_recovery_area_accepted("DISHWASHER", existing) is False


def test_recovery_rejects_non_whitelisted_area():
    """Non-appliance labels (e.g. cabinetry / joinery rows) must not be
    appended by the recovery pass even if they survived anchor clustering."""
    existing = {"OVEN"}
    for label in ("BASE CABINETRY COLOUR", "KICKBOARDS", "HANDLES - BASE CABS",
                  "FEATURE TIMBER LOOK CABINETRY", "TAPWARE (KITCHEN)"):
        assert _appliance_recovery_area_accepted(label, existing) is False


def test_recovery_rejects_blank_and_none():
    existing = {"OVEN"}
    assert _appliance_recovery_area_accepted("", existing) is False
    assert _appliance_recovery_area_accepted(None, existing) is False
    assert _appliance_recovery_area_accepted("   ", existing) is False
