from __future__ import annotations

from unittest import mock

from App.services import cleaning_rules, extraction_service


def _document() -> dict:
    return {
        "file_name": "evoca.pdf",
        "path": "evoca.pdf",
        "role": "spec",
        "pages": [
            {
                "page_no": 1,
                "text": "15 CABINETS\n16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM\nAlarm System Daiken",
                "raw_text": "15 CABINETS\n16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM\nAlarm System Daiken",
            }
        ],
    }


def _structured_payload() -> dict:
    return {
        "source_pdf": "evoca.pdf",
        "document_name": "evoca.pdf",
        "builder": "Evoca",
        "schema_version": "evoca_structured_v0",
        "pages": [{"page_no": 1}, {"page_no": 2}],
        "sections": [
            {
                "section_code": "15",
                "section_title": "15 CABINETS",
                "section_order": 1,
                "page_start": 1,
                "page_end": 1,
                "rooms": [
                    {
                        "room_label": "Kitchen",
                        "room_key": "kitchen",
                        "page_start": 1,
                        "page_end": 1,
                        "groups": [
                            {
                                "group_label": "Benchtops",
                                "page_start": 1,
                                "page_end": 1,
                                "rows": [
                                    {
                                        "label": "Manufacturer",
                                        "value": "Quantum Quartz",
                                        "page_no": 1,
                                        "row_order": 1,
                                        "raw_cells": ["-", "Manufacturer", "Quantum Quartz"],
                                        "source_method": "pdfplumber_table",
                                    },
                                    {
                                        "label": "Colour",
                                        "value": "Verona Gold WK Stone",
                                        "page_no": 1,
                                        "row_order": 2,
                                        "raw_cells": ["", "Colour", "Verona Gold WK Stone"],
                                        "source_method": "pdfplumber_table",
                                    },
                                ],
                            }
                        ],
                    }
                ],
                "groups": [],
            },
            {
                "section_code": "16",
                "section_title": "16 ELECTRICAL / ALARM SYSTEM / CCTV / SOLAR PV SYSTEM",
                "section_order": 2,
                "page_start": 2,
                "page_end": 2,
                "rooms": [],
                "groups": [
                    {
                        "group_label": "Alarm System",
                        "page_start": 2,
                        "page_end": 2,
                        "rows": [{"label": "Alarm System", "value": "Daiken", "page_no": 2}],
                    }
                ],
            },
        ],
    }


def _empty_structured_payload() -> dict:
    return {
        "source_pdf": "evoca.pdf",
        "document_name": "evoca.pdf",
        "builder": "Evoca",
        "schema_version": "evoca_structured_v0",
        "pages": [{"page_no": 1}],
        "sections": [],
        "statistics": {},
    }


def _legacy_snapshot(builder_name: str = "Evoca") -> dict:
    return {
        "job_no": "38148",
        "builder_name": builder_name,
        "source_kind": "spec",
        "rooms": [],
        "special_sections": [],
        "appliances": [],
        "others": {},
        "warnings": [],
        "analysis": {"mode": "heuristic_only", "parser_strategy": "global_conservative"},
    }


def _vision_meta() -> dict:
    meta = extraction_service._blank_vision_meta()
    meta["layout_attempted"] = True
    meta["layout_provider"] = "heuristic"
    meta["layout_mode"] = "lightweight"
    return meta


def test_evoca_structured_fast_path_builds_snapshot_before_legacy_pipeline() -> None:
    document = _document()
    progress: list[tuple[str, str]] = []
    with (
        mock.patch.object(extraction_service.runtime, "SPEC_EVOCA_STRUCTURED_ENABLED", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.evoca_structured_extractor.extract_evoca_pdf", return_value=_structured_payload()),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_layout_pipeline") as apply_layout,
        mock.patch("App.services.extraction_service.parsing.parse_documents") as parse_documents,
        mock.patch("App.services.extraction_service._try_openai") as try_openai,
        mock.patch("App.services.extraction_service._apply_builder_specific_polish") as polish,
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "38148", "site_address": "Lot 1042 Rufous"},
            builder={"name": "Evoca"},
            files=[{"path": "evoca.pdf", "original_name": "evoca.pdf"}],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )

    assert snapshot["analysis"]["parser_strategy"] == "evoca_structured_v0"
    assert snapshot["analysis"]["docling_attempted"] is False
    assert snapshot["analysis"]["vision_attempted"] is False
    assert snapshot["analysis"]["openai_attempted"] is False
    assert snapshot["site_address"] == "Lot 1042 Rufous"
    assert snapshot["rooms"][0]["material_rows"][0]["provenance"]["source_provider"] == "evoca_structured_v0"
    assert "pages" not in snapshot["source_documents"][0]
    assert "16 ELECTRICAL" not in str(snapshot)
    assert "Daiken" not in str(snapshot)
    assert progress[0][0] == "evoca_structured"
    apply_layout.assert_not_called()
    parse_documents.assert_not_called()
    try_openai.assert_not_called()
    polish.assert_not_called()


def test_evoca_structured_path_uses_builder_record_not_pdf_header() -> None:
    document = _document()
    with (
        mock.patch.object(extraction_service.runtime, "SPEC_EVOCA_STRUCTURED_ENABLED", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.evoca_structured_extractor.extract_evoca_pdf") as extract_evoca,
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_legacy_snapshot("Imperial")),
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._build_raw_spec_crosscheck_snapshot", return_value=_legacy_snapshot("Imperial")),
        mock.patch("App.services.extraction_service._crosscheck_imperial_snapshot_with_raw", side_effect=lambda payload, _raw: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "38148"},
            builder={"name": "Imperial"},
            files=[{"path": "evoca.pdf", "original_name": "evoca.pdf"}],
            template_files=[],
        )

    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    extract_evoca.assert_not_called()
    apply_layout.assert_called_once()


def test_evoca_structured_flag_off_uses_legacy_pipeline() -> None:
    document = _document()
    with (
        mock.patch.object(extraction_service.runtime, "SPEC_EVOCA_STRUCTURED_ENABLED", False),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.evoca_structured_extractor.extract_evoca_pdf") as extract_evoca,
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "38148"},
            builder={"name": "Evoca"},
            files=[{"path": "evoca.pdf", "original_name": "evoca.pdf"}],
            template_files=[],
        )

    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    extract_evoca.assert_not_called()
    apply_layout.assert_called_once()


def test_evoca_structured_multi_pdf_falls_back_to_legacy_pipeline() -> None:
    documents = [
        _document(),
        {
            "file_name": "evoca-2.pdf",
            "path": "evoca-2.pdf",
            "role": "spec",
            "pages": [{"page_no": 1, "text": "15 CABINETS", "raw_text": "15 CABINETS"}],
        },
    ]
    progress: list[tuple[str, str]] = []
    with (
        mock.patch.object(extraction_service.runtime, "SPEC_EVOCA_STRUCTURED_ENABLED", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=documents),
        mock.patch("App.services.extraction_service.evoca_structured_extractor.extract_evoca_pdf") as extract_evoca,
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=(documents, _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "38148"},
            builder={"name": "Evoca"},
            files=[
                {"path": "evoca.pdf", "original_name": "evoca.pdf"},
                {"path": "evoca-2.pdf", "original_name": "evoca-2.pdf"},
            ],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )

    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    assert any(stage == "evoca_structured_fallback" and "found 2" in message for stage, message in progress)
    extract_evoca.assert_not_called()
    apply_layout.assert_called_once()


def test_evoca_structured_exception_falls_back_to_legacy_pipeline() -> None:
    document = _document()
    progress: list[tuple[str, str]] = []
    with (
        mock.patch.object(extraction_service.runtime, "SPEC_EVOCA_STRUCTURED_ENABLED", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.evoca_structured_extractor.extract_evoca_pdf", side_effect=RuntimeError("structured boom")),
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "38148"},
            builder={"name": "Evoca"},
            files=[{"path": "evoca.pdf", "original_name": "evoca.pdf"}],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )

    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    assert any(stage == "evoca_structured_fallback" and "structured boom" in message for stage, message in progress)
    apply_layout.assert_called_once()


def test_evoca_structured_empty_snapshot_falls_back_to_legacy_pipeline() -> None:
    document = _document()
    progress: list[tuple[str, str]] = []
    with (
        mock.patch.object(extraction_service.runtime, "SPEC_EVOCA_STRUCTURED_ENABLED", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.evoca_structured_extractor.extract_evoca_pdf", return_value=_empty_structured_payload()) as extract_evoca,
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "38148"},
            builder={"name": "Evoca"},
            files=[{"path": "evoca.pdf", "original_name": "evoca.pdf"}],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )

    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    assert any(stage == "evoca_structured_fallback" and "no source-backed evidence" in message for stage, message in progress)
    extract_evoca.assert_called_once_with("evoca.pdf")
    apply_layout.assert_called_once()


def test_evoca_structured_strategy_has_user_visible_label() -> None:
    assert cleaning_rules.parser_strategy_label("evoca_structured_v0") == "Evoca Structured v0"
