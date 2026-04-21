from __future__ import annotations

from unittest import mock

from App.services import extraction_service


def _document() -> dict:
    return {
        "file_name": "haldham.pdf",
        "path": "haldham.pdf",
        "role": "spec",
        "pages": [{"page_no": 1, "text": "KITCHEN JOINERY SELECTION SHEET", "raw_text": "KITCHEN JOINERY SELECTION SHEET"}],
    }


def _v6_snapshot() -> dict:
    return {
        "job_no": "378131",
        "builder_name": "Imperial",
        "source_kind": "spec",
        "rooms": [
            {
                "room_key": "kitchen",
                "original_room_label": "KITCHEN",
                "material_rows": [
                    {
                        "area_or_item": "BENCHTOP",
                        "specs_or_description": "20mm Stone",
                        "provenance": {"source_provider": "v6", "source_extractor": "pdf_to_structured_json_v6"},
                    }
                ],
            }
        ],
        "special_sections": [],
        "appliances": [],
        "others": {},
        "warnings": [],
        "analysis": {"room_master_file": "haldham.pdf"},
    }


def _legacy_snapshot() -> dict:
    return {
        "job_no": "378131",
        "builder_name": "Imperial",
        "source_kind": "spec",
        "rooms": [{"room_key": "kitchen", "original_room_label": "KITCHEN", "material_rows": []}],
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


def test_imperial_v6_fast_path_preserves_v6_rows_through_build_spec_snapshot():
    document = _document()
    progress: list[tuple[str, str]] = []
    with (
        mock.patch.object(extraction_service.parsing, "USE_V6_IMPERIAL_EXTRACTOR", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_v6_snapshot()),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_layout_pipeline") as apply_layout,
        mock.patch("App.services.extraction_service._try_openai") as try_openai,
        mock.patch("App.services.extraction_service._apply_builder_specific_polish") as polish,
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "378131"},
            builder={"name": "Imperial"},
            files=[{"path": "haldham.pdf", "original_name": "haldham.pdf"}],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )
    row = snapshot["rooms"][0]["material_rows"][0]
    assert row["provenance"]["source_provider"] == "v6"
    assert snapshot["analysis"]["parser_strategy"] == "imperial_v6"
    assert snapshot["analysis"]["docling_attempted"] is False
    assert snapshot["analysis"]["vision_attempted"] is False
    assert progress[0][0] == "imperial_v6"
    apply_layout.assert_not_called()
    try_openai.assert_not_called()
    polish.assert_not_called()


def test_imperial_v6_zero_rows_falls_back_to_legacy_pipeline():
    document = _document()
    progress: list[tuple[str, str]] = []
    empty_v6 = {**_v6_snapshot(), "rooms": []}
    with (
        mock.patch.object(extraction_service.parsing, "USE_V6_IMPERIAL_EXTRACTOR", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.parsing.parse_documents", side_effect=[empty_v6, _legacy_snapshot()]),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._build_raw_spec_crosscheck_snapshot", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._crosscheck_imperial_snapshot_with_raw", side_effect=lambda payload, _raw: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "378131"},
            builder={"name": "Imperial"},
            files=[{"path": "haldham.pdf", "original_name": "haldham.pdf"}],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )
    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    assert any(stage == "imperial_v6_fallback" for stage, _ in progress)
    apply_layout.assert_called_once()


def test_imperial_v6_exception_falls_back_to_legacy_pipeline():
    document = _document()
    progress: list[tuple[str, str]] = []
    with (
        mock.patch.object(extraction_service.parsing, "USE_V6_IMPERIAL_EXTRACTOR", True),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.parsing.parse_documents", side_effect=[RuntimeError("v6 boom"), _legacy_snapshot()]),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._build_raw_spec_crosscheck_snapshot", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._crosscheck_imperial_snapshot_with_raw", side_effect=lambda payload, _raw: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "378131"},
            builder={"name": "Imperial"},
            files=[{"path": "haldham.pdf", "original_name": "haldham.pdf"}],
            template_files=[],
            progress_callback=lambda stage, message: progress.append((stage, message)),
        )
    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    assert any(stage == "imperial_v6_fallback" and "v6 boom" in message for stage, message in progress)
    apply_layout.assert_called_once()


def test_flag_off_uses_legacy_layout_pipeline():
    document = _document()
    with (
        mock.patch.object(extraction_service.parsing, "USE_V6_IMPERIAL_EXTRACTOR", False),
        mock.patch("App.services.extraction_service._load_documents", return_value=[document]),
        mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service.parsing.enrich_snapshot_rooms", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_layout_pipeline", return_value=([document], _vision_meta())) as apply_layout,
        mock.patch("App.services.extraction_service._try_openai", return_value=(None, {"mode": "heuristic_only", "parser_strategy": "global_conservative"})),
        mock.patch("App.services.extraction_service._stabilize_snapshot_layout", side_effect=lambda payload, **_kwargs: payload),
        mock.patch("App.services.extraction_service._apply_builder_specific_polish", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._enrich_snapshot_appliances", side_effect=lambda payload, *_args, **_kwargs: payload),
        mock.patch("App.services.extraction_service._build_raw_spec_crosscheck_snapshot", return_value=_legacy_snapshot()),
        mock.patch("App.services.extraction_service._crosscheck_imperial_snapshot_with_raw", side_effect=lambda payload, _raw: payload),
    ):
        snapshot = extraction_service.build_spec_snapshot(
            job={"job_no": "378131"},
            builder={"name": "Imperial"},
            files=[{"path": "haldham.pdf", "original_name": "haldham.pdf"}],
            template_files=[],
        )
    assert snapshot["analysis"]["parser_strategy"] == "global_conservative"
    apply_layout.assert_called_once()
