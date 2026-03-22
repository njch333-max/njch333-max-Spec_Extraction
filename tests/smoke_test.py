from __future__ import annotations

import importlib
import io
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

TEST_DATA_DIR = Path(tempfile.mkdtemp(prefix="spec-extraction-test-data-"))
os.environ["SPEC_EXTRACTION_DATA_DIR"] = str(TEST_DATA_DIR)

from fastapi.testclient import TestClient
from openpyxl import load_workbook

from App.main import _flatten_rooms, app
from App.services import extraction_service, store
from App.services.appliance_official import _build_direct_product_candidates, _extract_size_from_text, _primary_model_token
from App.services.parsing import enrich_snapshot_rooms, parse_documents


class SmokeTest(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)

    def setUp(self) -> None:
        self._reset_db()

    def tearDown(self) -> None:
        self._reset_db()

    def _reset_db(self) -> None:
        with store.connect() as conn:
            conn.execute("DELETE FROM auth_events")
            conn.execute("DELETE FROM reviews")
            conn.execute("DELETE FROM snapshots")
            conn.execute("DELETE FROM runs")
            conn.execute("DELETE FROM job_files")
            conn.execute("DELETE FROM jobs")
            conn.execute("DELETE FROM builder_templates")
            conn.execute("DELETE FROM builders")

    def test_health(self) -> None:
        client = TestClient(app)
        response = client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    def test_runtime_loads_env_file_before_constants(self) -> None:
        import App.services.runtime as runtime

        env_path = runtime.ENV_PATH
        original_text = env_path.read_text(encoding="utf-8") if env_path.exists() else None
        env_keys = ["SPEC_EXTRACTION_ENABLE_OPENAI", "SPEC_EXTRACTION_OPENAI_MODEL", "OPENAI_API_KEY"]
        original_env = {key: os.environ.get(key) for key in env_keys}
        for key in env_keys:
            os.environ.pop(key, None)
        try:
            env_path.write_text(
                "SPEC_EXTRACTION_ENABLE_OPENAI=1\n"
                "SPEC_EXTRACTION_OPENAI_MODEL=gpt-4.1-mini\n"
                "OPENAI_API_KEY=test-openai-key\n",
                encoding="utf-8",
            )
            runtime = importlib.reload(runtime)
            self.assertTrue(runtime.OPENAI_ENABLED)
            self.assertEqual(runtime.OPENAI_MODEL, "gpt-4.1-mini")
            self.assertEqual(runtime.OPENAI_API_KEY, "test-openai-key")
        finally:
            if original_text is None:
                env_path.unlink(missing_ok=True)
            else:
                env_path.write_text(original_text, encoding="utf-8")
            for key, value in original_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            importlib.reload(runtime)

    def test_parser_extracts_room_and_basic_appliance(self) -> None:
        snapshot = parse_documents(
            job_no="37529",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "sample.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Kitchen\n"
                                "Bench Tops 20mm stone by builder\n"
                                "Door Colour Polytec Classic White Matt\n"
                                "Kickboard Matching White\n"
                                "Handles Hettich 9070585 Chrome\n"
                                "Drawers Soft Close\n"
                                "Hinges Not Soft Close\n"
                                "Splashback Tiled by others\n"
                                "Flooring Hybrid flooring\n"
                                "Cooktop: Westinghouse WHC943BD 90cm\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        self.assertEqual(snapshot["rooms"][0]["room_key"], "kitchen")
        self.assertEqual(snapshot["rooms"][0]["drawers_soft_close"], "Soft Close")
        self.assertEqual(snapshot["rooms"][0]["hinges_soft_close"], "Not Soft Close")
        self.assertEqual(snapshot["appliances"][0]["model_no"], "WHC943BD")
        self.assertEqual(snapshot["analysis"]["mode"], "heuristic_only")
        self.assertEqual(snapshot["analysis"]["parser_strategy"], "global_conservative")

    def test_parser_extracts_explicit_appliance_model_numbers(self) -> None:
        snapshot = parse_documents(
            job_no="37529",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "appliance-schedule.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 7,
                            "text": (
                                "Under Bench Oven: WESTINGHOUSE 2 X WVE6515SDA 60CM ELECTRIC OVEN S/S ELECTRIC\n"
                                "Dishwasher Make: WESTINGHOUSE Freestanding (WSF6608X) 600mm S/S\n"
                                "Fridge: N/A CLIENT TO CHECK\n"
                                "Integrated Fridge/Freezer: FISHER & PAYKEL 2 X RB60V18\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        appliances = {row["appliance_type"]: row for row in snapshot["appliances"]}
        self.assertEqual(appliances["Oven"]["make"], "Westinghouse")
        self.assertEqual(appliances["Oven"]["model_no"], "2 x WVE6515SDA")
        self.assertEqual(appliances["Dishwasher"]["model_no"], "WSF6608X")
        self.assertEqual(appliances["Fridge"]["make"], "Fisher & Paykel")
        self.assertEqual(appliances["Fridge"]["model_no"], "2 x RB60V18")
        models = " ".join(row["model_no"] for row in snapshot["appliances"])
        self.assertNotIn("WESTINGHOUSE", models)
        self.assertNotIn("CLIENT", models)
        self.assertNotIn("OPENING", models)
        self.assertNotIn("ELECTRIC", models)

    def test_parser_normalizes_soft_close_states(self) -> None:
        snapshot = parse_documents(
            job_no="38111",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "soft-close.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Kitchen\n"
                                "Drawers Hettich Multitech standard construction runners - NOT soft close\n"
                                "Hinges Blumotion soft-close hinges\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        room = snapshot["rooms"][0]
        self.assertEqual(room["drawers_soft_close"], "Not Soft Close")
        self.assertEqual(room["hinges_soft_close"], "Soft Close")

    def test_parser_normalizes_brand_casing_and_preserves_benchtop_text(self) -> None:
        snapshot = parse_documents(
            job_no="37016",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "clarendon.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Kitchen\n"
                                "Bench Tops Quantum Zero Midnight Black - 20mm pencil round edge to cooktop run; "
                                "Quantum Zero Venatino Statuario - 40mm mitred apron edge to island bench\n"
                                "Door Colour PolYTEC Classic White Matt Finish Thermolaminate\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        room = snapshot["rooms"][0]
        self.assertIn("Quantum Zero Midnight Black - 20mm pencil round edge", room["bench_tops_wall_run"])
        self.assertIn("Quantum Zero Venatino Statuario - 40mm mitred apron edge", room["bench_tops_island"])
        self.assertEqual(room["door_panel_colours"][0], "Polytec Classic White Matt Finish Thermolaminate")

    def test_room_fixture_enrichment_splits_door_colours_and_filters_plumbing_appliances(self) -> None:
        documents = [
            {
                "file_name": "fixtures.txt",
                "role": "spec",
                "pages": [
                    {
                        "page_no": 1,
                        "text": (
                            "Kitchen\n"
                            "Door Colour 1 - Polytec Classic White Matt - to cooktop run base, upper + tall cabinetry\n"
                            "Door Colour 2 - Polytec Tempest Woodgrain - island base cabinetry + bar back panels\n"
                            "Sink Type: PARISI Quadro Double Bowl (PK8644)\n"
                            "Tap Type: PHOENIX Nostalgia Sink Mixer NS714-62\n"
                            "Cooktop: Westinghouse WHC943BD 90cm\n"
                            "Butler's Pantry\n"
                            "Sink Type/Model: PARISI Quadro Single Bowl (PK4444)\n"
                            "Tap Type: PHOENIX Nostalgia Sink Mixer NS738-62\n"
                            "Vanities\n"
                            "Vanity Inset Basin JOHNSON SUISSE Emilia Rectangular Undercounter Basin (JBSE250.PW6)\n"
                            "Vanity Tap Style: PHOENIX Nostalgia Basin Mixer NS748-62\n"
                        ),
                        "needs_ocr": False,
                    }
                ],
            }
        ]
        snapshot = parse_documents(job_no="37529", builder_name="Clarendon", source_kind="spec", documents=documents)
        enriched = enrich_snapshot_rooms(snapshot, documents)
        rooms = {row["room_key"]: row for row in enriched["rooms"]}
        self.assertEqual(rooms["kitchen"]["sink_info"], "Parisi Quadro Double Bowl (PK8644)")
        self.assertEqual(rooms["kitchen"]["tap_info"], "Phoenix Nostalgia Sink Mixer NS714-62")
        self.assertEqual(rooms["kitchen"]["door_colours_overheads"], "Polytec Classic White Matt")
        self.assertEqual(rooms["kitchen"]["door_colours_base"], "Polytec Classic White Matt")
        self.assertEqual(rooms["kitchen"]["door_colours_island"], "Polytec Tempest Woodgrain")
        self.assertEqual(rooms["kitchen"]["door_colours_bar_back"], "Polytec Tempest Woodgrain")
        self.assertEqual(rooms["butlers_pantry"]["sink_info"], "Parisi Quadro Single Bowl (PK4444)")
        self.assertEqual(
            rooms["vanity"]["basin_info"],
            "Johnson Suisse Emilia Rectangular Undercounter Basin (JBSE250.PW6)",
        )
        self.assertEqual(rooms["vanity"]["tap_info"], "Phoenix Nostalgia Basin Mixer NS748-62")
        appliance_types = [row["appliance_type"].lower() for row in enriched["appliances"]]
        self.assertIn("cooktop", appliance_types)
        self.assertNotIn("sink", appliance_types)

    def test_build_spec_snapshot_marks_openai_fallback_when_request_fails(self) -> None:
        with (
            mock.patch.object(extraction_service.runtime, "OPENAI_ENABLED", True),
            mock.patch.object(extraction_service.runtime, "OPENAI_API_KEY", "test-key"),
            mock.patch.object(extraction_service.runtime, "OPENAI_MODEL", "gpt-4.1-mini"),
            mock.patch("App.services.extraction_service._post_responses_api", side_effect=RuntimeError("boom")),
        ):
            snapshot = extraction_service.build_spec_snapshot(
                job={"job_no": "37529"},
                builder={"name": "Clarendon"},
                files=[],
                template_files=[],
        )
        self.assertEqual(snapshot["analysis"]["mode"], "openai_fallback")
        self.assertEqual(snapshot["analysis"]["parser_strategy"], "global_conservative")
        self.assertTrue(snapshot["analysis"]["openai_attempted"])
        self.assertFalse(snapshot["analysis"]["openai_succeeded"])
        self.assertIn("normalize_brand_casing", snapshot["analysis"]["rule_flags"])

    def test_build_spec_snapshot_accepts_fenced_openai_json(self) -> None:
        openai_payload = {
            "output_text": """```json
{"rooms":[{"room_key":"kitchen","original_room_label":"Kitchen"}],"appliances":[],"others":{},"warnings":[]}
```"""
        }
        with (
            mock.patch.object(extraction_service.runtime, "OPENAI_ENABLED", True),
            mock.patch.object(extraction_service.runtime, "OPENAI_API_KEY", "test-key"),
            mock.patch.object(extraction_service.runtime, "OPENAI_MODEL", "gpt-4.1-mini"),
            mock.patch("App.services.extraction_service._post_responses_api", return_value=openai_payload),
        ):
            snapshot = extraction_service.build_spec_snapshot(
                job={"job_no": "37529"},
                builder={"name": "Clarendon"},
                files=[],
                template_files=[],
            )
        self.assertEqual(snapshot["analysis"]["mode"], "openai_merged")
        self.assertEqual(snapshot["analysis"]["parser_strategy"], "global_conservative")
        self.assertTrue(snapshot["analysis"]["openai_succeeded"])
        self.assertEqual(snapshot["rooms"][0]["room_key"], "kitchen")

    def test_build_spec_snapshot_handles_invalid_openai_field_shapes(self) -> None:
        openai_payload = {
            "output_text": '{"rooms": [], "appliances": [], "others": "bad-shape", "warnings": "bad-shape"}'
        }
        with (
            mock.patch.object(extraction_service.runtime, "OPENAI_ENABLED", True),
            mock.patch.object(extraction_service.runtime, "OPENAI_API_KEY", "test-key"),
            mock.patch.object(extraction_service.runtime, "OPENAI_MODEL", "gpt-4.1-mini"),
            mock.patch("App.services.extraction_service._post_responses_api", return_value=openai_payload),
        ):
            snapshot = extraction_service.build_spec_snapshot(
                job={"job_no": "37529"},
                builder={"name": "Clarendon", "parser_strategy": "stable_hybrid"},
                files=[],
                template_files=[],
            )
        self.assertEqual(snapshot["analysis"]["mode"], "openai_merged")
        self.assertEqual(snapshot["others"]["flooring_notes"], "")
        self.assertEqual(snapshot["others"]["splashback_notes"], "")
        self.assertEqual(snapshot["warnings"], [])

    def test_build_spec_snapshot_enriches_official_appliance_resources(self) -> None:
        documents = [
            {
                "stored_name": "sample.pdf",
                "original_name": "sample.pdf",
                "path": "sample.pdf",
            }
        ]
        mock_snapshot = {
            "job_no": "37529",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "rooms": [],
            "appliances": [
                {
                    "appliance_type": "Cooktop",
                    "make": "Westinghouse",
                    "model_no": "WHC943BD",
                    "website_url": "",
                    "overall_size": "",
                    "source_file": "sample.pdf",
                    "page_refs": "1",
                    "evidence_snippet": "Cooktop row",
                    "confidence": 0.82,
                }
            ],
            "others": {},
            "warnings": [],
            "source_documents": [],
            "analysis": {"mode": "heuristic_only"},
        }
        with (
            mock.patch("App.services.extraction_service._load_documents", return_value=[]),
            mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=mock_snapshot),
            mock.patch(
                "App.services.extraction_service.parsing.enrich_snapshot_rooms",
                side_effect=lambda payload, _documents, rule_flags=None: payload,
            ),
            mock.patch("App.services.appliance_official.lookup_official_appliance_resources", return_value={
                "product_url": "https://official.example/product/WHC943BD",
                "spec_url": "https://official.example/spec/WHC943BD.pdf",
                "manual_url": "https://official.example/manual/WHC943BD.pdf",
                "website_url": "https://official.example/product/WHC943BD",
                "overall_size": "900 x 510 x 60 mm",
            }),
        ):
            snapshot = extraction_service.build_spec_snapshot(
                job={"job_no": "37529"},
                builder={"name": "Clarendon"},
                files=documents,
                template_files=[],
            )
        appliance = snapshot["appliances"][0]
        self.assertEqual(appliance["product_url"], "https://official.example/product/WHC943BD")
        self.assertEqual(appliance["spec_url"], "https://official.example/spec/WHC943BD.pdf")
        self.assertEqual(appliance["manual_url"], "https://official.example/manual/WHC943BD.pdf")
        self.assertEqual(appliance["website_url"], "https://official.example/product/WHC943BD")
        self.assertEqual(appliance["overall_size"], "900 x 510 x 60 mm")

    def test_extract_size_from_product_page_hwd_lines(self) -> None:
        self.assertEqual(
            _extract_size_from_text("Dimension 51 mm (H) 900 mm (W) 520 mm (D)"),
            "51 mm (H) x 900 mm (W) x 520 mm (D)",
        )

    def test_primary_model_token_and_direct_westinghouse_candidates(self) -> None:
        self.assertEqual(_primary_model_token("2 x WVE6515SDA"), "WVE6515SDA")
        self.assertEqual(
            _build_direct_product_candidates("Westinghouse", "Cooktop", "WHC943BD"),
            ["https://www.westinghouse.com.au/cooking/cooktops/whc943bd/"],
        )
        self.assertIn(
            "https://www.aegaustralia.com.au/cooking/cooktops/induction-cooktops/nik95i00fz/",
            _build_direct_product_candidates("AEG", "Cooktop", "NIK95I00FZ"),
        )

    def test_yellowwood_overlay_maps_back_benchtops_and_filters_non_cabinet_colours(self) -> None:
        documents = [
            {
                "file_name": "yellowwood.pdf",
                "role": "spec",
                "pages": [
                    {
                        "page_no": 20,
                        "text": (
                            "JOINERY - REFER TO CABINETRY PLANS FOR ALL FURTHER DETAIL\n"
                            "KITCHEN Back Benchtops 40mm\n"
                            "YDL Giusto Polished\n"
                            "Island\n"
                            "Benchtop 40mm +\n"
                            "Waterfall Ends\n"
                            "YDL Giusto Polished\n"
                            "Overhead Cupboards\n"
                            "Polytec Blossom White Matt\n"
                            "Base Cupboards &\n"
                            "Drawers\n"
                            "Polytec Blossom White Matt\n"
                            "Island Bar Back Polytec Topiary Matt\n"
                            "Island Bench\n"
                            "Base Cupboards &\n"
                            "Drawers\n"
                            "Polytec Topiary Matt\n"
                            "LAUNDRY\n"
                            "Benchtop 20mm\n"
                            "YDL Classic White Polished\n"
                            "Overhead Cupboards\n"
                            "Polytec Blossom White Matt\n"
                            "Base Cupboards & Drawers\n"
                            "Polytec Blossom White Matt\n"
                            "BATHROOM VANITY\n"
                            "Benchtop 20mm\n"
                            "YDL Classic White Polished\n"
                            "Floor Mounted Vanity\n"
                            "Polytec Nouveau Grey Matt\n"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 3,
                        "text": (
                            "EXTERNAL FINISHES\n"
                            "GUTTER Colorbond Dover White\n"
                            "ENTRY DOOR Painted Titanium White\n"
                        ),
                        "needs_ocr": False,
                    },
                ],
            }
        ]
        snapshot = {
            "job_no": "37014",
            "builder_name": "Yellowwood",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "rooms": [
                {
                    "room_key": "kitchen",
                    "original_room_label": "KITCHEN",
                    "bench_tops": ["{'back': 'YDL Giusto Polished 40mm', 'island': 'YDL Giusto Polished 40mm + Waterfall Ends'}", "40mm +"],
                    "door_panel_colours": [],
                    "door_colours_overheads": "",
                    "door_colours_base": "",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                },
                {
                    "room_key": "laundry",
                    "original_room_label": "LAUNDRY",
                    "bench_tops": ["20mm YDL Classic White Polished"],
                    "door_panel_colours": ["Dover White"],
                    "door_colours_overheads": "",
                    "door_colours_base": "Dover White",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                },
                {
                    "room_key": "bathroom",
                    "original_room_label": "BATHROOM",
                    "bench_tops": ["20mm YDL Classic White Polished"],
                    "door_panel_colours": ["Painted Titanium White"],
                    "door_colours_overheads": "",
                    "door_colours_base": "Painted Titanium White",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                },
            ],
            "appliances": [],
            "others": {},
            "warnings": [],
            "source_documents": [],
            "analysis": {"mode": "heuristic_only"},
        }
        enriched = enrich_snapshot_rooms(snapshot, documents)
        rooms = {row["room_key"]: row for row in enriched["rooms"]}
        self.assertEqual(rooms["kitchen"]["bench_tops_wall_run"], "40mm YDL Giusto Polished")
        self.assertEqual(rooms["kitchen"]["bench_tops_island"], "40mm + Waterfall Ends YDL Giusto Polished")
        self.assertEqual(rooms["kitchen"]["door_colours_overheads"], "Polytec Blossom White Matt")
        self.assertEqual(rooms["kitchen"]["door_colours_base"], "Polytec Blossom White Matt")
        self.assertEqual(rooms["kitchen"]["door_colours_island"], "Polytec Topiary Matt")
        self.assertEqual(rooms["kitchen"]["door_colours_bar_back"], "Polytec Topiary Matt")
        self.assertEqual(rooms["laundry"]["door_colours_base"], "Polytec Blossom White Matt")
        self.assertNotIn("Dover White", rooms["laundry"]["door_panel_colours"])
        self.assertEqual(rooms["bathroom"]["door_colours_base"], "Polytec Nouveau Grey Matt")
        self.assertNotIn("Painted Titanium White", rooms["bathroom"]["door_panel_colours"])

    def test_flatten_rooms_only_shows_split_benchtops_for_kitchen(self) -> None:
        rows = _flatten_rooms(
            {
                "rooms": [
                    {
                        "room_key": "kitchen",
                        "original_room_label": "KITCHEN",
                        "bench_tops": [],
                        "bench_tops_wall_run": "40mm YDL Giusto Polished",
                        "bench_tops_island": "40mm + Waterfall Ends YDL Giusto Polished",
                        "bench_tops_other": "",
                    },
                    {
                        "room_key": "laundry",
                        "original_room_label": "LAUNDRY",
                        "bench_tops": ["20mm YDL Classic White Polished"],
                        "bench_tops_wall_run": "20mm YDL Classic White Polished",
                        "bench_tops_island": "",
                        "bench_tops_other": "20mm YDL Classic White Polished",
                    },
                ]
            }
        )
        flattened = {row["room_key"]: row for row in rows}
        self.assertTrue(flattened["kitchen"]["show_split_benchtops"])
        self.assertFalse(flattened["laundry"]["show_split_benchtops"])
        self.assertEqual(flattened["laundry"]["bench_tops_other"], "20mm YDL Classic White Polished")

    def test_build_spec_snapshot_keeps_global_conservative_room_shape_when_openai_splits_rooms(self) -> None:
        base_snapshot = {
            "job_no": "37529",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "rooms": [
                {
                    "room_key": "kitchen",
                    "original_room_label": "Kitchen",
                    "bench_tops": ["20mm stone"],
                    "door_panel_colours": ["Polytec White"],
                    "toe_kick": [],
                    "bulkheads": [],
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                    "splashback": "",
                    "flooring": "",
                    "source_file": "sample.pdf",
                    "page_refs": "1",
                    "evidence_snippet": "Kitchen line",
                    "confidence": 0.7,
                },
                {
                    "room_key": "vanities",
                    "original_room_label": "Vanities",
                    "bench_tops": ["20mm stone"],
                    "door_panel_colours": ["Polytec White"],
                    "toe_kick": [],
                    "bulkheads": [],
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                    "splashback": "",
                    "flooring": "",
                    "source_file": "sample.pdf",
                    "page_refs": "2",
                    "evidence_snippet": "Vanities line",
                    "confidence": 0.7,
                },
            ],
            "appliances": [],
            "others": {"flooring_notes": "", "splashback_notes": ""},
            "warnings": [],
            "source_documents": [],
            "analysis": {"mode": "heuristic_only"},
        }
        openai_payload = {
            "output_text": (
                '{"rooms": ['
                '{"room_key": "kitchen", "original_room_label": "Kitchen", "bench_tops": ["AI bench"]},'
                '{"room_key": "main_bathroom", "original_room_label": "Main Bathroom", "bench_tops": ["AI vanity bench"]},'
                '{"room_key": "ensuite_1", "original_room_label": "Ensuite 1", "bench_tops": ["AI vanity bench"]}'
                '], "appliances": [], "others": {}, "warnings": []}'
            )
        }
        with (
            mock.patch.object(extraction_service.runtime, "OPENAI_ENABLED", True),
            mock.patch.object(extraction_service.runtime, "OPENAI_API_KEY", "test-key"),
            mock.patch.object(extraction_service.runtime, "OPENAI_MODEL", "gpt-4.1-mini"),
            mock.patch("App.services.extraction_service._load_documents", return_value=[]),
            mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=base_snapshot),
            mock.patch(
                "App.services.extraction_service.parsing.enrich_snapshot_rooms",
                side_effect=lambda payload, _documents, rule_flags=None: payload,
            ),
            mock.patch("App.services.extraction_service._post_responses_api", return_value=openai_payload),
        ):
            snapshot = extraction_service.build_spec_snapshot(
                job={"job_no": "37529"},
                builder={"name": "Clarendon", "parser_strategy": "stable_hybrid"},
                files=[],
                template_files=[],
            )
        self.assertEqual([row["room_key"] for row in snapshot["rooms"]], ["kitchen", "vanities"])
        self.assertEqual(snapshot["analysis"]["parser_strategy"], "global_conservative")
        self.assertEqual(snapshot["rooms"][0]["bench_tops"], ["20mm stone"])

    def test_build_spec_snapshot_compacts_clarendon_rooms_under_stable_hybrid(self) -> None:
        base_snapshot = {
            "job_no": "37017",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "rooms": [
                {"room_key": "powder", "original_room_label": "Powder", "bench_tops": [], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "1", "evidence_snippet": "", "confidence": 0.4},
                {"room_key": "bathroom", "original_room_label": "Bathroom", "bench_tops": [], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "Bathroom basin", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "2", "evidence_snippet": "", "confidence": 0.4},
                {"room_key": "ensuite", "original_room_label": "Ensuite", "bench_tops": [], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "Ensuite tap", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "3", "evidence_snippet": "", "confidence": 0.4},
                {"room_key": "vanity", "original_room_label": "Vanity", "bench_tops": ["20mm stone"], "door_panel_colours": ["Polytec White"], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "Primary vanity basin", "tap_info": "Primary vanity tap", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "4", "evidence_snippet": "", "confidence": 0.7},
                {"room_key": "laundry", "original_room_label": "Laundry", "bench_tops": ["Laminate"], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "5", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "butlers_pantry", "original_room_label": "Butler's Pantry", "bench_tops": ["Stone"], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "6", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "wip", "original_room_label": "WIP", "bench_tops": [], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "7", "evidence_snippet": "", "confidence": 0.2},
                {"room_key": "theatre", "original_room_label": "Theatre", "bench_tops": ["Laminate"], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "8", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "rumpus", "original_room_label": "Rumpus", "bench_tops": ["Laminate"], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "9", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "kitchen", "original_room_label": "Kitchen", "bench_tops": ["Stone"], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "10", "evidence_snippet": "", "confidence": 0.8},
                {"room_key": "pantry", "original_room_label": "Pantry", "bench_tops": [], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "source_file": "sample.pdf", "page_refs": "11", "evidence_snippet": "", "confidence": 0.2},
                {"room_key": "wir", "original_room_label": "WIR", "bench_tops": [], "door_panel_colours": [], "toe_kick": [], "bulkheads": [], "handles": [], "sink_info": "", "basin_info": "", "tap_info": "", "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "Carpet", "source_file": "sample.pdf", "page_refs": "12", "evidence_snippet": "", "confidence": 0.2},
            ],
            "appliances": [],
            "others": {},
            "warnings": [],
            "source_documents": [],
            "analysis": {"mode": "heuristic_only"},
        }
        with (
            mock.patch.object(extraction_service.runtime, "OPENAI_ENABLED", False),
            mock.patch("App.services.extraction_service._load_documents", return_value=[]),
            mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=base_snapshot),
            mock.patch(
                "App.services.extraction_service.parsing.enrich_snapshot_rooms",
                side_effect=lambda payload, _documents, rule_flags=None: payload,
            ),
        ):
            snapshot = extraction_service.build_spec_snapshot(
                job={"job_no": "37017"},
                builder={"name": "Clarendon", "parser_strategy": "stable_hybrid"},
                files=[],
                template_files=[],
            )
        self.assertEqual([row["room_key"] for row in snapshot["rooms"]], ["kitchen", "butlers_pantry", "vanities", "laundry", "theatre", "rumpus"])
        vanities = next(row for row in snapshot["rooms"] if row["room_key"] == "vanities")
        self.assertEqual(vanities["original_room_label"], "Vanities")
        self.assertEqual(vanities["bench_tops"], ["20mm stone"])
        self.assertEqual(vanities["basin_info"], "Primary vanity basin")

    def test_builder_defaults_to_global_conservative_for_all_builders(self) -> None:
        clarendon_id = store.create_builder("Clarendon", "clarendon", "")
        yellowwood_id = store.create_builder("Yellowwood", "yellowwood", "")
        self.assertEqual(store.get_builder(clarendon_id)["parser_strategy"], "global_conservative")
        self.assertEqual(store.get_builder(yellowwood_id)["parser_strategy"], "global_conservative")

    def test_spec_list_page_requires_login(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37529", builder_id, "Spec List Test", "")
        client = TestClient(app)
        response = client.get(f"/jobs/{job_id}/spec-list", follow_redirects=False)
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/login")

    def test_job_detail_shows_parse_actions_analysis_and_auto_upload_inputs(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37529", builder_id, "Diagnostics Test", "")
        raw_snapshot = {
            "job_no": "37529",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "analysis": {
                "mode": "openai_merged",
                "parser_strategy": "global_conservative",
                "openai_attempted": True,
                "openai_succeeded": True,
                "openai_model": "gpt-4.1-mini",
                "note": "OpenAI result merged with heuristic parsing.",
                "worker_pid": 4242,
                "app_build_id": "build-test",
            },
            "rooms": [],
            "appliances": [],
            "others": {},
            "warnings": [],
            "source_documents": [],
        }
        store.upsert_snapshot(job_id, "raw_spec", raw_snapshot)
        client = TestClient(app)
        self._login(client)
        response = client.get(f"/jobs/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Parse Spec Files", response.text)
        self.assertIn("Parse Drawing Files", response.text)
        self.assertIn("OpenAI merged", response.text)
        self.assertIn("Global Conservative", response.text)
        self.assertIn("build-test", response.text)
        self.assertIn("gpt-4.1-mini", response.text)
        self.assertIn("requestSubmit()", response.text)
        self.assertNotIn("Upload Specs", response.text)
        self.assertNotIn("Upload Drawings", response.text)
        self.assertNotIn("Builder Cleaning Rules", response.text)

    def test_job_detail_handles_scalar_room_fields_in_review_snapshot(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37006", builder_id, "Legacy Snapshot Shapes", "")
        snapshot = {
            "job_no": "37006",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "analysis": {
                "mode": "openai_merged",
                "openai_attempted": True,
                "openai_succeeded": True,
                "openai_model": "gpt-4.1-mini",
                "note": "Legacy scalar room fields should still render.",
            },
            "rooms": [
                {
                    "room_key": "Kitchen",
                    "original_room_label": "Kitchen",
                    "bench_tops": "20mm stone bench",
                    "door_panel_colours": "Polytec White",
                    "toe_kick": "Matching melamine",
                    "bulkheads": None,
                    "handles": "Hettich knob",
                    "drawers_soft_close": "Yes",
                    "hinges_soft_close": "No",
                    "splashback": "Tiled by others",
                    "flooring": "Tile",
                    "source_file": "legacy.pdf",
                    "page_refs": ["PAGE 2", "PAGE 3"],
                    "evidence_snippet": "Legacy mixed field shapes",
                    "confidence": "High",
                }
            ],
            "appliances": [],
            "others": {},
            "warnings": [],
            "source_documents": [],
        }
        store.upsert_snapshot(job_id, "raw_spec", snapshot)
        store.upsert_review(job_id, snapshot)
        client = TestClient(app)
        self._login(client)
        response = client.get(f"/jobs/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("20mm stone bench", response.text)
        self.assertIn("PAGE 2 | PAGE 3", response.text)
        self.assertIn("Soft Close", response.text)
        self.assertIn("Not Soft Close", response.text)

    def test_run_history_partial_shows_live_stage_and_message(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37529", builder_id, "Run Status Test", "")
        run_id = store.create_run(job_id, "spec")
        self.assertTrue(store.acquire_worker_lease("worker-a", 4242, "build-test"))
        self.assertFalse(store.acquire_worker_lease("worker-b", 9898, "build-other"))
        claimed = store.claim_next_run(worker_pid=4242, app_build_id="build-test", worker_token="worker-a")
        self.assertEqual(claimed["id"], run_id)
        store.update_run_runtime_metadata(run_id, "global_conservative", 4242, "build-test")
        store.update_run_progress(run_id, "official_size_extraction", "Extracting official dimensions from spec PDF for Westinghouse WHC943BD", worker_token="worker-a")
        client = TestClient(app)
        self._login(client)
        response = client.get(f"/jobs/{job_id}/run-history")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Official size extraction", response.text)
        self.assertIn("Extracting official dimensions from spec PDF for Westinghouse WHC943BD", response.text)
        self.assertIn("Global Conservative", response.text)
        self.assertIn("PID 4242 | build-test", response.text)
        self.assertIn("hx-trigger=\"load, every 2s\"", response.text)

    def test_parse_request_requires_uploaded_files(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37529", builder_id, "Missing Files Test", "")
        client = TestClient(app)
        self._login(client)
        job_page = client.get(f"/jobs/{job_id}")
        csrf = job_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]
        response = client.post(
            f"/jobs/{job_id}/runs/start",
            data={"run_kind": "spec", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], f"/jobs/{job_id}")
        self.assertEqual(store.list_runs(job_id), [])

    def test_jobs_page_filters_by_job_number_query(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        store.create_job("37529", builder_id, "Kitchen Spec", "")
        store.create_job("47001", builder_id, "Laundry Spec", "")
        client = TestClient(app)
        self._login(client)
        response = client.get("/jobs?q=375")
        self.assertEqual(response.status_code, 200)
        self.assertIn("37529", response.text)
        self.assertNotIn("47001", response.text)
        self.assertIn('value="375"', response.text)

    def test_builder_rules_routes_redirect_and_builders_page_uses_global_profile(self) -> None:
        builder_a = store.create_builder("Clarendon", "clarendon", "")
        builder_b = store.create_builder("Yellowwood", "yellowwood", "")
        job_id = store.create_job("37016", builder_a, "Rules Job", "")
        store.upsert_snapshot(
            job_id,
            "raw_spec",
            {
                "job_no": "37016",
                "builder_name": "Clarendon",
                "source_kind": "spec",
                "generated_at": "2026-03-22T10:00:00+00:00",
                "analysis": {
                    "mode": "heuristic_only",
                    "parser_strategy": "global_conservative",
                    "openai_attempted": False,
                    "openai_succeeded": False,
                    "openai_model": "",
                    "note": "",
                },
                "rooms": [],
                "appliances": [],
                "others": {},
                "warnings": [],
                "source_documents": [],
            },
        )
        client = TestClient(app)
        self._login(client)

        builders_page = client.get("/builders")
        self.assertEqual(builders_page.status_code, 200)
        self.assertIn("global conservative extraction profile", builders_page.text.lower())
        self.assertNotIn("Cleaning Rules", builders_page.text)
        csrf = builders_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

        page = client.get(f"/builders/{builder_a}/rules", follow_redirects=False)
        self.assertEqual(page.status_code, 303)
        self.assertEqual(page.headers["location"], "/builders")

        response = client.post(
            f"/builders/{builder_a}/rules",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], "/builders")
        builder_a_row = store.get_builder(builder_a)
        builder_b_row = store.get_builder(builder_b)
        self.assertEqual(builder_a_row["parser_strategy"], "global_conservative")
        self.assertEqual(builder_b_row["parser_strategy"], "global_conservative")
        self.assertTrue(builder_a_row["rule_flags"]["normalize_brand_casing"])
        self.assertTrue(builder_b_row["rule_flags"]["normalize_brand_casing"])

        job_page = client.get(f"/jobs/{job_id}")
        self.assertEqual(job_page.status_code, 200)
        self.assertIn("Global Extraction Profile", job_page.text)
        self.assertNotIn("Builder Cleaning Rules", job_page.text)

    def test_spec_list_page_shows_material_summary_official_links_and_unicode_excel(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37529", builder_id, "Unicode Test", "")
        raw_snapshot = {
            "job_no": "37529",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "analysis": {
                "mode": "heuristic_only",
                "openai_attempted": False,
                "openai_succeeded": False,
                "openai_model": "gpt-4.1-mini",
                "note": "OpenAI is disabled in runtime settings.",
            },
            "rooms": [
                {
                    "room_key": "kitchen",
                    "original_room_label": "Kitchen \u4e2d\u6587",
                    "bench_tops": [
                        "Quantum Zero Midnight Black 20mm pencil round edge to cooktop run and Quantum Zero Venatino Statuario 40mm mitred apron edge to island bench",
                    ],
                    "door_panel_colours": [
                        "Polytec Blossom White Matt Finish - overhead cabinetry",
                        "Polytec Blossom White Matt Finish - base cabinetry",
                        "Polytec Tempest Woodgrain - island base cabinetry",
                    ],
                    "door_colours_overheads": "Polytec Blossom White Matt Finish - overhead cabinetry",
                    "door_colours_base": "Polytec Blossom White Matt Finish - base cabinetry",
                    "door_colours_island": "Polytec Tempest Woodgrain - island base cabinetry",
                    "door_colours_bar_back": "",
                    "toe_kick": ["Matching finish"],
                    "bulkheads": ["Bulkhead A"],
                    "handles": [
                        "Hettich Cipri 9070585 Gloss Chrome Plated 30mm Knob - door location: 30mm in and 60mm up/down",
                        "Hettich Cipri 9070585 Gloss Chrome Plated 30mm Knob - drawer location: centre to profile",
                    ],
                    "sink_info": "PARISI Quadro Double Bowl (PK8644)",
                    "basin_info": "",
                    "tap_info": "PHOENIX Nostalgia Sink Mixer NS714-62",
                    "drawers_soft_close": "Soft Close",
                    "hinges_soft_close": "No",
                    "splashback": "Glass / \u4e2d\u6587",
                    "flooring": "Hybrid flooring",
                    "source_file": "sample spec.pdf",
                    "page_refs": "12",
                    "evidence_snippet": "Kitchen evidence with \u4e2d\u6587 and symbols",
                    "confidence": 0.91,
                }
            ],
            "appliances": [
                {
                    "appliance_type": "Cooktop",
                    "make": "Westinghouse",
                    "model_no": "WHC943BD",
                    "product_url": "https://official.example/product/WHC943BD",
                    "spec_url": "https://official.example/spec/WHC943BD.pdf",
                    "manual_url": "https://official.example/manual/WHC943BD.pdf",
                    "website_url": "https://official.example/product/WHC943BD",
                    "overall_size": "900 x 510 x 60 mm",
                    "source_file": "sample spec.pdf",
                    "page_refs": "13",
                    "evidence_snippet": "Cooktop evidence \u4e2d\u6587",
                    "confidence": 0.87,
                },
                {
                    "appliance_type": "Sink",
                    "make": "Parisi",
                    "model_no": "PK8644",
                    "product_url": "https://official.example/sink",
                    "spec_url": "",
                    "manual_url": "",
                    "website_url": "https://official.example/sink",
                    "overall_size": "860 x 440 x 210 mm",
                    "source_file": "sample spec.pdf",
                    "page_refs": "12",
                    "evidence_snippet": "Sink evidence",
                    "confidence": 0.81,
                },
            ],
            "others": {
                "flooring_notes": "Hybrid flooring / \u4e2d\u6587",
                "splashback_notes": "Glass splashback",
            },
            "warnings": ["Low-text page detected in template.pdf page 8."],
            "source_documents": [{"file_name": "template.pdf", "role": "spec", "page_count": "20"}],
        }
        reviewed_snapshot = {
            **raw_snapshot,
            "rooms": [{**raw_snapshot["rooms"][0], "splashback": "Reviewed value should not appear"}],
        }
        store.upsert_snapshot(job_id, "raw_spec", raw_snapshot)
        store.upsert_review(job_id, reviewed_snapshot)

        client = TestClient(app)
        self._login(client)

        page = client.get(f"/jobs/{job_id}/spec-list")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Material Summary", page.text)
        self.assertIn("2 distinct items", page.text)
        self.assertIn("Hettich Cipri 9070585 Gloss Chrome Plated 30mm Knob", page.text)
        self.assertIn("Quantum Zero Midnight Black 20mm pencil round edge", page.text)
        self.assertIn("Quantum Zero Venatino Statuario 40mm mitred apron edge", page.text)
        self.assertIn("Kitchen \u4e2d\u6587", page.text)
        self.assertIn("PARISI Quadro Double Bowl (PK8644)", page.text)
        self.assertIn("PHOENIX Nostalgia Sink Mixer NS714-62", page.text)
        self.assertIn("Wall Run Bench Top", page.text)
        self.assertIn("Island Bench Top", page.text)
        self.assertIn("https://official.example/product/WHC943BD", page.text)
        self.assertNotIn("Reviewed value should not appear", page.text)
        self.assertNotIn("<td>Sink</td>", page.text)
        self.assertIn("Not Soft Close", page.text)

        export_response = client.get(f"/jobs/{job_id}/spec-list.xlsx")
        self.assertEqual(export_response.status_code, 200)
        workbook = load_workbook(io.BytesIO(export_response.content))
        rooms_sheet = workbook["Rooms"]
        appliances_sheet = workbook["Appliances"]
        warnings_sheet = workbook["Warnings"]
        meta_sheet = workbook["Meta"]
        self.assertEqual(rooms_sheet["B2"].value, "Kitchen \u4e2d\u6587")
        self.assertEqual(rooms_sheet["D2"].value, "Quantum Zero Midnight Black 20mm pencil round edge")
        self.assertEqual(rooms_sheet["E2"].value, "Quantum Zero Venatino Statuario 40mm mitred apron edge")
        self.assertEqual(rooms_sheet["H2"].value, "Polytec Blossom White Matt Finish - overhead cabinetry")
        self.assertEqual(rooms_sheet["O2"].value, "PARISI Quadro Double Bowl (PK8644)")
        self.assertEqual(rooms_sheet["Q2"].value, "PHOENIX Nostalgia Sink Mixer NS714-62")
        self.assertEqual(rooms_sheet["R2"].value, "Soft Close")
        self.assertEqual(rooms_sheet["S2"].value, "Not Soft Close")
        self.assertEqual(appliances_sheet["A2"].value, "Cooktop")
        self.assertEqual(appliances_sheet["D2"].value, "https://official.example/product/WHC943BD")
        self.assertEqual(appliances_sheet["F2"].value, "900 x 510 x 60 mm")
        self.assertIsNotNone(appliances_sheet["D2"].hyperlink)
        self.assertIsNone(appliances_sheet["A3"].value)
        self.assertEqual(warnings_sheet["A2"].value, "Low-text page detected in template.pdf page 8.")
        meta_rows = [row[0] for row in meta_sheet.iter_rows(min_row=2, values_only=True)]
        self.assertIn("analysis_mode", meta_rows)
        self.assertIn("analysis_rule_flags", meta_rows)

    def test_spec_list_page_shows_empty_message_without_raw_snapshot(self) -> None:
        builder_id = store.create_builder("Yellowwood", "yellowwood", "")
        job_id = store.create_job("37974", builder_id, "No Snapshot", "")
        client = TestClient(app)
        self._login(client)
        response = client.get(f"/jobs/{job_id}/spec-list")
        self.assertEqual(response.status_code, 200)
        self.assertIn("No raw spec snapshot yet", response.text)

    def _login(self, client: TestClient) -> str:
        login_page = client.get("/login")
        csrf = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]
        response = client.post(
            "/login",
            data={"username": "admin", "password": "admin", "csrf_token": csrf},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        return csrf


if __name__ == "__main__":
    unittest.main()
