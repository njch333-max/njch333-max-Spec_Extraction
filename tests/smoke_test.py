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

from App.main import _build_material_summary, _flatten_rooms, app
from App.services import extraction_service, parsing as parsing_module, store
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

    def test_parser_splits_inline_kitchen_benchtop_sentence_into_wall_run_and_island(self) -> None:
        snapshot = parse_documents(
            job_no="37050",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "clarendon-kitchen.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Kitchen\n"
                                "Benchtop - Quantum Zero White Swirl - 20MM Pencil Round Edge - TO Cooktop Run / "
                                "40MM Mitred Apron Edge - TO Island Bench\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        room = snapshot["rooms"][0]
        self.assertEqual(room["bench_tops_wall_run"], "Quantum Zero White Swirl - 20MM Pencil Round Edge")
        self.assertEqual(room["bench_tops_island"], "Quantum Zero White Swirl - 40MM Mitred Apron Edge")
        self.assertIn("Back Benchtops Quantum Zero White Swirl - 20MM Pencil Round Edge", room["bench_tops"])
        self.assertIn("Island Benchtop Quantum Zero White Swirl - 40MM Mitred Apron Edge", room["bench_tops"])

    def test_parser_stops_benchtop_before_doors_panels_field(self) -> None:
        snapshot = parse_documents(
            job_no="37051",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "vanities.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Vanities\n"
                                "Benchtop Quantum Zero Luna White - 20MM Pencil Round Edge / 140MM Mitred Apron Edge - to Powder Room 2\n"
                                "Doors/Panels - Polytec Jamaican Oak Matt Finish Melamine with Matching 1MM ABS Edges (Vertical Grain Direction)\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        room = snapshot["rooms"][0]
        self.assertNotIn("Doors/Panels", " ".join(room["bench_tops"]))
        self.assertNotIn("Polytec Jamaican Oak", " ".join(room["bench_tops"]))
        self.assertIn("Polytec Jamaican Oak Matt Finish Melamine with Matching 1MM ABS Edges", room["door_colours_base"])
        self.assertEqual(room["door_colours_overheads"], "")

    def test_multi_file_parse_keeps_room_materials_from_room_master_only(self) -> None:
        snapshot = parse_documents(
            job_no="37052",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "supplement.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Kitchen\n"
                                "Benchtop Wrong Stone 40mm\n"
                                "Sink Type/Model Franke Box Sink\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
                {
                    "file_name": "master.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "KITCHEN COLOUR SCHEDULE\n"
                                "Benchtop Master Stone 20mm Pencil Round Edge\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
            ],
        )
        room = snapshot["rooms"][0]
        self.assertIn("Master Stone 20mm Pencil Round Edge", " ".join(room["bench_tops"]))
        self.assertNotIn("Wrong Stone", " ".join(room["bench_tops"]))
        self.assertIn("Franke Box", room["sink_info"])

    def test_labeled_appliance_details_do_not_jump_to_next_row_context(self) -> None:
        snapshot = parse_documents(
            job_no="37053",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "appliance-table.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Rangehood:\n"
                                "Westinghouse\n"
                                "HP280L\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        self.assertEqual(snapshot["appliances"][0]["appliance_type"], "Rangehood")
        self.assertEqual(snapshot["appliances"][0]["make"], "Westinghouse")
        self.assertEqual(snapshot["appliances"][0]["model_no"], "")
        self.assertEqual(parsing_module._limit_appliance_details_to_local_context("Westinghouse\nHP280L\n"), "Westinghouse")

    def test_clarendon_reference_polish_rebuilds_clean_room_fields(self) -> None:
        snapshot = {
            "job_no": "37031",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T00:00:00+00:00",
            "analysis": {"mode": "openai_merged", "parser_strategy": "global_conservative"},
            "rooms": [
                {
                    "room_key": "kitchen",
                    "original_room_label": "Kitchen",
                    "bench_tops": ["SINKCUT OUTCENTRE", "QUANTUM ZERO MIDNIGHT - 20MM PENCIL ROUND EDGE", "60MM UP/ DOWN TO DOORS DOOR"],
                    "bench_tops_wall_run": "",
                    "bench_tops_island": "",
                    "bench_tops_other": "SINKCUT OUTCENTRE | QUANTUM ZERO MIDNIGHT - 20MM PENCIL ROUND EDGE | 60MM UP/ DOWN TO DOORS DOOR",
                    "door_panel_colours": [],
                    "door_colours_overheads": "",
                    "door_colours_base": "",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "toe_kick": ["MATCHING MELAMINE FINISH / MATCHING THERMO FINISH"],
                    "bulkheads": ["BY BUILDERBULKHEAD SHADOWLINE AS MATCHING MELAMINE"],
                    "handles": [],
                    "drawers_soft_close": "Soft Close",
                    "hinges_soft_close": "Not Soft Close",
                    "splashback": "BY OTHERS20MM STONE",
                    "flooring": "",
                    "sink_info": "& TAP TOCABINET UNDER CUT OUT DETAIL FOR PARISI QUADRO PK8644 DOUBLE BOWL UNDERMOUNT",
                    "basin_info": "",
                    "tap_info": "",
                    "source_file": "schedule.pdf",
                    "page_refs": "1",
                    "evidence_snippet": "",
                    "confidence": 0.6,
                },
                {
                    "room_key": "butlers_pantry",
                    "original_room_label": "Butler's Pantry",
                    "bench_tops": ["bad"],
                    "bench_tops_wall_run": "",
                    "bench_tops_island": "",
                    "bench_tops_other": "bad",
                    "door_panel_colours": [],
                    "door_colours_overheads": "",
                    "door_colours_base": "",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "toe_kick": ["bad"],
                    "bulkheads": ["bad"],
                    "handles": [],
                    "drawers_soft_close": "Not Soft Close",
                    "hinges_soft_close": "",
                    "splashback": "bad",
                    "flooring": "",
                    "sink_info": "bad",
                    "basin_info": "bad",
                    "tap_info": "bad",
                    "source_file": "schedule.pdf",
                    "page_refs": "2",
                    "evidence_snippet": "",
                    "confidence": 0.6,
                },
                {
                    "room_key": "vanities",
                    "original_room_label": "Vanities",
                    "bench_tops": ["bad"],
                    "bench_tops_wall_run": "",
                    "bench_tops_island": "",
                    "bench_tops_other": "bad",
                    "door_panel_colours": [],
                    "door_colours_overheads": "",
                    "door_colours_base": "",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "toe_kick": ["bad"],
                    "bulkheads": ["bad"],
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                    "splashback": "",
                    "flooring": "",
                    "sink_info": "",
                    "basin_info": "",
                    "tap_info": "",
                    "source_file": "schedule.pdf",
                    "page_refs": "3",
                    "evidence_snippet": "",
                    "confidence": 0.6,
                },
                {
                    "room_key": "laundry",
                    "original_room_label": "Laundry",
                    "bench_tops": ["bad"],
                    "bench_tops_wall_run": "",
                    "bench_tops_island": "",
                    "bench_tops_other": "bad",
                    "door_panel_colours": [],
                    "door_colours_overheads": "",
                    "door_colours_base": "",
                    "door_colours_island": "",
                    "door_colours_bar_back": "",
                    "toe_kick": ["bad"],
                    "bulkheads": ["bad"],
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                    "splashback": "BY OTHERS20MM STONE BENCHTOP",
                    "flooring": "",
                    "sink_info": "bad",
                    "basin_info": "",
                    "tap_info": "bad",
                    "source_file": "schedule.pdf",
                    "page_refs": "4",
                    "evidence_snippet": "",
                    "confidence": 0.6,
                },
            ],
            "appliances": [],
            "others": {},
            "warnings": [],
        }
        documents = [
            {
                "file_name": "drawings.pdf",
                "role": "spec",
                "pages": [
                    {
                        "page_no": 1,
                        "text": (
                            "BENCHTOP COLOUR 1 - QUANTUM ZERO MIDNIGHT BLACK - 20MM PENCIL ROUND EDGE - TO COOKTOP RUN"
                            "BENCHTOP COLOUR 2 - QUANTUM ZERO VENATINO STATUARIO - 40MM MITRED APRON EDGE - TO ISLAND BENCH"
                            "DOOR COLOUR 1 - POL YTEC CLASSIC WHITE MATT FINISH THERMOLAMINATE - ATLANTA EM2 PROFILE - TO COOKTOP RUN BASE, UPPER + TALL CABINETRY "
                            "DOOR COLOUR 2 - POL YTEC TEMPEST WOODGRAIN FINISH THERMOLAMINATE - ATLANTA EM2 PROFILE (VERTICAL GRAIN DIRECTION) - ISLAND BASE CABINETRY + BAR BACK PANELS "
                            "HANDLE 1 - HETTICH CIPRI 9070585 GLOSS CHROME PLATED 30MM KNOB - 30MM IN AND 60MM UP/ DOWN TO DOORS "
                            "HANDLE 2 - MOMO FLORENCIA CTCP .CP .FG CHROME PLATED 104MM LONG - CENTRE TO DRAWER PROFILE "
                            "DOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSE "
                            "DRAWER RUNNERS - HETTICH INNOTECH ATIRA SOFT CLOSE RUNNERS "
                            "KITCHEN COLOUR SCHEDULE THERMOLAMINATE NOTES : * BULKHEAD SHADOWLINE : MATCHING MELAMINE FINISH* KICKBOARDS : MATCHING MELAMINE FINISH / MATCHING THERMO FINISH"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 2,
                        "text": (
                            "BENCHTOP - QUANTUM ZERO MIDNIGHT - 20MM PENCIL ROUND EDGE "
                            "DOOR COLOUR - POL YTEC CLASSIC WHITE MATT FINISH THERMOLAMINATE - ATLANTA EM2 PROFILE "
                            "THERMOLAMINATE NOTES : * BULKHEAD SHADOWLINE : MATCHING MELAMINE FINISH* KICKBOARDS : MATCHING MELAMINE FINISH "
                            "HANDLES - HETTICH CIPRI 9070585 GLOSS CHROME PLATED 30MM KNOB - 30MM IN AND 60MM UP/ DOWN TO DOORS "
                            "DOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSE "
                            "BUTLERS PANTRY COLOUR SCHEDULE"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 3,
                        "text": (
                            "BENCHTOP - QUANTUM ZERO MIDNIGHT - 20MM PENCIL ROUND EDGE "
                            "DOOR COLOUR - POL YTEC CLASSIC WHITE MATT FINISH THERMOLAMINATE - ATLANTA EM2 PROFILE "
                            "THERMOLAMINATE NOTES : * KICKBOARDS : N/A FLOATING "
                            "HANDLES - HETTICH CIPRI 9070585 GLOSS CHROME PLATED 30MM KNOB - DOOR LOCATION : 30MM IN AND 60MM UP/ DOWN TO DOORS DRAWER LOCATION : CTR TO PROFILE "
                            "DOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSE "
                            "DRAWER RUNNERS - HETTICH MUL TITECH STANDARD CONSTRUCTION RUNNERS - NOT SOFT CLOSE "
                            "VANITIES COLOUR SCHEDULE"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 4,
                        "text": (
                            "BENCHTOP - QUANTUM ZERO MIDNIGHT - 20MM PENCIL ROUND EDGE "
                            "DOOR COLOUR - POL YTEC CLASSIC WHITE MATT FINISH THERMOLAMINATE - ATLANTA EM2 PROFILE "
                            "THERMOLAMINATE NOTES : * BULKHEAD SHADOWLINE : MATCHING MELAMINE FINISH* KICKBOARDS : MATCHING MELAMINE FINISH "
                            "HANDLES - HETTICH CIPRI 9070585 GLOSS CHROME PLATED 30MM KNOB - 30MM IN AND 60MM UP/ DOWN TO DOORS "
                            "DOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSE "
                            "LAUNDRY COLOUR SCHEDULE"
                        ),
                        "needs_ocr": False,
                    },
                ],
            },
            {
                "file_name": "fixtures.pdf",
                "role": "spec",
                "pages": [
                    {
                        "page_no": 5,
                        "text": (
                            "KITCHEN SUPPLIER DESCRIPTION DESIGN COMMENTS "
                            "Sink Type: PARISI QUADRO_DOUBLE BOWL_STAINLESS STEEL (PK8644) UNDERMOUNT "
                            "Tap Type: PHOENIX Nostalgia Twin Handle Sink Mixer 230mm Shepherds Crook NS714-62 CHROME & WHITE "
                            "Splashback: "
                            "Sink Type/Model: PARISI UNDERMOUNT - QUADRO SINGLE BOWL STAINLESS STEEL (PK4444) UNDERMOUNT "
                            "Tap Type: PHOENIX Nostalgia Sink Mixer 220mm Shepherds Crook NS738-62 CHROME & WHITE "
                            "BUTLERS PANTRY"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 6,
                        "text": (
                            "Vanity Inset Basin JOHNSON SUISSE Emilia Rectangular Undercounter Basin (JBSE250.PW6) WHITE "
                            "Vanity Tap Style: PHOENIX NOSTALGIA BASIN MIXER 160MM SHEPHERDS CROOK (NS748-62) CHROME & WHITE "
                            "Vanity Waste Colour: CHROME"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 7,
                        "text": (
                            "LAUNDRY SUPPLIER DESCRIPTION DESIGN COMMENTS "
                            "Drop in Tub: EVERHARD "
                            "Splashback: "
                            "EVERHARD INDUSTRIES CLASSIC 45L UTILITY SINK (71245) "
                            "PINA SINK MIXER GOOSENECK 200MM_CHROME (153-7330-00) "
                            "15MM CP QUARTER TURN WASHING MACHINE COCK (60822)"
                        ),
                        "needs_ocr": False,
                    },
                ],
            },
        ]
        polished = extraction_service._apply_clarendon_reference_polish(
            snapshot,
            documents,
            builder_name="Clarendon",
            parser_strategy="global_conservative",
        )
        rooms = {row["room_key"]: row for row in polished["rooms"]}
        self.assertNotIn("SINKCUT", " ".join(rooms["kitchen"]["bench_tops"]))
        self.assertEqual(rooms["kitchen"]["bench_tops_wall_run"], "Quantum Zero Midnight Black - 20MM Pencil Round Edge")
        self.assertEqual(rooms["kitchen"]["bench_tops_island"], "Quantum Zero Venatino Statuario - 40MM Mitred Apron Edge")
        self.assertEqual(rooms["kitchen"]["splashback"], "Tiled splashback by others")
        self.assertEqual(
            rooms["kitchen"]["handles"],
            [
                "Hettich Cipri 9070585 Gloss Chrome Plated 30MM Knob",
                "Momo Florencia Ctcp .Cp .Fg Chrome Plated 104MM Long - Centre to Drawer Profile",
            ],
        )
        self.assertEqual(rooms["kitchen"]["drawers_soft_close"], "Soft Close")
        self.assertEqual(rooms["kitchen"]["hinges_soft_close"], "Not Soft Close")
        self.assertNotIn("\n", str(rooms["kitchen"]["sink_info"]))
        self.assertEqual(rooms["butlers_pantry"]["door_colours_base"], "Polytec Classic White Matt Finish Thermolaminate - Atlanta EM2 Profile")
        self.assertEqual(rooms["butlers_pantry"]["drawers_soft_close"], "Not Soft Close")
        self.assertEqual(rooms["vanities"]["toe_kick"], ["N/A floating - no kickboard"])
        self.assertTrue(str(rooms["vanities"]["basin_info"]).startswith("Johnson Suisse Emilia"))
        self.assertEqual(rooms["laundry"]["splashback"], "Tiled splashback by others")
        self.assertTrue(str(rooms["laundry"]["tap_info"]).startswith("Pina Sink Mixer Gooseneck"))

    def test_clarendon_reference_polish_handles_luxe_single_line_schedule_family(self) -> None:
        snapshot = {
            "job_no": "37868",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T00:00:00+00:00",
            "analysis": {"mode": "openai_merged", "parser_strategy": "global_conservative"},
            "rooms": [
                {"room_key": "kitchen", "original_room_label": "Kitchen", "bench_tops": ["noisy"], "bench_tops_wall_run": "", "bench_tops_island": "", "bench_tops_other": "noisy", "door_panel_colours": [], "door_colours_overheads": "", "door_colours_base": "", "door_colours_island": "", "door_colours_bar_back": "", "toe_kick": [], "bulkheads": [], "handles": [], "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "sink_info": "", "basin_info": "", "tap_info": "", "source_file": "schedule.pdf", "page_refs": "1", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "butlers_pantry", "original_room_label": "Butler's Pantry", "bench_tops": ["noisy"], "bench_tops_wall_run": "", "bench_tops_island": "", "bench_tops_other": "noisy", "door_panel_colours": [], "door_colours_overheads": "", "door_colours_base": "", "door_colours_island": "", "door_colours_bar_back": "", "toe_kick": [], "bulkheads": [], "handles": [], "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "sink_info": "", "basin_info": "", "tap_info": "", "source_file": "schedule.pdf", "page_refs": "2", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "vanities", "original_room_label": "Vanities", "bench_tops": ["noisy"], "bench_tops_wall_run": "", "bench_tops_island": "", "bench_tops_other": "noisy", "door_panel_colours": [], "door_colours_overheads": "", "door_colours_base": "", "door_colours_island": "", "door_colours_bar_back": "", "toe_kick": [], "bulkheads": [], "handles": [], "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "sink_info": "", "basin_info": "", "tap_info": "", "source_file": "schedule.pdf", "page_refs": "3", "evidence_snippet": "", "confidence": 0.6},
                {"room_key": "laundry", "original_room_label": "Laundry", "bench_tops": ["noisy"], "bench_tops_wall_run": "", "bench_tops_island": "", "bench_tops_other": "noisy", "door_panel_colours": [], "door_colours_overheads": "", "door_colours_base": "", "door_colours_island": "", "door_colours_bar_back": "", "toe_kick": [], "bulkheads": [], "handles": [], "drawers_soft_close": "", "hinges_soft_close": "", "splashback": "", "flooring": "", "sink_info": "", "basin_info": "", "tap_info": "", "source_file": "schedule.pdf", "page_refs": "4", "evidence_snippet": "", "confidence": 0.6},
            ],
            "appliances": [],
            "others": {},
            "warnings": [],
        }
        documents = [
            {
                "file_name": "luxe-drawings.pdf",
                "role": "spec",
                "pages": [
                    {
                        "page_no": 1,
                        "text": (
                            "KITCHEN COLOUR SCHEDULE "
                            "BENCHTOP - QUANTUM ZERO BELLA CARRARA - 20MM PENCIL ROUND EDGE - TO COOKTOP RUN / 40MM MITRED APRON EDGE - TO ISLAND BENCHTOP "
                            "WATERFALL ENDS (MITRED JOIN) "
                            "MIRROR SPLASHBACK - BRONZE MIRRORKOTE "
                            "DOOR COLOUR - POL YTEC SOFT WALNUT MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
                            "KICKBOARDS - AS POL YTEC CLASSIC WHITE 'MATT' FINISH MELAMINE "
                            "SQUARE EDGE RAILS - AS POL YTEC CLASSIC WHITE 'MATT' FINISH MELAMINE "
                            "BULKHEAD SHADOWLINE - AS POL YTEC CLASSIC WHITE 'MATT' FINISH MELAMINE "
                            "HANDLES - SQUARE EDGE HANDLELESS* NOTE : 10MM DOOR OVERHANG TO UPPER CABINETS "
                            "DOOR HINGES - HETTICH SOFT CLOSE "
                            "DRAWER RUNNERS - HETTICH INNOTECH ATIRA SOFT CLOSE RUNNERS"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 2,
                        "text": (
                            "BUTLERS PANTRY COLOUR SCHEDULE "
                            "BENCHTOP - POL YTEC ARGENTO STONE MATT FINISH - 21MM TIGHTFORM EDGE LAMINATE "
                            "DOOR COLOUR - POL YTEC SOFT WALNUT MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
                            "KICKBOARDS - AS POL YTEC CLASSIC WHITE 'MATT' FINISH MELAMINE "
                            "BULKHEAD SHADOWLINE - AS POL YTEC CLASSIC WHITE 'MATT' FINISH MELAMINE "
                            "HANDLES - SQUARE EDGE HANDLELESS* NOTE : 10MM DOOR OVERHANG TO UPPER CABINETS "
                            "DOOR HINGES - HETTICH SOFT CLOSE"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 3,
                        "text": (
                            "VANITIES COLOUR SCHEDULE "
                            "BENCHTOP - QUANTUM ZERO LUNA WHITE - 20MM PENCIL ROUND EDGE / 140MM MITRED APRON EDGE (POWDER ROOM 3) "
                            "DOOR/PANEL COLOUR - POL YTEC SOFT WALNUT MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
                            "KICKBOARDS - N/A FLOATING "
                            "SQUARE EDGE RAILS - AS POL YTEC CLASSIC WHITE 'MATT' FINISH MELAMINE "
                            "HANDLES - SQUARE EDGE HANDLELESS "
                            "DOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSE"
                        ),
                        "needs_ocr": False,
                    },
                    {
                        "page_no": 4,
                        "text": (
                            "LAUNDRY COLOUR SCHEDULE "
                            "BENCHTOP - POL YTEC ARGENTO STONE MATT FINISH - 21MM TIGHTFORM EDGE LAMINATE "
                            "DOOR COLOUR - POL YTEC SOFT WALNUT MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
                            "KICKBOARDS - AS POL YTEC CLASSIC WHITE MATT FINISH MELAMINE "
                            "HANDLES - SQUARE EDGE HANDLELESS* NOTE : 10MM DOOR OVERHANG TO UPPER CABINETS "
                            "DOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSE "
                            "SPLASHBACK - TILED SPLASHBACK BY OTHERS"
                        ),
                        "needs_ocr": False,
                    },
                ],
            }
        ]
        polished = extraction_service._apply_clarendon_reference_polish(
            snapshot,
            documents,
            builder_name="Clarendon",
            parser_strategy="global_conservative",
        )
        rooms = {row["room_key"]: row for row in polished["rooms"]}
        self.assertEqual(rooms["kitchen"]["bench_tops_wall_run"], "Quantum Zero Bella Carrara - 20MM Pencil Round Edge")
        self.assertEqual(rooms["kitchen"]["bench_tops_island"], "Quantum Zero Bella Carrara - 40MM Mitred Apron Edge Waterfall Ends (Mitred Join)")
        self.assertEqual(rooms["kitchen"]["splashback"], "Mirror Splashback - Bronze Mirrorkote")
        self.assertEqual(rooms["kitchen"]["handles"], ["Square Edge Handleless"])
        self.assertEqual(rooms["kitchen"]["door_colours_base"], "Polytec Soft Walnut Matt Finish Melamine with Matching 1MM ABS Edges (Vertical Grain Direction)")
        self.assertEqual(rooms["kitchen"]["hinges_soft_close"], "Soft Close")
        self.assertEqual(rooms["kitchen"]["drawers_soft_close"], "Soft Close")
        self.assertEqual(rooms["butlers_pantry"]["bench_tops"], ["Polytec Argento Stone Matt Finish - 21MM Tightform Edge Laminate"])
        self.assertEqual(rooms["butlers_pantry"]["handles"], ["Square Edge Handleless"])
        self.assertEqual(rooms["vanities"]["bench_tops"], ["Quantum Zero Luna White - 20MM Pencil Round Edge / 140MM Mitred Apron Edge (Powder Room 3)"])
        self.assertEqual(rooms["vanities"]["door_colours_base"], "Polytec Soft Walnut Matt Finish Melamine with Matching 1MM ABS Edges (Vertical Grain Direction)")
        self.assertEqual(rooms["vanities"]["handles"], ["Square Edge Handleless"])
        self.assertEqual(rooms["laundry"]["bench_tops"], ["Polytec Argento Stone Matt Finish - 21MM Tightform Edge Laminate"])
        self.assertEqual(rooms["laundry"]["splashback"], "Tiled splashback by others")
        self.assertEqual(rooms["laundry"]["hinges_soft_close"], "Not Soft Close")

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
            rooms["vanities"]["basin_info"],
            "Johnson Suisse Emilia Rectangular Undercounter Basin (JBSE250.PW6)",
        )
        self.assertEqual(rooms["vanities"]["tap_info"], "Phoenix Nostalgia Basin Mixer NS748-62")
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
        self.assertEqual(snapshot["rooms"][0]["bench_tops"], ["20MM Stone"])

    def test_build_spec_snapshot_keeps_source_driven_rooms_under_global_conservative(self) -> None:
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
        self.assertEqual(
            [row["room_key"] for row in snapshot["rooms"]],
            ["powder", "bathroom", "ensuite", "vanity", "laundry", "butlers_pantry", "wip", "theatre", "rumpus", "kitchen", "pantry", "wir"],
        )
        bathroom = next(row for row in snapshot["rooms"] if row["room_key"] == "bathroom")
        vanity = next(row for row in snapshot["rooms"] if row["room_key"] == "vanity")
        self.assertEqual(bathroom["basin_info"], "Bathroom Basin")
        self.assertEqual(vanity["bench_tops"], ["20MM Stone"])
        self.assertEqual(vanity["basin_info"], "Primary Vanity Basin")

    def test_parse_documents_keeps_bathroom_ensuite_and_powder_separate(self) -> None:
        snapshot = parse_documents(
            job_no="39001",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "rooms.txt",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Main Bathroom\n"
                                "Bench Tops 20mm stone\n"
                                "Ensuite 1\n"
                                "Bench Tops 30mm stone\n"
                                "Powder Room 3\n"
                                "Bench Tops 40mm stone\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        rooms = {row["room_key"]: row for row in snapshot["rooms"]}
        self.assertIn("main_bathroom", rooms)
        self.assertIn("ensuite_1", rooms)
        self.assertIn("powder_room_3", rooms)
        self.assertEqual(rooms["main_bathroom"]["bench_tops"], ["20mm stone"])
        self.assertEqual(rooms["ensuite_1"]["bench_tops"], ["30mm stone"])
        self.assertEqual(rooms["powder_room_3"]["bench_tops"], ["40mm stone"])

    def test_source_driven_room_matching_does_not_bleed_vanity_overlay_between_rooms(self) -> None:
        snapshot = {
            "job_no": "39002",
            "builder_name": "Clarendon",
            "source_kind": "spec",
            "generated_at": "2026-03-22T10:00:00+00:00",
            "rooms": [
                {
                    "room_key": "main_bathroom",
                    "original_room_label": "Main Bathroom",
                    "bench_tops": ["20mm stone"],
                    "door_panel_colours": [],
                    "toe_kick": [],
                    "bulkheads": [],
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                    "splashback": "",
                    "flooring": "",
                    "sink_info": "",
                    "basin_info": "Bathroom basin",
                    "tap_info": "",
                    "source_file": "sample.pdf",
                    "page_refs": "1",
                    "evidence_snippet": "",
                    "confidence": 0.7,
                },
                {
                    "room_key": "powder_room_3",
                    "original_room_label": "Powder Room 3",
                    "bench_tops": ["30mm stone"],
                    "door_panel_colours": [],
                    "toe_kick": [],
                    "bulkheads": [],
                    "handles": [],
                    "drawers_soft_close": "",
                    "hinges_soft_close": "",
                    "splashback": "",
                    "flooring": "",
                    "sink_info": "",
                    "basin_info": "",
                    "tap_info": "Powder tap",
                    "source_file": "sample.pdf",
                    "page_refs": "2",
                    "evidence_snippet": "",
                    "confidence": 0.7,
                },
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
            mock.patch("App.services.extraction_service.parsing.parse_documents", return_value=snapshot),
            mock.patch(
                "App.services.extraction_service.parsing.enrich_snapshot_rooms",
                side_effect=lambda payload, _documents, rule_flags=None: payload,
            ),
        ):
            result = extraction_service.build_spec_snapshot(
                job={"job_no": "39002"},
                builder={"name": "Clarendon"},
                files=[],
                template_files=[],
            )
        rooms = {row["room_key"]: row for row in result["rooms"]}
        self.assertIn("main_bathroom", rooms)
        self.assertIn("powder_room_3", rooms)
        self.assertEqual(rooms["main_bathroom"]["basin_info"], "Bathroom Basin")
        self.assertEqual(rooms["powder_room_3"]["tap_info"], "Powder Tap")

    def test_parse_documents_uses_room_master_file_for_multifile_clarendon(self) -> None:
        snapshot = parse_documents(
            job_no="37868",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "drawings-and-colours.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "KITCHEN COLOUR SCHEDULE\n"
                                "Bench Tops Quantum Zero Bella Carrara - 20MM Pencil Round Edge\n"
                                "VANITIES COLOUR SCHEDULE\n"
                                "Bench Tops Quantum Zero Luna White - 20MM Pencil Round Edge\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
                {
                    "file_name": "colours-afc.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Main Bathroom\n"
                                "Vanity Inset Basin JOHNSON SUISSE Emilia Basin (JBSE250.PW6)\n"
                                "Laundry Door Glazing: CLEAR GLAZING\n"
                                "Vanity Waste Colour: CHROME POP UP\n"
                                "Powder Room 3: TRANSLUCENT LAMINATE P/C Windows & Doors\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
            ],
        )
        rooms = {row["room_key"]: row for row in snapshot["rooms"]}
        analysis = snapshot["analysis"]
        warning_text = " | ".join(snapshot["warnings"])
        self.assertEqual(set(rooms.keys()), {"kitchen", "vanities"})
        self.assertEqual(analysis["room_master_file"], "drawings-and-colours.pdf")
        self.assertIn("drawings-and-colours.pdf selected as room master", analysis["room_master_reason"])
        self.assertEqual(analysis["supplement_files"], ["colours-afc.pdf"])
        self.assertGreaterEqual(analysis["ignored_room_like_lines_count"], 1)
        self.assertTrue(str(rooms["vanities"]["basin_info"]).startswith("Johnson Suisse Emilia"))
        self.assertIn("Ignored room-like section", warning_text)
        self.assertNotIn("Laundry Door", " ".join(rooms.keys()))

    def test_parse_documents_prefers_colour_schedule_file_as_room_master_for_multifile_clarendon(self) -> None:
        snapshot = parse_documents(
            job_no="49906613",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "49906613 Amended Signed Drawings REV C 04-09-25.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "KITCHEN COLOUR SCHEDULE\n"
                                "VANITIES COLOUR SCHEDULE\n"
                                "LAUNDRY COLOUR SCHEDULE\n"
                                "BUTLERS PANTRY COLOUR SCHEDULE\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
                {
                    "file_name": "49906613 COLOURS AFC AMENDED .pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 6,
                            "text": (
                                "VANITIES COLOUR SCHEDULE\n"
                                "Door/Panel Colour - Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile\n"
                                "Door/Panel Colour 2 - Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile\n"
                                "Floor Mounted Vanity - Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile\n"
                                "Back Benchtops Quantum Zero Luna White - 20MM Pencil Round Edge\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
            ],
        )
        analysis = snapshot["analysis"]
        vanities = next(room for room in snapshot["rooms"] if room["room_key"] == "vanities")
        self.assertEqual(analysis["room_master_file"], "49906613 COLOURS AFC AMENDED .pdf")
        self.assertEqual(vanities["source_file"], "49906613 COLOURS AFC AMENDED .pdf")
        self.assertEqual(vanities["page_refs"], "6")

    def test_parse_documents_defaults_grouped_vanities_door_colours_to_base_without_explicit_overheads(self) -> None:
        snapshot = parse_documents(
            job_no="37061",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "vanities-schedule.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "VANITIES COLOUR SCHEDULE\n"
                                "Door/Panel Colour - Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile\n"
                                "Door/Panel Colour 2 - Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile\n"
                                "Benchtop - Quantum Zero Luna White - 20MM Pencil Round Edge / 140MM Mitred Apron Edge - to Powder Room 2\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                }
            ],
        )
        vanities = snapshot["rooms"][0]
        self.assertEqual(vanities["room_key"], "vanities")
        self.assertEqual(vanities["door_colours_overheads"], "")
        self.assertIn("Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile", vanities["door_colours_base"])
        self.assertIn("Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile", vanities["door_colours_base"])

    def test_clarendon_schedule_overlay_defaults_doors_panels_to_base_only_without_explicit_overheads(self) -> None:
        overlay = extraction_service._extract_clarendon_schedule_overlay(
            "vanities",
            (
                "VANITIES COLOUR SCHEDULE "
                "BENCHTOP - QUANTUM ZERO LUNA WHITE - 20MM PENCIL ROUND EDGE / 140MM MITRED APRON EDGE - TO POWDER ROOM 2 "
                "DOORS/PANELS - POL YTEC JAMAICAN OAK MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
                "KICKBOARDS - N/A FLOATING"
            ),
        )
        self.assertFalse(overlay["has_explicit_overheads"])
        self.assertEqual(overlay["door_colours_overheads"], "")
        self.assertEqual(
            overlay["door_colours_base"].lower(),
            "polytec jamaican oak matt finish melamine with matching 1mm abs edges (vertical grain direction)",
        )

    def test_clarendon_vanities_fixture_fallback_does_not_merge_material_fields(self) -> None:
        overlay = extraction_service._select_clarendon_room_overlay(
            {"room_key": "vanities", "original_room_label": "Vanities"},
            {
                "vanities": {
                    "door_colours_base": "Polytec Jamaican Oak Matt Finish Melamine with Matching 1MM ABS Edges (Vertical Grain Direction)",
                    "basin_info": "",
                    "tap_info": "",
                },
                "vanity": {
                    "door_colours_base": "Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile",
                    "basin_info": "Johnson Suisse Emilia Rectangular Undercounter Basin (JBSE250.PW6) White",
                    "tap_info": "Phoenix Nostalgia Basin Mixer NS748-62",
                },
            },
        )
        self.assertEqual(
            overlay["door_colours_base"],
            "Polytec Jamaican Oak Matt Finish Melamine with Matching 1MM ABS Edges (Vertical Grain Direction)",
        )
        self.assertEqual(overlay["basin_info"], "Johnson Suisse Emilia Rectangular Undercounter Basin (JBSE250.PW6) White")
        self.assertEqual(overlay["tap_info"], "Phoenix Nostalgia Basin Mixer NS748-62")

    def test_clarendon_polish_prefers_same_room_vanities_materials_over_contaminated_row_values(self) -> None:
        row = {
            "room_key": "vanities",
            "original_room_label": "VANITIES",
            "bench_tops": ["Quantum Zero Luna White - 20MM Pencil Round Edge / 140MM Mitred Apron Edge - to Powder Room 2"],
            "bench_tops_wall_run": "",
            "bench_tops_island": "",
            "bench_tops_other": "Quantum Zero Luna White - 20MM Pencil Round Edge / 140MM Mitred Apron Edge - to Powder Room 2",
            "door_panel_colours": [
                "Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile",
                "Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile",
            ],
            "door_colours_overheads": "",
            "door_colours_base": "Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile | Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile",
            "door_colours_island": "",
            "door_colours_bar_back": "",
            "has_explicit_overheads": False,
            "has_explicit_base": False,
            "has_explicit_island": False,
            "has_explicit_bar_back": False,
            "toe_kick": ["N/A floating"],
            "bulkheads": [],
            "handles": [],
            "sink_info": "",
            "basin_info": "",
            "tap_info": "",
            "drawers_soft_close": "",
            "hinges_soft_close": "",
            "splashback": "",
            "flooring": "",
            "source_file": "49906613 Amended Signed Drawings REV C 04-09-25.pdf",
            "page_refs": "12",
            "evidence_snippet": "",
            "confidence": 0.6,
        }
        overlay = extraction_service._select_clarendon_room_overlay(
            {"room_key": "vanities", "original_room_label": "VANITIES"},
            {
                "vanities": extraction_service._extract_clarendon_schedule_overlay(
                    "vanities",
                    (
                        "VANITIES COLOUR SCHEDULE "
                        "BENCHTOP - QUANTUM ZERO LUNA WHITE - 20MM PENCIL ROUND EDGE / 140MM MITRED APRON EDGE - TO POWDER ROOM 2 "
                        "DOORS/PANELS - POL YTEC JAMAICAN OAK MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
                        "KICKBOARDS - N/A FLOATING"
                    ),
                ),
                "vanity": {
                    "basin_info": "Johnson Suisse Emilia Rectangular Undercounter Basin (JBSE250.PW6) White",
                    "tap_info": "Phoenix Nostalgia Basin Mixer NS748-62",
                },
            },
        )
        polished = extraction_service._polish_clarendon_room(row, overlay)
        self.assertEqual(
            polished["door_colours_base"],
            "Polytec Jamaican Oak Matt Finish Melamine with Matching 1MM ABS Edges (Vertical Grain Direction)",
        )
        self.assertEqual(polished["door_colours_overheads"], "")
        self.assertEqual(polished["basin_info"], "Johnson Suisse Emilia Rectangular Undercounter Basin (JBSE250.PW6) White")
        self.assertEqual(polished["tap_info"], "Phoenix Nostalgia Basin Mixer NS748-62")

    def test_clarendon_schedule_overlay_recovers_glued_vanities_doors_panels_from_realistic_ocr_text(self) -> None:
        text = (
            "NOTE: ALL PLUMBING SETOUT DIMENSIONS ARE FROM THE TIMBER FRAME\n"
            "REV C 29/07/25\n"
            "BENCHTOP - QUANTUM ZERO LUNA WHITE - 20MM PENCIL ROUND EDGE / 140MM MITRED APRON EDGE - TO POWDER ROOM 2"
            "DOORS/PANELS - POL YTEC JAMAICAN OAK MATT FINISH MELAMINE WITH MATCHING 1MM ABS EDGES (VERTICAL GRAIN DIRECTION) "
            "KICKBOARDS - N/A FLOATING BENCHTOP SHADOWLINE - AS DOOR COLOUR (HORIZONTAL GRAIN DIRECTION)"
            "CARCASS & SHELF EDGES - STANDARD WHITEHANDLES - HETTICH NARNI 9995574 BRUSHED STAINLESS STEEL 37MM LONG - PROUD MOUNTED DOORS "
            "WITH MIN' REVEALS TO DRAWERSDOOR HINGES - HETTICH STANDARD HINGES - NOT SOFT CLOSEDRAWER RUNNERS - HETTICH INNOTECH ATIRA SOFT CLOSE RUNNERS "
            "VANITIES COLOUR SCHEDULE"
        )
        overlay = extraction_service._extract_clarendon_schedule_overlay("vanities", text)
        self.assertEqual(overlay["door_colours_overheads"], "")
        self.assertEqual(
            overlay["door_colours_base"].lower(),
            "polytec jamaican oak matt finish melamine with matching 1mm abs edges (vertical grain direction)",
        )
        self.assertEqual(
            overlay["bench_tops_other"],
            "Quantum Zero Luna White - 20MM Pencil Round Edge / 140MM Mitred Apron Edge - to Powder Room 2",
        )

    def test_material_summary_keeps_distinct_benchtop_thickness_and_edge_variants(self) -> None:
        summary = _build_material_summary(
            {
                "rooms": [
                    {
                        "room_key": "kitchen",
                        "original_room_label": "Kitchen",
                        "bench_tops_wall_run": "Quantum Zero White Swirl - 20MM Pencil Round Edge",
                        "bench_tops_island": "Quantum Zero White Swirl - 40MM Mitred Apron Edge - to Island Bench",
                        "bench_tops_other": "",
                        "door_panel_colours": [],
                    }
                ]
            }
        )
        self.assertEqual(summary["bench_tops"]["count"], 2)
        self.assertIn("Quantum Zero White Swirl - 20MM Pencil Round Edge", summary["bench_tops"]["entries"])
        self.assertIn("Quantum Zero White Swirl - 40MM Mitred Apron Edge", summary["bench_tops"]["entries"])

    def test_parse_documents_recovers_glued_schedule_headings_from_room_master(self) -> None:
        snapshot = parse_documents(
            job_no="37869",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "drawings-and-colours.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "KITCHEN COLOUR SCHEDULEBENCHTOP - QUANTUM ZERO BELLA CARRARA - 20MM PENCIL ROUND EDGE\n"
                                "ROOM BUTLERS PANTRY COLOUR SCHEDULEPANTRY\n"
                                "VANITIES COLOUR SCHEDULENOTE : ALL PLUMBING SETOUT DIMENSIONS ARE FROM THE TIMBER FRAME\n"
                                "LAUNDRY COLOUR SCHEDULEBENCHTOP - POLYTEC ARGENTO STONE - 21MM TIGHTFORM EDGE LAMINATE\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
                {
                    "file_name": "colours-afc.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Study\n"
                                "Hybrid flooring\n"
                                "Main Bathroom\n"
                                "Vanity Inset Basin JOHNSON SUISSE Emilia Basin (JBSE250.PW6)\n"
                                "Laundry door: ALUMINUM SLIDING 2340MM HT\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
            ],
        )
        rooms = {row["room_key"]: row for row in snapshot["rooms"]}
        analysis = snapshot["analysis"]
        warning_text = " | ".join(snapshot["warnings"])
        self.assertEqual(set(rooms.keys()), {"kitchen", "butlers_pantry", "vanities", "laundry"})
        self.assertEqual(analysis["room_master_file"], "drawings-and-colours.pdf")
        self.assertEqual(analysis["supplement_files"], ["colours-afc.pdf"])
        self.assertGreaterEqual(analysis["ignored_room_like_lines_count"], 1)
        self.assertTrue(str(rooms["vanities"]["basin_info"]).startswith("Johnson Suisse Emilia"))
        self.assertIn("Study", warning_text)
        self.assertNotIn("KITCHEN COLOUR SCHEDULEBENCHTOP", " ".join(room["original_room_label"] for room in snapshot["rooms"]))

    def test_parse_documents_prefilters_supplement_rooms_even_when_master_file_is_second(self) -> None:
        snapshot = parse_documents(
            job_no="37736",
            builder_name="Clarendon",
            source_kind="spec",
            documents=[
                {
                    "file_name": "colours-afc.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "Kitchen/Pantry/Family/Meals\n"
                                "Theatre\n"
                                "Rumpus\n"
                                "WIR/S & Robes\n"
                                "Main Bathroom\n"
                                "Vanity Inset Basin JOHNSON SUISSE Emilia Basin (JBSE250.PW6)\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
                {
                    "file_name": "drawings-and-colours.pdf",
                    "role": "spec",
                    "pages": [
                        {
                            "page_no": 1,
                            "text": (
                                "KITCHEN COLOUR SCHEDULEBENCHTOP - QUANTUM ZERO BELLA CARRARA - 20MM PENCIL ROUND EDGE\n"
                                "ROOM BUTLERS PANTRY COLOUR SCHEDULEPANTRY\n"
                                "VANITIES COLOUR SCHEDULENOTE : ALL PLUMBING SETOUT DIMENSIONS ARE FROM THE TIMBER FRAME\n"
                                "LAUNDRY COLOUR SCHEDULEBENCHTOP - POLYTEC ARGENTO STONE - 21MM TIGHTFORM EDGE LAMINATE\n"
                            ),
                            "needs_ocr": False,
                        }
                    ],
                },
            ],
        )
        rooms = {row["room_key"]: row for row in snapshot["rooms"]}
        analysis = snapshot["analysis"]
        warning_text = " | ".join(snapshot["warnings"])
        self.assertEqual(set(rooms.keys()), {"kitchen", "butlers_pantry", "vanities", "laundry"})
        self.assertEqual(analysis["room_master_file"], "drawings-and-colours.pdf")
        self.assertEqual(analysis["supplement_files"], ["colours-afc.pdf"])
        self.assertGreaterEqual(analysis["ignored_room_like_lines_count"], 2)
        self.assertTrue(str(rooms["vanities"]["basin_info"]).startswith("Johnson Suisse Emilia"))
        self.assertEqual(rooms["vanities"]["original_room_label"], "VANITIES")
        self.assertIn("Kitchen/Pantry/Family/Meals", warning_text)
        self.assertIn("Theatre", warning_text)

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
                "room_master_file": "drawings-and-colours.pdf",
                "room_master_reason": "drawings-and-colours.pdf selected as room master by schedule density.",
                "supplement_files": ["colours-afc.pdf"],
                "ignored_room_like_lines_count": 7,
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
        self.assertIn("drawings-and-colours.pdf", response.text)
        self.assertIn("Ignored unmatched room-like lines", response.text)
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

    def test_spec_list_page_hides_non_kitchen_island_bar_back_and_implicit_overheads(self) -> None:
        builder_id = store.create_builder("Clarendon", "clarendon", "")
        job_id = store.create_job("37588", builder_id, "Non Kitchen Door Groups", "")
        store.upsert_snapshot(
            job_id,
            "raw_spec",
            {
                "job_no": "37588",
                "builder_name": "Clarendon",
                "source_kind": "spec",
                "generated_at": "2026-03-23T10:00:00+00:00",
                "analysis": {"mode": "heuristic_only", "openai_attempted": False, "openai_succeeded": False, "openai_model": "gpt-4.1-mini"},
                "rooms": [
                    {
                        "room_key": "vanities",
                        "original_room_label": "VANITIES",
                        "bench_tops": ["Quantum Zero Luna White - 20MM Pencil Round Edge"],
                        "door_panel_colours": [
                            "Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile",
                            "Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile",
                        ],
                        "door_colours_overheads": "Polytec Blossom White Matt Finish Thermolaminate - Hamptons EM9 Profile",
                        "door_colours_base": "Polytec Habitit Smooth Finish Thermolaminate - Hamptons EM9 Profile",
                        "door_colours_island": "",
                        "door_colours_bar_back": "",
                        "has_explicit_overheads": False,
                        "has_explicit_base": False,
                        "has_explicit_island": False,
                        "has_explicit_bar_back": False,
                        "toe_kick": [],
                        "bulkheads": [],
                        "handles": [],
                        "drawers_soft_close": "",
                        "hinges_soft_close": "",
                        "splashback": "",
                        "flooring": "",
                        "source_file": "schedule.pdf",
                        "page_refs": "3",
                        "evidence_snippet": "",
                        "confidence": 0.6,
                    }
                ],
                "appliances": [],
                "others": {},
                "warnings": [],
                "source_documents": [],
            },
        )
        client = TestClient(app)
        self._login(client)
        response = client.get(f"/jobs/{job_id}/spec-list")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("<strong>Island</strong>", response.text)
        self.assertNotIn("<strong>Bar Back</strong>", response.text)
        self.assertNotIn("<strong>Overheads</strong>", response.text)
        self.assertIn("<strong>Base</strong>", response.text)

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
