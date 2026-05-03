from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from App import main
from App.services import worker


@pytest.mark.parametrize(
    ("run_kind", "expected_mode"),
    (
        ("spec_evoca_structured", "structured"),
        ("spec_heuristic_only", "heuristic"),
    ),
)
def test_worker_routes_evoca_spec_run_kind_to_parser_mode(run_kind: str, expected_mode: str) -> None:
    snapshot = {
        "analysis": {"parser_strategy": "evoca_structured_v0" if expected_mode == "structured" else "global_conservative"},
        "rooms": [],
        "appliances": [],
        "special_sections": [],
        "source_documents": [],
    }
    with (
        mock.patch.object(worker.store, "get_job", return_value={"id": 77, "job_no": "38148", "builder_id": 1}),
        mock.patch.object(worker.store, "get_builder", return_value={"id": 1, "slug": "evoca", "name": "Evoca"}),
        mock.patch.object(worker, "ensure_job_dirs", return_value={"spec_dir": Path("spec"), "drawing_dir": Path("drawing")}),
        mock.patch.object(worker, "ensure_builder_dir", return_value=Path("templates")),
        mock.patch.object(worker.store, "list_job_files", return_value=[{"stored_name": "evoca.pdf"}]),
        mock.patch.object(worker.store, "list_builder_templates", return_value=[]),
        mock.patch.object(worker.store, "update_run_runtime_metadata"),
        mock.patch.object(worker.store, "update_run_progress"),
        mock.patch.object(worker.store, "upsert_snapshot") as upsert_snapshot,
        mock.patch.object(worker.store, "mark_run_succeeded"),
        mock.patch.object(worker.store, "mark_run_failed") as mark_failed,
        mock.patch.object(worker, "build_spec_snapshot", return_value=snapshot) as build_spec,
    ):
        worker.process_run({"id": 101, "job_id": 77, "run_kind": run_kind})

    mark_failed.assert_not_called()
    assert build_spec.call_args.kwargs["evoca_structured_mode"] == expected_mode
    upsert_snapshot.assert_called_once_with(77, "raw_spec", snapshot)


def test_evoca_run_kinds_are_spec_runs_for_ui_helpers() -> None:
    assert main.SPEC_RUN_KINDS == {"spec", "spec_evoca_structured", "spec_heuristic_only"}
    assert main._run_file_role("spec_evoca_structured") == "spec"
    assert main._run_file_role("spec_heuristic_only") == "spec"

    rows = main._present_runs(
        [
            {
                "id": 1,
                "job_id": 77,
                "run_kind": "spec_evoca_structured",
                "status": "succeeded",
                "result_json": "{}",
                "parser_strategy": "evoca_structured_v0",
            },
            {
                "id": 2,
                "job_id": 77,
                "run_kind": "spec_heuristic_only",
                "status": "succeeded",
                "result_json": "{}",
                "parser_strategy": "global_conservative",
            },
        ]
    )

    assert rows[0]["kind_label"] == "Evoca Structured"
    assert rows[0]["can_open_result"] is True
    assert rows[1]["kind_label"] == "Heuristic Only"
    assert rows[1]["can_open_result"] is True

    assert main._latest_completed_spec_run(rows)["id"] == 1
