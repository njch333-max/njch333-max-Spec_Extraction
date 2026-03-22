from __future__ import annotations

from pydantic import BaseModel, Field


class AnalysisMeta(BaseModel):
    mode: str = "heuristic_only"
    parser_strategy: str = "global_conservative"
    openai_attempted: bool = False
    openai_succeeded: bool = False
    openai_model: str = ""
    note: str = ""
    rule_config_updated_at: str = ""
    rule_flags: dict[str, bool] = Field(default_factory=dict)
    worker_pid: int = 0
    app_build_id: str = ""
    room_master_file: str = ""
    room_master_reason: str = ""
    supplement_files: list[str] = Field(default_factory=list)
    ignored_room_like_lines_count: int = 0


class RoomRow(BaseModel):
    room_key: str = ""
    original_room_label: str = ""
    bench_tops: list[str] = Field(default_factory=list)
    bench_tops_wall_run: str = ""
    bench_tops_island: str = ""
    bench_tops_other: str = ""
    door_panel_colours: list[str] = Field(default_factory=list)
    door_colours_overheads: str = ""
    door_colours_base: str = ""
    door_colours_island: str = ""
    door_colours_bar_back: str = ""
    toe_kick: list[str] = Field(default_factory=list)
    bulkheads: list[str] = Field(default_factory=list)
    handles: list[str] = Field(default_factory=list)
    sink_info: str = ""
    basin_info: str = ""
    tap_info: str = ""
    drawers_soft_close: str = ""
    hinges_soft_close: str = ""
    splashback: str = ""
    flooring: str = ""
    source_file: str = ""
    page_refs: str = ""
    evidence_snippet: str = ""
    confidence: float = 0.0


class ApplianceRow(BaseModel):
    appliance_type: str = ""
    make: str = ""
    model_no: str = ""
    product_url: str = ""
    spec_url: str = ""
    manual_url: str = ""
    website_url: str = ""
    overall_size: str = ""
    source_file: str = ""
    page_refs: str = ""
    evidence_snippet: str = ""
    confidence: float = 0.0


class SnapshotPayload(BaseModel):
    job_no: str
    builder_name: str = ""
    source_kind: str = "spec"
    generated_at: str = ""
    analysis: AnalysisMeta = Field(default_factory=AnalysisMeta)
    rooms: list[RoomRow] = Field(default_factory=list)
    appliances: list[ApplianceRow] = Field(default_factory=list)
    others: dict[str, str] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    source_documents: list[dict[str, str]] = Field(default_factory=list)
