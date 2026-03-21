from __future__ import annotations

from pydantic import BaseModel, Field


class RoomRow(BaseModel):
    room_key: str = ""
    original_room_label: str = ""
    bench_tops: list[str] = Field(default_factory=list)
    door_panel_colours: list[str] = Field(default_factory=list)
    toe_kick: list[str] = Field(default_factory=list)
    bulkheads: list[str] = Field(default_factory=list)
    handles: list[str] = Field(default_factory=list)
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
    rooms: list[RoomRow] = Field(default_factory=list)
    appliances: list[ApplianceRow] = Field(default_factory=list)
    others: dict[str, str] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    source_documents: list[dict[str, str]] = Field(default_factory=list)
