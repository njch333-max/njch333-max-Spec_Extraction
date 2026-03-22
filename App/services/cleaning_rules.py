from __future__ import annotations

import json
from collections import OrderedDict
from typing import Any

GLOBAL_PARSER_STRATEGY = "global_conservative"
GLOBAL_PARSER_STRATEGY_LABEL = "Global Conservative"


RULE_DEFINITIONS = [
    {
        "key": "normalize_brand_casing",
        "label": "Normalize brand casing",
        "description": "Standardize brand names such as Polytec, AEG, and Fisher & Paykel.",
        "group": "Normalization",
    },
    {
        "key": "preserve_full_benchtop_text",
        "label": "Preserve full benchtop text",
        "description": "Keep thickness, edge, apron, and waterfall wording in benchtop values.",
        "group": "Normalization",
    },
    {
        "key": "kitchen_only_split_benchtops",
        "label": "Kitchen-only split benchtops",
        "description": "Show wall-run and island benchtops only for kitchen rooms.",
        "group": "Normalization",
    },
    {
        "key": "cabinet_only_colour_filter",
        "label": "Cabinet-only colour filter",
        "description": "Exclude paint, Colorbond, frame, and external-finish colours from joinery output.",
        "group": "Normalization",
    },
    {
        "key": "strict_drawer_soft_close_inference",
        "label": "Strict drawer soft-close inference",
        "description": "Infer drawer soft-close only from drawer-local wording.",
        "group": "Inference",
    },
    {
        "key": "strict_hinge_soft_close_inference",
        "label": "Strict hinge soft-close inference",
        "description": "Infer hinge soft-close only from hinge-local wording.",
        "group": "Inference",
    },
    {
        "key": "official_product_lookup",
        "label": "Official product lookup",
        "description": "Resolve appliance product pages from official brand domains only.",
        "group": "Appliances",
    },
    {
        "key": "official_overall_size_lookup",
        "label": "Official overall-size lookup",
        "description": "Populate overall size only from official product pages or official PDFs.",
        "group": "Appliances",
    },
    {
        "key": "heuristic_first_room_layout",
        "label": "Heuristic-first room layout",
        "description": "Keep heuristic room grouping as the primary structure when AI output varies.",
        "group": "Layout",
    },
    {
        "key": "door_colour_dedupe_cleanup",
        "label": "Door-colour dedupe cleanup",
        "description": "Deduplicate overlapping door-colour groups while keeping the more specific wording.",
        "group": "Layout",
    },
]

PARSER_STRATEGIES = [
    {
        "key": GLOBAL_PARSER_STRATEGY,
        "label": GLOBAL_PARSER_STRATEGY_LABEL,
        "description": "Use the fixed 37016-style conservative merge profile for all builders.",
    },
    {
        "key": "stable_hybrid",
        "label": "Stable Hybrid",
        "description": "Keep heuristic room structure and cleaning primary; let AI fill missing fields only.",
    },
    {
        "key": "ai_hybrid",
        "label": "AI Hybrid",
        "description": "Allow OpenAI to participate more broadly in the merged result.",
    },
    {
        "key": "heuristic_only",
        "label": "Heuristic Only",
        "description": "Skip OpenAI merge and use heuristic parsing only.",
    },
]

DEFAULT_RULE_FLAGS = OrderedDict((item["key"], True) for item in RULE_DEFINITIONS)


def default_rule_flags() -> dict[str, bool]:
    return dict(DEFAULT_RULE_FLAGS)


def global_rule_flags() -> dict[str, bool]:
    return default_rule_flags()


def global_parser_strategy() -> str:
    return GLOBAL_PARSER_STRATEGY


def default_parser_strategy(builder_name: str = "", builder_slug: str = "") -> str:
    return GLOBAL_PARSER_STRATEGY


def normalize_parser_strategy(value: Any, builder_name: str = "", builder_slug: str = "") -> str:
    parsed = str(value or "").strip().lower().replace("-", "_")
    valid = {item["key"] for item in PARSER_STRATEGIES}
    if parsed in valid:
        return parsed
    return default_parser_strategy(builder_name=builder_name, builder_slug=builder_slug)


def parser_strategy_options(selected: Any, builder_name: str = "", builder_slug: str = "") -> list[dict[str, str]]:
    current = normalize_parser_strategy(selected, builder_name=builder_name, builder_slug=builder_slug)
    return [
        {
            "key": item["key"],
            "label": item["label"],
            "description": item["description"],
            "selected": item["key"] == current,
        }
        for item in PARSER_STRATEGIES
    ]


def parser_strategy_label(value: Any, builder_name: str = "", builder_slug: str = "") -> str:
    key = normalize_parser_strategy(value, builder_name=builder_name, builder_slug=builder_slug)
    for item in PARSER_STRATEGIES:
        if item["key"] == key:
            return item["label"]
    return key.replace("_", " ").title()


def normalize_rule_flags(value: Any) -> dict[str, bool]:
    parsed: dict[str, Any] = {}
    if isinstance(value, str):
        text = value.strip()
        if text:
            try:
                loaded = json.loads(text)
            except json.JSONDecodeError:
                loaded = {}
            if isinstance(loaded, dict):
                parsed = loaded
    elif isinstance(value, dict):
        parsed = dict(value)

    normalized = default_rule_flags()
    for key in normalized:
        if key in parsed:
            normalized[key] = bool(parsed[key])
    return normalized


def serialize_rule_flags(value: Any) -> str:
    return json.dumps(normalize_rule_flags(value), ensure_ascii=False)


def rule_sections(rule_flags: Any) -> list[dict[str, Any]]:
    flags = normalize_rule_flags(rule_flags)
    sections: dict[str, list[dict[str, Any]]] = OrderedDict()
    for definition in RULE_DEFINITIONS:
        sections.setdefault(definition["group"], []).append(
            {
                "key": definition["key"],
                "label": definition["label"],
                "description": definition["description"],
                "enabled": flags.get(definition["key"], True),
            }
        )
    return [{"group": group, "rules": rules} for group, rules in sections.items()]


def enabled_rule_labels(rule_flags: Any) -> list[str]:
    flags = normalize_rule_flags(rule_flags)
    return [item["label"] for item in RULE_DEFINITIONS if flags.get(item["key"], True)]


def rule_enabled(rule_flags: Any, key: str) -> bool:
    return normalize_rule_flags(rule_flags).get(key, True)
