import json
import logging
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_classes

logger = logging.getLogger(__name__)


FIELD_REF_PATTERN = re.compile(r"([A-Za-z0-9_'\" .-]+)\[([A-Za-z0-9_'\" .-]+)\]")
MEASURE_REF_PATTERN = re.compile(r"\[([A-Za-z0-9_'\" .-]+)\]")
DAX_KEYS = {
    "expression",
    "expr",
    "formula",
    "measure",
    "query",
    "prototypequery",
}


class VisualUpstreamResult(Dict[str, List[str]]):
    """Mapping of visual names to upstream field paths with transform metadata."""

    def __init__(self) -> None:
        super().__init__()
        self._transform_operations: Dict[str, str] = {}

    def set_result(
        self, visual_name: str, columns: Iterable[str], has_dax: bool
    ) -> None:
        unique: List[str] = []
        seen: Set[str] = set()
        for column in columns:
            if column not in seen:
                unique.append(column)
                seen.add(column)
        super().__setitem__(visual_name, unique)
        self._transform_operations[visual_name] = (
            "DAX_OR_QUERY" if has_dax else "DIRECT"
        )

    def get_transform_operation(self, visual_name: str) -> str:
        return self._transform_operations.get(visual_name, "DIRECT")


def locate_report_json(project_root: str) -> str:
    base = Path(project_root)
    if not base.exists():
        raise FileNotFoundError(f"Power BI project path {project_root} does not exist")

    preferred = [base / "Report" / "report.json", base / "Report" / "layout.json"]
    for candidate in preferred:
        if candidate.exists():
            return str(candidate)

    matches: List[Path] = []
    for candidate in base.rglob("Report/report.json"):
        matches.append(candidate)
    if not matches:
        matches.extend(base.rglob("Report/layout.json"))

    if matches:
        return str(matches[0])

    raise FileNotFoundError(
        f"Could not locate Report/report.json or Report/layout.json under {project_root}"
    )


def _clean_identifier(identifier: Optional[str]) -> str:
    if identifier is None:
        return ""
    cleaned = identifier.strip().strip("'\"")
    return cleaned


def canonicalize_identifier(identifier: str) -> str:
    cleaned = _clean_identifier(identifier)
    return re.sub(r"[^0-9A-Za-z]", "", cleaned).lower()


def canonicalize_field_path(field_path: str) -> str:
    parts = [canonicalize_identifier(part) for part in field_path.split(".")]
    return ".".join(part for part in parts if part)


def normalize_query_ref(ref: str) -> str:
    ref = ref.strip()
    match = FIELD_REF_PATTERN.search(ref)
    if match:
        table = _clean_identifier(match.group(1))
        column = _clean_identifier(match.group(2))
        if table:
            return f"{table}.{column}"
        return column

    measure_match = MEASURE_REF_PATTERN.search(ref)
    if measure_match:
        return _clean_identifier(measure_match.group(1))

    return _clean_identifier(ref)


def guess_columns_from_dax(expr: str) -> List[str]:
    columns: List[str] = []
    for match in FIELD_REF_PATTERN.finditer(expr or ""):
        columns.append(normalize_query_ref(match.group(0)))
    for match in MEASURE_REF_PATTERN.finditer(expr or ""):
        columns.append(normalize_query_ref(match.group(0)))
    return columns


def _extract_field_tokens(value: str) -> Set[str]:
    tokens: Set[str] = set()
    for match in FIELD_REF_PATTERN.finditer(value):
        tokens.add(match.group(0))
    for match in MEASURE_REF_PATTERN.finditer(value):
        tokens.add(match.group(0))
    return tokens


def _walk_for_references(node, tokens: Set[str], expressions: List[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            lowered = str(key).lower()
            if isinstance(value, str):
                tokens.update(_extract_field_tokens(value))
                if lowered in DAX_KEYS or "(" in value:
                    expressions.append(value)
            else:
                _walk_for_references(value, tokens, expressions)
    elif isinstance(node, list):
        for item in node:
            _walk_for_references(item, tokens, expressions)
    elif isinstance(node, str):
        tokens.update(_extract_field_tokens(node))


def _ensure_dict(config_value):
    if isinstance(config_value, str):
        try:
            return json.loads(config_value)
        except json.JSONDecodeError:
            logger.debug("Unable to parse visual config JSON: %s", config_value)
            return {}
    if isinstance(config_value, dict):
        return config_value
    return {}


def _visual_name(container: Dict) -> str:
    for key in ("name", "title", "id"):
        if container.get(key):
            return str(container[key])
    return "visual"


def _preferred_visual_name(config: Dict, fallback: str) -> str:
    for key in ("name", "displayName", "visualName"):
        value = config.get(key)
        if isinstance(value, str) and value.strip():
            return value
    single_visual = config.get("singleVisual") or {}
    if isinstance(single_visual, dict):
        for key in ("name", "displayName", "visualName"):
            value = single_visual.get(key)
            if isinstance(value, str) and value.strip():
                return value
    return fallback


def extract_visual_upstreams(
    project_root: str,
    workspace_id: str,
    report_id: str,
    dataset_columns: List[str],
    expand_measures: bool = True,
) -> VisualUpstreamResult:
    report_path = locate_report_json(project_root)

    with open(report_path, "r", encoding="utf-8") as handle:
        report_doc = json.load(handle)

    result = VisualUpstreamResult()
    field_lookup: Dict[str, str] = {}
    for field in dataset_columns:
        canonical = canonicalize_field_path(field)
        if canonical:
            field_lookup.setdefault(canonical, field)
        parts = field.split(".")
        if parts:
            column_canonical = canonicalize_field_path(parts[-1])
            if column_canonical:
                field_lookup.setdefault(column_canonical, field)

    sections = report_doc.get("sections") or []
    for section in sections:
        visuals = section.get("visualContainers") or section.get("visuals") or []
        for container in visuals:
            config = _ensure_dict(container.get("config", {}))
            visual_name = _preferred_visual_name(config, _visual_name(container))
            tokens: Set[str] = set()
            expressions: List[str] = []

            _walk_for_references(container, tokens, expressions)
            if config:
                _walk_for_references(config, tokens, expressions)

            normalized_refs: Set[str] = set()
            for token in tokens:
                normalized_refs.add(normalize_query_ref(token))

            for expr in expressions:
                for ref in guess_columns_from_dax(expr):
                    normalized_refs.add(ref)

            has_dax = bool(expressions)
            mapped_columns: List[str] = []
            for ref in normalized_refs:
                canonical = canonicalize_field_path(ref)
                mapped = field_lookup.get(canonical)
                if not mapped and "." in ref:
                    mapped = field_lookup.get(
                        canonicalize_field_path(ref.split(".")[-1])
                    )
                if mapped:
                    mapped_columns.append(mapped)

            if mapped_columns:
                result.set_result(visual_name, mapped_columns, has_dax)

    return result


def report_urn_for(
    platform: str, platform_instance: Optional[str], report_id: str
) -> str:
    return builder.make_dashboard_urn(
        platform=platform,
        platform_instance=platform_instance,
        name=data_classes.Report.get_urn_part_by_id(report_id),
    )


def dashboard_urn_for(
    platform: str, platform_instance: Optional[str], dashboard_id: str
) -> str:
    return builder.make_dashboard_urn(
        platform=platform,
        platform_instance=platform_instance,
        name=data_classes.Dashboard.get_urn_part_by_id(dashboard_id),
    )


def dataset_urn_for(
    platform: str,
    env: str,
    full_name: str,
    platform_instance: Optional[str] = None,
    lowercase: bool = False,
) -> str:
    urn = builder.make_dataset_urn_with_platform_instance(
        platform=platform,
        name=full_name,
        platform_instance=platform_instance,
        env=env,
    )
    if lowercase:
        return builder.lowercase_dataset_urn(urn)
    return urn
