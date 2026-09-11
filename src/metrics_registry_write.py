"""Add, edit, and delete KPI rows in ``config/my-metrics.yaml``.

Edits are surgical: only the target metric block is rewritten so file-level
comments and unrelated rows stay intact. This module does not generate values
or talk to LeanDNA / SQLite.
"""

from __future__ import annotations

import json
import re
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .config_paths import METRICS_FILE
from .metrics_registry import (
    VALID_METRIC_DIRECTIONS,
    VALID_METRIC_UNITS,
    get_registry_metric,
    load_metrics_registry,
    normalize_tag,
    registry_metric_tags,
    validate_metric_target_direction,
)

UNSET: Any = object()

_METRIC_KEY_RE = re.compile(
    r'^  (?P<quoted>(?:"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')):\s*(?:#.*)?$'
)
_METRICS_HEADER_RE = re.compile(r"^metrics:\s*(?:#.*)?$")


class MetricsRegistryWriteError(ValueError):
    """Invalid catalog edit (missing KPI, bad field, or malformed YAML)."""


@dataclass(frozen=True)
class MetricSpan:
    name: str
    key_line: int
    start_line: int
    end_line: int


@dataclass(frozen=True)
class KPICatalogChange:
    action: str
    name: str
    previous_name: str | None
    entry: dict[str, Any] | None
    path: Path
    dry_run: bool


def _yaml_load_quoted(quoted: str) -> str:
    loaded = yaml.safe_load(quoted)
    if loaded is None:
        return ""
    return str(loaded)


def _metrics_header_index(lines: list[str]) -> int:
    for i, line in enumerate(lines):
        if _METRICS_HEADER_RE.match(line.rstrip("\n")):
            return i
    raise MetricsRegistryWriteError("YAML file has no top-level metrics: mapping")


def _metric_spans(lines: list[str]) -> list[MetricSpan]:
    header = _metrics_header_index(lines)
    keys: list[tuple[int, str]] = []
    for i in range(header + 1, len(lines)):
        match = _METRIC_KEY_RE.match(lines[i].rstrip("\n"))
        if not match:
            continue
        keys.append((i, _yaml_load_quoted(match.group("quoted"))))
    spans: list[MetricSpan] = []
    for j, (key_i, name) in enumerate(keys):
        next_key = keys[j + 1][0] if j + 1 < len(keys) else len(lines)
        prev_floor = keys[j - 1][0] + 1 if j else header + 1
        start = key_i
        k = key_i - 1
        while k >= prev_floor:
            stripped = lines[k].strip()
            if stripped == "" or stripped.startswith("#"):
                start = k
                k -= 1
                continue
            break
        spans.append(MetricSpan(name=name, key_line=key_i, start_line=start, end_line=next_key))
    return spans


def _span_for_name(lines: list[str], name: str) -> MetricSpan:
    needle = str(name or "").strip()
    for span in _metric_spans(lines):
        if span.name == needle:
            return span
    raise MetricsRegistryWriteError(f"KPI not found in registry: {needle!r}")


def _dump_scalar(value: Any) -> str:
    dumped = yaml.safe_dump(
        value,
        allow_unicode=True,
        default_flow_style=True,
        width=1000,
        sort_keys=False,
    ).strip()
    if dumped.endswith("\n..."):
        dumped = dumped[:-4].strip()
    elif dumped.endswith("..."):
        dumped = dumped[:-3].strip()
    return dumped


def _format_description(text: str | None) -> list[str]:
    if text is None:
        return ["    description: null"]
    cleaned = str(text).strip()
    if not cleaned:
        return ["    description: null"]
    if "\n" in str(text) or len(cleaned) > 80:
        lines = ["    description: >-"]
        for para in str(text).strip().splitlines() or [cleaned]:
            wrapped = textwrap.fill(
                para.strip() or para,
                width=86,
                break_long_words=False,
                break_on_hyphens=False,
            )
            for wrap_line in wrapped.splitlines() or [para]:
                lines.append(f"      {wrap_line}")
        return lines
    return [f"    description: {_dump_scalar(cleaned)}"]


def _format_tags(tags: list[str]) -> str:
    if not tags:
        return "    tags: []"
    inner = ", ".join(tags)
    return f"    tags: [{inner}]"


def _coerce_metric_id(raw: Any) -> int | None:
    if raw is None or raw is UNSET:
        return None
    if isinstance(raw, bool):
        raise MetricsRegistryWriteError("metric-id must be an integer or null")
    if isinstance(raw, int):
        return raw
    text = str(raw).strip()
    if not text or text.lower() == "null":
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise MetricsRegistryWriteError(f"metric-id must be an integer or null, got {raw!r}") from exc


def _coerce_target(raw: Any) -> float | None:
    if raw is None or raw is UNSET:
        return None
    if isinstance(raw, bool):
        raise MetricsRegistryWriteError("target must be a number or null")
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).strip()
    if not text or text.lower() == "null":
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise MetricsRegistryWriteError(f"target must be a number or null, got {raw!r}") from exc


def normalize_tag_list(tags: Any) -> list[str]:
    if tags is None or tags is UNSET:
        return []
    if isinstance(tags, str):
        candidates = [part.strip() for part in tags.split(",")]
    elif isinstance(tags, (list, tuple)):
        candidates = list(tags)
    else:
        raise MetricsRegistryWriteError("tags must be a list or comma-separated string")
    out: list[str] = []
    for item in candidates:
        tag = normalize_tag(item)
        if tag and tag not in out:
            out.append(tag)
    return out


def default_metric_entry() -> dict[str, Any]:
    return {
        "description": None,
        "metric-id": None,
        "metric-generator": None,
        "tags": [],
    }


def public_metric_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Stable JSON/YAML-friendly view of a registry row."""
    out: dict[str, Any] = {
        "description": entry.get("description"),
        "metric-id": entry.get("metric-id"),
        "metric-generator": entry.get("metric-generator"),
        "tags": list(registry_metric_tags(entry)),
    }
    if "unit" in entry:
        out["unit"] = entry.get("unit")
    if "target" in entry:
        out["target"] = entry.get("target")
    if "direction" in entry:
        out["direction"] = entry.get("direction")
    return out


def format_metric_block(name: str, entry: dict[str, Any], *, leading_comment: bool = True) -> str:
    """Render one ``my-metrics.yaml`` metric block (no trailing extra blank)."""
    display = str(name or "").strip()
    if not display:
        raise MetricsRegistryWriteError("KPI name is required")
    lines: list[str] = []
    if leading_comment:
        lines.append(f"  # {display}")
    lines.append(f"  {json.dumps(display, ensure_ascii=False)}:")
    desc = entry.get("description")
    lines.extend(_format_description(desc if isinstance(desc, str) else None if desc is None else str(desc)))
    metric_id = entry.get("metric-id")
    if metric_id is None or metric_id == "":
        lines.append("    metric-id: null")
    else:
        lines.append(f"    metric-id: {_coerce_metric_id(metric_id)}")
    gen = entry.get("metric-generator")
    if gen is None or (isinstance(gen, str) and not gen.strip()):
        lines.append("    metric-generator: null")
    else:
        lines.append(f"    metric-generator: {str(gen).strip()}")
    lines.append(_format_tags(registry_metric_tags(entry) if "tags" in entry else []))
    unit = entry.get("unit")
    if unit is not None and str(unit).strip() != "":
        text = str(unit).strip().lower()
        if text not in VALID_METRIC_UNITS:
            raise MetricsRegistryWriteError(f"unit must be one of {sorted(VALID_METRIC_UNITS)}, got {unit!r}")
        lines.append(f"    unit: {text}")
    if entry.get("target") is not None and not (isinstance(entry.get("target"), str) and not str(entry.get("target")).strip()):
        target = _coerce_target(entry.get("target"))
        if target is not None and target == int(target):
            lines.append(f"    target: {int(target)}")
        elif target is not None:
            lines.append(f"    target: {target}")
    direction = entry.get("direction")
    if direction is not None and str(direction).strip() != "":
        text = str(direction).strip().lower()
        if text not in VALID_METRIC_DIRECTIONS:
            raise MetricsRegistryWriteError(
                f"direction must be one of {sorted(VALID_METRIC_DIRECTIONS)}, got {direction!r}"
            )
        lines.append(f"    direction: {text}")
    return "\n".join(lines) + "\n"


def _validate_entry(entry: dict[str, Any]) -> None:
    err = validate_metric_target_direction(entry)
    if err:
        raise MetricsRegistryWriteError(err)
    unit = entry.get("unit")
    if unit is not None and str(unit).strip() != "":
        text = str(unit).strip().lower()
        if text not in VALID_METRIC_UNITS:
            raise MetricsRegistryWriteError(f"unit must be one of {sorted(VALID_METRIC_UNITS)}, got {unit!r}")
    direction = entry.get("direction")
    if direction is not None and str(direction).strip() != "":
        text = str(direction).strip().lower()
        if text not in VALID_METRIC_DIRECTIONS:
            raise MetricsRegistryWriteError(
                f"direction must be one of {sorted(VALID_METRIC_DIRECTIONS)}, got {direction!r}"
            )


def apply_metric_fields(
    base: dict[str, Any],
    *,
    description: Any = UNSET,
    metric_id: Any = UNSET,
    generator: Any = UNSET,
    tags: Any = UNSET,
    add_tags: Any = UNSET,
    remove_tags: Any = UNSET,
    unit: Any = UNSET,
    target: Any = UNSET,
    direction: Any = UNSET,
    clear_description: bool = False,
    clear_metric_id: bool = False,
    clear_generator: bool = False,
    clear_tags: bool = False,
    clear_unit: bool = False,
    clear_target: bool = False,
    clear_direction: bool = False,
) -> dict[str, Any]:
    """Return a copy of *base* with CLI field updates applied."""
    entry = dict(base)
    if clear_description:
        entry["description"] = None
    elif description is not UNSET:
        text = None if description is None else str(description).strip()
        entry["description"] = text or None
    if clear_metric_id:
        entry["metric-id"] = None
    elif metric_id is not UNSET:
        entry["metric-id"] = _coerce_metric_id(metric_id)
    if clear_generator:
        entry["metric-generator"] = None
    elif generator is not UNSET:
        gen = None if generator is None else str(generator).strip()
        entry["metric-generator"] = gen or None
    current_tags = registry_metric_tags(entry)
    if clear_tags:
        current_tags = []
    elif tags is not UNSET:
        current_tags = normalize_tag_list(tags)
    if add_tags is not UNSET:
        for tag in normalize_tag_list(add_tags):
            if tag not in current_tags:
                current_tags.append(tag)
    if remove_tags is not UNSET:
        drop = set(normalize_tag_list(remove_tags))
        current_tags = [tag for tag in current_tags if tag not in drop]
    entry["tags"] = current_tags
    if clear_unit:
        entry.pop("unit", None)
    elif unit is not UNSET:
        if unit is None or str(unit).strip() == "":
            entry.pop("unit", None)
        else:
            entry["unit"] = str(unit).strip().lower()
    if clear_target:
        entry.pop("target", None)
    elif target is not UNSET:
        coerced = _coerce_target(target)
        if coerced is None:
            entry.pop("target", None)
        else:
            entry["target"] = coerced
    if clear_direction:
        entry.pop("direction", None)
    elif direction is not UNSET:
        if direction is None or str(direction).strip() == "":
            entry.pop("direction", None)
        else:
            entry["direction"] = str(direction).strip().lower()
    _validate_entry(entry)
    return entry


def _read_lines(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    if not text:
        raise MetricsRegistryWriteError(f"Registry file is empty: {path}")
    return text.splitlines(keepends=True)


def _join_and_write(path: Path, lines: list[str], *, dry_run: bool) -> None:
    body = "".join(lines)
    if not body.endswith("\n"):
        body += "\n"
    if dry_run:
        return
    path.write_text(body, encoding="utf-8")


def _append_block(lines: list[str], block: str) -> list[str]:
    out = list(lines)
    while out and out[-1].strip() == "":
        out.pop()
    if out and not out[-1].endswith("\n"):
        out[-1] += "\n"
    if out:
        out.append("\n")
    out.extend(ln if ln.endswith("\n") else ln + "\n" for ln in block.splitlines(keepends=True))
    return out


def add_registry_metric(
    name: str,
    *,
    path: Path | None = None,
    description: Any = UNSET,
    metric_id: Any = UNSET,
    generator: Any = UNSET,
    tags: Any = UNSET,
    unit: Any = UNSET,
    target: Any = UNSET,
    direction: Any = UNSET,
    dry_run: bool = False,
) -> KPICatalogChange:
    dest = Path(path) if path is not None else METRICS_FILE
    display = str(name or "").strip()
    if not display:
        raise MetricsRegistryWriteError("KPI name is required")
    registry = load_metrics_registry(path=dest)
    if not isinstance(registry.get("metrics"), dict):
        raise MetricsRegistryWriteError(f"YAML file has no metrics mapping: {dest}")
    if get_registry_metric(display, registry=registry) is not None:
        raise MetricsRegistryWriteError(f"KPI already exists: {display!r}")
    entry = apply_metric_fields(
        default_metric_entry(),
        description=description,
        metric_id=metric_id,
        generator=generator,
        tags=[] if tags is UNSET else tags,
        unit=unit,
        target=target,
        direction=direction,
    )
    block = format_metric_block(display, entry, leading_comment=True)
    lines = _append_block(_read_lines(dest), block)
    _join_and_write(dest, lines, dry_run=dry_run)
    return KPICatalogChange(
        action="add",
        name=display,
        previous_name=None,
        entry=public_metric_entry(entry),
        path=dest,
        dry_run=dry_run,
    )


def edit_registry_metric(
    name: str,
    *,
    path: Path | None = None,
    new_name: str | None = None,
    description: Any = UNSET,
    metric_id: Any = UNSET,
    generator: Any = UNSET,
    tags: Any = UNSET,
    add_tags: Any = UNSET,
    remove_tags: Any = UNSET,
    unit: Any = UNSET,
    target: Any = UNSET,
    direction: Any = UNSET,
    clear_description: bool = False,
    clear_metric_id: bool = False,
    clear_generator: bool = False,
    clear_tags: bool = False,
    clear_unit: bool = False,
    clear_target: bool = False,
    clear_direction: bool = False,
    dry_run: bool = False,
) -> KPICatalogChange:
    dest = Path(path) if path is not None else METRICS_FILE
    display = str(name or "").strip()
    if not display:
        raise MetricsRegistryWriteError("KPI name is required")
    registry = load_metrics_registry(path=dest)
    found = get_registry_metric(display, registry=registry)
    if found is None:
        raise MetricsRegistryWriteError(f"KPI not found in registry: {display!r}")
    _, current = found
    renamed = str(new_name).strip() if new_name else display
    if not renamed:
        raise MetricsRegistryWriteError("--new-name cannot be empty")
    if renamed != display and get_registry_metric(renamed, registry=registry) is not None:
        raise MetricsRegistryWriteError(f"KPI already exists: {renamed!r}")
    entry = apply_metric_fields(
        current,
        description=description,
        metric_id=metric_id,
        generator=generator,
        tags=tags,
        add_tags=add_tags,
        remove_tags=remove_tags,
        unit=unit,
        target=target,
        direction=direction,
        clear_description=clear_description,
        clear_metric_id=clear_metric_id,
        clear_generator=clear_generator,
        clear_tags=clear_tags,
        clear_unit=clear_unit,
        clear_target=clear_target,
        clear_direction=clear_direction,
    )
    lines = _read_lines(dest)
    span = _span_for_name(lines, display)
    # Keep any comment lines above the key; replace from the key through the body.
    prefix = lines[span.start_line : span.key_line]
    if renamed != display:
        updated_prefix: list[str] = []
        for line in prefix:
            stripped = line.strip()
            if stripped == f"# {display}":
                updated_prefix.append(f"  # {renamed}\n" if line.endswith("\n") else f"  # {renamed}")
            else:
                updated_prefix.append(line)
        prefix = updated_prefix
    block = format_metric_block(renamed, entry, leading_comment=False)
    block_lines = [ln if ln.endswith("\n") else ln + "\n" for ln in block.splitlines(keepends=True)]
    new_lines = lines[: span.start_line] + prefix + block_lines + lines[span.end_line :]
    _join_and_write(dest, new_lines, dry_run=dry_run)
    return KPICatalogChange(
        action="edit",
        name=renamed,
        previous_name=display if renamed != display else None,
        entry=public_metric_entry(entry),
        path=dest,
        dry_run=dry_run,
    )


def delete_registry_metric(
    name: str,
    *,
    path: Path | None = None,
    dry_run: bool = False,
) -> KPICatalogChange:
    dest = Path(path) if path is not None else METRICS_FILE
    display = str(name or "").strip()
    if not display:
        raise MetricsRegistryWriteError("KPI name is required")
    registry = load_metrics_registry(path=dest)
    found = get_registry_metric(display, registry=registry)
    if found is None:
        raise MetricsRegistryWriteError(f"KPI not found in registry: {display!r}")
    _, current = found
    lines = _read_lines(dest)
    span = _span_for_name(lines, display)
    new_lines = lines[: span.start_line] + lines[span.end_line :]
    # Avoid a run of extra blanks at the cut.
    if span.start_line > 0 and new_lines:
        while (
            span.start_line < len(new_lines)
            and span.start_line > 0
            and new_lines[span.start_line - 1].strip() == ""
            and new_lines[span.start_line].strip() == ""
        ):
            del new_lines[span.start_line]
    _join_and_write(dest, new_lines, dry_run=dry_run)
    return KPICatalogChange(
        action="delete",
        name=display,
        previous_name=None,
        entry=public_metric_entry(current),
        path=dest,
        dry_run=dry_run,
    )
