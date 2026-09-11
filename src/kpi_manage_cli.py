"""CLI for internal users to add, edit, delete, and show registry KPIs."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from src.config_paths import METRICS_FILE
from src.metrics_registry import (
    VALID_METRIC_DIRECTIONS,
    VALID_METRIC_UNITS,
    get_registry_metric,
    load_metrics_registry,
)
from src.metrics_registry_write import (
    KPICatalogChange,
    MetricsRegistryWriteError,
    UNSET,
    add_registry_metric,
    delete_registry_metric,
    edit_registry_metric,
    public_metric_entry,
)

_MANAGE_COMMANDS = frozenset({"add", "edit", "update", "delete", "rm", "remove", "show", "get"})


def is_kpi_manage_command(arg: str) -> bool:
    return arg in _MANAGE_COMMANDS


def _change_to_json(change: KPICatalogChange) -> dict[str, Any]:
    return {
        "action": change.action,
        "name": change.name,
        "previous_name": change.previous_name,
        "entry": change.entry,
        "path": str(change.path),
        "dry_run": change.dry_run,
    }


def _print_change(change: KPICatalogChange, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(_change_to_json(change), indent=2, default=str, ensure_ascii=False))
        return
    verb = {"add": "Added", "edit": "Updated", "delete": "Deleted"}[change.action]
    suffix = " (dry run)" if change.dry_run else ""
    if change.action == "edit" and change.previous_name:
        print(f"{verb} KPI {change.previous_name!r} → {change.name!r} in {change.path}{suffix}")
    else:
        print(f"{verb} KPI {change.name!r} in {change.path}{suffix}")
    if change.entry and change.action != "delete":
        print(json.dumps(change.entry, indent=2, default=str, ensure_ascii=False))


def _add_shared_write_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--registry",
        dest="registry_path",
        default=None,
        metavar="PATH",
        help=f"Registry YAML (default: {METRICS_FILE})",
    )
    ap.add_argument("--json", action="store_true", help="Print the result as JSON")
    ap.add_argument("--dry-run", action="store_true", help="Validate and print; do not write the file")


def _add_field_args(ap: argparse.ArgumentParser, *, for_edit: bool) -> None:
    ap.add_argument("--description", default=UNSET, help="Human-readable summary")
    ap.add_argument(
        "--mgmt-guidance",
        dest="mgmt_guidance",
        default=UNSET,
        help="How to manage to this KPI (two sentences max)",
    )
    ap.add_argument("--metric-id", dest="metric_id", default=UNSET, help="LeanDNA catalog id (integer)")
    ap.add_argument("--generator", dest="generator", default=UNSET, help="metric-generator function name")
    ap.add_argument(
        "--tags",
        default=UNSET,
        help="Replace tags (comma-separated, e.g. engineering,ai)",
    )
    ap.add_argument(
        "--tag",
        dest="tag_flags",
        action="append",
        default=None,
        metavar="NAME",
        help="Replace tags (repeatable; combined with --tags)",
    )
    ap.add_argument("--unit", default=UNSET, choices=sorted(VALID_METRIC_UNITS), help="Display unit")
    ap.add_argument("--target", default=UNSET, help="Numeric goal (requires --direction)")
    ap.add_argument(
        "--direction",
        default=UNSET,
        choices=sorted(VALID_METRIC_DIRECTIONS),
        help="higher or lower; required when --target is set",
    )
    if for_edit:
        ap.add_argument("--new-name", default=None, help="Rename this KPI")
        ap.add_argument("--add-tag", dest="add_tags", action="append", default=None, metavar="NAME")
        ap.add_argument("--remove-tag", dest="remove_tags", action="append", default=None, metavar="NAME")
        ap.add_argument("--clear-description", action="store_true")
        ap.add_argument("--clear-mgmt-guidance", action="store_true")
        ap.add_argument("--clear-metric-id", action="store_true")
        ap.add_argument("--clear-generator", action="store_true")
        ap.add_argument("--clear-tags", action="store_true")
        ap.add_argument("--clear-unit", action="store_true")
        ap.add_argument("--clear-target", action="store_true")
        ap.add_argument("--clear-direction", action="store_true")


def _merged_tags(ns: argparse.Namespace) -> Any:
    parts: list[str] = []
    if ns.tags is not UNSET:
        parts.extend(str(ns.tags).split(","))
    extra = getattr(ns, "tag_flags", None) or []
    parts.extend(extra)
    if ns.tags is UNSET and not extra:
        return UNSET
    return parts


def _registry_path(ns: argparse.Namespace) -> Path:
    return Path(ns.registry_path) if ns.registry_path else METRICS_FILE


def _confirm_delete(name: str, *, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        raise MetricsRegistryWriteError("refusing to delete without --yes (stdin is not a TTY)")
    reply = input(f"Delete KPI {name!r} from the registry? [y/N] ").strip().lower()
    return reply in {"y", "yes"}


def _cmd_add(ns: argparse.Namespace) -> int:
    change = add_registry_metric(
        ns.name,
        path=_registry_path(ns),
        description=ns.description,
        mgmt_guidance=ns.mgmt_guidance,
        metric_id=ns.metric_id,
        generator=ns.generator,
        tags=_merged_tags(ns),
        unit=ns.unit,
        target=ns.target,
        direction=ns.direction,
        dry_run=bool(ns.dry_run),
    )
    _print_change(change, as_json=bool(ns.json))
    return 0


def _cmd_edit(ns: argparse.Namespace) -> int:
    change = edit_registry_metric(
        ns.name,
        path=_registry_path(ns),
        new_name=ns.new_name,
        description=ns.description,
        mgmt_guidance=ns.mgmt_guidance,
        metric_id=ns.metric_id,
        generator=ns.generator,
        tags=_merged_tags(ns),
        add_tags=ns.add_tags if ns.add_tags else UNSET,
        remove_tags=ns.remove_tags if ns.remove_tags else UNSET,
        unit=ns.unit,
        target=ns.target,
        direction=ns.direction,
        clear_description=bool(ns.clear_description),
        clear_mgmt_guidance=bool(ns.clear_mgmt_guidance),
        clear_metric_id=bool(ns.clear_metric_id),
        clear_generator=bool(ns.clear_generator),
        clear_tags=bool(ns.clear_tags),
        clear_unit=bool(ns.clear_unit),
        clear_target=bool(ns.clear_target),
        clear_direction=bool(ns.clear_direction),
        dry_run=bool(ns.dry_run),
    )
    _print_change(change, as_json=bool(ns.json))
    return 0


def _cmd_delete(ns: argparse.Namespace) -> int:
    if not _confirm_delete(ns.name, assume_yes=bool(ns.yes)):
        print("Cancelled.", file=sys.stderr)
        return 1
    change = delete_registry_metric(
        ns.name,
        path=_registry_path(ns),
        dry_run=bool(ns.dry_run),
    )
    _print_change(change, as_json=bool(ns.json))
    return 0


def _cmd_show(ns: argparse.Namespace) -> int:
    dest = _registry_path(ns)
    registry = load_metrics_registry(path=dest)
    found = get_registry_metric(ns.name, registry=registry)
    if found is None:
        raise MetricsRegistryWriteError(f"KPI not found in registry: {ns.name!r}")
    name, entry = found
    payload = {"name": name, "entry": public_metric_entry(entry), "path": str(dest)}
    if ns.json:
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
        return 0
    print(f"{name}")
    print(json.dumps(payload["entry"], indent=2, default=str, ensure_ascii=False))
    return 0


def build_parser(*, prog: str = "cortex kpi") -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Add, edit, delete, or show KPI definitions in config/my-metrics.yaml. "
            "Value lookup: cortex kpi --all | cortex kpi TAG ...  Help: cortex --kpi"
        ),
    )
    sub = ap.add_subparsers(dest="manage_command", required=True)

    add_p = sub.add_parser("add", help="Create a KPI in the registry")
    add_p.add_argument("name", help="Display name (exact registry key)")
    _add_field_args(add_p, for_edit=False)
    _add_shared_write_args(add_p)

    edit_p = sub.add_parser("edit", aliases=("update",), help="Update fields on an existing KPI")
    edit_p.add_argument("name", help="Current display name")
    _add_field_args(edit_p, for_edit=True)
    _add_shared_write_args(edit_p)

    del_p = sub.add_parser("delete", aliases=("rm", "remove"), help="Remove a KPI from the registry")
    del_p.add_argument("name", help="Display name to delete")
    del_p.add_argument("-y", "--yes", action="store_true", help="Do not prompt for confirmation")
    _add_shared_write_args(del_p)

    show_p = sub.add_parser("show", aliases=("get",), help="Print one KPI definition")
    show_p.add_argument("name", help="Display name")
    show_p.add_argument(
        "--registry",
        dest="registry_path",
        default=None,
        metavar="PATH",
        help=f"Registry YAML (default: {METRICS_FILE})",
    )
    show_p.add_argument("--json", action="store_true", help="Print the result as JSON")
    return ap


def run_kpi_manage_cli(argv: Sequence[str] | None = None, *, prog: str = "cortex kpi") -> int:
    ap = build_parser(prog=prog)
    ns = ap.parse_args(list(argv) if argv is not None else None)
    command = ns.manage_command
    try:
        if command == "add":
            return _cmd_add(ns)
        if command in ("edit", "update"):
            return _cmd_edit(ns)
        if command in ("delete", "rm", "remove"):
            return _cmd_delete(ns)
        if command in ("show", "get"):
            return _cmd_show(ns)
    except (MetricsRegistryWriteError, FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    ap.error(f"unknown command {command!r}")
    return 2


def main(argv: Sequence[str] | None = None) -> int:
    return run_kpi_manage_cli(argv, prog="metrics-manage")
