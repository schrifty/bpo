"""CLI for internal users to add, edit, delete, show, and list registry KPIs."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from src.config_paths import KPI_OWNERS_FILE, METRICS_FILE
from src.kpi_owners import (
    load_kpi_owners_config,
    resolve_kpi_actor,
    resolve_owner_cli_value,
)
from src.metrics_registry import (
    VALID_METRIC_DIRECTIONS,
    VALID_METRIC_UNITS,
    all_registry_owners,
    get_registry_metric,
    iter_metrics_by_owner,
    load_metrics_registry,
    registry_metric_owner,
    registry_metric_tags,
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

_MANAGE_COMMANDS = frozenset(
    {
        "add",
        "edit",
        "update",
        "delete",
        "rm",
        "remove",
        "show",
        "get",
        "list",
        "ls",
        "owners",
    }
)


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
    ap.add_argument(
        "--owners-config",
        dest="owners_path",
        default=None,
        metavar="PATH",
        help=f"KPI owners YAML (default: {KPI_OWNERS_FILE})",
    )
    ap.add_argument(
        "--as-user",
        dest="actor",
        default=None,
        metavar="EMAIL",
        help="Acting user for ownership checks (default: CORTEX_KPI_ACTOR or catalog_admin)",
    )
    ap.add_argument("--json", action="store_true", help="Print the result as JSON")
    ap.add_argument("--dry-run", action="store_true", help="Validate and print; do not write the file")


def _add_shared_read_args(ap: argparse.ArgumentParser, *, with_actor: bool = True) -> None:
    ap.add_argument(
        "--registry",
        dest="registry_path",
        default=None,
        metavar="PATH",
        help=f"Registry YAML (default: {METRICS_FILE})",
    )
    ap.add_argument(
        "--owners-config",
        dest="owners_path",
        default=None,
        metavar="PATH",
        help=f"KPI owners YAML (default: {KPI_OWNERS_FILE})",
    )
    if with_actor:
        ap.add_argument(
            "--as-user",
            dest="actor",
            default=None,
            metavar="EMAIL",
            help="Acting user for 'me' owner resolution (default: CORTEX_KPI_ACTOR or catalog_admin)",
        )
    ap.add_argument("--json", action="store_true", help="Print the result as JSON")


def _add_field_args(ap: argparse.ArgumentParser, *, for_edit: bool) -> None:
    ap.add_argument("--description", default=UNSET, help="Human-readable summary")
    ap.add_argument(
        "--mgmt-guidance",
        dest="mgmt_guidance",
        default=UNSET,
        help="How to manage to this KPI (two sentences max)",
    )
    ap.add_argument(
        "--owner",
        default=UNSET,
        metavar="EMAIL|me",
        help="Owner email or 'me' (acting user); default on add: acting user",
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
        ap.add_argument(
            "--clear-owner",
            action="store_true",
            help="Clear owner (fails validation — owner is required)",
        )
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


def _owners_path(ns: argparse.Namespace) -> Path | None:
    return Path(ns.owners_path) if getattr(ns, "owners_path", None) else None


def _load_owners_cfg(ns: argparse.Namespace):
    path = _owners_path(ns)
    return load_kpi_owners_config(path=path) if path else load_kpi_owners_config()


def _resolve_list_owner(ns: argparse.Namespace) -> str:
    """Owner filter for list / owners --list-metrics: default and bare --owner → me."""
    cfg = _load_owners_cfg(ns)
    actor = getattr(ns, "actor", None)
    raw = getattr(ns, "owner", None)
    # argparse nargs='?' with const='me': omitted flag → None; bare --owner → 'me'
    resolved = resolve_owner_cli_value(
        raw,
        actor=actor,
        owners=cfg,
        default_to_me=True,
    )
    if not resolved:
        raise MetricsRegistryWriteError("owner filter resolved empty (use --owner EMAIL|me)")
    return resolved


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
        owner=ns.owner,
        metric_id=ns.metric_id,
        generator=ns.generator,
        tags=_merged_tags(ns),
        unit=ns.unit,
        target=ns.target,
        direction=ns.direction,
        actor=ns.actor,
        owners_path=_owners_path(ns),
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
        owner=ns.owner,
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
        clear_owner=bool(ns.clear_owner),
        clear_metric_id=bool(ns.clear_metric_id),
        clear_generator=bool(ns.clear_generator),
        clear_tags=bool(ns.clear_tags),
        clear_unit=bool(ns.clear_unit),
        clear_target=bool(ns.clear_target),
        clear_direction=bool(ns.clear_direction),
        actor=ns.actor,
        owners_path=_owners_path(ns),
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
        actor=ns.actor,
        owners_path=_owners_path(ns),
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


def _print_metrics_for_owner(
    *,
    owner: str,
    registry: dict[str, Any],
    dest: Path,
    as_json: bool,
) -> int:
    metrics = [
        {
            "name": name,
            "tags": registry_metric_tags(entry),
            "owner": registry_metric_owner(entry),
        }
        for name, entry in iter_metrics_by_owner(owner, registry=registry)
    ]
    payload = {"owner": owner, "metrics": metrics, "path": str(dest)}
    if as_json:
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
        return 0 if metrics else 1
    if not metrics:
        print(f"No KPIs owned by {owner!r} in {dest}", file=sys.stderr)
        return 1
    for row in metrics:
        tags = ", ".join(row["tags"]) if row["tags"] else "—"
        print(f"{row['name']}\t{tags}")
    return 0


def _cmd_list(ns: argparse.Namespace) -> int:
    """List KPI definitions for one owner (default: acting user / me)."""
    dest = _registry_path(ns)
    registry = load_metrics_registry(path=dest)
    owner = _resolve_list_owner(ns)
    return _print_metrics_for_owner(
        owner=owner,
        registry=registry,
        dest=dest,
        as_json=bool(ns.json),
    )


def _cmd_owners(ns: argparse.Namespace) -> int:
    """List owners configured in kpi_owners.yaml and counts from the registry."""
    dest = _registry_path(ns)
    cfg = _load_owners_cfg(ns)
    registry = load_metrics_registry(path=dest)
    counts = dict(all_registry_owners(registry=registry))
    rows: list[dict[str, Any]] = []
    rows.append(
        {
            "email": cfg.catalog_admin,
            "role": "catalog_admin",
            "display_name": None,
            "packs": [],
            "kpi_count": counts.get(cfg.catalog_admin, 0),
        }
    )
    for lead in cfg.leads:
        rows.append(
            {
                "email": lead.email,
                "role": "lead",
                "display_name": lead.display_name,
                "packs": list(lead.packs),
                "kpi_count": counts.get(lead.email, 0),
            }
        )
    if ns.list_metrics:
        owner = _resolve_list_owner(ns)
        return _print_metrics_for_owner(
            owner=owner,
            registry=registry,
            dest=dest,
            as_json=bool(ns.json),
        )
    if ns.json:
        print(
            json.dumps(
                {
                    "catalog_admin": cfg.catalog_admin,
                    "owners": rows,
                    "topic_packs": {
                        pid: {
                            "description": pack.description,
                            "required_any_tags": list(pack.required_any_tags),
                        }
                        for pid, pack in cfg.topic_packs.items()
                    },
                    "actor_default": resolve_kpi_actor(getattr(ns, "actor", None), owners=cfg),
                    "path": str(cfg.path),
                    "registry": str(dest),
                },
                indent=2,
                default=str,
                ensure_ascii=False,
            )
        )
        return 0
    print(f"KPI owners ({cfg.path}):")
    for row in rows:
        packs = f" packs={row['packs']}" if row["role"] == "lead" else ""
        print(f"  {row['email']}  [{row['role']}]  kpis={row['kpi_count']}{packs}")
    print("Topic packs:")
    for pid, pack in cfg.topic_packs.items():
        print(f"  {pid}: any of {list(pack.required_any_tags)}")
    print(
        "\nList one owner's KPIs: cortex kpi list [--owner me|EMAIL] "
        "or cortex kpi owners --list-metrics [--owner me|EMAIL]",
        file=sys.stderr,
    )
    return 0


def build_parser(*, prog: str = "cortex kpi") -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog=prog,
        description=(
            "Add, edit, delete, show, or list KPI definitions in config/my-metrics.yaml. "
            "Value lookup: cortex kpi --all | cortex kpi TAG ... | cortex kpi --owner [EMAIL|me]. "
            "Help: cortex --kpi"
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

    show_p = sub.add_parser("show", aliases=("get",), help="Print one KPI definition (read-all)")
    show_p.add_argument("name", help="Display name")
    _add_shared_read_args(show_p, with_actor=False)

    list_p = sub.add_parser(
        "list",
        aliases=("ls",),
        help="List KPI definitions for one owner (default: me / acting user)",
    )
    list_p.add_argument(
        "--owner",
        nargs="?",
        const="me",
        default=None,
        metavar="EMAIL|me",
        help="Owner filter (default and bare --owner: acting user / me)",
    )
    _add_shared_read_args(list_p, with_actor=True)

    owners_p = sub.add_parser("owners", help="List configured KPI owners / list KPIs by owner")
    _add_shared_read_args(owners_p, with_actor=True)
    owners_p.add_argument(
        "--owner",
        nargs="?",
        const="me",
        default=None,
        metavar="EMAIL|me",
        help="With --list-metrics: owner filter (default and bare --owner: me)",
    )
    owners_p.add_argument(
        "--list-metrics",
        action="store_true",
        help="List KPI names owned by --owner (default me; definitions only, no values)",
    )
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
        if command in ("list", "ls"):
            return _cmd_list(ns)
        if command == "owners":
            return _cmd_owners(ns)
    except (MetricsRegistryWriteError, FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    ap.error(f"unknown command {command!r}")
    return 2


def main(argv: Sequence[str] | None = None) -> int:
    return run_kpi_manage_cli(argv, prog="metrics-manage")
