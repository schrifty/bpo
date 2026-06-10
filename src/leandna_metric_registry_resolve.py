"""Resolve LeanDNA metric catalog ids for ``config/my-metrics.yaml`` upserts (lookup only)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.leandna_metrics_catalog import (
    MetricsCatalogError,
    authorized_site_ids_from_identity_body,
    fetch_data_api_identity,
    resolve_effective_requested_sites,
)
from src.leandna_metrics_client import metric_definition_label

# Portfolio metrics in ``config/my-metrics.yaml`` are owned on this LeanDNA site.
METRICS_REGISTRY_DEFAULT_SITE_ID = 416


class MetricRegistryResolveError(Exception):
    """Could not resolve a LeanDNA metric catalog id."""


@dataclass(frozen=True)
class MetricIdResolution:
    metric_id: int
    source: str  # registry | catalog
    detail: str | None = None


def _missing_metric_hint(metric_name: str, *, site_id: int) -> str:
    return (
        f"metric {metric_name!r} is not in GET /data/Metric (siteId={site_id}). "
        "Create the metric in the LeanDNA app UI, set metric-id in config/my-metrics.yaml "
        f"(or run metrics-get-mine --requested-sites {site_id} after creation), "
        "then re-run metrics-upsert."
    )


def _registry_metric_id(entry: dict[str, Any]) -> int | None:
    mid_raw = entry.get("metric-id")
    if mid_raw is None or str(mid_raw).strip() == "":
        return None
    return int(mid_raw)


def _find_metrics_by_exact_name(
    catalog: list[dict[str, Any]],
    metric_name: str,
    *,
    site_id: int | None,
) -> list[dict[str, Any]]:
    want = metric_name.strip().casefold()
    if not want:
        return []
    out: list[dict[str, Any]] = []
    for row in catalog:
        if not isinstance(row, dict):
            continue
        label = metric_definition_label(row).casefold()
        if label != want:
            continue
        if site_id is not None and row.get("siteId") is not None:
            try:
                if int(row["siteId"]) != int(site_id):
                    continue
            except (TypeError, ValueError):
                if str(row.get("siteId")) != str(site_id):
                    continue
        out.append(row)
    return out


def _resolve_site_id(*, requested_sites: str | None, identity_body: dict[str, Any]) -> int | None:
    if requested_sites is not None and str(requested_sites).strip():
        try:
            return int(str(requested_sites).strip())
        except ValueError:
            return None
    site_ids = authorized_site_ids_from_identity_body(identity_body)
    if len(site_ids) == 1:
        return site_ids[0]

    preferred: list[int] = []
    env_raw = (os.environ.get("BPO_LEANDNA_METRICS_SITE_ID") or "").strip()
    if env_raw:
        try:
            preferred.append(int(env_raw))
        except ValueError:
            pass
    preferred.append(METRICS_REGISTRY_DEFAULT_SITE_ID)

    for candidate in preferred:
        if candidate in site_ids:
            return candidate
    return None


def _site_id_resolve_error(identity_body: dict[str, Any]) -> str:
    site_ids = authorized_site_ids_from_identity_body(identity_body)
    ids_s = ", ".join(str(s) for s in site_ids[:12]) or "none"
    if len(site_ids) > 12:
        ids_s += ", …"
    return (
        "missing metric-id and could not infer siteId from identity "
        f"({len(site_ids)} authorized site(s): {ids_s}). "
        f"Pass --requested-sites (portfolio default is {METRICS_REGISTRY_DEFAULT_SITE_ID}) "
        "or set BPO_LEANDNA_METRICS_SITE_ID."
    )


def update_registry_metric_id_in_file(
    metric_name: str,
    metric_id: int,
    *,
    path: Path,
) -> bool:
    """Replace ``metric-id: null`` (or an existing id) for one registry key."""
    text = path.read_text(encoding="utf-8")
    pattern = rf'(  "{re.escape(metric_name)}":\s*\n\s*metric-id:\s*)(?:null|\d+)'
    new_text, count = re.subn(pattern, rf"\g<1>{int(metric_id)}", text, count=1)
    if count != 1:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


def resolve_registry_metric_id(
    metric_name: str,
    entry: dict[str, Any],
    *,
    requested_sites: str | None,
    dry_run: bool,
    timeout_seconds: float,
    registry_path: Path | None = None,
) -> MetricIdResolution:
    """Return a catalog id from YAML or owned ``GET /data/Metric`` lookup."""
    existing = _registry_metric_id(entry)
    if existing is not None:
        return MetricIdResolution(metric_id=existing, source="registry")

    try:
        identity = fetch_data_api_identity(
            requested_sites=requested_sites,
            timeout_seconds=min(timeout_seconds, 60.0),
        )
    except MetricsCatalogError as e:
        raise MetricRegistryResolveError(f"GET /data/identity failed: {e}") from e

    effective_sites = resolve_effective_requested_sites(
        requested_sites,
        identity_body=identity.body,
    )
    site_id = _resolve_site_id(requested_sites=effective_sites, identity_body=identity.body)
    if site_id is None:
        raise MetricRegistryResolveError(_site_id_resolve_error(identity.body))
    if effective_sites is None:
        effective_sites = str(site_id)

    from src.leandna_metrics_client import list_metric_definitions

    try:
        catalog = list_metric_definitions(
            requested_sites=effective_sites,
            timeout_seconds=timeout_seconds,
            extra_query=None,
        )
    except Exception as e:
        raise MetricRegistryResolveError(f"GET /data/Metric failed while resolving metric-id: {e}") from e

    owned = [
        row
        for row in catalog
        if isinstance(row, dict) and str(row.get("ownerId") or "").strip() == identity.user_id
    ]
    matches = _find_metrics_by_exact_name(owned, metric_name, site_id=site_id)
    if len(matches) > 1:
        ids = ", ".join(str(m.get("id")) for m in matches[:5])
        raise MetricRegistryResolveError(
            f"multiple owned metrics named {metric_name!r} (ids: {ids}) — set metric-id in config/my-metrics.yaml"
        )
    if len(matches) == 1:
        mid = int(matches[0]["id"])
        if registry_path is not None and not dry_run:
            update_registry_metric_id_in_file(metric_name, mid, path=registry_path)
        return MetricIdResolution(
            metric_id=mid,
            source="catalog",
            detail="found existing owned metric in GET /data/Metric",
        )

    raise MetricRegistryResolveError(_missing_metric_hint(metric_name, site_id=site_id))
