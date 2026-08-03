"""Assemble the ``cortex_meta`` report blob — Cortex measuring Cortex.

This is the data source behind the ``cortex_showcase`` deck: a deck *about* Cortex
that is generated from Cortex's own real artifacts, through the same enrich → plan →
render path as every customer deck. Nothing here fabricates a number — the static
facts are read from repo config/registries, and the live volume counts are pulled
from the same builders the real decks use, each guarded so a missing credential
surfaces as an explicit ``unavailable`` rather than a made-up value (fail-loud).

Blob shape::

    {
      "generated_at": ISO8601,
      "graph_breadth":   {data_elements, aliases_terms, source_systems, ...},
      "output_surface":  {slide_builders, slide_builder_modules, portfolio_deck_types},
      "governance_assets": {config_yaml_files, governance_docs},
      "export_economics": [{artifact, bytes, tokens, pct_of_budget, sections}, ...],
      "live_volume":     {salesforce, jira_engineering, github, cursor}   # only when live
    }
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

logger = logging.getLogger("cortex")

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_DIR = _REPO_ROOT / "config"

_DEFAULT_EXPORT_GLOBS = (
    "~/Downloads/LLM-Context-All_Customers*.md",
    "~/Downloads/Pendo Detailed Export*.md",
)


# ── Static facts (repo config + in-process registries; no network) ──────────────

def _catalog_facts() -> dict[str, Any]:
    path = _CONFIG_DIR / "comprehensive_data_element_list.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    entries = data.get("entries") or []
    terms = sum(len(e.get("terms") or []) for e in entries)
    top: dict[str, int] = {}
    for e in entries:
        seg = str(e.get("path") or "").split(".")[0].split("[")[0] or "(root)"
        top[seg] = top.get(seg, 0) + 1
    return {
        "data_elements": len(entries),
        "aliases_terms": terms,
        "distinct_top_level_paths": len(top),
        "catalog_version": data.get("version"),
    }


def _source_facts() -> dict[str, Any]:
    reg = yaml.safe_load((_CONFIG_DIR / "data_source_registry.yaml").read_text(encoding="utf-8"))
    sources = reg.get("sources") or {}
    by_authority: dict[str, list[str]] = {}
    endpoints = 0
    for name, meta in sources.items():
        by_authority.setdefault(str(meta.get("authority")), []).append(name)
        endpoints += len(meta.get("endpoints") or [])
    key_map = reg.get("report_key_to_source_id") or {}
    blobs_per_source: dict[str, int] = {}
    for src in key_map.values():
        blobs_per_source[src] = blobs_per_source.get(src, 0) + 1
    return {
        "source_systems": len(sources),
        "system_of_record": by_authority.get("system_of_record", []),
        "enrichment_sources": sorted(by_authority.get("enrichment", [])),
        "documented_api_endpoints": endpoints,
        "report_blobs_mapped": len(key_map),
        "report_blobs_per_source": dict(sorted(blobs_per_source.items(), key=lambda kv: -kv[1])),
    }


def _output_surface_facts() -> dict[str, Any]:
    from .slide_registry import _SLIDE_BUILDERS, _SLIDE_BUILDER_SPECS
    from .deck_orchestrator import _PORTFOLIO_DRIVE_TITLE_TAIL

    modules = {spec[0] for spec in _SLIDE_BUILDER_SPECS.values()}
    return {
        "slide_builders": len(_SLIDE_BUILDERS),
        "slide_builder_modules": len(modules),
        "portfolio_deck_types": sorted(_PORTFOLIO_DRIVE_TITLE_TAIL),
    }


def _config_asset_facts() -> dict[str, Any]:
    yamls = list(_CONFIG_DIR.rglob("*.yaml")) + list(_CONFIG_DIR.rglob("*.yml"))
    gov_dir = _REPO_ROOT / "docs" / "DATA-GOVERNANCE"
    governance_docs = list(gov_dir.glob("*.md")) if gov_dir.exists() else []
    return {
        "config_yaml_files": len(yamls),
        "governance_docs": len(governance_docs),
    }


def _export_economics(globs: tuple[str, ...] | list[str]) -> list[dict[str, Any]]:
    """Token-count local export artifacts. Empty when none of the globs match."""
    from .export_llm_context_snapshot import count_tokens, _LLM_EXPORT_DEFAULT_MAX_TOKENS

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for g in globs:
        for match in glob.glob(os.path.expanduser(g)):
            if match in seen or not os.path.isfile(match):
                continue
            seen.add(match)
            text = Path(match).read_text(encoding="utf-8", errors="replace")
            tok = count_tokens(text)
            out.append({
                "artifact": os.path.basename(match),
                "bytes": len(text.encode("utf-8")),
                "tokens": tok,
                "pct_of_budget": round(100.0 * tok / _LLM_EXPORT_DEFAULT_MAX_TOKENS, 1),
                "sections": len(re.findall(r"(?m)^##\s", text)),
                "token_budget": _LLM_EXPORT_DEFAULT_MAX_TOKENS,
            })
    return out


def token_budget() -> int:
    from .export_llm_context_snapshot import _LLM_EXPORT_DEFAULT_MAX_TOKENS

    return _LLM_EXPORT_DEFAULT_MAX_TOKENS


# ── Live volume counts (reuse the real builders; each source guarded) ───────────

def _guard(fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    """Run a live-count helper; capture any failure as an explicit ``unavailable``.

    We never fabricate a count: a missing credential or API error becomes a visible
    ``{"unavailable": <reason>}`` so the blob stays honest (fail-loud discipline).
    """
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - meta collector: surface any failure verbatim
        return {"unavailable": str(e)[:200]}


def _salesforce_counts() -> dict[str, Any]:
    from .salesforce_client import SalesforceClient

    return {"portfolio_customers": len(SalesforceClient().get_entity_accounts())}


def _jira_engineering_counts(days: int) -> dict[str, Any]:
    from .jira_client import JiraClient

    ep = JiraClient().get_engineering_portfolio(days=days)
    return {
        "in_flight_tickets": ep.get("in_flight_count"),
        "closed_tickets_window": ep.get("closed_count"),
        "open_bugs": len(ep.get("open_bugs") or []),
        "blockers_criticals": len(ep.get("blocker_critical") or []),
        "contributors": len(ep.get("by_assignee") or {}),
        "themes": len(ep.get("themes") or []),
        "window_days": ep.get("days"),
    }


def _github_counts(days: int) -> dict[str, Any]:
    from .github_productivity_report import build_github_productivity_report

    gp = build_github_productivity_report(window_days=days)
    if not gp or not gp.get("configured"):
        return {"unavailable": "GitHub not configured (GITHUB_TOKEN/GITHUB_ORG/GITHUB_REPOS)"}
    company = gp.get("company_all") or {}
    return {
        "repos": len(gp.get("repos") or []),
        "commits_window": company.get("commits"),
        "merged_prs_window": company.get("merged_prs"),
        "lines_added_window": company.get("lines_added"),
        "contributors": len(gp.get("top_contributors") or []),
        "window_days": gp.get("window_days"),
    }


def _cursor_counts(days: int) -> dict[str, Any]:
    from .cursor_usage_report import build_cursor_usage_report

    cu = build_cursor_usage_report(window_days=days)
    if not cu.get("configured"):
        return {"unavailable": "; ".join(cu.get("errors") or ["Cursor not configured"])}
    members = cu.get("members") or {}
    totals = cu.get("totals") or {}
    spend_cents = totals.get("spend_cents_cycle")
    return {
        "seats": members.get("total"),
        "active_users_window": members.get("active_window"),
        "total_tokens_window": totals.get("total_tokens"),
        "spend_usd_cycle": round(spend_cents / 100, 2) if spend_cents is not None else None,
        "window_days": cu.get("window_days"),
    }


def _live_volume_facts(days: int) -> dict[str, Any]:
    """Live counts that turn the architecture story concrete. Each source is guarded."""
    return {
        "window_days": days,
        "salesforce": _guard(_salesforce_counts),
        "jira_engineering": _guard(lambda: _jira_engineering_counts(days)),
        "github": _guard(lambda: _github_counts(days)),
        "cursor": _guard(lambda: _cursor_counts(days)),
    }


# ── Public entry point ──────────────────────────────────────────────────────────

def build_cortex_meta_report(
    *,
    days: int = 30,
    export_globs: tuple[str, ...] | list[str] | None = None,
    live: bool = False,
) -> dict[str, Any]:
    """Assemble the full ``cortex_meta`` blob. ``live=True`` adds volume counts."""
    meta: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "graph_breadth": {**_catalog_facts(), **_source_facts()},
        "output_surface": _output_surface_facts(),
        "governance_assets": _config_asset_facts(),
        "export_economics": _export_economics(
            export_globs if export_globs is not None else _DEFAULT_EXPORT_GLOBS
        ),
        "token_budget": token_budget(),
    }
    if live:
        meta["live_volume"] = _live_volume_facts(days)
    return meta
