"""Post-process all-customers LLM export portfolio rows (Pendo-derived) before markdown.

Supports:
    - Intersect headline customers / §5 signals with **active** Salesforce Customer Entity
      portfolio labels (Pendo match optional; see ``merge_active_salesforce_customers_for_llm_export``).
    - Drop customers that **matched** Salesforce rollups but are **inactive** (`active` False on
      contract status rollup — churned-style entities only).
    - Explicit name excludes (CLI, env comma-list, optional UTF-8 file of names).

Environment (defaults off unless noted):
    ``CORTEX_LLM_EXPORT_CUSTOMERS_SF_ALLOWLIST`` — ``1``/``true``/``yes``/``on`` to enable SF allowlist.
    ``CORTEX_LLM_EXPORT_EXCLUDE_SF_CHURNED_MATCHED`` — same truthy semantics for churned filter.
    ``CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS`` — comma-separated Pendo customer labels (case-insensitive).
    ``CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS_FILE`` — path to ``.yaml`` (``customers: [..]``) or plain text
    (one name per non-empty, non-``#`` line).

Allowlist requires Salesforce to be configured; otherwise :func:`apply_llm_export_customer_filters`
raises ``RuntimeError`` (fail loud).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

from .config import logger
from .data_source_health import _salesforce_configured
from .llm_export_salesforce_universe import (
    active_sf_allowlist_lower,
    customer_matches_active_sf_label,
)

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def _env_truthy(raw: str | None) -> bool:
    s = (raw or "").strip().lower()
    return s in _TRUTHY


def _load_exclude_file(path: Path) -> list[str]:
    if not path.is_file():
        raise FileNotFoundError(f"exclude customers file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        data = yaml.safe_load(text) or {}
        if isinstance(data, dict):
            items = data.get("customers")
            if isinstance(items, list):
                return [str(x).strip() for x in items if str(x).strip()]
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
        return []
    return [
        line.split("#")[0].strip()
        for line in text.splitlines()
        if line.split("#")[0].strip()
    ]


def _gather_exclude_labels(
    *,
    cli_names: list[str],
    env_csv: str,
    env_file: str,
) -> frozenset[str]:
    lowered: set[str] = set()
    for chunk in cli_names:
        z = chunk.strip().lower()
        if z:
            lowered.add(z)
    for piece in env_csv.replace(";", ",").split(","):
        z = piece.strip().lower()
        if z:
            lowered.add(z)
    fp = (env_file or "").strip()
    if fp:
        for label in _load_exclude_file(Path(fp)):
            z = label.strip().lower()
            if z:
                lowered.add(z)
    return frozenset(lowered)


def _resolve_portfolio_exclude() -> Callable[[str], bool]:
    try:
        from .pendo_client import customer_is_excluded_from_portfolio as _fn
        return _fn
    except ImportError:

        def customer_is_excluded_from_portfolio(_k: str) -> bool:
            logger.warning(
                "llm_export_customer_filter: customer_is_excluded_from_portfolio unavailable — "
                "portfolio denylist skips disabled",
            )
            return False

        return customer_is_excluded_from_portfolio


@dataclass(frozen=True)
class LlmExportCustomerFilterConfig:
    """Resolved once per CLI/env pass."""

    sf_allowlist: bool = False
    exclude_sf_churned_matched: bool = False
    exclude_names_lower: frozenset[str] = field(default_factory=frozenset)

    @staticmethod
    def from_cli_and_env(
        *,
        customers_sf_allowlist: bool,
        customers_exclude_sf_churned: bool,
        exclude_customer: list[str],
    ) -> LlmExportCustomerFilterConfig:
        env_names = _gather_exclude_labels(
            cli_names=list(exclude_customer),
            env_csv=os.environ.get("CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS", ""),
            env_file=os.environ.get("CORTEX_LLM_EXPORT_EXCLUDE_CUSTOMERS_FILE", ""),
        )
        return LlmExportCustomerFilterConfig(
            sf_allowlist=customers_sf_allowlist
            or _env_truthy(os.environ.get("CORTEX_LLM_EXPORT_CUSTOMERS_SF_ALLOWLIST")),
            exclude_sf_churned_matched=customers_exclude_sf_churned
            or _env_truthy(os.environ.get("CORTEX_LLM_EXPORT_EXCLUDE_SF_CHURNED_MATCHED")),
            exclude_names_lower=env_names,
        )

    def any_enabled(self) -> bool:
        return bool(self.sf_allowlist or self.exclude_sf_churned_matched or self.exclude_names_lower)


def _customer_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    raw = report.get("customers")
    return [r for r in raw if isinstance(r, dict) and str(r.get("customer") or "").strip()]


def _pendo_prefix_set(rows: list[dict[str, Any]]) -> frozenset[str]:
    out: set[str] = set()
    for r in rows:
        c = str(r.get("customer") or "").strip()
        if c:
            out.add(c)
    return frozenset(out)


def _inactive_sf_matched_names(sf: dict[str, Any]) -> frozenset[str]:
    out: set[str] = set()
    roll = sf.get("matched_customer_contract_rollups")
    if isinstance(roll, list):
        for row in roll:
            if not isinstance(row, dict):
                continue
            if row.get("active") is not False:
                continue
            name = str(row.get("customer") or "").strip()
            if name:
                out.add(name.lower())
    acct = sf.get("accounts")
    if isinstance(acct, list):
        for row in acct:
            if not isinstance(row, dict):
                continue
            if row.get("active_in_salesforce") is not False:
                continue
            name = str(row.get("Name") or "").strip()
            if name:
                out.add(name.lower())
    return frozenset(out)


def apply_llm_export_customer_filters(
    report: dict[str, Any],
    cfg: LlmExportCustomerFilterConfig,
) -> dict[str, Any]:
    """Shrink ``report[\"customers\"]`` and ``report[\"portfolio_signals\"]`` in place; rebuild §3 rollup.

    Returns a summary dict stashed at ``report[\"_llm_export_customer_filter\"]``.
    """
    if not cfg.any_enabled():
        return {"enabled": False}

    summary: dict[str, Any] = {
        "enabled": True,
        "sf_allowlist": cfg.sf_allowlist,
        "exclude_sf_churned_matched": cfg.exclude_sf_churned_matched,
        "explicit_excludes_loaded": sorted(cfg.exclude_names_lower),
        "before_customer_rows": len(_customer_rows(report)),
        "dropped_sf_allowlist": 0,
        "dropped_exclude_list": 0,
        "dropped_sf_churned_matched": 0,
        "warnings": [],
    }

    rows = _customer_rows(report)
    before_sig = (
        len(report["portfolio_signals"])
        if isinstance(report.get("portfolio_signals"), list)
        else 0
    )

    # 1) Explicit excludes (exact case-insensitive label match vs Pendo `customer`)
    if cfg.exclude_names_lower:
        dropped_n = 0
        kept: list[dict[str, Any]] = []
        xl = cfg.exclude_names_lower
        for r in rows:
            c = str(r.get("customer") or "").strip().lower()
            if c in xl:
                dropped_n += 1
            else:
                kept.append(r)
        rows = kept
        summary["dropped_exclude_list"] = dropped_n

    # 2) SF allowlist
    allowed_lower: frozenset[str] | None = None
    if cfg.sf_allowlist:
        if not _salesforce_configured():
            raise RuntimeError(
                "Cortex LLM export: Salesforce allowlist filtering requested but Salesforce is not configured "
                "(set JWT env vars per docs).Unset CORTEX_LLM_EXPORT_CUSTOMERS_SF_ALLOWLIST / omit "
                "`--customers-sf-allowlist`.",
            )
        active_lower, sf_labels, book = active_sf_allowlist_lower()
        if not active_lower:
            summary["warnings"].append(
                "Salesforce allowlist enabled but no active (non-churned) Customer Entity labels were resolved."
            )
        pendo_prefixes = _pendo_prefix_set(rows)
        summary["salesforce_allowlist_meta"] = {
            "salesforce_active_labels": len(active_lower),
            "salesforce_portfolio_label_count": len(sf_labels),
            "pendo_prefix_count": len(pendo_prefixes),
        }
        before_allow = len(rows)
        rows = [
            r
            for r in rows
            if customer_matches_active_sf_label(
                str(r.get("customer") or ""),
                active_sf_labels_lower=active_lower,
                pendo_prefixes=pendo_prefixes,
                sf_portfolio_labels=sf_labels,
            )
        ]
        summary["dropped_sf_allowlist"] = max(0, before_allow - len(rows))
        allowed_lower = active_lower
        sf_allowlist_labels = sf_labels
        _ = book  # revenue book retained for possible diagnostics
    else:
        sf_allowlist_labels = None

    # 3) Inactive Salesforce matches (explicit churn rollup)
    inactive: frozenset[str] | None = None
    if cfg.exclude_sf_churned_matched:
        sf_blob = report.get("salesforce") if isinstance(report.get("salesforce"), dict) else {}
        if sf_blob.get("error"):
            msg = (
                "exclude_sf_churned_matched requested but Salesforce block has error; "
                "churn-based drop skipped for this run."
            )
            summary["warnings"].append(msg)
            logger.warning("LLM export customer filter: %s", msg)
        else:
            inactive = _inactive_sf_matched_names(sf_blob)

    if inactive is not None:
        before_ch = len(rows)
        rows = [
            r
            for r in rows
            if str(r.get("customer") or "").strip().lower() not in inactive
        ]
        summary["dropped_sf_churned_matched"] = max(0, before_ch - len(rows))

    # Write back headline customers
    report["customers"] = rows
    if isinstance(report.get("portfolio_signals"), list):
        filtered_sig: list[Any] = []
        for item in report["portfolio_signals"]:
            if not isinstance(item, dict):
                filtered_sig.append(item)
                continue
            cust = str(item.get("customer") or "").strip()
            if not cust:
                filtered_sig.append(item)
                continue
            if cfg.sf_allowlist and sf_allowlist_labels is not None:
                if not customer_matches_active_sf_label(
                    cust,
                    active_sf_labels_lower=allowed_lower or frozenset(),
                    pendo_prefixes=_pendo_prefix_set(rows),
                    sf_portfolio_labels=sf_allowlist_labels,
                ):
                    continue
            elif allowed_lower is not None and cust.lower() not in allowed_lower:
                continue
            cust_l = cust.lower()
            if inactive is not None and cust_l in inactive:
                continue
            if cust_l and cust_l in cfg.exclude_names_lower:
                continue
            filtered_sig.append(item)
        report["portfolio_signals"] = filtered_sig

    cc = len(rows)
    if "customer_count" in report:
        report["customer_count"] = cc
    summary["after_customer_rows"] = cc
    summary["portfolio_signals_before"] = before_sig
    summary["portfolio_signals_after"] = (
        len(report["portfolio_signals"]) if isinstance(report.get("portfolio_signals"), list) else 0
    )

    try:
        from src.data_sources.loaders.salesforce_portfolio_aggregate import (
            salesforce_portfolio_aggregate_for_report,
        )

        report["salesforce"] = salesforce_portfolio_aggregate_for_report(report)
    except Exception as e:
        logger.warning(
            "llm_export_customer_filter: Salesforce portfolio aggregate refresh failed after filter: %s",
            e,
        )
        summary["salesforce_refresh_error"] = str(e)[:500]

    report["_llm_export_customer_filter"] = summary
    logger.info(
        "LLM export customer filter: %s → %s rows (signals %s → %s)",
        summary["before_customer_rows"],
        summary["after_customer_rows"],
        summary["portfolio_signals_before"],
        summary["portfolio_signals_after"],
    )
    return summary
