"""Cross-system customer name resolution (Salesforce → Pendo, CS Report, JSM)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config_paths import (
    CS_REPORT_CUSTOMER_ALIASES_FILE,
    JSM_ORGANIZATION_ALIASES_FILE,
    SF_PORTFOLIO_PENDO_ALIASES_FILE,
)


@dataclass
class CompanyMatchRow:
    """One Salesforce portfolio label with resolved names in other systems."""

    salesforce_label: str
    status: str
    contract_statuses: list[str] = field(default_factory=list)
    arr: float | None = None
    pendo_name: str | None = None
    csr_names: list[str] = field(default_factory=list)
    jsm_names: list[str] = field(default_factory=list)
    alias_notes: list[str] = field(default_factory=list)


def _pendo_prefixes(*, days: int) -> frozenset[str]:
    from .pendo_client import PendoClient, customer_is_excluded_from_portfolio

    by_customer = PendoClient().get_sites_by_customer(days=days)
    return frozenset(
        c
        for c in (by_customer.get("customer_list") or [])
        if c and c != "(unknown)" and not customer_is_excluded_from_portfolio(c)
    )


def _csr_customer_names() -> frozenset[str]:
    from .cs_report_client import _fetch_latest_report

    rows = _fetch_latest_report()
    return frozenset(
        (r.get("customer") or "").strip()
        for r in rows
        if (r.get("customer") or "").strip()
    )


def _jsm_organization_names() -> list[str]:
    from .jira_client import get_shared_jira_client

    return get_shared_jira_client()._list_jsm_organization_names()


def diagnose_jsm_org_directory() -> list[str]:
    """Explain empty JSM org directory (HTTP 200 with size=0 vs scope errors)."""
    import requests

    lines: list[str] = []
    try:
        from .jira_client import get_shared_jira_client

        jc = get_shared_jira_client()
    except Exception as e:
        return [f"Jira not available: {e}"]

    org_url = f"{jc.api_base_url}/rest/servicedeskapi/organization?start=0&limit=5"
    try:
        resp = requests.get(org_url, headers=jc._headers, timeout=30)
        if resp.ok:
            data = resp.json()
            size = data.get("size", len(data.get("values") or []))
            lines.append(
                f"GET /rest/servicedeskapi/organization → HTTP 200, size={size} "
                f"(empty list means the token is not seeing orgs as a JSM agent)"
            )
        else:
            lines.append(
                f"GET /rest/servicedeskapi/organization → HTTP {resp.status_code}: "
                f"{(resp.text or '')[:120]}"
            )
    except Exception as e:
        lines.append(f"GET /rest/servicedeskapi/organization failed: {e}")

    desk_url = f"{jc.api_base_url}/rest/servicedeskapi/servicedesk?limit=5"
    try:
        resp = requests.get(desk_url, headers=jc._headers, timeout=30)
        if resp.status_code == 401:
            lines.append(
                "GET /rest/servicedeskapi/servicedesk → HTTP 401 (scope does not match). "
                "Add JSM scopes: View Jira Service Desk request data + View organizations."
            )
        elif resp.ok:
            n = len(resp.json().get("values") or [])
            lines.append(f"GET /rest/servicedeskapi/servicedesk → HTTP 200 ({n} desk(s) visible)")
        else:
            lines.append(f"GET /rest/servicedeskapi/servicedesk → HTTP {resp.status_code}")
    except Exception as e:
        lines.append(f"GET /rest/servicedeskapi/servicedesk failed: {e}")

    lines.append(
        "In Atlassian Administration: grant the service account JSM agent on project HELP "
        "and token scope read:organization:jira-service-management (granular) or "
        "manage:servicedesk-customer (classic)."
    )
    return lines


def resolve_pendo_name(
    sf_label: str,
    pendo_prefixes: frozenset[str],
) -> tuple[str | None, list[str]]:
    """Map SF portfolio label → Pendo prefix; return alias provenance notes."""
    from .portfolio_salesforce_allowlist import (
        _load_sf_portfolio_pendo_alias_map,
        _resolve_sf_label_via_pendo_alias_file,
        resolve_sf_label_to_pendo_prefix,
    )

    notes: list[str] = []
    label = (sf_label or "").strip()
    if not label or not pendo_prefixes:
        return None, notes

    canon = {p.lower(): p for p in pendo_prefixes}
    alias_map = _load_sf_portfolio_pendo_alias_map()
    via_alias = _resolve_sf_label_via_pendo_alias_file(label, canon=canon)
    if via_alias:
        targets = alias_map.get(label.lower()) or []
        tgt = ", ".join(repr(t) for t in targets) if targets else "(see yaml)"
        notes.append(f"{SF_PORTFOLIO_PENDO_ALIASES_FILE.name}: {label!r} → {via_alias!r} ({tgt})")
        return via_alias, notes

    if label.lower() in canon:
        return canon[label.lower()], notes

    hit = resolve_sf_label_to_pendo_prefix(label, pendo_prefixes)
    if hit:
        if hit.lower() != label.lower():
            notes.append(f"Pendo heuristic match: {label!r} → {hit!r}")
        return hit, notes
    return None, notes


def resolve_csr_names(
    sf_label: str,
    pendo_name: str | None,
    *,
    csr_names: frozenset[str],
) -> tuple[list[str], list[str]]:
    """Return CS Report ``customer`` values matched for this account."""
    from .cs_report_client import (
        _load_cs_report_alias_map,
        _sites_for_customer_lookup,
        cs_report_lookup_keys_for_account,
    )

    notes: list[str] = []
    alias_map = _load_cs_report_alias_map()
    keys = cs_report_lookup_keys_for_account(
        salesforce_label=sf_label,
        pendo_customer_key=pendo_name,
    )
    matched: list[str] = []
    seen: set[str] = set()
    for key in keys:
        _sites, _lk, _tried, csr_hits = _sites_for_customer_lookup(key)
        for name in csr_hits:
            nl = name.lower()
            if nl in seen:
                continue
            seen.add(nl)
            matched.append(name)
            if nl != key.lower():
                extras = alias_map.get(key.lower()) or []
                if any(e.lower() == nl for e in extras):
                    notes.append(
                        f"{CS_REPORT_CUSTOMER_ALIASES_FILE.name}: {key!r} → {name!r}"
                    )
                else:
                    notes.append(f"CSR lookup key {key!r} → {name!r}")
    return matched, list(dict.fromkeys(notes))


def resolve_jsm_names(
    sf_label: str,
    pendo_name: str | None,
    *,
    jsm_orgs: list[str],
) -> tuple[list[str], list[str]]:
    """Return JSM organization directory labels matched for this account."""
    from .jira_client import (
        _fuzzy_pick_jsm_organizations,
        _load_jsm_org_alias_map,
        _merge_jsm_customer_alias_terms,
    )

    notes: list[str] = []
    alias_map = _load_jsm_org_alias_map()
    seeds = [sf_label]
    if pendo_name:
        seeds.append(pendo_name)
    terms = _merge_jsm_customer_alias_terms(seeds)
    for seed in seeds:
        s = (seed or "").strip()
        if not s:
            continue
        for extra in alias_map.get(s.lower()) or []:
            if extra in terms:
                notes.append(f"{JSM_ORGANIZATION_ALIASES_FILE.name}: {s!r} → {extra!r}")

    org_by_lower = {o.lower(): o for o in jsm_orgs}
    picked: list[str] = []
    seen: set[str] = set()

    for term in terms:
        exact = org_by_lower.get(term.lower())
        if exact and exact.lower() not in seen:
            seen.add(exact.lower())
            picked.append(exact)
            if exact.lower() != (sf_label or "").lower() and (
                not pendo_name or exact.lower() != pendo_name.lower()
            ):
                if term not in (alias_map.get((sf_label or "").lower()) or []):
                    if not pendo_name or term not in (alias_map.get(pendo_name.lower()) or []):
                        notes.append(f"JSM exact directory match for term {term!r}")

    fuzzy = _fuzzy_pick_jsm_organizations(terms, jsm_orgs) or []
    for org in fuzzy:
        if org.lower() not in seen:
            seen.add(org.lower())
            picked.append(org)
            notes.append(f"JSM fuzzy match (terms include alias-expanded labels)")

    return picked, list(dict.fromkeys(notes))


def build_company_match_report(
    *,
    pendo_days: int = 30,
    include_pendo: bool = True,
    include_csr: bool = True,
    include_jsm: bool = True,
) -> dict[str, Any]:
    """Build match rows grouped by Salesforce contract status bucket."""
    from .data_source_health import _salesforce_configured
    from .llm_export_salesforce_universe import salesforce_portfolio_rollups_split

    out: dict[str, Any] = {
        "salesforce_configured": _salesforce_configured(),
        "pendo_prefix_count": 0,
        "csr_customer_count": 0,
        "jsm_org_count": 0,
        "by_status": {"active": [], "churned": [], "renewal_in_negotiation": []},
        "errors": [],
    }
    if not out["salesforce_configured"]:
        out["errors"].append("Salesforce not configured (SF_* env vars)")
        return out

    active, churned, _labels, _book, renewal = salesforce_portfolio_rollups_split()
    all_rollups: list[tuple[str, dict[str, Any]]] = []
    for bucket, rows in (
        ("active", active),
        ("churned", churned),
        ("renewal_in_negotiation", renewal),
    ):
        for r in rows:
            if isinstance(r, dict) and (r.get("customer") or "").strip():
                all_rollups.append((bucket, r))

    pendo_prefixes: frozenset[str] = frozenset()
    if include_pendo:
        try:
            pendo_prefixes = _pendo_prefixes(days=pendo_days)
            out["pendo_prefix_count"] = len(pendo_prefixes)
        except Exception as e:
            out["errors"].append(f"Pendo: {e}")

    csr_names: frozenset[str] = frozenset()
    if include_csr:
        try:
            csr_names = _csr_customer_names()
            out["csr_customer_count"] = len(csr_names)
        except Exception as e:
            out["errors"].append(f"CS Report: {e}")

    jsm_orgs: list[str] = []
    if include_jsm:
        try:
            jsm_orgs = _jsm_organization_names()
            out["jsm_org_count"] = len(jsm_orgs)
            if not jsm_orgs:
                out["jsm_org_diagnostic"] = diagnose_jsm_org_directory()
                out["errors"].extend(
                    f"JSM org directory: {line}" for line in out["jsm_org_diagnostic"]
                )
        except Exception as e:
            out["errors"].append(f"Jira: {e}")

    rows_out: list[CompanyMatchRow] = []
    for bucket, rollup in sorted(all_rollups, key=lambda x: str(x[1].get("customer") or "").lower()):
        sf_label = str(rollup.get("customer") or "").strip()
        pendo_name, pendo_notes = (None, [])
        if include_pendo and pendo_prefixes:
            pendo_name, pendo_notes = resolve_pendo_name(sf_label, pendo_prefixes)

        csr_matched, csr_notes = ([], [])
        if include_csr and csr_names is not None:
            csr_matched, csr_notes = resolve_csr_names(
                sf_label, pendo_name, csr_names=csr_names
            )

        jsm_matched, jsm_notes = ([], [])
        if include_jsm and jsm_orgs is not None:
            jsm_matched, jsm_notes = resolve_jsm_names(
                sf_label, pendo_name, jsm_orgs=jsm_orgs
            )

        statuses = rollup.get("contract_statuses_distinct")
        if isinstance(statuses, str):
            statuses = [statuses]
        elif not isinstance(statuses, list):
            statuses = []

        row = CompanyMatchRow(
            salesforce_label=sf_label,
            status=bucket,
            contract_statuses=[str(s) for s in statuses if s],
            arr=rollup.get("arr"),
            pendo_name=pendo_name,
            csr_names=csr_matched,
            jsm_names=jsm_matched,
            alias_notes=pendo_notes + csr_notes + jsm_notes,
        )
        rows_out.append(row)
        out["by_status"].setdefault(bucket, []).append(_row_to_dict(row))

    out["total"] = len(rows_out)
    return out


def _row_to_dict(row: CompanyMatchRow) -> dict[str, Any]:
    return {
        "salesforce_label": row.salesforce_label,
        "status": row.status,
        "contract_statuses": row.contract_statuses,
        "arr": row.arr,
        "pendo_name": row.pendo_name,
        "csr_names": row.csr_names,
        "jsm_names": row.jsm_names,
        "alias_notes": row.alias_notes,
    }


def render_match_report_text(report: dict[str, Any]) -> str:
    """Human-readable report."""
    lines: list[str] = []
    if report.get("errors"):
        lines.append("Warnings / diagnostics:")
        for e in report["errors"]:
            lines.append(f"  - {e}")
        lines.append("")
    jsm_diag = report.get("jsm_org_diagnostic")
    if jsm_diag and not report.get("errors"):
        lines.append("JSM organization directory (diagnostic):")
        for line in jsm_diag:
            lines.append(f"  - {line}")
        lines.append("")

    if not report.get("salesforce_configured"):
        lines.append("Salesforce is not configured; nothing to list.")
        return "\n".join(lines)

    lines.append(
        f"Pendo prefixes: {report.get('pendo_prefix_count', 0)} · "
        f"CSR customers: {report.get('csr_customer_count', 0)} · "
        f"JSM orgs: {report.get('jsm_org_count', 0)} · "
        f"SF portfolio labels: {report.get('total', 0)}"
    )
    lines.append("")

    titles = {
        "active": "Active installed base",
        "churned": "Churned / inactive contract",
        "renewal_in_negotiation": "Renewal in negotiation",
    }
    by_status = report.get("by_status") or {}
    for key in ("active", "churned", "renewal_in_negotiation"):
        block = by_status.get(key) or []
        lines.append(f"=== {titles.get(key, key)} ({len(block)}) ===")
        if not block:
            lines.append("(none)")
            lines.append("")
            continue
        for row in block:
            lines.extend(_format_company_block(row))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _format_company_block(row: dict[str, Any]) -> list[str]:
    sf = row.get("salesforce_label") or ""
    arr = row.get("arr")
    arr_s = f"  ARR ${arr:,.0f}" if isinstance(arr, (int, float)) else ""
    statuses = row.get("contract_statuses") or []
    st_s = f"  [{', '.join(statuses)}]" if statuses else ""
    lines = [f"Salesforce: {sf}{arr_s}{st_s}"]

    pendo = row.get("pendo_name")
    lines.append(f"  Pendo: {pendo if pendo else '(no match)'}")
    csr = row.get("csr_names") or []
    lines.append(f"  CSR: {', '.join(csr) if csr else '(no match)'}")
    jsm = row.get("jsm_names") or []
    lines.append(f"  JSM: {', '.join(jsm) if jsm else '(no match)'}")
    for note in row.get("alias_notes") or []:
        lines.append(f"    ↳ alias: {note}")
    lines.append("")
    return lines
