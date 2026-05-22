"""Cross-system customer name resolution (Salesforce → Pendo, CS Report, JSM).

CLI: ``./bin/match-customer-names`` uploads to QBR ``Output/`` and ``{date} - Output/`` on Drive
(same rules as deck / LLM export). Use ``--no-drive`` for a local-only run.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

NO_MATCH_LABEL = "(no match)"
NOT_QUERIED_LABEL = "(not queried)"

from src.config_paths import (
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
    from src.pendo_client import PendoClient, customer_is_excluded_from_portfolio

    by_customer = PendoClient().get_sites_by_customer(days=days)
    return frozenset(
        c
        for c in (by_customer.get("customer_list") or [])
        if c and c != "(unknown)" and not customer_is_excluded_from_portfolio(c)
    )


def _pendo_sites_by_prefix(*, days: int) -> dict[str, list[str]]:
    """Map Pendo customer prefix → sitenames (portfolio-eligible prefixes only)."""
    from src.pendo_client import PendoClient, customer_is_excluded_from_portfolio

    by_customer = PendoClient().get_sites_by_customer(days=days)
    out: dict[str, list[str]] = {}
    for prefix, sites in (by_customer.get("by_customer") or {}).items():
        if not prefix or prefix == "(unknown)" or customer_is_excluded_from_portfolio(prefix):
            continue
        names = sorted(
            {(s.get("sitename") or "").strip() for s in sites if (s.get("sitename") or "").strip()},
            key=str.lower,
        )
        if names:
            out[prefix] = names
    return out


def _normalize_label_tokens(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (label or "").lower()).strip()


def _pendo_display_for_sf_label(
    sf_label: str,
    prefix: str,
    sites_by_prefix: dict[str, list[str]],
) -> str:
    """Prefer a concrete Pendo sitename when the SF label is site-specific (e.g. Acuna plant)."""
    sites = sites_by_prefix.get(prefix) or []
    if not sites:
        return prefix

    norm_sf = _normalize_label_tokens(sf_label)
    norm_prefix = _normalize_label_tokens(prefix)
    if not norm_sf or norm_sf == norm_prefix:
        return prefix

    sf_tokens = norm_sf.split()
    prefix_tokens = set(norm_prefix.split()) if norm_prefix else set()
    distinctive = [t for t in sf_tokens if t not in prefix_tokens]

    best_sn = prefix
    best_score = -1
    for sn in sites:
        norm_sn = _normalize_label_tokens(sn)
        if not norm_sn:
            continue
        sn_tokens = norm_sn.split()
        if distinctive and not all(t in sn_tokens for t in distinctive):
            continue
        overlap = len(set(sn_tokens) & set(sf_tokens))
        if not distinctive and overlap < max(1, len(sn_tokens) - 1):
            continue
        extra_tokens = len(set(sn_tokens) - set(sf_tokens))
        score = overlap * 100 - extra_tokens * 50
        if norm_sn == norm_sf or norm_sn in norm_sf or norm_sf in norm_sn:
            score += 5000
        elif norm_sf in norm_sn:
            score += 2000
        if score > best_score:
            best_score = score
            best_sn = sn
    return best_sn


def _csr_customer_names() -> frozenset[str]:
    from src.cs_report_client import _fetch_latest_report

    rows = _fetch_latest_report()
    return frozenset(
        (r.get("customer") or "").strip()
        for r in rows
        if (r.get("customer") or "").strip()
    )


def _jsm_organization_names() -> list[str]:
    from src.jira_client import get_shared_jira_client

    return get_shared_jira_client()._list_jsm_organization_names()


def diagnose_jsm_org_directory() -> list[str]:
    """Explain empty JSM org directory (HTTP 200 with size=0 vs scope errors)."""
    import requests

    lines: list[str] = []
    try:
        from src.jira_client import get_shared_jira_client

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


def _pendo_stale_alias_notes(sf_label: str, pendo_prefixes: frozenset[str]) -> list[str]:
    """YAML alias targets for *sf_label* that are not present in the Pendo customer list."""
    from src.portfolio_salesforce_allowlist import _load_sf_portfolio_pendo_alias_map

    label = (sf_label or "").strip()
    if not label:
        return []
    targets = _load_sf_portfolio_pendo_alias_map().get(label.lower()) or []
    if not targets:
        return []
    canon = {p.lower() for p in pendo_prefixes}
    stale = [t for t in targets if (t or "").strip() and t.lower() not in canon]
    if not stale:
        return []
    stale_s = ", ".join(repr(t) for t in stale)
    return [
        f"{SF_PORTFOLIO_PENDO_ALIASES_FILE.name}: {label!r} → {stale_s} not in Pendo customer list"
    ]


def _pendo_heuristic_on_sf_label(sf_label: str, pendo_prefixes: frozenset[str]) -> str | None:
    """Word-boundary / first-token match on the Salesforce label only (no YAML aliases)."""
    from src.portfolio_salesforce_allowlist import _name_matches_word_boundary

    sl = (sf_label or "").strip()
    if not sl or not pendo_prefixes:
        return None
    canon = {p.lower(): p for p in pendo_prefixes}
    low = sl.lower()
    if low in canon:
        return canon[low]

    candidates: list[str] = []
    for p in pendo_prefixes:
        if _name_matches_word_boundary(p, sl):
            candidates.append(canon[p.lower()])
    if candidates:
        return max(candidates, key=len)

    parts = sl.split()
    if parts:
        first = parts[0]
        if first.lower() in canon:
            return canon[first.lower()]
        first_hits: list[str] = []
        for p in pendo_prefixes:
            if _name_matches_word_boundary(p, first):
                first_hits.append(canon[p.lower()])
        if first_hits:
            return max(first_hits, key=len)
    return None


def resolve_pendo_name(
    sf_label: str,
    pendo_prefixes: frozenset[str],
    *,
    pendo_sites_by_prefix: dict[str, list[str]] | None = None,
) -> tuple[str | None, list[str]]:
    """Map SF portfolio label → Pendo sitename/prefix; YAML aliases only after SF-name match fails."""
    from src.portfolio_salesforce_allowlist import (
        _load_sf_portfolio_pendo_alias_map,
        _resolve_sf_label_via_pendo_alias_file,
    )

    notes: list[str] = []
    label = (sf_label or "").strip()
    if not label or not pendo_prefixes:
        return None, notes

    sites_map = pendo_sites_by_prefix or {}

    def _finish(prefix_hit: str, *, via: str) -> tuple[str, list[str]]:
        display = _pendo_display_for_sf_label(label, prefix_hit, sites_map)
        if display != prefix_hit:
            notes.append(f"Pendo sitename match: {label!r} → {display!r} (prefix {prefix_hit!r})")
        elif via == "heuristic" and display.lower() != label.lower():
            notes.append(f"Pendo heuristic match: {label!r} → {display!r}")
        return display, list(dict.fromkeys(notes))

    canon = {p.lower(): p for p in pendo_prefixes}
    if label.lower() in canon:
        return _finish(canon[label.lower()], via="exact")

    hit = _pendo_heuristic_on_sf_label(label, pendo_prefixes)
    if hit:
        return _finish(hit, via="heuristic")

    alias_map = _load_sf_portfolio_pendo_alias_map()
    via_alias = _resolve_sf_label_via_pendo_alias_file(label, canon=canon)
    notes.extend(_pendo_stale_alias_notes(label, pendo_prefixes))
    if via_alias:
        targets = alias_map.get(label.lower()) or []
        tgt = ", ".join(repr(t) for t in targets) if targets else "(see yaml)"
        notes.append(f"{SF_PORTFOLIO_PENDO_ALIASES_FILE.name}: {label!r} → {via_alias!r} ({tgt})")
        return _finish(via_alias, via="alias")
    return None, list(dict.fromkeys(notes))


def _csr_alias_lookup_keys_for_seed(seed: str) -> list[str]:
    """Extra lookup keys from YAML/cohort aliases for one seed (not the seed itself)."""
    from src.cs_report_client import (
        _load_cohort_customer_alias_map,
        _load_cs_report_alias_map,
    )

    s = (seed or "").strip()
    if not s:
        return []
    keys: list[str] = []
    seen: set[str] = {s.lower()}

    def add(term: str) -> None:
        t = (term or "").strip()
        if not t or t.lower() in seen:
            return
        seen.add(t.lower())
        keys.append(t)

    for target in _load_cs_report_alias_map().get(s.lower()) or []:
        add(target)
    for term in _load_cohort_customer_alias_map().get(s.lower()) or []:
        add(term)
    return keys


def _csr_stale_alias_notes_for_seed(seed: str, csr_names: frozenset[str]) -> list[str]:
    """Alias targets for *seed* that are not in the CS Report ``customer`` column."""
    from src.cs_report_client import (
        _load_cohort_customer_alias_map,
        _load_cs_report_alias_map,
    )

    csr_lower = {n.lower() for n in csr_names}
    s = (seed or "").strip()
    if not s:
        return []
    notes: list[str] = []
    seen: set[str] = set()
    cohort = _load_cohort_customer_alias_map()

    for key in _csr_alias_lookup_keys_for_seed(seed):
        if key.lower() in csr_lower:
            continue
        if key.lower() in {t.lower() for t in cohort.get(s.lower()) or []}:
            msg = f"cohorts.yaml alias: {s!r} → {key!r} not in CS Report customer list"
        else:
            msg = (
                f"{CS_REPORT_CUSTOMER_ALIASES_FILE.name}: {s!r} → {key!r} "
                "not in CS Report customer list"
            )
        if msg not in seen:
            seen.add(msg)
            notes.append(msg)
    return notes


def _csr_collect_hits(
    key: str,
    *,
    alias_map: dict[str, list[str]],
    seen: set[str],
    matched: list[str],
    notes: list[str],
) -> None:
    from src.cs_report_client import _sites_for_customer_lookup

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
                notes.append(f"{CS_REPORT_CUSTOMER_ALIASES_FILE.name}: {key!r} → {name!r}")
            else:
                notes.append(f"CSR lookup key {key!r} → {name!r}")


def resolve_csr_names(
    sf_label: str,
    pendo_name: str | None,
    *,
    csr_names: frozenset[str],
) -> tuple[list[str], list[str]]:
    """Return CS Report ``customer`` values; YAML aliases only after SF-name lookup fails."""
    from src.cs_report_client import _load_cs_report_alias_map

    notes: list[str] = []
    alias_map = _load_cs_report_alias_map()
    sf = (sf_label or "").strip()
    matched: list[str] = []
    seen: set[str] = set()

    if sf:
        _csr_collect_hits(sf, alias_map=alias_map, seen=seen, matched=matched, notes=notes)

    if not matched and sf:
        notes.extend(_csr_stale_alias_notes_for_seed(sf, csr_names))
        for key in _csr_alias_lookup_keys_for_seed(sf):
            _csr_collect_hits(key, alias_map=alias_map, seen=seen, matched=matched, notes=notes)
            if matched:
                break

    pendo = (pendo_name or "").strip()
    if not matched and pendo and pendo.lower() != sf.lower():
        _csr_collect_hits(pendo, alias_map=alias_map, seen=seen, matched=matched, notes=notes)

    if not matched and pendo:
        notes.extend(_csr_stale_alias_notes_for_seed(pendo, csr_names))
        for key in _csr_alias_lookup_keys_for_seed(pendo):
            _csr_collect_hits(key, alias_map=alias_map, seen=seen, matched=matched, notes=notes)
            if matched:
                break

    return matched, list(dict.fromkeys(notes))


def _jsm_alias_terms_for_seed(
    seed: str,
    jsm_orgs: list[str],
) -> tuple[list[str], list[str]]:
    """YAML alias targets for *seed* that exist in the JSM org directory (+ stale notes)."""
    from src.jira_client import _load_jsm_org_alias_map

    org_by_lower = {o.lower(): o for o in jsm_orgs}
    alias_map = _load_jsm_org_alias_map()
    terms: list[str] = []
    seen: set[str] = set()
    notes: list[str] = []
    s = (seed or "").strip()
    if not s:
        return terms, notes
    for extra in alias_map.get(s.lower()) or []:
        e = (extra or "").strip()
        if not e:
            continue
        if e.lower() in org_by_lower:
            canon = org_by_lower[e.lower()]
            if canon.lower() not in seen:
                seen.add(canon.lower())
                terms.append(canon)
        else:
            notes.append(
                f"{JSM_ORGANIZATION_ALIASES_FILE.name}: {s!r} → {e!r} "
                "not in JSM organization directory"
            )
    return terms, notes


def _jsm_pick_from_terms(
    terms: list[str],
    *,
    sf_label: str,
    pendo_name: str | None,
    jsm_orgs: list[str],
    alias_seed: str | None,
) -> tuple[list[str], list[str]]:
    """Exact + fuzzy JSM org picks for *terms* (no YAML expansion)."""
    from src.jira_client import _fuzzy_pick_jsm_organizations, _load_jsm_org_alias_map

    alias_map = _load_jsm_org_alias_map()
    org_by_lower = {o.lower(): o for o in jsm_orgs}
    picked: list[str] = []
    seen: set[str] = set()
    notes: list[str] = []
    sf = (sf_label or "").strip()
    pendo = (pendo_name or "").strip()

    for term in terms:
        t = (term or "").strip()
        if not t:
            continue
        exact = org_by_lower.get(t.lower())
        if exact and exact.lower() not in seen:
            seen.add(exact.lower())
            picked.append(exact)
            if alias_seed and exact in (alias_map.get(alias_seed.lower()) or []):
                notes.append(
                    f"{JSM_ORGANIZATION_ALIASES_FILE.name}: {alias_seed!r} → {exact!r}"
                )
            elif exact.lower() != sf.lower() and (not pendo or exact.lower() != pendo.lower()):
                notes.append(f"JSM exact directory match for term {t!r}")

    fuzzy = _fuzzy_pick_jsm_organizations(terms, jsm_orgs) or []
    for org in fuzzy:
        if org.lower() not in seen:
            seen.add(org.lower())
            picked.append(org)
            notes.append("JSM fuzzy match")
    return picked, notes


def resolve_jsm_names(
    sf_label: str,
    pendo_name: str | None,
    *,
    jsm_orgs: list[str],
) -> tuple[list[str], list[str]]:
    """Return JSM org labels; YAML aliases only after SF-name (then Pendo) lookup fails."""
    notes: list[str] = []
    sf = (sf_label or "").strip()
    pendo = (pendo_name or "").strip()

    if sf:
        picked, hit_notes = _jsm_pick_from_terms(
            [sf], sf_label=sf, pendo_name=pendo_name, jsm_orgs=jsm_orgs, alias_seed=None
        )
        notes.extend(hit_notes)
        if picked:
            return picked, list(dict.fromkeys(notes))
        alias_terms, alias_notes = _jsm_alias_terms_for_seed(sf, jsm_orgs)
        notes.extend(alias_notes)
        if alias_terms:
            picked, hit_notes = _jsm_pick_from_terms(
                alias_terms,
                sf_label=sf,
                pendo_name=pendo_name,
                jsm_orgs=jsm_orgs,
                alias_seed=sf,
            )
            notes.extend(hit_notes)
            if picked:
                return picked, list(dict.fromkeys(notes))

    if pendo and pendo.lower() != sf.lower():
        picked, hit_notes = _jsm_pick_from_terms(
            [pendo], sf_label=sf, pendo_name=pendo, jsm_orgs=jsm_orgs, alias_seed=None
        )
        notes.extend(hit_notes)
        if picked:
            return picked, list(dict.fromkeys(notes))
        alias_terms, alias_notes = _jsm_alias_terms_for_seed(pendo, jsm_orgs)
        notes.extend(alias_notes)
        if alias_terms:
            picked, hit_notes = _jsm_pick_from_terms(
                alias_terms,
                sf_label=sf,
                pendo_name=pendo,
                jsm_orgs=jsm_orgs,
                alias_seed=pendo,
            )
            notes.extend(hit_notes)
            if picked:
                return picked, list(dict.fromkeys(notes))

    return [], list(dict.fromkeys(notes))


def build_company_match_report(
    *,
    pendo_days: int = 30,
    include_pendo: bool = True,
    include_csr: bool = True,
    include_jsm: bool = True,
) -> dict[str, Any]:
    """Build match rows grouped by Salesforce contract status bucket."""
    from src.data_source_health import _salesforce_configured
    from src.llm_export_salesforce_universe import salesforce_portfolio_rollups_split

    out: dict[str, Any] = {
        "salesforce_configured": _salesforce_configured(),
        "pendo_prefix_count": 0,
        "csr_customer_count": 0,
        "jsm_org_count": 0,
        "by_status": {"active": [], "churned": [], "renewal_in_negotiation": []},
        "errors": [],
        "sources_queried": {
            "pendo": include_pendo,
            "csr": include_csr,
            "jsm": include_jsm,
        },
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
    pendo_sites_by_prefix: dict[str, list[str]] = {}
    if include_pendo:
        try:
            pendo_prefixes = _pendo_prefixes(days=pendo_days)
            pendo_sites_by_prefix = _pendo_sites_by_prefix(days=pendo_days)
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
            pendo_name, pendo_notes = resolve_pendo_name(
                sf_label,
                pendo_prefixes,
                pendo_sites_by_prefix=pendo_sites_by_prefix,
            )

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


def _missing_sources_for_row(
    row: dict[str, Any],
    sources_queried: dict[str, bool],
) -> list[str]:
    """Return source keys (pendo, csr, jsm) that were queried but have no match."""
    missing: list[str] = []
    if sources_queried.get("pendo") and not (row.get("pendo_name") or "").strip():
        missing.append("pendo")
    if sources_queried.get("csr") and not (row.get("csr_names") or []):
        missing.append("csr")
    if sources_queried.get("jsm") and not (row.get("jsm_names") or []):
        missing.append("jsm")
    return missing


_STATUS_TITLES = {
    "active": "active",
    "churned": "churned",
    "renewal_in_negotiation": "renewal in negotiation",
}

_MISSING_SUMMARY_SECTIONS = (
    ("pendo", "Pendo Missing"),
    ("csr", "CSR Missing"),
    ("jsm", "Atlassian Missing"),
)


def customers_missing_by_source(report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """SF customers with no match per queried source (each row appears only under missing sources)."""
    sq = report.get("sources_queried") or {}
    out: dict[str, list[dict[str, Any]]] = {key: [] for key, _ in _MISSING_SUMMARY_SECTIONS}
    if not any(sq.get(k) for k in out):
        return out

    by_status = report.get("by_status") or {}
    for status_key in ("active", "churned", "renewal_in_negotiation"):
        for row in by_status.get(status_key) or []:
            entry = {
                "salesforce_label": row.get("salesforce_label") or "",
                "status": status_key,
            }
            for source_key in _missing_sources_for_row(row, sq):
                if source_key in out:
                    out[source_key].append(entry)
    for entries in out.values():
        entries.sort(key=lambda x: (x.get("salesforce_label") or "").lower())
    return out


def customers_with_missing_matches(report: dict[str, Any]) -> list[dict[str, Any]]:
    """Customers missing at least one queried source (flat list with ``missing`` key list)."""
    by_source = customers_missing_by_source(report)
    merged: dict[tuple[str, str], set[str]] = {}
    for source_key, entries in by_source.items():
        for entry in entries:
            key = (entry.get("salesforce_label") or "", entry.get("status") or "")
            merged.setdefault(key, set()).add(source_key)
    out = [
        {
            "salesforce_label": sf,
            "status": st,
            "missing": sorted(missing),
        }
        for (sf, st), missing in merged.items()
    ]
    out.sort(key=lambda x: (x.get("salesforce_label") or "").lower())
    return out


def _format_partial_matches_summary(report: dict[str, Any]) -> list[str]:
    """Trailing sections grouped by missing source: Pendo, CSR, Atlassian (JSM)."""
    by_source = customers_missing_by_source(report)
    lines: list[str] = []
    for source_key, title in _MISSING_SUMMARY_SECTIONS:
        if not (report.get("sources_queried") or {}).get(source_key):
            continue
        entries = by_source.get(source_key) or []
        lines.append(f"=== {title} ({len(entries)}) ===")
        if not entries:
            lines.append("(none)")
        else:
            for entry in entries:
                sf = entry.get("salesforce_label") or ""
                st = _STATUS_TITLES.get(entry.get("status") or "", entry.get("status") or "")
                lines.append(f"  {sf}  [{st}]")
        lines.append("")
    return lines


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
        sq = report.get("sources_queried") or {}
        for row in block:
            lines.extend(_format_company_block(row, sources_queried=sq))
        lines.append("")
    lines.extend(_format_partial_matches_summary(report))
    return "\n".join(lines).rstrip() + "\n"


def _field_display(
    *,
    value: str | None,
    list_values: list[str] | None,
    queried: bool,
) -> str:
    if not queried:
        return NOT_QUERIED_LABEL
    if list_values is not None:
        return ", ".join(list_values) if list_values else NO_MATCH_LABEL
    return (value or "").strip() or NO_MATCH_LABEL


def _format_company_block(
    row: dict[str, Any],
    *,
    sources_queried: dict[str, bool] | None = None,
) -> list[str]:
    sq = sources_queried or {}
    sf = row.get("salesforce_label") or ""
    arr = row.get("arr")
    arr_s = f"  ARR ${arr:,.0f}" if isinstance(arr, (int, float)) else ""
    statuses = row.get("contract_statuses") or []
    st_s = f"  [{', '.join(statuses)}]" if statuses else ""
    lines = [f"Salesforce: {sf}{arr_s}{st_s}"]

    lines.append(
        f"  Pendo: {_field_display(value=row.get('pendo_name'), list_values=None, queried=bool(sq.get('pendo')))}"
    )
    lines.append(
        f"  CSR: {_field_display(value=None, list_values=row.get('csr_names') or [], queried=bool(sq.get('csr')))}"
    )
    lines.append(
        f"  JSM: {_field_display(value=None, list_values=row.get('jsm_names') or [], queried=bool(sq.get('jsm')))}"
    )

    for note in row.get("alias_notes") or []:
        lines.append(f"    ↳ alias: {note}")
    lines.append("")
    return lines


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


DEFAULT_MATCH_CUSTOMER_NAMES_OUT = _repo_root() / "output" / "match-customer-names.txt"


def _report_drive_filename(fmt: str) -> str:
    return "match-customer-names.json" if fmt == "json" else "match-customer-names.txt"


def _report_drive_mime_type(fmt: str) -> str:
    return "application/json" if fmt == "json" else "text/plain"


def _default_local_out_path(fmt: str) -> Path:
    root = _repo_root()
    if fmt == "json":
        return root / "output" / "match-customer-names.json"
    return DEFAULT_MATCH_CUSTOMER_NAMES_OUT


def _print_drive_upload_messages(meta: dict[str, str], *, nbytes: int) -> None:
    fname = meta["filename"]
    dated = meta["dated_label"]
    print(
        f"Wrote {nbytes} bytes → Drive Output/{fname} (id={meta['file_id_root']})",
        file=sys.stderr,
    )
    print(
        f"Wrote {nbytes} bytes → Drive Output/{dated}/{fname} (id={meta['file_id_dated']})",
        file=sys.stderr,
    )
    print(f"Output/ (stable): https://drive.google.com/file/d/{meta['file_id_root']}/view")
    print(f"Output/{dated}/: https://drive.google.com/file/d/{meta['file_id_dated']}/view")
    print(
        f"Output/ folder: https://drive.google.com/drive/folders/{meta['root_folder_id']}",
        file=sys.stderr,
    )
    print(
        f"Output/{dated}/ folder: https://drive.google.com/drive/folders/{meta['dated_folder_id']}",
        file=sys.stderr,
    )


def _render_cli_report(report: dict[str, Any], fmt: str) -> str:
    if fmt == "json":
        payload = dict(report)
        payload["customers_missing_by_source"] = customers_missing_by_source(report)
        payload["customers_with_missing_matches"] = customers_with_missing_matches(report)
        return json.dumps(payload, indent=2, default=str)
    return render_match_report_text(report)


def main(argv: list[str] | None = None) -> int:
    """CLI entry for ``bin/match-customer-names``."""
    ap = argparse.ArgumentParser(
        description="Salesforce customers by status with Pendo / CSR / JSM name matches.",
    )
    ap.add_argument("--days", type=int, default=30, help="Pendo sitename window (default 30)")
    ap.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default text)",
    )
    ap.add_argument(
        "--out",
        "-o",
        default=None,
        help="Also write a local copy to this path",
    )
    ap.add_argument(
        "--no-drive",
        action="store_true",
        help="Skip Drive upload; write locally only (default path under output/)",
    )
    ap.add_argument(
        "--stdout",
        action="store_true",
        help="Also print the report to stdout",
    )
    ap.add_argument("--no-pendo", action="store_true", help="Skip Pendo prefix resolution")
    ap.add_argument("--no-csr", action="store_true", help="Skip CS Report matching")
    ap.add_argument("--no-jira", action="store_true", help="Skip JSM organization matching")
    ap.add_argument("-v", "--verbose", action="store_true")
    ns = ap.parse_args(argv)

    logging.getLogger("bpo").setLevel(logging.INFO if ns.verbose else logging.WARNING)

    print("Loading Salesforce portfolio and cross-system names…", file=sys.stderr)
    report = build_company_match_report(
        pendo_days=ns.days,
        include_pendo=not ns.no_pendo,
        include_csr=not ns.no_csr,
        include_jsm=not ns.no_jira,
    )

    body = _render_cli_report(report, ns.format)
    nbytes = len(body.encode("utf-8"))

    if not ns.no_drive:
        from src.drive_config import upload_to_qbr_output_folders

        try:
            drive_meta = upload_to_qbr_output_folders(
                _report_drive_filename(ns.format),
                body,
                mime_type=_report_drive_mime_type(ns.format),
            )
        except RuntimeError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        _print_drive_upload_messages(drive_meta, nbytes=nbytes)

    if ns.out or ns.no_drive:
        out_path = Path(ns.out) if ns.out else _default_local_out_path(ns.format)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(body, encoding="utf-8")
        print(f"Wrote local {out_path}", file=sys.stderr)

    if ns.stdout:
        print(body)

    return 0 if report.get("salesforce_configured") else 1


if __name__ == "__main__":
    from src.cli_warning_filters import apply_cli_warning_filters

    apply_cli_warning_filters()
    from dotenv import load_dotenv

    load_dotenv(_repo_root() / ".env")
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
