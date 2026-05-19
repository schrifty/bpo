"""Reconcile Salesforce Customer Entity portfolio labels with Pendo sitename prefixes."""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from typing import Any, AbstractSet

from .llm_export_salesforce_universe import (
    _row_has_pendo_metrics,
    salesforce_portfolio_rollups_split,
)
from .portfolio_salesforce_allowlist import (
    portfolio_labels_from_entity_accounts,
    resolve_sf_label_to_pendo_prefix,
    summarize_salesforce_label_activity,
)

_CORP_SUFFIXES = frozenset(
    {
        "inc",
        "incorporated",
        "llc",
        "ltd",
        "limited",
        "corp",
        "corporation",
        "co",
        "company",
        "group",
        "holdings",
        "international",
        "intl",
        "na",
        "us",
        "uk",
        "the",
        "and",
        "of",
        "a",
        "an",
    }
)


def _normalize_name(text: str) -> str:
    raw = (text or "").strip().lower()
    raw = re.sub(r"[^\w\s]", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _tokens(text: str) -> list[str]:
    return [t for t in _normalize_name(text).split() if t and t not in _CORP_SUFFIXES]


def _acronym(tokens: list[str]) -> str:
    return "".join(t[0] for t in tokens if t)


def _token_jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


@dataclass(frozen=True)
class MatchHint:
    sf_label: str
    pendo_prefix: str
    score: float
    reasons: tuple[str, ...] = ()
    sf_entity_sample: tuple[str, ...] = ()


@dataclass
class ReconcileReport:
    days: int
    salesforce_configured: bool
    active_sf_labels: list[str] = field(default_factory=list)
    pendo_prefixes: list[str] = field(default_factory=list)
    sf_active_no_pendo: list[dict[str, Any]] = field(default_factory=list)
    pendo_no_sf: list[dict[str, Any]] = field(default_factory=list)
    suggested_pairs: list[MatchHint] = field(default_factory=list)
    provenance: dict[str, Any] = field(default_factory=dict)


def score_sf_pendo_pair(
    sf_label: str,
    pendo_prefix: str,
    *,
    sf_entity_names: AbstractSet[str] | None = None,
) -> tuple[float, list[str]]:
    """Heuristic similarity 0–1 with human-readable reasons."""
    reasons: list[str] = []
    sl = (sf_label or "").strip()
    pp = (pendo_prefix or "").strip()
    if not sl or not pp:
        return 0.0, reasons

    sn, pn = _normalize_name(sl), _normalize_name(pp)
    if sn == pn:
        return 1.0, ["exact normalized match"]

    if sn in pn or pn in sn:
        reasons.append("substring")
        base = 0.88
    else:
        base = 0.0

    tsf, tp = _tokens(sl), _tokens(pp)
    jac = _token_jaccard(tsf, tp)
    if jac >= 0.34:
        reasons.append(f"token overlap ({jac:.0%})")
    base = max(base, 0.55 + 0.4 * jac)

    ratio = difflib.SequenceMatcher(None, sn, pn).ratio()
    if ratio >= 0.72:
        reasons.append(f"spelling similarity ({ratio:.0%})")
    base = max(base, ratio)

    asf, ap = _acronym(tsf), _acronym(tp)
    if len(asf) >= 2 and len(ap) >= 2 and asf == ap:
        reasons.append(f"acronym {asf!r}")
        base = max(base, 0.82)
    if len(asf) >= 2 and asf in pn:
        reasons.append(f"SF acronym {asf!r} in Pendo name")
        base = max(base, 0.78)
    if len(ap) >= 2 and ap in sn:
        reasons.append(f"Pendo acronym {ap!r} in SF label")
        base = max(base, 0.78)

    if sf_entity_names:
        for ent in sf_entity_names:
            en = _normalize_name(ent)
            if not en:
                continue
            if pn == en or pn in en or en in pn:
                reasons.append(f"SF entity name contains Pendo prefix ({ent!r})")
                base = max(base, 0.9)
                break
            et = _tokens(ent)
            if _token_jaccard(et, tp) >= 0.5:
                reasons.append(f"entity↔pendo token overlap ({ent!r})")
                base = max(base, 0.85)
                break

    return min(1.0, base), reasons


def _pendo_prefixes_from_portfolio(portfolio: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in portfolio.get("customers") or []:
        if not isinstance(row, dict):
            continue
        c = str(row.get("customer") or "").strip()
        if not c:
            continue
        low = c.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(c)
    out.sort(key=str.lower)
    return out


def _pendo_row_by_key(portfolio: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in portfolio.get("customers") or []:
        if not isinstance(row, dict):
            continue
        c = str(row.get("customer") or "").strip()
        if c:
            out[c.lower()] = row
    return out


def _sf_label_has_pendo_metrics(
    sf_label: str,
    *,
    pendo_prefixes: AbstractSet[str],
    pendo_by_key: dict[str, dict[str, Any]],
) -> tuple[bool, str | None]:
    mapped = resolve_sf_label_to_pendo_prefix(sf_label, pendo_prefixes)
    if mapped and mapped.lower() in pendo_by_key:
        return _row_has_pendo_metrics(pendo_by_key[mapped.lower()]), mapped
    if sf_label.lower() in pendo_by_key:
        return _row_has_pendo_metrics(pendo_by_key[sf_label.lower()]), mapped
    return False, mapped


def _sf_labels_mapped_by_active_rollups(
    active_rollups: list[dict[str, Any]],
    pendo_prefixes: AbstractSet[str],
) -> dict[str, str | None]:
    """Pendo prefix → SF portfolio label (first active label that maps to prefix)."""
    out: dict[str, str | None] = {}
    for r in active_rollups:
        label = str(r.get("customer") or "").strip()
        if not label:
            continue
        mapped = resolve_sf_label_to_pendo_prefix(label, pendo_prefixes)
        if mapped and mapped not in out:
            out[mapped] = label
    return out


def build_reconcile_report(
    portfolio: dict[str, Any],
    *,
    days: int,
    entity_accounts: list[dict[str, Any]] | None = None,
) -> ReconcileReport:
    """Classify SF-active / Pendo-only gaps and suggest fuzzy pairings."""
    from .data_source_health import _salesforce_configured

    report = ReconcileReport(days=int(days), salesforce_configured=_salesforce_configured())
    prefixes = _pendo_prefixes_from_portfolio(portfolio)
    report.pendo_prefixes = prefixes
    pendo_set = frozenset(prefixes)
    pendo_by_key = _pendo_row_by_key(portfolio)

    if not report.salesforce_configured:
        report.provenance["error"] = "salesforce_not_configured"
        return report

    if entity_accounts is None:
        from .salesforce_client import SalesforceClient

        entity_accounts = SalesforceClient().get_entity_accounts()

    active_rollups, _churned, sf_labels, book = salesforce_portfolio_rollups_split()
    report.active_sf_labels = [
        str(r.get("customer") or "").strip()
        for r in active_rollups
        if str(r.get("customer") or "").strip()
    ]
    report.provenance["salesforce_entity_rows"] = len(entity_accounts)
    report.provenance["salesforce_revenue_book"] = {
        k: book.get(k)
        for k in ("configured", "matched_customers", "salesforce_unmatched_customers")
        if k in book
    }

    prefix_to_sf = _sf_labels_mapped_by_active_rollups(active_rollups, pendo_set)

    for r in active_rollups:
        label = str(r.get("customer") or "").strip()
        if not label:
            continue
        has_pendo, mapped = _sf_label_has_pendo_metrics(
            label, pendo_prefixes=pendo_set, pendo_by_key=pendo_by_key
        )
        if has_pendo:
            continue
        activity = summarize_salesforce_label_activity(label, entity_accounts)
        report.sf_active_no_pendo.append(
            {
                "salesforce_label": label,
                "mapped_pendo_prefix": mapped,
                "entity_row_count": activity.get("entity_row_count"),
                "arr_active": activity.get("arr_active"),
                "entity_names_sample": activity.get("entity_names_sample") or [],
                "portfolio_labels_matched": activity.get("portfolio_labels_matched") or [],
            }
        )

    active_label_lower = {x.lower() for x in report.active_sf_labels}
    for prefix in prefixes:
        row = pendo_by_key.get(prefix.lower()) or {}
        if not _row_has_pendo_metrics(row):
            continue
        sf_label = prefix_to_sf.get(prefix)
        if sf_label and sf_label.lower() in active_label_lower:
            continue
        report.pendo_no_sf.append(
            {
                "pendo_prefix": prefix,
                "total_users": row.get("total_users"),
                "active_users": row.get("active_users"),
                "login_pct": row.get("login_pct"),
                "inferred_sf_label": sf_label,
            }
        )

    pendo_with_metrics = [
        prefix
        for prefix in prefixes
        if _row_has_pendo_metrics(pendo_by_key.get(prefix.lower()) or {})
    ]
    hints: list[MatchHint] = []

    for sf_row in report.sf_active_no_pendo:
        sf_label = sf_row["salesforce_label"]
        entities = frozenset(str(x) for x in (sf_row.get("entity_names_sample") or []))
        mapped = (sf_row.get("mapped_pendo_prefix") or "").strip()
        candidates = list(pendo_with_metrics)
        if mapped and mapped in candidates:
            candidates = [p for p in candidates if p != mapped]
        for pp in candidates:
            score, reasons = score_sf_pendo_pair(
                sf_label, pp, sf_entity_names=entities
            )
            if score < 0.62:
                continue
            hints.append(
                MatchHint(
                    sf_label=sf_label,
                    pendo_prefix=pp,
                    score=round(score, 3),
                    reasons=tuple(reasons),
                    sf_entity_sample=tuple(sorted(entities)[:6]),
                )
            )

    hints.sort(key=lambda h: (-h.score, h.sf_label.lower(), h.pendo_prefix.lower()))
    seen_pairs: set[tuple[str, str]] = set()
    for h in hints:
        key = (h.sf_label.lower(), h.pendo_prefix.lower())
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        report.suggested_pairs.append(h)

    report.provenance["sf_active_no_pendo_count"] = len(report.sf_active_no_pendo)
    report.provenance["pendo_no_sf_count"] = len(report.pendo_no_sf)
    report.provenance["suggested_pair_count"] = len(report.suggested_pairs)
    report.provenance["pendo_with_metrics_count"] = len(pendo_with_metrics)
    return report


def render_reconcile_markdown(rep: ReconcileReport) -> str:
    """Human-readable findings for stdout or file output."""
    lines: list[str] = []
    lines.append("# Salesforce ↔ Pendo portfolio reconciliation")
    lines.append("")
    lines.append(f"- Pendo window: **{rep.days}** days (same as LLM export portfolio rollup)")
    lines.append(f"- Salesforce configured: **{rep.salesforce_configured}**")
    lines.append(f"- Active SF portfolio labels: **{len(rep.active_sf_labels)}**")
    lines.append(f"- Pendo customer prefixes: **{len(rep.pendo_prefixes)}**")
    lines.append("")

    lines.append("## 1. Active in Salesforce, no Pendo usage data")
    lines.append("")
    if not rep.salesforce_configured:
        lines.append("_Salesforce not configured — configure JWT env vars and re-run._")
    elif not rep.sf_active_no_pendo:
        lines.append("_None — every active Customer Entity label has a Pendo prefix with metrics._")
    else:
        lines.append(
            "| Salesforce label | Mapped Pendo | Entity rows | ARR (active) | Sample entity names |"
        )
        lines.append("| --- | --- | ---: | ---: | --- |")
        for row in rep.sf_active_no_pendo:
            ents = ", ".join((row.get("entity_names_sample") or [])[:4])
            arr = row.get("arr_active")
            arr_s = f"${arr:,.0f}" if isinstance(arr, (int, float)) else "—"
            mapped = row.get("mapped_pendo_prefix") or "—"
            lines.append(
                f"| {row['salesforce_label']} | {mapped} | {row.get('entity_row_count') or 0} "
                f"| {arr_s} | {ents or '—'} |"
            )
    lines.append("")

    lines.append("## 2. Pendo companies with no active Salesforce match")
    lines.append("")
    if not rep.pendo_no_sf:
        lines.append("_None — every Pendo prefix with metrics maps to an active SF portfolio label._")
    else:
        lines.append("| Pendo prefix | Users | Active users | Login % |")
        lines.append("| --- | ---: | ---: | ---: |")
        for row in rep.pendo_no_sf:
            login = row.get("login_pct")
            login_s = f"{login}%" if login is not None else "—"
            lines.append(
                f"| {row['pendo_prefix']} | {row.get('total_users') or '—'} | "
                f"{row.get('active_users') or '—'} | {login_s} |"
            )
    lines.append("")

    lines.append("## 3. Cross-gap hints (subsidiaries, acronyms, fuzzy names)")
    lines.append("")
    lines.append(
        "Pairs below link **§1** Salesforce labels to **other** Pendo prefixes (with usage data) "
        "when names overlap, acronyms align, or SF **entity** names resemble a sitename "
        "(e.g. `Key Technology` entities under `Duravant-*` → Pendo prefix `Duravant`)."
    )
    lines.append("")
    if not rep.suggested_pairs:
        lines.append("_No plausible cross-gap pairs above the confidence threshold (0.62)._")
    else:
        lines.append("| Score | Salesforce label | Pendo prefix | Evidence |")
        lines.append("| ---: | --- | --- | --- |")
        for h in rep.suggested_pairs[:80]:
            lines.append(
                f"| {h.score:.2f} | {h.sf_label} | {h.pendo_prefix} | "
                f"{'; '.join(h.reasons) or '—'} |"
            )
        if len(rep.suggested_pairs) > 80:
            lines.append(f"\n_… and {len(rep.suggested_pairs) - 80} more pair(s)._")

    lines.append("")
    lines.append("## Findings")
    lines.append("")
    lines.extend(_build_findings_paragraphs(rep))
    return "\n".join(lines) + "\n"


def _build_findings_paragraphs(rep: ReconcileReport) -> list[str]:
    bullets: list[str] = []
    n_sf = len(rep.sf_active_no_pendo)
    n_pendo = len(rep.pendo_no_sf)
    n_pairs = len(rep.suggested_pairs)

    if not rep.salesforce_configured:
        bullets.append(
            "- **Salesforce is not loaded.** Results reflect Pendo only; configure Salesforce "
            "to compare installed-base contract entities to product usage."
        )
        return bullets

    bullets.append(
        f"- **{n_sf}** active Salesforce portfolio label(s) appear in CRM with **no matching Pendo "
        "usage row** (no prefix match or matched prefix has null users/login)."
    )
    bullets.append(
        f"- **{n_pendo}** Pendo sitename prefix(es) have usage metrics but **no active Salesforce "
        "portfolio label** maps to them via the standard resolver (aliases + word-boundary rules)."
    )

    if n_pairs:
        strong = [h for h in rep.suggested_pairs if h.score >= 0.82]
        bullets.append(
            f"- **{n_pairs}** cross-gap pairing(s) scored ≥0.62; **{len(strong)}** are high "
            "confidence (≥0.82). Review these for missing `config/sf_portfolio_pendo_aliases.yaml` "
            "entries or parent/ultimate-parent rollup mismatches."
        )
        top = rep.suggested_pairs[:5]
        examples = "; ".join(f"{h.sf_label} ↔ {h.pendo_prefix}" for h in top)
        bullets.append(f"- Top suggested pairs: {examples}.")
    elif n_sf and n_pendo:
        bullets.append(
            "- No strong fuzzy link between the two gap lists — gaps are likely true orphans "
            "(churned plants, demo sites, spelling differences beyond heuristics, or CRM labels "
            "that never appear in Pendo sitenames)."
        )
    elif not n_sf and not n_pendo:
        bullets.append(
            "- **Portfolio alignment looks complete** for active Salesforce entities and Pendo "
            "prefixes under current matching rules."
        )

    unmapped_sf = [r for r in rep.sf_active_no_pendo if not r.get("mapped_pendo_prefix")]
    if unmapped_sf:
        labels = ", ".join(r["salesforce_label"] for r in unmapped_sf[:12])
        suffix = "…" if len(unmapped_sf) > 12 else ""
        bullets.append(
            f"- **{len(unmapped_sf)}** SF-only label(s) have **no Pendo prefix candidate** at all "
            f"(not only missing metrics): {labels}{suffix}."
        )

    return bullets
