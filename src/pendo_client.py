"""Pendo API client for the aggregation endpoint."""

from __future__ import annotations

import datetime
import json
import os
import re
import sys

import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm

from .config import (
    BPO_PENDO_CACHE_TTL_SECONDS,
    BPO_SIGNALS_LLM,
    BPO_SIGNALS_TRENDS,
    FEATURE_ADOPTION_INSIGHTS,
    PENDO_BASE_URL,
    PENDO_INTEGRATION_KEY,
    logger,
)
from .cross_source_signals import extend_health_report_signals
from .signals_llm import maybe_rewrite_signals_with_llm
from .slide_loader import cohort_findings_metadata, cohort_findings_rollup_params

PENDO_REQUEST_TIMEOUT_S = 90
PENDO_TOTAL_TIMEOUT_S = 300


def _name_matches(query: str, text: str) -> bool:
    """Check if query appears as a word boundary match in text.
    'AGI' matches 'AGI Omaha' but not 'Integrated Packaging Machinery'.
    """
    if not query or not text:
        return False
    return bool(re.search(rf'\b{re.escape(query)}\b', text, re.IGNORECASE))


_READ_HEAVY_PORTFOLIO_SIGNAL_RE = re.compile(r"read[-\s]?heavy", re.IGNORECASE)
_MAX_READ_HEAVY_PORTFOLIO_SIGNALS = 4
_MAX_PORTFOLIO_SIGNAL_LINES = 20


def _take_portfolio_signals_capping_read_heavy(
    ranked: list[dict[str, Any]],
    *,
    max_total: int = _MAX_PORTFOLIO_SIGNAL_LINES,
    max_read_heavy: int = _MAX_READ_HEAVY_PORTFOLIO_SIGNALS,
) -> list[dict[str, Any]]:
    """After severity sort, cap repetitive read-heavy lines so other portfolio alarms can surface."""
    out: list[dict[str, Any]] = []
    rh_used = 0
    for item in ranked:
        if len(out) >= max_total:
            break
        sig = str(item.get("signal") or "")
        if _READ_HEAVY_PORTFOLIO_SIGNAL_RE.search(sig):
            if rh_used >= max_read_heavy:
                continue
            rh_used += 1
        out.append(item)
    return out


def _time_series(days: int) -> dict[str, Any]:
    """Build timeSeries for aggregation pipeline."""
    return {
        "period": "dayRange",
        "first": "now()",
        "count": -days,  # Negative = look back
    }


def _merge_visitor_event_rows_by_dimension(
    results: list[dict],
    dimension_key: str,
) -> list[dict]:
    """Merge day-bucket time-series rows into one row per (visitorId, *dimension_key*).

    Pendo returns many rows per visitor–dimension pair when ``timeSeries`` uses dayRange; our
    consumers only sum ``numEvents`` / ``numMinutes``, so merging preserves totals in less RAM
    and smaller Drive preload JSON.
    """
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for ev in results:
        if not isinstance(ev, dict):
            continue
        vid = ev.get("visitorId")
        dim = ev.get(dimension_key)
        key = (str(vid), str(dim))
        ne = int(ev.get("numEvents") or 0)
        nm = int(ev.get("numMinutes") or 0)
        if key not in merged:
            merged[key] = {
                "visitorId": vid,
                dimension_key: dim,
                "numEvents": 0,
                "numMinutes": 0,
                "had_minutes": False,
            }
        m = merged[key]
        m["numEvents"] += ne
        m["numMinutes"] += nm
        if ev.get("numMinutes") is not None:
            m["had_minutes"] = True
    out: list[dict] = []
    for m in merged.values():
        row: dict[str, Any] = {
            "visitorId": m["visitorId"],
            dimension_key: m[dimension_key],
            "numEvents": m["numEvents"],
        }
        if m["had_minutes"] or m["numMinutes"] > 0:
            row["numMinutes"] = m["numMinutes"]
        out.append(row)
    return out


_FRUSTRATION_FIELDS = ("rageClickCount", "deadClickCount", "errorClickCount", "uTurnCount")


def _merge_visitor_event_rows_with_frustration(
    results: list[dict],
    dimension_key: str,
) -> list[dict]:
    """Merge day-bucket rows per (visitorId, dimension); sum frustration counters and events/minutes."""
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for ev in results:
        if not isinstance(ev, dict):
            continue
        vid = ev.get("visitorId")
        dim = ev.get(dimension_key)
        key = (str(vid), str(dim))
        ne = int(ev.get("numEvents") or 0)
        nm = int(ev.get("numMinutes") or 0)
        if key not in merged:
            merged[key] = {
                "visitorId": vid,
                dimension_key: dim,
                "numEvents": 0,
                "numMinutes": 0,
                "had_minutes": False,
                **{k: 0 for k in _FRUSTRATION_FIELDS},
            }
        m = merged[key]
        m["numEvents"] += ne
        m["numMinutes"] += nm
        if ev.get("numMinutes") is not None:
            m["had_minutes"] = True
        for fk in _FRUSTRATION_FIELDS:
            m[fk] += int(ev.get(fk) or 0)
    out: list[dict] = []
    for m in merged.values():
        row: dict[str, Any] = {
            "visitorId": m["visitorId"],
            dimension_key: m[dimension_key],
            "numEvents": m["numEvents"],
        }
        for fk in _FRUSTRATION_FIELDS:
            if m[fk]:
                row[fk] = m[fk]
        if m["had_minutes"] or m["numMinutes"] > 0:
            row["numMinutes"] = m["numMinutes"]
        out.append(row)
    return out


def _merge_guide_event_rows(results: list[dict]) -> list[dict]:
    """Merge guide event time buckets; preserve counts per (visitorId, guideId, type)."""
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for ev in results:
        if not isinstance(ev, dict):
            continue
        vid = ev.get("visitorId")
        gid = ev.get("guideId")
        typ = ev.get("type", "?")
        key = (str(vid), str(gid), str(typ))
        # Historically each row counted as one event; buckets may repeat with numEvents.
        n = int(ev.get("numEvents") or 0)
        inc = n if n > 0 else 1
        if key not in merged:
            merged[key] = {
                "visitorId": vid,
                "guideId": gid,
                "type": typ,
                "numEvents": 0,
            }
        merged[key]["numEvents"] += inc
    return list(merged.values())


def _aggregate_customer_page_events(
    events: list[dict],
    visitor_ids: set[str],
) -> dict[str, dict[str, int]]:
    page_counts: dict[str, dict[str, int]] = {}
    for ev in events:
        if ev.get("visitorId") not in visitor_ids:
            continue
        pid = ev.get("pageId", "")
        if pid not in page_counts:
            page_counts[pid] = {"events": 0, "minutes": 0}
        page_counts[pid]["events"] += int(ev.get("numEvents", 0) or 0)
        page_counts[pid]["minutes"] += int(ev.get("numMinutes", 0) or 0)
    return page_counts


def _aggregate_customer_feature_events(
    events: list[dict],
    visitor_ids: set[str],
) -> dict[str, int]:
    feat_counts: dict[str, int] = {}
    for ev in events:
        if ev.get("visitorId") not in visitor_ids:
            continue
        fid = ev.get("featureId", "")
        feat_counts[fid] = feat_counts.get(fid, 0) + int(ev.get("numEvents", 0) or 0)
    return feat_counts


def _feature_adoption_pattern_narrative(
    *,
    days: int,
    recent_days: int,
    prior_days: int,
    feat_full: dict[str, int],
    feat_recent: dict[str, int],
    feature_catalog: dict[str, str],
) -> str:
    """Short deterministic copy for the Feature Adoption slide (half-over-half)."""
    tf = sum(feat_full.values())
    tr = sum(feat_recent.values())
    tprior = tf - tr
    if tf <= 0:
        return ""

    def _short(nm: str) -> str:
        s = (nm or "").strip() or "?"
        return (s[:26] + "…") if len(s) > 26 else s

    parts: list[str] = []
    if tprior > 0:
        delta = round((tr - tprior) / tprior * 100)
        if abs(delta) >= 8:
            direction = "higher" if delta > 0 else "lower"
            parts.append(
                f"In the last {recent_days} days vs the prior {prior_days} days of this {days}-day window, "
                f"total feature clicks were {delta:+d}% {direction}."
            )
        else:
            parts.append(
                f"Feature click volume was similar in the last {recent_days} days vs the prior {prior_days} days."
            )
    elif tr > 0:
        parts.append(f"All recorded feature clicks in this window fell in the most recent {recent_days} days.")

    risers: list[str] = []
    fallers: list[str] = []
    for fid, total in sorted(feat_full.items(), key=lambda x: -x[1])[:15]:
        rec = feat_recent.get(fid, 0)
        prior = max(0, total - rec)
        if prior < 10 and rec < 10:
            continue
        if prior <= 0:
            if rec >= 15:
                risers.append(_short(feature_catalog.get(fid, fid)))
            continue
        ch = (rec - prior) / prior * 100
        label = _short(feature_catalog.get(fid, fid))
        if ch >= 28:
            risers.append(label)
        elif ch <= -28:
            fallers.append(label)

    risers = risers[:3]
    fallers = fallers[:3]
    if risers or fallers:
        bit = []
        if risers:
            bit.append(f"notably up: {', '.join(risers)}")
        if fallers:
            bit.append(f"softer: {', '.join(fallers)}")
        parts.append("Among top features — " + "; ".join(bit) + ".")
    elif parts:
        parts.append("No sharp half-over-half swings among leading features.")

    text = " ".join(parts).strip()
    if len(text) > 420:
        text = text[:417] + "…"
    return text


# Pendo metadata.agent.role values treated as executives (signals + KEI breakdown).
# Director/VP are excluded — they inflate counts on large accounts.
_EXECUTIVE_VISITOR_ROLES = frozenset({"C-Level", "Executive", "ExecutiveVP"})


def extract_customer_from_sitename(sitename: str) -> str:
    """Extract customer from sitename. Format is '{customer} {Site}' (e.g. 'Safran Ventilation Systems' -> 'Safran')."""
    if not sitename or not isinstance(sitename, str):
        return ""
    parts = sitename.strip().split()
    return parts[0] if parts else ""


# ── Cohort system ──

_cohort_data: dict[str, Any] | None = None
_alias_map: dict[str, str] = {}


def _load_cohorts() -> dict[str, Any]:
    global _cohort_data, _alias_map
    if _cohort_data is not None:
        return _cohort_data
    p = Path(__file__).resolve().parent.parent / "cohorts.yaml"
    if not p.exists():
        _cohort_data = {}
        return _cohort_data
    import yaml
    with open(p) as f:
        raw = yaml.safe_load(f) or {}
    _cohort_data = raw.get("cohorts", raw)
    _alias_map = {}
    for key, info in _cohort_data.items():
        if isinstance(info, dict):
            for alias in info.get("aliases", []):
                _alias_map[alias] = key
    return _cohort_data


def get_customer_cohort(customer_prefix: str) -> dict[str, Any]:
    """Look up cohort info for a customer prefix. Returns {} if not found."""
    data = _load_cohorts()
    canonical = _alias_map.get(customer_prefix, customer_prefix)
    info = data.get(canonical, {})
    if not isinstance(info, dict) or info.get("exclude"):
        return {}
    return info


def customer_is_excluded_from_portfolio(customer_prefix: str) -> bool:
    """True if portfolio/cohort rollup should not fetch a summary for this Pendo customer prefix.

    Uses the same **exclude: true** block in ``cohorts.yaml`` as benchmarking (see docs/CUSTOMER_COHORTS.md),
    plus optional env ``BPO_PORTFOLIO_EXCLUDE_CUSTOMERS`` (comma-separated prefixes, e.g. ``Automated,Foo``).
    Excluded names are dropped before ``get_customer_health`` so missing-visitor errors are not logged as ERROR.
    """
    raw = os.environ.get("BPO_PORTFOLIO_EXCLUDE_CUSTOMERS", "")
    extras = {x.strip() for x in raw.split(",") if x.strip()}
    if customer_prefix in extras:
        return True
    data = _load_cohorts()
    canonical = _alias_map.get(customer_prefix, customer_prefix)
    info = data.get(canonical, {})
    return isinstance(info, dict) and bool(info.get("exclude"))


_COHORT_DISPLAY = {
    "aerospace_defense": "Aerospace & Defense",
    "hvac_building": "HVAC & Building Systems",
    "vehicles": "Automotive & Vehicles",
    "medical_devices": "Medical Devices",
    "industrial_equipment": "Industrial Equipment",
    "electronics": "Electronics & Electrical",
    "advanced_materials": "Advanced Materials",
    "furniture": "Furniture & Office",
    "consumer_products": "Consumer Products",
}


def get_cohort_members(cohort: str) -> list[str]:
    """Return all customer prefixes belonging to a cohort (including aliases)."""
    data = _load_cohorts()
    members = []
    for key, info in data.items():
        if isinstance(info, dict) and not info.get("exclude") and info.get("cohort") == cohort:
            members.append(key)
            members.extend(info.get("aliases", []))
    return members


def _median_nums(vals: list[float]) -> float | None:
    nums = sorted(v for v in vals if isinstance(v, (int, float)) and v is not None)
    if not nums:
        return None
    mid = len(nums) // 2
    if len(nums) % 2:
        return round(float(nums[mid]), 1)
    return round((nums[mid - 1] + nums[mid]) / 2.0, 1)


def _cohort_findings_metadata_bullets(
    *,
    with_data: list[tuple[str, Any]],
    singletons: list[str],
    thin: list[str],
    thin_n: int,
    singleton_n: int,
    un: dict[str, Any],
    cfg: dict[str, Any],
) -> list[str]:
    """Build cohort metadata lines from ``cohort_findings`` metadata (YAML or built-in defaults)."""
    t = cfg.get("templates") or {}
    try:
        max_b = max(1, int(cfg.get("max_bullets") or 1))
    except (TypeError, ValueError):
        max_b = 1
    try:
        smb = max(1, int(cfg.get("singleton_list_max") or 8))
    except (TypeError, ValueError):
        smb = 8
    try:
        tmb = max(1, int(cfg.get("thin_list_max") or 6))
    except (TypeError, ValueError):
        tmb = 6
    priority = cfg.get("priority") or []
    out: list[str] = []
    for kind in priority:
        if len(out) >= max_b:
            break
        line: str | None = None
        if kind == "single_bucket":
            if len(with_data) < 2:
                line = (t.get("single_bucket") or "").strip() or None
        elif kind == "singleton":
            if singletons:
                names = ", ".join(singletons[:smb])
                ellipsis = "…" if len(singletons) > smb else ""
                if singleton_n == 1:
                    tmpl = t.get("singleton_one")
                    if tmpl:
                        line = tmpl.format(names=names, ellipsis=ellipsis)
                else:
                    tmpl = t.get("singleton_many")
                    if tmpl:
                        line = tmpl.format(singleton_n=singleton_n, names=names, ellipsis=ellipsis)
        elif kind == "thin_sample":
            if thin:
                names = ", ".join(thin[:tmb])
                ellipsis = "…" if len(thin) > tmb else ""
                tmpl = t.get("thin_sample")
                if tmpl:
                    line = tmpl.format(thin_n=thin_n, names=names, ellipsis=ellipsis)
        elif kind == "unclassified":
            if un.get("n"):
                tmpl = t.get("unclassified")
                if tmpl:
                    line = tmpl.format(n=int(un["n"]))
        elif kind == "provenance":
            line = (t.get("provenance") or "").strip() or None
        if line and line.strip():
            out.append(line.strip())
    return out


def compute_cohort_portfolio_rollup(
    customer_summaries: list[dict[str, Any]],
    *,
    use_cohort_findings_slide_yaml: bool = True,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Bucket portfolio rows by ``cohorts.yaml`` classification (via ``get_customer_cohort``).

    Does not define cohorts — only reads ``cohort`` from existing customer records.
    Returns ``(cohort_digest, findings_bullets)``.

    When ``use_cohort_findings_slide_yaml`` is False, cohort bullet tuning uses built-in defaults
    only (no ``slides/`` or Drive slide reads). Deck generation keeps the default True.
    """
    buckets: dict[str, list[dict[str, Any]]] = {}
    for s in customer_summaries:
        name = s.get("customer") or ""
        info = get_customer_cohort(name)
        cid = (info.get("cohort") or "").strip() if info else ""
        if not cid:
            cid = "unclassified"
        buckets.setdefault(cid, []).append(s)

    digest: dict[str, dict[str, Any]] = {}
    for cid, rows in buckets.items():
        if cid == "unclassified":
            display = "Unclassified / not in cohorts.yaml"
        else:
            display = _COHORT_DISPLAY.get(cid, cid.replace("_", " ").title())
        logins = [float(r.get("login_pct") or 0) for r in rows]
        writes = [float((r.get("depth") or {}).get("write_ratio") or 0) for r in rows]
        exports = [float((r.get("exports") or {}).get("total_exports") or 0) for r in rows]
        kei_yes = sum(1 for r in rows if (r.get("kei") or {}).get("total_queries", 0) > 0)
        n = len(rows)
        digest[cid] = {
            "cohort_id": cid,
            "display_name": display,
            "n": n,
            "customers": sorted(r.get("customer", "") for r in rows),
            "median_login_pct": _median_nums(logins),
            "median_write_ratio": _median_nums(writes),
            "median_exports": _median_nums(exports),
            "kei_adoption_pct": round(100.0 * kei_yes / n, 1) if n else 0.0,
            "total_active_users": sum(int(r.get("active_users") or 0) for r in rows),
            "total_users": sum(int(r.get("total_users") or 0) for r in rows),
        }

    bullets: list[str] = []
    with_data = [(cid, digest[cid]) for cid in digest if digest[cid]["n"] > 0]
    with_data.sort(key=lambda x: -x[1]["n"])
    if not customer_summaries:
        bullets.append("No customers in the portfolio window — rerun with a valid Pendo period.")
        return digest, bullets

    total_users_all = sum(int(s.get("total_users") or 0) for s in customer_summaries)
    total_active_all = sum(int(s.get("active_users") or 0) for s in customer_summaries)
    n_cust = len(customer_summaries)
    bullets.append(
        f"Portfolio (this window): {n_cust} customers · {total_users_all:,} total users · "
        f"{total_active_all:,} active (7d).",
    )

    def _fmt_pct(v: float | None) -> str:
        if v is None:
            return "—"
        return f"{round(float(v), 1)}%"

    def _fmt_num(v: float | None) -> str:
        if v is None:
            return "—"
        x = float(v)
        if abs(x - round(x)) < 1e-9:
            return str(int(round(x)))
        s = f"{x:.1f}".rstrip("0").rstrip(".")
        return s or "0"

    # Insight bullets: contrasts, outliers, and viewer-facing takeaways — not one stat line per cohort.
    if with_data:
        top = with_data[0][1]
        share = round(100.0 * top["n"] / n_cust, 1) if n_cust else 0.0
        bullets.append(
            f"Largest cohort: {top['display_name']} ({top['n']} customers, {share}% of portfolio).",
        )
        if len(with_data) > 1:
            second = with_data[1][1]
            s2 = round(100.0 * second["n"] / n_cust, 1) if n_cust else 0.0
            bullets.append(
                f"Next-largest: {second['display_name']} ({second['n']} customers, {s2}%).",
            )

    if use_cohort_findings_slide_yaml:
        rp = cohort_findings_rollup_params()
        meta_cfg = cohort_findings_metadata()
    else:
        from .slide_loader import cohort_findings_metadata_defaults, cohort_findings_rollup_defaults

        rp = cohort_findings_rollup_defaults()
        meta_cfg = cohort_findings_metadata_defaults()
    min_n_compare = max(1, rp["min_customers_for_cross_cohort_compare"])
    spread_min_pp = max(0, rp["min_login_spread_pp"])
    singleton_n = max(0, rp["singleton_n"])
    thin_n = max(0, rp["thin_sample_n"])
    ge_compare = [(cid, d) for cid, d in with_data if d["n"] >= min_n_compare]
    if len(ge_compare) >= 2:
        by_login = sorted(ge_compare, key=lambda x: (x[1].get("median_login_pct") or 0), reverse=True)
        hi, lo = by_login[0], by_login[-1]
        hiv = float(hi[1].get("median_login_pct") or 0)
        lov = float(lo[1].get("median_login_pct") or 0)
        spread = abs(hiv - lov)
        if hi[0] != lo[0] and spread >= spread_min_pp:
            bullets.append(
                f"Widest spread in median weekly login: {hi[1]['display_name']} ({_fmt_pct(hiv)}) vs "
                f"{lo[1]['display_name']} ({_fmt_pct(lov)}) — about {spread:.0f} points apart.",
            )
        by_write = sorted(ge_compare, key=lambda x: (x[1].get("median_write_ratio") or 0), reverse=True)
        w_hi, w_lo = by_write[0], by_write[-1]
        if w_hi[0] != w_lo[0]:
            bullets.append(
                f"Write-heavy vs read-heavy: {w_hi[1]['display_name']} leads write ratio "
                f"({_fmt_pct(w_hi[1].get('median_write_ratio'))}) vs lowest "
                f"{w_lo[1]['display_name']} ({_fmt_pct(w_lo[1].get('median_write_ratio'))}).",
            )
        if hi[0] != w_hi[0]:
            bullets.append(
                f"Different engagement modes: {hi[1]['display_name']} tops login median but "
                f"{w_hi[1]['display_name']} tops write ratio — worth a CS conversation on “why”.",
            )
        by_exp = sorted(ge_compare, key=lambda x: (x[1].get("median_exports") or 0), reverse=True)
        e_hi, e_lo = by_exp[0], by_exp[-1]
        if e_hi[0] != e_lo[0]:
            bullets.append(
                f"Export volume (median 30d) peaks in {e_hi[1]['display_name']} ({_fmt_num(e_hi[1].get('median_exports'))}) "
                f"and is lowest in {e_lo[1]['display_name']} ({_fmt_num(e_lo[1].get('median_exports'))}).",
            )
        by_kei = sorted(ge_compare, key=lambda x: x[1].get("kei_adoption_pct") or 0, reverse=True)
        k_hi, k_lo = by_kei[0], by_kei[-1]
        if k_hi[0] != k_lo[0]:
            kp = float(k_hi[1].get("kei_adoption_pct") or 0)
            kq = float(k_lo[1].get("kei_adoption_pct") or 0)
            bullets.append(
                f"Kei adoption gap: {k_hi[1]['display_name']} {kp:.1f}% of customers with any query vs "
                f"{k_lo[1]['display_name']} {kq:.1f}% — training, rollout, or use-case difference?",
            )

    un = digest.get("unclassified", {})
    if not isinstance(un, dict):
        un = {}
    singletons = [d["display_name"] for _, d in with_data if singleton_n > 0 and d["n"] == singleton_n]
    thin = [d["display_name"] for _, d in with_data if thin_n > 0 and d["n"] == thin_n]
    bullets.extend(
        _cohort_findings_metadata_bullets(
            with_data=with_data,
            singletons=singletons,
            thin=thin,
            thin_n=thin_n,
            singleton_n=singleton_n,
            un=un,
            cfg=meta_cfg,
        )
    )

    return digest, bullets


class PendoClient:
    """Client for Pendo aggregation API."""

    def __init__(
        self,
        integration_key: str | None = None,
        base_url: str | None = None,
    ):
        self.integration_key = integration_key or PENDO_INTEGRATION_KEY
        self.base_url = (base_url or PENDO_BASE_URL).rstrip("/")
        if not self.integration_key:
            raise ValueError(
                "Pendo integration key required. Set PENDO_INTEGRATION_KEY or pass integration_key."
            )
        # Session is not thread-safe; preload() uses a thread pool — one Session per thread.
        self._http_tls = threading.local()
        logger.debug("PendoClient initialized (base_url=%s)", self.base_url)

    def _http_session(self) -> requests.Session:
        s = getattr(self._http_tls, "session", None)
        if s is None:
            s = requests.Session()
            adapter = HTTPAdapter(pool_maxsize=4, max_retries=1)
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            self._http_tls.session = s
        return s

    def _headers(self) -> dict[str, str]:
        return {
            "X-Pendo-Integration-Key": self.integration_key,
            "Content-Type": "application/json",
        }

    def aggregate(self, pipeline: list[dict[str, Any]]) -> dict[str, Any]:
        """Execute an aggregation pipeline."""
        url = f"{self.base_url}/aggregation"
        logger.debug("Pendo API POST %s (pipeline steps=%d)", url, len(pipeline))
        payload = {
            "response": {"mimeType": "application/json"},
            "request": {
                "requestId": str(uuid4()),
                "pipeline": pipeline,
            },
        }
        resp = self._http_session().post(
            url, json=payload, headers=self._headers(),
            timeout=(10, PENDO_REQUEST_TIMEOUT_S),
        )
        resp.raise_for_status()
        data = resp.json()
        result_count = len(data.get("results", [])) if isinstance(data.get("results"), list) else "?"
        logger.debug("Pendo API response: %s results", result_count)
        return data

    def get_visitors(self, days: int = 30) -> dict[str, Any]:
        """Get visitor data for the last N days."""
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - (days * 24 * 60 * 60 * 1000)
        pipeline = [
            {
                "source": {
                    "visitors": {"startTime": start_ms, "endTime": end_ms}
                }
            }
        ]
        return self.aggregate(pipeline)

    def get_visitors_range(
        self,
        start_ms: int,
        end_ms: int,
        *,
        _timeout: tuple[int, float] | None = None,
    ) -> list[dict[str, Any]]:
        """Visitors with any activity in ``[start_ms, end_ms]`` (epoch ms). One aggregation call.

        Uses a one-off ``requests.post`` (not the shared session) so parallel trend fetches are safe.
        """
        if end_ms <= start_ms:
            return []
        connect_t, read_t = _timeout if _timeout is not None else (10, float(PENDO_REQUEST_TIMEOUT_S))
        url = f"{self.base_url}/aggregation"
        pipeline = [
            {
                "source": {
                    "visitors": {"startTime": int(start_ms), "endTime": int(end_ms)},
                }
            }
        ]
        payload = {
            "response": {"mimeType": "application/json"},
            "request": {
                "requestId": str(uuid4()),
                "pipeline": pipeline,
            },
        }
        resp = requests.post(
            url,
            json=payload,
            headers=self._headers(),
            timeout=(connect_t, read_t),
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results")
        return results if isinstance(results, list) else []

    def get_usage_for_customer(
        self, customer: str, days: int = 30, include_usage_metrics: bool = True
    ) -> dict[str, Any]:
        """Get usage data for a customer over the last N days.
        If no visitor/account matches, falls back to site name matching (e.g. 'Safran' -> 'Safran Ventilation Systems').
        """
        partition = self._get_visitor_partition(days)
        result = {"results": partition.get("all_visitors") or []}
        if "results" in result and isinstance(result["results"], list):
            result["results"] = [
                r
                for r in result["results"]
                if r.get("visitorId") == customer
                or r.get("accountId") == customer
                or (r.get("metadata") or {}).get("auto", {}).get("accountid") == customer
                or (customer in ((r.get("metadata") or {}).get("auto", {}).get("accountids") or []))
            ]
            # Fallback: if no match and customer looks like a site name (not numeric), try site matching
            if not result["results"] and not str(customer).strip().isdigit():
                site_result = self.get_usage_for_site(customer, days=days, include_usage_metrics=include_usage_metrics)
                if site_result.get("results"):
                    site_result["_matched_as"] = "site"
                    return site_result
        if include_usage_metrics and result.get("results"):
            metrics = self._get_usage_metrics_by_visitor(days)
            for r in result["results"]:
                vid = r.get("visitorId")
                if vid and vid in metrics:
                    r["usage"] = metrics[vid]
        return result

    def _get_usage_metrics_by_visitor(self, days: int) -> dict[str, dict[str, Any]]:
        """Fetch page/feature events and aggregate by visitorId."""
        metrics: dict[str, dict[str, Any]] = {}
        for source, key in [("pageEvents", "pageId"), ("featureEvents", "featureId")]:
            try:
                pipeline = [
                    {
                        "source": {
                            source: None,
                            "timeSeries": _time_series(days),
                        }
                    }
                ]
                data = self.aggregate(pipeline)
                for row in data.get("results") or []:
                    vid = str(row.get("visitorId", ""))
                    if not vid:
                        continue
                    if vid not in metrics:
                        metrics[vid] = {
                            "page_views": 0,
                            "feature_clicks": 0,
                            "total_events": 0,
                            "total_minutes": 0,
                            "unique_pages": set(),
                            "unique_features": set(),
                        }
                    m = metrics[vid]
                    n_events = row.get("numEvents", 0) or 0
                    n_mins = row.get("numMinutes", 0) or 0
                    m["total_events"] += n_events
                    m["total_minutes"] += n_mins
                    if source == "pageEvents":
                        m["page_views"] += n_events
                        if row.get(key):
                            m["unique_pages"].add(row[key])
                    else:
                        m["feature_clicks"] += n_events
                        if row.get(key):
                            m["unique_features"].add(row[key])
            except Exception as e:
                logger.debug("Could not fetch %s for usage metrics: %s", source, e)
        # Convert sets to counts for JSON
        for m in metrics.values():
            m["unique_pages"] = len(m["unique_pages"])
            m["unique_features"] = len(m["unique_features"])
        return metrics

    def get_sites(self, days: int = 30) -> dict[str, Any]:
        """Get unique sites from visitor metadata (metadata.agent.siteid/sitename).

        Uses the cached visitor partition when available (populated by ``preload``),
        falling back to a raw ``get_visitors`` call only if the cache is cold.
        """
        partition = self._get_visitor_partition(days)
        all_visitors = partition.get("all_visitors") or []
        sites: dict[int, dict[str, Any]] = {}
        for r in all_visitors:
            agent = (r.get("metadata") or {}).get("agent") or {}
            site_ids = agent.get("siteids") or ([agent["siteid"]] if agent.get("siteid") is not None else [])
            site_names = agent.get("sitenames") or ([agent["sitename"]] if agent.get("sitename") else [])
            for i, sid in enumerate(site_ids):
                if sid is not None:
                    sites[int(sid)] = {
                        "siteid": sid,
                        "sitename": site_names[i] if i < len(site_names) else str(sid),
                    }
        # Sort by siteid for consistent output
        def _sort_key(s: dict) -> int | str:
            sid = s["siteid"]
            return int(sid) if isinstance(sid, (int, float)) or (isinstance(sid, str) and sid.isdigit()) else str(sid)
        sorted_sites = sorted(sites.values(), key=_sort_key)
        return {"results": sorted_sites, "total": len(sites)}

    def get_usage_for_site(
        self, site: str | int, days: int = 30, include_usage_metrics: bool = True
    ) -> dict[str, Any]:
        """Get usage data for visitors in a site (by site ID or site name)."""
        partition = self._get_visitor_partition(days)
        result = {"results": partition.get("all_visitors") or []}
        site_str = str(site).strip()
        site_int = int(site) if site_str.isdigit() else None

        def _matches(v: dict[str, Any]) -> bool:
            agent = (v.get("metadata") or {}).get("agent") or {}
            if site_int is not None:
                if agent.get("siteid") == site_int:
                    return True
                for sid in agent.get("siteids") or []:
                    if sid == site_int:
                        return True
            name = agent.get("sitename") or ""
            names = agent.get("sitenames") or []
            site_lower = site_str.lower()
            # Exact match
            if name and name == site_str:
                return True
            if site_str in names:
                return True
            # Partial match: site name contains query (e.g. "Safran" matches "Safran Ventilation Systems")
            if name and site_lower in name.lower():
                return True
            for n in names:
                if n and site_lower in str(n).lower():
                    return True
            return False

        if "results" in result and isinstance(result["results"], list):
            result["results"] = [r for r in result["results"] if _matches(r)]
        if include_usage_metrics and result.get("results"):
            metrics = self._get_usage_metrics_by_visitor(days)
            for r in result["results"]:
                vid = r.get("visitorId")
                if vid and vid in metrics:
                    r["usage"] = metrics[vid]
        return result

    def get_usage_by_site(self, days: int = 30) -> dict[str, Any]:
        """Get usage aggregated by site using Pendo's native aggregation (page views, feature clicks, events, minutes)."""
        site_field = "properties.__sg__.visitormetadata.agent__sitename"
        ts = _time_series(days)

        def _pipeline(source: str) -> list[dict[str, Any]]:
            return [
                {"source": {source: None, "timeSeries": ts}},
                {"select": {"numEvents": "numEvents", "numMinutes": "numMinutes", "sitename": site_field}},
                {
                    "group": {
                        "group": ["sitename"],
                        "fields": {"totalEvents": {"sum": "numEvents"}, "totalMinutes": {"sum": "numMinutes"}},
                    }
                },
            ]

        page_data = self.aggregate(_pipeline("pageEvents"))
        feature_data = self.aggregate(_pipeline("featureEvents"))

        by_site: dict[str, dict[str, Any]] = {}
        for row in page_data.get("results") or []:
            site = row.get("sitename") or "(unknown)"
            by_site[site] = {
                "sitename": site,
                "page_views": row.get("totalEvents", 0),
                "feature_clicks": 0,
                "total_events": row.get("totalEvents", 0),
                "total_minutes": row.get("totalMinutes", 0),
            }
        for row in feature_data.get("results") or []:
            site = row.get("sitename") or "(unknown)"
            if site not in by_site:
                by_site[site] = {"sitename": site, "page_views": 0, "feature_clicks": 0, "total_events": 0, "total_minutes": 0}
            by_site[site]["feature_clicks"] = row.get("totalEvents", 0)
            by_site[site]["total_events"] += row.get("totalEvents", 0)
            by_site[site]["total_minutes"] += row.get("totalMinutes", 0)
        sorted_sites = sorted(by_site.values(), key=lambda s: (-s["total_events"], s["sitename"]))
        return {"results": sorted_sites, "total": len(by_site)}

    def get_usage_by_site_and_entity(self, days: int = 30) -> dict[str, Any]:
        """Get usage aggregated by site and entity (page views, feature clicks, events, minutes).
        Uses metadata.agent sitename and entity at event time. Rows without entity use empty string.
        """
        site_field = "properties.__sg__.visitormetadata.agent__sitename"
        entity_field = "properties.__sg__.visitormetadata.agent__entity"
        ts = _time_series(days)

        def _pipeline(source: str) -> list[dict[str, Any]]:
            return [
                {"source": {source: None, "timeSeries": ts}},
                {
                    "select": {
                        "numEvents": "numEvents",
                        "numMinutes": "numMinutes",
                        "sitename": site_field,
                        "entity": entity_field,
                    }
                },
                {
                    "group": {
                        "group": ["sitename", "entity"],
                        "fields": {"totalEvents": {"sum": "numEvents"}, "totalMinutes": {"sum": "numMinutes"}},
                    }
                },
            ]

        page_data = self.aggregate(_pipeline("pageEvents"))
        feature_data = self.aggregate(_pipeline("featureEvents"))

        by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for row in page_data.get("results") or []:
            site = (row.get("sitename") or "").strip() or "(unknown)"
            entity = (row.get("entity") or "").strip() if row.get("entity") else ""
            key = (site, entity)
            by_key[key] = {
                "sitename": site,
                "entity": entity,
                "page_views": row.get("totalEvents", 0),
                "feature_clicks": 0,
                "total_events": row.get("totalEvents", 0),
                "total_minutes": row.get("totalMinutes", 0),
            }
        for row in feature_data.get("results") or []:
            site = (row.get("sitename") or "").strip() or "(unknown)"
            entity = (row.get("entity") or "").strip() if row.get("entity") else ""
            key = (site, entity)
            if key not in by_key:
                by_key[key] = {"sitename": site, "entity": entity, "page_views": 0, "feature_clicks": 0, "total_events": 0, "total_minutes": 0}
            by_key[key]["feature_clicks"] = row.get("totalEvents", 0)
            by_key[key]["total_events"] += row.get("totalEvents", 0)
            by_key[key]["total_minutes"] += row.get("totalMinutes", 0)
        sorted_results = sorted(by_key.values(), key=lambda s: (-s["total_events"], s["sitename"], s["entity"]))
        return {"results": sorted_results, "total": len(by_key)}

    def get_all_sites_usage_report(
        self, days: int = 30, active_only: bool = False
    ) -> dict[str, Any]:
        """Get all sites (from visitor metadata) with usage data for each.
        active_only=True filters to sites with total_events > 0 (had page/feature activity).
        """
        all_sites = self.get_sites(days=days)
        usage_by_site = self.get_usage_by_site(days=days)
        usage_map = {r["sitename"]: r for r in (usage_by_site.get("results") or [])}
        report = []
        for s in all_sites.get("results") or []:
            sitename = s.get("sitename", "")
            siteid = s.get("siteid")
            usage = usage_map.get(sitename) or {
                "page_views": 0,
                "feature_clicks": 0,
                "total_events": 0,
                "total_minutes": 0,
            }
            row = {
                "siteid": siteid,
                "sitename": sitename,
                "customer": extract_customer_from_sitename(sitename),
                "page_views": usage.get("page_views", 0),
                "feature_clicks": usage.get("feature_clicks", 0),
                "total_events": usage.get("total_events", 0),
                "total_minutes": usage.get("total_minutes", 0),
            }
            if active_only and row["total_events"] == 0:
                continue
            report.append(row)
        report.sort(key=lambda r: (-r["total_events"], r["sitename"] or ""))
        return {"results": report, "total": len(report)}

    def get_sites_by_customer(
        self, days: int = 30, active_only: bool = False
    ) -> dict[str, Any]:
        """Get sites grouped by customer. Site names follow '{customer} {Site}' format.
        Returns {customers: {customer: [sites...]}, customer_list: [...]} for pipeline use
        (e.g. Google Slide per site, Slack per customer).
        """
        report = self.get_all_sites_usage_report(days=days, active_only=active_only)
        by_customer: dict[str, list[dict[str, Any]]] = {}
        for row in report.get("results") or []:
            cust = row.get("customer") or "(unknown)"
            if cust not in by_customer:
                by_customer[cust] = []
            by_customer[cust].append(row)
        for sites in by_customer.values():
            sites.sort(key=lambda r: (-r["total_events"], r["sitename"] or ""))
        customer_list = sorted(by_customer.keys(), key=lambda c: (c == "(unknown)", c))
        return {
            "by_customer": by_customer,
            "customer_list": customer_list,
            "total_sites": report.get("total", 0),
        }

    def get_sites_with_usage(self, days: int = 30) -> dict[str, Any]:
        """Get sites with visitor count and usage summary from the last N days."""
        partition = self._get_visitor_partition(days)
        all_visitors = partition.get("all_visitors") or []
        sites: dict[int, dict[str, Any]] = {}
        for r in all_visitors:
            agent = (r.get("metadata") or {}).get("agent") or {}
            auto = (r.get("metadata") or {}).get("auto") or {}
            site_ids = agent.get("siteids") or ([agent["siteid"]] if agent.get("siteid") is not None else [])
            site_names = agent.get("sitenames") or ([agent["sitename"]] if agent.get("sitename") else [])
            last_visit = auto.get("lastvisit") or auto.get("lastVisit")
            for i, sid in enumerate(site_ids):
                if sid is not None:
                    key = int(sid) if isinstance(sid, (int, float)) or (isinstance(sid, str) and str(sid).isdigit()) else sid
                    if key not in sites:
                        sites[key] = {
                            "siteid": sid,
                            "sitename": site_names[i] if i < len(site_names) else str(sid),
                            "visitor_count": 0,
                            "last_visit": None,
                        }
                    sites[key]["visitor_count"] += 1
                    if last_visit and (sites[key]["last_visit"] is None or last_visit > sites[key]["last_visit"]):
                        sites[key]["last_visit"] = last_visit
        def _sort_key(s: dict) -> int | str:
            sid = s["siteid"]
            return int(sid) if isinstance(sid, (int, float)) or (isinstance(sid, str) and str(sid).isdigit()) else str(sid)
        sorted_sites = sorted(sites.values(), key=lambda s: (-s["visitor_count"], _sort_key(s)))
        return {"results": sorted_sites, "total": len(sites)}

    def get_page_events(self, days: int = 30) -> dict[str, Any]:
        """Get page view/event usage data for the last N days."""
        pipeline = [
            {
                "source": {
                    "pageEvents": None,
                    "timeSeries": _time_series(days),
                }
            }
        ]
        return self.aggregate(pipeline)

    def get_feature_events(self, days: int = 30) -> dict[str, Any]:
        """Get feature click/usage events for the last N days."""
        pipeline = [
            {
                "source": {
                    "featureEvents": None,
                    "timeSeries": _time_series(days),
                }
            }
        ]
        return self.aggregate(pipeline)

    def get_track_events(
        self, days: int = 30, event_class: list[str] | None = None
    ) -> dict[str, Any]:
        """Get custom track events (web, ios, android, etc.) for the last N days."""
        event_class = event_class or ["web"]
        pipeline = [
            {
                "source": {
                    "events": {"eventClass": event_class},
                    "timeSeries": _time_series(days),
                }
            }
        ]
        return self.aggregate(pipeline)

    def list_accounts(self) -> dict[str, Any]:
        """All accounts from aggregation ``accounts`` source (metadata per account)."""
        pipeline = [{"source": {"accounts": None}}]
        return self.aggregate(pipeline)

    def get_customer_frustration_signals(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Rage/dead/error clicks and U-turns on page and feature events for a customer."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}
        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        ts = _time_series(days)
        try:
            raw_page = self.aggregate([
                {"source": {"pageEvents": None, "timeSeries": ts}},
            ]).get("results") or []
            page_rows = _merge_visitor_event_rows_with_frustration(raw_page, "pageId")
            raw_feat = self.aggregate([
                {"source": {"featureEvents": None, "timeSeries": ts}},
            ]).get("results") or []
            feat_rows = _merge_visitor_event_rows_with_frustration(raw_feat, "featureId")
        except Exception as e:
            return {"error": str(e), "customer": customer_name, "days": days}

        page_catalog = self._get_page_catalog_cached()
        feature_catalog = self._get_feature_catalog_cached()

        totals = {k: 0 for k in _FRUSTRATION_FIELDS}
        top_pages: list[dict[str, Any]] = []
        top_features: list[dict[str, Any]] = []

        page_scores: dict[str, dict[str, int]] = {}
        for ev in page_rows:
            if ev.get("visitorId") not in visitor_ids:
                continue
            pid = str(ev.get("pageId") or "")
            if pid not in page_scores:
                page_scores[pid] = {k: 0 for k in _FRUSTRATION_FIELDS}
            for fk in _FRUSTRATION_FIELDS:
                n = int(ev.get(fk) or 0)
                page_scores[pid][fk] += n
                totals[fk] += n

        for pid, scores in sorted(
            page_scores.items(),
            key=lambda x: sum(x[1].values()),
            reverse=True,
        )[:12]:
            if sum(scores.values()) <= 0:
                continue
            row = {"page": page_catalog.get(pid, pid), "page_id": pid}
            row.update(scores)
            top_pages.append(row)

        feat_scores: dict[str, dict[str, int]] = {}
        for ev in feat_rows:
            if ev.get("visitorId") not in visitor_ids:
                continue
            fid = str(ev.get("featureId") or "")
            if fid not in feat_scores:
                feat_scores[fid] = {k: 0 for k in _FRUSTRATION_FIELDS}
            for fk in _FRUSTRATION_FIELDS:
                n = int(ev.get(fk) or 0)
                feat_scores[fid][fk] += n
                totals[fk] += n

        for fid, scores in sorted(
            feat_scores.items(),
            key=lambda x: sum(x[1].values()),
            reverse=True,
        )[:12]:
            if sum(scores.values()) <= 0:
                continue
            row = {"feature": feature_catalog.get(fid, fid), "feature_id": fid}
            row.update(scores)
            top_features.append(row)

        total_signal = sum(totals.values())
        return {
            "customer": customer_name,
            "days": days,
            "totals": totals,
            "total_frustration_signals": total_signal,
            "top_pages": top_pages,
            "top_features": top_features,
        }

    def get_customer_poll_events(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """NPS and poll responses (``pollEvents``) for a customer."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}
        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        ts = _time_series(days)
        try:
            raw = self.aggregate([
                {"source": {"pollEvents": None, "timeSeries": ts}},
            ]).get("results") or []
        except Exception as e:
            return {"error": str(e), "customer": customer_name, "days": days}

        responses: list[dict[str, Any]] = []
        by_type: dict[str, int] = {}
        nps_scores: list[int] = []
        for ev in raw:
            if ev.get("visitorId") not in visitor_ids:
                continue
            poll_type = str(ev.get("pollType") or ev.get("type") or "?")
            pr = ev.get("pollResponse")
            by_type[poll_type] = by_type.get(poll_type, 0) + 1
            row = {
                "poll_id": ev.get("pollId"),
                "poll_type": poll_type,
                "poll_response": pr,
            }
            responses.append(row)
            if poll_type == "NPSRating" and isinstance(pr, (int, float)):
                nps_scores.append(int(pr))

        out: dict[str, Any] = {
            "customer": customer_name,
            "days": days,
            "response_count": len(responses),
            "by_poll_type": dict(sorted(by_type.items(), key=lambda x: -x[1])),
            "responses": responses[:50],
        }
        if nps_scores:
            nps_scores.sort()
            mid = len(nps_scores) // 2
            median = (
                nps_scores[mid]
                if len(nps_scores) % 2
                else (nps_scores[mid - 1] + nps_scores[mid]) / 2
            )
            out["nps"] = {
                "count": len(nps_scores),
                "median": float(median),
                "avg": round(sum(nps_scores) / len(nps_scores), 2),
            }
        return out

    def get_customer_track_events_breakdown(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Custom track events (``events`` source), grouped by track type name for a customer."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}
        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        try:
            rows = self._get_track_events_cached(days)
        except Exception as e:
            return {"error": str(e), "customer": customer_name, "days": days}

        by_track: dict[str, dict[str, Any]] = {}
        for ev in rows:
            if ev.get("visitorId") not in visitor_ids:
                continue
            name = str(ev.get("pageId") or "(unknown)")
            ne = int(ev.get("numEvents") or 0) or 0
            nm = int(ev.get("numMinutes") or 0) or 0
            if name not in by_track:
                by_track[name] = {"events": 0, "minutes": 0, "users": set()}
            by_track[name]["events"] += ne
            by_track[name]["minutes"] += nm
            by_track[name]["users"].add(ev.get("visitorId"))

        breakdown = []
        for name, info in sorted(by_track.items(), key=lambda x: -x[1]["events"])[:40]:
            breakdown.append({
                "track_name": name,
                "events": info["events"],
                "minutes": info["minutes"],
                "unique_users": len(info["users"]),
            })
        return {
            "customer": customer_name,
            "days": days,
            "distinct_track_types": len(by_track),
            "breakdown": breakdown,
        }

    def get_customer_visitor_languages(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Language distribution from visitor ``metadata.agent.language`` for a customer."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}
        counts: dict[str, int] = {}
        for v in customer_visitors:
            lang = (((v.get("metadata") or {}).get("agent") or {}).get("language") or "").strip() or "(unset)"
            counts[lang] = counts.get(lang, 0) + 1
        ranked = sorted(counts.items(), key=lambda x: -x[1])
        return {
            "customer": customer_name,
            "days": days,
            "total_visitors": len(customer_visitors),
            "languages": [{"language": k, "users": v} for k, v in ranked],
        }

    def get_tracktype_catalog_list(self) -> list[dict[str, Any]]:
        """REST ``GET /tracktype`` — track event type definitions."""
        resp = self._http_session().get(
            f"{self.base_url}/tracktype",
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def get_report_catalog_list(self) -> list[dict[str, Any]]:
        """REST ``GET /report`` — saved report definitions (not computed results)."""
        resp = self._http_session().get(
            f"{self.base_url}/report",
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def get_segment_catalog_list(self) -> list[dict[str, Any]]:
        """REST ``GET /segment`` — segment definitions."""
        resp = self._http_session().get(
            f"{self.base_url}/segment",
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def get_metadata_schema_visitor_raw(self) -> dict[str, Any]:
        """REST ``GET /metadata/schema/visitor`` — configured visitor metadata fields."""
        resp = self._http_session().get(
            f"{self.base_url}/metadata/schema/visitor",
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}

    def get_metadata_schema_account_raw(self) -> dict[str, Any]:
        """REST ``GET /metadata/schema/account`` — configured account metadata fields."""
        resp = self._http_session().get(
            f"{self.base_url}/metadata/schema/account",
            headers=self._headers(),
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}

    def get_pendo_catalog_appendix_summary(self, *, sample_n: int = 14) -> dict[str, Any]:
        """Totals plus sample names from Track Type, Segment, and Report REST catalogs (definitions only)."""
        try:
            tt = self.get_tracktype_catalog_list()
            seg = self.get_segment_catalog_list()
            rep = self.get_report_catalog_list()
        except Exception as e:
            return {"error": str(e)}

        def _sample_names(lst: list) -> list[str]:
            out: list[str] = []
            for x in (lst or [])[:sample_n]:
                if isinstance(x, dict):
                    nm = x.get("name") or x.get("trackTypeName") or x.get("id") or "?"
                    out.append(str(nm)[:56])
            return out

        return {
            "tracktype_total": len(tt),
            "segment_total": len(seg),
            "report_total": len(rep),
            "tracktype_sample_names": _sample_names(tt),
            "segment_sample_names": _sample_names(seg),
            "report_sample_names": _sample_names(rep),
        }

    # ── Catalog methods (for human-readable names) ──

    def get_page_catalog(self) -> dict[str, str]:
        """Fetch page catalog: {page_id: page_name}."""
        resp = self._http_session().get(
            f"{self.base_url}/page", headers=self._headers(), timeout=30
        )
        resp.raise_for_status()
        pages = resp.json()
        return {p["id"]: p.get("name", p["id"]) for p in pages} if isinstance(pages, list) else {}

    def get_feature_catalog(self) -> dict[str, str]:
        """Fetch feature catalog: {feature_id: feature_name}."""
        resp = self._http_session().get(
            f"{self.base_url}/feature", headers=self._headers(), timeout=30
        )
        resp.raise_for_status()
        features = resp.json()
        return {f["id"]: f.get("name", f["id"]) for f in features} if isinstance(features, list) else {}

    def _get_feature_catalog_cached(self) -> dict[str, str]:
        """Like page/guide catalogs — cache across preloads so QBR + portfolio snapshot don't refetch."""
        with self._cache_lock:
            if self._feature_catalog_cache is not None and self._cache_valid(self._feature_catalog_cache_ts):
                return self._feature_catalog_cache
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_FEATURE_CATALOG,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_FEATURE_CATALOG, None)
        if isinstance(blob, dict):
            with self._cache_lock:
                self._feature_catalog_cache = blob
                self._feature_catalog_cache_ts = time.time()
            return blob
        result = self.get_feature_catalog()
        with self._cache_lock:
            self._feature_catalog_cache = result
            self._feature_catalog_cache_ts = time.time()
        save_pendo_preload_payload(PRELOAD_KIND_FEATURE_CATALOG, None, result)
        return result

    def get_account_info(self, account_id: str) -> dict[str, Any]:
        """Fetch account metadata from REST API."""
        resp = self._http_session().get(
            f"{self.base_url}/account/{account_id}",
            headers=self._headers(),
            timeout=10,
        )
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()

    # ── Global cache (all caches share a single TTL, extended in batch mode) ──

    _visitor_cache: dict[str, Any] | None = None
    _visitor_cache_ts: float = 0
    _CACHE_TTL = BPO_PENDO_CACHE_TTL_SECONDS  # seconds; overridden by preload() for batch runs
    _page_events_cache: dict[str, Any] | None = None
    _page_events_cache_ts: float = 0
    _track_events_cache: dict[str, Any] | None = None
    _track_events_cache_ts: float = 0
    _guide_events_cache: dict[str, Any] | None = None
    _guide_events_cache_ts: float = 0
    _page_catalog_cache: dict[str, str] | None = None
    _guide_catalog_cache: dict[str, str] | None = None
    _feature_catalog_cache: dict[str, str] | None = None
    _feature_catalog_cache_ts: float = 0
    _usage_by_site_cache: dict[str, Any] | None = None
    _usage_by_site_cache_ts: float = 0
    _usage_by_site_entity_cache: dict[str, Any] | None = None
    _usage_by_site_entity_cache_ts: float = 0
    _cache_lock = threading.Lock()

    def _cache_valid(self, ts: float) -> bool:
        if self._CACHE_TTL <= 0:
            return False
        return (time.time() - ts) < self._CACHE_TTL

    @staticmethod
    def _visitor_is_internal(v: dict) -> bool:
        agent = (v.get("metadata") or {}).get("agent") or {}
        return bool(agent.get("isinternaluser")) or agent.get("role") == "LeanDNAStaff"

    def _visitor_partition_attach_callable(self, payload: dict[str, Any]) -> dict[str, Any]:
        out = dict(payload)
        out["_is_internal"] = self._visitor_is_internal
        return out

    def _get_visitor_partition(self, days: int = 30) -> dict[str, Any]:
        """Fetch all visitors and partition by customer. Cached for 120s to avoid
        redundant API calls when the agent invokes multiple tools in sequence."""
        with self._cache_lock:
            if (
                self._visitor_cache
                and self._cache_valid(self._visitor_cache_ts)
            ):
                cached_days = self._visitor_cache.get("days")
                if cached_days == days:
                    return self._visitor_cache

        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_VISITORS,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_VISITORS, days)
        if isinstance(blob, dict) and "all_visitors" in blob and "all_customer_stats" in blob:
            result = self._visitor_partition_attach_callable(blob)
            with self._cache_lock:
                self._visitor_cache = result
                self._visitor_cache_ts = time.time()
            return result

        now_ms = int(time.time() * 1000)
        all_visitors = self.get_visitors(days=days).get("results", [])

        all_customer_stats: dict[str, dict] = {}
        for v in all_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            if self._visitor_is_internal(v):
                continue
            sitenames = agent.get("sitenames") or []
            lv = auto.get("lastvisit", 0)
            for sn in sitenames:
                cust = str(sn).strip().split()[0] if sn else "?"
                if cust not in all_customer_stats:
                    all_customer_stats[cust] = {"total": 0, "active_7d": 0}
                all_customer_stats[cust]["total"] += 1
                if lv and (now_ms - lv) / (86400 * 1000) <= 7:
                    all_customer_stats[cust]["active_7d"] += 1
                break

        result = {
            "days": days,
            "now_ms": now_ms,
            "all_visitors": all_visitors,
            "all_customer_stats": all_customer_stats,
            "_is_internal": self._visitor_is_internal,
        }
        with self._cache_lock:
            self._visitor_cache = result
            self._visitor_cache_ts = time.time()
        store = {k: v for k, v in result.items() if k != "_is_internal"}
        save_pendo_preload_payload(PRELOAD_KIND_VISITORS, days, store)
        return result

    def _get_page_events_cached(self, days: int) -> list[dict]:
        with self._cache_lock:
            if self._page_events_cache and self._cache_valid(self._page_events_cache_ts):
                if self._page_events_cache.get("days") == days:
                    return self._page_events_cache["results"]
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_PAGE_EVENTS,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_PAGE_EVENTS, days)
        if isinstance(blob, list):
            with self._cache_lock:
                self._page_events_cache = {"days": days, "results": blob}
                self._page_events_cache_ts = time.time()
            return blob
        ts = _time_series(days)
        raw = self.aggregate([
            {"source": {"pageEvents": None, "timeSeries": ts}},
        ]).get("results", [])
        results = _merge_visitor_event_rows_by_dimension(raw, "pageId")
        with self._cache_lock:
            self._page_events_cache = {"days": days, "results": results}
            self._page_events_cache_ts = time.time()
        save_pendo_preload_payload(PRELOAD_KIND_PAGE_EVENTS, days, results)
        return results

    def _get_track_events_cached(self, days: int) -> list[dict]:
        with self._cache_lock:
            if self._track_events_cache and self._cache_valid(self._track_events_cache_ts):
                if self._track_events_cache.get("days") == days:
                    return self._track_events_cache["results"]
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_TRACK_EVENTS,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_TRACK_EVENTS, days)
        if isinstance(blob, list):
            with self._cache_lock:
                self._track_events_cache = {"days": days, "results": blob}
                self._track_events_cache_ts = time.time()
            return blob
        # Only *get_customer_kei* uses this list — it keeps rows with "kei" in pageId. A full
        # subscription extract with ``events: None`` is enormous and slow; restrict to the
        # platform classes Kei can fire on (same spirit as get_track_events, but multi-surface).
        ts = _time_series(days)
        raw = self.aggregate([
            {
                "source": {
                    "events": {"eventClass": ["web", "ios", "android"]},
                    "timeSeries": ts,
                }
            },
        ]).get("results", [])
        results = _merge_visitor_event_rows_by_dimension(raw, "pageId")
        with self._cache_lock:
            self._track_events_cache = {"days": days, "results": results}
            self._track_events_cache_ts = time.time()
        save_pendo_preload_payload(PRELOAD_KIND_TRACK_EVENTS, days, results)
        return results

    def _get_guide_events_cached(self, days: int) -> list[dict]:
        with self._cache_lock:
            if self._guide_events_cache and self._cache_valid(self._guide_events_cache_ts):
                if self._guide_events_cache.get("days") == days:
                    return self._guide_events_cache["results"]
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_GUIDE_EVENTS,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_GUIDE_EVENTS, days)
        if isinstance(blob, list):
            with self._cache_lock:
                self._guide_events_cache = {"days": days, "results": blob}
                self._guide_events_cache_ts = time.time()
            return blob
        ts = _time_series(days)
        raw = self.aggregate([
            {"source": {"guideEvents": None, "timeSeries": ts}},
        ]).get("results", [])
        results = _merge_guide_event_rows(raw)
        with self._cache_lock:
            self._guide_events_cache = {"days": days, "results": results}
            self._guide_events_cache_ts = time.time()
        save_pendo_preload_payload(PRELOAD_KIND_GUIDE_EVENTS, days, results)
        return results

    def _get_page_catalog_cached(self) -> dict[str, str]:
        with self._cache_lock:
            if self._page_catalog_cache is not None and self._CACHE_TTL > 0:
                return self._page_catalog_cache
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_PAGE_CATALOG,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_PAGE_CATALOG, None)
        if isinstance(blob, dict):
            with self._cache_lock:
                self._page_catalog_cache = blob
            return blob
        result = self.get_page_catalog()
        with self._cache_lock:
            self._page_catalog_cache = result
        save_pendo_preload_payload(PRELOAD_KIND_PAGE_CATALOG, None, result)
        return result

    def _get_guide_catalog_cached(self) -> dict[str, str]:
        with self._cache_lock:
            if self._guide_catalog_cache is not None and self._CACHE_TTL > 0:
                return self._guide_catalog_cache
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_GUIDE_CATALOG,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_GUIDE_CATALOG, None)
        if isinstance(blob, dict):
            with self._cache_lock:
                self._guide_catalog_cache = blob
            return blob
        try:
            resp = self._http_session().get(
                f"{self.base_url}/guide",
                headers={"x-pendo-integration-key": self.integration_key, "content-type": "application/json"},
                timeout=30,
            )
            result = {g["id"]: g.get("name", g["id"]) for g in resp.json()} if resp.ok else {}
        except Exception:
            result = {}
        with self._cache_lock:
            self._guide_catalog_cache = result
        save_pendo_preload_payload(PRELOAD_KIND_GUIDE_CATALOG, None, result)
        return result

    def _get_usage_by_site_cached(self, days: int) -> dict[str, Any]:
        with self._cache_lock:
            if self._usage_by_site_cache and self._cache_valid(self._usage_by_site_cache_ts):
                if self._usage_by_site_cache.get("days") == days:
                    return self._usage_by_site_cache
        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_USAGE_BY_SITE,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_USAGE_BY_SITE, days)
        if isinstance(blob, dict) and blob.get("days") == days:
            with self._cache_lock:
                self._usage_by_site_cache = blob
                self._usage_by_site_cache_ts = time.time()
            return blob
        result = self.get_usage_by_site(days=days)
        result["days"] = days
        with self._cache_lock:
            self._usage_by_site_cache = result
            self._usage_by_site_cache_ts = time.time()
        save_pendo_preload_payload(PRELOAD_KIND_USAGE_BY_SITE, days, result)
        return result

    def _get_usage_by_site_entity_cached(self, days: int) -> dict[str, Any]:
        with self._cache_lock:
            if self._usage_by_site_entity_cache and self._cache_valid(self._usage_by_site_entity_cache_ts):
                if self._usage_by_site_entity_cache.get("days") == days:
                    return self._usage_by_site_entity_cache
        result = self.get_usage_by_site_and_entity(days=days)
        result["days"] = days
        with self._cache_lock:
            self._usage_by_site_entity_cache = result
            self._usage_by_site_entity_cache_ts = time.time()
        return result

    def preload(self, days: int = 30) -> None:
        """Prefetch all global data for a batch run. Sets TTL to 1 hour.
        Fetches all data sources in parallel to minimize wall-clock time."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        PendoClient._CACHE_TTL = 3600
        logger.info("Pendo: preloading global data for %d-day window (parallel)...", days)
        t0 = time.time()

        loaders = {
            "visitors": lambda: self._get_visitor_partition(days),
            "feature events": lambda: self._get_feature_events_cached(days),
            "page events": lambda: self._get_page_events_cached(days),
            "track events": lambda: self._get_track_events_cached(days),
            "guide events": lambda: self._get_guide_events_cached(days),
            "page catalog": lambda: self._get_page_catalog_cached(),
            "feature catalog": lambda: self._get_feature_catalog_cached(),
            "guide catalog": lambda: self._get_guide_catalog_cached(),
            "usage by site": lambda: self._get_usage_by_site_cached(days),
        }

        with ThreadPoolExecutor(max_workers=len(loaders)) as pool:
            futures: dict[Any, str] = {}
            started: dict[Any, float] = {}
            for name, fn in loaders.items():
                fut = pool.submit(fn)
                futures[fut] = name
                started[fut] = time.time()
            for fut in as_completed(futures):
                name = futures[fut]
                elapsed = time.time() - started[fut]
                try:
                    fut.result()
                    logger.info("Pendo: %s — complete (%.1fs)", name, elapsed)
                except Exception as e:
                    logger.warning("Pendo: %s — FAILED after %.1fs (%s)", name, elapsed, e)

        logger.info("Pendo: preload complete in %.1fs", time.time() - t0)

    def _filter_customer_visitors(self, customer_name: str, partition: dict) -> tuple[list[dict], list[dict]]:
        """From a visitor partition, extract this customer's visitors and internal visitors."""
        _is_internal = partition["_is_internal"]
        customer_visitors = []
        internal_visitors = []
        for v in partition["all_visitors"]:
            agent = (v.get("metadata") or {}).get("agent") or {}
            sitenames = agent.get("sitenames") or []
            if any(_name_matches(customer_name, str(sn)) for sn in sitenames):
                if _is_internal(v):
                    internal_visitors.append(v)
                else:
                    customer_visitors.append(v)
        return customer_visitors, internal_visitors

    def _resolve_account(self, customer_name: str, customer_visitors: list[dict]) -> tuple[str | None, str, dict]:
        """Find the best-matching account ID and metadata for a customer."""
        account_ids_seen: dict[str, int] = {}
        for v in customer_visitors:
            auto = (v.get("metadata") or {}).get("auto") or {}
            aid = auto.get("accountid")
            if aid:
                account_ids_seen[str(aid)] = account_ids_seen.get(str(aid), 0) + 1

        account_id = None
        if account_ids_seen:
            for aid_str in sorted(account_ids_seen, key=account_ids_seen.get, reverse=True):
                try:
                    info = self.get_account_info(aid_str)
                    name = ((info.get("metadata") or {}).get("agent") or {}).get("name", "")
                    if name and name.lower().startswith(customer_name.lower()):
                        account_id = aid_str
                        break
                except Exception:
                    continue
            if account_id is None:
                account_id = max(account_ids_seen, key=account_ids_seen.get)

        account_meta = {}
        if account_id:
            try:
                account_meta = self.get_account_info(account_id)
            except Exception:
                pass

        acct_agent = (account_meta.get("metadata") or {}).get("agent") or {}
        acct_name = acct_agent.get("name", "")
        if not acct_name or not acct_name.lower().startswith(customer_name.lower()):
            acct_name = customer_name

        return account_id, acct_name, acct_agent

    # ── Focused data methods (each returns agent-interpretable summaries) ──

    def get_customer_health(self, customer_name: str, days: int = 30,
                            _precomputed_signals: dict | None = None) -> dict[str, Any]:
        """Engagement summary, role breakdown, benchmarks, and auto-detected signals.
        This is what a CSM needs to quickly assess account health."""
        partition = self._get_visitor_partition(days)
        now_ms = partition["now_ms"]
        customer_visitors, internal_visitors = self._filter_customer_visitors(customer_name, partition)

        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        account_id, acct_name, acct_agent = self._resolve_account(customer_name, customer_visitors)

        csm_names = set()
        for v in customer_visitors:
            on = ((v.get("metadata") or {}).get("agent") or {}).get("ownername")
            if on:
                csm_names.add(on)

        engagement = {"active_7d": 0, "active_30d": 0, "dormant": 0}
        role_active: dict[str, int] = {}
        role_dormant: dict[str, int] = {}

        for v in customer_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            role = agent.get("role", "Unknown")
            lv = auto.get("lastvisit", 0)
            if lv:
                days_ago = (now_ms - lv) / (86400 * 1000)
                if days_ago <= 7:
                    engagement["active_7d"] += 1
                    role_active[role] = role_active.get(role, 0) + 1
                elif days_ago <= 30:
                    engagement["active_30d"] += 1
                    role_active[role] = role_active.get(role, 0) + 1
                else:
                    engagement["dormant"] += 1
                    role_dormant[role] = role_dormant.get(role, 0) + 1
            else:
                engagement["dormant"] += 1
                role_dormant[role] = role_dormant.get(role, 0) + 1

        total_visitors = len(customer_visitors)
        customer_rate = engagement["active_7d"] / max(total_visitors, 1)

        all_peer_rates = []
        cohort_peer_rates = []
        cust_cohort_info = get_customer_cohort(customer_name)
        cust_cohort = cust_cohort_info.get("cohort", "")
        cohort_members = set(get_cohort_members(cust_cohort)) if cust_cohort else set()
        for cname, stats in partition["all_customer_stats"].items():
            if stats["total"] < 5:
                continue
            rate = stats["active_7d"] / stats["total"]
            c_info = get_customer_cohort(cname)
            if c_info.get("exclude"):
                continue
            all_peer_rates.append(rate)
            if cname in cohort_members:
                cohort_peer_rates.append(rate)
        all_peer_rates.sort()
        cohort_peer_rates.sort()
        median_rate = all_peer_rates[len(all_peer_rates) // 2] if all_peer_rates else 0
        cohort_median = cohort_peer_rates[len(cohort_peer_rates) // 2] if cohort_peer_rates else None
        bench_rate = cohort_median if cohort_median is not None else median_rate

        # Auto-detect signals
        signals: list[str] = []
        dormant_pct = engagement["dormant"] / max(total_visitors, 1)
        if dormant_pct > 0.5:
            signals.append(f"High dormancy: {engagement['dormant']}/{total_visitors} users ({dormant_pct:.0%}) inactive 30+ days")
        if customer_rate > bench_rate * 1.5 and total_visitors >= 5:
            if cohort_median is not None:
                signals.append(
                    f"Strong engagement: {customer_rate:.0%} weekly active rate vs {bench_rate:.0%} cohort median"
                )
            else:
                signals.append(
                    f"Strong engagement: {customer_rate:.0%} weekly active rate vs {bench_rate:.0%} portfolio median"
                )
        elif customer_rate < bench_rate * 0.5 and total_visitors >= 5:
            if cohort_median is not None:
                signals.append(
                    f"Low engagement: {customer_rate:.0%} weekly active rate vs {bench_rate:.0%} cohort median"
                )
            else:
                signals.append(
                    f"Low engagement: {customer_rate:.0%} weekly active rate vs {bench_rate:.0%} portfolio median"
                )
        if engagement["active_7d"] <= 2 and total_visitors >= 10:
            signals.append(f"Concentration risk: only {engagement['active_7d']} of {total_visitors} users active this week")

        # Count matching sites
        site_count = 0
        for v in customer_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            for sn in agent.get("sitenames") or []:
                if sn and _name_matches(customer_name, str(sn)):
                    site_count += 1
                    break

        site_names = set()
        for v in customer_visitors:
            for sn in ((v.get("metadata") or {}).get("agent") or {}).get("sitenames") or []:
                if sn and _name_matches(customer_name, str(sn)):
                    site_names.add(sn)

        user_activity = self._build_user_activity(customer_visitors, now_ms)
        exec_active = sum(
            1 for u in user_activity if u["role"] in _EXECUTIVE_VISITOR_ROLES and u["days_inactive"] <= 7
        )
        exec_total = sum(1 for u in user_activity if u["role"] in _EXECUTIVE_VISITOR_ROLES)
        if exec_total > 0 and exec_active == 0:
            signals.append(f"No executive engagement: {exec_total} executives, none active this week")
        elif exec_active > 0:
            signals.append(f"Executive engagement: {exec_active}/{exec_total} executives active this week")

        for sn in site_names:
            if "training" in sn.lower():
                signals.append(f"Active training site detected ({sn})")
                break

        if internal_visitors:
            signals.append(f"{len(internal_visitors)} LeanDNA staff visited (excluded from metrics)")

        pre = _precomputed_signals or {}
        self._add_behavioral_signals(
            signals, customer_name, days,
            depth_data=pre.get("depth"), export_data=pre.get("exports"),
            kei_data=pre.get("kei"), guide_data=pre.get("guides"),
        )

        # ── QA cross-checks ──
        self._run_pendo_qa_checks(
            customer_name, total_visitors, engagement, site_names,
            cust_cohort_info, customer_rate, median_rate, cohort_median,
        )

        return {
            "customer": customer_name,
            "days": days,
            "generated": datetime.datetime.now().strftime("%Y-%m-%d"),
            "account": {
                "name": acct_name,
                "region": acct_agent.get("region", ""),
                "csm": ", ".join(sorted(csm_names)) if csm_names else "Unknown",
                "account_id": account_id,
                "total_visitors": total_visitors,
                "internal_visitors": len(internal_visitors),
                "total_sites": len(site_names),
            },
            "engagement": {
                "active_7d": engagement["active_7d"],
                "active_30d": engagement["active_30d"],
                "dormant": engagement["dormant"],
                "active_rate_7d": round(customer_rate * 100, 1),
                "role_active": dict(sorted(role_active.items(), key=lambda x: -x[1])),
                "role_dormant": dict(sorted(role_dormant.items(), key=lambda x: -x[1])),
            },
            "benchmarks": {
                "customer_active_rate": round(customer_rate * 100, 1),
                "peer_median_rate": round(median_rate * 100, 1),
                "peer_count": len(all_peer_rates),
                "cohort": cust_cohort,
                "cohort_name": _COHORT_DISPLAY.get(cust_cohort, cust_cohort.replace("_", " ").title()) if cust_cohort else "",
                "cohort_median_rate": round(cohort_median * 100, 1) if cohort_median is not None else None,
                "cohort_count": len(cohort_peer_rates),
            },
            "signals": signals,
        }

    @staticmethod
    def _run_pendo_qa_checks(
        customer_name, total_visitors, engagement, site_names,
        cohort_info, customer_rate, median_rate, cohort_median,
    ):
        """Cross-validate Pendo data and flag discrepancies."""
        from .qa import qa

        # Engagement buckets should sum to total visitors
        eng_sum = engagement["active_7d"] + engagement["active_30d"] + engagement["dormant"]
        if eng_sum == total_visitors:
            qa.check()
        else:
            qa.flag("Pendo engagement buckets don't sum to total visitors",
                    expected=total_visitors, actual=eng_sum,
                    sources=("active_7d + active_30d + dormant", "total visitor count"),
                    severity="error")

        # Active rate should be consistent with the raw numbers
        expected_rate = engagement["active_7d"] / max(total_visitors, 1)
        if abs(expected_rate - customer_rate) < 0.001:
            qa.check()
        else:
            qa.flag("Active rate doesn't match active_7d / total_visitors",
                    expected=f"{expected_rate:.3f}", actual=f"{customer_rate:.3f}",
                    sources=("computed rate", "reported rate"),
                    severity="error")

        # Customer should exist in cohorts.yaml
        if cohort_info.get("cohort"):
            qa.check()
        elif cohort_info.get("exclude"):
            qa.check()
        else:
            qa.flag(f"Customer '{customer_name}' not found in cohorts.yaml",
                    sources=("Pendo customer list", "cohorts.yaml"),
                    severity="warning")

        # Flag unverified cohort classifications
        if cohort_info.get("unverified"):
            qa.flag(f"Cohort classification unverified for '{customer_name}' ({cohort_info.get('cohort', '?')})",
                    sources=("cohorts.yaml",),
                    severity="info")

        # Cohort median should exist if customer has a cohort with enough peers
        if cohort_info.get("cohort") and not cohort_info.get("exclude"):
            if cohort_median is not None:
                qa.check()
            else:
                qa.flag(f"No cohort median available for '{cohort_info.get('cohort')}' — too few peers",
                        sources=("cohort peer calculation",),
                        severity="info")

        # Zero sites is suspicious for a customer with visitors
        if total_visitors > 0 and len(site_names) == 0:
            qa.flag(f"Customer '{customer_name}' has {total_visitors} visitors but 0 sites",
                    sources=("Pendo visitors", "site name matching"),
                    severity="warning")
        else:
            qa.check()

    def _add_behavioral_signals(
        self, signals: list[str], customer_name: str, days: int,
        depth_data: dict | None = None, export_data: dict | None = None,
        kei_data: dict | None = None, guide_data: dict | None = None,
    ) -> None:
        """Append behavioral signals. Accepts pre-computed data to avoid redundant fetches."""
        try:
            d = depth_data or self.get_customer_depth(customer_name, days)
            write_ratio = d.get("write_ratio", 0)
            collab_events = d.get("collab_events", 0)
            if write_ratio >= 40:
                signals.append(f"Deep write adoption: {write_ratio}% write ratio (running operations in-app)")
            elif write_ratio <= 10:
                signals.append(f"Read-heavy usage: only {write_ratio}% write ratio (may be dashboard-only)")
            if collab_events > 0:
                signals.append(f"In-app collaboration: {collab_events:,} comment/chat/attachment events")
        except Exception:
            pass
        try:
            e = export_data or self.get_customer_exports(customer_name, days)
            total_exports = e.get("total_exports", 0)
            exports_per_user = e.get("exports_per_active_user", 0)
            if total_exports > 0:
                signals.append(f"Export activity: {total_exports:,} exports ({exports_per_user}/active user)")
        except Exception:
            pass
        try:
            k = kei_data or self.get_customer_kei(customer_name, days)
            kei_queries = k.get("total_queries", 0)
            kei_exec = k.get("executive_users", 0)
            if kei_queries > 0:
                msg = f"Kei AI active: {kei_queries:,} queries from {k.get('unique_users', 0)} users"
                if kei_exec > 0:
                    msg += f" (incl. {kei_exec} executives)"
                signals.append(msg)
            else:
                signals.append("No Kei AI usage detected — rollout opportunity")
        except Exception:
            pass
        try:
            g = guide_data or self.get_customer_guides(customer_name, days)
            dismiss_rate = g.get("dismiss_rate", 0)
            guide_reach = g.get("guide_reach", 0)
            if dismiss_rate > 30:
                signals.append(f"High guide dismiss rate: {dismiss_rate}% — possible onboarding friction")
            if guide_reach < 30 and g.get("active_users", 0) > 5:
                signals.append(f"Low guide reach: only {guide_reach}% of active users see guides")
        except Exception:
            pass

    def get_customer_sites(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Per-site (and per-entity when present) metrics: visitors, page views, feature clicks, events, minutes, last active.
        Returns a flat list of rows; each row has sitename and optional entity (Customer → Site → Entity).
        """
        partition = self._get_visitor_partition(days)
        now_ms = partition["now_ms"]
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        # Key by (sitename, entity) so we get one row per site-entity pair when entity is set
        site_data: dict[tuple[str, str], dict] = {}
        for v in customer_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            lv = auto.get("lastvisit", 0)
            entity_str = (agent.get("entity") or "").strip() if agent.get("entity") else ""
            site_names = agent.get("sitenames") or []
            if not site_names and agent.get("sitename"):
                site_names = [agent["sitename"]]
            for sn in site_names:
                if not sn or not _name_matches(customer_name, str(sn)):
                    continue
                key = (sn, entity_str)
                if key not in site_data:
                    site_data[key] = {"visitors": 0, "last_visit_ms": 0}
                site_data[key]["visitors"] += 1
                if lv and lv > site_data[key]["last_visit_ms"]:
                    site_data[key]["last_visit_ms"] = lv

        usage_by_site = self._get_usage_by_site_cached(days)
        usage_map = {r["sitename"]: r for r in (usage_by_site.get("results") or [])}
        try:
            usage_by_site_entity = self._get_usage_by_site_entity_cached(days)
            usage_entity_map = {(r["sitename"], r.get("entity") or ""): r for r in (usage_by_site_entity.get("results") or [])}
        except Exception:
            usage_entity_map = {}

        sites = []
        for (sn, entity_str), info in sorted(site_data.items(), key=lambda x: (-x[1]["visitors"], x[0][0], x[0][1])):
            usage = usage_entity_map.get((sn, entity_str)) or usage_map.get(sn, {})
            lv_ms = info["last_visit_ms"]
            row = {
                "sitename": sn,
                "visitors": info["visitors"],
                "page_views": usage.get("page_views", 0),
                "feature_clicks": usage.get("feature_clicks", 0),
                "total_events": usage.get("total_events", 0),
                "total_minutes": usage.get("total_minutes", 0),
                "last_active": datetime.datetime.fromtimestamp(lv_ms / 1000).strftime("%Y-%m-%d") if lv_ms else "N/A",
            }
            if entity_str:
                row["entity"] = entity_str
            sites.append(row)
        return {"customer": customer_name, "days": days, "sites": sites}

    def get_customer_features(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Top pages and features this customer uses, with human-readable names."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}

        page_catalog = self._get_page_catalog_cached()
        feature_catalog = self.get_feature_catalog()

        top_pages: list[dict] = []
        top_features: list[dict] = []
        all_page_events: list[dict] = []
        all_feat_events: list[dict] = []

        try:
            all_page_events = self._get_page_events_cached(days)
            page_counts: dict[str, dict] = {}
            for ev in all_page_events:
                if ev.get("visitorId") in visitor_ids:
                    pid = ev.get("pageId", "")
                    if pid not in page_counts:
                        page_counts[pid] = {"events": 0, "minutes": 0}
                    page_counts[pid]["events"] += ev.get("numEvents", 0) or 0
                    page_counts[pid]["minutes"] += ev.get("numMinutes", 0) or 0
            for pid, c in sorted(page_counts.items(), key=lambda x: -x[1]["events"])[:10]:
                top_pages.append({"name": page_catalog.get(pid, pid), "events": c["events"], "minutes": c["minutes"]})
        except Exception as e:
            logger.debug("Could not compute top pages: %s", e)

        try:
            all_feat_events = self._get_feature_events_cached(days)
            feat_counts: dict[str, int] = {}
            for ev in all_feat_events:
                if ev.get("visitorId") in visitor_ids:
                    fid = ev.get("featureId", "")
                    feat_counts[fid] = feat_counts.get(fid, 0) + (ev.get("numEvents", 0) or 0)
            for fid, count in sorted(feat_counts.items(), key=lambda x: -x[1])[:10]:
                top_features.append({"name": feature_catalog.get(fid, fid), "events": count})
        except Exception as e:
            logger.debug("Could not compute top features: %s", e)

        out: dict[str, Any] = {
            "customer": customer_name,
            "days": days,
            "top_pages": top_pages,
            "top_features": top_features,
        }

        if FEATURE_ADOPTION_INSIGHTS and days >= 12 and all_feat_events:
            try:
                half = min(max(days // 2, 5), days - 1)
                if 0 < half < days:
                    recent_feat = self._get_feature_events_cached(half)
                    ff = _aggregate_customer_feature_events(all_feat_events, visitor_ids)
                    fr = _aggregate_customer_feature_events(recent_feat, visitor_ids)
                    narrative = _feature_adoption_pattern_narrative(
                        days=days,
                        recent_days=half,
                        prior_days=days - half,
                        feat_full=ff,
                        feat_recent=fr,
                        feature_catalog=feature_catalog,
                    )
                    if narrative:
                        out["feature_adoption_insights"] = {
                            "recent_days": half,
                            "prior_days": days - half,
                            "narrative": narrative,
                            "feature_clicks_total": sum(ff.values()),
                            "feature_clicks_recent": sum(fr.values()),
                        }
            except Exception as e:
                logger.debug("Feature adoption usage patterns skipped: %s", e)

        return out

    def _build_user_activity(self, visitors: list[dict], now_ms: int) -> list[dict]:
        """Build user activity list from visitor records."""
        users = []
        for v in visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            lv = auto.get("lastvisit", 0)
            days_ago = (now_ms - lv) / (86400 * 1000) if lv else 999
            users.append({
                "email": agent.get("emailaddress", ""),
                "role": agent.get("role", "Unknown"),
                "language": (agent.get("language") or "").strip(),
                "last_visit": datetime.datetime.fromtimestamp(lv / 1000).strftime("%Y-%m-%d") if lv else "Never",
                "days_inactive": round(days_ago, 1),
            })
        return users

    # ── Behavioral categorization engine ──

    _BEHAVIOR_PATTERNS: dict[str, re.Pattern] = {
        "collaboration": re.compile(r'comment|chat|send message|watchers|attachment', re.I),
        "upload": re.compile(r'upload|excel upload', re.I),
        "inline_edit": re.compile(r'edit cell|editable cell|commit date.*cell|status.*cell|tracking number.*cell|late delivery cause.*cell', re.I),
        "task_mgmt": re.compile(r'task|action.*card|archive|snooze|unable to fix|done.*button|in progress|mark as fixed', re.I),
        "filter": re.compile(r'filter|quick filter', re.I),
        "drilldown": re.compile(r'drilldown|drill|nested|details.*drawer|expand.*panel|collapse.*panel|item code.*detail|burnoff', re.I),
        "search": re.compile(r'search', re.I),
        "export": re.compile(r'export|download.*(csv|xlsx|excel|template)', re.I),
        "widget": re.compile(r'^widget', re.I),
        "column_config": re.compile(r'^column', re.I),
        "share": re.compile(r'share|save.*view|save.*dashboard|make a copy', re.I),
        "kei_ai": re.compile(r'kei', re.I),
        "send_to_erp": re.compile(r'send to erp', re.I),
        "supplier": re.compile(r'supplier|scorecard|offer alternative|accept supplier', re.I),
        "delivery": re.compile(r'delivery|split deliv', re.I),
    }

    _categorized_features_cache: dict[str, dict[str, str]] | None = None

    def _get_categorized_features(self) -> dict[str, dict[str, str]]:
        """Categorize all features by behavior type. Returns {category: {fid: name}}."""
        if self._categorized_features_cache is not None and self._CACHE_TTL > 0:
            return self._categorized_features_cache
        catalog = self.get_feature_catalog()
        result: dict[str, dict[str, str]] = {cat: {} for cat in self._BEHAVIOR_PATTERNS}
        result["other"] = {}
        for fid, name in catalog.items():
            matched = False
            for cat, pattern in self._BEHAVIOR_PATTERNS.items():
                if pattern.search(name):
                    result[cat][fid] = name
                    matched = True
                    break
            if not matched:
                result["other"][fid] = name
        self._categorized_features_cache = result
        logger.debug("Categorized %d features into %d behavior types", len(catalog),
                      sum(1 for v in result.values() if v))
        return result

    _feat_events_cache: dict[str, Any] | None = None
    _feat_events_cache_ts: float = 0

    def _get_feature_events_cached(self, days: int) -> list[dict]:
        """Cached feature events for reuse across all behavioral tools."""
        with self._cache_lock:
            if (
                self._feat_events_cache
                and self._cache_valid(self._feat_events_cache_ts)
            ):
                if self._feat_events_cache.get("days") == days:
                    return self._feat_events_cache["results"]

        from .pendo_preload_cache_drive import (
            PRELOAD_KIND_FEATURE_EVENTS,
            save_pendo_preload_payload,
            try_load_pendo_preload_payload,
        )

        blob = try_load_pendo_preload_payload(PRELOAD_KIND_FEATURE_EVENTS, days)
        if isinstance(blob, list):
            with self._cache_lock:
                self._feat_events_cache = {"days": days, "results": blob}
                self._feat_events_cache_ts = time.time()
            return blob

        ts = _time_series(days)
        raw = self.aggregate([
            {"source": {"featureEvents": None, "timeSeries": ts}},
        ]).get("results", [])
        results = _merge_visitor_event_rows_by_dimension(raw, "featureId")

        with self._cache_lock:
            self._feat_events_cache = {"days": days, "results": results}
            self._feat_events_cache_ts = time.time()
        save_pendo_preload_payload(PRELOAD_KIND_FEATURE_EVENTS, days, results)
        return results

    def _visitor_info_map(self, visitors: list[dict]) -> dict[str, dict]:
        """Build {visitorId: {email, role}} from visitor records."""
        m = {}
        for v in visitors:
            vid = v.get("visitorId")
            if vid:
                agent = (v.get("metadata") or {}).get("agent") or {}
                m[vid] = {
                    "email": agent.get("emailaddress", ""),
                    "role": agent.get("role", "Unknown"),
                }
        return m

    def _count_active_users(self, visitors: list[dict], now_ms: int, window_days: int = 30) -> int:
        threshold = window_days * 86400 * 1000
        return sum(1 for v in visitors
                   if ((v.get("metadata") or {}).get("auto") or {}).get("lastvisit", 0)
                   and (now_ms - ((v.get("metadata") or {}).get("auto") or {}).get("lastvisit", 0)) <= threshold)

    # ── Behavioral depth ──

    def get_customer_depth(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Behavioral depth: how the customer uses the product across read/write/collab dimensions.
        Returns event counts per behavior category and a read-vs-write ratio."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        categories = self._get_categorized_features()
        all_events = self._get_feature_events_cached(days)

        fid_to_cat = {}
        for cat, fids in categories.items():
            for fid in fids:
                fid_to_cat[fid] = cat

        by_cat: dict[str, int] = {}
        by_cat_users: dict[str, set] = {}
        for ev in all_events:
            if ev.get("visitorId") not in visitor_ids:
                continue
            fid = ev.get("featureId", "")
            cat = fid_to_cat.get(fid, "other")
            ne = ev.get("numEvents", 0) or 0
            by_cat[cat] = by_cat.get(cat, 0) + ne
            if cat not in by_cat_users:
                by_cat_users[cat] = set()
            by_cat_users[cat].add(ev.get("visitorId"))

        total = sum(by_cat.values())
        active = self._count_active_users(customer_visitors, partition["now_ms"])

        # Read = search + filter + drilldown + widget + column_config
        # Write = inline_edit + upload + task_mgmt + delivery + send_to_erp
        # Collab = collaboration + share
        read_cats = {"search", "filter", "drilldown", "widget", "column_config"}
        write_cats = {"inline_edit", "upload", "task_mgmt", "delivery", "send_to_erp", "export"}
        collab_cats = {"collaboration", "share"}

        read_total = sum(by_cat.get(c, 0) for c in read_cats)
        write_total = sum(by_cat.get(c, 0) for c in write_cats)
        collab_total = sum(by_cat.get(c, 0) for c in collab_cats)

        breakdown = []
        display_order = ["collaboration", "upload", "inline_edit", "task_mgmt", "filter",
                         "drilldown", "search", "export", "widget", "column_config",
                         "share", "kei_ai", "send_to_erp", "supplier", "delivery", "other"]
        display_labels = {
            "collaboration": "Collaboration", "upload": "Data Upload",
            "inline_edit": "Inline Editing", "task_mgmt": "Task Management",
            "filter": "Filtering", "drilldown": "Drilldown/Details",
            "search": "Search", "export": "Export/Download",
            "widget": "Widget Config", "column_config": "Column Config",
            "share": "Share/Save Views", "kei_ai": "Kei AI",
            "send_to_erp": "Send to ERP", "supplier": "Supplier Mgmt",
            "delivery": "Delivery Mgmt", "other": "Other",
        }
        for cat in display_order:
            count = by_cat.get(cat, 0)
            if count > 0:
                breakdown.append({
                    "category": display_labels.get(cat, cat),
                    "events": count,
                    "users": len(by_cat_users.get(cat, set())),
                    "pct": round(count / max(total, 1) * 100, 1),
                })

        return {
            "customer": customer_name,
            "days": days,
            "total_feature_events": total,
            "active_users": active,
            "read_events": read_total,
            "write_events": write_total,
            "collab_events": collab_total,
            "write_ratio": round(write_total / max(read_total + write_total, 1) * 100, 1),
            "breakdown": breakdown,
        }

    # ── Export analysis ──

    def get_customer_exports(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Export behavior analysis: which data a customer exports, how often, and who does it."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        export_features = self._get_categorized_features().get("export", {})
        if not export_features:
            return {"customer": customer_name, "days": days, "exports": [], "total_exports": 0,
                    "note": "No export features found in catalog"}

        all_feat_events = self._get_feature_events_cached(days)

        by_feature: dict[str, int] = {}
        by_user: dict[str, int] = {}
        total = 0
        for ev in all_feat_events:
            fid = ev.get("featureId", "")
            if fid in export_features and ev.get("visitorId") in visitor_ids:
                ne = ev.get("numEvents", 0) or 0
                by_feature[fid] = by_feature.get(fid, 0) + ne
                vid = ev.get("visitorId", "")
                by_user[vid] = by_user.get(vid, 0) + ne
                total += ne

        exports = []
        for fid, count in sorted(by_feature.items(), key=lambda x: -x[1]):
            exports.append({"feature": export_features[fid], "exports": count})

        vid_to_info = self._visitor_info_map(customer_visitors)
        top_exporters = []
        for vid, count in sorted(by_user.items(), key=lambda x: -x[1])[:5]:
            info = vid_to_info.get(vid, {})
            top_exporters.append({"email": info.get("email", ""), "role": info.get("role", "Unknown"), "exports": count})

        active = self._count_active_users(customer_visitors, partition["now_ms"])
        return {
            "customer": customer_name,
            "days": days,
            "total_exports": total,
            "exports_per_active_user": round(total / max(active, 1), 1),
            "active_users": active,
            "by_feature": exports,
            "top_exporters": top_exporters,
        }

    # ── Kei AI analysis ──

    def get_customer_kei(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Kei AI chatbot usage: who's using it, how much, and critically whether executives are.
        Kei adoption is a leading indicator of strategic engagement and executive pull-through."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        kei_features = self._get_categorized_features().get("kei_ai", {})
        all_feat_events = self._get_feature_events_cached(days)
        vid_to_info = self._visitor_info_map(customer_visitors)

        by_user: dict[str, int] = {}
        total = 0
        for ev in all_feat_events:
            fid = ev.get("featureId", "")
            if fid in kei_features and ev.get("visitorId") in visitor_ids:
                ne = ev.get("numEvents", 0) or 0
                vid = ev.get("visitorId", "")
                by_user[vid] = by_user.get(vid, 0) + ne
                total += ne

        # Also check track events for "Kei AI: send-message"
        try:
            track_results = self._get_track_events_cached(days)
            for ev in track_results:
                if ev.get("visitorId") not in visitor_ids:
                    continue
                pid = ev.get("pageId", "")
                if "kei" in pid.lower():
                    ne = ev.get("numEvents", 0) or 0
                    vid = ev.get("visitorId", "")
                    by_user[vid] = by_user.get(vid, 0) + ne
                    total += ne
        except Exception:
            pass

        active = self._count_active_users(customer_visitors, partition["now_ms"])
        adoption_rate = round(len(by_user) / max(active, 1) * 100, 1)

        users = []
        exec_users = 0
        exec_queries = 0
        for vid, count in sorted(by_user.items(), key=lambda x: -x[1]):
            info = vid_to_info.get(vid, {})
            role = info.get("role", "Unknown")
            is_exec = role in _EXECUTIVE_VISITOR_ROLES
            if is_exec:
                exec_users += 1
                exec_queries += count
            users.append({
                "email": info.get("email", ""),
                "role": role,
                "queries": count,
                "is_executive": is_exec,
            })

        return {
            "customer": customer_name,
            "days": days,
            "total_queries": total,
            "unique_users": len(by_user),
            "active_users": active,
            "adoption_rate": adoption_rate,
            "executive_users": exec_users,
            "executive_queries": exec_queries,
            "users": users[:10],
        }

    # ── Guide engagement ──

    def get_customer_guides(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Guide engagement: are users seeing, advancing, or dismissing in-app guides?
        High dismiss rates signal onboarding friction. Low guide-seen counts may indicate
        users aren't reaching guided workflows."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}

        try:
            guide_events = self._get_guide_events_cached(days)
        except Exception as e:
            return {"error": f"Could not fetch guide events: {e}"}

        by_type: dict[str, int] = {}
        by_guide: dict[str, dict[str, int]] = {}
        users_with_guides: set[str] = set()

        for ev in guide_events:
            if ev.get("visitorId") not in visitor_ids:
                continue
            t = ev.get("type", "?")
            w = int(ev.get("numEvents") or 0) or 1
            by_type[t] = by_type.get(t, 0) + w
            gid = ev.get("guideId", "?")
            if gid not in by_guide:
                by_guide[gid] = {}
            by_guide[gid][t] = by_guide[gid].get(t, 0) + w
            users_with_guides.add(ev.get("visitorId"))

        seen = by_type.get("guideSeen", 0)
        advanced = by_type.get("guideAdvanced", 0)
        dismissed = by_type.get("guideDismissed", 0)
        total_visitors = len(customer_visitors)

        guide_names = self._get_guide_catalog_cached()

        top_guides = []
        for gid, counts in sorted(by_guide.items(), key=lambda x: -sum(x[1].values()))[:8]:
            top_guides.append({
                "guide": guide_names.get(gid, gid[:20]),
                "seen": counts.get("guideSeen", 0),
                "advanced": counts.get("guideAdvanced", 0),
                "dismissed": counts.get("guideDismissed", 0),
            })

        return {
            "customer": customer_name,
            "days": days,
            "total_guide_events": sum(by_type.values()),
            "users_who_saw_guides": len(users_with_guides),
            "total_visitors": total_visitors,
            "guide_reach": round(len(users_with_guides) / max(total_visitors, 1) * 100, 1),
            "seen": seen,
            "advanced": advanced,
            "dismissed": dismissed,
            "dismiss_rate": round(dismissed / max(seen, 1) * 100, 1),
            "advance_rate": round(advanced / max(seen, 1) * 100, 1),
            "top_guides": top_guides,
        }

    def get_customer_people(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Up to 5 champions (most recently active first) and 5 at-risk users (2 wk–~6 mo inactive, most recent lapse first)."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        user_activity = self._build_user_activity(customer_visitors, partition["now_ms"])
        # Champions: lowest days since last visit first (most recently active).
        champions = sorted(user_activity, key=lambda u: u["days_inactive"])[:5]
        # At-risk: same ordering — smallest days_inactive first (went quiet most recently).
        at_risk = sorted(
            [u for u in user_activity if 14 <= u["days_inactive"] < 183],
            key=lambda u: u["days_inactive"],
        )[:5]
        return {"customer": customer_name, "days": days, "champions": champions, "at_risk_users": at_risk}

    def list_customers(self, days: int = 30) -> dict[str, Any]:
        """Portfolio overview: all customers ranked by activity with summary stats.
        Returns what an agent needs to decide which customers to focus on."""
        partition = self._get_visitor_partition(days)
        stats = partition["all_customer_stats"]

        peer_rates = []
        for s in stats.values():
            if s["total"] >= 5:
                peer_rates.append(s["active_7d"] / s["total"])
        peer_rates.sort()
        median_rate = peer_rates[len(peer_rates) // 2] if peer_rates else 0

        customers = []
        for name, s in stats.items():
            if name == "?" or s["total"] < 2:
                continue
            rate = s["active_7d"] / s["total"]
            customers.append({
                "customer": name,
                "total_users": s["total"],
                "active_7d": s["active_7d"],
                "active_rate_7d": round(rate * 100, 1),
                "vs_median": round((rate - median_rate) * 100, 1),
            })
        customers.sort(key=lambda c: -c["total_users"])

        return {
            "days": days,
            "total_customers": len(customers),
            "peer_median_rate": round(median_rate * 100, 1),
            "customers": customers,
        }

    # ── Full health report (aggregates all focused methods for monolith deck) ──

    def get_customer_health_report(
        self,
        customer_name: str,
        days: int = 30,
        *,
        signals_llm_manifest_rules: str | None = None,
        signals_llm_slide_prompt: str | None = None,
    ) -> dict[str, Any]:
        """Comprehensive health report combining all focused methods.
        Used by the monolith deck generator and as a convenience method.

        When ``BPO_SIGNALS_LLM`` is enabled, optional ``signals_llm_manifest_rules`` (QBR Manifest
        excerpt) and ``signals_llm_slide_prompt`` (Notable Signals slide YAML brief) are passed
        through to the signals LLM as editorial context (Phase 3).
        """
        # Fetch all sub-reports first, then pass them to health for signal generation
        sites_data = self.get_customer_sites(customer_name, days)
        features_data = self.get_customer_features(customer_name, days)
        people_data = self.get_customer_people(customer_name, days)
        exports_data = self.get_customer_exports(customer_name, days)
        depth_data = self.get_customer_depth(customer_name, days)
        kei_data = self.get_customer_kei(customer_name, days)
        guides_data = self.get_customer_guides(customer_name, days)
        poll_events_data = self.get_customer_poll_events(customer_name, days)
        frustration_data = self.get_customer_frustration_signals(customer_name, days)
        track_events_breakdown_data = self.get_customer_track_events_breakdown(customer_name, days)
        visitor_languages_data = self.get_customer_visitor_languages(customer_name, days)

        health = self.get_customer_health(customer_name, days,
            _precomputed_signals={"depth": depth_data, "exports": exports_data,
                                   "kei": kei_data, "guides": guides_data})
        if "error" in health:
            return health

        # JIRA data (optional — skipped if JIRA is not configured)
        jira_data = {}
        try:
            from .jira_client import get_shared_jira_client
            jira_data = get_shared_jira_client().get_customer_jira(customer_name, days=90)
        except Exception as e:
            from .qa import qa
            qa.flag(f"JIRA data unavailable: {str(e)[:80]}",
                    sources=("JIRA API",), severity="warning")

        # Salesforce — required when configured (preflight); skipped if not configured
        from .data_source_health import _salesforce_configured
        salesforce_data: dict = {}
        salesforce_primary_account_id = None
        customer_key_type = None
        if _salesforce_configured():
            from .customer_identity import lookup_salesforce_identity
            from .salesforce_client import SalesforceClient

            sf_ids, sf_prim = lookup_salesforce_identity(customer_name)
            sf = SalesforceClient()
            salesforce_data = sf.get_customer_salesforce(
                customer_name,
                preferred_account_ids=sf_ids if sf_ids else None,
                primary_account_id=sf_prim,
            )
            salesforce_primary_account_id = salesforce_data.get("primary_account_id")
            res = salesforce_data.get("resolution")
            if res == "salesforce_account_id":
                customer_key_type = "salesforce_account_id"
            elif res == "name":
                customer_key_type = "name"
            else:
                customer_key_type = "none"

        # Cross-check: site count from health report vs detailed site list
        from .qa import qa
        health_site_count = health.get("account", {}).get("total_sites", 0)
        detail_site_count = len(sites_data.get("sites", []))
        if health_site_count == detail_site_count:
            qa.check()
        else:
            qa.flag("Site count mismatch between health summary and site detail",
                    expected=health_site_count, actual=detail_site_count,
                    sources=("health report account.total_sites", "sites list length"),
                    severity="warning")

        # CS Report data (optional — from Data Exports shared drive)
        cs_platform_health = {}
        cs_supply_chain = {}
        cs_platform_value = {}
        try:
            from .cs_report_client import (
                get_customer_platform_health,
                get_customer_supply_chain,
                get_customer_platform_value,
                cross_validate_with_pendo,
            )
            cs_platform_health = get_customer_platform_health(customer_name)
            cs_supply_chain = get_customer_supply_chain(customer_name)
            cs_platform_value = get_customer_platform_value(customer_name)
            cross_validate_with_pendo(customer_name, {**health, "sites": sites_data.get("sites", [])})
        except Exception as e:
            qa.flag(f"CS Report data unavailable: {str(e)[:80]}",
                    sources=("CS Report / Data Exports",), severity="warning")

        try:
            pendo_catalog_appendix = self.get_pendo_catalog_appendix_summary()
        except Exception as e:
            logger.warning("Pendo catalog appendix: %s", e)
            pendo_catalog_appendix = {"error": str(e)}

        merged = {
            **health,
            "sites": sites_data.get("sites", []),
            "top_pages": features_data.get("top_pages", []),
            "top_features": features_data.get("top_features", []),
            "feature_adoption_insights": features_data.get("feature_adoption_insights"),
            "champions": people_data.get("champions", []),
            "at_risk_users": people_data.get("at_risk_users", []),
            "exports": exports_data,
            "depth": depth_data,
            "kei": kei_data,
            "guides": guides_data,
            "poll_events": poll_events_data,
            "frustration": frustration_data,
            "track_events_breakdown": track_events_breakdown_data,
            "visitor_languages": visitor_languages_data,
            "pendo_catalog_appendix": pendo_catalog_appendix,
            "jira": jira_data,
            "salesforce": salesforce_data,
            "salesforce_primary_account_id": salesforce_primary_account_id,
            "customer_key_type": customer_key_type,
            "csr": {
                "platform_health": cs_platform_health,
                "supply_chain": cs_supply_chain,
                "platform_value": cs_platform_value,
            },
        }
        if BPO_SIGNALS_TRENDS:
            try:
                from .signals_trends import build_signals_trend_context

                _ctx = build_signals_trend_context(self, customer_name, days, merged)
                merged["signals_trend_context"] = _ctx
            except Exception as e:
                logger.warning("signals_trends: build failed (%s)", e)
                merged["signals_trend_context"] = None
        extend_health_report_signals(merged)
        if BPO_SIGNALS_LLM:
            if signals_llm_manifest_rules:
                merged["_signals_llm_manifest_rules"] = signals_llm_manifest_rules.strip()
            if signals_llm_slide_prompt:
                merged["_signals_llm_slide_prompt"] = signals_llm_slide_prompt.strip()
        maybe_rewrite_signals_with_llm(merged)
        if BPO_SIGNALS_TRENDS:
            try:
                from .signals_trends import finalize_signals_trends_banner

                finalize_signals_trends_banner(merged)
            except Exception as e:
                logger.warning("signals_trends: finalize failed (%s)", e)
        return merged

    # ── Portfolio-level methods (cross-customer analysis) ──

    def _portfolio_customer_summary(self, name: str, days: int) -> dict[str, Any] | None:
        """Compute a single customer's portfolio summary (called from thread pool)."""
        h = self.get_customer_health(name, days)
        if "error" in h:
            err = h.get("error")
            logger.error(
                "Portfolio: skipped customer %r — no summary (%s)",
                name,
                err if err is not None else "error in health payload",
            )
            return None
        depth = self.get_customer_depth(name, days)
        kei = self.get_customer_kei(name, days)
        guides = self.get_customer_guides(name, days)
        exports = self.get_customer_exports(name, days)
        eng = h.get("engagement", {})
        acct = h.get("account") or {}
        total_v = acct.get("total_visitors", 0)
        active_7d = eng.get("active_7d", 0)
        return {
            "customer": name,
            "pendo_csm": str(acct.get("csm") or "").strip() or "Unknown",
            "engagement": eng,
            "benchmarks": h.get("benchmarks", {}),
            "signals": h.get("signals", []),
            "active_users": active_7d,
            "total_users": total_v,
            "login_pct": eng.get("active_rate_7d", 0),
            "depth": depth,
            "kei": kei,
            "guides": guides,
            "exports": exports,
        }

    def get_portfolio_report(
        self,
        days: int = 30,
        max_customers: int | None = None,
        *,
        cohort_rollup_from_slide_yaml: bool = True,
    ) -> dict[str, Any]:
        """Full portfolio report for the book-of-business deck.
        Calls preload() then iterates all customers in parallel.

        Set ``cohort_rollup_from_slide_yaml=False`` for callers that must not read slide definitions
        from disk or Drive (cohort findings bullets use built-in defaults only).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self.preload(days)
        by_customer = self.get_sites_by_customer(days)
        raw_list = [c for c in by_customer["customer_list"] if c != "(unknown)"]
        skipped_ex = [c for c in raw_list if customer_is_excluded_from_portfolio(c)]
        all_names = [c for c in raw_list if c not in skipped_ex]
        if skipped_ex:
            preview = ", ".join(skipped_ex[:25])
            if len(skipped_ex) > 25:
                preview += ", …"
            logger.info(
                "Portfolio: skipping %d customer(s) excluded from portfolio (cohorts.yaml exclude "
                "or BPO_PORTFOLIO_EXCLUDE_CUSTOMERS): %s",
                len(skipped_ex),
                preview,
            )
        if max_customers:
            all_names = all_names[:max_customers]

        total = len(all_names)
        try:
            _nw = int(os.environ.get("BPO_PORTFOLIO_PARALLEL_WORKERS", "8").strip())
            pool_workers = max(1, min(32, _nw))
        except ValueError:
            pool_workers = 8
        logger.info("Portfolio: processing %d customers (parallel, %d workers)", total, pool_workers)
        t0 = time.time()
        stderr_tty = sys.stderr.isatty()

        customer_summaries: list[dict[str, Any]] = []
        done = 0
        pbar = tqdm(
            total=total,
            desc="Portfolio customers",
            unit="cust",
            file=sys.stderr,
            disable=not stderr_tty,
            dynamic_ncols=True,
            mininterval=0.2,
        )
        try:
            with ThreadPoolExecutor(max_workers=pool_workers) as pool:
                futures = {
                    pool.submit(self._portfolio_customer_summary, name, days): name
                    for name in all_names
                }
                for fut in as_completed(futures):
                    name = futures[fut]
                    done += 1
                    if not stderr_tty:
                        if done == 1 or done % 25 == 0 or done == total:
                            logger.info(
                                "Portfolio: completed %d/%d (%.0fs elapsed)",
                                done,
                                total,
                                time.time() - t0,
                            )
                    try:
                        row = fut.result()
                        if row is not None:
                            customer_summaries.append(row)
                    except Exception as e:
                        logger.error(
                            "Portfolio: skipped customer %r — exception during summary: %s",
                            name,
                            e,
                            exc_info=True,
                        )
                    pbar.update(1)
        finally:
            pbar.close()

        customer_summaries.sort(key=lambda r: r["customer"])
        logger.info(
            "Portfolio: computed %d customer summaries in %.1fs, building cohort rollup",
            len(customer_summaries), time.time() - t0,
        )
        cohort_digest, cohort_findings_bullets = compute_cohort_portfolio_rollup(
            customer_summaries,
            use_cohort_findings_slide_yaml=cohort_rollup_from_slide_yaml,
        )

        return {
            "type": "portfolio",
            "days": days,
            "generated": datetime.datetime.now().strftime("%Y-%m-%d"),
            "customer_count": len(customer_summaries),
            "customers": customer_summaries,
            "portfolio_signals": self._compute_portfolio_signals(customer_summaries),
            "portfolio_trends": self._compute_portfolio_trends(customer_summaries),
            "portfolio_leaders": self._compute_portfolio_leaders(customer_summaries),
            "cohort_digest": cohort_digest,
            "cohort_findings_bullets": cohort_findings_bullets,
        }

    def _compute_portfolio_signals(self, summaries: list[dict]) -> list[dict[str, Any]]:
        """Extract the most critical per-customer signals, ranked by severity."""
        # Avoid "only" — it matches "only 0.0% write" and makes read-heavy lines dominate severity.
        alarm_keywords = [
            "no active users", "declining", "dropped", "no kei", "dismiss",
            "read-heavy", "low guide reach", "at risk", "churned",
        ]
        signals: list[dict[str, Any]] = []
        for s in summaries:
            for sig in s.get("signals", []):
                severity = 0
                sig_lower = sig.lower()
                for kw in alarm_keywords:
                    if kw in sig_lower:
                        severity += 1
                if severity > 0:
                    signals.append({
                        "customer": s["customer"],
                        "signal": sig,
                        "severity": severity,
                        "score": s.get("score", 0),
                    })
        signals.sort(key=lambda x: (-x["severity"], x["score"]))
        return _take_portfolio_signals_capping_read_heavy(signals)

    def _compute_portfolio_trends(self, summaries: list[dict]) -> dict[str, Any]:
        """Aggregate product-level trends across all customers."""
        total_active = sum(s.get("active_users", 0) for s in summaries)
        total_users = sum(s.get("total_users", 0) for s in summaries)

        kei_adopters = [s for s in summaries if s.get("kei", {}).get("total_queries", 0) > 0]
        kei_zero = [s for s in summaries if s.get("kei", {}).get("total_queries", 0) == 0]
        high_dismiss = [s for s in summaries if s.get("guides", {}).get("dismiss_rate", 0) > 30]
        read_heavy = [s for s in summaries if s.get("depth", {}).get("write_ratio", 50) < 15]
        export_heavy = [s for s in summaries if s.get("exports", {}).get("total_exports", 0) > 100]
        low_login = [s for s in summaries if s.get("login_pct", 100) < 30]

        trends: list[dict[str, str]] = []
        if kei_zero:
            trends.append({
                "trend": f"Kei AI has zero usage at {len(kei_zero)} of {len(summaries)} customers",
                "type": "opportunity",
                "customers": ", ".join(s["customer"] for s in kei_zero[:5]),
            })
        if kei_adopters:
            total_kei_queries = sum(s.get("kei", {}).get("total_queries", 0) for s in kei_adopters)
            trends.append({
                "trend": f"Kei AI active at {len(kei_adopters)} customers ({total_kei_queries:,} queries)",
                "type": "positive",
                "customers": ", ".join(s["customer"] for s in kei_adopters[:5]),
            })
        if high_dismiss:
            trends.append({
                "trend": f"High guide dismiss rate (>30%) at {len(high_dismiss)} customers — onboarding friction",
                "type": "concern",
                "customers": ", ".join(s["customer"] for s in high_dismiss[:5]),
            })
        if read_heavy:
            trends.append({
                "trend": f"{len(read_heavy)} customers are read-heavy (<15% write ratio) — dashboard-only usage",
                "type": "concern",
                "customers": ", ".join(s["customer"] for s in read_heavy[:5]),
            })
        if export_heavy:
            trends.append({
                "trend": f"{len(export_heavy)} customers export heavily (>100 exports) — deep integration or workaround?",
                "type": "insight",
                "customers": ", ".join(s["customer"] for s in export_heavy[:5]),
            })
        if low_login:
            trends.append({
                "trend": f"{len(low_login)} customers below 30% login rate — adoption risk",
                "type": "concern",
                "customers": ", ".join(s["customer"] for s in low_login[:5]),
            })

        return {
            "total_active_users": total_active,
            "total_users": total_users,
            "overall_login_pct": round(total_active / total_users * 100) if total_users else 0,
            "trends": trends,
        }

    def _compute_portfolio_leaders(self, summaries: list[dict]) -> dict[str, list[dict]]:
        """Rank customers across key categories."""
        def _top(key_fn, label, n=5):
            ranked = sorted(summaries, key=key_fn, reverse=True)
            return [{"rank": i + 1, "customer": s["customer"], label: key_fn(s)}
                    for i, s in enumerate(ranked[:n]) if key_fn(s) > 0]

        return {
            "kei_adoption": _top(
                lambda s: s.get("kei", {}).get("adoption_rate", 0), "adoption_rate"),
            "executive_engagement": _top(
                lambda s: s.get("kei", {}).get("executive_users", 0), "executives"),
            "write_depth": _top(
                lambda s: s.get("depth", {}).get("write_ratio", 0), "write_ratio"),
            "export_intensity": _top(
                lambda s: s.get("exports", {}).get("total_exports", 0), "total_exports"),
            "login_rate": _top(
                lambda s: s.get("login_pct", 0), "login_pct"),
        }

    def rebuild_portfolio_aggregates(self, report: dict[str, Any]) -> None:
        """Recompute portfolio_signals, trends, leaders, and cohort rollup after filtering ``customers``."""
        summaries = [s for s in (report.get("customers") or []) if isinstance(s, dict)]
        report["customers"] = summaries
        report["customer_count"] = len(summaries)
        report["portfolio_signals"] = self._compute_portfolio_signals(summaries)
        report["portfolio_trends"] = self._compute_portfolio_trends(summaries)
        report["portfolio_leaders"] = self._compute_portfolio_leaders(summaries)
        digest, findings = compute_cohort_portfolio_rollup(summaries)
        report["cohort_digest"] = digest
        report["cohort_findings_bullets"] = findings

    def save_usage_to_file(
        self,
        data: dict[str, Any],
        output_path: str | Path,
    ) -> Path:
        """Save usage data to a JSON file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug("Saved usage data to %s", path)
        return path
