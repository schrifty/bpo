"""Pendo API client for the aggregation endpoint."""

import datetime
import json
import re
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests

from .config import (
    FEATURE_ADOPTION_INSIGHTS,
    PENDO_BASE_URL,
    PENDO_INTEGRATION_KEY,
    logger,
)


def _name_matches(query: str, text: str) -> bool:
    """Check if query appears as a word boundary match in text.
    'AGI' matches 'AGI Omaha' but not 'Integrated Packaging Machinery'.
    """
    if not query or not text:
        return False
    return bool(re.search(rf'\b{re.escape(query)}\b', text, re.IGNORECASE))


def _time_series(days: int) -> dict[str, Any]:
    """Build timeSeries for aggregation pipeline."""
    return {
        "period": "dayRange",
        "first": "now()",
        "count": -days,  # Negative = look back
    }


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


def compute_cohort_portfolio_rollup(
    customer_summaries: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Bucket portfolio rows by ``cohorts.yaml`` classification (via ``get_customer_cohort``).

    Does not define cohorts — only reads ``cohort`` from existing customer records.
    Returns ``(cohort_digest, findings_bullets)``.
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
        scores = [float(r.get("score") or 0) for r in rows]
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
            "median_score": _median_nums(scores),
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
    if len(with_data) < 2:
        bullets.append(
            "Only one cohort bucket has customers in this window — compare across cohorts when more accounts load.",
        )
    else:
        largest = with_data[0]
        bullets.append(
            f"Largest cohort in this deck: {largest[1]['display_name']} ({largest[1]['n']} customers).",
        )

    ge3 = [(cid, d) for cid, d in with_data if d["n"] >= 3]
    if len(ge3) >= 2:
        by_login = sorted(ge3, key=lambda x: (x[1].get("median_login_pct") or 0), reverse=True)
        hi, lo = by_login[0], by_login[-1]
        bullets.append(
            f"Median login % (cohorts with ≥3 customers): highest {hi[1]['display_name']} "
            f"({hi[1]['median_login_pct']}%) vs lowest {lo[1]['display_name']} ({lo[1]['median_login_pct']}%).",
        )
        by_write = sorted(ge3, key=lambda x: (x[1].get("median_write_ratio") or 0), reverse=True)
        w_hi, w_lo = by_write[0], by_write[-1]
        if w_hi[0] != w_lo[0]:
            bullets.append(
                f"Median write ratio: highest {w_hi[1]['display_name']} ({w_hi[1]['median_write_ratio']}%) "
                f"vs lowest {w_lo[1]['display_name']} ({w_lo[1]['median_write_ratio']}%).",
            )
        by_kei = sorted(ge3, key=lambda x: x[1].get("kei_adoption_pct") or 0, reverse=True)
        k_hi, k_lo = by_kei[0], by_kei[-1]
        if k_hi[0] != k_lo[0]:
            bullets.append(
                f"Kei adoption (share of customers with any query): highest {k_hi[1]['display_name']} "
                f"({k_hi[1]['kei_adoption_pct']}%) vs lowest {k_lo[1]['display_name']} ({k_lo[1]['kei_adoption_pct']}%).",
            )

    small = [d["display_name"] for _, d in with_data if d["n"] < 3]
    if small:
        bullets.append(
            f"Small sample (under 3 customers in this window): {', '.join(small[:6])}"
            f"{'…' if len(small) > 6 else ''} — treat medians as directional only.",
        )

    un = digest.get("unclassified", {})
    if un.get("n"):
        bullets.append(
            f"{un['n']} customer(s) are unclassified — add or alias them in cohorts.yaml to benchmark by industry cohort.",
        )

    bullets.append(
        "Cohort labels and membership come from cohorts.yaml and docs/CUSTOMER_COHORTS.md — not redefined in this deck.",
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
        logger.debug("PendoClient initialized (base_url=%s)", self.base_url)

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
        resp = requests.post(url, json=payload, headers=self._headers(), timeout=60)
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

    def get_usage_for_customer(
        self, customer: str, days: int = 30, include_usage_metrics: bool = True
    ) -> dict[str, Any]:
        """Get usage data for a customer over the last N days.
        If no visitor/account matches, falls back to site name matching (e.g. 'Safran' -> 'Safran Ventilation Systems').
        """
        result = self.get_visitors(days=days)
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
        """Get unique sites from visitor metadata (metadata.agent.siteid/sitename)."""
        result = self.get_visitors(days=days)
        sites: dict[int, dict[str, Any]] = {}
        for r in result.get("results", []) or []:
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
        result = self.get_visitors(days=days)
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
        result = self.get_visitors(days=days)
        sites: dict[int, dict[str, Any]] = {}
        for r in result.get("results", []) or []:
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

    # ── Catalog methods (for human-readable names) ──

    def get_page_catalog(self) -> dict[str, str]:
        """Fetch page catalog: {page_id: page_name}."""
        resp = requests.get(
            f"{self.base_url}/page", headers=self._headers(), timeout=30
        )
        resp.raise_for_status()
        pages = resp.json()
        return {p["id"]: p.get("name", p["id"]) for p in pages} if isinstance(pages, list) else {}

    def get_feature_catalog(self) -> dict[str, str]:
        """Fetch feature catalog: {feature_id: feature_name}."""
        resp = requests.get(
            f"{self.base_url}/feature", headers=self._headers(), timeout=30
        )
        resp.raise_for_status()
        features = resp.json()
        return {f["id"]: f.get("name", f["id"]) for f in features} if isinstance(features, list) else {}

    def get_account_info(self, account_id: str) -> dict[str, Any]:
        """Fetch account metadata from REST API."""
        resp = requests.get(
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
    _CACHE_TTL = 120  # seconds; overridden by preload() for batch runs
    _page_events_cache: dict[str, Any] | None = None
    _page_events_cache_ts: float = 0
    _track_events_cache: dict[str, Any] | None = None
    _track_events_cache_ts: float = 0
    _guide_events_cache: dict[str, Any] | None = None
    _guide_events_cache_ts: float = 0
    _page_catalog_cache: dict[str, str] | None = None
    _guide_catalog_cache: dict[str, str] | None = None
    _usage_by_site_cache: dict[str, Any] | None = None
    _usage_by_site_cache_ts: float = 0
    _usage_by_site_entity_cache: dict[str, Any] | None = None
    _usage_by_site_entity_cache_ts: float = 0
    _cache_lock = threading.Lock()

    def _cache_valid(self, ts: float) -> bool:
        return (time.time() - ts) < self._CACHE_TTL

    def _get_visitor_partition(self, days: int = 30) -> dict[str, Any]:
        """Fetch all visitors and partition by customer. Cached for 120s to avoid
        redundant API calls when the agent invokes multiple tools in sequence."""
        now = time.time()
        if self._visitor_cache and (now - self._visitor_cache_ts) < self._CACHE_TTL:
            cached_days = self._visitor_cache.get("days")
            if cached_days == days:
                return self._visitor_cache

        now_ms = int(now * 1000)
        all_visitors = self.get_visitors(days=days).get("results", [])

        def _is_internal(v: dict) -> bool:
            agent = (v.get("metadata") or {}).get("agent") or {}
            return bool(agent.get("isinternaluser")) or agent.get("role") == "LeanDNAStaff"

        all_customer_stats: dict[str, dict] = {}
        for v in all_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            if _is_internal(v):
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
            "_is_internal": _is_internal,
        }
        self._visitor_cache = result
        self._visitor_cache_ts = now
        return result

    def _get_page_events_cached(self, days: int) -> list[dict]:
        if self._page_events_cache and self._cache_valid(self._page_events_cache_ts):
            if self._page_events_cache.get("days") == days:
                return self._page_events_cache["results"]
        ts = _time_series(days)
        results = self.aggregate([
            {"source": {"pageEvents": None, "timeSeries": ts}},
        ]).get("results", [])
        self._page_events_cache = {"days": days, "results": results}
        self._page_events_cache_ts = time.time()
        return results

    def _get_track_events_cached(self, days: int) -> list[dict]:
        if self._track_events_cache and self._cache_valid(self._track_events_cache_ts):
            if self._track_events_cache.get("days") == days:
                return self._track_events_cache["results"]
        ts = _time_series(days)
        results = self.aggregate([
            {"source": {"events": None, "timeSeries": ts}},
        ]).get("results", [])
        self._track_events_cache = {"days": days, "results": results}
        self._track_events_cache_ts = time.time()
        return results

    def _get_guide_events_cached(self, days: int) -> list[dict]:
        if self._guide_events_cache and self._cache_valid(self._guide_events_cache_ts):
            if self._guide_events_cache.get("days") == days:
                return self._guide_events_cache["results"]
        ts = _time_series(days)
        results = self.aggregate([
            {"source": {"guideEvents": None, "timeSeries": ts}},
        ]).get("results", [])
        self._guide_events_cache = {"days": days, "results": results}
        self._guide_events_cache_ts = time.time()
        return results

    def _get_page_catalog_cached(self) -> dict[str, str]:
        if self._page_catalog_cache is not None:
            return self._page_catalog_cache
        self._page_catalog_cache = self.get_page_catalog()
        return self._page_catalog_cache

    def _get_guide_catalog_cached(self) -> dict[str, str]:
        if self._guide_catalog_cache is not None:
            return self._guide_catalog_cache
        try:
            resp = requests.get(
                f"{self.base_url}/guide",
                headers={"x-pendo-integration-key": self.integration_key, "content-type": "application/json"},
                timeout=30,
            )
            if resp.ok:
                self._guide_catalog_cache = {g["id"]: g.get("name", g["id"]) for g in resp.json()}
            else:
                self._guide_catalog_cache = {}
        except Exception:
            self._guide_catalog_cache = {}
        return self._guide_catalog_cache

    def _get_usage_by_site_cached(self, days: int) -> dict[str, Any]:
        if self._usage_by_site_cache and self._cache_valid(self._usage_by_site_cache_ts):
            if self._usage_by_site_cache.get("days") == days:
                return self._usage_by_site_cache
        result = self.get_usage_by_site(days=days)
        result["days"] = days
        self._usage_by_site_cache = result
        self._usage_by_site_cache_ts = time.time()
        return result

    def _get_usage_by_site_entity_cached(self, days: int) -> dict[str, Any]:
        if self._usage_by_site_entity_cache and self._cache_valid(self._usage_by_site_entity_cache_ts):
            if self._usage_by_site_entity_cache.get("days") == days:
                return self._usage_by_site_entity_cache
        result = self.get_usage_by_site_and_entity(days=days)
        result["days"] = days
        self._usage_by_site_entity_cache = result
        self._usage_by_site_entity_cache_ts = time.time()
        return result

    def preload(self, days: int = 30) -> None:
        """Prefetch all global data for a batch run. Sets TTL to 1 hour.
        Fetches all data sources in parallel to minimize wall-clock time."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self._CACHE_TTL = 3600
        logger.info("Preloading global data for %d-day window (parallel)...", days)
        t0 = time.time()

        loaders = {
            "visitors": lambda: self._get_visitor_partition(days),
            "feature events": lambda: self._get_feature_events_cached(days),
            "page events": lambda: self._get_page_events_cached(days),
            "track events": lambda: self._get_track_events_cached(days),
            "guide events": lambda: self._get_guide_events_cached(days),
            "page catalog": lambda: self._get_page_catalog_cached(),
            "feature catalog": lambda: self.get_feature_catalog(),
            "guide catalog": lambda: self._get_guide_catalog_cached(),
            "usage by site": lambda: self._get_usage_by_site_cached(days),
        }

        with ThreadPoolExecutor(max_workers=len(loaders)) as pool:
            futures = {pool.submit(fn): name for name, fn in loaders.items()}
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    fut.result()
                    logger.info("  %s: OK", name)
                except Exception as e:
                    logger.warning("  %s: FAILED (%s)", name, e)

        logger.info("Preload complete in %.1fs", time.time() - t0)

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
            signals.append(f"Strong engagement: {customer_rate:.0%} weekly active rate vs {bench_rate:.0%} peer median")
        elif customer_rate < bench_rate * 0.5 and total_visitors >= 5:
            signals.append(f"Low engagement: {customer_rate:.0%} weekly active rate vs {bench_rate:.0%} peer median")
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
                "data_traces": [
                    {
                        "description": "Weekly active rate (this account)",
                        "source": "Pendo",
                        "query": (
                            "active_7d / total_visitors over the report window; "
                            "7-day activity from visitor time-bucket aggregation"
                        ),
                    },
                    {
                        "description": "All-customer median active rate",
                        "source": "Pendo",
                        "query": (
                            "Median of the same weekly active rate across accounts "
                            "with Pendo data in the same period (peer_count in payload)"
                        ),
                    },
                    {
                        "description": "Cohort median active rate (when shown)",
                        "source": "Pendo + cohorts.yaml",
                        "query": (
                            "Median among accounts in the same manufacturing cohort "
                            "(get_customer_cohort / cohorts.yaml); only if cohort n≥3"
                        ),
                    },
                    {
                        "description": "Account size (users, sites) on slide",
                        "source": "Pendo",
                        "query": (
                            "account.total_visitors, account.total_sites from visitor records "
                            "and sitenames metadata for this customer"
                        ),
                    },
                ],
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
        if self._categorized_features_cache is not None:
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
        now = time.time()
        if self._feat_events_cache and (now - self._feat_events_cache_ts) < self._CACHE_TTL:
            if self._feat_events_cache.get("days") == days:
                return self._feat_events_cache["results"]

        ts = _time_series(days)
        results = self.aggregate([
            {"source": {"featureEvents": None, "timeSeries": ts}},
        ]).get("results", [])

        self._feat_events_cache = {"days": days, "results": results}
        self._feat_events_cache_ts = now
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
            by_type[t] = by_type.get(t, 0) + 1
            gid = ev.get("guideId", "?")
            if gid not in by_guide:
                by_guide[gid] = {}
            by_guide[gid][t] = by_guide[gid].get(t, 0) + 1
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

    def get_customer_health_report(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Comprehensive health report combining all focused methods.
        Used by the monolith deck generator and as a convenience method."""
        # Fetch all sub-reports first, then pass them to health for signal generation
        sites_data = self.get_customer_sites(customer_name, days)
        features_data = self.get_customer_features(customer_name, days)
        people_data = self.get_customer_people(customer_name, days)
        exports_data = self.get_customer_exports(customer_name, days)
        depth_data = self.get_customer_depth(customer_name, days)
        kei_data = self.get_customer_kei(customer_name, days)
        guides_data = self.get_customer_guides(customer_name, days)

        health = self.get_customer_health(customer_name, days,
            _precomputed_signals={"depth": depth_data, "exports": exports_data,
                                   "kei": kei_data, "guides": guides_data})
        if "error" in health:
            return health

        # JIRA data (optional — skipped if JIRA is not configured)
        jira_data = {}
        try:
            from .jira_client import JiraClient
            jira_data = JiraClient().get_customer_jira(customer_name, days=90)
        except Exception as e:
            from .qa import qa
            qa.flag(f"JIRA data unavailable: {str(e)[:80]}",
                    sources=("JIRA API",), severity="warning")

        # Salesforce — required when configured (preflight); skipped if not configured
        from .data_source_health import _salesforce_configured
        salesforce_data: dict = {}
        if _salesforce_configured():
            from .salesforce_client import SalesforceClient
            sf = SalesforceClient()
            salesforce_data = sf.get_customer_salesforce(customer_name)

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

        return {
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
            "jira": jira_data,
            "salesforce": salesforce_data,
            "cs_platform_health": cs_platform_health,
            "cs_supply_chain": cs_supply_chain,
            "cs_platform_value": cs_platform_value,
        }

    # ── Portfolio-level methods (cross-customer analysis) ──

    def get_portfolio_report(self, days: int = 30, max_customers: int | None = None) -> dict[str, Any]:
        """Full portfolio report for the book-of-business deck.
        Calls preload() then iterates all customers."""
        self.preload(days)
        by_customer = self.get_sites_by_customer(days)
        all_names = [c for c in by_customer["customer_list"] if c != "(unknown)"]
        if max_customers:
            all_names = all_names[:max_customers]

        customer_summaries: list[dict[str, Any]] = []
        for name in all_names:
            try:
                h = self.get_customer_health(name, days)
                if "error" in h:
                    continue
                depth = self.get_customer_depth(name, days)
                kei = self.get_customer_kei(name, days)
                guides = self.get_customer_guides(name, days)
                exports = self.get_customer_exports(name, days)
                customer_summaries.append({
                    "customer": name,
                    "engagement": h.get("engagement", {}),
                    "benchmarks": h.get("benchmarks", {}),
                    "signals": h.get("signals", []),
                    "score": h.get("engagement", {}).get("score", 0),
                    "active_users": h.get("engagement", {}).get("active_users", 0),
                    "total_users": h.get("engagement", {}).get("total_users", 0),
                    "login_pct": h.get("engagement", {}).get("login_pct", 0),
                    "depth": depth,
                    "kei": kei,
                    "guides": guides,
                    "exports": exports,
                })
            except Exception as e:
                logger.debug("Skipping %s: %s", name, e)

        cohort_digest, cohort_findings_bullets = compute_cohort_portfolio_rollup(customer_summaries)

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
        alarm_keywords = [
            "no active users", "declining", "dropped", "no kei", "dismiss",
            "read-heavy", "low guide reach", "only", "at risk", "churned",
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
        return signals[:20]

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
            "engagement_score": _top(
                lambda s: s.get("score", 0), "score"),
            "write_depth": _top(
                lambda s: s.get("depth", {}).get("write_ratio", 0), "write_ratio"),
            "export_intensity": _top(
                lambda s: s.get("exports", {}).get("total_exports", 0), "total_exports"),
            "login_rate": _top(
                lambda s: s.get("login_pct", 0), "login_pct"),
        }

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
