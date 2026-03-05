"""Pendo API client for the aggregation endpoint."""

import datetime
import json
import re
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests

from .config import PENDO_BASE_URL, PENDO_INTEGRATION_KEY, logger


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


def extract_customer_from_sitename(sitename: str) -> str:
    """Extract customer from sitename. Format is '{customer} {Site}' (e.g. 'Safran Ventilation Systems' -> 'Safran')."""
    if not sitename or not isinstance(sitename, str):
        return ""
    parts = sitename.strip().split()
    return parts[0] if parts else ""


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

    # ── Cached visitor partition (shared across focused data methods) ──

    _visitor_cache: dict[str, Any] | None = None
    _visitor_cache_ts: float = 0
    _CACHE_TTL = 120  # seconds

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

    def get_customer_health(self, customer_name: str, days: int = 30) -> dict[str, Any]:
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

        peer_rates = []
        for stats in partition["all_customer_stats"].values():
            if stats["total"] >= 5:
                peer_rates.append(stats["active_7d"] / stats["total"])
        peer_rates.sort()
        median_rate = peer_rates[len(peer_rates) // 2] if peer_rates else 0

        # Auto-detect signals
        signals: list[str] = []
        dormant_pct = engagement["dormant"] / max(total_visitors, 1)
        if dormant_pct > 0.5:
            signals.append(f"High dormancy: {engagement['dormant']}/{total_visitors} users ({dormant_pct:.0%}) inactive 30+ days")
        if customer_rate > median_rate * 1.5 and total_visitors >= 5:
            signals.append(f"Strong engagement: {customer_rate:.0%} weekly active rate vs {median_rate:.0%} peer median")
        elif customer_rate < median_rate * 0.5 and total_visitors >= 5:
            signals.append(f"Low engagement: {customer_rate:.0%} weekly active rate vs {median_rate:.0%} peer median")
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

        exec_roles = {"ExecutiveVP", "Director", "VP", "C-Level", "Executive"}
        user_activity = self._build_user_activity(customer_visitors, now_ms)
        exec_active = sum(1 for u in user_activity if u["role"] in exec_roles and u["days_inactive"] <= 7)
        exec_total = sum(1 for u in user_activity if u["role"] in exec_roles)
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

        # Behavioral depth signals
        try:
            depth_data = self.get_customer_depth(customer_name, days)
            write_ratio = depth_data.get("write_ratio", 0)
            collab_events = depth_data.get("collab_events", 0)
            if write_ratio >= 40:
                signals.append(f"Deep write adoption: {write_ratio}% write ratio (running operations in-app)")
            elif write_ratio <= 10:
                signals.append(f"Read-heavy usage: only {write_ratio}% write ratio (may be dashboard-only)")
            if collab_events > 0:
                signals.append(f"In-app collaboration: {collab_events:,} comment/chat/attachment events")
        except Exception:
            pass

        # Export intensity signal
        try:
            export_data = self.get_customer_exports(customer_name, days)
            total_exports = export_data.get("total_exports", 0)
            exports_per_user = export_data.get("exports_per_active_user", 0)
            if total_exports > 0:
                signals.append(f"Export activity: {total_exports:,} exports ({exports_per_user}/active user)")
        except Exception:
            pass

        # Kei AI signal
        try:
            kei_data = self.get_customer_kei(customer_name, days)
            kei_queries = kei_data.get("total_queries", 0)
            kei_exec = kei_data.get("executive_users", 0)
            if kei_queries > 0:
                msg = f"Kei AI active: {kei_queries:,} queries from {kei_data.get('unique_users', 0)} users"
                if kei_exec > 0:
                    msg += f" (incl. {kei_exec} executives)"
                signals.append(msg)
            else:
                signals.append("No Kei AI usage detected — rollout opportunity")
        except Exception:
            pass

        # Guide engagement signal
        try:
            guide_data = self.get_customer_guides(customer_name, days)
            dismiss_rate = guide_data.get("dismiss_rate", 0)
            guide_reach = guide_data.get("guide_reach", 0)
            if dismiss_rate > 30:
                signals.append(f"High guide dismiss rate: {dismiss_rate}% — possible onboarding friction")
            if guide_reach < 30 and guide_data.get("active_users", 0) > 5:
                signals.append(f"Low guide reach: only {guide_reach}% of active users see guides")
        except Exception:
            pass

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
                "peer_count": len(peer_rates),
            },
            "signals": signals,
        }

    def get_customer_sites(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Per-site metrics: visitors, page views, feature clicks, events, minutes, last active."""
        partition = self._get_visitor_partition(days)
        now_ms = partition["now_ms"]
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        site_data: dict[str, dict] = {}
        for v in customer_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            lv = auto.get("lastvisit", 0)
            for sn in agent.get("sitenames") or []:
                if not sn or not _name_matches(customer_name, str(sn)):
                    continue
                if sn not in site_data:
                    site_data[sn] = {"visitors": 0, "last_visit_ms": 0}
                site_data[sn]["visitors"] += 1
                if lv and lv > site_data[sn]["last_visit_ms"]:
                    site_data[sn]["last_visit_ms"] = lv

        usage_by_site = self.get_usage_by_site(days=days)
        usage_map = {r["sitename"]: r for r in (usage_by_site.get("results") or [])}

        sites = []
        for sn, info in sorted(site_data.items()):
            usage = usage_map.get(sn, {})
            lv_ms = info["last_visit_ms"]
            sites.append({
                "sitename": sn,
                "visitors": info["visitors"],
                "page_views": usage.get("page_views", 0),
                "feature_clicks": usage.get("feature_clicks", 0),
                "total_events": usage.get("total_events", 0),
                "total_minutes": usage.get("total_minutes", 0),
                "last_active": datetime.datetime.fromtimestamp(lv_ms / 1000).strftime("%Y-%m-%d") if lv_ms else "N/A",
            })
        return {"customer": customer_name, "days": days, "sites": sites}

    def get_customer_features(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Top pages and features this customer uses, with human-readable names."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        visitor_ids = {v.get("visitorId") for v in customer_visitors if v.get("visitorId")}
        ts = _time_series(days)

        page_catalog = {}
        feature_catalog = {}
        try:
            page_catalog = self.get_page_catalog()
        except Exception:
            pass
        try:
            feature_catalog = self.get_feature_catalog()
        except Exception:
            pass

        top_pages: list[dict] = []
        top_features: list[dict] = []

        try:
            all_page_events = self.aggregate([
                {"source": {"pageEvents": None, "timeSeries": ts}},
            ]).get("results", [])
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
            all_feat_events = self.aggregate([
                {"source": {"featureEvents": None, "timeSeries": ts}},
            ]).get("results", [])
            feat_counts: dict[str, int] = {}
            for ev in all_feat_events:
                if ev.get("visitorId") in visitor_ids:
                    fid = ev.get("featureId", "")
                    feat_counts[fid] = feat_counts.get(fid, 0) + (ev.get("numEvents", 0) or 0)
            for fid, count in sorted(feat_counts.items(), key=lambda x: -x[1])[:10]:
                top_features.append({"name": feature_catalog.get(fid, fid), "events": count})
        except Exception as e:
            logger.debug("Could not compute top features: %s", e)

        return {"customer": customer_name, "days": days, "top_pages": top_pages, "top_features": top_features}

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
            ts = _time_series(days)
            track_results = self.aggregate([
                {"source": {"events": None, "timeSeries": ts}},
            ]).get("results", [])
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

        exec_roles = {"ExecutiveVP", "Director", "VP", "C-Level", "Executive"}
        users = []
        exec_users = 0
        exec_queries = 0
        for vid, count in sorted(by_user.items(), key=lambda x: -x[1]):
            info = vid_to_info.get(vid, {})
            role = info.get("role", "Unknown")
            is_exec = role in exec_roles
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

        ts = _time_series(days)
        try:
            guide_events = self.aggregate([
                {"source": {"guideEvents": None, "timeSeries": ts}},
            ]).get("results", [])
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
        active = self._count_active_users(customer_visitors, partition["now_ms"])

        # Resolve guide names
        guide_names = {}
        try:
            resp = requests.get(
                f"{self.base_url}/guide",
                headers={"x-pendo-integration-key": self.integration_key, "content-type": "application/json"},
            )
            if resp.ok:
                for g in resp.json():
                    guide_names[g["id"]] = g.get("name", g["id"])
        except Exception:
            pass

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
            "active_users": active,
            "guide_reach": round(len(users_with_guides) / max(active, 1) * 100, 1),
            "seen": seen,
            "advanced": advanced,
            "dismissed": dismissed,
            "dismiss_rate": round(dismissed / max(seen, 1) * 100, 1),
            "advance_rate": round(advanced / max(seen, 1) * 100, 1),
            "top_guides": top_guides,
        }

    def get_customer_people(self, customer_name: str, days: int = 30) -> dict[str, Any]:
        """Champions (most active) and at-risk users (dormant), with roles and last visit."""
        partition = self._get_visitor_partition(days)
        customer_visitors, _ = self._filter_customer_visitors(customer_name, partition)
        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        user_activity = self._build_user_activity(customer_visitors, partition["now_ms"])
        champions = sorted(user_activity, key=lambda u: u["days_inactive"])[:5]
        at_risk = sorted(
            [u for u in user_activity if u["days_inactive"] > 30],
            key=lambda u: -u["days_inactive"],
        )[:8]
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
        health = self.get_customer_health(customer_name, days)
        if "error" in health:
            return health

        sites_data = self.get_customer_sites(customer_name, days)
        features_data = self.get_customer_features(customer_name, days)
        people_data = self.get_customer_people(customer_name, days)
        exports_data = self.get_customer_exports(customer_name, days)
        depth_data = self.get_customer_depth(customer_name, days)
        kei_data = self.get_customer_kei(customer_name, days)
        guides_data = self.get_customer_guides(customer_name, days)

        return {
            **health,
            "sites": sites_data.get("sites", []),
            "top_pages": features_data.get("top_pages", []),
            "top_features": features_data.get("top_features", []),
            "champions": people_data.get("champions", []),
            "at_risk_users": people_data.get("at_risk_users", []),
            "exports": exports_data,
            "depth": depth_data,
            "kei": kei_data,
            "guides": guides_data,
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
