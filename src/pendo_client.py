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

    # ── Health report (comprehensive data for CS decks) ──

    def get_customer_health_report(
        self,
        customer_name: str,
        days: int = 30,
    ) -> dict[str, Any]:
        """Gather all data needed for a CS-oriented health deck for one customer.

        Returns a dict with sections matching the deck slide structure:
        - account: name, csm, region, sites
        - engagement: tiers, role breakdown, total visitors
        - sites: per-site metrics (visitors, events, minutes, last active)
        - top_pages: top pages with human-readable names
        - top_features: top features with human-readable names
        - champions: most active users (name, role, events)
        - at_risk_users: dormant users with roles
        - benchmarks: customer's active rate vs peer median
        - signals: automatically detected notable patterns
        """
        now_ms = int(time.time() * 1000)
        all_visitors = self.get_visitors(days=days).get("results", [])

        def _is_internal(v: dict) -> bool:
            agent = (v.get("metadata") or {}).get("agent") or {}
            return bool(agent.get("isinternaluser")) or agent.get("role") == "LeanDNAStaff"

        # ── Partition visitors by customer (excluding internal users from stats) ──
        customer_visitors: list[dict] = []
        internal_visitors: list[dict] = []
        all_customer_stats: dict[str, dict] = {}

        for v in all_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            sitenames = agent.get("sitenames") or []
            lv = auto.get("lastvisit", 0)
            internal = _is_internal(v)

            if not internal:
                for sn in sitenames:
                    cust = str(sn).strip().split()[0] if sn else "?"
                    if cust not in all_customer_stats:
                        all_customer_stats[cust] = {"total": 0, "active_7d": 0}
                    all_customer_stats[cust]["total"] += 1
                    if lv and (now_ms - lv) / (86400 * 1000) <= 7:
                        all_customer_stats[cust]["active_7d"] += 1
                    break

            if any(_name_matches(customer_name, str(sn)) for sn in sitenames):
                if internal:
                    internal_visitors.append(v)
                else:
                    customer_visitors.append(v)

        if not customer_visitors:
            return {"error": f"No visitors found matching '{customer_name}'"}

        # ── Account info ──
        sample_agent = (customer_visitors[0].get("metadata") or {}).get("agent") or {}
        csm_names = set()
        for v in customer_visitors:
            on = ((v.get("metadata") or {}).get("agent") or {}).get("ownername")
            if on:
                csm_names.add(on)

        # Find the account ID — prefer accounts whose name starts with the customer
        account_id = None
        account_ids_seen: dict[str, int] = {}
        for v in customer_visitors:
            auto = (v.get("metadata") or {}).get("auto") or {}
            aid = auto.get("accountid")
            if aid:
                aid_str = str(aid)
                account_ids_seen[aid_str] = account_ids_seen.get(aid_str, 0) + 1

        if account_ids_seen:
            # Try each candidate account and prefer one whose name matches the customer
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
        # Use account name only if it starts with the customer name (word boundary)
        if not acct_name or not acct_name.lower().startswith(customer_name.lower()):
            acct_name = customer_name

        # ── Engagement tiers ──
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
        active_count = engagement["active_7d"] + engagement["active_30d"]

        # ── Per-site breakdown (only sites whose name matches the customer) ──
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

        sites_report = []
        for sn, info in sorted(site_data.items()):
            usage = usage_map.get(sn, {})
            lv_ms = info["last_visit_ms"]
            sites_report.append({
                "sitename": sn,
                "visitors": info["visitors"],
                "page_views": usage.get("page_views", 0),
                "feature_clicks": usage.get("feature_clicks", 0),
                "total_events": usage.get("total_events", 0),
                "total_minutes": usage.get("total_minutes", 0),
                "last_active": datetime.datetime.fromtimestamp(lv_ms / 1000).strftime("%Y-%m-%d") if lv_ms else "N/A",
            })

        # ── Top pages & features (via aggregation, grouped by ID) ──
        ts = _time_series(days)
        page_catalog = {}
        feature_catalog = {}
        try:
            page_catalog = self.get_page_catalog()
        except Exception as e:
            logger.debug("Could not fetch page catalog: %s", e)
        try:
            feature_catalog = self.get_feature_catalog()
        except Exception as e:
            logger.debug("Could not fetch feature catalog: %s", e)

        top_pages: list[dict] = []
        top_features: list[dict] = []

        try:
            page_pipeline = [
                {"source": {"pageEvents": None, "timeSeries": ts}},
                {"group": {
                    "group": ["pageId"],
                    "fields": {
                        "totalEvents": {"sum": "numEvents"},
                        "totalMinutes": {"sum": "numMinutes"},
                    },
                }},
                {"sort": ["totalEvents desc"]},
                {"limit": 200},
            ]
            page_results = self.aggregate(page_pipeline).get("results", [])

            # We need per-visitor attribution to filter to this customer.
            # Faster: use visitor-level page events and filter client-side.
            visitor_ids = {
                v.get("visitorId") for v in customer_visitors if v.get("visitorId")
            }
            page_by_visitor = [
                {"source": {"pageEvents": None, "timeSeries": ts}},
            ]
            all_page_events = self.aggregate(page_by_visitor).get("results", [])
            customer_page_counts: dict[str, dict] = {}
            for ev in all_page_events:
                if ev.get("visitorId") in visitor_ids:
                    pid = ev.get("pageId", "")
                    if pid not in customer_page_counts:
                        customer_page_counts[pid] = {"events": 0, "minutes": 0}
                    customer_page_counts[pid]["events"] += ev.get("numEvents", 0) or 0
                    customer_page_counts[pid]["minutes"] += ev.get("numMinutes", 0) or 0

            for pid, counts in sorted(customer_page_counts.items(), key=lambda x: -x[1]["events"])[:10]:
                top_pages.append({
                    "name": page_catalog.get(pid, pid),
                    "events": counts["events"],
                    "minutes": counts["minutes"],
                })
        except Exception as e:
            logger.debug("Could not compute top pages: %s", e)

        try:
            all_feat_events = self.aggregate([
                {"source": {"featureEvents": None, "timeSeries": ts}},
            ]).get("results", [])

            visitor_ids = {
                v.get("visitorId") for v in customer_visitors if v.get("visitorId")
            }
            customer_feat_counts: dict[str, int] = {}
            for ev in all_feat_events:
                if ev.get("visitorId") in visitor_ids:
                    fid = ev.get("featureId", "")
                    customer_feat_counts[fid] = customer_feat_counts.get(fid, 0) + (ev.get("numEvents", 0) or 0)

            for fid, count in sorted(customer_feat_counts.items(), key=lambda x: -x[1])[:10]:
                top_features.append({
                    "name": feature_catalog.get(fid, fid),
                    "events": count,
                })
        except Exception as e:
            logger.debug("Could not compute top features: %s", e)

        # ── Champions (most active) & at-risk (dormant) ──
        user_activity: list[dict] = []
        for v in customer_visitors:
            agent = (v.get("metadata") or {}).get("agent") or {}
            auto = (v.get("metadata") or {}).get("auto") or {}
            lv = auto.get("lastvisit", 0)
            email = agent.get("emailaddress", "")
            role = agent.get("role", "Unknown")
            days_ago = (now_ms - lv) / (86400 * 1000) if lv else 999
            user_activity.append({
                "email": email,
                "role": role,
                "last_visit": datetime.datetime.fromtimestamp(lv / 1000).strftime("%Y-%m-%d") if lv else "Never",
                "days_inactive": round(days_ago, 1),
            })

        champions = sorted(user_activity, key=lambda u: u["days_inactive"])[:5]
        at_risk = sorted(
            [u for u in user_activity if u["days_inactive"] > 30],
            key=lambda u: -u["days_inactive"],
        )[:8]

        # ── Benchmarks ──
        peer_rates = []
        for cname, stats in all_customer_stats.items():
            if stats["total"] >= 5:
                peer_rates.append(stats["active_7d"] / stats["total"])
        peer_rates.sort()
        median_rate = peer_rates[len(peer_rates) // 2] if peer_rates else 0
        customer_rate = engagement["active_7d"] / max(total_visitors, 1)

        # ── Signals (auto-detected notable patterns) ──
        signals: list[str] = []

        dormant_pct = engagement["dormant"] / max(total_visitors, 1)
        if dormant_pct > 0.5:
            signals.append(f"High dormancy: {engagement['dormant']}/{total_visitors} users ({dormant_pct:.0%}) haven't logged in for 30+ days")

        if customer_rate > median_rate * 1.5 and total_visitors >= 5:
            signals.append(f"Strong engagement: {customer_rate:.0%} weekly active rate is well above the {median_rate:.0%} peer median")
        elif customer_rate < median_rate * 0.5 and total_visitors >= 5:
            signals.append(f"Low engagement: {customer_rate:.0%} weekly active rate is well below the {median_rate:.0%} peer median")

        if active_count > 0:
            top_user_events = 0
            total_cust_events = sum(s.get("total_events", 0) for s in sites_report)
            # Single-user dependency is hard to compute without per-user event counts here,
            # but we can flag if only 1-2 users are active
            if engagement["active_7d"] <= 2 and total_visitors >= 10:
                signals.append(f"Concentration risk: only {engagement['active_7d']} of {total_visitors} users active this week")

        exec_roles = {"ExecutiveVP", "Director", "VP", "C-Level", "Executive"}
        exec_active = sum(1 for u in user_activity if u["role"] in exec_roles and u["days_inactive"] <= 7)
        exec_total = sum(1 for u in user_activity if u["role"] in exec_roles)
        if exec_total > 0 and exec_active == 0:
            signals.append(f"No executive engagement: {exec_total} executives on the account, none active this week")
        elif exec_active > 0:
            signals.append(f"Executive engagement: {exec_active}/{exec_total} executives active this week")

        training_site_visitors = 0
        for sn, info in site_data.items():
            if "training" in sn.lower():
                training_site_visitors += info["visitors"]
        if training_site_visitors > 0:
            signals.append(f"Active training: {training_site_visitors} users on training site (onboarding/expansion signal)")

        if internal_visitors:
            signals.append(f"Internal activity: {len(internal_visitors)} LeanDNA staff visited this account's sites (excluded from metrics above)")

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
                "total_sites": len(site_data),
            },
            "engagement": {
                "active_7d": engagement["active_7d"],
                "active_30d": engagement["active_30d"],
                "dormant": engagement["dormant"],
                "active_rate_7d": round(customer_rate * 100, 1),
                "role_active": dict(sorted(role_active.items(), key=lambda x: -x[1])),
                "role_dormant": dict(sorted(role_dormant.items(), key=lambda x: -x[1])),
            },
            "sites": sites_report,
            "top_pages": top_pages,
            "top_features": top_features,
            "champions": champions,
            "at_risk_users": at_risk,
            "benchmarks": {
                "customer_active_rate": round(customer_rate * 100, 1),
                "peer_median_rate": round(median_rate * 100, 1),
                "peer_count": len(peer_rates),
                "total_visitors_rank": "top" if total_visitors > 100 else "mid" if total_visitors > 20 else "small",
            },
            "signals": signals,
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
