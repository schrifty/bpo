"""Pendo API client for the aggregation endpoint."""

import json
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import requests

from .config import PENDO_BASE_URL, PENDO_INTEGRATION_KEY, logger


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
