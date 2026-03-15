"""JIRA Cloud client for fetching customer-related issues."""

from base64 import b64encode
from typing import Any

import requests

from .config import JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN, logger

CUSTOMER_FIELD = "customfield_10100"   # "Customer" multi-select
ORG_FIELD = "customfield_10502"        # "Organizations" (JSM)
SITE_IDS_FIELD = "customfield_10613"   # "Site IDs"
SEVERITY_FIELD = "customfield_10629"   # "Bug Severity"
TTFR_FIELD = "customfield_10666"       # "Time to first response" (JSM SLA)
TTR_FIELD = "customfield_10665"        # "Time to resolution" (JSM SLA)
SENTIMENT_FIELD = "customfield_10685"  # "Sentiment" (AI-detected)
REQUEST_TYPE_FIELD = "customfield_10604"  # "Request Type" (JSM)
SITE_CMDB_FIELD = "customfield_11121"   # "Site" (CMDB object ref)
ENTITY_CMDB_FIELD = "customfield_11154"  # "Entity" (CMDB object ref)

_ISSUE_FIELDS = [
    "summary", "status", "issuetype", "project", "priority",
    "labels", "components", "created", "updated", "resolution",
    "assignee", "reporter",
    CUSTOMER_FIELD, ORG_FIELD, SITE_IDS_FIELD, SEVERITY_FIELD,
    TTFR_FIELD, TTR_FIELD, SENTIMENT_FIELD, REQUEST_TYPE_FIELD,
    SITE_CMDB_FIELD, ENTITY_CMDB_FIELD,
]


class JiraClient:
    def __init__(self):
        if not all([JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN]):
            raise ValueError("JIRA_URL, JIRA_EMAIL, and JIRA_API_TOKEN must be set in .env")
        self.base_url = JIRA_URL.rstrip("/")
        auth = b64encode(f"{JIRA_EMAIL}:{JIRA_API_TOKEN}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        }

    def _search(self, jql: str, max_results: int = 100) -> list[dict]:
        results: list[dict] = []
        next_token: str | None = None
        while len(results) < max_results:
            body: dict[str, Any] = {
                "jql": jql,
                "maxResults": min(max_results - len(results), 100),
                "fields": _ISSUE_FIELDS,
            }
            if next_token:
                body["nextPageToken"] = next_token
            resp = requests.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body, timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("issues", []))
            if data.get("isLast", True):
                break
            next_token = data.get("nextPageToken")
            if not next_token:
                break
        return results

    def _normalize_issue(self, issue: dict) -> dict:
        f = issue["fields"]
        orgs = f.get(ORG_FIELD) or []
        org_names = [o.get("name", "") for o in orgs if isinstance(o, dict)]
        custs = f.get(CUSTOMER_FIELD) or []
        cust_names = [c.get("value", c.get("name", "")) for c in custs if isinstance(c, dict)]
        severity = f.get(SEVERITY_FIELD)
        sev_val = severity.get("value", "") if isinstance(severity, dict) else ""

        def _parse_sla(field_key):
            data = f.get(field_key) or {}
            ms = None; breached = False; waiting = False
            completed = data.get("completedCycles", [])
            if completed:
                ms = completed[0].get("elapsedTime", {}).get("millis")
                breached = completed[0].get("breached", False)
            elif data.get("ongoingCycle"):
                waiting = True
            return ms, breached, waiting

        ttfr_ms, ttfr_breached, ttfr_waiting = _parse_sla(TTFR_FIELD)
        ttr_ms, ttr_breached, ttr_waiting = _parse_sla(TTR_FIELD)

        sentiment_raw = f.get(SENTIMENT_FIELD) or []
        sentiment = sentiment_raw[0].get("name", "") if sentiment_raw and isinstance(sentiment_raw, list) else ""

        req_type_raw = f.get(REQUEST_TYPE_FIELD) or {}
        req_type = ""
        if isinstance(req_type_raw, dict):
            rt = req_type_raw.get("requestType") or {}
            req_type = rt.get("name", "") if isinstance(rt, dict) else ""

        return {
            "key": issue["key"],
            "summary": f.get("summary", ""),
            "type": f.get("issuetype", {}).get("name", ""),
            "status": f.get("status", {}).get("name", ""),
            "priority": f.get("priority", {}).get("name", "") if f.get("priority") else "",
            "severity": sev_val,
            "project": f.get("project", {}).get("key", ""),
            "labels": f.get("labels", []),
            "created": f.get("created", "")[:10],
            "updated": f.get("updated", "")[:10],
            "resolution": f.get("resolution", {}).get("name", "") if f.get("resolution") else "",
            "assignee": f.get("assignee", {}).get("displayName", "") if f.get("assignee") else "",
            "reporter": f.get("reporter", {}).get("displayName", "") if f.get("reporter") else "",
            "organizations": org_names,
            "customers": cust_names,
            "site_ids": f.get(SITE_IDS_FIELD) or "",
            "ttfr_ms": ttfr_ms,
            "ttfr_breached": ttfr_breached,
            "ttfr_waiting": ttfr_waiting,
            "ttr_ms": ttr_ms,
            "ttr_breached": ttr_breached,
            "ttr_waiting": ttr_waiting,
            "sentiment": sentiment,
            "request_type": req_type,
            "site_cmdb": f.get(SITE_CMDB_FIELD),
            "entity_cmdb": f.get(ENTITY_CMDB_FIELD),
        }

    def get_customer_jira(self, customer_name: str, days: int = 90) -> dict[str, Any]:
        """Get JIRA picture for a customer: open issues, recent activity, escalations.

        Matches on Organizations field (JSM) and summary prefix.
        """
        safe_name = customer_name.replace('"', '\\"')
        jql = (
            f'(Organizations = "{safe_name}" OR summary ~ "{safe_name}")'
            f" AND created >= -{days}d ORDER BY created DESC"
        )

        try:
            raw = self._search(jql, max_results=200)
        except Exception as e:
            logger.warning("JIRA search failed for %s: %s", customer_name, e)
            return {"error": str(e)}

        issues = [self._normalize_issue(i) for i in raw]

        open_issues = [i for i in issues if i["resolution"] == ""]
        resolved = [i for i in issues if i["resolution"] != ""]
        escalated = [i for i in issues if "jira_escalated" in i["labels"]
                     or i["type"] == "Developer escalation"]
        bugs = [i for i in issues if i["type"] == "Bug"]
        open_bugs = [i for i in bugs if i["resolution"] == ""]

        by_status: dict[str, int] = {}
        by_type: dict[str, int] = {}
        by_priority: dict[str, int] = {}
        for i in issues:
            by_status[i["status"]] = by_status.get(i["status"], 0) + 1
            by_type[i["type"]] = by_type.get(i["type"], 0) + 1
            by_priority[i["priority"]] = by_priority.get(i["priority"], 0) + 1

        by_sentiment: dict[str, int] = {}
        by_request_type: dict[str, int] = {}
        for i in issues:
            s = i.get("sentiment") or "Unknown"
            by_sentiment[s] = by_sentiment.get(s, 0) + 1
            rt = i.get("request_type") or "Other"
            by_request_type[rt] = by_request_type.get(rt, 0) + 1

        eng = self._get_engineering_tickets(safe_name)
        enhancements = self._get_enhancement_requests(safe_name)
        ttfr = self._compute_sla(issues, "ttfr")
        ttr = self._compute_sla(issues, "ttr")

        self._run_qa_checks(issues, open_issues, resolved, by_status, by_priority, by_type, ttfr, ttr)

        return {
            "base_url": self.base_url,
            "customer": customer_name,
            "days": days,
            "total_issues": len(issues),
            "open_issues": len(open_issues),
            "resolved_issues": len(resolved),
            "escalated": len(escalated),
            "open_bugs": len(open_bugs),
            "by_status": dict(sorted(by_status.items(), key=lambda x: -x[1])),
            "by_type": dict(sorted(by_type.items(), key=lambda x: -x[1])),
            "by_priority": dict(sorted(by_priority.items(), key=lambda x: -x[1])),
            "by_sentiment": dict(sorted(by_sentiment.items(), key=lambda x: -x[1])),
            "by_request_type": dict(sorted(by_request_type.items(), key=lambda x: -x[1])),
            "tickets_over_time": self._bucket_by_week(issues),
            "recent_issues": [
                {"key": i["key"], "summary": i["summary"][:60], "type": i["type"],
                 "status": i["status"], "priority": i["priority"], "created": i["created"]}
                for i in issues[:8]
            ],
            "escalated_issues": [
                {"key": i["key"], "summary": i["summary"][:60], "status": i["status"],
                 "created": i["created"]}
                for i in escalated[:5]
            ],
            "engineering": eng,
            "enhancements": enhancements,
            "ttfr": ttfr,
            "ttr": ttr,
        }

    @staticmethod
    def _bucket_by_week(issues: list[dict]) -> list[dict]:
        """Return weekly ticket counts sorted oldest-first.

        Each entry: {"week": "YYYY-Www", "label": "Mar 10", "created": N, "resolved": N}
        """
        from datetime import datetime, timedelta
        buckets: dict[str, dict] = {}
        for i in issues:
            for field, col in (("created", "created"), ("updated", "resolved")):
                raw = i.get(field, "")
                if not raw:
                    continue
                if col == "resolved" and i.get("resolution") == "":
                    continue
                try:
                    dt = datetime.strptime(raw[:10], "%Y-%m-%d")
                except ValueError:
                    continue
                # ISO week key
                iso = dt.isocalendar()
                key = f"{iso[0]}-W{iso[1]:02d}"
                # Monday of that week for the label
                monday = dt - timedelta(days=dt.weekday())
                if key not in buckets:
                    buckets[key] = {"week": key, "label": monday.strftime("%b %-d"), "created": 0, "resolved": 0}
                buckets[key][col] += 1

        return sorted(buckets.values(), key=lambda b: b["week"])

    @staticmethod
    def _compute_sla(issues: list[dict], prefix: str) -> dict[str, Any]:
        """Compute SLA statistics (TTFR or TTR) from JSM SLA data."""
        help_issues = [i for i in issues if i.get("project") == "HELP"]
        values = [i[f"{prefix}_ms"] for i in help_issues if i.get(f"{prefix}_ms") is not None]
        breached = sum(1 for i in help_issues if i.get(f"{prefix}_breached"))
        waiting = sum(1 for i in help_issues if i.get(f"{prefix}_waiting"))

        if not values:
            return {"tickets": len(help_issues), "measured": 0, "waiting": waiting}

        values.sort()
        avg_ms = sum(values) / len(values)
        med_ms = values[len(values) // 2]

        def _fmt(ms: int) -> str:
            mins = ms / 60_000
            if mins < 60:
                return f"{mins:.0f}m"
            hrs = mins / 60
            if hrs < 24:
                return f"{hrs:.1f}h"
            return f"{hrs / 24:.1f}d"

        return {
            "tickets": len(help_issues),
            "measured": len(values),
            "waiting": waiting,
            "breached": breached,
            "avg_ms": int(avg_ms),
            "median_ms": med_ms,
            "min_ms": values[0],
            "max_ms": values[-1],
            "avg": _fmt(int(avg_ms)),
            "median": _fmt(med_ms),
            "min": _fmt(values[0]),
            "max": _fmt(values[-1]),
        }

    def _get_engineering_tickets(self, safe_name: str) -> dict[str, Any]:
        """Fetch LEAN project tickets that reference a customer.

        Returns open/recent-closed tickets relevant to engineering work
        affecting this customer — useful for CS to know what's in flight.
        """
        jql = (
            f'project = LEAN AND (summary ~ "{safe_name}" OR description ~ "{safe_name}")'
            f" ORDER BY updated DESC"
        )
        try:
            raw = self._search(jql, max_results=50)
        except Exception as e:
            logger.warning("LEAN search failed for %s: %s", safe_name, e)
            return {"total": 0, "open": [], "recent_closed": []}

        issues = [self._normalize_issue(i) for i in raw]
        open_eng = [i for i in issues if i["resolution"] == ""]
        closed_eng = [i for i in issues if i["resolution"] != ""]

        def _fmt(i: dict) -> dict:
            return {"key": i["key"], "summary": i["summary"][:60], "type": i["type"],
                    "status": i["status"], "assignee": i["assignee"], "updated": i["updated"]}

        return {
            "total": len(issues),
            "open_count": len(open_eng),
            "closed_count": len(closed_eng),
            "open": [_fmt(i) for i in open_eng[:8]],
            "recent_closed": [_fmt(i) for i in closed_eng[:5]],
        }

    def _get_enhancement_requests(self, safe_name: str) -> dict[str, Any]:
        """Fetch ER project tickets for a customer.

        Returns open and recently shipped enhancement requests — shows
        the customer that their feedback drives product improvements.
        """
        jql = (
            f'project = ER AND (summary ~ "{safe_name}" OR description ~ "{safe_name}"'
            f' OR "Customer" in ("{safe_name}"))'
            f" ORDER BY updated DESC"
        )
        try:
            raw = self._search(jql, max_results=50)
        except Exception as e:
            logger.warning("ER search failed for %s: %s", safe_name, e)
            return {"total": 0, "open": [], "shipped": []}

        issues = [self._normalize_issue(i) for i in raw]
        open_er = [i for i in issues if i["resolution"] == ""]
        shipped = [i for i in issues if i["resolution"] in ("Fixed", "Done")]
        declined = [i for i in issues if i["resolution"] in ("Won't Do", "Won't Fix", "Declined", "Future Consideration")]

        def _fmt(i: dict) -> dict:
            return {"key": i["key"], "summary": i["summary"][:60], "type": i["type"],
                    "status": i["status"], "priority": i["priority"], "updated": i["updated"]}

        return {
            "total": len(issues),
            "open_count": len(open_er),
            "shipped_count": len(shipped),
            "declined_count": len(declined),
            "open": [_fmt(i) for i in open_er[:8]],
            "shipped": [_fmt(i) for i in shipped[:8]],
            "declined": [_fmt(i) for i in declined[:5]],
        }

    @staticmethod
    def _run_qa_checks(issues, open_issues, resolved, by_status, by_priority, by_type, ttfr, ttr):
        """Cross-validate JIRA data and flag discrepancies."""
        from .qa import qa

        total = len(issues)

        # Status breakdown should sum to total
        status_sum = sum(by_status.values())
        if status_sum == total:
            qa.check()
        else:
            qa.flag("JIRA status breakdown sum != total issue count",
                    expected=total, actual=status_sum,
                    sources=("JIRA search count", "status breakdown sum"),
                    severity="error")

        # Priority breakdown should sum to total
        priority_sum = sum(by_priority.values())
        if priority_sum == total:
            qa.check()
        else:
            qa.flag("JIRA priority breakdown sum != total issue count",
                    expected=total, actual=priority_sum,
                    sources=("JIRA search count", "priority breakdown sum"),
                    severity="error")

        # Type breakdown should sum to total
        type_sum = sum(by_type.values())
        if type_sum == total:
            qa.check()
        else:
            qa.flag("JIRA type breakdown sum != total issue count",
                    expected=total, actual=type_sum,
                    sources=("JIRA search count", "type breakdown sum"),
                    severity="error")

        # Open + resolved should equal total
        open_plus_resolved = len(open_issues) + len(resolved)
        if open_plus_resolved == total:
            qa.check()
        else:
            qa.flag("JIRA open + resolved != total",
                    expected=total, actual=open_plus_resolved,
                    sources=("open count + resolved count", "total search results"),
                    severity="error")

        # SLA measured + waiting should not exceed total HELP tickets
        for label, sla in [("TTFR", ttfr), ("TTR", ttr)]:
            tickets = sla.get("tickets", 0)
            measured = sla.get("measured", 0)
            waiting = sla.get("waiting", 0)
            if measured + waiting <= tickets:
                qa.check()
            else:
                qa.flag(f"{label} measured + waiting > HELP ticket count",
                        expected=f"<= {tickets}", actual=measured + waiting,
                        sources=(f"{label} SLA data", "HELP issue count"),
                        severity="warning")
