"""JIRA Cloud client for fetching customer-related issues."""

from base64 import b64encode
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import requests

from .config import JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN, LLM_MODEL_FAST, llm_client, logger

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
SPRINT_FIELD = "customfield_10204"       # "Sprint" (Agile sprint array)
STORY_POINTS_FIELD = "customfield_10202" # "Story Points"

# Minimal fields for per-project operational slides (open + recently resolved).
_PROJECT_SNAPSHOT_FIELDS = [
    "summary", "status", "issuetype", "created", "updated",
    "resolution", "resolutiondate", "assignee",
]

_TREND_FIELDS = [
    "created", "resolutiondate", "labels",
]

_CUSTOMER_TICKET_SLIDE_FIELDS = [
    "summary", "status", "issuetype", "project", "created", "updated",
    "resolution", "resolutiondate", TTFR_FIELD, TTR_FIELD,
]

_ISSUE_FIELDS = [
    "summary", "status", "issuetype", "project", "priority",
    "labels", "components", "created", "updated", "resolution",
    "assignee", "reporter", "description", "comment",
    CUSTOMER_FIELD, ORG_FIELD, SITE_IDS_FIELD, SEVERITY_FIELD,
    TTFR_FIELD, TTR_FIELD, SENTIMENT_FIELD, REQUEST_TYPE_FIELD,
    SITE_CMDB_FIELD, ENTITY_CMDB_FIELD,
]


def _extract_adf_text(node: Any, _depth: int = 0) -> str:
    """Recursively extract plain text from a Jira ADF (Atlassian Document Format) node."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if not isinstance(node, dict):
        return ""
    node_type = node.get("type", "")
    if node_type == "text":
        return node.get("text", "")
    parts: list[str] = []
    for child in node.get("content", []) or []:
        t = _extract_adf_text(child, _depth + 1)
        if t:
            parts.append(t)
    separator = "\n" if node_type in (
        "paragraph", "bulletList", "orderedList", "listItem",
        "heading", "blockquote", "codeBlock"
    ) else " "
    return separator.join(parts).strip()


def _summarize_ticket(issue: dict) -> str:
    """Use GPT-4o-mini to write a 2–3 sentence narrative for an engineering ticket."""
    summary = issue.get("summary", "")
    resolution = issue.get("resolution", "")
    status = issue.get("status", "")
    assignee = issue.get("assignee", "") or "unassigned"
    description = (issue.get("description_text") or "")[:600]
    comments = issue.get("comment_texts") or []
    comment_blob = "\n".join(f"- {c[:200]}" for c in comments[:3])

    prompt = (
        f"Jira ticket: {issue['key']}\n"
        f"Summary: {summary}\n"
        f"Status: {status}  |  Resolution: {resolution or 'unresolved'}  |  Assignee: {assignee}\n"
    )
    if description:
        prompt += f"Description: {description}\n"
    if comment_blob:
        prompt += f"Recent comments:\n{comment_blob}\n"

    prompt += (
        "\nWrite 2–3 concise sentences (plain text, no markdown) for an engineering review slide. "
        "Cover: (1) what the issue was, (2) how it was resolved or its current state, "
        "(3) anything notable or interesting from the comments. "
        "Be specific and factual. Do not start with 'This ticket'."
    )
    try:
        client = llm_client()
        resp = client.chat.completions.create(
            model=LLM_MODEL_FAST,
            temperature=0.3,
            max_tokens=120,
            messages=[
                {"role": "system", "content": "You summarize Jira tickets for engineering review slides."},
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.warning("GPT ticket summary failed for %s: %s", issue.get("key"), e)
        # Graceful fallback
        base = summary[:100]
        if resolution:
            return f"{base}. Resolved: {resolution}."
        return f"{base}. Status: {status}."


def _extract_comments(comment_field: Any) -> list[str]:
    """Extract plain text from Jira comment field (ADF bodies). Returns up to 5 most recent."""
    if not comment_field or not isinstance(comment_field, dict):
        return []
    comments = comment_field.get("comments") or []
    texts = []
    for c in reversed(comments[-5:]):
        body = c.get("body")
        t = _extract_adf_text(body).strip()
        if t:
            texts.append(t)
    return texts


def _generate_eng_insights(eng: dict) -> dict[str, list[str]]:
    """Generate 2-3 LeanDNA-style insight bullets for each engineering portfolio slide.

    Returns a dict keyed by slide name, each value a list of bullet strings.
    Runs all GPT calls in parallel.  Falls back to [] on any error.
    """
    from concurrent.futures import ThreadPoolExecutor

    _oai = llm_client()

    def _call(slide_name: str, prompt: str) -> tuple[str, list[str]]:
        try:
            resp = _oai.chat.completions.create(
                model=LLM_MODEL_FAST,
                messages=[
                    {"role": "system", "content": (
                        "You are a technical analyst writing slide bullets for an engineering review deck. "
                        "Follow these rules strictly:\n"
                        "- Write exactly 2-3 bullets\n"
                        "- Each bullet is one sentence, 8-14 words\n"
                        "- Lead with the insight or implication, not the raw number\n"
                        "- Use plain text, no markdown, no hyphens at the start\n"
                        "- Tone: direct, analytical, not salesy"
                    )},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=200,
            )
            raw = resp.choices[0].message.content.strip()
            bullets = [
                line.lstrip("•-–— ").strip()
                for line in raw.splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            return slide_name, [b for b in bullets if b][:3]
        except Exception as e:
            logger.warning("Insight generation failed for %s: %s", slide_name, e)
            return slide_name, []

    sprint = eng.get("sprint") or {}
    sprint_name = sprint.get("name", "current sprint")
    in_flight = eng.get("in_flight_count", 0)
    closed = eng.get("closed_count", 0)
    active = eng.get("by_status", {}).get("In Progress", 0)
    themes = eng.get("themes") or []
    top_theme = themes[0]["theme"] if themes else "unknown"
    by_type = eng.get("by_type") or {}
    bugs_if = by_type.get("Bug", 0)

    open_bugs = eng.get("open_bugs") or []
    blockers = eng.get("blocker_critical") or []
    by_prio_bug: dict[str, int] = {}
    for b in open_bugs:
        p = b.get("priority", "Unknown").split(":")[0]
        by_prio_bug[p] = by_prio_bug.get(p, 0) + 1

    throughput = eng.get("throughput") or []
    recent_tp = throughput[-4:] if throughput else []
    avg_closed = sum(w.get("resolved", 0) for w in recent_tp) / len(recent_tp) if recent_tp else 0
    avg_created = sum(w.get("created", 0) for w in recent_tp) / len(recent_tp) if recent_tp else 0

    sp = eng.get("support_pressure") or {}
    sp_total = sp.get("total", 0)
    sp_esc = sp.get("escalated_to_eng", 0)
    sp_bugs = sp.get("open_bugs", 0)
    days = eng.get("days", 30)

    enhancements = eng.get("enhancements") or {}
    er_open = enhancements.get("open_count", 0)
    er_shipped = enhancements.get("shipped_count", 0)

    tasks = [
        ("sprint_snapshot", (
            f"Sprint: {sprint_name}. Total open tickets: {in_flight} (includes backlog and in-progress). "
            f"Actively being worked (In Progress + In Review): {active}. "
            f"Closed this period: {closed}. Top theme by ticket count: {top_theme}. "
            f"Open bugs: {bugs_if}. Type breakdown: {dict(list(by_type.items())[:5])}. "
            "Write 2-3 insight bullets about the sprint's focus, how much is actively in motion vs backlog, and any risks."
        )),
        ("bug_health", (
            f"Open bugs: {len(open_bugs)}. Blockers/Critical: {len(blockers)}. "
            f"Priority breakdown: {by_prio_bug}. "
            f"Blocker examples: {', '.join(b['key'] + ': ' + b['summary'][:50] for b in blockers[:3])}. "
            "Write 2-3 insight bullets about the bug backlog severity and what needs attention."
        )),
        ("velocity", (
            f"4-week avg tickets created/week: {avg_created:.1f}. "
            f"4-week avg tickets closed/week: {avg_closed:.1f}. "
            f"Net flow (closed minus created): {(avg_closed - avg_created):.1f} per week. "
            f"Total in-flight: {in_flight}. Total closed this period: {closed}. "
            "Write 2-3 insight bullets about team throughput, backlog trend, and delivery pace."
        )),
        ("support_pressure", (
            f"Support tickets in last {days} days: {sp_total} total, {sp_esc} escalated to engineering. "
            f"Open support bugs: {sp_bugs}. "
            f"Escalation rate: {(sp_esc / sp_total * 100):.0f}% of total."
        ) if sp_total else (
            f"Support ticket data unavailable for last {days} days. Write a note that data is unavailable."
        )),
    ]

    insights: dict[str, list[str]] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_call, name, prompt) for name, prompt in tasks]
        for f in futures:
            name, bullets = f.result()
            insights[name] = bullets

    return insights


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
        self._jql_log: list[dict[str, str]] = []

    def _record_jql(self, jql: str, *, description: str | None = None) -> None:
        """Record JQL with a short human label for speaker notes (``[label] - JQL``)."""
        cleaned = (jql or "").strip()
        if not cleaned:
            return
        label = (description or "Jira issue search").strip()
        self._jql_log.append({"description": label, "jql": cleaned})

    def _jql_since(self, start_idx: int) -> list[dict[str, str]]:
        """Return unique JQL entries since start_idx, preserving order (dedupe by JQL text)."""
        seen: set[str] = set()
        out: list[dict[str, str]] = []
        for entry in self._jql_log[start_idx:]:
            jql = (entry.get("jql") or "").strip()
            if not jql or jql in seen:
                continue
            seen.add(jql)
            desc = (entry.get("description") or "Jira issue search").strip()
            out.append({"description": desc, "jql": jql})
        return out

    def _search(
        self,
        jql: str,
        max_results: int = 100,
        fields: list[str] | None = None,
        *,
        data_description: str | None = None,
    ) -> list[dict]:
        results: list[dict] = []
        next_token: str | None = None
        flds = fields if fields is not None else _ISSUE_FIELDS
        self._record_jql(jql, description=data_description or "Jira issue search")
        while len(results) < max_results:
            body: dict[str, Any] = {
                "jql": jql,
                "maxResults": min(max_results - len(results), 100),
                "fields": flds,
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

    @staticmethod
    def _customer_match_clause(customer_name: str, match_terms: list[str] | None = None) -> str:
        """Build a Jira JQL clause to match a customer by canonical name and aliases."""
        raw_terms = [customer_name] + list(match_terms or [])
        seen: set[str] = set()
        terms: list[str] = []
        for term in raw_terms:
            cleaned = (term or "").strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            terms.append(cleaned.replace('"', '\\"'))

        clauses: list[str] = []
        for term in terms:
            clauses.append(f'Organizations = "{term}"')
            clauses.append(f'summary ~ "{term}"')
            clauses.append(f'description ~ "{term}"')
        return "(" + " OR ".join(clauses) + ")"

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
            "description_text": _extract_adf_text(f.get("description")),
            "comment_texts": _extract_comments(f.get("comment")),
        }

    @staticmethod
    def _parse_jira_datetime(value: str | None) -> datetime | None:
        """Parse Jira Cloud date-time strings to timezone-aware UTC."""
        if not value or not isinstance(value, str):
            return None
        s = value.strip().replace("Z", "+00:00")
        if len(s) >= 5 and s.endswith("+0000"):
            s = s[:-5] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            try:
                dt = datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def get_project_operational_snapshot(self, project_key: str) -> dict[str, Any]:
        """Metrics for one project: open totals, open status mix, ages, assignee resolve counts.

        Used by engineering portfolio slides (HELP, CUSTOMER, LEAN).  Fetches:
        - All open issues (up to 1,500)
        - Issues resolved in the last 180 days (up to 1,500)

        Assignee columns are **cumulative** windows from today: last 14d, 30d, 90d, 180d.
        """
        pk = (project_key or "").strip().upper()
        if not pk:
            return {"error": "project_key required", "project_key": ""}

        jql_start = len(self._jql_log)
        now = datetime.now(timezone.utc)
        max_fetch = 1500

        try:
            # Use Jira workflow state rather than `resolution is EMPTY` because some HELP
            # tickets remain unresolved while already in a done-like status, which inflates
            # the "open" count on portfolio slides.
            open_raw = self._search(
                f'project = {pk} AND statusCategory != Done ORDER BY updated DESC',
                max_results=max_fetch,
                fields=_PROJECT_SNAPSHOT_FIELDS,
                data_description=f"{pk} project open issues (statusCategory != Done)",
            )
        except Exception as e:
            logger.warning("Jira open fetch failed for %s: %s", pk, e)
            return {"error": str(e), "project_key": pk, "base_url": self.base_url}

        try:
            resolved_raw = self._search(
                f"project = {pk} AND resolution is not EMPTY AND resolved >= -180d "
                f"ORDER BY resolved DESC",
                max_results=max_fetch,
                fields=_PROJECT_SNAPSHOT_FIELDS,
                data_description=f"{pk} project issues resolved in last 180 days",
            )
        except Exception as e:
            logger.warning("Jira resolved fetch failed for %s: %s", pk, e)
            resolved_raw = []

        def _norm_issue(issue: dict) -> dict[str, Any]:
            f = issue["fields"]
            assignee_o = f.get("assignee")
            aname = assignee_o.get("displayName", "") if assignee_o else ""
            return {
                "key": issue["key"],
                "status": (f.get("status") or {}).get("name", "Unknown"),
                "assignee": aname.strip() or "Unassigned",
                "created": f.get("created") or "",
                "updated": f.get("updated") or "",
                "resolutiondate": f.get("resolutiondate") or "",
            }

        open_issues = [_norm_issue(i) for i in open_raw]
        resolved_issues = [_norm_issue(i) for i in resolved_raw]

        by_status_open = Counter(i["status"] for i in open_issues)
        by_status_sorted = dict(
            sorted(by_status_open.items(), key=lambda x: (-x[1], x[0])),
        )

        open_ages_days: list[float] = []
        for i in open_issues:
            cdt = self._parse_jira_datetime(i["created"])
            if cdt:
                open_ages_days.append((now - cdt).total_seconds() / 86400.0)

        cycle_days: list[float] = []
        for i in resolved_issues:
            cdt = self._parse_jira_datetime(i["created"])
            rdt = self._parse_jira_datetime(i["resolutiondate"]) or self._parse_jira_datetime(
                i["updated"]
            )
            if cdt and rdt:
                cycle_days.append((rdt - cdt).total_seconds() / 86400.0)

        windows = (14, 30, 90, 180)
        assignee_by_window: dict[str, dict[int, int]] = {}
        for i in resolved_issues:
            rdt = self._parse_jira_datetime(i["resolutiondate"]) or self._parse_jira_datetime(
                i["updated"]
            )
            if not rdt:
                continue
            age_days = (now - rdt).total_seconds() / 86400.0
            if age_days < 0 or age_days > 180:
                continue
            who = i["assignee"]
            if who not in assignee_by_window:
                assignee_by_window[who] = {w: 0 for w in windows}
            for w in windows:
                if age_days <= w:
                    assignee_by_window[who][w] += 1

        # Top assignees by 180-day resolved volume
        ranked = sorted(
            assignee_by_window.items(),
            key=lambda x: x[1][180],
            reverse=True,
        )
        top = ranked[:8]
        assignee_table = [
            {
                "assignee": name,
                "2w": counts[14],
                "1m": counts[30],
                "3m": counts[90],
                "6m": counts[180],
            }
            for name, counts in top
        ]

        def _avg(vals: list[float]) -> float | None:
            return round(sum(vals) / len(vals), 1) if vals else None

        def _median(vals: list[float]) -> float | None:
            if not vals:
                return None
            ordered = sorted(vals)
            mid = len(ordered) // 2
            if len(ordered) % 2:
                return round(ordered[mid], 1)
            return round((ordered[mid - 1] + ordered[mid]) / 2, 1)

        return {
            "project_key": pk,
            "base_url": self.base_url,
            "open_count": len(open_issues),
            "by_status_open": by_status_sorted,
            "median_open_age_days": _median(open_ages_days),
            "avg_resolved_cycle_days": _avg(cycle_days),
            "resolved_in_6mo_count": len(resolved_issues),
            "assignee_resolved_table": assignee_table,
            "jql_queries": self._jql_since(jql_start),
        }

    def get_customer_jira(self, customer_name: str, days: int = 90) -> dict[str, Any]:
        """Get JIRA picture for a customer: open issues, recent activity, escalations.

        Matches on Organizations field (JSM) and summary prefix.
        """
        jql_start = len(self._jql_log)
        safe_name = customer_name.replace('"', '\\"')
        jql = (
            f'(Organizations = "{safe_name}" OR summary ~ "{safe_name}")'
            f" AND created >= -{days}d ORDER BY created DESC"
        )

        try:
            raw = self._search(
                jql,
                max_results=200,
                data_description=f"Customer issues (Organizations/summary, {days}d lookback)",
            )
        except Exception as e:
            logger.warning("JIRA search failed for %s: %s", customer_name, e)
            return {"error": str(e)}

        issues = [self._normalize_issue(i) for i in raw]

        open_issues = [i for i in issues if i["resolution"] == ""]
        resolved = [i for i in issues if i["resolution"] != ""]
        escalated = [i for i in issues if "jira_escalated" in i["labels"]
                     or i["type"] == "Developer escalation"
                     or i["status"] == "In Engineering Queue"
                     or "customer_escalation" in i["labels"]]
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
        customer_ticket_metrics = self.get_customer_ticket_metrics(customer_name)

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
            "customer_ticket_metrics": customer_ticket_metrics,
            "jql_queries": self._jql_since(jql_start),
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

    @staticmethod
    def _compute_sla_adherence(issues: list[dict]) -> dict[str, Any]:
        """Percent of HELP tickets that met every measured SLA on the issue."""
        help_issues = [i for i in issues if i.get("project") == "HELP"]
        measured = 0
        met = 0
        waiting = 0

        for issue in help_issues:
            ttfr_measured = issue.get("ttfr_ms") is not None
            ttr_measured = issue.get("ttr_ms") is not None
            if issue.get("ttfr_waiting") or issue.get("ttr_waiting"):
                waiting += 1
            if not ttfr_measured and not ttr_measured:
                continue

            measured += 1
            breached = (
                (ttfr_measured and bool(issue.get("ttfr_breached")))
                or (ttr_measured and bool(issue.get("ttr_breached")))
            )
            if not breached:
                met += 1

        pct = round(100 * met / measured, 1) if measured else None
        return {
            "tickets": len(help_issues),
            "measured": measured,
            "met": met,
            "waiting": waiting,
            "pct": pct,
        }

    def get_customer_ticket_metrics(
        self,
        customer_name: str,
        match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """Metrics for a single customer's support tickets across open/6mo/1y windows.

        Scoped to ``project = HELP`` only. The customer clause matches Organizations and
        text (summary/description); without a project filter, those text matches pull in
        LEAN and other projects and inflate Support Review KPIs.
        """
        jql_start = len(self._jql_log)
        base_filter = self._customer_match_clause(customer_name, match_terms)
        max_fetch = 1500
        # Support desk only — must match eng_help_volume_trends / JSM HELP usage.
        proj = "project = HELP AND "

        def _norm_snapshot_issue(issue: dict) -> dict[str, Any]:
            f = issue.get("fields", {})

            def _parse_sla(field_key: str) -> tuple[int | None, bool, bool]:
                data = f.get(field_key) or {}
                ms = None
                breached = False
                waiting = False
                completed = data.get("completedCycles", [])
                if completed:
                    ms = completed[0].get("elapsedTime", {}).get("millis")
                    breached = completed[0].get("breached", False)
                elif data.get("ongoingCycle"):
                    waiting = True
                return ms, breached, waiting

            ttfr_ms, ttfr_breached, ttfr_waiting = _parse_sla(TTFR_FIELD)
            ttr_ms, ttr_breached, ttr_waiting = _parse_sla(TTR_FIELD)

            return {
                "status": (f.get("status") or {}).get("name", "Unknown"),
                "type": (f.get("issuetype") or {}).get("name", "Unknown"),
                "project": (f.get("project") or {}).get("key", ""),
                "created": f.get("created") or "",
                "updated": f.get("updated") or "",
                "resolutiondate": f.get("resolutiondate") or "",
                "ttfr_ms": ttfr_ms,
                "ttfr_breached": ttfr_breached,
                "ttfr_waiting": ttfr_waiting,
                "ttr_ms": ttr_ms,
                "ttr_breached": ttr_breached,
                "ttr_waiting": ttr_waiting,
            }

        try:
            open_raw = self._search(
                f"{proj}{base_filter} AND statusCategory != Done ORDER BY updated DESC",
                max_results=max_fetch,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description="HELP open issues for customer (non-done)",
            )
            resolved_raw = self._search(
                f"{proj}{base_filter} AND resolution is not EMPTY AND resolved >= -180d ORDER BY resolved DESC",
                max_results=max_fetch,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description="HELP customer issues resolved in last 180 days",
            )
            year_raw = self._search(
                f"{proj}{base_filter} AND created >= -365d ORDER BY created DESC",
                max_results=max_fetch,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description="HELP customer issues created in last 365 days",
            )
        except Exception as e:
            logger.warning("Customer ticket metrics fetch failed for %s: %s", customer_name, e)
            return {"error": str(e), "customer": customer_name}

        open_issues = [_norm_snapshot_issue(i) for i in open_raw]
        resolved_issues = [_norm_snapshot_issue(i) for i in resolved_raw]
        year_issues = [_norm_snapshot_issue(i) for i in year_raw]

        by_type_open = dict(sorted(Counter(i["type"] for i in open_issues).items(), key=lambda x: (-x[1], x[0])))
        by_status_open = dict(sorted(Counter(i["status"] for i in open_issues).items(), key=lambda x: (-x[1], x[0])))
        ttfr = self._compute_sla(year_issues, "ttfr")
        ttr = self._compute_sla(year_issues, "ttr")
        sla_adherence = self._compute_sla_adherence(year_issues)

        return {
            "customer": customer_name,
            "unresolved_count": len(open_issues),
            "resolved_in_6mo_count": len(resolved_issues),
            "ttfr_1y": ttfr,
            "ttr_1y": ttr,
            "sla_adherence_1y": sla_adherence,
            "by_type_open": by_type_open,
            "by_status_open": by_status_open,
            "jql_queries": self._jql_since(jql_start),
        }

    def get_customer_help_recent_tickets(
        self,
        customer_name: str,
        match_terms: list[str] | None = None,
        *,
        opened_within_days: int = 45,
        closed_within_days: int = 45,
        max_each: int = 45,
    ) -> dict[str, Any]:
        """Recent HELP issues for one customer: opened in window vs resolved in window.

        Uses the same ``project = HELP`` + organization/text match as
        ``get_customer_ticket_metrics``.
        """
        jql_start = len(self._jql_log)
        base_filter = self._customer_match_clause(customer_name, match_terms)
        proj = "project = HELP AND "

        def _row(issue: dict) -> dict[str, Any]:
            f = issue.get("fields", {}) or {}
            cr = self._parse_jira_datetime(f.get("created"))
            rs = self._parse_jira_datetime(f.get("resolutiondate"))
            st = f.get("status") or {}
            status_name = st.get("name", "—") if isinstance(st, dict) else "—"
            return {
                "key": issue.get("key", ""),
                "summary": (f.get("summary") or "").strip(),
                "status": status_name,
                "created": f.get("created") or "",
                "created_short": cr.strftime("%Y-%m-%d") if cr else "—",
                "resolved_short": rs.strftime("%Y-%m-%d") if rs else "—",
            }

        od = int(opened_within_days)
        cd = int(closed_within_days)
        try:
            open_jql = f"{proj}{base_filter} AND created >= -{od}d ORDER BY created DESC"
            closed_jql = (
                f"{proj}{base_filter} AND resolution is not EMPTY AND resolved >= -{cd}d "
                "ORDER BY resolved DESC"
            )
            raw_open = self._search(
                open_jql,
                max_results=max_each,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description=f"HELP customer tickets created in last {od} days",
            )
            raw_closed = self._search(
                closed_jql,
                max_results=max_each,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description=f"HELP customer tickets resolved in last {cd} days",
            )
        except Exception as e:
            logger.warning("Customer HELP recent tickets fetch failed for %s: %s", customer_name, e)
            return {
                "error": str(e),
                "customer": customer_name,
                "opened_within_days": od,
                "closed_within_days": cd,
                "recently_opened": [],
                "recently_closed": [],
                "jql_queries": self._jql_since(jql_start),
            }

        return {
            "customer": customer_name,
            "opened_within_days": od,
            "closed_within_days": cd,
            "recently_opened": [_row(i) for i in raw_open],
            "recently_closed": [_row(i) for i in raw_closed],
            "jql_queries": self._jql_since(jql_start),
        }

    @staticmethod
    def _bucket_by_month(
        issues: list[dict],
        escalated_only: bool = False,
        exclude_escalated: bool = False,
    ) -> list[dict[str, Any]]:
        """Return the last 12 monthly created/resolved buckets, oldest first."""
        from datetime import datetime

        now = datetime.now(timezone.utc)
        year = now.year
        month = now.month
        month_starts: list[datetime] = []
        for _ in range(12):
            month_starts.append(datetime(year, month, 1, tzinfo=timezone.utc))
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        month_starts.reverse()

        buckets: list[dict[str, Any]] = []
        month_index: dict[tuple[int, int], dict[str, Any]] = {}
        for dt in month_starts:
            key = (dt.year, dt.month)
            row = {
                "month": f"{dt.year}-{dt.month:02d}",
                "label": dt.strftime("%b"),
                "created": 0,
                "resolved": 0,
            }
            buckets.append(row)
            month_index[key] = row

        for issue in issues:
            labels = issue.get("labels") or []
            if escalated_only and "jira_escalated" not in labels:
                continue
            if exclude_escalated and "jira_escalated" in labels:
                continue

            created_dt = JiraClient._parse_jira_datetime(issue.get("created"))
            if created_dt:
                created_key = (created_dt.year, created_dt.month)
                if created_key in month_index:
                    month_index[created_key]["created"] += 1

            resolved_dt = JiraClient._parse_jira_datetime(issue.get("resolutiondate"))
            if resolved_dt:
                resolved_key = (resolved_dt.year, resolved_dt.month)
                if resolved_key in month_index:
                    month_index[resolved_key]["resolved"] += 1

        return buckets

    def _get_help_ticket_volume_trends(self) -> dict[str, Any]:
        """Return 12-month HELP created vs resolved trends for all/escalated/non-escalated."""
        max_fetch = 5000
        jql = (
            "project = HELP AND (created >= -365d OR resolved >= -365d) "
            "ORDER BY created DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=max_fetch,
                fields=_TREND_FIELDS,
                data_description="HELP volume trends (12-month created vs resolved)",
            )
        except Exception as e:
            logger.warning("HELP ticket trend fetch failed: %s", e)
            return {"error": str(e), "all": [], "escalated": [], "non_escalated": []}

        issues = []
        for issue in raw:
            f = issue.get("fields", {})
            issues.append({
                "created": f.get("created") or "",
                "resolutiondate": f.get("resolutiondate") or "",
                "labels": f.get("labels") or [],
            })

        return {
            "all": self._bucket_by_month(issues, escalated_only=False),
            "escalated": self._bucket_by_month(issues, escalated_only=True),
            "non_escalated": self._bucket_by_month(issues, exclude_escalated=True),
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
            raw = self._search(
                jql,
                max_results=50,
                data_description="LEAN issues mentioning customer (engineering pipeline)",
            )
        except Exception as e:
            logger.warning("LEAN search failed for %s: %s", safe_name, e)
            return {"total": 0, "open": [], "recent_closed": []}

        issues = [self._normalize_issue(i) for i in raw]
        open_eng = [i for i in issues if i["resolution"] == ""]
        closed_eng = [i for i in issues if i["resolution"] != ""]

        def _fmt(i: dict) -> dict:
            return {"key": i["key"], "summary": i["summary"][:60], "type": i["type"],
                    "status": i["status"], "assignee": i["assignee"], "updated": i["updated"]}

        # Generate narratives in parallel
        open_show = open_eng[:8]
        closed_show = closed_eng[:5]
        all_show = open_show + closed_show

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=8) as pool:
            narratives = list(pool.map(_summarize_ticket, all_show))

        open_fmted = []
        for i, issue in enumerate(open_show):
            t = _fmt(issue)
            t["narrative"] = narratives[i]
            open_fmted.append(t)

        closed_fmted = []
        for i, issue in enumerate(closed_show):
            t = _fmt(issue)
            t["narrative"] = narratives[len(open_show) + i]
            closed_fmted.append(t)

        return {
            "total": len(issues),
            "open_count": len(open_eng),
            "closed_count": len(closed_eng),
            "open": open_fmted,
            "recent_closed": closed_fmted,
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
            raw = self._search(
                jql,
                max_results=50,
                data_description="ER enhancement requests for customer",
            )
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

    def get_engineering_portfolio(self, days: int = 30) -> dict[str, Any]:
        """Fetch a product/engineering-wide SDLC snapshot — not per-customer.

        Returns sprint state, work-in-progress by theme, velocity, bug health,
        enhancement backlog, and aggregate support pressure.
        """
        import re
        import requests as _req

        jql_start = len(self._jql_log)

        # ── Active sprint from Board 44 (LEAN Scrum - CURRENT Issues) ──
        sprint_info: dict = {}
        recent_sprints: list[dict] = []
        try:
            resp = _req.get(
                f"{self.base_url}/rest/agile/1.0/board/44/sprint?state=active",
                headers=self._headers, timeout=10,
            )
            if resp.ok:
                vals = resp.json().get("values", [])
                if vals:
                    s = vals[0]
                    sprint_info = {
                        "id": s["id"],
                        "name": s["name"],
                        "state": s["state"],
                        "start": s.get("startDate", "")[:10],
                        "end": s.get("endDate", "")[:10],
                        "goal": s.get("goal", ""),
                    }
            # Last 4 closed sprints for velocity
            resp2 = _req.get(
                f"{self.base_url}/rest/agile/1.0/board/44/sprint?state=closed&maxResults=4",
                headers=self._headers, timeout=10,
            )
            if resp2.ok:
                for s in reversed(resp2.json().get("values", [])[-4:]):
                    recent_sprints.append({
                        "id": s["id"],
                        "name": s["name"],
                        "start": s.get("startDate", "")[:10],
                        "end": s.get("endDate", "")[:10],
                    })
        except Exception as e:
            logger.warning("Sprint fetch failed: %s", e)

        # ── In-flight LEAN tickets (all open) ──
        _eng_fields = [
            "summary", "status", "issuetype", "priority", "assignee",
            "labels", "created", "updated", "resolution",
            SPRINT_FIELD, STORY_POINTS_FIELD,
        ]
        try:
            body_inflight = {
                "jql": "project = LEAN AND status in (\"In Progress\", \"In Review\", \"Open\", \"Reopened\") ORDER BY updated DESC",
                "maxResults": 200,
                "fields": _eng_fields,
            }
            self._record_jql(
                body_inflight["jql"],
                description="LEAN in-flight engineering work (Open / In Progress / In Review / Reopened)",
            )
            resp_if = _req.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body_inflight, timeout=30,
            )
            resp_if.raise_for_status()
            in_flight_raw = resp_if.json().get("issues", [])
        except Exception as e:
            logger.warning("LEAN in-flight fetch failed: %s", e)
            in_flight_raw = []

        # ── Recent closed LEAN tickets ──
        try:
            body_closed = {
                "jql": f"project = LEAN AND status = Closed AND updated >= -{days}d ORDER BY updated DESC",
                "maxResults": 200,
                "fields": _eng_fields,
            }
            self._record_jql(
                body_closed["jql"],
                description=f"LEAN issues closed or updated in last {days} days",
            )
            resp_c = _req.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body_closed, timeout=30,
            )
            resp_c.raise_for_status()
            closed_raw = resp_c.json().get("issues", [])
        except Exception as e:
            logger.warning("LEAN closed fetch failed: %s", e)
            closed_raw = []

        def _lean_norm(issue: dict) -> dict:
            f = issue.get("fields", {})
            sp_list = f.get(SPRINT_FIELD) or []
            sprint_names = [s.get("name", "") for s in sp_list if s.get("state") != "future"]
            return {
                "key": issue["key"],
                "summary": f.get("summary", ""),
                "status": f.get("status", {}).get("name", ""),
                "type": f.get("issuetype", {}).get("name", ""),
                "priority": (f.get("priority") or {}).get("name", ""),
                "assignee": (f.get("assignee") or {}).get("displayName", ""),
                "labels": f.get("labels") or [],
                "created": (f.get("created") or "")[:10],
                "updated": (f.get("updated") or "")[:10],
                "resolution": (f.get("resolution") or {}).get("name", "") if f.get("resolution") else "",
                "sprints": sprint_names,
            }

        in_flight = [_lean_norm(i) for i in in_flight_raw]
        closed = [_lean_norm(i) for i in closed_raw]

        # ── Theme extraction from bracket-prefixed summaries ──
        _theme_re = re.compile(r"^\[([^\]]+)\]")

        def _theme(summary: str) -> str:
            m = _theme_re.match(summary)
            return m.group(1) if m else "Other"

        themes: dict[str, list[dict]] = {}
        for t in in_flight:
            th = _theme(t["summary"])
            themes.setdefault(th, []).append(t)

        theme_summary = [
            {
                "theme": th,
                "total": len(tix),
                "in_progress": sum(1 for t in tix if t["status"] in ("In Progress", "In Review")),
                "open": sum(1 for t in tix if t["status"] in ("Open", "Reopened")),
                "bugs": sum(1 for t in tix if t["type"] == "Bug"),
                "tickets": [{"key": t["key"], "summary": t["summary"][:70],
                             "status": t["status"], "assignee": t["assignee"]}
                            for t in tix[:4]],
            }
            for th, tix in sorted(themes.items(), key=lambda x: -len(x[1]))
        ]

        # ── Type & status & assignee breakdowns ──
        by_type: dict[str, int] = {}
        by_status: dict[str, int] = {}
        by_assignee: dict[str, int] = {}
        for t in in_flight:
            by_type[t["type"]] = by_type.get(t["type"], 0) + 1
            by_status[t["status"]] = by_status.get(t["status"], 0) + 1
            if t["assignee"]:
                by_assignee[t["assignee"]] = by_assignee.get(t["assignee"], 0) + 1

        open_bugs = [t for t in in_flight if t["type"] == "Bug"]
        blocker_critical = [
            t for t in in_flight
            if t["priority"].startswith(("Blocker", "Critical"))
        ]

        # ── Velocity: tickets closed per recent sprint ──
        velocity: list[dict] = []
        for sp in recent_sprints:
            count = sum(1 for t in closed if sp["name"] in t.get("sprints", []))
            velocity.append({"sprint": sp["name"], "closed": count,
                              "start": sp["start"], "end": sp["end"]})

        # ── Enhancement requests (all, no customer filter) ──
        # Open tickets — updated in the last year, most recent first
        try:
            body_er_open = {
                "jql": (
                    "project = ER AND resolution is EMPTY "
                    "AND status not in (Done, Closed, \"Not Taken\") "
                    "AND updated >= -365d "
                    "ORDER BY updated DESC"
                ),
                "maxResults": 200,
                "fields": ["summary", "status", "issuetype", "priority",
                           "labels", "created", "updated", "resolution",
                           "description", "comment"],
            }
            self._record_jql(
                body_er_open["jql"],
                description="ER open enhancement backlog (last year)",
            )
            resp_er_open = _req.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body_er_open, timeout=30,
            )
            resp_er_open.raise_for_status()
            er_open_raw = resp_er_open.json().get("issues", [])
        except Exception as e:
            logger.warning("ER open fetch failed: %s", e)
            er_open_raw = []

        # Shipped tickets — resolved in the last year, most recently updated first
        try:
            body_er_shipped = {
                "jql": (
                    "project = ER AND resolution in (Fixed, Done) "
                    "AND updated >= -365d "
                    "ORDER BY updated DESC"
                ),
                "maxResults": 50,
                "fields": ["summary", "status", "issuetype", "priority",
                           "labels", "created", "updated", "resolution",
                           "description", "comment"],
            }
            self._record_jql(
                body_er_shipped["jql"],
                description="ER shipped or Done enhancements (last year)",
            )
            resp_er_shipped = _req.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body_er_shipped, timeout=30,
            )
            resp_er_shipped.raise_for_status()
            er_shipped_raw = resp_er_shipped.json().get("issues", [])
        except Exception as e:
            logger.warning("ER shipped fetch failed: %s", e)
            er_shipped_raw = []

        # Declined count only — no need for full fetch
        try:
            body_er_dec = {
                "jql": "project = ER AND resolution in (\"Won't Do\", \"Won't Fix\", Declined, \"Future Consideration\", \"Not Taken\")",
                "maxResults": 1,
                "fields": ["summary"],
            }
            self._record_jql(
                body_er_dec["jql"],
                description="ER declined / won't do / not taken (count query)",
            )
            resp_er_dec = _req.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body_er_dec, timeout=30,
            )
            resp_er_dec.raise_for_status()
            er_declined_count = resp_er_dec.json().get("total", 0) or 0
        except Exception as e:
            logger.warning("ER declined fetch failed: %s", e)
            er_declined_count = 0

        def _norm_er(i: dict) -> dict:
            f = i["fields"]
            return {
                "key": i["key"],
                "summary": f.get("summary", "")[:100],
                "status": f.get("status", {}).get("name", ""),
                "priority": (f.get("priority") or {}).get("name", ""),
                "labels": f.get("labels") or [],
                "updated": (f.get("updated") or "")[:10],
                "description_text": _extract_adf_text(f.get("description")),
                "comment_texts": _extract_comments(f.get("comment")),
            }

        er_open = [_norm_er(i) for i in er_open_raw]
        er_shipped = [_norm_er(i) for i in er_shipped_raw]

        # Generate narratives in parallel for all open + top shipped
        def _er_narrative(entry: dict) -> str:
            return _summarize_ticket({
                "key": entry["key"],
                "summary": entry["summary"],
                "status": entry["status"],
                "resolution": "",
                "assignee": "",
                "description_text": entry.get("description_text", ""),
                "comment_texts": entry.get("comment_texts", []),
            })

        # Cap narratives: first 20 open ERs + first 10 shipped — the rest show title-only
        _OPEN_NARRATIVE_CAP = 20
        _SHIPPED_NARRATIVE_CAP = 10
        er_open_with_narratives = er_open[:_OPEN_NARRATIVE_CAP]
        er_shipped_for_narratives = er_shipped[:_SHIPPED_NARRATIVE_CAP]

        from concurrent.futures import ThreadPoolExecutor as _TPE
        all_er_for_narr = er_open_with_narratives + er_shipped_for_narratives
        with _TPE(max_workers=12) as pool:
            all_narratives = list(pool.map(_er_narrative, all_er_for_narr))

        n_open_narr = len(er_open_with_narratives)  # actual count generated

        open_with_narratives = []
        for i, e in enumerate(er_open):
            e = dict(e)
            if i < n_open_narr:
                e["narrative"] = all_narratives[i]
            # tickets beyond cap get no narrative — slide renders title only
            open_with_narratives.append(e)

        shipped_with_narratives = []
        for i, e in enumerate(er_shipped[:_SHIPPED_NARRATIVE_CAP]):
            e = dict(e)
            e["narrative"] = all_narratives[n_open_narr + i]
            shipped_with_narratives.append(e)

        enhancements = {
            "total": len(er_open) + len(er_shipped) + er_declined_count,
            "open_count": len(er_open),
            "shipped_count": len(er_shipped),
            "declined_count": er_declined_count,
            "open": open_with_narratives,
            "shipped": shipped_with_narratives,
            "days": days,
        }

        # ── Aggregate support pressure (HELP tickets across all customers) ──
        try:
            body_help = {
                "jql": f"project = HELP AND created >= -{days}d ORDER BY created DESC",
                "maxResults": 500,
                "fields": ["summary", "status", "issuetype", "priority",
                           "created", "resolution", "labels"],
            }
            self._record_jql(
                body_help["jql"],
                description=f"HELP aggregate desk load (created last {days} days)",
            )
            resp_h = _req.post(
                f"{self.base_url}/rest/api/3/search/jql",
                headers=self._headers, json=body_help, timeout=30,
            )
            resp_h.raise_for_status()
            help_raw = resp_h.json().get("issues", [])
        except Exception as e:
            logger.warning("HELP global fetch failed: %s", e)
            help_raw = []

        help_open = sum(1 for i in help_raw if not i["fields"].get("resolution"))
        help_escalated = sum(
            1 for i in help_raw
            if i["fields"].get("status", {}).get("name") == "In Engineering Queue"
            or "customer_escalation" in (i["fields"].get("labels") or [])
        )
        help_bugs = sum(
            1 for i in help_raw
            if i["fields"].get("issuetype", {}).get("name") == "Bug"
        )
        help_by_priority: dict[str, int] = {}
        for i in help_raw:
            p = (i["fields"].get("priority") or {}).get("name", "Unknown")
            short = p.split(":")[0] if ":" in p else p
            help_by_priority[short] = help_by_priority.get(short, 0) + 1

        support_pressure = {
            "total": len(help_raw),
            "open": help_open,
            "escalated_to_eng": help_escalated,
            "open_bugs": help_bugs,
            "by_priority": dict(sorted(help_by_priority.items(), key=lambda x: -x[1])),
        }

        # ── Weekly LEAN throughput ──
        all_lean = in_flight + closed
        throughput = self._bucket_by_week([
            {"created": t["created"], "updated": t["updated"], "resolution": t["resolution"]}
            for t in all_lean
        ])

        # ── Per-project operational snapshots (HELP / CUSTOMER / LEAN slides) ──
        project_snapshots: dict[str, Any] = {}
        for _pk in ("HELP", "CUSTOMER", "LEAN"):
            try:
                project_snapshots[_pk] = self.get_project_operational_snapshot(_pk)
            except Exception as e:
                logger.warning("Project snapshot %s failed: %s", _pk, e)
                project_snapshots[_pk] = {"error": str(e), "project_key": _pk, "base_url": self.base_url}

        help_ticket_trends = self._get_help_ticket_volume_trends()

        eng_data = {
            "base_url": self.base_url,
            "days": days,
            "sprint": sprint_info,
            "recent_sprints": recent_sprints,
            "in_flight_count": len(in_flight),
            "closed_count": len(closed),
            "by_type": dict(sorted(by_type.items(), key=lambda x: -x[1])),
            "by_status": dict(sorted(by_status.items(), key=lambda x: -x[1])),
            "by_assignee": dict(sorted(by_assignee.items(), key=lambda x: -x[1])),
            "themes": theme_summary,
            "open_bugs": open_bugs,
            "blocker_critical": blocker_critical,
            "velocity": velocity,
            "throughput": throughput,
            "enhancements": enhancements,
            "support_pressure": support_pressure,
            "project_snapshots": project_snapshots,
            "help_ticket_trends": help_ticket_trends,
            "jql_queries": self._jql_since(jql_start),
        }

        # ── Generate slide-level insights in parallel ──
        eng_data["insights"] = _generate_eng_insights(eng_data)
        return eng_data

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
