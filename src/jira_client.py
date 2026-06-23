"""JIRA Cloud client for fetching customer-related issues."""

from __future__ import annotations

import difflib
import copy
import hashlib
import os
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .config import LLM_MODEL_FAST, llm_client, logger
from .config_paths import COHORTS_FILE, JSM_ORGANIZATION_ALIASES_FILE
from .jira_connection import build_jira_connection_settings

# ── Performance: shared JSM org directory (paginated API) ─────────────────
_JSM_ORG_GLOBAL_LOCK = threading.Lock()
_JSM_ORG_GLOBAL_CACHE: dict[str, tuple[float, list[str]]] = {}
_ATLASSIAN_TEAMS_RESPONSE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_ATLASSIAN_TEAMS_CACHE_LOCK = threading.Lock()
# TTL for JSM organization list (same tenant rarely changes during a batch run).
_JSM_ORG_CACHE_TTL_S = float(os.environ.get("CORTEX_JSM_ORG_CACHE_TTL_S", "900"))

_SHARED_JIRA_CLIENT_LOCK = threading.Lock()
_shared_jira_client: Any = None

# Cap HELP body fetch (slide lists / histograms); extra issues are omitted from breakdowns.
HELP_JIRA_BODY_MAX_RESULTS = int(os.environ.get("CORTEX_HELP_JIRA_BODY_MAX", "750"))
# Single merged JQL for ticket metrics (open ∪ 365d resolved ∪ 365d created).
HELP_METRICS_MERGED_MAX_RESULTS = int(os.environ.get("CORTEX_HELP_JIRA_METRICS_MAX", "2000"))
# Full Jira field fetch for LLM + Escalation metrics slide (capped; totals use _jql_match_total).
HELP_ESCALATION_LLM_MAX_ISSUES = int(os.environ.get("CORTEX_HELP_ESCALATION_LLM_MAX_ISSUES", "200"))
# HELP trend fetch cap (created/resolved monthly trend series).
HELP_TRENDS_MAX_RESULTS = int(os.environ.get("CORTEX_HELP_TRENDS_MAX", "12000"))
# HELP resolved-window TTR (JSM SLA ``Time to resolution``, customfield_10665).
HELP_TTR_RESOLVED_MAX_RESULTS = int(os.environ.get("CORTEX_HELP_TTR_RESOLVED_MAX", "2000"))
# Parallel Jira fetches (rate-limit aware).
_JIRA_PARALLEL_WORKERS = max(1, min(4, int(os.environ.get("CORTEX_JIRA_PARALLEL_WORKERS", "3"))))

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

# Exclude transient/infrastructure tickets (Outage, Healthcheck) from support metrics.
# These are typically caused by customer IT and don't reflect actionable support issues.
# Explicit NOT (...): multi-label issues still drop if either label is present (same as NOT IN for HELP).
_TRANSIENT_LABELS_EXCLUSION = 'NOT (labels = Outage OR labels = Healthcheck)'
# HELP monthly operational slide: match common label casings (Jira labels are case-sensitive).
_HELP_MONTHLY_NON_OUTAGE_LABELS = (
    "(labels is EMPTY OR labels not in (Outage, Healthcheck, outage, healthcheck))"
)
_HELP_MONTHLY_OUTAGE_ONLY_LABELS = "(labels in (Outage, Healthcheck, outage, healthcheck))"

# Inclusive day offsets from Salesforce ``factory_start_date`` (calendar days). Fourth element is a
# slide-facing label. Keys align with ``HELP_TICKET_DAY_BUCKETS`` in
# ``scripts/export_entity_contract_help_180d.py`` (that script imports this tuple).
HELP_FACTORY_START_DAY_BUCKETS: tuple[tuple[int, int, str, str], ...] = (
    (0, 40, "help_tickets_days_0_to_40", "Days 0–40 after factory start"),
    (41, 80, "help_tickets_days_41_to_80", "Days 41–80"),
    (81, 120, "help_tickets_days_81_to_120", "Days 81–120"),
    (121, 160, "help_tickets_days_121_to_160", "Days 121–160"),
    (161, 200, "help_tickets_days_161_to_200", "Days 161–200"),
)

# Speaker notes (one line each; index aligns with HELP_FACTORY_START_DAY_BUCKETS).
_HELP_FACTORY_BUCKET_SPEAKER_DESCRIPTIONS: tuple[str, ...] = (
    "Sum HELP tickets created days 0–40 after each entity's factory start (site-scoped JSM org + text).",
    "Sum HELP tickets created days 41–80 after factory start (same scope).",
    "Sum HELP tickets created days 81–120 after factory start (same scope).",
    "Sum HELP tickets created days 121–160 after factory start (same scope).",
    "Sum HELP tickets created days 161–200 after factory start (same scope).",
)

# CUSTOMER/LEAN support slides: exclude portfolio / utility work items (see JIRA_DATA_SCHEMA issue types).
_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION = "issuetype not in (Epic, SUT)"


def _jql_customer_lean_exclude_epic_sut(project: str) -> str:
    """Append to JQL for CUSTOMER or LEAN only; no-op for HELP and other projects."""
    p = (project or "").strip().upper()
    if p in ("CUSTOMER", "LEAN"):
        return f" AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION}"
    return ""

# Minimal fields for per-project operational slides (open + recently resolved).
_PROJECT_SNAPSHOT_FIELDS = [
    "summary", "status", "issuetype", "created", "updated",
    "resolution", "resolutiondate", "assignee",
]

_TREND_FIELDS = [
    "created", "resolutiondate", "labels",
]

JIRA_ESCALATED_LABEL = "jira_escalated"

_CUSTOMER_TICKET_SLIDE_FIELDS = [
    "summary", "status", "issuetype", "project", "priority", "created", "updated",
    "resolution", "resolutiondate", "labels", ORG_FIELD, TTFR_FIELD, TTR_FIELD,
]

_ISSUE_FIELDS = [
    "summary", "status", "issuetype", "project", "priority",
    "labels", "components", "created", "updated", "resolution",
    "assignee", "reporter", "description", "comment",
    CUSTOMER_FIELD, ORG_FIELD, SITE_IDS_FIELD, SEVERITY_FIELD,
    TTFR_FIELD, TTR_FIELD, SENTIMENT_FIELD, REQUEST_TYPE_FIELD,
    SITE_CMDB_FIELD, ENTITY_CMDB_FIELD,
]


_PROJECT_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,29}$")


def _validate_project_key(raw: str) -> str:
    """Sanitise and validate a Jira project key.

    Jira project keys are 2-10 uppercase ASCII letters/digits starting with a
    letter (we allow up to 30 chars and underscores for safety).  Raises
    ``ValueError`` if the cleaned value doesn't match.
    """
    pk = (raw or "").strip().upper()
    if not pk or not _PROJECT_KEY_RE.match(pk):
        raise ValueError(
            f"Invalid Jira project key: {pk!r}. "
            "Expected uppercase letters/digits starting with a letter (e.g. HELP, LEAN)."
        )
    return pk


def _jql_escape_string(value: str) -> str:
    """Escape a value for use inside JQL double-quoted strings.

    Handles the characters that can break out of or alter a JQL string literal.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _norm_org_for_match(s: str) -> str:
    """Normalize a label for fuzzy comparison against JSM organization names."""
    t = (s or "").lower().strip()
    t = re.sub(r"[\s,.'\"&]+", " ", t)
    return t.strip()


def _score_jsm_org_candidate(query: str, organization_name: str) -> float:
    """Similarity in [0, 1] between a search string and a JSM organization name."""
    q = _norm_org_for_match(query)
    o = _norm_org_for_match(organization_name)
    if not q or not o:
        return 0.0
    if q == o:
        return 1.0
    # Containment: require the shorter side to be long enough to avoid noise ("inc", "llc").
    shorter, longer = (q, o) if len(q) <= len(o) else (o, q)
    if len(shorter) >= 5 and shorter in longer:
        return 0.9
    return difflib.SequenceMatcher(None, q, o).ratio()


_JSM_ORG_ALIAS_FILE = JSM_ORGANIZATION_ALIASES_FILE
_COHORTS_FILE = COHORTS_FILE
_jsm_org_alias_map: dict[str, list[str]] | None = None
_cohort_customer_alias_map: dict[str, list[str]] | None = None


def _load_jsm_org_alias_map() -> dict[str, list[str]]:
    """Optional YAML: map lowercased customer key -> extra JSM org search strings (fuzzy + literals)."""
    global _jsm_org_alias_map
    if _jsm_org_alias_map is not None:
        return _jsm_org_alias_map
    _jsm_org_alias_map = {}
    if not _JSM_ORG_ALIAS_FILE.is_file():
        return _jsm_org_alias_map
    try:
        import yaml

        with open(_JSM_ORG_ALIAS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("JSM org aliases: could not load %s: %s", _JSM_ORG_ALIAS_FILE, e)
        return _jsm_org_alias_map
    if not isinstance(data, dict):
        return _jsm_org_alias_map
    for k, v in data.items():
        if not k or not isinstance(v, (list, tuple)):
            continue
        extras = [str(x).strip() for x in v if str(x).strip()]
        if extras:
            _jsm_org_alias_map[str(k).strip().lower()] = extras
    return _jsm_org_alias_map


def _merge_jsm_customer_alias_terms(terms: list[str | None]) -> list[str]:
    """Append alias strings for any term that appears as a key in config/jsm_organization_aliases.yaml."""
    am = _load_jsm_org_alias_map()
    if not am:
        return [t for t in terms if t and str(t).strip()]
    out: list[str] = []
    seen: set[str] = set()
    for t in terms:
        if not t or not (t or "").strip():
            continue
        c = t.strip()
        k = c.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(c)
    for t in list(out):
        extras = am.get(t.lower())
        if not extras:
            continue
        for e in extras:
            el = e.lower()
            if el not in seen:
                seen.add(el)
                out.append(e)
    return out


def _customer_name_variants(name: str) -> list[str]:
    """Conservative Jira text-search variants for a customer/company label."""
    raw = (name or "").strip()
    if not raw:
        return []
    out = [raw]
    # Common legal/geographic suffixes often appear in cohort canonical names but not Jira summaries.
    trimmed = re.sub(
        r"\s+(International|Corporation|Corp\.?|Incorporated|Inc\.?|LLC|Ltd\.?|Limited)$",
        "",
        raw,
        flags=re.IGNORECASE,
    ).strip()
    if trimmed and len(trimmed) >= 5 and trimmed.lower() != raw.lower():
        out.append(trimmed)
    return out


def _salesforce_activity_hint_for_customer_scope(customer_name: str | None) -> str:
    """One-line Salesforce active/churn context for HELP scope warnings (lazy SF load)."""
    query = (customer_name or "").strip()
    if not query:
        return ""
    try:
        from .data_source_health import _salesforce_configured

        if not _salesforce_configured():
            return ""
        from .portfolio_salesforce_allowlist import (
            format_salesforce_label_activity_hint,
            summarize_salesforce_customer_query_activity,
        )
        from .salesforce_client import SalesforceClient

        accounts = SalesforceClient().get_entity_accounts()
        activity = summarize_salesforce_customer_query_activity(query, accounts)
        return format_salesforce_label_activity_hint(activity)
    except Exception as e:
        logger.debug(
            "HELP scope: could not load Salesforce activity hint for %r: %s",
            query,
            e,
        )
        return ""


def _load_cohort_customer_alias_map() -> dict[str, list[str]]:
    """Map any cohort key/name/alias to all known terms for Jira customer text searches."""
    global _cohort_customer_alias_map
    if _cohort_customer_alias_map is not None:
        return _cohort_customer_alias_map
    _cohort_customer_alias_map = {}
    if not _COHORTS_FILE.is_file():
        return _cohort_customer_alias_map
    try:
        import yaml

        with open(_COHORTS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("cohorts aliases: could not load %s: %s", _COHORTS_FILE, e)
        return _cohort_customer_alias_map
    customers = data.get("customers") if isinstance(data, dict) else None
    if not isinstance(customers, dict) and isinstance(data, dict):
        customers = data.get("cohorts")
    if not isinstance(customers, dict):
        return _cohort_customer_alias_map
    for key, row in customers.items():
        terms: list[str] = []
        for t in _customer_name_variants(str(key)):
            terms.append(t)
        if isinstance(row, dict):
            for t in _customer_name_variants(str(row.get("name") or "")):
                terms.append(t)
            aliases = row.get("aliases") or []
            if isinstance(aliases, str):
                aliases = [aliases]
            if isinstance(aliases, (list, tuple)):
                for alias in aliases:
                    for t in _customer_name_variants(str(alias)):
                        terms.append(t)
        deduped: list[str] = []
        seen: set[str] = set()
        for t in terms:
            clean = t.strip()
            k = clean.lower()
            if clean and k not in seen:
                seen.add(k)
                deduped.append(clean)
        for t in deduped:
            _cohort_customer_alias_map[t.lower()] = deduped
    return _cohort_customer_alias_map


def _safe_jira_customer_search_term(term: str) -> bool:
    """Avoid broad one-word aliases that can over-match Jira text search."""
    words = [w for w in re.split(r"\s+", (term or "").strip()) if w]
    if not words:
        return False
    # Short account codes (JCI, GE, etc.) are intentional exact customer prefixes.
    if len(words) == 1 and words[0].isupper():
        return True
    # Single natural-language words like "Johnson" are too broad for Jira summary/description search.
    if len(words) == 1:
        return False
    return True


def jira_customer_search_terms(customer_name: str) -> list[str]:
    """Customer terms for Jira text fields, expanded via config/cohorts.yaml when available."""
    base = [t for t in _customer_name_variants(customer_name) if t.strip()]
    aliases = _load_cohort_customer_alias_map().get((customer_name or "").strip().lower(), [])
    out: list[str] = []
    seen: set[str] = set()
    for t in base + aliases:
        clean = t.strip()
        k = clean.lower()
        if clean and k not in seen and _safe_jira_customer_search_term(clean):
            seen.add(k)
            out.append(clean)
    return out


def _jql_text_match_any(fields: tuple[str, ...], terms: list[str]) -> str:
    clauses: list[str] = []
    for term in terms:
        safe = _jql_escape_string(term)
        for field in fields:
            clauses.append(f'{field} ~ "{safe}"')
    return "(" + " OR ".join(clauses) + ")" if clauses else "(summary ~ \"\")"


def _salesforce_entity_customer_primary_and_extras(entity_row: dict[str, Any]) -> tuple[str, list[str] | None]:
    """Match strings for JSM org resolution (same idea as Salesforce export scripts)."""
    name = (entity_row.get("Name") or "").strip()
    lean = (entity_row.get("LeanDNA_Entity_Name__c") or "").strip()
    if lean and name and lean.lower() != name.lower():
        return lean, [name]
    if lean:
        return lean, None
    return name, None


# Single-token labels that are too broad for HELP summary/description site narrowing (shared org noise).
_HELP_SITE_TEXT_SINGLETON_STOPWORDS = frozenset(
    {
        "carrier",
        "commercial",
        "refrigeration",
        "pending",
        "hvac",
        "customer",
        "entity",
    }
)


def _help_site_text_terms_from_salesforce_entity(entity_row: dict[str, Any]) -> list[str]:
    """Phrases for ``summary`` / ``description`` JQL to narrow HELP to one Customer Entity (site).

    JSM ``Organizations`` alone often resolves to a broad parent (e.g. ``Carrier``). AND-ing OR'd
    text clauses reduces double-counting across sites under the same org when ticket bodies mention
    the site / entity string.
    """
    seen_lower: set[str] = set()
    out: list[str] = []

    def push(term: str) -> None:
        t = (term or "").strip()
        if len(t) < 8:
            return
        wl = t.lower()
        if wl in seen_lower:
            return
        words = [w for w in re.split(r"\s+", t) if w]
        if len(words) == 1 and words[0].lower() in _HELP_SITE_TEXT_SINGLETON_STOPWORDS:
            return
        # Very short “words-only” tokens are usually noise unless acronym-heavy.
        if len(words) == 1 and len(words[0]) < 10 and not words[0].isupper():
            return
        seen_lower.add(wl)
        out.append(t)

    name = (entity_row.get("Name") or "").strip()
    if name:
        push(name)

    lean = (entity_row.get("LeanDNA_Entity_Name__c") or "").strip()
    if lean:
        for part in re.split(r"\s*:\s*", lean):
            push(part.strip())

    out.sort(key=len, reverse=True)
    return out


def _jql_in_quoted_values(field: str, terms: list[str]) -> str:
    vals = ", ".join(f'"{_jql_escape_string(t)}"' for t in terms if t.strip())
    return f'{field} in ({vals})' if vals else f'{field} in ("")'


def _fuzzy_pick_jsm_organizations(queries: list[str], candidates: list[str]) -> list[str]:
    """Map free-text customer names to exact JSM organization labels (enum-safe for JQL).

    Uses the Service Desk organization directory. Short or ambiguous queries are skipped
    so we do not OR in the wrong organization.
    """
    if not candidates:
        return []
    picked: list[str] = []
    seen_lower: set[str] = set()
    for raw_q in queries:
        q = (raw_q or "").strip()
        if not q:
            continue
        nq = _norm_org_for_match(q)
        thresh = 0.92 if len(nq) < 6 else 0.82
        scored = [(org, _score_jsm_org_candidate(q, org)) for org in candidates]
        scored.sort(key=lambda x: -x[1])
        if not scored or scored[0][1] < thresh:
            continue
        top_org, top_s = scored[0]
        second_s = scored[1][1] if len(scored) > 1 else 0.0
        if second_s >= top_s - 0.05:
            logger.debug(
                "JSM org fuzzy match ambiguous for %r: %r (%.2f) vs %r (%.2f); skipping",
                q,
                top_org,
                top_s,
                scored[1][0],
                second_s,
            )
            continue
        lk = top_org.strip().lower()
        if lk not in seen_lower:
            seen_lower.add(lk)
            picked.append(top_org)
    return picked


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


_ACTIVE_WIP_STATUSES = ("In Progress", "In Review")
# Labels / issue types that mark reactive (unplanned) work rather than roadmap.
_REACTIVE_LABELS = (
    "customer_escalation", "escalation", "escalated", "incident",
    "hotfix", "support", "production", "outage", "sev1", "sev2",
)
_REACTIVE_TYPES = ("Bug", "Incident", "Escalation", "Support", "Problem")


def _eng_parse_day(value: Any) -> "date | None":
    try:
        return date.fromisoformat(str(value or "")[:10])
    except (ValueError, TypeError):
        return None


def _eng_median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[mid], 1)
    return round((ordered[mid - 1] + ordered[mid]) / 2, 1)


def _is_reactive_ticket(ticket: dict) -> bool:
    """Classify a LEAN ticket as reactive/unplanned (bug, escalation, incident)."""
    if (ticket.get("type") or "") in _REACTIVE_TYPES:
        return True
    labels = {str(label).lower() for label in (ticket.get("labels") or [])}
    return any(reactive in labels for reactive in _REACTIVE_LABELS)


def _parse_jira_dt(value: Any) -> "datetime | None":
    """Parse a Jira timestamp into an aware datetime (date-only strings → UTC midnight)."""
    s = str(value or "").strip()
    if not s:
        return None
    # Normalize "+00:00" → "+0000" so %z parses consistently.
    s = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", s)
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    d = _eng_parse_day(s)
    if d:
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return None


def compute_status_timeline(
    histories: list[dict],
    *,
    created: Any,
    current_status: str,
    now: "datetime | None" = None,
) -> dict[str, Any]:
    """Derive time-in-status from a Jira changelog (``expand=changelog`` histories).

    Returns total days spent in each status, days in the *current* status (the
    precise "stalled" measure that replaces the noisy ``updated`` proxy), and a
    reopen count. ``histories`` is the raw ``changelog.histories`` list; each entry
    carries a ``created`` timestamp and ``items`` with ``field == 'status'``
    transitions (``fromString`` / ``toString``).
    """
    now = now or datetime.now(timezone.utc)
    created_dt = _parse_jira_dt(created)

    transitions: list[tuple[datetime, str, str]] = []  # (when, from, to)
    for h in histories or []:
        when = _parse_jira_dt(h.get("created"))
        if not when:
            continue
        for it in h.get("items") or []:
            if (it.get("field") or "").lower() == "status":
                transitions.append((when, it.get("fromString") or "", it.get("toString") or ""))
    transitions.sort(key=lambda t: t[0])

    time_in_status: dict[str, float] = {}

    def _add(status: str, start: "datetime | None", end: "datetime | None") -> None:
        if not status or not start or not end:
            return
        days = (end - start).total_seconds() / 86400.0
        if days > 0:
            time_in_status[status] = round(time_in_status.get(status, 0.0) + days, 2)

    # Initial segment status = first transition's "from", else the current status.
    seg_start = created_dt
    seg_status = transitions[0][1] if transitions else current_status
    reopened = 0
    _DONE_STATES = ("closed", "done", "resolved")
    for when, frm, to in transitions:
        _add(seg_status, seg_start, when)
        if (to or "").lower() == "reopened" or (frm or "").lower() in _DONE_STATES:
            reopened += 1
        seg_start = when
        seg_status = to
    _add(seg_status, seg_start, now)

    last_change = transitions[-1][0] if transitions else created_dt
    days_in_current = (
        round((now - last_change).total_seconds() / 86400.0, 1) if last_change else None
    )
    return {
        "time_in_status": time_in_status,
        "days_in_current_status": days_in_current,
        "reopened": reopened,
        "transitions": len(transitions),
    }


def summarize_status_flow(active_items: list[dict], *, now: "datetime | None" = None) -> dict[str, Any]:
    """Aggregate changelog-derived flow across active items, mutating each in place.

    Each item should carry ``status``, ``created``, ``changelog`` (histories list),
    and optionally ``flagged`` (bool). Sets ``days_in_status`` / ``time_in_status`` /
    ``reopened`` on each item and returns per-status median current-occupancy (so the
    real chokepoint is visible), an overall median, and a blocked (flagged) count.
    """
    now = now or datetime.now(timezone.utc)
    by_status_days: dict[str, list[float]] = {}
    all_days: list[float] = []
    blocked = 0
    enriched = 0
    for it in active_items:
        if it.get("flagged"):
            blocked += 1
        tl = compute_status_timeline(
            it.get("changelog") or [],
            created=it.get("created"),
            current_status=it.get("status") or "",
            now=now,
        )
        d = tl.get("days_in_current_status")
        it["days_in_status"] = d
        it["time_in_status"] = tl.get("time_in_status")
        it["reopened"] = tl.get("reopened")
        if d is not None:
            enriched += 1
            all_days.append(d)
            by_status_days.setdefault(it.get("status") or "—", []).append(d)
    return {
        "source": "changelog",
        "by_status_median_days": {s: _eng_median(v) for s, v in by_status_days.items()},
        "median_days_in_status": _eng_median(all_days),
        "blocked_count": blocked,
        "enriched_count": enriched,
    }


# Backlog staleness thresholds. The LEAN "open" set is dominated by abandoned work
# (~80% untouched in 30d, ~60% in 180d), which inflates WIP, stall, and queue numbers
# across the deck. We separate genuinely-active work from the zombie tail so the slides
# stay actionable and add an explicit hygiene number a VP can act on.
ENG_ABANDONED_DAYS = 180   # no movement in this long → abandoned/zombie, not real WIP
ENG_ACTIVE_WIP_DAYS = 30   # touched within this window → actively being worked


def compute_eng_flow(
    in_flight: list[dict],
    closed: list[dict],
    *,
    today: "date | None" = None,
    stage_age_by_key: "dict[str, float] | None" = None,
    flagged_keys: "set[str] | None" = None,
    abandoned_days: int = ENG_ABANDONED_DAYS,
) -> dict[str, Any]:
    """Derive flow / bottleneck signals from in-flight and recently closed LEAN tickets.

    Surfaces where work is piling up (active WIP by status), where it is stalling,
    how old active work is, and whether cycle time is trending up.

    The stall measure prefers ``stage_age_by_key`` — changelog-derived days in the
    current status — when supplied; otherwise it falls back to a ``updated`` idle
    proxy. ``flagged_keys`` marks items flagged as blocked/impediment in Jira. Both
    feed the stale counts, attention selection, ranking, and the slide headline.
    """
    today = today or date.today()
    stage_age_by_key = stage_age_by_key or {}
    flagged_set = set(flagged_keys or ())
    active = [t for t in in_flight if (t.get("status") or "") in _ACTIVE_WIP_STATUSES]

    def _idle_days(ticket: dict) -> int | None:
        d = _eng_parse_day(ticket.get("updated"))
        return (today - d).days if d else None

    def _age_days(ticket: dict) -> int | None:
        d = _eng_parse_day(ticket.get("created"))
        return (today - d).days if d else None

    active_ages = [a for a in (_age_days(t) for t in active) if a is not None]
    stale_gt5 = 0
    stale_gt10 = 0
    stale_recent = 0          # genuinely stalled but still actionable (10d < stall ≤ abandoned_days)
    abandoned_in_stage = 0    # parked in the same stage longer than abandoned_days (zombie WIP)
    carryover_count = 0
    carryover_points = 0.0
    blocked_count = 0
    # Active stage ages for items that are NOT abandoned — used to report honest stage
    # medians (the all-items median is dragged to years by zombies parked in-stage).
    active_status_eff: dict[str, list[float]] = {}
    # "Needs attention" = active items that are flagged blocked, carried over across a
    # sprint boundary (sprint_count ≥ 2), or stalled past the freshness threshold —
    # but NOT items abandoned in-stage past ``abandoned_days`` (those are a hygiene
    # problem, surfaced separately, and would otherwise drown the actionable list).
    # Stall uses changelog days-in-status when available, else the ``updated`` proxy.
    attention_rows: list[dict[str, Any]] = []
    abandoned_rows: list[dict[str, Any]] = []
    for ticket in active:
        key = ticket.get("key", "")
        idle = _idle_days(ticket)
        stage_age = stage_age_by_key.get(key)
        # Effective stall measure: changelog stage age beats the noisy update proxy.
        eff_stall = stage_age if stage_age is not None else idle
        sprint_count = int(ticket.get("sprint_count") or len(ticket.get("sprints") or []))
        is_carryover = sprint_count >= 2
        is_flagged = key in flagged_set
        is_abandoned = eff_stall is not None and eff_stall > abandoned_days
        sp = ticket.get("story_points")
        if is_flagged:
            blocked_count += 1
        if is_carryover:
            carryover_count += 1
            carryover_points += float(sp) if sp is not None else 0.0
        if eff_stall is not None and eff_stall > 5:
            stale_gt5 += 1
        if eff_stall is not None and eff_stall > 10:
            stale_gt10 += 1
        if eff_stall is not None and 10 < eff_stall <= abandoned_days:
            stale_recent += 1
        if eff_stall is not None and not is_abandoned:
            active_status_eff.setdefault(ticket.get("status") or "—", []).append(float(eff_stall))
        row = {
            "key": key,
            "summary": (ticket.get("summary") or "")[:90],
            "status": ticket.get("status", ""),
            "priority": ticket.get("priority", "") or "",
            "assignee": ticket.get("assignee", "") or "Unassigned",
            "story_points": sp,
            "sprint_count": sprint_count,
            "carryover": is_carryover,
            "flagged": is_flagged,
            "idle_days": idle,
            "days_in_status": stage_age,
            "age_days": _age_days(ticket),
        }
        if is_abandoned and not is_flagged:
            abandoned_in_stage += 1
            abandoned_rows.append(row)
            continue
        if is_flagged or is_carryover or (eff_stall is not None and eff_stall > 5):
            attention_rows.append(row)

    def _eff(row: dict) -> float:
        v = row.get("days_in_status")
        if v is None:
            v = row.get("idle_days")
        return v or 0

    # Rank: flagged first, then carried-over, then longest stalled, then oldest.
    attention_rows.sort(
        key=lambda r: (
            0 if r["flagged"] else 1,
            0 if r["carryover"] else 1,
            -_eff(r),
            -(r["age_days"] or 0),
        )
    )
    # Backward-compatible alias: ``stale_items`` historically meant stalled>5 rows.
    stale_rows = [r for r in attention_rows if _eff(r) > 5]
    stale_rows.sort(key=lambda r: -_eff(r))
    abandoned_rows.sort(key=lambda r: -_eff(r))

    # Cycle-time trend: median (resolved - created) of closed tickets bucketed by
    # the ISO week they closed in, oldest → newest (updated ≈ resolved for closed).
    week_cycles: dict[str, list[float]] = {}
    for ticket in closed:
        created = _eng_parse_day(ticket.get("created"))
        resolved = _eng_parse_day(ticket.get("updated"))
        if not (created and resolved) or resolved < created:
            continue
        iso = resolved.isocalendar()
        wk = f"{iso[0]}-W{iso[1]:02d}"
        week_cycles.setdefault(wk, []).append((resolved - created).days)
    cycle_trend = [
        {"week": wk, "median_cycle_days": _eng_median(vals), "closed": len(vals)}
        for wk, vals in sorted(week_cycles.items())
    ][-6:]
    cycle_delta = None
    medians = [p["median_cycle_days"] for p in cycle_trend if p["median_cycle_days"] is not None]
    if len(medians) >= 2:
        cycle_delta = round(medians[-1] - medians[0], 1)

    return {
        "active_count": len(active),
        "in_progress": sum(1 for t in active if (t.get("status") or "") == "In Progress"),
        "in_review": sum(1 for t in active if (t.get("status") or "") == "In Review"),
        "stale_gt5": stale_gt5,
        "stale_gt10": stale_gt10,
        "stale_recent": stale_recent,
        "abandoned_in_stage": abandoned_in_stage,
        "abandoned_days": abandoned_days,
        "carryover_count": carryover_count,
        "carryover_points": round(carryover_points, 1) if carryover_points else 0.0,
        "blocked_count": blocked_count,
        "median_active_age_days": _eng_median([float(a) for a in active_ages]),
        "oldest_active_age_days": max(active_ages) if active_ages else None,
        "by_status_median_active": {
            s: _eng_median(v) for s, v in active_status_eff.items() if v
        },
        "stale_items": stale_rows[:6],
        "attention_items": attention_rows[:12],
        "abandoned_items": abandoned_rows[:6],
        "cycle_trend": cycle_trend,
        "cycle_delta_days": cycle_delta,
    }


def compute_eng_work_split(
    in_flight: list[dict],
    closed: list[dict],
    *,
    escalated_to_eng: int = 0,
) -> dict[str, Any]:
    """Split engineering work into planned (roadmap) vs unplanned (reactive) load.

    Reactive = bugs, escalations, incidents, or escalation-labeled tickets.  Returns
    current WIP split, trailing closed-period split, and the reactive share of each,
    so a VP can see how much capacity keep-the-lights-on work is consuming.
    """
    def _split(tickets: list[dict]) -> dict[str, int]:
        reactive = sum(1 for t in tickets if _is_reactive_ticket(t))
        return {"planned": len(tickets) - reactive, "unplanned": reactive, "total": len(tickets)}

    wip = _split(in_flight)
    closed_split = _split(closed)
    bugs_wip = sum(1 for t in in_flight if (t.get("type") or "") == "Bug")
    return {
        "wip": wip,
        "closed": closed_split,
        "reactive_wip_pct": round(wip["unplanned"] / wip["total"] * 100) if wip["total"] else 0,
        "reactive_closed_pct": (
            round(closed_split["unplanned"] / closed_split["total"] * 100) if closed_split["total"] else 0
        ),
        "unplanned_breakdown": {
            "Bugs": bugs_wip,
            "Escalations / other": max(0, wip["unplanned"] - bugs_wip),
        },
        "escalated_to_eng": int(escalated_to_eng or 0),
    }


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
    themes = [t for t in (eng.get("themes") or []) if t.get("theme") != "Untagged"]
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


def _generate_eng_takeaways(eng: dict) -> dict[str, str]:
    """Generate one "what this means" implication sentence per content slide.

    Returns ``{slide_key: sentence}``. Each value is a single, plain-text sentence
    that states the implication / decision the slide drives — rendered in the slide's
    bottom takeaway band (replacing the old per-slide scope/methodology footer). Runs
    all calls in parallel; any failure yields an empty string (band is then skipped).
    """
    from concurrent.futures import ThreadPoolExecutor

    _oai = llm_client()

    def _call(slide_key: str, prompt: str) -> tuple[str, str]:
        try:
            resp = _oai.chat.completions.create(
                model=LLM_MODEL_FAST,
                messages=[
                    {"role": "system", "content": (
                        "You write the single-sentence 'so what' takeaway at the bottom of an "
                        "engineering-review slide for a VP of Engineering. Rules:\n"
                        "- Output EXACTLY one sentence, 12-26 words, plain text only\n"
                        "- State the implication or the decision it forces, not a restatement of the numbers\n"
                        "- Cite one concrete number for weight, and name a specific, actionable next step "
                        "(who/what to do), not a vague gesture\n"
                        "- BANNED vague filler: 'strategic review', 'root causes', 'investigate', 'demands attention', "
                        "'requires immediate action', 'closely monitor', 'reassess' — say the concrete action instead\n"
                        "- No markdown, no leading bullet/dash, no label, no preamble\n"
                        "- Tone: direct, analytical, board-room; never salesy or hedging"
                    )},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                # LLM_MODEL_FAST (gemini-2.5-flash) is a thinking model where max_tokens
                # is the TOTAL budget (reasoning + visible); a low cap leaves only a few
                # output tokens and truncates the sentence mid-word. Give ample headroom.
                max_tokens=1024,
            )
            text = " ".join((resp.choices[0].message.content or "").split()).strip()
            text = text.lstrip("•-–—* ").strip()
            return slide_key, text
        except Exception as e:
            logger.warning("Takeaway generation failed for %s: %s", slide_key, e)
            return slide_key, ""

    # ── Pull the same derived structures the slides render from ──
    scard = (eng.get("team_scorecard") or {}).get("summary") or {}
    flow = eng.get("flow") or {}
    sflow = flow.get("status_flow") or {}
    lean = (eng.get("project_snapshots") or {}).get("LEAN") or {}
    split = eng.get("work_split") or {}
    sp = eng.get("support_pressure") or {}
    bug_flow = eng.get("bug_flow") or {}
    epic_progress = eng.get("epic_progress") or {}

    def _epic_line(r: dict) -> str:
        base = f"{r.get('key')} {r.get('pct')}% complete with {r.get('remaining')} issues remaining"
        if r.get("stalled"):
            return base + " (STALLED, no activity 30d)"
        return base + f", {r.get('active_30d')} updated/30d"

    epic_top = "; ".join(
        _epic_line(r) for r in (epic_progress.get("epics") or [])[:3]
    ) or "none"

    sprint = eng.get("sprint") or {}
    sprint_name = sprint.get("name", "current sprint")
    in_flight = eng.get("in_flight_count", 0)
    closed = eng.get("closed_count", 0)
    by_status = eng.get("by_status") or {}
    active = int(by_status.get("In Progress", 0) or 0) + int(by_status.get("In Review", 0) or 0)
    by_type = eng.get("by_type") or {}
    bugs_if = by_type.get("Bug", 0)
    themes = [t for t in (eng.get("themes") or []) if t.get("theme") != "Untagged"]
    top_themes = ", ".join(
        f"{t.get('theme')} ({t.get('total')})" for t in themes[:4] if t.get("theme")
    )

    open_bugs = eng.get("open_bugs") or []
    blockers = eng.get("blocker_critical") or []
    bug_prio: dict[str, int] = {}
    for b in open_bugs:
        p = str(b.get("priority", "Unknown")).split(":")[0]
        bug_prio[p] = bug_prio.get(p, 0) + 1

    by_assignee = eng.get("by_assignee") or {}
    wip_vals = sorted((int(v) for v in by_assignee.values()), reverse=True)
    total_wip = sum(wip_vals)
    active_vals = sorted((int(v) for v in (eng.get("by_assignee_active") or {}).values()), reverse=True)
    total_active = sum(active_vals)
    top3_share = int(round(sum(active_vals[:3]) / total_active * 100)) if total_active else 0
    engineers = sum(1 for v in active_vals if v > 0)
    assigned_stale = sum(int(v) for v in (eng.get("by_assignee_stale") or {}).values())
    staleness = eng.get("backlog_staleness") or {}

    median_by_status = flow.get("by_status_median_active") or sflow.get("by_status_median_days") or {}
    stage_str = ", ".join(f"{k} {v:.0f}d" for k, v in list(median_by_status.items())[:4] if v is not None)

    try:
        from .eng_sprint_velocity import build_sprint_velocity_series
        vseries = build_sprint_velocity_series(eng.get("sprint_velocity"))
        sp_total = vseries.get("sp_total") or []
        tickets_total = vseries.get("tickets_total") or []
        sp_teams = ", ".join(vseries.get("teams") or []) or "scrum boards"
        zero_sp = ", ".join(vseries.get("zero_sp_teams") or [])
    except Exception:
        sp_total, tickets_total, sp_teams, zero_sp = [], [], "scrum boards", ""

    days = eng.get("days", 30)

    tasks = [
        ("team_scorecard", (
            f"Last sprint the engineering teams closed {scard.get('total_throughput')} issues total (throughput) "
            f"at an average lead time of {scard.get('average_median_lead_days')}d (created to resolved). "
            "Six LEAN squads run continuous flow with no fixed sprint commitment. The CUSTOMER 'Active Scrum' "
            "board parks a large standing backlog inside each weekly sprint (~100 issues roll over week to week), "
            "so its commit-vs-complete ratio is a sprint-hygiene artifact, not a true delivery rate. "
            "Implication: compare teams on throughput and lead time; flag the CUSTOMER board's bloated in-sprint "
            "scope as a backlog-hygiene issue to fix. Do NOT frame it as a delivery or predictability failure."
        )),
        ("current_sprint", (
            f"Sprint {sprint_name}: {in_flight} open items, {active} actively in progress/review, "
            f"{bugs_if} bugs in flight, {closed} closed this period. Top themes: {top_themes}. "
            f"{staleness.get('abandoned_open')} open items ({staleness.get('abandoned_pct')}%) untouched "
            f">{staleness.get('abandoned_days')}d. "
            "Implication about focus and how much is truly moving vs sitting in an abandoned backlog?"
        )),
        ("flow_bottlenecks", (
            f"Active WIP {flow.get('active_count')}, in review {flow.get('in_review')}, "
            f"recently stalled (10–{flow.get('abandoned_days')}d in stage) {flow.get('stale_recent')}, "
            f"abandoned in stage >{flow.get('abandoned_days')}d {flow.get('abandoned_in_stage')} (zombie WIP). "
            f"Stage medians on active (non-abandoned) items: {stage_str}. "
            "Implication: separate the actionable recent stalls to unblock from the abandoned backlog to triage/close?"
        )),
        ("backlog_health", (
            f"Open LEAN queue {lean.get('open_count')} tickets, median age {lean.get('median_open_age_days')}d, "
            f"{lean.get('open_over_90_count')} open >90 days, oldest {lean.get('oldest_open_age_days')}d, "
            f"avg resolve cycle {lean.get('avg_resolved_cycle_days')}d. "
            "Implication about queue hygiene and whether the backlog is being worked or just aging?"
        )),
        ("capacity", (
            f"{total_active} actively-worked WIP items (touched ≤{staleness.get('active_days')}d) across {engineers} engineers; "
            f"top 3 hold {top3_share}% of active WIP. Of {total_wip} total assigned open items, "
            f"{assigned_stale} are stale (assigned but untouched >{staleness.get('abandoned_days')}d). "
            "Implication about real load balance and key-person risk — and stale-assignment cleanup?"
        )),
        ("work_split", (
            f"Reactive share of WIP {split.get('reactive_wip_pct')}%, reactive share of closed "
            f"{split.get('reactive_closed_pct')}%; unplanned breakdown {split.get('unplanned_breakdown')}. "
            "Implication about how much roadmap capacity reactive work is consuming?"
        )),
        ("bug_health", (
            f"Open bugs {len(open_bugs)}, blocker/critical {len(blockers)}, priority mix {bug_prio}. "
            f"Top blockers: {', '.join(b.get('key','') for b in blockers[:3])}. "
            "Implication about severity and what demands attention this sprint?"
        )),
        ("bug_flow", (
            f"Bug backlog flow over the last {bug_flow.get('weeks_count')} weeks: "
            f"{bug_flow.get('created_total')} bugs created vs {bug_flow.get('resolved_total')} resolved "
            f"(net {bug_flow.get('net_total')}, trend {bug_flow.get('trend')}); {bug_flow.get('open_now')} open now. "
            "Implication: is the team out-pacing incoming bugs or falling behind — and what does the trend demand?"
        )),
        ("epic_progress", (
            f"{epic_progress.get('epic_count')} in-flight initiatives (epics) ranked by remaining work. "
            "Percentages are % of child issues COMPLETE (so a high % means nearly done, NOT a lot left); "
            "'remaining' is the count of open child issues. "
            f"{epic_progress.get('total_remaining')} child issues still open across all epics, "
            f"{epic_progress.get('early_stage_count')} early-stage (<50% complete), {epic_progress.get('at_risk_count')} at risk "
            f"(stalled = open work but no child activity in 30d). Top by remaining work: {epic_top}. "
            "Implication about whether the big rocks are actually moving and where delivery risk concentrates? "
            "Do not confuse % complete with % remaining."
        )),
        ("velocity", (
            f"Story points delivered per recent sprint (oldest to newest): {sp_total}. "
            f"Tickets delivered per sprint (oldest to newest): {tickets_total}. "
            f"SP-estimating boards: {sp_teams}. Boards without story points: {zero_sp or 'none'}. "
            "Implication about the delivery trend — consider BOTH story points and ticket throughput "
            "(ticket count can fall even when SP looks flat); avoid over-reading a one-sprint move?"
        )),
        ("support_pressure", (
            f"Support tickets last {days} days: {sp.get('total')} total, {sp.get('escalated_to_eng')} escalated to engineering, "
            f"{sp.get('open_bugs')} open support bugs, priority mix {sp.get('by_priority')}. "
            "Implication about inbound pressure on engineering capacity?"
        )) if sp.get("total") else (
            "support_pressure",
            f"No support ticket data for the last {days} days. State that inbound support volume is unavailable.",
        ),
    ]

    takeaways: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_call, key, prompt) for key, prompt in tasks]
        for f in futures:
            key, sentence = f.result()
            takeaways[key] = sentence

    return takeaways


class JiraClient:
    def __init__(self):
        conn = build_jira_connection_settings()
        self._connection = conn
        self.api_base_url = conn.api_base_url.rstrip("/")
        self.base_url = conn.browse_base_url.rstrip("/")
        self._headers = dict(conn.headers)
        self._jql_log: list[dict[str, str]] = []
        self._jql_lock = threading.Lock()
        self._jsm_cache_key = hashlib.sha256(
            f"{self.api_base_url}\0{self._headers.get('Authorization', '')}".encode()
        ).hexdigest()
        # One LLM resolution per (tenant, request terms) per JiraClient lifetime (avoids 15+ calls per support deck)
        self._jsm_llm_org_resolve_cache: dict[str, list[str]] = {}
        # Atlassian Teams (org-level rosters). orgId is required for the API path; the
        # admin API key (if present) is the preferred Bearer credential, with the site
        # gateway + Jira auth as a fallback. See get_atlassian_teams().
        self.atlassian_org_id = (os.environ.get("ATLASSIAN_ORG_ID") or "").strip() or None
        self.atlassian_api_key = (os.environ.get("ATLASSIAN_API_KEY") or "").strip() or None
        self._atlassian_user_name_cache: dict[str, str] = {}
        self._atlassian_user_email_cache: dict[str, str] = {}

    def _jql_log_len(self) -> int:
        with self._jql_lock:
            return len(self._jql_log)

    def _record_jql(self, jql: str, *, description: str | None = None) -> None:
        """Record JQL with a short human label for speaker notes (``[label] - JQL``)."""
        cleaned = (jql or "").strip()
        if not cleaned:
            return
        label = (description or "Jira issue search").strip()
        with self._jql_lock:
            self._jql_log.append({"description": label, "jql": cleaned})

    def _jql_since(self, start_idx: int) -> list[dict[str, str]]:
        """Return unique JQL entries since start_idx, preserving order (dedupe by JQL text)."""
        with self._jql_lock:
            tail = list(self._jql_log[start_idx:])
        seen: set[str] = set()
        out: list[dict[str, str]] = []
        for entry in tail:
            jql = (entry.get("jql") or "").strip()
            if not jql or jql in seen:
                continue
            seen.add(jql)
            desc = (entry.get("description") or "Jira issue search").strip()
            out.append({"description": desc, "jql": jql})
        return out

    # ──────────────────────────────────────────────────────────────────────────
    # Atlassian Teams (org rosters)
    # ──────────────────────────────────────────────────────────────────────────
    def _atlassian_teams_routes(self) -> list[tuple[str, dict[str, str]]]:
        """Candidate (base_url, headers) pairs for the Atlassian Teams public API.

        Prefer the public api.atlassian.com endpoint with the org admin key (Bearer);
        fall back to the site gateway, which is reachable with the existing Jira auth.
        Both require the real Atlassian orgId in the path (not the cloudId).
        """
        routes: list[tuple[str, dict[str, str]]] = []
        org = self.atlassian_org_id
        if self.atlassian_api_key:
            routes.append((
                f"https://api.atlassian.com/public/teams/v1/org/{org}",
                {
                    "Authorization": f"Bearer {self.atlassian_api_key}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
            ))
        routes.append((
            f"{self.base_url}/gateway/api/public/teams/v1/org/{org}",
            {**self._headers, "Content-Type": "application/json"},
        ))
        return routes

    def _resolve_account_names(self, account_ids: set[str], *, timeout: float = 30.0) -> dict[str, str]:
        """Resolve Atlassian accountIds → display names via Jira's bulk user API (cached)."""
        todo = [
            a for a in account_ids
            if a and (a not in self._atlassian_user_name_cache or a not in self._atlassian_user_email_cache)
        ]
        for i in range(0, len(todo), 90):
            chunk = todo[i:i + 90]
            try:
                # ``/user/bulk`` defaults to maxResults=10 — must request the full chunk
                # size or only the first 10 accountIds resolve.
                params = [("accountId", a) for a in chunk]
                params.append(("maxResults", str(len(chunk))))
                resp = requests.get(
                    f"{self.api_base_url}/rest/api/3/user/bulk",
                    headers=self._headers,
                    params=params,
                    timeout=timeout,
                )
                if resp.status_code != 200:
                    continue
                for user in resp.json().get("values") or []:
                    aid = user.get("accountId")
                    name = user.get("displayName")
                    if aid and name and user.get("accountType") == "atlassian":
                        self._atlassian_user_name_cache[aid] = name
                    email = (user.get("emailAddress") or "").strip()
                    if aid and email and user.get("accountType") == "atlassian":
                        self._atlassian_user_email_cache[aid] = email
            except requests.RequestException as e:
                logger.warning("Atlassian user bulk lookup failed: %s", e)
        return {a: self._atlassian_user_name_cache[a] for a in account_ids if a in self._atlassian_user_name_cache}

    def resolve_account_names(
        self, account_ids: set[str] | list[str], *, timeout: float = 30.0
    ) -> dict[str, str]:
        """Resolve Atlassian accountIds → display names (via Jira bulk user API, cached)."""
        return self._resolve_account_names({a for a in account_ids if a}, timeout=timeout)

    def resolve_account_emails(
        self, account_ids: set[str] | list[str], *, timeout: float = 30.0
    ) -> dict[str, str]:
        """Resolve Atlassian accountIds → corporate email (via Jira bulk user API, cached)."""
        ids = {a for a in account_ids if a}
        if not ids:
            return {}
        self._resolve_account_names(ids, timeout=timeout)
        return {a: self._atlassian_user_email_cache[a] for a in ids if a in self._atlassian_user_email_cache}

    def _atlassian_team_member_ids(
        self, base: str, headers: dict[str, str], team_id: str, *, timeout: float = 30.0
    ) -> list[str]:
        """All member accountIds for one team (paginated via POST .../members)."""
        ids: list[str] = []
        after: str | None = None
        for _ in range(100):  # hard page cap
            body: dict[str, Any] = {"first": 50}
            if after:
                body["after"] = after
            try:
                resp = requests.post(
                    f"{base}/teams/{team_id}/members", headers=headers, json=body, timeout=timeout
                )
            except requests.RequestException as e:
                logger.warning("Atlassian team members fetch failed (%s): %s", team_id, e)
                break
            if resp.status_code != 200:
                break
            data = resp.json()
            for member in data.get("results") or []:
                aid = member.get("accountId")
                if aid:
                    ids.append(aid)
            page = data.get("pageInfo") or {}
            if page.get("hasNextPage") and page.get("endCursor"):
                after = page["endCursor"]
            else:
                break
        return ids

    def _atlassian_teams_cache_key(
        self,
        *,
        with_members: bool,
        resolve_names: bool,
        max_teams: int,
    ) -> str:
        return f"{self.atlassian_org_id}|m={int(with_members)}|n={int(resolve_names)}|max={max_teams}"

    def get_atlassian_teams(
        self,
        *,
        with_members: bool = True,
        resolve_names: bool = True,
        max_teams: int = 500,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        """Fetch org Teams (and members) from the Atlassian Teams API.

        Requires ``ATLASSIAN_ORG_ID``; uses ``ATLASSIAN_API_KEY`` when present, else the
        site gateway with the existing Jira auth. Returns ``{"teams": [...], "error": ...}``
        where each team has ``team_id``, ``name``, ``members`` (display names), and counts.
        Fails loud with an ``error`` string rather than silently returning empty.
        """
        if not self.atlassian_org_id:
            return {"error": "ATLASSIAN_ORG_ID is not set", "teams": []}

        from .config import CORTEX_ATLASSIAN_TEAMS_CACHE_TTL_SECONDS

        cache_key = self._atlassian_teams_cache_key(
            with_members=with_members,
            resolve_names=resolve_names,
            max_teams=max_teams,
        )
        if CORTEX_ATLASSIAN_TEAMS_CACHE_TTL_SECONDS > 0:
            with _ATLASSIAN_TEAMS_CACHE_LOCK:
                hit = _ATLASSIAN_TEAMS_RESPONSE_CACHE.get(cache_key)
                if hit and time.time() - hit[0] < CORTEX_ATLASSIAN_TEAMS_CACHE_TTL_SECONDS:
                    logger.debug("Atlassian Teams cache hit")
                    return copy.deepcopy(hit[1])

        chosen: tuple[str, dict[str, str]] | None = None
        last_err: str | None = None
        for base, headers in self._atlassian_teams_routes():
            try:
                resp = requests.get(f"{base}/teams", headers=headers, params={"size": 50}, timeout=timeout)
                if resp.status_code == 200:
                    chosen = (base, headers)
                    break
                last_err = f"{resp.status_code}: {(resp.text or '')[:160]}"
            except requests.RequestException as e:
                last_err = str(e)
        if not chosen:
            return {"error": f"Atlassian Teams API unreachable: {last_err}", "teams": []}

        base, headers = chosen
        raw_teams: list[dict[str, Any]] = []
        cursor: str | None = None
        while len(raw_teams) < max_teams:
            params = {"size": 50}
            if cursor:
                params["cursor"] = cursor
            try:
                resp = requests.get(f"{base}/teams", headers=headers, params=params, timeout=timeout)
                resp.raise_for_status()
            except requests.RequestException as e:
                if raw_teams:
                    logger.warning("Atlassian teams pagination stopped early: %s", e)
                    break
                return {"error": f"Atlassian teams list failed: {e}", "teams": []}
            data = resp.json()
            entities = data.get("entities") or []
            raw_teams.extend(entities)
            cursor = data.get("cursor")
            if not cursor or not entities:
                break

        member_ids_by_team: dict[str, list[str]] = {}
        all_ids: set[str] = set()
        if with_members:
            for team in raw_teams:
                tid = team.get("teamId")
                if not tid:
                    continue
                ids = self._atlassian_team_member_ids(base, headers, tid, timeout=timeout)
                member_ids_by_team[tid] = ids
                all_ids.update(ids)

        names = self._resolve_account_names(all_ids, timeout=timeout) if (with_members and resolve_names) else {}

        teams: list[dict[str, Any]] = []
        for team in raw_teams:
            tid = team.get("teamId")
            ids = member_ids_by_team.get(tid, [])
            member_names = [names[a] for a in ids if a in names] if resolve_names else []
            teams.append({
                "team_id": tid,
                "name": team.get("displayName"),
                "description": team.get("description") or "",
                "state": team.get("state"),
                "member_account_ids": ids,
                "members": member_names,
                "member_count": len(member_names) if resolve_names else len(ids),
            })
        result = {"org_id": self.atlassian_org_id, "route": base, "teams": teams, "error": None}
        if CORTEX_ATLASSIAN_TEAMS_CACHE_TTL_SECONDS > 0 and not result.get("error"):
            with _ATLASSIAN_TEAMS_CACHE_LOCK:
                _ATLASSIAN_TEAMS_RESPONSE_CACHE[cache_key] = (time.time(), copy.deepcopy(result))
        return result

    def _jql_match_total(self, jql: str) -> int | None:
        """Return Jira's match count for JQL without fetching issue bodies.

        Uses ``POST /rest/api/3/search/approximate-count`` (Atlassian requires bounded JQL for some
        tenants; counts may lag slightly vs live search — see Jira docs).
        """
        try:
            body: dict[str, Any] = {"jql": jql.strip()}
            resp = requests.post(
                f"{self.api_base_url}/rest/api/3/search/approximate-count",
                headers=self._headers,
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            c = data.get("count")
            return int(c) if c is not None else None
        except Exception as e:
            logger.debug("JIRA jql match total failed: %s", e)
            return None

    def jql_match_count(self, jql: str, *, data_description: str | None = None) -> int | None:
        """Public wrapper: Jira's match count for *jql* without fetching issue bodies.

        Returns ``None`` when the count endpoint is unavailable so callers can fail loud.
        """
        self._record_jql(jql, description=data_description or "Jira issue count")
        return self._jql_match_total(jql)

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
                f"{self.api_base_url}/rest/api/3/search/jql",
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

    def _list_jsm_organization_names(self) -> list[str]:
        """All JSM organization names (exact strings valid in ``Organizations =`` JQL).

        Cached process-wide with TTL so every new ``JiraClient()`` does not re-walk the
        paginated Service Desk API. Set ``CORTEX_JIRA_SKIP_JSM_ORG_FUZZY=1`` to skip the
        directory fetch (literal ``Organizations =`` terms only).
        """
        if os.environ.get("CORTEX_JIRA_SKIP_JSM_ORG_FUZZY", "").strip() in ("1", "true", "yes"):
            return []

        now = time.monotonic()
        with _JSM_ORG_GLOBAL_LOCK:
            ent = _JSM_ORG_GLOBAL_CACHE.get(self._jsm_cache_key)
            if ent is not None:
                ts, names = ent
                if now - ts < _JSM_ORG_CACHE_TTL_S:
                    return names

        names: list[str] = []
        start = 0
        limit = 50
        try:
            while True:
                url = (
                    f"{self.api_base_url}/rest/servicedeskapi/organization"
                    f"?start={start}&limit={limit}"
                )
                resp = requests.get(url, headers=self._headers, timeout=45)
                resp.raise_for_status()
                data = resp.json()
                batch = data.get("values") or []
                for v in batch:
                    if not isinstance(v, dict):
                        continue
                    n = (v.get("name") or "").strip()
                    if n:
                        names.append(n)
                if data.get("isLastPage", True) or not batch:
                    break
                start += len(batch)
        except Exception as e:
            logger.warning(
                "Could not list JSM organizations (%s); JQL uses literal customer names only",
                e,
            )
            with _JSM_ORG_GLOBAL_LOCK:
                _JSM_ORG_GLOBAL_CACHE[self._jsm_cache_key] = (now, [])
            return []

        seen_ci: set[str] = set()
        unique: list[str] = []
        for n in names:
            k = n.lower()
            if k not in seen_ci:
                seen_ci.add(k)
                unique.append(n)
        if not unique:
            logger.warning(
                "JSM organization directory returned HTTP 200 with 0 organizations "
                "(tenant has ~186 orgs when an agent calls this API). Service account token "
                "needs scope read:organization:jira-service-management (UI: View organizations) "
                "and the account must be a JSM agent on HELP — not only Jira issue read. "
                "GET /rest/servicedeskapi/servicedesk often returns 401 until Service Desk scopes are added."
            )
        with _JSM_ORG_GLOBAL_LOCK:
            _JSM_ORG_GLOBAL_CACHE[self._jsm_cache_key] = (now, unique)
        return unique

    def _customer_match_clause(
        self,
        customer_name: str | None,
        match_terms: list[str] | None = None,
        *,
        organizations_only: bool = False,
    ) -> tuple[str, list[str]]:
        """Build JQL to match a customer for **project HELP** only: JSM ``Organizations`` (and optional text).

        For **CUSTOMER** and **LEAN** projects, use :meth:`_customer_project_text_match_clause` instead
        (``summary`` / ``description``), not this method.

        JSM ``Organizations`` is the authoritative link to a customer, but JQL requires
        the exact directory string. We list organizations via the Service Desk API and
        fuzzy-match the customer name plus any extra terms from
        ``config/jsm_organization_aliases.yaml`` (e.g. JCI → "Johnson Controls") to those labels, then OR
        ``Organizations = "<resolved>"`` together. By default we also OR ``summary`` /
        ``description`` matches for tickets that lack org metadata (QBR / discovery lists).

        For **Ticket Metrics** (KPIs, by-type / by-status breakdowns), set
        ``organizations_only=True`` so counts are not inflated by the word *Carrier* in
        title/body on unrelated orgs.

        If customer_name is None, returns an empty filter (matches all customers).

        Returns:
            ``(jql_fragment, resolved_jsm_organization_names)`` — the second value is
            the fuzzy-matched enum labels (may be empty if the API failed or no confident match).
        """
        # Handle "all customers" case
        if not customer_name:
            # Jira JQL-safe tautology used with "project = HELP AND ...".
            return ("key is not EMPTY", [])
            
        raw_terms = _merge_jsm_customer_alias_terms(
            [customer_name] + list(match_terms or []),
        )
        seen: set[str] = set()
        cleaned_terms: list[str] = []
        escaped_terms: list[str] = []
        for term in raw_terms:
            cleaned = (term or "").strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            cleaned_terms.append(cleaned)
            escaped_terms.append(_jql_escape_string(cleaned))

        candidates = self._list_jsm_organization_names()
        # JQL `Organizations` must be an exact JSM directory string (not a nickname like "JCI" unless that label exists)
        jsm_name_by_lower = {c.lower(): c for c in candidates}
        resolved_orgs: list[str] = list(
            _fuzzy_pick_jsm_organizations(cleaned_terms, candidates) or []
        )
        if not resolved_orgs and organizations_only and cleaned_terms:
            from .jsm_org_llm import resolve_jsm_customer_organizations_llm

            llm_orgs = resolve_jsm_customer_organizations_llm(
                tenant_key=self._jsm_cache_key,
                customer_name=customer_name.strip(),
                all_terms=cleaned_terms,
                all_organizations=candidates,
                cache=self._jsm_llm_org_resolve_cache,
            )
            for o in llm_orgs or []:
                ex = jsm_name_by_lower.get((o or "").strip().lower()) or (o or "").strip()
                if not ex:
                    continue
                if not any(x.lower() == ex.lower() for x in resolved_orgs):
                    resolved_orgs.append(ex)

        org_fragments: list[str] = []
        seen_org: set[str] = set()
        for term in cleaned_terms:
            exact = jsm_name_by_lower.get(term.lower())
            if not exact:
                continue
            esc = _jql_escape_string(exact)
            frag = f'Organizations = "{esc}"'
            if frag not in seen_org:
                seen_org.add(frag)
                org_fragments.append(frag)
        for org in resolved_orgs:
            ex = jsm_name_by_lower.get(org.strip().lower(), org.strip())
            esc = _jql_escape_string(ex)
            frag = f'Organizations = "{esc}"'
            if frag not in seen_org:
                seen_org.add(frag)
                org_fragments.append(frag)

        text_fragments: list[str] = []
        for esc in escaped_terms:
            text_fragments.append(f'summary ~ "{esc}"')
            text_fragments.append(f'description ~ "{esc}"')

        if organizations_only:
            if not org_fragments:
                # No Organizations literals (should be rare) — JQL that matches no real issues.
                return ('summary ~ "___CORTEX_NO_ORG_MATCH___"', resolved_orgs)
            clauses = org_fragments
        else:
            clauses = org_fragments + text_fragments
        return "(" + " OR ".join(clauses) + ")", resolved_orgs

    def _help_project_customer_filter(
        self,
        customer_name: str | None,
        match_terms: list[str] | None = None,
    ) -> tuple[str, list[str]]:
        """HELP + customer scope: prefer JSM ``Organizations`` literals only (metrics-safe).

        If no directory label can be derived for the account (spelling differs from JSM,
        aliases missing, fuzzy/LLM empty), fall back to ``summary`` / ``description`` OR-clauses
        so single-customer support decks still populate instead of silently returning no issues.
        """
        org_only_clause, orgs = self._customer_match_clause(
            customer_name, match_terms, organizations_only=True
        )
        if (
            customer_name
            and isinstance(org_only_clause, str)
            and "___CORTEX_NO_ORG_MATCH___" in org_only_clause
        ):
            cust = (customer_name or "").strip()
            sf_hint = _salesforce_activity_hint_for_customer_scope(cust)
            if sf_hint:
                warn_msg = (
                    f"HELP scope: no JSM Organizations match for {cust!r}; using summary/description "
                    f"fallback. {sf_hint}"
                )
            else:
                warn_msg = (
                    f"HELP scope: no JSM Organizations match for {cust!r}; using summary/description "
                    "fallback"
                )
            logger.warning("%s", warn_msg)
            try:
                from .data_governance_warnings import record_data_governance_warning

                record_data_governance_warning(
                    "help_jsm_org_fallback",
                    warn_msg,
                    context={"customer_name": cust},
                )
            except Exception:
                pass
            return self._customer_match_clause(
                customer_name, match_terms, organizations_only=False
            )
        return org_only_clause, orgs

    def help_salesforce_entity_site_scoped_clause(
        self,
        entity_row: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """HELP filter: JSM ``Organizations`` AND site-ish ``summary``/``description`` tokens.

        Use for **per-entity** Salesforce exports where a broad org (e.g. ``Carrier``) would otherwise
        attribute every org ticket to each site row.

        Returns ``(jql_fragment, meta)`` where ``meta`` includes ``resolved_jsm_orgs``, ``site_terms``,
        and ``site_text_scoped`` (whether non-empty text narrowing was applied).
        """
        primary, extras = _salesforce_entity_customer_primary_and_extras(entity_row)
        org_clause, resolved_orgs = self._help_project_customer_filter(primary, extras)
        meta: dict[str, Any] = {
            "resolved_jsm_orgs": resolved_orgs,
            "site_terms": [],
            "site_text_scoped": False,
        }
        if "___CORTEX_NO_ORG_MATCH___" in org_clause:
            return org_clause, meta

        site_terms = _help_site_text_terms_from_salesforce_entity(entity_row)
        nm = (entity_row.get("Name") or "").strip()
        if not site_terms and len(nm) >= 8:
            site_terms = [nm]

        if not site_terms:
            meta["site_terms"] = []
            meta["site_text_scoped"] = False
            logger.debug(
                "HELP site scope: no summary/description terms for entity %r — org-only clause",
                nm[:80],
            )
            return org_clause, meta

        meta["site_terms"] = list(site_terms)
        meta["site_text_scoped"] = True
        text_clause = _jql_text_match_any(("summary", "description"), site_terms)
        combined = f"({org_clause}) AND {text_clause}"
        return combined, meta

    def _customer_project_text_match_clause(
        self,
        customer_name: str | None,
        match_terms: list[str] | None = None,
    ) -> tuple[str, list[str]]:
        """CUSTOMER and LEAN projects: match the account with ``summary`` / ``description`` JQL.

        The JSM **Organizations** field is not how these projects are tied to the customer; use
        :meth:`_help_project_customer_filter` for ``project = HELP`` only.
        """
        if not (customer_name or "").strip() and not (match_terms or []):
            return ("key is not EMPTY", [])

        raw: list[str] = []
        cn = (customer_name or "").strip()
        if cn:
            raw.append(cn)
        for t in match_terms or []:
            t = (t or "").strip()
            if t:
                raw.append(t)

        seen: set[str] = set()
        terms: list[str] = []
        for t in raw:
            k = t.lower()
            if k in seen:
                continue
            seen.add(k)
            terms.append(t)
        if not terms:
            return ("key is not EMPTY", [])

        parts: list[str] = []
        for t in terms:
            esc = _jql_escape_string(t)
            parts.append(f'(summary ~ "{esc}" OR description ~ "{esc}")')
        if len(parts) == 1:
            return (parts[0], [])
        return ("(" + " OR ".join(parts) + ")", [])

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
            "resolutiondate": (f.get("resolutiondate") or "")[:10],
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
        try:
            pk = _validate_project_key(project_key)
        except ValueError as e:
            return {"error": str(e), "project_key": (project_key or "").strip()}

        jql_start = self._jql_log_len()
        now = datetime.now(timezone.utc)
        max_fetch = 1500
        is_cl = pk in ("CUSTOMER", "LEAN")
        is_ex = f" AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION}" if is_cl else ""

        try:
            # Use Jira workflow state rather than `resolution is EMPTY` because some HELP
            # tickets remain unresolved while already in a done-like status, which inflates
            # the "open" count on portfolio slides.
            open_raw = self._search(
                f"project = {pk}{is_ex} AND statusCategory != Done ORDER BY updated DESC",
                max_results=max_fetch,
                fields=_PROJECT_SNAPSHOT_FIELDS,
                data_description=f"{pk} project open issues (statusCategory != Done)",
            )
        except Exception as e:
            logger.warning("Jira open fetch failed for %s: %s", pk, e)
            return {"error": str(e), "project_key": pk, "base_url": self.base_url}

        try:
            resolved_raw = self._search(
                f"project = {pk}{is_ex} AND resolution is not EMPTY AND resolved >= -180d "
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

        # Open-ticket aging buckets (backlog health: is old work piling up?).
        open_age_buckets = {"0-7d": 0, "8-30d": 0, "31-90d": 0, "90d+": 0}
        for age in open_ages_days:
            if age <= 7:
                open_age_buckets["0-7d"] += 1
            elif age <= 30:
                open_age_buckets["8-30d"] += 1
            elif age <= 90:
                open_age_buckets["31-90d"] += 1
            else:
                open_age_buckets["90d+"] += 1

        return {
            "project_key": pk,
            "base_url": self.base_url,
            "open_count": len(open_issues),
            "open_count_capped": len(open_issues) >= max_fetch,
            "by_status_open": by_status_sorted,
            "median_open_age_days": _median(open_ages_days),
            "avg_resolved_cycle_days": _avg(cycle_days),
            "resolved_in_6mo_count": len(resolved_issues),
            "resolved_in_6mo_capped": len(resolved_issues) >= max_fetch,
            "fetch_cap": max_fetch,
            "assignee_resolved_table": assignee_table,
            "open_age_buckets": open_age_buckets,
            "open_over_90_count": open_age_buckets["90d+"],
            "oldest_open_age_days": round(max(open_ages_days), 1) if open_ages_days else None,
            "jql_queries": self._jql_since(jql_start),
        }

    def get_customer_jira(self, customer_name: str, days: int = 90) -> dict[str, Any]:
        """Get JIRA picture for a customer: open issues, recent activity, escalations.

        Scoped to **project HELP** (support desk) only. All HELP issue lists and
        :func:`get_customer_ticket_metrics` prebuilt slice use the same JQL: JSM
        ``Organizations`` only (no ``summary`` / ``description`` text match) so
        per-customer counts are not inflated by other orgs' tickets.
        """
        jql_start = self._jql_log_len()
        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(customer_name)
        jql = f"project = HELP AND {base_filter} AND {_TRANSIENT_LABELS_EXCLUSION} AND created >= -{days}d ORDER BY created DESC"
        clause_bundle = (base_filter, resolved_jsm_orgs)

        def _fetch_help_body() -> tuple[list[dict], int | None]:
            cap = HELP_JIRA_BODY_MAX_RESULTS
            total_hint = self._jql_match_total(jql)
            fetch_n = cap
            if total_hint is not None and total_hint > 0:
                fetch_n = min(cap, total_hint)
            raw_inner = self._search(
                jql,
                max_results=max(1, fetch_n),
                data_description=f"HELP project issues for customer ({days}d lookback)",
            )
            return raw_inner, total_hint

        def _safe_metrics() -> dict[str, Any]:
            try:
                return self.get_customer_ticket_metrics(
                    customer_name, _prebuilt_clause=clause_bundle
                )
            except Exception as e:
                return {
                    "error": str(e),
                    "customer": customer_name,
                    "jsm_organizations_resolved": clause_bundle[1],
                }

        raw: list[dict] = []
        help_jql_total: int | None = None
        customer_ticket_metrics: dict[str, Any] = {}
        eng: dict[str, Any] = {}
        enhancements: dict[str, Any] = {}
        try:
            with ThreadPoolExecutor(max_workers=_JIRA_PARALLEL_WORKERS) as pool:
                f_body = pool.submit(_fetch_help_body)
                f_met = pool.submit(_safe_metrics)
                f_eng = pool.submit(self._get_engineering_tickets, customer_name)
                f_er = pool.submit(self._get_enhancement_requests, customer_name)
                customer_ticket_metrics = f_met.result()
                try:
                    eng = f_eng.result()
                except Exception as e:
                    eng = {"total": 0, "open": [], "recent_closed": [], "error": str(e)}
                try:
                    enhancements = f_er.result()
                except Exception as e:
                    enhancements = {"total": 0, "open": [], "shipped": [], "error": str(e)}
                try:
                    raw, help_jql_total = f_body.result()
                except Exception as e:
                    logger.warning("JIRA HELP body fetch failed for %s: %s", customer_name, e)
                    return {"error": str(e)}
        except Exception as e:
            logger.warning("JIRA parallel fetch failed for %s: %s", customer_name, e)
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

        ttfr = self._compute_sla(issues, "ttfr")
        ttr = self._compute_sla(issues, "ttr")

        self._run_qa_checks(issues, open_issues, resolved, by_status, by_priority, by_type, ttfr, ttr)

        if help_jql_total is not None and help_jql_total > len(issues):
            from .qa import qa
            qa.flag(
                "JIRA HELP slide body sampled: breakdowns omit older issues in window",
                expected=help_jql_total,
                actual=len(issues),
                sources=("JQL total vs fetched issues", "CORTEX_HELP_JIRA_BODY_MAX"),
                severity="warning",
            )

        return {
            "base_url": self.base_url,
            "customer": customer_name,
            "days": days,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "help_scope": (
                f"Jira project HELP only. All lists and ticket metrics: JSM Organizations for "
                f"{customer_name!r} (no summary/description text match)."
            ),
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

        def _inc(col: str, raw: str) -> None:
            if not raw:
                return
            try:
                dt = datetime.strptime(raw[:10], "%Y-%m-%d")
            except ValueError:
                return
            iso = dt.isocalendar()
            key = f"{iso[0]}-W{iso[1]:02d}"
            monday = dt - timedelta(days=dt.weekday())
            if key not in buckets:
                buckets[key] = {"week": key, "label": monday.strftime("%b %-d"), "created": 0, "resolved": 0}
            buckets[key][col] += 1

        for i in issues:
            _inc("created", i.get("created") or "")
            if not i.get("resolution"):
                continue
            resolved_raw = i.get("resolutiondate") or i.get("updated") or ""
            _inc("resolved", resolved_raw)

        return sorted(buckets.values(), key=lambda b: b["week"])

    @staticmethod
    def _flow_weekly_in_window(
        issues: list[dict[str, Any]],
        *,
        window_days: int,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Weekly opened/resolved counts for issues in the trailing *window_days* (last ~13 ISO weeks)."""
        ref = now or datetime.now(timezone.utc)
        cutoff = ref - timedelta(days=max(1, int(window_days)))

        def _created_in_window(issue: dict[str, Any]) -> bool:
            cre = JiraClient._parse_jira_datetime(issue.get("created"))
            return cre is not None and cre >= cutoff

        flow_issues = [
            issue
            for issue in issues
            if _created_in_window(issue)
            or (
                (rd := JiraClient._parse_jira_datetime(issue.get("resolutiondate"))) is not None
                and rd >= cutoff
            )
        ]
        return JiraClient._bucket_by_week(flow_issues)[-13:]

    def fetch_project_jira_escalated_flow_weekly(
        self,
        project: str,
        customer_name: str | None,
        match_terms: list[str] | None = None,
        *,
        window_days: int = 90,
    ) -> dict[str, Any]:
        """Weekly opened vs resolved for ``jira_escalated`` tickets in CUSTOMER or LEAN."""
        jql_start = self._jql_log_len()
        try:
            proj = _validate_project_key(project)
        except ValueError as e:
            return {"error": str(e), "project": project, "flow_weekly": []}
        if proj not in ("CUSTOMER", "LEAN"):
            return {
                "error": "fetch_project_jira_escalated_flow_weekly supports CUSTOMER and LEAN only",
                "project": proj,
                "flow_weekly": [],
            }

        base_filter, _resolved_orgs = self._customer_project_text_match_clause(
            customer_name, match_terms
        )
        jql = (
            f"project = {proj} AND labels = \"{JIRA_ESCALATED_LABEL}\" AND {base_filter} "
            f"AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION} AND ("
            "statusCategory != Done OR "
            "(resolution is not EMPTY AND resolved >= -365d) OR "
            "created >= -365d"
            ") ORDER BY updated DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=HELP_TRENDS_MAX_RESULTS,
                fields=_TREND_FIELDS,
                data_description=(
                    f"{proj} jira_escalated volume ({int(window_days)}d window; excl. Epic, SUT)"
                ),
            )
        except Exception as e:
            logger.warning("%s jira_escalated flow fetch failed: %s", proj, e)
            return {
                "error": str(e),
                "project": proj,
                "flow_weekly": [],
                "jql_queries": self._jql_since(jql_start),
            }

        issues: list[dict[str, Any]] = []
        for issue in raw:
            f = issue.get("fields") or {}
            labels = f.get("labels") or []
            if JIRA_ESCALATED_LABEL not in labels:
                continue
            resolution = f.get("resolution")
            res_name = (
                resolution.get("name", "")
                if isinstance(resolution, dict)
                else (resolution or "")
            )
            issues.append(
                {
                    "created": (f.get("created") or "")[:10],
                    "updated": (f.get("updated") or "")[:10],
                    "resolutiondate": (f.get("resolutiondate") or "")[:10],
                    "resolution": res_name,
                }
            )

        return {
            "project": proj,
            "flow_weekly": self._flow_weekly_in_window(issues, window_days=window_days),
            "jql_queries": self._jql_since(jql_start),
        }

    def fetch_project_jira_escalated_open_backlog(
        self,
        project: str,
        customer_name: str | None,
        match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """Open ``jira_escalated`` backlog for CUSTOMER or LEAN (age buckets + status stacks)."""
        jql_start = self._jql_log_len()
        try:
            proj = _validate_project_key(project)
        except ValueError as e:
            return {"error": str(e), "project": project}
        if proj not in ("CUSTOMER", "LEAN"):
            return {
                "error": "fetch_project_jira_escalated_open_backlog supports CUSTOMER and LEAN only",
                "project": proj,
            }

        base_filter, _resolved_orgs = self._customer_project_text_match_clause(
            customer_name, match_terms
        )
        jql = (
            f"project = {proj} AND labels = \"{JIRA_ESCALATED_LABEL}\" AND {base_filter} "
            f"AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION} AND statusCategory != Done "
            f"ORDER BY created ASC"
        )
        try:
            raw = self._search(
                jql,
                max_results=HELP_TRENDS_MAX_RESULTS,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description=f"{proj} open jira_escalated backlog (excl. Epic, SUT)",
            )
        except Exception as e:
            logger.warning("%s jira_escalated open backlog fetch failed: %s", proj, e)
            return {
                "error": str(e),
                "project": proj,
                "jql_queries": self._jql_since(jql_start),
            }

        open_issues: list[dict] = []
        for issue in raw:
            f = issue.get("fields") or {}
            labels = f.get("labels") or []
            if JIRA_ESCALATED_LABEL not in labels:
                continue
            open_issues.append(self._normalize_issue(issue))

        buckets, stacked = self._backlog_age_breakdown(open_issues)
        return {
            "project": proj,
            "open_count": len(open_issues),
            "backlog_age_buckets": buckets,
            "backlog_age_stacked": stacked,
            "jql_queries": self._jql_since(jql_start),
        }

    @staticmethod
    def _compute_sla(
        issues: list[dict],
        prefix: str,
        *,
        project_key: str = "HELP",
    ) -> dict[str, Any]:
        """Compute SLA statistics (TTFR or TTR) from JSM SLA data."""
        import statistics

        scoped = [i for i in issues if i.get("project") == project_key]
        values = [i[f"{prefix}_ms"] for i in scoped if i.get(f"{prefix}_ms") is not None]
        breached = sum(1 for i in scoped if i.get(f"{prefix}_breached"))
        waiting = sum(1 for i in scoped if i.get(f"{prefix}_waiting"))

        if not values:
            return {"tickets": len(scoped), "measured": 0, "waiting": waiting}

        values.sort()
        avg_ms = sum(values) / len(values)
        med_ms = statistics.median(values)

        def _fmt(ms: int) -> str:
            mins = ms / 60_000
            if mins < 60:
                return f"{mins:.0f} min"
            hrs = mins / 60
            if hrs < 24:
                return f"{hrs:.1f}h"
            return f"{hrs / 24:.1f}d"

        return {
            "tickets": len(scoped),
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
    def _compute_calendar_ttr(issues: list[dict]) -> dict[str, Any]:
        """Compute calendar TTR from issue created -> resolutiondate timestamps."""
        import statistics

        help_issues = [i for i in issues if i.get("project") == "HELP"]
        values: list[int] = []
        invalid = 0
        for issue in help_issues:
            created_dt = JiraClient._parse_jira_datetime(issue.get("created"))
            resolved_dt = JiraClient._parse_jira_datetime(issue.get("resolutiondate"))
            if created_dt is None or resolved_dt is None:
                continue
            elapsed_ms = int((resolved_dt - created_dt).total_seconds() * 1000)
            if elapsed_ms < 0:
                invalid += 1
                continue
            values.append(elapsed_ms)

        if not values:
            return {"tickets": len(help_issues), "measured": 0, "waiting": 0, "source": "calendar"}

        values.sort()
        avg_ms = sum(values) / len(values)
        med_ms = statistics.median(values)

        def _fmt(ms: int) -> str:
            mins = ms / 60_000
            if mins < 60:
                return f"{mins:.0f} min"
            hrs = mins / 60
            if hrs < 24:
                return f"{hrs:.1f}h"
            return f"{hrs / 24:.1f}d"

        return {
            "tickets": len(help_issues),
            "measured": len(values),
            "waiting": 0,
            "avg_ms": int(avg_ms),
            "median_ms": med_ms,
            "min_ms": values[0],
            "max_ms": values[-1],
            "avg": _fmt(int(avg_ms)),
            "median": _fmt(med_ms),
            "min": _fmt(values[0]),
            "max": _fmt(values[-1]),
            "invalid": invalid,
            "source": "calendar",
        }

    @staticmethod
    def _compute_backlog_age(
        issues: list[dict],
        *,
        project_key: str = "HELP",
    ) -> dict[str, Any]:
        """Compute open-ticket backlog age from created -> now (NOT DONE, scoped by project)."""
        import statistics

        help_issues = [i for i in issues if i.get("project") == project_key]
        now = datetime.now(timezone.utc)
        values: list[int] = []
        invalid = 0
        for issue in help_issues:
            created_dt = JiraClient._parse_jira_datetime(issue.get("created"))
            if created_dt is None:
                continue
            elapsed_ms = int((now - created_dt).total_seconds() * 1000)
            if elapsed_ms < 0:
                invalid += 1
                continue
            values.append(elapsed_ms)

        if not values:
            return {"tickets": len(help_issues), "measured": 0, "waiting": 0, "source": "open_backlog_age"}

        values.sort()
        avg_ms = sum(values) / len(values)
        med_ms = statistics.median(values)

        def _fmt(ms: int) -> str:
            mins = ms / 60_000
            if mins < 60:
                return f"{mins:.0f} min"
            hrs = mins / 60
            if hrs < 24:
                return f"{hrs:.1f}h"
            return f"{hrs / 24:.1f}d"

        return {
            "tickets": len(help_issues),
            "measured": len(values),
            "waiting": 0,
            "avg_ms": int(avg_ms),
            "median_ms": med_ms,
            "min_ms": values[0],
            "max_ms": values[-1],
            "avg": _fmt(int(avg_ms)),
            "median": _fmt(med_ms),
            "min": _fmt(values[0]),
            "max": _fmt(values[-1]),
            "invalid": invalid,
            "source": "open_backlog_age",
        }

    @staticmethod
    def _compute_sla_adherence(
        issues: list[dict],
        *,
        project_key: str = "HELP",
    ) -> dict[str, Any]:
        """Percent of project tickets that met every measured SLA on the issue."""
        help_issues = [i for i in issues if i.get("project") == project_key]
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

    @staticmethod
    def _compute_ttr_sla_adherence_pct(
        issues: list[dict],
        *,
        project_key: str = "HELP",
    ) -> dict[str, Any]:
        """Percent of tickets with completed **Time to resolution** SLA that did not breach.

        Aligns with LeanDNA metric *TTR % (Trailing 30 Days)* / *Support Time to Resolution*:
        only ``ttr_ms`` / ``ttr_breached`` count; TTFR is ignored. Tickets without a completed
        TTR SLA cycle are excluded from ``measured``.
        """
        scoped = [i for i in issues if i.get("project") == project_key]
        measured = 0
        met = 0
        waiting = 0
        for issue in scoped:
            if issue.get("ttr_waiting"):
                waiting += 1
            if issue.get("ttr_ms") is None:
                continue
            measured += 1
            if not issue.get("ttr_breached"):
                met += 1
        breached = measured - met
        pct = round(100 * met / measured, 1) if measured else None
        return {
            "tickets": len(scoped),
            "measured": measured,
            "met": met,
            "breached": breached,
            "waiting": waiting,
            "pct": pct,
        }

    def _partition_help_metrics_merged(
        self,
        merged_raw: list[dict],
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        """Split merged HELP union query into open / resolved-180d / created-365d / resolved-365d."""
        now = datetime.now(timezone.utc)
        cutoff_res_180 = now - timedelta(days=180)
        cutoff_res_365 = now - timedelta(days=365)
        cutoff_cre = now - timedelta(days=365)
        open_raw: list[dict] = []
        resolved_raw: list[dict] = []
        year_raw: list[dict] = []
        resolved_year_raw: list[dict] = []
        for raw in merged_raw:
            f = raw.get("fields", {}) or {}
            status = f.get("status") or {}
            cat = ((status.get("statusCategory") or {}).get("key") or "").lower()
            is_done = cat == "done"
            res_dt = self._parse_jira_datetime(f.get("resolutiondate"))
            cre_dt = self._parse_jira_datetime(f.get("created"))
            if not is_done:
                open_raw.append(raw)
            if f.get("resolution") and res_dt is not None and res_dt >= cutoff_res_180:
                resolved_raw.append(raw)
            if f.get("resolution") and res_dt is not None and res_dt >= cutoff_res_365:
                resolved_year_raw.append(raw)
            if cre_dt is not None and cre_dt >= cutoff_cre:
                year_raw.append(raw)
        return open_raw, resolved_raw, year_raw, resolved_year_raw

    def get_customer_ticket_metrics(
        self,
        customer_name: str | None,
        match_terms: list[str] | None = None,
        *,
        _prebuilt_clause: tuple[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """Metrics for a single customer's support tickets across open/6mo/1y windows.

        Scoped to ``project = HELP`` only. The customer clause is **Organizations only**
        (literal and fuzzy JSM names) — **no** ``summary``/``description`` text match, so KPIs
        and pie charts match the JSM org, not every issue mentioning the account name in text.

        The **JQL** is a single **merged** query, not "open" alone:

        ``(statusCategory != Done OR (resolved in last 365d) OR (created in last 365d))``

        so one fetch can back TTFR, SLA, and backlog slices. Open/unresolved is partitioned
        client-side from that set (``statusCategory != Done``). A query that is only
        ``... AND statusCategory != Done`` would **omit** resolved/created-in-window issues
        and break 1y metrics.

        Pass ``_prebuilt_clause`` from ``get_customer_jira`` (same org-only fragment as the
        HELP body fetch) to skip a second org-resolution pass.
        """
        jql_start = self._jql_log_len()
        if _prebuilt_clause is not None:
            base_filter, resolved_jsm_orgs = _prebuilt_clause
        else:
            base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
                customer_name, match_terms
            )
        max_fetch = HELP_METRICS_MERGED_MAX_RESULTS
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

            raw_lbls = f.get("labels")
            if isinstance(raw_lbls, (list, tuple)):
                label_list = [str(x) for x in raw_lbls if x is not None]
            else:
                label_list = []

            return {
                "status": (f.get("status") or {}).get("name", "Unknown"),
                "type": (f.get("issuetype") or {}).get("name", "Unknown"),
                "project": (f.get("project") or {}).get("key", ""),
                "created": f.get("created") or "",
                "updated": f.get("updated") or "",
                "resolutiondate": f.get("resolutiondate") or "",
                "labels": label_list,
                "ttfr_ms": ttfr_ms,
                "ttfr_breached": ttfr_breached,
                "ttfr_waiting": ttfr_waiting,
                "ttr_ms": ttr_ms,
                "ttr_breached": ttr_breached,
                "ttr_waiting": ttr_waiting,
            }

        union_jql = (
            f"{proj}{base_filter} AND {_TRANSIENT_LABELS_EXCLUSION} AND ("
            "statusCategory != Done OR "
            "(resolution is not EMPTY AND resolved >= -365d) OR "
            "created >= -365d"
            ") ORDER BY updated DESC"
        )
        try:
            merged_raw = self._search(
                union_jql,
                max_results=max_fetch,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description="HELP customer metrics (merged open / 365d resolved / 365d created)",
            )
        except Exception as e:
            logger.warning("Customer ticket metrics fetch failed for %s: %s", customer_name, e)
            return {
                "error": str(e),
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
            }

        open_raw, resolved_raw, year_raw, resolved_year_raw = self._partition_help_metrics_merged(merged_raw)

        open_issues = [_norm_snapshot_issue(i) for i in open_raw]
        resolved_issues = [_norm_snapshot_issue(i) for i in resolved_raw]
        year_issues = [_norm_snapshot_issue(i) for i in year_raw]
        resolved_year_issues = [_norm_snapshot_issue(i) for i in resolved_year_raw]

        by_type_open = dict(sorted(Counter(i["type"] for i in open_issues).items(), key=lambda x: (-x[1], x[0])))
        by_status_open = dict(sorted(Counter(i["status"] for i in open_issues).items(), key=lambda x: (-x[1], x[0])))
        ttfr = self._compute_sla(year_issues, "ttfr")
        # For support deck, TTR is defined as age of open NOT DONE backlog tickets.
        ttr = self._compute_backlog_age(open_issues)
        sla_adherence = self._compute_sla_adherence(year_issues)

        return {
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "unresolved_count": len(open_issues),
            "resolved_in_6mo_count": len(resolved_issues),
            "ttfr_1y": ttfr,
            "ttr_1y": ttr,
            "sla_adherence_1y": sla_adherence,
            "by_type_open": by_type_open,
            "by_status_open": by_status_open,
            "jql_queries": self._jql_since(jql_start),
        }

    def get_project_ticket_metrics(
        self,
        project: str,
        customer_name: str | None,
        match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """KPI metrics for CUSTOMER or LEAN, mirroring ``get_customer_ticket_metrics`` for HELP.

        Customer scoping is ``summary`` / ``description`` text (not JSM ``Organizations``); use
        ``get_customer_ticket_metrics`` for the HELP project.

        Excludes Jira issue types **Epic** and **SUT** from the merged JQL (same as other
        CUSTOMER/LEAN support slides).
        """
        jql_start = self._jql_log_len()
        try:
            proj = _validate_project_key(project)
        except ValueError as e:
            return {
                "error": str(e),
                "project": (project or "").strip().upper(),
                "customer": customer_name,
            }
        if proj not in ("CUSTOMER", "LEAN"):
            return {
                "error": "get_project_ticket_metrics supports CUSTOMER and LEAN only",
                "project": proj,
                "customer": customer_name,
            }

        base_filter, resolved_jsm_orgs = self._customer_project_text_match_clause(
            customer_name, match_terms
        )

        max_fetch = HELP_METRICS_MERGED_MAX_RESULTS
        proj_prefix = f"project = {proj} AND "

        def _norm_snapshot_issue(issue: dict) -> dict[str, Any]:
            f = issue.get("fields", {}) or {}

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

        union_jql = (
            f"{proj_prefix}{base_filter} AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION} AND ("
            "statusCategory != Done OR "
            "(resolution is not EMPTY AND resolved >= -365d) OR "
            "created >= -365d"
            ") ORDER BY updated DESC"
        )
        try:
            merged_raw = self._search(
                union_jql,
                max_results=max_fetch,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description=f"{proj} project ticket metrics (merged open / 365d resolved / 365d created; excl. Epic, SUT)",
            )
        except Exception as e:
            logger.warning("Project ticket metrics fetch failed for %s %s: %s", proj, customer_name, e)
            return {
                "error": str(e),
                "project": proj,
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
            }

        open_raw, resolved_raw, year_raw, resolved_year_raw = self._partition_help_metrics_merged(merged_raw)
        open_issues = [_norm_snapshot_issue(i) for i in open_raw]
        resolved_issues = [_norm_snapshot_issue(i) for i in resolved_raw]
        year_issues = [_norm_snapshot_issue(i) for i in year_raw]
        by_type_open = dict(sorted(Counter(i["type"] for i in open_issues).items(), key=lambda x: (-x[1], x[0])))
        by_status_open = dict(sorted(Counter(i["status"] for i in open_issues).items(), key=lambda x: (-x[1], x[0])))
        ttfr = self._compute_sla(year_issues, "ttfr", project_key=proj)
        ttr = self._compute_backlog_age(open_issues, project_key=proj)
        sla_adherence = self._compute_sla_adherence(year_issues, project_key=proj)

        return {
            "project": proj,
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "unresolved_count": len(open_issues),
            "resolved_in_6mo_count": len(resolved_issues),
            "ttfr_1y": ttfr,
            "ttr_1y": ttr,
            "sla_adherence_1y": sla_adherence,
            "by_type_open": by_type_open,
            "by_status_open": by_status_open,
            "jql_queries": self._jql_since(jql_start),
        }

    def get_project_ticket_volume_trends(
        self,
        project: str,
        customer_name: str | None,
        match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """12-month created vs resolved trends for CUSTOMER or LEAN (all / escalated / non-escalated).

        Scoped with ``summary`` / ``description`` (see :meth:`_customer_project_text_match_clause`).

        Excludes **Epic** and **SUT** issue types.
        """
        jql_start = self._jql_log_len()
        try:
            proj = _validate_project_key(project)
        except ValueError as e:
            return {
                "error": str(e),
                "all": [],
                "escalated": [],
                "non_escalated": [],
            }
        if proj not in ("CUSTOMER", "LEAN"):
            return {
                "error": "get_project_ticket_volume_trends supports CUSTOMER and LEAN only",
                "all": [],
                "escalated": [],
                "non_escalated": [],
            }
        base, _ = self._customer_project_text_match_clause(customer_name, match_terms)
        jql = (
            f"project = {proj} AND {base} AND {_CUSTOMER_LEAN_ISSUETYPE_EXCLUSION} "
            "AND (created >= -365d OR resolved >= -365d) "
            "ORDER BY created DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=HELP_TRENDS_MAX_RESULTS,
                fields=_TREND_FIELDS,
                data_description=f"{proj} volume trends (12-month; excl. Epic, SUT)",
            )
        except Exception as e:
            logger.warning("%s ticket trend fetch failed: %s", proj, e)
            return {
                "error": str(e),
                "project": proj,
                "customer": customer_name,
                "all": [],
                "escalated": [],
                "non_escalated": [],
                "jql_queries": self._jql_since(jql_start),
            }
        issues = []
        for issue in raw:
            f = issue.get("fields", {}) or {}
            issues.append(
                {
                    "created": f.get("created") or "",
                    "resolutiondate": f.get("resolutiondate") or "",
                    "labels": f.get("labels") or [],
                }
            )
        return {
            "project": proj,
            "customer": customer_name,
            "all": self._bucket_by_month(issues, escalated_only=False),
            "escalated": self._bucket_by_month(issues, escalated_only=True),
            "non_escalated": self._bucket_by_month(issues, exclude_escalated=True),
            "jql_queries": self._jql_since(jql_start),
        }

    def get_customer_help_recent_tickets(
        self,
        customer_name: str,
        match_terms: list[str] | None = None,
        *,
        opened_within_days: int | None = 45,
        closed_within_days: int | None = 45,
        max_each: int = 100,
    ) -> dict[str, Any]:
        """Recent HELP issues for one customer: opened in window vs resolved in window.

        Same JSM ``Organizations``-only scoping as :func:`get_customer_ticket_metrics`.

        ``opened_within_days`` / ``closed_within_days``: pass ``None`` to omit a date
        clause in JQL and order by most recent created/resolved (still capped at *max_each*).
        """
        return self.get_customer_project_recent_tickets(
            "HELP",
            customer_name,
            match_terms,
            opened_within_days=opened_within_days,
            closed_within_days=closed_within_days,
            max_each=max_each,
        )
    
    def get_customer_project_recent_tickets(
        self,
        project: str,
        customer_name: str,
        match_terms: list[str] | None = None,
        *,
        opened_within_days: int | None = 45,
        closed_within_days: int | None = 45,
        max_each: int = 100,
    ) -> dict[str, Any]:
        """Recent tickets for any project for one customer: opened in window vs resolved in window.

        For HELP project, uses JSM ``Organizations`` only (no summary/description text).
        For other projects (CUSTOMER, LEAN), uses text match only.

        ``opened_within_days`` / ``closed_within_days``: pass ``None`` to omit the
        corresponding ``>= -Nd`` filter and return the *max_each* most recent
        by created/resolved.
        """
        jql_start = self._jql_log_len()
        
        # If customer_name is None on non-HELP projects, scope to all project tickets.
        if project == "HELP":
            base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
                customer_name, match_terms
            )
        else:
            base_filter, resolved_jsm_orgs = self._customer_project_text_match_clause(
                customer_name, match_terms
            )
        
        proj = f"project = {project} AND "

        def _row(issue: dict) -> dict[str, Any]:
            f = issue.get("fields", {}) or {}
            cr = self._parse_jira_datetime(f.get("created"))
            rs = self._parse_jira_datetime(f.get("resolutiondate"))
            st = f.get("status") or {}
            status_name = st.get("name", "—") if isinstance(st, dict) else "—"
            pr = f.get("priority") or {}
            priority_name = pr.get("name", "—") if isinstance(pr, dict) else "—"
            orgs = f.get(ORG_FIELD) or []
            org_names = [o.get("name", "") for o in orgs if isinstance(o, dict) and o.get("name")]
            return {
                "key": issue.get("key", ""),
                "summary": (f.get("summary") or "").strip(),
                "organization": ", ".join(org_names) if org_names else "—",
                "status": status_name,
                "priority": priority_name,
                "created": f.get("created") or "",
                "created_short": cr.strftime("%Y-%m-%d") if cr else "—",
                "resolved": f.get("resolutiondate") or "",
                "resolved_short": rs.strftime("%Y-%m-%d") if rs else "—",
            }

        od: int | None
        if opened_within_days is not None and int(opened_within_days) > 0:
            od = int(opened_within_days)
        else:
            od = None
        cd: int | None
        if closed_within_days is not None and int(closed_within_days) > 0:
            cd = int(closed_within_days)
        else:
            cd = None
        # Only apply transient label exclusion for HELP project (Outage/Healthcheck are HELP-specific).
        label_filter = f" AND {_TRANSIENT_LABELS_EXCLUSION}" if project == "HELP" else ""
        cl_iss = _jql_customer_lean_exclude_epic_sut(project)
        try:
            if od is not None:
                open_jql = f"{proj}{base_filter}{label_filter}{cl_iss} AND created >= -{od}d ORDER BY created DESC"
                open_desc = f"{project} customer tickets created in last {od} days"
            else:
                open_jql = f"{proj}{base_filter}{label_filter}{cl_iss} ORDER BY created DESC"
                open_desc = f"{project} customer tickets, most recent by created"
            if cd is not None:
                closed_jql = (
                    f"{proj}{base_filter}{label_filter}{cl_iss} AND resolution is not EMPTY AND resolved >= -{cd}d "
                    "ORDER BY resolved DESC"
                )
                closed_desc = f"{project} customer tickets resolved in last {cd} days"
            else:
                closed_jql = (
                    f"{proj}{base_filter}{label_filter}{cl_iss} AND resolution is not EMPTY "
                    "ORDER BY resolved DESC"
                )
                closed_desc = f"{project} customer tickets, most recent by resolution"
            with ThreadPoolExecutor(max_workers=2) as pool:
                f_open = pool.submit(
                    self._search,
                    open_jql,
                    max_each,
                    _CUSTOMER_TICKET_SLIDE_FIELDS,
                    data_description=open_desc,
                )
                f_closed = pool.submit(
                    self._search,
                    closed_jql,
                    max_each,
                    _CUSTOMER_TICKET_SLIDE_FIELDS,
                    data_description=closed_desc,
                )
                raw_open = f_open.result()
                raw_closed = f_closed.result()
        except Exception as e:
            logger.warning("Customer %s recent tickets fetch failed for %s: %s", project, customer_name, e)
            return {
                "error": str(e),
                "project": project,
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "opened_within_days": od,
                "closed_within_days": cd,
                "recently_opened": [],
                "recently_closed": [],
                "jql_queries": self._jql_since(jql_start),
            }

        return {
            "project": project,
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "opened_within_days": od,
            "closed_within_days": cd,
            "recently_opened": [_row(i) for i in raw_open],
            "recently_closed": [_row(i) for i in raw_closed],
            "jql_queries": self._jql_since(jql_start),
        }

    def get_customer_project_open_breakdown(
        self,
        project: str,
        customer_name: str | None,
        match_terms: list[str] | None = None,
        *,
        max_results: int = 1000,
    ) -> dict[str, Any]:
        """Open-ticket status/type breakdown for a project/customer scope."""
        jql_start = self._jql_log_len()
        try:
            proj = _validate_project_key(project)
        except ValueError as e:
            return {"error": str(e), "project": (project or "").strip().upper(), "customer": customer_name}

        if proj == "HELP":
            base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
                customer_name, match_terms
            )
        else:
            base_filter, resolved_jsm_orgs = self._customer_project_text_match_clause(
                customer_name, match_terms
            )

        ex_cl = _jql_customer_lean_exclude_epic_sut(proj)
        jql = (
            f"project = {proj} AND {base_filter}{ex_cl} AND statusCategory != Done "
            "ORDER BY updated DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=max_results,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description=f"{proj} customer open-ticket breakdown (excl. Epic, SUT)" if ex_cl else f"{proj} customer open-ticket breakdown",
            )
        except Exception as e:
            logger.warning("Customer %s open breakdown fetch failed for %s: %s", proj, customer_name, e)
            return {
                "error": str(e),
                "project": proj,
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "jql_queries": self._jql_since(jql_start),
            }

        open_rows: list[dict[str, str]] = []
        for issue in raw:
            f = issue.get("fields", {}) or {}
            status = (f.get("status") or {}).get("name", "Unknown")
            issue_type = (f.get("issuetype") or {}).get("name", "Unknown")
            open_rows.append({"status": status, "type": issue_type})

        by_type_open = dict(sorted(Counter(r["type"] for r in open_rows).items(), key=lambda x: (-x[1], x[0])))
        by_status_open = dict(sorted(Counter(r["status"] for r in open_rows).items(), key=lambda x: (-x[1], x[0])))
        return {
            "project": proj,
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "unresolved_count": len(open_rows),
            "by_type_open": by_type_open,
            "by_status_open": by_status_open,
            "jql_queries": self._jql_since(jql_start),
        }
    
    def get_resolved_tickets_by_assignee(
        self,
        project: str,
        customer_name: str | None,
        match_terms: list[str] | None = None,
        *,
        days: int = 90,
        max_results: int = 500,
    ) -> dict[str, Any]:
        """Get resolved tickets grouped by assignee for a project and customer.
        
        Args:
            project: Jira project key (e.g., "HELP", "CUSTOMER")
            customer_name: Customer name to filter by
            match_terms: Additional match terms for the customer
            days: Number of days to look back for resolved tickets
            max_results: Maximum tickets to fetch
        
        Returns:
            Dict with assignee counts sorted by count descending.
        """
        jql_start = self._jql_log_len()
        
        # If customer_name is None on non-HELP projects, scope to all project tickets.
        if project == "HELP":
            base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
                customer_name, match_terms
            )
        else:
            base_filter, resolved_jsm_orgs = self._customer_project_text_match_clause(
                customer_name, match_terms
            )
        
        # Only apply transient label exclusion for HELP project.
        label_filter = f" AND {_TRANSIENT_LABELS_EXCLUSION}" if project == "HELP" else ""
        ex_cl = _jql_customer_lean_exclude_epic_sut(project)
        jql = (
            f"project = {project} AND {base_filter}{label_filter}{ex_cl} AND resolution is not EMPTY "
            f"AND resolved >= -{days}d ORDER BY resolved DESC"
        )
        
        try:
            raw = self._search(
                jql,
                max_results=max_results,
                fields=["assignee", "resolutiondate"],
                data_description=(
                    f"{project} resolved by assignee (last {days}d; excl. Epic, SUT)"
                    if ex_cl
                    else f"{project} resolved tickets by assignee (last {days}d)"
                ),
            )
        except Exception as e:
            logger.warning("Resolved tickets by assignee fetch failed for %s %s: %s", project, customer_name, e)
            return {
                "error": str(e),
                "project": project,
                "customer": customer_name,
                "days": days,
                "by_assignee": [],
                "total_resolved": 0,
                "jql_queries": self._jql_since(jql_start),
            }
        
        # Group by assignee
        assignee_counts: dict[str, int] = {}
        for issue in raw:
            f = issue.get("fields", {}) or {}
            assignee = f.get("assignee") or {}
            if isinstance(assignee, dict):
                name = assignee.get("displayName") or assignee.get("name") or "Unassigned"
            else:
                name = "Unassigned"
            assignee_counts[name] = assignee_counts.get(name, 0) + 1
        
        # Sort by count descending
        sorted_assignees = sorted(assignee_counts.items(), key=lambda x: (-x[1], x[0]))
        
        return {
            "project": project,
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "days": days,
            "total_resolved": len(raw),
            "by_assignee": [{"assignee": name, "count": count} for name, count in sorted_assignees],
            "jql_queries": self._jql_since(jql_start),
        }

    def get_help_time_to_resolution(
        self,
        *,
        days: int = 30,
        customer_name: str | None = None,
        match_terms: list[str] | None = None,
        max_results: int | None = None,
        include_tickets: bool = False,
    ) -> dict[str, Any]:
        """HELP **TTR SLA adherence %** for tickets resolved in the last *days* (LeanDNA metric 1911-style).

        JQL: ``project = HELP``, customer scope (all customers when ``customer_name`` is None),
        transient label exclusion (Outage, Healthcheck), ``resolution is not EMPTY``, and
        ``resolved >= -{days}d``.

        Primary aggregate: :meth:`_compute_ttr_sla_adherence_pct` — among issues with a completed
        **Time to resolution** SLA (``customfield_10665``), the percent where ``breached`` is false.
        Tickets resolved in the window but without a completed TTR SLA are in
        ``resolved_in_window`` but not in ``ttr_sla_adherence.measured``.

        Returns ``ttr_sla_adherence`` (``pct``, ``met``, ``measured``, ``breached``) plus optional
        per-issue rows.
        """
        jql_start = self._jql_log_len()
        if days < 1:
            return {"error": "days must be >= 1", "days": days, "project": "HELP"}

        cap = max_results if max_results is not None else HELP_TTR_RESOLVED_MAX_RESULTS
        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
            customer_name, match_terms
        )
        jql = (
            f"project = HELP AND {base_filter} AND {_TRANSIENT_LABELS_EXCLUSION} "
            f"AND resolution is not EMPTY AND resolved >= -{int(days)}d "
            "ORDER BY resolved DESC"
        )
        jql_total = self._jql_match_total(jql)

        try:
            raw = self._search(
                jql,
                max_results=cap,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description=(
                    f"HELP Time to resolution SLA (resolved in last {int(days)}d"
                    + (f", customer {customer_name!r}" if customer_name else ", portfolio")
                    + ")"
                ),
            )
        except Exception as e:
            logger.warning(
                "HELP time-to-resolution fetch failed (days=%s customer=%r): %s",
                days,
                customer_name,
                e,
            )
            return {
                "error": str(e),
                "project": "HELP",
                "metric": "time_to_resolution",
                "window_days": int(days),
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "jql_queries": self._jql_since(jql_start),
            }

        issues = [self._normalize_issue(i) for i in raw]
        adherence = self._compute_ttr_sla_adherence_pct(issues, project_key="HELP")

        out: dict[str, Any] = {
            "project": "HELP",
            "metric": "ttr_sla_adherence_pct",
            "definition": (
                "Percent of resolved HELP tickets (trailing window) with Time to resolution "
                "SLA completed and not breached"
            ),
            "sla_field": TTR_FIELD,
            "window_days": int(days),
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "resolved_in_window": len(issues),
            "jql_total": jql_total,
            "fetch_cap": cap,
            "truncated": jql_total is not None and jql_total > len(issues),
            "ttr_sla_adherence": adherence,
            "jql_queries": self._jql_since(jql_start),
        }
        if include_tickets:
            slim: list[dict[str, Any]] = []
            for issue, norm in zip(raw, issues):
                rd = (issue.get("fields") or {}).get("resolutiondate") or ""
                has_ttr = norm.get("ttr_ms") is not None
                slim.append(
                    {
                        "key": norm["key"],
                        "summary": (norm.get("summary") or "")[:120],
                        "status": norm.get("status"),
                        "resolution": norm.get("resolution"),
                        "resolutiondate": rd[:10] if isinstance(rd, str) and rd else "",
                        "organizations": norm.get("organizations") or [],
                        "ttr_sla_measured": has_ttr,
                        "ttr_sla_met": has_ttr and not norm.get("ttr_breached"),
                        "ttr_breached": norm.get("ttr_breached"),
                        "ttr_waiting": norm.get("ttr_waiting"),
                    }
                )
            out["tickets"] = slim
        return out

    @staticmethod
    def _compute_sla_field_adherence_pct(
        issues: list[dict],
        prefix: str,
        *,
        project_key: str = "HELP",
    ) -> dict[str, Any]:
        """Percent of scoped tickets with completed *prefix* SLA (ttfr/ttr) that did not breach."""
        scoped = [i for i in issues if i.get("project") == project_key]
        measured = 0
        met = 0
        waiting = 0
        for issue in scoped:
            if issue.get(f"{prefix}_waiting"):
                waiting += 1
            if issue.get(f"{prefix}_ms") is None:
                continue
            measured += 1
            if not issue.get(f"{prefix}_breached"):
                met += 1
        breached = measured - met
        pct = round(100 * met / measured, 1) if measured else None
        return {
            "tickets": len(scoped),
            "measured": measured,
            "met": met,
            "breached": breached,
            "waiting": waiting,
            "pct": pct,
        }

    @staticmethod
    def _open_age_days(issue: dict) -> float | None:
        created_dt = JiraClient._parse_jira_datetime(issue.get("created"))
        if created_dt is None:
            return None
        return (datetime.now(timezone.utc) - created_dt).total_seconds() / 86400.0

    _BACKLOG_AGE_BUCKET_KEYS = ("0-7", "8-14", "15-30", "30+")

    @staticmethod
    def _backlog_age_bucket_key(age_days: float) -> str:
        if age_days <= 7:
            return "0-7"
        if age_days <= 14:
            return "8-14"
        if age_days <= 30:
            return "15-30"
        return "30+"

    @staticmethod
    def _backlog_bottleneck(status: str) -> str:
        """Classify open-ticket status for backlog stack: support vs customer vs engineering."""
        s = (status or "").strip().lower()
        if "customer" in s and ("waiting" in s or "awaiting" in s):
            return "waiting_on_customer"
        if "engineering" in s:
            return "waiting_on_engineering"
        return "with_support"

    @staticmethod
    def _backlog_age_breakdown(open_issues: list[dict]) -> tuple[dict[str, int], dict[str, Any]]:
        """Open tickets by age bucket, with counts stacked by who's holding progress."""
        keys = JiraClient._BACKLOG_AGE_BUCKET_KEYS
        buckets = {k: 0 for k in keys}
        stacked_by_age: dict[str, dict[str, int]] = {
            k: {"with_support": 0, "waiting_on_customer": 0, "waiting_on_engineering": 0}
            for k in keys
        }
        for issue in open_issues:
            age = JiraClient._open_age_days(issue)
            if age is None:
                continue
            bk = JiraClient._backlog_age_bucket_key(age)
            buckets[bk] += 1
            owner = JiraClient._backlog_bottleneck(issue.get("status") or "")
            stacked_by_age[bk][owner] += 1
        stacked = {
            "labels": ["0–7", "8–14", "15–30", "30+"],
            "series": {
                owner: [stacked_by_age[k][owner] for k in keys]
                for owner in ("with_support", "waiting_on_customer", "waiting_on_engineering")
            },
        }
        return buckets, stacked

    @staticmethod
    def _calendar_resolution_ms(issue: dict) -> int | None:
        created_dt = JiraClient._parse_jira_datetime(issue.get("created"))
        resolved_dt = JiraClient._parse_jira_datetime(issue.get("resolutiondate"))
        if created_dt is None or resolved_dt is None:
            return None
        elapsed_ms = int((resolved_dt - created_dt).total_seconds() * 1000)
        return elapsed_ms if elapsed_ms >= 0 else None

    @staticmethod
    def _percentile_ms(values: list[int], pct: float) -> int | None:
        if not values:
            return None
        ordered = sorted(values)
        idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
        return ordered[idx]

    def get_support_kpis(
        self,
        customer_name: str | None,
        match_terms: list[str] | None = None,
        *,
        window_days: int = 180,
    ) -> dict[str, Any]:
        """HELP operational KPI bundle for the ``support-kpis`` deck (single merged Jira fetch)."""
        jql_start = self._jql_log_len()
        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
            customer_name, match_terms
        )
        union_jql = (
            "project = HELP AND "
            f"{base_filter} AND {_TRANSIENT_LABELS_EXCLUSION} AND ("
            "statusCategory != Done OR "
            "(resolution is not EMPTY AND resolved >= -365d) OR "
            "created >= -365d"
            ") ORDER BY updated DESC"
        )
        try:
            merged_raw = self._search(
                union_jql,
                max_results=HELP_METRICS_MERGED_MAX_RESULTS,
                fields=list(dict.fromkeys(_CUSTOMER_TICKET_SLIDE_FIELDS + ["assignee"])),
                data_description="HELP support KPIs (merged open / 365d resolved / 365d created)",
            )
        except Exception as e:
            logger.warning("Support KPIs fetch failed for %s: %s", customer_name, e)
            return {
                "error": str(e),
                "customer": customer_name,
                "window_days": window_days,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "jql_queries": self._jql_since(jql_start),
            }

        open_raw, _resolved_6mo, year_raw, resolved_year_raw = self._partition_help_metrics_merged(
            merged_raw
        )
        open_issues = [self._normalize_issue(i) for i in open_raw]
        year_issues = [self._normalize_issue(i) for i in year_raw]
        resolved_year = [self._normalize_issue(i) for i in resolved_year_raw]

        now = datetime.now(timezone.utc)
        cutoff_window = now - timedelta(days=max(1, int(window_days)))

        def _created_in_window(issue: dict) -> bool:
            cre = self._parse_jira_datetime(issue.get("created"))
            return cre is not None and cre >= cutoff_window

        created_window = [i for i in year_issues if _created_in_window(i)]
        resolved_window = [
            i
            for i in resolved_year
            if (rd := self._parse_jira_datetime(i.get("resolutiondate"))) is not None
            and rd >= cutoff_window
        ]

        # Weekly intake / flow (last ~13 ISO weeks from windowed issues)
        intake_issues = [{"created": i.get("created", ""), "resolution": i.get("resolution", "")} for i in created_window]
        flow_issue_dicts = [
            {
                "created": i.get("created", ""),
                "updated": i.get("updated", ""),
                "resolutiondate": i.get("resolutiondate", ""),
                "resolution": i.get("resolution", ""),
            }
            for i in year_issues
        ]
        intake_weekly = self._bucket_by_week(intake_issues)[-13:]
        flow_weekly = self._flow_weekly_in_window(flow_issue_dicts, window_days=int(window_days), now=now)

        # Intake breakdowns (window)
        by_priority: Counter[str] = Counter()
        by_type: Counter[str] = Counter()
        by_customer: Counter[str] = Counter()
        for issue in created_window:
            pr = (issue.get("priority") or "—").split(":")[0]
            by_priority[pr] += 1
            by_type[issue.get("type") or "Unknown"] += 1
            orgs = issue.get("organizations") or []
            org = orgs[0] if orgs else "(No organization)"
            by_customer[org] += 1

        backlog_age_buckets, backlog_age_stacked = self._backlog_age_breakdown(open_issues)

        def _tail_row(issue: dict) -> dict[str, Any]:
            age = self._open_age_days(issue)
            orgs = issue.get("organizations") or []
            return {
                "organization": orgs[0] if orgs else "(No organization)",
                "status": (issue.get("status") or "").strip(),
                "summary": (issue.get("summary") or "").strip(),
                "age_days": round(age, 1) if age is not None else None,
            }

        open_by_age = sorted(
            open_issues,
            key=lambda i: self._open_age_days(i) or 0.0,
            reverse=True,
        )
        tail_risk = [_tail_row(i) for i in open_by_age[:5]]

        def _resolved_since(days: int) -> list[dict]:
            cutoff = now - timedelta(days=max(1, int(days)))
            return [
                issue
                for issue in resolved_year
                if (rd := self._parse_jira_datetime(issue.get("resolutiondate"))) is not None
                and rd >= cutoff
            ]

        sla_by_window: dict[str, dict[str, Any]] = {}
        for days in (30, 90, 365):
            rw = _resolved_since(days)
            sla_by_window[str(days)] = {
                "ttfr": self._compute_sla_field_adherence_pct(rw, "ttfr"),
                "ttr": self._compute_sla_field_adherence_pct(rw, "ttr"),
                "resolved_count": len(rw),
            }
        ttfr_sla = sla_by_window["90"]["ttfr"]
        ttr_sla = sla_by_window["90"]["ttr"]
        ttfr_stats = self._compute_sla(resolved_window, "ttfr")

        # Resolution median / p90 calendar TTR by type (resolved in window)
        by_type_ms: dict[str, list[int]] = {}
        for issue in resolved_window:
            ms = self._calendar_resolution_ms(issue)
            if ms is None:
                continue
            tname = issue.get("type") or "Unknown"
            by_type_ms.setdefault(tname, []).append(ms)

        def _fmt_ms(ms: int) -> str:
            mins = ms / 60_000
            if mins < 60:
                return f"{mins:.0f} min"
            hrs = mins / 60
            if hrs < 24:
                return f"{hrs:.1f}h"
            return f"{hrs / 24:.1f}d"

        resolution_by_type: list[dict[str, Any]] = []
        for tname, values in sorted(by_type_ms.items(), key=lambda x: (-len(x[1]), x[0])):
            med = self._percentile_ms(values, 50)
            p90 = self._percentile_ms(values, 90)
            resolution_by_type.append(
                {
                    "type": tname,
                    "count": len(values),
                    "median": _fmt_ms(med) if med is not None else "—",
                    "p90": _fmt_ms(p90) if p90 is not None else "—",
                }
            )

        from concurrent.futures import ThreadPoolExecutor, as_completed

        escalation_flow: dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_to_proj = {
                pool.submit(
                    self.fetch_project_jira_escalated_flow_weekly,
                    proj,
                    customer_name,
                    match_terms,
                    window_days=int(window_days),
                ): proj
                for proj in ("LEAN", "CUSTOMER")
            }
            for fut in as_completed(fut_to_proj):
                proj = fut_to_proj[fut]
                try:
                    escalation_flow[proj] = fut.result()
                except Exception as exc:
                    logger.warning("Escalation flow fetch failed for %s: %s", proj, exc)
                    escalation_flow[proj] = {
                        "project": proj,
                        "error": str(exc),
                        "flow_weekly": [],
                    }

        try:
            escalation_backlog_engineering = self.fetch_project_jira_escalated_open_backlog(
                "LEAN", customer_name, match_terms
            )
        except Exception as exc:
            logger.warning("LEAN jira_escalated open backlog fetch failed: %s", exc)
            escalation_backlog_engineering = {
                "project": "LEAN",
                "error": str(exc),
                "open_count": 0,
                "backlog_age_buckets": {},
                "backlog_age_stacked": {"labels": [], "series": {}},
            }

        try:
            escalation_backlog_data_integration = self.fetch_project_jira_escalated_open_backlog(
                "CUSTOMER", customer_name, match_terms
            )
        except Exception as exc:
            logger.warning("CUSTOMER jira_escalated open backlog fetch failed: %s", exc)
            escalation_backlog_data_integration = {
                "project": "CUSTOMER",
                "error": str(exc),
                "open_count": 0,
                "backlog_age_buckets": {},
                "backlog_age_stacked": {"labels": [], "series": {}},
            }

        # Customer health: orgs with 3+ open or any ticket 30+ days
        org_open: dict[str, list[dict]] = {}
        for issue in open_issues:
            orgs = issue.get("organizations") or ["(No organization)"]
            primary = orgs[0] if orgs else "(No organization)"
            org_open.setdefault(primary, []).append(issue)
        customer_health: list[dict[str, Any]] = []
        for org, tickets in org_open.items():
            ages = [self._open_age_days(t) for t in tickets if self._open_age_days(t) is not None]
            oldest = max(ages) if ages else 0.0
            if len(tickets) >= 3 or oldest >= 30:
                customer_health.append(
                    {
                        "organization": org,
                        "open_count": len(tickets),
                        "oldest_days": round(oldest, 1),
                    }
                )
        customer_health.sort(key=lambda r: (-r["open_count"], -r["oldest_days"]))

        sentiment_counts: Counter[str] = Counter()
        for issue in resolved_window + open_issues:
            s = (issue.get("sentiment") or "").strip()
            if s:
                sentiment_counts[s] += 1

        ttfr_goal_h = 48
        ttr_goal_h = 160
        ttfr_goal_days = ttfr_goal_h / 24.0
        ttr_goal_days = ttr_goal_h / 24.0
        aging_rows: list[dict[str, Any]] = []
        for issue in open_issues:
            age = self._open_age_days(issue)
            if age is None:
                continue
            reasons: list[str] = []
            if age > ttfr_goal_days and issue.get("ttfr_ms") is None:
                reasons.append(f"No first response >{ttfr_goal_h}h")
            if age > ttr_goal_days:
                reasons.append(f"Open >{ttr_goal_h}h")
            if reasons:
                aging_rows.append(
                    {
                        "key": issue.get("key"),
                        "summary": (issue.get("summary") or "")[:80],
                        "age_days": round(age, 1),
                        "status": issue.get("status"),
                        "reasons": "; ".join(reasons),
                        "assignee": issue.get("assignee") or "Unassigned",
                    }
                )
        aging_rows.sort(key=lambda r: r["age_days"], reverse=True)

        return {
            "customer": customer_name,
            "window_days": int(window_days),
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "open_count": len(open_issues),
            "intake_weekly": intake_weekly,
            "flow_weekly": flow_weekly,
            "intake_breakdown": {
                "by_priority": dict(by_priority.most_common(8)),
                "by_type": dict(by_type.most_common(8)),
                "by_customer": dict(by_customer.most_common(10)),
            },
            "backlog_age_buckets": backlog_age_buckets,
            "backlog_age_stacked": backlog_age_stacked,
            "tail_risk": tail_risk,
            "sla": {"ttfr": ttfr_sla, "ttr": ttr_sla},
            "sla_by_window": sla_by_window,
            "ttfr": ttfr_stats,
            "resolution_by_type": resolution_by_type,
            "escalation_flow": escalation_flow,
            "escalation_backlog_engineering": escalation_backlog_engineering,
            "escalation_backlog_data_integration": escalation_backlog_data_integration,
            "customer_health": customer_health[:25],
            "csat": {
                "by_sentiment": dict(sentiment_counts.most_common()),
                "note": "Jira AI sentiment on HELP tickets — supplementary; not a formal CSAT survey.",
            },
            "aging_beyond_thresholds": {
                "ttfr_goal_hours": ttfr_goal_h,
                "ttr_goal_hours": ttr_goal_h,
                "count": len(aging_rows),
                "tickets": aging_rows[:5],
            },
            "jql_queries": self._jql_since(jql_start),
        }

    def get_help_organizations_by_opened(
        self,
        *,
        days: int = 90,
        max_results: int = 5000,
    ) -> dict[str, Any]:
        """HELP issues created in the last *days* days, tallied by JSM ``Organizations``.

        All-customers / portfolio scope (no per-customer JQL). Issues with
        multiple organizations add one to each. Issues with none map to
        ``(No organization)``.
        """
        jql_start = self._jql_log_len()
        jql = (
            f"project = HELP AND {_TRANSIENT_LABELS_EXCLUSION} AND created >= -{days}d "
            "ORDER BY created DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=max_results,
                fields=[ORG_FIELD],
                data_description=f"HELP tickets by organization (created in last {days}d, portfolio)",
            )
        except Exception as e:
            logger.warning("HELP organizations by opened fetch failed: %s", e)
            return {
                "error": str(e),
                "days": days,
                "by_organization": [],
                "total_issues": 0,
                "jql_queries": self._jql_since(jql_start),
            }

        counts: Counter[str] = Counter()
        for issue in raw:
            f = issue.get("fields", {}) or {}
            orgs = f.get(ORG_FIELD) or []
            names: list[str] = []
            for o in orgs:
                if isinstance(o, dict):
                    n = (o.get("name") or "").strip()
                    if n:
                        names.append(n)
            if not names:
                counts["(No organization)"] += 1
            else:
                for n in names:
                    counts[n] += 1

        sorted_rows = sorted(counts.items(), key=lambda x: (-x[1], x[0].lower()))
        return {
            "days": days,
            "total_issues": len(raw),
            "by_organization": [
                {"organization": name, "count": count} for name, count in sorted_rows
            ],
            "jql_queries": self._jql_since(jql_start),
        }

    def get_help_customer_escalations(
        self,
        customer_name: str | None = None,
        match_terms: list[str] | None = None,
        *,
        max_results: int = 200,
    ) -> dict[str, Any]:
        """Open HELP issues with Jira label ``customer_escalation``, most recently updated first.

        JQL shape: ``project = HELP`` + :meth:`_help_project_customer_filter` (all
        customers: tautology) + ``labels = "customer_escalation"`` + ``statusCategory != Done``
        + ``ORDER BY updated DESC`` — matches the support-deck spec for this slide.
        """
        jql_start = self._jql_log_len()
        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
            customer_name, match_terms
        )
        jql = (
            f"project = HELP AND ({base_filter}) AND labels = \"customer_escalation\" "
            "AND statusCategory != Done ORDER BY updated DESC"
        )

        def _row(issue: dict) -> dict[str, Any]:
            f = issue.get("fields", {}) or {}
            cr = self._parse_jira_datetime(f.get("created"))
            up = self._parse_jira_datetime(f.get("updated"))
            rs = self._parse_jira_datetime(f.get("resolutiondate"))
            st = f.get("status") or {}
            status_name = st.get("name", "—") if isinstance(st, dict) else "—"
            pr = f.get("priority") or {}
            priority_name = pr.get("name", "—") if isinstance(pr, dict) else "—"
            orgs = f.get(ORG_FIELD) or []
            org_names = [o.get("name", "") for o in orgs if isinstance(o, dict) and o.get("name")]
            return {
                "key": issue.get("key", ""),
                "summary": (f.get("summary") or "").strip(),
                "organization": ", ".join(org_names) if org_names else "—",
                "status": status_name,
                "priority": priority_name,
                "created": f.get("created") or "",
                "created_short": cr.strftime("%Y-%m-%d") if cr else "—",
                "updated": f.get("updated") or "",
                "updated_short": up.strftime("%Y-%m-%d") if up else "—",
                "resolved": f.get("resolutiondate") or "",
                "resolved_short": rs.strftime("%Y-%m-%d") if rs else "—",
            }

        try:
            raw = self._search(
                jql,
                max_results=max_results,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description="HELP customer_escalation (open) by updated",
            )
        except Exception as e:
            logger.warning("HELP customer escalations fetch failed: %s", e)
            return {
                "error": str(e),
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "tickets": [],
                "jql_queries": self._jql_since(jql_start),
            }

        return {
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "tickets": [_row(i) for i in raw],
            "jql_queries": self._jql_since(jql_start),
        }

    def get_help_escalation_metrics(
        self,
        customer_name: str | None = None,
        match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """HELP escalation KPIs: open backlog TTR split by ``customer_escalation`` label, plus 90d open/close counts.

        Same JSM org scope and transient label exclusions (Outage, Healthcheck) as
        :meth:`get_customer_ticket_metrics`. TTR = now − created for open NOT DONE tickets
        (median/avg from :meth:`_compute_backlog_age`). Counts and 90d windows use the
        ``customer_escalation`` Jira label.
        """
        jql_start = self._jql_log_len()
        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
            customer_name, match_terms
        )
        excl = _TRANSIENT_LABELS_EXCLUSION
        max_open = HELP_METRICS_MERGED_MAX_RESULTS

        def _label_names(fields: dict) -> list[str]:
            lbs = fields.get("labels")
            if isinstance(lbs, (list, tuple)):
                return [str(x) for x in lbs if x is not None]
            return []

        def _to_backlog_rows(issues: list[dict]) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            for issue in issues:
                f = issue.get("fields", {}) or {}
                rows.append({
                    "project": (f.get("project") or {}).get("key", "HELP"),
                    "created": f.get("created") or "",
                })
            return rows

        jql_open = (
            f"project = HELP AND ({base_filter}) AND {excl} AND statusCategory != Done "
            "ORDER BY updated DESC"
        )
        try:
            raw_open = self._search(
                jql_open,
                max_results=max_open,
                fields=_CUSTOMER_TICKET_SLIDE_FIELDS,
                data_description="HELP open NOT DONE (partition escalation vs not for TTR)",
            )
        except Exception as e:
            logger.warning("HELP escalation metrics (open) fetch failed: %s", e)
            return {
                "error": str(e),
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "jql_queries": self._jql_since(jql_start),
            }

        raw_with_esc: list[dict] = []
        raw_without_esc: list[dict] = []
        for issue in raw_open:
            f = issue.get("fields", {}) or {}
            if "customer_escalation" in _label_names(f):
                raw_with_esc.append(issue)
            else:
                raw_without_esc.append(issue)

        ttr_esc = self._compute_backlog_age(
            _to_backlog_rows(raw_with_esc), project_key="HELP",
        )
        ttr_not_esc = self._compute_backlog_age(
            _to_backlog_rows(raw_without_esc), project_key="HELP",
        )

        jql_90_created = (
            f'project = HELP AND ({base_filter}) AND {excl} AND labels = "customer_escalation" '
            "AND created >= -90d ORDER BY created DESC"
        )
        jql_90_resolved = (
            f'project = HELP AND ({base_filter}) AND {excl} AND labels = "customer_escalation" '
            "AND resolution is not EMPTY AND resolved >= -90d ORDER BY resolved DESC"
        )
        n_90o: int | None = self._jql_match_total(jql_90_created)
        if n_90o is None:
            try:
                n_90o = len(
                    self._search(
                        jql_90_created,
                        max_results=5000,
                        fields=["summary"],
                        data_description="HELP customer_escalation — 90d created (count fallback)",
                    )
                )
            except Exception as e:
                logger.warning("HELP escalation metrics (90d created count) failed: %s", e)
                n_90o = 0
        n_90c: int | None = self._jql_match_total(jql_90_resolved)
        if n_90c is None:
            try:
                n_90c = len(
                    self._search(
                        jql_90_resolved,
                        max_results=5000,
                        fields=["summary"],
                        data_description="HELP customer_escalation — 90d resolved (count fallback)",
                    )
                )
            except Exception as e:
                logger.warning("HELP escalation metrics (90d resolved count) failed: %s", e)
                n_90c = 0

        cap = HELP_ESCALATION_LLM_MAX_ISSUES
        jql_lbl_open = (
            f'project = HELP AND ({base_filter}) AND {excl} AND labels = "customer_escalation" '
            "AND statusCategory != Done ORDER BY updated DESC"
        )
        try:
            raw_llm_open = self._search(
                jql_lbl_open,
                max_results=cap,
                fields=_ISSUE_FIELDS,
                data_description="HELP escalation — open w/ label (LLM + slide context)",
            )
        except Exception as e:
            logger.warning("HELP escalation LLM fetch (open labeled) failed: %s", e)
            raw_llm_open = []
        try:
            raw_llm_90c = self._search(
                jql_90_created,
                max_results=cap,
                fields=_ISSUE_FIELDS,
                data_description="HELP escalation — created 90d (LLM context)",
            )
        except Exception as e:
            logger.warning("HELP escalation LLM fetch (90d created) failed: %s", e)
            raw_llm_90c = []
        try:
            raw_llm_90r = self._search(
                jql_90_resolved,
                max_results=cap,
                fields=_ISSUE_FIELDS,
                data_description="HELP escalation — resolved 90d (LLM context)",
            )
        except Exception as e:
            logger.warning("HELP escalation LLM fetch (90d resolved) failed: %s", e)
            raw_llm_90r = []

        def _issue_for_escalation_llm(issue: dict) -> dict[str, Any]:
            d: dict[str, Any] = dict(self._normalize_issue(issue))
            d["description_text"] = (d.get("description_text") or "")[:3000]
            cts = d.get("comment_texts") or []
            if isinstance(cts, list):
                tail = [str(c) for c in cts[-20:]]
                d["comment_texts"] = [t[:1200] for t in tail]
            return d

        llm_ticket_context = {
            "jsm_organizations_resolved": list(resolved_jsm_orgs),
            "open_with_label": [_issue_for_escalation_llm(i) for i in raw_llm_open],
            "created_90d": [_issue_for_escalation_llm(i) for i in raw_llm_90c],
            "resolved_90d": [_issue_for_escalation_llm(i) for i in raw_llm_90r],
            "totals_90d": {
                "created": int(n_90o) if n_90o is not None else 0,
                "resolved": int(n_90c) if n_90c is not None else 0,
            },
            "sample_limits": {
                "per_bucket": cap,
                "description_chars": 3000,
                "comment_items_max": 20,
                "comment_char_cap": 1200,
            },
        }

        return {
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "not_done_escalation_count": len(raw_with_esc),
            "escalations_opened_90d": int(n_90o) if n_90o is not None else 0,
            "escalations_closed_90d": int(n_90c) if n_90c is not None else 0,
            "ttr_open_backlog_customer_escalation": ttr_esc,
            "ttr_open_backlog_not_customer_escalation": ttr_not_esc,
            "jql_queries": self._jql_since(jql_start),
            "llm_ticket_context": llm_ticket_context,
        }

    @staticmethod
    def _bucket_by_month(
        issues: list[dict],
        escalated_only: bool = False,
        exclude_escalated: bool = False,
    ) -> list[dict[str, Any]]:
        """Return the last 12 full-month created/resolved buckets, oldest first."""
        from datetime import datetime

        now = datetime.now(timezone.utc)
        # Use full months only (exclude the current partial month).
        year = now.year
        month = now.month - 1
        if month == 0:
            month = 12
            year -= 1
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

    def get_help_factory_start_day_buckets(
        self,
        customer_name: str | None,
        _match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """HELP ticket counts in fixed day windows after each entity's Salesforce factory start date.

        With a customer name: sums counts across Customer Entity rows matching that account (same rule
        as ARR rollups). With *customer_name* empty/None: **portfolio** mode — all Customer Entity
        rows in Salesforce (installed-base aggregate).

        Each entity uses :meth:`help_salesforce_entity_site_scoped_clause` and
        :data:`HELP_FACTORY_START_DAY_BUCKETS` (see export script). Excludes Outage/Healthcheck labels.
        """
        from .config import SF_ACCOUNT_FACTORY_START_DATE_FIELD
        from .salesforce_client import SalesforceClient, _customer_name_matches_entity_account, _parse_sf_contract_date

        cn = (customer_name or "").strip()
        portfolio = not bool(cn)
        fs_field = (SF_ACCOUNT_FACTORY_START_DATE_FIELD or "").strip() or "factory_start_date"
        labels = [tup[3] for tup in HELP_FACTORY_START_DAY_BUCKETS]
        keys = [tup[2] for tup in HELP_FACTORY_START_DAY_BUCKETS]
        empty_base: dict[str, Any] = {
            "customer": cn or None,
            "factory_start_date_field": fs_field,
            "bucket_keys": keys,
            "bucket_labels": labels,
            "counts": [0] * len(HELP_FACTORY_START_DAY_BUCKETS),
            "portfolio_aggregate": portfolio,
        }

        all_accounts = SalesforceClient().get_entity_accounts()
        if portfolio:
            entities = list(all_accounts)
            if entities:
                logger.info(
                    "HELP factory start buckets: portfolio aggregate over %d Customer Entity rows",
                    len(entities),
                )
        else:
            name_upper = cn.upper()
            entities = [a for a in all_accounts if _customer_name_matches_entity_account(name_upper, a)]

        if not entities:
            return {
                **empty_base,
                "error": (
                    "No Salesforce Customer Entity rows returned."
                    if portfolio
                    else "No Salesforce Customer Entity matched this customer."
                ),
                "entity_rows_matched": 0,
                "jql_queries": [
                    {
                        "description": _HELP_FACTORY_BUCKET_SPEAKER_DESCRIPTIONS[bi],
                        "jql": "(No JQL — no Salesforce Customer Entity rows in scope.)",
                        "total": 0,
                    }
                    for bi in range(len(HELP_FACTORY_START_DAY_BUCKETS))
                ],
            }

        totals = [0] * len(HELP_FACTORY_START_DAY_BUCKETS)
        example_jql_by_bucket: list[str | None] = [None] * len(HELP_FACTORY_START_DAY_BUCKETS)
        jira_failed = False
        skipped_no_factory = 0
        skipped_no_org = 0
        entities_counted = 0

        for row in entities:
            start = _parse_sf_contract_date(row.get("factory_start_date"))
            if not start:
                skipped_no_factory += 1
                continue
            scope_clause, _scope_meta = self.help_salesforce_entity_site_scoped_clause(row)
            if "___CORTEX_NO_ORG_MATCH___" in scope_clause:
                skipped_no_org += 1
                continue
            entities_counted += 1
            for bi, (lo, hi, _key, _lbl) in enumerate(HELP_FACTORY_START_DAY_BUCKETS):
                d0 = start + timedelta(days=lo)
                d1 = start + timedelta(days=hi + 1)
                jql = (
                    f"project = HELP AND {scope_clause} AND {_TRANSIENT_LABELS_EXCLUSION} "
                    f'AND created >= "{d0:%Y-%m-%d}" AND created < "{d1:%Y-%m-%d}"'
                )
                if example_jql_by_bucket[bi] is None:
                    example_jql_by_bucket[bi] = jql
                total = self._jql_match_total(jql)
                if total is None:
                    jira_failed = True
                else:
                    totals[bi] += int(total)

        jql_queries: list[dict[str, Any]] = []
        for bi, (_lo, _hi, _key, lbl) in enumerate(HELP_FACTORY_START_DAY_BUCKETS):
            desc = _HELP_FACTORY_BUCKET_SPEAKER_DESCRIPTIONS[bi]
            ex_jql = example_jql_by_bucket[bi]
            tot_i = int(totals[bi])
            if ex_jql is None:
                ex_jql = (
                    f"(No sample JQL — no entity had both factory start date and JSM org resolution.) "
                    f"Bucket: {lbl}"
                )
            note = (
                " Example JQL from the first counted entity; chart value sums approximate-count across all counted entities."
                if entities_counted > 1 or portfolio
                else ""
            )
            jql_queries.append({
                "description": desc,
                "jql": (ex_jql + note).strip(),
                "total": tot_i,
            })

        out: dict[str, Any] = {
            **empty_base,
            "counts": totals,
            "entity_rows_matched": len(entities),
            "entities_with_factory_and_org": entities_counted,
            "skipped_no_factory_start": skipped_no_factory,
            "skipped_no_jsm_org": skipped_no_org,
            "jira_count_partial_failure": jira_failed,
            "jql_queries": jql_queries,
        }
        if entities_counted == 0:
            out["error"] = (
                "No matching entities had both a factory start date and a resolved JSM organization."
            )
        return out

    def get_help_ticket_volume_trends(
        self,
        customer_name: str | None = None,
        match_terms: list[str] | None = None,
    ) -> dict[str, Any]:
        """Return 12-month HELP created vs resolved trends for a customer or all customers."""
        jql_start = self._jql_log_len()
        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
            customer_name, match_terms
        )
        jql = (
            f"project = HELP AND {base_filter} AND {_TRANSIENT_LABELS_EXCLUSION} "
            "AND (created >= -365d OR resolved >= -365d) "
            "ORDER BY created DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=HELP_TRENDS_MAX_RESULTS,
                fields=_TREND_FIELDS,
                data_description="HELP volume trends (12-month created vs resolved)",
            )
        except Exception as e:
            logger.warning("HELP ticket trend fetch failed: %s", e)
            return {
                "error": str(e),
                "customer": customer_name,
                "jsm_organizations_resolved": resolved_jsm_orgs,
                "all": [],
                "escalated": [],
                "non_escalated": [],
                "jql_queries": self._jql_since(jql_start),
            }

        issues = []
        for issue in raw:
            f = issue.get("fields", {})
            issues.append({
                "created": f.get("created") or "",
                "resolutiondate": f.get("resolutiondate") or "",
                "labels": f.get("labels") or [],
            })

        return {
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "all": self._bucket_by_month(issues, escalated_only=False),
            "escalated": self._bucket_by_month(issues, escalated_only=True),
            "non_escalated": self._bucket_by_month(issues, exclude_escalated=True),
            "jql_queries": self._jql_since(jql_start),
        }

    def get_help_monthly_operational_table(
        self,
        customer_name: str | None = None,
        match_terms: list[str] | None = None,
        *,
        num_months: int = 12,
    ) -> dict[str, Any]:
        """Monthly HELP counts aligned with operational spreadsheet (non-outage vs outage/healthcheck).

        Uses Jira approximate-count per query. Snapshots treat an issue as open at instant *T* when
        ``created < T`` and (``resolution IS EMPTY`` or ``resolved >= T``). *Open (EoM)* for month *M*
        equals the snapshot at the first instant of the month after *M*.

        ``num_months`` includes the current calendar month (may be partial vs closed months).
        """
        from calendar import monthrange
        from concurrent.futures import ThreadPoolExecutor, as_completed

        base_filter, resolved_jsm_orgs = self._help_project_customer_filter(
            customer_name, match_terms
        )
        scope = f"project = HELP AND ({base_filter})"
        now = datetime.now(timezone.utc)
        y_end, m_end = now.year, now.month

        months: list[tuple[int, int]] = []
        y, m = y_end, m_end
        for _ in range(num_months):
            months.append((y, m))
            m -= 1
            if m == 0:
                m = 12
                y -= 1
        months.reverse()

        def _iso(y_: int, m_: int) -> str:
            return f"{y_:04d}-{m_:02d}-01"

        boundaries_iso: list[str] = []
        for y_, m_ in months:
            boundaries_iso.append(_iso(y_, m_))
        yn, mn = months[-1][0], months[-1][1]
        if mn == 12:
            boundaries_iso.append(_iso(yn + 1, 1))
        else:
            boundaries_iso.append(_iso(yn, mn + 1))

        def _jql_open_snapshot(iso: str, *, outage_only: bool) -> str:
            lab = _HELP_MONTHLY_OUTAGE_ONLY_LABELS if outage_only else _HELP_MONTHLY_NON_OUTAGE_LABELS
            return (
                f'{scope} AND {lab} AND created < "{iso}" '
                f'AND (resolution IS EMPTY OR resolved >= "{iso}")'
            )

        def _jql_opened_in_range(s_iso: str, e_iso: str, *, outage_only: bool) -> str:
            lab = _HELP_MONTHLY_OUTAGE_ONLY_LABELS if outage_only else _HELP_MONTHLY_NON_OUTAGE_LABELS
            return (
                f'{scope} AND {lab} AND created >= "{s_iso}" AND created < "{e_iso}"'
            )

        def _jql_resolved_in_range(s_iso: str, e_iso: str) -> str:
            return (
                f"{scope} AND {_HELP_MONTHLY_NON_OUTAGE_LABELS} AND statusCategory = Done "
                f'AND resolved >= "{s_iso}" AND resolved < "{e_iso}"'
            )

        def _jql_resolved_outage_in_range(s_iso: str, e_iso: str) -> str:
            return (
                f"{scope} AND {_HELP_MONTHLY_OUTAGE_ONLY_LABELS} AND statusCategory = Done "
                f'AND resolved >= "{s_iso}" AND resolved < "{e_iso}"'
            )

        tasks: list[tuple[str, str]] = []
        for iso in boundaries_iso:
            tasks.append((f"snap_main:{iso}", _jql_open_snapshot(iso, outage_only=False)))
            tasks.append((f"snap_ot:{iso}", _jql_open_snapshot(iso, outage_only=True)))
        for k in range(len(months)):
            s_iso, e_iso = boundaries_iso[k], boundaries_iso[k + 1]
            tasks.append((f"opened_main:{k}", _jql_opened_in_range(s_iso, e_iso, outage_only=False)))
            tasks.append((f"resolved_main:{k}", _jql_resolved_in_range(s_iso, e_iso)))
            tasks.append((f"opened_ot:{k}", _jql_opened_in_range(s_iso, e_iso, outage_only=True)))
            tasks.append((f"resolved_ot:{k}", _jql_resolved_outage_in_range(s_iso, e_iso)))

        counts: dict[str, int | None] = {}
        partial_failure = False
        max_workers = max(1, min(10, _JIRA_PARALLEL_WORKERS * 3))

        def _run_one(item: tuple[str, str]) -> tuple[str, int | None]:
            key, jql = item
            return key, self._jql_match_total(jql)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_run_one, t) for t in tasks]
            for fut in as_completed(futures):
                key, val = fut.result()
                counts[key] = val
                if val is None:
                    partial_failure = True

        def _n(key: str) -> int:
            v = counts.get(key)
            return int(v) if v is not None else 0

        rows: list[dict[str, Any]] = []
        sample_jql: list[dict[str, str]] = [
            {
                "description": "HELP monthly — open snapshot (non-outage) example",
                "jql": _jql_open_snapshot(boundaries_iso[0], outage_only=False),
            },
            {
                "description": "HELP monthly — resolved in month (non-outage) example",
                "jql": _jql_resolved_in_range(boundaries_iso[0], boundaries_iso[1]),
            },
            {
                "description": "HELP monthly — outage opened in month example",
                "jql": _jql_opened_in_range(boundaries_iso[0], boundaries_iso[1], outage_only=True),
            },
            {
                "description": "HELP monthly — outage resolved in month example",
                "jql": _jql_resolved_outage_in_range(boundaries_iso[0], boundaries_iso[1]),
            },
        ]

        for k, (y_, m_) in enumerate(months):
            s_iso, e_iso = boundaries_iso[k], boundaries_iso[k + 1]
            dm = monthrange(y_, m_)[1]
            opened_main = _n(f"opened_main:{k}")
            resolved_main = _n(f"resolved_main:{k}")
            opened_ot = _n(f"opened_ot:{k}")
            resolved_ot = _n(f"resolved_ot:{k}")
            open_som_main = _n(f"snap_main:{s_iso}")
            open_eom_main = _n(f"snap_main:{e_iso}")
            open_som_ot = _n(f"snap_ot:{s_iso}")
            open_eom_ot = _n(f"snap_ot:{e_iso}")
            delta = opened_main - resolved_main
            tix_day = round(opened_main / float(dm), 1) if dm else 0.0
            ot_tix_day = round(opened_ot / float(dm), 1) if dm else 0.0
            outage_delta = opened_ot - resolved_ot
            partial = y_ == y_end and m_ == m_end
            label = f"{datetime(y_, m_, 1, tzinfo=timezone.utc).strftime('%b %Y')}"
            if partial:
                label = f"{label} *"
            rows.append({
                "month_key": f"{y_}-{m_:02d}",
                "label": label,
                "year": y_,
                "month": m_,
                "days_in_month": dm,
                "partial": partial,
                "total_open_eom": open_eom_main,
                "tix_per_day": tix_day,
                "opened": opened_main,
                "resolved": resolved_main,
                "open_start_of_month": open_som_main,
                "delta": delta,
                "outage_tix_per_day": ot_tix_day,
                "outage_opened": opened_ot,
                "outage_resolved": resolved_ot,
                "outage_delta": outage_delta,
                "outage_open_start": open_som_ot,
                "outage_open_eom": open_eom_ot,
            })

        return {
            "customer": customer_name,
            "jsm_organizations_resolved": resolved_jsm_orgs,
            "rows": rows,
            "jql_queries": sample_jql,
            "jira_count_partial_failure": partial_failure,
        }

    def _get_help_ticket_volume_trends(self) -> dict[str, Any]:
        """Backward-compatible all-customer HELP volume trend helper."""
        return self.get_help_ticket_volume_trends(None)

    def _get_engineering_tickets(self, customer_name: str) -> dict[str, Any]:
        """Fetch LEAN project tickets that reference a customer.

        Returns open/recent-closed tickets relevant to engineering work
        affecting this customer — useful for CS to know what's in flight.
        """
        terms = jira_customer_search_terms(customer_name)
        text_filter = _jql_text_match_any(("summary", "description"), terms)
        jql = (
            f"project = LEAN AND {text_filter}"
            f" ORDER BY updated DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=50,
                data_description="LEAN issues mentioning customer (engineering pipeline)",
            )
        except Exception as e:
            logger.warning("LEAN search failed for %s: %s", customer_name, e)
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

    def _get_enhancement_requests(self, customer_name: str) -> dict[str, Any]:
        """Fetch ER project tickets for a customer.

        Returns open and recently shipped enhancement requests — shows
        the customer that their feedback drives product improvements.
        """
        terms = jira_customer_search_terms(customer_name)
        text_filter = _jql_text_match_any(("summary", "description"), terms)
        customer_filter = _jql_in_quoted_values('"Customer"', terms)
        jql = (
            f"project = ER AND ({text_filter} OR {customer_filter})"
            f" ORDER BY updated DESC"
        )
        try:
            raw = self._search(
                jql,
                max_results=50,
                data_description="ER enhancement requests for customer",
            )
        except Exception as e:
            logger.warning("ER search failed for %s: %s", customer_name, e)
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

    def _get_flagged_field_id(self) -> "str | None":
        """Discover the Jira "Flagged" (impediment) custom-field id, cached per client."""
        cached = getattr(self, "_flagged_field_id_cache", "__unset__")
        if cached != "__unset__":
            return cached
        field_id: str | None = None
        try:
            resp = requests.get(
                f"{self.api_base_url}/rest/api/3/field", headers=self._headers, timeout=15
            )
            if resp.ok:
                for f in resp.json():
                    if (f.get("name") or "").strip().lower() == "flagged":
                        field_id = f.get("id")
                        break
        except Exception as e:
            logger.warning("Flagged field discovery failed: %s", e)
        self._flagged_field_id_cache = field_id
        return field_id

    def _fetch_issue_changelogs(
        self,
        keys: list[str],
        *,
        flagged_field_id: "str | None" = None,
        max_workers: int = 6,
    ) -> dict[str, dict]:
        """Fetch changelog histories (+ created, flagged) for issue keys.

        Returns ``{key: {"histories": [...], "created": iso, "flagged": bool}}``;
        keys that fail are omitted (callers fall back to the ``updated`` proxy).
        """
        out: dict[str, dict] = {}
        if not keys:
            return out
        fields = "status,created"
        if flagged_field_id:
            fields += f",{flagged_field_id}"

        def _one(key: str) -> tuple[str, dict | None]:
            try:
                resp = requests.get(
                    f"{self.api_base_url}/rest/api/3/issue/{key}",
                    headers=self._headers,
                    params={"expand": "changelog", "fields": fields},
                    timeout=20,
                )
                if not resp.ok:
                    return key, None
                data = resp.json()
                f = data.get("fields") or {}
                flagged = bool(f.get(flagged_field_id)) if flagged_field_id else False
                return key, {
                    "histories": (data.get("changelog") or {}).get("histories") or [],
                    "created": f.get("created"),
                    "flagged": flagged,
                }
            except Exception as e:
                logger.debug("changelog fetch failed for %s: %s", key, e)
                return key, None

        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, _JIRA_PARALLEL_WORKERS * 2))) as pool:
            for key, val in pool.map(_one, keys):
                if val is not None:
                    out[key] = val
        return out

    def _compute_changelog_signals(
        self, in_flight: list[dict]
    ) -> "tuple[dict[str, Any], dict[str, float], set[str]]":
        """Fetch changelogs for active items and derive time-in-status + flagged signals.

        Returns ``(status_flow, stage_age_by_key, flagged_keys)``; empty values when
        there is nothing active or the fetch fails (callers fall back to the proxy).
        """
        active_keys = [
            t["key"] for t in in_flight if (t.get("status") or "") in _ACTIVE_WIP_STATUSES
        ]
        if not active_keys:
            return {}, {}, set()
        # One changelog GET per active item; cap so a large WIP set can't explode the
        # fetch. in_flight is ``ORDER BY updated DESC`` so this keeps the freshest work.
        _CHANGELOG_FETCH_CAP = 300
        if len(active_keys) > _CHANGELOG_FETCH_CAP:
            active_keys = active_keys[:_CHANGELOG_FETCH_CAP]
            active_key_set = set(active_keys)
        else:
            active_key_set = None
        flagged_field_id = self._get_flagged_field_id()
        changelogs = self._fetch_issue_changelogs(active_keys, flagged_field_id=flagged_field_id)
        if not changelogs:
            return {}, {}, set()
        active_for_flow: list[dict] = []
        for t in in_flight:
            if (t.get("status") or "") not in _ACTIVE_WIP_STATUSES:
                continue
            if active_key_set is not None and t["key"] not in active_key_set:
                continue
            cl = changelogs.get(t["key"]) or {}
            active_for_flow.append({
                "key": t["key"],
                "status": t.get("status"),
                "created": cl.get("created") or t.get("created"),
                "changelog": cl.get("histories") or [],
                "flagged": cl.get("flagged", False),
            })
        status_flow = summarize_status_flow(active_for_flow)
        stage_age_by_key = {
            a["key"]: a["days_in_status"]
            for a in active_for_flow
            if a.get("days_in_status") is not None
        }
        flagged_keys = {a["key"] for a in active_for_flow if a.get("flagged")}
        return status_flow, stage_age_by_key, flagged_keys

    def get_engineering_portfolio(self, days: int = 30) -> dict[str, Any]:
        """Fetch a product/engineering-wide SDLC snapshot — not per-customer.

        Returns sprint state, work-in-progress by theme, velocity, bug health,
        enhancement backlog, and aggregate support pressure.
        """
        import re
        import requests as _req

        jql_start = self._jql_log_len()

        # ── Active sprint from Board 44 (LEAN Scrum - CURRENT Issues) ──
        sprint_info: dict = {}
        recent_sprints: list[dict] = []
        try:
            resp = _req.get(
                f"{self.api_base_url}/rest/agile/1.0/board/44/sprint?state=active",
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
                f"{self.api_base_url}/rest/agile/1.0/board/44/sprint?state=closed&maxResults=4",
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
            "labels", "created", "updated", "resolution", "description", "parent",
            SPRINT_FIELD, STORY_POINTS_FIELD,
        ]
        # ``/rest/api/3/search/jql`` caps each page at ~100 issues regardless of
        # ``maxResults``; ``_search`` follows ``nextPageToken`` so we get the full set
        # (in-flight LEAN WIP is ~1k issues, not the 100 a single page returns).
        try:
            in_flight_raw = self._search(
                "project = LEAN AND status in (\"In Progress\", \"In Review\", \"Open\", \"Reopened\") ORDER BY updated DESC",
                max_results=3000,
                fields=_eng_fields,
                data_description="LEAN in-flight engineering work (Open / In Progress / In Review / Reopened)",
            )
        except Exception as e:
            logger.warning("LEAN in-flight fetch failed: %s", e)
            in_flight_raw = []

        # ── Recent closed LEAN tickets ──
        try:
            closed_raw = self._search(
                f"project = LEAN AND status = Closed AND updated >= -{days}d ORDER BY updated DESC",
                max_results=2000,
                fields=_eng_fields,
                data_description=f"LEAN issues closed or updated in last {days} days",
            )
        except Exception as e:
            logger.warning("LEAN closed fetch failed: %s", e)
            closed_raw = []

        def _lean_norm(issue: dict) -> dict:
            f = issue.get("fields", {})
            sp_list = f.get(SPRINT_FIELD) or []
            # Non-future sprints this issue has belonged to. len > 1 ⇒ carried over
            # across sprint boundaries (spillover) — a stronger stall signal than
            # the ``updated`` idle proxy.
            sprint_names = [s.get("name", "") for s in sp_list if s.get("state") != "future"]
            sp_raw = f.get(STORY_POINTS_FIELD)
            try:
                story_points = float(sp_raw) if sp_raw is not None else None
            except (TypeError, ValueError):
                story_points = None
            desc_raw = _extract_adf_text(f.get("description"))
            parent = f.get("parent") or {}
            parent_summary = (parent.get("fields") or {}).get("summary", "") if parent else ""
            return {
                "key": issue["key"],
                "summary": f.get("summary", ""),
                "parent_summary": parent_summary,
                "status": f.get("status", {}).get("name", ""),
                "type": f.get("issuetype", {}).get("name", ""),
                "priority": (f.get("priority") or {}).get("name", ""),
                "assignee": (f.get("assignee") or {}).get("displayName", ""),
                "labels": f.get("labels") or [],
                "created": (f.get("created") or "")[:10],
                "updated": (f.get("updated") or "")[:10],
                "resolution": (f.get("resolution") or {}).get("name", "") if f.get("resolution") else "",
                "sprints": sprint_names,
                "sprint_count": len(sprint_names),
                "story_points": story_points,
                "description_text": (desc_raw or "")[:4000],
            }

        in_flight = [_lean_norm(i) for i in in_flight_raw]
        closed = [_lean_norm(i) for i in closed_raw]

        # ── Theme extraction: bracket-prefix → parent epic → untagged ──
        # Only ~40% of in-flight summaries carry a [Theme] prefix, so a summary-only
        # rule dumps the majority into "Other". Falling back to the parent epic name
        # recovers a real area for most of the rest; what's left is genuinely untagged.
        _theme_re = re.compile(r"^\[([^\]]+)\]")

        def _theme(t: dict) -> str:
            m = _theme_re.match(t.get("summary") or "")
            if m:
                return m.group(1).strip()
            parent = (t.get("parent_summary") or "").strip()
            if parent:
                parent = _theme_re.sub("", parent).strip()
                if parent:
                    return parent[:28]
            return "Untagged"

        themes: dict[str, list[dict]] = {}
        for t in in_flight:
            th = _theme(t)
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

        # ── Backlog staleness: split active vs abandoned WIP by last-update age ──
        # The "open" set is dominated by zombie tickets (untouched for months/years),
        # which inflates per-engineer WIP and the queue headline numbers. Split it so
        # the load/flow/sprint slides can show real WIP and an explicit hygiene number.
        _today_eng = date.today()

        def _eng_idle_days(t: dict) -> int | None:
            d = _eng_parse_day(t.get("updated"))
            return (_today_eng - d).days if d else None

        by_assignee_active: dict[str, int] = {}
        by_assignee_stale: dict[str, int] = {}
        abandoned_open = 0
        fresh_open = 0
        for t in in_flight:
            idle = _eng_idle_days(t)
            if idle is None:
                continue
            nm = t.get("assignee") or ""
            if idle <= ENG_ACTIVE_WIP_DAYS:
                fresh_open += 1
                if nm:
                    by_assignee_active[nm] = by_assignee_active.get(nm, 0) + 1
            if idle > ENG_ABANDONED_DAYS:
                abandoned_open += 1
                if nm:
                    by_assignee_stale[nm] = by_assignee_stale.get(nm, 0) + 1
        backlog_staleness = {
            "open_total": len(in_flight),
            "abandoned_days": ENG_ABANDONED_DAYS,
            "active_days": ENG_ACTIVE_WIP_DAYS,
            "abandoned_open": abandoned_open,
            "abandoned_pct": round(100 * abandoned_open / len(in_flight)) if in_flight else 0,
            "fresh_open": fresh_open,
        }

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
        # Open tickets — the full open backlog, most recently updated first. We do NOT
        # filter on ``updated`` here: the ER backlog is large but stale (only ~1 of
        # ~248 open ERs is touched in a year), so a recency filter collapses a real
        # backlog to a single row and hides the hygiene story.
        _er_fields = ["summary", "status", "issuetype", "priority",
                      "labels", "created", "updated", "resolution",
                      "description", "comment"]
        try:
            er_open_raw = self._search(
                (
                    "project = ER AND resolution is EMPTY "
                    "AND status not in (Done, Closed, \"Not Taken\") "
                    "ORDER BY updated DESC"
                ),
                max_results=500,
                fields=_er_fields,
                data_description="ER open enhancement backlog (all open)",
            )
        except Exception as e:
            logger.warning("ER open fetch failed: %s", e)
            er_open_raw = []

        # Shipped tickets — resolved in the last year, most recently updated first
        try:
            er_shipped_raw = self._search(
                (
                    "project = ER AND resolution in (Fixed, Done) "
                    "AND updated >= -365d "
                    "ORDER BY updated DESC"
                ),
                max_results=100,
                fields=_er_fields,
                data_description="ER shipped or Done enhancements (last year)",
            )
        except Exception as e:
            logger.warning("ER shipped fetch failed: %s", e)
            er_shipped_raw = []

        # Declined count only — the enhanced search endpoint no longer returns a
        # ``total``, so use the dedicated count endpoint (a body fetch would page).
        try:
            er_declined_count = self.jql_match_count(
                "project = ER AND resolution in (\"Won't Do\", \"Won't Fix\", Declined, \"Future Consideration\", \"Not Taken\")",
                data_description="ER declined / won't do / not taken (count query)",
            ) or 0
        except Exception as e:
            logger.warning("ER declined fetch failed: %s", e)
            er_declined_count = 0

        def _norm_er(i: dict) -> dict:
            f = i["fields"]
            return {
                "key": i["key"],
                "summary": f.get("summary", "")[:500],
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
            help_raw = self._search(
                f"project = HELP AND {_TRANSIENT_LABELS_EXCLUSION} AND created >= -{days}d ORDER BY created DESC",
                max_results=2000,
                fields=["summary", "status", "issuetype", "priority",
                        "created", "resolution", "labels"],
                data_description=f"HELP aggregate desk load (created last {days} days)",
            )
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
        priority_to_full_name: dict[str, str] = {}
        for i in help_raw:
            pr = i["fields"].get("priority") or {}
            full = (pr.get("name") or "").strip()
            if not full:
                short = "Unknown"
            else:
                short = full.split(":")[0] if ":" in full else full
            help_by_priority[short] = help_by_priority.get(short, 0) + 1
            if short not in priority_to_full_name and full:
                priority_to_full_name[short] = full

        base_help_scope = (
            f"project = HELP AND {_TRANSIENT_LABELS_EXCLUSION} AND created >= -{days}d"
        )
        aggregate_help_jql = f"{base_help_scope} ORDER BY created DESC"
        jql_by_priority_short: dict[str, str] = {}
        for short in help_by_priority:
            if short == "Unknown":
                jql_by_priority_short[short] = (
                    f"{base_help_scope} AND priority is EMPTY ORDER BY created DESC"
                )
            else:
                fulln = priority_to_full_name.get(short)
                if fulln:
                    jql_by_priority_short[short] = (
                        f'{base_help_scope} AND priority = "{_jql_escape_string(fulln)}" '
                        "ORDER BY created DESC"
                    )
                else:
                    jql_by_priority_short[short] = aggregate_help_jql

        support_pressure = {
            "total": len(help_raw),
            "open": help_open,
            "escalated_to_eng": help_escalated,
            "open_bugs": help_bugs,
            "by_priority": dict(sorted(help_by_priority.items(), key=lambda x: -x[1])),
            "jql_by_priority_short": jql_by_priority_short,
            "aggregate_jql": aggregate_help_jql,
        }

        # ── Weekly LEAN throughput ──
        all_lean = in_flight + closed
        throughput = self._bucket_by_week([
            {"created": t["created"], "updated": t["updated"], "resolution": t["resolution"]}
            for t in all_lean
        ])

        # ── Per-project operational snapshots (HELP / CUSTOMER / LEAN slides) ──
        project_snapshots: dict[str, Any] = {}
        _pks = ("HELP", "CUSTOMER", "LEAN")
        with ThreadPoolExecutor(max_workers=len(_pks)) as pool:
            future_to_pk = {pool.submit(self.get_project_operational_snapshot, pk): pk for pk in _pks}
            for fut in as_completed(future_to_pk):
                pk = future_to_pk[fut]
                try:
                    project_snapshots[pk] = fut.result()
                except Exception as e:
                    logger.warning("Project snapshot %s failed: %s", pk, e)
                    project_snapshots[pk] = {"error": str(e), "project_key": pk, "base_url": self.base_url}

        help_ticket_trends = self._get_help_ticket_volume_trends()

        try:
            from .eng_team_scorecard import build_eng_team_scorecard

            team_scorecard = build_eng_team_scorecard(self, days=days)
        except Exception as e:
            logger.warning("Team scorecard fetch failed: %s", e)
            team_scorecard = {"error": str(e), "teams": [], "summary": {}}

        try:
            from .eng_team_roster import build_eng_team_roster

            team_roster = build_eng_team_roster(self, timeout=60.0)
        except Exception as e:
            logger.warning("Team roster fetch failed: %s", e)
            team_roster = {"error": str(e), "teams": [], "total_engineers": 0}

        try:
            from .jira_sprint_story_points import get_sprint_story_points_history

            sprint_velocity = get_sprint_story_points_history(self, history_count=6, timeout=60.0)
        except Exception as e:
            logger.warning("Sprint story-point velocity fetch failed: %s", e)
            sprint_velocity = {"error": str(e), "boards": []}

        try:
            from .eng_bug_flow import build_eng_bug_flow

            bug_flow = build_eng_bug_flow(self, window_days=84, timeout=60.0)
        except Exception as e:
            logger.warning("Bug flow fetch failed: %s", e)
            bug_flow = {"error": str(e), "weeks": []}

        try:
            from .eng_epic_progress import build_eng_epic_progress

            epic_progress = build_eng_epic_progress(self, max_epics=8, timeout=60.0)
        except Exception as e:
            logger.warning("Epic progress fetch failed: %s", e)
            epic_progress = {"error": str(e), "epics": []}

        # ── Flow / bottleneck and planned-vs-unplanned signals (derived) ──
        # Tier 2: changelog-based time-in-status + Flagged signals feed the flow
        # computation directly (counts, selection, ranking, headline). Degrades to the
        # ``updated`` proxy if the changelog fetch fails.
        stage_age_by_key: dict[str, float] = {}
        flagged_keys: set[str] = set()
        status_flow: dict[str, Any] = {}
        try:
            status_flow, stage_age_by_key, flagged_keys = self._compute_changelog_signals(in_flight)
        except Exception as e:
            logger.warning("Flow changelog enrichment failed: %s", e)
        flow = compute_eng_flow(
            in_flight, closed,
            stage_age_by_key=stage_age_by_key or None,
            flagged_keys=flagged_keys or None,
        )
        if status_flow:
            flow["status_flow"] = status_flow
            flow["blocked_count"] = status_flow.get("blocked_count", flow.get("blocked_count", 0))
        work_split = compute_eng_work_split(
            in_flight, closed, escalated_to_eng=support_pressure.get("escalated_to_eng", 0)
        )

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
            "by_assignee_active": dict(sorted(by_assignee_active.items(), key=lambda x: -x[1])),
            "by_assignee_stale": dict(sorted(by_assignee_stale.items(), key=lambda x: -x[1])),
            "backlog_staleness": backlog_staleness,
            "themes": theme_summary,
            "open_bugs": open_bugs,
            "blocker_critical": blocker_critical,
            "velocity": velocity,
            "throughput": throughput,
            "enhancements": enhancements,
            "support_pressure": support_pressure,
            "project_snapshots": project_snapshots,
            "help_ticket_trends": help_ticket_trends,
            "team_scorecard": team_scorecard,
            "team_roster": team_roster,
            "sprint_velocity": sprint_velocity,
            "bug_flow": bug_flow,
            "epic_progress": epic_progress,
            "flow": flow,
            "work_split": work_split,
            "jql_queries": self._jql_since(jql_start),
        }

        # ── Generate per-slide "what this means" takeaways in parallel ──
        eng_data["takeaways"] = _generate_eng_takeaways(eng_data)
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


def reset_shared_jira_client() -> None:
    """Clear the process-wide singleton (tests and after .env changes)."""
    global _shared_jira_client
    with _SHARED_JIRA_CLIENT_LOCK:
        _shared_jira_client = None


def clear_atlassian_teams_cache_for_tests() -> None:
    with _ATLASSIAN_TEAMS_CACHE_LOCK:
        _ATLASSIAN_TEAMS_RESPONSE_CACHE.clear()


def get_shared_jira_client() -> JiraClient:
    """Return a process-wide singleton ``JiraClient`` (avoids repeated JSM org directory fetches)."""
    global _shared_jira_client
    with _SHARED_JIRA_CLIENT_LOCK:
        if _shared_jira_client is None:
            _shared_jira_client = JiraClient()
        return _shared_jira_client
