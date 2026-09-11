"""Operational HELP support KPIs for the metrics registry.

Includes trailing-window throughput (resolved/created) and escalation rate into
Engineering (LEAN) / Data Integration (CUSTOMER).
"""

from __future__ import annotations

import logging
from typing import Any

from .jira_client import JiraClient

logger = logging.getLogger("cortex")

DEFAULT_SUPPORT_OPS_DAYS = 30

_HELP_TRANSIENT = "NOT (labels = Outage OR labels = Healthcheck)"
_CUSTOMER_LEAN_EXCL = "issuetype not in (Epic, SUT)"


def _count_or_error(
    client: JiraClient,
    jql: str,
    *,
    label: str,
) -> dict[str, Any]:
    count = client.jql_match_count(jql, data_description=label)
    if count is None:
        return {
            "error": (
                f"Jira count unavailable for {label} "
                "(POST /rest/api/3/search/approximate-count returned no count)"
            ),
            "jql": jql,
        }
    return {"value": int(count), "jql": jql}


def get_help_resolved_created_ratio(
    client: JiraClient,
    *,
    days: int = DEFAULT_SUPPORT_OPS_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """HELP resolved ÷ created × 100 over a trailing window.

    Returns percent-scale ``value`` plus ``numerator``/``denominator`` counts.
    """
    window = max(1, int(days))
    created_jql = (
        f"project = HELP AND {_HELP_TRANSIENT} AND created >= -{window}d"
    )
    resolved_jql = (
        f"project = HELP AND {_HELP_TRANSIENT} AND resolution is not EMPTY "
        f"AND resolved >= -{window}d"
    )
    created = _count_or_error(
        client, created_jql, label=f"HELP created last {window}d"
    )
    if created.get("error"):
        return created
    resolved = _count_or_error(
        client, resolved_jql, label=f"HELP resolved last {window}d"
    )
    if resolved.get("error"):
        return resolved

    created_n = int(created["value"])
    resolved_n = int(resolved["value"])
    if created_n <= 0:
        return {
            "error": (
                f"HELP created count is 0 in last {window}d — "
                "cannot compute Resolved / Created"
            ),
            "created": created_n,
            "resolved": resolved_n,
            "window_days": window,
        }

    pct = round(100.0 * resolved_n / created_n, 2)
    logger.info(
        "HELP Resolved / Created: %s / %s = %s%% (window=%sd)",
        resolved_n,
        created_n,
        pct,
        window,
    )
    return {
        "value": pct,
        "numerator": float(resolved_n),
        "denominator": float(created_n),
        "resolved": resolved_n,
        "created": created_n,
        "window_days": window,
        "created_jql": created_jql,
        "resolved_jql": resolved_jql,
    }


def get_help_escalation_rate(
    client: JiraClient,
    *,
    days: int = DEFAULT_SUPPORT_OPS_DAYS,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    """LEAN+CUSTOMER ``jira_escalated`` created ÷ HELP created (trailing window)."""
    window = max(1, int(days))
    help_created_jql = (
        f"project = HELP AND {_HELP_TRANSIENT} AND created >= -{window}d"
    )
    lean_jql = (
        f'project = LEAN AND labels = "jira_escalated" AND {_CUSTOMER_LEAN_EXCL} '
        f"AND created >= -{window}d"
    )
    customer_jql = (
        f'project = CUSTOMER AND labels = "jira_escalated" AND {_CUSTOMER_LEAN_EXCL} '
        f"AND created >= -{window}d"
    )

    help_created = _count_or_error(
        client, help_created_jql, label=f"HELP created last {window}d"
    )
    if help_created.get("error"):
        return help_created
    lean = _count_or_error(
        client, lean_jql, label=f"LEAN jira_escalated created last {window}d"
    )
    if lean.get("error"):
        return lean
    customer = _count_or_error(
        client,
        customer_jql,
        label=f"CUSTOMER jira_escalated created last {window}d",
    )
    if customer.get("error"):
        return customer

    help_n = int(help_created["value"])
    lean_n = int(lean["value"])
    customer_n = int(customer["value"])
    escalated = lean_n + customer_n
    if help_n <= 0:
        return {
            "error": (
                f"HELP created count is 0 in last {window}d — "
                "cannot compute Escalation Rate"
            ),
            "help_created": help_n,
            "lean_escalated": lean_n,
            "customer_escalated": customer_n,
            "window_days": window,
        }

    pct = round(100.0 * escalated / help_n, 2)
    logger.info(
        "Escalation Rate: %s (LEAN %s + CUSTOMER %s) / %s HELP created = %s%% "
        "(window=%sd)",
        escalated,
        lean_n,
        customer_n,
        help_n,
        pct,
        window,
    )
    return {
        "value": pct,
        "numerator": float(escalated),
        "denominator": float(help_n),
        "escalated": escalated,
        "lean_escalated": lean_n,
        "customer_escalated": customer_n,
        "help_created": help_n,
        "window_days": window,
        "help_created_jql": help_created_jql,
        "lean_jql": lean_jql,
        "customer_jql": customer_jql,
    }
