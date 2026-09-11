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


def get_engineering_escalation_rate(
    client: JiraClient,
    *,
    days: int = DEFAULT_SUPPORT_OPS_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """LEAN ``jira_escalated`` created ÷ HELP created (trailing window)."""
    return _escalation_rate_for_project(
        client,
        project="LEAN",
        rate_label="Engineering Escalation Rate",
        days=days,
        timeout=timeout,
    )


def get_data_escalation_rate(
    client: JiraClient,
    *,
    days: int = DEFAULT_SUPPORT_OPS_DAYS,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """CUSTOMER ``jira_escalated`` created ÷ HELP created (trailing window)."""
    return _escalation_rate_for_project(
        client,
        project="CUSTOMER",
        rate_label="Data Escalation Rate",
        days=days,
        timeout=timeout,
    )


def _escalation_rate_for_project(
    client: JiraClient,
    *,
    project: str,
    rate_label: str,
    days: int,
    timeout: float = 60.0,  # noqa: ARG001
) -> dict[str, Any]:
    window = max(1, int(days))
    help_created_jql = (
        f"project = HELP AND {_HELP_TRANSIENT} AND created >= -{window}d"
    )
    escalated_jql = (
        f'project = {project} AND labels = "jira_escalated" AND {_CUSTOMER_LEAN_EXCL} '
        f"AND created >= -{window}d"
    )

    help_created = _count_or_error(
        client, help_created_jql, label=f"HELP created last {window}d"
    )
    if help_created.get("error"):
        return help_created
    escalated = _count_or_error(
        client,
        escalated_jql,
        label=f"{project} jira_escalated created last {window}d",
    )
    if escalated.get("error"):
        return escalated

    help_n = int(help_created["value"])
    escalated_n = int(escalated["value"])
    if help_n <= 0:
        return {
            "error": (
                f"HELP created count is 0 in last {window}d — "
                f"cannot compute {rate_label}"
            ),
            "help_created": help_n,
            "escalated": escalated_n,
            "project": project,
            "window_days": window,
        }

    pct = round(100.0 * escalated_n / help_n, 2)
    logger.info(
        "%s: %s %s / %s HELP created = %s%% (window=%sd)",
        rate_label,
        escalated_n,
        project,
        help_n,
        pct,
        window,
    )
    return {
        "value": pct,
        "numerator": float(escalated_n),
        "denominator": float(help_n),
        "escalated": escalated_n,
        "help_created": help_n,
        "project": project,
        "window_days": window,
        "help_created_jql": help_created_jql,
        "escalated_jql": escalated_jql,
    }
