"""LangChain tools for Jira Cloud (operational metrics)."""

import json

from langchain_core.tools import BaseTool

from ..config import logger


class JiraProjectSnapshotTool(BaseTool):
    """Expose per-project Jira metrics for agents and ad-hoc analysis."""

    name: str = "jira_project_snapshot"
    description: str = (
        "Fetch Jira operational metrics for a project key (e.g. HELP, CUSTOMER, LEAN). "
        "Returns: open_count, by_status_open (histogram of open tickets by status), "
        "median_open_age_days, avg_resolved_cycle_days (for tickets resolved in last 6 months), "
        "resolved_in_6mo_count, assignee_resolved_table (top assignees with cumulative "
        "resolved counts for 2w / 1m / 3m / 6m windows). Input: single project key string."
    )

    def _run(self, query: str) -> str:
        try:
            from ..jira_client import _validate_project_key, get_shared_jira_client

            try:
                pk = _validate_project_key(query)
            except ValueError as e:
                return json.dumps({"error": str(e)})
            data = get_shared_jira_client().get_project_operational_snapshot(pk)
            return json.dumps(data, indent=2)
        except ValueError as e:
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.warning("jira_project_snapshot failed: %s", e)
            return json.dumps({"error": str(e)})

    async def _arun(self, query: str) -> str:
        raise NotImplementedError
