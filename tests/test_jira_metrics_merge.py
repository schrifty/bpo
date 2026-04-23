"""HELP ticket metrics merged-query partitioning."""

from datetime import datetime, timedelta, timezone

from src.jira_client import JiraClient


def _issue(status_cat: str, *, resolution=None, created="2025-06-01T10:00:00.000+0000", res_date=None):
    return {
        "fields": {
            "status": {"name": "Open", "statusCategory": {"key": status_cat}},
            "resolution": resolution,
            "created": created,
            "resolutiondate": res_date,
        }
    }


def test_partition_open_done_and_year_window():
    jc = JiraClient.__new__(JiraClient)  # skip __init__; ``_parse_jira_datetime`` only
    now = datetime.now(timezone.utc)
    recent_res = (now - timedelta(days=40)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    year_cre = (now - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    old_open_cre = (now - timedelta(days=400)).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    merged = [
        _issue("done", resolution={"name": "Done"}, created="2025-01-01T00:00:00.000+0000",
               res_date=recent_res),
        _issue("new", resolution=None, created=old_open_cre),
        _issue("new", resolution=None, created=year_cre),
    ]
    o, r, y, ry = jc._partition_help_metrics_merged(merged)
    assert len(o) == 2
    assert len(r) == 1
    assert len(y) == 1
    assert len(ry) == 1  # resolved in last 365d (superset of 180d bucket for issue 1)


def test_shared_jira_client_singleton():
    from src.jira_client import get_shared_jira_client

    try:
        a = get_shared_jira_client()
        b = get_shared_jira_client()
    except ValueError:
        # Jira env not configured in CI — skip
        return
    assert a is b
