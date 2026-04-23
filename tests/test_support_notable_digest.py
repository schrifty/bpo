"""Support review Notable digest: keys must match jira ``get_*_ticket_metrics`` return shape."""

from src.support_notable_llm import build_support_review_digest

_METRICS = {
    "customer": "Acme",
    "unresolved_count": 3,
    "resolved_in_6mo_count": 9,
    "ttfr_1y": {"median_sla_ms": 100},
    "ttr_1y": {"p50_sla_ms": 200},
    "sla_adherence_1y": 0.91,
    "by_type_open": {"Bug": 1, "Task": 2},
    "by_status_open": {"Open": 1, "In Progress": 2},
    "jsm_organizations_resolved": ["Acme", "ParentCo"],
    "jql_queries": ["ignore this in digest" * 20],
}


def test_digest_includes_kpi_keys_from_get_customer_ticket_metrics():
    d = build_support_review_digest(
        {
            "customer": "Acme",
            "jira": {"customer_ticket_metrics": {**_METRICS}},
        },
        slide_titles=["a"],
    )
    h = d.get("help_tickets_for_this_customer") or {}
    assert h.get("unresolved_count") == 3
    assert h.get("resolved_in_6mo_count") == 9
    assert h.get("ttfr_1y") == {"median_sla_ms": 100}
    assert h.get("ttr_1y") == {"p50_sla_ms": 200}
    assert h.get("sla_adherence_1y") == 0.91
    assert h.get("by_type_open") == {"Bug": 1, "Task": 2}
    assert h.get("by_status_open") == {"Open": 1, "In Progress": 2}
    assert h.get("jsm_organizations_resolved") == ["Acme", "ParentCo"]


def test_digest_project_metrics_use_same_key_shape():
    m = {**_METRICS}
    d = build_support_review_digest(
        {
            "jira": {
                "customer_project_ticket_metrics": {**m, "project": "CUSTOMER"},
                "lean_project_ticket_metrics": {**m, "project": "LEAN"},
            }
        },
        slide_titles=[],
    )
    cp = d.get("customer_project_ticket_metrics") or {}
    lp = d.get("lean_project_ticket_metrics") or {}
    assert cp.get("project") == "CUSTOMER" and cp.get("sla_adherence_1y") == 0.91
    assert lp.get("project") == "LEAN" and lp.get("unresolved_count") == 3
    assert "jql_queries" not in cp and "jql_queries" not in lp
