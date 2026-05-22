"""Support-kpis Notable Findings digest includes the enriched KPI bundle."""

from src.support_notable_llm import (
    SUPPORT_KPIS_NOTABLE_BULLET_COUNT,
    build_support_kpis_digest,
    generate_support_kpis_notable_bullets_via_llm,
)


def test_build_support_kpis_digest_includes_trimmed_kpi_bundle():
    kpis = {
        "open_count": 12,
        "window_days": 180,
        "intake_weekly": [{"week": "2026-W01", "created": 3}],
        "sla_by_window": {"90": {"ttfr": {"pct": 88.0, "met": 7, "measured": 8}}},
        "jql_queries": ["should not appear in digest"],
    }
    d = build_support_kpis_digest(
        {"customer": "Acme", "days": 180, "jira": {"support_kpis": kpis}},
        slide_titles=["Intake", "Backlog"],
    )
    sk = d.get("support_kpis") or {}
    assert sk.get("open_count") == 12
    assert sk.get("window_days") == 180
    assert "jql_queries" not in sk
    assert d.get("deck_slides_built") == ["Intake", "Backlog"]
    assert d.get("deck_type") == "support-kpis"


def test_support_kpis_notable_env_off_uses_yaml_fallback_items():
    import os

    old = os.environ.get("BPO_SUPPORT_NOTABLE_LLM")
    os.environ["BPO_SUPPORT_NOTABLE_LLM"] = "false"
    try:
        bullets, src = generate_support_kpis_notable_bullets_via_llm(
            build_support_kpis_digest({"jira": {"support_kpis": {"open_count": 1}}}, slide_titles=[]),
            {"notable_items": ["INTAKE: Custom fallback only."]},
        )
    finally:
        if old is None:
            os.environ.pop("BPO_SUPPORT_NOTABLE_LLM", None)
        else:
            os.environ["BPO_SUPPORT_NOTABLE_LLM"] = old
    assert src == "env_off"
    assert bullets == ["INTAKE: Custom fallback only."]
    assert len(bullets) <= SUPPORT_KPIS_NOTABLE_BULLET_COUNT
