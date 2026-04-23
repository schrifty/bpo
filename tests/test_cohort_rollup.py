"""Unit tests for portfolio cohort rollup (no Pendo)."""

from unittest.mock import patch

import pytest

from src.pendo_client import compute_cohort_portfolio_rollup
from src.slide_loader import cohort_findings_min_customers_for_cross_cohort_compare


def _row(
    customer: str,
    *,
    login_pct: float = 50.0,
    write_ratio: float = 0.2,
    score: float = 70.0,
    exports: float = 10.0,
    kei_queries: int = 0,
    active_users: int = 5,
    total_users: int = 10,
) -> dict:
    return {
        "customer": customer,
        "login_pct": login_pct,
        "depth": {"write_ratio": write_ratio},
        "score": score,
        "exports": {"total_exports": exports},
        "kei": {"total_queries": kei_queries},
        "active_users": active_users,
        "total_users": total_users,
    }


@pytest.mark.slow
def test_compute_cohort_portfolio_rollup_buckets_and_medians():
    summaries = [
        _row("A", login_pct=80, write_ratio=0.3, score=90, kei_queries=1),
        _row("B", login_pct=60, write_ratio=0.1, score=70, kei_queries=0),
        _row("C", login_pct=40, write_ratio=0.2, score=50, kei_queries=0),
        _row("D", login_pct=20, write_ratio=0.05, score=30, kei_queries=0),
    ]

    def fake_cohort(name: str):
        cohorts = {"A": "alpha", "B": "alpha", "C": "beta", "D": ""}
        cid = cohorts.get(name, "")
        return {"cohort": cid} if cid else {}

    with patch("src.pendo_client.get_customer_cohort", side_effect=fake_cohort):
        digest, bullets = compute_cohort_portfolio_rollup(summaries)

    assert digest["alpha"]["n"] == 2
    assert digest["beta"]["n"] == 1
    assert digest["unclassified"]["n"] == 1
    assert digest["alpha"]["median_login_pct"] == 70.0
    assert digest["beta"]["median_login_pct"] == 40.0
    assert digest["alpha"]["kei_adoption_pct"] == 50.0
    assert any("Portfolio (this window)" in b for b in bullets)
    assert any("Alpha" in b for b in bullets)
    assert any("unclassified" in b.lower() for b in bullets)


def test_compute_cohort_portfolio_rollup_empty():
    digest, bullets = compute_cohort_portfolio_rollup([])
    assert digest == {}
    assert bullets and "No customers" in bullets[0]


def test_cohort_findings_median_comparisons_only_when_two_buckets_meet_yaml_threshold():
    """Cross-cohort bullets require n≥rollup_params.min_customers_for_cross_cohort_compare per cohort."""
    min_n = cohort_findings_min_customers_for_cross_cohort_compare()
    summaries = [
        _row(f"A{i}", login_pct=80, write_ratio=0.5, exports=5.0) for i in range(min_n)
    ]
    summaries += [
        _row(f"B{i}", login_pct=10, write_ratio=0.05, exports=1.0) for i in range(min_n)
    ]

    def fake_cohort(name: str):
        return {"cohort": "alpha" if name.startswith("A") else "beta"}

    with patch("src.pendo_client.get_customer_cohort", side_effect=fake_cohort):
        _digest, bullets = compute_cohort_portfolio_rollup(summaries)
    joined = " ".join(bullets)
    assert "Widest spread" in joined
    assert "Write-heavy" in joined


def test_cohort_findings_skips_median_comparisons_when_buckets_below_yaml_threshold():
    min_n = cohort_findings_min_customers_for_cross_cohort_compare()
    below = max(0, min_n - 1)
    summaries = [
        _row(f"A{i}", login_pct=80, write_ratio=0.5) for i in range(below)
    ]
    summaries += [_row(f"B{i}", login_pct=10, write_ratio=0.05) for i in range(below)]

    def fake_cohort(name: str):
        return {"cohort": "alpha" if name.startswith("A") else "beta"}

    with patch("src.pendo_client.get_customer_cohort", side_effect=fake_cohort):
        _digest, bullets = compute_cohort_portfolio_rollup(summaries)
    joined = " ".join(bullets)
    assert "Widest spread" not in joined
    assert "Write-heavy" not in joined
    assert "Export volume" not in joined
    assert "Kei adoption gap" not in joined
