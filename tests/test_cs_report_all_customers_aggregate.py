"""Tests for aggregated CS Report load across all CSR customers."""

from __future__ import annotations

from src.cs_report_client import load_csr_all_customers_week


def test_load_csr_all_customers_week_empty_rows(monkeypatch):
    from src import cs_report_client as m

    monkeypatch.setattr(m, "_fetch_latest_report", lambda: [])
    out = load_csr_all_customers_week()
    assert out["platform_health"].get("error")
    assert out["supply_chain"].get("error")
    assert out["platform_value"].get("error")


def test_load_csr_all_customers_week_merges(monkeypatch):
    from src import cs_report_client as m

    monkeypatch.setattr(
        m,
        "_fetch_latest_report",
        lambda: [
            {"customer": "Acme", "delta": "week"},
            {"customer": "Beta", "delta": "week"},
        ],
    )

    def ph(cn: str):
        return {
            "customer": cn,
            "source": "cs_report",
            "factory_count": 2,
            "health_distribution": {"GREEN": 2},
            "total_shortages": 3,
            "total_critical_shortages": 1,
            "sites": [{"factory": "f1", "shortages": 3}],
        }

    def sc(cn: str):
        return {
            "customer": cn,
            "source": "cs_report",
            "factory_count": 2,
            "totals": {"on_hand": 100.0, "on_order": 50.0},
            "sites": [{"factory": "f1", "on_hand_value": 100}],
        }

    def pv(cn: str):
        return {
            "customer": cn,
            "source": "cs_report",
            "factory_count": 2,
            "total_savings": 10.0,
            "total_open_ia_value": 20.0,
            "total_potential_savings": 30.0,
            "total_potential_to_sell": 40.0,
            "total_recs_created_30d": 5,
            "total_pos_placed_30d": 6,
            "total_overdue_tasks": 7,
            "sites": [{"factory": "f1", "savings_current_period": 10}],
        }

    monkeypatch.setattr(m, "get_customer_platform_health", ph)
    monkeypatch.setattr(m, "get_customer_supply_chain", sc)
    monkeypatch.setattr(m, "get_customer_platform_value", pv)

    out = load_csr_all_customers_week()
    mph = out["platform_health"]
    assert mph["distinct_csr_customers"] == 2
    assert mph["factory_count"] == 4
    assert mph["total_shortages"] == 6
    assert mph["health_distribution"]["GREEN"] == 4
    assert mph["sites"][0]["csr_customer"] in ("Acme", "Beta")

    msc = out["supply_chain"]
    assert msc["totals"]["on_hand"] == 200
    assert msc["totals"]["on_order"] == 100

    mpv = out["platform_value"]
    assert mpv["total_savings"] == 20
    assert mpv["total_recs_created_30d"] == 10
