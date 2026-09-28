"""Behavioral Pendo signals must not treat a failed fetch as zero usage."""

import logging

from src.pendo_client import PendoClient, logger


def _client() -> PendoClient:
    return PendoClient(integration_key="test-key", max_requests_per_minute=0)


def _signals(**overrides) -> list[str]:
    supplied = {
        "depth_data": {"write_ratio": 55, "collab_events": 0},
        "export_data": {"total_exports": 0},
        "kei_data": {"total_queries": 3, "unique_users": 1, "executive_users": 0},
        "guide_data": {"dismiss_rate": 0, "guide_reach": 90, "active_users": 1},
    }
    supplied.update(overrides)
    signals: list[str] = []
    _client()._add_behavioral_signals(signals, "Acme", 30, **supplied)
    return signals


def test_kei_error_is_unavailable_not_a_rollout_opportunity(caplog):
    with caplog.at_level(logging.WARNING, logger=logger.name):
        signals = _signals(kei_data={"error": "aggregation timeout"})
    assert any(s == "Pendo Kei unavailable: aggregation timeout" for s in signals)
    assert not any("No Kei AI usage" in s for s in signals)
    assert any(s.startswith("Deep write adoption") for s in signals)
    assert any("aggregation timeout" in rec.getMessage() for rec in caplog.records)


def test_depth_exception_is_unavailable_not_read_heavy(caplog):
    client = _client()

    def _boom(*_args, **_kwargs):
        raise RuntimeError("depth aggregation down")

    client.get_customer_depth = _boom  # type: ignore[method-assign]
    signals: list[str] = []
    with caplog.at_level(logging.WARNING, logger=logger.name):
        client._add_behavioral_signals(
            signals,
            "Acme",
            30,
            export_data={"total_exports": 0},
            kei_data={"total_queries": 0, "unique_users": 0, "executive_users": 0},
            guide_data={"dismiss_rate": 0, "guide_reach": 90, "active_users": 1},
        )
    assert any(s == "Pendo depth unavailable: depth aggregation down" for s in signals)
    assert not any("Read-heavy" in s for s in signals)
    assert any("depth aggregation down" in rec.getMessage() for rec in caplog.records)


def test_zero_kei_queries_still_reports_no_usage():
    signals = _signals(kei_data={"total_queries": 0, "unique_users": 0, "executive_users": 0})
    assert any(s.startswith("No Kei AI usage detected") for s in signals)
    assert not any("unavailable" in s for s in signals)


def test_kei_track_event_failure_with_no_queries_is_unavailable():
    signals = _signals(
        kei_data={"total_queries": 0, "unique_users": 0, "track_events_error": "track events 500"}
    )
    assert any(s == "Pendo Kei unavailable: track events 500" for s in signals)
    assert not any("No Kei AI usage" in s for s in signals)
