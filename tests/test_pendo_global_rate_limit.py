"""Tests for EFS-backed cross-process Pendo rate limiting."""

from __future__ import annotations

import threading
import time

import pytest

from src import config as cfg
from src.pendo_global_rate_limit import acquire_global_pendo_token, clear_global_pendo_rate_limit_for_tests


@pytest.fixture(autouse=True)
def _clean_global_state(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setattr(cfg, "CORTEX_CACHE_ROOT", tmp_path, raising=False)
    monkeypatch.setattr(cfg, "CORTEX_PENDO_GLOBAL_RATE_LIMIT", True, raising=False)
    clear_global_pendo_rate_limit_for_tests()
    yield
    clear_global_pendo_rate_limit_for_tests()


def test_global_rate_limit_disabled_returns_immediately(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cfg, "CORTEX_PENDO_GLOBAL_RATE_LIMIT", False, raising=False)
    start = time.monotonic()
    waited = acquire_global_pendo_token(rate_per_sec=1.0, capacity=1.0)
    assert waited == 0.0
    assert time.monotonic() - start < 0.05


def test_global_rate_limit_serializes_across_threads() -> None:
    # capacity 1, low rate → second acquire in another thread must wait.
    counter = {"n": 0}
    lock = threading.Lock()

    def worker():
        acquire_global_pendo_token(rate_per_sec=10.0, capacity=1.0)
        with lock:
            counter["n"] += 1

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    start = time.monotonic()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert counter["n"] == 2
    assert time.monotonic() - start >= 0.05
