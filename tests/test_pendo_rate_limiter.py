"""Tests for the Pendo aggregation request pacing token bucket."""

import threading
import time

from src.pendo_client import _TokenBucket


def test_disabled_when_rate_non_positive():
    bucket = _TokenBucket(rate_per_sec=0, capacity=10)
    assert bucket.enabled is False
    start = time.monotonic()
    for _ in range(50):
        assert bucket.acquire() == 0.0
    # No pacing: 50 acquires should be effectively instant.
    assert time.monotonic() - start < 0.1


def test_burst_capacity_fires_immediately():
    bucket = _TokenBucket(rate_per_sec=1.0, capacity=5)
    start = time.monotonic()
    for _ in range(5):
        assert bucket.acquire() == 0.0
    # The full burst drains without blocking.
    assert time.monotonic() - start < 0.1


def test_sustained_requests_are_paced():
    # capacity 1 so only the first token is free; the rest wait ~1/rate each.
    bucket = _TokenBucket(rate_per_sec=20.0, capacity=1)
    bucket.acquire()  # consume the initial token
    start = time.monotonic()
    waited_total = 0.0
    for _ in range(4):
        waited_total += bucket.acquire()
    elapsed = time.monotonic() - start
    # 4 requests at 20/s should take roughly 4 * 0.05s = 0.2s.
    assert elapsed >= 0.15
    assert waited_total > 0


def test_thread_safe_under_concurrency():
    bucket = _TokenBucket(rate_per_sec=200.0, capacity=4)
    counter = {"n": 0}
    lock = threading.Lock()

    def worker():
        for _ in range(10):
            bucket.acquire()
            with lock:
                counter["n"] += 1

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All 80 acquisitions complete without deadlock or lost updates.
    assert counter["n"] == 80
