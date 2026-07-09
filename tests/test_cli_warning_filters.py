"""CLI warning filter helpers."""

from __future__ import annotations

import warnings

from src.cli_warning_filters import apply_cli_warning_filters


def test_apply_cli_warning_filters_registers_google_and_urllib3_filters() -> None:
    apply_cli_warning_filters()
    patterns = []
    for item in warnings.filters:
        if len(item) >= 2 and item[1] is not None:
            msg = item[1]
            patterns.append(msg.pattern if hasattr(msg, "pattern") else str(msg))
    joined = " ".join(patterns)
    assert "urllib3 v2 only supports OpenSSL" in joined
    assert "non-supported Python version" in joined
    assert "Python version 3.9 past its end of life" in joined
