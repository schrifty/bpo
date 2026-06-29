"""Suppress noisy third-party warnings on CLI runs (system Python 3.9 + urllib3/OpenSSL)."""

from __future__ import annotations

import warnings


def apply_cli_warning_filters() -> None:
    """Call before ``requests`` / ``google.*`` imports (idempotent)."""
    warnings.filterwarnings(
        "ignore",
        message=r".*urllib3 v2 only supports OpenSSL.*",
        category=Warning,
    )
    warnings.filterwarnings(
        "ignore",
        message=r"You are using a Python version 3\.9 past its end of life.*",
        category=FutureWarning,
    )
    warnings.filterwarnings(
        "ignore",
        message=r"You are using a non-supported Python version.*",
        category=FutureWarning,
    )
