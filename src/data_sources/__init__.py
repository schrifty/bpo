"""Explicit datasource profiles and orchestrators.

Loaders and registry identifiers live here and must not depend on QBR or deck entrypoints.
"""

from __future__ import annotations

from .llm_export_report import build_llm_export_snapshot_report
from .profiles import (
    PROFILE_ID_LEANDNA_QBR_ENRICHMENTS,
    PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS,
    PROFILE_ID_SINGLE_CUSTOMER_HEALTH_CORE,
    PROFILE_LEANDNA_QBR_ENRICHMENTS,
    PROFILE_LLM_EXPORT_ALL_CUSTOMERS,
    PROFILE_SINGLE_CUSTOMER_HEALTH_CORE,
)
from .registry import SourceId

__all__ = (
    "SourceId",
    "PROFILE_ID_LLM_EXPORT_ALL_CUSTOMERS",
    "PROFILE_LLM_EXPORT_ALL_CUSTOMERS",
    "PROFILE_ID_LEANDNA_QBR_ENRICHMENTS",
    "PROFILE_LEANDNA_QBR_ENRICHMENTS",
    "PROFILE_ID_SINGLE_CUSTOMER_HEALTH_CORE",
    "PROFILE_SINGLE_CUSTOMER_HEALTH_CORE",
    "build_llm_export_snapshot_report",
)
