"""LangChain tools for Pendo data and slide generation.

Tool design: each tool returns data at a level the agent can interpret and
reason about, while leaving narrative/sequencing decisions to the agent.
"""

import functools
import json
from typing import Any, ClassVar, Optional

from langchain_core.tools import BaseTool
from pydantic import Field
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout

from ..config import PENDO_BASE_URL, PENDO_INTEGRATION_KEY, logger
from ..pendo_client import PendoClient
from .jira_tool import JiraProjectSnapshotTool


def _client(integration_key: str | None = None, base_url: str | None = None) -> PendoClient:
    return PendoClient(
        integration_key=integration_key or PENDO_INTEGRATION_KEY,
        base_url=base_url or PENDO_BASE_URL,
    )


def _network_safe(fn):
    """Decorator: catch network errors and return a structured error to the agent."""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except (ReqConnectionError, Timeout, OSError) as e:
            err_type = type(e).__name__
            logger.warning("Tool %s network error: %s: %s", self.name, err_type, str(e)[:120])
            return json.dumps({"error": f"Network error ({err_type}): could not reach API. Retry or skip this customer."})
    return wrapper


# ── Base classes ──


class _PendoDataTool(BaseTool):
    """Base for Pendo tools that take 'customer' or 'customer,days' and call a PendoClient method."""

    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)
    _client_method: ClassVar[str]

    @_network_safe
    def _run(self, query: str) -> str:
        from ..quarters import resolve_quarter
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        if len(parts) > 1 and parts[1].isdigit():
            days = int(parts[1])
        else:
            days = resolve_quarter().days
        logger.info("Tool: %s | %s, %dd", self.name, customer, days)
        method = getattr(_client(self.integration_key, self.base_url), self._client_method)
        return json.dumps(method(customer, days), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class _CSReportTool(BaseTool):
    """Base for CS Report tools that take 'customer' and call a cs_report_client function."""

    _report_function: ClassVar[str]

    @_network_safe
    def _run(self, query: str) -> str:
        from .. import cs_report_client
        customer = query.strip().split(",")[0].strip()
        logger.info("Tool: %s | %s", self.name, customer)
        fn = getattr(cs_report_client, self._report_function)
        return json.dumps(fn(customer), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


# ── Pendo data tools ──


class CustomerHealthTool(_PendoDataTool):
    """Engagement summary, role breakdown, benchmarks, and auto-detected signals for a customer."""

    name: str = "customer_health"
    description: str = (
        "Get a health summary for a customer: engagement tiers (active/dormant), "
        "role breakdown, weekly active rate vs peer median, and auto-detected signals "
        "(dormancy, concentration risk, executive engagement, etc). "
        "Input: customer name (e.g. 'AGI') or 'customer,days' (e.g. 'AGI,30')."
    )
    _client_method = "get_customer_health"


class CustomerSitesTool(_PendoDataTool):
    """Per-site metrics for a customer."""

    name: str = "customer_sites"
    description: str = (
        "Get per-site metrics for a customer: visitors, page views, feature clicks, "
        "total events, minutes, and last active date for each site. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_sites"


class CustomerFeaturesTool(_PendoDataTool):
    """Top pages and features a customer uses."""

    name: str = "customer_features"
    description: str = (
        "Get top pages and features a customer uses, with human-readable names and event counts. "
        "Shows what product value the customer is extracting and what they're ignoring. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_features"


class CustomerPeopleTool(_PendoDataTool):
    """Champions and at-risk users for a customer."""

    name: str = "customer_people"
    description: str = (
        "Get up to 5 champions (most recently active) and up to 5 at-risk users "
        "(last login 2 wk–~6 mo ago, most recently active within that band first) "
        "for a customer, with email, role, and last visit date. "
        "Use this to identify who to protect and who to re-engage. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_people"


class CustomerDepthTool(_PendoDataTool):
    """Behavioral depth analysis: how a customer uses the product across read/write/collab."""

    name: str = "customer_depth"
    description: str = (
        "Get behavioral depth for a customer: events broken down by category "
        "(collaboration, upload, inline editing, task management, filtering, drilldown, search, "
        "export, widget config, share/save, Kei AI, etc). Shows read vs write ratio — "
        "high write ratio means they run their supply chain in LeanDNA, not just read dashboards. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_depth"


class CustomerExportsTool(_PendoDataTool):
    """Export behavior analysis for a customer."""

    name: str = "customer_exports"
    description: str = (
        "Get export behavior for a customer: total exports, exports per active user, "
        "breakdown by feature (e.g. 'CTB: Export to Excel'), and top exporter users. "
        "High export volume can signal deep engagement or that users are working outside "
        "the product. Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_exports"


class CustomerKeiTool(_PendoDataTool):
    """Kei AI chatbot usage analysis."""

    name: str = "customer_kei"
    description: str = (
        "Get Kei AI chatbot usage for a customer: total queries, unique users, adoption rate, "
        "and critically whether executives are using it. Kei adoption is a leading indicator "
        "of strategic engagement and executive pull-through. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_kei"


class CustomerGuidesTool(_PendoDataTool):
    """Guide engagement analysis for a customer."""

    name: str = "customer_guides"
    description: str = (
        "Get in-app guide engagement for a customer: guides seen, advanced, dismissed, "
        "dismiss rate, and which guides get the most interaction. High dismiss rates "
        "signal onboarding friction. Low reach means users aren't hitting guided workflows. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    _client_method = "get_customer_guides"


class CustomerPollEventsTool(_PendoDataTool):
    """NPS and poll responses from Pendo pollEvents."""

    name: str = "customer_poll_events"
    description: str = (
        "Get NPS and poll/survey responses for a customer from Pendo pollEvents: "
        "response counts, poll types, median/average NPS when available. "
        "Input: customer name or 'customer,days'."
    )
    _client_method = "get_customer_poll_events"


class CustomerFrustrationTool(_PendoDataTool):
    """Rage clicks, dead clicks, error clicks, U-turns aggregated per page/feature."""

    name: str = "customer_frustration"
    description: str = (
        "Get UX frustration signals for a customer: rage clicks, dead clicks, error clicks, "
        "and U-turns summed from page and feature events, with top pages and features. "
        "Input: customer name or 'customer,days'."
    )
    _client_method = "get_customer_frustration_signals"


class CustomerTrackEventsBreakdownTool(_PendoDataTool):
    """Custom track events (pendo.track) grouped by track type name."""

    name: str = "customer_track_events_breakdown"
    description: str = (
        "Break down custom track events for a customer by track type name (web/ios/android "
        "classes used in-app): events, minutes, unique users per track name. "
        "Input: customer name or 'customer,days'."
    )
    _client_method = "get_customer_track_events_breakdown"


class CustomerVisitorLanguagesTool(_PendoDataTool):
    """Visitor UI language distribution from Pendo metadata."""

    name: str = "customer_visitor_languages"
    description: str = (
        "Get distribution of visitor languages (metadata.agent.language) for a customer's "
        "users. Input: customer name or 'customer,days'."
    )
    _client_method = "get_customer_visitor_languages"


def _json_truncate_list(obj: Any, max_items: int = 40) -> Any:
    """Truncate large lists in catalog payloads for LLM tool output."""
    if isinstance(obj, list) and len(obj) > max_items:
        return {"_truncated": True, "_total": len(obj), "items": obj[:max_items]}
    return obj


class PendoAccountsTool(BaseTool):
    """Subscription account list from aggregation ``accounts`` source."""

    name: str = "pendo_accounts"
    description: str = (
        "List all Pendo accounts (aggregation accounts source): account ids and metadata. "
        "Input: empty string or 'list'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    @_network_safe
    def _run(self, query: str = "") -> str:
        logger.info("Tool: pendo_accounts")
        data = _client(self.integration_key, self.base_url).list_accounts()
        results = data.get("results") if isinstance(data, dict) else None
        if isinstance(results, list):
            data = {
                **data,
                "results": _json_truncate_list(results, max_items=40),
            }
        return json.dumps(data, indent=2)

    async def _arun(self, query: str = "") -> str:
        raise NotImplementedError


class _PendoCatalogListTool(BaseTool):
    """Fetch a Pendo REST catalog list (track types, reports, or segments)."""

    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)
    _client_method: ClassVar[str]

    @_network_safe
    def _run(self, query: str = "") -> str:
        logger.info("Tool: %s", self.name)
        method = getattr(_client(self.integration_key, self.base_url), self._client_method)
        data = method()
        return json.dumps(_json_truncate_list(data, max_items=40), indent=2)

    async def _arun(self, query: str = "") -> str:
        raise NotImplementedError


class PendoTracktypeCatalogTool(_PendoCatalogListTool):
    """Track event type catalog (GET /tracktype)."""

    name: str = "pendo_tracktype_catalog"
    description: str = (
        "List Pendo track event type definitions (names, ids, rules). Does not include usage counts. "
        "Input: empty."
    )
    _client_method = "get_tracktype_catalog_list"


class PendoReportCatalogTool(_PendoCatalogListTool):
    """Saved report definitions (GET /report); not report results."""

    name: str = "pendo_report_catalog"
    description: str = (
        "List saved Pendo report definitions (paths, funnels, etc.). API returns definitions only, "
        "not computed results. Input: empty."
    )
    _client_method = "get_report_catalog_list"


class PendoSegmentCatalogTool(_PendoCatalogListTool):
    """Segment definitions (GET /segment)."""

    name: str = "pendo_segment_catalog"
    description: str = (
        "List Pendo audience segment definitions. Input: empty."
    )
    _client_method = "get_segment_catalog_list"


class _PendoSchemaTool(BaseTool):
    """Visitor or account metadata schema from Pendo REST."""

    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)
    _client_method: ClassVar[str]

    @_network_safe
    def _run(self, query: str = "") -> str:
        logger.info("Tool: %s", self.name)
        method = getattr(_client(self.integration_key, self.base_url), self._client_method)
        return json.dumps(method(), indent=2)

    async def _arun(self, query: str = "") -> str:
        raise NotImplementedError


class PendoVisitorMetadataSchemaTool(_PendoSchemaTool):
    """Configured visitor metadata field definitions."""

    name: str = "pendo_visitor_metadata_schema"
    description: str = (
        "Get Pendo visitor metadata schema (groups, field types, display names). "
        "Input: empty."
    )
    _client_method = "get_metadata_schema_visitor_raw"


class PendoAccountMetadataSchemaTool(_PendoSchemaTool):
    """Configured account metadata field definitions."""

    name: str = "pendo_account_metadata_schema"
    description: str = (
        "Get Pendo account metadata schema (groups, field types, display names). "
        "Input: empty."
    )
    _client_method = "get_metadata_schema_account_raw"


class ListCustomersTool(_PendoDataTool):
    """Portfolio overview: all customers ranked with activity stats."""

    name: str = "list_customers"
    description: str = (
        "Get all customers ranked by size and activity. Each customer shows: "
        "total users, active users (7d), weekly active rate, and how they compare "
        "to the peer median. Use this to decide which customers need attention. "
        "Input: days (e.g. '30') or empty for default 30 days."
    )
    _client_method = "list_customers"

    @_network_safe
    def _run(self, query: str = "") -> str:
        from ..quarters import resolve_quarter
        days = int(query.strip()) if query.strip().isdigit() else resolve_quarter().days
        logger.info("Tool: list_customers | %dd", days)
        return json.dumps(
            _client(self.integration_key, self.base_url).list_customers(days),
            indent=2,
        )


# ── Deck & slide tools ──


class ListDeckTypesTool(BaseTool):
    """List available deck types."""

    name: str = "list_deck_types"
    description: str = (
        "List all available deck types. Each type targets a different audience "
        "(e.g. 'Customer Success Health Review', 'Product Adoption Review', "
        "'Executive Summary') with a different slide lineup and purpose. "
        "Call this to see what deck types exist before choosing one. "
        "Input: empty string or 'list'."
    )

    def _run(self, query: str = "") -> str:
        from ..deck_loader import list_decks
        logger.info("Tool: list_deck_types")
        return json.dumps(list_decks(), indent=2)

    async def _arun(self, query: str = "") -> str:
        raise NotImplementedError


class GetDeckDefinitionTool(BaseTool):
    """Load a deck definition with resolved slide definitions for a customer."""

    name: str = "get_deck_definition"
    description: str = (
        "Load a specific deck definition for a customer. Returns the deck's purpose, "
        "audience, and the full slide plan: each slide's prompt, data tools, "
        "whether it's required, and any override notes. "
        "This replaces get_slide_definitions — use this instead. "
        "Input: 'deck_id,customer' (e.g. 'cs_health_review,AGI'). "
        "Use list_deck_types first to see available deck IDs."
    )

    def _run(self, query: str) -> str:
        from ..deck_loader import resolve_deck
        parts = [p.strip() for p in query.split(",")]
        if len(parts) < 2:
            return json.dumps({"error": "Input must be 'deck_id,customer' (e.g. 'cs_health_review,AGI')"})
        deck_id = parts[0]
        customer = parts[1]
        logger.info("Tool: get_deck_definition | %s for %s", deck_id, customer)
        return json.dumps(resolve_deck(deck_id, customer), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class GetSlideDefinitionsTool(BaseTool):
    """Load slide definitions for a customer (low-level, prefer get_deck_definition)."""

    name: str = "get_slide_definitions"
    description: str = (
        "Get the raw list of all slide definitions for a customer, without deck filtering. "
        "Prefer get_deck_definition instead — it applies the right slide lineup and overrides "
        "for a specific deck type. Use this only if you need to see ALL available slides. "
        "Input: customer name (e.g. 'AGI')."
    )

    def _run(self, query: str) -> str:
        from ..slide_loader import get_slide_prompts
        customer = query.strip()
        logger.info("Tool: get_slide_definitions | %s", customer)
        return json.dumps(get_slide_prompts(customer), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


# ── Slide tools (composable, agent decides structure and order) ──


class CreateDeckTool(BaseTool):
    """Create an empty Google Slides deck."""

    name: str = "create_deck"
    description: str = (
        "Create a new empty Google Slides presentation. Returns a deck_id and URL. "
        "Use this first, then add slides with add_slide. "
        "Input: 'customer' or 'customer,days' or 'customer,days,deck_name'. "
        "The deck_name comes from the deck definition (e.g. 'Product Adoption Review')."
    )

    @_network_safe
    def _run(self, query: str) -> str:
        from ..slides_client import create_empty_deck
        from ..quarters import resolve_quarter
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else resolve_quarter().days
        deck_name = parts[2] if len(parts) > 2 else None
        logger.info("Tool: create_deck | %s, %dd, %s", customer, days, deck_name)
        return json.dumps(create_empty_deck(customer, days, deck_name), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class AddSlideTool(BaseTool):
    """Add a single slide to an existing deck."""

    name: str = "add_slide"
    description: str = (
        "Add one slide to a deck. Input is JSON: "
        '{"deck_id": "...", "slide_type": "...", "data": {...}}. '
        "Slide types: title, health, engagement, sites, features, champions, benchmarks, "
        "exports, depth, kei, guides, custom, signals, team, jira, sla_health, engineering, "
        "enhancements, cross_validation, platform_health, supply_chain, platform_value. "
        "For standard types, pass the data tool output as the 'data' field. "
        "For 'custom' type, pass {\"title\": \"...\", \"sections\": [{\"header\": \"...\", \"body\": \"...\"}]}. "
        "Up to 3 sections are rendered as columns. "
        "You control the slide order — add them in whatever sequence tells the best story."
    )

    @_network_safe
    def _run(self, query: str) -> str:
        from ..slides_client import add_slide
        try:
            args = json.loads(query)
        except json.JSONDecodeError:
            return json.dumps({"error": "Input must be valid JSON with deck_id, slide_type, and data"})

        deck_id = args.get("deck_id", "")
        slide_type = args.get("slide_type", "")
        data = args.get("data", {})

        if not deck_id or not slide_type:
            return json.dumps({"error": "deck_id and slide_type are required"})

        logger.info("Tool: add_slide | %s -> %s", slide_type, deck_id[:20])
        return json.dumps(add_slide(deck_id, slide_type, data), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


# ── CS Report tools (platform metrics from Data Exports drive) ──


class CustomerPlatformHealthTool(_CSReportTool):
    """Platform health scores, component availability, and shortages from the CS Report."""

    name: str = "customer_platform_health"
    description: str = (
        "Get platform health data from the Customer Success Report: health scores (GREEN/YELLOW/RED), "
        "clear-to-build %, clear-to-commit %, component availability, shortage counts, and "
        "buyer mapping quality per factory/site. This is supply-chain operational health — "
        "complements Pendo app-usage engagement. "
        "Input: customer name (e.g. 'Daikin')."
    )
    _report_function = "get_customer_platform_health"


class CustomerSupplyChainTool(_CSReportTool):
    """Inventory values, DOI, excess, and shortage details from the CS Report."""

    name: str = "customer_supply_chain"
    description: str = (
        "Get supply chain metrics from the Customer Success Report: on-hand and on-order "
        "inventory values, days of inventory (DOI), excess inventory, late POs/PRs, and "
        "days coverage per factory/site. Shows the dollar context behind product usage. "
        "Input: customer name (e.g. 'Daikin')."
    )
    _report_function = "get_customer_supply_chain"


class CustomerPlatformValueTool(_CSReportTool):
    """ROI metrics: savings achieved, open pipeline, operational throughput from the CS Report."""

    name: str = "customer_platform_value"
    description: str = (
        "Get platform ROI/value metrics from the Customer Success Report: inventory action "
        "savings achieved, open IA value pipeline, recommendations created, POs placed, "
        "overdue workbench tasks, potential savings, and FY spend per factory/site. "
        "This is the hard-dollar proof of value for renewals. "
        "Input: customer name (e.g. 'Daikin')."
    )
    _report_function = "get_customer_platform_value"


# ── Full deck generation ──


class GenerateFullDeckTool(BaseTool):
    """Generate a complete deck in one shot — fetches data, builds all slides, creates the presentation."""

    name: str = "generate_full_deck"
    description: str = (
        "Generate a complete Google Slides deck for a customer in one step. "
        "This is the FASTEST way to create a deck — it fetches all Pendo data, "
        "builds every slide from the deck definition, and creates the presentation "
        "in a single batch API call. Use this instead of create_deck + add_slide. "
        "Input: 'customer,deck_id' or 'customer,deck_id,quarter' or 'customer,deck_id,days'. "
        "Quarter examples: 'Daikin,cs_health_review,Q1 2026' or 'Daikin,cs_health_review,prev'. "
        "If omitted, defaults to the current/previous quarter automatically. "
        "Use list_deck_types first to see available deck IDs."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    @_network_safe
    def _run(self, query: str) -> str:
        from ..slides_client import create_health_deck
        from ..quarters import resolve_quarter
        parts = [p.strip() for p in query.split(",")]
        if len(parts) < 2:
            return json.dumps({"error": "Input must be 'customer,deck_id' (e.g. 'Daikin,cs_health_review')"})
        customer = parts[0]
        deck_id = parts[1]

        if len(parts) > 2 and parts[2].isdigit():
            days = int(parts[2])
            qr = None
        else:
            qr = resolve_quarter(parts[2] if len(parts) > 2 else None)
            days = qr.days

        logger.info("Tool: generate_full_deck | %s, %s, %dd, %s", customer, deck_id, days, qr.label if qr else "no quarter")
        client = _client(self.integration_key, self.base_url)
        report = client.get_customer_health_report(customer, days=days)
        if "error" in report:
            return json.dumps({"error": report["error"]})
        if qr:
            report["quarter"] = qr.label
            report["quarter_start"] = qr.start.isoformat()
            report["quarter_end"] = qr.end.isoformat()
        return json.dumps(create_health_deck(report, deck_id=deck_id), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


# ── Tool registry ──

def get_pendo_tools(
    integration_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> list[BaseTool]:
    """Return all tools for use with a LangChain agent."""
    common = {"integration_key": integration_key, "base_url": base_url}
    return [
        # Pendo data (interpretable summaries)
        CustomerHealthTool(**common),
        CustomerSitesTool(**common),
        CustomerFeaturesTool(**common),
        CustomerPeopleTool(**common),
        CustomerDepthTool(**common),
        CustomerExportsTool(**common),
        CustomerKeiTool(**common),
        CustomerGuidesTool(**common),
        CustomerPollEventsTool(**common),
        CustomerFrustrationTool(**common),
        CustomerTrackEventsBreakdownTool(**common),
        CustomerVisitorLanguagesTool(**common),
        PendoAccountsTool(**common),
        PendoTracktypeCatalogTool(**common),
        PendoReportCatalogTool(**common),
        PendoSegmentCatalogTool(**common),
        PendoVisitorMetadataSchemaTool(**common),
        PendoAccountMetadataSchemaTool(**common),
        ListCustomersTool(**common),
        JiraProjectSnapshotTool(),
        # Decks & slides
        ListDeckTypesTool(),
        GetDeckDefinitionTool(),
        GetSlideDefinitionsTool(),
        # CS Report (platform metrics from Data Exports)
        CustomerPlatformHealthTool(),
        CustomerSupplyChainTool(),
        CustomerPlatformValueTool(),
        # Full deck generation (preferred — one call does everything)
        GenerateFullDeckTool(**common),
        # Slides (composable — for custom/advanced use)
        CreateDeckTool(),
        AddSlideTool(),
    ]
