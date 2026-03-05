"""LangChain tools for Pendo data and slide generation.

Tool design: each tool returns data at a level the agent can interpret and
reason about, while leaving narrative/sequencing decisions to the agent.
"""

import json
from typing import Any, Optional

from langchain_core.tools import BaseTool
from pydantic import Field

from ..config import PENDO_BASE_URL, PENDO_INTEGRATION_KEY, logger
from ..pendo_client import PendoClient


def _client(integration_key: str | None = None, base_url: str | None = None) -> PendoClient:
    return PendoClient(
        integration_key=integration_key or PENDO_INTEGRATION_KEY,
        base_url=base_url or PENDO_BASE_URL,
    )


# ── Data tools (interpretable summaries the agent can reason about) ──


class CustomerHealthTool(BaseTool):
    """Engagement summary, role breakdown, benchmarks, and auto-detected signals for a customer."""

    name: str = "customer_health"
    description: str = (
        "Get a health summary for a customer: engagement tiers (active/dormant), "
        "role breakdown, weekly active rate vs peer median, and auto-detected signals "
        "(dormancy, concentration risk, executive engagement, etc). "
        "Input: customer name (e.g. 'AGI') or 'customer,days' (e.g. 'AGI,30')."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_health | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_health(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerSitesTool(BaseTool):
    """Per-site metrics for a customer."""

    name: str = "customer_sites"
    description: str = (
        "Get per-site metrics for a customer: visitors, page views, feature clicks, "
        "total events, minutes, and last active date for each site. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_sites | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_sites(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerFeaturesTool(BaseTool):
    """Top pages and features a customer uses."""

    name: str = "customer_features"
    description: str = (
        "Get top pages and features a customer uses, with human-readable names and event counts. "
        "Shows what product value the customer is extracting and what they're ignoring. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_features | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_features(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerPeopleTool(BaseTool):
    """Champions and at-risk users for a customer."""

    name: str = "customer_people"
    description: str = (
        "Get champions (most active users) and at-risk users (30+ days inactive) "
        "for a customer, with email, role, and last visit date. "
        "Use this to identify who to protect and who to re-engage. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_people | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_people(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerDepthTool(BaseTool):
    """Behavioral depth analysis: how a customer uses the product across read/write/collab."""

    name: str = "customer_depth"
    description: str = (
        "Get behavioral depth for a customer: events broken down by category "
        "(collaboration, upload, inline editing, task management, filtering, drilldown, search, "
        "export, widget config, share/save, Kei AI, etc). Shows read vs write ratio — "
        "high write ratio means they run their supply chain in LeanDNA, not just read dashboards. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_depth | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_depth(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerExportsTool(BaseTool):
    """Export behavior analysis for a customer."""

    name: str = "customer_exports"
    description: str = (
        "Get export behavior for a customer: total exports, exports per active user, "
        "breakdown by feature (e.g. 'CTB: Export to Excel'), and top exporter users. "
        "High export volume can signal deep engagement or that users are working outside "
        "the product. Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_exports | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_exports(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerKeiTool(BaseTool):
    """Kei AI chatbot usage analysis."""

    name: str = "customer_kei"
    description: str = (
        "Get Kei AI chatbot usage for a customer: total queries, unique users, adoption rate, "
        "and critically whether executives are using it. Kei adoption is a leading indicator "
        "of strategic engagement and executive pull-through. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_kei | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_kei(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class CustomerGuidesTool(BaseTool):
    """Guide engagement analysis for a customer."""

    name: str = "customer_guides"
    description: str = (
        "Get in-app guide engagement for a customer: guides seen, advanced, dismissed, "
        "dismiss rate, and which guides get the most interaction. High dismiss rates "
        "signal onboarding friction. Low reach means users aren't hitting guided workflows. "
        "Input: customer name (e.g. 'AGI') or 'customer,days'."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        logger.info("Tool: customer_guides | %s, %dd", customer, days)
        result = _client(self.integration_key, self.base_url).get_customer_guides(customer, days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class ListCustomersTool(BaseTool):
    """Portfolio overview: all customers ranked with activity stats."""

    name: str = "list_customers"
    description: str = (
        "Get all customers ranked by size and activity. Each customer shows: "
        "total users, active users (7d), weekly active rate, and how they compare "
        "to the peer median. Use this to decide which customers need attention. "
        "Input: days (e.g. '30') or empty for default 30 days."
    )
    integration_key: Optional[str] = Field(default=None, exclude=True)
    base_url: Optional[str] = Field(default=None, exclude=True)

    def _run(self, query: str) -> str:
        days = int(query.strip()) if query.strip().isdigit() else 30
        logger.info("Tool: list_customers | %dd", days)
        result = _client(self.integration_key, self.base_url).list_customers(days)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


# ── Manifest & recipe tools (tell the agent what deck to build) ──


class ListDeckTypesTool(BaseTool):
    """List available deck types (manifests)."""

    name: str = "list_deck_types"
    description: str = (
        "List all available deck types. Each type targets a different audience "
        "(e.g. 'Customer Success Health Review', 'Product Adoption Review', "
        "'Executive Summary') with a different slide lineup and purpose. "
        "Call this to see what deck types exist before choosing one. "
        "Input: empty string or 'list'."
    )

    def _run(self, query: str) -> str:
        from ..manifest_loader import list_manifests
        logger.info("Tool: list_deck_types")
        return json.dumps(list_manifests(), indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class GetDeckManifestTool(BaseTool):
    """Load a deck manifest with resolved recipes for a customer."""

    name: str = "get_deck_manifest"
    description: str = (
        "Load a specific deck manifest for a customer. Returns the deck's purpose, "
        "audience, and the full slide plan: each slide's recipe prompt, data tools, "
        "whether it's required, and any override notes. "
        "This replaces get_slide_recipes — use this instead. "
        "Input: 'manifest_id,customer' (e.g. 'cs_health_review,AGI'). "
        "Use list_deck_types first to see available manifest IDs."
    )

    def _run(self, query: str) -> str:
        from ..manifest_loader import resolve_manifest
        parts = [p.strip() for p in query.split(",")]
        if len(parts) < 2:
            return json.dumps({"error": "Input must be 'manifest_id,customer' (e.g. 'cs_health_review,AGI')"})
        manifest_id = parts[0]
        customer = parts[1]
        logger.info("Tool: get_deck_manifest | %s for %s", manifest_id, customer)
        result = resolve_manifest(manifest_id, customer)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class GetSlideRecipesTool(BaseTool):
    """Load slide recipes for a customer (low-level, prefer get_deck_manifest)."""

    name: str = "get_slide_recipes"
    description: str = (
        "Get the raw list of all slide recipes for a customer, without manifest filtering. "
        "Prefer get_deck_manifest instead — it applies the right slide lineup and overrides "
        "for a specific deck type. Use this only if you need to see ALL available recipes. "
        "Input: customer name (e.g. 'AGI')."
    )

    def _run(self, query: str) -> str:
        from ..recipe_loader import get_recipe_prompts
        customer = query.strip()
        logger.info("Tool: get_slide_recipes | %s", customer)
        recipes = get_recipe_prompts(customer)
        return json.dumps(recipes, indent=2)

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
        "The deck_name comes from the manifest (e.g. 'Product Adoption Review')."
    )

    def _run(self, query: str) -> str:
        from ..slides_client import create_empty_deck
        parts = [p.strip() for p in query.split(",")]
        customer = parts[0]
        days = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 30
        deck_name = parts[2] if len(parts) > 2 else None
        logger.info("Tool: create_deck | %s, %dd, %s", customer, days, deck_name)
        result = create_empty_deck(customer, days, deck_name)
        return json.dumps(result, indent=2)

    async def _arun(self, query: str) -> str:
        raise NotImplementedError


class AddSlideTool(BaseTool):
    """Add a single slide to an existing deck."""

    name: str = "add_slide"
    description: str = (
        "Add one slide to a deck. Input is JSON: "
        '{"deck_id": "...", "slide_type": "...", "data": {...}}. '
        "Slide types: title, health, engagement, sites, features, champions, benchmarks, "
        "exports, depth, kei, guides, custom, signals. "
        "For standard types, pass the data tool output as the 'data' field. "
        "For 'custom' type, pass {\"title\": \"...\", \"sections\": [{\"header\": \"...\", \"body\": \"...\"}]}. "
        "Up to 3 sections are rendered as columns. "
        "You control the slide order — add them in whatever sequence tells the best story."
    )

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
        result = add_slide(deck_id, slide_type, data)
        return json.dumps(result, indent=2)

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
        # Data (interpretable summaries)
        CustomerHealthTool(**common),
        CustomerSitesTool(**common),
        CustomerFeaturesTool(**common),
        CustomerPeopleTool(**common),
        CustomerDepthTool(**common),
        CustomerExportsTool(**common),
        CustomerKeiTool(**common),
        CustomerGuidesTool(**common),
        ListCustomersTool(**common),
        # Manifests & recipes
        ListDeckTypesTool(),
        GetDeckManifestTool(),
        GetSlideRecipesTool(),
        # Slides (composable)
        CreateDeckTool(),
        AddSlideTool(),
    ]
