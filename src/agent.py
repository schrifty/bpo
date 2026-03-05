"""LangChain agent with Pendo API tools."""

from typing import Any

from langchain.agents import create_agent
from langchain_core.messages import HumanMessage

from .callbacks import ToolLifecycleCallback
from .config import PENDO_INTEGRATION_KEY, logger
from .tools import get_pendo_tools


def create_pendo_agent(
    model: str = "openai:gpt-4o-mini",
    integration_key: str | None = None,
    system_prompt: str | None = None,
) -> Any:
    """Create a LangChain agent with Pendo API tools.

    Args:
        model: Model string (e.g. 'openai:gpt-4o-mini', 'anthropic:claude-sonnet-4').
        integration_key: Pendo integration key. Defaults to PENDO_INTEGRATION_KEY from config.
        system_prompt: Optional system prompt for the agent.

    Returns:
        Configured agent that can invoke Pendo tools.
    """
    key = integration_key or PENDO_INTEGRATION_KEY
    tools = get_pendo_tools(integration_key=key)
    logger.debug("Creating agent with model=%s, %d tools", model, len(tools))

    default_prompt = (
        "You are a Customer Success analyst with access to Pendo product analytics. "
        "You help CSMs understand account health, identify churn risk, and prepare QBR decks.\n\n"
        "DATA TOOLS (each returns a summary you can interpret and act on):\n"
        "- list_customers: Portfolio overview — all customers ranked by size and activity vs peer median\n"
        "- customer_health: Health summary — engagement tiers, role breakdown, benchmarks, auto-detected signals\n"
        "- customer_sites: Per-site metrics — visitors, events, minutes, last active for each site\n"
        "- customer_features: Top pages and features — what product value they extract\n"
        "- customer_people: Champions (most active) and at-risk users (dormant) with roles\n"
        "- customer_depth: Behavioral depth — read/write/collab breakdown across all feature categories\n"
        "- customer_exports: Export behavior — total exports, by feature, per user, top exporters\n"
        "- customer_kei: Kei AI chatbot usage — adoption rate, executive usage (strategic priority)\n"
        "- customer_guides: Guide engagement — seen/dismissed/advanced rates, onboarding effectiveness\n\n"
        "MANIFEST & RECIPE TOOLS:\n"
        "- list_deck_types: See available deck types (CS Health Review, Product Adoption, Executive Summary, etc)\n"
        "- get_deck_manifest: Load a specific deck type for a customer. Returns the purpose, audience,\n"
        "  and full slide plan with recipe prompts, data tools, and override rules.\n"
        "- get_slide_recipes: Low-level — all recipes without manifest filtering. Rarely needed.\n\n"
        "SLIDE TOOLS:\n"
        "- create_deck: Create an empty presentation (pass customer, days, and deck name from manifest)\n"
        "- add_slide: Add one slide. Standard types: title, health, engagement, sites, features,\n"
        "  champions, benchmarks, exports, depth, kei, guides, signals.\n"
        "  Custom type: pass {title, sections: [{header, body}]} as the data.\n\n"
        "BUILDING A DECK:\n"
        "1. If no deck type is specified, call list_deck_types to show options\n"
        "2. Call get_deck_manifest with the chosen manifest and customer\n"
        "3. Read the manifest's purpose — it explains the audience and narrative arc\n"
        "4. For each slide in the plan, call the data tools it specifies\n"
        "5. Interpret the data through the lens of each recipe's prompt AND the manifest's purpose\n"
        "6. Create a deck (use the manifest name as the deck_name), then add slides in order\n"
        "7. Required slides MUST appear even if data is sparse. Excluded slides MUST NOT appear.\n"
        "8. For custom recipes, compose the content yourself and use slide_type 'custom'\n\n"
        "When analyzing customers, focus on actionable insights: churn risk signals, "
        "expansion opportunities, executive engagement, dormancy patterns, and feature adoption gaps."
    )

    return create_agent(
        model=model,
        tools=tools,
        system_prompt=system_prompt or default_prompt,
    )


def run_agent(agent: Any, query: str) -> Any:
    """Run the agent with a user query.

    Args:
        agent: The created agent.
        query: User question or request.

    Returns:
        Agent response.
    """
    logger.debug("Invoking agent with query: %s", query[:100] + "..." if len(query) > 100 else query)
    result = agent.invoke(
        {"messages": [HumanMessage(content=query)]},
        config={"callbacks": [ToolLifecycleCallback()]},
    )
    logger.debug("Agent returned %d messages", len(result.get("messages", [])))
    return result
