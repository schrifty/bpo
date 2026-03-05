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
        "You are a helpful assistant with access to Pendo product analytics. "
        "You can fetch usage data from the Pendo API. "
        "Tools: pendo_get_visitors (aggregate visitor data), pendo_get_usage (usage for a customer), "
        "pendo_get_sites (list sites), pendo_get_usage_by_site (usage aggregated by site), pendo_get_all_sites_usage_report (all sites with usage, optional active_only), "
        "pendo_get_sites_by_customer (sites grouped by customer for pipeline: Google Slide per site, Slack per customer), pendo_get_sites_with_usage, pendo_get_usage_for_site, "
        "pendo_get_page_events (page views), pendo_get_feature_events (feature clicks), "
        "pendo_get_track_events (custom events), pendo_save_usage (save data to file), "
        "pendo_generate_slides (Google Slide deck per customer, one slide per site). "
        "When asked about usage, customers, sites, pages, features, or analytics, use these tools. "
        "IMPORTANT: Usage data means actual product engagement - page views, feature clicks, total events, minutes spent. "
        "When presenting visitor or customer data, ALWAYS prominently display the 'usage' metrics (page_views, feature_clicks, total_events, total_minutes, unique_pages, unique_features) for each visitor. "
        "Do not focus only on profile metadata (email, role, site) - the usage numbers are the key analytics."
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
