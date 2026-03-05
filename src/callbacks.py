"""Callback handlers for agent tool lifecycle logging."""

from typing import Any

from langchain_core.agents import AgentAction
from langchain_core.callbacks import BaseCallbackHandler

from .config import logger


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len] + "..."


class ToolLifecycleCallback(BaseCallbackHandler):
    """Logs tool choice and call at INFO level."""

    def on_agent_action(self, action: AgentAction, **kwargs: object) -> None:
        """Called when the agent chooses to use a tool."""
        logger.info("Tool chosen: %s | input: %s", action.tool, action.tool_input)

    def on_tool_start(self, serialized: dict, input_str: str, **kwargs: object) -> None:
        """Called when the agent chooses a tool (before execution)."""
        name = serialized.get("name", serialized.get("id", ["unknown"])[-1])
        if isinstance(name, list):
            name = name[-1] if name else "unknown"
        logger.info("Tool chosen: %s | input: %s", name, _truncate(str(input_str), 200))

    def on_tool_end(self, output: Any, **kwargs: object) -> None:
        """Called when a tool finishes."""
        out_len = len(str(output)) if output is not None else 0
        logger.info("Tool completed | output length: %d chars", out_len)
