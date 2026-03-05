#!/usr/bin/env python3
"""Run the LangChain agent with Pendo API tools."""

import argparse

from src.agent import create_pendo_agent, run_agent
from src.config import logger


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LangChain agent with Pendo API - ask questions about usage data"
    )
    parser.add_argument(
        "query",
        nargs="?",
        default=None,
        help="Question to ask (e.g. 'Get usage for customer acme-123')",
    )
    parser.add_argument(
        "-m",
        "--model",
        default="openai:gpt-4o-mini",
        help="Model to use (default: openai:gpt-4o-mini)",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Run in interactive mode",
    )
    args = parser.parse_args()

    logger.info("Starting agent (model=%s)", args.model)
    agent = create_pendo_agent(model=args.model)

    if args.interactive:
        print("LangChain + Pendo agent. Type 'quit' to exit.\n")
        while True:
            try:
                query = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not query or query.lower() in ("quit", "exit", "q"):
                break
            logger.debug("User query: %s", query)
            result = run_agent(agent, query)
            messages = result.get("messages", [])
            if messages:
                last = messages[-1]
                content = getattr(last, "content", str(last))
                print(f"\nAgent: {content}\n")

    elif args.query:
        logger.debug("Query: %s", args.query)
        result = run_agent(agent, args.query)
        messages = result.get("messages", [])
        if messages:
            last = messages[-1]
            content = getattr(last, "content", str(last))
            print(content)
        else:
            print(result)
    else:
        parser.print_help()
        print("\nExample: python main.py 'Get usage data for customer acme-123'")


if __name__ == "__main__":
    main()
