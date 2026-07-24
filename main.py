#!/usr/bin/env python3
"""Run the LangChain agent with Pendo API tools."""

import argparse
import sys

from src.config import logger


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Pendo usage Q&A via a LangChain agent. "
            "Pass a question, use -i for interactive mode, or run with no args for help."
        )
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

    from src.agent import create_pendo_agent, run_agent

    logger.info("Starting agent (model=%s)", args.model)
    agent = create_pendo_agent(model=args.model)

    if args.interactive:
        print("LangChain + Pendo agent. Type 'quit' to exit.\n")
        while True:
            try:
                q = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not q or q.lower() in ("quit", "exit", "q"):
                break
            print(f"Agent: {run_agent(agent, q)}\n")
        return

    if not args.query:
        parser.print_help()
        sys.exit(0)

    print(run_agent(agent, args.query))


if __name__ == "__main__":
    main()
