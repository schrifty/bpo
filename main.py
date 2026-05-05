#!/usr/bin/env python3
"""Run the LangChain agent with Pendo API tools."""

import argparse
import sys

from src.config import logger


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] == "qbr":
        from src.qbr_template import run_qbr_cli

        run_qbr_cli(argv[1:], prog="python main.py qbr")
        return

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
        print("\nExamples:")
        print("  python main.py 'Get usage data for customer acme-123'")
        print('  python main.py qbr "Acme Corp"   # QBR + full companion bundle')
        print('  python main.py qbr --main-only "Acme Corp"   # Main QBR deck only')
        print('  decks qbr "Acme Corp"   # same pipeline via decks CLI')


if __name__ == "__main__":
    main()
