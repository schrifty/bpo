#!/usr/bin/env python3
"""Run the LangChain agent with Pendo API tools."""

import argparse
import sys

from src.config import logger


def main() -> None:
    argv = sys.argv[1:]
    if argv and argv[0] == "qbr":
        customer = " ".join(argv[1:]).strip()
        if not customer:
            print("Usage: python main.py qbr <customer>", file=sys.stderr)
            print("  Builds a QBR deck from the Drive template (see GOOGLE_QBR_GENERATOR_FOLDER_ID).", file=sys.stderr)
            sys.exit(2)
        from src.qbr_template import run_qbr_from_template

        import time as _time
        _qbr_t0 = _time.monotonic()
        logger.info("QBR run for customer query: %s", customer)
        result = run_qbr_from_template(customer)
        if result.get("error"):
            print(f"Error: {result['error']}", file=sys.stderr)
            if result.get("hint"):
                print(result["hint"], file=sys.stderr)
            sys.exit(1)
        print(result.get("url", ""))
        print(f"Customer: {result.get('customer')}")
        if result.get("bundle_folder_id"):
            print(
                f"Bundle folder: https://drive.google.com/drive/folders/{result['bundle_folder_id']}"
            )
        em = result.get("exec_manifest_slides", 0)
        ep = result.get("exec_slides_inserted", 0)
        if em:
            exec_line = f"Exec summary: {em} deck slide(s) → {ep} page(s)"
        else:
            exec_line = f"Exec summary: {ep} page(s)"
        print(
            f"Slides — {exec_line}; "
            f"hidden: {result.get('slides_hidden', 0)}; "
            f"adapted: {result.get('adapt_slides', 0)}"
        )
        if result.get("plan_notes"):
            print(f"Manifest plan: {result['plan_notes']}")
        for row in result.get("companion_decks") or []:
            label = row.get("key") or row.get("deck_id", "")
            if row.get("error"):
                print(f"  [{label}] skipped/failed: {row['error']}", flush=True)
                if row.get("hint"):
                    print(f"      {row['hint']}", flush=True)
            elif row.get("url"):
                print(f"  [{label}] {row['url']}", flush=True)
            else:
                print(
                    f"  [{label}] no URL in result (unexpected — see bpo logs for this companion)",
                    flush=True,
                )
        qt = result.get("qbr_timing_seconds") or {}
        if qt:
            top = sorted(
                ((k, v) for k, v in qt.items() if k != "total_elapsed_s" and isinstance(v, (int, float))),
                key=lambda x: -x[1],
            )[:12]
            parts = [f"{k}={v:.0f}s" for k, v in top if v >= 0.5]
            if parts:
                print("Time (QBR phases, top): " + ", ".join(parts), flush=True)
        ha = (result.get("hydrate_adapt_stats") or {}).get("timing") or {}
        if ha:
            def _f(key: str) -> float:
                v = ha.get(key)
                return float(v) if v is not None else 0.0

            print(
                f"Time (main deck adapt): preflight={_f('preflight_pres_and_thumb_urls_s'):.0f}s "
                f"phase_A_LLM={_f('phase_a_parallel_gpt_s'):.0f}s phase_B_Slides={_f('phase_b_sequential_slides_api_s'):.0f}s "
                f"notes={_f('speaker_notes_batch_s'):.0f}s tail={_f('tail_summary_slide_and_stats_s'):.0f}s "
                f"total={_f('total_adapt_s'):.0f}s",
                flush=True,
            )
        elapsed = _time.monotonic() - _qbr_t0
        mins, secs = divmod(int(elapsed), 60)
        logger.info("QBR complete in %dm %02ds", mins, secs)
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
        print("  python main.py qbr \"Acme Corp\"   # QBR from Drive template (see GOOGLE_QBR_GENERATOR_FOLDER_ID)")


if __name__ == "__main__":
    main()
