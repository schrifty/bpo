"""LLM-generated account/churn-risk insights for the all-customers export (optional section).

Failures are surfaced inside the markdown; the export run always completes unless the core
report fails earlier.
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any

logger = logging.getLogger("bpo")

# Max customers pulled into payload (Pendo headline / portfolio-order cap).
_DEFAULT_CUSTOMER_CAP = 40
# Customers per LLM request (bounded JSON size).
_DEFAULT_BATCH_SIZE = 10
# Per-customer Jira prefetch cap (get_customer_jira may issue many HELP queries).
_DEFAULT_JIRA_CUSTOMER_TIMEOUT_SECONDS = 120.0
# Single LLM batch (OpenAI/Gemini chat completion).
_DEFAULT_LLM_BATCH_TIMEOUT_SECONDS = 180.0


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1.0, float(raw))
    except ValueError:
        return default


def _risk_jira_customer_timeout_seconds() -> float:
    return _env_float(
        "BPO_LLM_EXPORT_RISK_JIRA_CUSTOMER_TIMEOUT_SECONDS",
        _DEFAULT_JIRA_CUSTOMER_TIMEOUT_SECONDS,
    )


def _risk_llm_batch_timeout_seconds() -> float:
    return _env_float(
        "BPO_LLM_EXPORT_RISK_LLM_BATCH_TIMEOUT_SECONDS",
        _DEFAULT_LLM_BATCH_TIMEOUT_SECONDS,
    )


def _compact_jira_slice(j: dict[str, Any] | None) -> dict[str, Any]:
    """Counts and rollups only (no issue titles/keys bodies). Mirrors export trimming."""
    if not j or not isinstance(j, dict):
        return {}
    if j.get("error"):
        return {"error": j.get("error")}
    keys_keep = (
        "customer",
        "days",
        "total_issues",
        "open_issues",
        "resolved_issues",
        "open_bugs",
        "escalated",
        "by_status",
        "by_type",
        "by_priority",
        "ttfr",
        "ttr",
    )
    out = {k: j.get(k) for k in keys_keep if k in j}
    tick = j.get("customer_ticket_metrics")
    if isinstance(tick, dict) and tick:
        out["customer_ticket_metrics"] = {
            k: tick.get(k)
            for k in (
                "unresolved_count",
                "resolved_in_6mo_count",
                "sla_adherence_1y",
                "error",
            )
            if k in tick
        }
    return out


def _csr_sites_for_customer(csr_block: dict[str, Any], pendo_customer: str, *, limit: int) -> list[dict[str, Any]]:
    name_l = (pendo_customer or "").strip().lower()
    if not name_l:
        return []
    sites_raw = csr_block.get("sites") if isinstance(csr_block.get("sites"), list) else []
    picked: list[dict[str, Any]] = []
    for s in sites_raw:
        if not isinstance(s, dict):
            continue
        cc = str(s.get("csr_customer") or "").strip().lower()
        if cc == name_l or name_l in cc or cc in name_l:
            picked.append(s)
            if len(picked) >= limit:
                break
    return picked


def _signals_for_customer(signals: list[Any], customer: str, *, limit: int) -> list[str]:
    out: list[str] = []
    c_low = customer.lower()
    for item in signals:
        if isinstance(item, dict):
            cust = str(item.get("customer") or "").strip().lower()
            sig = str(item.get("signal") or "").strip()
            if cust and cust != c_low and c_low not in cust and cust not in c_low:
                continue
            line = f"{item.get('customer') or ''}: {sig}".strip(": ").strip()
        else:
            line = str(item)
        if customer.lower() in line.lower():
            line = " ".join(line.split())
            if len(line) > 320:
                line = line[:319] + "…"
            out.append(line)
        if len(out) >= limit:
            break
    return out


def _slim_pendo_summary(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    kei = row.get("kei") if isinstance(row.get("kei"), dict) else {}
    guides = row.get("guides") if isinstance(row.get("guides"), dict) else {}
    kei_keys = ("total_queries", "unique_users", "adoption_rate", "error")
    gd_keys = ("guide_reach", "dismiss_rate", "advance_rate", "error")
    return {
        "customer": row.get("customer"),
        "pendo_csm": row.get("pendo_csm"),
        "total_users": row.get("total_users"),
        "active_users": row.get("active_users"),
        "login_pct": row.get("login_pct"),
        "engagement": row.get("engagement") if isinstance(row.get("engagement"), dict) else {},
        "kei": {k: kei[k] for k in kei_keys if k in kei},
        "guides": {k: guides[k] for k in gd_keys if k in guides},
    }


def _sf_row_for_customer(sf: dict[str, Any], customer: str) -> dict[str, Any]:
    accounts = sf.get("accounts") if isinstance(sf.get("accounts"), list) else []
    cust_l = customer.strip().lower()
    keep_af = frozenset(
        {
            "Name",
            "ARR__c",
            "active_in_salesforce",
            "contract_statuses_distinct",
            "contract_end_date_nearest",
            "days_until_contract_end_nearest",
            "entity_row_count",
        }
    )
    for a in accounts:
        if not isinstance(a, dict):
            continue
        nm = str(a.get("Name") or "").strip().lower()
        if nm == cust_l:
            return {k: a[k] for k in keep_af if k in a}
    # Loose match rollups matched_customer_contract_rollups
    roll = sf.get("matched_customer_contract_rollups")
    if isinstance(roll, list):
        for r in roll:
            if not isinstance(r, dict):
                continue
            nm = str(r.get("customer") or "").strip().lower()
            if nm == cust_l:
                rk = frozenset(
                    {
                        "customer",
                        "arr",
                        "active",
                        "contract_statuses_distinct",
                        "contract_end_date_nearest",
                        "days_until_contract_end_nearest",
                        "entity_row_count",
                    }
                )
                return {k: r.get(k) for k in rk if k in r}
    return {}


def build_customer_risk_payloads(
    report: dict[str, Any],
    *,
    customer_cap: int | None = None,
    jira_days: int,
    csr_site_limit: int = 6,
    signal_lines_per_customer: int = 10,
    jira_workers: int = 4,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Return list of per-customer dicts for the LLM and any warnings (non-fatal)."""
    cap = customer_cap if customer_cap is not None else _env_int("BPO_LLM_EXPORT_RISK_CUSTOMER_CAP", _DEFAULT_CUSTOMER_CAP)
    warnings: list[str] = []

    rows = report.get("customers") if isinstance(report.get("customers"), list) else []
    if not rows:
        return [], ["no portfolio customers on report; skipping risk payloads"]

    def _risk_rank(row: dict[str, Any]) -> tuple[float, str]:
        name = str(row.get("customer") or "")
        # Lower login first → higher churn discussion priority; tie-break alphabetically.
        login = row.get("login_pct")
        try:
            lg = float(login) if login is not None else 999.0
        except (TypeError, ValueError):
            lg = 999.0
        return (lg, name.lower())

    sorted_rows = sorted((r for r in rows if isinstance(r, dict) and r.get("customer")), key=_risk_rank)
    chosen = sorted_rows[:cap]

    names = [str(r["customer"]) for r in chosen]
    jira_timeout = _risk_jira_customer_timeout_seconds()
    logger.info(
        "LLM export §7: building risk payloads for %d customer(s) (portfolio cap %d)",
        len(names),
        cap,
    )
    csr = report.get("csr") if isinstance(report.get("csr"), dict) else {}
    ph = csr.get("platform_health") if isinstance(csr.get("platform_health"), dict) else {}
    sc = csr.get("supply_chain") if isinstance(csr.get("supply_chain"), dict) else {}
    pv = csr.get("platform_value") if isinstance(csr.get("platform_value"), dict) else {}

    signals = report.get("portfolio_signals") if isinstance(report.get("portfolio_signals"), list) else []
    sf = report.get("salesforce") if isinstance(report.get("salesforce"), dict) else {}

    jira_by_name: dict[str, dict[str, Any]] = {}
    try:
        from src.jira_client import get_shared_jira_client

        jc = get_shared_jira_client()

        def _one_jira(nm: str) -> tuple[str, dict[str, Any]]:
            """Run get_customer_jira in a nested worker so result(timeout=…) can interrupt slow calls."""

            def _fetch() -> dict[str, Any]:
                raw = jc.get_customer_jira(nm, days=min(int(jira_days), 365))
                return _compact_jira_slice(raw if isinstance(raw, dict) else {})

            try:
                with ThreadPoolExecutor(max_workers=1) as inner:
                    fut = inner.submit(_fetch)
                    return nm, fut.result(timeout=jira_timeout)
            except FuturesTimeoutError:
                msg = f"jira prefetch timed out after {jira_timeout:.0f}s"
                return nm, {"error": msg}
            except Exception as e:
                return nm, {"error": str(e)}

        logger.info(
            "LLM export §7: prefetching Jira HELP for %d customer(s) "
            "(%d workers, %.1fs timeout per customer)",
            len(names),
            max(1, jira_workers),
            jira_timeout,
        )
        t_jira = time.monotonic()
        done_jira = 0
        with ThreadPoolExecutor(max_workers=max(1, jira_workers)) as pool:
            futs = {pool.submit(_one_jira, n): n for n in names}
            for fut in as_completed(futs):
                nm, blob = fut.result()
                if isinstance(blob, dict) and str(blob.get("error") or "").startswith("jira prefetch timed out"):
                    logger.warning(
                        "LLM export §7: Jira prefetch timed out for %r (%d/%d, %.1fs elapsed)",
                        nm,
                        done_jira + 1,
                        len(names),
                        time.monotonic() - t_jira,
                    )
                    warnings.append(f"{nm}: {blob['error']}")
                jira_by_name[nm] = blob
                done_jira += 1
                logger.info(
                    "LLM export §7: Jira prefetch %d/%d (%.1fs) — %r",
                    done_jira,
                    len(names),
                    time.monotonic() - t_jira,
                    nm,
                )
        logger.info(
            "LLM export §7: Jira prefetch finished (%d/%d customers, %.1fs)",
            done_jira,
            len(names),
            time.monotonic() - t_jira,
        )
    except Exception as e:
        warnings.append(f"jira prefetch failed ({e}); omitting per-customer jira slices")
        for nm in names:
            jira_by_name[nm] = {"error": str(e)}

    payloads: list[dict[str, Any]] = []
    for row in chosen:
        cname = str(row.get("customer") or "").strip()
        if not cname:
            continue
        payload: dict[str, Any] = {
            "customer": cname,
            "pendo": _slim_pendo_summary(row),
            "salesforce": _sf_row_for_customer(sf, cname),
            "cs_report": {
                "platform_health_sites_sample": _csr_sites_for_customer(ph, cname, limit=csr_site_limit),
                "supply_chain_sites_sample": _csr_sites_for_customer(sc, cname, limit=csr_site_limit),
                "platform_value_sites_sample": _csr_sites_for_customer(pv, cname, limit=csr_site_limit),
            },
            "pendo_portfolio_signals_sample": _signals_for_customer(signals, cname, limit=signal_lines_per_customer),
            "jira_help": jira_by_name.get(cname, {}),
            "leandna_data_api": {
                "note": (
                    "LeanDNA Data API (item master, shortages, lean projects) is not merged into "
                    "the all-customers export report today; omit from conclusions or say data not present."
                ),
            },
        }
        payloads.append(payload)

    return payloads, warnings


def _call_risk_llm_batch(
    batch: list[dict[str, Any]],
    *,
    model: str,
    timeout_seconds: float | None = None,
) -> tuple[list[dict[str, Any]], str | None]:
    """Returns (parsed customer rows from model, error string or None)."""
    timeout = (
        timeout_seconds
        if timeout_seconds is not None
        else _risk_llm_batch_timeout_seconds()
    )

    def _invoke() -> tuple[list[dict[str, Any]], str | None]:
        from src.config import llm_client
        from src.llm_utils import _llm_create_with_retry

        system = (
            "You are a cautious customer success strategist. Output JSON only.\n\n"
            "TASK: For each object in input.customers[], output EXACTLY two insights focused on "
            "**account retention, contraction, or churn risk**. Use ONLY fields present under that "
            "customer entry (pendo, salesforce, cs_report samples, portfolio signals, jira_help counts).\n\n"
            "RULES:\n"
            "- Do NOT invent numbers, vendors, conversations, or missing fields.\n"
            "- Do NOT cite individual Jira ticket titles, keys, or descriptions (counts and buckets only).\n"
            "- If data is insufficient for a strong claim, write a conservative insight naming the gap.\n"
            "- Prefer signals that combine domains (adoption + contract timing + HELP load + operational health).\n\n"
            "Return one JSON object: { \"customers\": [ ... ] }. Each item has "
            '"customer" (exact string from input) and "insights" '
            "(array of **exactly** two objects). "
            'Each insight object: "title", "detail", "risk_level" (low|medium|high), '
            '"evidence" (array of short field labels you used).\n'
        )

        body = {"customers": batch}
        payload_json = json.dumps(body, separators=(",", ":"), default=str)

        client = llm_client()
        try:
            resp = _llm_create_with_retry(
                client,
                model=model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system},
                    {
                        "role": "user",
                        "content": (
                            "Data JSON (facts only):\n```json\n"
                            + payload_json[:240_000]
                            + ("\n```" if len(payload_json) <= 240_000 else "\n```\n(truncated)")
                        ),
                    },
                ],
            )
            raw_txt = (resp.choices[0].message.content or "").strip()
            data = json.loads(raw_txt)
        except Exception as e:
            logger.warning("risk insights LLM batch failed: %s", e)
            return [], str(e)

        rows_out: list[dict[str, Any]] = []
        try:
            for row in data.get("customers") or []:
                if isinstance(row, dict) and row.get("customer"):
                    rows_out.append(row)
        except Exception:
            return [], "LLM returned JSON but customers[] was not parseable"

        return rows_out, None

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(_invoke)
        try:
            return fut.result(timeout=timeout)
        except FuturesTimeoutError:
            names = [str(p.get("customer") or "") for p in batch if isinstance(p, dict)]
            logger.warning(
                "risk insights LLM batch timed out after %.0fs (customers: %s)",
                timeout,
                ", ".join(names[:5]) + ("…" if len(names) > 5 else ""),
            )
            return [], f"LLM batch timed out after {timeout:.0f}s"


def render_risk_insights_section(
    report: dict[str, Any],
    *,
    jira_days: int,
    model: str | None = None,
) -> str:
    """Build markdown section §7; never raises — errors go under an error subsection."""
    from src.config import LLM_MODEL

    model_name = model or LLM_MODEL
    lines: list[str] = [
        "",
        "## 7. Account & churn risk insights (LLM)",
        "",
        f"- **Model:** `{model_name}`",
        "- **Scope:** Two insights per customer from Pendo, Salesforce, CS Report samples, "
        "portfolio signal lines, and Jira HELP **aggregate** counts only. "
        "LeanDNA Data API is **not** in the export payload (see per-customer `leandna_data_api.note`).",
        "",
    ]

    logger.info("LLM export §7: starting account & churn risk insights")
    t_section = time.monotonic()
    payloads, warns = build_customer_risk_payloads(report, jira_days=jira_days)
    from .export_run_diagnostics import collect_export_warning

    for w in warns:
        lines.append(f"- *(Warning: {w})*")
        collect_export_warning(f"risk insights: {w}", llm_export=True)
    if warns:
        lines.append("")

    if not payloads:
        lines.extend(
            [
                "### Error",
                "",
                "No customer payloads could be built; risk section skipped.",
                "",
            ]
        )
        return "\n".join(lines)

    batch_n = _env_int("BPO_LLM_EXPORT_RISK_BATCH_SIZE", _DEFAULT_BATCH_SIZE)
    llm_timeout = _risk_llm_batch_timeout_seconds()
    total_batches = (len(payloads) + batch_n - 1) // batch_n
    all_parsed: list[dict[str, Any]] = []
    batch_errors: list[str] = []

    logger.info(
        "LLM export §7: running %d LLM batch(es) (%d customers, batch size %d, %.0fs timeout per batch)",
        total_batches,
        len(payloads),
        batch_n,
        llm_timeout,
    )

    for batch_idx, i in enumerate(range(0, len(payloads), batch_n), start=1):
        batch = payloads[i : i + batch_n]
        batch_names = [str(p.get("customer") or "") for p in batch if isinstance(p, dict)]
        preview = ", ".join(batch_names[:3])
        if len(batch_names) > 3:
            preview += ", …"
        logger.info(
            "LLM export §7: LLM batch %d/%d starting (%d customers: %s)",
            batch_idx,
            total_batches,
            len(batch),
            preview,
        )
        t_batch = time.monotonic()
        parsed, err = _call_risk_llm_batch(
            batch,
            model=model_name,
            timeout_seconds=llm_timeout,
        )
        elapsed = time.monotonic() - t_batch
        if err:
            logger.warning(
                "LLM export §7: LLM batch %d/%d failed in %.1fs — %s",
                batch_idx,
                total_batches,
                elapsed,
                err,
            )
            batch_errors.append(f"batch {batch_idx} ({len(batch)} customers): {err}")
            continue
        logger.info(
            "LLM export §7: LLM batch %d/%d OK in %.1fs (%d customer row(s) parsed)",
            batch_idx,
            total_batches,
            elapsed,
            len(parsed),
        )
        all_parsed.extend(parsed)

    name_to_llm = {str(r["customer"]).strip(): r for r in all_parsed if isinstance(r, dict) and r.get("customer")}

    if batch_errors:
        lines.append("### Error (partial or failed LLM run)")
        lines.append("")
        for e in batch_errors:
            lines.append(f"- {e}")
            collect_export_warning(f"risk insights: {e}", llm_export=True)
        lines.append("")
        lines.append("*Successful batches (if any) are still printed below.*")
        lines.append("")

    # Render in original payload order so output matches portfolio ordering.
    missing_llm = 0
    for payload in payloads:
        cname = str(payload["customer"])
        llm_row = name_to_llm.get(cname)

        lines.append(f"### {cname}")
        lines.append("")

        if not llm_row:
            missing_llm += 1
            lines.append("*No LLM result for this customer (batch failure or mismatch).*")
            lines.append("")
            continue

        insights = llm_row.get("insights")
        if not isinstance(insights, list):
            lines.append("*LLM insights missing or invalid shape.*")
            lines.append("")
            missing_llm += 1
            continue

        n = 0
        for ins in insights[:2]:
            if not isinstance(ins, dict):
                continue
            n += 1
            title = str(ins.get("title") or f"Insight {n}").strip()
            detail = str(ins.get("detail") or "").strip()
            risk = str(ins.get("risk_level") or "").strip()
            ev = ins.get("evidence")
            ev_txt = ""
            if isinstance(ev, list) and ev:
                ev_txt = "; ".join(str(x) for x in ev[:8])
            risk_part = f" **({risk})**" if risk else ""
            lines.append(f"{n}. **{title}**{risk_part}")
            if detail:
                lines.append(f"   {detail}")
            if ev_txt:
                lines.append(f"   *Evidence keys:* {ev_txt}")
            lines.append("")

        if n == 0:
            lines.append("*No usable insights returned.*")
            lines.append("")
            missing_llm += 1

    if missing_llm:
        lines.append("")
        lines.append(f"*Note: {missing_llm} customer block(s) lacked usable LLM output.*")

    logger.info(
        "LLM export §7: finished in %.1fs (%d payloads, %d LLM rows, %d batch error(s))",
        time.monotonic() - t_section,
        len(payloads),
        len(all_parsed),
        len(batch_errors),
    )
    return "\n".join(lines).rstrip() + "\n"
