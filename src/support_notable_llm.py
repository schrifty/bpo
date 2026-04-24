"""LLM-driven \"Notable\" slide for the support review deck: digest + six bullets for CS leaders."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from .config import LLM_MODEL, LLM_MODEL_FAST, logger, llm_client
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence

_MAX_DIGEST_JSON_CHARS = 24_000

# Shorter reminder for follow-up (top-up) LLM calls; full rules are in the main user prompt.
_NOTABLE_TREND_RULE_REMINDER = (
    "Reminder: Do not use words like 'trend', 'decrease', 'increase', or 'momentum' when you only have two months "
    "(or two periods). State both counts neutrally. Use direction/trend language only with three or more "
    "comparable time buckets in the digest. Avoid cause speculation (e.g. logging behavior) without evidence in the data."
)


class NotableLlmError(RuntimeError):
    """Notable slide LLM did not return usable content (do not fall back in strict mode)."""


def _flag_use_llm() -> bool:
    v = (os.environ.get("BPO_SUPPORT_NOTABLE_LLM", "true") or "").strip().lower()
    return v not in ("0", "false", "no", "off")


def _allow_notable_llm_fallback() -> bool:
    """If false (default), any LLM/parsing failure for Notable raises; no generic bullets."""
    v = (os.environ.get("BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on", "allow")


def _ticket_metrics_for_digest(
    m: dict[str, Any], *, scope: str, cap_map: int = 14, cap_orgs: int = 8
) -> dict[str, Any]:
    """Subset of :func:`~jira_client.get_customer_ticket_metrics` / project metrics (actual API keys)."""
    out: dict[str, Any] = {"_scope": scope}
    for k in ("unresolved_count", "resolved_in_6mo_count"):
        if m.get(k) is not None:
            out[k] = m[k]
    jor = m.get("jsm_organizations_resolved")
    if jor is not None:
        if isinstance(jor, (list, tuple)):
            out["jsm_organizations_resolved"] = [str(x) for x in jor[:cap_orgs]]
        else:
            out["jsm_organizations_resolved"] = jor
    for subk in ("by_type_open", "by_status_open"):
        d = m.get(subk)
        if isinstance(d, dict) and d:
            out[subk] = dict(list(d.items())[:cap_map])
    for subk in ("ttfr_1y", "ttr_1y", "sla_adherence_1y"):
        v = m.get(subk)
        if v is not None:
            out[subk] = v
    return out


def _trim_tickets(issues: list[dict] | None, n: int = 12) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for it in (issues or [])[:n]:
        if not isinstance(it, dict):
            continue
        out.append(
            {
                "key": str(it.get("key") or ""),
                "summary": (str(it.get("summary") or ""))[:220],
            }
        )
    return out


def _trend_row_summary(rows: list[dict] | None) -> list[dict[str, Any]]:
    if not rows:
        return []
    return rows[-6:] if len(rows) > 6 else rows


def _assignee_top(rows: list[dict] | None, n: int = 12) -> list[dict[str, Any]]:
    if not rows:
        return []
    return rows[:n]


def build_support_review_digest(
    report: dict[str, Any],
    *,
    slide_titles: list[str],
) -> dict[str, Any]:
    """Structured summary of everything the support deck builders use, for the LLM (kept small)."""
    from .slides_theme import _date_range

    days = int(report.get("days") or 365)
    j = report.get("jira") or {}
    customer = report.get("customer")
    c_disp = customer if customer else "All Customers"
    scope_label = f"for {customer}" if customer else "across all customers"
    
    r: dict[str, Any] = {
        "customer_name": c_disp,
        "data_scope": f"All ticket counts below are {scope_label} only. Do not confuse with project-wide totals.",
        "date_range": _date_range(
            days, report.get("quarter"), report.get("quarter_start"), report.get("quarter_end")
        ),
        "deck_slides_built": slide_titles,
    }

    ctm = j.get("customer_ticket_metrics")
    if isinstance(ctm, dict) and ctm and not ctm.get("error"):
        r["help_tickets_for_this_customer"] = _ticket_metrics_for_digest(
            ctm, scope=f"HELP project tickets {scope_label}"
        )
    elif isinstance(ctm, dict) and ctm.get("error"):
        r["help_tickets_error"] = str(ctm.get("error"))[:400]

    def _recent_block(name: str, proj_label: str) -> None:
        blob = j.get(name) or {}
        if not isinstance(blob, dict):
            return
        r[name] = {
            "_scope": f"{proj_label} tickets {scope_label}",
            "error": (blob.get("error") or "")[:300] or None,
            "recently_opened": _trim_tickets(blob.get("recently_opened") or []),
            "recently_closed": _trim_tickets(blob.get("recently_closed") or []),
        }

    _recent_block("customer_help_recent", "HELP project")
    _recent_block("customer_project_recent", "CUSTOMER project")
    _recent_block("lean_project_recent", "LEAN project")

    for proj, key in (("HELP", "help_resolved_by_assignee"), ("CUSTOMER", "customer_resolved_by_assignee"), ("LEAN", "lean_resolved_by_assignee")):
        blob = j.get(key) or {}
        if not isinstance(blob, dict) or not blob:
            continue
        r[key] = {
            "_scope": f"{proj} project resolved tickets {scope_label}",
            "total_resolved": blob.get("total_resolved", 0),
            "by_assignee": _assignee_top(list(blob.get("by_assignee") or [])),
            "error": (blob.get("error") or "")[:300] or None,
        }

    for bkey, proj in (
        ("customer_project_open_breakdown", "CUSTOMER"),
        ("lean_project_open_breakdown", "LEAN"),
    ):
        b = j.get(bkey) or {}
        if not isinstance(b, dict) or not b:
            continue
        r[bkey] = {
            "project": proj,
            "unresolved_count": b.get("unresolved_count"),
            "by_type_open": dict(list((b.get("by_type_open") or {}).items())[:12]),
            "by_status_open": dict(list((b.get("by_status_open") or {}).items())[:12]),
        }

    for tkey, proj in (
        ("customer_project_volume_trends", "CUSTOMER"),
        ("lean_project_volume_trends", "LEAN"),
    ):
        t = j.get(tkey) or {}
        if not isinstance(t, dict) or t.get("error"):
            r[tkey] = t if isinstance(t, dict) else None
            continue
        r[tkey] = {
            "project": proj,
            "all_tail": _trend_row_summary(list(t.get("all") or [])),
            "escalated_tail": _trend_row_summary(list(t.get("escalated") or [])),
        }

    for mkey, proj in (
        ("customer_project_ticket_metrics", "CUSTOMER"),
        ("lean_project_ticket_metrics", "LEAN"),
    ):
        m = j.get(mkey) or {}
        if isinstance(m, dict) and m and not m.get("error"):
            r[mkey] = {
                **_ticket_metrics_for_digest(m, scope=f"{proj} project tickets {scope_label}"),
                "project": proj,
            }

    eng = report.get("eng_portfolio") or {}
    ht = eng.get("help_ticket_trends")
    if isinstance(ht, dict) and not ht.get("error"):
        r["help_project_volume_trends"] = {
            "all_tail": _trend_row_summary(list(ht.get("all") or [])),
        }

    raw = json.dumps(r, default=str, ensure_ascii=False)
    if len(raw) > _MAX_DIGEST_JSON_CHARS:
        r["_truncated"] = f"True (was {len(raw)} chars, capped for LLM input)"
        while len(json.dumps(r, default=str, ensure_ascii=False)) > _MAX_DIGEST_JSON_CHARS:
            for k, v in list(r.items()):
                if isinstance(v, (list, dict)) and k not in ("help_account_metrics", "audience", "date_range"):
                    r[k] = str(v)[:400]
                    break
            else:
                break
    return r


def _fallback_items(entry: dict[str, Any]) -> list[str]:
    ni = entry.get("notable_items")
    if ni and isinstance(ni, (list, tuple)):
        return [str(x).strip() for x in ni if str(x).strip()][:6]
    return [
        "VOLUME: Review ticket creation trends — rising volume may indicate product friction or training gaps.",
        "SLA HEALTH: Check TTFR and TTR metrics against SLA targets; escalate persistent misses to Support leadership.",
        "BACKLOG: Identify tickets stuck in 'Waiting for customer' or 'In Engineering' for extended periods.",
        "TOP THEMES: Look for recurring issue types that could be addressed proactively via documentation or training.",
        "WORKLOAD: Note if resolution is concentrated on few assignees — capacity risk if key people are unavailable.",
        "ACTION: Before QBR, pull 2-3 specific ticket examples that illustrate the customer's top pain points.",
    ]


def _normalize_llm_json_string(s: str) -> str:
    """Common LLM confusions: smart quotes, BOM."""
    t = s.strip()
    t = t.replace("\ufeff", "")
    t = t.replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")
    return t


def _parse_bullets_from_markdown_lines(text: str) -> list[str] | None:
    """When JSON is invalid, recover numbered / markdown-style lines from the model reply."""
    lines: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        m = re.match(r"^[-*•]\s+(.+)$", s) or re.match(r"^\d+[\).]\s+(.+)$", s)
        if m:
            lines.append(m.group(1).strip())
    if not lines and '"bullets"' in text:
        m = re.search(
            r'"bullets"\s*:\s*\[([\s\S]+?)\]',
            text,
        )
        if m:
            inner = m.group(1)
            for part in re.findall(r'"(?:\\.|[^"\\])*"', inner):
                u = part[1:-1].replace(r"\\\"", '"').replace(r"\\n", " ").strip()
                if u:
                    lines.append(u)
    return lines if lines else None


def _fix_json_newlines_in_strings(s: str) -> str:
    """Replace unescaped newlines inside JSON strings with spaces.
    
    LLMs often produce newlines inside string values which breaks JSON parsing.
    This function finds strings and replaces newlines within them.
    """
    result = []
    in_string = False
    escape_next = False
    for char in s:
        if escape_next:
            result.append(char)
            escape_next = False
            continue
        if char == '\\':
            escape_next = True
            result.append(char)
            continue
        if char == '"':
            in_string = not in_string
            result.append(char)
            continue
        if in_string and char == '\n':
            result.append(' ')
            continue
        result.append(char)
    return ''.join(result)


def _salvage_bullet_strings_from_partial_json(s: str) -> list[str] | None:
    """When json.loads fails (truncated tail or bad last value), keep each complete string in ``bullets``."""
    m = re.search(r'"bullets"\s*:\s*\[', s, re.IGNORECASE)
    if not m:
        return None
    i = m.end()
    out: list[str] = []
    dec = json.JSONDecoder()
    while i < len(s):
        while i < len(s) and s[i] in " \t\n\r,":
            i += 1
        if i >= len(s) or s[i] == "]":
            break
        if s[i] != '"':
            i += 1
            continue
        try:
            val, j = dec.raw_decode(s, i)
        except (json.JSONDecodeError, ValueError, TypeError):
            # A middle bullet had unescaped quotes: skip to the next `, "…"` entry
            sub = s[i + 1 :]
            nxt = re.search(r",\s*\"", sub)
            if not nxt:
                break
            i = i + 1 + nxt.end() - 1
            if i < len(s) and s[i] == '"':
                continue
            break
        if isinstance(val, str) and str(val).strip():
            out.append(str(val).strip())
        i = j
    if out:
        logger.info("Notable: recovered %d complete bullet(s) from partial/truncated JSON", len(out))
    return out if out else None


def _parse_notable_bullets_json(
    content: str,
) -> list[str] | None:
    """Parse `{ \"bullets\": [...] }` from the model. Returns None to signal fallback to loose parsing."""
    raw = _strip_json_code_fence(_normalize_llm_json_string(content))
    if not raw:
        return None
    # Fix newlines inside strings (common LLM issue)
    raw = _fix_json_newlines_in_strings(raw)
    for attempt in (0, 1):
        s = raw
        if attempt == 1:
            if not s.lstrip().startswith("{"):
                s = "{" + s
            if not s.rstrip().endswith("}"):
                s = s + "}"
        try:
            data = json.loads(s)
        except json.JSONDecodeError:
            try:
                salv = _salvage_bullet_strings_from_partial_json(raw)
            except Exception as ex:
                logger.debug("Notable: salvage error: %s", ex)
                salv = None
            if salv:
                return salv
            if attempt == 0:
                continue
            return None
        if not isinstance(data, dict):
            return None
        bullets = data.get("bullets")
        if not isinstance(bullets, list):
            add = data.get("add")
            if isinstance(add, list):
                bullets = add
        if not isinstance(bullets, list):
            return None
        out = [str(b).strip() for b in bullets if str(b).strip()]
        if out:
            return out
    return None


def _dedupe_preserve(bullets: list[str], max_n: int = 6) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for b in bullets:
        t = (b or "").strip()
        if not t:
            continue
        k = re.sub(r"\s+", " ", t.lower())[:100]
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
        if len(out) >= max_n:
            break
    return out


def _heuristic_bullets_from_digest(
    digest: dict[str, Any],
    existing: list[str],
    need: int,
) -> list[str]:
    """Factual bullets from digest numbers when the first LLM call returns only part of the list.

    Stops after *need*; skips lines too similar to *existing*.
    """
    if need <= 0:
        return []
    cname = str(digest.get("customer_name") or "This customer")
    ex_low: set[str] = {re.sub(r"\s+", " ", (e or "").lower())[:60] for e in existing if e}

    def _take(text: str) -> bool:
        t = re.sub(r"\s+", " ", (text or "").lower())[:60]
        if t in ex_low or not text.strip():
            return False
        ex_low.add(t)
        return True

    cands: list[str] = []

    h = digest.get("help_tickets_for_this_customer")
    if isinstance(h, dict) and h:
        u, r = h.get("unresolved_count"), h.get("resolved_in_6mo_count")
        if u is not None:
            cands.append(
                f"VOLUME: {cname} has {u} open HELP ticket(s) (JSM org scope) and {r!s} resolved in the 6-month metrics window (digest). "
            )
        sla = h.get("sla_adherence_1y")
        if isinstance(sla, (int, float)):
            cands.append(
                f"SLA HEALTH: {cname}'s rolling-year HELP SLA adherence is {round(100.0 * float(sla), 0):.0f}% (digest). "
            )
        bt = h.get("by_type_open")
        if isinstance(bt, dict) and bt:
            top_t = max(bt.items(), key=lambda x: int(x[1] or 0))
            cands.append(
                f"TOP THEMES: Largest open HELP work type is {str(top_t[0])!r} ({int(top_t[1])}) (digest). "
            )

    for bkey, lab in (
        ("customer_project_ticket_metrics", "CUSTOMER"),
        ("lean_project_ticket_metrics", "LEAN"),
    ):
        m = digest.get(bkey)
        if not isinstance(m, dict) or not m:
            continue
        u, r = m.get("unresolved_count"), m.get("resolved_in_6mo_count")
        if u is not None or r is not None:
            cands.append(
                f"VOLUME: {lab} project — {u!s} open, {r!s} resolved in 6-month window (digest). "
            )
        sla = m.get("sla_adherence_1y")
        if isinstance(sla, (int, float)):
            cands.append(
                f"SLA HEALTH: {lab} rolling-year adherence about {round(100.0 * float(sla), 0):.0f}% (digest). "
            )

    for akey, lab in (
        ("help_resolved_by_assignee", "HELP"),
        ("customer_resolved_by_assignee", "CUSTOMER"),
        ("lean_resolved_by_assignee", "LEAN"),
    ):
        a = digest.get(akey)
        if not isinstance(a, dict):
            continue
        tot = a.get("total_resolved")
        rows = list(a.get("by_assignee") or [])
        if tot and rows and isinstance(rows[0], dict):
            top = rows[0]
            an = (top.get("assignee") or "Unassigned") or "Unassigned"
            c = int(top.get("count") or 0)
            cands.append(
                f"WORKLOAD: {lab} — {int(tot)} tickets resolved in window; heaviest assignee is {an!s} with {c} (digest). "
            )

    ht = digest.get("help_project_volume_trends")
    if isinstance(ht, dict):
        tail = list(ht.get("all_tail") or [])
        if len(tail) >= 2:
            t0, t1 = tail[-2], tail[-1]
            c0, c1 = int(t0.get("created", 0) or 0), int(t1.get("created", 0) or 0)
            r0, r1 = int(t0.get("resolved", 0) or 0), int(t1.get("resolved", 0) or 0)
            cands.append(
                f"VOLUME: Last two months in digest — HELP created {c0} to {c1} and resolved {r0} to {r1} (monthly series). "
            )

    for rk, lab in (("customer_help_recent", "HELP"), ("customer_project_recent", "CUSTOMER"), ("lean_project_recent", "LEAN")):
        b = digest.get(rk)
        if not isinstance(b, dict) or b.get("error"):
            continue
        ro, rc = b.get("recently_opened") or [], b.get("recently_closed") or []
        cands.append(
            f"RECENT: {lab} — {len(ro)} in recently opened sample, {len(rc)} in recently closed (digest, summaries trimmed). "
        )
        break

    out: list[str] = []
    for t in cands:
        if len(out) >= need:
            break
        t = t.strip()
        if not _take(t):
            continue
        out.append(t)
    return out


def _notable_normalize_add_key_json(content: str) -> str:
    c = _strip_json_code_fence(_normalize_llm_json_string(content))
    c = c.strip()
    if re.search(r"^\s*\{[\s\S]*?\"\s*add\s*\"\s*:\s*\[", c) and '"bullets"' not in c:
        c = re.sub(
            r'"\s*add\s*"\s*:\s*',
            '"bullets":',
            c,
            count=1,
            flags=re.IGNORECASE,
        )
    return c


def _top_up_notable_bullets(
    client: Any,
    digest: dict[str, Any],
    existing: list[str],
    n_more: int,
) -> list[str]:
    """Second LLM call: add *additional* bullets, JSON key ``add`` to avoid conflating parsers."""
    if n_more <= 0:
        return []
    cname = str(digest.get("customer_name", "this customer"))
    ex = "\n".join(f"{i+1}. {e}" for i, e in enumerate(existing))
    pl = json.dumps(digest, default=str, ensure_ascii=False)[:10_000]
    user = (
        f"For {cname}, the Notable deck slide already has these {len(existing)} point(s) — do not repeat or paraphrase them.\n{ex}\n\n"
        f"Write {n_more} new insight(s) in the same style: 1-2 sentences, start with a category in CAPS "
        f"(VOLUME, SLA HEALTH, BACKLOG, TOP THEMES, WORKLOAD, ACTION), cite only numbers in the data below, and attribute to {cname}.\n\n"
        f"Data digest (JSON fragment):\n{pl}\n\n"
        f'Output a JSON object: {{ "add": [ {n_more} strings ] }} only. '
        f"No unescaped double quotes inside a string. Use 'single quotes' to quote Jira phrasing. "
    )

    model = LLM_MODEL_FAST if len(pl) < 7_000 else LLM_MODEL
    kws: dict[str, Any] = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Valid JSON only; output key add with a string array. "
                    "Same rules as the main Notable pass: do not use 'trend' or similar for only two time periods; "
                    "need 3+ comparable buckets for trend language. No unfounded cause speculation."
                ),
            },
            {"role": "user", "content": user + "\n\n" + _NOTABLE_TREND_RULE_REMINDER},
        ],
        "max_tokens": 1_200,
        "temperature": 0.25,
    }
    ch = ""
    try:
        try:
            resp = _llm_create_with_retry(client, **{**kws, "response_format": {"type": "json_object"}})
        except Exception as e2:
            emsg = f"{e2!s}".lower()
            if (
                ("response" in emsg and "format" in emsg)
                or "json_object" in emsg
                or ("unknown" in emsg and "param" in emsg)
            ):
                resp = _llm_create_with_retry(client, **kws)
            else:
                raise
        ch = (resp.choices[0].message.content or "").strip() if resp and resp.choices else ""
    except Exception as e:
        logger.info("Notable top-up: LLM call failed: %s", e)
        return []
    if not ch:
        return []
    nch = _notable_normalize_add_key_json(ch)
    nch = _fix_json_newlines_in_strings(nch)
    try:
        got = _parse_notable_bullets_json(nch) or _parse_bullets_from_markdown_lines(_normalize_llm_json_string(ch)) or []
    except Exception as ex:
        logger.info("Notable: could not parse top-up response: %s", ex)
        got = []
    if got:
        logger.info("Notable: top-up LLM returned %d new bullet(s)", len(got))
    return [str(b).strip() for b in got if str(b).strip()][:n_more]


def _pad_notable_to_six(
    out: list[str],
    digest: dict[str, Any],
    entry: dict[str, Any],
) -> list[str]:
    """After the first pass, add bullets until 6: top-up LLM, digest heuristics, then static templates."""
    if len(out) >= 6:
        return out[:6]
    short = 6 - len(out)
    client: Any = None
    if short > 0:
        try:
            client = llm_client()
        except Exception:
            client = None
    if client and short > 0:
        extra = _top_up_notable_bullets(client, digest, out, short)
        for e in extra:
            if e and e not in out:
                out.append(e)
        out = _dedupe_preserve(out, 6)[:6]
    short = 6 - len(out)
    if short > 0:
        for h in _heuristic_bullets_from_digest(digest, out, short):
            if h and h not in out:
                out.append(h)
        out = _dedupe_preserve(out, 6)[:6]
    if len(out) < 6:
        for x in _fallback_items(entry or {}):
            if len(out) >= 6:
                break
            if x and x not in out:
                out.append(x)
    if len(out) < 6:
        for x in _fallback_items({}):
            if len(out) >= 6:
                break
            if x and x not in out:
                out.append(x)
        if len(out) < 6:
            logger.warning("Notable: have %d bullets after all pads (expected 6)", len(out))
    return out[:6]


def generate_notable_bullets_via_llm(
    digest: dict[str, Any],
    entry: dict[str, Any],
) -> tuple[list[str], str]:
    """Return (bullets, source). Source: ``llm``, ``env_off`` (LLM explicitly off), or ``yaml_fallback`` (opt-in)."""
    if not _flag_use_llm():
        return _fallback_items(entry), "env_off"
    allow_fb = _allow_notable_llm_fallback()
    try:
        client = llm_client()
    except RuntimeError as e:
        if not allow_fb:
            raise NotableLlmError(
                f"No LLM client: {e}. Set API keys, or BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true to use static bullets."
            ) from e
        logger.warning("Notable: LLM disabled (%s) — using YAML / default bullets", e)
        return _fallback_items(entry), "yaml_fallback"

    payload = json.dumps(digest, indent=0, default=str, ensure_ascii=False)[:_MAX_DIGEST_JSON_CHARS]
    customer_name = digest.get("customer_name", "this customer")
    sys = (
        "You are a support-data analyst preparing insights for Customer Success leaders. "
        "Your job is to find patterns, risks, and opportunities in Jira ticket data that CS should know before QBRs or when engaging Support. "
        "Extract specific, actionable insights — not generic advice. Reference actual numbers from the data. "
        "Be conservative with trend language: two data points (e.g. two months) are not a trend. "
        "Each bullet should answer: 'What does CS need to know or do based on this data?'"
    )
    user = (
        f"Analyze this support data for **{customer_name}** and produce exactly 6 insights.\n\n"
        f"IMPORTANT: All ticket counts in this data are specifically for {customer_name}. "
        "Do NOT confuse these with project-wide or all-customer totals. When you cite numbers, "
        f"say '{customer_name} has X tickets' not 'there are X tickets'.\n\n"
        f"DATA:\n{payload}\n\n"
        "For each bullet, identify ONE of these (cover at least 4 different categories across your 6 bullets):\n"
        f"• VOLUME: Ticket creation vs resolution and recent volume for {customer_name} (see trend rules below).\n"
        f"• SLA HEALTH: TTFR/TTR metrics for {customer_name}'s tickets. Are they meeting expectations?\n"
        f"• BACKLOG: {customer_name}'s unresolved tickets, aging tickets, or those stuck 'Waiting for customer'.\n"
        f"• TOP THEMES: What ticket types dominate for {customer_name}? Recurring issues?\n"
        f"• WORKLOAD: Who is handling {customer_name}'s tickets? Capacity concerns?\n"
        f"• ACTION: Specific next step for CS regarding {customer_name}.\n\n"
        "RULES:\n"
        f"- Always attribute numbers to {customer_name} (e.g., '{customer_name} has 8 tickets waiting').\n"
        "- Each bullet: 1-2 sentences, max 300 chars. Start with category in caps.\n"
        "- Do not invent data. Use only what's in the digest.\n"
        "- TREND / MOMENTUM: Do not call a change a 'trend', 'decrease', 'increase', 'momentum', or 'shift' "
        "if you are only comparing **two** periods (e.g. two months in a row). That is a single step, not a trend. "
        "For two months, report the numbers factually (e.g. 'February N created vs March M created') and avoid trend words. "
        "Use 'trend' or direction words only if the digest has **at least three** comparable time buckets (e.g. three+ months) showing the same direction, "
        "or say you are describing 'recent month-to-month' activity, not a multi-month trend.\n"
        "- Do not speculate about causes (e.g. 'logging behavior', 'reduced issues') unless the digest explicitly supports it; stick to what the numbers show.\n"
        "- In JSON, never use ASCII double-quote characters inside a bullet. Paraphrase Jira titles; use 'single quotes' if you must quote text.\n\n"
        "OUTPUT: JSON only:\n"
        "```json\n"
        '{ "bullets": ["...", "...", "...", "...", "...", "..."] }\n'
        "```"
    )

    try:
        model = LLM_MODEL_FAST
        if len(payload) > 8_000:
            model = LLM_MODEL
        kws: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "system", "content": sys}, {"role": "user", "content": user}],
            "max_tokens": 3_500,
            "temperature": 0.3,
        }
        # Request JSON object (OpenAI; Gemini OAPI may support for compatible models). Retry if rejected.
        try:
            resp = _llm_create_with_retry(
                client, **{**kws, "response_format": {"type": "json_object"}},
            )
        except Exception as e:
            emsg = f"{e!s}".lower()
            if (
                ("response" in emsg and "format" in emsg)
                or "json_object" in emsg
                or ("unknown" in emsg and "param" in emsg)
            ):
                logger.info("Notable: retrying without response_format: %s", str(e)[:200])
                resp = _llm_create_with_retry(client, **kws)
            else:
                raise
        ch = (resp.choices[0].message.content or "").strip() if resp and resp.choices else ""
        finish_reason = resp.choices[0].finish_reason if resp and resp.choices else "unknown"
        if not ch:
            raise ValueError("empty LLM content")

        logger.debug("Notable LLM finish_reason=%s, response length=%d", finish_reason, len(ch))
        out: list[str] | None = _parse_notable_bullets_json(ch)
        if not out:
            loose = _parse_bullets_from_markdown_lines(_normalize_llm_json_string(ch))
            if loose:
                out = loose
                logger.info("Notable: used markdown/loose line parse after JSON miss (%d line(s))", len(out))
        if not out:
            logger.warning("Notable: could not parse LLM response. Raw length=%d, first 1000 chars: %s", len(ch), ch[:1000])
            raise ValueError("Notable: could not parse bullets from LLM response")
        out = [str(b).strip() for b in out if str(b).strip()][:6]
        if len(out) < 1:
            raise ValueError("no bullet strings after parse")
        out = _pad_notable_to_six(out, digest, entry)
        return out, "llm"
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        if not allow_fb:
            logger.error("Notable: LLM parse failed (strict; not using fallback). %s: %s", type(e).__name__, e)
            raise NotableLlmError(
                f"Notable: LLM did not return parseable JSON bullets ({type(e).__name__}: {e}). "
                f"Set BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true to use static bullets, or fix the prompt/parse path."
            ) from e
        logger.warning("Notable: LLM parse/complete failed — using YAML / defaults. %s: %s", type(e).__name__, e)
        return _fallback_items(entry), "yaml_fallback"
    except NotableLlmError:
        raise
    except Exception as e:
        if not allow_fb:
            logger.error("Notable: LLM failed (strict; not using fallback).", exc_info=True)
            raise NotableLlmError(
                f"Notable: LLM request failed: {e}. "
                "Set BPO_SUPPORT_NOTABLE_LLM_ALLOW_FALLBACK=true for legacy soft fallback, or fix the error."
            ) from e
        logger.warning("Notable: LLM generation failed — using YAML / defaults. Error: %s", e, exc_info=True)
        return _fallback_items(entry), "yaml_fallback"
