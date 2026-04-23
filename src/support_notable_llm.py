"""LLM-driven \"Notable\" slide for the support review deck: digest + six bullets for CS leaders."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from .config import LLM_MODEL, LLM_MODEL_FAST, LLM_PROVIDER, logger, llm_client
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence

_MAX_DIGEST_JSON_CHARS = 24_000


def _flag_use_llm() -> bool:
    v = (os.environ.get("BPO_SUPPORT_NOTABLE_LLM", "true") or "").strip().lower()
    return v not in ("0", "false", "no", "off")


def _trim_tickets(issues: list[dict] | None, n: int = 10) -> list[dict[str, str]]:
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
    r: dict[str, Any] = {
        "audience": c_disp,
        "date_range": _date_range(
            days, report.get("quarter"), report.get("quarter_start"), report.get("quarter_end")
        ),
        "deck_slides_built": slide_titles,
    }

    ctm = j.get("customer_ticket_metrics")
    if isinstance(ctm, dict) and ctm and not ctm.get("error"):
        hm: dict[str, Any] = {}
        for k in (
            "open", "resolved", "created", "days", "customer", "unresolved", "in_progress",
        ):
            if k in ctm and ctm[k] is not None:
                hm[k] = ctm[k]
        for subk in ("by_priority", "by_status", "by_type"):
            d = ctm.get(subk)
            if isinstance(d, dict) and d:
                hm[subk] = dict(list(d.items())[:14])
        for sub in ("ttfr", "ttr"):
            if sub in ctm and isinstance(ctm[sub], dict):
                hm[sub] = ctm[sub]
        if hm:
            r["help_account_metrics"] = hm
    elif isinstance(ctm, dict) and ctm.get("error"):
        r["help_account_metrics_error"] = str(ctm.get("error"))[:400]

    def _recent_block(name: str) -> None:
        blob = j.get(name) or {}
        if not isinstance(blob, dict):
            return
        r[name] = {
            "error": (blob.get("error") or "")[:300] or None,
            "recently_opened": _trim_tickets(blob.get("recently_opened") or []),
            "recently_closed": _trim_tickets(blob.get("recently_closed") or []),
        }

    _recent_block("customer_help_recent")
    _recent_block("customer_project_recent")
    _recent_block("lean_project_recent")

    for proj, key in (("HELP", "help_resolved_by_assignee"), ("CUSTOMER", "customer_resolved_by_assignee"), ("LEAN", "lean_resolved_by_assignee")):
        blob = j.get(key) or {}
        if not isinstance(blob, dict) or not blob:
            continue
        r[key] = {
            "project": proj,
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
            r[mkey] = {**{k: m.get(k) for k in (
                "open", "resolved", "ttfr", "ttr", "backlog", "unresolved", "in_progress", "in_support_queue", "in_eng", "in_customer", "in_customer_queued",
            ) if k in m}, "project": proj}

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
        "Adoption and depth: Are the right people using the product in the ways that matter for business outcomes?",
        "Account health and risk: Churn, renewal, adoption trends, and what would worry you on this account.",
        "Value proof: Concrete metrics and outcomes the customer and their execs would recognize as progress or ROI.",
        "Champions and executive coverage: Sponsors, power users, and access at the right level.",
        "Support, friction, and product gaps: Ticket patterns, training vs. real gaps, and recurring blockers to value.",
        "Expectations and follow-through: What was committed, what shipped, what is still open, and what is next.",
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


def _parse_notable_bullets_json(
    content: str,
) -> list[str] | None:
    """Parse `{ \"bullets\": [...] }` from the model. Returns None to signal fallback to loose parsing."""
    raw = _strip_json_code_fence(_normalize_llm_json_string(content))
    if not raw:
        return None
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
            if attempt == 0:
                continue
            return None
        if not isinstance(data, dict):
            return None
        bullets = data.get("bullets")
        if not isinstance(bullets, list):
            return None
        out = [str(b).strip() for b in bullets if str(b).strip()]
        return out if out else None
    return None


def generate_notable_bullets_via_llm(
    digest: dict[str, Any],
    entry: dict[str, Any],
) -> tuple[list[str], str]:
    """Return (6 bullets, source) where source is 'llm' or 'yaml_fallback' or 'env_off'."""
    if not _flag_use_llm():
        return _fallback_items(entry), "env_off"
    try:
        client = llm_client()
    except RuntimeError as e:
        logger.warning("Notable: LLM disabled (%s) — using YAML / default bullets", e)
        return _fallback_items(entry), "yaml_fallback"

    payload = json.dumps(digest, indent=0, default=str, ensure_ascii=False)[:_MAX_DIGEST_JSON_CHARS]
    sys = (
        "You write concise, executive-style bullets for Customer Success and CS leadership. "
        "Use only the JSON facts provided. Do not invent Jira data that is not supported by the digest; "
        "it is fine to name themes or questions implied by the numbers. Each bullet 1-3 short sentences, "
        "or one sentence plus a second clause, max ~320 characters. Emphasize what would matter to renewal, health, and partnership."
    )
    user = (
        "Here is the support-review digest (Jira, projects HELP / CUSTOMER / LEAN) for a multi-slide review deck. "
        "The slide will list exactly six points titled \"Notable\" for a Customer Success leader.\n\n"
        f"{payload}\n\n"
        "Output rules for JSON: respond with a single JSON object and nothing else. "
        "The key is \"bullets\" (an array of exactly 6 strings). "
        "Inside each string, do not use raw double-quote characters—use single quotes for emphasis or the word *quote* if needed. "
        "Do not put line breaks inside a string value. Escape any required character as in JSON. Shape:\n"
        '{ "bullets": [ "one", "two", "three", "four", "five", "six" ] }'
    )

    try:
        model = LLM_MODEL_FAST
        if len(payload) > 8_000:
            model = LLM_MODEL
        kws: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "system", "content": sys}, {"role": "user", "content": user}],
            "max_tokens": 1_800,
            "temperature": 0.25,
        }
        # OpenAI: request valid JSON from the model (avoids unterminated strings from prose).
        if (LLM_PROVIDER or "").lower() == "openai":
            kws["response_format"] = {"type": "json_object"}
        resp = _llm_create_with_retry(client, **kws)
        ch = (resp.choices[0].message.content or "").strip() if resp and resp.choices else ""
        if not ch:
            raise ValueError("empty LLM content")

        out: list[str] | None = _parse_notable_bullets_json(ch)
        if not out:
            loose = _parse_bullets_from_markdown_lines(_normalize_llm_json_string(ch))
            if loose:
                out = loose
                logger.info("Notable: used markdown/loose line parse after JSON miss (%d line(s))", len(out))
        if not out:
            raise ValueError("Notable: could not parse bullets from LLM response")
        out = [str(b).strip() for b in out if str(b).strip()][:6]
        if len(out) < 1:
            raise ValueError("no bullet strings after parse")
        # Pad if model returned too few, using generic CS themes (still short)
        if len(out) < 6:
            fb = [x for x in _fallback_items({}) if x not in out]
            for s in fb:
                if len(out) >= 6:
                    break
                if s not in out:
                    out.append(s)
        out = out[:6]
        if len(out) < 6:
            raise ValueError("could not get 6 bullets after pad")
        return out, "llm"
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        logger.warning("Notable: LLM parse/complete failed — using YAML / defaults. %s: %s", type(e).__name__, e)
        return _fallback_items(entry), "yaml_fallback"
    except Exception as e:
        logger.warning("Notable: LLM generation failed — using YAML / defaults. Error: %s", e, exc_info=True)
        return _fallback_items(entry), "yaml_fallback"
