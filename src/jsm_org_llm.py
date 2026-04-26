"""LLM disambiguation: map a free-text customer request to a JSM organization directory name.

Used when string fuzzy-matching and optional YAML aliases fail to find any JSM org label, so
decks do not require per-customer manual alias entry for new accounts.
"""

from __future__ import annotations

import json
import os
import re
import hashlib
from typing import Any

from .config import LLM_MODEL, LLM_MODEL_FAST, logger, llm_client
from .llm_utils import _llm_create_with_retry, _strip_json_code_fence


def _jsm_llm_enabled() -> bool:
    v = (os.environ.get("BPO_JSM_ORG_LLM", "true") or "").strip().lower()
    return v not in ("0", "false", "no", "off")


def _jsm_max_candidate_chars() -> int:
    try:
        return max(5_000, int(os.environ.get("BPO_JSM_ORG_LLM_MAX_CANDIDATE_CHARS", "90000")))
    except ValueError:
        return 90_000


def _initials_subsequence(acronym: str, organization_name: str) -> bool:
    """True if the letters in *acronym* (alphanumeric only) appear in order on word starts, e.g. JCI in 'Johnson Controls Inc'."""
    n = "".join(c for c in acronym if c.isalnum()).upper()
    if not n or len(n) > 8:
        return False
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9-]*", organization_name)
    letters = [w[0].upper() for w in words if w]
    if len(letters) < 2:
        return False
    i = 0
    for c in n:
        while i < len(letters) and letters[i] != c:
            i += 1
        if i >= len(letters):
            return False
        i += 1
    return True


def prefilter_organizations_for_llm(
    terms: list[str],
    all_organizations: list[str],
    max_total_chars: int,
) -> tuple[list[str], str | None]:
    """Narrow a huge directory to fit in a single LLM prompt, without dropping obvious acronym fits."""
    if not all_organizations:
        return [], "empty JSM org directory"
    if sum(len(n) for n in all_organizations) + len(all_organizations) <= max_total_chars:
        return all_organizations, None

    from .jira_client import _score_jsm_org_candidate  # noqa: WPS433 — local import, avoid circular

    scored: list[tuple[str, float]] = []
    for o in all_organizations:
        sc = 0.0
        for t in terms:
            if not t:
                continue
            if _initials_subsequence(t, o):
                sc = max(sc, 0.82)
            sc = max(sc, _score_jsm_org_candidate(t, o))
        scored.append((o, sc))

    scored.sort(key=lambda x: -x[1])
    out: list[str] = []
    total = 0
    for o, _ in scored:
        cost = len(o) + 1
        if total + cost > max_total_chars and out:
            break
        out.append(o)
        total += cost
    n_all, n_out = len(all_organizations), len(out)
    return out, f"list truncated: included {n_out} of {n_all} (ranked by fuzzy + acronym signal)"


def _build_cache_key(tenant_key: str, terms: list[str]) -> str:
    h = hashlib.sha1("|".join(sorted(terms)).encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"{tenant_key}|{h}"


def _parse_json_content(content: str) -> dict[str, Any] | None:
    t = _strip_json_code_fence((content or "").strip())
    if not t or not t.startswith("{"):
        return None
    try:
        o = json.loads(t)
    except json.JSONDecodeError:
        return None
    if isinstance(o, dict):
        return o
    return None


def resolve_jsm_customer_organizations_llm(
    *,
    tenant_key: str,
    customer_name: str,
    all_terms: list[str],
    all_organizations: list[str],
    cache: dict[str, list[str]] | None = None,
) -> list[str]:
    """LLM: pick 0+ exact organization names from *all_organizations* for the request labels. Always validates against the directory."""
    if not _jsm_llm_enabled() or not all_organizations or not (customer_name or "").strip():
        return []
    terms = [t.strip() for t in (all_terms or []) if t and t.strip()]
    if not terms:
        return []
    ckey = _build_cache_key(tenant_key, [customer_name] + terms)
    if cache is not None and ckey in cache:
        return list(cache.get(ckey) or [])

    by_lower = {n.lower(): n for n in all_organizations}

    candidates, trunc_note = prefilter_organizations_for_llm(terms, all_organizations, _jsm_max_candidate_chars())
    body_lines = [f"  {i + 1}. {name}" for i, name in enumerate(candidates)]
    list_blob = "\n".join(body_lines) if body_lines else "(no names in candidate list)"

    sys = (
        "You map a user's customer or account label to a JSM organization name from a fixed list. "
        "The Jira Service Management 'Organizations' field may only be set to one of the directory strings exactly. "
        "If none fit, return null. Never guess a name that is not in the list. Be conservative: prefer null over a weak match. "
        "For acronyms (e.g. 'JCI'), map to a full company name only when it clearly refers to the same account."
    )
    user = (
        f"Requested account label (primary): {customer_name!r}\n"
        f"All search terms: {', '.join(repr(t) for t in terms)}\n"
    )
    if trunc_note:
        user += f"Note: {trunc_note}\n"
    user += (
        f"\nExact organization directory (choose only from this list, character-for-character):\n\n{list_blob}\n\n"
        "Reply with a JSON object only, no markdown:\n"
        '{ "organization": "exact name from the list or null" }\n'
        "Use a single best match, or null if there is no reasonable match."
    )

    try:
        cl = llm_client()
    except Exception as e:
        logger.info("JSM org LLM: no client: %s", e)
        return []

    used = LLM_MODEL if len(user) > 20_000 else LLM_MODEL_FAST
    try:
        kws: dict[str, Any] = {
            "model": used,
            "messages": [
                {"role": "system", "content": sys},
                {"role": "user", "content": user},
            ],
            "max_tokens": 400,
            "temperature": 0.0,
        }
        try:
            resp = _llm_create_with_retry(
                cl, **{**kws, "response_format": {"type": "json_object"}}
            )
        except TypeError:
            resp = _llm_create_with_retry(cl, **kws)
        except Exception as e:
            emsg = f"{e!s}".lower()
            if "response" in emsg and "format" in emsg:
                resp = _llm_create_with_retry(cl, **kws)
            else:
                raise
        ch = (resp.choices[0].message.content or "").strip() if resp and resp.choices else ""
    except Exception as e:
        logger.info("JSM org LLM: request failed: %s", e)
        if cache is not None:
            cache[ckey] = []
        return []

    data = _parse_json_content(ch)
    if not data and "{" in ch:
        data = _parse_json_content(ch[ch.index("{") :])
    if not data:
        if cache is not None:
            cache[ckey] = []
        return []

    raw = data.get("organization")
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        if cache is not None:
            cache[ckey] = []
        return []
    s = (raw or "").strip()
    exact = by_lower.get(s.lower())
    if not exact:
        logger.info("JSM org LLM: response %r is not a directory name (candidates n=%d)", s[:80], len(candidates))
        if cache is not None:
            cache[ckey] = []
        return []
    if cache is not None:
        cache[ckey] = [exact]
    return [exact]
