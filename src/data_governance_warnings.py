"""Structured data-governance warnings for LLM export (Salesforce ↔ Pendo ↔ Jira HELP scope)."""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from .portfolio_salesforce_allowlist import format_salesforce_label_activity_hint

_governance_warnings: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "data_governance_warnings",
    default=None,
)


def clear_data_governance_warnings() -> None:
    """Reset the per-export warning list (call at start of ``export_diagnostics_scope``)."""
    _governance_warnings.set([])


def _store() -> list[dict[str, Any]]:
    cur = _governance_warnings.get()
    if cur is None:
        cur = []
        _governance_warnings.set(cur)
    return cur


def record_data_governance_warning(
    category: str,
    message: str,
    *,
    salesforce_activity: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    """Record one governance warning for the active export (deduped by message text)."""
    msg = (message or "").strip()
    if not msg:
        return
    store = _store()
    seen = {str(e.get("message") or "").strip() for e in store}
    if msg in seen:
        return
    entry: dict[str, Any] = {
        "category": (category or "general").strip(),
        "message": msg,
    }
    if salesforce_activity:
        entry["salesforce_activity"] = salesforce_activity
    if context:
        entry["context"] = context
    store.append(entry)


def get_data_governance_warnings() -> list[dict[str, Any]]:
    """Copy of warnings recorded for the current export context."""
    cur = _governance_warnings.get()
    return list(cur) if cur else []


_LOG_PREFIX_TO_CATEGORY: tuple[tuple[str, str], ...] = (
    ("Portfolio (Salesforce allowlist):", "portfolio_sf_pendo_prefix_unmatched"),
    ("HELP scope: no JSM Organizations match", "help_jsm_org_fallback"),
    (
        "active Salesforce Customer Entity label(s) have no Pendo prefix",
        "salesforce_active_no_pendo_in_export",
    ),
)


def _category_from_log_message(message: str) -> str:
    for prefix, cat in _LOG_PREFIX_TO_CATEGORY:
        if prefix in message:
            return cat
    if message.lower().startswith("llm export — customer filter:"):
        return "llm_export_customer_filter"
    if message.lower().startswith("customer filter:"):
        return "llm_export_customer_filter"
    return "export_warning"


def _dedupe_key_for_entry(category: str, message: str, *, salesforce_label: str = "") -> str | None:
    """Secondary dedupe key when message text differs (e.g. log vs meta SF hint)."""
    if salesforce_label:
        return f"{category}:{salesforce_label.strip().lower()}"
    if category == "portfolio_sf_pendo_prefix_unmatched":
        marker = "Salesforce label "
        if marker in message:
            tail = message.split(marker, 1)[1]
            label = tail.split("'", 2)[1] if "'" in tail else ""
            if label:
                return f"{category}:{label.strip().lower()}"
    if category == "help_jsm_org_fallback":
        marker = "match for "
        if marker in message:
            tail = message.split(marker, 1)[1]
            label = tail.split("'", 2)[1] if "'" in tail else ""
            if label:
                return f"{category}:{label.strip().lower()}"
    return None


def _ingest_log_message(
    store: list[dict[str, Any]],
    seen: set[str],
    message: str,
    *,
    keys_seen: set[str] | None = None,
) -> None:
    msg = (message or "").strip()
    if not msg or msg in seen:
        return
    cat = _category_from_log_message(msg)
    if keys_seen is not None:
        dk = _dedupe_key_for_entry(cat, msg)
        if dk and dk in keys_seen:
            return
    seen.add(msg)
    if keys_seen is not None:
        dk = _dedupe_key_for_entry(cat, msg)
        if dk:
            keys_seen.add(dk)
    store.append(
        {
            "category": cat,
            "message": msg,
            "source": "cortex_logger",
        }
    )


def _ingest_unmatched_sf_allowlist(
    store: list[dict[str, Any]],
    seen: set[str],
    item: Any,
    *,
    keys_seen: set[str] | None = None,
) -> None:
    if isinstance(item, dict):
        label = str(item.get("salesforce_label") or "").strip()
        activity = item.get("salesforce_activity")
        if isinstance(activity, dict) and activity:
            hint = format_salesforce_label_activity_hint(activity)
            msg = (
                f"Portfolio (Salesforce allowlist): no Pendo customer prefix matches "
                f"Salesforce label {label!r} (visitor / sitename token) — skipping. {hint}"
            )
        else:
            msg = (
                f"Portfolio (Salesforce allowlist): no Pendo customer prefix matches "
                f"Salesforce label {label!r} (visitor / sitename token) — skipping."
            )
    else:
        label = str(item).strip()
        msg = (
            f"Portfolio (Salesforce allowlist): no Pendo customer prefix matches "
            f"Salesforce label {label!r} (visitor / sitename token) — skipping."
        )
    dk = _dedupe_key_for_entry(
        "portfolio_sf_pendo_prefix_unmatched",
        msg,
        salesforce_label=label,
    )
    if keys_seen is not None and dk and dk in keys_seen:
        # Replace log-only entry with richer meta (SF activity hint).
        for i, existing in enumerate(store):
            if _dedupe_key_for_entry(
                str(existing.get("category") or ""),
                str(existing.get("message") or ""),
                salesforce_label=str(existing.get("salesforce_label") or label),
            ) == dk:
                store[i] = {
                    "category": "portfolio_sf_pendo_prefix_unmatched",
                    "message": msg,
                    "salesforce_label": label,
                    "salesforce_activity": activity if isinstance(activity, dict) else None,
                    "source": "salesforce_allowlist_meta",
                }
                seen.discard(str(existing.get("message") or "").strip())
                seen.add(msg)
                return
        return
    if msg in seen:
        return
    seen.add(msg)
    if keys_seen is not None and dk:
        keys_seen.add(dk)
    store.append(
        {
            "category": "portfolio_sf_pendo_prefix_unmatched",
            "message": msg,
            "salesforce_label": label,
            "salesforce_activity": activity if isinstance(activity, dict) else None,
            "source": "salesforce_allowlist_meta",
        }
    )


def build_data_governance_warning_entries(
    report: dict[str, Any] | None = None,
    export_diag: Any | None = None,
) -> list[dict[str, Any]]:
    """Merge explicit records, export log warnings, and report meta into one list."""
    store: list[dict[str, Any]] = []
    seen: set[str] = set()
    keys_seen: set[str] = set()

    for entry in get_data_governance_warnings():
        msg = str(entry.get("message") or "").strip()
        if not msg or msg in seen:
            continue
        cat = str(entry.get("category") or "general")
        ctx = entry.get("context") if isinstance(entry.get("context"), dict) else {}
        sf_label = str(entry.get("salesforce_label") or ctx.get("salesforce_label") or "")
        dk = _dedupe_key_for_entry(cat, msg, salesforce_label=sf_label)
        if dk and dk in keys_seen:
            continue
        seen.add(msg)
        if dk:
            keys_seen.add(dk)
        store.append(dict(entry))

    if export_diag is not None:
        for w in getattr(export_diag, "warnings", None) or []:
            if isinstance(w, str):
                _ingest_log_message(store, seen, w, keys_seen=keys_seen)

    rep = report if isinstance(report, dict) else {}
    filt = rep.get("_llm_export_customer_filter")
    if isinstance(filt, dict):
        for w in filt.get("warnings") or []:
            if isinstance(w, str) and w.strip():
                _ingest_log_message(
                    store,
                    seen,
                    f"Customer filter: {w.strip()}",
                    keys_seen=keys_seen,
                )

    uni = rep.get("_llm_export_salesforce_universe")
    if isinstance(uni, dict):
        without = uni.get("salesforce_labels_without_pendo") or []
        if isinstance(without, list) and without:
            sample = ", ".join(str(x) for x in without[:12])
            extra = f" (+{len(without) - 12} more)" if len(without) > 12 else ""
            _ingest_log_message(
                store,
                seen,
                "LLM export: "
                f"{len(without)} active Salesforce Customer Entity label(s) have no Pendo prefix match "
                f"(included in §1 with Salesforce facts only): {sample}{extra}",
                keys_seen=keys_seen,
            )

    allow_meta = rep.get("_salesforce_portfolio_allowlist_meta")
    if isinstance(allow_meta, dict):
        for item in allow_meta.get("salesforce_labels_unmatched") or []:
            _ingest_unmatched_sf_allowlist(store, seen, item, keys_seen=keys_seen)

    return store


_CATEGORY_TITLES: dict[str, str] = {
    "portfolio_sf_pendo_prefix_unmatched": "Salesforce portfolio label — no Pendo prefix",
    "help_jsm_org_fallback": "HELP Jira scope — JSM Organizations fallback",
    "salesforce_active_no_pendo_in_export": "Active Salesforce customer — no Pendo metrics in export",
    "llm_export_customer_filter": "LLM export customer filter",
    "export_warning": "Export warning",
}


def render_data_governance_markdown_lines(entries: list[dict[str, Any]]) -> list[str]:
    """Markdown bullets for ``## Data Governance``."""
    lines: list[str] = [
        "Customer identity alignment, Salesforce contract status, and HELP Jira scoping "
        "issues detected during this export run.",
        "",
    ]
    if not entries:
        lines.append("- *No data-governance warnings were recorded for this run.*")
        return lines

    for i, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            continue
        cat = str(entry.get("category") or "general")
        title = _CATEGORY_TITLES.get(cat, cat.replace("_", " "))
        msg = str(entry.get("message") or "").strip()
        lines.append(f"### {i}. {title}")
        lines.append("")
        lines.append(f"- {msg}")
        label = entry.get("salesforce_label")
        if label:
            lines.append(f"- **Salesforce label:** `{label}`")
        activity = entry.get("salesforce_activity")
        if isinstance(activity, dict) and activity:
            hint = format_salesforce_label_activity_hint(activity)
            if hint and hint not in msg:
                lines.append(f"- **{hint}**")
        ctx = entry.get("context")
        if isinstance(ctx, dict) and ctx:
            for k, v in list(ctx.items())[:8]:
                if v is not None and str(v).strip():
                    lines.append(f"- **{k}:** `{v}`")
        lines.append("")
    return lines
