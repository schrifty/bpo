"""String truncation helpers for large export payloads."""

from __future__ import annotations

from typing import Any


def truncate_strings_in_obj(
    obj: Any,
    *,
    max_str: int,
    max_list_items: int,
    max_dict_keys: int | None = None,
) -> Any:
    """Recursively shorten long strings and cap list lengths for prompt size limits."""
    if isinstance(obj, str):
        return obj if len(obj) <= max_str else obj[: max_str - 1] + "…"
    if isinstance(obj, list):
        return [
            truncate_strings_in_obj(
                item,
                max_str=max_str,
                max_list_items=max_list_items,
                max_dict_keys=max_dict_keys,
            )
            for item in obj[:max_list_items]
        ]
    if isinstance(obj, dict):
        items = sorted(obj.items(), key=lambda kv: str(kv[0]))
        if max_dict_keys is not None and len(items) > max_dict_keys:
            items = items[:max_dict_keys]
        return {
            key: truncate_strings_in_obj(
                value,
                max_str=max_str,
                max_list_items=max_list_items,
                max_dict_keys=max_dict_keys,
            )
            for key, value in items
        }
    return obj
