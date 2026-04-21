"""LeanDNA Material Shortages API client for BPO.

Fetches time-series shortage data including weekly/daily forecasts, criticality levels,
scheduled deliveries, and production order impacts.

Thread-safe caching with Drive backup for expensive API calls.
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Any

import requests

from .config import logger

_weekly_cache: dict[str, Any] | None = None
_weekly_cache_timestamp: datetime | None = None
_cache_lock = threading.Lock()


def _get_base_url() -> str:
    """Get LeanDNA Data API base URL from config."""
    from .config import LEANDNA_DATA_API_BASE_URL
    return LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api"


def _get_bearer_token() -> str | None:
    """Get LeanDNA bearer token from config."""
    from .config import LEANDNA_DATA_API_BEARER_TOKEN
    return LEANDNA_DATA_API_BEARER_TOKEN


def _get_cache_ttl_hours() -> int:
    """Get cache TTL in hours from config (default 12h for shortage data)."""
    from .config import LEANDNA_SHORTAGE_CACHE_TTL_HOURS
    return LEANDNA_SHORTAGE_CACHE_TTL_HOURS or 12


def _headers(requested_sites: str | None = None) -> dict[str, str]:
    """Build request headers with auth and optional site scoping."""
    token = _get_bearer_token()
    if not token:
        raise ValueError("LEANDNA_DATA_API_BEARER_TOKEN not configured in .env")
    
    h = {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json",
        "User-Agent": "bpo-leandna-shortage-client/1.0",
    }
    if requested_sites:
        h["RequestedSites"] = requested_sites.strip()
    return h


def _cache_key(endpoint: str, sites: str | None) -> str:
    """Generate cache key for endpoint + site list."""
    site_part = sites or "all_sites"
    combined = f"{endpoint}:{site_part}"
    return hashlib.md5(combined.encode()).hexdigest()[:16]


def _is_weekly_cache_valid() -> bool:
    """Check if weekly in-memory cache is valid based on TTL."""
    global _weekly_cache_timestamp
    if _weekly_cache is None or _weekly_cache_timestamp is None:
        return False
    
    age_hours = (datetime.now(timezone.utc) - _weekly_cache_timestamp).total_seconds() / 3600
    return age_hours < _get_cache_ttl_hours()


def _try_load_from_drive(endpoint: str, cache_key: str) -> list[dict] | None:
    """Attempt to load cached shortage data from Drive."""
    try:
        from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID
        if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
            return None
        
        from .network_utils import network_timeout
        from .slides_api import _get_service
        _, drive, _ = _get_service()
        
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"shortage_{endpoint}_{cache_key}_{date_str}.json"
        
        query = f"name='{filename}' and trashed=false"
        with network_timeout(30.0, "Drive file listing"):
            results = drive.files().list(
                q=query,
                fields="files(id, name, modifiedTime)",
                spaces="drive",
                pageSize=5,
            ).execute()
        
        files = results.get("files", [])
        if not files:
            logger.debug("LeanDNA Shortage: no Drive cache found for %s", filename)
            return None
        
        file_info = files[0]
        
        # Check file age
        modified = file_info.get("modifiedTime", "")
        if modified:
            from dateutil import parser
            mod_dt = parser.parse(modified)
            age_hours = (datetime.now(timezone.utc) - mod_dt).total_seconds() / 3600
            if age_hours >= _get_cache_ttl_hours():
                logger.debug("LeanDNA Shortage: Drive cache is stale (%.1fh old)", age_hours)
                return None
        
        # Download and parse
        request = drive.files().get_media(fileId=file_info["id"])
        with network_timeout(30.0, "Drive file download"):
            content = request.execute()
        data = json.loads(content.decode("utf-8"))
        logger.info("LeanDNA Shortage: loaded %d items from Drive cache (%s)", len(data), filename)
        return data
        
    except Exception as e:
        logger.debug("Failed to load LeanDNA Shortage from Drive cache: %s", e)
        return None


def _save_to_drive(data: list[dict], endpoint: str, cache_key: str) -> None:
    """Save shortage data to Drive cache."""
    try:
        from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID
        if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
            return
        
        from .network_utils import network_timeout
        from .slides_api import _get_service
        from googleapiclient.http import MediaInMemoryUpload
        
        _, drive, _ = _get_service()
        
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"shortage_{endpoint}_{cache_key}_{date_str}.json"
        
        content = json.dumps(data, indent=2).encode("utf-8")
        media = MediaInMemoryUpload(content, mimetype="application/json", resumable=True)
        
        meta = {
            "name": filename,
            "parents": [GOOGLE_QBR_GENERATOR_FOLDER_ID],
            "mimeType": "application/json",
        }
        
        with network_timeout(30.0, "Drive file creation"):
            file_obj = drive.files().create(body=meta, media_body=media, fields="id").execute()
        logger.info("LeanDNA Shortage: saved %d items to Drive cache (%s, id=%s)", len(data), filename, file_obj["id"][:16])
        
    except Exception as e:
        logger.warning("Failed to save LeanDNA Shortage to Drive cache: %s", e)


def _normalize_weekly_buckets(row: dict) -> list[dict]:
    """Convert bucket1...bucket32 fields to list of bucket objects.
    
    Args:
        row: Shortage item row with bucketN* fields.
    
    Returns:
        List of normalized bucket dicts (up to 32 buckets).
    """
    buckets = []
    for i in range(1, 33):
        qty = row.get(f"bucket{i}quantity")
        start = row.get(f"bucket{i}startDate")
        
        # Only include buckets with date info
        if start is None:
            continue
        
        buckets.append({
            "week_num": i,
            "quantity": float(qty) if qty is not None else 0.0,
            "start_date": start,
            "end_date": row.get(f"bucket{i}endDate"),
            "criticality": row.get(f"bucket{i}criticality") or "Unknown",
        })
    
    return buckets


def _normalize_daily_buckets(row: dict) -> list[dict]:
    """Convert day1...day45 fields to list of day objects.
    
    Args:
        row: Shortage item row with dayN* fields.
    
    Returns:
        List of normalized day dicts (up to 45 days).
    """
    days = []
    for i in range(1, 46):
        date_val = row.get(f"day{i}date")
        if date_val is None:
            continue
        
        qty = row.get(f"day{i}quantity")
        supply = row.get(f"day{i}supply")
        reqs = row.get(f"day{i}requirements")
        crit = row.get(f"day{i}criticality") or "Unknown"
        
        days.append({
            "day_num": i,
            "date": date_val,
            "quantity": float(qty) if qty is not None else 0.0,
            "supply": float(supply) if supply is not None else 0.0,
            "requirements": float(reqs) if reqs is not None else 0.0,
            "criticality": crit,
        })
    
    return days


def get_shortages_by_item_weekly(
    sites: str | None = None,
    force_refresh: bool = False,
) -> list[dict]:
    """Retrieve weekly shortage forecast (32 weeks) from LeanDNA API.
    
    Args:
        sites: Comma-separated site IDs (optional; defaults to all authorized sites).
        force_refresh: Bypass cache and fetch fresh data.
    
    Returns:
        List of shortage item records with normalized bucket lists.
        Each item has:
        - Core fields: itemCode, itemDescription, site, criticalityLevel, daysInShortage
        - ctbShortageImpactedValue: Dollar impact on Clear-to-Build
        - buckets: List of 32 weekly forecast buckets (quantity, criticality, dates)
        - firstCriticalBucketWeek: Date of first critical bucket (if any)
    
    Raises:
        ValueError: If bearer token not configured.
        requests.HTTPError: If API returns error status.
    """
    global _weekly_cache, _weekly_cache_timestamp
    
    endpoint = "weekly"
    cache_key = _cache_key(endpoint, sites)
    
    # Check in-memory cache
    if not force_refresh and _is_weekly_cache_valid():
        logger.debug("LeanDNA Shortage (weekly): using in-memory cache (%d items)", len(_weekly_cache or []))
        return _weekly_cache or []
    
    with _cache_lock:
        # Double-check after acquiring lock
        if not force_refresh and _is_weekly_cache_valid():
            return _weekly_cache or []
        
        # Try Drive cache
        if not force_refresh:
            drive_data = _try_load_from_drive(endpoint, cache_key)
            if drive_data:
                _weekly_cache = drive_data
                _weekly_cache_timestamp = datetime.now(timezone.utc)
                return drive_data
        
        # Fetch from API
        url = f"{_get_base_url()}/data/MaterialShortages/ShortagesByItem/Weekly"
        logger.info("LeanDNA Shortage (weekly): fetching from API (sites=%s)", sites or "all")
        
        try:
            response = requests.get(url, headers=_headers(sites), timeout=180)
            response.raise_for_status()
            raw_data = response.json()
            
            if not isinstance(raw_data, list):
                logger.error("LeanDNA Shortage API returned non-list: %s", type(raw_data))
                return []
            
            # Normalize bucket fields
            data = []
            for row in raw_data:
                normalized = {
                    "itemCode": row.get("itemCode"),
                    "itemDescription": row.get("itemDescription"),
                    "itemAlias": row.get("itemAlias"),
                    "site": row.get("site"),
                    "criticalityLevel": row.get("criticalityLevel"),
                    "criticalityLevelLabel": row.get("criticalityLevelLabel"),
                    "daysInShortage": row.get("daysInShortage"),
                    "ctbShortageImpactedValue": row.get("ctbShortageImpactedValue"),
                    "ctbImpactedOrdersSingleShortageCount": row.get("ctbImpactedOrdersSingleShortageCount"),
                    "buyer": row.get("buyer"),
                    "planner": row.get("planner"),
                    "supplierName": row.get("supplierName"),
                    "firstCriticalBucketWeek": row.get("firstCriticalBucketWeek"),
                    "buckets": _normalize_weekly_buckets(row),
                    
                    # PO tracking fields
                    "firstPurchaseOrder": row.get("firstPurchaseOrder"),
                    "firstPORequestedDate": row.get("firstPORequestedDate"),
                    "firstPOCommitDate": row.get("firstPOCommitDate"),
                    "firstPoStatus": row.get("firstPoStatus"),
                    
                    # Impacted orders
                    "firstImpactedOrder": row.get("firstImpactedOrder"),
                    "firstImpactedOrderDate": row.get("firstImpactedOrderDate"),
                    
                    # Inventory context
                    "onHand": row.get("onHand"),
                    "onOrder": row.get("onOrder"),
                    "safetyStock": row.get("safetyStock"),
                    "avgDailyDemand": row.get("avgDailyDemand"),
                }
                data.append(normalized)
            
            logger.info("LeanDNA Shortage (weekly): fetched %d items from API", len(data))
            
            # Update cache
            _weekly_cache = data
            _weekly_cache_timestamp = datetime.now(timezone.utc)
            
            # Save to Drive
            try:
                _save_to_drive(data, endpoint, cache_key)
            except Exception as e:
                logger.warning("Drive cache save failed (non-fatal): %s", e)
            
            return data
            
        except requests.HTTPError as e:
            logger.error("LeanDNA Shortage API error: %s", e)
            if e.response is not None:
                logger.error("Response body: %s", e.response.text[:500])
            raise
        except Exception as e:
            logger.error("LeanDNA Shortage fetch failed: %s", e)
            raise


def get_shortages_by_item_daily(sites: str | None = None) -> list[dict]:
    """Retrieve daily shortage forecast (45 days) from LeanDNA API.
    
    NOTE: Not cached in-memory (less frequently used); Drive cache only.
    
    Args:
        sites: Comma-separated site IDs (optional).
    
    Returns:
        List of shortage item records with normalized daily buckets.
    """
    endpoint = "daily"
    cache_key = _cache_key(endpoint, sites)
    
    # Try Drive cache
    drive_data = _try_load_from_drive(endpoint, cache_key)
    if drive_data:
        return drive_data
    
    url = f"{_get_base_url()}/data/MaterialShortages/ShortagesByItem/Daily"
    logger.info("LeanDNA Shortage (daily): fetching from API (sites=%s)", sites or "all")
    
    try:
        response = requests.get(url, headers=_headers(sites), timeout=180)
        response.raise_for_status()
        raw_data = response.json()
        
        if not isinstance(raw_data, list):
            logger.error("LeanDNA Shortage (daily) API returned non-list: %s", type(raw_data))
            return []
        
        # Normalize
        data = []
        for row in raw_data:
            normalized = {
                "itemCode": row.get("itemCode"),
                "itemDescription": row.get("itemDescription"),
                "site": row.get("site"),
                "criticalityLevel": row.get("criticalityLevel"),
                "daysInShortage": row.get("daysInShortage"),
                "ctbShortageImpactedValue": row.get("ctbShortageImpactedValue"),
                "firstCriticalBucketDay": row.get("firstCriticalBucketDay"),
                "days": _normalize_daily_buckets(row),
            }
            data.append(normalized)
        
        logger.info("LeanDNA Shortage (daily): fetched %d items from API", len(data))
        
        # Save to Drive
        try:
            _save_to_drive(data, endpoint, cache_key)
        except Exception as e:
            logger.warning("Drive cache save failed (non-fatal): %s", e)
        
        return data
        
    except Exception as e:
        logger.error("LeanDNA Shortage (daily) fetch failed: %s", e)
        raise


def get_shortages_by_order(sites: str | None = None) -> list[dict]:
    """Retrieve shortage-by-production-order report from LeanDNA API.
    
    Links shortages to customer orders and production orders for impact analysis.
    
    NOTE: Not cached (less frequently used for QBR; mainly for deep dive deck).
    
    Args:
        sites: Comma-separated site IDs (optional).
    
    Returns:
        List of shortage-by-order records.
    """
    url = f"{_get_base_url()}/data/MaterialShortages/ShortagesByOrder"
    logger.info("LeanDNA Shortage (by order): fetching from API (sites=%s)", sites or "all")
    
    try:
        response = requests.get(url, headers=_headers(sites), timeout=180)
        response.raise_for_status()
        data = response.json()
        
        if not isinstance(data, list):
            logger.error("LeanDNA Shortage (by order) API returned non-list: %s", type(data))
            return []
        
        logger.info("LeanDNA Shortage (by order): fetched %d records from API", len(data))
        return data
        
    except Exception as e:
        logger.error("LeanDNA Shortage (by order) fetch failed: %s", e)
        raise


def get_shortages_with_scheduled_deliveries_weekly(sites: str | None = None) -> list[dict]:
    """Retrieve weekly shortages with scheduled delivery tracking.
    
    Same as weekly endpoint but includes:
    - scheduledDeliveries (count)
    - scheduledQuantity (total qty scheduled)
    - firstDeliveryDate, firstDeliveryQty, firstDeliveryTrackingNumber
    
    NOTE: Not cached in-memory; Drive cache only.
    
    Args:
        sites: Comma-separated site IDs (optional).
    
    Returns:
        List of shortage records with delivery tracking.
    """
    endpoint = "weekly_with_deliveries"
    cache_key = _cache_key(endpoint, sites)
    
    # Try Drive cache
    drive_data = _try_load_from_drive(endpoint, cache_key)
    if drive_data:
        return drive_data
    
    url = f"{_get_base_url()}/data/MaterialShortages/ShortagesByItemWithScheduledDeliveries/Weekly"
    logger.info("LeanDNA Shortage (weekly+deliveries): fetching from API (sites=%s)", sites or "all")
    
    try:
        response = requests.get(url, headers=_headers(sites), timeout=180)
        response.raise_for_status()
        raw_data = response.json()
        
        if not isinstance(raw_data, list):
            logger.error("LeanDNA Shortage (weekly+deliveries) API returned non-list")
            return []
        
        # Normalize (same as weekly, plus delivery fields)
        data = []
        for row in raw_data:
            normalized = {
                "itemCode": row.get("itemCode"),
                "site": row.get("site"),
                "criticalityLevel": row.get("criticalityLevel"),
                "daysInShortage": row.get("daysInShortage"),
                "ctbShortageImpactedValue": row.get("ctbShortageImpactedValue"),
                "buckets": _normalize_weekly_buckets(row),
                
                # Delivery tracking
                "scheduledDeliveries": row.get("scheduledDeliveries"),
                "scheduledQuantity": row.get("scheduledQuantity"),
                "firstDeliveryDate": row.get("firstDeliveryDate"),
                "firstDeliveryQty": row.get("firstDeliveryQty"),
                "firstDeliveryTrackingNumber": row.get("firstDeliveryTrackingNumber"),
            }
            data.append(normalized)
        
        logger.info("LeanDNA Shortage (weekly+deliveries): fetched %d items from API", len(data))
        
        # Save to Drive
        try:
            _save_to_drive(data, endpoint, cache_key)
        except Exception as e:
            logger.warning("Drive cache save failed (non-fatal): %s", e)
        
        return data
        
    except Exception as e:
        logger.error("LeanDNA Shortage (weekly+deliveries) fetch failed: %s", e)
        raise


def aggregate_shortage_forecast(
    weekly_data: list[dict],
    weeks_forward: int = 12,
) -> dict[str, Any]:
    """Aggregate weekly shortage data into time-series forecast.
    
    Args:
        weekly_data: Output from get_shortages_by_item_weekly().
        weeks_forward: Number of weeks to include (default 12).
    
    Returns:
        Dict with:
        - buckets: List of weekly aggregate dicts (total_qty, critical_items, high_items, etc.)
        - peak_week: Date of week with highest total shortage qty
        - total_shortage_value: Sum of CTB impact across all items
        - total_items: Total unique items in shortage
        - critical_items: Count of items with criticality >= 3
    """
    if not weekly_data:
        return {
            "buckets": [],
            "peak_week": None,
            "total_shortage_value": 0.0,
            "total_items": 0,
            "critical_items": 0,
        }
    
    # Build week-level aggregates
    week_agg: dict[str, dict] = {}
    
    for item in weekly_data:
        buckets = item.get("buckets", [])
        for bucket in buckets[:weeks_forward]:
            week_start = bucket["start_date"]
            if not week_start:
                continue
            
            if week_start not in week_agg:
                week_agg[week_start] = {
                    "week_start": week_start,
                    "week_end": bucket["end_date"],
                    "total_qty": 0.0,
                    "critical_items": 0,
                    "high_items": 0,
                    "medium_items": 0,
                    "low_items": 0,
                }
            
            agg = week_agg[week_start]
            agg["total_qty"] += bucket["quantity"]
            
            crit = (bucket.get("criticality") or "").lower()
            if crit == "critical":
                agg["critical_items"] += 1
            elif crit == "high":
                agg["high_items"] += 1
            elif crit == "medium":
                agg["medium_items"] += 1
            elif crit == "low":
                agg["low_items"] += 1
    
    # Sort by date
    buckets_list = sorted(week_agg.values(), key=lambda x: x["week_start"])
    
    # Find peak week
    peak_week = None
    if buckets_list:
        peak_bucket = max(buckets_list, key=lambda x: x["total_qty"])
        peak_week = peak_bucket["week_start"]
    
    # Total items and critical count
    total_items = len(weekly_data)
    critical_items = sum(
        1 for i in weekly_data
        if isinstance(i.get("criticalityLevel"), (int, float)) and i["criticalityLevel"] >= 3
    )
    
    # Total CTB impact
    total_value = sum(
        float(i.get("ctbShortageImpactedValue", 0) or 0)
        for i in weekly_data
    )
    
    return {
        "buckets": buckets_list,
        "peak_week": peak_week,
        "total_shortage_value": round(total_value),
        "total_items": total_items,
        "critical_items": critical_items,
    }


def get_critical_shortages_timeline(
    weekly_data: list[dict],
    threshold: int = 3,
    max_items: int = 20,
) -> list[dict]:
    """Extract items with critical shortage buckets and build timeline.
    
    Args:
        weekly_data: Output from get_shortages_by_item_weekly().
        threshold: Minimum criticality level (numeric 1-5; typically 3+).
        max_items: Max items to return.
    
    Returns:
        List of critical shortage items sorted by CTB impact descending.
        Each item has:
        - itemCode, itemDescription, site
        - firstCriticalWeek: Date of first critical bucket
        - daysInShortage
        - ctbImpact: Dollar impact
        - criticalityLevel
        - poStatus: first PO status (late/on-time/unknown)
    """
    critical = []
    
    for item in weekly_data:
        crit_level = item.get("criticalityLevel")
        if not isinstance(crit_level, (int, float)) or crit_level < threshold:
            continue
        
        # Find first critical bucket
        first_critical_week = item.get("firstCriticalBucketWeek")
        if not first_critical_week:
            # Fallback: scan buckets for first critical
            for bucket in item.get("buckets", []):
                if (bucket.get("criticality") or "").lower() == "critical":
                    first_critical_week = bucket["start_date"]
                    break
        
        ctb_impact = float(item.get("ctbShortageImpactedValue", 0) or 0)
        
        # Determine PO status
        po_status = "Unknown"
        first_po = item.get("firstPoStatus") or ""
        if "late" in first_po.lower() or "past" in first_po.lower():
            po_status = "Late"
        elif "on-time" in first_po.lower() or "on time" in first_po.lower():
            po_status = "On-time"
        
        critical.append({
            "itemCode": item.get("itemCode"),
            "itemDescription": item.get("itemDescription"),
            "site": item.get("site"),
            "firstCriticalWeek": first_critical_week,
            "daysInShortage": item.get("daysInShortage"),
            "ctbImpact": round(ctb_impact),
            "criticalityLevel": crit_level,
            "poStatus": po_status,
            "buyer": item.get("buyer"),
            "planner": item.get("planner"),
        })
    
    # Sort by CTB impact descending
    critical.sort(key=lambda x: x["ctbImpact"], reverse=True)
    return critical[:max_items]


def get_scheduled_deliveries_summary(
    weekly_data: list[dict],
    next_n_days: int = 7,
) -> dict[str, Any]:
    """Summarize scheduled delivery tracking across items.
    
    Args:
        weekly_data: Must be from get_shortages_with_scheduled_deliveries_weekly().
        next_n_days: Count deliveries arriving in next N days.
    
    Returns:
        Dict with:
        - items_with_schedules: Count of items with delivery tracking
        - avg_deliveries_per_item: Average scheduled deliveries per item
        - next_n_days_scheduled_qty: Total qty arriving in next N days
    """
    items_with_schedules = 0
    total_deliveries = 0
    next_n_qty = 0.0
    
    from datetime import timedelta
    cutoff_date = datetime.now(timezone.utc) + timedelta(days=next_n_days)
    
    for item in weekly_data:
        sched_count = item.get("scheduledDeliveries")
        if isinstance(sched_count, int) and sched_count > 0:
            items_with_schedules += 1
            total_deliveries += sched_count
            
            # Check if first delivery is within next_n_days
            first_del_date = item.get("firstDeliveryDate")
            if first_del_date:
                try:
                    from dateutil import parser
                    del_dt = parser.parse(first_del_date)
                    if del_dt <= cutoff_date:
                        first_del_qty = float(item.get("firstDeliveryQty", 0) or 0)
                        next_n_qty += first_del_qty
                except Exception:
                    pass
    
    avg_per_item = total_deliveries / items_with_schedules if items_with_schedules > 0 else 0.0
    
    return {
        "items_with_schedules": items_with_schedules,
        "avg_deliveries_per_item": round(avg_per_item, 1),
        "next_n_days_scheduled_qty": round(next_n_qty, 1),
    }


def check_reachable(sites: str | None = None) -> dict[str, Any]:
    """Health check: verify Shortage API is reachable and token is valid.
    
    Args:
        sites: Optional site filter.
    
    Returns:
        Dict with status, item_count, response_time_ms.
    """
    import time
    
    try:
        start = time.time()
        url = f"{_get_base_url()}/data/MaterialShortages/ShortagesByItem/Weekly"
        response = requests.get(url, headers=_headers(sites), timeout=30)
        elapsed_ms = (time.time() - start) * 1000
        
        response.raise_for_status()
        data = response.json()
        
        return {
            "status": "ok",
            "item_count": len(data) if isinstance(data, list) else 0,
            "response_time_ms": round(elapsed_ms, 1),
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
