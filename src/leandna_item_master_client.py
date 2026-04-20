"""LeanDNA Item Master Data API client for BPO.

Fetches comprehensive item-level supply chain data including DOI backwards,
risk scores, ABC classification, lead time variance, and excess inventory details.

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

_cache: dict[str, Any] | None = None
_cache_lock = threading.Lock()
_cache_timestamp: datetime | None = None


def _get_base_url() -> str:
    """Get LeanDNA Data API base URL from config."""
    from .config import LEANDNA_DATA_API_BASE_URL
    return LEANDNA_DATA_API_BASE_URL or "https://app.leandna.com/api"


def _get_bearer_token() -> str | None:
    """Get LeanDNA bearer token from config."""
    from .config import LEANDNA_DATA_API_BEARER_TOKEN
    return LEANDNA_DATA_API_BEARER_TOKEN


def _get_cache_ttl_hours() -> int:
    """Get cache TTL in hours from config."""
    from .config import LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS
    return LEANDNA_ITEM_MASTER_CACHE_TTL_HOURS or 24


def _headers(requested_sites: str | None = None) -> dict[str, str]:
    """Build request headers with auth and optional site scoping."""
    token = _get_bearer_token()
    if not token:
        raise ValueError("LEANDNA_DATA_API_BEARER_TOKEN not configured in .env")
    
    h = {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json",
        "User-Agent": "bpo-leandna-client/1.0",
    }
    if requested_sites:
        h["RequestedSites"] = requested_sites.strip()
    return h


def _cache_key(sites: str | None) -> str:
    """Generate cache key for site list."""
    if not sites:
        return "all_sites"
    return hashlib.md5(sites.encode()).hexdigest()[:16]


def _is_cache_valid() -> bool:
    """Check if in-memory cache is valid based on TTL."""
    global _cache_timestamp
    if _cache is None or _cache_timestamp is None:
        return False
    
    age_hours = (datetime.now(timezone.utc) - _cache_timestamp).total_seconds() / 3600
    return age_hours < _get_cache_ttl_hours()


def _try_load_from_drive(cache_key: str) -> list[dict] | None:
    """Attempt to load cached data from Drive.
    
    Simplified implementation: searches for cache file by name pattern,
    checks age, and downloads if valid.
    """
    try:
        from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID
        if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
            return None
        
        from .slides_api import _get_service
        _, drive, _ = _get_service()
        
        # Search for today's cache file
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"item_master_{cache_key}_{date_str}.json"
        
        # Query for file in cache subfolder (if exists) or generator root
        query = f"name='{filename}' and trashed=false"
        results = drive.files().list(
            q=query,
            fields="files(id, name, modifiedTime)",
            spaces="drive",
            pageSize=5,
        ).execute()
        
        files = results.get("files", [])
        if not files:
            logger.debug("LeanDNA Item Master: no Drive cache found for %s", filename)
            return None
        
        file_info = files[0]
        
        # Check file age
        modified = file_info.get("modifiedTime", "")
        if modified:
            from dateutil import parser
            mod_dt = parser.parse(modified)
            age_hours = (datetime.now(timezone.utc) - mod_dt).total_seconds() / 3600
            if age_hours >= _get_cache_ttl_hours():
                logger.debug("LeanDNA Item Master: Drive cache is stale (%.1fh old)", age_hours)
                return None
        
        # Download and parse
        request = drive.files().get_media(fileId=file_info["id"])
        content = request.execute()
        data = json.loads(content.decode("utf-8"))
        logger.info("LeanDNA Item Master: loaded %d items from Drive cache (%s)", len(data), filename)
        return data
        
    except Exception as e:
        logger.debug("Failed to load LeanDNA Item Master from Drive cache: %s", e)
        return None


def _save_to_drive(data: list[dict], cache_key: str) -> None:
    """Save data to Drive cache.
    
    Simplified implementation: creates JSON file in generator root folder.
    """
    try:
        from .config import GOOGLE_QBR_GENERATOR_FOLDER_ID
        if not GOOGLE_QBR_GENERATOR_FOLDER_ID:
            return
        
        from .slides_api import _get_service
        from googleapiclient.http import MediaInMemoryUpload
        
        _, drive, _ = _get_service()
        
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"item_master_{cache_key}_{date_str}.json"
        
        content = json.dumps(data, indent=2).encode("utf-8")
        media = MediaInMemoryUpload(content, mimetype="application/json", resumable=True)
        
        meta = {
            "name": filename,
            "parents": [GOOGLE_QBR_GENERATOR_FOLDER_ID],
            "mimeType": "application/json",
        }
        
        file_obj = drive.files().create(body=meta, media_body=media, fields="id").execute()
        logger.info("LeanDNA Item Master: saved %d items to Drive cache (%s, id=%s)", len(data), filename, file_obj["id"][:16])
        
    except Exception as e:
        logger.warning("Failed to save LeanDNA Item Master to Drive cache: %s", e)


def get_item_master_data(sites: str | None = None, force_refresh: bool = False) -> list[dict]:
    """Retrieve Item Master Data from LeanDNA API.
    
    Args:
        sites: Comma-separated site IDs (optional; defaults to all authorized sites).
        force_refresh: Bypass cache and fetch fresh data.
    
    Returns:
        List of item master records (dicts with item-level supply chain metrics).
    
    Raises:
        ValueError: If bearer token not configured.
        requests.HTTPError: If API returns error status.
    """
    global _cache, _cache_timestamp
    
    cache_key = _cache_key(sites)
    
    # Check in-memory cache
    if not force_refresh and _is_cache_valid():
        logger.debug("LeanDNA Item Master: using in-memory cache (%d items)", len(_cache or []))
        return _cache or []
    
    with _cache_lock:
        # Double-check after acquiring lock
        if not force_refresh and _is_cache_valid():
            return _cache or []
        
        # Try Drive cache
        if not force_refresh:
            drive_data = _try_load_from_drive(cache_key)
            if drive_data:
                _cache = drive_data
                _cache_timestamp = datetime.now(timezone.utc)
                return drive_data
        
        # Fetch from API
        url = f"{_get_base_url()}/data/ItemMasterData"
        logger.info("LeanDNA Item Master: fetching from API (sites=%s)", sites or "all")
        
        try:
            response = requests.get(url, headers=_headers(sites), timeout=120)
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                logger.error("LeanDNA Item Master API returned non-list: %s", type(data))
                return []
            
            logger.info("LeanDNA Item Master: fetched %d items from API", len(data))
            
            # Update cache
            _cache = data
            _cache_timestamp = datetime.now(timezone.utc)
            
            # Save to Drive (async, don't block)
            try:
                _save_to_drive(data, cache_key)
            except Exception as e:
                logger.warning("Drive cache save failed (non-fatal): %s", e)
            
            return data
            
        except requests.HTTPError as e:
            logger.error("LeanDNA Item Master API error: %s", e)
            if e.response is not None:
                logger.error("Response body: %s", e.response.text[:500])
            raise
        except Exception as e:
            logger.error("LeanDNA Item Master fetch failed: %s", e)
            raise


def get_high_risk_items(
    items: list[dict] | None = None,
    threshold: int = 80,
    max_items: int = 50,
    sites: str | None = None,
) -> list[dict]:
    """Get items with high aggregate risk scores.
    
    Args:
        items: Pre-fetched item list (optional; fetches if None).
        threshold: Minimum risk score (0-100).
        max_items: Max items to return.
        sites: Site filter if fetching fresh.
    
    Returns:
        List of high-risk items sorted by risk score descending.
    """
    if items is None:
        items = get_item_master_data(sites=sites)
    
    high_risk = [
        i for i in items
        if isinstance(i.get("aggregateRiskScore"), (int, float))
        and i["aggregateRiskScore"] >= threshold
    ]
    high_risk.sort(key=lambda x: x.get("aggregateRiskScore", 0), reverse=True)
    return high_risk[:max_items]


def get_doi_backwards_summary(items: list[dict] | None = None, sites: str | None = None) -> dict[str, Any]:
    """Aggregate DOI backwards metrics across items.
    
    Args:
        items: Pre-fetched item list (optional; fetches if None).
        sites: Site filter if fetching fresh.
    
    Returns:
        Dict with mean, median, min, max, items_over_60_days.
    """
    if items is None:
        items = get_item_master_data(sites=sites)
    
    values = [
        i["daysOfInventoryBackward"]
        for i in items
        if isinstance(i.get("daysOfInventoryBackward"), (int, float))
        and i["daysOfInventoryBackward"] > 0
    ]
    
    if not values:
        return {
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "items_over_60_days": 0,
            "total_items_with_doi_bwd": 0,
        }
    
    values_sorted = sorted(values)
    n = len(values)
    median = values_sorted[n // 2] if n % 2 == 1 else (values_sorted[n // 2 - 1] + values_sorted[n // 2]) / 2
    
    return {
        "mean": round(sum(values) / n, 1),
        "median": round(median, 1),
        "min": round(min(values), 1),
        "max": round(max(values), 1),
        "items_over_60_days": sum(1 for v in values if v > 60),
        "total_items_with_doi_bwd": n,
    }


def get_abc_distribution(items: list[dict] | None = None, sites: str | None = None) -> dict[str, int]:
    """Count items by ABC rank.
    
    Args:
        items: Pre-fetched item list (optional; fetches if None).
        sites: Site filter if fetching fresh.
    
    Returns:
        Dict with keys "A", "B", "C", "Unknown" and item counts.
    """
    if items is None:
        items = get_item_master_data(sites=sites)
    
    dist: dict[str, int] = {"A": 0, "B": 0, "C": 0, "Unknown": 0}
    for i in items:
        rank = (i.get("abcRank") or "").strip().upper()
        if rank in ("A", "B", "C"):
            dist[rank] += 1
        else:
            dist["Unknown"] += 1
    
    return dist


def get_lead_time_variance(
    items: list[dict] | None = None,
    sites: str | None = None,
    supplier: str | None = None,
    min_variance_pct: float = 20.0,
) -> list[dict]:
    """Get items with significant lead time variance (observed vs planned).
    
    Args:
        items: Pre-fetched item list (optional; fetches if None).
        sites: Site filter if fetching fresh.
        supplier: Filter by supplier name (optional).
        min_variance_pct: Minimum variance % to include.
    
    Returns:
        List of items with lead time variance, sorted by variance % descending.
    """
    if items is None:
        items = get_item_master_data(sites=sites)
    
    variances = []
    for i in items:
        planned = i.get("leadTime")
        observed = i.get("observedLeadTime")
        if not (isinstance(planned, (int, float)) and isinstance(observed, (int, float)) and planned > 0):
            continue
        
        if supplier and (i.get("supplier") or "").lower() != supplier.lower():
            continue
        
        variance_pct = ((observed - planned) / planned) * 100
        if abs(variance_pct) >= min_variance_pct:
            variances.append({
                "itemCode": i.get("itemCode"),
                "itemDescription": i.get("itemDescription"),
                "site": i.get("site"),
                "supplier": i.get("supplier"),
                "planned": round(planned, 1),
                "observed": round(observed, 1),
                "variance_pct": round(variance_pct, 1),
            })
    
    variances.sort(key=lambda x: abs(x["variance_pct"]), reverse=True)
    return variances


def get_excess_items(
    items: list[dict] | None = None,
    sites: str | None = None,
    max_items: int = 50,
) -> tuple[list[dict], float]:
    """Get items with excess on-hand inventory.
    
    Args:
        items: Pre-fetched item list (optional; fetches if None).
        sites: Site filter if fetching fresh.
        max_items: Max items to return.
    
    Returns:
        Tuple of (top excess items list, total excess value across all items).
    """
    if items is None:
        items = get_item_master_data(sites=sites)
    
    excess_items = []
    total_excess = 0.0
    
    for i in items:
        excess_val = i.get("excessOnHandValue")
        excess_qty = i.get("excessOnHandQty")
        if isinstance(excess_val, (int, float)) and excess_val > 0:
            total_excess += excess_val
            excess_items.append({
                "itemCode": i.get("itemCode"),
                "itemDescription": i.get("itemDescription"),
                "site": i.get("site"),
                "excessOnHandValue": round(excess_val),
                "excessOnHandQty": round(excess_qty, 1) if isinstance(excess_qty, (int, float)) else None,
            })
    
    excess_items.sort(key=lambda x: x["excessOnHandValue"], reverse=True)
    return excess_items[:max_items], total_excess


def check_reachable(sites: str | None = None) -> dict[str, Any]:
    """Health check: verify API is reachable and token is valid.
    
    Args:
        sites: Optional site filter.
    
    Returns:
        Dict with status, item_count, response_time_ms.
    """
    import time
    
    try:
        start = time.time()
        url = f"{_get_base_url()}/data/ItemMasterData"
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
