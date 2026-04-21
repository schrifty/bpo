"""LeanDNA Lean Projects API client.

Provides access to Lean Projects, savings tracking, and task/issue management.
Used by qbr_template.py for continuous improvement ROI tracking.

API docs: https://app.leandna.com/api/swagger.json
Endpoints: /data/LeanProject, /data/LeanProject/{projectIds}/Savings, etc.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Any

import requests

from .config import (
    logger,
    LEANDNA_DATA_API_BASE_URL,
    LEANDNA_DATA_API_BEARER_TOKEN,
)

# Thread-safe in-memory cache
_cache_lock = threading.Lock()
_projects_cache: dict[str, Any] = {}
_savings_cache: dict[str, Any] = {}


def _get_bearer_token() -> str:
    """Return the LeanDNA API bearer token from config."""
    token = LEANDNA_DATA_API_BEARER_TOKEN
    if not token:
        raise ValueError("LEANDNA_DATA_API_BEARER_TOKEN not configured in .env")
    return token


def _headers(sites: str | None = None) -> dict[str, str]:
    """Build request headers with auth and optional site filter."""
    h = {
        "Authorization": f"Bearer {_get_bearer_token()}",
        "Content-Type": "application/json",
    }
    if sites:
        h["RequestedSites"] = sites
    return h


def _get_cache_key(sites: str | None, date_from: str | None, date_to: str | None) -> str:
    """Generate cache key for projects query."""
    return f"{sites or 'all'}_{date_from or 'none'}_{date_to or 'none'}"


def _load_from_drive_cache(cache_prefix: str, cache_key: str, ttl_hours: int) -> dict[str, Any] | None:
    """Load cached JSON from Drive if still valid."""
    try:
        from .slides_api import _get_service
        _, drive, _ = _get_service()
        
        # Search for cache file
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        file_pattern = f"{cache_prefix}_{cache_key}_{today}.json"
        
        q = f"name = '{file_pattern}' and trashed = false"
        results = drive.files().list(q=q, pageSize=5, fields="files(id, name, createdTime)").execute()
        files = results.get("files", [])
        
        if not files:
            return None
        
        # Check age
        file_info = files[0]
        created = datetime.fromisoformat(file_info["createdTime"].replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600
        
        if age_hours > ttl_hours:
            logger.debug("Drive cache %s expired (%.1fh old, TTL=%dh)", file_pattern, age_hours, ttl_hours)
            return None
        
        # Download
        request = drive.files().get_media(fileId=file_info["id"])
        content = request.execute()
        data = json.loads(content)
        
        logger.info("LeanDNA Lean Projects: loaded from Drive cache %s (%.1fh old)", file_pattern, age_hours)
        return data
        
    except Exception as e:
        logger.debug("Drive cache load failed for %s: %s", cache_prefix, e)
        return None


def _save_to_drive_cache(cache_prefix: str, cache_key: str, data: dict[str, Any]) -> None:
    """Save JSON to Drive cache."""
    try:
        from .slides_api import _get_service
        from googleapiclient.http import MediaInMemoryUpload
        
        _, drive, _ = _get_service()
        
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"{cache_prefix}_{cache_key}_{today}.json"
        content = json.dumps(data, indent=2).encode("utf-8")
        
        media = MediaInMemoryUpload(content, mimetype="application/json", resumable=False)
        meta = {"name": filename, "mimeType": "application/json"}
        
        drive.files().create(body=meta, media_body=media, fields="id").execute()
        logger.debug("LeanDNA Lean Projects: saved to Drive cache %s", filename)
        
    except Exception as e:
        logger.warning("Drive cache save failed for %s: %s", cache_prefix, e)


def get_lean_projects(
    sites: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Fetch Lean Projects from API.
    
    Args:
        sites: Comma-separated site IDs (None = all authorized sites).
        date_from: ISO date string for start of range (e.g., "2026-01-01").
        date_to: ISO date string for end of range (e.g., "2026-03-31").
        force_refresh: Bypass cache and fetch fresh data.
    
    Returns:
        List of project dicts with id, name, stage, savings, etc.
    """
    cache_key = _get_cache_key(sites, date_from, date_to)
    
    # Check in-memory cache
    if not force_refresh:
        with _cache_lock:
            if cache_key in _projects_cache:
                logger.debug("LeanDNA Lean Projects: using in-memory cache (key=%s)", cache_key)
                return _projects_cache[cache_key]
    
    # Check Drive cache
    if not force_refresh:
        from .config import LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS
        cached = _load_from_drive_cache("lean_projects", cache_key, LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS)
        if cached:
            with _cache_lock:
                _projects_cache[cache_key] = cached
            return cached
    
    # Fetch from API
    url = f"{LEANDNA_DATA_API_BASE_URL}/data/LeanProject"
    params: dict[str, Any] = {}
    if date_from:
        params["dateFrom"] = date_from
    if date_to:
        params["dateTo"] = date_to
    
    try:
        logger.info("LeanDNA Lean Projects: fetching from API (sites=%s, dateFrom=%s, dateTo=%s)", 
                   sites or "all", date_from, date_to)
        response = requests.get(url, headers=_headers(sites), params=params, timeout=180)
        response.raise_for_status()
        data = response.json()
        
        projects = data if isinstance(data, list) else []
        logger.info("LeanDNA Lean Projects: fetched %d projects", len(projects))
        
        # Cache
        with _cache_lock:
            _projects_cache[cache_key] = projects
        _save_to_drive_cache("lean_projects", cache_key, projects)
        
        return projects
        
    except requests.RequestException as e:
        logger.error("LeanDNA Lean Projects API error: %s", e)
        return []


def get_project_savings(
    project_ids: list[str],
    sites: str | None = None,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Fetch monthly savings for projects.
    
    Args:
        project_ids: List of project IDs to fetch savings for.
        sites: Comma-separated site IDs (None = all authorized sites).
        force_refresh: Bypass cache.
    
    Returns:
        List of dicts with projectId and savings array (monthly breakdown).
    """
    if not project_ids:
        return []
    
    cache_key = f"{','.join(sorted(project_ids))}_{sites or 'all'}"
    
    # Check in-memory cache
    if not force_refresh:
        with _cache_lock:
            if cache_key in _savings_cache:
                logger.debug("LeanDNA Project Savings: using in-memory cache")
                return _savings_cache[cache_key]
    
    # Check Drive cache
    if not force_refresh:
        from .config import LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS
        cached = _load_from_drive_cache("project_savings", cache_key, LEANDNA_LEAN_PROJECTS_CACHE_TTL_HOURS)
        if cached:
            with _cache_lock:
                _savings_cache[cache_key] = cached
            return cached
    
    # Fetch from API
    url = f"{LEANDNA_DATA_API_BASE_URL}/data/LeanProject/{','.join(project_ids)}/Savings"
    
    try:
        logger.info("LeanDNA Project Savings: fetching for %d projects", len(project_ids))
        response = requests.get(url, headers=_headers(sites), timeout=180)
        response.raise_for_status()
        data = response.json()
        
        savings = data if isinstance(data, list) else []
        logger.info("LeanDNA Project Savings: fetched %d savings records", len(savings))
        
        # Cache
        with _cache_lock:
            _savings_cache[cache_key] = savings
        _save_to_drive_cache("project_savings", cache_key, savings)
        
        return savings
        
    except requests.RequestException as e:
        logger.error("LeanDNA Project Savings API error: %s", e)
        return []


def aggregate_portfolio_stats(projects: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute portfolio-level statistics from projects list.
    
    Returns:
        Dict with total_projects, active_projects, stage_distribution, 
        total_savings_actual, total_savings_target, best_practice_count, etc.
    """
    if not projects:
        return {
            "total_projects": 0,
            "active_projects": 0,
            "stage_distribution": {},
            "state_distribution": {},
            "total_savings_actual": 0.0,
            "total_savings_target": 0.0,
            "savings_achievement_pct": 0.0,
            "best_practice_count": 0,
            "validated_results_count": 0,
        }
    
    stage_dist: dict[str, int] = {}
    state_dist: dict[str, int] = {}
    total_actual = 0.0
    total_target = 0.0
    best_practice = 0
    validated = 0
    active = 0
    
    for p in projects:
        stage = p.get("stage", "Unknown")
        state = p.get("state", "unknown")
        
        stage_dist[stage] = stage_dist.get(stage, 0) + 1
        state_dist[state] = state_dist.get(state, 0) + 1
        
        if stage not in ("Closed", "Cancelled"):
            active += 1
        
        total_actual += p.get("totalActualSavingsForPeriod", 0.0) or 0.0
        total_target += p.get("totalTargetSavingsForPeriod", 0.0) or 0.0
        
        if p.get("isBestPractice"):
            best_practice += 1
        if p.get("isProjectResultsValidated"):
            validated += 1
    
    achievement = (total_actual / total_target * 100) if total_target > 0 else 0.0
    
    return {
        "total_projects": len(projects),
        "active_projects": active,
        "stage_distribution": stage_dist,
        "state_distribution": state_dist,
        "total_savings_actual": total_actual,
        "total_savings_target": total_target,
        "savings_achievement_pct": achievement,
        "best_practice_count": best_practice,
        "validated_results_count": validated,
    }


def aggregate_monthly_savings(
    savings_data: list[dict[str, Any]],
    months: int = 3,
) -> list[dict[str, Any]]:
    """Aggregate monthly savings across all projects.
    
    Args:
        savings_data: List of project savings from get_project_savings().
        months: Number of recent months to include.
    
    Returns:
        List of dicts with month, total_actual, total_target (sorted by month).
    """
    monthly_totals: dict[str, dict[str, float]] = {}
    
    for project_savings in savings_data:
        for entry in project_savings.get("savings", []):
            month = entry.get("month")
            if not month or not entry.get("includeInTotals"):
                continue
            
            if month not in monthly_totals:
                monthly_totals[month] = {"actual": 0.0, "target": 0.0}
            
            monthly_totals[month]["actual"] += entry.get("actual", 0.0) or 0.0
            monthly_totals[month]["target"] += entry.get("target", 0.0) or 0.0
    
    # Convert to list and sort
    result = [
        {"month": m, "actual": v["actual"], "target": v["target"]}
        for m, v in monthly_totals.items()
    ]
    result.sort(key=lambda x: x["month"], reverse=True)
    
    return result[:months]


def get_top_projects_by_savings(
    projects: list[dict[str, Any]],
    max_projects: int = 10,
) -> list[dict[str, Any]]:
    """Return top N projects sorted by actual savings descending."""
    sorted_projects = sorted(
        projects,
        key=lambda p: p.get("totalActualSavingsForPeriod", 0.0) or 0.0,
        reverse=True,
    )
    return sorted_projects[:max_projects]


def check_reachable(sites: str | None = None) -> bool:
    """Test if LeanDNA Lean Projects API is accessible.
    
    Returns:
        True if API responds with 200 (or 401 if token is invalid).
    """
    url = f"{LEANDNA_DATA_API_BASE_URL}/data/LeanProject"
    params = {"dateFrom": "2026-01-01", "dateTo": "2026-01-01"}  # minimal query
    
    try:
        response = requests.get(url, headers=_headers(sites), params=params, timeout=30)
        return response.status_code in (200, 401)
    except Exception:
        return False
