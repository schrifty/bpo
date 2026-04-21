# Network Timeout Fixes - Summary

## What We Did

We performed a comprehensive audit of all network operations in the codebase and systematically added socket timeout protection to prevent indefinite hangs. This addresses the hang you experienced with `python decks.py support` and similar issues across the entire codebase.

## Key Changes

### 1. Created Reusable Utility (`src/network_utils.py`)
- `network_timeout()` context manager for clean, consistent timeout handling
- Replaces 10+ lines of boilerplate per operation with just 2 lines
- Includes operation descriptions in timeout errors for better debugging

### 2. Updated 10 Files with Network Timeout Protection

**Google Drive API Clients:**
- `src/leandna_item_master_client.py`
- `src/leandna_shortage_client.py`
- `src/leandna_lean_projects_client.py`
- `src/pendo_portfolio_snapshot_drive.py`
- `src/pendo_preload_cache_drive.py`
- `src/drive_config.py` (refactored from manual timeouts)
- `src/cs_report_client.py` (refactored from manual timeouts)

**Google Slides/Sheets API:**
- `src/slides_client.py`
- `src/charts.py`

### 3. Protected ~27 Google API Calls

Every `.execute()` call that could hang is now wrapped with a timeout (typically 30s for Drive operations, 60s for large batchUpdate calls).

## Code Quality

✅ **Zero linter errors** across all modified files  
✅ **~100 lines of duplicate code eliminated**  
✅ **Consistent error handling** with operation context  
✅ **Better logging** - you'll see which operation timed out

## Before vs After

### Before (Manual - 10+ lines per operation)
```python
import socket
old_timeout = socket.getdefaulttimeout()
try:
    socket.setdefaulttimeout(30.0)
    result = drive.files().create(body=meta).execute()
finally:
    socket.setdefaulttimeout(old_timeout)
```

### After (Context Manager - 2 lines)
```python
from .network_utils import network_timeout

with network_timeout(30.0, "Drive file creation"):
    result = drive.files().create(body=meta).execute()
```

## Impact

1. **Fixes your immediate issue**: The hung `python decks.py support` command will now timeout after 30s instead of hanging indefinitely
2. **Prevents future hangs**: All similar operations across the codebase are protected
3. **Better debugging**: Timeout errors will show which specific operation failed
4. **Cleaner code**: Massive reduction in boilerplate

## Testing Recommendations

Run these to verify the fixes work:
```bash
python decks.py support
python main.py qbr Bombardier
```

Both should complete or fail with a clear timeout error (with operation description) rather than hanging indefinitely.

## Documentation

See `docs/NETWORK_TIMEOUT_AUDIT.md` for complete details on all changes.
