"""Network utilities for timeout management and error handling."""

import socket
from contextlib import contextmanager
from typing import Any

from .config import logger


@contextmanager
def network_timeout(timeout_seconds: float = 30.0, operation: str = "network operation"):
    """Context manager to set socket timeout for network operations.
    
    This prevents indefinite hangs on network I/O operations like Google API calls,
    HTTP requests, etc. The timeout is applied at the socket level and automatically
    restored when the context exits.
    
    Args:
        timeout_seconds: Timeout in seconds (default 30.0)
        operation: Description of the operation for logging (optional)
    
    Example:
        with network_timeout(30.0, "Drive file creation"):
            file = drive_service.files().create(body=meta).execute()
    """
    old_timeout = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(timeout_seconds)
        yield
    except socket.timeout as e:
        logger.error("%s timed out after %.1fs", operation, timeout_seconds)
        raise TimeoutError(f"{operation} timed out after {timeout_seconds}s") from e
    except Exception:
        raise
    finally:
        socket.setdefaulttimeout(old_timeout)


def ensure_requests_timeout(kwargs: dict[str, Any], default_timeout: float = 30.0) -> dict[str, Any]:
    """Ensure a requests library kwargs dict has a timeout parameter.
    
    Args:
        kwargs: Dictionary of keyword arguments for requests.get/post/etc
        default_timeout: Default timeout in seconds if not already set
        
    Returns:
        The updated kwargs dict (modified in place, also returned for convenience)
        
    Example:
        kwargs = {"headers": headers, "json": payload}
        ensure_requests_timeout(kwargs, 45.0)
        response = requests.post(url, **kwargs)
    """
    if "timeout" not in kwargs:
        kwargs["timeout"] = default_timeout
    return kwargs
