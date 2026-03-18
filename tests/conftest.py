"""Pytest configuration and shared fixtures."""
import sys
from pathlib import Path

# Ensure src is on the path when running tests from project root or tests/
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
