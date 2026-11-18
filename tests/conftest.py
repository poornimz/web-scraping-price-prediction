"""Pytest hooks for path resolution and pipeline test diagnostics."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


def pytest_configure(config: Any) -> None:
    """Register the price_tracker package root on sys.path before test collection."""
    project_root = str(Path(__file__).resolve().parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def pytest_report_header(config: Any) -> str:
    """Display active pipeline root in the pytest session header."""
    project_root = Path(__file__).resolve().parent.parent
    return f"price_tracker root: {project_root}"
