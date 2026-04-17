"""Project data directory: ./data locally, or LEADBOT_DATA_DIR (e.g. Railway volume)."""

from __future__ import annotations

import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent


def data_dir() -> Path:
    env = os.environ.get("LEADBOT_DATA_DIR", "").strip()
    if env:
        return Path(env)
    return _ROOT / "data"
