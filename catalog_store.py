"""Shared BIN catalog: website sendout + bot admin commands. Default price $0.90 per BIN."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

from data_paths import data_dir

logger = logging.getLogger(__name__)

CATALOG_PATH = data_dir() / "catalog.json"


def _backup_sidecar_if_nonempty(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        return
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        logger.warning("Skip .bak backup — %s is missing or not valid JSON", path)
        return
    try:
        shutil.copy2(path, path.with_name(path.name + ".bak"))
    except OSError as e:
        logger.warning("Could not backup %s: %s", path, e)

DEFAULT_PRICE = 0.90

# Default stock BINs (web sendout + bot Base 1) when catalog file is first created
SEED_BINS = [
    "400022",
    "403491",
    "405741",
    "434769",
    "510805",
    "523914",
    "533621",
    "537802",
]


def _defaults() -> dict:
    return {"price_per_bin": DEFAULT_PRICE, "bins": []}


def load_catalog() -> dict:
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CATALOG_PATH.is_file():
        data = {
            "price_per_bin": DEFAULT_PRICE,
            "bins": list(SEED_BINS),
        }
        CATALOG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return data
    bak = CATALOG_PATH.with_name(CATALOG_PATH.name + ".bak")
    raw = None
    for path in (CATALOG_PATH, bak):
        if not path.is_file():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if path is bak:
                logger.warning(
                    "catalog.json was unreadable — restored from catalog.json.bak"
                )
            break
        except (json.JSONDecodeError, OSError) as e:
            raw = None
            if path is CATALOG_PATH:
                logger.error("catalog.json invalid (%s); trying .bak if present", e)
    if raw is None:
        logger.error(
            "catalog.json and .bak missing or corrupt — using empty catalog in memory only "
            "(fix files or restore from backup; next save will write a new catalog.json)"
        )
        return _defaults()
    out = _defaults()
    if isinstance(raw, dict):
        out["price_per_bin"] = float(raw.get("price_per_bin", DEFAULT_PRICE))
        bins = raw.get("bins", [])
        if isinstance(bins, list):
            out["bins"] = [str(b).strip() for b in bins if str(b).strip()]
    return out


def save_catalog(data: dict) -> None:
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "price_per_bin": round(float(data.get("price_per_bin", DEFAULT_PRICE)), 2),
        "bins": [str(b).strip() for b in data.get("bins", []) if str(b).strip()],
    }
    _backup_sidecar_if_nonempty(CATALOG_PATH)
    CATALOG_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def clear_all_bins() -> None:
    save_catalog({"price_per_bin": DEFAULT_PRICE, "bins": []})


def merge_bins_to_catalog(bin_keys: list[str]) -> None:
    """Add any 6-digit BIN not already listed (keeps sendout in sync with web groups)."""
    data = load_catalog()
    changed = False
    for key in bin_keys:
        b = "".join(c for c in str(key) if c.isdigit())[:6]
        if len(b) != 6:
            continue
        if b not in data["bins"]:
            data["bins"].append(b)
            changed = True
    if changed:
        data["bins"].sort()
        save_catalog(data)


def add_bin(bin6: str) -> bool:
    b = "".join(c for c in bin6 if c.isdigit())[:6]
    if len(b) != 6:
        return False
    data = load_catalog()
    if b in data["bins"]:
        return False
    data["bins"].append(b)
    data["bins"].sort()
    save_catalog(data)
    return True


def format_sendout_text() -> str:
    from bin_leads_store import format_sendout_tiers_block

    return format_sendout_tiers_block()


def try_restore_catalog_from_bak() -> tuple[bool, str]:
    """Copy last good catalog from catalog.json.bak over catalog.json (admin recovery)."""
    bak = CATALOG_PATH.with_name(CATALOG_PATH.name + ".bak")
    if not bak.is_file():
        return False, "No catalog.json.bak on disk (backup is created on each successful save)."
    try:
        raw = json.loads(bak.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        return False, f"catalog.json.bak unreadable: {e}"
    if not isinstance(raw, dict):
        return False, "catalog.json.bak is not a JSON object."
    save_catalog(raw)
    return True, "Restored catalog.json from catalog.json.bak."
