"""Manual crypto top-ups: user submits claim, admin accepts/rejects."""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from pathlib import Path

from data_paths import data_dir

PATH = data_dir() / "pending_topups.json"


def _load() -> dict:
    PATH.parent.mkdir(parents=True, exist_ok=True)
    if not PATH.is_file():
        return {"by_id": {}}
    try:
        raw = json.loads(PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"by_id": {}}
    if not isinstance(raw, dict) or "by_id" not in raw:
        return {"by_id": {}}
    return raw


def _save(data: dict) -> None:
    PATH.parent.mkdir(parents=True, exist_ok=True)
    PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def user_has_open_pending(user_id: int) -> bool:
    for rec in _load()["by_id"].values():
        if not isinstance(rec, dict):
            continue
        if int(rec.get("user_id", 0)) == user_id and rec.get("status") == "pending":
            return True
    return False


def create_pending(
    user_id: int,
    username: str | None,
    full_name: str | None,
    amount_usd: float,
    currency: str,
) -> str | None:
    if user_has_open_pending(user_id):
        return None
    data = _load()
    pid = secrets.token_hex(8)
    data["by_id"][pid] = {
        "user_id": user_id,
        "username": username or "",
        "full_name": full_name or "",
        "amount_usd": round(float(amount_usd), 2),
        "currency": currency.lower(),
        "status": "pending",
        "created": datetime.now(timezone.utc).isoformat(),
    }
    _save(data)
    return pid


def get_pending(pid: str) -> dict | None:
    rec = _load()["by_id"].get(pid)
    return rec if isinstance(rec, dict) else None


def list_user_topups(user_id: int, *, limit: int = 12) -> list[tuple[str, dict]]:
    """Recent top-up records for this user (newest first)."""
    uid = int(user_id)
    rows: list[tuple[str, dict, str]] = []
    for pid, rec in _load()["by_id"].items():
        if not isinstance(rec, dict):
            continue
        if int(rec.get("user_id", 0)) != uid:
            continue
        created = str(rec.get("created") or rec.get("resolved") or "")
        rows.append((pid, rec, created))
    rows.sort(key=lambda x: x[2], reverse=True)
    return [(p, r) for p, r, _ in rows[:limit]]


def set_status(pid: str, status: str) -> dict | None:
    data = _load()
    rec = data["by_id"].get(pid)
    if not isinstance(rec, dict):
        return None
    rec["status"] = status
    rec["resolved"] = datetime.now(timezone.utc).isoformat()
    data["by_id"][pid] = rec
    _save(data)
    return rec


def list_all_topups(*, limit: int = 500) -> list[dict]:
    """Admin / portal: newest first. Each row includes ``id`` plus record fields."""
    rows: list[dict] = []
    for pid, rec in _load()["by_id"].items():
        if not isinstance(rec, dict):
            continue
        created = str(rec.get("created") or "")
        rows.append({"id": pid, **rec, "_sort": created})
    rows.sort(key=lambda x: x.get("_sort") or "", reverse=True)
    for r in rows:
        r.pop("_sort", None)
    return rows[:limit]
