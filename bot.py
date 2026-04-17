"""
LeadsBot-style Telegram UI (menus + catalog pagination).
Token: set TELEGRAM_BOT_TOKEN in .env (see .env.example).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import socket
import threading
from html import escape
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import io

from telegram import (
    BotCommand,
    BotCommandScopeChat,
    InputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.error import Conflict, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

from bin_leads_store import (
    SECONDHAND_PRICE_USD,
    bin_line_counts,
    card_brand_from_bin6,
    clear_bin_leads,
    count_matching_lines,
    filter_dimension_picklists,
    format_notebook_text,
    get_lines_for_bin,
    groups_from_raw_paste,
    matching_age_stats_combined,
    merge_groups_from_web,
    norm_stock_tier,
    norm_zip_filter_value,
    pop_n_random_any,
    pop_n_random_filtered,
    pop_n_random_from_bin,
    restore_pairs_triples,
    state_breakdown_for_bin,
    states_compact_for_bin,
    stock_tiers_api_payload,
    strip_lead_sync_suffix,
    total_line_count,
    try_restore_leads_from_bak,
)

PRICE_SECONDHAND = SECONDHAND_PRICE_USD
from catalog_store import (
    add_bin,
    clear_all_bins,
    format_sendout_text,
    load_catalog,
    try_restore_catalog_from_bak,
)
from pending_topups import (
    create_pending,
    get_pending,
    list_all_topups,
    list_user_topups,
    user_has_open_pending,
)
from topup_actions import try_accept_topup, try_reject_topup
from data_paths import data_dir

_ROOT = Path(__file__).resolve().parent
_BOT_FILE = Path(__file__).resolve()
BOT_BUILD = "shop-v26"
# Only one bot process per PC for this project (avoids Telegram getUpdates Conflict).
_INSTANCE_PORT = 37651
_keepalive_sock: socket.socket | None = None
load_dotenv(_ROOT / ".env")

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _acquire_single_instance() -> None:
    global _keepalive_sock
    if os.environ.get("SKIP_SINGLE_INSTANCE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        logger.info("Single-instance lock skipped (SKIP_SINGLE_INSTANCE).")
        return
    if os.environ.get("RAILWAY_ENVIRONMENT", "").strip():
        logger.info("Single-instance lock skipped (RAILWAY_ENVIRONMENT).")
        return
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", _INSTANCE_PORT))
    except OSError as e:
        raise SystemExit(
            f"\n*** Another LEADBOT window is already running on this PC, "
            f"or port {_INSTANCE_PORT} is in use.\n"
            "Close every other LEADBOT / python bot window, wait 10s, then run START_BOT.cmd again.\n***\n"
        ) from e
    s.listen(1)
    _keepalive_sock = s
    logger.info("Single-instance lock OK (port %s).", _INSTANCE_PORT)


DATA_DIR = data_dir()
USERS_PATH = DATA_DIR / "users.json"

ITEMS_PER_PAGE = 10
BULK_RANDOM_QTY = (50, 100, 150, 200)

DEFAULT_PUBLIC_CHANNEL_URL = "https://t.me/homerleadss"


def _public_channel_url() -> str:
    return (
        os.environ.get("PUBLIC_CHANNEL_URL")
        or os.environ.get("TELEGRAM_CHANNEL_URL")
        or DEFAULT_PUBLIC_CHANNEL_URL
    ).strip()


def _catalog_bin_price() -> float:
    return float(load_catalog().get("price_per_bin", 0.9))


def _catalog_bins_live() -> list[str]:
    return list(load_catalog().get("bins", []))


def _load_users() -> dict:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not USERS_PATH.is_file():
        return {}
    try:
        return json.loads(USERS_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_users(users: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    USERS_PATH.write_text(json.dumps(users, indent=2), encoding="utf-8")


def _read_min_topup_usd() -> float:
    floor = 1.0
    default = 1.0
    raw = (os.environ.get("MIN_TOPUP_USD") or "").strip()
    if raw:
        try:
            v = float(raw.replace("$", "").replace(",", ""))
            if v >= floor:
                return round(v, 2)
            logger.warning(
                "MIN_TOPUP_USD=%r is below $1 floor — using default %s", raw, default
            )
        except ValueError:
            logger.warning("Invalid MIN_TOPUP_USD=%r — using default %s", raw, default)
    return default


MIN_TOPUP_USD = _read_min_topup_usd()
logger.info("Minimum top-up: %s USD (MIN_TOPUP_USD env, default 1)", MIN_TOPUP_USD)


def _min_topup_display() -> str:
    m = MIN_TOPUP_USD
    if abs(m - round(m)) < 1e-6:
        return f"${int(round(m))}"
    return f"${m:.2f}"


def _topup_min_button_label() -> str:
    return _min_topup_display()


def get_balance(user_id: int) -> float:
    users = _load_users()
    entry = users.get(str(user_id), {})
    return float(entry.get("balance", 0.0))


def is_vip(user_id: int) -> bool:
    users = _load_users()
    return bool(users.get(str(user_id), {}).get("vip", False))


def set_balance(user_id: int, value: float) -> None:
    users = _load_users()
    uid = str(user_id)
    users[uid] = {**users.get(uid, {}), "balance": round(value, 2)}
    _save_users(users)


def debit_purchase(user_id: int, amount: float) -> bool:
    """Deduct balance and bump total_spent; False if insufficient funds."""
    if amount <= 0:
        return True
    ensure_user(user_id)
    users = _load_users()
    uid = str(user_id)
    entry = {**_USER_DEFAULTS, **users.get(uid, {})}
    bal = float(entry.get("balance", 0.0))
    if bal + 1e-9 < amount:
        return False
    entry["balance"] = round(bal - amount, 2)
    entry["total_spent"] = round(float(entry.get("total_spent", 0.0)) + amount, 2)
    users[uid] = {**users.get(uid, {}), **entry}
    _save_users(users)
    return True


def refund_purchase(user_id: int, amount: float) -> None:
    if amount <= 0:
        return
    ensure_user(user_id)
    users = _load_users()
    uid = str(user_id)
    entry = {**_USER_DEFAULTS, **users.get(uid, {})}
    entry["balance"] = round(float(entry.get("balance", 0.0)) + amount, 2)
    entry["total_spent"] = round(max(0.0, float(entry.get("total_spent", 0.0)) - amount), 2)
    users[uid] = {**users.get(uid, {}), **entry}
    _save_users(users)


PURCHASE_HISTORY_LIMIT = 100


def append_purchase_history(
    user_id: int, amount_usd: float, line_count: int, label: str
) -> None:
    """Append one fulfilled lead purchase for My Orders (cart + random buy paths)."""
    if amount_usd <= 0 and line_count <= 0:
        return
    ensure_user(user_id)
    users = _load_users()
    uid = str(user_id)
    prev = users.get(uid, {})
    entry = {**_USER_DEFAULTS, **prev}
    hist = list(entry.get("purchase_history") or [])
    lbl = (label or "Purchase").strip()
    if len(lbl) > 180:
        lbl = lbl[:177] + "..."
    hist.append(
        {
            "ts": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "amount_usd": round(float(amount_usd), 2),
            "lines": int(line_count),
            "label": lbl,
        }
    )
    entry["purchase_history"] = hist[-PURCHASE_HISTORY_LIMIT:]
    users[uid] = {**prev, **entry}
    _save_users(users)


def _cart_history_label(cart: list[dict]) -> str:
    if not cart:
        return "Cart checkout"
    bits: list[str] = []
    for it in cart:
        if it.get("kind") == "filter":
            br = _norm_filter_brand(it.get("brand"))
            bits.append(
                _FILTER_BRAND_LABELS[br] if br else "filter"
            )
        else:
            bits.append(f"BIN {it.get('bin', '')}")
    head = ", ".join(bits[:5])
    if len(bits) > 5:
        head += f", +{len(bits) - 5} more"
    n_lines = sum(int(it.get("qty", 0) or 0) for it in cart)
    return f"Cart · {head} · {n_lines} line(s)"


_USER_DEFAULTS: dict = {
    "balance": 0.0,
    "cart": [],
    "vip": False,
    "total_deposits": 0.0,
    "total_spent": 0.0,
    "status": "active",
    "purchase_history": [],
}


def ensure_user(user_id: int) -> None:
    users = _load_users()
    uid = str(user_id)
    if uid not in users:
        users[uid] = {**_USER_DEFAULTS}
        _save_users(users)
        return
    entry = users[uid]
    changed = False
    for key, val in _USER_DEFAULTS.items():
        if key not in entry:
            entry[key] = val
            changed = True
    if changed:
        _save_users(users)


def get_user_stats(user_id: int) -> dict:
    ensure_user(user_id)
    users = _load_users()
    entry = users.get(str(user_id), {})
    return {**_USER_DEFAULTS, **entry}


def _norm_bin_input(s: str) -> str | None:
    d = "".join(c for c in str(s) if c.isdigit())[:6]
    return d if len(d) == 6 else None


def _normalize_cart_entries(raw) -> list[dict]:
    out: list[dict] = []
    for it in raw or []:
        if not isinstance(it, dict):
            continue
        tier = norm_stock_tier(str(it.get("tier", "first")))
        kind = it.get("kind")
        if kind == "bin":
            nb = _norm_bin_input(str(it.get("bin", "")))
            q = int(it.get("qty", 0) or 0)
            if nb and q > 0:
                out.append({"kind": "bin", "bin": nb, "qty": q, "tier": tier})
        elif kind == "filter":
            q = int(it.get("qty", 0) or 0)
            if q < 1:
                continue
            st = (str(it.get("state") or "").strip() or None)
            if st:
                st = st.upper() if len(st) == 2 and st.isalpha() else st
            ct = (str(it.get("city") or "").strip() or None)
            bk = (str(it.get("bank") or "").strip() or None)
            bn = _norm_bin_input(str(it.get("bin", ""))) if it.get("bin") else None
            br = _norm_filter_brand(it.get("brand"))
            zp = (
                norm_zip_filter_value(it.get("zip"))
                if it.get("zip")
                else None
            )
            gen = (str(it.get("gender") or "").strip().lower() or None)
            if gen and gen not in ("male", "female"):
                gen = None
            if not any([st, ct, bk, bn, br, zp, gen]):
                continue
            out.append(
                {
                    "kind": "filter",
                    "tier": tier,
                    "qty": q,
                    "state": st,
                    "city": ct,
                    "bank": bk,
                    "bin": bn,
                    "brand": br,
                    "zip": zp,
                    "gender": gen,
                }
            )
    return out


def get_cart_entries(user_id: int) -> list[dict]:
    ensure_user(user_id)
    return _normalize_cart_entries(get_user_stats(user_id).get("cart"))


def save_cart_entries(user_id: int, cart: list[dict]) -> None:
    users = _load_users()
    uid = str(user_id)
    ensure_user(user_id)
    users = _load_users()
    entry = {**users.get(uid, {})}
    entry["cart"] = _normalize_cart_entries(cart)
    users[uid] = entry
    _save_users(users)


def _line_price_for_tier(tier: str) -> float:
    return PRICE_SECONDHAND if norm_stock_tier(tier) == "second" else _catalog_bin_price()


def _filter_cart_entry_same(a: dict, b: dict) -> bool:
    return (
        (a.get("bank") or "") == (b.get("bank") or "")
        and (a.get("state") or "") == (b.get("state") or "")
        and (a.get("city") or "") == (b.get("city") or "")
        and (a.get("bin") or "") == (b.get("bin") or "")
        and (a.get("brand") or "") == (b.get("brand") or "")
        and (a.get("zip") or "") == (b.get("zip") or "")
        and (a.get("gender") or "") == (b.get("gender") or "")
    )


def add_to_cart_filter(user_id: int, tier: str, qty: int, flt: dict) -> None:
    if qty < 1:
        return
    t = norm_stock_tier(tier)
    st = (str(flt.get("state") or "").strip() or None)
    if st:
        st = st.upper() if len(st) == 2 and st.isalpha() else st
    ct = (str(flt.get("city") or "").strip() or None)
    bk = (str(flt.get("bank") or "").strip() or None)
    bn = _norm_bin_input(str(flt.get("bin", ""))) if flt.get("bin") else None
    br = _norm_filter_brand(flt.get("brand"))
    zp = norm_zip_filter_value(flt.get("zip")) if flt.get("zip") else None
    gen = (str(flt.get("gender") or "").strip().lower() or None)
    if gen and gen not in ("male", "female"):
        gen = None
    if not any([st, ct, bk, bn, br, zp, gen]):
        return
    entry = {
        "kind": "filter",
        "tier": t,
        "qty": qty,
        "state": st,
        "city": ct,
        "bank": bk,
        "bin": bn,
        "brand": br,
        "zip": zp,
        "gender": gen,
    }
    cart = get_cart_entries(user_id)
    for it in cart:
        if it.get("kind") == "filter" and it.get("tier") == t and _filter_cart_entry_same(
            it, entry
        ):
            it["qty"] += qty
            save_cart_entries(user_id, cart)
            return
    cart.append(entry)
    save_cart_entries(user_id, cart)


def add_to_cart_bin(user_id: int, bin6: str, qty: int, tier: str = "first") -> None:
    nb = _norm_bin_input(bin6)
    if not nb or qty < 1:
        return
    t = norm_stock_tier(tier)
    cart = get_cart_entries(user_id)
    for it in cart:
        if it.get("kind") != "bin":
            continue
        if it["bin"] == nb and it.get("tier", "first") == t:
            it["qty"] += qty
            save_cart_entries(user_id, cart)
            return
    cart.append({"kind": "bin", "bin": nb, "qty": qty, "tier": t})
    save_cart_entries(user_id, cart)


def clear_cart_user(user_id: int) -> None:
    save_cart_entries(user_id, [])


def cart_subtotal_usd(user_id: int) -> float:
    tot = 0.0
    for it in get_cart_entries(user_id):
        tot += it["qty"] * _line_price_for_tier(it.get("tier", "first"))
    return round(tot, 2)


def cart_fulfillment_ok(user_id: int) -> tuple[bool, str]:
    cart = get_cart_entries(user_id)
    need: dict[tuple[str, str], int] = {}
    for it in cart:
        if it.get("kind") == "filter":
            t = it.get("tier", "first")
            n_match = count_matching_lines(
                t,
                state=it.get("state"),
                bin6=it.get("bin"),
                city=it.get("city"),
                bank=it.get("bank"),
                brand=it.get("brand"),
                zip_code=it.get("zip"),
                gender=it.get("gender"),
            )
            if n_match < it["qty"]:
                return False, f"Filter cart row: need {it['qty']}, only {n_match} in pile."
            continue
        t = it.get("tier", "first")
        k = (it["bin"], t)
        need[k] = need.get(k, 0) + it["qty"]
    for (b, t), n in need.items():
        c = bin_line_counts(t).get(b, 0)
        if c < n:
            tag = "2nd" if norm_stock_tier(t) == "second" else "1st"
            return False, f"BIN {b} ({tag}): need {n}, only {c} in stock."
    return True, ""


def run_cart_checkout(user_id: int) -> tuple[list[tuple[str, str]], float] | None:
    cart = get_cart_entries(user_id)
    if not cart:
        return None
    ok, _ = cart_fulfillment_ok(user_id)
    if not ok:
        return None
    total = cart_subtotal_usd(user_id)
    if get_balance(user_id) + 1e-9 < total:
        return None
    rolled: list[tuple[str, str, str]] = []
    pairs_flat: list[tuple[str, str]] = []
    for it in cart:
        t = it.get("tier", "first")
        if it.get("kind") == "filter":
            got = pop_n_random_filtered(
                it["qty"],
                t,
                state=it.get("state"),
                bin6=it.get("bin"),
                city=it.get("city"),
                bank=it.get("bank"),
                brand=it.get("brand"),
                zip_code=it.get("zip"),
                gender=it.get("gender"),
            )
            if not got or len(got) != it["qty"]:
                restore_pairs_triples(rolled)
                return None
            for b, line in got:
                rolled.append((b, line, t))
                pairs_flat.append((b, line))
            continue
        got = pop_n_random_from_bin(it["bin"], it["qty"], t)
        if not got or len(got) != it["qty"]:
            restore_pairs_triples(rolled)
            return None
        for b, line in got:
            rolled.append((b, line, t))
            pairs_flat.append((b, line))
    if not debit_purchase(user_id, total):
        restore_pairs_triples(rolled)
        return None
    append_purchase_history(user_id, total, len(pairs_flat), _cart_history_label(cart))
    clear_cart_user(user_id)
    return pairs_flat, total


def random_unit_usd(tier: str) -> float:
    return PRICE_SECONDHAND if tier == "second" else _catalog_bin_price()


def profile_screen_text(user_id: int, u) -> str:
    st = get_user_stats(user_id)
    name = escape(u.full_name or "—")
    uname = f"@{escape(u.username)}" if u.username else "—"
    bal = float(st["balance"])
    dep = float(st["total_deposits"])
    spent = float(st["total_spent"])
    status = escape(str(st.get("status", "active")))
    return (
        "👤 <b>Profile</b>\n\n"
        f"Name: {name}\n"
        f"Username: {uname}\n"
        f"Telegram ID: <code>{user_id}</code>\n"
        f"Balance: <code>${bal:.2f}</code>\n"
        f"Total Deposits: <code>${dep:.2f}</code>\n"
        f"Total Spent: <code>${spent:.2f}</code>\n"
        f"Status: {status}"
    )


def account_balance_text(user_id: int) -> str:
    bal = get_balance(user_id)
    vip_line = "⭐ Active" if is_vip(user_id) else "⭐ Not Active"
    return (
        "💰 <b>Account Balance</b>\n\n"
        f"Balance: <code>${bal:.2f}</code>\n"
        f"VIP: {vip_line}"
    )


def account_balance_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💰 Top-up Balance", callback_data="top"),
                InlineKeyboardButton("📊 VIP Details", callback_data="vip"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="home")],
        ]
    )


# —— 2.0 main menu (Telegram reply keyboard) ——
BTN_TOPUP = "💰 Top-up Balance"
BTN_BUY_LEADS = "🛒 Buy Leads"
BTN_MY_ORDERS = "📦 My Orders"
BTN_CHANNEL = "📣 Channel"
BTN_ADMIN_MENU = "🔧 Admin panel"
CREDITS_PREFIX = "Credits: "


def _credits_reply_label(user_id: int) -> str:
    bal = get_balance(user_id)
    if abs(bal - round(bal)) < 1e-6:
        return f"{CREDITS_PREFIX}{int(round(bal))}"
    return f"{CREDITS_PREFIX}{bal:.2f}"


def reply_main_menu_markup(user_id: int | None) -> ReplyKeyboardMarkup:
    """Persistent bottom menu (2.0 UI)."""
    rows: list[list[KeyboardButton]] = [
        [
            KeyboardButton(BTN_TOPUP),
            KeyboardButton(BTN_BUY_LEADS),
        ],
        [
            KeyboardButton(BTN_MY_ORDERS),
            KeyboardButton(_credits_reply_label(int(user_id)))
            if user_id is not None
            else KeyboardButton(f"{CREDITS_PREFIX}0"),
        ],
    ]
    rows.append([KeyboardButton(BTN_CHANNEL, url=_public_channel_url())])
    if user_id is not None and user_id in get_admin_ids():
        rows.append([KeyboardButton(BTN_ADMIN_MENU)])
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
    )


def vip_details_text() -> str:
    return (
        "📊 <b>VIP Details</b>\n\n"
        "Status: <b>Standard</b> (upgrade coming soon)\n\n"
        "VIP perks (when active):\n"
        "• Priority support & restocks\n"
        "• Fee discounts on bulk orders\n"
        "• Early access to new bases\n\n"
        "<i>Contact admin to activate VIP.</i>"
    )


def topup_amount_text() -> str:
    return (
        "<b>Select credits to buy:</b>\n\n"
        f"<i>Minimum <b>{_min_topup_display()}</b>. Pay with crypto; an admin credits your balance after verification.</i>"
    )


def topup_amount_keyboard(tu_back: str) -> InlineKeyboardMarkup:
    m = MIN_TOPUP_USD
    presets: list[tuple[str, float, str]] = [
        ("tua20", 20.0, "20"),
        ("tua50", 50.0, "50"),
        ("tua100", 100.0, "100"),
        ("tua200", 200.0, "200"),
        ("tua500", 500.0, "500"),
        ("tua1000", 1000.0, "1000"),
        ("tua2000", 2000.0, "2000"),
    ]
    eligible = [(cb, amt, lbl) for cb, amt, lbl in presets if amt + 1e-9 >= m]
    rows_out: list[list[InlineKeyboardButton]] = []
    if not eligible:
        rows_out.append(
            [InlineKeyboardButton(_topup_min_button_label(), callback_data="tumin")]
        )
    else:
        row1 = [
            InlineKeyboardButton(lbl, callback_data=cb)
            for cb, amt, lbl in eligible[:3]
        ]
        row2 = [
            InlineKeyboardButton(lbl, callback_data=cb)
            for cb, amt, lbl in eligible[3:6]
        ]
        row3: list[InlineKeyboardButton] = []
        if len(eligible) > 6:
            cb, amt, lbl = eligible[6]
            row3.append(InlineKeyboardButton(lbl, callback_data=cb))
        row3.append(InlineKeyboardButton("Custom...", callback_data="tuac"))
        rows_out.extend([r for r in (row1, row2, row3) if r])
    if tu_back == "bal":
        nav_lbl, nav_cb = "← Balance", "bal"
    else:
        nav_lbl, nav_cb = "← Main menu", "home"
    rows_out.append(
        [
            InlineKeyboardButton(nav_lbl, callback_data=nav_cb),
            InlineKeyboardButton("Cancel", callback_data="tu_cancel"),
        ]
    )
    return InlineKeyboardMarkup(rows_out)


def payment_method_text(amount: float) -> str:
    return (
        f"<b>Credits:</b> <code>${amount:.2f}</code>\n\n"
        "<b>Select payment currency:</b>"
    )


def payment_method_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("₿ Bitcoin (BTC)", callback_data="pmbtc"),
            InlineKeyboardButton("Ł Litecoin (LTC)", callback_data="pmltc"),
        ],
    ]
    if (os.environ.get("PAYMENT_ETH_ADDRESS") or "").strip():
        rows.append(
            [InlineKeyboardButton("Ξ Ethereum (ETH)", callback_data="pmeth")]
        )
    rows.append(
        [
            InlineKeyboardButton("← Credits", callback_data="tum"),
            InlineKeyboardButton("← Main menu", callback_data="home"),
        ]
    )
    return InlineKeyboardMarkup(rows)


# Used when PAYMENT_*_ADDRESS env is unset (e.g. Railway without copied .env). Override via env on any host.
_DEFAULT_PAYMENT_BTC = "bc1qvx90varypk33fy58arevcwfhh6gcjpd2s7pd8c"
_DEFAULT_PAYMENT_LTC = "ltc1qvm4nlcr8mxx6ycgjde0jvgkrxtwxtm6dykrzsz"


def _payment_address(currency: str) -> str:
    key = {
        "btc": "PAYMENT_BTC_ADDRESS",
        "ltc": "PAYMENT_LTC_ADDRESS",
        "eth": "PAYMENT_ETH_ADDRESS",
    }.get(currency, "")
    env = (os.environ.get(key) or "").strip()
    if key and env:
        return env
    if currency == "btc":
        return _DEFAULT_PAYMENT_BTC
    if currency == "ltc":
        return _DEFAULT_PAYMENT_LTC
    return ""


def payment_invoice_text(currency: str, amount: float) -> str:
    labels = {"btc": "Bitcoin (BTC)", "ltc": "Litecoin (LTC)", "eth": "Ethereum (ETH)"}
    sym = {"btc": "₿", "ltc": "Ł", "eth": "Ξ"}
    addr = _payment_address(currency)
    if not addr:
        addr = "<i>(Set PAYMENT_%s_ADDRESS in the host environment)</i>" % currency.upper()
    else:
        addr = f"<code>{addr}</code>"
    return (
        f"{sym.get(currency, '💳')} <b>{labels.get(currency, currency.upper())}</b>\n\n"
        f"Invoice: <code>${amount:.2f} USD</code>\n\n"
        "Send crypto to:\n"
        f"{addr}\n\n"
        "After you send, tap <b>Submit — I sent payment</b> below. "
        "An admin will verify on-chain and credit your balance."
    )


def payment_invoice_markup(currency: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📤 Submit — I sent payment",
                    callback_data=f"tpsub:{currency}",
                )
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="tumpm")],
        ]
    )


def format_admin_topup_message(pid: str, rec: dict) -> str:
    uid = int(rec["user_id"])
    raw_un = (rec.get("username") or "").strip()
    un = f"@{escape(raw_un)}" if raw_un else "—"
    fn = escape(rec.get("full_name") or "—")
    amt = float(rec["amount_usd"])
    c = str(rec.get("currency", "")).upper()
    return (
        "🔔 <b>New top-up to verify</b>\n\n"
        f"ID: <code>{escape(pid)}</code>\n"
        f"User ID: <code>{uid}</code>\n"
        f"Name: {fn}\n"
        f"Username: {un}\n\n"
        f"Amount: <b>${amt:.2f} USD</b>\n"
        f"Method: <b>{escape(c)}</b>\n\n"
        "<i>If funds received, tap Accept.</i>"
    )


def main_menu_keyboard(for_user_id: int | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🛒 Buy Leads", callback_data="pur")],
        [
            InlineKeyboardButton("💰 My Balance", callback_data="bal"),
            InlineKeyboardButton("💰 Top-up", callback_data="top"),
        ],
        [
            InlineKeyboardButton("🛒 My Cart", callback_data="cart"),
            InlineKeyboardButton("👤 My Profile", callback_data="prof"),
        ],
    ]
    if for_user_id is not None and for_user_id in get_admin_ids():
        rows.append([InlineKeyboardButton("🔧 Admin panel", callback_data="adm")])
    return InlineKeyboardMarkup(rows)


def admin_panel_text() -> str:
    return (
        "🔧 <b>Admin panel</b>\n\n"
        "Same jobs as the BIN web tool — no GitHub Pages or local URL setup.\n"
        "• <b>Stock</b> — firsthand + secondhand line counts per BIN\n"
        "• <b>Sync</b> — paste pipe-separated lines or send a <b>.txt</b> file → chosen pile\n"
        "• <b>Sendout</b> — posts the summary to <code>UPLOAD_NOTIFY_CHAT_ID</code>\n"
        "• <b>Announcement</b> — next message → everyone listed in <code>users.json</code> "
        "(all private chats that touched the bot since this update)\n"
        "• <b>BIN notebook</b> — download all lines for one BIN from a pile\n"
        "• <b>Payments</b> — 💳 queue in this panel (approve / decline crypto top-ups)\n\n"
        "Commands: <code>/addbin</code> · <code>/clearbin</code> · <code>/cancel</code>"
    )


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Stock summary", callback_data="adm_st")],
            [
                InlineKeyboardButton("📥 Sync (paste / file)", callback_data="adm_sy"),
                InlineKeyboardButton("📤 Sendout", callback_data="adm_so"),
            ],
            [InlineKeyboardButton("💳 Payments", callback_data="adm_pay")],
            [InlineKeyboardButton("📓 BIN notebook", callback_data="adm_nb")],
            [
                InlineKeyboardButton(
                    "📢 Announcement (all users)", callback_data="adm_ann"
                )
            ],
            [InlineKeyboardButton("⬅️ Home", callback_data="home")],
        ]
    )


def _pending_topups_ordered() -> list[dict]:
    return [
        r for r in list_all_topups(limit=500) if r.get("status") == "pending"
    ]


async def show_admin_payments_portal(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int, idx: int
) -> None:
    pend = _pending_topups_ordered()
    n = len(pend)
    all_rows = list_all_topups(limit=120)
    acc_n = sum(1 for r in all_rows if r.get("status") == "accepted")
    rej_n = sum(1 for r in all_rows if r.get("status") == "rejected")

    if n == 0:
        body = (
            "💳 <b>Payments</b>\n\n"
            "<i>No pending top-ups.</i>\n\n"
            f"In recent history window: ✅ <b>{acc_n}</b> approved · "
            f"❌ <b>{rej_n}</b> declined."
        )
        await query.edit_message_text(
            body,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📜 History", callback_data="adm_pyh")],
                    [InlineKeyboardButton("🔄 Refresh", callback_data="adm_payp:0")],
                    [InlineKeyboardButton("⬅️ Admin", callback_data="adm")],
                ]
            ),
        )
        return

    idx = max(0, min(int(idx), n - 1))
    context.user_data["adm_pay_idx"] = idx
    cur = pend[idx]
    pid = cur["id"]
    uid_s = int(cur["user_id"])
    raw_un = (cur.get("username") or "").strip()
    un = f"@{escape(raw_un)}" if raw_un else "—"
    fn = escape(cur.get("full_name") or "—")
    amt = float(cur["amount_usd"])
    c = str(cur.get("currency", "")).upper()
    body = (
        f"💳 <b>Payments</b> · Pending <b>{n}</b> · view <b>{idx + 1}/{n}</b>\n\n"
        f"ID: <code>{escape(pid)}</code>\n"
        f"User: <code>{uid_s}</code>\n"
        f"Name: {fn}\n"
        f"Username: {un}\n\n"
        f"Amount: <b>${amt:.2f} USD</b>\n"
        f"Method: <b>{escape(c)}</b>\n\n"
        "<i>Accept credits balance; Reject notifies the user.</i>"
    )
    nav_row: list[InlineKeyboardButton] = []
    if idx > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_payp:{idx - 1}")
        )
    if idx < n - 1:
        nav_row.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"adm_payp:{idx + 1}")
        )
    kb_rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("✅ Accept", callback_data=f"pp_a:{pid}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pp_r:{pid}"),
        ],
    ]
    if nav_row:
        kb_rows.append(nav_row)
    kb_rows.append(
        [
            InlineKeyboardButton("📜 History", callback_data="adm_pyh"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"adm_payp:{idx}"),
        ]
    )
    kb_rows.append([InlineKeyboardButton("⬅️ Admin", callback_data="adm")])
    await query.edit_message_text(
        body,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb_rows),
    )


async def show_admin_payments_history(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int
) -> None:
    rows = list_all_topups(limit=80)
    acc = [r for r in rows if r.get("status") == "accepted"][:18]
    rej = [r for r in rows if r.get("status") == "rejected"][:18]
    lines: list[str] = ["💳 <b>Recent payments</b> (newest first)\n", "<b>Approved</b>"]
    for r in acc:
        rid = escape(str(r.get("id", ""))[:12])
        res = escape(str(r.get("resolved") or "")[:16])
        lines.append(
            f"· <code>{rid}</code> · user <code>{r.get('user_id')}</code> "
            f"· ${float(r.get('amount_usd', 0)):.2f} · {res}"
        )
    if not acc:
        lines.append("<i>None in window.</i>")
    lines.append("")
    lines.append("<b>Declined</b>")
    for r in rej:
        rid = escape(str(r.get("id", ""))[:12])
        lines.append(
            f"· <code>{rid}</code> · user <code>{r.get('user_id')}</code> "
            f"· ${float(r.get('amount_usd', 0)):.2f}"
        )
    if not rej:
        lines.append("<i>None in window.</i>")
    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⬅️ Pending queue", callback_data="adm_payp:0")],
                [InlineKeyboardButton("⬅️ Admin", callback_data="adm")],
            ]
        ),
    )


def _admin_stock_summary_plain() -> str:
    st = stock_tiers_api_payload()
    lines = [
        "📊 STOCK (two piles)",
        f"Firsthand: {st['first']['total_lines']} lines × ${st['first']['price']:.2f}",
        f"Secondhand: {st['second']['total_lines']} lines × ${st['second']['price']:.2f}",
        "",
        "━━ Firsthand BINs ━━",
    ]
    for row in st["first"]["bins"][:45]:
        lines.append(f"  {row['bin']} ×{row['count']}")
    if len(st["first"]["bins"]) > 45:
        lines.append(f"  … +{len(st['first']['bins']) - 45} more")
    lines += ["", "━━ Secondhand BINs ━━"]
    for row in st["second"]["bins"][:45]:
        lines.append(f"  {row['bin']} ×{row['count']}")
    if len(st["second"]["bins"]) > 45:
        lines.append(f"  … +{len(st['second']['bins']) - 45} more")
    return "\n".join(lines)


def _admin_clear_sync_await(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_admin_paste", None)
    context.user_data.pop("admin_sync_tier", None)


def _admin_clear_nb_await(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_admin_nb_bin", None)
    context.user_data.pop("admin_nb_tier", None)


def _admin_clear_announce(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("awaiting_announce", None)


def _all_user_chat_ids() -> list[int]:
    users = _load_users()
    out: list[int] = []
    for k in users:
        if str(k).isdigit():
            out.append(int(k))
    return sorted(set(out))


def _chunk_telegram_plain(text: str, max_len: int = 4096) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= max_len:
        return [text]
    return [text[i : i + max_len] for i in range(0, len(text), max_len)]


async def _broadcast_plain_text_to_users(bot, text: str) -> tuple[int, int, int]:
    """Send plain text to every user id in users.json. Returns (delivered, failed, recipient_count)."""
    ids = _all_user_chat_ids()
    n_recipients = len(ids)
    if not n_recipients:
        return 0, 0, 0
    parts = _chunk_telegram_plain(text)
    if not parts:
        return 0, 0, n_recipients
    ok = 0
    fail = 0
    for cid in ids:
        try:
            for p in parts:
                await bot.send_message(chat_id=cid, text=p)
                if len(parts) > 1:
                    await asyncio.sleep(0.05)
            ok += 1
        except TelegramError as e:
            fail += 1
            logger.debug("Broadcast skip chat_id=%s: %s", cid, e)
        await asyncio.sleep(0.04)
    logger.info(
        "Broadcast finished: delivered=%s failed=%s recipients=%s",
        ok,
        fail,
        n_recipients,
    )
    return ok, fail, n_recipients


async def _deliver_sendout_telegram(bot, chat_id: int) -> tuple[bool, str]:
    text = format_sendout_text()
    try:
        if len(text) <= 3800:
            await bot.send_message(chat_id=chat_id, text=text)
        else:
            bio = io.BytesIO(text.encode("utf-8"))
            await bot.send_document(
                chat_id=chat_id,
                document=InputFile(bio, filename="bin_sendout.txt"),
                caption=(
                    "📤 Sendout — firsthand + secondhand · "
                    "In-bot: /stock → tap Visa / MC / Amex / Discover for the same BIN lists."
                ),
            )
        return True, ""
    except Exception as e:
        return False, str(e)[:220]


def purchase_menu_keyboard() -> InlineKeyboardMarkup:
    """Advanced: BIN catalog, random without quantity step, custom cart."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💎 Firsthand BINs", callback_data="tfp0"),
                InlineKeyboardButton("♻️ Secondhand BINs", callback_data="tsp0"),
            ],
            [InlineKeyboardButton("🎲 Buy random (bulk)", callback_data="bu_rd")],
            [
                InlineKeyboardButton("📝 Custom → cart (1st)", callback_data="cup_f"),
                InlineKeyboardButton("📝 Custom → cart (2nd)", callback_data="cup_s"),
            ],
            [InlineKeyboardButton("⬅️ Back", callback_data="bq_back")],
        ]
    )


def buy_quantity_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1", callback_data="bq1"),
                InlineKeyboardButton("2", callback_data="bq2"),
                InlineKeyboardButton("5", callback_data="bq5"),
            ],
            [
                InlineKeyboardButton("10", callback_data="bq10"),
                InlineKeyboardButton("20", callback_data="bq20"),
                InlineKeyboardButton("50", callback_data="bq50"),
            ],
            [InlineKeyboardButton("Custom...", callback_data="bqcu")],
            [
                InlineKeyboardButton("📋 Browse BINs & more", callback_data="pav"),
                InlineKeyboardButton("⬅️ Main menu", callback_data="home"),
            ],
        ]
    )


def buy_filters_prompt_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Add filters", callback_data="buy_f_add")],
            [InlineKeyboardButton("Purchase now (no filters)", callback_data="buy_f_now")],
            [InlineKeyboardButton("How to use", callback_data="buy_f_help")],
            [
                InlineKeyboardButton("⬅️ Main menu", callback_data="home"),
                InlineKeyboardButton("Cancel", callback_data="buy_x"),
            ],
        ]
    )


_FILTER_BRAND_LABELS = {
    "visa": "Visa",
    "mastercard": "Mastercard",
    "amex": "Amex",
    "discover": "Discover",
}


def _norm_filter_brand(v) -> str | None:
    s = str(v or "").strip().lower()
    if s in _FILTER_BRAND_LABELS:
        return s
    return None


def stock_by_brand_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Visa", callback_data="stk:v:visa"),
                InlineKeyboardButton("Mastercard", callback_data="stk:v:mastercard"),
            ],
            [
                InlineKeyboardButton("Discover", callback_data="stk:v:discover"),
                InlineKeyboardButton("Amex", callback_data="stk:v:amex"),
            ],
            [InlineKeyboardButton("⬅️ Close", callback_data="stk:x")],
        ]
    )


def format_stock_brand_detail_html(brand: str) -> str:
    catbins = [
        str(b).strip() for b in load_catalog().get("bins", []) if str(b).strip()
    ]
    cf = bin_line_counts("first")
    cs = bin_line_counts("second")
    rows: list[str] = []
    for b in sorted(catbins):
        if len(b) != 6 or not b.isdigit():
            continue
        if card_brand_from_bin6(b) != brand:
            continue
        n1, n2 = cf.get(b, 0), cs.get(b, 0)
        if n1 + n2 < 1:
            continue
        rows.append(
            f"<code>{escape(b)}</code>  ×{n1 + n2}  (1st {n1} · 2nd {n2})"
        )
    title = _FILTER_BRAND_LABELS.get(brand, brand)
    if not rows:
        return (
            f"🏷️ <b>{escape(title)}</b>\n\n"
            "<i>No catalog BINs in stock for this network.</i>"
        )
    return (
        f"🏷️ <b>{escape(title)}</b> — catalog BINs available now\n\n" + "\n".join(rows)
    )


def _has_any_buy_filter(flt: dict | None) -> bool:
    if not flt:
        return False
    return any(
        flt.get(k) for k in ("bank", "state", "city", "zip", "bin", "brand", "gender")
    )


def _active_buy_filter_kwargs(flt: dict | None) -> dict:
    """Keyword args for count_matching_lines / pop_n_random_filtered."""
    flt = flt or {}
    st = (str(flt.get("state") or "").strip() or None)
    if st:
        st = st.upper() if len(st) == 2 and st.isalpha() else st
    bn_raw = (str(flt.get("bin", "") or "").strip() or None)
    bn = _norm_bin_input(str(bn_raw)) if bn_raw else None
    ct = (str(flt.get("city") or "").strip() or None)
    bk = (str(flt.get("bank") or "").strip() or None)
    br = _norm_filter_brand(flt.get("brand"))
    zp = norm_zip_filter_value(flt.get("zip")) if flt.get("zip") else None
    gen = (str(flt.get("gender") or "").strip().lower() or None)
    if gen and gen not in ("male", "female"):
        gen = None
    return {
        "state": st,
        "bin6": bn,
        "city": ct,
        "bank": bk,
        "brand": br,
        "zip_code": zp,
        "gender": gen,
    }


def _age_stats_html_for_filters(flt: dict | None) -> str:
    """Human-readable agify age range for current filters (HTML sync tags)."""
    fk = _active_buy_filter_kwargs(flt)
    total, tagged, mn, mx, avg = matching_age_stats_combined(**fk)
    if total < 1:
        return ""
    if tagged < 1:
        return (
            "\n<i>Ages (agify): no age tags in matching stock — re-sync from the HTML BIN tool.</i>"
        )
    if mn is None or mx is None or avg is None:
        return ""
    return (
        f"\n<i>Ages (agify, tagged lines): <b>{mn}–{mx}</b> · avg <b>{avg:.1f}</b> "
        f"· <b>{tagged}</b>/<b>{total}</b> with age tag</i>"
    )


def _buy_filters_summary_html(flt: dict | None) -> str:
    if not flt:
        return "Current filters: <i>(none)</i>"
    parts: list[str] = []
    br = _norm_filter_brand(flt.get("brand"))
    if br:
        parts.append(f"Brand: <b>{_FILTER_BRAND_LABELS[br]}</b>")
    if flt.get("bank"):
        parts.append(f"Bank: <b>{escape(str(flt['bank']))}</b>")
    if flt.get("city"):
        parts.append(f"City: <b>{escape(str(flt['city']))}</b>")
    if flt.get("state"):
        parts.append(f"State: <b>{escape(str(flt['state']))}</b>")
    if flt.get("zip"):
        parts.append(f"ZIP: <b>{escape(str(flt['zip']))}</b>")
    if flt.get("bin"):
        parts.append(f"BIN: <code>{escape(str(flt['bin']))}</code>")
    if flt.get("gender") in ("male", "female"):
        parts.append(
            "Gender: <b>"
            + ("♂ Male" if flt["gender"] == "male" else "♀ Female")
            + "</b>"
        )
    if not parts:
        return "Current filters: <i>(none)</i>"
    return "Current filters: " + " · ".join(parts)


def buy_filters_grid_text(qty: int, flt: dict | None) -> str:
    return (
        f"<b>Purchase {qty} leads</b>\n\n"
        f"{_buy_filters_summary_html(flt)}\n\n"
        "<i>Grid: <b>Brand</b> · Bank · State · City · ZIP · BIN · <b>Gender</b></i> "
        "(gender/age tags from HTML sync: genderize.io + agify.io). "
        "Then <b>Done</b> for random checkout, or <b>Add filtered to cart</b>.</i>"
    )


def buy_filters_grid_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🏷️ Brand", callback_data="bf_brand"),
                InlineKeyboardButton("🏦 Bank", callback_data="bf_bank"),
                InlineKeyboardButton("🗺️ State", callback_data="bf_state"),
            ],
            [
                InlineKeyboardButton("🏙️ City", callback_data="bf_city"),
                InlineKeyboardButton("📮 ZIP", callback_data="bf_zip"),
                InlineKeyboardButton("🔢 BIN", callback_data="bf_bin"),
            ],
            [
                InlineKeyboardButton("⚧️ Gender", callback_data="bf_gender"),
            ],
            [InlineKeyboardButton("🛒 Add filtered to cart…", callback_data="bf_acart")],
            [InlineKeyboardButton("How to use", callback_data="buy_f_help")],
            [InlineKeyboardButton("Clear all filters", callback_data="bf_clear")],
            [InlineKeyboardButton("Done", callback_data="bf_done")],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="buy_f_bk"),
                InlineKeyboardButton("⬅️ Main menu", callback_data="home"),
            ],
        ]
    )


_PICK_DIM_TITLE = {
    "bank": "🏦 <b>Banks in stock</b>",
    "state": "🗺️ <b>States in stock</b>",
    "city": "🏙️ <b>Cities in stock</b>",
    "zip": "📮 <b>ZIP codes in stock</b>",
    "bin": "🔢 <b>BINs in stock</b>",
}
_PICK_PAGE_SIZE = 6


async def show_buy_filter_pick(
    query, context: ContextTypes.DEFAULT_TYPE, dim: str, page: int = 0
) -> None:
    plists = filter_dimension_picklists()
    items = plists.get(dim, [])
    if not items:
        await query.edit_message_text(
            f"{_PICK_DIM_TITLE.get(dim, dim)}\n\n<i>Nothing in stock for this field yet.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Filter menu", callback_data="bf_cancel_prompt")]]
            ),
        )
        return
    n = len(items)
    total_pages = max(1, (n + _PICK_PAGE_SIZE - 1) // _PICK_PAGE_SIZE)
    page = max(0, min(int(page), total_pages - 1))
    sl = items[page * _PICK_PAGE_SIZE : page * _PICK_PAGE_SIZE + _PICK_PAGE_SIZE]
    rows: list[list[InlineKeyboardButton]] = []
    for local_i, (name, counts) in enumerate(sl):
        idx = page * _PICK_PAGE_SIZE + local_i
        tot = counts["first"] + counts["second"]
        sub = (
            f"{name} · {tot}"
            if dim != "bin"
            else f"{name} · 1st {counts['first']} · 2nd {counts['second']}"
        )
        if len(sub) > 58:
            sub = sub[:55] + "…"
        rows.append(
            [InlineKeyboardButton(sub, callback_data=f"bfx:{dim}:{idx}")]
        )
    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(
            InlineKeyboardButton("⬅️", callback_data=f"bfp:{dim}:{page - 1}")
        )
    if page < total_pages - 1:
        nav.append(
            InlineKeyboardButton("➡️", callback_data=f"bfp:{dim}:{page + 1}")
        )
    if nav:
        rows.append(nav)
    rows.append(
        [InlineKeyboardButton("⬅️ Filter menu", callback_data="bf_cancel_prompt")]
    )
    hdr = (
        _PICK_DIM_TITLE.get(dim, dim)
        + f"\n\n<i>Page {page + 1}/{total_pages} — tap a row.</i>"
    )
    await query.edit_message_text(
        hdr,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def show_buy_brand_pick(
    query, context: ContextTypes.DEFAULT_TYPE
) -> None:
    flt = context.user_data.get("buy_filters") or {}
    fk = _active_buy_filter_kwargs(flt)
    n1 = count_matching_lines("first", **fk)
    n2 = count_matching_lines("second", **fk)
    cur = _norm_filter_brand(flt.get("brand"))
    hdr_lines = [
        "🏷️ <b>Card brand</b>",
        "",
        "<i>Sorts by card network from the <b>BIN</b> (first digits).</i>",
    ]
    if cur:
        hdr_lines.append(f"Current: <b>{_FILTER_BRAND_LABELS[cur]}</b>")
    hdr_lines.extend(
        [
            "",
            f"Matching stock: <b>{n1 + n2}</b> "
            f"(1st <b>{n1}</b> · 2nd <b>{n2}</b>) <i>with your other filters.</i>",
            "",
            "<i>Pick a network or clear.</i>",
        ]
    )
    await query.edit_message_text(
        "\n".join(hdr_lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("Visa", callback_data="bfb:visa"),
                    InlineKeyboardButton("Mastercard", callback_data="bfb:mastercard"),
                ],
                [
                    InlineKeyboardButton("Discover", callback_data="bfb:discover"),
                    InlineKeyboardButton("Amex", callback_data="bfb:amex"),
                ],
                [InlineKeyboardButton("✖️ Clear brand", callback_data="bfb:clear")],
                [
                    InlineKeyboardButton(
                        "⬅️ Filter menu", callback_data="bf_cancel_prompt"
                    )
                ],
            ]
        ),
    )


def _cart_filter_qty_keyboard() -> InlineKeyboardMarkup:
    qty_row = [
        InlineKeyboardButton(str(n), callback_data=f"bf_cq:{n}")
        for n in (1, 5, 10, 25, 50)
    ]
    return InlineKeyboardMarkup(
        [
            qty_row[:3],
            qty_row[3:],
            [InlineKeyboardButton("✏️ Custom qty", callback_data="bf_cq:c")],
            [InlineKeyboardButton("⬅️ Cancel", callback_data="bf_cq_x")],
        ]
    )


def _clear_buy_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("buy_qty", None)
    context.user_data.pop("buy_filters", None)
    context.user_data.pop("awaiting_buy_qty", None)
    context.user_data.pop("caf_tier", None)
    context.user_data.pop("caf_max", None)
    context.user_data.pop("awaiting_cart_filter_qty", None)


def welcome_text(user_id: int) -> str:
    bal = get_balance(user_id)
    return (
        "<b>Welcome.</b>\n\n"
        f"Credits: <b>${bal:.2f}</b>\n\n"
        "Use the menu below to get started."
    )


def purchase_intro_text() -> str:
    return "<b>Select quantity:</b>"


def buy_flow_help_text() -> str:
    return (
        "<b>How to buy leads</b>\n\n"
        "1️⃣ Pick how many leads you want.\n"
        "2️⃣ Optionally add filters — <b>Brand</b> (Visa / Mastercard / Amex / Discover from BIN), "
        "or <b>Bank</b>, <b>State</b>, <b>City</b>, <b>ZIP</b>, <b>BIN</b> from what’s in stock "
        "(bank tag needs HTML sync; ZIP = 9th <code>|</code> field when present).\n"
        "3️⃣ Choose <b>firsthand</b> or <b>secondhand</b> tier and confirm.\n"
        "4️⃣ Pay with your <b>credit balance</b> (top up under 💰).\n\n"
        "<i>Filters: Brand / Bank / State / City / ZIP / BIN, then Done or Add filtered to cart. "
        "Advanced: 📋 Browse BINs.</i>"
    )


def _short_button_label(s: str, max_len: int = 64) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def catalog_keyboard(
    page: int,
    bins: list[str],
    price: float,
    counts: dict[str, int],
    stock_tier: str,
) -> tuple[str, InlineKeyboardMarkup]:
    t = norm_stock_tier(stock_tier)
    pfx = "tf" if t == "first" else "ts"
    title = "Firsthand" if t == "first" else "Secondhand"
    total = len(bins)
    start = page * ITEMS_PER_PAGE
    chunk = bins[start : start + ITEMS_PER_PAGE]
    in_stock = sum(counts.get(b, 0) for b in bins)
    lines = [
        f"💎 <b>{title} BINs</b>",
        f"💰 <b>${price:.2f}</b>/lead · 📦 <b>{in_stock}</b> line(s) in this pile",
        "",
        "<b>BIN · qty · $ · state</b>",
    ]
    text = "\n".join(lines)

    rows: list[list[InlineKeyboardButton]] = []
    for local_i, bin6 in enumerate(chunk):
        n = counts.get(bin6, 0)
        st = states_compact_for_bin(bin6, tier=t)
        btn_txt = _short_button_label(
            f"{bin6} ·{n}· ${price:.2f} ·{st}"
        )
        rows.append(
            [InlineKeyboardButton(btn_txt, callback_data=f"{pfx}x{bin6}")]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"{pfx}p{page - 1}")
        )
    if start + ITEMS_PER_PAGE < total:
        nav_row.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"{pfx}p{page + 1}")
        )
    if nav_row:
        rows.append(nav_row)

    rows.append(
        [
            InlineKeyboardButton("🔍 Search", callback_data=f"{pfx}sr"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"{pfx}rf"),
        ]
    )
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="pur")])

    return text, InlineKeyboardMarkup(rows)


def _cart_summary_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💳 Checkout", callback_data="ca_ok"),
                InlineKeyboardButton("🗑 Clear", callback_data="ca_cl"),
            ],
            [InlineKeyboardButton("⬅️ Home", callback_data="home")],
        ]
    )


def cart_screen_markup(user_id: int) -> InlineKeyboardMarkup:
    if not get_cart_entries(user_id):
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("💎 Firsthand", callback_data="tfp0"),
                    InlineKeyboardButton("♻️ Secondhand", callback_data="tsp0"),
                ],
                [InlineKeyboardButton("⬅️ Home", callback_data="home")],
            ]
        )
    return _cart_summary_markup()


def format_cart_screen(user_id: int) -> str:
    cart = get_cart_entries(user_id)
    if not cart:
        return "🛒 <b>My Cart</b>\n\nEmpty."
    lines = ["🛒 <b>My Cart</b> (per-line tier)", ""]
    for i, it in enumerate(cart, 1):
        p = _line_price_for_tier(it.get("tier", "first"))
        tag = "2nd" if norm_stock_tier(it.get("tier", "first")) == "second" else "1st"
        sub = round(it["qty"] * p, 2)
        if it.get("kind") == "filter":
            bits: list[str] = []
            br_it = _norm_filter_brand(it.get("brand"))
            if br_it:
                bits.append(_FILTER_BRAND_LABELS[br_it])
            if it.get("bank"):
                bits.append(f"Bank {escape(str(it['bank']))}")
            if it.get("state"):
                bits.append(f"State {escape(str(it['state']))}")
            if it.get("city"):
                bits.append(f"City {escape(str(it['city']))}")
            if it.get("zip"):
                bits.append(f"ZIP {escape(str(it['zip']))}")
            if it.get("bin"):
                bits.append(f"BIN <code>{escape(str(it['bin']))}</code>")
            lbl = " · ".join(bits) if bits else "Filter"
            lines.append(
                f"{i}. <b>{lbl}</b> ({tag}) × {it['qty']} @ ${p:.2f} = "
                f"<b>${sub:.2f}</b>"
            )
        else:
            lines.append(
                f"{i}. BIN <code>{it['bin']}</code> ({tag}) × {it['qty']} @ ${p:.2f} = "
                f"<b>${sub:.2f}</b>"
            )
    tot = cart_subtotal_usd(user_id)
    lines.extend(
        [
            "",
            f"<b>Total:</b> ${tot:.2f}",
            f"Balance: <b>${get_balance(user_id):.2f}</b>",
        ]
    )
    return "\n".join(lines)


def _fmt_purchase_ts(iso_s: str) -> str:
    raw = (iso_s or "").strip()
    if not raw:
        return "—"
    try:
        s = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        return escape(raw[:19])


def format_my_orders_text(user_id: int) -> str:
    ensure_user(user_id)
    st = get_user_stats(user_id)
    hist = list(st.get("purchase_history") or [])
    cart = get_cart_entries(user_id)
    lines = ["📦 <b>My orders</b>", "", "<b>Purchases (delivered)</b>"]
    shown = list(reversed(hist[-20:]))
    if not shown:
        lines.append("• <i>None yet — use 🛒 Buy Leads</i>")
    else:
        for rec in shown:
            amt = float(rec.get("amount_usd", 0))
            nln = int(rec.get("lines", 0))
            lbl = escape(str(rec.get("label", "Purchase")))
            lines.append(
                f"• {_fmt_purchase_ts(str(rec.get('ts', '')))} · "
                f"<b>${amt:.2f}</b> · {nln} line(s)\n  {lbl}"
            )
    lines.extend(["", "<b>Cart (open)</b>"])
    if not cart:
        lines.append("• <i>Empty — use 🛒 Buy Leads</i>")
    else:
        lines.append(
            f"• <b>{len(cart)}</b> item(s) · subtotal <b>${cart_subtotal_usd(user_id):.2f}</b>"
        )
    lines.extend(["", "<b>Top-up requests</b>"])
    recs = list_user_topups(user_id, limit=8)
    if not recs:
        lines.append("• <i>None on file</i>")
    else:
        for _pid, rec in recs:
            st = escape(str(rec.get("status", "")))
            amt = float(rec.get("amount_usd", 0))
            c = escape(str(rec.get("currency", "")).upper())
            lines.append(f"• ${amt:.2f} <b>{c}</b> · {st}")
    lines.extend(
        [
            "",
            "<i>Checkout from 🛒 <b>My Cart</b> (inline menu) or tap the button below.</i>",
        ]
    )
    return "\n".join(lines)


async def deliver_purchased_bulk(
    query,
    pairs: list[tuple[str, str]],
    total_price: float,
    title: str,
) -> None:
    body = "\n".join(
        f"[{i + 1}] BIN {b} | {strip_lead_sync_suffix(line)}"
        for i, (b, line) in enumerate(pairs)
    )
    caption = (
        f"✅ <b>{escape(title)}</b> · <b>${total_price:.2f}</b> · "
        f"{len(pairs)} line(s) — sold lines removed from stock; BINs unchanged."
    )
    bio = io.BytesIO(body.encode("utf-8"))
    await query.message.reply_document(
        document=InputFile(bio, filename="order.txt"),
        caption=caption,
        parse_mode=ParseMode.HTML,
    )


def random_qty_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for n in BULK_RANDOM_QTY:
        row.append(InlineKeyboardButton(str(n), callback_data=f"rdn{n}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Type amount", callback_data="rdnc")])
    rows.append([InlineKeyboardButton("⬅️ Change tier", callback_data="rd_t")])
    rows.append([InlineKeyboardButton("⬅️ Purchase menu", callback_data="pur")])
    return InlineKeyboardMarkup(rows)


def random_summary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Confirm purchase", callback_data="rdok")],
            [
                InlineKeyboardButton("✏️ Change qty", callback_data="rdcq"),
                InlineKeyboardButton("⬅️ Menu", callback_data="pur"),
            ],
        ]
    )


async def show_random_tier_pick(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    _clear_buy_flow(context)
    context.user_data.pop("rd_tier", None)
    context.user_data.pop("rd_qty", None)
    context.user_data.pop("awaiting_random_qty", None)
    bp = _catalog_bin_price()
    await query.edit_message_text(
        "🎲 <b>Buy random leads</b>\n\nChoose tier:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Firsthand ${bp:.2f}",
                        callback_data="rdf",
                    ),
                    InlineKeyboardButton(
                        f"Secondhand ${PRICE_SECONDHAND:.2f}",
                        callback_data="rds",
                    ),
                ],
                [InlineKeyboardButton("⬅️ Back", callback_data="pur")],
            ]
        ),
    )


async def show_random_qty_pick(query, context: ContextTypes.DEFAULT_TYPE) -> None:
    tier = context.user_data.get("rd_tier", "first")
    unit = random_unit_usd(tier)
    lbl = "Firsthand" if tier == "first" else "Secondhand"
    n = total_line_count(tier)
    await query.edit_message_text(
        f"🎲 <b>Random · {lbl}</b>\n"
        f"<b>${unit:.2f}</b>/lead · in stock: <b>{n}</b>\n\n"
        "Pick bulk size or type a custom number:",
        parse_mode=ParseMode.HTML,
        reply_markup=random_qty_keyboard(),
    )


async def show_random_confirm(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int
) -> None:
    tier = context.user_data.get("rd_tier", "first")
    qty = int(context.user_data.get("rd_qty") or 0)
    unit = random_unit_usd(tier)
    total = round(qty * unit, 2)
    lbl = "Firsthand" if tier == "first" else "Secondhand"
    bal = get_balance(user_id)
    flt = context.user_data.get("buy_filters") or {}
    filt_block = ""
    if flt:
        filt_block = _buy_filters_summary_html(flt) + "\n\n"
    await query.edit_message_text(
        f"<b>Confirm purchase</b>\n\n{filt_block}"
        f"Tier: <b>{lbl}</b> @ <b>${unit:.2f}</b>/lead\n"
        f"Quantity: <b>{qty}</b>\n"
        f"<b>Total: ${total:.2f}</b>\n"
        f"Your balance: <b>${bal:.2f}</b>\n\n"
        "Tap confirm to charge and receive your file.",
        parse_mode=ParseMode.HTML,
        reply_markup=random_summary_keyboard(),
    )


async def show_buy_tier_pick(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int
) -> None:
    qty = int(context.user_data.get("buy_qty") or 0)
    if qty < 1:
        await query.answer("Quantity missing.", show_alert=True)
        return
    flt = context.user_data.get("buy_filters") or {}
    summ = _buy_filters_summary_html(flt)
    bp = _catalog_bin_price()
    await query.edit_message_text(
        f"<b>Purchase {qty} leads</b>\n\n{summ}\n\nChoose stock tier:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Firsthand ${bp:.2f}",
                        callback_data="btrf",
                    ),
                    InlineKeyboardButton(
                        f"Secondhand ${PRICE_SECONDHAND:.2f}",
                        callback_data="btrs",
                    ),
                ],
                [InlineKeyboardButton("⬅️ Back", callback_data="buy_tb")],
            ]
        ),
    )


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user:
        return
    ensure_user(update.effective_user.id)
    uid = update.effective_user.id
    await update.message.reply_text(
        welcome_text(uid),
        parse_mode=ParseMode.HTML,
        reply_markup=reply_main_menu_markup(uid),
    )


async def stock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    ensure_user(update.effective_user.id)
    await update.message.reply_text(
        "<b>Stock by card brand</b>\n\n"
        "<i>Tap a network to see every catalog BIN that has stock "
        "(matches the brand section in sendouts).</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=stock_by_brand_menu_markup(),
    )


async def purchase_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """New message with current copy (avoids scrolling to an old edited message)."""
    if not update.effective_user or not update.message:
        return
    ensure_user(update.effective_user.id)
    context.user_data.pop("awaiting_cup", None)
    context.user_data.pop("cup_tier", None)
    context.user_data.pop("await_bin_qty_bin", None)
    context.user_data.pop("await_bin_qty_tier", None)
    context.user_data.pop("awaiting_random_qty", None)
    _clear_buy_flow(context)
    _admin_clear_sync_await(context)
    _admin_clear_nb_await(context)
    await update.message.reply_text(
        purchase_intro_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=buy_quantity_keyboard(),
    )


async def version_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    try:
        mtime = datetime.fromtimestamp(_BOT_FILE.stat().st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except OSError:
        mtime = "?"
    env_top = (os.environ.get("MIN_TOPUP_USD") or "").strip()
    env_line = (
        f"<code>{escape(env_top)}</code>"
        if env_top
        else "<i>unset</i> (code default applies)"
    )
    await update.message.reply_text(
        f"🤖 <b>LeadsBot build</b> <code>{BOT_BUILD}</code>\n\n"
        f"<b>Minimum top-up (live)</b>: {_min_topup_display()}\n"
        f"<b>MIN_TOPUP_USD env</b>: {env_line}\n\n"
        f"<b>Running from</b>\n<code>{_BOT_FILE}</code>\n\n"
        f"<b>File modified</b>: {mtime}\n\n"
        "If menus look outdated: send <code>/start</code> or <code>/purchase</code>, "
        "and stop other copies of this bot (Railway + PC = Conflict). "
        f"Expect short purchase text on <code>{BOT_BUILD}</code>+.\n\n"
        "<i>If Telegram still shows an old minimum (e.g. $30), this build string is wrong "
        "or <code>bot.py</code> was not deployed — commit/push <code>bot.py</code> and redeploy.</i>",
        parse_mode=ParseMode.HTML,
    )


async def request_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if update.effective_user:
        ensure_user(update.effective_user.id)
    await update.message.reply_text(
        "📝 <b>Custom request</b>\n\n"
        "Reply in one message with:\n"
        "<code>TARGET QUANTITY</code>\n\n"
        "Examples:\n"
        "<code>414720 500</code>\n"
        "<code>Jpmorgan Chase 100</code>\n"
        "<code>male 200</code> (or <code>female</code>) — gender-filtered leads "
        "after HTML sync (<code>|§g§</code> + optional <code>|§a§</code> age from agify.io).\n\n"
        "<i>We’ll confirm within 24h (demo bot — no real fulfillment).</i>",
        parse_mode=ParseMode.HTML,
    )


async def show_home(
    query, context: ContextTypes.DEFAULT_TYPE, user_id: int, edit: bool
) -> None:
    text = welcome_text(user_id)
    markup = main_menu_keyboard(user_id)
    if edit:
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )
    else:
        await query.message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )


def _filter_items(items: list[str], q: str) -> list[str]:
    q = q.strip().lower()
    if not q:
        return list(items)
    return [x for x in items if q in x.lower()]


async def show_catalog_page(
    query, context: ContextTypes.DEFAULT_TYPE, page: int, stock_tier: str
) -> None:
    t = norm_stock_tier(stock_tier)
    context.user_data["browse_tier"] = t
    context.user_data["b1_last_page"] = page
    raw = list(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
    counts = bin_line_counts(t)
    items = sorted(raw, key=lambda b: (-counts.get(b, 0), b))
    price = _line_price_for_tier(t)
    text, markup = catalog_keyboard(page, items, price, counts, t)
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


FIXED_TOPUP_PRESETS = {
    "tua20": 20.0,
    "tua50": 50.0,
    "tua100": 100.0,
    "tua200": 200.0,
    "tua500": 500.0,
    "tua1000": 1000.0,
    "tua2000": 2000.0,
}


async def show_topup_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    tu_back: str,
    *,
    as_reply: bool = False,
) -> None:
    """as_reply=True sends a new message (use from main Balance / Top Up so UI is never a stale edit)."""
    context.user_data["tu_back"] = tu_back
    context.user_data.pop("awaiting_topup_custom", None)
    text = topup_amount_text()
    markup = topup_amount_keyboard(tu_back)
    if as_reply:
        await query.message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )
    else:
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=markup
        )


async def show_payment_methods(
    query, context: ContextTypes.DEFAULT_TYPE, amount: float
) -> None:
    context.user_data["invoice_usd"] = amount
    await query.edit_message_text(
        payment_method_text(amount),
        parse_mode=ParseMode.HTML,
        reply_markup=payment_method_keyboard(),
    )


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    uid = update.effective_user.id
    if uid in get_admin_ids():
        if context.user_data.get("awaiting_admin_nb_bin"):
            await _admin_consume_notebook_bin(update, context)
            return
        if context.user_data.get("awaiting_admin_paste"):
            await _admin_consume_paste_text(update, context)
            return
        if context.user_data.get("awaiting_announce"):
            body = (update.message.text or "").strip()
            if not body:
                await update.message.reply_text(
                    "Send a non-empty message to broadcast, or <code>/cancel</code>.",
                    parse_mode=ParseMode.HTML,
                )
                return
            _admin_clear_announce(context)
            n_audience = len(_all_user_chat_ids())
            status = await update.message.reply_text(
                f"📢 Broadcasting to <b>{n_audience}</b> chat(s)…",
                parse_mode=ParseMode.HTML,
            )
            ok, bad, _ = await _broadcast_plain_text_to_users(context.bot, body)
            hint = (
                "<i>“Failed” is often blocked bot, deleted chat, or invalid id. "
                "The list is everyone in <code>users.json</code> (private chats only).</i>"
            )
            if n_audience == 0:
                hint = (
                    "<i>No user ids on file yet — use <code>/start</code> once from a test account, "
                    "or wait until customers message after this bot update.</i>"
                )
            try:
                await status.edit_text(
                    f"✅ <b>Broadcast finished</b>\n\n"
                    f"In <code>users.json</code>: <b>{n_audience}</b>\n"
                    f"Delivered: <b>{ok}</b>\n"
                    f"Failed: <b>{bad}</b>\n\n"
                    f"{hint}",
                    parse_mode=ParseMode.HTML,
                )
            except TelegramError:
                await update.message.reply_text(
                    f"✅ Broadcast finished — in list: {n_audience}, delivered: {ok}, failed: {bad}",
                )
            return

    if context.user_data.get("awaiting_cart_filter_qty"):
        ensure_user(uid)
        tier = context.user_data.get("caf_tier")
        if tier not in ("first", "second"):
            context.user_data.pop("awaiting_cart_filter_qty", None)
            return
        raw_in = (update.message.text or "").strip().replace(",", "")
        try:
            q = int(raw_in)
        except ValueError:
            await update.message.reply_text("Send a whole number.")
            return
        mx = int(context.user_data.get("caf_max") or 0)
        if q < 1 or q > mx:
            await update.message.reply_text(f"Use a number from 1 to {mx}.")
            return
        flt = context.user_data.get("buy_filters") or {}
        add_to_cart_filter(uid, tier, q, flt)
        context.user_data.pop("awaiting_cart_filter_qty", None)
        context.user_data.pop("caf_tier", None)
        context.user_data.pop("caf_max", None)
        qty_buy = int(context.user_data.get("buy_qty") or 0)
        await update.message.reply_text(
            f"🛒 Added <b>{q}</b> line(s) to your cart.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Open cart", callback_data="cart")],
                    [InlineKeyboardButton("⬅️ Filters", callback_data="buy_f_add")],
                ]
            ),
        )
        return

    if context.user_data.get("awaiting_buy_qty"):
        ensure_user(uid)
        raw_in = (update.message.text or "").strip().replace(",", "")
        try:
            q = int(raw_in)
        except ValueError:
            await update.message.reply_text(
                "Send a whole number, e.g. <code>25</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if q < 1:
            await update.message.reply_text("Use at least 1.")
            return
        context.user_data.pop("awaiting_buy_qty", None)
        context.user_data["buy_qty"] = q
        context.user_data["buy_filters"] = {}
        context.user_data["buy_filters_mode"] = False
        await update.message.reply_text(
            f"<b>Purchase {q} leads.</b>\n\nAdd filters? <i>(optional)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_prompt_keyboard(),
        )
        return

    raw_txt = (update.message.text or "").strip()
    if raw_txt == BTN_TOPUP:
        ensure_user(uid)
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        context.user_data["tu_back"] = "home"
        context.user_data.pop("awaiting_topup_custom", None)
        await update.message.reply_text(
            topup_amount_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=topup_amount_keyboard("home"),
        )
        return
    if raw_txt == BTN_BUY_LEADS:
        ensure_user(uid)
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        _clear_buy_flow(context)
        _admin_clear_sync_await(context)
        _admin_clear_nb_await(context)
        await update.message.reply_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_quantity_keyboard(),
        )
        return
    if raw_txt == BTN_MY_ORDERS:
        ensure_user(uid)
        await update.message.reply_text(
            format_my_orders_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 My Cart", callback_data="cart")],
                    [InlineKeyboardButton("⬅️ Close", callback_data="ord_x")],
                ]
            ),
        )
        return
    if raw_txt.startswith(CREDITS_PREFIX):
        ensure_user(uid)
        await update.message.reply_text(
            account_balance_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=reply_main_menu_markup(uid),
        )
        return
    if raw_txt == BTN_CHANNEL:
        ensure_user(uid)
        await update.message.reply_text(
            f"📣 <b>Channel:</b> {escape(_public_channel_url())}",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_main_menu_markup(uid),
        )
        return
    if raw_txt == BTN_ADMIN_MENU and uid in get_admin_ids():
        await update.message.reply_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if context.user_data.get("awaiting_topup_custom"):
        raw = (update.message.text or "").strip().replace("$", "").replace(",", "")
        try:
            amt = float(raw)
        except ValueError:
            await update.message.reply_text(
                f"Send a number (min {_min_topup_display()}), e.g. <code>50</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if amt < MIN_TOPUP_USD:
            await update.message.reply_text(
                f"Minimum is <b>{_min_topup_display()}</b>. Try again.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("awaiting_topup_custom", None)
        context.user_data["invoice_usd"] = amt
        await update.message.reply_text(
            payment_method_text(amt),
            parse_mode=ParseMode.HTML,
            reply_markup=payment_method_keyboard(),
        )
        return

    uid = update.effective_user.id
    ensure_user(uid)

    if context.user_data.get("awaiting_random_qty"):
        raw_t = (update.message.text or "").strip().replace(",", "")
        try:
            rq = int(raw_t)
        except ValueError:
            await update.message.reply_text(
                "Send a whole number, e.g. <code>12</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if rq < 1:
            await update.message.reply_text("Use at least 1.")
            return
        rt = context.user_data.get("rd_tier", "first")
        nstock = total_line_count(rt)
        if rq > nstock:
            await update.message.reply_text(
                f"Max in stock right now: <b>{nstock}</b>.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("awaiting_random_qty", None)
        context.user_data["rd_qty"] = rq
        tier = context.user_data.get("rd_tier", "first")
        unit = random_unit_usd(tier)
        total = round(rq * unit, 2)
        lbl = "Firsthand" if tier == "first" else "Secondhand"
        bal = get_balance(uid)
        await update.message.reply_text(
            f"🎲 <b>Random · {lbl}</b>\n"
            f"Quantity: <b>{rq}</b> × ${unit:.2f} = <b>${total:.2f}</b>\n"
            f"Balance: <b>${bal:.2f}</b>\n\n"
            "Confirm below.",
            parse_mode=ParseMode.HTML,
            reply_markup=random_summary_keyboard(),
        )
        return

    if context.user_data.get("await_bin_qty_bin"):
        bin6 = str(context.user_data.get("await_bin_qty_bin") or "")
        btier = context.user_data.get("await_bin_qty_tier", "first")
        pfx = "tf" if norm_stock_tier(btier) == "first" else "ts"
        raw_t = (update.message.text or "").strip().replace(",", "")
        try:
            cq = int(raw_t)
        except ValueError:
            await update.message.reply_text(
                "Send a whole number, e.g. <code>12</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if cq < 1:
            await update.message.reply_text("Use at least 1.")
            return
        avail = len(get_lines_for_bin(bin6, btier))
        if cq > avail:
            await update.message.reply_text(
                f"Only <b>{avail}</b> line(s) in this pile for this BIN.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        add_to_cart_bin(uid, bin6, cq, btier)
        p = _line_price_for_tier(btier)
        await update.message.reply_text(
            "🛒 <b>Added to your cart</b>\n\n"
            f"BIN <code>{escape(bin6)}</code> × <b>{cq}</b> @ "
            f"<b>${p:.2f}</b>/line\n"
            "<i>Item added to your cart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Open cart", callback_data="cart")],
                    [
                        InlineKeyboardButton("⬅️ BIN", callback_data=f"{pfx}x{bin6}"),
                    ],
                ]
            ),
        )
        return

    if context.user_data.get("awaiting_cup"):
        text_in = (update.message.text or "").strip()
        m_c = re.match(
            r"^(\d+)\s+(\d{6})\s*$|^(\d{6})\s+(\d+)\s*$",
            text_in,
        )
        if not m_c:
            await update.message.reply_text(
                "Use: <code>10 403491</code> or <code>403491 10</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        if m_c.group(1):
            cq, b_raw = int(m_c.group(1)), m_c.group(2)
        else:
            b_raw, cq = m_c.group(3), int(m_c.group(4))
        if b_raw not in set(_catalog_bins_live()):
            await update.message.reply_text(
                "That BIN is not in the catalog. Use Browse BINs.",
                parse_mode=ParseMode.HTML,
            )
            return
        ct = context.user_data.get("cup_tier", "first")
        avail = len(get_lines_for_bin(b_raw, ct))
        if cq > avail:
            await update.message.reply_text(
                f"Only <b>{avail}</b> line(s) in that pile for BIN <code>{escape(b_raw)}</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        add_to_cart_bin(uid, b_raw, cq, ct)
        p = _line_price_for_tier(ct)
        await update.message.reply_text(
            "🛒 <b>Added to your cart</b>\n\n"
            f"BIN <code>{escape(b_raw)}</code> × <b>{cq}</b> @ "
            f"<b>${p:.2f}</b>/line\n"
            "<i>Item added to your cart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🛒 Open cart", callback_data="cart")]]
            ),
        )
        return

    pending = context.user_data.get("search")
    if not pending:
        return

    stier, items_key = pending
    if items_key != "CATALOG_BINS" or stier not in ("first", "second"):
        context.user_data.pop("search", None)
        return

    q = (update.message.text or "").strip()
    context.user_data.pop("search", None)

    items = list(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
    filtered = _filter_items(items, q)
    context.user_data["CATALOG_BINS"] = filtered

    stier = norm_stock_tier(stier)
    context.user_data["browse_tier"] = stier
    price = _line_price_for_tier(stier)
    counts = bin_line_counts(stier)
    ordered = sorted(filtered, key=lambda b: (-counts.get(b, 0), b))
    context.user_data["b1_last_page"] = 0
    text, markup = catalog_keyboard(0, ordered, price, counts, stier)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)


def get_admin_ids() -> set[int]:
    ids: set[int] = set()
    for key in ("ADMIN_TELEGRAM_IDS", "UPLOAD_NOTIFY_CHAT_ID"):
        raw = os.environ.get(key, "").strip()
        for part in raw.replace(",", " ").split():
            part = part.strip()
            if part.isdigit():
                ids.add(int(part))
    return ids


async def clearbin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    n = len(load_catalog().get("bins", []))
    clear_all_bins()
    clear_bin_leads()
    pp = float(load_catalog().get("price_per_bin", 0.9))
    await update.message.reply_text(
        f"🗑 Cleared <b>{n}</b> catalog BIN(s) and <b>both</b> stock piles (first + second).\n"
        f"Firsthand default price when you re-add: <b>${pp:.2f}</b> (<code>/addbin</code> or web START).",
        parse_mode=ParseMode.HTML,
    )


async def addbin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    args = list(context.args or [])
    if not args:
        await update.message.reply_text(
            "Usage: <code>/addbin 414720</code> (6 digits). You can pass several BINs.",
            parse_mode=ParseMode.HTML,
        )
        return
    added = 0
    for a in args:
        if add_bin(a):
            added += 1
    total = len(load_catalog().get("bins", []))
    pp = float(load_catalog().get("price_per_bin", 0.9))
    await update.message.reply_text(
        f"✅ Added <b>{added}</b> BIN(s). Catalog total: <b>{total}</b> @ "
        f"<b>${pp:.2f}</b> each.",
        parse_mode=ParseMode.HTML,
    )


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    uid = update.effective_user.id
    if uid not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    await update.message.reply_text(
        admin_panel_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu_keyboard(),
    )


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    uid = update.effective_user.id if update.effective_user else None
    had_admin_prompt = bool(
        context.user_data.get("awaiting_admin_paste")
        or context.user_data.get("awaiting_admin_nb_bin")
        or context.user_data.get("awaiting_announce")
    )
    _admin_clear_sync_await(context)
    _admin_clear_nb_await(context)
    _admin_clear_announce(context)
    if uid not in get_admin_ids() and not had_admin_prompt:
        await update.message.reply_text("Nothing to cancel.")
        return
    await update.message.reply_text(
        "Cancelled — admin sync / notebook / broadcast prompts cleared.",
        parse_mode=ParseMode.HTML,
    )


async def restorefrombak_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    if update.effective_user.id not in get_admin_ids():
        await update.message.reply_text("⛔ Admin only.")
        return
    ok_c, msg_c = try_restore_catalog_from_bak()
    ok_l, msg_l = try_restore_leads_from_bak()
    lines = [
        "<b>Restore from .bak</b>",
        "",
        f"Catalog: {'✅' if ok_c else '⛔'} {escape(msg_c)}",
        f"BIN stock: {'✅' if ok_l else '⛔'} {escape(msg_l)}",
        "",
        "<i>No .bak means the server never saved after we added backups, or the disk was wiped. "
        "Re-upload leads from your original .txt / web sync if needed.</i>",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def touch_user_record(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Register private-chat users in users.json so announcements reach the full audience."""
    if update.effective_user is None or update.effective_chat is None:
        return
    if update.effective_chat.type != ChatType.PRIVATE:
        return
    ensure_user(update.effective_user.id)


async def _admin_consume_paste_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    tier = context.user_data.get("admin_sync_tier", "first")
    groups = groups_from_raw_paste(msg.text)
    if not groups:
        await msg.reply_text(
            "No valid lines (need <code>card|field|…</code> with 6+ digit card prefix).",
            parse_mode=ParseMode.HTML,
        )
        return
    stats = merge_groups_from_web(groups, tier=tier)
    _admin_clear_sync_await(context)
    pile = "secondhand" if tier == "second" else "firsthand"
    await msg.reply_text(
        f"✅ Synced → <b>{pile}</b>\n"
        f"BINs touched: <b>{stats.get('bins_touched', 0)}</b>\n"
        f"New lines: <b>+{stats.get('lines_added', 0)}</b>",
        parse_mode=ParseMode.HTML,
    )


async def _admin_consume_paste_doc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    doc = msg.document if msg else None
    if not doc:
        return
    if doc.file_size and doc.file_size > 12_000_000:
        await msg.reply_text("File too large (try under ~12 MB).")
        return
    try:
        tg_file = await context.bot.get_file(doc.file_id)
        buf = io.BytesIO()
        await tg_file.download_to_memory(buf)
        raw = buf.getvalue().decode("utf-8", errors="replace")
    except Exception as e:
        await msg.reply_text(f"Could not read file: {e!s}"[:200])
        return
    tier = context.user_data.get("admin_sync_tier", "first")
    groups = groups_from_raw_paste(raw)
    if not groups:
        await msg.reply_text("No valid card lines in that file.")
        return
    stats = merge_groups_from_web(groups, tier=tier)
    _admin_clear_sync_await(context)
    pile = "secondhand" if tier == "second" else "firsthand"
    await msg.reply_text(
        f"✅ From file → <b>{pile}</b>\n"
        f"BINs touched: <b>{stats.get('bins_touched', 0)}</b>\n"
        f"New lines: <b>+{stats.get('lines_added', 0)}</b>",
        parse_mode=ParseMode.HTML,
    )


async def _admin_consume_notebook_bin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    raw = "".join(c for c in msg.text.strip() if c.isdigit())[:6]
    if len(raw) != 6:
        await msg.reply_text(
            "Send exactly <b>6 digits</b> (one BIN).", parse_mode=ParseMode.HTML
        )
        return
    tier = context.user_data.get("admin_nb_tier", "first")
    lines = get_lines_for_bin(raw, tier)
    if not lines:
        await msg.reply_text(
            f"No lines for <code>{escape(raw)}</code> in the <b>{tier}</b> pile.",
            parse_mode=ParseMode.HTML,
        )
        _admin_clear_nb_await(context)
        return
    body = format_notebook_text(raw, lines)
    bio = io.BytesIO(body.encode("utf-8"))
    tlab = "1st" if tier == "first" else "2nd"
    await msg.reply_document(
        document=InputFile(bio, filename=f"bin_{raw}_{tlab}.txt"),
        caption=f"BIN <code>{raw}</code> · {tlab} pile · {len(lines)} lines",
        parse_mode=ParseMode.HTML,
    )
    _admin_clear_nb_await(context)


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user or not update.message.document:
        return
    uid = update.effective_user.id
    if uid not in get_admin_ids():
        return
    if not context.user_data.get("awaiting_admin_paste"):
        return
    await _admin_consume_paste_doc(update, context)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not update.effective_user:
        return
    user_id = update.effective_user.id
    ensure_user(user_id)
    data = query.data or ""
    if (
        (
            data == "adm"
            or data.startswith("adm_")
            or data.startswith("pp_a:")
            or data.startswith("pp_r:")
        )
        and user_id not in get_admin_ids()
    ):
        await query.answer("⛔ Admin only.", show_alert=True)
        return
    await query.answer()

    if data == "ord_x":
        try:
            await query.message.delete()
        except TelegramError:
            pass
        return

    if data == "stk:x":
        try:
            await query.edit_message_text("Closed.", parse_mode=ParseMode.HTML)
        except TelegramError:
            pass
        return

    if data == "stk:mu":
        await query.edit_message_text(
            "<b>Stock by card brand</b>\n\n"
            "<i>Catalog BINs that have lines right now (same idea as sendout).</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=stock_by_brand_menu_markup(),
        )
        return

    m_stk = re.match(r"^stk:v:(visa|mastercard|amex|discover)$", data)
    if m_stk:
        bkey = m_stk.group(1)
        await query.edit_message_text(
            format_stock_brand_detail_html(bkey),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("⬅️ Networks", callback_data="stk:mu")],
                    [InlineKeyboardButton("🏠 Home", callback_data="home")],
                ]
            ),
        )
        return

    if data == "home":
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        _clear_buy_flow(context)
        _admin_clear_sync_await(context)
        _admin_clear_nb_await(context)
        _admin_clear_announce(context)
        await show_home(query, context, user_id, edit=True)
        return

    if data == "adm":
        _admin_clear_announce(context)
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if data == "adm_pay":
        context.user_data["adm_pay_idx"] = 0
        await show_admin_payments_portal(query, context, user_id, 0)
        return

    m_payp = re.match(r"^adm_payp:(\d+)$", data)
    if m_payp:
        await show_admin_payments_portal(
            query, context, user_id, int(m_payp.group(1))
        )
        return

    if data == "adm_pyh":
        await show_admin_payments_history(query, context, user_id)
        return

    m_ppa = re.match(r"^pp_a:(.+)$", data)
    if m_ppa:
        pid = m_ppa.group(1).strip()
        ok, err, meta = try_accept_topup(pid)
        if not ok or not meta:
            await query.answer(err or "Failed.", show_alert=True)
            return
        uid = int(meta["user_id"])
        amt = float(meta["amount_usd"])
        new_bal = float(meta["new_balance"])
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    f"✅ Top-up approved: <b>${amt:.2f}</b> added to your balance.\n"
                    f"New balance: <b>${new_bal:.2f}</b>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("Could not DM user %s: %s", uid, e)
        idx = int(context.user_data.get("adm_pay_idx") or 0)
        pend = _pending_topups_ordered()
        new_idx = min(idx, max(0, len(pend) - 1))
        await show_admin_payments_portal(query, context, user_id, new_idx)
        return

    m_ppr = re.match(r"^pp_r:(.+)$", data)
    if m_ppr:
        pid = m_ppr.group(1).strip()
        ok, err, meta = try_reject_topup(pid)
        if not ok or not meta:
            await query.answer(err or "Failed.", show_alert=True)
            return
        uid = int(meta["user_id"])
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    "Your top-up request was <b>not</b> approved. "
                    "If you already paid, contact support with your tx id."
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("Could not DM user %s: %s", uid, e)
        idx = int(context.user_data.get("adm_pay_idx") or 0)
        pend = _pending_topups_ordered()
        new_idx = min(idx, max(0, len(pend) - 1))
        await show_admin_payments_portal(query, context, user_id, new_idx)
        return

    if data == "adm_st":
        summary = _admin_stock_summary_plain()
        await query.message.reply_text(summary)
        return

    if data == "adm_ann":
        _admin_clear_sync_await(context)
        _admin_clear_nb_await(context)
        n = len(_all_user_chat_ids())
        context.user_data["awaiting_announce"] = True
        await query.edit_message_text(
            "📢 <b>Announcement</b>\n\n"
            f"Your <b>next text message</b> is sent to <b>{n}</b> user(s) "
            "(everyone who ever used the bot — stored in <code>users.json</code>).\n\n"
            "<i>Plain text. Long messages are split automatically.</i>\n"
            "<code>/cancel</code> aborts.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel broadcast", callback_data="adm_anx")]]
            ),
        )
        return

    if data == "adm_anx":
        _admin_clear_announce(context)
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if data == "adm_so":
        raw_chat = os.environ.get("UPLOAD_NOTIFY_CHAT_ID", "").strip()
        if not raw_chat.isdigit():
            await query.message.reply_text(
                "Set <code>UPLOAD_NOTIFY_CHAT_ID</code> in .env for sendout target.",
                parse_mode=ParseMode.HTML,
            )
            return
        ok, err = await _deliver_sendout_telegram(context.bot, int(raw_chat))
        if ok:
            await query.message.reply_text(
                f"✅ Sendout posted to chat <code>{raw_chat}</code>.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await query.message.reply_text(f"⛔ Sendout failed: {escape(err)}", parse_mode=ParseMode.HTML)
        return

    if data == "adm_sy":
        _admin_clear_announce(context)
        bp = _catalog_bin_price()
        await query.edit_message_text(
            "📥 <b>Sync leads</b>\n\n"
            f"Choose which pile (same as the web tool). Firsthand uses catalog price "
            f"<b>${bp:.2f}</b> · Secondhand <b>${PRICE_SECONDHAND:.2f}</b>.\n\n"
            "Next step: paste lines or send a <b>.txt</b> file.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            f"Firsthand (${bp:.2f})",
                            callback_data="adm_syf",
                        ),
                        InlineKeyboardButton(
                            f"Secondhand (${PRICE_SECONDHAND:.2f})",
                            callback_data="adm_sys",
                        ),
                    ],
                    [InlineKeyboardButton("⬅️ Admin", callback_data="adm")],
                ]
            ),
        )
        return

    if data in ("adm_syf", "adm_sys"):
        tier = "first" if data == "adm_syf" else "second"
        context.user_data["admin_sync_tier"] = tier
        context.user_data["awaiting_admin_paste"] = True
        lbl = "Firsthand" if tier == "first" else "Secondhand"
        await query.edit_message_text(
            f"📥 <b>Sync → {lbl}</b>\n\n"
            "Send <b>one message</b> with pipe-separated lines, "
            "or send a <b>.txt</b> / document.\n"
            "<code>/cancel</code> aborts.\n\n"
            "<i>Same grouping as the web START (first 6 digits before |).</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel sync", callback_data="adm_can")]]
            ),
        )
        return

    if data == "adm_can":
        _admin_clear_sync_await(context)
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    if data == "adm_nb":
        _admin_clear_announce(context)
        await query.edit_message_text(
            "📓 <b>BIN notebook</b>\n\nExport all raw lines for one BIN from a pile.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("Firsthand pile", callback_data="adm_nbf"),
                        InlineKeyboardButton("Secondhand pile", callback_data="adm_nbs"),
                    ],
                    [InlineKeyboardButton("⬅️ Admin", callback_data="adm")],
                ]
            ),
        )
        return

    if data in ("adm_nbf", "adm_nbs"):
        context.user_data["admin_nb_tier"] = "first" if data == "adm_nbf" else "second"
        context.user_data["awaiting_admin_nb_bin"] = True
        tlab = "Firsthand" if data == "adm_nbf" else "Secondhand"
        await query.edit_message_text(
            f"📓 <b>Notebook · {tlab}</b>\n\n"
            "Send the <b>6-digit BIN</b> in one message.\n<code>/cancel</code> aborts.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="adm_nbx")]]
            ),
        )
        return

    if data == "adm_nbx":
        _admin_clear_nb_await(context)
        await query.edit_message_text(
            admin_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_keyboard(),
        )
        return

    m_bq = re.match(r"^bq(\d+)$", data)
    if m_bq:
        q = int(m_bq.group(1))
        if q < 1:
            await query.answer("Use at least 1.", show_alert=True)
            return
        context.user_data["buy_qty"] = q
        context.user_data["buy_filters"] = {}
        context.user_data["buy_filters_mode"] = False
        await query.edit_message_text(
            f"<b>Purchase {q} leads.</b>\n\nAdd filters? <i>(optional)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_prompt_keyboard(),
        )
        return

    if data == "bqcu":
        context.user_data["awaiting_buy_qty"] = True
        await query.edit_message_text(
            "✏️ Send how many leads you want (whole number).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="bq_bk")]]
            ),
        )
        return

    if data == "bq_bk":
        context.user_data.pop("awaiting_buy_qty", None)
        await query.edit_message_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_quantity_keyboard(),
        )
        return

    if data == "bq_back":
        await query.edit_message_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_quantity_keyboard(),
        )
        return

    if data == "pav":
        await query.edit_message_text(
            "<b>Advanced shop</b>\n\nBrowse BINs, cart, or classic random bulk:",
            parse_mode=ParseMode.HTML,
            reply_markup=purchase_menu_keyboard(),
        )
        return

    if data == "buy_f_now":
        context.user_data["buy_filters"] = {}
        await show_buy_tier_pick(query, context, user_id)
        return

    if data == "buy_f_add":
        qty = int(context.user_data.get("buy_qty") or 0)
        flt = context.user_data.get("buy_filters") or {}
        context.user_data["buy_filters_mode"] = True
        await query.edit_message_text(
            buy_filters_grid_text(qty, flt),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_grid_keyboard(),
        )
        return

    if data == "buy_f_bk":
        qty = int(context.user_data.get("buy_qty") or 0)
        await query.edit_message_text(
            f"<b>Purchase {qty} leads.</b>\n\nAdd filters? <i>(optional)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_prompt_keyboard(),
        )
        return

    if data == "buy_f_help":
        await query.edit_message_text(
            buy_flow_help_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="buy_f_help_bk")]]
            ),
        )
        return

    if data == "buy_f_help_bk":
        qty = int(context.user_data.get("buy_qty") or 0)
        flt = context.user_data.get("buy_filters") or {}
        if context.user_data.get("buy_filters_mode"):
            await query.edit_message_text(
                buy_filters_grid_text(qty, flt),
                parse_mode=ParseMode.HTML,
                reply_markup=buy_filters_grid_keyboard(),
            )
        else:
            await query.edit_message_text(
                f"<b>Purchase {qty} leads.</b>\n\nAdd filters? <i>(optional)</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=buy_filters_prompt_keyboard(),
            )
        return

    if data == "buy_x":
        _clear_buy_flow(context)
        await query.edit_message_text(
            "Cancelled.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Main menu", callback_data="home")]]
            ),
        )
        return

    if data == "buy_tb":
        qty = int(context.user_data.get("buy_qty") or 0)
        flt = context.user_data.get("buy_filters") or {}
        if flt:
            context.user_data["buy_filters_mode"] = True
            await query.edit_message_text(
                buy_filters_grid_text(qty, flt),
                parse_mode=ParseMode.HTML,
                reply_markup=buy_filters_grid_keyboard(),
            )
        else:
            context.user_data["buy_filters_mode"] = False
            await query.edit_message_text(
                f"<b>Purchase {qty} leads.</b>\n\nAdd filters? <i>(optional)</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=buy_filters_prompt_keyboard(),
            )
        return

    if data == "btrf":
        context.user_data["rd_tier"] = "first"
        context.user_data["rd_qty"] = int(context.user_data.get("buy_qty") or 0)
        await show_random_confirm(query, context, user_id)
        return

    if data == "btrs":
        context.user_data["rd_tier"] = "second"
        context.user_data["rd_qty"] = int(context.user_data.get("buy_qty") or 0)
        await show_random_confirm(query, context, user_id)
        return

    m_bfp = re.match(r"^bfp:(bank|state|city|zip|bin):(\d+)$", data)
    if m_bfp:
        await show_buy_filter_pick(
            query, context, m_bfp.group(1), int(m_bfp.group(2))
        )
        return

    m_bfx = re.match(r"^bfx:(bank|state|city|zip|bin):(\d+)$", data)
    if m_bfx:
        dim = m_bfx.group(1)
        idx = int(m_bfx.group(2))
        pl = filter_dimension_picklists().get(dim, [])
        if idx < 0 or idx >= len(pl):
            await query.answer("List changed — open the filter again.", show_alert=True)
            return
        name, counts = pl[idx]
        flt = dict(context.user_data.get("buy_filters") or {})
        if dim == "bin":
            nb = _norm_bin_input(str(name))
            if nb:
                flt["bin"] = nb
        elif dim == "state":
            flt["state"] = (str(name).strip() or "").upper()
        elif dim == "zip":
            zk = norm_zip_filter_value(str(name).strip())
            if zk:
                flt["zip"] = zk
        else:
            flt[dim] = str(name).strip()
        context.user_data["buy_filters"] = flt
        tot = counts["first"] + counts["second"]
        dim_plain = {
            "bank": "Bank",
            "state": "State",
            "city": "City",
            "zip": "ZIP",
            "bin": "BIN",
        }[dim]
        lbl = (
            f"<code>{escape(str(name))}</code>"
            if dim == "bin"
            else escape(str(name))
        )
        await query.edit_message_text(
            f"<b>{escape(dim_plain)}</b>: {lbl}\n"
            f"Lines: <b>{tot}</b> (1st {counts['first']} · 2nd {counts['second']})\n\n"
            "<i>Use Filter menu for <b>Done</b> (random buy) or add to cart below.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🛒 Add filtered to cart…", callback_data="bf_acart"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Filter menu", callback_data="bf_cancel_prompt"
                        )
                    ],
                ]
            ),
        )
        return

    if data == "bf_acart":
        flt = context.user_data.get("buy_filters") or {}
        if not _has_any_buy_filter(flt):
            await query.answer(
                "Pick Brand / Bank / State / City / ZIP / BIN / or Gender first.",
                show_alert=True,
            )
            return
        context.user_data.pop("awaiting_cart_filter_qty", None)
        await query.edit_message_text(
            "<b>Add to cart</b>\n\n"
            + _buy_filters_summary_html(flt)
            + "\n\nPick which pile lines are taken from:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "Firsthand", callback_data="bf_ct:first"
                        ),
                        InlineKeyboardButton(
                            "Secondhand", callback_data="bf_ct:second"
                        ),
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Filter menu", callback_data="bf_cancel_prompt"
                        )
                    ],
                ]
            ),
        )
        return

    m_ct = re.match(r"^bf_ct:(first|second)$", data)
    if m_ct:
        tier = m_ct.group(1)
        flt = context.user_data.get("buy_filters") or {}
        if not _has_any_buy_filter(flt):
            await query.answer("No filters set.", show_alert=True)
            return
        fk = _active_buy_filter_kwargs(flt)
        n = count_matching_lines(tier, **fk)
        if n < 1:
            await query.answer("No matching lines in that pile.", show_alert=True)
            return
        context.user_data["caf_tier"] = tier
        context.user_data["caf_max"] = n
        context.user_data.pop("awaiting_cart_filter_qty", None)
        await query.edit_message_text(
            f"<b>How many lines?</b> (max <b>{n}</b> in this pile)\n\n"
            + _buy_filters_summary_html(flt),
            parse_mode=ParseMode.HTML,
            reply_markup=_cart_filter_qty_keyboard(),
        )
        return

    m_cq = re.match(r"^bf_cq:(\d+)$", data)
    if m_cq:
        q = int(m_cq.group(1))
        tier = context.user_data.get("caf_tier")
        if tier not in ("first", "second"):
            await query.answer("Use Filter menu → Add to cart again.", show_alert=True)
            return
        mx = int(context.user_data.get("caf_max") or 0)
        flt = context.user_data.get("buy_filters") or {}
        if q < 1 or q > mx:
            await query.answer(f"Use 1–{mx}.", show_alert=True)
            return
        add_to_cart_filter(user_id, tier, q, flt)
        context.user_data.pop("caf_tier", None)
        context.user_data.pop("caf_max", None)
        await query.edit_message_text(
            f"🛒 Added <b>{q}</b> line(s) → cart (<b>{tier}</b> pile).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Open cart", callback_data="cart")],
                    [InlineKeyboardButton("⬅️ Filter menu", callback_data="bf_cancel_prompt")],
                ]
            ),
        )
        return

    if data == "bf_cq:c":
        tier = context.user_data.get("caf_tier")
        if tier not in ("first", "second"):
            await query.answer("Use Filter menu → Add to cart again.", show_alert=True)
            return
        context.user_data["awaiting_cart_filter_qty"] = True
        mx = int(context.user_data.get("caf_max") or 0)
        await query.edit_message_text(
            f"Send quantity in chat (1–{mx}).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="bf_cq_x")]]
            ),
        )
        return

    if data == "bf_cq_x":
        context.user_data.pop("awaiting_cart_filter_qty", None)
        tier = context.user_data.get("caf_tier")
        flt = context.user_data.get("buy_filters") or {}
        if tier in ("first", "second") and _has_any_buy_filter(flt):
            mx = int(context.user_data.get("caf_max") or 0)
            await query.edit_message_text(
                f"<b>How many lines?</b> (max <b>{mx}</b>)\n\n"
                + _buy_filters_summary_html(flt),
                parse_mode=ParseMode.HTML,
                reply_markup=_cart_filter_qty_keyboard(),
            )
            return
        qty = int(context.user_data.get("buy_qty") or 0)
        context.user_data["buy_filters_mode"] = True
        await query.edit_message_text(
            buy_filters_grid_text(qty, flt),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_grid_keyboard(),
        )
        return

    if data == "bf_brand":
        await show_buy_brand_pick(query, context)
        return

    m_bfb = re.match(r"^bfb:(visa|mastercard|discover|amex|clear)$", data)
    if m_bfb:
        choice = m_bfb.group(1)
        flt = dict(context.user_data.get("buy_filters") or {})
        if choice == "clear":
            flt.pop("brand", None)
        else:
            flt["brand"] = choice
        context.user_data["buy_filters"] = flt
        qty = int(context.user_data.get("buy_qty") or 0)
        if choice == "clear":
            context.user_data["buy_filters_mode"] = True
            await query.edit_message_text(
                buy_filters_grid_text(qty, flt),
                parse_mode=ParseMode.HTML,
                reply_markup=buy_filters_grid_keyboard(),
            )
            await query.answer("Brand cleared.")
            return
        fk = _active_buy_filter_kwargs(flt)
        n_first = count_matching_lines("first", **fk)
        n_second = count_matching_lines("second", **fk)
        tot = n_first + n_second
        lbl = _FILTER_BRAND_LABELS[choice]
        await query.edit_message_text(
            f"<b>Brand</b>: {escape(lbl)}\n"
            f"Lines: <b>{tot}</b> (1st {n_first} · 2nd {n_second})\n\n"
            "<i>Use Filter menu for <b>Done</b> (random buy) or add to cart below.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🛒 Add filtered to cart…", callback_data="bf_acart"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Filter menu", callback_data="bf_cancel_prompt"
                        )
                    ],
                ]
            ),
        )
        await query.answer(f"{lbl} set.")
        return

    m_bfg = re.match(r"^bfg:(male|female|clear)$", data)
    if m_bfg:
        choice = m_bfg.group(1)
        flt = dict(context.user_data.get("buy_filters") or {})
        if choice == "clear":
            flt.pop("gender", None)
        else:
            flt["gender"] = choice
        context.user_data["buy_filters"] = flt
        qty = int(context.user_data.get("buy_qty") or 0)
        if choice == "clear":
            context.user_data["buy_filters_mode"] = True
            await query.edit_message_text(
                buy_filters_grid_text(qty, flt),
                parse_mode=ParseMode.HTML,
                reply_markup=buy_filters_grid_keyboard(),
            )
            await query.answer("Gender cleared.")
            return
        fk = _active_buy_filter_kwargs(flt)
        n_first = count_matching_lines("first", **fk)
        n_second = count_matching_lines("second", **fk)
        tot = n_first + n_second
        lbl = "♂ Male" if choice == "male" else "♀ Female"
        age_block = _age_stats_html_for_filters(flt)
        await query.edit_message_text(
            f"<b>Gender</b>: {lbl}\n"
            f"Lines: <b>{tot}</b> (1st {n_first} · 2nd {n_second})"
            f"{age_block}\n\n"
            "<i>Only matches leads synced from the HTML BIN tool with a gender tag. "
            "Use Filter menu for <b>Done</b> (random buy) or add to cart below.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🛒 Add filtered to cart…", callback_data="bf_acart"
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Filter menu", callback_data="bf_cancel_prompt"
                        )
                    ],
                ]
            ),
        )
        await query.answer(f"{lbl} set.")
        return

    if data == "bf_gender":
        await query.edit_message_text(
            "⚧️ <b>Gender</b>\n\n"
            "<i>Matches <code>|§g§male</code> / <code>|§g§female</code> on leads synced "
            "from the HTML BIN tool (genderize.io + agify.io age tag <code>|§a§</code>). "
            "Untagged lines do not match.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("♂ Male", callback_data="bfg:male"),
                        InlineKeyboardButton("♀ Female", callback_data="bfg:female"),
                    ],
                    [InlineKeyboardButton("Clear gender", callback_data="bfg:clear")],
                    [
                        InlineKeyboardButton(
                            "⬅️ Filter menu", callback_data="bf_cancel_prompt"
                        )
                    ],
                ]
            ),
        )
        return

    if data == "bf_bank":
        await show_buy_filter_pick(query, context, "bank", 0)
        return

    if data == "bf_city":
        await show_buy_filter_pick(query, context, "city", 0)
        return

    if data == "bf_zip":
        await show_buy_filter_pick(query, context, "zip", 0)
        return

    if data == "bf_state":
        await show_buy_filter_pick(query, context, "state", 0)
        return

    if data == "bf_bin":
        await show_buy_filter_pick(query, context, "bin", 0)
        return

    if data == "bf_cancel_prompt":
        context.user_data.pop("awaiting_cart_filter_qty", None)
        context.user_data.pop("caf_tier", None)
        context.user_data.pop("caf_max", None)
        qty = int(context.user_data.get("buy_qty") or 0)
        flt = context.user_data.get("buy_filters") or {}
        context.user_data["buy_filters_mode"] = True
        await query.edit_message_text(
            buy_filters_grid_text(qty, flt),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_grid_keyboard(),
        )
        return

    if data == "bf_clear":
        context.user_data["buy_filters"] = {}
        qty = int(context.user_data.get("buy_qty") or 0)
        await query.edit_message_text(
            buy_filters_grid_text(qty, {}),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_filters_grid_keyboard(),
        )
        return

    if data == "bf_done":
        await show_buy_tier_pick(query, context, user_id)
        return

    if data == "pur":
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        _admin_clear_sync_await(context)
        _admin_clear_nb_await(context)
        _admin_clear_announce(context)
        _clear_buy_flow(context)
        await query.edit_message_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_quantity_keyboard(),
        )
        return

    if data == "bal":
        await query.edit_message_text(
            account_balance_text(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=account_balance_keyboard(),
        )
        return

    if data == "vip":
        await query.edit_message_text(
            vip_details_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="bal")]]
            ),
        )
        return

    if data == "top":
        await show_topup_menu(query, context, "home", as_reply=True)
        return

    if data == "tub":
        await show_topup_menu(query, context, "bal", as_reply=True)
        return

    if data == "tum":
        tu_back = context.user_data.get("tu_back", "home")
        await show_topup_menu(query, context, tu_back)
        return

    if data == "tu_cancel":
        context.user_data.pop("awaiting_topup_custom", None)
        context.user_data.pop("invoice_usd", None)
        await query.edit_message_text(
            "Top-up cancelled.",
            parse_mode=ParseMode.HTML,
        )
        return

    if data == "tumin":
        await show_payment_methods(query, context, MIN_TOPUP_USD)
        return
    if data in FIXED_TOPUP_PRESETS:
        await show_payment_methods(
            query, context, FIXED_TOPUP_PRESETS[data]
        )
        return

    if data == "tuac":
        context.user_data["awaiting_topup_custom"] = True
        context.user_data.pop("search", None)
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        context.user_data.pop("await_bin_qty_bin", None)
        context.user_data.pop("await_bin_qty_tier", None)
        context.user_data.pop("awaiting_random_qty", None)
        await query.edit_message_text(
            "💰 <b>Custom amount</b>\n\n"
            f"Send the amount in USD (minimum <b>{_min_topup_display()}</b>).\n"
            "Example: <code>75</code> or <code>150.50</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="tum")]]
            ),
        )
        return

    if data in ("pmbtc", "pmltc", "pmeth"):
        cur = {"pmbtc": "btc", "pmltc": "ltc", "pmeth": "eth"}[data]
        amt = float(context.user_data.get("invoice_usd", 0.0))
        context.user_data["invoice_currency"] = cur
        await query.edit_message_text(
            payment_invoice_text(cur, amt),
            parse_mode=ParseMode.HTML,
            reply_markup=payment_invoice_markup(cur),
        )
        return

    if data.startswith("tpsub:"):
        cur = data.split(":", 1)[1] if ":" in data else ""
        if cur not in ("btc", "ltc", "eth"):
            await query.answer("Invalid payment type.", show_alert=True)
            return
        amt = float(context.user_data.get("invoice_usd", 0.0))
        if amt <= 0:
            await query.answer("No amount. Start Top Up again.", show_alert=True)
            return
        u = update.effective_user
        if user_has_open_pending(u.id):
            await query.answer(
                "You already have a pending top-up. Wait for admin.", show_alert=True
            )
            return
        pid = create_pending(u.id, u.username, u.full_name, amt, cur)
        if not pid:
            await query.answer("Could not submit. Try again.", show_alert=True)
            return
        rec = get_pending(pid)
        if not rec:
            await query.answer("Error saving request.", show_alert=True)
            return
        admin_kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Accept top-up", callback_data=f"tp_a:{pid}"
                    ),
                    InlineKeyboardButton("❌ Reject", callback_data=f"tp_r:{pid}"),
                ]
            ]
        )
        admin_txt = format_admin_topup_message(pid, rec)
        for aid in get_admin_ids():
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=admin_txt,
                    parse_mode=ParseMode.HTML,
                    reply_markup=admin_kb,
                )
            except Exception as e:
                logger.warning("Could not notify admin %s: %s", aid, e)
        await query.edit_message_text(
            payment_invoice_text(cur, amt)
            + "\n\n✅ <b>Submitted.</b> An admin will verify and credit your balance.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "⬅️ Payment methods", callback_data="tumpm"
                        )
                    ],
                    [InlineKeyboardButton("🏠 Home", callback_data="home")],
                ]
            ),
        )
        await query.answer("Submitted for review.")
        return

    if data.startswith("tp_a:"):
        pid = data.split(":", 1)[1].strip()
        if update.effective_user.id not in get_admin_ids():
            await query.answer("Admin only.", show_alert=True)
            return
        ok, err, meta = try_accept_topup(pid)
        if not ok or not meta:
            await query.answer(err or "Failed.", show_alert=True)
            return
        rec = meta["rec"]
        uid = int(meta["user_id"])
        amt = float(meta["amount_usd"])
        new_bal = float(meta["new_balance"])
        ensure_user(uid)
        await query.edit_message_text(
            format_admin_topup_message(pid, rec) + "\n\n✅ <b>ACCEPTED</b> — balance credited.",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
        await query.answer("Credited.")
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    f"✅ Top-up approved: <b>${amt:.2f}</b> added to your balance.\n"
                    f"New balance: <b>${new_bal:.2f}</b>"
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("Could not DM user %s: %s", uid, e)
        return

    if data.startswith("tp_r:"):
        pid = data.split(":", 1)[1].strip()
        if update.effective_user.id not in get_admin_ids():
            await query.answer("Admin only.", show_alert=True)
            return
        ok, err, meta = try_reject_topup(pid)
        if not ok or not meta:
            await query.answer(err or "Failed.", show_alert=True)
            return
        rec = meta["rec"]
        uid = int(meta["user_id"])
        await query.edit_message_text(
            format_admin_topup_message(pid, rec) + "\n\n❌ <b>REJECTED</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
        await query.answer("Rejected.")
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=(
                    "Your top-up request was <b>not</b> approved. "
                    "If you already paid, contact support with your tx id."
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("Could not DM user %s: %s", uid, e)
        return

    if data == "tumpm":
        amt = float(context.user_data.get("invoice_usd", 0.0))
        await query.edit_message_text(
            payment_method_text(amt),
            parse_mode=ParseMode.HTML,
            reply_markup=payment_method_keyboard(),
        )
        return

    if data == "cart":
        await query.edit_message_text(
            format_cart_screen(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=cart_screen_markup(user_id),
        )
        return

    if data == "prof":
        u = update.effective_user
        await query.edit_message_text(
            profile_screen_text(u.id, u),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="home")]]
            ),
        )
        return

    if data == "bu_rd":
        await show_random_tier_pick(query, context)
        return

    if data == "rdf":
        context.user_data["rd_tier"] = "first"
        await show_random_qty_pick(query, context)
        return

    if data == "rds":
        context.user_data["rd_tier"] = "second"
        await show_random_qty_pick(query, context)
        return

    if data == "rd_t":
        await show_random_tier_pick(query, context)
        return

    m = re.match(r"^rdn(\d+)$", data)
    if m:
        qty = int(m.group(1))
        rt = context.user_data.get("rd_tier", "first")
        nstock = total_line_count(rt)
        if qty > nstock:
            await query.answer(
                f"Only {nstock} line(s) in stock.",
                show_alert=True,
            )
            return
        context.user_data["rd_qty"] = qty
        await show_random_confirm(query, context, user_id)
        return

    if data == "rdnc":
        context.user_data["awaiting_random_qty"] = True
        await query.edit_message_text(
            "✏️ Send how many random leads you want (whole number).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data="rdcq")]]
            ),
        )
        return

    if data == "rdcq":
        context.user_data.pop("rd_qty", None)
        context.user_data.pop("awaiting_random_qty", None)
        if int(context.user_data.get("buy_qty") or 0) >= 1:
            await show_buy_tier_pick(query, context, user_id)
            return
        if context.user_data.get("rd_tier") not in ("first", "second"):
            await show_random_tier_pick(query, context)
        else:
            await show_random_qty_pick(query, context)
        return

    if data == "rdok":
        tier = context.user_data.get("rd_tier", "first")
        if tier not in ("first", "second"):
            await query.answer("Pick tier again.", show_alert=True)
            return
        qty = int(context.user_data.get("rd_qty") or 0)
        if qty < 1:
            await query.answer("Quantity missing.", show_alert=True)
            return
        unit = random_unit_usd(tier)
        total = round(qty * unit, 2)
        flt_rd = context.user_data.get("buy_filters") or {}
        fk = _active_buy_filter_kwargs(flt_rd)
        has_f = any(v for v in fk.values() if v)
        if has_f:
            n_match = count_matching_lines(tier, **fk)
            if n_match < qty:
                await query.answer(
                    f"Only {n_match} line(s) match your filters in this pile.",
                    show_alert=True,
                )
                return
        else:
            if total_line_count(tier) < qty:
                await query.answer("Not enough stock in that pile.", show_alert=True)
                return
        if get_balance(user_id) + 1e-9 < total:
            await query.answer(
                f"Insufficient balance. Need ${total:.2f}.",
                show_alert=True,
            )
            return
        if has_f:
            pairs = pop_n_random_filtered(qty, tier, **fk)
        else:
            pairs = pop_n_random_any(qty, tier)
        if not pairs or len(pairs) != qty:
            await query.answer("Could not fulfill. Try again.", show_alert=True)
            return
        if not debit_purchase(user_id, total):
            restore_pairs_triples([(b, line, tier) for b, line in pairs])
            await query.answer("Balance update failed.", show_alert=True)
            return
        label_base = "Random firsthand" if tier == "first" else "Random secondhand"
        if has_f:
            label_base += " (filtered)"
        append_purchase_history(user_id, total, len(pairs), label_base)
        context.user_data.pop("rd_tier", None)
        context.user_data.pop("rd_qty", None)
        _clear_buy_flow(context)
        ttl = "Random firsthand" if tier == "first" else "Random secondhand"
        await deliver_purchased_bulk(query, pairs, total, ttl)
        await query.edit_message_text(
            f"✅ <b>Random order complete</b> · <b>${total:.2f}</b> · {qty} line(s).\n"
            "Your file is in the next message.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Home", callback_data="home")]]
            ),
        )
        await query.answer("Done.")
        return

    if data in ("cup_f", "cup_s"):
        context.user_data["awaiting_cup"] = True
        context.user_data["cup_tier"] = "first" if data == "cup_f" else "second"
        context.user_data.pop("search", None)
        pile = "firsthand" if data == "cup_f" else "secondhand"
        await query.edit_message_text(
            f"📝 <b>Custom → cart ({pile})</b>\n\n"
            "Reply with <code>10 403491</code> or <code>403491 10</code>.\n"
            "We add to cart from that pile if stock allows.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Cancel", callback_data="cup_x")]]
            ),
        )
        return

    if data == "cup_x":
        context.user_data.pop("awaiting_cup", None)
        context.user_data.pop("cup_tier", None)
        await query.edit_message_text(
            purchase_intro_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=buy_quantity_keyboard(),
        )
        return

    if data == "ca_ok":
        if not get_cart_entries(user_id):
            await query.answer("Your cart is empty.", show_alert=True)
            return
        ok, msg = cart_fulfillment_ok(user_id)
        if not ok:
            await query.answer(msg[:180], show_alert=True)
            return
        tot = cart_subtotal_usd(user_id)
        if get_balance(user_id) + 1e-9 < tot:
            await query.answer(
                f"Insufficient balance. Need ${tot:.2f}.",
                show_alert=True,
            )
            return
        res = run_cart_checkout(user_id)
        if not res:
            await query.answer(
                "Checkout failed — stock or balance changed.",
                show_alert=True,
            )
            return
        pairs, total = res
        await deliver_purchased_bulk(query, pairs, total, "Cart checkout")
        await query.edit_message_text(
            f"✅ <b>Cart checkout</b> · <b>${total:.2f}</b> · {len(pairs)} line(s).\n"
            "File attached below.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Home", callback_data="home")]]
            ),
        )
        await query.answer("Purchase complete.")
        return

    if data == "ca_cl":
        clear_cart_user(user_id)
        await query.edit_message_text(
            format_cart_screen(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=cart_screen_markup(user_id),
        )
        await query.answer("Cart cleared.")
        return

    m = re.match(r"^t([fs])p(\d+)$", data)
    if m:
        st = "first" if m.group(1) == "f" else "second"
        await show_catalog_page(query, context, int(m.group(2)), st)
        return

    m = re.match(r"^t([fs])x(\d{6})$", data)
    if m:
        tier_f = "first" if m.group(1) == "f" else "second"
        bin6 = m.group(2)
        pfx = "tf" if tier_f == "first" else "ts"
        allowed = set(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
        if bin6 not in allowed:
            await query.answer("Open Browse BINs again.", show_alert=True)
            return
        price = _line_price_for_tier(tier_f)
        n = len(get_lines_for_bin(bin6, tier_f))
        states = state_breakdown_for_bin(bin6, tier=tier_f)
        bal = get_balance(user_id)
        st_line = (
            f"States: {escape(states)}\n"
            if states
            else "States: <i>not parsed / empty</i>\n"
        )
        lp = int(context.user_data.get("b1_last_page", 0))
        tlabel = "Firsthand" if tier_f == "first" else "Secondhand"
        await query.edit_message_text(
            f"💎 <b>BIN</b> <code>{escape(bin6)}</code> · <b>{tlabel}</b>\n"
            f"In this pile: <b>{n}</b>\n"
            f"{st_line}"
            f"Price: <b>${price:.2f}</b> per lead\n"
            f"Your balance: <b>${bal:.2f}</b>\n\n"
            "<i>Add to cart — lines removed from this pile only; BIN stays in catalog.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("+1", callback_data=f"{pfx}k{bin6}:1"),
                        InlineKeyboardButton("+5", callback_data=f"{pfx}k{bin6}:5"),
                        InlineKeyboardButton("+10", callback_data=f"{pfx}k{bin6}:10"),
                    ],
                    [
                        InlineKeyboardButton("+25", callback_data=f"{pfx}k{bin6}:25"),
                        InlineKeyboardButton("Custom", callback_data=f"{pfx}k{bin6}:0"),
                    ],
                    [
                        InlineKeyboardButton("🛒 Cart", callback_data="cart"),
                        InlineKeyboardButton("⬅️ Catalog", callback_data=f"{pfx}p{lp}"),
                    ],
                    [InlineKeyboardButton("🏠 Home", callback_data="home")],
                ]
            ),
        )
        return

    m = re.match(r"^t([fs])k(\d{6}):(\d+)$", data)
    if m:
        tier_f = "first" if m.group(1) == "f" else "second"
        pfx = "tf" if tier_f == "first" else "ts"
        bin6 = m.group(2)
        q = int(m.group(3))
        allowed = set(context.user_data.get("CATALOG_BINS", _catalog_bins_live()))
        if bin6 not in allowed:
            await query.answer("Open Browse BINs again.", show_alert=True)
            return
        if q == 0:
            context.user_data["await_bin_qty_bin"] = bin6
            context.user_data["await_bin_qty_tier"] = tier_f
            await query.edit_message_text(
                f"✏️ How many lines for BIN <code>{escape(bin6)}</code> "
                f"({'1st' if tier_f == 'first' else '2nd'} pile)?\n"
                "Send a number in chat.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Back to BIN",
                                callback_data=f"{pfx}x{bin6}",
                            )
                        ]
                    ]
                ),
            )
            return
        avail = len(get_lines_for_bin(bin6, tier_f))
        if q > avail:
            await query.answer(
                f"Only {avail} in this pile for this BIN.",
                show_alert=True,
            )
            return
        add_to_cart_bin(user_id, bin6, q, tier_f)
        await query.answer(f"Added {q} line(s) to cart.")
        lp = int(context.user_data.get("b1_last_page", 0))
        p = _line_price_for_tier(tier_f)
        await query.edit_message_text(
            "🛒 <b>Added to your cart</b>\n\n"
            f"BIN <code>{escape(bin6)}</code> ({'1st' if tier_f == 'first' else '2nd'}) × <b>{q}</b> @ "
            f"<b>${p:.2f}</b>/line\n"
            "<i>Item added to your cart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🛒 Open cart", callback_data="cart")],
                    [
                        InlineKeyboardButton("⬅️ BIN", callback_data=f"{pfx}x{bin6}"),
                        InlineKeyboardButton("📋 Catalog", callback_data=f"{pfx}p{lp}"),
                    ],
                ]
            ),
        )
        return

    m = re.match(r"^t([fs])sr$", data)
    if m:
        st = "first" if m.group(1) == "f" else "second"
        context.user_data["search"] = (st, "CATALOG_BINS")
        context.user_data["CATALOG_BINS"] = list(_catalog_bins_live())
        lbl = "firsthand" if st == "first" else "secondhand"
        await query.message.reply_text(
            f"🔍 <b>Search {lbl} BINs</b>\n\nEnter digits or part of the BIN.",
            parse_mode=ParseMode.HTML,
        )
        return

    m = re.match(r"^t([fs])rf$", data)
    if m:
        st = "first" if m.group(1) == "f" else "second"
        context.user_data.pop("CATALOG_BINS", None)
        await show_catalog_page(query, context, 0, st)
        return

    logger.warning("Unhandled callback: %s", data)


async def _log_started(app: Application) -> None:
    me = await app.bot.get_me()
    logger.info(
        "Logged in as @%s | build=%s | bot.py=%s",
        me.username,
        BOT_BUILD,
        _BOT_FILE,
    )


async def _register_bot_menu(app: Application) -> None:
    """Populates the blue ▶ Menu button in Telegram (setMyCommands)."""
    default_cmds = [
        BotCommand("start", "Home & main buttons"),
        BotCommand("purchase", "Purchase leads menu"),
        BotCommand(
            "stock", "Browse catalog BINs by Visa / Mastercard / Amex / Discover"
        ),
        BotCommand("request", "Custom lead request"),
        BotCommand("cancel", "Cancel admin / prompts"),
        BotCommand("panel", "Admin: stock, sync, sendout"),
    ]
    admin_cmds = default_cmds + [
        BotCommand("version", "Build info & tips (admin)"),
        BotCommand("admin", "Admin tools (same as panel)"),
        BotCommand("addbin", "Add BIN lines to catalog"),
        BotCommand("clearbin", "Clear BINs & both stock piles"),
        BotCommand("restorefrombak", "Restore catalog+stock from .bak"),
    ]
    try:
        await app.bot.set_my_commands(default_cmds)
        for aid in get_admin_ids():
            try:
                await app.bot.set_my_commands(
                    admin_cmds, scope=BotCommandScopeChat(chat_id=aid)
                )
            except Exception as e:
                logger.warning("set_my_commands admin scope %s: %s", aid, e)
        logger.info("Telegram command menu registered (blue Menu button).")
    except Exception as e:
        logger.warning("set_my_commands failed: %s", e)


async def _post_init(app: Application) -> None:
    await _log_started(app)
    await _register_bot_menu(app)


def _run_telegram_polling(token: str) -> None:
    app = (
        Application.builder()
        .token(token)
        .post_init(_post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stock", stock_cmd))
    app.add_handler(CommandHandler("purchase", purchase_cmd))
    app.add_handler(CommandHandler("version", version_cmd))
    app.add_handler(CommandHandler("clearbin", clearbin_cmd))
    app.add_handler(CommandHandler("addbin", addbin_cmd))
    app.add_handler(CommandHandler("request", request_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("panel", admin_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("restorefrombak", restorefrombak_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, on_text)
    )
    app.add_handler(TypeHandler(Update, touch_user_record), group=-1)

    async def _errors(_update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        if isinstance(err, Conflict):
            logger.error(
                "Telegram Conflict: this bot TOKEN is being polled from two places at once "
                "(second window, another PC, Replit/Railway, etc.). "
                "Stop every other copy, wait ~30s, then start again — or revoke the token in @BotFather "
                "and put a new one in .env if you do not know what else is running."
            )
            # On Railway, HTTP stays "healthy" while Telegram is dead — exit so the platform restarts
            # and logs show the failure. Only one process may poll this token.
            if os.environ.get("PORT", "").strip():
                os._exit(1)
            return
        logger.error("Handler error", exc_info=err)

    app.add_error_handler(_errors)

    logger.info("Starting poll (Ctrl+C to stop)…")
    # Worker thread (Railway: Telegram beside Waitress on main) cannot use asyncio signal handlers.
    poll_kw: dict = {
        "allowed_updates": Update.ALL_TYPES,
        "drop_pending_updates": False,
    }
    if threading.current_thread() is not threading.main_thread():
        poll_kw["stop_signals"] = None
    app.run_polling(**poll_kw)


def _telegram_thread_main(token: str) -> None:
    """Railway: if this thread crashes, exit the whole container (Waitress alone = 'online' but no replies)."""
    try:
        _run_telegram_polling(token)
    except BaseException:
        logger.exception("Telegram poller exited with an error")
        if os.environ.get("PORT", "").strip():
            os._exit(1)
        raise


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit(
            f"Set TELEGRAM_BOT_TOKEN in {_ROOT / '.env'} "
            "(or export it). Token not found — check you run from any folder: "
            "this bot loads .env next to bot.py."
        )

    _acquire_single_instance()

    _bin_html = _ROOT / "deepseek_html_20260320_822fcc.html"
    env_port = os.environ.get("PORT", "").strip()

    if env_port:
        # Railway expects the container process to listen on $PORT on the main thread.
        try:
            from web_upload import run_public_http_forever

            tg = threading.Thread(
                target=_telegram_thread_main,
                args=(token,),
                name="telegram-bot",
                daemon=False,
            )
            tg.start()
            run_public_http_forever(_bin_html)
        except Exception as e:
            logger.exception("Railway HTTP server failed: %s", e)
            raise
        return

    try:
        from web_upload import start_upload_server_background

        start_upload_server_background(_bin_html)
    except Exception as e:
        logger.warning("Web upload server not started: %s", e)

    _run_telegram_polling(token)


if __name__ == "__main__":
    main()
