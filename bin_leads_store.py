"""Tiered raw lines per BIN: first vs second pile. Synced from web sorter."""

from __future__ import annotations

import json
import logging
import random
import shutil
from collections import defaultdict
from pathlib import Path

from data_paths import data_dir

logger = logging.getLogger(__name__)

LEADS_PATH = data_dir() / "bin_leads.json"


def _backup_sidecar_if_nonempty(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("not an object")
    except (json.JSONDecodeError, OSError, ValueError):
        logger.warning("Skip .bak backup — %s is missing or not valid JSON", path)
        return
    try:
        shutil.copy2(path, path.with_name(path.name + ".bak"))
    except OSError as e:
        logger.warning("Could not backup %s: %s", path, e)

# Fixed second-tier retail (first tier uses catalog price_per_bin, default 0.90)
SECONDHAND_PRICE_USD = 0.35


def _norm_bin(key: str) -> str | None:
    d = "".join(c for c in str(key) if c.isdigit())[:6]
    return d if len(d) == 6 else None


def card_brand_from_bin6(bin6: str) -> str | None:
    """
    Map a 6-digit BIN to card network for filtering.
    Returns lowercase: visa | mastercard | amex | discover, or None if unrecognized.
    """
    nb = _norm_bin(bin6)
    if not nb:
        return None
    if nb[0] == "4":
        return "visa"
    first2 = int(nb[:2])
    first3 = int(nb[:3])
    first4 = int(nb[:4])
    first6 = int(nb)
    if first2 in (34, 37):
        return "amex"
    if 51 <= first2 <= 55:
        return "mastercard"
    if 222100 <= first6 <= 272099:
        return "mastercard"
    if first4 == 6011:
        return "discover"
    if 622126 <= first6 <= 622925:
        return "discover"
    if 644 <= first3 <= 649:
        return "discover"
    if first2 == 65:
        return "discover"
    return None


def norm_stock_tier(t: str) -> str:
    s = str(t).strip().lower()
    if s in ("second", "2", "secondhand", "sh"):
        return "second"
    return "first"


def _tier_dict_normalize(obj) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    if not isinstance(obj, dict):
        return out
    for k, v in obj.items():
        nb = _norm_bin(k)
        if not nb:
            continue
        if isinstance(v, list):
            out[nb] = [str(x).strip() for x in v if str(x).strip()]
        elif isinstance(v, str) and v.strip():
            out[nb] = [v.strip()]
    return out


def _parse_file_raw(raw: dict) -> dict[str, dict[str, list[str]]]:
    """v2: {first:{bin:[]}, second:{}}  v1: {bin:[]} -> all first."""
    if not isinstance(raw, dict):
        return {"first": {}, "second": {}}
    if "first" in raw or "second" in raw:
        return {
            "first": _tier_dict_normalize(raw.get("first")),
            "second": _tier_dict_normalize(raw.get("second")),
        }
    return {"first": _tier_dict_normalize(raw), "second": {}}


def load_all_tiers() -> dict[str, dict[str, list[str]]]:
    LEADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not LEADS_PATH.is_file():
        payload = {"first": {}, "second": {}}
        LEADS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return payload
    bak = LEADS_PATH.with_name(LEADS_PATH.name + ".bak")
    raw = None
    for path in (LEADS_PATH, bak):
        if not path.is_file():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if path is bak:
                logger.warning(
                    "bin_leads.json was unreadable — loaded from bin_leads.json.bak"
                )
            break
        except (json.JSONDecodeError, OSError) as e:
            raw = None
            if path is LEADS_PATH:
                logger.error("bin_leads.json invalid (%s); trying .bak if present", e)
    if raw is None:
        logger.error(
            "bin_leads.json and .bak missing or corrupt — treating as empty piles "
            "(restore from backup if needed)"
        )
        return {"first": {}, "second": {}}
    data = _parse_file_raw(raw)
    # Persist migration from v1 → v2 once
    if raw and "first" not in raw and "second" not in raw:
        save_all_tiers(data)
    return data


def save_all_tiers(data: dict[str, dict[str, list[str]]]) -> None:
    LEADS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "first": {k: v for k, v in data.get("first", {}).items() if v},
        "second": {k: v for k, v in data.get("second", {}).items() if v},
    }
    _backup_sidecar_if_nonempty(LEADS_PATH)
    LEADS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_leads(tier: str = "first") -> dict[str, list[str]]:
    t = norm_stock_tier(tier)
    return dict(load_all_tiers().get(t, {}))


def clear_bin_leads() -> None:
    save_all_tiers({"first": {}, "second": {}})


def merge_groups_from_web(groups: dict, tier: str = "first") -> dict:
    """
    Merge groups into first or second pile + catalog BIN list.
    tier: 'first' | 'second'
    """
    from catalog_store import merge_bins_to_catalog

    t = norm_stock_tier(tier)
    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    bin_keys: list[str] = []
    lines_added = 0

    for key, raw_lines in groups.items():
        nb = _norm_bin(key)
        if not nb:
            continue
        bin_keys.append(nb)
        if not isinstance(raw_lines, list):
            continue
        if nb not in data:
            data[nb] = []
        seen = set(data[nb])
        for line in raw_lines:
            s = str(line).strip()
            if not s or s in seen:
                continue
            data[nb].append(s)
            seen.add(s)
            lines_added += 1

    all_t[t] = data
    save_all_tiers(all_t)
    merge_bins_to_catalog(bin_keys)
    return {
        "tier": t,
        "bins_touched": len(set(bin_keys)),
        "lines_added": lines_added,
        "total_bins_with_data": len(data),
    }


def get_lines_for_bin(bin6: str, tier: str = "first") -> list[str]:
    nb = _norm_bin(bin6)
    if not nb:
        return []
    return list(load_leads(tier).get(nb, []))


def state_from_line(line: str) -> str:
    parts = line.split("|")
    if len(parts) <= 7:
        return ""
    s = parts[7].strip().strip('"').strip()
    return s.upper() if s else ""


# Appended by BIN web tool on sync so Telegram can filter by issuer (binlist.net).
META_BANK_SUFFIX = "|§b§"
# Appended after bank tag: first-name gender inference (genderize.io).
META_GENDER_SUFFIX = "|§g§"
# Appended after gender tag: first-name predicted age (agify.io).
META_AGE_SUFFIX = "|§a§"


def strip_lead_sync_suffix(line: str) -> str:
    """Strip trailing `|§b§` / `|§g§` / `|§a§` metadata added on HTML sync."""
    s = str(line).strip()
    if META_AGE_SUFFIX in s:
        s = s.rsplit(META_AGE_SUFFIX, 1)[0].rstrip()
    if META_GENDER_SUFFIX in s:
        s = s.rsplit(META_GENDER_SUFFIX, 1)[0].rstrip()
    if META_BANK_SUFFIX in s:
        s = s.rsplit(META_BANK_SUFFIX, 1)[0].rstrip()
    return s


def bank_from_line(line: str) -> str:
    """Issuer name if line was synced from HTML with bank tag; else empty."""
    s = str(line).strip()
    if META_BANK_SUFFIX not in s:
        return ""
    after = s.rsplit(META_BANK_SUFFIX, 1)[-1]
    if META_AGE_SUFFIX in after:
        after = after.split(META_AGE_SUFFIX, 1)[0].strip()
    if META_GENDER_SUFFIX in after:
        after = after.split(META_GENDER_SUFFIX, 1)[0].strip()
    return after.strip('"').strip()


def gender_from_line(line: str) -> str:
    """male | female | unknown from HTML sync tag; empty if not tagged."""
    s = str(line).strip()
    if META_GENDER_SUFFIX not in s:
        return ""
    mid = s.split(META_GENDER_SUFFIX, 1)[1]
    if META_AGE_SUFFIX in mid:
        mid = mid.split(META_AGE_SUFFIX, 1)[0].strip()
    g = mid.strip().lower()
    if g in ("male", "female", "unknown"):
        return g
    return ""


def age_from_line(line: str) -> int | None:
    """Predicted age from agify sync tag; None if missing or unknown."""
    s = str(line).strip()
    if META_AGE_SUFFIX not in s:
        return None
    tail = s.rsplit(META_AGE_SUFFIX, 1)[-1].strip().lower()
    if not tail or tail == "unknown":
        return None
    try:
        a = int(tail)
        if 0 <= a <= 120:
            return a
    except ValueError:
        pass
    return None


def city_from_line(line: str) -> str:
    parts = str(line).strip().split("|")
    if len(parts) <= 6:
        return ""
    return parts[6].strip().strip('"').strip()


def zip_from_line(line: str) -> str:
    """ZIP / postal field when present (pipe index 8: ...|city|state|zip|...)."""
    parts = str(line).strip().split("|")
    if len(parts) <= 8:
        return ""
    return parts[8].strip().strip('"').strip()


def _zip_digits(s: str) -> str:
    return "".join(c for c in str(s).strip() if c.isdigit())


def _zip_bucket_key(raw_zip: str) -> str:
    """Normalize ZIP for picklist keys and matching (US ZIP+4 → first 5 digits)."""
    d = _zip_digits(raw_zip)
    if len(d) >= 5:
        return d[:5]
    if len(d) >= 3:
        return d
    return ""


def _line_zip_matches(filter_key: str, line: str) -> bool:
    fk = _zip_bucket_key(filter_key)
    if not fk:
        return False
    ld = _zip_digits(zip_from_line(line))
    if not ld:
        return False
    return ld.startswith(fk) or fk.startswith(ld) or fk == ld


def norm_zip_filter_value(raw: object) -> str | None:
    """Normalize user/picklist ZIP filter; None if unusable."""
    if raw is None:
        return None
    z = _zip_bucket_key(str(raw).strip())
    return z if z else None


def _norm_match_token(s: str) -> str:
    return " ".join(str(s).split()).casefold()


def _aggregate_bank_state_city_zip() -> tuple[
    list[tuple[str, dict[str, int]]],
    list[tuple[str, dict[str, int]]],
    list[tuple[str, dict[str, int]]],
    list[tuple[str, dict[str, int]]],
]:
    banks: dict[str, dict[str, int]] = defaultdict(lambda: {"first": 0, "second": 0})
    states: dict[str, dict[str, int]] = defaultdict(lambda: {"first": 0, "second": 0})
    cities: dict[str, dict[str, int]] = defaultdict(lambda: {"first": 0, "second": 0})
    zips: dict[str, dict[str, int]] = defaultdict(lambda: {"first": 0, "second": 0})
    for tier in ("first", "second"):
        pile = load_leads(tier)
        for lines in pile.values():
            for line in lines:
                bk = bank_from_line(line)
                if bk:
                    banks[bk][tier] += 1
                st = state_from_line(line)
                if st:
                    states[st][tier] += 1
                ct = city_from_line(line)
                if ct:
                    cities[ct][tier] += 1
                zk = _zip_bucket_key(zip_from_line(line))
                if zk:
                    zips[zk][tier] += 1

    def sort_items(d: dict[str, dict[str, int]]) -> list[tuple[str, dict[str, int]]]:
        items = [(k, dict(v)) for k, v in d.items()]
        items.sort(
            key=lambda x: (-(x[1]["first"] + x[1]["second"]), x[0].casefold())
        )
        return items

    return (
        sort_items(banks),
        sort_items(states),
        sort_items(cities),
        sort_items(zips),
    )


def filter_pick_bins_merged() -> list[tuple[str, dict[str, int]]]:
    cf = bin_line_counts("first")
    cs = bin_line_counts("second")
    out: list[tuple[str, dict[str, int]]] = []
    for b in sorted(set(cf) | set(cs), key=lambda x: (-(cf.get(x, 0) + cs.get(x, 0)), x)):
        fc, sc = cf.get(b, 0), cs.get(b, 0)
        if fc + sc > 0:
            out.append((b, {"first": fc, "second": sc}))
    return out


def filter_dimension_picklists(
    *,
    max_cities: int = 200,
    max_zips: int = 200,
) -> dict[str, list[tuple[str, dict[str, int]]]]:
    b, s, c, z = _aggregate_bank_state_city_zip()
    return {
        "bank": b,
        "state": s,
        "city": c[:max_cities],
        "zip": z[:max_zips],
        "bin": filter_pick_bins_merged(),
    }


def total_line_count(tier: str | None = None) -> int:
    if tier is None:
        return total_line_count("first") + total_line_count("second")
    return sum(len(v) for v in load_leads(tier).values())


def bin_line_counts(tier: str = "first") -> dict[str, int]:
    return {b: len(lines) for b, lines in load_leads(tier).items()}


def state_breakdown_for_bin(bin6: str, max_states: int = 6, *, tier: str = "first") -> str:
    lines = get_lines_for_bin(bin6, tier)
    hist: dict[str, int] = {}
    for ln in lines:
        st = state_from_line(ln)
        if st:
            hist[st] = hist.get(st, 0) + 1
    if not hist:
        return ""
    parts = sorted(hist.items(), key=lambda x: (-x[1], x[0]))[:max_states]
    return ", ".join(f"{s}×{c}" for s, c in parts)


def states_compact_for_bin(bin6: str, *, tier: str = "first") -> str:
    lines = get_lines_for_bin(bin6, tier)
    uniq: list[str] = []
    seen: set[str] = set()
    for ln in lines:
        st = state_from_line(ln)
        if st and st not in seen:
            seen.add(st)
            uniq.append(st)
    if not uniq:
        return "—"
    n_distinct = len({state_from_line(l) for l in lines if state_from_line(l)})
    if len(uniq) == 1:
        return uniq[0][:10]
    more = "+" if n_distinct > 2 else ""
    a, b = uniq[0][:5], uniq[1][:5]
    return f"{a}|{b}{more}"[:14]


def _remove_one_line(
    data: dict[str, list[str]], bin6: str, line: str
) -> None:
    lines = data.get(bin6, [])
    try:
        idx = lines.index(line)
    except ValueError:
        return
    lines.pop(idx)
    if not lines:
        del data[bin6]
    else:
        data[bin6] = lines


def restore_pairs_triples(pairs: list[tuple[str, str, str]]) -> None:
    """(bin, line, tier)"""
    if not pairs:
        return
    all_t = load_all_tiers()
    for b, line, tier in pairs:
        k = norm_stock_tier(tier)
        nb = _norm_bin(b)
        if not nb:
            continue
        s = str(line).strip()
        if not s:
            continue
        slot = all_t.setdefault(k, {})
        slot.setdefault(nb, []).append(s)
    save_all_tiers(all_t)


def pop_n_random_from_bin(
    bin6: str, n: int, tier: str = "first"
) -> list[tuple[str, str]] | None:
    t = norm_stock_tier(tier)
    nb = _norm_bin(bin6)
    if not nb or n < 1:
        return None
    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    lines = list(data.get(nb, []))
    if len(lines) < n:
        return None
    picks = random.sample(lines, n)
    for line in picks:
        _remove_one_line(data, nb, line)
    all_t[t] = data
    save_all_tiers(all_t)
    return [(nb, line) for line in picks]


def pop_n_random_any(n: int, tier: str = "first") -> list[tuple[str, str]] | None:
    t = norm_stock_tier(tier)
    if n < 1:
        return None
    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    pool: list[tuple[str, str]] = []
    for b, lines in data.items():
        for line in lines:
            pool.append((b, line))
    if len(pool) < n:
        return None
    chosen = random.sample(pool, n)
    for b, line in chosen:
        _remove_one_line(data, b, line)
    all_t[t] = data
    save_all_tiers(all_t)
    return chosen


def _line_matches_filters(
    bin_key: str,
    line: str,
    *,
    state: str | None = None,
    bin6: str | None = None,
    city: str | None = None,
    bank: str | None = None,
    brand: str | None = None,
    zip_code: str | None = None,
    gender: str | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
) -> bool:
    if bin6:
        nb = _norm_bin(bin6)
        if nb and _norm_bin(bin_key) != nb:
            return False
    if state:
        st = state.strip().upper()
        if st and st != "ALL":
            ls = state_from_line(line)
            if ls != st:
                return False
    if city:
        fc = _norm_match_token(city)
        if fc and fc != "all":
            lc = _norm_match_token(city_from_line(line))
            if not lc:
                return False
            if fc not in lc and lc not in fc:
                return False
    if bank:
        fb = _norm_match_token(bank)
        if fb and fb != "all":
            lb = _norm_match_token(bank_from_line(line))
            if not lb:
                return False
            if fb not in lb and lb not in fb:
                return False
    if brand:
        fb = str(brand).strip().lower()
        if fb and fb != "all":
            lb = card_brand_from_bin6(bin_key)
            if lb != fb:
                return False
    if zip_code:
        zf = str(zip_code).strip()
        if zf and zf.upper() != "ALL":
            if not _line_zip_matches(zf, line):
                return False
    if gender:
        fg = str(gender).strip().lower()
        if fg and fg != "all":
            lg = gender_from_line(line)
            if lg != fg:
                return False
    if age_min is not None or age_max is not None:
        la = age_from_line(line)
        if la is None:
            return False
        lo = int(age_min) if age_min is not None else 0
        hi = int(age_max) if age_max is not None else 120
        if la < lo or la > hi:
            return False
    return True


def pop_n_random_filtered(
    n: int,
    tier: str = "first",
    *,
    state: str | None = None,
    bin6: str | None = None,
    city: str | None = None,
    bank: str | None = None,
    brand: str | None = None,
    zip_code: str | None = None,
    gender: str | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
) -> list[tuple[str, str]] | None:
    """Random draw from tier pile, optionally filtered by state, BIN, city, bank, brand, ZIP, gender, age tag."""
    t = norm_stock_tier(tier)
    if n < 1:
        return None
    st_clean = (state or "").strip().upper() or None
    if st_clean == "ALL":
        st_clean = None
    nb_filter = _norm_bin(bin6) if bin6 else None
    ct_clean = (city or "").strip() or None
    if ct_clean and ct_clean.upper() == "ALL":
        ct_clean = None
    bk_clean = (bank or "").strip() or None
    if bk_clean and bk_clean.upper() == "ALL":
        bk_clean = None
    br_clean = (brand or "").strip().lower() or None
    if br_clean and br_clean.upper() == "ALL":
        br_clean = None
    zip_clean = (zip_code or "").strip() or None
    if zip_clean and zip_clean.upper() == "ALL":
        zip_clean = None
    gen_clean = (gender or "").strip().lower() or None
    if gen_clean and gen_clean in ("all", "any"):
        gen_clean = None
    amin = age_min
    amax = age_max
    if amin is not None:
        try:
            amin = int(amin)
        except (TypeError, ValueError):
            amin = None
    if amax is not None:
        try:
            amax = int(amax)
        except (TypeError, ValueError):
            amax = None

    all_t = load_all_tiers()
    data = dict(all_t.get(t, {}))
    pool: list[tuple[str, str]] = []
    for b, lines in data.items():
        for line in lines:
            if _line_matches_filters(
                b,
                line,
                state=st_clean,
                bin6=nb_filter,
                city=ct_clean,
                bank=bk_clean,
                brand=br_clean,
                zip_code=zip_clean,
                gender=gen_clean,
                age_min=amin,
                age_max=amax,
            ):
                pool.append((b, line))
    if len(pool) < n:
        return None
    chosen = random.sample(pool, n)
    for b, line in chosen:
        _remove_one_line(data, b, line)
    all_t[t] = data
    save_all_tiers(all_t)
    return chosen


def count_matching_lines(
    tier: str,
    *,
    state: str | None = None,
    bin6: str | None = None,
    city: str | None = None,
    bank: str | None = None,
    brand: str | None = None,
    zip_code: str | None = None,
    gender: str | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
) -> int:
    t = norm_stock_tier(tier)
    st_clean = (state or "").strip().upper() or None
    if st_clean == "ALL":
        st_clean = None
    nb_filter = _norm_bin(bin6) if bin6 else None
    ct_clean = (city or "").strip() or None
    if ct_clean and ct_clean.upper() == "ALL":
        ct_clean = None
    bk_clean = (bank or "").strip() or None
    if bk_clean and bk_clean.upper() == "ALL":
        bk_clean = None
    br_clean = (brand or "").strip().lower() or None
    if br_clean and br_clean.upper() == "ALL":
        br_clean = None
    zip_clean = (zip_code or "").strip() or None
    if zip_clean and zip_clean.upper() == "ALL":
        zip_clean = None
    gen_clean = (gender or "").strip().lower() or None
    if gen_clean and gen_clean in ("all", "any"):
        gen_clean = None
    amin = age_min
    amax = age_max
    if amin is not None:
        try:
            amin = int(amin)
        except (TypeError, ValueError):
            amin = None
    if amax is not None:
        try:
            amax = int(amax)
        except (TypeError, ValueError):
            amax = None
    data = dict(load_all_tiers().get(t, {}))
    n = 0
    for b, lines in data.items():
        for line in lines:
            if _line_matches_filters(
                b,
                line,
                state=st_clean,
                bin6=nb_filter,
                city=ct_clean,
                bank=bk_clean,
                brand=br_clean,
                zip_code=zip_clean,
                gender=gen_clean,
                age_min=amin,
                age_max=amax,
            ):
                n += 1
    return n


def matching_age_stats_combined(
    *,
    state: str | None = None,
    bin6: str | None = None,
    city: str | None = None,
    bank: str | None = None,
    brand: str | None = None,
    zip_code: str | None = None,
    gender: str | None = None,
    age_min: int | None = None,
    age_max: int | None = None,
) -> tuple[int, int, int | None, int | None, float | None]:
    """Across first+second piles: total matching lines, count with age tag, min, max, avg age."""
    st_clean = (state or "").strip().upper() or None
    if st_clean == "ALL":
        st_clean = None
    nb_filter = _norm_bin(bin6) if bin6 else None
    ct_clean = (city or "").strip() or None
    if ct_clean and ct_clean.upper() == "ALL":
        ct_clean = None
    bk_clean = (bank or "").strip() or None
    if bk_clean and bk_clean.upper() == "ALL":
        bk_clean = None
    br_clean = (brand or "").strip().lower() or None
    if br_clean and br_clean.upper() == "ALL":
        br_clean = None
    zip_clean = (zip_code or "").strip() or None
    if zip_clean and zip_clean.upper() == "ALL":
        zip_clean = None
    gen_clean = (gender or "").strip().lower() or None
    if gen_clean and gen_clean in ("all", "any"):
        gen_clean = None
    amin = age_min
    amax = age_max
    if amin is not None:
        try:
            amin = int(amin)
        except (TypeError, ValueError):
            amin = None
    if amax is not None:
        try:
            amax = int(amax)
        except (TypeError, ValueError):
            amax = None
    ages: list[int] = []
    total = 0
    for tier in ("first", "second"):
        data = dict(load_all_tiers().get(tier, {}))
        for b, lines in data.items():
            for line in lines:
                if _line_matches_filters(
                    b,
                    line,
                    state=st_clean,
                    bin6=nb_filter,
                    city=ct_clean,
                    bank=bk_clean,
                    brand=br_clean,
                    zip_code=zip_clean,
                    gender=gen_clean,
                    age_min=amin,
                    age_max=amax,
                ):
                    total += 1
                    a = age_from_line(line)
                    if a is not None:
                        ages.append(a)
    tagged = len(ages)
    if tagged == 0:
        return total, 0, None, None, None
    return total, tagged, min(ages), max(ages), sum(ages) / tagged


def format_notebook_text(bin6: str, lines: list[str]) -> str:
    nb = _norm_bin(bin6) or bin6
    n = len(lines)
    header = "══════════════════════════════════════════════════\n"
    header += f"  BIN: {nb}  |  total entries: {n}\n"
    header += "══════════════════════════════════════════════════\n\n"
    body = "\n".join(
        f"[{i + 1}] {strip_lead_sync_suffix(line)}" for i, line in enumerate(lines)
    )
    body += "\n\n—— end of group ——"
    return header + body


def format_sendout_tiers_block() -> str:
    """Telegram sendout: catalog BINs with per-tier counts."""
    from catalog_store import load_catalog

    cat = load_catalog()
    first_p = float(cat.get("price_per_bin", 0.90))
    bins: list[str] = cat.get("bins", [])
    cf = bin_line_counts("first")
    cs = bin_line_counts("second")
    lines = [
        "📤 BIN SENDOUT (two piles)",
        f"Firsthand: ${first_p:.2f}/lead · Secondhand: ${SECONDHAND_PRICE_USD:.2f}/lead",
        "",
    ]
    if not bins:
        lines.append("(no BINs in catalog)")
        return "\n".join(lines)

    lines.append("━━ FIRSTHAND ━━")
    for b in bins:
        n = cf.get(b, 0)
        if n:
            lines.append(f"  {b}  ×{n}")
    if not any(cf.get(b, 0) for b in bins):
        lines.append("  (no firsthand lines)")

    lines.append("")
    lines.append("━━ SECONDHAND ━━")
    for b in bins:
        n = cs.get(b, 0)
        if n:
            lines.append(f"  {b}  ×{n}")
    if not any(cs.get(b, 0) for b in bins):
        lines.append("  (no secondhand lines)")

    lines.append(sendout_brand_breakdown_text(cat_bins=bins, cf=cf, cs=cs))
    return "\n".join(lines)


def sendout_brand_breakdown_text(
    *,
    cat_bins: list[str],
    cf: dict[str, int] | None = None,
    cs: dict[str, int] | None = None,
) -> str:
    """
    Appended to sendouts: per-brand list of catalog BINs with line counts
    (so e.g. Amex shows every Amex BIN currently in stock).
    """
    cf = cf if cf is not None else bin_line_counts("first")
    cs = cs if cs is not None else bin_line_counts("second")
    catalog = {str(b).strip() for b in cat_bins if str(b).strip()}
    order = (
        ("visa", "Visa"),
        ("mastercard", "Mastercard"),
        ("amex", "Amex"),
        ("discover", "Discover"),
    )
    by_brand: dict[str, list[tuple[str, int, int]]] = {k: [] for k, _ in order}
    other: list[tuple[str, int, int]] = []
    for b in sorted(catalog):
        if len(b) != 6 or not b.isdigit():
            continue
        n1, n2 = cf.get(b, 0), cs.get(b, 0)
        if n1 + n2 < 1:
            continue
        br = card_brand_from_bin6(b)
        if br and br in by_brand:
            by_brand[br].append((b, n1, n2))
        else:
            other.append((b, n1, n2))
    out: list[str] = [
        "",
        "━━ BY CARD BRAND (catalog BINs in stock) ━━",
        "Find Amex / Visa / MC / Discover — BIN × total lines (1st · 2nd).",
        "",
    ]
    for key, title in order:
        rows = by_brand[key]
        out.append(f"▸ {title}")
        if not rows:
            out.append("   (none)")
        else:
            for bin6, n1, n2 in rows:
                out.append(f"   {bin6}  ×{n1 + n2}  (1st {n1} · 2nd {n2})")
        out.append("")
    if other:
        out.append("▸ Other / unrecognized BIN range")
        for bin6, n1, n2 in other:
            out.append(f"   {bin6}  ×{n1 + n2}  (1st {n1} · 2nd {n2})")
        out.append("")
    return "\n".join(out).rstrip()


def stock_tiers_api_payload() -> dict:
    from catalog_store import load_catalog

    cat = load_catalog()
    first_p = float(cat.get("price_per_bin", 0.90))
    bins = cat.get("bins", [])
    cf = bin_line_counts("first")
    cs = bin_line_counts("second")

    def chips(counts: dict[str, int]) -> list[dict]:
        out = []
        for b in bins:
            c = counts.get(b, 0)
            if c:
                out.append({"bin": b, "count": c})
        for b in sorted(counts.keys()):
            if b not in bins and counts[b]:
                out.append({"bin": b, "count": counts[b]})
        return out

    return {
        "first": {
            "price": first_p,
            "total_lines": total_line_count("first"),
            "bins": chips(cf),
        },
        "second": {
            "price": SECONDHAND_PRICE_USD,
            "total_lines": total_line_count("second"),
            "bins": chips(cs),
        },
        "catalog_bins": bins,
    }


def extract_bin_from_line(line: str) -> str | None:
    """First 6 digits of card field (before first |), same rules as the web BIN tool."""
    s = line.strip()
    if not s:
        return None
    first_pipe = s.find("|")
    if first_pipe == -1:
        return None
    card_raw = s[:first_pipe].strip().strip('"').strip()
    digits_only = "".join(c for c in card_raw if c.isdigit())
    if len(digits_only) < 6:
        return None
    return digits_only[:6]


def groups_from_raw_paste(text: str) -> dict[str, list[str]]:
    """Group non-empty lines by BIN for merge_groups_from_web."""
    groups: dict[str, list[str]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        b = extract_bin_from_line(line)
        if not b:
            continue
        groups.setdefault(b, []).append(line)
    return groups


def try_restore_leads_from_bak() -> tuple[bool, str]:
    """Copy last good stock from bin_leads.json.bak over bin_leads.json (admin recovery)."""
    bak = LEADS_PATH.with_name(LEADS_PATH.name + ".bak")
    if not bak.is_file():
        return False, "No bin_leads.json.bak on disk (backup is created on each successful save)."
    try:
        raw = json.loads(bak.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        return False, f"bin_leads.json.bak unreadable: {e}"
    if not isinstance(raw, dict):
        return False, "bin_leads.json.bak is not a JSON object."
    data = _parse_file_raw(raw)
    save_all_tiers(data)
    return True, "Restored bin_leads.json from bin_leads.json.bak."
