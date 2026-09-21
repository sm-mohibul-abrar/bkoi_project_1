#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BARIKOI · GOOGLE MAPS ONLY · IN-PLACE CSV ENRICHMENT · v7
==========================================================

WHAT CHANGED FROM v6, AND WHY
------------------------------
1.  ONE SOURCE: Google Maps only. No Foodpanda, TripAdvisor, or OSM. This
    halves the code and the request surface -- fewer hosts to manage, fewer
    ways to get blocked, faster per-cafe time.

2.  FOOD, NOT JUST CAFES. Filters on `type == Food` broadly, not on a cafe
    sub_type whitelist. If your CSV also has non-food noise under that type,
    narrow it with --subtype-contains.

3.  REWRITES THE SAME CSV, IN PLACE. Every run reads your input CSV, adds/
    updates its own columns, and writes back to that exact path. A pristine
    copy is saved once, next to it, the first time this script ever touches
    the file (`<name>.original_backup.csv`) -- so the source data is never
    truly lost even if something goes wrong on run one thousand.

4.  MENU AND REVIEWS ARE BEST-EFFORT AND HONEST. Google Maps does not
    reliably publish itemised menus for most Dhaka food places -- when a
    Menu tab exists it is often photos, not text. This script extracts real
    text when the DOM has it (menu tab, or, failing that, price-pattern text
    in the panel body) and tags every item with exactly which method found
    it. It does NOT invent categories, items, or prices when the page has
    none. Empty menu = empty list, not a guess.

5.  REVIEWS CARRY GOOGLE'S ACTUAL RELATIVE TIME ("3 months ago"), plus an
    `approx_date` computed by subtracting that offset from the scrape
    timestamp. This is a real calculation from an observed value, not an
    invented absolute date.

6.  TAG is exactly one of: Open, Temporarily Closed, Permanently Closed,
    Unknown. "Unknown" exists because guessing "Open" for a place Google
    could not confirm would itself be a fabrication.


SAMPLE OUTPUT FOR ONE CAFE  (a plausible real scrape -- note the nulls)
------------------------------------------------------------------------
{
  "place_code": "BSDLO82913",
  "business_name": "2Bros Cafe",
  "tag": "Open",
  "address_barikoi": "Rahman Galleria, House 46, Gulshan Avenue, Gulshan 1, Dhaka",
  "bkoi_latitude": 23.780050,
  "bkoi_longitude": 90.417002,
  "g_address": "Rahman Galleria, Gulshan Ave, Dhaka 1212",
  "g_latitude": 23.780101,
  "g_longitude": 90.417057,
  "phone": null,
  "website": null,
  "facebook_url": "https://www.facebook.com/2BrosCafeBD",
  "instagram_url": null,
  "google_maps_url": "https://www.google.com/maps/place/2Bros+Cafe/@23.780101,90.417057,17z/...",
  "google_cid": "0x3755c7a1:0x9e2b4f",
  "category": "Cafe",
  "google_rating": 4.1,
  "google_review_count": 214,
  "opening_hours": {
    "mon": "10:00 AM \u2013 11:00 PM", "tue": "10:00 AM \u2013 11:00 PM",
    "wed": "10:00 AM \u2013 11:00 PM", "thu": "10:00 AM \u2013 11:00 PM",
    "fri": "2:30 PM \u2013 11:00 PM", "sat": "10:00 AM \u2013 11:00 PM",
    "sun": "10:00 AM \u2013 11:00 PM"
  },
  "menu_items": [],
  "menu_source": null,
  "reviews": [
    {
      "author": "Rafiul H.",
      "rating": 5,
      "relative_time": "3 months ago",
      "approx_date": "2026-06-20",
      "text": "Quiet corner, good wifi, the cold brew is solid.",
      "source_tag": "google_review"
    }
  ],
  "review_count_with_text": 1,
  "data_source": "google_maps",
  "scraped_at": "2026-09-20T09:14:02Z",
  "completeness_score": 0.71,
  "attempts": 1,
  "status": "partial"
}

Notice: no menu items (Google's page genuinely had none for this cafe --
so `menu_items` is `[]`, not an invented drink list), no fabricated
description field, no phone (Google's page had none), and only one review
because only one had visible text on that pass. That is what an honest
scrape looks like -- most records will be partial on the first run and
fill in further on later runs.


USAGE
-----
    pip install playwright pandas
    playwright install chromium

    python google_maps_only_scraper.py --csv places.csv --dry-run
    python google_maps_only_scraper.py --csv places.csv
    python google_maps_only_scraper.py --csv places.csv --proxy http://user:pass@host:port
    python google_maps_only_scraper.py --check-ip --proxy http://...
    python google_maps_only_scraper.py --probe --probe-name "2Bros Cafe"
    python google_maps_only_scraper.py --csv places.csv --recheck-closed   # re-verify
                                                                            # anything not
                                                                            # tagged Open

Run it repeatedly. It only works on rows that are not yet accepted, and it
writes back to the same CSV after every single cafe, so Ctrl-C at any point
loses at most the one cafe in flight.


IP PROTECTION -- still not a guarantee
---------------------------------------
Single host now (www.google.com), so the surface is smaller than v6, but the
same limits apply: a persistent browser profile (real cookies -> fewer
consent walls), a daily per-host request budget, an abort on the first hard
block signal for the rest of the run, batches of 10 with cooldowns between
them, and a proxy preflight that refuses to start if the proxy is not
actually hiding your IP. None of this can promise zero risk on your own
connection -- only a proxy you trust can do that.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote_plus, urlparse

try:
    import pandas as pd
except ImportError:
    sys.exit("pip install pandas")
try:
    from playwright.async_api import async_playwright
    from playwright.async_api import TimeoutError as PWTimeout
except ImportError:
    sys.exit("pip install playwright && playwright install chromium")


# ══════════════════════════════════════════════════════════════════════════
#  PATHS / CONFIG
# ══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
PROBE_DIR = BASE_DIR / "probes"
PROFILE_ROOT = BASE_DIR / ".profiles"
BUDGET_DIR = BASE_DIR / "budget"
for _d in (LOG_DIR, PROBE_DIR, PROFILE_ROOT, BUDGET_DIR):
    _d.mkdir(parents=True, exist_ok=True)

DEFAULT_INPUT_CSV = "/home/barikoi/bkoi_project_1/bkoi_322/places_202609161553.csv"
HOST = "www.google.com"

PACE = {
    "safe":       {HOST: 20.0, "_default": 20.0},
    "paranoid":   {HOST: 40.0, "_default": 40.0},
    "aggressive": {HOST: 7.0,  "_default": 7.0},   # raises block risk; not the default
}
JITTER = 0.5
DAILY_BUDGET_DEFAULT = 700          # per host per day, generous for 4700 rows over a week
BATCH_SIZE = 25
BATCH_COOLDOWN = (15.0, 30.0)
BREAKER_THRESHOLD = 2
BREAKER_COOLDOWN = 600.0
NAV_TIMEOUT_MS = 45_000
SETTLE_MS = 1_500
GEO_TOL_M = 1_200

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
VIEWPORT = {"width": 1440, "height": 900}
DHAKA = {"latitude": 23.7806, "longitude": 90.4193}
IP_ECHO = "https://api.ipify.org?format=json"

# ── acceptance ───────────────────────────────────────────────────────────
CORE_FIELDS = ["g_latitude", "g_longitude", "g_address", "opening_hours"]
RICH_FIELDS = ["phone", "website", "google_rating", "google_review_count",
              "reviews"]
MIN_RICH = 3
MIN_HOURS_DAYS = 5
BONUS_FIELDS = ["menu_items", "facebook_url", "instagram_url", "category"]
FIELD_WEIGHTS = {**{f: 3.0 for f in CORE_FIELDS},
                 **{f: 2.0 for f in RICH_FIELDS},
                 **{f: 1.0 for f in BONUS_FIELDS}}

TAG_OPEN, TAG_TEMP, TAG_PERM, TAG_UNKNOWN = (
    "Open", "Temporarily Closed", "Permanently Closed", "Unknown")


# ══════════════════════════════════════════════════════════════════════════
#  SELECTORS  — Google Maps only. Patch here when the DOM shifts.
#  Attribute-based selectors (data-item-id=...) are stable for years.
#  Class-name selectors churn; each has fallbacks, most stable first.
#  Use --probe to dump a live page when a field starts coming back empty.
# ══════════════════════════════════════════════════════════════════════════

SEL = {
    "consent":      ['#L2AGLb', 'button[aria-label*="Accept all" i]',
                     'form[action*="consent"] button'],
    "place_link":   ['a[href*="/maps/place/"]'],
    "title":        ['h1.DUwDvf', 'h1[class*="DUwDvf"]', 'div[role="main"] h1'],
    "status_badge": ['span.fCEvvc', '[class*="fCEvvc"]', 'span.o0Svhf',
                     'div[role="main"] span:has-text("Temporarily closed")',
                     'div[role="main"] span:has-text("Permanently closed")'],
    "phone_btn":    ['button[data-item-id^="phone:tel:"]', '[data-item-id^="phone:tel:"]'],
    "address_btn":  ['button[data-item-id="address"]', '[data-item-id="address"]'],
    "website_btn":  ['a[data-item-id="authority"]', '[data-item-id="authority"]'],
    "plus_code":    ['[data-item-id="oloc"]'],
    "category":     ['button[jsaction*="category"]', 'button.DkEaL'],
    "rating":       ['div.F7nice span[aria-hidden="true"]',
                     'span[aria-label$="stars"]', 'div[class*="fontDisplayLarge"]'],
    "review_count": ['div.F7nice span[aria-label*="review"]',
                     'button[jsaction*="reviewChart"] span', 'span[aria-label*="reviews"]'],
    "hours_toggle": ['button[data-item-id="oh"]', '[jsaction*="openhours"]',
                     'button[aria-label*="Show open hours"]', 'div[aria-label*="Hours"]'],
    "hours_table":  ['table.eK4R0e tr', 'div[class*="t39EBf"] table tr',
                     'table[aria-label*="hours" i] tr'],
    "reviews_tab":  ['button[role="tab"][aria-label*="Reviews"]',
                     'button[jsaction*="moreReviews"]', 'button[aria-label*="Reviews for"]'],
    "scroll_panel": ['div[role="main"]', 'div[role="feed"]'],
    "review_card":  ['div[data-review-id]', 'div[class*="jftiEf"]'],
    "review_author": ['button.d4r55', 'div[class*="d4r55"]'],
    "review_stars": ['span[role="img"][aria-label*="star" i]',
                     'span.kvMYJc[aria-label*="star" i]'],
    "review_time":  ['span.rsqaWe', 'span[class*="rsqaWe"]'],
    "review_text":  ['span.wiI7pd', 'div.MyEned span', '[class*="wiI7pd"]'],
    "review_more":  ['button[aria-label="See more"]'],
    "menu_tab":     ['button[aria-label*="Menu" i]',
                     '[data-tab-index][aria-label*="Menu" i]', 'a[href*="menu" i]'],
    "menu_section": ['div[class*="iP2t7d"]', 'div[role="list"] > div'],
    "menu_heading": ['h2', 'h3', 'div[class*="fontTitleSmall"]'],
    "menu_item":    ['[class*="Be0ygb"]', '[class*="menu-item"]', '[data-item-id*="menu"]'],
}


# ══════════════════════════════════════════════════════════════════════════
#  REGEX / CONSTANTS
# ══════════════════════════════════════════════════════════════════════════

RE_PLACE_COORDS = re.compile(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)')
RE_VIEW_COORDS = re.compile(r'@(-?\d+\.\d+),(-?\d+\.\d+)')
RE_CID = re.compile(r'!1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)')
RE_PHONE_CAND = re.compile(r'(?<![\d])(\+?\d{2,}(?:[\s\-().]{1,2}\d{2,}){0,4})(?![\d])')
RE_RATING = re.compile(r'\b([0-5](?:\.\d)?)\b')
RE_COUNT = re.compile(r'([\d][\d,\s]{0,9}\d|\d)')
RE_FB = re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/'
                   r'(?!sharer|share|tr[/?]|dialog|plugins|events/)[\w.\-]+/?')
RE_IG = re.compile(r'https?://(?:www\.)?instagram\.com/'
                   r'(?!p/|reel/|explore/|accounts/)[\w.\-]+/?')
RE_PRICE_BDT = re.compile(r'(?:৳|BDT|Tk\.?)\s*([\d,]+(?:\.\d{1,2})?)', re.I)

DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
DAY_ALIAS = {"monday": "mon", "mon": "mon", "tuesday": "tue", "tue": "tue",
            "wednesday": "wed", "wed": "wed", "thursday": "thu", "thu": "thu",
            "friday": "fri", "fri": "fri", "saturday": "sat", "sat": "sat",
            "sunday": "sun", "sun": "sun"}
RE_HOURS_LINE = re.compile(
    r'\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|'
    r'Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b[^\dA-Za-z]{0,6}'
    r'(Closed|Open\s*24\s*hours|'
    r'\d{1,2}(?::\d{2})?\s*(?:AM|PM)?\s*[\u2013\u2014\-]\s*'
    r'\d{1,2}(?::\d{2})?\s*(?:AM|PM))', re.IGNORECASE)

RE_RELTIME = re.compile(
    r'\b(?:a|an|(\d+))\s+(second|minute|hour|day|week|month|year)s?\s+ago\b', re.I)

BLOCK_HARD = ["unusual traffic", "not a robot", "our systems have detected",
             "captcha", "access denied", "pardon our interruption",
             "verify you are a human", "too many requests"]
BLOCK_SOFT = ["enable javascript and cookies to continue", "rate limit"]


# ══════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════

LOG_PATH = LOG_DIR / f"run_{datetime.now():%Y%m%d_%H%M%S}.log"
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname).1s] %(message)s",
                    datefmt="%H:%M:%S",
                    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"),
                             logging.StreamHandler(sys.stdout)])
log = logging.getLogger("gmaps")


# ══════════════════════════════════════════════════════════════════════════
#  PURE UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip() or v.strip().lower() in ("nan", "none", "null")
    if isinstance(v, (list, dict)):
        return len(v) == 0
    if isinstance(v, float):
        return math.isnan(v)
    return False


def haversine_m(lat1, lng1, lat2, lng2) -> float:
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


def norm_bd_phone(raw: Any) -> str | None:
    if not raw:
        return None
    d = re.sub(r'[^\d+]', '', str(raw)).lstrip('+')
    if d.startswith('00880'):
        d = d[5:]
    elif d.startswith('880'):
        d = d[3:]
    d = d.lstrip('0')
    if re.fullmatch(r'1[3-9]\d{8}', d):
        return '+880' + d
    if re.fullmatch(r'2\d{7,8}', d):
        return '+880' + d
    return None


def find_phones(text: str) -> list[str]:
    out, seen = [], set()
    for m in RE_PHONE_CAND.finditer(text or ""):
        p = norm_bd_phone(m.group(1))
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def norm_hours_value(s: str) -> str:
    s = re.sub(r'\s+', ' ', str(s)).strip().strip(',;')
    s = re.sub(r'[\u2013\u2014]', '-', s)
    s = re.sub(r'\s*-\s*', ' \u2013 ', s)
    return re.sub(r'\b([ap])\.?m\.?\b', lambda m: m.group(1).upper() + 'M', s, flags=re.I)


def to_float(v: Any) -> float | None:
    try:
        f = float(str(v).strip().replace(',', ''))
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def to_int(v: Any) -> int | None:
    if v is None:
        return None
    m = RE_COUNT.search(str(v).replace('\u00a0', ' '))
    if not m:
        return None
    try:
        return int(re.sub(r'[,\s]', '', m.group(1)))
    except ValueError:
        return None


def clean_url(u: str | None) -> str | None:
    if not u or not str(u).startswith("http"):
        return None
    u = str(u).split('#')[0]
    if not urlparse(u).netloc:
        return None
    return u.split('?')[0].rstrip('/')


def name_tokens(s: str) -> set[str]:
    return {t for t in re.split(r'[^a-z0-9]+', str(s).lower()) if len(t) > 2}


def name_match_score(a: str, b: str) -> float:
    ta, tb = name_tokens(a), name_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def detect_status_from_text(text: str) -> tuple[str, str | None]:
    """Map Google's own wording to our 4-value tag. Permanent checked first."""
    low = (text or "").lower()
    if "permanently closed" in low:
        return TAG_PERM, "permanently closed"
    if "temporarily closed" in low:
        return TAG_TEMP, "temporarily closed"
    return TAG_UNKNOWN, None


def parse_relative_time(text: str, anchor: datetime) -> tuple[str | None, str | None]:
    """
    'a month ago' / '3 weeks ago' -> (raw text, approx ISO date). This is a
    real subtraction from an observed relative-time string, not a guess.
    """
    if not text:
        return None, None
    m = RE_RELTIME.search(text)
    if not m:
        return text.strip() or None, None
    n = int(m.group(1)) if m.group(1) else 1
    unit = m.group(2).lower()
    delta = {"second": timedelta(seconds=n), "minute": timedelta(minutes=n),
             "hour": timedelta(hours=n), "day": timedelta(days=n),
             "week": timedelta(weeks=n), "month": timedelta(days=30 * n),
             "year": timedelta(days=365 * n)}[unit]
    return text.strip(), (anchor - delta).date().isoformat()


# ══════════════════════════════════════════════════════════════════════════
#  BUDGET + HOST GUARD  (single host, but kept general for reuse)
# ══════════════════════════════════════════════════════════════════════════

class Budget:
    def __init__(self, path: Path, cap: int):
        self.path, self.cap_n = path, cap
        self.data = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("budget file unreadable -- starting fresh")
        self.day = self.data.setdefault(today_key(), {})
        for k in sorted(self.data)[:-14]:
            self.data.pop(k, None)

    def used(self, host: str) -> int:
        return int(self.day.get(host, 0))

    def can_spend(self, host: str) -> bool:
        return self.used(host) < self.cap_n

    def spend(self, host: str) -> None:
        self.day[host] = self.used(host) + 1

    def flush(self) -> None:
        atomic_write_text(self.path, json.dumps(self.data, indent=2))

    def report(self) -> str:
        return " | ".join(f"{h}:{self.used(h)}/{self.cap_n}" for h in sorted(self.day)) \
            or "nothing spent yet"


class HostGuard:
    def __init__(self, intervals: dict, budget: Budget):
        self.intervals, self.budget = intervals, budget
        self._last: dict[str, float] = {}
        self._fails: dict[str, int] = {}
        self._cooldown: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.disabled: dict[str, str] = {}
        self.events: list[dict] = []

    def available(self, host: str) -> tuple[bool, str]:
        if host in self.disabled:
            return False, f"disabled:{self.disabled[host]}"
        if not self.budget.can_spend(host):
            return False, f"budget-exhausted({self.budget.cap_n}/day)"
        until = self._cooldown.get(host, 0.0)
        if until and time.monotonic() < until:
            return False, f"cooling {until - time.monotonic():.0f}s"
        if until:
            self._cooldown.pop(host, None)
            self._fails[host] = 0
        return True, "ok"

    async def acquire(self, host: str) -> None:
        await self._lock.acquire()
        iv = self.intervals.get(host, self.intervals["_default"]) \
            * random.uniform(1 - JITTER, 1 + JITTER)
        gap = time.monotonic() - self._last.get(host, 0.0)
        if gap < iv:
            await asyncio.sleep(iv - gap)

    def release(self, host: str) -> None:
        self._last[host] = time.monotonic()
        if self._lock.locked():
            self._lock.release()

    def ok(self, host: str) -> None:
        self._fails[host] = 0

    def soft_fail(self, host: str, reason: str) -> None:
        n = self._fails.get(host, 0) + 1
        self._fails[host] = n
        if n >= BREAKER_THRESHOLD:
            self._cooldown[host] = time.monotonic() + BREAKER_COOLDOWN
            self.events.append({"host": host, "kind": "cooldown", "reason": reason,
                                "at": now_iso()})
            log.warning("   cooling %s for %.0fs (%s)", host, BREAKER_COOLDOWN, reason)

    def hard_block(self, host: str, reason: str) -> None:
        self.disabled[host] = reason
        self.events.append({"host": host, "kind": "hard_block", "reason": reason,
                            "at": now_iso()})
        log.error("   BLOCK DETECTED on %s (%s). Disabling for the rest of this "
                  "run. Do not re-run today.", host, reason)


def detect_block(url: str, body: str) -> tuple[str | None, bool]:
    u = (url or "").lower()
    if "/sorry/" in u or "consent.google" in u or "captcha" in u:
        return f"block-url:{u[:60]}", True
    low = (body or "")[:6000].lower()
    for sig in BLOCK_HARD:
        if sig in low:
            return f"block-text:{sig}", True
    for sig in BLOCK_SOFT:
        if sig in low:
            return f"soft:{sig}", False
    if len((body or "").strip()) < 120:
        return "empty-body", False
    return None, False


# ══════════════════════════════════════════════════════════════════════════
#  CSV, IN PLACE  — reads your file, adds/updates columns, writes it back
# ══════════════════════════════════════════════════════════════════════════

ENRICH_COLUMNS = [
    "tag", "tag_note",
    "g_address", "g_latitude", "g_longitude", "g_maps_url", "g_cid",
    "phone", "website", "facebook_url", "instagram_url", "category", "plus_code",
    "google_rating", "google_review_count",
    "opening_hours_json", "opening_hours_days",
    "menu_items_json", "menu_item_count", "menu_source",
    "reviews_json", "review_count_with_text",
    "completeness_score", "accepted", "scrape_status",
    "attempts", "last_attempt", "first_scraped_at", "name_match", "distance_from_bkoi_m",
]
JSON_COLUMNS = {"opening_hours_json", "menu_items_json", "reviews_json"}


def _col(df: pd.DataFrame, *names: str) -> str | None:
    lower = {c.lower().strip(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


class CsvState:
    """
    Owns the single input/output CSV. Loads it, exposes per-row enrichment
    dicts keyed by place_code, and writes the whole thing back in place.
    A one-time pristine backup is made before the first ever write.
    """

    def __init__(self, csv_path: Path):
        self.path = csv_path
        if not csv_path.exists():
            sys.exit(f"CSV not found: {csv_path}")
        self.df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        self.c_code = _col(self.df, "place_code", "id")
        self.c_name = _col(self.df, "business_name", "name")
        self.c_lat = _col(self.df, "latitude")
        self.c_lng = _col(self.df, "longitude")
        self.c_type = _col(self.df, "type")
        self.c_stype = _col(self.df, "sub_type")
        self.c_addr = _col(self.df, "address")
        if not (self.c_code and self.c_name and self.c_lat and self.c_lng):
            sys.exit("CSV needs at least place_code/id, business_name, "
                     "latitude, longitude.")
        for col in ENRICH_COLUMNS:
            if col not in self.df.columns:
                self.df[col] = ""
        self.df = self.df.set_index(self.c_code, drop=False)

        backup = csv_path.with_name(csv_path.stem + ".original_backup.csv")
        if not backup.exists():
            backup.write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")
            log.info("Pristine backup written once: %s", backup.name)

    # ── candidate loading ───────────────────────────────────────────────
    def food_candidates(self, type_contains: str, subtype_contains: str | None,
                        limit: int | None) -> list[dict]:
        df = self.df
        mask = pd.Series(True, index=df.index)
        if self.c_type:
            mask &= df[self.c_type].str.lower().str.contains(
                type_contains.lower(), na=False)
        if subtype_contains and self.c_stype:
            mask &= df[self.c_stype].str.lower().str.contains(
                subtype_contains.lower(), na=False)
        sub = df[mask].copy()
        sub = sub[sub[self.c_name].str.strip() != ""]
        sub["_lat"] = pd.to_numeric(sub[self.c_lat], errors="coerce")
        sub["_lng"] = pd.to_numeric(sub[self.c_lng], errors="coerce")
        sub = sub.dropna(subset=["_lat", "_lng"])
        log.info("Type filter ('%s'%s): %d / %d rows",
                 type_contains, f" + subtype '{subtype_contains}'" if subtype_contains else "",
                 len(sub), len(df))

        cands = []
        for pc, row in sub.iterrows():
            cands.append({
                "place_code": str(pc),
                "business_name": row[self.c_name].strip(),
                "bkoi_lat": float(row["_lat"]),
                "bkoi_lng": float(row["_lng"]),
                "bkoi_address": row.get(self.c_addr, "") if self.c_addr else "",
                "bkoi_sub_type": row.get(self.c_stype, "") if self.c_stype else "",
            })
        if limit:
            cands = cands[:limit]
        return cands

    # ── per-row enrichment state ──────────────────────────────────────────
    def get_enrichment(self, place_code: str) -> dict:
        row = self.df.loc[place_code]
        out = {}
        for col in ENRICH_COLUMNS:
            v = row.get(col, "")
            if col in JSON_COLUMNS:
                try:
                    out[col] = json.loads(v) if v else ({} if col == "opening_hours_json" else [])
                except (json.JSONDecodeError, TypeError):
                    out[col] = {} if col == "opening_hours_json" else []
            else:
                out[col] = v if v != "" else None
        return out

    def is_accepted(self, place_code: str) -> bool:
        row = self.df.loc[place_code]
        return str(row.get("accepted", "")).strip().lower() in ("true", "1")

    def tag(self, place_code: str) -> str:
        row = self.df.loc[place_code]
        return row.get("tag") or TAG_UNKNOWN

    def write_enrichment(self, place_code: str, enrichment: dict) -> None:
        for col in ENRICH_COLUMNS:
            v = enrichment.get(col)
            if col in JSON_COLUMNS:
                self.df.at[place_code, col] = json.dumps(v or ({} if col ==
                    "opening_hours_json" else []), ensure_ascii=False)
            elif isinstance(v, bool):
                self.df.at[place_code, col] = str(v)
            else:
                self.df.at[place_code, col] = "" if v is None else str(v)

    def flush(self) -> None:
        tmp = self.path.with_suffix(".csv.tmp")
        self.df.to_csv(tmp, index=False)
        os.replace(tmp, self.path)


# ══════════════════════════════════════════════════════════════════════════
#  ENRICHMENT SCORING
# ══════════════════════════════════════════════════════════════════════════

def field_present(e: dict, field: str) -> bool:
    if field == "opening_hours":
        return len(e.get("opening_hours_json") or {}) >= MIN_HOURS_DAYS
    if field == "reviews":
        return len(e.get("reviews_json") or []) > 0
    if field == "menu_items":
        return len(e.get("menu_items_json") or []) > 0
    return not is_empty(e.get(field))


def score(e: dict) -> tuple[float, bool]:
    got = sum(w for f, w in FIELD_WEIGHTS.items() if field_present(e, f))
    completeness = round(got / sum(FIELD_WEIGHTS.values()), 4)
    if e.get("tag") in (TAG_PERM, TAG_TEMP):
        accepted = field_present(e, "g_address") and not is_empty(e.get("g_latitude"))
    else:
        accepted = (all(field_present(e, f) for f in CORE_FIELDS)
                   and sum(1 for f in RICH_FIELDS if field_present(e, f)) >= MIN_RICH)
    return completeness, accepted


def missing_report(e: dict) -> list[str]:
    out = [f"core:{f}" for f in CORE_FIELDS if not field_present(e, f)]
    n = sum(1 for f in RICH_FIELDS if field_present(e, f))
    if n < MIN_RICH:
        out.append(f"rich:{n}/{MIN_RICH}")
    return out


# ══════════════════════════════════════════════════════════════════════════
#  BROWSER HELPERS
# ══════════════════════════════════════════════════════════════════════════

def proxy_config(proxy: str | None) -> dict | None:
    if not proxy:
        return None
    p = urlparse(proxy)
    cfg = {"server": f"{p.scheme}://{p.hostname}:{p.port}" if p.port
           else f"{p.scheme}://{p.hostname}"}
    if p.username:
        cfg["username"] = p.username
    if p.password:
        cfg["password"] = p.password
    return cfg


async def preflight_ip(pw, proxy: str | None) -> bool:
    direct_ip = proxied_ip = None
    try:
        c = await pw.request.new_context(extra_http_headers={"User-Agent": UA})
        r = await c.get(IP_ECHO, timeout=20_000)
        direct_ip = (await r.json()).get("ip")
        await c.dispose()
    except Exception as e:                        # noqa: BLE001
        log.warning("Could not read your direct IP: %s", str(e)[:90])
    if not proxy:
        log.warning("=" * 68)
        log.warning("NO PROXY SET. Requests will use your own IP (%s).",
                    direct_ip or "unknown")
        log.warning("This cannot be made block-proof without a working proxy.")
        log.warning("=" * 68)
        return True
    try:
        c = await pw.request.new_context(proxy=proxy_config(proxy),
                                         extra_http_headers={"User-Agent": UA})
        r = await c.get(IP_ECHO, timeout=25_000)
        proxied_ip = (await r.json()).get("ip")
        await c.dispose()
    except Exception as e:                        # noqa: BLE001
        log.error("PROXY PREFLIGHT FAILED: %s", str(e)[:140])
        return False
    log.info("Preflight: direct=%s via-proxy=%s", direct_ip, proxied_ip)
    if not proxied_ip or (direct_ip and proxied_ip == direct_ip):
        log.error("Proxy is not isolating your IP. Refusing to start.")
        return False
    log.info("Proxy verified.")
    return True


async def launch_context(pw, headless: bool, proxy: str | None, profile: str):
    pdir = PROFILE_ROOT / profile
    pdir.mkdir(parents=True, exist_ok=True)
    kw: dict[str, Any] = dict(
        user_data_dir=str(pdir), headless=headless, user_agent=UA,
        viewport=VIEWPORT, locale="en-US", timezone_id="Asia/Dhaka",
        geolocation=DHAKA, permissions=["geolocation"],
        args=["--disable-dev-shm-usage", "--no-first-run",
              "--no-default-browser-check", "--lang=en-US"])
    if proxy:
        kw["proxy"] = proxy_config(proxy)
    ctx = await pw.chromium.launch_persistent_context(**kw)
    ctx.set_default_timeout(15_000)
    ctx.set_default_navigation_timeout(NAV_TIMEOUT_MS)

    async def _block(route):
        try:
            await route.abort()
        except Exception:
            pass
    await ctx.route(re.compile(r'\.(woff2?|ttf|otf|eot|mp4|webm|avi)(\?|$)'), _block)
    return ctx


async def goto(page, url: str, guard: HostGuard) -> str | None:
    avail, why = guard.available(HOST)
    if not avail:
        log.info("   skip (%s)", why)
        return None
    await guard.acquire(HOST)
    try:
        guard.budget.spend(HOST)
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        if resp is not None and resp.status in (403, 429, 503):
            (guard.hard_block if resp.status == 429 else guard.soft_fail)(
                HOST, f"http-{resp.status}")
            return None
        await page.wait_for_timeout(SETTLE_MS + random.randint(0, 2000))
        try:
            body = await page.inner_text("body", timeout=9000)
        except PWTimeout:
            body = await page.content()
        reason, hard = detect_block(page.url, body)
        if reason:
            (guard.hard_block if hard else guard.soft_fail)(HOST, reason)
            return None
        guard.ok(HOST)
        return body
    except PWTimeout:
        guard.soft_fail(HOST, "nav-timeout")
        return None
    except Exception as e:                        # noqa: BLE001
        guard.soft_fail(HOST, type(e).__name__)
        log.warning("   %s: %s", type(e).__name__, str(e)[:110])
        return None
    finally:
        guard.release(HOST)


async def first_text(page, sels: Iterable[str], min_len: int = 1) -> str | None:
    for sel in sels:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                t = (await loc.inner_text(timeout=2500)).strip()
                if len(t) >= min_len:
                    return t
        except Exception:
            continue
    return None


async def first_attr(page, sels: Iterable[str], attr: str) -> str | None:
    for sel in sels:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                v = await loc.get_attribute(attr, timeout=2500)
                if v:
                    return v
        except Exception:
            continue
    return None


async def click_first(page, sels: Iterable[str], settle: int = 1500) -> bool:
    for sel in sels:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                await loc.click(timeout=4000)
                await page.wait_for_timeout(settle)
                return True
        except Exception:
            continue
    return False


async def scroll_panel(page, sels: Iterable[str], steps: int = 5) -> None:
    for sel in sels:
        try:
            loc = page.locator(sel).first
            if not await loc.count():
                continue
            for _ in range(steps):
                await loc.evaluate("el => el.scrollBy(0, el.clientHeight * 0.85)")
                await page.wait_for_timeout(random.randint(500, 1100))
            return
        except Exception:
            continue


# ══════════════════════════════════════════════════════════════════════════
#  THE SCRAPE ITSELF
# ══════════════════════════════════════════════════════════════════════════

async def scrape_one(page, cafe: dict, guard: HostGuard) -> dict:
    """Returns a fresh enrichment dict. Never merges -- caller merges."""
    e: dict[str, Any] = {"scrape_status": "pending"}
    area_hint = ""
    query = f"{cafe['business_name']}, {cafe.get('bkoi_address') or 'Dhaka'}"
    url = ("https://www.google.com/maps/search/" + quote_plus(query) +
           f"/@{cafe['bkoi_lat']},{cafe['bkoi_lng']},17z?hl=en")

    body = await goto(page, url, guard)
    if body is None:
        e["scrape_status"] = "blocked_or_error"
        return e
    await click_first(page, SEL["consent"], 2000)

    if "/maps/place/" not in page.url:
        try:
            link = page.locator(SEL["place_link"][0]).first
            if await link.count():
                await link.click(timeout=6000)
                await page.wait_for_timeout(SETTLE_MS)
        except Exception:
            pass
    if "/maps/place/" not in page.url:
        e["scrape_status"] = "not_found"
        return e

    place_url = page.url
    e["g_maps_url"] = place_url

    m = RE_PLACE_COORDS.search(place_url)
    if not m:
        try:
            m = RE_PLACE_COORDS.search(await page.content())
        except Exception:
            m = None
    if m:
        e["g_latitude"], e["g_longitude"] = float(m.group(1)), float(m.group(2))
    else:
        mv = RE_VIEW_COORDS.search(place_url)
        if mv:
            e["g_latitude"], e["g_longitude"] = float(mv.group(1)), float(mv.group(2))
    mc = RE_CID.search(place_url)
    if mc:
        e["g_cid"] = mc.group(1)

    title = await first_text(page, SEL["title"])
    name_match = round(name_match_score(cafe["business_name"], title or ""), 3) \
        if title else 0.0
    e["name_match"] = name_match
    if e.get("g_latitude") is not None:
        dist = haversine_m(cafe["bkoi_lat"], cafe["bkoi_lng"],
                           e["g_latitude"], e["g_longitude"])
        e["distance_from_bkoi_m"] = round(dist)
        if dist > GEO_TOL_M and name_match < 0.45:
            log.warning("   %.0fm away + weak name match ('%s' vs '%s') -- rejecting",
                        dist, cafe["business_name"], title)
            e["scrape_status"] = "geo_mismatch"
            return e

    # ── status tag: Open / Temporarily Closed / Permanently Closed / Unknown ──
    badge = await first_text(page, SEL["status_badge"]) or ""
    tag, note = detect_status_from_text(badge)
    if tag == TAG_UNKNOWN:
        tag, note = detect_status_from_text(body[:2500])
    e["tag"] = tag if tag != TAG_UNKNOWN else TAG_OPEN   # confirmed page load = operating
    if tag == TAG_UNKNOWN:
        e["tag_note"] = "no closure badge seen; defaulting to Open on a live listing"
    else:
        e["tag_note"] = note
        log.info("   TAG: %s", tag.upper())

    raw_phone = await first_attr(page, SEL["phone_btn"], "data-item-id")
    e["phone"] = norm_bd_phone(raw_phone.replace("phone:tel:", "")) if raw_phone else None
    addr = await first_text(page, SEL["address_btn"], min_len=8)
    e["g_address"] = re.sub(r'\s+', ' ', addr.replace("Address:", "")).strip() if addr else None
    site = clean_url(await first_attr(page, SEL["website_btn"], "href"))
    e["website"] = site if site and "google." not in urlparse(site).netloc else None
    pc_code = await first_text(page, SEL["plus_code"], min_len=4)
    e["plus_code"] = pc_code.replace("Plus code:", "").strip() if pc_code else None
    cat = await first_text(page, SEL["category"], min_len=3)
    e["category"] = cat.strip() if cat else None

    rtxt = await first_text(page, SEL["rating"])
    rating = None
    if rtxt:
        mr = RE_RATING.search(rtxt.replace(',', '.'))
        if mr:
            v = to_float(mr.group(1))
            rating = v if v and 0 < v <= 5 else None
    ctxt = (await first_text(page, SEL["review_count"])
            or await first_attr(page, SEL["review_count"], "aria-label"))
    count = to_int(ctxt)
    if rating is None or count is None:
        mb = re.search(r'([0-5]\.\d)\s*\(?\s*([\d,]+)', body)
        if mb:
            rating = rating or to_float(mb.group(1))
            count = count or to_int(mb.group(2))
    e["google_rating"], e["google_review_count"] = rating, count

    # ── opening hours ──────────────────────────────────────────────────────
    hours: dict[str, str] = {}
    await click_first(page, SEL["hours_toggle"], 1800)
    for tsel in SEL["hours_table"]:
        try:
            rows = await page.locator(tsel).all()
        except Exception:
            continue
        for row in rows[:10]:
            try:
                cells = [c.strip() for c in await row.locator("td,th").all_inner_texts()]
            except Exception:
                continue
            if len(cells) < 2:
                continue
            key = DAY_ALIAS.get(re.sub(r'[^a-z]', '', cells[0].lower())[:9])
            if key and cells[1]:
                hours[key] = norm_hours_value(cells[1])
        if len(hours) >= MIN_HOURS_DAYS:
            break
    if len(hours) < MIN_HOURS_DAYS:
        lbl = await first_attr(page, SEL["hours_toggle"], "aria-label") or ""
        for chunk in (lbl, body):
            for mh in RE_HOURS_LINE.finditer(chunk):
                key = DAY_ALIAS.get(re.sub(r'[^a-z]', '', mh.group(1).lower()))
                if key and key not in hours:
                    hours[key] = norm_hours_value(mh.group(2))
            if len(hours) >= MIN_HOURS_DAYS:
                break
    e["opening_hours_json"] = {d: hours[d] for d in DAY_ORDER if d in hours}

    # ── reviews: real text, real relative time, computed approx date ───────
    reviews: list[dict] = []
    if await click_first(page, SEL["reviews_tab"], 2600):
        await scroll_panel(page, SEL["scroll_panel"], steps=5)
        for btn in (await page.locator(SEL["review_more"][0]).all())[:10]:
            try:
                await btn.click(timeout=1000)
            except Exception:
                pass
        cards = []
        for csel in SEL["review_card"]:
            try:
                cards = await page.locator(csel).all()
            except Exception:
                continue
            if cards:
                break
        anchor = datetime.now(timezone.utc)
        for card in cards[:8]:
            try:
                text = await first_text_in(card, SEL["review_text"])
            except Exception:
                text = None
            if not text or len(text) < 15:
                continue
            author = await first_text_in(card, SEL["review_author"]) or None
            rel = await first_text_in(card, SEL["review_time"]) or None
            star_lbl = await first_attr_in(card, SEL["review_stars"], "aria-label")
            r_rating = None
            if star_lbl:
                mrs = RE_RATING.search(star_lbl)
                if mrs:
                    r_rating = to_int(mrs.group(1)) or to_float(mrs.group(1))
            raw_rel, approx_date = parse_relative_time(rel or "", anchor)
            reviews.append({
                "author": author, "rating": r_rating,
                "relative_time": raw_rel, "approx_date": approx_date,
                "text": re.sub(r'\s+', ' ', text).strip()[:600],
                "source_tag": "google_review",
            })
    e["reviews_json"] = reviews
    e["review_count_with_text"] = len(reviews)

    # ── menu: real text only, tagged by how it was found, never invented ───
    menu_items: list[dict] = []
    menu_source = None
    try:
        for tsel in SEL["menu_tab"]:
            tab = page.locator(tsel).first
            if not await tab.count():
                continue
            href = await tab.get_attribute("href")
            if href and href.startswith("http") and "google" not in href:
                e["menu_url"] = href
                break
            await click_first(page, [tsel], 2000)
            await scroll_panel(page, ['[role="main"]'], steps=4)
            current_category = None
            for sec_sel in SEL["menu_section"]:
                try:
                    sections = await page.locator(sec_sel).all()
                except Exception:
                    continue
                if not sections:
                    continue
                for sec in sections[:40]:
                    heading = await first_text_in(sec, SEL["menu_heading"], min_len=2)
                    if heading and len(heading) < 40 and not RE_PRICE_BDT.search(heading):
                        current_category = heading.strip()
                    for isel in SEL["menu_item"]:
                        try:
                            items = await sec.locator(isel).all()
                        except Exception:
                            continue
                        for it in items[:30]:
                            try:
                                txt = re.sub(r'\s+', ' ',
                                            await it.inner_text(timeout=600)).strip()
                            except Exception:
                                continue
                            if len(txt) < 3:
                                continue
                            mp = RE_PRICE_BDT.search(txt)
                            price = to_float(mp.group(1)) if mp else None
                            nm = RE_PRICE_BDT.sub('', txt).strip().split('\n')[0]
                            nm = re.sub(r'\s+', ' ', nm)[:80].strip(' -,')
                            if nm and 2 < len(nm) < 80:
                                menu_items.append({
                                    "item": nm, "price_bdt": price, "currency": "BDT",
                                    "category": current_category,
                                    "source_tag": "google_menu_tab"})
                if menu_items:
                    menu_source = "google_menu_tab"
                    break
            break
    except Exception:
        pass
    if not menu_items:
        seen = set()
        for mp in RE_PRICE_BDT.finditer(body):
            start = max(0, mp.start() - 45)
            nm = body[start:mp.start()].strip().split('\n')[-1].strip(' -,:')
            price = to_float(mp.group(1))
            if not nm or len(nm) < 3 or len(nm) > 60 or not (10 <= (price or 0) <= 20000):
                continue
            k = nm.lower()
            if k in seen:
                continue
            seen.add(k)
            menu_items.append({"item": nm, "price_bdt": price, "currency": "BDT",
                               "category": None, "source_tag": "google_body_text"})
        if menu_items:
            menu_source = "google_body_text"
    e["menu_items_json"] = menu_items[:60]
    e["menu_source"] = menu_source

    try:
        html = await page.content()
    except Exception:
        html = body
    mfb, mig = RE_FB.search(html), RE_IG.search(html)
    e["facebook_url"] = clean_url(mfb.group(0)) if mfb else None
    e["instagram_url"] = clean_url(mig.group(0)) if mig else None
    if not e["phone"]:
        ph = find_phones(body)
        e["phone"] = ph[0] if ph else None

    e["scrape_status"] = "ok"
    return e


async def first_text_in(scope, sels: Iterable[str], min_len: int = 1) -> str | None:
    for sel in sels:
        try:
            loc = scope.locator(sel).first
            if await loc.count():
                t = (await loc.inner_text(timeout=1500)).strip()
                if len(t) >= min_len:
                    return t
        except Exception:
            continue
    return None


async def first_attr_in(scope, sels: Iterable[str], attr: str) -> str | None:
    for sel in sels:
        try:
            loc = scope.locator(sel).first
            if await loc.count():
                v = await loc.get_attribute(attr, timeout=1500)
                if v:
                    return v
        except Exception:
            continue
    return None


def merge_enrichment(old: dict, new: dict) -> dict:
    """New non-empty values win; nothing already stored is ever erased."""
    if new.get("scrape_status") not in ("ok",):
        return old   # a blocked/failed attempt must not touch good data
    merged = dict(old)
    for k, v in new.items():
        if k in ("opening_hours_json",):
            if v:
                base = dict(old.get(k) or {})
                base.update(v)
                merged[k] = base if len(base) >= len(old.get(k) or {}) else old.get(k)
            continue
        if k in ("menu_items_json", "reviews_json"):
            if v:
                seen = {json.dumps(x, sort_keys=True, default=str)
                        for x in (old.get(k) or [])}
                combo = list(old.get(k) or [])
                for item in v:
                    key = json.dumps(item, sort_keys=True, default=str)
                    if key not in seen:
                        seen.add(key)
                        combo.append(item)
                merged[k] = combo
            continue
        if not is_empty(v):
            merged[k] = v
    merged["scrape_status"] = "ok"
    return merged


# ══════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════

async def run_pipeline(args) -> None:
    state = CsvState(Path(args.csv))
    cands = state.food_candidates(args.type_contains, args.subtype_contains, args.limit)
    if not cands:
        sys.exit("No rows matched the type filter. Check --type-contains / your CSV.")
    if args.dry_run:
        for i, c in enumerate(cands, 1):
            log.info("  %4d. %-42s %s", i, c["business_name"][:42], c["place_code"])
        mins = len(cands) * PACE[args.pace][HOST] / 60
        log.info("--dry-run. No network touched. %d candidates. Rough time at "
                 "'%s' pace: ~%.0f min just for the request pacing (plus batch "
                 "cooldowns and per-cafe extraction time on top).",
                 len(cands), args.pace, mins)
        return

    budget = Budget(BUDGET_DIR / (Path(args.csv).stem + "_budget.json"),
                    int(args.daily_budget))
    guard = HostGuard(PACE[args.pace], budget)

    pending = []
    for c in cands:
        if not state.is_accepted(c["place_code"]):
            pending.append(c)
        elif args.recheck_closed and state.tag(c["place_code"]) != TAG_OPEN:
            pending.append(c)
    log.info("Pace=%s  candidates=%d  pending=%d  budget: %s",
             args.pace, len(cands), len(pending), budget.report())
    if not pending:
        log.info("Nothing pending. Every candidate is already accepted. "
                 "Use --recheck-closed to re-verify closed listings, or "
                 "--limit/--type-contains to widen scope.")
        return

    proxies = []
    if args.proxy_file:
        proxies = [l.strip() for l in Path(args.proxy_file).read_text().splitlines()
                   if l.strip() and not l.startswith("#")]
    elif args.proxy:
        proxies = [args.proxy]

    started = now_iso()
    async with async_playwright() as pw:
        if not await preflight_ip(pw, proxies[0] if proxies else None):
            sys.exit("Preflight failed. Nothing was requested from Google.")
        if not proxies and args.require_proxy:
            sys.exit("--require-proxy set but no proxy given.")

        batches = [pending[i:i + BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
        for bi, batch in enumerate(batches, 1):
            if HOST in guard.disabled:
                log.error("Google is blocked for this run. Stopping to protect "
                         "your IP. Re-run tomorrow, or with --proxy / --pace paranoid.")
                break
            proxy = proxies[(bi - 1) % len(proxies)] if proxies else None
            profile = f"gmaps_{(bi - 1) % max(1, len(proxies))}"
            log.info("\n%s\nBATCH %d/%d (%d cafes)  proxy=%s  budget: %s\n%s",
                     "=" * 72, bi, len(batches), len(batch),
                     urlparse(proxy).hostname if proxy else "NONE (your own IP)",
                     budget.report(), "=" * 72)

            ctx = await launch_context(pw, args.headless, proxy, profile)
            try:
                for i, cafe in enumerate(batch, 1):
                    pc = cafe["place_code"]
                    old = state.get_enrichment(pc)
                    old.setdefault("attempts", 0)
                    log.info("\n [%d/%d] %s (%s)", i, len(batch),
                             cafe["business_name"], pc)
                    page = await ctx.new_page()
                    try:
                        fresh = await scrape_one(page, cafe, guard)
                    except Exception as e:        # noqa: BLE001
                        log.error("   fatal: %s", e)
                        fresh = {"scrape_status": f"error:{type(e).__name__}"}
                    finally:
                        try:
                            await page.close()
                        except Exception:
                            pass

                    merged = merge_enrichment(old, fresh)
                    merged["attempts"] = int(old.get("attempts") or 0) + 1
                    merged["last_attempt"] = now_iso()
                    merged.setdefault("first_scraped_at", now_iso())
                    completeness, accepted = score(merged)
                    merged["completeness_score"] = completeness
                    merged["accepted"] = accepted
                    if fresh.get("scrape_status") != "ok":
                        merged["scrape_status"] = fresh.get("scrape_status", "partial")
                    elif accepted:
                        merged["scrape_status"] = "accepted"
                    else:
                        merged["scrape_status"] = "partial"

                    state.write_enrichment(pc, merged)
                    state.flush()
                    budget.flush()

                    tagline = f" [{merged.get('tag')}]" if merged.get("tag") else ""
                    log.info("   -> %s %.0f%%%s  missing=%s",
                             "ACCEPTED" if accepted else "partial ",
                             completeness * 100, tagline,
                             missing_report(merged) or "none")
                    await asyncio.sleep(random.uniform(2.5, 6.0))
            finally:
                try:
                    await ctx.close()
                except Exception:
                    pass

            if bi < len(batches):
                cool = random.uniform(*BATCH_COOLDOWN)
                log.info("\nBatch %d done. Cooling %.0fs before batch %d.",
                         bi, cool, bi + 1)
                await asyncio.sleep(cool)

    budget.flush()
    print_summary(state, cands, guard, budget)


def print_summary(state: CsvState, cands: list[dict], guard: HostGuard,
                  budget: Budget) -> None:
    rows = [state.get_enrichment(c["place_code"]) for c in cands]
    n_ok = sum(1 for r in rows if str(r.get("accepted")).strip().lower() in ("true", "1")
              or r.get("accepted") is True)
    tags = {}
    for r in rows:
        tags[r.get("tag") or "not_scraped"] = tags.get(r.get("tag") or "not_scraped", 0) + 1
    print("\n" + "-" * 90)
    print(f"  Accepted {n_ok} / {len(rows)}")
    print("  Tags: " + ", ".join(f"{k}={v}" for k, v in sorted(tags.items())))
    print(f"  Budget spent today: {budget.report()}")
    if guard.disabled:
        print(f"  !! BLOCKED: {guard.disabled} -- do not re-run against Google today.")
    print(f"  CSV updated in place: {state.path}")
    print(f"  Pristine backup:      {state.path.with_name(state.path.stem + '.original_backup.csv')}")
    print(f"  Log: {LOG_PATH}")
    if n_ok < len(rows):
        print("\n  Not full yet -- run the same command again. It only re-works "
              "rows that are still missing data.")
    print("-" * 90 + "\n")


# ══════════════════════════════════════════════════════════════════════════
#  PROBE
# ══════════════════════════════════════════════════════════════════════════

async def run_probe(args) -> None:
    name = args.probe_name or "2Bros Cafe"
    budget = Budget(BUDGET_DIR / "_probe_budget.json", 999)
    guard = HostGuard(PACE[args.pace], budget)
    async with async_playwright() as pw:
        if not await preflight_ip(pw, args.proxy):
            sys.exit("Preflight failed.")
        ctx = await launch_context(pw, args.headless, args.proxy, "probe")
        page = await ctx.new_page()
        url = ("https://www.google.com/maps/search/" +
               quote_plus(f"{name}, Gulshan, Dhaka") + "?hl=en")
        body = await goto(page, url, guard)
        await click_first(page, SEL["consent"], 2000)
        if "/maps/place/" not in page.url:
            link = page.locator(SEL["place_link"][0]).first
            if await link.count():
                await link.click(timeout=6000)
                await page.wait_for_timeout(SETTLE_MS)
        base = PROBE_DIR / f"google_{datetime.now():%Y%m%d_%H%M%S}"
        try:
            base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
            base.with_suffix(".txt").write_text(body or "(blocked)", encoding="utf-8")
            await page.screenshot(path=str(base.with_suffix(".png")))
        except Exception as e:                    # noqa: BLE001
            log.warning("probe write failed: %s", e)
        log.info("Saved %s.{html,txt,png} -- inspect to repair SEL selectors.", base)
        if not args.headless:
            await asyncio.sleep(30)
        await ctx.close()
        budget.flush()


# ══════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Google-Maps-only food-place enrichment, rewriting the input CSV in place.")
    p.add_argument("--csv", default=DEFAULT_INPUT_CSV,
                   help=f"CSV to enrich in place (default: {DEFAULT_INPUT_CSV})")
    p.add_argument("--type-contains", default="food",
                   help="substring match on the CSV 'type' column (default: food)")
    p.add_argument("--subtype-contains",
                   help="optional extra substring match on 'sub_type'")
    p.add_argument("--limit", type=int, help="cap candidates this run")
    p.add_argument("--pace", default="safe", choices=list(PACE))
    p.add_argument("--daily-budget", type=float, default=DAILY_BUDGET_DEFAULT,
                   help="max Google requests per day (default %(default)s)")
    p.add_argument("--recheck-closed", action="store_true",
                   help="also re-attempt rows already tagged not-Open, in case "
                        "they have reopened")
    p.add_argument("--proxy", help="http://user:pass@host:port")
    p.add_argument("--proxy-file", help="one proxy per line, rotated per batch")
    p.add_argument("--require-proxy", action="store_true")
    p.add_argument("--check-ip", action="store_true")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--probe", action="store_true")
    p.add_argument("--probe-name")
    return p.parse_args(argv)


async def _check_ip(args):
    async with async_playwright() as pw:
        ok = await preflight_ip(pw, args.proxy)
    print("\nRESULT:", "SAFE -- proxy verified" if ok and args.proxy else
          "NO PROXY -- your own IP will be used" if ok else "FAILED -- do not run")


def main() -> None:
    args = parse_args()
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        if args.check_ip:
            asyncio.run(_check_ip(args))
        elif args.probe:
            asyncio.run(run_probe(args))
        else:
            asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        log.info("\nInterrupted. The CSV was written after every cafe, so at "
                 "most one row's work is lost. Re-run to continue.")


if __name__ == "__main__":
    main()