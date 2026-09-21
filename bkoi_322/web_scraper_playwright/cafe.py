#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BARIKOI · GULSHAN CAFE ENRICHMENT · v5
======================================

What changed from your v4, and why
----------------------------------
1.  MENU NO LONGER COMES FROM GOOGLE.  Requiring `g_menu_items >= 1` as a
    mandatory field is why nothing ever reached accepted=True.  Google Maps
    does not reliably expose per-item prices for Dhaka cafes.  Foodpanda does.
    Menu is now sourced from Foodpanda and tagged `fp_`.

2.  GPS CHECK WAS A NO-OP.  v4 read `@lat,lng` out of the URL -- that is the
    *map viewport centre*, not the place.  In method_a you navigated to
    `@csv_lat,csv_lng` yourself, so the check compared the CSV against itself
    and always passed, even on a totally wrong listing.  Real place coords
    live in the `!3d<lat>!4d<lng>` segment.  Fixed.

3.  SOURCE TAGGING IS STRUCTURAL, NOT COSMETIC.  Every record carries a
    `sources{}` block (one sub-dict per platform, raw) plus a `resolved{}`
    block (best-of, flat, matching your sample output) plus `provenance{}`
    naming which platform won each resolved field.

4.  BLOCK DETECTION + CIRCUIT BREAKER.  v4 would happily extract from a
    CAPTCHA page and merge the garbage into your single output file.  v5
    detects consent walls / sorry pages / 403s, trips a per-host breaker,
    cools that host down, and keeps working the other sources.

5.  ATOMIC WRITES.  v4 truncated cafes_gulshan.json on every save.  A crash
    or Ctrl-C mid-write destroyed the file you said must never be lost.
    v5 writes .tmp then os.replace(), plus a rolling .bak.

6.  PERSISTENT BROWSER PROFILE instead of a stealth JS blob.  Keeping real
    cookies across runs kills the cold-start consent wall, which is the
    actual thing that was breaking you.  Spoofing navigator.webdriver is
    detected anyway.

7.  SOURCES RUN CONCURRENTLY per cafe (different hosts -> no contention),
    with a per-host rate limiter.  ~3x faster than v4's serial A->B->C
    with 45-120s sleeps, at lower load per host.


LEGAL NOTE
----------
Google Maps, Foodpanda and TripAdvisor all prohibit automated collection in
their Terms of Service.  For production use at Barikoi, the Google Places API
(Place Details + Text Search) returns coordinates, phone, opening hours,
rating and review count legitimately for ~$17-32 / 1000 places.  Twenty cafes
costs well under a dollar.  Use this script for prototyping; use the API for
anything that ships.


USAGE
-----
    pip install playwright pandas
    playwright install chromium

    # normal run -- merges into the same json/csv every time
    python cafe_scraper_v5.py --csv "D:/bkoi_project_1/places_202609161553.csv"

    # keep running until 10 cafes are fully accepted
    python cafe_scraper_v5.py --target 10 --pool 18 --passes 3

    # headless
    python cafe_scraper_v5.py --headless

    # one source only, for debugging
    python cafe_scraper_v5.py --only google

    # dump a live page's DOM so you can repair selectors in 5 minutes
    python cafe_scraper_v5.py --probe google --probe-name "North End Coffee Roasters"

    # see what would be scraped, scrape nothing
    python cafe_scraper_v5.py --dry-run


SELECTOR MAINTENANCE
--------------------
Google's Maps DOM uses obfuscated, churning class names.  Everything that can
be keyed off a *stable* attribute (data-item-id="phone:tel:", "address",
"authority") is.  The rest is in the SELECTORS dict at the top of this file --
one place to patch when Google reshuffles.  Use --probe to get the current DOM.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, quote_plus, urlparse

try:
    import pandas as pd
except ImportError:
    print("pip install pandas", file=sys.stderr)
    raise

try:
    from playwright.async_api import async_playwright
    from playwright.async_api import TimeoutError as PWTimeout
except ImportError:
    print("pip install playwright && playwright install chromium", file=sys.stderr)
    raise


# ══════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
PROFILE_DIR = BASE_DIR / ".browser_profile"   # persistent cookies -> fewer consent walls
PROBE_DIR = BASE_DIR / "probes"

DEFAULT_INPUT_CSV = Path(r"D:\bkoi_project_1\bkoi_322\places_202609161553.csv (1)\places_202609161553.csv")

OUT_JSON = DATA_DIR / "cafes_gulshan.json"
OUT_CSV = DATA_DIR / "cafes_gulshan.csv"
PROGRESS = DATA_DIR / "progress.json"

for _d in (DATA_DIR, LOG_DIR, PROBE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── source tags ───────────────────────────────────────────────────────────
SRC_BKOI = "bkoi"    # Barikoi internal CSV  (hint only, may be wrong)
SRC_G = "g"          # Google Maps
SRC_FP = "fp"        # Foodpanda Bangladesh
SRC_TA = "ta"        # TripAdvisor
SRC_WEB = "web"      # the venue's own site / Facebook, discovered via Google

ALL_SOURCES = [SRC_G, SRC_FP, SRC_TA]

# ── politeness / pacing ───────────────────────────────────────────────────
# Per-host minimum interval between requests. This is the knob that keeps you
# un-blocked. Lowering it is how you get a 429 and lose the whole run.
HOST_MIN_INTERVAL = {
    "www.google.com": 9.0,
    "www.foodpanda.com.bd": 7.0,
    "www.tripadvisor.com": 12.0,
    "html.duckduckgo.com": 4.0,
    "_default": 5.0,
}
HOST_JITTER = 0.55          # +/- fraction of the interval, randomised
BREAKER_THRESHOLD = 3       # consecutive failures on a host before it trips
BREAKER_COOLDOWN = 420.0    # seconds a tripped host stays out of rotation
NAV_TIMEOUT_MS = 40_000
SETTLE_MS = 3_500           # after navigation, before extraction

# ── selection / targets ───────────────────────────────────────────────────
DEFAULT_POOL = 18           # shortlist size (you asked for 15-20)
DEFAULT_TARGET = 10         # accepted cafes to stop at
DEFAULT_PASSES = 3          # sweeps over the pool before giving up
GEO_TOL_M = 1_200           # metres: CSV coords vs Google's real place coords

# ── acceptance tiers ──────────────────────────────────────────────────────
# A record is ACCEPTED when every CORE field and >= MIN_RICH rich fields are
# present. Menu is a bonus, not a gate -- that was the v4 deadlock.
CORE_FIELDS = ["business_name", "latitude", "longitude", "address", "phone",
               "opening_hours"]
RICH_FIELDS = ["rating", "review_count", "review_snippets", "website"]
BONUS_FIELDS = ["menu_items", "facebook_url", "instagram_url", "foodpanda_url",
                "tripadvisor_url", "price_range"]
MIN_RICH = 3
MIN_HOURS_DAYS = 5
FIELD_WEIGHTS = {**{f: 3.0 for f in CORE_FIELDS},
                 **{f: 2.0 for f in RICH_FIELDS},
                 **{f: 1.0 for f in BONUS_FIELDS}}

# Which source wins for each resolved field, best first.
RESOLUTION_PRIORITY = {
    "address":        [SRC_G, SRC_FP, SRC_BKOI],
    "latitude":       [SRC_G, SRC_BKOI],
    "longitude":      [SRC_G, SRC_BKOI],
    "phone":          [SRC_G, SRC_FP, SRC_WEB],
    "website":        [SRC_G, SRC_WEB],
    "opening_hours":  [SRC_G, SRC_FP],
    "rating":         [SRC_G, SRC_FP, SRC_TA],
    "review_count":   [SRC_G, SRC_FP, SRC_TA],
    "review_snippets": [SRC_G, SRC_TA],
    "menu_items":     [SRC_FP, SRC_G],
    "price_range":    [SRC_G, SRC_FP, SRC_TA],
    "facebook_url":   [SRC_WEB, SRC_G],
    "instagram_url":  [SRC_WEB, SRC_G],
}

# Browser identity. One realistic desktop identity, used consistently with a
# persistent profile. Rotating UA per request while reusing cookies is a
# *stronger* bot signal than not rotating at all.
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
VIEWPORT = {"width": 1440, "height": 900}
DHAKA = {"latitude": 23.7806, "longitude": 90.4193}


# ══════════════════════════════════════════════════════════════════════════
#  SELECTORS  — patch here when a site reshuffles its DOM
# ══════════════════════════════════════════════════════════════════════════

SELECTORS = {
    "google": {
        # Stable, attribute-based. These have survived years of redesigns.
        "phone_btn":   ['button[data-item-id^="phone:tel:"]',
                        '[data-item-id^="phone:tel:"]'],
        "address_btn": ['button[data-item-id="address"]',
                        '[data-item-id="address"]'],
        "website_btn": ['a[data-item-id="authority"]',
                        '[data-item-id="authority"]'],
        "plus_code":   ['[data-item-id="oloc"]'],
        # Churning class names. Multiple fallbacks, ordered most->least stable.
        "title":       ['h1.DUwDvf', 'h1[class*="DUwDvf"]', 'div[role="main"] h1'],
        "rating":      ['div.F7nice span[aria-hidden="true"]',
                        'span[aria-label$="stars"]',
                        'div[class*="fontDisplayLarge"]'],
        "review_count": ['div.F7nice span[aria-label*="review"]',
                         'button[jsaction*="reviewChart"] span',
                         'span[aria-label*="reviews"]'],
        "category":    ['button[jsaction*="category"]', 'button.DkEaL'],
        "price_range": ['span[aria-label*="Price"]', 'span[aria-label*="price range"]'],
        "hours_toggle": ['button[data-item-id="oh"]',
                         '[jsaction*="openhours"]',
                         'button[aria-label*="Show open hours"]',
                         'div[aria-label*="Hours"]'],
        "hours_table": ['table.eK4R0e tr', 'div[class*="t39EBf"] table tr',
                        'table[aria-label*="hours" i] tr'],
        "reviews_tab": ['button[role="tab"][aria-label*="Reviews"]',
                        'button[jsaction*="moreReviews"]',
                        'button[aria-label*="Reviews for"]'],
        "review_text": ['span.wiI7pd', 'div.MyEned span', '[class*="wiI7pd"]'],
        "place_link":  ['a[href*="/maps/place/"]'],
        "scroll_panel": ['div[role="main"]', 'div[role="feed"]'],
        "consent":     ['#L2AGLb', 'button[aria-label*="Accept all" i]',
                        'form[action*="consent"] button'],
    },
    "foodpanda": {
        "name":        ['h1[data-testid="vendor-name"]', 'h1.vendor-name', 'h1'],
        "rating":      ['[data-testid="vendor-rating"]', '.rating__score',
                        'span[class*="rating"]'],
        "review_count": ['[data-testid="vendor-review-count"]', '.rating__count'],
        "product_card": ['[data-testid="menu-product"]', 'li[data-testid*="product"]',
                         '.dish-card', '[class*="product-card"]'],
        "product_name": ['[data-testid="menu-product-name"]', '.dish-card-title',
                         '[class*="product-name"]', 'h3'],
        "product_price": ['[data-testid="menu-product-price"]', '.dish-card-price',
                          '[class*="price"]'],
        "cookie":      ['#onetrust-accept-btn-handler',
                        'button[data-testid="accept-cookies"]'],
    },
    "tripadvisor": {
        "cookie":      ['#onetrust-accept-btn-handler'],
        "review_text": ['span[data-automation="reviewText"]', 'q.QewHA span',
                        'div[data-test-target="review-body"] span'],
    },
}

# ══════════════════════════════════════════════════════════════════════════
#  REGEX
# ══════════════════════════════════════════════════════════════════════════

RE_PLACE_COORDS = re.compile(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)')
RE_VIEW_COORDS = re.compile(r'@(-?\d+\.\d+),(-?\d+\.\d+)')
RE_CID = re.compile(r'!1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)')
# Candidate spans, not final numbers: groups of >=2 digits joined by at most
# two separators. Tolerates "+880 1912 345678" and "(02) 9887654" while
# refusing digit-noise like "1 3 5 7 9". norm_bd_phone() is the real filter.
RE_PHONE_CAND = re.compile(r'(?<![\d])(\+?\d{2,}(?:[\s\-().]{1,2}\d{2,}){0,4})(?![\d])')
RE_RATING = re.compile(r'\b([0-5](?:\.\d)?)\b')
RE_COUNT = re.compile(r'([\d][\d,\s]{0,9}\d|\d)')
RE_FB = re.compile(r'https?://(?:www\.|m\.|web\.)?facebook\.com/'
                   r'(?!sharer|share|tr[/?]|dialog|plugins|events/)[\w.\-]+/?')
RE_IG = re.compile(r'https?://(?:www\.)?instagram\.com/'
                   r'(?!p/|reel/|explore/|accounts/)[\w.\-]+/?')
RE_PRICE_BDT = re.compile(r'(?:৳|BDT|Tk\.?)\s*([\d,]+(?:\.\d{1,2})?)', re.I)

DAY_ORDER = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
DAY_ALIAS = {
    "monday": "mon", "mon": "mon", "mo": "mon",
    "tuesday": "tue", "tue": "tue", "tues": "tue", "tu": "tue",
    "wednesday": "wed", "wed": "wed", "we": "wed",
    "thursday": "thu", "thu": "thu", "thurs": "thu", "th": "thu",
    "friday": "fri", "fri": "fri", "fr": "fri",
    "saturday": "sat", "sat": "sat", "sa": "sat",
    "sunday": "sun", "sun": "sun", "su": "sun",
}
RE_HOURS_LINE = re.compile(
    r'\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|'
    r'Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b[^\dA-Za-z]{0,6}'
    r'(Closed|Open\s*24\s*hours|'
    r'\d{1,2}(?::\d{2})?\s*(?:AM|PM)?\s*[\u2013\u2014\-]\s*'
    r'\d{1,2}(?::\d{2})?\s*(?:AM|PM))',
    re.IGNORECASE)

BLOCK_SIGNALS = [
    "unusual traffic", "not a robot", "our systems have detected",
    "captcha", "access denied", "pardon our interruption",
    "verify you are a human", "rate limit", "too many requests",
    "enable javascript and cookies to continue",
]
CAFE_HINTS = ["cafe", "café", "coffee", "roaster", "bakery", "bake",
              "tea", "patisserie", "pastry", "brew", "espresso", "dessert"]
GULSHAN_HINTS = ["gulshan", "banani", "baridhara", "niketan"]


# ══════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════

LOG_PATH = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname).1s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("cafe")
logging.getLogger("asyncio").setLevel(logging.WARNING)


# ══════════════════════════════════════════════════════════════════════════
#  SMALL UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write(path: Path, text: str) -> None:
    """Write via tmp + os.replace so a crash never truncates the real file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    if path.exists():
        bak = path.with_suffix(path.suffix + ".bak")
        try:
            bak.unlink(missing_ok=True)
            path.replace(bak)
        except OSError:
            pass
    os.replace(tmp, path)


def is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    if isinstance(v, (list, dict, tuple, set)):
        return len(v) == 0
    return False


def haversine_m(lat1, lng1, lat2, lng2) -> float:
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


def norm_bd_phone(raw: Any) -> str | None:
    """Normalise to +8801XXXXXXXXX (mobile) or +8802XXXXXXXX (Dhaka landline)."""
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
    s = re.sub(r'\b([ap])\.?m\.?\b', lambda m: m.group(1).upper() + 'M', s, flags=re.I)
    return s


def hhmm_to_ampm(t: str) -> str:
    m = re.fullmatch(r'(\d{1,2}):(\d{2})(?::\d{2})?', str(t).strip())
    if not m:
        return str(t).strip()
    h, mi = int(m.group(1)), m.group(2)
    if h >= 24:
        h -= 24
    suffix = 'AM' if h < 12 else 'PM'
    return f"{h % 12 or 12}:{mi} {suffix}"


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
    parsed = urlparse(u)
    if not parsed.netloc:
        return None
    return u.split('?')[0].rstrip('/') if parsed.query else u.rstrip('/')


def name_tokens(s: str) -> set[str]:
    return {t for t in re.split(r'[^a-z0-9]+', str(s).lower()) if len(t) > 2}


def name_match_score(a: str, b: str) -> float:
    """Jaccard-ish overlap. Guards against merging the wrong venue."""
    ta, tb = name_tokens(a), name_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


# ══════════════════════════════════════════════════════════════════════════
#  RATE LIMITER + CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════════════

class HostGuard:
    """One limiter + breaker per host. Serialises access and backs off."""

    def __init__(self) -> None:
        self._last: dict[str, float] = {}
        self._fails: dict[str, int] = {}
        self._cooldown: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self.blocked_events: list[dict] = []

    def _lock(self, host: str) -> asyncio.Lock:
        if host not in self._locks:
            self._locks[host] = asyncio.Lock()
        return self._locks[host]

    def is_open(self, host: str) -> bool:
        """True when the breaker has tripped and the host is out of rotation."""
        until = self._cooldown.get(host, 0.0)
        if until and time.monotonic() < until:
            return True
        if until:
            self._cooldown.pop(host, None)
            self._fails[host] = 0
            log.info("   breaker reset for %s", host)
        return False

    def cooldown_remaining(self, host: str) -> float:
        return max(0.0, self._cooldown.get(host, 0.0) - time.monotonic())

    async def acquire(self, host: str) -> None:
        await self._lock(host).acquire()
        interval = HOST_MIN_INTERVAL.get(host, HOST_MIN_INTERVAL["_default"])
        interval *= random.uniform(1 - HOST_JITTER, 1 + HOST_JITTER)
        elapsed = time.monotonic() - self._last.get(host, 0.0)
        if elapsed < interval:
            await asyncio.sleep(interval - elapsed)

    def release(self, host: str) -> None:
        self._last[host] = time.monotonic()
        lk = self._locks.get(host)
        if lk and lk.locked():
            lk.release()

    def ok(self, host: str) -> None:
        self._fails[host] = 0

    def fail(self, host: str, reason: str, hard: bool = False) -> None:
        n = self._fails.get(host, 0) + (BREAKER_THRESHOLD if hard else 1)
        self._fails[host] = n
        if n >= BREAKER_THRESHOLD:
            self._cooldown[host] = time.monotonic() + BREAKER_COOLDOWN
            self.blocked_events.append(
                {"host": host, "reason": reason, "at": now_iso()})
            log.warning("   BREAKER TRIPPED on %s (%s) -- cooling %.0fs",
                        host, reason, BREAKER_COOLDOWN)


GUARD = HostGuard()


def detect_block(url: str, body: str) -> str | None:
    """Return a reason string if this page is a block/consent/captcha wall."""
    u = (url or "").lower()
    if "/sorry/" in u or "consent.google" in u or "captcha" in u:
        return f"block-url:{u[:60]}"
    low = (body or "")[:6000].lower()
    for sig in BLOCK_SIGNALS:
        if sig in low:
            return f"block-text:{sig}"
    if len((body or "").strip()) < 120:
        return "empty-body"
    return None


# ══════════════════════════════════════════════════════════════════════════
#  RECORD SCHEMA
# ══════════════════════════════════════════════════════════════════════════

RESOLVED_FIELDS = CORE_FIELDS + RICH_FIELDS + BONUS_FIELDS + [
    "google_maps_url", "google_cid", "plus_code", "category",
]


def blank_source() -> dict:
    return {"fetched_at": None, "url": None, "status": "pending", "data": {}}


def blank_record(cafe: dict) -> dict:
    """
    Three layers:
      bkoi{}       -- raw Barikoi CSV, never overwritten, treated as a hint
      sources{}    -- one block per platform, raw + tagged + timestamped
      resolved{}   -- best-of flat view, matches your sample output shape
      provenance{} -- resolved field -> which source tag supplied it
    """
    return {
        "place_code": cafe["place_code"],
        "business_name": cafe["business_name"],
        "bkoi": {
            "address": cafe.get("bkoi_address"),
            "latitude": cafe.get("bkoi_lat"),
            "longitude": cafe.get("bkoi_lng"),
            "sub_area": cafe.get("bkoi_sub_area"),
            "area": cafe.get("bkoi_area"),
            "popularity_ranking": cafe.get("bkoi_popularity"),
            "type": cafe.get("bkoi_type"),
        },
        "sources": {s: blank_source() for s in (SRC_G, SRC_FP, SRC_TA, SRC_WEB)},
        "resolved": {f: ([] if f in ("review_snippets", "menu_items") else None)
                     for f in RESOLVED_FIELDS},
        "provenance": {},
        "meta": {
            "first_seen": now_iso(),
            "last_attempt": None,
            "attempts": 0,
            "completeness": 0.0,
            "accepted": False,
            "status": "pending",
            "notes": [],
        },
    }


# ══════════════════════════════════════════════════════════════════════════
#  RESOLUTION + SCORING
# ══════════════════════════════════════════════════════════════════════════

def resolve_record(rec: dict) -> dict:
    """Collapse sources{} into resolved{} using RESOLUTION_PRIORITY."""
    res = rec["resolved"]
    prov = rec["provenance"]
    buckets = {s: rec["sources"].get(s, {}).get("data", {}) for s in rec["sources"]}
    buckets[SRC_BKOI] = rec.get("bkoi", {})

    res["business_name"] = rec["business_name"]

    for field, order in RESOLUTION_PRIORITY.items():
        if field in ("review_snippets", "menu_items"):
            continue   # accumulated across sources below
        for src in order:
            val = buckets.get(src, {}).get(field)
            if not is_empty(val):
                if field == "opening_hours" and len(val) < MIN_HOURS_DAYS:
                    continue   # keep looking for a fuller week
                res[field] = val
                prov[field] = src
                break

    # Accumulate list fields from every source, tagging each entry.
    for field in ("review_snippets", "menu_items"):
        merged, seen = [], set()
        for src in RESOLUTION_PRIORITY.get(field, ALL_SOURCES):
            for item in buckets.get(src, {}).get(field, []) or []:
                if isinstance(item, dict):
                    item = {**item, "source_tag": item.get("source_tag", src)}
                    key = json.dumps({k: v for k, v in item.items()
                                      if k != "source_tag"}, sort_keys=True)
                else:
                    item = {"text": str(item), "source_tag": src}
                    key = item["text"][:120].lower()
                if key in seen:
                    continue
                seen.add(key)
                merged.append(item)
        res[field] = merged
        if merged:
            prov[field] = "+".join(sorted({m["source_tag"] for m in merged}))

    # Direct platform URLs, always tagged.
    res["foodpanda_url"] = rec["sources"].get(SRC_FP, {}).get("url")
    res["tripadvisor_url"] = rec["sources"].get(SRC_TA, {}).get("url")
    res["google_maps_url"] = rec["sources"].get(SRC_G, {}).get("url")
    for k in ("google_cid", "plus_code", "category"):
        v = buckets.get(SRC_G, {}).get(k)
        if not is_empty(v):
            res[k] = v
            prov[k] = SRC_G

    return rec


def field_present(res: dict, field: str) -> bool:
    v = res.get(field)
    if is_empty(v):
        return False
    if field == "opening_hours":
        return len(v) >= MIN_HOURS_DAYS
    return True


def score_record(rec: dict) -> dict:
    res = rec["resolved"]
    got = sum(FIELD_WEIGHTS[f] for f in FIELD_WEIGHTS if field_present(res, f))
    total = sum(FIELD_WEIGHTS.values())
    rec["meta"]["completeness"] = round(got / total, 4)

    core_ok = all(field_present(res, f) for f in CORE_FIELDS)
    rich_n = sum(1 for f in RICH_FIELDS if field_present(res, f))
    rec["meta"]["accepted"] = bool(core_ok and rich_n >= MIN_RICH)
    return rec


def missing_report(rec: dict) -> list[str]:
    res = rec["resolved"]
    out = [f"core:{f}" for f in CORE_FIELDS if not field_present(res, f)]
    rich_n = sum(1 for f in RICH_FIELDS if field_present(res, f))
    if rich_n < MIN_RICH:
        out.append(f"rich:{rich_n}/{MIN_RICH}"
                   f"({','.join(f for f in RICH_FIELDS if not field_present(res, f))})")
    return out


def merge_source(rec: dict, src: str, payload: dict, url: str | None,
                 status: str) -> dict:
    """Never discard good data: new non-empty values win, old ones survive."""
    block = rec["sources"].setdefault(src, blank_source())
    block["fetched_at"] = now_iso()
    block["status"] = status
    if url:
        block["url"] = url
    data = block.setdefault("data", {})
    for k, v in (payload or {}).items():
        if is_empty(v):
            continue
        old = data.get(k)
        if isinstance(v, list) and isinstance(old, list):
            seen = {json.dumps(x, sort_keys=True, default=str) for x in old}
            for item in v:
                key = json.dumps(item, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    old.append(item)
        elif isinstance(v, dict) and isinstance(old, dict):
            old.update({k2: v2 for k2, v2 in v.items() if not is_empty(v2)})
        else:
            data[k] = v
    return rec


# ══════════════════════════════════════════════════════════════════════════
#  PERSISTENCE  — one json + one csv, merged every run
# ══════════════════════════════════════════════════════════════════════════

CSV_COLUMNS = [
    "place_code", "business_name",
    "bkoi_address", "bkoi_latitude", "bkoi_longitude", "bkoi_sub_area",
    "bkoi_popularity_ranking",
    "address", "latitude", "longitude", "phone", "website",
    "google_maps_url", "google_cid", "plus_code", "category", "price_range",
    "facebook_url", "instagram_url", "foodpanda_url", "tripadvisor_url",
    "opening_hours",
    "g_rating", "g_review_count",
    "fp_rating", "fp_review_count",
    "ta_rating", "ta_review_count",
    "rating_resolved", "review_count_resolved",
    "review_snippets", "menu_items", "menu_item_count",
    "src_address", "src_phone", "src_opening_hours", "src_rating", "src_menu_items",
    "sources_ok", "completeness", "accepted", "status", "attempts", "last_attempt",
]


class Store:
    def __init__(self, json_path: Path, csv_path: Path, prog_path: Path):
        self.json_path, self.csv_path, self.prog_path = json_path, csv_path, prog_path
        self.records: dict[str, dict] = self._load_json()
        self.progress: dict = self._load_progress()
        log.info("Store: %d existing records | accepted=%d",
                 len(self.records), self.accepted_count)

    def _load_json(self) -> dict[str, dict]:
        for p in (self.json_path, self.json_path.with_suffix(".json.bak")):
            if not p.exists():
                continue
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                recs = raw.get("records", raw) if isinstance(raw, dict) else raw
                return {r["place_code"]: r for r in recs if r.get("place_code")}
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                log.warning("Could not read %s (%s) -- trying backup", p.name, e)
        return {}

    def _load_progress(self) -> dict:
        base = {"runs": [], "accepted": [], "dead": [], "attempts": {}}
        if self.prog_path.exists():
            try:
                base.update(json.loads(self.prog_path.read_text(encoding="utf-8")))
            except json.JSONDecodeError:
                log.warning("progress.json unreadable -- starting a fresh ledger")
        base.setdefault("attempts", {})
        return base

    @property
    def accepted_count(self) -> int:
        return sum(1 for r in self.records.values() if r["meta"]["accepted"])

    def get(self, place_code: str) -> dict | None:
        return self.records.get(place_code)

    def upsert(self, rec: dict) -> None:
        self.records[rec["place_code"]] = rec

    def flush(self) -> None:
        recs = sorted(self.records.values(),
                      key=lambda r: (not r["meta"]["accepted"],
                                     -r["meta"]["completeness"]))
        payload = {
            "generated_at": now_iso(),
            "schema_version": 5,
            "source_tags": {
                SRC_BKOI: "Barikoi internal CSV (hint only)",
                SRC_G: "Google Maps",
                SRC_FP: "Foodpanda Bangladesh",
                SRC_TA: "TripAdvisor",
                SRC_WEB: "venue website / social, discovered via search",
            },
            "counts": {
                "total": len(recs),
                "accepted": sum(1 for r in recs if r["meta"]["accepted"]),
            },
            "records": recs,
        }
        atomic_write(self.json_path,
                     json.dumps(payload, ensure_ascii=False, indent=2))
        self._write_csv(recs)
        atomic_write(self.prog_path,
                     json.dumps(self.progress, ensure_ascii=False, indent=2))

    def _write_csv(self, recs: list[dict]) -> None:
        rows = []
        for r in recs:
            res, prov, b = r["resolved"], r["provenance"], r["bkoi"]
            g = r["sources"].get(SRC_G, {}).get("data", {})
            fp = r["sources"].get(SRC_FP, {}).get("data", {})
            ta = r["sources"].get(SRC_TA, {}).get("data", {})
            ok = [s for s, blk in r["sources"].items()
                  if blk.get("status") == "ok"]
            rows.append({
                "place_code": r["place_code"],
                "business_name": r["business_name"],
                "bkoi_address": b.get("address"),
                "bkoi_latitude": b.get("latitude"),
                "bkoi_longitude": b.get("longitude"),
                "bkoi_sub_area": b.get("sub_area"),
                "bkoi_popularity_ranking": b.get("popularity_ranking"),
                "address": res.get("address"),
                "latitude": res.get("latitude"),
                "longitude": res.get("longitude"),
                "phone": res.get("phone"),
                "website": res.get("website"),
                "google_maps_url": res.get("google_maps_url"),
                "google_cid": res.get("google_cid"),
                "plus_code": res.get("plus_code"),
                "category": res.get("category"),
                "price_range": res.get("price_range"),
                "facebook_url": res.get("facebook_url"),
                "instagram_url": res.get("instagram_url"),
                "foodpanda_url": res.get("foodpanda_url"),
                "tripadvisor_url": res.get("tripadvisor_url"),
                "opening_hours": json.dumps(res.get("opening_hours") or {},
                                            ensure_ascii=False),
                "g_rating": g.get("rating"),
                "g_review_count": g.get("review_count"),
                "fp_rating": fp.get("rating"),
                "fp_review_count": fp.get("review_count"),
                "ta_rating": ta.get("rating"),
                "ta_review_count": ta.get("review_count"),
                "rating_resolved": res.get("rating"),
                "review_count_resolved": res.get("review_count"),
                "review_snippets": json.dumps(res.get("review_snippets") or [],
                                              ensure_ascii=False),
                "menu_items": json.dumps(res.get("menu_items") or [],
                                         ensure_ascii=False),
                "menu_item_count": len(res.get("menu_items") or []),
                "src_address": prov.get("address"),
                "src_phone": prov.get("phone"),
                "src_opening_hours": prov.get("opening_hours"),
                "src_rating": prov.get("rating"),
                "src_menu_items": prov.get("menu_items"),
                "sources_ok": "+".join(sorted(ok)),
                "completeness": r["meta"]["completeness"],
                "accepted": r["meta"]["accepted"],
                "status": r["meta"]["status"],
                "attempts": r["meta"]["attempts"],
                "last_attempt": r["meta"]["last_attempt"],
            })
        tmp = self.csv_path.with_suffix(".csv.tmp")
        with tmp.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, self.csv_path)


# ══════════════════════════════════════════════════════════════════════════
#  CANDIDATE SELECTION
# ══════════════════════════════════════════════════════════════════════════

def _col(df: pd.DataFrame, *names: str) -> str | None:
    lower = {c.lower().strip(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def load_candidates(csv_path: Path, pool: int, rank_order: str) -> list[dict]:
    if not csv_path.exists():
        log.error("CSV not found: %s", csv_path)
        sys.exit(2)

    df = pd.read_csv(csv_path)
    log.info("CSV columns: %s", list(df.columns))

    c_code = _col(df, "place_code", "uCode", "id")
    c_name = _col(df, "business_name", "name", "place_name")
    c_addr = _col(df, "address", "Address", "full_address")
    c_lat = _col(df, "latitude", "lat")
    c_lng = _col(df, "longitude", "lng", "lon")
    c_sub = _col(df, "sub_area", "subarea", "sub_Area")
    c_area = _col(df, "area", "Area")
    c_pop = _col(df, "popularity_ranking", "popularity", "rank")
    c_type = _col(df, "type", "pType", "category", "sub_type")

    if not (c_name and c_lat and c_lng):
        log.error("CSV must contain at least name, latitude and longitude columns.")
        sys.exit(2)

    df = df.dropna(subset=[c_name, c_lat, c_lng])
    before = len(df)

    # ── Gulshan filter ────────────────────────────────────────────────────
    hay = df[c_name].astype(str)
    for c in (c_addr, c_sub, c_area):
        if c:
            hay = hay + " " + df[c].astype(str)
    geo_mask = hay.str.lower().str.contains("|".join(GULSHAN_HINTS), na=False)
    if geo_mask.sum() >= 5:
        df = df[geo_mask]
        log.info("Gulshan filter: %d -> %d rows", before, len(df))
    else:
        log.warning("Gulshan filter matched only %d rows -- keeping all %d. "
                    "Check your sub_area/address columns.", int(geo_mask.sum()), before)

    # ── cafe filter ───────────────────────────────────────────────────────
    hay2 = df[c_name].astype(str)
    if c_type:
        hay2 = hay2 + " " + df[c_type].astype(str)
    cafe_mask = hay2.str.lower().str.contains("|".join(CAFE_HINTS), na=False)
    if cafe_mask.sum() >= 5:
        n0 = len(df)
        df = df[cafe_mask]
        log.info("Cafe filter: %d -> %d rows", n0, len(df))
    else:
        log.warning("Cafe filter matched only %d rows -- keeping all. "
                    "Widen CAFE_HINTS if your CSV uses different wording.",
                    int(cafe_mask.sum()))

    df = df.drop_duplicates(subset=[c_name]).reset_index(drop=True)

    # ── popularity ordering ───────────────────────────────────────────────
    # v4 used ascending=False unconditionally. If popularity_ranking is a
    # RANK (1 = most popular) that sorted the LEAST popular cafes first.
    if c_pop and df[c_pop].notna().any():
        vals = pd.to_numeric(df[c_pop], errors="coerce").dropna()
        if rank_order == "auto":
            looks_like_rank = bool(len(vals) and vals.min() <= 2 and
                                   (vals % 1 == 0).all() and vals.max() >= len(vals) * 0.5)
            ascending = looks_like_rank
            log.info("popularity_ranking range %.2f-%.2f -> treating as %s "
                     "(override with --rank-order)", vals.min(), vals.max(),
                     "RANK, 1=best" if ascending else "SCORE, high=best")
        else:
            ascending = (rank_order == "rank")
        df["_pop"] = pd.to_numeric(df[c_pop], errors="coerce")
        df = df.sort_values("_pop", ascending=ascending, na_position="last")
    else:
        log.warning("No popularity column found -- keeping CSV order.")

    out: list[dict] = []
    for i, (_, row) in enumerate(df.iterrows()):
        if i >= pool:
            break
        lat, lng = to_float(row[c_lat]), to_float(row[c_lng])
        if lat is None or lng is None:
            continue
        code = str(row[c_code]) if c_code else f"AUTO{i:05d}"
        out.append({
            "place_code": code,
            "business_name": str(row[c_name]).strip(),
            "bkoi_address": str(row[c_addr]).strip() if c_addr else None,
            "bkoi_lat": lat,
            "bkoi_lng": lng,
            "bkoi_sub_area": str(row[c_sub]).strip() if c_sub else "Gulshan",
            "bkoi_area": str(row[c_area]).strip() if c_area else "Dhaka",
            "bkoi_popularity": to_float(row[c_pop]) if c_pop else None,
            "bkoi_type": str(row[c_type]).strip() if c_type else None,
        })

    log.info("Shortlist: %d cafes", len(out))
    for i, c in enumerate(out, 1):
        log.info("   %2d. %-38s pop=%-8s %s", i, c["business_name"][:38],
                 c["bkoi_popularity"], c["place_code"])
    return out


# ══════════════════════════════════════════════════════════════════════════
#  BROWSER
# ══════════════════════════════════════════════════════════════════════════

async def launch_context(pw, headless: bool):
    """
    Persistent profile: cookies survive between runs, which is the single
    biggest win against Google's consent wall. No fingerprint spoofing.
    """
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    ctx = await pw.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=headless,
        user_agent=UA,
        viewport=VIEWPORT,
        locale="en-US",
        timezone_id="Asia/Dhaka",
        geolocation=DHAKA,
        permissions=["geolocation"],
        args=["--disable-dev-shm-usage", "--no-first-run",
              "--no-default-browser-check", "--lang=en-US"],
    )
    ctx.set_default_timeout(15_000)
    ctx.set_default_navigation_timeout(NAV_TIMEOUT_MS)
    # Drop heavy media we never parse. Keeps CSS/JS so pages render correctly.
    async def _block(route):
        try:
            await route.abort()
        except Exception:
            pass

    await ctx.route(re.compile(r'\.(woff2?|ttf|otf|eot|mp4|webm|avi)(\?|$)'), _block)
    return ctx


async def goto(page, url: str, *, host: str, wait: str = "domcontentloaded") -> str | None:
    """Rate-limited navigation with block detection. Returns body text or None."""
    if GUARD.is_open(host):
        log.info("   skip %s -- breaker open for %.0fs more",
                 host, GUARD.cooldown_remaining(host))
        return None
    await GUARD.acquire(host)
    try:
        resp = await page.goto(url, wait_until=wait, timeout=NAV_TIMEOUT_MS)
        if resp is not None and resp.status in (403, 429, 503):
            GUARD.fail(host, f"http-{resp.status}", hard=(resp.status == 429))
            return None
        await page.wait_for_timeout(SETTLE_MS + random.randint(0, 1800))
        try:
            body = await page.inner_text("body", timeout=8000)
        except PWTimeout:
            body = await page.content()
        reason = detect_block(page.url, body)
        if reason:
            GUARD.fail(host, reason, hard=reason.startswith("block"))
            log.warning("   %s -> %s", host, reason)
            return None
        GUARD.ok(host)
        return body
    except PWTimeout:
        GUARD.fail(host, "nav-timeout")
        log.warning("   %s -> navigation timeout", host)
        return None
    except Exception as e:                       # noqa: BLE001
        GUARD.fail(host, type(e).__name__)
        log.warning("   %s -> %s: %s", host, type(e).__name__, str(e)[:110])
        return None
    finally:
        GUARD.release(host)


async def first_text(page, selectors: Iterable[str], min_len: int = 1) -> str | None:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                t = (await loc.inner_text(timeout=2500)).strip()
                if len(t) >= min_len:
                    return t
        except Exception:
            continue
    return None


async def first_attr(page, selectors: Iterable[str], attr: str) -> str | None:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                v = await loc.get_attribute(attr, timeout=2500)
                if v:
                    return v
        except Exception:
            continue
    return None


async def click_first(page, selectors: Iterable[str], settle_ms: int = 1500) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count():
                await loc.click(timeout=4000)
                await page.wait_for_timeout(settle_ms)
                return True
        except Exception:
            continue
    return False


async def human_scroll(page, selectors: Iterable[str], steps: int = 6) -> None:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if not await loc.count():
                continue
            for _ in range(steps):
                await loc.evaluate("el => el.scrollBy(0, el.clientHeight * 0.85)")
                await page.wait_for_timeout(random.randint(450, 1100))
            return
        except Exception:
            continue
    for _ in range(steps):
        try:
            await page.mouse.wheel(0, random.randint(600, 1100))
            await page.wait_for_timeout(random.randint(400, 900))
        except Exception:
            return


async def jsonld_nodes(page) -> list[dict]:
    """
    Structured data the site publishes *for machines*. Far more stable than
    CSS classes, and the right thing to read when it's offered.
    """
    out: list[dict] = []
    try:
        raws = await page.locator(
            'script[type="application/ld+json"]').all_text_contents()
    except Exception:
        return out
    for raw in raws:
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        stack: list[Any] = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                out.append(node)
                stack.extend(v for v in node.values() if isinstance(v, (list, dict)))
    return out


def pick_business_node(nodes: list[dict]) -> dict | None:
    wanted = {"restaurant", "cafeorcoffeeshop", "foodestablishment",
              "localbusiness", "bakery", "store", "organization"}
    for node in nodes:
        t = node.get("@type")
        types = {str(x).lower() for x in (t if isinstance(t, list) else [t])}
        if types & wanted:
            return node
    return None


def parse_schema_hours(node: dict) -> dict:
    """Handle both openingHours strings and openingHoursSpecification objects."""
    hours: dict[str, str] = {}

    spec = node.get("openingHoursSpecification")
    if spec:
        for s in (spec if isinstance(spec, list) else [spec]):
            if not isinstance(s, dict):
                continue
            dow = s.get("dayOfWeek")
            days = dow if isinstance(dow, list) else [dow]
            opens, closes = s.get("opens"), s.get("closes")
            for d in days:
                key = DAY_ALIAS.get(str(d).rstrip('/').split('/')[-1].lower())
                if not key:
                    continue
                if opens and closes:
                    hours[key] = f"{hhmm_to_ampm(opens)} \u2013 {hhmm_to_ampm(closes)}"
                else:
                    hours[key] = "Closed"

    raw = node.get("openingHours")
    if raw and not hours:
        for line in (raw if isinstance(raw, list) else [raw]):
            m = re.match(r'\s*([A-Za-z]{2,9})\s*(?:-\s*([A-Za-z]{2,9}))?\s+'
                         r'(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})', str(line))
            if not m:
                continue
            d1 = DAY_ALIAS.get((m.group(1) or "").lower())
            d2 = DAY_ALIAS.get((m.group(2) or "").lower()) if m.group(2) else None
            val = f"{hhmm_to_ampm(m.group(3))} \u2013 {hhmm_to_ampm(m.group(4))}"
            if d1 and d2:
                i, j = DAY_ORDER.index(d1), DAY_ORDER.index(d2)
                span = DAY_ORDER[i:j + 1] if i <= j else DAY_ORDER[i:] + DAY_ORDER[:j + 1]
                for d in span:
                    hours[d] = val
            elif d1:
                hours[d1] = val
    return hours


# ══════════════════════════════════════════════════════════════════════════
#  SEARCH HELPER  (finding Foodpanda / TripAdvisor URLs)
# ══════════════════════════════════════════════════════════════════════════

async def find_url(page, query: str, host_contains: str,
                   path_must_contain: str | None = None) -> str | None:
    """
    DuckDuckGo's HTML endpoint -- no JS, light, and it does not need an
    account. Used only to *discover* a vendor page URL, nothing more.
    """
    url = "https://html.duckduckgo.com/html/?q=" + quote_plus(query)
    body = await goto(page, url, host="html.duckduckgo.com")
    if body is None:
        return None
    try:
        hrefs = await page.locator("a[href]").evaluate_all(
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)")
    except Exception:
        return None
    for href in hrefs:
        target = href
        if "duckduckgo.com/l/" in href or href.startswith("//duckduckgo.com/l/"):
            qs = parse_qs(urlparse("https:" + href if href.startswith("//") else href).query)
            target = (qs.get("uddg") or [None])[0]
        if not target or not target.startswith("http"):
            continue
        parsed = urlparse(target)
        if host_contains not in parsed.netloc:
            continue
        if path_must_contain and path_must_contain not in parsed.path:
            continue
        return target.split('?')[0]
    return None


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 1 · GOOGLE MAPS   (tag: g_)
# ══════════════════════════════════════════════════════════════════════════

async def scrape_google(page, cafe: dict) -> tuple[dict, str | None, str]:
    host = "www.google.com"
    name = cafe["business_name"]
    area = cafe.get("bkoi_sub_area") or "Gulshan"
    query = f"{name}, {area}, Dhaka, Bangladesh"
    url = ("https://www.google.com/maps/search/" + quote_plus(query) +
           f"/@{cafe['bkoi_lat']},{cafe['bkoi_lng']},17z?hl=en")

    body = await goto(page, url, host=host)
    if body is None:
        return {}, None, "blocked_or_error"

    await click_first(page, SELECTORS["google"]["consent"], 2000)

    # A search may land on the results feed or straight on a place panel.
    if "/maps/place/" not in page.url:
        try:
            link = page.locator(SELECTORS["google"]["place_link"][0]).first
            if await link.count():
                await link.click(timeout=6000)
                await page.wait_for_timeout(SETTLE_MS)
        except Exception:
            pass

    if "/maps/place/" not in page.url:
        return {}, page.url, "not_found"

    data: dict[str, Any] = {}
    place_url = page.url

    # ── coordinates: the REAL place coords, not the viewport centre ───────
    m = RE_PLACE_COORDS.search(place_url)
    if not m:
        try:
            html = await page.content()
            m = RE_PLACE_COORDS.search(html)
        except Exception:
            m = None
    if m:
        data["latitude"], data["longitude"] = float(m.group(1)), float(m.group(2))
    else:
        mv = RE_VIEW_COORDS.search(place_url)
        if mv:
            data["latitude"], data["longitude"] = float(mv.group(1)), float(mv.group(2))
            log.info("   [g] only viewport coords available (less precise)")

    mc = RE_CID.search(place_url)
    if mc:
        data["google_cid"] = mc.group(1)

    # ── identity guard: right pin, right name ─────────────────────────────
    title = await first_text(page, SELECTORS["google"]["title"])
    if title:
        data["matched_name"] = title
        sim = name_match_score(name, title)
        data["name_match"] = round(sim, 3)
        if sim < 0.2:
            log.warning("   [g] name mismatch: '%s' vs '%s' (%.2f)",
                        name, title, sim)
    if data.get("latitude") is not None:
        dist = haversine_m(cafe["bkoi_lat"], cafe["bkoi_lng"],
                           data["latitude"], data["longitude"])
        data["distance_from_bkoi_m"] = round(dist)
        if dist > GEO_TOL_M and (data.get("name_match") or 0) < 0.45:
            log.warning("   [g] %.0fm from Barikoi pin AND weak name match "
                        "-- rejecting listing", dist)
            return {}, place_url, "geo_mismatch"

    # ── attribute-keyed fields (stable) ───────────────────────────────────
    raw_phone = await first_attr(page, SELECTORS["google"]["phone_btn"], "data-item-id")
    if raw_phone:
        data["phone"] = norm_bd_phone(raw_phone.replace("phone:tel:", ""))
    addr = await first_text(page, SELECTORS["google"]["address_btn"], min_len=8)
    if addr:
        data["address"] = re.sub(r'\s+', ' ', addr.replace("Address:", "")).strip()
    site = await first_attr(page, SELECTORS["google"]["website_btn"], "href")
    site = clean_url(site)
    if site and "google." not in urlparse(site).netloc:
        data["website"] = site
    pluscode = await first_text(page, SELECTORS["google"]["plus_code"], min_len=4)
    if pluscode:
        data["plus_code"] = pluscode.replace("Plus code:", "").strip()
    cat = await first_text(page, SELECTORS["google"]["category"], min_len=3)
    if cat:
        data["category"] = cat.strip()

    # ── rating / review count ─────────────────────────────────────────────
    rtxt = await first_text(page, SELECTORS["google"]["rating"])
    if rtxt:
        mr = RE_RATING.search(rtxt.replace(',', '.'))
        if mr:
            val = to_float(mr.group(1))
            if val and 0 < val <= 5:
                data["rating"] = val
    ctxt = await first_text(page, SELECTORS["google"]["review_count"])
    if not ctxt:
        ctxt = await first_attr(page, SELECTORS["google"]["review_count"], "aria-label")
    n = to_int(ctxt)
    if n:
        data["review_count"] = n
    if "rating" not in data:
        mb = re.search(r'([0-5]\.\d)\s*\(?\s*([\d,]+)', body)
        if mb:
            data.setdefault("rating", to_float(mb.group(1)))
            data.setdefault("review_count", to_int(mb.group(2)))

    price = await first_attr(page, SELECTORS["google"]["price_range"], "aria-label")
    if price:
        data["price_range"] = price.strip()

    # ── opening hours ─────────────────────────────────────────────────────
    hours: dict[str, str] = {}
    await click_first(page, SELECTORS["google"]["hours_toggle"], 1800)
    for tbl_sel in SELECTORS["google"]["hours_table"]:
        try:
            rows = await page.locator(tbl_sel).all()
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
        lbl = await first_attr(page, SELECTORS["google"]["hours_toggle"], "aria-label") or ""
        for chunk in (lbl, body):
            for mh in RE_HOURS_LINE.finditer(chunk):
                key = DAY_ALIAS.get(re.sub(r'[^a-z]', '', mh.group(1).lower()))
                if key and key not in hours:
                    hours[key] = norm_hours_value(mh.group(2))
            if len(hours) >= MIN_HOURS_DAYS:
                break
    if hours:
        data["opening_hours"] = {d: hours[d] for d in DAY_ORDER if d in hours}

    # ── reviews ───────────────────────────────────────────────────────────
    snippets: list[dict] = []
    if await click_first(page, SELECTORS["google"]["reviews_tab"], 2600):
        await human_scroll(page, SELECTORS["google"]["scroll_panel"], steps=4)
        try:
            for btn in (await page.locator(
                    'button[aria-label="See more"], button:has-text("More")').all())[:8]:
                try:
                    await btn.click(timeout=1200)
                except Exception:
                    pass
        except Exception:
            pass
        for sel in SELECTORS["google"]["review_text"]:
            try:
                texts = await page.locator(sel).all_inner_texts()
            except Exception:
                continue
            for t in texts[:6]:
                t = re.sub(r'\s+', ' ', t).strip()
                if len(t) > 25:
                    snippets.append({"text": t[:450], "source_tag": "g_review"})
            if snippets:
                break
    if snippets:
        data["review_snippets"] = snippets[:5]

    # ── socials from the rendered page ────────────────────────────────────
    try:
        html = await page.content()
    except Exception:
        html = body
    mfb = RE_FB.search(html)
    if mfb:
        data["facebook_url"] = clean_url(mfb.group(0))
    mig = RE_IG.search(html)
    if mig:
        data["instagram_url"] = clean_url(mig.group(0))

    if "phone" not in data:
        phones = find_phones(body)
        if phones:
            data["phone"] = phones[0]

    return data, place_url, "ok"


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 2 · FOODPANDA   (tag: fp_)  — this is where MENUS live
# ══════════════════════════════════════════════════════════════════════════

async def scrape_foodpanda(page, cafe: dict,
                           known_url: str | None = None) -> tuple[dict, str | None, str]:
    host = "www.foodpanda.com.bd"
    name = cafe["business_name"]

    url = known_url
    if not url:
        url = await find_url(page, f'site:foodpanda.com.bd "{name}" Gulshan Dhaka',
                             "foodpanda.com.bd", "/restaurant/")
    if not url:
        url = await find_url(page, f'foodpanda Bangladesh {name} Gulshan',
                             "foodpanda.com.bd", "/restaurant/")
    if not url:
        return {}, None, "not_found"

    body = await goto(page, url, host=host)
    if body is None:
        return {}, url, "blocked_or_error"

    await click_first(page, SELECTORS["foodpanda"]["cookie"], 1200)
    data: dict[str, Any] = {}

    # ── JSON-LD first: structured, stable, intended for machines ──────────
    node = pick_business_node(await jsonld_nodes(page))
    if node:
        vendor_name = str(node.get("name") or "")
        if vendor_name and name_match_score(name, vendor_name) < 0.2:
            log.warning("   [fp] vendor '%s' does not match '%s' -- skipping",
                        vendor_name[:40], name)
            return {}, url, "name_mismatch"
        data["matched_name"] = vendor_name or None

        agg = node.get("aggregateRating") or {}
        if isinstance(agg, dict):
            r = to_float(agg.get("ratingValue"))
            if r and 0 < r <= 5:
                data["rating"] = r
            n = to_int(agg.get("reviewCount") or agg.get("ratingCount"))
            if n:
                data["review_count"] = n

        addr = node.get("address")
        if isinstance(addr, dict):
            parts = [addr.get(k) for k in ("streetAddress", "addressLocality",
                                           "addressRegion", "postalCode")]
            joined = ", ".join(str(p) for p in parts if p)
            if len(joined) > 8:
                data["address"] = joined
        elif isinstance(addr, str) and len(addr) > 8:
            data["address"] = addr

        tel = norm_bd_phone(node.get("telephone"))
        if tel:
            data["phone"] = tel

        geo = node.get("geo")
        if isinstance(geo, dict):
            la, lo = to_float(geo.get("latitude")), to_float(geo.get("longitude"))
            if la and lo:
                data["latitude"], data["longitude"] = la, lo

        sh = parse_schema_hours(node)
        if sh:
            data["opening_hours"] = {d: sh[d] for d in DAY_ORDER if d in sh}

        pr = node.get("priceRange")
        if pr:
            data["price_range"] = str(pr).strip()

    if not data.get("rating"):
        rt = await first_text(page, SELECTORS["foodpanda"]["rating"])
        if rt:
            mr = RE_RATING.search(rt)
            if mr:
                v = to_float(mr.group(1))
                if v and 0 < v <= 5:
                    data["rating"] = v
    if not data.get("review_count"):
        ct = await first_text(page, SELECTORS["foodpanda"]["review_count"])
        n = to_int(ct)
        if n:
            data["review_count"] = n

    # ── MENU: the reason Foodpanda is in this pipeline at all ─────────────
    items: list[dict] = []
    await human_scroll(page, ['[data-testid="menu"]', "main"], steps=7)
    for card_sel in SELECTORS["foodpanda"]["product_card"]:
        try:
            cards = await page.locator(card_sel).all()
        except Exception:
            continue
        if not cards:
            continue
        for card in cards[:70]:
            try:
                txt = re.sub(r'\s+', ' ', (await card.inner_text(timeout=900))).strip()
            except Exception:
                continue
            if len(txt) < 3:
                continue
            item_name = None
            for nsel in SELECTORS["foodpanda"]["product_name"]:
                try:
                    loc = card.locator(nsel).first
                    if await loc.count():
                        item_name = (await loc.inner_text(timeout=600)).strip()
                        break
                except Exception:
                    continue
            price = None
            mp = RE_PRICE_BDT.search(txt)
            if mp:
                price = to_float(mp.group(1))
            if not item_name:
                item_name = RE_PRICE_BDT.sub('', txt).strip().split('\n')[0]
            item_name = re.sub(r'\s+', ' ', item_name)[:80].strip(' -,')
            if item_name and 2 < len(item_name) < 80:
                items.append({"item": item_name, "price_bdt": price,
                              "currency": "BDT", "source_tag": "fp_menu"})
        if items:
            break

    # de-dupe by lowercase name
    seen, deduped = set(), []
    for it in items:
        k = it["item"].lower()
        if k not in seen:
            seen.add(k)
            deduped.append(it)
    if deduped:
        data["menu_items"] = deduped[:60]
        log.info("   [fp] %d menu items", len(deduped))

    if not data.get("phone"):
        phones = find_phones(body)
        if phones:
            data["phone"] = phones[0]

    return data, url, "ok" if data else "empty"


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 3 · TRIPADVISOR   (tag: ta_)  — best effort, blocks aggressively
# ══════════════════════════════════════════════════════════════════════════

async def scrape_tripadvisor(page, cafe: dict) -> tuple[dict, str | None, str]:
    host = "www.tripadvisor.com"
    name = cafe["business_name"]

    url = await find_url(page, f'site:tripadvisor.com "{name}" Dhaka restaurant',
                         "tripadvisor.", "Restaurant_Review")
    if not url:
        return {}, None, "not_found"

    body = await goto(page, url, host=host)
    if body is None:
        return {}, url, "blocked_or_error"

    await click_first(page, SELECTORS["tripadvisor"]["cookie"], 1200)
    data: dict[str, Any] = {}

    node = pick_business_node(await jsonld_nodes(page))
    if node:
        vendor = str(node.get("name") or "")
        if vendor and name_match_score(name, vendor) < 0.2:
            return {}, url, "name_mismatch"
        data["matched_name"] = vendor or None
        agg = node.get("aggregateRating") or {}
        if isinstance(agg, dict):
            r = to_float(agg.get("ratingValue"))
            if r and 0 < r <= 5:
                data["rating"] = r
            n = to_int(agg.get("reviewCount") or agg.get("ratingCount"))
            if n:
                data["review_count"] = n
        addr = node.get("address")
        if isinstance(addr, dict):
            joined = ", ".join(str(addr[k]) for k in
                               ("streetAddress", "addressLocality") if addr.get(k))
            if len(joined) > 8:
                data["address"] = joined
        tel = norm_bd_phone(node.get("telephone"))
        if tel:
            data["phone"] = tel
        pr = node.get("priceRange")
        if pr:
            data["price_range"] = str(pr).strip()

    snippets = []
    for sel in SELECTORS["tripadvisor"]["review_text"]:
        try:
            texts = await page.locator(sel).all_inner_texts()
        except Exception:
            continue
        for t in texts[:4]:
            t = re.sub(r'\s+', ' ', t).strip()
            if len(t) > 30:
                snippets.append({"text": t[:450], "source_tag": "ta_review"})
        if snippets:
            break
    if snippets:
        data["review_snippets"] = snippets[:3]

    return data, url, "ok" if data else "empty"


# ══════════════════════════════════════════════════════════════════════════
#  ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════

async def enrich_cafe(ctx, cafe: dict, rec: dict, sources: list[str]) -> dict:
    """
    Each source gets its own page and runs concurrently -- different hosts,
    so the per-host limiter serialises within a host but not across them.
    """
    rec["meta"]["attempts"] += 1
    rec["meta"]["last_attempt"] = now_iso()

    async def run(src: str):
        page = await ctx.new_page()
        try:
            if src == SRC_G:
                return src, *(await scrape_google(page, cafe))
            if src == SRC_FP:
                known = rec["sources"].get(SRC_FP, {}).get("url")
                return src, *(await scrape_foodpanda(page, cafe, known))
            if src == SRC_TA:
                return src, *(await scrape_tripadvisor(page, cafe))
            return src, {}, None, "unknown_source"
        except Exception as e:                   # noqa: BLE001
            log.warning("   [%s] unhandled %s: %s", src, type(e).__name__, str(e)[:120])
            return src, {}, None, f"error:{type(e).__name__}"
        finally:
            try:
                await page.close()
            except Exception:
                pass

    todo = [s for s in sources
            if not (rec["sources"].get(s, {}).get("status") == "ok"
                    and s == SRC_TA)]   # TA rarely improves on a second pass
    results = await asyncio.gather(*(run(s) for s in todo), return_exceptions=True)

    for r in results:
        if isinstance(r, BaseException):
            log.warning("   task failed: %s", r)
            continue
        src, data, url, status = r
        merge_source(rec, src, data, url, status)
        log.info("   [%-2s] %-16s %s", src, status,
                 ", ".join(sorted(k for k in data if not k.startswith("matched"))) or "-")

    # Socials discovered by Google go in the web bucket so provenance is honest.
    gdata = rec["sources"][SRC_G]["data"]
    web_payload = {k: gdata[k] for k in ("facebook_url", "instagram_url")
                   if gdata.get(k)}
    if web_payload:
        merge_source(rec, SRC_WEB, web_payload, gdata.get("website"), "derived")

    resolve_record(rec)
    score_record(rec)

    statuses = {s: rec["sources"][s]["status"] for s in rec["sources"]}
    if rec["meta"]["accepted"]:
        rec["meta"]["status"] = "accepted"
    elif all(v in ("not_found", "geo_mismatch", "name_mismatch")
             for v in (statuses[SRC_G], statuses[SRC_FP])):
        rec["meta"]["status"] = "not_found"
    elif any(v == "blocked_or_error" for v in statuses.values()):
        rec["meta"]["status"] = "partial_blocked"
    else:
        rec["meta"]["status"] = "partial"
    return rec


async def run_pipeline(args) -> None:
    candidates = load_candidates(Path(args.csv), args.pool, args.rank_order)
    if args.dry_run:
        log.info("--dry-run: stopping before any network access.")
        return

    store = Store(OUT_JSON, OUT_CSV, PROGRESS)
    sources = ALL_SOURCES if args.only == "all" else [args.only]
    log.info("Sources: %s | target=%d | pool=%d | passes=%d",
             "+".join(sources), args.target, len(candidates), args.passes)

    run_started = now_iso()

    async with async_playwright() as pw:
        ctx = await launch_context(pw, args.headless)
        try:
            for pass_no in range(1, args.passes + 1):
                pending = [c for c in candidates
                           if not (store.get(c["place_code"]) or {})
                           .get("meta", {}).get("accepted")
                           and (store.get(c["place_code"]) or {})
                           .get("meta", {}).get("status") != "not_found"]
                if store.accepted_count >= args.target:
                    log.info("Target of %d reached.", args.target)
                    break
                if not pending:
                    log.info("Nothing left to retry.")
                    break

                log.info("\n%s\nPASS %d/%d  |  %d pending  |  %d/%d accepted\n%s",
                         "=" * 72, pass_no, args.passes, len(pending),
                         store.accepted_count, args.target, "=" * 72)

                # Weakest-but-salvageable first: they need the most passes.
                pending.sort(key=lambda c: (store.get(c["place_code"]) or {})
                             .get("meta", {}).get("completeness", 0.0), reverse=True)

                for i, cafe in enumerate(pending, 1):
                    if store.accepted_count >= args.target:
                        break
                    pc = cafe["place_code"]
                    rec = store.get(pc) or blank_record(cafe)

                    log.info("\n[%d/%d] %s  (%s)", i, len(pending),
                             cafe["business_name"], pc)
                    try:
                        rec = await enrich_cafe(ctx, cafe, rec, sources)
                    except Exception as e:       # noqa: BLE001
                        log.error("   fatal on %s: %s", pc, e)
                        rec["meta"]["notes"].append(f"{now_iso()} {type(e).__name__}")

                    store.upsert(rec)
                    store.progress["attempts"][pc] = rec["meta"]["attempts"]
                    store.flush()               # every cafe, atomically

                    badge = "ACCEPTED" if rec["meta"]["accepted"] else "partial "
                    log.info("   -> %s  completeness=%.0f%%  missing=%s",
                             badge, rec["meta"]["completeness"] * 100,
                             missing_report(rec) or "none")

                    await asyncio.sleep(random.uniform(2.0, 5.0))

                if pass_no < args.passes and store.accepted_count < args.target:
                    cool = 45 * pass_no
                    log.info("\nPass %d done. Cooling %ds before the next sweep.",
                             pass_no, cool)
                    await asyncio.sleep(cool)
        finally:
            try:
                await ctx.close()
            except Exception:
                pass

    store.progress["runs"].append({
        "started": run_started, "finished": now_iso(),
        "accepted_after": store.accepted_count,
        "blocked_events": GUARD.blocked_events,
    })
    store.progress["accepted"] = [pc for pc, r in store.records.items()
                                  if r["meta"]["accepted"]]
    store.flush()
    print_summary(store)


def print_summary(store: Store) -> None:
    recs = sorted(store.records.values(),
                  key=lambda r: (not r["meta"]["accepted"],
                                 -r["meta"]["completeness"]))
    W = 112
    print("\n" + "-" * W)
    print(f"{'#':<3} {'Cafe':<30} {'OK':<3} {'Full':>5} {'Rat':>5} "
          f"{'Revs':>6} {'Hrs':>4} {'Menu':>5} {'Src':<10} Phone")
    print("-" * W)
    for i, r in enumerate(recs, 1):
        res = r["resolved"]
        ok = [s for s, b in r["sources"].items() if b.get("status") == "ok"]
        print(f"{i:<3} {r['business_name'][:29]:<30} "
              f"{'Y' if r['meta']['accepted'] else '.':<3} "
              f"{r['meta']['completeness']*100:>4.0f}% "
              f"{str(res.get('rating') or '-'):>5} "
              f"{str(res.get('review_count') or '-'):>6} "
              f"{str(len(res.get('opening_hours') or {}))+'d':>4} "
              f"{len(res.get('menu_items') or []):>5} "
              f"{'+'.join(sorted(ok)):<10} {res.get('phone') or '-'}")
    print("-" * W)
    n_ok = sum(1 for r in recs if r["meta"]["accepted"])
    print(f"\n  Accepted {n_ok} / {len(recs)}   "
          f"(core: {', '.join(CORE_FIELDS)}  +  >= {MIN_RICH} rich fields)")
    if GUARD.blocked_events:
        print(f"  Blocks encountered: {len(GUARD.blocked_events)} "
              f"-> {set(e['host'] for e in GUARD.blocked_events)}")
        print("  If this is frequent, raise HOST_MIN_INTERVAL. Do not lower it.")
    print(f"  JSON  {OUT_JSON}")
    print(f"  CSV   {OUT_CSV}")
    print(f"  Log   {LOG_PATH}\n")


# ══════════════════════════════════════════════════════════════════════════
#  PROBE MODE  — dump a live page so selectors can be repaired quickly
# ══════════════════════════════════════════════════════════════════════════

async def run_probe(args) -> None:
    name = args.probe_name or "North End Coffee Roasters"
    async with async_playwright() as pw:
        ctx = await launch_context(pw, args.headless)
        page = await ctx.new_page()
        if args.probe == "google":
            url = ("https://www.google.com/maps/search/" +
                   quote_plus(f"{name}, Gulshan, Dhaka") + "?hl=en")
            host = "www.google.com"
        elif args.probe == "foodpanda":
            url = await find_url(page, f'site:foodpanda.com.bd "{name}" Gulshan',
                                 "foodpanda.com.bd", "/restaurant/")
            host = "www.foodpanda.com.bd"
        else:
            url = await find_url(page, f'site:tripadvisor.com "{name}" Dhaka',
                                 "tripadvisor.", "Restaurant_Review")
            host = "www.tripadvisor.com"

        if not url:
            log.error("Could not find a URL to probe.")
            await ctx.close()
            return

        body = await goto(page, url, host=host)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = PROBE_DIR / f"{args.probe}_{stamp}"
        try:
            (base.with_suffix(".html")).write_text(await page.content(), encoding="utf-8")
            (base.with_suffix(".txt")).write_text(body or "(blocked)", encoding="utf-8")
            await page.screenshot(path=str(base.with_suffix(".png")), full_page=False)
        except Exception as e:                   # noqa: BLE001
            log.warning("probe write failed: %s", e)
        nodes = await jsonld_nodes(page)
        (base.parent / f"{base.name}_jsonld.json").write_text(
            json.dumps(nodes, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Probe saved: %s.{html,txt,png} + _jsonld.json", base)
        log.info("JSON-LD business node: %s",
                 json.dumps(pick_business_node(nodes) or {}, ensure_ascii=False)[:600])
        if not args.headless:
            log.info("Browser stays open 30s -- inspect with DevTools.")
            await asyncio.sleep(30)
        await ctx.close()


# ══════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Barikoi Gulshan cafe enrichment, multi-source and source-tagged.")
    p.add_argument("--csv", default=str(DEFAULT_INPUT_CSV),
                   help="Barikoi places CSV")
    p.add_argument("--pool", type=int, default=DEFAULT_POOL,
                   help="shortlist size (15-20 recommended)")
    p.add_argument("--target", type=int, default=DEFAULT_TARGET,
                   help="stop once this many cafes are ACCEPTED")
    p.add_argument("--passes", type=int, default=DEFAULT_PASSES,
                   help="sweeps over the pool before giving up")
    p.add_argument("--only", default="all",
                   choices=["all", SRC_G, SRC_FP, SRC_TA, "google", "foodpanda",
                            "tripadvisor"],
                   help="restrict to one source")
    p.add_argument("--rank-order", default="auto", choices=["auto", "rank", "score"],
                   help="'rank' = 1 is most popular; 'score' = higher is better")
    p.add_argument("--headless", action="store_true",
                   help="run without a visible browser window")
    p.add_argument("--dry-run", action="store_true",
                   help="show the shortlist, touch no network")
    p.add_argument("--probe", choices=["google", "foodpanda", "tripadvisor"],
                   help="dump one live page for selector repair")
    p.add_argument("--probe-name", help="cafe name to probe")
    a = p.parse_args(argv)
    a.only = {"google": SRC_G, "foodpanda": SRC_FP,
              "tripadvisor": SRC_TA}.get(a.only, a.only)
    return a


def main() -> None:
    args = parse_args()
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        if args.probe:
            asyncio.run(run_probe(args))
        else:
            asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        log.info("\nInterrupted. Everything written so far is intact "
                 "(atomic writes -- no partial files).")


if __name__ == "__main__":
    main()