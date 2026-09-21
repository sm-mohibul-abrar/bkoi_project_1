#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BARIKOI · MULTI-LOCATION CAFE ENRICHMENT · v6
=============================================
See FLOWCHART.md in this folder for the full process description.

WHAT THIS IS
------------
Reads a Barikoi places CSV, shortlists the cafes, and enriches each one from
four sources, tagging every value with where it came from. Writes ONE pair of
output files per location. Re-running merges into the same pair and only works
on what is still missing.

    data/gulshan_all_data.json      <- one file per location, merged forever
    data/gulshan_all_data.csv
    data/gulshan_progress.json
    data/gulshan_budget.json        <- per-day request ledger, IP protection

Change location and a new set is created automatically. The location key is
stamped inside the file; pointing the wrong CSV at an existing file aborts
rather than contaminating it.

SOURCES (in the order they are tried)
-------------------------------------
    osm   OpenStreetMap via Overpass API. Free, no key, no browser, and
          explicitly open to automated queries. ONE bulk request covers every
          cafe in the bounding box, so 100 cafes cost 1 request instead of 100.
          This is the source that keeps you off Google's radar.
          LICENCE WARNING: OSM is ODbL. Share-alike may affect a proprietary
          database. Every OSM value is tagged `osm` and every osm_* CSV column
          is droppable in one step. Talk to legal before merging.
    g     Google Maps. Richest, most protective. Strict budget.
    fp    Foodpanda Bangladesh. Where menus and delivery hours actually live.
    ta    TripAdvisor. Best effort, blocks hardest, lowest priority.

IP PROTECTION -- READ THIS
--------------------------
No scraper can guarantee your IP is never blocked. What this one does:

  1. OSM first, so many cafes are filled without touching Google at all.
  2. Hard per-day request budgets per host, persisted across runs.
  3. Abort-on-block: the FIRST hard block disables that host for the whole
     run. It never retries into a wall, which is what gets IPs flagged.
  4. Duplicate clustering: the same cafe under two place_codes is fetched once.
  5. Proxy preflight. With --proxy, the script checks your visible IP through
     the proxy against your direct IP and REFUSES TO START if they match.
     This is the only real guarantee available, and it needs a proxy you trust.

  Conservative pacing is the default. --aggressive exists but raises risk;
  it is not compatible with the promise of never being blocked.

USAGE
-----
    pip install playwright pandas
    playwright install chromium

    python cafe_scraper_v6.py --csv places.csv --dry-run      # plan only
    python cafe_scraper_v6.py --csv places.csv                # collect
    python cafe_scraper_v6.py --csv places.csv                # again: fills gaps
    python cafe_scraper_v6.py --csv places.csv --proxy http://user:pass@host:3128
    python cafe_scraper_v6.py --check-ip --proxy http://...   # verify only
    python cafe_scraper_v6.py --csv dhanmondi.csv             # new location file
    python cafe_scraper_v6.py --probe google --probe-name "Windy Terrace"

Run it daily until the summary stops improving. That is the intended workflow.
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
    sys.exit("pip install pandas")
try:
    from playwright.async_api import async_playwright
    from playwright.async_api import TimeoutError as PWTimeout
except ImportError:
    sys.exit("pip install playwright && playwright install chromium")


# ══════════════════════════════════════════════════════════════════════════
#  PATHS
# ══════════════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
PROBE_DIR = BASE_DIR / "probes"
PROFILE_ROOT = BASE_DIR / ".profiles"
for _d in (DATA_DIR, LOG_DIR, PROBE_DIR, PROFILE_ROOT):
    _d.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE TAGS
# ══════════════════════════════════════════════════════════════════════════

SRC_BKOI = "bkoi"   # Barikoi CSV, hint only
SRC_OSM = "osm"     # OpenStreetMap / Overpass   (ODbL -- see licence warning)
SRC_G = "g"         # Google Maps
SRC_FP = "fp"       # Foodpanda Bangladesh
SRC_TA = "ta"       # TripAdvisor
SRC_WEB = "web"     # venue site / socials, discovered via the above

SCRAPED_SOURCES = [SRC_G, SRC_FP, SRC_TA]
ALL_SOURCE_KEYS = [SRC_OSM, SRC_G, SRC_FP, SRC_TA, SRC_WEB]


# ══════════════════════════════════════════════════════════════════════════
#  PACING, BUDGETS, BATCHING   — the IP-safety knobs
# ══════════════════════════════════════════════════════════════════════════

PROFILES = {
    # seconds between consecutive requests to the same host
    "safe": {
        "www.google.com": 22.0,
        "www.foodpanda.com.bd": 16.0,
        "www.tripadvisor.com": 28.0,
        "html.duckduckgo.com": 9.0,
        "overpass-api.de": 35.0,
        "_default": 12.0,
    },
    "paranoid": {
        "www.google.com": 45.0,
        "www.foodpanda.com.bd": 30.0,
        "www.tripadvisor.com": 60.0,
        "html.duckduckgo.com": 16.0,
        "overpass-api.de": 40.0,
        "_default": 20.0,
    },
    "aggressive": {          # raises block risk. Not the default for a reason.
        "www.google.com": 8.0,
        "www.foodpanda.com.bd": 6.0,
        "www.tripadvisor.com": 12.0,
        "html.duckduckgo.com": 4.0,
        "overpass-api.de": 30.0,
        "_default": 5.0,
    },
}

# Hard ceilings per host per calendar day, persisted in <location>_budget.json.
# 100 cafes need ~100 Google hits, so a full Google sweep is one day's budget.
DAILY_BUDGET = {
    "www.google.com": 130,
    "www.foodpanda.com.bd": 110,
    "www.tripadvisor.com": 45,
    "html.duckduckgo.com": 170,
    "overpass-api.de": 12,
    "_default": 60,
}

HOST_JITTER = 0.5
BATCH_SIZE = 10                 # you asked: never more than 10 at once
BATCH_COOLDOWN = (180.0, 360.0)  # seconds between batches
BREAKER_THRESHOLD = 2           # soft failures before a host is cooled
BREAKER_COOLDOWN = 600.0
NAV_TIMEOUT_MS = 45_000
SETTLE_MS = 3_800
GEO_TOL_M = 1_200
DUP_RADIUS_M = 150              # same name within this radius == same cafe
OSM_MATCH_RADIUS_M = 220

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
IP_ECHO = "https://api.ipify.org?format=json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
# Overpass asks for a descriptive UA identifying the client. Be honest here.
OSM_UA = "BarikoiCafeEnrichment/6.0 (data collection; contact: ops@barikoi.com)"
VIEWPORT = {"width": 1440, "height": 900}
DHAKA = {"latitude": 23.7806, "longitude": 90.4193}


# ══════════════════════════════════════════════════════════════════════════
#  ACCEPTANCE
# ══════════════════════════════════════════════════════════════════════════

CORE_FIELDS = ["business_name", "latitude", "longitude", "address", "phone",
               "opening_hours"]
RICH_FIELDS = ["rating", "review_count", "review_snippets", "website"]
BONUS_FIELDS = ["menu_items", "facebook_url", "instagram_url", "foodpanda_url",
                "tripadvisor_url", "price_range", "osm_id"]
MIN_RICH = 3
MIN_HOURS_DAYS = 5
FIELD_WEIGHTS = {**{f: 3.0 for f in CORE_FIELDS},
                 **{f: 2.0 for f in RICH_FIELDS},
                 **{f: 1.0 for f in BONUS_FIELDS}}

RESOLUTION_PRIORITY = {
    "address":        [SRC_G, SRC_FP, SRC_OSM, SRC_BKOI],
    "latitude":       [SRC_G, SRC_OSM, SRC_BKOI],
    "longitude":      [SRC_G, SRC_OSM, SRC_BKOI],
    "phone":          [SRC_G, SRC_FP, SRC_OSM, SRC_WEB],
    "website":        [SRC_G, SRC_OSM, SRC_WEB],
    "opening_hours":  [SRC_G, SRC_FP, SRC_OSM],
    "rating":         [SRC_G, SRC_FP, SRC_TA],
    "review_count":   [SRC_G, SRC_FP, SRC_TA],
    "review_snippets": [SRC_G, SRC_TA],
    "menu_items":     [SRC_FP, SRC_G],
    "price_range":    [SRC_G, SRC_FP, SRC_TA],
    "facebook_url":   [SRC_WEB, SRC_G, SRC_OSM],
    "instagram_url":  [SRC_WEB, SRC_G, SRC_OSM],
    "business_status": [SRC_G, SRC_OSM, SRC_FP],
    "osm_id":         [SRC_OSM],
    "category":       [SRC_G, SRC_OSM],
}

# ── business status vocabulary ────────────────────────────────────────────
ST_OPERATIONAL = "OPERATIONAL"
ST_TEMP_CLOSED = "TEMPORARILY_CLOSED"
ST_PERM_CLOSED = "PERMANENTLY_CLOSED"
ST_MOVED = "MOVED"
ST_UNKNOWN = "UNKNOWN"

RESOLVED_FIELDS = CORE_FIELDS + RICH_FIELDS + BONUS_FIELDS + [
    "business_status", "business_status_note",
    "google_maps_url", "google_cid", "plus_code", "category",
]


# ══════════════════════════════════════════════════════════════════════════
#  SELECTORS  — patch here when a site reshuffles its DOM
# ══════════════════════════════════════════════════════════════════════════

SELECTORS = {
    "google": {
        "phone_btn":   ['button[data-item-id^="phone:tel:"]', '[data-item-id^="phone:tel:"]'],
        "address_btn": ['button[data-item-id="address"]', '[data-item-id="address"]'],
        "website_btn": ['a[data-item-id="authority"]', '[data-item-id="authority"]'],
        "plus_code":   ['[data-item-id="oloc"]'],
        "title":       ['h1.DUwDvf', 'h1[class*="DUwDvf"]', 'div[role="main"] h1'],
        "rating":      ['div.F7nice span[aria-hidden="true"]',
                        'span[aria-label$="stars"]', 'div[class*="fontDisplayLarge"]'],
        "review_count": ['div.F7nice span[aria-label*="review"]',
                         'button[jsaction*="reviewChart"] span', 'span[aria-label*="reviews"]'],
        "category":    ['button[jsaction*="category"]', 'button.DkEaL'],
        "price_range": ['span[aria-label*="Price"]', 'span[aria-label*="price range"]'],
        "status_badge": ['span.fCEvvc', '[class*="fCEvvc"]', 'span.o0Svhf',
                         'div[role="main"] span:has-text("Temporarily closed")',
                         'div[role="main"] span:has-text("Permanently closed")'],
        "hours_toggle": ['button[data-item-id="oh"]', '[jsaction*="openhours"]',
                         'button[aria-label*="Show open hours"]', 'div[aria-label*="Hours"]'],
        "hours_table": ['table.eK4R0e tr', 'div[class*="t39EBf"] table tr',
                        'table[aria-label*="hours" i] tr'],
        "reviews_tab": ['button[role="tab"][aria-label*="Reviews"]',
                        'button[jsaction*="moreReviews"]', 'button[aria-label*="Reviews for"]'],
        "review_text": ['span.wiI7pd', 'div.MyEned span', '[class*="wiI7pd"]'],
        "place_link":  ['a[href*="/maps/place/"]'],
        "scroll_panel": ['div[role="main"]', 'div[role="feed"]'],
        "consent":     ['#L2AGLb', 'button[aria-label*="Accept all" i]',
                        'form[action*="consent"] button'],
    },
    "foodpanda": {
        "name":        ['h1[data-testid="vendor-name"]', 'h1.vendor-name', 'h1'],
        "rating":      ['[data-testid="vendor-rating"]', '.rating__score', 'span[class*="rating"]'],
        "review_count": ['[data-testid="vendor-review-count"]', '.rating__count'],
        "closed_flag": ['[data-testid="vendor-closed"]', '[class*="closed-banner"]',
                        '[class*="vendor-closed"]'],
        "product_card": ['[data-testid="menu-product"]', 'li[data-testid*="product"]',
                         '.dish-card', '[class*="product-card"]'],
        "product_name": ['[data-testid="menu-product-name"]', '.dish-card-title',
                         '[class*="product-name"]', 'h3'],
        "cookie":      ['#onetrust-accept-btn-handler', 'button[data-testid="accept-cookies"]'],
    },
    "tripadvisor": {
        "cookie":      ['#onetrust-accept-btn-handler'],
        "review_text": ['span[data-automation="reviewText"]', 'q.QewHA span',
                        'div[data-test-target="review-body"] span'],
        "closed_flag": ['[class*="closedBanner"]', 'div:has-text("Permanently Closed")'],
    },
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
DAY_ALIAS = {
    "monday": "mon", "mon": "mon", "mo": "mon",
    "tuesday": "tue", "tue": "tue", "tues": "tue", "tu": "tue",
    "wednesday": "wed", "wed": "wed", "we": "wed",
    "thursday": "thu", "thu": "thu", "thurs": "thu", "th": "thu",
    "friday": "fri", "fri": "fri", "fr": "fri",
    "saturday": "sat", "sat": "sat", "sa": "sat",
    "sunday": "sun", "sun": "sun", "su": "sun",
}
OSM_DAY = {"mo": "mon", "tu": "tue", "we": "wed", "th": "thu",
           "fr": "fri", "sa": "sat", "su": "sun"}

RE_HOURS_LINE = re.compile(
    r'\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|'
    r'Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b[^\dA-Za-z]{0,6}'
    r'(Closed|Open\s*24\s*hours|'
    r'\d{1,2}(?::\d{2})?\s*(?:AM|PM)?\s*[\u2013\u2014\-]\s*'
    r'\d{1,2}(?::\d{2})?\s*(?:AM|PM))',
    re.IGNORECASE)

BLOCK_SIGNALS_HARD = [
    "unusual traffic", "not a robot", "our systems have detected",
    "captcha", "access denied", "pardon our interruption",
    "verify you are a human", "too many requests",
]
BLOCK_SIGNALS_SOFT = ["enable javascript and cookies to continue", "rate limit"]

CAFE_SUBTYPES = {"cafe", "coffee shop", "coffeeshop", "bakery", "tea stall",
                 "dessert", "patisserie", "confectionery"}
CAFE_HINTS = ["cafe", "café", "coffee", "roaster", "bakery", "bake", "tea",
              "patisserie", "pastry", "brew", "espresso", "dessert"]


# ══════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════

LOG_PATH = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname).1s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"),
              logging.StreamHandler(sys.stdout)])
log = logging.getLogger("cafe")


# ══════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def slugify(s: str) -> str:
    s = re.sub(r'[^a-z0-9]+', '_', str(s).strip().lower()).strip('_')
    return s or "unknown"


def atomic_write(path: Path, text: str) -> None:
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
    s = re.sub(r'\b([ap])\.?m\.?\b', lambda m: m.group(1).upper() + 'M', s, flags=re.I)
    return s


def hhmm_to_ampm(t: str) -> str:
    m = re.fullmatch(r'(\d{1,2}):(\d{2})(?::\d{2})?', str(t).strip())
    if not m:
        return str(t).strip()
    h, mi = int(m.group(1)), m.group(2)
    if h >= 24:
        h -= 24
    return f"{h % 12 or 12}:{mi} {'AM' if h < 12 else 'PM'}"


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


# ── OSM opening_hours syntax -> our day dict ──────────────────────────────

def _expand_osm_days(spec: str) -> list[str]:
    days: list[str] = []
    for part in spec.split(','):
        part = part.strip()
        m = re.fullmatch(r'([A-Za-z]{2})\s*-\s*([A-Za-z]{2})', part)
        if m:
            a, b = OSM_DAY.get(m.group(1).lower()), OSM_DAY.get(m.group(2).lower())
            if a and b:
                i, j = DAY_ORDER.index(a), DAY_ORDER.index(b)
                days += DAY_ORDER[i:j + 1] if i <= j else DAY_ORDER[i:] + DAY_ORDER[:j + 1]
        else:
            d = OSM_DAY.get(part.lower()[:2])
            if d:
                days.append(d)
    return days


def parse_osm_hours(s: str) -> dict:
    """e.g. 'Mo-Fr 07:00-22:30; Sa,Su 08:00-23:00' or '24/7' or 'Mo off'."""
    if not s or not str(s).strip():
        return {}
    out: dict[str, str] = {}
    for rule in str(s).split(';'):
        rule = rule.strip()
        if not rule:
            continue
        if re.fullmatch(r'24/7', rule, re.I):
            for d in DAY_ORDER:
                out[d] = "Open 24 hours"
            continue
        m = re.match(r'^((?:[A-Za-z]{2}(?:\s*-\s*[A-Za-z]{2})?\s*,?\s*)+?)\s+(.+)$', rule)
        if m and _expand_osm_days(m.group(1)):
            days, rest = _expand_osm_days(m.group(1)), m.group(2).strip()
        else:
            days, rest = list(DAY_ORDER), rule
        if re.search(r'\b(off|closed)\b', rest, re.I):
            val = "Closed"
        else:
            spans = re.findall(r'(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})', rest)
            if not spans:
                continue
            val = ", ".join(f"{hhmm_to_ampm(a)} \u2013 {hhmm_to_ampm(b)}" for a, b in spans)
        for d in days:
            out[d] = val
    return {d: out[d] for d in DAY_ORDER if d in out}


def detect_status_from_text(text: str) -> tuple[str, str | None]:
    """Map page wording to a business_status. Order matters: permanent first."""
    low = (text or "").lower()
    if "permanently closed" in low or "closed permanently" in low:
        return ST_PERM_CLOSED, "permanently closed"
    if "temporarily closed" in low or "temporarily unavailable" in low:
        return ST_TEMP_CLOSED, "temporarily closed"
    if "has moved" in low or "moved to a new location" in low:
        return ST_MOVED, "moved"
    return ST_UNKNOWN, None


# ══════════════════════════════════════════════════════════════════════════
#  BUDGET LEDGER  — persisted daily caps, the hard IP guard
# ══════════════════════════════════════════════════════════════════════════

class Budget:
    def __init__(self, path: Path, multiplier: float = 1.0):
        self.path = path
        self.mult = multiplier
        self.data = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("budget file unreadable -- starting a fresh ledger")
        self.day = self.data.setdefault(today_key(), {})
        # keep only the last 14 days
        for k in sorted(self.data)[:-14]:
            self.data.pop(k, None)

    def cap(self, host: str) -> int:
        return int(DAILY_BUDGET.get(host, DAILY_BUDGET["_default"]) * self.mult)

    def used(self, host: str) -> int:
        return int(self.day.get(host, 0))

    def remaining(self, host: str) -> int:
        return max(0, self.cap(host) - self.used(host))

    def can_spend(self, host: str) -> bool:
        return self.remaining(host) > 0

    def spend(self, host: str) -> None:
        self.day[host] = self.used(host) + 1

    def flush(self) -> None:
        atomic_write(self.path, json.dumps(self.data, indent=2))

    def report(self) -> str:
        return " | ".join(f"{h.split('.')[-2] if '.' in h else h}:{self.used(h)}/{self.cap(h)}"
                          for h in sorted(self.day)) or "nothing spent yet"


# ══════════════════════════════════════════════════════════════════════════
#  HOST GUARD  — pacing, breaker, abort-on-block
# ══════════════════════════════════════════════════════════════════════════

class HostGuard:
    def __init__(self, intervals: dict, budget: Budget):
        self.intervals = intervals
        self.budget = budget
        self._last: dict[str, float] = {}
        self._fails: dict[str, int] = {}
        self._cooldown: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self.disabled: dict[str, str] = {}   # host -> reason, for this whole run
        self.events: list[dict] = []

    def _lock(self, host: str) -> asyncio.Lock:
        return self._locks.setdefault(host, asyncio.Lock())

    def available(self, host: str) -> tuple[bool, str]:
        if host in self.disabled:
            return False, f"disabled:{self.disabled[host]}"
        if not self.budget.can_spend(host):
            return False, f"budget-exhausted({self.budget.cap(host)}/day)"
        until = self._cooldown.get(host, 0.0)
        if until and time.monotonic() < until:
            return False, f"cooling {until - time.monotonic():.0f}s"
        if until:
            self._cooldown.pop(host, None)
            self._fails[host] = 0
        return True, "ok"

    async def acquire(self, host: str) -> None:
        await self._lock(host).acquire()
        iv = self.intervals.get(host, self.intervals["_default"])
        iv *= random.uniform(1 - HOST_JITTER, 1 + HOST_JITTER)
        gap = time.monotonic() - self._last.get(host, 0.0)
        if gap < iv:
            await asyncio.sleep(iv - gap)

    def release(self, host: str) -> None:
        self._last[host] = time.monotonic()
        lk = self._locks.get(host)
        if lk and lk.locked():
            lk.release()

    def ok(self, host: str) -> None:
        self._fails[host] = 0

    def soft_fail(self, host: str, reason: str) -> None:
        n = self._fails.get(host, 0) + 1
        self._fails[host] = n
        if n >= BREAKER_THRESHOLD:
            self._cooldown[host] = time.monotonic() + BREAKER_COOLDOWN
            self.events.append({"host": host, "kind": "cooldown",
                                "reason": reason, "at": now_iso()})
            log.warning("   cooling %s for %.0fs after %d failures (%s)",
                        host, BREAKER_COOLDOWN, n, reason)

    def hard_block(self, host: str, reason: str) -> None:
        """A real block signal. Stop touching this host for the entire run."""
        self.disabled[host] = reason
        self.events.append({"host": host, "kind": "hard_block",
                            "reason": reason, "at": now_iso()})
        log.error("   BLOCK DETECTED on %s (%s). Disabling for the rest of "
                  "this run. Do not re-run against this host today.",
                  host, reason)


def detect_block(url: str, body: str) -> tuple[str | None, bool]:
    """Returns (reason, is_hard). Hard blocks disable the host for the run."""
    u = (url or "").lower()
    if "/sorry/" in u or "consent.google" in u or "captcha" in u:
        return f"block-url:{u[:60]}", True
    low = (body or "")[:6000].lower()
    for sig in BLOCK_SIGNALS_HARD:
        if sig in low:
            return f"block-text:{sig}", True
    for sig in BLOCK_SIGNALS_SOFT:
        if sig in low:
            return f"soft:{sig}", False
    if len((body or "").strip()) < 120:
        return "empty-body", False
    return None, False


# ══════════════════════════════════════════════════════════════════════════
#  RECORD SCHEMA
# ══════════════════════════════════════════════════════════════════════════

def blank_source() -> dict:
    return {"fetched_at": None, "url": None, "status": "pending", "data": {}}


def blank_record(cafe: dict, location_key: str) -> dict:
    return {
        "place_code": cafe["place_code"],
        "business_name": cafe["business_name"],
        "location_key": location_key,
        "duplicate_of": cafe.get("duplicate_of"),
        "bkoi": {
            "address": cafe.get("bkoi_address"),
            "latitude": cafe.get("bkoi_lat"),
            "longitude": cafe.get("bkoi_lng"),
            "sub_area": cafe.get("bkoi_sub_area"),
            "area": cafe.get("bkoi_area"),
            "city": cafe.get("bkoi_city"),
            "postcode": cafe.get("bkoi_postcode"),
            "type": cafe.get("bkoi_type"),
            "sub_type": cafe.get("bkoi_sub_type"),
            "popularity_ranking": cafe.get("bkoi_popularity"),
        },
        "sources": {s: blank_source() for s in ALL_SOURCE_KEYS},
        "resolved": {f: ([] if f in ("review_snippets", "menu_items") else None)
                     for f in RESOLVED_FIELDS},
        "provenance": {},
        "meta": {"first_seen": now_iso(), "last_attempt": None, "attempts": 0,
                 "completeness": 0.0, "accepted": False, "status": "pending",
                 "notes": []},
    }


def merge_source(rec: dict, src: str, payload: dict, url: str | None,
                 status: str) -> dict:
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
            old.update({a: b for a, b in v.items() if not is_empty(b)})
        else:
            data[k] = v
    return rec


def resolve_record(rec: dict) -> dict:
    res, prov = rec["resolved"], rec["provenance"]
    buckets = {s: rec["sources"].get(s, {}).get("data", {}) for s in rec["sources"]}
    buckets[SRC_BKOI] = rec.get("bkoi", {})
    res["business_name"] = rec["business_name"]

    for field, order in RESOLUTION_PRIORITY.items():
        if field in ("review_snippets", "menu_items"):
            continue
        for src in order:
            val = buckets.get(src, {}).get(field)
            if is_empty(val):
                continue
            if field == "opening_hours" and len(val) < MIN_HOURS_DAYS:
                continue
            if field == "business_status" and val == ST_UNKNOWN:
                continue
            res[field] = val
            prov[field] = src
            break

    for field in ("review_snippets", "menu_items"):
        merged, seen = [], set()
        for src in RESOLUTION_PRIORITY.get(field, SCRAPED_SOURCES):
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

    res["foodpanda_url"] = rec["sources"].get(SRC_FP, {}).get("url")
    res["tripadvisor_url"] = rec["sources"].get(SRC_TA, {}).get("url")
    res["google_maps_url"] = rec["sources"].get(SRC_G, {}).get("url")
    for k in ("google_cid", "plus_code"):
        v = buckets.get(SRC_G, {}).get(k)
        if not is_empty(v):
            res[k] = v
            prov[k] = SRC_G

    if is_empty(res.get("business_status")):
        res["business_status"] = ST_OPERATIONAL if any(
            rec["sources"][s]["status"] == "ok" for s in SCRAPED_SOURCES
        ) else ST_UNKNOWN
        prov["business_status"] = "inferred"
    notes = [b.get("business_status_note")
             for b in buckets.values() if b.get("business_status_note")]
    if notes:
        res["business_status_note"] = "; ".join(sorted(set(notes)))
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
    got = sum(w for f, w in FIELD_WEIGHTS.items() if field_present(res, f))
    rec["meta"]["completeness"] = round(got / sum(FIELD_WEIGHTS.values()), 4)
    closed = res.get("business_status") in (ST_PERM_CLOSED, ST_TEMP_CLOSED)
    if closed:
        # A closed venue will never have full hours. Do not chase it forever.
        rec["meta"]["accepted"] = field_present(res, "address") and \
            field_present(res, "latitude")
    else:
        rec["meta"]["accepted"] = (
            all(field_present(res, f) for f in CORE_FIELDS)
            and sum(1 for f in RICH_FIELDS if field_present(res, f)) >= MIN_RICH)
    return rec


def missing_report(rec: dict) -> list[str]:
    res = rec["resolved"]
    out = [f"core:{f}" for f in CORE_FIELDS if not field_present(res, f)]
    n = sum(1 for f in RICH_FIELDS if field_present(res, f))
    if n < MIN_RICH:
        out.append(f"rich:{n}/{MIN_RICH}")
    return out


# ══════════════════════════════════════════════════════════════════════════
#  STORE  — one file pair per location
# ══════════════════════════════════════════════════════════════════════════

CSV_COLUMNS = [
    "place_code", "business_name", "location_key", "duplicate_of",
    "bkoi_address", "bkoi_sub_area", "bkoi_area", "bkoi_postcode",
    "bkoi_latitude", "bkoi_longitude", "bkoi_sub_type",
    "business_status", "business_status_source", "business_status_note",
    "is_temporarily_closed", "is_permanently_closed",
    "address", "latitude", "longitude", "phone", "website",
    "google_maps_url", "google_cid", "plus_code", "category", "price_range",
    "facebook_url", "instagram_url", "foodpanda_url", "tripadvisor_url", "osm_id",
    # per-platform opening hours, kept side by side
    "g_opening_hours", "fp_opening_hours", "osm_opening_hours",
    "opening_hours_resolved", "opening_hours_source", "opening_hours_days",
    # per-platform ratings, kept side by side
    "g_rating", "g_review_count", "fp_rating", "fp_review_count",
    "ta_rating", "ta_review_count",
    "rating_resolved", "review_count_resolved", "rating_source",
    # per-platform phone/address so disagreements are visible
    "g_phone", "fp_phone", "osm_phone", "phone_source", "address_source",
    "review_snippets", "menu_items", "menu_item_count",
    "sources_ok", "completeness", "accepted", "status", "attempts", "last_attempt",
]


class Store:
    def __init__(self, location_key: str):
        self.key = location_key
        self.json_path = DATA_DIR / f"{location_key}_all_data.json"
        self.csv_path = DATA_DIR / f"{location_key}_all_data.csv"
        self.prog_path = DATA_DIR / f"{location_key}_progress.json"
        self.records = self._load()
        self.progress = self._load_progress()
        log.info("Store [%s]: %d records, %d accepted  ->  %s",
                 location_key, len(self.records), self.accepted_count,
                 self.csv_path.name)

    def _load(self) -> dict[str, dict]:
        for p in (self.json_path, self.json_path.with_suffix(".json.bak")):
            if not p.exists():
                continue
            try:
                raw = json.loads(p.read_text(encoding="utf-8"))
                stored_key = raw.get("location_key") if isinstance(raw, dict) else None
                if stored_key and stored_key != self.key:
                    sys.exit(f"ABORT: {p.name} belongs to location '{stored_key}' "
                             f"but this run is '{self.key}'. Refusing to mix "
                             f"locations. Use --location to override.")
                recs = raw.get("records", raw) if isinstance(raw, dict) else raw
                return {r["place_code"]: r for r in recs if r.get("place_code")}
            except json.JSONDecodeError as e:
                log.warning("%s unreadable (%s) -- trying backup", p.name, e)
        return {}

    def _load_progress(self) -> dict:
        base = {"runs": [], "location_key": self.key}
        if self.prog_path.exists():
            try:
                base.update(json.loads(self.prog_path.read_text(encoding="utf-8")))
            except json.JSONDecodeError:
                log.warning("progress unreadable -- fresh ledger")
        return base

    @property
    def accepted_count(self) -> int:
        return sum(1 for r in self.records.values() if r["meta"]["accepted"])

    def get(self, pc: str) -> dict | None:
        return self.records.get(pc)

    def upsert(self, rec: dict) -> None:
        self.records[rec["place_code"]] = rec

    def flush(self) -> None:
        recs = sorted(self.records.values(),
                      key=lambda r: (not r["meta"]["accepted"],
                                     -r["meta"]["completeness"]))
        atomic_write(self.json_path, json.dumps({
            "location_key": self.key,
            "generated_at": now_iso(),
            "schema_version": 6,
            "source_tags": {
                SRC_BKOI: "Barikoi CSV (hint only)",
                SRC_OSM: "OpenStreetMap via Overpass (ODbL -- check licensing)",
                SRC_G: "Google Maps", SRC_FP: "Foodpanda BD",
                SRC_TA: "TripAdvisor", SRC_WEB: "venue site / socials",
            },
            "counts": {"total": len(recs),
                       "accepted": sum(1 for r in recs if r["meta"]["accepted"]),
                       "temporarily_closed": sum(
                           1 for r in recs
                           if r["resolved"].get("business_status") == ST_TEMP_CLOSED),
                       "permanently_closed": sum(
                           1 for r in recs
                           if r["resolved"].get("business_status") == ST_PERM_CLOSED)},
            "records": recs,
        }, ensure_ascii=False, indent=2))
        self._write_csv(recs)
        atomic_write(self.prog_path, json.dumps(self.progress, indent=2))

    def _write_csv(self, recs: list[dict]) -> None:
        rows = []
        for r in recs:
            res, prov, b = r["resolved"], r["provenance"], r["bkoi"]
            d = {s: r["sources"].get(s, {}).get("data", {}) for s in ALL_SOURCE_KEYS}
            ok = [s for s, blk in r["sources"].items() if blk.get("status") == "ok"]
            st = res.get("business_status")
            rows.append({
                "place_code": r["place_code"], "business_name": r["business_name"],
                "location_key": r.get("location_key"),
                "duplicate_of": r.get("duplicate_of"),
                "bkoi_address": b.get("address"), "bkoi_sub_area": b.get("sub_area"),
                "bkoi_area": b.get("area"), "bkoi_postcode": b.get("postcode"),
                "bkoi_latitude": b.get("latitude"), "bkoi_longitude": b.get("longitude"),
                "bkoi_sub_type": b.get("sub_type"),
                "business_status": st,
                "business_status_source": prov.get("business_status"),
                "business_status_note": res.get("business_status_note"),
                "is_temporarily_closed": st == ST_TEMP_CLOSED,
                "is_permanently_closed": st == ST_PERM_CLOSED,
                "address": res.get("address"), "latitude": res.get("latitude"),
                "longitude": res.get("longitude"), "phone": res.get("phone"),
                "website": res.get("website"),
                "google_maps_url": res.get("google_maps_url"),
                "google_cid": res.get("google_cid"), "plus_code": res.get("plus_code"),
                "category": res.get("category"), "price_range": res.get("price_range"),
                "facebook_url": res.get("facebook_url"),
                "instagram_url": res.get("instagram_url"),
                "foodpanda_url": res.get("foodpanda_url"),
                "tripadvisor_url": res.get("tripadvisor_url"),
                "osm_id": res.get("osm_id"),
                "g_opening_hours": json.dumps(d[SRC_G].get("opening_hours") or {},
                                              ensure_ascii=False),
                "fp_opening_hours": json.dumps(d[SRC_FP].get("opening_hours") or {},
                                               ensure_ascii=False),
                "osm_opening_hours": json.dumps(d[SRC_OSM].get("opening_hours") or {},
                                                ensure_ascii=False),
                "opening_hours_resolved": json.dumps(res.get("opening_hours") or {},
                                                     ensure_ascii=False),
                "opening_hours_source": prov.get("opening_hours"),
                "opening_hours_days": len(res.get("opening_hours") or {}),
                "g_rating": d[SRC_G].get("rating"),
                "g_review_count": d[SRC_G].get("review_count"),
                "fp_rating": d[SRC_FP].get("rating"),
                "fp_review_count": d[SRC_FP].get("review_count"),
                "ta_rating": d[SRC_TA].get("rating"),
                "ta_review_count": d[SRC_TA].get("review_count"),
                "rating_resolved": res.get("rating"),
                "review_count_resolved": res.get("review_count"),
                "rating_source": prov.get("rating"),
                "g_phone": d[SRC_G].get("phone"), "fp_phone": d[SRC_FP].get("phone"),
                "osm_phone": d[SRC_OSM].get("phone"),
                "phone_source": prov.get("phone"),
                "address_source": prov.get("address"),
                "review_snippets": json.dumps(res.get("review_snippets") or [],
                                              ensure_ascii=False),
                "menu_items": json.dumps(res.get("menu_items") or [],
                                         ensure_ascii=False),
                "menu_item_count": len(res.get("menu_items") or []),
                "sources_ok": "+".join(sorted(ok)),
                "completeness": r["meta"]["completeness"],
                "accepted": r["meta"]["accepted"], "status": r["meta"]["status"],
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
#  CANDIDATES  — load, filter, dedupe-cluster
# ══════════════════════════════════════════════════════════════════════════

def _col(df: pd.DataFrame, *names: str) -> str | None:
    lower = {c.lower().strip(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def load_candidates(csv_path: Path, limit: int | None,
                    location_override: str | None) -> tuple[list[dict], str]:
    if not csv_path.exists():
        sys.exit(f"CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    log.info("CSV: %d rows, %d columns", len(df), len(df.columns))

    c_code = _col(df, "place_code", "uCode", "id")
    c_name = _col(df, "business_name", "name", "place_name")
    c_addr = _col(df, "address", "full_address")
    c_lat, c_lng = _col(df, "latitude", "lat"), _col(df, "longitude", "lng", "lon")
    c_sub = _col(df, "sub_area", "subarea")
    c_area = _col(df, "area")
    c_city = _col(df, "city")
    c_post = _col(df, "postcode", "post_code")
    c_type = _col(df, "type")
    c_stype = _col(df, "sub_type", "subtype")
    c_pop = _col(df, "popularity_ranking", "popularity")
    if not (c_name and c_lat and c_lng):
        sys.exit("CSV needs at least business_name, latitude and longitude.")

    df = df.dropna(subset=[c_name, c_lat, c_lng]).copy()

    # ── location key: from --location, else the dominant area value ───────
    if location_override:
        location_key = slugify(location_override)
    elif c_area and df[c_area].notna().any():
        location_key = slugify(df[c_area].astype(str).mode().iloc[0])
    elif c_sub and df[c_sub].notna().any():
        location_key = slugify(df[c_sub].astype(str).mode().iloc[0])
    else:
        location_key = slugify(csv_path.stem)
    log.info("Location key: '%s'  ->  data/%s_all_data.{json,csv}",
             location_key, location_key)

    # ── cafe filter: sub_type is authoritative when present ───────────────
    n0 = len(df)
    if c_stype and df[c_stype].notna().any():
        mask = df[c_stype].astype(str).str.strip().str.lower().isin(CAFE_SUBTYPES)
        if mask.sum() >= max(3, 0.05 * n0):
            df = df[mask]
            log.info("sub_type filter: %d -> %d rows", n0, len(df))
        else:
            log.warning("sub_type matched only %d rows -- falling back to keywords",
                        int(mask.sum()))
            c_stype = None
    if not c_stype or len(df) == n0:
        hay = df[c_name].astype(str)
        if c_type:
            hay = hay + " " + df[c_type].astype(str)
        mask = hay.str.lower().str.contains("|".join(CAFE_HINTS), na=False)
        if mask.sum() >= 3:
            df = df[mask]
            log.info("keyword filter: %d -> %d rows", n0, len(df))

    # ── ordering ──────────────────────────────────────────────────────────
    if c_pop and pd.to_numeric(df[c_pop], errors="coerce").fillna(0).abs().sum() > 0:
        df["_pop"] = pd.to_numeric(df[c_pop], errors="coerce")
        df = df.sort_values("_pop", ascending=True, na_position="last")
        log.info("Ordered by %s", c_pop)
    else:
        log.warning("popularity_ranking is empty/all-zero in this CSV -- "
                    "keeping file order. Every row gets collected anyway.")

    cands: list[dict] = []
    for i, (_, row) in enumerate(df.iterrows()):
        lat, lng = to_float(row[c_lat]), to_float(row[c_lng])
        if lat is None or lng is None:
            continue
        cands.append({
            "place_code": str(row[c_code]) if c_code else f"AUTO{i:05d}",
            "business_name": str(row[c_name]).strip(),
            "bkoi_address": str(row[c_addr]).strip() if c_addr else None,
            "bkoi_lat": lat, "bkoi_lng": lng,
            "bkoi_sub_area": str(row[c_sub]).strip() if c_sub else None,
            "bkoi_area": str(row[c_area]).strip() if c_area else None,
            "bkoi_city": str(row[c_city]).strip() if c_city else None,
            "bkoi_postcode": str(row[c_post]).strip() if c_post else None,
            "bkoi_type": str(row[c_type]).strip() if c_type else None,
            "bkoi_sub_type": str(row[c_stype]).strip() if c_stype else None,
            "bkoi_popularity": to_float(row[c_pop]) if c_pop else None,
        })

    # ── duplicate clustering ──────────────────────────────────────────────
    # Your CSV has the same cafe under two place_codes metres apart. Fetching
    # both wastes requests, which is exactly what gets an IP flagged. Fetch
    # the leader once; copy the result to its aliases.
    groups: list[list[dict]] = []
    for c in cands:
        for g in groups:
            h = g[0]
            if (name_match_score(c["business_name"], h["business_name"]) >= 0.8
                    and haversine_m(c["bkoi_lat"], c["bkoi_lng"],
                                    h["bkoi_lat"], h["bkoi_lng"]) <= DUP_RADIUS_M):
                g.append(c)
                break
        else:
            groups.append([c])

    leaders: list[dict] = []
    n_alias = 0
    for g in groups:
        leader = g[0]
        leader["aliases"] = []
        for alias in g[1:]:
            alias["duplicate_of"] = leader["place_code"]
            leader["aliases"].append(alias)
            n_alias += 1
        leaders.append(leader)
    if n_alias:
        log.info("Duplicate clustering: %d rows -> %d unique cafes "
                 "(%d aliases share a fetch)", len(cands), len(leaders), n_alias)

    if limit:
        leaders = leaders[:limit]
    log.info("To collect: %d unique cafes", len(leaders))
    return leaders, location_key


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 0 · OPENSTREETMAP  (bulk, free, no key, no browser)
# ══════════════════════════════════════════════════════════════════════════

def bbox_of(cands: list[dict], pad_deg: float = 0.004) -> tuple:
    lats = [c["bkoi_lat"] for c in cands]
    lngs = [c["bkoi_lng"] for c in cands]
    return (min(lats) - pad_deg, min(lngs) - pad_deg,
            max(lats) + pad_deg, max(lngs) + pad_deg)


async def fetch_osm(api, cands: list[dict], guard: HostGuard) -> dict[str, dict]:
    """
    ONE Overpass request for the whole bounding box. 100 cafes cost 1 request.
    Returns place_code -> payload.
    """
    if not cands:
        return {}
    s, w, n, e = bbox_of(cands)
    query = f"""[out:json][timeout:90];
(
  node["amenity"~"^(cafe|restaurant|fast_food|bar|ice_cream)$"]({s},{w},{n},{e});
  way["amenity"~"^(cafe|restaurant|fast_food|bar|ice_cream)$"]({s},{w},{n},{e});
  node["shop"~"^(bakery|coffee|pastry|confectionery)$"]({s},{w},{n},{e});
  way["shop"~"^(bakery|coffee|pastry|confectionery)$"]({s},{w},{n},{e});
);
out center tags;"""

    elements = []
    for endpoint in OVERPASS_ENDPOINTS:
        host = urlparse(endpoint).netloc
        avail, why = guard.available(host)
        if not avail:
            log.info("   [osm] %s unavailable (%s)", host, why)
            continue
        await guard.acquire(host)
        try:
            guard.budget.spend(host)
            resp = await api.post(endpoint, data=query, timeout=95_000,
                                  headers={"User-Agent": OSM_UA,
                                           "Content-Type": "text/plain"})
            if resp.status != 200:
                guard.soft_fail(host, f"http-{resp.status}")
                log.warning("   [osm] %s returned %d", host, resp.status)
                continue
            elements = (await resp.json()).get("elements", [])
            guard.ok(host)
            log.info("   [osm] %s returned %d elements in the bounding box",
                     host, len(elements))
            break
        except Exception as e:                    # noqa: BLE001
            guard.soft_fail(host, type(e).__name__)
            log.warning("   [osm] %s: %s", host, str(e)[:110])
        finally:
            guard.release(host)

    if not elements:
        return {}

    # index OSM elements with coords + name
    pool = []
    for el in elements:
        tags = el.get("tags") or {}
        nm = tags.get("name") or tags.get("name:en")
        if not nm:
            continue
        if "center" in el:
            lat, lng = el["center"]["lat"], el["center"]["lon"]
        else:
            lat, lng = el.get("lat"), el.get("lon")
        if lat is None or lng is None:
            continue
        pool.append({"id": f"{el.get('type')}/{el.get('id')}",
                     "name": nm, "lat": lat, "lng": lng, "tags": tags})

    out: dict[str, dict] = {}
    for c in cands:
        best, best_score = None, 0.0
        for p in pool:
            dist = haversine_m(c["bkoi_lat"], c["bkoi_lng"], p["lat"], p["lng"])
            if dist > OSM_MATCH_RADIUS_M:
                continue
            sim = name_match_score(c["business_name"], p["name"])
            score = sim - (dist / OSM_MATCH_RADIUS_M) * 0.25
            if sim >= 0.34 and score > best_score:
                best, best_score = p, score
        if not best:
            continue
        t = best["tags"]
        payload: dict[str, Any] = {
            "osm_id": best["id"], "matched_name": best["name"],
            "name_match": round(name_match_score(c["business_name"], best["name"]), 3),
            "latitude": best["lat"], "longitude": best["lng"],
            "phone": norm_bd_phone(t.get("phone") or t.get("contact:phone")),
            "website": clean_url(t.get("website") or t.get("contact:website")),
            "category": t.get("amenity") or t.get("shop"),
        }
        hrs = parse_osm_hours(t.get("opening_hours", ""))
        if hrs:
            payload["opening_hours"] = hrs
        street = " ".join(x for x in (t.get("addr:housenumber"),
                                      t.get("addr:street")) if x)
        addr = ", ".join(x for x in (street, t.get("addr:suburb"),
                                     t.get("addr:city"), t.get("addr:postcode")) if x)
        if len(addr) > 8:
            payload["address"] = addr
        fb = t.get("contact:facebook") or t.get("facebook")
        if fb:
            payload["facebook_url"] = clean_url(fb if str(fb).startswith("http")
                                                else f"https://facebook.com/{fb}")
        ig = t.get("contact:instagram") or t.get("instagram")
        if ig:
            payload["instagram_url"] = clean_url(ig if str(ig).startswith("http")
                                                 else f"https://instagram.com/{ig}")
        # OSM marks dead venues with disused:/was: prefixes
        if any(k.startswith(("disused:", "was:", "removed:")) for k in t):
            payload["business_status"] = ST_PERM_CLOSED
            payload["business_status_note"] = "osm disused/was tag"
        out[c["place_code"]] = {k: v for k, v in payload.items() if not is_empty(v)}

    log.info("   [osm] matched %d / %d cafes", len(out), len(cands))
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
    """
    The only real IP guarantee available: prove the proxy is actually
    carrying traffic BEFORE any target site is touched.
    """
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
        log.warning("NO PROXY SET. Requests will come from your own IP (%s).",
                    direct_ip or "unknown")
        log.warning("This cannot be made block-proof. Pass --proxy to isolate it.")
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
    log.info("Preflight: direct=%s  via-proxy=%s", direct_ip, proxied_ip)
    if not proxied_ip:
        log.error("Proxy returned no IP. Refusing to start.")
        return False
    if direct_ip and proxied_ip == direct_ip:
        log.error("PROXY IS NOT WORKING -- your real IP is still visible. "
                  "Refusing to start.")
        return False
    log.info("Proxy verified. Your real IP is not exposed to target sites.")
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


async def goto(page, url: str, *, host: str, guard: HostGuard) -> str | None:
    avail, why = guard.available(host)
    if not avail:
        log.info("   skip %s (%s)", host, why)
        return None
    await guard.acquire(host)
    try:
        guard.budget.spend(host)
        resp = await page.goto(url, wait_until="domcontentloaded",
                               timeout=NAV_TIMEOUT_MS)
        if resp is not None and resp.status in (403, 429, 503):
            if resp.status == 429:
                guard.hard_block(host, "http-429")
            else:
                guard.soft_fail(host, f"http-{resp.status}")
            return None
        await page.wait_for_timeout(SETTLE_MS + random.randint(0, 2200))
        try:
            body = await page.inner_text("body", timeout=9000)
        except PWTimeout:
            body = await page.content()
        reason, hard = detect_block(page.url, body)
        if reason:
            (guard.hard_block if hard else guard.soft_fail)(host, reason)
            return None
        guard.ok(host)
        return body
    except PWTimeout:
        guard.soft_fail(host, "nav-timeout")
        return None
    except Exception as e:                        # noqa: BLE001
        guard.soft_fail(host, type(e).__name__)
        log.warning("   %s -> %s: %s", host, type(e).__name__, str(e)[:110])
        return None
    finally:
        guard.release(host)


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
                await page.wait_for_timeout(random.randint(500, 1200))
            return
        except Exception:
            continue


async def jsonld_nodes(page) -> list[dict]:
    out: list[dict] = []
    try:
        raws = await page.locator('script[type="application/ld+json"]').all_text_contents()
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
    hours: dict[str, str] = {}
    spec = node.get("openingHoursSpecification")
    if spec:
        for s in (spec if isinstance(spec, list) else [spec]):
            if not isinstance(s, dict):
                continue
            dow = s.get("dayOfWeek")
            opens, closes = s.get("opens"), s.get("closes")
            for d in (dow if isinstance(dow, list) else [dow]):
                key = DAY_ALIAS.get(str(d).rstrip('/').split('/')[-1].lower())
                if key:
                    hours[key] = (f"{hhmm_to_ampm(opens)} \u2013 {hhmm_to_ampm(closes)}"
                                  if opens and closes else "Closed")
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
                for d in (DAY_ORDER[i:j + 1] if i <= j
                          else DAY_ORDER[i:] + DAY_ORDER[:j + 1]):
                    hours[d] = val
            elif d1:
                hours[d1] = val
    return {d: hours[d] for d in DAY_ORDER if d in hours}


async def find_url(page, query: str, host_contains: str,
                   path_contains: str | None, guard: HostGuard) -> str | None:
    body = await goto(page, "https://html.duckduckgo.com/html/?q=" + quote_plus(query),
                      host="html.duckduckgo.com", guard=guard)
    if body is None:
        return None
    try:
        hrefs = await page.locator("a[href]").evaluate_all(
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)")
    except Exception:
        return None
    for href in hrefs:
        target = href
        if "duckduckgo.com/l/" in href:
            full = "https:" + href if href.startswith("//") else href
            target = (parse_qs(urlparse(full).query).get("uddg") or [None])[0]
        if not target or not target.startswith("http"):
            continue
        p = urlparse(target)
        if host_contains not in p.netloc:
            continue
        if path_contains and path_contains not in p.path:
            continue
        return target.split('?')[0]
    return None


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 1 · GOOGLE MAPS
# ══════════════════════════════════════════════════════════════════════════

async def scrape_google(page, cafe: dict, guard: HostGuard) -> tuple[dict, str | None, str]:
    host = "www.google.com"
    area = cafe.get("bkoi_sub_area") or cafe.get("bkoi_area") or "Dhaka"
    query = f"{cafe['business_name']}, {area}, Dhaka, Bangladesh"
    url = ("https://www.google.com/maps/search/" + quote_plus(query) +
           f"/@{cafe['bkoi_lat']},{cafe['bkoi_lng']},17z?hl=en")

    body = await goto(page, url, host=host, guard=guard)
    if body is None:
        return {}, None, "blocked_or_error"
    await click_first(page, SELECTORS["google"]["consent"], 2000)

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

    m = RE_PLACE_COORDS.search(place_url)
    if not m:
        try:
            m = RE_PLACE_COORDS.search(await page.content())
        except Exception:
            m = None
    if m:
        data["latitude"], data["longitude"] = float(m.group(1)), float(m.group(2))
    else:
        mv = RE_VIEW_COORDS.search(place_url)
        if mv:
            data["latitude"], data["longitude"] = float(mv.group(1)), float(mv.group(2))
    mc = RE_CID.search(place_url)
    if mc:
        data["google_cid"] = mc.group(1)

    title = await first_text(page, SELECTORS["google"]["title"])
    if title:
        data["matched_name"] = title
        data["name_match"] = round(name_match_score(cafe["business_name"], title), 3)
    if data.get("latitude") is not None:
        dist = haversine_m(cafe["bkoi_lat"], cafe["bkoi_lng"],
                           data["latitude"], data["longitude"])
        data["distance_from_bkoi_m"] = round(dist)
        if dist > GEO_TOL_M and (data.get("name_match") or 0) < 0.45:
            log.warning("   [g] %.0fm away + weak name match -- rejecting", dist)
            return {}, place_url, "geo_mismatch"

    # ── OPEN / TEMPORARILY CLOSED / PERMANENTLY CLOSED ────────────────────
    badge = await first_text(page, SELECTORS["google"]["status_badge"]) or ""
    status, note = detect_status_from_text(badge)
    if status == ST_UNKNOWN:
        # panel text only, so a review mentioning "closed" cannot poison it
        status, note = detect_status_from_text(body[:2500])
    data["business_status"] = status if status != ST_UNKNOWN else ST_OPERATIONAL
    if note:
        data["business_status_note"] = f"g: {note}"
        log.info("   [g] %s", note.upper())

    raw_phone = await first_attr(page, SELECTORS["google"]["phone_btn"], "data-item-id")
    if raw_phone:
        data["phone"] = norm_bd_phone(raw_phone.replace("phone:tel:", ""))
    addr = await first_text(page, SELECTORS["google"]["address_btn"], min_len=8)
    if addr:
        data["address"] = re.sub(r'\s+', ' ', addr.replace("Address:", "")).strip()
    site = clean_url(await first_attr(page, SELECTORS["google"]["website_btn"], "href"))
    if site and "google." not in urlparse(site).netloc:
        data["website"] = site
    pc = await first_text(page, SELECTORS["google"]["plus_code"], min_len=4)
    if pc:
        data["plus_code"] = pc.replace("Plus code:", "").strip()
    cat = await first_text(page, SELECTORS["google"]["category"], min_len=3)
    if cat:
        data["category"] = cat.strip()

    rtxt = await first_text(page, SELECTORS["google"]["rating"])
    if rtxt:
        mr = RE_RATING.search(rtxt.replace(',', '.'))
        if mr:
            v = to_float(mr.group(1))
            if v and 0 < v <= 5:
                data["rating"] = v
    ctxt = (await first_text(page, SELECTORS["google"]["review_count"])
            or await first_attr(page, SELECTORS["google"]["review_count"], "aria-label"))
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

    hours: dict[str, str] = {}
    await click_first(page, SELECTORS["google"]["hours_toggle"], 1800)
    for tsel in SELECTORS["google"]["hours_table"]:
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

    snippets: list[dict] = []
    if await click_first(page, SELECTORS["google"]["reviews_tab"], 2600):
        await scroll_panel(page, SELECTORS["google"]["scroll_panel"], steps=4)
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

    try:
        html = await page.content()
    except Exception:
        html = body
    mfb, mig = RE_FB.search(html), RE_IG.search(html)
    if mfb:
        data["facebook_url"] = clean_url(mfb.group(0))
    if mig:
        data["instagram_url"] = clean_url(mig.group(0))
    if "phone" not in data:
        ph = find_phones(body)
        if ph:
            data["phone"] = ph[0]
    return data, place_url, "ok"


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 2 · FOODPANDA
# ══════════════════════════════════════════════════════════════════════════

async def scrape_foodpanda(page, cafe: dict, known: str | None,
                           guard: HostGuard) -> tuple[dict, str | None, str]:
    host = "www.foodpanda.com.bd"
    name = cafe["business_name"]
    area = cafe.get("bkoi_sub_area") or "Gulshan"
    url = known or await find_url(
        page, f'site:foodpanda.com.bd "{name}" {area} Dhaka',
        "foodpanda.com.bd", "/restaurant/", guard)
    if not url:
        return {}, None, "not_found"

    body = await goto(page, url, host=host, guard=guard)
    if body is None:
        return {}, url, "blocked_or_error"
    await click_first(page, SELECTORS["foodpanda"]["cookie"], 1200)

    data: dict[str, Any] = {}
    node = pick_business_node(await jsonld_nodes(page))
    if node:
        vendor = str(node.get("name") or "")
        if vendor and name_match_score(name, vendor) < 0.2:
            log.warning("   [fp] vendor '%s' != '%s' -- skipping", vendor[:36], name)
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
            j = ", ".join(str(addr[k]) for k in
                          ("streetAddress", "addressLocality", "addressRegion",
                           "postalCode") if addr.get(k))
            if len(j) > 8:
                data["address"] = j
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
            data["opening_hours"] = sh
        if node.get("priceRange"):
            data["price_range"] = str(node["priceRange"]).strip()

    # Foodpanda "closed" usually means closed RIGHT NOW, not shut down. Only
    # permanent wording is promoted to a status; everything else is a note.
    flag = await first_text(page, SELECTORS["foodpanda"]["closed_flag"]) or ""
    st, note = detect_status_from_text(flag)
    if st in (ST_PERM_CLOSED, ST_TEMP_CLOSED):
        data["business_status"] = st
        data["business_status_note"] = f"fp: {note}"
    elif flag:
        data["business_status_note"] = f"fp banner: {flag[:70]}"

    if not data.get("rating"):
        rt = await first_text(page, SELECTORS["foodpanda"]["rating"])
        mr = RE_RATING.search(rt or "")
        if mr:
            v = to_float(mr.group(1))
            if v and 0 < v <= 5:
                data["rating"] = v
    if not data.get("review_count"):
        n = to_int(await first_text(page, SELECTORS["foodpanda"]["review_count"]))
        if n:
            data["review_count"] = n

    items: list[dict] = []
    await scroll_panel(page, ['[data-testid="menu"]', "main"], steps=6)
    for csel in SELECTORS["foodpanda"]["product_card"]:
        try:
            cards = await page.locator(csel).all()
        except Exception:
            continue
        if not cards:
            continue
        for card in cards[:70]:
            try:
                txt = re.sub(r'\s+', ' ', await card.inner_text(timeout=900)).strip()
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
            mp = RE_PRICE_BDT.search(txt)
            price = to_float(mp.group(1)) if mp else None
            if not item_name:
                item_name = RE_PRICE_BDT.sub('', txt).strip().split('\n')[0]
            item_name = re.sub(r'\s+', ' ', item_name)[:80].strip(' -,')
            if item_name and 2 < len(item_name) < 80:
                items.append({"item": item_name, "price_bdt": price,
                              "currency": "BDT", "source_tag": "fp_menu"})
        if items:
            break
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
        ph = find_phones(body)
        if ph:
            data["phone"] = ph[0]
    return data, url, "ok" if data else "empty"


# ══════════════════════════════════════════════════════════════════════════
#  SOURCE 3 · TRIPADVISOR
# ══════════════════════════════════════════════════════════════════════════

async def scrape_tripadvisor(page, cafe: dict,
                             guard: HostGuard) -> tuple[dict, str | None, str]:
    host = "www.tripadvisor.com"
    name = cafe["business_name"]
    url = await find_url(page, f'site:tripadvisor.com "{name}" Dhaka restaurant',
                         "tripadvisor.", "Restaurant_Review", guard)
    if not url:
        return {}, None, "not_found"
    body = await goto(page, url, host=host, guard=guard)
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
            j = ", ".join(str(addr[k]) for k in
                          ("streetAddress", "addressLocality") if addr.get(k))
            if len(j) > 8:
                data["address"] = j
        tel = norm_bd_phone(node.get("telephone"))
        if tel:
            data["phone"] = tel
        if node.get("priceRange"):
            data["price_range"] = str(node["priceRange"]).strip()

    flag = await first_text(page, SELECTORS["tripadvisor"]["closed_flag"]) or ""
    st, note = detect_status_from_text(flag)
    if st != ST_UNKNOWN:
        data["business_status"] = st
        data["business_status_note"] = f"ta: {note}"

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
#  PER-CAFE ORCHESTRATION
# ══════════════════════════════════════════════════════════════════════════

async def enrich_cafe(ctx, cafe: dict, rec: dict, guard: HostGuard,
                      sources: list[str]) -> dict:
    rec["meta"]["attempts"] += 1
    rec["meta"]["last_attempt"] = now_iso()

    async def run(src: str):
        page = await ctx.new_page()
        try:
            if src == SRC_G:
                return src, *(await scrape_google(page, cafe, guard))
            if src == SRC_FP:
                return src, *(await scrape_foodpanda(
                    page, cafe, rec["sources"].get(SRC_FP, {}).get("url"), guard))
            if src == SRC_TA:
                return src, *(await scrape_tripadvisor(page, cafe, guard))
            return src, {}, None, "unknown_source"
        except Exception as e:                    # noqa: BLE001
            log.warning("   [%s] unhandled %s: %s", src, type(e).__name__, str(e)[:110])
            return src, {}, None, f"error:{type(e).__name__}"
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # Skip sources already satisfied, and any host disabled by a block.
    todo = []
    for s in sources:
        if rec["sources"].get(s, {}).get("status") == "ok" and s == SRC_TA:
            continue
        host = {SRC_G: "www.google.com", SRC_FP: "www.foodpanda.com.bd",
                SRC_TA: "www.tripadvisor.com"}[s]
        avail, why = guard.available(host)
        if not avail:
            log.info("   [%s] skipped: %s", s, why)
            continue
        todo.append(s)

    if todo:
        for r in await asyncio.gather(*(run(s) for s in todo),
                                      return_exceptions=True):
            if isinstance(r, BaseException):
                log.warning("   task error: %s", r)
                continue
            src, data, url, status = r
            merge_source(rec, src, data, url, status)
            keys = sorted(k for k in data if k not in
                          ("matched_name", "name_match", "distance_from_bkoi_m"))
            log.info("   [%-2s] %-16s %s", src, status, ", ".join(keys) or "-")

    g = rec["sources"][SRC_G]["data"]
    web = {k: g[k] for k in ("facebook_url", "instagram_url") if g.get(k)}
    if web:
        merge_source(rec, SRC_WEB, web, g.get("website"), "derived")

    resolve_record(rec)
    score_record(rec)
    stt = {s: rec["sources"][s]["status"] for s in rec["sources"]}
    if rec["meta"]["accepted"]:
        rec["meta"]["status"] = "accepted"
    elif all(stt[s] in ("not_found", "geo_mismatch", "name_mismatch")
             for s in (SRC_G, SRC_FP)):
        rec["meta"]["status"] = "not_found"
    elif any(v == "blocked_or_error" for v in stt.values()):
        rec["meta"]["status"] = "partial_blocked"
    else:
        rec["meta"]["status"] = "partial"
    return rec


def propagate_to_aliases(leader_rec: dict, leader: dict, store: Store,
                         location_key: str) -> int:
    """Copy the leader's harvest onto its duplicate place_codes."""
    n = 0
    for alias in leader.get("aliases", []):
        arec = store.get(alias["place_code"]) or blank_record(alias, location_key)
        arec["duplicate_of"] = leader["place_code"]
        for src in ALL_SOURCE_KEYS:
            blk = leader_rec["sources"].get(src, {})
            if blk.get("status") in (None, "pending"):
                continue
            merge_source(arec, src, blk.get("data", {}), blk.get("url"),
                         blk.get("status"))
        arec["meta"]["last_attempt"] = now_iso()
        arec["meta"]["notes"] = list({*arec["meta"].get("notes", []),
                                      f"shared fetch with {leader['place_code']}"})
        resolve_record(arec)
        score_record(arec)
        arec["meta"]["status"] = ("accepted" if arec["meta"]["accepted"]
                                  else "duplicate_partial")
        store.upsert(arec)
        n += 1
    return n


# ══════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════

async def run_pipeline(args) -> None:
    cands, location_key = load_candidates(Path(args.csv), args.limit, args.location)
    if not cands:
        sys.exit("No cafes matched the filters.")
    if args.dry_run:
        for i, c in enumerate(cands, 1):
            log.info("  %3d. %-40s %s%s", i, c["business_name"][:40],
                     c["place_code"],
                     f"  (+{len(c.get('aliases', []))} alias)" if c.get("aliases") else "")
        est = len(cands) * PROFILES[args.pace]["www.google.com"] / 60
        log.info("--dry-run. No network touched. Rough Google-only time at "
                 "'%s' pace: ~%.0f min for %d cafes.", args.pace, est, len(cands))
        return

    store = Store(location_key)
    budget = Budget(DATA_DIR / f"{location_key}_budget.json", args.budget_multiplier)
    guard = HostGuard(PROFILES[args.pace], budget)
    sources = SCRAPED_SOURCES if args.only == "all" else [args.only]
    log.info("Pace=%s  sources=%s  batch=%d  budget today: %s",
             args.pace, "+".join([SRC_OSM] + sources if not args.no_osm else sources),
             BATCH_SIZE, budget.report())

    proxies = []
    if args.proxy_file:
        proxies = [l.strip() for l in Path(args.proxy_file).read_text().splitlines()
                   if l.strip() and not l.startswith("#")]
    elif args.proxy:
        proxies = [args.proxy]

    started = now_iso()
    async with async_playwright() as pw:
        # ── PREFLIGHT: refuse to start if the proxy is not carrying traffic ──
        if not await preflight_ip(pw, proxies[0] if proxies else None):
            sys.exit("Preflight failed. Nothing was requested from any target site.")
        if not proxies and args.require_proxy:
            sys.exit("--require-proxy set but no proxy given. Stopping.")

        # ── OSM bulk: one request covers every cafe ───────────────────────
        if not args.no_osm:
            api = await pw.request.new_context(
                proxy=proxy_config(proxies[0]) if proxies else None,
                extra_http_headers={"User-Agent": OSM_UA})
            try:
                osm_map = await fetch_osm(api, cands, guard)
            finally:
                await api.dispose()
                budget.flush()
            for c in cands:
                payload = osm_map.get(c["place_code"])
                if not payload:
                    continue
                rec = store.get(c["place_code"]) or blank_record(c, location_key)
                merge_source(rec, SRC_OSM, payload, None, "ok")
                resolve_record(rec)
                score_record(rec)
                store.upsert(rec)
            store.flush()

        # ── batches of 10 ─────────────────────────────────────────────────
        pending = [c for c in cands
                   if not (store.get(c["place_code"]) or {})
                   .get("meta", {}).get("accepted")]
        log.info("%d cafes need scraped sources (%d already complete from OSM/"
                 "previous runs)", len(pending), len(cands) - len(pending))

        batches = [pending[i:i + BATCH_SIZE]
                   for i in range(0, len(pending), BATCH_SIZE)]
        for bi, batch in enumerate(batches, 1):
            if all(h in guard.disabled for h in
                   ("www.google.com", "www.foodpanda.com.bd")):
                log.error("Both primary hosts blocked. Stopping this run to "
                          "protect your IP. Re-run tomorrow.")
                break

            proxy = proxies[(bi - 1) % len(proxies)] if proxies else None
            profile = f"{location_key}_{(bi - 1) % max(1, len(proxies))}"
            log.info("\n%s\nBATCH %d/%d  (%d cafes)  proxy=%s\n  budget: %s\n%s",
                     "=" * 72, bi, len(batches), len(batch),
                     (urlparse(proxy).hostname if proxy else "NONE (your own IP)"),
                     budget.report(), "=" * 72)

            ctx = await launch_context(pw, args.headless, proxy, profile)
            try:
                for i, cafe in enumerate(batch, 1):
                    pc = cafe["place_code"]
                    rec = store.get(pc) or blank_record(cafe, location_key)
                    log.info("\n [%d/%d] %s (%s)", i, len(batch),
                             cafe["business_name"], pc)
                    try:
                        rec = await enrich_cafe(ctx, cafe, rec, guard, sources)
                    except Exception as e:        # noqa: BLE001
                        log.error("   fatal: %s", e)
                        rec["meta"]["notes"].append(f"{now_iso()} {type(e).__name__}")
                    store.upsert(rec)
                    n_alias = propagate_to_aliases(rec, cafe, store, location_key)
                    store.flush()
                    budget.flush()
                    st = rec["resolved"].get("business_status")
                    flag = (" [TEMP CLOSED]" if st == ST_TEMP_CLOSED else
                            " [PERM CLOSED]" if st == ST_PERM_CLOSED else "")
                    log.info("   -> %s %.0f%%%s%s  missing=%s",
                             "ACCEPTED" if rec["meta"]["accepted"] else "partial ",
                             rec["meta"]["completeness"] * 100, flag,
                             f"  (+{n_alias} alias)" if n_alias else "",
                             missing_report(rec) or "none")
                    await asyncio.sleep(random.uniform(3.0, 7.0))
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
    store.progress["runs"].append({
        "started": started, "finished": now_iso(), "pace": args.pace,
        "proxy_used": bool(proxies), "accepted_after": store.accepted_count,
        "events": guard.events, "budget_after": budget.day,
    })
    store.flush()
    print_summary(store, guard, budget)


def print_summary(store: Store, guard: HostGuard, budget: Budget) -> None:
    recs = sorted(store.records.values(),
                  key=lambda r: (not r["meta"]["accepted"],
                                 -r["meta"]["completeness"]))
    W = 120
    print("\n" + "-" * W)
    print(f"{'#':<4}{'Cafe':<30}{'OK':<3}{'Full':>5} {'Status':<12}"
          f"{'gRat':>5}{'fpRat':>6} {'g-Hrs':>6}{'fp-Hrs':>7}{'osm-Hrs':>8}"
          f"{'Menu':>5}  Phone")
    print("-" * W)
    for i, r in enumerate(recs, 1):
        res = r["resolved"]
        d = {s: r["sources"].get(s, {}).get("data", {}) for s in ALL_SOURCE_KEYS}
        print(f"{i:<4}{r['business_name'][:29]:<30}"
              f"{'Y' if r['meta']['accepted'] else '.':<3}"
              f"{r['meta']['completeness']*100:>4.0f}% "
              f"{str(res.get('business_status') or '-')[:11]:<12}"
              f"{str(d[SRC_G].get('rating') or '-'):>5}"
              f"{str(d[SRC_FP].get('rating') or '-'):>6} "
              f"{len(d[SRC_G].get('opening_hours') or {}):>5}d"
              f"{len(d[SRC_FP].get('opening_hours') or {}):>6}d"
              f"{len(d[SRC_OSM].get('opening_hours') or {}):>7}d"
              f"{len(res.get('menu_items') or []):>5}  {res.get('phone') or '-'}")
    print("-" * W)
    n_ok = sum(1 for r in recs if r["meta"]["accepted"])
    tc = sum(1 for r in recs if r["resolved"].get("business_status") == ST_TEMP_CLOSED)
    pcl = sum(1 for r in recs if r["resolved"].get("business_status") == ST_PERM_CLOSED)
    print(f"\n  Accepted {n_ok}/{len(recs)}   temporarily closed: {tc}   "
          f"permanently closed: {pcl}")
    print(f"  Budget spent today: {budget.report()}")
    if guard.disabled:
        print("\n  !! HOSTS BLOCKED THIS RUN: " +
              ", ".join(f"{h} ({why})" for h, why in guard.disabled.items()))
        print("     Do NOT re-run against these today. Use --pace paranoid, or a proxy.")
    print(f"\n  JSON  {store.json_path}")
    print(f"  CSV   {store.csv_path}")
    print(f"  Log   {LOG_PATH}\n")
    if n_ok < len(recs):
        print("  Not full yet -- run the same command again tomorrow. It will "
              "only work on what is still missing.\n")


# ══════════════════════════════════════════════════════════════════════════
#  PROBE
# ══════════════════════════════════════════════════════════════════════════

async def run_probe(args) -> None:
    name = args.probe_name or "Windy Terrace"
    budget = Budget(DATA_DIR / "_probe_budget.json", 1.0)
    guard = HostGuard(PROFILES[args.pace], budget)
    async with async_playwright() as pw:
        if not await preflight_ip(pw, args.proxy):
            sys.exit("Preflight failed.")
        ctx = await launch_context(pw, args.headless, args.proxy, "probe")
        page = await ctx.new_page()
        if args.probe == "google":
            url = ("https://www.google.com/maps/search/" +
                   quote_plus(f"{name}, Gulshan, Dhaka") + "?hl=en")
            host = "www.google.com"
        elif args.probe == "foodpanda":
            url = await find_url(page, f'site:foodpanda.com.bd "{name}" Dhaka',
                                 "foodpanda.com.bd", "/restaurant/", guard)
            host = "www.foodpanda.com.bd"
        else:
            url = await find_url(page, f'site:tripadvisor.com "{name}" Dhaka',
                                 "tripadvisor.", "Restaurant_Review", guard)
            host = "www.tripadvisor.com"
        if not url:
            await ctx.close()
            sys.exit("No URL found to probe.")
        body = await goto(page, url, host=host, guard=guard)
        base = PROBE_DIR / f"{args.probe}_{datetime.now():%Y%m%d_%H%M%S}"
        try:
            base.with_suffix(".html").write_text(await page.content(), encoding="utf-8")
            base.with_suffix(".txt").write_text(body or "(blocked)", encoding="utf-8")
            await page.screenshot(path=str(base.with_suffix(".png")))
        except Exception as e:                    # noqa: BLE001
            log.warning("probe write failed: %s", e)
        nodes = await jsonld_nodes(page)
        (base.parent / f"{base.name}_jsonld.json").write_text(
            json.dumps(nodes, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Saved %s.{html,txt,png} + _jsonld.json", base)
        log.info("Business node: %s",
                 json.dumps(pick_business_node(nodes) or {}, ensure_ascii=False)[:700])
        if not args.headless:
            await asyncio.sleep(30)
        await ctx.close()
        budget.flush()


# ══════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Barikoi multi-source cafe enrichment, one output per location.")
    p.add_argument("--csv", help="Barikoi places CSV")
    p.add_argument("--location", help="override the auto-detected location key")
    p.add_argument("--limit", type=int, help="cap unique cafes this run")
    p.add_argument("--pace", default="safe", choices=list(PROFILES),
                   help="safe (default) | paranoid | aggressive")
    p.add_argument("--budget-multiplier", type=float, default=1.0,
                   help="scale the daily per-host request caps")
    p.add_argument("--only", default="all",
                   choices=["all", "g", "fp", "ta", "google", "foodpanda", "tripadvisor"])
    p.add_argument("--no-osm", action="store_true", help="skip the OpenStreetMap step")
    p.add_argument("--proxy", help="http://user:pass@host:port")
    p.add_argument("--proxy-file", help="one proxy per line, rotated per batch")
    p.add_argument("--require-proxy", action="store_true",
                   help="refuse to run without a verified proxy")
    p.add_argument("--check-ip", action="store_true",
                   help="verify proxy/IP and exit without scraping")
    p.add_argument("--headless", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--probe", choices=["google", "foodpanda", "tripadvisor"])
    p.add_argument("--probe-name")
    a = p.parse_args(argv)
    a.only = {"google": SRC_G, "foodpanda": SRC_FP,
              "tripadvisor": SRC_TA}.get(a.only, a.only)
    if not (a.csv or a.probe or a.check_ip):
        p.error("--csv is required (or use --probe / --check-ip)")
    return a


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
        log.info("\nInterrupted. All files are intact (atomic writes). "
                 "Re-run to continue where you stopped.")


if __name__ == "__main__":
    main()