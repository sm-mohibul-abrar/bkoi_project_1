"""Small shared helpers: atomic IO, geo distance, text-to-number parsing,
phone/URL normalisation and name similarity.

Everything here is pure (no network, no filesystem beyond atomic_write), so
it is fully covered by unit tests.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger("cafe_pipeline")

# ── time / IO ───────────────────────────────────────────────────────────────


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z")


def today_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def atomic_write(path: Path, text: str) -> None:
    """Write text to *path* via tmp file + rename, keeping a .bak of the old
    content. A crash mid-write can never truncate an existing file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(text, encoding="utf-8")
    if path.exists():
        bak = path.with_suffix(path.suffix + ".bak")
        try:
            bak.unlink(missing_ok=True)
            path.replace(bak)
        except OSError:
            pass
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write(path, json.dumps(payload, ensure_ascii=False, indent=2))


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped.upper() == "N/A"
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


# ── geo ─────────────────────────────────────────────────────────────────────


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in metres between two WGS84 points."""
    radius = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(d_lng / 2) ** 2)
    return 2 * radius * math.asin(min(1.0, math.sqrt(a)))


# ── numbers ─────────────────────────────────────────────────────────────────

_RE_COUNT = re.compile(r"([\d][\d,\s.]*\d|\d)\s*([kKmM])?")
_RE_RATING = re.compile(r"\b([0-5](?:[.,]\d)?)\b")


def parse_count(raw: Any) -> tuple[int | None, bool]:
    """Parse a displayed count into (value, approximate).

    Handles "1,234" -> (1234, False), "1.2K" -> (1200, True),
    "3M" -> (3000000, True). Returns (None, False) when nothing numeric.
    """
    if raw is None:
        return None, False
    match = _RE_COUNT.search(str(raw).replace(" ", " "))
    if not match:
        return None, False
    number = match.group(1).replace(",", "").replace(" ", "")
    try:
        value = float(number)
    except ValueError:
        return None, False
    suffix = match.group(2)
    if suffix:
        value *= 1_000 if suffix.lower() == "k" else 1_000_000
        if value == int(value):
            return int(value), True
        return int(value), True
    return int(value), False


def parse_rating(raw: Any) -> float | None:
    """Extract a 0..5 rating value from displayed text like '4.6' or
    '4,6 stars'."""
    if raw is None:
        return None
    match = _RE_RATING.search(str(raw).replace(",", "."))
    if not match:
        return None
    value = float(match.group(1))
    return value if 0 < value <= 5 else None


def to_float(raw: Any) -> float | None:
    try:
        value = float(str(raw).strip().replace(",", ""))
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


# ── phones / URLs ───────────────────────────────────────────────────────────


def norm_bd_phone(raw: Any) -> str | None:
    """Normalise a Bangladeshi phone number to +880XXXXXXXXX."""
    if is_empty(raw):
        return None
    digits = re.sub(r"[^\d+]", "", str(raw)).lstrip("+")
    if digits.startswith("00880"):
        digits = digits[5:]
    elif digits.startswith("880"):
        digits = digits[3:]
    digits = digits.lstrip("0")
    if re.fullmatch(r"1[3-9]\d{8}", digits):        # mobile
        return "+880" + digits
    if re.fullmatch(r"2\d{7,8}", digits):           # Dhaka landline
        return "+880" + digits
    return None


def clean_url(raw: Any) -> str | None:
    """Canonicalise an http(s) URL: drop fragments, query noise, trailing
    slashes. Rejects non-http and empty input."""
    if is_empty(raw) or not str(raw).startswith("http"):
        return None
    url = str(raw).split("#")[0]
    if not urlparse(url).netloc:
        return None
    return url.split("?")[0].rstrip("/")


# ── names ───────────────────────────────────────────────────────────────────


def name_tokens(value: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", str(value).lower())
            if len(t) > 2}


def name_match_score(a: str, b: str) -> float:
    """Jaccard similarity of meaningful name tokens, 0..1."""
    tokens_a, tokens_b = name_tokens(a), name_tokens(b)
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)
