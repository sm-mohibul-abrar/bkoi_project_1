"""Importer for raw JSON left behind by earlier scraper versions.

``data/raw/google_maps/run_*/`` holds payloads in four different profile
shapes and three review shapes. This module normalises every one of them
into the canonical M-1 payload (the same shape the live Google Maps source
emits), picks the richest payload per place_code, and hands them to the
store so previous scraping effort is not wasted.

Privacy rule from the spec is enforced here too: reviewer names and review
image links are dropped at import time, never stored in outputs.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .hours import hours_lines_from_text, normalize_hours_dict
from .utils import clean_url, is_empty, norm_bd_phone, parse_count, parse_rating

log = logging.getLogger("cafe_pipeline.legacy")

RUN_DIR_RE = re.compile(r"run_(\d{8}_\d{6})$")

# Fields counted when deciding which legacy payload is "richest".
RICHNESS_KEYS = ("rating", "review_count", "reviews", "hours", "address",
                 "phone", "website", "facebook_url", "instagram_url",
                 "category", "price_level")


def _run_timestamp(run_dir: Path) -> str | None:
    match = RUN_DIR_RE.search(run_dir.name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1),
                                 "%Y%m%d_%H%M%S").isoformat(timespec="seconds")
    except ValueError:
        return None


def _first_usable(values: Any) -> str | None:
    """First non-N/A entry of a scalar-or-list field."""
    if values is None:
        return None
    items = values if isinstance(values, (list, tuple)) else [values]
    for item in items:
        if not is_empty(item):
            return str(item).strip()
    return None


def _convert_hours(raw: Any) -> tuple[dict[str, str], list[str]]:
    if isinstance(raw, dict):
        schedule = raw.get("schedule")
        if isinstance(schedule, dict):
            return normalize_hours_dict(schedule)
        if isinstance(schedule, str):
            return _hours_from_text(schedule)
        return normalize_hours_dict(raw)
    if isinstance(raw, (list, tuple)):
        return _hours_from_text(" ".join(str(v) for v in raw))
    if isinstance(raw, str):
        return _hours_from_text(raw)
    return {}, []


def _hours_from_text(text: str) -> tuple[dict[str, str], list[str]]:
    return hours_lines_from_text(text), []


def _convert_reviews(raw: list[Any]) -> list[dict[str, Any]]:
    """Any legacy review shape -> {stars, text, date}. Identity stripped."""
    out: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        text = item.get("text") or item.get("full_review_text")
        if not text or not str(text).strip():
            continue
        stars = item.get("rating") or item.get("stars")
        try:
            stars = int(stars) if stars is not None else None
        except (TypeError, ValueError):
            stars = None
        date = (item.get("date") or item.get("date_mmyy")
                or item.get("time_ago"))
        out.append({
            "stars": stars,
            "text": re.sub(r"\s+", " ", str(text)).strip()[:600],
            "date": str(date).strip() if date else None,
        })
    return out


def convert_profile(profile: dict[str, Any], run_ts: str | None,
                    extra_reviews: list[Any] | None) -> dict[str, Any]:
    """One legacy profile dict -> canonical M-1 payload."""
    payload: dict[str, Any] = {}

    rating = parse_rating(profile.get("google_rating"))
    if rating is not None:
        payload["rating"] = rating
    count, approximate = parse_count(
        profile.get("google_review_count")
        or profile.get("total_reviews_on_google"))
    if count is not None:
        payload["review_count"] = count
        payload["review_count_approximate"] = approximate

    contact = profile.get("contact_and_location") or {}
    phone = (profile.get("phone")
             or _first_usable(profile.get("phone_numbers"))
             or _first_usable(contact.get("phone")))
    phone = norm_bd_phone(phone)
    if phone:
        payload["phone"] = phone

    address = (_first_usable(contact.get("address_from_map"))
               or _first_usable(contact.get("address_from_google")))
    if address:
        payload["address"] = address

    coords = contact.get("coordinates") or {}
    for lat_key, lng_key in (("gmaps_lat", "gmaps_lng"), ("lat", "lng")):
        if coords.get(lat_key) is not None and coords.get(lng_key) is not None:
            payload["latitude"] = float(coords[lat_key])
            payload["longitude"] = float(coords[lng_key])
            break

    hours_raw = (profile.get("opening_hours")
                 or profile.get("opening_and_closing_hours")
                 or profile.get("opening_closing_hours"))
    schedule, _notes = _convert_hours(hours_raw)
    if len(schedule) >= 5:
        payload["hours"] = schedule

    reviews = _convert_reviews(profile.get("reviews") or [])
    if not reviews and extra_reviews:
        reviews = _convert_reviews(extra_reviews)
    if reviews:
        payload["reviews"] = reviews[:5]

    status = _first_usable(profile.get("status"))
    if status and status.upper() in ("OPERATIONAL", "TEMPORARILY_CLOSED",
                                     "PERMANENTLY_CLOSED"):
        payload["business_status"] = status.upper()

    website = clean_url(profile.get("website"))
    if website:
        payload["website"] = website
    for key in ("facebook_url", "instagram_url"):
        url = clean_url(profile.get(key))
        if url:
            payload[key] = url
    category = _first_usable(profile.get("category"))
    if category:
        payload["category"] = category
    price_level = _first_usable(profile.get("cafe_price_level"))
    if price_level:
        payload["price_level"] = price_level
    menu_url = clean_url(_first_usable(profile.get("menu_link")))
    if menu_url:
        payload["menu_url"] = menu_url

    if run_ts:
        payload["fetched_at"] = run_ts
        payload["fetched_at_source"] = "run directory name"
    return payload


def _richness(payload: dict[str, Any]) -> int:
    return sum(1 for key in RICHNESS_KEYS
               if payload.get(key) not in (None, [], {}))


def load_legacy_payloads(raw_root: Path) -> dict[str, dict[str, Any]]:
    """Best canonical payload per place_code across all runs."""
    best: dict[str, tuple[int, str, dict[str, Any]]] = {}
    for run_dir in sorted(p for p in raw_root.glob("run_*") if p.is_dir()):
        run_ts = _run_timestamp(run_dir)
        profiles_dir = run_dir / "profiles"
        reviews_dir = run_dir / "map_reviews"
        if not profiles_dir.is_dir():
            continue
        for path in sorted(profiles_dir.glob("*.json")):
            try:
                profile = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("skipping corrupt %s", path.name)
                continue
            if not isinstance(profile, dict):
                continue
            place_code = profile.get("place_code") or path.stem.split("_")[0]
            extra_reviews = None
            reviews_file = reviews_dir / f"{place_code}_reviews.json"
            if reviews_file.exists():
                try:
                    extra_reviews = json.loads(
                        reviews_file.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    extra_reviews = None
            payload = convert_profile(profile, run_ts, extra_reviews)
            score = _richness(payload)
            previous = best.get(place_code)
            # Later runs win ties: freshest data at equal richness.
            if previous is None or score >= previous[0]:
                best[place_code] = (score, run_ts or "", payload)

    log.info("legacy import: %d place_codes usable from %s",
             len(best), raw_root)
    return {code: entry[2] for code, entry in best.items()}
