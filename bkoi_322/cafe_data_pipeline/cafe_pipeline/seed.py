"""Seed loading: Barikoi places CSV -> in-scope cafe candidates.

Applies the project scope rules (Gulshan only, real beverage cafes), keeps
every rejected row in an ``excluded`` list with a reason so nothing
disappears silently, and clusters duplicate listings of the same venue so it
is fetched from Google once instead of twice.
"""

from __future__ import annotations

import csv
import logging
from typing import Any

from .settings import Settings
from .utils import haversine_m, name_match_score, to_float

log = logging.getLogger("cafe_pipeline.seed")

CAFE_SUBTYPE_HINTS = ("cafe", "café", "coffee", "bakery", "tea", "dessert",
                      "patisserie", "pastry")


def _field(row: dict[str, str], *names: str) -> str | None:
    """Column lookup tolerant of case/space differences in the header."""
    lowered = {k.strip().lower(): k for k in row}
    for name in names:
        key = lowered.get(name)
        if key and str(row.get(key) or "").strip():
            return str(row[key]).strip()
    return None


def parse_seed_row(row: dict[str, str]) -> dict[str, Any] | None:
    place_code = _field(row, "place_code", "ucode", "id")
    name = _field(row, "business_name", "name", "place_name")
    lat, lng = (to_float(_field(row, "latitude", "lat")),
                to_float(_field(row, "longitude", "lng", "lon")))
    if not (place_code and name and lat is not None and lng is not None):
        return None
    popularity = to_float(_field(row, "popularity_ranking", "popularity") or 0)
    return {
        "place_code": place_code,
        "business_name": name,
        "address": _field(row, "address", "full_address"),
        "sub_area": _field(row, "sub_area", "subarea"),
        "area": _field(row, "area"),
        "city": _field(row, "city"),
        "sub_type": _field(row, "sub_type", "subtype"),
        "latitude": lat,
        "longitude": lng,
        "popularity": popularity,
        "aliases": [],
        "duplicate_of": None,
    }


def _scope_verdict(cafe: dict[str, Any], cfg: Settings) -> str | None:
    """Reason the row is out of scope, or None when it belongs in the run."""
    location = cfg.location
    sub_type = (cafe.get("sub_type") or "").lower()
    haystack = f"{cafe.get('sub_area') or ''} {cafe.get('area') or ''}".lower()

    for pattern in location.exclude_sub_type_patterns:
        if pattern.lower() in sub_type:
            return f"sub_type matched exclusion pattern '{pattern}'"
    if location.require_cafe_subtype and sub_type:
        if not any(hint in sub_type for hint in CAFE_SUBTYPE_HINTS):
            return f"sub_type '{cafe.get('sub_type')}' is not a cafe type"
    if not any(area.lower() in haystack for area in location.in_scope_areas):
        return (f"area '{cafe.get('area')}/{cafe.get('sub_area')}' is outside "
                f"Gulshan scope")
    if not any(sub.lower() in haystack for sub in location.in_scope_sub_areas):
        return (f"sub_area '{cafe.get('sub_area')}' is outside Gulshan scope "
                f"(allowed: {', '.join(location.in_scope_sub_areas)})")
    return None


def cluster_duplicates(candidates: list[dict[str, Any]],
                       cfg: Settings) -> list[dict[str, Any]]:
    """Merge rows that are the same venue listed twice (same name within
    ``duplicate_radius_m``): one fetch, results copied to the alias."""
    radius = float(cfg.matching.duplicate_radius_m)
    threshold = float(cfg.matching.duplicate_name_score)
    groups: list[list[dict[str, Any]]] = []
    for cafe in candidates:
        for group in groups:
            head = group[0]
            if (name_match_score(cafe["business_name"], head["business_name"])
                    >= threshold
                    and haversine_m(cafe["latitude"], cafe["longitude"],
                                    head["latitude"], head["longitude"])
                    <= radius):
                group.append(cafe)
                break
        else:
            groups.append([cafe])

    leaders: list[dict[str, Any]] = []
    for group in groups:
        leader = group[0]
        for alias in group[1:]:
            alias["duplicate_of"] = leader["place_code"]
            leader["aliases"].append(alias)
        leaders.append(leader)

    n_aliases = sum(len(c["aliases"]) for c in leaders)
    if n_aliases:
        log.info("duplicate clustering: %d rows -> %d unique venues "
                 "(%d aliases share a fetch)", len(candidates), len(leaders),
                 n_aliases)
    return leaders


def load_seed(csv_path, cfg: Settings) -> tuple[list[dict[str, Any]],
                                                list[dict[str, str]]]:
    """Returns (leaders, excluded) where excluded rows carry a reason."""
    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        parsed = [p for p in (parse_seed_row(row)
                              for row in csv.DictReader(handle)) if p]

    candidates: list[dict[str, Any]] = []
    excluded: list[dict[str, str]] = []
    for cafe in parsed:
        reason = _scope_verdict(cafe, cfg)
        if reason:
            excluded.append({"name": cafe["business_name"],
                             "place_code": cafe["place_code"],
                             "reason": reason})
        else:
            candidates.append(cafe)

    # Popularity ascending: lower Barikoi rank = better known venue, and the
    # most important cafes should be collected first.
    candidates.sort(key=lambda c: (c["popularity"] is None,
                                   c["popularity"] if c["popularity"]
                                   is not None else 0.0))
    log.info("seed: %d in scope, %d excluded (%s)", len(candidates),
             len(excluded), csv_path.name)
    return cluster_duplicates(candidates, cfg), excluded
