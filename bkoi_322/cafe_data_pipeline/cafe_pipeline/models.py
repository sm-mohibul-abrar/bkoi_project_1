"""The output record: one cafe, one JSON object, exactly the agreed schema.

The shape follows the project spec: platform-tagged priority fields (menu,
rating, review count, reviews, hours) that are never merged or averaged
across platforms, plus secondary contact fields and a per-cafe ``missing``
list. M-1 (Google Maps) fills the record; M-2..M-5 will append their own
tagged entries later without touching the M-1 values.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import SOURCE_GOOGLE_MAPS, TAG_M1
from .hours import WEEK
from .utils import is_empty, now_iso

PLATFORM_TAG = {SOURCE_GOOGLE_MAPS: TAG_M1}

AREA_GULSHAN_1 = "Gulshan-1"
AREA_GULSHAN_2 = "Gulshan-2"
AREA_OTHER_GULSHAN = "Other Gulshan"


def area_from_sub_area(sub_area: str | None, area: str | None) -> str | None:
    """Map seed area fields onto the spec's three area values."""
    text = f"{sub_area or ''} {area or ''}".lower()
    if "gulshan 1" in text or "gulshan-1" in text or "circle 1" in text:
        return AREA_GULSHAN_1
    if "gulshan 2" in text or "gulshan-2" in text or "circle 2" in text:
        return AREA_GULSHAN_2
    if "gulshan" in text:
        return AREA_OTHER_GULSHAN
    return None


@dataclass
class CafeRecord:
    """Everything the pipeline knows about one cafe, serialisable as-is."""

    cafe_id: str
    place_code: str
    name: str
    area: str | None
    address: str | None = None
    match_confidence: str | None = None     # high | medium | low
    duplicate_of: str | None = None         # cafe_id of the fetched twin
    business_status: str = "UNKNOWN"        # OPERATIONAL | TEMPORARILY_CLOSED | ...
    platforms_found: list[str] = field(default_factory=list)
    menu: list[dict[str, Any]] = field(default_factory=list)
    rating: list[dict[str, Any]] = field(default_factory=list)
    review_count: list[dict[str, Any]] = field(default_factory=list)
    reviews: list[dict[str, Any]] = field(default_factory=list)
    hours: list[dict[str, Any]] = field(default_factory=list)
    conflict: bool = False
    missing: list[str] = field(default_factory=list)
    # Secondary fields (kept only when a source clearly stated them).
    phone: str | None = None
    website: str | None = None
    social_urls: list[dict[str, str]] = field(default_factory=list)
    cuisine: str | None = None
    price_level: str | None = None
    menu_url: str | None = None
    google_maps_url: str | None = None
    plus_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    seed_address: str | None = None
    scrape_status: str = "pending"          # pending | ok | not_found | ...
    attempts: int = 0
    last_attempt: str | None = None
    last_updated: str | None = None

    # ── construction from the seed row ────────────────────────────────────
    @classmethod
    def from_seed(cls, cafe_id: str, seed: dict[str, Any]) -> "CafeRecord":
        return cls(
            cafe_id=cafe_id,
            place_code=seed["place_code"],
            name=seed["business_name"],
            area=area_from_sub_area(seed.get("sub_area"), seed.get("area")),
            seed_address=seed.get("address"),
        )

    # ── M-1 payload merge ─────────────────────────────────────────────────
    def apply_google_payload(self, payload: dict[str, Any],
                             confidence: str, status: str) -> None:
        """Fold a canonical Google Maps payload into this record.

        A refresh only replaces the fields it actually captured; a field the
        new scrape failed to read keeps its earlier value instead of being
        blanked. Nothing is ever inferred or cross-filled, and values are
        never merged or averaged.
        """
        self.match_confidence = confidence
        self.scrape_status = status
        self.last_attempt = now_iso()
        # "legacy" payloads come from earlier raw runs and fill the same
        # fields a live scrape would; anything else carries no payload.
        if status not in ("ok", "legacy"):
            return
        self.last_updated = now_iso()
        self.business_status = (payload.get("business_status")
                                or self.business_status or "UNKNOWN")
        self.platforms_found = sorted(set(self.platforms_found) | {SOURCE_GOOGLE_MAPS})

        self.address = payload.get("address") or self.address
        self.phone = payload.get("phone") or self.phone
        self.website = payload.get("website") or self.website
        self.cuisine = payload.get("category") or self.cuisine
        self.price_level = payload.get("price_level") or self.price_level
        self.menu_url = payload.get("menu_url") or self.menu_url
        self.google_maps_url = payload.get("google_maps_url") or self.google_maps_url
        self.plus_code = payload.get("plus_code") or self.plus_code
        self.latitude = payload.get("latitude") or self.latitude
        self.longitude = payload.get("longitude") or self.longitude

        as_of = payload.get("fetched_at")
        if not is_empty(payload.get("rating")):
            self.rating = [{
                "platform": SOURCE_GOOGLE_MAPS, "source_tag": TAG_M1,
                "value": payload.get("rating"), "scale": 5, "as_of": as_of,
            }]

        count = payload.get("review_count")
        if count:
            self.review_count = [{
                "platform": SOURCE_GOOGLE_MAPS, "source_tag": TAG_M1,
                "count": count,
                "approximate": payload.get("review_count_approximate", False),
            }]

        if payload.get("reviews"):
            self.reviews = [{
                "platform": SOURCE_GOOGLE_MAPS, "source_tag": TAG_M1,
                "stars": r.get("stars"), "text": r.get("text"),
                "date": r.get("date"), "as_of": as_of,
            } for r in payload.get("reviews", [])]

        schedule = payload.get("hours")
        if schedule:
            self.hours = [{
                "platform": SOURCE_GOOGLE_MAPS, "source_tag": TAG_M1,
                "type": "dine_in", "schedule": schedule,
                "notes": payload.get("hours_notes") or "",
            }]

        socials = list(self.social_urls)
        by_platform = {s["platform"]: s for s in socials}
        for key, platform in (("facebook_url", "FACEBOOK"),
                              ("instagram_url", "INSTAGRAM")):
            if payload.get(key):
                by_platform[platform] = {"platform": platform,
                                         "url": payload[key]}
        self.social_urls = [by_platform[p] for p in
                            ("FACEBOOK", "INSTAGRAM") if p in by_platform]

    # ── derived ───────────────────────────────────────────────────────────
    @property
    def permanently_closed(self) -> bool:
        return self.business_status == "PERMANENTLY_CLOSED"

    @property
    def excluded_reason(self) -> str | None:
        """Why this record is kept out of the main list, or None."""
        if self.scrape_status == "out_of_scope":
            return "outside the configured Gulshan scope (see summary seed filter)"
        if self.permanently_closed:
            return "permanently closed on Google Maps"
        if self.duplicate_of:
            return f"duplicate listing of {self.duplicate_of}"
        return None

    def refresh_missing(self) -> None:
        """Recompute the per-cafe gap list over the priority fields."""
        gaps = []
        if not self.menu:
            gaps.append("menu")
        if not any(e.get("price_bdt") for e in self.menu):
            gaps.append("price")
        if not self.rating:
            gaps.append("rating")
        if not self.reviews:
            gaps.append("reviews")
        if not self.review_count:
            gaps.append("review_count")
        if not self.hours:
            gaps.append("hours")
        if self.scrape_status == "pending":
            gaps.append("not_scraped")
        self.missing = gaps

    def to_dict(self) -> dict[str, Any]:
        self.refresh_missing()
        return {
            "cafe_id": self.cafe_id,
            "place_code": self.place_code,
            "name": self.name,
            "area": self.area,
            "address": self.address,
            "match_confidence": self.match_confidence,
            "duplicate_of": self.duplicate_of,
            "business_status": self.business_status,
            "platforms_found": self.platforms_found,
            "menu": self.menu,
            "rating": self.rating,
            "review_count": self.review_count,
            "reviews": self.reviews,
            "hours": self.hours,
            "conflict": self.conflict,
            "missing": self.missing,
            "phone": self.phone,
            "website": self.website,
            "social_urls": self.social_urls,
            "cuisine": self.cuisine,
            "price_level": self.price_level,
            "menu_url": self.menu_url,
            "google_maps_url": self.google_maps_url,
            "plus_code": self.plus_code,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "seed_address": self.seed_address,
            "scrape_status": self.scrape_status,
            "attempts": self.attempts,
            "last_attempt": self.last_attempt,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CafeRecord":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})


def build_summary(records: list[CafeRecord]) -> dict[str, Any]:
    """The completeness summary appended after all cafe records."""
    live = [r for r in records if not r.excluded_reason]
    excluded = [{"cafe_id": r.cafe_id, "name": r.name, "reason": r.excluded_reason}
                for r in records if r.excluded_reason]
    names = lambda rs: [r.name for r in rs]           # noqa: E731

    missing_hours = [r.name for r in live if "hours" in r.missing]
    return {
        "total_cafes": len(live),
        "generated_at": now_iso(),
        "excluded": excluded,
        "missing_menu": [r.name for r in live if "menu" in r.missing],
        "missing_price": [r.name for r in live if "price" in r.missing],
        "missing_rating": [r.name for r in live if "rating" in r.missing],
        "missing_reviews": [r.name for r in live if "reviews" in r.missing],
        "missing_review_count": [r.name for r in live
                                 if "review_count" in r.missing],
        "missing_hours": missing_hours,
        "not_scraped_yet": names([r for r in live
                                  if r.scrape_status == "pending"]),
        "counts": {
            "records_total": len(records),
            "live": len(live),
            "permanently_closed": sum(1 for r in records if r.permanently_closed),
            "duplicates": sum(1 for r in records if r.duplicate_of),
            "with_rating": sum(1 for r in live if r.rating),
            "with_hours": sum(1 for r in live if r.hours),
            "with_reviews": sum(1 for r in live if r.reviews),
        },
        "week_order": WEEK,
    }
