"""The output store: one JSON + one CSV per location, merged forever.

Design points that matter for the workflow:

* **Interrupt safety.** Every upsert is followed by ``flush()`` from the
  pipeline, and ``flush()`` writes atomically (tmp file + rename, previous
  content kept as ``.bak``). Ctrl-C at any moment loses at most the cafe
  currently being scraped -- never the file.
* **Merge, never overwrite.** A re-run reloads this file, fills the gaps it
  can, and rewrites it. Values already present are only replaced by newer
  same-platform values, so repeated runs accumulate instead of resetting.
* **Stable cafe ids.** ``GUL-001``-style ids are assigned once, persisted in
  the state file, and never renumbered.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, Iterable

from . import SOURCE_GOOGLE_MAPS
from .hours import WEEK
from .models import CafeRecord, build_summary
from .settings import Settings
from .utils import atomic_write_json, now_iso

log = logging.getLogger("cafe_pipeline.store")

SCHEMA_VERSION = 1

CSV_COLUMNS = [
    "cafe_id", "place_code", "name", "area", "address", "match_confidence",
    "duplicate_of", "business_status", "scrape_status", "attempts",
    "platforms_found", "google_maps_url", "phone", "website", "facebook_url",
    "instagram_url", "menu_url", "cuisine", "price_level", "plus_code",
    "latitude", "longitude", "google_rating", "google_review_count",
    "review_count_approximate",
    *[f"hours_{day}" for day in WEEK],
    "hours_notes", "reviews_json", "review_snippet_count", "missing",
    "seed_address", "last_attempt", "last_updated",
]


class CafeStore:
    """Records keyed by place_code; ids stable across runs."""

    def __init__(self, cfg: Settings):
        key = cfg.location.key
        self.cfg = cfg
        self.json_path = cfg.output_dir / f"{key}_cafes.json"
        self.csv_path = cfg.output_dir / f"{key}_cafes.csv"
        self.state_path = cfg.output_dir / f"{key}_state.json"
        self.records: dict[str, CafeRecord] = {}
        self._id_map: dict[str, str] = {}
        self._next_id = 1
        self._load()

    # ── persistence ────────────────────────────────────────────────────────
    def _load(self) -> None:
        if self.json_path.exists():
            try:
                raw = json.loads(self.json_path.read_text(encoding="utf-8"))
                for item in raw.get("records", []):
                    record = CafeRecord.from_dict(item)
                    self.records[record.place_code] = record
            except (json.JSONDecodeError, TypeError, KeyError) as exc:
                log.warning("%s unreadable (%s) -- trying backup",
                            self.json_path.name, exc)
                self._load_backup()
        if self.state_path.exists():
            try:
                state = json.loads(self.state_path.read_text(encoding="utf-8"))
                self._id_map = state.get("cafe_ids", {})
                self._next_id = state.get("next_id", 1)
            except json.JSONDecodeError:
                log.warning("state unreadable -- ids will be re-derived")
        # Records themselves are the source of truth for ids.
        for record in self.records.values():
            self._id_map.setdefault(record.place_code, record.cafe_id)
            self._bump_next_id(record.cafe_id)
        if not self._id_map and self.records:
            log.warning("no id map but %d records -- rebuilding ids",
                        len(self.records))

    def _load_backup(self) -> None:
        backup = self.json_path.with_suffix(".json.bak")
        if not backup.exists():
            return
        try:
            raw = json.loads(backup.read_text(encoding="utf-8"))
            for item in raw.get("records", []):
                record = CafeRecord.from_dict(item)
                self.records[record.place_code] = record
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            log.error("backup also unreadable (%s) -- starting empty", exc)

    def _bump_next_id(self, cafe_id: str) -> None:
        try:
            number = int(cafe_id.rsplit("-", 1)[1])
            self._next_id = max(self._next_id, number + 1)
        except (IndexError, ValueError):
            pass

    # ── id assignment ──────────────────────────────────────────────────────
    def ensure_ids(self, seeds: Iterable[dict[str, Any]]) -> None:
        """Assign a stable GUL-### id to every seed cafe (and its duplicate
        aliases) that lacks one."""
        for seed in seeds:
            for row in [seed, *seed.get("aliases", [])]:
                place_code = row["place_code"]
                if place_code in self._id_map:
                    continue
                cafe_id = f"GUL-{self._next_id:03d}"
                self._next_id += 1
                self._id_map[place_code] = cafe_id
                log.debug("assigned %s -> %s", place_code, cafe_id)

    def cafe_id(self, place_code: str) -> str | None:
        return self._id_map.get(place_code)

    # ── records ────────────────────────────────────────────────────────────
    def get_or_create(self, seed: dict[str, Any]) -> CafeRecord:
        record = self.records.get(seed["place_code"])
        if record is None:
            cafe_id = self._id_map[seed["place_code"]]
            record = CafeRecord.from_seed(cafe_id, seed)
            self.records[seed["place_code"]] = record
        return record

    def materialize(self, seeds: Iterable[dict[str, Any]]) -> None:
        """Create a pending record for every seed row (leaders and aliases)
        so the outputs list all in-scope cafes, with their gaps, from the
        very first run."""
        for seed in seeds:
            self.get_or_create(seed)
            for alias in seed.get("aliases", []):
                self.get_or_create(alias)

    def upsert(self, record: CafeRecord) -> None:
        self.records[record.place_code] = record

    def all_records(self) -> list[CafeRecord]:
        return sorted(self.records.values(), key=lambda r: r.cafe_id)

    # ── flush ──────────────────────────────────────────────────────────────
    def summary(self) -> dict[str, Any]:
        return build_summary(self.all_records())

    def flush(self) -> None:
        records = self.all_records()
        atomic_write_json(self.json_path, {
            "location": self.cfg.location.name,
            "schema_version": SCHEMA_VERSION,
            "week_order": WEEK,
            "generated_at": now_iso(),
            "records": [r.to_dict() for r in records],
            "summary": build_summary(records),
        })
        self._write_csv(records)
        atomic_write_json(self.state_path, {
            "cafe_ids": self._id_map,
            "next_id": self._next_id,
            "updated_at": now_iso(),
        })

    def _write_csv(self, records: list[CafeRecord]) -> None:
        rows = []
        for record in records:
            socials = {s["platform"].lower(): s["url"]
                       for s in record.social_urls}
            google_hours = next((h for h in record.hours
                                 if h["platform"] == SOURCE_GOOGLE_MAPS), {})
            schedule = google_hours.get("schedule") or {}
            google_count = next((c for c in record.review_count
                                 if c["platform"] == SOURCE_GOOGLE_MAPS), {})
            rows.append({
                "cafe_id": record.cafe_id,
                "place_code": record.place_code,
                "name": record.name,
                "area": record.area,
                "address": record.address,
                "match_confidence": record.match_confidence,
                "duplicate_of": record.duplicate_of,
                "business_status": record.business_status,
                "scrape_status": record.scrape_status,
                "attempts": record.attempts,
                "platforms_found": "+".join(record.platforms_found),
                "google_maps_url": record.google_maps_url,
                "phone": record.phone,
                "website": record.website,
                "facebook_url": socials.get("facebook"),
                "instagram_url": socials.get("instagram"),
                "menu_url": record.menu_url,
                "cuisine": record.cuisine,
                "price_level": record.price_level,
                "plus_code": record.plus_code,
                "latitude": record.latitude,
                "longitude": record.longitude,
                "google_rating": (record.rating[0]["value"]
                                  if record.rating else None),
                "google_review_count": (google_count.get("count")
                                        if google_count else None),
                "review_count_approximate": (google_count.get("approximate")
                                             if google_count else None),
                **{f"hours_{day}": schedule.get(day) for day in WEEK},
                "hours_notes": google_hours.get("notes") or "",
                "reviews_json": json.dumps(record.reviews, ensure_ascii=False),
                "review_snippet_count": len(record.reviews),
                "missing": ",".join(record.missing),
                "seed_address": record.seed_address,
                "last_attempt": record.last_attempt,
                "last_updated": record.last_updated,
            })
        tmp = self.csv_path.with_suffix(".csv.tmp")
        with tmp.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS,
                                    extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        tmp.replace(self.csv_path)
