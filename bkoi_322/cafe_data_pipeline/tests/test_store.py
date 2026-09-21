"""Store: stable ids, interrupt-safe merge semantics, CSV round-trip."""

from __future__ import annotations

import json

from cafe_pipeline.settings import DEFAULTS, Settings
from cafe_pipeline.store import CafeStore

SEED_A = {"place_code": "AAA01", "business_name": "Cafe A",
          "sub_area": "Gulshan 1", "area": "Gulshan", "address": "addr A"}
SEED_B = {"place_code": "BBB02", "business_name": "Cafe B",
          "sub_area": "Gulshan 2", "area": "Gulshan", "address": "addr B"}


def make_store(tmp_path) -> CafeStore:
    cfg = Settings(DEFAULTS, tmp_path)
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    return CafeStore(cfg)


class TestIdsAndPersistence:
    def test_ids_stable_across_reload(self, tmp_path):
        store = make_store(tmp_path)
        store.ensure_ids([SEED_A, SEED_B])
        first = store.cafe_id("AAA01")
        store.get_or_create(SEED_A)
        store.flush()

        reloaded = make_store(tmp_path)
        reloaded.ensure_ids([SEED_A, SEED_B])
        assert reloaded.cafe_id("AAA01") == first == "GUL-001"
        assert reloaded.cafe_id("BBB02") == "GUL-002"

    def test_new_seed_gets_next_number(self, tmp_path):
        store = make_store(tmp_path)
        store.ensure_ids([SEED_A])
        store.flush()
        reloaded = make_store(tmp_path)
        reloaded.ensure_ids([SEED_A, SEED_B])
        assert reloaded.cafe_id("BBB02") == "GUL-002"


class TestMergeSemantics:
    PAYLOAD_V1 = {"business_status": "OPERATIONAL", "rating": 4.0,
                  "review_count": 10, "hours": {"Sat": "09:00-20:00"},
                  "address": "old address", "fetched_at": "2026-09-01"}
    PAYLOAD_V2 = {"business_status": "OPERATIONAL", "rating": 4.5,
                  "review_count": 12, "fetched_at": "2026-09-21"}

    def test_rerun_updates_values_and_keeps_gaps_filled(self, tmp_path):
        store = make_store(tmp_path)
        store.ensure_ids([SEED_A])
        record = store.get_or_create(SEED_A)
        record.apply_google_payload(self.PAYLOAD_V1, "high", "ok")
        store.upsert(record)
        store.flush()

        reloaded = make_store(tmp_path)
        reloaded.ensure_ids([SEED_A])
        record = reloaded.get_or_create(SEED_A)
        record.apply_google_payload(self.PAYLOAD_V2, "high", "ok")
        reloaded.upsert(record)
        reloaded.flush()

        final = json.loads(reloaded.json_path.read_text(encoding="utf-8"))
        data = final["records"][0]
        # refreshed values win, stale-but-still-true values survive
        assert data["rating"][0]["value"] == 4.5
        assert data["review_count"][0]["count"] == 12
        assert data["address"] == "old address"
        assert data["hours"][0]["schedule"] == {"Sat": "09:00-20:00"}

    def test_legacy_then_live_live_wins(self, tmp_path):
        store = make_store(tmp_path)
        store.ensure_ids([SEED_A])
        record = store.get_or_create(SEED_A)
        record.apply_google_payload(self.PAYLOAD_V1, "medium", "legacy")
        assert record.scrape_status == "legacy"
        record.apply_google_payload(self.PAYLOAD_V2, "high", "ok")
        # live refresh wins where it captured something
        assert record.rating[0]["value"] == 4.5
        assert record.review_count[0]["count"] == 12
        # and keeps earlier values where it captured nothing new
        assert record.address == "old address"
        assert record.hours[0]["schedule"] == {"Sat": "09:00-20:00"}


class TestOutputs:
    def test_flush_writes_json_csv_summary(self, tmp_path):
        store = make_store(tmp_path)
        store.ensure_ids([SEED_A])
        record = store.get_or_create(SEED_A)
        record.apply_google_payload(
            {"business_status": "OPERATIONAL", "rating": 4.2,
             "review_count": 9, "hours": {"Sat": "closed"},
             "fetched_at": "2026-09-21"}, "high", "ok")
        store.upsert(record)
        store.flush()

        final = json.loads(store.json_path.read_text(encoding="utf-8"))
        assert final["summary"]["total_cafes"] == 1
        assert final["records"][0]["rating"][0]["source_tag"] == "M-1"

        csv_text = store.csv_path.read_text(encoding="utf-8-sig")
        assert "cafe_id,place_code,name" in csv_text
        assert "hours_Sat" in csv_text and "hours_Fri" in csv_text
        assert "GUL-001" in csv_text

    def test_corrupt_json_falls_back_to_bak(self, tmp_path):
        store = make_store(tmp_path)
        store.ensure_ids([SEED_A])
        store.get_or_create(SEED_A)
        store.flush()
        store.flush()          # second flush leaves the first as .bak
        store.json_path.write_text("{broken", encoding="utf-8")

        reloaded = make_store(tmp_path)
        assert "AAA01" in reloaded.records
