"""Record model: spec schema shape, merge semantics, summary partitioning."""

from __future__ import annotations

from cafe_pipeline.models import (AREA_GULSHAN_2, CafeRecord, area_from_sub_area,
                                  build_summary)

SEED = {"place_code": "TEST01", "business_name": "Test Cafe",
        "sub_area": "Gulshan 2", "area": "Gulshan",
        "address": "House 1, Road 2, Gulshan 2"}

PAYLOAD = {
    "business_status": "OPERATIONAL",
    "rating": 4.4,
    "review_count": 187,
    "review_count_approximate": False,
    "reviews": [{"stars": 5, "text": "great coffee", "date": "a month ago"},
                {"stars": None, "text": "second review text", "date": None}],
    "hours": {"Sat": "09:00-22:00", "Mon": "closed"},
    "address": "58 Gulshan Ave, Dhaka 1212",
    "phone": "+8801939899573",
    "website": "https://example.com",
    "facebook_url": "https://facebook.com/testcafe",
    "category": "Coffee shop",
    "latitude": 23.78, "longitude": 90.41,
    "fetched_at": "2026-09-21T00:00:00Z",
}


def make_record() -> CafeRecord:
    record = CafeRecord.from_seed("GUL-001", SEED)
    record.apply_google_payload(PAYLOAD, "high", "ok")
    return record


class TestApplyPayload:
    def test_spec_keys_present(self):
        data = make_record().to_dict()
        for key in ("cafe_id", "name", "area", "address", "match_confidence",
                    "platforms_found", "menu", "rating", "review_count",
                    "reviews", "hours", "conflict", "missing"):
            assert key in data, key

    def test_platform_tags_on_every_priority_value(self):
        record = make_record()
        assert record.platforms_found == ["GOOGLE_MAPS"]
        for entry in record.rating + record.review_count + record.reviews \
                + record.hours:
            assert entry["platform"] == "GOOGLE_MAPS"
            assert entry["source_tag"] == "M-1"

    def test_no_reviewer_identity_leaks(self):
        record = make_record()
        for review in record.reviews:
            assert set(review) <= {"platform", "source_tag", "stars", "text",
                                   "date", "as_of"}

    def test_failed_status_keeps_seed_only(self):
        record = CafeRecord.from_seed("GUL-002", SEED)
        record.apply_google_payload({}, "low", "not_found")
        assert record.rating == [] and record.address is None
        assert record.scrape_status == "not_found"

    def test_missing_list_tracks_gaps(self):
        record = make_record()
        record.refresh_missing()
        assert "menu" in record.missing and "price" in record.missing
        assert "rating" not in record.missing
        assert "hours" not in record.missing


class TestAreas:
    def test_area_mapping(self):
        assert area_from_sub_area("Gulshan 1", "Gulshan") == "Gulshan-1"
        assert area_from_sub_area("Gulshan 2", "Gulshan") == AREA_GULSHAN_2
        assert area_from_sub_area("Gulshan", "Gulshan") == "Other Gulshan"
        assert area_from_sub_area("Banani", "Dhaka") is None


class TestSummary:
    def test_excluded_partitions(self):
        live = make_record()

        closed = CafeRecord.from_seed("GUL-002", SEED)
        closed.business_status = "PERMANENTLY_CLOSED"

        twin = make_record()
        twin.cafe_id, twin.duplicate_of = "GUL-003", "GUL-001"

        summary = build_summary([live, closed, twin])
        assert summary["total_cafes"] == 1
        reasons = [e["reason"] for e in summary["excluded"]]
        assert any("permanently closed" in r for r in reasons)
        assert any("duplicate" in r for r in reasons)
        assert summary["counts"]["records_total"] == 3
