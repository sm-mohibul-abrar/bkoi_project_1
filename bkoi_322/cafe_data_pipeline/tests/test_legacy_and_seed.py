"""Legacy importer: every raw shape found in data/raw must convert cleanly,
with reviewer identity stripped. Plus seed scope filtering."""

from __future__ import annotations

import json

from cafe_pipeline.legacy_import import convert_profile, load_legacy_payloads
from cafe_pipeline.seed import load_seed
from cafe_pipeline.settings import DEFAULTS, Settings

# Shape 1/2: old runs, reviews live in a sibling map_reviews file.
PROFILE_OLD = {
    "place_code": "OLD01", "business_name": "Old Shape Cafe",
    "status": "OPERATIONAL",
    "phone_numbers": ["N/A"],
    "menu_link": "",
    "opening_closing_hours": ["N/A"],
    "reviews_scraped_count": 5,
    "contact_and_location": {
        "address_from_csv": "Old Shape Cafe, Gulshan 2, Dhaka",
        "coordinates": {"csv_lat": 23.79, "csv_lng": 90.41,
                        "gmaps_lat": 23.7994, "gmaps_lng": 90.4155},
    },
}
REVIEWS_OLD = [
    {"reviewer": "Named Person", "date_mmyy": "05/26",
     "full_review_text": "Lovely coffee, will return.",
     "review_images": ["https://lh3.googleusercontent.com/xyz"]},
    {"reviewer": "Another Person", "date": "04/26",
     "full_review_text": "Great pancakes.", "item_wise_prices": [],
     "review_images": []},
]

# Shape 3: richest -- everything inline.
PROFILE_RICH = {
    "place_code": "RICH1", "business_name": "Rich Shape Cafe",
    "phone": "+8801709642004",
    "website": "https://foodpanda.com.bd/restaurant/s2hl/x?utm=1",
    "facebook_url": None, "instagram_url": None,
    "google_rating": 4.2, "google_review_count": 2116,
    "opening_hours": {"sat": "Closed", "sun": "8 am–10 pm"},
    "category": "Cafe",
    "reviews": [{"source": "google", "author": "Someone",
                 "rating": 5, "date": "5 months ago",
                 "text": "Hard to find at first but worth it."}],
}

# Shape 4: partial live scrape.
PROFILE_PARTIAL = {
    "place_code": "PART9", "business_name": "Partial Cafe",
    "status": "OPERATIONAL", "google_rating": 4.3,
    "google_review_count": 48,
    "contact_and_location": {"phone": "01841552898",
                             "address_from_map": "56 Gulshan Ave, Dhaka 1212",
                             "coordinates": {"lat": 23.7814, "lng": 90.4170}},
    "menu": "N/A",
    "opening_hours": {"schedule": "N/A (Check Maps)"},
    "reviews": [{"author": "X", "rating": 4, "date": "2 years ago",
                 "text": "Nice environment and quality foods."}],
}


class TestConvertProfiles:
    def test_old_shape_with_sibling_reviews(self):
        payload = convert_profile(PROFILE_OLD, "2026-09-17T15:00:00",
                                  REVIEWS_OLD)
        assert payload["latitude"] == 23.7994
        assert payload["longitude"] == 90.4155
        assert payload["business_status"] == "OPERATIONAL"
        assert payload["fetched_at"] == "2026-09-17T15:00:00"
        # reviews imported, identity and images dropped
        assert len(payload["reviews"]) == 2
        for review in payload["reviews"]:
            assert set(review) == {"stars", "text", "date"}
        assert payload["reviews"][0]["date"] == "05/26"

    def test_rich_shape(self):
        payload = convert_profile(PROFILE_RICH, None, None)
        assert payload["rating"] == 4.2
        assert payload["review_count"] == 2116
        assert payload["phone"] == "+8801709642004"
        assert payload["website"] == "https://foodpanda.com.bd/restaurant/s2hl/x"
        assert payload["category"] == "Cafe"
        assert payload["reviews"][0]["stars"] == 5
        # a two-day fragment is below hours.min_days and is dropped
        assert "hours" not in payload

    def test_partial_shape(self):
        payload = convert_profile(PROFILE_PARTIAL, "2026-09-17T16:00:00", None)
        assert payload["rating"] == 4.3
        assert payload["phone"] == "+8801841552898"
        assert payload["address"] == "56 Gulshan Ave, Dhaka 1212"
        assert payload["reviews"][0]["stars"] == 4
        assert "hours" not in payload          # "N/A (Check Maps)" dropped
        assert "menu_url" not in payload       # "N/A" menu link dropped

    def test_richest_payload_wins_per_place_code(self, tmp_path):
        run = tmp_path / "run_20260917_150000" / "profiles"
        run.mkdir(parents=True)
        (run / "OLD01_first.json").write_text(
            json.dumps({**PROFILE_OLD, "google_rating": 3.0}),
            encoding="utf-8")
        (run / "OLD01_second.json").write_text(
            json.dumps({**PROFILE_OLD, "google_rating": 4.4,
                        "google_review_count": 90}),
            encoding="utf-8")
        payloads = load_legacy_payloads(tmp_path)
        assert payloads["OLD01"]["rating"] == 4.4
        assert payloads["OLD01"]["review_count"] == 90


class TestSeedScope:
    def test_real_seed_filters_to_gulshan(self):
        import pathlib
        root = pathlib.Path(__file__).resolve().parents[1]
        csv_path = root / "gulshan_all_cafe.csv"
        if not csv_path.exists():
            return                                # running outside the repo
        cfg = Settings(DEFAULTS, root)
        leaders, excluded = load_seed(csv_path, cfg)
        assert 0 < len(leaders) < 200
        reasons = " | ".join(e["reason"] for e in excluded)
        assert "outside Gulshan scope" in reasons
        for seed in leaders:                     # everything kept is Gulshan
            hay = f"{seed.get('sub_area') or ''} {seed.get('area') or ''}"
            assert "gulshan" in hay.lower()
