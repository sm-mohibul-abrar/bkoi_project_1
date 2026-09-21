"""Pure helpers: numbers, phones, URLs, name matching."""

from __future__ import annotations

from cafe_pipeline.utils import (clean_url, is_empty, name_match_score,
                                 norm_bd_phone, parse_count, parse_rating)


class TestParseCount:
    def test_plain(self):
        assert parse_count("2116") == (2116, False)

    def test_thousands_separator(self):
        assert parse_count("1,234") == (1234, False)

    def test_k_suffix_is_approximate(self):
        assert parse_count("1.2K") == (1200, True)
        assert parse_count("1.2k reviews") == (1200, True)

    def test_m_suffix(self):
        assert parse_count("3M") == (3000000, True)

    def test_garbage(self):
        assert parse_count("N/A") == (None, False)
        assert parse_count(None) == (None, False)


class TestParseRating:
    def test_plain_and_styled(self):
        assert parse_rating("4.6") == 4.6
        assert parse_rating("4,6 stars") == 4.6

    def test_out_of_range_rejected(self):
        assert parse_rating("5.7") is None
        assert parse_rating("0") is None
        assert parse_rating("no rating") is None


class TestPhones:
    def test_mobile_local(self):
        assert norm_bd_phone("01939899573") == "+8801939899573"

    def test_mobile_with_country_code(self):
        assert norm_bd_phone("+880 1841-552898") == "+8801841552898"

    def test_landline(self):
        assert norm_bd_phone("02 8834567") == "+88028834567"

    def test_garbage(self):
        assert norm_bd_phone("N/A") is None
        assert norm_bd_phone(None) is None


class TestUrls:
    def test_strips_query_fragment_and_slash(self):
        assert (clean_url("https://x.com/a/?utm=1#frag")
                == "https://x.com/a")

    def test_rejects_non_http(self):
        assert clean_url("not a url") is None
        assert clean_url("N/A") is None


class TestHelpers:
    def test_is_empty_treats_na_as_empty(self):
        assert is_empty("N/A") and is_empty(" ") and is_empty([]) \
            and is_empty(None)
        assert not is_empty("0")

    def test_name_match_score(self):
        assert name_match_score("Coffee Buzz", "coffee buzz") == 1.0
        assert name_match_score("2 Bros Cafe", "North End Coffee Roasters") < 0.34
        # partial overlap
        assert 0.5 < name_match_score("Coffee Buzz Gulshan",
                                      "Coffee Buzz") < 1.0
        # tokens shorter than 3 chars ("1"/"2") are ignored, so branch
        # numbers alone do not separate twins -- distance has to do that
        assert name_match_score("X Cafe Gulshan 1", "X Cafe Gulshan 2") == 1.0
