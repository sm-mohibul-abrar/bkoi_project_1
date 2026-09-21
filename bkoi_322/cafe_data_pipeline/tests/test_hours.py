"""Hours normalisation: the conventions the whole pipeline depends on."""

from __future__ import annotations

from cafe_pipeline.hours import (WEEK, canonical_day, hours_lines_from_text,
                                 hours_text_to_hhmm, normalize_hours_dict)


class TestHoursTextToHhmm:
    def test_simple_am_pm_span(self):
        assert hours_text_to_hhmm("9 AM–10 PM") == "09:00-22:00"

    def test_minutes_and_en_dash(self):
        assert hours_text_to_hhmm("9:30 AM–10:30 PM") == "09:30-22:30"

    def test_noon_boundary(self):
        assert hours_text_to_hhmm("12–1:30 PM") == "12:00-13:30"

    def test_meridiem_applies_to_both_sides(self):
        assert hours_text_to_hhmm("5–7 PM") == "17:00-19:00"

    def test_past_midnight_end(self):
        # Closes after midnight: end earlier than start is the convention.
        assert hours_text_to_hhmm("8 AM–12:45 AM") == "08:00-00:45"

    def test_open_24_hours(self):
        assert hours_text_to_hhmm("Open 24 hours") == "00:00-24:00"
        assert hours_text_to_hhmm("24 hours") == "00:00-24:00"

    def test_closed(self):
        assert hours_text_to_hhmm("Closed") == "closed"

    def test_split_shift(self):
        assert (hours_text_to_hhmm("5–7 PM, 8–10 PM")
                == "17:00-19:00, 20:00-22:00")

    def test_unusable_values_return_none(self):
        for bad in ("", "N/A", "N/A (Check Maps)", "Check Maps", "Bakery",
                    "9 AM-10 PM-3 PM"):
            assert hours_text_to_hhmm(bad) is None, bad

    def test_already_24h_passes_through(self):
        assert hours_text_to_hhmm("10:00-22:00") == "10:00-22:00"


class TestWeek:
    def test_week_order_is_sat_to_fri(self):
        assert WEEK == ["Sat", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri"]

    def test_canonical_day(self):
        assert canonical_day("saturday") == "Sat"
        assert canonical_day("SAT") == "Sat"
        assert canonical_day("Monday") == "Mon"
        assert canonical_day("Fri ") == "Fri"
        assert canonical_day("Funday") is None

    def test_normalize_orders_and_converts(self):
        schedule, notes = normalize_hours_dict({
            "monday": "9 AM–10 PM", "sat": "Closed", "fri": "Open 24 hours",
        })
        assert list(schedule) == ["Sat", "Mon", "Fri"]
        assert schedule["Mon"] == "09:00-22:00"
        assert schedule["Sat"] == "closed"
        assert schedule["Fri"] == "00:00-24:00"
        assert notes == []

    def test_normalize_keeps_unparsable_raw_with_note(self):
        schedule, notes = normalize_hours_dict({"mon": "ask at counter"})
        assert schedule == {"Mon": "ask at counter"}
        assert len(notes) == 1

    def test_normalize_drops_na(self):
        schedule, _ = normalize_hours_dict({"sat": "N/A", "sun": ""})
        assert schedule == {}


class TestHoursLinesFromText:
    def test_aria_label_week(self):
        label = ("Show open hours for the week. Monday: 9 AM–10 PM; "
                 "Tuesday: 9 AM–10 PM; Wednesday: 9 AM–10 PM; "
                 "Thursday: 9 AM–10 PM; Friday: 1–11 PM; "
                 "Saturday: 9 AM–11 PM; Sunday: Closed")
        week = hours_lines_from_text(label)
        assert week["Mon"] == "09:00-22:00"
        assert week["Fri"] == "13:00-23:00"
        assert week["Sat"] == "09:00-23:00"
        assert week["Sun"] == "closed"
        assert len(week) == 7

    def test_garbage_text_yields_nothing(self):
        assert hours_lines_from_text("Hours: Open now") == {}
        assert hours_lines_from_text("") == {}
