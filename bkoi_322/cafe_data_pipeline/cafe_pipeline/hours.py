"""Opening-hours normalisation.

Everything converges on one representation: a dict keyed by the Bangladeshi
week Saturday -> Friday, with values in one of exactly three forms:

    "09:00-22:00"                    single span, 24-hour HH:MM
    "17:00-19:00, 20:00-22:30"       split shift, spans comma-joined
    "closed"                         the day is a closed day

Conventions (also in README):
  * "Open 24 hours" becomes "00:00-24:00".
  * When the end time is earlier than the start ("22:00-01:00") the venue
    closes after midnight; the end clock is still plain HH:MM.
  * Dine-in and delivery hours are never mixed into one schedule; each
    platform entry carries its own ``type``.
"""

from __future__ import annotations

import re

# The Bangladeshi week: Saturday first, Friday last (the weekend).
WEEK = ["Sat", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri"]

DAY_ALIAS = {
    "sat": "Sat", "saturday": "Sat",
    "sun": "Sun", "sunday": "Sun",
    "mon": "Mon", "monday": "Mon",
    "tue": "Tue", "tues": "Tue", "tuesday": "Tue",
    "wed": "Wed", "wednesday": "Wed",
    "thu": "Thu", "thur": "Thu", "thurs": "Thu", "thursday": "Thu",
    "fri": "Fri", "friday": "Fri",
}

_TIME_TOKEN = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([AaPp][Mm])?")
_SPAN_SPLIT = re.compile(r"\s*[–—-]\s*")
_SPAN_DELIMS = re.compile(r"\s*[,;]\s*|\s+and\s+", re.I)

CLOSED_VALUES = {"closed", "close", "off"}
# Google renders inline icons as private-use codepoints; strip them from
# scraped text before parsing.
PRIVATE_USE_RE = re.compile("[-​-‏﻿]")
OPEN_24_VALUES = {"open 24 hours", "open 24hrs", "24 hours", "24/7", "always open"}


def canonical_day(raw: str) -> str | None:
    """'saturday' / 'Sat' / 'SAT ' -> 'Sat' (our three-letter form)."""
    key = re.sub(r"[^a-z]", "", str(raw).lower())
    return DAY_ALIAS.get(key)


def _clock_to_hhmm(hour: int, minute: int, meridiem: str | None) -> str:
    if not meridiem:
        # No AM/PM means the token is already 24-hour ("22:00" must not
        # pass through the 12-hour modulo).
        return f"{hour:02d}:{minute:02d}"
    h = hour % 12
    if meridiem.lower() == "pm":
        h += 12
    return f"{h:02d}:{minute:02d}"


def _parse_side(token: str) -> tuple[int, int, str | None] | None:
    match = _TIME_TOKEN.fullmatch(token.strip())
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2) or 0)
    if hour > 24 or minute > 59:
        return None
    return hour, minute, match.group(3)


def parse_span(text: str) -> str | None:
    """One span like '9 AM–10 PM' / '5–7 PM' -> '09:00-22:00' / '17:00-19:00'.

    A meridiem written on only one side applies to both sides (that is how
    Google renders '5–7 PM'). Returns None when the span cannot be parsed.
    """
    parts = _SPAN_SPLIT.split(str(text).strip())
    if len(parts) != 2:
        return None
    left, right = _parse_side(parts[0]), _parse_side(parts[1])
    if not left or not right:
        return None
    meridiem = right[2] or left[2]
    start = _clock_to_hhmm(left[0], left[1], left[2] or meridiem)
    end = _clock_to_hhmm(right[0], right[1], right[2] or meridiem)
    if start == "00:00" and end == "00:00":
        return None                       # unparsable zeroes, not midnight
    return f"{start}-{end}"


def hours_text_to_hhmm(raw: str) -> str | None:
    """A full hours cell/value -> our canonical value, or None.

    Understands 'Open 24 hours', 'Closed', split shifts
    ('5–7 PM, 8–10 PM') and single spans, in any dash variety.
    """
    if raw is None:
        return None
    text = PRIVATE_USE_RE.sub("", str(raw))
    text = re.sub(r"\s+", " ", text).strip().strip(",;")
    if not text:
        return None
    low = text.lower().rstrip(".")
    if low in OPEN_24_VALUES:
        return "00:00-24:00"
    if low in CLOSED_VALUES:
        return "closed"
    if "n/a" in low or "check" in low or "not available" in low:
        return None
    spans = [parse_span(chunk) for chunk in _SPAN_DELIMS.split(text)]
    good = [s for s in spans if s]
    if not good or len(good) != len(spans):
        return None                       # partially parsable = not trustworthy
    return ", ".join(good)


def ordered_week(schedule: dict[str, str]) -> dict[str, str]:
    """Re-key a day->value dict to the Sat..Fri order, dropping unknown days."""
    return {day: schedule[day] for day in WEEK if day in schedule}


# "Monday 9 AM–10 PM" / "Wednesday: Closed" / "Friday Open 24 hours"
RE_HOURS_LINE = re.compile(
    r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b"
    r"[^\dA-Za-z]{0,6}(Closed|Open\s*24\s*hours|"
    r"\d{1,2}(?::\d{2})?\s*(?:AM|PM)?\s*[–—-]\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:AM|PM))", re.IGNORECASE)


def hours_lines_from_text(text: str) -> dict[str, str]:
    """Pull a week out of flat text like a button aria-label:
    'Hours: Monday 9 AM–10 PM; Tuesday 9 AM–10 PM ...'.
    Returns only days whose value converts cleanly."""
    raw: dict[str, str] = {}
    for match in RE_HOURS_LINE.finditer(text or ""):
        day = canonical_day(match.group(1))
        if day and day not in raw:
            raw[day] = match.group(2)
    return {day: converted for day, value in raw.items()
            if (converted := hours_text_to_hhmm(value)) is not None}


def normalize_hours_dict(raw: dict, ) -> tuple[dict[str, str], list[str]]:
    """Normalise a raw day->hours mapping (any key casing, values in any of
    the shapes above). Returns (schedule, notes) where notes records values
    that could not be converted -- those are kept verbatim so no data is
    silently dropped."""
    schedule: dict[str, str] = {}
    notes: list[str] = []
    for key, value in (raw or {}).items():
        day = canonical_day(str(key))
        if not day or value is None:
            continue
        text = str(value).strip()
        if not text or text.upper() == "N/A":
            continue
        converted = hours_text_to_hhmm(text)
        if converted is None:
            schedule[day] = text
            notes.append(f"{day}: kept raw value {text!r}")
        else:
            schedule[day] = converted
    return ordered_week(schedule), notes
