import os
import re
import json
import time
import random
import traceback
from datetime import datetime
from urllib.parse import quote, urlparse, parse_qs

import pandas as pd
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


# ============================================================
# CONFIGURATION
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.dirname(SCRIPT_DIR)

# ------------------------------------------------------------
# INPUT
# ------------------------------------------------------------

INPUT_CSV = os.path.join(
    PIPELINE_DIR,
    "data",
    "processed",
    "places.csv"
)

# ------------------------------------------------------------
# OUTPUT
# ------------------------------------------------------------

PROCESSED_DIR = os.path.join(
    PIPELINE_DIR,
    "data",
    "processed"
)

RAW_ROOT = os.path.join(
    PIPELINE_DIR,
    "data",
    "raw",
    "google_maps"
)

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(RAW_ROOT, exist_ok=True)

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

RAW_RUN_DIR = os.path.join(
    RAW_ROOT,
    f"run_{RUN_ID}",
    "profiles"
)

os.makedirs(RAW_RUN_DIR, exist_ok=True)


# ------------------------------------------------------------
# SCRAPER SETTINGS
# ------------------------------------------------------------

HEADLESS = False

MAX_REVIEWS = 30

MAX_IMAGES = 20

PAGE_TIMEOUT = 30000

WAIT_AFTER_PAGE_LOAD = (2.5, 4.5)

WAIT_AFTER_CLICK = (1.0, 2.0)

# How similar the Google result name should be
# before we accept it.
MIN_NAME_MATCH_SCORE = 0.55


# ============================================================
# GENERAL HELPERS
# ============================================================

def random_sleep(range_tuple):
    time.sleep(random.uniform(*range_tuple))


def clean_text(value):
    if value is None:
        return None

    value = str(value)

    value = value.replace("\u200b", "")
    value = value.replace("\u200c", "")
    value = value.replace("\u200d", "")
    value = value.replace("\ufeff", "")
    value = value.replace("\u202f", " ")

    value = re.sub(r"\s+", " ", value)

    return value.strip()


def safe_text(locator):
    try:
        if locator.count() == 0:
            return None

        text = locator.first.inner_text(timeout=2000)

        return clean_text(text)

    except Exception:
        return None


def safe_attribute(locator, attribute):
    try:
        if locator.count() == 0:
            return None

        value = locator.first.get_attribute(
            attribute,
            timeout=2000
        )

        return clean_text(value)

    except Exception:
        return None


def click_if_visible(locator, timeout=2500):
    try:

        if locator.count() == 0:
            return False

        element = locator.first

        if element.is_visible(timeout=timeout):
            element.click(
                timeout=timeout,
                force=True
            )

            random_sleep(WAIT_AFTER_CLICK)

            return True

    except Exception:
        pass

    return False


def unique_list(items):
    result = []

    seen = set()

    for item in items:

        if not item:
            continue

        item = clean_text(item)

        if not item:
            continue

        if item not in seen:

            seen.add(item)
            result.append(item)

    return result


# ============================================================
# TEXT / NAME MATCHING
# ============================================================

def normalize_name(name):

    if not name:
        return ""

    name = name.lower()

    name = re.sub(
        r"[^a-z0-9]+",
        " ",
        name
    )

    stop_words = {
        "cafe",
        "coffee",
        "restaurant",
        "dhaka",
        "bangladesh"
    }

    tokens = [
        x
        for x in name.split()
        if x not in stop_words
    ]

    return " ".join(tokens)


def name_similarity(a, b):

    a = normalize_name(a)
    b = normalize_name(b)

    if not a or not b:
        return 0.0

    a_tokens = set(a.split())
    b_tokens = set(b.split())

    if not a_tokens or not b_tokens:
        return 0.0

    intersection = len(a_tokens & b_tokens)

    union = len(a_tokens | b_tokens)

    return intersection / union


# ============================================================
# CONSENT / POPUPS
# ============================================================

def handle_google_popups(page):

    selectors = [

        # English
        'button:has-text("Accept all")',
        'button:has-text("Reject all")',

        # Some variants
        'button:has-text("I agree")',
        'button:has-text("Accept")',

        # Generic
        '[aria-label="Accept all"]',
        '[aria-label="Reject all"]',

    ]

    for selector in selectors:

        try:

            locator = page.locator(selector)

            if locator.count() > 0:

                if locator.first.is_visible(timeout=1000):

                    locator.first.click(
                        timeout=2000
                    )

                    random_sleep((1, 2))

                    break

        except Exception:
            pass


# ============================================================
# SEARCH GOOGLE MAPS
# ============================================================

def build_search_url(row):

    name = row.get("business_name", "")

    address = row.get("address_barikoi", "")

    latitude = row.get("latitude", "")

    longitude = row.get("longitude", "")

    # --------------------------------------------------------
    # Prefer business name + address.
    # --------------------------------------------------------

    query_parts = []

    if pd.notna(name):
        query_parts.append(str(name))

    if pd.notna(address):
        query_parts.append(str(address))

    query = ", ".join(query_parts)

    return (
        "https://www.google.com/maps/search/"
        + quote(query)
        + "?hl=en&gl=bd"
    )


def find_best_search_result(page, target_name):

    """
    Google Maps search results can contain multiple businesses.

    We inspect visible result cards and choose the result
    with the strongest name similarity.
    """

    candidates = []

    selectors = [

        'div[role="feed"] div.Nv2PK',

        'div.Nv2PK',

    ]

    for selector in selectors:

        try:

            cards = page.locator(selector)

            count = min(cards.count(), 20)

            for i in range(count):

                card = cards.nth(i)

                try:

                    text = clean_text(
                        card.inner_text(
                            timeout=1500
                        )
                    )

                    if not text:
                        continue

                    # Usually business title is available
                    # through the link below.
                    title_locator = card.locator(
                        'a.hfpxzc'
                    )

                    title = safe_attribute(
                        title_locator,
                        "aria-label"
                    )

                    if not title:

                        title_locator = card.locator(
                            'div.qBF1Pd'
                        )

                        title = safe_text(
                            title_locator
                        )

                    if not title:
                        title = text.split("\n")[0]

                    score = name_similarity(
                        target_name,
                        title
                    )

                    candidates.append(
                        {
                            "score": score,
                            "title": title,
                            "card": card
                        }
                    )

                except Exception:
                    continue

            if candidates:
                break

        except Exception:
            continue

    if not candidates:
        return False

    candidates.sort(
        key=lambda x: x["score"],
        reverse=True
    )

    best = candidates[0]

    print(
        f"    Search match: "
        f"{best['title']} "
        f"(score={best['score']:.2f})"
    )

    if best["score"] < MIN_NAME_MATCH_SCORE:

        print(
            "    ! No sufficiently strong "
            "business-name match."
        )

        return False

    try:

        best["card"].scroll_into_view_if_needed()

        best["card"].click(
            timeout=5000
        )

        random_sleep((2.5, 4))

        return True

    except Exception:

        # Try the actual place link.
        try:

            link = best["card"].locator(
                'a.hfpxzc'
            )

            link.first.click(
                timeout=5000
            )

            random_sleep((2.5, 4))

            return True

        except Exception:

            return False


# ============================================================
# BASIC BUSINESS INFORMATION
# ============================================================

def extract_business_name(page):

    selectors = [

        "h1.DUwDvf",

        'h1',

    ]

    for selector in selectors:

        value = safe_text(
            page.locator(selector)
        )

        if value:
            return value

    return None


def extract_category(page):

    selectors = [

        'button[jsaction*="category"]',

        'button[class*="DkEaL"]',

        'div.fontBodyMedium button',

    ]

    for selector in selectors:

        value = safe_text(
            page.locator(selector)
        )

        if value:
            return value

    return None


def extract_address(page):

    selectors = [

        'button[data-item-id="address"]',

        'button[aria-label^="Address:"]',

    ]

    for selector in selectors:

        locator = page.locator(selector)

        if locator.count() == 0:
            continue

        value = safe_attribute(
            locator,
            "aria-label"
        )

        if value:

            value = re.sub(
                r"^Address:\s*",
                "",
                value,
                flags=re.I
            )

            return clean_text(value)

        value = safe_text(locator)

        if value:
            return value

    return None


def extract_phone(page):

    selectors = [

        'button[data-item-id^="phone:tel:"]',

        'button[aria-label^="Phone:"]',

        'a[href^="tel:"]',

    ]

    for selector in selectors:

        locator = page.locator(selector)

        if locator.count() == 0:
            continue

        value = safe_attribute(
            locator,
            "aria-label"
        )

        if value:

            value = re.sub(
                r"^Phone:\s*",
                "",
                value,
                flags=re.I
            )

            return clean_text(value)

        value = safe_attribute(
            locator,
            "href"
        )

        if value:

            value = value.replace(
                "tel:",
                ""
            )

            return clean_text(value)

    return None


def extract_website(page):

    selectors = [

        'a[data-item-id="authority"]',

        'a[aria-label*="Website"]',

    ]

    for selector in selectors:

        href = safe_attribute(
            page.locator(selector),
            "href"
        )

        if href:
            return href

    return None


# ============================================================
# RATING / REVIEW COUNT
# ============================================================

def extract_rating(page):

    selectors = [

        'div.F7nice span[aria-hidden="true"]',

        'div.F7nice',

    ]

    for selector in selectors:

        value = safe_text(
            page.locator(selector)
        )

        if not value:
            continue

        match = re.search(
            r"\b([0-5](?:\.\d)?)\b",
            value
        )

        if match:

            try:
                return float(
                    match.group(1)
                )

            except Exception:
                pass

    return None


def extract_review_count(page):

    selectors = [

        'button[jsaction*="pane.rating.moreReviews"]',

        'button[aria-label*="reviews"]',

        'div.F7nice',

    ]

    for selector in selectors:

        locator = page.locator(selector)

        if locator.count() == 0:
            continue

        texts = []

        count = min(locator.count(), 5)

        for i in range(count):

            try:

                text = locator.nth(i).inner_text(
                    timeout=1500
                )

                if text:
                    texts.append(text)

            except Exception:
                pass

        combined = " ".join(texts)

        match = re.search(
            r"\(?\s*([\d,]+)\s*\)?\s*(?:reviews?)?",
            combined,
            flags=re.I
        )

        if match:

            try:

                return int(
                    match.group(1).replace(
                        ",",
                        ""
                    )
                )

            except Exception:
                pass

    return None


# ============================================================
# OPENING HOURS
# ============================================================

DAYS = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]


def normalize_day(day):

    day = day.lower().strip()

    mapping = {

        "monday": "mon",
        "mon": "mon",

        "tuesday": "tue",
        "tue": "tue",

        "wednesday": "wed",
        "wed": "wed",

        "thursday": "thu",
        "thu": "thu",

        "friday": "fri",
        "fri": "fri",

        "saturday": "sat",
        "sat": "sat",

        "sunday": "sun",
        "sun": "sun",
    }

    return mapping.get(day)


def extract_opening_hours(page):

    schedule = {

        "mon": "Unknown",
        "tue": "Unknown",
        "wed": "Unknown",
        "thu": "Unknown",
        "fri": "Unknown",
        "sat": "Unknown",
        "sun": "Unknown",

    }

    # --------------------------------------------------------
    # First try to open the hours section.
    # --------------------------------------------------------

    selectors = [

        'button[data-item-id*="oh"]',

        'div[role="button"][aria-label*="Hours"]',

        'button[aria-label*="Hours"]',

        'div:has-text("Hours")',

    ]

    for selector in selectors:

        try:

            locator = page.locator(selector)

            if locator.count() > 0:

                for i in range(
                    min(locator.count(), 3)
                ):

                    element = locator.nth(i)

                    try:

                        if element.is_visible(
                            timeout=1000
                        ):

                            element.click(
                                timeout=3000,
                                force=True
                            )

                            random_sleep(
                                WAIT_AFTER_CLICK
                            )

                            break

                    except Exception:
                        continue

        except Exception:
            continue

    # --------------------------------------------------------
    # Read rows from all visible tables.
    # --------------------------------------------------------

    try:

        rows = page.locator("tr")

        for i in range(rows.count()):

            row = rows.nth(i)

            try:

                cells = row.locator("td")

                if cells.count() < 2:
                    continue

                day_text = clean_text(
                    cells.nth(0).inner_text()
                )

                hours_text = clean_text(
                    cells.nth(1).inner_text()
                )

                if not day_text:
                    continue

                short_day = normalize_day(
                    day_text
                )

                if short_day:

                    if hours_text:

                        hours_text = (
                            hours_text
                            .replace(
                                "\n",
                                " / "
                            )
                            .replace(
                                " to ",
                                " – "
                            )
                        )

                        schedule[
                            short_day
                        ] = hours_text

            except Exception:
                continue

    except Exception:
        pass

    # --------------------------------------------------------
    # Fallback: inspect visible text.
    # --------------------------------------------------------

    return schedule


# ============================================================
# BUSINESS STATUS / PRICE
# ============================================================

def extract_business_status(page):

    try:

        body = clean_text(
            page.locator("body").inner_text()
        )

        if not body:
            return None

        if re.search(
            r"permanently closed",
            body,
            re.I
        ):
            return "permanently_closed"

        if re.search(
            r"temporarily closed",
            body,
            re.I
        ):
            return "temporarily_closed"

        return "open_or_unknown"

    except Exception:

        return None


def extract_price_level(page):

    try:

        body = clean_text(
            page.locator("body").inner_text()
        )

        if not body:
            return None

        # Examples:
        # $, $$, $$$, $$$$
        matches = re.findall(
            r"\${1,4}",
            body
        )

        if matches:

            # Avoid blindly returning a random
            # dollar symbol from the page.
            counts = {}

            for item in matches:
                counts[item] = (
                    counts.get(item, 0) + 1
                )

            return max(
                counts,
                key=counts.get
            )

    except Exception:
        pass

    return None


# ============================================================
# PLUS CODE
# ============================================================

def extract_plus_code(page):

    selectors = [

        'button[data-item-id="oloc"]',

        'button[aria-label*="Plus code"]',

    ]

    for selector in selectors:

        locator = page.locator(selector)

        if locator.count() == 0:
            continue

        value = safe_attribute(
            locator,
            "aria-label"
        )

        if value:

            value = re.sub(
                r"^Plus code:\s*",
                "",
                value,
                flags=re.I
            )

            return clean_text(value)

        value = safe_text(locator)

        if value:
            return value

    return None


# ============================================================
# LATITUDE / LONGITUDE
# ============================================================

def extract_coordinates_from_url(url):

    if not url:
        return None, None

    # Example:
    # .../@23.7801,90.4171,17z/...

    match = re.search(
        r"/@(-?\d+\.\d+),(-?\d+\.\d+)",
        url
    )

    if match:

        return (
            float(match.group(1)),
            float(match.group(2))
        )

    return None, None


# ============================================================
# SOCIAL LINKS
# ============================================================

def extract_social_links(page):

    result = {

        "facebook_url": None,
        "instagram_url": None,
        "youtube_url": None,
        "linkedin_url": None,

    }

    try:

        hrefs = page.locator(
            "a[href]"
        )

        for i in range(
            hrefs.count()
        ):

            try:

                href = hrefs.nth(i).get_attribute(
                    "href"
                )

                if not href:
                    continue

                href = href.split("?")[0]

                low = href.lower()

                if (
                    "facebook.com" in low
                    and not result["facebook_url"]
                ):
                    result["facebook_url"] = href

                elif (
                    "instagram.com" in low
                    and not result["instagram_url"]
                ):
                    result["instagram_url"] = href

                elif (
                    "youtube.com" in low
                    and not result["youtube_url"]
                ):
                    result["youtube_url"] = href

                elif (
                    "linkedin.com" in low
                    and not result["linkedin_url"]
                ):
                    result["linkedin_url"] = href

            except Exception:
                continue

    except Exception:
        pass

    return result


# ============================================================
# MENU
# ============================================================

def find_menu_link(page):

    selectors = [

        'a:has-text("Menu")',

        'a[aria-label*="Menu"]',

        'a[href*="menu"]',

    ]

    for selector in selectors:

        try:

            locator = page.locator(
                selector
            )

            count = locator.count()

            for i in range(
                min(count, 5)
            ):

                href = safe_attribute(
                    locator.nth(i),
                    "href"
                )

                if href:
                    return href

        except Exception:
            continue

    return None


def extract_menu_items_from_page(page):

    """
    Best-effort extraction.

    Google Maps does not expose menu information
    in a guaranteed stable structure.

    We therefore look for common item/price patterns.
    """

    items = []

    # Possible Google Maps menu containers.
    selectors = [

        '[data-menu-item]',

        'div[role="menuitem"]',

        'div:has-text("৳")',

    ]

    for selector in selectors:

        try:

            locator = page.locator(
                selector
            )

            for i in range(
                min(locator.count(), 200)
            ):

                try:

                    text = clean_text(
                        locator.nth(i).inner_text()
                    )

                    if not text:
                        continue

                    # Look for BDT prices.
                    price_match = re.search(
                        r"(?:৳|Tk\.?|BDT)\s*([\d,]+)",
                        text,
                        re.I
                    )

                    if not price_match:
                        continue

                    price = int(
                        price_match.group(1)
                        .replace(",", "")
                    )

                    item_name = re.sub(
                        r"(?:৳|Tk\.?|BDT)\s*[\d,]+",
                        "",
                        text,
                        flags=re.I
                    ).strip()

                    if not item_name:
                        continue

                    items.append(
                        {
                            "category": None,
                            "item": item_name,
                            "price_bdt": price,
                            "source": "google_maps"
                        }
                    )

                except Exception:
                    continue

        except Exception:
            continue

    # Remove duplicates.
    final = []

    seen = set()

    for item in items:

        key = (
            item["item"].lower(),
            item["price_bdt"]
        )

        if key not in seen:

            seen.add(key)

            final.append(item)

    return final


def extract_menu(page):

    menu_url = find_menu_link(page)

    menu_items = []

    # Try extracting menu from current page.
    try:

        menu_items = (
            extract_menu_items_from_page(
                page
            )
        )

    except Exception:
        pass

    return {
        "menu_url": menu_url,
        "menu_items": menu_items
    }


# ============================================================
# RESERVATION / ORDER LINKS
# ============================================================

def extract_action_links(page):

    result = {

        "reservation_url": None,
        "order_online_url": None,

    }

    try:

        links = page.locator(
            "a[href]"
        )

        for i in range(
            links.count()
        ):

            try:

                href = links.nth(i).get_attribute(
                    "href"
                )

                aria = links.nth(i).get_attribute(
                    "aria-label"
                )

                text = clean_text(
                    links.nth(i).inner_text()
                )

                combined = " ".join(
                    filter(
                        None,
                        [
                            aria,
                            text
                        ]
                    )
                ).lower()

                if (
                    "reservation" in combined
                    or "reserve" in combined
                ):

                    if not result[
                        "reservation_url"
                    ]:
                        result[
                            "reservation_url"
                        ] = href

                elif (
                    "order online" in combined
                    or "order" in combined
                ):

                    if not result[
                        "order_online_url"
                    ]:
                        result[
                            "order_online_url"
                        ] = href

            except Exception:
                continue

    except Exception:
        pass

    return result


# ============================================================
# REVIEWS
# ============================================================

def click_reviews_tab(page):

    selectors = [

        'button[aria-label*="Reviews"]',

        'button:has-text("Reviews")',

        '[role="tab"]:has-text("Reviews")',

        'button[jsaction*="reviews"]',

    ]

    for selector in selectors:

        try:

            locator = page.locator(
                selector
            )

            count = locator.count()

            for i in range(
                min(count, 5)
            ):

                element = locator.nth(i)

                if element.is_visible(
                    timeout=1000
                ):

                    element.click(
                        timeout=4000,
                        force=True
                    )

                    random_sleep(
                        (2, 3)
                    )

                    return True

        except Exception:
            continue

    return False


def expand_review_text(card):

    selectors = [

        'button:has-text("More")',

        'span:has-text("More")',

    ]

    for selector in selectors:

        try:

            locator = card.locator(
                selector
            )

            if locator.count():

                for i in range(
                    min(locator.count(), 3)
                ):

                    try:

                        if locator.nth(i).is_visible(
                            timeout=500
                        ):

                            locator.nth(i).click(
                                timeout=1000
                            )

                    except Exception:
                        pass

        except Exception:
            pass


def extract_review_from_card(card):

    expand_review_text(card)

    author = None
    rating = None
    date = None
    text = None

    # Author
    for selector in [
        'div.d4r55',
        '[class*="d4r55"]',
        'a[href*="/contrib/"]'
    ]:

        author = safe_text(
            card.locator(selector)
        )

        if author:
            break

    # Rating
    rating_selectors = [

        'span[role="img"][aria-label*="star"]',

        'span[aria-label*="star"]',

    ]

    for selector in rating_selectors:

        aria = safe_attribute(
            card.locator(selector),
            "aria-label"
        )

        if aria:

            match = re.search(
                r"([1-5](?:\.\d)?)",
                aria
            )

            if match:

                rating = float(
                    match.group(1)
                )

                break

    # Date
    for selector in [

        'span.rsqaWe',

        '[class*="rsqaWe"]',

    ]:

        date = safe_text(
            card.locator(selector)
        )

        if date:
            break

    # Review text
    for selector in [

        'span.wiI7pd',

        '[class*="wiI7pd"]',

    ]:

        text = safe_text(
            card.locator(selector)
        )

        if text:
            break

    if not text:

        # Last fallback:
        # inspect text and remove author/date.
        try:

            raw = clean_text(
                card.inner_text()
            )

            if raw:
                text = raw

        except Exception:
            pass

    if not any(
        [
            author,
            rating,
            date,
            text
        ]
    ):
        return None

    return {

        "source": "google",

        "author": author,

        "rating": rating,

        "date": date,

        "text": text,

    }


def extract_reviews(page, max_reviews=30):

    reviews = []

    if not click_reviews_tab(page):

        print(
            "    ! Could not open reviews tab."
        )

        return reviews

    # --------------------------------------------------------
    # Find review feed.
    # --------------------------------------------------------

    feed = None

    selectors = [

        'div[role="feed"]',

        'div.m6QErb',

    ]

    for selector in selectors:

        try:

            locator = page.locator(
                selector
            )

            if locator.count():

                for i in range(
                    min(locator.count(), 5)
                ):

                    element = locator.nth(i)

                    if element.is_visible(
                        timeout=1000
                    ):

                        feed = element
                        break

                if feed:
                    break

        except Exception:
            continue

    if not feed:

        return reviews

    # --------------------------------------------------------
    # Scroll and collect reviews.
    # --------------------------------------------------------

    previous_count = 0

    no_growth = 0

    for _ in range(30):

        cards = page.locator(
            'div.jftiEf, div[data-review-id]'
        )

        current_count = cards.count()

        for i in range(
            min(current_count, max_reviews)
        ):

            try:

                review = extract_review_from_card(
                    cards.nth(i)
                )

                if review:

                    # Prevent duplicates
                    signature = (
                        review.get("author"),
                        review.get("date"),
                        review.get("text")
                    )

                    if not any(
                        (
                            r.get("author"),
                            r.get("date"),
                            r.get("text")
                        ) == signature
                        for r in reviews
                    ):

                        reviews.append(
                            review
                        )

                if len(reviews) >= max_reviews:
                    break

            except Exception:
                continue

        if len(reviews) >= max_reviews:
            break

        if current_count <= previous_count:

            no_growth += 1

        else:

            no_growth = 0

        if no_growth >= 4:
            break

        previous_count = current_count

        # Scroll the feed.
        try:

            feed.evaluate(
                """
                element => {
                    element.scrollTop =
                        element.scrollHeight;
                }
                """
            )

        except Exception:

            try:

                page.mouse.wheel(
                    0,
                    2000
                )

            except Exception:
                pass

        random_sleep(
            (1.5, 2.5)
        )

    return reviews[:max_reviews]


# ============================================================
# DESCRIPTION / ABOUT
# ============================================================

def extract_description(page):

    selectors = [

        'div.PYvSYb',

        'div.WeS02d',

        'div[jsaction*="description"]',

    ]

    for selector in selectors:

        value = safe_text(
            page.locator(selector)
        )

        if value:
            return value

    return None


# ============================================================
# IMAGES
# ============================================================

def extract_images(page, max_images=20):

    images = []

    try:

        imgs = page.locator(
            'img[src]'
        )

        for i in range(
            min(imgs.count(), 200)
        ):

            try:

                src = imgs.nth(i).get_attribute(
                    "src"
                )

                if not src:
                    continue

                if (
                    "googleusercontent.com" not in src
                    and "ggpht.com" not in src
                ):
                    continue

                images.append(src)

                if len(
                    unique_list(images)
                ) >= max_images:
                    break

            except Exception:
                continue

    except Exception:
        pass

    return unique_list(images)[:max_images]


# ============================================================
# RAW PAGE SNAPSHOT
# ============================================================

def save_raw_snapshot(page, place_code):

    raw_dir = RAW_RUN_DIR

    json_path = os.path.join(
        raw_dir,
        f"{place_code}.json"
    )

    html_path = os.path.join(
        raw_dir,
        f"{place_code}.html"
    )

    screenshot_path = os.path.join(
        raw_dir,
        f"{place_code}.png"
    )

    try:

        with open(
            html_path,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(
                page.content()
            )

    except Exception:
        pass

    try:

        page.screenshot(
            path=screenshot_path,
            full_page=False
        )

    except Exception:
        pass

    return {
        "html": html_path,
        "screenshot": screenshot_path,
        "json": json_path
    }


# ============================================================
# COMPLETENESS SCORE
# ============================================================

def calculate_completeness(record):

    important_fields = [

        "business_name",
        "address_google",
        "latitude",
        "longitude",
        "phone",
        "website",
        "google_rating",
        "google_review_count",
        "opening_hours",
        "category",
        "description",
        "menu_url",
        "reviews",

    ]

    score = 0

    total = len(
        important_fields
    )

    for field in important_fields:

        value = record.get(field)

        if value is None:
            continue

        if value == "":
            continue

        if value == []:
            continue

        if value == {}:
            continue

        score += 1

    return round(
        score / total,
        2
    )


# ============================================================
# MAIN SINGLE PLACE SCRAPER
# ============================================================

def scrape_place(page, row):

    place_code = str(
        row["place_code"]
    ).strip()

    target_name = str(
        row["business_name"]
    ).strip()

    print(
        f"\n=================================================="
    )

    print(
        f"Fetching: {target_name}"
    )

    print(
        f"Code: {place_code}"
    )

    print(
        f"=================================================="
    )

    search_url = build_search_url(
        row
    )

    result = {

        # Identity
        "place_code": place_code,
        "business_name": target_name,

        # Barikoi/source database
        "address_barikoi": (
            clean_text(
                row.get("address_barikoi")
            )
            if pd.notna(
                row.get("address_barikoi")
            )
            else None
        ),

        # Google
        "address_google": None,
        "latitude": None,
        "longitude": None,
        "phone": None,
        "website": None,
        "google_maps_url": None,

        # Social
        "facebook_url": None,
        "instagram_url": None,
        "youtube_url": None,
        "linkedin_url": None,

        # Business
        "category": None,
        "price_level": None,
        "business_status": None,
        "plus_code": None,
        "description": None,

        # Rating
        "google_rating": None,
        "google_review_count": None,

        # Hours
        "opening_hours": {},

        # Menu
        "menu_url": None,
        "menu_items": [],

        # Actions
        "reservation_url": None,
        "order_online_url": None,

        # Reviews
        "reviews": [],

        # Images
        "image_urls": [],

        # Metadata
        "data_sources": [],
        "scraped_at": datetime.now().isoformat(),
        "completeness_score": 0,
        "status": "pending",
        "error": None,

    }

    try:

        # ----------------------------------------------------
        # Open Google Maps search
        # ----------------------------------------------------

        page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=PAGE_TIMEOUT
        )

        random_sleep(
            WAIT_AFTER_PAGE_LOAD
        )

        handle_google_popups(
            page
        )

        # Wait for Maps UI.
        try:

            page.locator(
                "body"
            ).wait_for(
                state="visible",
                timeout=10000
            )

        except Exception:
            pass

        # ----------------------------------------------------
        # Open correct place
        # ----------------------------------------------------

        opened = find_best_search_result(
            page,
            target_name
        )

        if not opened:

            # Sometimes Google goes directly
            # to a place page.
            direct_name = extract_business_name(
                page
            )

            if not direct_name:

                raise RuntimeError(
                    "Could not identify the correct "
                    "Google Maps place."
                )

        # Give the detail page time to render.
        random_sleep(
            (2.5, 4)
        )

        # ----------------------------------------------------
        # Extract basic fields
        # ----------------------------------------------------

        result[
            "business_name"
        ] = (
            extract_business_name(page)
            or target_name
        )

        result[
            "category"
        ] = extract_category(page)

        result[
            "address_google"
        ] = extract_address(page)

        result[
            "phone"
        ] = extract_phone(page)

        result[
            "website"
        ] = extract_website(page)

        result[
            "google_maps_url"
        ] = page.url

        # ----------------------------------------------------
        # Coordinates
        # ----------------------------------------------------

        lat, lon = (
            extract_coordinates_from_url(
                page.url
            )
        )

        result["latitude"] = lat
        result["longitude"] = lon

        # ----------------------------------------------------
        # Rating
        # ----------------------------------------------------

        result[
            "google_rating"
        ] = extract_rating(page)

        result[
            "google_review_count"
        ] = extract_review_count(page)

        # ----------------------------------------------------
        # Hours
        # ----------------------------------------------------

        result[
            "opening_hours"
        ] = extract_opening_hours(page)

        # ----------------------------------------------------
        # Other information
        # ----------------------------------------------------

        result[
            "price_level"
        ] = extract_price_level(page)

        result[
            "business_status"
        ] = extract_business_status(page)

        result[
            "plus_code"
        ] = extract_plus_code(page)

        result[
            "description"
        ] = extract_description(page)

        # ----------------------------------------------------
        # Social links
        # ----------------------------------------------------

        social = extract_social_links(
            page
        )

        result.update(
            social
        )

        # ----------------------------------------------------
        # Menu
        # ----------------------------------------------------

        menu = extract_menu(
            page
        )

        result[
            "menu_url"
        ] = menu["menu_url"]

        result[
            "menu_items"
        ] = menu["menu_items"]

        # ----------------------------------------------------
        # Reservation / order
        # ----------------------------------------------------

        actions = extract_action_links(
            page
        )

        result.update(
            actions
        )

        # ----------------------------------------------------
        # Images
        # ----------------------------------------------------

        result[
            "image_urls"
        ] = extract_images(
            page,
            MAX_IMAGES
        )

        # ----------------------------------------------------
        # Reviews
        # ----------------------------------------------------

        result[
            "reviews"
        ] = extract_reviews(
            page,
            MAX_REVIEWS
        )

        # ----------------------------------------------------
        # Data sources
        # ----------------------------------------------------

        result[
            "data_sources"
        ] = ["google_maps"]

        # ----------------------------------------------------
        # Completeness
        # ----------------------------------------------------

        result[
            "completeness_score"
        ] = calculate_completeness(
            result
        )

        result[
            "status"
        ] = "ok"

        print(
            f"    ✓ Name: "
            f"{result['business_name']}"
        )

        print(
            f"    ✓ Rating: "
            f"{result['google_rating']} "
            f"({result['google_review_count']} reviews)"
        )

        print(
            f"    ✓ Phone: "
            f"{result['phone']}"
        )

        print(
            f"    ✓ Hours extracted"
        )

        print(
            f"    ✓ Reviews extracted: "
            f"{len(result['reviews'])}"
        )

        print(
            f"    ✓ Menu items extracted: "
            f"{len(result['menu_items'])}"
        )

        print(
            f"    ✓ Completeness: "
            f"{result['completeness_score']}"
        )

    except Exception as e:

        result[
            "status"
        ] = "error"

        result[
            "error"
        ] = str(e)

        print(
            f"    ✗ ERROR: {e}"
        )

        traceback.print_exc()

    return result


# ============================================================
# SAVE RESULT
# ============================================================

def save_place_json(record):

    path = os.path.join(
        RAW_RUN_DIR,
        f"{record['place_code']}_profile.json"
    )

    with open(
        path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            record,
            f,
            indent=2,
            ensure_ascii=False
        )

    return path


# ============================================================
# CSV INPUT VALIDATION
# ============================================================

def validate_input_csv(df):

    required = [

        "place_code",
        "business_name",

    ]

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:

        raise ValueError(
            "Input CSV is missing required "
            f"columns: {missing}"
        )


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_scraper():

    print(
        "\n"
        "====================================================\n"
        " Google Maps Cafe Data Pipeline\n"
        "===================================================="
    )

    print(
        f"Input: {INPUT_CSV}"
    )

    print(
        f"Run: {RUN_ID}"
    )

    # --------------------------------------------------------
    # Read input CSV
    # --------------------------------------------------------

    if not os.path.exists(
        INPUT_CSV
    ):

        raise FileNotFoundError(
            f"Input CSV not found:\n{INPUT_CSV}"
        )

    df = pd.read_csv(
        INPUT_CSV
    )

    validate_input_csv(
        df
    )

    print(
        f"Places found: {len(df)}"
    )

    results = []

    # --------------------------------------------------------
    # Playwright
    # --------------------------------------------------------

    with sync_playwright() as p:

        browser = p.chromium.launch(
            headless=HEADLESS
        )

        context = browser.new_context(

            viewport={
                "width": 1366,
                "height": 900
            },

            locale="en-US",

            timezone_id="Asia/Dhaka",

            user_agent=(
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/140.0 Safari/537.36"
            ),

        )

        context.set_default_timeout(
            5000
        )

        page = context.new_page()

        # ----------------------------------------------------
        # Process every CSV row
        # ----------------------------------------------------

        for index, row in df.iterrows():

            record = scrape_place(
                page,
                row
            )

            # Save individual JSON
            save_place_json(
                record
            )

            results.append(
                record
            )

            # Small delay between businesses.
            random_sleep(
                (2, 4)
            )

        browser.close()

    # ========================================================
    # FINAL OUTPUT
    # ========================================================

    final_json = os.path.join(
        PROCESSED_DIR,
        "cafes_gmaps_final.json"
    )

    final_csv = os.path.join(
        PROCESSED_DIR,
        "cafes_gmaps_final.csv"
    )

    with open(
        final_json,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            results,
            f,
            indent=2,
            ensure_ascii=False
        )

    # Flatten nested structures for CSV.
    csv_rows = []

    for record in results:

        row = record.copy()

        row[
            "opening_hours"
        ] = json.dumps(
            record.get(
                "opening_hours",
                {}
            ),
            ensure_ascii=False
        )

        row[
            "menu_items"
        ] = json.dumps(
            record.get(
                "menu_items",
                []
            ),
            ensure_ascii=False
        )

        row[
            "reviews"
        ] = json.dumps(
            record.get(
                "reviews",
                []
            ),
            ensure_ascii=False
        )

        row[
            "image_urls"
        ] = json.dumps(
            record.get(
                "image_urls",
                []
            ),
            ensure_ascii=False
        )

        row[
            "data_sources"
        ] = json.dumps(
            record.get(
                "data_sources",
                []
            ),
            ensure_ascii=False
        )

        csv_rows.append(
            row
        )

    pd.DataFrame(
        csv_rows
    ).to_csv(
        final_csv,
        index=False,
        encoding="utf-8-sig"
    )

    # ========================================================
    # SUMMARY
    # ========================================================

    success = sum(
        1
        for r in results
        if r["status"] == "ok"
    )

    failed = len(results) - success

    print(
        "\n"
        "====================================================\n"
        " SCRAPING COMPLETE\n"
        "===================================================="
    )

    print(
        f"Total places : {len(results)}"
    )

    print(
        f"Successful   : {success}"
    )

    print(
        f"Failed       : {failed}"
    )

    print(
        f"\nFinal JSON:\n{final_json}"
    )

    print(
        f"\nFinal CSV:\n{final_csv}"
    )

    print(
        f"\nRaw profiles:\n{RAW_RUN_DIR}"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_scraper()