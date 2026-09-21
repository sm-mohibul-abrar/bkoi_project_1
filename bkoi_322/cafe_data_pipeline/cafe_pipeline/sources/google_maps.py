"""M-1 source: Google Maps place pages.

For every seed cafe we run one Maps search pinned to the seed coordinates,
open the first place, verify it is really the same venue (name similarity +
distance) and extract the priority fields: rating, review count, up to five
recent reviews (stars, date, text -- never reviewer names), opening hours,
plus the secondary contact fields.

DOM selectors are data, not code: defaults live below, config.yaml can
override any key when Google reshuffles its markup.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote_plus, urlparse

from ..browser import click_first, first_attr, first_text, goto, scroll_panel
from ..hours import canonical_day, hours_lines_from_text, hours_text_to_hhmm
from ..matching import classify_match
from ..pacing import HostGuard
from ..settings import Settings
from ..utils import (clean_url, haversine_m, name_match_score, norm_bd_phone,
                     now_iso, parse_count, parse_rating)

log = logging.getLogger("cafe_pipeline.gmaps")

HOST = "www.google.com"

RE_PLACE_COORDS = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)")
RE_VIEW_COORDS = re.compile(r"@(-?\d+\.\d+),(-?\d+\.\d+)")
RE_CID = re.compile(r"!1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)")
RE_STARS = re.compile(r"([1-5])")
RE_FB = re.compile(r"https?://(?:www\.|m\.|web\.)?facebook\.com/"
                   r"(?!sharer|share|tr[/?]|dialog|plugins|events/)[\w.\-]+/?")
RE_IG = re.compile(r"https?://(?:www\.)?instagram\.com/"
                   r"(?!p/|reel/|explore/|accounts/)[\w.\-]+/?")

# Status vocabulary shared with the record model.
OPERATIONAL = "OPERATIONAL"
TEMPORARILY_CLOSED = "TEMPORARILY_CLOSED"
PERMANENTLY_CLOSED = "PERMANENTLY_CLOSED"

DEFAULT_SELECTORS: dict[str, list[str]] = {
    "consent":       ['#L2AGLb', 'button[aria-label*="Accept all" i]',
                      'form[action*="consent"] button'],
    "dismiss":       ['button[aria-label="Dismiss"]', 'button:has-text("Dismiss")'],
    "title":         ['h1.DUwDvf', 'h1[class*="DUwDvf"]', 'div[role="main"] h1'],
    "place_link":    ['a[href*="/maps/place/"]'],
    "status_badge":  ['span.fCEvvc', '[class*="fCEvvc"]', 'span.o0Svhf'],
    "phone_btn":     ['button[data-item-id^="phone:tel:"]',
                      '[data-item-id^="phone:tel:"]'],
    "address_btn":   ['button[data-item-id="address"]', '[data-item-id="address"]'],
    "website_btn":   ['a[data-item-id="authority"]', '[data-item-id="authority"]'],
    "menu_btn":      ['a[data-item-id="menu"]', 'button[data-item-id="menu"]',
                      'a[jsaction*="menu"]'],
    "plus_code":     ['[data-item-id="oloc"]'],
    "category":      ['button[jsaction*="category"]', 'button.DkEaL'],
    "price_range":   ['[aria-label*="Price range"]', '[aria-label*="price range"]',
                      'span[aria-label*="Price"]'],
    "rating":        ['div.F7nice span[aria-hidden="true"]',
                      'span[aria-label$="stars"]',
                      'div[class*="fontDisplayLarge"]'],
    "review_count":  ['div.F7nice span[aria-label*="review"]',
                      'button[jsaction*="reviewChart"] span',
                      'span[aria-label*="reviews"]'],
    "hours_toggle":  ['button[data-item-id="oh"]',
                      'button[aria-label*="hours" i][jsaction]'],
    "reviews_tab":   ['button[role="tab"][aria-label*="Reviews"]',
                      'button[aria-label*="Reviews"]',
                      'button[role="tab"]:has-text("Reviews")',
                      'button[jsaction*="moreReviews"]'],
    "review_container": ['div[data-review-id]', 'div.jftiEf'],
    "review_text":   ['span.wiI7pd', 'div.MyEned span', '[class*="wiI7pd"]'],
    "review_stars":  ['span.kvMYJc', 'span[role="img"][aria-label*="star"]'],
    "review_date":   ['span.rsqaWe', 'span.xRkPPb', '[class*="rsqaWe"]'],
    "scroll_panel":  ['div[role="main"]', 'div[role="feed"]'],
}


def selectors_for(cfg: Settings) -> dict[str, list[str]]:
    """Default selectors overridden per-key by config.yaml."""
    merged = {k: list(v) for k, v in DEFAULT_SELECTORS.items()}
    for key, values in (cfg.selectors("google_maps") or {}).items():
        if isinstance(values, list) and values:
            merged[key] = list(values)
    return merged


@dataclass
class ScrapeOutcome:
    status: str                        # ok | not_found | geo_mismatch | no_match
    url: str | None = None             # the place URL we landed on
    payload: dict[str, Any] = field(default_factory=dict)
    confidence: str | None = None      # high | medium | low (only when ok)


def _search_url(seed: dict[str, Any]) -> str:
    area = seed.get("sub_area") or seed.get("area") or "Dhaka"
    query = f"{seed['business_name']}, {area}, Dhaka, Bangladesh"
    return ("https://www.google.com/maps/search/" + quote_plus(query)
            + f"/@{seed['latitude']},{seed['longitude']},17z?hl=en")


def _detect_status(badge_text: str, body_head: str) -> str:
    """Badge text first; body head only as fallback so a review mentioning
    'closed' can never poison the status."""
    for text in (badge_text, body_head):
        low = text.lower()
        if "permanently closed" in low or "closed permanently" in low:
            return PERMANENTLY_CLOSED
        if "temporarily closed" in low:
            return TEMPORARILY_CLOSED
    return OPERATIONAL


async def _node_attr(node, selectors: list[str], attr: str) -> str | None:
    """first_attr, but scoped to a container node instead of the page."""
    for selector in selectors:
        try:
            locator = node.locator(selector).first
            if await locator.count():
                value = await locator.get_attribute(attr, timeout=800)
                if value:
                    return value
        except Exception:                          # noqa: BLE001
            continue
    return None


async def _dismiss_dialogs(page, sel: dict[str, list[str]]) -> None:
    """Google pops a sign-in teaser over Maps; it swallows panel clicks."""
    await click_first(page, sel["dismiss"], settle_ms=800)


def _clean_price_level(label: str) -> str:
    """'Price range, ৳600–1,600 per person, Reported by 78 people' ->
    '৳600–1,600 per person'."""
    match = re.search(r"[৳Tk]?\s*[\d,]+\s*[–—-]\s*[৳Tk]?\s*[\d,]+"
                      r"(?:\s*per person)?", label or "")
    return match.group(0).strip() if match else (label or "").strip()


async def _collect_review_nodes(page, sel: dict[str, list[str]],
                                max_reviews: int,
                                min_len: int) -> list[dict[str, Any]]:
    """Read up to N reviews from whatever is currently rendered.
    Reviewer identity is dropped: only stars, text and date come out."""
    reviews: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for container_sel in sel["review_container"]:
        try:
            containers = await page.locator(container_sel).all()
        except Exception:                          # noqa: BLE001
            continue
        for node in containers:
            if len(reviews) >= max_reviews:
                break
            try:
                review_id = await node.get_attribute("data-review-id")
            except Exception:                      # noqa: BLE001
                review_id = None
            if review_id:
                if review_id in seen_ids:
                    continue
                seen_ids.add(review_id)
            try:
                text = (await node.locator(
                    ", ".join(sel["review_text"])).first
                    .inner_text(timeout=1200)).strip()
            except Exception:                      # noqa: BLE001
                continue
            text = re.sub(r"\s+", " ", text)
            if len(text) < min_len:
                continue
            stars = None
            stars_label = await _node_attr(node, sel["review_stars"],
                                           "aria-label")
            if stars_label:
                match = RE_STARS.search(stars_label)
                stars = int(match.group(1)) if match else None
            date = None
            for date_sel in sel["review_date"]:
                try:
                    date = (await node.locator(date_sel).first
                            .inner_text(timeout=800)).strip()
                    if date:
                        break
                except Exception:                  # noqa: BLE001
                    continue
            reviews.append({"stars": stars, "text": text[:600], "date": date})
        if reviews:
            break
    return reviews[:max_reviews]


async def _extract_reviews(page, sel: dict[str, list[str]],
                           max_reviews: int, min_len: int) -> list[dict[str, Any]]:
    """The place page already renders the most recent reviews inline; read
    them there first. The 'Reviews' tab is only a fallback -- in the
    current DOM clicking it navigates to a separate reviews view, which
    loses the place context."""
    await _dismiss_dialogs(page, sel)
    await scroll_panel(page, sel["scroll_panel"], steps=3)
    reviews = await _collect_review_nodes(page, sel, max_reviews, min_len)
    if reviews:
        return reviews

    # Last resort only: the tab click can navigate to a separate reviews
    # view, so it is used just when the place page rendered no reviews.
    if await click_first(page, sel["reviews_tab"], settle_ms=2600):
        await scroll_panel(page, sel["scroll_panel"], steps=4)
        reviews = await _collect_review_nodes(page, sel, max_reviews, min_len)
    return reviews


# Runs in the page: after the hours widget expands, day rows appear as
# "Monday 7 AM–12 AM" (markup varies -- tr, li, role=row -- text does not).
_COLLECT_DAY_ROWS_JS = """() => {
    const out = {};
    const re = /^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\\s*[:,]?\\s*(.+)$/;
    for (const node of document.querySelectorAll('tr, [role="row"], li')) {
        const text = (node.innerText || '')
            .replace(/[\\ue000-\\uf8ff\\u200b-\\u200f\\ufeff]/g, '')
            .replace(/\\s+/g, ' ').trim();
        const m = text.match(re);
        if (m && /AM|PM|Closed|24 hours/i.test(m[2]) && m[2].length < 40
            && !(m[1] in out)) {
            out[m[1]] = m[2];
        }
    }
    return out;
}"""


async def _extract_hours(page, sel: dict[str, list[str]]) -> dict[str, str]:
    """Expand the hours widget and read the week.

    A plain Playwright click on the hours button is swallowed by Google's
    jsaction layer; focusing it and pressing Enter expands the table
    reliably. The collapsed button's aria-label is the last resort -- some
    DOM variants carry the whole week there as text.
    """
    raw: dict[str, str] = {}
    for toggle_sel in sel["hours_toggle"]:
        try:
            button = page.locator(toggle_sel).first
            if not await button.count():
                continue
            await button.focus(timeout=3000)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(2200)
            rows = await page.evaluate(_COLLECT_DAY_ROWS_JS)
            if isinstance(rows, dict):
                raw.update({k: v for k, v in rows.items() if k not in raw})
            if len(raw) >= 5:
                break
        except Exception:                          # noqa: BLE001
            continue

    schedule: dict[str, str] = {}
    for day, value in raw.items():
        key = canonical_day(day)
        converted = hours_text_to_hhmm(value)
        if key and converted is not None:
            schedule[key] = converted
    if schedule:
        return schedule

    label = await first_attr(page, sel["hours_toggle"], "aria-label") or ""
    return hours_lines_from_text(label)


async def scrape_google_maps(page, seed: dict[str, Any], guard: HostGuard,
                             cfg: Settings) -> ScrapeOutcome:
    """Scrape one cafe. Returns the canonical payload on a verified match."""
    sel = selectors_for(cfg)
    body = await goto(page, _search_url(seed), HOST, guard, cfg)
    if body is None:
        return ScrapeOutcome("blocked_or_error")
    await click_first(page, sel["consent"], settle_ms=2000)
    await _dismiss_dialogs(page, sel)

    if "/maps/place/" not in page.url:
        try:
            link = page.locator(sel["place_link"][0]).first
            if await link.count():
                await link.click(timeout=6000)
                await page.wait_for_timeout(float(cfg.scraping.settle_ms))
        except Exception:                          # noqa: BLE001
            pass
    if "/maps/place/" not in page.url:
        return ScrapeOutcome("not_found")

    place_url = page.url
    payload: dict[str, Any] = {"fetched_at": now_iso(),
                               "google_maps_url": place_url.split("&")[0]}

    # ── identity: coordinates, cid, title, distance ────────────────────────
    match = RE_PLACE_COORDS.search(place_url) \
        or RE_PLACE_COORDS.search(await page.content())
    if match:
        payload["latitude"], payload["longitude"] = (float(match.group(1)),
                                                     float(match.group(2)))
    else:
        view = RE_VIEW_COORDS.search(place_url)
        if view:
            payload["latitude"], payload["longitude"] = (float(view.group(1)),
                                                         float(view.group(2)))
    cid = RE_CID.search(place_url)
    if cid:
        payload["google_cid"] = cid.group(1)

    title = await first_text(page, sel["title"], min_len=2)
    name_score = name_match_score(seed["business_name"], title or "")
    if title:
        payload["matched_name"] = title
        payload["name_match"] = round(name_score, 3)

    distance = None
    if payload.get("latitude") is not None:
        distance = haversine_m(seed["latitude"], seed["longitude"],
                               payload["latitude"], payload["longitude"])
        payload["distance_m"] = round(distance)
    confidence, status = classify_match(name_score, distance, cfg)
    if status != "ok":
        log.info("  [g] %s: name=%.2f dist=%s -> %s", seed["business_name"][:34],
                 name_score,
                 f"{distance:.0f}m" if distance is not None else "?",
                 status)
        return ScrapeOutcome(status, url=place_url)

    # ── status / contact / secondary fields ────────────────────────────────
    badge = await first_text(page, sel["status_badge"]) or ""
    payload["business_status"] = _detect_status(badge, body[:2500])

    phone_raw = await first_attr(page, sel["phone_btn"], "data-item-id")
    if phone_raw:
        payload["phone"] = norm_bd_phone(phone_raw.replace("phone:tel:", ""))
    address = await first_text(page, sel["address_btn"], min_len=8)
    if address:
        payload["address"] = re.sub(
            r"\s+", " ", address.replace("Address:", "")).strip()
    website = clean_url(await first_attr(page, sel["website_btn"], "href"))
    if website and "google." not in urlparse(website).netloc:
        payload["website"] = website
    menu_url = clean_url(await first_attr(page, sel["menu_btn"], "href"))
    if menu_url:
        payload["menu_url"] = menu_url
    plus_code = await first_text(page, sel["plus_code"], min_len=4)
    if plus_code:
        payload["plus_code"] = plus_code.replace("Plus code:", "").strip()
    category = await first_text(page, sel["category"], min_len=3)
    if category:
        payload["category"] = category.strip()
    price_level = await first_attr(page, sel["price_range"], "aria-label")
    if price_level:
        payload["price_level"] = _clean_price_level(price_level)

    # ── rating and review count ────────────────────────────────────────────
    rating_text = await first_text(page, sel["rating"])
    rating = parse_rating(rating_text)
    count_text = (await first_text(page, sel["review_count"])
                  or await first_attr(page, sel["review_count"], "aria-label"))
    count, approximate = parse_count(count_text)
    if rating is None or count is None:
        fallback = re.search(r"([0-5]\.\d)\s*\(?\s*([\d,.]+\s*[kKmM]?)", body)
        if fallback:
            rating = rating or parse_rating(fallback.group(1))
            if count is None:
                count, approximate = parse_count(fallback.group(2))
    if rating is not None:
        payload["rating"] = rating
    if count is not None:
        payload["review_count"] = count
        payload["review_count_approximate"] = approximate

    # ── reviews ────────────────────────────────────────────────────────────
    # Must run BEFORE the hours expansion: expanding the hours widget
    # re-renders the panel and drops the lazily-rendered review nodes.
    reviews = await _extract_reviews(
        page, sel, int(cfg.reviews.max_per_platform),
        int(cfg.reviews.min_text_len))
    if reviews:
        payload["reviews"] = reviews

    # ── opening hours (dine-in) ────────────────────────────────────────────
    schedule = await _extract_hours(page, sel)
    if len(schedule) >= int(cfg.hours.min_days):
        payload["hours"] = schedule

    # ── social links discovered on the page ────────────────────────────────
    html = await page.content()
    fb, ig = RE_FB.search(html), RE_IG.search(html)
    if fb:
        payload["facebook_url"] = clean_url(fb.group(0))
    if ig:
        payload["instagram_url"] = clean_url(ig.group(0))

    log.info("  [g] %s: rating=%s count=%s hours=%dd reviews=%d",
             (title or seed["business_name"])[:34],
             payload.get("rating"), payload.get("review_count"),
             len(payload.get("hours", {})), len(payload.get("reviews", [])))
    return ScrapeOutcome("ok", url=place_url, payload=payload,
                         confidence=confidence)
