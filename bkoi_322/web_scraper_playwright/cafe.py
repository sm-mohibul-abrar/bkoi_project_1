import asyncio
import csv
import json
import logging
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ─────────────────────────────────────────────
#  PATHS  (matches your VS Code project layout)
# ─────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent                        # bkoi_322/
DATA_DIR    = BASE_DIR / "data"
LOG_DIR     = BASE_DIR / "logs"
INPUT_CSV   = Path("/home/barikoi/Downloads/places_202609161553.csv")
OUT_JSON    = DATA_DIR / "cafes_gulshan.json"
OUT_CSV     = DATA_DIR / "cafes_gulshan.csv"
PROGRESS    = DATA_DIR / "progress.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
log_path = LOG_DIR / f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  10 POPULAR CAFES  (selected by popularity_ranking from CSV)
# ─────────────────────────────────────────────
TOP_10 = [
    {
        "place_code":    "ZGPBU53872",
        "business_name": "NORTH END coffee roasters",
        "address_barikoi": "NORTH END coffee roasters, City Bank Center, House 28, Gulshan Avenue, Gulshan 1, Gulshan, Dhaka",
        "latitude":  23.777572,
        "longitude": 90.416966,
    },
    {
        "place_code":    "NYKC3699",
        "business_name": "The Chocolate Room",
        "address_barikoi": "The Chocolate Room, Star Center, House 2, Road 138, Gulshan Avenue, Gulshan 1, Gulshan, Dhaka",
        "latitude":  23.778352,
        "longitude": 90.416830,
    },
    {
        "place_code":    "SGDY9062",
        "business_name": "Tabaq Coffee",
        "address_barikoi": "Tabaq Coffee, Crystal Palace, House 22, Bir Uttam Mir Shawkat Sarak, Gulshan 1, Gulshan, Dhaka",
        "latitude":  23.776869,
        "longitude": 90.416788,
    },
    {
        "place_code":    "OGSU6472",
        "business_name": "North End Coffee Roasters",
        "address_barikoi": "North End Coffee Roasters, Lotus Kamal Tower 2, House 59/61, Gulshan Avenue, Gulshan 1, Gulshan, Dhaka",
        "latitude":  23.781976,
        "longitude": 90.416401,
    },
    {
        "place_code":    "QGFBD20949",
        "business_name": "KOI The",
        "address_barikoi": "KOI The, Elegant Square, House 3/B, Gulshan Avenue, Gulshan 2, Gulshan, Dhaka",
        "latitude":  23.790222,
        "longitude": 90.416314,
    },
    {
        "place_code":    "GOUG0093",
        "business_name": "Arabika Coffee",
        "address_barikoi": "Arabika Coffee, Nasrin Casabella, House 2/A, Gulshan Avenue, Gulshan 2, Gulshan, Dhaka",
        "latitude":  23.798170,
        "longitude": 90.412587,
    },
    {
        "place_code":    "DPDVR63831",
        "business_name": "KONA Cafe",
        "address_barikoi": "KONA Cafe, Gulshan 2, Gulshan, Dhaka",
        "latitude":  23.790113,
        "longitude": 90.412408,
    },
    {
        "place_code":    "KZTBH98366",
        "business_name": "Biancaffe Gulshan",
        "address_barikoi": "Biancaffe Gulshan, Road 35, Gulshan 2, Gulshan, Dhaka",
        "latitude":  23.788184,
        "longitude": 90.414546,
    },
    {
        "place_code":    "GQAWI95253",
        "business_name": "Crimson Cup Coffee",
        "address_barikoi": "Crimson Cup Coffee, House 2, Road 24, Gulshan 1, Gulshan, Dhaka",
        "latitude":  23.783389,
        "longitude": 90.416239,
    },
    {
        "place_code":    "AWSP6513",
        "business_name": "The White Canary",
        "address_barikoi": "The White Canary, House 12/A, Road 86, Gulshan 2, Gulshan, Dhaka",
        "latitude":  23.799362,
        "longitude": 90.415685,
    },
]

# ─────────────────────────────────────────────
#  USER-AGENT POOL
# ─────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
BD_PHONE_RE = re.compile(
    r'(\+?880[-\s]?1[3-9]\d{8}|01[3-9]\d{8})'
)

def normalize_phone(raw: str) -> str | None:
    if not raw:
        return None
    digits = re.sub(r'[\s\-\(\)]', '', raw)
    if digits.startswith('880') and not digits.startswith('+880'):
        digits = '+' + digits
    elif digits.startswith('01'):
        digits = '+880' + digits
    return digits


def load_progress() -> dict:
    if PROGRESS.exists():
        with open(PROGRESS) as f:
            return json.load(f)
    return {"done": [], "failed": []}


def save_progress(prog: dict):
    with open(PROGRESS, "w") as f:
        json.dump(prog, f, indent=2)


def save_results(results: list):
    """Save JSON and CSV after every cafe (crash-safe)."""
    # ── JSON ──
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # ── CSV ── (flat: one row per cafe, hours/menu/reviews as JSON strings)
    if not results:
        return
    csv_fields = [
        "place_code", "business_name",
        "address_barikoi", "address_google",
        "latitude", "longitude",
        "phone", "website",
        "google_rating", "google_review_count",
        "opening_hours_json",
        "menu_url", "menu_items_json",
        "reviews_json",
        "facebook_url", "instagram_url",
        "scraped_at", "status",
    ]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            row = dict(r)
            row["opening_hours_json"] = json.dumps(r.get("opening_hours", {}), ensure_ascii=False)
            row["menu_items_json"]    = json.dumps(r.get("menu_items", []),    ensure_ascii=False)
            row["reviews_json"]       = json.dumps(r.get("reviews", []),       ensure_ascii=False)
            writer.writerow(row)


# ─────────────────────────────────────────────
#  CORE SCRAPER
# ─────────────────────────────────────────────
async def scrape_google_maps(page, cafe: dict) -> dict:
    """
    Scrape one cafe from Google Maps.
    Returns a dict matching the required JSON schema.
    """
    name = cafe["business_name"]

    result = {
        "place_code":         cafe["place_code"],
        "business_name":      name,
        "address_barikoi":    cafe["address_barikoi"],
        "address_google":     None,
        "latitude":           cafe["latitude"],
        "longitude":          cafe["longitude"],
        "phone":              None,
        "website":            None,
        "google_rating":      None,
        "google_review_count": None,
        "opening_hours":      {},
        "menu_url":           None,
        "menu_items":         [],
        "reviews":            [],
        "facebook_url":       None,
        "instagram_url":      None,
        "scraped_at":         datetime.utcnow().isoformat() + "Z",
        "status":             "pending",
    }

    try:
        # ── 1. Navigate to Google Maps search ──────────────────────────
        query = f"{name} Gulshan Dhaka Bangladesh"
        search_url = "https://www.google.com/maps/search/" + query.replace(" ", "+")
        log.info(f"  → GET {search_url}")

        await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(3_500)

        # ── 2. Click first result if on search-list page ───────────────
        try:
            first_link = page.locator('a[href*="/maps/place/"]').first
            if await first_link.count():
                await first_link.click()
                await page.wait_for_timeout(3_500)
        except PWTimeout:
            pass

        result["google_maps_url"] = page.url

        # ── 3. Full body text (used as regex fallback) ─────────────────
        body_text = await page.inner_text("body")

        # ── 4. Rating ──────────────────────────────────────────────────
        try:
            # aria-label like "4.5 stars"
            rating_el = page.locator('[aria-label*="stars"], [aria-label*="star"]').first
            lbl = await rating_el.get_attribute("aria-label", timeout=3_000)
            m = re.search(r'([\d.]+)\s*star', lbl or "")
            if m:
                result["google_rating"] = float(m.group(1))
        except Exception:
            pass

        if not result["google_rating"]:
            # fallback: first float in 1.0–5.0 range near the page top
            m = re.search(r'\b([1-4]\.\d|5\.0)\b', body_text[:3000])
            if m:
                result["google_rating"] = float(m.group(1))

        # ── 5. Review count ────────────────────────────────────────────
        try:
            rc_el = page.locator('button[jsaction*="review"] span, [aria-label*="review"]').first
            rc_txt = await rc_el.inner_text(timeout=3_000)
            m = re.search(r'([\d,]+)', rc_txt)
            if m:
                result["google_review_count"] = int(m.group(1).replace(",", ""))
        except Exception:
            pass

        if not result["google_review_count"]:
            m = re.search(r'([\d,]+)\s*(?:Google\s+)?reviews?', body_text, re.I)
            if m:
                result["google_review_count"] = int(m.group(1).replace(",", ""))

        # ── 6. Phone ───────────────────────────────────────────────────
        # data-item-id="phone:tel:+8801..."
        try:
            phone_el = page.locator('[data-item-id^="phone:tel:"]').first
            if await phone_el.count():
                raw_id = await phone_el.get_attribute("data-item-id")
                raw_ph = raw_id.replace("phone:tel:", "").strip()
                result["phone"] = normalize_phone(raw_ph)
        except Exception:
            pass

        if not result["phone"]:
            m = BD_PHONE_RE.search(body_text)
            if m:
                result["phone"] = normalize_phone(m.group(1))

        # ── 7. Website ─────────────────────────────────────────────────
        try:
            web_el = page.locator('[data-item-id="authority"]').first
            if await web_el.count():
                result["website"] = await web_el.get_attribute("href", timeout=3_000)
        except Exception:
            pass

        if not result["website"]:
            try:
                web_el2 = page.locator('a[href^="http"][data-item-id*="web"]').first
                if await web_el2.count():
                    result["website"] = await web_el2.get_attribute("href", timeout=2_000)
            except Exception:
                pass

        # ── 8. Verified address ────────────────────────────────────────
        try:
            addr_el = page.locator('[data-item-id="address"]').first
            if await addr_el.count():
                result["address_google"] = (await addr_el.inner_text(timeout=3_000)).strip()
        except Exception:
            pass

        if not result["address_google"]:
            try:
                # text after the pin icon
                addr_el2 = page.locator('button[data-item-id="address"] .fontBodyMedium').first
                if await addr_el2.count():
                    result["address_google"] = (await addr_el2.inner_text(timeout=2_000)).strip()
            except Exception:
                pass

        # ── 9. Opening hours ───────────────────────────────────────────
        try:
            # expand the hours dropdown
            for sel in [
                'button[aria-label*="hours"]',
                'button[data-item-id*="oh"]',
                '[jsaction*="openhours"]',
            ]:bkoi_322/web_scraper_playwright/cafe.py
                btn = page.locator(sel).first
                if await btn.count():
                    await btn.click()
                    await page.wait_for_timeout(1_500)
                    break

            # try structured table first
            rows = await page.locator('table.eK4R0e tr').all()
            if not rows:
                rows = await page.locator('tr[class*="hour"]').all()

            for row in rows:
                try:
                    cells = await row.locator("td").all_inner_texts()
                    if len(cells) >= 2:
                        day = cells[0].strip()[:3].lower()
                        hrs = cells[1].strip()
                        if day in {"mon","tue","wed","thu","fri","sat","sun"}:
                            result["opening_hours"][day] = hrs
                except Exception:
                    pass

        except Exception:
            pass

        # Fallback: regex parse from body text
        if not result["opening_hours"]:
            day_map = {
                "Monday":"mon","Tuesday":"tue","Wednesday":"wed",
                "Thursday":"thu","Friday":"fri","Saturday":"sat","Sunday":"sun",
            }
            pattern = re.compile(
                r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)'
                r'[\s:]*'
                r'([\d:]+\s*(?:AM|PM)?[\s\u2013\-]+[\d:]+\s*(?:AM|PM)?|Closed|Open 24 hours)',
                re.IGNORECASE,
            )
            for m in pattern.finditer(body_text):
                d = day_map.get(m.group(1).capitalize())
                if d:
                    result["opening_hours"][d] = m.group(2).strip()

        # ── 10. Reviews ────────────────────────────────────────────────
        try:
            # Click Reviews tab
            for sel in [
                'button[jsaction*="review"]',
                'a[href*="reviews"]',
                '[aria-label*="Reviews"]',
            ]:
                rev_btn = page.locator(sel).first
                if await rev_btn.count():
                    await rev_btn.click()
                    await page.wait_for_timeout(2_500)
                    break

            # Scroll to load more reviews
            review_panel = page.locator('[role="main"]').first
            for _ in range(3):
                await review_panel.evaluate("el => el.scrollBy(0, 800)")
                await page.wait_for_timeout(800)

            # Expand "More" buttons
            more_btns = await page.locator('button[aria-label="See more"]').all()
            for btn in more_btns[:10]:
                try:
                    await btn.click()
                    await page.wait_for_timeout(300)
                except Exception:
                    pass

            # Extract review cards
            cards = await page.locator('[data-review-id]').all()
            if not cards:
                cards = await page.locator('[class*="jftiEf"]').all()

            for card in cards[:10]:
                try:
                    author = ""
                    try:
                        author = (await card.locator('[class*="d4r55"]').first.inner_text(timeout=1_000)).strip()
                    except Exception:
                        pass

                    rating = None
                    try:
                        s_lbl = await card.locator('[aria-label*="star"]').first.get_attribute("aria-label", timeout=1_000)
                        sm = re.search(r'(\d)', s_lbl or "")
                        if sm:
                            rating = int(sm.group(1))
                    except Exception:
                        pass

                    date_txt = ""
                    try:
                        date_txt = (await card.locator('[class*="rsqaWe"]').first.inner_text(timeout=1_000)).strip()
                    except Exception:
                        pass

                    text = ""
                    try:
                        text = (await card.locator('[class*="wiI7pd"]').first.inner_text(timeout=1_000)).strip()
                    except Exception:
                        pass

                    if author or text:
                        result["reviews"].append({
                            "source": "google",
                            "author": author,
                            "rating": rating,
                            "date":   date_txt,
                            "text":   text,
                        })
                except Exception:
                    pass

        except Exception as e:
            log.warning(f"  Reviews error: {e}")

        # ── 11. Menu items ─────────────────────────────────────────────
        try:
            # Some Maps listings show a "Menu" section or tab
            menu_tab = page.locator('button[aria-label*="Menu"], [data-tab-index][aria-label*="Menu"]').first
            if await menu_tab.count():
                await menu_tab.click()
                await page.wait_for_timeout(2_000)

                # Grab menu URL if it's a link
                menu_link = page.locator('a[href*="menu"], a[aria-label*="menu"]').first
                if await menu_link.count():
                    result["menu_url"] = await menu_link.get_attribute("href")

                # Try to extract items from the menu panel
                item_els = await page.locator('[class*="menu-item"], [data-item-id*="menu"]').all()
                for el in item_els[:30]:
                    try:
                        item_text = (await el.inner_text(timeout=800)).strip()
                        price_m = re.search(r'[৳\u09F3]?\s*([\d,]+)', item_text)
                        price = int(price_m.group(1).replace(",", "")) if price_m else None
                        # Remove price from name
                        item_name = re.sub(r'\s*[৳\u09F3\d,]+\s*$', '', item_text).strip()
                        if item_name:
                            result["menu_items"].append({
                                "category":  None,
                                "item":      item_name,
                                "price_bdt": price,
                                "source":    "google_maps",
                            })
                    except Exception:
                        pass

            # Fallback: look for price patterns in body text
            if not result["menu_items"]:
                # Pattern: "Item Name ৳ 200" or "Item Name - 200 BDT"
                item_pattern = re.compile(
                    r'([A-Z][A-Za-z\s&\'\-]{3,40})\s+[৳\u09F3]\s*([\d,]+)',
                )
                for m in item_pattern.finditer(body_text):
                    item = m.group(1).strip()
                    price = int(m.group(2).replace(",", ""))
                    if 3 < len(item) < 50 and 50 < price < 5000:
                        result["menu_items"].append({
                            "category":  None,
                            "item":      item,
                            "price_bdt": price,
                            "source":    "google_maps_text",
                        })

        except Exception as e:
            log.warning(f"  Menu error: {e}")

        # ── 12. Social links from body ─────────────────────────────────
        fb_m = re.search(r'https?://(?:www\.)?facebook\.com/[\w.\-/]+', body_text)
        if fb_m:
            result["facebook_url"] = fb_m.group(0).rstrip(')')

        ig_m = re.search(r'https?://(?:www\.)?instagram\.com/[\w.\-/]+', body_text)
        if ig_m:
            result["instagram_url"] = ig_m.group(0).rstrip(')')

        result["status"] = "ok"
        log.info(
            f"  rating={result['google_rating']}  "
            f"reviews={len(result['reviews'])}  "
            f"hours={len(result['opening_hours'])} days  "
            f"menu={len(result['menu_items'])} items  "
            f"phone={result['phone'] or 'N/A'}"
        )

    except Exception as e:
        result["status"] = f"error: {str(e)[:120]}"
        log.error(f"  {name}: {e}")

    return result


# ─────────────────────────────────────────────
#  MAIN RUNNER
# ─────────────────────────────────────────────
async def main():
    log.info("=" * 60)
    log.info("  BARIKOI CAFE GULSHAN — GOOGLE MAPS SCRAPER")
    log.info(f"  Target: {len(TOP_10)} cafes")
    log.info(f"  Output: {OUT_JSON}")
    log.info("=" * 60)

    prog     = load_progress()
    done_set = set(prog["done"])
    results  = []

    # Load existing results so we don't overwrite on resume
    if OUT_JSON.exists():
        with open(OUT_JSON, encoding="utf-8") as f:
            results = json.load(f)
        log.info(f"Resuming — {len(results)} already saved")

    remaining = [c for c in TOP_10 if c["place_code"] not in done_set]
    log.info(f"Remaining: {len(remaining)} cafes\n")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        for i, cafe in enumerate(remaining):
            name = cafe["business_name"]
            log.info(f"\n[{i+1}/{len(remaining)}]  {name}")
            log.info(f"  place_code = {cafe['place_code']}")

            # New context per cafe (fresh cookies, less fingerprinting)
            ctx = await browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                timezone_id="Asia/Dhaka",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )

            # Block images/fonts to speed up loading
            await ctx.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,otf}",
                lambda route: route.abort(),
            )

            page = await ctx.new_page()

            # ── Stealth: hide webdriver flag ──
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = { runtime: {} };
            """)

            data = await scrape_google_maps(page, cafe)

            await ctx.close()

            # ── Save immediately ──
            # Remove stale entry then append fresh
            results = [r for r in results if r["place_code"] != cafe["place_code"]]
            results.append(data)
            save_results(results)

            prog["done"].append(cafe["place_code"])
            if data["status"] != "ok":
                prog["failed"].append({"place_code": cafe["place_code"], "reason": data["status"]})
            save_progress(prog)

            # ── Polite delay (3–8 s) before next cafe ──
            if i < len(remaining) - 1:
                delay = random.uniform(3.0, 8.0)
                log.info(f"  ⏳ Waiting {delay:.1f}s …")
                await asyncio.sleep(delay)

        await browser.close()

    # ── Final summary ──
    log.info("\n" + "=" * 60)
    log.info("  DONE")
    log.info(f"  Total records : {len(results)}")
    log.info(f"  Status OK     : {sum(1 for r in results if r['status'] == 'ok')}")
    log.info(f"  Errors        : {sum(1 for r in results if r['status'] != 'ok')}")
    log.info(f"  JSON → {OUT_JSON}")
    log.info(f"  CSV  → {OUT_CSV}")
    log.info("=" * 60)

    # Print quick result table
    print("\n── RESULTS SUMMARY ──────────────────────────────────────")
    print(f"{'#':<3} {'Cafe':<35} {'Rating':<7} {'Reviews':<8} {'Hours':<6} {'Phone'}")
    print("-" * 80)
    for i, r in enumerate(results, 1):
        print(
            f"{i:<3} {r['business_name'][:34]:<35} "
            f"{str(r.get('google_rating') or 'N/A'):<7} "
            f"{str(len(r.get('reviews', []))):<8} "
            f"{str(len(r.get('opening_hours', {}))) + 'd':<6} "
            f"{r.get('phone') or 'N/A'}"
        )
    print("-" * 80)


if __name__ == "__main__":
    asyncio.run(main())