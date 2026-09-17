import time
import re
import json
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ══════════════════════════════════════════════════════════════════════
#  CONFIG — Edit only this block to scrape a different brand/business
# ══════════════════════════════════════════════════════════════════════

TARGET_BRAND  = "Agora"
BRAND_ALIASES = ["আগোরা"]
NAME_KEYWORDS = ["agora"]

QUERIES = [
    # Broad national queries
    "Agora Supershop Bangladesh",
    "Agora Supermarket Bangladesh",

    # Dhaka zones — Google caps ~20 results per search so split by area
    "Agora Supershop Mirpur Dhaka",
    "Agora Supershop Uttara Dhaka",
    "Agora Supershop Gulshan Dhaka",
    "Agora Supershop Dhanmondi Dhaka",
    "Agora Supershop Mohammadpur Dhaka",
    "Agora Supershop Shyamoli Dhaka",
    "Agora Supershop Banani Dhaka",
    "Agora Supershop Bashundhara Dhaka",
    "Agora Supershop Malibagh Dhaka",
    "Agora Supershop Rampura Dhaka",
    "Agora Supershop Pallabi Dhaka",
    "Agora Supershop Badda Dhaka",
    "Agora Supershop Mohakhali Dhaka",
    "Agora Supershop Sheorapara Dhaka",
    "Agora Supershop Narayanganj",
    "Agora Supershop Gazipur",

    # Outside Dhaka
    "Agora Supershop Chittagong",
    "Agora Supermarket Chittagong",
    "Agora Supershop Sylhet",
    "Agora Supershop Rajshahi",
    "Agora Supershop Khulna",
    "Agora Supershop Comilla",
    "Agora Supershop Bogura",
    "Agora Supershop Mymensingh",
]

MAX_RESULTS_PER_QUERY = 20
FEED_SCROLL_ROUNDS    = 15
FEED_SCROLL_PAUSE     = 2.0

_TS          = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_JSON  = f"agora_{_TS}.json"
OUTPUT_CSV   = f"agora_{_TS}.csv"
OUTPUT_EXCEL = f"agora_{_TS}.xlsx"

# Known Google Maps "About" section headers (covers supermarkets,
# restaurants, cafes, retail — extend this list for other business types)
KNOWN_HEADERS = [
    "Accessibility", "Service options", "Highlights", "Offerings",
    "Amenities", "Atmosphere", "Crowd", "Planning", "Payments",
    "Children", "Pets", "Parking", "Recycling", "Getting here",
    "From the business", "Health & safety", "Dining options",
    "Popular for", "Lodging options", "Health and safety",
]

# ══════════════════════════════════════════════════════════════════════

JUNK = {"ফলাফল", "results", "n/a", "", "search results", "google maps"}

# Lines that must NEVER be treated as feature items even if they follow
# a valid section header (UI chrome, not real data)
SKIP_ITEMS = {
    "see more", "see less", "overview", "reviews", "about", "menu",
    "directions", "website", "save", "share", "send to phone",
    "nearby", "photos", "updates", "add photo", "suggest an edit",
    "share place", "save in your lists", "call", "claim this business",
}


# ── Basic field extractors ──────────────────────────────────────────

def is_target(name: str) -> bool:
    if not name or name.strip().lower() in JUNK:
        return False
    nl = name.strip().lower()
    if any(alias in name for alias in BRAND_ALIASES):
        return True
    if TARGET_BRAND.lower() not in nl:
        return False
    return any(kw in nl for kw in NAME_KEYWORDS)


def get_coords(url: str):
    m = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url)
    if m:
        return float(m.group(1)), float(m.group(2))
    la = re.search(r'!3d(-?\d+\.\d+)', url)
    lo = re.search(r'!4d(-?\d+\.\d+)', url)
    if la and lo:
        return float(la.group(1)), float(lo.group(1))
    m2 = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
    if m2:
        return float(m2.group(1)), float(m2.group(2))
    return None, None


def get_name(page) -> str:
    for sel in [
        'div[role="main"] h1.DUwDR',
        'div[role="main"] h1.fontHeadlineLarge',
        'div[role="main"] h1.lMbq3e',
        'div[role="main"] h1',
    ]:
        try:
            page.wait_for_selector(sel, timeout=6000)
            for el in page.locator(sel).all():
                if el.is_visible():
                    txt = el.inner_text().strip()
                    if txt and txt.lower() not in JUNK:
                        return txt
        except Exception:
            continue
    return "N/A"


def get_address(page) -> str:
    try:
        el = page.locator('button[data-item-id="address"]').first
        if el.is_visible(timeout=3000):
            raw = el.inner_text()
            return re.sub(r'\s+', ' ', raw.replace("\n", ", ")).strip()
    except Exception:
        pass
    return "N/A"


def get_phone(page) -> str:
    for sel in [
        'button[data-item-id^="phone:tel"]',
        'button[data-item-id*="phone"]',
        '[data-item-id*="phone"]',
        'a[href^="tel:"]',
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                txt = re.sub(r'[^\d\s\+\-\(\)]+', '', el.inner_text()).strip()
                if txt:
                    return txt
                href = el.get_attribute("href") or ""
                if href.startswith("tel:"):
                    return href.replace("tel:", "").strip()
        except Exception:
            continue
    return "N/A"


# ── About tab extraction ────────────────────────────────────────────

def click_about_tab(page) -> bool:
    """Find and click the About tab. Returns True if clicked."""
    try:
        page.wait_for_selector('div[role="main"]', timeout=8000)
        time.sleep(1)
    except Exception:
        pass

    for sel in [
        'button[role="tab"]:has-text("About")',
        '[role="tab"]:has-text("About")',
        'button[role="tab"][aria-label*="About"]',
        '[role="tab"][aria-label*="About"]',
    ]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=2000):
                el.click()
                time.sleep(2.5)
                return True
        except Exception:
            continue

    # JS text-match fallback
    try:
        clicked = page.evaluate("""
        () => {
            for (const el of document.querySelectorAll('[role="tab"], button')) {
                if ((el.innerText || '').trim() === 'About') {
                    el.click();
                    return true;
                }
            }
            return false;
        }
        """)
        if clicked:
            time.sleep(2.5)
            return True
    except Exception:
        pass

    return False


def get_panel_visible_text(page) -> str:
    """
    Return the VISIBLE text of the main detail panel.
    innerText (unlike textContent) only includes text that is actually
    rendered on screen — hidden tab panels (Overview, Reviews) are
    automatically excluded. This is what makes the parsing reliable:
    only the currently-active About tab's content comes through.
    """
    try:
        return page.evaluate("""
        () => {
            const main = document.querySelector('div[role="main"]');
            return main ? main.innerText : '';
        }
        """)
    except Exception:
        return ""


def scroll_about_panel(page):
    """Scroll whichever inner container is actually scrollable."""
    try:
        page.evaluate("""
        () => {
            const main = document.querySelector('div[role="main"]');
            if (!main) return;
            let target = main;
            for (const el of main.querySelectorAll('*')) {
                const s = getComputedStyle(el);
                if ((s.overflowY === 'auto' || s.overflowY === 'scroll')
                    && el.scrollHeight > el.clientHeight) {
                    target = el;
                }
            }
            target.scrollBy(0, 450);
        }
        """)
    except Exception:
        pass


def parse_about_text(text: str) -> dict:
    """
    Parse the About panel's visible text into a section dictionary.

    Handles three real-world quirks of Google's rendered text:
      1. Exact header line: "Accessibility"
      2. Header glued to its first item with no line break:
         "AccessibilityWheelchair-accessible car park"
      3. Multiple items packed onto one line (grid layout renders
         two columns as one text line): "No-contact delivery   Delivery"
    """
    header_lookup = {h.lower(): h for h in KNOWN_HEADERS}
    # Sort longest-first so "Health & safety" matches before "Health"
    headers_sorted = sorted(KNOWN_HEADERS, key=len, reverse=True)

    raw_lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Pre-process: split any line containing 2+ consecutive spaces/tabs
    # into separate candidate lines (handles grid-packed items)
    lines = []
    for line in raw_lines:
        parts = re.split(r'\s{2,}|\t+', line)
        lines.extend(p.strip() for p in parts if p.strip())

    result = {}
    current = None

    for line in lines:
        low = line.lower()

        # ── Case 1: exact header match ────────────────────────────────
        if low in header_lookup:
            current = header_lookup[low]
            result.setdefault(current, [])
            continue

        # ── Case 2: header glued to first item (no line break) ─────────
        matched_header = None
        remainder = None
        for h in headers_sorted:
            if low.startswith(h.lower()) and len(line) > len(h):
                matched_header = h
                remainder = line[len(h):].strip()
                break
        if matched_header:
            current = matched_header
            result.setdefault(current, [])
            if remainder and remainder.lower() not in SKIP_ITEMS and remainder.lower() not in JUNK:
                if remainder not in result[current]:
                    result[current].append(remainder)
            continue

        # No section started yet — skip (title text, nav chrome, etc.)
        if current is None:
            continue

        if low in SKIP_ITEMS or low in JUNK:
            continue

        # Skip long lines (descriptions/reviews, not feature tags)
        if len(line) > 80:
            continue

        if line not in result[current]:
            result[current].append(line)

    return {k: v for k, v in result.items() if v}


def get_about(page) -> dict:
    """
    Click About tab (if present) → scroll to reveal all sections →
    parse visible text into a clean section dictionary.
    Returns {} if there's no About tab, with no hang.
    """
    if not click_about_tab(page):
        print("       ℹ  No About tab")
        return {}

    # Poll briefly for the panel content to actually update after the click
    about_text = ""
    for _ in range(5):
        about_text = get_panel_visible_text(page)
        low = about_text.lower()
        if any(h.lower() in low for h in KNOWN_HEADERS):
            break
        time.sleep(1)

    # Scroll down in steps, merging any newly-revealed text each time
    full_text_lines = []
    seen_lines = set()

    def merge(txt):
        for line in txt.split("\n"):
            line = line.strip()
            if line and line not in seen_lines:
                seen_lines.add(line)
                full_text_lines.append(line)

    merge(about_text)

    for _ in range(10):
        scroll_about_panel(page)
        time.sleep(0.7)
        merge(get_panel_visible_text(page))

    combined_text = "\n".join(full_text_lines)
    metadata = parse_about_text(combined_text)

    if not metadata:
        print("       ⚠  About tab opened but no known sections matched")
        # Save the raw captured text so the failure can be diagnosed
        # from real data instead of guessing at the DOM structure blindly.
        try:
            import os
            debug_dir = "/mnt/user-data/outputs/about_debug"
            os.makedirs(debug_dir, exist_ok=True)
            safe_name = re.sub(r'[^A-Za-z0-9]+', '_', get_name(page))[:60]
            debug_path = f"{debug_dir}/{safe_name}.txt"
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(combined_text)
            print(f"       📝 Raw panel text saved to: {debug_path}")
        except Exception as e:
            print(f"       ⚠  Could not save debug file: {e}")
    else:
        print(f"       ✓  About data: {json.dumps(metadata, ensure_ascii=False, indent=2)}")

    # Return to Overview tab
    try:
        for sel in [
            'button[role="tab"]:has-text("Overview")',
            '[role="tab"]:has-text("Overview")',
            'button[role="tab"][aria-label*="Overview"]',
        ]:
            ov = page.locator(sel).first
            if ov.is_visible(timeout=1500):
                ov.click()
                time.sleep(1)
                break
    except Exception:
        pass

    return metadata


# ── Per-place scraping (fresh tab per place — no about:blank) ──────────

def scrape_one(ctx, url: str, query: str, results: list, seen: set) -> bool:
    if url in seen:
        return False

    tab = None
    try:
        tab = ctx.new_page()
        tab.set_default_timeout(30000)
        tab.goto(url, wait_until="domcontentloaded")
        time.sleep(3.5)

        if "about:blank" in tab.url or "/maps/place/" not in tab.url:
            print(f"  ⚠  Unexpected URL: {tab.url[:70]}")
            return False

        name = get_name(tab)
        if not is_target(name):
            print(f"  ✗  Skip: '{name}'")
            return False

        address  = get_address(tab)
        phone    = get_phone(tab)
        lat, lng = get_coords(tab.url)
        if lat is None:
            time.sleep(2)
            lat, lng = get_coords(tab.url)

        about = get_about(tab)

        seen.add(url)
        results.append({
            "name"          : name,
            "address"       : address,
            "phone"         : phone,
            "latitude"      : lat,
            "longitude"     : lng,
            "about_metadata": about if about else "N/A",
        })

        print(
            f"  ✓  {name}\n"
            f"     📍 {lat}, {lng}\n"
            f"     🏠 {address[:72]}{'…' if len(address) > 72 else ''}\n"
            f"     📞 {phone}\n"
            f"     📋 About: {json.dumps(about, ensure_ascii=False) if about else '(no About tab)'}"
        )
        return True

    except PWTimeout:
        print(f"  ⚠  Timeout: {url[:70]}")
        return False
    except Exception as e:
        print(f"  ⚠  Error: {e}")
        return False
    finally:
        if tab:
            try:
                tab.close()
            except Exception:
                pass


# ── Main scraper ─────────────────────────────────────────────────────

def scrape(queries: list, max_per_query: int) -> list:
    results   = []
    seen_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=["--lang=en-US", "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )

        # Feed tab stays on results page the entire run — never navigated away
        feed = ctx.new_page()
        feed.set_default_timeout(30000)

        for query in queries:
            print(f"\n{'═'*65}")
            print(f"  🔍  {query}")
            print(f"{'═'*65}")

            feed.goto("https://www.google.com/maps?hl=en",
                      wait_until="domcontentloaded")
            time.sleep(2)

            sb = feed.locator("input#searchboxinput, input[name='q']").first
            sb.fill(query)
            feed.keyboard.press("Enter")
            time.sleep(5)

            feed_panel = feed.locator('div[role="feed"]')

            if feed_panel.is_visible(timeout=6000):
                print(f"[+] Scrolling feed ({FEED_SCROLL_ROUNDS} rounds)…")
                for _ in range(FEED_SCROLL_ROUNDS):
                    feed_panel.evaluate("el => el.scrollBy(0, 1200)")
                    time.sleep(FEED_SCROLL_PAUSE)

                cards = feed.locator('a[href*="/maps/place/"]').all()
                print(f"[+] {len(cards)} cards found. Collecting URLs…")

                place_urls = []
                seen_hrefs = set()
                for card in cards:
                    try:
                        href = (card.get_attribute("href") or "").strip()
                        key  = href.split("?")[0]
                        if key and key not in seen_hrefs and "/maps/place/" in key:
                            seen_hrefs.add(key)
                            place_urls.append(href)
                    except Exception:
                        continue

                print(f"[+] {len(place_urls)} unique URLs. Scraping each…\n")

                count = 0
                for url in place_urls:
                    if count >= max_per_query:
                        break
                    if scrape_one(ctx, url, query, results, seen_urls):
                        count += 1
                    time.sleep(0.5)

                print(f"\n[+] Query done — {count} outlets added.")

            else:
                print("[!] Single result page detected.")
                if "/maps/place/" in feed.url:
                    scrape_one(ctx, feed.url, query, results, seen_urls)

        browser.close()
    return results


def save(data: list):
    if not data:
        print("\n[!] No data. Nothing saved.")
        return

    df = pd.DataFrame(data)
    df["about_metadata"] = df["about_metadata"].apply(
        lambda x: json.dumps(x, ensure_ascii=False, indent=2)
        if isinstance(x, dict) else x
    )

    before = len(df)
    df.drop_duplicates(subset=["name", "address"], inplace=True)
    if before - len(df):
        print(f"[i] Removed {before - len(df)} duplicates.")

    df.to_json(OUTPUT_JSON,  orient="records", indent=4, force_ascii=False)
    df.to_csv(OUTPUT_CSV,    index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_EXCEL, index=False)

    print(f"\n{'═'*65}")
    print(f"  ✔  {len(df)} outlets saved:")
    print(f"     • {OUTPUT_JSON}")
    print(f"     • {OUTPUT_CSV}")
    print(f"     • {OUTPUT_EXCEL}")
    print(f"{'═'*65}")


if __name__ == "__main__":
    data = scrape(QUERIES, MAX_RESULTS_PER_QUERY)
    save(data)