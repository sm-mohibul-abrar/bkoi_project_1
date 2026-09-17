import os
import time
import json
import pandas as pd
from playwright.sync_api import sync_playwright

# --- PATH CONFIGURATION ---
CSV_PATH = "../../places_202609161553.csv"
FP_MENU_DIR = "../data/raw/food_delivery_review/foodpanda/menus"
FP_REV_DIR = "../data/raw/food_delivery_review/foodpanda/reviews"
PATHAO_MENU_DIR = "../data/raw/food_delivery_review/pathao/menus"
PATHAO_REV_DIR = "../data/raw/food_delivery_review/pathao/reviews"

for d in [FP_MENU_DIR, FP_REV_DIR, PATHAO_MENU_DIR, PATHAO_REV_DIR]:
    os.makedirs(d, exist_ok=True)

def scrape_foodpanda(page, place_code, cafe_name):
    print(f"[Module 2] Foodpanda lookup: {cafe_name}")
    search_url = f"https://www.google.com/search?q={cafe_name}+Gulshan+Foodpanda+Bangladesh"
    page.goto(search_url, wait_until="domcontentloaded")
    time.sleep(2)
    
    first_link = page.locator('a[href*="foodpanda.com.bd/restaurant/"]').first
    if first_link.count() == 0:
        print(f"  ✗ Foodpanda page not found for {cafe_name}")
        return
        
    first_link.click()
    time.sleep(4)
    
    # Extract Foodpanda Menu
    items = []
    menu_elements = page.locator('div[data-qa="menu-item"], .dish-card').all()
    for el in menu_elements[:15]:
        try:
            title = el.locator('.dish-name, [data-qa="menu-item-name"]').inner_text()
            price = el.locator('.price, [data-qa="menu-item-price"]').inner_text()
            items.append({"item": title.strip(), "price": price.strip()})
        except Exception:
            continue

    # Extract Foodpanda Reviews
    reviews = []
    try:
        rev_btn = page.locator('button[data-qa="restaurant-info-button"], .vendor-info-button').first
        if rev_btn.is_visible():
            rev_btn.click()
            time.sleep(2)
            rev_elements = page.locator('.review-card, [data-qa="review-item"]').all()
            for r in rev_elements[:10]:
                try:
                    text = r.locator('.review-text, .comment').inner_text()
                    reviews.append({"review_text": text.strip()})
                except Exception:
                    continue
    except Exception:
        pass

    with open(f"{FP_MENU_DIR}/{place_code}_fp_menu.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=4, ensure_ascii=False)
        
    with open(f"{FP_REV_DIR}/{place_code}_fp_reviews.json", "w", encoding="utf-8") as f:
        json.dump(reviews, f, indent=4, ensure_ascii=False)
        
    print(f"  ✓ Foodpanda data saved for {place_code}")

def run_delivery():
    df = pd.read_csv(CSV_PATH)
    cafes = df[(df['area'] == 'Gulshan') & (df['sub_type'] == 'Cafe')].sort_values(by='popularity_ranking', ascending=False).head(10)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(viewport={"width": 1280, "height": 900})
        page = ctx.new_page()

        for _, row in cafes.iterrows():
            scrape_foodpanda(page, row['place_code'], row['business_name'])

        browser.close()

if __name__ == "__main__":
    run_delivery()