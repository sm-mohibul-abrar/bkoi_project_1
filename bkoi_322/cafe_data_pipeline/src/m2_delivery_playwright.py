import os
import glob
import json
import re
import pandas as pd
from datetime import datetime

# --- PATH CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.dirname(SCRIPT_DIR)
RAW_DATA_DIR = os.path.join(PIPELINE_DIR, "data", "raw", "google_maps")
PROCESSED_DATA_DIR = os.path.join(PIPELINE_DIR, "data", "processed")

os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

def get_latest_run_dir() -> str:
    """Finds the most recent scraper run directory in data/raw/google_maps/."""
    run_dirs = sorted(glob.glob(os.path.join(RAW_DATA_DIR, "run_*")))
    if not run_dirs:
        raise FileNotFoundError(f"No run directories found inside: {RAW_DATA_DIR}")
    return run_dirs[-1]

def clean_phone_number(phone: str) -> str:
    """Normalizes phone numbers to standard Bangladeshi international format (+880...)."""
    if not phone or phone == "N/A":
        return "N/A"
    
    digits = re.sub(r'\D', '', phone)
    if digits.startswith("880"):
        return f"+{digits}"
    elif digits.startswith("0"):
        return f"+88{digits}"
    elif len(digits) == 10:
        return f"+880{digits}"
    return phone

def clean_hours(hours_dict: dict) -> dict:
    """Standardizes opening and closing hours key formatting."""
    if not isinstance(hours_dict, dict) or "schedule" in hours_dict:
        return {"monday_to_sunday": "N/A"}
    
    cleaned = {}
    day_map = {
        "monday": "mon", "tuesday": "tue", "wednesday": "wed",
        "thursday": "thu", "friday": "fri", "saturday": "sat", "sunday": "sun"
    }
    for k, v in hours_dict.items():
        norm_key = day_map.get(k.lower().strip(), k.lower().strip())
        cleaned[norm_key] = v
    return cleaned

def process_and_export():
    latest_run = get_latest_run_dir()
    profiles_dir = os.path.join(latest_run, "profiles")
    profile_files = glob.glob(os.path.join(profiles_dir, "*.json"))

    print(f"Reading scraped profiles from: {profiles_dir}")
    print(f"Found {len(profile_files)} profile JSON files.\n")

    cleaned_records = []
    flat_rows = []

    for file_path in profile_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        contact_loc = data.get("contact_and_location", {})
        coords = contact_loc.get("coordinates", {})
        
        # Data Normalization
        clean_phone = clean_phone_number(contact_loc.get("phone", "N/A"))
        clean_opening_hours = clean_hours(data.get("opening_hours", {}))
        
        rating = data.get("google_rating")
        review_count = data.get("google_review_count", 0)
        reviews = data.get("reviews", [])

        # Master JSON Structure
        record = {
            "place_code": data.get("place_code"),
            "business_name": data.get("business_name"),
            "status": data.get("status", "OPERATIONAL"),
            "google_rating": rating if rating is not None else "N/A",
            "google_review_count": review_count,
            "contact": {
                "phone": clean_phone,
                "address": contact_loc.get("address_from_map", "N/A"),
                "latitude": coords.get("lat"),
                "longitude": coords.get("lng")
            },
            "menu_url": data.get("menu", "N/A"),
            "opening_hours": clean_opening_hours,
            "reviews_sample": reviews
        }
        cleaned_records.append(record)

        # Flattened CSV Row
        flat_rows.append({
            "place_code": data.get("place_code"),
            "business_name": data.get("business_name"),
            "google_rating": rating,
            "google_review_count": review_count,
            "phone": clean_phone,
            "address": contact_loc.get("address_from_map", "N/A"),
            "latitude": coords.get("lat"),
            "longitude": coords.get("lng"),
            "menu_url": data.get("menu", "N/A"),
            "hours_sat": clean_opening_hours.get("sat", "N/A"),
            "hours_sun": clean_opening_hours.get("sun", "N/A"),
            "hours_mon": clean_opening_hours.get("mon", "N/A"),
            "hours_tue": clean_opening_hours.get("tue", "N/A"),
            "hours_wed": clean_opening_hours.get("wed", "N/A"),
            "hours_thu": clean_opening_hours.get("thu", "N/A"),
            "hours_fri": clean_opening_hours.get("fri", "N/A"),
            "scraped_reviews_count": len(reviews)
        })

    # Save Processed JSON Master
    json_out_path = os.path.join(PROCESSED_DATA_DIR, "gulshan_top10_restaurants.json")
    with open(json_out_path, "w", encoding="utf-8") as f:
        json.dump(cleaned_records, f, indent=4, ensure_ascii=False)

    # Save Processed CSV Master
    csv_out_path = os.path.join(PROCESSED_DATA_DIR, "gulshan_top10_restaurants.csv")
    df = pd.DataFrame(flat_rows)
    df.to_csv(csv_out_path, index=False, encoding="utf-8-sig")

    print(f" Exports Successfully Created:")
    print(f"  • Master JSON: {json_out_path}")
    print(f"  • Master CSV:  {csv_out_path}")
    print(f"Total Operational Restaurants Processed: {len(cleaned_records)}")

if __name__ == "__main__":
    process_and_export()