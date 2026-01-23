import csv
import json
import time
import requests

BASE_URL = "https://api.warframe.market/v2/item/{}"
HEADERS = {
    "accept": "application/json"
}

INPUT_CSV = ("output_slugs.csv")
OUTPUT_JSON = "items_data.json"

all_items = []

# Step 1: Read slugs from CSV
with open(INPUT_CSV, "r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for idx, row in enumerate(reader, start=1):
        slug = row.get("slug")
        if not slug:
            continue

        try:
            response = requests.get(
                BASE_URL.format(slug.strip()),
                headers=HEADERS,
                timeout=10
            )
            response.raise_for_status()

            all_items.append(response.json())
            print(f"[{idx}] Fetched: {slug}")

            # Respect API rate limits
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"Failed for slug '{slug}': {e}")

# Step 2: Save all item data into one JSON file
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_items, f, indent=2, ensure_ascii=False)

print(f"\nSaved {len(all_items)} items to {OUTPUT_JSON}")
