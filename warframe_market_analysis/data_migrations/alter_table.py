import json
import sqlite3

# File paths
db_path = "wf_data.db"
json_path = "temp_file/items_data.json"

# Reading json file and connecting to sqlite db
with open(json_path, "r", encoding="utf-8") as f:
    items = json.load(f)

conn = sqlite3.connect(db_path)
cur = conn.cursor()

# sqlite table structure
cur.execute("""
CREATE TABLE IF NOT EXISTS item_master (
    item_id TEXT PRIMARY KEY,
    slug TEXT UNIQUE,
    api_version TEXT,
    name TEXT,
    description TEXT,
    wiki_link TEXT,
    game_ref TEXT,
    rarity TEXT,
    max_rank INTEGER,
    req_mastery_rank INTEGER,
    trading_tax INTEGER,
    ducats INTEGER,
    tradable INTEGER,
    bulk_tradable INTEGER,
    vaulted INTEGER,
    set_root INTEGER,
    tags TEXT,
    set_parts TEXT,
    subtypes TEXT,
    icon TEXT,
    thumb TEXT,
    sub_icon TEXT
)
""")

# Insert query
insert_sql = """
INSERT OR REPLACE INTO item_master VALUES (
    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
)
"""

# data entry
for entry in items:
    data = entry.get("data", {})
    i18n = data.get("i18n", {}).get("en", {})

    cur.execute(insert_sql, (
        data.get("id"),
        data.get("slug"),
        entry.get("apiVersion"),

        i18n.get("name"),
        i18n.get("description"),
        i18n.get("wikiLink"),

        data.get("gameRef"),
        data.get("rarity"),
        data.get("maxRank"),
        data.get("reqMasteryRank"),

        data.get("tradingTax"),
        data.get("ducats"),

        int(data.get("tradable", False)),
        int(data.get("bulkTradable", False)),
        int(data.get("vaulted", False)),
        int(data.get("setRoot", False)),

        json.dumps(data.get("tags")),
        json.dumps(data.get("setParts")),
        json.dumps(data.get("subtypes")),

        i18n.get("icon"),
        i18n.get("thumb"),
        i18n.get("subIcon")
    ))

conn.commit()
conn.close()

print("Done")
