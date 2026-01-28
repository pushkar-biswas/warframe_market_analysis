import json
import psycopg2

# ---------- CONFIG ----------
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "wf_data",
    "user": "postgres",
    "password": "admin1234"
}

json_path = "items_data.json"

# ---------- LOAD JSON ----------
with open(json_path, "r", encoding="utf-8") as f:
    items = json.load(f)

# ---------- CONNECT POSTGRES ----------
conn = psycopg2.connect(**PG_CONFIG)
cur = conn.cursor()

# ---------- INSERT QUERY ----------
insert_sql = """
INSERT INTO item_master (
    item_id, slug, api_version,
    name, description, wiki_link,
    game_ref, rarity, max_rank, req_mastery_rank,
    trading_tax, ducats,
    tradable, bulk_tradable, vaulted, set_root,
    tags, set_parts, subtypes,
    icon, thumb, sub_icon
)
VALUES (
    %s,%s,%s,
    %s,%s,%s,
    %s,%s,%s,%s,
    %s,%s,
    %s,%s,%s,%s,
    %s,%s,%s,
    %s,%s,%s
)
ON CONFLICT (item_id) DO UPDATE SET
    slug = EXCLUDED.slug,
    api_version = EXCLUDED.api_version,
    name = EXCLUDED.name,
    description = EXCLUDED.description,
    wiki_link = EXCLUDED.wiki_link,
    game_ref = EXCLUDED.game_ref,
    rarity = EXCLUDED.rarity,
    max_rank = EXCLUDED.max_rank,
    req_mastery_rank = EXCLUDED.req_mastery_rank,
    trading_tax = EXCLUDED.trading_tax,
    ducats = EXCLUDED.ducats,
    tradable = EXCLUDED.tradable,
    bulk_tradable = EXCLUDED.bulk_tradable,
    vaulted = EXCLUDED.vaulted,
    set_root = EXCLUDED.set_root,
    tags = EXCLUDED.tags,
    set_parts = EXCLUDED.set_parts,
    subtypes = EXCLUDED.subtypes,
    icon = EXCLUDED.icon,
    thumb = EXCLUDED.thumb,
    sub_icon = EXCLUDED.sub_icon
;
"""

# ---------- INSERT DATA ----------
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

        data.get("tradable", False),
        data.get("bulkTradable", False),
        data.get("vaulted", False),
        data.get("setRoot", False),

        json.dumps(data.get("tags")),
        json.dumps(data.get("setParts")),
        json.dumps(data.get("subtypes")),

        i18n.get("icon"),
        i18n.get("thumb"),
        i18n.get("subIcon")
    ))

conn.commit()
cur.close()
conn.close()

print("PostgreSQL insert completed successfully.")
