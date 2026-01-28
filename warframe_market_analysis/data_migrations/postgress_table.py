import psycopg2

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "wf_data",
    "user": "postgres",
    "password": "admin1234"
}

def get_conn():
    return psycopg2.connect(**PG_CONFIG)

def create_and_alter_tables():
    conn = get_conn()
    cur = conn.cursor()

    # =========================
    # users_info
    # =========================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users_info (
            id TEXT PRIMARY KEY,
            reputation INTEGER,
            platform TEXT,
            crossplay BOOLEAN,
            locale TEXT,
            status TEXT,
            lastSeen TIMESTAMP,
            first_seen TIMESTAMP,
            active_days INTEGER DEFAULT 0,
            last_active_date DATE
        );
    """)

    # =========================
    # orders
    # =========================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            type TEXT,
            platinum INTEGER,
            quantity INTEGER,
            rank INTEGER,
            charges INTEGER,
            subtype TEXT,
            amberStars INTEGER,
            cyanStars INTEGER,
            createdAt TIMESTAMP,
            itemId TEXT,
            group_name TEXT,
            uid TEXT REFERENCES users_info(id)
        );
    """)

    # =========================
    # item_master
    # =========================
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
            tradable BOOLEAN,
            bulk_tradable BOOLEAN,
            vaulted BOOLEAN,
            set_root BOOLEAN,
            tags JSONB,
            set_parts JSONB,
            subtypes JSONB,
            icon TEXT,
            thumb TEXT,
            sub_icon TEXT
        );
    """)

    # =========================
    # SAFE ALTER TABLE SECTION
    # (runs without breaking existing DB)
    # =========================
    alter_statements = [
        # users_info future-proofing
        "ALTER TABLE users_info ADD COLUMN IF NOT EXISTS active_day INTEGER DEFAULT 0;",
        "ALTER TABLE users_info ADD COLUMN IF NOT EXISTS last_active_date DATE;",

        # orders indexes (analytics)
        "CREATE INDEX IF NOT EXISTS idx_orders_createdat ON orders(createdAt);",
        "CREATE INDEX IF NOT EXISTS idx_orders_itemid ON orders(itemId);",
        "CREATE INDEX IF NOT EXISTS idx_orders_uid ON orders(uid);",

        # item_master indexes
        "CREATE INDEX IF NOT EXISTS idx_item_master_slug ON item_master(slug);",
        "CREATE INDEX IF NOT EXISTS idx_item_master_tags ON item_master USING GIN(tags);"
    ]

    for stmt in alter_statements:
        cur.execute(stmt)

    conn.commit()
    cur.close()
    conn.close()

    print("PostgreSQL tables created / altered successfully.")

if __name__ == "__main__":
    create_and_alter_tables()
