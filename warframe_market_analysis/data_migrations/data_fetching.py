import io
import csv
import time
import requests
import psycopg2
from datetime import datetime, date, timezone


# -------------------------------------------------
# CONFIG
# -------------------------------------------------
API_URL = "https://api.warframe.market/v2/orders/recent"
FETCH_INTERVAL = 60

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "wf_data",
    "user": "postgres",
    "password": "admin1234"
}

EXPECTED_ORDER_KEYS = [
    "id", "type", "platinum", "quantity", "rank", "charges",
    "subtype", "amberStars", "cyanStars", "createdAt",
    "itemId", "group"
]

# -------------------------------------------------
# DB
# -------------------------------------------------
def get_pg_conn():
    return psycopg2.connect(**POSTGRES_CONFIG)


# -------------------------------------------------
# HELPERS
# -------------------------------------------------
def to_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def to_timestamp(v):
    if v is None:
        return None
    return datetime.fromisoformat(
        v.replace("Z", "+00:00")
    ).astimezone(timezone.utc)


# -------------------------------------------------
# API
# -------------------------------------------------
def api_call():
    headers = {"Language": "en"}
    resp = requests.get(API_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()["data"]


# -------------------------------------------------
# UPSERT USERS
# -------------------------------------------------
def upsert_user(cur, user):
    today = datetime.now(timezone.utc).date()

    cur.execute("""
        SELECT first_seen, active_days, last_active_date
        FROM users_info
        WHERE id = %s
    """, (user["id"],))

    row = cur.fetchone()

    if row is None:
        cur.execute("""
            INSERT INTO users_info (
                id, reputation, platform, crossplay,
                locale, status, lastSeen,
                first_seen, active_days, last_active_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            user["id"],
            to_int(user.get("reputation")),
            user.get("platform"),
            bool(user.get("crossplay")) if user.get("crossplay") is not None else None,
            user.get("locale"),
            user.get("status"),
            to_timestamp(user.get("lastSeen")),
            to_timestamp(user.get("lastSeen")),
            1,
            today
        ))
    else:
        first_seen, active_days, last_active_date = row

        if last_active_date != today:
            active_days += 1
            last_active_date = today

        cur.execute("""
            UPDATE users_info
            SET reputation=%s,
                platform=%s,
                crossplay=%s,
                locale=%s,
                status=%s,
                lastSeen=%s,
                active_days=%s,
                last_active_date=%s
            WHERE id=%s
        """, (
            to_int(user.get("reputation")),
            user.get("platform"),
            bool(user.get("crossplay")) if user.get("crossplay") is not None else None,
            user.get("locale"),
            user.get("status"),
            to_timestamp(user.get("lastSeen")),
            active_days,
            last_active_date,
            user["id"]
        ))


# -------------------------------------------------
# INSERT ORDERS
# -------------------------------------------------
def insert_order(cur, order, uid):
    cur.execute("""
        INSERT INTO orders (
            id, type, platinum, quantity, rank, charges,
            subtype, amberStars, cyanStars,
            created_at, item_id, group_name, uid
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, (
        order.get("id"),
        order.get("type"),
        to_int(order.get("platinum")),
        to_int(order.get("quantity")),
        to_int(order.get("rank")),
        to_int(order.get("charges")),
        order.get("subtype"),
        to_int(order.get("amberStars")),
        to_int(order.get("cyanStars")),
        to_timestamp(order.get("createdAt")),
        order.get("itemId"),
        order.get("group"),
        uid
    ))



def ensure_partition(cur):
    cur.execute("SELECT ensure_orders_partition();")



# -------------------------------------------------
# MAIN LOOP
# -------------------------------------------------
def main():
    conn = get_pg_conn()
    cur = conn.cursor()

    # ensure partitions exist (current + next month)
    ensure_partition(cur)
    conn.commit()

    while True:
        try:
            data = api_call()

            for entry in data:
                user = entry["user"]
                upsert_user(cur, user)
                insert_order(cur, entry, user["id"])

            conn.commit()
            print(f"Inserted {len(data)} orders")

        except Exception as e:
            conn.rollback()
            print("Error:", e)

        time.sleep(FETCH_INTERVAL)



if __name__ == "__main__":
    main()
