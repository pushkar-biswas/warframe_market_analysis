import time
import requests
import json
import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = "wf_data.db"

expected_key_values = [
    "id",
    "type",
    "platinum",
    "quantity",
    "rank",
    "charges",
    "subtype",
    "amberStars",
    "cyanStars",
    "createdAt",
    "itemId",
    "group",
]

# DB connection
def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def api_call():
    url = "https://api.warframe.market/v2/orders/recent"
    headers = {"Language": "en"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_order_fields(order):
    data = {}
    for key in expected_key_values:
        data[key] = order.get(key)
    return data


def process_raw_data(raw_data):
    orders = []
    users = {}

    for order in raw_data["data"]:
        user = order["user"]

        users[user["id"]] = (
            user["id"],
            user.get("reputation"),
            user.get("platform"),
            user.get("crossplay"),
            user.get("locale"),
            user.get("status"),
            user.get("lastSeen"),
        )

        order_data = extract_order_fields(order)
        order_data["uid"] = user["id"]

        orders.append((
            order_data.get("id"),
            order_data.get("type"),
            order_data.get("platinum"),
            order_data.get("quantity"),
            order_data.get("rank"),
            order_data.get("charges"),
            order_data.get("subtype"),
            order_data.get("amberStars"),
            order_data.get("cyanStars"),
            order_data.get("createdAt"),
            order_data.get("itemId"),
            order_data.get("group"),
            order_data.get("uid"),
        ))

    return users, orders


def insert_data(conn, users, orders):
    cursor = conn.cursor()

    today = datetime.utcnow().date().isoformat()

    for user in users.values():
        (
            user_id,
            reputation,
            platform,
            crossplay,
            locale,
            status,
            lastSeen
        ) = user

        cursor.execute("""
            SELECT first_seen, active_day, last_active_date
            FROM users_info
            WHERE id = ?
        """, (user_id,))

        row = cursor.fetchone()

        if row is None:
            # First time user seen
            cursor.execute("""
                INSERT INTO users_info (
                    id, reputation, platform, crossplay,
                    locale, status, lastSeen,
                    first_seen, active_day, last_active_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id,
                reputation,
                platform,
                crossplay,
                locale,
                status,
                lastSeen,
                lastSeen,      # first_seen
                1,             # active_day
                today          # last_active_date
            ))
        else:
            first_seen, active_day, last_active_date = row

            # Update activity only once per day
            if last_active_date != today:
                active_day += 1
                last_active_date = today

            cursor.execute("""
                UPDATE users_info
                SET
                    reputation = ?,
                    platform = ?,
                    crossplay = ?,
                    locale = ?,
                    status = ?,
                    lastSeen = ?,
                    active_day = ?,
                    last_active_date = ?
                WHERE id = ?
            """, (
                reputation,
                platform,
                crossplay,
                locale,
                status,
                lastSeen,
                active_day,
                last_active_date,
                user_id
            ))

    # Orders remain unchanged
    cursor.executemany("""
        INSERT OR IGNORE INTO orders (
            id, type, platinum, quantity, rank, charges, subtype,
            amberStars, cyanStars, createdAt, itemId, group_name, uid
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, orders)

    conn.commit()



def main():
    conn = get_db_connection()

    while True:
        try:
            raw_data = api_call()
            users, orders = process_raw_data(raw_data)
            insert_data(conn, users, orders)
            print(f"Inserted {len(orders)} orders, {len(users)} users")

        except Exception as e:
            print("Error:", e)

        time.sleep(60)


if __name__ == "__main__":
    main()
