import sqlite3
import psycopg2
from datetime import datetime

# ----------------------------------
# CONFIG
# ----------------------------------
SQLITE_DB = "wf_data.db"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "wf_data",
    "user": "postgres",
    "password": "admin1234"
}

BATCH_SIZE = 1000

# ----------------------------------
# CONNECTIONS
# ----------------------------------
def get_sqlite_conn():
    return sqlite3.connect(SQLITE_DB)


def get_pg_conn():
    return psycopg2.connect(**POSTGRES_CONFIG)


# ----------------------------------
# NORMALIZERS
# ----------------------------------
def to_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def to_timestamp(value):
    """
    Convert ISO string to timestamp without timezone
    Example: 2026-01-24T05:00:44Z
    """
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", ""))
    except Exception:
        return None


# ----------------------------------
# ORDERS MIGRATION
# ----------------------------------
def migrate_orders():
    sqlite_conn = get_sqlite_conn()
    pg_conn = get_pg_conn()

    sqlite_cur = sqlite_conn.cursor()
    pg_cur = pg_conn.cursor()

    sqlite_cur.execute("SELECT * FROM orders")
    columns = [desc[0] for desc in sqlite_cur.description]

    insert_sql = """
        INSERT INTO orders (
            id, type, platinum, quantity, rank, charges,
            subtype, amberStars, cyanStars,
            createdAt, itemId, group_name, uid
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
    """

    count = 0

    while True:
        rows = sqlite_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            record = dict(zip(columns, row))

            cleaned = (
                record["id"],
                record["type"],
                to_int(record["platinum"]),
                to_int(record["quantity"]),
                to_int(record["rank"]),
                to_int(record["charges"]),
                record["subtype"],
                to_int(record["amberStars"]),
                to_int(record["cyanStars"]),
                to_timestamp(record["createdAt"]),
                record["itemId"],
                record["group_name"],
                record["uid"]
            )

            pg_cur.execute(insert_sql, cleaned)

        pg_conn.commit()
        count += len(rows)
        print(f"orders: inserted {count}")

    sqlite_cur.close()
    pg_cur.close()
    sqlite_conn.close()
    pg_conn.close()


# ----------------------------------
# USERS MIGRATION
# ----------------------------------
def migrate_users():
    sqlite_conn = get_sqlite_conn()
    pg_conn = get_pg_conn()

    sqlite_cur = sqlite_conn.cursor()
    pg_cur = pg_conn.cursor()

    sqlite_cur.execute("SELECT * FROM users_info")
    columns = [desc[0] for desc in sqlite_cur.description]

    insert_sql = """
        INSERT INTO users_info (
            id, reputation, platform, crossplay,
            locale, status, lastSeen,
            first_seen, active_days, last_active_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
    """

    count = 0

    while True:
        rows = sqlite_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            record = dict(zip(columns, row))

            cleaned = (
                record["id"],
                to_int(record["reputation"]),
                record["platform"],
                bool(record["crossplay"]) if record["crossplay"] is not None else None,
                record["locale"],
                record["status"],
                to_timestamp(record["lastSeen"]),
                to_timestamp(record["first_seen"]),
                to_int(record["active_days"]),
                record["last_active_date"]
            )

            pg_cur.execute(insert_sql, cleaned)

        pg_conn.commit()
        count += len(rows)
        print(f"users_info: inserted {count}")

    sqlite_cur.close()
    pg_cur.close()
    sqlite_conn.close()
    pg_conn.close()


# ----------------------------------
# MAIN
# ----------------------------------
def main():
    print("Starting clean SQLite → PostgreSQL migration")

    migrate_users()
    migrate_orders()

    print("Migration completed successfully")


if __name__ == "__main__":
    main()
