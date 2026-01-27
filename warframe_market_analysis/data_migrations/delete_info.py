import sqlite3

DB_PATH = "wf_data.db"
TABLES = ["orders", "users_info",'item_master']

def clear_tables(db_path, tables):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        for table in tables:
            cursor.execute(f"DELETE FROM {table};")
            print(f"Cleared table: {table}")
        conn.commit()
    except sqlite3.Error as e:
        print("SQLite error:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    clear_tables(DB_PATH, TABLES)