import sqlite3

DB_NAME = "wf_data.db"


def create_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON;")

    # users_info table
    cursor.execute("""
        CREATE TABLE users_info (
            id TEXT PRIMARY KEY,           
            reputation INTEGER,
            platform TEXT,
            crossplay BOOLEAN,
            locale TEXT,
            status TEXT,
            lastSeen TEXT,
            first_seen TEXT,
            active_day INTEGER,
            last_active_date TEXT
        );

    """)

    # orders table
    cursor.execute("""
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
            createdAt TEXT,
            itemId TEXT,
            group_name TEXT,
            uid TEXT,
            FOREIGN KEY (uid) REFERENCES users_info(id)
        );
    """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_database()
    print("Database 'wf_data.db' and updated tables created successfully.")
