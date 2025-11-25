# db.py
import sqlite3
from datetime import datetime
import os

def init_db(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            url TEXT,
            title TEXT,
            price TEXT,
            details TEXT,
            scraped_date TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_rows(path, rows):
    if not rows:
        return
    conn = sqlite3.connect(path)
    c = conn.cursor()
    for r in rows:
        c.execute("""
            INSERT INTO listings (source, url, title, price, details, scraped_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            r.get("source"),
            r.get("url"),
            r.get("title"),
            r.get("price"),
            r.get("details"),
            r.get("scraped_date")
        ))
    conn.commit()
    conn.close()
