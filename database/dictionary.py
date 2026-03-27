# task_A_llm_rag/database/dictionary.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Dict

from database.sqlite_db import init_db, create_connection


def create_dictionary_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dd_table (
        table_name TEXT PRIMARY KEY,
        description TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dd_column (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        data_type TEXT,
        not_null INTEGER,
        is_pk INTEGER,
        default_value TEXT,
        description TEXT,
        PRIMARY KEY (table_name, column_name),
        FOREIGN KEY (table_name) REFERENCES dd_table(table_name)
    );
    """)

    conn.commit()


def list_user_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute("""
    SELECT name
    FROM sqlite_master
    WHERE type='table'
      AND name NOT LIKE 'sqlite_%'
      AND name NOT LIKE 'dd_%'
    ORDER BY name;
    """)
    return [r[0] for r in cur.fetchall()]


def get_columns(conn: sqlite3.Connection, table: str) -> List[Dict]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = []
    for cid, name, ctype, notnull, dflt, pk in cur.fetchall():
        cols.append({
            "name": name,
            "type": ctype,
            "notnull": int(notnull),
            "default": None if dflt is None else str(dflt),
            "pk": int(pk),
        })
    return cols


def seed_descriptions() -> dict:
    # Descriptions pro (tu peux les adapter)
    return {
        "companies": "Companies dimension from companies.csv.",
        "brands": "Brands dimension from brands.csv (composite key: company_id + name).",
        "categories": "Categories hierarchy from categories.csv.",
        "chemicals": "Chemical reference from chemicals.csv (CAS fields included).",
        "ingredients_clean": "Cleaned ingredient reference from ingredients_clean.csv.",
        "products": "Products dataset (other group) from products.csv.",
        "cosmetics_clean": "Products dataset (your group) from cosmetics_clean.csv.",
        "product_chemicals": "Product–chemical relationship from product_chemicals.csv.",
        "cscpopendata_clean": "Incident summary dataset from cscpopendata_clean.csv.",
        "provenance_source": "Generated realistic sources (reports/articles) used for traceability.",
        "provenance_dataset": "Links each dataset_name to a generated provenance_source.",
    }


def populate_dictionary(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    desc = seed_descriptions()

    tables = list_user_tables(conn)

    # Insert tables
    for t in tables:
        cur.execute(
            """
            INSERT INTO dd_table(table_name, description)
            VALUES (?, ?)
            ON CONFLICT(table_name) DO UPDATE SET description=excluded.description;
            """,
            (t, desc.get(t, "")),
        )

    # Insert columns
    for t in tables:
        cols = get_columns(conn, t)
        for c in cols:
            cur.execute(
                """
                INSERT INTO dd_column(
                    table_name, column_name, data_type, not_null, is_pk, default_value, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(table_name, column_name) DO UPDATE SET
                    data_type=excluded.data_type,
                    not_null=excluded.not_null,
                    is_pk=excluded.is_pk,
                    default_value=excluded.default_value,
                    description=excluded.description;
                """,
                (
                    t,
                    c["name"],
                    c["type"],
                    c["notnull"],
                    c["pk"],
                    c["default"],
                    "",  # you can enrich later
                ),
            )

    conn.commit()


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    db_path = init_db(root)
    conn = create_connection(db_path)
    try:
        create_dictionary_tables(conn)
        populate_dictionary(conn)
        print("[OK] Data Dictionary created and populated: dd_table, dd_column")
        print(f"DB: {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
