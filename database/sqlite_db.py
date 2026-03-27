# task_A_llm_rag/database/sqlite_db.py
from __future__ import annotations

import sqlite3
from pathlib import Path


def get_db_path(project_root: Path) -> Path:
    return project_root / "database" / "cosmetovigilance_intermediate.db"


def create_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # =========================
    # Dimensions / CSV-based
    # =========================

    # companies.csv: id, name
    cur.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    """)

    # brands.csv: company_id, name  (no brand_id)
    # We keep exactly CSV columns.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS brands (
        company_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        PRIMARY KEY (company_id, name),
        FOREIGN KEY (company_id) REFERENCES companies(id)
    );
    """)

    # categories.csv: id, name, parent_id
    cur.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        parent_id INTEGER,
        FOREIGN KEY (parent_id) REFERENCES categories(id)
    );
    """)

    # chemicals.csv:
    # id, chemical_name, cas_id, cas_number, created_at, updated_at, date_removed
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chemicals (
        id INTEGER PRIMARY KEY,
        chemical_name TEXT NOT NULL,
        cas_id INTEGER,
        cas_number TEXT,
        created_at TEXT,
        updated_at TEXT,
        date_removed TEXT
    );
    """)

    # ingredients_clean.csv:
    # ingredient_id, ingredient_name, category, famous_name
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ingredients_clean (
        ingredient_id INTEGER PRIMARY KEY,
        ingredient_name TEXT NOT NULL,
        category TEXT,
        famous_name TEXT
    );
    """)

    # =========================
    # Products: two different CSVs => two tables
    # =========================

    # products.csv (other group)
    # id, product_name, csf_id, csf, company_id, primary_category_id, sub_category_id, brand_name
    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        product_name TEXT NOT NULL,
        csf_id REAL,
        csf TEXT,
        company_id INTEGER NOT NULL,
        primary_category_id INTEGER,
        sub_category_id INTEGER,
        brand_name TEXT,
        FOREIGN KEY (company_id) REFERENCES companies(id),
        FOREIGN KEY (primary_category_id) REFERENCES categories(id),
        FOREIGN KEY (sub_category_id) REFERENCES categories(id)
    );
    """)

    # cosmetics_clean.csv (your group)
    # product_id, category, brand, product_name, price, rank, ingredients_text, Combination, Dry, Normal, Oily, Sensitive
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cosmetics_clean (
        product_id INTEGER PRIMARY KEY,
        category TEXT,
        brand TEXT,
        product_name TEXT NOT NULL,
        price REAL,
        rank REAL,
        ingredients_text TEXT,
        Combination INTEGER,
        Dry INTEGER,
        Normal INTEGER,
        Oily INTEGER,
        Sensitive INTEGER
    );
    """)

    # product_chemicals.csv:
    # source_id, product_id, chemical_id, initial_date_reported, most_recent_date_reported, discontinued_date, chemical_count
    # product_id refers to products.id (other group)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS product_chemicals (
        source_id INTEGER,
        product_id INTEGER NOT NULL,
        chemical_id INTEGER NOT NULL,
        initial_date_reported TEXT,
        most_recent_date_reported TEXT,
        discontinued_date TEXT,
        chemical_count INTEGER,
        PRIMARY KEY (product_id, chemical_id),
        FOREIGN KEY (product_id) REFERENCES products(id),
        FOREIGN KEY (chemical_id) REFERENCES chemicals(id)
    );
    """)

    # cscpopendata_clean.csv:
    # brand, primary_category, sub_category, cas_number, chemical_name, incident_count, initial_date_reported, most_recent_date_reported
    cur.execute("""
    CREATE TABLE IF NOT EXISTS cscpopendata_clean (
        brand TEXT,
        primary_category TEXT,
        sub_category TEXT,
        cas_number TEXT,
        chemical_name TEXT,
        incident_count INTEGER,
        initial_date_reported TEXT,
        most_recent_date_reported TEXT
    );
    """)

    # =========================
    # Provenance (no extra cols in data tables)
    # =========================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS provenance_source (
        source_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_title TEXT NOT NULL,
        source_type TEXT NOT NULL,   -- "report" | "article" | "standard"
        publisher TEXT,
        year INTEGER,
        url TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS provenance_dataset (
        dataset_name TEXT PRIMARY KEY,           -- ex: "products", "cosmetics_clean"
        source_id INTEGER NOT NULL,
        generated_at TEXT NOT NULL DEFAULT (datetime('now')),
        note TEXT,
        FOREIGN KEY (source_id) REFERENCES provenance_source(source_id)
    );
    """)

    # Index (performance)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_company ON products(company_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pc_chemical ON product_chemicals(chemical_id);")

    conn.commit()


def init_db(project_root: Path) -> Path:
    db_path = get_db_path(project_root)
    conn = create_connection(db_path)
    try:
        create_tables(conn)
    finally:
        conn.close()
    return db_path
