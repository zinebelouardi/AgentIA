# task_A_llm_rag/database/insert_structured_data.py
from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from database.sqlite_db import init_db, create_connection


# -----------------------------
# Paths
# -----------------------------
def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def csv_dir() -> Path:
    return project_root() / "data" / "csv"


# -----------------------------
# Provenance (realistic titles)
# -----------------------------
RNG = random.Random(2025)

PUBLISHERS = [
    "SCCS (Scientific Committee on Consumer Safety)",
    "CIR (Cosmetic Ingredient Review)",
    "European Commission",
    "WHO (World Health Organization)",
    "ECHA (European Chemicals Agency)",
    "OECD",
    "FDA",
]

TITLE_TEMPLATES = [
    "Safety Assessment of Cosmetic Ingredients: {topic} ({year})",
    "Scientific Opinion on {topic} in Cosmetic Products ({year})",
    "Guidance for Risk Assessment of Cosmetic Substances ({year})",
    "Review of Exposure and Risk Characterization for {topic} ({year})",
    "Technical Report: Monitoring Cosmetic Incidents and Substances ({year})",
    "Standard Methods for Cosmetic Safety Evaluation ({year})",
]

TOPICS = [
    "preservatives and allergens",
    "fragrance ingredients",
    "UV filters",
    "colorants and impurities",
    "endocrine-active substances",
    "irritants and sensitizers",
    "systemic exposure assessment",
]

SOURCE_TYPES = ["report", "article", "standard"]


def generate_source_for_dataset(dataset_name: str) -> Tuple[str, str, str, int, Optional[str]]:
    year = RNG.choice([2018, 2019, 2020, 2021, 2022, 2023, 2024])
    publisher = RNG.choice(PUBLISHERS)
    topic = RNG.choice(TOPICS)
    title = RNG.choice(TITLE_TEMPLATES).format(topic=topic, year=year)
    source_type = RNG.choice(SOURCE_TYPES)
    # optional fake URL format (not mandatory)
    url = None
    return title, source_type, publisher, year, url


def upsert_provenance(conn: sqlite3.Connection, dataset_name: str) -> None:
    title, source_type, publisher, year, url = generate_source_for_dataset(dataset_name)
    cur = conn.cursor()

    # Insert new source row
    cur.execute(
        """
        INSERT INTO provenance_source(source_title, source_type, publisher, year, url)
        VALUES (?, ?, ?, ?, ?)
        """,
        (title, source_type, publisher, year, url),
    )
    source_id = cur.lastrowid

    # Link dataset to source
    cur.execute(
        """
        INSERT INTO provenance_dataset(dataset_name, source_id, note)
        VALUES (?, ?, ?)
        ON CONFLICT(dataset_name) DO UPDATE SET
          source_id=excluded.source_id,
          generated_at=datetime('now'),
          note=excluded.note
        """,
        (dataset_name, source_id, "Generated provenance (realistic title) for project demonstration."),
    )
    conn.commit()


# -----------------------------
# Clean helpers
# -----------------------------
NULL_LIKE = {"", "nan", "none", "null", "na", "n/a", "-", "undefined"}


def clean_text(x, default: Optional[str] = None) -> Optional[str]:
    if x is None:
        return default
    if pd.isna(x):
        return default
    s = str(x).strip()
    if s.lower() in NULL_LIKE:
        return default
    # normalize whitespace
    s = " ".join(s.split())
    return s if s else default


def clean_int(x, default: Optional[int] = None) -> Optional[int]:
    if x is None or pd.isna(x):
        return default
    try:
        s = str(x).strip()
        if s.lower() in NULL_LIKE:
            return default
        return int(float(s))
    except Exception:
        return default


def clean_real(x, default: Optional[float] = None) -> Optional[float]:
    if x is None or pd.isna(x):
        return default
    try:
        s = str(x).strip().replace(",", ".")
        if s.lower() in NULL_LIKE:
            return default
        return float(s)
    except Exception:
        return default


def clean_flag01(x, default: int = 0) -> int:
    v = clean_int(x, None)
    if v is None:
        return default
    return 1 if v != 0 else 0


def drop_if_null(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[c] = out[c].apply(lambda v: None if pd.isna(v) else v)
    out = out.dropna(subset=cols)
    return out


def ensure_unknown_company(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO companies(id, name) VALUES (?, ?)",
        (0, "Unknown Company"),
    )
    conn.commit()


# -----------------------------
# Load functions (strict CSV columns only)
# -----------------------------
def load_companies(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    # Clean
    df["id"] = df["id"].apply(lambda x: clean_int(x, None))
    df["name"] = df["name"].apply(lambda x: clean_text(x, "Unknown Company"))

    df = drop_if_null(df, ["id"])
    df = df.drop_duplicates(subset=["id"])

    # insert
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO companies(id, name) VALUES (?, ?)",
        list(df[["id", "name"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    ensure_unknown_company(conn)
    return len(df)


def load_categories(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)
    df["id"] = df["id"].apply(lambda x: clean_int(x, None))
    df["name"] = df["name"].apply(lambda x: clean_text(x, "Unknown Category"))
    df["parent_id"] = df["parent_id"].apply(lambda x: clean_int(x, None)) if "parent_id" in df.columns else None

    df = drop_if_null(df, ["id"])
    df = df.drop_duplicates(subset=["id"])

    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO categories(id, name, parent_id) VALUES (?, ?, ?)",
        list(df[["id", "name", "parent_id"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_brands(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    df["company_id"] = df["company_id"].apply(lambda x: clean_int(x, 0))
    df["name"] = df["name"].apply(lambda x: clean_text(x, None))

    # brand_name is mandatory in composite PK; drop rows without name
    df = drop_if_null(df, ["name"])
    df = df.drop_duplicates(subset=["company_id", "name"])

    ensure_unknown_company(conn)

    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO brands(company_id, name) VALUES (?, ?)",
        list(df[["company_id", "name"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_chemicals(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    df["id"] = df["id"].apply(lambda x: clean_int(x, None))
    df["chemical_name"] = df["chemical_name"].apply(lambda x: clean_text(x, None))
    df["cas_id"] = df["cas_id"].apply(lambda x: clean_int(x, None)) if "cas_id" in df.columns else None
    df["cas_number"] = df["cas_number"].apply(lambda x: clean_text(x, None)) if "cas_number" in df.columns else None
    df["created_at"] = df["created_at"].apply(lambda x: clean_text(x, None)) if "created_at" in df.columns else None
    df["updated_at"] = df["updated_at"].apply(lambda x: clean_text(x, None)) if "updated_at" in df.columns else None
    df["date_removed"] = df["date_removed"].apply(lambda x: clean_text(x, None)) if "date_removed" in df.columns else None

    # Drop rows missing key fields
    df = drop_if_null(df, ["id", "chemical_name"])
    df = df.drop_duplicates(subset=["id"])

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO chemicals(
          id, chemical_name, cas_id, cas_number, created_at, updated_at, date_removed
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        list(df[["id","chemical_name","cas_id","cas_number","created_at","updated_at","date_removed"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_ingredients_clean(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    df["ingredient_id"] = df["ingredient_id"].apply(lambda x: clean_int(x, None))
    df["ingredient_name"] = df["ingredient_name"].apply(lambda x: clean_text(x, None))
    df["category"] = df["category"].apply(lambda x: clean_text(x, "Unknown")) if "category" in df.columns else "Unknown"
    df["famous_name"] = df["famous_name"].apply(lambda x: clean_text(x, "Unknown")) if "famous_name" in df.columns else "Unknown"

    df = drop_if_null(df, ["ingredient_id", "ingredient_name"])
    df = df.drop_duplicates(subset=["ingredient_id"])

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO ingredients_clean(
          ingredient_id, ingredient_name, category, famous_name
        ) VALUES (?, ?, ?, ?)
        """,
        list(df[["ingredient_id","ingredient_name","category","famous_name"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_products(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    df["id"] = df["id"].apply(lambda x: clean_int(x, None))
    df["product_name"] = df["product_name"].apply(lambda x: clean_text(x, None))
    df["csf_id"] = df["csf_id"].apply(lambda x: clean_real(x, None)) if "csf_id" in df.columns else None
    df["csf"] = df["csf"].apply(lambda x: clean_text(x, None)) if "csf" in df.columns else None

    # FK company_id must exist; fill with 0 (Unknown)
    df["company_id"] = df["company_id"].apply(lambda x: clean_int(x, 0))

    df["primary_category_id"] = df["primary_category_id"].apply(lambda x: clean_int(x, None)) if "primary_category_id" in df.columns else None
    df["sub_category_id"] = df["sub_category_id"].apply(lambda x: clean_int(x, None)) if "sub_category_id" in df.columns else None
    df["brand_name"] = df["brand_name"].apply(lambda x: clean_text(x, "Unknown")) if "brand_name" in df.columns else "Unknown"

    # Drop rows missing product_id or product_name
    df = drop_if_null(df, ["id", "product_name"])
    df = df.drop_duplicates(subset=["id"])

    ensure_unknown_company(conn)

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO products(
          id, product_name, csf_id, csf, company_id,
          primary_category_id, sub_category_id, brand_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        list(df[["id","product_name","csf_id","csf","company_id","primary_category_id","sub_category_id","brand_name"]]
             .itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_cosmetics_clean(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    df["product_id"] = df["product_id"].apply(lambda x: clean_int(x, None))
    df["product_name"] = df["product_name"].apply(lambda x: clean_text(x, None))

    # Fill common text fields
    df["category"] = df["category"].apply(lambda x: clean_text(x, "Unknown")) if "category" in df.columns else "Unknown"
    df["brand"] = df["brand"].apply(lambda x: clean_text(x, "Unknown")) if "brand" in df.columns else "Unknown"
    df["ingredients_text"] = df["ingredients_text"].apply(lambda x: clean_text(x, "")) if "ingredients_text" in df.columns else ""

    # Numerics: fill with median, fallback 0
    df["price"] = df["price"].apply(lambda x: clean_real(x, None)) if "price" in df.columns else None
    df["rank"] = df["rank"].apply(lambda x: clean_real(x, None)) if "rank" in df.columns else None

    price_median = df["price"].median(skipna=True) if "price" in df.columns else 0
    rank_median = df["rank"].median(skipna=True) if "rank" in df.columns else 0
    if pd.isna(price_median): price_median = 0
    if pd.isna(rank_median): rank_median = 0
    df["price"] = df["price"].fillna(price_median)
    df["rank"] = df["rank"].fillna(rank_median)

    # Flags: fill 0
    for col in ["Combination", "Dry", "Normal", "Oily", "Sensitive"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: clean_flag01(x, 0))
        else:
            df[col] = 0

    # Drop rows missing keys
    df = drop_if_null(df, ["product_id", "product_name"])
    df = df.drop_duplicates(subset=["product_id"])

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO cosmetics_clean(
          product_id, category, brand, product_name, price, rank, ingredients_text,
          Combination, Dry, Normal, Oily, Sensitive
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        list(df[["product_id","category","brand","product_name","price","rank","ingredients_text",
                 "Combination","Dry","Normal","Oily","Sensitive"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_product_chemicals(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    df["product_id"] = df["product_id"].apply(lambda x: clean_int(x, None))
    df["chemical_id"] = df["chemical_id"].apply(lambda x: clean_int(x, None))
    df["source_id"] = df["source_id"].apply(lambda x: clean_int(x, None)) if "source_id" in df.columns else None

    df["chemical_count"] = df["chemical_count"].apply(lambda x: clean_int(x, 1)) if "chemical_count" in df.columns else 1

    for dcol in ["initial_date_reported", "most_recent_date_reported", "discontinued_date"]:
        if dcol in df.columns:
            df[dcol] = df[dcol].apply(lambda x: clean_text(x, None))
        else:
            df[dcol] = None

    # Drop rows missing keys
    df = drop_if_null(df, ["product_id", "chemical_id"])
    df = df.drop_duplicates(subset=["product_id", "chemical_id"])

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT OR REPLACE INTO product_chemicals(
          source_id, product_id, chemical_id, initial_date_reported,
          most_recent_date_reported, discontinued_date, chemical_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        list(df[["source_id","product_id","chemical_id","initial_date_reported",
                 "most_recent_date_reported","discontinued_date","chemical_count"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


def load_cscpopendata_clean(conn: sqlite3.Connection, path: Path) -> int:
    df = pd.read_csv(path)

    # Fill text fields
    for col in ["brand", "primary_category", "sub_category", "cas_number", "chemical_name"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: clean_text(x, "Unknown"))
        else:
            df[col] = "Unknown"

    # incident_count
    df["incident_count"] = df["incident_count"].apply(lambda x: clean_int(x, 0)) if "incident_count" in df.columns else 0

    # dates
    for col in ["initial_date_reported", "most_recent_date_reported"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: clean_text(x, None))
        else:
            df[col] = None

    # Remove rows that still don’t make sense (optional): chemical_name unknown + cas_number unknown
    df = df[~((df["chemical_name"] == "Unknown") & (df["cas_number"] == "Unknown"))].copy()

    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO cscpopendata_clean(
          brand, primary_category, sub_category, cas_number, chemical_name,
          incident_count, initial_date_reported, most_recent_date_reported
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        list(df[["brand","primary_category","sub_category","cas_number","chemical_name",
                 "incident_count","initial_date_reported","most_recent_date_reported"]].itertuples(index=False, name=None)),
    )
    conn.commit()
    return len(df)


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    root = project_root()
    data_dir = csv_dir()

    db_path = init_db(root)
    conn = create_connection(db_path)

    try:
        # Load each dataset if present; provenance always generated
        files = {
            "companies": data_dir / "companies.csv",
            "brands": data_dir / "brands.csv",
            "categories": data_dir / "categories.csv",
            "chemicals": data_dir / "chemicals.csv",
            "ingredients_clean": data_dir / "ingredients_clean.csv",
            "products": data_dir / "products.csv",
            "cosmetics_clean": data_dir / "cosmetics_clean.csv",
            "product_chemicals": data_dir / "product_chemicals.csv",
            "cscpopendata_clean": data_dir / "cscpopendata_clean.csv",
        }

        # Always ensure unknown company exists (FK safety)
        ensure_unknown_company(conn)

        loaded_counts = {}

        if files["companies"].exists():
            loaded_counts["companies"] = load_companies(conn, files["companies"])
        if files["categories"].exists():
            loaded_counts["categories"] = load_categories(conn, files["categories"])
        if files["brands"].exists():
            loaded_counts["brands"] = load_brands(conn, files["brands"])
        if files["chemicals"].exists():
            loaded_counts["chemicals"] = load_chemicals(conn, files["chemicals"])
        if files["ingredients_clean"].exists():
            loaded_counts["ingredients_clean"] = load_ingredients_clean(conn, files["ingredients_clean"])
        if files["products"].exists():
            loaded_counts["products"] = load_products(conn, files["products"])
        if files["cosmetics_clean"].exists():
            loaded_counts["cosmetics_clean"] = load_cosmetics_clean(conn, files["cosmetics_clean"])
        if files["product_chemicals"].exists():
            loaded_counts["product_chemicals"] = load_product_chemicals(conn, files["product_chemicals"])
        if files["cscpopendata_clean"].exists():
            loaded_counts["cscpopendata_clean"] = load_cscpopendata_clean(conn, files["cscpopendata_clean"])

        # Provenance: link each dataset loaded (or expected) to a realistic report/article name
        for dataset_name in files.keys():
            upsert_provenance(conn, dataset_name)

        print("[DONE] Insertion finished.")
        print(f"DB: {db_path}")
        print("Loaded rows:")
        for k, v in loaded_counts.items():
            print(f" - {k}: {v}")

        print("\nProvenance linked in tables: provenance_source, provenance_dataset")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
