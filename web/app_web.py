# task_A_llm_rag/web/app_web.py
from __future__ import annotations

import os
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, render_template, request
from rapidfuzz import fuzz, process

from database.sqlite_db import get_db_path, create_connection, init_db
from chatbot.chatbot_app import ChatbotRAG

# Gemini (optionnel: traduction valeurs table)
try:
    from google import genai
    from google.genai import types
except Exception:  # pragma: no cover
    genai = None
    types = None


# -------------------------
# Helpers (paths, db)
# -------------------------
def get_project_root() -> Path:
    # ./task_A_llm_rag/web/app_web.py -> parents[1] = task_A_llm_rag
    return Path(__file__).resolve().parents[1]


def get_conn() -> sqlite3.Connection:
    db_path = get_db_path(get_project_root())
    conn = create_connection(db_path)
    # optionnel: rows dict-like si tu veux
    # conn.row_factory = sqlite3.Row
    return conn


def _quote_ident(name: str) -> str:
    """
    Quote safe for SQLite identifiers: "name" with escaped quotes.
    We only use this after validation against sqlite_master.
    """
    return '"' + (name or "").replace('"', '""') + '"'


def list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
        """
    )
    return [r[0] for r in cur.fetchall()]


def _safe_table_name(conn: sqlite3.Connection, name: str) -> str:
    tables = set(list_tables(conn))
    if name not in tables:
        raise ValueError(f"Unknown table: {name}")
    return name


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    t = _quote_ident(table)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({t});")
    return [r[1] for r in cur.fetchall()]


def _like_escape(s: str) -> str:
    """
    We use ESCAPE '!' in SQL (single char). Do NOT change to multi-char.
    This is exactly to avoid: ESCAPE expression must be a single character. :contentReference[oaicite:1]{index=1}
    """
    s = s.replace("!", "!!")
    s = s.replace("%", "!%")
    s = s.replace("_", "!_")
    return s


def _normalize_q(q: str) -> str:
    q = (q or "").strip()
    q = re.sub(r"\s+", " ", q)
    return q


def _norm_for_match(s: str) -> str:
    """
    Normalisation "typo tolerant":
    - lowercase
    - remove accents
    - remove extra spaces
    """
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def _guess_text_cols(cols: List[str]) -> List[str]:
    keys = [
        "name",
        "label",
        "title",
        "ingredient",
        "brand",
        "company",
        "category",
        "chemical",
        "inci",
        "desc",
        "product",
        "function",
        "use",
        "warning",
        "risk",
    ]
    text_cols = [c for c in cols if any(k in c.lower() for k in keys)]
    if not text_cols:
        text_cols = cols[: min(4, len(cols))]
    return text_cols


# -------------------------
# Translation (optional, Gemini)
# -------------------------
def _gemini_available() -> bool:
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    return bool(api_key) and genai is not None and types is not None


@lru_cache(maxsize=2000)
def _translate_one_cached(text: str, target_lang: str) -> str:
    text = text or ""
    target_lang = (target_lang or "").strip().lower()
    if not text.strip() or not target_lang or target_lang == "fr":
        return text
    if not _gemini_available():
        return text

    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    client = genai.Client(api_key=api_key)
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

    prompt = f"""
Translate the following text to {target_lang}.
Return ONLY the translation (no quotes, no explanations).

TEXT:
{text}
""".strip()

    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=220,
        ),
    )
    out = (resp.text or "").strip()
    return out if out else text


def translate_rows(rows: List[Dict[str, Any]], target_lang: str, max_cells: int = 250) -> List[Dict[str, Any]]:
    target_lang = (target_lang or "").strip().lower()
    if not target_lang or target_lang == "fr":
        return rows

    used = 0
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr: Dict[str, Any] = {}
        for k, v in r.items():
            if isinstance(v, str) and v.strip() and used < max_cells:
                rr[k] = _translate_one_cached(v, target_lang)
                used += 1
            else:
                rr[k] = v
        out.append(rr)
    return out


# -------------------------
# Vocabulary (typo tolerant suggestions)
# -------------------------
@lru_cache(maxsize=64)
def _distinct_text_values(project_root_str: str, table: str) -> List[str]:
    """
    Vocabulaire depuis valeurs DISTINCT des colonnes text probables.
    Cache mémoire => rapide.
    """
    project_root = Path(project_root_str)
    conn = create_connection(get_db_path(project_root))
    try:
        table = _safe_table_name(conn, table)
        cols = _table_columns(conn, table)
        text_cols = _guess_text_cols(cols)

        t = _quote_ident(table)
        cur = conn.cursor()
        values = set()

        per_col_limit = 5000
        for c in text_cols:
            cc = _quote_ident(c)
            try:
                cur.execute(
                    f"""
                    SELECT DISTINCT CAST({cc} AS TEXT)
                    FROM {t}
                    WHERE {cc} IS NOT NULL AND TRIM(CAST({cc} AS TEXT)) != ''
                    LIMIT ?;
                    """,
                    (per_col_limit,),
                )
                for (v,) in cur.fetchall():
                    if v:
                        vv = str(v).strip()
                        if vv:
                            values.add(vv)
            except Exception:
                continue

        return sorted(values)
    finally:
        conn.close()


def _rapidfuzz_suggestions(q: str, vocab: List[str], top_k: int = 8) -> Tuple[Dict[str, Any] | None, List[Dict[str, Any]]]:
    """
    Suggestions robustes:
    - match sur versions normalisées
    - retourne les valeurs originales
    """
    if not vocab:
        return None, []

    qn = _norm_for_match(q)
    if not qn:
        return None, []

    # map norm -> list of originals (au cas où collisions)
    norm_map: Dict[str, List[str]] = {}
    norm_keys: List[str] = []
    for v in vocab:
        nv = _norm_for_match(v)
        if not nv:
            continue
        if nv not in norm_map:
            norm_map[nv] = [v]
            norm_keys.append(nv)
        else:
            # garde 1-2 variantes
            if len(norm_map[nv]) < 2 and v not in norm_map[nv]:
                norm_map[nv].append(v)

    hits = process.extract(
        qn,
        norm_keys,
        scorer=fuzz.WRatio,
        limit=max(top_k, 25),
    )

    suggestions: List[Dict[str, Any]] = []
    for (norm_hit, score, _) in hits:
        if score < 55:
            continue
        originals = norm_map.get(norm_hit, [])
        for orig in originals:
            suggestions.append({"text": orig, "score": int(score)})
            if len(suggestions) >= top_k:
                break
        if len(suggestions) >= top_k:
            break

    best = suggestions[0] if suggestions else None
    return best, suggestions


# -------------------------
# Dictionary fallback (no dd_* needed)
# -------------------------
def _dictionary_from_schema(conn: sqlite3.Connection, q: str) -> Dict[str, Any]:
    qn = _normalize_q(q).lower()
    tables = list_tables(conn)

    tables_out: List[Dict[str, Any]] = []
    cols_out: List[Dict[str, Any]] = []

    cur = conn.cursor()

    for tname in tables:
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (tname,))
        sql = cur.fetchone()
        desc = (sql[0] if sql and sql[0] else "")[:800]

        if (not qn) or (qn in tname.lower()) or (qn in desc.lower()):
            tables_out.append({"table_name": tname, "description": desc})

        t = _quote_ident(tname)
        cur.execute(f"PRAGMA table_info({t});")
        for row in cur.fetchall():
            col_name = row[1]
            col_type = row[2] or ""

            if qn:
                blob = f"{tname} {col_name} {col_type} {desc}".lower()
                if qn not in blob:
                    continue

            cols_out.append(
                {
                    "table_name": tname,
                    "column_name": col_name,
                    "data_type": col_type,
                    "description": "",
                }
            )

    if not qn:
        cols_out = cols_out[:300]

    return {"q": q, "tables": tables_out, "columns": cols_out, "mode": "schema"}


# -------------------------
# App
# -------------------------
def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # Ensure DB exists
    init_db(get_project_root())

    chatbot = ChatbotRAG(get_project_root())

    # ---- Pages ----
    @app.get("/")
    @app.get("/dashboard")
    def dashboard():
        return render_template("dashboard.html")

    @app.get("/chat")
    def chat_page():
        return render_template("chat.html")

    @app.get("/data")
    def data_page():
        return render_template("data.html")

    @app.get("/dictionary")
    def dictionary_page():
        return render_template("dictionary.html")

    # ---- Chat API ----
    @app.post("/api/chat")
    def api_chat():
        payload = request.get_json(force=True, silent=False) or {}
        question = (payload.get("question") or "").strip()
        top_k = int(payload.get("top_k") or 5)
        if not question:
            return jsonify({"error": "Question vide."}), 400
        try:
            ans = chatbot.answer(question, top_k=top_k)
            return jsonify({"answer": ans.answer, "sources": ans.sources})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # ---- Data APIs ----
    @app.get("/api/data/tables")
    def api_tables():
        conn = get_conn()
        try:
            return jsonify({"tables": list_tables(conn)})
        finally:
            conn.close()

    @app.get("/api/data/table/<table>")
    def api_table(table: str):
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
        lang = (request.args.get("lang") or "fr").lower()

        limit = max(1, min(limit, 200))
        offset = max(0, offset)

        conn = get_conn()
        try:
            table = _safe_table_name(conn, table)
            cols = _table_columns(conn, table)

            t = _quote_ident(table)

            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {t};")
            total = int(cur.fetchone()[0])

            cur.execute(f"SELECT * FROM {t} LIMIT ? OFFSET ?;", (limit, offset))
            rows = cur.fetchall()
            data = [dict(zip(cols, row)) for row in rows]

            data = translate_rows(data, lang, max_cells=250)

            return jsonify({"table": table, "columns": cols, "rows": data, "total": total, "limit": limit, "offset": offset})
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        finally:
            conn.close()

    @app.get("/api/data/suggest")
    def api_data_suggest():
        table = (request.args.get("table") or "").strip()
        q = _normalize_q(request.args.get("q") or "")
        top_k = int(request.args.get("top_k") or 8)

        if not table or not q:
            return jsonify({"table": table, "typed": q, "best_match": None, "suggestions": [], "similar": []})

        conn = get_conn()
        try:
            table = _safe_table_name(conn, table)
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        finally:
            conn.close()

        vocab = _distinct_text_values(str(get_project_root()), table)
        best, suggestions = _rapidfuzz_suggestions(q, vocab, top_k=top_k)
        _, similar = _rapidfuzz_suggestions(q, vocab, top_k=max(15, top_k))

        return jsonify({"table": table, "typed": q, "best_match": best, "suggestions": suggestions, "similar": similar})

    @app.get("/api/data/search")
    def api_data_search():
        """
        Recherche avancée (typos):
        - suggestions typo-tolerant
        - SQL LIKE préfiltre (sur q + quelques suggestions) -> plus de chances de récupérer des candidats
        - ranking RapidFuzz (sur texte concaténé)
        """
        table = (request.args.get("table") or "").strip()
        q = _normalize_q(request.args.get("q") or "")
        limit = int(request.args.get("limit", 30))
        lang = (request.args.get("lang") or "fr").lower()

        limit = max(1, min(limit, 100))
        if not table or not q:
            return jsonify({"table": table, "typed": q, "columns": [], "rows": [], "best_match": None, "suggestions": [], "similar": []})

        conn = get_conn()
        try:
            table = _safe_table_name(conn, table)
            cols = _table_columns(conn, table)
            text_cols = _guess_text_cols(cols)

            t = _quote_ident(table)

            # --- Suggestions typo-tolerant ---
            vocab = _distinct_text_values(str(get_project_root()), table)
            best, suggestions = _rapidfuzz_suggestions(q, vocab, top_k=8)
            _, similar = _rapidfuzz_suggestions(q, vocab, top_k=15)

            # --- SQL prefilter LIKE (sur q + top suggestions) ---
            # On ajoute 2-3 suggestions pour récupérer des rows quand q est très "cassé"
            like_terms = [q]
            for s in (suggestions or [])[:3]:
                st = (s.get("text") or "").strip()
                if st and st.lower() not in {x.lower() for x in like_terms}:
                    like_terms.append(st)

            # WHERE (col LIKE term1 OR col LIKE term2 ...) sur toutes les text_cols
            where_parts: List[str] = []
            params: List[Any] = []
            for c in text_cols:
                cc = _quote_ident(c)
                for term in like_terms:
                    like = f"%{_like_escape(term)}%"
                    where_parts.append(f"CAST({cc} AS TEXT) LIKE ? ESCAPE '!'")
                    params.append(like)

            sql = f"SELECT * FROM {t} WHERE " + " OR ".join(where_parts) + " LIMIT 800;"

            cur = conn.cursor()
            cur.execute(sql, params)
            candidates = cur.fetchall()
            rows = [dict(zip(cols, row)) for row in candidates]

            # --- Ranking RapidFuzz ---
            qn = _norm_for_match(q)

            scored: List[Tuple[int, Dict[str, Any]]] = []
            for r in rows:
                blob = " ".join(str(r.get(c) or "") for c in text_cols).strip()
                if not blob:
                    continue
                bn = _norm_for_match(blob)

                # scoring stable (WRatio est OK mais parfois "bruyant")
                score = max(
                    int(fuzz.WRatio(qn, bn)),
                    int(fuzz.token_set_ratio(qn, bn)),
                )

                if score >= 55:
                    scored.append((score, r))

            scored.sort(key=lambda x: x[0], reverse=True)
            top_rows = [r for _, r in scored[:limit]]
            top_rows = translate_rows(top_rows, lang, max_cells=250)

            return jsonify(
                {
                    "table": table,
                    "typed": q,
                    "columns": cols,
                    "text_columns_used": text_cols,
                    "rows": top_rows,
                    "best_match": best,
                    "suggestions": suggestions,
                    "similar": similar,
                }
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        finally:
            conn.close()

    # ---- Dictionary APIs ----
    @app.get("/api/dictionary/search")
    def api_dictionary_search():
        """
        - si dd_table/dd_column existent => on les utilise
        - sinon => fallback live schema (sqlite_master + PRAGMA)
        """
        q = _normalize_q(request.args.get("q") or "")

        conn = get_conn()
        try:
            tables = set(list_tables(conn))

            if "dd_table" in tables and "dd_column" in tables:
                cur = conn.cursor()
                if not q:
                    cur.execute("SELECT table_name, description FROM dd_table ORDER BY table_name;")
                    t = [{"table_name": a, "description": b} for (a, b) in cur.fetchall()]
                    return jsonify({"q": "", "tables": t, "columns": [], "mode": "dd"})

                like = f"%{_like_escape(q)}%"
                cur.execute(
                    """
                    SELECT table_name, description
                    FROM dd_table
                    WHERE table_name LIKE ? ESCAPE '!' OR description LIKE ? ESCAPE '!'
                    ORDER BY table_name
                    LIMIT 120;
                    """,
                    (like, like),
                )
                t = [{"table_name": a, "description": b} for (a, b) in cur.fetchall()]

                cur.execute(
                    """
                    SELECT table_name, column_name, data_type, description
                    FROM dd_column
                    WHERE table_name LIKE ? ESCAPE '!'
                       OR column_name LIKE ? ESCAPE '!'
                       OR description LIKE ? ESCAPE '!'
                    ORDER BY table_name, column_name
                    LIMIT 400;
                    """,
                    (like, like, like),
                )
                c = [{"table_name": a, "column_name": b, "data_type": d, "description": e} for (a, b, d, e) in cur.fetchall()]

                return jsonify({"q": q, "tables": t, "columns": c, "mode": "dd"})

            # fallback
            return jsonify(_dictionary_from_schema(conn, q))
        finally:
            conn.close()

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="127.0.0.1", port=8000, debug=True)
