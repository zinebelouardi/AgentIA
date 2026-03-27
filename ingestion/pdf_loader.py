# task_A_llm_rag/ingestion/pdf_loader.py
# ------------------------------------------------------------
# Extract text from PDFs with page-level metadata for RAG.
# Input :  data/raw_pdfs/*.pdf
# Output:  data/chunks/<pdf_name>.jsonl   (one JSON per page)
# Optional: data/raw_text/<pdf_name>.txt  (full text)
# ------------------------------------------------------------

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from tqdm import tqdm

# Primary PDF engine
import fitz  # PyMuPDF

# Fallback engine (sometimes better for tables)
try:
    import pdfplumber
except Exception:
    pdfplumber = None


@dataclass
class PDFLoaderConfig:
    input_dir: Path
    chunks_dir: Path
    raw_text_dir: Optional[Path] = None

    # If True, write a single txt file containing all pages text
    save_raw_text: bool = True

    # If True, also include a very small cleaned text version
    basic_cleaning: bool = True

    # Skip pages with too little extracted text
    min_chars_per_page: int = 20

    # Prefer PyMuPDF. If it yields too little, fallback to pdfplumber (if installed)
    enable_fallback_pdfplumber: bool = True


def _normalize_whitespace(text: str) -> str:
    # Remove repeated spaces, normalize line breaks a bit
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    # collapse many blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_with_pymupdf(pdf_path: Path) -> List[Dict]:
    """
    Returns a list of dicts:
    [{"page": 1, "text": "..."} ...]
    """
    pages: List[Dict] = []
    with fitz.open(pdf_path) as doc:
        for i in range(len(doc)):
            page = doc[i]
            # "text" is usually ok; "blocks" can be used for layout, but keep simple
            text = page.get_text("text") or ""
            pages.append({"page": i + 1, "text": text})
    return pages


def _extract_with_pdfplumber(pdf_path: Path) -> List[Dict]:
    if pdfplumber is None:
        raise RuntimeError("pdfplumber is not installed but fallback was requested.")
    pages: List[Dict] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            pages.append({"page": i + 1, "text": text})
    return pages


def extract_pdf_pages(pdf_path: Path, cfg: PDFLoaderConfig) -> List[Dict]:
    """
    Extract per-page text. Uses PyMuPDF first; optionally falls back to pdfplumber.
    """
    pages = _extract_with_pymupdf(pdf_path)

    # Fallback if the extraction looks poor (e.g., scanned PDFs or weird encodings)
    if cfg.enable_fallback_pdfplumber and pdfplumber is not None:
        total_chars = sum(len(p.get("text", "")) for p in pages)
        # Heuristic: if very little text overall, try pdfplumber
        if total_chars < 200:
            try:
                pages2 = _extract_with_pdfplumber(pdf_path)
                total_chars2 = sum(len(p.get("text", "")) for p in pages2)
                if total_chars2 > total_chars:
                    pages = pages2
            except Exception:
                # Keep PyMuPDF result if fallback fails
                pass

    if cfg.basic_cleaning:
        for p in pages:
            p["text"] = _normalize_whitespace(p.get("text", ""))

    # Filter very empty pages
    filtered = [p for p in pages if len(p.get("text", "")) >= cfg.min_chars_per_page]
    return filtered


def ensure_dirs(cfg: PDFLoaderConfig) -> None:
    cfg.input_dir.mkdir(parents=True, exist_ok=True)
    cfg.chunks_dir.mkdir(parents=True, exist_ok=True)
    if cfg.save_raw_text and cfg.raw_text_dir is not None:
        cfg.raw_text_dir.mkdir(parents=True, exist_ok=True)


def write_jsonl(out_path: Path, records: List[Dict]) -> None:
    """
    Writes JSON Lines (one JSON object per line).
    """
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def write_raw_text(out_path: Path, records: List[Dict]) -> None:
    """
    Writes a single .txt containing all pages.
    """
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            page = rec.get("page", "?")
            text = rec.get("text", "")
            f.write(f"=== Page {page} ===\n")
            f.write(text)
            f.write("\n\n")


def process_all_pdfs(cfg: PDFLoaderConfig) -> None:
    ensure_dirs(cfg)

    pdf_files = sorted(cfg.input_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"[WARN] No PDF found in: {cfg.input_dir}")
        print("       Put your PDFs there, then re-run.")
        return

    print(f"[INFO] Found {len(pdf_files)} PDF(s) in {cfg.input_dir}")

    for pdf_path in tqdm(pdf_files, desc="Extracting PDFs"):
        try:
            pages = extract_pdf_pages(pdf_path, cfg)

            # Prepare records with metadata for RAG
            doc_name = pdf_path.name
            doc_stem = pdf_path.stem

            records = []
            for p in pages:
                records.append(
                    {
                        "doc_name": doc_name,
                        "doc_path": str(pdf_path.as_posix()),
                        "page": int(p["page"]),
                        "text": p["text"],
                    }
                )

            # Save per-document JSONL
            out_jsonl = cfg.chunks_dir / f"{doc_stem}.jsonl"
            write_jsonl(out_jsonl, records)

            # Optional: save raw text
            if cfg.save_raw_text and cfg.raw_text_dir is not None:
                out_txt = cfg.raw_text_dir / f"{doc_stem}.txt"
                write_raw_text(out_txt, records)

        except Exception as e:
            print(f"\n[ERROR] Failed on {pdf_path.name}: {e}", file=sys.stderr)


def main() -> None:
    # Project root = task_A_llm_rag/
    project_root = Path(__file__).resolve().parents[1]

    cfg = PDFLoaderConfig(
        input_dir=project_root / "data" / "raw_pdfs",
        chunks_dir=project_root / "data" / "chunks",
        raw_text_dir=project_root / "data" / "raw_text",
        save_raw_text=True,
        basic_cleaning=True,
        min_chars_per_page=20,
        enable_fallback_pdfplumber=True,
    )

    process_all_pdfs(cfg)
    print("[DONE] Extraction finished.")
    print(f"       JSONL chunks: {cfg.chunks_dir}")
    if cfg.save_raw_text and cfg.raw_text_dir is not None:
        print(f"       Raw text:     {cfg.raw_text_dir}")


if __name__ == "__main__":
    main()
