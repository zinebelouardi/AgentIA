# task_A_llm_rag/rag/chunker.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Iterable

from tqdm import tqdm


@dataclass
class ChunkerConfig:
    input_pages_dir: Path               # data/chunks (pages .jsonl)
    output_chunks_dir: Path             # data/chunks_ready (chunks .jsonl)
    chunk_size: int = 900               # approx characters
    chunk_overlap: int = 150            # overlap characters
    min_chunk_chars: int = 80           # drop tiny chunks
    normalize_whitespace: bool = True


def _normalize(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _split_text_char(text: str, chunk_size: int, overlap: int) -> List[str]:
    """
    Simple character-based chunking with overlap.
    Good enough for a first RAG system (stable + predictable).
    """
    if not text:
        return []

    chunks: List[str] = []
    start = 0
    n = len(text)

    while start < n:
        end = min(start + chunk_size, n)
        chunk = text[start:end]
        chunks.append(chunk)

        if end == n:
            break
        start = max(0, end - overlap)

    return chunks


def iter_jsonl(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_jsonl(path: Path, records: List[Dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def chunk_all(cfg: ChunkerConfig) -> None:
    cfg.input_pages_dir.mkdir(parents=True, exist_ok=True)
    cfg.output_chunks_dir.mkdir(parents=True, exist_ok=True)

    page_files = sorted(cfg.input_pages_dir.glob("*.jsonl"))
    if not page_files:
        print(f"[WARN] No page JSONL found in: {cfg.input_pages_dir}")
        print("       Run ingestion/pdf_loader.py first.")
        return

    for page_file in tqdm(page_files, desc="Chunking JSONL pages"):
        out_file = cfg.output_chunks_dir / page_file.name  # same name, now chunk-level

        chunks_out: List[Dict] = []
        chunk_counter = 0

        for rec in iter_jsonl(page_file):
            doc_name = rec.get("doc_name", page_file.name)
            page = rec.get("page", None)
            text = rec.get("text", "") or ""

            if cfg.normalize_whitespace:
                text = _normalize(text)

            # Skip empty
            if len(text) < cfg.min_chunk_chars:
                continue

            pieces = _split_text_char(text, cfg.chunk_size, cfg.chunk_overlap)

            for piece in pieces:
                piece = piece.strip()
                if len(piece) < cfg.min_chunk_chars:
                    continue

                chunk_id = f"{doc_name}::p{page}::c{chunk_counter}"
                chunks_out.append(
                    {
                        "chunk_id": chunk_id,
                        "doc_name": doc_name,
                        "page": page,
                        "text": piece,
                    }
                )
                chunk_counter += 1

        write_jsonl(out_file, chunks_out)

    print("[DONE] Chunking completed.")
    print(f"       Output chunks: {cfg.output_chunks_dir}")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    cfg = ChunkerConfig(
        input_pages_dir=project_root / "data" / "chunks",
        output_chunks_dir=project_root / "data" / "chunks_ready",
        chunk_size=900,
        chunk_overlap=150,
        min_chunk_chars=80,
        normalize_whitespace=True,
    )
    chunk_all(cfg)


if __name__ == "__main__":
    main()
