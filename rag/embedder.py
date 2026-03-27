# task_A_llm_rag/rag/embedder.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Iterable

import numpy as np
from tqdm import tqdm
from sentence_transformers import SentenceTransformer


@dataclass
class EmbedderConfig:
    input_chunks_dir: Path              # data/chunks_ready
    output_dir: Path                    # data/embeddings
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    batch_size: int = 64
    max_text_chars: int = 2000          # safety truncation


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


# ----------------------------
# ✅ NEW: reusable embed function
# ----------------------------
_MODEL_CACHE: Dict[str, SentenceTransformer] = {}


def get_model(model_name: str) -> SentenceTransformer:
    if model_name not in _MODEL_CACHE:
        _MODEL_CACHE[model_name] = SentenceTransformer(model_name)
    return _MODEL_CACHE[model_name]


def embed_texts(
    texts: List[str],
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    normalize: bool = True,
) -> np.ndarray:
    """
    Encode une liste de textes avec le même modèle que l'index.
    IMPORTANT: normalize=True pour correspondre à build_embeddings(normalize_embeddings=True)
    Retour: np.ndarray float32 shape (n, dim)
    """
    model = get_model(model_name)
    emb = model.encode(
        texts,
        convert_to_numpy=True,
        show_progress_bar=False,
        normalize_embeddings=normalize,
    ).astype(np.float32)
    return emb


def build_embeddings(cfg: EmbedderConfig) -> None:
    cfg.input_chunks_dir.mkdir(parents=True, exist_ok=True)
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    chunk_files = sorted(cfg.input_chunks_dir.glob("*.jsonl"))
    if not chunk_files:
        print(f"[WARN] No chunk JSONL found in: {cfg.input_chunks_dir}")
        print("       Run rag/chunker.py first.")
        return

    model = get_model(cfg.model_name)

    texts: List[str] = []
    meta: List[Dict] = []

    for chunk_file in chunk_files:
        for rec in iter_jsonl(chunk_file):
            text = (rec.get("text", "") or "")[: cfg.max_text_chars]
            if not text.strip():
                continue
            texts.append(text)
            meta.append(
                {
                    "chunk_id": rec.get("chunk_id"),
                    "doc_name": rec.get("doc_name"),
                    "page": rec.get("page"),
                    "text": text,  # 🔥 IMPORTANT: text stored here
                }
            )

    if not texts:
        print("[WARN] No valid texts to embed.")
        return

    print(f"[INFO] Embedding {len(texts)} chunks with {cfg.model_name}")

    embeddings_list: List[np.ndarray] = []
    for i in tqdm(range(0, len(texts), cfg.batch_size), desc="Embedding"):
        batch = texts[i : i + cfg.batch_size]
        emb = model.encode(
            batch,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,  # 🔥 IMPORTANT: must match query normalize
        )
        embeddings_list.append(emb.astype(np.float32))

    embeddings = np.vstack(embeddings_list)

    # Save
    np.save(cfg.output_dir / "embeddings.npy", embeddings)
    write_jsonl(cfg.output_dir / "metadata.jsonl", meta)

    info = {
        "model_name": cfg.model_name,
        "n_chunks": int(embeddings.shape[0]),
        "dim": int(embeddings.shape[1]),
        "normalized": True,
    }
    (cfg.output_dir / "info.json").write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[DONE] Embeddings saved:")
    print(f"       {cfg.output_dir / 'embeddings.npy'}")
    print(f"       {cfg.output_dir / 'metadata.jsonl'}")
    print(f"       {cfg.output_dir / 'info.json'}")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    cfg = EmbedderConfig(
        input_chunks_dir=project_root / "data" / "chunks_ready",
        output_dir=project_root / "data" / "embeddings",
        model_name="sentence-transformers/all-MiniLM-L6-v2",
        batch_size=64,
        max_text_chars=2000,
    )
    build_embeddings(cfg)


if __name__ == "__main__":
    main()
