# task_A_llm_rag/rag/vector_store.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Iterable, Tuple

import numpy as np
import faiss


@dataclass
class VectorStoreConfig:
    embeddings_dir: Path            # data/embeddings
    out_dir: Path                   # data/embeddings/faiss
    index_type: str = "IP"          # "IP" (inner product) for normalized vectors, or "L2"


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


def build_faiss_index(cfg: VectorStoreConfig) -> None:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    emb_path = cfg.embeddings_dir / "embeddings.npy"
    meta_path = cfg.embeddings_dir / "metadata.jsonl"
    if not emb_path.exists() or not meta_path.exists():
        print("[ERROR] embeddings.npy or metadata.jsonl not found.")
        print("        Run rag/embedder.py first.")
        return

    embeddings = np.load(emb_path).astype(np.float32)
    metadata = list(iter_jsonl(meta_path))

    if embeddings.shape[0] != len(metadata):
        raise ValueError(f"Mismatch: embeddings={embeddings.shape[0]} rows, metadata={len(metadata)} rows")

    dim = embeddings.shape[1]

    # For normalized embeddings, use Inner Product (IP) == cosine similarity
    if cfg.index_type.upper() == "IP":
        index = faiss.IndexFlatIP(dim)
    else:
        index = faiss.IndexFlatL2(dim)

    index.add(embeddings)

    # Save index
    index_path = cfg.out_dir / "faiss.index"
    faiss.write_index(index, str(index_path))

    # Save metadata copy
    write_jsonl(cfg.out_dir / "metadata.jsonl", metadata)

    # Save id mapping (FAISS row id -> chunk_id)
    id_map = [{"faiss_id": i, "chunk_id": metadata[i].get("chunk_id")} for i in range(len(metadata))]
    write_jsonl(cfg.out_dir / "id_map.jsonl", id_map)

    info = {
        "index_type": cfg.index_type.upper(),
        "dim": int(dim),
        "n_vectors": int(embeddings.shape[0]),
        "files": {
            "index": "faiss.index",
            "metadata": "metadata.jsonl",
            "id_map": "id_map.jsonl",
        },
    }
    (cfg.out_dir / "info.json").write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[DONE] FAISS index built:")
    print(f"       {index_path}")
    print(f"       {cfg.out_dir / 'metadata.jsonl'}")
    print(f"       {cfg.out_dir / 'id_map.jsonl'}")
    print(f"       {cfg.out_dir / 'info.json'}")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    cfg = VectorStoreConfig(
        embeddings_dir=project_root / "data" / "embeddings",
        out_dir=project_root / "data" / "embeddings" / "faiss",
        index_type="IP",
    )
    build_faiss_index(cfg)


if __name__ == "__main__":
    main()
