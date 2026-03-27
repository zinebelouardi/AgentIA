# task_A_llm_rag/rag/retriever.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Iterable

import numpy as np
import faiss

from rag.embedder import embed_texts


@dataclass
class RetrievedChunk:
    text: str
    score: float
    metadata: Dict[str, Any]


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


class FaissRetriever:
    """
    Retriever FAISS basé sur:
      data/embeddings/faiss/faiss.index
      data/embeddings/faiss/metadata.jsonl

    Le metadata.jsonl DOIT contenir "text" (chunk text) et metadata (doc_name, page, chunk_id).
    """

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.emb_dir = project_root / "data" / "embeddings"
        self.faiss_dir = self.emb_dir / "faiss"

        self.index_path = self.faiss_dir / "faiss.index"
        self.meta_path = self.faiss_dir / "metadata.jsonl"
        self.info_path = self.emb_dir / "info.json"

        if not self.index_path.exists():
            raise FileNotFoundError(f"Missing FAISS index: {self.index_path}")
        if not self.meta_path.exists():
            raise FileNotFoundError(f"Missing FAISS metadata: {self.meta_path}")

        # Load FAISS
        self.index = faiss.read_index(str(self.index_path))

        # Load metadata list
        self.metadata = list(iter_jsonl(self.meta_path))

        # Load embedder info (model_name)
        self.model_name = "sentence-transformers/all-MiniLM-L6-v2"
        if self.info_path.exists():
            info = json.loads(self.info_path.read_text(encoding="utf-8"))
            self.model_name = info.get("model_name", self.model_name)

        # Sanity checks
        if self.index.ntotal != len(self.metadata):
            raise ValueError(f"Mismatch: index.ntotal={self.index.ntotal} vs metadata={len(self.metadata)}")

        # If using IP cosine, embeddings must be normalized (you did normalize_embeddings=True)
        # So query must be normalized as well (embed_texts(..., normalize=True)).

    def _embed_query(self, question: str) -> np.ndarray:
        q_emb = embed_texts([question], model_name=self.model_name, normalize=True)  # (1, dim), float32
        if q_emb.ndim != 2 or q_emb.shape[0] != 1:
            q_emb = q_emb.reshape(1, -1).astype(np.float32)
        return q_emb

    def retrieve(self, question: str, top_k: int = 5) -> List[RetrievedChunk]:
        if self.index.ntotal == 0:
            return []

        q = self._embed_query(question)

        scores, ids = self.index.search(q, top_k)  # (1,k), (1,k)
        scores = scores[0].tolist()
        ids = ids[0].tolist()

        out: List[RetrievedChunk] = []
        for score, idx in zip(scores, ids):
            if idx is None or idx < 0:
                continue

            meta = self.metadata[idx] if idx < len(self.metadata) else {}
            text = meta.get("text") or meta.get("chunk_text") or meta.get("content") or ""

            out.append(
                RetrievedChunk(
                    text=text,
                    score=float(score),
                    metadata=meta,
                )
            )
        return out
