# task_A_llm_rag/chatbot/chatbot_app.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any

from google import genai
from google.genai import types

from rag.retriever import FaissRetriever as ChromaRetriever, RetrievedChunk


MODEL_ID = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


@dataclass
class ChatAnswer:
    answer: str
    sources: List[Dict[str, Any]]  # title/page/chunk_id/score/snippet


def build_prompt(question: str, retrieved: List[RetrievedChunk]) -> str:
    context_blocks = []
    for i, ch in enumerate(retrieved, start=1):
        title = ch.metadata.get("source_title", ch.metadata.get("doc_name", "Unknown Source"))
        page = ch.metadata.get("page", "N/A")
        chunk_id = ch.metadata.get("chunk_id", f"chunk_{i}")
        context_blocks.append(
            f"[Source {i}] {title} | page={page} | chunk_id={chunk_id}\n{ch.text}"
        )

    context = "\n\n".join(context_blocks)

    # Prompt strict: n'utiliser QUE les sources
    prompt = f"""
Tu es un assistant spécialisé en cosmetovigilance.
Règles STRICTES :
- Réponds uniquement à partir des Sources ci-dessous.
- Si l'information n'est pas dans les sources, dis clairement : "Je ne trouve pas l'information dans les sources fournies."
- Ne devine pas. Ne complète pas avec des connaissances externes.

Question utilisateur :
{question}

Sources :
{context}

Format de sortie (en FR) :
1) Réponse (claire et concise)
2) "Sources utilisées :" puis liste de sources réellement utilisées au format :
   - <title> (page <page>)
"""
    return prompt.strip()


def _require_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError(
            "Clé Gemini manquante. Définis GEMINI_API_KEY (ou GOOGLE_API_KEY) dans l'environnement."
        )
    return key


def llm_generate(prompt: str) -> str:
    """
    Appel Gemini via Google Gen AI SDK (python-genai).
    - Utilise GEMINI_API_KEY / GOOGLE_API_KEY
    - Retourne response.text
    """
    api_key = _require_api_key()
    client = genai.Client(api_key=api_key)

    # System instruction (optionnel mais utile)
    sys_inst = (
        "You are a retrieval-augmented assistant. "
        "Use ONLY the provided sources. "
        "If missing, say you can't find it in provided sources."
    )

    # Safety settings (optionnel)
    # Tu peux les retirer si tu veux la config par défaut.
    safety = [
        types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_ONLY_HIGH"),
        types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_ONLY_HIGH"),
        types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_ONLY_HIGH"),
        types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_ONLY_HIGH"),
    ]

    resp = client.models.generate_content(
        model=MODEL_ID,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=sys_inst,
            temperature=0.2,
            max_output_tokens=800,
            safety_settings=safety,
        ),
    )

    # resp.text est la manière la plus simple
    text = getattr(resp, "text", None)
    if not text:
        # fallback si structure différente
        return str(resp)
    return text


class ChatbotRAG:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.retriever = ChromaRetriever(project_root)

    def answer(self, question: str, top_k: int = 5) -> ChatAnswer:
        retrieved = self.retriever.retrieve(question, top_k=top_k)

        # Si rien trouvé, on répond direct
        if not retrieved:
            return ChatAnswer(
                answer="Je ne trouve pas l'information dans les sources fournies.",
                sources=[],
            )

        prompt = build_prompt(question, retrieved)
        answer_text = llm_generate(prompt)

        sources = []
        for ch in retrieved:
            title = ch.metadata.get("source_title", ch.metadata.get("doc_name", "Unknown Source"))
            page = ch.metadata.get("page", "N/A")
            chunk_id = ch.metadata.get("chunk_id", "")
            snippet = (ch.text[:240] + "...") if len(ch.text) > 240 else ch.text
            sources.append({
                "title": title,
                "page": page,
                "chunk_id": chunk_id,
                "score": round(ch.score, 4),
                "snippet": snippet,
            })

        return ChatAnswer(answer=answer_text, sources=sources)
