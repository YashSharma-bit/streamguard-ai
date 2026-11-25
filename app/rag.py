from __future__ import annotations

import hashlib
import math
from pathlib import Path
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from app.config import settings


class LocalHashEmbeddings(Embeddings):
    """Deterministic lightweight embeddings for offline FAISS indexing without external API calls."""

    def __init__(self, dim: int = 128) -> None:
        self.dim = dim

    def _embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        tokens = [t.strip(".,:;()[]`\"'").lower() for t in text.split() if len(t) > 1]
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            idx = int.from_bytes(digest[:2], "big") % self.dim
            sign = 1.0 if digest[2] % 2 == 0 else -1.0
            vec[idx] += sign
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)


class RunbookRAGStore:
    """LangChain + FAISS vector store indexing SRE Markdown operational runbooks."""

    def __init__(self, runbooks_dir: Path | None = None) -> None:
        self.runbooks_dir = runbooks_dir or settings.runbooks_dir
        self._vector_store: FAISS | None = None

    def _get_embeddings(self) -> Embeddings:
        if settings.openai_api_key:
            from langchain_openai import OpenAIEmbeddings

            return OpenAIEmbeddings(api_key=settings.openai_api_key)
        return LocalHashEmbeddings()

    def build_index(self) -> FAISS:
        docs: list[Document] = []
        for md_file in sorted(self.runbooks_dir.glob("*.md")):
            content = md_file.read_text(encoding="utf-8")
            docs.append(
                Document(
                    page_content=content,
                    metadata={"source": md_file.name, "title": content.splitlines()[0].lstrip("# ").strip()},
                )
            )

        if not docs:
            docs.append(
                Document(
                    page_content="Default SRE runbook: inspect broker logs, disk usage, and consumer lag.",
                    metadata={"source": "default.md", "title": "Default Runbook"},
                )
            )

        self._vector_store = FAISS.from_documents(docs, self._get_embeddings())
        return self._vector_store

    def retrieve(self, query: str, k: int = 2) -> list[dict[str, str]]:
        """Retrieve the top-k matching operational runbook excerpts."""
        if self._vector_store is None:
            self.build_index()

        assert self._vector_store is not None
        results = self._vector_store.similarity_search(query, k=k)
        return [
            {
                "source": doc.metadata.get("source", "unknown.md"),
                "title": doc.metadata.get("title", "Runbook"),
                "content": doc.page_content,
            }
            for doc in results
        ]


rag_store = RunbookRAGStore()
