from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Optional
from django.conf import settings

# =========================================================
# Local Embedding (CPU ONLY, multilingual-e5)
# =========================================================

@dataclass(frozen=True)
class LocalEmbedConfig:
    model_name: str
    normalize: bool
    batch_size: int
    expected_dim: int


_embedder = None
_embedder_lock = threading.Lock()


# ---------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------
def _get_str_setting(name: str, default: str) -> str:
    v = getattr(settings, name, None)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return default


def _get_bool_setting(name: str, default: bool) -> bool:
    v = getattr(settings, name, None)
    if v is None:
        return default
    return bool(v)


def _get_int_setting(name: str, default: int) -> int:
    v = getattr(settings, name, None)
    try:
        return int(v)
    except Exception:
        return default


# ---------------------------------------------------------
# Config (CPU 고정)
# ---------------------------------------------------------
def get_local_embed_config() -> LocalEmbedConfig:
    model_name = _get_str_setting(
        "LOCAL_EMBED_MODEL",
        "intfloat/multilingual-e5-base",
    )

    normalize = _get_bool_setting("LOCAL_EMBED_NORMALIZE", True)
    batch_size = _get_int_setting("LOCAL_EMBED_BATCH_SIZE", 16)
    expected_dim = _get_int_setting("LOCAL_EMBED_DIM", -1)

    batch_size = max(1, min(batch_size, 512))

    return LocalEmbedConfig(
        model_name=model_name,
        normalize=normalize,
        batch_size=batch_size,
        expected_dim=expected_dim,
    )


# ---------------------------------------------------------
# Embedder (CPU only, singleton)
# ---------------------------------------------------------
def _get_embedder():
    global _embedder

    if _embedder is not None:
        return _embedder

    with _embedder_lock:
        if _embedder is not None:
            return _embedder

        from sentence_transformers import SentenceTransformer

        cfg = get_local_embed_config()
        _embedder = SentenceTransformer(
            cfg.model_name,
            device="cpu",
        )
        return _embedder


# ---------------------------------------------------------
# Utils
# ---------------------------------------------------------
def _clean_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


# =========================================================
# Public APIs
# =========================================================
def embed_passages(texts: list[str]) -> list[Optional[list[float]]]:
    """
    문서(기사) 임베딩
    - passage: prefix 사용
    - CPU ONLY
    """
    cfg = get_local_embed_config()

    cleaned = [_clean_text(t) for t in texts]
    prefixed = [f"passage: {t}" if t else "" for t in cleaned]

    idx_map: list[int] = []
    inputs: list[str] = []

    for i, t in enumerate(prefixed):
        if t and t.strip() != "passage:":
            idx_map.append(i)
            inputs.append(t)

    out: list[Optional[list[float]]] = [None] * len(texts)
    if not inputs:
        return out

    model = _get_embedder()

    vecs = model.encode(
        inputs,
        normalize_embeddings=cfg.normalize,
        batch_size=cfg.batch_size,
        show_progress_bar=False,
    )

    try:
        vecs_list = vecs.tolist()
    except Exception:
        vecs_list = [list(v) for v in vecs]

    for j, orig_idx in enumerate(idx_map):
        v = vecs_list[j]
        if not isinstance(v, list):
            continue

        if cfg.expected_dim > 0 and len(v) != cfg.expected_dim:
            continue

        out[orig_idx] = v

    return out


def embed_query(text: str) -> Optional[list[float]]:
    """
    검색 질의 임베딩
    - query: prefix 사용
    - CPU ONLY
    """
    q = _clean_text(text)
    if not q:
        return None

    cfg = get_local_embed_config()
    model = _get_embedder()

    v = model.encode(
        f"query: {q}",
        normalize_embeddings=cfg.normalize,
        show_progress_bar=False,
    )

    try:
        v_list = v.tolist()
    except Exception:
        v_list = list(v)

    if not isinstance(v_list, list):
        return None

    if cfg.expected_dim > 0 and len(v_list) != cfg.expected_dim:
        return None

    return v_list
