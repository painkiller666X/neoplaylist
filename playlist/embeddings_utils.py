# backend/playlist/embeddings_utils.py
import os
import json
import logging
import requests
import numpy as np
from typing import List

EMBEDDING_URL = os.getenv("EMBEDDING_URL", "http://127.0.0.1:11434/api/embeddings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "mxbai-embed-large")
EMBEDDINGS_CACHE = os.getenv("EMBEDDINGS_CACHE", "./logs/embeddings_cache.json")

# ============================================================
# 🧠 Funciones de Embeddings
# ============================================================

def get_embedding(text: str) -> List[float]:
    """
    Obtiene el embedding (vector) de un texto usando Ollama o API local.
    Si hay error, retorna vector nulo.
    """
    if not text:
        return [0.0] * 512
    try:
        payload = {"model": EMBEDDING_MODEL, "prompt": text}
        resp = requests.post(EMBEDDING_URL, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("embedding", [])
        logging.error(f"❌ Error de embeddings: {resp.text}")
    except Exception as e:
        logging.warning(f"⚠️ Fallback embedding local: {e}")
    return [0.0] * 512

def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    """
    Calcula similitud coseno entre dos vectores.
    """
    if not vec_a or not vec_b:
        return 0.0
    a, b = np.array(vec_a), np.array(vec_b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-9))

def compare_texts_similarity(text_a: str, text_b: str) -> float:
    """
    Compara similitud semántica entre dos textos usando embeddings.
    """
    emb_a = get_embedding(text_a)
    emb_b = get_embedding(text_b)
    return cosine_similarity(emb_a, emb_b)

def cache_embedding(text: str, vector: List[float]):
    """
    Guarda embeddings en caché local para evitar recomputar.
    """
    try:
        os.makedirs(os.path.dirname(EMBEDDINGS_CACHE), exist_ok=True)
        cache = {}
        if os.path.exists(EMBEDDINGS_CACHE):
            with open(EMBEDDINGS_CACHE, "r", encoding="utf-8") as f:
                cache = json.load(f)
        cache[text] = vector
        with open(EMBEDDINGS_CACHE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.debug(f"No se pudo escribir cache de embeddings: {e}")
