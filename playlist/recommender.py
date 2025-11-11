# backend/playlist/recommender.py
import logging
import random
import os
import re
from typing import List, Dict, Any
from database.connection import music_db
from repositories.track_repository import get_all_tracks
from playlist.services import call_ollama_safe
from playlist.ai_engine import heuristic_filter

logger = logging.getLogger("playlist.recommender")

# ============================================================
# 🔹 Recomendaciones basadas en usuario
# ============================================================
def recommend_for_user(user_email: str, context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Genera recomendaciones para un usuario:
    - Usa su historial, favoritos, y criterios contextuales.
    - Integra IA local para sugerencias complementarias.
    """
    context = context or {}
    logger.info(f"🎧 Generando recomendaciones para: {user_email} con contexto {context}")

    all_tracks = get_all_tracks()
    if not all_tracks:
        logger.warning("⚠️ No hay tracks disponibles en la base de datos.")
        return []

    # 1️⃣ Extraer historial del usuario (si existe)
    user_db = music_db["user_history"]
    user_data = user_db.find_one({"email": user_email})
    recent_artists = user_data.get("recent_artists", []) if user_data else []
    liked_genres = user_data.get("liked_genres", []) if user_data else []

    # 2️⃣ Aplicar filtro heurístico
    criteria = {}
    if liked_genres:
        criteria["genre"] = random.choice(liked_genres)
    if recent_artists:
        criteria["artist"] = random.choice(recent_artists)
    filtered = heuristic_filter(all_tracks, criteria)

    # 3️⃣ Complementar con IA si hay contexto adicional
    if context.get("prompt"):
        ai_result = call_ollama_safe(
            f"Genera canciones similares a {criteria} para un usuario que busca: {context['prompt']}"
        )
        if isinstance(ai_result, dict):
            keywords = ai_result.get("tracks") or []
            for t in all_tracks:
                if any(k.lower() in f"{t.get('artist','')} {t.get('title','')}".lower() for k in keywords):
                    filtered.append(t)

    # 4️⃣ Evitar duplicados
    unique = {t["id"]: t for t in filtered}.values()
    final_list = list(unique)

    if not final_list:
        final_list = random.sample(all_tracks, min(10, len(all_tracks)))
        logger.info("🎲 Fallback: recomendaciones aleatorias.")

    logger.info(f"✅ Recomendaciones generadas: {len(final_list)}")
    return final_list
