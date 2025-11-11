# backend/playlist/optimizer.py
import logging
from typing import Dict, Any
from database.connection import music_db

logger = logging.getLogger("playlist.optimizer")

# ============================================================
# 🔹 Actualizar feedback del usuario
# ============================================================
def record_feedback(user_email: str, track_id: str, feedback: str):
    """
    Guarda feedback del usuario sobre una canción:
    feedback ∈ {"like", "skip", "dislike"}
    """
    logger.info(f"📝 Registrando feedback: {user_email} -> {track_id} ({feedback})")

    user_db = music_db["user_feedback"]
    user_db.update_one(
        {"email": user_email},
        {"$push": {"feedback": {"track_id": track_id, "value": feedback}}},
        upsert=True
    )

# ============================================================
# 🔹 Ajustar puntuaciones de canciones
# ============================================================
def optimize_playlist_weights(user_email: str):
    """
    Recalcula pesos de canciones basándose en feedback acumulado.
    """
    logger.info(f"⚙️ Optimizando pesos de playlist para {user_email}")

    feedback_db = music_db["user_feedback"]
    fb = feedback_db.find_one({"email": user_email})
    if not fb or not fb.get("feedback"):
        logger.info("⚠️ Sin feedback para optimizar.")
        return {}

    weights = {}
    for entry in fb["feedback"]:
        tid = entry["track_id"]
        val = entry["value"]
        weights[tid] = weights.get(tid, 0)
        if val == "like":
            weights[tid] += 2
        elif val == "skip":
            weights[tid] -= 1
        elif val == "dislike":
            weights[tid] -= 2

    logger.info(f"✅ Pesos calculados para {len(weights)} canciones.")
    return weights
