# backend/repositories/track_repository.py
from database.connection import music_db
from bson import ObjectId
from bson.errors import InvalidId
from typing import List, Dict, Optional
import logging

logger = logging.getLogger("repositories.tracks")

# ============================================================
# 🗂️ Colección de tracks
# ============================================================
TRACKS_COLLECTION = music_db["tracks"]

# ============================================================
# 🔹 Serializador de track
# ============================================================
def serialize_track(doc: dict) -> Optional[Dict]:
    """Convierte un documento Mongo en un dict JSON serializable."""
    if not doc:
        return None
    track = dict(doc)
    track["id"] = str(track.get("_id"))
    track.pop("_id", None)
    return track

# ============================================================
# 🔹 Obtener todos los tracks
# ============================================================
def get_all_tracks(limit: Optional[int] = None) -> List[Dict]:
    """
    Devuelve todos los tracks disponibles en la base.
    Usado por los motores híbrido, smart y contextual.
    """
    try:
        cursor = TRACKS_COLLECTION.find({}, {
            "_id": 1,
            "artist": 1,
            "title": 1,
            "genre": 1,
            "mood": 1,
            "year": 1,
            "LastFMPlaycount": 1,
            "LastFMListeners": 1,
            "YouTubeViews": 1,
        })
        if limit:
            cursor = cursor.limit(limit)
        tracks = [serialize_track(doc) for doc in cursor]
        return tracks
    except Exception as e:
        logger.exception("❌ Error al obtener tracks desde MongoDB.")
        return []

# ============================================================
# 🔹 Obtener track por ID
# ============================================================
def get_track_by_id(track_id: str) -> Optional[Dict]:
    """Obtiene un track por su ObjectId (como string)."""
    try:
        obj_id = ObjectId(track_id)
    except InvalidId:
        logger.warning(f"⚠️ ID de track inválido: {track_id}")
        return None

    try:
        doc = TRACKS_COLLECTION.find_one({"_id": obj_id})
        return serialize_track(doc)
    except Exception as e:
        logger.debug(f"⚠️ Error obteniendo track {track_id}: {e}")
        return None
