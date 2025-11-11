from database.connection import music_db
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime
from typing import List, Optional, Dict, Any
from repositories.track_repository import get_track_by_id
import logging

# ============================================================
# 🗂️ Colección de playlists
# ============================================================
PLAYLISTS_COLLECTION = music_db["playlists"]

# Crear índice si no existe (búsqueda rápida por nombre)
try:
    PLAYLISTS_COLLECTION.create_index("name")
except Exception as e:
    logging.debug(f"⚠️ No se pudo crear índice 'name': {e}")

# ============================================================
# 🔹 Serializar playlist
# ============================================================
def serialize_playlist(doc: dict, include_tracks: bool = True) -> dict:
    """Convierte un documento Mongo en una playlist lista para API JSON."""
    if not doc:
        return {}

    playlist = {
        "id": str(doc.get("_id")),
        "name": doc.get("name", "Sin nombre"),
        "description": doc.get("description", ""),
        "created_at": doc.get("created_at", datetime.utcnow().isoformat()),
        "updated_at": doc.get("updated_at", datetime.utcnow().isoformat()),
        "total_tracks": len(doc.get("tracks", [])),
        "tracks": [],
    }

    if include_tracks and isinstance(doc.get("tracks"), list):
        track_objs = []
        for t_id in doc["tracks"]:
            try:
                track = get_track_by_id(str(t_id))
                if track:
                    track_objs.append(track)
            except Exception as e:
                logging.warning(f"⚠️ Error cargando track {t_id}: {e}")
        playlist["tracks"] = track_objs

    return playlist

# ============================================================
# 🔹 Obtener todas las playlists
# ============================================================
def get_all_playlists(limit: int = 50) -> List[dict]:
    """Devuelve una lista de playlists sin expandir tracks."""
    try:
        cursor = PLAYLISTS_COLLECTION.find().sort("created_at", -1).limit(limit)
        playlists = [serialize_playlist(doc, include_tracks=False) for doc in cursor]
        logging.info(f"📜 Se obtuvieron {len(playlists)} playlists del sistema.")
        return playlists
    except Exception as e:
        logging.exception("❌ Error obteniendo playlists desde la base de datos.")
        return []

# ============================================================
# 🔹 Obtener playlist por ID
# ============================================================
def get_playlist_by_id(playlist_id: str, include_tracks: bool = True) -> Optional[dict]:
    """Busca una playlist por ObjectId."""
    try:
        obj_id = ObjectId(playlist_id)
    except InvalidId:
        logging.warning(f"⚠️ ID de playlist inválido recibido: {playlist_id}")
        return None

    doc = PLAYLISTS_COLLECTION.find_one({"_id": obj_id})
    if not doc:
        logging.info(f"❌ Playlist no encontrada con ID {playlist_id}")
        return None

    return serialize_playlist(doc, include_tracks)

# ============================================================
# 🔹 Obtener playlist por nombre
# ============================================================
def get_playlist_by_name(name: str) -> Optional[dict]:
    """Busca una playlist por nombre (case-insensitive)."""
    try:
        doc = PLAYLISTS_COLLECTION.find_one(
            {"name": {"$regex": f"^{name}$", "$options": "i"}}
        )
        if not doc:
            logging.info(f"❌ Playlist no encontrada: {name}")
            return None
        return serialize_playlist(doc)
    except Exception as e:
        logging.exception(f"❌ Error buscando playlist por nombre: {name}")
        return None

# ============================================================
# 🔹 Crear playlist
# ============================================================
def create_playlist(name: str, description: str, tracks: list) -> str:
    """Crea una nueva playlist con metadatos y lista de tracks."""
    # Normalizar IDs (por seguridad)
    normalized_tracks = [str(t.get("id", t)) if isinstance(t, dict) else str(t) for t in (tracks or [])]

    playlist_doc = {
        "name": (name or "Playlist generada").strip(),
        "description": description or "Generada automáticamente",
        "tracks": normalized_tracks,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "total_tracks": len(normalized_tracks),
    }

    try:
        result = PLAYLISTS_COLLECTION.insert_one(playlist_doc)
        logging.info(f"✅ Playlist creada: {name} ({result.inserted_id})")
        return str(result.inserted_id)
    except Exception as e:
        logging.exception("❌ Error creando playlist en MongoDB.")
        raise e

# ============================================================
# 🔹 Actualizar playlist existente
# ============================================================
def update_playlist(playlist_id: str, update_data: Dict[str, Any]) -> bool:
    """Actualiza nombre, descripción o tracks."""
    try:
        obj_id = ObjectId(playlist_id)
    except InvalidId:
        logging.warning(f"⚠️ ID inválido para actualizar playlist: {playlist_id}")
        return False

    update_data["updated_at"] = datetime.utcnow().isoformat()
    result = PLAYLISTS_COLLECTION.update_one({"_id": obj_id}, {"$set": update_data})
    if result.modified_count > 0:
        logging.info(f"📝 Playlist actualizada correctamente: {playlist_id}")
        return True

    logging.warning(f"⚠️ Playlist no actualizada (sin cambios o inexistente): {playlist_id}")
    return False

# ============================================================
# 🔹 Eliminar playlist
# ============================================================
def delete_playlist(playlist_id: str) -> bool:
    """Elimina una playlist por su ID."""
    try:
        obj_id = ObjectId(playlist_id)
    except InvalidId:
        logging.warning(f"⚠️ ID inválido para eliminar playlist: {playlist_id}")
        return False

    result = PLAYLISTS_COLLECTION.delete_one({"_id": obj_id})
    if result.deleted_count > 0:
        logging.info(f"🗑️ Playlist eliminada: {playlist_id}")
        return True

    logging.warning(f"⚠️ No se encontró playlist para eliminar: {playlist_id}")
    return False
