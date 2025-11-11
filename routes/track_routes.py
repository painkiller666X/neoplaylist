# backend/routes/track_routes.py
from fastapi import APIRouter, HTTPException
from models.track import Track
from repositories.track_repository import (
    create_track, get_track_by_id, get_all_tracks,
    update_track, delete_track
)
import logging

router = APIRouter()

# ------------------------------------------------------------
# 🔹 Crear track
# ------------------------------------------------------------
@router.post("/", summary="Agregar nuevo track")
def add_track(track: Track):
    track_id = create_track(track)
    return {"message": "Track creado correctamente", "id": track_id}

# ------------------------------------------------------------
# 🔹 Listar tracks
# ------------------------------------------------------------
@router.get("/", summary="Obtener todos los tracks")
def list_tracks():
    return get_all_tracks()

# ------------------------------------------------------------
# 🔹 Obtener track por ID
# ------------------------------------------------------------
@router.get("/{track_id}", summary="Obtener track por ID")
def get_track(track_id: str):
    track = get_track_by_id(track_id)
    if not track:
        raise HTTPException(status_code=404, detail="Track no encontrado")
    return track

# ------------------------------------------------------------
# 🔹 Actualizar track
# ------------------------------------------------------------
@router.put("/{track_id}", summary="Actualizar track")
def edit_track(track_id: str, track: Track):
    updated = update_track(track_id, track)
    if not updated:
        raise HTTPException(status_code=404, detail="Track no encontrado o sin cambios")
    return {"message": "Track actualizado correctamente"}

# ------------------------------------------------------------
# 🔹 Eliminar track
# ------------------------------------------------------------
@router.delete("/{track_id}", summary="Eliminar track")
def remove_track(track_id: str):
    deleted = delete_track(track_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Track no encontrado")
    return {"message": "Track eliminado correctamente"}
