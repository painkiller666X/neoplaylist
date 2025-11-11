from fastapi import APIRouter, HTTPException, Body
from playlist.controllers import (
    fetch_all_playlists,
    fetch_playlist_by_id,
    fetch_playlist_by_name,
    generate_playlist,
    record_feedback_controller,
    fetch_user_feedback,
    query_controller,
)
import logging

router = APIRouter()
LOG = logging.getLogger("playlist.routes")

# ============================================================
# 🔹 Listar todas las playlists
# ============================================================
@router.get("/", summary="Listar todas las playlists")
def list_playlists():
    LOG.info("📜 Solicitando lista de todas las playlists...")
    try:
        return fetch_all_playlists()
    except Exception as e:
        LOG.exception("❌ Error al listar playlists")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Obtener playlist por nombre
# ============================================================
@router.get("/by-name/{name}", summary="Obtener playlist por nombre")
def get_playlist_by_name_route(name: str):
    LOG.info(f"🔎 Buscando playlist por nombre: {name}")
    try:
        return fetch_playlist_by_name(name)
    except HTTPException as e:
        raise e
    except Exception as e:
        LOG.exception(f"❌ Error al obtener playlist por nombre {name}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Obtener playlist por ID
# ============================================================
@router.get("/{playlist_id}", summary="Obtener playlist por ID")
def get_playlist(playlist_id: str):
    LOG.info(f"🔎 Buscando playlist por ID: {playlist_id}")
    try:
        return fetch_playlist_by_id(playlist_id)
    except HTTPException as e:
        raise e
    except Exception as e:
        LOG.exception(f"❌ Error al obtener playlist ID {playlist_id}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Generar playlist (IA / híbrido / heurístico)
# ============================================================
@router.post("/generate", summary="Generar playlist automáticamente")
def generate_playlist_route(payload: dict = Body(...)):
    """
    Genera una playlist según modo:
    - "simple": filtrado local (genre, artist)
    - "hybrid": IA + DB
    - "smart": IA avanzada (motor interno)

    Ejemplo de payload:
    {
        "name": "Entrenamiento",
        "description": "Rock de los 90 para motivarse",
        "criteria": {"genre": "rock"},
        "prompt": "quiero canciones potentes para hacer ejercicio",
        "mode": "hybrid"
    }
    """
    LOG.info(f"🎧 Petición de generación de playlist -> {payload}")
    if not payload:
        raise HTTPException(status_code=400, detail="El cuerpo de la solicitud está vacío.")
    try:
        return generate_playlist(payload)
    except HTTPException as e:
        raise e
    except Exception as e:
        LOG.exception("❌ Error generando playlist automática.")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# ============================================================
# 🔹 Feedback del usuario (like / skip / dislike)
# ============================================================
@router.post("/feedback", summary="Registrar feedback de usuario")
def record_feedback_route(payload: dict = Body(...)):
    """
    Registra feedback del usuario sobre tracks o playlists.
    Ejemplo de payload:
    {
        "user_email": "user@example.com",
        "playlist_id": "66a8bcd9...",
        "feedback": [
            {"track_id": "66a8cdef...", "action": "like"},
            {"track_id": "66a8cdee...", "action": "skip"}
        ]
    }
    """
    LOG.info(f"💬 Feedback recibido -> {payload}")
    if not payload:
        raise HTTPException(status_code=400, detail="Payload vacío o inválido.")
    try:
        return record_feedback_controller(payload)
    except HTTPException as e:
        raise e
    except Exception as e:
        LOG.exception("❌ Error registrando feedback de usuario.")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Consultar feedback histórico del usuario
# ============================================================
@router.get("/feedback/{user_email}", summary="Obtener feedback histórico del usuario")
def get_user_feedback_route(user_email: str):
    """Devuelve los feedbacks registrados de un usuario."""
    LOG.info(f"📊 Consultando feedback de usuario: {user_email}")
    try:
        return fetch_user_feedback(user_email)
    except Exception as e:
        LOG.exception(f"❌ Error consultando feedback de {user_email}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Endpoint núcleo: /query  (IA híbrida -> playlist)
# ============================================================
@router.post("/query", summary="Generar lista desde prompt/criterios (endpoint núcleo)")
def query_route(payload: dict = Body(...)):
    LOG.info(f"🔎 /playlist/query payload: {payload}")
    if not payload:
        raise HTTPException(status_code=400, detail="El cuerpo de la solicitud está vacío.")
    try:
        return query_controller(payload)
    except HTTPException as e:
        raise e
    except Exception as e:
        LOG.exception("❌ Error en /playlist/query")
        raise HTTPException(status_code=500, detail=str(e))
