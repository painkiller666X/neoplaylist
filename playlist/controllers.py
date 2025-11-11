# backend/playlist/controllers.py
from fastapi import HTTPException, Request
from bson import ObjectId

from repositories.playlist_repository import (
    get_playlist_by_id,
    get_all_playlists,
    get_playlist_by_name,
    create_playlist,
    PLAYLISTS_COLLECTION as playlists_col
)
from playlist.services import (
    hybrid_playlist_cycle_enhanced,
    get_global_max_values,
    compute_popularity,
    compute_relative_popularity_by_genre,
    deduplicate_tracks_by_title_keep_best,
    filter_gross_incongruities,
    apply_limits_and_fallback,
)
from playlist.intent_analysis import analyze_query_intent, enhance_region_detection
from playlist.popularity_utils import ensure_popularity_display, popularity_display
from playlist.utils import save_m3u
import re, json, math, logging
from datetime import datetime
from typing import List, Dict, Any
import os, re

# ============================================================
# 🔹 Configuración de logs
# ============================================================
os.makedirs("./logs", exist_ok=True)
logging.basicConfig(
    filename="./logs/playlist_activity.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("playlist.controllers")

# ============================================================
# 🔹 Listar todas las playlists
# ============================================================
def fetch_all_playlists():
    try:
        playlists = get_all_playlists()
        if not playlists:
            logger.warning("⚠️ No hay playlists disponibles en la base de datos.")
        return playlists
    except Exception as e:
        logger.exception("❌ Error listando playlists.")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# ============================================================
# 🔹 Obtener playlist por ID
# ============================================================
def fetch_playlist_by_id(playlist_id: str):
    try:
        playlist = get_playlist_by_id(playlist_id)
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist no encontrada.")
        return playlist
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Error al obtener playlist por ID: {playlist_id}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Obtener playlist por nombre
# ============================================================
def fetch_playlist_by_name(name: str):
    try:
        playlist = get_playlist_by_name(name)
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist no encontrada.")
        return playlist
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Error al obtener playlist por nombre: {name}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# 🔹 Generar playlist (IA / híbrida / heurística)
# ============================================================
def generate_playlist(payload: dict):
    """Crea una playlist con IA, heurística o modo híbrido según payload."""
    try:
        name = payload.get("name", "Playlist generada automáticamente")
        description = payload.get("description", "Generada mediante IA y criterios musicales")
        criteria = payload.get("criteria", {})
        prompt = payload.get("prompt") or payload.get("query")
        mode = payload.get("mode", "hybrid").lower()

        if prompt and "prompt" not in criteria:
            criteria["prompt"] = prompt
        if mode == "smart":
            criteria["smart"] = True

        generated_tracks = generate_playlist_service(criteria)
        if not generated_tracks:
            raise HTTPException(status_code=404, detail="No se pudieron generar resultados.")

        playlist_id = create_playlist(name, description, generated_tracks)

        logger.info(
            f"✅ Playlist generada -> {name} | modo={mode} | {len(generated_tracks)} tracks | ID={playlist_id}"
        )

        return {
            "message": f"✅ Playlist generada correctamente en modo {mode}",
            "id": playlist_id,
            "name": name,
            "description": description,
            "mode": mode,
            "total_tracks": len(generated_tracks),
            "tracks": generated_tracks,
            "created_at": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("❌ Error generando playlist automática")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# ============================================================
# 🔹 Registrar feedback del usuario
# ============================================================
def record_feedback_controller(payload: dict):
    """Guarda feedback de usuario sobre una playlist o track."""
    if not payload or not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload inválido o vacío")

    user_email = payload.get("user_email")
    if not user_email:
        raise HTTPException(status_code=400, detail="Falta user_email")

    try:
        fb_id = insert_feedback(payload)
        logger.info(f"✅ Feedback registrado para {user_email} -> ID: {fb_id}")
        return {"message": "Feedback registrado correctamente", "id": fb_id}
    except Exception as e:
        logger.exception("❌ Error registrando feedback en la base de datos.")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# ============================================================
# 🎸 Función auxiliar para manejar exclusiones
# ============================================================
def exclude_previous_tracks(tracks: list, excluded_titles: set, excluded_paths: set):
    """Elimina de la lista las pistas que ya estaban en una playlist previa."""
    if not excluded_titles and not excluded_paths:
        return tracks

    filtered = [
        t for t in tracks
        if (t.get("Titulo", "").strip().lower() not in excluded_titles)
        and (t.get("Ruta") not in excluded_paths)
    ]
    logger.debug(f"🧹 Filtradas {len(tracks) - len(filtered)} pistas repetidas de {len(tracks)}.")
    return filtered
    
# ============================================================
# 🔹 Consultar feedback de usuario
# ============================================================
def fetch_user_feedback(email: str):
    """Obtiene todos los feedbacks históricos de un usuario."""
    try:
        data = get_feedback_by_user(email)
        return {"user": email, "total": len(data), "feedback": data}
    except Exception as e:
        logger.exception("❌ Error al consultar feedbacks de usuario.")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# ============================================================
# 🔹 Controlador /query (núcleo compatible con monolítico V15)
# ============================================================
def query_controller(payload: dict, request: Request = None):
    """
    Replica el endpoint /query del monolítico (V15) lo más fiel posible,
    pero reutilizando las funciones modulares en playlist.services.
    """
    try:
        logger.info(f"🎧 Recibido payload: {payload}")

        # 1️⃣ Validación inicial del payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="El cuerpo debe ser JSON válido.")

        query_text = payload.get("query") or payload.get("prompt")
        regenerate = payload.get("regenerate", False)
        previous_playlist_id = payload.get("previous_playlist_id")
        if not query_text:
            raise HTTPException(status_code=400, detail="Falta campo 'query' o 'prompt'.")

        start_ts = datetime.utcnow()

        # 2️⃣ Autenticación del usuario (token en headers si se entregó request)
        user_email = "anonymous"
        try:
            auth_header = getattr(request, "headers", {}).get("Authorization") if request else None
            if auth_header and "Bearer" in auth_header:
                token = auth_header.replace("Bearer ", "").strip()
                user = playlists_col.database["users"].find_one({"session_token": token})
                if user:
                    user_email = user.get("email", "anonymous")
                    logger.debug(f"👤 Usuario autenticado: {user_email}")
        except Exception as e:
            logger.warning(f"⚠️ Error autenticando usuario: {e}")

        # 3️⃣ Excluir pistas previas si regenerate=True
        excluded_titles, excluded_paths = set(), set()
        if regenerate and previous_playlist_id:
            try:
                prev_doc = (
                    playlists_col.find_one({"_id": ObjectId(previous_playlist_id), "user_email": user_email})
                    or playlists_col.find_one({"playlist_uuid": previous_playlist_id, "user_email": user_email})
                )
                if prev_doc and "items" in prev_doc:
                    for it in prev_doc["items"]:
                        title = (it.get("Titulo") or it.get("title") or "").strip().lower()
                        path = it.get("Ruta") or it.get("ruta") or it.get("stream_url") or ""
                        if title:
                            excluded_titles.add(title)
                        if path:
                            excluded_paths.add(path)
                    logger.debug(f"🧹 Excluidas {len(excluded_titles)} pistas previas.")
            except Exception as e:
                logger.warning(f"⚠️ Error cargando playlist previa: {e}")

        # 4️⃣ Análisis semántico (Ollama vía services)
        llm_analysis = analyze_query_intent(query_text)
        llm_analysis = enhance_region_detection(llm_analysis, query_text)
        logger.info(f"🧠 Análisis semántico → {llm_analysis}")

        # Detectar límites y tipos (por defecto fiel al monolítico)
        detected_limit = llm_analysis.get("detected_limit", 40)
        intent_type = llm_analysis.get("type", "")
        country = llm_analysis.get("country")
        country_type = llm_analysis.get("country_type", None)
        artist = llm_analysis.get("artist")

        # -------------------------
        # 🌎 Modo: country / país
        # -------------------------
        if intent_type == "country_request" and country:
            logger.info(f"🌍 Generando playlist de país: {country}")
            tracks = emergency_country_search(country, llm_analysis.get("country_type", "origin"), limit=detected_limit)
            if regenerate:
                tracks = exclude_previous_tracks(tracks, excluded_titles, excluded_paths)

            # Normalizar géneros (evita listas) antes de calcular
            for t in tracks:
                g = t.get("Genero")
                if isinstance(g, list):
                    t["Genero"] = " ".join(map(str, g))

            global_max = get_global_max_values()
            for t in tracks:
                t["PopularityScore"] = compute_popularity(t, global_max)

            # preparar claves para compute_relative_popularity_by_genre
            for t in tracks:
                if "Genero" in t and "genre" not in t:
                    t["genre"] = t.get("Genero")
                if "popularity" not in t and "PopularityScore" in t:
                    t["popularity"] = t["PopularityScore"]

            enriched = compute_relative_popularity_by_genre(tracks)
            # map back to expected keys
            for t in enriched:
                t["RelativePopularityScore"] = round(t.get("relative_popularity", t.get("RelativePopularityScore", 0.0)), 4)

            # Ensure display strings (stars + numeric)
            try:
                ensure_popularity_display(enriched)
            except Exception:
                for t in enriched:
                    t["PopularityDisplay"] = popularity_display(t.get("RelativePopularityScore", 0.0))

            enriched.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
            final_tracks = enriched[:detected_limit]

            simplified = _simplify_tracks(final_tracks)
            m3u_path, playlist_uuid = save_m3u(simplified, f"pais_{country}")
            playlist_name = f"Música de {country}"

            playlists_col.insert_one({
                "query_original": query_text,
                "name": playlist_name,
                "items": simplified,
                "limit": detected_limit,
                "created_at": start_ts,
                "m3u_path": m3u_path,
                "playlist_uuid": playlist_uuid,
                "user_email": user_email,
                "type": "country",
            })

            return _build_response(query_text, playlist_name, simplified, m3u_path, playlist_uuid, user_email, llm_analysis)

        # -------------------------
        # 🎤 Modo: artista (best-of)
        # -------------------------
        if intent_type == "artist_request" and artist:
            logger.info(f"🎸 Generando playlist de artista: {artist}")
            tracks = get_best_of_artist(artist, limit=min(detected_limit, 50))
            if regenerate:
                tracks = exclude_previous_tracks(tracks, excluded_titles, excluded_paths)

            # Normalizar 'Genero'
            for t in tracks:
                g = t.get("Genero")
                if isinstance(g, list):
                    t["Genero"] = " ".join(map(str, g))
                if "genre" not in t:
                    t["genre"] = t.get("Genero")

            global_max = get_global_max_values()
            for t in tracks:
                t["PopularityScore"] = compute_popularity(t, global_max)
                if "popularity" not in t:
                    t["popularity"] = t.get("PopularityScore", 0)

            enriched = compute_relative_popularity_by_genre(tracks)
            for t in enriched:
                t["RelativePopularityScore"] = round(t.get("relative_popularity", t.get("RelativePopularityScore", 0.0)), 4)

            try:
                ensure_popularity_display(enriched)
            except Exception:
                for t in enriched:
                    t["PopularityDisplay"] = popularity_display(t.get("RelativePopularityScore", 0.0))

            enriched.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
            simplified = _simplify_tracks(enriched[:detected_limit])
            m3u_path, playlist_uuid = save_m3u(simplified, artist)
            playlist_name = f"Lo mejor de {artist}"

            playlists_col.insert_one({
                "query_original": query_text,
                "name": playlist_name,
                "items": simplified,
                "limit": detected_limit,
                "created_at": start_ts,
                "m3u_path": m3u_path,
                "playlist_uuid": playlist_uuid,
                "user_email": user_email,
                "type": "artist",
            })

            return _build_response(query_text, playlist_name, simplified, m3u_path, playlist_uuid, user_email, llm_analysis)

        # -------------------------
        # 🎧 Modo: similares
        # -------------------------
        if intent_type == "similar_to_request" and artist:
            logger.info(f"🎧 Buscando similares a {artist}")
            tracks = find_similar_artists(artist, limit=min(detected_limit * 2, 60))
            if regenerate:
                tracks = exclude_previous_tracks(tracks, excluded_titles, excluded_paths)

            for t in tracks:
                g = t.get("Genero")
                if isinstance(g, list):
                    t["Genero"] = " ".join(map(str, g))
                if "genre" not in t:
                    t["genre"] = t.get("Genero")

            global_max = get_global_max_values()
            for t in tracks:
                t["PopularityScore"] = compute_popularity(t, global_max)
                if "popularity" not in t:
                    t["popularity"] = t.get("PopularityScore", 0)

            deduped = deduplicate_tracks_by_title_keep_best(tracks)
            enriched = compute_relative_popularity_by_genre(deduped)
            for t in enriched:
                t["RelativePopularityScore"] = round(t.get("relative_popularity", t.get("RelativePopularityScore", 0.0)), 4)

            try:
                ensure_popularity_display(enriched)
            except Exception:
                for t in enriched:
                    t["PopularityDisplay"] = popularity_display(t.get("RelativePopularityScore", 0.0))

            enriched.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
            simplified = _simplify_tracks(enriched[:detected_limit])
            m3u_path, playlist_uuid = save_m3u(simplified, f"similares_a_{artist}")
            playlist_name = f"Similares a {artist}"

            playlists_col.insert_one({
                "query_original": query_text,
                "name": playlist_name,
                "items": simplified,
                "limit": detected_limit,
                "created_at": start_ts,
                "m3u_path": m3u_path,
                "playlist_uuid": playlist_uuid,
                "user_email": user_email,
                "type": "similar",
            })

            return _build_response(query_text, playlist_name, simplified, m3u_path, playlist_uuid, user_email, llm_analysis)

        # -------------------------
        # 🎶 Flujo estándar híbrido (IA + DB)
        # -------------------------
        logger.info("🎼 Ejecutando flujo híbrido estándar (IA + DB)")
        llm_raw = hybrid_playlist_cycle_enhanced(query_text, llm_analysis=llm_analysis) or {}
        logger.info(f"🔍 RESPUESTA BRUTA DE hybrid_playlist_cycle_enhanced: {list(llm_raw.keys())}")
        # ✅ BUSCAR PISTAS EN MÚLTIPLES CAMPOS POSIBLES
        results = []
        possible_track_fields = ["results", "playlist", "tracks", "items", "data"]

        for field in possible_track_fields:
            if field in llm_raw and isinstance(llm_raw[field], list):
                results = llm_raw[field]
                logger.info(f"✅ Encontradas {len(results)} pistas en campo: {field}")
                break

        if not results:
            logger.warning("⚠️ Sin resultados desde IA — aplicando fallback local.")
            # Fallback inmediato
            results = flexible_fallback_selection(query_text, limit=detected_limit)
            logger.info(f"🔄 FALLBACK: Usando {len(results)} pistas de fallback")

        if regenerate:
            results = exclude_previous_tracks(results, excluded_titles, excluded_paths)
            logger.info(f"🔄 REGENERATE: {len(results)} pistas después de excluir previas")
        
        results = llm_raw.get("results") or []
        if not results:
            logger.warning("⚠️ Sin resultados desde IA — aplicando fallback local.")

        if regenerate:
            results = exclude_previous_tracks(results, excluded_titles, excluded_paths)

        # Normalización de 'Genero' y copia a 'genre'
        for t in results:
            g = t.get("Genero")
            if isinstance(g, list):
                t["Genero"] = " ".join(map(str, g))
            if "genre" not in t:
                t["genre"] = t.get("Genero")

        global_max = get_global_max_values()
        for t in results:
            t["PopularityScore"] = compute_popularity(t, global_max)
            if "popularity" not in t:
                t["popularity"] = t.get("PopularityScore", 0)

        # compute relative popularity
        enriched = compute_relative_popularity_by_genre(results)
        for t in enriched:
            t["RelativePopularityScore"] = round(t.get("relative_popularity", t.get("RelativePopularityScore", 0.0)), 4)

        try:
            ensure_popularity_display(enriched)
        except Exception:
            for t in enriched:
                t["PopularityDisplay"] = popularity_display(t.get("RelativePopularityScore", 0.0))

        # cleaned / limits / fallback
        cleaned = filter_gross_incongruities(enriched, query_text)
        cleaned = apply_limits_and_fallback(cleaned, query_text, detected_limit)
        cleaned.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
        final_tracks = cleaned[:detected_limit]

        simplified = _simplify_tracks(final_tracks)
        safe_name = re.sub(r"[^\w\s-]", "", query_text.lower())[:50]
        m3u_path, playlist_uuid = save_m3u(simplified, safe_name)
        playlist_name = query_text[:60]

        playlists_col.insert_one({
            "query_original": query_text,
            "name": playlist_name,
            "items": simplified,
            "limit": detected_limit,
            "created_at": start_ts,
            "m3u_path": m3u_path,
            "playlist_uuid": playlist_uuid,
            "user_email": user_email,
            "type": "standard",
        })

        return _build_response(query_text, playlist_name, simplified, m3u_path, playlist_uuid, user_email, llm_analysis)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("❌ Error en query_controller mejorado")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 🔸 Helpers internos (simplificación y respuesta)
# ============================================================
def _simplify_tracks(tracks):
    """Convierte tracks a formato reducido para respuesta."""
    simplified = []
    for t in tracks:
        simplified.append({
            "Ruta": t.get("Ruta"),
            "Titulo": t.get("Titulo"),
            "Artista": t.get("Artista"),
            "Album": t.get("Album"),
            "Año": t.get("Año"),
            "Genero": t.get("Genero"),
            "Duracion_mmss": t.get("Duracion_mmss"),
            "Bitrate": t.get("Bitrate"),
            "Calidad": t.get("Calidad"),
            "CoverCarpeta": t.get("CoverCarpeta"),
            "RelativePopularityScore": round(t.get("RelativePopularityScore", 0.0), 3),
            "PopularityDisplay": popularity_display(t.get("RelativePopularityScore", 0.0)),
        })
    return simplified


def _build_response(query_text, playlist_name, simplified, m3u_path, playlist_uuid, user_email, llm_analysis):
    """Crea respuesta JSON idéntica al monolítico."""
    return {
        "query_original": query_text,
        "playlist_name": playlist_name,
        "criterio_orden": "RelativePopularityScore",
        "total": len(simplified),
        "playlist": simplified,
        "archivo_m3u": m3u_path,
        "playlist_uuid": playlist_uuid,
        "user_email": user_email,
        "debug_summary": {
            "llm_analysis": llm_analysis,
            "standard_mode": True,
            "excluded_count": 0,
        },
    }

def flexible_fallback_selection(original_query: str, limit: int = 30) -> List[Dict[str, Any]]:
    """
    Fallback robusto cuando no hay resultados del ciclo híbrido.
    """
    logger.warning(f"🆘 Activando fallback flexible para: '{original_query}'")
    
    try:
        from database.connection import music_db
        tracks_col = music_db["tracks"]
        
        words = [w for w in re.split(r"\W+", original_query.lower()) if len(w) > 3]
        if words:
            regex_or = [
                {"Genero": {"$regex": w, "$options": "i"}} for w in words
            ] + [
                {"Titulo": {"$regex": w, "$options": "i"}} for w in words
            ] + [
                {"Artista": {"$regex": w, "$options": "i"}} for w in words
            ]
            query = {"$or": regex_or}
            
            fallback_tracks = list(tracks_col.find(query).limit(limit * 2))
            logger.info(f"🔄 FALLBACK: Encontradas {len(fallback_tracks)} pistas")
            return fallback_tracks
        else:
            # Fallback a pistas populares
            popular_tracks = list(tracks_col.find().sort("PopularityScore", -1).limit(limit))
            logger.info(f"🔄 FALLBACK: Usando {len(popular_tracks)} pistas populares")
            return popular_tracks
            
    except Exception as e:
        logger.error(f"💥 Error en fallback flexible: {e}")
        return []