import re, time, logging
from playlist.services import apply_intelligent_postprocessing, finalize_enhanced_response
from database.connection import music_db

tracks_col = music_db.tracks
logger = logging.getLogger("playlist.fallbacks")


def emergency_fallback(user_prompt: str, limit: int, start_time: float, error_msg: str):
    """Fallback de emergencia cuando falla el ciclo principal."""
    logger.warning(f"🆘 Activando fallback de emergencia: {error_msg}")

    try:
        words = [w for w in re.split(r"\W+", user_prompt.lower()) if len(w) > 3]
        if words:
            regex_or = [{"Genero": {"$regex": w, "$options": "i"}} for w in words] + \
                       [{"Titulo": {"$regex": w, "$options": "i"}} for w in words] + \
                       [{"Artista": {"$regex": w, "$options": "i"}} for w in words]
            query = {"$or": regex_or}

            fallback_tracks = list(tracks_col.find(query).limit(limit * 2))
            processed = apply_intelligent_postprocessing(fallback_tracks, user_prompt, {}, limit)

            return finalize_enhanced_response(user_prompt, {"fallback": True, "error": error_msg},
                                              processed, 0, limit, start_time, None)
    except Exception as e:
        logger.error(f"💥 Fallback también falló: {e}")

    random_tracks = list(tracks_col.find().sort("PopularityScore", -1).limit(limit))
    return finalize_enhanced_response(user_prompt, {"emergency_fallback": True},
                                      random_tracks, 0, limit, start_time, None)
