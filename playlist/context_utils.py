import logging
from typing import Dict, Any
from database.connection import music_db

logger = logging.getLogger("playlist.context")

tracks_col = music_db.tracks


def collect_enriched_context(max_artists: int = 80, max_genres: int = 50, max_decades: int = 10) -> Dict[str, Any]:
    """
    Recolecta contexto enriquecido desde la base de datos MongoDB.
    Incluye estadísticas globales de artistas, géneros y décadas.
    """
    try:
        # 📊 ARTISTAS MÁS POPULARES
        pipeline_artists = [
            {"$group": {"_id": "$Artista", "count": {"$sum": 1}, "avg_popularity": {"$avg": "$PopularityScore"},
                        "genres": {"$addToSet": "$Genero"}, "decades": {"$addToSet": "$Decada"}}},
            {"$sort": {"avg_popularity": -1, "count": -1}},
            {"$limit": max_artists}
        ]
        top_artists = list(tracks_col.aggregate(pipeline_artists))

        # 🎵 GÉNEROS MÁS COMUNES
        pipeline_genres = [
            {"$unwind": "$Genero"},
            {"$group": {"_id": "$Genero", "count": {"$sum": 1},
                        "artist_sample": {"$addToSet": "$Artista"},
                        "avg_tempo": {"$avg": "$TempoBPM"},
                        "avg_energy": {"$avg": "$EnergyRMS"}}},
            {"$sort": {"count": -1}},
            {"$limit": max_genres}
        ]
        top_genres = list(tracks_col.aggregate(pipeline_genres))

        # 🕰️ DÉCADAS DISPONIBLES
        pipeline_decades = [
            {"$group": {"_id": "$Decada", "count": {"$sum": 1}, "top_genres": {"$push": "$Genero"}}},
            {"$sort": {"count": -1}},
            {"$limit": max_decades}
        ]
        decades_info = list(tracks_col.aggregate(pipeline_decades))

        # 🎭 PATRONES EMOCIONALES
        emotional_patterns = {}
        for genre_doc in top_genres[:15]:
            genre = genre_doc["_id"]
            emotion_stats = tracks_col.aggregate([
                {"$match": {"Genero": genre}},
                {"$group": {"_id": "$EMO_Sound", "count": {"$sum": 1},
                            "avg_tempo": {"$avg": "$TempoBPM"},
                            "avg_energy": {"$avg": "$EnergyRMS"}}},
                {"$sort": {"count": -1}},
                {"$limit": 3}
            ])
            emotional_patterns[genre] = list(emotion_stats)

        # 🏆 ARTISTAS POR DÉCADA
        artists_by_decade = {}
        for d in decades_info:
            decade = d["_id"]
            artists_by_decade[decade] = tracks_col.distinct("Artista", {"Decada": decade})[:10]

        context = {
            "artists": [a["_id"] for a in top_artists],
            "artists_detailed": top_artists[:20],
            "genres": [g["_id"] for g in top_genres],
            "genres_detailed": top_genres[:15],
            "decades": [d["_id"] for d in decades_info],
            "decades_detailed": decades_info,
            "emotional_patterns": emotional_patterns,
            "artists_by_decade": artists_by_decade,
            "stats": {"total_artists": len(top_artists), "total_genres": len(top_genres), "total_decades": len(decades_info)}
        }

        logger.debug(f"🎯 Contexto enriquecido: {len(context['artists'])} artistas, {len(context['genres'])} géneros, {len(context['decades'])} décadas")
        return context
    except Exception as e:
        logger.warning(f"⚠️ Error obteniendo contexto enriquecido: {e}")
        return {"artists": [], "genres": [], "decades": []}
