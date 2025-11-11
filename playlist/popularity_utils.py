import math
import logging
from typing import List, Dict, Any, Optional
from database.connection import music_db

logger = logging.getLogger("playlist.popularity")

# ============================================================
# 🔹 Helper seguro de normalización
# ============================================================
def norm_safe(value: float, max_value: float) -> float:
    """Normaliza un valor con protección contra división por cero."""
    try:
        return value / max_value if max_value > 0 else 0.0
    except Exception:
        return 0.0


# ============================================================
# 🔹 Obtener máximos globales (para normalización)
# ============================================================
def get_global_max_values() -> Dict[str, float]:
    """Obtiene los valores máximos globales de popularidad (para normalización)."""
    try:
        stats = music_db.tracks.aggregate([
            {
                "$group": {
                    "_id": None,
                    "max_playcount": {"$max": "$LastFMPlaycount"},
                    "max_listeners": {"$max": "$LastFMListeners"},
                    "max_youtube": {"$max": "$YouTubeViews"},
                }
            }
        ])
        doc = next(stats, {})
        return {
            "playcount": float(doc.get("max_playcount", 1.0)),
            "listeners": float(doc.get("max_listeners", 1.0)),
            "youtube": float(doc.get("max_youtube", 1.0)),
        }
    except Exception as e:
        logger.warning(f"⚠️ No se pudieron obtener máximos globales: {e}")
        return {"playcount": 1.0, "listeners": 1.0, "youtube": 1.0}


# ============================================================
# 🔹 Cálculo de popularidad global (ponderado)
# ============================================================
def compute_popularity(track: Dict[str, Any], global_max: Dict[str, float]) -> float:
    """
    Calcula un puntaje ponderado de popularidad (logarítmico).
    Pesos:
      - LastFMPlaycount → 50%
      - LastFMListeners → 30%
      - YouTubeViews    → 20%
    """
    try:
        play = norm_safe(math.log1p(float(track.get("LastFMPlaycount", 0))), math.log1p(global_max["playcount"]))
        listeners = norm_safe(math.log1p(float(track.get("LastFMListeners", 0))), math.log1p(global_max["listeners"]))
        youtube = norm_safe(math.log1p(float(track.get("YouTubeViews", 0))), math.log1p(global_max["youtube"]))

        score = (play * 0.5) + (listeners * 0.3) + (youtube * 0.2)
        return round(score, 4)
    except Exception as e:
        logger.debug(f"compute_popularity: error procesando track {track.get('title', '')}: {e}")
        return 0.0


# ============================================================
# 🔹 Popularidad relativa por género
# ============================================================
def compute_relative_popularity_by_genre(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normaliza los puntajes de popularidad dentro de cada género.
    Aplica logaritmo + curva perceptiva sqrt + piso mínimo (0.2).
    Soporta casos donde 'Genero' puede ser lista o string.
    """
    by_genre = {}

    for t in tracks:
        genero_val = t.get("Genero") or t.get("genre") or "Desconocido"

        # ✅ Si 'Genero' es lista, convertir a texto
        if isinstance(genero_val, list):
            genre = " / ".join(map(str, genero_val)).strip()
        else:
            genre = str(genero_val).strip() or "Desconocido"

        if "PopularityScore" not in t:
            global_max = get_global_max_values()
            t["PopularityScore"] = compute_popularity(t, global_max)

        # ✅ Asegurar que el género sea hashable (string)
        by_genre.setdefault(genre, []).append(t)

    result = []
    for genre, group in by_genre.items():
        if not group:
            continue
        scores = [t.get("PopularityScore", 0) for t in group]
        max_score = max(scores) if scores else 1
        for t in group:
            rel = norm_safe(t.get("PopularityScore", 0), max_score)
            rel_adj = math.sqrt(rel) * 0.8 + 0.2  # curva perceptiva suave
            t["RelativePopularityScore"] = round(rel_adj, 4)
            result.append(t)
        logger.debug(f"[{genre}] normalizados {len(group)} tracks (max={max_score:.3f})")
    return result


# ============================================================
# 🔹 Asegurar display de popularidad
# ============================================================
def ensure_popularity_display(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Garantiza que todos los tracks tengan un campo 'PopularityDisplay',
    incluso si no se pudo calcular el score.
    """
    for t in tracks:
        score = t.get("RelativePopularityScore") or t.get("PopularityScore") or 0
        t["PopularityDisplay"] = popularity_display(score)
    return tracks


# ============================================================
# 🔹 Representación visual de popularidad
# ============================================================
def popularity_display(score: Optional[float]) -> str:
    """
    Representa la popularidad con formato completo (idéntico al monolítico):
    - Escala 0–10 (un decimal)
    - Estrellas (★)
    - Etiqueta textual (Ícono, Estrella, Popular, Conocido, Emergente)
    """
    if score is None:
        return "N/A"

    try:
        # Asegurar rango [0, 1]
        score = max(0.0, min(1.0, float(score)))

        value_10 = round(score * 10, 1)
        stars_count = int(round(score * 5))
        stars = "★" * stars_count + "☆" * (5 - stars_count)

        if score >= 0.9:
            label = "Ícono"
        elif score >= 0.7:
            label = "Estrella"
        elif score >= 0.45:
            label = "Popular"
        elif score >= 0.25:
            label = "Conocido"
        else:
            label = "Emergente"

        return f"{value_10}/10 {stars} ({label})"

    except Exception:
        return "N/A"
