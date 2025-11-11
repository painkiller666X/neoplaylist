# backend/playlist/postprocessing_utils.py
import random
import logging
from typing import List, Dict, Any

logger = logging.getLogger("playlist.postprocessing")

# ============================================================
# 🔹 Filtrar inconsistencias groseras en los tracks
# ============================================================

def filter_gross_incongruities(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Elimina registros con campos nulos o inconsistentes:
      - Sin título o artista
      - Repetidos exactos
      - Duplicados de ID
    """
    if not tracks:
        return []

    clean = []
    seen_ids = set()
    seen_combo = set()

    for t in tracks:
        tid = str(t.get("id", "")).strip()
        artist = (t.get("artist") or "").strip()
        title = (t.get("title") or "").strip()

        if not artist or not title:
            continue
        combo = f"{artist.lower()}_{title.lower()}"
        if combo in seen_combo or tid in seen_ids:
            continue

        seen_ids.add(tid)
        seen_combo.add(combo)
        clean.append(t)

    logger.info(f"🧹 Filtrado de incongruencias: {len(clean)} válidos de {len(tracks)} originales.")
    return clean


# ============================================================
# 🔹 Limitar número de canciones por artista y álbum
# ============================================================

def limit_tracks_by_artist_album(tracks: List[Dict[str, Any]], max_per_artist: int = 3, max_per_album: int = 2) -> List[Dict[str, Any]]:
    """
    Aplica límite configurable de canciones por artista y álbum.
    Evita saturar playlists con un mismo artista o álbum.
    """
    if not tracks:
        return []

    filtered = []
    artist_counts = {}
    album_counts = {}

    for t in tracks:
        artist = (t.get("artist") or "Desconocido").lower()
        album = (t.get("album") or "Desconocido").lower()

        if artist_counts.get(artist, 0) >= max_per_artist:
            continue
        if album_counts.get(album, 0) >= max_per_album:
            continue

        filtered.append(t)
        artist_counts[artist] = artist_counts.get(artist, 0) + 1
        album_counts[album] = album_counts.get(album, 0) + 1

    logger.info(f"🎛️ Limitado a {len(filtered)} tracks (por artista/álbum).")
    return filtered


# ============================================================
# 🔹 Fallback flexible si hay pocos resultados
# ============================================================

def flexible_fallback_selection(all_tracks: List[Dict[str, Any]], existing: List[Dict[str, Any]], target_count: int = 40) -> List[Dict[str, Any]]:
    """
    Si hay menos resultados que el mínimo requerido, completa
    con canciones aleatorias de la base, sin repetir artistas.
    """
    if not all_tracks:
        return existing

    existing_ids = {t.get("id") for t in existing}
    existing_artists = {t.get("artist") for t in existing}

    candidates = [
        t for t in all_tracks
        if t.get("id") not in existing_ids and t.get("artist") not in existing_artists
    ]

    if not candidates:
        return existing

    need = max(0, target_count - len(existing))
    supplement = random.sample(candidates, min(need, len(candidates)))
    logger.info(f"🎲 Fallback añadió {len(supplement)} canciones.")
    return existing + supplement


# ============================================================
# 🔹 Aplicar límites + fallback final
# ============================================================

def apply_limits_and_fallback(
    tracks: List[Dict[str, Any]],
    all_tracks: List[Dict[str, Any]],
    max_total: int = 40,
    max_per_artist: int = 3,
    max_per_album: int = 2
) -> List[Dict[str, Any]]:
    """
    Orquesta los pasos finales del postprocesamiento:
      1️⃣ Filtra duplicados y vacíos
      2️⃣ Aplica límites por artista/álbum
      3️⃣ Fallback aleatorio si hay pocos resultados
    """
    logger.info("🧩 Aplicando postprocesamiento final de playlist...")

    clean = filter_gross_incongruities(tracks)
    limited = limit_tracks_by_artist_album(clean, max_per_artist, max_per_album)

    if len(limited) < max_total:
        limited = flexible_fallback_selection(all_tracks, limited, max_total)

    final = limited[:max_total]
    logger.info(f"✅ Playlist final lista con {len(final)} tracks.")
    return final

def extract_validated_tracks(result3: any, local_tracks: list, limit: int) -> list:
    """Extrae y valida pistas tras la fase 3 de validación."""
    validated = []
    if isinstance(result3, dict):
        validated = result3.get("suggestions", []) or local_tracks
    elif isinstance(result3, list):
        validated = result3
    else:
        validated = local_tracks

    if not validated or len(validated) < limit:
        validated = validated or local_tracks
        additional = [t for t in local_tracks if t not in validated]
        validated.extend(additional[:limit - len(validated)])

    return validated[:limit]
