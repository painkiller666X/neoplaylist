import time
import urllib.parse
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("playlist.finalize")

# ============================================================
# 🧩 Utilidad: conversión de rutas locales a URLs accesibles
# ============================================================
def convert_path_to_url(local_path: Optional[str]) -> str:
    """
    Convierte una ruta local (ej: F:\\Musica\\A\\Artist\\file.flac)
    en una URL HTTP servible por FastAPI (ej: /media/...).
    """
    if not local_path:
        return ""
    try:
        path_fixed = local_path.replace("\\", "/")
        if path_fixed.lower().startswith("f:/musica/"):
            rel_path = path_fixed[9:]  # quitar "F:/Musica/"
            rel_path = urllib.parse.quote(rel_path)
            return f"http://localhost:8000/media/{rel_path}"
        return path_fixed
    except Exception as e:
        logger.warning(f"⚠️ Error convirtiendo ruta: {local_path} → {e}")
        return local_path or ""

# ============================================================
# 🧩 finalize_response: versión estándar
# ============================================================
def finalize_response(
    prompt: str,
    filters: Dict[str, Any],
    tracks: List[Dict[str, Any]],
    iterations: int,
    limit: int
) -> Dict[str, Any]:
    """
    Arma la respuesta final para el cliente (versión estándar).
    - Normaliza rutas locales a URLs accesibles.
    - Mantiene campos originales.
    - Igual a la versión del monolítico.
    """

    for t in tracks:
        ruta = t.get("Ruta")
        cover = t.get("CoverCarpeta")
        if ruta:
            t["StreamURL"] = convert_path_to_url(ruta)
        if cover:
            t["CoverURL"] = convert_path_to_url(cover)

    return {
        "prompt": prompt,
        "filters": filters,
        "limit": limit,
        "iterations": iterations,
        "total_found": len(tracks),
        "from_local": len(tracks),
        "playlist": tracks
    }

# ============================================================
# 🧩 finalize_enhanced_response: VERSIÓN COMPATIBLE CON CONTROLLER
# ============================================================
def finalize_enhanced_response(
    prompt: str,
    filters: Dict[str, Any],
    tracks: List[Dict[str, Any]],
    iterations: int,
    limit: int,
    start_time: float,
    llm_analysis: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Versión COMPATIBLE con el controlador - usa estructura esperada.
    """
    total_time = time.time() - start_time
    
    logger.info(f"🎯 FINALIZE: {len(tracks)} pistas recibidas, fase {iterations}")

    # DEBUG: Verificar pistas
    if tracks:
        logger.info(f"📋 PRIMERAS 3 PISTAS EN FINALIZE:")
        for i, track in enumerate(tracks[:3]):
            logger.info(f"   {i+1}. {track.get('Titulo', 'Sin título')} - {track.get('Artista', 'Sin artista')}")
    else:
        logger.warning("❌ FINALIZE: Lista de pistas VACÍA")

    # Enriquecer pistas con URLs (igual al monolítico)
    for t in tracks:
        ruta = t.get("Ruta")
        cover = t.get("CoverCarpeta")
        if ruta:
            t["StreamURL"] = convert_path_to_url(ruta)
        if cover:
            t["CoverURL"] = convert_path_to_url(cover)

    # ✅ ESTRUCTURA COMPATIBLE CON CONTROLLER
    response = {
        "query_original": prompt,  # ✅ Campo que espera el controller
        "playlist_name": prompt[:60],  # ✅ Campo que espera el controller  
        "results": tracks,  # ✅ CAMPO CRÍTICO: el controller busca "results"
        "total": len(tracks),  # ✅ Campo adicional útil
        "performance_metrics": {
            "total_time_seconds": round(total_time, 2),
            "tracks_per_second": round(len(tracks) / total_time, 2) if total_time > 0 else 0,
            "iterations": iterations,
            "phase": iterations
        },
        "filters_applied": filters,
        "limit_requested": limit
    }

    # Añadir análisis semántico si está disponible
    if llm_analysis:
        response["llm_analysis"] = llm_analysis

    logger.info(f"✅ FINALIZE: Respuesta construida con {len(tracks)} pistas en campo 'results'")
    return response