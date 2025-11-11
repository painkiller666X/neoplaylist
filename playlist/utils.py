import os
import re
import json
import uuid
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("playlist.utils")
logger.setLevel(logging.INFO)

# ============================================================
# 🧠 Extraer y reparar JSON desde texto (respuestas LLM)
# ============================================================

def extract_json_from_text(text: str) -> Optional[Dict]:
    """
    Intenta extraer y reparar un JSON embebido en texto (respuestas LLM).
    Devuelve un dict válido si tiene éxito, o None si no se puede reparar.

    Estrategia:
      1️⃣ Buscar bloques { ... } o [ ... ]
      2️⃣ Intentar json.loads directo
      3️⃣ Reparar comillas simples, comas finales y claves sin comillas
    """
    if not text or not isinstance(text, str):
        return None

    # Buscar bloque JSON
    possible = None
    match_obj = re.search(r"(\{[\s\S]*\})", text)
    if match_obj:
        possible = match_obj.group(1)
    else:
        match_arr = re.search(r"(\[[\s\S]*\])", text)
        if match_arr:
            possible = match_arr.group(1)

    if not possible:
        logger.debug("extract_json_from_text: No se detectó bloque JSON en texto.")
        return None

    # Intento directo
    try:
        parsed = json.loads(possible)
        return parsed
    except Exception:
        pass

    # Fase de reparación ligera
    s = possible
    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"(?<=[:\s])'([^']*)'(?=[,\}\]])", r'"\1"', s)
    s = s.replace("'", '"')
    s = re.sub(r",\s*([\]}])", r"\1", s)  # eliminar comas finales
    s = re.sub(r"(\w+):", r'"\1":', s)    # claves sin comillas

    # Intentar nuevamente
    try:
        parsed = json.loads(s)
        return parsed
    except Exception as e:
        logger.debug(f"extract_json_from_text: fallo reparando JSON ({e})")
        return None


# ============================================================
# 🧩 Obtención segura de listas
# ============================================================

def safe_get_list(d: Dict, key: str) -> List:
    """Devuelve lista si existe y es válida, o lista vacía."""
    if not isinstance(d, dict):
        return []
    v = d.get(key)
    return v if isinstance(v, list) else []


# ============================================================
# 🎵 Normalización de listas de tracks
# ============================================================

def normalize_tracks_list(raw: Any) -> List[str]:
    """
    Recibe una lista o texto con posibles IDs o strings tipo 'Artista - Canción'.
    Devuelve una lista de strings limpios.
    """
    if not raw:
        return []

    if isinstance(raw, list):
        return [str(r).strip() for r in raw if r]

    if isinstance(raw, str):
        # Intentar parsear JSON primero
        parsed = extract_json_from_text(raw)
        if isinstance(parsed, list):
            return [str(r).strip() for r in parsed if r]
        # Si no es JSON, dividir por líneas o comas
        parts = re.split(r"[\n,]+", raw)
        return [p.strip() for p in parts if p.strip()]

    return []


# ============================================================
# 💾 Guardar playlist como archivo M3U (actualizado)
# ============================================================

def save_m3u(tracks, filename="playlist.m3u"):
    """
    Guarda una lista de tracks en formato M3U.
    Cada track debe contener al menos: 'Titulo', 'Artista' y 'Ruta' o 'path'.
    Devuelve una tupla: (file_path, playlist_uuid)
    """
    try:
        playlist_uuid = str(uuid.uuid4())

        base_name = os.path.splitext(filename)[0]
        safe_name = re.sub(r"[^\w\s-]", "", base_name.lower()).strip().replace(" ", "_")
        final_name = f"{safe_name}_{playlist_uuid[:8]}.m3u"

        export_dir = "./m3u_exports"
        os.makedirs(export_dir, exist_ok=True)

        file_path = os.path.join(export_dir, final_name)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for track in tracks:
                title = track.get("Titulo") or track.get("title") or "Desconocido"
                artist = track.get("Artista") or track.get("artist") or "Desconocido"
                duration = track.get("Duracion_mmss") or track.get("duration") or 0
                file_path_entry = (
                    track.get("Ruta")
                    or track.get("file_path")
                    or track.get("path")
                    or ""
                )

                line_info = f"#EXTINF:{duration},{artist} - {title}\n"
                f.write(line_info)
                f.write(f"{file_path_entry}\n")

        logger.info(f"✅ Playlist exportada como {file_path}")
        return file_path, playlist_uuid

    except Exception as e:
        logger.error(f"❌ Error al guardar M3U: {e}")
        return None, None


# ============================================================
# ⚙️ Ajuste de límite dinámico
# ============================================================

def adjust_limit_based_on_complexity(user_prompt: str, base_limit: int, llm_analysis: dict) -> int:
    """
    Ajusta el límite según la complejidad del análisis semántico.
    Considera país, década, género, mood y artista.
    """
    complexity = 0
    for k in ["country", "decade", "genre", "mood"]:
        if llm_analysis.get(k):
            complexity += 1
    if llm_analysis.get("artist"):
        complexity += 2

    if complexity >= 3:
        return min(base_limit, 20)
    elif complexity == 2:
        return min(base_limit, 25)
    return base_limit
