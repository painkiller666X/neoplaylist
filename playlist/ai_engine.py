import os
import re
import json
import random
import logging
import requests
from typing import List, Dict, Any, Optional

from repositories.track_repository import get_all_tracks
from playlist.hybrid_tools import extract_json_from_text, log_hybrid_result

# ============================================================
# 🎧 Motor IA híbrido de generación de playlists (v2)
# ============================================================

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
MODEL_NAME = os.getenv("MODEL_NAME", "neoplaylist-agent")
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "40"))

# Configurar logs
logger = logging.getLogger("playlist.ai_engine")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ============================================================
# 🔹 Utilidades base
# ============================================================

def normalize_text(text: str) -> str:
    """Normaliza texto removiendo símbolos y pasando a minúsculas."""
    return re.sub(r"[^a-zA-Z0-9áéíóúñüÁÉÍÓÚÑÜ ]+", "", text or "").strip().lower()


def build_prompt_from_criteria(criteria: Dict[str, Any]) -> str:
    """Crea un prompt natural a partir de criterios estructurados."""
    prompt = "Genera una playlist musical con "
    parts = []
    if "genre" in criteria:
        parts.append(f"género {criteria['genre']}")
    if "artist" in criteria:
        parts.append(f"artistas similares a {criteria['artist']}")
    if "mood" in criteria:
        parts.append(f"estado de ánimo {criteria['mood']}")
    if "year" in criteria:
        parts.append(f"temas de la década de {criteria['year']}")
    if not parts:
        return "Genera una playlist variada y equilibrada de distintos estilos musicales."
    return prompt + ", ".join(parts) + "."


# ============================================================
# 🔹 Llamada robusta a Ollama
# ============================================================

def call_ollama(prompt: str, model: str = MODEL_NAME, temperature: float = 0.7) -> Optional[str]:
    """
    Envía un prompt al modelo Ollama y obtiene la respuesta limpia.
    Maneja fallos de conexión y timeouts.
    """
    try:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature}
        }
        logger.info(f"🧠 Enviando prompt a Ollama ({model}): {prompt[:120]}...")
        resp = requests.post(OLLAMA_URL, json=payload, timeout=45)
        resp.raise_for_status()

        data = resp.json()
        text = data.get("response") or data.get("message") or data.get("completion")
        if not text:
            logger.warning("⚠️ Ollama devolvió respuesta vacía o sin campo 'response'.")
            return None
        return text.strip()

    except requests.Timeout:
        logger.warning("⏰ Timeout al llamar a Ollama.")
    except Exception as e:
        logger.error(f"❌ Error en llamada a Ollama: {e}")

    return None


# ============================================================
# 🔹 Filtro heurístico avanzado
# ============================================================

def heuristic_filter(tracks: List[dict], criteria: Dict[str, Any]) -> List[dict]:
    """
    Aplica filtros heurísticos ponderados (género, artista, mood, año).
    Retorna los tracks con puntaje y ordenados por relevancia.
    """
    results = []
    for t in tracks:
        score = 0
        if "genre" in criteria and criteria["genre"].lower() in t.get("genre", "").lower():
            score += 3
        if "artist" in criteria and criteria["artist"].lower() in t.get("artist", "").lower():
            score += 4
        if "mood" in criteria and criteria["mood"].lower() in t.get("mood", "").lower():
            score += 2
        if "year" in criteria and str(criteria["year"]) in str(t.get("year", "")):
            score += 1
        if score > 0:
            t["score"] = score
            results.append(t)

    sorted_results = sorted(results, key=lambda x: x.get("score", 0), reverse=True)
    logger.info(f"🎯 {len(sorted_results)} tracks tras filtro heurístico.")
    return sorted_results


# ============================================================
# 🔹 IA híbrida: sugerir tracks con ayuda de Ollama
# ============================================================

def generate_smart_playlist(criteria: Dict[str, Any]) -> List[dict]:
    """
    Genera una playlist combinando heurística, razonamiento IA (Ollama) y fallback DB.
    """
    all_tracks = get_all_tracks()
    if not all_tracks:
        logger.warning("⚠️ No hay tracks en la base de datos.")
        return []

    # 1️⃣ Construir prompt
    prompt = criteria.get("prompt") or criteria.get("description") or build_prompt_from_criteria(criteria)
    logger.info(f"🧠 Prompt generado: {prompt}")

    # 2️⃣ Llamar a Ollama para sugerencias
    response_text = call_ollama(
        f"{prompt}\nResponde en formato JSON: {{'tracks': ['Artista - Canción', ...]}}"
    )

    parsed = extract_json_from_text(response_text)
    ai_names = []
    if isinstance(parsed, dict):
        ai_names = parsed.get("tracks") or parsed.get("songs") or []
    elif isinstance(parsed, list):
        ai_names = parsed
    elif isinstance(parsed, str):
        ai_names = [parsed]

    ai_names = [n for n in ai_names if isinstance(n, str) and n.strip()]

    # 3️⃣ Filtro heurístico local
    heuristic_matches = heuristic_filter(all_tracks, criteria)

    # 4️⃣ Vincular sugerencias IA con DB local
    ai_matched = []
    for suggestion in ai_names:
        s_norm = normalize_text(suggestion)
        for t in all_tracks:
            full_name = normalize_text(f"{t.get('artist','')} {t.get('title','')}")
            if s_norm and s_norm in full_name:
                ai_matched.append(t)
                break

    # 5️⃣ Combinar y deduplicar
    combined = {t.get("id") or str(t.get("_id")): t for t in heuristic_matches + ai_matched}.values()
    final_tracks = list(combined)[:MAX_RESULTS]

    # 6️⃣ Fallback si no hay resultados
    if not final_tracks:
        final_tracks = random.sample(all_tracks, min(10, len(all_tracks)))
        logger.info("🎲 Fallback activado: selección aleatoria.")

    # 7️⃣ Registrar resultado híbrido
    try:
        log_hybrid_result({
            "criteria": criteria,
            "prompt": prompt,
            "count": len(final_tracks),
            "matches_ai": len(ai_matched),
            "matches_heuristic": len(heuristic_matches),
        })
    except Exception as e:
        logger.debug(f"No se pudo registrar resultado híbrido: {e}")

    logger.info(f"✅ Playlist híbrida generada con {len(final_tracks)} tracks (IA+Heurística).")
    return final_tracks

# ============================================================
# 🧠 Función auxiliar: Ejecutar modelo LLM local (Ollama)
# ============================================================
def run_local_llm(prompt: str, model: str = MODEL_NAME, timeout: int = 40) -> str:
    """
    Envía un prompt al modelo local Ollama con manejo robusto de errores.
    Retorna texto limpio o JSON si se detecta estructura válida.
    """
    payload = {"model": model, "prompt": prompt, "stream": False}
    
    try:
        logger.info(f"🧠 Enviando prompt al modelo local ({model})")
        res = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
        res.raise_for_status()
        data = res.json()

        raw_text = data.get("response") or data.get("output") or data.get("text") or ""
        
        if raw_text:
            # Limpieza básica (remover delimitadores tipo ```json ... ```)
            cleaned = re.sub(r"^```json\s*", "", raw_text.strip())
            cleaned = re.sub(r"```\s*$", "", cleaned).strip()

            # Intentar parsear JSON
            parsed = extract_json_from_text(cleaned)
            if parsed:
                return parsed

            return cleaned

        logger.warning("⚠️ run_local_llm no devolvió texto")
        return "{}"
        
    except Exception as e:
        logger.error(f"❌ Error en run_local_llm: {e}")
        return "{}"