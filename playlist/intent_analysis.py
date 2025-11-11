import re
import json
import logging
from typing import Dict, Any, Optional

from playlist.ai_engine import run_local_llm
from playlist.hybrid_tools import extract_json_from_text

logger = logging.getLogger("playlist.intent")

# ============================================================
# 🌍 Detección de país / región
# ============================================================
COUNTRY_KEYWORDS = {
    "chile": ("Chile", "origin"),
    "argent": ("Argentina", "origin"),
    "mexic": ("México", "origin"),
    "colomb": ("Colombia", "origin"),
    "espa": ("España", "origin"),
    "per": ("Perú", "origin"),
    "usa": ("Estados Unidos", "origin"),
    "estad": ("Estados Unidos", "origin"),
    "brasil": ("Brasil", "origin"),
    "franc": ("Francia", "origin"),
}

POPULARITY_KEYWORDS = [
    "popular en", "más escuchado en", "top en", "tendencias en"
]

REGION_DEFINITIONS = {
    "latam": {"name": "Latinoamérica", "countries": ["Chile", "Argentina", "México", "Colombia", "Perú", "Brasil"]},
    "europa": {"name": "Europa", "countries": ["España", "Francia", "Alemania", "Italia", "Reino Unido"]},
    "norteamerica": {"name": "Norteamérica", "countries": ["Estados Unidos", "Canadá", "México"]},
}

# ============================================================
# 🧠 Funciones auxiliares
# ============================================================
def detect_country_intent(text: str) -> Dict[str, Any]:
    """
    Detecta país y tipo de filtro (origen o popularidad).
    """
    lower = text.lower()
    for key, (country, ctype) in COUNTRY_KEYWORDS.items():
        if key in lower:
            # Popularidad tiene prioridad si hay "popular en"
            if any(p in lower for p in POPULARITY_KEYWORDS):
                ctype = "popular_in"
            return {"has_country_intent": True, "country": country, "country_type": ctype}
    return {"has_country_intent": False, "country": None, "country_type": None}


def detect_region_from_query(text: str) -> Optional[str]:
    """Detecta regiones amplias (ej: 'música latina')."""
    lower = text.lower()
    if any(w in lower for w in ["latina", "latino", "latam", "iberoamerica"]):
        return "latam"
    if any(w in lower for w in ["europea", "europeo", "europa"]):
        return "europa"
    if any(w in lower for w in ["norteamericana", "usa", "estadounidense", "canadiense"]):
        return "norteamerica"
    return None


def extract_limit_directly(text: str) -> Optional[int]:
    """Extrae límites explícitos como 'top 10' o '20 canciones'."""
    m = re.search(r"(?:top\s*)?(\d{1,3})\s*(?:canciones|temas|tracks)?", text.lower())
    if m:
        try:
            n = int(m.group(1))
            return max(5, min(n, 100))
        except Exception:
            pass
    return None


def validate_and_normalize_limit(value, text: str) -> int:
    """Normaliza límite a rango 10-100."""
    try:
        n = int(value)
        return max(10, min(n, 100))
    except Exception:
        n2 = extract_limit_directly(text)
        return n2 if n2 else 30

# ============================================================
# 🧠 Fallback básico si falla el LLM
# ============================================================
def get_improved_fallback_analysis(text: str) -> Dict[str, Any]:
    """Fallback rápido si Ollama no responde correctamente."""
    lower = text.lower()
    genre = None
    decade = None
    year = None

    if "rock" in lower: genre = "rock"
    elif "pop" in lower: genre = "pop"
    elif "metal" in lower: genre = "metal"
    elif "electr" in lower: genre = "electrónica"
    elif "jazz" in lower: genre = "jazz"

    m = re.search(r"(19|20)\d{2}", lower)
    if m:
        year = int(m.group(0))
        decade = f"{year // 10}0s"

    country_data = detect_country_intent(lower)
    return {
        "type": "fallback",
        "genre": genre,
        "decade": decade,
        "year": year,
        "country": country_data.get("country"),
        "country_type": country_data.get("country_type"),
        "limit": extract_limit_directly(text) or 30,
        "detected_limit": extract_limit_directly(text) or 30,
        "intent": "fallback_analysis"
    }

# ============================================================
# 🧭 Corrección de región si aplica
# ============================================================
def enhance_region_detection(analysis: Dict[str, Any], query_text: str) -> Dict[str, Any]:
    """Corrige o amplía el análisis si el texto apunta a una región."""
    detected_region = detect_region_from_query(query_text)
    if detected_region:
        region_info = REGION_DEFINITIONS[detected_region]
        analysis.update({
            "type": "region_request",
            "region": detected_region,
            "region_name": region_info["name"],
            "country": None,
            "country_type": None,
            "intent": f"Música de {region_info['name']}"
        })
        logger.debug(f"🗺️ Región detectada: {region_info['name']}")
    return analysis

# ============================================================
# 🧩 Análisis principal (usa LLM + fallback)
# ============================================================
def analyze_query_intent(query_text: str) -> Dict[str, Any]:
    """
    Interpreta el texto del usuario y extrae intención musical:
    género, década, país, límite, tipo de solicitud, etc.
    """
    country_info = detect_country_intent(query_text)
    prompt = f"""
Analiza esta solicitud musical y devuelve SOLO JSON con los campos:
{{
  "type": "artist_request|genre_or_mood_request|country_request",
  "artist": "",
  "track": "",
  "album": "",
  "genre": "",
  "mood": "",
  "decade": "",
  "year": null,
  "year_range": {{"from": 0, "to": 0}},
  "country": "",
  "country_type": "origin|popular_in",
  "limit": 10,
  "intent": "explicación resumida"
}}
Ejemplo: "rock de los 80s en Chile" → {{"genre": "rock", "decade": "1980s", "country": "Chile", "country_type": "origin"}}

Consulta: "{query_text}"
"""
    try:
        raw = run_local_llm(prompt)
        parsed = extract_json_from_text(raw) or {}
        if country_info["has_country_intent"]:
            parsed["country"] = country_info["country"]
            parsed["country_type"] = country_info["country_type"]
        parsed["detected_limit"] = validate_and_normalize_limit(parsed.get("limit"), query_text)
        return enhance_region_detection(parsed, query_text)
    except Exception as e:
        logger.warning(f"⚠️ Intent analysis failed: {e}")
        return get_improved_fallback_analysis(query_text)
