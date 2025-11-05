#!/usr/bin/env python3
# coding: utf-8
"""
NeoPlaylist API - refined V15
- Basada en V10 funcional
- Mejoras:
  * Parsing robusto de respuestas Ollama (maneja JSON mal formateado y listas en texto)
  * Ciclo híbrido DB-assisted más sólido (pide sugerencias, valida, pasa artistas locales, fallback directo)
  * Registro híbrido persistente (logs/hybrid_results_log.json)
  * Popularidad relativa por género
  * Filtros emocionales aplicados sólo si prompt contiene indicadores emocionales
  * Inspección final para eliminar incongruencias groseras
  * Mantiene dedupe, preferencia por bitrate, ranking, m3u, endpoints
"""

# ============================================================
# 🧩 IMPORTS LIMPIOS Y ORGANIZADOS
# ============================================================

# --- Librerías estándar ---
import os
import re
import json
import math
import time
import uuid
import logging
import urllib.parse
from datetime import datetime
from collections import Counter
from statistics import mean
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote_plus

# --- Librerías de terceros ---
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from bson import ObjectId
import uvicorn

# --- Módulos locales ---
from auth_beta2 import router as auth_router


load_dotenv()

# Codifica contraseña automáticamente
mongo_user = os.getenv("MONGO_USER", "NeoPlaylistUser")
mongo_pass = os.getenv("MONGO_PASS", "NeoUser123.!")
mongo_host = os.getenv("MONGO_HOST", "localhost:27017")
mongo_db_music = os.getenv("MONGO_DB_MUSIC", "musicdb")

# -----------------------
# Config
# -----------------------
#MONGO_URI_PL = os.getenv("MONGO_URI_PL", "mongodb://localhost:27017")

# Base de datos de usuarios / auth
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/?authSource=authdb")

#MONGO_URI = os.getenv("MONGO_URI", "mongodb://192.168.100.169:27017/?authSource=authdb")



# Base de datos musical
MONGO_URI_MUSIC = f"mongodb://{mongo_user}:{quote_plus(mongo_pass)}@{mongo_host}/{mongo_db_music}?authSource={mongo_db_music}"


MONGO_DB = os.getenv("MONGO_DB", "musicdb")

#OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
MODEL_NAME = os.getenv("MODEL_NAME", "neoplaylist-agent")
GENERATED_DIR = os.path.join(os.getcwd(), "generated_playlists")
LOGS_DIR = os.path.join(os.getcwd(), "logs")
HYBRID_LOG_PATH = os.path.join(LOGS_DIR, "hybrid_results_log.json")
os.makedirs(GENERATED_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# -----------------------
# Logging refinado (versión robusta)
# -----------------------
logger = logging.getLogger("neoplaylist_v15")

# Evita duplicar handlers al recargar
if not logger.hasHandlers():
    logger.setLevel(logging.DEBUG)

    # 🔸 Consola (solo INFO+)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    # 🔸 Archivo completo (DEBUG)
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(os.path.join("logs", "debug_full.log"), mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("🟣 [NeoPlaylist V15] Logging inicializado correctamente.")
    logger.debug("============================================")
    logger.debug(" Nuevo ciclo de ejecución iniciado")
    logger.debug("============================================")

# 🔇 Silencia log raíz de Uvicorn
logging.getLogger("uvicorn").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

# Mongo

# Conexiones separadas

client_music = MongoClient(MONGO_URI_MUSIC)
db_music = client_music[mongo_db_music]


client_auth = MongoClient(MONGO_URI)
db_auth = client_auth["authdb"]


tracks_col = db_music["tracks"]
playlists_col = db_music["playlists"]
feedback_col = db_music["playlist_feedback"]

app = FastAPI(title="NeoPlaylist API (V15 definitive)")

# ✅ CORS debe venir antes de los routers
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)




# Montar acceso estático a tu carpeta de música
app.mount("/media", StaticFiles(directory="F:\\Musica"), name="media")

# If you have an auth router file, include it here. It'll be optional.
# 🔐 Importar rutas de autenticación
try:
    app.include_router(auth_router)
    logger.debug("✅ auth_router cargado desde auth_beta2.py")
except ImportError:
    logger.debug("⚠️ No se encontró auth_beta2.py, continuando sin rutas de auth")


# -----------------------
# Campos permitidos y mapeos emocionales
# -----------------------
ALLOWED_FIELDS = {
    "Genero", "Año", "Decada", "TempoBPM", "EnergyRMS", "LoudnessLUFS",
    "SpectralCentroidHz", "CrestFactordB", "EMO_Context1", "EMO_Sound",
    "EMO_Lyrics", "PopularityScore", "LastFMPlaycount", "YouTubeViews",
    "LastFMListeners", "TopCountry1", "EstimatedKey", "Titulo", "Artista", "Album", "Idioma"
}
ALLOWED_OPERATORS = {"$gt", "$lt", "$gte", "$lte", "$in", "$regex", "$options", "$not", "$ne", "$exists", "$eq"}

# EMO maps (compact, extendible)
EMO_LYRICS_MAP = {
    "feliz": "Joy / Happy", "alegre": "Joy / Happy", "alegria": "Joy / Happy",
    "amor": "Love / Romantic", "romantico": "Love / Romantic", "romántico": "Love / Romantic",
    "triste": "Sadness", "melancol": "Sadness", "melancólico": "Sadness",
    "enojo": "Anger", "enojado": "Anger", "miedo": "Fear / Anxiety",
    "sorpresa": "Surprise / Wonder", "neutral": "Neutral / Storytelling",
    "desilusion": "Disappointment", "disappointment": "Disappointment",
    "curiosidad": "Curiosity", "confusion": "Confusion", "desaprobacion": "Disapproval",
    "deseo": "Desire", "desire": "Desire", "gracias": "Gratitude", "gratitud": "Gratitude",
    "superacion": "Joy / Happy", "superación": "Joy / Happy"
}

EMO_SOUND_MAP = {
    "energetic": "Energetic / Uplifting", "energética": "Energetic / Uplifting",
    "uplifting": "Energetic / Uplifting", "positivo": "Groovy / Positive", "positiva": "Groovy / Positive",
    "groovy": "Groovy / Positive", "bailable": "Groovy / Positive", "calm": "Calm / Neutral",
    "calma": "Calm / Neutral", "relaj": "Calm / Neutral", "sad": "Sad / Melancholic",
    "melancolic": "Sad / Melancholic", "melancólico": "Sad / Melancholic"
}

EMO_CONTEXT_FAMILIES = {
    "dolor": "Dolor y pérdida", "pérdida": "Dolor y pérdida", "desamor": "Dolor y pérdida",
    "soledad": "Dolor y pérdida", "nostalgia": "Dolor y pérdida",
    "amor": "Amor y deseo", "romance": "Amor y deseo",
    "traición": "Conflicto y traición", "venganza": "Conflicto y traición",
    "superación": "Superación y resiliencia", "resiliencia": "Superación y resiliencia",
    "fiesta": "Celebración y vida social", "baile": "Celebración y vida social",
    "amistad": "Celebración y vida social",
    "guerra": "Conflictos humanos", "protesta": "Conflictos humanos",
    "orgullo": "Orgullo y poder", "existencial": "Existencial / espiritual", "espiritual": "Existencial / espiritual"
}

# terms that indicate emotional intent in prompt (if present -> apply emotion filters)
EMOTION_INDICATORS = set(list(EMO_LYRICS_MAP.keys()) + list(EMO_SOUND_MAP.keys()) + list(EMO_CONTEXT_FAMILIES.keys()) + [
    "triste", "nostalg", "romant", "amor", "ira", "feliz", "alegr", "melancol", "enoj", "emocion", "emocional", "superación", "superacion"
])

# dance-like genres regex for quick checks
DANCE_GENRE_REGEX = re.compile(r"(dance|dancehall|disco|house|reggaeton|cumbia|salsa|merengue|funk|pop|electr[oó]nica|latina|tropical|afrobeat|samba|bachata)", re.I)
HEAVY_GENRE_REGEX = re.compile(r"(metal|heavy|hard rock|thrash|death metal|grindcore|metalcore|stoner|grunge)", re.I)


def parse_filters_from_llm(llm_filters: dict) -> dict:
    """
    Normaliza filtros que provienen del LLM con soporte para países, años específicos y décadas.
    Maneja diferentes formas en que el LLM puede expresar la temporalidad y ubicación.
    """
    if not llm_filters:
        return {}

    out = {}
    decada_detectada = None

    # ✅ NUEVO: Manejar filtros de país (ORIGEN vs POPULARIDAD)
    if "country" in llm_filters and llm_filters["country"]:
        country = llm_filters["country"]
        country_type = llm_filters.get("country_type", "origin")
        
        if country_type == "origin":
            # Filtro por país de origen del artista
            out["ArtistArea"] = {"$regex": f"^{re.escape(country)}$", "$options": "i"}
            logger.debug(f"🇨🇱 Filtro por país de origen: {country}")
        elif country_type == "popular_in":
            # Filtro por popularidad en el país (TopCountry1, TopCountry2, TopCountry3)
            out["$or"] = [
                {"TopCountry1": {"$regex": f"^{re.escape(country)}$", "$options": "i"}},
                {"TopCountry2": {"$regex": f"^{re.escape(country)}$", "$options": "i"}},
                {"TopCountry3": {"$regex": f"^{re.escape(country)}$", "$options": "i"}}
            ]
            logger.debug(f"🇨🇱 Filtro por popularidad en país: {country}")

    # ✅ MEJORADO: Distinguir entre AÑO ESPECÍFICO y DÉCADA
    # Prioridad: año específico > rango de años > década
    
    # 1️⃣ AÑO ESPECÍFICO (ej: "2015", "del 2018")
    if "year" in llm_filters and llm_filters["year"] is not None:
        year_val = llm_filters["year"]
        if isinstance(year_val, (int, float)) and 1950 <= year_val <= 2030:
            out["Año"] = {"$gte": int(year_val), "$lt": int(year_val) + 1}
            logger.debug(f"📅 Filtro por AÑO ESPECÍFICO: {year_val}")
            # Si hay año específico, NO aplicar década
            decada_detectada = None

    # 2️⃣ RANGO DE AÑOS ESPECÍFICOS (ej: "entre 2010 y 2015")
    elif "year_range" in llm_filters and isinstance(llm_filters["year_range"], dict):
        year_range = llm_filters["year_range"]
        if "from" in year_range and "to" in year_range:
            try:
                start_year = int(year_range["from"])
                end_year = int(year_range["to"])
                if 1950 <= start_year <= end_year <= 2030:
                    out["Año"] = {"$gte": start_year, "$lte": end_year}
                    logger.debug(f"📅 Filtro por RANGO DE AÑOS: {start_year}-{end_year}")
                    # Si hay rango de años, NO aplicar década
                    decada_detectada = None
            except (ValueError, TypeError):
                pass

    # 3️⃣ DÉCADA (solo si no hay año específico ni rango)
    elif "decada" in llm_filters or "década" in llm_filters or "decade" in llm_filters:
        decade_key = None
        for key in ["decada", "década", "decade"]:
            if key in llm_filters:
                decade_key = key
                break
        
        if decade_key:
            v = llm_filters[decade_key]
            
            # ✅ SOPORTE PARA LISTAS DE DÉCADAS (ej: "los 80 y 90")
            if isinstance(v, list):
                decade_ranges = []
                valid_decades = []
                for decade_str in v:
                    if isinstance(decade_str, str):
                        m = re.search(r"(\d{2,4})", decade_str)
                        if m:
                            yy = m.group(1)
                            if len(yy) == 2:
                                start = 1900 + int(yy)
                            else:
                                start = int(yy) if len(yy) == 4 else None
                            if start and 1950 <= start < 2030:
                                decade_ranges.append({"$gte": start, "$lt": start + 10})
                                valid_decades.append(decade_str)
                
                if decade_ranges:
                    # Crear condición OR para múltiples rangos de años
                    out["Año"] = {"$or": decade_ranges}
                    out["Decada"] = {"$in": valid_decades}
                    logger.debug(f"🕰️ MÚLTIPLES DÉCADAS aplicadas: {valid_decades}")
                    
            elif isinstance(v, str):
                # Procesamiento normal para década única
                m = re.search(r"(\d{2,4})", v)
                if m:
                    yy = m.group(1)
                    if len(yy) == 2:
                        start = 1900 + int(yy)
                    else:
                        start = int(yy) if len(yy) == 4 else None
                    if start and 1950 <= start < 2030:
                        out["Año"] = {"$gte": start, "$lt": start + 10}
                        decada_detectada = f"{start}s"
                        out["Decada"] = decada_detectada
                        logger.debug(f"🕰️ DÉCADA única aplicada: {decada_detectada}")
                        
            elif isinstance(v, dict):
                # Manejo de décadas en formato dict (compatibilidad)
                if "$gte" in v or "$gt" in v or "$lte" in v or "$lt" in v:
                    out["Año"] = {}
                    for op in ("$gte", "$gt", "$lte", "$lt"):
                        if op in v:
                            out["Año"][op] = v[op]
                    start = out["Año"].get("$gte")
                    if isinstance(start, (int, float)) and 1950 <= start < 2030:
                        decada_detectada = f"{int(start)//10}0s"
                        out["Decada"] = decada_detectada
                        logger.debug(f"🕰️ DÉCADA desde dict: {decada_detectada}")
                        
            elif isinstance(v, (int, float)) and 1950 <= v < 2030:
                start = int(v)
                out["Año"] = {"$gte": start, "$lt": start + 10}
                decada_detectada = f"{start}s"
                out["Decada"] = decada_detectada
                logger.debug(f"🕰️ DÉCADA desde número: {decada_detectada}")

    # 🔹 MANEJO TRADICIONAL del campo "Año" (para compatibilidad)
    if "Año" in llm_filters and "Año" not in out:
        v = llm_filters["Año"]
        if isinstance(v, dict):
            out["Año"] = v
            start = v.get("$gte") or v.get("$gt")
            if isinstance(start, (int, float)) and 1950 <= start < 2030:
                decada_detectada = f"{int(start)//10}0s"
                out["Decada"] = decada_detectada
        elif isinstance(v, (int, float)) and 1950 <= v < 2030:
            out["Año"] = {"$gte": int(v), "$lt": int(v) + 1}
            decada_detectada = f"{int(v)//10}0s"
            out["Decada"] = decada_detectada
        else:
            # Extraer años del string
            m = re.findall(r"\d{4}", str(v))
            if len(m) == 1:
                year_val = int(m[0])
                if 1950 <= year_val < 2030:
                    out["Año"] = {"$gte": year_val, "$lt": year_val + 1}
                    decada_detectada = f"{year_val//10}0s"
                    out["Decada"] = decada_detectada
            elif len(m) == 2:
                start_year, end_year = int(m[0]), int(m[1])
                if 1950 <= start_year <= end_year < 2030:
                    out["Año"] = {"$gte": start_year, "$lt": end_year + 1}
                    decada_detectada = f"{start_year//10}0s"
                    out["Decada"] = decada_detectada

    # 🔹 GÉNERO (evitar sesgos automáticos como "pop" para "música chilena")
    if "genero" in llm_filters or "género" in llm_filters or "genre" in llm_filters:
        genre_key = None
        for key in ["genero", "género", "genre"]:
            if key in llm_filters:
                genre_key = key
                break
        
        if genre_key:
            v = llm_filters[genre_key]
            # ✅ SOLO aplicar género si fue EXPLÍCITAMENTE solicitado
            # Evitar que el LLM añada géneros por su cuenta
            if v and v not in ["pop", "rock", "otros géneros comunes"]:  # Filtrar sugerencias automáticas
                if isinstance(v, str):
                    out["Genero"] = {"$regex": v, "$options": "i"}
                    logger.debug(f"🎵 Género aplicado (explícito): {v}")
                elif isinstance(v, list):
                    escaped = "|".join([re.escape(str(x)) for x in v if x])
                    if escaped:
                        out["Genero"] = {"$regex": f"({escaped})", "$options": "i"}
                        logger.debug(f"🎵 Múltiples géneros aplicados: {v}")
                elif isinstance(v, dict):
                    out["Genero"] = v

    # 🔹 CAMPOS EMOCIONALES (EMO_Sound, EMO_Lyrics, EMO_Context)
    for emo_field in ["EMO_Sound", "EMO_Lyrics", "EMO_Context1", "EMO_Context2", "EMO_Context3"]:
        if emo_field in llm_filters:
            v = llm_filters[emo_field]
            if isinstance(v, str):
                out[emo_field] = {"$regex": v, "$options": "i"}
            elif isinstance(v, dict):
                out[emo_field] = v

    # 🔹 OTROS CAMPOS PERMITIDOS
    allowed_fields = {
        "TempoBPM", "EnergyRMS", "LoudnessLUFS", "SpectralCentroidHz", 
        "CrestFactordB", "PopularityScore", "LastFMPlaycount", "YouTubeViews",
        "LastFMListeners", "EstimatedKey", "Titulo", "Artista", "Album", "Idioma"
    }
    
    for k, v in llm_filters.items():
        if k in allowed_fields and k not in out:
            if isinstance(v, str):
                out[k] = {"$regex": v, "$options": "i"}
            else:
                out[k] = v

    # 🔹 CONVERSIÓN FINAL: Si no hay año pero sí década detectada, convertir por compatibilidad
    if "Año" not in out and decada_detectada:
        try:
            start = int(decada_detectada[:4])
            if 1950 <= start < 2030:
                out["Año"] = {"$gte": start, "$lt": start + 10}
                logger.debug(f"🕰️ Década convertida a rango de años: {decada_detectada}")
        except Exception:
            pass

    # ✅ LOG FINAL DE FILTROS APLICADOS
    if out:
        logger.debug(f"🎯 FILTROS FINALES APLICADOS:")
        for key, value in out.items():
            if key == "$or":
                logger.debug(f"   ↳ {key}: [condiciones de país]")
            else:
                logger.debug(f"   ↳ {key}: {value}")
    else:
        logger.debug("🎯 No se aplicaron filtros específicos")

    return out


# -----------------------
# Helpers: LLM / sanitize / parse AI outputs robustly
# -----------------------
# =============================================================
# [V11.1+] Bloque de compatibilidad con nuevo Modelfile híbrido
# =============================================================


def parse_llm_response_v11_1(data: Any) -> Dict[str, Any]:
    """
    Interpreta el nuevo formato JSON híbrido:
    {
      "filters": {...},
      "suggestions": [...],
      "context_validation": {...},
      "sort_by": "...",
      "order": -1,
      "limit": 40
    }
    """
    if not data:
        return {"filters": {}, "suggestions": []}

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            try:
                m = re.search(r"(\{(?:.|\s)*\})", data)
                if m:
                    data = json.loads(m.group(1))
            except Exception:
                return {"filters": {}, "suggestions": []}

    filters = data.get("filters", {}) if isinstance(data.get("filters"), dict) else {}
    suggestions = data.get("suggestions", []) or []
    if not isinstance(suggestions, list):
        suggestions = []

    context_validation = data.get("context_validation", {})
    sort_by = data.get("sort_by")
    order = data.get("order", -1)
    limit = data.get("limit", 50)

    return {
        "filters": filters,
        "suggestions": suggestions,
        "context_validation": context_validation,
        "sort_by": sort_by,
        "order": order,
        "limit": limit
    }


def collect_local_context(max_artists: int = 100, max_genres: int = 60) -> Dict[str, List[str]]:
    """Obtiene artistas y géneros de la DB local para dar contexto al modelo."""
    try:
        artists = tracks.distinct("Artista")
        genres = tracks.distinct("Genero")
        artists = [a for a in artists if isinstance(a, str) and len(a.strip()) > 1][:max_artists]
        genres = [g for g in genres if isinstance(g, str) and len(g.strip()) > 1][:max_genres]
        return {"artists": artists, "genres": genres}
    except Exception as e:
        logger.debug(f"Error obteniendo contexto local: {e}")
        return {"artists": [], "genres": []}


def call_ollama_v11_1(prompt: str, context: Optional[Dict[str, List[str]]] = None, timeout: int = 40, max_retries: int = 2):
    """
    Envía prompt a Ollama (modelo neoplaylist-agent) con manejo robusto:
      - Soporta contexto local (artistas/géneros)
      - Timeouts y reintentos
      - Logs detallados del contenido y del JSON parseado
    """
    OLLAMA_URL = "http://localhost:11434/api/generate"
    MODEL_NAME = "neoplaylist-agent"

    if context:
        ctx_artists = ", ".join(context.get("artists", [])[:40])
        ctx_genres = ", ".join(context.get("genres", [])[:30])
        prompt = (
            f"{prompt}\n\n"
            f"--- CONTEXTO LOCAL ---\n"
            f"Artistas disponibles localmente:\n{ctx_artists}\n\n"
            f"Géneros locales:\n{ctx_genres}\n"
        )

    logging.info(f"🧠 Llamando a Ollama ({MODEL_NAME}) con timeout={timeout}s")
    payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False}

    for attempt in range(1, max_retries + 1):
        try:
            logging.debug(f"⚙️ Intento {attempt}/{max_retries} → prompt parcial: {prompt[:120]}...")
            r = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
            if r.status_code != 200:
                logging.warning(f"⚠️ Ollama devolvió {r.status_code}: {r.text}")
                continue

            raw_text = r.text.strip()
            logging.debug(f"🔍 Respuesta bruta ({len(raw_text)} bytes): {raw_text[:300]}")

            # Extraer bloque JSON válido
            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError:
                json_str = raw_text[raw_text.find("{"): raw_text.rfind("}") + 1]
                data = json.loads(json_str)

            if not isinstance(data, dict):
                logging.warning("⚠️ Respuesta no es JSON dict. Devolviendo vacío.")
                return {"filters": {}, "error": "respuesta malformada"}

            # Validación básica
            if "filters" not in data:
                logging.warning("⚠️ Falta campo 'filters' en respuesta Ollama.")
                data["filters"] = {}

            logging.info(f"✅ Ollama devolvió filtros: {list(data['filters'].keys())}")
            if "suggestions" in data:
                logging.info(f"💡 {len(data['suggestions'])} sugerencias híbridas incluidas.")
            if "context_validation" in data:
                logging.info(f"📘 Contexto validado por modelo: {data['context_validation']}")

            return data

        except requests.exceptions.Timeout:
            logging.error(f"⏳ Ollama no respondió en {timeout}s (intento {attempt}).")
        except requests.exceptions.ConnectionError:
            logging.error("🚫 No se pudo conectar a Ollama (verifica que esté ejecutándose).")
        except Exception as e:
            logging.exception(f"❌ Error inesperado llamando a Ollama: {e}")

    logging.error("❌ Todos los intentos de conexión fallaron. Se usará solo Mongo.")
    return {"filters": {}, "error": "Ollama no respondió"}


def hybrid_playlist_cycle(user_prompt: str, model="neoplaylist-agent", default_limit=30):
    """
    Ciclo híbrido de generación de playlist MEJORADO:
    1️⃣ Análisis de intención semántica
    2️⃣ Recomendaciones iniciales del modelo  
    3️⃣ Completitud con artistas locales si faltan resultados
    4️⃣ Validación y equilibrio final
    """

    logger.debug(f"🧠 Nueva consulta híbrida: {user_prompt}")

    # --- FASE 0: ANÁLISIS SEMÁNTICO MEJORADO ---
    llm_analysis = analyze_query_intent(user_prompt)
    detected_limit = llm_analysis.get("detected_limit", default_limit)
    actual_limit = min(detected_limit, 100)  # Límite máximo por seguridad
    limit = actual_limit
    
    # ✅ EXTRAER FILTROS DEL ANÁLISIS SEMÁNTICO
    semantic_filters = {}
    
    # Añadir género si está presente
    genre = llm_analysis.get("genre")
    if genre:
        semantic_filters["Genero"] = {"$regex": genre, "$options": "i"}
        logger.debug(f"🎵 Filtro semántico de género: {genre}")
    
    # Añadir década si está presente (CRÍTICO - esto es lo que falta)
    decade = llm_analysis.get("decade")
    if decade:
        semantic_filters["Decada"] = decade
        logger.debug(f"🕰️ Filtro semántico de década: {decade}")
        
        # También añadir rango de años para compatibilidad
        if decade == "1980s":
            semantic_filters["Año"] = {"$gte": 1980, "$lt": 1990}
        elif decade == "1990s":
            semantic_filters["Año"] = {"$gte": 1990, "$lt": 2000}
        elif decade == "2000s":
            semantic_filters["Año"] = {"$gte": 2000, "$lt": 2010}
        elif decade == "2010s":
            semantic_filters["Año"] = {"$gte": 2010, "$lt": 2020}
        elif decade == "2020s":
            semantic_filters["Año"] = {"$gte": 2020, "$lt": 2030}
    
    # Añadir mood/emoción si está presente
    mood = llm_analysis.get("mood")
    if mood:
        semantic_filters["EMO_Sound"] = {"$regex": mood, "$options": "i"}
        logger.debug(f"😊 Filtro semántico de mood: {mood}")

    logger.debug(f"🎯 Análisis semántico: {llm_analysis}")
    logger.debug(f"🔍 Filtros semánticos extraídos: {semantic_filters}")

    # --- FASE 1: Recomendaciones iniciales del modelo ---
    result = call_ollama_safe(user_prompt, model) or {}
    
    # ✅ DEBUG DETALLADO DE LA RESPUESTA
    if "error" in result:
        logger.warning(f"⚠️ Error en llamada a Ollama: {result['error']}")
    else:
        logger.debug(f"✅ Respuesta Ollama recibida, keys: {list(result.keys())}")
        
    llm_filters = result.get("filters", {}) or {}
    suggestions = result.get("suggestions", [])

    # --- COMBINAR FILTROS SEMÁNTICOS + FILTROS OLLAMA ---
    filters = parse_filters_from_llm(llm_filters)
    
    # ✅ COMBINAR CON FILTROS SEMÁNTICOS (los semánticos tienen prioridad)
    for key, value in semantic_filters.items():
        if key not in filters:  # Los filtros semánticos no sobrescriben los de Ollama
            filters[key] = value
            logger.debug(f"➕ Añadido filtro semántico: {key} = {value}")

    # 🔹 Normalizar campo temporal antes de aplicar filtro
    if "Año" in filters and isinstance(filters["Año"], dict):
        rango = filters["Año"]
        if "$gte" in rango and "$lt" in rango:
            start = rango["$gte"]
            if isinstance(start, (int, float)) and 1950 <= start < 2030:
                decada = f"{int(start)//10}0s"
                filters["Decada"] = decada
                logger.debug(f"🕰️ Convertido rango de años {rango} → Década '{decada}'")
        filters.pop("Año", None)
    elif "Año" in filters and isinstance(filters["Año"], str):
        if filters["Año"].endswith("s"):
            filters["Decada"] = filters.pop("Año")

    logger.debug(f"🔍 Filtros combinados finales: {filters}")

    # --- Buscar coincidencias locales flexibles ---
    local_tracks = search_tracks_with_emotional_filters(filters, limit, tracks_col)
    logger.debug(f"🎯 Fase 1: {len(local_tracks)} pistas encontradas / objetivo {limit}")

    if len(local_tracks) >= limit:
        return finalize_response(user_prompt, filters, local_tracks, 1, limit)

    # --- FASE 2: Completitud (faltan resultados) ---
    missing = limit - len(local_tracks)
    artists_local = list(tracks_col.distinct("Artista"))
    max_suggestions = min(30, missing * 3)

    completion_prompt = (
        f"Faltan resultados para completar la playlist del usuario.\n\n"
        f"Petición original: \"{user_prompt}\"\n\n"
        f"Análisis semántico: {json.dumps(llm_analysis, ensure_ascii=False, default=str)}\n\n"
        f"Filtros aplicados: {json.dumps(filters, ensure_ascii=False, default=str)}\n\n"
        f"A continuación, una lista de artistas disponibles localmente:\n"
        + ", ".join(artists_local[:30]) + ("\n..." if len(artists_local) > 30 else "\n")
        + f"\nProvee hasta {max_suggestions} sugerencias adicionales de canciones o artistas que encajen con la petición, "
        "manteniendo los mismos filtros de género, década, energía y emoción.\n"
        "Devuelve EXCLUSIVAMENTE JSON válido con formato:\n"
        "{\"suggestions\": [{\"titulo\": \"...\", \"artista\": \"...\", \"album\": \"...\"}]}\n"
        "Si no puedes sugerir nada coherente, devuelve {\"suggestions\": []}."
    )

    result2 = call_ollama_safe(completion_prompt, model) or {}
    suggestions2 = result2.get("suggestions", [])

    # 🔹 Mantener filtros previos si el modelo no devuelve nuevos
    filters = result2.get("filters") or filters or {}

    # 🔹 Reaplicar normalización temporal por seguridad
    if "Año" in filters and isinstance(filters["Año"], dict):
        rango = filters["Año"]
        if "$gte" in rango and "$lt" in rango:
            start = rango["$gte"]
            if isinstance(start, (int, float)) and 1950 <= start < 2030:
                decada = f"{int(start)//10}0s"
                filters["Decada"] = decada
                logger.debug(f"🕰️ Convertido rango de años {rango} → Década '{decada}'")
        filters.pop("Año", None)

    local_tracks2 = search_tracks_in_mongo(suggestions2, filters, missing, tracks_col)
    local_tracks += local_tracks2
    logger.debug(f"🎯 Fase 2: +{len(local_tracks2)} nuevas pistas → total {len(local_tracks)}")

    if len(local_tracks) >= limit:
        return finalize_response(user_prompt, filters, local_tracks, 2, limit)

    # --- FASE 3: Validación y equilibrio ---
    validation_prompt = (
        f"Valida y depura esta lista de {len(local_tracks)} pistas según el prompt original:\n"
        f"\"{user_prompt}\"\n\n"
        f"Análisis semántico: {json.dumps(llm_analysis, ensure_ascii=False, default=str)}\n\n"
        f"Filtros aplicados: {json.dumps(filters, ensure_ascii=False, default=str)}\n\n"
        "Elimina canciones incoherentes con el género, época, energía o emoción del prompt.\n"
        "Evita más del 20% de pistas por artista y máximo 2 del mismo álbum.\n"
        "Devuelve SOLO JSON válido en formato:\n"
        "{\"suggestions\": [{\"titulo\": \"...\", \"artista\": \"...\", \"album\": \"...\"}]}\n"
        "Si consideras que la lista ya es coherente, devuélvela igual."
    )

    validation_input = (
        validation_prompt
        + "\n\nLista de pistas actuales:\n"
        + json.dumps([{k: v for k, v in track.items() if k != '_id'} for track in local_tracks], 
                    ensure_ascii=False, default=str)
    )

    result3 = call_ollama_safe(validation_input, model) or {}

    # 🔹 Preservar filtros entre fases
        # SOLUCIÓN:
    if isinstance(result3, dict):
        filters = result3.get("filters") or filters or {}
    else:
        # Si result3 es una lista u otro tipo, mantener los filtros anteriores
        filters = filters or {}
        logger.warning(f"⚠️ Result3 no es dict, es {type(result3)}. Manteniendo filtros anteriores.")

    # 🔹 Revalidar campo temporal si reaparece "Año"
    if "Año" in filters and isinstance(filters["Año"], dict):
        rango = filters["Año"]
        if "$gte" in rango and "$lt" in rango:
            start = rango["$gte"]
            if isinstance(start, (int, float)) and 1950 <= start < 2030:
                decada = f"{int(start)//10}0s"
                filters["Decada"] = decada
                logger.debug(f"🕰️ Convertido rango de años {rango} → Década '{decada}'")
        filters.pop("Año", None)

    validated = result3.get("suggestions", [])
    if not validated:
        validated = local_tracks

    # Si eliminó demasiadas, rellenar con las previas coherentes
    if len(validated) < limit:
        validated += [t for t in local_tracks if t not in validated][:limit - len(validated)]

    logger.debug(f"✅ Fase 3 finalizada — total {len(validated[:limit])} pistas validadas")
    logger.debug(f"🕰️ Filtro temporal final aplicado: {filters.get('Decada')}")

    return finalize_response(user_prompt, filters, validated[:limit], 3, limit)



def search_tracks_in_mongo(suggestions, llm_filters, limit, tracks_col, user_prompt=None):
    """
    Busca sugerencias en Mongo combinando coincidencias flexibles (Titulo/Artista/Album)
    y los filtros normalizados del LLM.
    MEJORADO: Manejo robusto de filtros de década y país.
    """
    results = []
    seen_rutas = set()
    normalized_filters = parse_filters_from_llm(llm_filters or {})
    
    logger.debug(f"🔍 Buscando con {len(suggestions)} sugerencias y filtros: {normalized_filters}")

    # ✅ ESTRATEGIA 1: Búsqueda por sugerencias específicas (si existen)
    if suggestions:
        for s in suggestions:
            if len(results) >= limit:
                break

            titulo = (s.get("titulo") or "").strip()
            artista = (s.get("artista") or "").strip()
            album = (s.get("album") or "").strip()

            # Construir query
            and_clauses = []
            or_clauses = []

            if titulo:
                or_clauses.append({"Titulo": {"$regex": re.escape(titulo), "$options": "i"}})
            if artista:
                or_clauses.append({"Artista": {"$regex": re.escape(artista), "$options": "i"}})
            if album:
                or_clauses.append({"Album": {"$regex": re.escape(album), "$options": "i"}})

            if or_clauses:
                and_clauses.append({"$or": or_clauses})

            # Inyectar filtros LLM normalizados
            if normalized_filters:
                and_clauses.append(normalized_filters)

            # Si no hay condiciones (ni suggestion ni filtros), saltar
            if not and_clauses:
                continue

            query = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

            try:
                found = list(tracks_col.find(query).limit(5))  # buscar hasta 5 coincidencias por suggestion
            except Exception:
                logger.exception("Mongo find error in search_tracks_in_mongo")
                found = []

            for f in found:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break

    # ✅ ESTRATEGIA 2: Búsqueda DIRECTA por filtros (si no hay suficientes resultados)
    if len(results) < limit and normalized_filters:
        logger.debug(f"🎯 Pocos resultados ({len(results)}), buscando DIRECTAMENTE con filtros")
        
        try:
            # Buscar directamente con los filtros, ordenando por popularidad
            direct_query = normalized_filters
            
            # Agregar ordenamiento por popularidad
            direct_results = list(tracks_col.find(direct_query).sort("PopularityScore", -1).limit(limit * 2))
            
            for f in direct_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
                        
            logger.debug(f"🎯 Búsqueda directa añadió {len(direct_results)} pistas candidatas")
            
        except Exception as e:
            logger.debug(f"⚠️ Error en búsqueda directa por filtros: {e}")

    # ✅ ESTRATEGIA 3: Búsqueda por década específica si está en los filtros
    if len(results) < limit and "Decada" in normalized_filters:
        try:
            decade_query = {"Decada": normalized_filters["Decada"]}
            decade_results = list(tracks_col.find(decade_query).sort("PopularityScore", -1).limit(limit))
            
            for f in decade_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
                        
            logger.debug(f"🕰️ Búsqueda por década añadió {len(decade_results)} pistas")
            
        except Exception as e:
            logger.debug(f"⚠️ Error en búsqueda por década: {e}")

    # ✅ ESTRATEGIA 4: Búsqueda por palabras clave del prompt (fallback)
    if len(results) < limit and not suggestions and not normalized_filters and user_prompt:
        logger.debug("🔄 Usando búsqueda por palabras clave como fallback")
        
        # Extraer palabras clave del prompt
        words = [w for w in re.split(r"\W+", user_prompt) if len(w) > 3]
        if words:
            keyword_query = {
                "$or": [
                    {"Genero": {"$regex": w, "$options": "i"}} for w in words
                ] + [
                    {"Titulo": {"$regex": w, "$options": "i"}} for w in words
                ] + [
                    {"Artista": {"$regex": w, "$options": "i"}} for w in words
                ]
            }
            
            keyword_results = list(tracks_col.find(keyword_query).limit(limit))
            for f in keyword_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break

    logger.debug(f"🎯 search_tracks_in_mongo -> encontrados {len(results)} (limit {limit})")
    return results



def finalize_response(prompt, filters, tracks, iterations, limit):
    """
    Arma la respuesta final para el cliente.
    - Normaliza rutas locales a URLs accesibles desde el frontend.
    - Mantiene campos originales.
    """

    def convert_path_to_url(local_path: str) -> str:
        """Convierte ruta local (ej: F:\\Musica\\A\\Artist\\file.flac) a URL HTTP accesible."""
        if not local_path:
            return ""
        path_fixed = local_path.replace("\\", "/")
        if path_fixed.lower().startswith("f:/musica/"):
            rel_path = path_fixed[9:]  # quitar "F:/Musica/"
            rel_path = urllib.parse.quote(rel_path)
            return f"http://localhost:8000/media/{rel_path}"
        return local_path

    # Normalizar rutas de cada pista
    for t in tracks:
        ruta = t.get("Ruta")
        cover = t.get("CoverCarpeta")

        # Agregar URLs HTTP sin eliminar los originales
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


def attempt_json_repair(raw: str) -> dict:
    """Intenta reparar una salida JSON dañada del modelo."""

    logger.debug("🩹 Reparando JSON dañado desde Ollama...")
    cleaned = raw.strip()

    # Elimina contenido antes/después del bloque JSON
    cleaned = re.sub(r"^[^\[{]*", "", cleaned)
    cleaned = re.sub(r"[^\]}]*$", "", cleaned)

    # Reemplaza comillas rotas o comas sobrantes
    cleaned = cleaned.replace("`", '"')
    cleaned = cleaned.replace("“", '"').replace("”", '"')
    cleaned = re.sub(r",\s*}", "}", cleaned)
    cleaned = re.sub(r",\s*]", "]", cleaned)

    try:
        return json.loads(cleaned)
    except Exception as e:
        logger.error(f"💥 Reparación fallida: {e}")
        return {"filters": {}, "suggestions": []}


def build_mongo_only_response(prompt: str, limit: int) -> dict:
    """Fallback cuando Ollama no responde: intenta buscar algo útil solo con Mongo."""

    #client = MongoClient(MONGO_URI)
    #db = client[MONGO_DB]

    results = list(db.tracks.find({}, {"_id": 0}).limit(limit))
    return {
        "query": prompt,
        "filters": {},
        "limit": limit,
        "suggestions": results,
        "source": "mongo_only"
    }


def try_parse_json_from_text(text: str) -> Optional[Any]:
    """Try to extract a JSON object/array from messy text. Return Python object or None."""
    if not text or not isinstance(text, str):
        return None
    text = text.strip()
    # 1) direct json
    try:
        return json.loads(text)
    except Exception:
        pass
    # 2) find first {...} or [...] block
    m = re.search(r"(\{(?:.|\s)*\})", text)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    m = re.search(r"(\[(?:.|\s)*\])", text)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # 3) attempt to extract a "suggestions" array using regex
    m = re.search(r"\"?suggestions\"?\s*[:=]\s*(\[(?:.|\s)*\])", text, re.I)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # 4) fall back: extract quoted lines / bullet lines as list of strings
    items = []
    # split lines and try to parse "Title - Artist" or plain lines
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # skip lines that are obviously part of prompt/instructions
        if len(line) > 400:
            continue
        # remove numbering like "1)" or "- "
        line = re.sub(r"^\s*[\d\-\.\)]+\s*", "", line)
        # remove leading bullets
        line = re.sub(r"^[\-\*\•\u2022]\s*", "", line)
        # if line contains " - " or " — " or " – ", keep as single suggestion
        if re.search(r"\w\s*[-–—]\s*\w", line):
            items.append(line)
        else:
            # if line short, likely a title or artist
            if 2 <= len(line.split()) <= 8:
                items.append(line)
    if items:
        return items
    return None



# ================================================================
# NUEVAS FUNCIONES DE CONTROL DE LÍMITE POR ARTISTA Y ÁLBUM
# ================================================================
def limit_tracks_by_artist_album(
    tracks_list: List[Dict[str, Any]],
    max_per_artist: int = 20,
    max_per_album: int = 5
) -> List[Dict[str, Any]]:
    """Limita cantidad de pistas por artista y por álbum con logs detallados."""
    logger.debug(f"[LIMIT] Iniciando control de límite por artista ({max_per_artist}) y álbum ({max_per_album})")
    if not tracks_list:
        return []

    result = []
    artist_counts = {}
    album_counts = {}

    for t in sorted(tracks_list, key=lambda x: x.get("RelativePopularityScore", 0), reverse=True):
        artist = (t.get("Artista") or "").strip()
        album = (t.get("Album") or "").strip()
        artist_key = artist.lower()
        album_key = f"{artist.lower()}::{album.lower()}" if album else artist.lower()

        a_count = artist_counts.get(artist_key, 0)
        al_count = album_counts.get(album_key, 0)

        if a_count >= max_per_artist:
            #logger.debug(f"[FILTER] ❌ {artist} - {t.get('Titulo')} omitido: excede límite de {max_per_artist} por artista.")
            continue
        if al_count >= max_per_album:
            #logger.debug(f"[FILTER] ❌ {artist} - {t.get('Titulo')} omitido: excede límite de {max_per_album} por álbum ({album}).")
            continue

        result.append(t)
        artist_counts[artist_key] = a_count + 1
        album_counts[album_key] = al_count + 1
        #logger.debug(f"[INCLUDE] ✅ {artist} - {t.get('Titulo')} agregado (Artista:{artist_counts[artist_key]}, Álbum:{album_counts[album_key]})")

    logger.debug(f"[LIMIT] Playlist reducida de {len(tracks_list)} → {len(result)} tras aplicar límites.")
    return result

# ================================================================
# FALLBACK FLEXIBLE
# ================================================================
def flexible_fallback_selection(original_query: str, limit: int = 30) -> List[Dict[str, Any]]:
    """
    Si no hay resultados luego de aplicar filtros y límites, genera una
    búsqueda aproximada a partir de palabras clave del prompt.
    """
    logger.debug("[FALLBACK] Iniciando fallback flexible: búsqueda aproximada en la base local.")
    words = [w for w in re.split(r"\\W+", original_query.lower()) if len(w) > 3]
    regex_or = [{"Genero": {"$regex": w, "$options": "i"}} for w in words] + [{"Titulo": {"$regex": w, "$options": "i"}} for w in words]
    fallback_q = {"$or": regex_or}
    try:
        res = list(tracks_col.find(fallback_q).limit(limit))
        if res:
            logger.debug(f"[FALLBACK] {len(res)} resultados aproximados devueltos.")
        else:
            logger.debug("[FALLBACK] No se encontraron resultados en fallback.")
        return res
    except Exception as e:
        logger.exception(f"[FALLBACK] Error durante fallback flexible: {e}")
        return []

# ================================================================
# INTEGRACIÓN CON QUERY PRINCIPAL
# ================================================================
def apply_limits_and_fallback(results: List[Dict[str, Any]], query_text: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Aplica límites por artista/álbum y fallback flexible si queda vacía."""
    logger.debug("[APPLY] Iniciando postprocesamiento final (límite + fallback)")
    limited = limit_tracks_by_artist_album(results)
    if not limited:
        logger.debug("[APPLY] Playlist vacía tras límites → aplicando fallback flexible.")
        limited = flexible_fallback_selection(query_text, limit=limit)
    return limited[:limit]


def parse_ai_suggestions(ai_resp_raw: Any) -> List[str]:
    """
    Normalize AI response into a list of suggestion strings.
    Accepts dicts, lists, or raw text.
    """
    suggestions: List[str] = []
    if isinstance(ai_resp_raw, dict):
        # common keys
        for k in ("suggestions", "items", "results", "titles", "tracks"):
            val = ai_resp_raw.get(k)
            if isinstance(val, list):
                suggestions = [str(x).strip() for x in val if isinstance(x, (str, int)) and str(x).strip()]
                if suggestions:
                    return suggestions
        # maybe ai_resp_raw has nested text
        # try to stringify and parse
        raw_text = json.dumps(ai_resp_raw, ensure_ascii=False)
        parsed = try_parse_json_from_text(raw_text)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if isinstance(x, (str, int)) and str(x).strip()]
        if isinstance(parsed, dict):
            # attempt to find list inside
            for v in parsed.values():
                if isinstance(v, list):
                    return [str(x).strip() for x in v if isinstance(x, (str, int)) and str(x).strip()]
        # fallback: look for 'text' or 'response' fields
        for k in ("text", "response", "raw"):
            if k in ai_resp_raw and isinstance(ai_resp_raw[k], str):
                parsed2 = try_parse_json_from_text(ai_resp_raw[k])
                if isinstance(parsed2, list):
                    return [str(x).strip() for x in parsed2 if isinstance(x, (str, int)) and str(x).strip()]
                # else extract lines
                li = try_parse_json_from_text(ai_resp_raw[k]) if isinstance(ai_resp_raw[k], str) else None
    # if it's a list
    if isinstance(ai_resp_raw, list):
        return [str(x).strip() for x in ai_resp_raw if isinstance(x, (str, int)) and str(x).strip()]
    # if raw string
    if isinstance(ai_resp_raw, str):
        parsed = try_parse_json_from_text(ai_resp_raw)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if isinstance(x, (str, int)) and str(x).strip()]
        if isinstance(parsed, dict):
            # extract possible lists
            for v in parsed.values():
                if isinstance(v, list):
                    return [str(x).strip() for x in v if isinstance(x, (str, int)) and str(x).strip()]
        # else extract lines/bullets heuristically
        parsed_lines = try_parse_json_from_text(ai_resp_raw)
        if isinstance(parsed_lines, list):
            return [str(x).strip() for x in parsed_lines if isinstance(x, (str, int)) and str(x).strip()]
        # fallback: split lines and return best candidates
        items = []
        for line in ai_resp_raw.splitlines():
            line = line.strip()
            if not line:
                continue
            # remove numbering/bullets
            line = re.sub(r"^\s*[\d\-\.\)]+\s*", "", line)
            line = re.sub(r"^[\-\*\•\u2022]\s*", "", line)
            # ignore bracketed instructions
            if len(line) > 400:
                continue
            if len(line.split()) <= 1:
                continue
            items.append(line)
        return [i for i in items]
    # if dict-like but not captured, attempt stringify keys
    if isinstance(ai_resp_raw, dict):
        return []
    return suggestions

def call_ollama(prompt: str, model: str = MODEL_NAME, timeout: int = 40, retries: int = 2) -> Dict[str, Any]:
    """
    Llama al modelo Ollama con reintentos y parsing seguro de JSON.
    Usa la misma lógica robusta que call_ollama_safe.
    """
    # ✅ REUTILIZAR LA LÓGICA ROBUSTA DE call_ollama_safe
    result = call_ollama_safe(prompt, model, timeout)
    
    # Mantener compatibilidad con el formato de retorno original
    if "error" in result:
        return {"error": result["error"]}
    elif "raw_response" in result:
        return {"raw": result["raw_response"]}
    else:
        return result

def sanitize_filters(filters: Dict[str, Any]) -> Dict[str, Any]:
    safe = {}
    if not isinstance(filters, dict):
        return safe
    for k, v in filters.items():
        if k not in ALLOWED_FIELDS:
            continue
        if isinstance(v, dict):
            clean = {}
            for op, val in v.items():
                if op in ALLOWED_OPERATORS:
                    if isinstance(val, (str, int, float, list, bool, dict)):
                        clean[op] = val
            if clean:
                safe[k] = clean
        else:
            if isinstance(v, (str, int, float, list, bool)):
                safe[k] = v
    return safe

# -----------------------
# Popularidad: global y relativa por genero
# -----------------------
def get_global_max_values() -> Dict[str, float]:
    pipeline = [
        {"$group": {
            "_id": None,
            "max_playcount": {"$max": "$LastFMPlaycount"},
            "max_listeners": {"$max": "$LastFMListeners"},
            "max_views": {"$max": "$YouTubeViews"},
        }}
    ]
    try:
        res = list(tracks_col.aggregate(pipeline))
        if not res:
            return {"LastFMPlaycount": 1, "LastFMListeners": 1, "YouTubeViews": 1}
        r = res[0]
        return {
            "LastFMPlaycount": (r.get("max_playcount") or 1),
            "LastFMListeners": (r.get("max_listeners") or 1),
            "YouTubeViews": (r.get("max_views") or 1)
        }
    except Exception:
        return {"LastFMPlaycount": 1, "LastFMListeners": 1, "YouTubeViews": 1}

def normalize_field(value, max_value):
    try:
        return float(value) / float(max_value) if max_value else 0.0
    except Exception:
        return 0.0

def compute_popularity(track: Dict[str, Any], max_vals: Dict[str, float]) -> float:
    """
    Calcula un puntaje base de popularidad combinando métricas absolutas.
    Usa normalización proporcional y pondera por importancia:
      - LastFMPlaycount: 50%
      - LastFMListeners: 30%
      - YouTubeViews:    20%
    Se aplica logaritmo suavizado para evitar que grandes diferencias dominen.
    """
    # Normaliza con protección de división por cero
    def norm_safe(val, max_val):
        return (math.log1p(val) / math.log1p(max_val)) if max_val > 0 else 0.0

    playcount = norm_safe(track.get("LastFMPlaycount", 0), max_vals.get("LastFMPlaycount", 1))
    listeners = norm_safe(track.get("LastFMListeners", 0), max_vals.get("LastFMListeners", 1))
    views = norm_safe(track.get("YouTubeViews", 0), max_vals.get("YouTubeViews", 1))

    score = playcount * 0.5 + listeners * 0.3 + views * 0.2

    # refuerzo para temas con alto bitrate (calidad percibida)
    bitrate = track.get("Bitrate", 0) or 0
    if bitrate > 0:
        score *= 1 + min(0.1, math.log1p(bitrate / 1_000_000) / 20)  # hasta +10% de peso

    return round(score, 6)


def compute_relative_popularity_by_genre(tracks_list: List[Dict[str, Any]]) -> None:
    """
    Normaliza los puntajes de popularidad de una lista de canciones:
      - Aplica logaritmo para evitar compresión de valores altos.
      - Ajusta dentro de cada género (si hay suficientes muestras).
      - Evita penalizar canciones con alto valor absoluto aunque sean mínimas locales.
      - Aplica curva perceptiva (sqrt) y piso perceptivo (0.2).
    """
    if not tracks_list:
        return

    # 📊 Agrupar por género
    genre_buckets: Dict[str, List[float]] = {}
    for t in tracks_list:
        genres = t.get("Genero")
        if isinstance(genres, list) and genres:
            g = str(genres[0]).lower()
        elif isinstance(genres, str) and genres:
            g = genres.lower()
        else:
            g = "unknown"
        genre_buckets.setdefault(g, [])
        genre_buckets[g].append(t.get("PopularityScore", 0.0))

    # 📈 Estadísticas por género
    genre_stats: Dict[str, Dict[str, float]] = {}
    for g, scores in genre_buckets.items():
        if not scores:
            continue
        genre_stats[g] = {"count": len(scores), "min": min(scores), "max": max(scores)}

    # 🌍 Normalización global (log)
    all_scores = [max(0.0, t.get("PopularityScore", 0.0)) for t in tracks_list]
    log_scores = [math.log1p(s) for s in all_scores]
    global_min = min(log_scores) if log_scores else 0.0
    global_max = max(log_scores) if log_scores else 1.0
    if math.isclose(global_max, global_min):
        global_max = global_min + 1.0

    logger.debug(f"🎚️ Normalización global (log): min={global_min:.3f}, max={global_max:.3f}, total={len(all_scores)} tracks")

    # 🧮 Calcular puntuación relativa combinada
    for t in tracks_list:
        genres = t.get("Genero")
        if isinstance(genres, list) and genres:
            g = str(genres[0]).lower()
        elif isinstance(genres, str) and genres:
            g = genres.lower()
        else:
            g = "unknown"

        stats = genre_stats.get(g, {"count": 0, "min": 0.0, "max": 1.0})
        cnt = stats["count"]
        gmin, gmax = stats["min"], stats["max"]
        raw = max(0.0, t.get("PopularityScore", 0.0))
        raw_log = math.log1p(raw)

        # 🔹 Normalización global (logarítmica)
        norm_global = (raw_log - global_min) / (global_max - global_min)

        # 🔹 Normalización por género
        if math.isclose(gmax, gmin):
            norm_genre = 1.0
        else:
            norm_genre = (raw - gmin) / (gmax - gmin)

        # 🩹 Corrección: no castigar canciones con raw alto pero norm_genre bajo
        if norm_genre < 0.1 and raw > 0.6:
            norm_genre = 0.25 + 0.5 * norm_global  # se eleva según su peso global

        # 🧠 Peso adaptativo según tamaño del género
        alpha = min(0.95, 0.2 + 0.75 * (cnt / (cnt + 30)))
        combined = alpha * norm_genre + (1 - alpha) * norm_global

        # 🎚️ Curva perceptiva y piso mínimo
        combined = math.sqrt(combined)
        combined = 0.2 + 0.8 * combined

        t["RelativePopularityScore"] = round(combined, 6)
        '''
        logger.debug(
            f"   ↳ [{t.get('Artista','?')} - {t.get('Titulo','?')}] "
            f"raw={raw:.3f}, log={raw_log:.3f}, norm_genre={norm_genre:.3f}, "
            f"norm_global={norm_global:.3f}, combined={combined:.3f}"
        )
        '''

# -----------------------
# Heurísticas acústicas y EMO (con aplicación condicional)
# -----------------------
def enrich_filters_with_acoustics(text: str, filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte términos emocionales del prompt en filtros acústicos/emocionales específicos
    usando los valores exactos de tu sistema de análisis.
    """
    text_low = (text or "").lower()
    f = dict(filters)  # shallow copy

    # 🔥 MAPEO EXACTO usando tus valores reales
    emotional_acoustic_profiles = {
        # MÚSICA ALEGRE/FELIZ - usa "Joy / Happy" y "Energetic / Uplifting"
        "alegre": {
            "TempoBPM": {"$gte": 110, "$lte": 140},
            "EnergyRMS": {"$gte": 0.20},
            "EMO_Lyrics": "Joy / Happy",
            "EMO_Sound": "Energetic / Uplifting"
        },
        "feliz": {
            "TempoBPM": {"$gte": 100, "$lte": 135},
            "EnergyRMS": {"$gte": 0.18},
            "EMO_Lyrics": "Joy / Happy", 
            "EMO_Sound": "Energetic / Uplifting"
        },
        "contento": {
            "TempoBPM": {"$gte": 95, "$lte": 130},
            "EnergyRMS": {"$gte": 0.16},
            "EMO_Lyrics": "Joy / Happy",
            "EMO_Sound": "Groovy / Positive"
        },
        
        # MÚSICA BAILABLE/FIESTA - usa "Celebración y vida social"
        "bailable": {
            "TempoBPM": {"$gte": 115, "$lte": 130},
            "EnergyRMS": {"$gte": 0.22},
            "EMO_Sound": "Energetic / Uplifting",
            "EMO_Context1": "Celebración y vida social"
        },
        "fiesta": {
            "TempoBPM": {"$gte": 120, "$lte": 140},
            "EnergyRMS": {"$gte": 0.25},
            "EMO_Sound": "Energetic / Uplifting", 
            "EMO_Context1": "Celebración y vida social"
        },
        "baile": {
            "TempoBPM": {"$gte": 110, "$lte": 135},
            "EnergyRMS": {"$gte": 0.20},
            "EMO_Context1": "Celebración y vida social"
        },
        
        # MÚSICA ENERGÉTICA/INTENSA
        "energético": {
            "TempoBPM": {"$gte": 130},
            "EnergyRMS": {"$gte": 0.28},
            "EMO_Sound": "Energetic / Uplifting"
        },
        "intenso": {
            "TempoBPM": {"$gte": 140},
            "EnergyRMS": {"$gte": 0.30},
            "EMO_Sound": "Energetic / Uplifting"
        },
        "potente": {
            "TempoBPM": {"$gte": 125},
            "EnergyRMS": {"$gte": 0.26},
            "EMO_Sound": "Energetic / Uplifting"
        },
        
        # MÚSICA TRANQUILA/RELAJANTE - usa "Calm / Neutral"
        "tranquilo": {
            "TempoBPM": {"$lte": 100},
            "EnergyRMS": {"$lte": 0.15},
            "EMO_Sound": "Calm / Neutral"
        },
        "relajante": {
            "TempoBPM": {"$lte": 90},
            "EnergyRMS": {"$lte": 0.12},
            "EMO_Sound": "Calm / Neutral"
        },
        "calma": {
            "TempoBPM": {"$lte": 85},
            "EnergyRMS": {"$lte": 0.10},
            "EMO_Sound": "Calm / Neutral"
        },
        "suave": {
            "TempoBPM": {"$lte": 95},
            "EnergyRMS": {"$lte": 0.14},
            "EMO_Sound": "Calm / Neutral"
        },
        
        # MÚSICA TRISTE/MELANCÓLICA - usa "Sadness" y "Sad / Melancholic"
        "triste": {
            "TempoBPM": {"$lte": 80},
            "EnergyRMS": {"$lte": 0.12},
            "EMO_Lyrics": "Sadness",
            "EMO_Sound": "Sad / Melancholic"
        },
        "melancólico": {
            "TempoBPM": {"$lte": 75},
            "EnergyRMS": {"$lte": 0.10},
            "EMO_Lyrics": "Sadness",
            "EMO_Sound": "Sad / Melancholic"
        },
        "nostalgia": {
            "TempoBPM": {"$lte": 95},
            "EnergyRMS": {"$lte": 0.18},
            "EMO_Lyrics": "Sadness",
            "EMO_Context1": "Dolor y pérdida"
        },
        
        # MÚSICA ROMÁNTICA/AMOR - usa "Love / Romantic"
        "romántico": {
            "TempoBPM": {"$lte": 100},
            "EnergyRMS": {"$lte": 0.16},
            "EMO_Lyrics": "Love / Romantic",
            "EMO_Context1": "Amor y deseo"
        },
        "amor": {
            "TempoBPM": {"$lte": 110},
            "EnergyRMS": {"$lte": 0.20},
            "EMO_Lyrics": "Love / Romantic",
            "EMO_Context1": "Amor y deseo"
        },
        "pasión": {
            "TempoBPM": {"$lte": 105},
            "EnergyRMS": {"$lte": 0.22},
            "EMO_Lyrics": "Love / Romantic",
            "EMO_Context1": "Amor y deseo"
        },
        
        # MÚSICA CON ENFADO/CONFLICTO - usa "Anger"
        "enojo": {
            "TempoBPM": {"$gte": 120},
            "EnergyRMS": {"$gte": 0.24},
            "EMO_Lyrics": "Anger",
            "EMO_Context1": "Conflicto y traición"
        },
        "ira": {
            "TempoBPM": {"$gte": 130},
            "EnergyRMS": {"$gte": 0.28},
            "EMO_Lyrics": "Anger", 
            "EMO_Context1": "Conflicto y traición"
        },
        
        # MÚSICA DE SUPERACIÓN - usa "Superación y resiliencia"
        "superación": {
            "TempoBPM": {"$gte": 100, "$lte": 130},
            "EnergyRMS": {"$gte": 0.18},
            "EMO_Context1": "Superación y resiliencia"
        },
        "motivación": {
            "TempoBPM": {"$gte": 105, "$lte": 135},
            "EnergyRMS": {"$gte": 0.20},
            "EMO_Context1": "Superación y resiliencia"
        },
        
        # MÚSICA ESPIRITUAL/EXISTENCIAL
        "espiritual": {
            "TempoBPM": {"$lte": 95},
            "EnergyRMS": {"$lte": 0.16},
            "EMO_Context1": "Existencial / espiritual"
        },
        "existencial": {
            "TempoBPM": {"$lte": 90},
            "EnergyRMS": {"$lte": 0.14},
            "EMO_Context1": "Existencial / espiritual"
        }
    }

    # 🔍 DETECTAR Y APLICAR PERFIL EMOCIONAL
    applied_profile = None
    for emotion, profile in emotional_acoustic_profiles.items():
        if emotion in text_low:
            applied_profile = emotion
            logger.debug(f"🎭 Perfil emocional detectado: '{emotion}'")
            
            # Aplicar filtros del perfil (sin sobrescribir existentes)
            for field, value in profile.items():
                if field not in f:
                    f[field] = value
                    logger.debug(f"   🎵 {field} = {value}")
            break

    # 🎵 DETECCIÓN DE TÉRMINOS ACÚSTICOS ESPECÍFICOS
    # Rango de tempo explícito
    tempo_ranges = {
        "rápido": {"$gte": 130},
        "lento": {"$lte": 80},
        "medio": {"$gte": 90, "$lte": 120}
    }
    
    for tempo_term, tempo_range in tempo_ranges.items():
        if tempo_term in text_low and "TempoBPM" not in f:
            f["TempoBPM"] = tempo_range
            logger.debug(f"🎵 Rango de tempo '{tempo_term}' aplicado")

    # Niveles de energía
    if "alta energía" in text_low and "EnergyRMS" not in f:
        f["EnergyRMS"] = {"$gte": 0.25}
        logger.debug("⚡ Filtro de alta energía aplicado")
    elif "baja energía" in text_low and "EnergyRMS" not in f:
        f["EnergyRMS"] = {"$lte": 0.12}
        logger.debug("🌿 Filtro de baja energía aplicado")

    # 🔥 ESTRATEGIA INTELIGENTE: Si hay términos emocionales pero no perfil específico
    if not applied_profile and contains_emotion_indicator(text):
        logger.debug("🎨 Aplicando filtros emocionales básicos (fallback inteligente)")
        
        # Determinar dirección emocional general
        if any(w in text_low for w in ["alegre", "feliz", "fiesta", "baile", "celebración"]):
            # Dirección positiva/energética
            if "TempoBPM" not in f:
                f["TempoBPM"] = {"$gte": 100, "$lte": 135}
            if "EnergyRMS" not in f:
                f["EnergyRMS"] = {"$gte": 0.18}
            if "EMO_Sound" not in f:
                f["EMO_Sound"] = {"$in": ["Energetic / Uplifting", "Groovy / Positive"]}
                
        elif any(w in text_low for w in ["triste", "melancolía", "nostalgia", "dolor"]):
            # Dirección triste/calmada
            if "TempoBPM" not in f:
                f["TempoBPM"] = {"$lte": 95}
            if "EnergyRMS" not in f:
                f["EnergyRMS"] = {"$lte": 0.15}
            if "EMO_Sound" not in f:
                f["EMO_Sound"] = {"$in": ["Sad / Melancholic", "Calm / Neutral"]}
                
        elif any(w in text_low for w in ["amor", "romántico", "pasión"]):
            # Dirección romántica
            if "TempoBPM" not in f:
                f["TempoBPM"] = {"$lte": 110}
            if "EnergyRMS" not in f:
                f["EnergyRMS"] = {"$lte": 0.20}
            if "EMO_Lyrics" not in f:
                f["EMO_Lyrics"] = "Love / Romantic"

    return f

def contains_emotion_indicator(text: str) -> bool:
    """
    Detecta si el texto contiene indicadores emocionales usando tus categorías exactas.
    """
    if not text:
        return False
    
    text_low = text.lower()
    
    # Términos que mapean a tus categorías emocionales exactas
    emotion_indicators = [
        # Joy / Happy
        "alegre", "feliz", "contento", "alegría", "felicidad", "optimismo",
        # Love / Romantic  
        "amor", "romántico", "romance", "pasión", "corazón", "enamorado",
        # Sadness
        "triste", "tristeza", "melancolía", "melancólico", "dolor", "pena",
        # Anger
        "enojo", "ira", "enfado", "rabia", "furia", 
        # Fear / Anxiety
        "miedo", "temor", "ansiedad", "pánico",
        # Celebration
        "fiesta", "celebración", "baile", "juerga", "diversión",
        # Superación
        "superación", "motivación", "inspiración", "esperanza",
        # Spiritual
        "espiritual", "existencial", "fe", "religión", "destino"
    ]
    
    return any(term in text_low for term in emotion_indicators)
    
def search_tracks_with_emotional_filters(llm_filters, limit, tracks_col):
    """
    Búsqueda especializada para filtros emocionales usando valores exactos.
    Estrategia de fallback inteligente para máxima recuperación.
    """
    results = []
    seen_rutas = set()
    normalized_filters = parse_filters_from_llm(llm_filters or {})
    
    logger.debug(f"🎭 Buscando con filtros emocionales: {list(normalized_filters.keys())}")

    # ESTRATEGIA 1: Búsqueda exacta con todos los filtros (incluyendo emocionales)
    if normalized_filters:
        try:
            exact_query = normalized_filters
            exact_results = list(tracks_col.find(exact_query).sort("PopularityScore", -1).limit(limit * 3))
            
            for f in exact_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
            logger.debug(f"🎯 Estrategia 1 (exacta): {len(results)} resultados")
        except Exception as e:
            logger.debug(f"⚠️ Error en búsqueda exacta: {e}")

    # ESTRATEGIA 2: Relajar EMO_Context si hay pocos resultados
    emotional_context_fields = ["EMO_Context1", "EMO_Context2", "EMO_Context3"]
    if len(results) < limit and any(k in emotional_context_fields for k in normalized_filters.keys()):
        relaxed_filters = {k: v for k, v in normalized_filters.items() 
                          if k not in emotional_context_fields}
        
        if relaxed_filters:
            try:
                relaxed_results = list(tracks_col.find(relaxed_filters).sort("PopularityScore", -1).limit(limit * 2))
                for f in relaxed_results:
                    ruta = f.get("Ruta")
                    if ruta and ruta not in seen_rutas:
                        results.append(f)
                        seen_rutas.add(ruta)
                        if len(results) >= limit:
                            break
                logger.debug(f"🎯 Estrategia 2 (sin contextos): +{len(relaxed_results)} resultados")
            except Exception as e:
                logger.debug(f"⚠️ Error en búsqueda sin contextos: {e}")

    # ESTRATEGIA 3: Mantener solo EMO_Sound y EMO_Lyrics (más importantes)
    core_emotional_fields = ["EMO_Sound", "EMO_Lyrics"]
    if len(results) < limit and any(k in core_emotional_fields for k in normalized_filters.keys()):
        core_filters = {k: v for k, v in normalized_filters.items() 
                       if k in core_emotional_fields or k not in emotional_context_fields}
        
        # Añadir filtros acústicos si existen
        acoustic_fields = ["TempoBPM", "EnergyRMS", "LoudnessLUFS"]
        for field in acoustic_fields:
            if field in normalized_filters:
                core_filters[field] = normalized_filters[field]
        
        if core_filters:
            try:
                core_results = list(tracks_col.find(core_filters).sort("PopularityScore", -1).limit(limit * 2))
                for f in core_results:
                    ruta = f.get("Ruta")
                    if ruta and ruta not in seen_rutas:
                        results.append(f)
                        seen_rutas.add(ruta)
                        if len(results) >= limit:
                            break
                logger.debug(f"🎯 Estrategia 3 (solo emociones core): +{len(core_results)} resultados")
            except Exception as e:
                logger.debug(f"⚠️ Error en búsqueda core emocional: {e}")

    # ESTRATEGIA 4: Solo filtros acústicos (TempoBPM + EnergyRMS)
    if len(results) < limit and any(k in ["TempoBPM", "EnergyRMS"] for k in normalized_filters.keys()):
        acoustic_only = {}
        if "TempoBPM" in normalized_filters:
            acoustic_only["TempoBPM"] = normalized_filters["TempoBPM"]
        if "EnergyRMS" in normalized_filters:
            acoustic_only["EnergyRMS"] = normalized_filters["EnergyRMS"]
        
        if acoustic_only:
            try:
                acoustic_results = list(tracks_col.find(acoustic_only).sort("PopularityScore", -1).limit(limit * 2))
                for f in acoustic_results:
                    ruta = f.get("Ruta")
                    if ruta and ruta not in seen_rutas:
                        results.append(f)
                        seen_rutas.add(ruta)
                        if len(results) >= limit:
                            break
                logger.debug(f"🎯 Estrategia 4 (solo acústicos): +{len(acoustic_results)} resultados")
            except Exception as e:
                logger.debug(f"⚠️ Error en búsqueda acústica: {e}")

    # ESTRATEGIA 5: Solo por década + ordenar por popularidad
    if len(results) < limit and "Decada" in normalized_filters:
        try:
            decade_only = {"Decada": normalized_filters["Decada"]}
            decade_results = list(tracks_col.find(decade_only).sort("PopularityScore", -1).limit(limit))
            for f in decade_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
            logger.debug(f"🎯 Estrategia 5 (solo década): +{len(decade_results)} resultados")
        except Exception as e:
            logger.debug(f"⚠️ Error en búsqueda por década: {e}")

    logger.debug(f"🎭 Búsqueda emocional final: {len(results)} resultados")
    return results
    
    
# -----------------------
# Dedup + prefer best bitrate then popularity
# -----------------------
def normalize_title_for_dedupe(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s*\(.*?(remaster|remixed|live|album version|version|explicit|feat\.|ft\.).*?\)", "", s, flags=re.I)
    s = re.sub(r"\s*\[.*?\]", "", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()

def deduplicate_tracks_by_title_keep_best(tracks_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best = {}
    for t in tracks_list:
        key = normalize_title_for_dedupe(t.get("Titulo", "") or "")
        if not key:
            key = (t.get("Ruta") or "")[:200]
        cur_pop = t.get("PopularityScore", 0.0)
        bitrate = t.get("Bitrate") or 0
        if key not in best:
            best[key] = t
        else:
            prev = best[key]
            prev_bitrate = prev.get("Bitrate") or 0
            prev_pop = prev.get("PopularityScore", 0.0)
            if bitrate > prev_bitrate or (bitrate == prev_bitrate and cur_pop > prev_pop):
                best[key] = t
    return list(best.values())

# -----------------------
# Similarity: find reference track & build filters
# -----------------------
def find_reference_track(term: str) -> Optional[Dict[str, Any]]:
    if not term:
        return None
    try:
        doc = tracks_col.find_one({"Titulo": {"$regex": re.escape(term), "$options": "i"}}) or \
              tracks_col.find_one({"Artista": {"$regex": re.escape(term), "$options": "i"}})
        return doc
    except Exception:
        return None

def build_similarity_filters_from_track(t: Dict[str, Any], tolerances: Dict[str, float] = None) -> Dict[str, Any]:
    tolerances = tolerances or {"TempoBPM": 8, "EnergyRMS": 0.06, "LoudnessLUFS": 3}
    f = {}
    tempo = t.get("TempoBPM")
    if tempo:
        f["TempoBPM"] = {"$gte": max(0, tempo - tolerances["TempoBPM"]), "$lte": tempo + tolerances["TempoBPM"]}
    energy = t.get("EnergyRMS")
    if energy is not None:
        f["EnergyRMS"] = {"$gte": max(0.0, energy - tolerances["EnergyRMS"]), "$lte": min(1.0, energy + tolerances["EnergyRMS"])}
    loud = t.get("LoudnessLUFS")
    if loud is not None:
        f["LoudnessLUFS"] = {"$gte": loud - tolerances["LoudnessLUFS"], "$lte": loud + tolerances["LoudnessLUFS"]}
    key = t.get("EstimatedKey")
    if key:
        f["EstimatedKey"] = {"$in": [key]}
    genre = t.get("Genero")
    if genre:
        if isinstance(genre, list) and genre:
            sample = genre[0]
        else:
            sample = genre
        f["Genero"] = {"$regex": re.escape(sample), "$options": "i"}
    return f

# -----------------------
# Weighted rank
# -----------------------
def compute_weighted_rank(track: Dict[str, Any], acoustic_boost: bool = False) -> float:
    pop = track.get("RelativePopularityScore", track.get("PopularityScore", 0)) or 0
    if acoustic_boost:
        energy = float(track.get("EnergyRMS", 0) or 0)
        loudness = track.get("LoudnessLUFS", None)
        loud_norm = 0.0
        if loudness is not None:
            try:
                loud_norm = min(max((-float(loudness)) / 40.0, 0.0), 1.0)
            except Exception:
                loud_norm = 0.0
        acoustic_score = energy * 0.6 + loud_norm * 0.4
        return pop * 0.65 + acoustic_score * 0.35
    return pop

# -----------------------
# M3U generator
# -----------------------
def save_m3u(playlist_items: List[Dict[str, Any]], base_filename: str) -> Tuple[str, str]:
    uid = str(uuid.uuid4())
    safe_name = re.sub(r"[^a-z0-9_-]", "_", base_filename.lower())[:60]
    filename = f"{safe_name}_{uid}.m3u8"
    path = os.path.join(GENERATED_DIR, filename)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("#EXTM3U\n")
        if not playlist_items:
            fh.write("# Playlist generated but no items matched filters. Try relaxing filters.\n")
        for t in playlist_items:
            dur = t.get("Duracion_mmss", "")
            seconds = -1
            try:
                if dur and ":" in str(dur):
                    mm, ss = map(int, str(dur).split(":")[:2])
                    seconds = mm * 60 + ss
            except Exception:
                seconds = -1
            fh.write(f"#EXTINF:{seconds},{t.get('Artista','')} - {t.get('Titulo','')}\n")
            fh.write(f"{t.get('Ruta','')}\n")
    return path, uid

# -----------------------
# Input models
# -----------------------
class QueryIn(BaseModel):
    query: str
    regenerate: bool = False  # ✅ Nuevo campo
    previous_playlist_id: Optional[str] = None  # ✅ Nuevo campo

class FeedbackIn(BaseModel):
    playlist_id: str
    rating: int  # 1..10
    comment: Optional[str] = None

# -----------------------
# Hybrid AI augmentation (when few results) + DB-assisted interaction
# -----------------------
def append_hybrid_log(entry: Dict[str, Any]) -> None:
    try:
        if os.path.exists(HYBRID_LOG_PATH):
            with open(HYBRID_LOG_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        else:
            data = []
    except Exception:
        data = []
    entry["logged_at"] = datetime.utcnow().isoformat()
    data.append(entry)
    try:
        with open(HYBRID_LOG_PATH, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("Could not persist hybrid log")

def hybrid_augment_and_validate(original_query: str, existing_filters: Dict[str, Any], max_suggestions: int = 8) -> Dict[str, Any]:
    """
    Ask Ollama for suggestions (track titles / artists / genres) for the query,
    then validate those suggestions against Mongo (do they exist?). Return validated suggestions and log.
    Robust parsing of AI responses included.
    """
    prompt = (
        f"Provee hasta {max_suggestions} sugerencias de canciones, artistas o géneros que encajen con la siguiente petición "
        f"de usuario. Devuelve EXCLUSIVAMENTE JSON con formato: {{\"suggestions\": [\"text1\",\"text2\",...]}}.\n\n"
        f"Petición: \"{original_query}\"\n\n"
        "Las sugerencias deben priorizar canciones/artistas que probablemente existan y sean representativos del estilo.\n"
        "Si no puedes sugerir, devuelve {\"suggestions\": []}."
    )
    ai_resp = call_ollama(prompt)
    raw_ai = ai_resp.copy() if isinstance(ai_resp, dict) else {"raw": ai_resp}
    # Use our parser to extract suggestions
    suggestions = []
    if isinstance(ai_resp, dict):
        # try common keys
        suggestions = ai_resp.get("suggestions", []) or ai_resp.get("items", []) or ai_resp.get("results", []) or []
        if not suggestions:
            # try parsing raw text fields if present
            for k in ("raw", "text", "response"):
                if k in ai_resp and isinstance(ai_resp[k], str):
                    suggestions = parse_ai_suggestions(ai_resp[k])
                    if suggestions:
                        break
    elif isinstance(ai_resp, str):
        suggestions = parse_ai_suggestions(ai_resp)

    # final normalization
    suggestions = [str(s).strip() for s in suggestions if isinstance(s, (str, int)) and str(s).strip()]
    validated = []
    validated_texts = []
    for s in suggestions:
        doc = None
        try:
            doc = tracks_col.find_one({"$or": [{"Titulo": {"$regex": re.escape(s), "$options": "i"}}, {"Artista": {"$regex": re.escape(s), "$options": "i"}}]})
        except Exception:
            doc = None
        if doc:
            validated.append({"suggestion": s, "found": True, "sample_track": {"Titulo": doc.get("Titulo"), "Artista": doc.get("Artista"), "Ruta": doc.get("Ruta")}})
            validated_texts.append(s)
        else:
            validated.append({"suggestion": s, "found": False})

    entry = {
        "query": original_query,
        "filters_before": existing_filters,
        "ai_raw_response": raw_ai,
        "ai_suggestions": suggestions,
        "validated": validated
    }
    append_hybrid_log(entry)
    return {"validated": validated_texts, "raw": raw_ai, "validated_full": validated}

def gather_top_artists_from_mongo(filters: Dict[str, Any], top_n: int = 50) -> List[str]:
    """
    Return a list of top artists matching the current filters, ordered by frequency/popularity.
    """
    pipeline = []
    if filters:
        pipeline.append({"$match": filters})
    pipeline.extend([
        {"$group": {"_id": "$Artista", "count": {"$sum": 1}, "max_pop": {"$max": "$LastFMPlaycount"}}},
        {"$sort": {"count": -1, "max_pop": -1}},
        {"$limit": top_n}
    ])
    try:
        res = list(tracks_col.aggregate(pipeline))
        artists = [r["_id"] for r in res if r and r.get("_id")]
        return artists
    except Exception:
        logger.exception("Error gathering top artists from mongo")
        return []

def hybrid_db_assisted_cycle(original_query: str, existing_filters: Dict[str, Any], min_validated_threshold: int = 10) -> Dict[str, Any]:
    """
    Hybrid flow:
      1) Ask model for suggestions, validate.
      2) If not enough, gather top local artists and ask model to prioritize among them.
      3) If still not enough, fallback: use local artists directly (return artist names as validated)
    Returns validated suggestion strings and detailed log.
    """
    # First attempt
    ai_first = hybrid_augment_and_validate(original_query, existing_filters, max_suggestions=12)
    validated_first = ai_first.get("validated", []) or []
    log = {"stage": "initial_suggestions", "validated_first": validated_first, "raw_first": ai_first.get("raw")}

    if len(validated_first) >= min_validated_threshold:
        log["succeeded"] = True
        return {"validated": validated_first, "raw": ai_first.get("raw"), "log": log}

    # Gather local artists to help focus the model
    local_artists = gather_top_artists_from_mongo(existing_filters, top_n=80)
    log["local_artists_sample"] = local_artists[:30] if local_artists else []
    if not local_artists:
        log["succeeded"] = False
        log["reason"] = "no_local_artists"
        return {"validated": validated_first, "raw": {"first": ai_first.get("raw")}, "log": log}

    # Build prompt including local artists (shortened)
    sample_artists = local_artists[:60]
    # 🧩 PROMPT 1 — Recomendaciones iniciales
    prompt = (
        f"El usuario pidió: \"{original_query}\".\n"
        "Tu tarea es sugerir canciones o artistas que coincidan con la intención completa de esa petición, "
        "manteniendo su género, época, energía, emoción y estilo.\n\n"
        "A continuación hay una lista de artistas disponibles localmente en la base de datos:\n"
        + ", ".join(sample_artists[:30]) + ("\n..." if len(sample_artists) > 30 else "\n")
        + "\nUsa esta lista como referencia prioritaria para sugerir artistas o canciones coherentes con el pedido del usuario.\n"
        "Devuelve como máximo 20 sugerencias en formato JSON válido:\n"
        "{\"suggestions\": [{\"titulo\": \"...\", \"artista\": \"...\", \"album\": \"...\"}]}\n"
        "Asegúrate de conservar el contexto del prompt original (por ejemplo: si menciona 'rock de los 80s', "
        "no incluyas artistas de pop moderno ni fuera de esa época)."
    )
        
    
    ai_second = call_ollama(prompt)
    raw_second = ai_second.copy() if isinstance(ai_second, dict) else {"raw": ai_second}
    suggestions2 = []
    if isinstance(ai_second, dict):
        suggestions2 = ai_second.get("suggestions", []) or ai_second.get("items", []) or []
        if not suggestions2:
            # try parsing raw text in response
            for k in ("raw", "text", "response"):
                if k in ai_second and isinstance(ai_second[k], str):
                    suggestions2 = parse_ai_suggestions(ai_second[k])
                    if suggestions2:
                        break
    elif isinstance(ai_second, str):
        suggestions2 = parse_ai_suggestions(ai_second)

    suggestions2 = [s for s in suggestions2 if isinstance(s, str) and s.strip()]
    validated2 = []
    validated_texts2 = []
    for s in suggestions2:
        try:
            doc = tracks_col.find_one({"$or": [{"Titulo": {"$regex": re.escape(s), "$options": "i"}}, {"Artista": {"$regex": re.escape(s), "$options": "i"}}]})
        except Exception:
            doc = None
        if doc:
            validated2.append({"suggestion": s, "found": True, "sample_track": {"Titulo": doc.get("Titulo"), "Artista": doc.get("Artista"), "Ruta": doc.get("Ruta")}})
            validated_texts2.append(s)
        else:
            validated2.append({"suggestion": s, "found": False})

    # If model didn't return useful suggestions but we have local artists, fallback: use artist names directly
    if not validated_texts2:
        # query mongo for tracks from top local artists (guaranteed to exist locally) and return their artist-title combos
        fallback_validated = []
        try:
            # take top N artists and pull 3 tracks each (if available)
            sample_for_query = local_artists[:30]
            q = {"Artista": {"$in": sample_for_query}}
            cursor = tracks_col.find(q).limit(200)
            found = list(cursor)
            for doc in found:
                tstr = f"{doc.get('Titulo')}" if doc.get("Titulo") else None
                if tstr:
                    fallback_validated.append({"suggestion": tstr, "found": True, "sample_track": {"Titulo": doc.get("Titulo"), "Artista": doc.get("Artista"), "Ruta": doc.get("Ruta")}})
            # dedupe suggestion texts preserving order
            texts = []
            for v in fallback_validated:
                txt = v.get("suggestion")
                if txt and txt not in texts:
                    texts.append(txt)
            validated_texts2 = texts[:min(60, len(texts))]
        except Exception:
            logger.exception("Error during local-artist fallback extraction")
            validated_texts2 = []

    entry = {
        "query": original_query,
        "filters_before": existing_filters,
        "stage": "db_assisted",
        "local_artists_sample": sample_artists[:30],
        "ai_raw_response_first": ai_first.get("raw"),
        "ai_raw_response_second": raw_second,
        "ai_suggestions_second": suggestions2,
        "validated_second": validated2,
        "fallback_validated_from_local": len(validated_texts2) > 0
    }
    append_hybrid_log(entry)

    combined_validated = list(dict.fromkeys((validated_first or []) + validated_texts2))
    log["succeeded"] = len(combined_validated) >= min_validated_threshold
    log["final_count"] = len(combined_validated)
    return {"validated": combined_validated, "raw": {"first": ai_first.get("raw"), "second": raw_second}, "log": log}

# -----------------------
# Relax filters one-shot fallback
# -----------------------
def relax_filters(filters: Dict[str, Any]) -> Dict[str, Any]:
    newf = {}
    for k, v in filters.items():
        if isinstance(v, dict):
            nv = dict(v)
            nv.pop("$gt", None)
            nv.pop("$lt", None)
            if "$gte" in nv and isinstance(nv["$gte"], (int, float)):
                nv["$gte"] = max(0, int(nv["$gte"] * 0.7))
            if "$lte" in nv and isinstance(nv["$lte"], (int, float)):
                nv["$lte"] = int(nv["$lte"] * 1.3)
            if nv:
                newf[k] = nv
        else:
            newf[k] = v
    newf.pop("EstimatedKey", None)
    if "EnergyRMS" in newf:
        try:
            g = newf["EnergyRMS"].get("$gt")
            if g:
                newf["EnergyRMS"]["$gt"] = max(0.0, g - 0.08)
        except Exception:
            pass
    return newf

# -----------------------
# Final inspection: limpiar incongruencias groseras
# -----------------------
def filter_gross_incongruities(tracks_list: List[Dict[str, Any]], query_text: str) -> List[Dict[str, Any]]:
    """
    Remove tracks that clearly contradict the user's intent.
    Heuristics:
      - If user asked 'bailable' require tempo >= 100 OR genre matches dance list OR EMO_Sound groovy/energetic.
      - If user asked 'pesado' require heavy genre OR EnergyRMS > 0.22 OR loudness and genre match.
      - If user asked 'tranquilo' require EnergyRMS < 0.18 or EMO_Sound calm.
      - 'similar a' special cases are not filtered aggressively.
    """
    if not tracks_list:
        return tracks_list
    text = (query_text or "").lower()
    filtered = []
    for t in tracks_list:
        keep = True
        tempo = t.get("TempoBPM") or 0
        energy = t.get("EnergyRMS") or 0.0
        genre = t.get("Genero") or ""
        emo_sound = (t.get("EMO_Sound") or "").lower()

        # Bailables
        if any(w in text for w in ["bail", "dance", "bailable", "fiesta", "party", "groovy", "movido", "ritmo"]):
            genre_text = " ".join(genre) if isinstance(genre, list) else str(genre)
            if tempo < 100 and not DANCE_GENRE_REGEX.search(genre_text) and "groovy" not in emo_sound and energy < 0.18:
                keep = False

        # Pesado/agresivo
        if any(w in text for w in ["pesado", "agresivo", "heavy", "metal", "hard", "brutal", "intenso"]):
            genre_text = " ".join(genre) if isinstance(genre, list) else str(genre)
            if not HEAVY_GENRE_REGEX.search(genre_text) and energy < 0.2 and tempo < 100:
                keep = False

        # Tranquilo / baladas
        if any(w in text for w in ["tranquil", "relaj", "calm", "melancol", "lento", "soft", "balada", "romant"]):
            if energy > 0.24 and tempo > 110:
                keep = False

        # If user explicitly included an artist/title, don't be too aggressive
        if re.search(r"por\s+\w+|de\s+\w+|similar a", text):
            # relax rules
            pass

        if keep:
            filtered.append(t)
        else:
            logger.debug(f"Removed for incongruity: {t.get('Artista')} - {t.get('Titulo')} (tempo={tempo}, energy={energy}, genero={genre})")

    return filtered

# -----------------------
# Endpoint: /query (V15 con guardado por usuario) - MEJORADO PARA PAÍSES
# -----------------------
@app.post("/query")
def query_playlists(body: QueryIn, request: Request):
    query_text = body.query.strip()
    start_ts = datetime.utcnow()
    
    # ✅ OBTENER USUARIO AUTENTICADO
    try:
        auth_header = request.headers.get("Authorization")
        user_email = "anonymous"
        if auth_header and "Bearer" in auth_header:
            token = auth_header.replace("Bearer ", "").strip()
            user = db_auth.users.find_one({"session_token": token})
            if user:
                user_email = user.get("email", "anonymous")
                logger.debug(f"👤 Usuario autenticado: {user_email}")
            else:
                logger.debug("👤 Usuario no autenticado, usando 'anonymous'")
    except Exception as e:
        logger.warning(f"⚠️ Error obteniendo usuario: {e}")
        user_email = "anonymous"

    # ✅ USAR DIRECTAMENTE EL MODELO PYDANTIC
    regenerate = body.regenerate
    previous_playlist_id = body.previous_playlist_id
    
    logger.debug(f"🔎 Query received: {query_text}")
    logger.debug(f"🆕 Regenerate flag recibido: {regenerate}")
    logger.debug(f"📀 previous_playlist_id recibido: {previous_playlist_id}")
    logger.debug(f"👤 Usuario: {user_email}")

    # --- 🔧 Asegurar que el flag regenerate se interprete correctamente ---
    if isinstance(regenerate, str):
        regenerate = regenerate.strip().lower() in ("true", "1", "yes", "on")
    elif isinstance(regenerate, (int, float)):
        regenerate = bool(regenerate)

    logger.debug(f"🆕 Regenerate flag recibido (raw={getattr(body, 'regenerate', None)}) → interpretado como {regenerate}")
    logger.debug(f"📀 previous_playlist_id recibido: {previous_playlist_id}")

    # Contenedores para exclusiones (títulos en minúscula y rutas)
    excluded_titles = set()
    excluded_paths = set()

    # Intentamos cargar la playlist previa sólo si se pidió regenerar y se entregó id
    if regenerate:
        logger.debug("🆕 Regeneración solicitada por el cliente.")
        if previous_playlist_id:
            try:
                # Intentar convertir a ObjectId si es necesario (si usas pymongo)
                try:
                    prev_doc = playlists_col.find_one({"_id": ObjectId(previous_playlist_id), "user_email": user_email})
                except Exception:
                    # si la colección guarda id como string, intentar fallback
                    prev_doc = playlists_col.find_one({"playlist_uuid": previous_playlist_id, "user_email": user_email}) or playlists_col.find_one({"_id": previous_playlist_id, "user_email": user_email})
                
                if prev_doc and isinstance(prev_doc.get("items", None), list):
                    for it in prev_doc.get("items", []):
                        title = (it.get("Titulo") or it.get("title") or "").strip().lower()
                        path = it.get("Ruta") or it.get("ruta") or it.get("stream_url") or None
                        if title:
                            excluded_titles.add(title)
                        if path:
                            excluded_paths.add(path)
                    logger.debug(f"🆕 Cargada playlist previa: excluyendo {len(excluded_titles)} títulos y {len(excluded_paths)} rutas.")
                else:
                    logger.debug("🆕 No se encontró playlist previa o no tiene 'items' válidos; ignorando exclusiones.")
            except Exception as e:
                logger.warning(f"⚠️ Error al cargar playlist previa para regeneración: {e}")
        else:
            logger.debug("🆕 Regenerar=true pero no se entregó previous_playlist_id; no habrá exclusiones.")
    else:
        logger.debug("🆕 No se solicitó regeneración (regenerate=false).")

    # 1️⃣ Limpieza inicial de texto (quita prefijos comunes)
    query_clean = re.sub(r"^(lo|la|el|los|las)\s+", "", query_text, flags=re.I).strip()
    logger.debug(f"🧼 Query normalizada: {query_clean}")

    # 2️⃣ Análisis semántico del prompt con interpretación robusta
    llm_analysis = analyze_query_intent(query_clean)
    llm_analysis = enhance_region_detection(llm_analysis, query_text)
    
    # ✅ CASO ESPECIAL: Región + Género
    detected_region = llm_analysis.get("region")
    user_genre = llm_analysis.get("genre")
    
    if detected_region and detected_region in REGION_DEFINITIONS:
        logger.debug(f"🗺️ Modo REGIÓN activado: {detected_region}, género: {user_genre}")
        
        # Búsqueda especializada por región
        region_tracks = search_tracks_by_region(
            region_id=detected_region,
            user_genre=user_genre,  # Puede ser None
            limit=llm_analysis["detected_limit"]
        )
        
        if region_tracks:
            # Procesar y devolver resultados
            region_filters = {
                "region": detected_region,
                "genre": user_genre,
                "countries": REGION_DEFINITIONS[detected_region]["countries"]
            }
            
            # ... guardar playlist con nombre inteligente ...
            return create_region_playlist_response(
                query_text, region_tracks, region_filters, llm_analysis, user_email
            )
    
    # ✅ NUEVO: Manejo específico para solicitudes de país
    if llm_analysis.get("type") == "country_request" and llm_analysis.get("country"):
        logger.debug(f"🇨🇱 Modo país activado: {llm_analysis.get('country')} ({llm_analysis.get('country_type')})")
        
        # ✅ BÚSQUEDA DIRECTA DE EMERGENCIA con prioridad jerárquica en TopCountry
        emergency_tracks = emergency_country_search(
            llm_analysis["country"], 
            llm_analysis.get("country_type", "origin"),
            llm_analysis.get("detected_limit", 30)  # ✅ Siempre 30 por defecto
        )
        
        if emergency_tracks:
            # 🆕 Excluir pistas previas si estamos regenerando
            if regenerate:
                logger.debug("🆕 Aplicando exclusión de pistas previas en modo PAÍS.")
                emergency_tracks = exclude_previous_tracks(emergency_tracks, excluded_titles, excluded_paths)
                logger.debug(f"🆕 Tras exclusión, quedan {len(emergency_tracks)} pistas candidatas.")

            # ✅ Asegurar que tenemos exactamente el límite solicitado
            target_limit = llm_analysis.get("detected_limit", 30)
            if len(emergency_tracks) > target_limit:
                # Si tenemos más resultados que el límite, tomar los mejores
                emergency_tracks = emergency_tracks[:target_limit]
                logger.debug(f"🎯 Limité resultados de {len(emergency_tracks)} a {target_limit} pistas")
            
            # Procesar los resultados de emergencia
            global_max = get_global_max_values()
            for t in emergency_tracks:
                t["PopularityScore"] = compute_popularity(t, global_max)
            
            compute_relative_popularity_by_genre(emergency_tracks)
            
            # ✅ Ordenar por RelativePopularityScore (ya viene ordenado por PopularityScore de la búsqueda)
            emergency_tracks.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
            
            # ✅ Tomar exactamente el límite solicitado
            final_tracks = emergency_tracks[:target_limit]

            # Generar salida simplificada
            simplified = [{
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
            } for t in final_tracks]

            # Guardar M3U y registro CON USUARIO
            country_name = llm_analysis["country"]
            country_type = llm_analysis.get("country_type", "origin")
            base_filename = f"musica_{country_name.lower()}_{country_type}"
            m3u_path, playlist_uuid = save_m3u(simplified, base_filename)

            # ✅ GENERAR NOMBRE AMIGABLE PARA LA PLAYLIST
            if country_type == "origin":
                playlist_name = f"Música de {country_name}"
            else:
                playlist_name = f"Lo más escuchado en {country_name}"
                
            if len(simplified) < 5:
                playlist_name = f"{country_name} - Selección musical"

            # Construir filtros para el documento
            country_filters = {}
            if country_type == "origin":
                country_filters = {"ArtistArea": country_name}
            else:
                country_filters = {
                    "$or": [
                        {"TopCountry1": country_name},
                        {"TopCountry2": country_name},
                        {"TopCountry3": country_name}
                    ]
                }

            # ✅ Obtener estadísticas de distribución TopCountry
            topcountry_stats = get_topcountry_distribution(final_tracks, country_name) if country_type != "origin" else {}

            playlist_doc = {
                "query_original": query_text,
                "name": playlist_name,
                "filters": country_filters,
                "sort_by": "RelativePopularityScore",
                "limit": len(simplified),
                "created_at": start_ts,
                "m3u_path": m3u_path,
                "playlist_uuid": playlist_uuid,
                "items": simplified,
                "stats": {
                    "total": len(simplified), 
                    "country_mode": True,
                    "country": country_name,
                    "country_type": country_type,
                    "regenerated": regenerate,
                    "topcountry_distribution": topcountry_stats  # ✅ NUEVO: Estadísticas
                },
                "feedback_pending": True,
                "user_email": user_email,
                "type": "country"
            }

            try:
                res = playlists_col.insert_one(playlist_doc)
                playlist_id = str(res.inserted_id)
                logger.debug(f"💾 Playlist PAÍS guardada con id {playlist_id} para usuario {user_email}")
            except Exception as e:
                logger.exception(f"Error inserting playlist doc (country mode): {e}")
                playlist_id = None

            # Respuesta final para modo país
            return {
                "query_original": query_text,
                "playlist_name": playlist_name,
                "filtros": country_filters,
                "criterio_orden": "RelativePopularityScore",
                "total": len(simplified),
                "playlist": simplified,
                "archivo_m3u": m3u_path,
                "playlist_id": playlist_id,
                "playlist_uuid": playlist_uuid,
                "user_email": user_email,
                "debug_summary": {
                    "country_mode": True,
                    "country": country_name,
                    "country_type": country_type,
                    "llm_analysis": llm_analysis,
                    "normalization_applied": True,
                    "excluded_count": len(excluded_titles),
                    "topcountry_distribution": topcountry_stats  # ✅ NUEVO
                },
            }
        
    qtype = llm_analysis.get("type", "")
    artist_name = llm_analysis.get("artist") or None
    album_name = llm_analysis.get("album") or None
    track_name = llm_analysis.get("track") or None
    logger.debug(f"🧠 Análisis de intención (vía modelo local): {json.dumps(llm_analysis, ensure_ascii=False)}")
    
    # ✅ USAR LÍMITE DETECTADO
    detected_limit = llm_analysis.get("detected_limit", 30)
    logger.debug(f"🔢 Límite a usar: {detected_limit} (detectado del prompt)")

    logger.debug(f"🧠 Análisis semántico: {llm_analysis}")

    qtype = llm_analysis.get("type", "")
    artist_name = llm_analysis.get("artist") or None
    album_name = llm_analysis.get("album") or None
    track_name = llm_analysis.get("track") or None

    # 3️⃣ Detección directa de entidad en base local
    detected = detect_artist_album_track(query_clean, tracks_col)
    entity_type, entity_name = detected["tipo"], detected["nombre"]
    logger.debug(f"🎯 Entidad detectada: {entity_type} -> {entity_name}")

    # Si no hay artista claro desde LLM, usa el detectado localmente
    if not artist_name and entity_type == "artista":
        artist_name = entity_name
        logger.debug(f"🔁 Artist fallback: usando entidad local detectada → {artist_name}")

    # ============================================================
    # 🏆 4️⃣ Caso: "Lo mejor de X" o petición de artista
    # ============================================================
    intent_type = llm_analysis.get("type", "").strip()
    artist_name = llm_analysis.get("artist", "").strip()
    album_name = llm_analysis.get("album", "").strip()
    track_name = llm_analysis.get("track", "").strip()

    genre_value = llm_analysis.get("genre", "")
    if isinstance(genre_value, list):
        genre = ", ".join(map(str, genre_value))
    else:
        genre = str(genre_value).strip()
    
    decade_value = llm_analysis.get("decade", "")
    if isinstance(decade_value, list):
        decade = ", ".join(map(str, decade_value))
    else:
        decade = str(decade_value).strip()
    
    mood = llm_analysis.get("mood", "").strip()

    logger.debug(f"🎯 Tipo de solicitud detectado por LLM: {intent_type}")

    # Fallback por regex si el modelo no clasificó bien
    if not intent_type and re.search(r"(mejor de|best of|top de|grandes éxitos)", query_clean, re.I):
        intent_type = "artist_request"
        logger.debug("🔎 Fallback regex: Identificado como artist_request")
    elif not intent_type and re.search(r"(similares a|parecidas a|similar to)", query_clean, re.I):
        intent_type = "similar_to_request"
        logger.debug("🔎 Fallback regex: Identificado como similar_to_request")
    elif not intent_type:
        intent_type = "genre_or_mood_request"
        logger.debug("🔎 Fallback regex: Identificado como genre_or_mood_request")

    logger.debug(f"🧭 Modo elegido: {intent_type} | Artista='{artist_name}' | Álbum='{album_name}' | Track='{track_name}' | Género='{genre}' | Década='{decade}' | Mood='{mood}'")

    # ============================================================
    # 🏆 5️⃣ MODO ARTISTA: "Lo mejor de X"
    # ============================================================
    if intent_type == "artist_request" and artist_name:
        target_artist = artist_name
        logger.debug(f"🎸 Modo artista activado → '{target_artist}'")
        
        artist_limit = min(detected_limit, 50)  # Máximo 50 por seguridad
        # 1️⃣ Obtener mejores pistas
        best_tracks = get_best_of_artist(target_artist, tracks_col, limit=artist_limit, llm=run_local_llm)
        if not best_tracks:
            logger.debug(f"⚠️ Sin resultados directos para '{target_artist}', buscando similares...")
            best_tracks = get_best_of_artist(target_artist, tracks_col, limit=artist_limit, llm=run_local_llm)
        if not best_tracks:
            logger.debug(f"⚠️ Aún no hay pistas para '{target_artist}' tras busqueda de similares.")
            return {
                "query_original": query_text,
                "filtros": {"Artista": target_artist},
                "criterio_orden": "RelativePopularityScore",
                "total": 0,
                "playlist": [],
                "archivo_m3u": "",
                "debug_summary": {"artist_mode": True, "llm_analysis": llm_analysis},
            }

        # 🆕 Excluir pistas previas (si aplica regeneración)
        if regenerate:
            logger.debug("🆕 Aplicando exclusión de pistas previas en modo ARTISTA.")
            best_tracks = exclude_previous_tracks(best_tracks, excluded_titles, excluded_paths)
            logger.debug(f"🆕 Tras exclusión, quedan {len(best_tracks)} pistas candidatas para ordenar.")

        # 2️⃣ Calcular PopularityScore basado en playcount / views
        global_max = get_global_max_values()
        for t in best_tracks:
            t["PopularityScore"] = compute_popularity(t, global_max)
            if not t.get("PopularityScore"):
                base_score = t.get("LastFMPlaycount") or t.get("YouTubeViews") or 0
                t["PopularityScore"] = min(1.0, math.log1p(base_score) / 20.0)

        # 3️⃣ Normalizar relativa por género
        compute_relative_popularity_by_genre(best_tracks)

        # 4️⃣ Ordenar por mayor popularidad relativa
        best_tracks.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
        logger.debug(f"✅ Ordenadas {len(best_tracks)} pistas por RelativePopularityScore (desc).")

        # 5️⃣ Generar salida simplificada
        simplified = [{
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
        } for t in best_tracks]

        # 6️⃣ Guardar M3U y registro CON USUARIO
        base_filename = re.sub(r"\s+", "_", target_artist.lower())[:60]
        m3u_path, playlist_uuid = save_m3u(simplified, base_filename)

        # ✅ GENERAR NOMBRE AMIGABLE PARA LA PLAYLIST
        playlist_name = f"Lo mejor de {target_artist}"
        if len(simplified) < 5:
            playlist_name = f"{target_artist} - Selección"

        playlist_doc = {
            "query_original": query_text,
            "name": playlist_name,  # ✅ NOMBRE PARA MOSTRAR AL USUARIO
            "filters": {"Artista": target_artist},
            "sort_by": "RelativePopularityScore",
            "limit": len(simplified),
            "created_at": start_ts,
            "m3u_path": m3u_path,
            "playlist_uuid": playlist_uuid,
            "items": simplified,
            "stats": {
                "total": len(simplified), 
                "artist_mode": True, 
                "regenerated": regenerate
            },
            "feedback_pending": True,
            "user_email": user_email,  # ✅ ASOCIADO AL USUARIO
            "type": "artist"  # ✅ TIPO DE PLAYLIST
        }

        try:
            res = playlists_col.insert_one(playlist_doc)
            playlist_id = str(res.inserted_id)
            logger.debug(f"💾 Playlist ARTISTA guardada con id {playlist_id} para usuario {user_email}")
        except Exception as e:
            logger.exception(f"Error inserting playlist doc (artist mode): {e}")
            playlist_id = None

        # 7️⃣ Respuesta final
        return {
            "query_original": query_text,
            "playlist_name": playlist_name,  # ✅ NOMBRE PARA EL FRONTEND
            "filtros": {"Artista": target_artist},
            "criterio_orden": "RelativePopularityScore",
            "total": len(simplified),
            "playlist": simplified,
            "archivo_m3u": m3u_path,
            "playlist_id": playlist_id,
            "playlist_uuid": playlist_uuid,
            "user_email": user_email,  # ✅ INCLUIR EMAIL EN RESPUESTA
            "debug_summary": {
                "artist_mode": True,
                "llm_analysis": llm_analysis,
                "normalization_applied": True,
                "excluded_count": len(excluded_titles),
            },
        }

    # ============================================================
    # 🎧 6️⃣ Caso: "Similares a X"
    # ============================================================
    if intent_type == "similar_to_request":
        ref_name = artist_name or track_name or re.sub(
            r"(similares a|parecidas a|similar to)\s+", "", query_clean, flags=re.I
        ).strip()

        logger.debug(f"🔁 Modo similitud activado para: {ref_name}")

        # 1️⃣ Buscar artistas o temas similares
        similar_limit = min(detected_limit * 2, 60)  # Buscar más para tener opciones
        similar_tracks = find_similar_artists(ref_name, tracks_col, llm=run_local_llm, limit=similar_limit)

        if not similar_tracks:
            logger.debug(f"⚠️ No se encontraron pistas similares para '{ref_name}'")
            return {
                "query_original": query_text,
                "filtros": {"similar_a": ref_name},
                "criterio_orden": "RelativePopularityScore",
                "total": 0,
                "playlist": [],
                "archivo_m3u": "",
                "debug_summary": {"similarity_mode": True, "llm_analysis": llm_analysis},
            }

        # 🆕 Excluir pistas previas si estamos regenerando
        if regenerate:
            logger.debug("🆕 Aplicando exclusión de pistas previas en modo SIMILARES.")
            similar_tracks = exclude_previous_tracks(similar_tracks, excluded_titles, excluded_paths)
            logger.debug(f"🆕 Tras exclusión, quedan {len(similar_tracks)} pistas similares candidatas.")

        # 2️⃣ Calcular PopularityScore
        global_max = get_global_max_values()
        for t in similar_tracks:
            t["PopularityScore"] = compute_popularity(t, global_max)
            if not t.get("PopularityScore"):
                base_score = t.get("LastFMPlaycount") or t.get("YouTubeViews") or 0
                t["PopularityScore"] = min(1.0, math.log1p(base_score) / 20.0)

        # 3️⃣ Deduplicar versiones
        deduped_tracks = deduplicate_tracks_by_title_keep_best(similar_tracks)

        # 4️⃣ Aplicar normalización relativa por género
        compute_relative_popularity_by_genre(deduped_tracks)

        # 5️⃣ Ordenar y filtrar según filtros semánticos del prompt (género/década/mood)
        if genre or decade or mood:
            deduped_tracks = [
                t for t in deduped_tracks
                if (not genre or genre.lower() in str(t.get("Genero", "")).lower())
                and (not decade or decade in str(t.get("Decada", "")))
            ]
            logger.debug(f"🔎 Aplicados filtros semánticos sobre tracks similares: quedan {len(deduped_tracks)} items.")

        deduped_tracks.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
        logger.debug(f"✅ Ordenadas {len(deduped_tracks)} pistas similares por RelativePopularityScore (desc).")

        # 6️⃣ Generar salida simplificada
        simplified = [{
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
        } for t in deduped_tracks[:20]]

        # 7️⃣ Guardar y responder CON USUARIO
        m3u_path, playlist_uuid = save_m3u(simplified, ref_name)

        # ✅ GENERAR NOMBRE AMIGABLE
        playlist_name = f"Similares a {ref_name}"
        if len(simplified) < 5:
            playlist_name = f"Recomendaciones como {ref_name}"

        playlist_doc = {
            "query_original": query_text,
            "name": playlist_name,  # ✅ NOMBRE PARA MOSTRAR
            "filters": {"similar_a": ref_name, "Genero": genre, "Decada": decade},
            "sort_by": "RelativePopularityScore",
            "limit": len(simplified),
            "created_at": start_ts,
            "m3u_path": m3u_path,
            "playlist_uuid": playlist_uuid,
            "items": simplified,
            "stats": {
                "total": len(simplified), 
                "similarity_mode": True, 
                "regenerated": regenerate
            },
            "feedback_pending": True,
            "user_email": user_email,  # ✅ ASOCIADO AL USUARIO
            "type": "similar"  # ✅ TIPO DE PLAYLIST
        }

        try:
            res = playlists_col.insert_one(playlist_doc)
            playlist_id = str(res.inserted_id)
            logger.debug(f"💾 Playlist SIMILARES guardada con id {playlist_id} para usuario {user_email}")
        except Exception as e:
            logger.exception(f"Error inserting playlist doc (similarity mode): {e}")
            playlist_id = None

        return {
            "query_original": query_text,
            "playlist_name": playlist_name,  # ✅ NOMBRE PARA EL FRONTEND
            "filtros": {"similar_a": ref_name, "Genero": genre, "Decada": decade},
            "criterio_orden": "RelativePopularityScore",
            "total": len(simplified),
            "playlist": simplified,
            "archivo_m3u": m3u_path,
            "playlist_id": playlist_id,
            "playlist_uuid": playlist_uuid,
            "user_email": user_email,  # ✅ INCLUIR EMAIL EN RESPUESTA
            "debug_summary": {
                "similarity_mode": True,
                "llm_analysis": llm_analysis,
                "normalization_applied": True,
                "excluded_count": len(excluded_titles),
            },
        }

    # ============================================================
    # 🌈 7️⃣ Flujo estándar (género, mood, época, etc.) - MEJORADO CON PAÍS
    # ============================================================
    logger.debug("🎼 Ejecutando flujo estándar (género/estado de ánimo).")

    # ✅ NUEVO: Inyectar filtros de país en el flujo híbrido si están presentes
    llm_analysis_for_hybrid = llm_analysis.copy()
    
    # 1️⃣ Procesamiento base híbrido (LLM + heurísticas locales) con país
    llm_raw = hybrid_playlist_cycle_enhanced(query_clean, llm_analysis=llm_analysis_for_hybrid) or {}
    filters_raw = llm_raw.get("filters", {}) or {}
    suggestions = llm_raw.get("suggestions", [])
    sort_by = llm_raw.get("sort_by")
    order = int(llm_raw.get("order", -1)) if llm_raw.get("order") in (1, -1, None) else -1
    limit = min(detected_limit, 100)
    logger.debug(f"🔢 Límite final aplicado: {limit}")
    
    # 2️⃣ Combinar filtros semánticos con los del modelo híbrido
    filters_combined = dict(filters_raw)  # copia base
    
    # ✅ NUEVO: Añadir filtros de país del análisis semántico
    country = llm_analysis.get("country")
    country_type = llm_analysis.get("country_type")
    if country and country_type:
        country_filters = parse_filters_from_llm({
            "country": country,
            "country_type": country_type
        })
        filters_combined.update(country_filters)
        logger.debug(f"🇨🇱 Filtros de país añadidos al flujo estándar: {country} ({country_type})")
    
    # 3️⃣ Tomar filtros inferidos del análisis semántico previo (llm_analysis)
    genre = llm_analysis.get("genre")
    decade = llm_analysis.get("decade")
    if decade:
        # Aplicar filtro de década resuelta
        filters_combined["Decada"] = decade
        logger.debug(f"🕰️ Aplicando filtro de década: {decade}")
    mood = llm_analysis.get("mood")
    energy = llm_analysis.get("energy")
    intent = llm_analysis.get("intent")

    logger.debug(f"🎨 Enriqueciendo filtros estándar con análisis semántico → genre={genre}, decade={decade}, mood={mood}, energy={energy}, country={country}")

    # ➕ Añadir género
    if genre and "Genero" not in filters_combined:
        filters_combined["Genero"] = {"$regex": genre, "$options": "i"}

    # ➕ Añadir década o año - CON SOPORTE PARA MÚLTIPLES DÉCADAS
    if decade and "Decada" not in filters_combined and "Año" not in filters_combined:
        if isinstance(decade, list):
            # ✅ Múltiples décadas: ["1980s", "1990s"]
            decade_ranges = []
            for d in decade:
                if d == "1980s":
                    decade_ranges.append({"$gte": 1980, "$lt": 1990})
                elif d == "1990s":
                    decade_ranges.append({"$gte": 1990, "$lt": 2000})
                elif d == "2000s":
                    decade_ranges.append({"$gte": 2000, "$lt": 2010})
                elif d == "2010s":
                    decade_ranges.append({"$gte": 2010, "$lt": 2020})
                elif d == "2020s":
                    decade_ranges.append({"$gte": 2020, "$lt": 2030})
            
            if decade_ranges:
                filters_combined["Año"] = {"$or": decade_ranges}
                filters_combined["Decada"] = {"$in": decade}
        elif isinstance(decade, str):
            # ✅ Década única
            filters_combined["Decada"] = decade
            # También añadir rango de años para compatibilidad
            if decade == "1980s":
                filters_combined["Año"] = {"$gte": 1980, "$lt": 1990}
            elif decade == "1990s":
                filters_combined["Año"] = {"$gte": 1990, "$lt": 2000}
            elif decade == "2000s":
                filters_combined["Año"] = {"$gte": 2000, "$lt": 2010}
            elif decade == "2010s":
                filters_combined["Año"] = {"$gte": 2010, "$lt": 2020}
            elif decade == "2020s":
                filters_combined["Año"] = {"$gte": 2020, "$lt": 2030}

    # ➕ Añadir mood si corresponde (usa campos emocionales)
    if mood and not any(k.startswith("EMO_") for k in filters_combined.keys()):
        filters_combined["EMO_Sound"] = {"$regex": mood, "$options": "i"}

    # 4️⃣ Enriquecer con filtros acústicos y sanitizar
    filters_enriched = enrich_filters_with_acoustics(query_clean, filters_combined)
    filters_safe = sanitize_filters(filters_enriched)
    mongo_filters = dict(filters_safe)

    # ✅ DEBUG DETALLADO DE FILTROS
    logger.debug(f"🧩 Filtros combinados: {json.dumps(filters_combined, ensure_ascii=False)}")
    logger.debug(f"🧩 Filtros enriquecidos: {json.dumps(filters_enriched, ensure_ascii=False)}")
    logger.debug(f"🧩 Filtros finales aplicados → {json.dumps(mongo_filters, ensure_ascii=False)}")

    # 5️⃣ Consulta principal a la base de datos
    results = list(tracks_col.find(mongo_filters))
    logger.debug(f"📊 Resultados encontrados con filtros: {len(results)}")

    # 🆕 Excluir pistas previas si estamos regenerando en flujo estándar
    if regenerate:
        logger.debug("🆕 Aplicando exclusión de pistas previas en modo ESTÁNDAR.")
        results = exclude_previous_tracks(results, excluded_titles, excluded_paths)
        logger.debug(f"🆕 Tras exclusión, quedaron {len(results)} resultados en la búsqueda estándar.")

    if not results:
        logger.debug(f"⚠️ Sin resultados directos con filtros {mongo_filters}, intentando expansión...")
        
        # ✅ NUEVO: Expansión específica para países con búsqueda jerárquica
        if country and "ArtistArea" in mongo_filters:
            logger.debug(f"🔁 Expandiendo búsqueda de país {country} con prioridad jerárquica...")
            expanded_results = []
            
            # 1. Primero TopCountry1
            query_tc1 = {"TopCountry1": {"$regex": country, "$options": "i"}}
            results_tc1 = list(tracks_col.find(query_tc1).sort("PopularityScore", -1).limit(limit))
            expanded_results.extend(results_tc1)
            
            # 2. Si no alcanzamos, TopCountry2
            if len(expanded_results) < limit:
                remaining = limit - len(expanded_results)
                query_tc2 = {
                    "TopCountry2": {"$regex": country, "$options": "i"},
                    "_id": {"$nin": [r["_id"] for r in expanded_results]}
                }
                results_tc2 = list(tracks_col.find(query_tc2).sort("PopularityScore", -1).limit(remaining))
                expanded_results.extend(results_tc2)
            
            # 3. Si aún no alcanzamos, TopCountry3
            if len(expanded_results) < limit:
                remaining = limit - len(expanded_results)
                query_tc3 = {
                    "TopCountry3": {"$regex": country, "$options": "i"},
                    "_id": {"$nin": [r["_id"] for r in expanded_results]}
                }
                results_tc3 = list(tracks_col.find(query_tc3).sort("PopularityScore", -1).limit(remaining))
                expanded_results.extend(results_tc3)
            
            results = expanded_results
            logger.debug(f"🔁 Búsqueda expandida jerárquica de país, resultados obtenidos: {len(results)}")
        
        # Intentar primero sin género
        if not results and "Genero" in mongo_filters:
            fallback_filters = dict(mongo_filters)
            del fallback_filters["Genero"]
            results = list(tracks_col.find(fallback_filters))
            logger.debug(f"🔁 Búsqueda expandida sin 'Genero', resultados obtenidos: {len(results)}")
        
        # Si aún no hay resultados, intentar sin década
        if not results and "Decada" in mongo_filters:
            fallback_filters2 = dict(mongo_filters)
            del fallback_filters2["Decada"]
            # También quitar Año si existe
            fallback_filters2.pop("Año", None)
            results = list(tracks_col.find(fallback_filters2))
            logger.debug(f"🔁 Búsqueda expandida sin 'Decada', resultados obtenidos: {len(results)}")

    # 6️⃣ Calcular métricas y ordenar
    global_max = get_global_max_values()
    for t in results:
        t["PopularityScore"] = compute_popularity(t, global_max)

    results = deduplicate_tracks_by_title_keep_best(results)
    compute_relative_popularity_by_genre(results)
    cleaned_results = filter_gross_incongruities(results, query_clean)
    cleaned_results = apply_limits_and_fallback(cleaned_results, query_clean, limit)

    cleaned_results.sort(key=lambda x: x.get(sort_by or "RelativePopularityScore", 0), reverse=True)
    final_results = cleaned_results[:limit]

    # 7️⃣ Estructura simplificada
    simplified = [{
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
        "RelativePopularityScore": t.get("RelativePopularityScore"),
        "PopularityDisplay": popularity_display(t.get("RelativePopularityScore")),
    } for t in final_results]

    # 8️⃣ Guardar resultados y registro CON USUARIO
    m3u_path, playlist_uuid = save_m3u(simplified, re.sub(r"[^\w\s-]", "", query_clean)[:60])
    
    # ✅ GENERAR NOMBRE AMIGABLE
    playlist_name = query_text[:80]  # Usar el query como nombre, truncado
    if len(simplified) > 0:
        # Intentar crear nombre más descriptivo
        main_genre = simplified[0].get("Genero", "")
        if isinstance(main_genre, list) and main_genre:
            main_genre = main_genre[0]
        country_part = f" de {country}" if country else ""
        playlist_name = f"{country_part} - {query_text[:40]}..." if main_genre else f"{query_text[:60]}{country_part}"

    # ✅ Obtener estadísticas de distribución TopCountry si es país
    topcountry_stats = {}
    if country and country_type != "origin":
        topcountry_stats = get_topcountry_distribution(final_results, country)

    playlist_doc = {
        "query_original": query_text,
        "name": playlist_name,  # ✅ NOMBRE PARA MOSTRAR
        "filters": mongo_filters,
        "limit": limit,
        "created_at": start_ts,
        "m3u_path": m3u_path,
        "playlist_uuid": playlist_uuid,
        "items": simplified,
        "stats": {
            "total": len(simplified), 
            "standard_mode": True, 
            "country": country if country else None,
            "country_type": country_type if country else None,
            "regenerated": regenerate,
            "topcountry_distribution": topcountry_stats  # ✅ NUEVO: Estadísticas
        },
        "feedback_pending": True,
        "user_email": user_email,  # ✅ ASOCIADO AL USUARIO
        "type": "country" if country else "standard"  # ✅ TIPO DE PLAYLIST
    }
    
    try:
        res = playlists_col.insert_one(playlist_doc)
        playlist_id = str(res.inserted_id)
        logger.debug(f"💾 Playlist {'PAÍS' if country else 'ESTÁNDAR'} guardada con id {playlist_id} para usuario {user_email}")
    except Exception as e:
        logger.exception(f"Error inserting playlist doc ({'country' if country else 'standard'} mode): {e}")
        playlist_id = None

    # 9️⃣ Respuesta final
    debug_summary = {
        "standard_mode": True,
        "llm_analysis": llm_analysis,
        "filters_applied": mongo_filters,
        "excluded_count": len(excluded_titles),
    }
    
    # ✅ Añadir información de país al debug summary si está presente
    if country:
        debug_summary["country_mode"] = True
        debug_summary["country"] = country
        debug_summary["country_type"] = country_type
        debug_summary["topcountry_distribution"] = topcountry_stats  # ✅ NUEVO

    return {
        "query_original": query_text,
        "playlist_name": playlist_name,  # ✅ NOMBRE PARA EL FRONTEND
        "filtros": mongo_filters,
        "criterio_orden": sort_by or "RelativePopularityScore",
        "total": len(simplified),
        "playlist": simplified,
        "archivo_m3u": m3u_path,
        "playlist_id": playlist_id,
        "playlist_uuid": playlist_uuid,
        "user_email": user_email,  # ✅ INCLUIR EMAIL EN RESPUESTA
        "debug_summary": debug_summary,
    }

# -----------------------
# Helper: popularity display (based on relative score)
# -----------------------
def popularity_display(score: Optional[float]) -> str:
    if score is None:
        return "N/A"
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

# -----------------------
# feedback endpoint
# -----------------------
@app.post("/feedback")
def feedback(body: FeedbackIn):
    if body.rating < 1 or body.rating > 10:
        raise HTTPException(status_code=400, detail="rating must be 1..10")
    try:
        pid = ObjectId(body.playlist_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid playlist_id")
    pl = playlists_col.find_one({"_id": pid})
    if not pl:
        raise HTTPException(status_code=404, detail="playlist not found")
    doc = {
        "playlist_id": body.playlist_id,
        "rating": int(body.rating),
        "comment": body.comment or "",
        "created_at": datetime.utcnow()
    }
    feedback_col.insert_one(doc)
    playlists_col.update_one({"_id": pid}, {"$set": {"user_rating": int(body.rating), "feedback_pending": False}})
    return {"ok": True, "msg": "feedback registrado"}

# -----------------------
# get playlist by id (MEJORADO con opción de seguridad)
# -----------------------
@app.get("/playlist/{pid}")
def get_playlist(pid: str, request: Request = None, user_check: bool = False):
    """
    Obtiene una playlist por ID.
    Si user_check=True, verifica que pertenezca al usuario autenticado.
    """
    try:
        oid = ObjectId(pid)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid id")
    
    # Construir query base
    query = {"_id": oid}
    
    # Si se solicita verificación de usuario
    if user_check and request:
        try:
            auth_header = request.headers.get("Authorization")
            if auth_header and "Bearer" in auth_header:
                token = auth_header.replace("Bearer ", "").strip()
                user = db_auth.users.find_one({"session_token": token})
                if user:
                    query["user_email"] = user.get("email")
        except Exception as e:
            logger.warning(f"Error en verificación de usuario: {e}")
    
    p = playlists_col.find_one(query)
    if not p:
        raise HTTPException(status_code=404, detail="playlist not found")
    
    # Convertir ObjectId a string
    p["id"] = str(p["_id"])
    p.pop("_id", None)
    
    # ✅ Asegurar URLs de streaming
    if "items" in p and isinstance(p["items"], list):
        for item in p["items"]:
            if item.get("Ruta"):
                item["StreamURL"] = convert_path_to_url(item["Ruta"])
            if item.get("CoverCarpeta"):
                item["CoverURL"] = convert_path_to_url(item.get("CoverCarpeta"))
    
    return p

# -----------------------
# Root
# -----------------------
@app.get("/")
def root():
    return {"msg": "NeoPlaylist API (V15 híbrida) operativa 🚀"}

# -----------------------
# Main (run with uvicorn)
# -----------------------
if __name__ == "__main__":
    uvicorn.run("playlist_api_refinedV15enchanced:app", host="0.0.0.0", port=8000, reload=True)


def call_ollama_safe(prompt_text: str, model: str = "neoplaylist-agent", timeout: int = 40):
    """
    Invoca el modelo Ollama de manera segura y tolerante a errores.
    - Maneja respuestas JSON mal formadas de forma robusta
    - Extrae y repara JSON de texto mixto
    - Devuelve SIEMPRE un dict válido
    """
    OLLAMA_URL = "http://localhost:11434/api/generate"
    payload = {"model": model, "prompt": prompt_text, "stream": False}
    logging.info(f"🧠 Llamando a Ollama ({model}) con timeout={timeout}s")

    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        # 🔍 Extraer texto de respuesta de forma robusta
        raw_text = ""
        if isinstance(data, dict):
            raw_text = (
                data.get("response", "")
                or data.get("output", "")
                or data.get("text", "")
                or (data.get("message", {}).get("content") if isinstance(data.get("message"), dict) else "")
                or str(data)
            )
        elif isinstance(data, str):
            raw_text = data
        else:
            raw_text = str(data)

        if not raw_text.strip():
            logging.warning("⚠️ Ollama devolvió respuesta vacía.")
            return {}

        # ✅ INTENTAR PARSEAR DIRECTAMENTE PRIMERO
        try:
            parsed = json.loads(raw_text.strip())
            logging.info("✅ JSON parseado directamente sin reparación")
            return parsed
        except json.JSONDecodeError:
            pass  # Proceder con reparación

        # 🛠️ REPARACIÓN ROBUSTA DE JSON
        repaired_json = _repair_json_response(raw_text)
        if repaired_json:
            logging.info("✅ JSON reparado exitosamente")
            return repaired_json

        # 🔍 SI LA REPARACIÓN FALLA, intentar extraer objeto JSON con métodos más agresivos
        json_candidates = _extract_json_candidates(raw_text)
        for candidate in json_candidates:
            try:
                parsed = json.loads(candidate)
                logging.info("✅ JSON extraído con método agresivo")
                return parsed
            except json.JSONDecodeError:
                continue

        # 📝 SI TODO FALLA, crear respuesta básica con el texto
        logging.warning("⚠️ No se pudo extraer JSON válido, devolviendo estructura básica")
        return {"raw_response": raw_text[:500], "error": "no_se_pudo_parsear_json"}

    except requests.Timeout:
        logging.warning(f"⏰ Timeout al consultar Ollama ({timeout}s)")
        return {"error": "timeout"}
    except requests.RequestException as e:
        logging.error(f"❌ Error HTTP al consultar Ollama: {e}")
        return {"error": f"http_error: {str(e)}"}
    except Exception as e:
        logging.error(f"❌ Error inesperado en call_ollama_safe: {e}")
        return {"error": f"unexpected_error: {str(e)}"}


def _repair_json_response(raw_text: str) -> Optional[Dict]:
    """
    Repara respuestas JSON mal formadas del modelo.
    Maneja múltiples escenarios comunes de errores.
    """
    if not raw_text:
        return None

    text = raw_text.strip()
    
    # 1️⃣ ELIMINAR BLOQUES MARKDOWN
    text = re.sub(r'^```[a-zA-Z]*\n', '', text)  # Inicio de código
    text = re.sub(r'\n```$', '', text)           # Fin de código
    text = re.sub(r'^`|`$', '', text)            # Backticks sueltos
    
    # 2️⃣ CORREGIR COMILLAS
    text = text.replace('“', '"').replace('”', '"').replace("'", '"')
    text = text.replace('\\"', '"')  # Unescape comillas
    text = re.sub(r'(?<!\\)"', '"', text)  # Normalizar comillas
    
    # 3️⃣ CORREGIR COMILLAS SIMPLES EN STRINGS (pero mantener en JSON válido)
    # Reemplazar 'texto' por "texto" pero NO afectar comillas simples válidas en JSON
    text = re.sub(r"'(.*?)'(?=\s*[:,\]}])", r'"\1"', text)  # Solo en contexto de clave/valor
    
    # 4️⃣ ELIMINAR COMENTARIOS Y TEXTO EXTRA
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        line = line.strip()
        # Eliminar líneas que son claramente comentarios o instrucciones
        if line.startswith('//') or line.startswith('/*') or line.startswith('*') or line.startswith('#') or 'aquí' in line.lower():
            continue
        # Eliminar líneas que no contienen estructura JSON
        if not any(char in line for char in ['{', '}', '[', ']', ':', '"']):
            continue
        cleaned_lines.append(line)
    
    text = ' '.join(cleaned_lines)
    
    # 5️⃣ CORREGIR PROBLEMAS DE SINTAXIS COMUNES
    # Comas sobrantes antes de } o ]
    text = re.sub(r',\s*([}\]])', r'\1', text)
    # Puntos y comas en lugar de comas
    text = text.replace(';', ',')
    # Claves sin comillas
    text = re.sub(r'(\w+)\s*:', r'"\1":', text)
    # Valores booleanos mal escritos
    text = re.sub(r':\s*True\b', ':true', text, flags=re.IGNORECASE)
    text = re.sub(r':\s*False\b', ':false', text, flags=re.IGNORECASE)
    text = re.sub(r':\s*None\b', ':null', text, flags=re.IGNORECASE)
    
    # 6️⃣ EXTRAER SOLO EL BLOQUE JSON MÁS PROBABLE
    json_blocks = re.findall(r'\{[^{}]*\{[^{}]*\}[^{}]*\}|\{[^{}]*\}', text)
    if json_blocks:
        # Tomar el bloque más largo (más probable que sea completo)
        text = max(json_blocks, key=len)
    
    # 7️⃣ INTENTAR PARSEAR
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logging.debug(f"🔧 Intento de reparación falló: {e}")
        logging.debug(f"🔧 Texto reparado: {text[:200]}...")
        return None


def _extract_json_candidates(raw_text: str) -> List[str]:
    """
    Extrae candidatos a JSON del texto usando métodos más agresivos.
    """
    candidates = []
    
    # Método 1: Buscar entre llaves más externas
    brace_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', raw_text)
    candidates.extend(brace_matches)
    
    # Método 2: Buscar entre corchetes (para arrays)
    bracket_matches = re.findall(r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]', raw_text)
    candidates.extend(bracket_matches)
    
    # Método 3: Buscar desde el primer { hasta el último }
    start_idx = raw_text.find('{')
    end_idx = raw_text.rfind('}')
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        candidates.append(raw_text[start_idx:end_idx+1])
    
    # Método 4: Buscar desde el primer [ hasta el último ]
    start_idx = raw_text.find('[')
    end_idx = raw_text.rfind(']')
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        candidates.append(raw_text[start_idx:end_idx+1])
    
    # Filtrar y ordenar por longitud (los más largos suelen ser más completos)
    candidates = [c for c in candidates if 10 <= len(c) <= 10000]  # Longitudes razonables
    candidates.sort(key=len, reverse=True)
    
    return candidates



# =========================================================
# 🔍 1. Detección de tipo de entidad desde el prompt
# =========================================================
def detect_artist_album_track(prompt, tracks_col):
    """
    Detecta si el prompt menciona un artista, álbum o pista existente.
    Devuelve {'tipo': 'artista'|'album'|'track'|None, 'nombre': '...'}
    """
    prompt_norm = prompt.strip().lower()

    patterns = [
        ("artista", "Artista"),
        ("album", "Album"),
        ("track", "Titulo")
    ]

    # Coincidencia exacta primero
    for tipo, campo in patterns:
        result = tracks_col.find_one(
            {campo: {"$regex": f"^{re.escape(prompt_norm)}$", "$options": "i"}},
            {campo: 1}
        )
        if result:
            return {"tipo": tipo, "nombre": result[campo]}

    # Luego coincidencias parciales
    for tipo, campo in patterns:
        result = tracks_col.find_one(
            {campo: {"$regex": prompt_norm, "$options": "i"}},
            {campo: 1}
        )
        if result:
            return {"tipo": tipo, "nombre": result[campo]}

    return {"tipo": None, "nombre": None}


# =========================================================
# 🎵 2. Resumen local de características del artista
# =========================================================
def summarize_artist_features(artist_name, tracks_col):
    """
    Resume las características promedio o dominantes de un artista.
    Devuelve un dict con Genero, TempoBPM, EMO_Sound, EMO_Lyrics.
    """
    tracks = list(tracks_col.find(
        {"Artista": {"$regex": f"^{re.escape(artist_name)}$", "$options": "i"}},
        {"Genero": 1, "TempoBPM": 1, "EMO_Sound": 1, "EMO_Lyrics": 1}
    ))

    if not tracks:
        logger.debug(f"No se encontraron pistas para el artista '{artist_name}'")
        return None

    generos = [t.get("Genero") for t in tracks if t.get("Genero")]
    emos_sound = [t.get("EMO_Sound") for t in tracks if t.get("EMO_Sound")]
    emos_lyrics = [t.get("EMO_Lyrics") for t in tracks if t.get("EMO_Lyrics")]
    tempos = [t.get("TempoBPM") for t in tracks if isinstance(t.get("TempoBPM"), (int, float))]

    def most_common(lst):
        return Counter(lst).most_common(1)[0][0] if lst else None

    resumen = {
        "Genero": most_common(generos),
        "TempoBPM": round(mean(tempos), 1) if tempos else None,
        "EMO_Sound": most_common(emos_sound),
        "EMO_Lyrics": most_common(emos_lyrics)
    }

    logger.debug(f"🎧 Perfil promedio de '{artist_name}': {resumen}")
    return resumen


# =========================================================
# 🤖 3. Fallback con LLM (solo si hay pocos datos)
# =========================================================
def summarize_artist_features_ai(artist_name, sample_tracks, llm=None):
    """
    Si hay pocas pistas, obtiene un resumen estimado de características usando un modelo LLM.
    """
    if not sample_tracks:
        return None

    if llm is None:
        logger.debug("⚠️ summarize_artist_features_ai fue llamado sin LLM disponible.")
        return None

    context = "\n".join([
        f"- {t.get('Titulo', 'Sin título')} ({t.get('Genero', '?')}, {t.get('TempoBPM', '?')} BPM, {t.get('EMO_Sound', '?')})"
        for t in sample_tracks[:10]
    ])

    prompt = f"""
Analiza las siguientes pistas del artista {artist_name} y devuelve un JSON con los valores predominantes:
{context}

Formato JSON de salida:
{{
  "Genero": "...",
  "TempoBPM": <aproximado>,
  "EMO_Sound": "...",
  "EMO_Lyrics": "..."
}}
    """

    try:
        result = llm(prompt)
        if isinstance(result, dict):
            resumen = result
        else:
            resumen = json.loads(result)
        logger.debug(f"🤖 Resumen AI de '{artist_name}': {resumen}")
        return resumen
    except Exception as e:
        logger.exception(f"Error en summarize_artist_features_ai: {e}")
        return None


# =========================================================
# 🧩 4. Búsqueda de artistas similares
# =========================================================
def find_similar_artists(artist_name, tracks_col, llm=None, limit=5):
    """
    Busca artistas similares basándose en las características promedio del artista dado.
    """
    base_tracks = list(tracks_col.find(
        {"Artista": {"$regex": f"^{re.escape(artist_name)}$", "$options": "i"}},
        {"Titulo": 1, "Genero": 1, "TempoBPM": 1, "EMO_Sound": 1, "EMO_Lyrics": 1}
    ))

    if not base_tracks:
        logger.debug(f"⚠️ No se encontraron pistas para el artista '{artist_name}' en base local.")
        return []

    if len(base_tracks) >= 3:
        resumen = summarize_artist_features(artist_name, tracks_col)
    else:
        resumen = summarize_artist_features_ai(artist_name, base_tracks, llm)

    if not resumen:
        logger.debug(f"⚠️ No se pudo generar resumen de '{artist_name}' para similitud.")
        return []

    query = {
        "Genero": {"$regex": resumen.get("Genero") or "", "$options": "i"},
        "TempoBPM": {
            "$gte": max((resumen.get("TempoBPM") or 0) - 10, 0),
            "$lte": (resumen.get("TempoBPM") or 0) + 10
        },
        "EMO_Sound": {"$regex": resumen.get("EMO_Sound") or "", "$options": "i"},
        "Artista": {"$ne": artist_name},
    }

    similars = list(tracks_col.find(query).sort("RelativePopularityScore", -1).limit(limit))
    logger.debug(f"🎯 Artistas similares a '{artist_name}': encontrados {len(similars)} resultados")
    return similars


# =========================================================
# 🏆 5. Fallback principal cuando se pide “Lo mejor de X”
# =========================================================
def get_best_of_artist(artist_name, tracks_col, limit=15, llm=None):
    """
    Devuelve las canciones más populares de un artista basadas en:
    1. LastFMPlaycount (prioritario)
    2. YouTubeViews (fallback)
    3. Mejor bitrate entre versiones duplicadas
    ✅ RESPETA EL LÍMITE SOLICITADO
    """
    
    logger.debug(f"🎸 Buscando TOP {limit} canciones de '{artist_name}'")

    # 1️⃣ Buscar pistas del artista (match flexible) con LÍMITE
    query = {"Artista": {"$regex": artist_name, "$options": "i"}}
    
    # Obtener TODAS las pistas primero para poder ordenar por popularidad
    all_tracks = list(tracks_col.find(query))
    
    if not all_tracks:
        logger.debug(f"⚠️ No se encontraron canciones de '{artist_name}', buscando similares...")
        return find_similar_artists(artist_name, tracks_col, llm, limit=min(limit, 5))

    logger.debug(f"🎧 {len(all_tracks)} pistas encontradas para '{artist_name}'")

    # 2️⃣ Normalizar nombre de pista (quita sufijos: versiones, remasters, etc.)
    def normalize_title(title: str):
        if not title:
            return ""
        title_clean = re.sub(r"\(.*?\)", "", title, flags=re.I)  # quita paréntesis
        title_clean = re.sub(r"[-_]", " ", title_clean).strip().lower()
        title_clean = re.sub(r"\s+", " ", title_clean)
        return title_clean

    # 3️⃣ Agrupar versiones del mismo tema
    grouped = {}
    for t in all_tracks:
        norm_title = normalize_title(t.get("Titulo", ""))
        if not norm_title:
            continue
        playcount = t.get("LastFMPlaycount") or 0
        ytviews = t.get("YouTubeViews") or 0
        bitrate = t.get("Bitrate") or 0
        score = playcount if playcount > 0 else ytviews
        current_best = grouped.get(norm_title)
        if not current_best or (score > (current_best.get("score") or 0)) or (
            score == (current_best.get("score") or 0) and bitrate > (current_best.get("Bitrate") or 0)
        ):
            grouped[norm_title] = {**t, "score": score}

    # 4️⃣ Filtrar y ordenar por score descendente
    deduped = list(grouped.values())
    deduped.sort(key=lambda x: (x.get("score", 0), x.get("Bitrate", 0)), reverse=True)

    logger.debug(f"🏆 {len(deduped)} pistas únicas tras deduplicación y ranking")

    # 5️⃣ ✅ APLICAR EL LÍMITE SOLICITADO (no el límite hardcodeado)
    best_tracks = deduped[:limit]

    logger.debug(f"🎯 Devolviendo TOP {len(best_tracks)} canciones de '{artist_name}' (límite solicitado: {limit})")

    # 6️⃣ Log de diagnóstico (opcional)
    for i, t in enumerate(best_tracks[:5]):  # Solo log primeras 5 para no saturar
        logger.debug(
            f"🎵 #{i+1}: {t.get('Titulo')} | {t.get('Album')} | "
            f"LastFMPlaycount={t.get('LastFMPlaycount', 0)} | "
            f"YouTubeViews={t.get('YouTubeViews', 0)} | "
            f"Bitrate={t.get('Bitrate', 0)}"
        )

    return best_tracks




# =========================================================
# 🧠 6. Análisis semántico del prompt (LLM vUnified)
# =========================================================
def llm_prompt_intent_analysis(prompt: str, llm=None) -> dict:
    """
    Usa el modelo local NeoPlaylist (Ollama) para analizar la intención del prompt musical.
    Devuelve un JSON estructurado con los campos:
    - type: tipo de petición (artist_request, genre_or_mood_request, etc.)
    - artist, album, track, genre, mood, decade, intent
    """
    try:
        system_prompt = """
Analiza este prompt musical y determina qué tipo de petición es.
Devuelve **EXCLUSIVAMENTE JSON válido**, sin texto adicional.

Tipos posibles:
- "artist_request": cuando el usuario pide algo sobre un artista (ej. "lo mejor de Metallica")
- "album_request": cuando pide un álbum específico
- "track_request": cuando pide una canción puntual
- "similar_to_request": cuando menciona "similares a", "parecidas a", "similar to"
- "genre_or_mood_request": cuando menciona géneros, emociones, décadas o estilos (ej. "lo mejor del rock de los 80s", "música para relajarse")

Estructura esperada:
{
  "type": "...",
  "artist": "...",
  "album": "...",
  "track": "...",
  "genre": "...",
  "mood": "...",
  "decade": "...",
  "intent": "..."
}
"""

        if llm is None:
            logger.debug("⚠️ LLM no disponible, devolviendo tipo genérico.")
            return {"type": "genre_or_mood_request", "intent": "análisis básico sin LLM"}

        # 🔹 Solicitud directa al modelo local sin argumentos no soportados
        response = llm(prompt=prompt, system=system_prompt)

        # Si el modelo devuelve texto, intenta extraer el JSON limpio
        if isinstance(response, str):
            json_start = response.find("{")
            json_end = response.rfind("}")
            if json_start != -1 and json_end != -1:
                response = response[json_start:json_end + 1]
            try:
                result = json.loads(response)
            except json.JSONDecodeError:
                logger.warning(f"⚠️ LLM devolvió JSON inválido: {response}")
                result = {"type": "genre_or_mood_request", "intent": "fallback por error JSON"}
        else:
            result = response

        # Validación mínima de claves
        if "type" not in result:
            result["type"] = "genre_or_mood_request"
        result.setdefault("intent", f"Interpretar '{prompt}'")

        logger.debug(f"🧩 Intent analysis result: {result}")
        return result

    except Exception as e:
        logger.warning(f"⚠️ Intent analysis failed: {e}")
        return {"type": "genre_or_mood_request", "intent": f"error: {str(e)}"}



# =========================================================
# 🧠 Función auxiliar: Ejecutar modelo LLM local (Ollama)
# =========================================================
def run_local_llm(prompt: str) -> str:
    """
    Envía un prompt al modelo local con manejo robusto de errores.
    """
    OLLAMA_URL = "http://localhost:11434/api/generate"
    model = "neoplaylist-agent"

    payload = {"model": model, "prompt": prompt, "stream": False}
    
    try:
        res = requests.post(OLLAMA_URL, json=payload, timeout=40)
        res.raise_for_status()
        data = res.json()
        
        raw_text = data.get("response") or data.get("output") or data.get("text") or ""
        
        # Limpieza básica
        if raw_text:
            # Eliminar instrucciones de formato comunes
            cleaned = re.sub(r'^```json\s*', '', raw_text)
            cleaned = re.sub(r'```\s*$', '', cleaned)
            return cleaned.strip()
        
        return "{}"
        
    except Exception as e:
        logger.error(f"❌ Error en run_local_llm: {e}")
        return "{}"
        
        
def analyze_query_intent(query_text: str) -> Dict[str, Any]:
    """
    Clasifica la intención de una solicitud musical con mejor detección de países y años.
    """
    
    # Primero hacer análisis de país
    country_analysis = detect_country_intent(query_text)
    
    prompt = f"""
    Analiza la siguiente consulta musical y extrae:
    1. El tipo de petición
    2. El límite numérico explícito
    3. Las entidades musicales (artista, género, década, año específico, país)
    4. Si menciona un país, determina si es ORIGEN del artista o POPULARIDAD en ese país

    Consulta: "{query_text}"

    🔍 **DETECCIÓN DE PAÍSES:**
    - "música chilena" → país: "Chile", tipo: "origin"  
    - "artistas de Chile" → país: "Chile", tipo: "origin"
    - "lo más escuchado en Chile" → país: "Chile", tipo: "popular_in"
    - "popular en Argentina" → país: "Argentina", tipo: "popular_in"

    🔍 **DETECCIÓN DE TIEMPO:**
    - "años 80" o "década de los 80" → década: "1980s"
    - "los 80s y 90s" → década: ["1980s", "1990s"]  
    - "2015" o "del 2015" → año: 2015 (NO década)
    - "entre 2010 y 2015" → year_range: {{"from": 2010, "to": 2015}}

    🔍 **DETECCIÓN DE LÍMITES:**
    - "top 10" → limit: 10
    - "10 canciones" → limit: 10

    Devuelve EXCLUSIVAMENTE JSON válido con este formato:

    {{
      "type": "artist_request|similar_to_request|genre_or_mood_request|country_request",
      "artist": "",
      "track": "", 
      "album": "",
      "genre": "",
      "mood": "",
      "decade": "", // para décadas: "1980s", "1990s" o ["1980s", "1990s"]
      "year": null, // para año específico: 2015
      "year_range": {{"from": 2010, "to": 2015}}, // para rangos de años
      "country": "", // país detectado
      "country_type": "origin|popular_in", // tipo de filtro por país
      "limit": 10,
      "intent": "descripción de la intención"
    }}

    Ejemplos:
    - "música chilena" → "country": "Chile", "country_type": "origin", "type": "country_request"
    - "lo más escuchado en Chile" → "country": "Chile", "country_type": "popular_in", "type": "country_request"  
    - "rock de los 80s" → "decade": "1980s", "genre": "rock"
    - "lo mejor del 2015" → "year": 2015
    - "música entre 2010 y 2015" → "year_range": {{"from": 2010, "to": 2015}}
    """
    
    try:
        raw_response = run_local_llm(prompt)
        logger.debug(f"🔍 Raw response from Ollama: {raw_response}")
        
        analysis = parse_ollama_json_response(raw_response)
        
        # ✅ MEJORA: Combinar con análisis de país automático
        if country_analysis["has_country_intent"]:
            analysis["country"] = country_analysis["country"]
            analysis["country_type"] = country_analysis["country_type"] 
            analysis["type"] = "country_request"
            logger.debug(f"🇨🇱 Detección de país automática: {country_analysis}")
        
        # ✅ DETECCIÓN DIRECTA DE "top X" como fallback
        if analysis.get("limit") is None:
            direct_limit = extract_limit_directly(query_text)
            if direct_limit:
                analysis["limit"] = direct_limit
                logger.debug(f"🔢 Límite detectado directamente: {direct_limit}")

        # Validar y normalizar el límite
        detected_limit = validate_and_normalize_limit(analysis.get("limit"), query_text)
        analysis["detected_limit"] = detected_limit

        logger.debug(f"🧩 Intent analysis result: {analysis}")
        return analysis

    except Exception as e:
        logger.warning(f"⚠️ Intent analysis failed: {e}")
        return get_improved_fallback_analysis(query_text)

def extract_limit_directly(query_text: str) -> Optional[int]:
    """
    Detección directa y robusta de límites en el texto.
    """
    text_lower = query_text.lower()
    
    # Patrones más específicos para "top X"
    patterns = [
        r'\btop\s+(\d+)\b',                    # "top 10"
        r'\b(\d+)\s+canciones?\b',             # "10 canciones"  
        r'\b(\d+)\s+temas?\b',                 # "5 temas"
        r'\b(\d+)\s+pistas?\b',                # "8 pistas"
        r'\bprimer[oa]s?\s+(\d+)\b',           # "primeras 5"
        r'\b(\d+)\s+mejores\b',                # "10 mejores"
        r'\blas\s+(\d+)\s+mejores\b',          # "las 20 mejores"
        r'\b(\d+)\s+grandes\s+éxitos\b',       # "15 grandes éxitos"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text_lower)
        if matches:
            try:
                limit = int(matches[0])
                # Validar que sea un límite razonable, no un año
                if 1 <= limit <= 50 and not is_likely_year_in_context(limit, query_text):
                    logger.debug(f"🎯 Límite detectado por patrón '{pattern}': {limit}")
                    return limit
            except (ValueError, IndexError):
                continue
    
    return None
    
def parse_ollama_json_response(raw_response: str) -> Dict[str, Any]:
    """
    Parsea de forma robusta la respuesta JSON de Ollama.
    Maneja comillas simples, JSON mal formado, y texto extra.
    """
    if not raw_response:
        return get_default_analysis()
    
    logger.debug(f"🔧 Raw response para parsing: {raw_response[:500]}...")
    
    # ✅ INTENTAR PARSING DIRECTO PRIMERO
    try:
        parsed = json.loads(raw_response)
        logger.debug("✅ JSON parseado directamente")
        return parsed
    except json.JSONDecodeError:
        logger.debug("⚠️ JSON directo falló, intentando limpieza...")
    
    # ✅ LIMPIAR Y REPARAR LA RESPUESTA - MÁS AGRESIVO
    cleaned_response = clean_ollama_response(raw_response)
    
    # ✅ INTENTAR PARSING CON LA RESPUESTA LIMPIA
    try:
        parsed = json.loads(cleaned_response)
        logger.debug("✅ JSON parseado después de limpieza")
        return parsed
    except json.JSONDecodeError as e:
        logger.debug(f"⚠️ JSON limpio también falló: {e}")
    
    # ✅ EXTRAER JSON CON MÉTODOS MÁS AGRESIVOS
    json_candidates = extract_json_candidates(cleaned_response)
    
    for candidate in json_candidates:
        try:
            parsed = json.loads(candidate)
            logger.debug(f"✅ JSON extraído con método agresivo: {candidate[:100]}...")
            return parsed
        except json.JSONDecodeError:
            continue
    
    # ✅ SI TODO FALLA, BUSCAR PATRONES ESPECÍFICOS EN EL TEXTO
    analysis = extract_analysis_from_text(raw_response)
    if analysis:
        logger.debug("✅ Análisis extraído del texto")
        return analysis
    
    # ✅ ÚLTIMO RECURSO: USAR ANÁLISIS POR DEFECTO
    logger.warning("⚠️ No se pudo parsear JSON de Ollama, usando análisis por defecto")
    return get_default_analysis()

def extract_analysis_from_text(text: str) -> Dict[str, Any]:
    """
    Extrae análisis de intención directamente del texto cuando el JSON falla - MEJORADA.
    """
    analysis = get_default_analysis()
    text_lower = text.lower()
    
    # Buscar país en el texto
    country_patterns = {
        "chile": "Chile", "chilena": "Chile", "chileno": "Chile",
        "argentina": "Argentina", "mexico": "Mexico", "méxico": "Mexico",
        "españa": "Spain", "colombia": "Colombia", "brasil": "Brazil",
        "perú": "Peru", "eeuu": "United States", "estados unidos": "United States"
    }
    
    for term, country in country_patterns.items():
        if term in text_lower:
            analysis["country"] = country
            
            # Determinar tipo de país basado en contexto
            if any(pop_term in text_lower for pop_term in ["popular en", "escuchado en", "más sonado", "éxitos en"]):
                analysis["country_type"] = "popular_in"
            else:
                analysis["country_type"] = "origin"
                
            analysis["type"] = "country_request"
            logger.debug(f"🇨🇱 País detectado en texto: {country} ({analysis['country_type']})")
            break
    
    # Buscar límites en el texto
    limit_match = re.search(r'"limit":\s*(\d+)', text)
    if limit_match:
        try:
            limit = int(limit_match.group(1))
            if 1 <= limit <= 50:
                analysis["limit"] = limit
                analysis["detected_limit"] = limit
                logger.debug(f"🔢 Límite detectado en texto: {limit}")
        except (ValueError, TypeError):
            pass
    
    # Buscar tipo de solicitud
    if "country_request" in text_lower or "país" in text_lower or "country" in text_lower:
        analysis["type"] = "country_request"
    elif "artist_request" in text_lower:
        analysis["type"] = "artist_request"
    elif "similar" in text_lower:
        analysis["type"] = "similar_to_request"
    
    return analysis
    
def clean_ollama_response(response: str) -> str:
    """
    Limpia la respuesta de Ollama para hacerla JSON válido - MEJORADA.
    """
    if not response:
        return "{}"
    
    # Eliminar markdown code blocks
    cleaned = re.sub(r'```json\s*', '', response)
    cleaned = re.sub(r'```\s*', '', cleaned)
    
    # Eliminar comentarios de una línea (// comentario)
    cleaned = re.sub(r'//[^\n]*', '', cleaned)
    
    # Eliminar comentarios multi-línea (/* comentario */)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    
    # Reemplazar comillas simples por dobles (solo en claves y valores string)
    cleaned = re.sub(r"'([^']*)'", r'"\1"', cleaned)
    
    # Corregir problemas comunes de formato
    cleaned = re.sub(r',\s*}', '}', cleaned)  # Comas sobrantes antes de }
    cleaned = re.sub(r',\s*]', ']', cleaned)  # Comas sobrantes antes de ]
    cleaned = re.sub(r'(\w+):', r'"\1":', cleaned)  # Claves sin comillas
    
    # Normalizar valores booleanos y null
    cleaned = re.sub(r':\s*true\b', ':true', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r':\s*false\b', ':false', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r':\s*null\b', ':null', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r':\s*None\b', ':null', cleaned, flags=re.IGNORECASE)
    
    # Extraer solo el bloque JSON más probable
    json_match = re.search(r'\{[^{}]*\{[^{}]*\}[^{}]*\}|\{[^{}]*\}', cleaned)
    if json_match:
        cleaned = json_match.group(0)
    
    return cleaned.strip()


def extract_json_candidates(text: str) -> List[str]:
    """
    Extrae candidatos a JSON del texto.
    """
    candidates = []
    
    # Buscar entre llaves
    brace_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text)
    candidates.extend(brace_matches)
    
    # Buscar desde el primer { hasta el último }
    start_idx = text.find('{')
    end_idx = text.rfind('}')
    if start_idx != -1 and end_idx != -1 and start_idx < end_idx:
        candidates.append(text[start_idx:end_idx+1])
    
    # Filtrar por longitud razonable
    candidates = [c for c in candidates if 20 <= len(c) <= 5000]
    candidates.sort(key=len, reverse=True)  # Los más largos primero
    
    return candidates


def get_default_analysis() -> Dict[str, Any]:
    """
    Retorna un análisis por defecto cuando falla el parsing.
    """
    return {
        "type": "genre_or_mood_request", 
        "artist": "", 
        "track": "", 
        "album": "", 
        "genre": "", 
        "mood": "", 
        "decade": "",
        "limit": None,
        "intent": "Análisis por defecto",
        "detected_limit": 30
    }


def get_improved_fallback_analysis(query_text: str) -> Dict[str, Any]:
    """
    Análisis de fallback mejorado con detección básica.
    """
    text_lower = query_text.lower()
    
    # Detección básica de tipo
    if re.search(r"(similares a|parecidas a|similar to)", text_lower, re.I):
        ptype = "similar_to_request"
    elif re.search(r"(mejor de|best of|grandes éxitos|top de)", text_lower, re.I):
        ptype = "artist_request"
    else:
        ptype = "genre_or_mood_request"
    
    # Detección básica de década
    decade = None
    if "80" in text_lower or "ochenta" in text_lower:
        decade = "1980s"
    elif "90" in text_lower or "noventa" in text_lower:
        decade = "1990s"
    elif "2000" in text_lower or "dos mil" in text_lower:
        decade = "2000s"
    elif "70" in text_lower or "setenta" in text_lower:
        decade = "1970s"
    
    return {
        "type": ptype, 
        "artist": "", 
        "track": "", 
        "album": "", 
        "genre": "", 
        "mood": "", 
        "decade": decade,
        "limit": None,
        "detected_limit": 30,
        "intent": f"Fallback: {query_text}"
    }


def resolve_temporal_references(analysis: Dict[str, Any], current_year: int, current_decade: int, previous_decade: int) -> Dict[str, Any]:
    """
    Resuelve referencias temporales relativas en el análisis.
    """
    decade = analysis.get("decade", "")
    year_range = analysis.get("year_range", "")
    intent = analysis.get("intent", "").lower()
    
    # Mapeo de referencias relativas a décadas concretas
    temporal_mappings = {
        "anterior década": f"{previous_decade}s",
        "última década": f"{previous_decade}s", 
        "pasada década": f"{previous_decade}s",
        "década pasada": f"{previous_decade}s",
        "década anterior": f"{previous_decade}s",
        "década actual": f"{current_decade}s",
        "esta década": f"{current_decade}s",
        "hace 10 años": f"{(current_year - 10) // 10 * 10}s",
        "últimos 10 años": f"{(current_year - 10) // 10 * 10}s-{current_decade}s",
    }
    
    # Verificar en el intent/decade si hay referencias relativas
    for ref, resolved in temporal_mappings.items():
        if ref in intent.lower() or ref in str(decade).lower():
            analysis["decade"] = resolved
            analysis["resolved_temporal_reference"] = f"{ref} → {resolved}"
            logger.debug(f"🕰️ Resuelta referencia temporal: {ref} → {resolved}")
            break
    
    # Manejar casos específicos de "lo mejor de la anterior década"
    if "anterior década" in intent.lower() and not decade:
        analysis["decade"] = f"{previous_decade}s"
        analysis["resolved_temporal_reference"] = f"anterior década → {previous_decade}s"
    
    return analysis


def get_improved_fallback_analysis(query_text: str) -> Dict[str, Any]:
    """
    Análisis de fallback mejorado con detección robusta de países, años específicos y décadas.
    Usado cuando el análisis principal con Ollama falla.
    """
    text_lower = query_text.lower()
    default_limit = 30
    
    # ✅ DETECCIÓN DE PAÍSES en fallback
    country_analysis = detect_country_intent(query_text)
    
    # ✅ DETECCIÓN DE TIEMPO - Prioridad: año específico > rango > década
    time_analysis = detect_time_intent(query_text)
    
    # ✅ DETECCIÓN MUY CONSERVADORA DE LÍMITES
    conservative_limit = extract_conservative_limit(query_text)
    
    # ✅ DETECCIÓN DE TIPO DE SOLICITUD
    ptype = detect_query_type(text_lower, country_analysis, time_analysis)
    
    # ✅ DETECCIÓN DE GÉNERO (solo si es explícito)
    genre = detect_explicit_genre(query_text)
    
    # ✅ DETECCIÓN DE MOOD/EMOCIÓN
    mood = detect_mood_intent(query_text)
    
    # Construir intent descriptivo
    intent_parts = []
    if country_analysis["has_country_intent"]:
        intent_parts.append(f"país: {country_analysis['country']}({country_analysis['country_type']})")
    if time_analysis["has_time_intent"]:
        if time_analysis["year"]:
            intent_parts.append(f"año: {time_analysis['year']}")
        elif time_analysis["year_range"]:
            intent_parts.append(f"rango: {time_analysis['year_range']['from']}-{time_analysis['year_range']['to']}")
        elif time_analysis["decade"]:
            intent_parts.append(f"década: {time_analysis['decade']}")
    if genre:
        intent_parts.append(f"género: {genre}")
    if mood:
        intent_parts.append(f"mood: {mood}")
    if conservative_limit:
        intent_parts.append(f"límite: {conservative_limit}")
    
    intent_description = f"Fallback: {query_text}"
    if intent_parts:
        intent_description += f" [{' | '.join(intent_parts)}]"

    return {
        "type": ptype, 
        "artist": "", 
        "track": "", 
        "album": "", 
        "genre": genre,
        "mood": mood,
        "decade": time_analysis["decade"],
        "year": time_analysis["year"],
        "year_range": time_analysis["year_range"],
        "country": country_analysis["country"],
        "country_type": country_analysis["country_type"],
        "limit": conservative_limit,
        "detected_limit": conservative_limit or default_limit,
        "intent": intent_description
    }

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
# 🎧 Ejemplo de integración dentro de handle_standard_request
# ============================================================
def handle_standard_request(query_text, llm_analysis, excluded_titles=None, excluded_paths=None):
    """
    Maneja consultas genéricas (género, mood, época, etc.) con soporte de regeneración.
    """
    excluded_titles = excluded_titles or set()
    excluded_paths = excluded_paths or set()
    query_clean = normalize_text(query_text)

    logger.debug("🎼 Ejecutando flujo estándar (género/estado de ánimo).")

    llm_raw = hybrid_playlist_cycle_enhanced(query_clean) or {}
    filters_raw = llm_raw.get("filters", {})
    limit = int(llm_raw.get("limit", 50) or 50)

    filters_enriched = enrich_filters_with_acoustics(query_clean, filters_raw)
    filters_safe = sanitize_filters(filters_enriched)
    mongo_filters = dict(filters_safe)

    results = list(tracks_col.find(mongo_filters))
    results = exclude_previous_tracks(results, excluded_titles, excluded_paths)

    global_max = get_global_max_values()
    for t in results:
        t["PopularityScore"] = compute_popularity(t, global_max)

    results = deduplicate_tracks_by_title_keep_best(results)
    compute_relative_popularity_by_genre(results)
    results.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
    final_results = results[:limit]

    simplified = [{
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
        "RelativePopularityScore": t.get("RelativePopularityScore"),
        "PopularityDisplay": popularity_display(t.get("RelativePopularityScore")),
    } for t in final_results]

    m3u_path, playlist_uuid = save_m3u(simplified, re.sub(r"[^\w\s-]", "", query_clean)[:60])
    playlist_doc = {
        "query_original": query_text,
        "filters": mongo_filters,
        "limit": limit,
        "created_at": datetime.now(),
        "m3u_path": m3u_path,
        "playlist_uuid": playlist_uuid,
        "items": simplified,
        "stats": {"total": len(simplified), "regenerated": bool(excluded_titles)},
        "feedback_pending": True,
        "user_email": user_email,
    }

    res = playlists_col.insert_one(playlist_doc)
    playlist_id = str(res.inserted_id)

    return {
        "query_original": query_text,
        "filtros": mongo_filters,
        "criterio_orden": "RelativePopularityScore",
        "total": len(simplified),
        "playlist": simplified,
        "archivo_m3u": m3u_path,
        "playlist_id": playlist_id,
        "playlist_uuid": playlist_uuid,
        "debug_summary": {
            "standard_mode": True,
            "llm_analysis": llm_analysis,
            "excluded_count": len(excluded_titles),
        },
    }

# =========================================================
# 🔢 Función para detectar límites numéricos en prompts
# =========================================================
def extract_limit_from_prompt(prompt_text: str, default_limit: int = 30) -> int:
    """
    Función mantenida para compatibilidad.
    Ahora delega la detección a analyze_query_intent para mayor precisión.
    """
    try:
        analysis = analyze_query_intent(prompt_text)
        return analysis.get("detected_limit", default_limit)
    except Exception as e:
        logger.warning(f"⚠️ Error en extract_limit_from_prompt: {e}")
        return default_limit

def _looks_like_year(number: int, text: str) -> bool:
    """
    Mantenida para compatibilidad, ahora usa la función mejorada.
    """
    return is_likely_year_in_context(number, text)


# -----------------------
# get user's playlists
# -----------------------
@app.get("/user/playlists")
def get_user_playlists(request: Request):
    """Obtiene todas las playlists generadas por el usuario actual"""
    try:
        # Obtener el token de autorización
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(status_code=401, detail="Authorization header missing")
        
        token = auth_header.replace("Bearer ", "")
        
        # Buscar el usuario por token
        user = db_auth.users.find_one({"session_token": token})
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user_email = user.get("email")
        
        # Buscar playlists del usuario (asumiendo que guardamos el email del usuario)
        playlists = list(playlists_col.find({"user_email": user_email}).sort("created_at", -1))
        
        # Convertir ObjectId a string
        for playlist in playlists:
            playlist["_id"] = str(playlist["_id"])
        
        return {
            "user": user_email,
            "playlists": playlists,
            "total": len(playlists)
        }
        
    except Exception as e:
        logger.error(f"Error getting user playlists: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# -----------------------
# update playlist name
# -----------------------
@app.put("/playlist/{pid}/name")
def update_playlist_name(pid: str, request: Request):
    """Actualiza el nombre de una playlist"""
    try:
        # Verificar autenticación
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(status_code=401, detail="Authorization header missing")
        
        token = auth_header.replace("Bearer ", "")
        user = db_auth.users.find_one({"session_token": token})
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Obtener datos del body
        body = request.json()
        new_name = body.get("name")
        
        if not new_name:
            raise HTTPException(status_code=400, detail="Name is required")
        
        # Actualizar playlist
        result = playlists_col.update_one(
            {"_id": ObjectId(pid), "user_email": user.get("email")},
            {"$set": {"name": new_name}}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Playlist not found or access denied")
        
        return {"message": "Playlist name updated successfully"}
        
    except Exception as e:
        logger.error(f"Error updating playlist name: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")    
        
        
# -----------------------
# delete playlist
# -----------------------
@app.delete("/playlist/{pid}")
def delete_playlist(pid: str, request: Request):
    """Elimina una playlist del usuario"""
    try:
        # Verificar autenticación
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(status_code=401, detail="Authorization header missing")
        
        token = auth_header.replace("Bearer ", "")
        user = db_auth.users.find_one({"session_token": token})
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Eliminar playlist
        result = playlists_col.delete_one({
            "_id": ObjectId(pid), 
            "user_email": user.get("email")
        })
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Playlist not found or access denied")
        
        return {"message": "Playlist deleted successfully"}
        
    except Exception as e:
        logger.error(f"Error deleting playlist: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")        
        
# -----------------------
# get user's specific playlist (with security)
# -----------------------
@app.get("/user/playlist/{pid}")
def get_user_playlist(pid: str, request: Request):
    """Obtiene una playlist específica del usuario actual"""
    try:
        # Verificar autenticación
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            raise HTTPException(status_code=401, detail="Authorization header missing")
        
        token = auth_header.replace("Bearer ", "")
        user = db_auth.users.find_one({"session_token": token})
        if not user:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user_email = user.get("email")
        
        # Buscar playlist específica del usuario
        try:
            oid = ObjectId(pid)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid playlist ID")
        
        playlist = playlists_col.find_one({
            "_id": oid, 
            "user_email": user_email  # ✅ Solo playlists del usuario
        })
        
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist not found or access denied")
        
        # Convertir ObjectId a string y limpiar respuesta
        playlist["id"] = str(playlist["_id"])
        playlist.pop("_id", None)
        
        # ✅ Asegurar que las URLs de streaming estén presentes
        if "items" in playlist and isinstance(playlist["items"], list):
            for item in playlist["items"]:
                if item.get("Ruta"):
                    item["StreamURL"] = convert_path_to_url(item["Ruta"])
                if item.get("CoverCarpeta"):
                    item["CoverURL"] = convert_path_to_url(item.get("CoverCarpeta"))
        
        return {
            "playlist": playlist,
            "user": user_email
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user playlist: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# -----------------------
# Helper function para convertir rutas a URLs (si no existe)
# -----------------------
def convert_path_to_url(local_path: str) -> str:
    """Convierte ruta local a URL HTTP accesible."""
    if not local_path:
        return ""
    path_fixed = local_path.replace("\\", "/")
    if path_fixed.lower().startswith("f:/musica/"):
        rel_path = path_fixed[9:]  # quitar "F:/Musica/"
        rel_path = urllib.parse.quote(rel_path)
        return f"http://192.168.100.169:8000/media/{rel_path}"
    return local_path        
    
def validate_and_normalize_limit(limit_candidate: Any, original_query: str) -> int:
    """
    Valida y normaliza el límite detectado, con verificaciones contextuales.
    """
    default_limit = 30
    
    # Caso 1: Límite es None o vacío
    if limit_candidate is None:
        return default_limit
    
    # Caso 2: Límite es string - extraer números
    if isinstance(limit_candidate, str):
        numbers = re.findall(r'\d+', limit_candidate)
        if not numbers:
            return default_limit
        try:
            limit_candidate = int(numbers[0])
        except (ValueError, TypeError):
            return default_limit
    
    # Caso 3: Límite es número - validar
    if isinstance(limit_candidate, (int, float)):
        limit_value = int(limit_candidate)
        
        # ✅ Verificar rangos razonables
        if not (1 <= limit_value <= 100):
            logger.debug(f"🔢 Límite {limit_value} fuera de rango, usando default")
            return default_limit
        
        # ✅ VERIFICACIÓN CONTEXTUAL CRÍTICA: ¿Es realmente un límite o un año?
        if is_likely_year_in_context(limit_value, original_query):
            logger.debug(f"🔢 Número {limit_value} parece ser año/década, ignorando como límite")
            return default_limit
        
        # ✅ Verificar contexto de palabras clave alrededor del número
        if not has_limit_context(limit_value, original_query):
            logger.debug(f"🔢 Número {limit_value} sin contexto de límite, ignorando")
            return default_limit
        
        logger.debug(f"🔢 Límite validado: {limit_value}")
        return limit_value
    
    return default_limit
    
def is_likely_year_in_context(number: int, query: str) -> bool:
    """
    Determina si un número probablemente se refiere a un año/década.
    Versión mejorada para fallback.
    """
    query_lower = query.lower()
    
    # Si el número está en rango de años
    if 1950 <= number <= 2030:
        # Contextos que indican año/década
        year_indicators = [
            'año', 'años', 'decada', 'década', 'year', 'years', 'decade',
            'del', 'de los', 'de las', 'en', 'del año', 'los', 'las'
        ]
        
        # Patrones específicos de década/año
        decade_patterns = [
            f"{number}s",
            f"{number}'s", 
            f"año {number}",
            f"años {number}",
            f"decada {number}",
            f"década {number}",
            f"del {number}",
            f"los {number}",
            f"las {number}"
        ]
        
        # Verificar si hay indicadores temporales cerca del número
        has_temporal_context = any(indicator in query_lower for indicator in year_indicators)
        has_decade_pattern = any(pattern in query_lower for pattern in decade_patterns)
        
        # Si el número está junto a palabras temporales, es probablemente un año
        words = query_lower.split()
        try:
            number_index = words.index(str(number))
            # Verificar palabras cercanas
            start = max(0, number_index - 2)
            end = min(len(words), number_index + 3)
            context_words = words[start:end]
            
            has_nearby_temporal = any(word in year_indicators for word in context_words)
            
            return has_temporal_context or has_decade_pattern or has_nearby_temporal
            
        except ValueError:
            return has_temporal_context or has_decade_pattern
    
    return False

def has_limit_context(number: int, query: str) -> bool:
    """
    Verifica si el número aparece en un contexto que sugiere límite de cantidad.
    """
    query_lower = query.lower()
    
    # Palabras clave que indican contexto de límite/cantidad
    limit_indicators = [
        'top', 'primeros', 'primeras', 'mejores', 'mejor', 
        'canciones', 'temas', 'pistas', 'tracks', 'songs',
        'lista', 'list', 'solo', 'solamente', 'únicamente',
        'las', 'los', 'primer', 'primera'
    ]
    
    # Buscar el número en el texto y verificar palabras cercanas
    number_pattern = fr'\b{number}\b'
    match = re.search(number_pattern, query_lower)
    
    if not match:
        return False
    
    number_pos = match.start()
    
    # Extraer contexto alrededor del número (10 palabras antes/después)
    words = query_lower.split()
    try:
        number_index = words.index(str(number))
        start = max(0, number_index - 5)
        end = min(len(words), number_index + 6)
        context_words = words[start:end]
        
        # Verificar si hay indicadores de límite en el contexto
        context_has_indicators = any(indicator in ' '.join(context_words) for indicator in limit_indicators)
        return context_has_indicators
        
    except ValueError:
        return False

def get_conservative_fallback_analysis(query_text: str) -> Dict[str, Any]:
    """
    Análisis de fallback ultra-conservador para cuando Ollama falla.
    Solo detecta límites en casos muy explícitos y evita años/décadas.
    """
    text_lower = query_text.lower()
    default_limit = 30
    
    # ✅ DETECCIÓN MUY CONSERVADORA DE LÍMITES
    conservative_limit = None
    
    # Solo patrones muy explícitos de límites
    explicit_limit_patterns = [
        r'\b(?:top|primer[oa]s?)\s+(\d+)\s+(?:canciones|temas|pistas)\b',
        r'\b(\d+)\s+(?:canciones|temas|pistas)\s+(?:de|para)\b',
        r'\b(?:las|los)\s+(\d+)\s+mejores\s+(?:canciones|temas)\b',
    ]
    
    for pattern in explicit_limit_patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                candidate = int(match.group(1))
                # Verificación extra conservadora
                if (1 <= candidate <= 50) and not is_likely_year_in_context(candidate, query_text):
                    conservative_limit = candidate
                    break
            except (ValueError, IndexError):
                continue
    
    # Determinar tipo
    if re.search(r"(similares a|parecidas a|similar to)", text_lower, re.I):
        ptype = "similar_to_request"
    elif re.search(r"(mejor de|best of|grandes éxitos|top de)", text_lower, re.I):
        ptype = "artist_request"
    else:
        ptype = "genre_or_mood_request"
    
    return {
        "type": ptype, 
        "artist": "", "track": "", "album": "", 
        "genre": "", "mood": "", "decade": "",
        "limit": conservative_limit,
        "detected_limit": conservative_limit or default_limit,
        "intent": f"Fallback: {query_text}"
    }    
    
def collect_enriched_context(max_artists: int = 80, max_genres: int = 50, max_decades: int = 10) -> Dict[str, Any]:
    """
    Recolecta contexto enriquecido de la base de datos para el modelo.
    Incluye estadísticas, patrones y relaciones entre artistas/géneros/épocas.
    """
    try:
        # 📊 ARTISTAS MÁS POPULARES por género
        pipeline_artists = [
            {"$group": {
                "_id": "$Artista", 
                "count": {"$sum": 1},
                "avg_popularity": {"$avg": "$PopularityScore"},
                "genres": {"$addToSet": "$Genero"},
                "decades": {"$addToSet": "$Decada"}
            }},
            {"$sort": {"avg_popularity": -1, "count": -1}},
            {"$limit": max_artists}
        ]
        top_artists = list(tracks_col.aggregate(pipeline_artists))
        
        # 🎵 GÉNEROS MÁS COMUNES con ejemplos de artistas
        pipeline_genres = [
            {"$unwind": "$Genero"},
            {"$group": {
                "_id": "$Genero",
                "count": {"$sum": 1},
                "artist_sample": {"$addToSet": "$Artista"},
                "avg_tempo": {"$avg": "$TempoBPM"},
                "avg_energy": {"$avg": "$EnergyRMS"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": max_genres}
        ]
        top_genres = list(tracks_col.aggregate(pipeline_genres))
        
        # 🕰️ DÉCADAS DISPONIBLES con distribución
        pipeline_decades = [
            {"$group": {
                "_id": "$Decada",
                "count": {"$sum": 1},
                "top_genres": {"$push": "$Genero"}
            }},
            {"$sort": {"count": -1}},
            {"$limit": max_decades}
        ]
        decades_info = list(tracks_col.aggregate(pipeline_decades))
        
        # 🎭 PATRONES EMOCIONALES por género
        emotional_patterns = {}
        for genre_doc in top_genres[:15]:  # Solo los 15 géneros más comunes
            genre = genre_doc["_id"]
            emotion_stats = tracks_col.aggregate([
                {"$match": {"Genero": genre}},
                {"$group": {
                    "_id": "$EMO_Sound",
                    "count": {"$sum": 1},
                    "avg_tempo": {"$avg": "$TempoBPM"},
                    "avg_energy": {"$avg": "$EnergyRMS"}
                }},
                {"$sort": {"count": -1}},
                {"$limit": 3}
            ])
            emotional_patterns[genre] = list(emotion_stats)
        
        # 🏆 ARTISTAS POR DÉCADA (para contexto temporal)
        artists_by_decade = {}
        for decade_doc in decades_info:
            decade = decade_doc["_id"]
            decade_artists = tracks_col.distinct("Artista", {"Decada": decade})
            # Tomar los más populares de esa década
            artists_by_decade[decade] = decade_artists[:10]  # Top 10 por década
        
        context = {
            "artists": [artist["_id"] for artist in top_artists],
            "artists_detailed": top_artists[:20],  # Info detallada de top 20
            "genres": [genre["_id"] for genre in top_genres],
            "genres_detailed": top_genres[:15],    # Info detallada de top 15 géneros
            "decades": [decade["_id"] for decade in decades_info],
            "decades_detailed": decades_info,
            "emotional_patterns": emotional_patterns,
            "artists_by_decade": artists_by_decade,
            "stats": {
                "total_artists": len(top_artists),
                "total_genres": len(top_genres),
                "total_decades": len(decades_info)
            }
        }
        
        logger.debug(f"🎯 Contexto enriquecido: {len(context['artists'])} artistas, {len(context['genres'])} géneros, {len(context['decades'])} décadas")
        return context
        
    except Exception as e:
        logger.debug(f"Error obteniendo contexto enriquecido: {e}")
        return {"artists": [], "genres": [], "decades": []}    

def hybrid_playlist_cycle_enhanced(user_prompt: str, model="neoplaylist-agent", default_limit=30, llm_analysis=None):
    """
    Ciclo híbrido mejorado que prioriza filtros de país cuando están presentes.
    MEJORAS:
    - Manejo robusto de errores en cada fase
    - Cache de análisis semántico
    - Límites dinámicos basados en complejidad
    - Métricas de rendimiento
    - Fallbacks inteligentes
    """
    start_time = time.time()
    logger.debug(f"🧠 Nueva consulta híbrida MEJORADA: '{user_prompt}'")
    
    try:
        # 🎯 CONTEXTO ENRIQUECIDO desde el inicio
        enriched_context = collect_enriched_context()
        
        # ✅ ANÁLISIS SEMÁNTICO MEJORADO (con cache opcional)
        if llm_analysis is None:
            llm_analysis = analyze_query_intent(user_prompt)
        
        # 🔢 AJUSTAR LÍMITE BASADO EN COMPLEJIDAD
        adjusted_limit = adjust_limit_based_on_complexity(user_prompt, default_limit, llm_analysis)
        logger.debug(f"🎯 Límite ajustado: {default_limit} → {adjusted_limit}")
        
        # 📝 PROMPT MEJORADO para la FASE 1 - INCLUIR FILTROS DE PAÍS EXPLÍCITAMENTE
        phase1_prompt = build_enhanced_prompt_with_country(user_prompt, enriched_context, llm_analysis)
        
        logger.debug(f"📤 PROMPT FASE 1 ENVIADO A OLLAMA:")
        logger.debug(phase1_prompt[:500] + "..." if len(phase1_prompt) > 500 else phase1_prompt)
        
        # FASE 1: Recomendaciones iniciales con contexto completo
        result = call_ollama_safe(phase1_prompt, model) or {}
        logger.debug(f"📥 RESPUESTA OLLAMA FASE 1: {len(result.get('suggestions', []))} sugerencias")
        
        # ✅ COMBINAR FILTROS: País del análisis + respuesta de Ollama
        llm_filters = result.get("filters", {}) if isinstance(result, dict) else {}
        suggestions = result.get("suggestions", []) if isinstance(result, dict) else []
        
        # ✅ FORZAR FILTROS DE PAÍS SI ESTÁN EN EL ANÁLISIS
        if llm_analysis.get("country"):
            if not llm_filters:
                llm_filters = {}
            llm_filters["country"] = llm_analysis["country"]
            llm_filters["country_type"] = llm_analysis.get("country_type", "origin")
            logger.debug(f"🇨🇱 FORZANDO filtro de país: {llm_analysis['country']} ({llm_analysis['country_type']})")
        
        # Procesar filtros y búsqueda
        filters = parse_filters_from_llm(llm_filters)
        filters = enrich_filters_with_acoustics(user_prompt, filters)
        
        # ✅ DEBUG DETALLADO DE FILTROS
        logger.debug(f"🎯 FILTROS PARA BÚSQUEDA: {list(filters.keys())}")
        
        # 🎵 BÚSQUEDA CON MÉTRICAS
        search_start = time.time()
        local_tracks = search_tracks_in_mongo(suggestions, filters, adjusted_limit, tracks_col, user_prompt)
        search_time = time.time() - search_start
        
        logger.debug(f"🎯 Fase 1: {len(local_tracks)} pistas en {search_time:.2f}s / objetivo {adjusted_limit}")
        
        if len(local_tracks) >= adjusted_limit:
            return finalize_enhanced_response(user_prompt, filters, local_tracks, 1, adjusted_limit, start_time, llm_analysis)
        
        # 🔄 FASE 2: Completitud con contexto ESPECÍFICO del problema
        missing = adjusted_limit - len(local_tracks)
        logger.debug(f"🔄 Fase 2: Necesitamos {missing} pistas más")
        
        phase2_prompt = build_completion_prompt_with_country(user_prompt, filters, local_tracks, enriched_context, missing, llm_analysis)
        
        result2 = call_ollama_safe(phase2_prompt, model) or {}
        
        # ✅ MANTENER FILTROS DE PAÍS EN FASE 2
        suggestions2 = []
        if isinstance(result2, dict):
            suggestions2 = result2.get("suggestions", [])
            new_filters = result2.get("filters")
            if new_filters and isinstance(new_filters, dict):
                filters = new_filters
        
        # ✅ ASEGURAR que los filtros de país se mantengan
        if llm_analysis.get("country") and not has_country_filters(filters):
            country_filters = parse_filters_from_llm({
                "country": llm_analysis["country"],
                "country_type": llm_analysis.get("country_type", "origin")
            })
            filters.update(country_filters)
            logger.debug(f"🇨🇱 REAPLICANDO filtros de país en Fase 2")
        
        local_tracks2 = search_tracks_in_mongo(suggestions2, filters, missing, tracks_col, user_prompt)
        local_tracks.extend(local_tracks2)
        logger.debug(f"🎯 Fase 2: +{len(local_tracks2)} nuevas pistas → total {len(local_tracks)}")
        
        if len(local_tracks) >= adjusted_limit:
            return finalize_enhanced_response(user_prompt, filters, local_tracks, 2, adjusted_limit, start_time, llm_analysis)
        
        # ✅ FASE 3: Validación manteniendo contexto de país
        phase3_prompt = build_validation_prompt_with_country(user_prompt, filters, local_tracks, enriched_context, llm_analysis)
        
        result3 = call_ollama_safe(phase3_prompt, model) or {}
        
        validated = extract_validated_tracks(result3, local_tracks, adjusted_limit)
        
        # 🎯 APLICAR POST-PROCESAMIENTO INTELIGENTE
        final_tracks = apply_intelligent_postprocessing(validated, user_prompt, llm_analysis, adjusted_limit)
        
        logger.debug(f"✅ Fase 3 finalizada — {len(final_tracks)} pistas validadas")
        
        return finalize_enhanced_response(user_prompt, filters, final_tracks, 3, adjusted_limit, start_time, llm_analysis)
        
    except Exception as e:
        logger.error(f"💥 ERROR en ciclo híbrido: {e}")
        # 🆘 FALLBACK DE EMERGENCIA
        return emergency_fallback(user_prompt, default_limit, start_time, str(e))

def build_validation_prompt_with_country(user_prompt: str, filters: dict, current_tracks: list, 
                                       context: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    """
    Construye prompt para Fase 3 (validación) manteniendo contexto.
    """
    country_info = ""
    if analysis.get("country"):
        country_info = f"CRITERIO PAÍS: {analysis['country']} ({analysis.get('country_type', 'origin')})"
    
    decade_info = ""
    if analysis.get("decade"):
        decade_info = f"CRITERIO DÉCADA: {analysis['decade']}"
    
    # Analizar distribución actual
    artists_count = {}
    for t in current_tracks:
        artist = t.get("Artista")
        if artist:
            artists_count[artist] = artists_count.get(artist, 0) + 1
    
    problem_artists = [artist for artist, count in artists_count.items() if count > 3]
    
    prompt = f"""
VALIDA y DEPURA esta playlist según la petición original.

Petición: "{user_prompt}"
{country_info}
{decade_info}

Lista actual ({len(current_tracks)} pistas):
{chr(10).join([f"- {t.get('Artista', '?')} - {t.get('Titulo', '?')} ({t.get('Genero', '?')}, {t.get('Año', '?')})" for t in current_tracks[:15]])}

PROBLEMAS DETECTADOS:
- Artistas con muchas canciones: {', '.join(problem_artists) if problem_artists else 'Ninguno'}

INSTRUCCIONES DE VALIDACIÓN:
1. ELIMINA canciones que NO coincidan con país/década/género solicitado
2. LIMITA a máximo 3 canciones por artista
3. MANTÉN la diversidad musical
4. CONSERVA las canciones más populares y representativas

Devuelve EXCLUSIVAMENTE JSON con las pistas validadas:
{{
  "suggestions": [
    {{"titulo": "...", "artista": "...", "album": "..."}}
  ]
}}
"""
    return prompt
    
def build_completion_prompt_with_country(user_prompt: str, filters: dict, current_tracks: list, 
                                       context: Dict[str, Any], missing: int, analysis: Dict[str, Any]) -> str:
    """
    Construye prompt para Fase 2 (completitud) manteniendo contexto de país/década.
    """
    country_info = ""
    if analysis.get("country"):
        country_info = f"País: {analysis['country']} ({analysis.get('country_type', 'origin')})"
    
    decade_info = ""
    if analysis.get("decade"):
        decade_info = f"Década: {analysis['decade']}"
    
    current_artists = list(set(t.get("Artista") for t in current_tracks if t.get("Artista")))
    
    prompt = f"""
FALTAN RESULTADOS para completar la playlist. Necesito {missing} pistas más.

Petición original: "{user_prompt}"
{country_info}
{decade_info}

Filtros aplicados: {json.dumps(filters, ensure_ascii=False, default=str)}

Pistas ya incluidas ({len(current_tracks)}):
{chr(10).join([f"- {t.get('Artista', '?')} - {t.get('Titulo', '?')}" for t in current_tracks[:10]])}

Artistas ya incluidos: {', '.join(current_artists[:15])}

CONTEXTO LOCAL DISPONIBLE:
Artistas: {', '.join(context.get('artists', [])[:25])}

INSTRUCCIONES:
1. Sugiere NUEVOS artistas o canciones que NO estén en la lista anterior
2. MANTÉN los filtros de país/década/género
3. Prioriza diversidad de artistas
4. Sugiere hasta {min(missing * 2, 20)} opciones

Devuelve EXCLUSIVAMENTE JSON:
{{
  "suggestions": [
    {{"titulo": "...", "artista": "...", "album": "..."}}
  ]
}}
"""
    return prompt
    
def adjust_limit_based_on_complexity(user_prompt: str, base_limit: int, llm_analysis: dict) -> int:
    """
    Ajusta el límite basado en la complejidad de la consulta.
    """
    complexity_score = 0
    
    # Factores de complejidad
    if llm_analysis.get("country"):
        complexity_score += 1
    if llm_analysis.get("decade"):
        complexity_score += 1
    if llm_analysis.get("genre"):
        complexity_score += 1
    if llm_analysis.get("mood"):
        complexity_score += 1
    if llm_analysis.get("artist"):
        complexity_score += 2  # Las búsquedas de artista son más específicas
    
    # Ajustar límite: consultas más complejas → límites más pequeños
    if complexity_score >= 3:
        return min(base_limit, 20)
    elif complexity_score >= 2:
        return min(base_limit, 25)
    else:
        return base_limit    

def has_country_filters(filters: dict) -> bool:
    """
    Verifica si los filtros ya incluyen criterios de país.
    """
    country_indicators = ["ArtistArea", "TopCountry1", "TopCountry2", "TopCountry3", "country"]
    return any(indicator in filters for indicator in country_indicators)

def extract_validated_tracks(result3: any, local_tracks: list, limit: int) -> list:
    """
    Extrae y valida pistas de la respuesta de la Fase 3.
    """
    validated = []
    
    if isinstance(result3, dict):
        validated = result3.get("suggestions", []) or local_tracks
    elif isinstance(result3, list):
        validated = result3
    else:
        validated = local_tracks
    
    # Si eliminó demasiadas, rellenar con las previas coherentes
    if not validated or len(validated) < limit:
        validated = validated or local_tracks
        # Mantener el orden original tanto como sea posible
        additional_tracks = [t for t in local_tracks if t not in validated]
        validated.extend(additional_tracks[:limit - len(validated)])
    
    return validated[:limit]

def emergency_fallback(user_prompt: str, limit: int, start_time: float, error_msg: str):
    """
    Fallback de emergencia cuando falla el ciclo principal.
    """
    logger.warning(f"🆘 Activando fallback de emergencia: {error_msg}")
    
    try:
        # Búsqueda simple por palabras clave
        words = [w for w in re.split(r"\W+", user_prompt.lower()) if len(w) > 3]
        if words:
            regex_or = [
                {"Genero": {"$regex": w, "$options": "i"}} for w in words
            ] + [
                {"Titulo": {"$regex": w, "$options": "i"}} for w in words
            ] + [
                {"Artista": {"$regex": w, "$options": "i"}} for w in words
            ]
            
            fallback_q = {"$or": regex_or}
            fallback_tracks = list(tracks_col.find(fallback_q).limit(limit * 2))
            
            # Procesar resultados del fallback
            processed_tracks = apply_intelligent_postprocessing(fallback_tracks, user_prompt, {}, limit)
            
            return finalize_enhanced_response(
                user_prompt, 
                {"fallback": True, "error": error_msg},
                processed_tracks, 
                0,  # Iteración 0 indica fallback
                limit, 
                start_time, 
                None
            )
    except Exception as fallback_error:
        logger.error(f"💥 Fallback también falló: {fallback_error}")
    
    # Último recurso: pistas aleatorias populares
    random_tracks = list(tracks_col.find().sort("PopularityScore", -1).limit(limit))
    return finalize_enhanced_response(
        user_prompt,
        {"emergency_fallback": True},
        random_tracks,
        0,
        limit,
        start_time,
        None
    )
    
def finalize_enhanced_response(prompt: str, filters: dict, tracks: list, iterations: int, 
                             limit: int, start_time: float, llm_analysis: dict = None):
    """
    Versión mejorada de finalize_response con métricas.
    """
    total_time = time.time() - start_time
    
    # Enriquecer pistas con URLs
    for t in tracks:
        ruta = t.get("Ruta")
        cover = t.get("CoverCarpeta")
        
        if ruta:
            t["StreamURL"] = convert_path_to_url(ruta)
        if cover:
            t["CoverURL"] = convert_path_to_url(cover)
    
    response = {
        "prompt": prompt,
        "filters": filters,
        "limit": limit,
        "iterations": iterations,
        "total_found": len(tracks),
        "from_local": len(tracks),
        "playlist": tracks,
        "performance_metrics": {
            "total_time_seconds": round(total_time, 2),
            "tracks_per_second": round(len(tracks) / total_time, 2) if total_time > 0 else 0,
            "llm_analysis_used": llm_analysis is not None
        }
    }
    
    # Añadir análisis semántico si está disponible
    if llm_analysis:
        response["semantic_analysis"] = {
            "type": llm_analysis.get("type"),
            "genre": llm_analysis.get("genre"),
            "decade": llm_analysis.get("decade"),
            "country": llm_analysis.get("country"),
            "detected_limit": llm_analysis.get("detected_limit")
        }
    
    logger.debug(f"📊 Métricas finales: {response['performance_metrics']}")
    
    return response    
    
def apply_intelligent_postprocessing(tracks: list, user_prompt: str, llm_analysis: dict, limit: int) -> list:
    """
    Aplica post-procesamiento inteligente a las pistas.
    """
    if not tracks:
        return tracks
    
    # 1. Calcular métricas de popularidad
    global_max = get_global_max_values()
    for t in tracks:
        t["PopularityScore"] = compute_popularity(t, global_max)
    
    # 2. Deduplicar
    deduped = deduplicate_tracks_by_title_keep_best(tracks)
    
    # 3. Normalizar por género
    compute_relative_popularity_by_genre(deduped)
    
    # 4. Filtrar incongruencias
    filtered = filter_gross_incongruities(deduped, user_prompt)
    
    # 5. Aplicar límites por artista/álbum
    limited = limit_tracks_by_artist_album(filtered)
    
    # 6. Ordenar por popularidad relativa
    limited.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
    
    return limited[:limit]

    
def build_enhanced_prompt_with_country(user_prompt: str, context: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    """
    Construye prompt mejorado para Fase 1 con soporte de país y década.
    """
    # Construir sección de criterios específicos
    criteria_sections = []
    
    if analysis.get("country"):
        country = analysis["country"]
        country_type = analysis.get("country_type", "origin")
        criteria_sections.append(f"🎯 PAÍS: {country} ({'origen del artista' if country_type == 'origin' else 'popularidad en el país'})")
    
    if analysis.get("decade"):
        decade = analysis["decade"]
        criteria_sections.append(f"🎯 DÉCADA: {decade}")
    
    if analysis.get("genre"):
        genre = analysis["genre"]
        criteria_sections.append(f"🎯 GÉNERO: {genre}")
    
    criteria_text = "\n".join(criteria_sections) if criteria_sections else "🎯 CRITERIO GENERAL: Música popular y representativa"

    # Formatear contexto de artistas y géneros
    artists_sample = ", ".join(context.get('artists', [])[:25]) if context.get('artists') else "No disponible"
    genres_sample = ", ".join(context.get('genres', [])[:20]) if context.get('genres') else "No disponible"
    
    prompt = f"""
ANALIZA esta solicitud musical y genera recomendaciones ESPECÍFICAS:

SOLICITUD DEL USUARIO: "{user_prompt}"

{criteria_text}

BASE DE DATOS DISPONIBLE:
- Artistas: {artists_sample}
- Géneros: {genres_sample}

INSTRUCCIONES CRÍTICAS:
1. Sugiere canciones REALES que existan en la base de datos
2. Respeta ESTRICTAMENTE los criterios de país/década/género
3. Prioriza canciones POPULARES y REPRESENTATIVAS
4. Incluye entre 5-15 sugerencias específicas
5. Usa EXCLUSIVAMENTE artistas del contexto proporcionado

EJEMPLOS DE SUGERENCIAS VÁLIDAS:
- Para "rock de los 90s": "Smells Like Teen Spirit", "Wonderwall", "Creep"
- Para "pop chileno": "La Ley", "Los Prisioneros", "Los Tres"

DEVUELVE EXCLUSIVAMENTE JSON (sin texto adicional):
{{
  "filters": {{
    "Genero": "rock",
    "Decada": "1990s"
  }},
  "suggestions": [
    {{"titulo": "Smells Like Teen Spirit", "artista": "Nirvana", "album": "Nevermind"}},
    {{"titulo": "Wonderwall", "artista": "Oasis", "album": "(What's the Story) Morning Glory?"}},
    {{"titulo": "Creep", "artista": "Radiohead", "album": "Pablo Honey"}}
  ]
}}
"""

    return prompt

def emergency_country_search(country: str, country_type: str, limit: int = 30) -> List[Dict[str, Any]]:
    """
    Búsqueda directa de emergencia por país con prioridad jerárquica en TopCountry.
    """
    logger.debug(f"🚨 BÚSQUEDA DE EMERGENCIA para país: {country} ({country_type})")
    
    all_results = []
    
    if country_type == "origin":
        # Búsqueda por origen del artista
        query = {"ArtistArea": {"$regex": f"^{re.escape(country)}$", "$options": "i"}}
        try:
            results = list(tracks_col.find(query).sort("PopularityScore", -1).limit(limit * 3))
            all_results.extend(results)
            logger.debug(f"🚨 Resultados por ORIGEN: {len(results)} tracks")
        except Exception as e:
            logger.error(f"🚨 Error en búsqueda por origen: {e}")
    
    else:
        # ✅ BÚSQUEDA JERÁRQUICA POR TOPCOUNTRY
        # 1. Primero TopCountry1 (más relevante)
        try:
            query_tc1 = {"TopCountry1": {"$regex": f"^{re.escape(country)}$", "$options": "i"}}
            results_tc1 = list(tracks_col.find(query_tc1).sort("PopularityScore", -1).limit(limit))
            all_results.extend(results_tc1)
            logger.debug(f"🚨 Resultados TopCountry1: {len(results_tc1)} tracks")
            
            # 2. Si no alcanzamos el límite, buscar en TopCountry2
            if len(all_results) < limit:
                remaining = limit - len(all_results)
                query_tc2 = {
                    "TopCountry2": {"$regex": f"^{re.escape(country)}$", "$options": "i"},
                    "_id": {"$nin": [r["_id"] for r in all_results]}  # Evitar duplicados
                }
                results_tc2 = list(tracks_col.find(query_tc2).sort("PopularityScore", -1).limit(remaining))
                all_results.extend(results_tc2)
                logger.debug(f"🚨 + Resultados TopCountry2: {len(results_tc2)} tracks")
            
            # 3. Si aún no alcanzamos el límite, buscar en TopCountry3
            if len(all_results) < limit:
                remaining = limit - len(all_results)
                query_tc3 = {
                    "TopCountry3": {"$regex": f"^{re.escape(country)}$", "$options": "i"},
                    "_id": {"$nin": [r["_id"] for r in all_results]}  # Evitar duplicados
                }
                results_tc3 = list(tracks_col.find(query_tc3).sort("PopularityScore", -1).limit(remaining))
                all_results.extend(results_tc3)
                logger.debug(f"🚨 + Resultados TopCountry3: {len(results_tc3)} tracks")
                
        except Exception as e:
            logger.error(f"🚨 Error en búsqueda jerárquica: {e}")
    
    # Ordenar todos los resultados por popularidad
    all_results.sort(key=lambda x: x.get("PopularityScore", 0), reverse=True)
    
    # Aplicar límite final
    final_results = all_results[:limit]
    
    # ✅ DEBUG: Mostrar distribución por TopCountry
    if country_type != "origin":
        tc1_count = len([r for r in final_results if r.get("TopCountry1") and country.lower() in r.get("TopCountry1", "").lower()])
        tc2_count = len([r for r in final_results if r.get("TopCountry2") and country.lower() in r.get("TopCountry2", "").lower()])
        tc3_count = len([r for r in final_results if r.get("TopCountry3") and country.lower() in r.get("TopCountry3", "").lower()])
        logger.debug(f"📊 Distribución TopCountry: TC1={tc1_count}, TC2={tc2_count}, TC3={tc3_count}")
    
    logger.debug(f"🚨 Resultados finales de emergencia: {len(final_results)} tracks")
    return final_results
        
def build_enhanced_prompt(user_prompt: str, context: Dict[str, Any], mode: str) -> str:
    logger.debug(f"🔧 Construyendo prompt para modo: {mode}")
    logger.debug(f"🔧 Contexto recibido: {len(context.get('artists', []))} artistas, {len(context.get('genres', []))} géneros")
    """
    Construye prompts enriquecidos según el modo de operación.
    """
    base_prompt = f"""
Usuario solicita: "{user_prompt}"

🎯 CONTEXTO ENRIQUECIDO DE LA BASE DE DATOS:

ARTISTAS DISPONIBLES ({len(context.get('artists', []))}):
{format_artists_context(context)}

GÉNEROS DISPONIBLES ({len(context.get('genres', []))}):
{format_genres_context(context)}

DÉCADAS DISPONIBLES:
{format_decades_context(context)}

PATRONES EMOCIONALES POR GÉNERO:
{format_emotional_patterns(context)}

"""
    
    if mode == "initial_recommendation":
        base_prompt += """
INSTRUCCIONES PARA RECOMENDACIÓN INICIAL:
1. Usa SOLO los géneros y artistas listados arriba
2. Para "música alegre": prioriza pop, disco, dance, synthpop, funk
3. Para "rock energético": prioriza classic rock, hard rock, alternative
4. Considera los patrones emocionales típicos de cada género
5. Sugiere artistas que existan en la lista disponible

Devuelve JSON con formato estándar.
"""
    
    elif mode == "completion":
        base_prompt += """
INSTRUCCIONES PARA COMPLETITUD:
1. Faltan resultados - necesitas completar la playlist
2. PRIORIZA artistas de la lista disponible
3. Usa los patrones emocionales como guía
4. Si el género solicitado es escaso, sugiere géneros relacionados
5. Incluye una "razon" para cada sugerencia

Devuelve JSON con formato de completitud.
"""
    
    return base_prompt

def format_artists_context(context: Dict[str, Any]) -> str:
    """Formatea la información de artistas para el prompt"""
    artists = context.get("artists", [])
    detailed = context.get("artists_detailed", [])
    
    if not artists:
        return "No hay datos de artistas disponibles"
    
    # Agrupar artistas por década de mayor actividad
    artists_by_decade = {}
    for artist in detailed[:25]:  # Top 25 artistas
        decades = artist.get("decades", [])
        primary_decade = decades[0] if decades else "Desconocida"
        artists_by_decade.setdefault(primary_decade, []).append(artist["_id"])
    
    formatted = []
    for decade, artists_list in list(artists_by_decade.items())[:5]:  # Top 5 décadas
        formatted.append(f"  {decade}: {', '.join(artists_list[:8])}")
    
    return "\n".join(formatted) + f"\n  ... y {len(artists) - 25} artistas más"

def format_genres_context(context: Dict[str, Any]) -> str:
    """Formatea la información de géneros para el prompt"""
    genres_detailed = context.get("genres_detailed", [])
    
    if not genres_detailed:
        return "No hay datos de géneros disponibles"
    
    formatted = []
    for genre in genres_detailed[:15]:  # Top 15 géneros
        artists_sample = genre.get("artist_sample", [])[:5]
        formatted.append(
            f"  {genre['_id']}: {genre['count']} pistas, "
            f"tempo {genre.get('avg_tempo', 0):.0f} BPM, "
            f"energía {genre.get('avg_energy', 0):.2f}"
        )
    
    return "\n".join(formatted)

def format_emotional_patterns(context: Dict[str, Any]) -> str:
    """Formatea los patrones emocionales por género"""
    patterns = context.get("emotional_patterns", {})
    
    if not patterns:
        return "No hay datos de patrones emocionales"
    
    formatted = []
    for genre, emotions in list(patterns.items())[:10]:  # Top 10 géneros
        if emotions:
            primary_emotion = emotions[0]
            formatted.append(
                f"  {genre}: {primary_emotion['_id']} "
                f"({primary_emotion['count']} pistas)"
            )
    
    return "\n".join(formatted)

def format_decades_context(context: Dict[str, Any]) -> str:
    """Formatea la información de décadas"""
    decades_detailed = context.get("decades_detailed", [])
    
    if not decades_detailed:
        return "No hay datos de décadas disponibles"
    
    formatted = []
    for decade in decades_detailed:
        formatted.append(f"  {decade['_id']}: {decade['count']} pistas")
    
    return "\n".join(formatted)    
    
def build_completion_prompt(user_prompt: str, current_filters: Dict, current_tracks: List, 
                          context: Dict[str, Any], missing_count: int) -> str:
    """
    Prompt mejorado para completitud con análisis del problema actual.
    """
    current_artists = list(set(track.get("Artista") for track in current_tracks))
    current_genres = list(set(
        genre for track in current_tracks 
        for genre in (track.get("Genero") or [])
        if genre
    ))
    
    return f"""
PROBLEMA DE COMPLETITUD:
- Solicitud original: "{user_prompt}"
- Faltan {missing_count} pistas para completar la playlist
- Filtros actuales: {json.dumps(current_filters, ensure_ascii=False)}

CONTEXTO ACTUAL:
- Artistas ya incluidos: {', '.join(current_artists[:8]) or 'Ninguno'}
- Géneros ya incluidos: {', '.join(current_genres[:8]) or 'Ninguno'}

BASE DE DATOS DISPONIBLE:
{format_artists_context(context)}
{format_genres_context(context)}

INSTRUCCIONES CRÍTICAS:
1. EVITA repetir artistas ya incluidos
2. PRIORIZA géneros coherentes pero diferentes a los ya usados
3. Usa artistas de la lista disponible que encajen con el prompt
4. Para "música alegre": si hay mucho metal, sugiere más pop/dance
5. Incluye "razon" explicando por qué cada sugerencia encaja

Devuelve JSON de completitud.
"""

def build_validation_prompt(user_prompt: str, current_filters: Dict, 
                          current_tracks: List, context: Dict[str, Any]) -> str:
    """
    Prompt mejorado para validación final con análisis de coherencia.
    """
    # Analizar distribución actual
    artist_counts = {}
    genre_counts = {}
    
    for track in current_tracks:
        artist = track.get("Artista")
        genres = track.get("Genero") or []
        
        artist_counts[artist] = artist_counts.get(artist, 0) + 1
        for genre in genres:
            genre_counts[genre] = genre_counts.get(genre, 0) + 1
    
    # Identificar posibles problemas
    overrepresented_artists = [a for a, c in artist_counts.items() if c > len(current_tracks) * 0.2]
    overrepresented_genres = [g for g, c in genre_counts.items() if c > len(current_tracks) * 0.3]
    
    return f"""
VALIDACIÓN FINAL SOLICITADA:
- Prompt original: "{user_prompt}"
- {len(current_tracks)} pistas para validar

ANÁLISIS ACTUAL:
- Distribución por artista: {dict(sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5])}
- Distribución por género: {dict(sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:5])}
- Posibles problemas: {{
    "artistas_sobrerrepresentados": {overrepresented_artists},
    "géneros_sobrerrepresentados": {overrepresented_genres}
}}

CONTEXTO DE GÉNEROS PARA COHERENCIA:
{format_genres_context(context)}

INSTRUCCIONES DE VALIDACIÓN:
1. Elimina pistas que NO encajen con "{user_prompt}"
2. Limita a máximo 20% por artista ({max(2, len(current_tracks) // 5)} pistas/artista)
3. Limita a máximo 2 pistas por álbum
4. Prioriza coherencia emocional y de género
5. Mantén diversidad artística

Devuelve JSON de validación con lista filtrada.
"""    

def detect_country_intent(query_text: str) -> Dict[str, Any]:
    """
    Detecta intención de filtrado por país en el prompt.
    Versión mejorada para fallback.
    """
    text_lower = query_text.lower()
    
    # Mapeo de países comunes
    country_mappings = {
        "chile": "Chile", "chilena": "Chile", "chileno": "Chile",
        "argentina": "Argentina", "argentino": "Argentina", "argentina": "Argentina",
        "méxico": "Mexico", "mexico": "Mexico", "mexicana": "Mexico", "mexicano": "Mexico",
        "españa": "Spain", "española": "Spain", "español": "Spain", "spain": "Spain",
        "colombia": "Colombia", "colombiano": "Colombia",
        "brasil": "Brazil", "brasileña": "Brazil", "brasileño": "Brazil",
        "perú": "Peru", "peruana": "Peru", "peruano": "Peru",
        "eeuu": "United States", "estados unidos": "United States", "usa": "United States",
        "reino unido": "United Kingdom", "uk": "United Kingdom", "british": "United Kingdom",
        "francia": "France", "francesa": "France", "francés": "France",
        "alemania": "Germany", "alemana": "Germany", "alemán": "Germany",
        "italia": "Italy", "italiana": "Italy", "italiano": "Italy",
        "japón": "Japan", "japonesa": "Japan", "japonés": "Japan"
    }
    
    detected_country = None
    for term, country in country_mappings.items():
        if term in text_lower:
            detected_country = country
            break
    
    # Detectar tipo de filtro por país
    country_type = "origin"  # Por defecto, origen del artista
    
    # Patrones para "popular en [país]"
    popular_in_patterns = [
        r"popular en (\w+)",
        r"escuchado en (\w+)", 
        r"más sonado en (\w+)",
        r"éxitos en (\w+)",
        r"lo más escuchado en (\w+)"
    ]
    
    for pattern in popular_in_patterns:
        match = re.search(pattern, text_lower)
        if match:
            country_type = "popular_in"
            break
    
    # Si no se detectó popular_in, buscar patrones de origen
    if country_type == "origin":
        origin_patterns = [
            r"música (\w+)",  # "música chilena"
            r"artistas (\w+)",  # "artistas chilenos"
            r"bandas (\w+)",  # "bandas argentinas"
            r"cantantes (\w+)",  # "cantantes mexicanos"
            r"del (\w+)",  # "musica del perú"
            r"de (\w+)$"  # "musica de chile"
        ]
        
        for pattern in origin_patterns:
            match = re.search(pattern, text_lower)
            if match:
                country_type = "origin"
                break

    return {
        "country": detected_country,
        "country_type": country_type,
        "has_country_intent": detected_country is not None
    }
    
def detect_time_intent(query_text: str) -> Dict[str, Any]:
    """
    Detecta intención temporal: año específico, rango de años o década.
    """
    text_lower = query_text.lower()
    
    # 1️⃣ BUSCAR AÑO ESPECÍFICO primero (prioridad máxima)
    year_match = re.search(r'\b(19|20)\d{2}\b', query_text)
    year_specific = None
    if year_match:
        year_candidate = int(year_match.group())
        # Validar que sea un año razonable y no parte de otra cosa
        if 1950 <= year_candidate <= 2030:
            # Verificar contexto - no debe ser parte de una década
            context = query_text.lower()
            if not any(decade_term in context for decade_term in ["década", "decada", "años", "los"]):
                year_specific = year_candidate
                logger.debug(f"📅 Año específico detectado en fallback: {year_specific}")
    
    # 2️⃣ BUSCAR RANGO DE AÑOS
    year_range = None
    range_match = re.search(r'(\d{4})\s*(?:a|al|hasta|y|-)\s*(\d{4})', query_text)
    if range_match:
        start_year = int(range_match.group(1))
        end_year = int(range_match.group(2))
        if 1950 <= start_year <= end_year <= 2030:
            year_range = {"from": start_year, "to": end_year}
            logger.debug(f"📅 Rango de años detectado: {start_year}-{end_year}")
    
    # 3️⃣ BUSCAR DÉCADAS (solo si no hay año específico ni rango)
    decade = None
    if not year_specific and not year_range:
        decade_patterns = {
            "70": "1970s", "setenta": "1970s", "70s": "1970s",
            "80": "1980s", "ochenta": "1980s", "80s": "1980s", 
            "90": "1990s", "noventa": "1990s", "90s": "1990s",
            "2000": "2000s", "dos mil": "2000s", "2000s": "2000s",
            "2010": "2010s", "dos mil diez": "2010s", "2010s": "2010s",
            "2020": "2020s", "dos mil veinte": "2020s", "2020s": "2020s"
        }
        
        for term, decade_value in decade_patterns.items():
            if term in text_lower:
                # Verificar que sea contexto de década, no año suelto
                if f"años {term}" in text_lower or f"década {term}" in text_lower or f"los {term}" in text_lower:
                    decade = decade_value
                    logger.debug(f"🕰️ Década detectada en fallback: {decade}")
                    break
                elif term in ["70s", "80s", "90s", "2000s", "2010s", "2020s"]:
                    # Si termina en 's', es muy probable que sea década
                    decade = decade_value
                    logger.debug(f"🕰️ Década detectada por sufijo: {decade}")
                    break
    
    # 4️⃣ DETECTAR MÚLTIPLES DÉCADAS
    multiple_decades = []
    if not year_specific and not year_range and not decade:
        decade_terms = []
        for term in ["70", "80", "90", "2000", "2010", "2020"]:
            if term in text_lower:
                decade_terms.append(term)
        
        if len(decade_terms) >= 2:
            decade_map = {"70": "1970s", "80": "1980s", "90": "1990s", "2000": "2000s", "2010": "2010s", "2020": "2020s"}
            multiple_decades = [decade_map[term] for term in decade_terms if term in decade_map]
            if multiple_decades:
                decade = multiple_decades  # Lista de décadas
                logger.debug(f"🕰️ Múltiples décadas detectadas: {multiple_decades}")

    return {
        "year": year_specific,
        "year_range": year_range,
        "decade": decade,
        "has_time_intent": any([year_specific, year_range, decade])
    }
    
def extract_conservative_limit(query_text: str) -> Optional[int]:
    """
    Extrae límites de forma ultra-conservadora para evitar falsos positivos.
    """
    text_lower = query_text.lower()
    
    # Solo patrones muy explícitos de límites
    explicit_limit_patterns = [
        r'\b(?:top|primer[oa]s?)\s+(\d+)\s+(?:canciones|temas|pistas|temas)\b',
        r'\b(\d+)\s+(?:canciones|temas|pistas)\s+(?:de|para)\b',
        r'\b(?:las|los)\s+(\d+)\s+mejores\s+(?:canciones|temas)\b',
        r'\b(?:primer|primera)\s+(\d+)\s+(?:canciones|temas)\b',
        r'\bsolo\s+(\d+)\s+(?:canciones|temas)\b',
        r'\b(?:únicamente|solamente)\s+(\d+)\s+(?:canciones|temas)\b'
    ]
    
    for pattern in explicit_limit_patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                candidate = int(match.group(1))
                # Verificación extra conservadora
                if (1 <= candidate <= 50) and not is_likely_year_in_context(candidate, query_text):
                    logger.debug(f"🔢 Límite conservador detectado: {candidate}")
                    return candidate
            except (ValueError, IndexError):
                continue
    
    return None
    
def detect_query_type(text_lower: str, country_analysis: Dict, time_analysis: Dict) -> str:
    """
    Determina el tipo de consulta basado en patrones específicos.
    """
    # 1. Solicitudes de país tienen prioridad
    if country_analysis["has_country_intent"]:
        return "country_request"
    
    # 2. Solicitudes de similitud
    if re.search(r"(similares a|parecidas a|similar a|como|recomendaciones de)", text_lower, re.I):
        return "similar_to_request"
    
    # 3. Solicitudes de artista específico
    if re.search(r"(mejor de|best of|grandes éxitos|top de|discografía de|canciones de)\s+[^0-9]", text_lower, re.I):
        return "artist_request"
    
    # 4. Solicitudes de álbum
    if re.search(r"(álbum|album|disco)\s+.+\s+(de|del)", text_lower, re.I):
        return "album_request"
    
    # 5. Por defecto, género/mood/época
    return "genre_or_mood_request"    
    
def detect_explicit_genre(query_text: str) -> Optional[str]:
    """
    Detecta géneros musicales solo cuando son explícitamente mencionados.
    Evita añadir géneros por defecto como "pop".
    """
    text_lower = query_text.lower()
    
    # Géneros explícitos y sus patrones
    explicit_genres = {
        "rock": ["rock", "rock and roll", "rock & roll"],
        "pop": ["pop", "música pop"],
        "jazz": ["jazz"],
        "blues": ["blues"],
        "reggae": ["reggae"],
        "hip hop": ["hip hop", "hip-hop", "rap"],
        "electronic": ["electrónica", "electrónico", "electronic", "edm"],
        "classical": ["clásica", "clásico", "classical"],
        "folk": ["folk", "folclórica", "folclórico"],
        "metal": ["metal", "heavy metal"],
        "punk": ["punk"],
        "reggaeton": ["reggaeton", "reguetón"],
        "salsa": ["salsa"],
        "cumbia": ["cumbia"],
        "bachata": ["bachata"],
        "tango": ["tango"],
        "bolero": ["bolero"]
    }
    
    for genre, patterns in explicit_genres.items():
        for pattern in patterns:
            if pattern in text_lower:
                # Verificar que no sea parte de una palabra más larga
                if re.search(r'\b' + re.escape(pattern) + r'\b', text_lower):
                    logger.debug(f"🎵 Género explícito detectado: {genre}")
                    return genre
    
    return None

def detect_mood_intent(query_text: str) -> Optional[str]:
    """
    Detecta intenciones de mood/emoción en el prompt.
    """
    text_lower = query_text.lower()
    
    mood_mappings = {
        "alegre": ["alegre", "feliz", "contento", "alegría", "felicidad"],
        "triste": ["triste", "tristeza", "melancolía", "melancólico"],
        "energético": ["energético", "energética", "energía", "potente", "intenso"],
        "relajante": ["relajante", "relajado", "calma", "tranquilo", "suave"],
        "romántico": ["romántico", "romántica", "amor", "pasión", "corazón"],
        "nostálgico": ["nostálgico", "nostalgia", "recuerdos"],
        "bailable": ["bailable", "baile", "fiesta", "party", "dance"]
    }
    
    for mood, terms in mood_mappings.items():
        for term in terms:
            if term in text_lower:
                logger.debug(f"😊 Mood detectado: {mood}")
                return mood
    
    return None    
    
def get_topcountry_distribution(tracks: List[Dict[str, Any]], country: str) -> Dict[str, int]:
    """
    Calcula la distribución de canciones por TopCountry para un país específico.
    """
    distribution = {"TopCountry1": 0, "TopCountry2": 0, "TopCountry3": 0}
    country_lower = country.lower()
    
    for track in tracks:
        if track.get("TopCountry1") and country_lower in track.get("TopCountry1", "").lower():
            distribution["TopCountry1"] += 1
        elif track.get("TopCountry2") and country_lower in track.get("TopCountry2", "").lower():
            distribution["TopCountry2"] += 1
        elif track.get("TopCountry3") and country_lower in track.get("TopCountry3", "").lower():
            distribution["TopCountry3"] += 1
    
    return distribution    
    
    
REGION_DEFINITIONS = {
    "latin_america": {
        "name": "Latinoamérica",
        "countries": [
            "Mexico", "Argentina", "Chile", "Colombia", "Peru", "Brazil",
            "Cuba", "Puerto Rico", "Dominican Republic", "Venezuela",
            "Ecuador", "Uruguay", "Paraguay", "Bolivia", "Costa Rica",
            "Panama", "Guatemala", "Honduras", "El Salvador", "Nicaragua"
        ],
        "description": "Países de América Latina y el Caribe"
    },
    "europe": {
        "name": "Europa", 
        "countries": [
            "Spain", "France", "Italy", "Germany", "United Kingdom", "Portugal",
            "Netherlands", "Belgium", "Switzerland", "Sweden", "Norway", "Denmark",
            "Finland", "Ireland", "Austria", "Greece", "Poland", "Russia"
        ],
        "description": "Países europeos"
    },
    "asia": {
        "name": "Asia",
        "countries": [
            "Japan", "South Korea", "China", "India", "Thailand", "Philippines",
            "Vietnam", "Indonesia", "Malaysia", "Singapore", "Taiwan"
        ],
        "description": "Países asiáticos"
    },
    "north_america": {
        "name": "América del Norte", 
        "countries": ["United States", "Canada"],
        "description": "Estados Unidos y Canadá"
    },
    "africa": {
        "name": "África",
        "countries": [
            "Nigeria", "South Africa", "Egypt", "Kenya", "Ghana", "Morocco",
            "Ethiopia", "Tanzania", "Algeria", "Uganda"
        ],
        "description": "Países africanos"
    },
        "Oceania": {
        "name": "Oceania",
        "countries": [
            "australia", "Fiyi", "Kiribati", "Islas Marshall", "Micronesia",
            "Nauru", "Nueva Zelanda", "Palaos", "Papúa Nueva Guinea", "Samoa",
            "Islas Salomón", "Tonga", "Tuvalu", "Vanuatu"
        ],
        "description": "Países Oceania"
    }
}
def compute_region_relevance_score(track: Dict[str, Any], region_id: str, user_genre: str = None) -> float:
    """
    Calcula un score de relevancia para una región específica
    Considera popularidad + coherencia regional + género si está especificado
    """
    base_popularity = track.get("RelativePopularityScore", 0) or track.get("PopularityScore", 0)
    
    # Score base de popularidad (0-1)
    popularity_score = min(1.0, base_popularity * 1.5)  # Ajustar escala
    
    # Bonus por coherencia regional (artistas muy representativos de su región)
    regional_bonus = compute_regional_representativeness(track, region_id)
    
    # Bonus por matching de género si el usuario lo especificó
    genre_bonus = 0.0
    if user_genre:
        genre_bonus = compute_genre_match_bonus(track, user_genre, region_id)
    
    # Fórmula final
    final_score = (
        popularity_score * 0.7 +      # Popularidad es lo más importante
        regional_bonus * 0.2 +        # Representatividad regional
        genre_bonus * 0.1             # Género específico si se pidió
    )
    
    return round(final_score, 4)

def compute_regional_representativeness(track: Dict[str, Any], region_id: str) -> float:
    """
    Calcula qué tan representativo es un artista de su región
    Basado en popularidad regional y distintividad cultural
    """
    score = 0.0
    
    # Bonus por alta popularidad en países de la región
    region_countries = REGION_DEFINITIONS[region_id]["countries"]
    track_countries = []
    
    # Verificar TopCountry matches
    for i in range(1, 4):
        country_field = f"TopCountry{i}"
        country = track.get(country_field)
        if country and country in region_countries:
            score += 0.1  # Bonus por ser popular en su propia región
    
    # Bonus por idioma distintivo de la región
    language = track.get("Idioma", "").lower()
    if region_id == "latin_america" and language in ["spanish", "portuguese"]:
        score += 0.15
    elif region_id == "asia" and language in ["japanese", "korean", "mandarin", "hindi"]:
        score += 0.15
    
    # Bonus por género culturalmente distintivo (sin ser restrictivo)
    genre = track.get("Genero")
    if genre and is_culturally_distinctive(genre, region_id):
        score += 0.1
    
    return min(score, 0.3)  # Cap máximo

def compute_genre_match_bonus(track: Dict[str, Any], user_genre: str, region_id: str) -> float:
    """
    Bonus adicional cuando el usuario especifica un género
    """
    track_genre = track.get("Genero")
    if not track_genre:
        return 0.0
    
    # Normalizar géneros para matching
    track_genres = [track_genre] if isinstance(track_genre, str) else track_genre
    user_genre_lower = user_genre.lower()
    
    # Matching exacto o parcial
    for genre in track_genres:
        if genre and user_genre_lower in genre.lower():
            return 0.2  # Bonus por matching de género
    
    return 0.0

def is_culturally_distinctive(genre, region_id: str) -> bool:
    """
    Identifica géneros musicalmente distintivos de cada región
    SIN ser exclusivo - solo para bonus de relevancia
    """
    distinctive_genres = {
        "latin_america": {"salsa", "merengue", "bachata", "cumbia", "reggaeton", "samba", "tango", "bossa nova"},
        "asia": {"k-pop", "j-pop", "mandopop", "c-pop", "bollywood", "anison"},
        "africa": {"afrobeats", "highlife", "soukous", "bongo flava", "gqom"},
        "europe": {"europop", "eurodance", "eurodisco", "schlager", "fado", "flamenco"}
    }
    
    genre_str = genre.lower() if isinstance(genre, str) else str(genre).lower()
    region_genres = distinctive_genres.get(region_id, set())
    
    return any(distinctive in genre_str for distinctive in region_genres)    
    
def search_tracks_by_region(region_id: str, user_genre: str = None, limit: int = 30) -> List[Dict[str, Any]]:
    """
    Busca tracks por región geográfica con ordenamiento inteligente
    """
    if region_id not in REGION_DEFINITIONS:
        logger.warning(f"⚠️ Región desconocida: {region_id}")
        return []
    
    region_countries = REGION_DEFINITIONS[region_id]["countries"]
    
    # Filtro base: origen geográfico
    base_query = {"ArtistArea": {"$in": region_countries}}
    
    # Si el usuario especificó género, añadirlo como filtro (no restrictivo)
    if user_genre:
        base_query["Genero"] = {"$regex": user_genre, "$options": "i"}
    
    logger.debug(f"🗺️ Buscando {limit} tracks para región {region_id}, género: {user_genre or 'cualquiera'}")
    
    try:
        # Primera pasada: obtener candidatos
        candidate_tracks = list(tracks_col.find(base_query).limit(limit * 3))  # Buscar más para seleccionar
        
        if not candidate_tracks:
            logger.debug(f"⚠️ No se encontraron tracks para región {region_id}")
            return []
        
        # Calcular scores de relevancia regional para cada track
        for track in candidate_tracks:
            track["RegionRelevanceScore"] = compute_region_relevance_score(
                track, region_id, user_genre
            )
        
        # Ordenar por relevancia regional + popularidad
        candidate_tracks.sort(
            key=lambda x: (
                x.get("RegionRelevanceScore", 0), 
                x.get("RelativePopularityScore", 0)
            ), 
            reverse=True
        )
        
        # Aplicar límites por artista/álbum
        final_tracks = limit_tracks_by_artist_album(candidate_tracks)
        
        logger.debug(f"🎯 Región {region_id}: {len(candidate_tracks)} candidatos → {len(final_tracks)} finales")
        
        return final_tracks[:limit]
        
    except Exception as e:
        logger.error(f"❌ Error en búsqueda por región {region_id}: {e}")
        return []    
        
def build_region_genre_prompt(user_prompt: str, context: Dict[str, Any], analysis: Dict[str, Any]) -> str:
    """
    Prompt que maneja combinaciones de región + género
    """
    
    region_info = ""
    detected_region = analysis.get("region")
    user_genre = analysis.get("genre")
    
    if detected_region and detected_region in REGION_DEFINITIONS:
        region_data = REGION_DEFINITIONS[detected_region]
        region_info = f"""
🎯 SOLICITUD REGIONAL DETECTADA: {region_data['name']}
- REGIÓN: {region_data['description']}
- Países: {', '.join(region_data['countries'][:6])}{'...' if len(region_data['countries']) > 6 else ''}
- Género solicitado: {user_genre or 'CUALQUIER género'}
- INSTRUCCIÓN: Buscar artistas de ESTA región + género si se especifica
- NO limitar a géneros "típicos" - incluir TODOS los géneros de la región
        """
    
    prompt = f"""
ANALIZA esta solicitud musical y genera recomendaciones INTELIGENTES:

SOLICITUD: "{user_prompt}"

{region_info}

CONTEXTO DISPONIBLE:
- Artistas: {', '.join(context.get('artists', [])[:20])}
- Géneros: {', '.join(context.get('genres', [])[:12])}

INSTRUCCIONES CRÍTICAS:
1. Para "música [región]": filtrar por ORIGEN geográfico (ArtistArea)
2. Para "[género] de [región]": combinar origen + género
3. NO asumir géneros específicos para regiones
4. Priorizar popularidad + representatividad regional

EJEMPLOS:
- "rock asiático" → artistas asiáticos + género rock
- "música latina" → artistas latinoamericanos (cualquier género)
- "pop europeo" → artistas europeos + género pop

PARA "{user_prompt}", devuelve JSON:
{{
  "filters": {{
    "region": "{detected_region if detected_region else ''}",
    "genre": "{user_genre if user_genre else ''}"
  }},
  "suggestions": ["artistas representativos de la región"],
  "sort_by": "RegionRelevanceScore",  // ✅ NUEVO campo
  "order": -1
}}
"""
    return prompt        

def detect_region_from_query(query_text: str) -> Optional[str]:
    """
    Detecta automáticamente la región solicitada en el query
    """
    query_lower = query_text.lower()
    
    for region, keywords in REGION_DEFINITIONS .items():
        if any(keyword in query_lower for keyword in keywords):
            logger.debug(f"🗺️ Región detectada: {region}")
            return region
    
    return None

def enhance_region_detection(analysis: Dict[str, Any], query_text: str) -> Dict[str, Any]:
    """
    Mejora el análisis con detección de regiones
    """
    detected_region = detect_region_from_query(query_text)
    
    if detected_region:
        region_info = REGION_DEFINITIONS[detected_region]
        
        analysis.update({
            "type": "region_request",
            "region": detected_region,
            "region_name": region_info["name"],
            "genre": None,  # Limpiar género vago
            "country": None,
            "country_type": None,
            "region_corrected": True,
            "intent": f"Música {region_info['name']}: {query_text}"
        })
        logger.debug(f"🗺️ Corrección aplicada: Región {region_info['name']}")
    
    return analysis    