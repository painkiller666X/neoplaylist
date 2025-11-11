import os
import re
import json
import random
import time
import logging
import requests
import urllib.parse
from typing import List, Dict, Any, Optional

from repositories.track_repository import get_all_tracks
from database.connection import music_db
from playlist.ai_engine import generate_smart_playlist
from playlist.embeddings_utils import compare_texts_similarity
from playlist.hybrid_tools import extract_json_from_text, log_hybrid_result
from playlist.popularity_utils import (
    get_global_max_values,
    compute_popularity,
    compute_relative_popularity_by_genre,
    ensure_popularity_display,
)
from playlist.finalize import finalize_enhanced_response
from playlist.intent_analysis import analyze_query_intent, enhance_region_detection
from playlist.context_utils import collect_enriched_context
from playlist.filter_utils import enrich_filters_with_acoustics, has_country_filters
from playlist.utils import adjust_limit_based_on_complexity
from playlist.prompt_builder import build_enhanced_prompt_with_country, build_completion_prompt_with_country, build_validation_prompt_with_country
from playlist.postprocessing_utils import extract_validated_tracks


# ============================================================
# 🧠 Configuración y logging
# ============================================================
logger = logging.getLogger("playlist.services")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
MODEL_NAME = os.getenv("MODEL_NAME", "neoplaylist-agent")

# Colecciones principales
tracks_col = music_db["tracks"]
playlists_col = music_db["playlists"]

# ============================================================
# 🧠 Utilidades base
# ============================================================
def call_ollama_safe(prompt_text: str, model: str = MODEL_NAME, timeout: int = 45) -> Any:
    """Ejecuta una llamada segura al modelo Ollama."""
    payload = {"model": model, "prompt": prompt_text, "stream": False}
    try:
        logger.info(f"🧠 Llamando a Ollama ({model})...")
        resp = requests.post(OLLAMA_URL, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        text = data.get("response") or data.get("completion") or json.dumps(data)
        return extract_json_from_text(text) or {"raw": text}
    except Exception as e:
        logger.error(f"❌ Error en llamada Ollama: {e}")
        return {"error": str(e)}

# ============================================================
# 🔹 Normalización y deduplicación
# ============================================================
def normalize_title_for_dedupe(s: str) -> str:
    """Normalización MÁS AGRESIVA para eliminar versiones."""
    if not s:
        return ""
    
    # Convertir a minúsculas primero
    s = s.lower()
    
    # Eliminar TODO entre paréntesis y corchetes (más agresivo)
    s = re.sub(r"\s*[\[\(].*?[\]\)]", "", s)
    
    # Eliminar palabras comunes de versiones (lista expandida)
    version_patterns = [
        r"\b(remastered?|remaster|remix|remixed|live|version|album version|explicit|clean|single|edit|original|demo|acoustic|instrumental|radio edit|extended|short|long)\b",
        r"\b(\d{4} remaster|\d{4} version|\d{4} mix|\d{4} digital|\d{4} master)\b",
        r"\b(feat\.|ft\.|featuring|with|vs\.|pres\.|&)\b.*",
        r"\b(mono|stereo|digital|analog|hi-res|hires|lossless|flac|mp3|wav|aiff)\b",
        r"[-–]\s*(live|remaster|remix|version|edit|demo|acoustic).*$",
        r"\b(bonus track|deluxe|special edition|expanded|reissue|re-issue)\b",
        r"\b(from .*? soundtrack|original motion picture)\b",
        r"\b(take \d+|alternate|early|rough)\b"
    ]
    
    for pattern in version_patterns:
        s = re.sub(pattern, "", s, flags=re.IGNORECASE)
    
    # Eliminar caracteres especiales y espacios múltiples
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    
    result = s.strip()
    logger.debug(f"   🎯 Normalización: '{s}' -> '{result}'")
    return result

def deduplicate_tracks_by_title_keep_best(tracks_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Elimina duplicados manteniendo el track con mejor bitrate y popularidad."""
    logger.info(f"🔍 DEDUPLICACIÓN: Entrada con {len(tracks_list)} pistas")
    
    best = {}
    duplicates_found = 0
    
    for t in tracks_list:
        original_title = t.get("Titulo", "") or ""
        key = normalize_title_for_dedupe(original_title)
        
        if not key:
            key = (t.get("Ruta") or "")[:200]
        
        bitrate = t.get("Bitrate") or 0
        pop = t.get("PopularityScore") or 0.0

        if key not in best:
            best[key] = t
            logger.debug(f"   ✅ Nueva: '{original_title}' -> clave: '{key}'")
        else:
            duplicates_found += 1
            prev = best[key]
            prev_bitrate = prev.get("Bitrate") or 0
            prev_pop = prev.get("PopularityScore") or 0.0
            
            # DEBUG: Mostrar conflicto
            logger.debug(f"   ⚠️ Duplicado #{duplicates_found}: '{original_title}'")
            logger.debug(f"      Clave normalizada: '{key}'")
            logger.debug(f"      Actual: {bitrate} kbps, pop: {pop:.2f}")
            logger.debug(f"      Previo: {prev_bitrate} kbps, pop: {prev_pop:.2f}")
            
            if bitrate > prev_bitrate or (bitrate == prev_bitrate and pop > prev_pop):
                best[key] = t
                logger.debug(f"   🔄 REEMPLAZADO por mejor versión")

    result = list(best.values())
    logger.info(f"✅ DEDUPLICACIÓN: {len(tracks_list)} → {len(result)} pistas ({duplicates_found} duplicados eliminados)")
    
    # DEBUG: Mostrar pistas únicas
    if result:
        logger.info("🏆 PRIMERAS 5 PISTAS ÚNICAS:")
        for i, track in enumerate(result[:5]):
            logger.info(f"   {i+1}. {track.get('Titulo')} - {track.get('Artista')}")
    
    return result

def parse_filters_from_llm(llm_filters: dict) -> dict:
    """Normaliza filtros de año, década, país y género provenientes del LLM."""
    logger.info(f"🧹 PARSEANDO FILTROS LLM: {llm_filters}")
    
    if not llm_filters:
        logger.info("❌ No hay filtros para parsear")
        return {}

    out = {}

    # ✅ CORRECCIÓN: Manejar década como lista o string
    if "Decada" in llm_filters:
        val = llm_filters["Decada"]
        logger.info(f"🕰️ Procesando década: {val}")
        
        decades_to_process = []
        
        # Si es una lista de décadas
        if isinstance(val, list):
            decades_to_process = val
        # Si es un string individual
        elif isinstance(val, str):
            decades_to_process = [val]
        
        # Procesar cada década
        year_ranges = []
        for decade_str in decades_to_process:
            if isinstance(decade_str, str):
                # Extraer números de "1970s", "80s", etc.
                match = re.search(r"(\d{2,4})s?", decade_str)
                if match:
                    decade_num = match.group(1)
                    if len(decade_num) == 2:  # "80s"
                        start_year = 1900 + int(decade_num)
                    else:  # "1970s" 
                        start_year = int(decade_num)
                    
                    year_ranges.append((start_year, start_year + 10))
                    logger.info(f"🕰️ Década detectada: {start_year}s")
        
        # Crear filtro MongoDB para múltiples décadas
        if year_ranges:
            or_conditions = []
            for start_year, end_year in year_ranges:
                or_conditions.append({"Año": {"$gte": start_year, "$lt": end_year}})
            
            if len(or_conditions) == 1:
                out["Año"] = or_conditions[0]["Año"]
            elif len(or_conditions) > 1:
                out["$or"] = or_conditions
            
            # Agregar también el campo Decada para búsqueda directa
            decade_strings = [f"{start}s" for start, _ in year_ranges]
            if len(decade_strings) == 1:
                out["Decada"] = decade_strings[0]
            else:
                out["Decada"] = {"$in": decade_strings}

    # ✅ Género - manejar tanto string como diccionario
    genre_keys = ["genero", "género", "genre", "Genero", "género_principal"]
    for key in genre_keys:
        if key in llm_filters:
            v = llm_filters[key]
            if isinstance(v, str) and v.strip():
                out["Genero"] = {"$regex": v, "$options": "i"}
                logger.info(f"🎵 Filtro género aplicado: '{v}'")
                break
            elif isinstance(v, dict) and "$regex" in v:
                # Si ya viene en formato MongoDB, usarlo directamente
                out["Genero"] = v
                logger.info(f"🎵 Filtro género (formato Mongo): {v}")
                break

    # ✅ Año específico
    if "year" in llm_filters:
        year = llm_filters["year"]
        if isinstance(year, (int, str)) and str(year).isdigit():
            year_int = int(year)
            out["Año"] = {"$gte": year_int, "$lt": year_int + 1}
            logger.info(f"📅 Filtro año: {year_int}")

    logger.info(f"✅ FILTROS PARSEADOS FINALES: {out}")
    return out
    
# ============================================================
# 🔹 Filtro de incongruencias (idéntico al monolítico)
# ============================================================
def filter_gross_incongruities(tracks, query_text: str):
    """Elimina pistas incoherentes con el prompt."""
    cleaned = []
    for t in tracks:
        title = (t.get("Titulo") or "").lower()
        genero_val = t.get("Genero")
        genre = " ".join(genero_val).lower() if isinstance(genero_val, list) else (genero_val or "").lower()
        if any(x in query_text.lower() for x in [genre, title.split(" ")[0]]):
            cleaned.append(t)
    return cleaned


# ============================================================
# 🔹 Límite por artista / álbum (avanzado)
# ============================================================
def limit_tracks_by_artist_album(
    tracks_list: List[Dict[str, Any]],
    max_per_artist: int = 3,  # ✅ REDUCIDO de 20 a 3
    max_per_album: int = 2    # ✅ REDUCIDO de 5 a 2
) -> List[Dict[str, Any]]:
    """Limita cantidad de pistas por artista y álbum con logs detallados."""
    logger.info(f"👥 LIMITAR ARTISTA/ÁLBUM: Entrada {len(tracks_list)} pistas")
    
    result, artist_counts, album_counts = [], {}, {}
    limited_count = 0

    for t in sorted(tracks_list, key=lambda x: x.get("RelativePopularityScore", 0), reverse=True):
        artist = (t.get("Artista") or "").strip().lower()
        album = (t.get("Album") or "").strip().lower()
        artist_key = artist
        album_key = f"{artist}::{album}" if album else artist

        current_artist_count = artist_counts.get(artist_key, 0)
        current_album_count = album_counts.get(album_key, 0)

        if current_artist_count >= max_per_artist:
            logger.debug(f"   🚫 Límite artista: {artist} ({current_artist_count}/{max_per_artist}) - {t.get('Titulo')}")
            limited_count += 1
            continue
        if current_album_count >= max_per_album:
            logger.debug(f"   🚫 Límite álbum: {album} ({current_album_count}/{max_per_album}) - {t.get('Titulo')}")
            limited_count += 1
            continue

        result.append(t)
        artist_counts[artist_key] = current_artist_count + 1
        album_counts[album_key] = current_album_count + 1

    logger.info(f"✅ LIMITAR ARTISTA/ÁLBUM: {len(tracks_list)} → {len(result)} pistas ({limited_count} limitadas)")
    logger.info(f"   Artistas únicos: {len(artist_counts)}")
    
    # Mostrar distribución de artistas
    top_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    logger.info(f"   Top artistas: {top_artists}")
    
    return result

def apply_intelligent_postprocessing(tracks: list, user_prompt: str, llm_analysis: dict, limit: int) -> list:
    """Aplica deduplicación, popularidad y filtros avanzados con DEBUG."""
    logger.info(f"🧠 POSTPROCESAMIENTO: Entrada con {len(tracks)} pistas")
    
    if not tracks:
        logger.warning("❌ POSTPROCESAMIENTO: Lista de pistas vacía")
        return tracks

    # 1. Calcular popularidad
    global_max = get_global_max_values()
    for t in tracks:
        t["PopularityScore"] = compute_popularity(t, global_max)
    logger.info(f"📊 POSTPROCESAMIENTO: Popularidad calculada para {len(tracks)} pistas")

    # 2. Deduplicar
    deduped = deduplicate_tracks_by_title_keep_best(tracks)
    logger.info(f"🔍 POSTPROCESAMIENTO: Deduplicación {len(tracks)} → {len(deduped)} pistas")

    # 3. Popularidad relativa por género
    compute_relative_popularity_by_genre(deduped)
    logger.info(f"⭐ POSTPROCESAMIENTO: Popularidad relativa calculada")

    # 4. Filtrar incongruencias (DEBUG EXTENDIDO)
    filtered = filter_gross_incongruities(deduped, user_prompt)
    logger.info(f"🎯 POSTPROCESAMIENTO: Filtro incongruencias {len(deduped)} → {len(filtered)} pistas")
    
    # DEBUG: Mostrar qué pistas se eliminaron
    if len(filtered) < len(deduped):
        removed = set([t.get("Titulo") for t in deduped]) - set([t.get("Titulo") for t in filtered])
        logger.info(f"🗑️ POSTPROCESAMIENTO: Se eliminaron {len(removed)} pistas: {list(removed)[:5]}")

    # 5. Limitar por artista/álbum
    limited = limit_tracks_by_artist_album(filtered)
    logger.info(f"👥 POSTPROCESAMIENTO: Límite artista/álbum {len(filtered)} → {len(limited)} pistas")

    # 6. Ordenar por popularidad
    limited.sort(key=lambda x: x.get("RelativePopularityScore", 0), reverse=True)
    logger.info(f"📈 POSTPROCESAMIENTO: Ordenado por popularidad relativa")

    # 7. Aplicar límite final
    result = limited[:limit]
    logger.info(f"🎯 POSTPROCESAMIENTO: Límite final {len(limited)} → {len(result)} pistas")
    
    # DEBUG: Mostrar top 3 pistas finales
    if result:
        logger.info("🏆 TOP 3 PISTAS FINALES:")
        for i, track in enumerate(result[:3]):
            logger.info(f"   {i+1}. {track.get('Titulo')} - {track.get('Artista')} (Score: {track.get('RelativePopularityScore', 0):.2f})")

    return result

# ============================================================
# 🔍 Búsqueda avanzada en Mongo (CORREGIDA)
# ============================================================
def search_tracks_in_mongo(sugerencia, llm_filters, limit, collection, user_prompt=None):
    """
    Busca sugerencias en Mongo combinando coincidencias flexibles (Titulo/Artista/Album)
    y los filtros normalizados del LLM.
    """
    results = []
    seen_rutas = set()
    normalized_filters = parse_filters_from_llm(llm_filters or {})
    
    logger.info(f"🔍 BUSQUEDA MONGO: {len(sugerencia)} sugerencias, filtros: {normalized_filters}, límite: {limit}")

    # ✅ ESTRATEGIA 1: Búsqueda por sugerencias específicas
    if sugerencia:
        for s in sugerencia:
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

            if not and_clauses:
                continue

            query = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

            try:
                # ✅ CORRECCIÓN: usar .limit() en lugar de .limites()
                found = list(collection.find(query).limit(5))
                logger.debug(f"  🎯 Sugerencia '{titulo}' -> {len(found)} resultados")
            except Exception as e:
                logger.error(f"❌ Error en búsqueda Mongo: {e}")
                found = []

            for f in found:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break

    # ✅ ESTRATEGIA 2: Búsqueda DIRECTA por filtros
    if len(results) < limit and normalized_filters:
        logger.info("🎯 BUSQUEDA DIRECTA por filtros (pocos resultados)")
        
        try:
            direct_query = normalized_filters
            direct_results = list(collection.find(direct_query).sort("PopularityScore", -1).limit(limit * 2))
            
            for f in direct_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
                        
            logger.info(f"🎯 Búsqueda directa: +{len(direct_results)} pistas -> total {len(results)}")
            
        except Exception as e:
            logger.error(f"❌ Error en búsqueda directa: {e}")

    # ✅ ESTRATEGIA 3: Búsqueda por década
    if len(results) < limit and "Decada" in normalized_filters:
        try:
            decade = normalized_filters["Decada"]
            decade_query = {"Decada": decade}
            decade_results = list(collection.find(decade_query).sort("PopularityScore", -1).limit(limit))
            
            for f in decade_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
                        
            logger.info(f"🕰️ Búsqueda década '{decade}': +{len(decade_results)} pistas")
            
        except Exception as e:
            logger.error(f"❌ Error en búsqueda por década: {e}")

    # ✅ ESTRATEGIA 4: Búsqueda por palabras clave
    if len(results) < limit and not sugerencia and not normalized_filters and user_prompt:
        logger.info("🔄 BUSQUEDA POR PALABRAS CLAVE (fallback)")
        
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
            
            keyword_results = list(collection.find(keyword_query).limit(limit))
            for f in keyword_results:
                ruta = f.get("Ruta")
                if ruta and ruta not in seen_rutas:
                    results.append(f)
                    seen_rutas.add(ruta)
                    if len(results) >= limit:
                        break
            
            logger.info(f"🔤 Búsqueda keywords: +{len(keyword_results)} pistas")

    logger.info(f"✅ BUSQUEDA MONGO COMPLETADA: {len(results)} pistas encontradas")
    return results


# ============================================================
# 🔹 Ciclo híbrido principal
# ============================================================
def hybrid_playlist_cycle(prompt: str, model: str = MODEL_NAME, default_limit: int = 40):
    """Ciclo híbrido mejorado basado en el monolítico."""
    logger.info(f"🎧 Generando playlist híbrida: '{prompt}'")
    start_time = time.time()

    
    llm_analysis = analyze_query_intent(prompt)
    detected_limit = llm_analysis.get("detected_limit", default_limit)
    limit = min(detected_limit, 100)

    # 1️⃣ Llamada inicial a Ollama
    result = call_ollama_safe(prompt, model) or {}
    suggestions = result.get("suggestions", [])
    llm_filters = result.get("filters", {}) or {}

    # 2️⃣ Buscar coincidencias locales
    found = search_tracks_in_mongo(suggestions, llm_filters, limit, music_db.tracks, prompt)

    # 3️⃣ Postprocesamiento avanzado
    final_tracks = apply_intelligent_postprocessing(found, prompt, llm_analysis, limit)

    logger.info(f"✅ Playlist finalizada con {len(final_tracks)} pistas (prompt: '{prompt}')")

    # 4️⃣ Respuesta enriquecida (idéntica al monolítico)
    return finalize_enhanced_response(prompt, llm_filters, final_tracks, 3, limit, start_time, llm_analysis)

# ============================================================
# 🔁 Ciclo híbrido mejorado (COMPLETAMENTE CORREGIDO)
# ============================================================
def hybrid_playlist_cycle_enhanced(user_prompt: str, model: str = MODEL_NAME, default_limit: int = 40, llm_analysis: dict = None):
    """
    Ciclo híbrido mejorado con debugging extensivo y POSTPROCESAMIENTO EN TODAS LAS FASES.
    """
    start_time = time.time()
    logger.info(f"🚀 INICIANDO CICLO HÍBRIDO: '{user_prompt}'")

    try:
        # 🧩 1. CONTEXTO ENRIQUECIDO
        enriched_context = collect_enriched_context()
        logger.info(f"📊 CONTEXTO: {len(enriched_context.get('genres', []))} géneros, {len(enriched_context.get('artists', []))} artistas")

        # 🧠 2. ANÁLISIS SEMÁNTICO
        if llm_analysis is None:
            llm_analysis = analyze_query_intent(user_prompt)
        llm_analysis = enhance_region_detection(llm_analysis, user_prompt)
        logger.info(f"🎯 ANÁLISIS: {llm_analysis}")

        # 🎚️ 3. AJUSTE DE LÍMITE
        adjusted_limit = adjust_limit_based_on_complexity(user_prompt, default_limit, llm_analysis)
        logger.info(f"📏 LÍMITE: {default_limit} → {adjusted_limit}")

        # 📝 4. FASE 1: PROMPT INICIAL
        phase1_prompt = build_enhanced_prompt_with_country(user_prompt, enriched_context, llm_analysis)
        logger.info(f"📤 FASE 1 - PROMPT:\n{phase1_prompt[:500]}...")

        # 🤖 5. LLAMADA OLLAMA FASE 1
        result = call_ollama_safe(phase1_prompt, model) or {}
        llm_filters = result.get("filters", {}) if isinstance(result, dict) else {}
        suggestions = result.get("suggestions", []) if isinstance(result, dict) else []
        
        logger.info(f"🤖 FASE 1 - RESPUESTA OLLAMA: {len(suggestions)} sugerencias, filtros: {llm_filters}")

        # 🌎 6. FILTROS DE PAÍS
        if llm_analysis.get("country"):
            llm_filters["country"] = llm_analysis["country"]
            llm_filters["country_type"] = llm_analysis.get("country_type", "origin")
            logger.info(f"🇺🇸 FILTRO PAÍS forzado: {llm_analysis['country']}")

        # 🧮 7. PARSEAR FILTROS
        filters = parse_filters_from_llm(llm_filters)
        filters = enrich_filters_with_acoustics(user_prompt, filters)
        logger.info(f"🎯 FILTROS ACTIVOS: {filters}")

        # 🔍 8. BÚSQUEDA LOCAL FASE 1 (CORREGIDO)
        search_start = time.time()
        local_tracks = search_tracks_in_mongo(
            sugerencia=suggestions,
            llm_filters=filters,
            limit=adjusted_limit,
            collection=tracks_col,
            user_prompt=user_prompt
        )
        search_time = time.time() - search_start
        
        logger.info(f"🎧 FASE 1 - RESULTADOS: {len(local_tracks)} pistas en {search_time:.2f}s")

        # ✅ CORRECCIÓN CRÍTICA: APLICAR POSTPROCESAMIENTO EN FASE 1
        processed_tracks_phase1 = apply_intelligent_postprocessing(local_tracks, user_prompt, llm_analysis, adjusted_limit)
        logger.info(f"🧠 FASE 1 - POSTPROCESADO: {len(local_tracks)} → {len(processed_tracks_phase1)} pistas")

        if len(processed_tracks_phase1) >= adjusted_limit:
            logger.info("✅ SUFICIENTES RESULTADOS FASE 1 - FINALIZANDO")
            return finalize_enhanced_response(user_prompt, filters, processed_tracks_phase1, 1, adjusted_limit, start_time, llm_analysis)

        # 🔁 9. FASE 2: COMPLETAR RESULTADOS
        missing = adjusted_limit - len(processed_tracks_phase1)
        logger.info(f"🔄 FASE 2: Faltan {missing} pistas (después de postprocesamiento)")

        phase2_prompt = build_completion_prompt_with_country(
            user_prompt, filters, processed_tracks_phase1, enriched_context, missing, llm_analysis
        )
        logger.info(f"📤 FASE 2 - PROMPT:\n{phase2_prompt[:400]}...")
        
        result2 = call_ollama_safe(phase2_prompt, model) or {}
        suggestions2 = result2.get("suggestions", []) if isinstance(result2, dict) else []
        new_filters = result2.get("filters", {}) if isinstance(result2, dict) else {}
        
        logger.info(f"🤖 FASE 2 - RESPUESTA: {len(suggestions2)} nuevas sugerencias")

        # Fusionar filtros
        if new_filters:
            filters.update(parse_filters_from_llm(new_filters))

        # ✅ CORRECCIÓN: Parámetros correctos para Fase 2
        local_tracks2 = search_tracks_in_mongo(
            sugerencia=suggestions2,
            llm_filters=filters,
            limit=missing * 2,  # Buscar más para compensar postprocesamiento
            collection=tracks_col,
            user_prompt=user_prompt
        )

        # ✅ CORRECCIÓN: APLICAR POSTPROCESAMIENTO a los nuevos resultados de Fase 2
        processed_tracks2 = apply_intelligent_postprocessing(local_tracks2, user_prompt, llm_analysis, missing)
        logger.info(f"🧠 FASE 2 - POSTPROCESADO: {len(local_tracks2)} → {len(processed_tracks2)} nuevas pistas")

        # Combinar resultados de Fase 1 y Fase 2
        all_tracks_phase2 = processed_tracks_phase1 + processed_tracks2
        logger.info(f"🎯 FASE 2 - COMBINADO: {len(processed_tracks_phase1)} + {len(processed_tracks2)} = {len(all_tracks_phase2)} pistas")

        # ✅ CORRECCIÓN: APLICAR POSTPROCESAMIENTO FINAL al conjunto combinado
        final_processed_phase2 = apply_intelligent_postprocessing(all_tracks_phase2, user_prompt, llm_analysis, adjusted_limit)
        logger.info(f"🧠 FASE 2 - POSTPROCESADO FINAL: {len(all_tracks_phase2)} → {len(final_processed_phase2)} pistas")

        if len(final_processed_phase2) >= adjusted_limit:
            logger.info("✅ SUFICIENTES RESULTADOS FASE 2 - FINALIZANDO")
            return finalize_enhanced_response(user_prompt, filters, final_processed_phase2, 2, adjusted_limit, start_time, llm_analysis)

        # ✅ 10. FASE 3: VALIDACIÓN FINAL
        logger.info(f"🔍 FASE 3: Validación final con {len(final_processed_phase2)} pistas")
        phase3_prompt = build_validation_prompt_with_country(
            user_prompt, filters, final_processed_phase2, enriched_context, llm_analysis
        )
        logger.info(f"📤 FASE 3 - PROMPT:\n{phase3_prompt[:400]}...")
        
        result3 = call_ollama_safe(phase3_prompt, model) or {}
        validated = extract_validated_tracks(result3, final_processed_phase2, adjusted_limit)
        
        logger.info(f"✅ FASE 3 - VALIDACIÓN: {len(validated)} pistas validadas")

        # 🧠 11. POSTPROCESAMIENTO FINAL (EXTRA SEGURIDAD)
        final_tracks = apply_intelligent_postprocessing(validated, user_prompt, llm_analysis, adjusted_limit)
        logger.info(f"🎉 PROCESO COMPLETADO: {len(final_tracks)} pistas finales")

        # VERIFICACIÓN FINAL DE CALIDAD
        if final_tracks:
            artist_distribution = {}
            for track in final_tracks:
                artist = track.get("Artista", "Desconocido")
                artist_distribution[artist] = artist_distribution.get(artist, 0) + 1
            
            logger.info("🏆 DISTRIBUCIÓN FINAL DE ARTISTAS:")
            for artist, count in sorted(artist_distribution.items(), key=lambda x: x[1], reverse=True)[:8]:
                logger.info(f"   {artist}: {count} pistas")
            
            # Verificar duplicados
            titles = [normalize_title_for_dedupe(t.get("Titulo", "")) for t in final_tracks]
            unique_titles = set(titles)
            if len(titles) != len(unique_titles):
                logger.warning(f"⚠️ POSIBLES DUPLICADOS: {len(titles)} títulos → {len(unique_titles)} únicos")

        # 📊 12. RESPUESTA FINAL
        return finalize_enhanced_response(user_prompt, filters, final_tracks, 3, adjusted_limit, start_time, llm_analysis)

    except Exception as e:
        logger.error(f"💥 ERROR en ciclo híbrido: {e}", exc_info=True)
        return emergency_fallback(user_prompt, default_limit, start_time, str(e))

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
            # ✅ APLICAR POSTPROCESAMIENTO AL FALLBACK TAMBIÉN
            processed = apply_intelligent_postprocessing(fallback_tracks, user_prompt, {}, limit)

            return finalize_enhanced_response(user_prompt, {"fallback": True, "error": error_msg},
                                              processed, 0, limit, start_time, None)
    except Exception as e:
        logger.error(f"💥 Fallback también falló: {e}")

    random_tracks = list(tracks_col.find().sort("PopularityScore", -1).limit(limit))
    # ✅ APLICAR POSTPROCESAMIENTO AL FALLBACK DE EMERGENCIA TAMBIÉN
    processed_random = apply_intelligent_postprocessing(random_tracks, user_prompt, {}, limit)
    return finalize_enhanced_response(user_prompt, {"emergency_fallback": True},
                                      processed_random, 0, limit, start_time, None)

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
        
def apply_limits_and_fallback(results: List[Dict[str, Any]], query_text: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Aplica límites por artista/álbum y fallback flexible si queda vacía."""
    logger.debug("[APPLY] Iniciando postprocesamiento final (límite + fallback)")
    limited = limit_tracks_by_artist_album(results)
    if not limited:
        logger.debug("[APPLY] Playlist vacía tras límites → aplicando fallback flexible.")
        limited = flexible_fallback_selection(query_text, limit=limit)
    return limited[:limit]                                      
    
def emergency_fallback_response(user_prompt: str, error_msg: str):
    """Respuesta de fallback de emergencia mejorada."""
    logger.warning(f"🆘 FALLBACK DE EMERGENCIA: {error_msg}")
    
    try:
        # Buscar pistas por palabras clave
        fallback_tracks = flexible_fallback_selection(user_prompt, 15)
        
        response = {
            "query_original": user_prompt,
            "playlist_name": f"Playlist de emergencia - {user_prompt[:30]}...",
            "criterio_orden": "PopularityScore", 
            "total": len(fallback_tracks),
            "playlist": fallback_tracks,  # ✅ Asegurar que este campo tenga pistas
            "archivo_m3u": "",
            "playlist_uuid": str(uuid.uuid4()),
            "user_email": "anonymous",
            "debug_summary": {
                "error": error_msg,
                "fallback_used": True,
                "tracks_found": len(fallback_tracks)
            }
        }
        
        logger.info(f"✅ FALLBACK: {len(fallback_tracks)} pistas devueltas")
        return response
        
    except Exception as e:
        logger.error(f"💥 Fallback también falló: {e}")
        # Respuesta mínima pero con estructura correcta
        return {
            "query_original": user_prompt,
            "playlist_name": "Playlist vacía",
            "criterio_orden": "none",
            "total": 0,
            "playlist": [],  # ✅ Lista vacía pero presente
            "archivo_m3u": "",
            "playlist_uuid": str(uuid.uuid4()),
            "user_email": "anonymous", 
            "debug_summary": {"error": f"Original: {error_msg}, Fallback: {str(e)}"}
        }