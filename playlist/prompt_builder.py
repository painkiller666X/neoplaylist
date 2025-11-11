import json
from typing import Dict, Any

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
