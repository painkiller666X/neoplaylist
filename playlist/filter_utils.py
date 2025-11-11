import logging
from typing import Dict, Any

logger = logging.getLogger("playlist.filters")

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



def has_country_filters(filters: dict) -> bool:
    """
    Verifica si los filtros ya incluyen criterios de país.
    """
    country_indicators = ["ArtistArea", "TopCountry1", "TopCountry2", "TopCountry3", "country"]
    return any(indicator in filters for indicator in country_indicators)

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