import os
import json
import logging
import datetime
from typing import Any, Dict, Optional

# ============================================================
# 🔹 Configuración del logger híbrido
# ============================================================

logger = logging.getLogger("playlist.hybrid")
logger.setLevel(logging.INFO)

HYBRID_LOG_PATH = os.getenv("HYBRID_LOG_PATH", "./logs/hybrid_results_log.json")
os.makedirs(os.path.dirname(HYBRID_LOG_PATH), exist_ok=True)

# ============================================================
# 🔹 Extraer JSON (wrapper para utils)
# ============================================================

def extract_json_from_text(text: str) -> Optional[Dict]:
    """
    Wrapper simplificado que intenta extraer JSON robustamente de texto,
    manejando respuestas del modelo Ollama u otros LLMs.
    """
    if not text or not isinstance(text, str):
        return None

    import re, json

    try:
        # Buscar bloques { ... } o [ ... ]
        match = re.search(r"(\{[\s\S]*\})", text)
        if match:
            return json.loads(match.group(1))
        match = re.search(r"(\[[\s\S]*\])", text)
        if match:
            return json.loads(match.group(1))
    except Exception:
        pass

    # Reparación mínima
    try:
        text_fixed = (
            text.replace("'", '"')
            .replace("False", "false")
            .replace("True", "true")
            .replace("None", "null")
        )
        return json.loads(text_fixed)
    except Exception as e:
        logger.debug(f"extract_json_from_text: no se pudo parsear JSON ({e})")

    return None

# ============================================================
# 🔹 Registrar resultados híbridos (IA + DB)
# ============================================================

def log_hybrid_result(record: Dict[str, Any]) -> None:
    """
    Registra un resultado híbrido (prompt + lista de tracks) en el log JSON.
    Cada línea del archivo contiene un objeto JSON independiente.

    Ejemplo de record:
    {
        "prompt": "rock argentino 90s",
        "tracks": [...],
        "matches_ai": 12,
        "matches_heuristic": 8,
        "timestamp": "2025-11-06T00:00:00Z"
    }
    """
    if not isinstance(record, dict):
        logger.warning("log_hybrid_result recibió un tipo no dict, ignorando entrada.")
        return

    record["timestamp"] = datetime.datetime.utcnow().isoformat()

    try:
        with open(HYBRID_LOG_PATH, "a", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
            f.write("\n")
        logger.info(f"🧾 Log híbrido registrado ({len(record.get('tracks', []))} tracks).")
    except Exception as e:
        logger.error(f"❌ No se pudo escribir en log híbrido: {e}")

# ============================================================
# 🔹 Leer últimos resultados híbridos
# ============================================================

def read_recent_hybrid_logs(limit: int = 5) -> list:
    """
    Devuelve los últimos registros del log híbrido para depuración.
    """
    try:
        with open(HYBRID_LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-limit:]
            return [json.loads(l) for l in lines if l.strip()]
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"⚠️ No se pudieron leer logs híbridos: {e}")
        return []
