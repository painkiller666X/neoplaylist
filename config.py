# backend/config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# ============================================================
# 🌍 DETECTAR ENTORNO Y CARGAR .env CORRESPONDIENTE
# ============================================================
ENV = os.getenv("ENV", "production" if "PASSENGER_ENV" in os.environ else "development")

env_file = ".env.production" if ENV == "production" else ".env.development"
dotenv_path = Path(__file__).resolve().parent / env_file
load_dotenv(dotenv_path)

# ============================================================
# ⚙️ CONFIGURACIÓN GENERAL
# ============================================================
class Settings:
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "NeoPlaylist")
    VERSION: str = os.getenv("VERSION", "1.0")

    # 🔹 Mongo principal (música)
    MONGO_USER: str = os.getenv("MONGO_USER")
    MONGO_PASSWORD: str = os.getenv("MONGO_PASSWORD")
    MONGO_HOST: str = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT: str = os.getenv("MONGO_PORT", "27017")
    MONGO_DB: str = os.getenv("MONGO_DB", "musicdb")

    # 🔹 Base separada para autenticación
    MONGO_AUTH_DB: str = os.getenv("MONGO_AUTH", "authdb")

    # 🔹 Otros
    ALLOWED_ORIGINS: list = os.getenv("ALLOWED_ORIGINS", "*").split(",")
    DEBUG: bool = ENV == "development"
    ENV: str = ENV

settings = Settings()
