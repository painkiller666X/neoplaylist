# backend/database/connection.py
import os
import logging
from pymongo import MongoClient
from dotenv import load_dotenv
from config import settings

# ============================================================
# 🌱 CARGAR ENTORNO .env
# ============================================================
dotenv_path = os.path.join(os.path.dirname(__file__), "../.env.development")
load_dotenv(dotenv_path)

# ============================================================
# 🔧 CONSTRUCTOR DE URI
# ============================================================
def build_mongo_uri():
    user = os.getenv("MONGO_USER", settings.MONGO_USER)
    password = os.getenv("MONGO_PASSWORD", settings.MONGO_PASSWORD)
    host = os.getenv("MONGO_HOST", settings.MONGO_HOST)
    port = os.getenv("MONGO_PORT", settings.MONGO_PORT)
    return f"mongodb://{user}:{password}@{host}:{port}"

# ============================================================
# 🎵 CONEXIÓN A BASE DE DATOS DE MÚSICA
# ============================================================
def get_music_db():
    try:
        mongo_uri = build_mongo_uri()
        client = MongoClient(mongo_uri)
        db_name = os.getenv("MONGO_DB", settings.MONGO_DB)
        db = client[db_name]
        logging.info(f"✅ Conectado a base de música: {db_name}")
        return db
    except Exception as e:
        logging.error(f"❌ Error conectando a MongoDB (musicdb): {e}")
        raise e

# ============================================================
# 👥 CONEXIÓN A BASE DE DATOS DE AUTENTICACIÓN
# ============================================================
def get_auth_db():
    try:
        mongo_uri = build_mongo_uri()
        client = MongoClient(mongo_uri)
        auth_name = os.getenv("MONGO_AUTH_DB", settings.MONGO_AUTH_DB)
        db = client[auth_name]
        logging.info(f"✅ Conectado a base de autenticación: {auth_name}")
        return db
    except Exception as e:
        logging.error(f"❌ Error conectando a MongoDB (authdb): {e}")
        raise e

# ============================================================
# 🧩 INSTANCIAS GLOBALES
# ============================================================
music_db = get_music_db()
auth_db = get_auth_db()

# ============================================================
# 🚀 INICIALIZACIÓN DE BASES
# ============================================================
def init_db():
    try:
        if music_db is not None and auth_db is not None:
            logging.info("✅ Conexión inicializada correctamente a ambas bases.")
        else:
            logging.error("❌ Error al inicializar las bases de datos: una o ambas conexiones son nulas.")
    except Exception as e:
        print(f"❌ Error al inicializar las bases de datos: {e}")
