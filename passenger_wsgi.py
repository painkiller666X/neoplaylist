# backend/passenger_wsgi.py
import sys
import os
from pathlib import Path

# 🔹 Asegurar que el backend esté en el path de Python
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

# 🔹 Establecer variable de entorno
os.environ["ENV"] = "production"

# 🔹 Importar la app FastAPI
from main import app as application  # cPanel espera "application"
