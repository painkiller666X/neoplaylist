from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import settings
from database.connection import init_db
import logging

# =====================================================
# * Importación de Routers principales
# =====================================================
from auth.routes import router as auth_router
from playlist.routes import router as playlist_router

# =====================================================
# * Importación de Routers modulares (users, tracks)
# =====================================================
try:
    from routes.user_routes import router as user_router
except ImportError:
    user_router = None

try:
    from routes.track_routes import router as track_router
except ImportError:
    track_router = None

# =====================================================
# * Configuración de Logging global
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("main")

# =====================================================
# * Inicialización de la aplicación
# =====================================================
app = FastAPI(
    title=f"{settings.PROJECT_NAME} Backend",
    version=settings.VERSION,
    debug=settings.DEBUG
)

# =====================================================
# * Configuración CORS
# =====================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================================================
# * Inicialización de la Base de Datos
# =====================================================
db = init_db()
app.state.db = db  # acceso global a la DB

logger.info("✅ Base de datos inicializada correctamente y aplicación lista.")

# =====================================================
# * Registro de Rutas
# =====================================================
app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(playlist_router, prefix="/playlist", tags=["Playlist"])

if user_router:
    app.include_router(user_router, prefix="/users", tags=["Users"])
if track_router:
    app.include_router(track_router, prefix="/tracks", tags=["Tracks"])

logger.info("📜 Routers registrados:")
logger.info(" - /auth -> AuthRouter")
logger.info(" - /playlist -> PlaylistRouter")
if user_router:
    logger.info(" - /users -> UserRouter")
if track_router:
    logger.info(" - /tracks -> TrackRouter")

# =====================================================
# * Ruta raíz
# =====================================================
@app.get("/", summary="Ruta raíz del backend")
def root():
    return {
        "message": f"🚀 {settings.PROJECT_NAME} Backend activo",
        "version": settings.VERSION,
        "env": settings.ENV
    }

# =====================================================
# * Mensaje de arranque
# =====================================================
logger.info(f"🌍 {settings.PROJECT_NAME} backend iniciado en modo '{settings.ENV}'.")
