from fastapi import APIRouter, HTTPException, Header
from pymongo import MongoClient
from datetime import datetime, timedelta
import bcrypt, secrets, os, requests, json
from dotenv import load_dotenv



# ============================================================
# 馃敼 CARGA DE CONFIGURACIONES
# ============================================================

load_dotenv()
SESSION_TTL_HOURS = int(os.getenv("SESSION_TTL_HOURS", "24"))

router = APIRouter(prefix="/api", tags=["Auth Beta"])



# ============================================================
# 馃敼 CONEXI脫N MONGO
# ============================================================
mongo_url = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(mongo_url)
db = client["authdb"]
users_col = db["users"]

# ============================================================
# 馃敼 ENDPOINT: VERIFICAR INVITACI脫N
# ============================================================
@router.post("/check-invite")
def check_invite(payload: dict):
    """Valida si el email existe y determina el siguiente paso (signup/login)."""
    email = payload.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email requerido")

    user = users_col.find_one({"email": email})
    if not user:
        return {"invited": False, "is_registered": False}

    return {
        "invited": True,
        "is_registered": user.get("is_registered", False),
        "name": user.get("name"),
    }

# ============================================================
# 馃敼 ENDPOINT: REGISTRO DE USUARIO (Signup)
# ============================================================
@router.post("/signup")
def signup_user(payload: dict):
    email = payload.get("email")
    invite_code = payload.get("invite_code")
    username = payload.get("username")
    password = payload.get("password")
    bio = payload.get("bio", "")
    genres = payload.get("genres", [])
    name = payload.get("name", "")
    language = payload.get("language", "es")

    if not all([email, invite_code, username, password]):
        raise HTTPException(status_code=400, detail="Faltan datos obligatorios")

    user = users_col.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    if user.get("is_registered"):
        raise HTTPException(status_code=400, detail="Usuario ya registrado")

    if user["invite_code"] != invite_code:
        raise HTTPException(status_code=401, detail="C贸digo de invitaci贸n inv谩lido")

    if users_col.find_one({"username": username}):
        raise HTTPException(status_code=409, detail="Nombre de usuario no disponible")

    hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    users_col.update_one(
        {"email": email},
        {"$set": {
            "username": username,
            "password": hashed_pw,
            "bio": bio,
            "name": name,
            "preferences": {
                "genres": genres,
                "language": language,
                "dark_mode": True
            },
            "is_registered": True,
            "invite_code": None,
            "role": "user",
            "updated_at": datetime.utcnow().isoformat()
        }}
    )

    return {
        "status": "registered",
        "username": username,
        "email": email,
        "message": f"Usuario {username} registrado exitosamente."
    }

# ============================================================
# 馃敼 ENDPOINT: LOGIN CON CONTRASE脩A
# ============================================================
@router.post("/login-password")
def login_with_password(payload: dict):
    email = payload.get("email")
    password = payload.get("password")

    if not email or not password:
        raise HTTPException(status_code=400, detail="Email y password requeridos")

    user = users_col.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    if not user.get("is_registered", False):
        raise HTTPException(status_code=403, detail="Usuario no registrado")

    if not user.get("is_active", True):
        raise HTTPException(status_code=403, detail="Usuario desactivado")

    stored_pw = user.get("password")
    if not stored_pw or not bcrypt.checkpw(password.encode("utf-8"), stored_pw.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Credenciales inv谩lidas")

    token = secrets.token_hex(32)
    expiry = (datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS)).isoformat()

    users_col.update_one(
        {"email": email},
        {"$set": {
            "session_token": token,
            "session_expires_at": expiry,
            "last_login": datetime.utcnow().isoformat(),
            "stats.online": True
        }}
    )

    return {
        "status": "ok",
        "session_token": token,
        "session_expires_at": expiry,
        "username": user.get("username"),
        "email": email,
        "name": user.get("name"),
        "role": user.get("role", "user")
    }

# ============================================================
# 馃敼 ENDPOINT: VALIDAR TOKEN
# ============================================================
@router.get("/validate-token")
def validate_token(Authorization: str = Header(None)):
    if not Authorization:
        raise HTTPException(status_code=401, detail="Token requerido")

    token = Authorization.replace("Bearer ", "")
    user = users_col.find_one({"session_token": token})
    if not user:
        raise HTTPException(status_code=401, detail="Token inv谩lido")

    expires_at = user.get("session_expires_at")
    if expires_at:
        try:
            exp_dt = datetime.fromisoformat(expires_at)
            if datetime.utcnow() > exp_dt:
                users_col.update_one(
                    {"session_token": token},
                    {"$unset": {"session_token": "", "session_expires_at": ""}, "$set": {"stats.online": False}}
                )
                raise HTTPException(status_code=401, detail="Token expirado")
        except Exception:
            pass

    return {
        "valid": True,
        "email": user["email"],
        "name": user.get("name"),
        "username": user.get("username"),
        "role": user.get("role", "user")
    }

# ============================================================
# 馃敼 ENDPOINT: LOGOUT
# ============================================================
@router.post("/logout")
def logout_user(Authorization: str = Header(None)):
    if not Authorization:
        raise HTTPException(status_code=401, detail="Token requerido")

    token = Authorization.replace("Bearer ", "")
    users_col.update_one(
        {"session_token": token},
        {"$unset": {"session_token": "", "session_expires_at": ""}, "$set": {"stats.online": False}}
    )
    return {"status": "logged_out"}

# ============================================================
# 🎼 ENDPOINT: GENERAR INVITACIÓN (SOLO ADMIN)
# ============================================================
@router.post("/invite")
def create_invite(payload: dict):
    admin_key = payload.get("admin_key")
    if admin_key != os.getenv("ADMIN_KEY"):
        raise HTTPException(status_code=401, detail="No autorizado")

    name = payload.get("name")
    email = payload.get("email")
    if not name or not email:
        raise HTTPException(status_code=400, detail="Datos incompletos")

    # Prevenir duplicados
    existing = users_col.find_one({"email": email})
    if existing:
        raise HTTPException(status_code=409, detail="El usuario ya fue invitado o registrado.")

    invite_code = f"BETA-{secrets.token_hex(3).upper()}"
    user = {
        "email": email,
        "name": name,
        "invite_code": invite_code,
        "is_active": True,
        "is_registered": False,
        "role": "user",
        "created_at": datetime.utcnow().isoformat(),
        "stats": {"playlists_generated": 0, "feedbacks_given": 0, "online": False},
    }
    users_col.insert_one(user)

    # Envío del correo
    send_invite_email(email, name, invite_code)

    return {"invite_code": invite_code, "email": email}


# ============================================================
# 🎼 FUNCIÓN AUXILIAR: ENVÍO DE EMAIL CON BREVO
# ============================================================
def send_invite_email(to_email: str, name: str, invite_code: str):
    """Envía correo de invitación con HTML musical completo."""
    import requests, os, json

    BREVO_API_KEY = os.getenv("BREVO_API_KEY")
    subject = "🎧 Invitación a NeoPlaylist Beta"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>NeoPlaylist Beta</title>
      <style>
        body {{
            margin: 0;
            font-family: 'Arial', sans-serif;
            background-color: #0d0d0d;
            color: #ffffff;
        }}
        .container {{
            max-width: 700px;
            margin: 0 auto;
            background-color: #121212;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 0 15px rgba(138, 43, 226, 0.2);
        }}
        .header {{
            background: linear-gradient(90deg, #8a2be2, #6a5acd);
            text-align: center;
            padding: 40px 20px;
            color: #ffffff;
            position: relative;
        }}
        .header h1 {{
            margin: 0;
            font-size: 32px;
            text-shadow: 0 0 8px #8a2be2;
        }}
        .header::after {{
            content: "🎵";
            font-size: 50px;
            position: absolute;
            right: 20px;
            bottom: -10px;
            opacity: 0.2;
        }}
        .content {{
            padding: 30px 20px;
            line-height: 1.6;
            text-align: center;
        }}
        .content h2 {{
            color: #b19cd9;
            font-size: 24px;
        }}
        .content p {{
            color: #cccccc;
        }}
        .invite-code {{
            display: inline-block;
            padding: 15px 25px;
            margin: 20px 0;
            font-size: 22px;
            font-weight: bold;
            color: #b19cd9;
            background-color: #1a1a1a;
            border: 2px solid #8a2be2;
            border-radius: 10px;
            letter-spacing: 2px;
            text-align: center;
            user-select: all;
            word-break: break-word;
        }}
        .cta-button {{
            display: inline-block;
            padding: 15px 30px;
            margin: 25px 0;
            background-color: #8a2be2;
            color: #ffffff !important;
            text-decoration: none;
            font-weight: bold;
            font-size: 18px;
            border-radius: 8px;
            box-shadow: 0 0 12px #8a2be2;
            transition: all 0.3s ease;
        }}
        .cta-button:hover {{
            background-color: #6a5acd;
            color: #ffffff !important;
            box-shadow: 0 0 18px #6a5acd;
        }}
        .musical-elements {{
            margin: 20px 0;
        }}
        .musical-elements span {{
            font-size: 28px;
            margin: 0 6px;
            opacity: 0.6;
            animation: float 3s ease-in-out infinite alternate;
            display: inline-block;
        }}
        @keyframes float {{
            0% {{ transform: translateY(0px); }}
            100% {{ transform: translateY(-10px); }}
        }}
        .genres {{
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            margin: 20px 0;
        }}
        .genre {{
            background-color: #1a1a1a;
            border: 1px solid #6a5acd;
            border-radius: 8px;
            color: #b19cd9;
            padding: 8px 12px;
            margin: 5px;
            font-weight: bold;
            font-size: 14px;
            box-shadow: 0 0 8px #6a5acd;
        }}
        .footer {{
            background-color: #1a1a1a;
            color: #888888;
            font-size: 12px;
            text-align: center;
            padding: 20px;
        }}
        @media (max-width: 700px) {{
            .container {{ max-width: 100%; }}
            .header h1 {{ font-size: 28px; }}
            .content h2 {{ font-size: 20px; }}
            .invite-code {{ font-size: 20px; padding: 12px 20px; }}
            .cta-button {{ font-size: 16px; padding: 12px 25px; }}
            .musical-elements span {{ font-size: 24px; }}
            .genre {{ font-size: 12px; padding: 6px 10px; }}
        }}
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1>NeoPlaylist Beta</h1>
        </div>
        <div class="content">
          <h2>¡Hola {name}!</h2>
          <p>¡Has sido seleccionado para probar la beta exclusiva de <strong>NeoPlaylist</strong>! 🎶</p>
          <div class="musical-elements">
            <span>🎵</span><span>🎧</span><span>🎷</span><span>🎸</span><span>🎹</span>
            <span>🥁</span><span>🎺</span><span>🎼</span>
          </div>
          <p>Tu código de invitación único es:</p>
          <div class="invite-code">{invite_code}</div>
          <p>Activa tu cuenta haciendo clic en el botón e ingresa tu código:</p>
          <a href="http://192.168.100.169:5173" class="cta-button">Ir al sitio</a>
          <div class="genres">
            <div class="genre">Rock 🎸</div>
            <div class="genre">Pop 🎤</div>
            <div class="genre">Jazz 🎷</div>
            <div class="genre">Clásica 🎼</div>
            <div class="genre">Electrónica 🎹</div>
            <div class="genre">Hip-Hop 🎧</div>
            <div class="genre">Reggae 🪘</div>
            <div class="genre">Latina 🥁</div>
          </div>
          <p>Explora, crea y comparte playlists automáticas con IA. ¡Tu feedback ayudará a que NeoPlaylist sea más creativo y diverso!</p>
          <p>— El equipo <strong>NeoPlaylist</strong></p>
        </div>
        <div class="footer">
          Si no solicitaste esta invitación, ignora este correo. <br>
          NeoPlaylist &copy; 2025
        </div>
      </div>
    </body>
    </html>
    """


    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": BREVO_API_KEY,
        "Content-Type": "application/json"
    }
    payload_data = {
        "sender": {"name": "NeoPlaylist", "email": "contacto@agitech.cl"},
        "to": [{"email": to_email, "name": name}],
        "subject": subject,
        "htmlContent": html_content
    }

    response = requests.post(url, json=payload_data, headers=headers)
    if response.status_code in [200, 201]:
        print(f"鉁?Invitaci贸n enviada correctamente a {to_email}")
    else:
        print(f"鉂?Error al enviar correo: {response.status_code} - {response.text}")


# ============================================================
# 馃敼 ENDPOINT: USUARIOS ONLINE
# ============================================================
@router.get("/users/online")
def list_online_users():
    users = list(users_col.find({"stats.online": True}, {"email": 1, "name": 1, "username": 1, "_id": 0}))
    return {"online_users": users}

# 馃敼 Endpoint: Setup inicial del administrador (solo si no existen usuarios)
@router.post("/admin/setup")
def setup_admin(payload: dict):
    email = payload.get("email")
    name = payload.get("name")
    username = payload.get("username")
    password = payload.get("password")

    if not all([email, name, username, password]):
        raise HTTPException(status_code=400, detail="Faltan datos")

    existing_admin = users_col.find_one({"role": "admin"})
    if existing_admin:
        raise HTTPException(status_code=400, detail="Ya existe un administrador registrado")

    hashed_pw = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    admin_user = {
        "email": email,
        "name": name,
        "username": username,
        "password": hashed_pw,
        "bio": "Administrador del sistema",
        "role": "admin",
        "is_active": True,
        "is_registered": True,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
        "preferences": {"language": "es", "dark_mode": True},
        "stats": {"playlists_generated": 0, "feedbacks_given": 0, "online": False}
    }

    users_col.insert_one(admin_user)
    return {"status": "ok", "message": f"Administrador {name} creado correctamente."}
