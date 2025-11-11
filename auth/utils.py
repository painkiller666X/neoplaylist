# backend/auth/utils.py
import os
import jwt
import bcrypt
import requests
import secrets
from datetime import datetime, timedelta
from config import settings

SECRET_KEY = os.getenv("SECRET_KEY", "supersecret")
ALGORITHM = "HS256"
BREVO_API_KEY = os.getenv("BREVO_API_KEY")

# =====================================================
# 🔹 Hashing y Tokens
# =====================================================
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))

def create_access_token(data: dict, expires_minutes: int = 60) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=expires_minutes)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        return None

# =====================================================
# 🔹 Invitaciones
# =====================================================
def generate_invite_code() -> str:
    return secrets.token_hex(4).upper()

def send_invite_email(to_email: str, name: str, invite_code: str):
    """Envía correo de invitación utilizando la API de Brevo."""
    if not BREVO_API_KEY:
        print(f"⚠️ No se configuró BREVO_API_KEY. Código: {invite_code}")
        return
    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": BREVO_API_KEY,
        "content-type": "application/json"
    }
    payload = {
        "sender": {"name": "NeoPlaylist", "email": "no-reply@agitech.cl"},
        "to": [{"email": to_email, "name": name}],
        "subject": "Invitación a NeoPlaylist 🎧",
        "htmlContent": f"""
            <h2>Hola {name or ''},</h2>
            <p>Has sido invitado a unirte a <strong>NeoPlaylist</strong>.</p>
            <p>Tu código de invitación es: <b>{invite_code}</b></p>
            <p>Ingresa al portal para completar tu registro.</p>
        """
    }
    requests.post(url, headers=headers, json=payload)
