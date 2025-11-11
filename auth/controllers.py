# backend/auth/controllers.py
from fastapi import HTTPException
from database.connection import auth_db
from .utils import (
    hash_password, verify_password, create_access_token, decode_access_token,
    generate_invite_code, send_invite_email
)
from .models import UserRegister, UserLogin
from datetime import datetime, timedelta
from bson import ObjectId
import os
import logging

ADMIN_KEY = os.getenv("ADMIN_KEY", "admin123")
SESSION_TTL_HOURS = int(os.getenv("SESSION_TTL_HOURS", 8))

# =====================================================
# 🔹 Verificar invitación
# =====================================================
def check_invite(email: str):
    invite = auth_db.invites.find_one({"email": email})
    user = auth_db.users.find_one({"email": email})
    if user:
        return {"exists": True, "message": "El usuario ya está registrado."}
    if invite:
        return {"invited": True, "message": "El correo tiene una invitación activa."}
    raise HTTPException(status_code=404, detail="No se encontró invitación para este correo.")

# =====================================================
# 🔹 Registrar usuario (signup)
# =====================================================
def register_user(data: UserRegister):
    if auth_db.users.find_one({"email": data.email}):
        raise HTTPException(status_code=400, detail="El email ya está registrado.")

    invite = auth_db.invites.find_one({"email": data.email})
    if not invite:
        raise HTTPException(status_code=403, detail="El usuario no tiene una invitación activa.")

    created_at = datetime.utcnow().isoformat()
    user_doc = {
        "username": data.username,
        "email": data.email,
        "password": hash_password(data.password),
        "created_at": created_at,
        "status": "offline",
        "token": None,
    }
    result = auth_db.users.insert_one(user_doc)
    auth_db.invites.delete_one({"email": data.email})
    user_id = str(result.inserted_id)

    return {
        "message": "Usuario registrado con éxito.",
        "id": user_id,
        "username": data.username,
        "email": data.email,
        "created_at": created_at,
    }

# =====================================================
# 🔹 Login con password
# =====================================================
def login_with_password(data: UserLogin):
    user = auth_db.users.find_one({"email": data.email})
    if not user or not verify_password(data.password, user["password"]):
        raise HTTPException(status_code=401, detail="Credenciales inválidas.")

    # Generar token persistente
    token = create_access_token(
        {"email": data.email}, expires_minutes=SESSION_TTL_HOURS * 60
    )
    auth_db.users.update_one(
        {"email": data.email},
        {"$set": {"token": token, "status": "online", "last_login": datetime.utcnow().isoformat()}},
    )
    return {"token": token, "expires_in": SESSION_TTL_HOURS * 3600}

# =====================================================
# 🔹 Validar token
# =====================================================
def validate_token(token: str):
    decoded = decode_access_token(token)
    if not decoded:
        raise HTTPException(status_code=401, detail="Token inválido o expirado.")
    email = decoded.get("email")
    user = auth_db.users.find_one({"email": email})
    if not user or user.get("token") != token:
        raise HTTPException(status_code=403, detail="Token no coincide o usuario desconectado.")
    return {"valid": True, "email": email}

# =====================================================
# 🔹 Logout
# =====================================================
def logout_user(email: str):
    result = auth_db.users.update_one(
        {"email": email},
        {"$set": {"status": "offline", "token": None}},
    )
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Usuario no encontrado.")
    return {"message": f"Usuario {email} desconectado correctamente."}

# =====================================================
# 🔹 Generar invitación
# =====================================================
def create_invite(payload: dict):
    admin_key = payload.get("admin_key")
    email = payload.get("email")
    name = payload.get("name", "")

    if admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Clave de administrador inválida.")

    if auth_db.users.find_one({"email": email}):
        raise HTTPException(status_code=400, detail="El usuario ya está registrado.")

    code = generate_invite_code()
    invite = {"email": email, "code": code, "created_at": datetime.utcnow().isoformat()}
    auth_db.invites.insert_one(invite)

    send_invite_email(email, name, code)
    return {"message": "Invitación enviada correctamente.", "email": email, "code": code}

# =====================================================
# 🔹 Listar usuarios online
# =====================================================
def list_online_users():
    users = list(auth_db.users.find({"status": "online"}, {"_id": 0, "password": 0}))
    return users

# =====================================================
# 🔹 Configurar admin inicial
# =====================================================
def setup_admin(payload: dict):
    if auth_db.users.find_one({"role": "admin"}):
        return {"message": "Ya existe un administrador configurado."}

    admin_key = payload.get("admin_key")
    if admin_key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Clave de administrador inválida.")

    username = payload.get("username", "Admin")
    email = payload.get("email")
    password = hash_password(payload.get("password", "admin123"))

    user_doc = {
        "username": username,
        "email": email,
        "password": password,
        "role": "admin",
        "status": "offline",
        "created_at": datetime.utcnow().isoformat(),
    }
    auth_db.users.insert_one(user_doc)
    return {"message": "Administrador inicial creado correctamente."}
