# backend/routes/user_routes.py
from fastapi import APIRouter, HTTPException
from models.user import User
from repositories.user_repository import (
    create_user,
    get_user_by_email,
    get_all_users,
    delete_user_by_id
)
import logging

router = APIRouter()

# ------------------------------------------------------------
# 🔹 Registrar usuario
# ------------------------------------------------------------
@router.post("/", summary="Registrar nuevo usuario")
def register_user(user: User):
    logging.info(f"🧩 Intentando registrar usuario: {user.email}")
    
    existing_user = get_user_by_email(user.email)
    if existing_user:
        logging.warning(f"⚠️ Usuario ya existe: {user.email}")
        raise HTTPException(status_code=400, detail="El usuario ya existe.")

    user_id = create_user(user)
    logging.info(f"✅ Usuario creado con ID: {user_id}")

    # ✅ Respuesta compatible con frontend original
    return {
        "message": "Usuario creado correctamente",
        "id": str(user_id),
        "username": user.username,
        "email": user.email,
        "created_at": None  # se puede poblar si quieres obtener del repo
    }

# ------------------------------------------------------------
# 🔹 Listar usuarios
# ------------------------------------------------------------
@router.get("/", summary="Obtener lista de usuarios")
def list_users():
    users = get_all_users()
    if not users:
        logging.warning("⚠️ No se encontraron usuarios registrados.")
    return users

# ------------------------------------------------------------
# 🔹 Eliminar usuario
# ------------------------------------------------------------
@router.delete("/{user_id}", summary="Eliminar usuario por ID")
def remove_user(user_id: str):
    success = delete_user_by_id(user_id)
    if not success:
        logging.warning(f"⚠️ Usuario no encontrado para eliminación: {user_id}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    logging.info(f"✅ Usuario eliminado: {user_id}")
    return {"message": "Usuario eliminado correctamente"}
