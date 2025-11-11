# backend/repositories/user_repository.py
from database.connection import get_auth_db
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime
from typing import List, Optional
import logging

db = get_auth_db()
USERS_COLLECTION = db["users"]

# ------------------------------------------------------------
# 🔹 Serialización segura de usuario
# ------------------------------------------------------------
def serialize_user(user: dict) -> Optional[dict]:
    """Convierte ObjectId a str y limpia campos no serializables."""
    if not user:
        return None
    user_copy = dict(user)
    user_copy["id"] = str(user_copy["_id"])
    user_copy.pop("_id", None)
    user_copy.pop("password", None)  # nunca exponer password
    return user_copy

# ------------------------------------------------------------
# 🔹 Crear usuario
# ------------------------------------------------------------
def create_user(user) -> str:
    user_dict = user.dict()
    user_dict["created_at"] = datetime.utcnow().isoformat()
    result = USERS_COLLECTION.insert_one(user_dict)
    logging.info(f"✅ Usuario creado con ID {result.inserted_id}")
    return str(result.inserted_id)

# ------------------------------------------------------------
# 🔹 Obtener usuario por email
# ------------------------------------------------------------
def get_user_by_email(email: str) -> Optional[dict]:
    user = USERS_COLLECTION.find_one({"email": email})
    return serialize_user(user)

# ------------------------------------------------------------
# 🔹 Listar todos los usuarios
# ------------------------------------------------------------
def get_all_users() -> List[dict]:
    users = list(USERS_COLLECTION.find())
    return [serialize_user(user) for user in users]

# ------------------------------------------------------------
# 🔹 Eliminar usuario por ID
# ------------------------------------------------------------
def delete_user_by_id(user_id: str) -> bool:
    """Elimina un usuario por su ID. Devuelve True si se eliminó correctamente."""
    try:
        obj_id = ObjectId(user_id)
    except InvalidId:
        logging.warning(f"ID inválido para eliminar usuario: {user_id}")
        return False

    result = USERS_COLLECTION.delete_one({"_id": obj_id})
    if result.deleted_count > 0:
        logging.info(f"✅ Usuario eliminado con ID {user_id}")
        return True
    logging.warning(f"⚠️ Usuario no encontrado para eliminar: {user_id}")
    return False
