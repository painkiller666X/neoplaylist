# backend/auth/routes.py
from fastapi import APIRouter, HTTPException, Query
from .models import UserRegister, UserLogin
from .controllers import (
    register_user, login_with_password, validate_token, logout_user,
    create_invite, list_online_users, setup_admin, check_invite
)

router = APIRouter()

# ------------------------------------------------------------
# 🔹 Verificar invitación
# ------------------------------------------------------------
@router.get("/check-invite")
def check_invite_route(email: str = Query(..., description="Email a verificar")):
    return check_invite(email)

# ------------------------------------------------------------
# 🔹 Signup
# ------------------------------------------------------------
@router.post("/signup")
def signup(data: UserRegister):
    return register_user(data)

# ------------------------------------------------------------
# 🔹 Login
# ------------------------------------------------------------
@router.post("/login-password")
def login(data: UserLogin):
    return login_with_password(data)

# ------------------------------------------------------------
# 🔹 Validar token
# ------------------------------------------------------------
@router.post("/validate-token")
def validate(token: str = Query(..., description="Token JWT a validar")):
    return validate_token(token)

# ------------------------------------------------------------
# 🔹 Logout
# ------------------------------------------------------------
@router.post("/logout")
def logout(email: str = Query(...)):
    return logout_user(email)

# ------------------------------------------------------------
# 🔹 Generar invitación (solo admin)
# ------------------------------------------------------------
@router.post("/invite")
def invite(payload: dict):
    return create_invite(payload)

# ------------------------------------------------------------
# 🔹 Listar usuarios online
# ------------------------------------------------------------
@router.get("/users/online")
def online_users():
    return list_online_users()

# ------------------------------------------------------------
# 🔹 Crear admin inicial
# ------------------------------------------------------------
@router.post("/admin/setup")
def admin_setup(payload: dict):
    return setup_admin(payload)
