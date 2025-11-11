# backend/models/user.py
from pydantic import BaseModel, EmailStr
from typing import Optional

class User(BaseModel):
    id: Optional[str] = None
    username: str
    email: EmailStr
    password: str
    created_at: Optional[str] = None
