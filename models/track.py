# backend/models/track.py
from pydantic import BaseModel
from typing import Optional

class Track(BaseModel):
    id: Optional[str] = None
    title: str
    artist: str
    album: Optional[str] = None
    duration: Optional[int] = None  # duración en segundos
    created_at: Optional[str] = None
