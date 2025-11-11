# backend/repositories/feedback_repository.py
from datetime import datetime
import logging
from database.connection import music_db
from bson import ObjectId
from bson.errors import InvalidId
from typing import Optional, List

LOG = logging.getLogger("repositories.feedback")

COLL = music_db["playlist_feedback"]

def insert_feedback(feedback_doc: dict) -> str:
    """
    Inserta un documento de feedback y retorna el id string.
    feedback_doc ejemplo:
    {
        "user_email": "user@example.com",
        "playlist_id": "....",
        "playlist_tracks": ["id1","id2",...],    # optional
        "feedback": {"track_id": "id", "action":"like" / "skip" / "dislike" / ... } o lista
        "meta": {...}
    }
    """
    feedback_doc = dict(feedback_doc)
    feedback_doc.setdefault("created_at", datetime.utcnow().isoformat())
    res = COLL.insert_one(feedback_doc)
    LOG.info("Inserted feedback %s for user %s", str(res.inserted_id), feedback_doc.get("user_email"))
    return str(res.inserted_id)

def get_feedback_by_user(email: str) -> List[dict]:
    rows = list(COLL.find({"user_email": email}))
    for r in rows:
        r["id"] = str(r["_id"])
        r.pop("_id", None)
    return rows

def delete_feedback_by_id(fid: str) -> bool:
    try:
        oid = ObjectId(fid)
    except InvalidId:
        return False
    res = COLL.delete_one({"_id": oid})
    return res.deleted_count > 0

def get_feedback_by_playlist(playlist_id: str) -> List[dict]:
    rows = list(COLL.find({"playlist_id": playlist_id}))
    for r in rows:
        r["id"] = str(r["_id"])
        r.pop("_id", None)
    return rows
