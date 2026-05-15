"""
filename: tracks.py
author: [nombre]
date: 2026-05-14
version: 1.0
description: Schemas Pydantic para tracks del DWH.
"""

from datetime import datetime
from pydantic import BaseModel, ConfigDict


class TrackBase(BaseModel):
    spotify_id: str
    name: str
    album_name: str | None = None
    duration_ms: int | None = 0
    popularity: int | None = 0
    explicit: bool | None = False


class TrackRequest(TrackBase):
    """Payload de entrada."""
    pass


class TrackResponse(TrackBase):
    """Payload de salida con campos generados por la DB."""
    track_id: int
    artist_id: int | None = None
    loaded_at: datetime

    model_config = ConfigDict(from_attributes=True)
