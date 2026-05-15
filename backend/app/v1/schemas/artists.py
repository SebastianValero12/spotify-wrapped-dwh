"""
filename: artists.py
author: [nombre]
date: 2026-05-14
version: 1.0
description: Schemas Pydantic para artistas del DWH.
"""

from datetime import datetime
from pydantic import BaseModel, ConfigDict


class ArtistBase(BaseModel):
    spotify_id: str
    name: str
    popularity: int | None = 0
    followers_count: int | None = 0
    genres: list[str] = []


class ArtistRequest(ArtistBase):
    """Payload de entrada."""
    pass


class ArtistResponse(ArtistBase):
    """Payload de salida con campos generados por la DB."""
    artist_id: int
    loaded_at: datetime

    model_config = ConfigDict(from_attributes=True)
