"""
filename: profile.py
author: [nombre]
date: 2026-05-14
version: 1.0
description: Schemas Pydantic para el perfil del usuario autenticado.
"""

from datetime import datetime
from pydantic import BaseModel, ConfigDict


class UserProfileBase(BaseModel):
    spotify_id: str
    display_name: str | None = None
    email: str | None = None
    country: str | None = None
    followers: int | None = 0
    product: str | None = None


class UserProfileRequest(UserProfileBase):
    """Payload de entrada."""
    pass


class UserProfileResponse(UserProfileBase):
    """Payload de salida con campos generados por la DB."""
    user_id: int
    loaded_at: datetime

    model_config = ConfigDict(from_attributes=True)
