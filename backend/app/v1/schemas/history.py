"""
filename: history.py
author: [nombre]
date: 2026-05-14
version: 1.0
description: Schemas Pydantic para el historial de reproducciones y
             sub-endpoints analíticos (peak-hour, genres).
"""

from datetime import datetime
from pydantic import BaseModel, ConfigDict


class HistoryBase(BaseModel):
    played_at: datetime
    hour_of_day: int | None = None
    day_of_week: str | None = None
    context_type: str | None = None


class HistoryRequest(HistoryBase):
    """Payload de entrada."""
    pass


class HistoryResponse(HistoryBase):
    """Payload de salida con FKs resueltas."""
    id: int
    user_id: int
    track_id: int
    artist_id: int

    model_config = ConfigDict(from_attributes=True)


# ── Schemas para sub-endpoints analíticos ──────────────────────

class PeakHourResponse(BaseModel):
    """Hora del día con más reproducciones."""
    hour_of_day: int
    play_count: int


class GenreCountResponse(BaseModel):
    """Conteo de artistas por género."""
    genre: str
    artist_count: int
