"""
filename: history.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Router para el historial de reproducciones y sub-endpoints
             analíticos (peak-hour, genres). Todos protegidos con JWT.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.app.core.database import get_db
from backend.app.v1.dependencies import get_current_user
from backend.app.v1.services import history_service
from backend.app.v1.schemas.history import (
    HistoryResponse,
    PeakHourResponse,
    GenreCountResponse,
)

router = APIRouter(prefix="/history", tags=["History"])


@router.get("/recently-played")
def get_recently_played(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna las reproducciones recientes del usuario desde el DWH.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        list[dict]: Reproducciones con datos de track y artista.
    """
    return history_service.get_recently_played(spotify_id, db)


@router.get("/peak-hour", response_model=PeakHourResponse)
def get_peak_hour(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna la hora del día con más reproducciones del usuario.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        PeakHourResponse: Hora pico con conteo de reproducciones.
    """
    result = history_service.get_peak_hour(spotify_id, db)

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay datos de reproducciones. Ejecuta el ETL primero.",
        )

    return result


@router.get("/genres", response_model=list[GenreCountResponse])
def get_top_genres(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna los géneros dominantes usando UNNEST sobre dim_artists.genres.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        list[GenreCountResponse]: Géneros con conteo de artistas.
    """
    return history_service.get_top_genres(spotify_id, db)
