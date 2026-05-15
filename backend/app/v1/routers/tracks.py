"""
filename: tracks.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Router para los top tracks del usuario.
             Endpoint protegido con JWT.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.app.core.database import get_db
from backend.app.v1.dependencies import get_current_user
from backend.app.v1.services import tracks_service
from backend.app.v1.schemas.tracks import TrackResponse

router = APIRouter(prefix="/tracks", tags=["Tracks"])


@router.get("/top", response_model=list[TrackResponse])
def get_top_tracks(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna los top tracks desde dim_tracks ordenados por popularidad.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        list[TrackResponse]: Lista de tracks del DWH.
    """
    return tracks_service.get_top_tracks(db)
