"""
filename: artists.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Router para los top artistas del usuario.
             Endpoint protegido con JWT.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.app.core.database import get_db
from backend.app.v1.dependencies import get_current_user
from backend.app.v1.services import artists_service
from backend.app.v1.schemas.artists import ArtistResponse

router = APIRouter(prefix="/artists", tags=["Artists"])


@router.get("/top", response_model=list[ArtistResponse])
def get_top_artists(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna los top artistas desde dim_artists ordenados por popularidad.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        list[ArtistResponse]: Lista de artistas del DWH.
    """
    return artists_service.get_top_artists(db)
