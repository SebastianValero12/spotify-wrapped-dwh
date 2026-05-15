"""
filename: profile.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Router para el perfil del usuario autenticado.
             Endpoint protegido con JWT.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.app.core.database import get_db
from backend.app.v1.dependencies import get_current_user
from backend.app.v1.services import profile_service
from backend.app.v1.schemas.profile import UserProfileResponse

router = APIRouter(prefix="/profile", tags=["Profile"])


@router.get("/me", response_model=UserProfileResponse)
def get_my_profile(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna el perfil del usuario autenticado desde dim_users.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        UserProfileResponse: Datos del perfil del usuario.
    """
    profile = profile_service.get_user_profile(spotify_id, db)

    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado en el DWH",
        )

    return profile
