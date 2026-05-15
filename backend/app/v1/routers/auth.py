"""
filename: auth.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Router de autenticación. Implementa el flujo OAuth PKCE completo
             con Spotify: login (redirect) y callback (intercambio + JWT).
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from backend.app.core.database import get_db
from backend.app.v1.services import auth_service

router = APIRouter(prefix="/auth", tags=["Auth"])


@router.get("/login")
def login(db: Session = Depends(get_db)):
    """
    Inicia el flujo OAuth PKCE con Spotify.

    Genera code_verifier, code_challenge y state. Guarda el estado
    en pkce_sessions y redirige al usuario a Spotify para autorizar.

    Returns:
        RedirectResponse: Redirect 302 a accounts.spotify.com/authorize.
    """
    auth_url = auth_service.build_spotify_auth_url(db)
    return RedirectResponse(url=auth_url, status_code=status.HTTP_302_FOUND)


@router.get("/callback")
async def callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Callback de Spotify después de que el usuario autoriza la app.

    Recibe el authorization code y el state, intercambia por tokens,
    hace upsert en dim_users y emite un JWT de la app.
    Redirige al frontend con el JWT como query parameter.

    Args:
        code (str | None): Authorization code de Spotify.
        state (str | None): State para validar contra pkce_sessions.
        error (str | None): Error si el usuario rechazó la autorización.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        RedirectResponse: Redirect 302 a FRONTEND_URL/callback?token=<jwt>.
    """
    # Si el usuario rechazó la autorización en Spotify
    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Spotify authorization error: {error}",
        )

    if not code or not state:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Faltan parámetros code o state",
        )

    try:
        app_token = await auth_service.handle_callback(code, state, db)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )

    # Redirigir al frontend con el JWT
    from backend.app.core.config import settings

    redirect_url = f"{settings.FRONTEND_URL}/callback?token={app_token}"
    return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)
