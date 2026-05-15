"""
filename: dependencies.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Dependencias compartidas para los routers de v1.
             Contiene get_current_user que valida el JWT de la app
             y retorna el spotify_id del usuario autenticado.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

from backend.app.core.config import settings

bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> str:
    """
    Valida el JWT de la app y retorna el spotify_id del usuario autenticado.

    Args:
        credentials (HTTPAuthorizationCredentials): Token Bearer del header Authorization.

    Returns:
        str: spotify_id del usuario autenticado.

    Raises:
        HTTPException: 401 si el token es inválido, expirado o no contiene sub.
    """
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.SECRET_KEY,
            algorithms=["HS256"],
        )
        spotify_id: str = payload.get("sub")
        if spotify_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido: no contiene sub",
            )
        return spotify_id
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
        )
