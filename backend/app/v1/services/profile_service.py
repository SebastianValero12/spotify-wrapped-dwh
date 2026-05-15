"""
filename: profile_service.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Servicio para obtener el perfil del usuario autenticado
             desde dwh.dim_users.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_user_profile(spotify_id: str, db: Session) -> dict | None:
    """
    Consulta el perfil del usuario en dim_users por spotify_id.

    Args:
        spotify_id (str): ID de Spotify del usuario autenticado.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        dict | None: Datos del perfil o None si no existe.
    """
    result = db.execute(
        text("""
            SELECT user_id, spotify_id, display_name, email, country,
                   followers, product, loaded_at
            FROM dwh.dim_users
            WHERE spotify_id = :spotify_id
        """),
        {"spotify_id": spotify_id},
    ).fetchone()

    if result is None:
        return None

    return {
        "user_id": result[0],
        "spotify_id": result[1],
        "display_name": result[2],
        "email": result[3],
        "country": result[4],
        "followers": result[5],
        "product": result[6],
        "loaded_at": result[7],
    }
