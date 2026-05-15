"""
filename: artists_service.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Servicio para obtener los top artistas del usuario
             desde dwh.dim_artists.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_top_artists(db: Session, limit: int = 50) -> list[dict]:
    """
    Consulta los artistas en dim_artists ordenados por popularidad.

    Args:
        db (Session): Sesión de SQLAlchemy.
        limit (int): Cantidad máxima de artistas a retornar.

    Returns:
        list[dict]: Lista de artistas con sus datos del DWH.
    """
    result = db.execute(
        text("""
            SELECT artist_id, spotify_id, name, popularity,
                   followers_count, genres, loaded_at
            FROM dwh.dim_artists
            ORDER BY popularity DESC
            LIMIT :limit
        """),
        {"limit": limit},
    ).fetchall()

    return [
        {
            "artist_id": row[0],
            "spotify_id": row[1],
            "name": row[2],
            "popularity": row[3],
            "followers_count": row[4],
            "genres": row[5] if row[5] else [],
            "loaded_at": row[6],
        }
        for row in result
    ]
