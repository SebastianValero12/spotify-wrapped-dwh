"""
filename: tracks_service.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Servicio para obtener los top tracks del usuario
             desde dwh.dim_tracks.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def get_top_tracks(db: Session, limit: int = 50) -> list[dict]:
    """
    Consulta los tracks en dim_tracks ordenados por popularidad.

    Args:
        db (Session): Sesión de SQLAlchemy.
        limit (int): Cantidad máxima de tracks a retornar.

    Returns:
        list[dict]: Lista de tracks con sus datos del DWH.
    """
    result = db.execute(
        text("""
            SELECT t.track_id, t.spotify_id, t.name, t.artist_id,
                   t.album_name, t.duration_ms, t.popularity,
                   t.explicit, t.loaded_at,
                   a.name AS artist_name
            FROM dwh.dim_tracks t
            LEFT JOIN dwh.dim_artists a ON a.artist_id = t.artist_id
            ORDER BY t.popularity DESC
            LIMIT :limit
        """),
        {"limit": limit},
    ).fetchall()

    return [
        {
            "track_id": row[0],
            "spotify_id": row[1],
            "name": row[2],
            "artist_id": row[3],
            "album_name": row[4],
            "duration_ms": row[5],
            "popularity": row[6],
            "explicit": row[7],
            "loaded_at": row[8],
            "artist_name": row[9],
        }
        for row in result
    ]
