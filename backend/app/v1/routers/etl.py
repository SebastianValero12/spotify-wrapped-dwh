"""
filename: etl.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Router para ejecución y monitoreo del pipeline ETL.
             POST /v1/etl/run ejecuta el pipeline completo.
             GET /v1/etl/status muestra el estado del DWH y últimas ejecuciones.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.core.database import get_db
from backend.app.core import spotify_client
from backend.app.v1.dependencies import get_current_user
from backend.app.v1.services import etl_service
from backend.app.v1.schemas.etl import EtlRunResult, EtlStatus

router = APIRouter(prefix="/etl", tags=["ETL"])


async def _get_valid_token(spotify_id: str, db: Session) -> str:
    """
    Obtiene el access_token de Spotify del usuario. Si está por expirar
    (menos de 5 minutos), lo renueva con el refresh_token.

    Args:
        spotify_id (str): ID de Spotify del usuario.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        str: Access token válido de Spotify.

    Raises:
        HTTPException: 404 si el usuario no tiene tokens almacenados.
    """
    row = db.execute(
        text("""
            SELECT spotify_access_token, spotify_refresh_token, token_expires_at
            FROM dwh.dim_users
            WHERE spotify_id = :spotify_id
        """),
        {"spotify_id": spotify_id},
    ).fetchone()

    if not row or not row[0]:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No se encontraron tokens de Spotify. Vuelve a iniciar sesión.",
        )

    access_token, refresh_token, token_expires_at = row

    # Renovar si expira en menos de 5 minutos
    if token_expires_at and token_expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc) + timedelta(minutes=5):
        token_data = await spotify_client.refresh_access_token(refresh_token)
        access_token = token_data["access_token"]
        expires_in = token_data["expires_in"]
        new_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        db.execute(
            text("""
                UPDATE dwh.dim_users SET
                    spotify_access_token = :access_token,
                    token_expires_at = :token_expires_at
                WHERE spotify_id = :spotify_id
            """),
            {
                "access_token": access_token,
                "token_expires_at": new_expires_at,
                "spotify_id": spotify_id,
            },
        )
        db.commit()

    return access_token


@router.post("/run", response_model=EtlRunResult)
async def run_etl(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Ejecuta el pipeline ETL completo: extract, transform, load
    para las 4 entidades (users, artists, tracks, history).

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        EtlRunResult: Resultado con pasos, métricas y auditoría.
    """
    token = await _get_valid_token(spotify_id, db)
    result = await etl_service.run_etl_pipeline(token, spotify_id, db)
    return result


@router.get("/status", response_model=EtlStatus)
def get_etl_status(
    spotify_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Retorna el estado actual del DWH: conteo de registros por tabla
    y las últimas 10 ejecuciones del ETL.

    Args:
        spotify_id (str): Extraído del JWT por get_current_user.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        EtlStatus: Estado de tablas y historial de ejecuciones.
    """
    # ── Estado de las tablas ───────────────────────────────────
    tables_info = []
    table_queries = [
        ("dim_users", "dwh.dim_users", "loaded_at"),
        ("dim_artists", "dwh.dim_artists", "loaded_at"),
        ("dim_tracks", "dwh.dim_tracks", "loaded_at"),
        ("fact_listening_history", "dwh.fact_listening_history", "played_at"),
    ]

    for name, full_name, date_col in table_queries:
        row = db.execute(
            text(f"SELECT COUNT(*), MAX({date_col}) FROM {full_name}")
        ).fetchone()

        count = row[0] if row else 0
        last_loaded = row[1] if row else None

        if count == 0:
            table_status = "empty"
        elif last_loaded and (datetime.now(timezone.utc) - last_loaded.replace(tzinfo=timezone.utc)).total_seconds() > 86400:
            table_status = "stale"
        else:
            table_status = "loaded"

        tables_info.append({
            "name": name,
            "record_count": count,
            "last_loaded_at": str(last_loaded) if last_loaded else None,
            "status": table_status,
        })

    # ── Últimas ejecuciones ────────────────────────────────────
    runs = db.execute(
        text("""
            SELECT audit_id, started_at, duration_ms, status,
                   history_new, artists_new, tracks_new, error_message
            FROM dwh.etl_audit
            WHERE spotify_user_id = :spotify_id
            ORDER BY started_at DESC
            LIMIT 10
        """),
        {"spotify_id": spotify_id},
    ).fetchall()

    last_runs = [
        {
            "audit_id": r[0],
            "started_at": r[1],
            "duration_ms": r[2],
            "status": r[3],
            "history_new": r[4] or 0,
            "artists_new": r[5] or 0,
            "tracks_new": r[6] or 0,
            "error_message": r[7],
        }
        for r in runs
    ]

    return {"tables": tables_info, "last_runs": last_runs}
