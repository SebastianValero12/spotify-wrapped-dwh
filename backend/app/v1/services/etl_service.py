"""
filename: etl_service.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Servicio ETL que orquesta extract, transform y load desde
             Spotify hacia el DWH en PostgreSQL. Contiene las 3 fases
             separadas con funciones individuales por entidad, auditoría
             completa y carga incremental con cursor.
"""

import time
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.core import spotify_client


# ═══════════════════════════════════════════════════════════════
# EXTRACT — Solo llama a Spotify, retorna JSON crudo. Sin lógica.
# ═══════════════════════════════════════════════════════════════

async def extract_user(token: str) -> dict:
    """
    Llama al endpoint /v1/me de Spotify y retorna el perfil crudo.

    Args:
        token (str): Access token de Spotify (Bearer).

    Returns:
        dict: Perfil del usuario en formato JSON crudo de Spotify.
    """
    return await spotify_client.get_current_user(token)


async def extract_top_artists(token: str) -> list[dict]:
    """
    Llama al endpoint /v1/me/top/artists de Spotify y retorna la lista cruda.

    Args:
        token (str): Access token de Spotify (Bearer).

    Returns:
        list[dict]: Lista de objetos artista en formato JSON crudo de Spotify.
    """
    return await spotify_client.get_top_artists(token)


async def extract_top_tracks(token: str) -> list[dict]:
    """
    Llama al endpoint /v1/me/top/tracks de Spotify y retorna la lista cruda.

    Args:
        token (str): Access token de Spotify (Bearer).

    Returns:
        list[dict]: Lista de objetos track en formato JSON crudo de Spotify.
    """
    return await spotify_client.get_top_tracks(token)


async def extract_recently_played(token: str, after: int | None = None) -> list[dict]:
    """
    Llama al endpoint /v1/me/player/recently-played de Spotify.

    Args:
        token (str): Access token de Spotify (Bearer).
        after (int | None): Cursor Unix ms. Si se pasa, retorna solo
                            reproducciones después de ese momento.

    Returns:
        list[dict]: Lista de objetos PlayHistoryObject crudos de Spotify.
    """
    data = await spotify_client.get_recently_played(token, after=after)
    return data.get("items", [])


# ═══════════════════════════════════════════════════════════════
# TRANSFORM — Normaliza datos para el modelo dimensional.
# ═══════════════════════════════════════════════════════════════

def transform_user(raw: dict) -> dict:
    """
    Transforma el perfil crudo de Spotify al formato de dim_users.

    Args:
        raw (dict): Perfil crudo de Spotify (/v1/me).

    Returns:
        dict: Datos normalizados para insertar en dim_users.
    """
    return {
        "spotify_id": raw["id"],
        "display_name": raw.get("display_name"),
        "email": raw.get("email"),
        "country": raw.get("country"),
        "followers": raw.get("followers", {}).get("total", 0),
        "product": raw.get("product"),
    }


def transform_artists(raw_list: list[dict]) -> list[dict]:
    """
    Transforma la lista cruda de artistas al formato de dim_artists.

    Args:
        raw_list (list[dict]): Lista cruda de Spotify (/v1/me/top/artists).

    Returns:
        list[dict]: Lista de artistas normalizados para dim_artists.
    """
    return [
        {
            "spotify_id": artist["id"],
            "name": artist["name"],
            "popularity": artist.get("popularity", 0),
            "followers_count": artist.get("followers", {}).get("total", 0),
            "genres": artist.get("genres", []),
        }
        for artist in raw_list
    ]


def transform_tracks(raw_list: list[dict]) -> list[dict]:
    """
    Transforma la lista cruda de tracks al formato de dim_tracks.

    Args:
        raw_list (list[dict]): Lista cruda de Spotify (/v1/me/top/tracks).

    Returns:
        list[dict]: Lista de tracks normalizados para dim_tracks.
    """
    return [
        {
            "spotify_id": track["id"],
            "name": track["name"],
            "artist_spotify_id": track["artists"][0]["id"] if track.get("artists") else None,
            "album_name": track.get("album", {}).get("name"),
            "duration_ms": track.get("duration_ms", 0),
            "popularity": track.get("popularity", 0),
            "explicit": track.get("explicit", False),
        }
        for track in raw_list
    ]


def transform_history(raw_items: list[dict]) -> list[dict]:
    """
    Transforma los items crudos de recently-played al formato de
    fact_listening_history. Parsea timestamps, extrae hour_of_day,
    day_of_week y context_type.

    Args:
        raw_items (list[dict]): Lista de PlayHistoryObject de Spotify.

    Returns:
        list[dict]: Lista de reproducciones normalizadas.
    """
    transformed = []
    for item in raw_items:
        played_at_str = item["played_at"]
        played_at = datetime.fromisoformat(played_at_str.replace("Z", "+00:00"))

        track = item.get("track", {})
        artists = track.get("artists", [])

        transformed.append({
            "track_spotify_id": track.get("id"),
            "artist_spotify_id": artists[0]["id"] if artists else None,
            "played_at": played_at,
            "hour_of_day": played_at.hour,
            "day_of_week": played_at.strftime("%A"),
            "context_type": (item.get("context") or {}).get("type") or "unknown",
        })

    return transformed


# ═══════════════════════════════════════════════════════════════
# LOAD — Inserta en PostgreSQL con idempotencia (ON CONFLICT).
# ═══════════════════════════════════════════════════════════════

def load_user(data: dict, db: Session) -> dict:
    """
    Inserta o actualiza el usuario en dim_users.

    Args:
        data (dict): Datos del usuario transformados.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        dict: Métricas {"users_new": int}.
    """
    result = db.execute(
        text("""
            INSERT INTO dwh.dim_users (spotify_id, display_name, email, country, followers, product)
            VALUES (:spotify_id, :display_name, :email, :country, :followers, :product)
            ON CONFLICT (spotify_id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                followers = EXCLUDED.followers,
                product = EXCLUDED.product
            RETURNING (xmax = 0) AS inserted
        """),
        data,
    )
    row = result.fetchone()
    return {"users_new": 1 if row and row[0] else 0}


def load_artists(data_list: list[dict], db: Session) -> dict:
    """
    Inserta artistas en dim_artists con ON CONFLICT DO NOTHING.

    Args:
        data_list (list[dict]): Lista de artistas transformados.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        dict: Métricas {"artists_new": int, "artists_skipped": int}.
    """
    new = 0
    skipped = 0

    for data in data_list:
        result = db.execute(
            text("""
                INSERT INTO dwh.dim_artists (spotify_id, name, popularity, followers_count, genres)
                VALUES (:spotify_id, :name, :popularity, :followers_count, :genres)
                ON CONFLICT (spotify_id) DO UPDATE SET
                    popularity = EXCLUDED.popularity,
                    followers_count = EXCLUDED.followers_count,
                    genres = EXCLUDED.genres
                WHERE dwh.dim_artists.popularity = 0
                RETURNING artist_id
            """),
            {
                "spotify_id": data["spotify_id"],
                "name": data["name"],
                "popularity": data["popularity"],
                "followers_count": data["followers_count"],
                "genres": data["genres"],
            },
        )
        if result.fetchone():
            new += 1
        else:
            skipped += 1

    return {"artists_new": new, "artists_skipped": skipped}


def load_tracks(data_list: list[dict], db: Session) -> dict:
    """
    Inserta tracks en dim_tracks con ON CONFLICT DO NOTHING.
    Resuelve la FK artist_id buscando en dim_artists por spotify_id.

    Args:
        data_list (list[dict]): Lista de tracks transformados.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        dict: Métricas {"tracks_new": int, "tracks_skipped": int}.
    """
    new = 0
    skipped = 0

    for data in data_list:
        # Resolver FK artist_id
        # Resolver FK artist_id — si no existe, insertarlo
        artist_id = None
        if data["artist_spotify_id"]:
            artist_row = db.execute(
                text("SELECT artist_id FROM dwh.dim_artists WHERE spotify_id = :sid"),
                {"sid": data["artist_spotify_id"]},
            ).fetchone()
            if artist_row:
                artist_id = artist_row[0]
            else:
                new_artist = db.execute(
                    text("""
                        INSERT INTO dwh.dim_artists (spotify_id, name, popularity, followers_count, genres)
                        VALUES (:spotify_id, 'Unknown', 0, 0, '{}')
                        ON CONFLICT (spotify_id) DO NOTHING
                        RETURNING artist_id
                    """),
                    {"spotify_id": data["artist_spotify_id"]},
                ).fetchone()
                if new_artist:
                    artist_id = new_artist[0]

        result = db.execute(
            text("""
                INSERT INTO dwh.dim_tracks
                    (spotify_id, name, artist_id, album_name, duration_ms, popularity, explicit)
                VALUES
                    (:spotify_id, :name, :artist_id, :album_name, :duration_ms, :popularity, :explicit)
                ON CONFLICT (spotify_id) DO UPDATE SET
                    popularity = EXCLUDED.popularity,
                    duration_ms = EXCLUDED.duration_ms,
                    album_name = EXCLUDED.album_name
                WHERE dwh.dim_tracks.popularity = 0
                RETURNING track_id
            """),
            {
                "spotify_id": data["spotify_id"],
                "name": data["name"],
                "artist_id": artist_id,
                "album_name": data["album_name"],
                "duration_ms": data["duration_ms"],
                "popularity": data["popularity"],
                "explicit": data["explicit"],
            },
        )
        if result.fetchone():
            new += 1
        else:
            skipped += 1

    return {"tracks_new": new, "tracks_skipped": skipped}


def load_history(data_list: list[dict], spotify_id: str, db: Session) -> dict:
    """
    Inserta reproducciones en fact_listening_history con
    ON CONFLICT (user_id, played_at) DO NOTHING.
    Resuelve FKs user_id, track_id, artist_id.

    Args:
        data_list (list[dict]): Lista de reproducciones transformadas.
        spotify_id (str): ID de Spotify del usuario.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        dict: Métricas {"history_new": int, "history_skipped": int}.
    """
    # Resolver user_id
    user_row = db.execute(
        text("SELECT user_id FROM dwh.dim_users WHERE spotify_id = :sid"),
        {"sid": spotify_id},
    ).fetchone()

    if not user_row:
        return {"history_new": 0, "history_skipped": len(data_list)}

    user_id = user_row[0]
    new = 0
    skipped = 0

    for data in data_list:
        # Resolver track_id
        track_row = db.execute(
            text("SELECT track_id FROM dwh.dim_tracks WHERE spotify_id = :sid"),
            {"sid": data["track_spotify_id"]},
        ).fetchone()

        # Resolver artist_id
        artist_row = db.execute(
            text("SELECT artist_id FROM dwh.dim_artists WHERE spotify_id = :sid"),
            {"sid": data["artist_spotify_id"]},
        ).fetchone()

        if not track_row or not artist_row:
            skipped += 1
            continue

        result = db.execute(
            text("""
                INSERT INTO dwh.fact_listening_history
                    (user_id, track_id, artist_id, played_at, hour_of_day, day_of_week, context_type)
                VALUES
                    (:user_id, :track_id, :artist_id, :played_at, :hour_of_day, :day_of_week, :context_type)
                ON CONFLICT (user_id, played_at) DO NOTHING
                RETURNING id
            """),
            {
                "user_id": user_id,
                "track_id": track_row[0],
                "artist_id": artist_row[0],
                "played_at": data["played_at"],
                "hour_of_day": data["hour_of_day"],
                "day_of_week": data["day_of_week"],
                "context_type": data["context_type"],
            },
        )
        if result.fetchone():
            new += 1
        else:
            skipped += 1

    return {"history_new": new, "history_skipped": skipped}


# ═══════════════════════════════════════════════════════════════
# AUDIT — Registro de cada ejecución del ETL.
# ═══════════════════════════════════════════════════════════════

def insert_audit_start(spotify_user_id: str, db: Session) -> int:
    """
    Inserta una fila de auditoría al inicio de la ejecución del ETL.

    Args:
        spotify_user_id (str): ID de Spotify del usuario.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        int: audit_id de la fila insertada.
    """
    result = db.execute(
        text("""
            INSERT INTO dwh.etl_audit (spotify_user_id, started_at, status)
            VALUES (:spotify_user_id, :started_at, 'running')
            RETURNING audit_id
        """),
        {
            "spotify_user_id": spotify_user_id,
            "started_at": datetime.now(timezone.utc),
        },
    )
    db.commit()
    return result.fetchone()[0]


def get_last_cursor(spotify_user_id: str, db: Session) -> int | None:
    """
    Obtiene el cursor_next_ms de la última ejecución exitosa del ETL.

    Args:
        spotify_user_id (str): ID de Spotify del usuario.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        int | None: Cursor Unix ms o None si es la primera ejecución.
    """
    result = db.execute(
        text("""
            SELECT cursor_next_ms
            FROM dwh.etl_audit
            WHERE spotify_user_id = :spotify_user_id AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
        """),
        {"spotify_user_id": spotify_user_id},
    ).fetchone()

    return result[0] if result else None


def played_at_to_unix_ms(played_at: datetime) -> int:
    """
    Convierte un datetime a Unix ms para el cursor de Spotify.

    Args:
        played_at (datetime): Timestamp de reproducción.

    Returns:
        int: Timestamp en milisegundos Unix.
    """
    return int(played_at.timestamp() * 1000)


def update_audit_success(
    audit_id: int,
    duration_ms: int,
    cursor_after_ms: int | None,
    cursor_next_ms: int | None,
    metrics: dict,
    db: Session,
) -> None:
    """
    Actualiza la fila de auditoría al finalizar exitosamente el ETL.

    Args:
        audit_id (int): ID de la fila de auditoría.
        duration_ms (int): Duración total en milisegundos.
        cursor_after_ms (int | None): Cursor usado en esta ejecución.
        cursor_next_ms (int | None): Nuevo cursor para la próxima ejecución.
        metrics (dict): Métricas de registros procesados.
        db (Session): Sesión de SQLAlchemy.
    """
    db.execute(
        text("""
            UPDATE dwh.etl_audit SET
                finished_at = :finished_at,
                duration_ms = :duration_ms,
                status = 'success',
                users_new = :users_new,
                artists_new = :artists_new,
                artists_skipped = :artists_skipped,
                tracks_new = :tracks_new,
                tracks_skipped = :tracks_skipped,
                history_new = :history_new,
                history_skipped = :history_skipped,
                cursor_after_ms = :cursor_after_ms,
                cursor_next_ms = :cursor_next_ms
            WHERE audit_id = :audit_id
        """),
        {
            "finished_at": datetime.now(timezone.utc),
            "duration_ms": duration_ms,
            "audit_id": audit_id,
            "cursor_after_ms": cursor_after_ms,
            "cursor_next_ms": cursor_next_ms,
            **metrics,
        },
    )
    db.commit()


def update_audit_error(audit_id: int, duration_ms: int, error_message: str, db: Session) -> None:
    """
    Actualiza la fila de auditoría cuando el ETL falla.

    Args:
        audit_id (int): ID de la fila de auditoría.
        duration_ms (int): Duración hasta el error en milisegundos.
        error_message (str): Mensaje de error.
        db (Session): Sesión de SQLAlchemy.
    """
    db.execute(
        text("""
            UPDATE dwh.etl_audit SET
                finished_at = :finished_at,
                duration_ms = :duration_ms,
                status = 'error',
                error_message = :error_message
            WHERE audit_id = :audit_id
        """),
        {
            "finished_at": datetime.now(timezone.utc),
            "duration_ms": duration_ms,
            "error_message": error_message,
            "audit_id": audit_id,
        },
    )
    db.commit()


# ═══════════════════════════════════════════════════════════════
# PIPELINE — Orquesta todo el ETL en orden.
# ═══════════════════════════════════════════════════════════════

async def run_etl_pipeline(token: str, spotify_id: str, db: Session) -> dict:
    """
    Ejecuta el pipeline ETL completo en orden:
    Extract Users → Load Users → Extract Artists → Load Artists →
    Extract Tracks → Load Tracks → Extract Recently Played →
    Asegurar dimensiones del historial → Transform History →
    Load History → Audit.

    Args:
        token (str): Access token de Spotify (Bearer).
        spotify_id (str): ID de Spotify del usuario.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        dict: Resultado con audit_id, duration_ms, status, steps y metrics.
    """
    audit_id = insert_audit_start(spotify_id, db)
    t0 = time.time()
    steps = []
    metrics = {
        "users_new": 0,
        "artists_new": 0, "artists_skipped": 0,
        "tracks_new": 0, "tracks_skipped": 0,
        "history_new": 0, "history_skipped": 0,
    }

    try:
        # ── 1. Extract + Load User ────────────────────────────
        raw_user = await extract_user(token)
        steps.append({"phase": "Extract", "detail": "Perfil de usuario obtenido", "ok": True})

        user_data = transform_user(raw_user)
        user_metrics = load_user(user_data, db)
        metrics["users_new"] = user_metrics["users_new"]
        steps.append({
            "phase": "Load",
            "detail": f"dim_users — {user_metrics['users_new']} nuevo / {1 - user_metrics['users_new']} ya existía",
            "ok": True,
        })

        # ── 2. Extract + Load Artists ─────────────────────────
        raw_artists = await extract_top_artists(token)
        steps.append({"phase": "Extract", "detail": f"{len(raw_artists)} artistas obtenidos", "ok": True})

        artists_data = transform_artists(raw_artists)
        artists_metrics = load_artists(artists_data, db)
        metrics["artists_new"] = artists_metrics["artists_new"]
        metrics["artists_skipped"] = artists_metrics["artists_skipped"]
        steps.append({
            "phase": "Load",
            "detail": f"dim_artists — {artists_metrics['artists_new']} nuevos / {artists_metrics['artists_skipped']} ya existían",
            "ok": True,
        })

        # ── 3. Extract + Load Tracks ──────────────────────────
        raw_tracks = await extract_top_tracks(token)
        steps.append({"phase": "Extract", "detail": f"{len(raw_tracks)} canciones obtenidas", "ok": True})

        tracks_data = transform_tracks(raw_tracks)
        tracks_metrics = load_tracks(tracks_data, db)
        metrics["tracks_new"] = tracks_metrics["tracks_new"]
        metrics["tracks_skipped"] = tracks_metrics["tracks_skipped"]
        steps.append({
            "phase": "Load",
            "detail": f"dim_tracks — {tracks_metrics['tracks_new']} nuevos / {tracks_metrics['tracks_skipped']} ya existían",
            "ok": True,
        })

        # ── 4. Extract + Transform + Load History ─────────────
        cursor_after_ms = get_last_cursor(spotify_id, db)
        raw_history = await extract_recently_played(token, after=cursor_after_ms)
        steps.append({"phase": "Extract", "detail": f"{len(raw_history)} reproducciones recientes obtenidas", "ok": True})

        # Asegurar que los artistas y tracks del historial existan
        # en las dimensiones ANTES de cargar fact_listening_history.
        # Esto resuelve el problema de que el historial puede contener
        # canciones que no están en el top del usuario.
        for item in raw_history:
            track = item.get("track", {})
            artists = track.get("artists", [])
            if artists:
                artist = artists[0]
                db.execute(
                    text("""
                        INSERT INTO dwh.dim_artists (spotify_id, name, popularity, followers_count, genres)
                        VALUES (:spotify_id, :name, 0, 0, '{}')
                        ON CONFLICT (spotify_id) DO NOTHING
                    """),
                    {"spotify_id": artist["id"], "name": artist.get("name", "Unknown")},
                )
            if track.get("id"):
                artist_row = db.execute(
                    text("SELECT artist_id FROM dwh.dim_artists WHERE spotify_id = :sid"),
                    {"sid": artists[0]["id"] if artists else None},
                ).fetchone()
                db.execute(
                    text("""
                        INSERT INTO dwh.dim_tracks (spotify_id, name, artist_id, album_name, duration_ms, popularity, explicit)
                        VALUES (:spotify_id, :name, :artist_id, :album_name, :duration_ms, :popularity, :explicit)
                        ON CONFLICT (spotify_id) DO NOTHING
                    """),
                    {
                        "spotify_id": track["id"],
                        "name": track.get("name", "Unknown"),
                        "artist_id": artist_row[0] if artist_row else None,
                        "album_name": track.get("album", {}).get("name"),
                        "duration_ms": track.get("duration_ms", 0),
                        "popularity": track.get("popularity", 0),
                        "explicit": track.get("explicit", False),
                    },
                )
        db.commit()
        steps.append({"phase": "Transform", "detail": "Artistas y tracks del historial asegurados en dimensiones", "ok": True})

        history_data = transform_history(raw_history)
        steps.append({"phase": "Transform", "detail": "Timestamps normalizados, géneros procesados", "ok": True})

        history_metrics = load_history(history_data, spotify_id, db)
        metrics["history_new"] = history_metrics["history_new"]
        metrics["history_skipped"] = history_metrics["history_skipped"]
        steps.append({
            "phase": "Load",
            "detail": f"fact_listening_history — {history_metrics['history_new']} nuevos / {history_metrics['history_skipped']} ya existían",
            "ok": True,
        })

        # ── 5. Calcular cursor para próxima ejecución ─────────
        cursor_next_ms = None
        if history_data:
            max_played_at = max(item["played_at"] for item in history_data)
            cursor_next_ms = played_at_to_unix_ms(max_played_at)

        # ── 6. Commit y auditoría ─────────────────────────────
        db.commit()
        duration_ms = int((time.time() - t0) * 1000)

        update_audit_success(
            audit_id=audit_id,
            duration_ms=duration_ms,
            cursor_after_ms=cursor_after_ms,
            cursor_next_ms=cursor_next_ms,
            metrics=metrics,
            db=db,
        )

        steps.append({"phase": "Audit", "detail": f"Auditoría registrada — duración: {duration_ms / 1000:.2f} s", "ok": True})

        return {
            "audit_id": audit_id,
            "duration_ms": duration_ms,
            "status": "success",
            "steps": steps,
            "metrics": metrics,
        }

    except Exception as e:
        db.rollback()
        duration_ms = int((time.time() - t0) * 1000)
        update_audit_error(audit_id, duration_ms, str(e), db)
        steps.append({"phase": "Error", "detail": str(e), "ok": False})

        return {
            "audit_id": audit_id,
            "duration_ms": duration_ms,
            "status": "error",
            "steps": steps,
            "metrics": metrics,
        }