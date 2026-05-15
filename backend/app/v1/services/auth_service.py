"""
filename: auth_service.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Servicio de autenticación OAuth PKCE con Spotify.
             Genera PKCE, guarda estado en pkce_sessions, intercambia
             code por tokens, hace upsert en dim_users y emite JWT.
"""

import hashlib
import base64
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from jose import jwt
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.app.core.config import settings
from backend.app.core import spotify_client


# ── PKCE helpers ───────────────────────────────────────────────

def generate_code_verifier() -> str:
    """
    Genera un code_verifier aleatorio de 64 bytes, URL-safe.

    Returns:
        str: Code verifier para PKCE.
    """
    return secrets.token_urlsafe(64)


def generate_code_challenge(verifier: str) -> str:
    """
    Genera el code_challenge a partir del code_verifier usando SHA-256.

    Args:
        verifier (str): Code verifier generado previamente.

    Returns:
        str: Code challenge en formato BASE64URL (sin padding).
    """
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_state() -> str:
    """
    Genera un UUID aleatorio para el parámetro state de OAuth.

    Returns:
        str: UUID como string.
    """
    return str(uuid.uuid4())


# ── Login: construir URL de autorización ───────────────────────

def build_spotify_auth_url(db: Session) -> str:
    """
    Genera PKCE (verifier + challenge + state), guarda en pkce_sessions
    y retorna la URL de autorización de Spotify.

    Args:
        db (Session): Sesión de SQLAlchemy.

    Returns:
        str: URL completa de Spotify para redirect.
    """
    verifier = generate_code_verifier()
    challenge = generate_code_challenge(verifier)
    state = generate_state()

    # Guardar {state → verifier} en pkce_sessions (uso único)
    db.execute(
        text("""
            INSERT INTO public.pkce_sessions (state, verifier)
            VALUES (:state, :verifier)
        """),
        {"state": state, "verifier": verifier},
    )
    db.commit()

    # Construir URL de autorización
    scopes = "user-read-private user-read-email user-top-read user-read-recently-played"

    auth_url = (
        f"{spotify_client.SPOTIFY_AUTH_URL}"
        f"?client_id={settings.SPOTIFY_CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={settings.SPOTIFY_REDIRECT_URI}"
        f"&scope={scopes}"
        f"&code_challenge={challenge}"
        f"&code_challenge_method=S256"
        f"&state={state}"
    )

    return auth_url


# ── Callback: validar state, intercambiar code, emitir JWT ─────

async def handle_callback(code: str, state: str, db: Session) -> str:
    """
    Procesa el callback de Spotify: valida state, intercambia code
    por tokens, hace upsert en dim_users y emite JWT de la app.

    Args:
        code (str): Authorization code de Spotify.
        state (str): State recibido en el callback.
        db (Session): Sesión de SQLAlchemy.

    Returns:
        str: JWT de la app firmado con SECRET_KEY.

    Raises:
        ValueError: Si el state no existe en pkce_sessions.
    """
    # 1. Verificar que el state existe y recuperar verifier
    result = db.execute(
        text("SELECT verifier FROM public.pkce_sessions WHERE state = :state"),
        {"state": state},
    ).fetchone()

    if result is None:
        raise ValueError("State inválido o expirado")

    verifier = result[0]

    # 2. Eliminar la fila (uso único)
    db.execute(
        text("DELETE FROM public.pkce_sessions WHERE state = :state"),
        {"state": state},
    )
    db.commit()

    # 3. Intercambiar code por tokens de Spotify
    token_data = await spotify_client.exchange_code_for_tokens(code, verifier)

    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    expires_in = token_data["expires_in"]  # típicamente 3600 segundos

    # 4. Obtener perfil del usuario de Spotify
    profile = await spotify_client.get_current_user(access_token)

    spotify_id = profile["id"]
    display_name = profile.get("display_name")
    email = profile.get("email")
    country = profile.get("country")
    followers = profile.get("followers", {}).get("total", 0)
    product = profile.get("product")

    # 5. Calcular expiración del token de Spotify
    token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    # 6. UPSERT en dim_users
    db.execute(
        text("""
            INSERT INTO dwh.dim_users (
                spotify_id, display_name, email, country, followers, product,
                spotify_access_token, spotify_refresh_token, token_expires_at
            )
            VALUES (
                :spotify_id, :display_name, :email, :country, :followers, :product,
                :access_token, :refresh_token, :token_expires_at
            )
            ON CONFLICT (spotify_id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                followers = EXCLUDED.followers,
                product = EXCLUDED.product,
                spotify_access_token = EXCLUDED.spotify_access_token,
                spotify_refresh_token = EXCLUDED.spotify_refresh_token,
                token_expires_at = EXCLUDED.token_expires_at
        """),
        {
            "spotify_id": spotify_id,
            "display_name": display_name,
            "email": email,
            "country": country,
            "followers": followers,
            "product": product,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_expires_at": token_expires_at,
        },
    )
    db.commit()

    # 7. Emitir JWT de la app
    app_token = create_app_jwt(spotify_id)

    return app_token


# ── JWT de la app ──────────────────────────────────────────────

def create_app_jwt(spotify_id: str) -> str:
    """
    Crea un JWT firmado para la app con el spotify_id como subject.

    Args:
        spotify_id (str): ID de Spotify del usuario.

    Returns:
        str: JWT firmado con HS256, expira en 8 horas.
    """
    payload = {
        "sub": spotify_id,
        "exp": datetime.now(timezone.utc) + timedelta(hours=8),
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm="HS256")
