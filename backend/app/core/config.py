"""
filename: config.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Configuración centralizada de la aplicación usando pydantic-settings.
             Lee todas las variables de entorno desde el archivo .env.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Configuración global de la aplicación.

    Todas las variables se leen automáticamente del archivo .env
    en la raíz del proyecto. Nunca hardcodear valores sensibles aquí.
    """

    # ── Spotify OAuth ──────────────────────────────────────────
    SPOTIFY_CLIENT_ID: str
    SPOTIFY_CLIENT_SECRET: str
    SPOTIFY_REDIRECT_URI: str = "http://127.0.0.1:8000/v1/auth/callback"

    # ── PostgreSQL (Neon) ──────────────────────────────────────
    DATABASE_URL: str

    # ── App ────────────────────────────────────────────────────
    APP_NAME: str = "Spotify DWH API"
    APP_VERSION: str = "1.0.0"
    SECRET_KEY: str

    # ── Frontend ───────────────────────────────────────────────
    FRONTEND_URL: str = "http://localhost:3000"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
