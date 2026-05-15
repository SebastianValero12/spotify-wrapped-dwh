"""
filename: api.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Agrupa todos los routers de la versión 1 de la API.
             Se monta en main.py con prefijo /v1.
"""

from fastapi import APIRouter

from backend.app.v1.routers import auth, profile, artists, tracks, history, etl

router = APIRouter()

# ── Routers activos ────────────────────────────────────────────
router.include_router(auth.router)
router.include_router(profile.router)
router.include_router(artists.router)
router.include_router(tracks.router)
router.include_router(history.router)
router.include_router(etl.router)