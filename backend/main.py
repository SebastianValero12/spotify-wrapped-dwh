"""
filename: main.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Punto de entrada de la API FastAPI. Configura CORS y monta
             el router versionado v1.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.core.config import settings

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
)

# ── CORS ───────────────────────────────────────────────────────
# Permite que el frontend (localhost:3000) se comunique con el backend.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health check ───────────────────────────────────────────────
@app.get("/")
def health():
    """Endpoint de verificación básica. Retorna status ok."""
    return {"status": "ok"}


# ── Router v1 ─────────────────────────────────────────────────
from backend.app.v1.api import router as v1_router
app.include_router(v1_router, prefix="/v1")