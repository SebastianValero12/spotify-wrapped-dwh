"""
filename: database.py
author: Johan Sebastian Valero Basabe
date: 2026-05-14
version: 1.0
description: Configuración centralizada de SQLAlchemy para PostgreSQL (Neon).
             Provee el engine, la sesión y la Base declarativa.
             Centralizar esto evita dependencias circulares y facilita testing.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

from backend.app.core.config import settings

# ── Engine ─────────────────────────────────────────────────────
# pool_pre_ping=True: verifica que la conexión siga viva antes de usarla.
# Neon puede cerrar conexiones idle; esto evita errores silenciosos.
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    echo=False,  # Cambiar a True para debug de SQL
)

# ── Session ────────────────────────────────────────────────────
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

# ── Base declarativa ───────────────────────────────────────────
Base = declarative_base()


# ── Dependency para FastAPI ────────────────────────────────────
def get_db():
    """
    Generador de sesiones para inyección de dependencias en FastAPI.

    Yields:
        Session: Sesión de SQLAlchemy vinculada al engine de Neon.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()