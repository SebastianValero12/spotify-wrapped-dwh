"""
filename: env.py
author: [nombre]
date: 2026-05-14
version: 1.0
description: Configuración de Alembic para leer DATABASE_URL desde .env
             y conectarse a PostgreSQL en Neon.
"""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool
from alembic import context
from dotenv import load_dotenv

# ── Cargar .env desde la raíz del proyecto ─────────────────────
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

# ── Agregar el directorio raíz al sys.path ─────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ── Config de Alembic ──────────────────────────────────────────
config = context.config

# Sobreescribir sqlalchemy.url con el valor de .env
config.set_main_option("sqlalchemy.url", os.environ["DATABASE_URL"])

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata para autogenerate (None = migraciones manuales)
target_metadata = None


def run_migrations_offline() -> None:
    """Ejecuta migraciones en modo offline (genera SQL sin conectarse)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Ejecuta migraciones en modo online (conectado a Neon)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
