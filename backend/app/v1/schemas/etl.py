"""
filename: etl.py
author: [nombre]
date: 2026-05-14
version: 1.0
description: Schemas Pydantic para los endpoints de ETL.
             Define EtlStatus (estado del DWH) y EtlRunResult (resultado
             de una ejecución del pipeline).
"""

from datetime import datetime
from pydantic import BaseModel


class TableStatus(BaseModel):
    """Estado de una tabla individual del DWH."""
    name: str
    record_count: int
    last_loaded_at: str | None = None
    status: str  # "empty" | "loaded" | "stale"


class RunSummary(BaseModel):
    """Resumen de una ejecución del ETL."""
    audit_id: int
    started_at: datetime
    duration_ms: int | None = None
    status: str
    history_new: int
    artists_new: int
    tracks_new: int
    error_message: str | None = None


class EtlStatus(BaseModel):
    """Respuesta de GET /v1/etl/status."""
    tables: list[TableStatus]
    last_runs: list[RunSummary]


class EtlStep(BaseModel):
    """Un paso individual del pipeline ETL."""
    phase: str
    detail: str
    ok: bool


class EtlMetrics(BaseModel):
    """Métricas de registros procesados por el ETL."""
    users_new: int
    artists_new: int
    artists_skipped: int
    tracks_new: int
    tracks_skipped: int
    history_new: int
    history_skipped: int


class EtlRunResult(BaseModel):
    """Respuesta de POST /v1/etl/run."""
    audit_id: int
    duration_ms: int
    status: str
    steps: list[EtlStep]
    metrics: EtlMetrics
