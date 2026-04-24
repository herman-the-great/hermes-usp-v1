"""
Hermes-USP-v1 — Audit Logger

Universal action logger for all USP operations.
Writes to ~/.hermes/Hermes-USP-v1/usp.db audit_log table.

Isolation: Does NOT use jarvis.db. Uses usp.db only.
"""
import sqlite3
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DB_PATH = Path.home() / ".hermes" / "Hermes-USP-v1" / "usp.db"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(action: str, detail: Optional[str] = None, result: Optional[str] = None, engine: Optional[str] = None):
    """Log an action to the USP audit trail."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
            (action, engine, detail, result, _now())
        )
        conn.commit()
    finally:
        conn.close()


def get_recent(limit: int = 20, action_filter: Optional[str] = None):
    """Retrieve recent audit entries."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        if action_filter:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE action = ? ORDER BY created_at DESC LIMIT ?",
                (action_filter, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
