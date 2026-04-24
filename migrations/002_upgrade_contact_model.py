#!/usr/bin/env python3
"""
Migration 002: Upgrade contact model — separate path from quality.
Adds:
  - leads.contact_quality        TEXT (A/B/C/tiered/untiered/uncontactable)
  - leads.contact_named_person    TEXT (name of best named contact, if any)
  - leads.contact_named_role      TEXT (role of best named contact, if any)
Design:
  - Pre-flight: detect already-existing columns, skip them
  - Backup: before any ALTER
  - BEGIN IMMEDIATE transaction
  - ALTER only missing columns
  - COMMIT on success; ROLLBACK + restore on failure
  - 'skipped' if all columns already exist
  - No 'failed' row written after restore (restored DB = exact pre-migration state)
"""
import sqlite3
import shutil
import sys
from pathlib import Path
from datetime import datetime

DB_PATH = Path.home() / ".hermes/Hermes-USP-v1/usp.db"
BACKUP_DIR = Path.home() / ".hermes/Hermes-USP-v1/backups"
MIGRATION_NAME = "002_upgrade_contact_model"

NEW_COLUMNS = [
    ("contact_quality",     "TEXT DEFAULT 'untiered'"),
    ("contact_named_person","TEXT"),
    ("contact_named_role",  "TEXT"),
]

def backup_db():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"usp_pre_{MIGRATION_NAME}_{ts}.db"
    shutil.copy2(DB_PATH, backup_path)
    print(f"[MIGRATION] Backup: {backup_path}", flush=True)
    return backup_path

def column_exists(conn, table, column):
    result = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(row[1] == column for row in result)

def get_missing_columns(conn):
    return [c for c in NEW_COLUMNS if not column_exists(conn, "leads", c[0])]

def write_migration_row(conn, status):
    now = datetime.now().isoformat()
    existing = conn.execute(
        "SELECT status FROM schema_migrations WHERE migration_name = ?",
        (MIGRATION_NAME,)
    ).fetchone()
    if existing is None:
        conn.execute(
            "INSERT INTO schema_migrations (migration_name, status, applied_at) VALUES (?, ?, ?)",
            (MIGRATION_NAME, status, now)
        )
    else:
        conn.execute(
            "UPDATE schema_migrations SET status = ?, applied_at = ? WHERE migration_name = ?",
            (status, now, MIGRATION_NAME)
        )
    conn.commit()

def main():
    print(f"[MIGRATION] {MIGRATION_NAME} | Starting...", flush=True)

    conn = sqlite3.connect(DB_PATH)

    # Create schema_migrations table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            migration_name TEXT PRIMARY KEY,
            status         TEXT NOT NULL,
            applied_at     TEXT NOT NULL
        )
    """)

    missing = get_missing_columns(conn)

    if not missing:
        print(f"[MIGRATION] All columns already exist. Writing 'skipped'.", flush=True)
        write_migration_row(conn, "skipped")
        conn.close()
        print("[MIGRATION] Status: skipped | No changes made.", flush=True)
        return

    print(f"[MIGRATION] Columns to add: {[c[0] for c in missing]}", flush=True)

    backup_path = backup_db()
    print("[MIGRATION] Status: migrating...", flush=True)

    try:
        conn.execute("BEGIN IMMEDIATE")
        write_migration_row(conn, "started")

        for col_name, col_type in missing:
            sql = f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}"
            conn.execute(sql)
            print(f"  + leads.{col_name} ({col_type})", flush=True)

        conn.commit()
        write_migration_row(conn, "completed")
        print("[MIGRATION] Status: completed | All ALTERs applied successfully.", flush=True)
        print(f"[MIGRATION] To rollback: cp {backup_path} {DB_PATH}", flush=True)

    except Exception as e:
        print(f"[MIGRATION] Exception: {e}", flush=True)
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        shutil.copy2(backup_path, DB_PATH)
        print(f"[MIGRATION] Rolled back. Restored from: {backup_path}", flush=True)
        print("[MIGRATION] Status: failed | Restored DB has zero rows for this migration.", flush=True)
        sys.exit(1)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
