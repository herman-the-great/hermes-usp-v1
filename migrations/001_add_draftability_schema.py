#!/usr/bin/env python3
"""
Migration 001: Add draftability schema
Adds columns to leads and outreach_emails, creates review_packets table.
Design:
  - Pre-flight: detect already-existing columns, skip them
  - Backup: before any ALTER
  - Transaction: BEGIN IMMEDIATE
  - ALTER only missing columns
  - CREATE review_packets if not exists
  - COMMIT on success; ROLLBACK + restore on failure
  - 'skipped' if all columns/tables already exist
  - 'failed' row written only on exception (not written if restore succeeds)
"""
import sqlite3
import shutil
import sys
from pathlib import Path
from datetime import datetime

DB_PATH = Path.home() / ".hermes/Hermes-USP-v1/usp.db"
BACKUP_DIR = Path.home() / ".hermes/Hermes-USP-v1/backups"
MIGRATION_NAME = "001_add_draftability_schema"

LEADS_COLUMNS = [
    ("assigned_vertical",    "TEXT"),
    ("assigned_deck",        "TEXT"),
    ("business_legitimacy", "TEXT"),
    ("suppression_flag",     "TEXT"),
    ("fit_class",            "TEXT"),
    ("draftability_notes",   "TEXT"),
]

OUTREACH_EMAIL_COLUMNS = [
    ("sequence_number",        "INTEGER DEFAULT 0"),
    ("followup_stage",          "TEXT"),
    ("review_packet_id",        "INTEGER"),
    ("attachment_recommended",  "TEXT"),
    ("attachment_reason",       "TEXT"),
]

REVIEW_PACKETS_TABLE = """
CREATE TABLE IF NOT EXISTS review_packets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL,
    draft_id        INTEGER,
    sections        TEXT,
    formatted_text  TEXT,
    status          TEXT DEFAULT 'pending',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY (lead_id) REFERENCES leads(id)
);
"""

def backup_db():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"usp_pre_{MIGRATION_NAME}_{ts}.db"
    shutil.copy2(DB_PATH, backup_path)
    print(f"[MIGRATION] Backup: {backup_path}", flush=True)
    return backup_path

def table_exists(conn, table_name):
    result = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return result is not None

def column_exists(conn, table, column):
    result = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(row[1] == column for row in result)

def get_missing_leads_columns(conn):
    return [c for c in LEADS_COLUMNS if not column_exists(conn, "leads", c[0])]

def get_missing_outreach_columns(conn):
    return [c for c in OUTREACH_EMAIL_COLUMNS if not column_exists(conn, "outreach_emails", c[0])]

def write_migration_row(conn, status):
    now = datetime.now().isoformat()
    # migration_name is PRIMARY KEY — only INSERT once; UPDATE on subsequent calls
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

    # Pre-flight: check if ALL columns and review_packets already exist
    missing_leads = get_missing_leads_columns(conn)
    missing_outreach = get_missing_outreach_columns(conn)
    missing_review_packets = not table_exists(conn, "review_packets")

    if not missing_leads and not missing_outreach and not missing_review_packets:
        print(f"[MIGRATION] All columns/tables already exist. Writing 'skipped'.", flush=True)
        write_migration_row(conn, "skipped")
        conn.close()
        print("[MIGRATION] Status: skipped | No changes made.", flush=True)
        return

    print(f"[MIGRATION] Columns to add to leads: {[c[0] for c in missing_leads]}", flush=True)
    print(f"[MIGRATION] Columns to add to outreach_emails: {[c[0] for c in missing_outreach]}", flush=True)
    print(f"[MIGRATION] review_packets to create: {missing_review_packets}", flush=True)

    # Backup
    backup_path = backup_db()
    print("[MIGRATION] Status: migrating...", flush=True)

    try:
        # BEGIN IMMEDIATE acquires write lock
        conn.execute("BEGIN IMMEDIATE")
        write_migration_row(conn, "started")

        # ALTER leads — only missing columns
        for col_name, col_type in missing_leads:
            sql = f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}"
            conn.execute(sql)
            print(f"  + leads.{col_name}", flush=True)

        # ALTER outreach_emails — only missing columns
        for col_name, col_type in missing_outreach:
            sql = f"ALTER TABLE outreach_emails ADD COLUMN {col_name} {col_type}"
            conn.execute(sql)
            print(f"  + outreach_emails.{col_name}", flush=True)

        # CREATE review_packets
        if missing_review_packets:
            conn.execute(REVIEW_PACKETS_TABLE)
            print("  + review_packets table", flush=True)

        # Commit
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
        # Restore from backup — restored DB has exact pre-migration state, no failed row written
        shutil.copy2(backup_path, DB_PATH)
        print(f"[MIGRATION] Rolled back. Restored from: {backup_path}", flush=True)
        print("[MIGRATION] Status: failed | Restored DB has zero rows for this migration.", flush=True)
        sys.exit(1)

    finally:
        conn.close()

if __name__ == "__main__":
    main()
