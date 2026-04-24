#!/usr/bin/env python3
"""
Phase 1 Task 1: Schema Extension
Adds new-model columns to leads table and creates new USP system tables.
Safe: only creates/alters; does not delete or migrate data.
"""
import sqlite3, sys, os

DB = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")

def run():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    # ── New columns on leads (add only if not exist) ──────────────────────────
    new_leads_cols = [
        ("qualification_state",    "TEXT"),
        ("outbound_state",         "TEXT DEFAULT 'off_market'"),
        ("contact_path",           "TEXT"),
        ("enrichment_data",        "TEXT"),  # legacy col may exist; this is a no-op if present
    ]

    for col_name, col_def in new_leads_cols:
        try:
            c.execute(f"ALTER TABLE leads ADD COLUMN {col_name} {col_def}")
            print(f"  [+ Added column: {col_name}]")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"  [= Already exists: {col_name}]")
            else:
                raise

    # ── outreach_threads ───────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS outreach_threads (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id          INTEGER REFERENCES leads(id),
            vertical         TEXT,
            thread_state     TEXT DEFAULT 'drafting',
            current_email    INTEGER DEFAULT 0,
            close_reason     TEXT,
            gmail_draft_id   TEXT,
            gmail_thread_id  TEXT,
            created_at       TEXT DEFAULT (datetime('now')),
            updated_at       TEXT DEFAULT (datetime('now'))
        )
    """)
    print("  [+ Created table: outreach_threads]")

    # ── outreach_events ────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS outreach_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id   INTEGER REFERENCES outreach_threads(id),
            event_type  TEXT,
            event_data  TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    print("  [+ Created table: outreach_events]")

    # ── packets ────────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS packets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id     INTEGER REFERENCES leads(id),
            vertical    TEXT,
            packet_text TEXT,
            created_by  TEXT DEFAULT 'system',
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    print("  [+ Created table: packets]")

    # ── daily_runs ────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name         TEXT,
            run_status       TEXT,
            leads_processed  INTEGER DEFAULT 0,
            runs_output      TEXT,
            error_log        TEXT,
            started_at       TEXT DEFAULT (datetime('now')),
            completed_at     TEXT
        )
    """)
    print("  [+ Created table: daily_runs]")

    # ── config ────────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key         TEXT PRIMARY KEY,
            value       TEXT,
            updated_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    print("  [+ Created table: config]")

    conn.commit()
    conn.close()
    print("\nSchema migration complete.")

if __name__ == "__main__":
    run()
