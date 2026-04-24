#!/usr/bin/env python3
"""
Phase 2 Schema Migration — Adds lifecycle stage + event type columns.
Safe: only adds columns if they don't exist. Does not alter data.
"""
import sqlite3, sys, os

DB = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")

def col_exists(conn, table, col):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return col in {r[1] for r in cur.fetchall()}

def run():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    added = []

    # ── outreach_threads.stage ──────────────────────────────────────────────────
    # Tracks which follow-up stage a thread is in.
    # 1 = sent_1 (first email sent)
    # 2 = waiting_for_followup_2
    # 3 = waiting_for_followup_3
    # 4 = waiting_for_followup_4
    # 5 = closed (any reason)
    if not col_exists(conn, 'outreach_threads', 'stage'):
        c.execute("ALTER TABLE outreach_threads ADD COLUMN stage INTEGER DEFAULT 1")
        added.append("outreach_threads.stage")
        print("  [+ Added column: outreach_threads.stage]")
    else:
        print("  [= Already exists: outreach_threads.stage]")

    # ── outreach_threads.next_followup ──────────────────────────────────────────
    # ISO date string of next follow-up date (NULL = not scheduled)
    if not col_exists(conn, 'outreach_threads', 'next_followup'):
        c.execute("ALTER TABLE outreach_threads ADD COLUMN next_followup TEXT")
        added.append("outreach_threads.next_followup")
        print("  [+ Added column: outreach_threads.next_followup]")
    else:
        print("  [= Already exists: outreach_threads.next_followup]")

    # ── outreach_events.event_type values (no column change needed) ─────────────
    # New event types we'll now support:
    # 'followup_scheduled' — follow-up date set
    # 'followup_draft_ready' — follow-up draft created and in Gmail
    # 'followup_sent' — follow-up email sent
    # 'reply_received' — human reply detected
    # 'auto_reply_detected' — auto-reply detected
    # 'bounced_recovery_1' — first bounce recovery attempt
    # 'bounced_recovery_2' — second bounce recovery attempt
    # 'human_attention' — thread needs human review
    # 'lifecycle_reset' — thread reset for re-processing

    # ── Set initial stage for existing threads ─────────────────────────────────
    # accounting threads 4-9: sent_1 (stage=1)
    c.execute("""
        UPDATE outreach_threads
        SET stage = 1
        WHERE vertical = 'accounting_bookkeeping'
          AND thread_state IN ('active', 'pending_approval')
          AND stage IS NULL
    """)
    acct_updated = c.rowcount

    # home_services sent threads: sent_1 (stage=1)
    c.execute("""
        UPDATE outreach_threads
        SET stage = 1
        WHERE vertical = 'home_services'
          AND thread_state = 'active'
          AND gmail_thread_id IS NOT NULL
          AND stage IS NULL
    """)
    hs_updated = c.rowcount

    # pending_draft / drafting threads: stage=0 (pre-send)
    c.execute("""
        UPDATE outreach_threads
        SET stage = 0
        WHERE thread_state IN ('drafting', 'pending_draft')
          AND stage IS NULL
    """)
    pre_send_updated = c.rowcount

    # closed threads: stage=5
    c.execute("""
        UPDATE outreach_threads
        SET stage = 5
        WHERE thread_state = 'closed'
          AND stage IS NULL
    """)
    closed_updated = c.rowcount

    # pending_phone_only: stage=0 (not in email flow yet)
    c.execute("""
        UPDATE outreach_threads
        SET stage = 0
        WHERE thread_state = 'pending_phone_only'
          AND stage IS NULL
    """)
    phone_updated = c.rowcount

    conn.commit()

    print(f"\n  Stage initialized:")
    print(f"    accounting sent threads:  {acct_updated} → stage=1")
    print(f"    home_services sent:        {hs_updated} → stage=1")
    print(f"    pre-send threads:          {pre_send_updated} → stage=0")
    print(f"    closed threads:            {closed_updated} → stage=5")
    print(f"    phone_only threads:        {phone_updated} → stage=0")
    print(f"\nTotal changes: {len(added)} columns added")
    print("Done.")

if __name__ == "__main__":
    run()
