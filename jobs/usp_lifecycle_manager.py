#!/usr/bin/env python3
"""
usp_lifecycle_manager.py — Phase 2 Lifecycle Fix + Follow-up Scheduler

Handles:
1. Corrects thread stage values based on actual thread state
2. Computes next_followup dates for all active threads using sent event timestamps
3. Provides helpers for follow-up stage transitions

Weekday-aware follow-up rules:
  follow-up 2: +2 weekdays from sent_1
  follow-up 3: +4 weekdays from sent_1
  follow-up 4: +6 weekdays from sent_1

Usage:
  python3 usp_lifecycle_manager.py          # fix states + compute next_followup
  python3 usp_lifecycle_manager.py --dry   # show what would change
"""
import sqlite3, json, sys, os
from datetime import datetime, timedelta
from pathlib import Path

DB = Path.home() / ".hermes" / "Hermes-USP-v1" / "usp.db"
DRY = "--dry" in sys.argv

def get_business_days(start_date, offset):
    """Add offset weekdays (Mon-Fri) to start_date."""
    d = start_date
    added = 0
    while added < offset:
        d += timedelta(days=1)
        if d.weekday() < 5:  # Mon=0, Fri=4
            added += 1
    return d

def get_sent_date(conn, thread_id):
    """Get the sent_1 date for a thread from outreach_events."""
    row = conn.execute("""
        SELECT created_at FROM outreach_events
        WHERE thread_id = ? AND event_type = 'sent'
        ORDER BY id ASC LIMIT 1
    """, (thread_id,)).fetchone()
    if row:
        return datetime.fromisoformat(row[0].replace(" ", "T"))
    return None

def get_followup_date(sent_date, stage):
    """Return next follow-up date based on stage."""
    if stage == 1:
        return get_business_days(sent_date, 2)
    elif stage == 2:
        return get_business_days(sent_date, 4)
    elif stage == 3:
        return get_business_days(sent_date, 6)
    return None

def run():
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    changes = []

    # ── 1. Fix stage for closed threads (should be 5, not 1) ──────────────────
    closed = conn.execute("""
        SELECT id, thread_state, stage, close_reason FROM outreach_threads
        WHERE thread_state = 'closed' AND (stage != 5 OR stage IS NULL)
    """).fetchall()
    for r in closed:
        old = r["stage"] if r["stage"] is not None else "NULL"
        new = 5
        if not DRY:
            conn.execute("UPDATE outreach_threads SET stage=? WHERE id=?", (new, r["id"]))
        changes.append(f"Thread {r['id']}: closed → stage {old}→{new}")
        print(f"  Thread {r['id']}: closed (reason={r['close_reason']}) → stage {old}→{new}")

    # ── 2. Fix stage for pending_phone_only threads ────────────────────────────
    phone = conn.execute("""
        SELECT id FROM outreach_threads
        WHERE thread_state = 'pending_phone_only' AND (stage != 0 OR stage IS NULL)
    """).fetchall()
    for r in phone:
        if not DRY:
            conn.execute("UPDATE outreach_threads SET stage=0 WHERE id=?", (r["id"],))
        changes.append(f"Thread {r['id']}: pending_phone_only → stage 0")
        print(f"  Thread {r['id']}: pending_phone_only → stage 0")

    # ── 3. Fix stage for pending_draft / drafting threads ─────────────────────
    pre_send = conn.execute("""
        SELECT id FROM outreach_threads
        WHERE thread_state IN ('drafting', 'pending_draft')
          AND (stage != 0 OR stage IS NULL)
    """).fetchall()
    for r in pre_send:
        if not DRY:
            conn.execute("UPDATE outreach_threads SET stage=0 WHERE id=?", (r["id"],))
        changes.append(f"Thread {r['id']}: pre-send → stage 0")
        print(f"  Thread {r['id']}: pre-send → stage 0")

    # ── 4. Compute next_followup for active threads ────────────────────────────
    active = conn.execute("""
        SELECT t.id, t.thread_state, t.stage, t.lead_id,
               l.name as lead_name, l.outbound_state
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'active'
          AND t.gmail_thread_id IS NOT NULL
        ORDER BY t.id
    """).fetchall()

    print(f"\n  Active threads with next_followup:")
    for r in active:
        sent_date = get_sent_date(conn, r["id"])
        if sent_date is None:
            print(f"  Thread {r['id']} ({r['lead_name'][:25]}): NO sent event found — skipping")
            continue

        current_stage = r["stage"] or 1
        next_fu = get_followup_date(sent_date, current_stage)
        next_fu_str = next_fu.strftime("%Y-%m-%d") if next_fu else None

        # Check if already set correctly
        existing = conn.execute(
            "SELECT next_followup FROM outreach_threads WHERE id=?", (r["id"],)
        ).fetchone()
        existing_str = existing[0] if existing else None

        status = "already correct" if existing_str == next_fu_str else f"updated: {existing_str}→{next_fu_str}"
        print(f"  Thread {r['id']} ({r['lead_name'][:25]}): stage={current_stage}, next={next_fu_str} [{status}]")

        if not DRY and existing_str != next_fu_str:
            conn.execute(
                "UPDATE outreach_threads SET next_followup=? WHERE id=?",
                (next_fu_str, r["id"])
            )
            changes.append(f"Thread {r['id']}: next_followup {existing_str}→{next_fu_str}")

    if DRY:
        print("\n  DRY RUN — no changes applied")
    else:
        conn.commit()
        print(f"\n  {len(changes)} total changes applied")

    # ── 5. Summary ────────────────────────────────────────────────────────────
    print("\n  Final state summary:")
    cur = conn.execute("""
        SELECT thread_state,
               COUNT(*) as cnt,
               SUM(CASE WHEN stage=0 THEN 1 ELSE 0 END) as stage_0,
               SUM(CASE WHEN stage=1 THEN 1 ELSE 0 END) as stage_1,
               SUM(CASE WHEN stage=5 THEN 1 ELSE 0 END) as stage_5,
               SUM(CASE WHEN next_followup IS NOT NULL THEN 1 ELSE 0 END) as scheduled
        FROM outreach_threads
        GROUP BY thread_state
    """)
    for r in cur.fetchall():
        print(f"    {r[0]:22s}: total={r[1]:2d}  stage0={r[2] or 0}  stage1={r[3] or 0}  stage5={r[4] or 0}  scheduled={r[5] or 0}")

    conn.close()

if __name__ == "__main__":
    print("=== USP Lifecycle Manager ===")
    if DRY:
        print("DRY RUN MODE\n")
    run()
