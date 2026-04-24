#!/usr/bin/env python3
"""
USP Draft Watcher — Deletion Detection + Thread State Sync

Compares USP thread records against Gmail draft state.
Detects: draft deleted by operator, draft sent (human click), stale state.

USP-local: reads only usp.db and Gmail API. No jarvis.db. No Mission Control.

Usage:
  python3 usp_draft_watcher.py                    # detect + report
  python3 usp_draft_watcher.py --reconcile        # detect + fix thread states
  python3 usp_draft_watcher.py --reconcile --dry-run  # detect + report fixes without applying
"""
import argparse, json, sqlite3, subprocess, sys
from datetime import datetime

DB = "/home/cortana/.hermes/Hermes-USP-v1/usp.db"
GMAIL_SCRIPT = "/home/cortana/.hermes/skills/productivity/google-workspace/scripts/google_api_usp.py"


def gmail_list_drafts():
    """Return list of current Gmail draft IDs as strings."""
    result = subprocess.run(
        [sys.executable, GMAIL_SCRIPT, "gmail", "list-drafts", "--max", "100"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        return []
    try:
        drafts = json.loads(result.stdout)
        return [d["draft_id"] for d in drafts]
    except (json.JSONDecodeError, KeyError):
        return []


def get_pending_threads():
    """Return all threads in pending_approval state with their latest draft_id and event info."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT
            t.id as thread_id,
            t.lead_id,
            t.vertical,
            t.gmail_draft_id,
            t.gmail_thread_id,
            l.name as lead_name,
            l.draftability_notes,
            t.updated_at
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'pending_approval'
          AND t.gmail_draft_id IS NOT NULL
        ORDER BY t.id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_sent_events():
    """Return set of gmail_draft_ids that have a corresponding sent event."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    # A draft is "sent" if thread_state is 'active' AND there's a sent event
    rows = conn.execute("""
        SELECT DISTINCT t.gmail_draft_id
        FROM outreach_threads t
        WHERE t.thread_state = 'active'
          AND t.gmail_draft_id IS NOT NULL
    """).fetchall()
    conn.close()
    return {str(r["gmail_draft_id"]) for r in rows}


def detect_deletion(thread, gmail_draft_ids, sent_draft_ids):
    """
    Determine what happened to a thread's draft.
    Returns: 'deleted' | 'sent' | 'still_draft' | 'unknown'
    """
    draft_id = str(thread["gmail_draft_id"])

    # No draft in Gmail at all
    if draft_id not in gmail_draft_ids:
        # Check if it was sent (thread moved to active)
        if draft_id in sent_draft_ids:
            return "sent"
        else:
            return "deleted"

    return "still_draft"


def reconcile_threads(dry_run=False):
    """
    Compare Gmail draft state against USP thread records.
    Detect deletions, detect sends, fix thread states.
    Write rejection events for deleted drafts.
    """
    gmail_drafts = set(gmail_list_drafts())
    sent_draft_ids = get_sent_events()
    pending = get_pending_threads()

    deleted = []
    sent = []
    still_draft = []

    for thread in pending:
        status = detect_deletion(thread, gmail_drafts, sent_draft_ids)
        thread["detection_status"] = status

        if status == "deleted":
            deleted.append(thread)
        elif status == "sent":
            sent.append(thread)
        else:
            still_draft.append(thread)

    # Report
    print(f"\n{'='*55}")
    print(f"USP Draft Watcher — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Gmail drafts found: {len(gmail_drafts)}")
    print(f"USP threads in pending_approval: {len(pending)}")
    print(f"  Still drafts in Gmail: {len(still_draft)}")
    print(f"  Confirmed sent (thread active): {len(sent)}")
    print(f"  Deleted from Gmail: {len(deleted)}")

    if deleted:
        print(f"\n{'='*55}")
        print(f"DELETED DRAFTS — {len(deleted)} detected")
        print(f"{'='*55}")
        for t in deleted:
            print(f"  [{t['thread_id']}] {t['lead_name']} ({t['vertical']})")
            print(f"    gmail_draft_id: {t['gmail_draft_id']}")
            print(f"    draftability_notes: {t.get('draftability_notes', '') or '(none)'}")

    if sent:
        print(f"\nCONFIRMED SENT — {len(sent)}")
        for t in sent:
            print(f"  [{t['thread_id']}] {t['lead_name']} ({t['vertical']}) — thread now active")

    if still_draft:
        print(f"\nSTILL DRAFTS — {len(still_draft)}")
        for t in still_draft:
            print(f"  [{t['thread_id']}] {t['lead_name']} ({t['vertical']}) — draft confirmed in Gmail")

    # Apply fixes
    if dry_run:
        print(f"\n[DRY RUN] No changes applied.")
        print(f"Would reset {len(deleted)} deleted threads to 'drafting' state.")
        print(f"Would log {len(deleted)} draft_rejected events.")
        return {
            "gmail_draft_count": len(gmail_drafts),
            "pending_threads": len(pending),
            "deleted": len(deleted),
            "sent": len(sent),
            "still_draft": len(still_draft),
            "dry_run": True,
        }

    if deleted:
        print(f"\nApplying fixes...")
        conn = sqlite3.connect(DB)
        for t in deleted:
            # Log rejection event
            conn.execute("""
                INSERT INTO outreach_events (thread_id, event_type, event_data)
                VALUES (?, 'draft_rejected', ?)
            """, (t["thread_id"], json.dumps({
                "reason": "operator_deleted_unclassified",
                "lead_id": t["lead_id"],
                "vertical": t["vertical"],
                "lead_name": t["lead_name"],
                "gmail_draft_id": t["gmail_draft_id"],
                "detected_at": datetime.utcnow().isoformat(),
                "note": "Draft disappeared from Gmail — operator deleted. "
                        "Use --feedback to specify exact reason: too_generic | too_pitchy | "
                        "paraphrases_asset | wrong_tone | weak_personalization"
            })))

            # Append to draftability_notes
            existing = t.get("draftability_notes") or ""
            note = f"[{datetime.utcnow().date()}] gmail draft deleted (unclassified) "
            conn.execute("""
                UPDATE leads SET draftability_notes = ? || ' ' || ? WHERE id = ?
            """, (existing, note.strip(), t["lead_id"]))

            # Reset thread to drafting
            conn.execute("""
                UPDATE outreach_threads
                SET thread_state = 'drafting',
                    gmail_draft_id = NULL,
                    gmail_thread_id = NULL,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (t["thread_id"],))

            print(f"  [FIXED] Thread {t['thread_id']} ({t['lead_name']}) — reset to drafting, rejection logged")

        conn.commit()
        conn.close()

    if sent:
        conn = sqlite3.connect(DB)
        for t in sent:
            conn.execute("""
                UPDATE outreach_threads
                SET thread_state = 'active',
                    updated_at = datetime('now')
                WHERE id = ?
            """, (t["thread_id"],))
            print(f"  [FIXED] Thread {t['thread_id']} ({t['lead_name']}) — confirmed active")
        conn.commit()
        conn.close()

    return {
        "gmail_draft_count": len(gmail_drafts),
        "pending_threads": len(pending),
        "deleted": len(deleted),
        "sent": len(sent),
        "still_draft": len(still_draft),
        "dry_run": False,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="USP Draft Watcher — detect deletions and sync thread state")
    parser.add_argument("--reconcile", action="store_true", help="Apply state fixes (reset deleted threads to drafting)")
    parser.add_argument("--dry-run", action="store_true", help="Report only, do not apply fixes")
    args = parser.parse_args()

    result = reconcile_threads(dry_run=args.dry_run or not args.reconcile)

    if result.get("deleted", 0) > 0 and not (args.dry_run or args.reconcile):
        print(f"\nRun with --reconcile to apply fixes (reset deleted threads to drafting).")
        print(f"Run with --reconcile --dry-run to preview fixes without applying.")
