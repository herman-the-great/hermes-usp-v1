#!/usr/bin/env python3
"""
Fix inline image position: move image from TOP of email to BOTTOM (below text).
For all home_services threads in pending_approval state.

Reads current gmail_draft_id from DB, deletes from Gmail, resets thread to 'drafting',
then re-runs the draft generator which will call gmail_create_draft with the fixed code.
"""
import sys, os, time, sqlite3
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "skills" / "productivity" / "google-workspace" / "scripts"))

import google_api_usp

DB = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")

def get_threads():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.lead_id, t.gmail_draft_id, l.name, l.contact_email
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.vertical = 'home_services'
          AND t.thread_state = 'pending_approval'
          AND t.gmail_draft_id IS NOT NULL
        ORDER BY t.id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def reset_thread(thread_id):
    conn = sqlite3.connect(DB)
    conn.execute("UPDATE outreach_threads SET thread_state='drafting', gmail_draft_id=NULL WHERE id=?", (thread_id,))
    conn.commit()
    conn.close()

def delete_gmail_draft(draft_id):
    class FakeArgs:
        pass
    FakeArgs.draft_id = draft_id
    try:
        google_api_usp.gmail_delete_draft(FakeArgs())
        return True
    except Exception as e:
        print(f"  WARNING: could not delete draft {draft_id}: {e}")
        return False

def main():
    threads = get_threads()
    print(f"Found {len(threads)} threads to fix")
    
    for t in threads:
        print(f"\nThread {t['id']}: {t['name']}")
        print(f"  Deleting Gmail draft {t['gmail_draft_id']}...")
        delete_gmail_draft(t['gmail_draft_id'])
        
        print(f"  Resetting thread to 'drafting'...")
        reset_thread(t['id'])
    
    print(f"\nAll {len(threads)} threads reset to 'drafting'.")
    print("Run the draft generator to recreate them with image BELOW text:")
    print(f"  cd /home/cortana/.hermes/Hermes-USP-v1/jobs")
    print(f"  python3 usp_draft_generator.py --vertical home_services > /tmp/draft_gen.log 2>&1 &")

if __name__ == "__main__":
    main()
