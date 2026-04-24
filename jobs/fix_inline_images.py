#!/usr/bin/env python3
"""
Fix broken inline image drafts across all live verticals.

Root cause: Image attached with CID <inline_image> but HTML body never referenced it.
These drafts show the image as an attachment icon, not inline in the email body.

Fix: Delete broken drafts, reset threads to 'drafting', regenerate with correct MIME.

Usage:
    python3 fix_inline_images.py [--vertical home_services] [--dry-run]
"""
import sys, os, time, sqlite3, json, base64, email, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ROOT = os.path.expanduser("~/.hermes/Hermes-USP-v1")
DB = os.path.join(ROOT, "usp.db")
SCRIPT_DIR = "/home/cortana/.hermes/skills/productivity/google-workspace/scripts"

# Gmail API helpers
def build_service():
    sys.path.insert(0, SCRIPT_DIR)
    import google_api_usp
    return google_api_usp.build_service("gmail", "v1")

def check_inline_image(service, msg_id):
    """Return (is_ok, has_inline, has_img_tag)."""
    result = service.users().messages().get(userId="me", id=msg_id, format="raw").execute()
    raw_bytes = base64.urlsafe_b64decode(result["raw"] + "==")
    msg = email.message_from_bytes(raw_bytes)
    has_inline = False
    has_img_tag = False
    for part in msg.walk():
        ct = part.get_content_type()
        cid = part.get("Content-ID", "")
        if ct.startswith("image") and cid == "<inline_image>":
            has_inline = True
        if ct == "text/html":
            raw_html = base64.b64decode(part.get_payload()).decode("utf-8", errors="replace")
            if "cid:inline_image" in raw_html:
                has_img_tag = True
    return (has_inline and has_img_tag), has_inline, has_img_tag

def delete_gmail_draft(service, draft_id):
    service.users().drafts().delete(userId="me", id=draft_id).execute()

def reset_thread_to_drafting(thread_id):
    conn = sqlite3.connect(DB)
    conn.execute("""
        UPDATE outreach_threads
        SET gmail_draft_id = NULL,
            gmail_thread_id = NULL,
            thread_state    = 'drafting',
            updated_at       = datetime('now')
        WHERE id = ?
    """, (thread_id,))
    conn.commit()
    conn.close()

def get_threads_needing_fix(vertical=None):
    """Get threads with gmail_draft_id that have broken inline images."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    if vertical:
        rows = conn.execute("""
            SELECT t.id, l.name, t.thread_state, t.gmail_draft_id, t.gmail_thread_id,
                   l.contact_email, t.vertical
            FROM outreach_threads t
            JOIN leads l ON l.id = t.lead_id
            WHERE t.vertical = ?
              AND t.gmail_draft_id IS NOT NULL
              AND t.thread_state = 'pending_approval'
            ORDER BY t.id
        """, (vertical,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT t.id, l.name, t.thread_state, t.gmail_draft_id, t.gmail_thread_id,
                   l.contact_email, t.vertical
            FROM outreach_threads t
            JOIN leads l ON l.id = t.lead_id
            WHERE t.vertical IN ('home_services', 'accounting_bookkeeping', 'estate_planning_probate')
              AND t.gmail_draft_id IS NOT NULL
              AND t.thread_state = 'pending_approval'
            ORDER BY t.vertical, t.id
        """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fix broken inline image drafts")
    parser.add_argument("--vertical", default=None, help="Fix only one vertical")
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't fix")
    parser.add_argument("--skip-delete", action="store_true", help="Skip Gmail delete (just report)")
    args = parser.parse_args()

    service = build_service()
    threads = get_threads_needing_fix(args.vertical)

    print(f"Checking {len(threads)} threads for broken inline images...")
    print()

    to_fix = []
    already_ok = []
    errors = []

    for t in threads:
        try:
            # Try gmail_thread_id first (may be wrong — use gmail_draft_id to get real message_id)
            msg_id_to_check = t["gmail_thread_id"]
            
            # Special handling: if gmail_thread_id is stale/404, look up the real message_id from draft
            if t["gmail_draft_id"]:
                try:
                    draft_info = service.users().drafts().get(
                        userId="me", id=t["gmail_draft_id"], format="minimal"
                    ).execute()
                    real_msg_id = draft_info.get("message", {}).get("id", "")
                    if real_msg_id and real_msg_id != t["gmail_thread_id"]:
                        print(f"  Thread {t['id']:2d}: DB gmail_thread_id stale, correcting {t['gmail_thread_id'][:12]}… → {real_msg_id[:12]}…")
                        # Update DB with correct gmail_thread_id
                        conn2 = sqlite3.connect(DB)
                        conn2.execute("""
                            UPDATE outreach_threads SET gmail_thread_id = ? WHERE id = ?
                        """, (real_msg_id, t["id"]))
                        conn2.commit()
                        conn2.close()
                        msg_id_to_check = real_msg_id
                except Exception:
                    pass  # Use whatever we had

            is_ok, has_inline, has_img_tag = check_inline_image(service, msg_id_to_check)
            if is_ok:
                already_ok.append(t)
                print(f"  Thread {t['id']:2d} ({t['vertical']:25s}) {t['name'][:35]:35s} — already OK, skip")
            else:
                to_fix.append({**t, "has_inline": has_inline, "has_img_tag": has_img_tag})
                reason = []
                if not has_inline: reason.append("no image")
                if not has_img_tag: reason.append("no img tag")
                print(f"  Thread {t['id']:2d} ({t['vertical']:25s}) {t['name'][:35]:35s} — FIX NEEDED: {', '.join(reason)}")
        except Exception as e:
            errors.append((t, str(e)))
            print(f"  Thread {t['id']:2d} — ERROR: {e}")

    print()
    print(f"Already OK : {len(already_ok)}")
    print(f"Need fix   : {len(to_fix)}")
    print(f"Errors     : {len(errors)}")

    if args.dry_run or args.skip_delete:
        if to_fix:
            print()
            print("Would fix these threads:")
            for t in to_fix:
                print(f"  Thread {t['id']:2d}: {t['name']} ({t['contact_email']})")
        return

    if not to_fix:
        print("Nothing to fix.")
        return

    print()
    print("STEP 1: Deleting broken Gmail drafts...")
    for t in to_fix:
        try:
            print(f"  Deleting draft {t['gmail_draft_id']} (thread {t['id']})...", end=" ", flush=True)
            delete_gmail_draft(service, t["gmail_draft_id"])
            print("OK")
            time.sleep(0.5)
        except Exception as e:
            print(f"FAILED: {e}")
            errors.append((t, f"delete: {e}"))

    print()
    print("STEP 2: Resetting threads to 'drafting'...")
    for t in to_fix:
        reset_thread_to_drafting(t["id"])
        print(f"  Thread {t['id']:2d} reset to drafting")

    print()
    print("STEP 3: Regenerating drafts via usp_draft_generator.run()...")
    # Import after path is set
    import usp_draft_generator

    # Get verticals that need processing
    verticals_to_run = set(t["vertical"] for t in to_fix)
    for v in sorted(verticals_to_run):
        print(f"\n  Running draft generation for: {v}")
        result = usp_draft_generator.run(vertical=v, verbose=True)
        print(f"  Created: {result.get('created', 0)}, Blocked: {result.get('blocked', [])}")
        time.sleep(2)

    print()
    print("STEP 4: Verifying new drafts...")
    service = build_service()
    ok_count = 0
    fail_count = 0

    # Re-check all threads that were in to_fix
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    for t in to_fix:
        row = conn.execute("""
            SELECT t.id, l.name, t.thread_state, t.gmail_draft_id, t.gmail_thread_id
            FROM outreach_threads t
            JOIN leads l ON l.id = t.lead_id
            WHERE t.id = ?
        """, (t["id"],)).fetchone()
        if row and row["gmail_thread_id"]:
            try:
                is_ok, _, _ = check_inline_image(service, row["gmail_thread_id"])
                status = "✓ INLINE OK" if is_ok else "✗ STILL BROKEN"
                print(f"  Thread {row['id']:2d}: {status} — {row['name'][:40]}")
                if is_ok:
                    ok_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                print(f"  Thread {row['id']:2d}: ERROR verifying: {e}")
                fail_count += 1
        else:
            print(f"  Thread {t['id']:2d}: No gmail_thread_id after regeneration — may be blocked")
            fail_count += 1
    conn.close()

    print()
    print(f"=== SUMMARY ===")
    print(f"Fixed and verified: {ok_count}")
    print(f"Still broken:       {fail_count}")
    print(f"Already OK:         {len(already_ok)}")
    print(f"Errors:             {len(errors)}")
    if errors:
        print()
        print("Errors:")
        for t, err in errors:
            print(f"  Thread {t['id']}: {err}")

if __name__ == "__main__":
    main()
