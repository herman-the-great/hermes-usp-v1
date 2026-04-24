#!/usr/bin/env python3
"""
Direct draft regeneration for accounting threads with bad drafts.
Deletes old Gmail drafts and creates clean replacements.
"""
import sys, json, sqlite3, os, time, subprocess, re
sys.path.insert(0, '/home/cortana/.hermes/Hermes-USP-v1')

from jobs.usp_draft_generator import (
    build_email_body, load_offer, build_subject, call_local_model,
    SENDER_NAME, SENDER_EMAIL, DB
)

GMAIL_API = '/home/cortana/.hermes/skills/productivity/google-workspace/scripts/google_api_usp.py'
OFFER_PATH = '/home/cortana/.hermes/Hermes-USP-v1/offer_library/accounting_bookkeeping.json'
SCRIPT_DIR = '/home/cortana/.hermes/Hermes-USP-v1/jobs'

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Threads to regenerate
thread_ids = [4, 5, 6, 8, 9]

def gmail_delete_draft(draft_id):
    r = subprocess.run(
        ['python3', GMAIL_API, 'gmail', 'delete-draft', str(draft_id)],
        capture_output=True, text=True, timeout=20
    )
    return r.returncode == 0

def gmail_create_draft(to_email, subject, body_plain):
    """Create a Gmail draft using the API."""
    payload = {
        'to': to_email,
        'subject': subject,
        'body': body_plain,
    }
    r = subprocess.run(
        ['python3', GMAIL_API, 'gmail', 'create-draft',
         '--to', to_email, '--subject', subject,
         '--body', body],
        capture_output=True, text=True, timeout=30
    )
    if r.returncode != 0:
        return None, r.stderr[:200]
    try:
        data = json.loads(r.stdout)
        return data.get('draft_id'), None
    except:
        return None, r.stdout[:200]

# Load offer once
with open(OFFER_PATH) as f:
    offer = json.load(f)

results = []
for tid in thread_ids:
    t = conn.execute("""
        SELECT t.*, l.name as lead_name, l.contact_email, l.contact_quality,
               l.contact_named_person, l.enrichment_data, l.draftability_notes,
               l.assigned_vertical
        FROM outreach_threads t JOIN leads l ON l.id = t.lead_id
        WHERE t.id = ?
    """, (tid,)).fetchone()
    
    if not t:
        print(f"Thread {tid}: not found")
        continue
    
    lead = dict(t)
    enrichment = json.loads(t['enrichment_data'] or '{}')
    
    print(f"\nRegenerating thread {tid}: {t['lead_name']}")
    print(f"  email={t['contact_email']} cq={t['contact_quality']}")
    
    # Build subject
    subject = build_subject(lead, offer)
    print(f"  subject: {subject}")
    
    # Build body — use rejection_history if available
    rejection_note = dict(t).get('draftability_notes') or ''
    rejection_history = None
    if rejection_note and any(tag in rejection_note.lower() for tag in ['generic', 'pitchy', 'weak', 'wrong']):
        rejection_history = rejection_note
    
    body, gap = build_email_body(lead, offer, enrichment, rejection_history)
    
    if not body or len(body) < 50:
        print(f"  ERROR: body too short ({len(body) if body else 0} chars)")
        results.append({'tid': tid, 'status': 'error', 'reason': 'body_too_short'})
        continue
    
    print(f"  body preview: {body[:100]}...")
    print(f"  personalization_gap={gap}")
    
    # Create Gmail draft
    draft_id, err = gmail_create_draft(t['contact_email'], subject, body)
    if not draft_id:
        print(f"  ERROR creating draft: {err}")
        results.append({'tid': tid, 'status': 'error', 'reason': err})
        continue
    
    print(f"  Gmail draft created: {draft_id}")
    
    # Update DB
    conn.execute("""
        UPDATE outreach_threads
        SET gmail_draft_id = ?, updated_at = datetime('now')
        WHERE id = ?
    """, (draft_id, tid))
    
    # Log event
    conn.execute("""
        INSERT INTO outreach_events (thread_id, event_type, event_data)
        VALUES (?, 'draft_created', ?)
    """, (tid, json.dumps({
        'gmail_draft_id': draft_id,
        'subject': subject,
        'body_length': len(body),
        'personalization_gap': gap,
        'regenerated': True,
        'reason': 'wrapper_leakage_fix'
    })))
    
    results.append({'tid': tid, 'status': 'ok', 'draft_id': draft_id, 'subject': subject})
    print(f"  DB updated. Thread {tid} complete.")

conn.commit()
conn.close()

print(f"\n\nSUMMARY:")
for r in results:
    status = "OK" if r['status'] == 'ok' else f"ERROR: {r.get('reason')}"
    if r['status'] == 'ok':
        print(f"  Thread {r['tid']}: {status} — draft={r.get('draft_id')}")
    else:
        print(f"  Thread {r['tid']}: {status}")
