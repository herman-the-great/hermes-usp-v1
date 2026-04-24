#!/usr/bin/env python3
"""
Phase 3 — Home Services Draft Generation
Generates Gmail drafts for home_services threads that are in pending_draft state
and have no gmail_draft_id, OR are in drafting state (thread 26).

Threads to process (verified current state):
  26: Denver City Plumbing — drafting, no packet/draft yet
  27: Denver Dream Builders — pending_draft, no gmail_draft_id
  28: Peak Builders & Roofers — pending_draft, no gmail_draft_id
  29: Lifetime Roof & Solar — pending_draft, no gmail_draft_id
  30: Metro City Roofing — PENDING_APPROVAL, VALID GMAIL DRAFT — SKIP
  31: My Denver Plumber — pending_draft, no gmail_draft_id
  32: Relief Handyman Services — pending_draft, no gmail_draft_id
  33: Jenesis Roofing — pending_draft, no gmail_draft_id

Threads to NOT touch:
  14, 19, 21, 23 — already sent (active)
  20, 22 — phone_only (pending_phone_only)
  25 — closed

Model: llama3.2:3b (qwen3.5:4b confirmed unhealthy)
"""
import json, os, re, sqlite3, subprocess, sys, time
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent.parent
DB = ROOT / "usp.db"
GMAIL_API = Path.home() / ".hermes/skills/productivity/google-workspace/scripts/google_api_usp.py"
MODEL = "llama3.2:3b"
OLLAMA = "http://localhost:11434"

DB_COLS_THREAD = "id, lead_id, vertical, thread_state, gmail_thread_id, gmail_draft_id"
DB_COLS_LEAD = "id, name, contact_email, best_contact_path, contact_quality, qualification_state, outbound_state, assigned_vertical, enrichment_data"

SKIP_THREADS = {14, 19, 20, 21, 22, 23, 25}  # sent / phone_only / closed
SKIP_DRAFT_THREAD = {30}  # already has valid draft

PROMPT_TEMPLATE = """You are drafting a cold outreach email for a Denver home services company.
Generate ONLY the email body text — no subject lines, no meta comments.

Firm name: {firm_name}
Contact path: {contact_path}
Contact quality tier: {tier}
Assigned vertical: Home Services (HVAC, roofing, plumbing, handyman)

Key business problems to address (choose 2-3 that fit the business):
- Incoming leads falling through the cracks between initial contact and scheduled service
- Estimates and quotes getting lost in translation between office staff and field technicians
- Administrative time consuming technician hours that should be spent on revenue work
- Missed follow-up on estimates and quotes leading to lost jobs
- Coordination breakdowns between dispatch, office, and field crews

Email rules:
- Start with greeting: "{greeting}"
- Write 2 short paragraphs, each addressing a real operational pain point
- Include ONE clear CTA: "Reply to schedule a free 10-minute discovery call. No cost, no commitment — exploratory call only."
- Do NOT mention brand names
- Do NOT use meta wrapper phrases like "here's a cold outreach", "draft for the", "this outreach email"
- End with: "All the best,\nHerman Carter\nPartnership and Growth Consultant\nUSP LLC\nhttps://www.uspai.io/\nfounder-led implementation partner for faster follow-up, cleaner handoffs, and less manual admin in home service businesses"
- Keep under 200 words total body

Generate ONLY the email body text."""

STRIP_LINES = {
    "All the best,",
    "Herman Carter",
    "Partnership and Growth Consultant",
    "USP LLC",
    "https://www.uspai.io/",
}

PROHIBITED_PATTERNS = [
    "here's a cold outreach",
    "here is a cold outreach",
    "outreach email for",
    "draft of the cold outreach",
    "here's a draft",
    "here is a draft",
    "draft for the outreach",
    "this cold email",
    "this outreach email",
    "here's a brief",
    "i'm reaching out to share",
    "attached below",
    "visual overview below",
]

SUBJECTS = {
    "A": "Denver home service company workflow improvement opportunity",
    "B": "question about your company's workflow operations",
    "C": "HVAC, roofing, plumbing workflow review — Denver",
}


def get_confirmed_first_name(lead):
    ed = lead.get("enrichment_data", "")
    if not ed:
        return None
    try:
        data = json.loads(ed) if isinstance(ed, str) else ed
        np = data.get("named_person", "")
        if np and len(np) > 1:
            return np.split()[0]
    except:
        pass
    return None


def get_packet(conn, lead_id, vertical):
    row = conn.execute(
        "SELECT id, packet_text FROM packets WHERE lead_id=? AND vertical=? ORDER BY id DESC LIMIT 1",
        (lead_id, vertical)
    ).fetchone()
    return dict(row) if row else None


def call_ollama(prompt, model=MODEL):
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.7, "num_predict": 400}
    }
    try:
        result = subprocess.run(
            ["curl", "-s", "-X", "POST", f"{OLLAMA}/api/generate",
             "-d", json.dumps(payload), "--max-time", "60"],
            capture_output=True, text=True, timeout=65
        )
        if result.returncode != 0:
            return None, f"curl failed: {result.stderr[:100]}"
        data = json.loads(result.stdout)
        return data.get("response", "").strip(), None
    except Exception as e:
        return None, str(e)[:100]


def make_body(prompt_text):
    raw, err = call_ollama(prompt_text)
    if err or not raw:
        return None, err or "empty response"
    # Strip prohibited patterns
    lines = raw.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        skip = False
        for pat in PROHIBITED_PATTERNS:
            if pat.lower() in stripped.lower():
                skip = True
                break
        if skip:
            continue
        cleaned.append(line)
    body = "\n".join(cleaned).strip()
    # Ensure it ends properly
    if "All the best," not in body:
        body += "\n\nAll the best,\nHerman Carter\nPartnership and Growth Consultant\nUSP LLC\nhttps://www.uspai.io/\nfounder-led implementation partner for faster follow-up, cleaner handoffs, and less manual admin in home service businesses"
    return body, None


def create_gmail_draft(to_email, subject, body):
    result = subprocess.run(
        ["python3", str(GMAIL_API), "gmail", "create-draft",
         "--to", to_email, "--subject", subject, "--body", body],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        return None, result.stderr[:200]
    try:
        data = json.loads(result.stdout)
        return data.get("draft_id"), None
    except:
        return None, result.stdout[:200]


def update_thread(conn, thread_id, gmail_draft_id=None, thread_state=None):
    if gmail_draft_id:
        conn.execute(
            "UPDATE outreach_threads SET gmail_draft_id=?, thread_state=?, updated_at=datetime('now') WHERE id=?",
            (gmail_draft_id, thread_state or "pending_approval", thread_id)
        )
    elif thread_state:
        conn.execute(
            "UPDATE outreach_threads SET thread_state=?, updated_at=datetime('now') WHERE id=?",
            (thread_state, thread_id)
        )


def insert_event(conn, thread_id, event_type, data=None):
    conn.execute(
        "INSERT INTO outreach_events (thread_id, event_type, event_data) VALUES (?, ?, ?)",
        (thread_id, event_type, json.dumps(data or {}))
    )


def process_thread(conn, thread, dry=False):
    tid = thread["id"]
    lid = thread["lead_id"]
    lead = thread["lead"] if "lead" in thread else {}
    name = lead.get("name", "Unknown")
    email = lead.get("contact_email", "")
    path = lead.get("best_contact_path", "generic_inbox")
    tier = lead.get("contact_quality", "C")
    vertical = thread["vertical"]
    current_state = thread["thread_state"]

    if not email or email == "phone_only" or not email.strip():
        print(f"  Thread {tid} ({name}): NO VALID EMAIL — skipping")
        return False

    # Get greeting
    first_name = get_confirmed_first_name(lead)
    if tier in ("A", "B") and first_name:
        greeting = f"Hi {first_name},"
    else:
        firm_short = name.split()[0] if name else "your company"
        greeting = f"Hi {firm_short} team,"

    # Get packet
    packet = get_packet(conn, lid, vertical)
    if not packet:
        print(f"  Thread {tid} ({name}): NO PACKET — generating...")
        # Inline minimal packet
        packet_text = f"OUTREACH PACKET | {name} | Home Services | {email}"
        conn.execute(
            "INSERT INTO packets (lead_id, vertical, packet_text, created_by) VALUES (?, ?, ?, ?)",
            (lid, vertical, packet_text, "system_phase3")
        )
        conn.commit()
        print(f"  Thread {tid}: packet created")
    else:
        print(f"  Thread {tid} ({name}): packet found (id={packet['id']})")

    # Generate body
    prompt = PROMPT_TEMPLATE.format(
        firm_name=name,
        contact_path=path,
        tier=tier,
        greeting=greeting
    )
    body, err = make_body(prompt)
    if err:
        print(f"  Thread {tid}: FAILED to generate body: {err}")
        return False

    # Subject
    subject = SUBJECTS.get(tier, SUBJECTS["C"])

    if dry:
        print(f"  Thread {tid} ({name}): [DRY] would create draft to {email}, subject='{subject}'")
        print(f"    Body preview: {body[:100]}...")
        return True

    # Create Gmail draft
    draft_id, err = create_gmail_draft(email, subject, body)
    if err or not draft_id:
        print(f"  Thread {tid}: FAILED to create Gmail draft: {err}")
        return False

    # Update DB
    new_state = "pending_approval"
    update_thread(conn, tid, gmail_draft_id=draft_id, thread_state=new_state)
    insert_event(conn, tid, "draft_created", {
        "gmail_draft_id": draft_id,
        "subject": subject,
        "to_email": email,
        "model": MODEL,
        "greeting": greeting
    })
    conn.commit()

    print(f"  Thread {tid} ({name}): DRAFT CREATED — {draft_id} — state={new_state}")
    return True


def main():
    dry = "--dry" in sys.argv
    if dry:
        print("DRY RUN MODE\n")

    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # Get threads to process: home_services in pre-send states
    threads = conn.execute(f"""
        SELECT {DB_COLS_THREAD}
        FROM outreach_threads
        WHERE vertical = 'home_services'
          AND thread_state IN ('drafting', 'pending_draft')
          AND id NOT IN ({','.join('?' * len(SKIP_THREADS))})
        ORDER BY id
    """, tuple(SKIP_THREADS)).fetchall()

    print(f"Found {len(threads)} threads to process:")
    for t in threads:
        print(f"  Thread {t['id']}: state={t['thread_state']}, draft_id={t['gmail_draft_id']}")

    if not threads:
        print("Nothing to do.")
        return

    success = 0
    for t in threads:
        thread = dict(t)
        lead = conn.execute(
            f"SELECT {DB_COLS_LEAD} FROM leads WHERE id=?",
            (thread["lead_id"],)
        ).fetchone()
        thread["lead"] = dict(lead) if lead else {}
        print(f"\nProcessing thread {thread['id']}...")
        ok = process_thread(conn, thread, dry=dry)
        if ok:
            success += 1

    print(f"\n{'DRY RUN complete' if dry else 'Done'}: {success}/{len(threads)} threads processed successfully")
    conn.close()


if __name__ == "__main__":
    main()
