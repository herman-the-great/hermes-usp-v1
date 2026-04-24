#!/usr/bin/env python3
"""
Phase 2: Thread Watcher for USP — detects human Send and manages thread lifecycle.

Three phases:
  Phase 1 — pending_approval threads:
    Human clicks Send in Gmail → draft gone → thread_state becomes 'active'
    Automatic. No manual DB step. No Gmail send triggered by watcher.

  Phase 2 — active threads:
    Detect replies → classify → positive / negative / neutral → appropriate handler
    Detect bounces  → thread_state 'closed', close_reason 'bounced', lead suppressed

  Phase 2.5 — non-active threads that were sent (stuck in drafting, etc.):
    Detect replies using gmail_message_id from draft_created events
    Classify and handle same as Phase 2.

Reads:  usp.db (outreach_threads, outreach_events, leads)
Writes: usp.db (outreach_threads, outreach_events, leads)
Gmail:  read-only (get-draft, get, search). No send.
"""
import sqlite3, sys, os, logging, subprocess, json, base64
from datetime import datetime
from email.mime.text import MIMEText
from email.header import decode_header

# Import USP reply classifier (same directory)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from usp_reply_classifier import (
    classify_reply,
    should_read_body,
    get_reply_body,
    get_thread_enrichment,
    build_positive_alert_text,
    build_negative_alert_text,
    generate_negative_response_draft,
    generate_positive_response_draft,
    write_reply_event,
    flag_lead_negative_reply,
)

DB = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")
SCRIPT = "/home/cortana/.hermes/skills/productivity/google-workspace/scripts/google_api_usp.py"
SENDER_EMAIL = "hermancarter373@gmail.com"

# Build Gmail service once — reused across draft creation and message reads
_GMAIL_SERVICE = None
def _get_gmail_service():
    global _GMAIL_SERVICE
    if _GMAIL_SERVICE is None:
        sys.path.insert(0, '/home/cortana/.hermes/skills/productivity/google-workspace/scripts')
        from google_api_usp import build_service
        _GMAIL_SERVICE = build_service('gmail', 'v1')
    return _GMAIL_SERVICE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(os.path.expanduser("~/.hermes/Hermes-USP-v1/logs/usp_thread_watcher.log")),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("usp_thread_watcher")


# ── Gmail API wrappers ────────────────────────────────────────────────────────

class GmailAPIError(Exception):
    def __init__(self, exit_code, stderr):
        self.exit_code = exit_code
        self.stderr = stderr
        super().__init__(f"Gmail API error {exit_code}: {stderr}")


def gmail_api(action, *args):
    """Call google_api_usp.py and return parsed JSON. Raises GmailAPIError on non-zero exit."""
    cmd = [sys.executable, SCRIPT, "gmail", action] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise GmailAPIError(result.returncode, result.stderr)
    stdout = result.stdout.strip()
    if not stdout:
        return None
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return None


def gmail_get_draft(draft_id):
    """
    Get a Gmail draft by ID.
    Returns: {"draft_id": ..., "message_id": ..., "label_ids": [...], ...}
    Raises GmailAPIError on non-zero exit (including 404).
    """
    return gmail_api("get-draft", str(draft_id))


def gmail_get_message(msg_id):
    """
    Get a Gmail message by ID.
    Returns: {"id": ..., "threadId": ..., "labels": [...], ...}
    Raises GmailAPIError on non-zero exit (including 404).
    """
    return gmail_api("get", str(msg_id))


def gmail_list_messages(query, max_results=5):
    """
    Search Gmail messages.
    Returns: list of message metadata dicts, or empty list on no results.
    Never raises — "No messages found." is exit 0 with empty stdout.
    """
    try:
        result = gmail_api("search", query, f"--max={max_results}")
        if result is None:
            return []
        if isinstance(result, str):
            return []   # "No messages found."
        if not isinstance(result, list):
            return []
        return result
    except GmailAPIError:
        return []


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_lead(lead_id):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_thread_subject(thread_id):
    """
    Return the subject line stored at draft creation time.
    Source: outreach_events.draft_created.event_data['subject']
    Returns None if no draft_created event exists for this thread.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT event_data FROM outreach_events
        WHERE thread_id = ? AND event_type = 'draft_created'
        ORDER BY id DESC LIMIT 1
    """, (thread_id,)).fetchone()
    conn.close()
    if not row:
        return None
    data = json.loads(row['event_data'])
    return data.get('subject')


def get_draft_message_id(thread_id):
    """
    Return the Gmail message ID stored at draft creation.
    Source: outreach_events.draft_created.event_data['gmail_message_id']
    Returns None for legacy threads created before this fix.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT event_data FROM outreach_events
        WHERE thread_id = ? AND event_type = 'draft_created'
        ORDER BY id DESC LIMIT 1
    """, (thread_id,)).fetchone()
    conn.close()
    if not row:
        return None
    data = json.loads(row['event_data'])
    return data.get('gmail_message_id')


def get_pending_approval_threads():
    """
    Return threads in 'pending_approval' state.
    gmail_thread_id is NOT required — it may be NULL or stale.
    gmail_draft_id IS required.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.lead_id, t.vertical, t.thread_state,
               t.current_email, t.gmail_draft_id,
               l.name as lead_name, l.contact_email,
               l.outbound_state, l.qualification_state
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'pending_approval'
          AND t.gmail_draft_id IS NOT NULL
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_active_threads():
    """Return threads with thread_state = 'active' that need polling."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.lead_id, t.vertical, t.thread_state,
               t.current_email, t.gmail_thread_id,
               l.name as lead_name, l.qualification_state
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'active'
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def record_event(thread_id, event_type, event_data=None):
    """Write an outreach_event row."""
    conn = sqlite3.connect(DB)
    conn.execute("""
        INSERT INTO outreach_events (thread_id, event_type, event_data)
        VALUES (?, ?, ?)
    """, (thread_id, event_type, json.dumps(event_data or {})))
    conn.commit()
    conn.close()


def update_thread_state(thread_id, **fields):
    """Update mutable thread fields."""
    if not fields:
        return
    sets = [f"{k} = ?" for k in fields]
    vals = list(fields.values()) + [thread_id]
    conn = sqlite3.connect(DB)
    conn.execute(
        f"UPDATE outreach_threads SET {', '.join(sets)}, updated_at = datetime('now') WHERE id = ?",
        vals
    )
    conn.commit()
    conn.close()


# ── Phase 1: Send detection ──────────────────────────────────────────────────

def detect_send_state(thread):
    """
    Returns 'sent'        — draft gone from Gmail (human clicked Send)
    Returns 'still_draft' — draft confirmed present in Gmail with DRAFT label
    Returns 'unknown'     — transient failure, retry next cycle

    gmail_thread_id is NEVER required by this function.
    """
    draft_id = thread['gmail_draft_id']

    try:
        draft = gmail_get_draft(draft_id)
    except GmailAPIError as e:
        # Parse HTTP status from stderr
        stderr = e.stderr.lower()
        if '404' in stderr or 'notfound' in stderr or 'not found' in stderr:
            log.info(f"Thread {thread['id']}: draft {draft_id} returned 404 — send detected")
            return 'sent'
        # 400 (bad ID), 403 (rate limit), 429, 500, auth failure → fail open
        log.warning(f"Thread {thread['id']}: draft {draft_id} returned {e.exit_code} — transient, retry later")
        return 'unknown'
    except Exception as e:
        log.warning(f"Thread {thread['id']}: draft {draft_id} fetch failed ({type(e).__name__}) — retry later")
        return 'unknown'

    # Draft retrieved — DRAFT label is the authoritative signal.
    # Gmail API returns DRAFT label when the draft genuinely exists.
    # The label_ids field is at the TOP LEVEL of the API response (not inside 'message').
    # If fetch succeeded and DRAFT label is present → still a draft.
    # If fetch succeeded but DRAFT label is absent → treat as sent.
    # If fetch raised 404 → draft deleted by user → sent.
    if 'DRAFT' in (draft.get('label_ids') or []):
        log.debug(f"Thread {thread['id']}: draft {draft_id} confirmed present with DRAFT label")
        return 'still_draft'
    else:
        log.info(f"Thread {thread['id']}: draft {draft_id} fetch succeeded but no DRAFT label — send detected")
        return 'sent'


def correlate_sent_message(thread, lead, subject):
    """
    Find the sent message after a draft has been sent.
    Three-anchor model:

      Anchor 1 — gmail_message_id (preferred primary):
        The draft's message ID from event_data.
        Gmail: same message moves drafts→sent without new ID.
        Verified by: message found + 'SENT' in labels.

      Anchor 2 — subject + to_email (required fallback):
        Stored subject + stored to_email in search query.
        Verified by: 'SENT' label + from_email matches sender.

      Anchor 3 — to_email only (uncertain last resort):
        Most recent sent message to contact.
        Verified by: 'SENT' label + from_email matches sender.
        Output marked uncertain — human review required via daily digest.

    Returns: (message_id, thread_id, timestamp, correlation_quality)
    """
    contact_email = lead['contact_email']
    draft_msg_id  = get_draft_message_id(thread['id'])

    # ── ANCHOR 1 ───────────────────────────────────────────────────────────
    if draft_msg_id:
        try:
            msg = gmail_get_message(draft_msg_id)
            if msg and 'SENT' in (msg.get('label_ids') or []):
                log.info(f"Thread {thread['id']}: Anchor 1 success — msg={draft_msg_id} thread={msg.get('threadId')}")
                return draft_msg_id, msg.get('threadId'), msg.get('internalDate'), "Anchor1"
            elif msg:
                log.info(f"Thread {thread['id']}: Anchor 1 message found but not SENT — trying Anchor 2")
        except GmailAPIError as e:
            log.info(f"Thread {thread['id']}: Anchor 1 GmailAPIError ({e.exit_code}) — trying Anchor 2")
        except Exception as e:
            log.info(f"Thread {thread['id']}: Anchor 1 error ({e}) — trying Anchor 2")

    # ── ANCHOR 2 ───────────────────────────────────────────────────────────
    if subject:
        query = f"to:{contact_email} subject:{subject} is:sent"
        candidates = gmail_list_messages(query, max_results=5)
        for meta in candidates:
            try:
                msg = gmail_get_message(meta['id'])
            except Exception:
                continue
            if 'SENT' not in (msg.get('label_ids') or []):
                continue
            if SENDER_EMAIL not in (msg.get('from') or ''):
                continue
            log.info(f"Thread {thread['id']}: Anchor 2 success — msg={msg['id']} thread={msg.get('threadId')}")
            return msg['id'], msg.get('threadId'), msg.get('internalDate'), "Anchor2"

    # ── ANCHOR 3 (uncertain) ────────────────────────────────────────────────
    fallback_query = f"to:{contact_email} is:sent"
    candidates = gmail_list_messages(fallback_query, max_results=3)
    for meta in candidates:
        try:
            msg = gmail_get_message(meta['id'])
        except Exception:
            continue
        if 'SENT' not in (msg.get('label_ids') or []):
            continue
        if SENDER_EMAIL not in (msg.get('from') or ''):
            continue
        log.warning(f"Thread {thread['id']}: Anchor 3 uncertain match — msg={msg['id']} thread={msg.get('threadId')}")
        return msg['id'], msg.get('threadId'), msg.get('internalDate'), "Anchor3_Uncertain"

    log.warning(f"Thread {thread['id']}: No sent message found via any anchor")
    return None, None, None, "correlation_failed"


def process_pending_approval_thread(thread):
    """
    Detects human Send event and executes automatic state transitions.

    Human action required: clicking Send in Gmail only.
    Manual DB steps required: NONE.
    Gmail send triggered by watcher: NONE.
    """
    lead = get_lead(thread['lead_id'])
    send_state = detect_send_state(thread)

    if send_state == 'still_draft':
        log.debug(f"Thread {thread['id']}: draft still in Gmail — no action")
        return False

    if send_state != 'sent':
        log.debug(f"Thread {thread['id']}: send_state={send_state} — no action")
        return False

    # ── SEND DETECTED ─────────────────────────────────────────────────────
    log.info(f"Thread {thread['id']}: SEND DETECTED — transitioning to active")

    subject = get_thread_subject(thread['id'])
    msg_id, gmail_thread_id, sent_at, correlation = correlate_sent_message(thread, lead, subject)

    # ── GUARD: correlation_failed means no sent message was found in Gmail.
    # Never transition to active without proof of a real sent message.
    # A missing draft + missing sent message = operator deleted draft (Draft Watcher handles this).
    if correlation == "correlation_failed":
        log.error(f"Thread {thread['id']}: DRAFT GONE but no sent message found in Gmail — "
                  "will NOT transition to active. "
                  "This state requires human review. "
                  "Draft Watcher handles deletion → drafting reset.")
        return False

    is_uncertain = (correlation in ("Anchor3_Uncertain",))

    # ── TRANSITION 1: outreach_threads ────────────────────────────────────
    update_thread_state(thread['id'],
        thread_state   = 'active',
        gmail_thread_id = gmail_thread_id,
        current_email  = 1,
    )
    log.info(f"Thread {thread['id']}: thread_state=active, current_email=1")

    # ── TRANSITION 2: outreach_events ───────────────────────────────────────
    record_event(thread['id'], 'sent', {
        "message_id":    msg_id,
        "thread_id":     gmail_thread_id,
        "sent_at":       sent_at,
        "to_email":      lead['contact_email'],
        "draft_id":      thread['gmail_draft_id'],
        "subject":       subject if correlation != "Anchor3_Uncertain" else None,
        "correlation":   correlation,
        "uncertain":     is_uncertain,
        "needs_review":  is_uncertain,
        "reviewed":      False,
        "review_note":   None,
    })
    log.info(f"Thread {thread['id']}: sent event written — correlation={correlation}")

    # ── TRANSITION 3: leads.outbound_state ────────────────────────────────
    conn = sqlite3.connect(DB)
    conn.execute(
        "UPDATE leads SET outbound_state = 'in_pipeline' WHERE id = ? AND outbound_state != 'in_pipeline'",
        (thread['lead_id'],)
    )
    conn.commit()
    conn.close()
    log.info(f"Lead {thread['lead_id']}: outbound_state=in_pipeline")

    return True


# ── Phase 2: Reply/bounce detection (Phase 2.1 — active) ────────────────────

def poll_gmail_for_thread(thread):
    """
    Poll Gmail INBOX for replies and bounces on an active thread.

    Reply detection:
      Search INBOX for messages to the contact that are NOT from the sender.
      The sent message itself is labeled SENT — it does not appear in INBOX.
      Any message from the contact (or a third party) in INBOX within the
      same threadId = a reply.

    Bounce detection:
      Search INBOX for delivery-status notifications or mailer-daemon
      messages within the thread.

    Returns:
      ('replied',   message_data, updated_thread)  — contact or third party replied
      ('bounced',   message_data, updated_thread)  — bounce notification received
      ('silence',   None, None)                    — no change
    """
    gmail_thread_id = thread.get('gmail_thread_id')
    contact_email  = thread.get('contact_email')
    lead_name     = thread.get('lead_name', 'unknown')

    if not gmail_thread_id or not contact_email:
        return ('silence', None, None)

    # ── BOUNCE DETECTION ────────────────────────────────────────────────────
    # Bounce notifications are sent BY Gmail's mailer-daemon TO the sender
    # (hermancarter373@gmail.com) — NOT to the contact.
    # So we search INBOX for messages FROM mailer-daemon/postmaster that are
    # in this thread, and verify it was sent to the sender address.
    bounce_query = (
        f"from:(mailer-daemon OR postmaster) "
        f"subject:(\"Delivery Status\" OR \"Undelivered\" OR \"Returned\" OR \"failure\" OR \"not delivered\") "
        f"is:inbox"
    )
    try:
        bounce_candidates = gmail_list_messages(bounce_query, max_results=5)
        for meta in bounce_candidates:
            msg = gmail_get_message(meta['id'])
            if not msg:
                continue
            # Verify it's in the right thread and was addressed to the sender.
            # Bounces are sent to the envelope sender (Return-Path), which for
            # Gmail SPF-aligned sending is our sender address.
            msg_thread = msg.get('threadId', '')
            msg_to = msg.get('to', '')
            if msg_thread and msg_thread == gmail_thread_id and SENDER_EMAIL.lower() in msg_to.lower():
                log.warning(f"Thread {thread['id']}: BOUNCE detected — delivery failure for {contact_email}")
                return ('bounced', {
                    'message_id': msg['id'],
                    'thread_id':  msg_thread,
                    'from':       msg.get('from', ''),
                    'subject':    msg.get('subject', ''),
                    'internal_date': msg.get('internalDate'),
                    'detected_by': 'mailer_daemon_or_delivery_status',
                }, None)
    except Exception as e:
        log.warning(f"Thread {thread['id']}: bounce detection error: {e}")

    # ── REPLY DETECTION ────────────────────────────────────────────────────
    # Any INBOX message in this thread that is NOT from the sender is a reply.
    # We search by thread ID where possible; fall back to contact email.
    # Gmail search 'in:{threadId}' is not a standard operator, so we use
    # the threadId from the sent message and verify when fetching individual messages.
    #
    # Reply detection — search INBOX for messages FROM the contact.
    # The outbound email lives in SENT, not INBOX.
    # Any message FROM the contact in INBOX in the same threadId = a reply.
    reply_query = f"from:{contact_email} is:inbox"
    try:
        candidates = gmail_list_messages(reply_query, max_results=10)
        for meta in candidates:
            msg = gmail_get_message(meta['id'])
            if not msg:
                continue
            # Must be in the right Gmail thread
            msg_thread = msg.get('threadId', '')
            if not msg_thread or msg_thread != gmail_thread_id:
                continue
            # Must NOT be from the sender (sent messages are in SENT, not INBOX,
            # but guard anyway)
            msg_from = msg.get('from', '')
            if SENDER_EMAIL in msg_from:
                continue
            # Exclude bounce/complaint notifications
            subject_lower = (msg.get('subject') or '').lower()
            if any(kw in subject_lower for kw in ['bounce', 'undelivered', 'failure', 'delivery status']):
                continue
            # ── GUARD: Deduplicate — skip if this exact message_id was already
            # processed. Thread watcher runs every 3 hours. Without this guard,
            # the same reply is re-classified every cycle, creating duplicate
            # events, duplicate drafts, and spam Telegram alerts.
            _c = sqlite3.connect(DB)
            _c.row_factory = sqlite3.Row
            _seen = _c.execute("""
                SELECT 1 FROM outreach_events
                WHERE thread_id = ?
                  AND event_type IN ('negative_reply', 'positive_reply', 'neutral_reply', 'unclear_reply')
                  AND JSON_EXTRACT(event_data, '$.message_id') = ?
                LIMIT 1
            """, (thread['id'], msg['id'])).fetchone()
            _c.close()
            if _seen:
                log.debug(f"Thread {thread['id']}: message_id {msg['id']} already processed — skipping")
                continue
            # This is a real reply from the contact or a third party
            log.info(f"Thread {thread['id']}: REPLY detected from {msg_from} — subject: {msg.get('subject')}")
            return ('replied', {
                'message_id':     msg['id'],
                'thread_id':      msg_thread,
                'from':           msg_from,
                'subject':        msg.get('subject', ''),
                'internal_date':  msg.get('internalDate'),
                'snippet':        msg.get('snippet', ''),
            }, None)
    except Exception as e:
        log.warning(f"Thread {thread['id']}: reply detection error: {e}")

    return ('silence', None, None)


def process_thread(thread):
    """
    Process one active thread for reply/bounce events.
    Classifies all replies and routes to the appropriate handler.
    """
    event_type, message_data, updated_thread = poll_gmail_for_thread(thread)

    if event_type == 'silence':
        log.debug(f"Thread {thread['id']}: silence — no action")
        return False

    if event_type == 'bounced':
        log.info(f"Thread {thread['id']}: BOUNCE detected — suppressing lead")
        record_event(thread['id'], 'bounced', message_data)
        update_thread_state(thread['id'], thread_state='closed', close_reason='bounced')
        conn = sqlite3.connect(DB)
        conn.execute(
            "UPDATE leads SET outbound_state='suppressed', suppression_flag='bounce' WHERE id=?",
            (thread['lead_id'],)
        )
        conn.commit()
        conn.close()
        return True

    if event_type == 'replied':
        # ── GUARD: Deduplicate at process_thread level BEFORE classification.
        # This prevents duplicate classification, duplicate Telegram alerts, and
        # duplicate reply events even if the guard inside poll_gmail_for_thread
        # somehow allows a message through (belt-and-suspenders).
        _c = sqlite3.connect(DB)
        _c.row_factory = sqlite3.Row
        _seen = _c.execute("""
            SELECT 1 FROM outreach_events
            WHERE thread_id = ?
              AND event_type IN ('negative_reply', 'positive_reply', 'neutral_reply', 'unclear_reply')
              AND JSON_EXTRACT(event_data, '$.message_id') = ?
            LIMIT 1
        """, (thread['id'], message_data.get('message_id'))).fetchone()
        _c.close()
        if _seen:
            log.debug(f"Thread {thread['id']}: message_id {message_data.get('message_id')} already processed — skipping")
            return False

        # ── CLASSIFY THE REPLY ─────────────────────────────────────────────
        if not should_read_body(message_data.get('from', '')):
            log.info(f"Thread {thread['id']}: reply from excluded sender — skipping")
            return False

        body_text = get_reply_body(message_data.get('message_id'))
        classification = classify_reply(body_text)
        message_data['body_text'] = body_text
        log.info(f"Thread {thread['id']}: reply classified as '{classification}' — body preview: {body_text[:80]!r}")

        # ── POSITIVE ───────────────────────────────────────────────────────
        if classification == 'positive':
            log.info(f"Thread {thread['id']}: POSITIVE reply — alerting operator")
            enrichment = get_thread_enrichment(thread['id'])
            alert_text = build_positive_alert_text(thread['id'], message_data, classification, enrichment)
            write_reply_event(thread['id'], 'positive_reply', message_data, classification, enrichment)
            update_thread_state(thread['id'], thread_state='reply_received_positive')
            _telegram_notify(alert_text)
            _create_draft_for_review(thread['id'], message_data, classification, enrichment)
            return True

        # ── NEGATIVE ────────────────────────────────────────────────────────
        if classification == 'negative':
            log.info(f"Thread {thread['id']}: NEGATIVE reply — generating final response draft")
            enrichment = get_thread_enrichment(thread['id'])
            alert_text = build_negative_alert_text(thread['id'], message_data, classification, enrichment)
            write_reply_event(thread['id'], 'negative_reply', message_data, classification, enrichment)
            update_thread_state(thread['id'], thread_state='reply_received_negative')
            flag_lead_negative_reply(thread['lead_id'])
            _telegram_notify(alert_text)
            _create_draft_for_review(thread['id'], message_data, classification, enrichment)
            return True

        # ── NEUTRAL ────────────────────────────────────────────────────────
        if classification == 'neutral':
            log.info(f"Thread {thread['id']}: NEUTRAL reply (auto-reply/OOO) — logging and closing")
            record_event(thread['id'], 'neutral_reply', message_data)
            update_thread_state(thread['id'], thread_state='closed', close_reason='auto_reply')
            return True

        # ── UNCLEAR ───────────────────────────────────────────────────────
        # Unclear — log but don't auto-close. Alert operator to review manually.
        log.warning(f"Thread {thread['id']}: UNCLEAR reply — flagging for manual review")
        enrichment = get_thread_enrichment(thread['id'])
        alert_text = (
            f"[Thread {thread['id']}] Unclear reply received — needs manual review.\n"
            f"From: {message_data.get('from','?')}\n"
            f"Subject: {message_data.get('subject','?')}\n"
            f"Body preview: {body_text[:200]}"
        )
        _telegram_notify(alert_text)
        record_event(thread['id'], 'unclear_reply', message_data)
        return True

    log.debug(f"Thread {thread['id']}: unhandled event type {event_type}")
    return False


# ── Phase 2.5: Non-active threads with known gmail_message_id ──────────────

def get_replyable_non_active_threads():
    """
    Return threads that are NOT 'active' but have a gmail_message_id
    in their draft_created event — meaning an email was sent but the
    thread is stuck in drafting or another non-active state.

    These threads are reply-detectable via their gmail_message_id
    but were missed by Phase 2.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT
            t.id,
            t.lead_id,
            t.vertical,
            t.thread_state,
            t.current_email,
            l.name as lead_name,
            l.contact_email,
            l.outbound_state,
            l.enrichment_data,
            (
                SELECT JSON_EXTRACT(e.event_data, '$.gmail_message_id')
                FROM outreach_events e
                WHERE e.thread_id = t.id AND e.event_type = 'draft_created'
                ORDER BY e.id DESC LIMIT 1
            ) as gmail_message_id,
            (
                SELECT JSON_EXTRACT(e.event_data, '$.subject')
                FROM outreach_events e
                WHERE e.thread_id = t.id AND e.event_type = 'draft_created'
                ORDER BY e.id DESC LIMIT 1
            ) as subject,
            (
                SELECT JSON_EXTRACT(e.event_data, '$.to_email')
                FROM outreach_events e
                WHERE e.thread_id = t.id AND e.event_type = 'draft_created'
                ORDER BY e.id DESC LIMIT 1
            ) as to_email,
            t.stage
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state != 'active'
          AND t.thread_state NOT IN ('closed', 'off_market', 'pending_phone_only')
          AND EXISTS (
              SELECT 1 FROM outreach_events e
              WHERE e.thread_id = t.id AND e.event_type = 'draft_created'
                AND JSON_EXTRACT(e.event_data, '$.gmail_message_id') IS NOT NULL
          )
        ORDER BY t.id
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def poll_gmail_for_non_active_thread(thread):
    """
    Poll Gmail for replies on a non-active thread using gmail_message_id
    from the draft_created event.

    Same logic as poll_gmail_for_thread but accepts the gmail_message_id
    directly and uses the thread's contact_email.
    """
    gmail_thread_id = thread.get('gmail_message_id')
    contact_email = thread.get('contact_email') or thread.get('to_email')

    if not gmail_thread_id or not contact_email:
        return ('silence', None, None)

    # ── GUARD: Skip if this exact message_id was already processed ─────────
    # The reply_classifier phase2.5 runs every cron cycle on all non-active
    # threads. If a reply was already detected and recorded, re-running will
    # re-find the same message and create duplicate reply events + drafts.
    # Fix: check outreach_events for any reply event with this message_id.
    _conn = sqlite3.connect(DB)
    _conn.row_factory = sqlite3.Row
    _already = _conn.execute("""
        SELECT 1 FROM outreach_events
        WHERE thread_id = ?
          AND event_type IN ('negative_reply', 'positive_reply', 'neutral_reply', 'unclear_reply')
          AND JSON_EXTRACT(event_data, '$.message_id') = ?
        LIMIT 1
    """, (thread['id'], thread.get('gmail_message_id'))).fetchone()
    _conn.close()
    if _already:
        log.debug(f"Thread {thread['id']}: message_id {thread.get('gmail_message_id')} already processed — skipping")
        return ('silence', None, None)

    # Bounce detection
    bounce_query = (
        f"from:(mailer-daemon OR postmaster) "
        f"subject:(\"Delivery Status\" OR \"Undelivered\" OR \"Returned\" OR \"failure\" OR \"not delivered\") "
        f"is:inbox"
    )
    try:
        bounce_candidates = gmail_list_messages(bounce_query, max_results=5)
        for meta in bounce_candidates:
            msg = gmail_get_message(meta['id'])
            if not msg:
                continue
            msg_thread = msg.get('threadId', '')
            msg_to = msg.get('to', '')
            if msg_thread and msg_thread == gmail_thread_id and SENDER_EMAIL.lower() in msg_to.lower():
                return ('bounced', {
                    'message_id': msg['id'],
                    'thread_id': msg_thread,
                    'from': msg.get('from', ''),
                    'subject': msg.get('subject', ''),
                    'internal_date': msg.get('internalDate'),
                    'detected_by': 'mailer_daemon_or_delivery_status',
                }, None)
    except Exception as e:
        log.warning(f"Thread {thread['id']}: bounce detection error: {e}")

    # Reply detection— search INBOX for messages FROM the contact.
    # The outbound email lives in SENT, not INBOX.
    # Any message FROM the contact in INBOX in the same threadId = a reply.
    reply_query = f"from:{contact_email} is:inbox"
    try:
        candidates = gmail_list_messages(reply_query, max_results=10)
        for meta in candidates:
            msg = gmail_get_message(meta['id'])
            if not msg:
                continue
            msg_thread = msg.get('threadId', '')
            if not msg_thread or msg_thread != gmail_thread_id:
                continue
            msg_from = msg.get('from', '')
            if SENDER_EMAIL in msg_from:
                continue
            subject_lower = (msg.get('subject') or '').lower()
            if any(kw in subject_lower for kw in ['bounce', 'undelivered', 'failure', 'delivery status']):
                continue
            log.info(f"Thread {thread['id']}: REPLY detected from {msg_from} — subject: {msg.get('subject')}")
            return ('replied', {
                'message_id': msg['id'],
                'thread_id': msg_thread,
                'from': msg_from,
                'subject': msg.get('subject', ''),
                'internal_date': msg.get('internalDate'),
                'snippet': msg.get('snippet', ''),
            }, None)
    except Exception as e:
        log.warning(f"Thread {thread['id']}: reply detection error: {e}")

    return ('silence', None, None)


def process_non_active_thread(thread):
    """
    Process a non-active thread that may have received a reply.
    Same as process_thread but threads that are stuck in drafting/non-active
    need special handling — they may need to be moved to active first.
    """
    event_type, message_data, _ = poll_gmail_for_non_active_thread(thread)

    if event_type == 'silence':
        return False

    if event_type == 'bounced':
        log.info(f"Thread {thread['id']}: BOUNCE detected — suppressing lead")
        record_event(thread['id'], 'bounced', message_data)
        update_thread_state(thread['id'], thread_state='closed', close_reason='bounced')
        conn = sqlite3.connect(DB)
        conn.execute(
            "UPDATE leads SET outbound_state='suppressed', suppression_flag='bounce' WHERE id=?",
            (thread['lead_id'],)
        )
        conn.commit()
        conn.close()
        return True

    if event_type == 'replied':
        # Upgrade thread to active before applying reply logic
        update_thread_state(thread['id'], thread_state='active', current_email=1)
        log.info(f"Thread {thread['id']}: upgraded from {thread['thread_state']} to active — processing reply")

        # Re-query as active thread, injecting gmail_message_id as gmail_thread_id
        # since the thread record may not have it but the draft_created event does
        active_thread = dict(thread)
        active_thread['thread_state'] = 'active'
        if not active_thread.get('gmail_thread_id'):
            active_thread['gmail_thread_id'] = thread.get('gmail_message_id')
        return process_thread(active_thread)

    return False


# ── Telegram notification helper ────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get('HERMES_TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('HERMES_TELEGRAM_CHAT_ID', '1640746178')

def _telegram_notify(message):
    """
    Send a Telegram message to the operator.
    Uses HERMES_TELEGRAM_BOT_TOKEN and HERMES_TELEGRAM_CHAT_ID from env.
    Falls back silently if credentials not available.
    """
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN not set — skipping Telegram notification")
        print(f"[TELEGRAM WOULD SEND]: {message[:200]}")
        return

    import urllib.request
    import urllib.parse

    text = message[:4000]  # Telegram message limit
    data = urllib.parse.urlencode({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': text,
        'parse_mode': 'Markdown',
    }).encode()

    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=data,
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(f"Telegram send failed: {resp.status}")
    except Exception as e:
        log.warning(f"Telegram notification failed: {e}")
        print(f"[TELEGRAM ERROR]: {e}")
        print(f"[MESSAGE]: {message[:500]}")


# ── Gmail draft creation helper ────────────────────────────────────────────────

def _create_draft_for_review(thread_id, reply_data, classification, enrichment):
    """
    Create a Gmail draft in the operator's account for review.
    For positive: contact-aware response draft.
    For negative: Approach A final-ditch draft.
    For unclear: blank draft for the operator to write.
    Does NOT send — creates draft only.

    Guards against duplicate drafts: if this message_id already has a reply
    event recorded, do nothing. The caller (process_thread /
    process_non_active_thread) is also guarded, but this is a second layer
    in case the same message is re-detected via a different code path.
    """
    # ── GUARD: message_id already has a reply event → draft already created ──
    reply_msg_id = reply_data.get('message_id')
    if reply_msg_id:
        _c = sqlite3.connect(DB)
        _c.row_factory = sqlite3.Row
        _existing = _c.execute("""
            SELECT 1 FROM outreach_events
            WHERE thread_id = ?
              AND event_type IN ('negative_reply', 'positive_reply', 'neutral_reply', 'unclear_reply')
              AND JSON_EXTRACT(event_data, '$.message_id') = ?
            LIMIT 1
        """, (thread_id, reply_msg_id)).fetchone()
        _c.close()
        if _existing:
            log.debug(f"Thread {thread_id}: reply message_id {reply_msg_id} already processed — skipping draft creation")
            return

    if classification == 'positive':
        subject, body = generate_positive_response_draft(thread_id, reply_data, enrichment)
    elif classification == 'negative':
        subject, body = generate_negative_response_draft(thread_id, reply_data, enrichment)
    else:
        subject = f"Re: {reply_data.get('subject', 'your email')}"
        body = ""

    contact_email = enrichment.get('contact_email', '')
    if not contact_email:
        contact_email = reply_data.get('from', '').split('<')[-1].rstrip('>')

    # Use the Gmail thread from reply_data as primary; enrichment's gmail_thread_id
    # may be NULL in the DB for threads that were upgraded from non-active state.
    gmail_thread_id = reply_data.get('thread_id') or enrichment.get('gmail_thread_id')
    in_reply_to_id = reply_data.get('message_id') or gmail_thread_id

    # Build the MIME message directly — no subprocess, no temp files
    msg_root = MIMEText(body, 'plain', 'utf-8')
    msg_root['To'] = contact_email
    msg_root['Subject'] = subject
    if in_reply_to_id:
        msg_root['In-Reply-To'] = in_reply_to_id
        msg_root['References'] = in_reply_to_id

    raw = base64.urlsafe_b64encode(msg_root.as_bytes()).decode()
    draft_payload = {"message": {"raw": raw}}
    if gmail_thread_id:
        draft_payload["message"]["threadId"] = gmail_thread_id

    try:
        service = _get_gmail_service()
        result = service.users().drafts().create(userId='me', body=draft_payload).execute()
        draft_id = result.get('id', '')
        msg_id = result.get('message', {}).get('id', '')
        log.info(f"Thread {thread_id}: review draft created for {contact_email} (draft_id={draft_id})")
    except Exception as e:
        log.warning(f"Thread {thread_id}: draft creation error: {e}")



# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    log.info("USP Thread Watcher starting")

    # ── PHASE 1: pending_approval threads (detect human Send) ─────────────
    pending = get_pending_approval_threads()
    if pending:
        log.info(f"Phase 1: {len(pending)} pending_approval thread(s)")
        for t in pending:
            process_pending_approval_thread(t)
    else:
        log.info("Phase 1: no pending_approval threads")

    # ── PHASE 2: active threads (detect + classify replies) ──────────────
    active = get_active_threads()
    if active:
        log.info(f"Phase 2: {len(active)} active thread(s)")
        for t in active:
            process_thread(t)
    else:
        log.info("Phase 2: no active threads")

    # ── PHASE 2.5: non-active threads that were sent (reply detection) ────
    replyable = get_replyable_non_active_threads()
    if replyable:
        log.info(f"Phase 2.5: {len(replyable)} non-active replyable thread(s)")
        for t in replyable:
            process_non_active_thread(t)
    else:
        log.info("Phase 2.5: no non-active replyable threads")

    log.info("Thread Watcher complete")


if __name__ == "__main__":
    run()
