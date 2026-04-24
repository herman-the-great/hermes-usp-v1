#!/usr/bin/env python3
"""
USP Reply Classifier — classifies inbound lead replies into response categories.

Security model:
  - Static keyword/phrase matching only. No LLM. No external API calls.
  - Gmail searches are always scoped: to:{contact_email} is:inbox
  - From-header filtering excludes: hermancarter, mailer-daemon, no-reply, noreply,
    verify, notification, alert, google, github, oracle, formspree, accounts
  - Body is read only after metadata filtering passes
  - Classification result + metadata only — full body never stored beyond immediate use
  - All processing stays within ~/.hermes/Hermes-USP-v1/

Response categories:
  positive  — lead wants to talk, asks pricing, says yes/maybe
  negative  — lead is not interested, asks to stop, remove
  neutral   — OOO, auto-reply, generic acknowledgment
  unclear   — anything else
"""

import sqlite3, json, re
from datetime import datetime

DB = "/home/cortana/.hermes/Hermes-USP-v1/usp.db"

# ── Static classification patterns ──────────────────────────────────────────

POSITIVE_PATTERNS = [
    # Direct interest signals
    re.compile(r'\byes\b', re.I),
    re.compile(r"\blet's talk\b", re.I),
    re.compile(r"\blets talk\b", re.I),
    re.compile(r'\bschedule\b', re.I),
    re.compile(r'\bpricing\b', re.I),
    re.compile(r'\bcost\b', re.I),
    re.compile(r'\bfee\b', re.I),
    re.compile(r'\brate\b', re.I),
    re.compile(r'\bquote\b', re.I),
    re.compile(r'\binterested\b', re.I),
    re.compile(r'\bavailable\b', re.I),
    re.compile(r'\bcall me\b', re.I),
    re.compile(r'\bcall us\b', re.I),
    re.compile(r'\bphone call\b', re.I),
    re.compile(r'\bwhen can you\b', re.I),
    re.compile(r'\bwhen are you\b', re.I),
    re.compile(r'\bset up a\b', re.I),
    re.compile(r'\bbook a\b', re.I),
    re.compile(r'\bschedule a\b', re.I),
    re.compile(r'\bget on a\b', re.I),
    re.compile(r'\bget on the\b', re.I),
    re.compile(r'\bcalendar\b', re.I),
    re.compile(r'\bmeet(ing|up)?\b', re.I),
    re.compile(r'\bdemo\b', re.I),
    re.compile(r'\btry it\b', re.I),
    re.compile(r'\bhow much\b', re.I),
    re.compile(r'\bsend me (the|your|more|a)\b', re.I),
    re.compile(r"\bsounds good\b", re.I),
    re.compile(r"\bcount me in\b", re.I),
    re.compile(r"\bi'm interested\b", re.I),
    re.compile(r"\bi am interested\b", re.I),
    re.compile(r"\bwe are interested\b", re.I),
    re.compile(r"\bwe're interested\b", re.I),
    re.compile(r"\bwould like to\b", re.I),
    re.compile(r"\bwant to learn\b", re.I),
    re.compile(r"\bneed to talk\b", re.I),
    re.compile(r"\blet's discuss\b", re.I),
    re.compile(r"\blets discuss\b", re.I),
    re.compile(r"\bopen to\b", re.I),
    re.compile(r"\bexploring\b", re.I),
    re.compile(r"\blearn more\b", re.I),
]

NEGATIVE_PATTERNS = [
    # Direct rejection signals
    re.compile(r'\bnot interested\b', re.I),
    re.compile(r"\bnot inter(?:e)?sted\b", re.I),
    re.compile(r'\bremove me\b', re.I),
    re.compile(r'\bstop emailing\b', re.I),
    re.compile(r'\bdo not contact\b', re.I),
    re.compile(r'\bdo not call\b', re.I),
    re.compile(r'\bplease unsubscribe\b', re.I),
    re.compile(r'\bunsubscribe\b', re.I),
    re.compile(r'\bnot looking\b', re.I),
    re.compile(r"\bnot at this time\b", re.I),
    re.compile(r"\bnot right now\b", re.I),
    re.compile(r"\bnot for us\b", re.I),
    re.compile(r"\bnot a fit\b", re.I),
    re.compile(r"\bwe aren't\b", re.I),
    re.compile(r"\bwe're not\b", re.I),
    re.compile(r"\bnot a priority\b", re.I),
    re.compile(r"\bno thank you\b", re.I),
    re.compile(r"\bno thanks\b", re.I),
    re.compile(r"\bdelete (this|our|my)\b", re.I),
    re.compile(r"\blose this\b", re.I),
    re.compile(r"\bdon't bother\b", re.I),
    re.compile(r"\bpush back\b", re.I),
    re.compile(r"\bblocked\b", re.I),
    re.compile(r"\bno longer\b", re.I),
    re.compile(r"\bwent with\b", re.I),  # already chose someone else
    re.compile(r"\balready (have|use|working)\b", re.I),
    re.compile(r"\busing (something|another|a different)\b", re.I),
    re.compile(r"\bthanks but no\b", re.I),
    re.compile(r"\bnot needed\b", re.I),
    re.compile(r"\bdon't need\b", re.I),
    re.compile(r"\bdo not need\b", re.I),
    re.compile(r"\bcan't use\b", re.I),
    re.compile(r"\bwon't work\b", re.I),
    re.compile(r"\btoo expensive\b", re.I),
    re.compile(r"\bno budget\b", re.I),
    re.compile(r"\bout of budget\b", re.I),
    re.compile(r"\bnot in budget\b", re.I),
    re.compile(r"\bgoing in a different\b", re.I),
]

NEUTRAL_PATTERNS = [
    # Auto-reply / OOO / generic acknowledgment
    re.compile(r'\bout of office\b', re.I),
    re.compile(r'\boo[of]\b', re.I),
    re.compile(r'\bauto-?reply\b', re.I),
    re.compile(r'\bautoreply\b', re.I),
    re.compile(r'\bautomatic reply\b', re.I),
    re.compile(r'\bthank you for contacting\b', re.I),
    re.compile(r'\bwe have received\b', re.I),
    re.compile(r'\bwill review\b', re.I),
    re.compile(r'\bcurrently unavailable\b', re.I),
    re.compile(r'\bwill return\b', re.I),
    re.compile(r'\bback in\b', re.I),
    re.compile(r'\baway from\b', re.I),
    re.compile(r'\bholiday\b', re.I),
    re.compile(r'\bclosed for\b', re.I),
    re.compile(r'\bbusiness hours\b', re.I),
    re.compile(r'\bwe will (be|get back|respond)\b', re.I),
    re.compile(r'\bexpect a delay\b', re.I),
    re.compile(r'\bunavailable until\b', re.I),
    re.compile(r'\b寄给您\b', re.I),  # chinese OOO
    re.compile(r'\bthank you for reaching out.*will review', re.I),
]

# ── From-header exclusion list ────────────────────────────────────────────────
# Any message where the From header matches one of these is excluded immediately.
# Prevents reading body of system messages, notifications, bounce alerts.
FROM_EXCLUDE_PATTERNS = [
    re.compile(r'hérmancarter', re.I),  # own address
    re.compile(r'mailer-?daemon', re.I),
    re.compile(r'no-?reply@', re.I),
    re.compile(r'noreply@', re.I),
    re.compile(r'no.?reply', re.I),
    re.compile(r'verify@', re.I),
    re.compile(r'verification@', re.I),
    re.compile(r'notification@', re.I),
    re.compile(r'notifier@', re.I),
    re.compile(r'alert@', re.I),
    re.compile(r'google', re.I),
    re.compile(r'github', re.I),
    re.compile(r'oracle', re.I),
    re.compile(r'formspree', re.I),
    re.compile(r'accounts@', re.I),
    re.compile(r'aws-?ses', re.I),
    re.compile(r'support@', re.I),
    re.compile(r'帮你找到', re.I),  # chinese spam
    re.compile(r'帮助中心', re.I),   # chinese help center
]


def classify_reply(body_text):
    """
    Classify a reply body into: positive | negative | neutral | unclear.

    Pure function — no I/O, no external calls.
    """
    if not body_text or not body_text.strip():
        return 'unclear'

    # Neutral must be checked before negative (OOO contains "not")
    for pattern in NEUTRAL_PATTERNS:
        if pattern.search(body_text):
            return 'neutral'

    for pattern in NEGATIVE_PATTERNS:
        if pattern.search(body_text):
            return 'negative'

    for pattern in POSITIVE_PATTERNS:
        if pattern.search(body_text):
            return 'positive'

    return 'unclear'


def should_read_body(from_header):
    """
    Returns True only if the From header passes security filtering.
    Prevents reading body of system/notification emails.
    """
    if not from_header:
        return False
    for pattern in FROM_EXCLUDE_PATTERNS:
        if pattern.search(from_header):
            return False
    return True


def get_reply_body(msg_id, gmail_script=None, sys_executable=None):
    """
    Read the full plaintext body of a Gmail message.
    Uses build_service directly for raw MIME access.
    Returns empty string on failure.
    """
    try:
        import sys as _sys
        _sys.path.insert(0, '/home/cortana/.hermes/skills/productivity/google-workspace/scripts')
        from google_api_usp import build_service, get_credentials
        service = build_service('gmail', 'v1')
        msg = service.users().messages().get(userId='me', id=str(msg_id), format='raw').execute()
        raw_b64 = msg.get('raw', '')
        if not raw_b64:
            return ''
        import base64, email as email_lib
        decoded = base64.urlsafe_b64decode(raw_b64 + '==')
        msg_obj = email_lib.message_from_bytes(decoded)
        if msg_obj.is_multipart():
            for part in msg_obj.walk():
                if part.get_content_type() == 'text/plain':
                    charset = part.get_content_charset() or 'utf-8'
                    return part.get_payload(decode=True).decode(charset, errors='replace').strip()
        else:
            charset = msg_obj.get_content_charset() or 'utf-8'
            return msg_obj.get_payload(decode=True).decode(charset, errors='replace').strip()
        return ''
    except Exception:
        return ''


# ── Thread enrichment helpers ────────────────────────────────────────────────

def get_thread_enrichment(thread_id):
    """
    Return enrichment_data dict for the lead on a thread.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT l.enrichment_data, l.name, l.contact_email, l.contact_phone,
               l.assigned_vertical, t.vertical, t.stage, t.current_email,
               t.next_followup, t.gmail_thread_id
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.id = ?
    """, (thread_id,)).fetchone()
    conn.close()
    if not row:
        return {}
    result = dict(row)
    if result.get('enrichment_data'):
        try:
            result['enrichment_data'] = json.loads(result['enrichment_data'])
        except Exception:
            result['enrichment_data'] = {}
    else:
        result['enrichment_data'] = {}
    return result


def get_sequence_stage_label(stage, current_email):
    """
    Convert numeric stage + current_email to human-readable sequence stage name.
    """
    if current_email == 1 and stage == 1:
        return "Initial Email"
    stage_map = {
        (2, 1): "Follow-up Email 1",
        (3, 1): "Follow-up Email 2",
        (4, 1): "Follow-up Email 3",
        (5, 1): "Follow-up Email 4",
    }
    return stage_map.get((stage, current_email), f"Stage {stage}, Email {current_email}")


def build_positive_alert_text(thread_id, reply_data, classification, enrichment):
    """
    Build the full Telegram alert text for a positive reply.
    Full words, no internal tags.
    """
    lead_name = enrichment.get('name', 'Unknown Lead')
    contact_email = enrichment.get('contact_email', 'No email')
    vertical = enrichment.get('assigned_vertical') or enrichment.get('vertical', 'Unknown')
    stage_label = get_sequence_stage_label(
        enrichment.get('stage', 1),
        enrichment.get('current_email', 1)
    )
    next_followup = enrichment.get('next_followup', 'Not scheduled')
    gmail_thread_id = enrichment.get('gmail_thread_id', 'Unknown')
    thread_url = f"https://mail.google.com/mail/u/0/#inbox/{gmail_thread_id}" if gmail_thread_id else "N/A"

    # Build enrichment summary
    ed = enrichment.get('enrichment_data', {})
    website = ed.get('website', 'Not available')
    phone = ed.get('phone', enrichment.get('contact_phone', 'Not available'))
    address = ed.get('address', 'Not available')
    rating = ed.get('rating', 'Not available')
    biz_status = ed.get('business_status', 'Not available')

    # Review snippet
    reviews = ed.get('reviews', [])
    review_snippet = ""
    if reviews:
        r = reviews[0]
        review_snippet = f"  Most recent Google review: \"{r.get('author_name','Anonymous')}\" — {r.get('text','')[:150]}"

    # Web contacts
    wc = ed.get('web_contacts', {})
    web_contact_lines = []
    for page, data in wc.items():
        named = data.get('named_people', [])
        if named:
            for n in named[:2]:
                web_contact_lines.append(f"  {page}: {n[0]} ({n[1]})" if len(n) > 1 else f"  {page}: {n[0]}")

    body_text = reply_data.get('body_text', reply_data.get('snippet', 'No body text available')).strip()
    body_display = body_text[:500] + ("..." if len(body_text) > 500 else "")

    # Suggested direction
    suggested = _suggest_reply_direction(classification, vertical, body_text)

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        "POSITIVE REPLY RECEIVED",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Lead: {lead_name}",
        f"Company: {lead_name}",
        f"Vertical: {vertical}",
        f"Email: {contact_email}",
        f"Phone: {phone}",
        f"Subject: {reply_data.get('subject', 'N/A')}",
        f"Sequence stage: {stage_label}",
        f"Last email sent: {next_followup}",
        f"Gmail thread: {thread_url}",
        "",
        "Reply received:",
        f"\"{body_display}\"",
        "",
        "Lead enrichment data:",
        f"  Website: {website}",
        f"  Phone: {phone}",
        f"  Address: {address}",
        f"  Google rating: {rating}",
        f"  Business status: {biz_status}",
    ]

    if review_snippet:
        lines.append(review_snippet)
    if web_contact_lines:
        lines.append("  Named contacts found:")
        lines.extend(web_contact_lines[:5])

    lines.extend([
        "",
        "Suggested reply direction:",
        suggested,
        "",
        "A context-aware draft is ready for your review in Gmail.",
    ])

    return "\n".join(lines)


def build_negative_alert_text(thread_id, reply_data, classification, enrichment):
    """
    Build the Telegram alert text for a negative reply.
    Non-urgent, informative.
    """
    lead_name = enrichment.get('name', 'Unknown Lead')
    contact_email = enrichment.get('contact_email', 'No email')
    vertical = enrichment.get('assigned_vertical') or enrichment.get('vertical', 'Unknown')
    gmail_thread_id = enrichment.get('gmail_thread_id', 'Unknown')

    body_text = reply_data.get('body_text', reply_data.get('snippet', '')).strip()
    body_display = body_text[:300] + ("..." if len(body_text) > 300 else "")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        "NEGATIVE REPLY RECEIVED",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Lead: {lead_name}",
        f"Company: {lead_name}",
        f"Email: {contact_email}",
        f"Vertical: {vertical}",
        f"Subject: {reply_data.get('subject', 'N/A')}",
        "",
        "Reply received:",
        f"\"{body_display}\"",
        "",
        "A final-ditch response draft is ready for your review in Gmail.",
        "Thread has been flagged. No further automated outreach will occur.",
    ]
    return "\n".join(lines)


def _suggest_reply_direction(classification, vertical, body_text):
    """
    Internal: suggest a reply direction based on classification + vertical.
    """
    if classification == 'positive':
        return (
            "Lead has expressed interest. Reference their specific reply, "
            "acknowledge what they said, and move directly toward a 15 or 20-minute call. "
            "Offer two specific time slots. Keep it under 4 sentences. "
            "The goal is to get a calendar booking, not to explain your service."
        )
    elif classification == 'negative':
        return (
            "Lead has declined. Offer one free piece of actionable value related to their vertical. "
            "Frame it as a resource, not a pitch. Close cleanly — do not ask for a response. "
            "One email only. No follow-up sequence."
        )
    else:
        return (
            "Reply type is unclear. Review the body and use your judgment on whether "
            "to respond or wait for a clearer signal."
        )


# ── Negative response draft generator (Approach A) ────────────────────────────

APPROACH_A_TEMPLATES = {
    "home_services": (
        "Thanks for getting back to me, we completely respect your time and your response.\n\n"
        "One thing we have noticed a lot with plumbing and HVAC companies in Denver: "
        "service calls that get logged in the system but never actually followed up "
        "on within 24 hours. Small issue becomes a frustrated customer and "
        "potentially a bad review.\n\n"
        "If you're not already using something structured to close that loop, a "
        "simple shared task board between dispatch and field techs can cut missed "
        "follow-ups significantly.\n\n"
        "That's the kind of thing we'd happy to share more about if you ever want to "
        "compare notes. No pressure at all — just something we've seen work. "
        "Feel free to let us know if you change your mind or may have any questions "
        "about how we can help you run more efficiently.\n\n"
        "Once again, we'd like to say thank you for your response. We know how "
        "valuable your time is.\n\n"
        "Best,\n"
        "Herman"
    ),
    "accounting_bookkeeping": (
        "Hi {first_name},\n\n"
        "Thanks for getting back to me — I completely respect your time.\n\n"
        "One thing I see a lot with accounting firms here in Colorado: "
        "tax season creates a massive backlog of client follow-ups that never quite "
        "get back to normal by Q2. Clients who had urgent needs in March are still "
        "waiting for a proper check-in.\n\n"
        "If you're not already using something to systematize that follow-up rhythm, "
        "even a simple client health-check workflow can reclaim a lot of lost time "
        "and keep clients from drifting toward cheaper alternatives.\n\n"
        "That's the kind of thing I'd happy to share more about if you ever want to "
        "compare notes. No pressure at all — just something I've seen work.\n\n"
        "Best,\n"
        "Herman"
    ),
    "estate_planning_probate": (
        "Hi {first_name},\n\n"
        "Thanks for getting back to me — I completely respect your time.\n\n"
        "One thing I see a lot with estate planning firms: "
        "initial consultations go well, but the gap between the first meeting and "
        "getting the full engagement signed often stretches too long — and families "
        "end up calling around for second opinions in that window.\n\n"
        "A simple client momentum tracker that keeps every open engagement "
        "moving forward weekly can close that gap without adding admin overhead.\n\n"
        "That's the kind of thing I'd happy to share more about if you ever want to "
        "compare notes. No pressure at all — just something I've seen work.\n\n"
        "Best,\n"
        "Herman"
    ),
    "default": (
        "Hi {first_name},\n\n"
        "Thanks for getting back to me — I completely respect your time.\n\n"
        "One thing I often see in businesses like yours: "
        "there's a gap between what gets promised to clients and what actually "
        "gets delivered — usually not because of bad intentions, but because "
        "there's no structured system closing that loop.\n\n"
        "That's the kind of thing I'd happy to share more about if you ever want to "
        "compare notes. No pressure at all — just something I've seen work.\n\n"
        "Best,\n"
        "Herman"
    ),
}


def generate_negative_response_draft(thread_id, reply_data, enrichment):
    """
    Generate the Approach A negative response draft.
    Returns (subject, body_text) — NOT saved to Gmail, returned for review.
    """
    lead_name = enrichment.get('name', '')
    vertical = enrichment.get('assigned_vertical') or enrichment.get('vertical', 'default')

    # Extract first name
    first_name = "there"
    if lead_name:
        parts = lead_name.strip().split()
        if parts:
            first_name = parts[0]

    # Get template
    template = APPROACH_A_TEMPLATES.get(vertical, APPROACH_A_TEMPLATES["default"])
    body = template.format(first_name=first_name)

    # Subject — keep thread alive with Re:
    original_subject = reply_data.get('subject', '')
    if original_subject.lower().startswith('re:'):
        subject = original_subject
    elif original_subject:
        subject = f"Re: {original_subject}"
    else:
        subject = "Thanks for getting back to me"

    return subject, body


# ── Positive response draft generator ────────────────────────────────────────

def generate_positive_response_draft(thread_id, reply_data, enrichment):
    """
    Generate a context-aware positive response draft for the operator to review.
    Uses enrichment data + reply content + thread history.
    Returns (subject, body_text) — NOT auto-sent.
    """
    lead_name = enrichment.get('name', '')
    contact_email = enrichment.get('contact_email', '')
    vertical = enrichment.get('assigned_vertical') or enrichment.get('vertical', '')
    ed = enrichment.get('enrichment_data', {})

    # Extract first name
    first_name = "there"
    if lead_name:
        parts = lead_name.strip().split()
        if parts:
            first_name = parts[0]

    # Try to get a named contact from web_contacts
    named_person = ""
    web_contacts = ed.get('web_contacts', {})
    for page, data in web_contacts.items():
        named_people = data.get('named_people', [])
        if named_people:
            named_person = named_people[0][0] if named_people else ""
            break

    # Determine greeting
    if named_person:
        greeting = f"Hi {named_person},"
    else:
        greeting = f"Hi {first_name},"

    # Build body — reference reply
    reply_text = reply_data.get('body_text', reply_data.get('snippet', '')).strip()
    reply_snippet = reply_text[:200] if reply_text else ""

    # Vertical-specific CTA slot suggestions
    if vertical == 'home_services':
        slots = "Tuesday at 2pm, Wednesday at 10am, or Thursday at 3pm"
    elif vertical == 'accounting_bookkeeping':
        slots = "Wednesday at 11am, Thursday at 2pm, or Friday at 9am"
    elif vertical == 'estate_planning_probate':
        slots = "Monday at 1pm, Tuesday at 10am, or Wednesday at 3pm"
    else:
        slots = "Tuesday or Wednesday this week"

    body_lines = [
        f"{greeting}",
        "",
    ]

    if reply_snippet:
        body_lines.append(f"Thanks for getting back to me — I read your note and appreciate you taking the time to respond.")
        body_lines.append("")
    else:
        body_lines.append(f"Thanks for getting back to me. I'd love to explore whether there's a fit here.")
        body_lines.append("")

    body_lines.extend([
        f"I've got {slots} available for a quick 15-minute call if you're open to it. "
        f"Just reply with what works and I'll send a calendar invite right over.",
        "",
        f"No pressure if the timing isn't right — I just didn't want to leave you hanging.",
        "",
        "Best,",
        "Herman Carter",
        "Partnership and Growth Consultant",
        "USP LLC",
    ])

    body = "\n".join(body_lines)

    # Subject
    original_subject = reply_data.get('subject', '')
    if original_subject.lower().startswith('re:'):
        subject = original_subject
    elif original_subject:
        subject = f"Re: {original_subject}"
    else:
        subject = "Let's talk"

    return subject, body


# ── DB write helpers ─────────────────────────────────────────────────────────

def write_reply_event(thread_id, event_type, reply_data, classification, enrichment):
    """
    Write a reply event to the outreach_events table.
    """
    lead_name = enrichment.get('name', '')
    contact_email = enrichment.get('contact_email', '')
    vertical = enrichment.get('assigned_vertical') or enrichment.get('vertical', '')
    stage_label = get_sequence_stage_label(
        enrichment.get('stage', 1),
        enrichment.get('current_email', 1)
    )

    conn = sqlite3.connect(DB)
    conn.execute("""
        INSERT INTO outreach_events (thread_id, event_type, event_data)
        VALUES (?, ?, ?)
    """, (thread_id, event_type, json.dumps({
        "message_id": reply_data.get('message_id'),
        "thread_id": reply_data.get('thread_id'),
        "from": reply_data.get('from'),
        "subject": reply_data.get('subject'),
        "snippet": reply_data.get('snippet', ''),
        "body_text": reply_data.get('body_text', '')[:1000],  # truncate to 1000 chars
        "classification": classification,
        "lead_name": lead_name,
        "contact_email": contact_email,
        "vertical": vertical,
        "sequence_stage": stage_label,
        "reply_received_at": datetime.utcnow().isoformat(),
        "draft_created_for_review": True,
    })))
    conn.commit()
    conn.close()


def flag_lead_negative_reply(lead_id):
    """
    Flag the lead's outbound_state for negative reply pending.
    Does NOT suppress the lead — stays reachable.
    """
    conn = sqlite3.connect(DB)
    conn.execute("""
        UPDATE leads
        SET outbound_state = 'negative_reply_pending',
            suppression_flag = NULL
        WHERE id = ?
    """, (lead_id,))
    conn.commit()
    conn.close()
