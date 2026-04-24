#!/usr/bin/env python3
"""
Phase 3: Follow-Up Draft Generator — Thread-Aware Cold Outreach

Generates context-aware follow-up drafts for active threads that have passed
their next_followup date. Each follow-up is written ON THE SAME Gmail thread
as the original email and all previous follow-ups, referencing prior content.

KEY DIFFERENCES FROM INITIAL DRAFT GENERATOR (usp_draft_generator.py):
- Follow-ups do NOT attach one-pagers (initial drafts do)
- Follow-ups use "Re:" subject prefix (not "quick follow-up:")
- Follow-ups have more direct CTAs focused on booking time
- Follow-ups include ALL prior email content in thread context for Ollama

State rules:
  - Thread must be 'active' with next_followup <= today (or force=True)
  - Follow-up # = sent event count in DB + 1  (sent_count=1 → FU2 → next_followup +2 biz days)
  - After human sends FU#N: thread_watcher increments sent count, lifecycle manager
    re-computes next_followup for FU#N+1
  - Maximum 3 follow-ups per thread (FU2, FU3, FU4), then thread → closed
  - If prospect replies: operator marks reply → thread → closed

Gmail thread continuity:
  - Creates draft as reply to existing Gmail thread (References/In-Reply-To)
  - Ollama prompt receives full thread context: all previous email subjects,
    send dates, and body snippets

Usage:
  python3 usp_followup_generator.py                  # all verticals, ready threads
  python3 usp_followup_generator.py --dry-run        # show what would run
  python3 usp_followup_generator.py --vertical home_services  # single vertical
  python3 usp_followup_generator.py --force         # ignore next_followup date
"""
import json, os, sqlite3, sys, argparse, re, subprocess
from datetime import datetime, timedelta, date

DB   = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")
ROOT = os.path.expanduser("~/.hermes/Hermes-USP-v1")

SENDER_NAME  = "Herman Carter"
SENDER_TITLE = "Partnership and Growth Consultant"
SENDER_EMAIL = "hermancarter373@gmail.com"

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL    = "llama3.2:3b"

MAX_FOLLOWUPS = 3   # FU2, FU3, FU4 → max 4 total emails per thread

GMAIL_SCRIPT = "/home/cortana/.hermes/skills/productivity/google-workspace/scripts/google_api_usp.py"


# ── Config helpers ────────────────────────────────────────────────────────────

def get_config(key, default=None):
    conn = sqlite3.connect(DB)
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return row[0]
    return default


# ── Business day math ───────────────────────────────────────────────────────

def get_business_days(start_date, offset):
    """Add offset weekdays (Mon-Fri) to start_date."""
    d = start_date
    added = 0
    while added < offset:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


def next_followup_after(sent_date, current_stage):
    """Return next follow-up date given the stage we're on."""
    offsets = {1: 2, 2: 4, 3: 6}
    offset = offsets.get(current_stage, 6)
    return get_business_days(sent_date, offset)


# ── Gmail API helpers ────────────────────────────────────────────────────────

def gmail_get_thread_messages(gmail_thread_id, max_msgs=10):
    """
    Fetch all messages in a Gmail thread.
    Returns list of dicts: {id, from, to, subject, date, snippet, labels}
    """
    try:
        result = subprocess.run(
            [sys.executable, GMAIL_SCRIPT, "gmail", "get-thread", gmail_thread_id],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        # get-thread returns a list directly
        if isinstance(data, list):
            return data[:max_msgs]
        # Some API wrappers return {messages: [...]}
        if isinstance(data, dict) and "messages" in data:
            return data["messages"][:max_msgs]
        return []
    except Exception:
        return []


def gmail_create_followup_draft(to_email, subject, body_plain,
                                  gmail_thread_id, in_reply_to):
    """
    Create a Gmail draft as a reply to an existing thread.
    Sets References and In-Reply-To headers for proper thread nesting.
    NO inline images for follow-ups — one-pagers are for initial outreach only.
    """
    cmd = [
        sys.executable, GMAIL_SCRIPT,
        "gmail", "create-draft",
        "--to", to_email,
        "--subject", subject,
        "--body", body_plain,
        "--thread-id", gmail_thread_id,
        "--in-reply-to", in_reply_to,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return {"status": "error", "stderr": result.stderr}
    try:
        return {"status": "ok", "data": json.loads(result.stdout)}
    except json.JSONDecodeError:
        return {"status": "error", "stdout": result.stdout, "stderr": result.stderr}


# ── Follow-up CTA strategy ───────────────────────────────────────────────────

def build_followup_cta(fu_number, tier, first_name):
    """
    Return (cta_hook, cta_barrier, cta_directive) for this follow-up.
    
    CTA gets progressively more direct across follow-ups:
    - FU#2: Soft curiosity — acknowledges previous email, low pressure
    - FU#3: Direct booking — asks for 10-min call directly, no escape hatch
    - FU#4: Final close — urgency without being pushy, easy "no" to close thread
    
    The full CTA text goes in the email body. The directive controls prompt tone.
    """
    # FU#2: Soft opener + low barrier
    fu2 = {
        "hook": "if you saw my last note",
        "barrier": "just hit reply and I'll work around your schedule — no cost, no commitment",
        "directive": "warm and curious — acknowledge you sent something before without being apologetic. Keep it short."
    }
    
    # FU#3: Direct — goal is to book the 10-minute call
    fu3 = {
        "hook": "if a 10-minute call makes sense",
        "barrier": "send me a time that works and I'll hold it — no forms, no sales process",
        "directive": "more direct — the prior emails haven't gotten a response. Add a genuinely new observation or angle. End with a clear, single ask: book a 10-minute call."
    }
    
    # FU#4: Final — give them an easy out or a clear next step
    fu4 = {
        "hook": "if the answer is no, just say so and I'll close this out",
        "barrier": "but if there's any interest at all, reply here and I'll get something on your calendar",
        "directive": "brief and low-pressure — acknowledge this is the last email. Give them an easy out or a simple next step. Accept that most won't reply."
    }
    
    options = {2: fu2, 3: fu3, 4: fu4}
    return options.get(fu_number, fu3)


def build_followup_subject(prior_subjects=None):
    """
    Build subject line for follow-up.
    Uses 'Re: <original subject>' pattern — Gmail threads by subject match.
    prior_subjects: list of subject strings already used (to avoid repeats).
    """
    if prior_subjects is None:
        prior_subjects = []
    
    # Get the most recent subject (original sent email)
    anchor = prior_subjects[-1] if prior_subjects else "workflow improvement"
    
    # Strip Re:/FW: prefix for clean anchor
    anchor = re.sub(r"^(Re:\s*|FW:\s*|\"Re: Re: \s*)+", "", anchor, flags=re.IGNORECASE).strip()
    
    # Return simple Re: pattern — Gmail handles threading
    return f"Re: {anchor}"


# ── Ollama local model ──────────────────────────────────────────────────────

def call_local_model(prompt, max_tokens=400):
    """Generate text via Ollama local model."""
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "system": "You are a precise business writer. Output only the requested text — no preamble, no meta-commentary, no instructions.",
        "options": {"num_predict": max_tokens},
        "stream": False,
    }
    try:
        import urllib.request
        req = urllib.request.Request(
            OLLAMA_ENDPOINT,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=45) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("response", "").strip()
    except Exception as e:
        print(f"[OLLAMA ERROR] {e}", file=sys.stderr)
        return ""


# ── Follow-up body generation ───────────────────────────────────────────────

def build_followup_body(lead, offer, enrichment_signals, thread_context,
                        fu_number, tier, first_name):
    """
    Build a follow-up email body using full thread context.
    
    thread_context: list of {subject, date, snippet, from, labels} for prior messages.
    fu_number: which follow-up this is (2, 3, or 4).
    
    KEY RULES:
    - NO one-pagers / inline images for follow-ups
    - NO "(5 stars on Google)" as a filler or opener
    - CTA must be the LAST paragraph before signoff
    - New observation/angle for FU#3+ (not a repeat of FU#2)
    """
    firm_name   = lead["lead_name"]
    vertical    = lead["assigned_vertical"]
    v_short     = vertical.replace("_", " ").title()
    
    cta_data    = build_followup_cta(fu_number, tier, first_name)
    cta_hook    = cta_data["hook"]
    cta_barrier = cta_data["barrier"]
    tone_dir    = cta_data["directive"]
    
    # Greeting
    if tier in ("A", "B") and first_name:
        greeting = f"Hi {first_name},"
    else:
        greeting = f"Hi {firm_name} team,"

    # Build thread context text for Ollama
    prior_emails_text = ""
    if thread_context:
        email_lines = []
        for i, msg in enumerate(thread_context, 1):
            subj   = msg.get("subject", "")
            date_s = msg.get("date", "")
            snippet = msg.get("snippet", "")[:300]
            email_lines.append(f"  Email {i}: [{date_s}] {subj}")
            email_lines.append(f"    {snippet}")
        prior_emails_text = "\n" + "\n".join(email_lines)

    # Vertical-specific operational context
    VERTICAL_CONTEXT = {
        "estate_planning_probate": [
            "Estate planning and probate firms typically face specific operational friction: ",
            "client intake that stalls between first contact and signed engagement, ",
            "document gathering that requires consistent client follow-up, ",
            "multiple touchpoints across executors, beneficiaries, and attorneys, ",
            "and internal handoffs where things fall through before matters close."
        ],
        "accounting_bookkeeping": [
            "Accounting and bookkeeping firms typically face specific operational friction: ",
            "client document chasing that consumes staff time before work even starts, ",
            "workflow that lives too heavily in email inboxes with no structured follow-up, ",
            "intake and onboarding that requires repeated back-and-forth with clients, ",
            "and admin work that senior staff carry unnecessarily instead of delegating."
        ],
        "home_services": [
            "Home service businesses — roofing, HVAC, plumbing, handyman — typically face specific operational friction: ",
            "incoming leads that come in but get lost before a human ever follows up, ",
            "office-to-field handoffs where the technician shows up but the context is missing, ",
            "scheduling chaos, estimate drift, and callbacks that slip through the cracks, ",
            "and admin work — phones, inboxes, dispatch — that owners never have enough time for."
        ],
    }
    context_lines = VERTICAL_CONTEXT.get(vertical, VERTICAL_CONTEXT["home_services"])
    operational_context = "\n".join(context_lines)

    # Rating: only include if genuinely distinctive AND relevant, never as opener
    rating_val  = enrichment_signals.get("rating")
    rating_str  = f" ({rating_val} stars on Google)" if rating_val and rating_val >= 4.5 else ""
    
    # Review themes: only include if real data
    themes = enrichment_signals.get("review_themes", [])
    themes_str = f" Things clients mention: {', '.join(themes[:2])}." if themes else ""

    fu_count_label = {2: "second", 3: "third", 4: "final"}[fu_number]

    # FU#3 needs a genuinely new angle — give the model a hint
    fu3_new_angle_hints = {
        "home_services": "Try a different angle: the cost of a single missed follow-up on a new lead — one rejected estimate, one dropped scheduling callback — can exceed what a workflow fix would cost.",
        "accounting_bookkeeping": "Try a different angle: senior staff in firms like this are usually the ones doing the admin work that should be delegated — the opportunity cost of their time is the real expense.",
        "estate_planning_probate": "Try a different angle: client intake and document gathering is where firms like this lose the most time — not in the substantive work but in the coordination admin.",
    }
    new_angle_hint = fu3_new_angle_hints.get(vertical, "") if fu_number >= 3 else ""

    prompt = f"""Write cold follow-up email body #{fu_number} for a {v_short} company.

SENDER: Herman Carter, Partnership and Growth Consultant, USP LLC
RECIPIENT: {firm_name}
PREVIOUS EMAILS IN THREAD:
{prior_emails_text}

TONE DIRECTIVE: {tone_dir}

OPERATIONAL CONTEXT FOR {v_short}:
{operational_context}

{f'[REVIEW SIGNAL: {rating_str}{themes_str}]' if rating_str or themes_str else ''}
{f'[NEW ANGLE HINT: {new_angle_hint}]' if new_angle_hint else ''}

WHAT TO WRITE:
1. Opening: naturally acknowledge the prior email(s) — reference something specific from the previous email (a point you made, the subject line, or the fact that you sent before). Do NOT start with "I hope this finds you" or "Just following up."
2. Middle: one short paragraph with a new angle or observation — NOT a repeat of the opener from previous emails. For FU#3+, this is critical — the model must say something DIFFERENT from what it said in FU#2.
3. CTA (LAST paragraph before signoff): "{cta_hook}. {cta_barrier}"
4. Signoff: {SENDER_NAME} | {SENDER_TITLE} | USP LLC | https://www.uspai.io/

RULES:
- Do NOT start with "I hope this finds you well" or "Just following up" or "Following up" as an opener — those are weak
- Do NOT repeat the opening angle from the previous email in this thread
- Do NOT include any pricing or offer details
- Do NOT use placeholder text in brackets like [company name] or [specific detail]
- Do NOT produce meta-commentary about the email itself
- Do NOT attach any images, PDFs, or files
- Do NOT include "(5 stars on Google)" or similar rating filler unless genuinely relevant to the argument
- Keep total body to 100-150 words
- Output ONLY the email body text — no subject line, no bullet points

OUTPUT THE EMAIL BODY:"""

    body = call_local_model(prompt, max_tokens=300)

    if not body or len(body) < 80:
        # Minimal fallback — model failure
        body = f"""Hi {firm_name} team,

I sent over a note about getting a leaner follow-up and intake process in place — the idea was to free up your team's time without requiring a major rebuild.

{rating_str}{themes_str}

{cta_hook}. {cta_barrier}

{SENDER_NAME}
{SENDER_TITLE}
USP LLC
https://www.uspai.io/
"""

    # Clean model output
    lines = body.split("\n")
    cleaned = []
    skip_patterns = [
        "here's a cold follow-up", "here is a cold follow-up",
        "here's the follow-up", "here is the follow-up",
        "draft of the follow-up", "this follow-up email",
        "subject:", "body:", "email body:", "output the email",
    ]
    for line in lines:
        stripped = line.strip()
        skip = any(pat.lower() in stripped.lower() for pat in skip_patterns)
        if skip:
            continue
        # Remove bracket placeholders
        if "[" in stripped and "]" in stripped and not stripped.startswith("("):
            continue
        # Remove meta-commentary
        if stripped.lower().startswith(("write", "output", "here's", "here is", "this email")):
            continue
        cleaned.append(line)

    body = "\n".join(cleaned).strip()

    # Ensure signoff is clean
    sig_parts = [SENDER_NAME, SENDER_TITLE, "USP LLC", "https://www.uspai.io/"]
    if not any(p in body for p in sig_parts):
        body += f"\n\n{SENDER_NAME}\n{SENDER_TITLE}\nUSP LLC\nhttps://www.uspai.io/"

    return body


# ── Enrichment helpers ──────────────────────────────────────────────────────

def extract_enrichment_signals(enrichment_json):
    """Extract firm-specific signals from enrichment JSON."""
    if not enrichment_json:
        return {"types": [], "rating": None, "review_themes": [],
                "web_snippets": [], "is_weak": True}

    try:
        data = json.loads(enrichment_json) if isinstance(enrichment_json, str) else enrichment_json
    except (json.JSONDecodeError, TypeError):
        data = {}

    raw_types = data.get("types", [])
    if isinstance(raw_types, list):
        types = [t for t in raw_types if t not in ("establishment", "point_of_interest")]
    else:
        types = []

    rating = data.get("rating")
    reviews = data.get("reviews", [])
    if isinstance(reviews, list):
        review_texts = [r.get("text", "") for r in reviews if isinstance(r, dict) and r.get("text")]
    elif isinstance(reviews, dict):
        review_texts = []
        for v in reviews.values():
            if isinstance(v, list):
                review_texts.extend([r.get("text", "") for r in v if isinstance(r, dict)])
    else:
        review_texts = []

    themes = []
    if review_texts:
        combined = " ".join(review_texts[:6])
        prompt = f"Extract 2-3 short operational themes from these review excerpts. Return a comma-separated list only: {combined[:500]}"
        themes_raw = call_local_model(prompt, max_tokens=60)
        themes = [t.strip() for t in themes_raw.split(",") if t.strip()][:3]

    raw_snippets = data.get("web_snippets", [])
    web_snippets = raw_snippets[:3] if isinstance(raw_snippets, list) else []
    is_weak = len(review_texts) < 2 and not web_snippets

    return {
        "types": types,
        "rating": rating,
        "review_themes": themes,
        "web_snippets": web_snippets,
        "is_weak": is_weak,
    }


def get_confirmed_first_name(lead):
    """Returns confirmed personal first name or None. Source: contact_named_person ONLY."""
    ROLE_WORDS = frozenset([
        "owner", "founder", "ceo", "cfo", "coo", "cto", "president", "vice", "director",
        "manager", "office", "service", "sales", "marketing", "info", "contact",
        "support", "help", "team", "operations", "admin", "accounting", "bookkeeping",
        "bookkeeper", "accountant", "coordinator", "administrator", "receptionist",
        "dispatch", "dispatcher", "lead", "partner", "associate", "assistant",
    ])
    named = (lead.get("contact_named_person") or "").strip().lower()
    if not named:
        return None
    if named in ROLE_WORDS:
        return None
    tokens = named.split()
    if any(t in ROLE_WORDS for t in tokens):
        return None
    return named.title()


# ── Thread eligibility ─────────────────────────────────────────────────────

def get_ready_threads(vertical=None, force=False, verbose=True):
    """
    Return list of thread dicts ready for follow-up draft generation.
    - thread_state = 'active'
    - next_followup <= today (or force=True)
    - sent event count < MAX_FOLLOWUPS + 1  (i.e., stage 1→4, not stage 5/closed)
    - gmail_thread_id is not null
    - thread has no 'bounced' event in outreach_events
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    today = date.today().isoformat()
    query = """
        SELECT t.id as thread_id, t.lead_id, t.gmail_thread_id,
               t.vertical, t.stage, t.next_followup,
               l.name as lead_name, l.contact_email,
               l.contact_quality, l.contact_named_person,
               l.assigned_vertical, l.enrichment_data,
               l.contact_path
        FROM outreach_threads t
        JOIN leads l ON t.lead_id = l.id
        WHERE t.thread_state = 'active'
          AND t.gmail_thread_id IS NOT NULL
          AND t.gmail_thread_id != ''
          -- Exclude threads that have bounced (permanent failure)
          AND t.id NOT IN (
              SELECT DISTINCT thread_id FROM outreach_events
              WHERE event_type = 'bounced'
                AND JSON_EXTRACT(event_data, '$.bounce_type') = 'permanent'
          )
    """
    if not force:
        query += f" AND (t.next_followup IS NOT NULL AND t.next_followup <= '{today}')"
    if vertical:
        query += f" AND t.vertical = ?"
        rows = conn.execute(query, (vertical,)).fetchall()
    else:
        rows = conn.execute(query).fetchall()

    threads = []
    for r in rows:
        sent_count = conn.execute("""
            SELECT COUNT(*) FROM outreach_events
            WHERE thread_id = ? AND event_type = 'sent'
        """, (r["thread_id"],)).fetchone()[0]

        fu_stage = sent_count + 1  # e.g., sent_count=1 → FU2 → stage 2

        if fu_stage > MAX_FOLLOWUPS + 1:  # already at max (4 emails total)
            if verbose:
                print(f"  Thread {r['thread_id']}: at max follow-ups ({sent_count} sends), skipping")
            continue

        thread_dict = dict(r)
        thread_dict["sent_count"] = sent_count
        thread_dict["fu_number"] = fu_stage  # e.g. 2 for FU2
        thread_dict["next_stage"] = fu_stage
        threads.append(thread_dict)

    conn.close()
    return threads


# ── Main run ─────────────────────────────────────────────────────────────────

def run(vertical=None, dry_run=False, force=False, verbose=True):
    """
    Main entry point. Generates follow-up drafts for all ready threads.
    
    NOTE: Follow-ups NEVER attach one-pagers. The one-pager is for initial
    outreach only (usp_draft_generator.py handles that). Follow-ups are
    plain-text thread replies.
    """
    threads = get_ready_threads(vertical=vertical, force=force, verbose=verbose)

    if not threads:
        if verbose:
            print("No threads ready for follow-up.")
        return {"status": "ok", "created": [], "skipped": []}

    if verbose:
        print(f"\n{len(threads)} thread(s) ready for follow-up draft generation...")

    created = []
    skipped = []

    for t in threads:
        thread_id  = t["thread_id"]
        lead_id    = t["lead_id"]
        fu_number  = t["fu_number"]   # 2, 3, or 4
        sent_count = t["sent_count"]   # 1, 2, or 3

        # Load offer
        offer_path = os.path.join(ROOT, "offer_library", f"{t['assigned_vertical']}.json")
        if not os.path.exists(offer_path):
            if verbose:
                print(f"  [{thread_id}] No offer file for {t['assigned_vertical']}, skipping")
            skipped.append((thread_id, "no_offer"))
            continue

        with open(offer_path) as f:
            offer = json.load(f)

        # ── Thread context: read all prior emails from Gmail ──────────────────
        gmail_tid = t["gmail_thread_id"]
        thread_msgs = gmail_get_thread_messages(gmail_tid, max_msgs=8)
        if verbose and thread_msgs:
            print(f"  [{thread_id}] Gmail thread has {len(thread_msgs)} prior message(s)")

        # ── Gmail-level bounce detection ──────────────────────────────────────
        has_gmail_bounce = False
        for msg in thread_msgs:
            msg_from = msg.get("from", "")
            snippet  = msg.get("snippet", "").lower()
            if "mailer-daemon" in msg_from.lower() or \
               "mail delivery" in msg_from.lower() or \
               "failure" in snippet or \
               "not found" in snippet or \
               ("address" in snippet and "couldn" in snippet):
                has_gmail_bounce = True
                break

        if has_gmail_bounce:
            conn = sqlite3.connect(DB)
            conn.execute("""
                UPDATE outreach_threads
                SET thread_state = 'closed', stage = 5,
                    close_reason = 'bounced_permanent',
                    next_followup = NULL,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (thread_id,))
            conn.execute("""
                INSERT INTO outreach_events (thread_id, event_type, event_data)
                VALUES (?, 'bounced', ?)
            """, (thread_id, json.dumps({
                "bounce_type": "permanent",
                "detected_by": "gmail_thread_scan",
                "gmail_thread_id": gmail_tid,
                "bounced_at": datetime.utcnow().isoformat(),
                "note": "Bounce detected from Gmail thread scan during followup generation"
            })))
            conn.commit()
            conn.close()
            if verbose:
                print(f"  [{thread_id}] BOUNCE DETECTED in Gmail — thread closed, skipping")
            skipped.append((thread_id, "gmail_bounce"))
            created.append({"thread_id": thread_id, "status": "bounced"})
            continue

        # Build thread context for the model — include ALL messages
        thread_context = []
        prior_subjects = []
        for msg in thread_msgs:
            subj = msg.get("subject", "")
            thread_context.append({
                "subject": subj,
                "date":    msg.get("date", ""),
                "snippet": msg.get("snippet", ""),
                "from":    msg.get("from", ""),
                "is_sent": "SENT" in msg.get("labels", []),
            })
            if subj and subj not in prior_subjects:
                prior_subjects.append(subj)

        # Get last message for In-Reply-To header
        last_msg_id = thread_msgs[0].get("id", "") if thread_msgs else ""

        # ── Build follow-up ──────────────────────────────────────────────────
        enrichment_signals = extract_enrichment_signals(t.get("enrichment_data"))
        first_name = get_confirmed_first_name(t)
        tier = t.get("contact_quality", "C")

        subject = build_followup_subject(prior_subjects)
        body    = build_followup_body(
            lead=t,
            offer=offer,
            enrichment_signals=enrichment_signals,
            thread_context=thread_context,
            fu_number=fu_number,
            tier=tier,
            first_name=first_name,
        )

        if verbose:
            gap = " [WEAK ENRICHMENT]" if enrichment_signals.get("is_weak") else ""
            print(f"\n  [{thread_id}] {t['lead_name']} / {tier}-tier / "
                  f"FU#{fu_number}{gap}")
            print(f"    To:      {t['contact_email']}")
            print(f"    Subject: {subject}")
            print(f"    Context: {len(thread_context)} prior email(s) in thread")
            print(f"    Body preview: {body[:120]}...")

        if dry_run:
            print(f"    [DRY RUN] Would create draft in Gmail thread {gmail_tid}")
            created.append({"thread_id": thread_id, "status": "dry_run"})
            continue

        # ── Create Gmail draft as reply ──────────────────────────────────────
        # NO inline images for follow-ups — one-pagers are for initial outreach only
        result = gmail_create_followup_draft(
            to_email=t["contact_email"],
            subject=subject,
            body_plain=body,
            gmail_thread_id=gmail_tid,
            in_reply_to=last_msg_id,
        )

        conn = sqlite3.connect(DB)
        if result["status"] == "ok":
            data = result["data"]
            draft_id   = data.get("draft_id")
            msg_id     = data.get("message_id")
            thread_gid = data.get("thread_id")

            # Update thread: stays active, state → pending_approval, gmail_draft_id → new draft
            conn.execute("""
                UPDATE outreach_threads
                SET gmail_draft_id    = ?,
                    gmail_thread_id   = COALESCE(NULLIF(?, ''), gmail_thread_id),
                    thread_state      = 'pending_approval',
                    next_followup     = NULL,
                    updated_at        = datetime('now')
                WHERE id = ?
            """, (draft_id, thread_gid, thread_id))

            # Write followup_created event
            conn.execute("""
                INSERT INTO outreach_events
                    (thread_id, event_type, event_data)
                VALUES (?, 'followup_created', ?)
            """, (thread_id, json.dumps({
                "fu_number":                fu_number,
                "gmail_draft_id":           draft_id,
                "gmail_message_id":         msg_id,
                "subject":                  subject,
                "to_email":                 t["contact_email"],
                "from_email":               SENDER_EMAIL,
                "tier":                     tier,
                "vertical":                 t["assigned_vertical"],
                "sent_count_at_creation":   sent_count,
                "prior_emails_in_thread":   len(thread_context),
                "one_pager_attached":       False,  # follow-ups never have one-pagers
                "created_at":               datetime.utcnow().isoformat(),
            })))

            conn.commit()
            if verbose:
                print(f"    Draft: {draft_id} — SAVED (pending approval)")
            created.append({"thread_id": thread_id, "draft_id": draft_id, "fu_number": fu_number})
        else:
            err = result.get("stderr", result.get("stdout", "unknown error"))
            conn.execute("""
                INSERT INTO outreach_events
                    (thread_id, event_type, event_data)
                VALUES (?, 'followup_error', ?)
            """, (thread_id, json.dumps({
                "fu_number": fu_number,
                "error": err[:500],
                "created_at": datetime.utcnow().isoformat(),
            })))
            conn.commit()
            if verbose:
                print(f"    ERROR creating draft: {err[:200]}")
            skipped.append((thread_id, err[:100]))

        conn.close()

    return {"status": "ok", "created": created, "skipped": skipped}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="USP Follow-Up Draft Generator")
    parser.add_argument("--vertical", help="Process specific vertical only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would run without creating drafts")
    parser.add_argument("--force", action="store_true",
                        help="Ignore next_followup date — generate for all active threads")
    args = parser.parse_args()

    result = run(
        vertical=args.vertical,
        dry_run=args.dry_run,
        force=args.force,
        verbose=True,
    )
    n = len(result["created"])
    print(f"\nDone. {n} follow-up draft(s) created.")
    sys.exit(0)
