#!/usr/bin/env python3
"""
Phase 2: Draft Generator — Multi-Vertical (Corrected Draft Content Pass)

Gating: A vertical may autonomously generate drafts ONLY if ALL true:
  1. valid offer_library file exists
  2. CTA/operator identity is clean (no "Justin" contamination)
  3. required compliance fields present (for legal verticals)
  4. one_pager_policy exists in config
  5. drafting_approved_[vertical].approved = true
  6. one_pager_policy_[vertical].drafting_status = "extracted"
  7. verticals_enabled.[vertical] = true
  8. at least 1 email-qualified lead exists in off_market or draft_queued

Gmail: Saves draft via google_api_usp.py gmail_create_draft — NEVER sends.
Human clicks Send in Gmail. Thread watcher detects send.

Usage:
  python3 usp_draft_generator.py                      # all approved verticals
  python3 usp_draft_generator.py --vertical estate_planning_probate  # single vertical
  python3 usp_draft_generator.py --vertical estate_planning_probate --feedback "too_generic"  # rejection feedback
"""
import json, os, sqlite3, sys, argparse, re
from datetime import datetime
import urllib.request

DB   = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")
ROOT = os.path.expanduser("~/.hermes/Hermes-USP-v1")

SENDER_NAME    = "Herman Carter"
SENDER_TITLE   = "Partnership and Growth Consultant"
SENDER_EMAIL   = "hermancarter373@gmail.com"

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL    = "llama3.2:3b"

REJECTION_TAGS = frozenset([
    "too_generic",
    "too_pitchy",
    "paraphrases_asset",
    "wrong_tone",
    "weak_personalization",
])

COMPLIANCE_VERTICALS = {"collections_law", "family_law", "real_estate", "estate_planning_probate"}


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


def is_vertical_enabled(vertical):
    enabled = get_config("verticals_enabled", {})
    return enabled.get(vertical, False)


def is_drafting_approved(vertical):
    val = get_config(f"drafting_approved_{vertical}", None)
    if val is None:
        return False
    return val.get("approved", False) is True


def get_one_pager_policy(vertical):
    val = get_config(f"one_pager_policy_{vertical}", None)
    return val if val else {}


def get_enabled_approved_verticals():
    enabled = get_config("verticals_enabled", {})
    result = []
    for v, is_on in enabled.items():
        if not is_on:
            continue
        offer_path = os.path.join(ROOT, "offer_library", f"{v}.json")
        if not os.path.exists(offer_path):
            continue
        if not is_drafting_approved(v):
            continue
        policy = get_one_pager_policy(v)
        if policy.get("drafting_status") != "extracted":
            continue
        with open(offer_path) as f:
            offer = json.load(f)
        if "Justin" in offer.get("cta_language", {}).get("call_recipient", ""):
            continue
        if v in COMPLIANCE_VERTICALS and not offer.get("compliance_disclaimer"):
            continue
        result.append(v)
    return result


# ── Gmail API ────────────────────────────────────────────────────────────────

def gmail_create_draft(to_email, subject, body_plain, inline_image_path=None):
    """Create a Gmail draft via google_api_usp.py CLI. No send."""
    import subprocess, json as _json

    script = "/home/cortana/.hermes/skills/productivity/google-workspace/scripts/google_api_usp.py"
    cmd = [
        sys.executable, script,
        "gmail", "create-draft",
        "--to", to_email,
        "--subject", subject,
        "--body", body_plain,
    ]
    if inline_image_path:
        cmd += ["--html", "--inline-image-path", inline_image_path]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return {"status": "error", "stderr": result.stderr}
    try:
        return {"status": "ok", "data": _json.loads(result.stdout)}
    except _json.JSONDecodeError:
        return {"status": "error", "stdout": result.stdout, "stderr": result.stderr}


def gmail_draft_exists(draft_id):
    """
    Check whether a Gmail draft still exists in Gmail.
    Returns True if draft is found, False if it was deleted.
    Used to detect operator-deleted drafts and enable automatic recovery.
    """
    import subprocess
    script = "/home/cortana/.hermes/skills/productivity/google-workspace/scripts/google_api_usp.py"
    result = subprocess.run(
        [sys.executable, script, "gmail", "get-draft", draft_id],
        capture_output=True, text=True, timeout=15
    )
    return result.returncode == 0


def lead_has_active_thread(lead_id, vertical):
    """
    Return True if a lead already has an active outreach thread
    in any state other than 'closed', 'rejected', or 'bounced'.
    An active thread means the lead is already being tracked —
    packet + thread was already created for this lead.
    Used to prevent duplicate packet/thread creation.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT id, thread_state, gmail_draft_id
        FROM outreach_threads
        WHERE lead_id = ? AND vertical = ?
          AND thread_state NOT IN ('closed', 'rejected', 'bounced')
        LIMIT 1
    """, (lead_id, vertical)).fetchone()
    conn.close()
    return row is not None


def verify_draft_integrity(thread_id, lead_id, vertical, contact_email):
    """
    Verify that a tracked Gmail draft still exists and matches the expected recipient.
    Returns (is_valid, reason).
    If draft was deleted by operator, reset thread to 'drafting' so it can regenerate.
    If draft exists but email changed, treat as deleted (new draft needed).
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT gmail_draft_id, gmail_thread_id, thread_state, current_email
        FROM outreach_threads
        WHERE id = ? AND lead_id = ? AND vertical = ?
    """, (thread_id, lead_id, vertical)).fetchone()

    if not row:
        conn.close()
        return True, "thread_not_found"

    draft_id = row["gmail_draft_id"]
    current_email = row["current_email"]
    thread_state = row["thread_state"]

    conn.close()

    # If no draft tracked, nothing to verify
    if not draft_id:
        return True, "no_draft_tracked"

    # If thread is not in a draft-owning state, skip verification
    if thread_state not in ("pending_approval", "active", "drafting"):
        return True, f"thread_in_{thread_state}_state"

    # Check if email changed — if so, old draft is unusable
    if current_email and contact_email and current_email != contact_email:
        _reset_thread_to_drafting(thread_id)
        return False, f"email_changed_was_{current_email}_now_{contact_email}"

    # Verify draft exists in Gmail
    if not gmail_draft_exists(draft_id):
        _reset_thread_to_drafting(thread_id)
        return False, "draft_deleted_by_operator"

    return True, "draft_valid"


def _reset_thread_to_drafting(thread_id):
    """
    Reset a thread to 'drafting' state so draft generator can create a new draft.
    Only resets threads whose Gmail draft has been confirmed deleted.
    Preserves packet — packet_id stays associated with the thread.
    """
    conn = sqlite3.connect(DB)
    conn.execute("""
        UPDATE outreach_threads
        SET gmail_draft_id = NULL,
            gmail_thread_id = NULL,
            thread_state = 'drafting',
            updated_at = datetime('now')
        WHERE id = ?
          AND gmail_draft_id IS NOT NULL
          AND thread_state IN ('pending_approval', 'active')
    """, (thread_id,))
    conn.execute("""
        INSERT INTO outreach_events (thread_id, event_type, event_data)
        VALUES (?, 'draft_deleted_recovery', ?)
    """, (thread_id, json.dumps({
        "reason": "operator_deleted_draft",
        "recovered_at": datetime.utcnow().isoformat(),
    })))
    conn.commit()
    conn.close()


def handle_deleted_drafts(vertical, verbose=True):
    """
    USP-STANDARD draft-integrity routine.
    Runs before drafting to detect and recover from operator-deleted drafts.
    For each thread with a tracked gmail_draft_id:
      - Verify the draft still exists in Gmail
      - If deleted: reset thread to 'drafting', clear draft ID, log event
      - If email changed: treat as deleted, reset
      - If valid: leave alone
    Returns list of recovered thread_ids.
    This benefits ALL verticals — accounting, estate, home_services, and future verticals.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    threads = conn.execute("""
        SELECT t.id, t.lead_id, t.gmail_draft_id, l.contact_email
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.vertical = ?
          AND t.gmail_draft_id IS NOT NULL
          AND t.thread_state IN ('pending_approval', 'active')
    """, (vertical,)).fetchall()
    conn.close()

    recovered = []
    for t in threads:
        is_valid, reason = verify_draft_integrity(t["id"], t["lead_id"], vertical, t["contact_email"])
        if not is_valid:
            if verbose:
                print(f"  Recovered thread {t['id']}: {reason}")
            recovered.append(t["id"])

    return recovered


# ── Offer loading ────────────────────────────────────────────────────────────

def load_offer(vertical):
    path = os.path.join(ROOT, "offer_library", f"{vertical}.json")
    with open(path) as f:
        return json.load(f)


# ── Enrichment extraction ────────────────────────────────────────────────────

def extract_enrichment_signals(enrichment_json):
    """
    Extract firm-specific signals from enrichment JSON.
    Returns a dict with types, rating, review_themes, web_snippets, and is_weak.
    """
    if not enrichment_json:
        return {
            "types": [],
            "rating": None,
            "review_themes": [],
            "web_snippets": [],
            "is_weak": True,
        }

    try:
        data = json.loads(enrichment_json) if isinstance(enrichment_json, str) else enrichment_json
    except (json.JSONDecodeError, TypeError):
        data = {}

    # Business types
    raw_types = data.get("types", [])
    if isinstance(raw_types, list):
        types = [t for t in raw_types if t not in ("establishment", "point_of_interest")]
    else:
        types = []

    # Rating
    rating = data.get("rating")

    # Review themes — extract top themes from review texts
    reviews = data.get("reviews", [])
    if isinstance(reviews, dict):
        # Old format: reviews as dict
        review_texts = []
        for v in reviews.values():
            if isinstance(v, list):
                review_texts.extend([r.get("text", "") for r in v if isinstance(r, dict)])
    elif isinstance(reviews, list):
        review_texts = [r.get("text", "") for r in reviews if isinstance(r, dict) and r.get("text")]
    elif isinstance(reviews, int):
        # reviews is a count/integer — no actual text available
        review_texts = []
    else:
        review_texts = []

    review_themes = _summarize_review_themes(review_texts)

    # Website snippets
    raw_snippets = data.get("web_snippets", [])
    if isinstance(raw_snippets, list):
        web_snippets = [s for s in raw_snippets if s][:3]
    else:
        web_snippets = []

    # Business description (derived from website)
    business_desc = data.get("business_description", "")

    # Determine weakness
    is_weak = len(review_texts) < 2 and not web_snippets

    return {
        "types": types,
        "rating": rating,
        "review_themes": review_themes,
        "web_snippets": web_snippets,
        "is_weak": is_weak,
        "business_description": business_desc,
    }


def _summarize_review_themes(texts, max_themes=3):
    """
    Extract top operational/service themes from review texts.
    Returns list of short theme phrases.
    """
    if not texts:
        return []

    # Simple keyword-based theme extraction
    theme_keywords = {
        "communication": ["communication", "responsive", "responsiveness", "follow up", "follow-up"],
        "efficiency": ["efficient", "fast", "quick", "speed", "timely", "on time", "organized"],
        "professionalism": ["professional", "knowledgeable", "experienced", "expert", "skilled"],
        "client_service": ["helpful", "friendly", "helpful", "support", "patient", "attentive"],
        "quality": ["quality", "accurate", "thorough", "detail", "comprehensive"],
        "reliability": ["reliable", "consistent", "dependable", "consistent"],
        "clarity": ["clear", "explained", "understandable", "straightforward"],
    }

    theme_counts = {k: 0 for k in theme_keywords}

    for text in texts:
        text_lower = text.lower()
        for theme, keywords in theme_keywords.items():
            if any(kw in text_lower for kw in keywords):
                theme_counts[theme] += 1

    sorted_themes = sorted(theme_counts.items(), key=lambda x: -x[1])
    return [t[0] for t in sorted_themes if t[1] > 0][:max_themes]


# ── Personalization safety ───────────────────────────────────────────────────

ROLE_WORDS = frozenset([
    'partner', 'accountant', 'owner', 'attorney', 'lawyer', 'cpa',
    'manager', 'director', 'ceo', 'coo', 'cfo', 'president',
    'consultant', 'broker', 'agent', 'representative', 'coordinator',
    'administrator', 'bookkeeper', 'paralegal', 'website builder',
    'partner that truly', 'partner with',
    'published mar', 'firm denver', 'internet explorer',
    'tax preparation', 'certified tax', 'denver bookkeeping',
])


def get_confirmed_first_name(lead):
    """
    Returns a confirmed personal first name or None.
    Source of truth: contact_named_person column ONLY.
    Email local-part parsing is NEVER used for greeting construction.
    Rules:
      - contact_named_person must not be a role word (any single word OR phrase)
      - contact_quality must be A or B-tier to use first name
      - C-tier and below: firm greeting only regardless of named_person
    """
    named = (lead.get('contact_named_person') or '').strip().lower()
    if not named:
        return None
    # Check full phrase and individual words
    if named in ROLE_WORDS:
        return None
    # Any individual token is a role word → reject
    tokens = named.split()
    if any(t in ROLE_WORDS for t in tokens):
        return None
    return named.title()


# ── Local Ollama model call ──────────────────────────────────────────────────

def call_local_model(prompt, max_tokens=400):
    """
    Call local Ollama model for draft body generation.
    Returns the generated text or None on failure.
    """
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": max_tokens,
            "temperature": 0.7,
        }
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        OLLAMA_ENDPOINT,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return result.get("response", "").strip()
    except Exception as e:
        print(f"      [OLLAMA WARN] model call failed: {e}")
        return None


# ── Dynamic CTA generation ───────────────────────────────────────────────────

def build_dynamic_cta(lead_name, tier, enrichment_signals, offer):
    """
    Generate a dynamic, contextually-appropriate CTA.
    Tied to the lead's business context, not a rigid template.
    """
    cta_data = offer.get("cta_language", {})
    primary = cta_data.get("primary_cta", "reply to schedule a free 10-minute workflow fit call")
    barrier = cta_data.get("barrier_removal", "no cost, no commitment")

    # Build context-aware hook phrase
    themes = enrichment_signals.get("review_themes", [])
    is_weak = enrichment_signals.get("is_weak", False)

    if themes:
        if "efficiency" in themes:
            hook = f"If {lead_name} is looking to run more efficiently, {primary}"
        elif "communication" in themes:
            hook = f"If getting clearer communication loops with clients matters to {lead_name}, {primary}"
        elif "quality" in themes:
            hook = f"If cleaner, more reliable processes would help {lead_name}, {primary}"
        else:
            hook = f"If that's worth a quick conversation, {primary}"
    else:
        hook = f"If that resonates, {primary}"

    barrier_line = f"{barrier}."

    return hook, barrier_line


# ── Draft body assembly ───────────────────────────────────────────────────────

def build_email_body(lead, offer, enrichment_signals, rejection_history=None):
    """
    Build email body using enrichment-first approach.
    Attempts local model generation; falls back to vertical-generic if model fails.
    """
    firm_name  = lead["lead_name"]
    tier       = lead["contact_quality"]
    first_name = get_confirmed_first_name(lead)
    vertical   = lead["assigned_vertical"]
    v_short    = vertical.replace("_", " ").title()

    # Greeting: A or B tier with confirmed personal name gets first-name greeting
    if tier in ("A", "B") and first_name:
        greeting = f"Hi {first_name},"
    else:
        greeting = f"Hi {firm_name} team,"

    # Compliance
    compliance_text = ""
    if vertical in COMPLIANCE_VERTICALS:
        compliance_text = offer.get("compliance_disclaimer", "")
        if compliance_text:
            compliance_text = f"\n{compliance_text}"

    # CTA — dynamic
    cta_hook, cta_barrier = build_dynamic_cta(firm_name, tier, enrichment_signals, offer)

    # One-pager visual reference (always present in enrichment-first path)
    visual_ref = "I included a brief visual overview below so you can see how we think about this."

    # Footer
    footer_context = offer.get("body_language_rules", {}).get(
        "footer_context",
        "Partnership and Growth Consultant for cleaner operations and human-reviewed workflow systems"
    )

    # ── Try local model generation ─────────────────────────────────────────
    # Fire Ollama whenever we have firm name + vertical — produces better output
    # than hard fallback. is_weak only controls the prompt flavor, not whether to try.
    model_prompt = _build_model_prompt(
        firm_name=firm_name,
        first_name=first_name,
        tier=tier,
        vertical=vertical,
        v_short=v_short,
        enrichment=enrichment_signals,
        cta_hook=cta_hook,
        cta_barrier=cta_barrier,
        compliance_text=compliance_text,
        rejection_history=rejection_history,
    )
    model_body = call_local_model(model_prompt)
    personalization_gap = enrichment_signals.get("is_weak", True)

    # Known junk lines to strip from model output
    STRIP_LINES = {
        "All the best,",
        "Herman Carter",
        "Partnership and Growth Consultant",
        "USP LLC",
        "I included a brief visual overview below so you can see how we think about this.",
        "[Insert one-pager visual asset]",
        "[Insert visual overview]",
    }

    # Prohibited patterns — strip any line containing these
    PROHIBITED_PATTERNS = [
        "as a professional services firm",
        "professional services firm like",
        "firms like yours",
        "we work with firms",
        "we've helped",
        "we've partnered",
        "our clients",
        "our firm",
        "our expertise",
        "tailored solutions",
        "potential opportunities",
        "streamline processes",
        "optimize your operations",
        "transform your business",
        "best practices",
        "high-value activities",
        "client-first",
        "cutting-edge",
        "game-changing",
        "best-in-class",
        # Wrapper / meta language — model should never produce these
        "here's a cold outreach",
        "here is a cold outreach",
        "here's the cold outreach",
        "here is the cold outreach",
        "outreach email for",
        "draft of the cold outreach",
        "here's a draft",
        "here is a draft",
        "draft for the outreach",
        "this cold email",
        "this outreach email",
    ]

    if model_body and len(model_body) > 80:
        # Post-process: strip model-added signoff, visual ref, and any bracket text
        lines = model_body.rstrip().split("\n")
        body_lines = []
        seen_visual_ref = False
        for line in lines:
            stripped = line.strip()
            # Skip known junk / signoff lines
            if stripped in STRIP_LINES:
                continue
            # Skip bracket placeholders
            if "[" in stripped and "]" in stripped:
                continue
            # Skip any line containing prohibited patterns
            skip = False
            for pat in PROHIBITED_PATTERNS:
                if pat.lower() in stripped.lower():
                    skip = True
                    break
            if skip:
                continue
            # Track visual ref so we only add it once
            if "visual overview" in stripped.lower():
                if seen_visual_ref:
                    continue
                seen_visual_ref = True
            body_lines.append(line)
        # ── Assembly: strict paragraph order ─────────────────────────────────
        # No duplicated greeting, no duplicated signoff, no collapsed blocks.
        # Output order: greeting line → blank → para1 → blank → para2 →
        # blank → CTA → blank → visual-ref → blank → signoff → blank →
        # name/title/USP → footer_context → compliance
        output_lines = []
        # Detect if first body line is a greeting (starts with Hi / Hello / Dear)
        first_line = body_lines[0].strip() if body_lines else ""
        greeting_in_body = first_line.startswith(("Hi ", "Hello ", "Dear "))

        if not greeting_in_body:
            # Insert greeting at top as its own line
            output_lines.append(greeting)
            output_lines.append("")

        for line in body_lines:
            stripped = line.strip()
            if not stripped:
                # Only add blank line if previous line is not blank
                if output_lines and output_lines[-1].strip():
                    output_lines.append("")
            else:
                output_lines.append(line)

        # CTA block — always append clean CTA to guarantee it's present
        # Skip if body already has a "reply to schedule" CTA to avoid doubling
        cta_in_body = any(
            "reply to schedule" in ln.lower()
            for ln in body_lines
        )
        output_lines.append("")
        if not cta_in_body:
            output_lines.append(f"{cta_hook}. {cta_barrier}")
        else:
            # Body already wrote its own CTA — skip adding another one
            pass

        # Visual reference (once)
        output_lines.append("")
        output_lines.append(visual_ref)

        # Signoff block — all as ONE paragraph to avoid blank-line <p> splitting
        # ||__SIGNOFF__|| marker at end of last body paragraph triggers proper signoff HTML:
        # body paragraph text\n||__SIGNOFF__||\nHerman Carter\nPartnership...\nhttps://...
        # → last <p>paragraph text</p><p>Herman Carter<br>Partnership...<br><a href>USP URL</a></p>
        output_lines.append("||__SIGNOFF__||")
        output_lines.append(SENDER_NAME)
        output_lines.append(SENDER_TITLE)
        output_lines.append("USP LLC")
        output_lines.append("https://www.uspai.io/")
        output_lines.append(footer_context)

        return "\n".join(output_lines), personalization_gap

    # ── Fallback: vertical-generic body ────────────────────────────────────
    # Used only when Ollama call fails or returns garbage
    pain = offer.get("pain_point_mapping", {})
    pain_items = list(pain.items())[:2]

    # Build vertical-specific opener — ratings/reviews are NEVER the opener
    # Rating may be referenced later as soft supporting context only
    rating = enrichment_signals.get("rating")
    types  = enrichment_signals.get("types", [])
    themes = enrichment_signals.get("review_themes", [])

    # Vertical-specific fallback openers keyed by vertical
    FALLBACK_OPENERS = {
        "estate_planning_probate": {
            "themes":   "Estate and probate firms that get consistent client feedback on {t} tend to have underlying operational patterns that are fixable — usually around intake handoffs, document follow-up, or client communication loops.",
            "types":    "Estate planning and probate practices that focus on {t} face a specific kind of operational challenge: the work is relationship-driven but the admin side has to keep up or client trust erodes at the follow-up stage.",
            "rating":   "When estate planning clients have a good experience, the operational side has to match it — consistent document flow, reliable intake coordination, and clear communication loops are where that alignment actually happens.",
            "default":  "Estate planning and probate firms face a consistent challenge: the work is relationship-driven but the administrative side — client follow-up, document gathering, executor coordination — is where consistency tends to break down.",
        },
        "accounting_bookkeeping": {
            "themes":   "Accounting firms that get consistent client feedback on {t} tend to have underlying operational patterns that are fixable — usually around document chasing, intake bottlenecks, or workflow that lives too heavily in email.",
            "types":    "Accounting and bookkeeping firms focused on {t} face a specific kind of operational challenge: the client relationship work is solid but the admin and intake side — document gathering, onboarding, follow-up — is where time gets consumed.",
            "rating":   "When accounting clients have a good experience, the operational side has to match it — clean intake, reliable document flow, and consistent client communication are where that alignment actually happens.",
            "default":  "Accounting and bookkeeping firms face a consistent challenge: the client work is relationship-driven but the administrative side — document chasing, intake follow-up, inbox management — is where consistency tends to break down.",
        },
        "home_services": {
            "themes":   "Home service businesses that get consistent client feedback on {t} tend to have underlying operational patterns that are fixable — usually around lead follow-up speed, office-to-field handoffs, or scheduling consistency.",
            "types":    "Home service businesses focused on {t} face a specific kind of operational challenge: the field work is solid but the front end — missed callbacks, estimate drift, dispatch chaos — is where leads and revenue slip away.",
            "rating":   "When home service customers have a good experience, the operational side has to match it — fast follow-up, clean handoffs, and reliable scheduling are where that reputation actually gets built.",
            "default":  "Home service businesses face a consistent challenge: the field work is solid but the front end — incoming leads that go cold, scheduling mix-ups, and office-to-field handoffs — is where momentum tends to get lost.",
        },
    }
    openers = FALLBACK_OPENERS.get(vertical, FALLBACK_OPENERS["estate_planning_probate"])

    if themes:
        opener = openers["themes"].format(t=', '.join(themes[:2]))
    elif types:
        opener = openers["types"].format(t=', '.join(types[:2]))
    elif rating:
        opener = openers["rating"]
    else:
        opener = openers["default"]

    fallback_body_lines = [
        f"{greeting}",
        "",
        opener,
        "",
        f"{cta_hook}. {cta_barrier}",
        "",
        visual_ref,
        "||__SIGNOFF__||",
        "All the best,",
        SENDER_NAME,
        SENDER_TITLE,
        "USP LLC",
        "https://www.uspai.io/",
        footer_context,
    ]
    if compliance_text:
        fallback_body_lines.append(compliance_text)

    return "\n".join(fallback_body_lines), personalization_gap


def _build_model_prompt(firm_name, first_name, tier, vertical, v_short,
                         enrichment, cta_hook, cta_barrier,
                         compliance_text, rejection_history=None, offer=None):
    """Build the system+user prompt for local model body generation."""

    # Vertical-specific operational context — derived from offer_library pain_point_mapping
    # Each vertical leads with its own operational reality. No cross-contamination.
    VERTICAL_CONTEXT = {
        "estate_planning_probate": [
            "Probate and estate planning firms typically face specific operational friction:",
            "client intake that stalls between first contact and signed engagement,",
            "document gathering that requires consistent client follow-up,",
            "multiple touchpoints across executors, beneficiaries, and attorneys,",
            "and internal handoffs where things fall through before matters close.",
            "The opener must reflect these realities — not generic business consultant language.",
        ],
        "accounting_bookkeeping": [
            "Accounting and bookkeeping firms typically face specific operational friction:",
            "client document chasing that consumes staff time before work even starts,",
            "workflow that lives too heavily in email inboxes with no structured follow-up,",
            "intake and onboarding that requires repeated back-and-forth with clients,",
            "and admin work that senior staff carry unnecessarily instead of delegating.",
            "The opener must reflect these realities — not generic business consultant language.",
        ],
        "home_services": [
            "Home service businesses — roofing, HVAC, plumbing, handyman — typically face specific operational friction:",
            "incoming leads that come in but get lost before a human ever follows up,",
            "office-to-field handoffs where the technician shows up but the context is missing,",
            "scheduling chaos, estimate drift, and callbacks that slip through the cracks,",
            "and admin work — phones, inboxes, dispatch — that老板 never has enough time for.",
            "The opener must reflect these realities — not generic business consultant language.",
        ],
    }
    context_lines = VERTICAL_CONTEXT.get(vertical, VERTICAL_CONTEXT["estate_planning_probate"])
    operational_context = " ".join(context_lines)

    # Rating: verified fact, but NOT the opener — only secondary support if truly distinctive
    rating_val = enrichment.get("rating")
    rating_str = f" ({rating_val} stars on Google)" if rating_val else ""

    # Types: use only if genuinely specific, not as main framing
    types_list = enrichment.get("types", [])
    types_str = ", ".join(types_list[:3]) if types_list else ""

    # Review themes — only include if real data exists
    themes = enrichment.get("review_themes", [])
    if themes:
        themes_str = f"Things clients mention: {', '.join(themes[:3])}."
    else:
        themes_str = ""

    # Website snippets — only if real data
    snippets = enrichment.get("web_snippets", [])[:2]
    if snippets:
        snippets_str = f"From their website: {' | '.join(snippets[:2])}"
    else:
        snippets_str = ""

    # Rejection history context
    rejection_context = ""
    if rejection_history:
        rejection_context = (
            f"\n\nPREVIOUS DRAFT was rejected for: {rejection_history}\n"
            f"Avoid that failure mode. Do not repeat the same style.\n"
        )

    greeting_rule = (
        f'Use firm greeting: "Hi {firm_name} team,"'
        if tier == "C" or not first_name else
        f'Use personal greeting: "Hi {first_name},"'
    )

    # Vertical label for system prompt
    VERTICAL_SYSTEM_PROMPT = {
        "estate_planning_probate": "estate planning and probate firms",
        "accounting_bookkeeping": "accounting and bookkeeping firms",
        "home_services": "home service businesses — roofing, HVAC, plumbing, and handyman companies",
    }
    vert_label = VERTICAL_SYSTEM_PROMPT.get(vertical, VERTICAL_SYSTEM_PROMPT["estate_planning_probate"])

    # Vertical-specific rule overrides — tighten where model keeps failing
    VERTICAL_RULES_OVERRIDE = {
        "home_services": (
            "- HOME SERVICES RULE: First paragraph must be ONE concrete observation about a SPECIFIC "
            "day-to-day operational friction in a home service company — lead follow-up lag, "
            "office-to-field context loss, estimate drift, missed callback windows. "
            "NOT 'common challenge', NOT 'businesses like yours', NOT 'strong reputation'. "
            "NOT praise of any kind. "
            "- Max 70 words total body. Be concise. "
            "- NEVER say: common challenge, businesses like yours, strong reputation, efficiency and quality, "
            "well-designed operational system, exceptional service, streamlining your operations, "
            "run more efficiently, robust solution, proven track record"
        ),
    }
    extra_rules = VERTICAL_RULES_OVERRIDE.get(vertical, "")

    prompt = f"""You are a professional cold outreach writer. You help {vert_label} handle their operations more reliably.

TASK: Write a first cold outreach email body for {firm_name}. Output ONLY the email body text — no labels, no explanations, no metadata, no "subject:", no "body:", no "here's the email", no quotes.

RULES:
- Write ONLY about {firm_name} and how operational improvements could serve their {v_short.lower()} operations
- Do NOT invent facts about {firm_name} (employees, years in business, client count, awards, rankings)
- Do NOT claim USP has worked with, helped, partnered with, or delivered results for any firm — no "we've helped firms like yours", "in our experience", "our clients", "numerous firms", "similar firms", or equivalent language
- Do NOT paraphrase or copy the one-pager's headline or pitch text
- Tone: professional, direct, practical, peer-level — not salesy, not enthusiastic
- No pricing, no promises
- 90-140 words max for the body paragraphs
- Output ONLY the email body — start with the greeting on the first line, end with the CTA
- {greeting_rule}
{extra_rules}{rejection_context}

OPERATIONAL CONTEXT FOR {v_short}:
{operational_context}

WHAT YOU KNOW ABOUT {firm_name} (use sparingly — not as opener):{rating_str if rating_str else ''}
{f'- Business type: {types_str}' if types_str else ''}
{f'- {themes_str}' if themes_str else ''}
{f'- {snippets_str}' if snippets_str else ''}
- "No client review themes available" if neither themes nor snippets exist

CTA TO USE: {cta_hook}. {cta_barrier}

COMPLIANCE (legal verticals only, verbatim at end): {compliance_text if compliance_text else "omit"}

SIGN-OFF (add these lines at the end, only once):
All the best,

Herman Carter
Partnership and Growth Consultant
USP LLC

VISUAL REFERENCE (add as its own line at the very end, only once):
I included a brief visual overview below so you can see how we think about this.

Write the body now. The first paragraph must be a {v_short}-specific operational observation — not a rating, not a firm description, not a generic business sentence."""
    return prompt


# ── Lead query ───────────────────────────────────────────────────────────────

def get_frozen_verticals():
    """
    Return dict of verticals that are frozen from drafting.
    frozen_until is ISO timestamp string. None = no freeze.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT key, value FROM config WHERE key LIKE 'drafting_approved_%'").fetchall()
    conn.close()
    frozen = {}
    for row in rows:
        try:
            val = json.loads(row["value"]) if isinstance(row["value"], str) else row["value"]
            if val.get("frozen_until"):
                frozen[row["key"].replace("drafting_approved_", "")] = val["frozen_until"]
        except (json.JSONDecodeError, KeyError):
            pass
    return frozen


def is_frozen(vertical, frozen_verticals):
    """Return True if vertical has an active freeze."""
    if vertical not in frozen_verticals:
        return False
    freeze_time = frozen_verticals[vertical]
    if freeze_time is None:
        return False
    from datetime import datetime
    try:
        freeze_dt = datetime.fromisoformat(freeze_time.replace("Z", "+00:00"))
        if datetime.now(freeze_dt.tzinfo) < freeze_dt:
            return True
    except (ValueError, TypeError):
        pass
    return False


def get_drafting_leads(vertical=None):
    """
    Get threads in 'drafting' state with their lead and packet data.
    Loads enrichment_data from leads table.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    if vertical:
        rows = conn.execute("""
            SELECT t.id as thread_id, t.lead_id, t.vertical,
                   l.name as lead_name, l.contact_email, l.contact_path,
                   l.contact_quality, l.contact_named_person,
                   l.enrichment_data, l.draftability_notes,
                   l.assigned_vertical,
                   p.packet_text,
                   p.id as packet_id
            FROM outreach_threads t
            JOIN leads l ON l.id = t.lead_id
            JOIN packets p ON p.lead_id = t.lead_id
              AND p.id = (SELECT MAX(p2.id) FROM packets p2 WHERE p2.lead_id = t.lead_id)
            WHERE t.thread_state = 'drafting'
              AND t.gmail_draft_id IS NULL
              AND l.contact_email != 'phone_only'
              AND l.outbound_state IN ('new', 'reply_received')
              AND t.vertical = ?
            ORDER BY t.id
        """, (vertical,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT t.id as thread_id, t.lead_id, t.vertical,
                   l.name as lead_name, l.contact_email, l.contact_path,
                   l.contact_quality, l.contact_named_person,
                   l.enrichment_data, l.draftability_notes,
                   l.assigned_vertical,
                   p.packet_text,
                   p.id as packet_id
            FROM outreach_threads t
            JOIN leads l ON l.id = t.lead_id
            JOIN packets p ON p.lead_id = t.lead_id
              AND p.id = (SELECT MAX(p2.id) FROM packets p2 WHERE p2.lead_id = t.lead_id)
            WHERE t.thread_state = 'drafting'
              AND t.gmail_draft_id IS NULL
              AND l.contact_email != 'phone_only'
              AND l.outbound_state IN ('new', 'reply_received')
            ORDER BY t.id
        """).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def get_rejection_history(lead_id):
    """
    Get the most recent rejection reason for a lead from outreach_events.
    Returns the most recent rejection reason string or None.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute("""
        SELECT event_data FROM outreach_events
        WHERE thread_id IN (SELECT id FROM outreach_threads WHERE lead_id = ?)
          AND event_type = 'draft_rejected'
        ORDER BY created_at DESC
        LIMIT 1
    """, (lead_id,)).fetchone()
    conn.close()
    if row:
        try:
            ed = json.loads(row["event_data"])
            return ed.get("reason")
        except (json.JSONDecodeError, TypeError):
            return None
    return None


# ── Gate check ───────────────────────────────────────────────────────────────

def check_gate(vertical, verbose=True):
    """
    Check all gating requirements for a vertical.
    Returns (passed, failures_list).
    """
    failures = []

    offer_path = os.path.join(ROOT, "offer_library", f"{vertical}.json")
    if not os.path.exists(offer_path):
        failures.append("no offer_library file")
        return False, failures

    with open(offer_path) as f:
        offer = json.load(f)
    if "Justin" in offer.get("cta_language", {}).get("call_recipient", ""):
        failures.append("CTA contamination (Justin)")

    if vertical in COMPLIANCE_VERTICALS and not offer.get("compliance_disclaimer"):
        failures.append("missing compliance_disclaimer")

    policy = get_one_pager_policy(vertical)
    if not policy:
        failures.append("no one_pager_policy in config")

    if not is_drafting_approved(vertical):
        failures.append("drafting_approved = false")

    if policy.get("drafting_status") != "extracted":
        failures.append(f"drafting_status = {policy.get('drafting_status')}")

    if not is_vertical_enabled(vertical):
        failures.append("vertical not enabled")

    conn = sqlite3.connect(DB)
    cnt = conn.execute("""
        SELECT COUNT(*) FROM leads l
        WHERE l.assigned_vertical = ?
          AND l.contact_quality IN ('A','B','C')
          AND l.contact_email IS NOT NULL
          AND l.contact_email != ''
          AND l.contact_email != 'phone_only'
          AND (
              l.outbound_state IN ('off_market','draft_queued')
              OR EXISTS (
                  SELECT 1 FROM outreach_threads t
                  WHERE t.lead_id = l.id
                    AND t.thread_state = 'drafting'
                    AND t.gmail_draft_id IS NULL
              )
          )
    """, (vertical,)).fetchone()[0]
    conn.close()
    if cnt == 0:
        failures.append("no email-qualified leads in off_market, draft_queued, or drafting-thread")

    passed = len(failures) == 0
    if verbose and not passed:
        print(f"  GATE FAILED for {vertical}: {'; '.join(failures)}")
    return passed, failures


# ── One-pager asset helpers ──────────────────────────────────────────────────

def validate_asset(path):
    """
    USP-local asset validator.
    Returns (True, dimensions_str) if valid, (False, reason) if invalid.
    Rejects placeholders, tiny files, and non-image content.
    """
    if not path or not os.path.exists(path):
        return False, "file_missing"
    size = os.path.getsize(path)
    if size < 1000:
        return False, f"file_too_small_{size}_bytes"
    try:
        from PIL import Image
        with Image.open(path) as img:
            w, h = img.size
            if w < 100 or h < 100:
                return False, f"dimensions_too_small_{w}x{h}"
        return True, f"{w}x{h}"
    except Exception as e:
        return False, f"not_a_valid_image"


def get_one_pager_asset_path(vertical):
    """
    Return the confirmed one-pager asset path for a vertical.
    Checks both config and offer_library file.
    Returns (path, is_confirmed).
    Rejects placeholder/tiny/invalid files via validate_asset().
    """
    offer_path = os.path.join(ROOT, "offer_library", f"{vertical}.json")
    if os.path.exists(offer_path):
        with open(offer_path) as f:
            offer = json.load(f)
        asset_path = offer.get("one_pager_asset_path")
        if asset_path:
            valid, detail = validate_asset(asset_path)
            if valid:
                return asset_path, True
            else:
                return asset_path, False  # path known but invalid — hard stop

    policy = get_one_pager_policy(vertical)
    asset_path = policy.get("one_pager_asset_path")
    if asset_path:
        valid, detail = validate_asset(asset_path)
        if valid:
            return asset_path, True

    return None, False


# ── Main ─────────────────────────────────────────────────────────────────────

def run(vertical=None, verbose=True, feedback_tag=None):
    """
    Run draft generation.
    vertical: specific vertical to process (bypasses gate for targeted ops)
    verbose: print progress
    feedback_tag: rejection tag string from operator feedback
    """
    if vertical:
        verticals_to_run = [(vertical,)]
        targeted = True
    else:
        verticals_to_run = [(v,) for v in get_enabled_approved_verticals()]
        targeted = False

    if not verticals_to_run:
        if verbose:
            print("No verticals passed gating requirements.")
            print("All interpreted verticals are blocked from autonomous drafting.")
            print("Enable verticals one at a time by setting:")
            print("  UPDATE config SET value=json_set(value,'$.<vertical>',true)")
            print("  WHERE key='verticals_enabled'")
            print("And set drafting_approved_<vertical>.approved = true after review.")
        return {"status": "no_approved_verticals", "created": [], "blocked": []}

    all_created = []
    all_blocked = []

    for (v,) in verticals_to_run:
        if targeted:
            passed, failures = check_gate(v, verbose=verbose)
            if not passed:
                if verbose:
                    print(f"Skipping {v} (gate failed: {'; '.join(failures)})")
                all_blocked.append((v, failures))
                continue
        else:
            passed, failures = check_gate(v, verbose=False)
            if not passed:
                all_blocked.append((v, failures))
                continue

        if verbose:
            print(f"\nProcessing vertical: {v}")

        # ── Freeze check: prevent re-drafting on a frozen vertical ──────────
        frozen_verticals = get_frozen_verticals()
        if is_frozen(v, frozen_verticals):
            if verbose:
                print(f"  FROZEN: {v} is locked from drafting. Use unfreeze_vertical() to release.")
            all_blocked.append((v, ["vertical_frozen"]))
            continue

        # Check one-pager asset BEFORE loading leads
        asset_path, asset_confirmed = get_one_pager_asset_path(v)
        if not asset_confirmed or not asset_path:
            if verbose:
                print(f"  HARD STOP: one-pager asset missing or unconfirmed for {v}")
            # Log blocked event for any threads in drafting state
            conn = sqlite3.connect(DB)
            drafting_threads = conn.execute("""
                SELECT id FROM outreach_threads
                WHERE vertical = ? AND thread_state = 'drafting'
            """, (v,)).fetchall()
            for (tid,) in drafting_threads:
                # These threads are stuck because the one-pager is missing — the draft
                # generator owns them, not the draft watcher. The draft watcher would
                # log draft_blocked every 4h which pollutes the audit log. We do NOT
                # log draft_blocked here. Instead, record a recoverable_stuck event
                # so the daily report can detect and surface this condition.
                conn.execute("""
                    INSERT INTO outreach_events (thread_id, event_type, event_data)
                    VALUES (?, 'recoverable_stuck', ?)
                """, (tid, json.dumps({
                    "reason": "one_pager_missing",
                    "vertical": v,
                    "asset_path": asset_path,
                    "stuck_at": datetime.utcnow().isoformat(),
                    "note": "Thread is stuck waiting for one-pager asset. "
                            "Add the image to unblock the draft generator.",
                })))
            conn.commit()
            conn.close()
            all_blocked.append((v, ["one_pager_missing"]))
            continue

        if verbose:
            print(f"  One-pager confirmed: {asset_path}")

        offer = load_offer(v)

        # USP-STANDARD: Verify draft integrity before drafting.
        # Detects operator-deleted drafts and auto-resets threads to 'drafting'.
        # This recovers deleted-draft threads across ALL verticals automatically.
        recovered = handle_deleted_drafts(v, verbose=verbose)
        if recovered and verbose:
            print(f"  Draft integrity check: {len(recovered)} thread(s) recovered from deleted drafts")

        threads = get_drafting_leads(vertical=v)

        if not threads:
            if verbose:
                print(f"  No threads in 'drafting' state for {v}.")
                print("  Run packet generator first.")
            all_blocked.append((v, ["no drafting-state threads"]))
            continue

        if verbose:
            print(f"  {len(threads)} thread(s) to process...")

        created_drafts = []

        for t in threads:
            lead_id = t["lead_id"]
            thread_id = t["thread_id"]

            # Process rejection feedback if provided for this lead
            personalization_gap = None
            if feedback_tag and feedback_tag in REJECTION_TAGS:
                conn = sqlite3.connect(DB)
                conn.execute("""
                    UPDATE leads SET draftability_notes = COALESCE(draftability_notes, '') || ? ||
                        ' [rejected: ' || ? || ' @ ' || ? || ']'
                    WHERE id = ?
                """, (" ", feedback_tag, datetime.utcnow().isoformat(), lead_id))
                conn.execute("""
                    INSERT INTO outreach_events (thread_id, event_type, event_data)
                    VALUES (?, 'draft_rejected', ?)
                """, (thread_id, json.dumps({
                    "reason": feedback_tag,
                    "lead_id": lead_id,
                    "vertical": v,
                    "feedback_at": datetime.utcnow().isoformat(),
                })))
                conn.commit()
                conn.close()
                personalization_gap = feedback_tag

            # Extract enrichment signals
            enrichment_signals = extract_enrichment_signals(t.get("enrichment_data"))

            # Get rejection history for this lead
            rejection_history = get_rejection_history(lead_id)

            # Build subject
            subject = build_subject(t, offer)

            # Build body
            body, is_fallback = build_email_body(t, offer, enrichment_signals, rejection_history)

            if is_fallback and enrichment_signals.get("is_weak"):
                personalization_gap = "weak_enrichment"

            to_email = t["contact_email"]

            if verbose:
                gap_note = f" [PERSONALIZATION GAP: {personalization_gap}]" if personalization_gap else ""
                print(f"\n  [{thread_id}] {t['lead_name']} — {t['contact_path']} / {t['contact_quality']}-tier{gap_note}")
                print(f"    To:    {to_email}")
                print(f"    Subj:  {subject}")
                if is_fallback:
                    print(f"    NOTE:   enrichment-weak fallback used")

            # Create Gmail draft — HTML for inline image support
            html_body = _plain_to_html(body)
            result = gmail_create_draft(to_email, subject, html_body, inline_image_path=asset_path)

            conn = sqlite3.connect(DB)

            if result["status"] == "ok":
                data       = result["data"]
                draft_id   = data["draft_id"]
                msg_id     = data["message_id"]
                thread_gid = data["thread_id"]

                conn.execute("""
                    UPDATE outreach_threads
                    SET gmail_draft_id = ?,
                        gmail_thread_id = ?,
                        thread_state    = 'pending_approval',
                        updated_at      = datetime('now')
                    WHERE id = ?
                """, (draft_id, thread_gid, thread_id))

                event_data = {
                    "gmail_draft_id":   draft_id,
                    "gmail_message_id": msg_id,
                    "subject":          subject,
                    "to_email":         to_email,
                    "from_email":       SENDER_EMAIL,
                    "tier":             t["contact_quality"],
                    "contact_path":     t["contact_path"],
                    "vertical":         v,
                    "sent_via":         "human_click",
                    "created_at":       datetime.utcnow().isoformat(),
                    "personalization_gap": personalization_gap,
                    "one_pager_asset_path": asset_path,
                    "one_pager_mode":   "inline",
                }

                conn.execute("""
                    INSERT INTO outreach_events (thread_id, event_type, event_data)
                    VALUES (?, 'draft_created', ?)
                """, (thread_id, json.dumps(event_data)))

                conn.commit()
                if verbose:
                    print(f"    Draft:  {draft_id} — SAVED (pending approval)")

                created_drafts.append({
                    "thread_id": thread_id,
                    "lead_name": t["lead_name"],
                    "draft_id":  draft_id,
                    "vertical":  v,
                    "status":    "pending_approval",
                    "personalization_gap": personalization_gap,
                })

            else:
                conn.execute("""
                    INSERT INTO outreach_events (thread_id, event_type, event_data)
                    VALUES (?, 'draft_error', ?)
                """, (thread_id, json.dumps({"error": str(result)})))
                conn.commit()
                if verbose:
                    print(f"    ERROR:  {result.get('stderr', result)}")

            conn.close()

        all_created.extend(created_drafts)

    if verbose:
        print(f"\n{'='*55}")
        print(f"Drafts created: {len(all_created)}")
        if all_blocked:
            blocked_by_vertical = {}
            for v, f in all_blocked:
                blocked_by_vertical.setdefault(v, []).extend(f)
            print(f"Blocked verticals:")
            for v, fs in blocked_by_vertical.items():
                print(f"  {v}: {'; '.join(fs)}")
        print(f"Drafts are Gmail drafts — nothing has been sent.")
        print(f"After sending: thread_state becomes 'active', follow-up begins.")

    return {
        "status": "ok",
        "created": all_created,
        "blocked": all_blocked,
        "total": len(all_created),
    }


def _plain_to_html(text):
    """
    Convert plain-text email body to HTML with proper paragraph formatting.
    - Splits on blank lines (one or more consecutive newlines) into <p> paragraphs
    - Line breaks within paragraphs become <br>
    - URLs become proper <a href> links
    - ||__SIGNOFF__|| marker (at end of last body paragraph) triggers special signoff:
        The content after the marker becomes the signoff block, rendered as one <p>
        with <br> between lines. The raw https://www.uspai.io/ URL is replaced with
        an anchor tag. This produces the same rendering as the original drafts.
    """
    import re

    text = text.strip()
    # Split on blank lines: one or more consecutive newlines
    # Use lookahead (?=\n)\n+ to split AFTER the newline(s), keeping them in the preceding block
    raw_blocks = re.split(r'(?<=\n)\n+', text)

    signoff_lines = []
    html_parts = []

    for block in raw_blocks:
        block = block.rstrip('\n')
        if not block.strip():
            continue

        marker = '||__SIGNOFF__||'
        if marker in block:
            # Content before marker is the last body paragraph
            before = block[:block.find(marker)].rstrip()
            after = block[block.find(marker) + len(marker):].strip()
            # Emit body paragraph (without trailing br)
            if before:
                block_html = before.replace('\n', '<br>')
                block_html = re.sub(
                    r'(?<!href=["\'])(?<!src=["\'])(https?://[^\s<>"\']+)',
                    r'<a href="\1" target="_blank">\1</a>',
                    block_html,
                )
                html_parts.append(f'<p>{block_html}</p>')
            # Collect signoff lines (skip blank lines and the raw https://uspai.io/ URL)
            signoff_lines = [
                ln.strip()
                for ln in after.split('\n')
                if ln.strip() and not ln.strip().startswith('http')
            ]
            continue

        # Regular block
        block_html = block.replace('\n', '<br>')
        block_html = re.sub(
            r'(?<!href=["\'])(?<!src=["\'])(https?://[^\s<>"\']+)',
            r'<a href="\1" target="_blank">\1</a>',
            block_html,
        )
        html_parts.append(f'<p>{block_html}</p>')

    # Build signoff paragraph as one <p> with <br> separators and anchor URL
    if signoff_lines:
        signoff_html = '<br>'.join(signoff_lines)
        signoff_html += '<br><a href="https://www.uspai.io/" target="_blank">https://www.uspai.io/</a>'
        html_parts.append(f'<p>{signoff_html}</p>')

    return '\n'.join(html_parts)


# ── Subject line ─────────────────────────────────────────────────────────────

def build_subject(lead, offer):
    """Select subject line based on contact quality tier."""
    templates = offer.get("subject_line_templates", {})
    if lead["contact_quality"] in ("A", "B"):
        key = "A_tier_named"
    else:
        key = "C_tier_generic"

    choices = templates.get(key, [])
    if not choices:
        return f"workflow improvement for {lead['lead_name']}"
    idx = (lead['lead_id'] - 1) % len(choices)
    subject = choices[idx]
    subject = subject.replace("{firm_name}", lead["lead_name"])
    return subject


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="USP Draft Generator — Multi-Vertical (Corrected)")
    parser.add_argument("--vertical", help="Process specific vertical only (bypasses gate)")
    parser.add_argument("--feedback", help=f"Rejection tag: {' | '.join(sorted(REJECTION_TAGS))}")
    args = parser.parse_args()

    if args.feedback and args.feedback not in REJECTION_TAGS:
        print(f"ERROR: Unknown rejection tag '{args.feedback}'")
        print(f"Valid tags: {' | '.join(sorted(REJECTION_TAGS))}")
        sys.exit(1)

    result = run(vertical=args.vertical, feedback_tag=args.feedback)
    sys.exit(0 if result["status"] == "ok" else 1)
