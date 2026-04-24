#!/usr/bin/env python3
"""
USP Phase 1 — Normalize estate_planning_probate x Denver
Scope: Re-review 27 leads using corrected policy.
Policy:
  packet_eligible        — clean email (named personal OR clean generic inbox), not on suspicious/weak domain
  retain_phone_or_future_contact — website-bearing, legit firm with no email found; valid for phone/outreach
  hold_requires_review  — email exists but domain is Gmail, hosting, agency, or other needing human review
  suppress              — placeholder/test/artifact email ONLY; no firm is suppressed merely for having no email

No review packets. No Gmail drafts. No broadening.
"""
import json, sqlite3, sys
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))

enrich_by_id = {}
with open(USP_ROOT / 'enrich_results_estate_denver.json') as f:
    for e in json.load(f):
        enrich_by_id[e['id']] = e

# ── Email classification helpers ──────────────────────────────────────────────

GENERIC_PREFIXES = [
    "info", "contact", "hello", "admin", "support", "sales",
    "office", "web", "website", "help", "service",
]

WEAK_FIT_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
}

HOSTING_DOMAINS = {
    'myftpupload.com',
}

AGENCY_VENDOR_DOMAINS = {
    'designbuildweb.co',
}

PLACEHOLDER_DOMAINS = {
    'domain.com',
}

SYSTEM_DOMAINS = {
    'sentry.io', 'sentry.wixpress.com', 'sentry-next.wixpress.com',
}

IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico'}

def _classify_email_path(email: str):
    """Returns 'named_contact_direct' or 'generic_inbox' per USP model."""
    email_lower = email.lower()
    for prefix in GENERIC_PREFIXES:
        if email_lower.startswith(prefix + "@"):
            return "generic_inbox"
    return "named_contact_direct"


def _evaluate_email(email: str):
    """
    Returns (classification, reason).
    classification: packet_eligible | hold_requires_review | suppress
    """
    domain = email.lower().split('@')[1] if '@' in email else ''

    # 1. Image filename artifact — suppress
    if any(ext in domain for ext in IMAGE_EXTENSIONS):
        return 'suppress', 'image_filename_misparsed_as_email'

    # 2. Placeholder test domain — suppress
    if domain in PLACEHOLDER_DOMAINS:
        return 'suppress', 'placeholder_email_domain'

    # 3. System/error-tracker addresses — suppress (not firm contacts)
    if domain in SYSTEM_DOMAINS:
        return 'suppress', 'system_error_tracker_address'

    # 4. Hosting/file-service domain — hold (not a firm email)
    if domain in HOSTING_DOMAINS:
        return 'hold_requires_review', 'hosting_file_service_domain'

    # 5. Agency/vendor domain — hold (may be hosted by third party)
    if domain in AGENCY_VENDOR_DOMAINS:
        return 'hold_requires_review', 'agency_vendor_domain'

    # 6. Free personal email (Gmail, Yahoo, etc.) — hold for human review
    if domain in WEAK_FIT_DOMAINS:
        path = _classify_email_path(email)
        return 'hold_requires_review', f'free_personal_email_{domain}'

    # 7. Clean — packet eligible
    path = _classify_email_path(email)
    return 'packet_eligible', f'clean_{path}'


# ── Classify all 27 leads ─────────────────────────────────────────────────────

print("=" * 100)
print("CORRECTED ESTATE CLASSIFICATION (per corrected policy):")
print(f"{'ID':3} | {'Name':42} | {'Email':50} | Outcome             | Reason")
print("-" * 100)

outcomes = {}  # lead_id -> final outcome dict

for lead_id in range(44, 71):
    e = enrich_by_id.get(lead_id, {})
    name = e.get('name', '')
    emails = e.get('emails', [])
    website = e.get('website', '')
    phone = e.get('phone', '')

    # Default: no email found
    if not emails:
        # Policy: do NOT suppress a website-bearing firm merely because no email was found
        if website:
            outcome = 'retain_phone_or_future_contact'
            reason = 'website_bearing_no_email_found_legit_firm'
        else:
            outcome = 'suppress'
            reason = 'no_website_no_email_no_phone'
        print(f"{lead_id:3d} | {name[:42]:42s} | [no email]                        | {outcome:28s} | {reason}")
        outcomes[lead_id] = {'outcome': outcome, 'reason': reason,
                             'email': '', 'domain': '', 'name': name}
    else:
        # Multiple emails possible — evaluate each in order, take first non-suppress
        first_hold = None
        first_clean = None
        suppress_reason = None

        for email in emails:
            classification, reason = _evaluate_email(email)
            domain = email.lower().split('@')[1] if '@' in email else ''
            if classification == 'suppress':
                # System/placeholder artifacts: skip and continue scanning
                # These are NOT firm contacts — infrastructure noise, not staff page contacts
                # e.g. sentry.io error trackers, image filenames, domain.com placeholders
                continue
            elif classification == 'packet_eligible' and first_clean is None:
                first_clean = {'email': email, 'reason': reason, 'domain': domain}
            elif classification == 'hold_requires_review' and first_hold is None:
                first_hold = {'email': email, 'reason': reason, 'domain': domain}

        # Decision: suppress only if NO usable contact found after scanning all emails
        # Otherwise: packet_eligible > hold > phone_or_future_contact
        if first_clean:
            outcome = 'packet_eligible'
            reason = first_clean['reason']
            email_used = first_clean['email']
            domain = first_clean['domain']
        elif first_hold:
            outcome = 'hold_requires_review'
            reason = first_hold['reason']
            email_used = first_hold['email']
            domain = first_hold['domain']
        elif suppress_reason:
            outcome = 'suppress'
            reason = suppress_reason
            email_used = ''
            domain = ''
        else:
            # All emails were suppress-classified (all artifacts) — suppress as no valid firm contact
            outcome = 'suppress'
            reason = 'all_emails_are_infrastructure_artifacts'
            email_used = ''
            domain = ''

        print(f"{lead_id:3d} | {name[:42]:42s} | {email_used:50s} | {outcome:28s} | {reason}")
        outcomes[lead_id] = {'outcome': outcome, 'reason': reason,
                             'email': email_used, 'domain': domain, 'name': name}

print()
print("=" * 100)
print("OUTCOME SUMMARY:")
for bucket in ['packet_eligible', 'retain_phone_or_future_contact',
               'hold_requires_review', 'suppress']:
    count = sum(1 for o in outcomes.values() if o['outcome'] == bucket)
    print(f"  {bucket}: {count}")

# ── Write corrected state to usp.db ───────────────────────────────────────────

conn = sqlite3.connect(str(USP_ROOT / 'usp.db'))
now = datetime.now(timezone.utc).isoformat()

for lead_id, d in outcomes.items():
    outcome = d['outcome']
    reason = d['reason']
    email = d['email']
    domain = d['domain']

    if outcome == 'packet_eligible':
        path = _classify_email_path(email)
        quality = 'A' if path == 'named_contact_direct' else 'C'
        conn.execute("""
            UPDATE leads SET
                best_contact_path = ?,
                contact_email = ?,
                contact_quality = ?,
                send_readiness = 'ready',
                suppression_flag = NULL,
                updated_at = ?
            WHERE id = ?
        """, (path, email, quality, now, lead_id))
        print(f"  RETAINED (packet_eligible): ID {lead_id} ({d['name']}) — {path} / {quality} / {email}")

    elif outcome == 'retain_phone_or_future_contact':
        conn.execute("""
            UPDATE leads SET
                best_contact_path = 'phone_or_future_contact',
                contact_email = NULL,
                contact_quality = 'B',
                send_readiness = 'retain_phone_or_future_contact',
                suppression_flag = NULL,
                updated_at = ?
            WHERE id = ?
        """, (now, lead_id))
        print(f"  RETAINED (phone/future): ID {lead_id} ({d['name']}) — no email found, website present")

    elif outcome == 'hold_requires_review':
        path = _classify_email_path(email)
        quality = 'A' if path == 'named_contact_direct' else 'C'
        conn.execute("""
            UPDATE leads SET
                best_contact_path = ?,
                contact_email = ?,
                contact_quality = ?,
                send_readiness = 'hold_requires_review',
                suppression_flag = NULL,
                updated_at = ?
            WHERE id = ?
        """, (path, email, quality, now, lead_id))
        print(f"  HELD: ID {lead_id} ({d['name']}) — {reason} / {email}")

    else:  # suppress
        conn.execute("""
            UPDATE leads SET
                best_contact_path = 'suppressed',
                contact_email = NULL,
                contact_quality = 'D',
                send_readiness = 'suppressed',
                suppression_flag = 'wrong_fit',
                updated_at = ?
            WHERE id = ?
        """, (now, lead_id))
        print(f"  SUPPRESSED: ID {lead_id} ({d['name']}) — {reason}")

conn.commit()
conn.close()
print("\nDatabase updated.")
