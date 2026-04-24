#!/usr/bin/env python3
"""
Phase 1 Task 2: Two-Pass Backfill

PASS 1 — Derive contact_path, contact_quality, initial qualification_state
         for all 97 leads using the actual DB column layout.

         Data sources:
           contact_email        — actual email address (legacy column, correct data)
           contact_phone        — phone number (legacy column)
           enrichment_data      — GMB blob (has phone/top-level; email lives in contact_email)
           best_contact_path    — legacy contact classification (for cross-reference only)
           contact_named_role   — role text: 'accountant', 'Owner', 'partner' (not first name)

PASS 2 — Apply bounce and manual-disqualify overrides.
         contact_path is PRESERVED from Pass 1 (not overwritten).

Email sources:
  - contact_email column: primary source (e.g. kyle.dickmann@dickmanntaxgroup.com)
  - enrichment_data.email: NOT present in GMB blob — ignore

Name sources:
  - enrichment_data.first_name: NOT present in GMB blob — always absent
  - contact_named_role: role text, NOT a person name — ignore for first_name
  - email local-part: parse first word as potential first name (kyle, suzie, etc.)

Constants:
  PERSONAL_EMAIL_DOMAINS — consumer email domains → named_email tier
  BUSINESS_EMAIL_DOMAINS — known business domains → treated as named_business if name found
  ROLE_INBOX_LOCAL_PARTS — role inboxes → catchall_email tier
"""
import sqlite3, json, re, sys, os
from datetime import datetime

DB = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")

PERSONAL_EMAIL_DOMAINS = frozenset([
    'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com',
    'icloud.com','protonmail.com','live.com','msn.com','mail.com',
    'ymail.com','inbox.com',
])

# Domains that are business domains but where role-email is expected
BUSINESS_EMAIL_DOMAINS = frozenset([
    'cpa-firm-denver.com', 'coloradobusinesscpa.com', 'denverlegacylaw.com',
    'davidurbanlaw.com', 'pmillerlawoffice.com', 'enichenlaw.com',
])

ROLE_INBOX_LOCAL_PARTS = frozenset([
    'info','contact','hello','admin','office','support',
    'team','inquiries','enquiries','reception','frontdesk',
])


def parse_enrichment(raw):
    if not raw or not isinstance(raw, str):
        return {}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def extract_domain(email):
    if not email or '@' not in email:
        return ''
    return email.strip().rsplit('@', 1)[1].lower()


def extract_local_part(email):
    if not email or '@' not in email:
        return ''
    return email.strip().rsplit('@', 1)[0].lower()


def likely_person_name(local):
    """Return first_name if local-part looks like a person name, else None."""
    if not local:
        return None
    # Contains a dot, underscore, or is a single word that isn't a role term
    parts = re.split(r'[._-]+', local)
    first = parts[0].lower() if parts else ''
    if not first:
        return None
    # Reject role/institution terms
    role_terms = {'info','contact','hello','admin','office','team','support',
                  'hello','bookkeeping','accounting','law','legal','cpa','tax',
                  'estate','bookkeep','consult','service','sales','marketing'}
    if first in role_terms:
        return None
    # Accept if it looks like a plausible first name (no digits, reasonable length)
    if re.match(r'^[a-z]{2,12}$', first):
        return first.capitalize()
    return None


def classify_contact_path(email, phone):
    """
    Classify based on contact_email and contact_phone.

    Rules:
      email + personal domain (gmail.com etc.) + name in local → named_email, A
      email + personal domain + no name                        → named_email, A  (personal = quality signal)
      email + business domain + name in local                 → named_business, B
      email + business domain + role inbox (info@ etc.)       → catchall_email, D
      email + business domain + no name, not role inbox       → role_email, C
      no email + phone                                        → phone_only, uncontactable
      no email + no phone                                     → none, uncontactable
    """
    if email and email.lower() not in ('', 'none', 'null'):
        domain    = extract_domain(email)
        local     = extract_local_part(email)
        firstName = likely_person_name(local)

        is_personal = domain in PERSONAL_EMAIL_DOMAINS
        is_role     = local in ROLE_INBOX_LOCAL_PARTS

        if firstName:
            # Named person
            if is_personal:
                return ('named_email', 'A')
            else:
                return ('named_business', 'B')

        # No first name resolved
        if is_role:
            return ('catchall_email', 'D')
        else:
            return ('role_email', 'C')

    if phone and phone not in ('', 'NONE', 'null'):
        return ('phone_only', 'uncontactable')

    return ('none', 'uncontactable')


def derive_qualification_state(contact_quality):
    if contact_quality in ('A', 'B', 'C'):
        return 'qualified'
    # D (catchall) and uncontactable are both 'candidate' per spec
    return 'candidate'


def run():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    # Fetch all leads — use contact_email as primary email source
    leads = c.execute("""
        SELECT id, name, contact_email, contact_phone,
               contact_quality, best_contact_path,
               enrichment_data, enrichment_last_run,
               suppression_flag, send_readiness
        FROM leads
    """).fetchall()

    total = len(leads)
    print(f"Processing {total} leads...")

    pass1 = []   # list of dicts for UPDATE
    pass2 = []   # list of dicts for overrides

    for row in leads:
        lid, name, contact_email, contact_phone, \
            old_cq, old_bcp, enrichment_raw, elr, suppression, send_ready = row

        # Skip invalid
        if not lid:
            continue

        # Parse enrichment_data (GMB blob)
        ed = parse_enrichment(enrichment_raw)

        # Normalise
        email = contact_email.strip() if contact_email else ''
        if email.lower() in ('', 'none', 'null'):
            email = ''
        phone = contact_phone.strip() if contact_phone else ''
        if phone.lower() in ('', 'none', 'null'):
            phone = ''

        # Enrichment blob has phone at top level
        ed_phone = ed.get('phone', '')
        if ed_phone and phone == '':
            phone = ed_phone

        # Classify
        contact_path, contact_quality = classify_contact_path(email, phone)
        qualification_state = derive_qualification_state(contact_quality)
        outbound_state       = 'off_market'
        enrichment_ts        = elr or datetime.utcnow().isoformat()

        # Capture pass1 result for pass2 override check
        pass1.append({
            'lid':                lid,
            'contact_path':       contact_path,
            'contact_quality':    contact_quality,
            'qualification_state': qualification_state,
            'outbound_state':     outbound_state,
            'enrichment_last_run': enrichment_ts,
        })

        # PASS 2 override
        bounce_flag = suppression == 'bounce'
        wrong_fit   = suppression == 'wrong_fit'

        if bounce_flag:
            pass2.append({
                'lid':                lid,
                'qualification_state': 'disqualified',
                'outbound_state':      'suppressed',
                'suppression_flag':   'bounce',
            })
        elif wrong_fit:
            pass2.append({
                'lid':                lid,
                'qualification_state': 'disqualified',
                'outbound_state':      'off_market',
                'suppression_flag':   'wrong_fit',
            })

    # Execute Pass 1
    updated = 0
    for u in pass1:
        c.execute("""
            UPDATE leads SET
                contact_path         = ?,
                contact_quality     = ?,
                qualification_state = ?,
                outbound_state      = ?,
                enrichment_last_run = ?
            WHERE id = ?
        """, (u['contact_path'], u['contact_quality'], u['qualification_state'],
               u['outbound_state'], u['enrichment_last_run'], u['lid']))
        updated += 1

    # Execute Pass 2
    overridden = 0
    for o in pass2:
        c.execute("""
            UPDATE leads SET
                qualification_state = ?,
                outbound_state      = ?,
                suppression_flag    = ?
            WHERE id = ?
        """, (o['qualification_state'], o['outbound_state'], o['suppression_flag'], o['lid']))
        overridden += 1

    conn.commit()

    # Summary
    rows = c.execute("""
        SELECT qualification_state, contact_path, contact_quality,
               outbound_state, suppression_flag, COUNT(*) as cnt
        FROM leads
        GROUP BY qualification_state, contact_path, contact_quality,
                 outbound_state, suppression_flag
        ORDER BY qualification_state, contact_quality, contact_path
    """).fetchall()

    print(f"\nBackfill complete. {updated}/{total} leads processed.")
    print(f"Pass 2 overrides applied: {overridden}")
    print()
    print(f"{'qual_state':<18} {'contact_path':<22} {'cq':<3} {'out_state':<18} {'suppress':<12} {'cnt'}")
    print("-"*88)
    for r in rows:
        print(f"{r[0] or 'NULL':<18} {r[1] or 'NULL':<22} {r[2] or '?':<3} {r[3] or 'NULL':<18} {str(r[4] or ''):<12} {r[5]}")

    # Subtotals
    print()
    print("=== QUALIFICATION STATE TOTALS ===")
    for r in c.execute("SELECT qualification_state, COUNT(*) FROM leads GROUP BY qualification_state ORDER BY qualification_state").fetchall():
        print(f"  {r[0] or 'NULL'}: {r[1]}")

    print("\n=== CONTACT QUALITY TOTALS ===")
    for r in c.execute("SELECT contact_quality, COUNT(*) FROM leads GROUP BY contact_quality ORDER BY contact_quality").fetchall():
        print(f"  {r[0] or 'NULL'}: {r[1]}")

    print("\n=== OUTBOUND STATE TOTALS ===")
    for r in c.execute("SELECT outbound_state, COUNT(*) FROM leads GROUP BY outbound_state ORDER BY outbound_state").fetchall():
        print(f"  {r[0] or 'NULL'}: {r[1]}")

    print("\n=== CONTACT PATH TOTALS ===")
    for r in c.execute("SELECT contact_path, COUNT(*) FROM leads GROUP BY contact_path ORDER BY contact_path").fetchall():
        print(f"  {r[0] or 'NULL'}: {r[1]}")

    conn.close()


if __name__ == "__main__":
    run()
