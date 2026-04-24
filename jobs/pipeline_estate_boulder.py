#!/usr/bin/env python3
"""
USP Phase 1 — Full Pipeline: estate_planning_probate x Boulder CO
Scope: discovery (done), Details phase, enrichment, targeting, suppression.

Do not generate review packets. No Gmail drafts.
Isolation: only ~/.hermes/Hermes-USP-v1/
"""
import json, re, sys, time, urllib.request, urllib.parse
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))
import sqlite3

_DB_PATH = USP_ROOT / "usp.db"
_CONFIG_PATH = USP_ROOT / "config.json"
_DETAILS_OUT = USP_ROOT / "estate_boulder_details.json"
_ENRICH_OUT  = USP_ROOT / "enrich_results_estate_boulder.json"

DETAILS_FIELDS = "website,formatted_address,geometry,icon,name,opening_hours,photos,place_id,rating,types,user_ratings_total,url"

STAFF_PATTERNS   = ["about","team","attorneys","lawyers","staff","our-people","meet-us"," profiles","bio"]
CONTACT_PATTERNS = ["contact","reach-us","get-in-touch","schedule","consultation","appointment"]
EMAIL_PATTERN   = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

GENERIC_PREFIXES = ["info","contact","hello","admin","support","sales","office","web","website","help","service"]
WEAK_DOMAINS    = {'gmail.com','yahoo.com','hotmail.com','outlook.com','aol.com'}
HOSTING_DOMAINS = {'myftpupload.com'}
AGENCY_DOMAINS  = {'designbuildweb.co'}
PLACEHOLDER_DOMAINS = {'domain.com'}
SYSTEM_DOMAINS  = {'sentry.io','sentry.wixpress.com','sentry-next.wixpress.com'}
IMAGE_EXTENSIONS = {'.png','.jpg','.jpeg','.gif','.webp','.svg','.ico'}

def _cfg():
    with open(_CONFIG_PATH) as f:
        return json.load(f)

def _now():
    return datetime.now(timezone.utc).isoformat()

def _audit(conn, action, engine, detail, result=None):
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, engine, detail, result, _now()))
    conn.commit()

def _places_details(place_id, api_key):
    url = (f"https://maps.googleapis.com/maps/api/place/details/json"
           f"?place_id={place_id}&fields={DETAILS_FIELDS}&key={api_key}")
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode())

def _fetch_homepage(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            ct = resp.headers.get("Content-Type","")
            charset = "utf-8"
            if "charset=" in ct:
                charset = ct.split("charset=")[-1]
            return resp.read().decode(charset, errors="replace")
    except:
        return ""

def _extract_emails(html):
    emails = set()
    for m in EMAIL_PATTERN.finditer(html):
        e = m.group().lower()
        if not any(b in e for b in ["example.com","test.com","noreply","nobody"]):
            emails.add(e)
    return list(emails)

def _check_signals(html_lower):
    return (any(p in html_lower for p in STAFF_PATTERNS),
            any(p in html_lower for p in CONTACT_PATTERNS))

def _classify_email_path(email):
    e = email.lower()
    for prefix in GENERIC_PREFIXES:
        if e.startswith(prefix + "@"):
            return "generic_inbox"
    return "named_contact_direct"

def _evaluate_email(email):
    """Returns (classification, reason)."""
    domain = email.lower().split('@')[1] if '@' in email else ''
    if any(ext in domain for ext in IMAGE_EXTENSIONS):
        return 'suppress', 'image_filename_misparsed_as_email'
    if domain in PLACEHOLDER_DOMAINS:
        return 'suppress', 'placeholder_email_domain'
    if domain in SYSTEM_DOMAINS:
        return 'suppress', 'system_error_tracker_address'
    if domain in HOSTING_DOMAINS:
        return 'hold', 'hosting_file_service_domain'
    if domain in AGENCY_DOMAINS:
        return 'hold', 'agency_vendor_domain'
    if domain in WEAK_DOMAINS:
        return 'hold', f'free_personal_email_{domain}'
    return 'packet_eligible', f'clean_{_classify_email_path(email)}'

def _normalize_outcome(emails):
    """
    Returns (outcome, reason, email_used, domain).
    Scans all emails: suppress artifacts, prefer packet_eligible > hold.
    If no emails and website found: retain_phone_or_future_contact.
    """
    if not emails:
        return 'retain_phone_or_future_contact', 'no_email_found_website_present', '', ''
    first_clean = first_hold = suppress_reason = None
    for email in emails:
        cls, reason = _evaluate_email(email)
        domain = email.lower().split('@')[1] if '@' in email else ''
        if cls == 'suppress':
            suppress_reason = reason
            continue  # skip artifacts, keep scanning
        elif cls == 'packet_eligible' and first_clean is None:
            first_clean = {'email': email, 'reason': reason, 'domain': domain}
        elif cls == 'hold' and first_hold is None:
            first_hold = {'email': email, 'reason': reason, 'domain': domain}
    if first_clean:
        return 'packet_eligible', first_clean['reason'], first_clean['email'], first_clean['domain']
    if first_hold:
        return 'hold_requires_review', first_hold['reason'], first_hold['email'], first_hold['domain']
    if suppress_reason:
        return 'suppress', suppress_reason, '', ''
    return 'suppress', 'all_emails_are_infrastructure_artifacts', '', ''

# ── PHASE 1: DETAILS ───────────────────────────────────────────────────────────

print("=" * 70)
print("[DETAILS] Fetching Google Places Details for Boulder leads...")
cfg = _cfg()
api_key = cfg["discovery"]["google_places_api_key"]

conn = sqlite3.connect(str(_DB_PATH))
conn.row_factory = sqlite3.Row

# All Boulder estate leads (IDs 71-97)
leads = conn.execute("""
    SELECT id, name, enrichment_data, notes
    FROM leads
    WHERE id BETWEEN 71 AND 97
    ORDER BY id
""").fetchall()
print(f"  {len(leads)} leads to detail")

details_results = []
details_success = details_failed = 0

for lead in leads:
    ed = json.loads(lead["enrichment_data"] or "{}")
    notes = json.loads(lead["notes"] or "{}")
    place_id = notes.get("place_id","")

    if not place_id:
        print(f"  ID {lead['id']}: no place_id in notes, skipping")
        details_failed += 1
        continue

    try:
        raw = _places_details(place_id, api_key)
    except Exception as e:
        print(f"  ID {lead['id']}: API error {e}")
        details_failed += 1
        continue

    if raw.get("status") != "OK":
        print(f"  ID {lead['id']}: status={raw.get('status')}")
        details_failed += 1
        continue

    result = raw.get("result", {})
    website = result.get("website","")
    phone   = result.get("formatted_phone_number","")
    rating  = result.get("rating")
    reviews = result.get("user_ratings_total", 0)
    addr    = result.get("formatted_address","")

    updated_ed = {
        **ed,
        "website": website,
        "phone": phone,
        "rating": rating,
        "reviews": reviews,
        "address": addr,
        "details_fetched": _now(),
    }

    conn.execute("""
        UPDATE leads SET enrichment_data = ?, enrichment_last_run = ?
        WHERE id = ?
    """, (json.dumps(updated_ed), _now(), lead["id"]))

    details_results.append({
        "id": lead["id"], "name": lead["name"],
        "website": website, "phone": phone,
        "rating": rating, "reviews": reviews,
    })
    print(f"  ID {lead['id']:3d}: {lead['name'][:40]:40s} | website={'YES' if website else 'NO':3s} | rating={rating} | {reviews} reviews")
    details_success += 1
    time.sleep(0.3)

conn.commit()
conn.close()

with open(_DETAILS_OUT, "w") as f:
    json.dump(details_results, f, indent=2)
print(f"[DETAILS] Done: {details_success} success / {details_failed} failed")
print("=" * 70)

# ── PHASE 2: ENRICHMENT ───────────────────────────────────────────────────────

print("[ENRICH] Fetching homepages for Boulder leads...")

conn = sqlite3.connect(str(_DB_PATH))
conn.row_factory = sqlite3.Row
leads = conn.execute("""
    SELECT id, name, enrichment_data
    FROM leads
    WHERE id BETWEEN 71 AND 97
    ORDER BY id
""").fetchall()

enrich_results = []
enrich_success = enrich_failed = 0
email_found = staff_sig = contact_sig = 0

for lead in leads:
    ed = json.loads(lead["enrichment_data"] or "{}")
    website = ed.get("website","")

    if not website:
        enrich_failed += 1
        continue

    if not website.startswith("http"):
        website = "https://" + website

    html = _fetch_homepage(website)
    html_lower = html.lower()
    has_staff, has_contact = _check_signals(html_lower)
    emails = _extract_emails(html) if html else []

    updated_ed = {**ed,
        "homepage_fetched": _now(),
        "homepage_success": bool(html),
        "homepage_length": len(html),
        "has_staff": has_staff,
        "has_contact": has_contact,
        "emails": emails,
    }
    conn.execute("""
        UPDATE leads SET enrichment_data = ?, enrichment_last_run = ?
        WHERE id = ?
    """, (json.dumps(updated_ed), _now(), lead["id"]))

    enrich_results.append({
        "id": lead["id"], "name": lead["name"],
        "website": website,
        "has_staff": has_staff, "has_contact": has_contact,
        "emails": emails,
        "html_length": len(html),
        "homepage_success": bool(html),
    })

    if emails: email_found += 1
    if has_staff: staff_sig += 1
    if has_contact: contact_sig += 1
    if html: enrich_success += 1
    else: enrich_failed += 1

    status = "HAS_EMAIL" if emails else "PHONE_ONLY"
    print(f"  ID {lead['id']:3d}: {lead['name'][:40]:40s} | staff={has_staff} | contact={has_contact} | {status}")
    time.sleep(0.8)

conn.commit()
conn.close()

with open(_ENRICH_OUT, "w") as f:
    json.dump(enrich_results, f, indent=2)
print(f"[ENRICH] Done: {enrich_success} success / {enrich_failed} failed")
print(f"         {email_found} with email | {staff_sig} staff pages | {contact_sig} contact pages")
print("=" * 70)

# ── PHASE 3: TARGETING ─────────────────────────────────────────────────────────

print("[TARGET] Running contact targeting on Boulder leads...")

conn = sqlite3.connect(str(_DB_PATH))
conn.row_factory = sqlite3.Row
leads = conn.execute("""
    SELECT id, name, enrichment_data
    FROM leads
    WHERE id BETWEEN 71 AND 97
    ORDER BY id
""").fetchall()

# Load enrich results for cross-reference
enrich_by_id = {}
with open(_ENRICH_OUT) as f:
    for e in json.load(f):
        enrich_by_id[e['id']] = e

packet_eligible = retain_phone = hold_review = suppress_count = 0
suppressed_leads = []
held_leads = []
retained_phone_leads = []
clean_leads = []

for lead in leads:
    lid = lead["id"]
    e = enrich_by_id.get(lid, {})
    emails = e.get("emails", [])
    outcome, reason, email_used, domain = _normalize_outcome(emails)

    if outcome == 'packet_eligible':
        path = _classify_email_path(email_used)
        quality = 'A' if path == 'named_contact_direct' else 'C'
        conn.execute("""
            UPDATE leads SET
                best_contact_path = ?, contact_email = ?, contact_quality = ?,
                send_readiness = 'ready', updated_at = ?
            WHERE id = ?
        """, (path, email_used, quality, _now(), lid))
        packet_eligible += 1
        clean_leads.append({'id': lid, 'name': lead['name'], 'email': email_used,
                           'path': path, 'quality': quality})

    elif outcome == 'retain_phone_or_future_contact':
        conn.execute("""
            UPDATE leads SET
                best_contact_path = 'phone_or_future_contact',
                contact_email = NULL, contact_quality = 'B',
                send_readiness = 'retain_phone_or_future_contact', updated_at = ?
            WHERE id = ?
        """, (_now(), lid))
        retain_phone += 1
        retained_phone_leads.append({'id': lid, 'name': lead['name'], 'reason': reason})

    elif outcome == 'hold_requires_review':
        path = _classify_email_path(email_used)
        quality = 'A' if path == 'named_contact_direct' else 'C'
        conn.execute("""
            UPDATE leads SET
                best_contact_path = ?, contact_email = ?, contact_quality = ?,
                send_readiness = 'hold_requires_review', updated_at = ?
            WHERE id = ?
        """, (path, email_used, quality, _now(), lid))
        hold_review += 1
        held_leads.append({'id': lid, 'name': lead['name'], 'email': email_used,
                           'reason': reason})

    else:  # suppress
        conn.execute("""
            UPDATE leads SET
                best_contact_path = 'suppressed', contact_email = NULL,
                contact_quality = 'D', send_readiness = 'suppressed',
                suppression_flag = 'wrong_fit', updated_at = ?
            WHERE id = ?
        """, (_now(), lid))
        suppress_count += 1
        suppressed_leads.append({'id': lid, 'name': lead['name'], 'reason': reason})

    _audit(conn, "boulder_targeting", "contact_targeting",
           f"ID {lid} ({lead['name']}): {outcome} / {reason}",
           outcome)

conn.commit()
conn.close()

print(f"[TARGET] Results: {packet_eligible} packet_eligible | {retain_phone} retain_phone | {hold_review} hold | {suppress_count} suppress")

print("\n[CLEAN LEADS — PACKET ELIGIBLE]:")
for l in clean_leads:
    print(f"  ID {l['id']:3d} | {l['name'][:50]:50s} | {l['email']:45s} | {l['path']} / {l['quality']}")

print("\n[HELD — REQUIRES REVIEW]:")
for l in held_leads:
    print(f"  ID {l['id']:3d} | {l['name'][:50]:50s} | {l['email']} | {l['reason']}")

print("\n[SUPPRESSED]:")
for l in suppressed_leads:
    print(f"  ID {l['id']:3d} | {l['name'][:50]:50s} | {l['reason']}")

print("\n[RETAINED — PHONE/FUTURE]:")
for l in retained_phone_leads:
    print(f"  ID {l['id']:3d} | {l['name'][:50]:50s} | {l['reason']}")

print("\n[DATABASE UPDATED]")
