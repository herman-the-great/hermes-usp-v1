#!/usr/bin/env python3
"""
USP Phase 0 — Insert + Enrich: accounting_bookkeeping × Denver (top 23 website-bearing).
No targeting. No review packets. No Gmail drafts.
"""
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))

import sqlite3
import subprocess

_DB_PATH = USP_ROOT / "usp.db"
_CONFIG_PATH = USP_ROOT / "config.json"

def _log(action, detail=None, result=None):
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, "insert_enrich", detail, result, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()

def _load_config():
    with open(_CONFIG_PATH) as f:
        return json.load(f)

def insert_leads(candidates):
    """Insert 23 leads into usp.db leads table."""
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    inserted = []
    skipped = []

    for c in candidates:
        # Check for duplicate by name
        existing = conn.execute(
            "SELECT id FROM leads WHERE LOWER(name) = ?", (c["name"].lower(),)
        ).fetchone()
        if existing:
            skipped.append(c["name"])
            continue

        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO leads (
                name, category, stage, source, source_url,
                contact_method, contact_phone,
                enrichment_data, enrichment_last_run,
                send_readiness, assigned_vertical,
                business_legitimacy, contact_quality,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            c["name"],
            "auto_discovery",
            "identified",
            "google_places",
            c.get("maps_url", ""),
            "email",
            c.get("phone", ""),
            json.dumps({"website": c.get("website", ""), "rating": c.get("rating"), "reviews": c.get("reviews")}),
            None,
            "not_assessed",
            "accounting_bookkeeping",
            "OPERATIONAL",
            "phone_only" if not c.get("website") else "has_website",
            now, now
        ))
        inserted.append(c["name"])

    conn.commit()
    conn.close()
    return inserted, skipped

def enrich_lead(lead_id, api_key):
    """Enrich one lead: Places Details (already done) + homepage fetch + contact extraction."""
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    lead = conn.execute(
        "SELECT id, name, enrichment_data FROM leads WHERE id = ?", (lead_id,)
    ).fetchone()
    if not lead:
        conn.close()
        return {"lead_id": lead_id, "status": "not_found"}

    enrichment = json.loads(lead["enrichment_data"] or "{}")
    website = enrichment.get("website", "")
    conn.close()

    result_data = {
        "website": website,
        "rating": enrichment.get("rating"),
        "reviews": enrichment.get("reviews"),
        "contacts": [],
        "has_staff_page": False,
        "has_contact_page": False,
        "enrichment_success": False,
    }

    if not website:
        return {"lead_id": lead_id, "status": "no_website", **result_data}

    # Fetch homepage
    try:
        html = subprocess.run(
            ["curl", "-s", "-L", "--max-time", "10", "-A",
             "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
             website],
            capture_output=True, text=True, timeout=12
        ).stdout
    except Exception as e:
        _log("enrich_homepage_error", detail=f"lead_id={lead_id}: {e}")
        return {"lead_id": lead_id, "status": "homepage_error", **result_data}

    if not html or len(html) < 200:
        return {"lead_id": lead_id, "status": "homepage_empty", **result_data}

    html_lower = html.lower()

    # Detect staff/team/contact pages
    staff_keywords = ["our team", "about us", "meet the", "our staff", "attorneys", "accountants", "advisors", "principals"]
    contact_keywords = ["contact", "get in touch", "reach us", "schedule"]

    result_data["has_staff_page"] = any(k in html_lower for k in staff_keywords)
    result_data["has_contact_page"] = any(k in html_lower for k in contact_keywords)

    # Extract email addresses
    emails = list(set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html)))
    # Filter out common noise
    emails = [e for e in emails if not any(x in e.lower() for x in ["noreply", "no-reply", "example", "test"])]

    # Extract phone numbers
    phones = list(set(re.findall(r"\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", html)))

    result_data["emails_found"] = emails[:10]  # cap at 10
    result_data["phones_found"] = phones[:5]
    result_data["enrichment_success"] = True

    # Update lead
    conn2 = sqlite3.connect(str(_DB_PATH))
    now = datetime.now(timezone.utc).isoformat()
    conn2.execute("""
        UPDATE leads SET
            enrichment_data = ?,
            enrichment_last_run = ?,
            contact_email = ?,
            contact_phone = COALESCE(NULLIF(contact_phone,''), ?),
            contact_quality = ?,
            best_contact_path = ?
        WHERE id = ?
    """, (
        json.dumps(result_data),
        now,
        emails[0] if emails else "",
        phones[0] if phones else "",
        "has_email" if emails else "phone_only",
        "named_contact_direct" if emails else ("has_website_no_email" if website else "phone_only"),
        lead_id
    ))
    conn2.commit()
    conn2.close()

    return {"lead_id": lead_id, "status": "success", **result_data}

# ── Load the 23 candidates from the Details results ──────────────────
details_path = USP_ROOT / "accounting_denver_details.json"
with open(details_path) as f:
    all_details = json.load(f)

EXCLUDE_NAMES = {"SBA CPA", "Elite Bookkeeping Solutions"}

candidates = []
for d in all_details:
    if d["name"] in EXCLUDE_NAMES:
        continue
    if d.get("website") in ("NONE", "ERROR", ""):
        continue  # no website = not eligible per approval
    candidates.append({
        "name": d["name"],
        "website": d["website"],
        "phone": d.get("phone", ""),
        "maps_url": d.get("url", ""),
        "rating": d.get("rating"),
        "reviews": d.get("reviews"),
    })

print(f"[INSERT] Candidates to insert: {len(candidates)}")
for c in candidates:
    print(f"  - {c['name']} | {c['website'][:50]}")

# ── Insert ────────────────────────────────────────────────────────────
print("\n[INSERT] Running inserts...")
inserted, skipped = insert_leads(candidates)
print(f"[INSERT] Inserted: {len(inserted)}")
print(f"[INSERT] Skipped (duplicates): {len(skipped)}")

# ── Get new lead IDs ─────────────────────────────────────────────────
conn = sqlite3.connect(str(_DB_PATH))
conn.row_factory = sqlite3.Row
new_leads = conn.execute("""
    SELECT id, name FROM leads
    WHERE assigned_vertical = 'accounting_bookkeeping'
    AND source = 'google_places'
    AND category = 'auto_discovery'
    AND created_at >= datetime('now', '-5 minutes')
    ORDER BY id
""").fetchall()
conn.close()

print(f"\n[ENRICH] New leads to enrich: {len(new_leads)}")

cfg = _load_config()
api_key = cfg["discovery"]["google_places_api_key"]

enrich_results = []
for lead in new_leads:
    print(f"  Enriching: {lead['name'][:50]}...", end=" ", flush=True)
    r = enrich_lead(lead["id"], api_key)
    status = r.get("status", "unknown")
    emails = r.get("emails_found", [])
    phones = r.get("phones_found", [])
    staff = r.get("has_staff_page", False)
    contact = r.get("has_contact_page", False)
    print(f"{status} | emails={len(emails)} | staff={staff} | contact={contact}")
    enrich_results.append(r)
    time.sleep(0.5)

# ── Summary ─────────────────────────────────────────────────────────
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
success = sum(1 for r in enrich_results if r.get("status") == "success")
no_website = sum(1 for r in enrich_results if r.get("status") == "no_website")
homepage_err = sum(1 for r in enrich_results if r.get("status") in ("homepage_error", "homepage_empty"))

emails_found = sum(1 for r in enrich_results if r.get("emails_found"))
staff_found = sum(1 for r in enrich_results if r.get("has_staff_page"))
contact_found = sum(1 for r in enrich_results if r.get("has_contact_page"))

print(f"  Inserted: {len(inserted)}")
print(f"  Skipped duplicates: {len(skipped)}")
print(f"  Enriched successfully: {success}")
print(f"  No website (skipped): {no_website}")
print(f"  Homepage error/empty: {homepage_err}")
print(f"  Email addresses found: {emails_found}/23")
print(f"  Staff page signal: {staff_found}/23")
print(f"  Contact page signal: {contact_found}/23")

# ── Write results for reporting ──────────────────────────────────────
with open(USP_ROOT / "insert_enrich_results.json", "w") as f:
    json.dump({
        "inserted": inserted,
        "skipped": skipped,
        "enrich_results": enrich_results,
        "summary": {
            "inserted_count": len(inserted),
            "skipped_count": len(skipped),
            "enriched_success": success,
            "no_website": no_website,
            "homepage_err": homepage_err,
            "emails_found": emails_found,
            "staff_found": staff_found,
            "contact_found": contact_found,
        }
    }, f, default=str)
print(f"\n[Done] Results saved to insert_enrich_results.json")
