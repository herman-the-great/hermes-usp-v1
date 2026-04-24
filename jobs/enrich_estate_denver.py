#!/usr/bin/env python3
"""
USP Phase 1 — Enrichment: estate_planning_probate x Denver
Fetches homepage for each of the 27 leads to detect staff/contact pages and extract emails.
Stores results in enrichment_data JSONB on each lead.
"""
import json
import sys
import time
import urllib.request
import urllib.parse
import re
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))

import sqlite3

_DB_PATH = USP_ROOT / "usp.db"
_CONFIG_PATH = USP_ROOT / "config.json"
_RESULTS_PATH = USP_ROOT / "enrich_results_estate_denver.json"

STAFF_PATTERNS = ["about", "team", "attorneys", "lawyers", "staff", "our-people", "meet-us", " profiles", "bio"]
CONTACT_PATTERNS = ["contact", "reach-us", "get-in-touch", "schedule", "consultation", "appointment"]
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _fetch_homepage(url):
    """Fetch homepage HTML, return text content."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            charset = "utf-8"
            content_type = resp.headers.get("Content-Type", "")
            if "charset=" in content_type:
                charset = content_type.split("charset=")[-1]
            html = resp.read().decode(charset, errors="replace")
        return html
    except Exception as e:
        return ""


def _extract_emails(text):
    """Extract unique emails from text."""
    emails = set()
    for match in EMAIL_PATTERN.finditer(text):
        email = match.group().lower()
        if not any(bad in email for bad in ["example.com", "test.com", "noreply", "nobody"]):
            emails.add(email)
    return list(emails)


def _check_page_signals(html_lower):
    """Return has_staff, has_contact."""
    staff = any(p in html_lower for p in STAFF_PATTERNS)
    contact = any(p in html_lower for p in CONTACT_PATTERNS)
    return staff, contact


def _classify_email_quality(emails):
    """Return (quality, path, confidence) tuple."""
    if not emails:
        return "none", "phone_only", 0.4

    for email in emails:
        parts = email.split("@")[0].lower()
        if any(name in parts for name in ["info", "contact", "hello", "admin", "office", "law"]):
            return "generic_inbox", "generic_inbox", 0.6
        # Named personal email
        return "personal", "named_contact_direct", 0.8

    return "generic_inbox", "generic_inbox", 0.6


def _now():
    return datetime.now(timezone.utc).isoformat()


def _log(conn, action, engine, detail, result=None):
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, engine, detail, result, _now())
    )
    conn.commit()


def main():
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load all estate_planning leads with enrichment_data from Details
    leads = conn.execute("""
        SELECT id, name, enrichment_data
        FROM leads
        WHERE id >= 44 AND id <= 70
        ORDER BY id
    """).fetchall()

    print(f"[ENRICH] Starting homepage enrichment for {len(leads)} leads...")

    results = []
    success = 0
    failed = 0
    email_found = 0
    staff_page = 0
    contact_page = 0

    for lead in leads:
        ed = json.loads(lead["enrichment_data"] or "{}")
        website = ed.get("website", "")

        if not website:
            print(f"  ID {lead['id']}: {lead['name'][:40]:40} — NO WEBSITE, skipping")
            failed += 1
            _log(conn, "estate_enrich_skip", "estate_denver_enrich",
                 f"ID {lead['id']} no website", "skipped")
            continue

        # Normalize URL
        if not website.startswith("http"):
            website = "https://" + website

        html = _fetch_homepage(website)
        html_lower = html.lower()

        # Check signals
        has_staff, has_contact = _check_page_signals(html_lower)
        emails = _extract_emails(html)
        email_found += 1 if emails else 0
        staff_page += 1 if has_staff else 0
        contact_page += 1 if has_contact else 0

        # Update enrichment_data with homepage results
        homepage_data = {
            **ed,
            "homepage_fetched": _now(),
            "homepage_fetched_url": website,
            "homepage_success": bool(html),
            "homepage_length": len(html),
            "has_staff_page": has_staff,
            "has_contact_page": has_contact,
            "emails_found": emails,
            "email_count": len(emails),
        }

        conn.execute("""
            UPDATE leads SET enrichment_data = ?, enrichment_last_run = ?
            WHERE id = ?
        """, (json.dumps(homepage_data), _now(), lead["id"]))

        status = "HAS_EMAIL" if emails else "PHONE_ONLY"
        print(f"  ID {lead['id']:3d}: {lead['name'][:40]:40} | staff={has_staff} | contact={has_contact} | {status} | {len(emails)} emails | {len(html)} chars")

        results.append({
            "id": lead["id"],
            "name": lead["name"],
            "website": website,
            "has_staff": has_staff,
            "has_contact": has_contact,
            "emails": emails,
            "html_length": len(html),
            "homepage_success": bool(html),
        })

        if html:
            success += 1
        else:
            failed += 1

        _log(conn, "estate_enrich_homepage", "estate_denver_enrich",
             f"ID {lead['id']} {lead['name'][:40]}: staff={has_staff} contact={has_contact} emails={len(emails)}",
             json.dumps({"success": bool(html), "emails": emails, "staff": has_staff, "contact": has_contact}))

        time.sleep(1.0)  # rate limit

    conn.commit()
    conn.close()

    with open(_RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n[ENRICH] Complete: {success} fetched | {failed} failed | {email_found} with email | {staff_page} staff pages | {contact_page} contact pages")
    print(f"[ENRICH] Results saved to: {_RESULTS_PATH}")

    return {"success": success, "failed": failed, "email_found": email_found,
            "staff_page": staff_page, "contact_page": contact_page}


if __name__ == "__main__":
    main()
