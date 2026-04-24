#!/usr/bin/env python3
"""
USP Phase 1.5 — Home Services Contact Re-extraction
===================================================
Re-extracts email contacts from home_services lead websites using the
accounting-style email filter (accepts info@, contact@, hello@, sales@, etc.
as fallback for home_services when no named-person email is available).

Isolation: Uses only usp.db. Writes to leads table.
Does NOT touch jarvis.db, mission_control, or PPN Gmail systems.
Does NOT modify any shared engines or scripts.

Usage:
    python3 jobs/reprocess_home_services_contacts.py          # all home_services leads
    python3 jobs/reprocess_home_services_contacts.py --limit 10  # first 10 only
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(USP_ROOT))

_DB_PATH = USP_ROOT / "usp.db"

# ── Email extraction (accounting-style — accepts generic inboxes) ──────────────

_EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.MULTILINE
)

_GENERIC_EXCLUDE = {"noreply", "no-reply", "donotreply", "example", "test"}
_GENERIC_INBOXES = {"info", "contact", "hello", "admin", "support", "sales",
                    "office", "web", "website", "help", "service"}


def _strip_html(html: str) -> str:
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<[^>]+>', ' ', html)
    html = re.sub(r'\s+', ' ', html)
    return html.strip()


def _extract_emails_accounting_style(html: str) -> list:
    """
    Accounting-style email extraction.
    Returns named-person emails (no common generic prefix) as 'named',
    and generic inbox emails as 'generic'.
    """
    text = _strip_html(html)
    all_emails = list(set(_EMAIL_PATTERN.findall(text)))[:10]
    named = []
    generic = []
    for e in all_emails:
        prefix = e.lower().split('@')[0]
        if any(prefix.startswith(g) for g in _GENERIC_EXCLUDE):
            continue
        if any(prefix == g for g in _GENERIC_INBOXES):
            generic.append(e)
        else:
            named.append(e)
    return named, generic


def _fetch_page(url: str, timeout: int = 10) -> str:
    """Fetch a single page, return empty string on failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            if "text/html" not in ct:
                return ""
            return resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _extract_contacts_from_homepage(website: str) -> dict:
    """
    Fetch homepage and extract named + generic emails per page.
    Returns per-page dict matching the schema contact_targeting.py reads:
      web_contacts.<page>.named_people  -> list of [role, name] pairs (for targeting engine)
      web_contacts.<page>.specific_emails -> named-person emails
      web_contacts.<page>.generic_emails -> role-based inbox emails
    Also returns flat all_named_emails / all_generic_emails for classification.
    """
    if not website or website in ("NONE", "ERROR", ""):
        return {
            "pages": {},
            "all_named_emails": [],
            "all_generic_emails": [],
            "pages_checked": []
        }

    if not website.startswith("http"):
        website = "https://" + website

    pages_to_check = [
        ("homepage", website),
    ]
    base = website.rstrip("/")
    for path in ["/contact", "/contact-us", "/about", "/about-us", "/team", "/about-team"]:
        pages_to_check.append((path.lstrip("/"), base + path))

    all_named = []
    all_generic = []
    checked = []
    pages_result = {}

    for page_name, url in pages_to_check:
        html = _fetch_page(url)
        if not html:
            continue
        checked.append(url)
        n, g = _extract_emails_accounting_style(html)

        # Build named_people list (role + email as proxy for real name)
        named_people = []
        for email in n:
            prefix = email.split("@")[0].lower()
            # Try to extract a plausible first name
            parts = re.split(r"[._\-]", prefix)
            first = parts[0] if parts else ""
            if re.match(r"^[a-z]{2,15}$", first) and first not in _GENERIC_INBOXES:
                named_people.append([first.title(), email])

        pages_result[page_name] = {
            "named_people": named_people,
            "specific_emails": n,
            "generic_emails": g,
        }

        for e in n:
            if e not in all_named:
                all_named.append(e)
        for e in g:
            if e not in all_generic:
                all_generic.append(e)

        # Stop if we found a named email
        if all_named:
            break

    return {
        "pages": pages_result,
        "all_named_emails": all_named,
        "all_generic_emails": all_generic,
        "pages_checked": checked,
    }


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _log(action, detail=None, result=None):
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, "reprocess_home_services_contacts", detail, result, _now_iso())
    )
    conn.commit()
    conn.close()


def _extract_first_name_from_email(email: str) -> str:
    """
    Extract a plausible first name from a named-person email address.
    E.g. 'chris@730southexteriors.com' -> 'Chris'
    E.g. 'mike.johnson@example.com' -> 'Mike'
    Returns empty string if no name can be extracted.
    """
    if not email or "@" not in email:
        return ""
    prefix = email.split("@")[0].lower()
    # Skip if prefix is clearly a role word or generic
    ROLE_PREFIXES = {"info", "contact", "hello", "admin", "support", "sales",
                    "office", "web", "website", "help", "service", "noreply",
                    "no-reply", "donotreply", "team", "manager", "owner"}
    if prefix in ROLE_PREFIXES:
        return ""
    # Remove common separators, take first part
    parts = re.split(r"[._\-]", prefix)
    first = parts[0]
    # Only use if it looks like a name (2-15 chars, alpha only, starts with letter)
    if re.match(r"^[a-z]{2,15}$", first):
        return first.title()
    return ""


def _classify_for_home_services(named_emails, generic_emails, phone):
    """
    Home-services-specific contact classification.
    Returns (best_contact_path, contact_quality, to_email, named_person)
    Priority: named-person email > verified team-domain inbox > phone_only
    """
    if named_emails:
        first_name = _extract_first_name_from_email(named_emails[0])
        return "named_contact_direct", "A", named_emails[0], first_name

    if generic_emails:
        # Accept team-domain generic inbox for home_services only
        return "generic_inbox", "C", generic_emails[0], ""

    if phone:
        return "phone_only", "B", "", ""

    return "unresolved", "uncontactable", "", ""


def reprocess_home_services_lead(lead_id: int, force: bool = False) -> dict:
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    lead = conn.execute(
        "SELECT id, name, enrichment_data, contact_email, contact_confidence FROM leads WHERE id = ?",
        (lead_id,)
    ).fetchone()
    if not lead:
        conn.close()
        return {"success": False, "error": f"Lead {lead_id} not found"}

    # Only process home_services leads
    if not conn.execute(
        "SELECT 1 FROM leads WHERE id=? AND assigned_vertical='home_services'",
        (lead_id,)
    ).fetchone():
        conn.close()
        return {"success": False, "error": "Not a home_services lead", "skipped": True}

    # Skip if already has named-person email unless force is set
    existing_is_named = (
        lead["contact_confidence"] in ("named_contact_direct",) and
        lead["contact_email"] and
        not any(lead["contact_email"].lower().startswith(g + "@")
                for g in _GENERIC_INBOXES)
    )
    if existing_is_named and not force:
        conn.close()
        return {"success": False, "error": "Lead already has named-person email", "skipped": True}

    enrichment = json.loads(lead["enrichment_data"] or "{}")
    website = enrichment.get("website", "")
    phone = enrichment.get("phone", "")

    result = _extract_contacts_from_homepage(website)
    named = result["all_named_emails"]
    generic = result["all_generic_emails"]
    pages = result["pages"]

    # Merge new per-page contact data into enrichment_data.web_contacts
    if pages:
        web_contacts = enrichment.get("web_contacts", {})
        for page_name, page_data in pages.items():
            if page_name not in web_contacts:
                web_contacts[page_name] = {}
            # Merge: only overwrite if new data is better
            existing = web_contacts[page_name]
            for key in ["named_people", "specific_emails", "generic_emails"]:
                if key in page_data and page_data[key]:
                    existing[key] = page_data[key]
            web_contacts[page_name] = existing
        enrichment["web_contacts"] = web_contacts

    best_path, quality, to_email, named_person_str = _classify_for_home_services(
        named, generic, phone
    )

    # Write contact results + updated enrichment_data
    now = _now_iso()
    conn.execute(
        """UPDATE leads SET
           contact_email       = ?,
           contact_quality     = ?,
           best_contact_path   = ?,
           contact_confidence  = ?,
           contact_named_person = ?,
           enrichment_data     = ?,
           qualification_state = CASE
             WHEN ? IN ('A', 'B', 'C') THEN 'qualified'
             ELSE qualification_state
           END,
           updated_at          = ?
           WHERE id = ?""",
        (to_email, quality, best_path, best_path,
         named_person_str, json.dumps(enrichment), best_path, now, lead_id)
    )
    conn.commit()
    conn.close()

    _log("reprocess_home_services",
         detail=f"lead {lead_id} ({lead['name']}): "
                f"path={best_path}, quality={quality}, "
                f"named={named}, generic={generic}, pages={result['pages_checked']}",
         result=best_path)

    return {
        "success": True,
        "lead_id": lead_id,
        "name": lead["name"],
        "best_contact_path": best_path,
        "contact_quality": quality,
        "to_email": to_email,
        "named_emails": named,
        "generic_emails": generic,
        "phone": phone,
        "pages_checked": len(result["pages_checked"]),
        "pages": list(pages.keys()),
        "contact_named_person": named_person_str,
    }


def reprocess_all_home_services(limit: int = None):
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    query = "SELECT id, name FROM leads WHERE assigned_vertical='home_services'"
    if limit:
        query += f" LIMIT {limit}"
    leads = conn.execute(query).fetchall()
    conn.close()

    results = []
    for lead in leads:
        r = reprocess_home_services_lead(lead["id"])
        results.append(r)
        print(f"  [{r['success']}] id={lead['id']} {lead['name'][:40]}: "
              f"path={r.get('best_contact_path')} quality={r.get('contact_quality')} "
              f"email={r.get('to_email', '')[:40]}", flush=True)
        time.sleep(0.3)  # be respectful to target websites

    named = sum(1 for r in results if r.get("best_contact_path") == "named_contact_direct")
    generic = sum(1 for r in results if r.get("best_contact_path") == "generic_inbox")
    phone_only = sum(1 for r in results if r.get("best_contact_path") == "phone_only")
    unresolved = sum(1 for r in results if r.get("best_contact_path") in ("unresolved", "uncontactable"))

    summary = {
        "total": len(results),
        "named_contact_direct": named,
        "generic_inbox": generic,
        "phone_only": phone_only,
        "unresolved": unresolved,
        "results": results,
    }
    return summary


def qualify_home_services_leads():
    """
    Set qualification_state=qualified for home_services leads
    that have a viable outreach path (email or phone).
    """
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    now = _now_iso()

    conn.execute(
        """UPDATE leads SET
           qualification_state = 'qualified',
           updated_at          = ?
           WHERE assigned_vertical = 'home_services'
             AND qualification_state IS NULL
             AND contact_quality IN ('A', 'B', 'C')
             AND best_contact_path IN ('named_contact_direct', 'generic_inbox', 'phone_only')
        """,
        (now,)
    )
    qualified = conn.total_changes
    conn.commit()
    conn.close()

    _log("qualify_home_services",
         detail=f"Qualified {qualified} home_services leads for outreach",
         result="qualified")
    return qualified


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Re-extract contacts for home_services leads")
    parser.add_argument("--limit", type=int, default=None, help="Limit to N leads")
    args = parser.parse_args()

    print(f"[STAGE 3] Re-extracting contacts for home_services leads... (limit={args.limit})", flush=True)
    summary = reprocess_all_home_services(limit=args.limit)

    print(f"\n[STAGE 3] Contact extraction complete:", flush=True)
    print(f"  Total:         {summary['total']}", flush=True)
    print(f"  Named email:    {summary['named_contact_direct']}", flush=True)
    print(f"  Generic inbox:  {summary['generic_inbox']}", flush=True)
    print(f"  Phone only:     {summary['phone_only']}", flush=True)
    print(f"  Unresolved:     {summary['unresolved']}", flush=True)

    print(f"\n[STAGE 3b] Qualifying home_services leads... ", end="", flush=True)
    qualified = qualify_home_services_leads()
    print(f"{qualified} qualified.", flush=True)

    print(f"\n[STAGE 3] Done.", flush=True)


if __name__ == "__main__":
    main()
