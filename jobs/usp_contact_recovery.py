#!/usr/bin/env python3
"""
USP Contact Recovery — all 3 live verticals
=============================================
Re-scrapes website contacts for leads that came back with phone_only, generic_inbox,
or unresolved contact. Tries harder pages (/team, /about, /contact) and feeds
results back through the contact_targeting engine.

For home_services: quality A/B/C all welcome — generic inbox accepted for C-tier.
For accounting_bookkeeping: named-person emails preferred; generic acceptable for C.
For estate_planning_probate: named-person emails preferred; generic acceptable for C.

Target leads (all verticals):
  - phone_only: has phone, no email found — re-scrape website
  - generic_inbox: has generic email, re-scrape for named-person email
  - unresolved/uncontactable: no email, no phone — last-resort scrape

Does NOT overwrite existing A-tier named-person emails.
Does NOT re-scrape leads that already have named_contact_direct with A or B quality.

Isolation: Uses only usp.db. Writes to leads table and enrichment_data.
Does NOT touch jarvis.db, mission_control, or PPN Gmail systems.
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

# Pages to try for contact recovery — broader than initial enrichment
_RECOVERY_PAGES = [
    "/team",
    "/about",
    "/about-us",
    "/about-team",
    "/contact",
    "/contact-us",
    "/our-team",
    "/who-we-are",
    "/staff",
    "/attorneys",     # estate planning specific
    "/lawyers",       # estate planning specific
    "/partners",      # estate planning / professional services
    "/leadership",
    "/owners",
]

_EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.MULTILINE
)

_GENERIC_EXCLUDE = {"noreply", "no-reply", "donotreply", "example", "test"}
_GENERIC_INBOXES = {
    "info", "contact", "hello", "admin", "support", "sales",
    "office", "web", "website", "help", "service",
}


def _strip_html(html: str) -> str:
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<[^>]+>', ' ', html)
    html = re.sub(r'\s+', ' ', html)
    return html.strip()


def _fetch_page(url: str, timeout: int = 12) -> str:
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


def _extract_emails(html: str) -> tuple[list, list]:
    """
    Extract emails from HTML. Returns (named_emails, generic_emails).
    Named = email prefix is NOT a generic role word.
    Generic = email prefix IS a generic role word.
    """
    text = _strip_html(html)
    all_emails = list(set(_EMAIL_PATTERN.findall(text)))[:10]
    named = []
    generic = []
    for e in all_emails:
        prefix = e.lower().split('@')[0]
        if any(prefix.startswith(g) for g in _GENERIC_EXCLUDE):
            continue
        if prefix in _GENERIC_INBOXES:
            generic.append(e)
        else:
            named.append(e)
    return named, generic


def _scrape_contact_pages(website: str) -> dict:
    """
    Scrape all recovery pages for a website.
    Returns per-page dict matching contact_targeting.py schema:
      pages.<page>.specific_emails -> named-person emails
      pages.<page>.generic_emails  -> role-based inbox emails
      pages.<page>.named_people     -> [name, email] pairs
    """
    if not website or website in ("NONE", "ERROR", ""):
        return {"pages": {}, "all_named_emails": [], "all_generic_emails": [], "pages_checked": []}

    if not website.startswith("http"):
        website = "https://" + website

    base = website.rstrip("/")
    all_named = []
    all_generic = []
    checked = []
    pages_result = {}

    # Homepage always checked first
    for page_name, url in [("homepage", website)] + [
        (p.lstrip("/"), base + p) for p in _RECOVERY_PAGES
    ]:
        html = _fetch_page(url)
        if not html:
            continue
        checked.append(url)
        n, g = _extract_emails(html)

        # Build named_people list from named emails
        named_people = []
        for email in n:
            prefix = email.split("@")[0].lower()
            parts = re.split(r"[._\-]", prefix)
            first = parts[0] if parts else ""
            if re.match(r"^[a-z]{2,15}$", first) and first not in _GENERIC_INBOXES:
                named_people.append([first.title(), email])

        pages_result[page_name] = {
            "specific_emails": n,
            "generic_emails": g,
            "named_people": named_people,
        }

        for e in n:
            if e not in all_named:
                all_named.append(e)
        for e in g:
            if e not in all_generic:
                all_generic.append(e)

        # Stop early if we found a named person email
        if all_named:
            break

    return {
        "pages": pages_result,
        "all_named_emails": all_named,
        "all_generic_emails": all_generic,
        "pages_checked": checked,
    }


def _log(action, detail=None, result=None):
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        conn.execute(
            "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
            (action, "contact_recovery", detail, result, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    finally:
        conn.close()


def recover_contact_for_lead(lead_id: int, force: bool = False) -> dict:
    """
    Re-scrape contact pages for a single lead.
    Skips leads that already have named-person A/B quality email.
    Writes updated enrichment_data.web_contacts + re-runs targeting.
    """
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    lead = conn.execute(
        """SELECT id, name, enrichment_data, contact_email, contact_quality,
                  contact_confidence, best_contact_path, assigned_vertical
           FROM leads WHERE id = ?""",
        (lead_id,)
    ).fetchone()
    if not lead:
        conn.close()
        return {"success": False, "error": f"Lead {lead_id} not found"}

    # Skip if already has named-person email (A or B quality with named contact path)
    if lead["best_contact_path"] == "named_contact_direct" and lead["contact_quality"] in ("A", "B"):
        conn.close()
        return {"success": False, "error": "Already has named-person email", "skipped": True}

    # Skip suppressed leads
    if lead["contact_quality"] == "uncontactable" and lead["best_contact_path"] in ("unresolved", "suppressed"):
        # Still allow re-scrape for unresolved leads — they may have a website
        pass

    enrichment = json.loads(lead["enrichment_data"] or "{}")
    website = enrichment.get("website", "")

    if not website:
        conn.close()
        return {"success": False, "error": "No website in enrichment_data", "skipped": True}

    # Run recovery scrape
    result = _scrape_contact_pages(website)
    pages = result["pages"]
    named_emails = result["all_named_emails"]
    generic_emails = result["all_generic_emails"]

    if not pages:
        conn.close()
        return {
            "success": False,
            "lead_id": lead_id,
            "name": lead["name"],
            "error": "No pages scraped successfully",
            "pages_checked": result["pages_checked"],
        }

    # Merge into enrichment_data.web_contacts
    web_contacts = enrichment.get("web_contacts", {})
    improved = False
    for page_name, page_data in pages.items():
        if page_name not in web_contacts:
            web_contacts[page_name] = {}
        existing = web_contacts[page_name]
        for key in ["specific_emails", "generic_emails", "named_people"]:
            if key in page_data and page_data[key]:
                if key not in existing or not existing[key]:
                    existing[key] = page_data[key]
                    improved = True
                elif page_data[key] and not existing[key]:
                    existing[key] = page_data[key]
                    improved = True
        web_contacts[page_name] = existing
    enrichment["web_contacts"] = web_contacts

    # Re-run targeting on this lead to update contact fields
    from engines.contact_targeting import target_lead as _target_lead
    targeting_result = _target_lead(lead_id, force=True)

    now = datetime.now(timezone.utc).isoformat()

    _log("contact_recovery",
         detail=f"lead {lead_id} ({lead['name']}): "
                f"vertical={lead['assigned_vertical']}, "
                f"named_emails={named_emails}, generic_emails={generic_emails}, "
                f"pages_checked={result['pages_checked']}, improved={improved}",
         result=targeting_result.get("best_contact_path", "unknown"))

    conn.close()

    return {
        "success": True,
        "improved": improved,
        "lead_id": lead_id,
        "name": lead["name"],
        "vertical": lead["assigned_vertical"],
        "named_emails": named_emails,
        "generic_emails": generic_emails,
        "pages_checked": result["pages_checked"],
        "pages_found": list(pages.keys()),
        "targeting_result": {
            "best_contact_path": targeting_result.get("best_contact_path"),
            "contact_quality": targeting_result.get("contact_quality"),
            "to_email": targeting_result.get("to_email"),
        },
    }


def recover_all_contactless(vertical: str = None, limit: int = 50) -> dict:
    """
    Run contact recovery for leads in all live verticals that need it.
    Target: phone_only, generic_inbox, unresolved/uncontactable leads.

    Args:
        vertical: None = all live verticals, or specific vertical name
        limit: max leads to process per run (prevent runaway scraping)
    """
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Build query for leads that need contact recovery
    # NOT leads that already have named_person A or B
    query = """
        SELECT id, name, assigned_vertical, contact_quality, best_contact_path
        FROM leads
        WHERE outbound_state IN ('off_market', 'suppressed')
          AND (qualification_state = 'candidate'
               OR qualification_state = 'disqualified'
               OR qualification_state IS NULL)
          AND contact_quality IN ('B', 'C', 'D', 'uncontactable')
          AND best_contact_path IN ('phone_only', 'generic_inbox', 'unresolved')
    """
    params = []

    if vertical:
        query += " AND assigned_vertical = ?"
        params.append(vertical)

    query += " ORDER BY contact_quality ASC, RANDOM() LIMIT ?"
    params.append(limit)

    leads = conn.execute(query, params).fetchall()
    conn.close()

    if not leads:
        return {"total": 0, "processed": 0, "improved": 0, "results": []}

    results = []
    improved_count = 0

    for lead in leads:
        r = recover_contact_for_lead(lead["id"])
        results.append(r)
        if r.get("improved"):
            improved_count += 1
        print(f"  [{r['success']}] id={lead['id']} {lead['name'][:40]}: "
              f"path={r.get('targeting_result', {}).get('best_contact_path', 'N/A')} "
              f"quality={r.get('targeting_result', {}).get('contact_quality', 'N/A')} "
              f"email={r.get('targeting_result', {}).get('to_email', 'N/A') or 'N/A'}"
              f"{' IMPROVED' if r.get('improved') else ''}",
              flush=True)
        time.sleep(0.5)  # polite delay between leads

    return {
        "total": len(results),
        "processed": sum(1 for r in results if r.get("success")),
        "improved": improved_count,
        "skipped": sum(1 for r in results if r.get("skipped")),
        "failed": sum(1 for r in results if not r.get("success") and not r.get("skipped")),
        "results": results,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="USP Contact Recovery — all 3 live verticals")
    parser.add_argument("--vertical", type=str, default=None,
                        help="Specific vertical: home_services, accounting_bookkeeping, estate_planning_probate")
    parser.add_argument("--lead-id", type=int, default=None,
                        help="Single lead ID to recover")
    parser.add_argument("--limit", type=int, default=50,
                        help="Max leads to process (default 50)")
    args = parser.parse_args()

    if args.lead_id:
        print(f"[CONTACT RECOVERY] Single lead {args.lead_id}", flush=True)
        result = recover_contact_for_lead(args.lead_id)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"[CONTACT RECOVERY] All verticals (limit={args.limit})...", flush=True)
        result = recover_all_contactless(vertical=args.vertical, limit=args.limit)
        print(f"\n[CONTACT RECOVERY] Done: {result['processed']} processed, "
              f"{result['improved']} improved, {result['skipped']} skipped, {result['failed']} failed",
              flush=True)


if __name__ == "__main__":
    main()
