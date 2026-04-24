#!/usr/bin/env python3
"""
Hermes-USP-v1 — Enrichment Engine
Fetches Google Places Details + homepage content for discovered leads.
Stores evidence in enrichment_cache. Does NOT invent facts.

Isolation: Uses only usp.db and USP config.
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
from typing import Optional

_USP_ROOT = Path(__file__).parent.parent.resolve()
_DB_PATH = _USP_ROOT / "usp.db"
_CONFIG_PATH = _USP_ROOT / "config.json"


# ── Config ──────────────────────────────────────────────────────────────────

def _load_config():
    with open(_CONFIG_PATH) as f:
        return json.load(f)


# ── Audit Logger ─────────────────────────────────────────────────────────────

def _log(action, detail=None, result=None, engine="enrichment"):
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        conn.execute(
            "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
            (action, engine, detail, result, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    finally:
        conn.close()


# ── Google Places Details ─────────────────────────────────────────────────────

def _places_details(place_id: str, api_key: str) -> Optional[dict]:
    """Fetch full Places Details for a place_id. Returns dict or None on error."""
    url = (
        f"https://maps.googleapis.com/maps/api/place/details/json"
        f"?place_id={place_id}"
        f"&fields="
        f"name,website,formatted_phone_number,formatted_address,"
        f"url,rating,business_status,opening_hours,types,reviews"
        f"&key={api_key}"
    )
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            result = json.loads(r.read())
        if result.get("status") == "OK":
            return result.get("result")
        _log("enrich_details_error", detail=f"{place_id}: {result.get('status')}", engine="enrichment")
        return None
    except Exception as e:
        _log("enrich_details_exception", detail=f"{place_id}: {e}", engine="enrichment")
        return None


# ── Homepage Fetcher ──────────────────────────────────────────────────────────

def _fetch_homepage(url: str, timeout: int = 10) -> Optional[str]:
    """Fetch homepage HTML using curl (most sites block urllib). Returns text or None."""
    try:
        if not url.startswith("http"):
            url = "https://" + url
        result = __import__('subprocess').run(
            ['curl', '-s', '-L', '--max-time', str(timeout),
             '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
             '-H', 'Accept: text/html,application/xhtml+xml',
             url],
            capture_output=True, text=True, timeout=timeout + 2
        )
        if result.returncode == 0 and result.stdout and len(result.stdout) > 100:
            return result.stdout
    except Exception:
        pass
    return None


# ── Web Evidence Extractor ────────────────────────────────────────────────────

# Patterns for finding contact/relevant info on business websites
_EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.MULTILINE
)

_NAME_TITLE_PATTERNS = [
    re.compile(r'\b([A-Z][a-zA-Z\'\-]+ [A-Z][a-zA-Z\'\-]+),?\s+(owner|founder|president|partner|director|manager|principal|ceo|coo|cto|cfo|head)', re.IGNORECASE),
    re.compile(r'\b(owner|founder|president|partner)\s+([A-Z][a-zA-Z\'\-]+ [A-Z][a-zA-Z\'\-]+)', re.IGNORECASE),
]

_SERVICE_PATTERNS = [
    re.compile(r'<li[^>]*>(.*?)</li>', re.DOTALL | re.IGNORECASE),
    re.compile(r'<h[23][^>]*>(.*?)</h[23]>', re.DOTALL | re.IGNORECASE),
    re.compile(r'<p[^>]*>(.*?)</p>', re.DOTALL | re.IGNORECASE),
]


def _strip_html(html: str) -> str:
    if not html:
        return ""
    # Remove scripts and style blocks
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<[^>]+>', ' ', html)
    html = re.sub(r'\s+', ' ', html)
    return html.strip()


def _extract_contacts(html: str) -> dict:
    """Extract email addresses and named contacts from HTML text."""
    text = _strip_html(html)
    emails = list(set(_EMAIL_PATTERN.findall(text)))[:5]  # max 5 emails

    # Filter out common non-contact emails
    exclude = {"noreply", "no-reply", "donotreply", "example", "test", "admin@", "info@", "contact@", "hello@", "support@", "sales@"}
    contacts = [e for e in emails if not any(b in e.lower() for b in exclude)]
    generic = [e for e in emails if any(b in e.lower() for b in exclude)]

    # Look for named people
    named_people = []
    for pattern in _NAME_TITLE_PATTERNS:
        matches = pattern.findall(text)
        named_people.extend(matches[:3])

    return {
        "specific_emails": contacts,
        "generic_emails": generic,
        "named_people": list(set(named_people))[:5],
    }


def _extract_snippets(html: str, keywords: list[str]) -> list[str]:
    """Extract short text snippets around keyword matches."""
    text = _strip_html(html)
    snippets = []
    for kw in keywords:
        pattern = re.compile(rf'.{{0,80}}{re.escape(kw)}.{{0,80}}', re.IGNORECASE)
        for match in pattern.findall(text):
            cleaned = match.strip()
            if len(cleaned) > 30:
                snippets.append(cleaned)
    return list(set(snippets))[:5]


def _fetch_service_pages(base_url: str, api_key: str) -> dict:
    """
    Attempt to fetch about, services, team, and contact pages.
    Returns dict of page_name -> page content snippet.
    """
    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    pages = {
        "homepage": base_url,
    }

    # Try common subpages
    for path in ["/about", "/about-us", "/services", "/team", "/contact", "/contact-us"]:
        pages[path.lstrip("/")] = base_url.rstrip("/") + path

    results = {}
    for name, url in pages.items():
        if name == "homepage":
            content = _fetch_homepage(url)
        else:
            content = _fetch_homepage(url, timeout=5)
        if content:
            text = _strip_html(content)
            if len(text) > 100:  # Only keep pages with real content
                results[name] = text[:3000]  # Store first 3000 chars
        time.sleep(0.5)  # Be polite to target servers

    return results


# ── Main Enrichment Run ───────────────────────────────────────────────────────

def enrich_lead(lead_id: int, force: bool = False) -> dict:
    """
    Enrich a single lead by fetching Places Details and homepage content.
    Stores results in enrichment_cache and updates leads.enrichment_data.

    Args:
        lead_id: Database ID of the lead to enrich
        force: If True, re-fetch even if already cached

    Returns:
        dict with enrichment results
    """
    cfg = _load_config()
    api_key = cfg.get("discovery", {}).get("google_places_api_key", "")
    if not api_key:
        return {"success": False, "error": "No Places API key in config"}

    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load lead
    lead = conn.execute(
        "SELECT id, name, notes FROM leads WHERE id = ?", (lead_id,)
    ).fetchone()
    if not lead:
        conn.close()
        return {"success": False, "error": f"Lead {lead_id} not found"}

    # Parse place_id from notes
    try:
        notes = json.loads(lead["notes"] or "{}")
        place_id = notes.get("place_id", "")
    except (json.JSONDecodeError, TypeError):
        place_id = ""

    if not place_id:
        conn.close()
        return {"success": False, "error": "No place_id in lead notes"}

    # Check cache
    if not force:
        cached = conn.execute(
            "SELECT data, cached_at FROM enrichment_cache "
            "WHERE entity_type='lead' AND entity_id=? AND source='places_details'",
            (lead_id,)
        ).fetchone()
        if cached:
            _log("enrich_cache_hit", detail=f"lead {lead_id}", engine="enrichment")
            conn.close()
            return {"success": True, "cached": True, "data": json.loads(cached["data"])}

    # ── Step 1: Places Details ──────────────────────────────────────────────
    _log("enrich_details_start", detail=f"lead {lead_id}: {place_id}", engine="enrichment")
    details = _places_details(place_id, api_key)
    if not details:
        conn.close()
        return {"success": False, "error": f"Places Details failed for {place_id}"}

    enrichment = {
        "website": details.get("website"),
        "phone": details.get("formatted_phone_number"),
        "address": details.get("formatted_address"),
        "maps_url": details.get("url"),
        "rating": details.get("rating"),
        "business_status": details.get("business_status"),
        "types": details.get("types", []),
        "opening_hours": details.get("opening_hours", {}).get("weekday_text", []) if details.get("opening_hours") else [],
        "reviews": details.get("reviews", [])[:3] if details.get("reviews") else [],  # Store first 3 reviews only
        "web_contacts": {},
        "web_snippets": [],
        "pages_fetched": [],
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Step 2: Homepage + subpage fetch ────────────────────────────────────
    website = details.get("website", "")
    if website:
        _log("enrich_web_fetch", detail=f"lead {lead_id}: {website}", engine="enrichment")
        pages = _fetch_service_pages(website, api_key)

        service_keywords = [
            "accounting", "bookkeeping", "tax", "payroll", "estate", "trust",
            "probate", "family law", "divorce", "custody", "collections",
            "bankruptcy", "home services", "hvac", "plumbing", "electrical",
            "real estate", "property", "website", "web design", "seo",
            "marketing", "branding"
        ]

        for page_name, content in pages.items():
            contacts = _extract_contacts(content)
            snippets = _extract_snippets(content, service_keywords)
            enrichment["web_contacts"][page_name] = contacts
            if snippets:
                enrichment["web_snippets"].extend(snippets)
        enrichment["pages_fetched"] = list(pages.keys())
    else:
        enrichment["web_contacts"] = {"error": "no_website_in_places_details"}
        enrichment["pages_fetched"] = []

    # Deduplicate snippets
    enrichment["web_snippets"] = list(set(enrichment["web_snippets"]))[:10]

    # ── Step 3: Write to cache + update lead ────────────────────────────────
    cache_data = json.dumps(enrichment)

    # Upsert enrichment_cache
    conn.execute(
        """INSERT INTO enrichment_cache (entity_type, entity_id, source, data, cached_at)
           VALUES ('lead', ?, 'places_details', ?, ?)
           ON CONFLICT(entity_type, entity_id, source) DO UPDATE SET data=excluded.data, cached_at=excluded.cached_at""",
        (lead_id, cache_data, datetime.now(timezone.utc).isoformat())
    )

    # Update leads.enrichment_data and enrichment_last_run
    conn.execute(
        "UPDATE leads SET enrichment_data=?, enrichment_last_run=? WHERE id=?",
        (cache_data, datetime.now(timezone.utc).isoformat(), lead_id)
    )
    conn.commit()
    conn.close()

    _log("enrich_complete",
         detail=f"lead {lead_id}: website={bool(enrichment.get('website'))}, "
                f"phone={bool(enrichment.get('phone'))}, "
                f"pages={len(enrichment.get('pages_fetched', []))}",
         result="success",
         engine="enrichment")
    return {"success": True, "cached": False, "data": enrichment}


def enrich_all_unenriched(batch_limit: int = 20) -> dict:
    """Enrich all leads that don't have enrichment_data yet."""
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name FROM leads WHERE enrichment_last_run IS NULL LIMIT ?",
        (batch_limit,)
    ).fetchall()
    conn.close()

    results = []
    for row in rows:
        result = enrich_lead(row["id"])
        results.append({"lead_id": row["id"], "name": row["name"], **result})
        time.sleep(1)  # Rate limit between leads

    return {
        "total": len(results),
        "successful": sum(1 for r in results if r.get("success")),
        "failed": sum(1 for r in results if not r.get("success")),
        "details": results,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="USP Lead Enrichment")
    parser.add_argument("--lead-id", type=int, help="Enrich single lead by ID")
    parser.add_argument("--all", action="store_true", help="Enrich all unenriched leads")
    parser.add_argument("--force", action="store_true", help="Re-fetch even if cached")
    args = parser.parse_args()

    if args.lead_id:
        result = enrich_lead(args.lead_id, force=args.force)
    elif args.all:
        result = enrich_all_unenriched()
    else:
        print("Use --lead-id <id> or --all")
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))
