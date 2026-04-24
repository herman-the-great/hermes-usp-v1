#!/usr/bin/env python3
"""
Hermes-USP-v1 — Contact Targeting Engine
Derives contact strategy from enrichment data for each lead.
Does NOT guess or fabricate contact information.

Isolation: Uses only usp.db. Writes to leads table.
Does NOT touch jarvis.db, mission_control, or PPN Gmail systems.
"""
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_USP_ROOT = Path(__file__).parent.parent.resolve()
_DB_PATH = _USP_ROOT / "usp.db"


# ── Role Priority by Vertical ─────────────────────────────────────────────────

# Outbound viable = can receive cold email
# phone_intake = must call, no usable email found
ROLE_PRIORITY_OUTBOUND = {
    # Accounting / Bookkeeping
    "accounting": [
        "owner", "managing partner", "cfo", "finance director",
        "office manager", "operations manager", "firm administrator",
        "bookkeeping manager", "accounting manager",
    ],
    # Estate Planning / Trusts / Probate
    "estate_planning": [
        "managing partner", "estate attorney", "trust attorney",
        "probate attorney", "office manager", "paralegal",
        "intake coordinator", "client services manager",
    ],
    # Home Services (plumbing, HVAC, electrical, etc.)
    "home_services": [
        "owner", "operations manager", "office manager",
        "dispatcher", "service manager", "general manager",
        "admin manager", "office administrator",
    ],
    # Collections Law
    "collections_law": [
        "managing partner", "collections attorney", "paralegal",
        "office manager", "intake specialist", "debt collection manager",
    ],
    # Family Law
    "family_law": [
        "managing partner", "family law attorney", "paralegal",
        "office manager", "intake coordinator", "client services",
    ],
    # Real Estate
    "real_estate": [
        "owner", "broker", "managing broker", "team lead",
        "office manager", "transaction coordinator",
        "operations manager", "marketing director",
    ],
    # Default (no vertical match)
    "default": [
        "owner", "founder", "partner", "director", "manager",
        "office manager", "operations manager", "admin",
    ],
}

PHONE_ONLY_THRESHOLD = 0.3  # If confidence below this, mark phone_only


# ── Helpers ───────────────────────────────────────────────────────────────────

def _score_contact(named_person: str, role: str, vertical: str) -> float:
    """Score a named person + role combination. Higher = better for outreach."""
    priorities = ROLE_PRIORITY_OUTBOUND.get(vertical, ROLE_PRIORITY_OUTBOUND["default"])
    role_lower = role.lower().strip()
    name_lower = named_person.lower()

    # Check if this role is in our priority list
    role_score = 0.0
    for i, priority_role in enumerate(priorities):
        if priority_role in role_lower or role_lower in priority_role:
            role_score = 1.0 - (i / len(priorities))
            break

    # Penalize common generic names in role positions
    generic_terms = ["manager", "owner", "director"]
    if role_score > 0 and all(t in role_lower for t in generic_terms):
        role_score *= 0.7

    return role_score


def _extract_business_type(enrichment_data: dict) -> str:
    """Infer vertical from types and website snippets."""
    types = enrichment_data.get("types", [])
    snippets = " ".join(enrichment_data.get("web_snippets", []))
    combined = " ".join(types).lower() + " " + snippets.lower()

    if any(kw in combined for kw in ["accounting", "bookkeeping", "cpa", "tax prep", "payroll"]):
        return "accounting"
    if any(kw in combined for kw in ["estate", "trust", "probate", "wills", "estate planning"]):
        return "estate_planning"
    if any(kw in combined for kw in ["hvac", "plumbing", "electric", "roofing", "paving", "contractor", "home service", "repair"]):
        return "home_services"
    if any(kw in combined for kw in ["collections", "debt collection", "creditors"]):
        return "collections_law"
    if any(kw in combined for kw in ["family law", "divorce", "custody", "family attorney"]):
        return "family_law"
    if any(kw in combined for kw in ["real estate", "realtor", "broker", "property"]):
        return "real_estate"
    return "default"


def _classify_email(email: str) -> tuple[str, str]:
    """
    Classify an email address as named_contact_direct, generic_inbox, or contact_form_only.
    - named_contact_direct: email appears in context of a named person (from named_people list)
    - generic_inbox: generic/shared inbox (info, hello, sales, etc.)
    - contact_form_only: no email found, contact form is the only option

    Returns (path_type, contact_quality_tier).
    """
    email_lower = email.lower()
    generic_prefixes = [
        "info", "contact", "hello", "admin", "support", "sales",
        "office", "web", "website", "help", "service",
    ]
    for prefix in generic_prefixes:
        if email_lower.startswith(prefix + "@") or email_lower == prefix + "@":
            return "generic_inbox", "C"
    return "named_contact_direct", "A"


# ── Main Targeting Function ───────────────────────────────────────────────────

def target_lead(lead_id: int, force: bool = False) -> dict:
    """
    Analyze enrichment data for a lead and derive contact strategy.
    Writes best_contact_path, contact_email, contact_confidence to leads table.

    Args:
        lead_id: Database ID of the lead
        force: Re-analyze even if already targeted

    Returns:
        dict with targeting results
    """
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load lead and enrichment data
    lead = conn.execute(
        "SELECT id, name, enrichment_data, contact_confidence, best_contact_path, category FROM leads WHERE id = ?",
        (lead_id,)
    ).fetchone()
    if not lead:
        conn.close()
        return {"success": False, "error": f"Lead {lead_id} not found"}

    # Skip if already targeted unless force
    if not force and lead["contact_confidence"] != "unresolved":
        conn.close()
        return {"success": False, "error": "Lead already targeted", "skipped": True}

    if not lead["enrichment_data"]:
        conn.close()
        return {"success": False, "error": "No enrichment data — run enrichment first"}

    # Skip suppressed leads — do not write any contact path or email
    lead_full = conn.execute(
        "SELECT suppression_flag, send_readiness FROM leads WHERE id = ?",
        (lead_id,)
    ).fetchone()
    if lead_full and lead_full["suppression_flag"] in ("wrong_fit", "do_not_contact", "suppressed"):
        conn.close()
        return {"success": False, "error": "Lead is suppressed", "skipped": True, "suppressed": True}

    try:
        enrichment = json.loads(lead["enrichment_data"])
    except (json.JSONDecodeError, TypeError):
        conn.close()
        return {"success": False, "error": "Invalid enrichment data JSON"}

    # ── Step 1: Business type inference ─────────────────────────────────────
    vertical = _extract_business_type(enrichment)
    website = enrichment.get("website", "")
    phone = enrichment.get("phone", "")
    generic_emails = []
    specific_emails = []
    named_contacts = []
    web_contacts = enrichment.get("web_contacts", {})

    # ── Step 2: Extract all contacts from all pages ──────────────────────────
    for page_name, contacts in web_contacts.items():
        if not isinstance(contacts, dict):
            continue
        for email in contacts.get("specific_emails", []):
            etype, _ = _classify_email(email)
            if etype == "specific" and email not in specific_emails:
                specific_emails.append(email)
        for email in contacts.get("generic_emails", []):
            if email not in generic_emails:
                generic_emails.append(email)
        for person_tuple in contacts.get("named_people", []):
            if isinstance(person_tuple, (list, tuple)) and len(person_tuple) >= 2:
                named_contacts.append({
                    "name": " ".join(str(p).strip() for p in person_tuple if p),
                    "role": str(person_tuple[0]).strip() if person_tuple[0] else "",
                    "page": page_name,
                })
            elif isinstance(person_tuple, str):
                named_contacts.append({"name": str(person_tuple).strip(), "role": "", "page": page_name})

    # ── Step 3: Score and rank contacts ─────────────────────────────────────
    scored_contacts = []
    for contact in named_contacts:
        score = _score_contact(contact["name"], contact["role"], vertical)
        if score > 0:
            scored_contacts.append({**contact, "score": score})

    scored_contacts.sort(key=lambda x: x["score"], reverse=True)
    best_named = scored_contacts[0] if scored_contacts else None

    # ── Step 4: Determine best contact path and quality ──────────────────────
    named_person = best_named["name"] if best_named else ""
    named_role   = best_named["role"] if best_named else ""

    if specific_emails:
        best_email       = specific_emails[0]
        best_contact_path = "named_contact_direct"
        contact_quality  = "A"
        to_email         = best_email
    elif generic_emails:
        best_email       = generic_emails[0]
        best_contact_path = "generic_inbox"
        contact_quality  = "C"
        to_email         = best_email
    elif phone:
        best_contact_path = "phone_only"
        contact_quality  = "B"
        to_email         = "phone_only"  # Sentinel value: marks phone-only leads for phone CTA flow
    else:
        best_contact_path = "unresolved"
        contact_quality   = "uncontactable"
        to_email          = ""
        named_person      = ""
        named_role        = ""

    # ── Step 5: Write all 7 contact fields to leads table ──────────────────
    # Set qualification_state and outbound_state based on targeting outcome.
    # This closes the pipeline gap: backfill set these for initial leads, but
    # target_lead() must set them for every newly discovered lead.
    if contact_quality in ("A", "B", "C"):
        qualification_state = "qualified"
        outbound_state = "off_market"  # draft_generator will graduate to draft_queued
    elif contact_quality == "uncontactable":
        qualification_state = "disqualified"
        outbound_state = "off_market"
    else:
        qualification_state = "candidate"
        outbound_state = "off_market"

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE leads SET
           best_contact_path     = ?,
           contact_email         = ?,
           contact_confidence    = ?,
           contact_quality      = ?,
           contact_named_person  = ?,
           contact_named_role    = ?,
           qualification_state   = ?,
           outbound_state       = ?,
           assigned_vertical    = ?,
           updated_at           = ?
           WHERE id = ?""",
        (best_contact_path, to_email, contact_quality,
         contact_quality, named_person, named_role,
         qualification_state, outbound_state, vertical, now, lead_id)
    )
    conn.commit()
    conn.close()

    # ── Audit log ──────────────────────────────────────────────────────────────
    _log_audit("contact_targeted",
               detail=f"lead {lead_id} ({lead['name']}): "
                      f"vertical={vertical}, path={best_contact_path}, "
                      f"quality={contact_quality}, email={to_email}, "
                      f"named={bool(best_named)}, generic_count={len(generic_emails)}",
               result=best_contact_path)

    return {
        "success": True,
        "lead_id": lead_id,
        "name": lead["name"],
        "vertical": vertical,
        "best_contact_path": best_contact_path,
        "contact_quality": contact_quality,
        "to_email": to_email,
        "contact_named_person": named_person,
        "contact_named_role": named_role,
        "best_named_contact": best_named,
        "all_specific_emails": specific_emails,
        "all_generic_emails": generic_emails,
        "all_named_contacts": scored_contacts[:5],
    }


def target_all_uncontacted() -> dict:
    """Run contact targeting on all leads that have enrichment but unresolved contact."""
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, name FROM leads
           WHERE enrichment_last_run IS NOT NULL
             AND (contact_confidence = 'unresolved' OR best_contact_path = 'unresolved')
             AND (suppression_flag IS NULL OR suppression_flag NOT IN ('wrong_fit', 'do_not_contact', 'suppressed'))"""
    ).fetchall()
    conn.close()

    results = []
    for row in rows:
        result = target_lead(row["id"])
        results.append({"lead_id": row["id"], "name": row["name"], **result})

    return {
        "total": len(results),
        "viable": sum(1 for r in results if r.get("outreach_viability") == "viable"),
        "weak_generic": sum(1 for r in results if r.get("outreach_viability") == "weak_generic"),
        "phone_only": sum(1 for r in results if r.get("outreach_viability") == "phone_only"),
        "no_contact": sum(1 for r in results if r.get("outreach_viability") == "no_contact_info"),
        "details": results,
    }


def _log_audit(action, detail=None, result=None):
    conn = sqlite3.connect(str(_DB_PATH))
    try:
        conn.execute(
            "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
            (action, "contact_targeting", detail, result, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    finally:
        conn.close()


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="USP Contact Targeting")
    parser.add_argument("--lead-id", type=int, help="Target single lead by ID")
    parser.add_argument("--all", action="store_true", help="Target all unenriched leads")
    parser.add_argument("--force", action="store_true", help="Re-analyze even if already targeted")
    args = parser.parse_args()

    if args.lead_id:
        result = target_lead(args.lead_id, force=args.force)
    elif args.all:
        result = target_all_uncontacted()
    else:
        print("Use --lead-id <id> or --all")
        import sys
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))
