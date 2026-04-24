#!/usr/bin/env python3
"""
USP Phase 1 — Targeting: accounting_bookkeeping x Denver (top 23).
Scope: 22 leads (excluding suppressed Dimov ID 26).
Email-bearing (13): full targeting assessment.
Phone-only (9): phone-contact assessment only.
No review packets. No drafts.
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

_DB_PATH = USP_ROOT / "usp.db"
_AUDIT_PATH = USP_ROOT / "targeting_results_accounting_denver.json"


def _log_audit(action, engine, detail=None, result=None):
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, engine, detail, result, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()


def _classify_email(email_local: str) -> tuple[str, str]:
    """
    Classify email local part as named or generic.
    Returns (path_type, quality_tier).
    Named: contains '.' or is a single-word first-name (lou, dustin, keely, amber, kyle, etc.)
    Generic: info@, contact@, hello@, cpa@, etc.
    """
    el = email_local.lower().strip()
    named_signals = ['.', ' ']
    if any(s in el for s in named_signals):
        return "named_contact_direct", "A"
    # Single-word first-name local parts
    common_first_names = {
        'kyle', 'lou', 'dustin', 'keely', 'amber', 'john', 'mike', 'chris',
        'tom', 'david', 'james', 'ryan', 'nick', 'pat', 'sam', 'joe', 'steve'
    }
    if el in common_first_names:
        return "named_contact_direct", "A"
    return "generic_inbox", "C"


def _fetch_staff_contact_pages(website: str) -> dict:
    """
    Fetch website and detect staff/team pages and contact pages.
    Returns dict with staff_urls, contact_urls, and named people found.
    """
    result = {
        "staff_urls": [],
        "contact_urls": [],
        "named_people": [],  # list of {"name": str, "role": str, "page": str}
        "staff_keyword_found": False,
        "contact_keyword_found": False,
        "fetch_success": False,
        "raw_html": "",
    }

    if not website or website in ("NONE", "ERROR", ""):
        return result

    staff_keywords = [
        "our team", "about us", "meet the", "our staff",
        "the team", "our people", "leadership", "attorneys",
        "accountants", "advisors", "principals", "staff"
    ]
    contact_keywords = [
        "contact", "get in touch", "reach us", "schedule"
    ]
    name_pattern = re.compile(
        r'\b([A-Z][a-z]{1,20}\s+[A-Z][a-z]{1,20})\b'
    )
    role_patterns = [
        re.compile(r'\b(CPA|Accountant|Bookkeeper|Owner|Partner|Manager|Administrator|Director|CFO|Finance)\b', re.I),
    ]

    try:
        html = urllib.request.urlopen(website, timeout=10).read().decode("utf-8", errors="ignore")
        result["fetch_success"] = True
        result["raw_html"] = html.lower()
    except Exception:
        return result

    hl = result["raw_html"]

    # Detect staff/contact keywords
    result["staff_keyword_found"] = any(k in hl for k in staff_keywords)
    result["contact_keyword_found"] = any(k in hl for k in contact_keywords)

    # Extract named people from HTML
    names_found = name_pattern.findall(html)
    for full_name in names_found:
        parts = full_name.split()
        if len(parts) >= 2 and len(full_name) > 5:
            role = ""
            for rp in role_patterns:
                m = rp.search(html)
                if m:
                    role = m.group(1)
                    break
            result["named_people"].append({
                "name": full_name,
                "role": role,
                "page": website,
            })

    return result


def _score_named_contact(named_person: str, role: str) -> float:
    """Score a named person + role for outbound viability."""
    ROLE_PRIORITY = [
        "owner", "managing partner", "cfo", "finance director",
        "office manager", "operations manager", "firm administrator",
        "bookkeeping manager", "accounting manager",
    ]
    if not role:
        return 0.3
    role_lower = role.lower()
    for i, priority_role in enumerate(ROLE_PRIORITY):
        if priority_role in role_lower:
            return 1.0 - (i / len(ROLE_PRIORITY))
    return 0.4  # generic role, moderate score


def _target_email_lead(lead_id: int, name: str, website: str,
                        contact_email: str, enrichment_data: dict) -> dict:
    """
    Full targeting assessment for an email-bearing lead.
    Returns targeting decision dict.
    """
    has_staff = enrichment_data.get("has_staff_page", False)
    has_contact = enrichment_data.get("has_contact_page", False)
    emails_in_data = enrichment_data.get("emails_found", [])
    real_emails = [e for e in emails_in_data if not any(x in e.lower() for x in [
        "noreply", "no-reply", "example", "test", "sentry", "adobestock", "shadow"
    ])]

    # Step 1: Classify the email address
    if contact_email:
        email_local = contact_email.split("@")[0]
        base_path, base_qual = _classify_email(email_local)
    else:
        base_path, base_qual = "generic_inbox", "D"
        email_local = ""

    # Step 2: If staff page exists, try to find named people
    staff_contact = {"named_people": [], "staff_keyword_found": False}
    if has_staff or has_contact:
        staff_contact = _fetch_staff_contact_pages(website)
        if staff_contact["named_people"]:
            scored = []
            for person in staff_contact["named_people"]:
                score = _score_named_contact(person["name"], person["role"])
                if score > 0:
                    scored.append({**person, "score": score})
            scored.sort(key=lambda x: x["score"], reverse=True)
            best = scored[0] if scored else None
            if best and base_path == "named_contact_direct":
                # Found named person on staff page + named email = A-tier
                final_path = "named_contact_direct"
                final_qual = "A"
                named_person = best["name"]
                named_role = best["role"]
            elif best:
                final_path = "named_contact_indirect"
                final_qual = "C"
                named_person = best["name"]
                named_role = best["role"]
            else:
                final_path = base_path
                final_qual = base_qual
                named_person = ""
                named_role = ""
        else:
            final_path = base_path
            final_qual = base_qual
            named_person = ""
            named_role = ""
    else:
        final_path = base_path
        final_qual = base_qual
        named_person = ""
        named_role = ""

    # Step 3: Determine packet eligibility
    packet_eligible = (
        final_path in ("named_contact_direct", "named_contact_indirect") and
        final_qual in ("A", "B", "C") and
        contact_email and
        not any(x in contact_email.lower() for x in ["sentry", "adobestock", "shadow"])
    )

    reason = ""
    if packet_eligible:
        reason = f"{final_qual}-tier {final_path} | email={contact_email}"
        if named_person:
            reason += f" | named_contact={named_person} ({named_role})"
    elif final_path == "generic_inbox" and final_qual == "D":
        reason = f"D-tier generic_inbox — no staff page, no named contact found"
    elif not contact_email:
        reason = "no email found in enrichment — phone_only"
    else:
        reason = f"{final_qual}-tier {final_path} — below packet-eligibility threshold"

    return {
        "lead_id": lead_id,
        "name": name,
        "website": website,
        "final_best_contact_path": final_path,
        "final_contact_quality": final_qual,
        "contact_email": contact_email,
        "contact_confidence": "targeted",
        "packet_eligible": packet_eligible,
        "reason": reason,
        "named_person": named_person,
        "named_role": named_role,
        "staff_page_found": staff_contact.get("staff_keyword_found", False),
        "contact_page_found": staff_contact.get("contact_keyword_found", False),
        "named_people_count": len(staff_contact.get("named_people", [])),
    }


def _target_phone_lead(lead_id: int, name: str, website: str,
                         enrichment_data: dict) -> dict:
    """
    Phone-contact assessment for phone-only leads.
    Returns assessment dict with retain/hold/suppress recommendation.
    """
    has_staff = enrichment_data.get("has_staff_page", False)
    has_contact = enrichment_data.get("has_contact_page", False)
    phones = enrichment_data.get("phones_found", [])
    valid_phones = [p for p in phones if re.match(r'.*\d{3}.*\d{3}.*\d{4}.*', p) and len(p) >= 10]

    # Assess phone quality
    if valid_phones:
        phone_confidence = "medium"  # standard 10-digit phone found
        if len(valid_phones) >= 2:
            phone_confidence = "high"  # multiple phone numbers suggest a real office
    else:
        phone_confidence = "low"  # no valid phone found

    # Assess website quality
    website_signal = False
    if website and website not in ("NONE", "ERROR", ""):
        website_signal = True

    # Determine retention status
    if phone_confidence == "high" and website_signal:
        status = "retain"
        reason = f"phone_contact={phone_confidence} | has_website | {len(valid_phones)} phone(s) found"
    elif phone_confidence in ("medium", "high") and website_signal:
        status = "retain"
        reason = f"phone_contact={phone_confidence} | has_website"
    elif phone_confidence == "low":
        status = "suppress"
        reason = "no_valid_phone_found | unreachable"
    else:
        status = "hold"
        reason = f"phone_contact={phone_confidence} | no_website_signal"

    return {
        "lead_id": lead_id,
        "name": name,
        "website": website,
        "phones_found": len(valid_phones),
        "phone_contact_confidence": phone_confidence,
        "status": status,
        "reason": reason,
    }


def main():
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load all 22 non-suppressed leads (IDs 21-43, excluding 26 Dimov)
    # website is stored inside enrichment_data JSON
    leads = conn.execute("""
        SELECT id, name, best_contact_path, contact_quality, contact_email,
               contact_confidence, send_readiness, enrichment_data, suppression_flag
        FROM leads
        WHERE id >= 21 AND id <= 43 AND id != 26
        ORDER BY id
    """).fetchall()

    email_leads = [l for l in leads if l["contact_email"]]
    phone_leads = [l for l in leads if not l["contact_email"]]

    print(f"[TARGETING] Email-bearing: {len(email_leads)} | Phone-only: {len(phone_leads)}")

    results = []
    for lead in leads:
        ed = json.loads(lead["enrichment_data"] or "{}")
        if lead["contact_email"]:
            r = _target_email_lead(
                lead["id"], lead["name"],
                ed.get("website", ""), lead["contact_email"], ed
            )
        else:
            r = _target_phone_lead(
                lead["id"], lead["name"],
                ed.get("website", ""), ed
            )
        results.append(r)

        # Write targeting result to leads table
        if lead["contact_email"]:
            conn.execute("""
                UPDATE leads SET
                    best_contact_path = ?,
                    contact_quality = ?,
                    contact_confidence = 'targeted',
                    contact_named_person = ?,
                    contact_named_role = ?,
                    send_readiness = CASE WHEN ? = 1 THEN 'ready' ELSE 'not_ready' END,
                    updated_at = ?
                WHERE id = ?
            """, (
                r["final_best_contact_path"],
                r["final_contact_quality"],
                r.get("named_person", ""),
                r.get("named_role", ""),
                1 if r["packet_eligible"] else 0,
                datetime.now(timezone.utc).isoformat(),
                lead["id"]
            ))
            _log_audit(
                "contact_targeted",
                "accounting_denver_targeting",
                detail=f"ID {lead['id']} {lead['name']}: path={r['final_best_contact_path']} qual={r['final_contact_quality']} packet={r['packet_eligible']}",
                result=r["reason"]
            )
        else:
            conn.execute("""
                UPDATE leads SET
                    contact_confidence = 'targeted',
                    send_readiness = ?,
                    updated_at = ?
                WHERE id = ?
            """, (
                r["status"],  # retain/hold/suppress
                datetime.now(timezone.utc).isoformat(),
                lead["id"]
            ))
            _log_audit(
                "phone_assessed",
                "accounting_denver_targeting",
                detail=f"ID {lead['id']} {lead['name']}: status={r['status']} conf={r['phone_contact_confidence']}",
                result=r["reason"]
            )

    conn.commit()
    conn.close()

    # Save results
    with open(_AUDIT_PATH, "w") as f:
        json.dump(results, f, indent=2, default=str)

    # Summary
    email_results = [r for r in results if "packet_eligible" in r]
    phone_results = [r for r in results if "phone_contact_confidence" in r]

    packet_eligible = [r for r in email_results if r["packet_eligible"]]
    not_eligible = [r for r in email_results if not r["packet_eligible"]]
    retain = [r for r in phone_results if r["status"] == "retain"]
    hold = [r for r in phone_results if r["status"] == "hold"]
    suppress = [r for r in phone_results if r["status"] == "suppress"]

    print(f"\n{'='*60}")
    print("TARGETING SUMMARY")
    print(f"{'='*60}")
    print(f"Email leads assessed: {len(email_results)}")
    print(f"  Packet-eligible: {len(packet_eligible)}")
    for r in packet_eligible:
        print(f"    ID {r['lead_id']}: {r['name'][:40]} | {r['reason'][:60]}")
    print(f"  Not packet-eligible: {len(not_eligible)}")
    for r in not_eligible:
        print(f"    ID {r['lead_id']}: {r['name'][:40]} | {r['reason'][:60]}")
    print(f"\nPhone leads assessed: {len(phone_results)}")
    print(f"  Retain: {len(retain)}")
    for r in retain:
        print(f"    ID {r['lead_id']}: {r['name'][:40]} | {r['reason']}")
    print(f"  Hold: {len(hold)}")
    for r in hold:
        print(f"    ID {r['lead_id']}: {r['name'][:40]} | {r['reason']}")
    print(f"  Suppress: {len(suppress)}")
    for r in suppress:
        print(f"    ID {r['lead_id']}: {r['name'][:40]} | {r['reason']}")

    print(f"\nResults saved to: {_AUDIT_PATH}")
    print(f"Total: {len(results)} | Packet-eligible: {len(packet_eligible)} | Retain: {len(retain)} | Hold: {len(hold)} | Suppress: {len(suppress)}")

    return {
        "total_assessed": len(results),
        "packet_eligible": len(packet_eligible),
        "not_eligible": len(not_eligible),
        "phone_retain": len(retain),
        "phone_hold": len(hold),
        "phone_suppress": len(suppress),
        "results": results,
    }


if __name__ == "__main__":
    main()
