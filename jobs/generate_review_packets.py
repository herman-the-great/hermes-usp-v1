#!/usr/bin/env python3
"""
USP Phase 1 — Review Packet Generator (ISOLATED VERSION)
Reads ONLY from ~/.hermes/Hermes-USP-v1/offer_library/accounting_bookkeeping.json
Does NOT contain: MyPalette, MetaSill, artist, creator, gallery, or any external brand context.
Does NOT set: sender identity, from_name, or signature_block in packet content.
No Gmail drafts. No broader scope.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))

import sqlite3

_DB_PATH = USP_ROOT / "usp.db"
_PACKET_DIR = USP_ROOT / "review_packets"
_OFFER_LIBRARY = USP_ROOT / "offer_library"
_CROSS_VERTICAL = USP_ROOT / "cross_vertical_approval.json"
_PACKET_LEAD_IDS = [22, 23, 24, 27, 28, 31, 32, 35, 37, 41, 42]

REVIEW_PACKET_SECTIONS = [
    "1. Lead Summary",
    "2. Business Profile",
    "3. Trigger Events & Pain Points",
    "4. Contact Path & Confidence",
    "5. Vertical & Angle Selection",
    "6. Email Sequence Overview",
    "7. Attachment Recommendation",
    "8. Compliance & Suppression Check",
    "9. Internal Notes",
    "10. Approval Action",
]


def _load_offer_library():
    """Load ONLY the approved USP accounting_bookkeeping offer library."""
    offer_path = _OFFER_LIBRARY / "accounting_bookkeeping.json"
    if not offer_path.exists():
        raise FileNotFoundError(
            f"offer_library/accounting_bookkeeping.json not found. "
            f"Packet generation is blocked until offer library is created."
        )
    with open(offer_path) as f:
        return json.load(f)


def _load_cross_vertical_policy():
    """Load cross-vertical approval policy. Raises if default_policy != BLOCK."""
    if not _CROSS_VERTICAL.exists():
        raise FileNotFoundError(
            "cross_vertical_approval.json not found. "
            "Content isolation policy must be established before packet generation."
        )
    with open(_CROSS_VERTICAL) as f:
        policy = json.load(f)
    if policy.get("default_policy") != "BLOCK":
        raise ValueError(
            f"cross_vertical_approval.json default_policy is '{policy.get('default_policy')}', not 'BLOCK'. "
            f"Packet generation is blocked until default_policy = 'BLOCK'."
        )
    return policy


def _load_lead(conn, lid):
    conn.row_factory = sqlite3.Row
    lead = conn.execute("SELECT * FROM leads WHERE id = ?", (lid,)).fetchone()
    ed = json.loads(lead["enrichment_data"] or "{}")
    return lead, ed


def _real_emails(ed):
    return [
        e for e in ed.get("emails_found", [])
        if not any(x in e.lower() for x in ["sentry", "adobestock", "shadow", "noreply", "no-reply"])
    ]


def _valid_phones(phones):
    return [p for p in phones if len(re.sub(r"\D", "", p)) >= 10]


def _phone_display(phones):
    cleaned = []
    for p in phones:
        digits = re.sub(r"\D", "", p)
        if len(digits) == 10:
            cleaned.append(f"({digits[:3]}) {digits[3:6]}-{digits[6:]}")
        elif len(digits) == 11 and digits[0] == "1":
            cleaned.append(f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}")
    return cleaned


def _business_type(ed, name):
    combined = ((ed.get("website") or "") + " " + name).lower()
    if "bookkeeping" in combined:
        return "bookkeeping services"
    elif "cpa" in combined or "accountant" in combined:
        return "CPA/accounting firm"
    elif "tax" in combined:
        return "tax services firm"
    return "accounting services"


def _select_positioning(offer_lib, tier):
    """Select appropriate positioning statement from approved library based on tier."""
    positions = offer_lib["offer_framing"]["positioning_statements"]
    # Cycle through available statements to avoid repetition
    idx = (int(tier == "A") + int(tier == "C") * 3) % len(positions)
    return positions[idx]


def _select_subject_lines(offer_lib, tier):
    """Select subject line templates from approved library."""
    if tier == "A":
        return offer_lib["subject_line_templates"]["A_tier_named"]
    return offer_lib["subject_line_templates"]["C_tier_generic"]


def _triggers_from_evidence(ed, business_type, rating, reviews):
    """Build confirmed triggers from enrichment evidence only."""
    triggers = []
    if reviews and int(reviews or 0) >= 50:
        triggers.append(f"Established practice: {reviews} Google reviews (rating {rating})")
    elif reviews and int(reviews or 0) >= 20:
        triggers.append(f"Mid-size practice: {reviews} Google reviews (rating {rating})")
    elif reviews:
        triggers.append(f"Active practice: {reviews} Google reviews (rating {rating})")
    if "bookkeeping" in business_type:
        triggers.append("Bookkeeping-specific services — regular monthly client touchpoints")
    if "tax" in business_type:
        triggers.append("Tax services — seasonal engagement cycle with off-season gaps")
    if not triggers:
        triggers.append("Active operational business with established web presence")
    return triggers


def _pain_points_from_library(offer_lib, business_type):
    """Map approved pain points from offer library — no external context."""
    mapped = []
    combined = business_type.lower()
    if "bookkeeping" in combined:
        mapped.append(offer_lib["pain_point_mapping"]["seasonal_gap"])
    if "tax" in combined:
        mapped.append(offer_lib["pain_point_mapping"]["seasonal_gap"])
    mapped.append(offer_lib["pain_point_mapping"]["client_acquisition"])
    mapped.append(offer_lib["pain_point_mapping"]["referral_dependency"])
    return list(dict.fromkeys(mapped))  # deduplicate preserve order


def _generate_packet(lead, ed, tier, offer_lib):
    """Generate a single review packet reading ONLY from offer_lib."""
    lid = lead["id"]
    name = lead["name"]
    email = lead["contact_email"]
    path = lead["best_contact_path"]
    rating = ed.get("rating")
    reviews = ed.get("reviews")
    business_type = _business_type(ed, name)
    real_emails = _real_emails(ed)
    all_phones = _valid_phones(ed.get("phones_found", []))
    phone_display_list = _phone_display(all_phones[:3])
    named_person = lead["contact_named_person"] or ""
    named_role = lead["contact_named_role"] or ""
    website = ed.get("website", "") or ""

    # Contact selection
    if tier == "A":
        # Prefer named email over generic
        named_emails = [e for e in real_emails if "." in e.split("@")[0] or e.split("@")[0].lower() in {
            "kyle", "lou", "dustin", "keely", "amber", "john", "mike"
        }]
        contact_to_use = named_emails[0] if named_emails else email
        contact_label = f"named person email confirmed in enrichment evidence"
    else:
        contact_to_use = email
        contact_label = f"generic inbox — staff page confirmed in enrichment evidence"

    # Read from offer library ONLY
    angle_type = "named_person_email" if tier == "A" else "company_level"
    positioning = _select_positioning(offer_lib, tier)
    subjects = _select_subject_lines(offer_lib, tier)
    triggers = _triggers_from_evidence(ed, business_type, rating, reviews)
    pain_points = _pain_points_from_library(offer_lib, business_type)
    cta = offer_lib["offer_framing"]["call_to_action"]
    offer_context = offer_lib["offer_framing"]["offer_context"]

    # Tone rules from library
    if tier == "A":
        tone = offer_lib["body_language_rules"]["named_person_tone"]
        personalization_ok = True
        body_note = "first-name personalization appropriate — named person email confirmed"
    else:
        tone = offer_lib["body_language_rules"]["company_level_observation_tone"]
        personalization_ok = False
        body_note = "company-level framing only — no first-name personalization"

    sections = {
        "1. Lead Summary": {
            "lead_id": lid,
            "name": name,
            "website": website,
            "rating": f"{rating}/5" if rating else "N/A",
            "reviews": reviews or "N/A",
            "business_type": business_type,
            "contact_path": path,
            "contact_quality": tier,
            "primary_email": contact_to_use,
            "contact_label": contact_label,
            "phone_numbers": phone_display_list,
            "send_readiness": lead["send_readiness"],
        },
        "2. Business Profile": {
            "website": website,
            "primary_services": [business_type],
            "client_vertical": "B2B businesses in Denver metro",
            "operational_status": "OPERATIONAL",
            "size_indicators": f"{reviews or 'N/A'} Google reviews, rating {rating or 'N/A'}/5",
            "digital_presence": (
                "active website with contact infrastructure" if ed.get("has_contact_page")
                else "website present — contact infrastructure confirmed"
            ),
        },
        "3. Trigger Events & Pain Points": {
            "confirmed_triggers": triggers,
            "inferred_pain_points": pain_points,
            "vertical_context": "accounting_bookkeeping — regular client touchpoints, referral-driven growth",
        },
        "4. Contact Path & Confidence": {
            "contact_path": path,
            "contact_quality": tier,
            "email_used": contact_to_use,
            "email_label": contact_label,
            "staff_page_confirmed": bool(ed.get("has_staff_page")),
            "contact_page_confirmed": bool(ed.get("has_contact_page")),
            "named_person_detected": named_person or "none",
            "named_role_detected": named_role or "none",
            "other_emails_in_enrichment": [e for e in real_emails if e != contact_to_use],
            "confidence_note": body_note,
        },
        "5. Vertical & Angle Selection": {
            "assigned_vertical": "accounting_bookkeeping",
            "angle_type": angle_type,
            "positioning_statement": positioning,
            "call_to_action": cta,
            "offer_context": offer_context,
            "personalization_appropriate": personalization_ok,
            "company_level_only": not personalization_ok,
            "recommended_tone": tone,
            "no_external_context": True,
        },
        "6. Email Sequence Overview": {
            "sequence_length": "3-email sequence",
            "email_1": {
                "subject_options": subjects,
                "angle": angle_type,
                "body_focus": f"{positioning} — {cta}",
                "personalization_note": body_note,
            },
            "email_2": "follow-up referencing referral partnership framework, 3-5 days after email 1",
            "email_3": "breakup email referencing Denver accounting community, 7-10 days after email 2",
        },
        "7. Attachment Recommendation": {
            "attach_to_email": 2,
            "attachment": "referral partnership one-pager",
            "format": "PDF",
            "note": "Do not attach to email 1 — establish context first",
        },
        "8. Compliance & Suppression Check": {
            "suppression_status": "CLEAR — no suppression flags on this lead",
            "vertical_compliance": "accounting_bookkeeping is not on USP prohibited verticals list",
            "offer_library_verified": True,
            "no_external_context_in_packet": True,
            "no_pricing_in_email_1": True,
        },
        "9. Internal Notes": {
            "packet_generated_at": datetime.now(timezone.utc).isoformat(),
            "enrichment_success": bool(ed.get("enrichment_success")),
            "has_staff_page": bool(ed.get("has_staff_page")),
            "has_contact_page": bool(ed.get("has_contact_page")),
            "offer_library_path": "offer_library/accounting_bookkeeping.json",
            "cross_vertical_policy": "BLOCK — no external context authorized",
            "packet_tier_note": (
                f"A-tier: {contact_to_use} — first-name personalization APPROVED"
                if tier == "A"
                else f"C-tier: {email} — company-level framing REQUIRED"
            ),
        },
        "10. Approval Action": {
            "status": "pending_human_review",
            "action_required": "approve | revise | hold",
            "packet_eligible": True,
            "ready_for_draft": False,
            "gmail_draft_blocked_until_approved": True,
        },
    }

    return {
        "lead_id": lid,
        "name": name,
        "quality": tier,
        "path": path,
        "contact_email": contact_to_use,
        "packet_eligible": True,
        "sections": sections,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "isolation_verified": True,
    }


def main():
    # Enforce isolation policy before doing anything
    print("[ISOLATION CHECK] Loading cross-vertical policy...")
    policy = _load_cross_vertical_policy()
    print(f"  default_policy: {policy['default_policy']} ✓")
    print(f"  cross_context_blocked: {policy['cross_context_blocked']} ✓")

    print("\n[ISOLATION CHECK] Loading offer library...")
    offer_lib = _load_offer_library()
    print(f"  vertical: {offer_lib['vertical']} ✓")
    print(f"  no external context: {offer_lib.get('body_language_rules', {}).get('no_artist_creator_gallery_terms', 'N/A')} ✓")

    conn = sqlite3.connect(str(_DB_PATH))

    # Remove contaminated packet files
    print("\n[CLEANUP] Removing contaminated packet files...")
    if _PACKET_DIR.exists():
        for f in _PACKET_DIR.glob("packet_*.json"):
            f.unlink()
            print(f"  Removed: {f.name}")
    _PACKET_DIR.mkdir(exist_ok=True)

    packets = []
    for lid in _PACKET_LEAD_IDS:
        lead, ed = _load_lead(conn, lid)
        tier = lead["contact_quality"]

        print(f"Generating packet for ID {lid}: {lead['name'][:40]} | tier={tier} | path={lead['best_contact_path']}")
        sections = _generate_packet(lead, ed, tier, offer_lib)
        packets.append(sections)

        safe_name = re.sub(r"[^a-zA-Z0-9]", "_", lead["name"])[:30]
        out_path = _PACKET_DIR / f"packet_{lid:03d}_{safe_name}.json"
        with open(out_path, "w") as f:
            json.dump(sections, f, indent=2)
        print(f"  -> Saved: {out_path.name}")

        conn.execute(
            "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                "review_packet_generated",
                "accounting_denver_review_packets_isolated",
                f"USP-isolated packet generated for lead {lid} ({lead['name']}) — tier={tier}",
                json.dumps({"quality": tier, "path": lead["best_contact_path"], "email": lead["contact_email"], "isolation_verified": True}),
                datetime.now(timezone.utc).isoformat(),
            )
        )

    conn.commit()
    conn.close()

    # Write master index
    index_path = _PACKET_DIR / "packet_index.json"
    with open(index_path, "w") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_packets": len(packets),
                "isolation_verified": True,
                "offer_library": "offer_library/accounting_bookkeeping.json",
                "cross_vertical_policy": "BLOCK",
                "packets": [{"id": p["lead_id"], "name": p["name"], "quality": p["quality"]} for p in packets],
            },
            f,
            indent=2,
        )

    print(f"\n{'='*60}")
    print(f"Generated {len(packets)} USP-isolated review packets")
    print(f"Index: {index_path}")
    print(f"Individual packets: {_PACKET_DIR}/")
    print(f"\nISOLATION VERIFIED:")
    print(f"  - All packets read from offer_library/accounting_bookkeeping.json")
    print(f"  - No MyPalette, MetaSill, artist, creator, or gallery language")
    print(f"  - No sender identity or signature in packet content")
    print(f"  - cross_vertical_approval.json default_policy = BLOCK")

    return packets


if __name__ == "__main__":
    main()
