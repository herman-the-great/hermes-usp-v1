#!/usr/bin/env python3
"""
Phase 2: Packet Generator — Multi-Vertical
Generalized from accounting_bookkeeping-only to all enabled verticals.

Trigger: leads with outbound_state = 'queued'
  AND qualification_state = 'qualified'
  AND contact_quality IN ('A', 'B', 'C')
  AND assigned_vertical IN enabled verticals
  AND verticals_enabled[vertical] = true

Reads: offer_library/[vertical].json + leads table
Writes: packets table + outreach_threads table + leads.outbound_state update

Usage:
  python3 usp_packet_generator.py                      # all enabled verticals
  python3 usp_packet_generator.py --vertical estate_planning_probate  # single vertical
"""
import json, os, sqlite3, sys, argparse
from datetime import datetime

DB   = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")
ROOT = os.path.expanduser("~/.hermes/Hermes-USP-v1")


# ── Config helpers ────────────────────────────────────────────────────────────

def get_config(key, default=None):
    conn = sqlite3.connect(DB)
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return row[0]
    return default


def is_vertical_enabled(vertical):
    enabled = get_config("verticals_enabled", {})
    return enabled.get(vertical, False)


def get_enabled_verticals():
    """Return list of verticals where verticals_enabled = true AND offer file exists."""
    enabled = get_config("verticals_enabled", {})
    result = []
    for v, is_on in enabled.items():
        if not is_on:
            continue
        path = os.path.join(ROOT, "offer_library", f"{v}.json")
        if os.path.exists(path):
            result.append(v)
    return result


# ── Offer loading ────────────────────────────────────────────────────────────

def load_offer(vertical):
    path = os.path.join(ROOT, "offer_library", f"{vertical}.json")
    with open(path) as f:
        return json.load(f)


# ── Packet builder ────────────────────────────────────────────────────────────

def build_packet_text(lead, offer, vertical_display):
    """Assemble a human-readable packet from offer_library data + lead record."""
    name     = lead["name"]
    pain     = offer["pain_point_mapping"]

    sections = [
        f"━━━ OUTREACH PACKET ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Firm:       {name}",
        f"Vertical:   {vertical_display}",
        f"Contact:    {lead['contact_email']}",
        f"Path:       {lead['contact_path']} ({lead['contact_quality']}-tier)",
        f"Generated:  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        f"━━━ OPENING ANGLE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        offer["offer_framing"]["opening_angle"],
        "",
        f"━━━ PAIN POINTS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for key, desc in pain.items():
        sections.append(f"  • {key}: {desc}")

    sections += [
        "",
        f"━━━ POSITIONING ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for stmt in offer["offer_framing"]["positioning_statements"]:
        sections.append(f"  • {stmt}")

    sections += [
        "",
        f"━━━ TARGET PROFILE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    prof = offer["target_client_profile"]
    for key, val in prof.items():
        sections.append(f"  {key}: {val}")

    sections += [
        "",
        f"━━━ OFFER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for opt_key in offer.get("offer_options", {}):
        opt = offer["offer_options"][opt_key]
        sections += [
            f"  {opt_key}: {opt.get('name','—')}",
            f"  Price:   {opt.get('price_display','DISCUSSED_ON_CALL')}",
            f"  Includes: {', '.join(opt.get('includes',[]))}",
            "",
        ]

    sections += [
        f"━━━ PROCESS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━────────",
    ]
    for step, desc in offer.get("process_steps", {}).items():
        sections.append(f"  {step}: {desc}")

    sections += [
        "",
        f"━━━ CTA ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        offer["offer_framing"]["call_to_action"],
        f"Barrier: {offer['offer_framing']['offer_context']}",
        "",
        f"━━━ SUBJECT LINE TEMPLATES ━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for tier, templates in offer.get("subject_line_templates", {}).items():
        for t in templates:
            sections.append(f"  [{tier}] {t}")

    sections += [
        "",
        f"━━━ LANGUAGE RULES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"  No pricing in cold outreach: {offer['body_language_rules'].get('no_pricing', True)}",
        f"  No brand names: {offer['body_language_rules'].get('no_brand_references', True)}",
        f"  Footer: {offer['body_language_rules'].get('footer_context','')}",
        "",
        f"━━━ END PACKET ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(sections)


VERTICAL_DISPLAY = {
    "accounting_bookkeeping":   "Accounting / Bookkeeping",
    "estate_planning_probate":  "Estate Planning / Probate",
    "collections_law":          "Collections Law",
    "family_law":               "Family Law",
    "real_estate":              "Real Estate",
    "home_services":            "Home Services",
}


# ── Lead query ───────────────────────────────────────────────────────────────

def get_eligible_leads(vertical=None):
    """
    Return leads ready for packet generation.
    If vertical is specified, return leads for that vertical only.
    Otherwise return leads for all enabled verticals.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    if vertical:
        # Single vertical
        rows = conn.execute("""
            SELECT l.id, l.name, l.contact_email, l.contact_path,
                   l.contact_quality, l.assigned_vertical, l.outbound_state,
                   l.qualification_state
            FROM leads l
            WHERE l.outbound_state = 'queued'
              AND l.qualification_state = 'qualified'
              AND l.contact_quality IN ('A', 'B', 'C')
              AND l.assigned_vertical = ?
        """, (vertical,)).fetchall()
    else:
        # All enabled verticals
        rows = conn.execute("""
            SELECT l.id, l.name, l.contact_email, l.contact_path,
                   l.contact_quality, l.assigned_vertical, l.outbound_state,
                   l.qualification_state
            FROM leads l
            WHERE l.outbound_state = 'queued'
              AND l.qualification_state = 'qualified'
              AND l.contact_quality IN ('A', 'B', 'C')
        """).fetchall()

    conn.close()
    return [dict(r) for r in rows]


def move_offmarket_to_queued(vertical):
    """
    Select qualified leads from off_market to queued for a vertical.
    GUARD: Never move a lead to queued if:
      (a) it already has an outreach thread (any non-terminal state), OR
      (b) it has no real email address (phone_only / generic_inbox / unresolved)
    This prevents duplicate thread creation AND prevents queuing leads
    that cannot receive Gmail drafts.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    leads = conn.execute("""
        SELECT l.id, l.name, l.contact_email, l.contact_path, l.contact_quality,
               l.assigned_vertical, l.outbound_state, l.qualification_state,
               t.id as thread_id, t.thread_state
        FROM leads l
        LEFT JOIN outreach_threads t ON t.lead_id = l.id
            AND t.thread_state NOT IN ('closed', 'rejected', 'bounced')
        WHERE l.qualification_state = 'qualified'
          AND l.contact_quality IN ('A', 'B', 'C')
          AND l.assigned_vertical = ?
          AND l.outbound_state = 'off_market'
          AND l.contact_email IS NOT NULL
          AND l.contact_email != ''
          AND l.contact_email != 'phone_only'
          AND (t.thread_state IS NULL OR t.thread_state NOT IN ('closed', 'rejected', 'bounced'))
        ORDER BY l.id
    """, (vertical,)).fetchall()
    conn.close()

    # Filter: skip leads that already have an active thread
    eligible = []
    skipped  = []
    for row in leads:
        if row['thread_id'] is not None:
            # Lead already has a thread — do NOT move to queued
            skipped.append(dict(row))
        else:
            eligible.append(dict(row))

    if skipped:
        print(f"  WARNING: {len(skipped)} off_market lead(s) already have threads — NOT moving to queued:")
        for s in skipped:
            print(f"    Lead {s['id']}: {s['name']} (thread {s['thread_id']} in '{s['thread_state']}')")

    if not eligible:
        return []

    conn = sqlite3.connect(DB)
    for l in eligible:
        conn.execute("UPDATE leads SET outbound_state='queued' WHERE id=?", (l['id'],))
    conn.commit()
    conn.close()
    return eligible


# ── Main ─────────────────────────────────────────────────────────────────────

def run(vertical=None, verbose=True):
    """
    Run packet generation.
    If vertical is specified, process that vertical only.
    If vertical is None, process all enabled verticals.
    """
    # Determine verticals to process
    if vertical:
        if not is_vertical_enabled(vertical):
            if verbose:
                print(f"Vertical '{vertical}' is not enabled in config.")
            return {"status": "disabled", "vertical": vertical}

        offer_path = os.path.join(ROOT, "offer_library", f"{vertical}.json")
        if not os.path.exists(offer_path):
            if verbose:
                print(f"No offer_library file for '{vertical}'. Skipping.")
            return {"status": "no_offer_file", "vertical": vertical}

        verticals_to_run = [(vertical,)]
    else:
        verticals_to_run = [(v,) for v in get_enabled_verticals()]

    all_created = []
    all_skipped = []

    for (v,) in verticals_to_run:
        offer = load_offer(v)

        # Check for queued leads first
        eligible = get_eligible_leads(v)

        if not eligible:
            # Try moving off_market → queued as fallback
            if verbose:
                print(f"No 'queued' leads for {v}. Trying off_market fallback...")
            moved = move_offmarket_to_queued(v)
            if moved:
                eligible = moved
                if verbose:
                    print(f"  Moved {len(moved)} leads to queued: {[l['name'] for l in moved]}")
            else:
                if verbose:
                    print(f"No eligible leads for {v}.")
                all_skipped.append(v)
                continue

        display = VERTICAL_DISPLAY.get(v, v)
        created_packets = []

        conn = sqlite3.connect(DB)

        for lead in eligible:
            # USP-STANDARD duplicate-thread guard.
            # Even though get_eligible_leads only returns 'queued' leads,
            # operator actions or race conditions can leave leads in 'queued'
            # that already have a thread. Skip them to prevent duplicates.
            # Import from draft generator (same DB path assumed).
            try:
                from usp_draft_generator import lead_has_active_thread
            except ImportError:
                # Fallback inline check if import fails
                existing = conn.execute(
                    "SELECT id FROM outreach_threads WHERE lead_id=? AND vertical=? AND thread_state NOT IN ('rejected','bounced') LIMIT 1",
                    (lead['id'], v)
                ).fetchone()
                has_thread = existing is not None
            else:
                has_thread = lead_has_active_thread(lead['id'], v)

            if has_thread:
                if verbose:
                    print(f"  SKIPPED {lead['name']} (id={lead['id']}): already has an active thread — skipping packet creation")
                # Move it back to off_market so it doesn't get picked up again
                conn.execute("UPDATE leads SET outbound_state='off_market' WHERE id=?", (lead['id'],))
                continue

            packet_text = build_packet_text(lead, offer, display)

            # Write packet
            c = conn.cursor()
            c.execute("""
                INSERT INTO packets (lead_id, vertical, packet_text, created_by)
                VALUES (?, ?, ?, ?)
            """, (lead['id'], v, packet_text, 'system'))
            packet_id = c.lastrowid

            # Create outreach thread
            c.execute("""
                INSERT INTO outreach_threads
                    (lead_id, vertical, thread_state, current_email)
                VALUES (?, ?, 'drafting', 0)
            """, (lead['id'], v))
            thread_id = c.lastrowid

            # Update lead to draft_queued
            c.execute("UPDATE leads SET outbound_state='draft_queued' WHERE id=?", (lead['id'],))

            created_packets.append({
                'lead_id': lead['id'],
                'name': lead['name'],
                'packet_id': packet_id,
                'thread_id': thread_id,
                'tier': lead['contact_quality'],
            })

            if verbose:
                print(f"  Packet #{packet_id} + Thread #{thread_id}: {lead['name']} "
                      f"({lead['contact_path']} / {lead['contact_quality']}-tier)")

        conn.commit()
        conn.close()

        all_created.extend(created_packets)

    if verbose:
        print(f"\nPackets created: {len(all_created)}")
        if all_skipped:
            print(f"Skipped (no leads or not enabled): {all_skipped}")
        print(f"All leads moved to: draft_queued")
        print(f"Next step: Run draft generator to create Gmail drafts.")

    return {
        "status": "ok",
        "created": all_created,
        "skipped": all_skipped,
        "total": len(all_created),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="USP Packet Generator — Multi-Vertical")
    parser.add_argument("--vertical", help="Process specific vertical only")
    args = parser.parse_args()

    result = run(vertical=args.vertical)
    sys.exit(0 if result["status"] == "ok" else 1)
