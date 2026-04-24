#!/usr/bin/env python3
"""
Phase 1 Task 3: Config Table Initialization

Inserts the 14 required config rows if they don't already exist.
Idempotent: re-running is safe (skips existing keys).
"""
import sqlite3, json, os

DB = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")

ROWS = [
    # ── 6 one_pager_policy rows ───────────────────────────────────────────
    ("one_pager_policy_accounting_bookkeeping", json.dumps({
        "placement": "email_2",
        "format": "inline_image",
        "image_file": "accounting_bookkeeping_one_pager.jpg",
        "drafting_status": "extracted"
    })),

    ("one_pager_policy_estate_planning_probate", json.dumps({
        "placement": "email_2",
        "format": "inline_image",
        "image_file": "estate_planning_probate_one_pager.jpg",
        "drafting_status": "extracted"
    })),

    ("one_pager_policy_collections_law", json.dumps({
        "placement": "email_2",
        "format": "inline_image",
        "image_file": "collections_law_one_pager.jpg",
        "drafting_status": "extracted"
    })),

    ("one_pager_policy_family_law", json.dumps({
        "placement": "email_2",
        "format": "inline_image",
        "image_file": "family_law_one_pager.jpg",
        "drafting_status": "extracted"
    })),

    ("one_pager_policy_real_estate", json.dumps({
        "placement": "email_2",
        "format": "inline_image",
        "image_file": "real_estate_one_pager.jpg",
        "drafting_status": "extracted"
    })),

    ("one_pager_policy_home_services", json.dumps({
        "placement": "email_2",
        "format": "inline_image",
        "image_file": "home_services_one_pager.jpg",
        "drafting_status": "extracted"
    })),

    # ── vertical enabled/disabled state ──────────────────────────────────
    ("verticals_enabled", json.dumps({
        "accounting_bookkeeping": False,
        "estate_planning_probate": False,
        "collections_law": False,
        "family_law": False,
        "real_estate": False,
        "home_services": False
    })),

    # ── discovery auto-enabled state ─────────────────────────────────────
    ("discovery_auto_enabled", json.dumps({
        "accounting_bookkeeping": False,
        "estate_planning_probate": False,
        "collections_law": False,
        "family_law": False,
        "real_estate": False,
        "home_services": False
    })),

    # ── manual outreach schedule ──────────────────────────────────────────
    ("manual_outreach_schedule", json.dumps({
        "days": ["monday", "wednesday"],
        "max_phone_surfaced": 3
    })),

    # ── 4 drafting_approved flags (interpreted verticals) ─────────────────
    ("drafting_approved_collections_law", json.dumps({
        "approved": False,
        "approved_by": None,
        "approved_at": None
    })),

    ("drafting_approved_family_law", json.dumps({
        "approved": False,
        "approved_by": None,
        "approved_at": None
    })),

    ("drafting_approved_real_estate", json.dumps({
        "approved": False,
        "approved_by": None,
        "approved_at": None
    })),

    ("drafting_approved_home_services", json.dumps({
        "approved": False,
        "approved_by": None,
        "approved_at": None
    })),
]


def run():
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    created = 0
    skipped = 0

    for key, value in ROWS:
        try:
            c.execute(
                "INSERT INTO config (key, value) VALUES (?, ?)",
                (key, value)
            )
            created += 1
            print(f"  [+ Created: {key}]")
        except sqlite3.IntegrityError:
            skipped += 1
            print(f"  [= Skipped (exists): {key}]")

    conn.commit()

    total = c.execute("SELECT COUNT(*) FROM config").fetchone()[0]
    print(f"\nConfig init complete. Created: {created}, Skipped: {skipped}, Total rows: {total}")

    conn.close()


if __name__ == "__main__":
    run()
