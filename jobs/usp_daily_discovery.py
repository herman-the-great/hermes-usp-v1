#!/usr/bin/env python3
"""
USP Daily Discovery — runs every morning at 6:05 AM MT.
Discovers new leads for all 3 verticals, enriches them, and runs targeting.
Safe to run daily — idempotent (skips leads already in DB by place_id,
skips leads already enriched/targeted).

Cost: ~10 Google Places API calls/day = ~$0.32/day = ~$9.60/month
(enrollment: 23 accounting leads are pre-loaded and fully enriched; they
run only if unenriched accounting leads exist.)
"""
import subprocess
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "usp.db"


def _log(msg):
    ts = datetime.now(timezone.utc).isoformat()
    print(f"[{ts}] {msg}", flush=True)


def _db_count(query, params=()):
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()
    cur.execute(query, params)
    result = cur.fetchone()[0]
    conn.close()
    return result


def run_script(name):
    """Run a discovery/enrichment script and return success."""
    path = ROOT / "jobs" / name
    _log(f">>> Running {name}...")
    try:
        result = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True, text=True, timeout=300
        )
        if result.stdout:
            _log(result.stdout[-400:])
        if result.stderr:
            _log(f"  STDERR: {result.stderr[-200:]}")
        return result.returncode == 0
    except Exception as e:
        _log(f"  ERROR: {e}")
        return False


def main():
    _log("=== USP Daily Discovery — Starting ===")

    # Step 1: Discovery — home_services across ALL Colorado metros
    # New businesses open every day — hitting all cities expands the funnel
    run_script("discover_home_services_denver.py")
    run_script("discover_home_services_boulder.py")
    run_script("discover_home_services_colorado_springs.py")
    run_script("discover_home_services_fort_collins.py")
    run_script("discover_home_services_loveland.py")

    # Step 2: Discovery — estate_planning_probate across ALL Colorado metros
    # Estate attorneys open practices regularly; needs fresh discovery every day
    # 4 cities x 5 queries = up to 400 new estate leads/week
    run_script("discover_estate_denver.py")
    run_script("discover_estate_boulder.py")
    run_script("discover_estate_colorado_springs.py")
    run_script("discover_estate_fort_collins.py")

    # Step 3: Accounting — DISABLED. Managed by USP Weekly Discovery (Sundays).
    # Accounting leads are fully loaded (23 leads from initial enrichment).
    # New accounting leads are discovered only by the Sunday cron job.
    # This prevents daily discovery from re-running accounting unnecessarily.
    _log(">>> Accounting: managed by weekly cron (Sundays) — skipping daily run.")

    # Step 4: Enrichment batch — processes up to 20 new leads
    # Safe to run daily; skips leads already enriched
    _log(">>> Running enrichment batch...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "jobs" / "run_enrichment_batch.py")],
        capture_output=True, text=True, timeout=300
    )
    if result.stdout:
        _log(result.stdout[-400:])
    if result.stderr:
        _log(f"  STDERR: {result.stderr[-200:]}")

    # Step 5: Targeting batch — qualifies enriched leads for outreach
    _log(">>> Running targeting batch...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "jobs" / "run_targeting_batch.py")],
        capture_output=True, text=True, timeout=300
    )
    if result.stdout:
        _log(result.stdout[-400:])
    if result.stderr:
        _log(f"  STDERR: {result.stderr[-200:]}")

    # ── Steps 6-8: Pipeline — moves qualified leads through to Gmail drafts ──
    # These are idempotent. Safe to run daily. Never auto-sends — drafts only.

    # Step 6: Move qualified off-market leads to queued (per enabled vertical)
    # Reads verticals_enabled from config; skips disabled verticals.
    _log(">>> Running move_offmarket_to_queued (all enabled verticals)...")
    try:
        from usp_packet_generator import move_offmarket_to_queued, get_enabled_verticals
        for vert in get_enabled_verticals():
            moved = move_offmarket_to_queued(vert)
            if moved:
                _log(f"  {vert}: {len(moved)} lead(s) moved off_market -> queued")
            else:
                _log(f"  {vert}: no off-market leads to move")
    except Exception as e:
        _log(f"  ERROR in move_offmarket_to_queued: {e}")

    # Step 7: Packet generator — creates packets + outreach threads for queued leads
    _log(">>> Running packet generator...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "jobs" / "usp_packet_generator.py")],
        capture_output=True, text=True, timeout=300
    )
    if result.stdout:
        _log(result.stdout[-600:])
    if result.stderr:
        _log(f"  STDERR: {result.stderr[-200:]}")

    # Step 8: Draft generator — creates Gmail drafts (draft state only, never sends)
    # Timeout 600s: enough for ~10 leads with Ollama inference + Gmail API calls
    _log(">>> Running draft generator...")
    result = subprocess.run(
        [sys.executable, str(ROOT / "jobs" / "usp_draft_generator.py")],
        capture_output=True, text=True, timeout=600
    )
    if result.stdout:
        _log(result.stdout[-600:])
    if result.stderr:
        _log(f"  STDERR: {result.stderr[-200:]}")

    # Summary
    total_leads = _db_count("SELECT COUNT(*) FROM leads")
    qualified = _db_count(
        "SELECT COUNT(*) FROM leads WHERE qualification_state='qualified' AND contact_quality IN ('A','B','C')"
    )
    email_qualified = _db_count(
        "SELECT COUNT(*) FROM leads WHERE qualification_state='qualified' AND contact_quality IN ('A','B','C') AND contact_email IS NOT NULL AND contact_email != '' AND contact_email != 'phone_only'"
    )
    active_threads = _db_count("SELECT COUNT(*) FROM outreach_threads WHERE thread_state='active'")

    _log("=== USP Daily Discovery — Complete ===")
    _log(f"  Total leads in system:    {total_leads}")
    _log(f"  Qualified leads:           {qualified}")
    _log(f"  Email-qualified leads:     {email_qualified}")
    _log(f"  Active outreach threads:   {active_threads}")


if __name__ == "__main__":
    main()
