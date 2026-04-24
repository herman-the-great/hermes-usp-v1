#!/usr/bin/env python3
"""
USP Phase 1 — Place Details: estate_planning_probate x Denver
Fetches Google Places Details API for leads 44-70 (27 estate_planning leads).
Stores website, phone, rating, reviews, photos in enrichment_data.
Does NOT insert/delete leads. Does NOT generate packets or drafts.
"""
import json
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
_CONFIG_PATH = USP_ROOT / "config.json"
_DETAILS_RESULTS_PATH = USP_ROOT / "estate_denver_details.json"

DETAILS_FIELDS = "website,formatted_address,geometry,icon,name,opening_hours,photos,place_id,rating,types,user_ratings_total,url"


def _load_config():
    with open(_CONFIG_PATH) as f:
        return json.load(f)


def _places_details(place_id, api_key):
    """Fetch Place Details for a single place_id."""
    url = (
        "https://maps.googleapis.com/maps/api/place/details/json"
        f"?place_id={place_id}&fields={DETAILS_FIELDS}&key={api_key}"
    )
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode())


def _now():
    return datetime.now(timezone.utc).isoformat()


def _log(conn, action, engine, detail, result=None):
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, engine, detail, result, _now())
    )
    conn.commit()


def main():
    cfg = _load_config()
    api_key = cfg["discovery"]["google_places_api_key"]
    if not api_key:
        print("ERROR: no google_places_api_key in config.json")
        return

    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Load all 27 estate_planning leads
    leads = conn.execute("""
        SELECT id, name, enrichment_data, notes
        FROM leads
        WHERE id >= 44 AND id <= 70
        ORDER BY id
    """).fetchall()

    print(f"[DETAILS] Fetching Details for {len(leads)} estate_planning leads...")

    all_details = []
    success = 0
    failed = 0
    no_website = 0

    for lead in leads:
        notes = json.loads(lead["notes"] or "{}")
        place_id = notes.get("place_id", "")

        if not place_id:
            print(f"  ID {lead['id']}: {lead['name'][:40]:40} — NO place_id, skipping")
            failed += 1
            continue

        try:
            result = _places_details(place_id, api_key)
            status = result.get("status", "UNKNOWN")
        except Exception as e:
            print(f"  ID {lead['id']}: {lead['name'][:40]:40} — API ERROR: {e}")
            failed += 1
            continue

        if status != "OK":
            print(f"  ID {lead['id']}: {lead['name'][:40]:40} — API status: {status}")
            failed += 1
            continue

        details = result.get("result", {})
        website = details.get("website", "")
        phone = details.get("formatted_phone_number", "")
        rating = details.get("rating")
        reviews = details.get("user_ratings_total", 0)
        address = details.get("formatted_address", "")
        url = details.get("url", "")
        photo_refs = [p.get("photo_reference", "") for p in details.get("photos", [])[:3]]

        # Build enriched details
        details_record = {
            "place_id": place_id,
            "website": website,
            "phone": phone,
            "rating": rating,
            "reviews": reviews,
            "address": address,
            "maps_url": url,
            "photo_refs": photo_refs,
            "types": details.get("types", []),
            "details_fetched_at": _now(),
        }
        all_details.append(details_record)

        # Update enrichment_data in leads table (notes field is preserved)
        conn.execute("""
            UPDATE leads SET
                enrichment_data = ?,
                enrichment_last_run = ?,
                assigned_vertical = COALESCE(assigned_vertical, 'estate_planning_probate')
            WHERE id = ?
        """, (json.dumps(details_record), _now(), lead["id"]))

        website_status = "HAS_WEBSITE" if website else "NO_WEBSITE"
        print(f"  ID {lead['id']:3d}: {lead['name'][:40]:40} | rating={rating} reviews={reviews} | {website_status}")

        if website:
            success += 1
        else:
            no_website += 1

        _log(conn, "estate_details_fetched", "estate_denver_details",
             f"ID {lead['id']} {lead['name'][:40]}: website={bool(website)}",
             json.dumps({"status": status, "website": bool(website)}))

        time.sleep(0.5)  # rate limit

    conn.commit()
    conn.close()

    # Save full details results
    with open(_DETAILS_RESULTS_PATH, "w") as f:
        json.dump(all_details, f, indent=2)

    print(f"\n[DETAILS] Complete: {success} with website | {no_website} no website | {failed} failed")
    print(f"[DETAILS] Results saved to: {_DETAILS_RESULTS_PATH}")

    return {
        "total": len(leads),
        "success": success,
        "no_website": no_website,
        "failed": failed,
    }


if __name__ == "__main__":
    main()
