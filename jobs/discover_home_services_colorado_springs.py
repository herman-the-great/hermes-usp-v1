#!/usr/bin/env python3
"""
USP Phase 1 — Discovery: home_services x Colorado Springs, CO
EXPANSION CITY: Colorado Springs
Scope: 3 queries, 20 results each.
Dedupe: against all existing place_ids in usp.db.
Does NOT generate packets or Gmail drafts.
"""
import json, os, sys, urllib.request, urllib.parse, time
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))

_DB_PATH = USP_ROOT / "usp.db"
_CONFIG_PATH = USP_ROOT / "config.json"

COLORADO_SPRINGS_LOCATION = {"lat": 38.8339, "lng": -104.8214}

_HOME_SERVICE_QUERIES = [
    "roofing companies Colorado Springs Colorado",
    "HVAC companies Colorado Springs Colorado",
    "plumbing services Colorado Springs Colorado",
]


def _load_config():
    cfg = {"discovery": {}}
    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH) as f:
            cfg = json.load(f)
    return {"google_places_api_key": cfg.get("discovery", {}).get("google_places_api_key")}


def _places_search(query, location, radius, api_key):
    base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "location": f"{location['lat']},{location['lng']}",
              "radius": radius, "key": api_key}
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode())


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _log(action, detail=None, result=None):
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute(
        "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
        (action, "discovery_home_services_colorsprings", detail, result, _now_iso())
    )
    conn.commit()
    conn.close()


def run_discovery_home_services_colorado_springs(radius_meters=15000, dry_run=False):
    cfg = _load_config()
    api_key = cfg["google_places_api_key"]
    if not api_key:
        _log("discovery_run", result="error: no API key")
        raise ValueError("google_places_api_key not set")

    total_found = total_inserted = 0
    import sqlite3
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row

    for query in _HOME_SERVICE_QUERIES:
        _log("discovery_query_start", detail=f"[home_services/ColoradoSprings] {query}")
        try:
            raw = _places_search(query, COLORADO_SPRINGS_LOCATION, radius_meters, api_key)
        except Exception as e:
            _log("discovery_query_error", detail=f"{query}: {e}")
            continue

        results = raw.get("results", [])
        conn.execute(
            """INSERT INTO discovery_places (query, location_lat, location_lng, radius_meters, raw_response, places_found, run_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (query, COLORADO_SPRINGS_LOCATION["lat"], COLORADO_SPRINGS_LOCATION["lng"], radius_meters,
             json.dumps(raw), len(results), _now_iso())
        )
        conn.commit()

        if raw.get("status") != "OK":
            _log("discovery_api_status", detail=f"{query}: {raw.get('status')}")
            continue

        for place in results[:20]:
            total_found += 1
            name = place.get("name", "Unknown")
            addr = place.get("formatted_address", "")
            place_id = place.get("place_id", "")
            lat = place.get("geometry", {}).get("location", {}).get("lat")
            lng = place.get("geometry", {}).get("location", {}).get("lng")
            biz_type = ", ".join(place.get("types", []))
            rating = place.get("rating")
            open_status = place.get("opening_hours", {}).get("open_now")
            photo_ref = (place.get("photos", [{}])[0].get("photo_reference", "") if place.get("photos") else "")

            if conn.execute("SELECT id FROM leads WHERE notes LIKE ?", (f"%{place_id}%",)).fetchone():
                continue
            if dry_run:
                continue

            now = _now_iso()
            conn.execute(
                """INSERT INTO leads (name, type, category, stage, source, source_url, contact_method,
                   contact_confidence, best_contact_path, notes, priority, revenue_potential,
                   enrichment_data, enrichment_last_run, send_readiness, assigned_vertical,
                   created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (name, biz_type, "home_services", "identified", "google_places",
                 f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                 "email", "unresolved", "unresolved",
                 json.dumps({"place_id": place_id, "address": addr, "lat": lat, "lng": lng,
                             "rating": rating, "open_now": open_status, "photo_ref": photo_ref}),
                 5, "unknown", None, None, "not_assessed", "home_services", now, now)
            )
            total_inserted += 1
            conn.commit()

        _log("discovery_query_done", detail=f"{query}: {len(results)} returned, {total_inserted} new inserted")
        time.sleep(1)

    conn.close()
    _log("discovery_run_complete",
         detail=f"total found={total_found}, inserted={total_inserted}",
         result="success" if total_inserted > 0 else "no_new_leads")
    return {"found": total_found, "inserted": total_inserted}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="USP Home Services Discovery — Colorado Springs, CO")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting home_services x Colorado Springs, CO")
    result = run_discovery_home_services_colorado_springs(radius_meters=15000, dry_run=args.dry_run)
    print(json.dumps(result))
