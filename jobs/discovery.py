#!/usr/bin/env python3
"""
Hermes-USP-v1 — Lead Discovery Job
Finds business candidates using Google Places API and saves to usp.db.

Isolation: Uses ~/.hermes/Hermes-USP-v1/config.json and usp.db only.
Does NOT touch jarvis.db, mission_control, or PPN Gmail/Gateway systems.
"""
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# Resolve paths relative to this file's location
_USP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DB_PATH = os.path.join(_USP_ROOT, "usp.db")
_CONFIG_PATH = os.path.join(_USP_ROOT, "config.json")


# ── Config Loader ────────────────────────────────────────────────

def _load_config():
    cfg = {"discovery": {}}
    if os.path.exists(_CONFIG_PATH):
        with open(_CONFIG_PATH) as f:
            cfg = json.load(f)
    return {
        "google_places_api_key": cfg.get("discovery", {}).get("google_places_api_key"),
        "default_radius_meters": cfg.get("discovery", {}).get("default_radius_meters", 8000),
        "default_location": cfg.get("discovery", {}).get("default_location", {"lat": 39.7392, "lng": -104.9903}),
        "max_results_per_query": cfg.get("discovery", {}).get("max_results_per_query", 20),
    }


# ── Audit Logger ─────────────────────────────────────────────────

def _log(action, detail=None, result=None, engine="discovery"):
    import sqlite3
    conn = sqlite3.connect(_DB_PATH)
    try:
        conn.execute(
            "INSERT INTO audit_log (action, engine, detail, result, created_at) VALUES (?, ?, ?, ?, ?)",
            (action, engine, detail, result, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    finally:
        conn.close()


# ── Google Places API ────────────────────────────────────────────

def _places_search(query, location, radius, api_key):
    """Text search via Google Places API. Returns parsed JSON dict."""
    base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "location": f"{location['lat']},{location['lng']}",
        "radius": radius,
        "key": api_key,
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read().decode())


# ── Normalization ────────────────────────────────────────────────

def _normalize_status(status: str) -> str:
    """Map Google Places operational status to our stage values."""
    mapping = {
        "OPERATIONAL": "identified",
        "CLOSED_TEMPORARILY": "dormant",
        "CLOSED_PERMANENTLY": "closed",
    }
    return mapping.get(status, "identified")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


# ── Main Discovery Run ────────────────────────────────────────────

def run_discovery(search_queries: dict = None, radius_meters: int = None,
                 location: dict = None, dry_run: bool = False):
    """
    Run lead discovery.

    Args:
        search_queries: dict of {category: [query_strings]} to search.
                        If None, uses built-in defaults.
        radius_meters: override config default.
        location: dict {lat, lng}; uses config default if None.
        dry_run: if True, fetch results but don't write to DB.
    """
    cfg = _load_config()
    api_key = cfg["google_places_api_key"]

    if not api_key:
        _log("discovery_run", result="error: no API key in USP config.json")
        raise ValueError("google_places_api_key not set in ~/.hermes/Hermes-USP-v1/config.json")

    location = location or cfg["default_location"]
    radius = radius_meters or cfg["default_radius_meters"]
    max_results = cfg["max_results_per_query"]

    # Default query set — edit these in USP config or pass at call time
    if search_queries is None:
        search_queries = {
            "web_agency": [
                "web design agencies Denver Colorado",
                "digital agencies Denver Colorado",
                "website development agencies Denver",
            ],
            "local_business": [
                "Denver small business websites",
                "local businesses without websites Denver",
            ],
        }

    total_found = 0
    total_inserted = 0
    import sqlite3
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row

    for category, queries in search_queries.items():
        for query in queries:
            _log("discovery_query_start", detail=f"[{category}] {query}", engine="discovery")

            # Fetch from Google Places
            try:
                raw = _places_search(query, location, radius, api_key)
            except Exception as e:
                _log("discovery_query_error", detail=f"{query}: {e}", engine="discovery")
                continue

            status = raw.get("status", "UNKNOWN")
            results = raw.get("results", [])

            # Store raw response
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO discovery_places
                   (query, location_lat, location_lng, radius_meters, raw_response, places_found, run_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (query, location["lat"], location["lng"], radius,
                 json.dumps(raw), len(results), _now_iso())
            )
            conn.commit()

            if status != "OK":
                _log("discovery_api_status", detail=f"{query}: {status}", engine="discovery")
                continue

            for place in results[:max_results]:
                total_found += 1

                # Check for duplicate by place_id
                existing = conn.execute(
                    "SELECT id FROM leads WHERE source='google_places' AND notes LIKE ?",
                    (f"%{place.get('place_id')}%",)
                ).fetchone()

                if existing:
                    continue  # skip already-known places

                # Extract fields
                name = place.get("name", "Unknown")
                addr = place.get("formatted_address", "")
                place_id = place.get("place_id", "")
                lat = place.get("geometry", {}).get("location", {}).get("lat")
                lng = place.get("geometry", {}).get("location", {}).get("lng")
                biz_type = ", ".join(place.get("types", []))
                rating = place.get("rating")
                open_status = place.get("opening_hours", {}).get("open_now")
                photo_ref = place.get("photos", [{}])[0].get("photo_reference", "") if place.get("photos") else ""

                stage = _normalize_status(
                    place.get("opening_hours", {}).get("periods_data", [{}])[0].get("open", {}).get("status", "OPERATIONAL")
                    if isinstance(place.get("opening_hours"), dict) and "periods_data" in place.get("opening_hours", {})
                    else "OPERATIONAL"
                )

                if dry_run:
                    continue

                cur.execute(
                    """INSERT INTO leads
                       (name, type, category, stage, source, source_url,
                        contact_method, contact_confidence, best_contact_path,
                        notes, priority, revenue_potential,
                        enrichment_data, enrichment_last_run,
                        send_readiness, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        name, biz_type, category, stage,
                        "google_places",
                        f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                        "email", "unresolved", "unresolved",
                        json.dumps({
                            "place_id": place_id,
                            "address": addr,
                            "lat": lat, "lng": lng,
                            "rating": rating,
                            "open_now": open_status,
                            "photo_ref": photo_ref,
                        }),
                        5,  # default priority
                        "unknown",
                        None, None,
                        "not_assessed",
                        _now_iso(), _now_iso(),
                    )
                )
                total_inserted += 1
                conn.commit()

            _log("discovery_query_done", detail=f"{query}: {len(results)} returned, {total_inserted} new", engine="discovery")

    conn.close()
    _log("discovery_run_complete",
         detail=f"total found={total_found}, inserted={total_inserted}, category={category}",
         result="success" if total_inserted > 0 else "no_new_leads",
         engine="discovery")
    return {"found": total_found, "inserted": total_inserted}


# ── CLI Entry Point ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="USP Lead Discovery")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    parser.add_argument("--category", help="Run only this category")
    parser.add_argument("--radius", type=int, help="Radius in meters override")
    parser.add_argument("--query", help="Single search query string (for testing)")
    args = parser.parse_args()

    cfg = _load_config()
    queries = None
    if args.query:
        queries = {"manual": [args.query]}
    elif args.category:
        default_queries = {
            "web_agency": ["web design agencies Denver Colorado"],
            "local_business": ["Denver small business websites"],
        }
        queries = {args.category: default_queries.get(args.category, [])}

    result = run_discovery(
        search_queries=queries,
        radius_meters=args.radius,
        dry_run=args.dry_run,
    )
    print(json.dumps(result))
