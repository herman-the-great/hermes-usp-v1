#!/usr/bin/env python3
"""
USP Discovery - Full Text Search Pass: 5 industries × 7 geographies.
Text Search API only. No Place Details. No inserts to leads.
Stores raw responses in discovery_places for reporting.
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

from jobs.discovery import _log, _now_iso, _places_search, _load_config

# ── Config ──────────────────────────────────────────────────────────
cfg = _load_config()
API_KEY = cfg["google_places_api_key"]
RADIUS = 15000

# ── Industry query packs ────────────────────────────────────────────
QUERY_PACKS = {
    "accounting_bookkeeping": [
        "{city} Colorado certified public accountant CPA firm",
        "{city} Colorado small business bookkeeping services",
        "{city} Colorado accounting firm",
    ],
    "estate_planning_probate": [
        "{city} Colorado estate planning attorney trust lawyer",
        "{city} Colorado probate attorney",
        "{city} Colorado trust administration lawyer",
    ],
    "home_services": [
        "{city} Colorado plumber HVAC contractor",
        "{city} Colorado electrician plumbing service",
        "{city} Colorado home repair contractor",
    ],
    "collections_law": [
        "{city} Colorado debt collection attorney",
        "{city} Colorado collections law firm",
        "{city} Colorado creditor rights lawyer",
    ],
    "family_law": [
        "{city} Colorado family law attorney divorce",
        "{city} Colorado custody lawyer mediation",
        "{city} Colorado divorce attorney",
    ],
}

# ── Geographies ─────────────────────────────────────────────────────
GEOGRAPHIES = [
    ("Denver",         39.7392,  -104.9903),
    ("Broomfield",     39.8408,  -105.0467),
    ("Boulder",        40.0150,  -105.2705),
    ("Longmont",       40.1692,  -105.1017),
    ("Berthoud",       40.3083,  -105.0811),
    ("Loveland",       40.4153,  -105.0447),
    ("Fort Collins",   40.5853,  -105.0844),
]

# ── Suppression patterns ────────────────────────────────────────────
SUPPRESSION = [
    "subway", "mcdonald", "starbucks", "dunkin", "pizza hut", "domino",
    "home depot", "lowe's", "target", "walmart", "costco", "best buy",
    "franchise", "chain", "lead generation", "marketing solutions",
]

def passes_gates(place):
    rating  = place.get("rating")
    reviews = place.get("user_ratings_total")
    g_rating  = (rating  is not None and rating  >= 4.0)
    g_reviews = (reviews is not None and reviews >= 5)
    return g_rating, g_reviews

def is_suppressed(name):
    name_l = name.lower()
    return any(p in name_l for p in SUPPRESSION)

def text_search(query, location, radius):
    params = {
        "query": query,
        "location": f"{location[0]},{location[1]}",
        "radius": radius,
        "key": API_KEY,
    }
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode())

# ── Main ─────────────────────────────────────────────────────────────
print(f"[PASS] Starting Text Search pass: {len(QUERY_PACKS)} industries × {len(GEOGRAPHIES)} geographies", flush=True)
print(f"[PASS] Total API calls: {len(QUERY_PACKS) * len(GEOGRAPHIES) * 3}", flush=True)

all_results = []   # {industry, geography, query, raw_count, pass_rating, pass_reviews, pass_both, suppressed, results}
place_ids_seen = {}
names_seen = {}

for industry, queries in QUERY_PACKS.items():
    for geo_name, lat, lng in GEOGRAPHIES:
        location = (lat, lng)
        geo_queries = [q.replace("{city}", geo_name) for q in queries]

        for query in geo_queries:
            raw = text_search(query, location, RADIUS)
            results = raw.get("results", [])
            status  = raw.get("status", "UNKNOWN")

            pass_rating = pass_reviews = pass_both = suppressed = 0
            dup_place_id = dup_name = 0
            result_places = []

            for place in results:
                name     = place.get("name", "")
                pid      = place.get("place_id", "")
                rating   = place.get("rating")
                reviews  = place.get("user_ratings_total")
                types    = place.get("types", [])[:3]
                address  = place.get("formatted_address", "")
                g_r, g_rev = passes_gates(place)
                sup = is_suppressed(name)

                if g_r:  pass_rating  += 1
                if g_rev: pass_reviews += 1
                if g_r and g_rev: pass_both += 1
                if sup: suppressed += 1
                if pid in place_ids_seen: dup_place_id += 1
                if name.lower() in names_seen: dup_name += 1

                place_ids_seen[pid] = (industry, geo_name, query)
                names_seen[name.lower()] = (industry, geo_name, query)
                result_places.append({
                    "name": name, "place_id": pid,
                    "rating": rating, "reviews": reviews,
                    "types": types, "address": address,
                    "gate_both": g_r and g_rev, "suppressed": sup
                })

            record = {
                "industry": industry, "geography": geo_name,
                "query": query, "status": status,
                "raw_count": len(results),
                "pass_rating": pass_rating, "pass_reviews": pass_reviews,
                "pass_both": pass_both, "suppressed": suppressed,
                "dup_place_id": dup_place_id, "dup_name": dup_name,
                "places": result_places,
            }
            all_results.append(record)

            time.sleep(0.5)   # rate limit between calls

print(f"[PASS] All API calls complete. {len(all_results)} query records.", flush=True)

# ── Aggregate reporting ─────────────────────────────────────────────
print("\n" + "="*70)
print("PER-INDUSTRY SUMMARY")
print("="*70)
for industry in QUERY_PACKS:
    rows = [r for r in all_results if r["industry"] == industry]
    total_raw  = sum(r["raw_count"]   for r in rows)
    total_pr   = sum(r["pass_rating"]  for r in rows)
    total_pv   = sum(r["pass_reviews"] for r in rows)
    total_pb   = sum(r["pass_both"]    for r in rows)
    total_sup  = sum(r["suppressed"]   for r in rows)
    total_dpid = sum(r["dup_place_id"] for r in rows)
    total_dnm  = sum(r["dup_name"]     for r in rows)
    print(f"\n{industry}:")
    print(f"  Raw: {total_raw} | Rating>=4: {total_pr} | Rev>=5: {total_pv} | Both gates: {total_pb} | Suppressed: {total_sup}")
    print(f"  Dup place_id: {total_dpid} | Dup name: {total_dnm}")

print("\n" + "="*70)
print("PER-GEOGRAPHY SUMMARY (across all industries)")
print("="*70)
for geo_name, _, _ in GEOGRAPHIES:
    rows = [r for r in all_results if r["geography"] == geo_name]
    total_raw = sum(r["raw_count"] for r in rows)
    total_pb  = sum(r["pass_both"]  for r in rows)
    print(f"  {geo_name}: raw={total_raw}, both_gates={total_pb}")

print("\n" + "="*70)
print("CROSS-INDUSTRY DUPLICATE PRESSURE")
print("="*70)
print(f"  Unique place_ids seen: {len(place_ids_seen)}")
print(f"  Unique business names seen: {len(names_seen)}")
total_raw = sum(r["raw_count"] for r in all_results)
total_dpid = sum(r["dup_place_id"] for r in all_results)
total_dnm  = sum(r["dup_name"]     for r in all_results)
print(f"  Total raw results: {total_raw}")
print(f"  Total dup place_id hits: {total_dpid}")
print(f"  Total dup name hits: {total_dnm}")

# ── Top candidates per industry ──────────────────────────────────────
print("\n" + "="*70)
print("TOP CANDIDATES PER INDUSTRY (top 3 by rating×reviews)")
print("="*70)
for industry in QUERY_PACKS:
    rows = [r for r in all_results if r["industry"] == industry]
    all_places = []
    for r in rows:
        for p in r["places"]:
            if p["gate_both"] and not p["suppressed"]:
                score = (p["rating"] or 0) * (p["reviews"] or 0)
                all_places.append({**p, "score": score})
    all_places.sort(key=lambda x: -x["score"])
    print(f"\n{industry} (top {min(3, len(all_places))} by rating×reviews):")
    for p in all_places[:3]:
        print(f"  ★{p['rating']} ({p['reviews']} reviews) | {p['name']} | {p['types']} | {p['address'][:55]}")

# ── Projected Details volume ─────────────────────────────────────────
total_both = sum(r["pass_both"] for r in all_results)
# After dedupe by place_id: unique passing candidates
unique_passing_place_ids = set()
for r in all_results:
    for p in r["places"]:
        if p["gate_both"] and not p["suppressed"] and p["place_id"]:
            unique_passing_place_ids.add(p["place_id"])
unique_passing = len(unique_passing_place_ids)
est_details = int(unique_passing * 0.95)  # 95% pass OPERATIONAL + website gate

print("\n" + "="*70)
print("PROJECTED PLACE DETAILS VOLUME")
print("="*70)
print(f"  Total passing Text Search gates (raw): {total_both}")
print(f"  Unique passing place_ids (after place_id dedupe): {unique_passing}")
print(f"  Estimated Details calls (at 95% pass-through): ~{est_details}")
print(f"  Estimated Details cost: ~${est_details * 0.017:.2f}")
print(f"  Text Search cost: $0.32")
print(f"  Total estimated pass cost: ~${est_details * 0.017 + 0.32:.2f}")

# ── Save full results to a temp file for inspection ──────────────────
output_path = USP_ROOT / "textsearch_pass_results.json"
with open(output_path, "w") as f:
    json.dump({
        "all_results": all_results,
        "unique_place_ids": list(place_ids_seen.keys()),
        "unique_names": list(names_seen.keys()),
    }, f, default=str)
print(f"\n[PASS] Full results saved to: {output_path}")
