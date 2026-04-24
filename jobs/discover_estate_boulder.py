#!/usr/bin/env python3
"""
USP Phase 1 — Discovery: estate_planning_probate x Boulder, CO
Scope: 5 queries, 20 results each, Boulder city center.
Dedupe: against all existing place_ids in usp.db.

Do not generate review packets. No Gmail drafts.
"""
import json, sys
from datetime import datetime, timezone
from pathlib import Path

USP_ROOT = Path.home() / ".hermes/Hermes-USP-v1"
sys.path.insert(0, str(USP_ROOT))

from jobs.discovery import run_discovery

BOULDER_LOCATION = {"lat": 40.0150, "lng": -105.2705}
BOULDER_QUERIES = {
    "estate_planning_probate": [
        "estate planning attorney Boulder Colorado",
        "probate attorney Boulder Colorado",
        "trust attorney Boulder Colorado",
        "wills and estate attorney Boulder Colorado",
        "estate planning services Boulder Colorado",
    ]
}

def _log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

_log("Starting estate_planning_probate x Boulder discovery")
_log(f"Location: {BOULDER_LOCATION}")
_log(f"Queries: {len(BOULDER_QUERIES['estate_planning_probate'])}")

result = run_discovery(
    search_queries=BOULDER_QUERIES,
    radius_meters=12000,
    location=BOULDER_LOCATION,
)

_log(f"Discovery complete: {result}")
print(json.dumps(result))
