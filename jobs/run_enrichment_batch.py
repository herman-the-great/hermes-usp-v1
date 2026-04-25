#!/usr/bin/env python3
"""
USP Draft Pipeline - Enrichment Batch Runner
Calls engines/enrichment.py:enrich_all_unenriched for all unenriched leads.
Enrichment fetches Google Places + homepage content. Does NOT use Ollama.
If enrichment_last_run is already set for a lead, it is skipped automatically.
"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.enrichment import enrich_all_unenriched

def main():
    print("[STEP 2] Starting enrichment batch...", flush=True)
    result = enrich_all_unenriched(batch_limit=200)
    print(json.dumps(result, indent=2, default=str), flush=True)

    print(f"[STEP 2] Enrichment complete: {result['successful']} succeeded, {result['failed']} failed", flush=True)

if __name__ == "__main__":
    main()
