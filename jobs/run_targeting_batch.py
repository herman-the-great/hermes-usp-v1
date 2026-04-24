#!/usr/bin/env python3
"""
USP Draft Pipeline - Contact Targeting Batch Runner
Calls engines/contact_targeting.py:target_all_uncontacted.
Targets all leads that have enrichment_data but unresolved contact_confidence.
Writes best_contact_path, contact_email, contact_confidence to leads table.
Does NOT use Ollama.
"""
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from engines.contact_targeting import target_all_uncontacted

def main():
    print("[STEP 3] Starting targeting batch...", flush=True)
    result = target_all_uncontacted()
    print(json.dumps(result, indent=2, default=str), flush=True)

    print(f"[STEP 3] Targeting complete: {result['total']} total, {result['viable']} viable, {result['weak_generic']} weak_generic, {result['phone_only']} phone_only, {result['no_contact']} no_contact", flush=True)

if __name__ == "__main__":
    main()
