#!/usr/bin/env python3
"""
USP Draft Pipeline - Smoke Tests
Phase 0: gate logic tests only, no Ollama required.
Tests Lead 1 (Tag Team Design - gate pass), Lead 4 (Anchovies - suppression),
Lead 3 (PRYDE - gate 4 fail).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

def get_lead_by_id(lead_id: int) -> dict:
    """Fetch a lead from the DB. Stub for smoke test only."""
    import sqlite3
    db_path = Path.home() / ".hermes/Hermes-USP-v1/usp.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def test_lead_1_suppression_check():
    """Lead 1 (Tag Team Design): should not be suppressed."""
    lead = get_lead_by_id(1)
    if lead is None:
        return {"test": "lead_1_suppression_check", "status": "FAIL", "reason": "lead not found"}
    suppression = lead.get("suppression_flag") or lead.get("send_readiness") or "not_assessed"
    passed = suppression not in ("suppressed", "wrong_fit", "do_not_contact")
    return {
        "test": "lead_1_suppression_check",
        "status": "PASS" if passed else "FAIL",
        "lead_id": 1,
        "suppression_flag": suppression,
        "expected": "not suppressed"
    }

def test_lead_4_suppression():
    """Lead 4 (Anchovies): should be suppressed."""
    lead = get_lead_by_id(4)
    if lead is None:
        return {"test": "lead_4_suppression", "status": "FAIL", "reason": "lead not found"}
    suppression = lead.get("suppression_flag") or "none"
    send_readiness = lead.get("send_readiness") or "not_assessed"
    passed = suppression in ("wrong_fit", "do_not_contact") or send_readiness == "suppressed"
    return {
        "test": "lead_4_suppression",
        "status": "PASS" if passed else "FAIL",
        "lead_id": 4,
        "suppression_flag": suppression,
        "send_readiness": send_readiness,
        "expected": "suppressed"
    }

def test_lead_3_gate_4_fail():
    """Lead 3 (PRYDE): phone only, no email - gate 4 should fail."""
    lead = get_lead_by_id(3)
    if lead is None:
        return {"test": "lead_3_gate_4_fail", "status": "FAIL", "reason": "lead not found"}
    contact_email = lead.get("contact_email") or ""
    contact_phone = lead.get("contact_phone") or ""
    has_email = bool(contact_email.strip()) and contact_email.strip() not in ("none", "N/A", "")
    gate_4_pass = has_email
    return {
        "test": "lead_3_gate_4_fail",
        "status": "PASS" if not gate_4_pass else "FAIL",
        "lead_id": 3,
        "contact_email": contact_email,
        "contact_phone": contact_phone,
        "gate_4_passed": gate_4_pass,
        "expected": "gate_4_fail"
    }

def main():
    tests = [
        test_lead_1_suppression_check,
        test_lead_4_suppression,
        test_lead_3_gate_4_fail,
    ]
    results = []
    for t in tests:
        try:
            results.append(t())
        except Exception as e:
            results.append({"test": t.__name__, "status": "ERROR", "reason": str(e)})

    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    errors = sum(1 for r in results if r["status"] == "ERROR")

    print("=== PHASE 0 SMOKE TESTS ===")
    for r in results:
        print(f"  [{r['status']}] {r['test']}: {r.get('reason') or r.get('expected') or ''}")
    print(f"Summary: {passed} passed, {failed} failed, {errors} errors")

    if failed > 0 or errors > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
