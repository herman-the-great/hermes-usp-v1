#!/usr/bin/env python3
"""
USP Daily Report — Multi-Vertical, USP-Local Only
Writes to: ~/.hermes/Hermes-USP-v1/reports/usp_daily_<YYYY-MM-DD>.md
           ~/.hermes/Hermes-USP-v1/reports/phone_monwed_<YYYY-MM-DD>.md  (Mon/Wed only)

Zero Gmail API calls. Zero jarvis.db access. Zero Mission Control.
Uses only: usp.db + offer_library files + local config.
"""
import sqlite3, json, os
from datetime import datetime, date

DB    = os.path.expanduser("~/.hermes/Hermes-USP-v1/usp.db")
ROOT  = os.path.expanduser("~/.hermes/Hermes-USP-v1")
REPORTS_DIR = os.path.join(ROOT, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

LOCKED_VERTICALS = [
    "accounting_bookkeeping",
    "estate_planning_probate",
    "collections_law",
    "family_law",
    "real_estate",
    "home_services",
]

VERTICAL_DISPLAY = {
    "accounting_bookkeeping":  "Accounting / Bookkeeping",
    "estate_planning_probate": "Estate Planning / Probate",
    "collections_law":        "Collections Law",
    "family_law":             "Family Law",
    "real_estate":            "Real Estate",
    "home_services":          "Home Services",
}

VERTICAL_SHORT = {
    "accounting_bookkeeping":  "Acctg",
    "estate_planning_probate": "Estate",
    "collections_law":        "Coll. Law",
    "family_law":             "Family Law",
    "real_estate":            "Real Est.",
    "home_services":          "Home Svc",
}

PAIN_POINTS = {
    "accounting_bookkeeping":  "document chasing and inbox overload",
    "estate_planning_probate": "client intake friction and document collection",
    "collections_law":        "demand letter response rates and intake velocity",
    "family_law":             "consult scheduling and document triage",
    "real_estate":            "lead follow-up failure and transaction coordination",
    "home_services":           "incoming inquiry capture and quote follow-up",
}


# ── Config helpers ────────────────────────────────────────────────────────────

def get_config(key, default=None):
    conn = sqlite3.connect(DB)
    row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
    conn.close()
    if row:
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, TypeError):
            return row[0]
    return default


# ── Data queries ──────────────────────────────────────────────────────────────

def get_lead_counts():
    """Per-vertical, per-qualification lead counts."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT
            COALESCE(assigned_vertical, 'unassigned') as vertical,
            qualification_state,
            contact_quality,
            outbound_state,
            COUNT(*) as cnt
        FROM leads
        WHERE assigned_vertical IN ({})
        GROUP BY assigned_vertical, qualification_state, contact_quality, outbound_state
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()

    # Restructure
    result = {}
    for v in LOCKED_VERTICALS:
        result[v] = {
            "qualified": 0, "candidate": 0, "disqualified": 0,
            "A": 0, "B": 0, "C": 0, "D": 0, "uncontactable": 0,
            "off_market": 0, "draft_queued": 0, "in_pipeline": 0,
            "phone_only": 0, "email_qualified": 0,
        }
    for row in rows:
        v   = row["vertical"]
        qs  = row["qualification_state"]
        cq  = row["contact_quality"]
        os_ = row["outbound_state"]
        cnt = row["cnt"]
        if v == 'unassigned' or v not in result:
            continue
        if qs == 'qualified': result[v]['qualified'] += cnt
        elif qs == 'candidate': result[v]['candidate'] += cnt
        elif qs == 'disqualified': result[v]['disqualified'] += cnt
        if cq in result[v]: result[v][cq] += cnt
        if cq in ('A', 'B', 'C'): result[v]['email_qualified'] += cnt
        if os_ == 'off_market': result[v]['off_market'] += cnt
        elif os_ == 'draft_queued': result[v]['draft_queued'] += cnt
        elif os_ == 'in_pipeline': result[v]['in_pipeline'] += cnt
        if cq == 'phone_only': result[v]['phone_only'] += cnt
    return result


def get_thread_state_counts():
    """Per-vertical thread counts by state."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT
            t.vertical,
            t.thread_state,
            COUNT(*) as cnt
        FROM outreach_threads t
        WHERE t.vertical IN ({})
        GROUP BY t.vertical, t.thread_state
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()

    result = {v: {} for v in LOCKED_VERTICALS}
    for v, state, cnt in rows:
        result.setdefault(v, {})[state] = cnt
    return result


def get_pending_drafts():
    """Threads in pending_approval — drafts awaiting human Send."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.vertical, t.gmail_draft_id, t.gmail_thread_id,
               l.name as lead_name, l.contact_quality, l.contact_path,
               t.updated_at
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'pending_approval'
          AND t.vertical IN ({})
        ORDER BY t.updated_at DESC
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stuck_drafting_threads():
    """
    Threads in 'drafting' state that are waiting for the draft generator.
    These have no gmail_draft_id — they're stuck because either:
      (a) the one-pager image is missing (HARD STOP), or
      (b) the lead's contact_email is phone_only.
    Returns dict keyed by vertical with list of {id, lead_name, days_stuck}.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.vertical, t.created_at,
               l.name as lead_name,
               l.contact_email,
               l.contact_path
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'drafting'
          AND t.gmail_draft_id IS NULL
          AND t.vertical IN ({})
        ORDER BY t.created_at ASC
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()

    result = {}
    for r in rows:
        result.setdefault(r['vertical'], []).append({
            'id': r['id'],
            'lead_name': r['lead_name'],
            'contact_email': r['contact_email'],
            'contact_path': r['contact_path'],
            'created_at': r['created_at'],
        })
    return result


def get_negative_reply_threads():
    """
    Threads in reply_received_negative state — lead replied with a no,
    but the system might be re-detecting the same reply every cycle.
    These need suppression to stop further outbound attempts.
    """
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.vertical, t.gmail_thread_id,
               l.name as lead_name, l.outbound_state,
               (SELECT e.created_at FROM outreach_events e
                WHERE e.thread_id = t.id AND e.event_type = 'negative_reply'
                ORDER BY e.id DESC LIMIT 1) as last_negative_at,
               (SELECT COUNT(*) FROM outreach_events e2
                WHERE e2.thread_id = t.id AND e2.event_type = 'negative_reply') as negative_count
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        WHERE t.thread_state = 'reply_received_negative'
          AND t.vertical IN ({})
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_active_threads():
    """Threads in active state."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT t.id, t.vertical, t.thread_state, t.current_email,
               l.name as lead_name, l.contact_quality,
               e.event_type, e.event_data, e.created_at
        FROM outreach_threads t
        JOIN leads l ON l.id = t.lead_id
        LEFT JOIN outreach_events e ON e.thread_id = t.id
        WHERE t.thread_state = 'active'
          AND t.vertical IN ({})
        ORDER BY t.updated_at DESC
        LIMIT 50
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_last_daily_run():
    conn = sqlite3.connect(DB)
    row = conn.execute("""
        SELECT MAX(started_at) FROM daily_runs WHERE job_name = 'usp_daily_report'
    """).fetchone()
    conn.close()
    return row[0] if row and row[0] else "never"


def get_phone_only_leads(limit=3):
    """Top phone_only leads ranked by opportunity score."""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT id, name, contact_phone, assigned_vertical,
               qualification_state, enrichment_last_run,
               enrichment_data
        FROM leads
        WHERE contact_path = 'phone_only'
          AND outbound_state = 'off_market'
          AND enrichment_last_run IS NOT NULL
          AND assigned_vertical IN ({})
        ORDER BY enrichment_last_run DESC
    """.format(','.join('?' * len(LOCKED_VERTICALS))), LOCKED_VERTICALS).fetchall()
    conn.close()

    scored = []
    for r in rows:
        d = dict(r)
        score = 0
        elr = d.get('enrichment_last_run', '')

        # Named contact signal: business name has 2+ words (likely has person name)
        name_parts = (d['name'] or '').split()
        has_named_contact = len(name_parts) >= 2
        if has_named_contact:
            score += 8

        # Vertical locked signal
        if d['assigned_vertical'] in LOCKED_VERTICALS:
            score += 3

        # Description richness from enrichment_data
        en_data = d.get('enrichment_data', '')
        if en_data:
            try:
                en = json.loads(en_data)
                desc = (en.get('business_description', '') or '').lower()
                if len(desc) > 80:
                    score += 4  # rich description signal
                if any(kw in desc for kw in ['estate', 'probate', 'trust', 'will', 'planning']):
                    score += 2
                if any(kw in desc for kw in ['accounting', 'bookkeeping', 'cpa', 'tax']):
                    score += 2
            except (json.JSONDecodeError, TypeError):
                pass

        # Freshness: enriched within last 7 days
        if elr:
            try:
                elr_dt = datetime.fromisoformat(elr.replace('Z', '+00:00'))
                days_ago = (datetime.now(elr_dt.tzinfo) - elr_dt).days
                freshness = max(0, min(3, (7 - days_ago) // 3))
                score += freshness
            except Exception:
                pass

        scored.append((score, d))

    scored.sort(key=lambda x: (-x[0], x[1].get('enrichment_last_run', '') or ''))
    return scored[:limit]


# ── Report builders ───────────────────────────────────────────────────────────

def build_daily_report():
    today_str = date.today().strftime("%Y-%m-%d")
    today_dow = date.today().strftime("%A")
    now_str   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    counts  = get_lead_counts()
    threads = get_thread_state_counts()
    pending = get_pending_drafts()
    active  = get_active_threads()
    last_run = get_last_daily_run()

    # Config state
    verts_enabled  = get_config("verticals_enabled", {})
    drafting_appr  = {v: get_config(f"drafting_approved_{v}", {}) for v in LOCKED_VERTICALS}
    one_pager      = {v: get_config(f"one_pager_policy_{v}", {}) for v in LOCKED_VERTICALS}

    # ── Section 1: Vertical Status Table ─────────────────────────────────
    vrows = []
    for v in LOCKED_VERTICALS:
        c = counts.get(v, {})
        en  = verts_enabled.get(v, False)
        da  = drafting_appr.get(v, {}).get("approved", False)
        op  = one_pager.get(v, {})
        ds  = op.get("drafting_status", "missing")
        dr  = op.get("drift_review_required", False)

        drafting_eligible = (
            en and da and ds == "extracted" and
            not bool(op.get("_note", ""))  # no outstanding issues
        )

        draft_eligible_str = "YES" if drafting_eligible else "BLOCKED"
        if not en:           draft_eligible_str = "disabled"
        elif not da:         draft_eligible_str = "no approval"
        elif ds != "extracted": draft_eligible_str = f"one-pager {ds}"

        qual   = c.get("qualified", 0)
        cand   = c.get("candidate", 0)
        disc   = c.get("disqualified", 0)
        eq     = c.get("email_qualified", 0)
        total  = qual + cand + disc

        th_drafting  = threads.get(v, {}).get("drafting", 0)
        # Count pending directly from the get_pending_drafts() result to ensure
        # consistency with the "Drafts Awaiting Send" section below.
        # get_thread_state_counts() was returning 0 for home_services despite
        # 7 threads being in pending_approval — using the pending list as
        # source of truth resolves the discrepancy.
        th_pending   = len([p for p in pending if p['vertical'] == v])
        th_active    = threads.get(v, {}).get("active", 0)

        vrows.append({
            "vertical":   VERTICAL_SHORT.get(v, v),
            "en":         "ON" if en else "OFF",
            "da":         "YES" if da else "no",
            "status":     ds.upper(),
            "draftable":  draft_eligible_str,
            "total":      total,
            "qualified":  qual,
            "eq":         eq,
            "cand":       cand,
            "disc":       disc,
            "off_mkt":    c.get("off_market", 0),
            "draft_q":    c.get("draft_queued", 0),
            "in_pipe":    c.get("in_pipeline", 0),
            "threads_dr": th_drafting,
            "threads_pd": th_pending,
            "threads_ac": th_active,
        })

    # ── Section 2: Pending drafts for review ─────────────────────────────
    pending_section = ""
    if pending:
        plines = ["", "## Drafts Awaiting Send", ""]
        plines.append("| # | Vertical | Lead | Tier | Draft Age |")
        plines.append("|---|----------|------|------|-----------|")
        for i, p in enumerate(pending, 1):
            age = "new"
            if p.get('updated_at'):
                try:
                    upd = datetime.fromisoformat(p['updated_at'].replace('Z','+00:00'))
                    age = f"{(datetime.now(upd.tzinfo) - upd).days}d"
                except Exception:
                    age = "?"
            vshort = VERTICAL_SHORT.get(p['vertical'], p['vertical'])
            plines.append(
                f"| {i} | {vshort} | {p['lead_name'][:30]} | "
                f"{p['contact_quality']} | {age} |"
            )
        pending_section = "\n".join(plines)
    else:
        pending_section = "\n## Drafts Awaiting Send\n\n  None."

    # ── Section 3: Active threads ─────────────────────────────────────────
    active_section = ""
    if active:
        alines = ["", "## Active Threads", ""]
        alines.append("| # | Vertical | Lead | Emails | Last Event |")
        alines.append("|---|----------|------|--------|------------|")
        seen = {}
        for i, a in enumerate(active, 1):
            tid = a['id']
            if tid in seen:
                continue
            seen[tid] = True
            vshort = VERTICAL_SHORT.get(a['vertical'], a['vertical'])
            last_evt = a.get('event_type') or 'unknown'
            alines.append(
                f"| {i} | {vshort} | {a['lead_name'][:25]} | "
                f"{a['current_email']} | {last_evt[:20]} |"
            )
        active_section = "\n".join(alines)
    else:
        active_section = "\n## Active Threads\n\n  None."

    # ── Section 4: What system did today ──────────────────────────────────
    # Get recent events from today
    conn = sqlite3.connect(DB)
    today_events = conn.execute("""
        SELECT COUNT(*) FROM outreach_events
        WHERE created_at >= date('now')
    """).fetchone()[0]
    today_packets = conn.execute("""
        SELECT COUNT(*) FROM packets
        WHERE created_at >= date('now')
    """).fetchone()[0]
    today_drafts = conn.execute("""
        SELECT COUNT(*) FROM outreach_events
        WHERE created_at >= date('now') AND event_type = 'draft_created'
    """).fetchone()[0]
    conn.close()

    # ── Section 5: Blockers ───────────────────────────────────────────────
    blockers = []
    for v in LOCKED_VERTICALS:
        op = one_pager.get(v, {})
        ds = op.get("drafting_status", "missing")
        en = verts_enabled.get(v, False)
        da = drafting_appr.get(v, {}).get("approved", False)
        vshort = VERTICAL_SHORT.get(v, v)
        if ds == "interpreted":
            blockers.append(f"  [{vshort}] one-pager needs real image (currently interpreted)")
        if ds == "missing":
            blockers.append(f"  [{vshort}] offer_library file missing — cannot draft")
        if not en:
            blockers.append(f"  [{vshort}] vertical disabled in config")
        if not da:
            blockers.append(f"  [{vshort}] drafting_approved = false — gate closed")
        # Check offer file
        if v != "accounting_bookkeeping":
            offer_path = os.path.join(ROOT, "offer_library", f"{v}.json")
            if not os.path.exists(offer_path):
                blockers.append(f"  [{vshort}] offer_library file missing")

    blockers_section = "\n".join(blockers) if blockers else "  None."

    # ── Section 5b: Stuck drafting threads ──────────────────────────────────
    # Threads in 'drafting' state that have no gmail_draft_id — stuck waiting
    # for the draft generator. The draft generator skips them if the one-pager
    # image is missing. Surface these so they get fixed.
    stuck_drafting = get_stuck_drafting_threads()
    stuck_lines = []
    for v in LOCKED_VERTICALS:
        threads_list = stuck_drafting.get(v, [])
        if not threads_list:
            continue
        vshort = VERTICAL_SHORT.get(v, v)
        for t in threads_list:
            # Compute days stuck
            try:
                created = datetime.fromisoformat(t['created_at'].replace('Z', '+00:00'))
                days = (datetime.now(created.tzinfo) - created).days
                days_str = f"{days}d"
            except Exception:
                days_str = "?"
            stuck_lines.append(
                f"  [{vshort}] thread {t['id']} — {t['lead_name'][:35]} — "
                f"stuck {days_str} | contact: {t['contact_path']}"
            )
    stuck_section = "\n".join(stuck_lines) if stuck_lines else "  None."

    # ── Section 5c: Negative reply threads needing suppression ─────────────
    # Threads in reply_received_negative that have duplicate negative_reply events
    # (same msg_id processed multiple times) — these need suppression to stop
    # the watcher from re-processing them every cycle.
    negative_reply = get_negative_reply_threads()
    neg_lines = []
    for t in negative_reply:
        vshort = VERTICAL_SHORT.get(t['vertical'], t['vertical'])
        dup_count = (t['negative_count'] or 1) - 1  # subtract 1 for the original
        dup_str = f" ({dup_count} duplicate events)" if dup_count > 0 else ""
        neg_lines.append(
            f"  [{vshort}] thread {t['id']} — {t['lead_name'][:35]} — "
            f"negative reply{dup_str}"
        )
    neg_section = "\n".join(neg_lines) if neg_lines else "  None."

    # ── Section 6: What system will do next ───────────────────────────────
    next_steps = []
    for v in LOCKED_VERTICALS:
        c = counts.get(v, {})
        en  = verts_enabled.get(v, False)
        da  = drafting_appr.get(v, {}).get("approved", False)
        op  = one_pager.get(v, {})
        ds  = op.get("drafting_status", "missing")
        draftable = en and da and ds == "extracted"
        eq = c.get("email_qualified", 0)
        off = c.get("off_market", 0)
        vshort = VERTICAL_SHORT.get(v, v)
        if draftable and off > 0:
            next_steps.append(f"  [{vshort}] ready to generate {off} drafts")
        elif draftable and off == 0:
            next_steps.append(f"  [{vshort}] approved but no off-market leads")
        elif not en:
            next_steps.append(f"  [{vshort}] enable in config when ready")
        elif not da:
            next_steps.append(f"  [{vshort}] needs human approval to draft")
        elif ds != "extracted":
            next_steps.append(f"  [{vshort}] one-pager must be confirmed extracted")

    next_steps_section = "\n".join(next_steps) if next_steps else "  None."

    # ── Assemble ──────────────────────────────────────────────────────────
    report_lines = [
        f"# USP Daily Summary — {today_str} ({today_dow})",
        "",
        "## Vertical Status",
        "",
        "| Vertical | En | Appr | OnePager | Draftable | Total | Qual | EQ | Cand | Disc | OffMkt | InPipe | Drft | Pndg | Act |",
        "|----------|----|------|----------|-----------|-------|------|----|------|------|--------|--------|-------|------|-----|",
    ]
    for r in vrows:
        report_lines.append(
            f"| {r['vertical']} | {r['en']} | {r['da']} | "
            f"{r['status']} | {r['draftable']} | "
            f"{r['total']} | {r['qualified']} | {r['eq']} | {r['cand']} | {r['disc']} | "
            f"{r['off_mkt']} | {r['in_pipe']} | "
            f"{r['threads_dr']} | {r['threads_pd']} | {r['threads_ac']} |"
        )

    report_lines += [
        "",
        pending_section,
        active_section,
        "",
        "## System Activity — Today",
        "",
        f"  Packets generated: {today_packets}",
        f"  Gmail drafts created: {today_drafts}",
        f"  Total events logged: {today_events}",
        f"  Last daily report run: {last_run}",
        "",
        "## Blockers",
        "",
        blockers_section,
        "",
        "## Stuck Drafting Threads",
        "",
        f"  {stuck_section}",
        "",
        "## Negative Reply Threads",
        "",
        f"  {neg_section}",
        "",
        "## Next Steps",
        "",
        next_steps_section,
        "",
        "## Human Action Required",
        "",
    ]

    # Human action required
    human_actions = []
    if pending:
        human_actions.append(f"  Review and click Send for {len(pending)} draft(s) in Gmail")
    for p in pending:
        vshort = VERTICAL_SHORT.get(p['vertical'], p['vertical'])
        human_actions.append(f"  [{p['gmail_draft_id']}] {p['lead_name']} ({vshort}) — in Gmail drafts")
    if not human_actions:
        human_actions.append("  None — system running cleanly")

    report_lines.extend(human_actions)
    report_lines += [
        "",
        f"_Generated: {now_str} — USP-local only, no jarvis/MC writes_",
    ]

    return "\n".join(report_lines)


def build_phone_report():
    """Monday/Wednesday phone-only surfacing report."""
    today_str = date.today().strftime("%Y-%m-%d")
    today_dow  = date.today().strftime("%A")
    is_mon_or_wed = date.today().weekday() in (0, 2)

    if not is_mon_or_wed:
        return None  # Only runs Mon/Wed

    leads = get_phone_only_leads(limit=3)
    if not leads:
        return None

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        f"# USP Phone-Only Surface — {today_dow} — {today_str}",
        "",
        "Manual outreach candidates — phone_only, qualified, off-market:",
        "",
        "| # | Business | Vertical | Phone | Score | Why |",
        "|---|----------|----------|-------|-------|-----|",
    ]

    for i, (score, lead) in enumerate(leads, 1):
        vdisp = VERTICAL_DISPLAY.get(lead['assigned_vertical'], lead['assigned_vertical'])
        phone = lead.get('contact_phone') or 'no phone'
        pain  = PAIN_POINTS.get(lead['assigned_vertical'], 'operational workflow')
        elr   = lead.get('enrichment_last_run', 'unknown')
        name  = lead['name']

        # Build why
        why_parts = []
        name_parts = (name or '').split()
        if len(name_parts) >= 2:
            why_parts.append("named contact")
        why_parts.append(f"in {vdisp}")
        why = ", ".join(why_parts)

        opener = (f"Hi {name_parts[0] if name_parts else 'there'}, "
                  f"I'm Herman Carter with USP LLC — I help {vdisp.lower()} firms clean up their {pain}. "
                  f"Do you have 90 seconds?")

        lines.append(
            f"| {i} | {name[:35]} | {vdisp} | {phone} | {score} | {why} |"
        )
        lines.append(f"|   | **Opener:** \"{opener}\" | | | | |")
        lines.append(f"|   | **Last enriched:** {elr} | | | | |")
        lines.append("| | | | | | |")

    lines += [
        "",
        f"_Generated: {now_str} — for manual outreach use only — not part of Gmail draft system_",
    ]

    return "\n".join(lines)


def run():
    # Daily report
    daily = build_daily_report()
    daily_path = os.path.join(REPORTS_DIR, f"usp_daily_{date.today().strftime('%Y-%m-%d')}.md")
    with open(daily_path, "w") as f:
        f.write(daily)
    print(f"Daily report: {daily_path}")

    # Phone report (Mon/Wed only)
    phone = build_phone_report()
    if phone:
        phone_path = os.path.join(REPORTS_DIR, f"phone_monwed_{date.today().strftime('%Y-%m-%d')}.md")
        with open(phone_path, "w") as f:
            f.write(phone)
        print(f"Phone report: {phone_path}")
    else:
        print("Phone report: not today (Mon/Wed only)")

    return daily_path


if __name__ == "__main__":
    run()
