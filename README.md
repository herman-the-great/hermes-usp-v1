# Hermes-USP-v1

**Status:** Foundation Built — 2026-04-10

Fully isolated from PPN/MyPalette systems. Uses `google_api_usp.py` and `google_token_usp.json` only.

## Isolation Boundaries

| Boundary | Mechanism |
|---|---|
| Database | `~/.hermes/Hermes-USP-v1/usp.db` — never shares with `jarvis.db` |
| Gmail | `google_token_usp.json` + `google_api_usp.py` — separate OAuth app token |
| Config | `~/.hermes/Hermes-USP-v1/config.json` — isolated from MC config |
| Cron | NOT YET CREATED — jobs/ folder is empty placeholder |
| Telegram | NOT YET CONFIGURED — summary_target null |

## Structure

```
Hermes-USP-v1/
├── config.json        # Isolated config (no shared credentials)
├── usp.db            # Isolated SQLite (separate from jarvis.db)
├── jobs/             # Cron jobs — NOT YET CREATED
├── engines/          # Draft engines — NOT YET CREATED
└── audits/           # Audit snapshots — NOT YET CREATED
```

## V1 Scope (This Foundation)

- [x] leads table (discovery logic ready — Google Places API key TBD)
- [x] outreach_emails table (draft storage + Gmail draft prep)
- [x] audit_log table (universal action log)
- [x] discovery_places table (Places API raw response cache)
- [x] enrichment_cache table (enrichment data cache)
- [ ] Draft generation engine
- [ ] Gmail draft save (via google_api_usp.py)
- [ ] Telegram summary preparation
- [ ] Daily action-item pipeline

## Google Places API Key

Set in `config.json` under `discovery.google_places_api_key`. Copy from:
`~/.hermes/mission_control/config.json` → `google_places_api_key` field.

## DO NOT

- Do NOT add cron jobs to system crontab
- Do NOT modify `~/.hermes/mission_control/` or `~/.hermes/jobs/`
- Do NOT use `google_api.py` (PPN) or `google_token.json` (PPN)
- Do NOT create Telegram routing entries
- Do NOT modify `jarvis.db` or any shared PPN database
