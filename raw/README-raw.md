# `/raw` — immutable source archive

Treat every file here as a **historical record**. Do not rewrite, reformat, or delete ingested sources. New material is **appended** as new files (or versioned filenames).

## Contents (initial seed, 2026-07-30)

| File | Origin |
|------|--------|
| `README.md` | Snapshot of repo root README at wiki init |
| `env.example.md` | Snapshot of `.env.example` (no secrets) |
| `flash_tokens.md` | Project `doc/flash_tokens.md` if present |
| `session-2026-07-30-graph-expansion.md` | Operational notes from graph poolset expansion dry-runs |

## Ingestion rule

1. Drop new notes/transcripts/logs under `/raw/` with a dated name.
2. Tell the agent `/ingest <path>` (or paste content for archival).
3. Wiki pages under `/wiki/` are **compiled** from raw; they may be rewritten freely.
