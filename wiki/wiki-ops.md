---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README-raw.md
---

# Wiki Operations

This repository hosts a compounding LLM wiki.

## Layout

| Path | Role |
|------|------|
| `/raw/` | Immutable sources — never overwrite |
| `/wiki/` | Synthesized concept pages with YAML front matter |
| `/index.md` | Master map of all pages |
| `/log.md` | Chronological ledger of ingest / compile / lint |

## Front matter (required on every wiki page)

```yaml
---
created: YYYY-MM-DD
updated: YYYY-MM-DD
sources:
  - raw/...
---
```

## Wikilinks

Use `[[page-slug]]` matching the filename without `.md` (e.g. `[[routing-graph]]` → `wiki/routing-graph.md`).

## Commands (agent)

| Intent | Action |
|--------|--------|
| `/ingest <path\|text>` | Append to `/raw/`, update 10–15 wiki pages + `index.md` + `log.md` |
| Lint / health | Scan orphans, broken links, stale contradictions; fix in `/wiki/` only |
| Init | Scaffold dirs + seed pages (this session) |

## Rules

1. **Never** mutate prior raw files.
2. Prefer updating existing nodes over spawning duplicates.
3. Log every batch action in `log.md` with date and summary.
4. Keep secrets out of raw and wiki (env values, keys, auth headers).

## Related

- [[rpbot-overview]]
