# Atomicity Sentinel

Autonomous notifier that watches for the technical breakthroughs that would make
**cross-chain flashloan arbitrage** feasible — cross-chain shared sequencing,
native atomic bridging (cross-chain revert semantics), and ZK coprocessors that
lock state across chains — and pings you the moment one lands.

```
[arXiv / ePrint / ethresear.ch / Flashbots / GitHub releases]
        → keyword pre-filter (cheap, local)
        → Claude gatekeeper (claude-opus-4-8, structured verdict)
        → Telegram / Discord alert
```

The keyword pre-filter plays the role of the "vector database screen" — same
job (keep marketing noise away from the LLM), zero infrastructure.

## Setup

```sh
cd sentinel
pip install -r requirements.txt
cp .env.example .env   # fill in ANTHROPIC_API_KEY + a notification channel
```

## Usage

```sh
python3 sentinel.py --dry-run       # fetch + pre-filter only (no API key needed)
python3 sentinel.py --test-notify   # verify Telegram/Discord wiring
python3 sentinel.py                 # one full pass: fetch → score → alert
python3 sentinel.py --loop 21600    # daemon mode, every 6 hours
```

Cron (every 6 hours):

```
0 */6 * * * cd /home/x/arb/c/sentinel && /usr/bin/python3 sentinel.py >> sentinel.log 2>&1
```

## Tuning

| Flag | Default | Meaning |
|---|---|---|
| `--min-prefilter` | 3 | keyword score needed to send an item to Claude |
| `--min-score` | 6 | feasibility impact (0–10) needed to fire an alert |
| `--max-evals` | 15 | LLM-evaluation cap per pass (cost control) |
| `--state` | `state.json` | dedupe file (seen item IDs) |

Alerts at impact ≥ 8 are tagged 🚨 CRITICAL (e.g. live testnet with atomic
revert semantics); 6–7 are 📡 signals (strong theory / early implementations).

Sources are the `SOURCES` list at the top of `sentinel.py` — any RSS/Atom URL
works, including more GitHub `.../releases.atom` feeds. The breakthrough
criteria live in `SYSTEM_PROMPT`; the cheap gate keywords in
`STRONG_SIGNALS` / `WEAK_SIGNALS` / `NOISE`.

If no Telegram/Discord credentials are configured, alerts print to stdout, so
the pipeline is testable end-to-end without any messaging setup.
