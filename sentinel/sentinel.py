#!/usr/bin/env python3
"""Cross-chain atomicity sentinel.

Watches research feeds and protocol repos for breakthroughs that would make
cross-chain flashloan arbitrage feasible (cross-chain shared sequencing,
native atomic bridging, ZK coprocessors locking state across chains), scores
candidates with Claude, and pushes alerts to Telegram / Discord.

Pipeline:  feeds -> keyword pre-filter -> Claude gatekeeper -> alert

Usage:
    python3 sentinel.py --dry-run        # fetch + pre-filter only, no LLM/alerts
    python3 sentinel.py                  # one full pass
    python3 sentinel.py --loop 21600     # run forever, every 6h
    python3 sentinel.py --test-notify    # send a test alert to configured channels

Env vars (also read from sentinel/.env if present):
    ANTHROPIC_API_KEY      required unless --dry-run
    TELEGRAM_BOT_TOKEN     optional  (with TELEGRAM_CHAT_ID)
    TELEGRAM_CHAT_ID       optional
    DISCORD_WEBHOOK_URL    optional
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import feedparser
import requests
from pydantic import BaseModel, Field

HERE = Path(__file__).resolve().parent
DEFAULT_STATE = HERE / "state.json"

# --------------------------------------------------------------------------
# 1. Data ingestion — the places where the relevant work actually gets posted
# --------------------------------------------------------------------------

ARXIV_QUERY = (
    'http://export.arxiv.org/api/query?search_query='
    '%28cat:cs.CR+OR+cat:cs.DC%29+AND+%28'
    'all:%22cross-chain%22+OR+all:%22shared+sequencer%22+OR+'
    'all:%22atomic+composability%22+OR+all:%22interoperability%22%29'
    '&sortBy=submittedDate&sortOrder=descending&max_results=40'
)

SOURCES: list[tuple[str, str]] = [
    ("arXiv cs.CR/cs.DC", ARXIV_QUERY),
    ("Cryptology ePrint", "https://eprint.iacr.org/rss/rss.xml"),
    ("ethresear.ch", "https://ethresear.ch/latest.rss"),
    ("Flashbots forum", "https://collective.flashbots.net/latest.rss"),
    # Protocol release feeds (GitHub Atom, no auth needed)
    ("LayerZero v2 releases", "https://github.com/LayerZero-Labs/LayerZero-v2/releases.atom"),
    ("Chainlink releases", "https://github.com/smartcontractkit/chainlink/releases.atom"),
    ("Wormhole releases", "https://github.com/wormhole-foundation/wormhole/releases.atom"),
    ("Espresso releases", "https://github.com/EspressoSystems/espresso-network/releases.atom"),
    ("Astria releases", "https://github.com/astriaorg/astria/releases.atom"),
]


@dataclass
class Item:
    uid: str
    source: str
    title: str
    summary: str
    link: str
    published: str


def fetch_all(timeout: int = 90) -> list[Item]:
    items: list[Item] = []
    headers = {"User-Agent": "atomicity-sentinel/1.0"}
    for source, url in SOURCES:
        feed = None
        for attempt in (1, 2):
            try:
                resp = requests.get(url, headers=headers, timeout=timeout)
                resp.raise_for_status()
                feed = feedparser.parse(resp.content)
                break
            except Exception as exc:  # noqa: BLE001 - a dead feed must not kill the run
                if attempt == 1:
                    time.sleep(10)  # arXiv et al. rate-limit; one backoff retry
                else:
                    print(f"[warn] {source}: fetch failed: {exc}", file=sys.stderr)
        if feed is None:
            continue
        for e in feed.entries:
            uid = e.get("id") or e.get("link") or f"{source}:{e.get('title', '')}"
            summary = re.sub(r"<[^>]+>", " ", e.get("summary", "") or "")
            items.append(Item(
                uid=uid,
                source=source,
                title=(e.get("title") or "").strip(),
                summary=re.sub(r"\s+", " ", summary).strip()[:4000],
                link=e.get("link", ""),
                published=e.get("published", e.get("updated", "")),
            ))
    return items


# --------------------------------------------------------------------------
# 2. Keyword pre-filter — cheap gate so only plausible candidates hit the LLM
#    (stands in for the article's vector-DB screen; same job, zero infra)
# --------------------------------------------------------------------------

STRONG_SIGNALS = [
    "cross-chain atomic", "atomic cross-chain", "cross-chain flash loan",
    "cross-chain flashloan", "shared sequencer", "shared sequencing",
    "atomic composability", "synchronous composability", "atomic inclusion",
    "cross-rollup atomic", "atomic bridging", "zk coprocessor",
    "zero-knowledge coprocessor", "cross-chain state sync", "atomic settlement",
]
WEAK_SIGNALS = [
    "atomicity", "cross-chain", "cross-rollup", "interoperability",
    "sequencer", "composability", "state lock", "flash loan", "flashloan",
    "bridge", "rollup", "zk proof", "zero-knowledge", "coprocessor",
]
NOISE = [
    "airdrop", "token launch", "funding round", "raises $", "price prediction",
    "listing", "partnership announcement", "grant program", "hackathon",
]


def prefilter_score(item: Item) -> int:
    text = f"{item.title} {item.summary}".lower()
    score = sum(3 for kw in STRONG_SIGNALS if kw in text)
    score += sum(1 for kw in WEAK_SIGNALS if kw in text)
    score -= sum(2 for kw in NOISE if kw in text)
    return score


# --------------------------------------------------------------------------
# 3. LLM gatekeeper — Claude scores candidates against the breakthrough bar
# --------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a Web3 research analyst monitoring breakthroughs in blockchain \
interoperability on behalf of a cross-chain arbitrage operator.

Context: cross-chain flashloan arbitrage is currently infeasible because \
flashloans require borrow, trade, and repayment in a single atomic \
transaction, and separate chains cannot natively wrap actions into one \
atomic block. Feasibility changes only if one of these lands:

1. Frameworks enabling multi-chain atomic transactions, where a revert on \
Chain B forces a revert on Chain A.
2. Shared sequencers that batch cross-L2 or cross-chain state transitions \
natively.
3. New primitives for cross-chain flash loans using ZK proofs or \
locked-liquidity constructions.

Disregard generic marketing announcements about new bridges, speed upgrades, \
funding rounds, token launches, or ordinary protocol releases. Routine \
message-passing bridges (lock-and-mint, optimistic or light-client \
verification) do NOT qualify — they do not provide atomicity.

Mark is_breakthrough true only when the text describes a theoretical or live \
implementation of one of the three items above. Score feasibility_impact \
0-10 for how much it moves cross-chain flashloan arbitrage toward being \
practically executable (theory-only papers rarely exceed 5; a live testnet \
with atomic revert semantics is 7+; production mainnet atomicity is 9+).\
"""


class Evaluation(BaseModel):
    is_breakthrough: bool = Field(description="True only if it matches one of the three breakthrough categories")
    category: str = Field(description="One of: atomic_transactions, shared_sequencing, zk_flashloan_primitive, not_relevant")
    maturity: str = Field(description="One of: theoretical, testnet, mainnet, unknown")
    feasibility_impact: int = Field(ge=0, le=10, description="0-10 impact on cross-chain flashloan arbitrage feasibility")
    headline: str = Field(description="One-line description of the breakthrough (or why it was rejected)")
    why_it_changes_feasibility: str = Field(description="2-3 sentences on the mechanism, or empty if not a breakthrough")
    action_item: str = Field(description="Concrete next step for the operator, or empty if not a breakthrough")


def evaluate(client, item: Item) -> Evaluation:
    response = client.messages.parse(
        model="claude-opus-4-8",
        max_tokens=16000,
        thinking={"type": "adaptive"},
        system=SYSTEM_PROMPT,
        messages=[{
            "role": "user",
            "content": (
                f"Source: {item.source}\n"
                f"Title: {item.title}\n"
                f"Link: {item.link}\n"
                f"Published: {item.published}\n\n"
                f"Content:\n{item.summary or '(no summary available)'}"
            ),
        }],
        output_format=Evaluation,
    )
    return response.parsed_output


# --------------------------------------------------------------------------
# 4. Notification layer
# --------------------------------------------------------------------------

def format_alert(item: Item, ev: Evaluation) -> str:
    tag = "🚨 CRITICAL TECH ALERT" if ev.feasibility_impact >= 8 else "📡 Atomicity signal"
    return (
        f"{tag}: {ev.headline}\n\n"
        f"Source: {item.source} — {item.title}\n"
        f"Category: {ev.category} | Maturity: {ev.maturity}\n"
        f"Why it changes feasibility: {ev.why_it_changes_feasibility}\n"
        f"Feasibility impact: {ev.feasibility_impact}/10\n"
        f"Action item: {ev.action_item}\n"
        f"Link: {item.link}"
    )


def notify(text: str) -> bool:
    sent = False
    tg_token, tg_chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if tg_token and tg_chat:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{tg_token}/sendMessage",
                json={"chat_id": tg_chat, "text": html.unescape(text)[:4000],
                      "disable_web_page_preview": True},
                timeout=15,
            )
            r.raise_for_status()
            sent = True
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] telegram send failed: {exc}", file=sys.stderr)
    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    if webhook:
        try:
            r = requests.post(webhook, json={"content": text[:1990]}, timeout=15)
            r.raise_for_status()
            sent = True
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] discord send failed: {exc}", file=sys.stderr)
    if not sent:
        print("\n" + "=" * 70 + f"\n{text}\n" + "=" * 70)
    return sent


# --------------------------------------------------------------------------
# State + orchestration
# --------------------------------------------------------------------------

def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def load_state(path: Path) -> set[str]:
    if path.is_file():
        try:
            return set(json.loads(path.read_text()).get("seen", []))
        except json.JSONDecodeError:
            pass
    return set()


def save_state(path: Path, seen: set[str]) -> None:
    path.write_text(json.dumps({"seen": sorted(seen)[-5000:]}, indent=0))


def run_once(args, seen: set[str]) -> None:
    items = fetch_all()
    fresh = [i for i in items if i.uid not in seen]
    candidates = [(prefilter_score(i), i) for i in fresh]
    candidates = sorted((c for c in candidates if c[0] >= args.min_prefilter),
                        key=lambda c: -c[0])[: args.max_evals]
    print(f"[info] fetched {len(items)} items, {len(fresh)} new, "
          f"{len(candidates)} passed pre-filter")

    for i in fresh:
        seen.add(i.uid)

    if args.dry_run:
        for score, i in candidates:
            print(f"  [{score:>2}] {i.source}: {i.title}  {i.link}")
        return

    import anthropic
    client = anthropic.Anthropic()

    for score, item in candidates:
        try:
            ev = evaluate(client, item)
        except anthropic.RateLimitError as exc:
            wait = int(exc.response.headers.get("retry-after", "60"))
            print(f"[warn] rate limited, sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        except anthropic.APIStatusError as exc:
            print(f"[warn] API error {exc.status_code} on '{item.title}': "
                  f"{exc.message}", file=sys.stderr)
            continue
        except anthropic.APIConnectionError as exc:
            print(f"[warn] network error: {exc}", file=sys.stderr)
            continue

        verdict = "BREAKTHROUGH" if ev.is_breakthrough else "rejected"
        print(f"  [{verdict} {ev.feasibility_impact}/10] {item.title[:80]} — {ev.headline}")
        if ev.is_breakthrough and ev.feasibility_impact >= args.min_score:
            notify(format_alert(item, ev))


def main() -> int:
    load_env_file(HERE / ".env")
    p = argparse.ArgumentParser(description="Cross-chain atomicity sentinel")
    p.add_argument("--dry-run", action="store_true",
                   help="fetch + pre-filter only; no LLM calls, no alerts")
    p.add_argument("--loop", type=int, metavar="SECONDS",
                   help="run forever with this interval (e.g. 21600 = 6h)")
    p.add_argument("--min-prefilter", type=int, default=3,
                   help="min keyword score to send an item to Claude (default 3)")
    p.add_argument("--min-score", type=int, default=6,
                   help="min feasibility impact (0-10) to fire an alert (default 6)")
    p.add_argument("--max-evals", type=int, default=15,
                   help="max LLM evaluations per pass (cost cap, default 15)")
    p.add_argument("--state", type=Path, default=DEFAULT_STATE,
                   help=f"dedupe state file (default {DEFAULT_STATE})")
    p.add_argument("--test-notify", action="store_true",
                   help="send a test message to configured channels and exit")
    args = p.parse_args()

    if args.test_notify:
        ok = notify("✅ atomicity sentinel: notification channel test")
        print("test alert sent" if ok else "no channel configured — printed to stdout")
        return 0

    if not args.dry_run and not (os.getenv("ANTHROPIC_API_KEY")
                                 or os.getenv("ANTHROPIC_AUTH_TOKEN")):
        print("error: ANTHROPIC_API_KEY not set (use --dry-run to test without it)",
              file=sys.stderr)
        return 1

    seen = load_state(args.state)
    while True:
        run_once(args, seen)
        if not args.dry_run:
            save_state(args.state, seen)
        if not args.loop:
            return 0
        print(f"[info] sleeping {args.loop}s")
        time.sleep(args.loop)


if __name__ == "__main__":
    sys.exit(main())
