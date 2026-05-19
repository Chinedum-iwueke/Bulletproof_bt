#!/usr/bin/env python3
"""CLI for the Strategy Research Terminal read-only command layer."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestrator.research_terminal import terminal


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strategy Research Terminal commands.")
    parser.add_argument("--db", default="research_db/research.sqlite", help="Research SQLite DB path.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("research-status")
    queue = sub.add_parser("queue-status")
    queue.add_argument("--queue-name", default=None)

    verdict = sub.add_parser("latest-verdict")
    verdict.add_argument("--hypothesis", required=True)

    compare = sub.add_parser("compare")
    compare.add_argument("--hypothesis", default=None)
    compare.add_argument("--phase", default=None)

    failure = sub.add_parser("explain-failure")
    failure.add_argument("--hypothesis", default=None)

    state = sub.add_parser("state-summary")
    state.add_argument("--cards-json", required=True)

    similar = sub.add_parser("similar-runs")
    similar.add_argument("--card-type", default=None)
    similar.add_argument("--limit", type=int, default=10)
    similar.add_argument("--memory-db", default=None, help="Research memory SQLite DB for state similarity lookup.")
    similar.add_argument("--state-json", default=None, help="State JSON for similar historical lookup.")

    rec = sub.add_parser("recommendations")
    rec.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "research-status":
        payload = terminal.research_status(args.db)
    elif args.command == "queue-status":
        payload = terminal.queue_status(args.db, queue_name=args.queue_name)
    elif args.command == "latest-verdict":
        payload = terminal.latest_hypothesis_verdict(args.db, hypothesis_name=args.hypothesis)
    elif args.command == "compare":
        payload = terminal.experiment_comparison(args.db, hypothesis_name=args.hypothesis, phase=args.phase)
    elif args.command == "explain-failure":
        payload = terminal.failure_explanation(args.db, hypothesis_name=args.hypothesis)
    elif args.command == "state-summary":
        payload = terminal.state_bucket_summary(args.cards_json)
    elif args.command == "similar-runs":
        if args.memory_db or args.state_json:
            payload = terminal.similar_historical_run_lookup(
                args.memory_db or args.db,
                state_json=args.state_json,
            )
        else:
            payload = terminal.similar_historical_runs(args.db, card_type=args.card_type, limit=args.limit)
    elif args.command == "recommendations":
        payload = terminal.recommendation_summary(args.db, limit=args.limit)
    else:  # pragma: no cover
        raise ValueError(args.command)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
