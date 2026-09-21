#!/usr/bin/env python3
"""Cafe data pipeline CLI.

Typical workflow (see README.md for the full picture):

    python main.py import-legacy          # reuse raw JSON from old runs
    python main.py run --limit 3          # first careful live batch
    python main.py run                    # fill everything still missing
    python main.py summary                # rebuild outputs, no network

Every command is safe to Ctrl-C and safe to re-run: outputs are merged, and
per-day request budgets persist across runs.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cafe_pipeline import __version__                     # noqa: E402
from cafe_pipeline.pipeline import (rebuild_outputs,      # noqa: E402
                                    run_import_legacy, run_scrape)
from cafe_pipeline.settings import load_settings          # noqa: E402

ROOT = Path(__file__).resolve().parent


def setup_logging(logs_dir: Path, verbose: bool) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"run_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname).1s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    return log_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Gulshan cafe data pipeline (M-1: Google Maps).")
    parser.add_argument("--version", action="version",
                        version=f"cafe_pipeline {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="scrape Google Maps for pending cafes")
    run.add_argument("--limit", type=int,
                     help="cap the number of cafes this run")
    run.add_argument("--pace", choices=["safe", "paranoid"],
                     help="override the pacing profile from config.yaml")
    run.add_argument("--headless", action="store_true",
                     help="run Chromium headless (default: headful)")
    run.add_argument("--proxy",
                     help="http://user:pass@host:port -- strongly recommended")

    sub.add_parser("import-legacy",
                   help="fold old raw runs into the store (no network)")
    sub.add_parser("summary",
                   help="rebuild outputs from the store (no network)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_settings(ROOT)
    if getattr(args, "pace", None):
        cfg.scraping["pace"] = args.pace          # settings are mutable dicts
    if getattr(args, "headless", False):
        cfg.scraping["headless"] = True
    log_path = setup_logging(cfg.logs_dir, args.verbose)
    logging.getLogger("cafe_pipeline").info(
        "cafe_pipeline %s | command=%s | log=%s", __version__, args.command,
        log_path)

    try:
        if args.command == "run":
            asyncio.run(run_scrape(cfg, limit=args.limit,
                                   proxy=getattr(args, "proxy", None)))
        elif args.command == "import-legacy":
            run_import_legacy(cfg)
        elif args.command == "summary":
            rebuild_outputs(cfg)
    except KeyboardInterrupt:
        logging.getLogger("cafe_pipeline").info(
            "\ninterrupted -- everything scraped so far is already flushed "
            "to disk; re-run to continue where this stopped")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
