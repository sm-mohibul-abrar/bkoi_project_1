"""Orchestration: seed -> (legacy import) -> Google Maps batches -> outputs.

The loop is built around the two operational rules of the project:

* nothing is lost on Ctrl-C: every cafe is followed by a store flush and a
  budget flush, and raw payloads are archived per cafe immediately;
* re-runs only work on what is still missing: cafes whose Google scrape
  already succeeded (or exhausted its attempts) are skipped.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Any

from . import SOURCE_GOOGLE_MAPS
from .browser import launch_context
from .legacy_import import load_legacy_payloads
from .pacing import Budget, HostGuard
from .seed import load_seed
from .settings import Settings
from .sources.google_maps import HOST, scrape_google_maps
from .store import CafeStore
from .utils import atomic_write_json, now_iso

log = logging.getLogger("cafe_pipeline.run")

# Statuses a later run should try again. "legacy" data is a stopgap: a live
# scrape is always better, so legacy-only records stay in the queue.
RETRYABLE = {"blocked_or_error", "not_found", "geo_mismatch", "no_match",
             "legacy", "pending"}


def _raw_run_dir(cfg: Settings) -> Path:
    run_dir = cfg.raw_root / f"run_{datetime.now():%Y%m%d_%H%M%S}"
    (run_dir / "profiles").mkdir(parents=True, exist_ok=True)
    return run_dir


def _dump_raw(run_dir: Path, seed: dict[str, Any], outcome,
              confidence: str | None) -> Path:
    slug = "".join(c if c.isalnum() else "_" for c in seed["business_name"])
    path = run_dir / "profiles" / f"{seed['place_code']}_{slug[:40]}.json"
    atomic_write_json(path, {
        "place_code": seed["place_code"],
        "business_name": seed["business_name"],
        "scraped_at": now_iso(),
        "status": outcome.status,
        "match_confidence": confidence,
        "url": outcome.url,
        "payload": outcome.payload,
    })
    return path


def _apply_outcome(store: CafeStore, seed: dict[str, Any], outcome,
                   confidence: str | None) -> None:
    record = store.get_or_create(seed)
    record.attempts += 1
    record.apply_google_payload(outcome.payload, confidence or "low",
                                outcome.status)
    store.upsert(record)

    # Duplicates share the leader's harvest; each is still its own record.
    for alias in seed.get("aliases", []):
        alias_record = store.get_or_create(alias)
        alias_record.attempts += 1
        leader_id = store.cafe_id(seed["place_code"])
        alias_record.duplicate_of = leader_id
        alias_record.apply_google_payload(outcome.payload,
                                          confidence or "low",
                                          outcome.status)
        store.upsert(alias_record)


def _materialize_all(store: CafeStore, leaders: list[dict[str, Any]],
                     excluded: list[dict[str, str]]) -> None:
    """Ensure every in-scope and excluded row has a record in the store."""
    store.ensure_ids(leaders)
    store.materialize(leaders)
    for row in excluded:
        seed_like = {"place_code": row["place_code"],
                     "business_name": row["name"]}
        store.ensure_ids([seed_like])
        record = store.get_or_create(seed_like)
        record.scrape_status = "out_of_scope"
        store.upsert(record)


def _harvest_complete(record) -> bool:
    """An ok scrape counts as done only when it captured every priority
    field M-1 can provide; partial harvests go back in the queue (bounded
    by retry.max_attempts) so later runs fill the gaps."""
    return bool(record.rating and record.review_count and record.reviews
                and record.hours)


def _pending(store: CafeStore, leaders: list[dict[str, Any]],
             cfg: Settings, limit: int | None) -> list[dict[str, Any]]:
    max_attempts = int(cfg.retry.max_attempts)
    pending = []
    for seed in leaders:
        record = store.records.get(seed["place_code"])
        if record is not None:
            if record.scrape_status == "ok":
                if _harvest_complete(record) or record.attempts >= max_attempts:
                    continue
            elif (record.scrape_status not in RETRYABLE
                  or record.attempts >= max_attempts):
                continue
        pending.append(seed)
    return pending[:limit] if limit else pending


async def run_scrape(cfg: Settings, limit: int | None = None,
                     proxy: str | None = None) -> None:
    from playwright.async_api import async_playwright

    leaders, excluded = load_seed(cfg.seed_csv, cfg)
    log.info("%d venues to consider, %d excluded by scope", len(leaders),
             len(excluded))
    if not leaders:
        log.error("nothing in scope -- check the location config")
        return

    store = CafeStore(cfg)
    _materialize_all(store, leaders, excluded)
    store.flush()

    todo = _pending(store, leaders, cfg, limit)
    log.info("%d cafes need Google Maps (%d already done or capped)",
             len(todo), len(leaders) - len(todo))
    if not todo:
        _print_summary(store)
        return

    budget = Budget(cfg.output_dir / f"{cfg.location.key}_budget.json",
                    {k: int(v) for k, v in cfg.scraping.daily_budget.items()})
    guard = HostGuard.from_settings(cfg, budget)
    run_dir = _raw_run_dir(cfg)
    batch_size = int(cfg.scraping.batch_size)
    batches = [todo[i:i + batch_size] for i in range(0, len(todo), batch_size)]
    log.info("pace=%s batch=%d batches=%d budget: %s", cfg.scraping.pace,
             batch_size, len(batches), budget.report())

    async with async_playwright() as pw:
        for number, batch in enumerate(batches, 1):
            if HOST in guard.disabled:
                log.error("Google is blocked for this run -- stopping early "
                          "to protect the IP. Re-run tomorrow.")
                break
            log.info("=" * 68)
            log.info("BATCH %d/%d (%d cafes) | budget: %s", number,
                     len(batches), len(batch), budget.report())
            context = await launch_context(pw, cfg,
                                           f"{cfg.location.key}_{number % 4}",
                                           proxy)
            try:
                page = await context.new_page()
                for i, seed in enumerate(batch, 1):
                    if HOST in guard.disabled:
                        log.error("block detected mid-batch -- stopping")
                        break
                    log.info("[%d/%d] %s (%s)", i, len(batch),
                             seed["business_name"], seed["place_code"])
                    try:
                        outcome = await scrape_google_maps(page, seed, guard,
                                                           cfg)
                    except Exception as exc:      # noqa: BLE001
                        log.error("  fatal: %s: %s", type(exc).__name__,
                                  str(exc)[:120])
                        continue
                    _dump_raw(run_dir, seed, outcome, outcome.confidence)
                    _apply_outcome(store, seed, outcome, outcome.confidence)
                    store.flush()
                    budget.flush()
                    record = store.records[seed["place_code"]]
                    log.info("  -> %s | rating=%s hours=%dd reviews=%d",
                             record.scrape_status,
                             record.rating[0]["value"] if record.rating
                             else None,
                             len(record.hours[0]["schedule"])
                             if record.hours else 0,
                             len(record.reviews))
                    await asyncio.sleep(random.uniform(3.0, 7.0))
            finally:
                try:
                    await context.close()
                except Exception:                  # noqa: BLE001
                    pass
            if number < len(batches) and HOST not in guard.disabled:
                cooldown = random.uniform(*cfg.scraping.batch_cooldown_s)
                log.info("batch done -- cooling %.0fs", cooldown)
                await asyncio.sleep(cooldown)

    budget.flush()
    _print_summary(store)


def run_import_legacy(cfg: Settings) -> None:
    """Fold earlier raw runs into the store without touching the network."""
    leaders, excluded = load_seed(cfg.seed_csv, cfg)
    payloads = load_legacy_payloads(cfg.raw_root)

    store = CafeStore(cfg)
    _materialize_all(store, leaders, excluded)
    matched = 0
    for seed in leaders:
        payload = payloads.get(seed["place_code"])
        if not payload:
            continue
        matched += 1
        record = store.get_or_create(seed)
        # Legacy data fills gaps only: a live scrape result always wins.
        if record.scrape_status != "ok":
            record.apply_google_payload(payload, "medium", "legacy")
        store.upsert(record)
        for alias in seed.get("aliases", []):
            alias_record = store.get_or_create(alias)
            if alias_record.scrape_status != "ok":
                alias_record.apply_google_payload(payload, "medium", "legacy")
                alias_record.duplicate_of = store.cafe_id(seed["place_code"])
            store.upsert(alias_record)
    store.flush()
    log.info("legacy import: %d/%d venues enriched", matched, len(leaders))
    _print_summary(store)


def rebuild_outputs(cfg: Settings) -> None:
    """Rebuild JSON/CSV/summary from the existing store (no network)."""
    leaders, excluded = load_seed(cfg.seed_csv, cfg)
    store = CafeStore(cfg)
    _materialize_all(store, leaders, excluded)
    store.flush()
    _print_summary(store)


def _print_summary(store: CafeStore) -> None:
    summary = store.summary()
    counts = summary["counts"]
    print("\n" + "-" * 72)
    print(f"records {summary['total_cafes']} live / "
          f"{counts['records_total']} total | with_rating {counts['with_rating']} | "
          f"with_hours {counts['with_hours']} | with_reviews {counts['with_reviews']}")
    print(f"permanently closed {counts['permanently_closed']} | "
          f"duplicates {counts['duplicates']} | "
          f"not scraped yet {len(summary['not_scraped_yet'])}")
    print("-" * 72)
    print(f"JSON {store.json_path}")
    print(f"CSV  {store.csv_path}")
