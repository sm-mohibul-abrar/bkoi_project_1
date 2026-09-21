# Gulshan Cafe Data Pipeline

Turns the Barikoi places export for Gulshan (Dhaka) plus live platform data
into one clean, platform-tagged record per cafe. This repository implements
**M-1 (Google Maps)**; the package layout leaves room for M-2..M-5
(delivery apps, social media, venue websites, BD restaurant sites).

## Quick start

```bash
pip install -r requirements.txt
playwright install chromium          # once

python main.py import-legacy         # fold old raw runs into the store (offline)
python main.py summary               # rebuild outputs (offline)
python main.py run --limit 3         # first careful live batch
python main.py run                   # keep filling what is missing
```

Run `python main.py run` daily until the summary stops improving. That is
the intended workflow: every run is incremental.

## What each command does

| command | network | effect |
|---|---|---|
| `import-legacy` | none | normalises `data/raw/google_maps/run_*/` (old scraper output) into the store |
| `summary` | none | rebuilds JSON/CSV/summary from the store |
| `run` | Google Maps | scrapes every cafe still missing an accepted M-1 result |

Useful flags: `--limit N` (cap cafes this run), `--pace paranoid` (double
intervals), `--proxy http://user:pass@host:port`, `-v` (debug logging).
`--headless` exists for machines with no display, but prefer the default
headful mode: in headless Chromium Google serves a degraded Maps page where
the hours table never expands.

## Outputs (all under `data/output/`)

* `gulshan_cafes.json` — the canonical store: every record plus the
  completeness `summary` at the end. One record per cafe, per the agreed
  schema: platform-tagged `menu`, `rating`, `review_count`, `reviews`,
  `hours`; secondary contact fields; `conflict`; `missing`.
* `gulshan_cafes.csv` — a flattened view of the same records (hours get one
  column per day, `Sat`..`Fri`; reviews are a JSON column). Rewritten from
  the store on every flush, merged by `cafe_id`, so re-runs add to it
  rather than reset it.
* `gulshan_state.json` — the stable `GUL-###` id assignment.
* `gulshan_budget.json` — the per-day request ledger (IP protection).
* Raw payloads per run are archived under `data/raw/google_maps/run_*/`.

## Conventions

* **Week**: Saturday → Friday (`Sat, Sun, Mon, Tue, Wed, Thu, Fri`).
* **Hours**: 24-hour `HH:MM`. `"closed"` marks a closed day;
  `"00:00-24:00"` means open 24 hours; an end earlier than the start
  (`"22:00-01:00"`) means closing after midnight. Split shifts are
  comma-joined. Dine-in and delivery hours are kept separate (`type`).
* **Tagging**: every priority value carries `platform` (`GOOGLE_MAPS`) and
  `source_tag` (`M-1`) and, where known, `as_of`. Values are never merged,
  averaged or cross-filled between platforms. When M-2+ lands, disagreeing
  values will coexist and `conflict` flips to `true`.
* **Privacy**: reviewer names, review images and any personal data are
  dropped at ingestion; only stars, text and date survive.
* **Missing data is `null`** — never estimated, never guessed.
* **Reviews**: at most 5 most-recent per platform.
* **Review counts**: `"1.2K"` imports as `1200` with `approximate: true`.

## Interrupt safety & multi-run behaviour

* The store is flushed after **every cafe** with atomic writes (tmp +
  rename, previous file kept as `.bak`). Ctrl-C loses at most the cafe in
  flight — re-run continues from where it stopped.
* A refresh only replaces fields it actually captured; a failed extraction
  never erases a previously captured value. Re-runs therefore accumulate
  coverage, which is why "run it again tomorrow" fixes partial data.
* Cafes whose Google match failed (`not_found`, `geo_mismatch`,
  `blocked_or_error`) are retried on later runs until `retry.max_attempts`.
* Legacy-imported records keep status `legacy` and stay in the queue: a
  live scrape always supersedes them.

## IP-safety policy (no aggressive scraping)

1. Minimum interval between Google requests (default 22 s ± jitter;
   `paranoid` = 45 s). There is no "aggressive" mode on purpose.
2. Hard daily ceiling per host (default 120/day), persisted across runs —
   a crashed run's re-run cannot double the day's load.
3. Batches of 10 with a 3–6 minute cooldown between batches.
4. Abort-on-block: the first hard block signal (CAPTCHA, `/sorry/`, HTTP
   429) disables the host for the rest of the run. The script never
   retries into a wall — that is what gets IPs flagged.
5. A persistent browser profile (cookies survive) reduces consent walls,
   and geolocation/timezone are pinned to Dhaka.
6. `--proxy` supported; using one you trust is the only real protection
   for your own IP.

## Scope rules (config.yaml)

In scope: `Gulshan 1`, `Gulshan 2`, `Gulshan` (avenue/circle/north/south).
Everything else — Tejgaon, Nikunja, Niketon, Banani-adjacent rows, cyber
cafes, non-cafe subtypes, venues marked permanently closed on Google — is
kept out of the main list and named under `summary.excluded` with a reason.
Duplicate seed rows (same name within 150 m) are fetched once; each alias
still gets its own record marked `duplicate_of`.

## Layout

```
main.py                  CLI entrypoint
config.yaml              every knob: scope, pacing, budgets, selectors
cafe_pipeline/
  settings.py            config loading (YAML over safe defaults)
  seed.py                Barikoi CSV -> scoped, deduplicated candidates
  models.py              CafeRecord + summary (the output schema)
  hours.py               Sat-Fri / 24-hour normalisation
  utils.py               parsing + atomic IO helpers
  pacing.py              Budget + HostGuard + block detection
  browser.py             Playwright context/navigation helpers
  store.py               interrupt-safe incremental JSON/CSV store
  legacy_import.py       old raw-run normaliser
  pipeline.py            orchestration (batches, flush-per-cafe)
  sources/google_maps.py M-1 scraper
tests/                   pytest suite (offline logic only)
data/raw/google_maps/    archived raw payloads per run
data/output/             the store and exports
```

Selector maintenance: when Google reshuffles its DOM, override any
selector list under `sources.google_maps.selectors` in `config.yaml` — no
code change needed.

## Tests

```bash
python -m pytest tests/ -q
```
