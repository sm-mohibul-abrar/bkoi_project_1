"""Pipeline configuration.

All knobs (paths, pacing, budgets, scope rules, selectors) live in
``config.yaml`` next to the project root. This module loads that file,
deep-merges it over safe defaults, and exposes it through a small
attribute-access wrapper so the rest of the code reads
``cfg.scraping.batch_size`` instead of dict indexing.

Only two rules matter when editing this file or the YAML:

1. Unknown keys are a warning, not a crash -- forward compatibility for
   config files written by newer versions.
2. The defaults below are the polite-scraping defaults. The YAML can make
   them stricter but nothing in the code will make them harsher silently.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger("cafe_pipeline")

# Defaults are also the documentation of last resort. config.yaml mirrors
# this structure and overrides it.
DEFAULTS: dict[str, Any] = {
    "location": {
        "name": "Gulshan, Dhaka",
        "key": "gulshan",
        # A seed row is in scope only if its area/sub_area matches one of
        # these (case-insensitive, substring). Per the project scope:
        # Gulshan-1, Gulshan-2, Gulshan Avenue, Circle-1/2, North/South.
        "in_scope_areas": ["gulshan"],
        "in_scope_sub_areas": ["gulshan 1", "gulshan 2"],
        # Rows matching these sub_types are dropped (internet "cyber" cafes
        # are not beverage cafes).
        "exclude_sub_type_patterns": ["cyber"],
        # A seed row must mention cafe/bakery/tea/coffee/dessert in sub_type.
        "require_cafe_subtype": True,
    },
    "seed_csv": "gulshan_all_cafe.csv",
    "paths": {
        "raw_root": "data/raw/google_maps",
        "output_dir": "data/output",
        "browser_profiles": ".profiles",
        "logs": "logs",
    },
    "scraping": {
        "pace": "safe",                     # safe | paranoid (never aggressive)
        "paces": {
            # seconds between consecutive requests to the same host
            "safe": {"www.google.com": 22.0, "_default": 12.0},
            "paranoid": {"www.google.com": 45.0, "_default": 20.0},
        },
        # Hard per-host per-day ceilings, persisted across runs.
        "daily_budget": {"www.google.com": 120, "_default": 60},
        "host_jitter": 0.5,                 # +/- fraction on the interval
        "batch_size": 10,                   # cafes per browser context
        "batch_cooldown_s": [180.0, 360.0],  # pause between batches
        "breaker_threshold": 2,             # soft fails before cooldown
        "breaker_cooldown_s": 600.0,
        "nav_timeout_ms": 45_000,
        "settle_ms": 3_800,
        "headless": False,
        "geolocation": {"latitude": 23.7806, "longitude": 90.4193},
    },
    "matching": {
        "geo_tolerance_m": 1200,            # beyond this = geo_mismatch
        "reject_name_below": 0.45,          # ...combined with geo tolerance
        "high": {"name": 0.6, "dist_m": 300},
        "medium": {"name": 0.4, "dist_m": 800},
        "duplicate_radius_m": 150,          # same name within = same venue
        "duplicate_name_score": 0.8,
    },
    "reviews": {
        "max_per_platform": 5,
        "min_text_len": 25,                 # shorter snippets are UI noise
    },
    "hours": {
        "min_days": 5,                      # fewer days = not a real schedule
    },
    "retry": {
        "max_attempts": 3,                  # per cafe, across runs
    },
}


class DotDict(dict):
    """Dict with attribute access, built from nested plain dicts."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


def _wrap(value: Any) -> Any:
    if isinstance(value, dict) and not isinstance(value, DotDict):
        return DotDict({k: _wrap(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_wrap(v) for v in value]
    return value


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, value in override.items():
        if key not in out:
            log.debug("config key %r is not in defaults (kept anyway)", key)
            out[key] = value
        elif isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


class Settings:
    """Loaded configuration bound to the project root."""

    def __init__(self, data: dict[str, Any], root: Path):
        self._data = _wrap(data)
        self.root = root

    # -- typed accessors used all over the pipeline ------------------------
    @property
    def location(self) -> DotDict:
        return self._data.location

    @property
    def scraping(self) -> DotDict:
        return self._data.scraping

    @property
    def matching(self) -> DotDict:
        return self._data.matching

    @property
    def paths(self) -> DotDict:
        return self._data.paths

    @property
    def reviews(self) -> DotDict:
        return self._data.reviews

    @property
    def hours(self) -> DotDict:
        return self._data.hours

    @property
    def retry(self) -> DotDict:
        return self._data.retry

    @property
    def intervals(self) -> dict[str, float]:
        pace = self.scraping.paces[self.scraping.pace]
        return {k: float(v) for k, v in pace.items()}

    # -- filesystem helpers (config paths are relative to the project) -----
    def path(self, *parts: str) -> Path:
        return self.root.joinpath(*parts)

    @property
    def seed_csv(self) -> Path:
        return self.path(self._data.seed_csv)

    @property
    def raw_root(self) -> Path:
        return self.path(self.paths.raw_root)

    @property
    def output_dir(self) -> Path:
        return self.path(self.paths.output_dir)

    @property
    def logs_dir(self) -> Path:
        return self.path(self.paths.logs)

    @property
    def profile_root(self) -> Path:
        return self.path(self.paths.browser_profiles)

    def selectors(self, source: str) -> dict[str, list[str]]:
        """CSS selector fallback lists for a source, from config if present."""
        return self._data.get("sources", {}).get(source, {}).get("selectors", {})

    def raw(self) -> dict[str, Any]:
        """The whole configuration as a plain dict (for logging/dumps)."""
        return dict(self._data)


def load_settings(root: Path, config_name: str = "config.yaml") -> Settings:
    """Load config.yaml from *root*, deep-merged over DEFAULTS."""
    cfg_path = root / config_name
    data: dict[str, Any] = dict(DEFAULTS)
    if cfg_path.exists():
        loaded = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"{cfg_path} must contain a YAML mapping")
        data = _deep_merge(data, loaded)
    else:
        log.warning("No %s found -- running on built-in defaults", cfg_path)
    return Settings(data, root)
