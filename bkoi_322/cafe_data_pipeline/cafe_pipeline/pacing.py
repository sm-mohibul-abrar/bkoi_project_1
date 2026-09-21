"""Request pacing, daily budgets and block detection -- the IP-safety layer.

No scraper can promise an IP is never blocked; this module is what keeps the
promise *cheap* to keep:

1. A minimum interval (with jitter) between consecutive requests per host.
2. Hard per-host, per-day request ceilings persisted across runs, so a
   re-run can never double the day's load after a crash.
3. A soft-failure breaker: repeated failures cool the host down.
4. Abort-on-block: the first hard block signal (CAPTCHA, /sorry/, 429)
   disables the host for the rest of the run. Retrying into a wall is what
   actually gets IPs flagged, so we never do it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from pathlib import Path

from .settings import Settings
from .utils import atomic_write_json, now_iso, today_key

log = logging.getLogger("cafe_pipeline")

BLOCK_SIGNALS_HARD = [
    "unusual traffic", "not a robot", "our systems have detected",
    "captcha", "access denied", "pardon our interruption",
    "verify you are a human", "too many requests",
]
BLOCK_SIGNALS_SOFT = ["enable javascript and cookies to continue", "rate limit"]


class Budget:
    """Persisted per-day request ledger, one entry per host."""

    def __init__(self, path: Path, caps: dict[str, int]):
        self.path = path
        self.caps = caps
        self.data: dict[str, dict[str, int]] = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                log.warning("budget file unreadable -- starting a fresh ledger")
        self.day = self.data.setdefault(today_key(), {})
        for stale in sorted(self.data)[:-14]:      # keep two weeks of history
            self.data.pop(stale, None)

    def cap(self, host: str) -> int:
        return self.caps.get(host, self.caps["_default"])

    def used(self, host: str) -> int:
        return self.day.get(host, 0)

    def remaining(self, host: str) -> int:
        return max(0, self.cap(host) - self.used(host))

    def can_spend(self, host: str) -> bool:
        return self.remaining(host) > 0

    def spend(self, host: str) -> None:
        self.day[host] = self.used(host) + 1

    def flush(self) -> None:
        atomic_write_json(self.path, self.data)

    def report(self) -> str:
        if not self.day:
            return "nothing spent yet"
        return " | ".join(f"{host}:{self.used(host)}/{self.cap(host)}"
                          for host in sorted(self.day))


class HostGuard:
    """Paces requests to one host and watches for block signals."""

    def __init__(self, intervals: dict[str, float], budget: Budget,
                 jitter: float, breaker_threshold: int,
                 breaker_cooldown_s: float):
        self.intervals = intervals
        self.budget = budget
        self.jitter = jitter
        self.breaker_threshold = breaker_threshold
        self.breaker_cooldown_s = breaker_cooldown_s
        self._last: dict[str, float] = {}
        self._fails: dict[str, int] = {}
        self._cooldown_until: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self.disabled: dict[str, str] = {}        # host -> reason, whole run
        self.events: list[dict[str, str]] = []

    @classmethod
    def from_settings(cls, cfg: Settings, budget: Budget) -> "HostGuard":
        return cls(
            intervals=cfg.intervals,
            budget=budget,
            jitter=float(cfg.scraping.host_jitter),
            breaker_threshold=int(cfg.scraping.breaker_threshold),
            breaker_cooldown_s=float(cfg.scraping.breaker_cooldown_s),
        )

    def _lock(self, host: str) -> asyncio.Lock:
        return self._locks.setdefault(host, asyncio.Lock())

    def available(self, host: str) -> tuple[bool, str]:
        if host in self.disabled:
            return False, f"disabled:{self.disabled[host]}"
        if not self.budget.can_spend(host):
            return False, f"budget-exhausted({self.budget.cap(host)}/day)"
        until = self._cooldown_until.get(host, 0.0)
        if until and time.monotonic() < until:
            return False, f"cooling {until - time.monotonic():.0f}s"
        if until:
            self._cooldown_until.pop(host, None)
            self._fails[host] = 0
        return True, "ok"

    async def acquire(self, host: str) -> None:
        """Serialise per host and sleep out the configured interval."""
        await self._lock(host).acquire()
        interval = self.intervals.get(host, self.intervals["_default"])
        interval *= random.uniform(1 - self.jitter, 1 + self.jitter)
        gap = time.monotonic() - self._last.get(host, 0.0)
        if gap < interval:
            await asyncio.sleep(interval - gap)

    def release(self, host: str) -> None:
        self._last[host] = time.monotonic()
        lock = self._locks.get(host)
        if lock and lock.locked():
            lock.release()

    def ok(self, host: str) -> None:
        self._fails[host] = 0

    def soft_fail(self, host: str, reason: str) -> None:
        count = self._fails.get(host, 0) + 1
        self._fails[host] = count
        if count >= self.breaker_threshold:
            self._cooldown_until[host] = time.monotonic() + self.breaker_cooldown_s
            self.events.append({"host": host, "kind": "cooldown",
                                "reason": reason, "at": now_iso()})
            log.warning("cooling %s for %.0fs after %d failures (%s)",
                        host, self.breaker_cooldown_s, count, reason)

    def hard_block(self, host: str, reason: str) -> None:
        """A real block signal: stop touching this host for the entire run."""
        self.disabled[host] = reason
        self.events.append({"host": host, "kind": "hard_block",
                            "reason": reason, "at": now_iso()})
        log.error("BLOCK DETECTED on %s (%s). Disabling for the rest of this "
                  "run -- do not re-run against this host today.",
                  host, reason)


def detect_block(url: str, body: str) -> tuple[str | None, bool]:
    """Returns (reason, is_hard). Hard blocks disable the host for the run."""
    lowered_url = (url or "").lower()
    if "/sorry/" in lowered_url or "consent.google" in lowered_url \
            or "captcha" in lowered_url:
        return f"block-url:{lowered_url[:60]}", True
    lowered_body = (body or "")[:6000].lower()
    for signal in BLOCK_SIGNALS_HARD:
        if signal in lowered_body:
            return f"block-text:{signal}", True
    for signal in BLOCK_SIGNALS_SOFT:
        if signal in lowered_body:
            return f"soft:{signal}", False
    if len((body or "").strip()) < 120:
        return "empty-body", False
    return None, False
