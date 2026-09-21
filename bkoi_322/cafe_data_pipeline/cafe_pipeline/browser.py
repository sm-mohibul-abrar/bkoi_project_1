"""Playwright browser plumbing shared by every source module.

Launches one persistent Chromium context per batch (cookies survive between
batches, which keeps consent walls rare), applies the HostGuard to every
navigation, and offers tolerant selector helpers.
"""

from __future__ import annotations

import logging
import random
import re
from typing import Any, Iterable
from urllib.parse import urlparse

from .pacing import HostGuard, detect_block
from .settings import Settings

log = logging.getLogger("cafe_pipeline")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
VIEWPORT = {"width": 1440, "height": 900}
_IP_ECHO = "https://api.ipify.org?format=json"


def proxy_config(proxy: str | None) -> dict[str, str] | None:
    if not proxy:
        return None
    parsed = urlparse(proxy)
    config = {"server": f"{parsed.scheme}://{parsed.hostname}"
              + (f":{parsed.port}" if parsed.port else "")}
    if parsed.username:
        config["username"] = parsed.username
    if parsed.password:
        config["password"] = parsed.password
    return config


async def launch_context(pw, cfg: Settings, profile_name: str,
                         proxy: str | None = None):
    """One persistent Chromium context, Dhaka-localised."""
    profile_dir = cfg.profile_root / profile_name
    profile_dir.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = dict(
        user_data_dir=str(profile_dir),
        headless=bool(cfg.scraping.headless),
        user_agent=USER_AGENT,
        viewport=VIEWPORT,
        locale="en-US",
        timezone_id="Asia/Dhaka",
        geolocation=dict(cfg.scraping.geolocation),
        permissions=["geolocation"],
        args=["--disable-dev-shm-usage", "--no-first-run",
              "--no-default-browser-check", "--lang=en-US"],
    )
    if proxy:
        kwargs["proxy"] = proxy_config(proxy)
    context = await pw.chromium.launch_persistent_context(**kwargs)
    context.set_default_timeout(15_000)
    context.set_default_navigation_timeout(int(cfg.scraping.nav_timeout_ms))

    async def _abort(route):
        try:
            await route.abort()
        except Exception:                          # noqa: BLE001
            pass

    # Fonts and media only cost bandwidth; blocking them looks like a normal
    # slow connection and saves nothing we need.
    await context.route(
        re.compile(r"\.(woff2?|ttf|otf|eot|mp4|webm|avi)(\?|$)"), _abort)
    return context


async def goto(page, url: str, host: str, guard: HostGuard,
               cfg: Settings) -> str | None:
    """Navigate with guard pacing; returns visible body text or None."""
    available, why = guard.available(host)
    if not available:
        log.info("  skip %s (%s)", host, why)
        return None
    await guard.acquire(host)
    try:
        guard.budget.spend(host)
        response = await page.goto(url, wait_until="domcontentloaded")
        if response is not None and response.status in (403, 429, 503):
            if response.status == 429:
                guard.hard_block(host, "http-429")
            else:
                guard.soft_fail(host, f"http-{response.status}")
            return None
        await page.wait_for_timeout(
            int(cfg.scraping.settle_ms) + random.randint(0, 2200))
        try:
            body = await page.inner_text("body", timeout=9000)
        except Exception:                          # noqa: BLE001
            body = await page.content()
        reason, hard = detect_block(page.url, body)
        if reason:
            (guard.hard_block if hard else guard.soft_fail)(host, reason)
            return None
        guard.ok(host)
        return body
    except Exception as exc:                       # noqa: BLE001
        guard.soft_fail(host, type(exc).__name__)
        log.warning("  %s -> %s: %s", host, type(exc).__name__, str(exc)[:110])
        return None
    finally:
        guard.release(host)


async def first_text(page, selectors: Iterable[str],
                     min_len: int = 1) -> str | None:
    """First selector that exists and yields non-trivial text."""
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count():
                text = (await locator.inner_text(timeout=2500)).strip()
                if len(text) >= min_len:
                    return text
        except Exception:                          # noqa: BLE001
            continue
    return None


async def first_attr(page, selectors: Iterable[str],
                     attr: str) -> str | None:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count():
                value = await locator.get_attribute(attr, timeout=2500)
                if value:
                    return value
        except Exception:                          # noqa: BLE001
            continue
    return None


async def click_first(page, selectors: Iterable[str],
                      settle_ms: int = 1500) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count():
                await locator.click(timeout=4000)
                await page.wait_for_timeout(settle_ms)
                return True
        except Exception:                          # noqa: BLE001
            continue
    return False


async def scroll_panel(page, selectors: Iterable[str], steps: int = 5) -> None:
    """Scroll a scrollable pane (results feed, reviews list) in human-sized
    increments so lazy content renders."""
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if not await locator.count():
                continue
            for _ in range(steps):
                await locator.evaluate(
                    "el => el.scrollBy(0, el.clientHeight * 0.85)")
                await page.wait_for_timeout(random.randint(500, 1200))
            return
        except Exception:                          # noqa: BLE001
            continue
