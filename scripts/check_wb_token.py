"""Smoke diagnostics for WB browser-refresh proxy bundles."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (  # noqa: E402
    WB_PROXY_BUNDLES,
    WB_PROXY_URL,
    WB_TOKEN_COOKIE_NAME,
    WB_TOKEN_REFRESH_MAX_ATTEMPTS,
    WB_TOKEN_REFRESH_URL,
    WB_TOKEN_REFRESH_WAIT_SECONDS,
    WB_USER_AGENT,
    WBProxyBundle,
)
from src.wb_token_provider import WbTokenProvider  # noqa: E402


@dataclass(frozen=True)
class ProxySmokeResult:
    """Compact summary for one browser-refresh smoke attempt."""

    label: str
    proxy_host: str
    ok: bool
    cookies_count: int
    token_present: bool
    document_cookie_len: int
    page_source_len: int
    ready_state: str
    page_hint: str
    final_url: str
    title: str
    error: str


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for proxy/browser smoke diagnostics."""
    parser = argparse.ArgumentParser(
        description="Check which WB proxy bundles can build a browser cookie context.",
    )
    parser.add_argument(
        "--all-bundles",
        action="store_true",
        help="Check all WB_PROXY_XX_* bundles from the current environment.",
    )
    parser.add_argument(
        "--proxy-url",
        default="",
        help="Optional single proxy URL. Falls back to WB_PROXY_URL when provided.",
    )
    parser.add_argument(
        "--label",
        default="single_proxy",
        help="Label for the single-proxy smoke result.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print results as JSON in addition to human-readable logs.",
    )
    return parser.parse_args()


def extract_proxy_host(proxy_url: str) -> str:
    """Return host:port without credentials for safe logs."""
    sanitized = proxy_url.strip()
    if not sanitized:
        return "-"

    parts = sanitized.split("://", maxsplit=1)
    raw = parts[1] if len(parts) == 2 else parts[0]
    if "@" in raw:
        raw = raw.split("@", maxsplit=1)[1]
    if raw.count(":") >= 3:
        tokens = raw.split(":")
        return f"{tokens[0]}:{tokens[1]}"
    return raw


def build_provider(*, proxy_url: str) -> WbTokenProvider:
    """Create a WB token provider for one proxy/browser smoke run."""
    return WbTokenProvider(
        user_agent=WB_USER_AGENT,
        url=WB_TOKEN_REFRESH_URL,
        cookie_name=WB_TOKEN_COOKIE_NAME,
        max_attempts=WB_TOKEN_REFRESH_MAX_ATTEMPTS,
        wait_seconds=WB_TOKEN_REFRESH_WAIT_SECONDS,
        proxy=proxy_url or None,
    )


def collect_targets(args: argparse.Namespace) -> list[tuple[str, str]]:
    """Build the list of `(label, proxy_url)` smoke targets."""
    if args.all_bundles:
        targets = [
            (bundle.label, bundle.proxy_url)
            for bundle in WB_PROXY_BUNDLES
            if bundle.proxy_url.strip()
        ]
        if targets:
            return targets

    proxy_url = (args.proxy_url or WB_PROXY_URL).strip()
    if proxy_url:
        return [(args.label, proxy_url)]

    raise ValueError(
        "No proxy targets found. Use --all-bundles or pass --proxy-url / WB_PROXY_URL.",
    )


def run_smoke(label: str, proxy_url: str) -> ProxySmokeResult:
    """Run one browser-refresh smoke check through the provider."""
    provider = build_provider(proxy_url=proxy_url)
    logger.info(
        "Start WB proxy smoke: label={} proxy_host={}",
        label,
        extract_proxy_host(proxy_url),
    )

    cookie_string = provider.get_cookie_string()
    diagnostics = provider.get_last_diagnostics()
    cookies_count = int(diagnostics.get("cookies_count", 0) or 0)
    token_present = bool(diagnostics.get("token_present", False))
    document_cookie_len = int(diagnostics.get("document_cookie_len", 0) or 0)
    page_source_len = int(diagnostics.get("page_source_len", 0) or 0)
    ready_state = str(diagnostics.get("ready_state", "") or "")
    page_hint = str(diagnostics.get("page_hint", "") or "")
    final_url = str(diagnostics.get("final_url", "") or "")
    title = str(diagnostics.get("title", "") or "")
    error = str(diagnostics.get("error", "") or "")

    ok = bool(cookie_string) and cookies_count > 0 and document_cookie_len > 0
    return ProxySmokeResult(
        label=label,
        proxy_host=extract_proxy_host(proxy_url),
        ok=ok,
        cookies_count=cookies_count,
        token_present=token_present,
        document_cookie_len=document_cookie_len,
        page_source_len=page_source_len,
        ready_state=ready_state,
        page_hint=page_hint,
        final_url=final_url,
        title=title[:120],
        error=error,
    )


def log_results(results: Iterable[ProxySmokeResult], *, as_json: bool) -> None:
    """Print a compact summary for each proxy smoke result."""
    result_list = list(results)
    for result in result_list:
        logger.info(
            "WB proxy smoke result: label={} proxy_host={} ok={} cookies_count={} token_present={} document_cookie_len={} page_source_len={} ready_state={} page_hint={} final_url={} error={}",
            result.label,
            result.proxy_host,
            result.ok,
            result.cookies_count,
            result.token_present,
            result.document_cookie_len,
            result.page_source_len,
            result.ready_state or "-",
            result.page_hint or "-",
            result.final_url or "-",
            result.error or "-",
        )

    if as_json:
        payload = [asdict(item) for item in result_list]
        print(json.dumps(payload, ensure_ascii=False, indent=2))


def main() -> None:
    """CLI entrypoint for proxy/browser smoke diagnostics."""
    args = parse_args()
    targets = collect_targets(args)
    results = [run_smoke(label, proxy_url) for label, proxy_url in targets]
    log_results(results, as_json=args.json)

    ok_count = sum(1 for item in results if item.ok)
    logger.info("WB proxy smoke summary: total={} ok={}", len(results), ok_count)


if __name__ == "__main__":
    main()
