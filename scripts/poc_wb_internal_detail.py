"""POC for comparing Wildberries detail endpoints on a small article sample.

This script is intentionally isolated from the production pipeline:
- does not touch ClickHouse;
- does not touch checkpoint;
- does not reuse production circuit breaker;
- does not perform mass batch processing.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.wb_service import WildberriesService


LOGGER = logging.getLogger("poc_wb_internal_detail")

DEFAULT_SAMPLE_DELAY = 0.35
DEFAULT_TIMEOUT_SECONDS = 15

OLD_ENDPOINT = "https://card.wb.ru/cards/v4/detail"
INTERNAL_ENDPOINT = "https://www.wildberries.ru/__internal/card/cards/v4/detail"

PROJECT_PARAMS = {
    "appType": 1,
    "curr": "rub",
    "dest": "-1257786",
    "spp": 30,
    "hide_vflags": 4294967296,
    "ab_testing": "false",
    "lang": "ru",
}

INTERNAL_PARAMS = {
    "appType": 1,
    "curr": "rub",
    "dest": "-446082",
    "hide_dtype": 13,
    "spp": 30,
    "ab_testing": "false",
    "lang": "ru",
}

MOBILE_CHROME_UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Mobile Safari/537.36"
)

DESKTOP_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class SecretContext:
    authorization: str | None
    cookie: str | None
    device_id: str | None

    @property
    def authorization_present(self) -> bool:
        return bool(self.authorization)

    @property
    def cookie_present(self) -> bool:
        return bool(self.cookie)

    @property
    def device_id_present(self) -> bool:
        return bool(self.device_id)


@dataclass(frozen=True)
class RequestProfile:
    name: str
    description: str

    def build_headers(self, article_id: int, secrets: SecretContext) -> dict[str, str]:
        referer = f"https://www.wildberries.ru/catalog/{article_id}/detail.aspx"
        if self.name == "A":
            return {
                "Accept": "*/*",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": referer,
                "User-Agent": MOBILE_CHROME_UA,
            }

        headers: dict[str, str] = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": referer,
            "Sec-CH-UA": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": DESKTOP_CHROME_UA,
        }

        if secrets.device_id_present:
            headers["deviceid"] = str(secrets.device_id)
        if self.name == "C":
            if secrets.authorization_present:
                headers["Authorization"] = str(secrets.authorization)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
        return headers


@dataclass(frozen=True)
class EndpointVariant:
    name: str
    endpoint: str
    params_template: dict[str, Any]

    def build_params(self, article_id: int) -> dict[str, Any]:
        params = dict(self.params_template)
        params["nm"] = article_id
        return params


@dataclass
class ProbeResult:
    profile: str
    endpoint: str
    nm: int
    status_code: int | None
    has_products: bool
    has_sizes: bool
    has_price: bool
    price_product: int | None
    price_rub: float | None
    total_qty: int | None
    has_stocks: bool
    response_size: int
    x_pow: str
    parser_price_found: bool
    error: str


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="POC for WB internal detail endpoint.")
    parser.add_argument(
        "--articles",
        type=str,
        default=os.getenv("POC_WB_ARTICLES", ""),
        help="Comma-separated article IDs. Can also be provided via POC_WB_ARTICLES.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Optional limit for the parsed article list.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_SAMPLE_DELAY,
        help="Delay between requests in seconds.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def parse_articles(raw_articles: str, limit: int) -> list[int]:
    if not raw_articles.strip():
        raise ValueError("Articles are required. Use --articles or POC_WB_ARTICLES.")

    articles: list[int] = []
    for token in raw_articles.split(","):
        stripped = token.strip()
        if not stripped:
            continue
        articles.append(int(stripped))
    if limit > 0:
        return articles[:limit]
    return articles


def load_local_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)


def load_secrets() -> SecretContext:
    return SecretContext(
        authorization=os.getenv("WB_AUTHORIZATION"),
        cookie=os.getenv("WB_COOKIE"),
        device_id=os.getenv("WB_DEVICE_ID"),
    )


def get_profiles() -> list[RequestProfile]:
    return [
        RequestProfile(name="A", description="minimal mobile chrome"),
        RequestProfile(name="B", description="browser-like without auth"),
        RequestProfile(name="C", description="browser-like with optional env secrets"),
    ]


def get_endpoint_variants() -> list[EndpointVariant]:
    return [
        EndpointVariant("card_v4/project_params", OLD_ENDPOINT, PROJECT_PARAMS),
        EndpointVariant("card_v4/internal_params", OLD_ENDPOINT, INTERNAL_PARAMS),
        EndpointVariant("internal_v4/project_params", INTERNAL_ENDPOINT, PROJECT_PARAMS),
        EndpointVariant("internal_v4/internal_params", INTERNAL_ENDPOINT, INTERNAL_PARAMS),
    ]


def extract_metrics(payload: dict[str, Any]) -> tuple[bool, bool, bool, int | None, float | None, int | None, bool]:
    products = payload.get("products")
    if not isinstance(products, list) or not products:
        return False, False, False, None, None, None, False

    first_product = products[0]
    if not isinstance(first_product, dict):
        return False, False, False, None, None, None, False

    sizes = first_product.get("sizes")
    size_items = sizes if isinstance(sizes, list) else []
    has_sizes = bool(size_items)

    price_product: int | None = None
    has_stocks = False
    for size in size_items:
        if not isinstance(size, dict):
            continue
        stocks = size.get("stocks")
        if isinstance(stocks, list) and stocks:
            has_stocks = True
        price_info = size.get("price")
        if isinstance(price_info, dict):
            raw_price = price_info.get("product")
            if isinstance(raw_price, (int, float)):
                price_product = int(raw_price)
                break

    total_qty_raw = first_product.get("totalQuantity")
    total_qty = int(total_qty_raw) if isinstance(total_qty_raw, (int, float)) else None
    price_rub = (price_product / 100) if price_product is not None else None

    return True, has_sizes, price_product is not None, price_product, price_rub, total_qty, has_stocks


def normalize_x_pow(headers: Any) -> str:
    if not headers:
        return ""
    x_pow = headers.get("x-pow") or headers.get("X-Pow") or headers.get("x_pow")
    if not x_pow:
        return ""
    return "present"


def evaluate_current_parser(payload: dict[str, Any]) -> bool:
    service = WildberriesService()
    product = service.parse_product_details(payload)
    return bool(product and product.price is not None)


async def probe_once(
    session: AsyncSession,
    profile: RequestProfile,
    variant: EndpointVariant,
    article_id: int,
    secrets: SecretContext,
) -> ProbeResult:
    headers = profile.build_headers(article_id, secrets)
    params = variant.build_params(article_id)
    error = ""
    status_code: int | None = None
    response_size = 0
    payload: dict[str, Any] = {}
    parser_price_found = False
    x_pow = ""

    try:
        response = await session.get(variant.endpoint, params=params, headers=headers)
        status_code = int(response.status_code)
        response_size = len(response.content or b"")
        x_pow = normalize_x_pow(response.headers)
        if "application/json" in str(response.headers.get("content-type", "")).lower():
            payload = response.json()
            parser_price_found = evaluate_current_parser(payload)
        else:
            error = f"non_json_response:{response.headers.get('content-type', '')}"
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"

    has_products, has_sizes, has_price, price_product, price_rub, total_qty, has_stocks = extract_metrics(payload)
    return ProbeResult(
        profile=profile.name,
        endpoint=variant.name,
        nm=article_id,
        status_code=status_code,
        has_products=has_products,
        has_sizes=has_sizes,
        has_price=has_price,
        price_product=price_product,
        price_rub=price_rub,
        total_qty=total_qty,
        has_stocks=has_stocks,
        response_size=response_size,
        x_pow=x_pow,
        parser_price_found=parser_price_found,
        error=error,
    )


def to_markdown_table(rows: Iterable[dict[str, Any]]) -> str:
    prepared = list(rows)
    if not prepared:
        return "No rows."

    headers = list(prepared[0].keys())
    widths = {header: len(header) for header in headers}
    for row in prepared:
        for header in headers:
            widths[header] = max(widths[header], len(str(row[header])))

    def format_row(row: dict[str, Any]) -> str:
        return "| " + " | ".join(str(row[h]).ljust(widths[h]) for h in headers) + " |"

    separator = "| " + " | ".join("-" * widths[h] for h in headers) + " |"
    lines = [format_row({header: header for header in headers}), separator]
    lines.extend(format_row(row) for row in prepared)
    return "\n".join(lines)


def summarize(results: list[ProbeResult]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[ProbeResult]] = {}
    for result in results:
        grouped.setdefault((result.profile, result.endpoint), []).append(result)

    summary_rows: list[dict[str, Any]] = []
    for (profile, endpoint), items in grouped.items():
        count = len(items)
        http_200_count = sum(1 for item in items if item.status_code == 200)
        http_403_count = sum(1 for item in items if item.status_code == 403)
        http_498_count = sum(1 for item in items if item.status_code == 498)
        other_http_count = sum(
            1 for item in items if item.status_code is not None and item.status_code not in (200, 403, 498)
        )
        network_error_count = sum(1 for item in items if item.status_code is None)
        summary_rows.append(
            {
                "profile": profile,
                "endpoint": endpoint,
                "requests": count,
                "200_count": http_200_count,
                "403_count": http_403_count,
                "498_count": http_498_count,
                "other_http_count": other_http_count,
                "network_error_count": network_error_count,
                "http_200_rate": round(http_200_count / count, 3),
                "price_found_rate": round(sum(1 for item in items if item.has_price) / count, 3),
                "parser_price_rate": round(sum(1 for item in items if item.parser_price_found) / count, 3),
                "403_rate": round(http_403_count / count, 3),
                "498_rate": round(http_498_count / count, 3),
                "avg_response_size": round(mean(item.response_size for item in items), 1),
            }
        )
    return summary_rows


async def run_poc(args: argparse.Namespace) -> int:
    load_local_env()
    secrets = load_secrets()
    articles = parse_articles(args.articles, args.limit)

    LOGGER.info("Project root: %s", PROJECT_ROOT)
    LOGGER.info(
        "Secret flags: authorization_present=%s cookie_present=%s deviceid_present=%s",
        secrets.authorization_present,
        secrets.cookie_present,
        secrets.device_id_present,
    )
    LOGGER.info("Articles count: %s", len(articles))
    LOGGER.info("Profiles: %s", ", ".join(profile.name for profile in get_profiles()))

    results: list[ProbeResult] = []
    session = AsyncSession(impersonate="chrome120", timeout=args.timeout)
    try:
        for profile in get_profiles():
            if profile.name == "C" and not any(
                [secrets.authorization_present, secrets.cookie_present, secrets.device_id_present]
            ):
                LOGGER.warning("Skipping profile C because no env secrets were provided.")
                continue

            for variant in get_endpoint_variants():
                LOGGER.info("Running profile=%s endpoint=%s", profile.name, variant.name)
                for article_id in articles:
                    result = await probe_once(
                        session=session,
                        profile=profile,
                        variant=variant,
                        article_id=article_id,
                        secrets=secrets,
                    )
                    results.append(result)
                    await asyncio.sleep(max(0.0, args.delay))
    finally:
        await session.close()

    detail_rows = [
        {
            "profile": result.profile,
            "endpoint": result.endpoint,
            "nm": result.nm,
            "status_code": result.status_code,
            "has_products": result.has_products,
            "has_sizes": result.has_sizes,
            "has_price": result.has_price,
            "price_product": result.price_product,
            "price_rub": f"{result.price_rub:.2f}" if result.price_rub is not None else "",
            "total_qty": result.total_qty,
            "has_stocks": result.has_stocks,
            "response_size": result.response_size,
            "x_pow": result.x_pow,
            "error": result.error,
        }
        for result in results
    ]
    print("\nDetailed results:")
    print(to_markdown_table(detail_rows))

    print("\nSummary:")
    print(to_markdown_table(summarize(results)))

    return 0


def main() -> int:
    args = parse_args()
    configure_logging(verbose=args.verbose)
    try:
        return asyncio.run(run_poc(args))
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
