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
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from time import perf_counter
from typing import Any, Iterable
from urllib.parse import urlsplit

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
U_CARD_DETAIL_ENDPOINT = "https://www.wildberries.ru/__internal/u-card/cards/v4/detail"

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

U_CARD_DETAIL_PARAMS = {
    "appType": 1,
    "curr": "rub",
    "dest": "-1257786",
    "spp": 30,
    "hide_vflags": 4294967296,
    "hide_dtype": 15,
    "mtype": 257,
    "lang": "ru",
    "ab_testing": "false",
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

ANDROID_CHROME_UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Mobile Safari/537.36"
)

YANDEX_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 YaBrowser/26.4.0.0 Safari/537.36"
)

SAFE_BODY_KEYS = ("code", "message", "error", "status", "detail", "request_id")


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

        headers = self._build_browser_headers(referer=referer)

        if self.name == "C":
            if secrets.device_id_present:
                headers["deviceid"] = str(secrets.device_id)
            if secrets.authorization_present:
                headers["Authorization"] = str(secrets.authorization)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
        if self.name == "D":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.device_id_present:
                headers["deviceid"] = str(secrets.device_id)
            if secrets.authorization_present:
                headers["Authorization"] = str(secrets.authorization)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
        if self.name == "E":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
        if self.name == "F":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
            if secrets.device_id_present:
                headers["deviceid"] = str(secrets.device_id)
        if self.name == "G":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
            if secrets.authorization_present:
                headers["Authorization"] = str(secrets.authorization)
        if self.name == "H":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
            if secrets.authorization_present:
                headers["Authorization"] = str(secrets.authorization)
            if secrets.device_id_present:
                headers["deviceid"] = str(secrets.device_id)
        if self.name == "I":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.authorization_present:
                headers["Authorization"] = str(secrets.authorization)
        if self.name == "J":
            headers = self._build_browser_parity_headers(referer=referer)
            if secrets.device_id_present:
                headers["deviceid"] = str(secrets.device_id)
        if self.name == "K":
            headers = self._build_yandex_browser_parity_headers(referer=referer)
            if secrets.cookie_present:
                headers["Cookie"] = str(secrets.cookie)
            if secrets.device_id_present:
                headers["deviceid"] = str(secrets.device_id)
        return headers

    @staticmethod
    def _build_browser_headers(referer: str) -> dict[str, str]:
        return {
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

    @staticmethod
    def _build_browser_parity_headers(referer: str) -> dict[str, str]:
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Origin": "https://www.wildberries.ru",
            "Pragma": "no-cache",
            "Referer": referer,
            "Sec-CH-UA": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "Sec-CH-UA-Mobile": "?1",
            "Sec-CH-UA-Platform": '"Android"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": ANDROID_CHROME_UA,
        }

    @staticmethod
    def _build_yandex_browser_parity_headers(referer: str) -> dict[str, str]:
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru,en;q=0.9",
            "Priority": "u=1, i",
            "Referer": referer,
            "Sec-CH-UA": '"Chromium";v="146", "Not-A.Brand";v="24", "YaBrowser";v="26.4", "Yowser";v="2.5"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": YANDEX_BROWSER_UA,
            "X-Requested-With": "XMLHttpRequest",
            "X-Spa-Version": "14.14.2",
        }


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
    params_variant: str
    nm: int
    latency_ms: float
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
    x_request_id: str
    content_type: str
    server: str
    parser_price_found: bool
    origin_present: bool
    referer_present: bool
    authorization_present: bool
    cookie_present: bool
    deviceid_present: bool
    user_agent_family: str
    sec_ch_ua_present: bool
    sec_fetch_present: bool
    body_hint: str
    error: str


@dataclass(frozen=True)
class ProxyContext:
    url: str | None
    enabled: bool
    host: str


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
    parser.add_argument(
        "--profiles",
        type=str,
        default="A,B,C,D",
        help="Comma-separated profile names to run, e.g. C,D.",
    )
    parser.add_argument(
        "--endpoints",
        type=str,
        default="card_v4,internal_v4",
        help="Comma-separated endpoint groups to run, e.g. card_v4 or card_v4,internal_v4.",
    )
    parser.add_argument(
        "--proxy-url",
        type=str,
        default=os.getenv("WB_PROXY_URL", ""),
        help="Optional proxy URL for this POC run. Can also be provided via WB_PROXY_URL.",
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


def load_proxy_context(raw_proxy_url: str) -> ProxyContext:
    proxy_url = raw_proxy_url.strip()
    if not proxy_url:
        return ProxyContext(url=None, enabled=False, host="")

    parsed = urlsplit(proxy_url)
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return ProxyContext(url=proxy_url, enabled=True, host=host)


def get_profiles(selected_profiles: set[str] | None = None) -> list[RequestProfile]:
    profiles = [
        RequestProfile(name="A", description="minimal mobile chrome"),
        RequestProfile(name="B", description="browser-like without auth"),
        RequestProfile(name="C", description="browser-like with optional env secrets"),
        RequestProfile(name="D", description="browser-parity with optional env secrets"),
        RequestProfile(name="E", description="browser-parity cookie only"),
        RequestProfile(name="F", description="browser-parity cookie plus deviceid"),
        RequestProfile(name="G", description="browser-parity cookie plus authorization"),
        RequestProfile(name="H", description="browser-parity full bundle"),
        RequestProfile(name="I", description="browser-parity authorization only"),
        RequestProfile(name="J", description="browser-parity deviceid only"),
        RequestProfile(name="K", description="yandex browser parity cookie plus deviceid"),
    ]
    if not selected_profiles:
        return profiles
    return [profile for profile in profiles if profile.name in selected_profiles]


def get_endpoint_variants() -> list[EndpointVariant]:
    return [
        EndpointVariant("card_v4/project_params", OLD_ENDPOINT, PROJECT_PARAMS),
        EndpointVariant("card_v4/internal_params", OLD_ENDPOINT, INTERNAL_PARAMS),
        EndpointVariant("internal_v4/project_params", INTERNAL_ENDPOINT, PROJECT_PARAMS),
        EndpointVariant("internal_v4/internal_params", INTERNAL_ENDPOINT, INTERNAL_PARAMS),
        EndpointVariant("u_card_v4/detail_params", U_CARD_DETAIL_ENDPOINT, U_CARD_DETAIL_PARAMS),
    ]


def parse_selected_profiles(raw_profiles: str) -> set[str]:
    selected = {token.strip().upper() for token in raw_profiles.split(",") if token.strip()}
    if not selected:
        raise ValueError("At least one profile is required in --profiles.")
    allowed = {profile.name for profile in get_profiles()}
    unknown = selected - allowed
    if unknown:
        raise ValueError(f"Unknown profiles requested: {', '.join(sorted(unknown))}.")
    return selected


def parse_selected_endpoints(raw_endpoints: str) -> set[str]:
    selected = {token.strip().lower() for token in raw_endpoints.split(",") if token.strip()}
    if not selected:
        raise ValueError("At least one endpoint group is required in --endpoints.")
    allowed = {"card_v4", "internal_v4", "u_card_v4"}
    unknown = selected - allowed
    if unknown:
        raise ValueError(f"Unknown endpoint groups requested: {', '.join(sorted(unknown))}.")
    return selected


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


def normalize_header_presence(headers: Any, *names: str) -> str:
    if not headers:
        return ""
    for name in names:
        if headers.get(name):
            return "present"
    return ""


def sanitize_debug_text(text: str) -> str:
    sanitized = text
    patterns = (
        (r"Bearer\s+[A-Za-z0-9._\-+/=]+", "Bearer <masked>"),
        (r"x_wbaas_token=[^;\\s\"']+", "x_wbaas_token=<masked>"),
        (r"wbx-validation-key=[^;\\s\"']+", "wbx-validation-key=<masked>"),
        (r"deviceid=[^;\\s\"']+", "deviceid=<masked>"),
        (r"routeb=[^;\\s\"']+", "routeb=<masked>"),
        (r"(?i)Cookie:\s*[^\r\n]+", "Cookie: <masked>"),
        (r"(?i)Authorization:\s*[^\r\n]+", "Authorization: <masked>"),
    )
    for pattern, replacement in patterns:
        sanitized = re.sub(pattern, replacement, sanitized)
    return sanitized[:300]


def extract_safe_body_hint(response: Any, content_type: str) -> str:
    if "application/json" in content_type.lower():
        try:
            payload = response.json()
        except Exception:
            return sanitize_debug_text((response.text or "")[:300])
        if isinstance(payload, dict):
            safe_payload = {
                key: payload[key]
                for key in SAFE_BODY_KEYS
                if key in payload and isinstance(payload[key], (str, int, float, bool, type(None)))
            }
            if safe_payload:
                return sanitize_debug_text(json.dumps(safe_payload, ensure_ascii=False))
        return sanitize_debug_text((response.text or "")[:300])
    return sanitize_debug_text((response.text or "")[:300])


def get_user_agent_family(headers: dict[str, str]) -> str:
    user_agent = headers.get("User-Agent", "")
    lowered = user_agent.lower()
    if "android" in lowered and "chrome" in lowered:
        return "android_chrome"
    if "windows" in lowered and "chrome" in lowered:
        return "desktop_chrome"
    if "mobile" in lowered and "chrome" in lowered:
        return "mobile_chrome"
    if "chrome" in lowered:
        return "chrome"
    return "unknown"


def get_profile_bundle(profile_name: str) -> str:
    bundles = {
        "A": "minimal_headers",
        "B": "browser_headers_only",
        "C": "browser_headers+cookie+authorization+deviceid",
        "D": "browser_parity+cookie+authorization+deviceid",
        "E": "cookie_only",
        "F": "cookie+deviceid",
        "G": "cookie+authorization",
        "H": "cookie+authorization+deviceid",
        "I": "authorization_only",
        "J": "deviceid_only",
        "K": "yandex_browser_cookie+deviceid",
    }
    return bundles.get(profile_name, "unknown")


def profile_has_required_secrets(profile_name: str, secrets: SecretContext) -> bool:
    if profile_name in {"A", "B"}:
        return True
    if profile_name == "C":
        return any(
            [secrets.authorization_present, secrets.cookie_present, secrets.device_id_present]
        )
    if profile_name == "D":
        return any(
            [secrets.authorization_present, secrets.cookie_present, secrets.device_id_present]
        )
    if profile_name == "E":
        return secrets.cookie_present
    if profile_name == "F":
        return secrets.cookie_present and secrets.device_id_present
    if profile_name == "G":
        return secrets.cookie_present and secrets.authorization_present
    if profile_name == "H":
        return (
            secrets.cookie_present
            and secrets.authorization_present
            and secrets.device_id_present
        )
    if profile_name == "I":
        return secrets.authorization_present
    if profile_name == "J":
        return secrets.device_id_present
    if profile_name == "K":
        return secrets.cookie_present and secrets.device_id_present
    return False


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
    x_request_id = ""
    content_type = ""
    server = ""
    body_hint = ""
    started_at = perf_counter()

    try:
        response = await session.get(variant.endpoint, params=params, headers=headers)
        status_code = int(response.status_code)
        response_size = len(response.content or b"")
        x_pow = normalize_x_pow(response.headers)
        x_request_id = normalize_header_presence(response.headers, "x-request-id", "X-Request-Id")
        content_type = str(response.headers.get("content-type", ""))
        server = str(response.headers.get("server", ""))
        if status_code in (400, 498):
            body_hint = extract_safe_body_hint(response, content_type)
        if "application/json" in content_type.lower():
            payload = response.json()
            parser_price_found = evaluate_current_parser(payload)
        else:
            error = f"non_json_response:{content_type}"
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"

    latency_ms = round((perf_counter() - started_at) * 1000, 1)
    has_products, has_sizes, has_price, price_product, price_rub, total_qty, has_stocks = extract_metrics(payload)
    return ProbeResult(
        profile=profile.name,
        endpoint=variant.name,
        params_variant=variant.name.split("/", maxsplit=1)[-1],
        nm=article_id,
        latency_ms=latency_ms,
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
        x_request_id=x_request_id,
        content_type=content_type,
        server=server,
        parser_price_found=parser_price_found,
        origin_present="Origin" in headers,
        referer_present="Referer" in headers,
        authorization_present="Authorization" in headers,
        cookie_present="Cookie" in headers,
        deviceid_present="deviceid" in headers,
        user_agent_family=get_user_agent_family(headers),
        sec_ch_ua_present="Sec-CH-UA" in headers,
        sec_fetch_present=any(key.startswith("Sec-Fetch-") for key in headers),
        body_hint=body_hint,
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
        first_403_index = next(
            (index for index, item in enumerate(items, start=1) if item.status_code == 403),
            None,
        )
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
                "has_products_rate": round(sum(1 for item in items if item.has_products) / count, 3),
                "price_found_rate": round(sum(1 for item in items if item.has_price) / count, 3),
                "parser_price_rate": round(sum(1 for item in items if item.parser_price_found) / count, 3),
                "403_rate": round(http_403_count / count, 3),
                "498_rate": round(http_498_count / count, 3),
                "avg_latency_ms": round(mean(item.latency_ms for item in items), 1),
                "first_403_index": first_403_index if first_403_index is not None else "",
                "avg_response_size": round(mean(item.response_size for item in items), 1),
            }
        )
    return summary_rows


def summarize_bundle_matrix(results: list[ProbeResult]) -> list[dict[str, Any]]:
    grouped: dict[str, list[ProbeResult]] = {}
    for result in results:
        grouped.setdefault(result.profile, []).append(result)

    rows: list[dict[str, Any]] = []
    endpoint_order = ("card_v4", "internal_v4", "u_card_v4")
    endpoint_rank = {200: 0, 403: 1, 400: 2, 498: 3}

    for profile in sorted(grouped):
        items = grouped[profile]
        endpoint_status: dict[str, str] = {}
        notes: list[str] = []
        best_rank = 999
        best_status = "none"

        for endpoint_prefix in endpoint_order:
            endpoint_items = [item for item in items if item.endpoint.startswith(endpoint_prefix)]
            statuses = sorted(
                {
                    item.status_code
                    for item in endpoint_items
                    if item.status_code is not None
                }
            )
            status_label = ",".join(str(status) for status in statuses) if statuses else "none"
            endpoint_status[endpoint_prefix] = status_label
            for status in statuses:
                rank = endpoint_rank.get(status, 10)
                if rank < best_rank:
                    best_rank = rank
                    best_status = str(status)

        if endpoint_status.get("card_v4") == "400":
            notes.append("card_shifted_403_to_400")
        if endpoint_status.get("internal_v4") == "498":
            notes.append("internal_still_498")

        rows.append(
            {
                "Profile": profile,
                "Bundle": get_profile_bundle(profile),
                "card_v4": endpoint_status.get("card_v4", "none"),
                "internal_v4": endpoint_status.get("internal_v4", "none"),
                "u_card_v4": endpoint_status.get("u_card_v4", "none"),
                "Best status": best_status,
                "Notes": ",".join(notes) if notes else "",
            }
        )

    return rows


async def run_poc(args: argparse.Namespace) -> int:
    load_local_env()
    secrets = load_secrets()
    proxy = load_proxy_context(args.proxy_url)
    articles = parse_articles(args.articles, args.limit)
    selected_profiles = parse_selected_profiles(args.profiles)
    selected_endpoints = parse_selected_endpoints(args.endpoints)
    profiles = get_profiles(selected_profiles)
    endpoint_variants = [
        variant
        for variant in get_endpoint_variants()
        if variant.name.split("/", maxsplit=1)[0] in selected_endpoints
    ]

    LOGGER.info("Project root: %s", PROJECT_ROOT)
    LOGGER.info(
        "Secret flags: authorization_present=%s cookie_present=%s deviceid_present=%s",
        secrets.authorization_present,
        secrets.cookie_present,
        secrets.device_id_present,
    )
    LOGGER.info("Proxy flags: proxy_enabled=%s proxy_host=%s", proxy.enabled, proxy.host or "-")
    LOGGER.info("Articles count: %s", len(articles))
    LOGGER.info("Profiles: %s", ", ".join(profile.name for profile in profiles))
    LOGGER.info("Endpoint groups: %s", ", ".join(sorted(selected_endpoints)))

    results: list[ProbeResult] = []
    session = AsyncSession(
        impersonate="chrome120",
        timeout=args.timeout,
        trust_env=False,
        proxy=proxy.url,
    )
    try:
        for profile in profiles:
            if not profile_has_required_secrets(profile.name, secrets):
                LOGGER.warning(
                    "Skipping profile %s because required env secrets are missing.",
                    profile.name,
                )
                continue

            for variant in endpoint_variants:
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
            "params_variant": result.params_variant,
            "nm": result.nm,
            "latency_ms": result.latency_ms,
            "status_code": result.status_code,
            "content_type": result.content_type,
            "server": result.server,
            "has_products": result.has_products,
            "has_sizes": result.has_sizes,
            "has_price": result.has_price,
            "price_product": result.price_product,
            "price_rub": f"{result.price_rub:.2f}" if result.price_rub is not None else "",
            "total_qty": result.total_qty,
            "has_stocks": result.has_stocks,
            "response_size": result.response_size,
            "x_pow": result.x_pow,
            "x_request_id": result.x_request_id,
            "origin_present": result.origin_present,
            "referer_present": result.referer_present,
            "authorization_present": result.authorization_present,
            "cookie_present": result.cookie_present,
            "deviceid_present": result.deviceid_present,
            "user_agent_family": result.user_agent_family,
            "sec_ch_ua_present": result.sec_ch_ua_present,
            "sec_fetch_present": result.sec_fetch_present,
            "body_hint": result.body_hint,
            "error": result.error,
        }
        for result in results
    ]
    print("\nDetailed results:")
    print(to_markdown_table(detail_rows))

    print("\nSummary:")
    print(to_markdown_table(summarize(results)))

    print("\nBundle matrix:")
    print(to_markdown_table(summarize_bundle_matrix(results)))

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
