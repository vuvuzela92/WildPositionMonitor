"""POC for experimentally probing anti-bot thresholds on WB detail endpoint.

This script is isolated from the production pipeline:
- does not touch checkpoint;
- does not touch ClickHouse;
- does not use production circuit breaker;
- does not import the production batch loop.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import WB_DEFAULT_DEST, WB_DETAIL_URL, WB_TIMEOUT


LOGGER = logging.getLogger("poc_wb_antibot_threshold")

DETAIL_PARAMS_TEMPLATE = {
    "appType": 1,
    "curr": "rub",
    "dest": WB_DEFAULT_DEST,
    "spp": 30,
    "hide_vflags": 4294967296,
    "ab_testing": "false",
    "lang": "ru",
}

PROJECT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://www.wildberries.ru/",
}

DEFAULT_DELAY_SECONDS = 0.25
DEFAULT_LIMIT_ARTICLES = 10
DEFAULT_SESSION_ROTATE_COUNTS = (10, 25, 50)


@dataclass(frozen=True)
class RequestPayloadMetrics:
    has_products: bool
    has_price: bool


@dataclass(frozen=True)
class Scenario:
    experiment: str
    name: str
    requests_total: int | None
    runtime_seconds: int | None
    concurrency: int
    delay_seconds: float
    session_strategy: str
    session_rotate_every: int | None = None


@dataclass
class RequestObservation:
    scenario_name: str
    request_index: int
    article_id: int
    started_monotonic: float
    finished_monotonic: float
    started_wall_ts: float
    status_code: int | None
    latency_ms: int
    has_products: bool
    has_price: bool
    response_size: int
    response_headers_count: int
    x_request_id: str
    x_cache_status: str
    x_pow: str
    error: str


@dataclass(frozen=True)
class ScenarioSummary:
    experiment: str
    name: str
    requests_total: int
    concurrency: int
    delay_seconds: float
    session_strategy: str
    runtime_seconds: int | None
    count_200: int
    count_403: int
    count_other_http: int
    count_network_error: int
    rate_200: float
    rate_403: float
    first_403_request_index: int | None
    first_403_timestamp: str
    avg_response_time_ms: float
    max_response_time_ms: int
    has_products_rate: float
    has_price_rate: float


class SessionManager:
    """Controls AsyncSession lifecycle for one scenario."""

    def __init__(self, timeout_seconds: int, strategy: str, rotate_every: int | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.strategy = strategy
        self.rotate_every = rotate_every
        self._lock = asyncio.Lock()
        self._shared_session: AsyncSession | None = None
        self._requests_on_current_session = 0

    async def get_session(self) -> AsyncSession:
        if self.strategy == "per_request":
            return self._build_session()

        async with self._lock:
            if self._shared_session is None:
                self._shared_session = self._build_session()
                self._requests_on_current_session = 0
            elif self.strategy == "rotate_n" and self.rotate_every and self._requests_on_current_session >= self.rotate_every:
                await self._shared_session.close()
                self._shared_session = self._build_session()
                self._requests_on_current_session = 0

            self._requests_on_current_session += 1
            return self._shared_session

    async def release_session(self, session: AsyncSession) -> None:
        if self.strategy == "per_request":
            await session.close()

    async def close(self) -> None:
        async with self._lock:
            if self._shared_session is not None:
                await self._shared_session.close()
                self._shared_session = None

    def _build_session(self) -> AsyncSession:
        return AsyncSession(
            impersonate="chrome120",
            timeout=self.timeout_seconds,
            headers=PROJECT_HEADERS,
        )


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="POC for WB anti-bot threshold detection.")
    parser.add_argument(
        "--articles",
        type=str,
        default=os.getenv("POC_WB_ARTICLES", ""),
        help="Comma-separated article IDs. Can also be provided via POC_WB_ARTICLES.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT_ARTICLES,
        help="Optional limit for parsed article list.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=max(5, int(WB_TIMEOUT)),
        help="HTTP timeout in seconds for one request.",
    )
    parser.add_argument(
        "--experiments",
        type=str,
        default="all",
        help="Comma-separated subset: concurrency,count,session,runtime or all.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def load_local_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)


def parse_articles(raw_articles: str, limit: int) -> list[int]:
    if not raw_articles.strip():
        raise ValueError("Articles are required. Use --articles or POC_WB_ARTICLES.")

    articles = [int(token.strip()) for token in raw_articles.split(",") if token.strip()]
    if limit > 0:
        return articles[:limit]
    return articles


def build_params(article_id: int) -> dict[str, Any]:
    params = dict(DETAIL_PARAMS_TEMPLATE)
    params["nm"] = article_id
    return params


def extract_payload_metrics(payload: dict[str, Any]) -> RequestPayloadMetrics:
    products = payload.get("products")
    if not isinstance(products, list) or not products:
        return RequestPayloadMetrics(has_products=False, has_price=False)

    first_product = products[0]
    if not isinstance(first_product, dict):
        return RequestPayloadMetrics(has_products=False, has_price=False)

    sizes = first_product.get("sizes")
    if not isinstance(sizes, list):
        return RequestPayloadMetrics(has_products=True, has_price=False)

    for size in sizes:
        if not isinstance(size, dict):
            continue
        price_info = size.get("price")
        if isinstance(price_info, dict) and isinstance(price_info.get("product"), (int, float)):
            return RequestPayloadMetrics(has_products=True, has_price=True)

    return RequestPayloadMetrics(has_products=True, has_price=False)


def monotonic_to_wall(start_wall_ts: float, start_monotonic: float, current_monotonic: float) -> str:
    wall_ts = start_wall_ts + (current_monotonic - start_monotonic)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(wall_ts))


def to_markdown_table(rows: Iterable[dict[str, Any]]) -> str:
    prepared = list(rows)
    if not prepared:
        return "No rows."

    headers = list(prepared[0].keys())
    widths = {header: len(header) for header in headers}
    for row in prepared:
        for header in headers:
            widths[header] = max(widths[header], len(str(row[header])))

    def render_row(row: dict[str, Any]) -> str:
        return "| " + " | ".join(str(row[header]).ljust(widths[header]) for header in headers) + " |"

    separator = "| " + " | ".join("-" * widths[header] for header in headers) + " |"
    lines = [render_row({header: header for header in headers}), separator]
    lines.extend(render_row(row) for row in prepared)
    return "\n".join(lines)


async def execute_request(
    scenario: Scenario,
    session_manager: SessionManager,
    article_id: int,
    request_index: int,
    started_wall_ts: float,
    started_monotonic_run: float,
) -> RequestObservation:
    request_started = time.monotonic()
    session = await session_manager.get_session()
    status_code: int | None = None
    has_products = False
    has_price = False
    response_size = 0
    response_headers_count = 0
    x_request_id = ""
    x_cache_status = ""
    x_pow = ""
    error = ""

    try:
        response = await session.get(WB_DETAIL_URL, params=build_params(article_id))
        status_code = int(response.status_code)
        response_size = len(response.content or b"")
        response_headers_count = len(response.headers or {})
        x_request_id = str(response.headers.get("x-request-id") or "")
        x_cache_status = str(response.headers.get("x-cache-status") or "")
        x_pow = str(response.headers.get("x-pow") or "")
        content_type = str(response.headers.get("content-type") or "").lower()
        if "application/json" in content_type:
            payload = response.json()
            metrics = extract_payload_metrics(payload if isinstance(payload, dict) else {})
            has_products = metrics.has_products
            has_price = metrics.has_price
        else:
            error = f"non_json_response:{content_type}"
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"
    finally:
        await session_manager.release_session(session)

    request_finished = time.monotonic()
    latency_ms = int((request_finished - request_started) * 1000)

    LOGGER.debug(
        "Request done scenario=%s idx=%s article=%s status=%s latency_ms=%s has_products=%s has_price=%s "
        "x_request_id=%s x_cache_status=%s x_pow_present=%s response_size=%s response_headers_count=%s error=%s",
        scenario.name,
        request_index,
        article_id,
        status_code,
        latency_ms,
        has_products,
        has_price,
        x_request_id,
        x_cache_status,
        bool(x_pow),
        response_size,
        response_headers_count,
        error,
    )

    return RequestObservation(
        scenario_name=scenario.name,
        request_index=request_index,
        article_id=article_id,
        started_monotonic=request_started,
        finished_monotonic=request_finished,
        started_wall_ts=started_wall_ts,
        status_code=status_code,
        latency_ms=latency_ms,
        has_products=has_products,
        has_price=has_price,
        response_size=response_size,
        response_headers_count=response_headers_count,
        x_request_id=x_request_id,
        x_cache_status=x_cache_status,
        x_pow=x_pow,
        error=error,
    )


def build_scenarios(selected: set[str]) -> list[Scenario]:
    scenarios: list[Scenario] = []

    if "concurrency" in selected:
        scenarios.extend(
            [
                Scenario("concurrency", "conc_1_delay_1s", 100, None, 1, 1.0, "single"),
                Scenario("concurrency", "conc_2_delay_0.5s", 100, None, 2, 0.5, "single"),
                Scenario("concurrency", "conc_3_delay_0.25s", 100, None, 3, 0.25, "single"),
                Scenario("concurrency", "conc_5_delay_0s", 100, None, 5, 0.0, "single"),
                Scenario("concurrency", "conc_10_delay_0s", 100, None, 10, 0.0, "single"),
            ]
        )

    if "count" in selected:
        scenarios.extend(
            [
                Scenario("count", "count_10", 10, None, 1, 1.0, "single"),
                Scenario("count", "count_50", 50, None, 1, 1.0, "single"),
                Scenario("count", "count_100", 100, None, 1, 1.0, "single"),
                Scenario("count", "count_250", 250, None, 1, 1.0, "single"),
                Scenario("count", "count_500", 500, None, 1, 1.0, "single"),
            ]
        )

    if "session" in selected:
        scenarios.append(Scenario("session", "session_single", 100, None, 2, DEFAULT_DELAY_SECONDS, "single"))
        scenarios.append(Scenario("session", "session_per_request", 100, None, 2, DEFAULT_DELAY_SECONDS, "per_request"))
        for rotate_every in DEFAULT_SESSION_ROTATE_COUNTS:
            scenarios.append(
                Scenario(
                    "session",
                    f"session_rotate_{rotate_every}",
                    100,
                    None,
                    2,
                    DEFAULT_DELAY_SECONDS,
                    "rotate_n",
                    session_rotate_every=rotate_every,
                )
            )

    if "runtime" in selected:
        scenarios.extend(
            [
                Scenario("runtime", "runtime_10s", None, 10, 2, DEFAULT_DELAY_SECONDS, "single"),
                Scenario("runtime", "runtime_30s", None, 30, 2, DEFAULT_DELAY_SECONDS, "single"),
                Scenario("runtime", "runtime_60s", None, 60, 2, DEFAULT_DELAY_SECONDS, "single"),
                Scenario("runtime", "runtime_120s", None, 120, 2, DEFAULT_DELAY_SECONDS, "single"),
            ]
        )

    return scenarios


async def run_requests_scenario(scenario: Scenario, articles: list[int], timeout_seconds: int) -> list[RequestObservation]:
    session_manager = SessionManager(
        timeout_seconds=timeout_seconds,
        strategy=scenario.session_strategy,
        rotate_every=scenario.session_rotate_every,
    )
    semaphore = asyncio.Semaphore(max(1, scenario.concurrency))
    observations: list[RequestObservation] = []
    tasks: list[asyncio.Task[RequestObservation]] = []
    started_wall_ts = time.time()
    started_monotonic_run = time.monotonic()

    async def runner(request_index: int, article_id: int) -> RequestObservation:
        async with semaphore:
            return await execute_request(
                scenario=scenario,
                session_manager=session_manager,
                article_id=article_id,
                request_index=request_index,
                started_wall_ts=started_wall_ts,
                started_monotonic_run=started_monotonic_run,
            )

    try:
        assert scenario.requests_total is not None
        for request_index in range(1, scenario.requests_total + 1):
            article_id = articles[(request_index - 1) % len(articles)]
            tasks.append(asyncio.create_task(runner(request_index, article_id)))
            if scenario.delay_seconds > 0:
                await asyncio.sleep(scenario.delay_seconds)
        observations = await asyncio.gather(*tasks)
    finally:
        await session_manager.close()

    return sorted(observations, key=lambda item: item.request_index)


async def run_runtime_scenario(scenario: Scenario, articles: list[int], timeout_seconds: int) -> list[RequestObservation]:
    session_manager = SessionManager(
        timeout_seconds=timeout_seconds,
        strategy=scenario.session_strategy,
        rotate_every=scenario.session_rotate_every,
    )
    semaphore = asyncio.Semaphore(max(1, scenario.concurrency))
    observations: list[RequestObservation] = []
    tasks: list[asyncio.Task[RequestObservation]] = []
    started_wall_ts = time.time()
    started_monotonic_run = time.monotonic()
    request_index = 0

    async def runner(local_request_index: int, article_id: int) -> RequestObservation:
        async with semaphore:
            return await execute_request(
                scenario=scenario,
                session_manager=session_manager,
                article_id=article_id,
                request_index=local_request_index,
                started_wall_ts=started_wall_ts,
                started_monotonic_run=started_monotonic_run,
            )

    try:
        assert scenario.runtime_seconds is not None
        deadline = time.monotonic() + scenario.runtime_seconds
        while time.monotonic() < deadline:
            request_index += 1
            article_id = articles[(request_index - 1) % len(articles)]
            tasks.append(asyncio.create_task(runner(request_index, article_id)))
            if scenario.delay_seconds > 0:
                await asyncio.sleep(scenario.delay_seconds)
        if tasks:
            observations = await asyncio.gather(*tasks)
    finally:
        await session_manager.close()

    return sorted(observations, key=lambda item: item.request_index)


def summarize_scenario(experiment: str, scenario: Scenario, observations: list[RequestObservation]) -> ScenarioSummary:
    requests_total = len(observations)
    count_200 = sum(1 for item in observations if item.status_code == 200)
    count_403 = sum(1 for item in observations if item.status_code == 403)
    count_other_http = sum(
        1 for item in observations if item.status_code is not None and item.status_code not in (200, 403)
    )
    count_network_error = sum(1 for item in observations if item.status_code is None)
    first_403 = next((item for item in observations if item.status_code == 403), None)

    return ScenarioSummary(
        experiment=experiment,
        name=scenario.name,
        requests_total=requests_total,
        concurrency=scenario.concurrency,
        delay_seconds=scenario.delay_seconds,
        session_strategy=(
            scenario.session_strategy
            if scenario.session_rotate_every is None
            else f"{scenario.session_strategy}:{scenario.session_rotate_every}"
        ),
        runtime_seconds=scenario.runtime_seconds,
        count_200=count_200,
        count_403=count_403,
        count_other_http=count_other_http,
        count_network_error=count_network_error,
        rate_200=round(count_200 / requests_total, 3) if requests_total else 0.0,
        rate_403=round(count_403 / requests_total, 3) if requests_total else 0.0,
        first_403_request_index=first_403.request_index if first_403 else None,
        first_403_timestamp=(
            monotonic_to_wall(first_403.started_wall_ts, first_403.started_monotonic, first_403.finished_monotonic)
            if first_403
            else ""
        ),
        avg_response_time_ms=round(mean(item.latency_ms for item in observations), 1) if observations else 0.0,
        max_response_time_ms=max((item.latency_ms for item in observations), default=0),
        has_products_rate=round(sum(1 for item in observations if item.has_products) / requests_total, 3)
        if requests_total
        else 0.0,
        has_price_rate=round(sum(1 for item in observations if item.has_price) / requests_total, 3)
        if requests_total
        else 0.0,
    )


def build_table_concurrency(summaries: list[ScenarioSummary]) -> list[dict[str, Any]]:
    return [
        {
            "Requests": summary.requests_total,
            "Concurrency": summary.concurrency,
            "Delay": summary.delay_seconds,
            "200": summary.count_200,
            "403": summary.count_403,
            "First 403 At": summary.first_403_request_index or "",
        }
        for summary in summaries
        if summary.experiment == "concurrency"
    ]


def build_table_session(summaries: list[ScenarioSummary]) -> list[dict[str, Any]]:
    return [
        {
            "Requests": summary.requests_total,
            "Session Strategy": summary.session_strategy,
            "200 Rate": summary.rate_200,
            "403 Rate": summary.rate_403,
        }
        for summary in summaries
        if summary.experiment == "session"
    ]


def build_table_runtime(summaries: list[ScenarioSummary]) -> list[dict[str, Any]]:
    return [
        {
            "Runtime Seconds": summary.runtime_seconds or "",
            "200 Rate": summary.rate_200,
            "403 Rate": summary.rate_403,
        }
        for summary in summaries
        if summary.experiment == "runtime"
    ]


def build_table_count(summaries: list[ScenarioSummary]) -> list[dict[str, Any]]:
    return [
        {
            "Requests": summary.requests_total,
            "Concurrency": summary.concurrency,
            "Delay": summary.delay_seconds,
            "200": summary.count_200,
            "403": summary.count_403,
            "First 403 At": summary.first_403_request_index or "",
        }
        for summary in summaries
        if summary.experiment == "count"
    ]


def build_table_summary(summaries: list[ScenarioSummary]) -> list[dict[str, Any]]:
    return [
        {
            "experiment": summary.experiment,
            "scenario": summary.name,
            "requests_total": summary.requests_total,
            "concurrency": summary.concurrency,
            "delay_seconds": summary.delay_seconds,
            "session_strategy": summary.session_strategy,
            "runtime_seconds": summary.runtime_seconds or "",
            "200_count": summary.count_200,
            "403_count": summary.count_403,
            "other_http_count": summary.count_other_http,
            "network_error_count": summary.count_network_error,
            "200_rate": summary.rate_200,
            "403_rate": summary.rate_403,
            "first_403_request_index": summary.first_403_request_index or "",
            "first_403_timestamp": summary.first_403_timestamp,
            "avg_response_time_ms": summary.avg_response_time_ms,
            "max_response_time_ms": summary.max_response_time_ms,
            "has_products_rate": summary.has_products_rate,
            "has_price_rate": summary.has_price_rate,
        }
        for summary in summaries
    ]


async def run_all(args: argparse.Namespace) -> int:
    load_local_env()
    articles = parse_articles(args.articles, args.limit)
    selected = {item.strip() for item in args.experiments.split(",") if item.strip()}
    if "all" in selected:
        selected = {"concurrency", "count", "session", "runtime"}

    scenarios = build_scenarios(selected)
    LOGGER.info("Project root: %s", PROJECT_ROOT)
    LOGGER.info("Endpoint: %s", WB_DETAIL_URL)
    LOGGER.info("Articles count: %s", len(articles))
    LOGGER.info("Selected experiments: %s", ", ".join(sorted(selected)))
    LOGGER.info("Scenarios total: %s", len(scenarios))

    summaries: list[ScenarioSummary] = []
    for scenario in scenarios:
        LOGGER.info(
            "Running scenario=%s experiment=%s requests=%s runtime_seconds=%s concurrency=%s delay=%s session=%s",
            scenario.name,
            scenario.experiment,
            scenario.requests_total,
            scenario.runtime_seconds,
            scenario.concurrency,
            scenario.delay_seconds,
            scenario.session_strategy if scenario.session_rotate_every is None else f"{scenario.session_strategy}:{scenario.session_rotate_every}",
        )
        if scenario.runtime_seconds is not None:
            observations = await run_runtime_scenario(scenario, articles, args.timeout)
        else:
            observations = await run_requests_scenario(scenario, articles, args.timeout)
        summaries.append(summarize_scenario(scenario.experiment, scenario, observations))

    print("\nScenario summary:")
    print(to_markdown_table(build_table_summary(summaries)))

    print("\nTable #1: Concurrency threshold")
    print(to_markdown_table(build_table_concurrency(summaries)))

    print("\nTable #2: Session strategy")
    print(to_markdown_table(build_table_session(summaries)))

    print("\nTable #3: Runtime")
    print(to_markdown_table(build_table_runtime(summaries)))

    print("\nTable #4: Request volume")
    print(to_markdown_table(build_table_count(summaries)))

    return 0


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)
    try:
        return asyncio.run(run_all(args))
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
