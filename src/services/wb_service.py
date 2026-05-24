"""Асинхронный сервис для работы с Wildberries API."""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from typing import Any, Dict, Optional, Set, Tuple

from curl_cffi.requests import AsyncSession
from loguru import logger

from src.config import (
    CONCURRENT_REQUESTS_LIMIT,
    WB_DEFAULT_DEST,
    WB_DETAIL_URL,
    WB_CIRCUIT_COOLDOWN,
    WB_FORBIDDEN_THRESHOLD,
    WB_MAX_RETRIES,
    WB_MAX_RPS,
    WB_RATE_LIMIT_DELAY,
    WB_RETRY_DELAY,
    WB_SIMILAR_URL,
    WB_TIMEOUT,
)
from src.data_models import ProductDetails, SimilarProductsResult, WBRequestResult


class WildberriesService:
    def __init__(self) -> None:
        self.session: Optional[AsyncSession] = None
        self.logger = logger
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
        self.current_concurrency_limit = CONCURRENT_REQUESTS_LIMIT
        self.max_retries = max(1, WB_MAX_RETRIES)
        self.base_retry_delay = max(0.2, float(WB_RETRY_DELAY))
        self.timeout = max(1, int(WB_TIMEOUT))
        self.forbidden_threshold = max(1, WB_FORBIDDEN_THRESHOLD)
        self.cooldown_seconds = max(10, int(WB_CIRCUIT_COOLDOWN or WB_RATE_LIMIT_DELAY))
        self.consecutive_forbidden = 0
        self.circuit_open_until = 0.0
        self.circuit_open_reason = ""
        self.half_open_probe_in_flight = False
        self.max_rps = max(1, WB_MAX_RPS)
        self.rps_window_seconds = 1.0
        self._request_timestamps: deque[float] = deque()
        self.default_headers: Dict[str, str] = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://www.wildberries.ru/",
        }

    async def initialize(self) -> None:
        """Инициализация HTTP-сессии."""
        self.session = AsyncSession(
            impersonate="chrome120",
            timeout=self.timeout,
            headers=self.default_headers,
        )
        self.logger.info("Сервис WB инициализирован, создана постоянная async-сессия")

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.logger.info("Сессия сервиса WB закрыта")

    async def update_concurrency_limit(self, new_limit: int) -> None:
        """Мягко изменяет лимит конкурентности без остановки активных запросов."""
        if new_limit < 1:
            new_limit = 1
        if new_limit == self.current_concurrency_limit:
            return
        self.semaphore = asyncio.Semaphore(new_limit)
        self.current_concurrency_limit = new_limit
        self.logger.info("Лимит конкурентности WB обновлен: {}", new_limit)

    async def get_product_details(self, product_id: int, request_id: str) -> WBRequestResult:
        """Получает данные товара: основной endpoint + fallback на basket."""
        detail_params = {
            "appType": 1,
            "curr": "rub",
            "dest": WB_DEFAULT_DEST,
            "spp": 30,
            "hide_vflags": 4294967296,
            "ab_testing": "false",
            "lang": "ru",
            "nm": product_id,
        }
        detail_response = await self._request_with_retry(
            url=WB_DETAIL_URL,
            endpoint="card_detail_v4",
            request_id=request_id,
            params=detail_params,
            context={"article_id": product_id},
        )
        if detail_response.ok:
            return detail_response

        basket_data = self._get_basket_data(product_id)
        basket_url = (
            f"https://basket-{basket_data['basket']}.wbbasket.ru/"
            f"vol{basket_data['vol']}/part{basket_data['part']}/{product_id}/info/ru/card.json"
        )
        self.logger.warning(
            "Переход на fallback basket endpoint: request_id={} article_id={} prev_status={} prev_error={}",
            request_id,
            product_id,
            detail_response.status_class,
            detail_response.error,
        )
        return await self._request_with_retry(
            url=basket_url,
            endpoint="basket_card",
            request_id=request_id,
            context={"article_id": product_id},
        )

    async def get_similar_products(
        self,
        product: ProductDetails,
        request_id: str,
    ) -> SimilarProductsResult:
        """Получает похожие товары через recom endpoint."""
        params = {
            "q1": f"nm{product.id}key {product.name}",
            "query": f"похожие {product.id}",
            "resultset": "catalog",
            "spp": 30,
            "curr": "rub",
            "dest": WB_DEFAULT_DEST,
        }
        response = await self._request_with_retry(
            url=WB_SIMILAR_URL,
            endpoint="recom_search",
            request_id=request_id,
            params=params,
            context={"article_id": product.id},
        )
        if not response.ok:
            return SimilarProductsResult(
                original_product=product,
                similar_products=[],
                error=f"{response.status_class}:{response.error or ''}".rstrip(":"),
            )

        payload = response.payload or {}
        products = payload.get("data", {}).get("products", [])
        if not isinstance(products, list):
            return SimilarProductsResult(
                original_product=product,
                similar_products=[],
                error="parse_error",
            )
        return SimilarProductsResult(
            original_product=product,
            similar_products=products,
            error=None,
        )

    def find_our_article_in_similar(
        self,
        similar: SimilarProductsResult,
        our_articles: Set[int],
    ) -> Tuple[Optional[int], Optional[int]]:
        """Возвращает найденный артикул и позицию в выдаче (1-based)."""
        for idx, item in enumerate(similar.similar_products, start=1):
            try:
                article_id = int(item.get("id"))
            except (TypeError, ValueError):
                continue
            if article_id in our_articles:
                return article_id, idx
        return None, None

    def parse_product_details(self, payload: Dict[str, Any]) -> Optional[ProductDetails]:
        """Нормализует ответ карточки в ProductDetails."""
        product_payload: Dict[str, Any]
        products = payload.get("products")
        if isinstance(products, list) and products:
            first_product = products[0]
            if not isinstance(first_product, dict):
                return None
            product_payload = first_product
        else:
            product_payload = payload

        try:
            product_id = int(product_payload.get("nm_id") or product_payload.get("id"))
        except (TypeError, ValueError):
            return None

        sizes = product_payload.get("sizes") or []
        price: Optional[int] = None
        if sizes and isinstance(sizes, list):
            for size in sizes:
                if not isinstance(size, dict):
                    continue
                price_info = size.get("price") or {}
                if not isinstance(price_info, dict):
                    continue
                raw_price = price_info.get("product")
                if isinstance(raw_price, (int, float)):
                    price = int(raw_price) // 100
                    break

        return ProductDetails(
            id=product_id,
            name=str(product_payload.get("imt_name") or product_payload.get("name") or ""),
            brand=str(
                product_payload.get("selling", {}).get("brand_name")
                if isinstance(product_payload.get("selling"), dict)
                else product_payload.get("brand_name") or ""
            ),
            price=price,
            raw_data=product_payload,
        )

    async def _request_with_retry(
        self,
        url: str,
        endpoint: str,
        request_id: str,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> WBRequestResult:
        if not self.session:
            return WBRequestResult(
                ok=False,
                status_class="network_error",
                retriable=True,
                error="session_not_initialized",
            )

        if self._is_circuit_open():
            self.logger.warning(
                "HTTP-запрос WB пропущен: request_id={} endpoint={} reason=circuit_open",
                request_id,
                endpoint,
            )
            return WBRequestResult(
                ok=False,
                status_class="forbidden",
                retriable=True,
                error="circuit_open",
            )

        retries_used = 0
        started_total = time.monotonic()
        self.logger.debug(
            "Старт HTTP-запроса WB: request_id={} endpoint={} max_retries={} context={}",
            request_id,
            endpoint,
            self.max_retries,
            context or {},
        )
        last_result = WBRequestResult(ok=False, status_class="network_error", retriable=True)
        for attempt in range(1, self.max_retries + 1):
            await self._throttle()
            await asyncio.sleep(random.uniform(0.01, 0.08))
            start = time.monotonic()
            try:
                async with self.semaphore:
                    response = await self.session.get(url, params=params)
                latency_ms = int((time.monotonic() - start) * 1000)
                status_code = int(response.status_code)
                status_class, retriable = self._classify_status(status_code)

                self.logger.info(
                    "Попытка HTTP-запроса WB: request_id={} endpoint={} status_code={} status_class={} retry_no={} latency_ms={} context={}",
                    request_id,
                    endpoint,
                    status_code,
                    status_class,
                    attempt - 1,
                    latency_ms,
                    context or {},
                )

                if status_class == "success":
                    self.consecutive_forbidden = 0
                    self._close_circuit_if_half_open()
                    payload = response.json()
                    return WBRequestResult(
                        ok=True,
                        status_class=status_class,
                        status_code=status_code,
                        retriable=False,
                        payload=payload if isinstance(payload, dict) else {"raw": payload},
                        latency_ms=latency_ms,
                        retries_used=retries_used,
                    )

                last_result = WBRequestResult(
                    ok=False,
                    status_class=status_class,
                    status_code=status_code,
                    retriable=retriable,
                    error=f"http_{status_code}",
                    latency_ms=latency_ms,
                    retries_used=retries_used,
                )
                if status_class == "forbidden":
                    self.consecutive_forbidden += 1
                    if self.consecutive_forbidden >= self.forbidden_threshold:
                        self._open_circuit(reason=f"forbidden_threshold:{self.consecutive_forbidden}")
                    retriable = retriable and attempt < 2

                if not retriable or attempt == self.max_retries:
                    self.logger.warning(
                        "HTTP-запрос WB завершился ошибкой: request_id={} endpoint={} final_status_class={} status_code={} retries_used={}",
                        request_id,
                        endpoint,
                        status_class,
                        status_code,
                        retries_used,
                    )
                    return last_result

                retries_used += 1
                await self._sleep_before_retry(
                    attempt=attempt,
                    status_code=status_code,
                    retry_after_header=response.headers.get("Retry-After"),
                )
            except asyncio.TimeoutError:
                retries_used += 1
                latency_ms = int((time.monotonic() - start) * 1000)
                last_result = WBRequestResult(
                    ok=False,
                    status_class="timeout",
                    retriable=True,
                    error="timeout",
                    latency_ms=latency_ms,
                    retries_used=retries_used,
                )
                self.logger.warning(
                    "Попытка HTTP-запроса WB: request_id={} status_class=timeout endpoint={} retry_no={} latency_ms={} context={}",
                    request_id,
                    endpoint,
                    attempt - 1,
                    latency_ms,
                    context or {},
                )
                if attempt == self.max_retries:
                    self.logger.warning(
                        "HTTP-запрос WB завершился таймаутом: request_id={} endpoint={} retries_used={}",
                        request_id,
                        endpoint,
                        retries_used,
                    )
                    return last_result
                await self._sleep_before_retry(attempt=attempt, status_code=None, retry_after_header=None)
            except Exception as exc:
                retries_used += 1
                latency_ms = int((time.monotonic() - start) * 1000)
                last_result = WBRequestResult(
                    ok=False,
                    status_class="network_error",
                    retriable=True,
                    error=str(exc),
                    latency_ms=latency_ms,
                    retries_used=retries_used,
                )
                self.logger.warning(
                    "Попытка HTTP-запроса WB: request_id={} endpoint={} status_class=network_error retry_no={} latency_ms={} context={} error={}",
                    request_id,
                    endpoint,
                    attempt - 1,
                    latency_ms,
                    context or {},
                    exc,
                )
                if attempt == self.max_retries:
                    self.logger.warning(
                        "HTTP-запрос WB завершился сетевой ошибкой: request_id={} endpoint={} retries_used={}",
                        request_id,
                        endpoint,
                        retries_used,
                    )
                    return last_result
                await self._sleep_before_retry(attempt=attempt, status_code=None, retry_after_header=None)

            if time.monotonic() - started_total > self.timeout * self.max_retries * 2:
                break
        return last_result

    async def _throttle(self) -> None:
        """Простой RPS limiter без внешних зависимостей."""
        now = time.monotonic()
        while self._request_timestamps and now - self._request_timestamps[0] > self.rps_window_seconds:
            self._request_timestamps.popleft()
        if len(self._request_timestamps) >= self.max_rps:
            sleep_for = self.rps_window_seconds - (now - self._request_timestamps[0])
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        self._request_timestamps.append(time.monotonic())

    async def _sleep_before_retry(
        self,
        attempt: int,
        status_code: Optional[int],
        retry_after_header: Optional[str],
    ) -> None:
        if status_code == 429 and retry_after_header:
            try:
                delay = float(retry_after_header)
            except ValueError:
                delay = self.cooldown_seconds
        elif status_code == 429:
            delay = self.cooldown_seconds
        elif status_code in (403, 498):
            delay = min(self.cooldown_seconds, self.base_retry_delay * (2 ** attempt))
        else:
            delay = self.base_retry_delay * (2 ** (attempt - 1))
        jitter = random.uniform(0.05, 0.25)
        self.logger.debug(
            "Пауза перед retry WB: status_code={} attempt={} base_delay_s={} jitter_s={:.3f}",
            status_code,
            attempt,
            delay,
            jitter,
        )
        await asyncio.sleep(delay + jitter)

    def _is_circuit_open(self) -> bool:
        now = time.monotonic()
        if now < self.circuit_open_until:
            return True
        if self.circuit_open_until > 0 and not self.half_open_probe_in_flight:
            self.half_open_probe_in_flight = True
            self.logger.info("Circuit breaker WB: half-open probe разрешен")
            return False
        if self.half_open_probe_in_flight:
            return True
        return False

    def _open_circuit(self, reason: str) -> None:
        self.circuit_open_until = time.monotonic() + self.cooldown_seconds
        self.circuit_open_reason = reason
        self.half_open_probe_in_flight = False
        self.logger.warning(
            "Открыт circuit breaker WB: cooldown_s={} reason={}",
            self.cooldown_seconds,
            reason,
        )

    def _close_circuit_if_half_open(self) -> None:
        if self.half_open_probe_in_flight or self.circuit_open_until > 0:
            self.circuit_open_until = 0.0
            self.circuit_open_reason = ""
            self.half_open_probe_in_flight = False
            self.logger.info("Circuit breaker WB закрыт после успешной half-open пробы")

    @staticmethod
    def _classify_status(status_code: int) -> Tuple[str, bool]:
        if status_code == 200:
            return "success", False
        if status_code == 404:
            return "not_found", False
        if status_code == 429:
            return "rate_limited", True
        if status_code in (403, 498):
            return "forbidden", True
        if 500 <= status_code <= 599:
            return "upstream_5xx", True
        if 400 <= status_code <= 499:
            return "client_4xx", False
        return "unknown_status", True

    @staticmethod
    def _get_basket_data(product_id: int) -> Dict[str, Any]:
        vol = product_id // 100000
        part = product_id // 1000
        if vol <= 143:
            basket = "01"
        elif vol <= 287:
            basket = "02"
        elif vol <= 431:
            basket = "03"
        elif vol <= 719:
            basket = "04"
        elif vol <= 1007:
            basket = "05"
        elif vol <= 1061:
            basket = "06"
        elif vol <= 1115:
            basket = "07"
        elif vol <= 1169:
            basket = "08"
        elif vol <= 1313:
            basket = "09"
        elif vol <= 1601:
            basket = "10"
        elif vol <= 1655:
            basket = "11"
        elif vol <= 1919:
            basket = "12"
        elif vol <= 2045:
            basket = "13"
        elif vol <= 2189:
            basket = "14"
        elif vol <= 2405:
            basket = "15"
        elif vol <= 2621:
            basket = "16"
        elif vol <= 2837:
            basket = "17"
        elif vol <= 3053:
            basket = "18"
        elif vol <= 3269:
            basket = "19"
        elif vol <= 3485:
            basket = "20"
        elif vol <= 3809:
            basket = "21"
        else:
            basket = "22"
        return {"basket": basket, "vol": vol, "part": part}
