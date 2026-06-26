"""Асинхронный сервис для работы с Wildberries API.

Модуль реализует:
- получение карточки товара;
- получение похожих товаров;
- нормализацию payload;
- устойчивый HTTP-контур с retry/backoff/throttle/circuit breaker.

WARNING:
Этот код чувствителен к anti-bot профилю:
- TLS fingerprint (`impersonate`),
- timing запросов,
- конкурентность,
- retry-поведение и паузы.

Даже небольшие изменения без наблюдения по логам могут увеличить 403/429
и снизить стабильность парсинга.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import Timeout
from loguru import logger

from src.config import (
    CONCURRENT_REQUESTS_LIMIT,
    WB_CIRCUIT_COOLDOWN,
    WB_PROXY_BUNDLES,
    WB_PROXY_BUNDLES_ENABLED,
    WB_PROXY_ROTATE_ON_CIRCUIT,
    WB_COOKIE,
    WB_COOKIE_ENABLED,
    WB_DEFAULT_DEST,
    WB_DETAIL_ENDPOINT_MODE,
    WB_DETAIL_URL,
    WB_DEVICE_ID,
    WB_FORBIDDEN_THRESHOLD,
    WB_MAX_RETRIES,
    WB_MAX_RPS,
    WB_PROXY_URL,
    WB_RATE_LIMIT_DELAY,
    WB_RETRY_DELAY,
    WB_SESSION_ROTATE_EVERY,
    WB_SESSION_ROTATION_ENABLED,
    WB_SESSION_ROTATION_SCOPE,
    WB_SIMILAR_URL,
    WB_TOKEN_AUTO_REFRESH_ENABLED,
    WB_TOKEN_COOKIE_NAME,
    WB_TOKEN_REFRESH_MAX_ATTEMPTS,
    WB_TOKEN_REFRESH_URL,
    WB_TOKEN_REFRESH_WAIT_SECONDS,
    WB_TIMEOUT,
    WB_U_CARD_DETAIL_URL,
    WB_USER_AGENT,
    WBProxyBundle,
)
from src.data_models import ProductDetails, SimilarProductsResult, WBRequestResult
from src.wb_cookie_manager import WbCookieManager
from src.wb_token_provider import WbTokenProvider


class WildberriesService:
    """Сервис HTTP-взаимодействия с Wildberries.

    Ключевые обязанности:
    - управлять lifecycle единой async-сессии;
    - ограничивать конкурентность и RPS;
    - классифицировать ошибки и выполнять безопасные retry;
    - изолировать anti-bot чувствительную логику внутри одного модуля.
    """

    def __init__(self) -> None:
        """Инициализирует runtime-состояние сервиса без открытия сети."""
        self.session: Optional[AsyncSession] = None
        self.logger = logger

        # Session rotation конфигурация и runtime-состояние.
        # На этом шаге поля только подготавливаются и не участвуют в логике запросов.
        self._session_rotation_enabled = WB_SESSION_ROTATION_ENABLED
        self._session_rotate_every = WB_SESSION_ROTATE_EVERY
        self._session_rotation_scope = WB_SESSION_ROTATION_SCOPE

        self._session_generation = 0
        self._session_request_count = 0

        self._session_lock = asyncio.Lock()

        self._session_inflight_by_generation: Dict[int, int] = {}
        self._retired_sessions: Dict[int, AsyncSession] = {}

        self._session_rotations_total = 0
        self._session_rotation_errors_total = 0
        self._session_creation_failures_total = 0
        self._session_retired_max = 0

        self._first_403_after_rotation_generation: Optional[int] = None
        self._last_rotation_started_at: Optional[float] = None

        # Глобальный лимитер конкурентности запросов.
        # WARNING: увеличение лимита может изменить timing profile и trigger anti-bot.
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
        self.current_concurrency_limit = CONCURRENT_REQUESTS_LIMIT

        # Retry/timeout параметры.
        self.max_retries = max(1, WB_MAX_RETRIES)
        self.base_retry_delay = max(0.2, float(WB_RETRY_DELAY))
        self.timeout = max(1, int(WB_TIMEOUT))
        self.detail_endpoint_mode = (WB_DETAIL_ENDPOINT_MODE or "card_v4").strip().lower()
        self._bundle_rotation_enabled = WB_PROXY_BUNDLES_ENABLED and bool(WB_PROXY_BUNDLES)
        self._bundle_rotate_on_circuit = WB_PROXY_ROTATE_ON_CIRCUIT
        self._proxy_bundles: List[WBProxyBundle] = list(WB_PROXY_BUNDLES)
        self._active_bundle_index = 0
        self._bundle_rotation_requested = False
        self._bundle_rotation_reason = ""
        self._bundle_rotations_total = 0
        self._token_refresh_enabled = WB_TOKEN_AUTO_REFRESH_ENABLED
        self._token_refresh_lock = asyncio.Lock()
        self._cookie_manager: Optional[WbCookieManager] = None
        self.cookie_enabled = False
        self.raw_cookie = ""
        self.device_id = ""
        self.proxy_url = ""
        self.proxy_host = ""
        self._apply_runtime_identity(
            cookie=WB_COOKIE,
            cookie_enabled=WB_COOKIE_ENABLED,
            device_id=WB_DEVICE_ID,
            proxy_url=WB_PROXY_URL,
        )
        if self._bundle_rotation_enabled:
            self._apply_bundle(self._active_bundle_index, reason="initial_bundle")

        # Circuit breaker параметры для серии forbidden-ответов.
        self.forbidden_threshold = max(1, WB_FORBIDDEN_THRESHOLD)
        self.cooldown_seconds = max(10, int(WB_CIRCUIT_COOLDOWN or WB_RATE_LIMIT_DELAY))
        self.consecutive_forbidden = 0
        self.circuit_open_until = 0.0
        self.circuit_open_reason = ""
        self.half_open_probe_in_flight = False

        # Локальный RPS limiter (token-window).
        self.max_rps = max(1, WB_MAX_RPS)
        self.rps_window_seconds = 1.0
        self._request_timestamps: deque[float] = deque()

        # Стабильный набор заголовков.
        # WARNING: это часть поведенческого профиля клиента.
        self.default_headers: Dict[str, str] = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": "https://www.wildberries.ru/",
        }

    def _apply_runtime_identity(
        self,
        *,
        cookie: str,
        cookie_enabled: bool,
        device_id: str,
        proxy_url: str,
    ) -> None:
        """Применяет текущий proxy/session context без раскрытия секретов."""
        normalized_cookie = cookie.strip()
        self.cookie_enabled = cookie_enabled and bool(normalized_cookie)
        self.raw_cookie = normalized_cookie if self.cookie_enabled else ""
        self.device_id = device_id.strip()
        self.proxy_url = proxy_url.strip()
        self.proxy_host = self._extract_proxy_host(self.proxy_url)
        self._cookie_manager = self._build_cookie_manager()

    def _apply_bundle(self, index: int, *, reason: str) -> None:
        """Применяет один согласованный proxy bundle."""
        bundle = self._proxy_bundles[index]
        self._active_bundle_index = index
        self._apply_runtime_identity(
            cookie=bundle.cookie,
            cookie_enabled=True,
            device_id=bundle.device_id,
            proxy_url=bundle.proxy_url,
        )
        self.logger.info(
            "WB proxy bundle applied label={} index={} total={} reason={} proxy_host={}",
            bundle.label,
            index + 1,
            len(self._proxy_bundles),
            reason,
            self.proxy_host or "-",
        )

    def _build_token_provider(self) -> WbTokenProvider:
        """Создаёт token provider для текущего proxy/session context."""
        return WbTokenProvider(
            user_agent=WB_USER_AGENT,
            url=WB_TOKEN_REFRESH_URL,
            cookie_name=WB_TOKEN_COOKIE_NAME,
            max_attempts=WB_TOKEN_REFRESH_MAX_ATTEMPTS,
            wait_seconds=WB_TOKEN_REFRESH_WAIT_SECONDS,
            proxy=self.proxy_url or None,
        )

    def _build_cookie_manager(self) -> Optional[WbCookieManager]:
        """Создаёт runtime-менеджер cookies для текущего bundle."""
        if not self.cookie_enabled or not self.raw_cookie:
            return None
        return WbCookieManager(
            raw_cookies=self.raw_cookie,
            token_provider=self._build_token_provider(),
            cookie_name=WB_TOKEN_COOKIE_NAME,
            auto_refresh_enabled=self._token_refresh_enabled,
        )

    def _update_runtime_cookie(self, new_cookie: str) -> None:
        """Обновляет cookie в runtime и в активном bundle без записи в .env."""
        self._apply_runtime_identity(
            cookie=new_cookie,
            cookie_enabled=True,
            device_id=self.device_id,
            proxy_url=self.proxy_url,
        )
        if self._bundle_rotation_enabled and self._proxy_bundles:
            active_bundle = self._proxy_bundles[self._active_bundle_index]
            self._proxy_bundles[self._active_bundle_index] = WBProxyBundle(
                label=active_bundle.label,
                proxy_url=active_bundle.proxy_url,
                cookie=new_cookie,
                device_id=active_bundle.device_id,
            )

    async def _refresh_cookie_token(
        self,
        *,
        request_id: str,
        endpoint: str,
        failed_cookie: str,
    ) -> bool:
        """Пытается обновить только x_wbaas_token для текущего bundle."""
        if not self._token_refresh_enabled or endpoint != "u_card_detail_v4":
            return False

        async with self._token_refresh_lock:
            if not self._cookie_manager:
                self.logger.warning(
                    "WB token refresh skipped: request_id={} reason=no_cookie_manager",
                    request_id,
                )
                return False

            if failed_cookie and self.raw_cookie and failed_cookie != self.raw_cookie:
                self.logger.info(
                    "WB token refresh reused existing updated cookie: request_id={}",
                    request_id,
                )
                return True

            refreshed = await asyncio.to_thread(self._cookie_manager.refresh_full_cookies)
            refresh_mode = "full_cookie"
            if not refreshed:
                refreshed = await asyncio.to_thread(self._cookie_manager.refresh_x_wbaas_token)
                refresh_mode = "token_only"
            if not refreshed:
                self.logger.warning(
                    "WB token refresh failed: request_id={} proxy_host={}",
                    request_id,
                    self.proxy_host or "-",
                )
                return False

            refreshed_cookie = self._cookie_manager.get_cookies()
            self._update_runtime_cookie(refreshed_cookie)
            self.logger.warning(
                "WB token refresh applied: request_id={} proxy_host={} mode={} token_changed={}",
                request_id,
                self.proxy_host or "-",
                refresh_mode,
                self._cookie_manager.last_refresh_changed,
            )
            return True

    def _has_next_bundle(self) -> bool:
        """Возвращает `True`, если доступен следующий bundle."""
        return self._bundle_rotation_enabled and self._active_bundle_index + 1 < len(self._proxy_bundles)

    def _schedule_bundle_rotation(self, *, reason: str) -> None:
        """Планирует переключение на следующий bundle при следующем lease."""
        if not self._has_next_bundle():
            self.logger.warning(
                "WB proxy bundle rotation skipped: reason={} active_index={} total={}",
                reason,
                self._active_bundle_index + 1,
                len(self._proxy_bundles),
            )
            return
        if self._bundle_rotation_requested:
            return
        self._bundle_rotation_requested = True
        self._bundle_rotation_reason = reason
        self.logger.warning(
            "WB proxy bundle rotation scheduled: reason={} next_index={} total={}",
            reason,
            self._active_bundle_index + 2,
            len(self._proxy_bundles),
        )

    def _build_detail_request_headers(self, product_id: int) -> Optional[Dict[str, str]]:
        """Возвращает минимальный cookie-only контур для detail endpoint."""
        if not self.cookie_enabled:
            return None
        if self.detail_endpoint_mode == "u_card_v4":
            return self._build_u_card_detail_request_headers(product_id)
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Cookie": self.raw_cookie,
            "Origin": "https://www.wildberries.ru",
            "Pragma": "no-cache",
            "Referer": f"https://www.wildberries.ru/catalog/{product_id}/detail.aspx",
            "Sec-CH-UA": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "Sec-CH-UA-Mobile": "?1",
            "Sec-CH-UA-Platform": '"Android"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 10; K) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Mobile Safari/537.36"
            ),
        }

    def _build_u_card_detail_request_headers(self, product_id: int) -> Optional[Dict[str, str]]:
        """Возвращает browser-parity контур для u_card detail endpoint."""
        if not self.cookie_enabled or not self.device_id:
            return None
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru,en;q=0.9",
            "Cookie": self.raw_cookie,
            "Priority": "u=1, i",
            "Referer": f"https://www.wildberries.ru/catalog/{product_id}/detail.aspx",
            "Sec-CH-UA": '"Chromium";v="146", "Not-A.Brand";v="24", "YaBrowser";v="26.4", "Yowser";v="2.5"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 YaBrowser/26.4.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "X-Spa-Version": "14.14.2",
            "deviceid": self.device_id,
        }

    def _build_recom_request_headers(self, product_id: int) -> Optional[Dict[str, str]]:
        """Возвращает browser-like cookie-only контур для recom_search."""
        if not self.cookie_enabled:
            return None
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "Cookie": self.raw_cookie,
            "Origin": "https://www.wildberries.ru",
            "Pragma": "no-cache",
            "Referer": f"https://www.wildberries.ru/catalog/{product_id}/detail.aspx",
            "Sec-CH-UA": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "Sec-CH-UA-Mobile": "?1",
            "Sec-CH-UA-Platform": '"Android"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 10; K) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Mobile Safari/537.36"
            ),
        }

    def _rebuild_request_headers(
        self,
        *,
        endpoint: str,
        product_id: Optional[int],
    ) -> Optional[Dict[str, str]]:
        """Пересобирает headers после runtime-обновления cookie/token."""
        if product_id is None:
            return None
        if endpoint == "u_card_detail_v4":
            return self._build_u_card_detail_request_headers(product_id)
        if endpoint == "card_detail_v4":
            return self._build_detail_request_headers(product_id)
        return None

    @staticmethod
    def _extract_proxy_host(proxy_url: str) -> str:
        """Возвращает host:port proxy без user/password для безопасного логирования."""
        if not proxy_url:
            return ""
        parsed = urlsplit(proxy_url)
        host = parsed.hostname or ""
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return host

    async def _create_session(self) -> AsyncSession:
        """Создаёт новую AsyncSession для будущей ротации."""
        generation = self._session_generation + 1
        try:
            session = AsyncSession(
                impersonate="chrome120",
                timeout=self.timeout,
                headers=self.default_headers,
                trust_env=False,
                proxy=self.proxy_url or None,
            )
        except Exception as exc:
            self._session_creation_failures_total += 1
            self.logger.exception("WB session creation failed generation={} error={}", generation, exc)
            raise

        self.logger.info(
            "WB session created generation={} detail_endpoint_mode={} proxy_enabled={} proxy_host={}",
            generation,
            self.detail_endpoint_mode,
            bool(self.proxy_url),
            self.proxy_host or "-",
        )
        return session

    def _should_rotate_session(self) -> bool:
        """Возвращает `True`, если условия для ротации session уже выполнены."""
        return (
            self._session_rotation_enabled is True
            and self._session_rotation_scope == "detail"
            and self._session_rotate_every > 0
            and self._session_request_count >= self._session_rotate_every
            and self.session is not None
        )

    async def _rotate_session_if_needed(self) -> None:
        """Выполняет ротацию active session при достижении порога."""
        if not self._should_rotate_session():
            return

        old_session = self.session
        old_generation = self._session_generation
        self._last_rotation_started_at = time.monotonic()
        self.logger.info(
            "WB session rotation requested generation={} request_count={} rotate_every={}",
            old_generation,
            self._session_request_count,
            self._session_rotate_every,
        )

        try:
            new_session = await self._create_session()
        except Exception:
            self._session_rotation_errors_total += 1
            raise

        if old_session is not None:
            self._retired_sessions[old_generation] = old_session

        new_generation = old_generation + 1
        self.session = new_session
        self._session_generation = new_generation
        self._session_request_count = 0
        self._session_inflight_by_generation.setdefault(new_generation, 0)
        self._session_rotations_total += 1
        self._session_retired_max = max(self._session_retired_max, len(self._retired_sessions))
        self.logger.info(
            "WB session rotated old_generation={} new_generation={} retired_count={}",
            old_generation,
            new_generation,
            len(self._retired_sessions),
        )

    async def _rotate_bundle_if_needed(self) -> None:
        """Переключает active session на следующий proxy bundle при деградации."""
        if not self._bundle_rotation_requested or not self._has_next_bundle():
            return

        old_session = self.session
        old_generation = self._session_generation
        old_bundle_index = self._active_bundle_index
        reason = self._bundle_rotation_reason or "bundle_rotation_requested"

        self._apply_bundle(old_bundle_index + 1, reason=reason)
        self._bundle_rotation_requested = False
        self._bundle_rotation_reason = ""

        try:
            new_session = await self._create_session()
        except Exception:
            self._bundle_rotation_requested = True
            self._bundle_rotation_reason = reason
            self._apply_bundle(old_bundle_index, reason="bundle_rotation_revert")
            raise

        if old_session is not None:
            self._retired_sessions[old_generation] = old_session

        new_generation = old_generation + 1
        self.session = new_session
        self._session_generation = new_generation
        self._session_request_count = 0
        self._session_inflight_by_generation.setdefault(new_generation, 0)
        self._bundle_rotations_total += 1
        self._session_rotations_total += 1
        self._session_retired_max = max(self._session_retired_max, len(self._retired_sessions))
        self.consecutive_forbidden = 0
        self.circuit_open_until = 0.0
        self.circuit_open_reason = ""
        self.half_open_probe_in_flight = False
        self._first_403_after_rotation_generation = None
        self.logger.warning(
            "WB proxy bundle rotated old_index={} new_index={} generation={} reason={} proxy_host={}",
            old_bundle_index + 1,
            self._active_bundle_index + 1,
            new_generation,
            reason,
            self.proxy_host or "-",
        )

    async def _acquire_session_lease(
        self,
        *,
        count_for_rotation: bool,
    ) -> Tuple[AsyncSession, int]:
        """Выдаёт active session и generation, резервируя in-flight lease."""
        async with self._session_lock:
            if self.session is None:
                raise RuntimeError("WB session lease requested before session initialization")

            await self._rotate_bundle_if_needed()

            if count_for_rotation:
                self._session_request_count += 1
                await self._rotate_session_if_needed()

            session = self.session
            if session is None:
                raise RuntimeError("WB session is unavailable after rotation check")

            generation = self._session_generation
            self._session_inflight_by_generation[generation] = (
                self._session_inflight_by_generation.get(generation, 0) + 1
            )
            return session, generation

    async def _release_session_lease(self, generation: int) -> None:
        """Освобождает lease и при необходимости закрывает retired session."""
        session_to_close: Optional[AsyncSession] = None

        async with self._session_lock:
            current_inflight = self._session_inflight_by_generation.get(generation)
            if current_inflight is None:
                self.logger.warning(
                    "WB session lease release requested for unknown generation={}",
                    generation,
                )
                return

            new_inflight = current_inflight - 1
            if new_inflight < 0:
                self.logger.warning(
                    "WB session inflight became negative generation={} inflight_before={}",
                    generation,
                    current_inflight,
                )
                new_inflight = 0

            self._session_inflight_by_generation[generation] = new_inflight

            if generation in self._retired_sessions and new_inflight > 0:
                self.logger.info(
                    "WB session close deferred generation={} inflight={}",
                    generation,
                    new_inflight,
                )

            if generation in self._retired_sessions and new_inflight == 0:
                session_to_close = self._retired_sessions.pop(generation)
                self._session_inflight_by_generation.pop(generation, None)

        if session_to_close is not None:
            await session_to_close.close()
            self.logger.info("WB session closed generation={}", generation)

    def _current_retired_inflight(self) -> int:
        """Возвращает суммарное число in-flight запросов по retired generations."""
        return sum(
            self._session_inflight_by_generation.get(generation, 0)
            for generation in self._retired_sessions
        )

    def get_session_rotation_metrics(self) -> Dict[str, Any]:
        """Возвращает безопасные диагностические метрики session rotation."""
        return {
            "session_rotation_enabled": self._session_rotation_enabled,
            "session_generation": self._session_generation,
            "session_request_count": self._session_request_count,
            "session_rotations_total": self._session_rotations_total,
            "session_rotation_errors_total": self._session_rotation_errors_total,
            "session_creation_failures_total": self._session_creation_failures_total,
            "bundle_rotation_enabled": self._bundle_rotation_enabled,
            "bundle_rotations_total": self._bundle_rotations_total,
            "bundle_active_index": self._active_bundle_index + 1 if self._proxy_bundles else 0,
            "bundle_total": len(self._proxy_bundles),
            "session_retired_total": len(self._retired_sessions),
            "session_retired_inflight_current": self._current_retired_inflight(),
            "session_retired_max": self._session_retired_max,
            "first_403_after_rotation_generation": self._first_403_after_rotation_generation,
        }

    async def _close_active_and_retired_sessions(self) -> None:
        """Закрывает active session и все retired sessions."""
        async with self._session_lock:
            active_session = self.session
            active_generation = self._session_generation
            retired_sessions = list(self._retired_sessions.items())

            self.session = None
            self._retired_sessions = {}
            self._session_inflight_by_generation = {}

        if active_session is not None:
            await active_session.close()
            self.logger.info("WB session closed generation={}", active_generation)

        for generation, session in retired_sessions:
            await session.close()
            self.logger.info("WB session closed generation={}", generation)

    async def initialize(self) -> None:
        """Создаёт постоянную AsyncSession.

        Почему `curl_cffi.AsyncSession`:
        - позволяет контролировать TLS fingerprint через `impersonate`;
        - в практике этого проекта даёт более устойчивый доступ к WB.

        Почему `impersonate="chrome120"`:
        - это проверенный baseline для текущего окружения;
        - смена версии может изменить fingerprint и антибот-реакции.
        """
        self.session = await self._create_session()
        self._session_generation = 1
        self._session_request_count = 0
        self._session_inflight_by_generation = {1: 0}
        self._retired_sessions = {}
        self.logger.info("Сервис WB инициализирован, создана постоянная async-сессия")

    async def close(self) -> None:
        """Закрывает AsyncSession.

        Побочный эффект:
        - прекращаются keep-alive соединения, следующий запуск создаст новую сессию.
        """
        if self.session or self._retired_sessions:
            await self._close_active_and_retired_sessions()
            self.logger.info("Сессия сервиса WB закрыта")

    async def update_concurrency_limit(self, new_limit: int) -> None:
        """Мягко обновляет лимит конкурентности.

        Параметры:
        - `new_limit`: новый предел одновременных запросов.

        Риск:
        - резкие колебания лимита ухудшают предсказуемость timing profile.
        """
        if new_limit < 1:
            new_limit = 1
        if new_limit == self.current_concurrency_limit:
            return
        self.semaphore = asyncio.Semaphore(new_limit)
        self.current_concurrency_limit = new_limit
        self.logger.info("Лимит конкурентности WB обновлен: {}", new_limit)

    async def get_product_details(self, product_id: int, request_id: str) -> WBRequestResult:
        """Получает карточку товара.

        Алгоритм:
        1. Основной detail endpoint;
        2. fallback на basket-card endpoint при неуспехе.

        Возвращает:
        - `WBRequestResult` с payload и классификацией статуса.
        """
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
        detail_url = WB_DETAIL_URL
        detail_endpoint = "card_detail_v4"
        if self.detail_endpoint_mode == "u_card_v4":
            detail_url = WB_U_CARD_DETAIL_URL
            detail_endpoint = "u_card_detail_v4"
            detail_params["hide_dtype"] = 15
            detail_params["mtype"] = 257
        detail_session, detail_generation = await self._acquire_session_lease(
            count_for_rotation=True,
        )
        try:
            detail_response = await self._request_with_retry(
                url=detail_url,
                endpoint=detail_endpoint,
                request_id=request_id,
                params=detail_params,
                context={"article_id": product_id},
                request_headers=self._build_detail_request_headers(product_id),
                session=detail_session,
                session_generation=detail_generation,
            )
        finally:
            await self._release_session_lease(detail_generation)
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
        fallback_session, fallback_generation = await self._acquire_session_lease(
            count_for_rotation=False,
        )
        try:
            return await self._request_with_retry(
                url=basket_url,
                endpoint="basket_card",
                request_id=request_id,
                context={"article_id": product_id},
                session=fallback_session,
                session_generation=fallback_generation,
            )
        finally:
            await self._release_session_lease(fallback_generation)

    async def get_similar_products(
        self,
        product: ProductDetails,
        request_id: str,
    ) -> SimilarProductsResult:
        """Получает похожие товары через recom endpoint.

        Важно:
        - используется исторически стабильный набор query-параметров;
        - изменение параметров часто приводит к росту 400-ответов.
        """
        params = {
            "q1": f"nm{product.id}key {product.name}",
            "query": f"похожие {product.id}",
            "resultset": "catalog",
            "spp": 30,
            "curr": "rub",
            "dest": WB_DEFAULT_DEST,
        }
        session, generation = await self._acquire_session_lease(
            count_for_rotation=False,
        )
        try:
            response = await self._request_with_retry(
                url=WB_SIMILAR_URL,
                endpoint="recom_search",
                request_id=request_id,
                params=params,
                context={"article_id": product.id},
                request_headers=self._build_recom_request_headers(product.id),
                session=session,
                session_generation=generation,
            )
        finally:
            await self._release_session_lease(generation)
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
        """Ищет первый наш артикул в списке похожих товаров.

        Возвращает:
        - `(article_id, position)` при успехе;
        - `(None, None)` если совпадений нет.
        """
        for idx, item in enumerate(similar.similar_products, start=1):
            try:
                article_id = int(item.get("id"))
            except (TypeError, ValueError):
                continue
            if article_id in our_articles:
                return article_id, idx
        return None, None

    @staticmethod
    def _normalize_price_candidate(raw_price: Any) -> Optional[int]:
        """Нормализует ценовой кандидат WB из minor-units формата."""
        if not isinstance(raw_price, (int, float)):
            return None
        if raw_price <= 0:
            return None
        return int(raw_price) // 100

    def _extract_price_from_size(self, size: Dict[str, Any]) -> Optional[int]:
        """Пытается извлечь цену из size, nested price и stocks."""
        candidate_fields = (
            "product",
            "price",
            "finalPrice",
            "salePrice",
            "walletPrice",
            "clientPrice",
            "basic",
            "total",
            "sale",
        )

        for field_name in candidate_fields:
            normalized = self._normalize_price_candidate(size.get(field_name))
            if normalized is not None:
                return normalized

        price_info = size.get("price") or {}
        if isinstance(price_info, dict):
            for field_name in candidate_fields:
                normalized = self._normalize_price_candidate(price_info.get(field_name))
                if normalized is not None:
                    return normalized

        stocks = size.get("stocks")
        if isinstance(stocks, list):
            for stock in stocks:
                if not isinstance(stock, dict):
                    continue
                for field_name in candidate_fields:
                    normalized = self._normalize_price_candidate(stock.get(field_name))
                    if normalized is not None:
                        return normalized

                stock_price = stock.get("price")
                if isinstance(stock_price, dict):
                    for field_name in candidate_fields:
                        normalized = self._normalize_price_candidate(stock_price.get(field_name))
                        if normalized is not None:
                            return normalized

        return None

    def parse_product_details(self, payload: Dict[str, Any]) -> Optional[ProductDetails]:
        """Нормализует payload карточки в `ProductDetails`.

        Поддерживает два формата:
        - `{"products": [...]}` для detail endpoint;
        - плоский `card.json` объект для basket fallback.
        """
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
                extracted_price = self._extract_price_from_size(size)
                if extracted_price is not None:
                    price = extracted_price
                    break
                price_info = size.get("price") or {}
                if not isinstance(price_info, dict):
                    continue
                raw_price = price_info.get("product")
                if isinstance(raw_price, (int, float)):
                    # WB часто отдаёт цену в \"копейках * 100\" формате.
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

    def build_price_diagnostics(self, product: ProductDetails) -> Dict[str, Any]:
        """Возвращает компактную диагностику структуры карточки для кейсов без цены."""
        raw_data = product.raw_data if isinstance(product.raw_data, dict) else {}
        sizes = raw_data.get("sizes")
        size_items: List[Dict[str, Any]] = sizes if isinstance(sizes, list) else []
        candidate_fields = (
            "product",
            "basic",
            "total",
            "logistics",
            "sale",
            "price",
            "finalPrice",
            "salePrice",
            "walletPrice",
            "clientPrice",
        )

        size_samples = []
        for size in size_items[:2]:
            if not isinstance(size, dict):
                size_samples.append({"size_type": type(size).__name__})
                continue

            nested_price = size.get("price")
            nested_price_dict = nested_price if isinstance(nested_price, dict) else {}
            stocks = size.get("stocks")
            stock_items = stocks if isinstance(stocks, list) else []
            candidate_values: Dict[str, Any] = {}
            for field_name in candidate_fields:
                if field_name in size and not isinstance(size[field_name], (dict, list)):
                    candidate_values[f"size.{field_name}"] = size[field_name]
                if field_name in nested_price_dict and not isinstance(nested_price_dict[field_name], (dict, list)):
                    candidate_values[f"size.price.{field_name}"] = nested_price_dict[field_name]

            stock_samples = []
            for stock in stock_items[:2]:
                if not isinstance(stock, dict):
                    stock_samples.append({"stock_type": type(stock).__name__})
                    continue
                stock_price = stock.get("price")
                stock_price_dict = stock_price if isinstance(stock_price, dict) else {}
                stock_candidate_values: Dict[str, Any] = {}
                for field_name in candidate_fields:
                    if field_name in stock and not isinstance(stock[field_name], (dict, list)):
                        stock_candidate_values[f"stock.{field_name}"] = stock[field_name]
                    if field_name in stock_price_dict and not isinstance(stock_price_dict[field_name], (dict, list)):
                        stock_candidate_values[f"stock.price.{field_name}"] = stock_price_dict[field_name]
                stock_samples.append(
                    {
                        "stock_keys": sorted(stock.keys()),
                        "stock_price_keys": sorted(stock_price_dict.keys()) if stock_price_dict else [],
                        "stock_candidate_values": stock_candidate_values,
                    }
                )

            size_samples.append(
                {
                    "size_keys": sorted(size.keys()),
                    "price_keys": sorted(nested_price_dict.keys()) if nested_price_dict else [],
                    "candidate_values": candidate_values,
                    "stock_samples": stock_samples,
                }
            )

        return {
            "product_id": product.id,
            "top_level_keys": sorted(raw_data.keys()),
            "sizes_count": len(size_items),
            "size_samples": size_samples,
        }

    async def _request_with_retry(
        self,
        url: str,
        endpoint: str,
        request_id: str,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
        *,
        request_headers: Optional[Dict[str, str]] = None,
        session: AsyncSession,
        session_generation: int,
    ) -> WBRequestResult:
        """Выполняет HTTP-запрос с retry/backoff/throttle/circuit-breaker.

        Это ключевая anti-bot чувствительная функция.
        Любые изменения в порядке вызовов, паузах или лимитах должны проверяться
        на реальных логах с метриками forbidden/rate_limited.
        """
        if session is None:
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
        if endpoint in {"card_detail_v4", "u_card_detail_v4"}:
            self.logger.info(
                "WB detail request context: request_id={} endpoint={} cookie_present={} deviceid_present={}",
                request_id,
                endpoint,
                bool(request_headers and request_headers.get("Cookie")),
                bool(request_headers and request_headers.get("deviceid")),
            )
        last_result = WBRequestResult(ok=False, status_class="network_error", retriable=True)
        token_refresh_attempted = False
        for attempt in range(1, self.max_retries + 1):
            # RPS-тормоз и небольшой jitter снижают burst-паттерн.
            await self._throttle()
            await asyncio.sleep(random.uniform(0.01, 0.08))
            start = time.monotonic()
            try:
                # semaphore удерживает верхнюю границу конкурентности.
                async with self.semaphore:
                    response = await session.get(url, params=params, headers=request_headers)
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
                if (
                    status_code == 498
                    and endpoint == "u_card_detail_v4"
                    and not token_refresh_attempted
                ):
                    product_id: Optional[int] = None
                    if context and context.get("article_id") is not None:
                        try:
                            product_id = int(context["article_id"])
                        except (TypeError, ValueError):
                            product_id = None

                    refreshed = await self._refresh_cookie_token(
                        request_id=request_id,
                        endpoint=endpoint,
                        failed_cookie=request_headers.get("Cookie", "") if request_headers else "",
                    )
                    if refreshed:
                        rebuilt_headers = self._rebuild_request_headers(
                            endpoint=endpoint,
                            product_id=product_id,
                        )
                        if rebuilt_headers:
                            request_headers = rebuilt_headers
                            token_refresh_attempted = True
                            self.logger.warning(
                                "WB request will retry after token refresh: request_id={} endpoint={} product_id={}",
                                request_id,
                                endpoint,
                                product_id or "-",
                            )
                            await asyncio.sleep(0.2)
                            continue

                if status_class == "forbidden":
                    self.consecutive_forbidden += 1
                    if (
                        self._session_generation > 1
                        and self._first_403_after_rotation_generation is None
                    ):
                        self._first_403_after_rotation_generation = session_generation
                        self.logger.warning(
                            "First 403 after session rotation generation={}",
                            session_generation,
                        )
                    if self.consecutive_forbidden >= self.forbidden_threshold:
                        self._open_circuit(reason=f"forbidden_threshold:{self.consecutive_forbidden}")
                    # Для forbidden делаем максимально ограниченный retry.
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

            # Guardrail: не даём одному запросу занимать слишком много общего времени.
            if time.monotonic() - started_total > self.timeout * self.max_retries * 2:
                break
        return last_result

    async def _throttle(self) -> None:
        """Ограничивает RPS в скользящем окне.

        Почему локально в сервисе:
        - не нужен внешний rate-limiter;
        - поведение полностью детерминировано в рамках процесса.
        """
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
        """Вычисляет паузу перед retry с backoff и jitter.

        Политика:
        - 429: уважать `Retry-After`, иначе cooldown;
        - 403/498: ограниченный backoff;
        - остальное: экспоненциальный backoff от базовой задержки.
        """
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
        """Проверяет состояние circuit breaker.

        Half-open логика:
        - после cooldown разрешается одна probe-попытка;
        - остальные запросы в этот момент остаются закрытыми.
        """
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
        """Открывает circuit breaker на cooldown-период."""
        self.circuit_open_until = time.monotonic() + self.cooldown_seconds
        self.circuit_open_reason = reason
        self.half_open_probe_in_flight = False
        if self._bundle_rotate_on_circuit:
            self._schedule_bundle_rotation(reason=reason)
        self.logger.warning(
            "Открыт circuit breaker WB: cooldown_s={} reason={}",
            self.cooldown_seconds,
            reason,
        )

    def _close_circuit_if_half_open(self) -> None:
        """Закрывает breaker после успешной half-open пробы."""
        if self.half_open_probe_in_flight or self.circuit_open_until > 0:
            self.circuit_open_until = 0.0
            self.circuit_open_reason = ""
            self.half_open_probe_in_flight = False
            self.logger.info("Circuit breaker WB закрыт после успешной half-open пробы")

    @staticmethod
    def _classify_status(status_code: int) -> Tuple[str, bool]:
        """Классифицирует HTTP-статус и признак retriable."""
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
        """Вычисляет basket/vol/part для fallback card.json URL.

        Это технический legacy-алгоритм маршрутизации WB-хранилищ.
        Менять его без проверки на реальных артикулах не рекомендуется.
        """
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
