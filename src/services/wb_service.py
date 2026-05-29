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
<<<<<<< HEAD
Асинхронный сервис для работы с Wildberries API с TLS-авторизацией
"""

import asyncio
import csv
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Dict, Optional, Any, Tuple, Set

# Импортируем только сессию, никаких скрытых модулей backend
=======

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from typing import Any, Dict, Optional, Set, Tuple

>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import Timeout
from loguru import logger

from src.config import (
<<<<<<< HEAD
    WB_DETAIL_URL, WB_SIMILAR_URL, WB_DEFAULT_DEST,
    WB_TIMEOUT, WB_MAX_RETRIES, WB_RETRY_DELAY, WB_RATE_LIMIT_DELAY,
    CONCURRENT_REQUESTS_LIMIT, WB_RAW_COOKIES, WB_USER_AGENT,
    LOG_DIR, WB_PRICE_LOG_FILE, WB_PRICE_ERRORS_CSV,
    MAX_TOKEN_REFRESH_RETRIES, MAX_CONSECUTIVE_498_ERRORS,
    LOG_RETENTION_DAYS, WB_TOKEN_AUTO_REFRESH_ENABLED,
    WB_TOKEN_REFRESH_URL, WB_TOKEN_COOKIE_NAME,
    WB_TOKEN_REFRESH_MAX_ATTEMPTS, WB_TOKEN_REFRESH_WAIT_SECONDS,
    WB_TOKEN_REFRESH_MAX_RETRIES_PER_ARTICLE
)
from src.data_models import ProductDetails, SimilarProductsResult
from src.wb_cookie_manager import WbCookieManager
from src.wb_token_provider import WbTokenProvider


@dataclass
class WbRequestResult:
    data: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    status_code: Optional[int] = None
    response_preview: Optional[str] = None


@dataclass
class PriceParsingStats:
    total_count: int = 0
    processed_count: int = 0
    success_count: int = 0
    error_count: int = 0
    invalid_token_count: int = 0
    token_refresh_attempts: int = 0
    token_refresh_success: int = 0
    token_refresh_failed: int = 0
    recovered_after_refresh: int = 0
    failed_after_refresh: int = 0
    started_at: Optional[datetime] = None
    start_perf_counter: Optional[float] = None


class WildberriesService:
    """Асинхронный сервис для работы с Wildberries API с эмуляцией браузера Chrome"""

    _price_log_sink_id: Optional[int] = None
=======
    CONCURRENT_REQUESTS_LIMIT,
    WB_CIRCUIT_COOLDOWN,
    WB_DEFAULT_DEST,
    WB_DETAIL_URL,
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
    """Сервис HTTP-взаимодействия с Wildberries.
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507

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
<<<<<<< HEAD
        self.semaphore = None
        self.price_logger = logger.bind(component="wb_price_parser")
        self.price_stats = PriceParsingStats()
        self.price_errors_csv_path = self._get_price_errors_csv_path()
        token_provider = WbTokenProvider(
            user_agent=WB_USER_AGENT,
            url=WB_TOKEN_REFRESH_URL,
            cookie_name=WB_TOKEN_COOKIE_NAME,
            max_attempts=WB_TOKEN_REFRESH_MAX_ATTEMPTS,
            wait_seconds=WB_TOKEN_REFRESH_WAIT_SECONDS,
        )
        self.cookie_manager = WbCookieManager(
            raw_cookies=WB_RAW_COOKIES,
            token_provider=token_provider,
            cookie_name=WB_TOKEN_COOKIE_NAME,
            auto_refresh_enabled=WB_TOKEN_AUTO_REFRESH_ENABLED,
        )
        self.token_refresh_lock = asyncio.Lock()
        self.consecutive_498_errors = 0
        self.stop_due_to_498 = False

    async def initialize(self):
        """
        Инициализация асинхронной сессии с эмуляцией TLS-отпечатка Chrome.
        Использует только базовые параметры для гарантированной совместимости 
        с любой версией библиотеки curl_cffi.
        """
        # Создаем сессию без использования дополнительных словарей (config/options),
        # чтобы избежать ошибок TypeError на разных версиях библиотеки.
        self.session = AsyncSession(
            impersonate="chrome110", 
            timeout=WB_TIMEOUT
        )
        
        # Инициализируем семафор для контроля количества одновременных запросов
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
        
        self.logger.info("Инициализирован универсальный сервис Wildberries (Режим максимальной совместимости)")

    def setup_price_logging(self) -> None:
        """Настраивает отдельный лог-файл и CSV для ошибок парсинга цен."""
        log_dir = Path(LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)

        if WildberriesService._price_log_sink_id is None:
            # Для основного price-лога используем loguru rotation/retention:
            # файл ротируется по дням, а старые архивы удаляются по сроку хранения.
            # Отдельный filter не смешивает эти записи с общим логом приложения.
            WildberriesService._price_log_sink_id = logger.add(
                log_dir / WB_PRICE_LOG_FILE,
                level="INFO",
                encoding="utf-8",
                rotation="1 day",
                retention=f"{LOG_RETENTION_DAYS} days",
                enqueue=True,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
                filter=lambda record: record["extra"].get("component") == "wb_price_parser",
            )

        self._ensure_price_errors_csv()

    def _get_price_errors_csv_path(self) -> Path:
        """Возвращает daily CSV ошибок, чтобы один файл не рос бесконечно."""
        base_path = Path(LOG_DIR) / WB_PRICE_ERRORS_CSV
        date_suffix = datetime.now().strftime("%Y-%m-%d")
        return base_path.with_name(f"{base_path.stem}_{date_suffix}{base_path.suffix}")

    def _ensure_price_errors_csv(self) -> None:
        """Создаёт CSV ошибок за текущий день и пишет заголовок один раз."""
        self.price_errors_csv_path = self._get_price_errors_csv_path()
        if not self.price_errors_csv_path.exists():
            with self.price_errors_csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([
                    "article_id",
                    "error_type",
                    "error_message",
                    "status_code",
                    "response_preview",
                    "created_at",
                ])

    def start_price_parsing(
            self,
            total_count: int,
            mode: Optional[str] = None,
            source: Optional[str] = None
    ) -> None:
        self.setup_price_logging()
        self.price_stats = PriceParsingStats(
            total_count=total_count,
            started_at=datetime.now(),
            start_perf_counter=perf_counter(),
        )
        self.price_logger.info("🚀 Старт парсинга цен WB")
        self.price_logger.info(f"Дата и время старта: {self.price_stats.started_at:%Y-%m-%d %H:%M:%S}")
        self.price_logger.info(f"Всего артикулов к обработке: {total_count}")
        if mode:
            self.price_logger.info(f"Режим работы: {mode}")
        if source:
            self.price_logger.info(f"Источник списка артикулов: {source}")

    def finish_price_parsing(self) -> None:
        if not self.price_stats.started_at or self.price_stats.start_perf_counter is None:
            return

        elapsed_seconds = int(perf_counter() - self.price_stats.start_perf_counter)
        elapsed = str(datetime.utcfromtimestamp(elapsed_seconds).time())
        self.price_logger.info("🏁 Парсинг цен WB завершён")
        self.price_logger.info(f"Всего артикулов: {self.price_stats.total_count}")
        self.price_logger.info(f"Обработано: {self.price_stats.processed_count}")
        self.price_logger.info(f"Успешно: {self.price_stats.success_count}")
        self.price_logger.info(f"Ошибок: {self.price_stats.error_count}")
        self.price_logger.info(f"HTTP 498: {self.price_stats.invalid_token_count}")
        self.price_logger.info(f"Попыток refresh cookies: {self.price_stats.token_refresh_attempts}")
        self.price_logger.info(f"Успешных refresh cookies: {self.price_stats.token_refresh_success}")
        self.price_logger.info(f"Неуспешных refresh cookies: {self.price_stats.token_refresh_failed}")
        self.price_logger.info(f"Восстановлено после refresh: {self.price_stats.recovered_after_refresh}")
        self.price_logger.info(f"Ошибок после refresh: {self.price_stats.failed_after_refresh}")
        self.price_logger.info(f"Время выполнения: {elapsed}")

    def log_price_success(self, article_id: int, parsed_data: ProductDetails) -> None:
        # Успешные артикула пишем только в лог: так оператор быстро видит цену,
        # но CSV остаётся компактным и содержит только проблемные случаи.
        self.price_stats.processed_count += 1
        self.price_stats.success_count += 1
        raw_data = parsed_data.raw_data or {}
        message = (
            f"✅ Успешно | article_id={article_id} | price={parsed_data.price}"
        )
        if raw_data.get("brand"):
            message += f" | brand={raw_data.get('brand')}"
        if raw_data.get("name"):
            message += f" | name={raw_data.get('name')}"
        if raw_data.get("reviewRating"):
            message += f" | rating={raw_data.get('reviewRating')}"
        self.price_logger.info(message)

    def log_price_error(
            self,
            article_id: int,
            error_type: str,
            error_message: str,
            status_code: Optional[int] = None,
            response_preview: Optional[str] = None
    ) -> None:
        # Ошибки пишем и в лог, и в daily CSV. CSV удобен для фильтрации в Excel
        # или Google Sheets, поэтому туда попадают только безопасные поля без
        # cookies, токенов и authorization headers.
        self.setup_price_logging()
        self.price_stats.processed_count += 1
        self.price_stats.error_count += 1

        log_message = (
            f"❌ {error_message} | article_id={article_id}"
            f" | error_type={error_type}"
        )
        if status_code is not None:
            log_message += f" | status={status_code}"
        if response_preview:
            log_message += f" | response_preview={response_preview}"
        self.price_logger.error(log_message)

        try:
            safe_preview = (response_preview or "")[:500].replace("\r", " ").replace("\n", " ")
            with self.price_errors_csv_path.open("a", newline="", encoding="utf-8-sig") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([
                    article_id,
                    error_type,
                    error_message,
                    status_code or "",
                    safe_preview,
                    datetime.now().isoformat(timespec="seconds"),
                ])
        except Exception as exc:
            self.price_logger.error(f"Не удалось записать ошибку в CSV | article_id={article_id} | error={exc}")

    async def close(self):
        """Закрытие сессии после завершения работы"""
=======

        # Глобальный лимитер конкурентности запросов.
        # WARNING: увеличение лимита может изменить timing profile и trigger anti-bot.
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
        self.current_concurrency_limit = CONCURRENT_REQUESTS_LIMIT

        # Retry/timeout параметры.
        self.max_retries = max(1, WB_MAX_RETRIES)
        self.base_retry_delay = max(0.2, float(WB_RETRY_DELAY))
        self.timeout = max(1, int(WB_TIMEOUT))

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

    async def initialize(self) -> None:
        """Создаёт постоянную AsyncSession.

        Почему `curl_cffi.AsyncSession`:
        - позволяет контролировать TLS fingerprint через `impersonate`;
        - в практике этого проекта даёт более устойчивый доступ к WB.

        Почему `impersonate="chrome120"`:
        - это проверенный baseline для текущего окружения;
        - смена версии может изменить fingerprint и антибот-реакции.
        """
        self.session = AsyncSession(
            impersonate="chrome120",
            timeout=self.timeout,
            headers=self.default_headers,
        )
        self.logger.info("Сервис WB инициализирован, создана постоянная async-сессия")

    async def close(self) -> None:
        """Закрывает AsyncSession.

        Побочный эффект:
        - прекращаются keep-alive соединения, следующий запуск создаст новую сессию.
        """
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
        if self.session:
            await self.session.close()
            self.logger.info("Сессия сервиса WB закрыта")

<<<<<<< HEAD
    @staticmethod
    def _response_preview(response: Any) -> str:
        try:
            return (response.text or "")[:500]
        except Exception:
            return ""

    def _build_headers(self, referer_article: Optional[int]) -> Dict[str, str]:
        # Cookie header собирается из WbCookieManager. Это позволяет заменить
        # x_wbaas_token в памяти после HTTP 498 без ручной правки config.py.
        headers = {
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
            "Connection": "keep-alive",
            "Cookie": self.cookie_manager.get_cookies(),
            "Origin": "https://www.wildberries.ru",
            "User-Agent": WB_USER_AGENT
        }

        if referer_article:
            headers["Referer"] = f"https://www.wildberries.ru/catalog/{referer_article}/detail.aspx"

        return headers

    async def _refresh_cookies_after_498(self, article_id: Optional[int]) -> bool:
        # Несколько async-задач могут одновременно получить 498. Lock нужен,
        # чтобы SeleniumBase запускала только одна задача, а остальные дождались
        # результата и повторили запрос уже с обновлённым токеном в памяти.
        current_token = self.cookie_manager.extract_cookie_value(
            self.cookie_manager.get_cookies(),
            WB_TOKEN_COOKIE_NAME,
        )
        async with self.token_refresh_lock:
            refreshed_token = self.cookie_manager.extract_cookie_value(
                self.cookie_manager.get_cookies(),
                WB_TOKEN_COOKIE_NAME,
            )
            if refreshed_token and refreshed_token != current_token:
                self.price_logger.info(
                    "WB cookies уже обновлены другой задачей | article_id={} | x_wbaas_token={}",
                    article_id,
                    self.cookie_manager.mask_token(refreshed_token),
                )
                return True

            self.price_stats.token_refresh_attempts += 1
            self.price_logger.warning(
                "Запускаем обновление x_wbaas_token через SeleniumBase | article_id={}",
                article_id,
            )
            refreshed = self.cookie_manager.refresh_x_wbaas_token()
            if refreshed:
                self.price_stats.token_refresh_success += 1
                self.price_logger.info(
                    "WB cookies обновлены | article_id={} | x_wbaas_token={} | changed={}",
                    article_id,
                    self.cookie_manager.get_masked_token(),
                    self.cookie_manager.last_refresh_changed,
                )
                return True

            self.price_stats.token_refresh_failed += 1
            self.price_logger.error(
                "Не удалось обновить x_wbaas_token через SeleniumBase | article_id={}",
                article_id,
            )
            return False

    def _register_498_error(self, article_id: Optional[int]) -> None:
        # HTTP 498 в текущем WB-сценарии обычно означает невалидный или устаревший
        # x_wbaas_token. Ограничитель подряд идущих 498 защищает от ситуации, когда
        # cookies в файле тоже старые, а парсер продолжает нагружать WB API.
        self.price_stats.invalid_token_count += 1
        self.consecutive_498_errors += 1
        if self.consecutive_498_errors >= MAX_CONSECUTIVE_498_ERRORS:
            self.stop_due_to_498 = True
            self.price_logger.critical(
                "Слишком много подряд 498 ошибок: {}. Останавливаем WB-запросы, вероятно cookies невалидны.",
                self.consecutive_498_errors,
            )

    async def _make_request(self, url: str, params: Dict[str, Any], referer_article: Optional[int] = None) -> WbRequestResult:
        """
        Выполняет асинхронный запрос к API с повторными попытками.
        Проверяет статус токена защиты (кук).
        """
        retries = 0
        token_refresh_retries = 0
        retried_after_refresh = False

        if self.stop_due_to_498:
            return WbRequestResult(
                error_type="invalid_wbaas_token",
                error_message="WB-запросы остановлены: слишком много подряд HTTP 498",
                status_code=498,
            )

        while retries < WB_MAX_RETRIES:
            try:
                async with self.semaphore:
                    headers = self._build_headers(referer_article)
                    response = await self.session.get(url, params=params, headers=headers)
                    
                    if response.status_code == 200:
                        self.consecutive_498_errors = 0
                        if retried_after_refresh:
                            self.price_stats.recovered_after_refresh += 1
                            self.price_logger.info(
                                "Повторный запрос успешен после refresh | article_id={}",
                                referer_article,
                            )
                        try:
                            return WbRequestResult(data=response.json())
                        except Exception as exc:
                            return WbRequestResult(
                                error_type="json_error",
                                error_message=f"Ошибка JSON: {exc}",
                                status_code=response.status_code,
                                response_preview=self._response_preview(response),
                            )
                        
                    elif response.status_code == 498:
                        self._register_498_error(referer_article)
                        self.price_logger.warning(
                            "498 Unauthorized/Invalid token | article_id={} | retry={}",
                            referer_article,
                            token_refresh_retries,
                        )
                        if self.stop_due_to_498:
                            return WbRequestResult(
                                error_type="invalid_wbaas_token",
                                error_message="Получен HTTP 498. Превышен лимит подряд идущих 498.",
                                status_code=response.status_code,
                                response_preview=self._response_preview(response),
                            )
                        refresh_limit = min(
                            MAX_TOKEN_REFRESH_RETRIES,
                            WB_TOKEN_REFRESH_MAX_RETRIES_PER_ARTICLE,
                        )
                        if token_refresh_retries < refresh_limit:
                            # Refresh ограничен: бесконечные повторы не исправят
                            # невалидные cookies, зато могут привести к лавине 498.
                            token_refresh_retries += 1
                            refreshed = await self._refresh_cookies_after_498(referer_article)
                            if refreshed:
                                retried_after_refresh = True
                                retries += 1
                                continue

                        if retried_after_refresh:
                            self.price_stats.failed_after_refresh += 1
                            self.price_logger.error(
                                "Повторный запрос после refresh снова завершился ошибкой | article_id={} | status=498",
                                referer_article,
                            )
                        return WbRequestResult(
                            error_type="invalid_wbaas_token",
                            error_message="Получен HTTP 498. Вероятно устарел x_wbaas_token/cookies.",
                            status_code=response.status_code,
                            response_preview=self._response_preview(response),
                        )
                        
                    elif response.status_code == 429:
                        self.logger.warning(f"Rate Limit (429). Ждем {WB_RATE_LIMIT_DELAY}с...")
                        last_error = WbRequestResult(
                            error_type="request_error",
                            error_message="Антибот/429: превышен лимит запросов",
                            status_code=response.status_code,
                            response_preview=self._response_preview(response),
                        )
                        await asyncio.sleep(WB_RATE_LIMIT_DELAY)
                        retries += 1
                        continue
                    else:
                        self.logger.warning(f"Статус {response.status_code} при запросе к {url}")
                        if retried_after_refresh:
                            self.price_stats.failed_after_refresh += 1
                            self.price_logger.error(
                                "Повторный запрос после refresh завершился ошибкой | article_id={} | status={}",
                                referer_article,
                                response.status_code,
                            )
                        return WbRequestResult(
                            error_type="request_error",
                            error_message=f"Ошибка запроса: HTTP {response.status_code}",
                            status_code=response.status_code,
                            response_preview=self._response_preview(response),
                        )
            except Timeout as e:
                return WbRequestResult(
                    error_type="timeout",
                    error_message=f"Таймаут: {e}",
                )
            except Exception as e:
                self.logger.error(f"Ошибка сети при запросе к API: {e}")
                last_error = WbRequestResult(
                    error_type="request_error",
                    error_message=f"Ошибка запроса: {e}",
                )

            retries += 1
            if retries < WB_MAX_RETRIES:
                sleep_delay = (WB_RETRY_DELAY * retries) + random.uniform(0.3, 1.2)
                await asyncio.sleep(sleep_delay)

        return locals().get(
            "last_error",
            WbRequestResult(error_type="unknown_error", error_message="Неизвестная ошибка запроса")
        )

    async def get_product_details(self, product_id: int) -> Optional[ProductDetails]:
        """Получает информацию о товаре по его ID"""
        params = {
            "appType": 1,
            "curr": "rub",
            "dest": WB_DEFAULT_DEST,
            "nm": product_id
=======
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
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
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

<<<<<<< HEAD
        try:
            request_result = await self._make_request(WB_DETAIL_URL, params, referer_article=product_id)
            response_data = request_result.data
            if not response_data:
                self.log_price_error(
                    article_id=product_id,
                    error_type=request_result.error_type or "empty_response",
                    error_message=request_result.error_message or "Пустой ответ",
                    status_code=request_result.status_code,
                    response_preview=request_result.response_preview,
                )
                return None

            products = response_data.get('products', [])
            if not products:
                self.logger.warning(f"Товар {product_id} не найден в ответе Wildberries")
                self.log_price_error(
                    article_id=product_id,
                    error_type="empty_response",
                    error_message="Пустой ответ: products отсутствует или пустой",
                    response_preview=str(response_data)[:500],
                )
                return None

            product = products[0]

            price = None
            if product.get('sizes') and len(product['sizes']) > 0:
                price_data = product['sizes'][0].get('price', {})
                product_price_kopecks = price_data.get('product')
                if product_price_kopecks:
                    price = product_price_kopecks // 100

            if price is None:
                self.log_price_error(
                    article_id=product_id,
                    error_type="price_not_found",
                    error_message="Цена не найдена в ответе",
                    response_preview=f"available_keys={list(product.keys())}",
                )

            product_details = ProductDetails(
                id=product['id'],
                name=product.get('name', ''),
                brand=product.get('brand', ''),
                price=price,
                raw_data=product
            )
            if price is not None:
                self.log_price_success(product_id, product_details)
            return product_details
        except Exception as e:
            self.logger.error(f"Ошибка при обработке данных о товаре {product_id}: {e}")
            self.log_price_error(
                article_id=product_id,
                error_type="unknown_error",
                error_message=f"Ошибка обработки данных: {e}",
            )
            return None

    async def get_similar_products(self, product_details: ProductDetails) -> SimilarProductsResult:
        """Получает список похожих товаров"""
        if not product_details:
=======
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
        response = await self._request_with_retry(
            url=WB_SIMILAR_URL,
            endpoint="recom_search",
            request_id=request_id,
            params=params,
            context={"article_id": product.id},
        )
        if not response.ok:
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
            return SimilarProductsResult(
                original_product=product,
                similar_products=[],
<<<<<<< HEAD
                error="Отсутствуют входные данные о товаре"
            )

        try:
            product_id = product_details.id
            product_name = product_details.name

            params = {
                "q1": f"nm{product_id}key {product_name}",
                "query": f"похожие {product_id}",
                "resultset": "catalog",
                "spp": 30,
                "curr": "rub",
                "dest": WB_DEFAULT_DEST
            }
            
            request_result = await self._make_request(WB_SIMILAR_URL, params, referer_article=product_id)
            response_data = request_result.data
            if not response_data:
                return SimilarProductsResult(
                    original_product=product_details,
                    similar_products=[],
                    error="Не удалось получить данные о похожих товарах"
                )
                
            similar_products = []
            if 'data' in response_data and 'products' in response_data['data']:
                similar_products = response_data['data']['products']
                self.logger.info(f"Получено {len(similar_products)} похожих товаров для артикула {product_id}")
            else:
                self.logger.warning(f"Похожие товары не найдены для артикула {product_id}")

            return SimilarProductsResult(
                original_product=product_details,
                similar_products=similar_products
            )
        except Exception as e:
            error_msg = f"Ошибка при получении похожих товаров для артикула {product_id}: {e}"
            self.logger.error(error_msg)
            return SimilarProductsResult(
                original_product=product_details,
=======
                error=f"{response.status_class}:{response.error or ''}".rstrip(":"),
            )

        payload = response.payload or {}
        products = payload.get("data", {}).get("products", [])
        if not isinstance(products, list):
            return SimilarProductsResult(
                original_product=product,
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
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
<<<<<<< HEAD
        """Находит наш артикул среди похожих товаров"""
        if not similar_result or similar_result.error or not similar_result.similar_products or not our_articles:
            return None, None

        for position, product in enumerate(similar_result.similar_products, 1):
            product_id = product.get('id')
            if product_id in our_articles:
                self.logger.info(f"МАТЧ! Найден наш артикул {product_id} на позиции {position}")
                return product_id, position
=======
        """Ищет первый наш артикул в списке похожих товаров.
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507

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

    async def _request_with_retry(
        self,
        url: str,
        endpoint: str,
        request_id: str,
        params: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> WBRequestResult:
        """Выполняет HTTP-запрос с retry/backoff/throttle/circuit-breaker.

        Это ключевая anti-bot чувствительная функция.
        Любые изменения в порядке вызовов, паузах или лимитах должны проверяться
        на реальных логах с метриками forbidden/rate_limited.
        """
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
            # RPS-тормоз и небольшой jitter снижают burst-паттерн.
            await self._throttle()
            await asyncio.sleep(random.uniform(0.01, 0.08))
            start = time.monotonic()
            try:
                # semaphore удерживает верхнюю границу конкурентности.
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
