"""
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
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import Timeout
from loguru import logger

from src.config import (
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

    def __init__(self):
        self.session = None
        self.logger = logger
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
        if self.session:
            self.session.close()
            self.logger.info("Сессия Wildberries успешно закрыта")

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
        }

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
            return SimilarProductsResult(
                original_product=ProductDetails(id=0, name="", brand=""),
                similar_products=[],
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
                similar_products=[],
                error=error_msg
            )

    def find_our_article_in_similar(
            self,
            similar_result: SimilarProductsResult,
            our_articles: Set[int]
    ) -> Tuple[Optional[int], Optional[int]]:
        """Находит наш артикул среди похожих товаров"""
        if not similar_result or similar_result.error or not similar_result.similar_products or not our_articles:
            return None, None

        for position, product in enumerate(similar_result.similar_products, 1):
            product_id = product.get('id')
            if product_id in our_articles:
                self.logger.info(f"МАТЧ! Найден наш артикул {product_id} на позиции {position}")
                return product_id, position

        return None, None
