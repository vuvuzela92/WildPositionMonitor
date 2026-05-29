"""Основной orchestration-модуль мониторинга Wildberries.

Архитектурная роль модуля:
- управляет жизненным циклом клиентов БД и HTTP-сервиса WB;
- обрабатывает артикулы батчами и сохраняет результаты;
- ведёт runtime-метрики и checkpoint для восстановления;
- адаптирует конкурентность по сигналам rate-limit/forbidden.

WARNING:
Модуль чувствителен к:
- timing patterns (ритм запросов),
- retry behavior,
- concurrency level,
- session lifecycle WB-сервиса.

Изменения этих аспектов без анализа production-логов могут привести
к росту 403/429 и снижению полноты данных.
"""

from __future__ import annotations

import asyncio
import json
from asyncio import to_thread
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import quantiles
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from loguru import logger

from src.config import (
<<<<<<< HEAD
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB,
    BATCH_SIZE, DATA_SOURCE, GOOGLE_SHEET_NAME,
    LOG_DIR, LOG_CLEANUP_ENABLED, LOG_CLEANUP_INTERVAL_HOURS,
    LOG_RETENTION_DAYS, LOG_CLEANUP_STATE_FILE
=======
    ADAPTIVE_WINDOW_SIZE,
    BATCH_SIZE,
    CHECKPOINT_FILE_PATH,
    CLICKHOUSE_DB,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CONCURRENCY_STEP,
    CONCURRENT_REQUESTS_LIMIT,
    GOOGLE_SHEET_NAME,
    MIN_CONCURRENT_REQUESTS_LIMIT,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
)
from src.data_models import ProcessingResult, RuntimeMetrics
from src.db.clickhouse_client import ClickHouseClient
<<<<<<< HEAD
from src.log_cleanup import cleanup_old_log_files
=======
from src.db.postgres_client import PostgresClient
from src.logger import setup_logger
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
from src.services.wb_service import WildberriesService
from src.utils.google_sheets_reader import GoogleSheetsReader


@dataclass
class CheckpointState:
    """Состояние checkpoint для восстановления после падений.

    Поля:
    - `pending`: артикулы, ожидающие обработки;
    - `in_progress`: артикулы, обрабатываемые прямо сейчас;
    - `done`: успешно обработанные артикулы;
    - `failed_retriable`: счётчик ретраев для временных ошибок;
    - `failed_terminal`: артикулы с финальной ошибкой.
    """

    pending: Set[int] = field(default_factory=set)
    in_progress: Set[int] = field(default_factory=set)
    done: Set[int] = field(default_factory=set)
    failed_retriable: Dict[int, int] = field(default_factory=dict)
    failed_terminal: Set[int] = field(default_factory=set)


class CheckpointStore:
    """Файловое хранилище checkpoint в JSON-формате."""

    def __init__(self, file_path: str) -> None:
        """Инициализирует путь хранения checkpoint-файла."""
        self.path = Path(file_path)

    def load(self) -> CheckpointState:
        """Загружает checkpoint из файла.

        Возвращает:
        - `CheckpointState` из файла;
        - пустое состояние, если файл отсутствует/повреждён.

        Риск:
        - повреждённый JSON не должен падать в исключение на уровне запуска,
          иначе hourly-run может полностью остановиться.
        """
        if not self.path.exists():
            return CheckpointState()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            return CheckpointState(
                pending={int(x) for x in payload.get("pending", [])},
                in_progress={int(x) for x in payload.get("in_progress", [])},
                done={int(x) for x in payload.get("done", [])},
                failed_retriable={int(k): int(v) for k, v in payload.get("failed_retriable", {}).items()},
                failed_terminal={int(x) for x in payload.get("failed_terminal", [])},
            )
        except Exception as exc:
            logger.error("Ошибка загрузки checkpoint: path={} error={}", self.path, exc)
            return CheckpointState()

    def save(self, state: CheckpointState) -> None:
        """Сохраняет checkpoint в JSON-файл (UTF-8).

        Побочный эффект:
        - создаёт родительскую директорию при необходимости.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pending": sorted(state.pending),
            "in_progress": sorted(state.in_progress),
            "done": sorted(state.done),
            "failed_retriable": {str(k): v for k, v in state.failed_retriable.items()},
            "failed_terminal": sorted(state.failed_terminal),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# Глобальные singleton-зависимости (legacy-паттерн проекта).
# WARNING: их жизненный цикл завязан на жизненный цикл процесса.
psql_client = PostgresClient(
    {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "database": POSTGRES_DB,
    }
)
click_client = ClickHouseClient(
    {
        "host": CLICKHOUSE_HOST,
        "port": CLICKHOUSE_PORT,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "database": CLICKHOUSE_DB,
    }
)
wb_serv = WildberriesService()


class WildPosition:
    """Оркестратор процесса мониторинга позиций.

    Класс объединяет:
    - подготовку входных данных,
    - async-обработку товаров,
    - сохранение результатов,
    - адаптивную регулировку конкурентности.

    WARNING (legacy):
    Реализован как singleton. Не меняйте этот паттерн без анализа всех
    side-effect в точке входа и тестах, иначе можно получить дубли инициализации.
    """

    __instance = None

    def __init__(
        self,
        postgres_client: PostgresClient,
        clickhouse_client: ClickHouseClient,
        wb_service: WildberriesService,
    ) -> None:
        """Инициализирует зависимости оркестратора и runtime-состояние."""
        self.postgres_client = postgres_client
        self.clickhouse_client = clickhouse_client
        self.wb_service = wb_service
<<<<<<< HEAD
    
    async def run(self, articles_data: List[Dict[str, Any]]) -> bool | None:
        """
        Запускает процесс мониторинга для списка артикулов
        
        Args:
            articles_data: Список словарей с информацией об артикулах
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        logger.info("Запуск мониторинга товаров Wildberries")
        
        try:
            # Очистка служебных .log/.csv запускается один раз на старт процесса.
            # Она не должна выполняться внутри цикла по артикулам, иначе логирование
            # и запись CSV ошибок будут конкурировать с удалением файлов.
            try:
                cleanup_old_log_files(
                    log_dir=LOG_DIR,
                    retention_days=LOG_RETENTION_DAYS,
                    cleanup_state_file=LOG_CLEANUP_STATE_FILE,
                    cleanup_interval_hours=LOG_CLEANUP_INTERVAL_HOURS,
                    enabled=LOG_CLEANUP_ENABLED,
                )
            except Exception as cleanup_error:
                logger.warning(f"Не удалось выполнить очистку логов: {cleanup_error}")

            # Асинхронное подключение к PostgreSQL
            if not await self.postgres_client.connect():
                logger.error("Ошибка подключения к PostgreSQL")
                return False
                
            # Синхронное подключение к ClickHouse
            if not self.clickhouse_client.connect():
                logger.error("Ошибка подключения к ClickHouse")
                return False
=======
        self.metrics = RuntimeMetrics()
        self.current_concurrency = CONCURRENT_REQUESTS_LIMIT
        self.max_retry_per_item = 2
        self.checkpoint_store = CheckpointStore(CHECKPOINT_FILE_PATH)
        self.checkpoint_state = self.checkpoint_store.load()
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507

    async def run(self, articles_data: List[Dict[str, Any]]) -> bool:
        """Запускает полный цикл мониторинга.

        Параметры:
        - `articles_data`: входной список словарей с `article_id` и контекстом.

        Возвращает:
        - `True`, если pipeline выполнен (даже если часть артикулов с ошибками);
        - `False`, если произошла критическая ошибка инициализации/выполнения.

        Побочные эффекты:
        - сетевые запросы к WB;
        - чтение/запись в PostgreSQL/ClickHouse;
        - запись checkpoint и runtime-логов.
        """
        logger.info("Запуск мониторинга WB, входных элементов={}", len(articles_data))
        try:
            if not await self.postgres_client.connect():
                logger.error("Не удалось подключиться к PostgreSQL")
                return False
            if not self.clickhouse_client.connect():
                logger.error("Не удалось подключиться к ClickHouse")
                return False
            await self.wb_service.initialize()
<<<<<<< HEAD
            source_name = (
                f"google_sheets:{GOOGLE_SHEET_NAME}"
                if DATA_SOURCE == "google_sheets"
                else DATA_SOURCE
            )
            self.wb_service.start_price_parsing(
                total_count=len(articles_data),
                mode="monitoring",
                source=source_name,
            )
            
            # Получаем список наших артикулов (асинхронно)
=======

            filtered_articles = self._prepare_articles_for_run(articles_data)
            if not filtered_articles:
                logger.warning("Нет артикулов для обработки после фильтра checkpoint")
                return True

>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
            our_articles = await self.postgres_client.get_our_articles()
            if not our_articles:
                logger.error("Не удалось получить список наших артикулов")
                return False

            total_batches = (len(filtered_articles) + BATCH_SIZE - 1) // BATCH_SIZE
            for i in range(0, len(filtered_articles), BATCH_SIZE):
                batch = filtered_articles[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                logger.info("Старт батча {}/{} (размер={})", batch_num, total_batches, len(batch))

                # Async-обработка батча и sync-запись в ClickHouse через thread pool,
                # чтобы не блокировать event loop.
                batch_results = await self._process_batch(batch, our_articles, batch_num=batch_num)
                await to_thread(self.clickhouse_client.save_results, batch_results)

                # Обновляем состояние восстановления и адаптируем конкурентность
                # только после фиксации результатов текущего батча.
                self._update_checkpoint_after_batch(batch_results)
                await self._adapt_concurrency(batch_results)
                self._log_batch_metrics(batch_num, total_batches, batch_results)

            self._log_final_metrics()
            logger.info("Мониторинг WB завершен успешно, всего обработано={}", len(filtered_articles))
            return True
        except Exception as exc:
            logger.exception("Мониторинг WB завершился с ошибкой: {}", exc)
            return False
        finally:
<<<<<<< HEAD
            # Закрываем соединения
            self.wb_service.finish_price_parsing()
=======
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
            await self._close_connections()

    async def _close_connections(self) -> None:
        """Закрывает все внешние ресурсы в контролируемом порядке."""
        logger.info("Начато закрытие ресурсов")
        await self.wb_service.close()
        await self.postgres_client.close()
        self.clickhouse_client.close()
        logger.info("Закрытие ресурсов завершено")

    def _prepare_articles_for_run(self, articles_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Подготавливает итоговый список артикулов к запуску.

        Логика:
        - синхронизирует вход с checkpoint;
        - добавляет retriable-ошибки в повторную обработку;
        - исключает terminal-ошибки;
        - делает reset checkpoint, если состояние полностью "выгорело".

        WARNING:
        Поведение reset нужно для hourly-run. Удаление этого блока приведёт
        к ситуации, когда периодический запуск перестанет обрабатывать вход.
        """
        indexed = {int(item["article_id"]): item for item in articles_data if item.get("article_id")}
        if not self.checkpoint_state.pending:
            self.checkpoint_state.pending = set(indexed.keys())
        else:
            self.checkpoint_state.pending = {
                article_id
                for article_id in self.checkpoint_state.pending
                if article_id in indexed
            }

        recoverable_failed = {
            article_id
            for article_id, retries in self.checkpoint_state.failed_retriable.items()
            if retries <= self.max_retry_per_item
        }
        target_ids = (self.checkpoint_state.pending | recoverable_failed) - self.checkpoint_state.done
        target_ids -= self.checkpoint_state.failed_terminal

        # Если checkpoint полностью "выжег" список, но входные данные есть,
        # выполняем полный проход заново (важно для периодического hourly-run).
        if not target_ids and indexed:
            logger.warning(
                "Checkpoint не содержит задач для обработки, выполняем сброс состояния и полный проход по текущему входу"
            )
            self.checkpoint_state = CheckpointState(pending=set(indexed.keys()))
            target_ids = set(indexed.keys())

        self.checkpoint_store.save(self.checkpoint_state)
        return [indexed[article_id] for article_id in target_ids if article_id in indexed]

    async def _process_batch(
        self,
        articles_data: List[Dict[str, Any]],
        our_articles: Set[int],
        batch_num: int,
    ) -> List[ProcessingResult]:
        """Параллельно обрабатывает один батч артикулов.

        Почему `asyncio.gather`:
        - сохраняется управляемая конкурентность внутри WB-сервиса через semaphore;
        - оркестратор остаётся простым и не дублирует ограничение конкуренции.
        """
        tasks = [
            self._process_single_article(article_info, our_articles, batch_num=batch_num)
            for article_info in articles_data
        ]
        results = await asyncio.gather(*tasks)
        return results

    async def _process_single_article(
        self,
        article_info: Dict[str, Any],
        our_articles: Set[int],
        batch_num: int,
    ) -> ProcessingResult:
        """Обрабатывает один артикул: detail -> parse -> similar -> match.

        Параметры:
        - `article_info`: входной словарь с артикулом и вспомогательными полями;
        - `our_articles`: множество наших артикулов для быстрого поиска;
        - `batch_num`: номер батча (используется в request_id и логах).

        Возвращает:
        - `ProcessingResult` с данными или ошибкой.

        Побочные эффекты:
        - HTTP-запросы в WB;
        - изменения checkpoint state (`in_progress`/`pending`).
        """
        article_id = int(article_info["article_id"])
        request_id = f"{batch_num}-{article_id}-{uuid4().hex[:8]}"
        wild_value = article_info.get("wild", "")
        competitor_status = article_info.get("competitor_status", "")

        # Фиксируем переход артикула в in_progress до начала сетевых вызовов.
        self.checkpoint_state.in_progress.add(article_id)
        self.checkpoint_state.pending.discard(article_id)
        logger.debug(
            "Старт обработки артикула: request_id={} article_id={} batch_num={}",
            request_id,
            article_id,
            batch_num,
        )
        try:
            product_response = await self.wb_service.get_product_details(article_id, request_id=request_id)
            self._collect_http_metrics(product_response, stage="detail")
            if not product_response.ok:
                return self._failed_result(
                    article_id=article_id,
                    status=product_response.status_class,
                    message=product_response.error or "ошибка_получения_товара",
                    wild=wild_value,
                    competitor_status=competitor_status,
                )

            payload = product_response.payload or {}
            product = self.wb_service.parse_product_details(payload)
            if not product:
                return self._failed_result(
                    article_id=article_id,
                    status="parse_error",
                    message="некорректный_ответ_товара",
                    wild=wild_value,
                    competitor_status=competitor_status,
                )
            if product.price is None:
                logger.warning(
                    "Цена не распарсена: request_id={} article_id={} product_id={} price=None",
                    request_id,
                    article_id,
                    product.id,
                )
            else:
                logger.info(
                    "Цена распарсена: request_id={} article_id={} product_id={} price={}",
                    request_id,
                    article_id,
                    product.id,
                    product.price,
                )

            similar = await self.wb_service.get_similar_products(product, request_id=request_id)
            self._collect_similar_metrics(similar)
            if similar.error:
                return self._failed_result(
                    article_id=article_id,
                    status="similar_fetch_error",
                    message=similar.error,
                    wild=wild_value,
                    competitor_status=competitor_status,
                    price=product.price,
                )

            found_id, position = self.wb_service.find_our_article_in_similar(similar, our_articles)
            if found_id:
                logger.info(
                    "Найден наш артикул: request_id={} article_id={} found_article={} position={}",
                    request_id,
                    article_id,
                    found_id,
                    position,
                )
            self.metrics.batch_successful_items += 1
            logger.debug(
                "Обработка артикула завершена успешно: request_id={} article_id={}",
                request_id,
                article_id,
            )
            logger.info(
                "Итог по артикулу: article_id={} price={} status=ok found_article={} position={}",
                article_id,
                product.price,
                found_id,
                position,
            )
            return ProcessingResult(
                article_id=article_id,
                price=product.price,
                found_article=found_id,
                position=position,
                processed_at=datetime.now(),
                wild=wild_value,
                concurrent=competitor_status,
            )
        except Exception as exc:
            logger.exception(
                "Исключение при обработке артикула: request_id={} article_id={} batch_num={} error={}",
                request_id,
                article_id,
                batch_num,
                exc,
            )
            return self._failed_result(
                article_id=article_id,
                status="ошибка_обработки",
                message=str(exc),
                wild=wild_value,
                competitor_status=competitor_status,
            )
        finally:
            # Гарантируем очистку in_progress при любом исходе.
            self.checkpoint_state.in_progress.discard(article_id)

    def _failed_result(
        self,
        article_id: int,
        status: str,
        message: str,
        wild: str,
        competitor_status: str,
        price: Optional[int] = None,
    ) -> ProcessingResult:
        """Формирует единый объект ошибки и учитывает метрики batch-неудач."""
        self.metrics.batch_failed_items += 1
        logger.warning(
            "Обработка артикула завершилась ошибкой: article_id={} price={} status={} error={}",
            article_id,
            price,
            status,
            message,
        )
        return ProcessingResult(
            article_id=article_id,
            price=price,
            error=f"{status}:{message}",
            processed_at=datetime.now(),
            wild=wild,
            concurrent=competitor_status,
        )

    def _collect_http_metrics(self, response: Any, stage: str) -> None:
        """Агрегирует HTTP-метрики для финального отчёта по запуску."""
        self.metrics.total_requests += 1
        self.metrics.retries_total += int(response.retries_used)
        self.metrics.latencies_ms.append(int(response.latency_ms))
        if response.ok:
            self.metrics.successful_requests += 1
            if stage == "detail":
                self.metrics.detail_success_total += 1
        else:
            self.metrics.failed_requests += 1
            if stage == "detail":
                self.metrics.detail_failed_total += 1
        if response.status_class == "rate_limited":
            self.metrics.rate_limited_total += 1
        if response.status_class == "forbidden":
            self.metrics.forbidden_total += 1
        if response.status_class == "timeout":
            self.metrics.timeouts_total += 1
        if response.error == "circuit_open":
            self.metrics.short_circuited_total += 1

    def _collect_similar_metrics(self, similar: Any) -> None:
        """Агрегирует счётчики успешности этапа similar-поиска."""
        if getattr(similar, "error", None):
            self.metrics.similar_failed_total += 1
        else:
            self.metrics.similar_success_total += 1

    def _update_checkpoint_after_batch(self, batch_results: List[ProcessingResult]) -> None:
        """Обновляет checkpoint по результатам батча.

        Логика делит ошибки на retriable/terminal по текстовому контракту
        (`rate_limited`, `forbidden`, `timeout`, ...).

        WARNING:
        Это legacy-совместимый механизм. Изменение error-contract строк без
        синхронной правки WB-сервиса может сломать восстановление после падений.
        """
        for result in batch_results:
            article_id = int(result.article_id)
            if not result.error:
                self.checkpoint_state.done.add(article_id)
                self.checkpoint_state.failed_retriable.pop(article_id, None)
                self.checkpoint_state.failed_terminal.discard(article_id)
                continue

            is_retriable = any(
                key in result.error
                for key in ("rate_limited", "forbidden", "upstream_5xx", "timeout", "network_error", "circuit_open")
            )
            if is_retriable:
                retries = self.checkpoint_state.failed_retriable.get(article_id, 0) + 1
                self.checkpoint_state.failed_retriable[article_id] = retries
                if retries > self.max_retry_per_item:
                    self.checkpoint_state.failed_terminal.add(article_id)
                    self.checkpoint_state.failed_retriable.pop(article_id, None)
                else:
                    self.checkpoint_state.pending.add(article_id)
            else:
                self.checkpoint_state.failed_terminal.add(article_id)
                self.checkpoint_state.failed_retriable.pop(article_id, None)

        self.checkpoint_store.save(self.checkpoint_state)

    async def _adapt_concurrency(self, batch_results: List[ProcessingResult]) -> None:
        """Адаптивно регулирует конкурентность WB-запросов.

        Идея:
        - при росте forbidden/rate_limited снижаем параллелизм;
        - при стабильности постепенно повышаем обратно.

        WARNING:
        Это антибот-чувствительный механизм. Ускорение step-up или повышение
        верхнего лимита без анализа может изменить timing profile клиента.
        """
        if not batch_results:
            return
        window = batch_results[-ADAPTIVE_WINDOW_SIZE:]
        forbidden_or_rate_limited = sum(
            1
            for item in window
            if item.error and ("rate_limited" in item.error or "forbidden" in item.error)
        )
        ratio = forbidden_or_rate_limited / max(1, len(window))

        if ratio > 0.2 and self.current_concurrency > MIN_CONCURRENT_REQUESTS_LIMIT:
            self.current_concurrency = max(
                MIN_CONCURRENT_REQUESTS_LIMIT,
                self.current_concurrency - CONCURRENCY_STEP,
            )
            await self.wb_service.update_concurrency_limit(self.current_concurrency)
            logger.warning("Снижен лимит конкурентности: new_limit={} ratio={:.2f}", self.current_concurrency, ratio)
        elif ratio < 0.05 and self.current_concurrency < CONCURRENT_REQUESTS_LIMIT:
            self.current_concurrency = min(
                CONCURRENT_REQUESTS_LIMIT,
                self.current_concurrency + CONCURRENCY_STEP,
            )
            await self.wb_service.update_concurrency_limit(self.current_concurrency)
            logger.info("Повышен лимит конкурентности: new_limit={} ratio={:.2f}", self.current_concurrency, ratio)

    def _log_batch_metrics(self, batch_num: int, total_batches: int, batch_results: List[ProcessingResult]) -> None:
        """Логирует метрики одного батча."""
        batch_total = len(batch_results)
        batch_success = sum(1 for item in batch_results if not item.error)
        batch_ratio = batch_success / max(1, batch_total)
        logger.info(
            "Батч завершен: batch_num={} total_batches={} batch_total={} batch_success_ratio={:.3f} wb_concurrency_current={}",
            batch_num,
            total_batches,
            batch_total,
            batch_ratio,
            self.current_concurrency,
        )

    def _log_final_metrics(self) -> None:
        """Логирует агрегированные метрики всего запуска."""
        latencies = self.metrics.latencies_ms
        p50 = p95 = p99 = 0
        if latencies:
            sorted_values = sorted(latencies)
            p50 = sorted_values[len(sorted_values) // 2]
            if len(sorted_values) > 1:
                qs = quantiles(sorted_values, n=100)
                p95 = int(qs[94])
                p99 = int(qs[98])
        logger.info(
            "Итоговые метрики: wb_requests_total={} wb_success_total={} wb_failed_total={} wb_retries_total={} "
            "wb_rate_limited_total={} wb_forbidden_total={} wb_timeouts_total={} wb_short_circuited_total={} "
            "detail_success_total={} detail_failed_total={} similar_success_total={} similar_failed_total={} "
            "wb_request_latency_ms_p50={} wb_request_latency_ms_p95={} wb_request_latency_ms_p99={} "
            "batch_success_total={} batch_failed_total={}",
            self.metrics.total_requests,
            self.metrics.successful_requests,
            self.metrics.failed_requests,
            self.metrics.retries_total,
            self.metrics.rate_limited_total,
            self.metrics.forbidden_total,
            self.metrics.timeouts_total,
            self.metrics.short_circuited_total,
            self.metrics.detail_success_total,
            self.metrics.detail_failed_total,
            self.metrics.similar_success_total,
            self.metrics.similar_failed_total,
            p50,
            p95,
            p99,
            self.metrics.batch_successful_items,
            self.metrics.batch_failed_items,
        )

    def __new__(cls, *args: Any, **kwargs: Any) -> "WildPosition":
        """Singleton-конструктор.

        WARNING:
        Не удаляйте singleton-логику без аудита точек вызова, иначе можно
        получить повторную инициализацию клиентов и гонки закрытия ресурсов.
        """
        if not cls.__instance:
            cls.__instance = super(WildPosition, cls).__new__(cls, *args, **kwargs)
            cls.__instance.__init__(
                postgres_client=psql_client,
                clickhouse_client=click_client,
                wb_service=wb_serv,
            )
        return cls.__instance

    @staticmethod
    def get_instance() -> "WildPosition":
        """Возвращает singleton-экземпляр оркестратора."""
        if not WildPosition.__instance:
            WildPosition.__new__(WildPosition)
        return WildPosition.__instance


wild_position = WildPosition.get_instance()


async def main() -> None:
    """Async-точка входа процесса мониторинга."""
    setup_logger()
    articles_data = GoogleSheetsReader().get_articles_from_sheet(GOOGLE_SHEET_NAME)
    if not articles_data:
        logger.error("Список артикулов из Google Sheets пуст")
        return
    await wild_position.run(articles_data)


if __name__ == "__main__":
    asyncio.run(main())
