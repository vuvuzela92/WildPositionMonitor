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
    WB_ALLOW_MISSING_PRODUCT,
    WB_ALLOW_MISSING_PRICE,
    WB_DETAIL_ENDPOINT_MODE,
    WB_ROLLOUT_ARTICLES_LIMIT,
    WB_DETAIL_SUBMIT_DELAY,
    WB_SKIP_SIMILAR_STAGE,
)
from src.data_models import ProcessingResult, RuntimeMetrics
from src.db.clickhouse_client import ClickHouseClient
from src.db.postgres_client import PostgresClient
from src.logger import setup_logger
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

    pending: Set[str] = field(default_factory=set)
    in_progress: Set[str] = field(default_factory=set)
    done: Set[str] = field(default_factory=set)
    failed_retriable: Dict[str, int] = field(default_factory=dict)
    failed_terminal: Set[str] = field(default_factory=set)


@dataclass
class RunDiagnosticsState:
    """Снимок runtime-состояния для диагностики досрочного завершения."""

    current_stage: str = "init"
    current_batch_index: int = 0
    total_batches: int = 0
    current_batch_size: int = 0
    save_results_started: bool = False
    save_results_finished: bool = False
    checkpoint_updated: bool = False
    batch_metrics_logged: bool = False


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
                pending={str(x) for x in payload.get("pending", [])},
                in_progress={str(x) for x in payload.get("in_progress", [])},
                done={str(x) for x in payload.get("done", [])},
                failed_retriable={str(k): int(v) for k, v in payload.get("failed_retriable", {}).items()},
                failed_terminal={str(x) for x in payload.get("failed_terminal", [])},
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


def build_task_key(article_info: Dict[str, Any]) -> str:
    """Строит составной ключ задачи, чтобы не терять строки с одинаковым article_id."""
    article_id = int(article_info["article_id"])
    wild = str(article_info.get("wild", "") or "").strip()
    competitor_status = str(article_info.get("competitor_status", "") or "").strip()
    return f"{wild}|{article_id}|{competitor_status}"


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
        self.metrics = RuntimeMetrics()
        self.current_concurrency = CONCURRENT_REQUESTS_LIMIT
        self.max_retry_per_item = 2
        self.price_diagnostics_limit = 10
        self.price_diagnostics_logged = 0
        self.checkpoint_store = CheckpointStore(CHECKPOINT_FILE_PATH)
        self.checkpoint_state = self.checkpoint_store.load()
        self.run_diagnostics = RunDiagnosticsState()

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
        self._reset_run_diagnostics()
        self._set_run_stage("connect_postgres")
        logger.info("Запуск мониторинга WB, входных элементов={}", len(articles_data))
        try:
            if not await self.postgres_client.connect():
                logger.error("Не удалось подключиться к PostgreSQL")
                return False
            self._set_run_stage("connect_clickhouse")
            if not self.clickhouse_client.connect():
                logger.error("Не удалось подключиться к ClickHouse")
                return False
            self._set_run_stage("initialize_wb_service")
            await self.wb_service.initialize()

            self._set_run_stage("prepare_articles")
            filtered_articles = self._prepare_articles_for_run(articles_data)
            prepared_articles = len(filtered_articles)
            if WB_ROLLOUT_ARTICLES_LIMIT > 0:
                logger.info(
                    "Rollout limit: configured_limit={} prepared_articles={}",
                    WB_ROLLOUT_ARTICLES_LIMIT,
                    prepared_articles,
                )
                limited_articles = min(prepared_articles, WB_ROLLOUT_ARTICLES_LIMIT)
                filtered_articles = filtered_articles[:WB_ROLLOUT_ARTICLES_LIMIT]
                self._restrict_checkpoint_to_selected_articles(filtered_articles)
                logger.info(
                    "Rollout limit applied: prepared_articles={} limited_articles={} excluded_by_rollout_limit={}",
                    prepared_articles,
                    limited_articles,
                    prepared_articles - limited_articles,
                )
            else:
                logger.info("Rollout limit disabled: prepared_articles={}", prepared_articles)
            if not filtered_articles:
                logger.warning("Нет артикулов для обработки после фильтра checkpoint")
                return True

            self._set_run_stage("load_our_articles")
            our_articles = await self.postgres_client.get_our_articles()
            if not our_articles:
                logger.error("Не удалось получить список наших артикулов")
                return False

            self._set_run_stage("build_batches")
            total_batches = (len(filtered_articles) + BATCH_SIZE - 1) // BATCH_SIZE
            self.run_diagnostics.total_batches = total_batches
            for i in range(0, len(filtered_articles), BATCH_SIZE):
                batch = filtered_articles[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                self._start_batch_diagnostics(
                    batch_num=batch_num,
                    total_batches=total_batches,
                    batch_size=len(batch),
                )
                logger.info("Старт батча {}/{} (размер={})", batch_num, total_batches, len(batch))

                # Async-обработка батча и sync-запись в ClickHouse через thread pool,
                # чтобы не блокировать event loop.
                self._set_run_stage("process_batch")
                batch_results = await self._process_batch(batch, our_articles, batch_num=batch_num)
                self._set_run_stage("save_results")
                self.run_diagnostics.save_results_started = True
                self.run_diagnostics.save_results_finished = False
                logger.info(
                    "Сохранение батча: старт batch_num={} total_batches={} results_count={}",
                    batch_num,
                    total_batches,
                    len(batch_results),
                )
                try:
                    await to_thread(self.clickhouse_client.save_results, batch_results)
                except asyncio.CancelledError:
                    logger.exception(
                        "Сохранение батча: отменено во время ожидания snapshot={}",
                        self._diagnostics_snapshot(),
                    )
                    raise
                except Exception:
                    logger.exception(
                        "Сохранение батча: ошибка snapshot={}",
                        self._diagnostics_snapshot(),
                    )
                    raise
                else:
                    self.run_diagnostics.save_results_finished = True
                    logger.info(
                        "Сохранение батча: успешно batch_num={} total_batches={} results_count={}",
                        batch_num,
                        total_batches,
                        len(batch_results),
                    )

                # Обновляем состояние восстановления и адаптируем конкурентность
                # только после фиксации результатов текущего батча.
                self._set_run_stage("update_checkpoint")
                self._update_checkpoint_after_batch(batch_results)
                self.run_diagnostics.checkpoint_updated = True
                self._set_run_stage("adapt_concurrency")
                await self._adapt_concurrency(batch_results)
                self._set_run_stage("log_batch_metrics")
                self._log_batch_metrics(batch_num, total_batches, batch_results)
                self.run_diagnostics.batch_metrics_logged = True

            self._set_run_stage("final_metrics")
            self._log_final_metrics()
            logger.info("Мониторинг WB завершен успешно, всего обработано={}", len(filtered_articles))
            return True
        except asyncio.CancelledError:
            logger.exception(
                "Мониторинг WB отменён извне: asyncio.CancelledError snapshot={}",
                self._diagnostics_snapshot(),
            )
            raise
        except Exception as exc:
            logger.exception(
                "Мониторинг WB завершился с ошибкой: {} snapshot={}",
                exc,
                self._diagnostics_snapshot(),
            )
            return False
        except BaseException:
            logger.exception(
                "Мониторинг WB завершился через BaseException snapshot={}",
                self._diagnostics_snapshot(),
            )
            raise
        finally:
            self._set_run_stage("close_connections")
            await self._close_connections()

    def _reset_run_diagnostics(self) -> None:
        """Сбрасывает диагностическое состояние перед новым запуском."""
        self.run_diagnostics = RunDiagnosticsState()

    def _set_run_stage(self, stage: str) -> None:
        """Обновляет текущую стадию выполнения для аварийной диагностики."""
        self.run_diagnostics.current_stage = stage

    def _start_batch_diagnostics(self, batch_num: int, total_batches: int, batch_size: int) -> None:
        """Подготавливает диагностическое состояние к обработке нового батча."""
        self.run_diagnostics.current_batch_index = batch_num
        self.run_diagnostics.total_batches = total_batches
        self.run_diagnostics.current_batch_size = batch_size
        self.run_diagnostics.save_results_started = False
        self.run_diagnostics.save_results_finished = False
        self.run_diagnostics.checkpoint_updated = False
        self.run_diagnostics.batch_metrics_logged = False

    def _diagnostics_snapshot(self) -> Dict[str, Any]:
        """Возвращает снимок состояния выполнения для логирования."""
        return {
            "current_stage": self.run_diagnostics.current_stage,
            "current_batch_index": self.run_diagnostics.current_batch_index,
            "total_batches": self.run_diagnostics.total_batches,
            "current_batch_size": self.run_diagnostics.current_batch_size,
            "save_results_started": self.run_diagnostics.save_results_started,
            "save_results_finished": self.run_diagnostics.save_results_finished,
            "checkpoint_updated": self.run_diagnostics.checkpoint_updated,
            "batch_metrics_logged": self.run_diagnostics.batch_metrics_logged,
        }

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
        indexed: Dict[str, Dict[str, Any]] = {}
        for item in articles_data:
            if not item.get("article_id"):
                continue
            task_item = dict(item)
            task_item["task_key"] = build_task_key(task_item)
            indexed[task_item["task_key"]] = task_item

        if self._has_legacy_checkpoint_keys():
            logger.warning(
                "Обнаружен legacy checkpoint с ключами только по article_id, выполняем безопасный сброс состояния для перехода на составные task_key"
            )
            self.checkpoint_state = CheckpointState()

        if not self.checkpoint_state.pending:
            self.checkpoint_state.pending = set(indexed.keys())
        else:
            self.checkpoint_state.pending = {
                task_key
                for task_key in self.checkpoint_state.pending
                if task_key in indexed
            }

        recoverable_failed = {
            task_key
            for task_key, retries in self.checkpoint_state.failed_retriable.items()
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

    def _restrict_checkpoint_to_selected_articles(self, selected_articles: List[Dict[str, Any]]) -> None:
        """Сужает transient checkpoint-state до реально выбранного лимитного поднабора.

        Используется только для diagnostic/smoke запусков с rollout limit, чтобы
        `pending` не раздувался до полного списка из Google Sheets.
        Исторические `done`/`failed_terminal` не трогаем, чтобы не ломать семантику
        обычных полноразмерных прогонов.
        """
        selected_task_keys = {
            str(article.get("task_key") or build_task_key(article))
            for article in selected_articles
            if article.get("article_id")
        }
        self.checkpoint_state.pending &= selected_task_keys
        self.checkpoint_state.in_progress &= selected_task_keys
        self.checkpoint_state.failed_retriable = {
            task_key: retries
            for task_key, retries in self.checkpoint_state.failed_retriable.items()
            if task_key in selected_task_keys
        }
        self.checkpoint_store.save(self.checkpoint_state)

    def _has_legacy_checkpoint_keys(self) -> bool:
        """Определяет, что checkpoint ещё использует старые ключи по одному article_id."""
        key_groups = (
            self.checkpoint_state.pending,
            self.checkpoint_state.in_progress,
            self.checkpoint_state.done,
            self.checkpoint_state.failed_terminal,
            set(self.checkpoint_state.failed_retriable.keys()),
        )
        return any("|" not in key for group in key_groups for key in group)

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
        if WB_DETAIL_SUBMIT_DELAY > 0:
            logger.info(
                "WB detail submit delay enabled: delay={} batch_size={}",
                WB_DETAIL_SUBMIT_DELAY,
                len(articles_data),
            )

        tasks: List[asyncio.Task[ProcessingResult]] = []
        total_articles = len(articles_data)
        for index, article_info in enumerate(articles_data, start=1):
            task = asyncio.create_task(
                self._process_single_article(article_info, our_articles, batch_num=batch_num)
            )
            tasks.append(task)

            if WB_DETAIL_SUBMIT_DELAY > 0 and index < total_articles:
                await asyncio.sleep(WB_DETAIL_SUBMIT_DELAY)

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
        task_key = str(article_info.get("task_key") or build_task_key(article_info))
        request_id = f"{batch_num}-{article_id}-{uuid4().hex[:8]}"
        wild_value = article_info.get("wild", "")
        competitor_status = article_info.get("competitor_status", "")

        # Фиксируем переход артикула в in_progress до начала сетевых вызовов.
        self.checkpoint_state.in_progress.add(task_key)
        self.checkpoint_state.pending.discard(task_key)
        logger.debug(
            "Старт обработки артикула: request_id={} task_key={} article_id={} batch_num={}",
            request_id,
            task_key,
            article_id,
            batch_num,
        )
        try:
            product_response = await self.wb_service.get_product_details(article_id, request_id=request_id)
            self._collect_http_metrics(product_response, stage="detail")
            if not product_response.ok:
                return self._failed_result(
                    article_id=article_id,
                    task_key=task_key,
                    status=product_response.status_class,
                    message=product_response.error or "ошибка_получения_товара",
                    wild=wild_value,
                    competitor_status=competitor_status,
                )

            payload = product_response.payload or {}
            product = self.wb_service.parse_product_details(payload)
            if not product:
                if WB_ALLOW_MISSING_PRODUCT and WB_DETAIL_ENDPOINT_MODE == "u_card_v4":
                    self.metrics.batch_successful_items += 1
                    logger.warning(
                        "Карточка WB отсутствует или не распарсилась, пропуск разрешен: request_id={} article_id={} task_key={} status=ok_missing_product",
                        request_id,
                        article_id,
                        task_key,
                    )
                    return ProcessingResult(
                        article_id=article_id,
                        task_key=task_key,
                        status="ok",
                        price=None,
                        found_article=None,
                        position=None,
                        processed_at=datetime.now(),
                        error="missing_product_allowed",
                        wild=wild_value,
                        concurrent=competitor_status,
                    )
                return self._failed_result(
                    article_id=article_id,
                    task_key=task_key,
                    status="parse_error",
                    message="некорректный_ответ_товара",
                    wild=wild_value,
                    competitor_status=competitor_status,
                )
            if product.price is not None:
                logger.info(
                    "Цена распарсена: request_id={} article_id={} product_id={} price={}",
                    request_id,
                    article_id,
                    product.id,
                    product.price,
                )

            if WB_SKIP_SIMILAR_STAGE:
                logger.warning(
                    "Similar stage skipped by config: request_id={} article_id={}",
                    request_id,
                    article_id,
                )
                similar = None
                found_id, position = None, None
            else:
                similar = await self.wb_service.get_similar_products(product, request_id=request_id)
                self._collect_similar_metrics(similar)
                if similar.error:
                    logger.warning(
                        "Similar stage soft-failed: request_id={} article_id={} error={}",
                        request_id,
                        article_id,
                        similar.error,
                    )
                    found_id, position = None, None
                else:
                    found_id, position = self.wb_service.find_our_article_in_similar(similar, our_articles)
            if found_id:
                logger.info(
                    "Найден наш артикул: request_id={} article_id={} found_article={} position={}",
                    request_id,
                    article_id,
                    found_id,
                    position,
                )
            if product.price is None:
                if WB_ALLOW_MISSING_PRICE and WB_DETAIL_ENDPOINT_MODE == "u_card_v4":
                    self.metrics.batch_successful_items += 1
                    self._log_price_not_found(
                        request_id=request_id,
                        article_id=article_id,
                        task_key=task_key,
                        product=product,
                        wild=wild_value,
                        competitor_status=competitor_status,
                    )
                    logger.info(
                        "Итог по артикулу: task_key={} article_id={} price=None status=ok_missing_price found_article={} position={} wild={} competitor_status={}",
                        task_key,
                        article_id,
                        found_id,
                        position,
                        wild_value,
                        competitor_status,
                    )
                    return ProcessingResult(
                        article_id=article_id,
                        task_key=task_key,
                        status="ok",
                        price=None,
                        found_article=found_id,
                        position=position,
                        processed_at=datetime.now(),
                        error=(
                            "missing_price_allowed"
                            if WB_SKIP_SIMILAR_STAGE or not similar or not similar.error
                            else f"missing_price_allowed:similar_soft_error:{similar.error}"
                        ),
                        wild=wild_value,
                        concurrent=competitor_status,
                    )
                self.metrics.batch_failed_items += 1
                self._log_price_not_found(
                    request_id=request_id,
                    article_id=article_id,
                    task_key=task_key,
                    product=product,
                    wild=wild_value,
                    competitor_status=competitor_status,
                )
                logger.info(
                    "Итог по артикулу: task_key={} article_id={} price=None status=price_not_found found_article={} position={} wild={} competitor_status={}",
                    task_key,
                    article_id,
                    found_id,
                    position,
                    wild_value,
                    competitor_status,
                )
                return ProcessingResult(
                    article_id=article_id,
                    task_key=task_key,
                    status="price_not_found",
                    price=None,
                    found_article=found_id,
                    position=position,
                    processed_at=datetime.now(),
                    error="price_not_found:цена_не_извлечена",
                    wild=wild_value,
                    concurrent=competitor_status,
                )
            self.metrics.batch_successful_items += 1
            logger.debug(
                "Обработка артикула завершена успешно: request_id={} task_key={} article_id={}",
                request_id,
                task_key,
                article_id,
            )
            logger.info(
                "Итог по артикулу: task_key={} article_id={} price={} status=ok found_article={} position={} wild={} competitor_status={}",
                task_key,
                article_id,
                product.price,
                found_id,
                position,
                wild_value,
                competitor_status,
            )
            return ProcessingResult(
                article_id=article_id,
                task_key=task_key,
                status="ok",
                price=product.price,
                found_article=found_id,
                position=position,
                processed_at=datetime.now(),
                error=(
                    "similar_stage_skipped"
                    if WB_SKIP_SIMILAR_STAGE
                    else f"similar_soft_error:{similar.error}" if similar and similar.error else None
                ),
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
                task_key=task_key,
                status="ошибка_обработки",
                message=str(exc),
                wild=wild_value,
                competitor_status=competitor_status,
            )
        finally:
            # Гарантируем очистку in_progress при любом исходе.
            self.checkpoint_state.in_progress.discard(task_key)

    def _failed_result(
        self,
        article_id: int,
        task_key: str,
        status: str,
        message: str,
        wild: str,
        competitor_status: str,
        price: Optional[int] = None,
    ) -> ProcessingResult:
        """Формирует единый объект ошибки и учитывает метрики batch-неудач."""
        self.metrics.batch_failed_items += 1
        logger.warning(
            "Обработка артикула завершилась ошибкой: task_key={} article_id={} price={} status={} wild={} competitor_status={} error={}",
            task_key,
            article_id,
            price,
            status,
            wild,
            competitor_status,
            message,
        )
        return ProcessingResult(
            article_id=article_id,
            task_key=task_key,
            status=status,
            price=price,
            error=f"{status}:{message}",
            processed_at=datetime.now(),
            wild=wild,
            concurrent=competitor_status,
        )

    def _log_price_not_found(
        self,
        request_id: str,
        article_id: int,
        task_key: str,
        product: Any,
        wild: str,
        competitor_status: str,
    ) -> None:
        """Логирует отдельный сигнал для карточек без извлечённой цены."""
        logger.warning(
            "Цена не извлечена: request_id={} task_key={} article_id={} product_id={} wild={} competitor_status={} reason=price_not_found",
            request_id,
            task_key,
            article_id,
            product.id,
            wild,
            competitor_status,
        )
        if self.price_diagnostics_logged >= self.price_diagnostics_limit:
            return

        self.price_diagnostics_logged += 1
        diagnostics = self.wb_service.build_price_diagnostics(product)
        logger.warning(
            "Диагностика price_not_found: request_id={} task_key={} article_id={} diagnostics={}",
            request_id,
            task_key,
            article_id,
            diagnostics,
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
            task_key = str(result.task_key or result.article_id)
            # Финализируем transient-состояние батча независимо от исхода:
            # задача больше не должна оставаться в in_progress после обработки.
            self.checkpoint_state.in_progress.discard(task_key)
            # Для checkpoint опираемся на business-status результата, а не на наличие
            # soft diagnostic-маркеров в `error` (`missing_price_allowed`,
            # `missing_product_allowed`, `similar_stage_skipped` и т.п.).
            if result.status == "ok":
                self.checkpoint_state.done.add(task_key)
                self.checkpoint_state.pending.discard(task_key)
                self.checkpoint_state.failed_retriable.pop(task_key, None)
                self.checkpoint_state.failed_terminal.discard(task_key)
                continue

            is_retriable = any(
                key in result.error
                for key in ("rate_limited", "forbidden", "upstream_5xx", "timeout", "network_error", "circuit_open")
            )
            if is_retriable:
                self.checkpoint_state.done.discard(task_key)
                self.checkpoint_state.failed_terminal.discard(task_key)
                retries = self.checkpoint_state.failed_retriable.get(task_key, 0) + 1
                self.checkpoint_state.failed_retriable[task_key] = retries
                if retries > self.max_retry_per_item:
                    self.checkpoint_state.failed_terminal.add(task_key)
                    self.checkpoint_state.pending.discard(task_key)
                    self.checkpoint_state.failed_retriable.pop(task_key, None)
                else:
                    self.checkpoint_state.pending.add(task_key)
            else:
                self.checkpoint_state.done.discard(task_key)
                self.checkpoint_state.failed_terminal.add(task_key)
                self.checkpoint_state.pending.discard(task_key)
                self.checkpoint_state.failed_retriable.pop(task_key, None)

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
        batch_success = sum(1 for item in batch_results if item.status == "ok")
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
    exit_code = 0
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        exit_code = 130
        logger.exception("Процесс остановлен вручную: KeyboardInterrupt exit_code={}", exit_code)
        raise
    except asyncio.CancelledError:
        exit_code = 1
        logger.exception("Главная coroutine отменена: asyncio.CancelledError exit_code={}", exit_code)
        raise
    except BaseException:
        exit_code = 1
        logger.exception("Процесс завершился через BaseException exit_code={}", exit_code)
        raise
    finally:
        logger.info("Завершение entrypoint: exit_code={}", exit_code)
