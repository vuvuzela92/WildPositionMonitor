"""
Основной модуль для мониторинга позиций товаров Wildberries.
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
)
from src.data_models import ProcessingResult, RuntimeMetrics
from src.db.clickhouse_client import ClickHouseClient
from src.db.postgres_client import PostgresClient
from src.logger import setup_logger
from src.services.wb_service import WildberriesService
from src.utils.google_sheets_reader import GoogleSheetsReader


@dataclass
class CheckpointState:
    pending: Set[int] = field(default_factory=set)
    in_progress: Set[int] = field(default_factory=set)
    done: Set[int] = field(default_factory=set)
    failed_retriable: Dict[int, int] = field(default_factory=dict)
    failed_terminal: Set[int] = field(default_factory=set)


class CheckpointStore:
    def __init__(self, file_path: str) -> None:
        self.path = Path(file_path)

    def load(self) -> CheckpointState:
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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pending": sorted(state.pending),
            "in_progress": sorted(state.in_progress),
            "done": sorted(state.done),
            "failed_retriable": {str(k): v for k, v in state.failed_retriable.items()},
            "failed_terminal": sorted(state.failed_terminal),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    __instance = None

    def __init__(
        self,
        postgres_client: PostgresClient,
        clickhouse_client: ClickHouseClient,
        wb_service: WildberriesService,
    ) -> None:
        self.postgres_client = postgres_client
        self.clickhouse_client = clickhouse_client
        self.wb_service = wb_service
        self.metrics = RuntimeMetrics()
        self.current_concurrency = CONCURRENT_REQUESTS_LIMIT
        self.max_retry_per_item = 2
        self.checkpoint_store = CheckpointStore(CHECKPOINT_FILE_PATH)
        self.checkpoint_state = self.checkpoint_store.load()

    async def run(self, articles_data: List[Dict[str, Any]]) -> bool:
        logger.info("Запуск мониторинга WB, входных элементов={}", len(articles_data))
        try:
            if not await self.postgres_client.connect():
                logger.error("Не удалось подключиться к PostgreSQL")
                return False
            if not self.clickhouse_client.connect():
                logger.error("Не удалось подключиться к ClickHouse")
                return False
            await self.wb_service.initialize()

            filtered_articles = self._prepare_articles_for_run(articles_data)
            if not filtered_articles:
                logger.warning("Нет артикулов для обработки после фильтра checkpoint")
                return True

            our_articles = await self.postgres_client.get_our_articles()
            if not our_articles:
                logger.error("Не удалось получить список наших артикулов")
                return False

            total_batches = (len(filtered_articles) + BATCH_SIZE - 1) // BATCH_SIZE
            for i in range(0, len(filtered_articles), BATCH_SIZE):
                batch = filtered_articles[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                logger.info("Старт батча {}/{} (размер={})", batch_num, total_batches, len(batch))

                batch_results = await self._process_batch(batch, our_articles, batch_num=batch_num)
                await to_thread(self.clickhouse_client.save_results, batch_results)
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
            await self._close_connections()

    async def _close_connections(self) -> None:
        logger.info("Начато закрытие ресурсов")
        await self.wb_service.close()
        await self.postgres_client.close()
        self.clickhouse_client.close()
        logger.info("Закрытие ресурсов завершено")

    def _prepare_articles_for_run(self, articles_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        self.checkpoint_store.save(self.checkpoint_state)
        return [indexed[article_id] for article_id in target_ids if article_id in indexed]

    async def _process_batch(
        self,
        articles_data: List[Dict[str, Any]],
        our_articles: Set[int],
        batch_num: int,
    ) -> List[ProcessingResult]:
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
        article_id = int(article_info["article_id"])
        request_id = f"{batch_num}-{article_id}-{uuid4().hex[:8]}"
        wild_value = article_info.get("wild", "")
        competitor_status = article_info.get("competitor_status", "")
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
            self._collect_http_metrics(product_response)
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
        self.metrics.batch_failed_items += 1
        logger.warning(
            "Обработка артикула завершилась ошибкой: article_id={} status={} error={}",
            article_id,
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

    def _collect_http_metrics(self, response: Any) -> None:
        self.metrics.total_requests += 1
        self.metrics.retries_total += int(response.retries_used)
        self.metrics.latencies_ms.append(int(response.latency_ms))
        if response.ok:
            self.metrics.successful_requests += 1
        else:
            self.metrics.failed_requests += 1
        if response.status_class == "rate_limited":
            self.metrics.rate_limited_total += 1
        if response.status_class == "forbidden":
            self.metrics.forbidden_total += 1
        if response.status_class == "timeout":
            self.metrics.timeouts_total += 1
        if response.error == "circuit_open":
            self.metrics.short_circuited_total += 1

    def _update_checkpoint_after_batch(self, batch_results: List[ProcessingResult]) -> None:
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
            p50,
            p95,
            p99,
            self.metrics.batch_successful_items,
            self.metrics.batch_failed_items,
        )

    def __new__(cls, *args: Any, **kwargs: Any) -> "WildPosition":
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
        if not WildPosition.__instance:
            WildPosition.__new__(WildPosition)
        return WildPosition.__instance


wild_position = WildPosition.get_instance()


async def main() -> None:
    setup_logger()
    articles_data = GoogleSheetsReader().get_articles_from_sheet(GOOGLE_SHEET_NAME)
    if not articles_data:
        logger.error("Список артикулов из Google Sheets пуст")
        return
    await wild_position.run(articles_data)


if __name__ == "__main__":
    asyncio.run(main())
