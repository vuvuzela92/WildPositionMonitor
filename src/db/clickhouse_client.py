"""Модуль для работы с ClickHouse (синхронный).

Сохранение в ClickHouse выполняется синхронным драйвером.
В оркестраторе вызов этого клиента уходит в `asyncio.to_thread`, чтобы не
блокировать event loop и не ломать async-поведение приложения.
"""

from typing import List

from clickhouse_driver import Client
from loguru import logger

from src.data_models import ProcessingResult


class ClickHouseClient:
    """Синхронный клиент для записи результатов в ClickHouse."""

    def __init__(self, connection_params: dict):
        """Сохраняет параметры подключения и инициализирует состояние клиента."""
        self.connection_params = connection_params
        self.client = None
        self.logger = logger

    def connect(self) -> bool:
        """Подключается к ClickHouse и выполняет простую health-проверку."""
        try:
            self.logger.info(
                "Подключение к ClickHouse: старт host={} port={} db={}",
                self.connection_params["host"],
                self.connection_params["port"],
                self.connection_params["database"],
            )
            self.client = Client(
                host=self.connection_params["host"],
                port=self.connection_params["port"],
                user=self.connection_params["user"],
                password=self.connection_params["password"],
                database=self.connection_params["database"],
                settings={"use_numpy": False},
            )
            result = self.client.execute("SELECT 1")
            if result and result[0][0] == 1:
                self.logger.info("Подключение к ClickHouse: успешно")
                return True
            self.logger.error("Подключение к ClickHouse: проверка соединения не пройдена")
            return False
        except Exception as exc:
            self.logger.exception("Подключение к ClickHouse: ошибка {}", exc)
            return False

    def close(self) -> None:
        """Логически закрывает клиент ClickHouse."""
        self.client = None
        self.logger.info("Соединение ClickHouse закрыто")

    def save_results(self, results: List[ProcessingResult]) -> bool:
        """Сохраняет батч результатов в таблицу `product_positions`.

        Параметры:
        - `results`: список результатов обработки артикулов.

        Возвращает:
        - `True`, если вставка успешна или батч пуст;
        - `False`, если запись не выполнена.

        WARNING:
        Схема `INSERT` должна строго соответствовать фактической схеме таблицы.
        Любое изменение порядка полей без миграции может привести к тихой порче данных.
        """
        if not self.client:
            self.logger.error("Нет соединения с ClickHouse")
            return False
        if not results:
            self.logger.info("Сохранение в ClickHouse пропущено: пустой батч")
            return True

        try:
            self.logger.info("Сохранение в ClickHouse: старт count={}", len(results))
            records = [
                (
                    result.article_id,
                    result.price,
                    result.found_article,
                    result.position,
                    result.processed_at,
                    result.wild,
                    result.concurrent,
                )
                for result in results
            ]
            self.client.execute(
                """
                INSERT INTO product_positions
                (article_id, price, found_article, position, processed_at, wild, concurrent)
                VALUES
                """,
                records,
            )
            self.logger.info("Сохранение в ClickHouse: успешно count={}", len(results))
            return True
        except Exception as exc:
            self.logger.exception("Сохранение в ClickHouse: ошибка count={} error={}", len(results), exc)
            return False
