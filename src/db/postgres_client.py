"""Модуль для работы с PostgreSQL (асинхронный).

WARNING:
Методы выполняются в event loop. Изменение на синхронные операции
или добавление тяжёлой пост-обработки в этих методах может блокировать
весь async-пайплайн мониторинга.
"""

from typing import List, Set

import asyncpg
from loguru import logger


class PostgresClient:
    """Асинхронный клиент PostgreSQL для чтения артикулов."""

    def __init__(self, connection_params: dict):
        """Инициализирует клиент и сохраняет параметры подключения."""
        self.connection_params = connection_params
        self.connection = None
        self.logger = logger

    async def connect(self) -> bool:
        """Подключается к PostgreSQL.

        Возвращает:
        - `True`, если подключение успешно;
        - `False`, если подключиться не удалось.

        Риск:
        - при неверных сетевых параметрах/учётных данных метод возвращает False,
          а оркестратор должен корректно завершить запуск.
        """
        try:
            self.logger.info(
                "Подключение к PostgreSQL: старт host={} port={} db={}",
                self.connection_params["host"],
                self.connection_params["port"],
                self.connection_params["database"],
            )
            self.connection = await asyncpg.connect(
                host=self.connection_params["host"],
                port=self.connection_params["port"],
                user=self.connection_params["user"],
                password=self.connection_params["password"],
                database=self.connection_params["database"],
            )
            self.logger.info("Подключение к PostgreSQL: успешно")
            return True
        except Exception as exc:
            self.logger.exception("Подключение к PostgreSQL: ошибка {}", exc)
            return False

    async def close(self) -> None:
        """Закрывает соединение с PostgreSQL, если оно было открыто."""
        if self.connection:
            await self.connection.close()
            self.logger.info("Соединение PostgreSQL закрыто")

    async def get_our_articles(self) -> Set[int]:
        """Читает множество наших артикулов из PostgreSQL.

        Возвращает:
        - множество `article_id` без `None` значений;
        - пустое множество при ошибке.

        Почему `Set[int]`:
        - быстрые membership-проверки при поиске в похожих товарах (`O(1)`).
        """
        if not self.connection:
            self.logger.error("Нет соединения с PostgreSQL")
            return set()

        try:
            self.logger.info("Чтение наших артикулов из PostgreSQL: старт")
            rows = await self.connection.fetch("SELECT article_id FROM card_data")
            articles = {row["article_id"] for row in rows if row["article_id"] is not None}
            self.logger.info("Чтение наших артикулов из PostgreSQL: успешно, count={}", len(articles))
            return articles
        except Exception as exc:
            self.logger.exception("Чтение наших артикулов из PostgreSQL: ошибка {}", exc)
            return set()

    async def get_articles_batch(self, offset: int, limit: int) -> List[int]:
        """Читает батч артикулов через LIMIT/OFFSET.

        WARNING:
        Для очень больших таблиц OFFSET может деградировать по производительности.
        Метод оставлен как legacy-friendly утилита и используется ограниченно.
        """
        if not self.connection:
            self.logger.error("Нет соединения с PostgreSQL")
            return []

        try:
            self.logger.debug("Чтение батча из PostgreSQL: старт offset={} limit={}", offset, limit)
            rows = await self.connection.fetch(
                "SELECT article_id FROM card_data ORDER BY article_id LIMIT $1 OFFSET $2",
                limit,
                offset,
            )
            articles = [row["article_id"] for row in rows if row["article_id"] is not None]
            self.logger.info("Чтение батча из PostgreSQL: успешно count={} offset={}", len(articles), offset)
            return articles
        except Exception as exc:
            self.logger.exception(
                "Чтение батча из PostgreSQL: ошибка offset={} limit={} error={}",
                offset,
                limit,
                exc,
            )
            return []
