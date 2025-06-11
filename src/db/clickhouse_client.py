"""
Модуль для работы с базой данных ClickHouse (полностью синхронный)
"""
from typing import List, Dict, Any, Optional
from clickhouse_driver import Client
from loguru import logger

from src.data_models import ProcessingResult


class ClickHouseClient:
    """Синхронный клиент для работы с ClickHouse"""

    def __init__(self, connection_params: dict):
        """
        Инициализация клиента ClickHouse
        
        Args:
            connection_params: Параметры подключения к базе данных
        """
        self.connection_params = connection_params
        self.client = None
        self.logger = logger

    def connect(self) -> bool:
        """
        Синхронное подключение к базе данных ClickHouse
        
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        try:
            self.client = Client(
                host=self.connection_params['host'],
                port=self.connection_params['port'],
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                database=self.connection_params['database'],
                settings={
                    'use_numpy': False,  # Отключаем numpy для лучшей совместимости
                }
            )

            # Проверяем подключение выполнением простого запроса
            result = self.client.execute("SELECT 1")
            if result and result[0][0] == 1:
                self.logger.info(
                    f"Подключено к ClickHouse: {self.connection_params['host']}:{self.connection_params['port']}"
                )
                return True
            else:
                self.logger.error("Ошибка проверки подключения к ClickHouse")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка при подключении к ClickHouse: {e}")
            return False

    def close(self) -> None:
        """Закрытие соединения с базой данных ClickHouse"""
        self.client = None


    def save_results(self, results: List[ProcessingResult]) -> bool:
        """
        Сохраняет результаты обработки в таблицу product_positions
        
        Args:
            results: Список объектов ProcessingResult
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        if not self.client:
            self.logger.error("Нет соединения с базой данных ClickHouse")
            return False

        if not results:
            return True

        try:
            # Подготавливаем данные для вставки
            records = []
            for result in results:
                records.append((
                    result.article_id,
                    result.price,
                    result.found_article,
                    result.position,
                    result.processed_at,
                    result.wild,
                    result.concurrent
                ))

            # Выполняем вставку всех данных одним запросом
            self.client.execute(
                """
                INSERT INTO product_positions 
                (article_id, price, found_article, position, processed_at, wild, concurrent) 
                VALUES
                """,
                records
            )

            self.logger.info(f"Сохранено {len(results)} записей в ClickHouse")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении результатов в ClickHouse: {e}")
            return False

