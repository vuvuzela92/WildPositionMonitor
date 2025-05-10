"""
Модуль для работы с базой данных PostgreSQL (асинхронное подключение)
"""
from typing import Set, List
import asyncpg
from loguru import logger


class PostgresClient:
    """Клиент для работы с PostgreSQL"""

    def __init__(self, connection_params: dict):
        """
        Инициализация клиента PostgreSQL
        
        Args:
            connection_params: Параметры подключения к базе данных
        """
        self.connection_params = connection_params
        self.connection = None
        self.logger = logger

    async def connect(self) -> bool:
        """
        Устанавливает асинхронное соединение с базой данных PostgreSQL
        
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        try:
            self.connection = await asyncpg.connect(
                host=self.connection_params['host'],
                port=self.connection_params['port'],
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                database=self.connection_params['database']
            )
            self.logger.info(
                f"Подключено к PostgreSQL: {self.connection_params['host']}:{self.connection_params['port']}"
            )
            return True
        except Exception as e:
            self.logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            return False

    async def close(self) -> None:
        """Закрывает асинхронное соединение с базой данных PostgreSQL"""
        if self.connection:
            await self.connection.close()

    async def get_our_articles(self) -> Set[int]:
        """
        Получает список артикулов из таблицы card_data
        
        Returns:
            Set[int]: Множество артикулов
        """
        if not self.connection:
            self.logger.error("Нет соединения с базой данных PostgreSQL")
            return set()
            
        try:
            # Асинхронное выполнение запроса
            rows = await self.connection.fetch("SELECT article_id FROM card_data")
            
            # Преобразуем результаты в множество целых чисел
            articles = {row['article_id'] for row in rows if row['article_id'] is not None}
            self.logger.info(f"Получено {len(articles)} артикулов из PostgreSQL")
            return articles
                
        except Exception as e:
            self.logger.error(f"Ошибка при получении артикулов из PostgreSQL: {e}")
            return set()
    
    async def get_articles_batch(self, offset: int, limit: int) -> List[int]:
        """
        Получает батч артикулов из таблицы card_data
        
        Args:
            offset: Смещение
            limit: Количество записей
            
        Returns:
            List[int]: Список артикулов
        """
        if not self.connection:
            self.logger.error("Нет соединения с базой данных PostgreSQL")
            return []
            
        try:
            # Асинхронное выполнение запроса
            rows = await self.connection.fetch(
                "SELECT article_id FROM card_data ORDER BY article_id LIMIT $1 OFFSET $2", 
                limit, offset
            )
            
            # Преобразуем результаты в список целых чисел
            articles = [row['article_id'] for row in rows if row['article_id'] is not None]
            self.logger.info(f"Получен батч {len(articles)} артикулов (offset: {offset})")
            return articles
                
        except Exception as e:
            self.logger.error(f"Ошибка при получении батча артикулов: {e}")
            return []
