"""
Основной модуль для мониторинга позиций товаров Wildberries
"""

import asyncio
from datetime import datetime
from typing import List, Set, Dict, Any, Optional

from loguru import logger

from src.config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB,
    BATCH_SIZE, DATA_SOURCE, GOOGLE_SHEET_NAME
)
from src.logger import setup_logger
from src.db.postgres_client import PostgresClient
from src.db.clickhouse_client import ClickHouseClient
from src.services.wb_service import WildberriesService
from src.data_models import ProcessingResult
from src.utils.excel_reader import ExcelReader
from src.utils.google_sheets_reader import GoogleSheetsReader


class WildPosition:
    """
    Класс для мониторинга позиций товаров Wildberries
    с асинхронной обработкой API запросов
    """
    
    def __init__(self):
        setup_logger()
        self.logger = logger
        
        # Создаем клиенты баз данных
        self.postgres_client = PostgresClient({
            'host': POSTGRES_HOST,
            'port': POSTGRES_PORT,
            'user': POSTGRES_USER,
            'password': POSTGRES_PASSWORD,
            'database': POSTGRES_DB
        })
        
        # ClickHouse клиент (полностью синхронный)
        self.clickhouse_client = ClickHouseClient({
            'host': CLICKHOUSE_HOST,
            'port': CLICKHOUSE_PORT,
            'user': CLICKHOUSE_USER,
            'password': CLICKHOUSE_PASSWORD,
            'database': CLICKHOUSE_DB
        })
        
        # Сервис Wildberries (асинхронный)
        self.wb_service = WildberriesService()
    
    async def run(self, articles: List[int]) -> bool:
        """
        Запускает процесс мониторинга для списка артикулов
        
        Args:
            articles: Список артикулов для обработки
            
        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        self.logger.info("Запуск мониторинга товаров Wildberries")
        
        try:
            # Асинхронное подключение к PostgreSQL
            if not await self.postgres_client.connect():
                self.logger.error("Ошибка подключения к PostgreSQL")
                return False
                
            # Синхронное подключение к ClickHouse
            if not self.clickhouse_client.connect():
                self.logger.error("Ошибка подключения к ClickHouse")
                return False

            
            # Асинхронная инициализация Wildberries API
            await self.wb_service.initialize()
            
            # Получаем список наших артикулов (асинхронно)
            our_articles = await self.postgres_client.get_our_articles()
            if not our_articles:
                self.logger.error("Не удалось получить список наших артикулов")
                return False
            
            # Обработка артикулов батчами
            all_results = []
            for i in range(0, len(articles), BATCH_SIZE):
                batch = articles[i:i + BATCH_SIZE]
                batch_num = i//BATCH_SIZE + 1
                total_batches = (len(articles) + BATCH_SIZE - 1)//BATCH_SIZE
                
                self.logger.info(f"Обработка батча {batch_num}/{total_batches} ({len(batch)} артикулов)")
                
                # Асинхронная обработка батча
                batch_results = await self._process_batch(batch, our_articles)
                all_results.extend(batch_results)
                
                # Синхронное сохранение результатов батча в ClickHouse
                self.clickhouse_client.save_results(batch_results)

            
        except Exception as e:
            self.logger.error(f"Ошибка: {e}")
            return False
        finally:
            # Закрываем соединения
            await self._close_connections()
    
    async def _close_connections(self) -> None:
        """Закрытие соединений с базами данных и сервисами"""
        # Асинхронное закрытие
        await self.wb_service.close()
        await self.postgres_client.close()
        
        # Синхронное закрытие
        self.clickhouse_client.close()
    
    async def _process_batch(self, articles: List[int], our_articles: Set[int]) -> List[ProcessingResult]:
        """
        Асинхронная обработка батча артикулов
        
        Args:
            articles: Список артикулов для обработки
            our_articles: Множество наших артикулов
            
        Returns:
            List[ProcessingResult]: Список результатов обработки
        """
        # Используем gather для параллельной обработки артикулов в батче
        tasks = [self._process_single_article(article_id, our_articles) for article_id in articles]
        results = await asyncio.gather(*tasks)
        return results
    
    async def _process_single_article(self, article_id: int, our_articles: Set[int]) -> ProcessingResult:
        """
        Асинхронная обработка одного артикула
        
        Args:
            article_id: ID артикула
            our_articles: Множество наших артикулов
            
        Returns:
            ProcessingResult: Результат обработки
        """
        try:
            # Получаем данные о товаре
            product = await self.wb_service.get_product_details(article_id)
            if not product:
                return ProcessingResult(
                    article_id=article_id,
                    error="Товар не найден",
                    processed_at=datetime.now()
                )
            
            # Запоминаем цену
            price = product.price
            
            # Ищем похожие товары
            similar = await self.wb_service.get_similar_products(product)
            if similar.error:
                return ProcessingResult(
                    article_id=article_id,
                    price=price,
                    error=similar.error,
                    processed_at=datetime.now()
                )
            
            # Проверяем наличие наших артикулов среди похожих (синхронная операция)
            found_id, position = self.wb_service.find_our_article_in_similar(similar, our_articles)
            
            # Если нашли, выводим информацию
            if found_id:
                self.logger.info(f"Найден артикул {found_id} на позиции {position}")
            
            # Возвращаем результат
            return ProcessingResult(
                article_id=article_id,
                price=price,
                found_article=found_id,
                position=position,
                processed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка обработки артикула {article_id}: {e}")
            return ProcessingResult(
                article_id=article_id,
                error=str(e),
                processed_at=datetime.now()
            )



# Точка входа
async def main():
    articles = GoogleSheetsReader().get_articles_from_sheet(GOOGLE_SHEET_NAME)
    if not articles:
        logger.error("Не удалось получить список артикулов. Завершение программы.")
        return
        
    await WildPosition().run(articles)


if __name__ == "__main__":
    asyncio.run(main())
