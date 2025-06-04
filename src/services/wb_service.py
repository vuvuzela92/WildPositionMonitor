"""
Асинхронный сервис для работы с Wildberries API
"""

import asyncio
from typing import Dict, Optional, Any, Tuple, Set

import aiohttp
from loguru import logger

from src.config import (
    WB_DETAIL_URL, WB_SIMILAR_URL, WB_DEFAULT_DEST,
    WB_TIMEOUT, WB_MAX_RETRIES, WB_RETRY_DELAY,
    CONCURRENT_REQUESTS_LIMIT
)
from src.data_models import ProductDetails, SimilarProductsResult


class WildberriesService:
    """Асинхронный сервис для работы с Wildberries API"""

    def __init__(self):
        self.session = None
        self.logger = logger
        self.semaphore = None

    async def initialize(self):
        """Инициализация сервиса"""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=WB_TIMEOUT))
        self.semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
        self.logger.info("Инициализирован сервис Wildberries")

    async def close(self):
        """Закрытие сервиса"""
        if self.session:
            await self.session.close()

    async def _make_request(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Выполняет асинхронный запрос к API с повторными попытками
        Args:
            url: URL для запроса
            params: Параметры запроса
        Returns:
            Optional[Dict[str, Any]]: Ответ API в виде словаря или None в случае ошибки
        """
        retries = 0
        while retries < WB_MAX_RETRIES:
            try:
                async with self.semaphore:
                    async with self.session.get(url, params=params, ssl=False) as response:
                        if response.status == 200:
                            return await response.json()
                        self.logger.warning(f"Статус {response.status} при запросе API")
                        return None
            except asyncio.TimeoutError:
                self.logger.warning(f"Таймаут при запросе {url}")
            except Exception as e:
                self.logger.error(f"Ошибка при запросе API: {e}")

            retries += 1
            if retries < WB_MAX_RETRIES:
                delay = WB_RETRY_DELAY * retries
                await asyncio.sleep(delay)

        self.logger.error(f"Исчерпаны попытки для запроса API")
        return None

    async def get_product_details(self, product_id: int) -> Optional[ProductDetails]:
        """
        Получает информацию о товаре по его ID
        Args:
            product_id: ID товара
        Returns:
            Optional[ProductDetails]: Информация о товаре или None в случае ошибки
        """
        params = {
            "appType": 1,
            "curr": "rub",
            "dest": WB_DEFAULT_DEST,
            "spp": 30,
            "ab_testing": "false",
            "lang": "ru",
            "nm": product_id
        }

        try:
            response_data = await self._make_request(WB_DETAIL_URL, params)
            if not response_data:
                return None

            if ('data' in response_data and 'products' in response_data['data']
                    and len(response_data['data']['products']) > 0):
                product = response_data['data']['products'][0]
                price = None
                if product.get('sizes') and len(product['sizes']) > 0:
                    price_data = product['sizes'][0].get('price', {})
                    price = price_data.get('product') // 100 if price_data.get('product') else price_data.get('product')

                return ProductDetails(
                    id=product['id'],
                    name=product.get('name', ''),
                    brand=product.get('brand', ''),
                    price=price,
                    raw_data=product
                )

            return None
        except Exception as e:
            self.logger.error(f"Ошибка при получении данных о товаре {product_id}: {e}")
            return None

    async def get_similar_products(self, product_details: ProductDetails) -> SimilarProductsResult:
        """
        Получает список похожих товаров
        Args:
            product_details: Информация о товаре
        Returns:
            SimilarProductsResult: Результат запроса похожих товаров
        """
        if not product_details:
            return SimilarProductsResult(
                original_product=ProductDetails(id=0, name="", brand=""),
                similar_products=[],
                error="Отсутствуют данные о товаре"
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
            response_data = await self._make_request(WB_SIMILAR_URL, params)
            if not response_data:
                return SimilarProductsResult(
                    original_product=product_details,
                    similar_products=[],
                    error="Не удалось получить данные о похожих товарах"
                )
            similar_products = []
            if 'data' in response_data and 'products' in response_data['data']:
                similar_products = response_data['data']['products']
                self.logger.info(f"Получено {len(similar_products)} похожих товаров")
            else:
                self.logger.warning(f"Похожие товары не найдены для артикула {product_id}")

            return SimilarProductsResult(
                original_product=product_details,
                similar_products=similar_products
            )
        except Exception as e:
            error_msg = f"Ошибка при получении похожих товаров: {e}"
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
        """
        Находит наш артикул среди похожих товаров
        Args:
            similar_result: Результат запроса похожих товаров
            our_articles: Множество наших артикулов
        Returns:
            Tuple[Optional[int], Optional[int]]: (найденный артикул, позиция)
        """
        if not similar_result or similar_result.error or not similar_result.similar_products or not our_articles:
            return None, None

        for position, product in enumerate(similar_result.similar_products, 1):
            product_id = product.get('id')
            if product_id in our_articles:
                self.logger.info(f"Найден наш артикул {product_id} на позиции {position}")
                return product_id, position

        return None, None
