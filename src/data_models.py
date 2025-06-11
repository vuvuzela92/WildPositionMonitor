"""
Модели данных для проекта WildPositionMonitor
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from datetime import datetime


@dataclass
class ProductDetails:
    """Информация о товаре из Wildberries"""
    id: int
    name: str
    brand: str
    price: Optional[int] = None
    raw_data: Optional[Dict[str, Any]] = None


@dataclass
class SimilarProductsResult:
    """Результат запроса похожих товаров"""
    original_product: ProductDetails
    similar_products: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class ProcessingResult:
    """Результат обработки артикула"""
    article_id: int
    price: Optional[int] = None
    found_article: Optional[int] = None
    position: Optional[int] = None
    processed_at: datetime = field(default_factory=datetime.now)
    error: Optional[str] = None
    wild: Optional[str] = None
    concurrent: Optional[str] = None
