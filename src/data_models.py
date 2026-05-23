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


@dataclass
class WBRequestResult:
    """Типизированный результат HTTP-запроса в WB."""
    ok: bool
    status_class: str
    status_code: Optional[int] = None
    retriable: bool = False
    payload: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    latency_ms: int = 0
    retries_used: int = 0


@dataclass
class RuntimeMetrics:
    """Простые runtime-метрики без внешних зависимостей."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    retries_total: int = 0
    rate_limited_total: int = 0
    forbidden_total: int = 0
    timeouts_total: int = 0
    short_circuited_total: int = 0
    batch_successful_items: int = 0
    batch_failed_items: int = 0
    latencies_ms: List[int] = field(default_factory=list)
