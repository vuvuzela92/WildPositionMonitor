"""Модели данных проекта WildPositionMonitor.

Модуль содержит dataclass-структуры для обмена между слоями:
- HTTP-сервис Wildberries,
- оркестратор обработки батчей,
- слои сохранения в БД.

Преимущество dataclass здесь — явные контракты без тяжёлого ORM.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ProductDetails:
    """Нормализованные данные карточки товара из Wildberries.

    Поля:
    - `id`: артикул / идентификатор товара;
    - `name`: название товара;
    - `brand`: бренд товара;
    - `price`: цена в рублях (если удалось извлечь);
    - `raw_data`: исходный payload карточки для дополнительной диагностики.

    Риск:
    - `raw_data` может быть объёмным, поэтому его нельзя бездумно логировать
      целиком в production-цикле.
    """

    id: int
    name: str
    brand: str
    price: Optional[int] = None
    raw_data: Optional[Dict[str, Any]] = None


@dataclass
class SimilarProductsResult:
    """Результат запроса похожих товаров."""

    original_product: ProductDetails
    similar_products: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class ProcessingResult:
    """Итог обработки одного артикула в оркестраторе.

    Используется как единая запись для сохранения в ClickHouse
    и для расчёта runtime-метрик.
    """

    article_id: int
    task_key: str = ""
    status: str = "ok"
    price: Optional[int] = None
    found_article: Optional[int] = None
    position: Optional[int] = None
    processed_at: datetime = field(default_factory=datetime.now)
    error: Optional[str] = None
    wild: Optional[str] = None
    concurrent: Optional[str] = None


@dataclass
class WBRequestResult:
    """Типизированный результат HTTP-запроса к Wildberries.

    Контракт нужен, чтобы не передавать "сырые" исключения в оркестратор,
    а работать с предсказуемой классификацией ошибок.
    """

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
    """Runtime-метрики процесса без внешней системы мониторинга.

    WARNING:
    Эти счётчики живут в памяти текущего запуска. Для долгосрочного трендинга
    их нужно агрегировать вне процесса (например, в БД/TSDB).
    """

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
    detail_success_total: int = 0
    detail_failed_total: int = 0
    similar_success_total: int = 0
    similar_failed_total: int = 0
