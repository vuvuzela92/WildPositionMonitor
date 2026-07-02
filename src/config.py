"""Конфигурация проекта WildPositionMonitor.

Модуль централизует runtime-настройки, которые влияют на:
- доступ к источникам данных (Google Sheets / Excel),
- подключение к PostgreSQL и ClickHouse,
- поведение HTTP-клиента Wildberries,
- параметры устойчивости (retry, rate limit, circuit breaker),
- параметры параллелизма и батч-обработки,
- параметры логирования и checkpoint.

WARNING:
Проект чувствителен к паттернам тайминга, уровню параллелизма, fingerprint TLS
и жизненному циклу сессии. Безопасно менять значения только после анализа
реальных логов и метрик, иначе можно резко увеличить 403/429 и деградацию
доступа к данным.
"""

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Загружаем переменные окружения один раз при импорте модуля.
# Это базовая точка управления конфигурацией во всём проекте.
load_dotenv()

# Базовые директории проекта.
BASE_DIR = Path(__file__).parent.parent.absolute()
INPUT_DIR = BASE_DIR / "input"
LOG_DIR = BASE_DIR / "logs"

# Legacy-источник: Excel-файл. Поддерживается для обратной совместимости,
# хотя основной production-поток использует Google Sheets.
EXCEL_DIR = os.getenv("EXCEL_DIR", str(BASE_DIR / "excel_files"))
EXCEL_FILE_PATH = os.getenv("EXCEL_FILE_PATH", str(Path(EXCEL_DIR) / "Артикул.xlsx"))
DATA_SOURCE = os.getenv("DATA_SOURCE", "excel")

# Параметры Google Sheets.
# Имя production-таблицы и рабочей вкладки фиксированы в коде, чтобы
# источник данных не зависел от локального `.env` и порядка листов.
GOOGLE_SHEET_NAME = "UNIT 2.0 (tested)"
GOOGLE_WORKSHEET_NAME = "Конкуренты"
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", str(BASE_DIR / "creds.json"))

# Логирование и checkpoint.
LOG_FILE = os.getenv("LOG_FILE", "wild_position_monitor.log")
LOG_ROTATION = os.getenv("LOG_ROTATION", "1 hour")
LOG_RETENTION = os.getenv("LOG_RETENTION", "24 hours")
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "processing_checkpoint.json")
CHECKPOINT_FILE_PATH = str(LOG_DIR / CHECKPOINT_FILE)

# ClickHouse конфигурация.
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "wild_position_monitor")

# PostgreSQL конфигурация.
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Wildberries HTTP endpoints.
WB_DETAIL_URL = os.getenv("WB_DETAIL_URL", "https://card.wb.ru/cards/v4/detail")
WB_U_CARD_DETAIL_URL = os.getenv(
    "WB_U_CARD_DETAIL_URL",
    "https://www.wildberries.ru/__internal/u-card/cards/v4/detail",
)
WB_SIMILAR_URL = os.getenv("WB_SIMILAR_URL", "https://recom.wb.ru/recom/ru/common/v5/search")
WB_DEFAULT_DEST = os.getenv("WB_DEFAULT_DEST", "-1257786")
WB_DETAIL_ENDPOINT_MODE = os.getenv("WB_DETAIL_ENDPOINT_MODE", "card_v4")
WB_ALLOW_MISSING_PRICE = os.getenv("WB_ALLOW_MISSING_PRICE", "False").lower() == "true"
WB_ALLOW_MISSING_PRODUCT = os.getenv("WB_ALLOW_MISSING_PRODUCT", "False").lower() == "true"
WB_DISABLE_BASKET_FALLBACK_ON_DETAIL_FORBIDDEN = (
    os.getenv("WB_DISABLE_BASKET_FALLBACK_ON_DETAIL_FORBIDDEN", "False").lower() == "true"
)

# Таймаут запроса (сек). Влияет на время ожидания curl_cffi AsyncSession.
WB_TIMEOUT = int(os.getenv("WB_TIMEOUT", "10"))
WB_COOKIE = os.getenv("WB_COOKIE", "")
WB_COOKIE_ENABLED = os.getenv("WB_COOKIE_ENABLED", "False").lower() == "true"
WB_DEVICE_ID = os.getenv("WB_DEVICE_ID", "")
WB_PROXY_URL = os.getenv("WB_PROXY_URL", "")
WB_PROXY_BUNDLES_ENABLED = os.getenv("WB_PROXY_BUNDLES_ENABLED", "False").lower() == "true"
WB_PROXY_ROTATE_ON_CIRCUIT = os.getenv("WB_PROXY_ROTATE_ON_CIRCUIT", "True").lower() == "true"
WB_PROXY_ROTATE_EVERY = int(os.getenv("WB_PROXY_ROTATE_EVERY", "0"))
WB_PROXY_ROTATE_ON_FIRST_FORBIDDEN = (
    os.getenv("WB_PROXY_ROTATE_ON_FIRST_FORBIDDEN", "True").lower() == "true"
)
WB_ALL_BUNDLES_498_COOLDOWN_ENABLED = (
    os.getenv("WB_ALL_BUNDLES_498_COOLDOWN_ENABLED", "False").lower() == "true"
)
WB_ALL_BUNDLES_498_COOLDOWN_SECONDS = int(
    os.getenv("WB_ALL_BUNDLES_498_COOLDOWN_SECONDS", "300")
)
WB_TOKEN_AUTO_REFRESH_ENABLED = os.getenv("WB_TOKEN_AUTO_REFRESH_ENABLED", "False").lower() == "true"
WB_TOKEN_COOKIE_NAME = os.getenv("WB_TOKEN_COOKIE_NAME", "x_wbaas_token")
WB_TOKEN_REFRESH_URL = os.getenv("WB_TOKEN_REFRESH_URL", "https://www.wildberries.ru/")
WB_TOKEN_REFRESH_MAX_ATTEMPTS = int(os.getenv("WB_TOKEN_REFRESH_MAX_ATTEMPTS", "3"))
WB_TOKEN_REFRESH_WAIT_SECONDS = int(os.getenv("WB_TOKEN_REFRESH_WAIT_SECONDS", "5"))
WB_USER_AGENT = os.getenv(
    "WB_USER_AGENT",
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 YaBrowser/26.4.0.0 Safari/537.36"
    ),
)

# Retry-параметры. WARNING: агрессивные значения могут усилить блокировки,
# слишком мягкие — ухудшить полноту данных при транзиентных сбоях.
WB_MAX_RETRIES = int(os.getenv("WB_MAX_RETRIES", "3"))
WB_RETRY_DELAY = float(os.getenv("WB_RETRY_DELAY", "2"))
WB_RATE_LIMIT_DELAY = int(os.getenv("WB_RATE_LIMIT_DELAY", "60"))

# Ограничение частоты запросов (RPS). Ключевой антибот-параметр.
WB_MAX_RPS = int(os.getenv("WB_MAX_RPS", "4"))

# Порог forbidden-ответов для открытия circuit breaker и длительность cooldown.
WB_FORBIDDEN_THRESHOLD = int(os.getenv("WB_FORBIDDEN_THRESHOLD", "8"))
WB_CIRCUIT_COOLDOWN = int(os.getenv("WB_CIRCUIT_COOLDOWN", "20"))

# Параметры session rotation. По умолчанию feature выключен и не влияет
# на production-поведение до отдельного rollout.
WB_SESSION_ROTATION_ENABLED = os.getenv("WB_SESSION_ROTATION_ENABLED", "False").lower() == "true"
WB_SESSION_ROTATE_EVERY = int(os.getenv("WB_SESSION_ROTATE_EVERY", "50"))
WB_SESSION_ROTATION_SCOPE = os.getenv("WB_SESSION_ROTATION_SCOPE", "detail")
WB_SAFE_CONCURRENCY_LIMIT = int(os.getenv("WB_SAFE_CONCURRENCY_LIMIT", "2"))
WB_SAFE_REQUEST_DELAY = float(os.getenv("WB_SAFE_REQUEST_DELAY", "0.25"))
WB_ROLLOUT_ARTICLES_LIMIT = int(os.getenv("WB_ROLLOUT_ARTICLES_LIMIT", "0"))
WB_DETAIL_SUBMIT_DELAY = float(os.getenv("WB_DETAIL_SUBMIT_DELAY", "0"))
WB_SKIP_SIMILAR_STAGE = os.getenv("WB_SKIP_SIMILAR_STAGE", "False").lower() == "true"
WB_BATCH_FORBIDDEN_STOP_LOSS_ENABLED = (
    os.getenv("WB_BATCH_FORBIDDEN_STOP_LOSS_ENABLED", "False").lower() == "true"
)
WB_BATCH_FORBIDDEN_STOP_LOSS_RATIO = float(os.getenv("WB_BATCH_FORBIDDEN_STOP_LOSS_RATIO", "0.35"))
WB_BATCH_FORBIDDEN_STOP_LOSS_MIN_BATCH_SIZE = int(
    os.getenv("WB_BATCH_FORBIDDEN_STOP_LOSS_MIN_BATCH_SIZE", "20")
)

# Батчинг и конкурентность.
# WARNING: повышение значений без A/B-проверки может изменить timing profile
# и привести к росту 403/429 даже при неизменной бизнес-логике.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
CONCURRENT_REQUESTS_LIMIT = int(os.getenv("CONCURRENT_REQUESTS_LIMIT", "5"))
MIN_CONCURRENT_REQUESTS_LIMIT = int(os.getenv("MIN_CONCURRENT_REQUESTS_LIMIT", "2"))
CONCURRENCY_STEP = int(os.getenv("CONCURRENCY_STEP", "1"))
ADAPTIVE_WINDOW_SIZE = int(os.getenv("ADAPTIVE_WINDOW_SIZE", "100"))
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))

# Retry для Google Sheets (синхронный клиент gspread в текущей реализации).
GOOGLE_MAX_RETRIES = int(os.getenv("GOOGLE_MAX_RETRIES", "10"))
GOOGLE_RETRY_DELAY = int(os.getenv("GOOGLE_RETRY_DELAY", "3"))


@dataclass(frozen=True)
class WBProxyBundle:
    """Описывает один согласованный proxy/session bundle для WB."""

    label: str
    proxy_url: str
    cookie: str
    device_id: str


def _load_wb_proxy_bundles() -> list[WBProxyBundle]:
    """Читает proxy bundles из окружения по шаблону `WB_PROXY_XX_*`."""
    bundle_indexes: set[str] = set()
    pattern = re.compile(r"^WB_PROXY_(\d{2})_URL$")
    for key in os.environ:
        match = pattern.match(key)
        if match:
            bundle_indexes.add(match.group(1))

    bundles: list[WBProxyBundle] = []
    for index in sorted(bundle_indexes):
        proxy_url = os.getenv(f"WB_PROXY_{index}_URL", "").strip()
        cookie = os.getenv(f"WB_PROXY_{index}_COOKIE", "").strip()
        device_id = os.getenv(f"WB_PROXY_{index}_DEVICE_ID", "").strip()
        if not proxy_url or not cookie or not device_id:
            continue
        bundles.append(
            WBProxyBundle(
                label=f"proxy_{index}",
                proxy_url=proxy_url,
                cookie=cookie,
                device_id=device_id,
            )
        )
    return bundles


WB_PROXY_BUNDLES = _load_wb_proxy_bundles()
