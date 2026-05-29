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
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Конкурентный анализ Вектор")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", str(BASE_DIR / "creds.json"))

# Логирование и checkpoint.
LOG_FILE = os.getenv("LOG_FILE", "wild_position_monitor.log")
LOG_ROTATION = os.getenv("LOG_ROTATION", "1 hour")
LOG_RETENTION = os.getenv("LOG_RETENTION", "24 hours")
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "processing_checkpoint.json")
CHECKPOINT_FILE_PATH = str(LOG_DIR / CHECKPOINT_FILE)

<<<<<<< HEAD
LOG_FILE = "wild_position_monitor.log"
WB_PRICE_LOG_FILE = "wb_price_parser.log"
WB_PRICE_ERRORS_CSV = "wb_price_parser_errors.csv"
WB_COOKIES_FILE = os.getenv("WB_COOKIES_FILE", "secrets/wb_cookies.txt")
LOG_CLEANUP_ENABLED = os.getenv("LOG_CLEANUP_ENABLED", "true").lower() == "true"
LOG_CLEANUP_INTERVAL_HOURS = int(os.getenv("LOG_CLEANUP_INTERVAL_HOURS", "24"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
LOG_CLEANUP_STATE_FILE = os.path.join(LOG_DIR, ".last_cleanup")

# ClickHouse конфигурация
=======
# ClickHouse конфигурация.
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
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

<<<<<<< HEAD
# --- Находится в конце файла src/config.py ---

# Wildberries API параметры
WB_DETAIL_URL = "https://www.wildberries.ru/__internal/card/cards/v4/detail"
WB_SIMILAR_URL = "https://recom.wb.ru/recom/ru/common/v5/search"
WB_DEFAULT_DEST = "-1257786"
WB_TIMEOUT = 20  # секунды
WB_MAX_RETRIES = 3
WB_RETRY_DELAY = 2  # секунды
WB_RATE_LIMIT_DELAY = 60  # секунды задержки при получении 429 статуса
MAX_TOKEN_REFRESH_RETRIES = int(os.getenv("MAX_TOKEN_REFRESH_RETRIES", "1"))
MAX_CONSECUTIVE_498_ERRORS = int(os.getenv("MAX_CONSECUTIVE_498_ERRORS", "20"))
WB_TOKEN_AUTO_REFRESH_ENABLED = os.getenv("WB_TOKEN_AUTO_REFRESH_ENABLED", "true").lower() == "true"
WB_TOKEN_REFRESH_URL = os.getenv("WB_TOKEN_REFRESH_URL", "https://www.wildberries.ru/")
WB_TOKEN_COOKIE_NAME = os.getenv("WB_TOKEN_COOKIE_NAME", "x_wbaas_token")
WB_TOKEN_REFRESH_MAX_ATTEMPTS = int(os.getenv("WB_TOKEN_REFRESH_MAX_ATTEMPTS", "3"))
WB_TOKEN_REFRESH_WAIT_SECONDS = int(os.getenv("WB_TOKEN_REFRESH_WAIT_SECONDS", "5"))
WB_TOKEN_REFRESH_MAX_RETRIES_PER_ARTICLE = int(
    os.getenv("WB_TOKEN_REFRESH_MAX_RETRIES_PER_ARTICLE", "1")
)

# Параметры авторизации для обхода защиты (подтягиваются из .env или берутся дефолтные)
WB_RAW_COOKIES = (
    "external-locale=ru; _wbauid=2477851001764666101; "
    "x-supplier-id-external=11ab4eb4-7970-46fa-bee0-d2f552620e8a; "
    "cfidsw-wb=pKF0dyplztYFHsrWM52CB0N5POPzetZFp/och+l+rkWy/y3oCRCbsdHV7vusHD9eFP8UGr/K8k97bawlCxiwqnsyCm0g7WTdywRJ8DSsBCVMeukjfwjf9R02557auDiZtRpBrnUwp6Ws/9imc4hClvAE8SlEagmF90aQon0=; "
    "__zzatw-wb=MDA0dC0yYBwREFsKEH49WgsbSl1pCENQGC9LXz1uLWEPJ3wjYnwgGWsvC1RDMmUIPkBNOTM5NGZwVydgTmAkS1ZVfycdEndtH0FLVCNyM3dlaXceViUTFmcPRyJ1F0hAGxI6aCU6f1JpGWUzDldjGAsmVDVfP3wnHxZ3byxScX9NfXY3PmJ+MQ9pOSRjCh9+OFoLDWk3XBQ8dWU+SHR4MTxtI2FLWh9EUT9FbllGaXUVF0M8HHsNKkNtLToZUXYQQlh4cBpEN0AYfxVZUnUpbn06MBtFVx0YTF4jQw8JfyciQ3skKVQ4EmNudnN1Lz8eURp7FiJER0lrZU5TQixmG3EVTQgNND1aciIPWzklWAgSPwsmIBh8cyRXDQ1fPkFubxt/Nl0cOWMRCxl+OmNdRkc3FSR7dSYKCTU3YnAvTCB7SykWRxsyYV5GaXUVAg8FUF2J3cmQm4kX0RdJXkOSjNaThF0JyUNDBBdQ0NtMDFsVxlRDxZhDhYYRRcje0I3Yhk4QhgvPV8/YngiD2lIYCZIXU4JKxsVeXEpS3FPLH12X30beylOIA0lVBMhP05yxKmKfQ==; "
    "x_wbaas_token=1.1000.a7e6c50bd317439caceed17fac650bda.MHw3OC4xNDIuMjM5LjYxfE1vemlsbGEvNS4wIChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hUTUwsIGxpa2UgR2Vja28pIENocm9tZS8xNDYuMC4wLjAgWWFCcm93c2VyLzI2LjQuMC4wIFNhZmFyaS81MzcuMzZ8MTc4MDIzODEyNHxyZXVzYWJsZXwyfGV5Sm9ZWE5vSWpvaUluMD18MnwzfDE3ODAxMDg1MjR8MQ==.MEUCIAJEngwOuyU2zoDURJUqptDXIvY3mYdrCXUxKJOVnnl2AiEAlyC6g+k1ZPx1L24NbrSeenlzDjN+VefxejcARgYAldw="
)

WB_USER_AGENT = os.getenv("WB_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36")

# Параметры асинхронной обработки
BATCH_SIZE = 50  # Размер батча для обработки артикулов
CONCURRENT_REQUESTS_LIMIT = 5  # Максимальное количество одновременных запросов к API
DB_POOL_SIZE = 5  # Размер пула соединений с базой данных

# Параметры Google Sheets
GOOGLE_MAX_RETRIES = 10  # Максимальное количество попыток подключения к Google Sheets
GOOGLE_RETRY_DELAY = 3  # Задержка между попытками подключения (секунды)
=======
# Wildberries HTTP endpoints.
WB_DETAIL_URL = os.getenv("WB_DETAIL_URL", "https://card.wb.ru/cards/v4/detail")
WB_SIMILAR_URL = os.getenv("WB_SIMILAR_URL", "https://recom.wb.ru/recom/ru/common/v5/search")
WB_DEFAULT_DEST = os.getenv("WB_DEFAULT_DEST", "-1257786")

# Таймаут запроса (сек). Влияет на время ожидания curl_cffi AsyncSession.
WB_TIMEOUT = int(os.getenv("WB_TIMEOUT", "10"))

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
>>>>>>> 39e1d09fbb95eba434b392739d843118dfd5a507
