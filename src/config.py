"""
Конфигурационный файл проекта WildPositionMonitor
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Базовые пути
BASE_DIR = Path(__file__).parent.parent.absolute()
INPUT_DIR = os.path.join(BASE_DIR, 'input')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Директория с Excel файлами (устаревшее)
EXCEL_DIR = os.getenv("EXCEL_DIR", os.path.join(BASE_DIR, 'excel_files'))

# Путь к конкретному Excel файлу с артикулами
EXCEL_FILE_PATH = os.getenv("EXCEL_FILE_PATH", os.path.join(EXCEL_DIR, 'Артикул.xlsx'))

# Источник данных (excel или google_sheets)
DATA_SOURCE = os.getenv("DATA_SOURCE", "excel")

# Название Google таблицы
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Конкурентный анализ Вектор")

# Путь к файлу с учетными данными для Google Sheets
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", os.path.join(BASE_DIR, 'creds.json'))

LOG_FILE = "wild_position_monitor.log"
WB_PRICE_LOG_FILE = "wb_price_parser.log"
WB_PRICE_ERRORS_CSV = "wb_price_parser_errors.csv"
WB_COOKIES_FILE = os.getenv("WB_COOKIES_FILE", "secrets/wb_cookies.txt")
LOG_CLEANUP_ENABLED = os.getenv("LOG_CLEANUP_ENABLED", "true").lower() == "true"
LOG_CLEANUP_INTERVAL_HOURS = int(os.getenv("LOG_CLEANUP_INTERVAL_HOURS", "24"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
LOG_CLEANUP_STATE_FILE = os.path.join(LOG_DIR, ".last_cleanup")

# ClickHouse конфигурация
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "wild_position_monitor")

# PostgreSQL конфигурация
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

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
