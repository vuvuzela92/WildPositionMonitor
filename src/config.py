"""
Конфигурационный файл проекта WildPositionMonitor.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent.absolute()
INPUT_DIR = BASE_DIR / "input"
LOG_DIR = BASE_DIR / "logs"

EXCEL_DIR = os.getenv("EXCEL_DIR", str(BASE_DIR / "excel_files"))
EXCEL_FILE_PATH = os.getenv("EXCEL_FILE_PATH", str(Path(EXCEL_DIR) / "Артикул.xlsx"))
DATA_SOURCE = os.getenv("DATA_SOURCE", "excel")

GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Конкурентный анализ Вектор")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", str(BASE_DIR / "creds.json"))

LOG_FILE = os.getenv("LOG_FILE", "wild_position_monitor.log")
LOG_ROTATION = os.getenv("LOG_ROTATION", "1 hour")
LOG_RETENTION = os.getenv("LOG_RETENTION", "24 hours")
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "processing_checkpoint.json")
CHECKPOINT_FILE_PATH = str(LOG_DIR / CHECKPOINT_FILE)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "wild_position_monitor")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

WB_DETAIL_URL = os.getenv("WB_DETAIL_URL", "https://card.wb.ru/cards/v4/detail")
WB_SIMILAR_URL = os.getenv("WB_SIMILAR_URL", "https://recom.wb.ru/recom/ru/common/v5/search")
WB_DEFAULT_DEST = os.getenv("WB_DEFAULT_DEST", "-1257786")
WB_TIMEOUT = int(os.getenv("WB_TIMEOUT", "10"))
WB_MAX_RETRIES = int(os.getenv("WB_MAX_RETRIES", "3"))
WB_RETRY_DELAY = float(os.getenv("WB_RETRY_DELAY", "2"))
WB_RATE_LIMIT_DELAY = int(os.getenv("WB_RATE_LIMIT_DELAY", "60"))
WB_MAX_RPS = int(os.getenv("WB_MAX_RPS", "4"))
WB_FORBIDDEN_THRESHOLD = int(os.getenv("WB_FORBIDDEN_THRESHOLD", "8"))
WB_CIRCUIT_COOLDOWN = int(os.getenv("WB_CIRCUIT_COOLDOWN", "20"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
CONCURRENT_REQUESTS_LIMIT = int(os.getenv("CONCURRENT_REQUESTS_LIMIT", "5"))
MIN_CONCURRENT_REQUESTS_LIMIT = int(os.getenv("MIN_CONCURRENT_REQUESTS_LIMIT", "2"))
CONCURRENCY_STEP = int(os.getenv("CONCURRENCY_STEP", "1"))
ADAPTIVE_WINDOW_SIZE = int(os.getenv("ADAPTIVE_WINDOW_SIZE", "100"))
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))

GOOGLE_MAX_RETRIES = int(os.getenv("GOOGLE_MAX_RETRIES", "10"))
GOOGLE_RETRY_DELAY = int(os.getenv("GOOGLE_RETRY_DELAY", "3"))
