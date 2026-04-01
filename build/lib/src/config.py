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

# Wildberries API параметры
WB_DETAIL_URL = "https://card.wb.ru/cards/v4/detail"
WB_SIMILAR_URL = "https://recom.wb.ru/recom/ru/common/v5/search"
WB_DEFAULT_DEST = "-1257786"
WB_TIMEOUT = 10  # секунды
WB_MAX_RETRIES = 3
WB_RETRY_DELAY = 2  # секунды
WB_RATE_LIMIT_DELAY = 60  # секунды задержки при получении 429 статуса

# Параметры асинхронной обработки
BATCH_SIZE = 100  # Размер батча для обработки артикулов
CONCURRENT_REQUESTS_LIMIT = 10  # Максимальное количество одновременных запросов к API
DB_POOL_SIZE = 5  # Размер пула соединений с базой данных

# Параметры Google Sheets
GOOGLE_MAX_RETRIES = 10  # Максимальное количество попыток подключения к Google Sheets
GOOGLE_RETRY_DELAY = 3  # Задержка между попытками подключения (секунды)
