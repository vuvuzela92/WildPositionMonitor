"""Утилиты чтения входных артикулов из Google Sheets.

Модуль выполняет:
- авторизацию service-account в Google API;
- открытие рабочего листа;
- чтение строк в pandas DataFrame;
- нормализацию и валидацию поля артикула.

WARNING:
Текущая реализация синхронная (gspread + pandas) и вызывается из async-процесса.
Менять её на асинхронную нужно отдельным анализом, чтобы не сломать
существующий lifecycle и retry-поведение.
"""

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from gspread import Client, Worksheet, service_account
from loguru import logger

from src.config import (
    GOOGLE_CREDS_PATH,
    GOOGLE_MAX_RETRIES,
    GOOGLE_RETRY_DELAY,
    GOOGLE_SHEET_NAME,
)


class GoogleSheetsReader:
    """Читает и подготавливает артикулы из Google Sheets."""

    def __init__(self):
        """Инициализирует настройки источника и retry-параметры."""
        self.logger = logger
        self.creds_path = GOOGLE_CREDS_PATH
        self.sheet_name = GOOGLE_SHEET_NAME
        self.max_retries = GOOGLE_MAX_RETRIES
        self.retry_delay = GOOGLE_RETRY_DELAY

        if not os.path.exists(self.creds_path):
            self.logger.error("Файл учетных данных Google Sheets не найден: path={}", self.creds_path)

    def client_init_json(self) -> Optional[Client]:
        """Инициализирует gspread client через service account.

        Возвращает:
        - объект `Client` при успехе;
        - `None` после исчерпания retry.

        Почему sync-retry:
        - gspread здесь используется синхронно;
        - метод вызывается до запуска интенсивной WB-части, поэтому простая
          блокирующая задержка приемлема в текущей архитектуре.
        """
        retries = 0
        while retries < self.max_retries:
            try:
                self.logger.debug(
                    "Инициализация клиента Google Sheets: попытка {} из {}",
                    retries + 1,
                    self.max_retries,
                )
                return service_account(filename=self.creds_path)
            except Exception as exc:
                retries += 1
                self.logger.warning(
                    "Инициализация клиента Google Sheets не удалась: попытка {} из {}, retry_delay_s={}, error={}",
                    retries,
                    self.max_retries,
                    self.retry_delay,
                    exc,
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.exception(
                        "Инициализация клиента Google Sheets провалена после {} попыток, error={}",
                        self.max_retries,
                        exc,
                    )
                    return None
        return None

    def connect_to_sheet(self, sheet_name: str) -> Tuple[Optional[Worksheet], Optional[str]]:
        """Открывает worksheet по имени таблицы.

        Возвращает:
        - `(worksheet, None)` при успехе;
        - `(None, error_message)` при неуспехе.
        """
        retries = 0
        while retries < self.max_retries:
            try:
                client = self.client_init_json()
                if not client:
                    return None, "gsheets_client_not_initialized"

                self.logger.info(
                    "Открытие Google таблицы: sheet_name={} попытка {} из {}",
                    sheet_name,
                    retries + 1,
                    self.max_retries,
                )
                sheet = client.open(sheet_name)
                worksheet = sheet.get_worksheet(0)
                if not worksheet:
                    raise RuntimeError("worksheet_not_found")

                self.logger.info("Открытие Google таблицы: успешно sheet_name={}", sheet_name)
                return worksheet, None
            except Exception as exc:
                retries += 1
                self.logger.warning(
                    "Открытие Google таблицы не удалось: sheet_name={} попытка {} из {} retry_delay_s={} error={}",
                    sheet_name,
                    retries,
                    self.max_retries,
                    self.retry_delay,
                    exc,
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    error_msg = f"Не удалось открыть Google таблицу: sheet_name={sheet_name} max_attempts={self.max_retries}"
                    self.logger.exception("{} error={}", error_msg, exc)
                    return None, error_msg
        return None, "gsheets_open_unknown_failure"

    def get_articles_from_sheet(self, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Читает артикулы и возвращает нормализованный список словарей.

        Источник столбцов:
        - обязательный: `Артикул конкурента`;
        - опциональные: `wild`, `Статус конкурента`.

        Возвращает:
        - список элементов формата:
          `{\"article_id\": int, \"wild\": str, \"competitor_status\": str}`.

        WARNING:
        Код опирается на текущий формат Google Sheets. Переименование столбцов
        без синхронной правки здесь приведёт к пустому входу в мониторинг.
        """
        sheet_name = sheet_name or self.sheet_name
        self.logger.info("Чтение Google таблицы: старт sheet_name={}", sheet_name)
        try:
            worksheet, error = self.connect_to_sheet(sheet_name)
            if not worksheet:
                self.logger.error("Чтение Google таблицы: ошибка sheet_name={} error={}", sheet_name, error)
                return []

            all_data = worksheet.get_all_records()
            df = pd.DataFrame(all_data)
            self.logger.info("Чтение Google таблицы: успешно sheet_name={} rows={}", sheet_name, len(df))

            source_col = "Артикул конкурента"
            if source_col not in df.columns:
                self.logger.error(
                    "В Google таблице отсутствует обязательный столбец: column={} available_columns={}",
                    source_col,
                    list(df.columns),
                )
                return []

            if "wild" not in df.columns:
                self.logger.warning("В Google таблице отсутствует опциональный столбец: column=wild")
            if "Статус конкурента" not in df.columns:
                self.logger.warning("В Google таблице отсутствует опциональный столбец: column=Статус конкурента")

            # Нормализуем артикулы в int64 и отбрасываем нечисловые значения.
            df["Артикул"] = pd.to_numeric(df[source_col], errors="coerce")
            df = df.dropna(subset=["Артикул"])
            df["Артикул"] = df["Артикул"].astype("int64")

            if len(df) == 0:
                self.logger.warning("В Google таблице не найдено числовых артикулов: sheet_name={}", sheet_name)
                return []

            result: List[Dict[str, Any]] = []
            for _, row in df.iterrows():
                result.append(
                    {
                        "article_id": int(row["Артикул"]),
                        "wild": row.get("wild", "") if "wild" in df.columns else "",
                        "competitor_status": row.get("Статус конкурента", "") if "Статус конкурента" in df.columns else "",
                    }
                )

            self.logger.info("Артикулы из Google таблицы подготовлены: sheet_name={} count={}", sheet_name, len(result))
            return result
        except Exception as exc:
            self.logger.exception("Исключение при чтении Google таблицы: sheet_name={} error={}", sheet_name, exc)
            return []
