"""
Модуль для работы с Google Sheets.
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
    """Класс для чтения артикулов из Google Sheets."""

    def __init__(self):
        self.logger = logger
        self.creds_path = GOOGLE_CREDS_PATH
        self.sheet_name = GOOGLE_SHEET_NAME
        self.max_retries = GOOGLE_MAX_RETRIES
        self.retry_delay = GOOGLE_RETRY_DELAY

        if not os.path.exists(self.creds_path):
            self.logger.error("Файл учетных данных Google Sheets не найден: path={}", self.creds_path)

    def client_init_json(self) -> Optional[Client]:
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
