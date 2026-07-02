"""Утилиты чтения входных артикулов из Google Sheets.

Модуль выполняет:
- авторизацию service-account в Google API;
- открытие нужной таблицы и вкладки;
- чтение строк листа;
- нормализацию и валидацию поля артикула.

WARNING:
Текущая реализация синхронная (gspread) и вызывается из async-процесса.
Менять ее на асинхронную нужно отдельно, чтобы не сломать текущий lifecycle.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

from gspread import Client, Worksheet, service_account
from loguru import logger

from src.config import (
    GOOGLE_CREDS_PATH,
    GOOGLE_MAX_RETRIES,
    GOOGLE_RETRY_DELAY,
    GOOGLE_SHEET_NAME,
    GOOGLE_WORKSHEET_NAME,
)


class GoogleSheetsReader:
    """Читает и подготавливает артикула из Google Sheets."""

    REQUIRED_ARTICLE_COLUMN = "Артикул конкурента"
    WILD_COLUMN = "wild"
    COMPETITOR_STATUS_COLUMN = "Статус конкурента"
    TARGET_COLUMNS = {
        REQUIRED_ARTICLE_COLUMN,
        WILD_COLUMN,
        COMPETITOR_STATUS_COLUMN,
    }

    def __init__(self) -> None:
        self.logger = logger
        self.creds_path = GOOGLE_CREDS_PATH
        self.sheet_name = GOOGLE_SHEET_NAME
        self.worksheet_name = GOOGLE_WORKSHEET_NAME
        self.max_retries = GOOGLE_MAX_RETRIES
        self.retry_delay = GOOGLE_RETRY_DELAY

        if not os.path.exists(self.creds_path):
            self.logger.error(
                "Файл учетных данных Google Sheets не найден: path={}",
                self.creds_path,
            )

    def client_init_json(self) -> Optional[Client]:
        """Инициализирует gspread client через service account."""
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

    def connect_to_sheet(self, sheet_name: str) -> tuple[Optional[Worksheet], Optional[str]]:
        """Открывает worksheet по имени таблицы и имени вкладки."""
        retries = 0
        while retries < self.max_retries:
            try:
                client = self.client_init_json()
                if not client:
                    return None, "gsheets_client_not_initialized"

                self.logger.info(
                    "Открытие Google таблицы: sheet_name={} worksheet_name={} попытка {} из {}",
                    sheet_name,
                    self.worksheet_name,
                    retries + 1,
                    self.max_retries,
                )
                sheet = client.open(sheet_name)
                worksheet = sheet.worksheet(self.worksheet_name)
                if not worksheet:
                    raise RuntimeError(f"worksheet_not_found:{self.worksheet_name}")

                self.logger.info(
                    "Открытие Google таблицы: успешно sheet_name={} worksheet_name={}",
                    sheet_name,
                    self.worksheet_name,
                )
                return worksheet, None
            except Exception as exc:
                retries += 1
                self.logger.warning(
                    "Открытие Google таблицы не удалось: sheet_name={} worksheet_name={} попытка {} из {} retry_delay_s={} error={}",
                    sheet_name,
                    self.worksheet_name,
                    retries,
                    self.max_retries,
                    self.retry_delay,
                    exc,
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    error_msg = (
                        "Не удалось открыть Google таблицу: "
                        f"sheet_name={sheet_name} worksheet_name={self.worksheet_name} "
                        f"max_attempts={self.max_retries}"
                    )
                    self.logger.exception("{} error={}", error_msg, exc)
                    return None, error_msg
        return None, "gsheets_open_unknown_failure"

    def get_articles_from_sheet(self, sheet_name: Optional[str] = None) -> list[dict[str, Any]]:
        """Читает артикула и возвращает нормализованный список словарей."""
        target_sheet_name = sheet_name or self.sheet_name
        self.logger.info(
            "Чтение Google таблицы: старт sheet_name={} worksheet_name={}",
            target_sheet_name,
            self.worksheet_name,
        )
        try:
            worksheet, error = self.connect_to_sheet(target_sheet_name)
            if not worksheet:
                self.logger.error(
                    "Чтение Google таблицы: ошибка sheet_name={} worksheet_name={} error={}",
                    target_sheet_name,
                    self.worksheet_name,
                    error,
                )
                return []

            rows = worksheet.get_all_values()
            if not rows:
                self.logger.warning(
                    "В Google таблице нет данных: sheet_name={} worksheet_name={}",
                    target_sheet_name,
                    self.worksheet_name,
                )
                return []

            headers = [str(value).strip() for value in rows[0]]
            header_indexes = self._build_header_indexes(headers)

            if self.REQUIRED_ARTICLE_COLUMN not in header_indexes:
                self.logger.error(
                    "В Google таблице отсутствует обязательный столбец: column={} available_columns={}",
                    self.REQUIRED_ARTICLE_COLUMN,
                    headers,
                )
                return []

            if self.WILD_COLUMN not in header_indexes:
                self.logger.warning(
                    "В Google таблице отсутствует опциональный столбец: column={}",
                    self.WILD_COLUMN,
                )
            if self.COMPETITOR_STATUS_COLUMN not in header_indexes:
                self.logger.warning(
                    "В Google таблице отсутствует опциональный столбец: column={}",
                    self.COMPETITOR_STATUS_COLUMN,
                )

            prepared_rows: list[dict[str, Any]] = []
            skipped_empty_articles = 0
            skipped_invalid_articles = 0
            for row in rows[1:]:
                raw_article = self._get_cell_value(
                    row=row,
                    header_indexes=header_indexes,
                    column_name=self.REQUIRED_ARTICLE_COLUMN,
                )
                if raw_article == "":
                    skipped_empty_articles += 1
                    continue

                article_id = self._parse_article_id(raw_article)
                if article_id is None:
                    skipped_invalid_articles += 1
                    continue

                prepared_rows.append(
                    {
                        "article_id": article_id,
                        "wild": self._get_cell_value(
                            row=row,
                            header_indexes=header_indexes,
                            column_name=self.WILD_COLUMN,
                        ),
                        "competitor_status": self._get_cell_value(
                            row=row,
                            header_indexes=header_indexes,
                            column_name=self.COMPETITOR_STATUS_COLUMN,
                        ),
                    }
                )

            if not prepared_rows:
                self.logger.warning(
                    "В Google таблице не найдено валидных артикулов: sheet_name={} worksheet_name={}",
                    target_sheet_name,
                    self.worksheet_name,
                )
                return []

            self.logger.info(
                "Артикулы из Google таблицы подготовлены: sheet_name={} worksheet_name={} count={} skipped_empty_articles={} skipped_invalid_articles={}",
                target_sheet_name,
                self.worksheet_name,
                len(prepared_rows),
                skipped_empty_articles,
                skipped_invalid_articles,
            )
            return prepared_rows
        except Exception as exc:
            self.logger.exception(
                "Исключение при чтении Google таблицы: sheet_name={} worksheet_name={} error={}",
                target_sheet_name,
                self.worksheet_name,
                exc,
            )
            return []

    def _build_header_indexes(self, headers: list[str]) -> dict[str, int]:
        """Строит индекс нужных заголовков, игнорируя пустые и лишние колонки."""
        header_indexes: dict[str, int] = {}
        for index, header in enumerate(headers):
            if not header or header not in self.TARGET_COLUMNS or header in header_indexes:
                continue
            header_indexes[header] = index
        return header_indexes

    @staticmethod
    def _get_cell_value(
        row: list[str],
        header_indexes: dict[str, int],
        column_name: str,
    ) -> str:
        """Безопасно возвращает строковое значение ячейки по имени колонки."""
        column_index = header_indexes.get(column_name)
        if column_index is None or column_index >= len(row):
            return ""
        return str(row[column_index]).strip()

    @staticmethod
    def _parse_article_id(raw_value: str) -> Optional[int]:
        """Нормализует значение артикула из Google Sheets в int."""
        normalized = raw_value.strip().replace(" ", "")
        if not normalized:
            return None
        try:
            return int(float(normalized))
        except (TypeError, ValueError):
            return None
