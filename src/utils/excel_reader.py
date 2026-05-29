"""Утилиты чтения артикулов из Excel/CSV (legacy-источник).

Модуль сохранён для обратной совместимости и локальных сценариев.
Основной production-поток сейчас читает вход из Google Sheets.
"""

import os
from typing import List, Optional

import pandas as pd
from loguru import logger

from src.config import EXCEL_FILE_PATH


class ExcelReader:
    """Читает список артикулов из файла и нормализует к `List[int]`."""

    def __init__(self):
        """Инициализирует путь к файлу из конфигурации."""
        self.logger = logger
        self.file_path = EXCEL_FILE_PATH

        if not os.path.exists(self.file_path):
            self.logger.error("Файл не найден: {}", self.file_path)

    def get_articles_from_file(self, file_path: Optional[str] = None) -> List[int]:
        """Возвращает артикулы из столбца `Артикул`.

        Параметры:
        - `file_path`: явный путь к файлу; если не задан, берётся из config.

        Возвращает:
        - список целочисленных артикулов;
        - пустой список при ошибке.

        Особенности:
        - поддерживает `.csv`, `.xlsx`, `.xls`;
        - для CSV применяет fallback-кодировки (`utf-8` -> `cp1251` -> `utf-8;sep=';'`).

        WARNING:
        В проекте это legacy-механизм. Изменение правил парсинга нужно проверять
        на исторических входных файлах, чтобы не потерять совместимость.
        """
        if file_path is None:
            file_path = self.file_path

        if not os.path.exists(file_path):
            self.logger.error("Файл не найден: {}", file_path)
            return []

        try:
            _, ext = os.path.splitext(file_path)
            ext = ext.lower()

            if ext == ".csv":
                try:
                    df = pd.read_csv(file_path, encoding="utf-8")
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(file_path, encoding="cp1251")
                    except UnicodeDecodeError:
                        df = pd.read_csv(file_path, encoding="utf-8", sep=";")
            elif ext in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path)
            else:
                self.logger.error("Неподдерживаемый формат файла: {}", ext)
                return []

            self.logger.info("Успешно прочитан файл: {}", file_path)
            self.logger.info("Количество строк: {}", len(df))

            if "Артикул" not in df.columns:
                self.logger.error(
                    "Столбец 'Артикул' не найден. Доступные столбцы: {}",
                    ", ".join(df.columns),
                )
                return []

            # Приводим столбец к числам, отбрасываем мусор/NaN.
            df["Артикул"] = pd.to_numeric(df["Артикул"], errors="coerce")
            articles = df["Артикул"].dropna().astype("int64").tolist()

            if not articles:
                self.logger.warning("Не найдено числовых артикулов в файле")
                return []

            self.logger.info("Найдено {} артикулов", len(articles))
            return articles
        except Exception as exc:
            self.logger.error("Ошибка при чтении файла {}: {}", file_path, exc)
            return []
