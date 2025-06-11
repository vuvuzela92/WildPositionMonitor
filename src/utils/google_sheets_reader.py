"""
Модуль для работы с Google Sheets и извлечения данных
"""
from typing import List, Optional, Tuple, Any, Dict
import os
import time
from gspread import Client, service_account, Worksheet
from loguru import logger
import pandas as pd
import numpy as np

from src.config import (
    GOOGLE_SHEET_NAME, 
    GOOGLE_CREDS_PATH, 
    GOOGLE_MAX_RETRIES, 
    GOOGLE_RETRY_DELAY
)


class GoogleSheetsReader:
    """Класс для работы с Google Sheets"""
    
    def __init__(self):
        """Инициализация класса"""
        self.logger = logger
        self.creds_path = GOOGLE_CREDS_PATH
        self.sheet_name = GOOGLE_SHEET_NAME
        self.max_retries = GOOGLE_MAX_RETRIES
        self.retry_delay = GOOGLE_RETRY_DELAY

        if not os.path.exists(self.creds_path):
            self.logger.error(f"Файл учетных данных не найден: {self.creds_path}")
    
    def client_init_json(self) -> Optional[Client]:
        """
        Создание клиента для работы с Google Sheets с повторными попытками
        Returns:
            Optional[Client]: Клиент для работы с Google Sheets или None в случае ошибки
        """
        retries = 0
        while retries < self.max_retries:
            try:
                return service_account(filename=self.creds_path)
            except Exception as e:
                retries += 1
                self.logger.warning(
                    f"Попытка {retries}/{self.max_retries} подключения к Google Sheets не удалась: {e}"
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"Не удалось инициализировать клиент Google Sheets после {self.max_retries} попыток")
                    return None
    
    def connect_to_sheet(self, sheet_name: str) -> Tuple[Optional[Worksheet], Optional[str]]:
        """
        Подключение к Google таблице с повторными попытками
        Args:
            sheet_name: Название таблицы
        Returns:
            Tuple[Optional[Worksheet], Optional[str]]: 
                Кортеж из (рабочий лист, сообщение об ошибке)
        """
        retries = 0
        while retries < self.max_retries:
            try:
                # Получаем клиент для работы с Google Sheets
                client = self.client_init_json()
                if not client:
                    return None, "Не удалось инициализировать клиент Google Sheets"
                
                # Открываем таблицу по названию
                self.logger.info(f"Попытка {retries+1}/{self.max_retries} открыть таблицу: {sheet_name}")
                sheet = client.open(sheet_name)
                
                # Выбираем первый лист (worksheet)
                worksheet = sheet.get_worksheet(0)
                if not worksheet:
                    raise Exception("Не удалось получить лист таблицы")
                
                self.logger.info(f"Успешно подключено к таблице: {sheet_name}")
                return worksheet, None
            
            except Exception as e:
                retries += 1
                self.logger.warning(
                    f"Попытка {retries}/{self.max_retries} подключения к таблице {sheet_name} не удалась: {e}"
                )
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    error_msg = f"Не удалось подключиться к таблице {sheet_name} после {self.max_retries} попыток"
                    self.logger.error(error_msg)
                    return None, error_msg
    
    def get_articles_from_sheet(self, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Получает список артикулов и дополнительной информации из Google Таблицы
        Args:
            sheet_name: Опциональное название таблицы (если None, используется SHEET_NAME)
        Returns:
            List[Dict[str, Any]]: Список словарей с информацией об артикулах и дополнительных полях
        """
        if sheet_name is None:
            sheet_name = self.sheet_name
            
        try:
            worksheet, error = self.connect_to_sheet(sheet_name)
            if not worksheet:
                self.logger.error(error)
                return []

            all_data = worksheet.get_all_records()

            df = pd.DataFrame(all_data)
            
            self.logger.info(f"Успешно прочитана Google таблица: {sheet_name}")
            self.logger.info(f"Количество строк: {len(df)}")

            if 'Артикул конкурента' not in df.columns:
                self.logger.error(f"Столбец 'Артикул конкурента' не найден в таблице. Доступные столбцы: {', '.join(df.columns)}")
                return []

            # Проверяем наличие необходимых столбцов
            if 'wild' not in df.columns:
                self.logger.warning(f"Столбец 'wild' не найден в таблице. Будет использовано значение по умолчанию.")
            
            if 'Статус конкурента' not in df.columns:
                self.logger.warning(f"Столбец 'Статус конкурента' не найден в таблице. Будет использовано значение по умолчанию.")
            
            # Преобразуем столбец с артикулами к числовому типу
            df['Артикул'] = pd.to_numeric(df['Артикул конкурента'], errors='coerce')
            
            # Отбрасываем строки с NaN в столбце Артикул
            df = df.dropna(subset=['Артикул'])
            
            # Преобразуем артикулы к целочисленному типу
            df['Артикул'] = df['Артикул'].astype('int64')
            
            if len(df) == 0:
                self.logger.warning("Не найдено числовых артикулов в таблице")
                return []
            
            # Создаем список словарей с артикулами и дополнительной информацией
            result = []
            for _, row in df.iterrows():
                article_info = {
                    'article_id': int(row['Артикул']),
                    'wild': row.get('wild', '') if 'wild' in df.columns else '',
                    'competitor_status': row.get('Статус конкурента', '') if 'Статус конкурента' in df.columns else ''
                }
                result.append(article_info)
                
            self.logger.info(f"Найдено {len(result)} артикулов в Google таблице")
                
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка при чтении Google таблицы {sheet_name}: {e}")
            return []
