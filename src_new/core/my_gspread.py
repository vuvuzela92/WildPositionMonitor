import re
import os
import time
import logging
import gspread
import pandas as pd
from datetime import datetime
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from gspread.utils import rowcol_to_a1

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class GoogleTabs():
    """Класс для работы с гугл таблицами"""
    def __init__(self, table_title: str, sheet_title: str):
        """ Инициализируем класс для работы с гугл-таблицами.
        spreadsheet_title: Название гугл-таблицы
        worksheet_title: Название страницы"""
        self.creds_file = Path(__file__).resolve().parents[2]/'creds/creds.json' # Доступы к таблицам
        self.table_title = table_title # Названия таблиц
        self.table = None  # ← Открытая таблица
        self.sheet_title = sheet_title
        self._safe_connect()  # ← Подключаемся сразу

    def _safe_connect(self, retries=5, delay=2):        
            """
            Пытается открыть таблицу и лист с повторными попытками.
            """
            self.gc = gspread.service_account(filename=self.creds_file)
            
            for attempt in range(1, retries + 1):
                try:
                    # 1. Открываем саму таблицу
                    table = self.gc.open(self.table_title)
                    self.table = table # Сохраняем в атрибут класса
                    
                    # 2. Открываем конкретный лист 
                    self.sheet_title = table.worksheet(self.sheet_title) 
                    
                    print(f"✅ Успешное подключение к {self.table_title} -> {self.sheet_title.title}")
                    return # Выходим из функции, всё готово
                    
                except gspread.exceptions.APIError as e:
                    if "503" in str(e):
                        print(f"[Попытка {attempt}/{retries}] APIError 503 — повтор через {delay} сек.")
                        time.sleep(delay)
                    else:
                        raise 
                except gspread.exceptions.WorksheetNotFound:
                    raise RuntimeError(f"Ошибка: Лист '{self.sheet_title}' не найден в таблице '{self.table_title}'")

            raise RuntimeError(f"Не удалось открыть таблицу '{self.table_title}' после {retries} попыток.")

    def _update_df_in_google(self, df: pd.DataFrame, sheet):
        """
        Перезаписывает данные DataFrame на указанный лист Google Таблицы.
        Также добавляет дату и время последнего обновления в первую строку последней колонки.
        
        Параметры:
        df (DataFrame): DataFrame, который нужно отправить.
        sheet (gspread.models.Worksheet): Объект листа, на который будут записаны данные.

        Возвращаемое значение:
        None
        """
        try:
            # Обрабатываем NaN значения в DataFrame (заменяем на пустые строки)
            df = df.fillna('')

            # Очищаем лист перед записью новых данных
            sheet.clear()

            # Подготовка данных для записи
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()

            # Запись данных на лист
            sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
            print("Данные успешно перезаписаны на лист.")

        except Exception as e:
            print(f"Произошла ошибка: {e}")
            # Проверяем на ошибку, связанную с лимитом ячеек
            if "APIError: [400]: This action would increase the number of cells in the workbook" in str(e):
                print("Превышен лимит ячеек Google Таблицы. Создание резервной копии в Excel...")

    def _send_df_to_google(self, df, sheet):
        """
        Отправляет DataFrame на указанный лист Google Таблицы.

        Параметры:
        df (DataFrame): DataFrame, который нужно отправить.
        sheet (gspread.models.Worksheet): Объект листа, на который будут добавлены данные.

        Возвращаемое значение:
        None
        """
        try:
            # Данные, которые нужно добавить
            df_data_to_append = [df.columns.values.tolist()] + df.values.tolist()
            
            # Проверка существующих данных на листе
            existing_data = sheet.get_all_values()
            
            if len(existing_data) <= 1:  # Если данных нет
                print("Добавляем заголовки и данные")
                sheet.append_rows(df_data_to_append, value_input_option='USER_ENTERED')
            else:
                print("Добавляем только данные")
                sheet.append_rows(df_data_to_append[1:], value_input_option='USER_ENTERED')
                
        except Exception as e:
            print(f"An error occurred: {e}")

    def update_column_by_name(self, column_name: str, data_to_write: list):
            """
            Находит колонку по названию и обновляет её содержимое, начиная со 2-й строки.
            data_to_write: плоский список значений ['реклама', '', 'реклама'...]
            """
            try:
                # 1. Получаем все заголовки из первой строки
                headers = self.sheet_title.row_values(1)
                
                if column_name not in headers:
                    raise ValueError(f"Колонка '{column_name}' не найдена в таблице!")

                # 2. Определяем индекс колонки (в gspread нумерация с 1)
                col_idx = headers.index(column_name) + 1
                
                # 3. Готовим данные: gspread ожидает список списков для диапазона
                # Пример: [['реклама'], [''], ['реклама']]
                vertical_values = [[val] for val in data_to_write]
                
                # 4. Определяем диапазон. Например, если col_idx=3, то это будет "C2:C100"
                from gspread.utils import rowcol_to_a1
                start_cell = rowcol_to_a1(2, col_idx) # Строка 2, нужная колонка
                end_cell = rowcol_to_a1(len(data_to_write) + 1, col_idx)
                range_label = f"{start_cell}:{end_cell}"

                # 5. Обновляем данные одной командой
                self.sheet_title.update(range_label, vertical_values)
                print(f"✅ Данные успешно записаны в колонку '{column_name}' (диапазон {range_label})")

            except Exception as e:
                print(f"❌ Ошибка при динамическом обновлении: {e}")

    def set_df_to_google(self, df: pd.DataFrame):

        df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        date_columns = ['date', 'updated_at', 'created_at', 'date_from', 'date_to', 'dt', 'start_date', 'end_date', 'month', 'supply_date', 'Дата создания документа', 'Дата поставки', 'Ожидаемая дата прихода']
        # Преобразуем все колонки с датами в строковый формат, если они есть в DataFrame
        for col in date_columns:
            if col in df.columns:  
                df[col] = df[col].astype(str)
            else:
                continue

        try:
            google_connect = GoogleTabs(
                table_title=self.table_title,
                sheet_title=self.sheet_title.title
            )

            ws = google_connect.sheet_title

            # --- 1. Подготовка данных ---
            df = df.copy()

            # сохраняем типы
            df = df.where(pd.notnull(df), None)
            values = [df.columns.tolist()] + df.values.tolist()

            # Определяем конечный диапазон для обновления (например, A1:Z1000)
            rows = len(values) # Количество строк, включая заголовок
            cols = len(values[0]) # Количество столбцов

            end_range = rowcol_to_a1(rows, cols) # Конвертируем в формат A1 (например, Z1000)

            # --- 2. Перезапись данных ---
            ws.update(
                        f"A1:{end_range}",
                        values,
                        value_input_option="USER_ENTERED"
                    )

            # --- 3. если таблица была больше раньше ---
            ws.batch_clear([f"{end_range}:Z"])

            print("Таблица полностью обновлена")

        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Не найдена таблица {self.table_title}")
        except gspread.exceptions.WorksheetNotFound:
            print(f"Не найден лист {self.sheet_title.title}")
        except Exception as e:
            print(f"Ошибка: {e}")