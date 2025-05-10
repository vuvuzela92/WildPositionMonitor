"""
Упрощенный модуль для работы с Excel файлом и извлечения данных
"""
from typing import List, Optional
import os
import pandas as pd
from loguru import logger

from src.config import EXCEL_FILE_PATH


class ExcelReader:
    """Упрощенный класс для работы с Excel файлом"""
    
    def __init__(self):
        """Инициализация класса"""
        self.logger = logger
        self.file_path = EXCEL_FILE_PATH
        
        # Проверяем, существует ли файл
        if not os.path.exists(self.file_path):
            self.logger.error(f"Файл не найден: {self.file_path}")
    
    def get_articles_from_file(self, file_path: Optional[str] = None) -> List[int]:
        """
        Получает список артикулов из файла из столбца 'Артикул'
        
        Args:
            file_path: Опциональный путь к файлу (если None, используется из конфигурации)
            
        Returns:
            List[int]: Список артикулов
        """
        if file_path is None:
            file_path = self.file_path
            
        if not os.path.exists(file_path):
            self.logger.error(f"Файл не найден: {file_path}")
            return []
            
        try:
            # Определяем расширение файла
            _, ext = os.path.splitext(file_path)
            ext = ext.lower()
            
            # Читаем файл в зависимости от формата
            if ext == '.csv':
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    try:
                        df = pd.read_csv(file_path, encoding='cp1251')
                    except UnicodeDecodeError:
                        df = pd.read_csv(file_path, encoding='utf-8', sep=';')
            elif ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                self.logger.error(f"Неподдерживаемый формат файла: {ext}")
                return []
                
            self.logger.info(f"Успешно прочитан файл: {file_path}")
            self.logger.info(f"Количество строк: {len(df)}")
            
            # Проверяем наличие столбца 'Артикул'
            if 'Артикул' not in df.columns:
                self.logger.error(f"Столбец 'Артикул' не найден в файле. Доступные столбцы: {', '.join(df.columns)}")
                return []
                
            # Преобразуем столбец к числовому типу
            df['Артикул'] = pd.to_numeric(df['Артикул'], errors='coerce')
            
            # Отбрасываем NaN значения и преобразуем к int
            articles = df['Артикул'].dropna().astype('int64').tolist()
            
            # Проверка на пустой список
            if not articles:
                self.logger.warning("Не найдено числовых артикулов в файле")
                return []
                
            self.logger.info(f"Найдено {len(articles)} артикулов")

                
            return articles
            
        except Exception as e:
            self.logger.error(f"Ошибка при чтении файла {file_path}: {e}")
            return []