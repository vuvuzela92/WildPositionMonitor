"""
Модуль настройки логирования для всего приложения
"""
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from loguru import logger

from src.config import LOG_DIR, LOG_FILE


class InterceptHandler(logging.Handler):
    """Обработчик для перенаправления стандартных логов Python в loguru"""
    def emit(self, record):
        # Получаем соответствующий уровень логирования loguru
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Находим вызывающий файл и строку
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logger():
    """
    Настраивает и возвращает настроенный логгер
    
    Настройки логирования:
    - Логи в консоль (INFO и выше)
    - Логи в файл с ротацией (максимальный размер файла 10 МБ)
    - При превышении размера файла старые записи перезаписываются
    """
    # Создаем директорию для логов, если она не существует
    os.makedirs(LOG_DIR, exist_ok=True)
    
    log_file_path = os.path.join(LOG_DIR, LOG_FILE)
    
    # Удаляем все обработчики loguru
    logger.remove()
    
    # Добавляем вывод в консоль
    logger.add(
        sys.stdout, 
        level="INFO", 
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
    )
    
    # Настраиваем RotatingFileHandler с максимальным размером 10 МБ и одним файлом бэкапа
    # Это обеспечит, что будет использоваться только один файл, который не превысит 10 МБ
    file_handler = RotatingFileHandler(
        log_file_path, 
        maxBytes=10*1024*1024,  # 10 МБ
        backupCount=0,  # Не создавать дополнительные файлы для бэкапа
        encoding='utf-8'
    )
    
    # Настраиваем формат логов
    log_format = logging.Formatter(
        "{asctime} | {levelname:<8} | {message}", 
        "%Y-%m-%d %H:%M:%S", 
        style="{"
    )
    file_handler.setFormatter(log_format)
    file_handler.setLevel(logging.ERROR)
    
    # Настраиваем корневой логгер стандартной библиотеки Python
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.ERROR)
    root_logger.addHandler(file_handler)
    
    # Перехват сообщений стандартного логгера Python в loguru
    root_logger.addHandler(InterceptHandler())
    
    logger.info(f"Логгер настроен, файл: {log_file_path} (макс. 10 МБ)")
    
    return logger
