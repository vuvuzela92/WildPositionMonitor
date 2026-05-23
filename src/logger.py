"""
Модуль настройки централизованного логирования проекта.
"""

import logging
import os
import sys

from loguru import logger

from src.config import LOG_DIR, LOG_FILE, LOG_RETENTION, LOG_ROTATION


class InterceptHandler(logging.Handler):
    """Перенаправляет стандартный logging в loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logger():
    """
    Настраивает логирование:
    - stdout: INFO+
    - файл: INFO+ с ротацией по времени и retention за 24 часа
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(LOG_DIR, LOG_FILE)

    logger.remove()
    base_format = (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | "
        "{name}:{function}:{line} | {message}"
    )

    logger.add(
        sys.stdout,
        level="INFO",
        format=base_format,
        enqueue=True,
    )
    logger.add(
        log_file_path,
        level="INFO",
        format=base_format,
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        encoding="utf-8",
        enqueue=True,
    )

    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(InterceptHandler())

    logger.info(
        "Логгер инициализирован, файл={} rotation={} retention={}",
        log_file_path,
        LOG_ROTATION,
        LOG_RETENTION,
    )
    return logger
