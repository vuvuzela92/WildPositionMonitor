"""Централизованная настройка логирования проекта.

Модуль формирует единый pipeline логов:
- `loguru` как основной sink и форматтер,
- перехват стандартного `logging` через `InterceptHandler`,
- ротация и retention лог-файлов без внешнего cron.

Это важно для эксплуатации: в проекте много async-операций, и однородный формат
логов нужен для диагностики race-сценариев, retry-поведения и сетевых деградаций.
"""

import logging
import os
import sys

from loguru import logger

from src.config import LOG_DIR, LOG_FILE, LOG_RETENTION, LOG_ROTATION


class InterceptHandler(logging.Handler):
    """Перенаправляет стандартный `logging` в `loguru`.

    Зачем это нужно:
    - сторонние библиотеки (asyncpg, gspread и т.д.) часто пишут в `logging`,
      а не в `loguru`;
    - без перехвата пришлось бы анализировать логи в разных форматах.

    Побочный эффект:
    - все сообщения через `logging` начинают подчиняться уровню/формату `loguru`.
    """

    def emit(self, record: logging.LogRecord) -> None:
        """Преобразует запись `logging` в событие `loguru`.

        Параметры:
        - `record`: исходная запись стандартного логгера.

        Риски:
        - если изменить логику вычисления `depth`, трассировка в логах
          может указывать на неверный уровень вызова.
        """
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            # Фолбэк на числовой уровень, если у loguru нет именованного уровня.
            level = record.levelno

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logger():
    """Настраивает централизованное логирование приложения.

    Что делает:
    - создаёт директорию логов, если её нет;
    - добавляет stdout-sink (оперативный runtime просмотр);
    - добавляет file-sink с ротацией и retention (операционный архив);
    - перенаправляет стандартный `logging` в `loguru`.

    Почему так:
    - проект запускается регулярно, поэтому ротация/retention должны работать
      автоматически без внешних скриптов очистки;
    - в async-контуре важно включать `enqueue=True`, чтобы избежать блокировки
      основных задач при записи в лог.

    Возвращает:
    - глобальный объект `loguru.logger` после настройки.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file_path = os.path.join(LOG_DIR, LOG_FILE)

    # Сбрасываем sinks, чтобы повторный вызов не дублировал сообщения.
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

    # Подключаем перехват стандартного logging.
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
