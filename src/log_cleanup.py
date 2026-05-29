"""
Безопасная очистка служебных логов проекта.

Модуль удаляет только старые .log и .csv файлы внутри папки logs. Он не знает
ничего о cookies, исходном коде или настройках окружения и специально не выходит
за пределы log_dir, чтобы очистка не могла затронуть секреты или данные проекта.
"""

from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger


def cleanup_old_log_files(
    log_dir: str,
    retention_days: int,
    cleanup_state_file: str,
    cleanup_interval_hours: int = 24,
    enabled: bool = True,
) -> None:
    """
    Очищает старые .log и .csv файлы в папке logs не чаще заданного интервала.

    Очистка запускается один раз на старт процесса, а не в цикле по артикулам.
    Это защищает парсер от лишней работы и исключает риск удалить активный файл
    прямо во время частой записи. Файлы моложе retention_days не трогаются.
    """
    if not enabled:
        logger.info("Очистка логов отключена настройкой LOG_CLEANUP_ENABLED")
        return

    log_path = Path(log_dir).resolve()
    state_path = Path(cleanup_state_file).resolve()
    log_path.mkdir(parents=True, exist_ok=True)

    if not _is_path_inside(state_path, log_path):
        logger.warning("Файл состояния очистки находится вне LOG_DIR: {}", state_path)
        return

    now = datetime.now()
    if not _cleanup_is_due(state_path, now, cleanup_interval_hours):
        return

    cutoff = now - timedelta(days=retention_days)
    deleted_count = 0

    for file_path in log_path.iterdir():
        try:
            resolved_file = file_path.resolve()
            if not _is_path_inside(resolved_file, log_path):
                continue
            if not resolved_file.is_file():
                continue
            if resolved_file.suffix.lower() not in {".log", ".csv"}:
                continue

            modified_at = datetime.fromtimestamp(resolved_file.stat().st_mtime)
            if modified_at >= cutoff:
                continue

            resolved_file.unlink()
            deleted_count += 1
            logger.info("Удалён старый файл логов: {}", resolved_file)
        except Exception as exc:
            logger.warning("Не удалось очистить файл логов {}: {}", file_path, exc)

    try:
        state_path.write_text(now.isoformat(timespec="seconds"), encoding="utf-8")
    except Exception as exc:
        logger.warning("Не удалось обновить файл состояния очистки логов: {}", exc)

    logger.info(
        "Очистка логов завершена | deleted={} | retention_days={}",
        deleted_count,
        retention_days,
    )


def _cleanup_is_due(
    state_path: Path,
    now: datetime,
    cleanup_interval_hours: int,
) -> bool:
    """Проверяет, прошло ли достаточно времени с последней очистки."""
    if not state_path.exists():
        return True

    try:
        last_cleanup = datetime.fromisoformat(state_path.read_text(encoding="utf-8").strip())
    except Exception as exc:
        logger.warning("Не удалось прочитать время последней очистки логов: {}", exc)
        return True

    return now - last_cleanup >= timedelta(hours=cleanup_interval_hours)


def _is_path_inside(candidate: Path, parent: Path) -> bool:
    """Защита от удаления файлов вне LOG_DIR через случайный или неверный путь."""
    try:
        candidate.relative_to(parent)
        return True
    except ValueError:
        return False
