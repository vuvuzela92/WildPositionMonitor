"""Базовые контракты для DB-клиентов.

Модуль задаёт минимальный абстрактный интерфейс клиентов БД.
Это legacy-слой: текущая реализация в проекте частично использует
конкретные клиенты напрямую, но общий контракт полезен для тестов и
дальнейшего расширения.
"""

from abc import ABC, abstractmethod

from loguru import logger


class BaseDBClient(ABC):
    """Абстрактный базовый класс для клиентов баз данных."""

    def __init__(self, connection_params: dict):
        """Сохраняет параметры подключения и создаёт logger-хэндл.

        Параметры:
        - `connection_params`: словарь с host/port/user/password/database.

        Побочный эффект:
        - инициализирует состояние соединения (`self.connection = None`).
        """
        self.connection_params = connection_params
        self.connection = None
        self.logger = logger

    @abstractmethod
    def connect(self) -> bool:
        """Устанавливает соединение с базой данных.

        Возвращает:
        - `True`, если соединение успешно;
        - `False`, если соединение не установлено.
        """

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с базой данных."""
