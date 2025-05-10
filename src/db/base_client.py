"""
Базовый клиент для работы с базами данных
"""
from abc import ABC, abstractmethod
from loguru import logger


class BaseDBClient(ABC):
    """Абстрактный базовый класс для клиентов баз данных"""

    def __init__(self, connection_params: dict):
        """
        Инициализация базового клиента

        Args:
            connection_params: Параметры подключения к базе данных
        """
        self.connection_params = connection_params
        self.connection = None
        self.logger = logger

    @abstractmethod
    def connect(self) -> bool:
        """
        Устанавливает соединение с базой данных

        Returns:
            bool: True в случае успеха, False в случае ошибки
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с базой данных"""
        pass
