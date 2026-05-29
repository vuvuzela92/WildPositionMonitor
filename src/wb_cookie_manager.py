"""
Runtime-менеджер cookies Wildberries.

config.py остаётся статической конфигурацией и не используется как хранилище
runtime-состояния. При HTTP 498 менеджер получает новый x_wbaas_token через
WbTokenProvider и заменяет только этот cookie в текущей raw cookie string.
"""

from typing import Optional

from loguru import logger

from src.wb_token_provider import WbTokenProvider


class WbCookieManager:
    """Хранит текущие WB cookies в памяти процесса и обновляет x_wbaas_token."""

    def __init__(
        self,
        raw_cookies: str,
        token_provider: WbTokenProvider,
        cookie_name: str = "x_wbaas_token",
        auto_refresh_enabled: bool = True,
    ) -> None:
        self._cookies = raw_cookies
        self.token_provider = token_provider
        self.cookie_name = cookie_name
        self.auto_refresh_enabled = auto_refresh_enabled
        self.last_refresh_changed = False

    def get_cookies(self) -> str:
        """Возвращает raw cookie string в формате, который уже ожидает парсер."""
        return self._cookies

    def refresh_x_wbaas_token(self) -> bool:
        """
        Получает новый x_wbaas_token и обновляет cookies только в памяти.

        В первой версии меняется только значение x_wbaas_token. Если после
        refresh WB всё равно возвращает 498, это будет видно в логах; значит,
        следующий шаг - обновлять всю cookie-сессию, а не один параметр.
        """
        if not self.auto_refresh_enabled:
            logger.warning("Автообновление x_wbaas_token отключено настройкой")
            return False

        old_token = self.extract_cookie_value(self._cookies, self.cookie_name)
        new_token = self.token_provider.get_x_wbaas_token()
        if not new_token:
            logger.warning("x_wbaas_token не обновлён: provider вернул пустой токен")
            return False

        self._cookies = self.replace_cookie_value(
            raw_cookies=self._cookies,
            cookie_name=self.cookie_name,
            new_value=new_token,
        )
        self.last_refresh_changed = new_token != old_token
        logger.info(
            "Cookies обновлены в памяти процесса | {}={} | changed={}",
            self.cookie_name,
            self.mask_token(new_token),
            self.last_refresh_changed,
        )
        return True

    def replace_cookie_value(
        self,
        raw_cookies: str,
        cookie_name: str,
        new_value: str,
    ) -> str:
        """Заменяет cookie по имени или добавляет его, если в строке его нет."""
        cookie_parts = []
        replaced = False

        for part in raw_cookies.split(";"):
            stripped = part.strip()
            if not stripped:
                continue

            key, separator, value = stripped.partition("=")
            if separator and key == cookie_name:
                cookie_parts.append(f"{cookie_name}={new_value}")
                replaced = True
            else:
                cookie_parts.append(stripped)

        if not replaced:
            cookie_parts.append(f"{cookie_name}={new_value}")

        return "; ".join(cookie_parts)

    def get_masked_token(self) -> str:
        """Возвращает маскированный текущий токен для безопасной диагностики."""
        return self.mask_token(
            self.extract_cookie_value(self._cookies, self.cookie_name) or ""
        )

    @staticmethod
    def extract_cookie_value(raw_cookies: str, cookie_name: str) -> Optional[str]:
        """Достаёт значение cookie из raw string без логирования всей строки."""
        for part in raw_cookies.split(";"):
            key, separator, value = part.strip().partition("=")
            if separator and key == cookie_name:
                return value.strip()
        return None

    @staticmethod
    def mask_token(token: str) -> str:
        """Маскирует токен, чтобы он не попал в логи целиком."""
        if not token:
            return "<empty>"
        if len(token) <= 10:
            return f"{token[:2]}...{token[-2:]}"
        return f"{token[:6]}...{token[-4:]}"
