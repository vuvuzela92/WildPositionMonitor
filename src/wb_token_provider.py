"""
Получение нового x_wbaas_token через браузерный контекст SeleniumBase.

Этот модуль не хранит cookies и не меняет src/config.py. Его единственная
ответственность - открыть страницу Wildberries, дождаться cookie с нужным именем
и вернуть только значение токена. Полный токен в логи не пишется.
"""

import time
from typing import Optional

from loguru import logger


class WbTokenProvider:
    """Получает x_wbaas_token из cookies браузерной сессии SeleniumBase."""

    def __init__(
        self,
        user_agent: str,
        url: str = "https://www.wildberries.ru/",
        cookie_name: str = "x_wbaas_token",
        max_attempts: int = 3,
        wait_seconds: int = 5,
    ) -> None:
        self.user_agent = user_agent
        self.url = url
        self.cookie_name = cookie_name
        self.max_attempts = max_attempts
        self.wait_seconds = wait_seconds

    def get_x_wbaas_token(self) -> Optional[str]:
        """
        Открывает WB через SeleniumBase и ищет x_wbaas_token в cookies.

        SeleniumBase импортируется внутри метода, чтобы обычный импорт проекта не
        падал на окружениях, где браузерная диагностика не используется. Driver
        обязательно закрывается в finally, иначе зависшие процессы браузера со
        временем начнут мешать парсеру.
        """
        driver = None
        try:
            from seleniumbase import Driver

            logger.info("Запускаем обновление x_wbaas_token через SeleniumBase")
            driver = Driver(
                uc=True,
                headed=False,
                headless=True,
                agent=self.user_agent,
            )
            driver.open(self.url)

            for attempt in range(1, self.max_attempts + 1):
                cookies = driver.execute_cdp_cmd("Network.getAllCookies", {})
                for cookie in cookies.get("cookies", []):
                    if cookie.get("name") == self.cookie_name:
                        token = cookie.get("value")
                        logger.info(
                            "Новый x_wbaas_token получен | token={}",
                            self.mask_token(token or ""),
                        )
                        return token

                logger.info(
                    "x_wbaas_token пока не найден | attempt={}/{}",
                    attempt,
                    self.max_attempts,
                )
                time.sleep(self.wait_seconds)

            logger.warning("x_wbaas_token не найден после SeleniumBase refresh")
            return None
        except Exception as exc:
            logger.error("Ошибка SeleniumBase при получении x_wbaas_token: {}", exc)
            return None
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception as exc:
                    logger.warning("Не удалось закрыть SeleniumBase driver: {}", exc)

    @staticmethod
    def mask_token(token: str) -> str:
        """Маскирует токен: в логах нельзя раскрывать runtime-секреты."""
        if not token:
            return "<empty>"
        if len(token) <= 10:
            return f"{token[:2]}...{token[-2:]}"
        return f"{token[:6]}...{token[-4:]}"
