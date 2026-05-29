"""Диагностика получения x_wbaas_token без запуска основного парсера."""

import sys
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import (
    WB_TOKEN_COOKIE_NAME,
    WB_TOKEN_REFRESH_MAX_ATTEMPTS,
    WB_TOKEN_REFRESH_URL,
    WB_TOKEN_REFRESH_WAIT_SECONDS,
    WB_USER_AGENT,
)
from src.wb_token_provider import WbTokenProvider


def main() -> None:
    provider = WbTokenProvider(
        user_agent=WB_USER_AGENT,
        url=WB_TOKEN_REFRESH_URL,
        cookie_name=WB_TOKEN_COOKIE_NAME,
        max_attempts=WB_TOKEN_REFRESH_MAX_ATTEMPTS,
        wait_seconds=WB_TOKEN_REFRESH_WAIT_SECONDS,
    )
    token = provider.get_x_wbaas_token()
    if token:
        logger.success("x_wbaas_token получен: {}", provider.mask_token(token))
        return

    logger.error("x_wbaas_token получить не удалось")


if __name__ == "__main__":
    main()
