"""
Runtime cookie manager for Wildberries.

The manager keeps WB cookies only in process memory. It can refresh either the
full cookie bundle or only x_wbaas_token through WbTokenProvider, without
writing secrets back to config files.
"""

from __future__ import annotations

from typing import Optional

from loguru import logger

from src.wb_token_provider import WbTokenProvider


class WbCookieManager:
    """Keep current WB cookies in memory and refresh runtime session cookies."""

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
        """Return raw cookie string in the format already expected by the parser."""
        return self._cookies

    def refresh_full_cookies(self) -> bool:
        """Refresh the full cookie bundle in process memory."""
        if not self.auto_refresh_enabled:
            logger.warning("WB full-cookie auto refresh is disabled by config")
            return False

        old_cookies = self._cookies
        new_cookies = self.token_provider.get_cookie_string()
        if not new_cookies:
            logger.warning("WB full-cookie refresh failed: provider returned empty cookie string")
            return False

        self._cookies = new_cookies
        self.last_refresh_changed = new_cookies != old_cookies
        logger.info(
            "WB full cookies refreshed in process memory | changed={} token={}",
            self.last_refresh_changed,
            self.get_masked_token(),
        )
        return True

    def refresh_x_wbaas_token(self) -> bool:
        """
        Refresh only x_wbaas_token in the current raw cookie string.

        This narrow fallback stays useful when a full cookie refresh is either
        unavailable or too expensive for the current recovery path.
        """
        if not self.auto_refresh_enabled:
            logger.warning("x_wbaas_token auto refresh is disabled by config")
            return False

        old_token = self.extract_cookie_value(self._cookies, self.cookie_name)
        new_token = self.token_provider.get_x_wbaas_token()
        if not new_token:
            logger.warning("x_wbaas_token was not refreshed: provider returned empty token")
            return False

        self._cookies = self.replace_cookie_value(
            raw_cookies=self._cookies,
            cookie_name=self.cookie_name,
            new_value=new_token,
        )
        self.last_refresh_changed = new_token != old_token
        logger.info(
            "Cookies refreshed in process memory | {}={} | changed={}",
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
        """Replace cookie by name or append it if it does not exist yet."""
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
        """Return masked current token for safe diagnostics."""
        return self.mask_token(
            self.extract_cookie_value(self._cookies, self.cookie_name) or ""
        )

    @staticmethod
    def extract_cookie_value(raw_cookies: str, cookie_name: str) -> Optional[str]:
        """Extract one cookie value from raw string without logging the whole cookie."""
        for part in raw_cookies.split(";"):
            key, separator, value = part.strip().partition("=")
            if separator and key == cookie_name:
                return value.strip()
        return None

    @staticmethod
    def mask_token(token: str) -> str:
        """Mask token so the full runtime secret never appears in logs."""
        if not token:
            return "<empty>"
        if len(token) <= 10:
            return f"{token[:2]}...{token[-2:]}"
        return f"{token[:6]}...{token[-4:]}"
