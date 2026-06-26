"""
Runtime refresh cookies Wildberries through SeleniumBase browser context.

The provider does not persist secrets and does not write to src/config.py.
It opens Wildberries through the current proxy/browser profile, waits for
browser cookies, and returns either a full raw cookie string or a single
`x_wbaas_token` value for narrow diagnostics.
"""

from __future__ import annotations

import time
from typing import Optional

from loguru import logger


class WbTokenProvider:
    """Get runtime WB cookies and x_wbaas_token from SeleniumBase."""

    def __init__(
        self,
        user_agent: str,
        url: str = "https://www.wildberries.ru/",
        cookie_name: str = "x_wbaas_token",
        max_attempts: int = 3,
        wait_seconds: int = 5,
        proxy: str | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.url = url
        self.cookie_name = cookie_name
        self.max_attempts = max_attempts
        self.wait_seconds = wait_seconds
        self.proxy = proxy

    def get_x_wbaas_token(self) -> Optional[str]:
        """Return only x_wbaas_token from a freshly collected cookie bundle."""
        cookie_string = self.get_cookie_string()
        if not cookie_string:
            return None

        for part in cookie_string.split(";"):
            key, separator, value = part.strip().partition("=")
            if separator and key == self.cookie_name:
                logger.info(
                    "New x_wbaas_token received | token={}",
                    self.mask_token(value),
                )
                return value

        logger.warning("x_wbaas_token not found in refreshed cookie bundle")
        return None

    def get_cookie_string(self) -> Optional[str]:
        """
        Open WB through SeleniumBase and return a full raw cookie string.

        SeleniumBase is imported inside the method so the normal project import
        does not depend on a browser stack unless refresh is actually needed.
        """
        driver = None
        try:
            from seleniumbase import Driver

            logger.info("Start WB cookie refresh through SeleniumBase")
            driver = Driver(
                uc=True,
                headed=False,
                headless=True,
                agent=self.user_agent,
                proxy=self.proxy,
            )
            driver.get(self.url)
            self._wait_for_dom_ready(driver, timeout_seconds=self.wait_seconds)
            self._log_browser_state(driver, stage="initial_open")

            for attempt in range(1, self.max_attempts + 1):
                if attempt == 2:
                    self._warmup_homepage(driver)
                elif attempt == 3:
                    self._refresh_page(driver)

                self._wait_for_dom_ready(driver, timeout_seconds=self.wait_seconds)
                cookies = self._collect_cookies(driver)
                raw_cookie_string = self._build_cookie_string(cookies)
                if raw_cookie_string:
                    logger.info(
                        "Fresh WB cookie bundle received | cookies_count={} x_wbaas_token_present={}",
                        len(cookies),
                        self.cookie_name in raw_cookie_string,
                    )
                    return raw_cookie_string

                self._log_browser_state(driver, stage=f"attempt_{attempt}")
                logger.info(
                    "WB cookies are not ready yet | attempt={}/{}",
                    attempt,
                    self.max_attempts,
                )
                time.sleep(self.wait_seconds)

            logger.warning("WB cookies were not found after SeleniumBase refresh")
            return None
        except Exception as exc:
            logger.error("SeleniumBase cookie refresh failed: {}", exc)
            return None
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception as exc:
                    logger.warning("Failed to close SeleniumBase driver: {}", exc)

    def _collect_cookies(self, driver: object) -> list[dict]:
        """
        Collect cookies from the active browser session.

        The primary path uses the standard WebDriver cookie API, which is more
        stable than CDP on Windows. CDP remains as a fallback because some
        browser/runtime combinations can expose a wider cookie set there.
        """
        webdriver_cookies = self._collect_cookies_via_webdriver(driver)
        cdp_cookies = self._collect_cookies_via_cdp(driver)
        return self._merge_cookies(webdriver_cookies, cdp_cookies)

    def _collect_cookies_via_webdriver(self, driver: object) -> list[dict]:
        """Read cookies through the standard Selenium WebDriver API."""
        try:
            cookies = driver.get_cookies()
            logger.debug(
                "WB cookies collected through WebDriver API | cookies_count={}",
                len(cookies or []),
            )
            return cookies or []
        except Exception as exc:
            logger.warning("WB WebDriver cookie read failed: {}", exc)
            return []

    def _collect_cookies_via_cdp(self, driver: object) -> list[dict]:
        """Fallback cookie collection through Chrome DevTools Protocol."""
        try:
            cookies_payload = driver.execute_cdp_cmd("Network.getAllCookies", {})
            cookies = cookies_payload.get("cookies", [])
            logger.debug(
                "WB cookies collected through CDP fallback | cookies_count={}",
                len(cookies or []),
            )
            return cookies or []
        except Exception as exc:
            logger.warning("WB CDP cookie read failed: {}", exc)
            return []

    def _wait_for_dom_ready(self, driver: object, timeout_seconds: int) -> None:
        """Wait briefly until the browser reports a usable document state."""
        deadline = time.monotonic() + max(1, timeout_seconds)
        while time.monotonic() < deadline:
            try:
                ready_state = driver.execute_script("return document.readyState")
                if ready_state in {"interactive", "complete"}:
                    return
            except Exception:
                pass
            time.sleep(0.25)

    def _warmup_homepage(self, driver: object) -> None:
        """Retry cookie bootstrap from the WB homepage."""
        try:
            driver.get("https://www.wildberries.ru/")
            logger.info("WB cookie refresh warmup: homepage reopened")
        except Exception as exc:
            logger.warning("WB cookie refresh homepage warmup failed: {}", exc)

    def _refresh_page(self, driver: object) -> None:
        """Reload the current page as the final lightweight retry step."""
        try:
            driver.refresh()
            logger.info("WB cookie refresh warmup: page refreshed")
        except Exception as exc:
            logger.warning("WB cookie refresh page refresh failed: {}", exc)

    def _log_browser_state(self, driver: object, *, stage: str) -> None:
        """Log a safe summary of the browser state without leaking secrets."""
        try:
            cookies = self._collect_cookies(driver)
            current_url = str(getattr(driver, "current_url", "") or "")
            title = str(getattr(driver, "title", "") or "").strip()
            ready_state = self._get_ready_state(driver)
            page_hint = self._get_page_hint(driver)
            cookie_names = {str(cookie.get("name") or "") for cookie in cookies}
            logger.info(
                "WB browser state | stage={} url={} title={} ready_state={} cookies_count={} token_present={} page_hint={}",
                stage,
                current_url[:200],
                title[:120],
                ready_state,
                len(cookies),
                self.cookie_name in cookie_names,
                page_hint,
            )
        except Exception as exc:
            logger.warning("WB browser state logging failed: stage={} error={}", stage, exc)

    def _get_ready_state(self, driver: object) -> str:
        """Return DOM readyState for safe diagnostics."""
        try:
            return str(driver.execute_script("return document.readyState") or "")
        except Exception:
            return "unknown"

    def _get_page_hint(self, driver: object) -> str:
        """Extract a compact page hint from title/source for diagnostics."""
        fragments: list[str] = []
        try:
            title = str(getattr(driver, "title", "") or "").lower()
            page_source = str(getattr(driver, "page_source", "") or "").lower()
            haystack = f"{title}\n{page_source[:4000]}"
        except Exception:
            return "unavailable"

        patterns = (
            ("captcha", "captcha"),
            ("challenge", "challenge"),
            ("access denied", "access_denied"),
            ("forbidden", "forbidden"),
            ("too many requests", "too_many_requests"),
            ("cloudflare", "cloudflare"),
            ("robot", "robot"),
            ("wbaas", "wbaas"),
            ("wildberries", "wildberries"),
        )
        for needle, label in patterns:
            if needle in haystack:
                fragments.append(label)

        if not fragments:
            return "no_known_markers"
        return ",".join(fragments)

    @staticmethod
    def _merge_cookies(*cookie_lists: list[dict]) -> list[dict]:
        """Merge cookies by name, preferring the first non-empty occurrence."""
        merged: dict[str, dict] = {}
        for cookie_list in cookie_lists:
            for cookie in cookie_list:
                name = str(cookie.get("name") or "").strip()
                value = str(cookie.get("value") or "").strip()
                if not name or not value or name in merged:
                    continue
                merged[name] = cookie
        return list(merged.values())

    @staticmethod
    def _build_cookie_string(cookies: list[dict]) -> str:
        """Convert browser cookies into a raw Cookie header string."""
        parts: list[str] = []
        for cookie in cookies:
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "").strip()
            if not name or not value:
                continue
            parts.append(f"{name}={value}")
        return "; ".join(parts)

    @staticmethod
    def mask_token(token: str) -> str:
        """Mask token value to avoid leaking runtime secrets to logs."""
        if not token:
            return "<empty>"
        if len(token) <= 10:
            return f"{token[:2]}...{token[-2:]}"
        return f"{token[:6]}...{token[-4:]}"
