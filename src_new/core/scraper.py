# src/wb/services/http_client.py
import asyncio
import aiohttp
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class HTTPClient:
    """
    Универсальный клиент для HTTP-запросов.
    Поддерживает как авторизованные, так и публичные запросы.
    """
    
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: Optional[str] = None,
        account: Optional[str] = None,
        timeout: float = 60.0,
        headers: dict = None
    ):
        self.session = session
        self.api_key = api_key
        self.account = account or "Public"
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        if headers:
            self.headers = headers

        else:
            self.headers = {"Authorization": api_key} if api_key else {}
    
    async def request(
        self,
        method: str,
        url: str,
        params: Optional[Dict] = None,
        json: Optional[Dict] = None,
        headers: Optional[Dict] = None, 
        retries: int = 3,
        delay: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        Делает HTTP-запрос с повторными попытками.
        """
        # Объединяем заголовки (метод может переопределить стандартные)
        request_headers = {**self.headers, **(headers or {})}
        
        # Красивое имя для логов
        log_name = f"[{self.account}]" if self.account else ""
        
        for attempt in range(1, retries + 1):
            try:
                async with self.session.request(
                    method,
                    url,
                    headers=request_headers, 
                    params=params,
                    json=json,
                    timeout=self.timeout
                ) as res:
                    if res.status == 200:
                        return await res.json()
                    elif res.status == 429:
                        logger.warning(f"⏳ {log_name} Лимит (429). Попытка {attempt}/{retries}")
                        await asyncio.sleep(delay * attempt)
                        continue
                    elif res.status == 401:
                        if self.api_key:  # ← Ошибка только если токен был
                            logger.error(f"🔑 {log_name} Неверный токен (401)")
                        else:
                            logger.error(f"🔑 {log_name} Требуется авторизация (401)")
                        return None
                    elif res.status >= 500:
                        logger.error(f"🖥 {log_name} Ошибка сервера ({res.status}). Попытка {attempt}/{retries}")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        error_text = await res.text()
                        logger.error(f"❌ {log_name} Ошибка {res.status}: {error_text}")
                        return None
                        
            except aiohttp.ClientError as e:
                logger.error(f"💥 {log_name} Сетевая ошибка: {e}")
                if attempt < retries:
                    await asyncio.sleep(delay)
                else:
                    return None
            except asyncio.TimeoutError:
                logger.error(f"⏰ {log_name} Таймаут запроса")
                if attempt < retries:
                    await asyncio.sleep(delay)
                else:
                    return None
        
        return None
    
    # Обертка get запроса 
    async def get(self, url: str, **kwargs) -> Optional[Dict]:
        return await self.request("GET", url, **kwargs)
    # Обертка post запроса 
    async def post(self, url: str, **kwargs) -> Optional[Dict]:
        return await self.request("POST", url, **kwargs)