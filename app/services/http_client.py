import asyncio
import json
import logging
import random
from typing import Dict, List, Optional, Sequence, Set
from urllib.parse import urlparse

import aiohttp

from config import (
    SCRAPER_BACKOFF_BASE_SECONDS,
    SCRAPER_BACKOFF_MAX_SECONDS,
    SCRAPER_MAX_DELAY_SECONDS,
    SCRAPER_MAX_RETRIES,
    SCRAPER_MIN_DELAY_SECONDS,
    SCRAPER_PROXIES,
    SCRAPER_TIMEOUT_SECONDS,
)

DEFAULT_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

RETRYABLE_STATUS_CODES: Set[int] = {
    403,
    408,
    409,
    425,
    429,
    500,
    502,
    503,
    504,
    520,
    521,
    522,
    524,
}


class DomainRateLimiter:
    """Controla la frecuencia de requests por dominio para reducir bloqueos."""

    def __init__(self, min_delay_seconds: float, max_delay_seconds: float) -> None:
        self._min_delay_seconds = max(0.0, min_delay_seconds)
        self._max_delay_seconds = max(self._min_delay_seconds, max_delay_seconds)
        self._last_hit_by_domain: Dict[str, float] = {}
        self._locks_by_domain: Dict[str, asyncio.Lock] = {}

    async def wait_turn(self, domain: str) -> None:
        if not domain:
            return

        lock = self._locks_by_domain.setdefault(domain, asyncio.Lock())
        async with lock:
            now = asyncio.get_running_loop().time()
            last_hit = self._last_hit_by_domain.get(domain, 0.0)
            target_gap = random.uniform(self._min_delay_seconds, self._max_delay_seconds)
            elapsed = now - last_hit

            if elapsed < target_gap:
                await asyncio.sleep(target_gap - elapsed)

            self._last_hit_by_domain[domain] = asyncio.get_running_loop().time()


class ResilientScraperClient:
    """Cliente HTTP asíncrono con backoff, rotación de identidad y control de ritmo."""

    def __init__(
        self,
        timeout_seconds: float = SCRAPER_TIMEOUT_SECONDS,
        max_retries: int = SCRAPER_MAX_RETRIES,
        min_delay_seconds: float = SCRAPER_MIN_DELAY_SECONDS,
        max_delay_seconds: float = SCRAPER_MAX_DELAY_SECONDS,
        backoff_base_seconds: float = SCRAPER_BACKOFF_BASE_SECONDS,
        backoff_max_seconds: float = SCRAPER_BACKOFF_MAX_SECONDS,
        proxies: Optional[Sequence[str]] = None,
    ) -> None:
        self._timeout_seconds = max(2.0, timeout_seconds)
        self._max_retries = max(0, max_retries)
        self._backoff_base_seconds = max(0.5, backoff_base_seconds)
        self._backoff_max_seconds = max(self._backoff_base_seconds, backoff_max_seconds)
        self._rate_limiter = DomainRateLimiter(min_delay_seconds, max_delay_seconds)
        self._session: Optional[aiohttp.ClientSession] = None

        configured_proxies = list(proxies) if proxies is not None else list(SCRAPER_PROXIES)
        self._proxies = [proxy.strip() for proxy in configured_proxies if proxy and proxy.strip()]
        self._proxy_index = 0

    async def __aenter__(self) -> "ResilientScraperClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _ensure_session(self) -> None:
        if self._session and not self._session.closed:
            return

        timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
        self._session = aiohttp.ClientSession(timeout=timeout, trust_env=True)

    def _build_headers(
        self,
        referer: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "User-Agent": random.choice(DEFAULT_USER_AGENTS),
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        if referer:
            headers["Referer"] = referer
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _pick_proxy(self) -> Optional[str]:
        if not self._proxies:
            return None

        proxy = self._proxies[self._proxy_index % len(self._proxies)]
        self._proxy_index += 1
        return proxy

    def _compute_backoff(self, attempt: int) -> float:
        exp = self._backoff_base_seconds * (2 ** attempt)
        jitter = random.uniform(0.15, 0.9)
        return min(self._backoff_max_seconds, exp + jitter)

    async def request_text(
        self,
        method: str,
        url: str,
        *,
        referer: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        expected_status: Optional[Set[int]] = None,
        max_retries: Optional[int] = None,
    ) -> Optional[str]:
        """Ejecuta una request y devuelve texto si obtiene status esperado."""
        await self._ensure_session()
        if not self._session:
            return None

        status_ok = expected_status or {200}
        retries = self._max_retries if max_retries is None else max(0, max_retries)
        domain = urlparse(url).netloc

        for attempt in range(retries + 1):
            await self._rate_limiter.wait_turn(domain)

            proxy = self._pick_proxy()
            request_headers = self._build_headers(referer=referer, extra_headers=headers)

            try:
                async with self._session.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    proxy=proxy,
                    allow_redirects=True,
                ) as response:
                    text = await response.text()

                    if response.status in status_ok:
                        return text

                    if response.status in RETRYABLE_STATUS_CODES and attempt < retries:
                        wait_seconds = self._compute_backoff(attempt)
                        logging.warning(
                            "Status %s en %s. Reintento %s/%s en %.2fs",
                            response.status,
                            domain,
                            attempt + 1,
                            retries,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    logging.warning(
                        "Request sin exito para %s. Status=%s, reintentos agotados=%s",
                        url,
                        response.status,
                        attempt >= retries,
                    )
                    return None

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= retries:
                    logging.error("Error HTTP en %s sin reintentos restantes: %s", url, str(exc))
                    return None

                wait_seconds = self._compute_backoff(attempt)
                logging.warning(
                    "Error HTTP en %s: %s. Reintento %s/%s en %.2fs",
                    domain,
                    str(exc),
                    attempt + 1,
                    retries,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)

        return None

    async def get_text(
        self,
        url: str,
        *,
        referer: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        expected_status: Optional[Set[int]] = None,
        max_retries: Optional[int] = None,
    ) -> Optional[str]:
        return await self.request_text(
            method="GET",
            url=url,
            referer=referer,
            headers=headers,
            expected_status=expected_status,
            max_retries=max_retries,
        )

    async def get_json(
        self,
        url: str,
        *,
        referer: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        expected_status: Optional[Set[int]] = None,
        max_retries: Optional[int] = None,
    ) -> Optional[object]:
        text = await self.get_text(
            url=url,
            referer=referer,
            headers=headers,
            expected_status=expected_status,
            max_retries=max_retries,
        )
        if not text:
            return None

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            logging.warning("No se pudo parsear JSON de %s", url)
            return None
