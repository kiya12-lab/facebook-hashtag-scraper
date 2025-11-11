thonimport itertools
import logging
import os
from typing import Dict, Iterable, List, Optional, Union

logger = logging.getLogger(__name__)

ProxyType = Union[str, Dict[str, str]]

class ProxyManager:
    """
    Simple round-robin proxy manager.

    Accepts either:
    - A list of proxy URLs (strings), or
    - A list of dicts suitable for `requests` proxies, e.g.:
      {"http": "http://user:pass@host:port", "https": "http://user:pass@host:port"}
    """

    def __init__(self, proxy_list: Optional[Iterable[ProxyType]] = None) -> None:
        if proxy_list is None:
            proxy_list = self._load_from_env()

        self._proxies: List[ProxyType] = list(proxy_list)
        self._cycle = itertools.cycle(self._proxies) if self._proxies else None

        if self._proxies:
            logger.info("ProxyManager initialized with %d proxies.", len(self._proxies))
        else:
            logger.info("ProxyManager initialized with no proxies (direct connection).")

    def _load_from_env(self) -> List[ProxyType]:
        """
        Load proxies from environment variable `SCRAPER_PROXIES`, where
        each proxy is separated by commas.
        Example:
            SCRAPER_PROXIES="http://user:pass@host1:port,http://host2:port"
        """
        env_val = os.getenv("SCRAPER_PROXIES", "").strip()
        if not env_val:
            return []

        proxies: List[ProxyType] = []
        for token in env_val.split(","):
            token = token.strip()
            if not token:
                continue
            proxies.append(
                {
                    "http": token,
                    "https": token,
                }
            )
        return proxies

    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        """
        Returns the next proxy configuration for `requests`, or None if
        no proxies are configured.
        """
        if not self._cycle:
            return None

        proxy = next(self._cycle)
        if isinstance(proxy, str):
            return {"http": proxy, "https": proxy}
        return proxy