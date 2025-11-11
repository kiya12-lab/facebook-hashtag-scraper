thonimport logging
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from .content_cleaner import (
    clean_content,
    compute_total_engagement,
    safe_int,
)

logger = logging.getLogger(__name__)

@dataclass
class FacebookPost:
    permalink: str
    content: str
    media_type: str
    like_count: int
    comment_count: int
    share_count: int
    total_engagement: int
    video_views_count: int
    date: str

class FacebookHashtagScraper:
    """
    A lightweight, HTML-based scraper for public Facebook hashtag pages.

    This implementation is intentionally conservative:
    - It uses standard HTTP requests with optional proxies & rotating user agents.
    - It parses HTML using heuristics that may need tuning for Facebook's real DOM.
    - It focuses on stability and error handling rather than completeness.
    """

    def __init__(
        self,
        hashtag: str,
        max_pages: int,
        settings: Dict[str, Any],
        proxy_manager: Any,
        user_agent_rotator: Any,
    ) -> None:
        self.hashtag = hashtag.lstrip("#")
        self.max_pages = max_pages
        self.settings = settings or {}
        self.proxy_manager = proxy_manager
        self.user_agent_rotator = user_agent_rotator

        self.base_url: str = self.settings.get(
            "base_url", "https://www.facebook.com/hashtag/"
        )
        self.timeout: int = int(self.settings.get("request_timeout", 15))
        self.sleep_between_requests: float = float(
            self.settings.get("sleep_between_requests", 1.5)
        )
        self.session = requests.Session()

    def run(self) -> List[Dict[str, Any]]:
        all_posts: List[Dict[str, Any]] = []

        for page in range(1, self.max_pages + 1):
            url = self._build_page_url(page)
            logger.info("Fetching page %s: %s", page, url)

            html = self._fetch_page(url)
            if not html:
                logger.warning("No HTML returned for page %s; stopping.", page)
                break

            page_posts = self._parse_page(html)
            logger.info("Parsed %d posts from page %s", len(page_posts), page)

            if not page_posts:
                # No more posts found, stop early
                break

            all_posts.extend(asdict(post) for post in page_posts)

            time.sleep(self.sleep_between_requests)

        return all_posts

    def _build_page_url(self, page: int) -> str:
        encoded = quote_plus(self.hashtag)
        # This is a generic, non-authenticated hashtag URL; real-world scraping
        # may require different parameters or mobile endpoints.
        if page <= 1:
            return f"{self.base_url}{encoded}"
        # Simulate pagination with a page query.
        return f"{self.base_url}{encoded}?page={page}"

    def _fetch_page(self, url: str) -> Optional[str]:
        headers = {
            "User-Agent": self.user_agent_rotator.get_user_agent(),
            "Accept-Language": "en-US,en;q=0.9",
        }

        proxies = self.proxy_manager.get_next_proxy()
        request_kwargs: Dict[str, Any] = {
            "headers": headers,
            "timeout": self.timeout,
        }

        if proxies:
            request_kwargs["proxies"] = proxies

        max_retries = int(self.settings.get("max_retries", 3))
        backoff_factor = float(self.settings.get("backoff_factor", 1.5))

        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, **request_kwargs)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(
                    "Non-200 status code (%s) on attempt %s for %s",
                    resp.status_code,
                    attempt,
                    url,
                )
            except requests.RequestException as exc:
                logger.warning(
                    "Request error on attempt %s for %s: %s", attempt, url, exc
                )

            if attempt < max_retries:
                sleep_time = backoff_factor * attempt
                logger.debug("Sleeping %.2fs before retry...", sleep_time)
                time.sleep(sleep_time)

        logger.error("All retries failed for URL: %s", url)
        return None

    def _parse_page(self, html: str) -> List[FacebookPost]:
        """
        Parse a Facebook hashtag HTML page into structured post records.

        NOTE: Facebook markup changes frequently. This parser is written to be:
        - Defensive against missing elements.
        - Easy to adapt to different selectors.

        It uses a combination of `article` tags and common data attributes
        to guess posts. For unit tests or offline usage, you can feed
        synthetic HTML that matches these expectations.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Heuristic: posts are often represented as <article> or <div role="article">
        candidates = soup.find_all(["article", "div"], attrs={"role": "article"})
        if not candidates:
            # Fallback: look for generic post containers with 'data-ft'
            candidates = soup.find_all("div", attrs={"data-ft": True})

        posts: List[FacebookPost] = []
        for node in candidates:
            try:
                post = self._parse_single_post(node)
                if post:
                    posts.append(post)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Error parsing post node: %s", exc, exc_info=True)

        return posts

    def _parse_single_post(self, node: Any) -> Optional[FacebookPost]:
        # Permalink: try to find an <a> that looks like a post URL.
        permalink = ""
        link = node.find("a", href=True)
        if link and "facebook.com" in link["href"]:
            permalink = link["href"]
        elif link:
            permalink = "https://www.facebook.com" + link["href"]

        # Content: look for text blocks with common selectors.
        content_blocks: List[str] = []
        for cls in ["userContent", "ecm0bbzt"]:
            for div in node.find_all("div", class_=cls):
                text = div.get_text(" ", strip=True)
                if text:
                    content_blocks.append(text)

        if not content_blocks:
            # Generic fallback: take the first sizable text chunk from the node.
            text = node.get_text(" ", strip=True)
            if text:
                content_blocks.append(text)

        raw_content = " ".join(content_blocks)
        content = clean_content(raw_content)

        if not content and not permalink:
            # Very weak candidate; skip.
            return None

        media_type = self._detect_media_type(node)

        like_count = safe_int(self._extract_stat(node, ["like", "reaction"]))
        comment_count = safe_int(self._extract_stat(node, ["comment"]))
        share_count = safe_int(self._extract_stat(node, ["share"]))
        video_views_count = safe_int(self._extract_stat(node, ["view"]))

        total_engagement = compute_total_engagement(
            like_count, comment_count, share_count, video_views_count
        )

        date_str = self._extract_date(node)

        return FacebookPost(
            permalink=permalink,
            content=content,
            media_type=media_type,
            like_count=like_count,
            comment_count=comment_count,
            share_count=share_count,
            total_engagement=total_engagement,
            video_views_count=video_views_count,
            date=date_str,
        )

    def _detect_media_type(self, node: Any) -> str:
        # Simple heuristics to guess media type
        if node.find("video"):
            return "video"
        if node.find("img"):
            return "photo"
        return "text"

    def _extract_stat(self, node: Any, keywords: List[str]) -> int:
        """
        Attempt to extract a numeric stat (likes, comments, shares, views) by
        scanning text near matching keywords.
        """
        text = node.get_text(" ", strip=True).lower()
        candidate_value = 0

        for keyword in keywords:
            idx = text.find(keyword)
            if idx == -1:
                continue

            # Look a little bit around the keyword for numbers
            window = text[max(0, idx - 10) : idx + 20]
            tokens = window.split()
            for tok in tokens:
                val = safe_int(tok)
                if val > candidate_value:
                    candidate_value = val

        return candidate_value

    def _extract_date(self, node: Any) -> str:
        # Try time element first
        time_tag = node.find("abbr")
        if time_tag and time_tag.has_attr("data-utime"):
            # Many FB timestamps are unix seconds
            try:
                ts = int(time_tag["data-utime"])
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            except (ValueError, TypeError):
                pass

        # Fallback: plain text from <time> or <abbr>
        for tag_name in ["time", "abbr"]:
            t = node.find(tag_name)
            if t and t.get("title"):
                return t["title"]
            if t and t.get_text(strip=True):
                return t.get_text(strip=True)

        # If nothing found, return empty string; caller can post-process.
        return ""