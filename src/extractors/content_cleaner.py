thonimport logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

WHITESPACE_RE = re.compile(r"\s+")
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]")

def clean_content(text: str) -> str:
    """
    Normalize post text:
    - Remove control characters.
    - Collapse whitespace.
    - Strip leading/trailing spaces.
    """
    if text is None:
        return ""

    original = text
    text = CONTROL_CHARS_RE.sub("", text)
    text = WHITESPACE_RE.sub(" ", text).strip()

    # Limit extreme lengths for safety
    max_len = 10_000
    if len(text) > max_len:
        logger.debug(
            "Truncating overly long content from %d to %d chars", len(text), max_len
        )
        text = text[:max_len]

    if logger.isEnabledFor(logging.DEBUG) and original != text:
        logger.debug("Cleaned content: '%s' -> '%s'", original[:80], text[:80])

    return text

def safe_int(value: Optional[str]) -> int:
    """
    Parse an integer from various formats like '1.2K', '3,456', '789'.
    Non-parsable values result in 0.
    """
    if value is None:
        return 0

    if isinstance(value, int):
        return value

    s = str(value).strip().lower()
    if not s:
        return 0

    # Handle shorthand like "1.2k", "3m"
    multiplier = 1
    if s.endswith("k"):
        multiplier = 1_000
        s = s[:-1]
    elif s.endswith("m"):
        multiplier = 1_000_000
        s = s[:-1]

    # Remove commas and other non-digit, non-dot chars
    cleaned = re.sub(r"[^0-9.]", "", s)
    if not cleaned:
        return 0

    try:
        if "." in cleaned:
            num = float(cleaned)
        else:
            num = int(cleaned)
        return int(num * multiplier)
    except ValueError:
        return 0

def compute_total_engagement(
    like_count: int, comment_count: int, share_count: int, video_views_count: int
) -> int:
    like_count = like_count or 0
    comment_count = comment_count or 0
    share_count = share_count or 0
    video_views_count = video_views_count or 0

    total = like_count + comment_count + share_count + video_views_count
    if total < 0:
        logger.debug(
            "Computed negative engagement (%d); coercing to zero.", total
        )
        total = 0
    return total