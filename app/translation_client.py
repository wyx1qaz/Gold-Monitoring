from __future__ import annotations

import re
from html import unescape

import httpx


LATIN_PATTERN = re.compile(r"[A-Za-z]{3,}")
CJK_PATTERN = re.compile(r"[\u4e00-\u9fff]")


class TranslationClient:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds

    async def to_chinese(self, text: str, *, max_length: int = 500) -> str:
        cleaned = self._clean_text(text, max_length=max_length)
        if not cleaned:
            return ""
        if not self._needs_translation(cleaned):
            return cleaned

        params = {
            "client": "gtx",
            "sl": "auto",
            "tl": "zh-CN",
            "dt": "t",
            "q": cleaned,
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                response = await client.get(
                    "https://translate.googleapis.com/translate_a/single",
                    params=params,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                response.raise_for_status()
            payload = response.json()
            translated = "".join(part[0] for part in payload[0] if part and part[0])
            return translated.strip() or cleaned
        except Exception:
            return cleaned

    @staticmethod
    def _needs_translation(text: str) -> bool:
        return bool(LATIN_PATTERN.search(text)) and not bool(CJK_PATTERN.search(text))

    @staticmethod
    def _clean_text(text: str, *, max_length: int) -> str:
        normalized = unescape(text or "").strip()
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized[:max_length].strip()
