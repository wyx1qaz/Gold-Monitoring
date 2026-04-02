from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Iterable
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import httpx


@dataclass
class RssEntry:
    feed_url: str
    source: str
    title: str
    link: str
    summary: str
    published_at: datetime | None
    content_hash: str


@dataclass
class RssFetchResult:
    entries: list[RssEntry]
    errors: list[str]


class RssFeedClient:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds

    async def fetch_many(self, feed_urls: Iterable[str]) -> RssFetchResult:
        urls = [url.strip() for url in feed_urls if url and url.strip()]
        if not urls:
            return RssFetchResult(entries=[], errors=[])

        results = await asyncio.gather(
            *(self._fetch_one(url) for url in urls),
            return_exceptions=True,
        )

        entries: list[RssEntry] = []
        errors: list[str] = []
        for url, result in zip(urls, results, strict=False):
            if isinstance(result, Exception):
                errors.append(f"{url}: {result}")
                continue
            entries.extend(result)
        return RssFetchResult(entries=entries, errors=errors)

    async def _fetch_one(self, url: str) -> list[RssEntry]:
        async with httpx.AsyncClient(
            timeout=self.timeout_seconds,
            follow_redirects=True,
            verify=not self._should_disable_tls_verification(url),
        ) as client:
            response = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
        return self._parse_feed(url, response.text)

    def _parse_feed(self, feed_url: str, xml_text: str) -> list[RssEntry]:
        root = ET.fromstring(xml_text)
        channel_title = self._get_channel_title(root) or feed_url
        items = root.findall(".//item")
        if items:
            return [
                self._parse_rss_item(feed_url, channel_title, item)
                for item in items
                if self._safe_get_text(item, "title")
            ]

        atom_entries = root.findall(".//{http://www.w3.org/2005/Atom}entry")
        return [
            self._parse_atom_entry(feed_url, channel_title, entry)
            for entry in atom_entries
            if self._safe_get_namespaced_text(entry, "{http://www.w3.org/2005/Atom}title")
        ]

    @staticmethod
    def _get_channel_title(root: ET.Element) -> str | None:
        title = root.findtext("./channel/title")
        if title:
            return title.strip()
        atom_title = root.findtext(".//{http://www.w3.org/2005/Atom}title")
        return atom_title.strip() if atom_title else None

    def _parse_rss_item(self, feed_url: str, source: str, item: ET.Element) -> RssEntry:
        title = self._safe_get_text(item, "title")
        link = self._safe_get_text(item, "link")
        summary = (
            self._safe_get_text(item, "summary")
            or self._safe_get_text(item, "description")
            or self._safe_get_text(item, "summary")
            or ""
        )
        published_text = (
            self._safe_get_text(item, "pubDate")
            or self._safe_get_text(item, "published")
            or self._safe_get_text(item, "updated")
        )
        return RssEntry(
            feed_url=feed_url,
            source=source,
            title=title,
            link=link,
            summary=self._clean_summary(summary),
            published_at=self._parse_datetime(published_text),
            content_hash=self._build_hash(feed_url, title, link),
        )

    def _parse_atom_entry(self, feed_url: str, source: str, entry: ET.Element) -> RssEntry:
        title = self._safe_get_namespaced_text(entry, "{http://www.w3.org/2005/Atom}title")
        link = ""
        for link_el in entry.findall("{http://www.w3.org/2005/Atom}link"):
            href = (link_el.attrib.get("href") or "").strip()
            if href:
                link = href
                break
        summary = (
            self._safe_get_namespaced_text(entry, "{http://www.w3.org/2005/Atom}summary")
            or self._safe_get_namespaced_text(entry, "{http://www.w3.org/2005/Atom}content")
            or ""
        )
        published_text = (
            self._safe_get_namespaced_text(entry, "{http://www.w3.org/2005/Atom}published")
            or self._safe_get_namespaced_text(entry, "{http://www.w3.org/2005/Atom}updated")
        )
        return RssEntry(
            feed_url=feed_url,
            source=source,
            title=title,
            link=link,
            summary=self._clean_summary(summary),
            published_at=self._parse_datetime(published_text),
            content_hash=self._build_hash(feed_url, title, link),
        )

    @staticmethod
    def _safe_get_text(element: ET.Element, tag: str) -> str:
        child = element.find(tag)
        if child is None or child.text is None:
            return ""
        return child.text.strip()

    @staticmethod
    def _safe_get_namespaced_text(element: ET.Element, tag: str) -> str:
        child = element.find(tag)
        if child is None or child.text is None:
            return ""
        return child.text.strip()

    @staticmethod
    def _parse_datetime(raw_value: str) -> datetime | None:
        if not raw_value:
            return None
        raw_value = raw_value.strip()
        try:
            if raw_value.endswith("Z"):
                return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
            parsed = datetime.fromisoformat(raw_value)
        except ValueError:
            try:
                parsed = parsedate_to_datetime(raw_value)
            except (TypeError, ValueError):
                return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _build_hash(feed_url: str, title: str, link: str) -> str:
        return hashlib.sha256(f"{feed_url}|{title}|{link}".encode("utf-8")).hexdigest()

    @staticmethod
    def _clean_summary(text: str, max_length: int = 600) -> str:
        normalized = unescape(text or "")
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized[:max_length].strip()

    @staticmethod
    def _should_disable_tls_verification(url: str) -> bool:
        host = (urlparse(url).hostname or "").lower()
        return host.endswith("quanwenrss.com")
