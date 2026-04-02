from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

import httpx

from .config import SINA_QUOTE_URL


QUOTE_PATTERN = re.compile(r'var hq_str_(?P<symbol>[^=]+)="(?P<body>.*?)";')


@dataclass
class QuoteSnapshot:
    shfe_price_cny_per_g: float
    shfe_timestamp: datetime | None
    sge_au9999_price_cny_per_g: float | None
    sge_au9999_timestamp: datetime | None
    sge_autd_price_cny_per_g: float | None
    sge_autd_timestamp: datetime | None
    london_price_usd_per_oz: float
    london_timestamp: datetime | None
    comex_price_usd_per_oz: float | None
    comex_timestamp: datetime | None
    usdcny_rate: float
    fx_timestamp: datetime | None
    raw_text: str


class SinaQuoteClient:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds

    async def fetch(self) -> QuoteSnapshot:
        headers = {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            response = await client.get(SINA_QUOTE_URL, headers=headers)
            response.raise_for_status()
        payload = self._parse_response(response.text)
        shfe_fields = payload["nf_AU0"].split(",")
        au9999_fields = payload["gds_AU9999"].split(",") if payload.get("gds_AU9999") else []
        autd_fields = payload["gds_AUTD"].split(",") if payload.get("gds_AUTD") else []
        xau_fields = payload["hf_XAU"].split(",")
        gc_fields = payload["hf_GC"].split(",") if payload.get("hf_GC") else []
        fx_fields = payload["USDCNY"].split(",")
        return QuoteSnapshot(
            shfe_price_cny_per_g=float(shfe_fields[8]),
            shfe_timestamp=self._parse_shfe_time(shfe_fields),
            sge_au9999_price_cny_per_g=self._parse_optional_float(au9999_fields, 0),
            sge_au9999_timestamp=self._parse_gds_time(au9999_fields),
            sge_autd_price_cny_per_g=self._parse_optional_float(autd_fields, 0),
            sge_autd_timestamp=self._parse_gds_time(autd_fields),
            london_price_usd_per_oz=float(xau_fields[0]),
            london_timestamp=self._parse_standard_date_time(xau_fields[12], xau_fields[6]),
            comex_price_usd_per_oz=self._parse_optional_float(gc_fields, 0),
            comex_timestamp=self._parse_standard_date_time(gc_fields[12], gc_fields[6]) if len(gc_fields) > 12 else None,
            usdcny_rate=float(fx_fields[1]),
            fx_timestamp=self._parse_standard_date_time(fx_fields[10], fx_fields[0]),
            raw_text=response.text.strip(),
        )

    def _parse_response(self, text: str) -> dict[str, str]:
        parsed: dict[str, str] = {}
        for match in QUOTE_PATTERN.finditer(text):
            parsed[match.group("symbol")] = match.group("body")
        expected = {"nf_AU0", "gds_AU9999", "gds_AUTD", "hf_XAU", "hf_GC", "USDCNY"}
        missing = expected.difference(parsed)
        if missing:
            raise ValueError(f"Sina response missing symbols: {', '.join(sorted(missing))}")
        return parsed

    @staticmethod
    def _parse_optional_float(fields: list[str], index: int) -> float | None:
        if len(fields) <= index:
            return None
        raw_value = fields[index].strip()
        if not raw_value:
            return None
        try:
            return float(raw_value)
        except ValueError:
            return None

    @staticmethod
    def _parse_shfe_time(fields: list[str]) -> datetime | None:
        if len(fields) < 18:
            return None
        raw_time = fields[1]
        raw_date = fields[17]
        if len(raw_time) != 6 or not raw_time.isdigit():
            return None
        formatted_time = f"{raw_time[:2]}:{raw_time[2:4]}:{raw_time[4:6]}"
        return SinaQuoteClient._parse_standard_date_time(raw_date, formatted_time)

    @staticmethod
    def _parse_gds_time(fields: list[str]) -> datetime | None:
        if len(fields) < 13:
            return None
        raw_time = fields[6].strip()
        raw_date = fields[12].strip()
        if not raw_time or not raw_date:
            return None
        return SinaQuoteClient._parse_standard_date_time(raw_date, raw_time)

    @staticmethod
    def _parse_standard_date_time(raw_date: str, raw_time: str) -> datetime | None:
        try:
            return datetime.fromisoformat(f"{raw_date}T{raw_time}")
        except ValueError:
            return None
