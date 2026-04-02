from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .config import SHANGHAI_TZ, TROY_OUNCE_TO_GRAMS
from .db import Database
from .market_hours import is_comex_gold_open, is_london_gold_open, is_shfe_gold_open
from .sina_client import QuoteSnapshot


CN_TZ = ZoneInfo(SHANGHAI_TZ)


@dataclass
class InternationalGoldPrice:
    price_usd_per_oz: float
    price_cny_per_g: float
    source: str


@dataclass
class DomesticGoldPrice:
    price_cny_per_g: float
    source: str
    is_proxy: bool = False


def resolve_international_gold_price(
    snapshot: QuoteSnapshot,
    fetched_at: datetime,
) -> InternationalGoldPrice | None:
    if is_london_gold_open(fetched_at) and snapshot.london_price_usd_per_oz > 0:
        london_cny = snapshot.london_price_usd_per_oz * snapshot.usdcny_rate / TROY_OUNCE_TO_GRAMS
        return InternationalGoldPrice(
            price_usd_per_oz=snapshot.london_price_usd_per_oz,
            price_cny_per_g=london_cny,
            source="新浪 hf_XAU 伦敦金现货",
        )

    if is_comex_gold_open(fetched_at) and snapshot.comex_price_usd_per_oz and snapshot.comex_price_usd_per_oz > 0:
        comex_cny = snapshot.comex_price_usd_per_oz * snapshot.usdcny_rate / TROY_OUNCE_TO_GRAMS
        return InternationalGoldPrice(
            price_usd_per_oz=snapshot.comex_price_usd_per_oz,
            price_cny_per_g=comex_cny,
            source="新浪 hf_GC 纽约金主力",
        )

    return None


def resolve_domestic_gold_price(
    db: Database,
    snapshot: QuoteSnapshot,
    fetched_at: datetime,
    international_price_cny_per_g: float,
) -> DomesticGoldPrice | None:
    if is_shfe_gold_open(fetched_at):
        for price, source in (
            (snapshot.sge_au9999_price_cny_per_g, "新浪 gds_AU9999 沪金99"),
            (snapshot.sge_autd_price_cny_per_g, "新浪 gds_AUTD 黄金延期"),
            (snapshot.shfe_price_cny_per_g, "新浪 nf_AU0 黄金连续"),
        ):
            if price and price > 0:
                return DomesticGoldPrice(price_cny_per_g=price, source=source, is_proxy=False)

    latest_effective = db.get_latest_effective_premium_sample()
    if not latest_effective:
        return None

    last_domestic = latest_effective.get("shfe_price_cny_per_g")
    last_international = latest_effective.get("london_price_cny_per_g")
    if last_domestic is None or last_international in (None, 0):
        return None

    ratio = float(last_domestic) / float(last_international)
    proxy_price = international_price_cny_per_g * ratio
    reference_time = datetime.fromisoformat(str(latest_effective["fetched_at"])).astimezone(CN_TZ)
    source = f"代理人民币金价(基于最近有效样本比例，参考 {reference_time.strftime('%m-%d %H:%M')})"
    return DomesticGoldPrice(price_cny_per_g=proxy_price, source=source, is_proxy=True)


def build_price_source_note(
    domestic: DomesticGoldPrice,
    international: InternationalGoldPrice,
    extra_note: str = "",
) -> str:
    parts = [
        f"国内源: {domestic.source}",
        f"国际源: {international.source}",
    ]
    if extra_note:
        parts.append(extra_note)
    return "；".join(parts)
