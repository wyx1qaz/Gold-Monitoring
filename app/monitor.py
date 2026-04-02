from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .config import SHANGHAI_TZ, TROY_OUNCE_TO_GRAMS
from .db import Database
from .dingtalk import post_text_to_targets_detailed, resolve_notification_targets
from .market_hours import is_comex_gold_open, is_london_gold_open, is_shfe_gold_open
from .price_source import build_price_source_note, resolve_domestic_gold_price, resolve_international_gold_price
from .sina_client import QuoteSnapshot, SinaQuoteClient


CN_TZ = ZoneInfo(SHANGHAI_TZ)


@dataclass
class MonitorCycleResult:
    sample_id: int | None
    premium_cny_per_g: float | None
    alert_triggered: bool
    note: str
    fetched_at: str


class MonitorService:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def run_cycle(self) -> MonitorCycleResult:
        settings = self.db.get_settings()
        fetched_at = datetime.now(CN_TZ)
        if not self._has_any_monitor_window(fetched_at):
            return MonitorCycleResult(
                sample_id=None,
                premium_cny_per_g=None,
                alert_triggered=False,
                note="国内金与国际金均休市，已暂停 SGE 监控",
                fetched_at=fetched_at.isoformat(),
            )
        started = time.perf_counter()
        try:
            client = SinaQuoteClient(timeout_seconds=settings["request_timeout_seconds"])
            snapshot = await client.fetch()
            sample_id, premium, note, price_source_text = self._store_snapshot(
                fetched_at=fetched_at,
                snapshot=snapshot,
                poll_interval_seconds=settings["poll_interval_seconds"],
            )
            alert_triggered = False
            if sample_id and premium is not None:
                sample = self.db.get_latest_sample()
                alert_triggered = await self._maybe_send_alert(
                    sample_id=sample_id,
                    premium_cny_per_g=premium,
                    fetched_at=fetched_at,
                    settings=settings,
                    sample=sample,
                    price_source_text=price_source_text,
                )
            self.db.insert_fetch_run(
                fetched_at=fetched_at,
                success=True,
                poll_interval_seconds=settings["poll_interval_seconds"],
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            return MonitorCycleResult(
                sample_id=sample_id,
                premium_cny_per_g=premium,
                alert_triggered=alert_triggered,
                note=note,
                fetched_at=fetched_at.isoformat(),
            )
        except Exception as exc:
            self.db.insert_fetch_run(
                fetched_at=fetched_at,
                success=False,
                poll_interval_seconds=settings["poll_interval_seconds"],
                duration_ms=int((time.perf_counter() - started) * 1000),
                error_message=str(exc),
            )
            raise

    def _store_snapshot(
        self,
        *,
        fetched_at: datetime,
        snapshot: QuoteSnapshot,
        poll_interval_seconds: int,
    ) -> tuple[int | None, float | None, str, str]:
        international_price = resolve_international_gold_price(snapshot, fetched_at)
        if not international_price:
            return None, None, "无可用国际金价源，已暂停 SGE 监控", "国际源: 无"

        domestic_price = resolve_domestic_gold_price(
            self.db,
            snapshot,
            fetched_at,
            international_price.price_cny_per_g,
        )
        if not domestic_price:
            return None, None, "国内金休市且缺少代理基准，已暂停 SGE 监控", f"国际源: {international_price.source}"

        london_cny_per_g = round(international_price.price_cny_per_g, 4)
        premium = round(domestic_price.price_cny_per_g - london_cny_per_g, 4)
        note = build_price_source_note(domestic_price, international_price)
        payload = {
            "fetched_at": fetched_at.isoformat(),
            "shfe_price_cny_per_g": round(domestic_price.price_cny_per_g, 4),
            "london_price_usd_per_oz": round(international_price.price_usd_per_oz, 4),
            "usdcny_rate": round(snapshot.usdcny_rate, 6),
            "london_price_cny_per_g": london_cny_per_g,
            "premium_cny_per_g": premium,
            "poll_interval_seconds": poll_interval_seconds,
            "both_markets_open": int(is_shfe_gold_open(fetched_at) and is_london_gold_open(fetched_at)),
            "shfe_market_open": int(is_shfe_gold_open(fetched_at)),
            "london_market_open": int(bool(international_price.source == "新浪 hf_XAU 伦敦金现货")),
            "alert_triggered": 0,
            "raw_payload": snapshot.raw_text,
            "note": note,
        }
        sample_id = self.db.insert_sample(payload)
        return sample_id, premium, note, note

    async def _maybe_send_alert(
        self,
        *,
        sample_id: int,
        premium_cny_per_g: float,
        fetched_at: datetime,
        settings: dict[str, Any],
        sample: dict[str, Any] | None,
        price_source_text: str,
    ) -> bool:
        threshold = float(settings["premium_threshold"])
        if premium_cny_per_g < threshold or not resolve_notification_targets(settings):
            return False

        last_success = self.db.get_last_successful_alert()
        if last_success:
            last_sent = datetime.fromisoformat(last_success["sent_at"])
            cooldown = timedelta(seconds=int(settings["alert_cooldown_seconds"]))
            if fetched_at - last_sent < cooldown:
                return False

        if not sample:
            return False
        text = (
            "SGE 溢价预警\n"
            f"时间: {fetched_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"人民币金价: {sample['shfe_price_cny_per_g']:.4f} 元/克\n"
            f"国际金折算: {sample['london_price_cny_per_g']:.4f} 元/克\n"
            f"SGE 溢价金额: {premium_cny_per_g:.4f} 元/克\n"
            f"阈值: {threshold:.4f} 元/克\n"
            f"价格来源: {price_source_text}\n"
            "监控台: http://127.0.0.1:8001/"
        )
        success, response_text, details = await post_text_to_targets_detailed(settings, text, timeout_seconds=10)
        self.db.insert_alert_event(
            {
                "sample_id": sample_id,
                "sent_at": fetched_at.isoformat(),
                "premium_cny_per_g": premium_cny_per_g,
                "threshold_cny_per_g": threshold,
                "success": int(success),
                "response_text": response_text,
                "webhook_url": settings.get("dingtalk_webhook", ""),
            }
        )
        for detail in details:
            self.db.insert_notification_log(
                sent_at=fetched_at,
                channel="dingtalk",
                event_type="sge_premium_alert",
                target_name=detail["target_name"],
                webhook_url=detail["webhook_url"],
                success=detail["success"] == "1",
                content=text,
                response_text=detail["response_text"],
            )
        if success:
            self.db.set_sample_alert_triggered(sample_id)
        return success

    @staticmethod
    def _has_any_monitor_window(fetched_at: datetime) -> bool:
        return is_shfe_gold_open(fetched_at) or is_london_gold_open(fetched_at) or is_comex_gold_open(fetched_at)
