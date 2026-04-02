from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from .config import SHANGHAI_TZ
from .db import Database
from .dingtalk import post_text_to_targets_detailed, resolve_notification_targets
from .gold_event_scoring import score_gold_risk_event
from .market_hours import is_comex_gold_open, is_london_gold_open, is_shfe_gold_open
from .price_source import build_price_source_note, resolve_domestic_gold_price, resolve_international_gold_price
from .rss_client import RssEntry, RssFeedClient
from .rss_ml import RssMlService
from .sina_client import QuoteSnapshot, SinaQuoteClient
from .translation_client import TranslationClient


CN_TZ = ZoneInfo(SHANGHAI_TZ)
HTML_TAG_RE = re.compile(r"<[^>]+>")
SEMANTIC_SPLIT_RE = re.compile(r"[^a-z0-9\u4e00-\u9fff]+")
SEMANTIC_STOPWORDS = {
    "的",
    "了",
    "和",
    "与",
    "及",
    "在",
    "是",
    "将",
    "称",
    "表示",
    "今日",
    "最新",
    "news",
    "update",
    "breaking",
    "report",
}

POLITICAL_POSITIVE_KEYWORDS = (
    "ceasefire",
    "truce",
    "talks",
    "negotiation",
    "diplomacy",
    "mediat",
    "framework",
    "停火",
    "谈判",
    "会谈",
    "外交",
    "斡旋",
    "和谈",
)

CONTEXT_KEYWORDS = (
    "middle east",
    "iran",
    "israel",
    "gaza",
    "hormuz",
    "red sea",
    "gulf",
    "oil",
    "lng",
    "gold",
    "bullion",
    "shipping",
    "tanker",
    "ceasefire",
    "停火",
    "伊朗",
    "以色列",
    "霍尔木兹",
    "中东",
    "红海",
    "航运",
    "油轮",
    "原油",
    "黄金",
)

WAR_PROGRESS_KEYWORDS = (
    "reopen",
    "resum",
    "restore",
    "shipping lane",
    "shipping resume",
    "corridor",
    "convoy",
    "escort",
    "port reopen",
    "terminal reopen",
    "clearance",
    "复航",
    "恢复通行",
    "恢复出口",
    "恢复装船",
    "重启装船",
    "清障",
    "护航",
)

NEGATIVE_FILTER_KEYWORDS = (
    "retaliat",
    "missile",
    "drone",
    "attack",
    "airstrike",
    "escalat",
    "closure",
    "closed",
    "shutdown",
    "袭击",
    "导弹",
    "升级",
    "报复",
    "封锁",
    "关闭",
)


@dataclass
class GoldReversalCycleResult:
    sample_id: int | None
    signal_level: int
    triggered_conditions: list[str]
    alert_triggered: bool
    note: str
    fetched_at: str
    matched_events: int
    rss_errors: list[str]


@dataclass
class PriceSignalEvaluation:
    triggered: bool
    note: str
    rebound_pct: float
    recent_low: float | None
    short_ma: float | None


class GoldReversalMonitorService:
    def __init__(self, db: Database, ml_service: RssMlService | None = None) -> None:
        self.db = db
        self.translator = TranslationClient()
        self.ml_service = ml_service

    async def run_cycle(self) -> GoldReversalCycleResult:
        settings = self.db.get_settings()
        fetched_at = datetime.now(CN_TZ)
        if not self._has_any_monitor_window(fetched_at):
            return GoldReversalCycleResult(
                sample_id=None,
                signal_level=0,
                triggered_conditions=[],
                alert_triggered=False,
                note="国内金与国际金均休市，已暂停黄金盘面监控（RSS定时抓取独立运行）",
                fetched_at=fetched_at.isoformat(),
                matched_events=0,
                rss_errors=[],
            )

        started = time.perf_counter()
        try:
            quote_client = SinaQuoteClient(timeout_seconds=settings["request_timeout_seconds"])
            snapshot = await quote_client.fetch()
            international_price = resolve_international_gold_price(snapshot, fetched_at)
            if not international_price:
                return GoldReversalCycleResult(
                    sample_id=None,
                    signal_level=0,
                    triggered_conditions=[],
                    alert_triggered=False,
                    note="无可用国际金价源，已暂停黄金盘面监控",
                    fetched_at=fetched_at.isoformat(),
                    matched_events=0,
                    rss_errors=[],
                )
            domestic_price = resolve_domestic_gold_price(
                self.db,
                snapshot,
                fetched_at,
                international_price.price_cny_per_g,
            )
            price_source_note = build_price_source_note(
                domestic_price,
                international_price,
            ) if domestic_price else f"国际源: {international_price.source}"

            price_eval = self._evaluate_price_signal(
                gold_price=international_price.price_usd_per_oz,
                fetched_at=fetched_at,
                settings=settings,
            )
            political_signal, political_titles, political_events = self._evaluate_recent_event_signal(
                event_type="political",
                fetched_at=fetched_at,
                settings=settings,
            )
            war_signal, war_titles, war_events = self._evaluate_recent_event_signal(
                event_type="war",
                fetched_at=fetched_at,
                settings=settings,
            )

            us10y_triggered, us10y_note = self._evaluate_us10y_condition(
                fetched_at=fetched_at,
                signal_window_minutes=int(settings["reversal_signal_window_minutes"]),
                settings=settings,
            )
            conditions = {
                "price": price_eval.triggered,
                "political": political_signal,
                "war": war_signal,
                "us10y": us10y_triggered,
            }
            triggered_conditions = [name for name, value in conditions.items() if value]
            signal_level = self._resolve_signal_level(triggered_conditions)

            note_parts = [price_eval.note, price_source_note]
            note_parts.append(us10y_note)
            if political_titles:
                note_parts.append(f"政治事件: {' | '.join(political_titles[:2])}")
            if war_titles:
                note_parts.append(f"战争进度: {' | '.join(war_titles[:2])}")
            note_parts.append(self._build_rss_status_note())

            sample_id = self.db.insert_reversal_sample(
                {
                    "fetched_at": fetched_at.isoformat(),
                    "gold_price_usd_per_oz": round(international_price.price_usd_per_oz, 4),
                    "usdcny_rate": round(snapshot.usdcny_rate, 6),
                    "price_signal": int(price_eval.triggered),
                    "political_signal": int(political_signal),
                    "war_signal": int(war_signal),
                    "us10y_signal": int(conditions["us10y"]),
                    "signal_level": signal_level,
                    "triggered_conditions": ",".join(triggered_conditions),
                    "note": "；".join(part for part in note_parts if part),
                }
            )
            alert_triggered = False
            # Hard gate: only L1/L2 are allowed to push.
            if signal_level in {1, 2}:
                alert_triggered = await self._maybe_send_alert(
                    sample_id=sample_id,
                    fetched_at=fetched_at,
                    gold_price=international_price.price_usd_per_oz,
                    usdcny_rate=snapshot.usdcny_rate,
                    signal_level=signal_level,
                    triggered_conditions=triggered_conditions,
                    settings=settings,
                    price_eval=price_eval,
                    snapshot=snapshot,
                    domestic_price=domestic_price,
                    international_price_source=international_price.source,
                    political_titles=political_titles,
                    war_titles=war_titles,
                    political_events=political_events,
                    war_events=war_events,
                )
            self.db.insert_reversal_run(
                fetched_at=fetched_at,
                success=True,
                poll_interval_seconds=settings["poll_interval_seconds"],
                duration_ms=int((time.perf_counter() - started) * 1000),
                rss_error_count=0,
            )
            matched_event_count = len({str(item.get("id", "")) for item in (political_events + war_events) if item.get("id")})
            if not matched_event_count:
                matched_event_count = len(political_events) + len(war_events)
            return GoldReversalCycleResult(
                sample_id=sample_id,
                signal_level=signal_level,
                triggered_conditions=triggered_conditions,
                alert_triggered=alert_triggered,
                note="；".join(part for part in note_parts if part) or "无信号",
                fetched_at=fetched_at.isoformat(),
                matched_events=matched_event_count,
                rss_errors=[],
            )
        except Exception as exc:
            self.db.insert_reversal_run(
                fetched_at=fetched_at,
                success=False,
                poll_interval_seconds=settings["poll_interval_seconds"],
                duration_ms=int((time.perf_counter() - started) * 1000),
                rss_error_count=0,
                error_message=str(exc),
            )
            raise

    async def run_rss_scheduled_cycle(self) -> None:
        settings = self.db.get_settings()
        fetched_at = datetime.now(CN_TZ)
        await self._fetch_and_store_rss_events(
            fetched_at=fetched_at,
            settings=settings,
            force_refresh=False,
            include_unmatched=False,
            feed_urls_override=None,
        )

    async def run_rss_cycle(
        self,
        *,
        force_refresh: bool = True,
        include_unmatched: bool = False,
        feed_urls_override: list[str] | None = None,
        full_store: bool = False,
    ) -> dict[str, Any]:
        settings = self.db.get_settings()
        fetched_at = datetime.now(CN_TZ)
        rss_events, rss_errors, rss_note = await self._fetch_and_store_rss_events(
            fetched_at=fetched_at,
            settings=settings,
            force_refresh=force_refresh,
            include_unmatched=include_unmatched,
            feed_urls_override=feed_urls_override,
            full_store=full_store,
        )
        return {
            "fetched_at": fetched_at.isoformat(),
            "matched_events": len(rss_events),
            "rss_errors": rss_errors,
            "note": rss_note,
        }

    @staticmethod
    def _normalize_semantic_text(text: str) -> str:
        clean = HTML_TAG_RE.sub(" ", str(text or ""))
        clean = clean.lower().strip()
        clean = re.sub(r"\s+", " ", clean)
        return clean

    def _build_semantic_key(self, title: str, summary: str) -> str:
        merged = f"{self._normalize_semantic_text(title)} {self._normalize_semantic_text(summary)}"
        parts = [token for token in SEMANTIC_SPLIT_RE.split(merged) if token]
        tokens: list[str] = []
        for part in parts:
            if part in SEMANTIC_STOPWORDS:
                continue
            if len(part) <= 1 and not ("\u4e00" <= part <= "\u9fff"):
                continue
            tokens.append(part)
        if not tokens:
            tokens = [merged[:80]]
        # sorted unique token bag as semantic fingerprint base
        base = "|".join(sorted(set(tokens))[:80])
        return hashlib.sha1(base.encode("utf-8")).hexdigest()[:24]

    def _build_rss_status_note(self) -> str:
        latest_run = self.db.get_latest_rss_fetch_run()
        if not latest_run or not latest_run.get("fetched_at"):
            return "RSS独立调度: 暂无抓取记录"

        fetched_at = datetime.fromisoformat(str(latest_run["fetched_at"]))
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=CN_TZ)
        run_time_text = fetched_at.astimezone(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        if latest_run.get("success"):
            return f"RSS独立调度: 上次抓取成功({run_time_text})"
        message = str(latest_run.get("error_message") or "失败")
        return f"RSS独立调度: 上次抓取失败({run_time_text}) {message}"

    async def _fetch_and_store_rss_events(
        self,
        *,
        fetched_at: datetime,
        settings: dict[str, Any],
        force_refresh: bool = False,
        include_unmatched: bool = False,
        feed_urls_override: list[str] | None = None,
        full_store: bool = False,
    ) -> tuple[list[dict[str, Any]], list[str], str]:
        rss_interval = int(settings.get("rss_poll_interval_seconds", settings["poll_interval_seconds"]))
        last_fetch_run = self.db.get_latest_rss_fetch_run()
        if last_fetch_run and not force_refresh:
            last_fetched_at = datetime.fromisoformat(str(last_fetch_run["fetched_at"]))
            if last_fetched_at.tzinfo is None:
                last_fetched_at = last_fetched_at.replace(tzinfo=CN_TZ)
            if fetched_at - last_fetched_at < timedelta(seconds=rss_interval):
                return [], [], f"RSS缓存中，下次抓取窗口 {rss_interval}s"

        started = time.perf_counter()
        rss_client = RssFeedClient(timeout_seconds=settings["request_timeout_seconds"])
        feed_urls = feed_urls_override if feed_urls_override is not None else settings.get("rss_feed_urls", [])
        fetch_result = await rss_client.fetch_many(feed_urls)
        inserted: list[dict[str, Any]] = []
        seen_semantic_keys: set[str] = set()
        try:
            for entry in fetch_result.entries:
                classified = self._classify_entry(entry)
                if include_unmatched and not classified:
                    classified = [("general", ())]
                for event_type, matched_keywords in classified:
                    translated_title = await self.translator.to_chinese(entry.title, max_length=180)
                    translated_summary = await self.translator.to_chinese(entry.summary, max_length=360)
                    scored = score_gold_risk_event(
                        {
                            "title": translated_title or entry.title,
                            "summary": translated_summary or entry.summary[:2000],
                            "event_type": event_type,
                        }
                    )
                    semantic_key = self._build_semantic_key(scored["title"], scored["summary"])
                    if semantic_key in seen_semantic_keys:
                        continue
                    existed = self.db.get_rss_event_by_semantic_key(semantic_key)
                    if existed:
                        continue
                    event_time = entry.published_at.isoformat() if entry.published_at else fetched_at.isoformat()
                    gold_price, gold_change_pct = self.db.get_reversal_price_context(event_time)
                    payload = {
                        "fetched_at": fetched_at.isoformat(),
                        "published_at": entry.published_at.isoformat() if entry.published_at else None,
                        "source": entry.source,
                        "feed_url": entry.feed_url,
                        "title": scored["title"],
                        "link": entry.link,
                        "summary": scored["summary"],
                        "event_type": event_type,
                        "matched_keywords": ",".join(matched_keywords),
                        "content_hash": (
                            f"{entry.content_hash}:{event_type}:{fetched_at.isoformat()}"
                            if full_store
                            else f"{entry.content_hash}:{event_type}"
                        ),
                        "semantic_key": semantic_key,
                        "impact_score": scored["impact_score"],
                        "impact_level": scored["impact_level"],
                        "impact_note": scored["impact_note"],
                        "event_gold_price_usd_per_oz": gold_price,
                        "event_gold_change_pct": gold_change_pct,
                    }
                    event_id = self.db.insert_rss_event(payload)
                    if event_id:
                        seen_semantic_keys.add(semantic_key)
                        event_text = str(payload["title"]).strip()
                        self.db.insert_rss_ml_sample(
                            event_id=event_id,
                            event_text=event_text,
                            gold_price_usd_per_oz=None,
                            gold_change_pct=None,
                            target_score=int(scored["impact_score"]),
                            created_at=fetched_at.isoformat(),
                        )
                        inserted.append(
                            {
                                "event_id": event_id,
                                "event_text": event_text,
                                "gold_price": gold_price,
                                "gold_change_pct": gold_change_pct,
                            }
                        )

            dedup_result = self.db.deduplicate_rss_events_by_semantic_key()
            if self.ml_service:
                if inserted:
                    self.ml_service.maybe_train()
                now_iso = datetime.now(CN_TZ).isoformat()
                for item in inserted:
                    predicted = self.ml_service.predict_score(
                        event_text=str(item["event_text"]),
                        gold_price_usd_per_oz=item["gold_price"],
                        gold_change_pct=item["gold_change_pct"],
                    )
                    if not predicted:
                        continue
                    score, model_version, bucket_label, prob_map = predicted
                    event_id = int(item["event_id"])
                    self.db.update_rss_event_ml_score(
                        event_id=event_id,
                        ml_score=score,
                        ml_model_version=model_version,
                        ml_scored_at=now_iso,
                        ml_bucket_label=bucket_label,
                        ml_class_probs=json.dumps(prob_map, ensure_ascii=False),
                    )
                    self.db.update_rss_ml_prediction(
                        event_id=event_id,
                        predicted_score=score,
                        model_version=model_version,
                        scored_at=now_iso,
                    )
                # Backfill ML score for existing unscored events after each fetch cycle,
                # then sync fetch-events CSV snapshot.
                try:
                    self.ml_service.score_unscored_rss_events(limit=5000)
                except Exception:
                    pass
                try:
                    self.ml_service.sync_fetched_csv_from_db(overwrite=True)
                except Exception:
                    pass
            self.db.insert_rss_fetch_run(
                fetched_at=fetched_at,
                success=not fetch_result.errors,
                duration_ms=int((time.perf_counter() - started) * 1000),
                item_count=len(inserted),
                error_count=len(fetch_result.errors),
                error_message=" | ".join(fetch_result.errors[:3]) if fetch_result.errors else None,
            )
            return inserted, fetch_result.errors, f"RSS新增 {len(inserted)} 条"
        except Exception as exc:
            self.db.insert_rss_fetch_run(
                fetched_at=fetched_at,
                success=False,
                duration_ms=int((time.perf_counter() - started) * 1000),
                item_count=len(inserted),
                error_count=len(fetch_result.errors) + 1,
                error_message=str(exc),
            )
            raise

    def _evaluate_price_signal(
        self,
        *,
        gold_price: float,
        fetched_at: datetime,
        settings: dict[str, Any],
    ) -> PriceSignalEvaluation:
        lookback_minutes = int(settings["reversal_price_lookback_minutes"])
        ma_window = int(settings["reversal_price_ma_window"])
        rebound_pct = float(settings["reversal_price_rebound_pct"])
        since = fetched_at - timedelta(minutes=lookback_minutes)
        samples = self.db.get_reversal_samples_since(since.isoformat(), limit=1000)
        if len(samples) < 3:
            return PriceSignalEvaluation(
                triggered=False,
                note=f"盘面样本预热中({len(samples)}/3)，已获取实时价格",
                rebound_pct=0.0,
                recent_low=None,
                short_ma=None,
            )

        historical_prices = [float(sample["gold_price_usd_per_oz"]) for sample in samples]
        recent_low = min(historical_prices)
        effective_ma_window = min(ma_window, len(historical_prices))
        short_ma_prices = historical_prices[-effective_ma_window:]
        short_ma = sum(short_ma_prices) / len(short_ma_prices)
        previous_price = historical_prices[-1]
        rebound = ((gold_price / recent_low) - 1) * 100 if recent_low else 0.0
        signal = (
            gold_price >= recent_low * (1 + rebound_pct / 100)
            and gold_price > short_ma
            and gold_price > previous_price
        )
        warmup_suffix = ""
        if len(samples) < ma_window:
            warmup_suffix = f"（样本预热 {len(samples)}/{ma_window}，已使用MA{effective_ma_window}）"
        note = (
            f"盘面: 现价 {gold_price:.2f} / 低点 {recent_low:.2f} / "
            f"MA{effective_ma_window} {short_ma:.2f} / 反弹 {rebound:.2f}%"
            f"{warmup_suffix}"
        )
        return PriceSignalEvaluation(
            triggered=signal,
            note=note,
            rebound_pct=round(rebound, 2),
            recent_low=recent_low,
            short_ma=short_ma,
        )

    def _evaluate_recent_event_signal(
        self,
        *,
        event_type: str,
        fetched_at: datetime,
        settings: dict[str, Any],
    ) -> tuple[bool, list[str], list[dict[str, Any]]]:
        signal_window_minutes = int(settings["reversal_signal_window_minutes"])
        since = fetched_at - timedelta(minutes=signal_window_minutes)
        events = self.db.get_recent_rss_events(
            limit=20,
            event_type=event_type,
            since_iso=since.isoformat(),
        )
        titles = [str(event["title"]) for event in events[:3]]
        return bool(events), titles, events[:3]

    def _classify_entry(self, entry: RssEntry) -> list[tuple[str, list[str]]]:
        text = f"{entry.title} {entry.summary}".lower()
        matches: list[tuple[str, list[str]]] = []
        context_hits = self._match_keywords(text, CONTEXT_KEYWORDS)
        if not context_hits:
            return matches
        negative_hits = self._match_keywords(text, NEGATIVE_FILTER_KEYWORDS)
        political_hits = self._match_keywords(text, POLITICAL_POSITIVE_KEYWORDS)
        war_hits = self._match_keywords(text, WAR_PROGRESS_KEYWORDS)

        if political_hits and len(political_hits) >= len(negative_hits):
            matches.append(("political", political_hits))
        if war_hits and len(war_hits) >= len(negative_hits):
            matches.append(("war", war_hits))
        return matches

    @staticmethod
    def _match_keywords(text: str, keywords: tuple[str, ...]) -> list[str]:
        matches: list[str] = []
        for keyword in keywords:
            if keyword.isascii() and keyword.replace(" ", "").isalpha():
                pattern = rf"\b{re.escape(keyword)}\w*\b"
                if re.search(pattern, text):
                    matches.append(keyword)
            elif keyword in text:
                matches.append(keyword)
        return matches

    @staticmethod
    def _resolve_signal_level(triggered_conditions: list[str]) -> int:
        match len(triggered_conditions):
            case 4:
                return 1
            case 3:
                return 2
            case 2:
                return 3
            case 1:
                return 4
            case _:
                return 0

    def _evaluate_us10y_condition(
        self,
        *,
        fetched_at: datetime,
        signal_window_minutes: int,
        settings: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        tenors = (settings or {}).get("us10y_tenors", ["10y"])
        primary_tenor = "10y" if "10y" in tenors else tenors[0]
        latest = self.db.get_latest_us10y_sample(tenor=primary_tenor)
        if not latest:
            return False, f"{primary_tenor.upper()}美债: 暂无数据"
        try:
            latest_time = datetime.fromisoformat(str(latest["fetched_at"]))
        except Exception:
            return False, f"{primary_tenor.upper()}美债: 最新样本时间解析失败"
        if latest_time.tzinfo is None:
            latest_time = latest_time.replace(tzinfo=CN_TZ)
        lag = fetched_at - latest_time.astimezone(CN_TZ)
        if lag > timedelta(minutes=signal_window_minutes):
            return False, (
                f"{primary_tenor.upper()}美债: 最新样本过期({int(lag.total_seconds() // 60)}分钟)"
            )
        signal = bool(int(latest.get("yield_signal", 0)))
        note = str(latest.get("note", "")).strip()
        return signal, note or f"{primary_tenor.upper()}美债: 无说明"

    async def _maybe_send_alert(
        self,
        *,
        sample_id: int,
        fetched_at: datetime,
        gold_price: float,
        usdcny_rate: float,
        signal_level: int,
        triggered_conditions: list[str],
        settings: dict[str, Any],
        price_eval: PriceSignalEvaluation,
        snapshot: QuoteSnapshot,
        domestic_price: Any,
        international_price_source: str,
        political_titles: list[str],
        war_titles: list[str],
        political_events: list[dict[str, Any]],
        war_events: list[dict[str, Any]],
    ) -> bool:
        if signal_level not in {1, 2}:
            return False

        if not resolve_notification_targets(settings):
            return False

        last_success = self.db.get_last_successful_reversal_alert()
        if last_success:
            last_sent = datetime.fromisoformat(last_success["sent_at"])
            cooldown = timedelta(seconds=int(settings["reversal_cooldown_seconds"]))
            if last_sent.tzinfo is None:
                last_sent = last_sent.replace(tzinfo=timezone.utc).astimezone(CN_TZ)
            if fetched_at - last_sent < cooldown and signal_level >= int(last_success["signal_level"]):
                return False

        level_name = {1: "一级", 2: "二级", 3: "三级", 4: "四级"}[signal_level]
        premium_text = self._build_sge_premium_text(
            snapshot=snapshot,
            fetched_at=fetched_at,
            domestic_price=domestic_price,
        )
        event_link = self._pick_event_link(political_events, war_events)
        text_lines = [
            f"黄金反转{level_name}信号",
            f"时间: {fetched_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"现货金: {gold_price:.2f} 美元/盎司",
            f"USDCNY: {usdcny_rate:.4f}",
            f"触发条件: {', '.join(triggered_conditions)}",
            f"反转幅度: {price_eval.rebound_pct:.2f}%",
            premium_text,
            f"国际金来源: {international_price_source}",
        ]
        if domestic_price:
            text_lines.append(f"人民币金来源: {domestic_price.source}")
        if political_titles:
            text_lines.append(f"政治事件: {political_titles[0]}")
        if war_titles:
            text_lines.append(f"战争进度: {war_titles[0]}")
        if event_link:
            text_lines.append(f"事件链接: {event_link}")

        message_text = "\n".join(text_lines)
        success, response_text, details = await post_text_to_targets_detailed(settings, message_text, timeout_seconds=10)
        self.db.insert_reversal_alert_event(
            {
                "sample_id": sample_id,
                "sent_at": fetched_at.isoformat(),
                "signal_level": signal_level,
                "triggered_conditions": ",".join(triggered_conditions),
                "success": int(success),
                "response_text": response_text,
                "webhook_url": settings.get("dingtalk_webhook", ""),
            }
        )
        for detail in details:
            self.db.insert_notification_log(
                sent_at=fetched_at,
                channel="dingtalk",
                event_type="reversal_alert",
                target_name=detail["target_name"],
                webhook_url=detail["webhook_url"],
                success=detail["success"] == "1",
                content=message_text,
                response_text=detail["response_text"],
            )
        return success

    def _build_sge_premium_text(self, *, snapshot: QuoteSnapshot, fetched_at: datetime, domestic_price: Any) -> str:
        international_price = resolve_international_gold_price(snapshot, fetched_at)
        if international_price and domestic_price:
            premium = domestic_price.price_cny_per_g - international_price.price_cny_per_g
            return f"SGE 溢价金额: {premium:.4f} 元/克"

        latest_effective = self.db.get_latest_effective_premium_sample()
        if latest_effective:
            sample_time = datetime.fromisoformat(str(latest_effective["fetched_at"]))
            sample_time_text = sample_time.astimezone(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")
            return (
                f"最近有效 SGE 溢价: {float(latest_effective['premium_cny_per_g']):.4f} 元/克 "
                f"({sample_time_text})"
            )
        return "SGE 溢价金额: 当前休市或无法计算"

    @staticmethod
    def _pick_event_link(*event_groups: list[dict[str, Any]]) -> str:
        for events in event_groups:
            for event in events:
                link = GoldReversalMonitorService._normalize_event_link(
                    str(event.get("link", "")).strip(),
                    str(event.get("feed_url", "")).strip(),
                )
                if link:
                    return link
        return ""

    @staticmethod
    def _normalize_event_link(link: str, feed_url: str = "") -> str:
        candidate = link.strip()
        if candidate.startswith(("http://", "https://")):
            return candidate
        if candidate.startswith("/") and feed_url:
            return urljoin(feed_url, candidate)
        parsed_feed = urlparse(feed_url)
        if candidate and parsed_feed.scheme and parsed_feed.netloc:
            return urljoin(f"{parsed_feed.scheme}://{parsed_feed.netloc}", candidate)
        if feed_url.startswith(("http://", "https://")):
            return feed_url
        return ""

    async def send_test_alert(self, *, level: int = 3, note: str = "") -> tuple[bool, str]:
        settings = self.db.get_settings()
        if not resolve_notification_targets(settings):
            return False, "No enabled notification targets"
        level_name = {1: "一级", 2: "二级", 3: "三级", 4: "四级"}.get(level, "四级")
        trigger_map = {
            1: "price, political, war, us10y",
            2: "price, political, war",
            3: "price, political",
            4: "price",
        }
        recent_events = self.db.get_recent_rss_events(limit=6)
        political_events = [event for event in recent_events if str(event.get("event_type", "")) == "political"]
        war_events = [event for event in recent_events if str(event.get("event_type", "")) == "war"]
        event_link = self._pick_event_link(political_events, war_events)
        text_lines = [
            f"黄金反转{level_name}信号测试",
            f"时间: {datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')}",
            "现货金: 4496.98 美元/盎司",
            "USDCNY: 7.2280",
            f"触发条件: {trigger_map.get(level, 'price')}",
            "反转幅度: 1.86%",
            self._build_test_premium_text(),
            f"说明: {note or '这是后台测试推送，用于验证 webhook 与加签配置。'}",
        ]
        if level <= 2 and political_events:
            text_lines.append(f"政治事件: {political_events[0]['title']}")
        if level == 1 and war_events:
            text_lines.append(f"战争进度: {war_events[0]['title']}")
        if event_link:
            text_lines.append(f"事件链接: {event_link}")
        text = "\n".join(text_lines)
        success, response_text, details = await post_text_to_targets_detailed(settings, text, timeout_seconds=10)
        now = datetime.now(CN_TZ)
        for detail in details:
            self.db.insert_notification_log(
                sent_at=now,
                channel="dingtalk",
                event_type="reversal_test_alert",
                target_name=detail["target_name"],
                webhook_url=detail["webhook_url"],
                success=detail["success"] == "1",
                content=text,
                response_text=detail["response_text"],
            )
        return success, response_text

    def _build_test_premium_text(self) -> str:
        latest_effective = self.db.get_latest_effective_premium_sample()
        if latest_effective:
            return f"SGE 溢价金额: {float(latest_effective['premium_cny_per_g']):.4f} 元/克"
        return "SGE 溢价金额: 1.5234 元/克"

    @staticmethod
    def _has_any_monitor_window(fetched_at: datetime) -> bool:
        return is_shfe_gold_open(fetched_at) or is_london_gold_open(fetched_at) or is_comex_gold_open(fetched_at)
