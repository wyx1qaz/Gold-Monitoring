from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import BASE_DIR, DEFAULT_SETTINGS, SHANGHAI_TZ
from .db import Database
from .gold_event_scoring import score_gold_risk_event
from .market_hours import is_comex_gold_open, is_london_gold_open, is_shfe_gold_open
from .monitor import MonitorService
from .reversal_monitor import GoldReversalMonitorService
from .rss_ml import RssMlService
from .us10y_monitor import US10YMonitorService


CN_TZ = ZoneInfo(SHANGHAI_TZ)
logger = logging.getLogger(__name__)
db = Database()
monitor = MonitorService(db)
rss_ml_service = RssMlService(db)
reversal_monitor = GoldReversalMonitorService(db, ml_service=rss_ml_service)
us10y_monitor = US10YMonitorService(db)
scheduler = AsyncIOScheduler(timezone=SHANGHAI_TZ)
static_dir = BASE_DIR / "static"
RANGE_DELTA_MAP: dict[str, timedelta] = {
    "1H": timedelta(hours=1),
    "1D": timedelta(days=1),
    "1W": timedelta(weeks=1),
}
HISTORY_LIMIT_FLOOR = 2000
HISTORY_LIMIT_CAP = 120000
EXTENDED_TEST_RSS_FEEDS = [
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://news.google.com/rss/search?q=gold+market&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=iran+israel+middle+east&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=fed+rate+cut+hike&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=hormuz+shipping+oil&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=treasury+yield+10y+20y+5y&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=ceasefire+war+negotiation&hl=en-US&gl=US&ceid=US:en",
]


class NotificationTargetUpdate(BaseModel):
    name: str = Field(default="默认机器人", min_length=1, max_length=50)
    webhook: str = Field(default="", min_length=1)
    secret: str = Field(default="")
    enabled: bool = Field(default=True)


class RssFeedUpdate(BaseModel):
    name: str = Field(default="RSS源", min_length=1, max_length=80)
    url: str = Field(default="", min_length=1)
    enabled: bool = Field(default=True)


class SettingsUpdate(BaseModel):
    dingtalk_webhook: str = Field(default=DEFAULT_SETTINGS.dingtalk_webhook)
    dingtalk_secret: str = Field(default=DEFAULT_SETTINGS.dingtalk_secret)
    dingtalk_at_user_ids: list[str] = Field(default_factory=lambda: list(DEFAULT_SETTINGS.dingtalk_at_user_ids))
    notification_targets: list[NotificationTargetUpdate] = Field(default_factory=list)
    premium_threshold: float = Field(default=DEFAULT_SETTINGS.premium_threshold, ge=0)
    poll_interval_seconds: int = Field(default=DEFAULT_SETTINGS.poll_interval_seconds, ge=5, le=3600)
    alert_cooldown_seconds: int = Field(default=DEFAULT_SETTINGS.alert_cooldown_seconds, ge=0, le=86400)
    request_timeout_seconds: float = Field(default=DEFAULT_SETTINGS.request_timeout_seconds, ge=1, le=60)
    reversal_cooldown_seconds: int = Field(default=DEFAULT_SETTINGS.reversal_cooldown_seconds, ge=0, le=86400)
    reversal_price_lookback_minutes: int = Field(default=DEFAULT_SETTINGS.reversal_price_lookback_minutes, ge=30, le=1440)
    reversal_price_rebound_pct: float = Field(default=DEFAULT_SETTINGS.reversal_price_rebound_pct, ge=0.1, le=20)
    reversal_price_ma_window: int = Field(default=DEFAULT_SETTINGS.reversal_price_ma_window, ge=3, le=240)
    reversal_signal_window_minutes: int = Field(default=DEFAULT_SETTINGS.reversal_signal_window_minutes, ge=15, le=1440)
    us10y_poll_interval_seconds: int = Field(default=DEFAULT_SETTINGS.us10y_poll_interval_seconds, ge=5, le=3600)
    us10y_drop_lookback_hours: float = Field(default=DEFAULT_SETTINGS.us10y_drop_lookback_hours, ge=1, le=240)
    us10y_drop_threshold_bp: float = Field(default=DEFAULT_SETTINGS.us10y_drop_threshold_bp, ge=0.1, le=100)
    us10y_alert_cooldown_seconds: int = Field(default=DEFAULT_SETTINGS.us10y_alert_cooldown_seconds, ge=0, le=86400)
    us10y_alert_dedup_hours: int = Field(default=DEFAULT_SETTINGS.us10y_alert_dedup_hours, ge=0, le=168)
    us10y_tenors: list[str] = Field(default_factory=lambda: list(DEFAULT_SETTINGS.us10y_tenors))
    rss_poll_interval_seconds: int = Field(default=DEFAULT_SETTINGS.rss_poll_interval_seconds, ge=30, le=3600)
    rss_feeds: list[RssFeedUpdate] = Field(default_factory=list)
    rss_feed_urls: list[str] = Field(default_factory=lambda: list(DEFAULT_SETTINGS.rss_feed_urls))


class ReversalSettingsUpdate(BaseModel):
    dingtalk_webhook: str = Field(default=DEFAULT_SETTINGS.dingtalk_webhook)
    dingtalk_secret: str = Field(default=DEFAULT_SETTINGS.dingtalk_secret)
    dingtalk_at_user_ids: list[str] = Field(default_factory=lambda: list(DEFAULT_SETTINGS.dingtalk_at_user_ids))
    notification_targets: list[NotificationTargetUpdate] = Field(default_factory=list)
    poll_interval_seconds: int = Field(default=DEFAULT_SETTINGS.poll_interval_seconds, ge=5, le=3600)
    request_timeout_seconds: float = Field(default=DEFAULT_SETTINGS.request_timeout_seconds, ge=1, le=60)
    reversal_cooldown_seconds: int = Field(default=DEFAULT_SETTINGS.reversal_cooldown_seconds, ge=0, le=86400)
    reversal_price_lookback_minutes: int = Field(default=DEFAULT_SETTINGS.reversal_price_lookback_minutes, ge=30, le=1440)
    reversal_price_rebound_pct: float = Field(default=DEFAULT_SETTINGS.reversal_price_rebound_pct, ge=0.1, le=20)
    reversal_price_ma_window: int = Field(default=DEFAULT_SETTINGS.reversal_price_ma_window, ge=3, le=240)
    reversal_signal_window_minutes: int = Field(default=DEFAULT_SETTINGS.reversal_signal_window_minutes, ge=15, le=1440)
    us10y_poll_interval_seconds: int = Field(default=DEFAULT_SETTINGS.us10y_poll_interval_seconds, ge=5, le=3600)
    us10y_drop_lookback_hours: float = Field(default=DEFAULT_SETTINGS.us10y_drop_lookback_hours, ge=1, le=240)
    us10y_drop_threshold_bp: float = Field(default=DEFAULT_SETTINGS.us10y_drop_threshold_bp, ge=0.1, le=100)
    us10y_alert_cooldown_seconds: int = Field(default=DEFAULT_SETTINGS.us10y_alert_cooldown_seconds, ge=0, le=86400)
    us10y_alert_dedup_hours: int = Field(default=DEFAULT_SETTINGS.us10y_alert_dedup_hours, ge=0, le=168)
    us10y_tenors: list[str] = Field(default_factory=lambda: list(DEFAULT_SETTINGS.us10y_tenors))
    rss_poll_interval_seconds: int = Field(default=DEFAULT_SETTINGS.rss_poll_interval_seconds, ge=30, le=3600)
    rss_feeds: list[RssFeedUpdate] = Field(default_factory=list)
    rss_feed_urls: list[str] = Field(default_factory=lambda: list(DEFAULT_SETTINGS.rss_feed_urls))


class TestAlertPayload(BaseModel):
    level: int = Field(default=4, ge=1, le=4)
    note: str = Field(default="")


class RssMlConfigUpdate(BaseModel):
    rss_ml_learning_rate: float = Field(default=0.001, ge=0.000001, le=1.0)
    rss_ml_max_epochs: int = Field(default=300, ge=20, le=2000)
    rss_ml_early_stop_patience: int = Field(default=25, ge=3, le=200)
    rss_ml_train_step_size: int = Field(default=100, ge=10, le=1000)
    rss_ml_min_train_samples: int = Field(default=30, ge=10, le=1000)
    rss_ml_active_window_hours: int = Field(default=24, ge=1, le=24)
    rss_ml_weak_move_pct: float = Field(default=0.10, ge=0.01, le=5)
    rss_ml_strong_move_pct: float = Field(default=0.35, ge=0.02, le=10)
    rss_ml_decay_half_life_hours: float = Field(default=24, ge=1, le=240)
    rss_ml_label_mode: Literal["future_return", "manual_score"] = Field(default="future_return")


class RssMlTrainPayload(BaseModel):
    force: bool = Field(default=True)
    min_samples_override: int | None = Field(default=None, ge=10, le=10000)


class RssMlTrainControlPayload(BaseModel):
    action: Literal["pause", "resume", "cancel"] = Field(default="pause")


class RssBulkFillPayload(BaseModel):
    rounds: int = Field(default=3, ge=1, le=20)
    include_unmatched: bool = Field(default=True)
    use_extended_sources: bool = Field(default=False)


class RssMlClearPayload(BaseModel):
    remove_model_file: bool = Field(default=True)


class RssMlCsvSyncPayload(BaseModel):
    overwrite: bool = Field(default=False)


async def run_all_monitors() -> dict[str, dict]:
    sge_result = await monitor.run_cycle()
    reversal_result = await reversal_monitor.run_cycle()
    return {
        "sge": sge_result.__dict__,
        "us10y_reversal": db.get_latest_us10y_sample(),
        "gold_reversal": reversal_result.__dict__,
    }


async def run_all_monitors_with_rss() -> dict[str, dict]:
    result = await run_all_monitors()
    result["rss"] = await reversal_monitor.run_rss_cycle()
    return result


def build_market_state() -> dict[str, dict[str, str | bool]]:
    now = datetime.now(CN_TZ)
    shfe_open = is_shfe_gold_open(now)
    london_open = is_london_gold_open(now)
    comex_open = is_comex_gold_open(now)
    any_market_open = shfe_open or london_open or comex_open
    return {
        "sge": {
            "active": any_market_open,
            "label": "运行中" if any_market_open else "已暂停",
            "detail": "国内金或国际金有可用价格源" if any_market_open else "国内金与国际金均休市",
        },
        "reversal": {
            "active": any_market_open,
            "label": "运行中" if any_market_open else "已暂停",
            "detail": "黄金盘面监控可用" if any_market_open else "国内金与国际金均休市",
        },
        "us10y": {
            "active": True,
            "label": "可用",
            "detail": "十年期美债数据源可用（Sina优先，FRED回退）",
        },
        "rss": {
            "active": True,
            "label": "可用",
            "detail": "RSS 手动抓取和定时抓取配置可用",
        },
    }


def reschedule_monitor_job() -> None:
    settings = db.get_settings()
    interval = settings["poll_interval_seconds"]
    job_id = "monitor-cycle"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    scheduler.add_job(
        run_all_monitors,
        "interval",
        seconds=interval,
        id=job_id,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(CN_TZ) + timedelta(seconds=interval),
        misfire_grace_time=30,
    )


def reschedule_rss_job() -> None:
    settings = db.get_settings()
    interval = settings["rss_poll_interval_seconds"]
    job_id = "rss-cycle"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    now = datetime.now(CN_TZ)
    next_run_time = now + timedelta(seconds=interval)
    last_fetch_run = db.get_latest_rss_fetch_run()
    if last_fetch_run and last_fetch_run.get("fetched_at"):
        last_fetched_at = datetime.fromisoformat(str(last_fetch_run["fetched_at"]))
        if last_fetched_at.tzinfo is None:
            last_fetched_at = last_fetched_at.replace(tzinfo=CN_TZ)
        next_run_time = max(last_fetched_at + timedelta(seconds=interval), now + timedelta(seconds=1))
    scheduler.add_job(
        reversal_monitor.run_rss_scheduled_cycle,
        "interval",
        seconds=interval,
        id=job_id,
        max_instances=1,
        coalesce=True,
        next_run_time=next_run_time,
        misfire_grace_time=60,
    )


def reschedule_us10y_job() -> None:
    settings = db.get_settings()
    interval = settings["us10y_poll_interval_seconds"]
    job_id = "us10y-cycle"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    scheduler.add_job(
        us10y_monitor.run_cycle,
        "interval",
        seconds=interval,
        id=job_id,
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(CN_TZ) + timedelta(seconds=interval),
        misfire_grace_time=30,
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.initialize()
    if not scheduler.running:
        scheduler.start()
    try:
        await run_all_monitors()
    except Exception:
        logger.exception("Initial monitor cycle failed during startup")
    try:
        await reversal_monitor.run_rss_scheduled_cycle()
    except Exception:
        logger.exception("Initial RSS cycle failed during startup")
    try:
        await us10y_monitor.run_cycle()
    except Exception:
        logger.exception("Initial US10Y cycle failed during startup")
    reschedule_monitor_job()
    reschedule_rss_job()
    reschedule_us10y_job()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="黄金监控中台", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


POSITIVE_KEYWORDS = (
    "ceasefire",
    "truce",
    "talks",
    "negotiation",
    "agreement",
    "de-escalat",
    "reopen",
    "restore",
    "shipping resume",
    "corridor reopen",
    "hormuz reopen",
    "fed cut",
    "rate cut",
    "dovish",
    "easing",
    "停火",
    "和谈",
    "会谈",
    "协议",
    "降温",
    "复航",
    "恢复通航",
    "恢复出口",
    "霍尔木兹恢复",
    "降息",
)

NEGATIVE_KEYWORDS = (
    "attack",
    "airstrike",
    "missile",
    "drone",
    "explosion",
    "killed",
    "casualties",
    "blockade",
    "closure",
    "retaliat",
    "escalat",
    "fed hike",
    "rate hike",
    "hawkish",
    "冲突",
    "袭击",
    "导弹",
    "空袭",
    "伤亡",
    "封锁",
    "报复",
    "升级",
    "加息",
)

LOW_CONFIDENCE_KEYWORDS = (
    "said",
    "says",
    "warned",
    "warning",
    "threat",
    "tweet",
    "post",
    "commentary",
    "opinion",
    "rumor",
    "可能",
    "或许",
    "预计",
    "传闻",
    "喊话",
    "表态",
    "嘴炮",
    "舆论",
)

STRONG_NEGATIVE_PATTERNS = (
    "最后通牒",
    "最后期限",
    "紧张局势加剧",
    "战争紧张局势加剧",
    "tensions escalate",
    "tensions rise",
    "ultimatum",
    "deadline",
)


def _score_gold_risk_event(event: dict) -> dict:
    return score_gold_risk_event(event)


def _score_gold_risk_events(events: list[dict]) -> list[dict]:
    return [_score_gold_risk_event(event) for event in events]


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")


@app.get("/api/status")
async def get_status() -> dict:
    latest = db.get_latest_sample()
    job = scheduler.get_job("monitor-cycle")
    rss_job = scheduler.get_job("rss-cycle")
    return {
        "settings": db.get_settings(),
        "market_state": build_market_state(),
        "latest_sample": latest,
        "recent_alerts": db.get_recent_alerts(limit=10),
        "recent_fetch_runs": db.get_recent_fetch_runs(limit=10),
        "recent_samples": db.get_recent_samples(limit=20),
        "gold_reversal": {
            "latest_sample": db.get_latest_reversal_sample(),
            "recent_alerts": db.get_recent_reversal_alerts(limit=10),
            "recent_runs": db.get_recent_reversal_runs(limit=10),
            "recent_events": _score_gold_risk_events(db.get_recent_rss_events(limit=20)),
            "recent_rss_fetch_runs": db.get_recent_rss_fetch_runs(limit=10),
        },
        "us10y_reversal": {
            "latest_sample": db.get_latest_us10y_sample(tenor="10y"),
            "latest_samples": db.get_latest_us10y_samples(db.get_settings().get("us10y_tenors", ["10y"])),
            "recent_runs": db.get_recent_us10y_runs(limit=10),
        },
        "rss_ml": rss_ml_service.get_status(),
        "scheduler": {
            "running": scheduler.running,
            "next_run_time": job.next_run_time.isoformat() if job and job.next_run_time else None,
            "rss_next_run_time": rss_job.next_run_time.isoformat() if rss_job and rss_job.next_run_time else None,
            "us10y_next_run_time": scheduler.get_job("us10y-cycle").next_run_time.isoformat() if scheduler.get_job("us10y-cycle") and scheduler.get_job("us10y-cycle").next_run_time else None,
        },
    }


def _get_since_by_range(range_key: Literal["1H", "1D", "1W"]) -> datetime:
    return datetime.now(CN_TZ) - RANGE_DELTA_MAP[range_key]


def _estimate_history_limit(
    *,
    range_key: Literal["1H", "1D", "1W"],
    interval_seconds: int,
    series_count: int = 1,
) -> int:
    safe_interval = max(5, int(interval_seconds))
    safe_series_count = max(1, int(series_count))
    expected = int((RANGE_DELTA_MAP[range_key].total_seconds() / safe_interval) * safe_series_count)
    buffer = max(120, expected // 10)
    return max(HISTORY_LIMIT_FLOOR, min(HISTORY_LIMIT_CAP, expected + buffer))


def _downsample_by_stride(items: list[dict], stride: int) -> list[dict]:
    safe_stride = max(1, int(stride))
    if safe_stride <= 1 or len(items) <= 2:
        return items
    sampled = items[::safe_stride]
    last_item = items[-1]
    if not sampled or sampled[-1].get("id") != last_item.get("id"):
        sampled.append(last_item)
    return sampled


def _downsample_us10y_items(items: list[dict], stride: int) -> list[dict]:
    safe_stride = max(1, int(stride))
    if safe_stride <= 1:
        return items
    buckets: dict[str, list[dict]] = {}
    for item in items:
        tenor = str(item.get("tenor") or "10y").lower()
        buckets.setdefault(tenor, []).append(item)
    sampled: list[dict] = []
    for tenor_items in buckets.values():
        sampled.extend(_downsample_by_stride(tenor_items, safe_stride))
    return sorted(sampled, key=lambda item: item.get("fetched_at", ""))


@app.get("/api/history")
async def get_history(range: Literal["1H", "1D", "1W"] = "1D") -> dict:
    since = _get_since_by_range(range)
    return {
        "range": range,
        "since": since.isoformat(),
        "items": db.get_history(since.isoformat()),
    }


@app.get("/api/alerts")
async def get_alerts(limit: int = 20) -> dict:
    return {"items": db.get_recent_alerts(limit=limit)}


@app.get("/api/notification/logs")
async def get_notification_logs(limit: int = 50) -> dict:
    return {"items": db.get_recent_notification_logs(limit=limit)}


@app.get("/api/reversal/status")
async def get_reversal_status() -> dict:
    job = scheduler.get_job("monitor-cycle")
    rss_job = scheduler.get_job("rss-cycle")
    return {
        "settings": db.get_settings(),
        "market_state": build_market_state(),
        "latest_sample": db.get_latest_reversal_sample(),
        "recent_alerts": db.get_recent_reversal_alerts(limit=10),
        "recent_runs": db.get_recent_reversal_runs(limit=10),
        "recent_events": _score_gold_risk_events(db.get_recent_rss_events(limit=50)),
        "recent_rss_fetch_runs": db.get_recent_rss_fetch_runs(limit=10),
        "rss_ml": rss_ml_service.get_status(),
        "scheduler": {
            "running": scheduler.running,
            "next_run_time": job.next_run_time.isoformat() if job and job.next_run_time else None,
            "rss_next_run_time": rss_job.next_run_time.isoformat() if rss_job and rss_job.next_run_time else None,
        },
    }


@app.get("/api/us10y/status")
async def get_us10y_status() -> dict:
    settings = db.get_settings()
    tenors = settings.get("us10y_tenors", ["10y"])
    primary = "10y" if "10y" in tenors else tenors[0]
    job = scheduler.get_job("us10y-cycle")
    return {
        "settings": settings,
        "market_state": build_market_state(),
        "latest_sample": db.get_latest_us10y_sample(tenor=primary),
        "latest_samples": db.get_latest_us10y_samples(tenors),
        "source_status": us10y_monitor.get_source_status(),
        "recent_runs": db.get_recent_us10y_runs(limit=20),
        "scheduler": {
            "running": scheduler.running,
            "next_run_time": job.next_run_time.isoformat() if job and job.next_run_time else None,
        },
    }


@app.get("/api/reversal/history")
async def get_reversal_history(range: Literal["1H", "1D", "1W"] = "1D", stride: int = 1) -> dict:
    settings = db.get_settings()
    since = _get_since_by_range(range)
    limit = _estimate_history_limit(
        range_key=range,
        interval_seconds=int(settings.get("poll_interval_seconds", 60)),
    )
    items = db.get_reversal_history(since.isoformat(), limit=limit)
    sampled = _downsample_by_stride(items, stride=max(1, min(200, int(stride))))
    return {
        "range": range,
        "since": since.isoformat(),
        "items": sampled,
    }


@app.get("/api/us10y/history")
async def get_us10y_history(range: Literal["1H", "1D", "1W"] = "1D", stride: int = 1) -> dict:
    settings = db.get_settings()
    since = _get_since_by_range(range)
    tenors = settings.get("us10y_tenors", ["10y"]) or ["10y"]
    limit = _estimate_history_limit(
        range_key=range,
        interval_seconds=int(settings.get("us10y_poll_interval_seconds", 60)),
        series_count=len(tenors),
    )
    items = db.get_us10y_history(since.isoformat(), limit=limit, tenors=tenors)
    sampled = _downsample_us10y_items(items, stride=max(1, min(200, int(stride))))
    return {
        "range": range,
        "since": since.isoformat(),
        "items": sampled,
    }


@app.get("/api/reversal/events")
async def get_reversal_events(limit: int = 20, event_type: str | None = None) -> dict:
    return {"items": _score_gold_risk_events(db.get_recent_rss_events(limit=limit, event_type=event_type))}


@app.get("/api/settings")
async def get_settings() -> dict:
    return db.get_settings()


@app.put("/api/settings")
async def update_settings(payload: SettingsUpdate) -> dict:
    updated = db.update_settings(payload.model_dump(exclude_unset=True))
    reschedule_monitor_job()
    reschedule_rss_job()
    reschedule_us10y_job()
    return updated


@app.put("/api/reversal/settings")
async def update_reversal_settings(payload: ReversalSettingsUpdate) -> dict:
    updated = db.update_settings(payload.model_dump(exclude_unset=True))
    reschedule_monitor_job()
    reschedule_rss_job()
    reschedule_us10y_job()
    return updated


@app.get("/api/rss-ml/status")
async def get_rss_ml_status() -> dict:
    return rss_ml_service.get_status()


@app.put("/api/rss-ml/config")
async def update_rss_ml_config(payload: RssMlConfigUpdate) -> dict:
    db.update_settings(payload.model_dump(exclude_unset=True))
    config = rss_ml_service.reload_runtime_config()
    return {"config": config}


@app.post("/api/rss-ml/train")
async def run_rss_ml_train(payload: RssMlTrainPayload) -> dict:
    started = rss_ml_service.start_training_async(
        force=payload.force,
        min_samples_override=payload.min_samples_override,
    )
    return {
        **started,
        "status": rss_ml_service.get_status(),
    }


@app.get("/api/rss-ml/train-status")
async def get_rss_ml_train_status() -> dict:
    return {
        "runtime": rss_ml_service.get_live_status(),
        "status": rss_ml_service.get_status(),
    }


@app.post("/api/rss-ml/train-control")
async def control_rss_ml_train(payload: RssMlTrainControlPayload) -> dict:
    result = rss_ml_service.control_training(payload.action)
    return {
        **result,
        "runtime": rss_ml_service.get_live_status(),
    }


@app.post("/api/rss-ml/clear-samples")
async def clear_rss_ml_samples(payload: RssMlClearPayload) -> dict:
    result = rss_ml_service.clear_samples(remove_model_file=payload.remove_model_file)
    if result.get("error") == "training_running":
        raise HTTPException(status_code=409, detail="训练进行中，不能清空样本")
    return {
        "ok": True,
        "result": result,
        "status": rss_ml_service.get_status(),
    }


@app.post("/api/rss-ml/sync-csv")
async def sync_rss_ml_csv(payload: RssMlCsvSyncPayload) -> dict:
    result = rss_ml_service.sync_csv_from_db(overwrite=payload.overwrite)
    if not result.get("ok", True):
        raise HTTPException(status_code=409, detail=result.get("message") or "CSV 同步失败")
    return {
        "ok": True,
        "result": result,
        "status": rss_ml_service.get_status(),
    }


@app.post("/api/reversal/rss-bulk-fill")
async def run_reversal_rss_bulk_fill(payload: RssBulkFillPayload) -> dict:
    settings = db.get_settings()
    feed_urls = [
        str(item.get("url", "")).strip()
        for item in settings.get("rss_feeds", [])
        if item.get("enabled") and str(item.get("url", "")).strip()
    ]
    if not feed_urls:
        feed_urls = list(settings.get("rss_feed_urls", []))
    if payload.use_extended_sources:
        for url in EXTENDED_TEST_RSS_FEEDS:
            if url not in feed_urls:
                feed_urls.append(url)
    if not feed_urls:
        raise HTTPException(status_code=400, detail="没有可用的启用RSS源，请先在RSS源配置里启用至少一个源")

    runs: list[dict] = []
    total_items = 0
    total_errors = 0
    for _ in range(payload.rounds):
        result = await reversal_monitor.run_rss_cycle(
            force_refresh=True,
            include_unmatched=payload.include_unmatched,
            feed_urls_override=feed_urls,
            full_store=True,
        )
        runs.append(result)
        total_items += int(result.get("matched_events") or 0)
        total_errors += len(result.get("rss_errors") or [])

    return {
        "rounds": payload.rounds,
        "total_items": total_items,
        "total_errors": total_errors,
        "used_feed_count": len(feed_urls),
        "runs": runs,
    }


@app.post("/api/reversal/rss-dedup")
async def run_reversal_rss_dedup() -> dict:
    dedup_result = db.deduplicate_rss_events_by_semantic_key()
    csv_sync = rss_ml_service.sync_csv_from_db(overwrite=False)
    return {
        "ok": True,
        "dedup": dedup_result,
        "csv_sync": csv_sync,
        "status": rss_ml_service.get_status(),
    }


@app.post("/api/reversal/run-once")
async def run_reversal_once() -> dict:
    try:
        result = await reversal_monitor.run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result.__dict__


@app.post("/api/us10y/run-once")
async def run_us10y_once() -> dict:
    try:
        result = await us10y_monitor.run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result.__dict__


@app.post("/api/reversal/rss-run-once")
async def run_reversal_rss_once() -> dict:
    try:
        return await reversal_monitor.run_rss_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/reversal/test-alert")
async def send_reversal_test_alert(payload: TestAlertPayload) -> dict:
    success, response_text = await reversal_monitor.send_test_alert(level=payload.level, note=payload.note)
    return {
        "success": success,
        "response_text": response_text,
    }


@app.post("/api/run-once")
async def run_once() -> dict:
    try:
        result = await run_all_monitors_with_rss()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result
