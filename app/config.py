from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "monitor.db"


def _split_csv_env(name: str) -> tuple[str, ...]:
    raw_value = os.getenv(name, "")
    if not raw_value.strip():
        return ()
    return tuple(item.strip() for item in raw_value.split(",") if item.strip())


@dataclass(frozen=True)
class DefaultSettings:
    dingtalk_webhook: str = os.getenv("DINGTALK_WEBHOOK", "")
    dingtalk_secret: str = os.getenv("DINGTALK_SECRET", "")
    dingtalk_at_user_ids: tuple[str, ...] = _split_csv_env("DINGTALK_AT_USER_IDS")
    premium_threshold: float = float(os.getenv("PREMIUM_THRESHOLD", "20"))
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    alert_cooldown_seconds: int = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
    reversal_cooldown_seconds: int = int(os.getenv("REVERSAL_COOLDOWN_SECONDS", "1800"))
    reversal_price_lookback_minutes: int = int(os.getenv("REVERSAL_PRICE_LOOKBACK_MINUTES", "360"))
    reversal_price_rebound_pct: float = float(os.getenv("REVERSAL_PRICE_REBOUND_PCT", "1.2"))
    reversal_price_ma_window: int = int(os.getenv("REVERSAL_PRICE_MA_WINDOW", "15"))
    reversal_signal_window_minutes: int = int(os.getenv("REVERSAL_SIGNAL_WINDOW_MINUTES", "180"))
    us10y_poll_interval_seconds: int = int(os.getenv("US10Y_POLL_INTERVAL_SECONDS", "60"))
    us10y_drop_lookback_hours: float = float(os.getenv("US10Y_DROP_LOOKBACK_HOURS", "24"))
    us10y_drop_threshold_bp: float = float(os.getenv("US10Y_DROP_THRESHOLD_BP", "1.0"))
    us10y_alert_cooldown_seconds: int = int(os.getenv("US10Y_ALERT_COOLDOWN_SECONDS", "1800"))
    us10y_alert_dedup_hours: int = int(os.getenv("US10Y_ALERT_DEDUP_HOURS", "4"))
    us10y_tenors: tuple[str, ...] = ("10y",)
    rss_poll_interval_seconds: int = int(os.getenv("RSS_POLL_INTERVAL_SECONDS", "3600"))
    rss_ml_learning_rate: float = float(os.getenv("RSS_ML_LEARNING_RATE", "0.001"))
    rss_ml_max_epochs: int = int(os.getenv("RSS_ML_MAX_EPOCHS", "300"))
    rss_ml_early_stop_patience: int = int(os.getenv("RSS_ML_EARLY_STOP_PATIENCE", "25"))
    rss_ml_train_step_size: int = int(os.getenv("RSS_ML_TRAIN_STEP_SIZE", "100"))
    rss_ml_min_train_samples: int = int(os.getenv("RSS_ML_MIN_TRAIN_SAMPLES", "30"))
    rss_ml_active_window_hours: int = int(os.getenv("RSS_ML_ACTIVE_WINDOW_HOURS", "24"))
    rss_ml_weak_move_pct: float = float(os.getenv("RSS_ML_WEAK_MOVE_PCT", "0.10"))
    rss_ml_strong_move_pct: float = float(os.getenv("RSS_ML_STRONG_MOVE_PCT", "0.35"))
    rss_ml_decay_half_life_hours: float = float(os.getenv("RSS_ML_DECAY_HALF_LIFE_HOURS", "24"))
    rss_ml_label_mode: str = os.getenv("RSS_ML_LABEL_MODE", "future_return")
    rss_feed_urls: tuple[str, ...] = (
        "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml",
        "https://www.aljazeera.com/xml/rss/all.xml",
        "https://news.google.com/rss/search?q=(gold+OR+bullion)+(Hormuz+OR+Iran+OR+ceasefire+OR+shipping)&hl=en-US&gl=US&ceid=US:en",
    )


DEFAULT_SETTINGS = DefaultSettings()
TROY_OUNCE_TO_GRAMS = 31.1034768
SHANGHAI_TZ = "Asia/Shanghai"
NEW_YORK_TZ = "America/New_York"
SINA_QUOTE_URL = "https://hq.sinajs.cn/list=nf_AU0,gds_AU9999,gds_AUTD,hf_XAU,hf_GC,USDCNY"
