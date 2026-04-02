from __future__ import annotations

import json
import re
import sqlite3
from difflib import SequenceMatcher
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from .config import DATA_DIR, DB_PATH, DEFAULT_SETTINGS


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class Database:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    def initialize(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS quote_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    shfe_price_cny_per_g REAL,
                    london_price_usd_per_oz REAL,
                    usdcny_rate REAL,
                    london_price_cny_per_g REAL,
                    premium_cny_per_g REAL,
                    poll_interval_seconds INTEGER NOT NULL,
                    both_markets_open INTEGER NOT NULL,
                    shfe_market_open INTEGER NOT NULL,
                    london_market_open INTEGER NOT NULL,
                    alert_triggered INTEGER NOT NULL DEFAULT 0,
                    raw_payload TEXT,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_id INTEGER,
                    sent_at TEXT NOT NULL,
                    premium_cny_per_g REAL NOT NULL,
                    threshold_cny_per_g REAL NOT NULL,
                    success INTEGER NOT NULL,
                    response_text TEXT,
                    webhook_url TEXT,
                    FOREIGN KEY(sample_id) REFERENCES quote_samples(id)
                );

                CREATE TABLE IF NOT EXISTS fetch_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    poll_interval_seconds INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS reversal_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    gold_price_usd_per_oz REAL NOT NULL,
                    usdcny_rate REAL NOT NULL,
                    price_signal INTEGER NOT NULL,
                    political_signal INTEGER NOT NULL,
                    war_signal INTEGER NOT NULL,
                    us10y_signal INTEGER NOT NULL DEFAULT 0,
                    signal_level INTEGER NOT NULL,
                    triggered_conditions TEXT NOT NULL,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS rss_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    published_at TEXT,
                    source TEXT NOT NULL,
                    feed_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    link TEXT,
                    summary TEXT,
                    event_type TEXT NOT NULL,
                    matched_keywords TEXT,
                    content_hash TEXT NOT NULL UNIQUE,
                    semantic_key TEXT,
                    impact_score INTEGER,
                    impact_level TEXT,
                    impact_note TEXT,
                    event_gold_price_usd_per_oz REAL,
                    event_gold_change_pct REAL,
                    ml_score REAL,
                    ml_model_version TEXT,
                    ml_scored_at TEXT,
                    ml_bucket_label TEXT,
                    ml_class_probs TEXT
                );

                CREATE TABLE IF NOT EXISTS rss_ml_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL UNIQUE,
                    event_text TEXT NOT NULL,
                    gold_price_usd_per_oz REAL,
                    gold_change_pct REAL,
                    target_score INTEGER NOT NULL,
                    predicted_score REAL,
                    model_version TEXT,
                    created_at TEXT NOT NULL,
                    scored_at TEXT,
                    FOREIGN KEY(event_id) REFERENCES rss_events(id)
                );

                CREATE TABLE IF NOT EXISTS rss_ml_training_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trained_at TEXT NOT NULL,
                    sample_count INTEGER NOT NULL,
                    model_version TEXT NOT NULL,
                    learning_rate REAL NOT NULL,
                    max_epochs INTEGER NOT NULL,
                    early_stop_patience INTEGER NOT NULL,
                    train_loss REAL,
                    val_loss REAL,
                    train_accuracy REAL,
                    val_accuracy REAL,
                    best_epoch INTEGER NOT NULL,
                    notes TEXT,
                    epoch_history_json TEXT
                );

                CREATE TABLE IF NOT EXISTS reversal_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_id INTEGER,
                    sent_at TEXT NOT NULL,
                    signal_level INTEGER NOT NULL,
                    triggered_conditions TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    response_text TEXT,
                    webhook_url TEXT,
                    FOREIGN KEY(sample_id) REFERENCES reversal_samples(id)
                );

                CREATE TABLE IF NOT EXISTS reversal_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    poll_interval_seconds INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    rss_error_count INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS us10y_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    tenor TEXT NOT NULL DEFAULT '10y',
                    yield_pct REAL NOT NULL,
                    yield_signal INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS us10y_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    tenor TEXT NOT NULL DEFAULT 'all',
                    success INTEGER NOT NULL,
                    poll_interval_seconds INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS us10y_alert_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sent_at TEXT NOT NULL,
                    tenors TEXT NOT NULL,
                    lookback_hours REAL NOT NULL,
                    threshold_bp REAL NOT NULL,
                    drop_bp_values TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    response_text TEXT
                );

                CREATE TABLE IF NOT EXISTS rss_fetch_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fetched_at TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    item_count INTEGER NOT NULL DEFAULT 0,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS notification_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sent_at TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    target_name TEXT,
                    webhook_url TEXT,
                    success INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    response_text TEXT
                );
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(reversal_samples)").fetchall()
            }
            if "us10y_signal" not in columns:
                conn.execute(
                    "ALTER TABLE reversal_samples ADD COLUMN us10y_signal INTEGER NOT NULL DEFAULT 0"
                )
            us10y_sample_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(us10y_samples)").fetchall()
            }
            if "tenor" not in us10y_sample_columns:
                conn.execute(
                    "ALTER TABLE us10y_samples ADD COLUMN tenor TEXT NOT NULL DEFAULT '10y'"
                )
            us10y_run_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(us10y_runs)").fetchall()
            }
            if "tenor" not in us10y_run_columns:
                conn.execute(
                    "ALTER TABLE us10y_runs ADD COLUMN tenor TEXT NOT NULL DEFAULT 'all'"
                )
            rss_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(rss_events)").fetchall()
            }
            if "impact_score" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN impact_score INTEGER")
            if "impact_level" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN impact_level TEXT")
            if "impact_note" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN impact_note TEXT")
            if "event_gold_price_usd_per_oz" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN event_gold_price_usd_per_oz REAL")
            if "event_gold_change_pct" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN event_gold_change_pct REAL")
            if "ml_score" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN ml_score REAL")
            if "ml_model_version" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN ml_model_version TEXT")
            if "ml_scored_at" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN ml_scored_at TEXT")
            if "ml_bucket_label" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN ml_bucket_label TEXT")
            if "ml_class_probs" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN ml_class_probs TEXT")
            if "semantic_key" not in rss_columns:
                conn.execute("ALTER TABLE rss_events ADD COLUMN semantic_key TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rss_events_semantic_key ON rss_events(semantic_key)")
            ml_run_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(rss_ml_training_runs)").fetchall()
            }
            if "epoch_history_json" not in ml_run_columns:
                conn.execute("ALTER TABLE rss_ml_training_runs ADD COLUMN epoch_history_json TEXT")
            if "train_accuracy" not in ml_run_columns:
                conn.execute("ALTER TABLE rss_ml_training_runs ADD COLUMN train_accuracy REAL")
            if "val_accuracy" not in ml_run_columns:
                conn.execute("ALTER TABLE rss_ml_training_runs ADD COLUMN val_accuracy REAL")

        defaults = {
            "dingtalk_webhook": DEFAULT_SETTINGS.dingtalk_webhook,
            "dingtalk_secret": DEFAULT_SETTINGS.dingtalk_secret,
            "dingtalk_at_user_ids": json.dumps(list(DEFAULT_SETTINGS.dingtalk_at_user_ids), ensure_ascii=False),
            "notification_targets": json.dumps([], ensure_ascii=False),
            "premium_threshold": str(DEFAULT_SETTINGS.premium_threshold),
            "poll_interval_seconds": str(DEFAULT_SETTINGS.poll_interval_seconds),
            "alert_cooldown_seconds": str(DEFAULT_SETTINGS.alert_cooldown_seconds),
            "request_timeout_seconds": str(DEFAULT_SETTINGS.request_timeout_seconds),
            "reversal_cooldown_seconds": str(DEFAULT_SETTINGS.reversal_cooldown_seconds),
            "reversal_price_lookback_minutes": str(DEFAULT_SETTINGS.reversal_price_lookback_minutes),
            "reversal_price_rebound_pct": str(DEFAULT_SETTINGS.reversal_price_rebound_pct),
            "reversal_price_ma_window": str(DEFAULT_SETTINGS.reversal_price_ma_window),
            "reversal_signal_window_minutes": str(DEFAULT_SETTINGS.reversal_signal_window_minutes),
            "us10y_poll_interval_seconds": str(DEFAULT_SETTINGS.us10y_poll_interval_seconds),
            "us10y_drop_lookback_hours": str(DEFAULT_SETTINGS.us10y_drop_lookback_hours),
            "us10y_drop_threshold_bp": str(DEFAULT_SETTINGS.us10y_drop_threshold_bp),
            "us10y_alert_cooldown_seconds": str(DEFAULT_SETTINGS.us10y_alert_cooldown_seconds),
            "us10y_alert_dedup_hours": str(DEFAULT_SETTINGS.us10y_alert_dedup_hours),
            "us10y_tenors": json.dumps(list(DEFAULT_SETTINGS.us10y_tenors), ensure_ascii=False),
            "rss_poll_interval_seconds": str(DEFAULT_SETTINGS.rss_poll_interval_seconds),
            "rss_ml_learning_rate": str(DEFAULT_SETTINGS.rss_ml_learning_rate),
            "rss_ml_max_epochs": str(DEFAULT_SETTINGS.rss_ml_max_epochs),
            "rss_ml_early_stop_patience": str(DEFAULT_SETTINGS.rss_ml_early_stop_patience),
            "rss_ml_train_step_size": str(DEFAULT_SETTINGS.rss_ml_train_step_size),
            "rss_ml_min_train_samples": str(DEFAULT_SETTINGS.rss_ml_min_train_samples),
            "rss_ml_active_window_hours": str(DEFAULT_SETTINGS.rss_ml_active_window_hours),
            "rss_ml_weak_move_pct": str(DEFAULT_SETTINGS.rss_ml_weak_move_pct),
            "rss_ml_strong_move_pct": str(DEFAULT_SETTINGS.rss_ml_strong_move_pct),
            "rss_ml_decay_half_life_hours": str(DEFAULT_SETTINGS.rss_ml_decay_half_life_hours),
            "rss_ml_label_mode": str(DEFAULT_SETTINGS.rss_ml_label_mode),
            "rss_feed_urls": json.dumps(list(DEFAULT_SETTINGS.rss_feed_urls), ensure_ascii=False),
            "rss_feeds": json.dumps(
                [
                    {"name": f"RSS源{i+1}", "url": url, "enabled": True}
                    for i, url in enumerate(DEFAULT_SETTINGS.rss_feed_urls)
                ],
                ensure_ascii=False,
            ),
        }
        with self.connection() as conn:
            for key, value in defaults.items():
                conn.execute(
                    "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                    (key, value),
                )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = _dict_factory
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def get_settings(self) -> dict[str, Any]:
        with self.connection() as conn:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
        settings = {row["key"]: row["value"] for row in rows}
        at_user_ids = self._parse_json_list(settings.get("dingtalk_at_user_ids", "[]"))
        rss_feed_urls = self._parse_json_list(settings.get("rss_feed_urls", "[]"))
        rss_feeds = self._parse_rss_feeds(settings.get("rss_feeds", "[]"))
        if not rss_feed_urls and not rss_feeds:
            rss_feed_urls = list(DEFAULT_SETTINGS.rss_feed_urls)
        if not rss_feeds and rss_feed_urls:
            rss_feeds = [
                {"name": f"RSS源{i+1}", "url": url, "enabled": True}
                for i, url in enumerate(rss_feed_urls)
            ]
        enabled_rss_feed_urls = [
            item["url"] for item in rss_feeds if item.get("enabled") and item.get("url")
        ]
        if not rss_feeds:
            enabled_rss_feed_urls = rss_feed_urls
        us10y_tenors = self._parse_json_list(settings.get("us10y_tenors", '["10y"]')) or ["10y"]
        notification_targets = self._parse_notification_targets(settings.get("notification_targets", "[]"))
        if not notification_targets and settings.get("dingtalk_webhook", "").strip():
            notification_targets = [
                {
                    "name": "默认机器人",
                    "webhook": settings.get("dingtalk_webhook", "").strip(),
                    "secret": settings.get("dingtalk_secret", "").strip(),
                    "enabled": True,
                }
            ]
        return {
            "dingtalk_webhook": settings.get("dingtalk_webhook", ""),
            "dingtalk_secret": settings.get("dingtalk_secret", ""),
            "dingtalk_at_user_ids": at_user_ids,
            "notification_targets": notification_targets,
            "premium_threshold": float(settings.get("premium_threshold", "20")),
            "poll_interval_seconds": int(settings.get("poll_interval_seconds", "60")),
            "alert_cooldown_seconds": int(settings.get("alert_cooldown_seconds", "900")),
            "request_timeout_seconds": float(settings.get("request_timeout_seconds", "10")),
            "reversal_cooldown_seconds": int(settings.get("reversal_cooldown_seconds", "1800")),
            "reversal_price_lookback_minutes": int(settings.get("reversal_price_lookback_minutes", "360")),
            "reversal_price_rebound_pct": float(settings.get("reversal_price_rebound_pct", "1.2")),
            "reversal_price_ma_window": int(settings.get("reversal_price_ma_window", "15")),
            "reversal_signal_window_minutes": int(settings.get("reversal_signal_window_minutes", "180")),
            "us10y_poll_interval_seconds": int(settings.get("us10y_poll_interval_seconds", "60")),
            "us10y_drop_lookback_hours": float(settings.get("us10y_drop_lookback_hours", "24")),
            "us10y_drop_threshold_bp": float(settings.get("us10y_drop_threshold_bp", "1.0")),
            "us10y_alert_cooldown_seconds": int(
                settings.get("us10y_alert_cooldown_seconds", "1800")
            ),
            "us10y_alert_dedup_hours": int(settings.get("us10y_alert_dedup_hours", "4")),
            "us10y_tenors": [item for item in us10y_tenors if item in {"5y", "10y", "20y"}] or ["10y"],
            "rss_poll_interval_seconds": int(settings.get("rss_poll_interval_seconds", "3600")),
            "rss_ml_learning_rate": float(settings.get("rss_ml_learning_rate", "0.001")),
            "rss_ml_max_epochs": int(settings.get("rss_ml_max_epochs", "300")),
            "rss_ml_early_stop_patience": int(settings.get("rss_ml_early_stop_patience", "25")),
            "rss_ml_train_step_size": int(settings.get("rss_ml_train_step_size", "100")),
            "rss_ml_min_train_samples": int(settings.get("rss_ml_min_train_samples", "30")),
            "rss_ml_active_window_hours": int(settings.get("rss_ml_active_window_hours", "24")),
            "rss_ml_weak_move_pct": float(settings.get("rss_ml_weak_move_pct", "0.10")),
            "rss_ml_strong_move_pct": float(settings.get("rss_ml_strong_move_pct", "0.35")),
            "rss_ml_decay_half_life_hours": float(settings.get("rss_ml_decay_half_life_hours", "24")),
            "rss_ml_label_mode": str(settings.get("rss_ml_label_mode", "future_return")).strip() or "future_return",
            "rss_feeds": rss_feeds,
            "rss_feed_urls": enabled_rss_feed_urls,
        }

    def update_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as conn:
            for key, value in payload.items():
                serialized_value = self._serialize_setting_value(key, value)
                conn.execute(
                    """
                    INSERT INTO settings(key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value
                    """,
                    (key, serialized_value),
                )
        return self.get_settings()

    @staticmethod
    def _parse_json_list(raw_value: str) -> list[str]:
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [str(item).strip() for item in parsed if str(item).strip()]

    @staticmethod
    def _serialize_setting_value(key: str, value: Any) -> str:
        if key in {"dingtalk_at_user_ids", "rss_feed_urls", "us10y_tenors"}:
            if isinstance(value, str):
                items = [item.strip() for item in value.splitlines() if item.strip()]
            else:
                items = [str(item).strip() for item in value if str(item).strip()]
            if key == "us10y_tenors":
                valid = []
                for item in items:
                    item_lower = item.lower()
                    if item_lower in {"5y", "10y", "20y"} and item_lower not in valid:
                        valid.append(item_lower)
                items = valid or ["10y"]
            return json.dumps(items, ensure_ascii=False)
        if key == "rss_feeds":
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    parsed = []
            else:
                parsed = value
            items: list[dict[str, Any]] = []
            if isinstance(parsed, list):
                for i, item in enumerate(parsed):
                    if not isinstance(item, dict):
                        continue
                    url = str(item.get("url", "")).strip()
                    if not url:
                        continue
                    items.append(
                        {
                            "name": str(item.get("name", "")).strip() or f"RSS源{i+1}",
                            "url": url,
                            "enabled": bool(item.get("enabled", True)),
                        }
                    )
            return json.dumps(items, ensure_ascii=False)
        if key == "notification_targets":
            items: list[dict[str, Any]] = []
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    parsed = []
            else:
                parsed = value
            if isinstance(parsed, list):
                for item in parsed:
                    if not isinstance(item, dict):
                        continue
                    webhook = str(item.get("webhook", "")).strip()
                    if not webhook:
                        continue
                    items.append(
                        {
                            "name": str(item.get("name", "")).strip() or "默认机器人",
                            "webhook": webhook,
                            "secret": str(item.get("secret", "")).strip(),
                            "enabled": bool(item.get("enabled", True)),
                        }
                    )
            return json.dumps(items, ensure_ascii=False)
        return str(value)

    @staticmethod
    def _parse_notification_targets(raw_value: str) -> list[dict[str, Any]]:
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        items: list[dict[str, Any]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            webhook = str(item.get("webhook", "")).strip()
            if not webhook:
                continue
            items.append(
                {
                    "name": str(item.get("name", "")).strip() or "默认机器人",
                    "webhook": webhook,
                    "secret": str(item.get("secret", "")).strip(),
                    "enabled": bool(item.get("enabled", True)),
                }
            )
        return items

    @staticmethod
    def _parse_rss_feeds(raw_value: str) -> list[dict[str, Any]]:
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        items: list[dict[str, Any]] = []
        for i, item in enumerate(parsed):
            if not isinstance(item, dict):
                continue
            url = str(item.get("url", "")).strip()
            if not url:
                continue
            items.append(
                {
                    "name": str(item.get("name", "")).strip() or f"RSS源{i+1}",
                    "url": url,
                    "enabled": bool(item.get("enabled", True)),
                }
            )
        return items

    def insert_sample(self, payload: dict[str, Any]) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO quote_samples (
                    fetched_at,
                    shfe_price_cny_per_g,
                    london_price_usd_per_oz,
                    usdcny_rate,
                    london_price_cny_per_g,
                    premium_cny_per_g,
                    poll_interval_seconds,
                    both_markets_open,
                    shfe_market_open,
                    london_market_open,
                    alert_triggered,
                    raw_payload,
                    note
                ) VALUES (
                    :fetched_at,
                    :shfe_price_cny_per_g,
                    :london_price_usd_per_oz,
                    :usdcny_rate,
                    :london_price_cny_per_g,
                    :premium_cny_per_g,
                    :poll_interval_seconds,
                    :both_markets_open,
                    :shfe_market_open,
                    :london_market_open,
                    :alert_triggered,
                    :raw_payload,
                    :note
                )
                """,
                payload,
            )
            return int(cursor.lastrowid)

    def set_sample_alert_triggered(self, sample_id: int) -> None:
        with self.connection() as conn:
            conn.execute(
                "UPDATE quote_samples SET alert_triggered=1 WHERE id=?",
                (sample_id,),
            )

    def insert_alert_event(self, payload: dict[str, Any]) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO alert_events (
                    sample_id,
                    sent_at,
                    premium_cny_per_g,
                    threshold_cny_per_g,
                    success,
                    response_text,
                    webhook_url
                ) VALUES (
                    :sample_id,
                    :sent_at,
                    :premium_cny_per_g,
                    :threshold_cny_per_g,
                    :success,
                    :response_text,
                    :webhook_url
                )
                """,
                payload,
            )
            return int(cursor.lastrowid)

    def insert_fetch_run(
        self,
        *,
        fetched_at: datetime,
        success: bool,
        poll_interval_seconds: int,
        duration_ms: int,
        error_message: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO fetch_runs (
                    fetched_at,
                    success,
                    poll_interval_seconds,
                    duration_ms,
                    error_message
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    fetched_at.isoformat(),
                    int(success),
                    poll_interval_seconds,
                    duration_ms,
                    error_message,
                ),
            )

    def get_latest_sample(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM quote_samples ORDER BY fetched_at DESC LIMIT 1"
            ).fetchone()
        return row

    def get_latest_effective_premium_sample(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM quote_samples
                WHERE premium_cny_per_g IS NOT NULL
                ORDER BY fetched_at DESC
                LIMIT 1
                """
            ).fetchone()
        return row

    def get_history(self, since_iso: str, limit: int = 2000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM quote_samples
                WHERE fetched_at >= ?
                ORDER BY fetched_at ASC
                LIMIT ?
                """,
                (since_iso, limit),
            ).fetchall()
        return rows

    def get_recent_samples(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM quote_samples ORDER BY fetched_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def get_recent_alerts(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM alert_events ORDER BY sent_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def get_recent_fetch_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM fetch_runs ORDER BY fetched_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def get_last_successful_alert(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM alert_events
                WHERE success=1
                ORDER BY sent_at DESC
                LIMIT 1
                """
            ).fetchone()
        return row

    def insert_reversal_sample(self, payload: dict[str, Any]) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO reversal_samples (
                    fetched_at,
                    gold_price_usd_per_oz,
                    usdcny_rate,
                    price_signal,
                    political_signal,
                    war_signal,
                    us10y_signal,
                    signal_level,
                    triggered_conditions,
                    note
                ) VALUES (
                    :fetched_at,
                    :gold_price_usd_per_oz,
                    :usdcny_rate,
                    :price_signal,
                    :political_signal,
                    :war_signal,
                    :us10y_signal,
                    :signal_level,
                    :triggered_conditions,
                    :note
                )
                """,
                payload,
            )
            return int(cursor.lastrowid)

    def get_latest_reversal_sample(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM reversal_samples ORDER BY fetched_at DESC LIMIT 1"
            ).fetchone()
        return row

    def get_reversal_history(self, since_iso: str, limit: int = 2000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM (
                    SELECT * FROM reversal_samples
                    WHERE fetched_at >= ?
                    ORDER BY fetched_at DESC
                    LIMIT ?
                )
                ORDER BY fetched_at ASC
                """,
                (since_iso, limit),
            ).fetchall()
        return rows

    def get_recent_reversal_samples(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM reversal_samples ORDER BY fetched_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def get_reversal_samples_since(self, since_iso: str, limit: int = 1000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM reversal_samples
                WHERE fetched_at >= ?
                ORDER BY fetched_at ASC
                LIMIT ?
                """,
                (since_iso, limit),
            ).fetchall()
        return rows

    def insert_rss_event(self, payload: dict[str, Any]) -> int | None:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO rss_events (
                    fetched_at,
                    published_at,
                    source,
                    feed_url,
                    title,
                    link,
                    summary,
                    event_type,
                    matched_keywords,
                    content_hash,
                    semantic_key,
                    impact_score,
                    impact_level,
                    impact_note,
                    event_gold_price_usd_per_oz,
                    event_gold_change_pct,
                    ml_score,
                    ml_model_version,
                    ml_scored_at
                ) VALUES (
                    :fetched_at,
                    :published_at,
                    :source,
                    :feed_url,
                    :title,
                    :link,
                    :summary,
                    :event_type,
                    :matched_keywords,
                    :content_hash,
                    :semantic_key,
                    :impact_score,
                    :impact_level,
                    :impact_note,
                    :event_gold_price_usd_per_oz,
                    :event_gold_change_pct,
                    :ml_score,
                    :ml_model_version,
                    :ml_scored_at
                )
                """,
                {
                    **payload,
                    "semantic_key": payload.get("semantic_key"),
                    "impact_score": payload.get("impact_score"),
                    "impact_level": payload.get("impact_level"),
                    "impact_note": payload.get("impact_note"),
                    "event_gold_price_usd_per_oz": payload.get("event_gold_price_usd_per_oz"),
                    "event_gold_change_pct": payload.get("event_gold_change_pct"),
                    "ml_score": payload.get("ml_score"),
                    "ml_model_version": payload.get("ml_model_version"),
                    "ml_scored_at": payload.get("ml_scored_at"),
                },
            )
            if cursor.rowcount <= 0:
                return None
            return int(cursor.lastrowid)

    def get_recent_rss_events(
        self,
        *,
        limit: int = 20,
        event_type: str | None = None,
        since_iso: str | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM rss_events"
        params: list[Any] = []
        where_clauses: list[str] = []
        if event_type:
            where_clauses.append("event_type = ?")
            params.append(event_type)
        if since_iso:
            where_clauses.append("fetched_at >= ?")
            params.append(since_iso)
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        query += " ORDER BY COALESCE(published_at, fetched_at) DESC LIMIT ?"
        params.append(limit)
        with self.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return rows

    def get_rss_events_for_export(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        query = """
            SELECT
                id,
                fetched_at,
                published_at,
                source,
                feed_url,
                title,
                link,
                summary,
                event_type,
                matched_keywords,
                impact_score,
                impact_level,
                impact_note,
                ml_score,
                ml_bucket_label,
                ml_model_version,
                ml_scored_at,
                ml_class_probs
            FROM rss_events
            ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
        """
        params: list[Any] = []
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        with self.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return rows

    def get_rss_events_without_ml_score(self, *, limit: int = 2000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, title, summary
                FROM rss_events
                WHERE ml_score IS NULL
                ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return rows

    def get_rss_event_count(self) -> int:
        with self.connection() as conn:
            row = conn.execute("SELECT COUNT(1) AS total FROM rss_events").fetchone()
        return int(row["total"]) if row else 0

    def get_rss_scored_event_count(self) -> int:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(1) AS total FROM rss_events WHERE ml_score IS NOT NULL"
            ).fetchone()
        return int(row["total"]) if row else 0

    def get_rss_event_by_semantic_key(self, semantic_key: str) -> dict[str, Any] | None:
        if not semantic_key:
            return None
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM rss_events
                WHERE semantic_key = ?
                ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                LIMIT 1
                """,
                (semantic_key,),
            ).fetchone()
        return row

    @staticmethod
    def _normalize_title_for_fuzzy_dedup(text: str) -> str:
        normalized = str(text or "").lower()
        # Normalize common wording differences that often describe the same headline event.
        replacements = {
            "船员称": "船只称",
            "船员": "船只",
            "液化天然气船": "油轮",
            "液化天然气轮": "油轮",
            "lng船": "油轮",
            "lng轮": "油轮",
        }
        for src, dst in replacements.items():
            normalized = normalized.replace(src, dst)
        normalized = re.sub(r"\s+", "", normalized)
        normalized = re.sub(r"[^\w\u4e00-\u9fff]", "", normalized)
        return normalized

    @staticmethod
    def _char_bigrams(text: str) -> set[str]:
        if len(text) < 2:
            return {text} if text else set()
        return {text[i : i + 2] for i in range(len(text) - 1)}

    @classmethod
    def _is_fuzzy_duplicate_title(cls, left: str, right: str) -> bool:
        if not left or not right:
            return False
        if left == right:
            return True
        max_len = max(len(left), len(right))
        min_len = min(len(left), len(right))
        if max_len <= 0:
            return False
        if (max_len - min_len) / max_len > 0.40:
            return False
        ratio = SequenceMatcher(None, left, right).ratio()
        if ratio >= 0.90:
            return True
        if min_len >= 18 and (left in right or right in left):
            return True
        left_bigrams = cls._char_bigrams(left)
        right_bigrams = cls._char_bigrams(right)
        if not left_bigrams or not right_bigrams:
            return False
        inter = len(left_bigrams & right_bigrams)
        union = len(left_bigrams | right_bigrams) or 1
        jaccard = inter / union
        return jaccard >= 0.82

    def deduplicate_rss_events_by_semantic_key(self) -> dict[str, Any]:
        removed_events = 0
        removed_samples = 0
        groups = 0
        exact_removed_events = 0
        fuzzy_removed_events = 0
        removed_by_source: dict[str, int] = {}
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT semantic_key, COUNT(1) AS cnt
                FROM rss_events
                WHERE semantic_key IS NOT NULL AND semantic_key != ''
                GROUP BY semantic_key
                HAVING COUNT(1) > 1
                """
            ).fetchall()
            for row in rows:
                key = str(row.get("semantic_key") or "").strip()
                if not key:
                    continue
                ids_rows = conn.execute(
                    """
                    SELECT id
                    FROM rss_events
                    WHERE semantic_key = ?
                    ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                    """,
                    (key,),
                ).fetchall()
                ids = [int(item["id"]) for item in ids_rows]
                if len(ids) <= 1:
                    continue
                keep_id = ids[0]
                drop_ids = ids[1:]
                groups += 1
                source_rows = conn.execute(
                    f"SELECT source FROM rss_events WHERE id IN ({','.join('?' for _ in drop_ids)})",
                    drop_ids,
                ).fetchall()
                for srow in source_rows:
                    source = str(srow.get("source") or "unknown").strip() or "unknown"
                    removed_by_source[source] = removed_by_source.get(source, 0) + 1
                marks = ",".join("?" for _ in drop_ids)
                sample_deleted = conn.execute(
                    f"DELETE FROM rss_ml_samples WHERE event_id IN ({marks})",
                    drop_ids,
                ).rowcount
                event_deleted = conn.execute(
                    f"DELETE FROM rss_events WHERE id IN ({marks})",
                    drop_ids,
                ).rowcount
                removed_samples += int(sample_deleted or 0)
                removed_events += int(event_deleted or 0)
                exact_removed_events += int(event_deleted or 0)
                # keep score snapshot for the retained record (no-op, explicit for readability)
                _ = keep_id

            # Fuzzy title-level dedup stage: catch semantically equivalent headlines that do
            # not share exactly the same semantic_key.
            fuzzy_rows = conn.execute(
                """
                SELECT id, source, title
                FROM rss_events
                ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                """
            ).fetchall()
            keep_norm_titles: list[str] = []
            drop_ids: list[int] = []
            for row in fuzzy_rows:
                event_id = int(row["id"])
                normalized_title = self._normalize_title_for_fuzzy_dedup(str(row.get("title") or ""))
                if not normalized_title:
                    keep_norm_titles.append("")
                    continue
                is_dup = False
                for kept in keep_norm_titles:
                    if not kept:
                        continue
                    if self._is_fuzzy_duplicate_title(normalized_title, kept):
                        is_dup = True
                        break
                if is_dup:
                    drop_ids.append(event_id)
                    source = str(row.get("source") or "unknown").strip() or "unknown"
                    removed_by_source[source] = removed_by_source.get(source, 0) + 1
                else:
                    keep_norm_titles.append(normalized_title)
            if drop_ids:
                marks = ",".join("?" for _ in drop_ids)
                sample_deleted = conn.execute(
                    f"DELETE FROM rss_ml_samples WHERE event_id IN ({marks})",
                    drop_ids,
                ).rowcount
                event_deleted = conn.execute(
                    f"DELETE FROM rss_events WHERE id IN ({marks})",
                    drop_ids,
                ).rowcount
                removed_samples += int(sample_deleted or 0)
                removed_events += int(event_deleted or 0)
                fuzzy_removed_events += int(event_deleted or 0)

        top_sources = sorted(removed_by_source.items(), key=lambda item: item[1], reverse=True)[:10]
        return {
            "removed_events": removed_events,
            "removed_samples": removed_samples,
            "dedup_groups": groups,
            "exact_removed_events": exact_removed_events,
            "fuzzy_removed_events": fuzzy_removed_events,
            "removed_by_source": removed_by_source,
            "top_duplicate_sources": [{"source": k, "count": v} for k, v in top_sources],
        }

    def update_rss_event_impact(
        self,
        *,
        event_id: int,
        impact_score: int,
        impact_level: str,
        impact_note: str,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE rss_events
                SET impact_score=?, impact_level=?, impact_note=?
                WHERE id=?
                """,
                (impact_score, impact_level, impact_note, event_id),
            )

    def update_rss_event_ml_score(
        self,
        *,
        event_id: int,
        ml_score: float,
        ml_model_version: str,
        ml_scored_at: str,
        ml_bucket_label: str | None = None,
        ml_class_probs: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE rss_events
                SET ml_score=?, ml_model_version=?, ml_scored_at=?, ml_bucket_label=?, ml_class_probs=?
                WHERE id=?
                """,
                (
                    float(ml_score),
                    ml_model_version,
                    ml_scored_at,
                    ml_bucket_label,
                    ml_class_probs,
                    event_id,
                ),
            )

    def get_reversal_price_context(self, at_iso: str) -> tuple[float | None, float | None]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT gold_price_usd_per_oz
                FROM reversal_samples
                WHERE fetched_at <= ?
                ORDER BY fetched_at DESC
                LIMIT 2
                """,
                (at_iso,),
            ).fetchall()
        if not rows:
            return None, None
        latest_price = rows[0].get("gold_price_usd_per_oz")
        previous_price = rows[1].get("gold_price_usd_per_oz") if len(rows) > 1 else None
        if latest_price is None:
            return None, None
        if previous_price in (None, 0):
            return float(latest_price), None
        change_pct = (float(latest_price) - float(previous_price)) / float(previous_price) * 100.0
        return float(latest_price), float(change_pct)

    def insert_rss_ml_sample(
        self,
        *,
        event_id: int,
        event_text: str,
        gold_price_usd_per_oz: float | None,
        gold_change_pct: float | None,
        target_score: int,
        created_at: str,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO rss_ml_samples (
                    event_id,
                    event_text,
                    gold_price_usd_per_oz,
                    gold_change_pct,
                    target_score,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    event_text,
                    gold_price_usd_per_oz,
                    gold_change_pct,
                    int(target_score),
                    created_at,
                ),
            )

    def update_rss_ml_prediction(
        self,
        *,
        event_id: int,
        predicted_score: float,
        model_version: str,
        scored_at: str,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE rss_ml_samples
                SET predicted_score=?, model_version=?, scored_at=?
                WHERE event_id=?
                """,
                (float(predicted_score), model_version, scored_at, event_id),
            )

    def get_rss_ml_sample_count(self) -> int:
        with self.connection() as conn:
            row = conn.execute("SELECT COUNT(1) AS total FROM rss_ml_samples").fetchone()
        return int(row["total"]) if row else 0

    def get_rss_ml_training_rows(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    m.event_id,
                    m.event_text,
                    m.gold_price_usd_per_oz,
                    m.gold_change_pct,
                    m.target_score,
                    m.created_at,
                    e.published_at,
                    e.fetched_at
                FROM rss_ml_samples m
                LEFT JOIN rss_events e ON e.id = m.event_id
                ORDER BY m.created_at ASC, m.id ASC
                """
            ).fetchall()
        return rows

    def get_gold_price_at_or_before(self, at_iso: str) -> float | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT gold_price_usd_per_oz
                FROM reversal_samples
                WHERE fetched_at <= ?
                ORDER BY fetched_at DESC
                LIMIT 1
                """,
                (at_iso,),
            ).fetchone()
        if not row:
            return None
        value = row.get("gold_price_usd_per_oz")
        return float(value) if value is not None else None

    def get_gold_price_at_or_after(self, at_iso: str) -> float | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT gold_price_usd_per_oz
                FROM reversal_samples
                WHERE fetched_at >= ?
                ORDER BY fetched_at ASC
                LIMIT 1
                """,
                (at_iso,),
            ).fetchone()
        if not row:
            return None
        value = row.get("gold_price_usd_per_oz")
        return float(value) if value is not None else None

    def get_rss_events_missing_ml_samples(self, limit: int = 2000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    e.id,
                    e.title,
                    e.summary,
                    e.impact_score,
                    e.event_gold_price_usd_per_oz,
                    e.event_gold_change_pct,
                    e.fetched_at
                FROM rss_events e
                LEFT JOIN rss_ml_samples m ON m.event_id = e.id
                WHERE m.event_id IS NULL AND e.impact_score IS NOT NULL
                ORDER BY e.fetched_at ASC, e.id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return rows

    def insert_rss_ml_training_run(
        self,
        *,
        trained_at: str,
        sample_count: int,
        model_version: str,
        learning_rate: float,
        max_epochs: int,
        early_stop_patience: int,
        train_loss: float | None,
        val_loss: float | None,
        train_accuracy: float | None,
        val_accuracy: float | None,
        best_epoch: int,
        notes: str | None,
        epoch_history_json: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO rss_ml_training_runs (
                    trained_at,
                    sample_count,
                    model_version,
                    learning_rate,
                    max_epochs,
                    early_stop_patience,
                    train_loss,
                    val_loss,
                    train_accuracy,
                    val_accuracy,
                    best_epoch,
                    notes,
                    epoch_history_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trained_at,
                    int(sample_count),
                    model_version,
                    float(learning_rate),
                    int(max_epochs),
                    int(early_stop_patience),
                    train_loss,
                    val_loss,
                    train_accuracy,
                    val_accuracy,
                    int(best_epoch),
                    notes,
                    epoch_history_json,
                ),
            )

    def get_latest_rss_ml_training_run(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM rss_ml_training_runs ORDER BY trained_at DESC, id DESC LIMIT 1"
            ).fetchone()
        return row

    def get_recent_rss_ml_training_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM rss_ml_training_runs ORDER BY trained_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def clear_rss_ml_samples(self) -> dict[str, int]:
        with self.connection() as conn:
            sample_count_row = conn.execute("SELECT COUNT(1) AS total FROM rss_ml_samples").fetchone()
            run_count_row = conn.execute("SELECT COUNT(1) AS total FROM rss_ml_training_runs").fetchone()
            event_count_row = conn.execute(
                """
                SELECT COUNT(1) AS total FROM rss_events
                WHERE ml_score IS NOT NULL OR ml_model_version IS NOT NULL OR ml_scored_at IS NOT NULL
                """
            ).fetchone()
            sample_count = int(sample_count_row["total"]) if sample_count_row else 0
            run_count = int(run_count_row["total"]) if run_count_row else 0
            event_count = int(event_count_row["total"]) if event_count_row else 0
            conn.execute("DELETE FROM rss_ml_samples")
            conn.execute("DELETE FROM rss_ml_training_runs")
            conn.execute(
                """
                UPDATE rss_events
                SET ml_score=NULL, ml_model_version=NULL, ml_scored_at=NULL
                """
            )
        return {
            "deleted_samples": sample_count,
            "deleted_training_runs": run_count,
            "cleared_event_ml_fields": event_count,
        }

    def insert_rss_fetch_run(
        self,
        *,
        fetched_at: datetime,
        success: bool,
        duration_ms: int,
        item_count: int,
        error_count: int,
        error_message: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO rss_fetch_runs (
                    fetched_at,
                    success,
                    duration_ms,
                    item_count,
                    error_count,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    fetched_at.isoformat(),
                    int(success),
                    duration_ms,
                    item_count,
                    error_count,
                    error_message,
                ),
            )

    def get_recent_rss_fetch_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM rss_fetch_runs ORDER BY fetched_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def get_latest_rss_fetch_run(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM rss_fetch_runs ORDER BY fetched_at DESC LIMIT 1"
            ).fetchone()
        return row

    def insert_reversal_alert_event(self, payload: dict[str, Any]) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO reversal_alert_events (
                    sample_id,
                    sent_at,
                    signal_level,
                    triggered_conditions,
                    success,
                    response_text,
                    webhook_url
                ) VALUES (
                    :sample_id,
                    :sent_at,
                    :signal_level,
                    :triggered_conditions,
                    :success,
                    :response_text,
                    :webhook_url
                )
                """,
                payload,
            )
            return int(cursor.lastrowid)

    def get_recent_reversal_alerts(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM reversal_alert_events ORDER BY sent_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def get_last_successful_reversal_alert(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM reversal_alert_events
                WHERE success=1
                ORDER BY sent_at DESC
                LIMIT 1
                """
            ).fetchone()
        return row

    def insert_reversal_run(
        self,
        *,
        fetched_at: datetime,
        success: bool,
        poll_interval_seconds: int,
        duration_ms: int,
        rss_error_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO reversal_runs (
                    fetched_at,
                    success,
                    poll_interval_seconds,
                    duration_ms,
                    rss_error_count,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    fetched_at.isoformat(),
                    int(success),
                    poll_interval_seconds,
                    duration_ms,
                    rss_error_count,
                    error_message,
                ),
            )

    def get_recent_reversal_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM reversal_runs ORDER BY fetched_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def insert_us10y_sample(self, payload: dict[str, Any]) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO us10y_samples (
                    fetched_at,
                    tenor,
                    yield_pct,
                    yield_signal,
                    source,
                    note
                ) VALUES (
                    :fetched_at,
                    :tenor,
                    :yield_pct,
                    :yield_signal,
                    :source,
                    :note
                )
                """,
                payload,
            )
            return int(cursor.lastrowid)

    def get_latest_us10y_sample(self, tenor: str | None = None) -> dict[str, Any] | None:
        with self.connection() as conn:
            if tenor:
                row = conn.execute(
                    "SELECT * FROM us10y_samples WHERE tenor=? ORDER BY fetched_at DESC LIMIT 1",
                    (tenor,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM us10y_samples ORDER BY fetched_at DESC LIMIT 1"
                ).fetchone()
        return row

    def get_latest_us10y_samples(self, tenors: list[str]) -> dict[str, dict[str, Any] | None]:
        return {tenor: self.get_latest_us10y_sample(tenor=tenor) for tenor in tenors}

    def get_us10y_samples_since(
        self,
        since_iso: str,
        limit: int = 1000,
        tenor: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            if tenor:
                rows = conn.execute(
                    """
                    SELECT * FROM us10y_samples
                    WHERE fetched_at >= ? AND tenor = ?
                    ORDER BY fetched_at ASC
                    LIMIT ?
                    """,
                    (since_iso, tenor, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM us10y_samples
                    WHERE fetched_at >= ?
                    ORDER BY fetched_at ASC
                    LIMIT ?
                    """,
                    (since_iso, limit),
                ).fetchall()
        return rows

    def get_us10y_history(
        self,
        since_iso: str,
        limit: int = 2000,
        tenors: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        with self.connection() as conn:
            if not tenors:
                rows = conn.execute(
                    """
                    SELECT * FROM (
                        SELECT * FROM us10y_samples
                        WHERE fetched_at >= ?
                        ORDER BY fetched_at DESC
                        LIMIT ?
                    )
                    ORDER BY fetched_at ASC
                    """,
                    (since_iso, limit),
                ).fetchall()
            else:
                placeholders = ",".join("?" for _ in tenors)
                params: list[Any] = [since_iso, *tenors, limit]
                rows = conn.execute(
                    f"""
                    SELECT * FROM (
                        SELECT * FROM us10y_samples
                        WHERE fetched_at >= ? AND tenor IN ({placeholders})
                        ORDER BY fetched_at DESC
                        LIMIT ?
                    )
                    ORDER BY fetched_at ASC
                    """,
                    params,
                ).fetchall()
        return rows

    def get_recent_us10y_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM us10y_runs ORDER BY fetched_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def insert_us10y_run(
        self,
        *,
        fetched_at: datetime,
        tenor: str = "all",
        success: bool,
        poll_interval_seconds: int,
        duration_ms: int,
        error_message: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO us10y_runs (
                    fetched_at,
                    tenor,
                    success,
                    poll_interval_seconds,
                    duration_ms,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    fetched_at.isoformat(),
                    tenor,
                    int(success),
                    poll_interval_seconds,
                    duration_ms,
                    error_message,
                ),
            )

    def insert_us10y_alert_event(
        self,
        *,
        sent_at: datetime,
        tenors: list[str],
        lookback_hours: float,
        threshold_bp: float,
        drop_bp_values: dict[str, float],
        success: bool,
        response_text: str | None = None,
    ) -> int:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO us10y_alert_events (
                    sent_at,
                    tenors,
                    lookback_hours,
                    threshold_bp,
                    drop_bp_values,
                    success,
                    response_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sent_at.isoformat(),
                    ",".join(tenors),
                    lookback_hours,
                    threshold_bp,
                    json.dumps(drop_bp_values, ensure_ascii=False),
                    int(success),
                    response_text,
                ),
            )
            return int(cursor.lastrowid)

    def get_latest_us10y_alert_event(self, *, success_only: bool = False) -> dict[str, Any] | None:
        query = "SELECT * FROM us10y_alert_events"
        params: list[Any] = []
        if success_only:
            query += " WHERE success = 1"
        query += " ORDER BY sent_at DESC, id DESC LIMIT 1"
        with self.connection() as conn:
            row = conn.execute(query, params).fetchone()
        return row

    def insert_notification_log(
        self,
        *,
        sent_at: datetime,
        channel: str,
        event_type: str,
        target_name: str,
        webhook_url: str,
        success: bool,
        content: str,
        response_text: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO notification_logs (
                    sent_at,
                    channel,
                    event_type,
                    target_name,
                    webhook_url,
                    success,
                    content,
                    response_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sent_at.isoformat(),
                    channel,
                    event_type,
                    target_name,
                    webhook_url,
                    int(success),
                    content,
                    response_text,
                ),
            )

    def get_recent_notification_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM notification_logs ORDER BY sent_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return rows

    def export_state(self) -> str:
        return json.dumps(
            {
                "settings": self.get_settings(),
                "latest_sample": self.get_latest_sample(),
                "recent_alerts": self.get_recent_alerts(),
                "latest_reversal_sample": self.get_latest_reversal_sample(),
                "recent_reversal_alerts": self.get_recent_reversal_alerts(),
            },
            ensure_ascii=False,
        )
