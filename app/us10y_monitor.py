from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from zoneinfo import ZoneInfo

import httpx

from .config import SHANGHAI_TZ
from .db import Database
from .dingtalk import post_text_to_targets_detailed, resolve_notification_targets


CN_TZ = ZoneInfo(SHANGHAI_TZ)
SINA_PATTERN = re.compile(r'var hq_str_(?P<symbol>[^=]+)="(?P<body>.*?)";')
NUMBER_PATTERN = re.compile(r"-?\d+(?:\.\d+)?")
FRED_SERIES_BY_TENOR = {
    "5y": "DGS5",
    "10y": "DGS10",
    "20y": "DGS20",
}
SINA_CANDIDATES_BY_TENOR = {
    "5y": "US5Y,gb_US5Y,hf_US5Y,znb_US5Y",
    "10y": "US10Y,gb_US10Y,hf_US10Y,znb_US10Y",
    "20y": "US20Y,gb_US20Y,hf_US20Y,znb_US20Y",
}
EASTMONEY_SECID_BY_TENOR = {
    "5y": "171.US5Y",
    "10y": "171.US10Y",
    "20y": "171.US20Y",
}
SOURCE_FRESHNESS_MAX_AGE_SECONDS = 900


@dataclass
class US10YCycleResult:
    sample_ids: dict[str, int]
    triggered_tenors: list[str]
    latest_yields: dict[str, float]
    note: str
    fetched_at: str


class US10YMonitorService:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._last_source_status: dict[str, list[dict[str, str | float | bool]]] = {}

    def get_source_status(self) -> dict[str, list[dict[str, str | float | bool]]]:
        return self._last_source_status

    async def run_cycle(self) -> US10YCycleResult:
        settings = self.db.get_settings()
        fetched_at = datetime.now(CN_TZ)
        tenors = settings.get("us10y_tenors", ["10y"]) or ["10y"]
        lookback_hours = float(settings.get("us10y_drop_lookback_hours", 24))
        threshold_bp = float(settings.get("us10y_drop_threshold_bp", 1.0))
        interval = int(settings.get("us10y_poll_interval_seconds", 60))
        started = time.perf_counter()

        sample_ids: dict[str, int] = {}
        latest_yields: dict[str, float] = {}
        triggered_tenors: list[str] = []
        drop_bp_values: dict[str, float] = {}
        detail_notes: list[str] = []
        source_status: dict[str, list[dict[str, str | float | bool]]] = {}

        try:
            for tenor in tenors:
                yield_pct, source, fetch_status = await self._fetch_latest_yield(
                    tenor=tenor,
                    timeout_seconds=float(settings["request_timeout_seconds"]),
                )
                source_status[tenor] = fetch_status
                latest_yields[tenor] = yield_pct
                triggered, drop_bp, note = self._evaluate_yield_signal(
                    tenor=tenor,
                    fetched_at=fetched_at,
                    current_yield=yield_pct,
                    lookback_hours=lookback_hours,
                    threshold_bp=threshold_bp,
                )
                drop_bp_values[tenor] = round(drop_bp, 3)
                if triggered:
                    triggered_tenors.append(tenor)
                detail_notes.append(f"{tenor.upper()}: {note}")
                sample_ids[tenor] = self.db.insert_us10y_sample(
                    {
                        "fetched_at": fetched_at.isoformat(),
                        "tenor": tenor,
                        "yield_pct": round(yield_pct, 6),
                        "yield_signal": int(triggered),
                        "source": source,
                        "note": note,
                    }
                )

            if triggered_tenors:
                await self._maybe_send_alert(
                    fetched_at=fetched_at,
                    triggered_tenors=triggered_tenors,
                    latest_yields=latest_yields,
                    lookback_hours=lookback_hours,
                    threshold_bp=threshold_bp,
                    drop_bp_values=drop_bp_values,
                    settings=settings,
                )

            self.db.insert_us10y_run(
                fetched_at=fetched_at,
                tenor="all",
                success=True,
                poll_interval_seconds=interval,
                duration_ms=int((time.perf_counter() - started) * 1000),
            )
            self._last_source_status = source_status
            return US10YCycleResult(
                sample_ids=sample_ids,
                triggered_tenors=triggered_tenors,
                latest_yields=latest_yields,
                note="；".join(detail_notes),
                fetched_at=fetched_at.isoformat(),
            )
        except Exception as exc:
            self._last_source_status = source_status
            self.db.insert_us10y_run(
                fetched_at=fetched_at,
                tenor="all",
                success=False,
                poll_interval_seconds=interval,
                duration_ms=int((time.perf_counter() - started) * 1000),
                error_message=str(exc),
            )
            raise

    async def _fetch_latest_yield(
        self,
        *,
        tenor: str,
        timeout_seconds: float,
    ) -> tuple[float, str, list[dict[str, str | float | bool]]]:
        statuses: list[dict[str, str | float | bool]] = []
        class _StaleQuoteError(RuntimeError):
            pass

        async def _attempt(source: str, fn):
            started = time.perf_counter()
            try:
                value, obs_time = await fn()
                age = self._calc_age_seconds(obs_time)
                stale = age is not None and age > SOURCE_FRESHNESS_MAX_AGE_SECONDS
                status: dict[str, str | float | bool] = {
                    "source": source,
                    "ok": not stale,
                    "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                    "value": round(value, 6),
                    "observed_at": obs_time.isoformat() if obs_time else "",
                    "age_seconds": round(age, 1) if age is not None else -1,
                    "message": "ok" if not stale else "stale",
                }
                if age is None:
                    status["message"] = "ok(no-ts)"
                if stale:
                    status["message"] = f"stale({round(age,1)}s)"
                statuses.append(status)
                if stale:
                    raise _StaleQuoteError("stale quote")
                return value, source
            except _StaleQuoteError:
                raise
            except Exception as exc:
                statuses.append(
                    {
                        "source": source,
                        "ok": False,
                        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
                        "value": -1.0,
                        "observed_at": "",
                        "age_seconds": -1,
                        "message": str(exc)[:180],
                    }
                )
                raise

        for source_name, fn in (
            ("Eastmoney", lambda: self._fetch_eastmoney_yield(tenor=tenor, timeout_seconds=timeout_seconds)),
            ("Sina hq.sinajs.cn", lambda: self._fetch_sina_yield(tenor=tenor, timeout_seconds=timeout_seconds)),
        ):
            try:
                value, source = await _attempt(source_name, fn)
                return value, source, statuses
            except Exception:
                continue

        series = FRED_SERIES_BY_TENOR.get(tenor, "DGS10")
        try:
            value, source = await _attempt(
                f"FRED {series}",
                lambda: self._fetch_fred_yield(tenor=tenor, timeout_seconds=timeout_seconds),
            )
            return value, source, statuses
        except Exception as exc:
            raise RuntimeError(f"{tenor.upper()} all sources failed: {exc}") from exc

    @staticmethod
    def _calc_age_seconds(observed_at: datetime | None) -> float | None:
        if observed_at is None:
            return None
        now = datetime.now(CN_TZ)
        return max(0.0, (now - observed_at.astimezone(CN_TZ)).total_seconds())

    async def _fetch_eastmoney_yield(self, *, tenor: str, timeout_seconds: float) -> tuple[float, datetime | None]:
        secid = EASTMONEY_SECID_BY_TENOR.get(tenor, "171.US10Y")
        url = (
            "https://push2.eastmoney.com/api/qt/stock/get"
            f"?secid={secid}&fields=f43,f84,f85,f86,f124,f57,f58"
        )
        headers = {
            "Referer": "https://quote.eastmoney.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
        payload = response.text.strip()
        if payload.startswith("jQuery"):
            left = payload.find("(")
            right = payload.rfind(")")
            if left > 0 and right > left:
                payload = payload[left + 1:right]
        data = json.loads(payload).get("data") or {}
        raw = next((data.get(key) for key in ("f43", "f84", "f85", "f86") if data.get(key) not in (None, "")), None)
        if raw is None:
            raise ValueError("missing eastmoney quote field")
        value = float(raw)
        if value > 1000:
            value = value / 10000.0
        elif value > 100:
            value = value / 100.0
        if value <= 0 or value >= 20:
            raise ValueError(f"eastmoney out-of-range value: {value}")
        obs_time = self._parse_eastmoney_observed_at(data)
        return value, obs_time

    @staticmethod
    def _parse_eastmoney_observed_at(data: dict) -> datetime | None:
        # Eastmoney US Treasury payloads usually carry update epoch in f86.
        # f124 may be 0 for this market, so we probe both fields safely.
        for key in ("f86", "f124"):
            dt = US10YMonitorService._parse_epoch_like_datetime(data.get(key))
            if dt is not None:
                return dt
        return None

    @staticmethod
    def _parse_epoch_like_datetime(raw: object) -> datetime | None:
        if raw in (None, ""):
            return None
        try:
            value = float(str(raw).strip())
        except (TypeError, ValueError):
            return None
        if value <= 0:
            return None
        # millisecond timestamp
        if value > 1e12:
            value = value / 1000.0
        # sanity window: year 2000 ~ year 2100
        if value < 946684800 or value > 4102444800:
            return None
        return datetime.fromtimestamp(value, tz=CN_TZ)

    async def _fetch_sina_yield(self, *, tenor: str, timeout_seconds: float) -> tuple[float, datetime | None]:
        headers = {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        sina_codes = SINA_CANDIDATES_BY_TENOR.get(tenor, SINA_CANDIDATES_BY_TENOR["10y"])
        sina_url = f"https://hq.sinajs.cn/list={sina_codes}"
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            response = await client.get(sina_url, headers=headers)
            response.raise_for_status()
        value, obs_time = self._parse_sina_payload_strict(response.text)
        if value is None:
            raise ValueError("missing sina quote field")
        return value, obs_time

    async def _fetch_fred_yield(self, *, tenor: str, timeout_seconds: float) -> tuple[float, datetime | None]:
        series = FRED_SERIES_BY_TENOR.get(tenor, "DGS10")
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
        return self._parse_fred_csv(response.text, series), None

    @staticmethod
    def _parse_sina_payload_strict(text: str) -> tuple[float | None, datetime | None]:
        for match in SINA_PATTERN.finditer(text):
            body = match.group("body")
            fields = [item.strip() for item in body.split(",")]
            numbers = [float(item) for item in NUMBER_PATTERN.findall(body)]
            value = next((num for num in numbers if 0.01 < num < 20), None)
            obs_time: datetime | None = None
            if len(fields) >= 2:
                date_s = fields[-2]
                time_s = fields[-1]
                try:
                    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_s) and re.match(r"^\d{2}:\d{2}:\d{2}$", time_s):
                        obs_time = datetime.fromisoformat(f"{date_s}T{time_s}").replace(tzinfo=CN_TZ)
                except Exception:
                    obs_time = None
            if value is not None:
                return value, obs_time
        return None, None

    @staticmethod
    def _parse_fred_csv(text: str, series: str) -> float:
        last_value: float | None = None
        reader = csv.DictReader(StringIO(text))
        for row in reader:
            raw = str(row.get(series, "")).strip()
            if not raw or raw == ".":
                continue
            try:
                last_value = float(raw)
            except ValueError:
                continue
        if last_value is None:
            raise ValueError(f"{series} has no valid numeric value")
        return last_value

    def _evaluate_yield_signal(
        self,
        *,
        tenor: str,
        fetched_at: datetime,
        current_yield: float,
        lookback_hours: float,
        threshold_bp: float,
    ) -> tuple[bool, float, str]:
        since = fetched_at - timedelta(hours=lookback_hours)
        samples = self.db.get_us10y_samples_since(since.isoformat(), limit=2000, tenor=tenor)
        if len(samples) < 3:
            return False, 0.0, f"{tenor.upper()}样本预热中({len(samples)}/3)，已获取实时收益率"

        historical = [float(item["yield_pct"]) for item in samples]
        recent_high = max(historical)
        previous = historical[-1]
        drop_bp = (recent_high - current_yield) * 100
        triggered = current_yield < previous and drop_bp >= threshold_bp
        note = (
            f"{tenor.upper()}: 现值 {current_yield:.3f}% / 高点 {recent_high:.3f}% / "
            f"{lookback_hours:.1f}h回落 {drop_bp:.2f}bp (阈值 {threshold_bp:.2f}bp)"
        )
        return triggered, drop_bp, note

    async def _maybe_send_alert(
        self,
        *,
        fetched_at: datetime,
        triggered_tenors: list[str],
        latest_yields: dict[str, float],
        lookback_hours: float,
        threshold_bp: float,
        drop_bp_values: dict[str, float],
        settings: dict,
    ) -> bool:
        if not resolve_notification_targets(settings):
            return False
        dedup_hours = max(0, int(settings.get("us10y_alert_dedup_hours", 4)))
        if dedup_hours > 0:
            latest = self.db.get_latest_us10y_alert_event(success_only=True)
            if latest and latest.get("sent_at"):
                try:
                    last_sent = datetime.fromisoformat(str(latest["sent_at"]))
                    if last_sent.tzinfo is None:
                        last_sent = last_sent.replace(tzinfo=CN_TZ)
                    if fetched_at - last_sent < timedelta(hours=dedup_hours):
                        return False
                except Exception:
                    pass

        cooldown_seconds = int(
            settings.get(
                "us10y_alert_cooldown_seconds",
                settings.get("reversal_cooldown_seconds", 1800),
            )
        )
        if cooldown_seconds > 0:
            latest = self.db.get_latest_us10y_alert_event(success_only=True)
            if latest and latest.get("sent_at"):
                try:
                    last_sent = datetime.fromisoformat(str(latest["sent_at"]))
                    if last_sent.tzinfo is None:
                        last_sent = last_sent.replace(tzinfo=CN_TZ)
                    if fetched_at - last_sent < timedelta(seconds=cooldown_seconds):
                        return False
                except Exception:
                    pass

        text_lines = [
            "美债收益率回落预警",
            f"时间: {fetched_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"触发品类: {', '.join(tenor.upper() for tenor in triggered_tenors)}",
            f"规则: {lookback_hours:.1f}h 回落 >= {threshold_bp:.2f}bp",
        ]
        for tenor in sorted(latest_yields.keys()):
            text_lines.append(
                f"{tenor.upper()}: {latest_yields[tenor]:.3f}% / 回落 {drop_bp_values.get(tenor, 0.0):.2f}bp"
            )
        message_text = "\n".join(text_lines)
        success, response_text, details = await post_text_to_targets_detailed(settings, message_text, timeout_seconds=10)

        self.db.insert_us10y_alert_event(
            sent_at=fetched_at,
            tenors=triggered_tenors,
            lookback_hours=lookback_hours,
            threshold_bp=threshold_bp,
            drop_bp_values=drop_bp_values,
            success=success,
            response_text=response_text,
        )
        for detail in details:
            self.db.insert_notification_log(
                sent_at=fetched_at,
                channel="dingtalk",
                event_type="us10y_alert",
                target_name=detail["target_name"],
                webhook_url=detail["webhook_url"],
                success=detail["success"] == "1",
                content=message_text,
                response_text=detail["response_text"],
            )
        return success
