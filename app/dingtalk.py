from __future__ import annotations

import base64
import hashlib
import hmac
import re
import time
from urllib.parse import quote_plus

from typing import Any, Sequence

import httpx


def _extract_alert_level(text: str) -> int | None:
    patterns = (
        r"(?:警报|预警|信号)?(?:等级|级别)\s*[:：]\s*([0-9]{1,2})\s*级?",
        r"alert\s*level\s*[:：]?\s*([0-9]{1,2})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            return int(match.group(1))
        except ValueError:
            continue
    return None


def build_signed_webhook(webhook: str, secret: str) -> str:
    secret = secret.strip()
    if not secret:
        return webhook
    timestamp = str(int(time.time() * 1000))
    sign_payload = f"{timestamp}\n{secret}".encode("utf-8")
    sign = hmac.new(secret.encode("utf-8"), sign_payload, digestmod=hashlib.sha256).digest()
    encoded_sign = quote_plus(base64.b64encode(sign))
    delimiter = "&" if "?" in webhook else "?"
    return f"{webhook}{delimiter}timestamp={timestamp}&sign={encoded_sign}"


async def post_text_message(
    webhook: str,
    text: str,
    *,
    secret: str = "",
    at_user_ids: Sequence[str] | None = None,
    timeout_seconds: float = 10.0,
) -> tuple[bool, str]:
    signed_webhook = build_signed_webhook(webhook, secret)
    payload = {
        "msgtype": "text",
        "text": {"content": text},
        "at": {
            "atUserIds": list(at_user_ids or []),
            "isAtAll": False,
        },
    }
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            response = await client.post(signed_webhook, json=payload)
            response.raise_for_status()
        return True, response.text[:500]
    except Exception as exc:
        return False, str(exc)


def resolve_notification_targets(settings: dict[str, Any]) -> list[dict[str, str]]:
    raw_targets = settings.get("notification_targets") or []
    targets: list[dict[str, str]] = []
    for target in raw_targets:
        if not isinstance(target, dict):
            continue
        webhook = str(target.get("webhook", "")).strip()
        if not webhook:
            continue
        targets.append(
            {
                "name": str(target.get("name", "")).strip() or "默认机器人",
                "webhook": webhook,
                "secret": str(target.get("secret", "")).strip(),
                "enabled": "1" if target.get("enabled", True) else "0",
            }
        )
    if targets:
        return [target for target in targets if target["enabled"] == "1"]

    legacy_webhook = str(settings.get("dingtalk_webhook", "")).strip()
    if not legacy_webhook:
        return []
    return [
        {
            "name": "默认机器人",
            "webhook": legacy_webhook,
            "secret": str(settings.get("dingtalk_secret", "")).strip(),
            "enabled": "1",
        }
    ]


async def post_text_to_targets(
    settings: dict[str, Any],
    text: str,
    *,
    timeout_seconds: float = 10.0,
) -> tuple[bool, str]:
    success, summary, _ = await post_text_to_targets_detailed(
        settings,
        text,
        timeout_seconds=timeout_seconds,
    )
    return success, summary


async def post_text_to_targets_detailed(
    settings: dict[str, Any],
    text: str,
    *,
    timeout_seconds: float = 10.0,
) -> tuple[bool, str, list[dict[str, str]]]:
    # Defensive gate: never push reversal L3/L4 notifications.
    if ("\u9ec4\u91d1\u53cd\u8f6c\u4e09\u7ea7\u4fe1\u53f7" in text) or ("\u9ec4\u91d1\u53cd\u8f6c\u56db\u7ea7\u4fe1\u53f7" in text):
        return False, "blocked: reversal level 3/4 should not be pushed", []
    # Kill-switch for legacy RSS event alert template to avoid push storms.
    if "\u0052\u0053\u0053\u4e8b\u4ef6\u8b66\u62a5" in text:
        return False, "blocked: legacy RSS event alert push disabled", []
    # Extra defensive gate: if message carries a generic alert level, block level >=3.
    level_value = _extract_alert_level(text)
    if level_value is not None and level_value >= 3:
        return False, f"blocked: alert level {level_value} should not be pushed", []
    # Legacy RSS-event alert template fallback guard.
    if ("\u0052\u0053\u0053\u4e8b\u4ef6\u8b66\u62a5" in text) and re.search(
        "(?:\u8b66\u62a5)?\u7b49\u7ea7\\s*[:\uff1a]\\s*([3-9][0-9]?)\\s*\u7ea7",
        text,
    ):
        return False, "blocked: RSS event alert level >=3 should not be pushed", []

    targets = resolve_notification_targets(settings)
    if not targets:
        return False, "No enabled notification targets", []

    results: list[str] = []
    details: list[dict[str, str]] = []
    any_success = False
    for target in targets:
        success, response_text = await post_text_message(
            target["webhook"],
            text,
            secret=target["secret"],
            at_user_ids=settings.get("dingtalk_at_user_ids", []),
            timeout_seconds=timeout_seconds,
        )
        any_success = any_success or success
        status = "ok" if success else "fail"
        results.append(f"{target['name']}:{status}:{response_text[:120]}")
        details.append(
            {
                "target_name": target["name"],
                "webhook_url": target["webhook"],
                "success": "1" if success else "0",
                "response_text": response_text[:500],
            }
        )
    return any_success, " | ".join(results)[:1000], details
