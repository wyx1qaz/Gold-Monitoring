from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from .config import NEW_YORK_TZ, SHANGHAI_TZ


SH_TZ = ZoneInfo(SHANGHAI_TZ)
NY_TZ = ZoneInfo(NEW_YORK_TZ)


def is_shfe_gold_open(now: datetime) -> bool:
    local = now.astimezone(SH_TZ)
    weekday = local.weekday()
    current = local.time()

    if weekday >= 5:
        return False

    day_sessions = (
        (time(9, 0), time(11, 30)),
        (time(13, 30), time(15, 0)),
    )
    night_session = (time(21, 0), time(23, 59, 59))
    early_session = (time(0, 0), time(2, 30))

    # Monday early morning has no session after weekend break.
    if weekday == 0 and current <= early_session[1]:
        return False
    if weekday == 4 and current >= night_session[0]:
        return False

    return (
        any(start <= current <= end for start, end in day_sessions)
        or night_session[0] <= current <= night_session[1]
        or (weekday in {1, 2, 3, 4} and early_session[0] <= current <= early_session[1])
    )


def is_london_gold_open(now: datetime) -> bool:
    local = now.astimezone(NY_TZ)
    weekday = local.weekday()
    current = local.time()
    close_time = time(17, 0)

    if weekday in {0, 1, 2, 3}:
        return True
    if weekday == 4:
        return current < close_time
    if weekday == 6:
        return current >= close_time
    return False


def is_comex_gold_open(now: datetime) -> bool:
    local = now.astimezone(NY_TZ)
    weekday = local.weekday()
    current = local.time()
    daily_break_start = time(17, 0)
    daily_break_end = time(18, 0)

    if weekday == 5:
        return False
    if weekday == 6:
        return current >= daily_break_end
    if weekday == 4 and current >= daily_break_start:
        return False
    if daily_break_start <= current < daily_break_end:
        return False
    return True
