from __future__ import annotations


POSITIVE_KEYWORDS = (
    "ceasefire",
    "truce",
    "de-escalat",
    "deescalat",
    "negotiat",
    "talks",
    "agreement",
    "deal",
    "reopen",
    "shipping resume",
    "shipping lane reopen",
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


def score_gold_risk_event(event: dict) -> dict:
    title = str(event.get("title", ""))
    summary = str(event.get("summary", ""))
    text = f"{title} {summary}".lower()

    # 评分含义：地缘“缓和程度”评分（1=显著恶化，10=显著缓和）
    score = 5.0
    reasons: list[str] = []

    has_ceasefire = ("停火" in text or "ceasefire" in text or "truce" in text)
    has_reopen = (
        "恢复通航" in text
        or "恢复航运" in text
        or "reopen" in text
        or "restore shipping" in text
        or "shipping resume" in text
    )
    has_hormuz = ("霍尔木兹" in text or "hormuz" in text)
    has_blockade = ("封锁" in text or "closure" in text or "blockade" in text)
    has_escalation = (
        "升级" in text
        or "escalat" in text
        or "袭击" in text
        or "attack" in text
        or "导弹" in text
        or "missile" in text
        or "空袭" in text
        or "airstrike" in text
    )
    has_rate_cut = ("降息" in text or "rate cut" in text or "fed cut" in text)
    has_rate_hike = ("加息" in text or "rate hike" in text or "fed hike" in text)
    has_strong_negative = any(pattern in text for pattern in STRONG_NEGATIVE_PATTERNS)

    if has_ceasefire and has_rate_cut:
        score = 10
        reasons.append("命中规则: 停火+降息 => 10分")
    elif has_escalation and has_rate_hike:
        score = 1
        reasons.append("命中规则: 战争升级+加息 => 1分")
    elif has_hormuz and has_blockade:
        score = 2
        reasons.append("命中规则: 霍尔木兹封锁 => 2分")
    elif has_hormuz and has_reopen:
        score = 9
        reasons.append("命中规则: 霍尔木兹恢复通航 => 9分")

    if has_strong_negative:
        score = min(score, 2.5)
        reasons.append("命中规则: 最后通牒/最后期限/紧张升级 => 低分")

    positive_hits = sum(1 for kw in POSITIVE_KEYWORDS if kw in text)
    if positive_hits:
        score += min(4.0, positive_hits * 0.8)
        reasons.append(f"利好词命中{positive_hits}个")

    negative_hits = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text)
    if negative_hits:
        score -= min(4.0, negative_hits * 0.9)
        reasons.append(f"利空词命中{negative_hits}个")

    low_conf_hits = sum(1 for kw in LOW_CONFIDENCE_KEYWORDS if kw in text)
    if low_conf_hits:
        score -= min(2.0, low_conf_hits * 0.5)
        reasons.append(f"低可信表态词命中{low_conf_hits}个")

    if has_escalation:
        score -= 1.2
        reasons.append("地缘紧张升级，降分")
    if has_ceasefire or has_reopen:
        score += 1.2
        reasons.append("地缘缓和信号，升分")

    final_score = max(1, min(10, int(round(score))))
    if has_strong_negative:
        final_score = min(final_score, 3)

    if final_score >= 8:
        level = "高"
    elif final_score >= 5:
        level = "中"
    else:
        level = "低"

    item = dict(event)
    item["impact_score"] = final_score
    item["impact_level"] = level
    item["impact_note"] = "；".join(reasons) if reasons else "未命中明显风险词，按基础分"
    return item

