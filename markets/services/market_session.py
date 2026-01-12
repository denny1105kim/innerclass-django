from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from typing import Optional

import pandas as pd
from django.utils import timezone

from markets.services.session_status import MarketSessionStatus
from markets.services.market_calendar import _get_calendar, _calendar_code_for_market, _to_utc


@dataclass(frozen=True)
class MarketSessionInfo:
    market: str
    status: MarketSessionStatus
    asof: datetime  # timezone-aware
    calendar_code: str
    reason: str
    next_open_at: Optional[datetime] = None
    prev_close_at: Optional[datetime] = None


def get_market_session_info(
    *,
    market: str,
    now: Optional[datetime] = None,
    pre_open_grace_min: int = 0,
    post_close_grace_min: int = 0,
) -> MarketSessionInfo:
    now = now or timezone.now()
    now_utc = _to_utc(now)

    cal_code = _calendar_code_for_market(market)
    cal = _get_calendar(cal_code)

    # always use UTC Timestamp for calendar comparisons
    ts = pd.Timestamp(now_utc).tz_convert("UTC")

    # 1) 정규장 OPEN 여부
    is_open = False
    if hasattr(cal, "is_open_on_minute"):
        try:
            is_open = bool(cal.is_open_on_minute(ts))
        except Exception:
            is_open = False

    # next/prev session bounds (UI용 메타)
    next_open_at: Optional[datetime] = None
    prev_close_at: Optional[datetime] = None

    # NOTE: calendar 객체에 next/prev 관련 메서드가 없을 수 있으니 안전하게 감쌈
    try:
        if hasattr(cal, "next_session_label") and hasattr(cal, "session_open"):
            next_session = cal.next_session_label(ts)
            next_open_at = cal.session_open(next_session).to_pydatetime().astimezone(dt_timezone.utc)
    except Exception:
        pass

    try:
        if hasattr(cal, "previous_session_label") and hasattr(cal, "session_close"):
            prev_session = cal.previous_session_label(ts)
            prev_close_at = cal.session_close(prev_session).to_pydatetime().astimezone(dt_timezone.utc)
    except Exception:
        pass

    # OPEN
    if is_open:
        return MarketSessionInfo(
            market=market,
            status=MarketSessionStatus.OPEN,
            asof=now,
            calendar_code=cal_code,
            reason="regular session open",
            next_open_at=next_open_at,
            prev_close_at=prev_close_at,
        )

    # 2) PRE_OPEN grace
    pre = max(0, int(pre_open_grace_min))
    if pre > 0 and next_open_at is not None:
        next_open_ts = pd.Timestamp(next_open_at).tz_convert("UTC")
        if (next_open_ts - pd.Timedelta(minutes=pre)) <= ts < next_open_ts:
            return MarketSessionInfo(
                market=market,
                status=MarketSessionStatus.PRE_OPEN,
                asof=now,
                calendar_code=cal_code,
                reason=f"pre-open grace ({pre}m)",
                next_open_at=next_open_at,
                prev_close_at=prev_close_at,
            )

    # 3) POST_CLOSE grace
    post = max(0, int(post_close_grace_min))
    if post > 0 and prev_close_at is not None:
        prev_close_ts = pd.Timestamp(prev_close_at).tz_convert("UTC")
        if prev_close_ts < ts <= (prev_close_ts + pd.Timedelta(minutes=post)):
            return MarketSessionInfo(
                market=market,
                status=MarketSessionStatus.POST_CLOSE,
                asof=now,
                calendar_code=cal_code,
                reason=f"post-close grace ({post}m)",
                next_open_at=next_open_at,
                prev_close_at=prev_close_at,
            )

    # 4) HOLIDAY vs CLOSED 구분 (FIX)
    # 핵심:
    # - minute_to_session(direction="none")는 "휴장"이 아니라 "거래 분이 아님"에서도 ValueError를 던짐
    # - 따라서 ValueError는 CLOSED로 처리(장외시간), "세션 자체 없음"만 HOLIDAY로 처리
    try:
        # exchange_calendars 계열이면 is_session이 가장 직관적/정확
        if hasattr(cal, "is_session"):
            session_label = ts.normalize().date()
            if bool(cal.is_session(session_label)):
                return MarketSessionInfo(
                    market=market,
                    status=MarketSessionStatus.CLOSED,
                    asof=now,
                    calendar_code=cal_code,
                    reason="session day but outside regular hours",
                    next_open_at=next_open_at,
                    prev_close_at=prev_close_at,
                )
            return MarketSessionInfo(
                market=market,
                status=MarketSessionStatus.HOLIDAY,
                asof=now,
                calendar_code=cal_code,
                reason="no session (weekend/holiday)",
                next_open_at=next_open_at,
                prev_close_at=prev_close_at,
            )

        # is_session이 없으면 minute_to_session로 우회.
        # direction="none"에서 예외가 나더라도, ValueError는 CLOSED로 보는 것이 안전.
        cal.minute_to_session(ts, direction="none")
        return MarketSessionInfo(
            market=market,
            status=MarketSessionStatus.CLOSED,
            asof=now,
            calendar_code=cal_code,
            reason="session day but outside regular hours",
            next_open_at=next_open_at,
            prev_close_at=prev_close_at,
        )

    except ValueError:
        # 중요: "거래 분이 아님"(장외)일 수 있으므로 CLOSED로 처리
        return MarketSessionInfo(
            market=market,
            status=MarketSessionStatus.CLOSED,
            asof=now,
            calendar_code=cal_code,
            reason="not a trading minute (outside regular hours)",
            next_open_at=next_open_at,
            prev_close_at=prev_close_at,
        )

    except Exception:
        # 이 경우는 캘린더/세션 판정 자체가 실패한 케이스로, 보수적으로 HOLIDAY 처리
        return MarketSessionInfo(
            market=market,
            status=MarketSessionStatus.HOLIDAY,
            asof=now,
            calendar_code=cal_code,
            reason="no session (weekend/holiday)",
            next_open_at=next_open_at,
            prev_close_at=prev_close_at,
        )
