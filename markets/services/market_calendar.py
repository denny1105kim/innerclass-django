from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
from functools import lru_cache
from typing import Optional, Tuple

import pandas as pd
from django.utils import timezone

from markets.models import MarketChoices

try:
    import exchange_calendars as ecals
except Exception as e:  # pragma: no cover
    ecals = None  # type: ignore
    _IMPORT_ERROR = e


@dataclass(frozen=True)
class MarketCalendarStatus:
    market: str
    calendar_code: str
    now_utc: datetime
    is_open: bool
    reason: str


def _to_utc(dt: datetime) -> datetime:
    if timezone.is_naive(dt):
        return dt.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc)


def _calendar_code_for_market(market: str) -> str:
    if market in (MarketChoices.KOSPI, MarketChoices.KOSDAQ):
        return "XKRX"
    if market == MarketChoices.NASDAQ:
        return "XNAS"
    raise ValueError(f"Unsupported market: {market!r}")


@lru_cache(maxsize=16)
def _get_calendar(calendar_code: str):
    if ecals is None:  # pragma: no cover
        raise RuntimeError(
            "exchange_calendars is required for market open/close gating. "
            "Install it with: pip install exchange-calendars"
        ) from _IMPORT_ERROR
    return ecals.get_calendar(calendar_code)


def _get_session_bounds_utc(cal, now_utc: datetime) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Return (open_utc, close_utc) for the session that is closest to 'now' date.
    If today is not a session (holiday/weekend), use the next session for pre-open
    checks and previous session for post-close checks via grace logic in caller.
    """
    ts = pd.Timestamp(now_utc).tz_convert("UTC")

    start = (now_utc.date() - timedelta(days=7)).isoformat()
    end = (now_utc.date() + timedelta(days=7)).isoformat()
    sched = cal.schedule(start_date=start, end_date=end)
    if sched is None or len(sched.index) == 0:
        return None

    # If ts lands on a session, use that session.
    try:
        session = cal.minute_to_session(ts, direction="none")
        return cal.session_open(session), cal.session_close(session)
    except Exception:
        pass

    # Otherwise, get nearest sessions around ts.
    # next_session for pre-open grace; prev_session for post-close grace.
    try:
        next_sess = cal.next_session_label(ts)
        prev_sess = cal.previous_session_label(ts)
    except Exception:
        # Fallback to index-based approach
        try:
            sessions = sched.index
            # Find insertion point
            pos = sessions.searchsorted(ts.normalize())
            prev_sess = sessions[max(0, pos - 1)]
            next_sess = sessions[min(len(sessions) - 1, pos)]
        except Exception:
            return None

    # Return bounds for both candidates by picking "closest day" to now
    # Caller will compute grace window around whichever bound matches.
    # Here we return the "next session" bounds primarily, and caller may also
    # consult previous session by calling this helper again if needed.
    try:
        open_utc = cal.session_open(next_sess)
        close_utc = cal.session_close(next_sess)
        return open_utc, close_utc
    except Exception:
        return None


def is_market_open_now(*, market: str, now: Optional[datetime] = None) -> MarketCalendarStatus:
    now = now or timezone.now()
    now_utc = _to_utc(now)
    calendar_code = _calendar_code_for_market(market)

    cal = _get_calendar(calendar_code)
    ts = pd.Timestamp(now_utc).tz_convert("UTC")

    # Direct API if available
    if hasattr(cal, "is_open_on_minute"):
        try:
            open_ = bool(cal.is_open_on_minute(ts))
            return MarketCalendarStatus(
                market=market, calendar_code=calendar_code, now_utc=now_utc, is_open=open_, reason="checked via is_open_on_minute"
            )
        except Exception:
            pass

    # Fallback: session bounds
    bounds = _get_session_bounds_utc(cal, now_utc)
    if not bounds:
        return MarketCalendarStatus(
            market=market,
            calendar_code=calendar_code,
            now_utc=now_utc,
            is_open=False,
            reason="no sessions in range (holiday/weekend or calendar unavailable)",
        )

    open_utc, close_utc = bounds
    open_ = bool(open_utc <= ts <= close_utc)
    return MarketCalendarStatus(
        market=market, calendar_code=calendar_code, now_utc=now_utc, is_open=open_, reason="checked via session_open/session_close"
    )


def should_run_sync(
    *,
    market: str,
    now: Optional[datetime] = None,
    force: bool = False,
    pre_open_grace_min: int = 0,
    post_close_grace_min: int = 0,
) -> MarketCalendarStatus:
    """
    Decide whether the sync should run.

    Rules
    -----
    1) force=True => always run
    2) regular session open => run
    3) otherwise, run if within grace window:
       - pre-open: now in [open - pre, open)
       - post-close: now in (close, close + post]
       For holidays/weekends, "pre-open" targets the next session open,
       "post-close" targets the previous session close.
    """
    now = now or timezone.now()
    now_utc = _to_utc(now)

    if force:
        return MarketCalendarStatus(
            market=market,
            calendar_code=_calendar_code_for_market(market),
            now_utc=now_utc,
            is_open=True,
            reason="forced",
        )

    base = is_market_open_now(market=market, now=now)
    if base.is_open:
        return base

    calendar_code = _calendar_code_for_market(market)
    cal = _get_calendar(calendar_code)
    ts = pd.Timestamp(now_utc).tz_convert("UTC")

    # Grace windows disabled => 그대로 종료
    pre = max(0, int(pre_open_grace_min))
    post = max(0, int(post_close_grace_min))
    if pre == 0 and post == 0:
        return base

    # Determine next session open (for pre-open grace)
    # and previous session close (for post-close grace)
    try:
        next_session = cal.next_session_label(ts)
        next_open = cal.session_open(next_session)
    except Exception:
        next_open = None

    try:
        prev_session = cal.previous_session_label(ts)
        prev_close = cal.session_close(prev_session)
    except Exception:
        prev_close = None

    # pre-open grace
    if pre > 0 and next_open is not None:
        pre_start = next_open - pd.Timedelta(minutes=pre)
        if pre_start <= ts < next_open:
            return MarketCalendarStatus(
                market=market,
                calendar_code=calendar_code,
                now_utc=now_utc,
                is_open=True,
                reason=f"pre-open grace ({pre}m) for next session",
            )

    # post-close grace
    if post > 0 and prev_close is not None:
        post_end = prev_close + pd.Timedelta(minutes=post)
        if prev_close < ts <= post_end:
            return MarketCalendarStatus(
                market=market,
                calendar_code=calendar_code,
                now_utc=now_utc,
                is_open=True,
                reason=f"post-close grace ({post}m) for previous session",
            )

    return base
