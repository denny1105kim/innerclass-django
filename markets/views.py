from __future__ import annotations

from datetime import date as _date

from django.utils import timezone
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from .models import DailyStockSnapshot, Market


def _serialize_snapshot(s: DailyStockSnapshot) -> dict:
    return {
        "symbol": s.stock.symbol,
        "name": s.stock.name,
        "exchange": s.stock.exchange,
        "currency": s.stock.currency,
        "open": float(s.open) if s.open is not None else None,
        "close": float(s.close) if s.close is not None else None,
        "intraday_pct": float(s.intraday_pct) if s.intraday_pct is not None else None,
        "change_pct": float(s.change_pct) if s.change_pct is not None else None,
        "market_cap": s.market_cap,
        "volume": s.volume,
        "date": s.date.isoformat(),
    }


@api_view(["GET"])
def today_market(request: Request):
    market = (request.query_params.get("market", "KR") or "KR").upper().strip()
    if market not in (Market.KR, Market.US):
        return Response({"detail": "market must be KR or US"}, status=400)

    exchange = (request.query_params.get("exchange") or "").upper().strip()  # optional: KOSPI/KOSDAQ/NASDAQ

    date_str = (request.query_params.get("date") or "").strip()
    if date_str:
        try:
            y, m, d = map(int, date_str.split("-"))
            target = _date(y, m, d)
        except Exception:
            return Response({"detail": "date must be YYYY-MM-DD"}, status=400)
    else:
        target = timezone.localdate()

    qs = DailyStockSnapshot.objects.filter(stock__market=market, date__lte=target)
    if exchange:
        qs = qs.filter(stock__exchange=exchange)

    asof = qs.order_by("-date").values_list("date", flat=True).first()

    if not asof:
        return Response(
            {
                "market": market,
                "exchange": exchange or None,
                "asof": target.isoformat(),
                "top_market_cap": [],
                "top_gainers": [],
                "top_drawdown": [],
            }
        )

    base_qs = DailyStockSnapshot.objects.select_related("stock").filter(stock__market=market, date=asof)
    if exchange:
        base_qs = base_qs.filter(stock__exchange=exchange)

    top_market_cap = base_qs.exclude(market_cap__isnull=True).order_by("-market_cap")[:5]
    top_gainers = base_qs.exclude(intraday_pct__isnull=True).order_by("-intraday_pct")[:5]
    top_drawdown = base_qs.exclude(intraday_pct__isnull=True).order_by("intraday_pct")[:5]

    return Response(
        {
            "market": market,
            "exchange": exchange or None,
            "asof": asof.isoformat(),
            "top_market_cap": [_serialize_snapshot(x) for x in top_market_cap],
            "top_gainers": [_serialize_snapshot(x) for x in top_gainers],
            "top_drawdown": [_serialize_snapshot(x) for x in top_drawdown],
        }
    )
