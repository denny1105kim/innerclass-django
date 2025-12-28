from datetime import date as _date

from django.http import JsonResponse
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.utils import timezone

from .models import DailyStockSnapshot, Market


def _serialize_snapshot(s: DailyStockSnapshot) -> dict:
    return {
        "symbol": s.stock.symbol,
        "name": s.stock.name,
        "exchange": s.stock.exchange,
        "currency": s.stock.currency,
        "open": float(s.open) if s.open is not None else None,
        "close": float(s.close) if s.close is not None else None,
        # for UI convenience
        "intraday_pct": float(s.intraday_pct) if s.intraday_pct is not None else None,
        "change_pct": float(s.change_pct) if s.change_pct is not None else None,
        "market_cap": s.market_cap,
        "volume": s.volume,
        "date": s.date.isoformat(),
    }


@api_view(["GET"])
def today_market(request):
    market = request.query_params.get("market", "KR").upper()
    if market not in (Market.KR, Market.US):
        return Response({"detail": "market must be KR or US"}, status=400)

    # 1) 사용자가 date를 주면 그 날짜를 기준으로,
    #    없으면 "오늘"을 기준으로 한다.
    date_str = request.query_params.get("date")
    if date_str:
        try:
            y, m, d = map(int, date_str.split("-"))
            target = _date(y, m, d)
        except Exception:
            return Response({"detail": "date must be YYYY-MM-DD"}, status=400)
    else:
        target = timezone.localdate()

    # 2) target(오늘 또는 지정일) 이하에서 DB에 존재하는 가장 최신 거래일을 찾는다.
    asof = (
        DailyStockSnapshot.objects
        .filter(stock__market=market, date__lte=target)
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    if not asof:
        return Response({
            "market": market,
            "asof": target.isoformat(),
            "top_market_cap": [],
            "top_drawdown": [],
        })

    base_qs = (
        DailyStockSnapshot.objects
        .select_related("stock")
        .filter(stock__market=market, date=asof)
    )

    top_market_cap = base_qs.exclude(market_cap__isnull=True).order_by("-market_cap")[:5]
    top_drawdown = base_qs.exclude(intraday_pct__isnull=True).order_by("intraday_pct")[:5]

    return Response({
        "market": market,
        "asof": asof.isoformat(),
        "top_market_cap": [_serialize_snapshot(x) for x in top_market_cap],
        "top_drawdown": [_serialize_snapshot(x) for x in top_drawdown],
    })
