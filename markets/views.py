# apps/markets/views.py
from __future__ import annotations

from datetime import date as _date
from typing import Any, Dict, List, Optional

from django.db.models import Q
from django.utils import timezone
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response

from .models import DailyRankingSnapshot, MarketChoices, RankingTypeChoices


def _parse_date_yyyy_mm_dd(date_str: str) -> _date:
    y, m, d = map(int, date_str.split("-"))
    return _date(y, m, d)


def _serialize_ranking(s: DailyRankingSnapshot) -> dict:
    """
    DailyRankingSnapshot 단일 row 직렬화.
    - payload는 기본적으로 포함하지 않음(응답이 커질 수 있음).
    - 필요하면 include_payload=1 옵션으로 포함 가능하게 처리.
    """
    return {
        "rank": int(s.rank),
        "symbol_code": s.symbol_code,
        "name": s.name,
        "trade_price": float(s.trade_price) if s.trade_price is not None else None,
        "change_rate": float(s.change_rate) if s.change_rate is not None else None,
    }


@api_view(["GET"])
def today_rankings(request: Request):
    """
    일별 랭킹 스냅샷 API

    Query Params
    - market: KOSPI | KOSDAQ | NASDAQ (default: KOSPI)
    - date: YYYY-MM-DD (optional, default: today in local timezone)
    - limit: int (optional, default: 5)  # top N
    - include_payload: 0|1 (optional, default: 0)

    Response
    - top_market_cap: ranking_type=MARKET_CAP, rank<=limit
    - top_gainers: ranking_type=RISE, rank<=limit
    - top_drawdown: ranking_type=FALL, rank<=limit
    """
    market = (request.query_params.get("market", MarketChoices.KOSPI) or MarketChoices.KOSPI).upper().strip()
    allowed_markets = {MarketChoices.KOSPI, MarketChoices.KOSDAQ, MarketChoices.NASDAQ}
    if market not in allowed_markets:
        return Response(
            {"detail": "market must be one of KOSPI, KOSDAQ, NASDAQ"},
            status=400,
        )

    # limit
    limit_str = (request.query_params.get("limit") or "").strip()
    try:
        limit = int(limit_str) if limit_str else 5
        if limit <= 0:
            raise ValueError
        limit = min(limit, 200)
    except Exception:
        return Response({"detail": "limit must be a positive integer"}, status=400)

    include_payload = (request.query_params.get("include_payload") or "0").strip() in ("1", "true", "True")

    # target date
    date_str = (request.query_params.get("date") or "").strip()
    if date_str:
        try:
            target = _parse_date_yyyy_mm_dd(date_str)
        except Exception:
            return Response({"detail": "date must be YYYY-MM-DD"}, status=400)
    else:
        target = timezone.localdate()

    # target 이하 중 가장 최신 asof_date 선택
    asof = (
        DailyRankingSnapshot.objects.filter(market=market, asof_date__lte=target)
        .order_by("-asof_date")
        .values_list("asof_date", flat=True)
        .first()
    )

    if not asof:
        return Response(
            {
                "market": market,
                "asof": target.isoformat(),
                "top_market_cap": [],
                "top_gainers": [],
                "top_drawdown": [],
            }
        )

    base_qs = DailyRankingSnapshot.objects.filter(market=market, asof_date=asof)

    def _fetch_top(ranking_type: str) -> list[dict]:
        qs = base_qs.filter(ranking_type=ranking_type, rank__lte=limit).order_by("rank")
        items = []
        for x in qs:
            d = _serialize_ranking(x)
            if include_payload:
                d["payload"] = x.payload
            items.append(d)
        return items

    top_market_cap = _fetch_top(RankingTypeChoices.MARKET_CAP)
    top_gainers = _fetch_top(RankingTypeChoices.RISE)
    top_drawdown = _fetch_top(RankingTypeChoices.FALL)

    return Response(
        {
            "market": market,
            "asof": asof.isoformat(),
            "top_market_cap": top_market_cap,
            "top_gainers": top_gainers,
            "top_drawdown": top_drawdown,
        }
    )


@api_view(["GET"])
def symbol_suggest(request: Request):
    """
    종목명/티커 자동완성 (DailyRankingSnapshot 기반)

    Query Params
    - q: string (required)
    - market: KOSPI | KOSDAQ | NASDAQ | ALL (default: ALL)
    - limit: int (default: 10, max: 50)
    - date: YYYY-MM-DD (optional, default: today in local timezone)
      -> 해당 날짜 이하 중 가장 최신 asof_date를 골라서 그 날짜 데이터로만 suggest

    Response
    {
      "market": "...",
      "asof": "YYYY-MM-DD",
      "results": [
        {"symbol": "AAPL", "name": "Apple Inc.", "market": "NASDAQ"},
        ...
      ]
    }
    """
    q = (request.query_params.get("q") or "").strip()
    if not q:
        return Response({"market": "ALL", "asof": None, "results": []})

    market = (request.query_params.get("market") or "ALL").upper().strip()
    allowed = {MarketChoices.KOSPI, MarketChoices.KOSDAQ, MarketChoices.NASDAQ, "ALL"}
    if market not in allowed:
        return Response({"detail": "market must be one of KOSPI, KOSDAQ, NASDAQ, ALL"}, status=400)

    # limit
    limit_str = (request.query_params.get("limit") or "").strip()
    try:
        limit = int(limit_str) if limit_str else 10
        if limit <= 0:
            raise ValueError
        limit = min(limit, 50)
    except Exception:
        return Response({"detail": "limit must be a positive integer"}, status=400)

    # target date
    date_str = (request.query_params.get("date") or "").strip()
    if date_str:
        try:
            target = _parse_date_yyyy_mm_dd(date_str)
        except Exception:
            return Response({"detail": "date must be YYYY-MM-DD"}, status=400)
    else:
        target = timezone.localdate()

    # asof 선택 (market별/ALL)
    base = DailyRankingSnapshot.objects.filter(asof_date__lte=target)
    if market != "ALL":
        base = base.filter(market=market)

    asof = base.order_by("-asof_date").values_list("asof_date", flat=True).first()
    if not asof:
        return Response({"market": market, "asof": target.isoformat(), "results": []})

    # 검색 대상: 해당 asof의 스냅샷에서만
    qs = DailyRankingSnapshot.objects.filter(asof_date=asof)
    if market != "ALL":
        qs = qs.filter(market=market)

    # q는 symbol_code / name 둘 다에서 검색
    # - symbol_code: "A005930" / "AAPL" 등
    # - name: "삼성전자" / "Apple Inc." 등
    # 대소문자/부분일치
    q_norm = q.strip()
    qs = qs.filter(Q(symbol_code__icontains=q_norm) | Q(name__icontains=q_norm))

    # 최신 랭킹/중복 제거를 위해 symbol_code 기준 distinct 느낌으로
    # (DB에 distinct on이 어려우면, Python에서 후처리)
    qs = qs.order_by("symbol_code", "rank")[:500]

    results: List[Dict[str, Any]] = []
    seen = set()
    for row in qs:
        sym = (row.symbol_code or "").strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        results.append({"symbol": sym, "name": row.name, "market": row.market})
        if len(results) >= limit:
            break

    return Response({"market": market, "asof": asof.isoformat(), "results": results})
