# apps/markets/services/daily_rank_sync.py
from __future__ import annotations

from datetime import date
from typing import Any, Dict, Iterable, Optional, List

from django.db import transaction

from markets.models import DailyRankingSnapshot, MarketChoices, RankingTypeChoices
from markets.services.finance import DaumFinanceClient, SlickChartsNasdaq100Client


# -------------------------------------------------
# helpers
# -------------------------------------------------
def _extract_symbol_code(row: Dict[str, Any]) -> str:
    return (row.get("symbolCode") or row.get("symbol") or row.get("code") or "").strip()


def _extract_name(row: Dict[str, Any]) -> str:
    return (row.get("name") or row.get("stockName") or "").strip()


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _normalize_change_rate(*, market: str, row: Dict[str, Any]) -> Optional[float]:
    """
    change_rate는 "퍼센트 포인트"로 저장한다.
      -14.19 == -14.19%
      +10.8  == +10.8%

    KR(Daum):
      - 어떤 날은 ratio(0.1419)로 오고, 어떤 날은 percent(14.19)로 옴 -> |x|<=1.5면 *100

    NASDAQ(SlickCharts):
      - changeRate는 이미 percent이며, 부호(±)가 방향을 의미함 -> 절대값/부호강제 금지
    """
    raw = _to_float(row.get("changeRate"))
    if raw is None:
        return None

    if market in (MarketChoices.KOSPI, MarketChoices.KOSDAQ):
        if abs(raw) <= 1.5:
            raw = raw * 100.0
        return raw

    # NASDAQ: 그대로(부호 유지)
    return raw


def _row_to_defaults(*, market: str, row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol_code": _extract_symbol_code(row),
        "name": _extract_name(row),
        "trade_price": row.get("tradePrice"),
        "change_rate": _normalize_change_rate(market=market, row=row),
        "payload": row,
    }


def _sort_rows(*, ranking_type: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    _norm_cr 기준으로 정렬:
    - RISE: desc
    - FALL: asc
    - MARKET_CAP: marketCap desc
    """
    if ranking_type == RankingTypeChoices.MARKET_CAP:
        return sorted(
            rows,
            key=lambda r: (r.get("marketCap") is None, -(r.get("marketCap") or 0)),
        )

    if ranking_type == RankingTypeChoices.RISE:
        return sorted(
            rows,
            key=lambda r: (r.get("_norm_cr") is None, -(r.get("_norm_cr") or 0.0)),
        )

    if ranking_type == RankingTypeChoices.FALL:
        return sorted(
            rows,
            key=lambda r: (r.get("_norm_cr") is None, (r.get("_norm_cr") or 0.0)),
        )

    return rows


def _filter_rows_for_type(*, market: str, ranking_type: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    ✅ NASDAQ는 rise/fall을 sign 기준으로 필터링한다.
    (KR은 Daum API가 이미 RISE/FALL 분리된 데이터라 그냥 통과)
    """
    if market != MarketChoices.NASDAQ:
        return rows

    if ranking_type == RankingTypeChoices.RISE:
        return [r for r in rows if (r.get("_norm_cr") is not None and r["_norm_cr"] > 0)]
    if ranking_type == RankingTypeChoices.FALL:
        return [r for r in rows if (r.get("_norm_cr") is not None and r["_norm_cr"] < 0)]

    return rows


# -------------------------------------------------
# core
# -------------------------------------------------
@transaction.atomic
def replace_ranking(*, asof: date, market: str, ranking_type: str, rows: Iterable[Dict[str, Any]]) -> int:
    DailyRankingSnapshot.objects.filter(
        asof_date=asof,
        market=market,
        ranking_type=ranking_type,
    ).delete()

    src = list(rows)

    # 정렬/필터용 norm 값 주입
    for r in src:
        r["_norm_cr"] = _normalize_change_rate(market=market, row=r)

    # ✅ NASDAQ rise/fall은 sign으로 필터
    src = _filter_rows_for_type(market=market, ranking_type=ranking_type, rows=src)

    # 정렬
    src = _sort_rows(ranking_type=ranking_type, rows=src)

    objs: List[DailyRankingSnapshot] = []
    for idx, row in enumerate(src, start=1):
        defaults = _row_to_defaults(market=market, row=row)

        if not defaults["symbol_code"] or not defaults["name"]:
            continue

        objs.append(
            DailyRankingSnapshot(
                asof_date=asof,
                market=market,
                ranking_type=ranking_type,
                rank=idx,
                **defaults,
            )
        )

    DailyRankingSnapshot.objects.bulk_create(objs, batch_size=500)
    return len(objs)


def sync_daily_rankings(*, asof: Optional[date] = None, per_page: int = 200) -> Dict[str, int]:
    asof = asof or date.today()

    daum = DaumFinanceClient()
    slick = SlickChartsNasdaq100Client()

    results: Dict[str, int] = {}

    # ----------------------------
    # KR
    # ----------------------------
    for market in (MarketChoices.KOSPI, MarketChoices.KOSDAQ):
        cap = daum.get_market_cap(market=market, page=1, per_page=per_page)
        results[f"{market}.MARKET_CAP"] = replace_ranking(
            asof=asof, market=market, ranking_type=RankingTypeChoices.MARKET_CAP, rows=cap.data
        )

        rise = daum.get_price_performance(market=market, change_type="RISE", page=1, per_page=per_page)
        results[f"{market}.RISE"] = replace_ranking(
            asof=asof, market=market, ranking_type=RankingTypeChoices.RISE, rows=rise.data
        )

        fall = daum.get_price_performance(market=market, change_type="FALL", page=1, per_page=per_page)
        results[f"{market}.FALL"] = replace_ranking(
            asof=asof, market=market, ranking_type=RankingTypeChoices.FALL, rows=fall.data
        )

    # ----------------------------
    # NASDAQ
    # ----------------------------
    market = MarketChoices.NASDAQ
    try:
        nas_cap = slick.get_nasdaq_market_cap(per_page=per_page)
        results[f"{market}.MARKET_CAP"] = replace_ranking(
            asof=asof, market=market, ranking_type=RankingTypeChoices.MARKET_CAP, rows=nas_cap.data
        )

        nas_rise = slick.get_nasdaq_rise(per_page=per_page)
        results[f"{market}.RISE"] = replace_ranking(
            asof=asof, market=market, ranking_type=RankingTypeChoices.RISE, rows=nas_rise.data
        )

        nas_fall = slick.get_nasdaq_fall(per_page=per_page)
        results[f"{market}.FALL"] = replace_ranking(
            asof=asof, market=market, ranking_type=RankingTypeChoices.FALL, rows=nas_fall.data
        )

    except Exception as e:
        print(f"[NASDAQ] sync skipped due to error: {e!r}")
        results[f"{market}.ERROR"] = 0

    return results
