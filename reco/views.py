from __future__ import annotations

from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import TrendKeywordDaily, TrendScope, TrendKeywordNews
from .services.analyze_trend_news import analyze_trend_news


@api_view(["GET"])
@permission_classes([AllowAny])  # 홈에서 토큰 없이도 호출 가능
def trend_keywords(request: Request):
    """
    GET /api/reco/keywords/?scope=KR&limit=3&with_news=0

    - scope: KR/US만 허용
    - 오늘 데이터가 있으면 오늘자 반환
    - 없으면 scope 기준 가장 최신 날짜 fallback
    - with_news=1이면 TrendKeywordNews(자식)도 함께 반환
    """
    scope = (request.query_params.get("scope") or TrendScope.KR).upper().strip()
    if scope not in (TrendScope.KR, TrendScope.US):
        scope = TrendScope.KR

    try:
        limit = int((request.query_params.get("limit") or "3").strip())
    except Exception:
        limit = 3
    limit = max(1, min(5, limit))

    with_news = (request.query_params.get("with_news") or "0").strip().lower() in ("1", "true", "yes", "y")

    kst = ZoneInfo("Asia/Seoul")
    today = timezone.now().astimezone(kst).date()

    base_qs = TrendKeywordDaily.objects.filter(date=today, scope=scope).order_by("rank")
    if with_news:
        base_qs = base_qs.prefetch_related("news_items")
    qs = base_qs[:limit]

    # 오늘 데이터가 없다면 "가장 최신 날짜" fallback
    if not qs.exists():
        latest = (
            TrendKeywordDaily.objects.filter(scope=scope)
            .order_by("-date")
            .values_list("date", flat=True)
            .first()
        )
        if latest:
            base_qs = TrendKeywordDaily.objects.filter(date=latest, scope=scope).order_by("rank")
            if with_news:
                base_qs = base_qs.prefetch_related("news_items")
            qs = base_qs[:limit]

    items = []
    for x in qs:
        row = {"keyword": x.keyword, "reason": x.reason}

        if with_news:
            row["news"] = [
                {
                    "id": n.id,  # ✅ trend 뉴스도 id 내려줌(모달 분석 호출 가능)
                    "title": n.title,
                    "summary": n.summary,
                    "link": n.link,
                    "image_url": n.image_url,
                    "published_at": n.published_at,
                    "needs_image_gen": bool(getattr(n, "needs_image_gen", False)),
                }
                for n in x.news_items.all()
            ]

        items.append(row)

    date_str = str(qs[0].date) if items else str(today)
    return Response({"scope": scope, "date": date_str, "items": items})


# =========================================================
# helpers: analysis normalize (flat schema)
# =========================================================
def _as_list(v: Any) -> list:
    return v if isinstance(v, list) else []


def _coalesce_list(d: Dict[str, Any], key: str) -> list:
    v = d.get(key)
    return v if isinstance(v, list) else []


def _normalize_analysis_payload(raw: Any) -> Optional[Dict[str, Any]]:
    """
    프론트(NewsInsightModal)가 기대하는 flat schema로 정규화.

    허용 입력 형태 예:
    1) flat:
       {"bullet_points":[...], "what_is_this":[...], ...}

    2) nested (news main-summary에서 보던 형태):
       {
         "analysis": {
           "summary": "...",
           "bullet_points":[...],
           ...
         },
         "keywords":[...],
         "vocabulary":[...]
       }

    3) 더 깊이:
       {"analysis": {"analysis": {...}, "vocabulary":[...]}}
    """
    if not raw or not isinstance(raw, dict):
        return None

    # 0) 이미 flat인지 먼저 확인
    if any(k in raw for k in ("bullet_points", "what_is_this", "why_important", "stock_impact", "investment_action")):
        flat = dict(raw)
    else:
        # 1) raw.get("analysis")가 dict면 그걸 1차로 본다
        a = raw.get("analysis") if isinstance(raw.get("analysis"), dict) else raw

        # 2) a 안에 analysis가 또 있으면 (analysis.analysis) 언랩
        if isinstance(a, dict) and isinstance(a.get("analysis"), dict) and any(
            k in a["analysis"] for k in ("bullet_points", "what_is_this", "why_important", "stock_impact", "investment_action")
        ):
            core = a["analysis"]
            flat = dict(core)

            # vocabulary가 바깥(a)에 있을 수 있으니 병합
            if "vocabulary" not in flat and isinstance(a.get("vocabulary"), list):
                flat["vocabulary"] = a.get("vocabulary")
        else:
            # 3) a 자체가 core일 수도 있음
            if isinstance(a, dict):
                flat = dict(a)
            else:
                return None

        # 4) raw(최상위)에 vocabulary가 있고 flat에 없으면 병합
        if "vocabulary" not in flat and isinstance(raw.get("vocabulary"), list):
            flat["vocabulary"] = raw.get("vocabulary")

    # -------------------------
    # ✅ flat schema 강제 보정(빈값 기본값 채움)
    # -------------------------
    flat.setdefault("bullet_points", [])
    flat.setdefault("what_is_this", [])
    flat.setdefault("why_important", [])
    flat.setdefault("investment_action", [])
    flat.setdefault("vocabulary", [])

    if not isinstance(flat.get("bullet_points"), list):
        flat["bullet_points"] = []
    if not isinstance(flat.get("what_is_this"), list):
        flat["what_is_this"] = []
    if not isinstance(flat.get("why_important"), list):
        flat["why_important"] = []
    if not isinstance(flat.get("investment_action"), list):
        # action_guide(문자열)만 있는 경우를 investment_action으로 승격
        ag = flat.get("action_guide")
        if isinstance(ag, str) and ag.strip():
            flat["investment_action"] = [ag.strip()]
        else:
            flat["investment_action"] = []

    # stock_impact 구조 보정
    si = flat.get("stock_impact")
    if not isinstance(si, dict):
        si = {}
    si.setdefault("positives", [])
    si.setdefault("warnings", [])
    if not isinstance(si.get("positives"), list):
        si["positives"] = []
    if not isinstance(si.get("warnings"), list):
        si["warnings"] = []
    flat["stock_impact"] = si

    # strategy_guide 구조 보정
    sg = flat.get("strategy_guide")
    if not isinstance(sg, dict):
        sg = {"short_term": "정보 없음", "long_term": "정보 없음"}
    else:
        sg.setdefault("short_term", sg.get("short_term") or "정보 없음")
        sg.setdefault("long_term", sg.get("long_term") or "정보 없음")
    flat["strategy_guide"] = sg

    # vocabulary 항목 보정( {term, definition} )
    vocab = flat.get("vocabulary")
    if not isinstance(vocab, list):
        vocab = []
    norm_vocab = []
    for x in vocab:
        if not isinstance(x, dict):
            continue
        term = str(x.get("term") or "").strip()
        definition = str(x.get("definition") or "").strip()
        if term:
            norm_vocab.append({"term": term, "definition": definition})
    flat["vocabulary"] = norm_vocab

    return flat


# =========================================================
# (NEW) reco Trend news detail summary view (LLM) - news.NewsSummaryView style
# =========================================================
class TrendNewsSummaryView(APIView):
    """
    GET /api/reco/news/<id>/summary/
    - TrendKeywordNews.analysis 없으면 analyze_trend_news로 생성 후 저장
    - news.NewsSummaryView와 동일한 "analysis 캐시" 패턴
    """
    permission_classes = [AllowAny]

    def get(self, request, news_id: int):
        try:
            item = TrendKeywordNews.objects.select_related("trend").get(id=news_id)
        except TrendKeywordNews.DoesNotExist:
            return Response({"error": "트렌드 뉴스를 찾을 수 없습니다."}, status=404)

        analysis_data = getattr(item, "analysis", None)

        if not analysis_data:
            analysis_data = analyze_trend_news(item, save_to_db=True)

        flat = _normalize_analysis_payload(analysis_data)
        if not flat:
            return Response({"error": "분석에 실패했습니다."}, status=500)

        return Response(
            {
                "success": True,
                "trend_news_id": item.id,
                "trend_scope": item.trend.scope,
                "trend_date": str(item.trend.date),
                "article_title": item.title,
                "analysis": flat,
            }
        )
