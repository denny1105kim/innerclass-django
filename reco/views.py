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
                    "id": n.id,  #
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
# helpers: analysis normalize (NEW main-summary schema)
# =========================================================
def _as_list(v: Any) -> list:
    return v if isinstance(v, list) else []


def _normalize_vocabulary(v: Any) -> list:
    """
    vocabulary 항목 보정: [{term, definition}, ...]
    """
    if not isinstance(v, list):
        return []
    out = []
    for x in v:
        if not isinstance(x, dict):
            continue
        term = str(x.get("term") or "").strip()
        definition = str(x.get("definition") or "").strip()
        if term:
            out.append({"term": term, "definition": definition})
    return out


def _normalize_stock_impact(v: Any) -> Dict[str, list]:
    """
    stock_impact 보정: {"positives": [...], "warnings": [...]}
    """
    if not isinstance(v, dict):
        v = {}
    positives = v.get("positives")
    warnings = v.get("warnings")
    return {
        "positives": positives if isinstance(positives, list) else [],
        "warnings": warnings if isinstance(warnings, list) else [],
    }


def _coerce_int_0_100(v: Any, default: int = 50) -> int:
    try:
        x = int(float(v))
    except Exception:
        return default
    return max(0, min(100, x))


def _normalize_analysis_payload(raw: Any) -> Optional[Dict[str, Any]]:
    """
    프론트(모달)가 사용할 수 있도록, '아까 만든 main-summary 스키마'에 맞춘 flat payload로 정규화.

    목표 출력(flat):
    {
      "keywords": [...],
      "sentiment_score": 0~100,
      "vocabulary": [{term, definition}, ...],
      "analysis": {
        "body_summary_30pct": "...",
        "summary": "...",
        "bullet_points": [...],
        "what_is_this": [...],
        "why_important": [...],
        "stock_impact": {"positives":[...], "warnings":[...]}
      }
    }

    허용 입력 형태(최대한 복구):
    1) new schema:
       {
         "keywords":[...],
         "sentiment_score": 75,
         "vocabulary":[...],
         "analysis": {...}
       }

    2) flat(core만):
       {"summary":"...", "bullet_points":[...], ...}

    3) nested:
       {"analysis": {"summary":"...", ...}, "vocabulary":[...], ...}

    4) 더 깊이:
       {"analysis": {"analysis": {...}, "vocabulary":[...]}}
    """
    if not raw or not isinstance(raw, dict):
        return None

    # -------------------------
    # 1) core analysis dict 찾기
    # -------------------------
    core: Dict[str, Any] = {}
    vocab_src: Any = None

    # new schema 형태(상위에 analysis dict 존재)
    if isinstance(raw.get("analysis"), dict) and any(
        k in raw["analysis"] for k in ("summary", "bullet_points", "what_is_this", "why_important", "stock_impact", "body_summary_30pct")
    ):
        core = raw["analysis"]
        vocab_src = raw.get("vocabulary")

    else:
        # raw 자체가 core일 수도(legacy flat)
        if any(k in raw for k in ("summary", "bullet_points", "what_is_this", "why_important", "stock_impact", "body_summary_30pct")):
            core = raw
            vocab_src = raw.get("vocabulary")
        else:
            # nested unwrap: a = raw["analysis"] or raw
            a = raw.get("analysis") if isinstance(raw.get("analysis"), dict) else raw

            # analysis.analysis 형태 언랩
            if isinstance(a, dict) and isinstance(a.get("analysis"), dict) and any(
                k in a["analysis"] for k in ("summary", "bullet_points", "what_is_this", "why_important", "stock_impact", "body_summary_30pct")
            ):
                core = a["analysis"]
                vocab_src = a.get("vocabulary") or raw.get("vocabulary")
            elif isinstance(a, dict):
                core = a
                vocab_src = a.get("vocabulary") or raw.get("vocabulary")
            else:
                return None

    # -------------------------
    # 2) 상위 메타(키워드/감성/용어) 정규화
    # -------------------------
    keywords = raw.get("keywords")
    if not isinstance(keywords, list):
        # legacy: comma string
        if isinstance(keywords, str) and keywords.strip():
            keywords = [s.strip() for s in keywords.split(",") if s.strip()]
        else:
            keywords = []

    sentiment_score = _coerce_int_0_100(raw.get("sentiment_score"), default=50)
    vocabulary = _normalize_vocabulary(vocab_src)

    # -------------------------
    # 3) core analysis 정규화
    # -------------------------
    analysis = {
        "body_summary_30pct": str(core.get("body_summary_30pct") or "").strip(),
        "summary": str(core.get("summary") or "").strip(),
        "bullet_points": _as_list(core.get("bullet_points")),
        "what_is_this": _as_list(core.get("what_is_this")),
        "why_important": _as_list(core.get("why_important")),
        "stock_impact": _normalize_stock_impact(core.get("stock_impact")),
    }

    # 빈값 기본 보정(항상 키가 존재하도록)
    analysis.setdefault("body_summary_30pct", "")
    analysis.setdefault("summary", "")
    analysis.setdefault("bullet_points", [])
    analysis.setdefault("what_is_this", [])
    analysis.setdefault("why_important", [])
    analysis.setdefault("stock_impact", {"positives": [], "warnings": []})

    return {
        "keywords": keywords,
        "sentiment_score": sentiment_score,
        "vocabulary": vocabulary,
        "analysis": analysis,
    }


# =========================================================
# (NEW) reco Trend news detail summary view (LLM)
# =========================================================
class TrendNewsSummaryView(APIView):
    """
    GET /api/reco/news/<id>/summary/
    - TrendKeywordNews.analysis 없으면 analyze_trend_news로 생성 후 저장
    - main-summary 스키마(아까 만든 형태)로 정규화해서 반환
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

        normalized = _normalize_analysis_payload(analysis_data)
        if not normalized:
            return Response({"error": "분석에 실패했습니다."}, status=500)

        return Response(
            {
                "success": True,
                "trend_news_id": item.id,
                "trend_scope": item.trend.scope,
                "trend_date": str(item.trend.date),
                "article_title": item.title,
                "analysis": normalized,
            }
        )
