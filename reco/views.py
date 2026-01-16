# apps/reco/views.py
from __future__ import annotations

import re
from zoneinfo import ZoneInfo

from django.db.models import Prefetch
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import (
    TrendKeywordDaily,
    TrendScope,
    TrendKeywordNews,
    TrendKeywordNewsAnalysis,
)


# ─────────────────────────────────────────────────────────────
# Utilities (news 앱과 동일 컨셉)
# ─────────────────────────────────────────────────────────────
def _clamp_level(x: int) -> int:
    try:
        x = int(x)
    except Exception:
        return 1
    return max(1, min(5, x))


def _normalize_title(title: str) -> str:
    cleaned = re.sub(r"^[\d\.\s]+", "", title or "")
    cleaned = " ".join(cleaned.split())
    return cleaned[:80]


def _get_user_profile(request: Request):
    if request.user and request.user.is_authenticated:
        try:
            return request.user.profile
        except Exception:
            return None
    return None


def _get_user_level(request: Request) -> int:
    profile = _get_user_profile(request)
    if profile and hasattr(profile, "knowledge_level"):
        return _clamp_level(profile.knowledge_level)
    return 3  # 비로그인 기본


def _resolve_scope(raw: str | None) -> str:
    scope = (raw or TrendScope.KR).upper().strip()
    if scope not in (TrendScope.KR, TrendScope.US):
        scope = TrendScope.KR
    return scope


def _kst_today():
    kst = ZoneInfo("Asia/Seoul")
    return timezone.now().astimezone(kst).date()


def _latest_date_for_scope(scope: str):
    """
    오늘 데이터가 있으면 오늘, 없으면 scope 기준 최신 날짜 fallback
    """
    today = _kst_today()
    if TrendKeywordDaily.objects.filter(scope=scope, date=today).exists():
        return today

    latest = (
        TrendKeywordDaily.objects.filter(scope=scope)
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )
    return latest or today


def _prefetch_level_analysis(user_level: int):
    """
    TrendKeywordNewsAnalysis.related_name = "analyses" (모델에서 확정)
    """
    return Prefetch(
        "analyses",
        queryset=TrendKeywordNewsAnalysis.objects.filter(level=user_level),
        to_attr="_lv_analysis",
    )


def _pick_summary_and_tags_from_analysis(n: TrendKeywordNews) -> tuple[str, list[str] | None]:
    """
    요구사항: summary는 크롤링/수집 summary(n.summary)가 아니라,
    analyze_trend_news.py가 저장해 둔 TrendKeywordNewsAnalysis.analysis["summary"]에서 가져온다.

    반환:
      - summary: 분석 summary (없으면 빈 문자열)
      - tags: 분석 keywords (있으면 최대 2개), 없으면 None (caller에서 fallback 처리)
    """
    lv_analysis = None
    if hasattr(n, "_lv_analysis") and n._lv_analysis:
        lv_analysis = n._lv_analysis[0]

    if not lv_analysis or not isinstance(lv_analysis.analysis, dict):
        return "", None

    a = lv_analysis.analysis
    s = (a.get("summary") or "").strip()

    kws = a.get("keywords") or []
    tags: list[str] = []
    if isinstance(kws, list):
        tags = [str(x) for x in kws[:2] if str(x).strip()]

    return s, (tags if tags else None)


def _build_trend_news_list_payload(*, qs, user_level: int, limit: int):
    """
    공통: TrendKeywordNews queryset -> payload
    - summary는 "분석 summary만" 사용 (없으면 빈 문자열/고정문구 처리)
    """
    qs = qs.order_by("-created_at").prefetch_related(_prefetch_level_analysis(user_level))[:limit]

    out = []
    for n in qs:
        summary, tags = _pick_summary_and_tags_from_analysis(n)

        if not summary:
            # 정책: 크롤링 summary로 fallback 금지.
            summary = "분석 데이터가 아직 준비되지 않았습니다."

        if not tags:
            tags = ["트렌드"]

        out.append(
            {
                "id": n.id,
                "title": n.title,
                "summary": summary,
                "tags": tags,
                "published_at": n.published_at,
                "url": n.link,  # 프론트 통일
                "image_url": n.image_url,
                "needs_image_gen": bool(n.needs_image_gen),
                "level": user_level,
            }
        )
    return out


# ─────────────────────────────────────────────────────────────
# keywords (기존 + with_news=1일 때도 분석 summary 제공하도록 개선)
# ─────────────────────────────────────────────────────────────
@api_view(["GET"])
@permission_classes([AllowAny])
def trend_keywords(request: Request):
    """
    GET /api/reco/keywords/?scope=KR&limit=3&with_news=0

    with_news=1이면:
      - 예전: TrendKeywordNews.summary(수집)만 반환
      - 변경: user_level에 맞는 TrendKeywordNewsAnalysis.analysis["summary"]를 우선/강제 반환
    """
    scope = _resolve_scope(request.query_params.get("scope"))

    try:
        limit = int((request.query_params.get("limit") or "3").strip())
    except Exception:
        limit = 3
    limit = max(1, min(5, limit))

    with_news = (request.query_params.get("with_news") or "0").strip().lower() in ("1", "true", "yes", "y")

    date = _latest_date_for_scope(scope)

    user_level = _get_user_level(request)

    base_qs = TrendKeywordDaily.objects.filter(date=date, scope=scope).order_by("rank")
    if with_news:
        # news_items에 level 분석 prefetch까지 같이
        base_qs = base_qs.prefetch_related(
            Prefetch(
                "news_items",
                queryset=TrendKeywordNews.objects.all().prefetch_related(_prefetch_level_analysis(user_level)),
            )
        )

    qs = base_qs[:limit]

    items = []
    for x in qs:
        row = {"keyword": x.keyword, "reason": x.reason}

        if with_news:
            news_rows = []
            for n in x.news_items.all():
                summary, _tags = _pick_summary_and_tags_from_analysis(n)
                if not summary:
                    summary = "분석 데이터가 아직 준비되지 않았습니다."

                news_rows.append(
                    {
                        "id": n.id,
                        "title": n.title,
                        "summary": summary,
                        "link": n.link,
                        "image_url": n.image_url,
                        "published_at": n.published_at,
                        "needs_image_gen": bool(getattr(n, "needs_image_gen", False)),
                        "level": user_level,
                    }
                )
            row["news"] = news_rows

        items.append(row)

    return Response({"scope": scope, "date": str(date), "items": items, "level": user_level})


# ─────────────────────────────────────────────────────────────
# ai-recommend (분석 "조회만", summary는 분석에서만)
# ─────────────────────────────────────────────────────────────
class TrendNewsRecommendView(APIView):
    """
    GET /api/recommend/ai-recommend/?scope=KR&limit=20&keyword_limit=3

    - TrendKeywordDaily(rank) 상위 keyword_limit개에서 news_items로 추천 구성
    - summary는 TrendKeywordNewsAnalysis.analysis["summary"]에서만 가져옴 (크롤링 summary 사용 금지)
    """
    permission_classes = [AllowAny]

    def get(self, request: Request):
        scope = _resolve_scope(request.query_params.get("scope"))
        user_level = _get_user_level(request)

        try:
            limit = int((request.query_params.get("limit") or "20").strip())
        except Exception:
            limit = 20
        limit = max(1, min(50, limit))

        try:
            keyword_limit = int((request.query_params.get("keyword_limit") or "3").strip())
        except Exception:
            keyword_limit = 3
        keyword_limit = max(1, min(5, keyword_limit))

        date = _latest_date_for_scope(scope)

        kw_qs = (
            TrendKeywordDaily.objects.filter(scope=scope, date=date)
            .order_by("rank")
            .prefetch_related("news_items")[:keyword_limit]
        )

        picked_ids: list[int] = []
        id_to_keyword: dict[int, str] = {}
        seen_titles: set[str] = set()

        for kw in kw_qs:
            for n in kw.news_items.all():
                tkey = _normalize_title(n.title)
                if not tkey or tkey in seen_titles:
                    continue
                seen_titles.add(tkey)

                picked_ids.append(n.id)
                id_to_keyword[n.id] = kw.keyword

                if len(picked_ids) >= limit:
                    break
            if len(picked_ids) >= limit:
                break

        if not picked_ids:
            return Response({"scope": scope, "date": str(date), "level": user_level, "news": [], "keywords": []})

        base_news = (
            TrendKeywordNews.objects.filter(id__in=picked_ids)
            .prefetch_related(_prefetch_level_analysis(user_level))
        )
        by_id = {x.id: x for x in base_news}

        final = []
        for nid in picked_ids:
            n = by_id.get(nid)
            if not n:
                continue

            summary, tags = _pick_summary_and_tags_from_analysis(n)
            if not summary:
                summary = "분석 데이터가 아직 준비되지 않았습니다."

            if not tags:
                kw = id_to_keyword.get(n.id)
                tags = [kw] if kw else ["트렌드"]

            final.append(
                {
                    "id": n.id,
                    "title": n.title,
                    "summary": summary,
                    "tags": tags,
                    "published_at": n.published_at,
                    "url": n.link,
                    "image_url": n.image_url,
                    "needs_image_gen": bool(n.needs_image_gen),
                    "level": user_level,
                }
            )

        keywords = [f"#{kw.keyword}" for kw in kw_qs]
        keywords = list(dict.fromkeys(keywords))[:4]

        return Response(
            {
                "scope": scope,
                "date": str(date),
                "level": user_level,
                "news": final,
                "keywords": keywords,
            }
        )


# ─────────────────────────────────────────────────────────────
# summary (분석 "조회만")
# ─────────────────────────────────────────────────────────────
class TrendNewsSummaryView(APIView):
    """
    GET /api/recommend/<id>/summary/
    - TrendKeywordNewsAnalysis(level)에서 analysis 그대로 반환
    """
    permission_classes = [AllowAny]

    def get(self, request: Request, news_id: int):
        try:
            item = TrendKeywordNews.objects.get(id=news_id)
        except TrendKeywordNews.DoesNotExist:
            return Response({"error": "뉴스를 찾을 수 없습니다."}, status=404)

        user_level = _get_user_level(request)

        row = TrendKeywordNewsAnalysis.objects.filter(news=item, level=user_level).first()
        if not row or not isinstance(row.analysis, dict):
            return Response({"error": "해당 레벨 분석 데이터가 없습니다."}, status=404)

        final_analysis = row.analysis.copy()

        if "action_guide" in final_analysis and "investment_action" not in final_analysis:
            ag = final_analysis.get("action_guide")
            final_analysis["investment_action"] = [ag] if isinstance(ag, str) else (ag or [])

        if "strategy_guide" not in final_analysis or not final_analysis["strategy_guide"]:
            final_analysis["strategy_guide"] = {
                "short_term": "분석 데이터가 충분하지 않습니다.",
                "long_term": "추후 업데이트 될 예정입니다.",
            }

        return Response(
            {
                "success": True,
                "news_id": item.id,
                "title": item.title,
                "level": user_level,
                "analysis": final_analysis,
            }
        )
