# apps/reco/views.py
from __future__ import annotations

from zoneinfo import ZoneInfo

from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response

from .models import TrendKeywordDaily, TrendScope


@api_view(["GET"])
@permission_classes([AllowAny])
def trend_keywords(request: Request):
    """
    GET /api/reco/keywords/?scope=KR&limit=3&with_news=0&with_content=0

    - scope: KR/US만 허용
    - 오늘 데이터가 있으면 오늘자 반환
    - 없으면 scope 기준 가장 최신 날짜 fallback
    - with_news=1이면 TrendKeywordNews(자식)도 함께 반환
    - with_content=1이면 TrendKeywordNews.content(본문)도 포함
      (payload가 커질 수 있으니 기본은 0)
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
    with_content = (request.query_params.get("with_content") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
    )

    kst = ZoneInfo("Asia/Seoul")
    today = timezone.now().astimezone(kst).date()

    base_qs = TrendKeywordDaily.objects.filter(date=today, scope=scope).order_by("rank")
    if with_news:
        base_qs = base_qs.prefetch_related("news_items")
    qs = base_qs[:limit]

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
            news_rows = []
            for n in x.news_items.all():
                d = {
                    "title": n.title,
                    "summary": n.summary,
                    "link": n.link,
                    "image_url": n.image_url,
                    "published_at": n.published_at,
                    "needs_image_gen": bool(getattr(n, "needs_image_gen", False)),
                }
                if with_content:
                    d["content"] = getattr(n, "content", "") or ""
                news_rows.append(d)

            row["news"] = news_rows

        items.append(row)

    date_str = str(qs[0].date) if items else str(today)
    return Response({"scope": scope, "date": date_str, "items": items})
