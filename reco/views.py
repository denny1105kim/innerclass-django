# apps/reco/views.py
from __future__ import annotations

from zoneinfo import ZoneInfo

from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny
from rest_framework.request import Request
from rest_framework.response import Response

from .models import ThemePickDaily, ThemeScope, TrendKeywordDaily


@api_view(["GET"])
@permission_classes([AllowAny])  # 홈에서 토큰 없이도 호출 가능
def recommend_themes(request: Request):
    """
    GET /api/reco/themes/?scope=ALL&limit=3

    - 오늘 데이터가 있으면 오늘자 반환
    - 없으면 scope 기준 가장 최신 날짜 fallback
    """
    scope = (request.query_params.get("scope") or ThemeScope.ALL).upper().strip()
    if scope not in (ThemeScope.ALL, ThemeScope.KR, ThemeScope.US):
        scope = ThemeScope.ALL

    try:
        limit = int((request.query_params.get("limit") or "3").strip())
    except Exception:
        limit = 3
    limit = max(1, min(5, limit))

    kst = ZoneInfo("Asia/Seoul")
    today = timezone.now().astimezone(kst).date()

    qs = ThemePickDaily.objects.filter(date=today, scope=scope).order_by("rank")[:limit]

    # 오늘 데이터가 없다면 "가장 최신 날짜" fallback
    if not qs.exists():
        latest = (
            ThemePickDaily.objects.filter(scope=scope)
            .order_by("-date")
            .values_list("date", flat=True)
            .first()
        )
        if latest:
            qs = ThemePickDaily.objects.filter(date=latest, scope=scope).order_by("rank")[:limit]

    items = [{"theme": x.theme, "symbol": x.symbol, "name": x.name, "reason": x.reason} for x in qs]
    date_str = str(qs[0].date) if items else str(today)

    return Response({"scope": scope, "date": date_str, "items": items})


@api_view(["GET"])
@permission_classes([AllowAny])  # 홈에서 토큰 없이도 호출 가능
def trend_keywords(request: Request):
    """
    GET /api/reco/keywords/?scope=ALL&limit=3

    - 오늘 데이터가 있으면 오늘자 반환
    - 없으면 scope 기준 가장 최신 날짜 fallback
    """
    scope = (request.query_params.get("scope") or ThemeScope.ALL).upper().strip()
    if scope not in (ThemeScope.ALL, ThemeScope.KR, ThemeScope.US):
        scope = ThemeScope.ALL

    try:
        limit = int((request.query_params.get("limit") or "3").strip())
    except Exception:
        limit = 3
    limit = max(1, min(5, limit))

    kst = ZoneInfo("Asia/Seoul")
    today = timezone.now().astimezone(kst).date()

    qs = TrendKeywordDaily.objects.filter(date=today, scope=scope).order_by("rank")[:limit]

    # 오늘 데이터가 없다면 "가장 최신 날짜" fallback
    if not qs.exists():
        latest = (
            TrendKeywordDaily.objects.filter(scope=scope)
            .order_by("-date")
            .values_list("date", flat=True)
            .first()
        )
        if latest:
            qs = TrendKeywordDaily.objects.filter(date=latest, scope=scope).order_by("rank")[:limit]

    items = [{"keyword": x.keyword, "reason": x.reason} for x in qs]
    date_str = str(qs[0].date) if items else str(today)

    return Response({"scope": scope, "date": date_str, "items": items})
