from __future__ import annotations

import re
import openai
from django.conf import settings
from django.db.models import Prefetch
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from pgvector.django import CosineDistance

from .models import (
    NewsArticle,
    NewsArticleAnalysis,
    NewsTheme,
    NewsMarket,
)
from .services.analyze_news import analyze_news


def get_embedding(text: str):
    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding


def _clamp_level(x: int) -> int:
    try:
        x = int(x)
    except Exception:
        return 3
    return max(1, min(5, x))


def _normalize_title(title: str) -> str:
    cleaned = re.sub(r"^[\d\.\s]+", "", title or "")
    cleaned = " ".join(cleaned.split())
    return cleaned[:80]


def _market_filter_qs(base_qs, market_filter: str):
    mf = (market_filter or "all").strip().lower()
    if mf == "domestic":
        return base_qs.filter(market=NewsMarket.KOREA)
    if mf == "international":
        return base_qs.filter(market=NewsMarket.INTERNATIONAL)
    return base_qs.filter(market__in=[NewsMarket.KOREA, NewsMarket.INTERNATIONAL])


def _get_user_profile(request):
    if request.user.is_authenticated:
        try:
            return request.user.profile
        except Exception:
            return None
    return None


def _get_user_level(request) -> int:
    profile = _get_user_profile(request)
    if profile and hasattr(profile, "knowledge_level"):
        return _clamp_level(profile.knowledge_level)
    return 3


def _build_news_list_payload(*, request, qs, user_level: int, limit: int):
    """
    공통: NewsArticle queryset을 받아 최신순 리스트 payload로 변환
    - 분석 summary/keywords는 user_level의 NewsArticleAnalysis에서 가져옴
    - tags: keywords 최대 2개, 없으면 sector/ticker fallback
    """
    qs = qs.order_by("-published_at")

    # user_level 분석 prefetch
    qs = qs.prefetch_related(
        Prefetch(
            "analyses",
            queryset=NewsArticleAnalysis.objects.filter(level=user_level),
            to_attr="_lv_analysis",
        )
    )[:limit]

    news_data = []
    for n in qs:
        tags = []

        lv_analysis = None
        if hasattr(n, "_lv_analysis") and n._lv_analysis:
            lv_analysis = n._lv_analysis[0]

        display_summary = n.summary
        if lv_analysis and isinstance(lv_analysis.analysis, dict):
            a = lv_analysis.analysis
            s = (a.get("summary") or "").strip()
            if s:
                display_summary = s

            kws = a.get("keywords") or []
            if isinstance(kws, list):
                tags.extend([str(x) for x in kws[:2] if str(x).strip()])

        if not tags:
            if n.sector:
                tags.append(n.sector)
            if n.ticker:
                tags.append(n.ticker)
            if not tags:
                tags.append("뉴스")

        market_tag = "국내" if n.market == NewsMarket.KOREA else "해외"

        news_data.append(
            {
                "id": n.id,
                "title": n.title,
                "summary": display_summary,
                "ticker": n.ticker,
                "tags": tags,
                "published_at": n.published_at,
                "url": n.url,
                "image_url": n.image_url,
                "market": n.market,
                "market_tag": market_tag,
                "theme": n.theme,
                "level": user_level,
            }
        )

    return news_data


class NewsThemesView(APIView):
    """
    GET /api/news/themes/
    - 왼쪽 Theme 리스트를 위해 Theme key/label 반환
    """
    permission_classes = [AllowAny]

    def get(self, request):
        themes = [{"key": key, "label": label} for (key, label) in NewsTheme.choices]
        return Response({"themes": themes})


class ThemeNewsView(APIView):
    """
    GET /api/news/by-theme/?theme=SEMICONDUCTOR_AI&market=all&limit=20
    - theme별 최신 뉴스 리스트
    - market: all|domestic|international
    """
    permission_classes = [AllowAny]

    def get(self, request):
        theme = (request.query_params.get("theme") or "").strip()
        if theme not in dict(NewsTheme.choices):
            theme = NewsTheme.ETC

        market_filter = (request.query_params.get("market") or "all").strip().lower()

        try:
            limit = int((request.query_params.get("limit") or "20").strip())
        except Exception:
            limit = 20
        limit = max(1, min(50, limit))

        user_level = _get_user_level(request)

        qs = NewsArticle.objects.filter(theme=theme)
        qs = _market_filter_qs(qs, market_filter)

        news_data = _build_news_list_payload(request=request, qs=qs, user_level=user_level, limit=limit)

        return Response(
            {
                "theme": theme,
                "theme_label": dict(NewsTheme.choices).get(theme, theme),
                "market": market_filter,
                "level": user_level,
                "news": news_data,
            }
        )


class NewsView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        # ------------------------------------------------------------
        # 1) 프로필 로드 (있으면 개인화)
        # ------------------------------------------------------------
        profile = _get_user_profile(request)

        # ------------------------------------------------------------
        # 2) market 필터: all | domestic | international
        # ------------------------------------------------------------
        market_filter = (request.query_params.get("market", "all") or "all").strip().lower()
        base_news = _market_filter_qs(NewsArticle.objects.all(), market_filter)

        # ------------------------------------------------------------
        # 3) user_level 산출 (Lv1~Lv5)
        # ------------------------------------------------------------
        user_level = 1
        if profile and hasattr(profile, "knowledge_level"):
            user_level = _clamp_level(profile.knowledge_level)

        # ------------------------------------------------------------
        # 4) (요구사항 2) 내 보유 종목 뉴스 2개 로직 전체 주석 처리
        # ------------------------------------------------------------
        my_stock_news = []  # 강제로 비움

        # ------------------------------------------------------------
        # 5) 프로필 기반 벡터 검색 query_text 개인화(sectors/risk_profile) 유지
        #    - 요구사항 1: "최대 유사한거 20개 채우면 스탑"
        # ------------------------------------------------------------
        if profile:
            sectors = ", ".join(profile.sectors) if getattr(profile, "sectors", None) else "경제"
            risk = profile.risk_profile if getattr(profile, "risk_profile", None) else "일반"
        else:
            sectors = "경제"
            risk = "일반"

        keyword = request.query_params.get("keyword")
        if keyword:
            query_text = f"{keyword} 관련 트렌드와 뉴스"
        else:
            query_text = f"{sectors} 산업의 트렌드와 {risk} 투자 정보"

        query_vec = get_embedding(query_text)

        # ------------------------------------------------------------
        # 6) user_level 분석 row를 미리 가져오기 (리스트 summary/tags용)
        # ------------------------------------------------------------
        base_news = base_news.prefetch_related(
            Prefetch(
                "analyses",
                queryset=NewsArticleAnalysis.objects.filter(level=user_level),
                to_attr="_lv_analysis",
            )
        )

        # ------------------------------------------------------------
        # 7) 벡터 유사도 검색
        #    - distance 순으로 가져온 뒤,
        #    - 중복 제거하면서 20개 찰 때까지 순차적으로 채우고 stop
        # ------------------------------------------------------------
        exclude_ids = [n.id for n in my_stock_news]
        candidate_count = 300 if not keyword else 80
        max_fill = 20

        vector_candidates = (
            base_news.exclude(id__in=exclude_ids)
            .annotate(distance=CosineDistance("embedding", query_vec))
            .order_by("distance")[:candidate_count]
        )

        seen_titles = set()
        vector_news: list[NewsArticle] = []
        for n in vector_candidates:
            k = _normalize_title(n.title)
            if k in seen_titles:
                continue
            seen_titles.add(k)
            vector_news.append(n)
            if len(vector_news) >= max_fill:
                break

        # ------------------------------------------------------------
        # 8) all 모드일 때 국내/해외 균형 맞추기
        # ------------------------------------------------------------
        combined_result = list(my_stock_news) + list(vector_news)

        unique_result = []
        seen_titles2 = set()
        for n in combined_result:
            k = _normalize_title(n.title)
            if k not in seen_titles2:
                seen_titles2.add(k)
                unique_result.append(n)

        if market_filter == "all":
            kr = [n for n in unique_result if n.market == NewsMarket.KOREA]
            intl = [n for n in unique_result if n.market == NewsMarket.INTERNATIONAL]

            target = 15
            target_kr = 8
            target_intl = 7

            final = []
            final.extend(kr[:target_kr])
            final.extend(intl[:target_intl])

            if len(final) < target:
                remain = target - len(final)
                rest = [n for n in unique_result if n not in final]
                rest_sorted = sorted(rest, key=lambda x: x.published_at, reverse=True)
                final.extend(rest_sorted[:remain])

            final_result = final[:target]
        else:
            final_result = unique_result[:15]

        # ------------------------------------------------------------
        # 9) Response 데이터 구성
        # ------------------------------------------------------------
        news_data = []
        for n in final_result:
            tags = []

            lv_analysis = None
            if hasattr(n, "_lv_analysis") and n._lv_analysis:
                lv_analysis = n._lv_analysis[0]

            display_summary = n.summary
            if lv_analysis and isinstance(lv_analysis.analysis, dict):
                a = lv_analysis.analysis
                s = (a.get("summary") or "").strip()
                if s:
                    display_summary = s

                kws = a.get("keywords") or []
                if isinstance(kws, list):
                    tags.extend([str(x) for x in kws[:2] if str(x).strip()])

            if not tags:
                if n.sector:
                    tags.append(n.sector)
                if n.ticker:
                    tags.append(n.ticker)
                if not tags:
                    tags.append("뉴스")

            market_tag = "국내" if n.market == NewsMarket.KOREA else "해외"

            news_data.append(
                {
                    "id": n.id,
                    "title": n.title,
                    "summary": display_summary,
                    "ticker": n.ticker,
                    "tags": tags,
                    "published_at": n.published_at,
                    "url": n.url,
                    "image_url": n.image_url,
                    "market": n.market,
                    "market_tag": market_tag,
                    "theme": n.theme,
                    "level": user_level,
                }
            )

        # ------------------------------------------------------------
        # 10) 추천 키워드 생성 (원본 유지)
        # ------------------------------------------------------------
        if profile:
            keywords_list = ([f"#{s}" for s in profile.sectors] if getattr(profile, "sectors", None) else [])
            if getattr(profile, "portfolio", None):
                keywords_list += [f"#{t}" for t in profile.portfolio[:2]]
            if getattr(profile, "risk_profile", None):
                keywords_list.append(f"#{profile.risk_profile}")
        else:
            keywords_list = ["#경제", "#시장동향", "#투자"]

        final_keywords = list(dict.fromkeys(keywords_list))[:4]
        if len(final_keywords) < 2:
            final_keywords += ["#경제", "#시장동향"]

        return Response({"news": news_data, "keywords": final_keywords[:4]})


class NewsSummaryView(APIView):
    """
    GET /api/news/<id>/summary/
    - 레벨별 분석은 NewsArticleAnalysis에서 가져옴
    - 없으면(예외) analyze_news()로 생성 후 다시 조회
    """
    permission_classes = [AllowAny]

    def get(self, request, news_id: int):
        try:
            article = NewsArticle.objects.get(id=news_id)
        except NewsArticle.DoesNotExist:
            return Response({"error": "뉴스를 찾을 수 없습니다."}, status=404)

        user_level = _get_user_level(request)

        row = NewsArticleAnalysis.objects.filter(article=article, level=user_level).first()

        if not row or not isinstance(row.analysis, dict):
            analyze_news(article, save_to_db=True)
            row = NewsArticleAnalysis.objects.filter(article=article, level=user_level).first()

        if not row or not isinstance(row.analysis, dict):
            return Response({"error": "분석에 실패했습니다."}, status=500)

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
                "article_id": article.id,
                "article_title": article.title,
                "theme": article.theme,
                "level": user_level,
                "analysis": final_analysis,
            }
        )
