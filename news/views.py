# news/views.py
from __future__ import annotations

import re
import openai
from django.conf import settings
from django.db.models import Prefetch
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from pgvector.django import CosineDistance

from .models import NewsArticle, NewsArticleAnalysis


def get_embedding(text: str):
    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding


def _clamp_level(x: int) -> int:
    try:
        x = int(x)
    except Exception:
        return 1
    return max(1, min(5, x))


class NewsView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        # ------------------------------------------------------------
        # 1) 프로필 로드 (있으면 개인화)
        # ------------------------------------------------------------
        profile = None
        if request.user.is_authenticated:
            try:
                profile = request.user.profile
            except Exception:
                profile = None

        # ------------------------------------------------------------
        # 2) market 필터: all | domestic | international
        # ------------------------------------------------------------
        market_filter = (request.query_params.get("market", "all") or "all").strip().lower()

        if market_filter == "domestic":
            base_news = NewsArticle.objects.filter(market="Korea")
        elif market_filter == "international":
            base_news = NewsArticle.objects.filter(market="International")
        else:
            base_news = NewsArticle.objects.filter(market__in=["Korea", "International"])

        # ------------------------------------------------------------
        # 3) user_level 산출 (Lv1~Lv5)
        # ------------------------------------------------------------
        user_level = 1
        if profile and hasattr(profile, "knowledge_level"):
            user_level = _clamp_level(profile.knowledge_level)

        # ------------------------------------------------------------
        # 4) (요구사항 2) 내 보유 종목 뉴스 2개 로직 전체 주석 처리
        # ------------------------------------------------------------
        # my_stock_news_qs = None
        # my_stock_news = []
        # if profile:
        #     my_stock_news_qs = base_news.filter(ticker__in=profile.portfolio).order_by("-published_at")[:2]
        #     my_stock_news = list(my_stock_news_qs)

        my_stock_news = []  # 강제로 비움 (주석처리 대체)

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
        exclude_ids = [n.id for n in my_stock_news]  # 현재는 [] 이지만 구조 유지
        candidate_count = 300 if not keyword else 80  # 후보는 넉넉히 가져오고
        max_fill = 20  # ✅ 요구사항: 최대 20개 채우면 stop

        vector_candidates = (
            base_news.exclude(id__in=exclude_ids)
            .annotate(distance=CosineDistance("embedding", query_vec))
            .order_by("distance")[:candidate_count]
        )

        def normalize_title(title: str) -> str:
            cleaned = re.sub(r"^[\d\.\s]+", "", title or "")
            cleaned = " ".join(cleaned.split())
            return cleaned[:80]

        seen_titles = set()
        vector_news: list[NewsArticle] = []
        for n in vector_candidates:
            k = normalize_title(n.title)
            if k in seen_titles:
                continue
            seen_titles.add(k)
            vector_news.append(n)
            if len(vector_news) >= max_fill:
                break

        # ------------------------------------------------------------
        # 8) all 모드일 때 국내/해외 균형 맞추기 (원본 로직 유지)
        #    - 다만 추천 pool이 20개이므로 여기서 15개로 컷
        # ------------------------------------------------------------
        combined_result = list(my_stock_news) + list(vector_news)

        unique_result = []
        seen_titles2 = set()
        for n in combined_result:
            k = normalize_title(n.title)
            if k not in seen_titles2:
                seen_titles2.add(k)
                unique_result.append(n)

        if market_filter == "all":
            kr = [n for n in unique_result if n.market == "Korea"]
            intl = [n for n in unique_result if n.market == "International"]

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
        #    - tags: NewsArticleAnalysis(level=user_level).analysis["keywords"] 최대 2개
        #    - summary: NewsArticleAnalysis(level=user_level).analysis["summary"] 우선
        #    - theme: NewsArticle.theme (Lv1 결정 값)
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
                    "theme": n.theme,  # ✅ 레벨보다 상위 필드: Lv1에서 결정해 저장된 값
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


from .services.analyze_news import analyze_news


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

        user_level = 1
        if request.user.is_authenticated and hasattr(request.user, "profile"):
            try:
                user_level = _clamp_level(request.user.profile.knowledge_level)
            except Exception:
                user_level = 1

        row = NewsArticleAnalysis.objects.filter(article=article, level=user_level).first()

        if not row or not isinstance(row.analysis, dict):
            # 크롤러에서 이미 만들어져야 정상인데,
            # 안전장치로 없으면 생성 후 다시 조회
            analyze_news(article, save_to_db=True)
            row = NewsArticleAnalysis.objects.filter(article=article, level=user_level).first()

        if not row or not isinstance(row.analysis, dict):
            return Response({"error": "분석에 실패했습니다."}, status=500)

        final_analysis = row.analysis.copy()

        # 프론트 호환: investment_action 리스트 보장
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
                "theme": article.theme,  # ✅ Lv1에서 결정된 대표 theme
                "level": user_level,
                "analysis": final_analysis,
            }
        )
