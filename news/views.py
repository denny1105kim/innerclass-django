from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from pgvector.django import CosineDistance

from .models import NewsArticle
import openai
from django.conf import settings
import re

# 임베딩 함수
def get_embedding(text: str):
    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding


class NewsView(APIView):
    permission_classes = [AllowAny]  # 비로그인도 접근 가능 (개발용)

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
        #    - domestic: market="Korea"
        #    - international: market="International"
        #    - all: Korea + International (둘 다)
        # ------------------------------------------------------------
        market_filter = (request.query_params.get("market", "all") or "all").strip().lower()

        if market_filter == "domestic":
            base_news = NewsArticle.objects.filter(market="Korea")
        elif market_filter == "international":
            base_news = NewsArticle.objects.filter(market="International")
        else:
            # all
            base_news = NewsArticle.objects.filter(market__in=["Korea", "International"])

        # ------------------------------------------------------------
        # 3) 내 보유 종목 뉴스 (프로필 있을 때만)
        # ------------------------------------------------------------
        if profile:
            # portfolio가 리스트/배열이라는 전제(기존 코드 유지)
            my_stock_news_qs = base_news.filter(ticker__in=profile.portfolio).order_by("-published_at")[:2]
            my_stock_news = list(my_stock_news_qs)

            sectors = ", ".join(profile.sectors) if getattr(profile, "sectors", None) else "경제"
            risk = profile.risk_profile if getattr(profile, "risk_profile", None) else "일반"
        else:
            my_stock_news = []
            sectors = "경제"
            risk = "일반"

        # ------------------------------------------------------------
        # 4) 키워드 검색 지원
        # ------------------------------------------------------------
        keyword = request.query_params.get("keyword")
        if keyword:
            my_stock_news = []  # 키워드 검색 시 보유종목 우선 슬롯 제거
            query_text = f"{keyword} 관련 트렌드와 뉴스"
        else:
            query_text = f"{sectors} 산업의 트렌드와 {risk} 투자 정보"

        query_vec = get_embedding(query_text)

        # ------------------------------------------------------------
        # 5) 벡터 유사도 검색
        # ------------------------------------------------------------
        exclude_ids = [n.id for n in my_stock_news]
        candidate_count = 10 if keyword else 200  # 해외까지 포함하면 후보를 조금 늘려도 됨

        vector_news_candidates = (
            base_news.exclude(id__in=exclude_ids)
            .annotate(distance=CosineDistance("embedding", query_vec))
            .order_by("distance")[:candidate_count]
        )

        if keyword:
            vector_news = list(vector_news_candidates[:4])
        else:
            # 유사도 후보군에서 최신순으로 상위 4개
            vector_news = sorted(vector_news_candidates, key=lambda x: x.published_at, reverse=True)[:4]

        # ------------------------------------------------------------
        # 6) all 모드일 때 국내/해외 섞기 (요청사항 반영)
        #    - 목표: 국내 10개 + 해외 10개 수준의 구조로 확장 가능하지만
        #      현 코드 구조(내보유2 + AI추천4) 유지하면서
        #      all일 경우 domestic/international을 섞어 다양성 확보
        #
        #    여기서는 "내보유(최대2) + AI추천(최대4)" 기본 유지하되,
        #    all일 때 추천 쪽에서 국내/해외가 둘 다 섞이도록 후보군을 넓혀놨고
        #    추가로 최종 15개 컷에서 국내/해외 균형을 약간 맞춤.
        # ------------------------------------------------------------
        combined_result = list(my_stock_news) + list(vector_news)

        # ------------------------------------------------------------
        # 7) 제목 기준 중복 제거
        # ------------------------------------------------------------
        def normalize_title(title: str) -> str:
            cleaned = re.sub(r"^[\d\.\s]+", "", title or "")
            cleaned = " ".join(cleaned.split())
            return cleaned[:50]

        seen_titles = set()
        unique_result = []
        for n in combined_result:
            normalized = normalize_title(n.title)
            if normalized not in seen_titles:
                seen_titles.add(normalized)
                unique_result.append(n)

        # ------------------------------------------------------------
        # 8) all 모드일 때: 국내/해외 균형을 조금 맞춘 15개 구성
        #    - 국내/해외 모두 있을 때: 각 7~8개 정도 목표
        #    - 한 쪽이 부족하면 있는 쪽으로 채움
        # ------------------------------------------------------------
        if market_filter == "all":
            kr = [n for n in unique_result if n.market == "Korea"]
            intl = [n for n in unique_result if n.market == "International"]

            target = 15
            target_kr = 8
            target_intl = 7

            final = []
            final.extend(kr[:target_kr])
            final.extend(intl[:target_intl])

            # 부족분 채우기
            if len(final) < target:
                remain = target - len(final)
                # 아직 안 들어간 것들에서 최신순으로 채움
                rest = [n for n in unique_result if n not in final]
                rest_sorted = sorted(rest, key=lambda x: x.published_at, reverse=True)
                final.extend(rest_sorted[:remain])

            final_result = final[:target]
        else:
            final_result = unique_result[:15]

        # ------------------------------------------------------------
        # 9) Response 데이터 구성
        # ------------------------------------------------------------
        news_data = [
            {
                "id": n.id,
                "title": n.title,
                "summary": n.summary,
                "ticker": n.ticker,
                "tag": "내 보유 종목" if n in my_stock_news else "AI 추천",
                "published_at": n.published_at,
                "url": n.url,
                "image_url": n.image_url,
                "market": n.market,
            }
            for n in final_result
        ]

        # ------------------------------------------------------------
        # 10) 추천 키워드 생성 (최대 4개)
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


from .services import analyze_news


class NewsSummaryView(APIView):
    """
    GET /api/news/<id>/summary/

    기사 본문을 기반으로 LLM이 요약 및 Q&A 콘텐츠 생성.
    이미 분석된 데이터가 있으면 DB에서 바로 반환 (캐싱 효과).
    없으면 실시간 생성 후 저장.
    """
    permission_classes = [AllowAny]

    def get(self, request, news_id):
        try:
            article = NewsArticle.objects.get(id=news_id)
        except NewsArticle.DoesNotExist:
            return Response({"error": "뉴스를 찾을 수 없습니다."}, status=404)

        # 1. DB에 저장된 분석 결과가 있는지 확인
        if article.analysis:
            return Response(
                {
                    "success": True,
                    "article_id": article.id,
                    "article_title": article.title,
                    "analysis": article.analysis,
                }
            )

        # 2. 없으면 서비스 호출하여 생성 및 저장
        result = analyze_news(article, save_to_db=True)

        if result:
            return Response(
                {
                    "success": True,
                    "article_id": article.id,
                    "article_title": article.title,
                    "analysis": result,
                }
            )
        else:
            return Response({"error": "분석에 실패했습니다."}, status=500)
