from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from pgvector.django import CosineDistance #유사도 계산

from .models import NewsArticle
from auth_app.models import UserProfile
import openai
from django.conf import settings
import random

# 임베딩 함수
def get_embedding(text):
    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding

class NewsView(APIView):
    permission_classes = [AllowAny]  # 비로그인도 접근 가능 (개발용)

    def get(self, request):
        # 프로필 가져오기 (로그인 안 했으면 기본값 사용)
        profile = None
        if request.user.is_authenticated:
            try:
                profile = request.user.profile
            except:
                pass

        # 시장 필터 파라미터 (all, domestic, international)
        market_filter = request.query_params.get('market', 'all')
        
        # 기본 쿼리셋 (국내 뉴스만 조회)
        if market_filter == 'international':
             # 해외 뉴스는 현재 기능 비활성화로 빈 리스트 반환
             base_news = NewsArticle.objects.none()
        else:
             # 'domestic' 또는 'all' 인 경우 -> 국내 뉴스만 반환
             # 'all'인 경우 exclude('International')을 사용하므로, 
             # 이론적으로는 국내(+기타) 뉴스가 나옴. 
             # 프론트엔드에서 'domestic'으로 요청을 보내면 더 확실함.
             key = 'Korea' if market_filter == 'domestic' else None
             if key:
                base_news = NewsArticle.objects.filter(market=key)
             else:
                base_news = NewsArticle.objects.exclude(market='International')

        # 프로필이 없으면 (비로그인) 기본값 사용
        if profile:
            my_stock_news = base_news.filter(ticker__in=profile.portfolio).order_by('-published_at')[:2]
            sectors = ", ".join(profile.sectors) if profile.sectors else "경제"
            risk = profile.risk_profile if profile.risk_profile else "일반"
        else:
            my_stock_news = []
            sectors = "경제"
            risk = "일반"

        query_text = f"{sectors} 산업의 트렌드와 {risk} 투자 정보"
        
        # 키워드 검색 지원
        keyword = request.query_params.get('keyword')
        if keyword:
            my_stock_news = []
            query_text = f"{keyword} 관련 트렌드와 뉴스"
        query_vec = get_embedding(query_text)

        # 벡터 유사도로 검색
        exclude_ids = [n.id for n in my_stock_news]
        candidate_count = 10 if keyword else 100
        
        vector_news_candidates = base_news.exclude(id__in=exclude_ids) \
            .annotate(distance=CosineDistance('embedding', query_vec)) \
            .order_by('distance')[:candidate_count]

        if keyword:
             vector_news = vector_news_candidates[:4]
        else:
             vector_news = sorted(vector_news_candidates, key=lambda x: x.published_at, reverse=True)[:4]

        # 결과 합치기
        combined_result = list(my_stock_news) + list(vector_news)
        
        # 'all' 모드일 때: 국내 2개 + 해외 1개 배치

        
        # 제목 기준 중복 제거 (번호 등 제거 후 핵심 내용만 비교)
        import re
        def normalize_title(title):
            # 앞쪽 번호/기호 제거 (예: "05. " or "03. " -> "")
            cleaned = re.sub(r'^[\d\.\s]+', '', title)
            # 공백 정규화
            cleaned = ' '.join(cleaned.split())
            return cleaned[:50]  # 앞 50자만 비교
        
        seen_titles = set()
        unique_result = []
        for n in combined_result:
            normalized = normalize_title(n.title)
            if normalized not in seen_titles:
                seen_titles.add(normalized)
                unique_result.append(n)
        
        final_result = unique_result[:15]


        news_data = [{
            "id": n.id,
            "title": n.title,
            "summary": n.summary,
            "ticker": n.ticker,
            "tag": "내 보유 종목" if n in my_stock_news else "AI 추천",
            "published_at": n.published_at,
            "url": n.url,
            "image_url": n.image_url,
            "market": n.market
        } for n in final_result]

        # 추천 키워드 생성 (최대 4개)
        if profile:
            keywords_list = ([f"#{s}" for s in profile.sectors] if profile.sectors else [])
            if profile.portfolio:
                keywords_list += [f"#{t}" for t in profile.portfolio[:2]]
            if profile.risk_profile:
                keywords_list.append(f"#{profile.risk_profile}")
        else:
            keywords_list = ["#경제", "#시장동향", "#투자"]
        
        final_keywords = list(dict.fromkeys(keywords_list))[:4]
        if len(final_keywords) < 2:
            final_keywords += ["#경제", "#시장동향"]

        return Response({
            "news": news_data,
            "keywords": final_keywords[:4]
        })



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

        # 1. DB에 저장된 분석 결과가 있는지비 확인
        if article.analysis:
            return Response({
                "success": True,
                "article_id": article.id,
                "article_title": article.title,
                "analysis": article.analysis,
            })

        # 2. 없으면 서비스 호출하여 생성 및 저장
        result = analyze_news(article, save_to_db=True)
        
        if result:
            return Response({
                "success": True,
                "article_id": article.id,
                "article_title": article.title,
                "analysis": result,
            })
        else:
            return Response({"error": "분석에 실패했습니다."}, status=500)