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
        news_data = []
        for n in final_result:
            tags = []
            
            # 1. 태그: 내 보유 종목 (최우선)
            if n in my_stock_news:
                tags.append("내 보유 종목")
            
            # 2. 태그: AI 분석 키워드 (있으면 2개까지)
            if n.analysis and isinstance(n.analysis, dict) and 'keywords' in n.analysis:
                keywords = n.analysis.get('keywords', [])
                if keywords:
                    tags.extend(keywords[:2])
            
            # 3. 태그: 키워드가 없을 경우를 대비한 Fallback (섹터/티커)
            if len(tags) == 0:
                if n.sector:
                    tags.append(n.sector)
                if n.ticker:
                    tags.append(n.ticker)
                # 그래도 없으면 기본값
                if not tags:
                    tags.append("뉴스")

            # --------------------------------------------------------
            # [NEW] 사용자 투자 지식 수준에 따른 맞춤형 요약 선택
            # --------------------------------------------------------
            # 기본값: 요약문(summary) 사용
            display_summary = n.summary
            
            # 1. 사용자 레벨 가져오기 (비로그인/프로필 없음 -> Lv.1)
            user_level = 1
            if request.user.is_authenticated:
                try:
                    # related_name="profile" 가정
                    if hasattr(request.user, 'profile'):
                        user_level = request.user.profile.knowledge_level
                        # 범위 보정 (1~5)
                        if user_level < 1: user_level = 1
                        if user_level > 5: user_level = 5
                except:
                    user_level = 1
            
            # 2. 분석 데이터에서 해당 레벨 요약 꺼내기
            if n.analysis and isinstance(n.analysis, dict):
                versions = n.analysis.get('summary_versions', {})
                key = f"lv{user_level}" # lv1, lv2, ...
                if key in versions:
                    display_summary = versions[key]

            news_data.append({
                "id": n.id,
                "title": n.title,
                "summary": display_summary, # 맞춤형 요약으로 덮어씀
                "ticker": n.ticker,
                "tags": tags,
                "published_at": n.published_at,
                "url": n.url,
                "image_url": n.image_url,
                "market": n.market,
            })


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

        # 1. 분석 데이터 확보 (DB or 생성)
        analysis_data = article.analysis
        if not analysis_data:
            analysis_data = analyze_news(article, save_to_db=True)
            
        if not analysis_data:
             return Response({"error": "분석에 실패했습니다."}, status=500)

        # 2. 사용자 레벨에 따라 응답 데이터 커스터마이징 (메모리상에서만 변경)
        # 중요: DB에 있는 JSON을 직접 수정하지 않도록 deep copy 혹은 shallow copy 사용
        # 여기서는 1depth copy로 충분할 수 있으나, nested structures가 있으므로 주의.
        # 단순히 필드 교체만 하므로 copy()로 충분.
        final_analysis = analysis_data.copy()

        user_level = 1
        if request.user.is_authenticated:
            try:
                if hasattr(request.user, 'profile'):
                    user_level = request.user.profile.knowledge_level
                    # 범위 보정 (1~5)
                    if user_level < 1: user_level = 1
                    if user_level > 5: user_level = 5
            except:
                pass
        
        # 3. 새로운 다중 레벨 구조(level_content)가 있는 경우, 해당 레벨 데이터로 덮어쓰기
        if 'level_content' in final_analysis:
            level_key = f"lv{user_level}"
            level_data = final_analysis['level_content'].get(level_key)
            
            if level_data:
                # level_data 내부의 키(summary, bullet_points 등)를 상위로 끌어올림 (Flatten)
                final_analysis.update(level_data)
                
                # 프론트엔드 호환성을 위해 'investment_action'이 'action_guide'로 되어있을 경우 매핑
                if 'action_guide' in level_data and 'investment_action' not in level_data:
                     # action_guide가 문자열이면 리스트로 변환 (프론트가 리스트 기대 시)
                     # 기존 프롬프트에서는 action_guide가 문자열이거나 리스트일 수 있음.
                     # NewsDetailModal.tsx에서는 investment_action.map(...)을 사용하므로 리스트여야 함.
                     ag = level_data['action_guide']
                     if isinstance(ag, str):
                         final_analysis['investment_action'] = [ag]
                     else:
                         final_analysis['investment_action'] = ag

        # [Safety Patch] strategy_guide가 없는 경우 (프론트엔드 에러 방지)
        # 이전 버전의 데이터나, 생성 중 누락된 경우를 대비하여 기본값 주입
        if 'strategy_guide' not in final_analysis or not final_analysis['strategy_guide']:
            final_analysis['strategy_guide'] = {
                "short_term": "분석 데이터가 충분하지 않습니다.",
                "long_term": "추후 업데이트 될 예정입니다."
            }

        # 4. (구버전 호환) 전문가(Lv.4 이상)인 경우 전문가용 필드로 교체 (level_content가 없는 구버전 데이터 대비)
        elif user_level >= 4:
            if 'bullet_points_expert' in final_analysis:
                final_analysis['bullet_points'] = final_analysis['bullet_points_expert']
            if 'what_is_this_expert' in final_analysis:
                final_analysis['what_is_this'] = final_analysis['what_is_this_expert']
            if 'why_important_expert' in final_analysis:
                final_analysis['why_important'] = final_analysis['why_important_expert']
            if 'stock_impact_expert' in final_analysis:
                final_analysis['stock_impact'] = final_analysis['stock_impact_expert']

        return Response(
            {
                "success": True,
                "article_id": article.id,
                "article_title": article.title,
                "analysis": final_analysis,
            }
        )
