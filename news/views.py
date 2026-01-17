from __future__ import annotations

import re
from typing import List, Optional

import openai
from django.conf import settings
from django.db.models import Prefetch, Q
from pgvector.django import CosineDistance
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import (
    NewsArticle,
    NewsArticleAnalysis,
    NewsTheme,
    NewsMarket,
)
from .services.analyze_news import analyze_news


# =========================================================
# Embedding
# =========================================================
def get_embedding(text: str):
    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
    response = client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding


# =========================================================
# Utils
# =========================================================
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


def _normalize_portfolio_tokens(raw_list: Optional[list]) -> List[str]:
    """
    profile.portfolio 칩/문자열을 뉴스 ticker/name 매칭용으로 정규화
    - '#삼성전자' -> '삼성전자'
    - ' 005930 ' -> '005930'
    """
    if not raw_list:
        return []
    out: List[str] = []
    for x in raw_list:
        s = str(x or "").strip()
        if not s:
            continue
        if s.startswith("#"):
            s = s[1:].strip()
        if not s:
            continue
        out.append(s)
    # 중복 제거(순서 유지)
    return list(dict.fromkeys(out))


def _build_news_list_payload(*, request, qs, user_level: int, limit: int):
    """
    공통: NewsArticle queryset을 받아 최신순 리스트 payload로 변환
    - 분석 summary/keywords는 user_level의 NewsArticleAnalysis에서 가져옴
    - tags: keywords 최대 2개, 없으면 sector/ticker fallback
    """
    qs = qs.order_by("-published_at")

    # ✅ 여기서만 _lv_analysis prefetch를 담당 (중복 prefetch 방지)
    qs = qs.prefetch_related(
        Prefetch(
            "analyses",
            queryset=NewsArticleAnalysis.objects.filter(level=user_level),
            to_attr="_lv_analysis",
        )
    )[:limit]

    news_data = []
    for n in qs:
        tags: List[str] = []

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


def _portfolio_news_queryset(*, base_news_qs, portfolio_tokens: List[str]):
    """
    portfolio 칩과 동일한 값 위주로 NewsArticle을 뽑는다.
    우선순위:
    - ticker 정확 매칭
    - name 부분 매칭 (icontains)
    """
    if not portfolio_tokens:
        return base_news_qs.none()

    q = Q()

    # ticker: exact match
    q |= Q(ticker__in=portfolio_tokens)

    # name: icontains match (각 토큰 OR)
    for tok in portfolio_tokens:
        if tok:
            q |= Q(name__icontains=tok)

    return base_news_qs.filter(q)


def _dedupe_by_title_keep_order(articles: List[NewsArticle]) -> List[NewsArticle]:
    seen = set()
    out: List[NewsArticle] = []
    for a in articles:
        k = _normalize_title(a.title)
        if k in seen:
            continue
        seen.add(k)
        out.append(a)
    return out


# ✅ 키워드(칩) 정규화/필터링: #B 제거
def _normalize_keyword_chip(x: object) -> str:
    s = str(x or "").strip()
    if not s:
        return ""
    if s.startswith("#"):
        s = s[1:].strip()
    s = " ".join(s.split())
    return s


def _is_blocked_keyword(s: str) -> bool:
    """
    UI에서 제외할 키워드.
    현재 요구사항: '#B'만 제거 (대소문자 무시)
    """
    if not s:
        return True
    if s.strip().lower() == "b":
        return True
    return False


# =========================================================
# Theme label mapping (Profile UI -> Model key)
# =========================================================
PROFILE_THEME_TO_KEY = {
    # 프론트 options (한글)
    "반도체/AI": NewsTheme.SEMICONDUCTOR_AI,
    "배터리": NewsTheme.BATTERY,
    "베터리": NewsTheme.BATTERY,  # ✅ 오타 허용
    "IT/인터넷": NewsTheme.ICT_PLATFORM,
    "IT / 인터넷": NewsTheme.ICT_PLATFORM,
    "IT": NewsTheme.ICT_PLATFORM,
    "바이오/건강": NewsTheme.BIO_HEALTH,
    "자동차": NewsTheme.AUTO,
    "친환경/석유에너지": NewsTheme.ENERGY,
    "친환경/에너지": NewsTheme.ENERGY,
    "석유에너지": NewsTheme.ENERGY,
    "에너지": NewsTheme.ENERGY,
    "금융/지주": NewsTheme.FINANCE_HOLDING,
    "금융": NewsTheme.FINANCE_HOLDING,
    "지주": NewsTheme.FINANCE_HOLDING,
}


def _canon_theme_label(s: str) -> str:
    """
    theme 라벨 입력 정규화:
    - 공백 정리
    - 소문자
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = " ".join(s.split())
    return s.lower()


def _resolve_theme_from_keyword(keyword: str) -> Optional[str]:
    """
    keyword가 theme key/label/프로필 한글 라벨 중 하나면 theme key로 반환.
    - key: SEMICONDUCTOR_AI ...
    - label: "Semiconductor/AI" 등 (models.py)
    - profile label: "반도체/AI", "친환경/석유에너지" 등 (프론트)
    """
    kw_raw = (keyword or "").strip()
    if not kw_raw:
        return None

    kw = _normalize_keyword_chip(kw_raw)
    if not kw:
        return None

    # 1) theme key 직접 입력이면 그대로
    choices = list(NewsTheme.choices)
    key_set = {k for (k, _) in choices}
    if kw in key_set:
        return kw

    # 2) 프로필 한글 라벨 매핑 우선 처리
    kw_canon = _canon_theme_label(kw)
    for k_label, v_key in PROFILE_THEME_TO_KEY.items():
        if _canon_theme_label(k_label) == kw_canon:
            return v_key

    # 3) models.py label(영문) 매칭
    for k, label in choices:
        if _canon_theme_label(str(label)) == kw_canon:
            return k

    return None


def _keyword_news_queryset(*, base_news_qs, keyword: str):
    """
    ✅ 키워드 뉴스는 벡터 유사도 대신 DB 필터링:
    - keyword가 theme이면 theme 매칭
    - 아니면 name/ticker로 매칭 (name icontains, ticker exact/contains)
    """
    kw = _normalize_keyword_chip(keyword)
    if not kw:
        return base_news_qs.none()

    theme_key = _resolve_theme_from_keyword(kw)
    if theme_key:
        return base_news_qs.filter(theme=theme_key)

    q = Q(name__icontains=kw) | Q(ticker__iexact=kw) | Q(ticker__icontains=kw)
    return base_news_qs.filter(q)


# =========================================================
# Views
# =========================================================
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
    """
    GET /api/news/ai-recommend/?market=all&keyword=...
    Response:
      - news: 최종 추천 뉴스 (최대 20개)
      - keywords: 프로필 기반 추천 키워드
    """
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
        user_level = 3
        if profile and hasattr(profile, "knowledge_level"):
            user_level = _clamp_level(profile.knowledge_level)

        # ------------------------------------------------------------
        # 4) ✅ keyword 모드(=내 키워드 뉴스): theme/name 기반 필터링
        #    (중요: base_news에 _lv_analysis prefetch를 걸지 않음 — 중복 에러 방지)
        # ------------------------------------------------------------
        keyword = request.query_params.get("keyword")
        if keyword:
            try:
                limit = int((request.query_params.get("limit") or "20").strip())
            except Exception:
                limit = 20
            limit = max(1, min(50, limit))

            kw_qs = _keyword_news_queryset(base_news_qs=base_news, keyword=keyword)
            news_data = _build_news_list_payload(request=request, qs=kw_qs, user_level=user_level, limit=limit)

            # 키워드 목록(프로필 기반 + #B 제거)
            MAX_KEYWORDS = 30
            if profile:
                keywords_list: List[str] = []

                if getattr(profile, "sectors", None):
                    for s in profile.sectors:
                        norm = _normalize_keyword_chip(s)
                        if _is_blocked_keyword(norm):
                            continue
                        keywords_list.append(f"#{norm}")

                if getattr(profile, "portfolio", None):
                    for t in profile.portfolio:
                        norm = _normalize_keyword_chip(t)
                        if _is_blocked_keyword(norm):
                            continue
                        keywords_list.append(f"#{norm}")

                if getattr(profile, "risk_profile", None):
                    rp_norm = _normalize_keyword_chip(profile.risk_profile)
                    if rp_norm and not _is_blocked_keyword(rp_norm):
                        keywords_list.append(f"#{rp_norm}")
            else:
                keywords_list = ["#경제", "#시장동향", "#투자"]

            final_keywords = list(dict.fromkeys(keywords_list))[:MAX_KEYWORDS]
            if len(final_keywords) < 2:
                final_keywords += ["#경제", "#시장동향"]

            return Response({"news": news_data, "keywords": final_keywords})

        # ------------------------------------------------------------
        # 5) ✅ AI 브리핑 모드(=기존 유지): embedding 유사도 + 보유종목 부스팅
        # ------------------------------------------------------------
        if profile:
            sectors = ", ".join(profile.sectors) if getattr(profile, "sectors", None) else "경제"
            risk = profile.risk_profile if getattr(profile, "risk_profile", None) else "일반"
        else:
            sectors = "경제"
            risk = "일반"

        query_text = f"{sectors} 산업의 트렌드와 {risk} 투자 정보"
        query_vec = get_embedding(query_text)

        # ------------------------------------------------------------
        # 6) ✅ 보유 종목 기반 추천 (portfolio 칩과 ticker/name 매칭)
        # ------------------------------------------------------------
        portfolio_tokens: List[str] = []
        if profile and getattr(profile, "portfolio", None):
            portfolio_tokens = _normalize_portfolio_tokens(profile.portfolio)

        MY_STOCK_MAX = 8

        my_stock_news_qs = _portfolio_news_queryset(base_news_qs=base_news, portfolio_tokens=portfolio_tokens)
        my_stock_news = list(my_stock_news_qs.order_by("-published_at")[:200])
        my_stock_news = _dedupe_by_title_keep_order(my_stock_news)[:MY_STOCK_MAX]

        # ------------------------------------------------------------
        # 7) 벡터 유사도 검색으로 나머지 채우기 (총 20개)
        # ------------------------------------------------------------
        exclude_ids = [n.id for n in my_stock_news]
        candidate_count = 300

        TOTAL_TARGET = 20
        max_fill = max(0, TOTAL_TARGET - len(my_stock_news))

        vector_candidates = (
            base_news.exclude(id__in=exclude_ids)
            .annotate(distance=CosineDistance("embedding", query_vec))
            .order_by("distance")[:candidate_count]
        )

        seen_titles = set(_normalize_title(n.title) for n in my_stock_news)
        vector_news: List[NewsArticle] = []
        for n in vector_candidates:
            k = _normalize_title(n.title)
            if k in seen_titles:
                continue
            seen_titles.add(k)
            vector_news.append(n)
            if len(vector_news) >= max_fill:
                break

        # ------------------------------------------------------------
        # 8) 합치고 중복 제거
        # ------------------------------------------------------------
        combined = list(my_stock_news) + list(vector_news)
        unique_result = _dedupe_by_title_keep_order(combined)

        # ------------------------------------------------------------
        # 9) all 모드일 때 국내/해외 균형 맞추기 (총 20개)
        # ------------------------------------------------------------
        if market_filter == "all":
            kr = [n for n in unique_result if n.market == NewsMarket.KOREA]
            intl = [n for n in unique_result if n.market == NewsMarket.INTERNATIONAL]

            target = TOTAL_TARGET
            target_kr = 10
            target_intl = 10

            final: List[NewsArticle] = []
            final.extend(kr[:target_kr])
            final.extend(intl[:target_intl])

            if len(final) < target:
                remain = target - len(final)
                rest = [n for n in unique_result if n not in final]
                rest_sorted = sorted(rest, key=lambda x: x.published_at, reverse=True)
                final.extend(rest_sorted[:remain])

            final_result = final[:target]
        else:
            final_result = unique_result[:TOTAL_TARGET]

        # ------------------------------------------------------------
        # 10) Response 데이터 구성 (payload builder 사용)
        # ------------------------------------------------------------
        news_data = _build_news_list_payload(
            request=request,
            qs=NewsArticle.objects.filter(id__in=[n.id for n in final_result]).order_by("-published_at"),
            user_level=user_level,
            limit=TOTAL_TARGET,
        )

        # ------------------------------------------------------------
        # 11) 추천 키워드 생성 (portfolio 칩 포함) + ✅ #B 제거
        # -----------------------------------------------------------
        MAX_KEYWORDS = 30
        if profile:
            keywords_list: List[str] = []

            if getattr(profile, "sectors", None):
                for s in profile.sectors:
                    norm = _normalize_keyword_chip(s)
                    if _is_blocked_keyword(norm):
                        continue
                    keywords_list.append(f"#{norm}")

            if getattr(profile, "portfolio", None):
                for t in profile.portfolio:
                    norm = _normalize_keyword_chip(t)
                    if _is_blocked_keyword(norm):
                        continue
                    keywords_list.append(f"#{norm}")

            if getattr(profile, "risk_profile", None):
                rp_norm = _normalize_keyword_chip(profile.risk_profile)
                if rp_norm and not _is_blocked_keyword(rp_norm):
                    keywords_list.append(f"#{rp_norm}")
        else:
            keywords_list = ["#경제", "#시장동향", "#투자"]

        final_keywords = list(dict.fromkeys(keywords_list))[:MAX_KEYWORDS]
        if len(final_keywords) < 2:
            final_keywords += ["#경제", "#시장동향"]

        return Response({"news": news_data, "keywords": final_keywords})


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
