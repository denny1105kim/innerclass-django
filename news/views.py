from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Optional

from django.conf import settings
from django.db.models import QuerySet, Count
from pgvector.django import CosineDistance
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from news.models import NewsArticle, NewsMarket, NewsSector
from news.services.embedding_news import embed_query
from news.services.analyze_news import analyze_news

# (NEW) main analysis service (Lv3-style only, no level_content)
from news.services.analyze_news_main import analyze_news_main


# =========================================================
# Market helpers
# =========================================================
def _get_int_setting(name: str, default: int) -> int:
    v = getattr(settings, name, None)
    try:
        return int(v)
    except Exception:
        return default


def _normalize_ticker(t: str) -> str:
    return (t or "").strip().upper()


def _guess_market_from_ticker(ticker: str) -> str:
    """
    티커 문자열만으로 시장을 '대략' 추정합니다.
    - 한국: 숫자코드(005930), 또는 .KS/.KQ, 또는 6자리 숫자
    - 해외(미국): NVDA, AAPL 등 알파벳 중심
    """
    t = _normalize_ticker(ticker)
    if not t:
        return NewsMarket.KR

    if t.endswith(".KS") or t.endswith(".KQ"):
        return NewsMarket.KR

    if re.fullmatch(r"\d{5,6}", t):
        return NewsMarket.KR

    if re.match(r"^\d", t):
        return NewsMarket.KR

    return NewsMarket.INTERNATIONAL


def _split_portfolio_by_market(portfolio: List[str]) -> Tuple[List[str], List[str]]:
    kr: List[str] = []
    intl: List[str] = []
    for t in portfolio or []:
        mk = _guess_market_from_ticker(t)
        if mk == NewsMarket.KR:
            kr.append(t)
        else:
            intl.append(t)
    return kr, intl


def _normalize_title(title: str) -> str:
    cleaned = re.sub(r"^[\d\.\s]+", "", title or "")
    cleaned = " ".join(cleaned.split()).strip()
    return cleaned


def _dedupe_by_title(items: List[NewsArticle], limit_title_len: int = 50) -> List[NewsArticle]:
    seen = set()
    out: List[NewsArticle] = []
    for n in items:
        nt = _normalize_title(n.title)[:limit_title_len]
        if nt in seen:
            continue
        seen.add(nt)
        out.append(n)
    return out


def _balance_all_markets(
    items: List[NewsArticle],
    target_total: int,
    target_kr: int,
    target_intl: int,
) -> List[NewsArticle]:
    kr = [n for n in items if n.market == NewsMarket.KR]
    intl = [n for n in items if n.market == NewsMarket.INTERNATIONAL]

    final: List[NewsArticle] = []
    final.extend(kr[:target_kr])
    final.extend(intl[:target_intl])

    if len(final) < target_total:
        remain = target_total - len(final)
        rest = [n for n in items if n not in final]
        rest_sorted = sorted(rest, key=lambda x: x.published_at, reverse=True)
        final.extend(rest_sorted[:remain])

    return final[:target_total]


def _get_user_level(request) -> int:
    lv = 1
    if request.user.is_authenticated:
        try:
            if hasattr(request.user, "profile"):
                lv = int(request.user.profile.knowledge_level)
        except Exception:
            lv = 1
    return max(1, min(lv, 5))


def _pick_display_summary(article: NewsArticle, user_level: int) -> str:
    """
    list 카드에 보여줄 summary 결정 우선순위:
    1) analysis.summary_versions[lvX] (레거시)
    2) analysis.level_content.lvX.summary (현재 analyze_news 포맷)
    3) article.summary
    """
    display_summary = article.summary

    if article.analysis and isinstance(article.analysis, dict):
        versions = article.analysis.get("summary_versions")
        if isinstance(versions, dict):
            key = f"lv{user_level}"
            v = versions.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()

    if article.analysis and isinstance(article.analysis, dict):
        lc = article.analysis.get("level_content")
        if isinstance(lc, dict):
            lvk = f"lv{user_level}"
            lv_obj = lc.get(lvk)
            if isinstance(lv_obj, dict):
                s = lv_obj.get("summary")
                if isinstance(s, str) and s.strip():
                    return s.strip()

    return display_summary


# (NEW) main용 summary pick (analyze_news_main 포맷)
def _pick_main_display_summary(article: NewsArticle) -> str:
    """
    main 카드/메인 요약에 보여줄 summary:
    1) article.analysis.analysis.summary (analyze_news_main 포맷)
    2) article.summary
    """
    if article.analysis and isinstance(article.analysis, dict):
        a = article.analysis.get("analysis")
        if isinstance(a, dict):
            s = a.get("summary")
            if isinstance(s, str) and s.strip():
                return s.strip()
    return article.summary


def _build_tags(article: NewsArticle, is_my_stock: bool) -> List[str]:
    tags: List[str] = []
    if is_my_stock:
        tags.append("내 보유 종목")

    if article.analysis and isinstance(article.analysis, dict):
        kws = article.analysis.get("keywords", [])
        if isinstance(kws, list) and kws:
            tags.extend([k for k in kws[:2] if isinstance(k, str) and k.strip()])

    if not tags:
        if article.sector:
            try:
                tags.append(NewsSector(article.sector).label)
            except Exception:
                tags.append(str(article.sector))
        if article.ticker:
            tags.append(article.ticker)
        if not tags:
            tags.append("뉴스")

    return list(dict.fromkeys(tags))


def _parse_market_filter(raw: str) -> str:
    mf = (raw or "all").strip().lower()
    if mf not in ("all", "domestic", "international"):
        mf = "all"
    return mf


def _base_queryset_by_market(market_filter: str) -> QuerySet[NewsArticle]:
    if market_filter == "domestic":
        return NewsArticle.objects.filter(market=NewsMarket.KR)
    if market_filter == "international":
        return NewsArticle.objects.filter(market=NewsMarket.INTERNATIONAL)
    return NewsArticle.objects.filter(market__in=[NewsMarket.KR, NewsMarket.INTERNATIONAL])


# =========================================================
# (NEW) Sector list + Sector news APIs for Home layout
# =========================================================
class NewsSectorListView(APIView):
    """
    GET /api/news/sectors/?market=all|domestic|international

    return:
    {
      "market": "all",
      "items": [
        {"sector":"SEMICONDUCTOR_AI","label":"반도체 / AI","count":64},
        ...
      ]
    }
    """
    permission_classes = [AllowAny]

    def get(self, request):
        market_filter = _parse_market_filter(request.query_params.get("market", "all"))
        qs = _base_queryset_by_market(market_filter)

        rows = qs.values("sector").annotate(count=Count("id")).order_by("-count")
        count_map = {r["sector"]: int(r["count"]) for r in rows if r.get("sector")}

        items: List[Dict[str, Any]] = []
        for value, label in NewsSector.choices:
            c = count_map.get(value, 0)
            if c <= 0:
                continue
            items.append({"sector": value, "label": label, "count": c})

        if not items and NewsSector.ETC in count_map:
            items.append(
                {
                    "sector": NewsSector.ETC,
                    "label": NewsSector(NewsSector.ETC).label,
                    "count": count_map[NewsSector.ETC],
                }
            )

        return Response({"market": market_filter, "items": items})


class NewsBySectorView(APIView):
    """
    GET /api/news/by-sector/?sector=FINANCE_HOLDING&market=all&limit=20
    """
    permission_classes = [AllowAny]

    def get(self, request):
        market_filter = _parse_market_filter(request.query_params.get("market", "all"))
        sector = (request.query_params.get("sector") or "").strip()
        limit = _get_int_setting("NEWS_SECTOR_LIST_LIMIT_DEFAULT", 30)
        try:
            limit = int(request.query_params.get("limit") or limit)
        except Exception:
            limit = limit
        limit = max(1, min(limit, 100))

        valid = {c[0] for c in NewsSector.choices}
        if sector not in valid:
            sector = NewsSector.ETC

        qs = _base_queryset_by_market(market_filter).filter(sector=sector).order_by("-published_at")[:limit]
        label = NewsSector(sector).label if sector in valid else "기타"

        news = []
        for n in qs:
            news.append(
                {
                    "id": n.id,
                    "title": n.title,
                    "related_name": (n.related_name or "").strip(),
                    "ticker": (n.ticker or "").strip(),
                    "published_at": n.published_at,
                    "url": n.url,
                    "market": n.market,
                    "sector": n.sector,
                }
            )

        return Response({"sector": sector, "label": label, "market": market_filter, "news": news})


# =========================================================
# Worker Result Ingest API
# =========================================================
class WorkerResultIngestView(APIView):
    """
    POST /api/news/worker/result/
    """
    authentication_classes = []
    permission_classes = [AllowAny]

    def _get_bearer_token(self, request) -> str:
        auth = (request.headers.get("Authorization") or "").strip()
        if not auth:
            return ""
        parts = auth.split(" ", 1)
        if len(parts) != 2:
            return ""
        scheme, token = parts[0].strip().lower(), parts[1].strip()
        if scheme != "bearer":
            return ""
        return token

    def _safe_float_0_1(self, v: Any, default: float = 0.0) -> float:
        try:
            f = float(v)
        except Exception:
            return default
        if f < 0.0:
            return 0.0
        if f > 1.0:
            return 1.0
        return f

    def post(self, request):
        expected = (getattr(settings, "NEWS_WORKER_TOKEN", "") or "").strip()
        if expected:
            token = self._get_bearer_token(request)
            if not token or token != expected:
                return Response({"ok": False, "error": "Unauthorized"}, status=401)

        payload = request.data if isinstance(request.data, dict) else {}
        article_id = payload.get("article_id") or payload.get("id")

        try:
            article_id = int(article_id)
        except Exception:
            return Response({"ok": False, "error": "article_id is required"}, status=400)

        sector = (payload.get("sector") or "").strip()
        valid_sectors = {c[0] for c in NewsSector.choices}
        if sector not in valid_sectors:
            sector = NewsSector.ETC

        confidence = self._safe_float_0_1(payload.get("confidence", 0.0), default=0.0)
        reason = (payload.get("reason") or "").strip()
        related_name = (payload.get("related_name") or "").strip()
        related_symbol = (payload.get("related_symbol") or "").strip()

        try:
            article = NewsArticle.objects.get(id=article_id)
        except NewsArticle.DoesNotExist:
            return Response({"ok": False, "error": "Article not found"}, status=404)

        article.sector = sector
        article.confidence = confidence
        article.related_name = related_name
        article.ticker = related_symbol
        article.save(update_fields=["sector", "confidence", "related_name", "ticker"])

        return Response({"ok": True, "article_id": article_id, "sector": sector, "confidence": confidence}, status=200)


# =========================================================
# Main list view (AI recommend)
# =========================================================
class NewsView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        profile = None
        if request.user.is_authenticated:
            try:
                profile = request.user.profile
            except Exception:
                profile = None

        market_filter = _parse_market_filter(request.query_params.get("market", "all"))
        base_news = _base_queryset_by_market(market_filter)

        my_stock_news: List[NewsArticle] = []
        sectors = "경제"
        risk = "일반"

        if profile:
            try:
                pf_portfolio = list(getattr(profile, "portfolio", []) or [])
            except Exception:
                pf_portfolio = []

            sectors = ", ".join(profile.sectors) if getattr(profile, "sectors", None) else "경제"
            risk = profile.risk_profile if getattr(profile, "risk_profile", None) else "일반"

            if pf_portfolio:
                if market_filter == "all":
                    kr_ticks, intl_ticks = _split_portfolio_by_market(pf_portfolio)

                    my_stock_kr = (
                        NewsArticle.objects.filter(market=NewsMarket.KR, ticker__in=kr_ticks).order_by("-published_at")[:1]
                        if kr_ticks
                        else NewsArticle.objects.none()
                    )
                    my_stock_intl = (
                        NewsArticle.objects.filter(market=NewsMarket.INTERNATIONAL, ticker__in=intl_ticks).order_by("-published_at")[:1]
                        if intl_ticks
                        else NewsArticle.objects.none()
                    )

                    my_stock_news = list(my_stock_kr) + list(my_stock_intl)
                    my_stock_news = _dedupe_by_title(my_stock_news)
                else:
                    my_stock_news_qs = base_news.filter(ticker__in=pf_portfolio).order_by("-published_at")[:2]
                    my_stock_news = list(my_stock_news_qs)

        keyword = (request.query_params.get("keyword") or "").strip()
        if keyword:
            my_stock_news = []
            query_text = f"{keyword} 관련 트렌드와 뉴스"
        else:
            query_text = f"{sectors} 산업의 트렌드와 {risk} 투자 정보"

        query_vec = embed_query(query_text)
        if not query_vec:
            qs = base_news.order_by("-published_at")[:15]
            user_level = _get_user_level(request)

            news_data = []
            for n in qs:
                news_data.append(
                    {
                        "id": n.id,
                        "title": n.title,
                        "summary": _pick_display_summary(n, user_level),
                        "ticker": n.ticker,
                        "tags": _build_tags(n, is_my_stock=False),
                        "published_at": n.published_at,
                        "url": n.url,
                        "image_url": n.image_url,
                        "market": n.market,
                    }
                )
            return Response({"news": news_data, "keywords": ["#경제", "#시장동향", "#투자"], "fallback": True})

        exclude_ids = [n.id for n in my_stock_news]
        candidate_count = 200
        if keyword:
            candidate_count = 400 if market_filter == "all" else 200

        vector_news_candidates = (
            base_news.exclude(id__in=exclude_ids)
            .exclude(embedding_local__isnull=True)
            .annotate(distance=CosineDistance("embedding_local", query_vec))
            .order_by("distance")[:candidate_count]
        )

        candidates_list = list(vector_news_candidates)

        if keyword:
            picked = candidates_list[:12] if market_filter == "all" else candidates_list[:4]
            picked = sorted(picked, key=lambda x: x.published_at, reverse=True)
        else:
            picked = sorted(candidates_list, key=lambda x: x.published_at, reverse=True)[
                : (12 if market_filter == "all" else 4)
            ]

        combined = _dedupe_by_title(list(my_stock_news) + list(picked))

        target_total = 15
        target_kr = _get_int_setting("NEWS_ALL_TARGET_KR", 8)
        target_intl = _get_int_setting("NEWS_ALL_TARGET_INTL", 7)

        if market_filter == "all":
            final_result = _balance_all_markets(
                items=combined,
                target_total=target_total,
                target_kr=target_kr,
                target_intl=target_intl,
            )
        else:
            final_result = combined[:target_total]

        user_level = _get_user_level(request)

        my_stock_ids = {n.id for n in my_stock_news}
        news_data = []
        for n in final_result:
            news_data.append(
                {
                    "id": n.id,
                    "title": n.title,
                    "summary": _pick_display_summary(n, user_level),
                    "ticker": n.ticker,
                    "tags": _build_tags(n, is_my_stock=(n.id in my_stock_ids)),
                    "published_at": n.published_at,
                    "url": n.url,
                    "image_url": n.image_url,
                    "market": n.market,
                }
            )

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


# =========================================================
# Detail summary view (LLM) - legacy multi-level
# =========================================================
class NewsSummaryView(APIView):
    """
    GET /api/news/<id>/summary/
    - analysis 없으면 analyze_news로 생성 후 저장
    - user_level에 맞춰 level_content 펼쳐서 반환
    """
    permission_classes = [AllowAny]

    def get(self, request, news_id: int):
        try:
            article = NewsArticle.objects.get(id=news_id)
        except NewsArticle.DoesNotExist:
            return Response({"error": "뉴스를 찾을 수 없습니다."}, status=404)

        analysis_data = article.analysis
        if not analysis_data:
            analysis_data = analyze_news(article, save_to_db=True)

        if not analysis_data:
            return Response({"error": "분석에 실패했습니다."}, status=500)

        final_analysis: Dict[str, Any] = dict(analysis_data) if isinstance(analysis_data, dict) else {}

        user_level = _get_user_level(request)

        lc = final_analysis.get("level_content")
        if isinstance(lc, dict):
            lk = f"lv{user_level}"
            level_data = lc.get(lk)
            if isinstance(level_data, dict):
                final_analysis.update(level_data)

                if "action_guide" in level_data and "investment_action" not in final_analysis:
                    ag = level_data.get("action_guide")
                    if isinstance(ag, str):
                        final_analysis["investment_action"] = [ag]
                    elif isinstance(ag, list):
                        final_analysis["investment_action"] = ag

        if not final_analysis.get("strategy_guide"):
            final_analysis["strategy_guide"] = {
                "short_term": "분석 데이터가 충분하지 않습니다.",
                "long_term": "추후 업데이트 될 예정입니다.",
            }

        return Response(
            {
                "success": True,
                "article_id": article.id,
                "article_title": article.title,
                "analysis": final_analysis,
            }
        )


# =========================================================
# (NEW) Detail summary view (LLM) - main single-format (Lv3-style)
# =========================================================
class NewsMainSummaryView(APIView):
    """
    GET /api/news/<id>/main-summary/
    - main 포맷(analyze_news_main) 분석이 없으면 생성 후 저장
    - level_content 펼침 없이 main 포맷 그대로 반환
    """
    permission_classes = [AllowAny]

    def get(self, request, news_id: int):
        try:
            article = NewsArticle.objects.get(id=news_id)
        except NewsArticle.DoesNotExist:
            return Response({"error": "뉴스를 찾을 수 없습니다."}, status=404)

        analysis_data = article.analysis

        # main 포맷인지 판별: analysis.analysis.summary 존재 여부로 판단
        is_main_format = False
        if isinstance(analysis_data, dict):
            a = analysis_data.get("analysis")
            if isinstance(a, dict):
                s = a.get("summary")
                if isinstance(s, str) and s.strip():
                    is_main_format = True

        if not is_main_format:
            analysis_data = analyze_news_main(article, save_to_db=True)

        if not analysis_data or not isinstance(analysis_data, dict):
            return Response({"error": "분석에 실패했습니다."}, status=500)

        return Response(
            {
                "success": True,
                "article_id": article.id,
                "article_title": article.title,
                "summary": _pick_main_display_summary(article),
                "analysis": analysis_data,
            }
        )
