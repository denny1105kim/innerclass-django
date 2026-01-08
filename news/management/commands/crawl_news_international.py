import time
import requests
from datetime import datetime
from typing import Iterable, Optional

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from django.db import transaction

import openai

from news.models import NewsArticle


class Command(BaseCommand):
    help = "NewsAPI 기반 해외(International) 뉴스(다양한 섹터/테마) 최신순 크롤링 후 DB 저장(+선행 분석). (API KEY 풀 자동 교체)"

    # =========================
    # 고정 설정 (ARGS 없이 운영)
    # =========================
    MAX_ARTICLES = 200          # 최종 저장 목표 (중복/실패로 실제는 그 이하일 수 있음)
    PAGE_SIZE = 100             # NewsAPI everything pageSize 최대 100
    DAYS_LOOKBACK = 3           # 최근 N일
    LANGUAGE = "en"
    MARKET = "International"
    DEFAULT_SECTOR = "금융/경제"

    # 카테고리별 쿼리 (다양성 확보용)
    QUERIES = {
        "macro": (
            "economy OR macro OR markets OR stocks OR equities OR earnings OR guidance OR "
            "\"interest rates\" OR inflation OR \"central bank\" OR Fed OR ECB OR BOJ OR "
            "recession OR GDP OR unemployment OR \"bond yields\" OR treasury"
        ),
        "ai_semis_bigtech": (
            "Nvidia OR NVDA OR AMD OR Intel OR Qualcomm OR TSMC OR ASML OR "
            "\"artificial intelligence\" OR AI chips OR semiconductors OR GPU OR "
            "Microsoft OR Apple OR Google OR Alphabet OR Amazon OR Meta"
        ),
        "energy_oil_gas": (
            "oil OR crude OR Brent OR WTI OR OPEC OR shale OR refinery OR gasoline OR "
            "\"natural gas\" OR LNG OR Exxon OR Chevron OR Shell OR BP"
        ),
        "renewables_cleantech": (
            "renewable OR solar OR wind OR hydrogen OR geothermal OR \"clean energy\" OR "
            "decarbonization OR \"carbon credits\" OR battery storage"
        ),
        "ev_auto_battery": (
            "EV OR electric vehicle OR Tesla OR BYD OR Rivian OR Lucid OR "
            "battery OR lithium-ion OR charging network OR autonomous driving"
        ),
        "financials": (
            "banks OR banking OR \"net interest margin\" OR fintech OR payments OR "
            "Visa OR Mastercard OR JPMorgan OR Goldman OR Morgan Stanley"
        ),
        "healthcare_biotech": (
            "biotech OR pharma OR pharmaceuticals OR FDA OR clinical trial OR "
            "Novo Nordisk OR Eli Lilly OR Pfizer OR Moderna"
        ),
        "industrials_defense": (
            "aerospace OR defense OR Boeing OR Airbus OR Lockheed OR Raytheon OR "
            "supply chain OR manufacturing OR industrial production"
        ),
        "materials_metals": (
            "copper OR lithium OR nickel OR cobalt OR rare earths OR iron ore OR steel OR "
            "mining OR \"critical minerals\""
        ),
        "consumer_retail": (
            "consumer spending OR retail OR e-commerce OR Walmart OR Costco OR "
            "Nike OR luxury goods OR travel demand"
        ),
        "crypto": (
            "Bitcoin OR BTC OR Ethereum OR ETH OR crypto market OR \"spot ETF\" OR "
            "SEC OR stablecoin"
        ),
    }

    # 200개 목표를 카테고리에 분배 (다양성 확보)
    CATEGORY_QUOTA = {
        "macro": 40,
        "ai_semis_bigtech": 25,
        "energy_oil_gas": 20,
        "renewables_cleantech": 20,
        "ev_auto_battery": 20,
        "financials": 15,
        "healthcare_biotech": 15,
        "industrials_defense": 15,
        "materials_metals": 15,
        "consumer_retail": 10,
        "crypto": 5,
    }

    # NewsAPI 키 관련 에러 코드(이 경우 다음 키로 교체)
    ROTATE_ON_STATUS = {401, 403, 429}

    def handle(self, *args, **kwargs):
        keys = self._get_newsapi_keys()
        if not keys:
            self.stdout.write(
                self.style.ERROR(
                    "NEWSAPI 키가 없습니다. settings.NEWSAPI_KEYS(리스트) 또는 settings.NEWSAPI_KEY(단일)를 설정하세요."
                )
            )
            return

        if not getattr(settings, "OPENAI_API_KEY", None):
            self.stdout.write(self.style.ERROR("settings.OPENAI_API_KEY 가 설정되어 있지 않습니다."))
            return

        self.stdout.write("=========================================")
        self.stdout.write("🌍 해외 뉴스 크롤링(NewsAPI) 시스템 가동 시작 (다양한 섹터/테마)")
        self.stdout.write(f"- keys: {len(keys)}개 (자동 교체 활성화)")
        self.stdout.write("=========================================")

        total_saved = self.crawl_newsapi_multiquery()

        self.stdout.write("=========================================")
        self.stdout.write(self.style.SUCCESS(f"✅ 해외 뉴스 크롤링 완료. (총 신규 저장: {total_saved}개)"))
        self.stdout.write("=========================================")

    # =========================================================
    # Key Pool
    # =========================================================
    def _get_newsapi_keys(self) -> list[str]:
        """
        우선순위:
        1) settings.NEWSAPI_KEYS (list[str])
        2) settings.NEWSAPI_KEY (single str)
        """
        keys = getattr(settings, "NEWSAPI_KEYS", None)
        if isinstance(keys, (list, tuple)):
            keys = [str(k).strip() for k in keys if str(k).strip()]
            return keys

        key = getattr(settings, "NEWSAPI_KEY", None)
        if isinstance(key, str) and key.strip():
            return [key.strip()]

        return []

    # =========================================================
    # NewsAPI request with auto-rotation
    # =========================================================
    def _newsapi_get(self, base_url: str, params: dict) -> requests.Response:
        """
        - apiKey는 params로 주입 (가장 안정적)
        - 401/403/429면 다음 키로 자동 교체
        - 네트워크 예외도 다음 키로 재시도
        """
        keys = self._get_newsapi_keys()
        last_err: Optional[str] = None

        for idx, api_key in enumerate(keys, start=1):
            params_with_key = dict(params)
            params_with_key["apiKey"] = api_key

            try:
                res = requests.get(base_url, params=params_with_key, timeout=20)

                if res.status_code == 200:
                    return res

                # 키/한도 문제면 다음 키로 교체
                if res.status_code in self.ROTATE_ON_STATUS:
                    last_err = f"{res.status_code} {res.text[:200]}"
                    self.stdout.write(
                        self.style.WARNING(
                            f"⚠️ NewsAPI 키 실패/한도 (status={res.status_code}) → 다음 키로 교체 ({idx}/{len(keys)})"
                        )
                    )
                    continue

                # 그 외 오류는 즉시 중단(재시도해도 의미 없는 경우가 많음)
                last_err = f"{res.status_code} {res.text[:200]}"
                break

            except requests.RequestException as e:
                last_err = str(e)
                self.stdout.write(
                    self.style.WARNING(
                        f"⚠️ NewsAPI 네트워크 오류 → 다음 키로 교체 ({idx}/{len(keys)}): {e}"
                    )
                )
                continue

        raise RuntimeError(f"NewsAPI 호출 실패: {last_err or 'unknown error'}")

    # =========================================================
    # OpenAI Embedding
    # =========================================================
    def get_embedding(self, text: str):
        try:
            client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
            response = client.embeddings.create(
                input=text,
                model="text-embedding-3-small",
            )
            return response.data[0].embedding
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ 임베딩 생성 실패: {e}"))
            return None

    # =========================================================
    # Save (국내 커맨드와 최대한 동일)
    # =========================================================
    def save_article(
        self,
        title: str,
        summary: str,
        link: str,
        image_url: str | None,
        source_name: str,
        sector: str = "기타",
        market: str = "International",
        content: str | None = None,
        published_at=None,
    ) -> int:
        # 중복 체크: 제목 또는 URL이 같으면 중복
        if NewsArticle.objects.filter(title=title).exists():
            self.stdout.write(f"  - [{source_name}] (중복-제목) {title[:15]}...")
            return 0

        if NewsArticle.objects.filter(url=link).exists():
            self.stdout.write(f"  - [{source_name}] (중복-URL) {title[:15]}...")
            return 0

        self.stdout.write(f"  + [{source_name}] [New] {title[:15]}...")

        # 임베딩: summary 기반 (NewsAPI content는 종종 잘림)
        vector = self.get_embedding(summary)
        if not vector:
            self.stdout.write("    -> 벡터 생성 실패로 저장 건너뜀")
            return 0

        try:
            published_at = published_at or timezone.now()

            with transaction.atomic():
                article = NewsArticle.objects.create(
                    title=title,
                    summary=summary,
                    content=content,
                    url=link,
                    image_url=image_url,
                    sector=sector,
                    market=market,
                    ticker=None,
                    published_at=published_at,
                    embedding=vector,
                )

                # LLM 선행 분석 및 저장 (국내 커맨드와 동일)
                from news.services import analyze_news
                analyze_news(article, save_to_db=True)

            return 1

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

    # =========================================================
    # NewsAPI Multi-Query Crawl
    # =========================================================
    def crawl_newsapi_multiquery(self) -> int:
        base_url = "https://newsapi.org/v2/everything"

        from_dt = timezone.now() - timezone.timedelta(days=self.DAYS_LOOKBACK)
        from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        total_saved = 0
        total_saved_target = self.MAX_ARTICLES

        for category, query in self.QUERIES.items():
            if total_saved >= total_saved_target:
                break

            quota = int(self.CATEGORY_QUOTA.get(category, 10))
            if quota <= 0:
                continue

            self.stdout.write(f"\n>>> [NewsAPI] category={category} quota={quota} 최신순 수집 중...")

            saved_in_category = 0
            seen_in_category = 0

            # quota가 100 넘지 않으므로 보통 1페이지로 충분하지만,
            # 중복/실패 대비로 최대 2페이지까지 시도 (필요시 늘릴 수 있음)
            max_pages = 2
            for page in range(1, max_pages + 1):
                if saved_in_category >= quota:
                    break
                if total_saved >= total_saved_target:
                    break

                remaining_cat = quota - saved_in_category

                # 중복 대비로 조금 더 요청 (단, NewsAPI max 100)
                page_size = min(self.PAGE_SIZE, max(1, remaining_cat * 2))
                page_size = min(page_size, self.PAGE_SIZE)

                params = {
                    "q": query,
                    "language": self.LANGUAGE,
                    "sortBy": "publishedAt",
                    "pageSize": page_size,
                    "page": page,
                    "from": from_str,
                }

                try:
                    res = self._newsapi_get(base_url, params)
                    data = res.json()

                    articles = data.get("articles") or []
                    if not articles:
                        self.stdout.write(f"  - articles=0 (category={category})")
                        break

                    # 최신순 재정렬(안전)
                    articles.sort(key=lambda a: (a.get("publishedAt") or ""), reverse=True)

                    for a in articles:
                        if saved_in_category >= quota:
                            break
                        if total_saved >= total_saved_target:
                            break

                        seen_in_category += 1

                        title = (a.get("title") or "").strip()
                        link = (a.get("url") or "").strip()
                        if not title or not link:
                            continue

                        summary = (a.get("description") or "").strip() or title
                        content = (a.get("content") or "").strip() or None
                        image_url = (a.get("urlToImage") or "").strip() or None
                        published_at = self._parse_published_at(a.get("publishedAt")) or timezone.now()

                        source_name = "NewsAPI"
                        src = a.get("source") or {}
                        if isinstance(src, dict):
                            source_name = (src.get("name") or "").strip() or source_name

                        # DB 스키마를 국내와 최대한 동일하게 유지: sector는 "금융/경제" 고정(원하면 category로 변경 가능)
                        sector = self.DEFAULT_SECTOR

                        saved = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=image_url,
                            source_name=source_name,
                            sector=sector,
                            market=self.MARKET,
                            content=content,
                            published_at=published_at,
                        )

                        if saved:
                            saved_in_category += 1
                            total_saved += 1

                    time.sleep(0.2)

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ NewsAPI 요청/파싱 실패(category={category}): {e}"))
                    break

            self.stdout.write(
                f"<<< category={category} done: saved={saved_in_category}/{quota}, seen={seen_in_category}"
            )

        return total_saved

    def _parse_published_at(self, s: str):
        # 예: 2026-01-08T08:12:00Z
        if not s:
            return None
        try:
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt, timezone=timezone.utc)
            return dt
        except Exception:
            return None
