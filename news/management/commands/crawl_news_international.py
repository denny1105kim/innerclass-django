from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone as py_timezone
from typing import Optional
from urllib.parse import urlparse, urlsplit, urlunsplit

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from news.models import NewsArticle
from news.services.analyze_news import analyze_news

import openai


class Command(BaseCommand):
    help = (
        "NewsAPI 기반 해외 뉴스 크롤링 (MASTER_TERMS 배치 쿼리 호출) "
        "+ OpenAI embedding 저장 + Lv1~Lv5 선행 분석 저장 + theme 저장"
    )

    # -------------------------
    # Targets / limits
    # -------------------------
    MAX_ARTICLES = 200

    PAGE_SIZE = 50
    MAX_PAGES = 2
    DAYS_LOOKBACK = 3
    LANGUAGE = "en"
    MARKET = "International"

    # 요청 간격
    SLEEP_BETWEEN_PAGES = 0.2
    SLEEP_BETWEEN_BATCHES = 0.35

    # -------------------------
    # Image filtering
    # -------------------------
    BAD_IMAGE_PATTERNS = [
        r"placeholder",
        r"default",
        r"no[-_ ]?image",
        r"no[-_ ]?photo",
        r"image[-_ ]?not[-_ ]?available",
        r"not[-_ ]?found",
        r"spacer",
        r"sprite",
        r"blank",
        r"transparent",
        r"1x1",
        r"pixel",
        r"favicon",
        r"icon",
        r"logo",
        r"thumb",
    ]
    BAD_PATH_EXT = (".html", ".htm", ".php", ".aspx", ".jsp")

    VALIDATE_IMAGE_HEAD = True
    IMAGE_HEAD_TIMEOUT = 4
    MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10MB

    # -------------------------
    # Source filtering
    # -------------------------
    # ✅ thefly.com은 이미지 품질 이슈로 제외
    BLOCKED_DOMAINS = {
        "thefly.com",
        "www.thefly.com",
    }

    # -------------------------
    # MASTER TERMS (100)
    # -------------------------
    MASTER_TERMS = [
        # AI / Semicon (25)
        "AI",
        "\"artificial intelligence\"",
        "LLM",
        "GenAI",
        "semiconductor",
        "chip",
        "GPU",
        "HBM",
        "DRAM",
        "foundry",
        "fab",
        "EUV",
        "ASML",
        "TSMC",
        "Nvidia",
        "NVDA",
        "AMD",
        "Intel",
        "Qualcomm",
        "ARM",
        "RISC-V",
        "\"data center\"",
        "inference",
        "training",
        "accelerator",
        # Battery / EV (15)
        "battery",
        "\"lithium-ion\"",
        "\"solid-state\"",
        "cathode",
        "anode",
        "electrolyte",
        "lithium",
        "nickel",
        "cobalt",
        "LFP",
        "NMC",
        "CATL",
        "\"battery recycling\"",
        "EV",
        "charging",
        # Clean Energy (12)
        "nuclear",
        "SMR",
        "uranium",
        "\"clean energy\"",
        "renewable",
        "solar",
        "wind",
        "hydrogen",
        "geothermal",
        "\"carbon capture\"",
        "grid",
        "\"energy storage\"",
        # Finance (10)
        "bank",
        "banking",
        "fintech",
        "payments",
        "Visa",
        "Mastercard",
        "JPMorgan",
        "Goldman",
        "\"Morgan Stanley\"",
        "\"interest rate\"",
        # Platform / Cloud (12)
        "cloud",
        "SaaS",
        "platform",
        "Microsoft",
        "Apple",
        "Google",
        "Alphabet",
        "Amazon",
        "Meta",
        "telecom",
        "5G",
        "\"app store\"",
        # Bio (10)
        "biotech",
        "pharma",
        "healthcare",
        "\"clinical trial\"",
        "\"drug approval\"",
        "FDA",
        "\"medical device\"",
        "Novo",
        "\"Eli Lilly\"",
        "Pfizer",
        # Auto (10)
        "automotive",
        "automaker",
        "Tesla",
        "BYD",
        "Toyota",
        "Volkswagen",
        "Hyundai",
        "Kia",
        "ADAS",
        "\"self-driving\"",
        # Shipbuilding (6)
        "shipbuilding",
        "shipyard",
        "maritime",
        "\"LNG carrier\"",
        "tanker",
        "\"offshore wind\"",
    ]

    # NewsAPI 키 관련 에러 코드(이 경우 다음 키로 교체)
    ROTATE_ON_STATUS = {401, 403, 429}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
                )
            }
        )

    # -------------------------
    # Time helpers (UTC normalize)
    # -------------------------
    def _to_utc(self, dt: Optional[datetime]) -> datetime:
        if not dt:
            return timezone.now().astimezone(py_timezone.utc)

        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, py_timezone.utc)

        return dt.astimezone(py_timezone.utc)

    # -------------------------
    # Keys
    # -------------------------
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

    # -------------------------
    # NewsAPI request with auto-rotation
    # -------------------------
    def _newsapi_get(self, base_url: str, params: dict) -> requests.Response:
        keys = self._get_newsapi_keys()
        last_err: Optional[str] = None

        for idx, api_key in enumerate(keys, start=1):
            params_with_key = dict(params)
            params_with_key["apiKey"] = api_key

            try:
                res = self.session.get(base_url, params=params_with_key, timeout=20)

                if res.status_code == 200:
                    return res

                if res.status_code in self.ROTATE_ON_STATUS:
                    last_err = f"{res.status_code} {res.text[:200]}"
                    self.stdout.write(
                        self.style.WARNING(
                            f"⚠️ NewsAPI 키 실패/한도 (status={res.status_code}) → 다음 키로 교체 ({idx}/{len(keys)})"
                        )
                    )
                    continue

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

    # -------------------------
    # Query batches (MASTER_TERMS 기반)
    # -------------------------
    def _build_query_batches(self, chunk: int = 10) -> list[str]:
        batches: list[str] = []
        chunk = max(3, min(chunk, 20))
        for i in range(0, len(self.MASTER_TERMS), chunk):
            terms = self.MASTER_TERMS[i : i + chunk]
            q = "(" + " OR ".join(terms) + ")"
            batches.append(q)
        return batches

    # -------------------------
    # Normalize / Duplicate
    # -------------------------
    def _normalize_title(self, title: str) -> str:
        t = title or ""
        t = re.sub(r"^[\d\.\s]+", "", t)
        t = " ".join(t.split()).strip()
        return t

    def _normalize_url(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            parts = urlsplit(u)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        except Exception:
            return u

    def _is_duplicate(self, title: str, url: str) -> bool:
        title_n = self._normalize_title(title)
        url_n = self._normalize_url(url)

        if url_n and NewsArticle.objects.filter(url=url_n).exists():
            return True
        if title_n and NewsArticle.objects.filter(title=title_n).exists():
            return True
        return False

    # -------------------------
    # Source filtering
    # -------------------------
    def _is_blocked_source(self, url: str, source_name: str) -> bool:
        """
        thefly.com 차단.
        - URL 도메인 기준
        - source_name(NewsAPI source.name)에도 혹시 포함되면 차단
        """
        u = (url or "").strip().lower()
        sn = (source_name or "").strip().lower()

        try:
            host = (urlparse(u).netloc or "").lower()
        except Exception:
            host = ""

        if host in self.BLOCKED_DOMAINS:
            return True

        # host가 없거나 변형된 케이스 대비 (subdomain 포함)
        if host and any(host == d or host.endswith("." + d.lstrip("www.")) for d in self.BLOCKED_DOMAINS):
            return True

        if "thefly.com" in u:
            return True
        if "thefly" in sn:
            # source_name이 "The Fly" 등으로 오는 경우 방어적 차단
            return True

        return False

    # -------------------------
    # Image checks
    # -------------------------
    def _looks_like_bad_image_url(self, image_url: str) -> bool:
        u = (image_url or "").strip()
        if not u:
            return True
        if not (u.startswith("http://") or u.startswith("https://")):
            return True

        path = urlparse(u).path.lower()
        if path.endswith(self.BAD_PATH_EXT):
            return True

        low = u.lower()
        for pat in self.BAD_IMAGE_PATTERNS:
            if re.search(pat, low):
                return True
        return False

    def _is_real_image_by_head(self, image_url: str) -> bool:
        try:
            r = self.session.head(image_url, timeout=self.IMAGE_HEAD_TIMEOUT, allow_redirects=True)
            if r.status_code >= 400:
                return False

            ctype = (r.headers.get("Content-Type") or "").lower()
            clen = r.headers.get("Content-Length")
            if clen:
                try:
                    if int(clen) > self.MAX_IMAGE_BYTES:
                        return False
                except Exception:
                    pass

            if ctype.startswith("image/"):
                return True

            # HEAD가 부정확한 서버 대비: Range GET
            rg = self.session.get(
                image_url,
                timeout=self.IMAGE_HEAD_TIMEOUT,
                allow_redirects=True,
                stream=True,
                headers={"Range": "bytes=0-2047"},
            )
            if rg.status_code >= 400:
                return False

            ctype2 = (rg.headers.get("Content-Type") or "").lower()
            clen2 = (rg.headers.get("Content-Length") or "").strip()
            if clen2:
                try:
                    if int(clen2) > self.MAX_IMAGE_BYTES:
                        return False
                except Exception:
                    pass

            return ctype2.startswith("image/")
        except Exception:
            return False

    def _pick_valid_image_url(self, image_url: Optional[str]) -> Optional[str]:
        u = (image_url or "").strip()
        if not u:
            return None
        if self._looks_like_bad_image_url(u):
            return None
        if self.VALIDATE_IMAGE_HEAD and not self._is_real_image_by_head(u):
            return None
        return u

    # -------------------------
    # Datetime parse
    # -------------------------
    def _parse_iso_dt(self, s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt, py_timezone.utc)
            return dt
        except Exception:
            return None

    # -------------------------
    # OpenAI Embedding
    # -------------------------
    def get_embedding(self, text: str):
        """
        text-embedding-3-small: 1536 dims
        """
        try:
            client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
            resp = client.embeddings.create(input=text, model="text-embedding-3-small")
            return resp.data[0].embedding
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ 임베딩 생성 실패: {e}"))
            return None

    # -------------------------
    # Save + Analyze (Lv1~Lv5 저장 + theme 저장은 analyze_news가 담당)
    # -------------------------
    def save_article(
        self,
        *,
        title: str,
        summary: str,
        link: str,
        image_url: Optional[str],
        source_name: str,
        content: Optional[str],
        published_at: Optional[datetime],
    ) -> int:
        title_n = self._normalize_title(title)
        link_n = self._normalize_url(link)

        if not title_n or not link_n:
            return 0

        # ✅ thefly.com 차단
        if self._is_blocked_source(link_n, source_name):
            self.stdout.write(f"  - [{source_name}] (blocked: thefly) {title_n[:60]}... -> skip")
            return 0

        if self._is_duplicate(title_n, link_n):
            self.stdout.write(f"  - [{source_name}] (중복) {title_n[:60]}...")
            return 0

        valid_image_url = self._pick_valid_image_url(image_url)
        if not valid_image_url:
            self.stdout.write(f"  - [{source_name}] (이미지 invalid/없음) {title_n[:60]}... -> skip")
            return 0

        # 임베딩 텍스트: summary 우선
        emb_text = (summary or title_n).strip() or title_n
        vector = self.get_embedding(emb_text)
        if not vector:
            self.stdout.write("    -> 벡터 생성 실패로 저장 건너뜀")
            return 0

        pub_utc = self._to_utc(published_at)

        try:
            with transaction.atomic():
                article = NewsArticle.objects.create(
                    title=title_n,
                    summary=summary,
                    content=content,
                    url=link_n,
                    image_url=valid_image_url,
                    market=self.MARKET,
                    published_at=pub_utc,
                    sector="금융/경제",
                    ticker=None,
                    embedding=vector,
                )
                analyze_news(article, save_to_db=True)

            self.stdout.write(f"  + [{source_name}] [New] {title_n[:60]}... (analyzed Lv1~Lv5 + themed)")
            return 1

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

    # -------------------------
    # Crawl 1 query batch
    # -------------------------
    def _crawl_with_query(self, query: str, *, from_str: str) -> int:
        base_url = "https://newsapi.org/v2/everything"
        saved = 0

        for page in range(1, self.MAX_PAGES + 1):
            if saved >= self.MAX_ARTICLES:
                break

            params = {
                "q": query,
                "language": self.LANGUAGE,
                "sortBy": "publishedAt",
                "pageSize": self.PAGE_SIZE,
                "page": page,
                "from": from_str,
            }

            try:
                res = self._newsapi_get(base_url, params)
                data = res.json()
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"  - NewsAPI 호출 실패(page={page}): {e}"))
                continue

            articles = data.get("articles", []) if isinstance(data, dict) else []
            if not articles:
                break

            # 안전 최신순 정렬
            articles.sort(key=lambda a: (a.get("publishedAt") or ""), reverse=True)

            for a in articles:
                if saved >= self.MAX_ARTICLES:
                    break

                title = (a.get("title") or "").strip()
                url = (a.get("url") or "").strip()
                if not title or not url:
                    continue

                # source name
                source_name = "NewsAPI"
                src = a.get("source") or {}
                if isinstance(src, dict):
                    source_name = (src.get("name") or "").strip() or source_name

                # ✅ thefly.com 차단 (가능한 빨리 거르기: 불필요한 이미지/임베딩/LLM 비용 방지)
                if self._is_blocked_source(url, source_name):
                    self.stdout.write(f"  - [{source_name}] (blocked: thefly) {title[:60]}... -> skip")
                    continue

                img = a.get("urlToImage")
                summary = (a.get("description") or title).strip()
                content = (a.get("content") or "").strip() or None
                pub_dt = self._parse_iso_dt(a.get("publishedAt"))

                inc = self.save_article(
                    title=title,
                    summary=summary,
                    link=url,
                    image_url=img,
                    source_name=source_name,
                    content=content,
                    published_at=pub_dt,
                )
                saved += inc

            time.sleep(self.SLEEP_BETWEEN_PAGES)

        return saved

    # -------------------------
    # Main
    # -------------------------
    def handle(self, *args, **kwargs):
        if not getattr(settings, "OPENAI_API_KEY", None):
            self.stdout.write(self.style.ERROR("settings.OPENAI_API_KEY 가 설정되어 있지 않습니다."))
            return

        keys = self._get_newsapi_keys()
        if not keys:
            self.stdout.write(
                self.style.ERROR("NEWSAPI 키가 없습니다. settings.NEWSAPI_KEYS 또는 settings.NEWSAPI_KEY 설정 필요")
            )
            return

        assert len(self.MASTER_TERMS) == 100, "MASTER_TERMS must be exactly 100"

        self.stdout.write("=========================================")
        self.stdout.write("🌍 International News Crawling (NewsAPI)")
        self.stdout.write("- OpenAI embedding: ON (text-embedding-3-small)")
        self.stdout.write("- LLM analyze(Lv1~Lv5 + theme): ON (analyze_news)")
        self.stdout.write(f"- keys: {len(keys)}개 (자동 교체 활성화)")
        self.stdout.write(f"- 이미지 필터: head_validate={self.VALIDATE_IMAGE_HEAD}")
        self.stdout.write(f"- blocked_domains: {', '.join(sorted(self.BLOCKED_DOMAINS))}")
        self.stdout.write(
            f"- lookback_days={self.DAYS_LOOKBACK}, page_size={self.PAGE_SIZE}, max_pages={self.MAX_PAGES}, max_articles={self.MAX_ARTICLES}"
        )
        self.stdout.write("=========================================")

        from_dt_utc = timezone.now().astimezone(py_timezone.utc) - timedelta(days=self.DAYS_LOOKBACK)
        from_str = from_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        queries = self._build_query_batches(chunk=10)

        total_saved = 0
        for idx, q in enumerate(queries, start=1):
            if total_saved >= self.MAX_ARTICLES:
                break

            self.stdout.write(f"\n>>> Query batch {idx}/{len(queries)}")
            saved = self._crawl_with_query(q, from_str=from_str)
            total_saved += saved

            if total_saved >= self.MAX_ARTICLES:
                break

            time.sleep(self.SLEEP_BETWEEN_BATCHES)

        self.stdout.write("=========================================")
        self.stdout.write(self.style.SUCCESS(f"✅ 완료: 신규 저장 {total_saved}건"))
        self.stdout.write("=========================================")
