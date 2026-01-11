# news/management/commands/crawl_news_international.py
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

from news.models import NewsArticle, NewsSector, NewsMarket
from news.services.embedding_news import embed_passages
from news.services.news_queue import enqueue_article_for_classify


class Command(BaseCommand):
    help = (
        "NewsAPI 기반 해외 뉴스 크롤링 (쿼리 분할 호출) "
        "(로컬 임베딩 + 이미지 필터링/검증 + Redis 큐 enqueue; LLM 분류는 별도 worker가 수행)"
    )

    # -------------------------
    # NewsAPI limits
    # -------------------------
    PAGE_SIZE = 50
    MAX_PAGES = 2
    DAYS_LOOKBACK = 3
    LANGUAGE = "en"

    # ✅ market 통일: 모델 enum 사용
    MARKET = NewsMarket.INTERNATIONAL

    # 요청 간격
    SLEEP_BETWEEN_PAGES = 0.2
    SLEEP_BETWEEN_BATCHES = 0.35

    # -------------------------
    # Image filtering (국내 크롤러와 최대한 동일)
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

    def handle(self, *args, **kwargs):
        assert len(self.MASTER_TERMS) == 100, "MASTER_TERMS must be exactly 100"

        self.stdout.write("=========================================")
        self.stdout.write("🌍 International News Crawling (NewsAPI)")
        self.stdout.write("- LLM 분류/분석: OFF (sector-worker가 처리)")
        self.stdout.write(f"- 이미지 필터: head_validate={self.VALIDATE_IMAGE_HEAD}")
        self.stdout.write(
            f"- lookback_days={self.DAYS_LOOKBACK}, page_size={self.PAGE_SIZE}, max_pages={self.MAX_PAGES}"
        )
        self.stdout.write("=========================================")

        queries = self._build_query_batches(chunk=10)
        total_saved = 0

        for idx, query in enumerate(queries, start=1):
            self.stdout.write(f"\n>>> Query batch {idx}/{len(queries)}")
            total_saved += self._crawl_with_query(query)
            time.sleep(self.SLEEP_BETWEEN_BATCHES)

        self.stdout.write("=========================================")
        self.stdout.write(self.style.SUCCESS(f"✅ 완료: 신규 저장 {total_saved}건"))
        self.stdout.write("=========================================")

    def _build_query_batches(self, chunk: int = 10) -> list[str]:
        batches: list[str] = []
        chunk = max(3, min(chunk, 20))
        for i in range(0, len(self.MASTER_TERMS), chunk):
            terms = self.MASTER_TERMS[i : i + chunk]
            q = "(" + " OR ".join(terms) + ")"
            batches.append(q)
        return batches

    def _get_newsapi_keys(self) -> list[str]:
        keys = getattr(settings, "NEWSAPI_KEYS", None)
        if isinstance(keys, (list, tuple)):
            return [k for k in keys if k]
        key = getattr(settings, "NEWSAPI_KEY", None)
        return [key] if key else []

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

    def save_article(
        self,
        *,
        title: str,
        summary: str,
        link: str,
        image_url: Optional[str],
        source_name: str,
        market: str,
        content: Optional[str],
        published_at: Optional[datetime],
    ) -> int:
        title = self._normalize_title(title)
        link = self._normalize_url(link)

        if not title or not link:
            return 0

        if self._is_duplicate(title, link):
            self.stdout.write(f"  - [{source_name}] (중복) {title[:25]}...")
            return 0

        valid_image_url = self._pick_valid_image_url(image_url)
        if not valid_image_url:
            self.stdout.write(f"  - [{source_name}] (이미지 invalid/없음) {title[:25]}... -> skip")
            return 0

        emb_text = (summary or title).strip() or title

        try:
            vecs = embed_passages([emb_text])
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 임베딩 실패: {e}"))
            return 0

        vector = vecs[0] if vecs else None
        if not vector:
            self.stdout.write("    -> 로컬 임베딩 실패로 저장 건너뜀")
            return 0

        pub_utc = self._to_utc(published_at)

        try:
            with transaction.atomic():
                article = NewsArticle.objects.create(
                    title=title,
                    summary=summary,
                    content=content,
                    url=link,
                    image_url=valid_image_url,
                    market=market,
                    published_at=pub_utc,

                    sector=NewsSector.ETC,
                    related_name="",
                    ticker="",
                    confidence=0.0,

                    embedding_local=vector,
                )

            try:
                enqueue_article_for_classify(
                    article_id=article.id,
                    title=article.title,
                    content=(article.content or article.summary or ""),
                )
                self.stdout.write(f"  + [{source_name}] [New] {title[:25]}... (sector=ETC -> queued)")
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"    -> enqueue 실패(무시하고 계속): {e}"))

            return 1
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

    def _crawl_with_query(self, query: str) -> int:
        base_url = "https://newsapi.org/v2/everything"

        from_dt_utc = timezone.now().astimezone(py_timezone.utc) - timedelta(days=self.DAYS_LOOKBACK)
        from_str = from_dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        saved = 0
        keys = self._get_newsapi_keys()
        if not keys:
            self.stdout.write(
                self.style.ERROR("❌ NEWSAPI_KEY(S) 설정이 없습니다. settings에 NEWSAPI_KEY 또는 NEWSAPI_KEYS를 넣어주세요.")
            )
            return 0

        for page in range(1, self.MAX_PAGES + 1):
            params = {
                "q": query,
                "language": self.LANGUAGE,
                "sortBy": "publishedAt",
                "pageSize": self.PAGE_SIZE,
                "page": page,
                "from": from_str,
            }

            res = None
            last_err = None

            for key in keys:
                try:
                    params["apiKey"] = key
                    res = self.session.get(base_url, params=params, timeout=20)
                    if res.status_code == 200:
                        break
                    last_err = f"status={res.status_code} body={res.text[:180]}"
                except Exception as e:
                    last_err = str(e)
                    res = None

            if not res or res.status_code != 200:
                self.stdout.write(self.style.WARNING(f"  - NewsAPI 호출 실패(page={page}): {last_err}"))
                continue

            data = res.json() if (res.headers.get("Content-Type") or "").startswith("application/json") else {}
            articles = data.get("articles", []) if isinstance(data, dict) else []
            if not articles:
                break

            for a in articles:
                try:
                    title = (a.get("title") or "").strip()
                    url = (a.get("url") or "").strip()
                    if not title or not url:
                        continue

                    img = self._pick_valid_image_url(a.get("urlToImage"))
                    if not img:
                        continue

                    summary = (a.get("description") or title).strip()
                    content = (a.get("content") or "").strip() or None
                    pub_dt = self._parse_iso_dt(a.get("publishedAt"))

                    inc = self.save_article(
                        title=title,
                        summary=summary,
                        link=url,
                        image_url=img,
                        source_name="NewsAPI",
                        market=self.MARKET,  # ✅ 여기서도 enum값 저장
                        content=content,
                        published_at=pub_dt,
                    )
                    saved += inc
                except Exception:
                    continue

            time.sleep(self.SLEEP_BETWEEN_PAGES)

        return saved
