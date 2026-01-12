from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from typing import Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from news.models import NewsArticle, NewsSector, NewsMarket
from news.services.embedding_news import embed_passages
from news.services.news_queue import enqueue_article_for_classify


@dataclass(frozen=True)
class GenericSource:
    name: str
    urls: list[str]
    base_url: str
    # 섹션/리스트 페이지에서 "기사 링크"로 인정할 패턴(하나라도 매칭되면 후보)
    must_contain_any: tuple[str, ...] = ()
    # 제외 패턴(하나라도 매칭되면 제외)
    exclude_any: tuple[str, ...] = ()
    # 링크 도메인 강제(빈 값이면 base_url 기반)
    domain_prefix: str = ""
    # URL이 "기사" 형태인지 정규식으로 강제 (하나라도 매칭되면 통과)
    must_match_regex: tuple[str, ...] = ()


class Command(BaseCommand):
    help = (
        "국내 주요 경제/산업 뉴스를 섹션 URL 기반으로 크롤링하여 DB 저장합니다. "
        "(로컬 임베딩 + 이미지 필터링/검증(완화) + Redis 큐 enqueue; LLM 분류는 별도 worker가 수행)"
    )

    # 섹션(URL) 1개당 최대 신규 저장
    MAX_PER_SOURCE = 500

    # 요청 간격(과도한 트래픽 방지)
    SLEEP_BETWEEN_ITEMS = 0.08
    SLEEP_BETWEEN_SECTIONS = 0.25

    # ------------------------------------------------------
    # Image filtering / validation (완화)
    # ------------------------------------------------------
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
    ]
    BAD_PATH_EXT = (".html", ".htm", ".php", ".aspx", ".jsp")

    VALIDATE_IMAGE_HEAD = True
    IMAGE_HEAD_TIMEOUT = 4
    MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10MB

    # ------------------------------------------------------
    # URL / Title filtering 강화 (메인/섹션/메뉴 링크 차단)
    # ------------------------------------------------------
    ARTICLE_DATE_RE = re.compile(r"/20\d{2}/\d{2}/\d{2}/")
    ARTICLE_HTMLDIR_RE = re.compile(r"/site/data/html_dir/")

    # "기사"일 확률이 높은 공통 패턴
    ARTICLE_LIKELY_RE_LIST = [
        ARTICLE_DATE_RE,
        ARTICLE_HTMLDIR_RE,
        re.compile(r"/article/"),
        re.compile(r"/news/view"),
        re.compile(r"/news/read"),
        re.compile(r"/news/articleView\.html"),
        re.compile(r"/view\.php"),
        re.compile(r"/view/"),
        re.compile(r"/mtview\.php"),
        re.compile(r"/NewsView/"),
        re.compile(r"/news/view/"),
        re.compile(r"/news/article/"),
        re.compile(r"/BP\?command=(mobile_view|article_view)"),
    ]

    # "기사 아닌" 허브/카테고리/메뉴 URL 패턴
    NON_ARTICLE_URL_RE_LIST = [
        re.compile(r"/(search|login|member|subscription|subscribe|mypage)(/|$)"),
        re.compile(r"/(photo|video|vod|podcast|gallery)(/|$)"),
        re.compile(r"/(section|category|categories|tag|tags|topic|topics)(/|$)"),
        re.compile(r"/(company|about|notice|event|press|policy)(/|$)"),
        re.compile(r"/news/?$"),
        re.compile(r"/news/section"),
        re.compile(r"/NewsList/"),
        re.compile(r"/Stock/?$"),
        re.compile(r"/economy/?$"),
        re.compile(r"/industry/?$"),
        re.compile(r"/stock/?$"),
        re.compile(r"/it/?$"),
        re.compile(r"/weeklybiz/?$"),
        re.compile(r"/(lists|list)\b"),
    ]

    MENU_TITLE_KEYWORDS = (
        "바로가기",
        "공지",
        "알림",
        "더보기",
        "전체보기",
        "전체",
        "검색",
        "로그인",
        "구독",
        "멤버십",
        "회원",
        "메뉴",
        "섹션",
        "카테고리",
        "라이브",
        "영상",
        "포토",
        "사진",
        "갤러리",
        "기획",
        "칼럼",
        "사설",
        "오피니언",
        "기자의",
        "특파원",
        "전문가",
        "시각",
        "방송",
        "미디어",
        "IT·인터넷",
        "전기·전자·통신",
        "朝鮮칼럼",
        "The Column",
        # 문제로 제시된 것들
        "enter on news",
        "ai 스튜디오",
        "the biz times",
        "스페셜에디션",
        "special edition",
        "special",
        "edition",
        "Desk pick",
    )

    MENU_TITLE_SHORT_RE = re.compile(r"^(국내|해외|경제|산업|증권|정치|사회|국제|문화|스포츠|연예|IT|테크)$")

    BAD_HREF_PREFIXES = ("javascript:", "mailto:", "tel:")
    BAD_HREF_EXACT = ("#", "")

    # 타이틀 노이즈 제거용
    TITLE_DATE_TIME_RE = re.compile(r"(20\d{2}[-./]\d{2}[-./]\d{2})(\s+\d{2}:\d{2})?")
    TITLE_ONLY_PIPES_RE = re.compile(r"^[\s\|\-–—·•\u00b7]+$")
    TITLE_ARROW_RE = re.compile(r"[❯›»>]+")
    TITLE_MULTI_SPACE_RE = re.compile(r"\s+")
    TITLE_CATEGORY_NOISE_RE = re.compile(r"^(국내|해외)\s*$")

    # ------------------------------------------------------
    # Section URLs
    # ------------------------------------------------------
    NAVER_URLS = ["https://finance.naver.com/news/"]

    HANKYUNG_URLS = [
        "https://www.hankyung.com/industry",
        "https://www.hankyung.com/industry/semicon-electronics",
        "https://www.hankyung.com/industry/auto-battery",
        "https://www.hankyung.com/industry/ship-marine",
        "https://www.hankyung.com/industry/steel-chemical",
        "https://www.hankyung.com/industry/robot-future",
    ]

    MK_URLS = [
        "https://www.mk.co.kr/news/economy/",
        "https://www.mk.co.kr/news/business/",
        "https://www.mk.co.kr/news/business/electronic/",
        "https://www.mk.co.kr/news/business/automobile/",
        "https://www.mk.co.kr/news/business/chemical/",
        "https://www.mk.co.kr/news/it/",
        "https://www.mk.co.kr/news/it/internet/",
        "https://www.mk.co.kr/news/it/science/",
        "https://www.mk.co.kr/news/stock/",
        "https://www.mk.co.kr/news/stock/business-information/",
    ]

    YONHAP_URLS = [
        "https://www.yna.co.kr/economy/index?site=navi_economy_depth01",
        "https://www.yna.co.kr/industry/all",
        "https://www.yna.co.kr/industry/industrial-enterprise",
        "https://www.yna.co.kr/industry/electronics",
        "https://www.yna.co.kr/industry/heavy-chemistry",
        "https://www.yna.co.kr/industry/automobile",
        "https://www.yna.co.kr/industry/energy-resource",
        "https://www.yna.co.kr/industry/technology-science",
        "https://www.yna.co.kr/industry/bioindustry-health",
        "https://www.yna.co.kr/market-plus/all",
        "https://www.yna.co.kr/market-plus/domestic-stock",
    ]

    CHOSUN_URLS = [
        "https://www.chosun.com/economy/tech_it/",
        "https://www.chosun.com/economy/auto/",
        "https://www.chosun.com/economy/real_estate/",
        "https://www.chosun.com/economy/science/",
        "https://www.chosun.com/weeklybiz/",
    ]

    # ------------------------------------------------------
    # Generic sources
    # ------------------------------------------------------
    GENERIC_SOURCES: list[GenericSource] = [
        GenericSource(
            name="DaumFinance",
            urls=["https://finance.daum.net/news"],
            base_url="https://finance.daum.net",
            must_contain_any=("/news/",),
            exclude_any=("/news/home",),
            must_match_regex=(r"/news/[^/]{2,}$",),
        ),
        GenericSource(
            name="Sedaily",
            urls=["https://www.sedaily.com/NewsList/GD", "https://www.sedaily.com/Stock"],
            base_url="https://www.sedaily.com",
            must_contain_any=("/NewsView/",),
            must_match_regex=(r"/NewsView/\d+",),
        ),
        GenericSource(
            name="Asiae",
            urls=["https://www.asiae.co.kr/list/economy"],
            base_url="https://www.asiae.co.kr",
            must_contain_any=("/article/",),
            must_match_regex=(r"/article/\d+",),
        ),
        GenericSource(
            name="Edaily",
            urls=["https://www.edaily.co.kr/news/section/economy"],
            base_url="https://www.edaily.co.kr",
            must_contain_any=("/news/read", "/news/news_detail"),
            must_match_regex=(r"/news/read", r"/news/news_detail"),
        ),
        GenericSource(
            name="Fnnews",
            urls=["https://www.fnnews.com/economy"],
            base_url="https://www.fnnews.com",
            must_contain_any=("/news/",),
            must_match_regex=(r"/news/\d+", r"/news/\w+\d+"),
        ),
        GenericSource(
            name="MoneyToday",
            urls=["https://news.mt.co.kr/newsList.html?type=ent"],
            base_url="https://news.mt.co.kr",
            must_contain_any=("/mtview.php",),
            must_match_regex=(r"/mtview\.php\?no=\d+",),
        ),
        GenericSource(
            name="Etoday",
            urls=["https://www.etoday.co.kr/news/section/subsection?MID=1200"],
            base_url="https://www.etoday.co.kr",
            must_contain_any=("/news/view/",),
            must_match_regex=(r"/news/view/\d+",),
        ),
        GenericSource(
            name="NewsPim",
            urls=["https://www.newspim.com/news/lists/?category=eco"],
            base_url="https://www.newspim.com",
            must_contain_any=("/news/view",),
            must_match_regex=(r"/news/view",),
        ),
        GenericSource(
            name="MoneyS",
            urls=["https://www.moneys.co.kr/"],
            base_url="https://www.moneys.co.kr",
            must_contain_any=("/article/",),
            must_match_regex=(r"/article/\d+",),
        ),
        GenericSource(
            name="ChosunBiz",
            urls=["https://biz.chosun.com/"],
            base_url="https://biz.chosun.com",
            must_contain_any=("/site/data/html_dir/", "/economy/", "/industry/", "/stock/"),
            exclude_any=("/photo/", "/entertainments/", "/video/", "/vod/", "/subscription/", "/member/"),
            must_match_regex=(r"/20\d{2}/\d{2}/\d{2}/", r"/site/data/html_dir/"),
        ),
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

    # -------------------------------
    # Time helpers (UTC normalize)
    # -------------------------------
    def _to_utc(self, dt: Optional[datetime]) -> datetime:
        if not dt:
            now = timezone.now()
            if timezone.is_naive(now):
                now = timezone.make_aware(now, timezone.get_current_timezone())
            return now.astimezone(dt_timezone.utc)

        if timezone.is_naive(dt):
            dt = timezone.make_aware(dt, timezone.get_current_timezone())

        return dt.astimezone(dt_timezone.utc)

    def handle(self, *args, **kwargs):
        self.stdout.write("=========================================")
        self.stdout.write("📡 뉴스 크롤링 시스템 가동 시작 (로컬 임베딩 + Redis 큐 enqueue)")
        self.stdout.write("- LLM 분류/분석: OFF (sector-worker가 처리)")
        self.stdout.write(f"- 이미지 필터: head_validate={self.VALIDATE_IMAGE_HEAD} (실패해도 저장은 진행)")
        self.stdout.write(f"- max_per_section: {self.MAX_PER_SOURCE}")
        self.stdout.write("=========================================")

        total_saved = 0
        total_saved += self.crawl_naver()
        total_saved += self.crawl_yonhap()
        total_saved += self.crawl_hankyung()
        total_saved += self.crawl_mk()
        total_saved += self.crawl_chosun()
        total_saved += self.crawl_generic_sources()

        self.stdout.write("=========================================")
        self.stdout.write(self.style.SUCCESS(f"✅ 통합 크롤링 완료. (총 신규 저장: {total_saved}개)"))
        self.stdout.write("=========================================")

    # -------------------------------
    # Utilities
    # -------------------------------
    def _normalize_url(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            parts = urlsplit(u)
            # query 유지(기관/보도자료 등)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
        except Exception:
            return u

    def _normalize_url_noquery(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            parts = urlsplit(u)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        except Exception:
            return u

    def _clean_title_text(self, raw: str) -> str:
        """
        섹션/메뉴에서 섞여 들어오는 잡텍스트를 최대한 제거해서
        "기사 제목"만 남기도록 정제.
        """
        t = (raw or "").strip()
        if not t:
            return ""

        # 줄바꿈/탭 등 공백 통일
        t = t.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        t = self.TITLE_MULTI_SPACE_RE.sub(" ", t).strip()

        # 화살표/네비 표식 제거
        t = self.TITLE_ARROW_RE.sub("", t).strip()

        # 날짜/시간 토큰 제거(메뉴에 붙는 케이스)
        t = self.TITLE_DATE_TIME_RE.sub("", t).strip()

        # 양 끝 구분자 제거
        t = t.strip(" |·•-–—>›»❯")

        t = self.TITLE_MULTI_SPACE_RE.sub(" ", t).strip()
        return t[:500]

    def _normalize_title(self, title: str) -> str:
        t = title or ""
        t = re.sub(r"^[\d\.\s]+", "", t)
        t = self._clean_title_text(t)
        return t[:500]

    def _is_duplicate(self, title: str, url: str) -> bool:
        title_n = self._normalize_title(title)
        url_n = self._normalize_url_noquery(url)

        if title_n and NewsArticle.objects.filter(title=title_n).exists():
            return True
        if url_n and NewsArticle.objects.filter(url=url_n).exists():
            return True
        if title and NewsArticle.objects.filter(title=title).exists():
            return True
        if url and NewsArticle.objects.filter(url=url).exists():
            return True
        return False

    # -------------------------------
    #  “메뉴/섹션 링크” 판별(강화)
    # -------------------------------
    def _looks_like_menu_or_section_title(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return True

        if self.MENU_TITLE_SHORT_RE.match(t):
            return True
        if self.TITLE_CATEGORY_NOISE_RE.match(t):
            return True

        # 너무 짧으면 기사일 확률 낮음
        if len(t) < 8:
            return True

        # 기호만
        if self.TITLE_ONLY_PIPES_RE.match(t):
            return True

        low = t.lower()

        for kw in self.MENU_TITLE_KEYWORDS:
            if kw and (kw.lower() in low):
                return True

        if "·" in t and len(t) <= 16:
            return True

        if "enter on" in low and "news" in low:
            return True

        return False

    def _looks_like_article_url(self, url: str) -> bool:
        u = (url or "").strip()
        if not u:
            return False

        # non-article이면 즉시 컷
        for rx in self.NON_ARTICLE_URL_RE_LIST:
            if rx.search(u):
                return False

        # 강한 기사 시그널
        if self.ARTICLE_DATE_RE.search(u):
            return True
        if self.ARTICLE_HTMLDIR_RE.search(u):
            return True

        # 그 외는 likely 패턴
        for rx in self.ARTICLE_LIKELY_RE_LIST:
            if rx.search(u):
                return True

        return False

    # ----------------------------
    # Image filtering helpers (완화)
    # ----------------------------
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

    # -------------------------------
    # Detail page helpers (OG + JSON-LD)
    # -------------------------------
    def _fetch_og(self, url: str) -> tuple[Optional[str], Optional[str], Optional[datetime], bool]:
        """
        return: (og_image, og_desc, published_at, is_article_like)
        """
        try:
            res = self.session.get(url, timeout=10)
            if res.status_code >= 400:
                return None, None, None, False

            soup = BeautifulSoup(res.text, "html.parser")

            og_image = None
            og_desc = None
            published_at = None

            m_img = soup.find("meta", property="og:image")
            if m_img and m_img.get("content"):
                og_image = (m_img.get("content") or "").strip()

            m_desc = soup.find("meta", property="og:description")
            if m_desc and m_desc.get("content"):
                og_desc = (m_desc.get("content") or "").strip()

            m_pub = soup.find("meta", property="article:published_time")
            if m_pub and m_pub.get("content"):
                published_at = self._parse_iso_dt(m_pub.get("content"))

            is_article_like = False

            og_type = soup.find("meta", property="og:type")
            if og_type and (og_type.get("content") or "").strip().lower() in ("article", "news", "newsarticle"):
                is_article_like = True

            if not is_article_like:
                for s in soup.find_all("script", attrs={"type": "application/ld+json"})[:10]:
                    txt = (s.get_text() or "").strip()
                    if not txt:
                        continue
                    low = txt.lower()
                    if '"@type"' in low and ("newsarticle" in low or '"article"' in low or '"reportage"' in low):
                        is_article_like = True
                        break

            return og_image, og_desc, published_at, is_article_like
        except Exception:
            return None, None, None, False

    def _parse_iso_dt(self, s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if timezone.is_naive(dt):
                dt = timezone.make_aware(dt, timezone.get_current_timezone())
            return dt.astimezone(dt_timezone.utc)
        except Exception:
            return None

    # -------------------------------
    # Save + Enqueue
    # -------------------------------
    def save_article(
        self,
        title: str,
        summary: str,
        link: str,
        image_url: Optional[str],
        source_name: str,
        market: str = NewsMarket.KR,
        content: Optional[str] = None,
        published_at: Optional[datetime] = None,
    ) -> int:
        title = self._normalize_title(title)
        link_noquery = self._normalize_url_noquery(link)
        link = self._normalize_url(link)

        if not title or not link:
            return 0

        # 최종 방어: 메뉴/허브 제목 컷
        if self._looks_like_menu_or_section_title(title):
            return 0

        # 최종 방어: 기사형 URL 아니면 컷
        if not self._looks_like_article_url(link):
            return 0

        if self._is_duplicate(title, link_noquery):
            self.stdout.write(f"  - [{source_name}] (중복) {title[:30]}...")
            return 0

        valid_image_url = self._pick_valid_image_url(image_url)
        if not valid_image_url:
            self.stdout.write(f"  - [{source_name}] (이미지 없음/검증실패) {title[:30]}... -> keep(no image)")

        pub_utc = self._to_utc(published_at)

        try:
            vecs = embed_passages([(summary or title).strip()])
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 임베딩 실패: {e}"))
            return 0

        vector = vecs[0] if vecs else None
        if not vector:
            self.stdout.write("    -> 로컬 임베딩 실패로 저장 건너뜀")
            return 0

        try:
            with transaction.atomic():
                article = NewsArticle.objects.create(
                    title=title,
                    summary=summary,
                    content=content,
                    url=link_noquery,  # 저장은 canonical(no query)
                    image_url=valid_image_url,
                    sector=NewsSector.ETC,
                    market=market,
                    published_at=pub_utc,
                    embedding_local=vector,
                    related_name="",
                    ticker="",
                    confidence=0.0,
                )

            try:
                enqueue_article_for_classify(
                    article_id=article.id,
                    title=article.title,
                    content=(article.content or article.summary or ""),
                )
                self.stdout.write(f"  + [{source_name}] [New] {title[:30]}... (sector=ETC -> queued)")
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"    -> enqueue 실패(무시하고 계속): {e}"))

            return 1
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

    # ---------------------------------------------------------------------
    # Generic crawler
    # ---------------------------------------------------------------------
    def _is_candidate_link(self, href: str, source: GenericSource) -> bool:
        h = (href or "").strip()
        if not h or h in self.BAD_HREF_EXACT:
            return False

        low = h.lower()
        if any(low.startswith(p) for p in self.BAD_HREF_PREFIXES):
            return False

        for ex in source.exclude_any:
            if ex and ex in h:
                return False

        if source.must_contain_any:
            ok = False
            for m in source.must_contain_any:
                if m and m in h:
                    ok = True
                    break
            if not ok:
                return False

        if source.must_match_regex:
            ok = False
            for rpat in source.must_match_regex:
                if rpat and re.search(rpat, h):
                    ok = True
                    break
            if not ok:
                return False

        return True

    def _absolute_link(self, href: str, source: GenericSource, list_url: str) -> str:
        h = (href or "").strip()
        if not h:
            return ""

        if h.startswith("http://") or h.startswith("https://"):
            return self._normalize_url(h)

        if source.domain_prefix:
            return self._normalize_url(urljoin(source.domain_prefix, h))

        base = list_url if list_url.startswith("http") else source.base_url
        abs_url = urljoin(base, h)
        return self._normalize_url(abs_url)

    def crawl_generic_sources(self) -> int:
        total = 0
        self.stdout.write("\n=========================================")
        self.stdout.write("🧩 Generic Sources crawling start")
        self.stdout.write(f"- sources: {len(self.GENERIC_SOURCES)}")
        self.stdout.write("=========================================")

        for s in self.GENERIC_SOURCES:
            self.stdout.write(f"\n>>> [GENERIC] {s.name}")
            for list_url in s.urls:
                self.stdout.write(f"  - list: {list_url}")
                count = 0
                processed: set[str] = set()

                try:
                    res = self.session.get(list_url, timeout=12)
                    if res.status_code >= 400:
                        self.stdout.write(self.style.WARNING(f"    -> list fetch fail: {res.status_code}"))
                        continue

                    soup = BeautifulSoup(res.text, "html.parser")
                    anchors = soup.find_all("a", href=True)

                    for a in anchors:
                        if count >= self.MAX_PER_SOURCE:
                            break

                        href = (a.get("href") or "").strip()
                        if not self._is_candidate_link(href, s):
                            continue

                        link = self._absolute_link(href, s, list_url)
                        if not link:
                            continue

                        if not self._looks_like_article_url(link):
                            continue

                        link_noquery = self._normalize_url_noquery(link)
                        if link_noquery in processed:
                            continue
                        processed.add(link_noquery)

                        raw_title = (a.get_text(" ", strip=True) or "").strip()
                        title = self._normalize_title(raw_title)
                        if self._looks_like_menu_or_section_title(title):
                            continue

                        og_img, og_desc, pub_dt, og_is_article = self._fetch_og(link)

                        # OG/JSON-LD 기사 단서 없으면 컷(허브 제거 목적)
                        if not og_is_article and not pub_dt:
                            continue

                        summary = (og_desc or title).strip()
                        summary = self.TITLE_MULTI_SPACE_RE.sub(" ", summary).strip()

                        inc = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=og_img,
                            source_name=s.name,
                            market=NewsMarket.KR,
                            published_at=pub_dt,
                        )
                        count += inc
                        total += inc
                        time.sleep(self.SLEEP_BETWEEN_ITEMS)

                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"    -> generic crawl error: {e}"))

                time.sleep(self.SLEEP_BETWEEN_SECTIONS)

        return total

    # ---------------------------------------------------------------------
    # Specialized crawlers
    # ---------------------------------------------------------------------
    def crawl_naver(self) -> int:
        total = 0
        for url in self.NAVER_URLS:
            self.stdout.write(f"\n>>> [1/5] 네이버증권 크롤링 중... url={url}")
            count = 0
            try:
                response = self.session.get(url, timeout=10)
                response.encoding = "cp949"
                soup = BeautifulSoup(response.text, "html.parser")

                candidates = soup.find_all("a", href=True)
                processed = set()

                for a in candidates:
                    if count >= self.MAX_PER_SOURCE:
                        break
                    try:
                        href = (a.get("href") or "").strip()
                        if not href:
                            continue

                        if "read.naver" not in href and "/news/" not in href:
                            continue

                        link = urljoin("https://finance.naver.com", href)
                        link = self._normalize_url(link)

                        if not self._looks_like_article_url(link):
                            continue

                        link_noquery = self._normalize_url_noquery(link)
                        if not link_noquery or link_noquery in processed:
                            continue
                        processed.add(link_noquery)

                        title = self._normalize_title((a.get_text(" ", strip=True) or "").strip())
                        if self._looks_like_menu_or_section_title(title):
                            continue

                        og_img, og_desc, pub_dt, og_is_article = self._fetch_og(link)
                        if not og_is_article and not pub_dt:
                            continue

                        summary = (og_desc or title).strip()

                        inc = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=og_img,
                            source_name="Naver",
                            market=NewsMarket.KR,
                            published_at=pub_dt,
                        )
                        count += inc
                        total += inc
                        time.sleep(self.SLEEP_BETWEEN_ITEMS)
                    except Exception:
                        continue

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ 네이버증권 크롤링 오류: {e}"))

            time.sleep(self.SLEEP_BETWEEN_SECTIONS)

        return total

    def crawl_yonhap(self) -> int:
        total = 0
        for url in self.YONHAP_URLS:
            self.stdout.write(f"\n>>> [2/5] 연합뉴스 크롤링 중... url={url}")
            count = 0
            try:
                response = self.session.get(url, timeout=10)
                response.encoding = response.apparent_encoding
                soup = BeautifulSoup(response.text, "html.parser")

                candidates = soup.find_all("a", href=True)
                processed_links = set()

                for a_tag in candidates:
                    if count >= self.MAX_PER_SOURCE:
                        break
                    try:
                        href = (a_tag.get("href") or "").strip()
                        if not href:
                            continue

                        if "/view/" not in href:
                            continue

                        link = urljoin("https://www.yna.co.kr", href)
                        link = self._normalize_url(link)

                        if not self._looks_like_article_url(link):
                            continue

                        link_noquery = self._normalize_url_noquery(link)
                        if not link_noquery or link_noquery in processed_links:
                            continue
                        processed_links.add(link_noquery)

                        title = self._normalize_title((a_tag.get_text(" ", strip=True) or "").strip())
                        if self._looks_like_menu_or_section_title(title):
                            continue

                        if len(title) < 12:
                            continue

                        og_img, og_desc, pub_dt, og_is_article = self._fetch_og(link)
                        if not og_is_article and not pub_dt:
                            continue

                        summary = (og_desc or title).strip()

                        inc = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=og_img,
                            source_name="Yonhap",
                            market=NewsMarket.KR,
                            published_at=pub_dt,
                        )
                        count += inc
                        total += inc
                        time.sleep(self.SLEEP_BETWEEN_ITEMS)
                    except Exception:
                        continue

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ 연합뉴스 크롤링 오류: {e}"))

            time.sleep(self.SLEEP_BETWEEN_SECTIONS)

        return total

    def crawl_hankyung(self) -> int:
        total = 0
        for url in self.HANKYUNG_URLS:
            self.stdout.write(f"\n>>> [3/5] 한국경제(Hankyung) 크롤링 중... url={url}")
            count = 0
            try:
                response = self.session.get(url, timeout=10)
                soup = BeautifulSoup(response.text, "html.parser")

                candidates = soup.find_all("a", href=True)
                processed = set()

                for a in candidates:
                    if count >= self.MAX_PER_SOURCE:
                        break
                    try:
                        href = (a.get("href") or "").strip()
                        if not href:
                            continue

                        if "/article/" not in href:
                            continue

                        link = href if href.startswith("http") else urljoin("https://www.hankyung.com", href)
                        link = self._normalize_url(link)

                        if not self._looks_like_article_url(link):
                            continue

                        link_noquery = self._normalize_url_noquery(link)
                        if not link_noquery or link_noquery in processed:
                            continue
                        processed.add(link_noquery)

                        title = self._normalize_title((a.get_text(" ", strip=True) or "").strip())
                        if self._looks_like_menu_or_section_title(title):
                            continue

                        og_img, og_desc, pub_dt, og_is_article = self._fetch_og(link)
                        if not og_is_article and not pub_dt:
                            continue

                        summary = (og_desc or title).strip()

                        inc = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=og_img,
                            source_name="Hankyung",
                            market=NewsMarket.KR,
                            published_at=pub_dt,
                        )
                        count += inc
                        total += inc
                        time.sleep(self.SLEEP_BETWEEN_ITEMS)
                    except Exception:
                        continue

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ 한국경제 크롤링 오류: {e}"))

            time.sleep(self.SLEEP_BETWEEN_SECTIONS)

        return total

    def crawl_mk(self) -> int:
        total = 0
        for url in self.MK_URLS:
            self.stdout.write(f"\n>>> [4/5] 매일경제(MK) 크롤링 중... url={url}")
            count = 0
            try:
                response = self.session.get(url, timeout=10)
                soup = BeautifulSoup(response.text, "html.parser")

                candidates = soup.find_all("a", href=True)
                processed = set()

                for a in candidates:
                    if count >= self.MAX_PER_SOURCE:
                        break
                    try:
                        href = (a.get("href") or "").strip()
                        if not href:
                            continue

                        if "/news/" not in href:
                            continue

                        link = href if href.startswith("http") else urljoin("https://www.mk.co.kr", href)
                        link = self._normalize_url(link)

                        if not self._looks_like_article_url(link):
                            continue

                        link_noquery = self._normalize_url_noquery(link)
                        if not link_noquery or link_noquery in processed:
                            continue
                        processed.add(link_noquery)

                        title = self._normalize_title((a.get_text(" ", strip=True) or "").strip())
                        if self._looks_like_menu_or_section_title(title):
                            continue

                        og_img, og_desc, pub_dt, og_is_article = self._fetch_og(link)
                        if not og_is_article and not pub_dt:
                            continue

                        summary = (og_desc or title).strip()

                        inc = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=og_img,
                            source_name="MK",
                            market=NewsMarket.KR,
                            published_at=pub_dt,
                        )
                        count += inc
                        total += inc
                        time.sleep(self.SLEEP_BETWEEN_ITEMS)
                    except Exception:
                        continue

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ 매일경제 크롤링 오류: {e}"))

            time.sleep(self.SLEEP_BETWEEN_SECTIONS)

        return total

    def crawl_chosun(self) -> int:
        total = 0
        for url in self.CHOSUN_URLS:
            self.stdout.write(f"\n>>> [5/5] 조선일보 크롤링 중... url={url}")
            count = 0
            try:
                response = self.session.get(url, timeout=10)
                response.encoding = response.apparent_encoding
                soup = BeautifulSoup(response.text, "html.parser")

                candidates = soup.find_all("a", href=True)
                processed = set()

                for a in candidates:
                    if count >= self.MAX_PER_SOURCE:
                        break
                    try:
                        href = (a.get("href") or "").strip()
                        if not href or href == "#":
                            continue

                        low = href.lower()
                        if any(low.startswith(p) for p in self.BAD_HREF_PREFIXES):
                            continue

                        link = href if href.startswith("http") else urljoin("https://www.chosun.com", href)
                        link = self._normalize_url(link)

                        if not link.startswith("https://www.chosun.com/"):
                            continue

                        if not (self.ARTICLE_DATE_RE.search(link) or self.ARTICLE_HTMLDIR_RE.search(link)):
                            continue

                        link_noquery = self._normalize_url_noquery(link)
                        if link_noquery in processed:
                            continue
                        processed.add(link_noquery)

                        title = self._normalize_title((a.get_text(" ", strip=True) or "").strip())
                        if self._looks_like_menu_or_section_title(title):
                            continue

                        og_img, og_desc, pub_dt, og_is_article = self._fetch_og(link)

                        if not og_is_article and not pub_dt:
                            continue
                        if not og_desc:
                            continue  # 조선은 허브 섞임이 많아 desc 없는 건 제외

                        summary = (og_desc or title).strip()

                        inc = self.save_article(
                            title=title,
                            summary=summary,
                            link=link,
                            image_url=og_img,
                            source_name="Chosun",
                            market=NewsMarket.KR,
                            published_at=pub_dt,
                        )
                        count += inc
                        total += inc
                        time.sleep(self.SLEEP_BETWEEN_ITEMS)
                    except Exception:
                        continue

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ 조선일보 크롤링 오류: {e}"))

            time.sleep(self.SLEEP_BETWEEN_SECTIONS)

        return total
