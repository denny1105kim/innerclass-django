from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

import openai

from news.models import NewsArticle


@dataclass(frozen=True)
class CandidateItem:
    title: str
    link: str
    summary: str = ""
    image_url: Optional[str] = None


class Command(BaseCommand):
    """
    국내 뉴스 크롤링 (섹션 URL 기반 공통 로직 + 디테일(OG/JSON-LD) 검증 + 본문 품질 강화)
    - 링크 후보 수집 -> 메뉴/섹션/허브 제거 -> URL/타이틀 휴리스틱 -> 디테일(OG/JSON-LD) 기사 확정
    - 본문(content) 추출: 사이트별 selector + 정제 + boilerplate 제거 + 길이/품질 gate
    - 저장(embedding) + analyze_news(save_to_db=True)
    """

    help = "국내(네이버금융/연합인포맥스/한국경제/매일경제) 뉴스 크롤링 후 DB 저장(+theme/Lv1~Lv5 선행 분석)."

    # -------------------------
    # Crawling limits / pacing
    # -------------------------
    MAX_PER_SOURCE = 80
    MAX_CANDIDATES_PER_SOURCE = 260
    MAX_RAW_ANCHORS_SCAN = 2500

    SLEEP_BETWEEN_ITEMS = 0.08
    SLEEP_BETWEEN_SOURCES = 0.25

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )

    # -------------------------
    # Image filtering (완화 + 최적화)
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
        r"logo",
        r"icon",
    ]
    BAD_PATH_EXT = (".html", ".htm", ".php", ".aspx", ".jsp")

    VALIDATE_IMAGE_HEAD = True
    IMAGE_HEAD_TIMEOUT = 4
    MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10MB

    TRUSTED_IMAGE_NETLOCS = (
        "ssl.pstatic.net",
        "imgnews.pstatic.net",
        "file.mk.co.kr",
        "image.mk.co.kr",
        "img.hankyung.com",
        "image.hankyung.com",
        "photo.einfomax.co.kr",
        "newsimg.sedaily.com",
        "t1.daumcdn.net",
    )

    # -------------------------
    # URL/Title filtering
    # -------------------------
    ARTICLE_DATE_RE = re.compile(r"/20\d{2}/\d{2}/\d{2}/")
    ARTICLE_HTMLDIR_RE = re.compile(r"/site/data/html_dir/")

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
    ]

    NON_ARTICLE_URL_RE_LIST = [
        re.compile(r"/(search|login|member|subscription|subscribe|mypage)(/|$)"),
        re.compile(r"/(photo|video|vod|podcast|gallery)(/|$)"),
        re.compile(r"/(tag|tags|topic|topics)(/|$)"),
        re.compile(r"/(company|about|notice|event|press|policy)(/|$)"),
        re.compile(r"/news/?$"),
        re.compile(r"/news/section"),
        re.compile(r"/NewsList/"),
        re.compile(r"/(lists|list)\b"),
    ]

    # 메뉴성 제목 제거(완화 버전)
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
        "사설",
        "오피니언",
        "특파원",
        "전문가",
        "시각",
        "방송",
        "미디어",
        "朝鮮칼럼",
        "The Column",
        "Desk pick",
        "special edition",
        "스페셜에디션",
    )
    MENU_TITLE_SHORT_RE = re.compile(r"^(국내|해외|경제|산업|증권|정치|사회|국제|문화|스포츠|연예|IT|테크)$")

    BAD_HREF_PREFIXES = ("javascript:", "mailto:", "tel:")
    BAD_HREF_EXACT = ("#", "")

    TITLE_DATE_TIME_RE = re.compile(r"(20\d{2}[-./]\d{2}[-./]\d{2})(\s+\d{2}:\d{2})?")
    TITLE_ONLY_PIPES_RE = re.compile(r"^[\s\|\-–—·•\u00b7]+$")
    TITLE_ARROW_RE = re.compile(r"[❯›»>]+")
    TITLE_MULTI_SPACE_RE = re.compile(r"\s+")

    # -------------------------
    # URL canonical 정책
    # -------------------------
    KEEP_QUERY_NETLOCS = {
        "finance.naver.com",
        "news.einfomax.co.kr",
    }
    KEEP_QUERY_KEYS_BY_NETLOC: Dict[str, Tuple[str, ...]] = {
        "finance.naver.com": ("article_id", "office_id", "mode", "date", "page", "idx", "type"),
        "news.einfomax.co.kr": ("idxno",),
    }
    DROP_QUERY_KEYS_COMMON = (
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "fbclid",
        "gclid",
        "ref",
        "source",
        "spm",
    )

    # -------------------------
    # Duplicate 정책
    # -------------------------
    DUP_TITLE_LOOKBACK_DAYS = 2

    # -------------------------
    # Content 품질 gate
    # -------------------------
    MIN_CONTENT_CHARS = 400          # 너무 짧은 본문은 저장하지 않거나(혹은 fallback) 분석 품질 저하
    MAX_CONTENT_CHARS = 12000        # 과도 길이 방지(LLM 분석 비용/속도)
    CONTENT_FALLBACK_MAX = 4000      # fallback 텍스트 최대

    # boilerplate 제거 정규식(공통)
    RE_WHITESPACE = re.compile(r"[ \t\u00A0]+")
    RE_MULTI_NEWLINES = re.compile(r"\n{3,}")
    RE_EMAIL = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b")
    RE_PHONE = re.compile(r"(0\d{1,2})[-\s]?\d{3,4}[-\s]?\d{4}")
    RE_COPYRIGHT = re.compile(r"(무단|재배포|전재|복제|배포|저작권|copyright|all rights reserved)", re.IGNORECASE)
    RE_REPORTER = re.compile(r"(기자\s*[=·:])|(\b기자\b)", re.IGNORECASE)
    RE_PROMO = re.compile(r"(구독|광고|문의|제휴|보도자료|제보|이벤트|앱\s*다운로드|관련기사|이전기사|다음기사)", re.IGNORECASE)

    # -------------------------
    # Source URLs
    # -------------------------
    NAVER_LIST_URL = "https://finance.naver.com/news/mainnews.naver"
    YONHAP_LIST_URL = "https://news.einfomax.co.kr/news/articleList.html?sc_section_code=S1N1"
    HANKYUNG_LIST_URL = "https://www.hankyung.com/economy"
    MK_LIST_URL = "https://www.mk.co.kr/news/economy/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.USER_AGENT})

        # OpenAI client 재사용
        self.oa_client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

    # -------------------------------
    # OpenAI embedding
    # -------------------------------
    def get_embedding(self, text: str):
        resp = self.oa_client.embeddings.create(input=text, model="text-embedding-3-small")
        return resp.data[0].embedding

    # -------------------------------
    # URL helpers
    # -------------------------------
    def _strip_tracking_query(self, netloc: str, qs: Dict[str, List[str]]) -> Dict[str, List[str]]:
        for k in list(qs.keys()):
            if k in self.DROP_QUERY_KEYS_COMMON:
                qs.pop(k, None)
        allow = self.KEEP_QUERY_KEYS_BY_NETLOC.get(netloc)
        if allow:
            qs = {k: v for k, v in qs.items() if k in allow}
        return qs

    def _canonical_url(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            parts = urlsplit(u)
            if not parts.scheme or not parts.netloc:
                return u

            netloc = parts.netloc.lower()
            path = parts.path or "/"

            if netloc in self.KEEP_QUERY_NETLOCS:
                qs = parse_qs(parts.query, keep_blank_values=False)
                qs2 = self._strip_tracking_query(netloc, qs)
                q = urlencode([(k, vv) for k, vals in qs2.items() for vv in vals], doseq=True)
                return urlunsplit((parts.scheme, parts.netloc, path, q, ""))

            return urlunsplit((parts.scheme, parts.netloc, path, "", ""))
        except Exception:
            return u

    def _normalize_url(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            parts = urlsplit(u)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
        except Exception:
            return u

    # -------------------------------
    # Title helpers
    # -------------------------------
    def _clean_title_text(self, raw: str) -> str:
        t = (raw or "").strip()
        if not t:
            return ""
        t = t.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        t = self.TITLE_MULTI_SPACE_RE.sub(" ", t).strip()
        t = self.TITLE_ARROW_RE.sub("", t).strip()
        t = self.TITLE_DATE_TIME_RE.sub("", t).strip()
        t = t.strip(" |·•-–—>›»❯")
        t = self.TITLE_MULTI_SPACE_RE.sub(" ", t).strip()
        return t[:500]

    def _normalize_title(self, title: str) -> str:
        t = title or ""
        t = re.sub(r"^[\d\.\s]+", "", t)
        t = self._clean_title_text(t)
        return t[:500]

    # -------------------------------
    # duplicate
    # -------------------------------
    def _is_duplicate(self, *, title: str, canonical_url: str) -> bool:
        title_n = self._normalize_title(title)
        url_n = self._canonical_url(canonical_url)

        if url_n and NewsArticle.objects.filter(url=url_n).exists():
            return True

        if title_n:
            since = timezone.now() - timedelta(days=self.DUP_TITLE_LOOKBACK_DAYS)
            if NewsArticle.objects.filter(title=title_n, published_at__gte=since).exists():
                return True

        return False

    # -------------------------------
    # menu/section detection
    # -------------------------------
    def _looks_like_menu_or_section_title(self, title: str) -> bool:
        t = (title or "").strip()
        if not t:
            return True
        if self.MENU_TITLE_SHORT_RE.match(t):
            return True
        if len(t) <= 4:
            return True
        if self.TITLE_ONLY_PIPES_RE.match(t):
            return True

        low = t.lower()
        for kw in self.MENU_TITLE_KEYWORDS:
            if kw and kw.lower() in low:
                return True

        if "·" in t and len(t) <= 10:
            return True
        return False

    def _looks_like_article_url(self, url: str) -> bool:
        u = (url or "").strip()
        if not u:
            return False

        for rx in self.NON_ARTICLE_URL_RE_LIST:
            if rx.search(u):
                return False

        if self.ARTICLE_DATE_RE.search(u) or self.ARTICLE_HTMLDIR_RE.search(u):
            return True

        for rx in self.ARTICLE_LIKELY_RE_LIST:
            if rx.search(u):
                return True

        return False

    # -------------------------------
    # Image validation
    # -------------------------------
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

    def _is_trusted_image_host(self, image_url: str) -> bool:
        try:
            netloc = urlparse(image_url).netloc.lower()
            return any(netloc.endswith(x) for x in self.TRUSTED_IMAGE_NETLOCS)
        except Exception:
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
            return ctype2.startswith("image/")
        except Exception:
            return False

    def _pick_valid_image_url(self, image_url: Optional[str]) -> Optional[str]:
        u = (image_url or "").strip()
        if not u:
            return None
        if self._looks_like_bad_image_url(u):
            return None
        if self.VALIDATE_IMAGE_HEAD and not self._is_trusted_image_host(u):
            if not self._is_real_image_by_head(u):
                return None
        return u

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
    # JSON-LD helpers
    # -------------------------------
    def _jsonld_has_article_type(self, obj) -> bool:
        if obj is None:
            return False
        if isinstance(obj, dict):
            t = obj.get("@type") or obj.get("type")
            if isinstance(t, str):
                low = t.lower()
                if low in ("newsarticle", "article", "reportage"):
                    return True
            if isinstance(t, list):
                for x in t:
                    if isinstance(x, str) and x.lower() in ("newsarticle", "article", "reportage"):
                        return True

            for v in obj.values():
                if self._jsonld_has_article_type(v):
                    return True
            return False

        if isinstance(obj, list):
            return any(self._jsonld_has_article_type(x) for x in obj)
        return False

    # =========================================================================
    # Content extraction (품질 강화 핵심)
    # =========================================================================
    def _remove_junk_nodes(self, root: Tag) -> None:
        """
        root 아래에서 본문 품질에 해로운 노드 제거.
        """
        for sel in [
            "script",
            "style",
            "noscript",
            "iframe",
            "form",
            "button",
            "input",
            "svg",
            "canvas",
            "figure figcaption",
            "aside",
        ]:
            for n in root.select(sel):
                n.decompose()

        # class/id 기반 junk
        junk_keywords = [
            "ad",
            "ads",
            "banner",
            "promotion",
            "promo",
            "recommend",
            "related",
            "share",
            "sns",
            "comment",
            "reply",
            "footer",
            "header",
            "nav",
            "menu",
            "subscribe",
            "login",
            "copyright",
            "reporter",
            "press",
            "byline",
            "tool",
        ]
        for n in root.find_all(True):
            cls = " ".join(n.get("class", [])).lower()
            _id = (n.get("id") or "").lower()
            hit = any(k in cls for k in junk_keywords) or any(k in _id for k in junk_keywords)
            if hit:
                # 단, 본문 컨테이너 자체가 오탐될 수 있어 과한 제거 방지
                # 너무 큰 컨테이너(자식이 매우 많음)는 남기고, 작은 블록만 제거
                if len(list(n.descendants)) < 250:
                    n.decompose()

    def _text_from_node(self, node: Tag) -> str:
        """
        node에서 사람이 읽는 텍스트만 최대한 깔끔하게 추출.
        """
        # 링크 텍스트도 포함되도록 get_text 사용
        txt = node.get_text("\n", strip=True) if node else ""
        if not txt:
            return ""

        # 라인 단위 정리
        lines = []
        for raw in txt.split("\n"):
            s = raw.strip()
            if not s:
                continue
            s = self.RE_WHITESPACE.sub(" ", s).strip()

            # boilerplate 라인 제거
            low = s.lower()
            if self.RE_COPYRIGHT.search(s):
                continue
            if self.RE_PROMO.search(s):
                # “관련기사”, “구독” 등 안내문 제거(과도 제거 방지: 아주 긴 라인은 남김)
                if len(s) < 120:
                    continue
            if self.RE_EMAIL.search(s) and len(s) < 120:
                continue
            if self.RE_PHONE.search(s) and len(s) < 120:
                continue

            # 기자 라인 제거(짧은 byline 위주)
            if self.RE_REPORTER.search(s) and len(s) < 80:
                continue

            lines.append(s)

        out = "\n".join(lines).strip()
        out = self.RE_MULTI_NEWLINES.sub("\n\n", out)
        return out

    def _extract_by_selectors(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[Tag]:
        for sel in selectors:
            n = soup.select_one(sel)
            if n:
                return n
        return None

    def _extract_content(self, url: str, soup: BeautifulSoup) -> Optional[str]:
        """
        사이트별 본문 selector + 공통 정제 + 품질 gate.
        실패 시 article/div fallback로 회수하되 품질을 최대한 유지.
        """
        netloc = urlparse(url).netloc.lower()

        # 1) 사이트별 우선 selector
        node: Optional[Tag] = None

        if netloc.endswith("finance.naver.com"):
            # 네이버 금융 뉴스(구형/신형 레이아웃 대비)
            node = self._extract_by_selectors(
                soup,
                selectors=[
                    "#content",  # 금융뉴스 본문 컨테이너
                    "#newsct_article",
                    "#articleBodyContents",
                    "#contentarea_left",
                    "article",
                ],
            )

            # 좁은 본문만 남기기 위해 추가로 더 구체적인 후보 탐색
            if node:
                for sel in ["#newsct_article", "#articleBodyContents", ".articleCont", ".articleConts"]:
                    nn = node.select_one(sel)
                    if nn:
                        node = nn
                        break

        elif netloc.endswith("news.einfomax.co.kr"):
            node = self._extract_by_selectors(
                soup,
                selectors=[
                    "#article-view-content-div",
                    "article",
                    ".view_cont",
                    "#articleBody",
                ],
            )

        elif netloc.endswith("www.hankyung.com") or netloc.endswith("hankyung.com"):
            node = self._extract_by_selectors(
                soup,
                selectors=[
                    "article",
                    ".article-body",
                    ".article-body__content",
                    ".article-content",
                    "#articletxt",
                ],
            )

        elif netloc.endswith("www.mk.co.kr") or netloc.endswith("mk.co.kr"):
            node = self._extract_by_selectors(
                soup,
                selectors=[
                    "article",
                    "#articleBody",
                    ".news_cnt_detail",
                    ".article_body",
                    ".article_cnt",
                ],
            )

        # 2) 공통 fallback
        if not node:
            node = soup.find("article") or soup.select_one("main") or soup.body

        if not node:
            return None

        # 3) 정제: junk 제거 후 텍스트 추출
        try:
            self._remove_junk_nodes(node)
        except Exception:
            pass

        text = self._text_from_node(node)

        # 4) 품질 gate: 너무 짧으면 두 번째 fallback 시도(전체 article/body에서 재추출)
        if not text or len(text) < self.MIN_CONTENT_CHARS:
            # fallback 1: article 전체
            fb_node = soup.find("article")
            if fb_node and fb_node is not node:
                try:
                    self._remove_junk_nodes(fb_node)
                except Exception:
                    pass
                fb_text = self._text_from_node(fb_node)
                if fb_text and len(fb_text) > len(text):
                    text = fb_text

        if not text or len(text) < self.MIN_CONTENT_CHARS:
            # fallback 2: body에서 강하게 정제 후 추출(길이는 늘지만 잡음도 있으므로 상한 적용)
            fb_node2 = soup.body
            if fb_node2:
                try:
                    self._remove_junk_nodes(fb_node2)
                except Exception:
                    pass
                fb_text2 = self._text_from_node(fb_node2)
                if fb_text2:
                    text = fb_text2[: self.CONTENT_FALLBACK_MAX]

        if not text:
            return None

        # 5) 길이 상한
        if len(text) > self.MAX_CONTENT_CHARS:
            text = text[: self.MAX_CONTENT_CHARS].rstrip()

        return text.strip() or None

    # -------------------------------
    # Detail fetch (OG + JSON-LD + Content 강화)
    # -------------------------------
    def _fetch_detail_signals(
        self, url: str
    ) -> Tuple[Optional[str], Optional[str], Optional[datetime], Optional[str], bool]:
        """
        return: (og_image, og_desc, published_at, content_text, is_article_like)
        """
        try:
            res = self.session.get(url, timeout=10)
            if res.status_code >= 400:
                return None, None, None, None, False

            # 인코딩 보정
            try:
                if not res.encoding:
                    res.encoding = res.apparent_encoding
            except Exception:
                pass

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

            # 기사 단서: og:type/article 또는 JSON-LD NewsArticle
            is_article_like = False
            og_type = soup.find("meta", property="og:type")
            if og_type and (og_type.get("content") or "").strip().lower() in ("article", "news", "newsarticle"):
                is_article_like = True

            if not is_article_like:
                for s in soup.find_all("script", attrs={"type": "application/ld+json"})[:12]:
                    txt = (s.get_text() or "").strip()
                    if not txt:
                        continue
                    low = txt.lower()
                    if '"@type"' in low and ("newsarticle" in low or '"article"' in low or '"reportage"' in low):
                        is_article_like = True
                        break
                    try:
                        obj = json.loads(txt)
                        if self._jsonld_has_article_type(obj):
                            is_article_like = True
                            break
                    except Exception:
                        continue

            # 본문(content) 고품질 추출
            content_text = self._extract_content(url, soup)

            # 기사 판별 보강: 본문이 충분히 길면 article 가능성 가산
            if content_text and len(content_text) >= self.MIN_CONTENT_CHARS:
                is_article_like = True if is_article_like or True else False  # 명시적

            return og_image, og_desc, published_at, content_text, is_article_like
        except Exception:
            return None, None, None, None, False

    # -------------------------------
    # Save + Analyze (theme/Lv1~Lv5)
    # -------------------------------
    def save_article(
        self,
        *,
        title: str,
        summary: str,
        link: str,
        image_url: Optional[str],
        source_name: str,
        sector: str = "금융/경제",
        market: str = "Korea",
        content: Optional[str] = None,
        published_at: Optional[datetime] = None,
    ) -> int:
        title = self._normalize_title(title)
        link = self._normalize_url(link)
        canonical = self._canonical_url(link)

        if not title or not link:
            return 0

        if self._looks_like_menu_or_section_title(title):
            return 0

        # 1차 URL 휴리스틱
        if not self._looks_like_article_url(link) and not self._looks_like_article_url(canonical):
            return 0

        if self._is_duplicate(title=title, canonical_url=canonical):
            self.stdout.write(f"  - [{source_name}] (중복) {title[:40]}...")
            return 0

        # summary fallback: og_desc 없고 summary가 너무 짧으면 title로 보정
        summary = (summary or "").strip()
        if not summary:
            summary = title

        # embedding: summary 우선, 너무 짧으면 content 일부를 섞어 품질 향상
        emb_text = summary
        if len(emb_text) < 40 and content:
            emb_text = (summary + "\n" + content[:800]).strip()

        try:
            vector = self.get_embedding(emb_text)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ 임베딩 생성 실패: {e}"))
            return 0

        # DB create만 atomic, 분석은 트랜잭션 밖
        try:
            with transaction.atomic():
                article = NewsArticle.objects.create(
                    title=title,
                    summary=summary,
                    content=content,
                    url=canonical,
                    image_url=image_url,
                    sector=sector,
                    market=market,
                    published_at=published_at or timezone.now(),
                    embedding=vector,
                )
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

        try:
            from news.services.analyze_news import analyze_news

            analyze_news(article, save_to_db=True)
            self.stdout.write(f"  + [{source_name}] [New] {title[:50]}... (analyzed)")
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"  ! [{source_name}] 저장은 성공, 분석 실패: {e}"))

        return 1

    # =========================================================================
    # Candidate builders (무분별 URL 생성 방지)
    # =========================================================================
    def _iter_candidates_from_anchors(
        self,
        *,
        soup: BeautifulSoup,
        base_url: str,
        href_must_contain: Optional[str] = None,
        href_regex: Optional[re.Pattern] = None,
        container_selectors: Optional[List[str]] = None,
    ) -> Iterable[CandidateItem]:
        containers: List[Tag] = []
        if container_selectors:
            for sel in container_selectors:
                containers.extend(soup.select(sel))
        if not containers:
            containers = [soup]

        seen = set()
        scanned = 0
        kept = 0

        for c in containers:
            for a in c.find_all("a", href=True):
                scanned += 1
                if scanned > self.MAX_RAW_ANCHORS_SCAN:
                    return

                href = (a.get("href") or "").strip()
                if not href or href in self.BAD_HREF_EXACT:
                    continue
                if any(href.lower().startswith(p) for p in self.BAD_HREF_PREFIXES):
                    continue

                if href_must_contain and href_must_contain not in href:
                    continue
                if href_regex and not href_regex.search(href):
                    continue

                link = href if href.startswith("http") else urljoin(base_url, href)
                link = self._normalize_url(link)
                canonical = self._canonical_url(link)

                if not canonical or canonical in seen:
                    continue
                seen.add(canonical)

                title = self._normalize_title(a.get_text(" ", strip=True) or "")
                if not title:
                    continue
                if self._looks_like_menu_or_section_title(title):
                    continue
                if len(title) < 8:
                    continue

                kept += 1
                if kept > self.MAX_CANDIDATES_PER_SOURCE:
                    return

                yield CandidateItem(title=title, link=link)

    # =========================================================================
    # Command entry
    # =========================================================================
    def handle(self, *args, **kwargs):
        if not getattr(settings, "OPENAI_API_KEY", None):
            self.stdout.write(self.style.ERROR("settings.OPENAI_API_KEY 가 설정되어 있지 않습니다."))
            return

        self.stdout.write("=========================================")
        self.stdout.write("📡 국내 뉴스 크롤링 (후보 제한 + canonical URL + OG/JSON-LD + 본문 품질 강화)")
        self.stdout.write("=========================================")

        total_saved = 0
        total_saved += self.crawl_naver()
        time.sleep(self.SLEEP_BETWEEN_SOURCES)

        total_saved += self.crawl_yonhap_infomax()
        time.sleep(self.SLEEP_BETWEEN_SOURCES)

        total_saved += self.crawl_hankyung()
        time.sleep(self.SLEEP_BETWEEN_SOURCES)

        total_saved += self.crawl_mk()

        self.stdout.write("=========================================")
        self.stdout.write(self.style.SUCCESS(f"✅ 통합 크롤링 완료. (총 신규 저장: {total_saved}개)"))
        self.stdout.write("=========================================")

    # =========================================================================
    # 1) Naver Finance
    # =========================================================================
    def crawl_naver(self) -> int:
        self.stdout.write("\n>>> [1/4] 네이버 금융 뉴스 크롤링 중...")
        url = self.NAVER_LIST_URL

        saved = 0
        try:
            res = self.session.get(url, timeout=10)
            res.encoding = "cp949"
            soup = BeautifulSoup(res.text, "html.parser")

            items = soup.select(".mainNewsList li")[: self.MAX_CANDIDATES_PER_SOURCE]

            for li in items:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    a = li.select_one(".articleSubject a")
                    s = li.select_one(".articleSummary")
                    if not a:
                        continue

                    title = a.get_text(strip=True)
                    link = urljoin("https://finance.naver.com", a.get("href") or "")
                    link = self._normalize_url(link)

                    image_url = None
                    img = li.select_one("img")
                    if img and img.get("src"):
                        base = (img.get("src") or "").split("?")[0]
                        image_url = f"{base}?type=w660"

                    raw_summary = s.get_text("\n", strip=True) if s else ""
                    summary = raw_summary.split("\n")[0].strip() if raw_summary else title

                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(link)
                    if not is_article_like and not pub_dt:
                        continue

                    if og_desc:
                        summary = og_desc.strip()

                    image_url = self._pick_valid_image_url(og_img or image_url)

                    inc = self.save_article(
                        title=title,
                        summary=summary,
                        link=link,
                        image_url=image_url,
                        source_name="Naver",
                        sector="금융/경제",
                        market="Korea",
                        content=content_text,
                        published_at=pub_dt or timezone.now(),
                    )
                    saved += inc
                    time.sleep(self.SLEEP_BETWEEN_ITEMS)
                except Exception:
                    continue

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 네이버 크롤링 오류: {e}"))

        return saved

    # =========================================================================
    # 2) Yonhap Infomax
    # =========================================================================
    def crawl_yonhap_infomax(self) -> int:
        self.stdout.write("\n>>> [2/4] 연합인포맥스 크롤링 중...")
        url = self.YONHAP_LIST_URL

        saved = 0
        try:
            res = self.session.get(url, timeout=10)
            res.encoding = res.apparent_encoding
            soup = BeautifulSoup(res.text, "html.parser")

            candidates = list(
                self._iter_candidates_from_anchors(
                    soup=soup,
                    base_url="https://news.einfomax.co.kr",
                    href_must_contain="articleView.html",
                    href_regex=re.compile(r"[?&]idxno=\d+"),
                    container_selectors=["main", ".article-list", "#container", "body"],
                )
            )

            for it in candidates:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(it.link)
                    if not is_article_like and not pub_dt:
                        continue

                    summary = (og_desc or it.title).strip()
                    image_url = self._pick_valid_image_url(og_img)

                    inc = self.save_article(
                        title=it.title,
                        summary=summary,
                        link=it.link,
                        image_url=image_url,
                        source_name="Infomax",
                        sector="금융/경제",
                        market="Korea",
                        content=content_text,
                        published_at=pub_dt or timezone.now(),
                    )
                    saved += inc
                    time.sleep(self.SLEEP_BETWEEN_ITEMS)
                except Exception:
                    continue

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 연합인포맥스 크롤링 오류: {e}"))

        return saved

    # =========================================================================
    # 3) Hankyung
    # =========================================================================
    def crawl_hankyung(self) -> int:
        self.stdout.write("\n>>> [3/4] 한국경제(Hankyung) 크롤링 중...")
        url = self.HANKYUNG_LIST_URL

        saved = 0
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")

            candidates = list(
                self._iter_candidates_from_anchors(
                    soup=soup,
                    base_url="https://www.hankyung.com",
                    href_must_contain="/article/",
                    container_selectors=["main", ".news-list", ".section-content", "#container", "body"],
                )
            )

            for it in candidates:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(it.link)
                    if not is_article_like and not pub_dt:
                        continue

                    summary = (og_desc or it.title).strip()
                    image_url = self._pick_valid_image_url(og_img)

                    inc = self.save_article(
                        title=it.title,
                        summary=summary,
                        link=it.link,
                        image_url=image_url,
                        source_name="Hankyung",
                        sector="금융/경제",
                        market="Korea",
                        content=content_text,
                        published_at=pub_dt or timezone.now(),
                    )
                    saved += inc
                    time.sleep(self.SLEEP_BETWEEN_ITEMS)
                except Exception:
                    continue

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 한국경제 크롤링 오류: {e}"))

        return saved

    # =========================================================================
    # 4) MK
    # =========================================================================
    def crawl_mk(self) -> int:
        self.stdout.write("\n>>> [4/4] 매일경제(MK) 크롤링 중...")
        url = self.MK_LIST_URL

        saved = 0
        try:
            res = self.session.get(url, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")

            candidates = list(
                self._iter_candidates_from_anchors(
                    soup=soup,
                    base_url="https://www.mk.co.kr",
                    href_must_contain="/news/",
                    container_selectors=["main", ".news_list", ".sec_body", "#container", "body"],
                )
            )

            for it in candidates:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(it.link)
                    if not is_article_like and not pub_dt:
                        continue

                    summary = (og_desc or it.title).strip()
                    image_url = self._pick_valid_image_url(og_img)

                    inc = self.save_article(
                        title=it.title,
                        summary=summary,
                        link=it.link,
                        image_url=image_url,
                        source_name="MK",
                        sector="금융/경제",
                        market="Korea",
                        content=content_text,
                        published_at=pub_dt or timezone.now(),
                    )
                    saved += inc
                    time.sleep(self.SLEEP_BETWEEN_ITEMS)
                except Exception:
                    continue

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 매일경제 크롤링 오류: {e}"))

        return saved
