# news/management/commands/crawl_news.py
from __future__ import annotations

import re
import time
from datetime import datetime, timezone as dt_timezone
from typing import Optional
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

import openai

from news.models import NewsArticle


class Command(BaseCommand):
    """
    국내 뉴스 크롤링 (섹션 URL 기반 공통 로직 적용)
    - 링크 후보 수집 -> 메뉴/섹션/허브 제거 -> 기사 가능성 판별 -> 디테일(OG/JSON-LD)로 확정
    - 저장(OpenAI embedding) + analyze_news(save_to_db=True)로 theme/Lv1~Lv5 저장
    """

    help = "국내(네이버금융/연합인포맥스/한국경제/매일경제) 뉴스 크롤링 후 DB 저장(+theme/Lv1~Lv5 선행 분석)."

    # -------------------------
    # Crawling limits / pacing
    # -------------------------
    MAX_PER_SOURCE = 80
    SLEEP_BETWEEN_ITEMS = 0.08
    SLEEP_BETWEEN_SOURCES = 0.25

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    )

    # -------------------------
    # Image filtering (완화)
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
    ]
    BAD_PATH_EXT = (".html", ".htm", ".php", ".aspx", ".jsp")

    VALIDATE_IMAGE_HEAD = True
    IMAGE_HEAD_TIMEOUT = 4
    MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10MB

    # -------------------------
    # URL/Title filtering 강화
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
    # Source URLs (기존 crawler 기반)
    # -------------------------
    NAVER_LIST_URL = "https://finance.naver.com/news/mainnews.naver"
    YONHAP_LIST_URL = "https://news.einfomax.co.kr/news/articleList.html?sc_section_code=S1N1"
    HANKYUNG_LIST_URL = "https://www.hankyung.com/economy"
    MK_LIST_URL = "https://www.mk.co.kr/news/economy/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    # -------------------------------
    # OpenAI embedding
    # -------------------------------
    def get_embedding(self, text: str):
        client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        resp = client.embeddings.create(input=text, model="text-embedding-3-small")
        return resp.data[0].embedding

    # -------------------------------
    # URL helpers
    # -------------------------------
    def _normalize_url(self, url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            parts = urlsplit(u)
            # query 유지(언론사별 view 파라미터가 기사 식별에 쓰이는 경우가 있어 유지)
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
    def _is_duplicate(self, title: str, url: str) -> bool:
        title_n = self._normalize_title(title)
        url_n = self._normalize_url_noquery(url)

        if title_n and NewsArticle.objects.filter(title=title_n).exists():
            return True
        if url_n and NewsArticle.objects.filter(url=url_n).exists():
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
        if len(t) < 8:
            return True
        if self.TITLE_ONLY_PIPES_RE.match(t):
            return True

        low = t.lower()
        for kw in self.MENU_TITLE_KEYWORDS:
            if kw and kw.lower() in low:
                return True

        if "·" in t and len(t) <= 16:
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
        if self.VALIDATE_IMAGE_HEAD and not self._is_real_image_by_head(u):
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
    # Detail fetch (OG + JSON-LD)
    # -------------------------------
    def _fetch_detail_signals(
        self, url: str
    ) -> tuple[Optional[str], Optional[str], Optional[datetime], Optional[str], bool]:
        """
        return: (og_image, og_desc, published_at, content_text, is_article_like)
        """
        try:
            res = self.session.get(url, timeout=10)
            if res.status_code >= 400:
                return None, None, None, None, False

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
                for s in soup.find_all("script", attrs={"type": "application/ld+json"})[:10]:
                    txt = (s.get_text() or "").strip()
                    if not txt:
                        continue
                    low = txt.lower()
                    if '"@type"' in low and ("newsarticle" in low or '"article"' in low or '"reportage"' in low):
                        is_article_like = True
                        break

            # 본문(가벼운 저장용): 너무 길면 자르기
            content_text = None
            # 사이트별 공통 selector는 어려워서, 가장 안전한 "article" 우선
            article_tag = soup.find("article")
            if article_tag:
                content_text = article_tag.get_text("\n", strip=True)
            else:
                # infomax selector fallback
                div = soup.select_one("#article-view-content-div")
                if div:
                    content_text = div.get_text("\n", strip=True)

            if content_text:
                content_text = content_text.strip()
                content_text = content_text[:4000] if len(content_text) > 4000 else content_text

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
        link_noquery = self._normalize_url_noquery(link)

        if not title or not link:
            return 0

        if self._looks_like_menu_or_section_title(title):
            return 0
        if not self._looks_like_article_url(link):
            return 0
        if self._is_duplicate(title, link_noquery):
            self.stdout.write(f"  - [{source_name}] (중복) {title[:30]}...")
            return 0

        # embedding
        emb_text = (summary or "").strip() or title
        try:
            vector = self.get_embedding(emb_text)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ 임베딩 생성 실패: {e}"))
            return 0

        try:
            with transaction.atomic():
                article = NewsArticle.objects.create(
                    title=title,
                    summary=summary,
                    content=content,
                    url=link_noquery,  # canonical(no query)
                    image_url=image_url,
                    sector=sector,
                    market=market,
                    published_at=published_at or timezone.now(),
                    embedding=vector,
                )

                # ✅ theme + Lv1~Lv5 저장
                from news.services.analyze_news import analyze_news
                analyze_news(article, save_to_db=True)

            self.stdout.write(f"  + [{source_name}] [New] {title[:40]}... (analyzed)")
            return 1
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

    # =========================================================================
    # Command entry
    # =========================================================================
    def handle(self, *args, **kwargs):
        if not getattr(settings, "OPENAI_API_KEY", None):
            self.stdout.write(self.style.ERROR("settings.OPENAI_API_KEY 가 설정되어 있지 않습니다."))
            return

        self.stdout.write("=========================================")
        self.stdout.write("📡 국내 뉴스 크롤링 (섹션/메뉴 제거 + OG/JSON-LD 기사 판별)")
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
    # 1) Naver Finance (list 구조가 안정적이라 list selector 활용)
    # =========================================================================
    def crawl_naver(self) -> int:
        self.stdout.write("\n>>> [1/4] 네이버 금융 뉴스 크롤링 중...")
        url = self.NAVER_LIST_URL
        headers = {"User-Agent": self.USER_AGENT}

        saved = 0
        try:
            res = self.session.get(url, headers=headers, timeout=10)
            res.encoding = "cp949"
            soup = BeautifulSoup(res.text, "html.parser")

            items = soup.select(".mainNewsList li")
            for li in items:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    a = li.select_one(".articleSubject a")
                    s = li.select_one(".articleSummary")
                    if not a or not s:
                        continue

                    title = a.get_text(strip=True)
                    link = urljoin("https://finance.naver.com", a.get("href") or "")

                    # 썸네일(네이버는 list에서 안정적)
                    image_url = None
                    img = li.select_one("img")
                    if img and img.get("src"):
                        base = (img.get("src") or "").split("?")[0]
                        image_url = f"{base}?type=w660"

                    raw_summary = s.get_text("\n", strip=True)
                    summary = raw_summary.split("\n")[0].strip() if raw_summary else title

                    # 네이버도 디테일 확인(허브/메뉴 섞임 방지)
                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(link)
                    if not is_article_like and not pub_dt:
                        continue

                    if og_desc:
                        summary = og_desc.strip()

                    # image는 og 우선
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
    # 2) Yonhap Infomax (기사 링크 패턴이 명확)
    # =========================================================================
    def crawl_yonhap_infomax(self) -> int:
        self.stdout.write("\n>>> [2/4] 연합인포맥스 크롤링 중...")
        url = self.YONHAP_LIST_URL
        headers = {"User-Agent": self.USER_AGENT}

        saved = 0
        try:
            res = self.session.get(url, headers=headers, timeout=10)
            res.encoding = res.apparent_encoding
            soup = BeautifulSoup(res.text, "html.parser")

            anchors = soup.find_all("a", href=True)
            processed = set()

            for a in anchors:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    href = (a.get("href") or "").strip()
                    if not href or href in self.BAD_HREF_EXACT:
                        continue
                    if any(href.lower().startswith(p) for p in self.BAD_HREF_PREFIXES):
                        continue

                    if "articleView.html" not in href or "idxno" not in href:
                        continue

                    link = href if href.startswith("http") else urljoin("https://news.einfomax.co.kr", href)
                    link = self._normalize_url(link)
                    link_noquery = self._normalize_url_noquery(link)

                    if link_noquery in processed:
                        continue
                    processed.add(link_noquery)

                    title = self._normalize_title(a.get_text(" ", strip=True) or "")
                    if self._looks_like_menu_or_section_title(title):
                        continue
                    if len(title) < 12:
                        continue

                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(link)
                    if not is_article_like and not pub_dt:
                        continue

                    summary = (og_desc or title).strip()
                    image_url = self._pick_valid_image_url(og_img)

                    inc = self.save_article(
                        title=title,
                        summary=summary,
                        link=link,
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
    # 3) Hankyung (list에서 anchor 대량 -> 디테일로 확정)
    # =========================================================================
    def crawl_hankyung(self) -> int:
        self.stdout.write("\n>>> [3/4] 한국경제(Hankyung) 크롤링 중...")
        url = self.HANKYUNG_LIST_URL
        headers = {"User-Agent": self.USER_AGENT}

        saved = 0
        try:
            res = self.session.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")

            anchors = soup.find_all("a", href=True)
            processed = set()

            for a in anchors:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    href = (a.get("href") or "").strip()
                    if not href or href in self.BAD_HREF_EXACT:
                        continue
                    if any(href.lower().startswith(p) for p in self.BAD_HREF_PREFIXES):
                        continue

                    # Hankyung는 /article/ 링크 위주
                    if "/article/" not in href:
                        continue

                    link = href if href.startswith("http") else urljoin("https://www.hankyung.com", href)
                    link = self._normalize_url(link)
                    link_noquery = self._normalize_url_noquery(link)

                    if link_noquery in processed:
                        continue
                    processed.add(link_noquery)

                    title = self._normalize_title(a.get_text(" ", strip=True) or "")
                    if self._looks_like_menu_or_section_title(title):
                        continue
                    if len(title) < 12:
                        continue

                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(link)
                    if not is_article_like and not pub_dt:
                        continue

                    summary = (og_desc or title).strip()
                    image_url = self._pick_valid_image_url(og_img)

                    inc = self.save_article(
                        title=title,
                        summary=summary,
                        link=link,
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
    # 4) MK (list에서 /news/ 위주 + 디테일로 확정)
    # =========================================================================
    def crawl_mk(self) -> int:
        self.stdout.write("\n>>> [4/4] 매일경제(MK) 크롤링 중...")
        url = self.MK_LIST_URL
        headers = {"User-Agent": self.USER_AGENT}

        saved = 0
        try:
            res = self.session.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")

            anchors = soup.find_all("a", href=True)
            processed = set()

            for a in anchors:
                if saved >= self.MAX_PER_SOURCE:
                    break
                try:
                    href = (a.get("href") or "").strip()
                    if not href or href in self.BAD_HREF_EXACT:
                        continue
                    if any(href.lower().startswith(p) for p in self.BAD_HREF_PREFIXES):
                        continue

                    # MK는 /news/ 형태가 많음
                    if "/news/" not in href:
                        continue

                    link = href if href.startswith("http") else urljoin("https://www.mk.co.kr", href)
                    link = self._normalize_url(link)
                    link_noquery = self._normalize_url_noquery(link)

                    if link_noquery in processed:
                        continue
                    processed.add(link_noquery)

                    title = self._normalize_title(a.get_text(" ", strip=True) or "")
                    if self._looks_like_menu_or_section_title(title):
                        continue
                    if len(title) < 12:
                        continue

                    og_img, og_desc, pub_dt, content_text, is_article_like = self._fetch_detail_signals(link)
                    if not is_article_like and not pub_dt:
                        continue

                    summary = (og_desc or title).strip()
                    image_url = self._pick_valid_image_url(og_img)

                    inc = self.save_article(
                        title=title,
                        summary=summary,
                        link=link,
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
