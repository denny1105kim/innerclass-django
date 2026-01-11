# apps/reco/management/commands/generate_trend_keywords_daily.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, List, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from ...models import TrendKeywordDaily, TrendKeywordNews, TrendScope
from main.services.gemini_client import get_gemini_client, ChatMessage


# =========================================================
# Config
# =========================================================
KEYWORD_LIMIT = 3

# ✅ 최종 저장 개수(키워드별)
NEWS_LIMIT = 15

# ✅ 후보 풀: 키워드별로 최대 100개까지 모은 후 선별
CANDIDATE_POOL_LIMIT = 100

# ✅ LLM에서 한 번에 요청할 뉴스 개수
BATCH_SIZE = 25

# ✅ 후보가 부족하면 추가 검색 반복 횟수
MAX_REFILL_ATTEMPTS = 10

REQUEST_TIMEOUT = 8.0
KST = ZoneInfo("Asia/Seoul")

# ✅ “과거 뉴스 절대 안됨”
MAX_AGE_DAYS = 2

# ✅ 본문 저장 최대 길이
CONTENT_MAX_CHARS = 6000

# ✅ 기사 본문 최소 길이(너무 짧으면 목록/메인/중계일 확률 높음)
MIN_ARTICLE_TEXT_CHARS = 180

BLOCKED_DOMAINS = {
    "example.com",
    "vertexaisearch.cloud.google.com",
    "webcache.googleusercontent.com",
    # 검색/중계 류(필요 시 확장)
    "news.google.com",
}

BLOCKED_HOST_KEYWORDS = (
    "vertexaisearch",
    "example.com",
)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# =========================================================
# Prompt
# =========================================================
TREND_JSON_INSTRUCTION = f"""
너는 현재 시각 기준 주식/금융 시장의 실시간 트렌드를 분석하는 AI 엔진이다.
Google Search(실시간 검색)를 반드시 활용하여 최신 정보를 바탕으로 답해라.

출력은 반드시 아래 JSON 포맷만 허용한다. (마크다운, 코드블록 금지)

{{
  "items": [
    {{
      "keyword": "키워드(5글자이내)",
      "reason": "선정 이유(2문장 이내, 리스크 1개 포함)",
      "news": [
        {{
          "title": "뉴스 제목",
          "summary": "뉴스 요약(1문장)",
          "link": "실제 기사 URL (중계/placeholder 금지)",
          "image_url": "이미지 URL (없으면 빈문자열)",
          "published_at": "발행일시(YYYY-MM-DD HH:MM, KST 권장)"
        }}
      ]
    }}
  ]
}}

[절대 규칙]
- items 개수는 정확히 {KEYWORD_LIMIT}개.
- keyword는 공백 포함 최대 5글자(또는 매우 짧게).
- news는 각 키워드당 최소 {BATCH_SIZE}개 이상을 제공하려고 노력해라(부족하면 최대한).
- news의 link는 반드시 가져와서 요약한 실제 기사 URL이어야 한다.
  - example.com 같은 placeholder 금지
  - vertexaisearch.cloud.google.com 같은 중계 URL 금지
- published_at은 가능하면 'YYYY-MM-DD HH:MM' 형식(KST)으로 채워라.

[최신성 강제]
- news는 "오늘(KST) 또는 최근 {MAX_AGE_DAYS}일 이내(KST)" 기사만 허용한다. (그 이전 금지)
- 부족하면 다른 매체의 최신 기사로 다시 찾아 채워라.
""".strip()


def _now_kst() -> datetime:
    return timezone.now().astimezone(KST)


def _build_user_msg(scope: str, now_kst: datetime) -> str:
    scope = (scope or "").strip().upper()
    base = (
        "Google Search 도구를 사용하여 '현재 시간(Real-time)'의 뉴스를 검색해라.\n"
        f"현재 KST 시각: {now_kst.strftime('%Y-%m-%d %H:%M')}\n"
        f"조건: 반드시 오늘 또는 최근 {MAX_AGE_DAYS}일 이내(KST) 기사만 사용.\n"
        "조건: link는 실제 기사 URL만. example.com/vertexaisearch 등 금지.\n"
        "조건: published_at은 YYYY-MM-DD HH:MM(KST)로 출력.\n"
    )

    if scope == TrendScope.KR:
        target = "한국(KR) 주식 시장 및 경제"
        ratio = f"키워드 {KEYWORD_LIMIT}개 모두 한국 관련 이슈로 선정."
    else:
        target = "미국(US) 주식 시장 및 경제"
        ratio = f"키워드 {KEYWORD_LIMIT}개 모두 미국 관련 이슈로 선정."

    return f"""{base}
대상 시장: {target}
요청 사항: {ratio}
각 키워드마다 관련 최신 뉴스 목록을 최대한 많이(최소 {BATCH_SIZE}개 목표) 채워라.
""".strip()


def _build_keyword_refill_msg(
    scope: str,
    keyword: str,
    now_kst: datetime,
    exclude_urls: Iterable[str],
    batch_size: int,
) -> str:
    scope = (scope or "").strip().upper()
    target = "한국(KR)" if scope == TrendScope.KR else "미국(US)"
    excl = "\n".join(f"- {u}" for u in list(exclude_urls)[:80])

    return f"""
Google Search 도구를 사용하여 최신 뉴스를 검색해라.
현재 KST 시각: {now_kst.strftime('%Y-%m-%d %H:%M')}

[목표]
키워드: "{keyword}" (대상 시장: {target})
news를 최소 {batch_size}개 이상 반환하려고 노력해라.
반드시 오늘 또는 최근 {MAX_AGE_DAYS}일 이내(KST) 기사만 허용.
link는 실제 기사 URL만 허용(placeholder/중계 URL 금지).
published_at은 YYYY-MM-DD HH:MM(KST)로 출력.

[이미 사용한 URL - 중복 금지]
{excl if excl else "(없음)"}

출력은 반드시 아래 JSON만:
{{
  "news": [
    {{
      "title": "뉴스 제목",
      "summary": "뉴스 요약(1문장)",
      "link": "실제 기사 URL",
      "image_url": "이미지 URL(없으면 빈문자열)",
      "published_at": "YYYY-MM-DD HH:MM"
    }}
  ]
}}
""".strip()


# =========================================================
# JSON helpers
# =========================================================
def _safe_json_load(s: str) -> dict:
    s = (s or "").strip()
    if not s:
        return {}

    if s.startswith("```"):
        lines = s.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()

    l = s.find("{")
    r = s.rfind("}")
    if l >= 0 and r >= 0 and r > l:
        s = s[l : r + 1]

    try:
        return json.loads(s)
    except Exception:
        return {}


def _sanitize_keyword(s: Any) -> str:
    kw = str(s or "").strip()
    if len(kw) > 7:
        kw = kw[:7]
    return kw


def _sanitize_text(s: Any, limit: int) -> str:
    return str(s or "")[:limit]


# =========================================================
# De-dup helpers (URL + Title)
# =========================================================
_TITLE_TRIM_PREFIX = re.compile(r"^\s*(\[[^\]]+\]|\([^)]+\)|<[^>]+>|[0-9]+[.)\]]\s*)\s*")
_TITLE_TRIM_SUFFIX = re.compile(r"\s*[-–—]\s*[^-–—]{1,25}\s*$")  # 끝의 매체명/기자명 류


def _normalize_title(title: str) -> str:
    """
    제목 기반 중복 제거용 정규화:
    - [속보], (종합) 같은 prefix 제거
    - 끝의 "- 조선일보" 같은 suffix 제거(대략적인 휴리스틱)
    - 공백 정리, 소문자화
    """
    t = (title or "").strip()
    if not t:
        return ""
    t = _TITLE_TRIM_PREFIX.sub("", t).strip()
    t = _TITLE_TRIM_SUFFIX.sub("", t).strip()
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t[:160]


# =========================================================
# URL validation + canonicalize (HARDENED)
# =========================================================
def _is_http_url(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False


def _is_blocked_url(url: str) -> bool:
    if not _is_http_url(url):
        return True
    host = (urlparse(url).netloc or "").lower().replace("www.", "")
    if host in BLOCKED_DOMAINS:
        return True
    if any(k in host for k in BLOCKED_HOST_KEYWORDS):
        return True
    return False


_REDIRECT_PARAM_KEYS = ("url", "u", "q", "target", "dest", "destination", "redirect", "redir")

# 섹션/목록/메인/랭킹 페이지로 자주 보이는 path 키워드(도메인 공통 휴리스틱)
_NON_ARTICLE_PATH_HINTS = (
    "/index",
    "/main",
    "/home",
    "/all",
    "/list",
    "/lists",
    "/section",
    "/sections",
    "/category",
    "/categories",
    "/market_cap",
    "/volume",
    "/rise_stocks",
    "/fall_stocks",
)


def _strip_fragment(url: str) -> str:
    try:
        u = urlparse(url)
        return urlunparse((u.scheme, u.netloc, u.path, u.params, u.query, ""))  # fragment 제거
    except Exception:
        return (url or "").strip()


def _unwrap_redirect_url(url: str) -> str:
    """
    1차: 리다이렉터 URL에서 실제 URL이 query param으로 들어있는 경우를 언랩.
    """
    u = (url or "").strip()
    if not _is_http_url(u):
        return u

    try:
        pu = urlparse(u)
        qs = parse_qs(pu.query)
        for k in _REDIRECT_PARAM_KEYS:
            vals = qs.get(k)
            if not vals:
                continue
            cand = (vals[0] or "").strip()
            cand = unquote(cand)
            if _is_http_url(cand):
                return cand
    except Exception:
        pass

    return u


def _extract_canonical_url_from_html(html: str, base_url: str) -> str:
    """
    HTML에서 canonical/og:url 추출.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")

        link = soup.find("link", attrs={"rel": re.compile(r"\bcanonical\b", re.I)})
        if link and link.get("href"):
            href = (link["href"] or "").strip()
            if href.startswith("/"):
                href = urljoin(base_url, href)
            if _is_http_url(href):
                return href

        meta = soup.find("meta", attrs={"property": "og:url"})
        if meta and meta.get("content"):
            href = (meta["content"] or "").strip()
            if href.startswith("/"):
                href = urljoin(base_url, href)
            if _is_http_url(href):
                return href
    except Exception:
        pass

    return ""


def _looks_like_article_url(url: str) -> bool:
    """
    URL이 '기사'로 보이는지 휴리스틱 검사.
    - path가 너무 짧거나 섹션/목록 힌트가 있으면 False
    - 날짜/긴 숫자(id) 패턴이 있으면 True 가산
    """
    try:
        pu = urlparse(url)
        path = (pu.path or "").lower()
        if not path or path in ("/", ""):
            return False

        for hint in _NON_ARTICLE_PATH_HINTS:
            if hint in path:
                return False

        # 너무 짧은 path는 섹션일 가능성
        if len(path.strip("/").split("/")) <= 1 and len(path) < 18:
            return False

        # 기사 ID/날짜 류가 있으면 기사일 확률 증가
        if re.search(r"\b(20\d{2}[./-]\d{1,2}[./-]\d{1,2})\b", path):
            return True
        if re.search(r"\b\d{6,}\b", path):
            return True

        # 위에서 섹션류는 걸렀으므로 기본 True
        return True
    except Exception:
        return False


def _finalize_article_url(url: str) -> tuple[str, Optional[str]]:
    """
    URL을 '정식 기사 URL'로 확정:
    1) redirect param 언랩
    2) GET으로 리다이렉트 따라 final url 확보
    3) HTML에서 canonical/og:url로 최종 확정
    반환: (final_url, html_or_none)
    """
    u0 = _unwrap_redirect_url(url)
    if not _is_http_url(u0):
        return u0, None

    try:
        with requests.Session() as s:
            r = s.get(
                u0,
                headers=DEFAULT_HEADERS,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            final_url = str(r.url or u0)
            final_url = _strip_fragment(final_url)

            ct = (r.headers.get("Content-Type") or "").lower()
            html = r.text if (r.status_code == 200 and "text/html" in ct) else None

            if html:
                canon = _extract_canonical_url_from_html(html, base_url=final_url)
                canon = _strip_fragment(canon)
                if canon and _is_http_url(canon):
                    return canon, html

            return final_url, html
    except Exception:
        return u0, None


def _canonicalize_article_url(url: str) -> str:
    final_url, _html = _finalize_article_url(url)
    return final_url


# =========================================================
# Time parsing / recency
# =========================================================
def _parse_datetime_any(s: str) -> Optional[datetime]:
    t = (s or "").strip()
    if not t:
        return None

    m = re.search(r"\b(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})\b", t)
    if m:
        try:
            return datetime.fromisoformat(f"{m.group(1)} {m.group(2)}").replace(tzinfo=KST)
        except Exception:
            pass

    iso = t.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except Exception:
        pass

    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", t)
    if m:
        try:
            return datetime.fromisoformat(m.group(1)).replace(tzinfo=KST, hour=12, minute=0)
        except Exception:
            return None

    return None


def _format_kst_min(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M")


def _is_recent_kst(dt: datetime, now_kst: datetime) -> bool:
    d = (now_kst.date() - dt.astimezone(KST).date()).days
    return 0 <= d <= MAX_AGE_DAYS


# =========================================================
# Fetch HTML + parse published time / content / og image
# =========================================================
_PUB_META_KEYS = (
    ("meta", {"property": "article:published_time"}),
    ("meta", {"property": "og:published_time"}),
    ("meta", {"property": "article:modified_time"}),
    ("meta", {"property": "og:updated_time"}),
    ("meta", {"name": "pubdate"}),
    ("meta", {"name": "publishdate"}),
    ("meta", {"name": "timestamp"}),
    ("meta", {"name": "date"}),
)


def _fetch_html(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        ct = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ct:
            return None
        return r.text
    except Exception:
        return None


def _extract_published_at_from_html(html: str) -> Optional[datetime]:
    soup = BeautifulSoup(html, "html.parser")

    for tag_name, attrs in _PUB_META_KEYS:
        tag = soup.find(tag_name, attrs=attrs)
        if tag and tag.get("content"):
            dt = _parse_datetime_any(tag.get("content", ""))
            if dt:
                return dt

    for t in soup.find_all("time")[:5]:
        dt_attr = t.get("datetime") or ""
        dt = _parse_datetime_any(dt_attr)
        if dt:
            return dt
        dt = _parse_datetime_any(t.get_text(" ").strip())
        if dt:
            return dt

    return None


def _extract_og_image_from_html(html: str, base_url: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    candidates: List[str] = []

    tag = soup.find("meta", attrs={"property": "og:image"})
    if tag and tag.get("content"):
        candidates.append(tag["content"])

    tag = soup.find("meta", attrs={"name": "twitter:image"})
    if tag and tag.get("content"):
        candidates.append(tag["content"])

    tag = soup.find("meta", attrs={"name": "twitter:image:src"})
    if tag and tag.get("content"):
        candidates.append(tag["content"])

    for img in candidates:
        img = (img or "").strip()
        if not img:
            continue
        if img.startswith("/"):
            img = urljoin(base_url, img)
        return img
    return ""


def _clean_text(s: str) -> str:
    t = re.sub(r"\s+", " ", (s or "").strip())
    return t


def _extract_article_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "aside", "nav"]):
        tag.decompose()

    for sel in ["article", "main"]:
        node = soup.select_one(sel)
        if node:
            ps = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = _clean_text(" ".join(ps))
            if len(text) >= 200:
                return text

    candidates_sel = [
        "#articleBody",
        "#article_body",
        "#newsct_article",
        "#content",
        "#contents",
        ".article-body",
        ".articleBody",
        ".news_body",
        ".newsBody",
        ".story-body",
        ".entry-content",
        ".post-content",
        ".post_body",
    ]
    for sel in candidates_sel:
        node = soup.select_one(sel)
        if node:
            ps = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = _clean_text(" ".join(ps)) or _clean_text(node.get_text(" ", strip=True))
            if len(text) >= 200:
                return text

    ps = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = _clean_text(" ".join(ps))
    return text


def _resolve_published_at_kst_min(article_url: str, candidate: str) -> Optional[str]:
    dt = _parse_datetime_any(candidate)
    if dt:
        return _format_kst_min(dt)

    html = _fetch_html(article_url)
    if html:
        dt2 = _extract_published_at_from_html(html)
        if dt2:
            return _format_kst_min(dt2)

    return None


# =========================================================
# Image resolution
# =========================================================
def _is_valid_image_url(url: str, timeout: float = 4.0) -> bool:
    url = (url or "").strip()
    if not _is_http_url(url):
        return False

    try:
        r = requests.head(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
        ct = (r.headers.get("Content-Type") or "").lower()
        if r.status_code == 200 and ct.startswith("image/"):
            return True
    except Exception:
        pass

    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True, stream=True)
        ct = (r.headers.get("Content-Type") or "").lower()
        return r.status_code == 200 and ct.startswith("image/")
    except Exception:
        return False


def _fallback_favicon(article_url: str) -> str:
    try:
        host = urlparse(article_url).netloc
        if not host:
            return ""
        return f"https://www.google.com/s2/favicons?domain={host}&sz=128"
    except Exception:
        return ""


def _resolve_image_url(article_link: str, candidate_image_url: str, html: Optional[str]) -> tuple[str, bool]:
    candidate = (candidate_image_url or "").strip()
    if candidate and _is_valid_image_url(candidate):
        return candidate, False

    if html:
        og = _extract_og_image_from_html(html, base_url=article_link)
        if og and _is_valid_image_url(og):
            return og, False

    fav = _fallback_favicon(article_link)
    if fav:
        return fav, False

    return "", True


# =========================================================
# Normalize / rank
# =========================================================
@dataclass
class NewsNorm:
    title: str
    summary: str
    link: str
    image_url: str
    published_dt: datetime
    published_at: str  # YYYY-MM-DD HH:MM
    needs_image_gen: bool
    content: str
    normalized_title: str

    @property
    def has_image(self) -> bool:
        return bool(self.image_url) and not self.image_url.startswith("https://www.google.com/s2/favicons")


def _normalize_news_item(n: dict, now_kst: datetime) -> Optional[NewsNorm]:
    link_raw = (n.get("link") or "").strip()
    if not link_raw:
        return None

    # ✅ 1) 정식 URL 확정 + HTML 1회 확보
    link, html = _finalize_article_url(link_raw)
    link = (link or "").strip()
    if not link:
        return None

    # ✅ 2) 블락/중계 도메인 제거
    if _is_blocked_url(link):
        return None

    # ✅ 3) 기사 URL 형태 휴리스틱(섹션/목록/메인 제거)
    if not _looks_like_article_url(link):
        return None

    title = _sanitize_text(n.get("title"), 300).strip()
    summary = _sanitize_text(n.get("summary"), 1000).strip()

    # ✅ 4) 발행시각 확정(후보 -> HTML meta/time fallback)
    pub_str = _resolve_published_at_kst_min(link, _sanitize_text(n.get("published_at"), 100))
    if not pub_str:
        return None

    dt = _parse_datetime_any(pub_str)
    if not dt:
        return None

    if not _is_recent_kst(dt, now_kst):
        return None

    # ✅ 5) 콘텐츠 확보 (이미 html을 받았으면 재사용)
    if not html:
        html = _fetch_html(link)

    img_url, needs_gen = _resolve_image_url(link, str(n.get("image_url") or ""), html=html)

    content = ""
    if html:
        content = _extract_article_text_from_html(html)
        content = content[:CONTENT_MAX_CHARS]

    # ✅ 6) 기사성 최종 검증: 본문이 너무 짧으면 목록/메인일 확률 높음
    if len((content or "").strip()) < MIN_ARTICLE_TEXT_CHARS:
        return None

    nt = _normalize_title(title)

    return NewsNorm(
        title=title,
        summary=summary,
        link=link[:1000],
        image_url=(img_url or "")[:1000],
        published_dt=dt.astimezone(KST),
        published_at=_format_kst_min(dt),
        needs_image_gen=needs_gen,
        content=content or "",
        normalized_title=nt,
    )


def _collect_candidates(
    now_kst: datetime,
    raw_news_batches: Iterable[List[dict]],
    used_urls: set[str],
    used_titles: set[str],
    pool_limit: int,
) -> List[NewsNorm]:
    """
    후보 수집 시점에서 1차 중복 제거:
    - canonical URL 중복 제거
    - normalized title 중복 제거
    """
    out: List[NewsNorm] = []
    for batch in raw_news_batches:
        for n in batch:
            norm = _normalize_news_item(n, now_kst)
            if not norm:
                continue

            if norm.link in used_urls:
                continue
            if norm.normalized_title and norm.normalized_title in used_titles:
                continue

            used_urls.add(norm.link)
            if norm.normalized_title:
                used_titles.add(norm.normalized_title)

            out.append(norm)
            if len(out) >= pool_limit:
                return out
    return out


def _rank_and_pick(
    cands: List[NewsNorm],
    limit: int,
    global_seen_urls: set[str],
    global_seen_titles: set[str],
) -> List[NewsNorm]:
    """
    1) 최신순(published_dt desc)
    2) 이미지 있는 기사 우선
    3) scope 전역 중복 제거(같은 기사가 다른 키워드에 또 나오지 않게)
    """
    if not cands:
        return []

    cands_sorted = sorted(cands, key=lambda x: x.published_dt, reverse=True)

    def take_unique(src: List[NewsNorm], need: int) -> List[NewsNorm]:
        picked: List[NewsNorm] = []
        for x in src:
            if x.link in global_seen_urls:
                continue
            if x.normalized_title and x.normalized_title in global_seen_titles:
                continue

            picked.append(x)
            global_seen_urls.add(x.link)
            if x.normalized_title:
                global_seen_titles.add(x.normalized_title)

            if len(picked) >= need:
                break
        return picked

    with_img = [x for x in cands_sorted if x.has_image]
    without_img = [x for x in cands_sorted if not x.has_image]

    picked = take_unique(with_img, limit)
    if len(picked) < limit:
        picked.extend(take_unique(without_img, limit - len(picked)))

    return picked[:limit]


def _final_dedupe_for_save(picked: List[NewsNorm]) -> List[NewsNorm]:
    """
    저장 직전 최종 방어 중복 제거(키워드 내부에서 혹시 남아있을 수 있는 중복 제거).
    """
    out: List[NewsNorm] = []
    seen_u: set[str] = set()
    seen_t: set[str] = set()
    for x in picked:
        if x.link in seen_u:
            continue
        nt = x.normalized_title
        if nt and nt in seen_t:
            continue
        seen_u.add(x.link)
        if nt:
            seen_t.add(nt)
        out.append(x)
    return out


# =========================================================
# LLM calls
# =========================================================
def _llm_chat(client, msgs: List[ChatMessage]) -> str:
    try:
        return client.chat(msgs, use_search=True)
    except TypeError:
        return client.chat(msgs)


def _refill_news_for_keyword(
    client,
    scope: str,
    keyword: str,
    now_kst: datetime,
    exclude_urls: set[str],
    batch_size: int,
) -> List[dict]:
    msg = _build_keyword_refill_msg(
        scope=scope,
        keyword=keyword,
        now_kst=now_kst,
        exclude_urls=exclude_urls,
        batch_size=batch_size,
    )
    msgs = [
        ChatMessage(role="system", content="너는 JSON만 출력한다. 다른 텍스트 금지."),
        ChatMessage(role="user", content=msg),
    ]
    raw = _llm_chat(client, msgs)
    data = _safe_json_load(raw)
    news = data.get("news") or []
    return news if isinstance(news, list) else []


# =========================================================
# DB save
# =========================================================
def _save_to_db(today_date, scope: str, items: list[dict]) -> int:
    with transaction.atomic():
        TrendKeywordDaily.objects.filter(date=today_date, scope=scope).delete()

        for rank, it in enumerate(items, start=1):
            kw_obj = TrendKeywordDaily.objects.create(
                date=today_date,
                scope=scope,
                rank=rank,
                keyword=it["keyword"],
                reason=it["reason"],
            )

            news_objs: List[TrendKeywordNews] = []
            picked: List[NewsNorm] = it.get("picked_news", [])[:NEWS_LIMIT]
            picked = _final_dedupe_for_save(picked)

            for n in picked:
                news_objs.append(
                    TrendKeywordNews(
                        trend=kw_obj,
                        title=n.title,
                        summary=n.summary,
                        content=(n.content or "")[:CONTENT_MAX_CHARS],
                        link=n.link,
                        image_url=n.image_url,
                        published_at=n.published_at,  # YYYY-MM-DD HH:MM
                        needs_image_gen=n.needs_image_gen,
                    )
                )

            if news_objs:
                TrendKeywordNews.objects.bulk_create(news_objs)

    return len(items)


# =========================================================
# Management Command
# =========================================================
class Command(BaseCommand):
    help = (
        "Generate trend keywords (KR/US 3 each) and news per keyword: "
        "collect up to 100 candidates, de-dup by url/title, pick newest with images, "
        "and de-dup across keywords per scope; save up to 15 incl. content."
    )

    def handle(self, *args, **opts):
        scopes = [TrendScope.KR, TrendScope.US]
        now_kst = _now_kst()
        today = now_kst.date()

        client = get_gemini_client()

        for scope in scopes:
            self.stdout.write(f"Requesting {scope} trends with Google Search...")

            # ✅ scope 전역 중복 제거 세트(키워드 간 중복 방지)
            global_seen_urls: set[str] = set()
            global_seen_titles: set[str] = set()

            user_msg = _build_user_msg(scope, now_kst=now_kst)
            msgs = [
                ChatMessage(role="system", content=TREND_JSON_INSTRUCTION),
                ChatMessage(role="user", content=user_msg),
            ]

            raw = _llm_chat(client, msgs)
            data = _safe_json_load(raw)
            items_raw = data.get("items") or []
            if not isinstance(items_raw, list):
                items_raw = []

            items: List[dict] = []
            for x in items_raw[:KEYWORD_LIMIT]:
                if not isinstance(x, dict):
                    continue
                items.append(
                    {
                        "keyword": _sanitize_keyword(x.get("keyword")),
                        "reason": _sanitize_text(x.get("reason"), 2000),
                        "news_seed": x.get("news") if isinstance(x.get("news"), list) else [],
                    }
                )

            while len(items) < KEYWORD_LIMIT:
                items.append({"keyword": "N/A", "reason": "데이터 없음", "news_seed": []})

            # 키워드별 후보 100개 수집 -> 최신 + 이미지 우선 15개 저장(+본문)
            for it in items:
                kw = it["keyword"]

                # ✅ 키워드 후보 수집 단계의 중복 제거(키워드 내부)
                used_urls: set[str] = set()
                used_titles: set[str] = set()

                raw_batches: List[List[dict]] = [it.get("news_seed") or []]

                candidates = _collect_candidates(
                    now_kst=now_kst,
                    raw_news_batches=raw_batches,
                    used_urls=used_urls,
                    used_titles=used_titles,
                    pool_limit=CANDIDATE_POOL_LIMIT,
                )

                attempts = 0
                while len(candidates) < CANDIDATE_POOL_LIMIT and attempts < MAX_REFILL_ATTEMPTS:
                    attempts += 1
                    refill = _refill_news_for_keyword(
                        client=client,
                        scope=scope,
                        keyword=kw,
                        now_kst=now_kst,
                        exclude_urls=used_urls,  # URL 위주로만 exclude 전달 (LLM용)
                        batch_size=BATCH_SIZE,
                    )
                    if not refill:
                        continue

                    new_cands = _collect_candidates(
                        now_kst=now_kst,
                        raw_news_batches=[refill],
                        used_urls=used_urls,
                        used_titles=used_titles,
                        pool_limit=(CANDIDATE_POOL_LIMIT - len(candidates)),
                    )
                    candidates.extend(new_cands)

                    if attempts >= 3 and len(new_cands) == 0:
                        break

                # ✅ pick 단계에서 "scope 전역 중복"까지 제거
                picked = _rank_and_pick(
                    cands=candidates,
                    limit=NEWS_LIMIT,
                    global_seen_urls=global_seen_urls,
                    global_seen_titles=global_seen_titles,
                )

                # ✅ 저장 직전 방어 중복 제거
                picked = _final_dedupe_for_save(picked)

                it["picked_news"] = picked

                self.stdout.write(
                    f"  - {scope} keyword='{kw}' candidates={len(candidates)}/{CANDIDATE_POOL_LIMIT} picked={len(picked)}/{NEWS_LIMIT}"
                )

            saved = _save_to_db(today, scope, items)
            self.stdout.write(
                self.style.SUCCESS(
                    f"[{today}] scope={scope} saved={saved} keywords with up to {NEWS_LIMIT} news each (content included, de-duplicated)."
                )
            )
