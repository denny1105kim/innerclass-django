from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

import openai
from django.conf import settings
from django.db import transaction

from markets.models import DailyRankingSnapshot, MarketChoices, RankingTypeChoices
from news.models import NewsArticle, NewsArticleAnalysis, NewsTheme


THEME_CHOICES = [
    "SEMICONDUCTOR_AI",
    "BATTERY",
    "ENERGY",
    "FINANCE_HOLDING",
    "ICT_PLATFORM",
    "BIO_HEALTH",
    "AUTO",
    "ETC",
]

# ✅ 관련 종목 판단 임계치(이 점수 이상이면 ticker/sector/name 저장)
RELATED_STOCK_MIN_CONFIDENCE = int(getattr(settings, "NEWS_RELATED_STOCK_MIN_CONFIDENCE", 70))

# ✅ LLM에 넘길 종목 후보 최대 개수(너무 크면 토큰 낭비)
MAX_CANDIDATES_FOR_LLM = int(getattr(settings, "NEWS_RELATED_STOCK_MAX_CANDIDATES", 40))

# ✅ DB에서 후보를 가져올 때 시장별로 상위 N (시총 상위)
TOPN_PER_MARKET = int(getattr(settings, "NEWS_RELATED_STOCK_TOPN_PER_MARKET", 120))


def _strip_code_fences(text: str) -> str:
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    parts = t.split("```")
    if len(parts) < 3:
        return t
    inner = parts[1].strip()
    if inner.lower().startswith("json"):
        inner = inner[4:].strip()
    return inner.strip()


def _safe_theme(v: str) -> str:
    vv = (v or "").strip().upper()
    allowed = {x for x, _ in NewsTheme.choices}
    return vv if vv in allowed else NewsTheme.ETC


# ✅ 레벨 라벨 prefix 제거
_LEVEL_PREFIX_RE = re.compile(
    r"^\s*(?:주린이용|초보자용|중급자용|숙련자용|전문가용)\s*[:：\-]\s*",
    flags=re.IGNORECASE,
)


def _strip_level_prefix(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    return _LEVEL_PREFIX_RE.sub("", s).strip()


def _clean_level_content_prefixes(level_content: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(level_content, dict):
        return level_content

    for lv_key in ("lv1", "lv2", "lv3", "lv4", "lv5"):
        block = level_content.get(lv_key)
        if not isinstance(block, dict):
            continue

        if isinstance(block.get("summary"), str):
            block["summary"] = _strip_level_prefix(block["summary"])

        for list_field in ("bullet_points", "what_is_this", "why_important"):
            v = block.get(list_field)
            if isinstance(v, list):
                new_list = []
                changed = False
                for item in v:
                    if isinstance(item, str):
                        cleaned = _strip_level_prefix(item)
                        if cleaned != item:
                            changed = True
                        new_list.append(cleaned)
                    else:
                        new_list.append(item)
                if changed:
                    block[list_field] = new_list

        if isinstance(block.get("action_guide"), str):
            block["action_guide"] = _strip_level_prefix(block["action_guide"])

        sg = block.get("strategy_guide")
        if isinstance(sg, dict):
            if isinstance(sg.get("short_term"), str):
                sg["short_term"] = _strip_level_prefix(sg["short_term"])
            if isinstance(sg.get("long_term"), str):
                sg["long_term"] = _strip_level_prefix(sg["long_term"])

        level_content[lv_key] = block

    return level_content


def _build_level_payload(full: Dict[str, Any], level_key: str) -> Dict[str, Any]:
    common = {
        "deep_analysis_reasoning": full.get("deep_analysis_reasoning", ""),
        "theme": full.get("theme", NewsTheme.ETC),
        "keywords": full.get("keywords", []),
        "sentiment_score": full.get("sentiment_score", None),
        "vocabulary": full.get("vocabulary", []),
        # ✅ 종목 판단 결과를 분석 JSON에도 포함
        "related_stock": full.get("related_stock", None),
    }
    level_content = (full.get("level_content") or {}).get(level_key) or {}
    if not isinstance(level_content, dict):
        level_content = {}
    merged = dict(common)
    merged.update(level_content)
    return merged


# =========================================================
# Title translation (한글 없으면 "요약형 헤드라인"으로 번역 후 title 저장)
# =========================================================
def _has_hangul(text: str) -> bool:
    text = text or ""
    for ch in text:
        code = ord(ch)
        if 0xAC00 <= code <= 0xD7A3:
            return True
        if 0x1100 <= code <= 0x11FF:
            return True
        if 0x3130 <= code <= 0x318F:
            return True
    return False


# ✅ "~다/~했다" 종결 제거용(마지막에 붙는 경우만 최소 제거)
_TRAILING_DECLARATIVE_RE = re.compile(r"(?:\s*)(했다|하였다|한다|됐다|되었다|된다|밝혔다|말했다|전했다|예상했다|추정했다)\s*$")


def _postprocess_ko_headline(title_ko: str) -> str:
    """
    헤드라인 스타일로 보이도록 후처리:
    - 끝의 '~다/~했다' 류 종결이 붙으면 제거(강제는 아님, 최소 적용)
    - 불필요한 따옴표/공백 정리
    """
    t = (title_ko or "").strip()

    # 쌍따옴표로 둘러싸인 경우 제거
    if len(t) >= 2 and ((t[0] == '"' and t[-1] == '"') or (t[0] == "“" and t[-1] == "”")):
        t = t[1:-1].strip()

    # 문장형 종결 제거(있을 때만)
    t2 = _TRAILING_DECLARATIVE_RE.sub("", t).strip()
    if t2:
        t = t2

    # 맨 끝 마침표 제거(있을 때만)
    if t.endswith("."):
        t = t[:-1].strip()

    # 중복 공백 정리
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _translate_title_to_ko(title: str) -> Optional[str]:
    """
    목표: '뉴스 제목 번역 적용' 같은 "요약형 헤드라인" 톤
    - 문장 종결(~다/~했다) 지양
    - 헤드라인 관용(“…”, ‘…’) 최소화
    - 고유명사/티커/숫자 보존
    """
    title = (title or "").strip()
    if not title:
        return None

    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

    prompt = (
        "너는 경제/금융 뉴스 데스크의 헤드라인 에디터다.\n"
        "아래 '영문/비한글 제목'을 한국어 헤드라인으로 번역하라.\n\n"
        "[핵심 톤]\n"
        "- 문장형 종결(예: ~다, ~했다) 금지. '요약형 헤드라인'으로 작성.\n"
        "- 불필요한 따옴표/수식어 최소화, 정보 밀도 높게.\n\n"
        "[보존 규칙]\n"
        "- 고유명사(기업/인물/제품), 티커, 숫자, 단위는 가능한 원문을 유지.\n"
        "- 의미를 바꾸는 의역/요약 금지. 제목의 의미를 그대로 옮기되 헤드라인 문체로만 변환.\n\n"
        "[출력]\n"
        "- 반드시 JSON만 출력\n"
        '- 형식: {"ko_title": "..."}\n\n'
        f'원문 제목: "{title}"'
    )

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a Korean financial headline editor. Output JSON only."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=200,
    )

    text = _strip_code_fences((resp.choices[0].message.content or "").strip())
    try:
        data = json.loads(text)
        ko = (data.get("ko_title") or "").strip()
        ko = _postprocess_ko_headline(ko)
        return ko or None
    except Exception:
        return None


def _maybe_translate_and_save_title(article: NewsArticle) -> bool:
    title = (article.title or "").strip()
    if not title:
        return False
    if _has_hangul(title):
        return False

    ko = _translate_title_to_ko(title)
    if not ko or ko == title:
        return False

    article.title = ko
    article.save(update_fields=["title"])
    return True


# =========================================================
# Related stock detection (DailyRankingSnapshot 기반)
# =========================================================
def _normalize_for_match(s: str) -> str:
    s = (s or "").lower()
    # 공백/특수문자 정리(너무 공격적으로 하면 한글 매칭 깨질 수 있어 최소화)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _resolve_latest_asof_date_for_market(market: str) -> Optional[Any]:
    return (
        DailyRankingSnapshot.objects.filter(market=market)
        .order_by("-asof_date")
        .values_list("asof_date", flat=True)
        .first()
    )


def _fetch_top_ranked_stocks(market: str, topn: int) -> List[DailyRankingSnapshot]:
    """
    시장별 최신 asof_date 기준, 시총(MARKET_CAP) 랭킹 상위 topn을 가져옴.
    """
    asof = _resolve_latest_asof_date_for_market(market)
    if not asof:
        return []

    return list(
        DailyRankingSnapshot.objects.filter(
            market=market,
            asof_date=asof,
            ranking_type=RankingTypeChoices.MARKET_CAP,
        )
        .order_by("rank")[: max(1, topn)]
    )


def _build_candidate_universe_for_article(article: NewsArticle) -> List[Dict[str, str]]:
    """
    기사 market에 따라 후보 universe를 구성:
    - Korea -> KOSPI + KOSDAQ
    - International -> NASDAQ
    """
    if article.market == "Korea":
        markets = [MarketChoices.KOSPI, MarketChoices.KOSDAQ]
    else:
        markets = [MarketChoices.NASDAQ]

    rows: List[DailyRankingSnapshot] = []
    for m in markets:
        rows.extend(_fetch_top_ranked_stocks(m, TOPN_PER_MARKET))

    # dedup by symbol_code (가장 먼저 나온 row 유지)
    seen = set()
    out: List[Dict[str, str]] = []
    for r in rows:
        if r.symbol_code in seen:
            continue
        seen.add(r.symbol_code)
        out.append(
            {
                "ticker": r.symbol_code,
                "name": r.name,
                "sector": r.market,  # ✅ sector에 KOSPI/KOSDAQ/NASDAQ 저장
            }
        )
    return out


def _shortlist_candidates_by_text(article: NewsArticle, universe: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    1차로 문자열 포함 매칭으로 후보를 줄임(토큰 절약).
    - 기사 텍스트(제목+요약+본문 일부)에 name이 포함되면 우선 선발
    - 부족하면 앞쪽(시총상위)에서 채움
    """
    text = " ".join(
        [
            (article.title or ""),
            (article.summary or ""),
            (article.content or "")[: 2000],
        ]
    )
    text_n = _normalize_for_match(text)

    hits: List[Dict[str, str]] = []
    for c in universe:
        nm = _normalize_for_match(c.get("name", ""))
        tk = _normalize_for_match(c.get("ticker", ""))

        # 이름이 본문/제목에 들어가면 강력 후보
        if nm and nm in text_n:
            hits.append(c)
            continue

        # 티커가 그대로 기사에 들어오는 경우(예: AAPL, 005930 등)
        if tk and tk in text_n:
            hits.append(c)
            continue

    # dedup + limit
    seen = set()
    uniq_hits: List[Dict[str, str]] = []
    for c in hits:
        k = c["ticker"]
        if k in seen:
            continue
        seen.add(k)
        uniq_hits.append(c)

    if len(uniq_hits) >= MAX_CANDIDATES_FOR_LLM:
        return uniq_hits[:MAX_CANDIDATES_FOR_LLM]

    # 부족하면 universe에서 채움(시총상위 우선)
    for c in universe:
        if c["ticker"] in seen:
            continue
        uniq_hits.append(c)
        seen.add(c["ticker"])
        if len(uniq_hits) >= MAX_CANDIDATES_FOR_LLM:
            break

    return uniq_hits


def _detect_related_stock_with_llm(
    article: NewsArticle,
    candidates: List[Dict[str, str]],
) -> Dict[str, Any]:
    """
    LLM으로 "이 뉴스가 후보 종목들 중 어느 종목과 가장 관련있는지" 판단.
    - 없으면 null
    - 있으면 ticker/sector/name/confidence 반환
    """
    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

    cand_json = json.dumps(candidates, ensure_ascii=False)

    content_to_analyze = (article.content or "").strip() or (article.summary or "").strip()
    content_to_analyze = content_to_analyze[:6000]

    prompt = f"""아래 뉴스가 "아래 후보 종목들" 중 특정 종목과 실질적으로 관련(기업 자체/실적/사업/주가 촉매/규제/계약/공급망/경쟁 등) 있는지 판단해.
관련이 있으면 가장 관련성이 높은 종목 1개를 고르고, 없으면 null로 답해.

[뉴스]
제목: {article.title}
요약: {article.summary}
본문: {content_to_analyze}

[후보 종목들(JSON)]
{cand_json}

[출력 규칙]
- 반드시 JSON만 출력
- 관련이 없으면:
  {{"related": false, "ticker": null, "sector": null, "confidence": 0, "reason": "..." }}
- 관련이 있으면:
  {{"related": true, "ticker": "<후보 ticker>", "sector": "<후보 sector>", "confidence": 0~100 정수, "reason": "근거(짧게)" }}
- 후보에 없는 ticker/sector를 만들어내면 안 된다.
"""

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a strict financial entity linker. Output JSON only."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=350,
    )

    text = _strip_code_fences((resp.choices[0].message.content or "").strip())
    try:
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("not a dict")

        related = bool(data.get("related"))
        ticker = data.get("ticker")
        sector = data.get("sector")
        conf = data.get("confidence", 0)
        try:
            conf = int(conf)
        except Exception:
            conf = 0
        conf = max(0, min(100, conf))
        reason = str(data.get("reason") or "").strip()

        name: Optional[str] = None

        if related:
            allowed = {(c["ticker"], c["sector"]) for c in candidates}
            if (ticker, sector) not in allowed:
                return {
                    "related": False,
                    "ticker": None,
                    "sector": None,
                    "name": None,
                    "confidence": 0,
                    "reason": "invalid-candidate",
                }

            # ✅ ticker/sector로 candidates에서 name 역조회해서 반환
            for c in candidates:
                if c.get("ticker") == ticker and c.get("sector") == sector:
                    name = c.get("name")
                    break

        return {
            "related": related,
            "ticker": ticker,
            "sector": sector,
            "name": name,
            "confidence": conf,
            "reason": reason,
        }

    except Exception:
        return {
            "related": False,
            "ticker": None,
            "sector": None,
            "name": None,
            "confidence": 0,
            "reason": "parse-fail",
        }


def _maybe_set_ticker_sector(article: NewsArticle) -> Dict[str, Any]:
    """
    DailyRankingSnapshot 기반 후보를 만들고 LLM으로 연결한 뒤,
    임계치 이상이면 article.ticker/sector/name에 저장.
    """
    universe = _build_candidate_universe_for_article(article)
    if not universe:
        return {"related": False, "ticker": None, "sector": None, "name": None, "confidence": 0, "reason": "no-universe"}

    shortlist = _shortlist_candidates_by_text(article, universe)
    res = _detect_related_stock_with_llm(article, shortlist)

    if res.get("related") and int(res.get("confidence") or 0) >= RELATED_STOCK_MIN_CONFIDENCE:
        ticker = res.get("ticker")
        sector = res.get("sector")
        name = res.get("name")
        if ticker and sector:
            article.ticker = str(ticker)
            article.sector = str(sector)  # ✅ KOSPI/KOSDAQ/NASDAQ 저장
            article.name = (str(name).strip() if name else None)  # ✅ 종목명 저장
            article.save(update_fields=["ticker", "sector", "name"])
    return res


# =========================================================
# Main
# =========================================================
def analyze_news(article: NewsArticle, save_to_db: bool = True) -> Optional[Dict[str, Any]]:
    """
    save_to_db=True일 경우:
      - title이 한글이 아니면 "요약형 헤드라인"으로 번역 후 article.title 저장
      - 관련 종목(ticker/sector/name)을 DailyRankingSnapshot 기반으로 판단 후 저장(신뢰도 임계치 이상)
      - theme 저장
      - NewsArticleAnalysis Lv1~Lv5 upsert
    """
    related_res: Optional[Dict[str, Any]] = None

    if save_to_db:
        # 1) 제목 번역(헤드라인 톤)
        try:
            _maybe_translate_and_save_title(article)
        except Exception as e:
            print(f"WARN: title translation failed (id={getattr(article, 'id', None)}): {e}")

        # 2) 종목 연결(실패해도 전체 분석은 진행)
        try:
            related_res = _maybe_set_ticker_sector(article)
        except Exception as e:
            print(f"WARN: related-stock detection failed (id={getattr(article, 'id', None)}): {e}")
            related_res = None

    content_to_analyze = (article.content or "").strip() or (article.summary or "").strip()
    if not content_to_analyze:
        return None

    try:
        client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

        prompt = f"""다음 뉴스 기사를 심층 분석하여 아래 형식의 JSON으로 응답해줘.
다른 말은 덧붙이지 말고 반드시 JSON 데이터만 출력해.

[기사 정보]
제목: {article.title}
내용: {content_to_analyze}

[추가 요청 - Theme 분류]
아래 theme 중 이 뉴스가 어디에 속하는지 하나만 선택해서 "theme" 필드에 넣어줘:
{THEME_CHOICES}
- 반도체/AI/칩/파운드리/HBM/GPU/데이터센터/LLM 인프라 중심이면 SEMICONDUCTOR_AI
- 배터리/리튬/양극재/전해질/2차전지 밸류체인이면 BATTERY
- 석유에너지/오일에너지/방사능/원자력발전소/재생에너지/탄소중립/수소/태양광/풍력/정책이면 ENERGY
- 은행/증권/보험/금융지주/금리/대출/예대마진이면 FINANCE_HOLDING
- 플랫폼/클라우드/SaaS/인터넷/ICT 서비스면 ICT_PLATFORM
- 바이오/제약/헬스케어/임상/FDA면 BIO_HEALTH
- 자동차/부품/자율주행/모빌리티면 AUTO
- 그 외는 ETC

[응답 형식 (JSON)]
{{
    "theme": "ETC",

    "deep_analysis_reasoning": "여기에는 뉴스 분석을 위한 심층적인 사고 과정을 서술해. 먼저 팩트를 나열하고, 이것이 거시경제(금리, 환율)와 해당 산업 밸류체인에 미칠 영향을 논리적으로 추론해. 이 필드는 사용자에게 보여지지 않지만, 뒤이어 나올 전문가용(Lv5) 분석의 질을 높이기 위한 브레인스토밍 공간이야.",
    
    "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],
    "sentiment_score": 75,
    "vocabulary": [
        {{"term": "기사에_나온_어려운_용어", "definition": "해당 용어에 대한 초보자용 해설"}}
    ],

    "level_content": {{
        "lv1": {{
            "summary": "주린이용: 초등학생도 이해할 수 있는 아주 쉽고 친절한 말투, 투자 경고와 기본 개념 위주 설명 (전문 용어 절대 금지)",
            "bullet_points": ["아주 쉬운 핵심 요약 1", "아주 쉬운 핵심 요약 2", "아주 쉬운 핵심 요약 3"],
            "what_is_this": ["이 뉴스가 뭔지 쉽게 설명 1", "이 뉴스의 배경 설명 2"],
            "why_important": ["이게 왜 중요한지 생활 밀착형 설명 1", "이게 왜 중요한지 2"],
            "stock_impact": {{
                "positives": ["좋은 점 1", "좋은 점 2"],
                "warnings": ["조심할 점 1", "조심할 점 2"]
            }},
            "strategy_guide": {{
                "short_term": "주린이를 위한 단기 조언 (예: 지금은 관망하세요)",
                "long_term": "주린이를 위한 장기 조언 (예: 우량주 위주 적립식 매수)"
            }},
            "action_guide": "주린이를 위한 아주 기초적인 조언 (예: 섣불리 사지 마세요)"
        }},
        "lv2": {{
            "summary": "초보자용: 뉴스의 현상과 원인을 인과관계 중심으로 쉽게 풀어서 설명",
            "bullet_points": ["쉬운 요약 1", "쉬운 요약 2", "쉬운 요약 3"],
            "what_is_this": ["뉴스의 핵심 내용 설명 1", "배경 설명 2"],
            "why_important": ["시장에 중요한 이유 1", "시장에 중요한 이유 2"],
            "stock_impact": {{
                "positives": ["긍정적 요인 1", "긍정적 요인 2"],
                "warnings": ["부정적 요인 1", "부정적 요인 2"]
            }},
            "strategy_guide": {{
                "short_term": "초보자를 위한 단기 대응법",
                "long_term": "초보자를 위한 장기 투자 관점"
            }},
            "action_guide": "초보자를 위한 투자 조언"
        }},
        "lv3": {{
            "summary": "중급자용: 산업 트렌드와 기술적 용어를 포함하여 포트폴리오 관점에서 설명",
            "bullet_points": ["핵심 요약 1", "핵심 요약 2", "핵심 요약 3"],
            "what_is_this": ["심도 있는 뉴스 해석 1", "심도 있는 뉴스 해석 2"],
            "why_important": ["산업 및 시장 영향 분석 1", "시장 영향 분석 2"],
            "stock_impact": {{
                "positives": ["상승 재료 1", "상승 재료 2"],
                "warnings": ["하락 리스크 1", "하락 리스크 2"]
            }},
            "strategy_guide": {{
                "short_term": "기술적 분석을 포함한 단기 전략",
                "long_term": "산업 사이클을 고려한 장기 전략"
            }},
            "action_guide": "중급자를 위한 포트폴리오 조정 조언"
        }},
        "lv4": {{
            "summary": "숙련자용: 밸류에이션(PER/PBR), 정량적 지표, 과거 유사 사례와 비교하여 깊이 있는 인사이트 제공",
            "bullet_points": ["전문적 요약 1", "전문적 요약 2", "전문적 요약 3"],
            "what_is_this": ["구조적/재무적 관점의 분석 1", "구조적/재무적 관점의 분석 2"],
            "why_important": ["밸류체인 및 거시경제 영향 1", "영향 2"],
            "stock_impact": {{
                "positives": ["펀더멘털 개선 요인 1", "수급/모멘텀 요인 2"],
                "warnings": ["밸류에이션 부담 1", "리스크 요인 2"]
            }},
            "strategy_guide": {{
                "short_term": "트레이딩 관점의 매매 전략 (지지/저항 등)",
                "long_term": "밸류에이션 리레이팅 가능성 분석"
            }},
            "action_guide": "숙련자를 위한 매매/헤징 전략"
        }},
        "lv5": {{
            "summary": "전문가용: 펀드매니저 레벨. 매크로 환경 역학, 컨센서스 변화, 리스크 프리미엄 등 업계 전문 용어(Jargon)를 적극 사용하여 냉철하고 건조하게 분석.",
            "bullet_points": ["Insightful Summary 1", "Insightful Summary 2", "Insightful Summary 3"],
            "what_is_this": ["심층 분석 (Deep Dive) 1", "심층 분석 2"],
            "why_important": ["Global Macro & Sector Impact 1", "Impact 2"],
            "stock_impact": {{
                "positives": ["Upside Potential Logic 1", "Catalyst 2"],
                "warnings": ["Downside Risk 1", "Risk Factors 2"]
            }},
            "strategy_guide": {{
                "short_term": "Arbitrage / Event-Driven Strategy",
                "long_term": "Thematic / Structural Growth Thesis"
            }},
            "action_guide": "기관 투자자급의 High-Level 전략 (Long/Short, Arbitrage 등)"
        }}
  }}
}}

[작성 지침]
1. 'deep_analysis_reasoning'을 가장 먼저 작성하여 깊이 있는 분석을 선행할 것.
2. 각 레벨(lv1~lv5)별로 어조와 깊이를 명확히 차별화할 것.
3. Lv1은 아주 쉽게, Lv5는 매우 전문적으로 작성할 것.
4. sentiment_score는 0~100 정수.
"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "당신은 월스트리트의 수석 애널리스트이자, 동시에 친절한 금융 교육자입니다. JSON만 출력하세요.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
            max_tokens=3000,
        )

        result_text = _strip_code_fences((response.choices[0].message.content or "").strip())
        full = json.loads(result_text)

        theme = _safe_theme(str(full.get("theme", "")))
        full["theme"] = theme

        # ✅ related_stock 결과를 full에도 포함 (분석 JSON에서 사용 가능)
        if related_res:
            full["related_stock"] = related_res

        level_content = full.get("level_content")
        if isinstance(level_content, dict):
            full["level_content"] = _clean_level_content_prefixes(level_content)

        if save_to_db:
            with transaction.atomic():
                # ✅ theme 저장
                if article.theme != theme:
                    article.theme = theme
                    article.save(update_fields=["theme"])

                # ✅ Lv1~Lv5 저장
                level_map = {1: "lv1", 2: "lv2", 3: "lv3", 4: "lv4", 5: "lv5"}
                for level, key in level_map.items():
                    payload = _build_level_payload(full, key)
                    NewsArticleAnalysis.objects.update_or_create(
                        article=article,
                        level=level,
                        defaults={"theme": theme, "analysis": payload},
                    )

        return full

    except Exception as e:
        print(f"Error analyzing news: {e}")
        return None
