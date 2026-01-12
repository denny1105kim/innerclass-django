# apps/reco/services/analyze_trend_news.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from main.services.gemini_client import get_gemini_client, ChatMessage
from ..models import TrendKeywordNews, TrendKeywordNewsAnalysis


# =========================================================
# Config
# =========================================================
MODEL_NAME = getattr(settings, "GEMINI_TREND_ANALYSIS_MODEL", "")  # gemini_client 내부에서 모델 선택하면 비워도 됨
TEMPERATURE = float(getattr(settings, "GEMINI_TREND_ANALYSIS_TEMPERATURE", 0.7))
MAX_OUTPUT_TOKENS = int(getattr(settings, "GEMINI_TREND_ANALYSIS_MAX_TOKENS", 2500))

# Lv 분석 시, 너무 긴 본문은 비용/시간이 커져서 잘라서 보냄(원하면 늘리면 됨)
MAX_INPUT_CHARS = int(getattr(settings, "GEMINI_TREND_ANALYSIS_MAX_INPUT_CHARS", 6000))


# =========================================================
# JSON helpers
# =========================================================
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


def _safe_json_load(s: str) -> Optional[Dict[str, Any]]:
    s = (s or "").strip()
    if not s:
        return None

    s = _strip_code_fences(s)

    l = s.find("{")
    r = s.rfind("}")
    if l >= 0 and r >= 0 and r > l:
        s = s[l : r + 1]

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _build_level_payload(full: Dict[str, Any], level_key: str) -> Dict[str, Any]:
    """
    news/services/analyze_news.py 패턴과 동일하되 theme 제외.
    공통(meta) + level_content[level_key] merge
    """
    common = {
        "deep_analysis_reasoning": full.get("deep_analysis_reasoning", ""),
        "keywords": full.get("keywords", []),
        "sentiment_score": full.get("sentiment_score", None),
        "vocabulary": full.get("vocabulary", []),
    }
    level_content = (full.get("level_content") or {}).get(level_key) or {}
    if not isinstance(level_content, dict):
        level_content = {}

    merged = dict(common)
    merged.update(level_content)
    return merged


def _normalize_full(full: Dict[str, Any]) -> Dict[str, Any]:
    """
    최소 구조 보정 (프론트/DB 안정성)
    """
    if not isinstance(full.get("deep_analysis_reasoning"), str):
        full["deep_analysis_reasoning"] = ""

    if not isinstance(full.get("keywords"), list):
        full["keywords"] = []

    ss = full.get("sentiment_score", None)
    if ss is not None:
        try:
            ss_int = int(ss)
            ss_int = max(0, min(100, ss_int))
            full["sentiment_score"] = ss_int
        except Exception:
            full["sentiment_score"] = None

    if not isinstance(full.get("vocabulary"), list):
        full["vocabulary"] = []

    if not isinstance(full.get("level_content"), dict):
        full["level_content"] = {}

    # 누락된 레벨 키 보정
    for k in ["lv1", "lv2", "lv3", "lv4", "lv5"]:
        if k not in full["level_content"] or not isinstance(full["level_content"].get(k), dict):
            full["level_content"][k] = {}

    return full


# =========================================================
# Prompt (Gemini)
# =========================================================
def _build_prompt(title: str, content: str) -> str:
    t = (title or "").strip()
    c = (content or "").strip()
    c = c[:MAX_INPUT_CHARS]

    return f"""다음 뉴스 기사를 심층 분석하여 아래 형식의 JSON으로만 응답해줘.
다른 말은 덧붙이지 말고 반드시 JSON 데이터만 출력해. (마크다운/코드블록 금지)

[기사 정보]
제목: {t}
내용: {c}

[응답 형식 (JSON)]
{{
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
        "short_term": "주린이를 위한 단기 조언",
        "long_term": "주린이를 위한 장기 조언"
      }},
      "action_guide": "주린이를 위한 아주 기초적인 조언"
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
        "short_term": "트레이딩 관점의 매매 전략",
        "long_term": "밸류에이션 리레이팅 가능성 분석"
      }},
      "action_guide": "숙련자를 위한 매매/헤징 전략"
    }},
    "lv5": {{
      "summary": "전문가용: 펀드매니저 레벨. 매크로/컨센서스/리스크 프리미엄 등 업계 전문 용어(Jargon)를 적극 사용하여 냉철하고 건조하게 분석.",
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
      "action_guide": "기관 투자자급의 High-Level 전략"
    }}
  }}
}}

[작성 지침]
1) deep_analysis_reasoning을 가장 먼저 작성.
2) lv1~lv5는 관점/깊이를 확실히 다르게.
3) sentiment_score는 0~100 정수.
""".strip()


def _llm_chat(client, msgs: list[ChatMessage]) -> str:
    """
    gemini_client의 chat 시그니처가 (msgs, use_search=...) 형태도 있고, (msgs)만 되는 형태도 있어 방어.
    """
    try:
        return client.chat(msgs, use_search=False)
    except TypeError:
        return client.chat(msgs)


# =========================================================
# Public API
# =========================================================
def analyze_trend_keyword_news(
    *,
    news: TrendKeywordNews,
    save_to_db: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    TrendKeywordNews 1건을 Gemini로 분석하고:
      - TrendKeywordNews.analysis_full
      - TrendKeywordNews.analyzed_at
      - TrendKeywordNewsAnalysis (lv1~lv5) upsert
    """
    content_to_analyze = (news.content or "").strip() or (news.summary or "").strip()
    if not content_to_analyze:
        return None

    client = get_gemini_client()

    prompt = _build_prompt(news.title, content_to_analyze)
    msgs = [
        ChatMessage(role="system", content="너는 JSON만 출력한다. 다른 텍스트/마크다운 금지."),
        ChatMessage(role="user", content=prompt),
    ]

    raw = _llm_chat(client, msgs)
    full = _safe_json_load(raw)
    if not full:
        return None

    full = _normalize_full(full)

    if not save_to_db:
        return full

    with transaction.atomic():
        news.analysis_full = full
        news.analyzed_at = timezone.now()
        news.save(update_fields=["analysis_full", "analyzed_at"])

        level_map = {1: "lv1", 2: "lv2", 3: "lv3", 4: "lv4", 5: "lv5"}
        for level, key in level_map.items():
            payload = _build_level_payload(full, key)
            TrendKeywordNewsAnalysis.objects.update_or_create(
                news=news,
                level=level,
                defaults={"analysis": payload},
            )

    return full
