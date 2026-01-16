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

    # ✅ 변경 포인트:
    # - lv1~lv5 summary는 반드시 깔끔한 1줄
    # - 이모티콘/이모지/특수기호/불릿/번호/줄바꿈 금지 강제
    return f"""다음 뉴스 기사를 심층 분석하여 아래 형식의 JSON으로만 응답해 주세요.
다른 말은 덧붙이지 말고 반드시 JSON 데이터만 출력해 주세요. (마크다운/코드블록 금지)

[중요: 말투 지침]
- 모든 필드는 반드시 '존댓말'로 작성해 주세요.
  (예: "~입니다/~합니다/~하세요/~됩니다" 형태)
- deep_analysis_reasoning 포함, lv1~lv5의 summary/bullet_points/what_is_this/why_important/stock_impact/strategy_guide/action_guide 등
  JSON 내 모든 문자열에 적용해 주세요.
- 반말/구어체("~해", "~함", "~임", "~한다") 금지입니다.

[중요: summary 출력 규칙]
- lv1~lv5의 summary는 반드시 한 문장(1줄)로 작성해 주세요.
- 줄바꿈(\\n) 금지이며, summary 문자열 안에 개행이 포함되면 안 됩니다.
- summary에 이모지/이모티콘/장식문자(예: ✅, 🔥, 📌 등) 금지입니다.
- summary에 특수기호로 시작하는 목록/불릿/번호(예: "-", "•", "*", "1)", "1.", "(1)") 금지입니다.
- summary는 문장부호(마침표/쉼표)는 허용하되, 과도한 장식은 피하고 자연스러운 문장으로 작성해 주세요.

[기사 정보]
제목: {t}
내용: {c}

[응답 형식 (JSON)]
{{
  "deep_analysis_reasoning": "여기에는 뉴스 분석을 위한 심층적인 사고 과정을 서술해 주세요. 먼저 팩트를 나열하고, 이것이 거시경제(금리, 환율)와 해당 산업 밸류체인에 미칠 영향을 논리적으로 추론해 주세요. 이 필드는 사용자에게 보여지지 않지만, 뒤이어 나올 전문가용(Lv5) 분석의 질을 높이기 위한 브레인스토밍 공간입니다.",

  "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],
  "sentiment_score": 75,
  "vocabulary": [
    {{"term": "기사에_나온_어려운_용어", "definition": "해당 용어에 대한 초보자용 해설입니다."}}
  ],

  "level_content": {{
    "lv1": {{
      "summary": "한 문장(1줄) 요약입니다. 줄바꿈, 이모지, 불릿, 번호, 장식문자 없이 작성해 주세요.",
      "bullet_points": ["아주 쉬운 핵심 요약 1입니다.", "아주 쉬운 핵심 요약 2입니다.", "아주 쉬운 핵심 요약 3입니다."],
      "what_is_this": ["이 뉴스가 무엇인지 쉽게 설명해 드립니다.", "이 뉴스의 배경을 설명해 드립니다."],
      "why_important": ["이게 왜 중요한지 생활 밀착형으로 설명해 드립니다.", "중요한 이유를 추가로 설명해 드립니다."],
      "stock_impact": {{
        "positives": ["좋은 점 1입니다.", "좋은 점 2입니다."],
        "warnings": ["조심할 점 1입니다.", "조심할 점 2입니다."]
      }},
      "strategy_guide": {{
        "short_term": "주린이를 위한 단기 조언입니다.",
        "long_term": "주린이를 위한 장기 조언입니다."
      }},
      "action_guide": "주린이를 위한 아주 기초적인 조언입니다."
    }},
    "lv2": {{
      "summary": "한 문장(1줄) 요약입니다. 줄바꿈, 이모지, 불릿, 번호, 장식문자 없이 작성해 주세요.",
      "bullet_points": ["쉬운 요약 1입니다.", "쉬운 요약 2입니다.", "쉬운 요약 3입니다."],
      "what_is_this": ["뉴스의 핵심 내용을 설명해 드립니다.", "배경을 추가로 설명해 드립니다."],
      "why_important": ["시장에 중요한 이유를 설명해 드립니다.", "중요한 이유를 추가로 설명해 드립니다."],
      "stock_impact": {{
        "positives": ["긍정적 요인 1입니다.", "긍정적 요인 2입니다."],
        "warnings": ["부정적 요인 1입니다.", "부정적 요인 2입니다."]
      }},
      "strategy_guide": {{
        "short_term": "초보자를 위한 단기 대응법입니다.",
        "long_term": "초보자를 위한 장기 투자 관점입니다."
      }},
      "action_guide": "초보자를 위한 투자 조언입니다."
    }},
    "lv3": {{
      "summary": "한 문장(1줄) 요약입니다. 줄바꿈, 이모지, 불릿, 번호, 장식문자 없이 작성해 주세요.",
      "bullet_points": ["핵심 요약 1입니다.", "핵심 요약 2입니다.", "핵심 요약 3입니다."],
      "what_is_this": ["심도 있는 뉴스 해석을 제공해 드립니다.", "심도 있는 해석을 추가로 제공해 드립니다."],
      "why_important": ["산업 및 시장 영향 분석을 제공해 드립니다.", "영향 분석을 추가로 제공해 드립니다."],
      "stock_impact": {{
        "positives": ["상승 재료 1입니다.", "상승 재료 2입니다."],
        "warnings": ["하락 리스크 1입니다.", "하락 리스크 2입니다."]
      }},
      "strategy_guide": {{
        "short_term": "기술적 분석을 포함한 단기 전략입니다.",
        "long_term": "산업 사이클을 고려한 장기 전략입니다."
      }},
      "action_guide": "중급자를 위한 포트폴리오 조정 조언입니다."
    }},
    "lv4": {{
      "summary": "한 문장(1줄) 요약입니다. 줄바꿈, 이모지, 불릿, 번호, 장식문자 없이 작성해 주세요.",
      "bullet_points": ["전문적 요약 1입니다.", "전문적 요약 2입니다.", "전문적 요약 3입니다."],
      "what_is_this": ["구조적/재무적 관점의 분석을 제공해 드립니다.", "분석을 추가로 제공해 드립니다."],
      "why_important": ["밸류체인 및 거시경제 영향을 설명해 드립니다.", "영향을 추가로 설명해 드립니다."],
      "stock_impact": {{
        "positives": ["펀더멘털 개선 요인 1입니다.", "수급/모멘텀 요인 2입니다."],
        "warnings": ["밸류에이션 부담 1입니다.", "리스크 요인 2입니다."]
      }},
      "strategy_guide": {{
        "short_term": "트레이딩 관점의 매매 전략입니다.",
        "long_term": "밸류에이션 리레이팅 가능성 분석입니다."
      }},
      "action_guide": "숙련자를 위한 매매/헤징 전략입니다."
    }},
    "lv5": {{
      "summary": "한 문장(1줄) 요약입니다. 줄바꿈, 이모지, 불릿, 번호, 장식문자 없이 작성해 주세요. 업계 전문 용어를 적극 사용해 주세요.",
      "bullet_points": ["Insightful Summary 1입니다.", "Insightful Summary 2입니다.", "Insightful Summary 3입니다."],
      "what_is_this": ["심층 분석(Deep Dive)을 제공해 드립니다.", "심층 분석을 추가로 제공해 드립니다."],
      "why_important": ["Global Macro & Sector Impact를 설명해 드립니다.", "영향을 추가로 설명해 드립니다."],
      "stock_impact": {{
        "positives": ["Upside Potential Logic 1입니다.", "Catalyst 2입니다."],
        "warnings": ["Downside Risk 1입니다.", "Risk Factors 2입니다."]
      }},
      "strategy_guide": {{
        "short_term": "Arbitrage / Event-Driven Strategy를 제안해 드립니다.",
        "long_term": "Thematic / Structural Growth Thesis를 제시해 드립니다."
      }},
      "action_guide": "기관 투자자급의 High-Level 전략을 제안해 드립니다."
    }}
  }}
}}

[작성 지침]
1) deep_analysis_reasoning을 가장 먼저 작성해 주세요.
2) lv1~lv5의 summary는 반드시 한 문장(1줄)로 작성해 주시고 줄바꿈(\\n)은 금지입니다.
3) summary에는 번호/불릿/나열(예: "1) 2)", "-", "•", "*") 및 이모지/장식문자를 넣지 말아 주세요.
4) lv1~lv5는 관점/깊이를 확실히 다르게 작성해 주세요.
5) sentiment_score는 0~100 정수로 작성해 주세요.
6) 모든 문장은 존댓말을 사용해 주세요.
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

    ✅ 변경: 레벨별 summary가 "이모티콘/장식 없이 깔끔한 1줄"로 생성되도록 프롬프트 지침만 강화
    """
    content_to_analyze = (news.content or "").strip() or (news.summary or "").strip()
    if not content_to_analyze:
        return None

    client = get_gemini_client()

    prompt = _build_prompt(news.title, content_to_analyze)
    msgs = [
        ChatMessage(
            role="system",
            content=(
                "당신은 JSON만 출력합니다. 다른 텍스트/마크다운은 금지입니다. "
                "모든 문장은 존댓말로 작성합니다. "
                "특히 lv1~lv5 summary는 이모지/장식문자/불릿/번호 없이 한 문장(1줄)로 작성합니다."
            ),
        ),
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
