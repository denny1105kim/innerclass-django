# news/services/analyze_news_main.py
from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, List

import openai
from django.conf import settings

from ..models import NewsArticle


# ----------------------------
# Config
# ----------------------------
MODEL_NAME = getattr(settings, "OPENAI_NEWS_ANALYSIS_MODEL", "gpt-4o-mini")
TEMPERATURE = float(getattr(settings, "OPENAI_NEWS_ANALYSIS_TEMPERATURE", 0.5))
MAX_TOKENS = int(getattr(settings, "OPENAI_NEWS_ANALYSIS_MAX_TOKENS", 1600))


# ----------------------------
# Helpers
# ----------------------------
def _strip_code_fences(text: str) -> str:
    t = (text or "").strip()
    if not t.startswith("```"):
        return t
    parts = t.split("```")
    if len(parts) < 3:
        return t
    inner = parts[1].strip()
    if inner.startswith("json"):
        inner = inner[4:].strip()
    return inner.strip()


def _extract_json_object(text: str) -> str:
    t = _strip_code_fences(text)
    # If already looks like a JSON object, return as-is
    if t.startswith("{") and t.endswith("}"):
        return t
    # Best-effort: grab first {...}
    m = re.search(r"\{.*\}", t, flags=re.DOTALL)
    if not m:
        raise ValueError("No JSON object found in model output.")
    return m.group(0)


def _coerce_keywords(v: Any) -> List[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str) and v.strip():
        # allow comma-separated fallback
        return [s.strip() for s in v.split(",") if s.strip()]
    return []


def _coerce_vocabulary(v: Any) -> List[Dict[str, str]]:
    if not isinstance(v, list):
        return []
    out: List[Dict[str, str]] = []
    for item in v:
        if isinstance(item, dict):
            term = str(item.get("term", "")).strip()
            definition = str(item.get("definition", "")).strip()
            if term and definition:
                out.append({"term": term, "definition": definition})
    return out


def _coerce_int_0_100(v: Any, default: int = 50) -> int:
    try:
        x = int(float(v))
    except Exception:
        return default
    return max(0, min(100, x))


def _ensure_schema(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonical output for main-page analysis.
    - NO level_content / lv1~lv5 fields.
    - Single 'analysis' block is Lv3-style.
    """
    return {
        "deep_analysis_reasoning": str(result.get("deep_analysis_reasoning", "")).strip(),
        "keywords": _coerce_keywords(result.get("keywords")),
        "sentiment_score": _coerce_int_0_100(result.get("sentiment_score"), default=50),
        "vocabulary": _coerce_vocabulary(result.get("vocabulary")),
        "analysis": {
            "summary": str(result.get("analysis", {}).get("summary", "")).strip()
            if isinstance(result.get("analysis"), dict)
            else str(result.get("summary", "")).strip(),
            "bullet_points": (
                result.get("analysis", {}).get("bullet_points", [])
                if isinstance(result.get("analysis"), dict)
                else result.get("bullet_points", [])
            )
            or [],
            "what_is_this": (
                result.get("analysis", {}).get("what_is_this", [])
                if isinstance(result.get("analysis"), dict)
                else result.get("what_is_this", [])
            )
            or [],
            "why_important": (
                result.get("analysis", {}).get("why_important", [])
                if isinstance(result.get("analysis"), dict)
                else result.get("why_important", [])
            )
            or [],
            "stock_impact": (
                result.get("analysis", {}).get("stock_impact", {})
                if isinstance(result.get("analysis"), dict)
                else result.get("stock_impact", {})
            )
            or {"positives": [], "warnings": []},
            "strategy_guide": (
                result.get("analysis", {}).get("strategy_guide", {})
                if isinstance(result.get("analysis"), dict)
                else result.get("strategy_guide", {})
            )
            or {"short_term": "", "long_term": ""},
            "action_guide": str(
                (
                    result.get("analysis", {}).get("action_guide", "")
                    if isinstance(result.get("analysis"), dict)
                    else result.get("action_guide", "")
                )
            ).strip(),
        },
    }


# ----------------------------
# Public API
# ----------------------------
def analyze_news_main(article: NewsArticle, save_to_db: bool = True) -> Optional[Dict[str, Any]]:
    """
    Main-page oriented analysis (Lv3-style only).
    - Removes the whole level_content concept.
    - Produces a single 'analysis' section written in Lv3 tone:
      industry trend + terminology allowed + portfolio perspective.
    """
    content_to_analyze = article.content if article.content else article.summary
    if not content_to_analyze:
        return None

    client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

    prompt = f"""다음 뉴스 기사를 '중급자(Lv3) 관점'으로만 심층 분석하여 아래 형식의 JSON으로 응답해줘.
다른 말은 덧붙이지 말고 반드시 JSON 데이터만 출력해.

[Lv3 작성 가이드(반드시 반영)]
- 산업 트렌드/밸류체인 관점으로 설명할 것.
- 기술적/산업 용어 사용 가능(단, 용어는 vocabulary에 초보자용 정의를 함께 제공).
- 포트폴리오 관점(업종 노출, 리스크 요인, 촉매/모멘텀, 시나리오)에서 해석할 것.
- 단기/중장기 전략을 구분해 제시할 것(예: 이벤트 드리븐/모멘텀 vs 사이클/구조적 성장).
- 과도한 확정 표현 금지: 가능한 경우 조건부/시나리오 기반으로 작성.

[기사 정보]
제목: {article.title}
내용: {content_to_analyze}

[응답 형식 (JSON)]
{{
  "deep_analysis_reasoning": "팩트 정리 → 매크로(금리/환율/유동성) 연결 → 산업 밸류체인 영향 → 가격/수급/실적 경로로 추론. (이 필드는 사용자에게 보여지지 않는 내부 메모)",
  "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],
  "sentiment_score": 75,
  "vocabulary": [
    {{"term": "기사에_나온_어려운_용어", "definition": "해당 용어에 대한 초보자용 해설"}}
  ],
  "analysis": {{
    "summary": "Lv3 톤으로 요약(산업/포트폴리오 관점)",
    "bullet_points": ["핵심 요약 1", "핵심 요약 2", "핵심 요약 3"],
    "what_is_this": ["심도 있는 뉴스 해석 1", "심도 있는 뉴스 해석 2"],
    "why_important": ["산업 및 시장 영향 분석 1", "시장 영향 분석 2"],
    "stock_impact": {{
      "positives": ["상승 재료 1", "상승 재료 2"],
      "warnings": ["하락 리스크 1", "하락 리스크 2"]
    }},
    "strategy_guide": {{
      "short_term": "기술적/모멘텀/이벤트 요인을 포함한 단기 대응",
      "long_term": "산업 사이클/구조적 성장/리레이팅 가능성을 포함한 중장기 관점"
    }},
    "action_guide": "중급자를 위한 포트폴리오 액션(비중/헤지/관찰지표)"
  }}
}}

[추가 지침]
1) deep_analysis_reasoning을 반드시 가장 먼저 작성.
2) 반드시 위 JSON 스키마를 준수.
3) sentiment_score는 0~100 정수.
"""

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "당신은 buy-side 애널리스트 관점의 중급자(Lv3)용 리서치 노트를 작성합니다. "
                        "반드시 JSON만 출력하세요."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )

        raw = response.choices[0].message.content or ""
        json_text = _extract_json_object(raw)
        parsed = json.loads(json_text)

        result = _ensure_schema(parsed)

        if save_to_db:
            article.analysis = result
            article.save(update_fields=["analysis"])

        return result

    except Exception as e:
        print(f"[analyze_news_main] Error analyzing news: {e}")
        return None
