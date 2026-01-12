# news/services/analyze_news.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional

import openai
from django.conf import settings
from django.db import transaction

from news.models import NewsArticle, NewsArticleAnalysis, NewsTheme


THEME_CHOICES = [
    "SEMICONDUCTOR_AI",
    "BATTERY",
    "GREEN_ENERGY",
    "FINANCE_HOLDING",
    "ICT_PLATFORM",
    "BIO_HEALTH",
    "AUTO",
    "ETC",
]


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


def _build_level_payload(full: Dict[str, Any], level_key: str) -> Dict[str, Any]:
    """
    full 결과에서 공통(meta) + 특정 레벨(level_content[level_key])를 합쳐서
    해당 레벨 row에 저장하기 좋은 JSON으로 만든다.
    """
    common = {
        "deep_analysis_reasoning": full.get("deep_analysis_reasoning", ""),
        "theme": full.get("theme", NewsTheme.ETC),
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


def analyze_news(article: NewsArticle, save_to_db: bool = True) -> Optional[Dict[str, Any]]:
    """
    기사 객체를 받아 LLM 분석을 수행하고 결과를 반환합니다.
    save_to_db=True일 경우:
      - NewsArticle.theme 를 Lv1 기반 theme으로 저장
      - NewsArticleAnalysis에 Lv1~Lv5 row를 각각 upsert
    """
    content_to_analyze = (article.content or "").strip() or (article.summary or "").strip()
    if not content_to_analyze:
        return None

    try:
        client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

        # ====== 원본 프롬프트 유지 + theme 분류만 추가 ======
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
- 재생에너지/탄소중립/수소/태양광/풍력/정책이면 GREEN_ENERGY
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
2. 각 레벨(lv1~lv5)별로 어조(Tone & Manner)와 내용의 깊이(Depth)를 명확히 차별화할 것. 똑같은 내용을 말만 바꾸지 말고, *관점* 자체를 다르게 가져갈 것.
3. Lv1은 아주 쉽고 친절하게, Lv5는 매우 전문적이고 냉철하게 작성할 것.
4. 감정 점수(sentiment_score)는 0(매우 부정)~100(매우 긍정) 사이의 정수.
"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "당신은 월스트리트의 수석 애널리스트이자, 동시에 친절한 금융 교육자입니다. JSON만 출력하세요."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
            max_tokens=3000,
        )

        result_text = (response.choices[0].message.content or "").strip()
        result_text = _strip_code_fences(result_text)

        full = json.loads(result_text)

        # theme 보정
        theme = _safe_theme(str(full.get("theme", "")))
        full["theme"] = theme

        if save_to_db:
            with transaction.atomic():
                # 대표 theme은 Lv1이 정한 theme으로 NewsArticle에 저장
                if article.theme != theme:
                    article.theme = theme
                    article.save(update_fields=["theme"])

                # Lv1~Lv5 각각 row 저장
                level_map = {
                    1: "lv1",
                    2: "lv2",
                    3: "lv3",
                    4: "lv4",
                    5: "lv5",
                }
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
