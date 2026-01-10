import openai
from django.conf import settings
import json
from .models import NewsArticle

def analyze_news(article: NewsArticle, save_to_db=True):
    """
    기사 객체를 받아 LLM 분석을 수행하고 결과를 반환합니다.
    save_to_db=True일 경우 결과를 DB에 저장합니다.
    """
    content_to_analyze = article.content if article.content else article.summary
    
    if not content_to_analyze:
        return None

    try:
        # settings에 있는 API 키 사용 (로컬 LLM/Ollama 사용 시 base_url 등 설정 필요)
        client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        
        prompt = f"""다음 뉴스 기사를 심층 분석하여 아래 형식의 JSON으로 응답해줘. 
다른 말은 덧붙이지 말고 반드시 JSON 데이터만 출력해.

[기사 정보]
제목: {article.title}
내용: {content_to_analyze}

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
                # 시스템 프롬프트: '금융 전문가' 페르소나를 강력하게 주입. CoT를 유도하기 위한 지시 추가.
                {"role": "system", "content": "당신은 월스트리트의 수석 애널리스트이자, 동시에 친절한 금융 교육자입니다. 당신의 목표는 독자의 수준(Lv1~Lv5)에 맞춰 완벽하게 다른 톤앤매너로 정보를 전달하는 것입니다. 분석 전 반드시 심층 추론(deep_analysis_reasoning)을 수행하세요."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=3000 # 분석 내용이 길어질 수 있으므로 토큰 여유 확보
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # JSON 블록 추출 (마크다운 포맷 제거)
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.strip().startswith("json"):
                result_text = result_text.strip()[4:]
        
        result = json.loads(result_text)
        
        if save_to_db:
            article.analysis = result
            article.save()
            
        return result
        
    except Exception as e:
        print(f"Error analyzing news: {e}") # 디버깅을 위해 에러 출력 추가
        return None