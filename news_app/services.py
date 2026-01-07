import openai
from django.conf import settings
import json
from .models import NewsArticle

def analyze_news(article: NewsArticle, save_to_db=True):
    """
    기사 객체를 받아 LLM 분석을 수행하고 결과를 반환합니다.
    save_to_db=True일 경우 결과를 DB에 저장합니다.
    """
    # 이미 분석된 데이터가 있고 DB 저장을 원하지 않으면 그것을 반환 (Optional)
    # 하지만 이 함수는 강제 분석 또는 최초 분석 시 호출됨을 가정함
    
    content_to_analyze = article.content if article.content else article.summary
    
    if not content_to_analyze:
        return None

    try:
        client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        
        prompt = f"""다음 뉴스 기사를 분석해서 아래 형식의 JSON으로 응답해줘. 반드시 JSON만 출력해.

기사 제목: {article.title}
기사 내용: {content_to_analyze}

응답 형식:
{{
    "bullet_points": ["핵심 요약 1", "핵심 요약 2", "핵심 요약 3"],
    "what_is_this": ["이 뉴스의 핵심 내용 설명 1", "이 뉴스의 핵심 내용 설명 2"],
    "why_important": ["중요한 이유 1", "중요한 이유 2", "중요한 이유 3"],
    "stock_impact": {{
        "positives": ["긍정적인 점 1", "긍정적인 점 2", "긍정적인 점 3"],
        "warnings": ["주의할 점 1", "주의할 점 2"]
    }},
    "sentiment_score": 75,
    "strategy_guide": {{
        "short_term": "단기 관점 분석",
        "long_term": "장기 관점 분석"
    }},
    "investment_action": ["투자 관련 조언 1", "투자 관련 조언 2"],
    "vocabulary": [
        {{"term": "용어1", "definition": "용어1에 대한 쉬운 설명"}},
        {{"term": "용어2", "definition": "용어2에 대한 쉬운 설명"}},
        {{"term": "용어3", "definition": "용어3에 대한 쉬운 설명"}}
    ]
}}

주의사항:
- 모든 내용은 투자 초보자도 이해하기 쉽게 작성
- 감정 점수는 0-100 사이 (50 중립, 100 매우 긍정)
- 전문 용어는 vocabulary에서 쉽게 설명
- 투자 권유가 아닌 정보 제공 목적임을 명시"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "너는 금융 뉴스 분석 전문가야. 투자 초보자도 이해할 수 있도록 쉽게 설명해줘."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # JSON 블록 추출
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
        
        result = json.loads(result_text)
        
        if save_to_db:
            article.analysis = result
            article.save()
            
        return result
        
    except Exception as e:
        return None
