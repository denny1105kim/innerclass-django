from django.test import TestCase
from django.conf import settings
from django.utils import timezone  # 날짜 입력을 위해 필요
from news.models import NewsArticle
from pgvector.django import CosineDistance
import openai

class RealAIEmbeddingTest(TestCase):
    
    # [1] 공통 함수: OpenAI API 호출
    def get_embedding(self, text):
        client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        response = client.embeddings.create(
            input=text,
            model="text-embedding-3-small"
        )
        return response.data[0].embedding

    # [2] 데이터 적재
    def setUp(self):
        
        news_data = [
            {
                "title": "손흥민, 시즌 10호골 폭발",
                "summary": "토트넘의 캡틴 손흥민이 극적인 결승골을 터뜨렸다.", # content -> summary로 변경
                "sector": "스포츠",
                "market": "KR" # 모델에 정의된 필수값
            },
            {
                "title": "미 연준, 금리 동결 시사",
                "summary": "파월 의장이 인플레이션 완화를 언급하며 금리를 유지하겠다고 밝혔다.", # content -> summary로 변경
                "sector": "경제",
                "market": "US" # 모델에 정의된 필수값
            }
        ]

        for news in news_data:
            vector = self.get_embedding(news["summary"]) # summary 내용을 벡터화
            
            # 모델 정의에 맞춰 필수 필드(url, published_at 등)를 모두 채워줍니다.
            NewsArticle.objects.create(
                title=news["title"],
                summary=news["summary"],   # content 대신 summary 사용
                sector=news["sector"],
                market=news["market"],
                url="http://test.com",     # 필수 필드라 임시 값 입력
                published_at=timezone.now(), # 필수 필드라 현재 시간 입력
                embedding=vector
            )

    # [3] 의미 기반 검색 테스트
    def test_semantic_search(self):

        query_text = "유럽 축구 경기 결과 어때?" 

        query_vec = self.get_embedding(query_text)

        result = NewsArticle.objects.annotate(
            distance=CosineDistance('embedding', query_vec)
        ).order_by('distance').first()

        
        self.assertEqual(result.title, "손흥민, 시즌 10호골 폭발")

    def test_economy_search(self):
        
        query_text = "요즘 달러 환율이나 이자율 상황"
        
        query_vec = self.get_embedding(query_text)
        
        result = NewsArticle.objects.annotate(
            distance=CosineDistance('embedding', query_vec)
        ).order_by('distance').first()
        
        
        self.assertEqual(result.title, "미 연준, 금리 동결 시사")
