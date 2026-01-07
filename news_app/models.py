from django.db import models
from pgvector.django import VectorField

class NewsArticle(models.Model):
    title = models.CharField(max_length=500)
    summary = models.TextField()
    content = models.TextField(null=True, blank=True)  # 기사 본문 (LLM 요약용)
    analysis = models.JSONField(null=True, blank=True) # LLM 분석 결과 저장 (요약, Q&A 등)
    url = models.URLField(max_length=1000)
    image_url = models.URLField(max_length=1000, null=True, blank=True)
    published_at = models.DateTimeField()

    # 필터링용 메타데이터 (정확도 검색용)
    market = models.CharField(max_length=50)  # 'US' or 'KR' or 'International'
    ticker = models.CharField(max_length=20, null=True, blank=True) # 'AAPL'
    sector = models.CharField(max_length=50, null=True, blank=True) # '자동차'

    # 벡터 데이터 (문맥 검색용) - 1536차원 (OpenAI 모델 기준)
    embedding = VectorField(dimensions=1536)

    def __str__(self):
        return self.title