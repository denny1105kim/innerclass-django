from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from pgvector.django import VectorField


class NewsMarket(models.TextChoices):
    KR = "KR", "국내"
    INTERNATIONAL = "INTERNATIONAL", "해외"


class NewsSector(models.TextChoices):
    SEMICONDUCTOR_AI = "SEMICONDUCTOR_AI", "반도체 / AI"
    BATTERY = "BATTERY", "배터리 (2차전지)"
    GREEN_ENERGY = "GREEN_ENERGY", "원자력 / 친환경에너지"
    FINANCE_HOLDING = "FINANCE_HOLDING", "금융 / 지주사"
    ICT_PLATFORM = "ICT_PLATFORM", "정보통신 / 플랫폼"
    BIO_HEALTH = "BIO_HEALTH", "바이오 / 헬스케어"
    AUTO = "AUTO", "자동차"
    SHIPBUILDING = "SHIPBUILDING", "조선"
    ETC = "ETC", "기타"


class NewsArticle(models.Model):
    title = models.CharField(max_length=500)
    summary = models.TextField()
    content = models.TextField(null=True, blank=True)
    analysis = models.JSONField(null=True, blank=True)

    url = models.URLField(max_length=1000)
    image_url = models.URLField(max_length=1000, null=True, blank=True)

    published_at = models.DateTimeField(db_index=True)

    # ✅ market 통일: KR / INTERNATIONAL
    market = models.CharField(
        max_length=16,
        choices=NewsMarket.choices,
        default=NewsMarket.KR,
        db_index=True,
    )

    related_name = models.CharField(
        max_length=255,
        blank=True,
        default="",
        db_index=True,
    )

    ticker = models.CharField(
        max_length=20,
        blank=True,
        default="",
        db_index=True,
    )

    sector = models.CharField(
        max_length=32,
        choices=NewsSector.choices,
        default=NewsSector.ETC,
        db_index=True,
    )

    confidence = models.FloatField(
        default=0.0,
        db_index=True,
        validators=[MinValueValidator(0.0), MaxValueValidator(1.0)],
    )
    
    # ✅ 임베딩은 로컬 하나로만 통일 (768: multilingual-e5-base 기준)
    embedding_local = VectorField(dimensions=768, null=True, blank=True)

    def __str__(self) -> str:
        return self.title
