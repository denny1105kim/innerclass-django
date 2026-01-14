from __future__ import annotations

from django.db import models
from pgvector.django import VectorField


class NewsTheme(models.TextChoices):
    SEMICONDUCTOR_AI = "SEMICONDUCTOR_AI", "Semiconductor/AI"
    BATTERY = "BATTERY", "Battery"
    GREEN_ENERGY = "GREEN_ENERGY", "Green Energy"
    FINANCE_HOLDING = "FINANCE_HOLDING", "Finance/Holding"
    ICT_PLATFORM = "ICT_PLATFORM", "ICT/Platform"
    BIO_HEALTH = "BIO_HEALTH", "Bio/Health"
    AUTO = "AUTO", "Auto"
    ETC = "ETC", "ETC"


class NewsMarket(models.TextChoices):
    KOREA = "Korea", "Korea"
    INTERNATIONAL = "International", "International"


class NewsArticle(models.Model):
    title = models.CharField(max_length=500)
    summary = models.TextField()
    content = models.TextField(null=True, blank=True)  # 원문(가능하면)
    url = models.URLField(max_length=1000, unique=True)
    image_url = models.URLField(max_length=1000, null=True, blank=True)
    published_at = models.DateTimeField(db_index=True)

    market = models.CharField(max_length=50, choices=NewsMarket.choices)
    ticker = models.CharField(max_length=20, null=True, blank=True)
    sector = models.CharField(max_length=50, null=True, blank=True)

    # 대표 theme(리스트 필터/정렬용) - Lv1 분석으로 채움
    theme = models.CharField(
        max_length=30,
        choices=NewsTheme.choices,
        default=NewsTheme.ETC,
        db_index=True,
    )

    embedding = VectorField(dimensions=1536)

    class Meta:
        indexes = [
            models.Index(fields=["market", "-published_at"]),
            models.Index(fields=["theme", "-published_at"]),
        ]

    def __str__(self) -> str:
        return self.title


class AnalysisLevel(models.IntegerChoices):
    LV1 = 1, "Lv1"
    LV2 = 2, "Lv2"
    LV3 = 3, "Lv3"
    LV4 = 4, "Lv4"
    LV5 = 5, "Lv5"


class NewsArticleAnalysis(models.Model):
    """
    기사 1개당 Lv1~Lv5 분석 결과를 각각 1 row씩 저장.
    """
    article = models.ForeignKey(
        NewsArticle,
        on_delete=models.CASCADE,
        related_name="analyses",
    )

    level = models.IntegerField(choices=AnalysisLevel.choices, db_index=True)

    theme = models.CharField(
        max_length=30,
        choices=NewsTheme.choices,
        default=NewsTheme.ETC,
        db_index=True,
    )

    analysis = models.JSONField(null=True, blank=True)  # 해당 레벨 분석 JSON
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["article", "level"], name="uniq_article_level"),
        ]
        indexes = [
            models.Index(fields=["article", "level"]),
        ]

    def __str__(self) -> str:
        return f"{self.article_id} Lv{self.level}"
