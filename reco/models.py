# apps/reco/models.py
from __future__ import annotations

from django.db import models


class TrendScope(models.TextChoices):
    KR = "KR", "Korea"
    US = "US", "United States"


class TrendKeywordDaily(models.Model):
    date = models.DateField(db_index=True)
    scope = models.CharField(max_length=3, choices=TrendScope.choices, db_index=True)

    rank = models.PositiveSmallIntegerField(default=1)
    keyword = models.CharField(max_length=80)
    reason = models.TextField()

    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("date", "scope", "rank")]
        indexes = [
            models.Index(fields=["date", "scope"]),
            models.Index(fields=["scope", "updated_at"]),
        ]
        ordering = ["rank"]

    def __str__(self) -> str:
        return f"TrendKeywordDaily {self.date} {self.scope} #{self.rank} {self.keyword}"


class TrendKeywordNews(models.Model):
    trend = models.ForeignKey(
        TrendKeywordDaily,
        on_delete=models.CASCADE,
        related_name="news_items",
        db_index=True,
    )

    title = models.CharField(max_length=300)
    summary = models.TextField()

    content = models.TextField(blank=True, default="")

    link = models.URLField(max_length=1000, db_index=True)
    image_url = models.URLField(max_length=1000, blank=True, default="")

    # "YYYY-MM-DD HH:MM" (KST)
    published_at = models.CharField(max_length=50, blank=True, default="")

    needs_image_gen = models.BooleanField(default=False, db_index=True)

    # =========================
    # NEW: analysis storage
    # =========================
    # full JSON (deep_analysis_reasoning + keywords + sentiment_score + vocabulary + level_content)
    analysis_full = models.JSONField(blank=True, null=True)

    analyzed_at = models.DateTimeField(blank=True, null=True, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["trend", "created_at"]),
            models.Index(fields=["needs_image_gen", "created_at"]),
            models.Index(fields=["analyzed_at", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"TrendKeywordNews {self.trend_id} {self.title[:40]}"


class TrendKeywordNewsAnalysis(models.Model):
    """
    NewsArticleAnalysis와 동일 패턴:
    - news 1건에 대해 level(1~5) 행을 저장
    - analysis JSON에는 공통(meta) + 해당 레벨 content merge 저장
    """
    news = models.ForeignKey(
        TrendKeywordNews,
        on_delete=models.CASCADE,
        related_name="analyses",
        db_index=True,
    )

    level = models.PositiveSmallIntegerField(db_index=True)  # 1..5
    analysis = models.JSONField(default=dict)

    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [("news", "level")]
        indexes = [
            models.Index(fields=["news", "level"]),
            models.Index(fields=["level", "updated_at"]),
        ]
        ordering = ["level"]

    def __str__(self) -> str:
        return f"TrendKeywordNewsAnalysis news={self.news_id} lv={self.level}"
