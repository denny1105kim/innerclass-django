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

    analysis = models.JSONField(null=True, blank=True)

    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["trend", "created_at"]),
            models.Index(fields=["needs_image_gen", "created_at"]),
            models.Index(fields=["updated_at"]),
        ]

    def __str__(self) -> str:
        return f"TrendKeywordNews {self.trend_id} {self.title[:40]}"
