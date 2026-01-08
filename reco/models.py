from __future__ import annotations

from django.db import models


# =========================================================
# Daily Theme Picks (LLM output persisted, overwritten daily)
# =========================================================
class ThemeScope(models.TextChoices):
    ALL = "ALL", "All"
    KR = "KR", "Korea"
    US = "US", "United States"


class ThemePickDaily(models.Model):
    """
    매일 1회 생성된 테마 추천 결과를 저장.
    - date+scope+rank로 unique → 매일 덮어쓰기 구현이 쉽다.
    - 종목은 단순 symbol/name을 저장(Stock FK 강제 X) : LLM 결과가 신규/별칭일 수 있으므로 유연하게.
    """

    date = models.DateField(db_index=True)  # 기준일 (KST date)
    scope = models.CharField(max_length=3, choices=ThemeScope.choices, db_index=True)

    rank = models.PositiveSmallIntegerField(default=1)  # 1..N
    theme = models.CharField(max_length=80)
    symbol = models.CharField(max_length=32)
    name = models.CharField(max_length=128)
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
        return f"ThemePickDaily {self.date} {self.scope} #{self.rank} {self.theme} {self.symbol}"


# =========================================================
# Daily Trend Keywords (LLM output persisted, overwritten daily)
# =========================================================
class TrendKeywordDaily(models.Model):
    """
    매일 1회 생성된 트렌드 키워드 3개를 저장.
    - date+scope+rank unique → 매일 덮어쓰기
    - keyword/reason만 저장 (종목 추천과 분리)
    """

    date = models.DateField(db_index=True)  # 기준일 (KST date)
    scope = models.CharField(max_length=3, choices=ThemeScope.choices, db_index=True)

    rank = models.PositiveSmallIntegerField(default=1)  # 1..N
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
