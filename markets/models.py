from __future__ import annotations

from django.db import models


class MarketChoices(models.TextChoices):
    KOSPI = "KOSPI", "KOSPI"
    KOSDAQ = "KOSDAQ", "KOSDAQ"
    NASDAQ = "NASDAQ", "NASDAQ"


class RankingTypeChoices(models.TextChoices):
    MARKET_CAP = "MARKET_CAP", "시가총액"
    RISE = "RISE", "상승률"
    FALL = "FALL", "하락률"


class DailyRankingSnapshot(models.Model):
    """
    일별 랭킹 스냅샷을 저장.

    - 원본 row를 payload(JSON)에 그대로 저장하여 필드 변경에도 방어
    - 자주 쓰는 필드(symbol_code, name, trade_price, change_rate)는 별도 컬럼으로 중복 저장
    """

    asof_date = models.DateField(db_index=True)
    market = models.CharField(max_length=10, choices=MarketChoices.choices, db_index=True)
    ranking_type = models.CharField(max_length=20, choices=RankingTypeChoices.choices, db_index=True)
    rank = models.PositiveIntegerField(db_index=True)

    symbol_code = models.CharField(max_length=20, db_index=True)  # e.g., "A005930" or "AAPL"
    name = models.CharField(max_length=200)

    trade_price = models.FloatField(null=True, blank=True)
    change_rate = models.FloatField(null=True, blank=True)

    payload = models.JSONField()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("asof_date", "market", "ranking_type", "rank")
        indexes = [
            models.Index(fields=["asof_date", "market", "ranking_type", "rank"]),
            models.Index(fields=["asof_date", "market", "ranking_type"]),
            models.Index(fields=["symbol_code", "asof_date"]),
        ]

    def __str__(self) -> str:
        return f"{self.asof_date} {self.market} {self.ranking_type} #{self.rank} {self.symbol_code} {self.name}"
