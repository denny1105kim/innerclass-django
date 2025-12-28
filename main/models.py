from django.db import models


class Market(models.TextChoices):
    KR = "KR", "Korea"
    US = "US", "United States"


class Stock(models.Model):
    market = models.CharField(max_length=2, choices=Market.choices, db_index=True)
    symbol = models.CharField(max_length=32, db_index=True)  # KR: "005930", US: "AAPL"
    name = models.CharField(max_length=128)

    currency = models.CharField(max_length=8, default="KRW")  # KRW / USD
    exchange = models.CharField(max_length=32, blank=True, default="")  # KOSPI/KOSDAQ/NASDAQ/NYSE 등

    class Meta:
        unique_together = [("market", "symbol")]

    def __str__(self) -> str:
        return f"{self.market}:{self.symbol} {self.name}"


class DailyStockSnapshot(models.Model):
    stock = models.ForeignKey(Stock, on_delete=models.CASCADE, related_name="snapshots")
    date = models.DateField(db_index=True)

    # intraday metrics
    open = models.DecimalField(max_digits=20, decimal_places=4, null=True)
    close = models.DecimalField(max_digits=20, decimal_places=4, null=True)

    # kept for compatibility / future use
    prev_close = models.DecimalField(max_digits=20, decimal_places=4, null=True)

    # by default we store intraday_pct in change_pct too (so existing UI can treat it as "change")
    change_pct = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    intraday_pct = models.DecimalField(max_digits=10, decimal_places=4, null=True)

    market_cap = models.BigIntegerField(null=True)
    volume = models.BigIntegerField(null=True)

    # retained for future extension; currently not populated by the 5-min job
    volatility_20d = models.DecimalField(max_digits=10, decimal_places=4, null=True)

    class Meta:
        unique_together = [("stock", "date")]
        indexes = [
            models.Index(fields=["date", "stock"]),
            models.Index(fields=["date", "market_cap"]),
            models.Index(fields=["date", "intraday_pct"]),
        ]