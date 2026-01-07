from django.contrib import admin

from .models import Stock, DailyStockSnapshot


@admin.register(Stock)
class StockAdmin(admin.ModelAdmin):
    list_display = ("market", "exchange", "symbol", "name", "currency")
    list_filter = ("market", "exchange", "currency")
    search_fields = ("symbol", "name")

class DailyStockSnapshotAdmin(admin.ModelAdmin):
    """
    Daily snapshot (EOD + intraday overwrite)
    """
    list_display = (
        "date",
        "stock",
        "close",
        "intraday_pct",
        "change_pct",
        "market_cap",
        "volume",
    )
    list_filter = ("date", "stock__market", "stock__exchange")
    search_fields = ("stock__symbol", "stock__name")
    ordering = ("-date",)
