from django.contrib import admin

from .models import DailyStockSnapshot, PromptTemplate, Stock


@admin.register(Stock)
class StockAdmin(admin.ModelAdmin):
    list_display = ("market", "symbol", "name", "exchange", "currency")
    list_filter = ("market", "exchange", "currency")
    search_fields = ("symbol", "name")


@admin.register(DailyStockSnapshot)
class DailyStockSnapshotAdmin(admin.ModelAdmin):
    list_display = ("date", "stock", "market_cap", "intraday_pct", "change_pct")
    list_filter = ("date", "stock__market")
    search_fields = ("stock__symbol", "stock__name")


@admin.register(PromptTemplate)
class PromptTemplateAdmin(admin.ModelAdmin):
    list_display = ("key", "name", "is_active", "updated_at")
    list_filter = ("is_active",)
    search_fields = ("key", "name", "description", "system_prompt", "user_prompt_template")
