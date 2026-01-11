from __future__ import annotations

from django.core.management.base import BaseCommand

from markets.services.daily_rank_sync import sync_daily_rankings


class Command(BaseCommand):
    help = "KR(Daum) + NASDAQ(SlickCharts tickers + yfinance) 기반 일별 랭킹 스냅샷을 DB에 저장합니다."

    def add_arguments(self, parser):
        parser.add_argument("--per-page", type=int, default=200)

    def handle(self, *args, **options):
        per_page = int(options["per_page"])
        results = sync_daily_rankings(per_page=per_page)

        for k, v in results.items():
            self.stdout.write(f"{k}: {v} rows")