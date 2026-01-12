from __future__ import annotations

from django.core.management.base import BaseCommand
from markets.services.daily_rank_sync import sync_daily_rankings


class Command(BaseCommand):
    help = "KR(Daum) + NASDAQ(SlickCharts tickers + yfinance) 기반 일별 랭킹 스냅샷을 DB에 저장합니다."

    def add_arguments(self, parser):
        parser.add_argument("--per-page", type=int, default=200)
        parser.add_argument("--no-check-open", action="store_true")
        parser.add_argument("--force", action="store_true")

        # NEW: grace windows
        parser.add_argument(
            "--pre-open-grace-min",
            type=int,
            default=5,
            help="개장 전 N분부터 sync를 허용합니다(다음 세션 기준). 기본 5.",
        )
        parser.add_argument(
            "--post-close-grace-min",
            type=int,
            default=10,
            help="폐장 후 N분까지 sync를 허용합니다(이전 세션 기준). 기본 10.",
        )

    def handle(self, *args, **options):
        per_page = int(options["per_page"])
        check_open = not bool(options.get("no_check_open"))
        force = bool(options.get("force"))
        pre = int(options.get("pre_open_grace_min") or 0)
        post = int(options.get("post_close_grace_min") or 0)

        results = sync_daily_rankings(
            per_page=per_page,
            check_open=check_open,
            force=force,
            pre_open_grace_min=pre,
            post_close_grace_min=post,
        )

        for k, v in results.items():
            self.stdout.write(f"{k}: {v} rows")
