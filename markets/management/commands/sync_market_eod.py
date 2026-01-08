from datetime import date as _date

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from ...services.market_sync import sync_market_eod


# 단일 실행 허용 market
ALLOWED = {"KOSPI", "KOSDAQ", "NASDAQ", "KR", "US"}

# ARGS 없을 때 자동 실행할 시장 목록
DEFAULT_MARKETS = ["KOSPI", "KOSDAQ", "NASDAQ"]


class Command(BaseCommand):
    help = (
        "Sync EOD market data (top 100 by market cap).\n"
        "- With --market: sync only that market\n"
        "- Without args: sync KOSPI, KOSDAQ, NASDAQ sequentially"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--market",
            type=str,
            default="",
            help="KOSPI | KOSDAQ | NASDAQ | KR | US (optional)",
        )
        parser.add_argument("--date", type=str, default="", help="YYYY-MM-DD (optional)")

    def handle(self, *args, **options):
        # =========================
        # 날짜 결정
        # =========================
        date_str = (options.get("date") or "").strip()
        if date_str:
            try:
                y, m, d = map(int, date_str.split("-"))
                target = _date(y, m, d)
            except Exception:
                raise CommandError("--date must be YYYY-MM-DD")
        else:
            target = timezone.localdate()

        # =========================
        # market 결정
        # =========================
        market_opt = (options.get("market") or "").upper().strip()

        if market_opt:
            if market_opt not in ALLOWED:
                raise CommandError(f"--market must be one of {sorted(ALLOWED)}")
            markets = [market_opt]
        else:
            # ARGS 없으면 기본 3개 시장 전부 실행
            markets = DEFAULT_MARKETS

        # =========================
        # 실행
        # =========================
        for market in markets:
            self.stdout.write(
                self.style.WARNING(
                    f"[RUN] EOD sync start: market={market} date={target.isoformat()}"
                )
            )

            try:
                res = sync_market_eod(market=market, target_date=target)
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"[FAIL] market={market} error={e}")
                )
                continue

            self.stdout.write(
                self.style.SUCCESS(
                    f"[OK] EOD market={res.market} asof={res.asof.isoformat()} "
                    f"stocks_upserted={res.stocks_upserted} "
                    f"indicators_upserted={res.indicators_upserted}"
                )
            )
