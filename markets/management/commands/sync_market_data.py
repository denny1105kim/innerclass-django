# apps/markets/management/commands/sync_market_data.py

from datetime import date as _date

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from ...services.market_sync import sync_market


ALLOWED = {"KOSPI", "KOSDAQ", "NASDAQ", "KR", "US"}


class Command(BaseCommand):
    help = "Sync market data into DB. Intended to run every 5 minutes."

    def add_arguments(self, parser):
        parser.add_argument(
            "--market",
            type=str,
            default="KOSDAQ",
            help="KOSPI | KOSDAQ | NASDAQ | KR | US",
        )
        parser.add_argument("--date", type=str, default="", help="YYYY-MM-DD (optional)")

    def handle(self, *args, **options):
        market = (options.get("market") or "KOSDAQ").upper().strip()
        if market not in ALLOWED:
            raise CommandError(f"--market must be one of {sorted(ALLOWED)}")

        date_str = (options.get("date") or "").strip()
        if date_str:
            try:
                y, m, d = map(int, date_str.split("-"))
                target = _date(y, m, d)
            except Exception:
                raise CommandError("--date must be YYYY-MM-DD")
        else:
            target = timezone.localdate()

        res = sync_market(market=market, target_date=target)
        self.stdout.write(
            self.style.SUCCESS(
                f"[OK] market={res.market} asof={res.asof} "
                f"indicators_upserted={res.indicators_upserted} stocks_upserted={res.stocks_upserted}"
            )
        )
