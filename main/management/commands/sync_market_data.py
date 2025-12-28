from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand, CommandError

from main.models import Market
from main.services.market_sync import SyncOptions, sync_market_data


class Command(BaseCommand):
    help = "Fetch market data and overwrite today's snapshots (top market cap + top drawdown)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--market",
            type=str,
            required=True,
            choices=[Market.KR, Market.US],
            help="Market code: KR or US",
        )
        parser.add_argument(
            "--asof",
            type=str,
            default=None,
            help="Date in YYYY-MM-DD (default: today)",
        )
        parser.add_argument(
            "--universe",
            nargs="*",
            default=None,
            help=(
                "Symbols to consider (recommended/required for US). "
                "Example: --universe AAPL MSFT NVDA TSLA"
            ),
        )
        parser.add_argument(
            "--topn-mcap",
            type=int,
            default=5,
            help="Top N by market cap (default: 5)",
        )
        parser.add_argument(
            "--topn-dd",
            type=int,
            default=5,
            help="Top N by drawdown (intraday open->close) (default: 5)",
        )
        parser.add_argument(
            "--no-overwrite",
            action="store_true",
            help="Do not delete today's rows; append instead (default behavior is overwrite)",
        )

    def handle(self, *args, **options):
        market = options["market"]

        asof_str = options.get("asof")
        if asof_str:
            try:
                y, m, d = [int(x) for x in asof_str.split("-")]
                asof = date(y, m, d)
            except Exception:
                raise CommandError("--asof must be YYYY-MM-DD")
        else:
            asof = date.today()

        universe = options.get("universe")
        topn_mcap = int(options["topn_mcap"])
        topn_dd = int(options["topn_dd"])
        overwrite_today = not options["no_overwrite"]

        opts = SyncOptions(
            market=market,
            asof=asof,
            overwrite_today=overwrite_today,
            topn_market_cap=topn_mcap,
            topn_drawdown=topn_dd,
            universe=universe,
        )

        try:
            result = sync_market_data(opts)
        except ValueError as e:
            raise CommandError(str(e))

        self.stdout.write(self.style.SUCCESS("sync_market_data completed"))
        self.stdout.write(str(result))
