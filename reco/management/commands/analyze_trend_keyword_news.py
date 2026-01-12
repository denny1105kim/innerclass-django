# apps/reco/management/commands/analyze_trend_keyword_news.py
from __future__ import annotations

from datetime import date
from typing import Optional

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone
from zoneinfo import ZoneInfo

from ...models import TrendKeywordDaily, TrendKeywordNews, TrendScope
from ...services.analyze_trend_news import analyze_trend_keyword_news


class Command(BaseCommand):
    """
    Args 없이 실행하면:
      - KR/US 각각 '가장 최신 날짜'를 잡고
      - 해당 날짜의 TrendKeywordDaily 아래 TrendKeywordNews 중
      - 아직 분석 안 된 것만(analysis_full/analyzed_at 기준) 전부 분석한다.

    옵션:
      --force: 이미 분석된 것도 재분석
      --limit: 스코프별 최대 처리 개수
      --scope: KR 또는 US만 실행
      --date: 특정 날짜만 실행 (YYYY-MM-DD)
      --latest: (기본 True와 동일 개념) 해당 scope 최신 날짜 사용
    """

    help = (
        "Analyze TrendKeywordNews (Gemini Lv1~Lv5). "
        "Default: for KR/US, pick latest date and analyze all pending news rows."
    )

    def add_arguments(self, parser):
        parser.add_argument("--scope", type=str, default="", help="KR or US (default: both)")
        parser.add_argument("--date", type=str, default="", help="YYYY-MM-DD (default: latest per scope)")
        parser.add_argument("--latest", action="store_true", help="Use latest available date for the scope (default)")
        parser.add_argument("--force", action="store_true", help="Analyze even if already analyzed")
        parser.add_argument("--limit", type=int, default=2000, help="Max news rows per scope (default: 2000)")

    def _today_kst(self) -> date:
        kst = ZoneInfo("Asia/Seoul")
        return timezone.now().astimezone(kst).date()

    def _parse_date(self, s: str) -> Optional[date]:
        s = (s or "").strip()
        if not s:
            return None
        try:
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    def _resolve_latest_date(self, scope: str) -> date:
        latest = (
            TrendKeywordDaily.objects.filter(scope=scope)
            .order_by("-date")
            .values_list("date", flat=True)
            .first()
        )
        return latest or self._today_kst()

    def _resolve_target_date(self, scope: str, date_str: str, latest_flag: bool) -> date:
        # date가 들어오면 date 우선, 아니면 최신 날짜
        d = self._parse_date(date_str)
        if d:
            return d
        # latest_flag는 사실상 default 동작과 동일
        return self._resolve_latest_date(scope)

    def _run_for_scope(self, scope: str, target_date: date, force: bool, limit: int) -> tuple[int, int]:
        kw_qs = TrendKeywordDaily.objects.filter(date=target_date, scope=scope).order_by("rank")
        if not kw_qs.exists():
            self.stdout.write(self.style.WARNING(f"[{scope}] No TrendKeywordDaily found: date={target_date}"))
            return (0, 0)

        news_qs = (
            TrendKeywordNews.objects.filter(trend__in=kw_qs)
            .select_related("trend")
            .order_by("-created_at")
        )

        if not force:
            news_qs = news_qs.filter(Q(analysis_full__isnull=True) | Q(analyzed_at__isnull=True))

        news_list = list(news_qs[:limit])
        self.stdout.write(f"[{scope}] date={target_date} target_news={len(news_list)} force={force} limit={limit}")

        ok = 0
        fail = 0
        total = len(news_list)

        for i, n in enumerate(news_list, start=1):
            try:
                res = analyze_trend_keyword_news(news=n, save_to_db=True)
                if res:
                    ok += 1
                    self.stdout.write(f"[{scope}] [{i}/{total}] OK   id={n.id}  title={n.title[:80]}")
                else:
                    fail += 1
                    self.stdout.write(f"[{scope}] [{i}/{total}] FAIL id={n.id} empty-result")
            except Exception as e:
                fail += 1
                self.stdout.write(f"[{scope}] [{i}/{total}] ERROR id={n.id} {e}")

        return ok, fail

    def handle(self, *args, **opts):
        raw_scope = (opts.get("scope") or "").upper().strip()
        date_str = (opts.get("date") or "").strip()
        latest_flag = bool(opts.get("latest")) or (date_str == "")  # date 미지정이면 최신이 기본
        force = bool(opts.get("force"))
        limit = int(opts.get("limit") or 2000)
        limit = max(1, min(10000, limit))

        scopes: list[str]
        if raw_scope in (TrendScope.KR, TrendScope.US):
            scopes = [raw_scope]
        else:
            scopes = [TrendScope.KR, TrendScope.US]

        grand_ok = 0
        grand_fail = 0

        for scope in scopes:
            target_date = self._resolve_target_date(scope, date_str, latest_flag)
            ok, fail = self._run_for_scope(scope, target_date, force, limit)
            grand_ok += ok
            grand_fail += fail

        self.stdout.write(self.style.SUCCESS(f"Done. ok={grand_ok} fail={grand_fail} scopes={scopes}"))
