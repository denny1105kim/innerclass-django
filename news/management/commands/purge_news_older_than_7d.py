from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from news.models import NewsArticle


class Command(BaseCommand):
    help = "Delete NewsArticle rows older than 7 days (based on published_at). No args."

    # 고정 보관 기간 (ARGS 없이 운영)
    RETENTION_DAYS = 7

    def handle(self, *args, **kwargs):
        now = timezone.now()
        cutoff = now - timedelta(days=self.RETENTION_DAYS)

        # published_at 기준 7일 초과 삭제
        qs = NewsArticle.objects.filter(published_at__lt=cutoff)

        candidates = qs.count()

        self.stdout.write("=========================================")
        self.stdout.write("🧹 뉴스 만료 데이터 정리 시작")
        self.stdout.write(f"- retention_days: {self.RETENTION_DAYS}")
        self.stdout.write(f"- now: {now.isoformat()}")
        self.stdout.write(f"- cutoff(published_at <): {cutoff.isoformat()}")
        self.stdout.write(f"- candidates: {candidates}")
        self.stdout.write("=========================================")

        if candidates == 0:
            self.stdout.write(self.style.SUCCESS("✅ 삭제 대상 없음"))
            return

        try:
            with transaction.atomic():
                deleted_count, deleted_detail = qs.delete()

            # deleted_count는 CASCADE 포함 총 삭제 수일 수 있음
            self.stdout.write(self.style.SUCCESS(f"✅ 삭제 완료: deleted_total={deleted_count}"))
            # 필요하면 상세도 출력 가능(너무 길어질 수 있어 기본은 비활성)
            # self.stdout.write(str(deleted_detail))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 삭제 실패: {e}"))
            raise
