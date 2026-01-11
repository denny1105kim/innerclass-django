from __future__ import annotations

from django.contrib import admin
from django.db import models
from django.forms import Textarea

from .models import DailyRankingSnapshot


@admin.register(DailyRankingSnapshot)
class DailyRankingSnapshotAdmin(admin.ModelAdmin):
    """
    DailyRankingSnapshot Admin

    스냅샷 테이블은 row가 빠르게 증가하므로:
    - 목록 화면은 '자주 쓰는 컬럼' 위주로 가볍게
    - payload(JSON)는 상세 화면에서만 확인(기본 readonly)
    """

    # 리스트에서 한눈에 봐야 하는 것
    list_display = (
        "asof_date",
        "market",
        "ranking_type",
        "rank",
        "symbol_code",
        "name",
        "trade_price",
        "change_rate",
        "created_at",
    )

    # 필터링(운영 시 가장 많이 씀)
    list_filter = (
        "asof_date",
        "market",
        "ranking_type",
    )

    # 빠른 검색
    search_fields = (
        "symbol_code",
        "name",
    )

    # 최신 날짜 우선, 동일 날짜 내에서는 rank 오름차순
    ordering = ("-asof_date", "market", "ranking_type", "rank")

    # 날짜 네비게이션
    date_hierarchy = "asof_date"

    # 데이터가 많아질 때를 대비해 페이지 사이즈 제한
    list_per_page = 50

    # payload는 스냅샷 성격상 수정할 일이 거의 없으므로 read-only 추천
    readonly_fields = ("created_at", "payload")

    # JSON이 너무 길면 Admin에서 보기 힘드니 textarea로 크게
    formfield_overrides = {
        models.JSONField: {"widget": Textarea(attrs={"rows": 20, "cols": 120})},
    }

    # 선택적으로, 삭제 실수 방지: 스냅샷은 교체 저장하므로 개별 삭제를 막고 싶으면 사용
    # actions = None

    # 대량 데이터에서 count 쿼리 부담 줄이기(원하면 True)
    show_full_result_count = False
