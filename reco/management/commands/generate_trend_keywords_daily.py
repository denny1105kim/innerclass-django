import json
from typing import Any

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from zoneinfo import ZoneInfo

from ...models import TrendKeywordDaily, ThemeScope
from main.services.gemini_client import get_gemini_client, ChatMessage


# =========================================================
# LLM SYSTEM PROMPT (scope별 KR/US/ALL 강제)
# =========================================================
TREND_JSON_INSTRUCTION = """
너는 주식/금융 뉴스 및 시장 담론에서 "오늘" 자주 언급되는 트렌드 키워드를 뽑는 엔진이다.
반드시 아래 JSON만 출력해라. 다른 텍스트/설명/마크다운/코드블록 금지.

출력 스키마:
{
  "items": [
    { "keyword": string, "reason": string }
  ]
}

공통 규칙:
- items 길이는 반드시 3
- 각 item의 keyword는 서로 달라야 한다
- keyword는 2~10자(한국어) 또는 2~5단어(영어) 정도로 간결하게
- reason은 1~2문장, 과장/수익보장 금지, 리스크/불확실성 1개 포함
- 너무 포괄적인 단어(예: "주식", "경제", "시장") 금지. 구체 키워드로.

Scope 규칙:
- scope=KR: items 3개 모두 한국 시장 담론 중심 키워드
- scope=US: items 3개 모두 미국 시장 담론 중심 키워드
- scope=ALL: items 3개 중 한국 2개 + 미국 1개 키워드로 구성
""".strip()


# =========================================================
# Utils
# =========================================================
def _safe_json_load(s: str) -> dict:
    s = (s or "").strip()
    l = s.find("{")
    r = s.rfind("}")
    if l >= 0 and r >= 0 and r > l:
        s = s[l : r + 1]
    return json.loads(s)


def _sanitize_item(x: Any) -> dict:
    if not isinstance(x, dict):
        return {"keyword": "", "reason": ""}
    return {
        "keyword": str(x.get("keyword") or "")[:80],
        "reason": str(x.get("reason") or "")[:2000],
    }


def _build_user_msg(scope: str) -> str:
    scope = (scope or "").strip().upper()

    if scope == ThemeScope.KR:
        market_txt = "국내(KR)"
        extra = "키워드 3개 모두 국내 시장/국내 기업/국내 정책·이슈 중심으로 선정해라."
    elif scope == ThemeScope.US:
        market_txt = "해외(US)"
        extra = "키워드 3개 모두 미국 시장/미국 기업/미국 정책·이슈 중심으로 선정해라."
    else:
        market_txt = "국내(KR)+해외(US) 혼합"
        extra = "키워드 3개 중 국내 2개, 해외(미국) 1개로 구성해라."

    return f"""
오늘 기준으로 {market_txt}에서
지금 가장 자주 언급되는 트렌드 키워드 3개를 선정하고,
{extra}
""".strip()


def _write_scope_rows(today, scope: str, items: list[dict]) -> int:
    """
    date+scope+rank unique 구조로 scope별 1..3 덮어쓰기 저장.
    """
    with transaction.atomic():
        TrendKeywordDaily.objects.filter(date=today, scope=scope).delete()
        objs = [
            TrendKeywordDaily(
                date=today,
                scope=scope,
                rank=i,
                keyword=it["keyword"],
                reason=it["reason"],
            )
            for i, it in enumerate(items, start=1)
        ]
        TrendKeywordDaily.objects.bulk_create(objs)
    return len(items)


# =========================================================
# Management Command
# =========================================================
class Command(BaseCommand):
    help = "Generate today's trend keywords for ALL/KR/US (each scope 3 keywords)."

    def handle(self, *args, **opts):
        LIMIT = 3
        SCOPES = [ThemeScope.ALL, ThemeScope.KR, ThemeScope.US]

        # ===== KST 기준 날짜 =====
        kst = ZoneInfo("Asia/Seoul")
        today = timezone.now().astimezone(kst).date()

        client = get_gemini_client()

        for scope in SCOPES:
            user_msg = _build_user_msg(scope)
            msgs = [
                ChatMessage(role="system", content=TREND_JSON_INSTRUCTION),
                ChatMessage(role="user", content=user_msg),
            ]

            raw = client.chat(msgs)

            try:
                data = _safe_json_load(raw)
                items = (data.get("items") or [])[:LIMIT]
                items = [_sanitize_item(x) for x in items]

                # 항상 3개 저장 (프론트/쿼리 안정성)
                while len(items) < LIMIT:
                    items.append({"keyword": "", "reason": ""})
            except Exception:
                items = [{"keyword": "", "reason": ""} for _ in range(LIMIT)]

            saved = _write_scope_rows(today, scope, items)
            self.stdout.write(self.style.SUCCESS(f"[{today}] scope={scope} saved={saved}"))
