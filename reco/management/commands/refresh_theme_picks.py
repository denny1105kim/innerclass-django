import json
from dataclasses import dataclass
from typing import Any

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from zoneinfo import ZoneInfo

from ...models import ThemePickDaily, ThemeScope
from main.services.gemini_client import get_gemini_client, ChatMessage

THEME_JSON_INSTRUCTION = """
너는 주식 테마 기반 종목 추천 엔진이다.
반드시 아래 JSON만 출력해라. 다른 텍스트/설명/마크다운/코드블록 금지.

출력 스키마:
{
  "items": [
    { "theme": string, "symbol": string, "name": string, "reason": string }
  ]
}

규칙:
- items 길이는 요청된 limit과 동일 (기본 3)
- reason은 1~2문장, 과장/수익보장 금지, 리스크 1개 포함
- KR이면 한국 종목(티커 또는 종목코드), US이면 미국 티커
""".strip()


def _safe_json_load(s: str) -> dict:
    s = (s or "").strip()
    l = s.find("{")
    r = s.rfind("}")
    if l >= 0 and r >= 0 and r > l:
        s = s[l : r + 1]
    return json.loads(s)


@dataclass
class LlmMsg:
    role: str
    content: str


def _build_user_msg(scope: str, limit: int) -> str:
    market_txt = "국내(KR)와 해외(US) 혼합" if scope == "ALL" else ("국내(KR)" if scope == "KR" else "해외(US)")
    return f"""
오늘 기준으로 {market_txt}에서 시장에서 자주 언급되는 테마 1~3개를 선정하고,
각 테마에서 대표 종목을 {limit}개 추천해라.
""".strip()


def _sanitize_item(x: Any) -> dict:
    # 파싱된 JSON이 조금 이상해도 안전하게 정리
    if not isinstance(x, dict):
        return {"theme": "", "symbol": "", "name": "", "reason": ""}
    return {
        "theme": str(x.get("theme") or "")[:80],
        "symbol": str(x.get("symbol") or "")[:32],
        "name": str(x.get("name") or "")[:128],
        "reason": str(x.get("reason") or "")[:2000],
    }


class Command(BaseCommand):
    help = "Refresh daily theme picks into DB (overwrites by date/scope)"

    def add_arguments(self, parser):
        parser.add_argument("--scope", type=str, default="ALL", choices=["ALL", "KR", "US", "ALL,KR,US"])
        parser.add_argument("--limit", type=int, default=3)
        parser.add_argument("--kst_hour", type=int, default=7, help="(optional) just info; scheduling is external")

    def handle(self, *args, **opts):
        scope_opt = (opts["scope"] or "ALL").strip().upper()
        limit = int(opts["limit"] or 3)
        limit = max(1, min(5, limit))

        scopes = ["ALL", "KR", "US"] if scope_opt in ("ALL,KR,US", "ALLKRUS") else [scope_opt]

        # ===== KST 기준 날짜 =====
        kst = ZoneInfo("Asia/Seoul")
        today = timezone.now().astimezone(kst).date()

        # ===== 너 프로젝트의 LLM client 불러오기 =====
        # TODO: import path만 너 프로젝트 구조에 맞춰 수정

        client = get_gemini_client()

        for scope in scopes:
            user_msg = _build_user_msg(scope, limit)
            msgs = [
                ChatMessage(role="system", content=THEME_JSON_INSTRUCTION),
                ChatMessage(role="user", content=user_msg),
            ]

            raw = client.chat(msgs)

            try:
                data = _safe_json_load(raw)
                items = (data.get("items") or [])[:limit]
                items = [_sanitize_item(x) for x in items]
            except Exception:
                items = []

            # ===== 덮어쓰기 =====
            with transaction.atomic():
                ThemePickDaily.objects.filter(date=today, scope=scope).delete()

                objs = []
                for i, it in enumerate(items, start=1):
                    objs.append(
                        ThemePickDaily(
                            date=today,
                            scope=scope,
                            rank=i,
                            theme=it["theme"],
                            symbol=it["symbol"],
                            name=it["name"],
                            reason=it["reason"],
                        )
                    )
                ThemePickDaily.objects.bulk_create(objs)

            self.stdout.write(self.style.SUCCESS(f"[{today}] scope={scope} saved={len(items)}"))
