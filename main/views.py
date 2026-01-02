from __future__ import annotations

from datetime import date as _date
from typing import Any, Dict, List, Optional

import requests
from django.conf import settings
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response

from .models import DailyStockSnapshot, Market, PromptTemplate
from .services.gemini_client import ChatMessage, get_gemini_client


# -----------------------------
# Markets
# -----------------------------

def _serialize_snapshot(s: DailyStockSnapshot) -> dict:
    return {
        "symbol": s.stock.symbol,
        "name": s.stock.name,
        "exchange": s.stock.exchange,
        "currency": s.stock.currency,
        "open": float(s.open) if s.open is not None else None,
        "close": float(s.close) if s.close is not None else None,
        "intraday_pct": float(s.intraday_pct) if s.intraday_pct is not None else None,
        "change_pct": float(s.change_pct) if s.change_pct is not None else None,
        "market_cap": s.market_cap,
        "volume": s.volume,
        "date": s.date.isoformat(),
    }


@api_view(["GET"])
def today_market(request: Request):
    market = request.query_params.get("market", "KR").upper()
    if market not in (Market.KR, Market.US):
        return Response({"detail": "market must be KR or US"}, status=400)

    date_str = request.query_params.get("date")
    if date_str:
        try:
            y, m, d = map(int, date_str.split("-"))
            target = _date(y, m, d)
        except Exception:
            return Response({"detail": "date must be YYYY-MM-DD"}, status=400)
    else:
        target = timezone.localdate()

    asof = (
        DailyStockSnapshot.objects
        .filter(stock__market=market, date__lte=target)
        .order_by("-date")
        .values_list("date", flat=True)
        .first()
    )

    if not asof:
        return Response({
            "market": market,
            "asof": target.isoformat(),
            "top_market_cap": [],
            "top_drawdown": [],
        })

    base_qs = (
        DailyStockSnapshot.objects
        .select_related("stock")
        .filter(stock__market=market, date=asof)
    )

    top_market_cap = base_qs.exclude(market_cap__isnull=True).order_by("-market_cap")[:5]
    top_drawdown = base_qs.exclude(intraday_pct__isnull=True).order_by("intraday_pct")[:5]

    return Response({
        "market": market,
        "asof": asof.isoformat(),
        "top_market_cap": [_serialize_snapshot(x) for x in top_market_cap],
        "top_drawdown": [_serialize_snapshot(x) for x in top_drawdown],
    })


# -----------------------------
# Chatbot - Prompt helpers
# -----------------------------

# 금융/주식 도메인 안전 규칙(출력에 "투자 권유 아님" 문구 강제 X)
# - 대신 과도한 확신/수익보장/루머단정 금지 유지
# - "추천" 요청이면 반드시 종목을 직접 제시하도록 별도 Recommendation Policy로 강제
FALLBACK_DOMAIN_GUARDRAILS = """
당신은 금융/주식 도메인의 응답을 생성하는 어시스턴트다.

공통 규칙:
- 수익 보장/확실/무조건 같은 단정적 수익 약속 금지.
- 근거 없는 루머/미확인 사실 단정 금지. 사실/추정/의견을 구분.
- 장점만 나열하지 말고 리스크(하락 요인) 1~2개를 반드시 포함.
- 에둘러 말하지 말 것. 질문이 "추천"이면 종목을 바로 제시할 것.
""".strip()


def _get_default_template() -> Optional[PromptTemplate]:
    return (
        PromptTemplate.objects
        .filter(is_active=True)
        .order_by("-updated_at", "-id")
        .first()
    )


def _safe_history(raw: Any) -> List[ChatMessage]:
    msgs: List[ChatMessage] = []
    if not isinstance(raw, list):
        return msgs

    for item in raw[-20:]:
        if not isinstance(item, dict):
            continue
        role = (item.get("role") or "").strip()
        content = (item.get("content") or "").strip()
        if role in ("user", "assistant") and content:
            msgs.append(ChatMessage(role=role, content=content))
    return msgs


def _risk_profile_text(code: str) -> str:
    return {
        "A": "공격형(고위험·고수익 선호, 성장/모멘텀 중심, 손절/변동성 관리 중요)",
        "B": "중립형(시장수익률 지향, 분산/우량/ETF 중심)",
        "C": "안정형(원금보존/변동성 최소화, 배당/방어 중심)",
    }.get(code or "", "미지정")


def _normalize_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",")]
        return [p for p in parts if p]
    return []


def _clamp_level(level: int) -> int:
    try:
        level = int(level)
    except Exception:
        return 3
    if level < 1:
        return 1
    if level > 5:
        return 5
    return level


def _level_system_instruction(level: int) -> str:
    """
    Level 1~5 페르소나/형식. 단, "추천 요청 시 추천을 해야 한다"는 정책을
    별도 Recommendation Policy에서 강제한다.
    """
    level = _clamp_level(level)

    if level == 1:
        return """
[System Instruction - Level 1 (주린이/입문자)]
Role: 주식 투자를 처음 시작한 입문자를 돕는 튜터.

Tone & Manner:
- 친절한 해요체 사용.
- 문장마다 이모지 필수.
- 어려운 개념은 일상 비유로 설명.

Constraints:
- 전문 용어/약어 남발 금지(필요 시 아주 쉬운 말로 즉시 풀어쓰기).
- 에둘러 말하지 말 것: 질문이 요구하는 결과를 먼저 제시.
- 마지막 문장은 행동지침 1줄로 끝낼 것(예: 오늘 체크할 것/조심할 것).

Response Format:
- 4~8문장.
- 첫 1~2문장에서 결론(추천 종목/답)을 먼저 제시.
""".strip()

    if level == 2:
        return """
[System Instruction - Level 2 (초보자/기초)]
Role: 초보 투자자를 돕는 멘토.

Tone & Manner:
- 편안한 해요체.
- 핵심 용어는 1줄 쉬운 풀이를 곁들임.

Constraints:
- 에둘러 말하지 말고 결론부터.
- 복잡한 수치 나열 지양.

Response Format:
- 3~5개 불릿:
  1) 결론(추천/핵심)
  2) 이유(1~2개)
  3) 체크 포인트(1~2개)
  4) 리스크(1개)
""".strip()

    if level == 3:
        return """
[System Instruction - Level 3 (일반인/중급 - Default)]
Role: 객관적이고 명확한 경제 뉴스 앵커.

Tone & Manner:
- 합쇼체 사용, 팩트 중심.
- 용어는 괄호로 짧게 설명.

Constraints:
- 에둘러 말하지 말 것: 요구한 출력(추천/목록/요약)을 먼저 제시.
- 추천이 요청되면 종목을 명시하고 근거/리스크를 붙일 것.

Response Format:
- [추천] (종목 2~3개를 먼저 제시)
- [근거] 2~4문장
- [체크포인트/리스크] 3~5문장
""".strip()

    if level == 4:
        return """
[System Instruction - Level 4 (숙련자/고급)]
Role: 증권사 PB.

Tone & Manner:
- 하십시오체.
- 데이터 기반(%, bp 등) 언급 허용.

Constraints:
- 추천 요청이면 종목을 먼저 제시하고, 밸류/펀더멘털/테크니컬/리스크를 분리.
- 에둘러 말하지 말 것.

Response Format:
- Summary
- Picks(2~3)
- Rationale
- Risk/Invalidation
""".strip()

    return """
[System Instruction - Level 5 (전문가/Professional)]
Role: 헤지펀드 애널리스트.

Tone & Manner:
- 개조식, 건조체.

Constraints:
- 추천 요청이면 티커/종목을 먼저 제시.
- 에둘러 말하지 말 것.

Response Format:
- Picks(2~3)
- Thesis
- Triggers
- Risk
- Action Items
""".strip()


def _risk_overrides(risk: str) -> str:
    if risk == "A":
        return """
[Risk Overlay - 공격형]
- 공격형에 맞춰 성장/모멘텀/변동성 감수 관점으로 종목을 고를 것.
- 단, 수익 보장 표현은 금지.
""".strip()
    if risk == "C":
        return """
[Risk Overlay - 안정형]
- 방어/현금흐름/배당/저변동 관점으로 종목을 고를 것.
- 단, 수익 보장 표현은 금지.
""".strip()
    return """
[Risk Overlay - 중립형]
- 분산/우량/리스크-리턴 균형 관점으로 종목을 고를 것.
- 단, 수익 보장 표현은 금지.
""".strip()


def _build_user_context_from_payload(profile_data: Dict[str, Any]) -> str:
    asset_type = profile_data.get("assetType") or profile_data.get("asset_type") or "미지정"
    sectors_list = _normalize_list(profile_data.get("sectors"))
    portfolio_list = _normalize_list(profile_data.get("portfolio"))
    risk = (profile_data.get("riskProfile") or profile_data.get("risk_profile") or "").strip()
    level = _clamp_level(profile_data.get("knowledgeLevel") or profile_data.get("knowledge_level") or 3)

    sectors_csv = ", ".join(sectors_list) if sectors_list else "None"
    portfolio_csv = ", ".join(portfolio_list) if portfolio_list else "Empty"

    portfolio_rule = """
[Portfolio Rule]
- 포트폴리오 종목이 존재하면 질문과 무관하더라도 최소 1개 종목을 연결해 언급하고,
  체크 포인트(실적/이슈/수급) 1~2개를 포함할 것.
""".strip()

    return "\n".join([
        "[User Profile]",
        f"- Asset Type: {asset_type}",
        f"- Interested Sectors: {sectors_csv}",
        f"- Risk Profile: {_risk_profile_text(risk)}",
        f"- Knowledge Level: Level {level}",
        f"- Portfolio: {portfolio_csv}",
        "",
        portfolio_rule,
    ]).strip()


def _try_get_profile_via_model(request: Request) -> Optional[Dict[str, Any]]:
    try:
        from auth_app.models import UserProfile  # type: ignore
    except Exception:
        return None

    profile = None
    try:
        profile = UserProfile.objects.filter(user=request.user).first()
    except Exception:
        for field in ("owner", "account", "member"):
            try:
                profile = UserProfile.objects.filter(**{field: request.user}).first()
                if profile:
                    break
            except Exception:
                continue

    if not profile:
        return None

    return {
        "assetType": getattr(profile, "assetType", None) or getattr(profile, "asset_type", None),
        "sectors": getattr(profile, "sectors", None),
        "portfolio": getattr(profile, "portfolio", None),
        "riskProfile": getattr(profile, "riskProfile", None) or getattr(profile, "risk_profile", None),
        "knowledgeLevel": getattr(profile, "knowledgeLevel", None) or getattr(profile, "knowledge_level", None),
    }


def _try_get_profile_via_http(request: Request) -> Optional[Dict[str, Any]]:
    auth_header = request.headers.get("Authorization") or request.META.get("HTTP_AUTHORIZATION")
    if not auth_header:
        return None

    base_url = getattr(settings, "INTERNAL_API_BASE_URL", "http://127.0.0.1:8000")
    url = f"{base_url}/api/user/onboarding/"

    try:
        res = requests.get(url, headers={"Authorization": auth_header}, timeout=5)
        if res.status_code != 200:
            return None
        data = res.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _get_user_profile_data(request: Request) -> Optional[Dict[str, Any]]:
    d = _try_get_profile_via_model(request)
    if d:
        return d
    return _try_get_profile_via_http(request)


def _is_recommendation_intent(message: str) -> bool:
    """
    추천/픽/살만한/사야할/매수할/탑픽 등 '추천' 의도를 감지.
    """
    m = (message or "").strip().lower()
    keys = [
        "추천", "추천주", "종목 추천", "오늘 추천", "오늘의 추천",
        "top pick", "pick", "picks", "살만", "사야", "매수", "사면", "사볼", "담을"
    ]
    return any(k.lower() in m for k in keys)


def _recommendation_policy(level: int) -> str:
    """
    추천 요청 시: 무조건 종목을 명시하게 만드는 강제 정책.
    - "에둘러 말하지 말라"를 시스템에서 강제
    - 출력 포맷을 고정해 모델이 우회하지 못하게 함
    """
    level = _clamp_level(level)

    # 레벨별로 말투만 다르고, '추천을 반드시 주기'는 공통으로 강제
    if level == 1:
        return """
[Recommendation Policy]
- 사용자가 추천을 요구하면, 첫 문장에서 국내 주식 종목 2~3개를 '오늘의 추천'으로 바로 제시할 것. 😲
- 에둘러 말하지 말고, 종목명부터 말할 것. 👀
- 각 종목마다 이유 1줄 + 오늘 체크할 포인트 1줄을 붙일 것. 📌
- 마지막 문장은 행동지침 1줄로 끝낼 것. 🧭
""".strip()

    if level == 2:
        return """
[Recommendation Policy]
- 추천 요청이면 종목 2~3개를 먼저 제시.
- 각 종목: 이유 1줄 + 체크포인트 1줄.
- 마지막에 리스크 1줄 추가.
- 에둘러 말하지 말 것.
""".strip()

    if level == 3:
        return """
[Recommendation Policy]
- 추천 요청이면 [추천] 섹션에 종목 2~3개를 먼저 제시.
- 각 종목별로 (근거 1~2문장 + 체크포인트 1~2개 + 리스크 1개) 포함.
- 우회 금지: "추천은 못 한다" 같은 문장 금지.
""".strip()

    if level == 4:
        return """
[Recommendation Policy]
- 추천 요청이면 Picks(2~3)를 먼저 제시.
- 각 Pick에 대해 Rationale(1~2), Risk/Invalidation(1) 포함.
- 우회 금지.
""".strip()

    return """
[Recommendation Policy]
- Picks(2~3) 먼저.
- Thesis/Triggers/Risk/Action을 개조식으로.
- 우회 금지.
""".strip()


# -----------------------------
# Chatbot endpoints
# -----------------------------

@api_view(["GET"])
def chatbot_prompts(request: Request):
    qs = PromptTemplate.objects.filter(is_active=True).order_by("-updated_at", "name")
    templates = [
        {
            "id": t.id,
            "key": t.key,
            "name": t.name,
            "description": t.description,
            "updated_at": t.updated_at.isoformat(),
        }
        for t in qs
    ]
    return Response({"templates": templates})


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def chatbot_chat(request: Request):
    template_id = request.data.get("template_id")
    template_key = request.data.get("template_key")
    message = (request.data.get("message") or "").strip()
    history = request.data.get("history") or []

    if not message:
        return Response({"detail": "message is required"}, status=400)

    # 1) 템플릿 선택
    template: Optional[PromptTemplate] = None
    if template_id:
        try:
            template = PromptTemplate.objects.get(id=int(template_id), is_active=True)
        except Exception:
            return Response({"detail": "Invalid template_id"}, status=400)
    elif template_key:
        template = PromptTemplate.objects.filter(key=template_key, is_active=True).first()
        if template is None:
            return Response({"detail": "Invalid template_key"}, status=400)
    else:
        template = _get_default_template()

    # 2) base system / user prompt template
    base_system = ""
    user_prompt_template = "{message}"

    if template is not None:
        base_system = (template.system_prompt or "").strip()
        user_prompt_template = template.user_prompt_template or "{message}"

    if not base_system:
        base_system = FALLBACK_DOMAIN_GUARDRAILS

    # 3) 유저 프로필(온보딩) 조회
    profile_data = _get_user_profile_data(request)
    user_context = ""
    risk = ""
    level = 3
    if profile_data:
        user_context = _build_user_context_from_payload(profile_data)
        risk = (profile_data.get("riskProfile") or profile_data.get("risk_profile") or "").strip()
        level = _clamp_level(profile_data.get("knowledgeLevel") or profile_data.get("knowledge_level") or 3)

    # 4) Level + Risk
    level_inst = _level_system_instruction(level)
    risk_inst = _risk_overrides(risk) if risk else ""

    # 5) 추천 의도 감지 시: 추천 정책을 추가로 강제
    rec_inst = _recommendation_policy(level) if _is_recommendation_intent(message) else ""

    # 6) system prompt 조립
    # 우선순위: 도메인 규칙 → 레벨 페르소나/형식 → 리스크 → (추천정책) → 유저 컨텍스트
    system_parts: List[str] = [
        base_system,
        level_inst,
        risk_inst,
        rec_inst,
        user_context,
    ]
    system_prompt = "\n\n".join([p for p in system_parts if p.strip()]).strip()

    # 7) user prompt format
    try:
        user_content = user_prompt_template.format(message=message)
    except Exception:
        user_content = message

    msgs: List[ChatMessage] = []
    if system_prompt:
        msgs.append(ChatMessage(role="system", content=system_prompt))

    msgs.extend(_safe_history(history))
    msgs.append(ChatMessage(role="user", content=user_content))

    client = get_gemini_client()
    try:
        answer = client.chat(msgs)
    except Exception as e:
        return Response({"detail": f"Chat failed: {str(e)}"}, status=502)

    resp: Dict[str, Any] = {"answer": answer}
    if template is not None:
        resp["template"] = {"id": template.id, "key": template.key}
    else:
        resp["template"] = {"id": None, "key": "fallback"}
    resp["profile_loaded"] = bool(profile_data)
    resp["applied_level"] = level
    resp["applied_risk"] = risk or None
    resp["recommendation_mode"] = bool(rec_inst)
    return Response(resp)
