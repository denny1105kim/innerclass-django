# apps/chatbot/views.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import timedelta

import requests
from django.conf import settings
from django.core.paginator import EmptyPage, Paginator
from django.utils import timezone
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response

from main.services.gemini_client import ChatMessage as LlmMessage, get_gemini_client

from .models import (
    PromptTemplate,
    ChatSession,
    ChatMessage as ChatLog,
)


# -----------------------------
# Chat persistence settings
# -----------------------------
CHAT_RETENTION_DAYS = 3
CHAT_MAX_MESSAGE_CHARS = 2000
CHAT_CONTEXT_MESSAGES = 30
CHAT_SESSION_PAGE_THRESHOLD = 100
CHAT_PAGE_SIZE_DEFAULT = 50
CHAT_PAGE_SIZE_MAX = 100


def _chat_cleanup_retention() -> None:
    cutoff = timezone.now() - timedelta(days=CHAT_RETENTION_DAYS)
    ChatLog.objects.filter(created_at__lt=cutoff).delete()
    ChatSession.objects.filter(messages__isnull=True).delete()


def _make_session_title(first_user_message: str, max_len: int = 28) -> str:
    t = " ".join((first_user_message or "").strip().split())
    if not t:
        return "새 대화"
    return t if len(t) <= max_len else (t[: max_len - 1] + "…")


def _serialize_session(s: ChatSession) -> Dict[str, Any]:
    return {
        "id": s.id,
        "title": s.title,
        "template_id": s.template_id,
        "updated_at": s.updated_at.isoformat(),
        "created_at": s.created_at.isoformat(),
    }


def _serialize_chatlog(m: ChatLog) -> Dict[str, Any]:
    return {
        "id": m.id,
        "role": m.role,
        "content": m.content,
        "created_at": m.created_at.isoformat(),
    }


# -----------------------------
# Prompt helpers
# -----------------------------
FALLBACK_DOMAIN_GUARDRAILS = """
당신은 금융/주식 도메인의 응답을 생성하는 어시스턴트다.

공통 규칙:
- 수익 보장/확실/무조건 같은 단정적 수익 약속 금지.
- 근거 없는 루머/미확인 사실 단정 금지. 사실/추정/의견을 구분.
- 장점만 나열하지 말고 리스크(하락 요인) 1~2개를 반드시 포함.
- 에둘러 말하지 말 것. 질문이 "추천"이면 종목을 직접 제시할 것.
""".strip()


def _get_default_template() -> Optional[PromptTemplate]:
    return PromptTemplate.objects.filter(is_active=True).order_by("-updated_at", "-id").first()


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


def _clamp_level(level: Any) -> int:
    try:
        level = int(level)
    except Exception:
        return 3
    return max(1, min(5, level))


def _level_system_instruction(level: int) -> str:
    level = _clamp_level(level)

    if level == 1:
        return """
[System Instruction - Level 1 (주린이/입문자)]
- 친절한 해요체, 쉬운 비유.
- 결론 먼저.
- 마지막 문장은 행동지침 1줄.
""".strip()

    if level == 2:
        return """
[System Instruction - Level 2 (초보자/기초)]
- 편안한 해요체.
- 결론 먼저, 3~5 불릿.
""".strip()

    if level == 3:
        return """
[System Instruction - Level 3 (일반/중급)]
- 합쇼체, 팩트 중심.
- [추천] 먼저, [근거], [체크포인트/리스크]
""".strip()

    if level == 4:
        return """
[System Instruction - Level 4 (숙련자)]
- 하십시오체.
- Summary / Picks / Rationale / Risk
""".strip()

    return """
[System Instruction - Level 5 (전문가)]
- 개조식.
- Picks / Thesis / Triggers / Risk / Action
""".strip()


def _risk_overrides(risk: str) -> str:
    if risk == "A":
        return "[Risk Overlay - 공격형] 성장/모멘텀 관점. 수익보장 금지."
    if risk == "C":
        return "[Risk Overlay - 안정형] 방어/현금흐름 관점. 수익보장 금지."
    return "[Risk Overlay - 중립형] 분산/균형 관점. 수익보장 금지."


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
- 포트폴리오가 있으면 최소 1개는 연결 언급 + 체크포인트(실적/이슈/수급) 1~2개.
""".strip()

    return "\n".join(
        [
            "[User Profile]",
            f"- Asset Type: {asset_type}",
            f"- Interested Sectors: {sectors_csv}",
            f"- Risk Profile: {_risk_profile_text(risk)}",
            f"- Knowledge Level: Level {level}",
            f"- Portfolio: {portfolio_csv}",
            "",
            portfolio_rule,
        ]
    ).strip()


def _try_get_profile_via_model(request: Request) -> Optional[Dict[str, Any]]:
    try:
        from accounts.models import UserProfile  # type: ignore
    except Exception:
        return None

    profile = UserProfile.objects.filter(user=request.user).first()
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
    return d if d else _try_get_profile_via_http(request)


def _is_recommendation_intent(message: str) -> bool:
    m = (message or "").strip().lower()
    keys = ["추천", "추천주", "종목 추천", "오늘 추천", "오늘의 추천", "top pick", "pick", "매수", "사볼", "담을"]
    return any(k.lower() in m for k in keys)


def _recommendation_policy(level: int) -> str:
    level = _clamp_level(level)
    if level <= 2:
        return "[Recommendation Policy] 추천이면 종목 2~3개 먼저 + 이유/체크포인트 + 리스크 1줄."
    if level == 3:
        return "[Recommendation Policy] [추천] 먼저, 종목별 근거/체크포인트/리스크 포함."
    if level == 4:
        return "[Recommendation Policy] Picks 먼저, Rationale, Risk/Invalidation."
    return "[Recommendation Policy] Picks/Thesis/Triggers/Risk/Action."


# -----------------------------
# Endpoints
# -----------------------------
@api_view(["GET"])
@permission_classes([AllowAny])
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


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def chatbot_sessions(request: Request):
    _chat_cleanup_retention()

    try:
        limit = int(request.query_params.get("limit", 20))
    except Exception:
        limit = 20
    limit = max(1, min(50, limit))

    qs = ChatSession.objects.filter(user=request.user).order_by("-updated_at", "-id")[:limit]
    return Response({"sessions": [_serialize_session(s) for s in qs]})


@api_view(["GET", "DELETE"])
@permission_classes([IsAuthenticated])
def chatbot_session_detail(request: Request, session_id: int):
    _chat_cleanup_retention()

    try:
        session = ChatSession.objects.get(id=session_id, user=request.user)
    except ChatSession.DoesNotExist:
        return Response({"detail": "Session not found"}, status=404)

    if request.method == "DELETE":
        session.delete()
        return Response({"ok": True})

    try:
        page = int(request.query_params.get("page", 1))
    except Exception:
        page = 1
    try:
        page_size = int(request.query_params.get("page_size", CHAT_PAGE_SIZE_DEFAULT))
    except Exception:
        page_size = CHAT_PAGE_SIZE_DEFAULT

    page = max(1, page)
    page_size = max(1, min(CHAT_PAGE_SIZE_MAX, page_size))

    base_qs = ChatLog.objects.filter(session=session).order_by("-created_at", "-id")
    total = base_qs.count()
    paginator = Paginator(base_qs, page_size)

    try:
        page_obj = paginator.page(page)
    except EmptyPage:
        return Response(
            {
                "session": _serialize_session(session),
                "messages": [],
                "page": page,
                "page_size": page_size,
                "total": total,
                "has_next": False,
            }
        )

    return Response(
        {
            "session": _serialize_session(session),
            "messages": [_serialize_chatlog(m) for m in page_obj.object_list],
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_next": page_obj.has_next(),
            "pagination_recommended": total > CHAT_SESSION_PAGE_THRESHOLD,
        }
    )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def chatbot_chat(request: Request):
    _chat_cleanup_retention()

    template_id = request.data.get("template_id")
    template_key = request.data.get("template_key")
    session_id = request.data.get("session_id")
    message = (request.data.get("message") or "").strip()

    if not message:
        return Response({"detail": "message is required"}, status=400)
    if len(message) > CHAT_MAX_MESSAGE_CHARS:
        return Response({"detail": f"message is too long (max {CHAT_MAX_MESSAGE_CHARS})"}, status=400)

    # template resolve
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

    base_system = (template.system_prompt or "").strip() if template else ""
    user_prompt_template = template.user_prompt_template if template else "{message}"
    if not base_system:
        base_system = FALLBACK_DOMAIN_GUARDRAILS

    # session resolve/create
    if session_id:
        try:
            session = ChatSession.objects.get(id=int(session_id), user=request.user)
        except Exception:
            return Response({"detail": "Invalid session_id"}, status=400)
    else:
        session = ChatSession.objects.create(user=request.user, template=template, title="")

    # profile
    profile_data = _get_user_profile_data(request)
    risk = ""
    level = 3
    user_context = ""
    if profile_data:
        risk = (profile_data.get("riskProfile") or profile_data.get("risk_profile") or "").strip()
        level = _clamp_level(profile_data.get("knowledgeLevel") or profile_data.get("knowledge_level") or 3)
        user_context = _build_user_context_from_payload(profile_data)

    level_inst = _level_system_instruction(level)
    risk_inst = _risk_overrides(risk) if risk else ""
    rec_inst = _recommendation_policy(level) if _is_recommendation_intent(message) else ""

    system_prompt = "\n\n".join(
        [p for p in [base_system, level_inst, risk_inst, rec_inst, user_context] if p.strip()]
    ).strip()

    try:
        user_content = (user_prompt_template or "{message}").format(message=message)
    except Exception:
        user_content = message

    # persist user
    ChatLog.objects.create(session=session, role="user", content=user_content)

    if not (session.title or "").strip():
        session.title = _make_session_title(message)
        session.updated_at = timezone.now()
        session.save(update_fields=["title", "updated_at"])

    # history
    recent_logs = list(
        ChatLog.objects.filter(session=session).order_by("-created_at", "-id")[:CHAT_CONTEXT_MESSAGES]
    )[::-1]

    llm_msgs: List[LlmMessage] = []
    if system_prompt:
        llm_msgs.append(LlmMessage(role="system", content=system_prompt))
    for log in recent_logs:
        if log.role in ("user", "assistant") and log.content:
            llm_msgs.append(LlmMessage(role=log.role, content=log.content))

    client = get_gemini_client()
    try:
        answer = client.chat(llm_msgs)
    except Exception as e:
        return Response({"detail": f"Chat failed: {str(e)}"}, status=502)

    ChatLog.objects.create(
        session=session,
        role="assistant",
        content=answer[: CHAT_MAX_MESSAGE_CHARS * 5],
    )

    ChatSession.objects.filter(id=session.id).update(
        updated_at=timezone.now(),
        template_id=(template.id if template else None),
    )

    resp: Dict[str, Any] = {
        "answer": answer,
        "session_id": session.id,
        "template": {"id": template.id, "key": template.key} if template else {"id": None, "key": "fallback"},
        "profile_loaded": bool(profile_data),
        "applied_level": level,
        "applied_risk": risk or None,
        "recommendation_mode": bool(rec_inst),
    }
    return Response(resp)
