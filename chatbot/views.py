from __future__ import annotations

import re
from datetime import timedelta
from typing import Any, Dict, List, Optional

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
    ChatMessage as ChatLog,
    ChatSession,
    PromptTemplate,
)

# =========================================================
# Chat persistence settings
# =========================================================
CHAT_RETENTION_DAYS = 3
CHAT_MAX_MESSAGE_CHARS = 2000
CHAT_CONTEXT_MESSAGES = 30

CHAT_SESSION_PAGE_THRESHOLD = 100
CHAT_PAGE_SIZE_DEFAULT = 50
CHAT_PAGE_SIZE_MAX = 100


# =========================================================
# Utilities
# =========================================================
def _join_nonempty(parts: List[str], sep: str = "\n\n") -> str:
    return sep.join([p.strip() for p in parts if (p or "").strip()]).strip()


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


# =========================================================
# Output sanitization (hard-stop for markdown like **, ###, ``` ...)
# =========================================================
_MARKDOWN_CODE_FENCE_RE = re.compile(r"```.*?```", flags=re.DOTALL)
_MARKDOWN_HEADING_RE = re.compile(r"(?m)^\s{0,3}#{1,6}\s+")
_MARKDOWN_BOLD_ITALIC_RE = re.compile(r"(\*\*|__)(.+?)(\*\*|__)", flags=re.DOTALL)
_MARKDOWN_INLINE_CODE_RE = re.compile(r"`([^`]+)`")
_MARKDOWN_LIST_BULLET_RE = re.compile(r"(?m)^\s*[\*\-]\s+(?=\S)")
_MARKDOWN_BLOCKQUOTE_RE = re.compile(r"(?m)^\s*>\s?")


def _sanitize_llm_answer(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s

    # 1) 코드펜스 제거 (필요 시 최소 1~2줄만 허용하는 정책이지만, 여기서는 안전하게 제거)
    s = _MARKDOWN_CODE_FENCE_RE.sub("", s).strip()

    # 2) 헤딩(#...) 제거: 라인 시작의 #만 제거하고 나머지는 남김
    s = _MARKDOWN_HEADING_RE.sub("", s)

    # 3) 볼드/강조(**, __) 제거: 감싸는 기호만 제거하고 내용은 남김
    #    예: **SK하이닉스 분석** -> SK하이닉스 분석
    while True:
        new_s = _MARKDOWN_BOLD_ITALIC_RE.sub(r"\2", s)
        if new_s == s:
            break
        s = new_s

    # 4) 인라인 코드(`...`) 제거: 백틱만 제거하고 내용은 남김
    s = _MARKDOWN_INLINE_CODE_RE.sub(r"\1", s)

    # 5) 마크다운 블록쿼트(>) 제거
    s = _MARKDOWN_BLOCKQUOTE_RE.sub("", s)

    # 6) 카드 포맷과 충돌할 수 있는 '*' 리스트를 '-'로 정규화
    s = _MARKDOWN_LIST_BULLET_RE.sub("- ", s)

    # 7) 남아있는 **, __ 같은 잔여 심볼도 최종적으로 제거 (요청: ** 절대 금지)
    s = s.replace("**", "")
    s = s.replace("__", "")

    # 8) 공백 정리
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{4,}", "\n\n\n", s).strip()

    return s


# =========================================================
# Prompt helpers (clean UI-like output + less noise)
# =========================================================

BANNED_MARKUP_RULES = """
출력 규칙(엄격)
- 다음은 절대 사용하지 마세요: "###", "#", "**", "__", "***", "```", "`", 마크다운 헤딩/제목/강조/코드펜스
- 굵게/기울임/밑줄 같은 서식 표현 금지 (별표/언더바로 감싸는 표현 전부 금지)
- 링크/코드는 정말 필요할 때만 1~2줄로 최소화하고, 가능하면 "텍스트로 설명"하세요.
- 위 규칙을 어기면 답변은 실패입니다. 규칙을 최우선으로 지키세요.
""".strip()

CARD_FORMAT_RULES = """
카드 출력 규칙(항상 적용)
- 문단/주제가 바뀌면 반드시 "새 카드"로 분리하세요.
- 카드 형식(반드시 이 형식 그대로):
  [카드명] 이모지 1개
  - 불릿 2~5개(짧게, 중복 금지)
- 카드 사이 구분선은 정확히 아래 한 줄만 사용:
────────────────
- 카드 개수는 최대 4개.
- 카드명에 마크다운/강조/번호 붙이지 마세요.
""".strip()

ANTI_FLUFF_RULES = """
과한 말 금지
- 사용자 프로필을 "첫 문장에 길게 요약"하지 마세요.
- 사용자가 묻지 않은 정보를 장황하게 안내하지 마세요.
- "제가 ~해드리겠습니다" 같은 소개 문장은 1줄 이내로.
- 같은 카드 제목을 반복하지 마세요(예: [핵심 뉴스] 여러 장 연속 금지).
""".strip()

FALLBACK_DOMAIN_GUARDRAILS = """
당신은 금융/주식 도메인의 어시스턴트입니다. 💹

필수 원칙
- 수익 보장/확실/무조건 같은 단정 금지
- 루머/미확인 사실을 사실처럼 단정 금지 (사실/추정/의견 구분)
- 장점만 말하지 말고 리스크(하락 요인) 1~2개는 반드시 포함
- 질문이 “추천”이면 회피하지 말고 종목을 직접 제시 (근거/리스크 필수)
- 출력은 반드시 카드 규칙을 따르며, 마크다운은 절대 금지
""".strip()


def _get_default_template() -> Optional[PromptTemplate]:
    return PromptTemplate.objects.filter(is_active=True).order_by("-updated_at", "-id").first()


def _risk_profile_text(code: str) -> str:
    return {
        "A": "공격형(고위험·고수익 선호, 성장/모멘텀 중심, 변동성 관리 중요)",
        "B": "중립형(시장수익률 지향, 분산/우량/ETF 중심)",
        "C": "안정형(변동성 최소화, 배당/방어·현금흐름 중심)",
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


# ---------------------------------------------------------
# Intent detection (to reduce unnecessary verbose output)
# ---------------------------------------------------------
def _normalize_text(s: str) -> str:
    return " ".join((s or "").strip().split())


def _is_smalltalk(message: str) -> bool:
    """
    '안녕' 같은 인삿말/짧은 잡담에는
    투자 프로필/뉴스 카드 폭격 금지.
    """
    m = _normalize_text(message).lower()
    if not m:
        return True

    smalltalk_phrases = [
        "안녕",
        "안녕하세요",
        "하이",
        "ㅎㅇ",
        "hello",
        "hi",
        "반가워",
        "반가워요",
        "고마워",
        "감사",
        "땡큐",
        "잘자",
        "굿나잇",
        "좋은 아침",
        "좋은아침",
        "점심 뭐",
        "저녁 뭐",
        "뭐해",
    ]
    if any(p in m for p in smalltalk_phrases) and len(m) <= 20:
        return True

    if len(m) <= 4:
        return True

    return False


def _is_finance_intent(message: str) -> bool:
    m = _normalize_text(message).lower()
    keys = [
        "뉴스",
        "증시",
        "코스피",
        "코스닥",
        "나스닥",
        "s&p",
        "금리",
        "환율",
        "fomc",
        "cpi",
        "실적",
        "전망",
        "매수",
        "매도",
        "추천",
        "종목",
        "포트폴리오",
        "etf",
        "주식",
        "채권",
        "배당",
        "리스크",
        "섹터",
        "반도체",
        "하이닉스",
        "삼성전자",
    ]
    return any(k in m for k in keys)


def _is_recommendation_intent(message: str) -> bool:
    m = _normalize_text(message).lower()
    keys = ["추천", "추천주", "종목 추천", "오늘 추천", "오늘의 추천", "top pick", "pick", "매수", "사볼", "담을"]
    return any(k.lower() in m for k in keys)


def _conversation_mode(message: str) -> str:
    """
    mode:
    - smalltalk: 인사/잡담 (짧고 가볍게)
    - finance: 금융 질문 (카드 기반)
    """
    if _is_smalltalk(message) and not _is_finance_intent(message):
        return "smalltalk"
    return "finance"


# ---------------------------------------------------------
# Prompt building blocks
# ---------------------------------------------------------
def _level_system_instruction(level: int) -> str:
    """
    레벨이 올라갈수록 더 개조식/압축.
    """
    level = _clamp_level(level)

    if level == 1:
        return "말투/난이도: 입문자 🙂 (해요체, 쉬운 표현, 결론 먼저, 3줄 요약)"
    if level == 2:
        return "말투/난이도: 초보 🙂 (해요체, 불릿 3~5개로 간단히)"
    if level == 3:
        return "말투/난이도: 일반 (합쇼체, 팩트 중심, 짧게)"
    if level == 4:
        return "말투/난이도: 숙련자 (하십시오체, 압축)"
    return "말투/난이도: 전문가 (개조식, 최소 문장)"


def _risk_overrides(risk: str) -> str:
    if risk == "A":
        return "리스크 성향: 공격형 🚀 (성장/모멘텀 관점, 수익보장 금지)"
    if risk == "C":
        return "리스크 성향: 안정형 🛡️ (방어/현금흐름 관점, 수익보장 금지)"
    return "리스크 성향: 중립형 ⚖️ (분산/균형 관점, 수익보장 금지)"


def _build_user_context_from_payload(profile_data: Dict[str, Any]) -> str:
    asset_type = profile_data.get("assetType") or profile_data.get("asset_type") or "미지정"
    sectors_list = _normalize_list(profile_data.get("sectors"))
    portfolio_list = _normalize_list(profile_data.get("portfolio"))
    risk = (profile_data.get("riskProfile") or profile_data.get("risk_profile") or "").strip()
    level = _clamp_level(profile_data.get("knowledgeLevel") or profile_data.get("knowledge_level") or 3)

    sectors_csv = ", ".join(sectors_list) if sectors_list else "None"
    portfolio_csv = ", ".join(portfolio_list) if portfolio_list else "Empty"

    return _join_nonempty(
        [
            "사용자 컨텍스트(참고용, 답변에 과하게 반복하지 말 것)",
            f"- 자산 유형: {asset_type}",
            f"- 관심 섹터: {sectors_csv}",
            f"- 리스크 성향: {_risk_profile_text(risk)}",
            f"- 지식 레벨: Level {level}",
            f"- 포트폴리오: {portfolio_csv}",
            "",
            "사용 규칙\n- 사용자가 '뉴스 요약/추천/포트 점검'을 요청할 때만 1~2줄로 최소 반영",
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


def _recommendation_policy(level: int) -> str:
    level = _clamp_level(level)
    if level <= 2:
        return "추천 모드 ✅  종목 2~3개 먼저 → 이유/체크포인트/리스크는 각 1~2줄로 최소화"
    if level == 3:
        return "추천 모드 ✅  Picks 먼저 → 종목별 근거/체크포인트/리스크를 짧게"
    if level == 4:
        return "추천 모드 ✅  Picks → Rationale → Risk/Invalidation (카드로 분리)"
    return "추천 모드 ✅  Picks/Thesis/Triggers/Risk/Action (카드로 분리)"


def _system_prompt_for_mode(
    *,
    mode: str,
    base_system: str,
    level_inst: str,
    risk_inst: str,
    rec_inst: str,
    user_context: str,
) -> str:

    if mode == "smalltalk":
        smalltalk_rules = """
대화 모드: 일상 대화 🙂
- 1~3문장으로 짧게 답하세요.
- 사용자 프로필/성향/포트폴리오를 먼저 언급하지 마세요.
- 마지막에 선택지를 주는 질문 1개만 하세요.
- 카드/구분선 사용 금지.
- 마크다운/강조(** 등) 절대 금지.
""".strip()

        return _join_nonempty(
            [
                base_system,
                level_inst,
                BANNED_MARKUP_RULES,
                ANTI_FLUFF_RULES,
                smalltalk_rules,
            ]
        )

    finance_rules = """
대화 모드: 금융 답변 💹
- 출력은 반드시 "카드 출력 규칙"을 따르세요.
- 카드 개수는 최대 4개까지만.
- 뉴스/포인트가 많아도 중요도 상위만.
- 같은 카드 제목 반복 금지.
- 마크다운(특히 **, ###, ``` )이 나오면 실패입니다.
""".strip()

    return _join_nonempty(
        [
            base_system,
            level_inst,
            risk_inst,
            rec_inst,
            user_context,
            BANNED_MARKUP_RULES,
            CARD_FORMAT_RULES,
            ANTI_FLUFF_RULES,
            finance_rules,
        ]
    )


def _should_include_user_context(mode: str, message: str) -> bool:
    if mode == "smalltalk":
        return False

    m = _normalize_text(message).lower()
    triggers = ["요약", "정리", "뉴스", "추천", "포트", "포트폴리오", "보유", "관심", "내 종목", "점검"]
    return any(t in m for t in triggers)


# =========================================================
# Endpoints
# =========================================================
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
                "messages": [_serialize_chatlog(m) for m in page_obj.object_list],
                "page": page,
                "page_size": page_size,
                "total": total,
                "has_next": False,
                "pagination_recommended": total > CHAT_SESSION_PAGE_THRESHOLD,
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
    raw_message = (request.data.get("message") or "").strip()

    if not raw_message:
        return Response({"detail": "message is required"}, status=400)
    if len(raw_message) > CHAT_MAX_MESSAGE_CHARS:
        return Response({"detail": f"message is too long (max {CHAT_MAX_MESSAGE_CHARS})"}, status=400)

    # -----------------------------
    # template resolve
    # -----------------------------
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

    # -----------------------------
    # session resolve/create
    # -----------------------------
    if session_id:
        try:
            session = ChatSession.objects.get(id=int(session_id), user=request.user)
        except Exception:
            return Response({"detail": "Invalid session_id"}, status=400)
    else:
        session = ChatSession.objects.create(user=request.user, template=template, title="")

    # -----------------------------
    # profile context (load but do not always inject)
    # -----------------------------
    profile_data = _get_user_profile_data(request)

    risk = ""
    level = 3
    built_user_context = ""
    if profile_data:
        risk = (profile_data.get("riskProfile") or profile_data.get("risk_profile") or "").strip()
        level = _clamp_level(profile_data.get("knowledgeLevel") or profile_data.get("knowledge_level") or 3)
        built_user_context = _build_user_context_from_payload(profile_data)

    # -----------------------------
    # mode + system prompt
    # -----------------------------
    mode = _conversation_mode(raw_message)

    level_inst = _level_system_instruction(level)
    risk_inst = _risk_overrides(risk) if (risk and mode == "finance") else ""
    rec_inst = _recommendation_policy(level) if (_is_recommendation_intent(raw_message) and mode == "finance") else ""

    user_context = built_user_context if _should_include_user_context(mode, raw_message) else ""

    system_prompt = _system_prompt_for_mode(
        mode=mode,
        base_system=base_system,
        level_inst=level_inst,
        risk_inst=risk_inst,
        rec_inst=rec_inst,
        user_context=user_context,
    )

    # -----------------------------
    # user content (template)
    # -----------------------------
    try:
        user_content = (user_prompt_template or "{message}").format(message=raw_message)
    except Exception:
        user_content = raw_message

    # persist user
    ChatLog.objects.create(session=session, role="user", content=user_content)

    # set title if empty
    if not (session.title or "").strip():
        session.title = _make_session_title(raw_message)
        session.updated_at = timezone.now()
        session.save(update_fields=["title", "updated_at"])

    # -----------------------------
    # history (chronological)
    # -----------------------------
    recent_logs = list(
        ChatLog.objects.filter(session=session).order_by("-created_at", "-id")[:CHAT_CONTEXT_MESSAGES]
    )[::-1]

    llm_msgs: List[LlmMessage] = []
    if system_prompt:
        llm_msgs.append(LlmMessage(role="system", content=system_prompt))

    for log in recent_logs:
        if log.role in ("user", "assistant") and (log.content or "").strip():
            llm_msgs.append(LlmMessage(role=log.role, content=log.content))

    # -----------------------------
    # LLM call
    # -----------------------------
    client = get_gemini_client()
    try:
        answer = client.chat(llm_msgs)
    except Exception as e:
        return Response({"detail": f"Chat failed: {str(e)}"}, status=502)

    # -----------------------------
    # sanitize answer (NO MARKDOWN, keep card style)
    # -----------------------------
    answer_clean = _sanitize_llm_answer(answer)

    # persist assistant
    ChatLog.objects.create(
        session=session,
        role="assistant",
        content=answer_clean[: CHAT_MAX_MESSAGE_CHARS * 5],
    )

    # bump session timestamp
    ChatSession.objects.filter(id=session.id).update(
        updated_at=timezone.now(),
        template_id=(template.id if template else None),
    )

    resp: Dict[str, Any] = {
        "answer": answer_clean,
        "session_id": session.id,
        "template": {"id": template.id, "key": template.key} if template else {"id": None, "key": "fallback"},
        "profile_loaded": bool(profile_data),
        "applied_level": level,
        "applied_risk": (risk or None),
        "recommendation_mode": bool(rec_inst),
        "mode": mode,  # 프론트 디버그
    }
    return Response(resp)
