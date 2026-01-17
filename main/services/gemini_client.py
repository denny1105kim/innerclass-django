from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import os

from google import genai
from google.genai import types  # 설정을 위한 types 모듈 import

@dataclass(frozen=True)
class ChatMessage:
    role: str  # "system" | "user" | "assistant"
    content: str


class GeminiClient:
    def __init__(self, model: str = "gemini-2.0-flash-001") -> None:
        """
        참고: 검색 기능을 사용하려면 'gemini-1.5-pro', 'gemini-2.0-flash' 등
        Search Grounding을 지원하는 모델을 사용해야 합니다.
        """
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY is required")

        self.client = genai.Client(api_key=api_key)
        self.model = model

    def chat(self, messages: List[ChatMessage], use_search: bool = True) -> str:
        """
        Gemini SDK는 contents를 단순 string or list로 받음.
        system / user / assistant role은 문자열로 합쳐서 전달.
        
        Args:
            messages: 대화 메시지 목록
            use_search: True일 경우 구글 검색(Grounding)을 사용함
        """

        # 1. 프롬프트 구성
        prompt_parts = []
        for m in messages:
            if m.role == "system":
                prompt_parts.append(f"[SYSTEM]\n{m.content}")
            elif m.role == "user":
                prompt_parts.append(f"[USER]\n{m.content}")
            else:
                prompt_parts.append(f"[ASSISTANT]\n{m.content}")

        prompt = "\n\n".join(prompt_parts).strip()

        # 2. 검색(Tools) 설정 구성
        generate_config = None
        if use_search:
            generate_config = types.GenerateContentConfig(
                tools=[
                    types.Tool(
                        google_search=types.GoogleSearch()  # 구글 검색 도구 활성화
                    )
                ],
                # 검색 결과에 따라 답변 스타일이 달라질 수 있음 (필요 시 조절)
                # temperature=0.7 
            )

        # 3. API 호출
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=generate_config, # 설정 전달
            )

            # 검색 결과가 포함된 경우에도 text 속성으로 답변을 가져올 수 있음
            text = getattr(response, "text", None)
            return text if text is not None else ""
            
        except Exception as e:
            # 에러 로깅 등을 추가할 수 있음
            print(f"Gemini API Error: {e}")
            return ""


def get_gemini_client() -> GeminiClient:
    # 검색 기능을 쓰려면 모델명이 중요합니다. (최신 모델 권장)
    # 예: gemini-2.0-flash-exp, gemini-1.5-pro-002 등
    model = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-001")
    return GeminiClient(model=model)