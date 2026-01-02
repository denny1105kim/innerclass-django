from __future__ import annotations

from dataclasses import dataclass
from typing import List
import os

from google import genai


@dataclass(frozen=True)
class ChatMessage:
    role: str  # "system" | "user" | "assistant"
    content: str


class GeminiClient:
    def __init__(self, model: str = "gemini-3-flash-preview") -> None:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY is required")

        # 명시적으로 api_key 전달 (환경에 따라 안정성 향상)
        self.client = genai.Client(api_key=api_key)
        self.model = model

    def chat(self, messages: List[ChatMessage]) -> str:
        """
        Gemini SDK는 contents를 단순 string or list로 받음.
        system / user / assistant role은 문자열로 합쳐서 전달.
        """

        prompt_parts = []
        for m in messages:
            if m.role == "system":
                prompt_parts.append(f"[SYSTEM]\n{m.content}")
            elif m.role == "user":
                prompt_parts.append(f"[USER]\n{m.content}")
            else:
                prompt_parts.append(f"[ASSISTANT]\n{m.content}")

        prompt = "\n\n".join(prompt_parts).strip()

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
        )

        text = getattr(response, "text", None)
        return text if text is not None else ""


def get_gemini_client() -> GeminiClient:
    model = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
    return GeminiClient(model=model)
