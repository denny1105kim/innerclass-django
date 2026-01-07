from __future__ import annotations

from django.conf import settings
from django.db import models

# =========================================================
# Chat / Prompt (기존 유지 가능) - 요청 범위 밖이지만 구조만 정리
# =========================================================
class PromptTemplate(models.Model):
    key = models.SlugField(max_length=64, unique=True)
    name = models.CharField(max_length=128)
    description = models.TextField(blank=True, default="")

    system_prompt = models.TextField(blank=True, default="")
    user_prompt_template = models.TextField(
        blank=True,
        default="{message}",
        help_text="Use {message} placeholder for the user's input.",
    )

    is_active = models.BooleanField(default=True, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "name"]

    def __str__(self) -> str:
        return f"{self.key} ({'active' if self.is_active else 'inactive'})"


class ChatSession(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="chat_sessions",
        db_index=True,
    )
    title = models.CharField(max_length=120, blank=True, default="")
    template = models.ForeignKey(
        "PromptTemplate",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="chat_sessions",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        ordering = ["-updated_at", "-id"]

    def __str__(self) -> str:
        return f"ChatSession#{self.id} ({self.user_id})"


class ChatMessage(models.Model):
    session = models.ForeignKey(
        ChatSession,
        on_delete=models.CASCADE,
        related_name="messages",
        db_index=True,
    )
    role = models.CharField(max_length=20)  # "user" | "assistant"
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["created_at", "id"]
        indexes = [
            models.Index(fields=["session", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"ChatMessage#{self.id} {self.role}"