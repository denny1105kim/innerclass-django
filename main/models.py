from django.conf import settings
from django.db import models


class Market(models.TextChoices):
    KR = "KR", "Korea"
    US = "US", "United States"


class Stock(models.Model):
    market = models.CharField(max_length=2, choices=Market.choices, db_index=True)
    symbol = models.CharField(max_length=32, db_index=True)  # KR: "005930", US: "AAPL"
    name = models.CharField(max_length=128)

    currency = models.CharField(max_length=8, default="KRW")  # KRW / USD
    exchange = models.CharField(max_length=32, blank=True, default="")  # KOSPI/KOSDAQ/NASDAQ/NYSE 등

    class Meta:
        unique_together = [("market", "symbol")]

    def __str__(self) -> str:
        return f"{self.market}:{self.symbol} {self.name}"


class DailyStockSnapshot(models.Model):
    stock = models.ForeignKey(Stock, on_delete=models.CASCADE, related_name="snapshots")
    date = models.DateField(db_index=True)

    # intraday metrics
    open = models.DecimalField(max_digits=20, decimal_places=4, null=True)
    close = models.DecimalField(max_digits=20, decimal_places=4, null=True)

    # kept for compatibility / future use
    prev_close = models.DecimalField(max_digits=20, decimal_places=4, null=True)

    # by default we store intraday_pct in change_pct too (so existing UI can treat it as "change")
    change_pct = models.DecimalField(max_digits=10, decimal_places=4, null=True)
    intraday_pct = models.DecimalField(max_digits=10, decimal_places=4, null=True)

    market_cap = models.BigIntegerField(null=True)
    volume = models.BigIntegerField(null=True)

    # retained for future extension; currently not populated by the 5-min job
    volatility_20d = models.DecimalField(max_digits=10, decimal_places=4, null=True)

    class Meta:
        unique_together = [("stock", "date")]
        indexes = [
            models.Index(fields=["date", "stock"]),
            models.Index(fields=["date", "market_cap"]),
            models.Index(fields=["date", "intraday_pct"]),
        ]


class PromptTemplate(models.Model):
    """Reusable prompt template for the chatbot.

    - system_prompt: injected as the system message
    - user_prompt_template: formatted with {message} and sent as the user message
    """

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
    """User-scoped chat session (conversation thread)."""

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
    """Single message in a chat session."""

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
