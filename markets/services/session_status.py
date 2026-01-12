from __future__ import annotations

from enum import Enum

class MarketSessionStatus(str, Enum):
    OPEN = "OPEN"
    PRE_OPEN = "PRE_OPEN"
    POST_CLOSE = "POST_CLOSE"
    CLOSED = "CLOSED"
    HOLIDAY = "HOLIDAY"
