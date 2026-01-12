from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from reco.models import TrendKeywordNews

from news.services.analyze_news_main import analyze_news_main


@dataclass
class _ArticleLike:
    """
    analyze_news()가 요구하는 최소 속성들을 맞춰주기 위한 duck-typing 어댑터.
    analyze_news 내부 구현이 필드 접근 위주라면 그대로 동작합니다.
    """
    title: str
    summary: str
    content: str
    url: str

    published_at: Optional[Any] = None
    market: str = "KR"
    ticker: str = ""
    sector: str = "ETC"
    related_name: str = ""
    confidence: float = 0.0

    analysis: Optional[Dict[str, Any]] = None


def analyze_trend_news(item: TrendKeywordNews, *, save_to_db: bool = True) -> Optional[Dict[str, Any]]:
    """
    reco TrendKeywordNews를 news의 analyze_news() 포맷으로 분석.
    - save_to_db=True이면 item.analysis에 저장
    """
    content = (item.content or "").strip()
    if not content:
        content = (item.summary or "").strip()

    article_like = _ArticleLike(
        title=(item.title or "").strip(),
        summary=(item.summary or "").strip(),
        content=content,
        url=(item.link or "").strip(),
        market="KR",  
    )

    result = analyze_news_main(article_like, save_to_db=False) 

    if save_to_db and isinstance(result, dict):
        item.analysis = result
        item.save(update_fields=["analysis", "updated_at"])

    return result if isinstance(result, dict) else None
