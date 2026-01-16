from django.urls import path

from .views import (
    trend_keywords,
    TrendNewsRecommendView,
    TrendNewsSummaryView,
)

urlpatterns = [
    path("keywords/", trend_keywords),
    path("ai-recommend/", TrendNewsRecommendView.as_view()),
    path("<int:news_id>/summary/", TrendNewsSummaryView.as_view()),
]
