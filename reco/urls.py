from django.urls import path
from . import views

urlpatterns = [
    path("keywords/", views.trend_keywords),
    path("news/<int:news_id>/summary/", views.TrendNewsSummaryView.as_view(), name="reco-trend-news-summary"),
]
