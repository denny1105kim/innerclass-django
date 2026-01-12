# news/urls.py
from django.urls import path
from .views import NewsView, NewsSummaryView

urlpatterns = [
    path("ai-recommend/", NewsView.as_view()),
    path("<int:news_id>/summary/", NewsSummaryView.as_view()),
]