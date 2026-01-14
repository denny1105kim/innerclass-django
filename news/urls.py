from django.urls import path
from .views import NewsView, NewsSummaryView,  NewsThemesView, ThemeNewsView

urlpatterns = [
    path("ai-recommend/", NewsView.as_view()),
    path("<int:news_id>/summary/", NewsSummaryView.as_view()),
    path("themes/", NewsThemesView.as_view()),        
    path("by-theme/", ThemeNewsView.as_view()),
]