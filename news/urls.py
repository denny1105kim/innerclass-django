from django.urls import path
from .views import NewsView, NewsSummaryView, WorkerResultIngestView, NewsSectorListView, NewsBySectorView

urlpatterns = [
    path('ai-recommend/', NewsView.as_view()),
    path('<int:news_id>/summary/', NewsSummaryView.as_view()),
    path("worker/result/", WorkerResultIngestView.as_view(), name="news-worker-result"),
    path("sectors/", NewsSectorListView.as_view()),        
    path("by-sector/", NewsBySectorView.as_view()),    
]
