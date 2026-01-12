from django.urls import path

from . import views

urlpatterns = [
    path("today/", views.today_rankings, name="today_rankings"),
    path("symbols/suggest/", views.symbol_suggest, name="symbol-suggest"),
    path("sessions/", views.MarketSessionsView.as_view()),
]
