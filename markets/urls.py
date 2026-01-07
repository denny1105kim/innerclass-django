# apps/markets/urls.py
from django.urls import path

from . import views

urlpatterns = [
    path("today/", views.today_market, name="today_market"),
]
