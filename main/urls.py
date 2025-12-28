# main/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path("api/markets/today/", views.today_market, name="today_market"),
    ]