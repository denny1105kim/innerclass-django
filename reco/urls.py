# apps/reco/urls.py
from django.urls import path

from . import views

urlpatterns = [
    path("keywords/", views.trend_keywords),

]