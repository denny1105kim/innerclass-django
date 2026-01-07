# apps/reco/urls.py
from django.urls import path

from . import views

urlpatterns = [
    path("themes/", views.recommend_themes, name="recommend_themes"),
]