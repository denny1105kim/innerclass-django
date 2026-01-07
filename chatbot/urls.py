# apps/chatbot/urls.py
from django.urls import path

from . import views

urlpatterns = [
    path("prompts/", views.chatbot_prompts, name="chatbot-prompts"),
    path("sessions/", views.chatbot_sessions, name="chatbot-sessions"),
    path("sessions/<int:session_id>/", views.chatbot_session_detail, name="chatbot-session-detail"),
    path("chat/", views.chatbot_chat, name="chatbot-chat"),
]
