from django.urls import path

from . import views

urlpatterns = [
    path("api/markets/today/", views.today_market, name="today_market"),
    path("api/chatbot/prompts/", views.chatbot_prompts, name="chatbot_prompts"),
    path("api/chatbot/sessions/", views.chatbot_sessions, name="chatbot_sessions"),
    path("api/chatbot/sessions/<int:session_id>/", views.chatbot_session_detail, name="chatbot_session_detail"),
    path("api/chatbot/chat/", views.chatbot_chat, name="chatbot_chat"),
]
