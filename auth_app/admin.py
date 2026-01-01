from django.contrib import admin

# Register your models here.

from .models import UserProfile

@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ['user', 'asset_type', 'risk_profile', 'knowledge_level']
    search_fields = ['user__username', 'user__email']
