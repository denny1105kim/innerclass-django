from django.contrib import admin

from .models import PromptTemplate


@admin.register(PromptTemplate)
class PromptTemplateAdmin(admin.ModelAdmin):
    list_display = ("key", "name", "is_active", "updated_at")
    list_filter = ("is_active",)
    search_fields = (
        "key",
        "name",
        "description",
        "system_prompt",
        "user_prompt_template",
    )
