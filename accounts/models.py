from django.contrib.auth import get_user_model
from django.contrib.postgres.fields import ArrayField
from django.db import models

User = get_user_model()

# Create your models here.

class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    asset_type = models.CharField(max_length=50, blank=True, null=True)
    sectors = ArrayField(models.CharField(max_length=100), blank=True, default=list)
    portfolio = ArrayField(models.CharField(max_length=100), blank=True, default=list)
    risk_profile = models.CharField(max_length=10, blank=True, null=True)
    knowledge_level = models.IntegerField(default=0)

    def __str__(self):
        return f"{self.user.username}'s Profile"
