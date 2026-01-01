from rest_framework import serializers
from .models import UserProfile

class UserProfileSerializer(serializers.ModelSerializer):
    assetType = serializers.CharField(source='asset_type', required=False, allow_null=True)
    riskProfile = serializers.CharField(source='risk_profile', required=False, allow_null=True)
    knowledgeLevel = serializers.IntegerField(source='knowledge_level', required=False)

    class Meta:
        model = UserProfile
        fields = ['assetType', 'sectors', 'portfolio', 'riskProfile', 'knowledgeLevel']
    
    def create(self, validated_data):
        user = self.context['request'].user
        profile, created = UserProfile.objects.update_or_create(
            user=user,
            defaults=validated_data
        )
        return profile
