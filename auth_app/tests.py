from django.test import TestCase
from django.contrib.auth import get_user_model
from rest_framework.test import APIClient
from rest_framework import status
from .models import UserProfile

User = get_user_model()

class OnboardingTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', email='test@example.com', password='password')
        self.client = APIClient()
        self.client.force_authenticate(user=self.user)
        self.url = '/api/user/onboarding/'

    def test_create_profile(self):
        data = {
            'assetType': '국내주식',
            'sectors': ['반도체', 'AI'],
            'portfolio': ['삼성전자'],
            'riskProfile': 'A',
            'knowledgeLevel': 3
        }
        response = self.client.post(self.url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(UserProfile.objects.filter(user=self.user).exists())
        
        profile = UserProfile.objects.get(user=self.user)
        self.assertEqual(profile.asset_type, '국내주식')
        self.assertEqual(profile.sectors, ['반도체', 'AI'])

    def test_get_profile(self):
        # Create profile first
        UserProfile.objects.create(
            user=self.user,
            asset_type='미국주식',
            sectors=['바이오'],
            portfolio=['Apple'],
            risk_profile='B',
            knowledge_level=2
        )

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['assetType'], '미국주식')
        self.assertEqual(response.data['knowledgeLevel'], 2)
