import os
import requests
from django.contrib.auth import get_user_model
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.tokens import RefreshToken

User = get_user_model()

class GoogleLoginView(APIView):
    def post(self, request):
        code = request.data.get('code')

        if not code:
            return Response({'error': 'Code is required'}, status=status.HTTP_400_BAD_REQUEST)
        
        client_id = os.environ.get('SOCIAL_AUTH_GOOGLE_CLIENT_ID')
        client_secret = os.environ.get('SOCIAL_AUTH_GOOGLE_SECRET')
        redirect_uri = os.environ.get('GOOGLE_REDIRECT_URI')

        # 1. 구글로부터 Access Token 받아오기
        token_req = requests.post(
            f"https://oauth2.googleapis.com/token",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": redirect_uri,
            }
        )
        token_req_json = token_req.json()
        error = token_req_json.get("error")

        if error is not None:
            return Response({'error': f"Google Token Error: {error}"}, status=status.HTTP_400_BAD_REQUEST)
        
        google_access_token = token_req_json.get('access_token')

        # 2. 유저 정보 받아오기
        user_req = requests.get(
            f"https://www.googleapis.com/oauth2/v1/userinfo",
            headers={"Authorization": f"Bearer {google_access_token}"}
        )
        user_req_json = user_req.json()
        email = user_req_json.get('email')
        name = user_req_json.get('name')

        if not email:
            return Response({'error': 'Email not found in Google account'}, status=status.HTTP_400_BAD_REQUEST)
        
        # 3. 회원가입 또는 로그인 처리
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            user = User.objects.create(email=email, username=email, first_name=name)
            user.set_unusable_password()
            user.save()

        refresh = RefreshToken.for_user(user)
        
        return Response({
            'access': str(refresh.access_token),
            'refresh': str(refresh),
            'user': {
                'email': user.email,
                'name': user.first_name
            }
        }, status=status.HTTP_200_OK)