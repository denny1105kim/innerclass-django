
## 🚀 프로젝트 실행 방법 (Getting Started)
Docker를 사용하여 백엔드(Django), 데이터베이스(PostgreSQL)를 한 번에 실행
### 1. 필수 프로그램 설치
- Git
- Docker Desktop (실행 중이어야 함)

### 2. 환경 변수 설정
env.example 참고

### 3. 실행하기 (Docker)
Bash# Docker 이미지 빌드 및 실행
docker-compose up --build

### 4. 초기 DB 세팅 (최초 1회)
서버가 켜진 상태에서 새 터미널을 열고, 데이터베이스 테이블과 관리자 계정을 생성
1. 테이블 생성 (마이그레이션)
docker-compose exec backend python manage.py migrate
2. 관리자(Superuser) 계정 생성
docker-compose exec backend python manage.py createsuperuser

### 5. DB 시각화 도구(pgAdmin) 연결 방법
http://localhost:5050 접속 후 아래 정보로 로그인 및 연결하세요.
1. pgAdmin 로그인:
- Email: admin@admin.com
- Password: root
2. Server 등록 (Register Server):
- Host name: db
- Username: admin
- Password: secret1234
- Maintenance DB: innerclass_db
