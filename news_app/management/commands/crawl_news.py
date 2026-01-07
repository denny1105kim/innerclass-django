import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import timezone
from news_app.models import NewsArticle
import openai
import traceback

class Command(BaseCommand):
    help = '네이버 금융, 인베스팅닷컴, 연합인포맥스, 한국경제, 매일경제 뉴스를 크롤링하여 DB에 저장합니다.'

    def handle(self, *args, **kwargs):
        self.stdout.write("=========================================")
        self.stdout.write("📡 뉴스 크롤링 시스템 가동 시작")
        self.stdout.write("=========================================")

        total_saved = 0
        
        # 1. Naver Finance
        total_saved += self.crawl_naver()
        

        
        # 3. Yonhap Infomax
        total_saved += self.crawl_yonhap()

        # 4. Hankyung (New)
        total_saved += self.crawl_hankyung()

        # 5. Maeil Business (New)
        total_saved += self.crawl_mk()

        self.stdout.write("=========================================")
        self.stdout.write(self.style.SUCCESS(f"✅ 통합 크롤링 완료. (총 신규 저장: {total_saved}개)"))
        self.stdout.write("=========================================")

    def get_embedding(self, text):
        try:
            client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
            response = client.embeddings.create(
                input=text,
                model="text-embedding-3-small"
            )
            return response.data[0].embedding
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ 임베딩 생성 실패: {e}"))
            return None

    def save_article(self, title, summary, link, image_url, source_name, sector="기타", market="Korea", content=None):
        # 중복 체크: 제목 또는 URL이 같으면 중복
        if NewsArticle.objects.filter(title=title).exists():
            self.stdout.write(f"  - [{source_name}] (중복-제목) {title[:15]}...")
            return 0
        
        if NewsArticle.objects.filter(url=link).exists():
            self.stdout.write(f"  - [{source_name}] (중복-URL) {title[:15]}...")
            return 0

        self.stdout.write(f"  + [{source_name}] [New] {title[:15]}...")
        
        vector = self.get_embedding(summary)
        if not vector:
            self.stdout.write("    -> 벡터 생성 실패로 저장 건너뜀")
            return 0
        
        try:
            article = NewsArticle.objects.create(
                title=title,
                summary=summary,
                content=content, # 본문 (없으면 None)
                url=link,
                image_url=image_url,
                sector=sector,
                market=market,  # 파라미터 사용
                published_at=timezone.now(),
                embedding=vector
            )
            
            # LLM 선행 분석 및 저장
            from news_app.services import analyze_news
            analyze_news(article, save_to_db=True)
            
            return 1
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"    -> DB 저장 실패: {e}"))
            return 0

    # =========================================================================
    # 1. 네이버 금융 (Naver Finance)
    # =========================================================================
    def crawl_naver(self):
        self.stdout.write("\n>>> [1/3] 네이버 금융 뉴스 크롤링 중...")
        url = "https://finance.naver.com/news/mainnews.naver"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/98.0.4758.102 Safari/537.36"
        }
        count = 0
        try:
            response = requests.get(url, headers=headers)
            response.encoding = 'cp949'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            articles = soup.select('.mainNewsList li')
            for article in articles:
                try:
                    subject_tag = article.select_one('.articleSubject a')
                    summary_tag = article.select_one('.articleSummary')
                    
                    if not subject_tag or not summary_tag:
                        continue

                    title = subject_tag.text.strip()
                    link = "https://finance.naver.com" + subject_tag['href']
                    
                    # 썸네일
                    image_url = None
                    thumb_tag = article.select_one('img')
                    if thumb_tag:
                        # 썸네일 파라미터 제거 후 리사이징 호출
                        base_url = thumb_tag['src'].split('?')[0]
                        image_url = f"{base_url}?type=w660"

                    # 요약 정리
                    raw_summary = summary_tag.text.strip()
                    summary = raw_summary.split('\n')[0]

                    count += self.save_article(title, summary, link, image_url, "Naver", sector="금융/경제")
                    
                except Exception:
                    continue
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 네이버 크롤링 오류: {e}"))
        
        return count



    # =========================================================================
    # 3. 연합인포맥스 (Yonhap Infomax)
    # =========================================================================
    def crawl_yonhap(self):
        self.stdout.write("\n>>> [3/3] 연합인포맥스 크롤링 중...")
        url = "https://news.einfomax.co.kr/news/articleList.html?sc_section_code=S1N1"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        count = 0
        try:
            response = requests.get(url, headers=headers)
            response.encoding = response.apparent_encoding
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links that look like article views
            candidates = soup.find_all('a', href=True)
            
            processed_links = set()

            for a_tag in candidates:
                href = a_tag['href']
                if 'articleView.html' in href and 'idxno' in href:
                    if href in processed_links:
                        continue
                    processed_links.add(href)

                    title = a_tag.text.strip()
                    if len(title) < 5: 
                        continue
                        
                    # Fix URL construction
                    if href.startswith('http'):
                        full_link = href
                    else:
                        # Ensure no double slash if href starts with /
                        if href.startswith('/'):
                            full_link = "https://news.einfomax.co.kr" + href
                        else:
                            full_link = "https://news.einfomax.co.kr/news/" + href
                    
                    # Fetch Detail Page for Image & Summary
                    image_url = None
                    summary = title # Default to title
                    
                    try:
                        detail_res = requests.get(full_link, headers=headers, timeout=5)
                        detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                        
                        # 1. Image (OG Image is best)
                        og_image = detail_soup.find('meta', property='og:image')
                        if og_image:
                            image_url = og_image.get('content')
                        else:
                            # Fallback to body image
                            content_div = detail_soup.select_one('#article-view-content-div')
                            if content_div:
                                body_img = content_div.select_one('img')
                                if body_img:
                                    image_url = body_img.get('src')
                                    if image_url and not image_url.startswith('http'):
                                         image_url = "https://news.einfomax.co.kr" + image_url
                        
                        # 2. Summary (OG Description or Article Body)
                        og_desc = detail_soup.find('meta', property='og:description')
                        if og_desc:
                            summary = og_desc.get('content').strip()
                        else:
                             # Fallback: First sentence of body
                             content_div = detail_soup.select_one('#article-view-content-div')
                             if content_div:
                                 summary = content_div.text.strip()[:200]
                                 
                    except Exception:
                        continue

                    count += self.save_article(title, summary, full_link, image_url, "Infomax", sector="금융/경제")
                    
                    if count >= 10: 
                        break

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ 연합인포맥스 크롤링 오류: {e}"))

        return count

    # =========================================================================
    # 4. 한국경제 (Hankyung)
    # =========================================================================
    def crawl_hankyung(self):
        self.stdout.write("\n>>> [4/5] 한국경제(Hankyung) 크롤링 중...")
        url = "https://www.hankyung.com/economy"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36" 
        }

        count = 0
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 1. Headline & Major news often in .news-item
            articles = soup.select('.news-item')
            
            # If no articles found, try fallback
            if not articles:
                 articles = soup.select('.news-list li')

            for article in articles[:10]:
                try:
                    # Title & Link
                    title_tag = article.select_one('.news-tit a') or article.select_one('.tit a') or article.select_one('h3 a') or article.select_one('a')
                    
                    if not title_tag: 
                        continue

                    title = title_tag.text.strip()
                    link = title_tag['href']
                    if not link.startswith('http'):
                        link = link # Hankyung links often absolute, but check just in case

                    # Image
                    image_url = None
                    img_tag = article.select_one('img')
                    if img_tag:
                        image_url = img_tag.get('src')
                    
                    # Summary
                    summary_tag = article.select_one('.lead') or article.select_one('.txt')
                    summary = summary_tag.text.strip() if summary_tag else title

                    count += self.save_article(title, summary, link, image_url, "Hankyung", sector="금융/경제")
                
                except Exception:
                    continue

        except Exception as e:
             self.stdout.write(self.style.ERROR(f"❌ 한국경제 크롤링 오류: {e}"))
        
        return count

    # =========================================================================
    # 5. 매일경제 (MK)
    # =========================================================================
    def crawl_mk(self):
        self.stdout.write("\n>>> [5/5] 매일경제(MK) 크롤링 중...")
        url = "https://www.mk.co.kr/news/economy/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        count = 0
        try:
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Select article list
            # Found via debug: .news_node is the reliable class
            articles = soup.select('.news_node')
            
            # Fallback
            if not articles:
                articles = soup.select('.news_list .list_area') or soup.select('.list_news_area .list_news_item')
            
            for article in articles[:10]:
                try:
                    # Title & Link (Updated based on HTML debug)
                    title_tag = article.select_one('.news_ttl') or article.select_one('.news_title')
                    link_tag = article.select_one('a.link') or article.select_one('a.news_item')
                    
                    if not title_tag or not link_tag:
                        continue
                        
                    title = title_tag.text.strip()
                    link = link_tag['href']
                    
                    if not link.startswith('http'):
                        link = "https://www.mk.co.kr" + link

                    # Init variables for detail fetching
                    image_url = None
                    summary = title 
                    
                    # Fetch Detail Page for High-Res Image & Summary
                    try:
                        detail_res = requests.get(link, headers=headers, timeout=5)
                        detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                        
                        # OG Image is usually best quality
                        og_image = detail_soup.find('meta', property='og:image')
                        if og_image:
                            image_url = og_image.get('content')
                        
                        # OG Description for summary
                        og_desc = detail_soup.find('meta', property='og:description')
                        if og_desc:
                            summary = og_desc.get('content').strip()
                        else:
                             # Fallback summary
                            body = detail_soup.select_one('.news_cnt_detail_wrap')
                            if body:
                                summary = body.text.strip()[:200]
                                
                    except Exception as e:
                        # Fallback to list view logic if detail fails
                        img_tag = article.select_one('img')
                        if img_tag:
                             image_url = img_tag.get('src')
                        desc_tag = article.select_one('.news_desc')
                        if desc_tag:
                             summary = desc_tag.text.strip()

                    count += self.save_article(title, summary, link, image_url, "MK", sector="금융/경제")

                except Exception:
                    continue

        except Exception as e:
             self.stdout.write(self.style.ERROR(f"❌ 매일경제 크롤링 오류: {e}"))

        return count