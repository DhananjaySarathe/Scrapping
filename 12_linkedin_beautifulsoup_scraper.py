"""
LinkedIn Ad Library Scraper - BeautifulSoup Enhanced
Uses cookie-based pagination + BeautifulSoup for accurate detail page scraping
Properly extracts ad text, assets, and metadata using BeautifulSoup
"""

import requests
import json
import time
import os
import re
import hashlib
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, unquote
from bs4 import BeautifulSoup

# Selenium for cookie extraction (used only once)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("⚠ Selenium not installed. Install with: pip install selenium")


class LinkedInBeautifulSoupScraper:
    """
    LinkedIn Ad Library Scraper with BeautifulSoup for detail pages
    
    Strategy:
    1. Use cookie-based pagination to get all ad IDs
    2. For each ad detail page, use BeautifulSoup to extract:
       - Ad text (properly cleaned)
       - Assets (images, videos, logos, posters)
       - Metadata (advertiser, ad type, CTA, etc.)
    3. Download assets (highest quality videos only)
    
    Usage:
        scraper = LinkedInBeautifulSoupScraper()
        scraper.fetch_cookies()  # Run once
        details = scraper.scrape_complete("Nike", max_results=100)
    """
    
    def __init__(self, cookies_file: str = "cookies.json"):
        """Initialize scraper"""
        self.cookies_file = cookies_file
        self.api_url = "https://www.linkedin.com/ad-library/searchPaginationFragment"
        self.search_base_url = "https://www.linkedin.com/ad-library/search"
        self.detail_base_url = "https://www.linkedin.com/ad-library/detail"
        self.session = requests.Session()
        self._setup_headers()
        
        # Track seen assets for deduplication
        self.seen_assets = {
            "logos": {},
            "images": {},
            "videos": {},  # base_path -> local_path
            "posters": {}
        }
        
    def _setup_headers(self):
        """Setup request headers"""
        self.headers = {
            "Accept": "application/vnd.linkedin.normalized+json+2.1, application/json, text/html",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.linkedin.com/ad-library/search",
            "Origin": "https://www.linkedin.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        self.session.headers.update(self.headers)
    
    def _update_headers_with_csrf(self):
        """Update headers with CSRF token from cookies"""
        cookies = self.load_cookies()
        if cookies and "JSESSIONID" in cookies:
            jsessionid = cookies["JSESSIONID"]
            if jsessionid.startswith("ajax:"):
                self.session.headers["csrf-token"] = jsessionid
    
    def _normalize_pagination_token(self, token: Optional[str]) -> Tuple[str, Optional[str]]:
        """Normalize pagination token to determine pagination type"""
        if not token:
            return ("api", None)
        
        if '#' in token:
            try:
                offset = token.split('#')[0]
                return ("offset", int(offset))
            except (ValueError, IndexError):
                return ("api", token)
        
        return ("api", token)
    
    def extract_next_token_from_html(self, html: str) -> Optional[str]:
        """Extract next pagination token from HTML response"""
        patterns = [
            r'"paginationToken"\s*:\s*"([^"]+)"',
            r'paginationToken["\']?\s*[:=]\s*["\']([^"\']+)',
            r'data-pagination-token="([^"]+)"',
            r'pagination-token["\']?\s*[:=]\s*["\']([^"\']+)',
        ]
        
        tokens_found = []
        
        for pattern in patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                token = match if isinstance(match, str) else match[0] if match else None
                if token and token != "null" and token not in tokens_found:
                    tokens_found.append(token)
        
        # If multiple tokens found, prefer offset tokens (#) over API tokens
        if tokens_found:
            # Sort: offset tokens first, then API tokens
            offset_tokens = [t for t in tokens_found if '#' in t]
            api_tokens = [t for t in tokens_found if '#' not in t]
            
            if offset_tokens:
                return offset_tokens[0]
            elif api_tokens:
                return api_tokens[0]
        
        return None
    
    def fetch_cookies(self, account_owner: str = "Nike", headless: bool = True) -> bool:
        """Use Selenium ONCE to fetch LinkedIn cookies"""
        if not SELENIUM_AVAILABLE:
            print("✗ Selenium not available. Cannot fetch cookies.")
            return False
        
        try:
            print("=" * 80)
            print("FETCHING LINKEDIN COOKIES USING SELENIUM")
            print("=" * 80)
            
            options = Options()
            if headless:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            
            driver = webdriver.Chrome(options=options)
            
            url = f"https://www.linkedin.com/ad-library/search?accountOwner={account_owner}"
            driver.get(url)
            
            print("Waiting 8 seconds for LinkedIn to set cookies...")
            time.sleep(8)
            
            try:
                driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(2)
            except:
                pass
            
            cookies = driver.get_cookies()
            
            with open(self.cookies_file, "w") as f:
                json.dump(cookies, f, indent=2)
            
            driver.quit()
            
            print(f"✓ Cookies saved to {self.cookies_file}")
            print(f"✓ Found {len(cookies)} cookies")
            print("=" * 80)
            
            return True
            
        except Exception as e:
            print(f"✗ Error fetching cookies: {e}")
            return False
    
    def load_cookies(self) -> Dict[str, str]:
        """Load cookies from JSON file"""
        try:
            with open(self.cookies_file, "r") as f:
                raw_cookies = json.load(f)
            return {cookie["name"]: cookie["value"] for cookie in raw_cookies}
        except FileNotFoundError:
            print(f"✗ Cookies file not found: {self.cookies_file}")
            return {}
        except Exception as e:
            print(f"✗ Error loading cookies: {e}")
            return {}
    
    def fetch_api_page(self, account_owner: str, pagination_token: Optional[str] = None) -> Optional[Dict]:
        """Fetch a single page from LinkedIn pagination API"""
        params = {"accountOwner": account_owner}
        if pagination_token:
            params["paginationToken"] = pagination_token
        
        cookies = self.load_cookies()
        if not cookies:
            return None
        
        self._update_headers_with_csrf()
        
        try:
            response = self.session.get(self.api_url, params=params, cookies=cookies, timeout=15)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and ('html' in data or 'paginationToken' in data):
                        return data
                except json.JSONDecodeError:
                    pass
                
                # LinkedIn returned HTML
                if 'text/html' in response.headers.get('Content-Type', '').lower():
                    html_content = response.text
                    pagination_token = self.extract_next_token_from_html(html_content)
                    return {"html": html_content, "paginationToken": pagination_token}
                
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return None
            else:
                return None
                
        except requests.exceptions.RequestException:
            return None
    
    def fetch_offset_page(self, account_owner: str, offset: int = 0) -> Optional[Dict]:
        """Fetch page using offset-based pagination"""
        url = self.search_base_url
        params = {"accountOwner": account_owner, "countries": "ALL", "start": str(offset)}
        
        cookies = self.load_cookies()
        if not cookies:
            return None
        
        self._update_headers_with_csrf()
        
        try:
            response = self.session.get(url, params=params, cookies=cookies, timeout=15)
            
            if response.status_code == 200:
                html = response.text
                next_token = self.extract_next_token_from_html(html)
                return {"html": html, "paginationToken": next_token}
            return None
                
        except requests.exceptions.RequestException:
            return None
    
    def extract_ad_ids_from_html(self, html_fragment: str) -> List[str]:
        """Extract ad IDs from HTML fragment using regex"""
        ad_ids = []
        pattern = r'/ad-library/detail/(\d+)'
        matches = re.findall(pattern, html_fragment)
        
        seen = set()
        for ad_id in matches:
            if ad_id not in seen:
                seen.add(ad_id)
                ad_ids.append(ad_id)
        
        return ad_ids
    
    def scrape_search_pages(self, account_owner: str, max_results: int = 100,
                           delay: float = 1.0) -> List[str]:
        """
        Scrape all ad IDs using pagination
        
        Returns:
            List of ad IDs
        """
        all_ad_ids = []
        pagination_token = None
        page_num = 1
        
        print(f"\n{'='*80}")
        print(f"SCRAPING AD IDs VIA PAGINATION")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        print(f"{'='*80}\n")
        
        seen_tokens = set()  # Track seen tokens to detect loops
        
        while len(all_ad_ids) < max_results:
            print(f"{'─'*80}")
            print(f"📄 PAGE {page_num} - Starting new page")
            print(f"{'─'*80}")
            print(f"Fetching with token: {pagination_token or 'None (first page)'}")
            
            # Detect infinite loop: if we've seen this token before
            if pagination_token and pagination_token in seen_tokens:
                print(f"  ⚠ Warning: Token already seen! Possible infinite loop.")
                print(f"  → Stopping to prevent infinite loop")
                break
            
            if pagination_token:
                seen_tokens.add(pagination_token)
            
            mode, value = self._normalize_pagination_token(pagination_token)
            
            if mode == "api":
                print(f"  → Using API pagination method")
                data = self.fetch_api_page(account_owner, value)
            elif mode == "offset":
                print(f"  → Using offset pagination method (offset={value})")
                data = self.fetch_offset_page(account_owner, offset=value)
            else:
                print(f"  → Using API pagination method (first page)")
                data = self.fetch_api_page(account_owner, None)
            
            if not data:
                print("  ✗ Failed to fetch page, stopping")
                break
            
            html_fragment = data.get("html", "")
            if not html_fragment:
                print("  ✗ No HTML fragment in response, stopping")
                break
            
            print(f"  ✓ Received HTML fragment ({len(html_fragment)} chars)")
            
            ad_ids = self.extract_ad_ids_from_html(html_fragment)
            
            if not ad_ids:
                print("  ⚠ No ad IDs found in this page")
                if not data.get("paginationToken"):
                    print("  ✓ Reached end of results")
                    break
            else:
                print(f"  ✓ Found {len(ad_ids)} ads in this page")
                
                ads_before = len(all_ad_ids)
                for ad_id in ad_ids:
                    if len(all_ad_ids) >= max_results:
                        break
                    if ad_id not in all_ad_ids:
                        all_ad_ids.append(ad_id)
                
                ads_added = len(all_ad_ids) - ads_before
                if ads_added == 0:
                    print(f"  ⚠ No new ads found (all duplicates)")
                    # If no new ads and we've seen this token, break
                    if pagination_token and pagination_token in seen_tokens:
                        print(f"  → Stopping: no new ads and token already seen")
                        break
                else:
                    print(f"  ✓ Added {ads_added} new ads")
                
                print(f"  ✓ Total ad IDs collected: {len(all_ad_ids)}/{max_results}")
            
            next_token = data.get("paginationToken")
            
            # If next token is same as current, we're stuck
            if next_token == pagination_token:
                print(f"  ⚠ Next token is same as current token. Reached end or stuck.")
                break
            
            pagination_token = next_token
            
            if not pagination_token:
                print("  ✓ No more pages available")
                break
            
            page_num += 1
            
            # Safety limit: prevent infinite loops
            if page_num > 100:
                print(f"  ⚠ Safety limit reached (100 pages). Stopping.")
                break
            
            if delay > 0 and len(all_ad_ids) < max_results:
                time.sleep(delay)
        
        print(f"\n{'='*80}")
        print(f"SEARCH SCRAPING COMPLETE!")
        print(f"Total pages scraped: {page_num - 1}")
        print(f"Total ad IDs collected: {len(all_ad_ids)}")
        print(f"{'='*80}\n")
        
        return all_ad_ids
    
    # ==================== BeautifulSoup Detail Page Scraping ====================
    
    def scrape_ad_detail_with_bs4(self, ad_id: str) -> Dict:
        """
        Scrape a single ad detail page using BeautifulSoup
        
        Args:
            ad_id: Ad ID
            
        Returns:
            Dictionary with complete ad details
        """
        url = f"{self.detail_base_url}/{ad_id}"
        cookies = self.load_cookies()
        
        ad_detail = {
            "ad_id": ad_id,
            "detail_url": url,
            "advertiser": None,
            "ad_text": None,
            "ad_type": None,
            "call_to_action": None,
            "paid_for_by": None,
            "logo_url": None,
            "assets": {
                "images": [],
                "videos": [],
                "posters": []
            }
        }
        
        try:
            print(f"  Scraping ad ID: {ad_id}...")
            response = self.session.get(url, cookies=cookies, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract advertiser name
                advertiser_selectors = [
                    'h1',
                    'h2',
                    'a[href*="/company/"]',
                    '[data-test-id="advertiser-name"]',
                    '.advertiser-name',
                    'span[class*="advertiser"]',
                ]
                
                for selector in advertiser_selectors:
                    element = soup.select_one(selector)
                    if element:
                        text = element.get_text(strip=True)
                        if text and len(text) < 100 and text.lower() not in ['ad details', 'ad detail']:
                            ad_detail["advertiser"] = text
                            break
                
                # Extract ad text/content - look for main content areas
                content_selectors = [
                    '.commentary__content',
                    'p.commentary__content',
                    '.ad-content',
                    '.ad-text',
                    '[class*="commentary"]',
                    '[class*="content"]',
                    'p',
                ]
                
                ad_text_parts = []
                seen_texts = set()
                
                for selector in content_selectors:
                    elements = soup.select(selector)
                    for elem in elements[:10]:  # Check first 10 matches
                        text = elem.get_text(strip=True)
                        # Filter out navigation, footer, and other non-ad content
                        if (text and 
                            10 < len(text) < 2000 and 
                            text not in seen_texts and
                            not any(skip in text.lower() for skip in [
                                'cookie', 'privacy', 'policy', 'about', 
                                'linkedin corporation', 'please note',
                                'terms of service', 'ad details',
                                'view details', 'see more', '…see more',
                                'sign in', 'sign up', 'join now'
                            ])):
                            seen_texts.add(text)
                            ad_text_parts.append(text)
                
                # Clean and combine ad text
                if ad_text_parts:
                    # Remove duplicates and combine
                    unique_texts = []
                    for text in ad_text_parts:
                        is_duplicate = False
                        for existing in unique_texts:
                            if text in existing or existing in text:
                                is_duplicate = True
                                break
                        if not is_duplicate:
                            unique_texts.append(text)
                    
                    ad_detail["ad_text"] = "\n\n".join(unique_texts[:5])  # Max 5 paragraphs
                
                # Extract ad type
                page_text = soup.get_text()
                ad_type_patterns = [
                    r'(Video Ad|Image Ad|Carousel Ad|Single Image Ad|Sponsored Content)',
                    r'Ad Type[:\s]+(\w+)',
                    r'type["\']?\s*[:=]\s*["\']([^"\']+)',
                ]
                
                for pattern in ad_type_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        ad_detail["ad_type"] = match.group(1)
                        break
                
                # Extract call-to-action
                cta_selectors = [
                    'button[data-tracking-control-name*="cta"]',
                    'a[class*="cta"]',
                    'button',
                    'a[class*="button"]',
                ]
                
                ctas = []
                for selector in cta_selectors:
                    elements = soup.select(selector)
                    for elem in elements[:5]:
                        text = elem.get_text(strip=True)
                        href = elem.get('href', '')
                        if (text and 
                            len(text) < 100 and 
                            text.lower() not in ['see more', '…see more', 'view details', 'sign in']):
                            ctas.append({"text": text, "link": href})
                
                if ctas:
                    ad_detail["call_to_action"] = ctas[:3]  # Max 3 CTAs
                
                # Extract "Paid for by"
                paid_for_patterns = [
                    r'Paid for by[:\s]+(.+?)(?:\n|$)',
                    r'Paid for by[:\s]+(.+?)(?:\.|$)',
                ]
                
                for pattern in paid_for_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        ad_detail["paid_for_by"] = match.group(1).strip()
                        break
                
                # Extract logo using BeautifulSoup
                logo_url = self._extract_logo_with_bs4(soup)
                if logo_url:
                    ad_detail["logo_url"] = logo_url
                
                # Extract assets using BeautifulSoup
                assets = self._extract_assets_with_bs4(soup)
                ad_detail["assets"] = assets
                
                print(f"  ✓ Successfully scraped ad ID: {ad_id}")
                return ad_detail
            else:
                print(f"  ✗ Failed: Status code {response.status_code}")
                ad_detail["error"] = f"HTTP {response.status_code}"
                return ad_detail
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error: {e}")
            ad_detail["error"] = str(e)
            return ad_detail
        except Exception as e:
            print(f"  ✗ Error parsing: {e}")
            ad_detail["error"] = str(e)
            return ad_detail
    
    def _extract_logo_with_bs4(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract logo URL using BeautifulSoup"""
        try:
            logo_selectors = [
                'img[alt*="logo" i]',
                'img[alt*="advertiser" i]',
                'a[href*="company"] img',
                '.advertiser-logo img',
                'img[data-delayed-url*="logo" i]',
                'img[src*="logo" i]',
            ]
            
            for selector in logo_selectors:
                img = soup.select_one(selector)
                if img:
                    logo_url = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                    if logo_url and logo_url.startswith('http'):
                        return unquote(logo_url.replace('&amp;', '&'))
            
            # Look for company links with images
            advertiser_links = soup.find_all('a', href=re.compile(r'/company/'))
            for link in advertiser_links:
                img = link.find('img')
                if img:
                    logo_url = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                    if logo_url and logo_url.startswith('http'):
                        return unquote(logo_url.replace('&amp;', '&'))
            
            return None
        except Exception:
            return None
    
    def _extract_assets_with_bs4(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract assets using BeautifulSoup"""
        assets = {
            "images": [],
            "videos": [],
            "posters": []
        }
        
        try:
            # Extract images
            images = soup.find_all('img')
            seen_images = set()
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                if src:
                    src = unquote(src.replace('&amp;', '&'))
                    if (src.startswith('http') and 
                        'logo' not in src.lower() and
                        src not in seen_images):
                        seen_images.add(src)
                        assets["images"].append(src)
            
            # Extract videos
            videos = soup.find_all('video')
            video_urls = []
            
            for video in videos:
                src = video.get('src') or video.get('data-src')
                if src and src.startswith('http'):
                    video_urls.append(unquote(src.replace('&amp;', '&')))
                
                # Check data-sources attribute (JSON encoded)
                data_sources = video.get('data-sources')
                if data_sources:
                    try:
                        decoded = unquote(data_sources.replace('&quot;', '"').replace('&amp;', '&'))
                        sources = json.loads(decoded)
                        if isinstance(sources, list):
                            for source in sources:
                                if isinstance(source, dict) and 'src' in source:
                                    video_urls.append(source['src'])
                    except (json.JSONDecodeError, AttributeError):
                        pass
                
                # Extract poster
                poster = video.get('data-poster-url') or video.get('poster')
                if poster and poster.startswith('http'):
                    assets["posters"].append(unquote(poster.replace('&amp;', '&')))
            
            # Also check for video URLs in script tags or data attributes
            for elem in soup.find_all(attrs={'data-sources': True}):
                data_sources = elem.get('data-sources')
                if data_sources:
                    try:
                        decoded = unquote(data_sources.replace('&quot;', '"').replace('&amp;', '&'))
                        sources = json.loads(decoded)
                        if isinstance(sources, list):
                            for source in sources:
                                if isinstance(source, dict) and 'src' in source:
                                    url = source['src']
                                    if url.startswith('http'):
                                        video_urls.append(unquote(url))
                    except (json.JSONDecodeError, AttributeError):
                        pass
            
            # Group videos by base path and select highest quality
            video_groups = {}
            for url in video_urls:
                base_path = self._get_video_base_path(url)
                if base_path not in video_groups:
                    video_groups[base_path] = []
                video_groups[base_path].append(url)
            
            # Select highest quality from each group
            for base_path, urls in video_groups.items():
                best_url = max(urls, key=lambda x: max([int(m) for m in re.findall(r'(\d+)p', x)] + [0]))
                assets["videos"].append(best_url)
            
            return assets
            
        except Exception as e:
            print(f"    Error extracting assets: {e}")
            return assets
    
    def _get_video_base_path(self, url: str) -> str:
        """Extract base path for video (removes quality indicators)"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            path = re.sub(r'/mp4-\d+p-\d+fp-[^/]+/', '/', path)
            path = re.sub(r'/mp4-\d+p/', '/', path)
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        except:
            return url
    
    # ==================== Asset Downloading ====================
    
    def _get_file_extension(self, url: str) -> str:
        """Determine file extension from URL"""
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        if '.jpg' in path or '.jpeg' in path:
            return '.jpg'
        elif '.png' in path:
            return '.png'
        elif '.mp4' in path:
            return '.mp4'
        elif '.webm' in path:
            return '.webm'
        elif 'video' in path or 'playlist' in path:
            return '.mp4'
        else:
            return '.jpg'
    
    def _generate_filename(self, url: str, asset_type: str, ad_id: str, index: int = 0) -> str:
        """Generate filename for asset"""
        parsed = urlparse(url)
        path = parsed.path
        
        path_parts = [p for p in path.split('/') if p and p not in ['dms', 'image', 'v2', 'playlist', 'vid']]
        
        if path_parts:
            base_name = path_parts[-1]
            base_name = re.sub(r'[^a-zA-Z0-9_-]', '_', base_name)[:50]
        else:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            base_name = f"{asset_type}_{url_hash}"
        
        ext = self._get_file_extension(url)
        return f"{ad_id}_{base_name}_{index}{ext}"
    
    def _download_asset(self, url: str, output_path: str) -> bool:
        """Download a single asset"""
        cookies = self.load_cookies()
        
        try:
            response = self.session.get(url, cookies=cookies, timeout=30, stream=True, allow_redirects=True)
            
            if response.status_code == 200:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                return True
            return False
        except Exception:
            return False
    
    def _download_ad_assets(self, ad_id: str, logo_url: Optional[str],
                           assets: Dict[str, List[str]],
                           output_dir: str) -> Dict[str, List[str]]:
        """Download assets for an ad (highest quality videos only)"""
        downloaded = {
            "logo": None,
            "images": [],
            "videos": [],
            "posters": []
        }
        
        ad_dir = os.path.join(output_dir, ad_id)
        
        # Download logo
        if logo_url:
            logo_filename = self._generate_filename(logo_url, "logo", ad_id, 0)
            logo_path = os.path.join(ad_dir, "logo", logo_filename)
            if self._download_asset(logo_url, logo_path):
                downloaded["logo"] = logo_path
                print(f"    ✓ Logo downloaded")
        
        # Download images
        if assets.get("images"):
            for i, img_url in enumerate(assets["images"], 1):
                img_filename = self._generate_filename(img_url, "image", ad_id, i)
                img_path = os.path.join(ad_dir, "images", img_filename)
                if self._download_asset(img_url, img_path):
                    downloaded["images"].append(img_path)
            if downloaded["images"]:
                print(f"    ✓ Downloaded {len(downloaded['images'])} images")
        
        # Download videos (already filtered to highest quality)
        if assets.get("videos"):
            for i, video_url in enumerate(assets["videos"], 1):
                video_filename = self._generate_filename(video_url, "video", ad_id, i)
                video_path = os.path.join(ad_dir, "videos", video_filename)
                if self._download_asset(video_url, video_path):
                    downloaded["videos"].append(video_path)
            if downloaded["videos"]:
                print(f"    ✓ Downloaded {len(downloaded['videos'])} videos (highest quality)")
        
        # Download posters
        if assets.get("posters"):
            for i, poster_url in enumerate(assets["posters"], 1):
                poster_filename = self._generate_filename(poster_url, "poster", ad_id, i)
                poster_path = os.path.join(ad_dir, "posters", poster_filename)
                if self._download_asset(poster_url, poster_path):
                    downloaded["posters"].append(poster_path)
            if downloaded["posters"]:
                print(f"    ✓ Downloaded {len(downloaded['posters'])} posters")
        
        return downloaded
    
    # ==================== Complete Workflow ====================
    
    def scrape_complete(self, account_owner: str, max_results: int = 100,
                       delay: float = 2.0, download_assets: bool = True,
                       assets_output_dir: str = "downloaded_assets",
                       output_json: str = "complete_ad_details.json") -> List[Dict]:
        """
        Complete scraping workflow
        
        Args:
            account_owner: Advertiser name
            max_results: Maximum number of ads
            delay: Delay between requests
            download_assets: Whether to download assets
            assets_output_dir: Directory for assets
            output_json: Output JSON filename
            
        Returns:
            List of complete ad detail dictionaries
        """
        print(f"\n{'='*80}")
        print(f"COMPLETE LINKEDIN AD SCRAPING (BeautifulSoup Enhanced)")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        if download_assets:
            print(f"Assets Directory: {assets_output_dir}/")
        print(f"{'='*80}\n")
        
        # Check if cookies exist
        cookies = self.load_cookies()
        if not cookies:
            print("⚠ No cookies found. Fetching cookies using Selenium...")
            if not self.fetch_cookies(account_owner):
                print("✗ Failed to fetch cookies. Cannot proceed.")
                return []
        
        # Step 1: Scrape search pages to get ad IDs
        print("STEP 1: Scraping search pages to get ad IDs...")
        ad_ids = self.scrape_search_pages(
            account_owner=account_owner,
            max_results=max_results,
            delay=delay
        )
        
        if not ad_ids:
            print("No ad IDs found")
            return []
        
        # Step 2: Scrape detail pages using BeautifulSoup
        print(f"\nSTEP 2: Scraping detail pages using BeautifulSoup...")
        all_details = []
        
        for i, ad_id in enumerate(ad_ids, 1):
            print(f"[{i}/{len(ad_ids)}] ", end="")
            
            detail = self.scrape_ad_detail_with_bs4(ad_id)
            
            # Download assets if enabled
            if download_assets:
                print(f"    Downloading assets...")
                downloaded = self._download_ad_assets(
                    ad_id=ad_id,
                    logo_url=detail.get("logo_url"),
                    assets=detail.get("assets", {}),
                    output_dir=assets_output_dir
                )
                
                detail["logo_local_path"] = downloaded["logo"]
                detail["assets_local_paths"] = {
                    "images": downloaded["images"],
                    "videos": downloaded["videos"],
                    "posters": downloaded["posters"]
                }
            
            all_details.append(detail)
            
            # Save progress periodically
            if i % 10 == 0:
                try:
                    with open(output_json, 'w', encoding='utf-8') as f:
                        json.dump(all_details, f, indent=2, ensure_ascii=False)
                    print(f"    💾 Progress saved ({i}/{len(ad_ids)})")
                except Exception as e:
                    print(f"    ⚠ Could not save progress: {e}")
            
            if i < len(ad_ids) and delay > 0:
                time.sleep(delay)
        
        # Step 3: Save final results
        print(f"\nSTEP 3: Saving final results...")
        try:
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(all_details, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved {len(all_details)} ad details to {output_json}")
        except Exception as e:
            print(f"✗ Error saving to JSON: {e}")
        
        # Summary
        print(f"\n{'='*80}")
        print(f"SCRAPING COMPLETE!")
        print(f"{'='*80}")
        print(f"Total ads scraped: {len(all_details)}")
        
        if download_assets:
            logos_count = sum(1 for d in all_details if d.get('logo_url'))
            videos_count = sum(1 for d in all_details if d.get('assets', {}).get('videos'))
            images_count = sum(1 for d in all_details if d.get('assets', {}).get('images'))
            
            print(f"Ads with logos: {logos_count}")
            print(f"Ads with videos: {videos_count}")
            print(f"Ads with images: {images_count}")
        
        print(f"{'='*80}\n")
        
        return all_details


def main():
    """Example usage"""
    scraper = LinkedInBeautifulSoupScraper()
    
    # Step 1: Fetch cookies using Selenium (run once, or when cookies expire)
    print("Fetching cookies...")
    scraper.fetch_cookies(account_owner="Nike", headless=True)
    
    # Step 2: Complete scraping workflow
    details = scraper.scrape_complete(
        account_owner="Nike",
        max_results=50,  # Limit for testing
        delay=2.0,  # 2 second delay between requests
        download_assets=True,
        assets_output_dir="downloaded_assets",
        output_json="nike_beautifulsoup_details.json"
    )
    
    if details:
        print("\nSample ad detail:")
        sample = details[0]
        print(f"  Ad ID: {sample.get('ad_id')}")
        print(f"  Advertiser: {sample.get('advertiser')}")
        print(f"  Ad Text (first 100 chars): {sample.get('ad_text', '')[:100]}...")
        print(f"  Videos: {len(sample.get('assets_local_paths', {}).get('videos', []))}")
        print(f"  Images: {len(sample.get('assets_local_paths', {}).get('images', []))}")


if __name__ == "__main__":
    main()

