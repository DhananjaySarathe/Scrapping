"""
LinkedIn Ad Library API Scraper with Auto-Fallback
Uses LinkedIn's pagination API endpoint for efficient scraping
Automatically falls back to HTML scraping if API requires authentication
Optional Selenium-based cookie extraction for automated sessions
"""

import requests
import json
import time
import os
import re
import hashlib
from typing import List, Dict, Optional, Tuple
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, urlencode

# Optional: Selenium for cookie extraction
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Note: Selenium not installed. Auto cookie extraction disabled. Install with: pip install selenium")

# Optional: for CSV export
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed. CSV export disabled. Install with: pip install pandas")


class LinkedInAPIScraper:
    """
    LinkedIn Ad Library Scraper using Pagination API
    Uses the searchPaginationFragment endpoint for efficient pagination
    
    Usage:
        scraper = LinkedInAPIScraper()
        # Set cookies (required for API access)
        scraper.set_cookies(li_at="your_li_at_cookie", jsessionid="your_jsessionid")
        details = scraper.scrape_complete("Nike", max_results=100, download_assets=True)
    """
    
    def __init__(self, use_selenium_for_cookies: bool = False):
        """
        Initialize scraper
        
        Args:
            use_selenium_for_cookies: If True, will use Selenium to extract cookies (requires login)
        """
        self.ua = UserAgent()
        self.pagination_api_url = "https://www.linkedin.com/ad-library/searchPaginationFragment"
        self.search_base_url = "https://www.linkedin.com/ad-library/search"
        self.detail_base_url = "https://www.linkedin.com"
        self.session = requests.Session()
        self._setup_headers()
        
        # Cookies and CSRF token
        self.li_at_cookie = None
        self.jsessionid_cookie = None
        self.csrf_token = None
        self.use_selenium = use_selenium_for_cookies and SELENIUM_AVAILABLE
        
        # Track seen assets globally to avoid duplicates
        self.seen_assets = {
            "logos": {},
            "images": {},
            "videos": {},
            "posters": {}
        }
        
        # Fallback mode (use HTML scraping if API fails)
        self.fallback_to_html = True
        
    def _setup_headers(self):
        """Setup request headers"""
        self.headers = {
            "Accept": "*/*",
            "User-Agent": self.ua.random,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.linkedin.com/ad-library/search",
            "Origin": "https://www.linkedin.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        self.session.headers.update(self.headers)
    
    def extract_cookies_with_selenium(self, linkedin_email: str = None, linkedin_password: str = None) -> bool:
        """
        Automatically extract cookies using Selenium (optional)
        
        Args:
            linkedin_email: LinkedIn email (if provided, will attempt login)
            linkedin_password: LinkedIn password (if provided, will attempt login)
            
        Returns:
            True if cookies extracted successfully, False otherwise
        """
        if not SELENIUM_AVAILABLE:
            print("⚠ Selenium not available. Cannot extract cookies automatically.")
            return False
        
        try:
            print("Attempting to extract cookies using Selenium...")
            driver = webdriver.Chrome()
            
            # Go to LinkedIn login page
            driver.get("https://www.linkedin.com/login")
            time.sleep(2)
            
            if linkedin_email and linkedin_password:
                # Try to login
                try:
                    email_field = driver.find_element(By.ID, "username")
                    password_field = driver.find_element(By.ID, "password")
                    email_field.send_keys(linkedin_email)
                    password_field.send_keys(linkedin_password)
                    driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
                    time.sleep(5)
                except Exception as e:
                    print(f"  ⚠ Could not auto-login: {e}")
                    print("  Please log in manually in the browser window...")
                    input("Press Enter after you've logged in...")
            else:
                print("  Please log in manually in the browser window...")
                input("Press Enter after you've logged in...")
            
            # Navigate to Ad Library to get proper cookies
            driver.get("https://www.linkedin.com/ad-library/")
            time.sleep(3)
            
            # Extract cookies
            cookies = driver.get_cookies()
            for cookie in cookies:
                if cookie['name'] == 'li_at':
                    self.li_at_cookie = cookie['value']
                    self.session.cookies.set("li_at", cookie['value'])
                elif cookie['name'] == 'JSESSIONID':
                    self.jsessionid_cookie = cookie['value']
                    self.session.cookies.set("JSESSIONID", cookie['value'])
            
            # Extract CSRF token from page
            try:
                page_source = driver.page_source
                csrf_match = re.search(r'"csrfToken":"([^"]+)"', page_source)
                if csrf_match:
                    self.csrf_token = csrf_match.group(1)
                    self.session.headers["csrf-token"] = self.csrf_token
            except Exception:
                pass
            
            driver.quit()
            
            if self.li_at_cookie:
                print("✓ Cookies extracted successfully!")
                return True
            else:
                print("⚠ Could not extract li_at cookie")
                return False
                
        except Exception as e:
            print(f"✗ Error extracting cookies: {e}")
            return False
    
    def set_cookies(self, li_at: str, jsessionid: str = None, csrf_token: str = None):
        """
        Set LinkedIn cookies and CSRF token manually
        
        Args:
            li_at: LinkedIn authentication cookie (required)
            jsessionid: JSESSIONID cookie (optional but recommended)
            csrf_token: CSRF token (optional, will try to extract if not provided)
        """
        self.li_at_cookie = li_at
        self.jsessionid_cookie = jsessionid
        
        # Set cookies in session
        self.session.cookies.set("li_at", li_at)
        if jsessionid:
            self.session.cookies.set("JSESSIONID", jsessionid)
        
        # Set CSRF token in headers if provided
        if csrf_token:
            self.csrf_token = csrf_token
            self.session.headers["csrf-token"] = csrf_token
        else:
            # Try to extract CSRF token from cookie or generate default format
            # LinkedIn CSRF tokens are usually in format: "ajax:numbers"
            self.csrf_token = f"ajax:{hashlib.md5(li_at.encode()).hexdigest()[:19]}"
            self.session.headers["csrf-token"] = self.csrf_token
        
        print("✓ Cookies and CSRF token set")
    
    def fetch_pagination_page(self, account_owner: str, pagination_token: Optional[str] = None, 
                             retry_count: int = 0) -> Optional[Dict]:
        """
        Fetch a single page from LinkedIn pagination API with retry logic
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            pagination_token: Token for next page (None for first page)
            retry_count: Number of retries attempted
            
        Returns:
            JSON response with html and paginationToken, or None if failed
        """
        params = {
            "accountOwner": account_owner
        }
        
        if pagination_token:
            params["paginationToken"] = pagination_token
        
        try:
            response = self.session.get(self.pagination_api_url, params=params, timeout=15)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except json.JSONDecodeError:
                    print(f"  ✗ Failed to parse JSON response")
                    print(f"  Response: {response.text[:200]}")
                    return None
            elif response.status_code == 429:
                # Rate limited - wait and retry
                if retry_count < 3:
                    wait_time = (2 ** retry_count) * 5  # Exponential backoff: 5s, 10s, 20s
                    print(f"  ⚠ Rate limited (429). Waiting {wait_time}s before retry {retry_count + 1}/3...")
                    time.sleep(wait_time)
                    return self.fetch_pagination_page(account_owner, pagination_token, retry_count + 1)
                else:
                    print(f"  ✗ Rate limited after {retry_count + 1} retries. Falling back to HTML scraping.")
                    return None
            elif response.status_code == 401:
                print(f"  ⚠ Authentication required (401). API method unavailable.")
                print(f"  → Falling back to HTML scraping method...")
                return None
            else:
                print(f"  ✗ API request failed: Status {response.status_code}")
                if response.status_code == 403:
                    print("  ⚠ Access forbidden. Falling back to HTML scraping...")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error fetching page: {e}")
            return None
    
    def extract_ads_from_html_fragment(self, html_fragment: str) -> List[Dict]:
        """
        Extract ad data from HTML fragment returned by API
        
        Args:
            html_fragment: HTML string from API response
            
        Returns:
            List of ad dictionaries
        """
        ads = []
        
        try:
            soup = BeautifulSoup(html_fragment, 'html.parser')
            
            # Look for ad cards - LinkedIn uses various class names
            ad_selectors = [
                '.ad-library-card',
                '[data-test-id*="ad"]',
                '.ad-card',
                '.ad-item',
                'article',
                'div[class*="ad"]',
            ]
            
            ad_elements = []
            for selector in ad_selectors:
                elements = soup.select(selector)
                if elements:
                    ad_elements = elements
                    break
            
            # If no specific selector works, try finding links to detail pages
            if not ad_elements:
                detail_links = soup.find_all('a', href=re.compile(r'/ad-library/detail/\d+'))
                if detail_links:
                    # Create ad objects from links
                    for link in detail_links:
                        ad_data = {
                            'text': link.get_text(strip=True),
                            'links': [link.get('href')]
                        }
                        # Try to find parent container
                        parent = link.find_parent(['div', 'article', 'section'])
                        if parent:
                            ad_data['text'] = parent.get_text(strip=True)
                        ads.append(ad_data)
                    return ads
            
            # Extract data from ad elements
            for element in ad_elements:
                ad_data = {}
                
                # Extract text content
                text = element.get_text(strip=True)
                if text:
                    ad_data['text'] = text
                
                # Extract links
                links = element.find_all('a', href=True)
                if links:
                    ad_data['links'] = [link.get('href') for link in links]
                
                # Extract images
                images = element.find_all('img')
                if images:
                    ad_data['images'] = [
                        img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                        for img in images if img.get('src') or img.get('data-src')
                    ]
                
                # Extract data attributes
                for attr, value in element.attrs.items():
                    if attr.startswith('data-'):
                        ad_data[attr] = value
                
                if ad_data:
                    ads.append(ad_data)
            
            return ads
            
        except Exception as e:
            print(f"  Error extracting ads from HTML fragment: {e}")
            return []
    
    def _extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """Extract JSON data from HTML response (for HTML fallback)"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            script_tags = soup.find_all('script', type='application/json')
            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if data and isinstance(data, dict):
                        return data
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
                r'window\.__APOLLO_STATE__\s*=\s*({.+?});',
                r'"elements"\s*:\s*(\[.+?\])',
                r'"results"\s*:\s*(\[.+?\])',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, (dict, list)) and data:
                            return data if isinstance(data, dict) else {"elements": data}
                    except json.JSONDecodeError:
                        continue
            
            return None
        except Exception:
            return None
    
    def _build_search_url(self, account_owner: str, start: int = 0) -> str:
        """Build search URL for HTML scraping fallback"""
        params = {
            "accountOwner": account_owner,
            "countries": "ALL"
        }
        if start > 0:
            params["start"] = str(start)
        query_string = urlencode(params, doseq=True)
        return f"{self.search_base_url}?{query_string}"
    
    def scrape_search_pages_html_fallback(self, account_owner: str, max_results: int = 100,
                                          results_per_page: int = 12, delay: float = 2.0) -> List[Dict]:
        """Fallback method: Scrape using HTML pages (no authentication needed)"""
        print(f"\n{'─'*80}")
        print(f"FALLBACK MODE: Using HTML scraping (no authentication required)")
        print(f"{'─'*80}\n")
        
        all_ads = []
        start = 0
        
        while len(all_ads) < max_results:
            url = self._build_search_url(account_owner, start)
            
            try:
                print(f"Fetching HTML page: start={start}...")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    # Try to extract JSON first
                    json_data = self._extract_json_from_html(response.text)
                    if json_data:
                        if isinstance(json_data, dict):
                            for key in ['elements', 'results', 'data', 'ads']:
                                if key in json_data and isinstance(json_data[key], list):
                                    ads = json_data[key]
                                    break
                            else:
                                ads = []
                        else:
                            ads = []
                    else:
                        # Parse HTML structure
                        ads = self.extract_ads_from_html_fragment(response.text)
                    
                    if ads:
                        print(f"  ✓ Extracted {len(ads)} ads")
                        ads_to_add = ads[:max_results - len(all_ads)]
                        all_ads.extend(ads_to_add)
                        print(f"  ✓ Total ads collected: {len(all_ads)}/{max_results}")
                    else:
                        print("  ⚠ No ads found, stopping")
                        break
                    
                    if len(ads) < results_per_page:
                        break
                    
                    start += results_per_page
                    if delay > 0:
                        time.sleep(delay)
                else:
                    print(f"  ✗ Request failed: Status {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"  ✗ Error: {e}")
                break
        
        return all_ads
    
    def scrape_search_pages_api(self, account_owner: str, max_results: int = 100,
                                delay: float = 1.0) -> List[Dict]:
        """
        Scrape all ads using pagination API
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            max_results: Maximum number of ads to scrape
            delay: Delay between requests in seconds
            
        Returns:
            List of ad dictionaries
        """
        if not self.li_at_cookie:
            print("⚠ Warning: No cookies set! Call set_cookies() first.")
            print("  API may return empty results without authentication.")
        
        all_ads = []
        pagination_token = None
        page_num = 1
        
        print(f"\n{'='*80}")
        print(f"SCRAPING VIA PAGINATION API")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        print(f"{'='*80}\n")
        
        while len(all_ads) < max_results:
            print(f"{'─'*80}")
            print(f"PAGE {page_num}")
            print(f"{'─'*80}")
            print(f"Fetching page with token: {pagination_token or 'None (first page)'}")
            
            # Fetch page from API
            data = self.fetch_pagination_page(account_owner, pagination_token)
            
            if not data:
                # API failed - try fallback to HTML scraping
                if self.fallback_to_html:
                    print(f"\n{'─'*80}")
                    print(f"API method unavailable. Switching to HTML scraping fallback...")
                    print(f"{'─'*80}\n")
                    return self.scrape_search_pages_html_fallback(account_owner, max_results, delay=delay)
                else:
                    print("  ✗ Failed to fetch page, stopping")
                    break
            
            # Extract HTML fragment
            html_fragment = data.get("html", "")
            if not html_fragment:
                print("  ✗ No HTML fragment in response, stopping")
                break
            
            # Extract ads from HTML fragment
            ads = self.extract_ads_from_html_fragment(html_fragment)
            
            if not ads:
                print("  ⚠ No ads found in this page")
                # Check if we've reached the end
                if not data.get("paginationToken"):
                    print("  ✓ Reached end of results")
                    break
            else:
                print(f"  ✓ Found {len(ads)} ads in this page")
                
                # Add ads to collection
                ads_to_add = ads[:max_results - len(all_ads)]
                all_ads.extend(ads_to_add)
                
                print(f"  ✓ Total ads collected: {len(all_ads)}/{max_results}")
            
            # Get next pagination token
            pagination_token = data.get("paginationToken")
            
            if not pagination_token:
                print("  ✓ No more pages available")
                break
            
            page_num += 1
            
            # Rate limiting
            if delay > 0 and len(all_ads) < max_results:
                time.sleep(delay)
        
        print(f"\n{'='*80}")
        print(f"API SCRAPING COMPLETE!")
        print(f"Total pages scraped: {page_num - 1}")
        print(f"Total ads collected: {len(all_ads)}")
        print(f"{'='*80}\n")
        
        return all_ads
    
    # ==================== Detail Page Scraping (reuse from file 9) ====================
    
    def _extract_ad_id_from_link(self, link: str) -> Optional[str]:
        """Extract ad ID from detail link"""
        try:
            match = re.search(r'/ad-library/detail/(\d+)', link)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None
    
    def _build_full_url(self, link: str) -> str:
        """Build full URL from relative link"""
        if link.startswith('http'):
            return link
        elif link.startswith('/'):
            return f"{self.detail_base_url}{link}"
        else:
            return f"{self.detail_base_url}/ad-library/detail/{link}"
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL by removing query parameters"""
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        except Exception:
            return url
    
    def _get_video_base_path(self, url: str) -> str:
        """Extract base path for video (removes quality indicators)"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            path = re.sub(r'/mp4-\d+p-\d+fp-[^/]+/', '/', path)
            path = re.sub(r'/mp4-\d+p/', '/', path)
            path = re.sub(r'-\d+p-', '-', path)
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        except Exception:
            return self._normalize_url(url)
    
    def _is_duplicate_asset(self, url: str, asset_type: str) -> Tuple[bool, Optional[str]]:
        """Check if asset is duplicate"""
        if asset_type == "video":
            base_path = self._get_video_base_path(url)
            if base_path in self.seen_assets["videos"]:
                return True, self.seen_assets["videos"][base_path]
        else:
            normalized = self._normalize_url(url)
            asset_key = asset_type + "s"
            if asset_key in self.seen_assets and normalized in self.seen_assets[asset_key]:
                return True, self.seen_assets[asset_key][normalized]
        return False, None
    
    def _extract_logo_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract logo URL from HTML"""
        try:
            logo_selectors = [
                'img[alt*="logo"]',
                'img[alt*="advertiser"]',
                'a[href*="company"] img',
                '.advertiser-logo img',
                'img[data-delayed-url*="logo"]',
            ]
            
            for selector in logo_selectors:
                img = soup.select_one(selector)
                if img:
                    logo_url = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                    if logo_url and logo_url.startswith('http'):
                        return unquote(logo_url.replace('&amp;', '&'))
            
            advertiser_links = soup.find_all('a', href=re.compile(r'/company/'))
            for link in advertiser_links:
                img = link.find('img')
                if img:
                    logo_url = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                    if logo_url and logo_url.startswith('http'):
                        return unquote(logo_url.replace('&amp;', '&'))
            return None
        except Exception as e:
            print(f"  Error extracting logo: {e}")
            return None
    
    def _extract_assets_from_html(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract images and videos from HTML"""
        assets = {
            "images": [],
            "videos": [],
            "posters": []
        }
        
        try:
            images = soup.find_all('img')
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                if src:
                    src = unquote(src.replace('&amp;', '&'))
                    if src.startswith('http') and 'logo' not in src.lower():
                        assets["images"].append(src)
            
            videos = soup.find_all('video')
            for video in videos:
                src = video.get('src') or video.get('data-src')
                if src and src.startswith('http'):
                    src = unquote(src.replace('&amp;', '&'))
                    assets["videos"].append(src)
                
                data_sources = video.get('data-sources')
                if data_sources:
                    try:
                        data_sources = unquote(data_sources.replace('&amp;', '&').replace('&quot;', '"'))
                        sources = json.loads(data_sources)
                        if isinstance(sources, list):
                            for source in sources:
                                if isinstance(source, dict) and 'src' in source:
                                    video_url = source['src']
                                    if video_url.startswith('http'):
                                        video_url = unquote(video_url)
                                        assets["videos"].append(video_url)
                    except (json.JSONDecodeError, AttributeError):
                        pass
                
                poster = video.get('data-poster-url') or video.get('poster')
                if poster and poster.startswith('http'):
                    poster = unquote(poster.replace('&amp;', '&'))
                    assets["posters"].append(poster)
            
            for elem in soup.find_all(attrs={'data-sources': True}):
                data_sources = elem.get('data-sources')
                if data_sources:
                    try:
                        data_sources = unquote(data_sources.replace('&amp;', '&').replace('&quot;', '"'))
                        sources = json.loads(data_sources)
                        if isinstance(sources, list):
                            for source in sources:
                                if isinstance(source, dict) and 'src' in source:
                                    url = source['src']
                                    if url.startswith('http'):
                                        url = unquote(url)
                                        assets["videos"].append(url)
                    except (json.JSONDecodeError, AttributeError):
                        pass
            
            # Deduplicate
            deduplicated = {"images": [], "videos": [], "posters": []}
            for key in ["images", "videos", "posters"]:
                seen = set()
                for url in assets[key]:
                    if key == "videos":
                        base = self._get_video_base_path(url)
                        if base not in seen:
                            seen.add(base)
                            deduplicated[key].append(url)
                    else:
                        normalized = self._normalize_url(url)
                        if normalized not in seen:
                            seen.add(normalized)
                            deduplicated[key].append(url)
            
            return deduplicated
            
        except Exception as e:
            print(f"  Error extracting assets: {e}")
            return assets
    
    def scrape_ad_detail(self, ad_id: str, link: str) -> Dict:
        """Scrape a single ad detail page"""
        url = self._build_full_url(link)
        
        ad_detail = {
            "ad_id": ad_id,
            "original_link": link,
            "detail_url": url,
            "advertiser": None,
            "ad_text": None,
            "ad_type": None,
            "call_to_action": None,
            "paid_for_by": None,
            "logo_url": None,
            "logo_local_path": None,
            "assets": {
                "images": [],
                "videos": [],
                "posters": []
            },
            "assets_local_paths": {
                "images": [],
                "videos": [],
                "posters": []
            },
            "metadata": {}
        }
        
        try:
            print(f"  Scraping ad ID: {ad_id}...")
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                advertiser_selectors = ['h1', 'h2', 'a[href*="/company/"]', '[data-test-id="advertiser-name"]']
                for selector in advertiser_selectors:
                    element = soup.select_one(selector)
                    if element:
                        text = element.get_text(strip=True)
                        if text and len(text) < 100 and text != "Ad Details":
                            ad_detail["advertiser"] = text
                            break
                
                content_selectors = ['.commentary__content', 'p.commentary__content', '.ad-content', 'p']
                ad_text_parts = []
                for selector in content_selectors:
                    elements = soup.select(selector)
                    for elem in elements[:3]:
                        text = elem.get_text(strip=True)
                        if text and 10 < len(text) < 500:
                            if not any(skip in text.lower() for skip in ['cookie', 'privacy', 'policy', 'about', 'linkedin corporation', 'please note']):
                                ad_text_parts.append(text)
                
                if ad_text_parts:
                    ad_detail["ad_text"] = "\n".join(ad_text_parts[:2])
                
                page_text = soup.get_text()
                ad_type_patterns = [
                    r'(Video Ad|Image Ad|Carousel Ad|Single Image Ad)',
                    r'Ad Type[:\s]+(\w+)',
                ]
                
                for pattern in ad_type_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        ad_detail["ad_type"] = match.group(1)
                        break
                
                cta_selectors = ['button[data-tracking-control-name*="cta"]', 'button', 'a[class*="button"]']
                ctas = []
                for selector in cta_selectors:
                    elements = soup.select(selector)
                    for elem in elements[:3]:
                        text = elem.get_text(strip=True)
                        href = elem.get('href', '')
                        if text and len(text) < 50 and text.lower() not in ['see more', '…see more']:
                            ctas.append({"text": text, "link": href})
                
                if ctas:
                    ad_detail["call_to_action"] = ctas
                
                paid_for_patterns = [
                    r'Paid for by[:\s]+(.+?)(?:\n|$)',
                    r'Paid for by[:\s]+(.+?)(?:\.|$)',
                ]
                
                for pattern in paid_for_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        ad_detail["paid_for_by"] = match.group(1).strip()
                        break
                
                logo_url = self._extract_logo_from_html(soup)
                if logo_url:
                    ad_detail["logo_url"] = logo_url
                
                assets = self._extract_assets_from_html(soup)
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
    
    # ==================== Asset Downloading ====================
    
    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """Determine file extension from URL"""
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        if '.jpg' in path or '.jpeg' in path:
            return '.jpg'
        elif '.png' in path:
            return '.png'
        elif '.gif' in path:
            return '.gif'
        elif '.webp' in path:
            return '.webp'
        elif '.mp4' in path:
            return '.mp4'
        elif '.webm' in path:
            return '.webm'
        elif '.mov' in path:
            return '.mov'
        
        if content_type:
            if 'image/jpeg' in content_type:
                return '.jpg'
            elif 'image/png' in content_type:
                return '.png'
            elif 'video/mp4' in content_type:
                return '.mp4'
        
        if 'video' in url.lower() or 'playlist' in url.lower():
            return '.mp4'
        elif 'logo' in url.lower() or 'image' in url.lower():
            return '.jpg'
        
        return '.bin'
    
    def _generate_filename(self, url: str, asset_type: str, ad_id: str, index: int = 0) -> str:
        """Generate filename for asset"""
        parsed = urlparse(url)
        path = parsed.path
        
        path_parts = [p for p in path.split('/') if p and p not in ['dms', 'image', 'v2', 'playlist', 'vid']]
        
        if path_parts:
            base_name = path_parts[-1]
            base_name = re.sub(r'[^a-zA-Z0-9_-]', '_', base_name)
            base_name = base_name[:50]
        else:
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            base_name = f"{asset_type}_{url_hash}"
        
        ext = self._get_file_extension(url)
        return f"{ad_id}_{base_name}_{index}{ext}"
    
    def _download_asset(self, url: str, output_path: str) -> bool:
        """Download a single asset"""
        try:
            head_response = self.session.head(url, timeout=10, allow_redirects=True)
            
            if head_response.status_code == 200:
                response = self.session.get(url, timeout=30, stream=True, allow_redirects=True)
                
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
    
    def _download_ad_assets_with_dedup(self, ad_id: str, logo_url: Optional[str],
                                      assets: Dict[str, List[str]],
                                      output_dir: str) -> Dict[str, List[str]]:
        """Download assets with duplicate detection"""
        downloaded = {
            "logo": None,
            "images": [],
            "videos": [],
            "posters": []
        }
        
        ad_dir = os.path.join(output_dir, ad_id)
        
        if logo_url:
            is_dup, existing_path = self._is_duplicate_asset(logo_url, "logo")
            if is_dup and existing_path:
                downloaded["logo"] = existing_path
                print(f"    ✓ Logo (reused): {os.path.basename(existing_path)}")
            else:
                logo_filename = self._generate_filename(logo_url, "logo", ad_id, 0)
                logo_path = os.path.join(ad_dir, "logo", logo_filename)
                if self._download_asset(logo_url, logo_path):
                    downloaded["logo"] = logo_path
                    normalized = self._normalize_url(logo_url)
                    self.seen_assets["logos"][normalized] = logo_path
                    print(f"    ✓ Logo downloaded")
        
        if assets.get("images"):
            for i, img_url in enumerate(assets["images"], 1):
                is_dup, existing_path = self._is_duplicate_asset(img_url, "image")
                if is_dup and existing_path:
                    downloaded["images"].append(existing_path)
                else:
                    img_filename = self._generate_filename(img_url, "image", ad_id, i)
                    img_path = os.path.join(ad_dir, "images", img_filename)
                    if self._download_asset(img_url, img_path):
                        downloaded["images"].append(img_path)
                        normalized = self._normalize_url(img_url)
                        self.seen_assets["images"][normalized] = img_path
        
        if assets.get("videos"):
            video_groups = {}
            for video_url in assets["videos"]:
                base_path = self._get_video_base_path(video_url)
                if base_path not in video_groups:
                    video_groups[base_path] = []
                video_groups[base_path].append(video_url)
            
            for base_path, video_urls in video_groups.items():
                is_dup, existing_path = self._is_duplicate_asset(video_urls[0], "video")
                if is_dup and existing_path:
                    downloaded["videos"].append(existing_path)
                    print(f"    ✓ Video (reused): {os.path.basename(existing_path)}")
                else:
                    best_url = max(video_urls, key=lambda x: max([int(m) for m in re.findall(r'(\d+)p', x)] + [0]))
                    video_filename = self._generate_filename(best_url, "video", ad_id, len(downloaded["videos"]) + 1)
                    video_path = os.path.join(ad_dir, "videos", video_filename)
                    if self._download_asset(best_url, video_path):
                        downloaded["videos"].append(video_path)
                        self.seen_assets["videos"][base_path] = video_path
                        quality = max([int(m) for m in re.findall(r'(\d+)p', best_url)] + [0])
                        print(f"    ✓ Video downloaded (quality: {quality}p)")
        
        if assets.get("posters"):
            for i, poster_url in enumerate(assets["posters"], 1):
                is_dup, existing_path = self._is_duplicate_asset(poster_url, "poster")
                if is_dup and existing_path:
                    downloaded["posters"].append(existing_path)
                else:
                    poster_filename = self._generate_filename(poster_url, "poster", ad_id, i)
                    poster_path = os.path.join(ad_dir, "posters", poster_filename)
                    if self._download_asset(poster_url, poster_path):
                        downloaded["posters"].append(poster_path)
                        normalized = self._normalize_url(poster_url)
                        self.seen_assets["posters"][normalized] = poster_path
        
        return downloaded
    
    # ==================== Complete Workflow ====================
    
    def extract_detail_links(self, ads: List[Dict]) -> List[Dict]:
        """Extract detail links from ads list"""
        detail_links = []
        
        for i, ad in enumerate(ads):
            if isinstance(ad, dict) and 'links' in ad:
                links = ad['links']
                if isinstance(links, list) and len(links) > 0:
                    for link in links:
                        if isinstance(link, str) and '/ad-library/detail/' in link:
                            ad_id = self._extract_ad_id_from_link(link)
                            if ad_id:
                                detail_links.append({
                                    "index": i,
                                    "ad_id": ad_id,
                                    "link": link,
                                    "original_ad": ad
                                })
                                break
        
        return detail_links
    
    def scrape_complete(self, account_owner: str, max_results: int = 100,
                       delay: float = 1.0, download_assets: bool = True,
                       assets_output_dir: str = "downloaded_assets",
                       save_intermediate: bool = False,
                       intermediate_json: str = "intermediate_ads.json",
                       output_json: str = "complete_ad_details.json",
                       try_api_first: bool = True) -> List[Dict]:
        """
        Complete scraping workflow using pagination API
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            max_results: Maximum number of ads to scrape
            delay: Delay between requests in seconds
            download_assets: Whether to download assets
            assets_output_dir: Directory to save downloaded assets
            save_intermediate: Save search results to intermediate JSON
            intermediate_json: Filename for intermediate results
            output_json: Filename for final results
            
        Returns:
            List of complete ad detail dictionaries
        """
        print(f"\n{'='*80}")
        print(f"COMPLETE LINKEDIN AD SCRAPING")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        if download_assets:
            print(f"Assets Directory: {assets_output_dir}/")
        print(f"{'='*80}\n")
        
        # Step 1: Scrape search pages
        if try_api_first and self.li_at_cookie:
            print("STEP 1: Scraping search pages via API...")
            ads = self.scrape_search_pages_api(
                account_owner=account_owner,
                max_results=max_results,
                delay=delay
            )
        else:
            print("STEP 1: Scraping search pages via HTML (no authentication)...")
            ads = self.scrape_search_pages_html_fallback(
                account_owner=account_owner,
                max_results=max_results,
                delay=delay
            )
        
        if not ads:
            print("No ads found")
            return []
        
        # Save intermediate results if requested
        if save_intermediate:
            try:
                with open(intermediate_json, 'w', encoding='utf-8') as f:
                    json.dump(ads, f, indent=2, ensure_ascii=False)
                print(f"\n✓ Saved intermediate results to {intermediate_json}")
            except Exception as e:
                print(f"Error saving intermediate results: {e}")
        
        # Step 2: Extract detail links
        print(f"\nSTEP 2: Extracting detail links...")
        detail_links = self.extract_detail_links(ads)
        print(f"✓ Found {len(detail_links)} detail links")
        
        if not detail_links:
            print("No detail links found")
            return []
        
        # Step 3: Scrape detail pages
        print(f"\nSTEP 3: Scraping detail pages...")
        all_details = []
        
        for i, link_info in enumerate(detail_links, 1):
            print(f"[{i}/{len(detail_links)}] ", end="")
            
            detail = self.scrape_ad_detail(link_info["ad_id"], link_info["link"])
            
            # Step 4: Download assets with deduplication
            if download_assets:
                print(f"    Downloading assets...")
                downloaded = self._download_ad_assets_with_dedup(
                    ad_id=link_info["ad_id"],
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
            
            detail["original_ad_index"] = link_info["index"]
            all_details.append(detail)
            
            if i < len(detail_links) and delay > 0:
                time.sleep(delay)
        
        # Step 5: Save final results
        print(f"\nSTEP 4: Saving results...")
        try:
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(all_details, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved {len(all_details)} ad details to {output_json}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
        
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
            
            total_videos_before = sum(len(d.get('assets', {}).get('videos', [])) for d in all_details)
            total_videos_after = sum(len(d.get('assets_local_paths', {}).get('videos', [])) for d in all_details)
            if total_videos_before > total_videos_after:
                print(f"Videos deduplicated: {total_videos_before - total_videos_after} duplicates removed")
        
        print(f"{'='*80}\n")
        
        return all_details


def main():
    """Example usage"""
    scraper = LinkedInAPIScraper(use_selenium_for_cookies=False)
    
    # OPTION 1: Use without cookies (will fallback to HTML scraping)
    # This works for public pages without authentication
    print("Running without authentication - will use HTML scraping fallback\n")
    
    # OPTION 2: Extract cookies automatically with Selenium (if you have credentials)
    # scraper.extract_cookies_with_selenium(
    #     linkedin_email="your_email@example.com",
    #     linkedin_password="your_password"
    # )
    
    # OPTION 3: Set cookies manually (if you have them)
    # scraper.set_cookies(
    #     li_at="YOUR_LI_AT_COOKIE_HERE",
    #     jsessionid=None,
    #     csrf_token=None
    # )
    
    # Complete scraping workflow
    # Will automatically fallback to HTML scraping if API fails
    details = scraper.scrape_complete(
        account_owner="Nike",
        max_results=50,  # Limit for testing
        delay=2.0,  # 2 second delay between requests
        download_assets=True,
        assets_output_dir="downloaded_assets",
        save_intermediate=True,
        intermediate_json="nike_ads_api_intermediate.json",
        output_json="nike_complete_api_details.json",
        try_api_first=False  # Set to False to skip API and use HTML directly
    )
    
    if details:
        print("\nSample ad detail:")
        sample = details[0]
        print(f"  Ad ID: {sample.get('ad_id')}")
        print(f"  Advertiser: {sample.get('advertiser')}")
        print(f"  Ad Type: {sample.get('ad_type')}")
        print(f"  Logo: {'Yes' if sample.get('logo_url') else 'No'}")
        print(f"  Videos: {len(sample.get('assets_local_paths', {}).get('videos', []))}")
        print(f"  Images: {len(sample.get('assets_local_paths', {}).get('images', []))}")


if __name__ == "__main__":
    main()

