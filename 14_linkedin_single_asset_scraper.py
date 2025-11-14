"""
LinkedIn Ad Library Scraper - Single Asset Download
Downloads logo + one asset per ad only
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

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False


class LinkedInSingleAssetScraper:
    
    def __init__(self, cookies_file: str = "cookies.json", proxies: List[Dict[str, str]] = None):
        self.cookies_file = cookies_file
        self.proxies = proxies or []
        self.current_proxy_index = 0
        
        self.api_url = "https://www.linkedin.com/ad-library/searchPaginationFragment"
        self.search_base_url = "https://www.linkedin.com/ad-library/search"
        self.detail_base_url = "https://www.linkedin.com/ad-library/detail"
        
        self.session = requests.Session()
        self._setup_headers()
    
    def _setup_headers(self):
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
    
    def _get_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxies:
            return None
        
        proxy = self.proxies[self.current_proxy_index % len(self.proxies)]
        self.current_proxy_index += 1
        
        if isinstance(proxy, dict):
            return proxy
        elif isinstance(proxy, str):
            if proxy.startswith('http://') or proxy.startswith('https://'):
                return {"http": proxy, "https": proxy}
            elif '://' not in proxy:
                return {"http": f"http://{proxy}", "https": f"https://{proxy}"}
            else:
                return {"http": proxy, "https": proxy}
        
        return None
    
    def _update_headers_with_csrf(self):
        cookies = self.load_cookies()
        if cookies and "JSESSIONID" in cookies:
            jsessionid = cookies["JSESSIONID"]
            if jsessionid.startswith("ajax:"):
                self.session.headers["csrf-token"] = jsessionid
    
    def _normalize_pagination_token(self, token: Optional[str]) -> Tuple[str, Optional[str]]:
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
        
        if tokens_found:
            offset_tokens = [t for t in tokens_found if '#' in t]
            api_tokens = [t for t in tokens_found if '#' not in t]
            
            if offset_tokens:
                return offset_tokens[0]
            elif api_tokens:
                return api_tokens[0]
        
        return None
    
    def fetch_cookies(self, account_owner: str = "Nike", headless: bool = True) -> bool:
        if not SELENIUM_AVAILABLE:
            print("✗ Selenium not available. Cannot fetch cookies.")
            print("  Install with: pip install selenium")
            return False
        
        print("=" * 80)
        print("FETCHING LINKEDIN COOKIES USING SELENIUM")
        print("=" * 80)
        print(f"Advertiser: {account_owner}")
        print(f"Headless mode: {headless}")
        print("=" * 80)
        
        try:
            options = Options()
            if headless:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            
            if self.proxies:
                proxy = self._get_proxy()
                if proxy:
                    proxy_url = proxy.get('http', '').replace('http://', '').replace('https://', '')
                    if proxy_url:
                        print(f"Using proxy: {proxy_url[:50]}...")
                        options.add_argument(f"--proxy-server={proxy_url}")
            
            print("Initializing Chrome driver...")
            driver = webdriver.Chrome(options=options)
            
            url = f"https://www.linkedin.com/ad-library/search?accountOwner={account_owner}"
            print(f"Loading URL: {url}")
            driver.get(url)
            
            print("Waiting 8 seconds for LinkedIn to set cookies...")
            time.sleep(8)
            
            # Check page title/URL to see if we're on the right page
            current_url = driver.current_url
            page_title = driver.title
            print(f"Current URL: {current_url}")
            print(f"Page Title: {page_title[:100]}")
            
            # Check if we're redirected to login page
            if "login" in current_url.lower() or "authwall" in current_url.lower():
                print("⚠ WARNING: Redirected to login page!")
                print("  LinkedIn may require login. Try running in non-headless mode.")
            
            try:
                print("Scrolling page to trigger cookie setting...")
                driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(2)
            except Exception as e:
                print(f"  ⚠ Could not scroll: {e}")
            
            print("Extracting cookies...")
            cookies = driver.get_cookies()
            
            print(f"Found {len(cookies)} cookies")
            
            if len(cookies) == 0:
                print("⚠ WARNING: No cookies found!")
                print("  Possible reasons:")
                print("  1. LinkedIn blocked the request")
                print("  2. Page didn't load properly")
                print("  3. Need to wait longer for cookies")
                print("  4. LinkedIn requires login")
                print("\n  Try:")
                print("  - Running in non-headless mode: fetch_cookies(headless=False)")
                print("  - Checking if LinkedIn is accessible")
                print("  - Waiting longer for page to load")
            else:
                # Check for important cookies
                cookie_names = {c["name"] for c in cookies}
                important_cookies = ["lang", "JSESSIONID", "lidc", "bcookie", "bscookie"]
                missing = [name for name in important_cookies if name not in cookie_names]
                
                if missing:
                    print(f"⚠ Warning: Missing important cookies: {missing}")
                
                print(f"✓ Cookie names found: {', '.join(sorted(cookie_names)[:10])}")
                if len(cookie_names) > 10:
                    print(f"  ... and {len(cookie_names) - 10} more")
            
            print(f"Saving cookies to {self.cookies_file}...")
            with open(self.cookies_file, "w") as f:
                json.dump(cookies, f, indent=2)
            
            driver.quit()
            
            if len(cookies) > 0:
                print(f"✓ Cookies saved successfully ({len(cookies)} cookies)")
                print("=" * 80)
                return True
            else:
                print("✗ Failed: No cookies to save")
                print("=" * 80)
                return False
            
        except Exception as e:
            print(f"✗ Error fetching cookies: {e}")
            import traceback
            print("Full error traceback:")
            traceback.print_exc()
            print("=" * 80)
            return False
    
    def load_cookies(self) -> Dict[str, str]:
        try:
            with open(self.cookies_file, "r") as f:
                raw_cookies = json.load(f)
            
            if not raw_cookies:
                print(f"⚠ cookies.json is empty (no cookies found)")
                return {}
            
            if not isinstance(raw_cookies, list):
                print(f"⚠ cookies.json has invalid format (expected list, got {type(raw_cookies)})")
                return {}
            
            cookies_dict = {}
            for cookie in raw_cookies:
                if isinstance(cookie, dict) and "name" in cookie and "value" in cookie:
                    cookies_dict[cookie["name"]] = cookie["value"]
                else:
                    print(f"⚠ Invalid cookie format: {cookie}")
            
            if cookies_dict:
                print(f"✓ Loaded {len(cookies_dict)} cookies from {self.cookies_file}")
            else:
                print(f"⚠ No valid cookies found in {self.cookies_file}")
            
            return cookies_dict
            
        except FileNotFoundError:
            print(f"✗ Cookies file not found: {self.cookies_file}")
            print("  Run fetch_cookies() first to generate cookies.")
            return {}
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing cookies.json: {e}")
            return {}
        except Exception as e:
            print(f"✗ Error loading cookies: {e}")
            return {}
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        proxy = self._get_proxy()
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                kwargs['proxies'] = proxy
                kwargs['timeout'] = kwargs.get('timeout', 15)
                
                if method.upper() == 'GET':
                    response = self.session.get(url, **kwargs)
                elif method.upper() == 'POST':
                    response = self.session.post(url, **kwargs)
                else:
                    return None
                
                if response.status_code == 200:
                    return response
                elif response.status_code in (429, 503):
                    wait_time = (attempt + 1) * 2
                    time.sleep(wait_time)
                    continue
                else:
                    return None
                    
            except requests.exceptions.ProxyError:
                if attempt < max_retries - 1:
                    proxy = self._get_proxy()
                    continue
                return None
            except requests.exceptions.RequestException:
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None
        
        return None
    
    def fetch_api_page(self, account_owner: str, pagination_token: Optional[str] = None) -> Optional[Dict]:
        params = {"accountOwner": account_owner}
        if pagination_token:
            params["paginationToken"] = pagination_token
        
        cookies = self.load_cookies()
        if not cookies:
            print(f"  ✗ No cookies available")
            return None
        
        self._update_headers_with_csrf()
        
        response = self._make_request('GET', self.api_url, params=params, cookies=cookies)
        
        if not response:
            print(f"  ✗ API request failed")
            return None
        
        try:
            data = response.json()
            if isinstance(data, dict) and ('html' in data or 'paginationToken' in data):
                return data
        except json.JSONDecodeError:
            pass
        
        if 'text/html' in response.headers.get('Content-Type', '').lower():
            html_content = response.text
            pagination_token = self.extract_next_token_from_html(html_content)
            return {"html": html_content, "paginationToken": pagination_token}
        
        try:
            return response.json()
        except json.JSONDecodeError:
            print(f"  ✗ Failed to parse JSON response")
            return None
    
    def fetch_offset_page(self, account_owner: str, offset: int = 0) -> Optional[Dict]:
        url = self.search_base_url
        params = {"accountOwner": account_owner, "countries": "ALL", "start": str(offset)}
        
        cookies = self.load_cookies()
        if not cookies:
            print(f"  ✗ No cookies available")
            return None
        
        self._update_headers_with_csrf()
        
        response = self._make_request('GET', url, params=params, cookies=cookies)
        
        if not response:
            print(f"  ✗ Offset page request failed")
            return None
        
        if response.status_code != 200:
            print(f"  ✗ Offset page request failed: Status {response.status_code}")
            return None
        
        html = response.text
        next_token = self.extract_next_token_from_html(html)
        return {"html": html, "paginationToken": next_token}
    
    def extract_ad_ids_from_html(self, html_fragment: str) -> List[str]:
        ad_ids = []
        pattern = r'/ad-library/detail/(\d+)'
        matches = re.findall(pattern, html_fragment)
        
        seen = set()
        for ad_id in matches:
            if ad_id not in seen:
                seen.add(ad_id)
                ad_ids.append(ad_id)
        
        return ad_ids
    
    def scrape_search_pages(self, account_owner: str, max_results: int = 100, delay: float = 1.0) -> List[str]:
        print(f"\n{'='*80}")
        print(f"SCRAPING AD IDs VIA PAGINATION")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        print(f"{'='*80}\n")
        
        all_ad_ids = []
        pagination_token = None
        page_num = 1
        seen_tokens = set()
        
        while len(all_ad_ids) < max_results:
            print(f"{'─'*80}")
            print(f"📄 PAGE {page_num} - Starting new page")
            print(f"{'─'*80}")
            print(f"Fetching with token: {pagination_token or 'None (first page)'}")
            
            if pagination_token and pagination_token in seen_tokens:
                print(f"  ⚠ Warning: Token already seen! Possible infinite loop.")
                print(f"  → Stopping to prevent infinite loop")
                break
            
            if pagination_token:
                seen_tokens.add(pagination_token)
            
            mode, value = self._normalize_pagination_token(pagination_token)
            print(f"  → Using {mode} pagination method")
            
            if mode == "api":
                data = self.fetch_api_page(account_owner, value)
            elif mode == "offset":
                print(f"  → Offset value: {value}")
                data = self.fetch_offset_page(account_owner, offset=value)
            else:
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
                    if pagination_token and pagination_token in seen_tokens:
                        print(f"  → Stopping: no new ads and token already seen")
                        break
                else:
                    print(f"  ✓ Added {ads_added} new ads")
                
                print(f"  ✓ Total ad IDs collected: {len(all_ad_ids)}/{max_results}")
            
            next_token = data.get("paginationToken")
            
            if next_token == pagination_token:
                print(f"  ⚠ Next token is same as current token. Reached end or stuck.")
                break
            
            pagination_token = next_token
            
            if not pagination_token:
                print("  ✓ No more pages available")
                break
            
            page_num += 1
            
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
    
    def scrape_ad_detail_with_bs4(self, ad_id: str) -> Dict:
        url = f"{self.detail_base_url}/{ad_id}"
        cookies = self.load_cookies()
        
        print(f"  Scraping ad ID: {ad_id}...")
        
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
            response = self._make_request('GET', url, cookies=cookies)
            
            if not response:
                print(f"  ✗ Failed: No response")
                ad_detail["error"] = "No response"
                return ad_detail
            
            if response.status_code != 200:
                print(f"  ✗ Failed: Status code {response.status_code}")
                ad_detail["error"] = f"HTTP {response.status_code}"
                return ad_detail
            
            print(f"  ✓ Page loaded successfully")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            advertiser_selectors = [
                'h1', 'h2', 'a[href*="/company/"]',
                '[data-test-id="advertiser-name"]',
                '.advertiser-name', 'span[class*="advertiser"]',
            ]
            
            for selector in advertiser_selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text and len(text) < 100 and text.lower() not in ['ad details', 'ad detail']:
                        ad_detail["advertiser"] = text
                        break
            
            content_selectors = [
                '.commentary__content', 'p.commentary__content',
                '.ad-content', '.ad-text',
                '[class*="commentary"]', '[class*="content"]', 'p',
            ]
            
            ad_text_parts = []
            seen_texts = set()
            
            for selector in content_selectors:
                elements = soup.select(selector)
                for elem in elements[:10]:
                    text = elem.get_text(strip=True)
                    if (text and 10 < len(text) < 2000 and text not in seen_texts and
                        not any(skip in text.lower() for skip in [
                            'cookie', 'privacy', 'policy', 'about',
                            'linkedin corporation', 'please note',
                            'terms of service', 'ad details',
                            'view details', 'see more', '…see more',
                            'sign in', 'sign up', 'join now'
                        ])):
                        seen_texts.add(text)
                        ad_text_parts.append(text)
            
            if ad_text_parts:
                unique_texts = []
                for text in ad_text_parts:
                    is_duplicate = False
                    for existing in unique_texts:
                        if text in existing or existing in text:
                            is_duplicate = True
                            break
                    if not is_duplicate:
                        unique_texts.append(text)
                
                ad_detail["ad_text"] = "\n\n".join(unique_texts[:5])
            
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
            
            if not ad_detail["ad_type"]:
                assets = ad_detail["assets"]
                if assets.get("videos"):
                    ad_detail["ad_type"] = "Video Ad"
                elif len(assets.get("images", [])) > 1:
                    ad_detail["ad_type"] = "Carousel Ad"
                elif assets.get("images"):
                    ad_detail["ad_type"] = "Image Ad"
            
            cta_selectors = [
                'button[data-tracking-control-name*="cta"]',
                'a[class*="cta"]', 'button', 'a[class*="button"]',
            ]
            
            ctas = []
            for selector in cta_selectors:
                elements = soup.select(selector)
                for elem in elements[:5]:
                    text = elem.get_text(strip=True)
                    href = elem.get('href', '')
                    if (text and len(text) < 100 and
                        text.lower() not in ['see more', '…see more', 'view details', 'sign in']):
                        ctas.append({"text": text, "link": href})
            
            if ctas:
                ad_detail["call_to_action"] = ctas[:3]
            
            paid_for_patterns = [
                r'Paid for by[:\s]+(.+?)(?:\n|$)',
                r'Paid for by[:\s]+(.+?)(?:\.|$)',
            ]
            
            for pattern in paid_for_patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    ad_detail["paid_for_by"] = match.group(1).strip()
                    break
            
            logo_url = self._extract_logo_with_bs4(soup)
            if logo_url:
                ad_detail["logo_url"] = logo_url
            
            assets = self._extract_assets_with_bs4(soup)
            ad_detail["assets"] = assets
            
            print(f"  ✓ Extracted: {len(assets.get('images', []))} images, {len(assets.get('videos', []))} videos")
            print(f"  ✓ Successfully scraped ad ID: {ad_id}")
            
            return ad_detail
            
        except Exception as e:
            print(f"  ✗ Error parsing: {e}")
            ad_detail["error"] = str(e)
            return ad_detail
    
    def _extract_logo_with_bs4(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            logo_selectors = [
                'img[alt*="logo" i]', 'img[alt*="advertiser" i]',
                'a[href*="company"] img', '.advertiser-logo img',
                'img[data-delayed-url*="logo" i]', 'img[src*="logo" i]',
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
        except Exception:
            return None
    
    def _extract_assets_with_bs4(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        assets = {
            "images": [],
            "videos": [],
            "posters": []
        }
        
        try:
            images = soup.find_all('img')
            seen_images = set()
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                if src:
                    src = unquote(src.replace('&amp;', '&'))
                    if (src.startswith('http') and 'logo' not in src.lower() and src not in seen_images):
                        seen_images.add(src)
                        assets["images"].append(src)
            
            videos = soup.find_all('video')
            video_urls = []
            
            for video in videos:
                src = video.get('src') or video.get('data-src')
                if src and src.startswith('http'):
                    video_urls.append(unquote(src.replace('&amp;', '&')))
                
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
            
            video_groups = {}
            for url in video_urls:
                base_path = self._get_video_base_path(url)
                if base_path not in video_groups:
                    video_groups[base_path] = []
                video_groups[base_path].append(url)
            
            for base_path, urls in video_groups.items():
                best_url = max(urls, key=lambda x: max([int(m) for m in re.findall(r'(\d+)p', x)] + [0]))
                assets["videos"].append(best_url)
            
            return assets
            
        except Exception:
            return assets
    
    def _get_video_base_path(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            path = parsed.path
            path = re.sub(r'/mp4-\d+p-\d+fp-[^/]+/', '/', path)
            path = re.sub(r'/mp4-\d+p/', '/', path)
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        except:
            return url
    
    def _get_file_extension(self, url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        if '.jpg' in path or '.jpeg' in path:
            return '.jpg'
        elif '.png' in path:
            return '.png'
        elif '.gif' in path:
            return '.gif'
        elif '.mp4' in path:
            return '.mp4'
        elif '.webm' in path:
            return '.webm'
        elif 'video' in path or 'playlist' in path:
            return '.mp4'
        else:
            return '.jpg'
    
    def _generate_filename(self, url: str, asset_type: str, ad_id: str, index: int = 0) -> str:
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
        cookies = self.load_cookies()
        proxy = self._get_proxy()
        
        try:
            response = self.session.get(
                url, cookies=cookies, proxies=proxy,
                timeout=30, stream=True, allow_redirects=True
            )
            
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
    
    def _get_single_asset(self, assets: Dict[str, List[str]]) -> Optional[Tuple[str, str]]:
        """
        Get single asset from assets dict
        Priority: videos > images > posters
        Returns: (asset_type, asset_url) or None
        """
        if assets.get("videos"):
            return ("video", assets["videos"][0])
        elif assets.get("images"):
            return ("image", assets["images"][0])
        elif assets.get("posters"):
            return ("poster", assets["posters"][0])
        
        return None
    
    def _download_ad_assets(self, ad_id: str, logo_url: Optional[str],
                           assets: Dict[str, List[str]],
                           output_dir: str) -> Dict[str, Optional[str]]:
        downloaded = {
            "logo_path": None,
            "asset_path": None,
            "asset_type": None
        }
        
        ad_dir = os.path.join(output_dir, ad_id)
        
        if logo_url:
            print(f"    Downloading logo...")
            logo_filename = self._generate_filename(logo_url, "logo", ad_id, 0)
            logo_path = os.path.join(ad_dir, "logo", logo_filename)
            if self._download_asset(logo_url, logo_path):
                downloaded["logo_path"] = logo_path
                print(f"    ✓ Logo downloaded")
            else:
                print(f"    ✗ Logo download failed")
        
        single_asset = self._get_single_asset(assets)
        
        if single_asset:
            asset_type, asset_url = single_asset
            print(f"    Downloading {asset_type} asset...")
            asset_filename = self._generate_filename(asset_url, asset_type, ad_id, 0)
            asset_path = os.path.join(ad_dir, asset_type, asset_filename)
            
            if self._download_asset(asset_url, asset_path):
                downloaded["asset_path"] = asset_path
                downloaded["asset_type"] = asset_type
                print(f"    ✓ {asset_type.capitalize()} downloaded")
            else:
                print(f"    ✗ {asset_type.capitalize()} download failed")
        else:
            print(f"    ⚠ No assets found, skipping asset download")
        
        return downloaded
    
    def scrape_complete(self, account_owner: str, max_results: int = 100,
                       delay: float = 2.0, download_assets: bool = True,
                       assets_output_dir: str = "downloaded_assets",
                       output_json: str = "complete_ad_details.json") -> List[Dict]:
        
        print(f"\n{'='*80}")
        print(f"COMPLETE LINKEDIN AD SCRAPING (Single Asset)")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        if download_assets:
            print(f"Assets Directory: {assets_output_dir}/")
        print(f"{'='*80}\n")
        
        cookies = self.load_cookies()
        if not cookies:
            print("⚠ No cookies found. Fetching cookies using Selenium...")
            if not self.fetch_cookies(account_owner):
                print("✗ Failed to fetch cookies. Cannot proceed.")
                return []
            print("✓ Cookies fetched successfully")
        
        print("STEP 1: Scraping search pages to get ad IDs...")
        ad_ids = self.scrape_search_pages(
            account_owner=account_owner,
            max_results=max_results,
            delay=delay
        )
        
        if not ad_ids:
            print("✗ No ad IDs found")
            return []
        
        print(f"\nSTEP 2: Scraping detail pages using BeautifulSoup...")
        print(f"Total ads to scrape: {len(ad_ids)}\n")
        all_details = []
        
        for i, ad_id in enumerate(ad_ids, 1):
            print(f"[{i}/{len(ad_ids)}] ", end="")
            
            detail = self.scrape_ad_detail_with_bs4(ad_id)
            
            if download_assets:
                print(f"    Downloading assets...")
                downloaded = self._download_ad_assets(
                    ad_id=ad_id,
                    logo_url=detail.get("logo_url"),
                    assets=detail.get("assets", {}),
                    output_dir=assets_output_dir
                )
                
                detail["logo_local_path"] = downloaded["logo_path"]
                
                if downloaded["asset_path"]:
                    detail["asset_local_path"] = downloaded["asset_path"]
                    detail["asset_type"] = downloaded["asset_type"]
                else:
                    detail["asset_local_path"] = None
                    detail["asset_type"] = None
            
            all_details.append(detail)
            
            if i % 10 == 0:
                try:
                    with open(output_json, 'w', encoding='utf-8') as f:
                        json.dump(all_details, f, indent=2, ensure_ascii=False)
                    print(f"    💾 Progress saved ({i}/{len(ad_ids)})")
                except Exception as e:
                    print(f"    ⚠ Could not save progress: {e}")
            
            if i < len(ad_ids) and delay > 0:
                time.sleep(delay)
        
        print(f"\nSTEP 3: Saving final results...")
        try:
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(all_details, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved {len(all_details)} ad details to {output_json}")
        except Exception as e:
            print(f"✗ Error saving to JSON: {e}")
        
        print(f"\n{'='*80}")
        print(f"SCRAPING COMPLETE!")
        print(f"{'='*80}")
        print(f"Total ads scraped: {len(all_details)}")
        
        if download_assets:
            logos_count = sum(1 for d in all_details if d.get('logo_local_path'))
            assets_count = sum(1 for d in all_details if d.get('asset_local_path'))
            
            print(f"Ads with logos: {logos_count}")
            print(f"Ads with assets: {assets_count}")
        
        print(f"{'='*80}\n")
        
        return all_details


def main():
    proxies = [
    #   {proxy_url}
    ]
    
    scraper = LinkedInSingleAssetScraper(proxies=proxies)
    
    scraper.fetch_cookies(account_owner="Nike", headless=True)
    
    details = scraper.scrape_complete(
        account_owner="Nike",
        max_results=50,
        delay=2.0,
        download_assets=True,
        assets_output_dir="downloaded_assets",
        output_json="nike_single_asset_details.json"
    )
    
    print(f"Scraped {len(details)} ads")


if __name__ == "__main__":
    main()

