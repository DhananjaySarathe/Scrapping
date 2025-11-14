"""
LinkedIn Ad Library Scraper - Cookie-Based Approach
Uses Selenium ONCE to get browser cookies, then pure requests + JSON parsing
No BeautifulSoup, no fallback - fastest and most efficient approach
"""

import requests
import json
import time
import os
import re
import hashlib
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, unquote

# Selenium for cookie extraction (used only once)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("⚠ Selenium not installed. Install with: pip install selenium")


class LinkedInCookieScraper:
    """
    LinkedIn Ad Library Scraper using cookie-based approach
    
    Strategy:
    1. Use Selenium ONCE to get browser cookies (no login needed)
    2. Save cookies to cookies.json
    3. Use requests + JSON parsing for all scraping (no BeautifulSoup)
    4. Use pagination tokens for infinite scroll
    
    Usage:
        scraper = LinkedInCookieScraper()
        scraper.fetch_cookies()  # Run once to get cookies
        details = scraper.scrape_complete("Nike", max_results=100)
    """
    
    def __init__(self, cookies_file: str = "cookies.json"):
        """Initialize scraper"""
        self.cookies_file = cookies_file
        self.api_url = "https://www.linkedin.com/ad-library/searchPaginationFragment"
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
        
    def _setup_headers(self, csrf_token: Optional[str] = None):
        """Setup request headers"""
        self.headers = {
            "Accept": "application/vnd.linkedin.normalized+json+2.1, application/json",
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
        
        # Add CSRF token if provided
        if csrf_token:
            self.headers["csrf-token"] = csrf_token
        
        self.session.headers.update(self.headers)
    
    def _update_headers_with_csrf(self):
        """Update headers with CSRF token from cookies"""
        cookies = self.load_cookies()
        if cookies and "JSESSIONID" in cookies:
            jsessionid = cookies["JSESSIONID"]
            if jsessionid.startswith("ajax:"):
                self.session.headers["csrf-token"] = jsessionid
    
    def _normalize_pagination_token(self, token: Optional[str]) -> Tuple[str, Optional[str]]:
        """
        Normalize pagination token to determine pagination type
        
        Args:
            token: Pagination token from LinkedIn
            
        Returns:
            Tuple of (mode, value) where:
            - mode: "api" for API-style tokens (numeric-numeric) or "offset" for offset tokens (offset#count)
            - value: The token value or offset number
        """
        if not token:
            return ("api", None)
        
        # Check if it's an offset-based token (format: "offset#count" like "0#24", "24#24")
        if '#' in token:
            try:
                offset = token.split('#')[0]
                return ("offset", int(offset))
            except (ValueError, IndexError):
                # If parsing fails, treat as API token
                return ("api", token)
        
        # Default: API-style token (format: "numeric-numeric" like "569182064-1737754931000")
        return ("api", token)
    
    def extract_next_token_from_html(self, html: str) -> Optional[str]:
        """
        Extract next pagination token from HTML response
        
        Args:
            html: HTML content
            
        Returns:
            Next pagination token or None
        """
        # Try JSON-style token in script tags
        patterns = [
            r'"paginationToken"\s*:\s*"([^"]+)"',
            r'paginationToken["\']?\s*[:=]\s*["\']([^"\']+)',
            r'data-pagination-token="([^"]+)"',
            r'data-pagination-token=["\']([^"\']+)["\']',
            r'pagination-token["\']?\s*[:=]\s*["\']([^"\']+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                token = match.group(1)
                if token and token != "null":
                    return token
        
        return None
    
    def fetch_offset_page(self, account_owner: str, offset: int = 0) -> Optional[Dict]:
        """
        Fetch page using offset-based pagination (for tokens like "0#24", "24#24")
        
        Args:
            account_owner: Advertiser name
            offset: Starting offset (from token like "24#24" -> offset=24)
            
        Returns:
            Dictionary with html and paginationToken, or None if failed
        """
        url = "https://www.linkedin.com/ad-library/search"
        
        params = {
            "accountOwner": account_owner,
            "countries": "ALL",
            "start": str(offset)
        }
        
        cookies = self.load_cookies()
        if not cookies:
            return None
        
        # Update CSRF token in headers
        self._update_headers_with_csrf()
        
        try:
            response = self.session.get(
                url,
                params=params,
                cookies=cookies,
                timeout=15
            )
            
            if response.status_code == 200:
                html = response.text
                
                # Extract next pagination token from HTML
                next_token = self.extract_next_token_from_html(html)
                
                return {
                    "html": html,
                    "paginationToken": next_token
                }
            else:
                print(f"  ✗ Offset page request failed: Status {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error fetching offset page: {e}")
            return None
    
    def fetch_cookies(self, account_owner: str = "Nike", headless: bool = True) -> bool:
        """
        Use Selenium ONCE to fetch LinkedIn cookies (no login needed)
        
        Args:
            account_owner: Advertiser name for the page to load
            headless: Run browser in headless mode
            
        Returns:
            True if cookies fetched successfully, False otherwise
        """
        if not SELENIUM_AVAILABLE:
            print("✗ Selenium not available. Cannot fetch cookies.")
            return False
        
        try:
            print("=" * 80)
            print("FETCHING LINKEDIN COOKIES USING SELENIUM")
            print("=" * 80)
            print(f"Loading Ad Library page for: {account_owner}")
            print("(This is the ONLY time Selenium will run)")
            print("=" * 80)
            
            options = Options()
            if headless:
                options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            # Add user agent to make it look more like a real browser
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            driver = webdriver.Chrome(options=options)
            
            # Load the Ad Library search page (no login needed)
            url = f"https://www.linkedin.com/ad-library/search?accountOwner={account_owner}"
            print(f"Loading URL: {url}")
            driver.get(url)
            
            # Wait longer for LinkedIn to set all cookies and load the page
            print("Waiting 8 seconds for LinkedIn to set cookies and load page...")
            time.sleep(8)
            
            # Try to interact with the page to trigger more cookies
            try:
                # Scroll down a bit to trigger lazy loading
                driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(2)
            except:
                pass
            
            # Extract all cookies
            cookies = driver.get_cookies()
            
            # Check for important cookies
            cookie_names = {c["name"] for c in cookies}
            important_cookies = ["lang", "JSESSIONID", "lidc", "bcookie", "bscookie"]
            missing = [name for name in important_cookies if name not in cookie_names]
            
            if missing:
                print(f"⚠ Warning: Missing cookies: {missing}")
            
            # Save to JSON file
            with open(self.cookies_file, "w") as f:
                json.dump(cookies, f, indent=2)
            
            driver.quit()
            
            print(f"✓ Cookies saved to {self.cookies_file}")
            print(f"✓ Found {len(cookies)} cookies")
            print(f"✓ Cookie names: {', '.join(sorted(cookie_names))}")
            print("=" * 80)
            
            return True
            
        except Exception as e:
            print(f"✗ Error fetching cookies: {e}")
            return False
    
    def load_cookies(self) -> Dict[str, str]:
        """
        Load cookies from JSON file and convert to requests-compatible format
        
        Returns:
            Dictionary of cookie name -> value
        """
        try:
            with open(self.cookies_file, "r") as f:
                raw_cookies = json.load(f)
            
            # Convert Selenium cookie format to requests format
            cookies_dict = {cookie["name"]: cookie["value"] for cookie in raw_cookies}
            return cookies_dict
            
        except FileNotFoundError:
            print(f"✗ Cookies file not found: {self.cookies_file}")
            print("  Run fetch_cookies() first to generate cookies.")
            return {}
        except Exception as e:
            print(f"✗ Error loading cookies: {e}")
            return {}
    
    def _refresh_cookies_if_needed(self, account_owner: str = "Nike"):
        """Refresh cookies if they expired"""
        print("⚠ Cookies expired or invalid. Refreshing...")
        if self.fetch_cookies(account_owner):
            print("✓ Cookies refreshed successfully")
        else:
            print("✗ Failed to refresh cookies")
    
    def fetch_api_page(self, account_owner: str, pagination_token: Optional[str] = None) -> Optional[Dict]:
        """
        Fetch a single page from LinkedIn pagination API
        
        Args:
            account_owner: Advertiser name
            pagination_token: Token for next page (None for first page)
            
        Returns:
            JSON response with html and paginationToken, or None if failed
        """
        params = {"accountOwner": account_owner}
        if pagination_token:
            params["paginationToken"] = pagination_token
        
        cookies = self.load_cookies()
        if not cookies:
            return None
        
        # Update CSRF token in headers from cookies
        self._update_headers_with_csrf()
        
        # Also try the search endpoint directly if API fails
        try:
            # First try the pagination fragment endpoint
            response = self.session.get(
                self.api_url,
                params=params,
                cookies=cookies,
                timeout=15
            )
            
            # If that fails, try the main search page with start parameter
            if response.status_code != 200 or ('text/html' in response.headers.get('Content-Type', '') and not pagination_token):
                # For first page, try the main search URL
                search_url = f"https://www.linkedin.com/ad-library/search"
                search_params = {"accountOwner": account_owner, "countries": "ALL"}
                if pagination_token:
                    # Try to use pagination token as start parameter
                    try:
                        # Extract numeric part if token is like "123-456"
                        if '-' in pagination_token:
                            search_params["start"] = pagination_token.split('-')[0]
                    except:
                        pass
                
                response = self.session.get(
                    search_url,
                    params=search_params,
                    cookies=cookies,
                    timeout=15
                )
            
            if response.status_code == 200:
                # Check content type
                content_type = response.headers.get('Content-Type', '').lower()
                
                # Try to parse as JSON first
                try:
                    data = response.json()
                    # Check if it has the expected structure
                    if isinstance(data, dict) and ('html' in data or 'paginationToken' in data):
                        return data
                except json.JSONDecodeError:
                    pass
                
                # LinkedIn returned HTML - parse it directly
                if 'text/html' in content_type or response.text.strip().startswith('<!DOCTYPE'):
                    print(f"  ⚠ LinkedIn returned HTML instead of JSON")
                    print(f"  → Parsing HTML response directly...")
                    
                    # Extract HTML fragment from response
                    html_content = response.text
                    
                    # Try to find pagination token in HTML (might be in script tags or data attributes)
                    pagination_token = None
                    
                    # Look for pagination token in various places
                    token_patterns = [
                        r'"paginationToken"\s*:\s*"([^"]+)"',
                        r'data-pagination-token="([^"]+)"',
                        r'paginationToken["\']?\s*[:=]\s*["\']([^"\']+)',
                    ]
                    
                    for pattern in token_patterns:
                        match = re.search(pattern, html_content, re.IGNORECASE)
                        if match:
                            pagination_token = match.group(1)
                            break
                    
                    # Return HTML as if it were the API response
                    return {
                        "html": html_content,
                        "paginationToken": pagination_token
                    }
                
                # If not HTML, try to parse as JSON one more time
                try:
                    return response.json()
                except json.JSONDecodeError:
                    print(f"  ✗ Failed to parse response")
                    print(f"  Content-Type: {content_type}")
                    print(f"  Response preview (first 200 chars):")
                    print(f"  {response.text[:200]}")
                    return None
            elif response.status_code in (401, 403):
                # Cookies expired - refresh them
                self._refresh_cookies_if_needed(account_owner)
                cookies = self.load_cookies()
                if cookies:
                    response = self.session.get(
                        self.api_url,
                        params=params,
                        cookies=cookies,
                        timeout=15
                    )
                    if response.status_code == 200:
                        return response.json()
                return None
            else:
                print(f"  ✗ API request failed: Status {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error fetching page: {e}")
            return None
    
    def extract_ad_ids_from_html(self, html_fragment: str) -> List[str]:
        """
        Extract ad IDs from HTML fragment using regex (no BeautifulSoup)
        
        Args:
            html_fragment: HTML string from API response
            
        Returns:
            List of ad IDs
        """
        ad_ids = []
        
        # Pattern: /ad-library/detail/123456789
        pattern = r'/ad-library/detail/(\d+)'
        matches = re.findall(pattern, html_fragment)
        
        # Deduplicate while preserving order
        seen = set()
        for ad_id in matches:
            if ad_id not in seen:
                seen.add(ad_id)
                ad_ids.append(ad_id)
        
        return ad_ids
    
    def extract_ad_text_from_html(self, html_fragment: str) -> List[str]:
        """
        Extract ad text snippets from HTML using regex
        
        Args:
            html_fragment: HTML string
            
        Returns:
            List of text snippets
        """
        texts = []
        
        # Try to extract text from common HTML tags
        patterns = [
            r'<p[^>]*>(.*?)</p>',
            r'<span[^>]*>(.*?)</span>',
            r'<div[^>]*class="[^"]*text[^"]*"[^>]*>(.*?)</div>',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html_fragment, re.DOTALL | re.IGNORECASE)
            for match in matches:
                # Clean HTML tags
                text = re.sub(r'<[^>]+>', '', match)
                text = text.strip()
                if text and len(text) > 10 and len(text) < 500:
                    texts.append(text)
        
        return texts[:20]  # Limit to first 20
    
    def scrape_search_pages(self, account_owner: str, max_results: int = 100,
                           delay: float = 1.0) -> List[Dict]:
        """
        Scrape all ads using pagination API
        
        Args:
            account_owner: Advertiser name
            max_results: Maximum number of ads to scrape
            delay: Delay between requests
            
        Returns:
            List of ad dictionaries with IDs and links
        """
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
            print(f"📄 PAGE {page_num} - Starting new page")
            print(f"{'─'*80}")
            print(f"Fetching with token: {pagination_token or 'None (first page)'}")
            
            # Detect pagination token type and fetch accordingly
            mode, value = self._normalize_pagination_token(pagination_token)
            
            if mode == "api":
                # API-style token (numeric-numeric format)
                print(f"  → Using API pagination method")
                data = self.fetch_api_page(account_owner, value)
            elif mode == "offset":
                # Offset-based token (offset#count format)
                print(f"  → Using offset pagination method (offset={value})")
                data = self.fetch_offset_page(account_owner, offset=value)
            else:
                # First page - no token
                print(f"  → Using API pagination method (first page)")
                data = self.fetch_api_page(account_owner, None)
            
            if not data:
                print("  ✗ Failed to fetch page, stopping")
                break
            
            # Extract HTML fragment
            html_fragment = data.get("html", "")
            if not html_fragment:
                print("  ✗ No HTML fragment in response, stopping")
                break
            
            print(f"  ✓ Received HTML fragment ({len(html_fragment)} chars)")
            
            # Extract ad IDs using regex (no BeautifulSoup)
            ad_ids = self.extract_ad_ids_from_html(html_fragment)
            
            if not ad_ids:
                print("  ⚠ No ad IDs found in this page")
                if not data.get("paginationToken"):
                    print("  ✓ Reached end of results")
                    break
            else:
                print(f"  ✓ Found {len(ad_ids)} ads in this page")
                
                # Extract text snippets
                texts = self.extract_ad_text_from_html(html_fragment)
                
                # Create ad objects
                for i, ad_id in enumerate(ad_ids):
                    if len(all_ads) >= max_results:
                        break
                    
                    ad_data = {
                        "ad_id": ad_id,
                        "detail_url": f"{self.detail_base_url}/{ad_id}",
                        "text": texts[i] if i < len(texts) else None,
                        "page_number": page_num
                    }
                    all_ads.append(ad_data)
                
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
    
    # ==================== Detail Page Scraping ====================
    
    def scrape_ad_detail(self, ad_id: str) -> Dict:
        """
        Scrape a single ad detail page using requests (no BeautifulSoup)
        
        Args:
            ad_id: Ad ID
            
        Returns:
            Dictionary with ad details
        """
        url = f"{self.detail_base_url}/{ad_id}"
        cookies = self.load_cookies()
        
        ad_detail = {
            "ad_id": ad_id,
            "detail_url": url,
            "advertiser": None,
            "ad_text": None,
            "ad_type": None,
            "logo_url": None,
            "assets": {
                "images": [],
                "videos": [],
                "posters": []
            }
        }
        
        try:
            response = self.session.get(url, cookies=cookies, timeout=15)
            
            if response.status_code == 200:
                html = response.text
                
                # Extract advertiser using regex
                advertiser_patterns = [
                    r'<h1[^>]*>(.*?)</h1>',
                    r'<h2[^>]*>(.*?)</h2>',
                    r'href="/company/([^"]+)"',
                ]
                
                for pattern in advertiser_patterns:
                    match = re.search(pattern, html, re.IGNORECASE)
                    if match:
                        advertiser = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                        if advertiser and len(advertiser) < 100:
                            ad_detail["advertiser"] = advertiser
                            break
                
                # Extract ad text
                text_patterns = [
                    r'<p[^>]*class="[^"]*commentary[^"]*"[^>]*>(.*?)</p>',
                    r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
                ]
                
                texts = []
                for pattern in text_patterns:
                    matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                    for match in matches:
                        text = re.sub(r'<[^>]+>', '', match).strip()
                        if text and 10 < len(text) < 500:
                            texts.append(text)
                
                if texts:
                    ad_detail["ad_text"] = "\n".join(texts[:3])
                
                # Extract assets using regex
                assets = self._extract_assets_from_html(html)
                ad_detail["assets"] = assets
                
                # Extract logo
                logo_url = self._extract_logo_from_html(html)
                if logo_url:
                    ad_detail["logo_url"] = logo_url
                
                return ad_detail
            else:
                ad_detail["error"] = f"HTTP {response.status_code}"
                return ad_detail
                
        except Exception as e:
            ad_detail["error"] = str(e)
            return ad_detail
    
    def _extract_logo_from_html(self, html: str) -> Optional[str]:
        """Extract logo URL using regex"""
        patterns = [
            r'<img[^>]*alt="[^"]*logo[^"]*"[^>]*src="([^"]+)"',
            r'<img[^>]*src="([^"]*logo[^"]*)"[^>]*>',
            r'href="/company/[^"]*"[^>]*<img[^>]*src="([^"]+)"',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                url = match.group(1)
                if url.startswith('http'):
                    return unquote(url.replace('&amp;', '&'))
        
        return None
    
    def _extract_assets_from_html(self, html: str) -> Dict[str, List[str]]:
        """Extract asset URLs using regex (no BeautifulSoup)"""
        assets = {
            "images": [],
            "videos": [],
            "posters": []
        }
        
        # Extract images
        img_pattern = r'<img[^>]*src="([^"]+)"[^>]*>'
        img_matches = re.findall(img_pattern, html, re.IGNORECASE)
        for url in img_matches:
            url = unquote(url.replace('&amp;', '&'))
            if url.startswith('http') and 'logo' not in url.lower():
                assets["images"].append(url)
        
        # Extract videos
        video_patterns = [
            r'<video[^>]*src="([^"]+)"[^>]*>',
            r'<source[^>]*src="([^"]+)"[^>]*type="video[^"]*"[^>]*>',
            r'"videoUrl":"([^"]+)"',
            r'data-sources="([^"]+)"',
        ]
        
        video_urls = []
        for pattern in video_patterns:
            matches = re.findall(pattern, html, re.IGNORECASE)
            for match in matches:
                # Handle JSON-encoded data-sources
                if 'data-sources' in pattern:
                    try:
                        decoded = unquote(match.replace('&quot;', '"'))
                        sources = json.loads(decoded)
                        if isinstance(sources, list):
                            for source in sources:
                                if isinstance(source, dict) and 'src' in source:
                                    video_urls.append(source['src'])
                    except:
                        pass
                else:
                    url = unquote(match.replace('&amp;', '&'))
                    if url.startswith('http'):
                        video_urls.append(url)
        
        # Get highest quality video only (deduplicate by base path)
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
        
        # Extract posters
        poster_pattern = r'<video[^>]*poster="([^"]+)"[^>]*>'
        poster_matches = re.findall(poster_pattern, html, re.IGNORECASE)
        for url in poster_matches:
            url = unquote(url.replace('&amp;', '&'))
            if url.startswith('http'):
                assets["posters"].append(url)
        
        return assets
    
    def _get_video_base_path(self, url: str) -> str:
        """Extract base path for video (removes quality indicators)"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            # Remove quality indicators like /mp4-360p-30fp-crf28/
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
        
        # Download images
        if assets.get("images"):
            for i, img_url in enumerate(assets["images"], 1):
                img_filename = self._generate_filename(img_url, "image", ad_id, i)
                img_path = os.path.join(ad_dir, "images", img_filename)
                if self._download_asset(img_url, img_path):
                    downloaded["images"].append(img_path)
        
        # Download videos (already filtered to highest quality)
        if assets.get("videos"):
            for i, video_url in enumerate(assets["videos"], 1):
                video_filename = self._generate_filename(video_url, "video", ad_id, i)
                video_path = os.path.join(ad_dir, "videos", video_filename)
                if self._download_asset(video_url, video_path):
                    downloaded["videos"].append(video_path)
        
        # Download posters
        if assets.get("posters"):
            for i, poster_url in enumerate(assets["posters"], 1):
                poster_filename = self._generate_filename(poster_url, "poster", ad_id, i)
                poster_path = os.path.join(ad_dir, "posters", poster_filename)
                if self._download_asset(poster_url, poster_path):
                    downloaded["posters"].append(poster_path)
        
        return downloaded
    
    # ==================== Complete Workflow ====================
    
    def scrape_complete(self, account_owner: str, max_results: int = 100,
                       delay: float = 1.0, download_assets: bool = True,
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
        print(f"COMPLETE LINKEDIN AD SCRAPING (COOKIE-BASED)")
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
        
        # Step 1: Scrape search pages
        print("STEP 1: Scraping search pages via API...")
        ads = self.scrape_search_pages(
            account_owner=account_owner,
            max_results=max_results,
            delay=delay
        )
        
        if not ads:
            print("No ads found")
            return []
        
        # Step 2: Scrape detail pages
        print(f"\nSTEP 2: Scraping detail pages...")
        all_details = []
        
        for i, ad in enumerate(ads, 1):
            print(f"[{i}/{len(ads)}] Scraping ad ID: {ad['ad_id']}...")
            
            detail = self.scrape_ad_detail(ad['ad_id'])
            
            # Download assets if enabled
            if download_assets:
                downloaded = self._download_ad_assets(
                    ad_id=ad['ad_id'],
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
            
            if i < len(ads) and delay > 0:
                time.sleep(delay)
        
        # Step 3: Save results
        print(f"\nSTEP 3: Saving results...")
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
    scraper = LinkedInCookieScraper()
    
    # Step 1: Fetch cookies using Selenium (run once, or when cookies expire)
    print("Fetching cookies...")
    scraper.fetch_cookies(account_owner="Nike", headless=True)
    
    # Step 2: Complete scraping workflow
    details = scraper.scrape_complete(
        account_owner="Nike",
        max_results=50,  # Limit for testing
        delay=1.0,
        download_assets=True,
        assets_output_dir="downloaded_assets",
        output_json="nike_cookie_details.json"
    )
    
    if details:
        print("\nSample ad detail:")
        sample = details[0]
        print(f"  Ad ID: {sample.get('ad_id')}")
        print(f"  Advertiser: {sample.get('advertiser')}")
        print(f"  Videos: {len(sample.get('assets_local_paths', {}).get('videos', []))}")
        print(f"  Images: {len(sample.get('assets_local_paths', {}).get('images', []))}")


if __name__ == "__main__":
    main()

