"""
LinkedIn Complete Ad Scraper with Deduplication
Combines search page scraping and detail page scraping into one workflow
Automatically deduplicates assets (videos, images) to avoid downloading duplicates
"""

import requests
import json
import time
import os
import re
import hashlib
from typing import List, Dict, Optional, Set, Tuple
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urlparse, unquote

# Optional: for CSV export
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed. CSV export disabled. Install with: pip install pandas")


class LinkedInCompleteScraper:
    """
    Complete LinkedIn Ad Library Scraper
    Combines search page scraping and detail page scraping with asset deduplication
    
    Usage:
        scraper = LinkedInCompleteScraper()
        details = scraper.scrape_complete("Nike", max_results=50, download_assets=True)
    """
    
    def __init__(self):
        """Initialize scraper with headers"""
        self.ua = UserAgent()
        self.search_base_url = "https://www.linkedin.com/ad-library/search"
        self.detail_base_url = "https://www.linkedin.com"
        self.session = requests.Session()
        self._setup_headers()
        
        # Track seen assets globally to avoid duplicates
        self.seen_assets = {
            "logos": {},  # normalized_url -> local_path
            "images": {},  # normalized_url -> local_path
            "videos": {},  # base_path -> local_path (for video quality variants)
            "posters": {}  # normalized_url -> local_path
        }
        
    def _setup_headers(self):
        """Setup request headers to mimic browser"""
        self.headers = {
            "User-Agent": self.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.linkedin.com/ad-library/",
            "Origin": "https://www.linkedin.com",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
        }
        self.session.headers.update(self.headers)
    
    # ==================== URL Normalization & Duplicate Detection ====================
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL by removing query parameters for comparison"""
        try:
            parsed = urlparse(url)
            # Return scheme + netloc + path (without query and fragment)
            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            return normalized
        except Exception:
            return url
    
    def _get_video_base_path(self, url: str) -> str:
        """Extract base path for video (removes quality indicators like 360p, 720p)"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            
            # Remove quality indicators from path
            # Pattern: /mp4-360p-30fp-crf28/ or /mp4-720p-30fp-crf28/
            path = re.sub(r'/mp4-\d+p-\d+fp-[^/]+/', '/', path)
            path = re.sub(r'/mp4-\d+p/', '/', path)
            path = re.sub(r'-\d+p-', '-', path)
            
            # Return base path without query params
            base_path = f"{parsed.scheme}://{parsed.netloc}{path}"
            return base_path
        except Exception:
            return self._normalize_url(url)
    
    def _is_duplicate_asset(self, url: str, asset_type: str) -> Tuple[bool, Optional[str]]:
        """
        Check if asset is duplicate and return existing path if found
        
        Args:
            url: Asset URL
            asset_type: Type of asset ('logo', 'image', 'video', 'poster')
            
        Returns:
            Tuple of (is_duplicate, existing_path)
        """
        if asset_type == "video":
            # For videos, compare base paths (same video, different quality)
            base_path = self._get_video_base_path(url)
            if base_path in self.seen_assets["videos"]:
                return True, self.seen_assets["videos"][base_path]
        else:
            # For images/logos/posters, normalize URL
            normalized = self._normalize_url(url)
            asset_key = asset_type + "s"  # logos, images, posters
            if asset_key in self.seen_assets and normalized in self.seen_assets[asset_key]:
                return True, self.seen_assets[asset_key][normalized]
        
        return False, None
    
    def _deduplicate_assets(self, assets: Dict[str, List[str]], asset_type: str) -> Dict[str, List[str]]:
        """Remove duplicate assets from a list"""
        deduplicated = {
            "images": [],
            "videos": [],
            "posters": []
        }
        
        for key in ["images", "videos", "posters"]:
            seen_urls = set()
            for url in assets.get(key, []):
                if key == "videos":
                    base_path = self._get_video_base_path(url)
                    if base_path not in seen_urls:
                        seen_urls.add(base_path)
                        deduplicated[key].append(url)
                else:
                    normalized = self._normalize_url(url)
                    if normalized not in seen_urls:
                        seen_urls.add(normalized)
                        deduplicated[key].append(url)
        
        return deduplicated
    
    # ==================== Search Page Scraping (from file 5) ====================
    
    def _build_search_url(self, account_owner: str, keyword: str = "", 
                         countries: List[str] = None, start: int = 0, 
                         startdate: str = "", enddate: str = "") -> str:
        """Build the search URL with all parameters"""
        if countries is None:
            countries = ["ALL"]
        
        params = {
            "accountOwner": account_owner,
            "keyword": keyword,
            "startdate": startdate,
            "enddate": enddate,
        }
        
        for country in countries:
            params[f"countries"] = country
        
        if start > 0:
            params["start"] = str(start)
        
        query_string = urlencode(params, doseq=True)
        url = f"{self.search_base_url}?{query_string}"
        return url
    
    def _extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """Extract JSON data from HTML response"""
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
                r'window\.__INITIAL_DATA__\s*=\s*({.+?});',
                r'"elements"\s*:\s*(\[.+?\])',
                r'"results"\s*:\s*(\[.+?\])',
                r'"ads"\s*:\s*(\[.+?\])',
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
            
            all_scripts = soup.find_all('script')
            for script in all_scripts:
                if not script.string:
                    continue
                script_text = script.string.strip()
                if script_text.startswith('{') or script_text.startswith('['):
                    try:
                        data = json.loads(script_text)
                        if isinstance(data, dict) and ('elements' in data or 'results' in data or 'data' in data or 'ads' in data):
                            return data
                    except json.JSONDecodeError:
                        continue
            
            return None
        except Exception as e:
            print(f"Error extracting JSON from HTML: {e}")
            return None
    
    def _extract_ads_from_html(self, html_content: str) -> List[Dict]:
        """Extract ad data from HTML page"""
        ads = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            json_data = self._extract_json_from_html(html_content)
            if json_data:
                if isinstance(json_data, dict):
                    for key in ['elements', 'results', 'data', 'ads', 'items']:
                        if key in json_data and isinstance(json_data[key], list):
                            ads.extend(json_data[key])
                            break
                elif isinstance(json_data, list):
                    ads = json_data
            
            if not ads:
                ad_containers = soup.find_all(['div', 'article', 'section'], 
                                               class_=re.compile(r'ad|card|item|result', re.I))
                
                for container in ad_containers:
                    ad_data = {}
                    text = container.get_text(strip=True)
                    if text:
                        ad_data['text'] = text
                    
                    images = container.find_all('img')
                    if images:
                        ad_data['images'] = [img.get('src') or img.get('data-src') for img in images]
                    
                    links = container.find_all('a', href=True)
                    if links:
                        ad_data['links'] = [link.get('href') for link in links]
                    
                    for attr in container.attrs:
                        if 'data' in attr.lower():
                            ad_data[attr] = container.get(attr)
                    
                    if ad_data:
                        ads.append(ad_data)
            
            return ads
        except Exception as e:
            print(f"Error extracting ads from HTML: {e}")
            return []
    
    def fetch_search_page(self, account_owner: str, keyword: str = "", 
                         countries: List[str] = None, start: int = 0,
                         startdate: str = "", enddate: str = "") -> Optional[List[Dict]]:
        """Fetch a single page of ads from LinkedIn Ad Library"""
        url = self._build_search_url(account_owner, keyword, countries, start, startdate, enddate)
        
        try:
            print(f"Fetching page: {account_owner}, start={start}...")
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                ads = self._extract_ads_from_html(response.text)
                if ads:
                    print(f"✓ Extracted {len(ads)} ads from HTML")
                    return ads
                else:
                    print("No ads found in HTML response")
                    return []
            else:
                print(f"Request failed with status code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def scrape_search_pages(self, account_owner: str, keyword: str = "", 
                           countries: List[str] = None, max_results: int = 100,
                           results_per_page: int = 12, delay: float = 2.0,
                           startdate: str = "", enddate: str = "") -> List[Dict]:
        """Scrape all ads for a given advertiser with pagination"""
        if countries is None:
            countries = ["ALL"]
        
        all_ads = []
        start = 0
        
        print(f"\n{'='*60}")
        print(f"Scraping ads for: {account_owner}")
        if keyword:
            print(f"Keyword filter: {keyword}")
        print(f"Max results: {max_results}")
        print(f"{'='*60}\n")
        
        while len(all_ads) < max_results:
            ads = self.fetch_search_page(account_owner, keyword, countries, start, startdate, enddate)
            
            if ads is None:
                print("Request failed, stopping")
                break
            
            if not ads:
                print("No more ads found, stopping")
                break
            
            ads_to_add = ads[:max_results - len(all_ads)]
            all_ads.extend(ads_to_add)
            
            print(f"✓ Total ads collected: {len(all_ads)}/{max_results}")
            
            if len(ads) < results_per_page or len(all_ads) >= max_results:
                break
            
            start += results_per_page
            if delay > 0:
                time.sleep(delay)
        
        print(f"\n{'='*60}")
        print(f"Search scraping complete! Total ads collected: {len(all_ads)}")
        print(f"{'='*60}\n")
        
        return all_ads
    
    # ==================== Detail Page Scraping (from file 8) ====================
    
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
        """Extract images and videos from HTML with deduplication"""
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
            
            # Deduplicate assets before returning
            return self._deduplicate_assets(assets, "all")
            
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
    
    # ==================== Asset Downloading with Deduplication ====================
    
    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """Determine file extension from URL or content type"""
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
        """Generate a filename for the asset"""
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
        """Download all assets for an ad with duplicate detection"""
        downloaded = {
            "logo": None,
            "images": [],
            "videos": [],
            "posters": []
        }
        
        ad_dir = os.path.join(output_dir, ad_id)
        
        # Download logo with deduplication
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
        
        # Download images with deduplication
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
            if downloaded["images"]:
                print(f"    ✓ Downloaded {len(downloaded['images'])} images")
        
        # Download videos with deduplication (keep highest quality)
        if assets.get("videos"):
            # Group videos by base path and keep highest quality
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
                    # Choose highest quality (prefer URLs with higher numbers like 720p over 360p)
                    best_url = max(video_urls, key=lambda x: max([int(m) for m in re.findall(r'(\d+)p', x)] + [0]))
                    video_filename = self._generate_filename(best_url, "video", ad_id, len(downloaded["videos"]) + 1)
                    video_path = os.path.join(ad_dir, "videos", video_filename)
                    if self._download_asset(best_url, video_path):
                        downloaded["videos"].append(video_path)
                        self.seen_assets["videos"][base_path] = video_path
                        print(f"    ✓ Video downloaded (quality: {max([int(m) for m in re.findall(r'(\d+)p', best_url)] + [0])}p)")
            
            if downloaded["videos"]:
                print(f"    ✓ Total videos: {len(downloaded['videos'])}")
        
        # Download posters with deduplication
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
            if downloaded["posters"]:
                print(f"    ✓ Downloaded {len(downloaded['posters'])} posters")
        
        return downloaded
    
    # ==================== Combined Workflow ====================
    
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
    
    def scrape_complete(self, account_owner: str, keyword: str = "",
                       countries: List[str] = None, max_results: int = 100,
                       results_per_page: int = 12, delay: float = 2.0,
                       startdate: str = "", enddate: str = "",
                       download_assets: bool = True,
                       assets_output_dir: str = "downloaded_assets",
                       save_intermediate: bool = False,
                       intermediate_json: str = "intermediate_ads.json",
                       output_json: str = "complete_ad_details.json") -> List[Dict]:
        """
        Complete scraping workflow: search pages -> detail pages -> download assets
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            keyword: Search keyword (optional)
            countries: List of country codes (default: ["ALL"])
            max_results: Maximum number of ads to scrape
            results_per_page: Number of results per page
            delay: Delay between requests in seconds
            startdate: Start date filter (optional)
            enddate: End date filter (optional)
            download_assets: Whether to download assets
            assets_output_dir: Directory to save downloaded assets
            save_intermediate: Save search results to intermediate JSON
            intermediate_json: Filename for intermediate results
            output_json: Filename for final results
            
        Returns:
            List of complete ad detail dictionaries
        """
        print(f"\n{'='*80}")
        print(f"COMPLETE LINKEDIN AD SCRAPING WORKFLOW")
        print(f"{'='*80}")
        print(f"Advertiser: {account_owner}")
        print(f"Max Results: {max_results}")
        if download_assets:
            print(f"Assets Directory: {assets_output_dir}/")
        print(f"{'='*80}\n")
        
        # Step 1: Scrape search pages
        print("STEP 1: Scraping search pages...")
        ads = self.scrape_search_pages(
            account_owner=account_owner,
            keyword=keyword,
            countries=countries,
            max_results=max_results,
            results_per_page=results_per_page,
            delay=delay,
            startdate=startdate,
            enddate=enddate
        )
        
        if not ads:
            print("No ads found in search pages")
            return []
        
        # Save intermediate results if requested
        if save_intermediate:
            try:
                with open(intermediate_json, 'w', encoding='utf-8') as f:
                    json.dump(ads, f, indent=2, ensure_ascii=False)
                print(f"✓ Saved intermediate results to {intermediate_json}")
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
            
            # Deduplication stats
            total_videos_before = sum(len(d.get('assets', {}).get('videos', [])) for d in all_details)
            total_videos_after = sum(len(d.get('assets_local_paths', {}).get('videos', [])) for d in all_details)
            if total_videos_before > total_videos_after:
                print(f"Videos deduplicated: {total_videos_before - total_videos_after} duplicates removed")
        
        print(f"{'='*80}\n")
        
        return all_details


def main():
    """Example usage"""
    scraper = LinkedInCompleteScraper()
    
    # Complete scraping workflow
    details = scraper.scrape_complete(
        account_owner="Nike",
        keyword="",
        countries=["ALL"],
        max_results=50,  # Limit for testing
        results_per_page=12,
        delay=2.0,
        download_assets=True,
        assets_output_dir="downloaded_assets",
        save_intermediate=True,
        intermediate_json="nike_ads_intermediate.json",
        output_json="nike_complete_details.json"
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

