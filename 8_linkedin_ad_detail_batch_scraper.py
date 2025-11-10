"""
LinkedIn Ad Detail Batch Scraper
Reads ads from JSON/CSV file, extracts detail page links, and scrapes each detail page
Extracts logos, assets (images/videos), and other details from each ad
"""

import requests
import json
import time
import os
import re
import hashlib
from typing import List, Dict, Optional
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote

# Optional: for CSV import
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed. CSV import disabled. Install with: pip install pandas")


class LinkedInAdDetailBatchScraper:
    """
    Batch scraper for LinkedIn Ad Library detail pages
    Reads ads from JSON/CSV, extracts detail links, and scrapes each page
    
    Usage:
        scraper = LinkedInAdDetailBatchScraper()
        details = scraper.scrape_from_json("nike_ads.json", "nike_ad_details.json")
    """
    
    def __init__(self):
        """Initialize scraper with headers"""
        self.ua = UserAgent()
        self.base_url = "https://www.linkedin.com"
        self.session = requests.Session()
        self._setup_headers()
        
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
    
    def _extract_ad_id_from_link(self, link: str) -> Optional[str]:
        """Extract ad ID from detail link"""
        try:
            # Pattern: /ad-library/detail/656802214 or /ad-library/detail/656802214?trk=...
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
            return f"{self.base_url}{link}"
        else:
            return f"{self.base_url}/ad-library/detail/{link}"
    
    def _extract_logo_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract logo URL from HTML"""
        try:
            # Look for logo images - usually in advertiser section
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
            
            # Fallback: look for any image near advertiser name
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
            elif 'image/gif' in content_type:
                return '.gif'
            elif 'image/webp' in content_type:
                return '.webp'
            elif 'video/mp4' in content_type:
                return '.mp4'
            elif 'video/webm' in content_type:
                return '.webm'
        
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
                else:
                    return False
            else:
                return False
                
        except Exception:
            return False
    
    def _extract_assets_from_html(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extract images and videos from HTML"""
        assets = {
            "images": [],
            "videos": [],
            "posters": []
        }
        
        try:
            # Extract images (excluding logos)
            images = soup.find_all('img')
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                if src:
                    src = unquote(src.replace('&amp;', '&'))
                    if src.startswith('http') and 'logo' not in src.lower():
                        if src not in assets["images"]:
                            assets["images"].append(src)
            
            # Extract videos
            videos = soup.find_all('video')
            for video in videos:
                # Check src attribute
                src = video.get('src') or video.get('data-src')
                if src and src.startswith('http'):
                    src = unquote(src.replace('&amp;', '&'))
                    if src not in assets["videos"]:
                        assets["videos"].append(src)
                
                # Check data-sources attribute (JSON array)
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
                                        if video_url not in assets["videos"]:
                                            assets["videos"].append(video_url)
                    except (json.JSONDecodeError, AttributeError):
                        pass
                
                # Check poster (thumbnail)
                poster = video.get('data-poster-url') or video.get('poster')
                if poster and poster.startswith('http'):
                    poster = unquote(poster.replace('&amp;', '&'))
                    if poster not in assets["posters"]:
                        assets["posters"].append(poster)
            
            # Look for video URLs in data attributes
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
                                        if url not in assets["videos"]:
                                            assets["videos"].append(url)
                    except (json.JSONDecodeError, AttributeError):
                        pass
            
            return assets
            
        except Exception as e:
            print(f"  Error extracting assets: {e}")
            return assets
    
    def _download_ad_assets(self, ad_id: str, logo_url: Optional[str], assets: Dict[str, List[str]], 
                           output_dir: str) -> Dict[str, List[str]]:
        """Download all assets for an ad"""
        downloaded = {
            "logo": None,
            "images": [],
            "videos": [],
            "posters": []
        }
        
        # Create ad-specific directory
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
        
        # Download videos
        if assets.get("videos"):
            for i, video_url in enumerate(assets["videos"], 1):
                video_filename = self._generate_filename(video_url, "video", ad_id, i)
                video_path = os.path.join(ad_dir, "videos", video_filename)
                if self._download_asset(video_url, video_path):
                    downloaded["videos"].append(video_path)
            if downloaded["videos"]:
                print(f"    ✓ Downloaded {len(downloaded['videos'])} videos")
        
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
    
    def scrape_ad_detail(self, ad_id: str, link: str) -> Dict:
        """
        Scrape a single ad detail page
        
        Args:
            ad_id: Ad ID
            link: Original link from JSON
            
        Returns:
            Dictionary with ad details
        """
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
                
                # Extract advertiser name
                advertiser_selectors = [
                    'h1',
                    'h2',
                    'a[href*="/company/"]',
                    '[data-test-id="advertiser-name"]',
                ]
                
                for selector in advertiser_selectors:
                    element = soup.select_one(selector)
                    if element:
                        text = element.get_text(strip=True)
                        if text and len(text) < 100 and text != "Ad Details":
                            ad_detail["advertiser"] = text
                            break
                
                # Extract ad text/content
                content_selectors = [
                    '.commentary__content',
                    'p.commentary__content',
                    '.ad-content',
                    'p',
                ]
                
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
                
                # Extract ad type
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
                
                # Extract call-to-action
                cta_selectors = [
                    'button[data-tracking-control-name*="cta"]',
                    'button',
                    'a[class*="button"]',
                ]
                
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
                
                # Extract logo
                logo_url = self._extract_logo_from_html(soup)
                if logo_url:
                    ad_detail["logo_url"] = logo_url
                
                # Extract assets (images/videos)
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
    
    def extract_detail_links_from_json(self, json_file: str) -> List[Dict]:
        """
        Extract detail links from JSON file
        
        Args:
            json_file: Path to JSON file with ads
            
        Returns:
            List of dictionaries with ad_id and link
        """
        detail_links = []
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                ads = json.load(f)
            
            if not isinstance(ads, list):
                print("Error: JSON file should contain a list of ads")
                return detail_links
            
            for i, ad in enumerate(ads):
                if isinstance(ad, dict) and 'links' in ad:
                    links = ad['links']
                    if isinstance(links, list) and len(links) > 0:
                        # Get first link that is a detail link
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
                                    break  # Use first detail link found
            
            print(f"✓ Extracted {len(detail_links)} detail links from {json_file}")
            return detail_links
            
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            return detail_links
    
    def scrape_from_json(self, input_json: str, output_json: str, 
                        delay: float = 2.0, max_ads: Optional[int] = None,
                        download_assets: bool = True, assets_output_dir: str = "downloaded_assets") -> List[Dict]:
        """
        Read ads from JSON, extract detail links, and scrape each detail page
        
        Args:
            input_json: Path to input JSON file (e.g., "nike_ads.json")
            output_json: Path to output JSON file (e.g., "nike_ad_details.json")
            delay: Delay between requests in seconds
            max_ads: Maximum number of ads to scrape (None for all)
            download_assets: Whether to download assets (default: True)
            assets_output_dir: Directory to save downloaded assets
            
        Returns:
            List of ad detail dictionaries
        """
        print(f"\n{'='*60}")
        print(f"Batch Scraping Ad Details")
        print(f"Input: {input_json}")
        print(f"Output: {output_json}")
        if download_assets:
            print(f"Assets Directory: {assets_output_dir}/")
        print(f"{'='*60}\n")
        
        # Extract detail links
        detail_links = self.extract_detail_links_from_json(input_json)
        
        if not detail_links:
            print("No detail links found in JSON file")
            return []
        
        # Limit if max_ads specified
        if max_ads:
            detail_links = detail_links[:max_ads]
            print(f"Limiting to first {max_ads} ads\n")
        
        # Scrape each detail page
        all_details = []
        
        for i, link_info in enumerate(detail_links, 1):
            print(f"[{i}/{len(detail_links)}] ", end="")
            
            detail = self.scrape_ad_detail(link_info["ad_id"], link_info["link"])
            
            # Download assets if enabled
            if download_assets:
                print(f"    Downloading assets...")
                downloaded = self._download_ad_assets(
                    ad_id=link_info["ad_id"],
                    logo_url=detail.get("logo_url"),
                    assets=detail.get("assets", {}),
                    output_dir=assets_output_dir
                )
                
                # Add local paths to detail
                detail["logo_local_path"] = downloaded["logo"]
                detail["assets_local_paths"] = {
                    "images": downloaded["images"],
                    "videos": downloaded["videos"],
                    "posters": downloaded["posters"]
                }
            
            # Add original ad data reference
            detail["original_ad_index"] = link_info["index"]
            
            all_details.append(detail)
            
            # Rate limiting
            if i < len(detail_links) and delay > 0:
                time.sleep(delay)
        
        # Save to JSON
        try:
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(all_details, f, indent=2, ensure_ascii=False)
            print(f"\n{'='*60}")
            print(f"✓ Saved {len(all_details)} ad details to {output_json}")
            print(f"{'='*60}\n")
        except Exception as e:
            print(f"\nError saving to JSON: {e}")
        
        return all_details


def main():
    """Example usage"""
    scraper = LinkedInAdDetailBatchScraper()
    
    # Scrape ad details from nike_ads.json
    input_file = "nike_ads.json"
    output_file = "nike_ad_details.json"
    
    if os.path.exists(input_file):
        print(f"Processing ads from: {input_file}\n")
        
        details = scraper.scrape_from_json(
            input_json=input_file,
            output_json=output_file,
            delay=2.0,  # 2 second delay between requests
            max_ads=None,  # Set to a number to limit, or None for all
            download_assets=True,  # Download logos, images, videos
            assets_output_dir="downloaded_assets"  # Directory for downloaded assets
        )
        
        if details:
            print("\nSummary:")
            print(f"  Total ads scraped: {len(details)}")
            
            # Count ads with logos
            logos_count = sum(1 for d in details if d.get('logo_url'))
            print(f"  Ads with logos: {logos_count}")
            
            # Count ads with videos
            videos_count = sum(1 for d in details if d.get('assets', {}).get('videos'))
            print(f"  Ads with videos: {videos_count}")
            
            # Count ads with images
            images_count = sum(1 for d in details if d.get('assets', {}).get('images'))
            print(f"  Ads with images: {images_count}")
            
            # Show sample
            if details:
                print("\nSample ad detail:")
                sample = details[0]
                print(f"  Ad ID: {sample.get('ad_id')}")
                print(f"  Advertiser: {sample.get('advertiser')}")
                print(f"  Ad Type: {sample.get('ad_type')}")
                print(f"  Logo: {'Yes' if sample.get('logo_url') else 'No'}")
                print(f"  Videos: {len(sample.get('assets', {}).get('videos', []))}")
                print(f"  Images: {len(sample.get('assets', {}).get('images', []))}")
    else:
        print(f"Input file not found: {input_file}")
        print("\nUsage example:")
        print("  scraper = LinkedInAdDetailBatchScraper()")
        print("  details = scraper.scrape_from_json('nike_ads.json', 'nike_ad_details.json')")


if __name__ == "__main__":
    main()

