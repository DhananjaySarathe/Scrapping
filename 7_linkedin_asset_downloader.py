"""
LinkedIn Asset Downloader
Downloads images, videos, and other assets from LinkedIn Ad Library pages
Handles signed URLs with expiration tokens
"""

import requests
import json
import time
import os
import re
from typing import List, Dict, Optional, Tuple
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote
import hashlib


class LinkedInAssetDownloader:
    """
    Downloads LinkedIn assets (images, videos) from ad detail pages
    Usage:
        downloader = LinkedInAssetDownloader()
        assets = downloader.extract_assets_from_html("ad_detail_debug_656802214.html")
        downloader.download_assets(assets, output_dir="downloaded_assets")
    """
    
    def __init__(self):
        """Initialize downloader with session and headers"""
        self.ua = UserAgent()
        self.session = requests.Session()
        self._setup_headers()
        
    def _setup_headers(self):
        """Setup request headers to mimic browser"""
        self.headers = {
            "User-Agent": self.ua.random,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.linkedin.com/ad-library/",
            "Origin": "https://www.linkedin.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        self.session.headers.update(self.headers)
    
    def _get_file_extension(self, url: str, content_type: str = None) -> str:
        """Determine file extension from URL or content type"""
        # Try to get from URL first
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Check for common image extensions
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
        
        # Check content type
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
        
        # Default based on URL pattern
        if 'video' in url.lower() or 'playlist' in url.lower():
            return '.mp4'
        elif 'logo' in url.lower() or 'image' in url.lower():
            return '.jpg'
        
        return '.bin'
    
    def _generate_filename(self, url: str, asset_type: str, index: int = 0) -> str:
        """Generate a filename for the asset"""
        # Extract filename from URL if possible
        parsed = urlparse(url)
        path = parsed.path
        
        # Try to extract meaningful name from path
        path_parts = [p for p in path.split('/') if p and p not in ['dms', 'image', 'v2', 'playlist', 'vid']]
        
        if path_parts:
            # Use last meaningful part
            base_name = path_parts[-1]
            # Clean up the name
            base_name = re.sub(r'[^a-zA-Z0-9_-]', '_', base_name)
            base_name = base_name[:50]  # Limit length
        else:
            # Generate hash-based name
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            base_name = f"{asset_type}_{url_hash}"
        
        # Get extension
        ext = self._get_file_extension(url)
        
        return f"{base_name}_{index}{ext}"
    
    def extract_assets_from_html(self, html_file: str) -> Dict[str, List[str]]:
        """
        Extract all asset URLs from HTML file
        
        Args:
            html_file: Path to HTML file
            
        Returns:
            Dictionary with 'images', 'videos', 'posters', 'logos' lists
        """
        assets = {
            "images": [],
            "videos": [],
            "posters": [],
            "logos": [],
            "other": []
        }
        
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract images
            images = soup.find_all('img')
            for img in images:
                src = img.get('src') or img.get('data-src') or img.get('data-delayed-url')
                if src:
                    # Decode HTML entities
                    src = unquote(src.replace('&amp;', '&'))
                    if src.startswith('http'):
                        if 'logo' in src.lower():
                            assets["logos"].append(src)
                        else:
                            assets["images"].append(src)
            
            # Extract videos and video sources
            videos = soup.find_all('video')
            for video in videos:
                # Check src attribute
                src = video.get('src') or video.get('data-src')
                if src and src.startswith('http'):
                    assets["videos"].append(unquote(src.replace('&amp;', '&')))
                
                # Check data-sources attribute (JSON array)
                data_sources = video.get('data-sources')
                if data_sources:
                    try:
                        # Decode HTML entities first
                        data_sources = unquote(data_sources.replace('&amp;', '&').replace('&quot;', '"'))
                        sources = json.loads(data_sources)
                        if isinstance(sources, list):
                            for source in sources:
                                if isinstance(source, dict) and 'src' in source:
                                    video_url = source['src']
                                    if video_url.startswith('http'):
                                        assets["videos"].append(unquote(video_url))
                    except (json.JSONDecodeError, AttributeError):
                        pass
                
                # Check poster (thumbnail)
                poster = video.get('data-poster-url') or video.get('poster')
                if poster and poster.startswith('http'):
                    assets["posters"].append(unquote(poster.replace('&amp;', '&')))
            
            # Extract video sources from script tags or data attributes
            all_elements = soup.find_all(attrs=lambda x: x and any('video' in str(v).lower() or 'source' in str(v).lower() for v in (x.values() if isinstance(x, dict) else [x])))
            
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
                                        assets["videos"].append(unquote(url))
                    except (json.JSONDecodeError, AttributeError):
                        pass
            
            # Remove duplicates while preserving order
            for key in assets:
                seen = set()
                unique_assets = []
                for asset in assets[key]:
                    if asset not in seen:
                        seen.add(asset)
                        unique_assets.append(asset)
                assets[key] = unique_assets
            
            print(f"✓ Extracted assets from HTML:")
            print(f"  - Images: {len(assets['images'])}")
            print(f"  - Videos: {len(assets['videos'])}")
            print(f"  - Posters: {len(assets['posters'])}")
            print(f"  - Logos: {len(assets['logos'])}")
            
            return assets
            
        except Exception as e:
            print(f"Error extracting assets: {e}")
            return assets
    
    def download_asset(self, url: str, output_path: str) -> bool:
        """
        Download a single asset
        
        Args:
            url: Asset URL
            output_path: Full path where to save the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use HEAD request first to check if file exists and get content type
            head_response = self.session.head(url, timeout=10, allow_redirects=True)
            
            if head_response.status_code == 200:
                content_type = head_response.headers.get('Content-Type', '')
                
                # Now download the actual file
                response = self.session.get(url, timeout=30, stream=True, allow_redirects=True)
                
                if response.status_code == 200:
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    
                    # Write file in chunks
                    with open(output_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    file_size = os.path.getsize(output_path)
                    print(f"  ✓ Downloaded: {os.path.basename(output_path)} ({file_size:,} bytes)")
                    return True
                else:
                    print(f"  ✗ Failed: {os.path.basename(output_path)} (Status: {response.status_code})")
                    return False
            else:
                print(f"  ✗ Failed: {os.path.basename(output_path)} (HEAD Status: {head_response.status_code})")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error downloading {url}: {e}")
            return False
        except Exception as e:
            print(f"  ✗ Error saving {output_path}: {e}")
            return False
    
    def download_assets(self, assets: Dict[str, List[str]], output_dir: str = "downloaded_assets", 
                       delay: float = 1.0) -> Dict[str, List[str]]:
        """
        Download all assets to local directory
        
        Args:
            assets: Dictionary with asset URLs (from extract_assets_from_html)
            output_dir: Directory to save downloaded files
            delay: Delay between downloads in seconds
            
        Returns:
            Dictionary with paths of successfully downloaded files
        """
        downloaded = {
            "images": [],
            "videos": [],
            "posters": [],
            "logos": [],
            "other": []
        }
        
        # Create output directory structure
        os.makedirs(output_dir, exist_ok=True)
        for asset_type in ["images", "videos", "posters", "logos", "other"]:
            os.makedirs(os.path.join(output_dir, asset_type), exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"Downloading assets to: {output_dir}/")
        print(f"{'='*60}\n")
        
        total_assets = sum(len(urls) for urls in assets.values())
        downloaded_count = 0
        
        # Download each type of asset
        for asset_type, urls in assets.items():
            if not urls:
                continue
            
            print(f"\nDownloading {asset_type} ({len(urls)} files)...")
            
            for i, url in enumerate(urls, 1):
                print(f"[{downloaded_count + 1}/{total_assets}] ", end="")
                
                # Generate filename
                filename = self._generate_filename(url, asset_type, i)
                output_path = os.path.join(output_dir, asset_type, filename)
                
                # Download
                if self.download_asset(url, output_path):
                    downloaded[asset_type].append(output_path)
                    downloaded_count += 1
                
                # Rate limiting
                if i < len(urls) and delay > 0:
                    time.sleep(delay)
        
        print(f"\n{'='*60}")
        print(f"Download complete!")
        print(f"Successfully downloaded: {downloaded_count}/{total_assets} assets")
        print(f"{'='*60}\n")
        
        return downloaded
    
    def download_from_html_file(self, html_file: str, output_dir: str = "downloaded_assets", 
                               delay: float = 1.0) -> Dict[str, List[str]]:
        """
        Extract and download all assets from HTML file in one step
        
        Args:
            html_file: Path to HTML file
            output_dir: Directory to save downloaded files
            delay: Delay between downloads in seconds
            
        Returns:
            Dictionary with paths of successfully downloaded files
        """
        print(f"Extracting assets from: {html_file}")
        assets = self.extract_assets_from_html(html_file)
        
        if any(assets.values()):
            return self.download_assets(assets, output_dir, delay)
        else:
            print("No assets found in HTML file")
            return {}


def main():
    """Example usage"""
    downloader = LinkedInAssetDownloader()
    
    # Example: Download assets from the debug HTML file
    html_file = "ad_detail_debug_656802214.html"
    
    if os.path.exists(html_file):
        print(f"Processing HTML file: {html_file}\n")
        
        # Extract and download all assets
        downloaded = downloader.download_from_html_file(
            html_file=html_file,
            output_dir="downloaded_assets",
            delay=1.0  # 1 second delay between downloads
        )
        
        # Print summary
        print("\nDownload Summary:")
        for asset_type, paths in downloaded.items():
            if paths:
                print(f"  {asset_type}: {len(paths)} files")
                for path in paths[:3]:  # Show first 3
                    print(f"    - {path}")
                if len(paths) > 3:
                    print(f"    ... and {len(paths) - 3} more")
    else:
        print(f"HTML file not found: {html_file}")
        print("\nUsage example:")
        print("  downloader = LinkedInAssetDownloader()")
        print("  assets = downloader.extract_assets_from_html('your_file.html')")
        print("  downloaded = downloader.download_assets(assets, 'output_folder')")


if __name__ == "__main__":
    main()

