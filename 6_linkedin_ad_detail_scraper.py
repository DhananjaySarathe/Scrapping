"""
LinkedIn Ad Detail Scraper
Scrapes detailed information from individual LinkedIn Ad Library detail pages
Example URL: https://www.linkedin.com/ad-library/detail/656802214
"""

import requests
import json
import time
import os
import re
from typing import List, Dict, Optional
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Optional: for CSV export
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed. CSV export disabled. Install with: pip install pandas")


class LinkedInAdDetailScraper:
    """
    Scraper for LinkedIn Ad Library detail pages
    Usage:
        scraper = LinkedInAdDetailScraper()
        # Option 1: Scrape from list of ad IDs
        ad_ids = ["656802214", "123456789"]
        details = scraper.scrape_ad_details(ad_ids)
        
        # Option 2: Scrape from list of URLs
        urls = ["https://www.linkedin.com/ad-library/detail/656802214"]
        details = scraper.scrape_ad_details_from_urls(urls)
        
        # Save results
        scraper.save_to_json(details, "ad_details.json")
        scraper.save_to_csv(details, "ad_details.csv")
    """
    
    def __init__(self):
        """Initialize scraper with headers"""
        self.ua = UserAgent()
        self.base_url = "https://www.linkedin.com/ad-library/detail"
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
    
    def _extract_ad_id_from_url(self, url: str) -> Optional[str]:
        """Extract ad ID from a LinkedIn ad detail URL"""
        try:
            # Pattern: /ad-library/detail/656802214
            match = re.search(r'/ad-library/detail/(\d+)', url)
            if match:
                return match.group(1)
            return None
        except Exception:
            return None
    
    def _build_detail_url(self, ad_id: str) -> str:
        """Build the detail page URL from ad ID"""
        return f"{self.base_url}/{ad_id}"
    
    def _extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """Extract JSON data from HTML response"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Method 1: Look for JSON in script tags
            script_tags = soup.find_all('script', type='application/json')
            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if data and isinstance(data, dict):
                        return data
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            # Method 2: Look for window variables
            patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
                r'window\.__APOLLO_STATE__\s*=\s*({.+?});',
                r'window\.__INITIAL_DATA__\s*=\s*({.+?});',
                r'window\.__data__\s*=\s*({.+?});',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html_content, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        if isinstance(data, dict) and data:
                            return data
                    except json.JSONDecodeError:
                        continue
            
            # Method 3: Look for JSON in any script tag
            all_scripts = soup.find_all('script')
            for script in all_scripts:
                if not script.string:
                    continue
                script_text = script.string.strip()
                if script_text.startswith('{') or script_text.startswith('['):
                    try:
                        data = json.loads(script_text)
                        if isinstance(data, dict) and data:
                            return data
                    except json.JSONDecodeError:
                        continue
            
            return None
            
        except Exception as e:
            print(f"Error extracting JSON from HTML: {e}")
            return None
    
    def scrape_ad_detail(self, ad_id: str) -> Optional[Dict]:
        """
        Scrape detailed information from a single ad detail page
        
        Args:
            ad_id: LinkedIn ad ID (e.g., "656802214")
            
        Returns:
            Dictionary containing ad details, or None if failed
        """
        url = self._build_detail_url(ad_id)
        
        try:
            print(f"Scraping ad ID: {ad_id}...")
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Initialize ad detail dictionary
                ad_detail = {
                    "ad_id": ad_id,
                    "url": url,
                    "advertiser": None,
                    "ad_text": None,
                    "ad_type": None,
                    "call_to_action": None,
                    "paid_for_by": None,
                    "images": [],
                    "videos": [],
                    "links": [],
                    "metadata": {}
                }
                
                # Extract JSON data first (most reliable)
                json_data = self._extract_json_from_html(response.text)
                if json_data:
                    ad_detail["metadata"]["json_data"] = json_data
                
                # Extract advertiser name
                # Look for "Nike" or advertiser name in various places
                advertiser_selectors = [
                    'h1',  # Main heading often contains advertiser
                    '[data-test-id="advertiser-name"]',
                    '.advertiser-name',
                    'h2',
                ]
                
                for selector in advertiser_selectors:
                    element = soup.select_one(selector)
                    if element:
                        text = element.get_text(strip=True)
                        if text and len(text) < 100:  # Reasonable advertiser name length
                            ad_detail["advertiser"] = text
                            break
                
                # Extract ad text/content
                # Look for main ad content
                content_selectors = [
                    '[data-test-id="ad-text"]',
                    '.ad-text',
                    '.ad-content',
                    'p',
                    '[class*="ad"]',
                ]
                
                ad_text_parts = []
                for selector in content_selectors:
                    elements = soup.select(selector)
                    for elem in elements[:5]:  # Limit to first 5 matches
                        text = elem.get_text(strip=True)
                        if text and len(text) > 10 and len(text) < 500:
                            # Avoid navigation/footer text
                            if not any(skip in text.lower() for skip in ['cookie', 'privacy', 'policy', 'about', 'linkedin corporation']):
                                ad_text_parts.append(text)
                
                if ad_text_parts:
                    ad_detail["ad_text"] = "\n".join(ad_text_parts[:3])  # Take first 3 relevant parts
                
                # Extract ad type (Video Ad, Image Ad, etc.)
                # Look for "Video Ad" or "Image Ad" text
                ad_type_patterns = [
                    r'(Video Ad|Image Ad|Carousel Ad|Single Image Ad)',
                    r'Ad Type[:\s]+(\w+)',
                ]
                
                page_text = soup.get_text()
                for pattern in ad_type_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        ad_detail["ad_type"] = match.group(1)
                        break
                
                # Extract call-to-action buttons/links
                cta_selectors = [
                    'button',
                    'a[class*="cta"]',
                    'a[class*="button"]',
                    '[data-test-id="cta"]',
                ]
                
                ctas = []
                for selector in cta_selectors:
                    elements = soup.select(selector)
                    for elem in elements[:5]:
                        text = elem.get_text(strip=True)
                        href = elem.get('href', '')
                        if text and len(text) < 50:
                            ctas.append({"text": text, "link": href})
                
                if ctas:
                    ad_detail["call_to_action"] = ctas
                
                # Extract "Paid for by" information
                paid_for_patterns = [
                    r'Paid for by[:\s]+(.+?)(?:\n|$)',
                    r'Paid for by[:\s]+(.+?)(?:\.|$)',
                ]
                
                for pattern in paid_for_patterns:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        ad_detail["paid_for_by"] = match.group(1).strip()
                        break
                
                # Extract images
                images = soup.find_all('img')
                for img in images:
                    src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
                    if src and src.startswith('http'):
                        if src not in ad_detail["images"]:
                            ad_detail["images"].append(src)
                
                # Extract videos
                videos = soup.find_all(['video', 'source'])
                for video in videos:
                    src = video.get('src') or video.get('data-src')
                    if src and src.startswith('http'):
                        if src not in ad_detail["videos"]:
                            ad_detail["videos"].append(src)
                
                # Extract all links
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href')
                    if href and href.startswith('http'):
                        if href not in ad_detail["links"]:
                            ad_detail["links"].append(href)
                
                # Extract any additional metadata from data attributes
                data_attrs = {}
                for elem in soup.find_all(attrs=lambda x: x and any(k.startswith('data-') for k in x.keys())):
                    for key, value in elem.attrs.items():
                        if key.startswith('data-'):
                            data_attrs[key] = value
                
                if data_attrs:
                    ad_detail["metadata"]["data_attributes"] = data_attrs
                
                print(f"✓ Successfully scraped ad ID: {ad_id}")
                return ad_detail
                
            else:
                print(f"✗ Failed to fetch ad ID {ad_id}: Status code {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching ad ID {ad_id}: {e}")
            return None
        except Exception as e:
            print(f"✗ Error parsing ad ID {ad_id}: {e}")
            # Save HTML for debugging
            try:
                debug_filename = f"ad_detail_debug_{ad_id}.html"
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"  Saved debug HTML to {debug_filename}")
            except:
                pass
            return None
    
    def scrape_ad_details(self, ad_ids: List[str], delay: float = 2.0) -> List[Dict]:
        """
        Scrape multiple ad detail pages
        
        Args:
            ad_ids: List of ad IDs to scrape
            delay: Delay between requests in seconds (default: 2.0)
            
        Returns:
            List of ad detail dictionaries
        """
        print(f"\n{'='*60}")
        print(f"Scraping {len(ad_ids)} ad detail pages...")
        print(f"{'='*60}\n")
        
        ad_details = []
        
        for i, ad_id in enumerate(ad_ids, 1):
            print(f"[{i}/{len(ad_ids)}] ", end="")
            detail = self.scrape_ad_detail(ad_id)
            
            if detail:
                ad_details.append(detail)
            
            # Rate limiting
            if i < len(ad_ids) and delay > 0:
                time.sleep(delay)
        
        print(f"\n{'='*60}")
        print(f"Scraping complete! Successfully scraped {len(ad_details)}/{len(ad_ids)} ads")
        print(f"{'='*60}\n")
        
        return ad_details
    
    def scrape_ad_details_from_urls(self, urls: List[str], delay: float = 2.0) -> List[Dict]:
        """
        Scrape ad details from a list of URLs
        
        Args:
            urls: List of LinkedIn ad detail URLs
            delay: Delay between requests in seconds (default: 2.0)
            
        Returns:
            List of ad detail dictionaries
        """
        # Extract ad IDs from URLs
        ad_ids = []
        for url in urls:
            ad_id = self._extract_ad_id_from_url(url)
            if ad_id:
                ad_ids.append(ad_id)
            else:
                print(f"Warning: Could not extract ad ID from URL: {url}")
        
        return self.scrape_ad_details(ad_ids, delay)
    
    def save_to_json(self, ad_details: List[Dict], filename: str = "ad_details.json"):
        """Save ad details to JSON file"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(ad_details, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved {len(ad_details)} ad details to {filename}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def save_to_csv(self, ad_details: List[Dict], filename: str = "ad_details.csv"):
        """Save ad details to CSV file (requires pandas)"""
        if not PANDAS_AVAILABLE:
            print("Error: pandas not installed. Install with: pip install pandas")
            return
        
        if not ad_details:
            print("No ad details to save")
            return
        
        try:
            # Flatten nested dictionaries for CSV
            flattened_details = []
            for detail in ad_details:
                flat_detail = self._flatten_dict(detail)
                flattened_details.append(flat_detail)
            
            df = pd.DataFrame(flattened_details)
            df.to_csv(filename, index=False, encoding="utf-8")
            print(f"✓ Saved {len(ad_details)} ad details to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = "_") -> Dict:
        """Flatten nested dictionary for CSV export"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert list to string representation
                items.append((new_key, json.dumps(v) if v else ""))
            else:
                items.append((new_key, v))
        return dict(items)


def main():
    """Example usage"""
    scraper = LinkedInAdDetailScraper()
    
    # Example 1: Scrape from ad IDs
    print("Example 1: Scraping from ad IDs...\n")
    ad_ids = ["656802214"]  # Add more ad IDs here
    details = scraper.scrape_ad_details(ad_ids, delay=2.0)
    
    if details:
        scraper.save_to_json(details, "ad_details.json")
        if PANDAS_AVAILABLE:
            scraper.save_to_csv(details, "ad_details.csv")
        
        # Print sample detail
        print("\nSample ad detail:")
        print(json.dumps(details[0], indent=2)[:1000])
    
    # Example 2: Scrape from URLs
    print("\n\nExample 2: Scraping from URLs...\n")
    urls = [
        "https://www.linkedin.com/ad-library/detail/656802214",
        # Add more URLs here
    ]
    details_from_urls = scraper.scrape_ad_details_from_urls(urls, delay=2.0)
    
    if details_from_urls:
        scraper.save_to_json(details_from_urls, "ad_details_from_urls.json")


if __name__ == "__main__":
    main()

