"""
LinkedIn Ad Library Scraper - HTML Page Scraping Approach
Scrapes ads by fetching the full HTML page and parsing it
Handles pagination by modifying URL parameters
"""

import requests
import json
import time
import os
import re
from typing import List, Dict, Optional
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urlencode, parse_qs, urlparse, urlunparse

# Optional: for CSV export
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed. CSV export disabled. Install with: pip install pandas")


class LinkedInAdScraperHTML:
    """
    Scraper for LinkedIn Ad Library using HTML page scraping
    Usage:
        scraper = LinkedInAdScraperHTML()
        ads = scraper.scrape_ads("Nike", max_results=100)
        scraper.save_to_json(ads, "nike_ads.json")
        scraper.save_to_csv(ads, "nike_ads.csv")
    """
    
    def __init__(self):
        """Initialize scraper with headers"""
        self.ua = UserAgent()
        self.base_url = "https://www.linkedin.com/ad-library/search"
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
    
    def _build_search_url(self, account_owner: str, keyword: str = "", 
                         countries: List[str] = None, start: int = 0, 
                         startdate: str = "", enddate: str = "") -> str:
        """
        Build the search URL with all parameters
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            keyword: Search keyword (optional)
            countries: List of country codes (default: ["ALL"])
            start: Starting index for pagination (default: 0)
            startdate: Start date filter (optional)
            enddate: End date filter (optional)
            
        Returns:
            Complete search URL
        """
        if countries is None:
            countries = ["ALL"]
        
        params = {
            "accountOwner": account_owner,
            "keyword": keyword,
            "startdate": startdate,
            "enddate": enddate,
        }
        
        # Add all countries as separate parameters
        for country in countries:
            params[f"countries"] = country
        
        # Add pagination parameter if start > 0
        if start > 0:
            params["start"] = str(start)
        
        # Build URL with parameters
        query_string = urlencode(params, doseq=True)
        url = f"{self.base_url}?{query_string}"
        
        return url
    
    def _extract_json_from_html(self, html_content: str) -> Optional[Dict]:
        """
        Extract JSON data from HTML response
        LinkedIn embeds JSON data in <script> tags or JavaScript variables
        
        Args:
            html_content: HTML response text
            
        Returns:
            Extracted JSON data as dictionary, or None if not found
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Method 1: Look for JSON in script tags with type="application/json"
            script_tags = soup.find_all('script', type='application/json')
            for script in script_tags:
                try:
                    data = json.loads(script.string)
                    if data and isinstance(data, dict):
                        return data
                except (json.JSONDecodeError, AttributeError):
                    continue
            
            # Method 2: Look for window.__INITIAL_STATE__ or similar patterns
            patterns = [
                r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
                r'window\.__APOLLO_STATE__\s*=\s*({.+?});',
                r'window\.__INITIAL_DATA__\s*=\s*({.+?});',
                r'window\.__data__\s*=\s*({.+?});',
                r'"elements"\s*:\s*(\[.+?\])',  # Look for "elements" array
                r'"results"\s*:\s*(\[.+?\])',   # Look for "results" array
                r'"ads"\s*:\s*(\[.+?\])',        # Look for "ads" array
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
            
            # Method 3: Look for any JSON-like structures in script tags
            all_scripts = soup.find_all('script')
            for script in all_scripts:
                if not script.string:
                    continue
                script_text = script.string.strip()
                
                # Look for JSON objects/arrays
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
        """
        Extract ad data from HTML page
        Tries multiple methods to find ad information
        
        Args:
            html_content: HTML response text
            
        Returns:
            List of ad dictionaries
        """
        ads = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Method 1: Try to extract JSON data first
            json_data = self._extract_json_from_html(html_content)
            if json_data:
                # Extract ads from JSON structure
                if isinstance(json_data, dict):
                    for key in ['elements', 'results', 'data', 'ads', 'items']:
                        if key in json_data and isinstance(json_data[key], list):
                            ads.extend(json_data[key])
                            break
                elif isinstance(json_data, list):
                    ads = json_data
            
            # Method 2: If no JSON found, try parsing HTML structure
            if not ads:
                # Look for common ad container classes/IDs
                # LinkedIn might use specific classes for ad cards
                ad_containers = soup.find_all(['div', 'article', 'section'], 
                                               class_=re.compile(r'ad|card|item|result', re.I))
                
                for container in ad_containers:
                    ad_data = {}
                    
                    # Extract text content
                    text = container.get_text(strip=True)
                    if text:
                        ad_data['text'] = text
                    
                    # Extract images
                    images = container.find_all('img')
                    if images:
                        ad_data['images'] = [img.get('src') or img.get('data-src') for img in images]
                    
                    # Extract links
                    links = container.find_all('a', href=True)
                    if links:
                        ad_data['links'] = [link.get('href') for link in links]
                    
                    # Extract any data attributes
                    for attr in container.attrs:
                        if 'data' in attr.lower():
                            ad_data[attr] = container.get(attr)
                    
                    if ad_data:
                        ads.append(ad_data)
            
            return ads
            
        except Exception as e:
            print(f"Error extracting ads from HTML: {e}")
            return []
    
    def fetch_page(self, account_owner: str, keyword: str = "", 
                   countries: List[str] = None, start: int = 0,
                   startdate: str = "", enddate: str = "") -> Optional[List[Dict]]:
        """
        Fetch a single page of ads from LinkedIn Ad Library
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            keyword: Search keyword (optional)
            countries: List of country codes (default: ["ALL"])
            start: Starting index for pagination (default: 0)
            startdate: Start date filter (optional)
            enddate: End date filter (optional)
            
        Returns:
            List of ad dictionaries, or None if request fails
        """
        url = self._build_search_url(account_owner, keyword, countries, start, startdate, enddate)
        
        try:
            print(f"Fetching page: {account_owner}, start={start}...")
            print(f"URL: {url[:100]}...")  # Print first 100 chars of URL
            
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 200:
                # Extract ads from HTML
                ads = self._extract_ads_from_html(response.text)
                
                if ads:
                    print(f"✓ Extracted {len(ads)} ads from HTML")
                    return ads
                else:
                    print("No ads found in HTML response")
                    # Save HTML for debugging
                    debug_filename = f"linkedin_debug_{account_owner}_{start}.html"
                    with open(debug_filename, "w", encoding="utf-8") as f:
                        f.write(response.text)
                    print(f"Saved HTML to {debug_filename} for inspection")
                    return []
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def scrape_ads(self, account_owner: str, keyword: str = "", 
                   countries: List[str] = None, max_results: int = 100,
                   results_per_page: int = 12, delay: float = 2.0,
                   startdate: str = "", enddate: str = "") -> List[Dict]:
        """
        Scrape all ads for a given advertiser with pagination
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            keyword: Search keyword (optional)
            countries: List of country codes (default: ["ALL"])
            max_results: Maximum number of ads to scrape (default: 100)
            results_per_page: Number of results per page (default: 12)
            delay: Delay between requests in seconds (default: 2.0)
            startdate: Start date filter (optional)
            enddate: End date filter (optional)
            
        Returns:
            List of ad dictionaries
        """
        if countries is None:
            countries = ["ALL"]
        
        all_ads = []
        start = 0
        
        print(f"\n{'='*60}")
        print(f"Scraping ads for: {account_owner}")
        if keyword:
            print(f"Keyword filter: {keyword}")
        print(f"Countries: {', '.join(countries[:5])}{'...' if len(countries) > 5 else ''}")
        print(f"Max results: {max_results}")
        print(f"{'='*60}\n")
        
        while len(all_ads) < max_results:
            # Fetch current page
            ads = self.fetch_page(account_owner, keyword, countries, start, startdate, enddate)
            
            if ads is None:
                print("Request failed, stopping")
                break
            
            if not ads:
                print("No more ads found, stopping")
                break
            
            # Add ads to collection
            ads_to_add = ads[:max_results - len(all_ads)]
            all_ads.extend(ads_to_add)
            
            print(f"✓ Total ads collected: {len(all_ads)}/{max_results}")
            
            # Check if we've reached the end or max results
            if len(ads) < results_per_page or len(all_ads) >= max_results:
                break
            
            # Update start for next page
            start += results_per_page
            
            # Rate limiting - be respectful
            if delay > 0:
                time.sleep(delay)
        
        print(f"\n{'='*60}")
        print(f"Scraping complete! Total ads collected: {len(all_ads)}")
        print(f"{'='*60}\n")
        
        return all_ads
    
    def save_to_json(self, ads: List[Dict], filename: str = "linkedin_ads.json"):
        """Save ads to JSON file"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(ads, f, indent=2, ensure_ascii=False)
            print(f"✓ Saved {len(ads)} ads to {filename}")
        except Exception as e:
            print(f"Error saving to JSON: {e}")
    
    def save_to_csv(self, ads: List[Dict], filename: str = "linkedin_ads.csv"):
        """Save ads to CSV file (requires pandas)"""
        if not PANDAS_AVAILABLE:
            print("Error: pandas not installed. Install with: pip install pandas")
            return
        
        if not ads:
            print("No ads to save")
            return
        
        try:
            # Flatten nested dictionaries for CSV
            flattened_ads = []
            for ad in ads:
                flat_ad = self._flatten_dict(ad)
                flattened_ads.append(flat_ad)
            
            df = pd.DataFrame(flattened_ads)
            df.to_csv(filename, index=False, encoding="utf-8")
            print(f"✓ Saved {len(ads)} ads to {filename}")
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
    
    def download_creatives(self, ads: List[Dict], output_dir: str = "ad_creatives"):
        """
        Download images/videos from ads
        
        Args:
            ads: List of ad dictionaries
            output_dir: Directory to save downloaded files
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print(f"\nDownloading creatives to {output_dir}/...")
        
        downloaded = 0
        for i, ad in enumerate(ads):
            # Extract media URLs
            media_urls = []
            
            if isinstance(ad, dict):
                # Check for image URLs in various formats
                for key in ["images", "image", "media", "imageUrl", "videoUrl", "creative"]:
                    if key in ad:
                        media = ad[key]
                        if isinstance(media, str):
                            media_urls.append(media)
                        elif isinstance(media, list):
                            media_urls.extend([m for m in media if isinstance(m, str)])
                        elif isinstance(media, dict) and "url" in media:
                            media_urls.append(media["url"])
            
            # Download each media URL
            for j, url in enumerate(media_urls):
                if not url or not url.startswith("http"):
                    continue
                
                try:
                    # Determine file extension
                    ext = ".jpg"
                    if ".png" in url.lower():
                        ext = ".png"
                    elif ".gif" in url.lower():
                        ext = ".gif"
                    elif ".mp4" in url.lower() or "video" in url.lower():
                        ext = ".mp4"
                    
                    filename = f"{output_dir}/ad_{i}_{j}{ext}"
                    
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200:
                        with open(filename, "wb") as f:
                            f.write(response.content)
                        downloaded += 1
                        print(f"  ✓ Downloaded: {filename}")
                    
                    time.sleep(0.5)  # Small delay between downloads
                    
                except Exception as e:
                    print(f"  ✗ Failed to download {url}: {e}")
        
        print(f"\n✓ Downloaded {downloaded} creative files\n")


def main():
    """Example usage"""
    scraper = LinkedInAdScraperHTML()
    
    # Example: Scrape Nike ads
    print("Example: Scraping Nike ads from HTML page...\n")
    
    # You can specify countries or use ["ALL"] for all countries
    # For the full list like in your URL, you can pass all country codes
    countries = ["ALL"]  # Or specify specific countries: ["US", "GB", "IN"]
    
    ads = scraper.scrape_ads(
        account_owner="Nike",
        keyword="",  # Optional keyword filter
        countries=countries,
        max_results=50,  # Limit to 50 ads for testing
        results_per_page=12,
        delay=2.0  # 2 second delay between requests (be respectful)
    )
    
    if ads:
        # Save to JSON
        scraper.save_to_json(ads, "nike_ads.json")
        
        # Save to CSV (if pandas available)
        if PANDAS_AVAILABLE:
            scraper.save_to_csv(ads, "nike_ads.csv")
        
        # Optionally download creatives
        # scraper.download_creatives(ads, "nike_creatives")
        
        # Print sample ad
        if ads:
            print("\nSample ad structure:")
            print(json.dumps(ads[0], indent=2)[:500])
    else:
        print("No ads found. Check:")
        print("1. The URL structure is correct")
        print("2. Headers match what browser sends")
        print("3. The advertiser name is correct")
        print("4. Check the debug HTML files to see the actual page structure")


if __name__ == "__main__":
    main()

