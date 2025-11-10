"""
LinkedIn Ad Library Scraper
Scrapes ads from LinkedIn Ad Library API without authentication
Follows the approach: Find API endpoint → Send requests → Paginate → Parse & Save
"""

import requests
import json
import time
import os
import re
from typing import List, Dict, Optional
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

# Optional: for CSV export
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Note: pandas not installed. CSV export disabled. Install with: pip install pandas")


class LinkedInAdScraper:
    """
    Scraper for LinkedIn Ad Library API
    Usage:
        scraper = LinkedInAdScraper()
        ads = scraper.scrape_ads("Nike", max_results=100)
        scraper.save_to_json(ads, "nike_ads.json")
        scraper.save_to_csv(ads, "nike_ads.csv")
    """
    
    def __init__(self):
        """Initialize scraper with headers"""
        self.ua = UserAgent()
        self.base_url = "https://www.linkedin.com/ad-library/api/search"
        self.session = requests.Session()
        self._setup_headers()
        
    def _setup_headers(self):
        """Setup request headers to mimic browser"""
        self.headers = {
            "User-Agent": self.ua.random,
            "Accept": "application/json, text/javascript, */*; q=0.01",
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
    
    def fetch_page(self, account_owner: str, keyword: str = "", count: int = 12, start: int = 0) -> Optional[Dict]:
        """
        Fetch a single page of ads from LinkedIn Ad Library API
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            keyword: Search keyword (optional)
            count: Number of results per page (default: 12)
            start: Starting index for pagination (default: 0)
            
        Returns:
            JSON response as dictionary, or None if request fails
        """
        params = {
            "accountOwner": account_owner,
            "keyword": keyword,
            "count": count,
            "start": start
        }
        
        try:
            print(f"Fetching page: start={start}, count={count}...")
            response = self.session.get(self.base_url, params=params, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except json.JSONDecodeError:
                    print(f"Warning: Non-JSON response received")
                    print(f"First 500 chars: {response.text[:500]}")
                    return None
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response: {response.text[:200]}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def scrape_ads(self, account_owner: str, keyword: str = "", max_results: int = 100, 
                   results_per_page: int = 12, delay: float = 1.0) -> List[Dict]:
        """
        Scrape all ads for a given advertiser with pagination
        
        Args:
            account_owner: Advertiser name (e.g., "Nike")
            keyword: Search keyword (optional)
            max_results: Maximum number of ads to scrape (default: 100)
            results_per_page: Number of results per page (default: 12)
            delay: Delay between requests in seconds (default: 1.0)
            
        Returns:
            List of ad dictionaries
        """
        all_ads = []
        start = 0
        
        print(f"\n{'='*60}")
        print(f"Scraping ads for: {account_owner}")
        if keyword:
            print(f"Keyword filter: {keyword}")
        print(f"Max results: {max_results}")
        print(f"{'='*60}\n")
        
        while len(all_ads) < max_results:
            # Fetch current page
            data = self.fetch_page(account_owner, keyword, results_per_page, start)
            
            if not data:
                print("No more data available or request failed")
                break
            
            # Extract ads from response
            # LinkedIn API structure may vary - adjust based on actual response
            ads = []
            
            # Try different possible response structures
            if isinstance(data, dict):
                if "elements" in data:
                    ads = data["elements"]
                elif "results" in data:
                    ads = data["results"]
                elif "data" in data:
                    ads = data["data"]
                elif "ads" in data:
                    ads = data["ads"]
                else:
                    # If response is a list or single ad object
                    if isinstance(data, list):
                        ads = data
                    else:
                        # Try to extract any array-like structure
                        ads = [data] if data else []
            
            if not ads:
                print(f"No ads found in response. Response structure:")
                print(json.dumps(data, indent=2)[:500])
                break
            
            # Add ads to collection
            ads_to_add = ads[:max_results - len(all_ads)]
            all_ads.extend(ads_to_add)
            
            print(f"✓ Fetched {len(ads_to_add)} ads (Total: {len(all_ads)})")
            
            # Check if we've reached the end
            if len(ads) < results_per_page:
                print("Reached last page")
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
            # Extract media URLs (structure may vary)
            media_urls = []
            
            if isinstance(ad, dict):
                # Try different possible keys for media
                for key in ["media", "image", "video", "creative", "imageUrl", "videoUrl"]:
                    if key in ad:
                        media = ad[key]
                        if isinstance(media, str):
                            media_urls.append(media)
                        elif isinstance(media, dict):
                            if "url" in media:
                                media_urls.append(media["url"])
                            elif "src" in media:
                                media_urls.append(media["src"])
                        elif isinstance(media, list):
                            for item in media:
                                if isinstance(item, str):
                                    media_urls.append(item)
                                elif isinstance(item, dict) and "url" in item:
                                    media_urls.append(item["url"])
            
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
    scraper = LinkedInAdScraper()
    
    # Example 1: Scrape Nike ads
    print("Example: Scraping Nike ads...\n")
    ads = scraper.scrape_ads(
        account_owner="Nike",
        keyword="",  # Optional keyword filter
        max_results=50,  # Limit to 50 ads for testing
        results_per_page=12,
        delay=1.0  # 1 second delay between requests
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
        print("1. The API endpoint URL is correct")
        print("2. Headers match what browser sends")
        print("3. The advertiser name is correct")
        print("4. LinkedIn hasn't changed their API structure")


if __name__ == "__main__":
    main()

