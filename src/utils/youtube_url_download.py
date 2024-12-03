"""
This script is used to download the html source code from any given url.
Usage: python youtube_url_download.py --url <url>

Example:

# Download from a single URL
python youtube_url_download.py --url https://www.youtube.com/@lidangzzz/videos

# With custom delays
python youtube_url_download.py --url urls.csv --from-csv --min-delay 2 --max-delay 5

# Default delays (1-3 seconds)
python youtube_url_download.py --url urls.csv --from-csv

# Download from a CSV file containing URLs
python youtube_url_download.py --url path/to/urls.csv --from-csv

# Download from a specific column in CSV file
python youtube_url_download.py --url /Users/yuanlu/Code/youtube-top-10000-channels/data/split_0.csv --from-csv --column validated_url --output-dir /Users/yuanlu/Code/youtube-top-10000-channels/data/split_0

# Specify custom output directory
python youtube_url_download.py --url https://www.youtube.com/@lidangzzz/videos --output-dir /Users/yuanlu/Code/youtube-top-10000-channels/data/source_code
"""

import requests
import argparse
from typing import Optional
import os
import pandas as pd
from datetime import datetime
import time
import random


def download_html(url: str, delay_range: tuple = (1, 2)) -> Optional[str]:
    """
    Download HTML content from the given URL with rate limiting.
    
    Args:
        url (str): The URL to download HTML from
        delay_range (tuple): Range of seconds to wait between requests (min, max)
        
    Returns:
        Optional[str]: HTML content if successful, None if failed
    """
    try:
        # Random delay before request
        delay = random.uniform(*delay_range)
        time.sleep(delay)
        
        # Add headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Additional delay if response indicates rate limiting
        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            time.sleep(retry_after)
            
        return response.text
        
    except requests.RequestException as e:
        print(f"Error downloading URL: {e}")
        
        # Handle rate limiting errors specifically
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
            retry_after = int(e.response.headers.get('Retry-After', 60))
            print(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            
        return None


def save_html(html_content: str, url: str, output_dir: str = "@data") -> str:
    """
    Save HTML content to a file in the specified directory.
    
    Args:
        html_content (str): The HTML content to save
        url (str): The URL from which the content was downloaded
        output_dir (str): Directory to save the file
        
    Returns:
        str: Path to the saved file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename from URL and timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_url = url.replace("://", "_").replace("/", "_").replace(".", "_")
    filename = f"{safe_url}_{timestamp}.html"
    filepath = os.path.join(output_dir, filename)
    
    # Save the content
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return filepath


def main():
    parser = argparse.ArgumentParser(description='Download HTML from a given URL')
    parser.add_argument('--url', type=str, required=True, help='URL to download HTML from')
    parser.add_argument('--output-dir', type=str, default='@data', help='Output directory (default: @data)')
    parser.add_argument('--from-csv', action='store_true', help='Treat input as CSV file containing URLs')
    parser.add_argument('--column', type=str, help='Specify the column name in CSV file containing URLs')
    parser.add_argument('--min-delay', type=float, default=1, help='Minimum delay between requests in seconds')
    parser.add_argument('--max-delay', type=float, default=3, help='Maximum delay between requests in seconds')
    
    args = parser.parse_args()
    
    if args.from_csv:
        try:
            df = pd.read_csv(args.url)
            
            if args.column:
                if args.column not in df.columns:
                    raise ValueError(f"Column '{args.column}' not found in CSV. Available columns: {', '.join(df.columns)}")
                url_column = args.column
            else:
                url_column = 'url' if 'url' in df.columns else df.columns[0]
                print(f"Using column: {url_column}")
            
            urls = df[url_column].tolist()
            print(f"Found {len(urls)} URLs to download")
            
            # Group URLs by domain to respect per-domain rate limits
            from urllib.parse import urlparse
            domain_urls = {}
            for url in urls:
                domain = urlparse(url).netloc
                if domain not in domain_urls:
                    domain_urls[domain] = []
                domain_urls[domain].append(url)
            
            print(f"Found {len(domain_urls)} unique domains")
            
            # Process URLs domain by domain
            for domain, domain_url_list in domain_urls.items():
                print(f"\nProcessing domain: {domain} ({len(domain_url_list)} URLs)")
                for i, url in enumerate(domain_url_list, 1):
                    print(f"Downloading {i}/{len(domain_url_list)}: {url}")
                    html_content = download_html(url, delay_range=(args.min_delay, args.max_delay))
                    if html_content:
                        filepath = save_html(html_content, url, args.output_dir)
                        print(f"Saved to {filepath}")
                
                # Reduced delay between domains
                if domain != list(domain_urls.keys())[-1]:
                    domain_delay = random.uniform(0.5, 1)
                    print(f"Waiting {domain_delay:.1f}s before processing next domain...")
                    time.sleep(domain_delay)
            
        except Exception as e:
            print(f"Error: {e}")
            return
            
    else:
        html_content = download_html(args.url, delay_range=(args.min_delay, args.max_delay))
        if html_content:
            filepath = save_html(html_content, args.url, args.output_dir)
            print(f"HTML content saved to {filepath}")


if __name__ == "__main__":
    main()