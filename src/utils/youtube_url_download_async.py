"""
This script is used to download HTML source code from YouTube URLs asynchronously.
Usage: python youtube_url_download_async.py --url <url>

Example:

# Download from a single URL
python youtube_url_download_async.py --url https://www.youtube.com/@lidangzzz/videos

# With custom concurrency
python youtube_url_download_async.py --url urls.csv --from-csv --concurrency 50

# With custom delays
python youtube_url_download_async.py --url urls.csv --from-csv --min-delay 0.1 --max-delay 0.2

# Download from a CSV file containing URLs
python youtube_url_download_async.py --url path/to/urls.csv --from-csv

# Download from a specific column in CSV file
python youtube_url_download_async.py --url /Users/yuanlu/Desktop/video_id.csv --from-csv --column video_id_url --output-dir /Users/yuanlu/Desktop/fetch_test

# Specify custom output directory
python youtube_url_download_async.py --url https://www.youtube.com/@lidangzzz/videos --output-dir data/source_code
"""

import aiohttp
import asyncio
import argparse
import os
import pandas as pd
from datetime import datetime
import time
import random
from typing import Optional, List, Dict
from urllib.parse import urlparse
import logging
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient, ExponentialRetry
from asyncio import Semaphore
from tqdm import tqdm

class AsyncYouTubeDownloader:
    def __init__(self, 
                 concurrency: int = 25,
                 min_delay: float = 0.1,
                 max_delay: float = 0.4,
                 output_dir: str = "output_dir",
                 timeout: int = 30):
        self.concurrency = concurrency
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.output_dir = output_dir
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrency)
        self.last_request_time: Dict[str, float] = {}
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Configure logging to write to output directory
        log_file = os.path.join(output_dir, "youtube_download.log")
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Remove any existing handlers to avoid duplicate logging
        self.logger.handlers = []
        
        # Add handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Put checkpoint file in output directory
        self.checkpoint_file = os.path.join(output_dir, "download_checkpoint.txt")
        self.completed_urls = self._load_checkpoint()
        
    def _load_checkpoint(self) -> set:
        """Load completed URLs from checkpoint file"""
        completed = set()
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    completed = set(line.strip() for line in f if line.strip())
                self.logger.info(f"Loaded {len(completed)} completed URLs from checkpoint")
            except Exception as e:
                self.logger.error(f"Error loading checkpoint: {e}")
        return completed
        
    def _save_checkpoint(self, url: str):
        """Save completed URL to checkpoint file"""
        try:
            with open(self.checkpoint_file, 'a') as f:
                f.write(f"{url}\n")
                f.flush()  # Force write to disk
                os.fsync(f.fileno())  # Ensure it's written to disk
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
            
    async def download_html(self, url: str, session: RetryClient) -> Optional[str]:
        """
        Download HTML content from the given URL with rate limiting.
        
        Args:
            url (str): The URL to download HTML from
            session (RetryClient): Aiohttp session with retry capability
            
        Returns:
            Optional[str]: HTML content if successful, None if failed
        """
        domain = urlparse(url).netloc
        
        # Rate limiting per domain
        current_time = time.time()
        if domain in self.last_request_time:
            time_since_last_request = current_time - self.last_request_time[domain]
            if time_since_last_request < self.min_delay:
                delay = random.uniform(self.min_delay - time_since_last_request, 
                                    self.max_delay - time_since_last_request)
                if delay > 0:
                    await asyncio.sleep(delay)
        
        # Update last request time
        self.last_request_time[domain] = time.time()
        
        try:
            # Add headers to mimic a browser request
            headers = {
                'User-Agent': random.choice([
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
                    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0'
                ])
            }
            
            async with self.semaphore:
                async with session.get(url, headers=headers) as response:
                    if response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', 60))
                        self.logger.warning(f"Rate limited on {domain}. Waiting {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        return None
                        
                    response.raise_for_status()
                    return await response.text()
                    
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return None
            
    def save_html(self, html_content: str, url: str) -> str:
        """
        Save HTML content to a file in the specified directory.
        
        Args:
            html_content (str): The HTML content to save
            url (str): The URL from which the content was downloaded
            
        Returns:
            str: Path to the saved file
        """
        # Create filename from URL and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_url = url.replace("://", "_").replace("/", "_").replace(".", "_")
        filename = f"{safe_url}_{timestamp}.html"
        filepath = os.path.join(self.output_dir, filename)
        
        # Save the content
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return filepath
        
    async def process_urls(self, urls: List[str]) -> Dict[str, bool]:
        """Process multiple URLs with accurate progress tracking"""
        # Initialize progress bar
        pbar = tqdm(total=len(urls), desc="Downloading", unit="channel")
        results = {}
        
        # Group URLs by domain to handle rate limiting per domain
        domain_urls = self._group_urls_by_domain(urls)
        
        async with aiohttp.ClientSession() as session:
            retry_options = ExponentialRetry(attempts=3)
            retry_client = RetryClient(client_session=session, retry_options=retry_options)
            
            for domain, domain_url_list in domain_urls.items():
                domain_results = []
                for url in domain_url_list:
                    if url in self.completed_urls:
                        domain_results.append((url, True))
                        pbar.update(1)
                        continue
                        
                    try:
                        success = await self.process_url(url, retry_client)
                        domain_results.append((url, success))
                        
                        if success:
                            self._save_checkpoint(url)
                            self.completed_urls.add(url)
                            
                        # Update progress only after actual download attempt
                        pbar.update(1)
                        pbar.set_postfix({
                            "success": len([r for _, r in domain_results if r]),
                            "domain": domain
                        })
                        
                        # Add delay between requests to same domain
                        await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))
                        
                    except Exception as e:
                        self.logger.error(f"Error processing {url}: {e}")
                        domain_results.append((url, False))
                        pbar.update(1)
                
                results.update(dict(domain_results))
                
        pbar.close()
        return results

    async def process_url(self, url: str, session: RetryClient) -> bool:
        """Process single URL with rate limit handling"""
        async with self.semaphore:
            try:
                html_content = await self.download_html(url, session)
                if html_content:
                    filepath = self.save_html(html_content, url)
                    self.logger.debug(f"Saved {url} to {filepath}")
                    return True
                return False
            except Exception as e:
                self.logger.error(f"Failed to process {url}: {e}")
                return False

    async def download_html(self, url: str, session: RetryClient) -> Optional[str]:
        """Download HTML with proper rate limit handling"""
        domain = urlparse(url).netloc
        headers = {
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
                'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0'
            ])
        }
        
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 429:
                    wait_time = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited on {domain}. Waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    return await self.download_html(url, session)
                else:
                    self.logger.warning(f"Unexpected status {response.status} for {url}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Download error for {url}: {e}")
            return None

    def _group_urls_by_domain(self, urls: List[str]) -> Dict[str, List[str]]:
        """Group URLs by domain for better rate limit handling"""
        domain_urls = {}
        for url in urls:
            domain = urlparse(url).netloc
            if domain not in domain_urls:
                domain_urls[domain] = []
            domain_urls[domain].append(url)
        return domain_urls

async def download_channel_info(session, url, semaphore):
    """Download info for a single channel with rate limiting"""
    async with semaphore:  # Use semaphore to limit concurrent requests
        try:
            async with session.get(url) as response:
                if response.status == 429:  # Rate limit hit
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"Rate limited. Waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    return await download_channel_info(session, url, semaphore)
                
                # ... rest of download logic ...
                
        except Exception as e:
            logging.error(f"Error downloading {url}: {e}")
            return None

async def download_all_channels(urls):
    """Download info for multiple channels with rate limiting"""
    # Limit concurrent connections
    semaphore = Semaphore(5)  # Allow max 5 concurrent requests
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(
                download_channel_info(session, url, semaphore)
            )
            tasks.append(task)
            
            # Add small delay between starting tasks
            await asyncio.sleep(0.1)
            
        results = await asyncio.gather(*tasks)
        return results

async def main():
    parser = argparse.ArgumentParser(description='Download HTML from YouTube URLs asynchronously')
    parser.add_argument('--url', type=str, required=True, help='URL or path to CSV file containing URLs')
    parser.add_argument('--output-dir', type=str, default='output_dir', help='Output directory')
    parser.add_argument('--from-csv', action='store_true', help='Treat input as CSV file containing URLs')
    parser.add_argument('--column', type=str, help='Column name in CSV containing URLs')
    parser.add_argument('--min-delay', type=float, default=0.1, help='Minimum delay between requests to same domain')
    parser.add_argument('--max-delay', type=float, default=0.2, help='Maximum delay between requests to same domain')
    parser.add_argument('--concurrency', type=int, default=100, help='Maximum number of concurrent downloads')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout in seconds')
    
    args = parser.parse_args()
    
    downloader = AsyncYouTubeDownloader(
        concurrency=args.concurrency,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        output_dir=args.output_dir,
        timeout=args.timeout
    )
    
    if args.from_csv:
        try:
            df = pd.read_csv(args.url)
            
            if args.column:
                if args.column not in df.columns:
                    raise ValueError(f"Column '{args.column}' not found in CSV. Available columns: {', '.join(df.columns)}")
                url_column = args.column
            else:
                url_column = 'url' if 'url' in df.columns else df.columns[0]
                logging.info(f"Using column: {url_column}")
                
            urls = df[url_column].tolist()
            logging.info(f"Found {len(urls)} URLs to download")
            
            await downloader.process_urls(urls)
            
        except Exception as e:
            logging.error(f"Error processing CSV: {str(e)}")
            return
            
    else:
        await downloader.process_urls([args.url])

if __name__ == "__main__":
    asyncio.run(main()) 