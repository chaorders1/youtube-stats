"""
YouTube CSV Validator using YouTube Data API v3.
Validates YouTube channel URLs from a CSV file and extracts channel information.

Example:
    python youtube_csv_validator_api.py --input_file "channels.csv" --url_column "Youtube_Channel_URL" --api_key "YOUR_API_KEY"
    python youtube_csv_validator_api.py --input_file "./data/youtube_channels.csv" --url_column "Youtube_Channel_URL" --api_key "YOUR_API_KEY"
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import argparse
import sys
import time
import re
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from collections import deque
import warnings
from googleapiclient.discovery_cache.base import Cache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_validator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
warnings.filterwarnings('ignore', message='file_cache is unavailable when using oauth2client >= 4.0.0')

class MemoryCache(Cache):
    """A simple in-memory cache for Google API discovery documents."""
    _CACHE = {}

    def get(self, url):
        return MemoryCache._CACHE.get(url)

    def set(self, url, content):
        MemoryCache._CACHE[url] = content

class RateLimiter:
    """Manages API request rates."""
    def __init__(self, max_requests_per_min: int = 1800000, daily_quota: int = 10000):
        """
        Initialize rate limiter.
        
        Args:
            max_requests_per_min (int): Maximum requests allowed per minute
            daily_quota (int): Daily quota limit
        """
        self.max_requests_per_min = max_requests_per_min
        self.daily_quota = daily_quota
        self.requests = deque()
        self.quota_used = 0
    
    def can_proceed(self, quota_cost: int) -> bool:
        """Check if a new request can proceed."""
        now = datetime.now()
        
        # Check minute-based rate limit
        while self.requests and (now - self.requests[0]).total_seconds() > 60:
            self.requests.popleft()
            
        if len(self.requests) >= self.max_requests_per_min:
            return False
            
        # Check daily quota limit
        if self.quota_used + quota_cost > self.daily_quota:
            return False
            
        return True
    
    def add_request(self, quota_cost: int):
        """Record a new request."""
        self.requests.append(datetime.now())
        self.quota_used += quota_cost

class YouTubeValidator:
    """Validates YouTube channel URLs and extracts channel information using YouTube Data API."""
    
    def __init__(self, api_key: str):
        """
        Initialize YouTube API client.
        
        Args:
            api_key (str): YouTube Data API key
        """
        self._api_key = api_key
        self._rate_limiter = RateLimiter(max_requests_per_min=1800000, daily_quota=10000)
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._quota_used = 0
        self._daily_quota = 10000  # Default quota
        
    def _get_youtube_client(self):
        """Create a new YouTube client instance for each thread."""
        return build('youtube', 'v3', developerKey=self._api_key)

    def _extract_channel_id(self, url: str) -> Optional[str]:
        """Extract channel ID from various YouTube URL formats."""
        patterns = {
            'channel': r'youtube\.com/channel/(UC[\w-]+)',
            'user': r'youtube\.com/user/([\w-]+)',
            'custom': r'youtube\.com/(@[\w-]+)',
            'c': r'youtube\.com/c/([\w-]+)',
        }
        
        for pattern in patterns.values():
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    async def _wait_for_rate_limit(self, quota_cost: int = 1):
        """
        Wait until we can make another request.
        
        Args:
            quota_cost (int): Cost in quota units for the upcoming request
        """
        while not self._rate_limiter.can_proceed(quota_cost):
            await asyncio.sleep(0.005)
        self._rate_limiter.add_request(quota_cost)

    def _track_quota(self, cost: int):
        """Track quota usage"""
        self._quota_used += cost
        if self._quota_used >= self._daily_quota:
            raise Exception("Daily quota limit reached")
            
    async def get_channel_info_async(self, identifier: str) -> Dict[str, Any]:
        """Asynchronous version of channel info retrieval."""
        # Use different quota costs based on the request type
        quota_cost = 100 if identifier.startswith('@') else 1
        await self._wait_for_rate_limit(quota_cost)
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._get_channel_info_sync,
            identifier
        )
    
    def _get_channel_info_sync(self, identifier: str) -> Dict[str, Any]:
        """Synchronous version of get_channel_info."""
        try:
            youtube = self._get_youtube_client()
            
            # Try direct channel.list first for all types (costs only 1 unit)
            try:
                channel_response = youtube.channels().list(
                    id=identifier if identifier.startswith('UC') else None,
                    forUsername=identifier if not identifier.startswith(('@', 'UC')) else None,
                    part='snippet,statistics'
                ).execute()
                
                if channel_response['items']:
                    # Process channel info...
                    return {...}
                    
            except HttpError:
                pass
            
            # Only use search as last resort for custom URLs
            if identifier.startswith('@'):
                response = youtube.search().list(
                    q=identifier,
                    type='channel',
                    part='id'
                ).execute()
                # Process search results...

        except HttpError as e:
            error_message = str(e)
            if 'quotaExceeded' in error_message:
                raise Exception("YouTube API quota exceeded") from e
            return {'error': f'API error: {error_message}', 'is_valid': False}
        except Exception as e:
            return {'error': f'Validation error: {str(e)}', 'is_valid': False}

class YoutubeCSVValidator:
    """Validates YouTube channel URLs from CSV files using YouTube API."""

    def __init__(self, input_file: str, url_column: str, api_key: str, limit: Optional[int] = None):
        """
        Initialize the validator.

        Args:
            input_file (str): Path to input CSV file
            url_column (str): Name of column containing YouTube URLs
            api_key (str): YouTube Data API key
            limit (Optional[int]): Maximum number of URLs to process
        """
        self._input_file = Path(input_file)
        self._url_column = url_column
        self._limit = limit
        self._validator = YouTubeValidator(api_key)
        self._df: Optional[pd.DataFrame] = None

    def _load_csv(self) -> None:
        """Load and prepare the CSV file."""
        try:
            self._df = pd.read_csv(self._input_file)
            
            if self._url_column not in self._df.columns:
                raise ValueError(f"Column '{self._url_column}' not found in CSV file")
            
            # Initialize result columns
            new_columns = {
                'channel_id': '',
                'channel_title': '',
                'subscribers': pd.NA,
                'handle': '',
                'is_valid': pd.NA,
                'error': ''
            }
            
            for col, default_value in new_columns.items():
                if col not in self._df.columns:
                    self._df[col] = default_value
            
            if self._limit:
                self._df = self._df.iloc[:self._limit].copy()
                
            logging.info(f"Loaded CSV file with {len(self._df)} rows")
            
        except Exception as e:
            logging.error(f"Error loading CSV: {str(e)}")
            raise

    async def _process_batch(self, batch_df: pd.DataFrame) -> None:
        """Process a batch of channels concurrently."""
        tasks = []
        logging.info(f"Starting batch processing of {len(batch_df)} channels")
        
        for index, row in batch_df.iterrows():
            url = row[self._url_column]
            channel_id = self._validator._extract_channel_id(url)
            
            if not channel_id:
                self._df.loc[index, 'error'] = 'Invalid URL format'
                self._df.loc[index, 'is_valid'] = False
                logging.warning(f"Invalid URL format: {url}")
                continue
            
            logging.debug(f"Creating task for channel ID: {channel_id}")
            task = asyncio.create_task(
                self._validator.get_channel_info_async(channel_id)
            )
            tasks.append((index, task))
        
        logging.info(f"Created {len(tasks)} tasks, waiting for completion...")
        
        for index, task in tasks:
            try:
                result = await task
                logging.debug(f"Processed channel at index {index}")
                
                if 'error' in result and result['error']:
                    logging.warning(f"Error for channel at index {index}: {result['error']}")
                
                for key, value in result.items():
                    if key in self._df.columns:
                        self._df.loc[index, key] = value
            except Exception as e:
                error_msg = str(e)
                logging.error(f"Error processing channel at index {index}: {error_msg}")
                self._df.loc[index, 'error'] = error_msg
                self._df.loc[index, 'is_valid'] = False

    async def process_async(self) -> None:
        """Process the CSV file asynchronously."""
        self._load_csv()
        
        unprocessed_mask = self._df['subscribers'].isna()
        remaining_df = self._df[unprocessed_mask]
        
        total_channels = len(self._df)
        remaining_channels = len(remaining_df)
        processed_channels = total_channels - remaining_channels
        
        logging.info(f"Total channels: {total_channels}")
        logging.info(f"Already processed: {processed_channels}")
        logging.info(f"Remaining to process: {remaining_channels}")
        
        if remaining_channels == 0:
            logging.info("All channels have been processed. Nothing to do.")
            return
        
        # Calculate safe batch size based on remaining quota
        remaining_quota = self._validator._daily_quota - self._validator._quota_used
        estimated_cost_per_channel = 150  # Worst case: search + channel.list
        safe_batch_size = min(20, remaining_quota // estimated_cost_per_channel)
        
        if safe_batch_size == 0:
            logging.warning("Insufficient quota remaining for processing")
            return
            
        batch_size = safe_batch_size
        total_batches = (remaining_channels + batch_size - 1) // batch_size
        
        for batch_num, start_idx in enumerate(range(0, len(remaining_df), batch_size)):
            batch_df = remaining_df.iloc[start_idx:start_idx + batch_size]
            logging.info(f"Processing batch {batch_num + 1}/{total_batches}")
            
            try:
                await self._process_batch(batch_df)
                
                # Save progress after each batch
                self._df.to_csv(self._input_file, index=False)
                processed_count = min(start_idx + batch_size, remaining_channels)
                logging.info(f"Progress: {processed_count}/{remaining_channels} channels processed")
                
                # Add a small delay between batches to prevent overwhelming
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logging.error(f"Error processing batch {batch_num + 1}: {str(e)}")
                # Save progress even if batch fails
                self._df.to_csv(self._input_file, index=False)
                # Continue with next batch instead of failing completely
                continue
        
        logging.info("Processing completed")

    def process(self) -> None:
        """Synchronous wrapper for async processing."""
        asyncio.run(self.process_async())

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs using YouTube API')
    parser.add_argument('--input_file', required=True, help='Path to input CSV file')
    parser.add_argument('--url_column', required=True, help='Name of column containing YouTube URLs')
    parser.add_argument('--api_key', required=True, help='YouTube Data API key')
    parser.add_argument('--limit', type=int, help='Maximum number of URLs to process')
    
    args = parser.parse_args()
    
    try:
        validator = YoutubeCSVValidator(
            input_file=args.input_file,
            url_column=args.url_column,
            api_key=args.api_key,
            limit=args.limit
        )
        validator.process()
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 