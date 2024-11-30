"""
This script validates YouTube channel URLs and extracts channel information.

Examples:

    # Validate a single YouTube channel URL
    python youtube-url-validator2.py --url "https://www.youtube.com/@bestpartners"

    # Process URLs from database
    python youtube-url-validator2.py --input-db input.db \
                                   --input-table channels \
                                   --url-column channel_url \
                                   --output-db output.db \
                                   --output-table youtube_channel_info \
                                   --batch-size 100

    # Process URLs from database
    python youtube-url-validator2.py --input-db /Users/yuanlu/Code/youtube-top-10000-channels/data/videoamigo-processed-test.db \
                                   --input-table test_unique_youtube_channel_urls \
                                   --url-column Youtube_Channel_URL \
                                   --output-db /Users/yuanlu/Code/youtube-top-10000-channels/data/output.db \
                                   --output-table youtube_channel_info \
                                   --batch-size 100


"""

import requests
import re
import time
import logging
from urllib.parse import urlparse
import argparse
import sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contextlib import contextmanager
from datetime import datetime
import sys
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_validator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ChannelInfo:
    """Data class to store channel information"""
    url: str
    is_valid: bool
    channel_id: Optional[str] = None
    handle: Optional[str] = None
    subscribers: Optional[str] = None
    error_message: Optional[str] = None

class YouTubeValidator:
    """Handles YouTube channel URL validation and information extraction"""
    
    def __init__(self, max_retries: int = 3):
        self.session = self._create_session(max_retries)
        self._compile_patterns()
        self._consecutive_429_count = 0
        self.MAX_CONSECUTIVE_429 = 3
        self._last_request_time = 0
        self.REQUEST_INTERVAL = 0.6  # 每分钟100个请求 = 每个请求0.6秒
        
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create a requests session with retry strategy"""
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]  # 移除429，我们单独处理它
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        return session

    def _compile_patterns(self):
        """Compile regex patterns for better performance"""
        self.channel_id_patterns = [
            re.compile(pattern) for pattern in [
                r'"channelId":"([^"]+)"',
                r'"externalChannelId":"([^"]+)"',
                r'"ucid":"([^"]+)"',
                r'channel/([^/"]+)',
            ]
        ]
        self.handle_patterns = [
            re.compile(pattern) for pattern in [
                r'"channelHandle":"(@[^"]+)"',
                r'"vanityChannelUrl":"http://www.youtube.com/(@[^"]+)"',
                r'youtube\.com/(@[^"\s/]+)',
            ]
        ]
        self.subscriber_patterns = [
            re.compile(pattern, re.IGNORECASE) for pattern in [
                r'"metadataParts":\[{"text":{"content":"([^"]+?\s*subscribers?)"}}\]',
                r'"text":{"content":"([^"]+?\s*subscribers?)"}',
                r'subscribers"[^>]*?>([^<]+?)\s*(?:subscriber|subscribers)',
            ]
        ]

    def _wait_for_next_request(self):
        """简单的速率限制：确保请求间隔至少0.6秒"""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.REQUEST_INTERVAL:
            time.sleep(self.REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def validate_url(self, url: str) -> ChannelInfo:
        """Validate a YouTube channel URL and extract information"""
        try:
            self._wait_for_next_request()
            
            # Clean URL
            parsed_url = urlparse(url)
            if not parsed_url.scheme:
                url = 'https://' + url
            
            if not any(domain in url.lower() for domain in ['youtube.com', 'youtu.be']):
                return ChannelInfo(url=url, is_valid=False, error_message="Not a YouTube URL")

            response = self.session.get(url, timeout=10)
            
            # 处理429错误
            if response.status_code == 429:
                self._consecutive_429_count += 1
                if self._consecutive_429_count >= self.MAX_CONSECUTIVE_429:
                    raise Exception("Too many consecutive 429 errors, stopping process")
                return ChannelInfo(
                    url=url,
                    is_valid=False,
                    error_message="Rate limit exceeded"
                )
            else:
                self._consecutive_429_count = 0  # 重置计数器
            
            if response.status_code != 200:
                return ChannelInfo(
                    url=url,
                    is_valid=False,
                    error_message=f"HTTP {response.status_code}"
                )

            # Extract information
            channel_info = self._extract_channel_info(response.text)
            
            if not channel_info.get('channel_id'):
                return ChannelInfo(
                    url=url,
                    is_valid=False,
                    error_message="Could not extract channel information"
                )

            return ChannelInfo(
                url=url,
                is_valid=True,
                channel_id=channel_info.get('channel_id'),
                handle=channel_info.get('handle'),
                subscribers=channel_info.get('subscribers')
            )

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            return ChannelInfo(url=url, is_valid=False, error_message=str(e))

    def _extract_channel_info(self, html_content: str) -> Dict[str, Optional[str]]:
        """Extract channel information from HTML content"""
        info = {}
        
        # Extract channel ID
        for pattern in self.channel_id_patterns:
            if match := pattern.search(html_content):
                info['channel_id'] = match.group(1)
                break

        # Extract handle
        for pattern in self.handle_patterns:
            if match := pattern.search(html_content):
                info['handle'] = match.group(1)
                break

        # Extract subscribers
        for pattern in self.subscriber_patterns:
            if match := pattern.search(html_content):
                subscriber_count = match.group(1).strip()
                info['subscribers'] = re.sub(r'\s*subscribers?\s*$', '', subscriber_count, flags=re.IGNORECASE)
                break

        return info

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, input_db: str, output_db: str):
        self.input_db = input_db
        self.output_db = output_db
        self._init_output_db()

    def _init_output_db(self):
        """Initialize output database schema"""
        with sqlite3.connect(self.output_db) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS youtube_channel_info (
                    url TEXT PRIMARY KEY,
                    is_valid INTEGER,
                    error_message TEXT,
                    channel_id TEXT,
                    handle TEXT,
                    subscribers TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

    def get_unprocessed_urls(self, table: str, url_column: str, 
                            batch_size: int) -> List[str]:
        """Get URLs that haven't been processed yet"""
        with sqlite3.connect(self.input_db) as conn:
            query = f"""
                SELECT DISTINCT a.{url_column} 
                FROM {table} a
                LEFT JOIN youtube_channel_info b ON a.{url_column} = b.url
                WHERE b.url IS NULL
                LIMIT {batch_size}
            """
            return [row[0] for row in conn.execute(query).fetchall()]

    def save_results(self, results: List[ChannelInfo]):
        """Save validation results to database"""
        with sqlite3.connect(self.output_db) as conn:
            conn.executemany('''
                INSERT OR REPLACE INTO youtube_channel_info 
                (url, is_valid, error_message, channel_id, handle, subscribers)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', [(r.url, r.is_valid, r.error_message, r.channel_id, 
                  r.handle, r.subscribers) for r in results])

def process_database(input_db: str, input_table: str, url_column: str, 
                    output_db: str, batch_size: int = 100):
    """
    Process URLs from input database and save results to output database.
    """
    validator = YouTubeValidator()
    db = DatabaseManager(input_db, output_db)
    
    while True:
        urls = db.get_unprocessed_urls(input_table, url_column, batch_size)
        if not urls:
            break
            
        results = []
        for url in tqdm(urls, desc="Processing URLs"):
            results.append(validator.validate_url(url))
        
        db.save_results(results)

def main():
    """
    Main function to process YouTube channel URLs from database or command line.
    """
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs')
    
    # Create a mutually exclusive group for URL and database inputs
    group = parser.add_mutually_exclusive_group(required=True)
    
    # Add URL argument to the group
    group.add_argument('--url', help='Single YouTube channel URL to validate')
    
    # Add database arguments to the group
    group.add_argument('--input-db', help='Input SQLite database path')
    
    # Other database-related arguments
    parser.add_argument('--input-table', help='Input table name')
    parser.add_argument('--url-column', help='Column name containing URLs')
    parser.add_argument('--output-db', help='Output SQLite database path')
    parser.add_argument('--output-table', help='Output table name', default='youtube_channel_info')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    args = parser.parse_args()

    # Check if we're in single URL mode
    if args.url:
        print(f"\nTesting URL: {args.url}")
        validator = YouTubeValidator()
        result = validator.validate_url(args.url)
        
        if result.is_valid:
            print("✓ Valid channel!")
            print(f"Handle: {result.handle or 'Not found'}")
            print(f"Channel ID: {result.channel_id or 'Not found'}")
            print(f"Subscribers: {result.subscribers or 'Not found'}")
        else:
            print(f"✗ Invalid: {result.error_message}")
    
    # Check if we have all required database arguments
    elif args.input_db:
        if not all([args.input_table, args.url_column, args.output_db]):
            parser.error("When using database mode, --input-table, --url-column, and --output-db are required")
        
        process_database(
            input_db=args.input_db,
            input_table=args.input_table,
            url_column=args.url_column,
            output_db=args.output_db,
            batch_size=args.batch_size
        )

if __name__ == "__main__":
    main()