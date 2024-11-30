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
                                   --input-table unique_youtube_channel_urls \
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
from typing import Optional, Dict, Tuple, List, Any
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from contextlib import contextmanager
from datetime import datetime
import sys
from tqdm import tqdm
from threading import Lock

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

class YouTubeValidatorError(Exception):
    """Base exception class for YouTube validator errors"""
    pass

class RateLimitExceededError(YouTubeValidatorError):
    """Raised when rate limits are exceeded"""
    pass

class ConnectionConfig:
    """Configuration for HTTP connections"""
    DEFAULT_TIMEOUT = 10
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 1
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    STATUS_FORCELIST = [500, 502, 503, 504]

@dataclass
class ChannelInfo:
    """Data class to store channel information"""
    url: str
    url_status_code: Optional[int] = None
    url_status: Optional[str] = None
    youtube_channel_id: Optional[str] = None
    youtube_channel_handle: Optional[str] = None
    subscriber_count: Optional[str] = None
    processed_at: Optional[datetime] = None
    batch_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage"""
        return {
            'url': self.url,
            'url_status_code': self.url_status_code,
            'url_status': self.url_status,
            'youtube_channel_id': self.youtube_channel_id,
            'youtube_channel_handle': self.youtube_channel_handle,
            'subscriber_count': self.subscriber_count,
            'processed_at': self.processed_at or datetime.now(),
            'batch_id': self.batch_id
        }

class RateLimiter:
    """Token bucket rate limiter implementation"""
    def __init__(self, tokens_per_second: float, bucket_size: int):
        self._tokens = bucket_size
        self._bucket_size = bucket_size
        self._tokens_per_second = tokens_per_second
        self._last_update = time.time()
        self._lock = Lock()
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a token, blocking if none are available
        
        Args:
            timeout: Maximum time to wait for a token
            
        Returns:
            bool: True if token acquired, False if timeout
        """
        start_time = time.time()
        with self._lock:
            while self._tokens <= 0:
                self._add_tokens()
                if self._tokens <= 0:
                    if timeout and (time.time() - start_time) > timeout:
                        return False
                    time.sleep(0.1)
            self._tokens -= 1
            return True

    def _add_tokens(self) -> None:
        now = time.time()
        elapsed = now - self._last_update
        new_tokens = elapsed * self._tokens_per_second
        self._tokens = min(self._tokens + new_tokens, self._bucket_size)
        self._last_update = now

class YouTubeValidator:
    """Handles YouTube channel URL validation and information extraction"""
    
    def __init__(self, 
                 max_retries: int = ConnectionConfig.MAX_RETRIES,
                 request_timeout: int = ConnectionConfig.DEFAULT_TIMEOUT):
        self._session = self._create_session(max_retries)
        self._compile_patterns()
        self._consecutive_429_count = 0
        self.MAX_CONSECUTIVE_429 = 3
        self._request_timeout = request_timeout
        # Adjusted rate limit to handle larger volumes
        self._rate_limiter = RateLimiter(tokens_per_second=2.0, bucket_size=20)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    @staticmethod
    def _create_session(max_retries: int) -> requests.Session:
        """Create a requests session with retry strategy"""
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=ConnectionConfig.BACKOFF_FACTOR,
            status_forcelist=ConnectionConfig.STATUS_FORCELIST
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=100,  # Increased pool size
            pool_maxsize=100
        )
        session.mount("https://", adapter)
        session.headers.update(ConnectionConfig.DEFAULT_HEADERS)
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

    def validate_url(self, url: str) -> ChannelInfo:
        """Validate a YouTube channel URL and extract information"""
        # Acquire token before making request
        self._rate_limiter.acquire()
        try:
            # Clean URL
            parsed_url = urlparse(url)
            if not parsed_url.scheme:
                url = 'https://' + url
            
            if not any(domain in url.lower() for domain in ['youtube.com', 'youtu.be']):
                return ChannelInfo(
                    url=url,
                    url_status_code=400,
                    url_status="Not a YouTube URL"
                )

            response = self._session.get(url, timeout=self._request_timeout)
            
            # Handle 429 errors
            if response.status_code == 429:
                self._consecutive_429_count += 1
                if self._consecutive_429_count >= self.MAX_CONSECUTIVE_429:
                    raise Exception("Too many consecutive 429 errors, stopping process")
                return ChannelInfo(
                    url=url,
                    url_status_code=429,
                    url_status="Rate limit exceeded"
                )
            else:
                self._consecutive_429_count = 0  # Reset counter
            
            if response.status_code != 200:
                return ChannelInfo(
                    url=url,
                    url_status_code=response.status_code,
                    url_status=f"HTTP {response.status_code}"
                )

            # Extract information
            channel_info = self._extract_channel_info(response.text)
            
            if not channel_info.get('channel_id'):
                return ChannelInfo(
                    url=url,
                    url_status_code=200,
                    url_status="Could not extract channel information"
                )

            return ChannelInfo(
                url=url,
                url_status_code=200,
                url_status="Success",
                youtube_channel_id=channel_info.get('channel_id'),
                youtube_channel_handle=channel_info.get('handle'),
                subscriber_count=channel_info.get('subscribers')
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
        self.input_db = Path(input_db)
        self.output_db = Path(output_db)
        self._connection_lock = Lock()
        self._init_databases()

    def _init_databases(self) -> None:
        """Initialize database schemas"""
        # Initialize output database
        with self._get_connection(self.output_db) as conn:
            # Create main table first with all columns
            conn.execute('''
                CREATE TABLE IF NOT EXISTS youtube_channel_info (
                    url TEXT PRIMARY KEY,
                    url_status_code INTEGER,
                    url_status TEXT,
                    youtube_channel_id TEXT,
                    youtube_channel_handle TEXT,
                    subscriber_count TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    batch_id TEXT
                )
            ''')
            
            # Create indexes after table and columns exist
            conn.execute('CREATE INDEX IF NOT EXISTS idx_url ON youtube_channel_info(url)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_processed_at ON youtube_channel_info(processed_at)')
        
        # Initialize input database
        with self._get_connection(self.input_db) as conn:
            # Create checkpoint table in input database
            conn.execute('''
                CREATE TABLE IF NOT EXISTS processing_checkpoint (
                    batch_id INTEGER PRIMARY KEY,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    total_urls INTEGER,
                    processed_urls INTEGER,
                    last_processed_url TEXT,
                    status TEXT
                )
            ''')

    def save_results(self, results: List[ChannelInfo]):
        """Save validation results to database"""
        with self._get_connection(self.output_db) as conn:
            # Convert results to tuples for database insertion
            values = [(
                r.url,
                r.url_status_code,
                r.url_status,
                r.youtube_channel_id,
                r.youtube_channel_handle,
                r.subscriber_count,
                r.processed_at or datetime.now(),
                r.batch_id
            ) for r in results]
            
            # Insert or replace results
            conn.executemany('''
                INSERT OR REPLACE INTO youtube_channel_info (
                    url,
                    url_status_code,
                    url_status,
                    youtube_channel_id,
                    youtube_channel_handle,
                    subscriber_count,
                    processed_at,
                    batch_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', values)

    @contextmanager
    def _get_connection(self, db_path: Path):
        """Get a database connection with proper timeout and isolation level"""
        conn = sqlite3.connect(
            str(db_path),
            timeout=60.0,
            isolation_level='IMMEDIATE'
        )
        try:
            yield conn
            conn.commit()  # Commit any pending changes
        finally:
            conn.close()

    def get_unprocessed_urls(self, table: str, url_column: str, batch_size: int) -> Tuple[int, List[str]]:
        """Get URLs that haven't been processed yet with batch tracking"""
        with self._get_connection(self.input_db) as conn:
            # Get the last successful checkpoint
            last_url = conn.execute('''
                SELECT last_processed_url 
                FROM processing_checkpoint 
                WHERE status = 'completed' 
                ORDER BY batch_id DESC LIMIT 1
            ''').fetchone()

            # Build query based on last processed URL
            if last_url:
                query = f"""
                    SELECT DISTINCT {url_column} 
                    FROM {table}
                    WHERE {url_column} > ?
                    ORDER BY {url_column}
                    LIMIT {batch_size}
                """
                urls = [row[0] for row in conn.execute(query, (last_url[0],)).fetchall()]
            else:
                query = f"""
                    SELECT DISTINCT {url_column} 
                    FROM {table}
                    ORDER BY {url_column}
                    LIMIT {batch_size}
                """
                urls = [row[0] for row in conn.execute(query).fetchall()]

            # Create new checkpoint
            if urls:
                with self._get_connection(self.output_db) as conn:
                    batch_id = conn.execute('''
                        INSERT INTO processing_checkpoint 
                        (start_time, total_urls, processed_urls, status) 
                        VALUES (CURRENT_TIMESTAMP, ?, 0, 'in_progress')
                        RETURNING batch_id
                    ''', (len(urls),)).fetchone()[0]
                return batch_id, urls
            return None, []

    def update_checkpoint(self, batch_id: int, processed_url: str):
        """Update checkpoint progress"""
        with self._get_connection(self.output_db) as conn:
            conn.execute('''
                UPDATE processing_checkpoint 
                SET processed_urls = processed_urls + 1,
                    last_processed_url = ?
                WHERE batch_id = ?
            ''', (processed_url, batch_id))

    def complete_checkpoint(self, batch_id: int):
        """Mark checkpoint as completed"""
        with self._get_connection(self.output_db) as conn:
            conn.execute('''
                UPDATE processing_checkpoint 
                SET status = 'completed',
                    end_time = CURRENT_TIMESTAMP
                WHERE batch_id = ?
            ''', (batch_id,))

def process_database(
    input_db: str,
    input_table: str,
    url_column: str,
    output_db: str,
    batch_size: int = 100,
    num_threads: int = 4
) -> None:
    """Process URLs from input database with improved concurrency"""
    with YouTubeValidator() as validator:
        db = DatabaseManager(input_db, output_db)
        
        while True:
            batch_id, urls = db.get_unprocessed_urls(input_table, url_column, batch_size)
            if not urls:
                break
                
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                for url in urls:
                    futures.append(
                        executor.submit(validator.validate_url, url)
                    )
                
                results = []
                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="Processing URLs"
                ):
                    try:
                        result = future.result()
                        results.append(result)
                        db.update_checkpoint(batch_id, result.url)
                    except Exception as e:
                        logger.error(f"Error processing URL: {e}")
            
            db.save_results(results)
            db.complete_checkpoint(batch_id)

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