"""
This script validates YouTube channel URLs and extracts channel information.

Examples:

    # Validate a single YouTube channel URL
    python youtube-url-validator2.py --url "https://www.youtube.com/@channelname"

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
                                   --batch-size 800


"""

import aiohttp
import asyncio
import re
import time
import logging
from urllib.parse import urlparse
import argparse
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any
from dataclasses import dataclass
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

class YouTubeValidatorError(Exception):
    """Base exception class for YouTubeValidator errors"""
    pass

class YouTubeValidator:
    """Handles YouTube channel URL validation and information extraction using aiohttp"""
    
    def __init__(self, 
                 max_retries: int = 3, 
                 timeout: int = 10, 
                 min_request_interval: float = 0.5,
                 max_concurrent_requests: int = 30):
        """
        Initialize YouTubeValidator with configurable parameters.
        
        Args:
            max_retries: Maximum number of retry attempts for failed requests
            timeout: Request timeout in seconds
            min_request_interval: Minimum time between requests in seconds
            max_concurrent_requests: Maximum number of concurrent requests
        """
        self.max_retries = max_retries
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.max_concurrent_requests = max_concurrent_requests
        self._compile_patterns()
        self._last_request_time = 0.0
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def _get_semaphore(self) -> asyncio.Semaphore:
        """Get or create the rate limiting semaphore"""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        return self._semaphore

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        """
        Fetch the content of a URL asynchronously with rate limiting and retries.
        
        Args:
            session: aiohttp client session
            url: URL to fetch
            
        Returns:
            str: HTML content of the page
            
        Raises:
            YouTubeValidatorError: If all retry attempts fail
        """
        semaphore = await self._get_semaphore()
        async with semaphore:
            current_time = time.time()
            time_since_last_request = current_time - self._last_request_time
            if time_since_last_request < self.min_request_interval:
                await asyncio.sleep(self.min_request_interval - time_since_last_request)
            
            for attempt in range(self.max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=self.timeout)
                    async with session.get(url, timeout=timeout) as response:
                        self._last_request_time = time.time()
                        
                        if response.status == 429:  # Rate limit hit
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                            continue
                            
                        if response.status == 200:
                            return await response.text()
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            
                except asyncio.TimeoutError:
                    logger.error(f"Timeout on attempt {attempt + 1} for {url}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
            
            raise YouTubeValidatorError(f"Failed to fetch {url} after {self.max_retries} attempts")

    async def validate_url(self, session: aiohttp.ClientSession, url: str) -> ChannelInfo:
        """
        Validate a YouTube channel URL and extract information asynchronously.
        
        Args:
            session: aiohttp client session
            url: YouTube channel URL to validate
            
        Returns:
            ChannelInfo: Object containing validation results and channel information
        """
        try:
            if not url or not isinstance(url, str):
                return ChannelInfo(url=str(url), is_valid=False, error_message="Invalid URL format")

            parsed_url = urlparse(url)
            if not parsed_url.scheme:
                url = 'https://' + url
            
            if not any(domain in url.lower() for domain in ['youtube.com', 'youtu.be']):
                return ChannelInfo(url=url, is_valid=False, error_message="Not a YouTube URL")

            html_content = await self._fetch(session, url)
            channel_info = self._extract_channel_info(html_content)
            
            if not channel_info.get('channel_id'):
                return ChannelInfo(url=url, is_valid=False, error_message="Could not extract channel information")

            return ChannelInfo(
                url=url,
                is_valid=True,
                channel_id=channel_info.get('channel_id'),
                handle=channel_info.get('handle'),
                subscribers=channel_info.get('subscribers')
            )

        except YouTubeValidatorError as e:
            logger.error(f"Validation error for URL {url}: {str(e)}")
            return ChannelInfo(url=url, is_valid=False, error_message=str(e))
        except Exception as e:
            logger.error(f"Unexpected error validating URL {url}: {str(e)}")
            return ChannelInfo(url=url, is_valid=False, error_message=f"Unexpected error: {str(e)}")

    def _compile_patterns(self) -> None:
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

    def _extract_channel_info(self, html_content: str) -> Dict[str, Optional[str]]:
        """
        Extract channel information from HTML content.
        
        Args:
            html_content: HTML content of the YouTube channel page
            
        Returns:
            Dict containing extracted channel information
        """
        info: Dict[str, Optional[str]] = {
            'channel_id': None,
            'handle': None,
            'subscribers': None
        }
        
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

class DatabaseError(Exception):
    """Base exception class for database errors"""
    pass

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self, input_db: str, output_db: str):
        """
        Initialize DatabaseManager.
        
        Args:
            input_db: Path to input SQLite database
            output_db: Path to output SQLite database
        """
        self.input_db = input_db
        self.output_db = output_db
        self._ensure_output_db_exists()
        self._init_output_db()
        self._verify_tables()

    @contextmanager
    def _get_connection(self, db_path: str):
        """
        Context manager for database connections with proper cleanup.
        
        Args:
            db_path: Path to SQLite database
            
        Yields:
            sqlite3.Connection: Database connection
        """
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            yield conn
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            raise DatabaseError(f"Database error: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.error(f"Error closing database connection: {e}")

    def _ensure_output_db_exists(self) -> None:
        """Ensure the output database file exists"""
        output_path = Path(self.output_db)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if not output_path.exists():
            logger.info(f"Creating new database file: {self.output_db}")
            with self._get_connection(self.output_db) as conn:
                pass

    def _init_output_db(self) -> None:
        """Initialize output database schema"""
        try:
            with self._get_connection(self.output_db) as conn:
                logger.info("Creating youtube_channel_info table if not exists")
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
                conn.commit()
                logger.info("Table creation completed")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise DatabaseError(f"Database initialization error: {e}")

    def _verify_tables(self) -> None:
        """Verify that required tables exist"""
        try:
            with self._get_connection(self.output_db) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='youtube_channel_info'
                """)
                if not cursor.fetchone():
                    raise DatabaseError("youtube_channel_info table was not created properly")
                logger.info("Table verification successful")
        except Exception as e:
            logger.error(f"Database verification failed: {e}")
            raise DatabaseError(f"Database verification failed: {e}")

    def get_unprocessed_urls(self, table: str, url_column: str, batch_size: int, offset: int) -> List[str]:
        """
        Get URLs that haven't been processed yet.
        
        Args:
            table: Input table name
            url_column: Column name containing URLs
            batch_size: Number of URLs to retrieve
            offset: Starting offset
            
        Returns:
            List of unprocessed URLs
        """
        try:
            with self._get_connection(self.input_db) as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table,))
                if not cursor.fetchone():
                    raise DatabaseError(f"Input table '{table}' does not exist")

                query = f"""
                    SELECT DISTINCT {url_column} 
                    FROM {table}
                    LIMIT {batch_size} OFFSET {offset}
                """
                return [row[0] for row in cursor.execute(query).fetchall()]
        except Exception as e:
            logger.error(f"Error getting unprocessed URLs: {e}")
            raise DatabaseError(f"Error getting unprocessed URLs: {e}")

    def save_results(self, results: List[ChannelInfo], batch_id: str) -> None:
        """
        Save validation results to database.
        
        Args:
            results: List of ChannelInfo objects
            batch_id: Identifier for the current batch
        """
        try:
            with self._get_connection(self.output_db) as conn:
                conn.executemany('''
                    INSERT OR REPLACE INTO youtube_channel_info 
                    (url, url_status_code, url_status, youtube_channel_id, 
                    youtube_channel_handle, subscriber_count, batch_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', [
                    (
                        result.url,
                        200 if result.is_valid else 400,
                        'valid' if result.is_valid else result.error_message,
                        result.channel_id,
                        result.handle,
                        result.subscribers,
                        batch_id
                    )
                    for result in results
                ])
                conn.commit()
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            raise DatabaseError(f"Error saving results: {e}")

    def get_total_urls(self, table: str) -> int:
        """Get total number of URLs in the input table"""
        with self._get_connection(self.input_db) as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            return cursor.fetchone()[0]

    def get_last_processed_offset(self, table: str) -> int:
        """Get the last processed offset from the output table"""
        with self._get_connection(self.output_db) as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            return cursor.fetchone()[0]

async def process_urls_async(validator: YouTubeValidator, urls: List[str], 
                           max_concurrent_requests: int = 30) -> List[ChannelInfo]:
    """Process URLs asynchronously using aiohttp with concurrency control"""
    results = []
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    
    async def process_with_semaphore(url: str) -> ChannelInfo:
        async with semaphore:
            return await validator.validate_url(session, url)
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
    ) as session:
        tasks = [process_with_semaphore(url) for url in urls]
        for future in tqdm(asyncio.as_completed(tasks), total=len(urls)):
            try:
                result = await future
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing URL: {e}")
                continue
    
    return results

async def process_database_async(input_db: str, input_table: str, url_column: str, 
                                 output_db: str, output_table: str, batch_size: int = 800) -> None:
    """Process URLs from input database with improved error handling"""
    validator = YouTubeValidator()
    db_manager = DatabaseManager(input_db, output_db)
    
    try:
        total_urls = db_manager.get_total_urls(input_table)
        logger.info(f"Total URLs to process: {total_urls}")
        
        offset = db_manager.get_last_processed_offset(output_table)
        logger.info(f"Resuming from offset: {offset}")
        
        with tqdm(total=total_urls, initial=offset) as pbar:
            while True:
                try:
                    urls = db_manager.get_unprocessed_urls(input_table, url_column, batch_size, offset)
                    if not urls:
                        break
                    
                    results = await process_urls_async(validator, urls, max_concurrent_requests=30)
                    db_manager.save_results(results, batch_id=str(offset))
                    
                    processed = len(results)
                    offset += processed
                    pbar.update(processed)
                    
                    # Add small delay between batches
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing batch at offset {offset}: {e}")
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

def main():
    """
    Main function to process YouTube channel URLs from database or command line.
    """
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs')
    
    # Add URL as a positional argument with nargs='?' to make it optional
    parser.add_argument('url', nargs='?', help='Single YouTube channel URL to validate')
    
    # Optional arguments for database processing
    parser.add_argument('--input-db', help='Input SQLite database path')
    parser.add_argument('--input-table', help='Input table name')
    parser.add_argument('--url-column', help='Column name containing URLs')
    parser.add_argument('--output-db', help='Output SQLite database path')
    parser.add_argument('--output-table', help='Output table name', default='youtube_channel_info')
    parser.add_argument('--batch-size', type=int, default=800, help='Batch size for processing')
    
    args = parser.parse_args()

    if args.input_db and args.input_table and args.url_column and args.output_db and args.output_table:
        # Process database mode
        asyncio.run(process_database_async(
            input_db=args.input_db,
            input_table=args.input_table,
            url_column=args.url_column,
            output_db=args.output_db,
            output_table=args.output_table,
            batch_size=args.batch_size
        ))
    elif args.url:
        # Single URL mode
        print(f"\nTesting URL: {args.url}")
        asyncio.run(process_single_url(args.url))
    else:
        parser.print_help()

async def process_single_url(url: str):
    """Process a single URL asynchronously"""
    validator = YouTubeValidator()
    async with aiohttp.ClientSession() as session:
        channel_info = await validator.validate_url(session, url)
        if channel_info.is_valid:
            print("✓ Valid channel!")
            print(f"Handle: {channel_info.handle or 'Not found'}")
            print(f"Channel ID: {channel_info.channel_id or 'Not found'}")
            print(f"Subscribers: {channel_info.subscribers or 'Not found'}")
        else:
            print(f"✗ Invalid: {channel_info.error_message}")

if __name__ == "__main__":
    main()