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
    
    def __init__(self, max_retries: int = 3, timeout: int = 10):
        self.session = self._create_session(max_retries, timeout)
        self._compile_patterns()

    def _create_session(self, max_retries: int, timeout: int) -> requests.Session:
        """Create a requests session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
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

    def validate_url(self, url: str) -> ChannelInfo:
        """Validate a YouTube channel URL and extract information"""
        try:
            # Clean URL
            parsed_url = urlparse(url)
            if not parsed_url.scheme:
                url = 'https://' + url
            
            if not any(domain in url.lower() for domain in ['youtube.com', 'youtu.be']):
                return ChannelInfo(url=url, is_valid=False, error_message="Not a YouTube URL")

            response = self.session.get(url, timeout=10)
            
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
        with self._get_output_connection() as conn:
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
            conn.execute('CREATE INDEX IF NOT EXISTS idx_batch_id ON youtube_channel_info(batch_id)')

    @contextmanager
    def _get_input_connection(self):
        """Context manager for input database connection"""
        conn = sqlite3.connect(self.input_db)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _get_output_connection(self):
        """Context manager for output database connection"""
        conn = sqlite3.connect(self.output_db)
        try:
            yield conn
        finally:
            conn.close()

    def get_unprocessed_urls(self, table: str, url_column: str, batch_size: int, 
                            batch_id: str) -> List[str]:
        """Get URLs that haven't been processed yet"""
        with self._get_input_connection() as in_conn, self._get_output_connection() as out_conn:
            query = f"""
                SELECT a.{url_column} 
                FROM {table} a
                LEFT JOIN youtube_channel_info b ON a.{url_column} = b.url
                WHERE b.url IS NULL
                LIMIT {batch_size}
            """
            return [row[0] for row in in_conn.execute(query).fetchall()]

    def save_results(self, results: List[ChannelInfo], batch_id: str):
        """Save validation results to database"""
        with self._get_output_connection() as conn:
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

def process_urls_parallel(validator: YouTubeValidator, urls: List[str], 
                         max_workers: int = 10) -> List[ChannelInfo]:
    """Process URLs in parallel using ThreadPoolExecutor"""
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(validator.validate_url, url): url 
                        for url in urls}
        
        for future in tqdm(as_completed(future_to_url), total=len(urls)):
            results.append(future.result())
    return results

def process_database(input_db: str, input_table: str, url_column: str, 
                    output_db: str, output_table: str, batch_size: int = 100) -> None:
    """
    Process URLs from input database and save results to output database.
    
    Args:
        input_db (str): Path to input SQLite database
        input_table (str): Name of table containing URLs
        url_column (str): Name of column containing YouTube URLs
        output_db (str): Path to output SQLite database
        output_table (str): Name of table to store results
        batch_size (int): Number of URLs to process in each batch
    """
    # Connect to input database
    in_conn = sqlite3.connect(input_db)
    in_cursor = in_conn.cursor()
    
    # Connect to output database
    out_conn = sqlite3.connect(output_db)
    out_cursor = out_conn.cursor()
    
    validator = YouTubeValidator()  # Create an instance of YouTubeValidator
    
    try:
        # Ensure the table is created
        out_cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {output_table} (
                url TEXT PRIMARY KEY,
                url_status_code INTEGER,
                url_status TEXT,
                youtube_channel_id TEXT,
                youtube_channel_handle TEXT,
                subscriber_count TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        out_conn.commit()  # Ensure the table creation is committed
        print(f"Table '{output_table}' ensured in the output database.")
        
        # Get total count of URLs
        in_cursor.execute(f"SELECT COUNT(*) FROM {input_table}")
        total_urls = in_cursor.fetchone()[0]
        print(f"Total URLs to process: {total_urls}")
        
        # Process URLs in batches
        offset = 0
        while True:
            in_cursor.execute(f"SELECT {url_column} FROM {input_table} LIMIT {batch_size} OFFSET {offset}")
            urls = in_cursor.fetchall()
            if not urls:
                break
                
            for url_row in urls:
                url = url_row[0]
                if not url:
                    continue
                    
                print(f"\nProcessing URL: {url}")
                channel_info = validator.validate_url(url)  # Use validate_url method
                
                if channel_info.is_valid:
                    out_cursor.execute(f'''
                        INSERT OR REPLACE INTO {output_table} 
                        (url, url_status_code, url_status, youtube_channel_id, youtube_channel_handle, subscriber_count)
                        VALUES (?, 200, 'valid', ?, ?, ?)
                    ''', (url, channel_info.channel_id, channel_info.handle, channel_info.subscribers))
                else:
                    out_cursor.execute(f'''
                        INSERT OR REPLACE INTO {output_table} 
                        (url, url_status_code, url_status, youtube_channel_id, youtube_channel_handle, subscriber_count)
                        VALUES (?, 400, ?, NULL, NULL, NULL)
                    ''', (url, channel_info.error_message))
                
                out_conn.commit()
                time.sleep(1)  # Rate limiting
                
            offset += batch_size
            print(f"Processed {min(offset, total_urls)}/{total_urls} URLs")
            
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    finally:
        in_conn.close()
        out_conn.close()

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
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    args = parser.parse_args()

    if args.input_db and args.input_table and args.url_column and args.output_db and args.output_table:
        # Process database mode
        process_database(
            input_db=args.input_db,
            input_table=args.input_table,
            url_column=args.url_column,
            output_db=args.output_db,
            output_table=args.output_table,
            batch_size=args.batch_size
        )
    elif args.url:
        # Single URL mode
        print(f"\nTesting URL: {args.url}")
        is_valid, result = get_youtube_channel_handle(args.url)
        if is_valid:
            print("✓ Valid channel!")
            print(f"Handle: {result.get('handle', 'Not found')}")
            print(f"Channel ID: {result.get('channel_id', 'Not found')}")
            print(f"Subscribers: {result.get('subscribers', 'Not found')}")
        else:
            print(f"✗ Invalid: {result}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()