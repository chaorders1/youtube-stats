"""
YouTube URL Validator

A tool to validate YouTube channel URLs either individually or in bulk from a database.

Usage:
    Single URL validation:
        python youtube-url-validator.py --url "https://www.youtube.com/channel/UCzxsWdAqfZfak63rTC6m1XQ"
        python youtube-url-validator.py --url "https://www.youtube.com/@gh.s"
        python youtube-url-validator.py --url "https://www.youtube.com/@channelname"

    Bulk validation from database:
        python youtube-url-validator.py --db-path /path/to/database.db --table-name youtube_channels --url-column youtube_channel_url --batch-size 800
        python src/utils/youtube-url-validator.py --db-path /Users/yuanlu/Code/youtube-top-10000-channels/data/videoamigo-raw.db --table-name "unique_youtube_channel_urls" --url-column "YouTube_Channel_URL" --batch-size 800
data/output-edit.db
Options:
    --url TEXT          Single YouTube channel URL to validate
    --db-path TEXT      Path to SQLite database
    --table-name TEXT   Name of table containing URLs
    --url-column TEXT   Name of column containing URLs [default: youtube_channel_url]
    --batch-size INT    Number of URLs to process per batch [default: 800]
"""

import requests
import concurrent.futures
import re
from typing import Dict, List, Tuple
import pandas as pd
from urllib.parse import urlparse
import sqlite3
import time
import random
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm
import argparse

class YouTubeURLValidator:
    def __init__(self):
        # 减少重试次数和等待时间
        retry_strategy = Retry(
            total=1,  # 从2减到1次重试
            backoff_factor=0.1,  # 从0.5减到0.1
            status_forcelist=[429, 500, 502, 503, 504],
            # 添加快速失败的状态码
            raise_on_status=False,
            respect_retry_after_header=False  # 忽略服务器的 retry-after 头
        )
        
        # 减少并发session数量以降低内存占用
        self.sessions = [self._create_session(retry_strategy) for _ in range(10)]  # 从20减到10
        self.current_session = 0
        self.request_times = []
        # 增加每分钟请求限制
        self.max_requests_per_minute = 500  # 从300增加到500
        
    def _create_session(self, retry_strategy):
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry_strategy, pool_maxsize=100))
        session.headers.update({
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 100)}.0.{random.randint(4000, 5000)}.0 Safari/537.36'
        })
        return session
        
    def _get_session(self):
        """Get next session in round-robin fashion"""
        session = self.sessions[self.current_session]
        self.current_session = (self.current_session + 1) % len(self.sessions)
        return session
        
    def _rate_limit(self):
        """优化的速率限制"""
        current_time = time.time()
        minute_ago = current_time - 60
        
        # 只保留最近1分钟的请求记录
        self.request_times = [t for t in self.request_times if t > minute_ago]
        
        if len(self.request_times) >= self.max_requests_per_minute:
            sleep_time = max(0, self.request_times[0] - minute_ago)
            if sleep_time > 0:
                time.sleep(sleep_time * 0.5)  # 减少等待时间
            self.request_times = self.request_times[1:]
        
        self.request_times.append(current_time)

    def check_url_accessibility(self, url: str) -> Tuple[bool, str]:
        """优化的URL访问检查"""
        self._rate_limit()
        try:
            session = self._get_session()
            response = session.head(url, timeout=3, allow_redirects=True)
            # Consider both 200 and 204 as successful responses
            if response.status_code in [200, 204]:
                return True, "Accessible"
            elif response.status_code == 404:
                return False, "Channel not found"
            else:
                return False, f"HTTP {response.status_code}"
        except requests.RequestException as e:
            return False, f"Request error: {str(e)}"

    def batch_validate(self, urls: List[Dict], max_workers: int = 50) -> List[Dict]:
        """Concurrent URL validation with improved efficiency"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {
                executor.submit(self.validate_single_url, url_data): url_data 
                for url_data in urls
            }
            
            for future in concurrent.futures.as_completed(future_to_url):
                result = future.result()
                results.append(result)
                
        return results

    def process_database(self, db_path: str, table_name: str, batch_size: int = 200):
        """Process URLs from database with optimized batch processing"""
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Add the required columns if they don't exist
            try:
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                existing_columns = [row[1] for row in cursor.fetchall()]
                
                # Add new columns if they don't exist
                new_columns = {
                    'url_verified_status': 'TEXT',
                    'last_checked': 'TEXT',
                    'format_valid': 'BOOLEAN',
                    'format_message': 'TEXT',
                    'accessible': 'BOOLEAN',
                    'accessibility_message': 'TEXT'
                }
                
                for col_name, col_type in new_columns.items():
                    if col_name not in existing_columns:
                        cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN {col_name} {col_type}')
                conn.commit()
                
                # Get total count and unprocessed URLs
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM "{table_name}" 
                    WHERE url_verified_status IS NULL
                """)
                total_urls = cursor.fetchone()[0]
                
                if total_urls == 0:
                    print("No URLs to process!")
                    return True
                
                print(f"Processing {total_urls} URLs...")
                
                # Process in larger batches
                processed = 0
                with tqdm(total=total_urls, desc="Validating URLs") as pbar:
                    while processed < total_urls:
                        # Get next batch of unprocessed URLs
                        cursor.execute(f"""
                            SELECT rowid, youtube_channel_url 
                            FROM "{table_name}"
                            WHERE url_verified_status IS NULL
                            LIMIT ?
                        """, (batch_size,))
                        batch = cursor.fetchall()
                        
                        if not batch:
                            break
                        
                        # Process batch
                        urls_to_validate = [
                            {
                                'rowid': row[0],
                                'channel_handle': self._extract_handle(row[1]),
                                'youtube_channel_url': row[1],
                                'subscribers': None
                            }
                            for row in batch
                        ]

                        results = self.batch_validate(urls_to_validate, max_workers=50)
                        
                        # Bulk update using executemany
                        update_data = [
                            (
                                'good' if r['format_valid'] and r['accessible'] else 'bad',
                                datetime.now().isoformat(),
                                r['format_valid'],
                                r['format_message'],
                                r['accessible'],
                                r['accessibility_message'],
                                r['rowid']
                            )
                            for r in results
                        ]
                        
                        cursor.executemany(f"""
                            UPDATE "{table_name}"
                            SET 
                                url_verified_status = ?,
                                last_checked = ?,
                                format_valid = ?,
                                format_message = ?,
                                accessible = ?,
                                accessibility_message = ?
                            WHERE rowid = ?
                        """, update_data)
                        
                        conn.commit()
                        processed += len(batch)
                        pbar.update(len(batch))
                
                return True
                
            except sqlite3.Error as e:
                print(f"Database error: {e}")
                return False

    def validate_url_format(self, url: str) -> Tuple[bool, str]:
        """验证URL格式是否符合YouTube频道URL规范"""
        try:
            parsed = urlparse(url)
            if parsed.netloc != 'www.youtube.com':
                return False, "Invalid domain"
            
            path = parsed.path
            if path.startswith('/channel/'):
                # 验证channel ID格式 (24个字符)
                channel_id = path.split('/channel/')[1]
                if not re.match(r'^[A-Za-z0-9_-]{24}$', channel_id):
                    return False, "Invalid channel ID format"
            elif path.startswith('/@'):
                # 验证handle格式 - 更宽松的规则
                handle = path.split('/@')[1]
                # Allow almost any character except spaces and special URL characters
                if not re.match(r'^[^\s/?#<>\\]+$', handle):
                    return False, "Invalid handle format"
                if len(handle) > 30:  # YouTube handles have a maximum length
                    return False, "Handle too long"
            else:
                return False, "Invalid path format"
                
            return True, "Valid format"
        except Exception as e:
            return False, f"Format validation error: {str(e)}"

    def validate_single_url(self, url_data: Dict) -> Dict:
        """验证单个URL并返回结果"""
        url = url_data['youtube_channel_url']
        format_valid, format_msg = self.validate_url_format(url)
        
        result = {
            'rowid': url_data['rowid'],
            'channel_handle': url_data['channel_handle'],
            'youtube_channel_url': url,
            'subscribers': url_data['subscribers'],
            'format_valid': format_valid,
            'format_message': format_msg,
            'accessible': None,
            'accessibility_message': None
        }
        
        if format_valid:
            accessible, access_msg = self.check_url_accessibility(url)
            result['accessible'] = accessible
            result['accessibility_message'] = access_msg
            
        return result

    def _extract_handle(self, url: str) -> str:
        """Extract channel handle from URL"""
        try:
            if '/@' in url:
                return url.split('/@')[1]
            elif '/channel/' in url:
                return url.split('/channel/')[1]
            return ''
        except:
            return ''

def main():
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs')
    parser.add_argument('--url', help='Single YouTube channel URL to validate')
    parser.add_argument('--db-path', help='Path to SQLite database')
    parser.add_argument('--table-name', help='Name of table containing URLs')
    parser.add_argument('--url-column', default='youtube_channel_url', help='Name of column containing URLs')
    parser.add_argument('--batch-size', type=int, default=800, help='Number of URLs to process per batch')
    
    args = parser.parse_args()
    
    if args.url:
        validator = YouTubeURLValidator()
        result = validator.validate_single_url({
            'rowid': None,
            'channel_handle': validator._extract_handle(args.url),
            'youtube_channel_url': args.url,
            'subscribers': None
        })
        print(f"Format valid: {result['format_valid']} ({result['format_message']})")
        if result['accessible'] is not None:
            print(f"Accessible: {result['accessible']} ({result['accessibility_message']})")
    
    elif args.db_path and args.table_name:
        # Connect to database
        conn = sqlite3.connect(args.db_path)
        cursor = conn.cursor()
        
        # Check if required columns exist
        cursor.execute(f'PRAGMA table_info("{args.table_name}")')
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Add new columns if they don't exist
        new_columns = {
            'url_verified_status': 'TEXT',
            'last_checked': 'TEXT',
            'format_valid': 'BOOLEAN',
            'format_message': 'TEXT',
            'accessible': 'BOOLEAN',
            'accessibility_message': 'TEXT'
        }
        
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                print(f"Adding {col_name} column...")
                cursor.execute(f'ALTER TABLE "{args.table_name}" ADD COLUMN {col_name} {col_type}')
        conn.commit()
        
        # Initialize validator and process URLs
        validator = YouTubeURLValidator()
        validator.process_database(args.db_path, args.table_name, args.batch_size)
        
    else:
        parser.print_help()

if __name__ == '__main__':
    main()