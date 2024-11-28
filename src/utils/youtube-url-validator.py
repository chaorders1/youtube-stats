import requests
import concurrent.futures
import re
from typing import Dict, List, Tuple
import pandas as pd
from urllib.parse import urlparse
import sqlite3

class YouTubeURLValidator:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
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
                # 验证handle格式
                handle = path.split('/@')[1]
                if not re.match(r'^[A-Za-z0-9_-]{1,30}$', handle):
                    return False, "Invalid handle format"
            else:
                return False, "Invalid path format"
                
            return True, "Valid format"
        except Exception as e:
            return False, f"Format validation error: {str(e)}"

    def check_url_accessibility(self, url: str) -> Tuple[bool, str]:
        """验证URL是否可访问"""
        try:
            response = requests.head(url, headers=self.headers, timeout=5, allow_redirects=True)
            if response.status_code == 200:
                return True, "Accessible"
            elif response.status_code == 404:
                return False, "Channel not found"
            else:
                return False, f"HTTP {response.status_code}"
        except requests.RequestException as e:
            return False, f"Request error: {str(e)}"

    def batch_validate(self, urls: List[Dict]) -> List[Dict]:
        """批量验证URL"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {
                executor.submit(self.validate_single_url, url_data): url_data 
                for url_data in urls
            }
            
            for future in concurrent.futures.as_completed(future_to_url):
                result = future.result()
                results.append(result)
                
        return results

    def validate_single_url(self, url_data: Dict) -> Dict:
        """验证单个URL并返回结果"""
        url = url_data['youtube_channel_url']
        format_valid, format_msg = self.validate_url_format(url)
        
        result = {
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

    def generate_report(self, results: List[Dict]) -> pd.DataFrame:
        """生成验证报告"""
        df = pd.DataFrame(results)
        
        # 添加验证状态分类
        df['validation_status'] = df.apply(
            lambda row: 'Valid' if row['format_valid'] and row['accessible']
            else 'Format Invalid' if not row['format_valid']
            else 'Inaccessible',
            axis=1
        )
        
        return df

    def save_report(self, df: pd.DataFrame, output_path: str):
        """保存验证报告"""
        df.to_csv(output_path, index=False)

    def process_database(self, db_path: str, table_name: str, limit: int = None):
        """
        Process URLs from database and update verification status
        Args:
            db_path: Path to SQLite database
            table_name: Name of the table containing URLs
            limit: Optional limit on number of URLs to process
        """
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Modify query to include LIMIT if specified
            query = f"SELECT youtube_channel_url FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            urls = cursor.fetchall()
            
            print(f"Processing {len(urls)} URLs...")
            
            # Rest of the processing remains the same
            urls_to_validate = [
                {
                    'channel_handle': self._extract_handle(url[0]),
                    'youtube_channel_url': url[0],
                    'subscribers': None
                }
                for url in urls
            ]

            results = self.batch_validate(urls_to_validate)
            
            # Print some results for debugging
            print(f"Processed {len(results)} URLs. Updating database...")
            
            updated_count = 0
            for result in results:
                status = 'good' if result['format_valid'] and result['accessible'] else 'bad'
                cursor.execute(
                    f"""
                    UPDATE {table_name}
                    SET url_verified_status = ?
                    WHERE youtube_channel_url = ?
                    """,
                    (status, result['youtube_channel_url'])
                )
                updated_count += cursor.rowcount
            
            print(f"Updated {updated_count} rows in database")
            conn.commit()
            return True

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
    validator = YouTubeURLValidator()
    
    # Database configuration
    db_path = '/Users/yuanlu/Code/test/youtube-top-10000-channels/data/output-edit.db'
    table_name = 'unique_youtube_channel_urls'
    url_limit = 100  # Set how many URLs to process
    
    # Add the new column if it doesn't exist
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [info[1] for info in cursor.fetchall()]
        
        if 'url_verified_status' not in columns:
            print("Adding url_verified_status column...")
            cursor.execute(f"""
                ALTER TABLE {table_name} 
                ADD COLUMN url_verified_status TEXT
            """)
            conn.commit()
            print("Column added successfully")
        else:
            print("url_verified_status column already exists")
    
    # Process the database with limit
    print("Starting URL validation process...")
    success = validator.process_database(db_path, table_name, limit=url_limit)
    if success:
        print("URL validation completed successfully")
    else:
        print("URL validation encountered errors")

if __name__ == "__main__":
    main()