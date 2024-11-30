'''

With a URL argument: 
python youtube-url-validator2.py https://www.youtube.com/@tseries
python youtube-url-validator2.py https://www.youtube.com/channel/UCaB8suou7DYdKwuaLxuvplQ

python youtube-url-validator2.py \
    --input-db /Users/yuanlu/Code/youtube-top-10000-channels/data/videoamigo-processed-test.db \
    --input-table test_unique_youtube_channel_urls \
    --url-column Youtube_Channel_URL \
    --output-db /Users/yuanlu/Code/youtube-top-10000-channels/data/videoamigo-processed-test-output.db \
    --batch-size 100

'''

import requests
import re
import time
from urllib.parse import urlparse
import argparse
import sqlite3
from pathlib import Path

def get_youtube_channel_handle(url: str) -> tuple[bool, dict]:
    """
    Validate a YouTube channel URL and extract channel information from HTML.
    
    Args:
        url (str): The YouTube channel URL to validate
        
    Returns:
        tuple[bool, dict]: A tuple containing:
            - bool: True if valid, False if invalid
            - dict: Channel info if valid (handle, channel_id, subscribers), error message if invalid
    """
    # Clean and validate the URL
    try:
        parsed_url = urlparse(url)
        if not parsed_url.scheme:
            url = 'https://' + url
        
        # Ensure it's a YouTube URL
        if not any(domain in url.lower() for domain in ['youtube.com', 'youtu.be']):
            return False, "Not a YouTube URL"
        
        # Browser-like headers to potentially reduce rate limiting
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        # Make a request to verify the channel exists
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
        
        if response.status_code == 200:
            channel_info = {}
            
            # Pattern for channel ID (expanded patterns)
            channel_id_patterns = [
                r'"channelId":"([^"]+)"',
                r'"externalChannelId":"([^"]+)"',
                r'"ucid":"([^"]+)"',
                r'channel/([^/"]+)',
            ]
            
            # Pattern for handle (expanded patterns)
            handle_patterns = [
                r'"channelHandle":"(@[^"]+)"',
                r'"vanityChannelUrl":"http://www.youtube.com/(@[^"]+)"',
                r'youtube\.com/(@[^"\s/]+)',
            ]
            
            # Updated patterns for subscriber count
            subscriber_patterns = [
                # New metadata pattern (matches "280M subscribers")
                r'"metadataParts":\[{"text":{"content":"([^"]+?\s*subscribers?)"}}\]',
                r'"text":{"content":"([^"]+?\s*subscribers?)"}',
                # Existing patterns as fallback
                r'subscribers"[^>]*?>([^<]+?)\s*(?:subscriber|subscribers)',
                r'yt-core-attributed-string[^>]*?>([^<]+?)\s*(?:subscriber|subscribers)',
                r'<span[^>]*?>(\d+(?:\.\d+)?[KMB]?) subscriber',
            ]
            
            # Try all channel ID patterns
            for pattern in channel_id_patterns:
                channel_id_match = re.search(pattern, response.text)
                if channel_id_match:
                    channel_info['channel_id'] = channel_id_match.group(1)
                    break
            
            # Try all handle patterns
            for pattern in handle_patterns:
                handle_match = re.search(pattern, response.text)
                if handle_match:
                    channel_info['handle'] = handle_match.group(1)
                    break
                    
            # Try all subscriber patterns
            for pattern in subscriber_patterns:
                subscriber_match = re.search(pattern, response.text, re.IGNORECASE)
                if subscriber_match:
                    # Clean up the subscriber count (remove extra whitespace and "subscribers" text)
                    subscriber_count = subscriber_match.group(1).strip()
                    subscriber_count = re.sub(r'\s*subscribers?\s*$', '', subscriber_count, flags=re.IGNORECASE)
                    channel_info['subscribers'] = subscriber_count
                    break
            
            # For debugging (uncomment if needed)
            # if 'subscribers' not in channel_info:
            #     print("Debug - HTML snippet:", response.text[:2000])
            
            if 'channel_id' in channel_info:
                return True, channel_info
            
            return False, "Could not extract channel information from page"
            
        elif response.status_code == 404:
            return False, "Channel not found"
        elif response.status_code == 429:
            return False, "Rate limited by YouTube (HTTP 429) - Please try again later"
        else:
            return False, f"Error accessing channel: HTTP {response.status_code}"
            
    except requests.exceptions.RequestException as e:
        return False, f"Network error: {str(e)}"
    except Exception as e:
        return False, f"Error: {str(e)}"

def process_database(input_db: str, input_table: str, url_column: str, 
                    output_db: str, batch_size: int = 100) -> None:
    """
    Process URLs from input database and save results to output database.
    
    Args:
        input_db (str): Path to input SQLite database
        input_table (str): Name of table containing URLs
        url_column (str): Name of column containing YouTube URLs
        output_db (str): Path to output SQLite database
        batch_size (int): Number of URLs to process in each batch
    """
    # Connect to input database
    in_conn = sqlite3.connect(input_db)
    in_cursor = in_conn.cursor()
    
    # Create output database and table
    out_conn = sqlite3.connect(output_db)
    out_cursor = out_conn.cursor()
    out_cursor.execute('''
        CREATE TABLE IF NOT EXISTS youtube_channel_info (
            url TEXT PRIMARY KEY,
            url_status_code INTEGER,
            url_status TEXT,
            youtube_channel_id TEXT,
            youtube_channel_handle TEXT,
            subscriber_count TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    try:
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
                is_valid, result = get_youtube_channel_handle(url)
                
                if is_valid:
                    out_cursor.execute('''
                        INSERT OR REPLACE INTO youtube_channel_info 
                        (url, url_status_code, url_status, youtube_channel_id, youtube_channel_handle, subscriber_count)
                        VALUES (?, 200, 'valid', ?, ?, ?)
                    ''', (url, result.get('channel_id'), result.get('handle'), result.get('subscribers')))
                else:
                    out_cursor.execute('''
                        INSERT OR REPLACE INTO youtube_channel_info 
                        (url, url_status_code, url_status, youtube_channel_id, youtube_channel_handle, subscriber_count)
                        VALUES (?, 400, ?, NULL, NULL, NULL)
                    ''', (url, str(result)))
                
                out_conn.commit()
                time.sleep(1)  # Rate limiting
                
            offset += batch_size
            print(f"Processed {min(offset, total_urls)}/{total_urls} URLs")
            
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
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    args = parser.parse_args()

    if args.input_db and args.input_table and args.url_column and args.output_db:
        # Process database mode
        process_database(
            input_db=args.input_db,
            input_table=args.input_table,
            url_column=args.url_column,
            output_db=args.output_db,
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