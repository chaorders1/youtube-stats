'''

With a URL argument: 
python youtube-url-validator2.py https://www.youtube.com/@tseries
python youtube-url-validator2.py https://www.youtube.com/channel/UCaB8suou7DYdKwuaLxuvplQ

'''

import requests
import re
import time
from urllib.parse import urlparse
import argparse

def get_youtube_channel_handle(url: str) -> tuple[bool, dict]:
    """
    Validate a YouTube channel URL and extract channel information from HTML.
    
    Args:
        url (str): The YouTube channel URL to validate
        
    Returns:
        tuple[bool, dict]: A tuple containing:
            - bool: True if valid, False if invalid
            - dict: Channel info if valid (handle, channel_id), error message if invalid
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
            # Try multiple patterns to extract channel info
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
            
            # If we at least have a channel ID, consider it valid
            if 'channel_id' in channel_info:
                return True, channel_info
            
            # For debugging (optional)
            # print("Debug - HTML snippet:", response.text[:1000])
            
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

def main():
    """
    Main function to test the YouTube channel handle validator.
    Can accept a single URL as command line argument.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs')
    parser.add_argument('url', nargs='?', help='YouTube channel URL to validate')
    args = parser.parse_args()

    if args.url:
        # Process single URL from command line
        print(f"\nTesting URL: {args.url}")
        is_valid, result = get_youtube_channel_handle(args.url)
        if is_valid:
            print("✓ Valid channel!")
            print(f"Handle: {result.get('handle', 'Not found')}")
            print(f"Channel ID: {result.get('channel_id', 'Not found')}")
        else:
            print(f"✗ Invalid: {result}")
    else:
        # Fall back to test cases if no URL provided
        test_urls = [
            'https://www.youtube.com/@tseries',
            'https://www.youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA',
            'https://www.youtube.com/@invalid_channel_123456789',
            'https://www.youtube.com/not_a_channel',
            'https://www.youtube.com/channel/UCaB8suou7DYdKwuaLxuvplQ'
        ]
        
        for url in test_urls:
            print(f"\nTesting URL: {url}")
            is_valid, result = get_youtube_channel_handle(url)
            if is_valid:
                print("✓ Valid channel!")
                print(f"Handle: {result.get('handle', 'Not found')}")
                print(f"Channel ID: {result.get('channel_id', 'Not found')}")
            else:
                print(f"✗ Invalid: {result}")
            # Add a delay between requests to be more considerate of rate limits
            time.sleep(2)

if __name__ == "__main__":
    main()