import requests
import re
import time
from urllib.parse import urlparse

def get_youtube_channel_handle(url: str) -> tuple[bool, str]:
    """
    Validate a YouTube channel URL and extract the channel handle.
    
    Args:
        url (str): The YouTube channel URL to validate
        
    Returns:
        tuple[bool, str]: A tuple containing:
            - bool: True if valid, False if invalid
            - str: Channel handle/ID if valid, error message if invalid
    """
    # Clean and validate the URL
    try:
        parsed_url = urlparse(url)
        if not parsed_url.scheme:
            url = 'https://' + url
        
        # Ensure it's a YouTube URL
        if not any(domain in url.lower() for domain in ['youtube.com', 'youtu.be']):
            return False, "Not a YouTube URL"
        
        # Define patterns for different YouTube channel URL formats
        patterns = {
            'handle': r'youtube\.com/@([a-zA-Z0-9_-]+)',
            'channel_id': r'youtube\.com/channel/([a-zA-Z0-9_-]+)'
        }
        
        # Try to match the URL against our patterns
        channel_identifier = None
        for pattern_type, pattern in patterns.items():
            match = re.search(pattern, url)
            if match:
                channel_identifier = match.group(1)
                break
        
        if not channel_identifier:
            return False, "Invalid YouTube channel URL format"
        
        # Browser-like headers to potentially reduce rate limiting
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        # Make a request to verify the channel exists
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
        
        if response.status_code == 200:
            return True, channel_identifier
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
    """
    # Test cases
    test_urls = [
        'https://www.youtube.com/@tseries',
        'https://www.youtube.com/channel/UCX6OQ3DkcsbYNE6H8uQQuVA',
        'https://www.youtube.com/@invalid_channel_123456789',
        'https://www.youtube.com/not_a_channel',
    ]
    
    for url in test_urls:
        print(f"\nTesting URL: {url}")
        is_valid, result = get_youtube_channel_handle(url)
        if is_valid:
            print(f"✓ Valid channel! Handle/ID: {result}")
        else:
            print(f"✗ Invalid: {result}")
        # Add a delay between requests to be more considerate of rate limits
        time.sleep(2)

if __name__ == "__main__":
    main()