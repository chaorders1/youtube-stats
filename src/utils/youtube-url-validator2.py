"""
This script validates YouTube channel URLs and extracts channel information.

Example:
    # Validate a single YouTube channel URL
    python youtube-url-validator2.py --url "https://www.youtube.com/@bestpartners"
"""

import requests
import re
import logging
from urllib.parse import urlparse
import argparse
from typing import Optional
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

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

    def _extract_channel_info(self, html_content: str) -> dict[str, Optional[str]]:
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

def main():
    """Main function to validate a single YouTube channel URL."""
    parser = argparse.ArgumentParser(description='Validate YouTube channel URL')
    parser.add_argument('--url', required=True, help='YouTube channel URL to validate')
    args = parser.parse_args()

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

if __name__ == "__main__":
    main()