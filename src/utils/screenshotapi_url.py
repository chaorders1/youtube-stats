#!/usr/bin/python3
import urllib.parse
import urllib.request
import os
import argparse
import logging
from typing import Dict, Optional, Tuple
from dotenv import load_dotenv
import ssl
from datetime import datetime
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

class ScreenshotAPIError(Exception):
    """Base exception for ScreenshotAPI"""
    pass

class URLError(ScreenshotAPIError):
    """Raised when URL is invalid or unsupported"""
    pass

class APIError(ScreenshotAPIError):
    """Raised when API request fails"""
    pass

class ScreenshotAPI:
    """Handles screenshot capture using ScreenshotAPI service."""
    
    BASE_URL = "https://shot.screenshotapi.net/screenshot"
    SUPPORTED_PLATFORMS = {'youtube', 'twitter', 'instagram'}
    SUPPORTED_PAGETYPES = {
        'youtube': ['home', 'videos', 'playlists', 'community', 'channels', 'about'],
        'twitter': ['home', 'media', 'likes'],
        'instagram': ['home', 'reels', 'posts']
    }
    
    DEFAULT_OPTIONS = {
        'output': 'image',
        'file_type': 'png',
        'width': '1920',
        'height': '3240',
        'full_page': 'false',
        'delay': '1000',
        'wait_for_event': 'load',  #networkidle0
        'lazy_load': 'false',
        'fresh': 'true',
        'block_ads': 'true',
        'no_cookie_banners': 'true',
        'retina': 'false'    
    }
    
    def __init__(self, api_token: str, output_dir: str = 'screenshots'):
        """
        Initialize ScreenshotAPI with token and output directory.
        
        Args:
            api_token: API authentication token
            output_dir: Directory for saving screenshots
        """
        if not api_token:
            raise ValueError("API token is required")
            
        self.api_token = api_token
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        try:
            os.makedirs(output_dir, exist_ok=True)
            # Test write permissions
            test_file = os.path.join(output_dir, '.test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except OSError as e:
            raise OSError(f"Failed to create or write to output directory {output_dir}: {e}")
            
        # Allow unverified SSL for the API
        ssl._create_default_https_context = ssl._create_unverified_context
        
    def validate_url(self, url: str) -> bool:
        """
        Validate if URL is properly formatted and from supported platform.
        
        Args:
            url: URL to validate
            
        Returns:
            bool: True if URL is valid, False otherwise
        """
        try:
            result = urllib.parse.urlparse(url)
            if not all([result.scheme, result.netloc]):
                return False
                
            domain = result.netloc.lower()
            return any(platform in domain for platform in ['youtube.com', 'youtu.be', 'twitter.com', 'x.com', 'instagram.com'])
        except Exception:
            return False
        
    def _sanitize_filename(self, text: str) -> str:
        """Remove invalid characters from filename."""
        return re.sub(r'[<>:"/\\|?*]', '_', text)
    
    def _extract_platform_info(self, url: str) -> Tuple[str, str]:
        """
        Extract platform and channel ID from URL.
        
        Args:
            url: Target URL
            
        Returns:
            Tuple[str, str]: (platform, channel_id)
        
        Raises:
            URLError: If platform is not supported
        """
        url_lower = url.lower()
        
        # YouTube
        if 'youtube.com' in url_lower or 'youtu.be' in url_lower:
            if '@' in url:
                channel_id = url.split('@')[-1].split('/')[0]
            else:
                channel_id = url.split('/')[-1]
            return 'youtube', channel_id
            
        # Twitter
        elif 'twitter.com' in url_lower or 'x.com' in url_lower:
            channel_id = url.split('/')[-1].split('?')[0]
            return 'twitter', channel_id
            
        # Instagram
        elif 'instagram.com' in url_lower:
            channel_id = url.split('/')[-1].split('?')[0]
            return 'instagram', channel_id
            
        else:
            raise URLError(f"Unsupported platform URL: {url}")
    
    def _detect_page_type(self, url: str, platform: str) -> str:
        """
        Detect page type from URL.
        
        Args:
            url: Target URL
            platform: Platform name
            
        Returns:
            str: Page type
        """
        url_lower = url.lower()
        
        if platform == 'youtube':
            if '/videos' in url_lower:
                return 'videos'
            elif '/playlists' in url_lower:
                return 'playlists'
            elif '/community' in url_lower:
                return 'community'
            elif '/channels' in url_lower:
                return 'channels'
            elif '/about' in url_lower:
                return 'about'
            else:
                return 'home'
                
        elif platform == 'twitter':
            if '/media' in url_lower:
                return 'media'
            elif '/likes' in url_lower:
                return 'likes'
            else:
                return 'home'
                
        elif platform == 'instagram':
            if '/reels' in url_lower:
                return 'reels'
            elif '/posts' in url_lower:
                return 'posts'
            else:
                return 'home'
                
        return 'home'
    
    def _generate_filename(self, url: str) -> str:
        """
        Generate filename using format: platform_channelid_pagetype_timestamp.png
        
        Args:
            url: Target URL
            
        Returns:
            str: Generated filename
        """
        # Extract platform and channel info
        platform, channel_id = self._extract_platform_info(url)
        
        # Detect page type
        page_type = self._detect_page_type(url, platform)
        
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Sanitize components
        channel_id = self._sanitize_filename(channel_id)
        
        # Combine components
        filename = f"{platform}_{channel_id}_{page_type}_{timestamp}.png"
        
        return filename
        
    def capture(self, url: str, custom_options: Optional[Dict] = None, retries: int = 3, retry_delay: int = 5) -> str:
        """
        Capture screenshot of URL and save with organized naming scheme.
        
        Args:
            url: Target URL to capture
            custom_options: Optional custom API parameters
            retries: Number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            str: Path to saved screenshot file
            
        Raises:
            URLError: If URL is invalid
            APIError: If screenshot capture fails
        """
        # Validate URL
        if not self.validate_url(url):
            raise URLError(f"Invalid URL format or unsupported platform: {url}")
            
        try:
            # Generate filename
            filename = self._generate_filename(url)
            output_path = os.path.join(self.output_dir, filename)
            
            # Prepare parameters
            params = self.DEFAULT_OPTIONS.copy()
            if custom_options:
                params.update(custom_options)
            params['token'] = self.api_token
            params['url'] = url
            
            # Construct query URL
            query = f"{self.BASE_URL}?{urllib.parse.urlencode(params)}"
            
            # Capture screenshot with retry
            for attempt in range(retries):
                try:
                    urllib.request.urlretrieve(query, output_path)
                    
                    # Verify file was created and has size > 0
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                        logger.info(f"Screenshot saved to: {output_path}")
                        return output_path
                    else:
                        raise APIError("Screenshot file is empty or not created")
                        
                except Exception as e:
                    if attempt < retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                        if retry_delay:
                            import time
                            time.sleep(retry_delay)
                    else:
                        raise APIError(f"Failed to capture screenshot after {retries} attempts: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Failed to capture screenshot for {url}: {str(e)}")
            raise

def main():
    """Main function to handle command line arguments."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description='Capture website screenshots using ScreenshotAPI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
    # Basic usage - capture YouTube channel home page
    python %(prog)s --url="https://www.youtube.com/@veritasium"
    python screenshotapi_url.py --url="https://www.youtube.com/@veritasium"
    
    # Specify custom output directory
    python %(prog)s --url="https://www.youtube.com/@veritasium" --output-dir="my_screenshots"
    
    # Capture specific YouTube channel pages
    python %(prog)s --url="https://www.youtube.com/@veritasium/videos"
    python %(prog)s --url="https://www.youtube.com/@veritasium/community"
    
    # Capture other platforms
    python %(prog)s --url="https://twitter.com/elonmusk"
    python %(prog)s --url="https://www.instagram.com/natgeo"
        '''
    )
    
    parser.add_argument('--url', required=True, help='URL to capture')
    parser.add_argument('--output-dir', default='screenshots', 
                       help='Output directory for screenshots (default: screenshots)')
    args = parser.parse_args()
    
    # Get API token from environment
    api_token = os.getenv('SCREENSHOT_API_TOKEN')
    if not api_token:
        logger.error("SCREENSHOT_API_TOKEN not found in environment variables")
        return 1
        
    try:
        # Initialize API and capture screenshot
        api = ScreenshotAPI(api_token, args.output_dir)
        api.capture(args.url)
        return 0
        
    except Exception as e:
        logger.error(str(e))
        return 1

if __name__ == "__main__":
    exit(main())