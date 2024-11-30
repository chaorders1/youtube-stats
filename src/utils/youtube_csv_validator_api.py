"""
YouTube CSV Validator using YouTube Data API v3.
Validates YouTube channel URLs from a CSV file and extracts channel information.

Example:
    python youtube_csv_validator_api.py --input_file "channels.csv" --url_column "Youtube_Channel_URL" --api_key "YOUR_API_KEY"
    python youtube_csv_validator_api.py --input_file "/Users/yuanlu/Code/youtube-top-10000-channels/src/utils/test.csv" --url_column "Youtube_Channel_URL" --api_key "AIzaSyBux9x-GuKazCY6dBjUHf2EnA8GkLpl3k8"
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import argparse
import sys
import time
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_validator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class YouTubeValidator:
    """Validates YouTube channel URLs and extracts channel information using YouTube Data API."""
    
    def __init__(self, api_key: str):
        """
        Initialize YouTube API client.
        
        Args:
            api_key (str): YouTube Data API key
        """
        self._youtube = build('youtube', 'v3', developerKey=api_key)
        
    def _extract_channel_id(self, url: str) -> Optional[str]:
        """Extract channel ID from various YouTube URL formats."""
        patterns = {
            'channel': r'youtube\.com/channel/(UC[\w-]+)',
            'user': r'youtube\.com/user/([\w-]+)',
            'custom': r'youtube\.com/(@[\w-]+)',
            'c': r'youtube\.com/c/([\w-]+)',
        }
        
        for pattern in patterns.values():
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def _get_channel_info(self, identifier: str) -> Dict[str, Any]:
        """
        Get channel information using YouTube API.
        
        Args:
            identifier (str): Channel ID, username, or custom URL
        
        Returns:
            Dict containing channel information
        """
        try:
            if identifier.startswith('@'):
                # Handle custom URLs
                response = self._youtube.search().list(
                    q=identifier,
                    type='channel',
                    part='id'
                ).execute()
                
                if not response['items']:
                    return {'error': 'Channel not found'}
                    
                channel_id = response['items'][0]['id']['channelId']
            elif identifier.startswith('UC'):
                # Direct channel ID
                channel_id = identifier
            else:
                # Handle username
                response = self._youtube.channels().list(
                    forUsername=identifier,
                    part='id'
                ).execute()
                
                if not response['items']:
                    return {'error': 'Channel not found'}
                    
                channel_id = response['items'][0]['id']

            # Get channel details
            channel_response = self._youtube.channels().list(
                id=channel_id,
                part='snippet,statistics'
            ).execute()

            if not channel_response['items']:
                return {'error': 'Channel not found'}

            channel = channel_response['items'][0]
            return {
                'channel_id': channel['id'],
                'title': channel['snippet']['title'],
                'subscribers': int(channel['statistics'].get('subscriberCount', 0)),
                'handle': channel['snippet'].get('customUrl', ''),
                'is_valid': True,
                'error': ''
            }

        except HttpError as e:
            return {'error': f'API error: {str(e)}', 'is_valid': False}
        except Exception as e:
            return {'error': f'Validation error: {str(e)}', 'is_valid': False}

class YoutubeCSVValidator:
    """Validates YouTube channel URLs from CSV files using YouTube API."""

    def __init__(self, input_file: str, url_column: str, api_key: str, limit: Optional[int] = None):
        """
        Initialize the validator.

        Args:
            input_file (str): Path to input CSV file
            url_column (str): Name of column containing YouTube URLs
            api_key (str): YouTube Data API key
            limit (Optional[int]): Maximum number of URLs to process
        """
        self._input_file = Path(input_file)
        self._url_column = url_column
        self._limit = limit
        self._validator = YouTubeValidator(api_key)
        self._df: Optional[pd.DataFrame] = None

    def _load_csv(self) -> None:
        """Load and prepare the CSV file."""
        try:
            self._df = pd.read_csv(self._input_file)
            
            if self._url_column not in self._df.columns:
                raise ValueError(f"Column '{self._url_column}' not found in CSV file")
            
            # Initialize result columns
            new_columns = {
                'channel_id': '',
                'channel_title': '',
                'subscribers': pd.NA,
                'handle': '',
                'is_valid': pd.NA,
                'error': ''
            }
            
            for col, default_value in new_columns.items():
                if col not in self._df.columns:
                    self._df[col] = default_value
            
            if self._limit:
                self._df = self._df.iloc[:self._limit].copy()
                
            logging.info(f"Loaded CSV file with {len(self._df)} rows")
            
        except Exception as e:
            logging.error(f"Error loading CSV: {str(e)}")
            raise

    def process(self) -> None:
        """Process the CSV file and validate YouTube channels."""
        self._load_csv()
        
        for index, row in self._df.iterrows():
            url = row[self._url_column]
            
            try:
                channel_id = self._validator._extract_channel_id(url)
                if not channel_id:
                    self._df.loc[index, 'error'] = 'Invalid URL format'
                    self._df.loc[index, 'is_valid'] = False
                    continue

                result = self._validator._get_channel_info(channel_id)
                
                # Update DataFrame with results
                for key, value in result.items():
                    if key in self._df.columns:
                        self._df.loc[index, key] = value
                
                # Save progress periodically
                if index % 10 == 0:
                    self._df.to_csv(self._input_file, index=False)
                    logging.info(f"Processed {index + 1}/{len(self._df)} channels")
                
                # Respect YouTube API quotas
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"Error processing URL {url}: {str(e)}")
                self._df.loc[index, 'error'] = str(e)
                self._df.loc[index, 'is_valid'] = False
        
        # Save final results
        self._df.to_csv(self._input_file, index=False)
        logging.info("Processing completed")

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs using YouTube API')
    parser.add_argument('--input_file', required=True, help='Path to input CSV file')
    parser.add_argument('--url_column', required=True, help='Name of column containing YouTube URLs')
    parser.add_argument('--api_key', required=True, help='YouTube Data API key')
    parser.add_argument('--limit', type=int, help='Maximum number of URLs to process')
    
    args = parser.parse_args()
    
    try:
        validator = YoutubeCSVValidator(
            input_file=args.input_file,
            url_column=args.url_column,
            api_key=args.api_key,
            limit=args.limit
        )
        validator.process()
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 