'''
YouTube Page Parser

Extracts video metadata from a saved YouTube page using BeautifulSoup4.

Information extracted:
- Video ID and URL
- Title
- Duration
- View count
- Upload date (relative and absolute)
- Thumbnail URL
- Description snippet

Usage:
    python youtube_parser_video.py <html_file> [output_csv_file]

Example:
    python youtube_parser_video.py /Users/yuanlu/Desktop/youtube_video_raw/https_www_youtube_com_channel_UCZZHPXsg6LopvdOKF7qM6cQ_videos_20241202_133249.html my_videos.csv
'''

import csv
import logging
import os
import sys
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('youtube_parser.log')
    ]
)
logger = logging.getLogger(__name__)

class YoutubeParser:
    def __init__(self, html_file: str, output_file: str = 'videos_output.csv'):
        self.html_file = html_file
        self.output_file = output_file
        self.video_data: List[Dict] = []
        
    def parse_duration(self, duration_text: str) -> int:
        """
        Convert duration text (e.g. '1:23' or '12:34:56') to seconds
        
        Args:
            duration_text: String in format 'MM:SS' or 'HH:MM:SS'
            
        Returns:
            Integer representing total seconds
        """
        try:
            parts = duration_text.split(':')
            if len(parts) == 2:
                minutes, seconds = parts
                hours = '0'
            else:
                hours, minutes, seconds = parts
            
            return int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse duration '{duration_text}': {str(e)}")
            return 0
    
    def parse_relative_date(self, relative_date: str, reference_timestamp: str) -> Optional[str]:
        """
        Convert relative date to absolute date
        
        Args:
            relative_date: String like '2 weeks ago'
            reference_timestamp: String in format 'YYYYMMDD_HHMMSS'
            
        Returns:
            String date in 'YYYY-MM-DD' format or None if parsing fails
        """
        if not relative_date or not reference_timestamp:
            return None
            
        try:
            reference_dt = datetime.strptime(reference_timestamp, '%Y%m%d_%H%M%S')
            parts = relative_date.lower().split()
            if len(parts) < 2:
                return None
                
            number = int(parts[0])
            unit = parts[1]
            
            delta_map = {
                'second': timedelta(seconds=number),
                'minute': timedelta(minutes=number),
                'hour': timedelta(hours=number),
                'day': timedelta(days=number),
                'week': timedelta(weeks=number),
                'month': timedelta(days=number * 30),  # Approximate
                'year': timedelta(days=number * 365)  # Approximate
            }
            
            for key, delta in delta_map.items():
                if key in unit:
                    publish_date = reference_dt - delta
                    return publish_date.strftime('%Y-%m-%d')
                    
            return None
        except Exception as e:
            logger.warning(f"Failed to parse relative date '{relative_date}': {str(e)}")
            return None
    
    def validate_input_file(self) -> bool:
        """Validate input file exists and is readable"""
        if not os.path.exists(self.html_file):
            logger.error(f"Input file not found: {self.html_file}")
            return False
        if not os.path.isfile(self.html_file):
            logger.error(f"Input path is not a file: {self.html_file}")
            return False
        if not os.access(self.html_file, os.R_OK):
            logger.error(f"Input file is not readable: {self.html_file}")
            return False
        return True
    
    def extract_video_info(self) -> None:
        """Extract video information from a YouTube page HTML file"""
        if not self.validate_input_file():
            return

        try:
            filename = os.path.basename(self.html_file)
            timestamp_match = re.search(r'_(\d{8}_\d{6})\.html$', filename)
            reference_timestamp = timestamp_match.group(1) if timestamp_match else None
            
            if not reference_timestamp:
                logger.warning("Could not extract timestamp from filename")
            
            with open(self.html_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            logger.info(f"Successfully loaded HTML file: {self.html_file}")
            
            # Find all script tags containing video information
            scripts = soup.find_all('script')
            
            for script in scripts:
                if script.string and 'var ytInitialData = ' in script.string:
                    try:
                        json_str = script.string.split('var ytInitialData = ')[1]
                        json_str = json_str.split(';</script>')[0]
                        json_str = re.sub(r';(?:\s+)?$', '', json_str)
                        data = json.loads(json_str)
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON at position {e.pos}: {e.msg}")
                        print(f"Surrounding content: {json_str[max(0, e.pos-50):e.pos+50]}")
                        continue
                    
                    # Navigate through the JSON structure to find video information
                    contents = data.get('contents', {}).get('twoColumnBrowseResultsRenderer', {}).get('tabs', [])
                    
                    for tab in contents:
                        if 'tabRenderer' in tab and tab['tabRenderer'].get('selected', False):
                            items = tab['tabRenderer'].get('content', {}).get('richGridRenderer', {}).get('contents', [])
                            
                            for item in items:
                                video = item.get('richItemRenderer', {}).get('content', {}).get('videoRenderer', {})
                                if video:
                                    video_id = video.get('videoId', '')
                                    
                                    # Get description snippet
                                    description = ''
                                    desc_snippet = video.get('descriptionSnippet', {}).get('runs', [])
                                    if desc_snippet:
                                        description = desc_snippet[0].get('text', '')
                                    
                                    # Calculate absolute publish date
                                    relative_date = video.get('publishedTimeText', {}).get('simpleText', '')
                                    publish_date = self.parse_relative_date(relative_date, reference_timestamp)
                                    
                                    video_info = {
                                        'video_id': video_id,
                                        'video_id_url': f'https://www.youtube.com/watch?v={video_id}',
                                        'video_title': video.get('title', {}).get('runs', [{}])[0].get('text', ''),
                                        'video_duration': video.get('lengthText', {}).get('simpleText', '0:00'),
                                        'video_view_count': video.get('viewCountText', {}).get('simpleText', '0 views').split(' ')[0],
                                        'video_upload_date': relative_date,
                                        'video_publish_date_absolute': publish_date,
                                        'video_thumbnail_url': f'https://img.youtube.com/vi/{video_id}/maxresdefault.jpg',
                                        'video_description': description
                                    }
                                    self.video_data.append(video_info)
            
        except Exception as e:
            logger.error(f"Error processing file {self.html_file}: {str(e)}")
            raise
    
    def save_to_csv(self) -> None:
        """Save video information to a CSV file"""
        if not self.video_data:
            logger.warning("No video data to save")
            return
        
        try:
            output_dir = os.path.dirname(self.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            fieldnames = [
                'video_id', 'video_id_url', 'video_title', 'video_duration',
                'video_view_count', 'video_upload_date', 'video_publish_date_absolute',
                'video_thumbnail_url', 'video_description'
            ]
            
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.video_data)
                
            logger.info(f"Successfully saved {len(self.video_data)} videos to {self.output_file}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV file {self.output_file}: {str(e)}")
            raise
    
    def run(self) -> bool:
        """
        Main method to run the parser
        
        Returns:
            Boolean indicating success/failure
        """
        try:
            self.extract_video_info()
            self.save_to_csv()
            return True
        except Exception as e:
            logger.error(f"Parser failed: {str(e)}")
            return False

def main() -> int:
    """
    Main function to parse YouTube page and save video information
    
    Returns:
        Integer exit code (0 for success, 1 for failure)
    """
    try:
        if len(sys.argv) < 2:
            logger.error("Usage: python youtube_parser_video.py <html_file> [output_csv_file]")
            return 1
        
        html_file = sys.argv[1]
        default_output = 'output_' + os.path.basename(html_file).replace('.html', '.csv')
        output_file = sys.argv[2] if len(sys.argv) > 2 else default_output
        
        parser = YoutubeParser(html_file, output_file)
        success = parser.run()
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())