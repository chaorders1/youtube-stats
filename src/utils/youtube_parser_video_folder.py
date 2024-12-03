'''
YouTube Page Parser - Folder Processor

A utility to process multiple YouTube HTML files in a folder and extract video metadata into CSV format.

Arguments:
    input_folder    Required. Path to the folder containing YouTube HTML files
    output_folder   Optional. Path where output CSV files will be saved
                   If not specified, files will be saved to './output'
    --workers      Optional. Maximum number of concurrent workers (default: 4)
                   Example: --workers 8

Usage:
    python youtube_parser_video_folder.py <input_folder> [output_folder] [--workers N]

Examples:
    # Basic usage with default output folder and workers
    python youtube_parser_video_folder.py ./youtube_pages

    # Specify custom output folder
    python youtube_parser_video_folder.py ./youtube_pages ./parsed_results
    
    # Specify number of workers
    python youtube_parser_video_folder.py ./youtube_pages --workers 8
    
    # Using all options
    python youtube_parser_video_folder.py ./youtube_pages ./parsed_results --workers 8
    python youtube_parser_video_folder.py /Users/yuanlu/Desktop/youtube_video_raw_copy /Users/yuanlu/Desktop/output --workers 8
'''

import os
import sys
import logging
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from youtube_parser_video import YoutubeParser
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('youtube_parser_folder.log')
    ]
)
logger = logging.getLogger(__name__)

class YoutubeFolderParser:
    def __init__(self, input_folder: str, output_folder: Optional[str] = None):
        self.input_folder = input_folder
        self.output_folder = output_folder or 'output'
        
    def validate_folders(self) -> bool:
        """Validate input folder exists and output folder is writable"""
        if not os.path.exists(self.input_folder):
            logger.error(f"Input folder not found: {self.input_folder}")
            return False
        if not os.path.isdir(self.input_folder):
            logger.error(f"Input path is not a directory: {self.input_folder}")
            return False
            
        # Create output folder if it doesn't exist
        try:
            os.makedirs(self.output_folder, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create output folder: {str(e)}")
            return False
            
        return True
        
    def get_html_files(self) -> List[str]:
        """Get list of HTML files in the input folder"""
        html_files = []
        try:
            for file in os.listdir(self.input_folder):
                if file.endswith('.html'):
                    html_files.append(os.path.join(self.input_folder, file))
        except Exception as e:
            logger.error(f"Error listing directory: {str(e)}")
            
        return html_files
        
    def process_single_file(self, html_file: str) -> bool:
        """Process a single HTML file"""
        try:
            output_name = f'output_{os.path.basename(html_file)}'.replace('.html', '.csv')
            output_path = os.path.join(self.output_folder, output_name)
            
            parser = YoutubeParser(html_file, output_path)
            success = parser.run()
            
            if success:
                logger.info(f"Successfully processed {html_file}")
            else:
                logger.warning(f"Failed to process {html_file}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error processing {html_file}: {str(e)}")
            return False
            
    def process_folder(self, max_workers: int = 4) -> bool:
        """
        Process all HTML files in the input folder
        
        Args:
            max_workers: Maximum number of concurrent workers for processing
            
        Returns:
            Boolean indicating overall success/failure
        """
        if not self.validate_folders():
            return False
            
        html_files = self.get_html_files()
        if not html_files:
            logger.warning(f"No HTML files found in {self.input_folder}")
            return False
            
        logger.info(f"Found {len(html_files)} HTML files to process")
        
        success_count = 0
        failure_count = 0
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(self.process_single_file, html_file): html_file 
                for html_file in html_files
            }
            
            for future in as_completed(future_to_file):
                html_file = future_to_file[future]
                try:
                    if future.result():
                        success_count += 1
                    else:
                        failure_count += 1
                except Exception as e:
                    logger.error(f"Unexpected error processing {html_file}: {str(e)}")
                    failure_count += 1
                    
        logger.info(f"Processing complete. Success: {success_count}, Failures: {failure_count}")
        return failure_count == 0

def main() -> int:
    """
    Main function to process folder of YouTube pages
    
    Returns:
        Integer exit code (0 for success, 1 for failure)
    """
    try:
        parser = argparse.ArgumentParser(description='Process YouTube HTML files in a folder')
        parser.add_argument('input_folder', help='Path to the folder containing YouTube HTML files')
        parser.add_argument('output_folder', nargs='?', help='Path where output CSV files will be saved')
        parser.add_argument('--workers', type=int, default=4, help='Maximum number of concurrent workers (default: 4)')
        
        args = parser.parse_args()
        
        folder_parser = YoutubeFolderParser(args.input_folder, args.output_folder)
        success = folder_parser.process_folder(max_workers=args.workers)
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 