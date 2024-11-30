'''
Command line arguments:
    - input_file: the path to the input csv file
    - url_column: the name of the column containing the YouTube URLs
    
Example:
    # Validate YouTube channel URLs from a CSV file
    python youtube_csv_validator.py --input_file "youtube_channel_urls.csv" --url_column "Youtube_Channel_URL" --limit 100
    python youtube_csv_validator.py --input_file "/Users/yuanlu/Code/youtube-top-10000-channels/src/utils/youtube_channel_urls.csv" --url_column "Youtube_Channel_URL" --limit 100
'''

import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from youtube_url_validator import YouTubeValidator
import argparse
import sys

# Configure root logger to output to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_validator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class YoutubeCSVValidator:
    """
    A class to validate YouTube channel URLs from CSV files and extract channel information.
    
    This class reads URLs from a CSV file, validates them using the YouTubeValidator,
    and saves the results back to a new CSV file with additional validation information.
    """

    def __init__(self, input_file: str, url_column: str, limit: Optional[int] = None):
        """
        Initialize the YouTube CSV validator.

        Args:
            input_file (str): Path to the input CSV file
            url_column (str): Name of the column containing YouTube URLs
            limit (Optional[int]): Maximum number of URLs to process. None means process all.
        
        Raises:
            ValueError: If input parameters are invalid
        """
        if not input_file:
            raise ValueError("Input file path cannot be empty")
        if not url_column:
            raise ValueError("URL column name cannot be empty")
        if limit is not None and limit <= 0:
            raise ValueError("Limit must be a positive number")
        
        self._input_file = Path(input_file)
        self._url_column = url_column
        self._limit = limit
        self._validator = YouTubeValidator()
        self._df: Optional[pd.DataFrame] = None
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging for the validator."""
        logging.basicConfig(
            filename='youtube_csv_validator.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def _generate_output_filename(self) -> Path:
        """Generate the output filename with '_validated' suffix."""
        stem = self._input_file.stem
        return self._input_file.parent / f"{stem}_validated{self._input_file.suffix}"

    def _load_csv(self) -> None:
        """Load the CSV file into a pandas DataFrame."""
        try:
            # First check if file exists
            if not self._input_file.exists():
                raise FileNotFoundError(f"Input file not found: {self._input_file}")
            
            # Read CSV in chunks if it's large
            chunk_size = 10000  # Adjust based on your needs
            chunks = []
            total_rows = 0
            
            for chunk in pd.read_csv(self._input_file, chunksize=chunk_size):
                if self._url_column not in chunk.columns:
                    raise ValueError(f"Column '{self._url_column}' not found in CSV file")
                
                if self._limit is not None:
                    remaining_rows = self._limit - total_rows
                    if remaining_rows <= 0:
                        break
                    if len(chunk) > remaining_rows:
                        chunk = chunk.iloc[:remaining_rows]
                
                chunks.append(chunk)
                total_rows += len(chunk)
                
                if self._limit is not None and total_rows >= self._limit:
                    break
            
            self._df = pd.concat(chunks) if chunks else pd.DataFrame()
            logging.info(f"Successfully loaded CSV file: {self._input_file} ({len(self._df)} rows)")
        except Exception as e:
            logging.error(f"Error loading CSV file: {str(e)}")
            raise

    def _validate_urls(self) -> None:
        """Validate URLs and add results to the DataFrame."""
        if self._df is None:
            raise ValueError("DataFrame not initialized. Call load_csv first.")

        logging.info(f"Starting URL validation for {len(self._df)} URLs...")
        results = []
        
        for index, url in enumerate(self._df[self._url_column]):
            logging.info(f"Processing URL {index + 1}/{len(self._df)}: {url}")
            try:
                if pd.isna(url):
                    result = {
                        'validated_url': '',
                        'is_valid': False,
                        'channel_id': '',
                        'handle': '',
                        'subscribers': 0,
                        'error': 'Empty URL'
                    }
                else:
                    validation_result = self._validator.validate_url(url)
                    result = {
                        'validated_url': validation_result.url,
                        'is_valid': validation_result.is_valid,
                        'channel_id': validation_result.channel_id or '',
                        'handle': validation_result.handle or '',
                        'subscribers': validation_result.subscribers or 0,
                        'error': validation_result.error_message or ''
                    }
            except Exception as e:
                logging.error(f"Error validating URL {url}: {str(e)}")
                result = {
                    'validated_url': url,
                    'is_valid': False,
                    'channel_id': '',
                    'handle': '',
                    'subscribers': 0,
                    'error': str(e)
                }
            results.append(result)
            
            if (index + 1) % 10 == 0:
                logging.info(f"Processed {index + 1} URLs...")
        
        logging.info("URL validation completed. Adding results to DataFrame...")
        
        # Add results to DataFrame
        self._df['Validated_Youtube_Channel_URL'] = [r['validated_url'] for r in results]
        self._df['Is_Valid'] = [r['is_valid'] for r in results]
        self._df['Channel_ID'] = [r['channel_id'] for r in results]
        self._df['Handle'] = [r['handle'] for r in results]
        self._df['Subscribers'] = [r['subscribers'] for r in results]
        self._df['Validation_Error'] = [r.get('error', '') for r in results]
        
        logging.info("Results added to DataFrame successfully.")

    def _save_validated_csv(self) -> None:
        """Save the validated results to a new CSV file."""
        if self._df is None:
            raise ValueError("No data to save. Process the URLs first.")
        
        output_file = self._generate_output_filename()
        try:
            self._df.to_csv(output_file, index=False)
            logging.info(f"Successfully saved validated results to: {output_file}")
        except Exception as e:
            logging.error(f"Error saving validated results: {str(e)}")
            raise

    def process(self) -> None:
        """Process the CSV file: load, validate URLs, and save results."""
        try:
            self._load_csv()
            self._validate_urls()
            self._save_validated_csv()
            logging.info("URL validation process completed successfully")
        except Exception as e:
            logging.error(f"Error during processing: {str(e)}")
            raise

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Validate YouTube channel URLs from a CSV file')
    parser.add_argument('--input_file', required=True, help='Path to the input CSV file')
    parser.add_argument('--url_column', required=True, help='Name of the column containing YouTube URLs')
    parser.add_argument('--limit', type=int, help='Maximum number of URLs to process')
    
    args = parser.parse_args()
    
    try:
        validator = YoutubeCSVValidator(
            input_file=args.input_file,
            url_column=args.url_column,
            limit=args.limit
        )
        validator.process()
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 