'''
Command line arguments:
    - input_file: the path to the input csv file
    - url_column: the name of the column containing the YouTube URLs
    
Example:
    # Validate YouTube channel URLs from a CSV file
    python youtube_csv_validator.py --input_file "youtube_channel_urls.csv" --url_column "Youtube_Channel_URL" --limit 100
    python youtube_csv_validator.py --input_file "/Users/yuanlu/Code/youtube-top-10000-channels/src/utils/youtube_channel_urls_web.csv" --url_column "Youtube_Channel_URL" --limit 100
'''

import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from youtube_url_validator import YouTubeValidator
import argparse
import sys
import json
from enum import Enum
from datetime import datetime, timedelta
import time
import random

# Configure root logger to output to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('youtube_validator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class ValidationStatus(Enum):
    """Enumeration for validation status tracking."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class ProcessingStats:
    """Class to track processing statistics."""
    def __init__(self):
        self.start_time = datetime.now()
        self.processed_count = 0
        self.valid_count = 0
        self.invalid_count = 0
        self.error_count = 0
        self.last_batch_time = datetime.now()

    def update(self, batch_results: list) -> None:
        """Update processing statistics."""
        now = datetime.now()
        self.processed_count += len(batch_results)
        self.valid_count += sum(1 for r in batch_results if r['is_valid'])
        self.invalid_count += sum(1 for r in batch_results if not r['is_valid'])
        self.error_count += sum(1 for r in batch_results if r['error'])
        
        # Calculate processing rate
        batch_duration = (now - self.last_batch_time).total_seconds()
        self.last_batch_time = now
        
        return {
            'processed': self.processed_count,
            'valid': self.valid_count,
            'invalid': self.invalid_count,
            'errors': self.error_count,
            'rate': len(batch_results) / batch_duration if batch_duration > 0 else 0,
            'elapsed': (now - self.start_time).total_seconds()
        }

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
        self._checkpoint_size = 10
        self._rate_settings = {
            'min_delay': 0.1,
            'max_delay': 1,
            'burst_size': 20,
            'burst_delay': 0.2,
            'error_delay': 1.0,
        }
        self._request_times = []
        self._error_count = 0
        self._last_error_time = None
        self._setup_logging()
        self._status_file = self._input_file.parent / f"{self._input_file.stem}_status.json"
        self._status = self._load_status()

    def _setup_logging(self) -> None:
        """Configure logging for the validator."""
        logging.basicConfig(
            filename='youtube_csv_validator.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def _load_csv(self) -> None:
        """Load the CSV file and initialize new columns if needed."""
        try:
            if not self._input_file.exists():
                raise FileNotFoundError(f"Input file not found: {self._input_file}")
            
            # Read the entire CSV file
            self._df = pd.read_csv(self._input_file)
            
            if self._url_column not in self._df.columns:
                raise ValueError(f"Column '{self._url_column}' not found in CSV file")
            
            # Initialize new columns if they don't exist, preserving existing data
            new_columns = {
                'validated_url': '',
                'is_valid': pd.NA,
                'channel_id': '',
                'handle': '',
                'subscribers': pd.NA,
                'error': ''
            }
            
            for col, default_value in new_columns.items():
                if col not in self._df.columns:
                    # Only add new column with default value if it doesn't exist
                    self._df[col] = default_value
            
            # Apply limit if specified, but only for processing
            if self._limit is not None:
                self._df = self._df.iloc[:self._limit].copy()
            
            logging.info(f"Successfully loaded CSV file: {self._input_file} ({len(self._df)} rows)")
        
        except Exception as e:
            logging.error(f"Error loading CSV file: {str(e)}")
            raise

    def _save_checkpoint(self, results: list, start_idx: int) -> None:
        """Update the input CSV file with new results while preserving original data."""
        max_retries = 3
        retry_count = 0
        
        validation_columns = [
            'validated_url', 'is_valid', 'channel_id',
            'handle', 'subscribers', 'error'
        ]
        
        while retry_count < max_retries:
            try:
                # Read the current state of the file
                current_df = pd.read_csv(self._input_file)
                
                # Create backup of current file
                backup_file = self._input_file.parent / f"{self._input_file.stem}_backup.csv"
                current_df.to_csv(backup_file, index=False)
                
                # Ensure validation columns exist
                for col in validation_columns:
                    if col not in current_df.columns:
                        current_df[col] = pd.NA if col in ['is_valid', 'subscribers'] else ''
                
                # Update only the new validation columns for processed URLs
                for result in results:
                    url = result[self._url_column]
                    idx = current_df[current_df[self._url_column] == url].index
                    if len(idx) > 0:
                        # Only update the validation columns, preserve all other data
                        for col in validation_columns:
                            if col in result:
                                current_df.loc[idx[0], col] = result[col]
                
                # Save the updated DataFrame back to the original file
                current_df.to_csv(self._input_file, index=False)
                
                logging.info(f"Saved checkpoint at index {start_idx + len(results)}")
                self._update_status(results)
                
                # Remove backup if save was successful
                if backup_file.exists():
                    backup_file.unlink()
                return
                
            except Exception as e:
                retry_count += 1
                logging.error(f"Checkpoint save attempt {retry_count} failed: {e}")
                if retry_count == max_retries:
                    self._status['status'] = ValidationStatus.FAILED.value
                    self._status['errors'].append(str(e))
                    self._update_status([])
                    raise
                time.sleep(1)

    def _get_delay(self) -> float:
        """
        Determine the appropriate delay before the next request based on recent activity.
        """
        now = datetime.now()
        
        # Clean up old request times (older than 60 seconds)
        self._request_times = [t for t in self._request_times 
                             if now - t < timedelta(seconds=60)]
        
        # If we've had recent errors, increase delays
        if self._last_error_time and now - self._last_error_time < timedelta(seconds=300):
            return max(
                self._rate_settings['error_delay'],
                random.uniform(self._rate_settings['max_delay'], 
                             self._rate_settings['max_delay'] * 2)
            )
        
        # Check if we're in a burst
        recent_requests = len([t for t in self._request_times 
                             if now - t < timedelta(seconds=5)])
        
        if recent_requests >= self._rate_settings['burst_size']:
            return self._rate_settings['burst_delay']
        
        # Return random delay within configured range
        return random.uniform(
            self._rate_settings['min_delay'],
            self._rate_settings['max_delay']
        )

    def _validate_urls(self) -> None:
        """Validate URLs with improved rate limiting and resume capability."""
        stats = ProcessingStats()
        
        if self._df is None:
            raise ValueError("DataFrame not initialized. Call load_csv first.")

        total_urls = len(self._df)
        
        # Create a mask for unprocessed rows (where subscribers is NA)
        unprocessed_mask = self._df['subscribers'].isna()
        remaining_df = self._df[unprocessed_mask].copy()
        
        logging.info(f"Found {len(remaining_df)} unprocessed URLs out of {total_urls} total URLs")
        
        current_batch = []
        
        for index, row in remaining_df.iterrows():
            url = row[self._url_column]
            logging.info(f"Processing URL at index {index}: {url}")
            
            try:
                delay = self._get_delay()
                time.sleep(delay)
                
                if pd.isna(url):
                    result = {
                        self._url_column: url,
                        'validated_url': '',
                        'is_valid': False,
                        'channel_id': '',
                        'handle': '',
                        'subscribers': 0,
                        'error': 'Empty URL'
                    }
                else:
                    # 添加详细的时间记录
                    start_time = time.time()
                    validation_result = self._validator.validate_url(url)
                    end_time = time.time()
                    request_time = end_time - start_time
                    
                    logging.info(f"Request time for {url}: {request_time:.3f}s")
                    if request_time > 1.0:  # 记录较慢的请求
                        logging.warning(f"Slow request detected: {request_time:.3f}s for {url}")
                    
                    self._request_times.append(datetime.now())
                    self._error_count = max(0, self._error_count - 1)
                    
                    result = {
                        self._url_column: url,
                        'validated_url': validation_result.url,
                        'is_valid': validation_result.is_valid,
                        'channel_id': validation_result.channel_id or '',
                        'handle': validation_result.handle or '',
                        'subscribers': validation_result.subscribers or 0,
                        'error': validation_result.error_message or ''
                    }
            
            except Exception as e:
                error_message = str(e).lower()
                if any(term in error_message for term in ['rate limit', '429', 'too many requests']):
                    # Save the current batch before terminating
                    if current_batch:
                        self._save_checkpoint(current_batch, index - len(current_batch) + 1)
                    logging.error(f"Rate limit detected. Terminating process. Error: {str(e)}")
                    self._status['status'] = ValidationStatus.FAILED.value
                    self._status['errors'].append(str(e))
                    self._update_status([])
                    sys.exit(1)  # Terminate the program with error code 1
                
                logging.error(f"Error validating URL {url}: {str(e)}")
                result = {
                    self._url_column: url,
                    'validated_url': url,
                    'is_valid': False,
                    'channel_id': '',
                    'handle': '',
                    'subscribers': 0,
                    'error': str(e)
                }
            
            current_batch.append(result)
            
            if len(current_batch) >= self._checkpoint_size:
                self._save_checkpoint(current_batch, index - len(current_batch) + 1)
                progress_stats = stats.update(current_batch)
                logging.info(
                    f"Progress: {progress_stats['processed']}/{len(remaining_df)} "
                    f"(Valid: {progress_stats['valid']}, "
                    f"Invalid: {progress_stats['invalid']}, "
                    f"Rate: {progress_stats['rate']:.2f} URLs/sec)"
                )
                current_batch = []
        
        if current_batch:
            self._save_checkpoint(current_batch, len(remaining_df) - len(current_batch))
        
        logging.info("URL validation completed.")

    def process(self) -> None:
        """Process the CSV file: load, validate URLs, and save results."""
        try:
            self._load_csv()
            self._validate_urls()
            logging.info("URL validation process completed successfully")
        except Exception as e:
            logging.error(f"Error during processing: {str(e)}")
            raise

    def _load_status(self) -> dict:
        """Load processing status from status file."""
        try:
            if self._status_file.exists():
                with open(self._status_file, 'r') as f:
                    return json.load(f)
            return {
                'status': ValidationStatus.PENDING.value,
                'last_processed_index': 0,
                'last_update': None,
                'total_processed': 0,
                'total_valid': 0,
                'total_invalid': 0,
                'errors': []
            }
        except Exception as e:
            logging.warning(f"Error loading status file: {e}")
            return self._create_new_status()

    def _update_status(self, batch_results: list) -> None:
        """Update and save processing status."""
        try:
            self._status['last_update'] = datetime.now().isoformat()
            self._status['total_processed'] += len(batch_results)
            self._status['total_valid'] += sum(1 for r in batch_results if r['is_valid'])
            self._status['total_invalid'] += sum(1 for r in batch_results if not r['is_valid'])
            
            with open(self._status_file, 'w') as f:
                json.dump(self._status, f, indent=2)
        except Exception as e:
            logging.error(f"Error updating status: {e}")

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