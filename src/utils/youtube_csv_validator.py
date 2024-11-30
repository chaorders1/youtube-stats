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
import json
from enum import Enum
from datetime import datetime
import time

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
        self._output_file = self._generate_output_filename()
        self._checkpoint_size = 10
        self._processed_count = self._get_processed_count()
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

    def _get_processed_count(self) -> int:
        """Get the number of already processed URLs with valid results."""
        try:
            if self._output_file.exists():
                df = pd.read_csv(self._output_file)
                if 'Is_Valid' in df.columns:
                    # Count rows where Is_Valid is not null
                    return df['Is_Valid'].notna().sum()
            return 0
        except Exception as e:
            logging.warning(f"Error reading existing output file: {e}")
            return 0

    def _save_checkpoint(self, results: list, start_idx: int) -> None:
        """Save a checkpoint of processed results to CSV with retry mechanism."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Create backup of existing file if it exists
                if self._output_file.exists():
                    backup_file = self._output_file.parent / f"{self._output_file.stem}_backup{self._output_file.suffix}"
                    import shutil
                    shutil.copy2(self._output_file, backup_file)
                
                # Create DataFrame for this batch
                batch_df = pd.DataFrame(results)
                
                if self._output_file.exists() and start_idx > 0:
                    # Read existing data
                    existing_df = pd.read_csv(self._output_file)
                    
                    # Update only new results, preserving existing validated entries
                    for idx, row in batch_df.iterrows():
                        url = row[self._url_column]
                        # Update or append based on URL match
                        mask = existing_df[self._url_column] == url
                        if mask.any():
                            # Update existing entry if Is_Valid is null
                            null_mask = existing_df.loc[mask, 'Is_Valid'].isna()
                            if null_mask.any():
                                existing_df.loc[mask & null_mask] = row
                        else:
                            # Append new entry
                            existing_df = pd.concat([existing_df, pd.DataFrame([row])], ignore_index=True)
                    
                    existing_df.to_csv(self._output_file, index=False)
                else:
                    # First batch - save directly
                    batch_df.to_csv(self._output_file, index=False)
                
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
                time.sleep(1)  # Wait before retry

    def _validate_urls(self) -> None:
        """Validate URLs with progress tracking."""
        stats = ProcessingStats()
        
        if self._df is None:
            raise ValueError("DataFrame not initialized. Call load_csv first.")

        total_urls = len(self._df)
        logging.info(f"Starting URL validation from index {self._processed_count} of {total_urls} URLs...")
        
        # Skip already processed URLs
        remaining_df = self._df.iloc[self._processed_count:]
        current_batch = []
        
        for index, url in enumerate(remaining_df[self._url_column], start=self._processed_count):
            logging.info(f"Processing URL {index + 1}/{total_urls}: {url}")
            try:
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
                    validation_result = self._validator.validate_url(url)
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
            
            # Save checkpoint every self._checkpoint_size URLs
            if len(current_batch) >= self._checkpoint_size:
                self._save_checkpoint(current_batch, index - len(current_batch) + 1)
                progress_stats = stats.update(current_batch)
                logging.info(
                    f"Progress: {progress_stats['processed']}/{total_urls} "
                    f"(Valid: {progress_stats['valid']}, "
                    f"Invalid: {progress_stats['invalid']}, "
                    f"Rate: {progress_stats['rate']:.2f} URLs/sec)"
                )
                current_batch = []
        
        # Save any remaining results
        if current_batch:
            self._save_checkpoint(current_batch, total_urls - len(current_batch))
        
        logging.info("URL validation completed.")
        
        # Load the final results into self._df
        self._df = pd.read_csv(self._output_file)

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