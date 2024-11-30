'''
Command line arguments:
    - input_file: the path to the input csv file
    - url_column: the name of the column containing the YouTube URLs
    
Example:
    # Validate YouTube channel URLs from a CSV file
    python youtube-csv-validator.py --input_file "youtube_channels.csv" --url_column "Youtube_Channel_URL"
'''

import pandas as pd
import logging
from pathlib import Path
from typing import Optional
from youtube_url_validator import YouTubeValidator

class YoutubeCSVValidator:
    """
    A class to validate YouTube channel URLs from CSV files and extract channel information.
    
    This class reads URLs from a CSV file, validates them using the YouTubeValidator,
    and saves the results back to a new CSV file with additional validation information.
    """

    def __init__(self, input_file: str, url_column: str):
        """
        Initialize the YouTube CSV validator.

        Args:
            input_file (str): Path to the input CSV file
            url_column (str): Name of the column containing YouTube URLs
        """
        self._input_file = Path(input_file)
        self._url_column = url_column
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
            self._df = pd.read_csv(self._input_file)
            if self._url_column not in self._df.columns:
                raise ValueError(f"Column '{self._url_column}' not found in CSV file")
            logging.info(f"Successfully loaded CSV file: {self._input_file}")
        except Exception as e:
            logging.error(f"Error loading CSV file: {str(e)}")
            raise

    def _validate_urls(self) -> None:
        """Validate URLs and add results to the DataFrame."""
        if self._df is None:
            raise ValueError("DataFrame not initialized. Call load_csv first.")

        new_columns = {
            'Validated_Youtube_Channel_URL': [],
            'Is_Valid': [],
            'Channel_ID': [],
            'Handle': [],
            'Subscribers': []
        }

        for url in self._df[self._url_column]:
            try:
                result = self._validator.validate_url(url)
                new_columns['Validated_Youtube_Channel_URL'].append(result.get('validated_url', ''))
                new_columns['Is_Valid'].append(result.get('is_valid', False))
                new_columns['Channel_ID'].append(result.get('channel_id', ''))
                new_columns['Handle'].append(result.get('handle', ''))
                new_columns['Subscribers'].append(result.get('subscribers', 0))
            except Exception as e:
                logging.error(f"Error validating URL {url}: {str(e)}")
                for column in new_columns:
                    new_columns[column].append('')

        for column, values in new_columns.items():
            self._df[column] = values

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
        """
        Process the CSV file: load, validate URLs, and save results.
        """
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
    try:
        validator = YoutubeCSVValidator(
            input_file="youtube_channel_urls.csv",
            url_column="Channel_URL"
        )
        validator.process()
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 