'''
This script converts specified CSV columns to integers.

Usage:
    python csv-column-integer.py --file path/to/file.csv --columns "col1,col2,col3"

Example:
    python csv-column-integer.py --file data/youtubers.csv --columns "Subscribers,Avg_Views,Avg_Likes,Avg_Comments"
'''

import pandas as pd
import argparse
import sys
from pathlib import Path

def convert_columns_to_integer(file_path, columns):
    try:
        # Check if file exists
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Read the CSV file
        print(f"Reading file: {file_path}")
        df = pd.read_csv(file_path)
        
        # Verify all columns exist in the DataFrame
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found in CSV: {', '.join(missing_columns)}")

        # Convert specified columns to integer
        for column in columns:
            print(f"Converting column '{column}' to integer...")
            try:
                df[column] = df[column].astype(int)
            except ValueError as e:
                print(f"Error converting column '{column}': {str(e)}")
                print("Please ensure the column contains only numeric values.")
                sys.exit(1)
        
        # Save the modified DataFrame back to CSV
        print(f"Saving changes to {file_path}")
        df.to_csv(file_path, index=False)
        print("Conversion completed successfully!")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        sys.exit(1)

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Convert specified CSV columns to integers.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--file', required=True, help='Path to the CSV file')
    parser.add_argument('--columns', required=True, 
                       help='Comma-separated list of column names to convert to integer')

    # Parse arguments
    args = parser.parse_args()

    # Process the columns string into a list
    columns_to_convert = [col.strip() for col in args.columns.split(',')]

    # Convert the columns
    convert_columns_to_integer(args.file, columns_to_convert)

if __name__ == "__main__":
    main()

