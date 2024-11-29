'''
This script converts CSV files to a SQLite database.
For CSV files with identical columns, data will be combined into a single table.

Usage:
    python csv-to-sqlite.py --file input.csv                    # single file
    python csv-to-sqlite.py --folder /path/to/folder            # process all CSVs in folder
    python csv-to-sqlite.py --folder ./data --output custom.db  # specify output db
'''

import sqlite3
import pandas as pd
import sys
import argparse
from pathlib import Path

def process_csv_files(file_paths, db_file=None, table_name='combined_data'):
    # If no db_file specified, create one from folder/file name
    if db_file is None:
        if isinstance(file_paths, list):
            db_file = Path(file_paths[0]).parent / 'output.db'
        else:
            db_file = file_paths.parent / 'output.db'
    
    print(f"Creating/updating database: {db_file}")
    
    # Initialize an empty list to store all dataframes
    all_data = []
    
    if isinstance(file_paths, list):
        csv_files = file_paths
    else:  # Path object (folder)
        csv_files = list(file_paths.glob('*.csv'))
    
    if not csv_files:
        print("No CSV files found!")
        return
        
    # Read and combine all CSV files
    for csv_file in csv_files:
        try:
            print(f"Processing {csv_file}...")
            df = pd.read_csv(csv_file)
            # Add source file column if you want to track which file the data came from
            df['source_file'] = csv_file.name
            all_data.append(df)
            
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
    
    if all_data:
        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Total rows collected: {len(combined_df)}")
        
        # Save to SQLite
        with sqlite3.connect(db_file) as conn:
            combined_df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Successfully created table: {table_name}")

def main():
    parser = argparse.ArgumentParser(description='Convert CSV files to SQLite database.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', help='Single CSV file to convert')
    group.add_argument('--folder', help='Folder containing CSV files to convert')
    parser.add_argument('--output', help='Output database file (optional)')
    parser.add_argument('--table', default='combined_data', 
                       help='Name of the table in SQLite (default: combined_data)')
    
    args = parser.parse_args()
    
    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"Error: File {file_path} does not exist!")
            return
        process_csv_files([file_path], args.output, args.table)
    
    elif args.folder:
        folder_path = Path(args.folder)
        if not folder_path.exists() or not folder_path.is_dir():
            print(f"Error: Folder {folder_path} does not exist!")
            return
        process_csv_files(folder_path, args.output, args.table)

if __name__ == "__main__":
    main()
