'''
This script converts a CSV file to a SQLite database.

Usage:
    python csv-to-sqlite.py input.csv                # auto-generates output.db
    python csv-to-sqlite.py input.csv output.db      # specifies output file
'''

import sqlite3
import pandas as pd
import sys

def csv_to_sqlite(csv_file, db_file=None):
    # If no db_file specified, create one from csv_file name
    if db_file is None:
        db_file = csv_file.rsplit('.', 1)[0] + '.db'
    
    # Convert CSV to SQLite
    df = pd.read_csv(csv_file)
    table_name = csv_file.split('/')[-1].split('.')[0]
    
    with sqlite3.connect(db_file) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python csv-to-sqlite.py input.csv [output.db]")
        sys.exit(1)
        
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    csv_to_sqlite(input_file, output_file)
